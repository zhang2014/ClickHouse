#include <QingCloud/Interpreters/Paxos/QingCloudPaxosLearner.h>
#include <Common/setThreadName.h>
#include <Common/CurrentThread.h>
#include <QingCloud/Common/differentClusters.h>
#include <QingCloud/Datastream/QingCloudErroneousBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Parsers/ParserSelectQuery.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Interpreters/SpecializedAggregator.h>
#include <Parsers/ParserQuery.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/InterpreterFactory.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <QingCloud/Parsers/ParserPaxosQuery.h>
#include <QingCloud/Interpreters/InterpreterPaxosQuery.h>


namespace DB
{

static Block executeLocalQuery(const String &query_string, const Context &context)
{
    Context query_context(context);
    query_context.getSettingsRef().internal_query = true;

    ParserQuery query_parser(query_string.data() + query_string.size());
    ASTPtr query = parseQuery(query_parser, query_string.data(), query_string.data() + query_string.size(), "", 0);
    std::unique_ptr<IInterpreter> interpreter = InterpreterFactory::get(query, const_cast<Context &>(context));
    if (BlockInputStreamPtr input = interpreter->execute().in)
        return std::make_shared<SquashingBlockInputStream>(input, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())->read();
    return {};
}

QingCloudPaxosLearner::QingCloudPaxosLearner(DDLEntity &entity_state, const StoragePtr &state_machine_storage,
                                             const ClusterPtr &work_cluster, const Context &context)
    : context(context), state_machine_storage(state_machine_storage), entity_state(entity_state), work_cluster(work_cluster)
{
    /// TODO sleep_time for settings
    sleep_time = std::chrono::milliseconds(5000);
    std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(context.getConfigRef(), "", "listen_host");
    self_address = listen_hosts.empty() ?  "localhost" : listen_hosts[0];
    auto work_cluster_connections = getConnectionPoolsFromClusters({work_cluster});
    for (const auto & connection : work_cluster_connections)
    {
        if (connection.first.is_local)
        {
            connections = work_cluster_connections;
            return;
        }
    }
}

void QingCloudPaxosLearner::work()
{
    CurrentThread::initializeQuery();
    setThreadName("QingCloudPaxosLearner");
    std::unique_lock<std::recursive_mutex> state_lock(entity_state.mutex);
    const auto quit_requested = [this] { return quit.load(std::memory_order_relaxed); };

    while (!quit_requested())
    {
        try
        {
            learning();
            applyDDLQueries();
            entity_state.learning_cond.wait_for(state_lock, sleep_time, quit_requested);
        }
        catch (...)
        {
            tryLogCurrentException(&Logger::get("QingCloudPaxosLearner"));
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
}

void QingCloudPaxosLearner::wakeup()
{
    cond.notify_one();
    std::lock_guard<std::recursive_mutex> state_lock(entity_state.mutex);
}

void QingCloudPaxosLearner::learning()
{
    while (true)
    {
        const String offset = toString(entity_state.accepted_paxos_id);
        const String query_without_agg = "SELECT * FROM system.ddl_queue WHERE proposer_id > " + offset + " ORDER BY proposer_id LIMIT 10";
        const String query_with_agg = "SELECT DISTINCT id, proposer_id, query_string, from FROM system.ddl_queue WHERE _res_code = 0 ORDER BY proposer_id";

        Settings query_settings = context.getSettingsRef();
        Block fetch_res = queryWithTwoLevel(query_without_agg, query_with_agg);

        if (!fetch_res || !fetch_res.rows())
            return;

        size_t rows = fetch_res.rows();
        BlockOutputStreamPtr out = state_machine_storage->write({}, query_settings);
        out->writePrefix();out->write(fetch_res);out->writeSuffix();
        entity_state.accepted_paxos_id = typeid_cast<const ColumnUInt64 &>(*fetch_res.getByName("proposer_id").column).getUInt(rows - 1);
        entity_state.accepted_entity_id = typeid_cast<const ColumnUInt64 &>(*fetch_res.getByName("id").column).getUInt(rows - 1);
        entity_state.accepted_entity_value = typeid_cast<const ColumnString &>(*fetch_res.getByName("query_string").column).getDataAt(rows - 1).toString();
        entity_state.store();
    }
}

void QingCloudPaxosLearner::applyDDLQueries()
{
    while (true)
    {
        const String offset = toString(entity_state.applied_paxos_id);
        const String query_with_distinct = "SELECT DISTINCT id, proposer_id, query_string, from FROM system.ddl_queue WHERE proposer_id > " + offset + " ORDER BY proposer_id LIMIT 10";

        Block wait_apply_res = executeLocalQuery(query_with_distinct, context);

        if (!wait_apply_res || !wait_apply_res.rows())
            return;

        for (size_t row = 0; row < wait_apply_res.rows(); ++row)
        {
            const UInt64 paxos_id = typeid_cast<const ColumnUInt64 &>(*wait_apply_res.getByName("proposer_id").column).getUInt(row);
            const UInt64 entity_id = typeid_cast<const ColumnUInt64 &>(*wait_apply_res.getByName("id").column).getUInt(row);
            const String from = typeid_cast<const ColumnString &>(*wait_apply_res.getByName("from").column).getDataAt(row).toString();;
            const String apply_query = typeid_cast<const ColumnString &>(*wait_apply_res.getByName("query_string").column).getDataAt(row).toString();

            applyDDLQuery(entity_id, apply_query, from);
            entity_state.applied_paxos_id = paxos_id;
            entity_state.applied_entity_id = entity_id;
            entity_state.store();
        }
    }
}

void QingCloudPaxosLearner::applyDDLQuery(const UInt64 &entity_id, const String &apply_query, const String &from)
{
    try
    {
        executeLocalQuery(apply_query, context);
        sendQueryToPaxosProxy(
            "PAXOS PAXOS_NOTIFY res_state=0, entity_id=" + toString(entity_id) + ", exception_message='', from ='" + self_address + "'", from);
    }
    catch (...)
    {
        int error_code = getCurrentExceptionCode();
        const String exception_message = getCurrentExceptionMessage(false);
        sendQueryToPaxosProxy("PAXOS PAXOS_NOTIFY res_state=" + toString(error_code) + ",entity_id=" +
                              toString(entity_id) + ",exception_message='" + escapeForFileName(exception_message) + "',from='" + self_address + "'",
                              from);
    }
}

Block QingCloudPaxosLearner::queryWithTwoLevel(const String & first_query, const String & second_query)
{
    BlockInputStreams streams;
    Settings settings = context.getSettingsRef();
    Block header = state_machine_storage->getSampleBlock();

    for (size_t index = 0; index < connections.size(); ++index)
    {
        if (!connections[index].first.is_local)
        {
            ConnectionPoolPtrs failover_connections;
            failover_connections.emplace_back(connections[index].second);
            ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
                failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

            streams.emplace_back(std::make_shared<QingCloudErroneousBlockInputStream>(
                std::make_shared<RemoteBlockInputStream>(shard_pool, first_query, header, context, &settings)));
        }
    }

    if (streams.empty())
        return {};

    ParserSelectQuery select_parser;
    ASTPtr node = parseQuery(select_parser, second_query.data(), second_query.data() + second_query.size(), "", 0);
    InterpreterSelectQuery interpreter(node, context, std::make_shared<UnionBlockInputStream<>>(streams, nullptr, streams.size()));
    return std::make_shared<SquashingBlockInputStream>(interpreter.execute().in, std::numeric_limits<size_t>::max(),
                                                       std::numeric_limits<size_t>::max())->read();
}

Block QingCloudPaxosLearner::sendQueryToPaxosProxy(const String &send_query, const String & from)
{
    std::cout << "Send Query to Paxos Proxy " << send_query << "\n";
    Block header =  Block{};
    Settings settings = context.getSettingsRef();

    BlockInputStreamPtr source;
    for (const auto & connection : connections)
    {
        if (connection.first.host_name == from && connection.first.is_local)
        {
            ParserPaxos paxos_parser;
            ASTPtr node = parseQuery(paxos_parser, send_query.data(), send_query.data() + send_query.size(), "", 0);
            InterpreterPaxosQuery interpreter(node, context);
            source = interpreter.execute().in;
        }
        else if (connection.first.host_name == from)
        {
            ConnectionPoolPtrs failover_connections;
            failover_connections.emplace_back(connection.second);
            ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
                failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

            source = std::make_shared<RemoteBlockInputStream>(shard_pool, send_query, header, context, &settings);
        }

        if (source)
            return std::make_shared<SquashingBlockInputStream>(source, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())->read();
    }

    return {};
}

QingCloudPaxosLearner::~QingCloudPaxosLearner()
{
    if (!quit)
    {
        {
            quit = true;
            std::lock_guard<std::recursive_mutex> lock{entity_state.mutex};
        }
        entity_state.learning_cond.notify_one();
        thread.join();
    }
}

}
