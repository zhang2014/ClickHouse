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
#include <Interpreters/InterpreterFactory.h>


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
    : context(context), state_machine_storage(state_machine_storage), entity_state(entity_state), work_cluster(work_cluster),
      connections(getConnectionPoolsFromClusters({work_cluster}))
{
    /// TODO sleep_time for settings
    sleep_time = std::chrono::milliseconds(5000);
}

void QingCloudPaxosLearner::work()
{
    CurrentThread::initializeQuery();
    setThreadName("QingCloudPaxosLearner");
    std::unique_lock<std::mutex> lock{mutex};
    const auto quit_requested = [this] { return quit.load(std::memory_order_relaxed); };

    while (!quit_requested())
    {
        try
        {
            std::lock_guard<std::mutex> state_lock(entity_state.mutex);

            size_t learning_size = learning();
            if (learning_size == applyDDLQueries() && learning_size == 10)
                continue;
        }
        catch (...)
        {
            tryLogCurrentException(&Logger::get("QingCloudPaxosLearner"));
        }

        cond.wait_for(lock, sleep_time, quit_requested);
    }
}

size_t QingCloudPaxosLearner::learning()
{
    const String offset = toString(entity_state.accepted_paxos_id);
    const String query_without_agg = "SELECT * FROM system.ddl_queue WHERE proposer_id > " + offset + " ORDER BY proposer_id LIMIT 10";
    const String query_with_agg = "SELECT DISTINCT id, proposer_id, query_string FROM system.ddl_queue WHERE _res_code = 0 ORDER BY proposer_id";

    Settings query_settings = context.getSettingsRef();
    Block fetch_res = queryWithTwoLevel(query_without_agg, query_with_agg);
    BlockOutputStreamPtr out = state_machine_storage->write({}, query_settings);
    out->writePrefix();out->write(fetch_res);out->writeSuffix();

    entity_state.accepted_paxos_id = typeid_cast<const ColumnUInt64 &>(*fetch_res.getByName("proposer_id").column).getUInt(rows - 1);
    entity_state.accepted_entity_id = typeid_cast<const ColumnUInt64 &>(*fetch_res.getByName("id").column).getUInt(rows - 1);
    entity_state.accepted_entity_value = typeid_cast<const ColumnString &>(*fetch_res.getByName("query_string").column).getDataAt(rows - 1).toString();
    entity_state.store();

    return fetch_res.rows();
}

size_t QingCloudPaxosLearner::applyDDLQueries()
{
    const String offset = toString(entity_state.applied_paxos_id);
    const String query_with_distinct = "SELECT DISTINCT id, proposer_id, query_string FROM system.ddl_queue WHERE proposer_id > " + offset + " ORDER BY proposer_id";

    Block wait_apply_res = executeLocalQuery(query_with_distinct, context);

    for (size_t row = 0; row < wait_apply_res.rows(); ++row)
    {
        const String query = typeid_cast<const ColumnString &>(*fetch_res.getByName("query_string").column).getDataAt(rows - 1).toString();
        executeLocalQuery(query, context);
        /// TODO: try catch exception
    }

    entity_state.applied_paxos_id = typeid_cast<const ColumnUInt64 &>(*wait_apply_res.getByName("proposer_id").column).getUInt(rows - 1);
    entity_state.applied_entity_id = typeid_cast<const ColumnUInt64 &>(*wait_apply_res.getByName("id").column).getUInt(rows - 1);
    return wait_apply_res.rows();
}

Block QingCloudPaxosLearner::queryWithTwoLevel(const String & first_query, const String & second_query)
{
    BlockInputStreams streams;
    Settings settings = context.getSettingsRef();

    for (size_t index = 0; index < connections.size(); ++index)
    {
        ConnectionPoolPtrs failover_connections;
        failover_connections.emplace_back(connections[index]);
        ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
            failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

        streams.emplace_back(std::make_shared<QingCloudErroneousBlockInputStream>(
            std::make_shared<RemoteBlockInputStream>(shard_pool, first_query, header, context, &settings)));
    }

    ParserSelectQuery select_parser;
    ASTPtr node = parseQuery(select_parser, second_query.data(), second_query.data() + second_query.size(), "", 0);
    InterpreterSelectQuery interpreter(node, context, std::make_shared<UnionBlockInputStream<>>(streams, nullptr, streams.size()));
    return std::make_shared<SquashingBlockInputStream>(interpreter.execute().in, std::numeric_limits<size_t>::max(),
                                                       std::numeric_limits<size_t>::max())->read();
}

}
