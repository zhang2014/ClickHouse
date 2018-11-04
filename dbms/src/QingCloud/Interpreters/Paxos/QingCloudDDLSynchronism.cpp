#include <QingCloud/Interpreters/Paxos/QingCloudDDLSynchronism.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Functions/FunctionsComparison.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Common/setThreadName.h>
#include <Parsers/ParserInsertQuery.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Parsers/ParserQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <QingCloud/Datastream/QingCloudErroneousBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
#include <Parsers/ASTLiteral.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <IO/WriteBufferFromFile.h>
#include <DataStreams/copyData.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
extern const int PAXOS_EXCEPTION;
}

static BlockIO executeQuery(const String &query_string, const Context &context)
{
    Context query_context(context);
    query_context.getSettingsRef().internal_query = true;

    ParserQuery query_parser(query_string.data() + query_string.size());
    ASTPtr query = parseQuery(query_parser, query_string.data(), query_string.data() + query_string.size(), "", 0);
    std::unique_ptr<IInterpreter> interpreter = InterpreterFactory::get(query, const_cast<Context &>(context));
    return interpreter->execute();
}

QingCloudDDLSynchronism::QingCloudDDLSynchronism(const Context &context, const String &node_id) : context(context)
{

//    context.getConfigRef().getString("");
    /// TODO sleep_time
    storage = createDDLQueue(context);
    sleep_time = std::chrono::milliseconds(5000);
    data_path = context.getConfigRef().getString("");
    const auto multiplexed_version = context.getMultiplexedVersion();
    updateAddressesAndConnections(node_id, multiplexed_version->getAddressesAndConnections());
    thread = std::thread{&QingCloudDDLSynchronism::work, this};
}

StoragePtr QingCloudDDLSynchronism::createDDLQueue(const Context & context)
{
    StoragePtr storage = context.tryGetTable("system", "ddl_queue");
    if (!storage)
    {
        String create_query = "CREATE TABLE system.ddl_queue(id UInt64, proposer_id UInt64, query_string String)";
        String storage_declare = "ENGINE = MergeTree PARTITION BY proposer_id / 300000 ORDER BY (id, proposer_id)";
        executeQuery(create_query + storage_declare, context);
        storage = context.getTable("system", "ddl_queue");
    }
    return storage;
}

void
QingCloudDDLSynchronism::updateAddressesAndConnections(const String &node_id, const AddressesWithConnections &addresses_with_connections)
{
    std::unique_lock<std::mutex> lock{mutex};

    connections.clear();
    connections.reserve(addresses_with_connections.size());
    for (auto & addresses_with_connection : addresses_with_connections)
        connections.emplace_back(addresses_with_connection.second);

    auto resolution_function = [this](UInt64 proposer, LogEntity entity, String from)
    {
        LOG_DEBUG(&Logger::get("QingCloudDDLSynchronism"), "Paxos execute from " + from);

        {
            std::unique_lock<std::mutex> lock_2{mutex};

            DDLEntity committed = loadCommitted();
            if (proposer > committed.paxos_id && entity.first - committed.entity_id <= 1)
            {
                executeQuery("INSERT INTO system.ddl_queue(id, proposer_id, query_string) VALUES(" + toString(entity.first) + ", " +
                             toString(proposer) + ", '" + entity.second + "')", context);

                committed.local_paxos_id = proposer;
                committed.local_entity_id = entity.first;
                committed.local_ddl_query_string = entity.second;
                storeCommitted(committed);
            }
        }

        cond.notify_one();
        std::unique_lock<std::mutex> lock_1{mutex};
    };

    DDLEntity committed = loadCommitted();
    paxos = std::make_shared<QingCloudPaxos>(node_id, committed.paxos_id, std::pair(committed.entity_id, committed.local_ddl_query_string),
                                             connections, context, resolution_function);
}

bool QingCloudDDLSynchronism::enqueue(const String &query_string, std::function<bool()> quit_state)
{
    while (!quit_state())
    {
        QingCloudPaxos::State res;

        {
            std::unique_lock<std::mutex> lock{mutex};

            DDLEntity committed = loadCommitted();
            res = paxos->sendPrepare(std::pair(committed.paxos_id + 1, query_string));
        }

        switch (res)
        {
            case QingCloudPaxos::FAILURE:
                break;
            case QingCloudPaxos::NEED_LEARN:
            {
                cond.notify_one();
                std::unique_lock<std::mutex> lock{mutex};
                break;
            }
            case QingCloudPaxos::SUCCESSFULLY:
            {
                LOG_DEBUG(&Logger::get("QingCloudDDLSynchronism"), "Query is enqueue queue. now waiting for query run result.");
                return true;
            }
        }
    }

    return false;
}

void QingCloudDDLSynchronism::work()
{
    setThreadName("QingCloudDDLSynchronism");

    std::unique_lock<std::mutex> lock{mutex};
    const auto quit_requested = [this]
    { return quit.load(std::memory_order_relaxed); };

    while (!quit_requested())
    {
        DDLEntity committed = loadCommitted();

        /// Sync other nodes into local system.ddl_queue or ddl_queue has no process ddl
        if (committed.paxos_id < committed.local_paxos_id || fetchOtherDDL(committed.local_paxos_id))
        {
            committed = loadCommitted();

            BlockIO res = executeQuery("SELECT id, proposer_id, query_string FROM system.ddl_queue WHERE id > " +
                                       toString(committed.entity_id) + " ORDER BY id, proposer_id", context);

            paxos->setLastProposer(committed.local_paxos_id, std::pair(committed.local_entity_id, committed.local_ddl_query_string));
            /// TODO: 逐条与committed.paxos_id 对比执行,
            storeCommitted(committed);
            continue;
        }

        cond.wait_for(lock, sleep_time, quit_requested);
    }
}

size_t QingCloudDDLSynchronism::fetchOtherDDL(UInt64 last_committed_id)
{
    /// TODO: maybe use two query and only query max node ?
    BlockInputStreams streams;
    Block header = storage->getSampleBlock();
    Settings settings = context.getSettingsRef();
    String query_string = "SELECT * FROM system.ddl_queue WHERE proposer_id > " + toString(last_committed_id) + " ORDER BY proposer_id LIMIT 10";

    for (size_t index = 0; index < connections.size(); ++index)
    {
        ConnectionPoolPtrs failover_connections;
        failover_connections.emplace_back(connections[index]);
        ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
            failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

        streams.emplace_back(std::make_shared<QingCloudErroneousBlockInputStream>(
            std::make_shared<RemoteBlockInputStream>(shard_pool, query_string, header, context)));
    }

    ParserSelectQuery select_parser;
    String agg_select = "SELECT DISTINCT id, proposer_id, query_string FROM system.ddl_queue WHERE _res_code = 0 ORDER BY proposer_id";
    ASTPtr agg_query_ast = parseQuery(select_parser, agg_select.data(), agg_select.data() + agg_select.size(), "", 0);

    InterpreterSelectQuery interpreter(agg_query_ast, context, std::make_shared<UnionBlockInputStream<>>(streams, streams.size()));
    Block unique_paxos_block = std::make_shared<SquashingBlockInputStream>(interpreter.execute().in, std::numeric_limits<size_t>::max(),
                                                                           std::numeric_limits<size_t>::max())->read();

    BlockOutputStreamPtr out = storage->write({}, settings);
    out->writePrefix(); out->write(unique_paxos_block); out->writeSuffix();
    size_t rows = unique_paxos_block.rows();
    if (rows > 0)
    {
        DDLEntity committed = loadCommitted();
        committed.local_paxos_id = typeid_cast<const ColumnUInt64 &>(*unique_paxos_block.getByName("proposer_id").column).getUInt(rows - 1);
        committed.local_entity_id = typeid_cast<const ColumnUInt64 &>(*unique_paxos_block.getByName("id").column).getUInt(rows - 1);
        committed.local_ddl_query_string = typeid_cast<const ColumnString &>(*unique_paxos_block.getByName("query_string").column).getDataAt(rows - 1).toString();
        storeCommitted(committed);
    }

    return rows;
}

QingCloudDDLSynchronism::DDLEntity QingCloudDDLSynchronism::loadCommitted()
{
    /// TODO: exception like file not exists ?
    DDLEntity entity;

    if (Poco::File(data_path).exists())
    {
        ReadBufferFromFile buffer(data_path);

        readIntText(entity.paxos_id, buffer);
        readIntText(entity.entity_id, buffer);
        readIntText(entity.local_paxos_id, buffer);
        readIntText(entity.local_entity_id, buffer);
        readString(entity.local_ddl_query_string, buffer);
    }

    return entity;
}

void QingCloudDDLSynchronism::storeCommitted(const QingCloudDDLSynchronism::DDLEntity &entity)
{
    WriteBufferFromFile buffer(data_path);

    writeIntText(entity.paxos_id, buffer);
    writeIntText(entity.entity_id, buffer);
    writeIntText(entity.local_paxos_id, buffer);
    writeIntText(entity.local_entity_id, buffer);
    writeString(entity.local_ddl_query_string, buffer);
}

QingCloudDDLSynchronism::~QingCloudDDLSynchronism()
{
    if (!quit)
    {
        {
            quit = true;
            std::lock_guard<std::mutex> lock{mutex};
        }
        cond.notify_one();
        thread.join();
    }
}

}