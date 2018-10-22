#include <QingCloud/Interpreters/QingCloudDDLSynchronism.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Functions/FunctionsComparison.h>
#include <QingCloud/Interpreters/Paxos/QingCloudPaxos.h>
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


namespace DB
{

static Block getBlockWithAllStreamData(const BlockInputStreamPtr &stream)
{
    auto res = std::make_shared<SquashingBlockInputStream>(stream, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max());
    return res->read();
}

QingCloudDDLSynchronism::QingCloudDDLSynchronism(const Context & context) : context(context)
{
    /// TODO sleep_time
    sleep_time = std::chrono::milliseconds(5000);
    const auto multiplexed_version = context.getMultiplexedVersion();
    storage = createDDLQueue(context);
    updateAddressesAndConnections(multiplexed_version->getAddressesAndConnections());
    thread = std::thread{&QingCloudDDLSynchronism::work, this};
}

StoragePtr QingCloudDDLSynchronism::createDDLQueue(const Context & context) const
{
    StoragePtr storage = context.tryGetTable("system", "ddl_queue");
    if (!storage)
    {
        ParserCreateQuery create_parser;
        String create_query_string = "CREATE TABLE system.ddl_queue(id UInt64, proposer_id UInt64, query_string String) ENGINE = MergeTree PARTITION BY proposer_id / 300000 ORDER BY (id, proposer_id)";
        ASTPtr create_query = parseQuery(create_parser, create_query_string.data(), create_query_string.data() + create_query_string.size(),
                                         "Storage to create table for QingCloudAsynchronism", 0);

        InterpreterCreateQuery interpreter(create_query, const_cast<Context &>(context));
        interpreter.setInternal(true);
        interpreter.execute();
        storage = context.getTable("system", "ddl_queue");
    }
    return storage;
}

void QingCloudDDLSynchronism::updateAddressesAndConnections(const AddressesWithConnections & addresses_with_connections)
{
    std::unique_lock<std::mutex> lock{mutex};

    String node_id;
    connections.resize(addresses_with_connections.size());
    for (auto & addresses_with_connection : addresses_with_connections)
    {
        if (addresses_with_connection.first.is_local)
            node_id = addresses_with_connection.first.toString();
        connections.emplace_back(addresses_with_connection.second);
    }

    auto resolution_function = [this](UInt64 proposer, LogEntity entity, String from)
    {
        LOG_DEBUG(&Logger::get("QingCloudDDLSynchronism"), "Paxos execute from " + from);
        executeAndPersistenceQuery(entity.first, proposer, entity.second);
    };
    const auto last_query = getLastQuery(context);
    paxos = std::make_shared<QingCloudPaxos>(node_id, last_query.first, last_query.second, connections, context, resolution_function);
}

void QingCloudDDLSynchronism::enqueue(const String & query_string, const Context & context)
{
    std::unique_lock<std::mutex> lock{mutex};

    while (true)
    {
        auto last_query_id = getLastQuery(context);
        if (paxos->sendPrepare(std::pair(last_query_id.first + 1, query_string)))
        {
            LOG_DEBUG(&Logger::get("QingCloudDDLSynchronism"), "Query is enqueue queue. now waiting for query run result.");
            /// TODO: 等待语句执行结果
            return;
        }
    }
}

void QingCloudDDLSynchronism::work()
{
    setThreadName("QingCloudDDLSynchronism");

    std::unique_lock<std::mutex> lock{mutex};

    const auto quit_requested = [this] { return quit.load(std::memory_order_relaxed); };

    while (!quit_requested())
    {
        auto query_id = getLastQuery(context);
        /// TODO: others server addresses.
        String sync_other_servers_query = "SELECT id, query_string FROM remote(, system.ddl_queue) WHERE id > "
                                          + toString(query_id) + " GROUP BY id, query_string";

        ParserSelectQuery select_parser;
        ASTPtr select_query = parseQuery(select_parser, sync_other_servers_query.data(),
                                         sync_other_servers_query.data() + sync_other_servers_query.size(), "QingCloudDDLSynchronism", 0);

        InterpreterSelectQuery interpreter(select_query, context);
        Block sync_ddl_queue = getBlockWithAllStreamData(interpreter.execute().in);

        for (size_t row = 0; row < sync_ddl_queue.rows(); ++ row)
        {
            UInt64 id = typeid_cast<const ColumnUInt64 &>(*sync_ddl_queue.safeGetByPosition(0).column).getUInt(row);
            UInt64 proposer_id = typeid_cast<const ColumnUInt64 &>(*sync_ddl_queue.safeGetByPosition(1).column).getUInt(row);
            String query_string = typeid_cast<const ColumnString &>(*sync_ddl_queue.safeGetByPosition(2).column).getDataAt(row).toString();

            executeAndPersistenceQuery(id, proposer_id, query_string);
        }


        cond.wait_for(lock, sleep_time, quit_requested);
    }
}

std::pair<UInt64, LogEntity> QingCloudDDLSynchronism::getLastQuery(const Context &context)
{
    String query_for_max_query_id = "SELECT id, proposer_id, query_string FROM system.ddl_queue WHERE id = SELECT MAX(id) FROM system.ddl_queue;";

    ParserSelectQuery select_parser;
    ASTPtr select_query = parseQuery(select_parser, query_for_max_query_id.data(),
                                     query_for_max_query_id.data() + query_for_max_query_id.size(),
                                     "Query Last Query id for QingCloudDDLSynchronism", 1);

    InterpreterSelectQuery interpreter(select_query, context);
    Block query_block = getBlockWithAllStreamData(interpreter.execute().in);
    UInt64 id = typeid_cast<const ColumnUInt64 &>(*query_block.safeGetByPosition(0).column).getUInt(0);
    UInt64 proposer_id = typeid_cast<const ColumnUInt64 &>(*query_block.safeGetByPosition(1).column).getUInt(0);
    String query_string = typeid_cast<const ColumnString &>(*query_block.safeGetByPosition(2).column).getDataAt(0).toString();
    return std::pair(proposer_id, std::pair(id, query_string));
}

void QingCloudDDLSynchronism::executeAndPersistenceQuery(const UInt64 &id, const UInt64 & proposer_id, const String &query_string)
{
    String query_to_execute = "/* ddl_entry=" + toString(id) + " */ " + query_string;

    Context local_context(context);
    local_context.setCurrentQueryId(""); // generate random query_id
    local_context.getSettingsRef().internal_query = true;
    /// TODO: exception for execute need callback
    BlockIO res = executeQuery(query_to_execute, local_context);

    Block data = storage->getSampleBlock().cloneEmpty();
    for (size_t index = 0; index < data.columns(); ++index)
    {
        if (data.safeGetByPosition(index).name == "id")
            data.safeGetByPosition(index).column->insert(Field(id));
        else if (data.safeGetByPosition(index).name == "proposer_id")
            data.safeGetByPosition(index).column->insert(Field(proposer_id));
        else if (data.safeGetByPosition(index).name == "query_string")
            data.safeGetByPosition(index).column->insert(query_to_execute);
    }

    BlockIO res_for_insert = executeQuery("INSERT INTO system.ddl_queue VALUES", local_context);
    res_for_insert.out->writePrefix();
    res_for_insert.out->write(Block{{}});
    res_for_insert.out->writeSuffix();
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