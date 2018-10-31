#include <QingCloud/Interpreters/QingCloudDDLSynchronism.h>
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


namespace DB
{

static Block getBlockWithAllStreamData(const BlockInputStreamPtr &stream)
{
    auto res = std::make_shared<SquashingBlockInputStream>(stream, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max());
    return res->read();
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

QingCloudDDLSynchronism::QingCloudDDLSynchronism(const Context & context)
    : context(context)
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
        String create_query = "CREATE TABLE system.ddl_queue(id UInt64, proposer_id UInt64, query_string String)";
        String storage_declare = "ENGINE = MergeTree PARTITION BY proposer_id / 300000 ORDER BY (id, proposer_id)";
        executeQuery(create_query + storage_declare, context);
        storage = context.getTable("system", "ddl_queue");
    }
    return storage;
}

void QingCloudDDLSynchronism::updateAddressesAndConnections(const AddressesWithConnections & addresses_with_connections)
{
    std::unique_lock<std::mutex> lock{mutex};

    String node_id;

    connections.clear();
    connections.reserve(addresses_with_connections.size());
    for (auto & addresses_with_connection : addresses_with_connections)
    {
        if (addresses_with_connection.first.is_local)
            node_id = addresses_with_connection.first.toString();
        connections.emplace_back(addresses_with_connection.second);
        Cluster::Address address = addresses_with_connection.first;
        if (!remotes_addresses.empty())
            remotes_addresses += ",";
        remotes_addresses += address.host_name + ":" + toString(address.port);;
    }

    auto resolution_function = [this](UInt64 proposer, LogEntity entity, String from)
    {
        LOG_DEBUG(&Logger::get("QingCloudDDLSynchronism"), "Paxos execute from " + from);
        processQuery(entity.first, proposer, entity.second);
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

void QingCloudDDLSynchronism::processQuery(const UInt64 &id, const UInt64 & proposer_id, const String & query_string)
{
    /// TODO: exception for execute need callback
    BlockIO res = executeQuery("/* ddl_entry=" + toString(id) + " */ " + query_string, context);

    Block header = storage->getSampleBlockForColumns({"id", "proposer_id", "query_string"});

    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(id);
    columns[1]->insert(proposer_id);
    columns[2]->insert(query_string);

    BlockIO res_for_insert = executeQuery("INSERT INTO system.ddl_queue VALUES", context);
    res_for_insert.out->writePrefix();
    res_for_insert.out->write(header.cloneWithColumns(std::move(columns)));
    res_for_insert.out->writeSuffix();
}

void QingCloudDDLSynchronism::work()
{
    setThreadName("QingCloudDDLSynchronism");

    std::unique_lock<std::mutex> lock{mutex};

    const auto quit_requested = [this] { return quit.load(std::memory_order_relaxed); };

    while (!quit_requested())
    {
        try
        {
            auto last_query = getLastQuery(context);
            BlockIO res = executeQuery("SELECT DISTINCT id, proposer_id, query_string FROM remote('" + remotes_addresses +
                                       "', system, ddl_queue) WHERE proposer_id > " + toString(last_query.first) + " ORDER BY id;", context);

            Block sync_ddl_queue = getBlockWithAllStreamData(res.in);

            for (size_t row = 0; row < sync_ddl_queue.rows(); ++ row)
            {
                UInt64 id = typeid_cast<const ColumnUInt64 &>(*sync_ddl_queue.safeGetByPosition(0).column).getUInt(row);
                UInt64 proposer_id = typeid_cast<const ColumnUInt64 &>(*sync_ddl_queue.safeGetByPosition(1).column).getUInt(row);
                String query_string = typeid_cast<const ColumnString &>(*sync_ddl_queue.safeGetByPosition(2).column).getDataAt(row).toString();

                processQuery(id, proposer_id, query_string);
            }
        }
        catch (...)
        {
            tryLogCurrentException(&Logger::get("QingCloudDDLSynchronism"), "QingCloudDDLSynchronism exception :");
        }

        cond.wait_for(lock, sleep_time, quit_requested);
    }
}

std::pair<UInt64, LogEntity> QingCloudDDLSynchronism::getLastQuery(const Context & context)
{
    BlockIO res = executeQuery("SELECT id, proposer_id, query_string FROM system.ddl_queue WHERE id = (SELECT MAX(id) FROM system.ddl_queue);", context);

    Block query_block = getBlockWithAllStreamData(res.in);

    if (!query_block)
        return std::pair(0, std::pair(0, ""));

    UInt64 id = typeid_cast<const ColumnUInt64 &>(*query_block.safeGetByPosition(0).column).getUInt(0);
    UInt64 proposer_id = typeid_cast<const ColumnUInt64 &>(*query_block.safeGetByPosition(1).column).getUInt(0);
    String query_string = typeid_cast<const ColumnString &>(*query_block.safeGetByPosition(2).column).getDataAt(0).toString();
    return std::pair(proposer_id, std::pair(id, query_string));
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