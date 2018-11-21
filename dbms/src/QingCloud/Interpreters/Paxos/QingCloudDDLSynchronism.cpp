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
#include "QingCloudDDLSynchronism.h"


namespace DB
{

namespace ErrorCodes
{
extern const int PAXOS_EXCEPTION;
}

static Block executeLocalQuery(const String &query_string, const Context &context)
{
    Context query_context(context);
    query_context.getSettingsRef().internal_query = true;

    ParserQuery query_parser(query_string.data() + query_string.size());
    ASTPtr query = parseQuery(query_parser, query_string.data(), query_string.data() + query_string.size(), "", 0);
    std::unique_ptr<IInterpreter> interpreter = InterpreterFactory::get(query, const_cast<Context &>(context));
    if (BlockInputStreamPtr input = interpreter->execute().in)
    {
        return std::make_shared<SquashingBlockInputStream>(input, std::numeric_limits<size_t>::max(),
                                                           std::numeric_limits<size_t>::max())->read();
    }
    return {};
}


QingCloudDDLSynchronism::QingCloudDDLSynchronism(const Context &context, const String &node_id) : context(context)
{
    /// TODO sleep_time
    storage = createDDLQueue(context);
    sleep_time = std::chrono::milliseconds(5000);
    const auto multiplexed_version = context.getMultiplexedVersion();
    data_path = context.getConfigRef().getString("path") + "/" + "paxos.info";
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
        executeLocalQuery(create_query + storage_declare, context);
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
        new std::thread([this, proposer, entity, from]()
        {
            {
                std::unique_lock<std::mutex> lock_2{mutex};

                DDLEntity committed = loadCommitted();
                if (proposer > committed.paxos_id && entity.first - committed.entity_id <= 1)
                {
                    Block header = storage->getSampleBlock();

                    MutableColumns mutable_columns = header.mutateColumns();
                    mutable_columns[0]->insert(entity.first);
                    mutable_columns[1]->insert(proposer);
                    mutable_columns[2]->insert(entity.second);
                    header.setColumns(std::move(mutable_columns));

                    BlockOutputStreamPtr output = storage->write({}, context.getSettingsRef());
                    output->writePrefix(); output->write(header); output->writeSuffix();

                    committed.local_paxos_id = proposer;
                    committed.local_entity_id = entity.first;
                    committed.local_ddl_query_string = entity.second;
                    storeCommitted(committed);
                    paxos->setLastProposer(committed.local_paxos_id, std::pair(committed.local_entity_id, committed.local_ddl_query_string));
                }
            }

            cond.notify_one();
            std::unique_lock<std::mutex> lock_1{mutex};
        });
    };

    DDLEntity committed = loadCommitted();
    paxos = std::make_shared<QingCloudPaxos>(node_id, committed.paxos_id, std::pair(committed.entity_id, committed.local_ddl_query_string),
                                             connections, context, resolution_function);
}

bool QingCloudDDLSynchronism::enqueue(const String &query_string, std::function<bool()> quit_state)
{
    while (!quit_state())
    {
        std::unique_lock<std::mutex> lock{mutex};

        DDLEntity committed = loadCommitted();
        switch(paxos->sendPrepare(std::pair(committed.entity_id + 1, query_string)))
        {
            case QingCloudPaxos::FAILURE: break;
            case QingCloudPaxos::NEED_LEARN: learningDDLEntities(); break;
            case QingCloudPaxos::SUCCESSFULLY: return true;
        }
    }

    return false;
}

void QingCloudDDLSynchronism::work()
{
    setThreadName("QingCloudDDLSynchronism");

    std::unique_lock<std::mutex> lock{mutex};

    CurrentThread::initializeQuery();
    const auto quit_requested = [this] { return quit.load(std::memory_order_relaxed); };

    while (!quit_requested())
    {
        try
        {
            /// Update paxos status to the latest.
            learningDDLEntities();

            /// fetch quorum ddl queries & execute it.
            DDLEntity committed = loadCommitted();

            String columns = "id, proposer_id, query_string";
            String filter_exception = "_res_code = 0";
            String ddl_quorum = "count() = " + toString(connections.size()) + " OR count() >= " + toString(connections.size() / 2 + 1);

            String query_quorum_ddl_queries = "SELECT " + columns + " FROM system.ddl_queue WHERE " + filter_exception + " GROUP BY " +
                                              columns + " HAVING " + ddl_quorum + " ORDER BY id, proposer_id";

            Block ddl_queue = executeQueryWithConnections(query_quorum_ddl_queries, committed.paxos_id, 10);

            for (size_t index = 0; index < ddl_queue.rows(); ++index)
            {
                UInt64 entity_id = typeid_cast<const ColumnUInt64 &>(*ddl_queue.getByPosition(0).column).getUInt(index);
                UInt64 proposer_id = typeid_cast<const ColumnUInt64 &>(*ddl_queue.getByPosition(1).column).getUInt(index);
                String query_string = typeid_cast<const ColumnString &>(*ddl_queue.getByPosition(2).column).getDataAt(index).toString();

                if (entity_id - committed.entity_id > 1)
                {
                    LOG_WARNING(&Logger::get("QingCloudDDLSynchronism"), "Cannot execute ddl query, because ddl not continuous");
                    break;
                }

                /// TODO: 执行结果返回给 from
                executeLocalQuery(query_string, context);
                committed.entity_id = entity_id;
                committed.paxos_id = proposer_id;
                storeCommitted(committed);
            }
        }
        catch (...)
        {
            tryLogCurrentException(&Logger::get("QingCloudDDLSynchronism"));
        }

        cond.wait_for(lock, sleep_time, quit_requested);
    }
}

void QingCloudDDLSynchronism::learningDDLEntities()
{
    DDLEntity committed = loadCommitted();

    String fetch_query = "SELECT DISTINCT id, proposer_id, query_string FROM system.ddl_queue WHERE _res_code = 0 ORDER BY proposer_id";
    Block unique_paxos_block = executeQueryWithConnections(fetch_query, committed.local_paxos_id, 10);
    
    if (size_t rows = unique_paxos_block.rows())
    {
        Settings query_settings = context.getSettingsRef();
        BlockOutputStreamPtr out = storage->write({}, query_settings);
        out->writePrefix(); out->write(unique_paxos_block); out->writeSuffix();

        committed.local_paxos_id = typeid_cast<const ColumnUInt64 &>(*unique_paxos_block.getByName("proposer_id").column).getUInt(rows - 1);
        committed.local_entity_id = typeid_cast<const ColumnUInt64 &>(*unique_paxos_block.getByName("id").column).getUInt(rows - 1);
        committed.local_ddl_query_string = typeid_cast<const ColumnString &>(*unique_paxos_block.getByName("query_string").column).getDataAt(rows - 1).toString();
        paxos->setLastProposer(committed.local_paxos_id, std::pair(committed.local_entity_id, committed.local_ddl_query_string));
        storeCommitted(committed);
    }
}

QingCloudDDLSynchronism::DDLEntity QingCloudDDLSynchronism::loadCommitted()
{
    DDLEntity entity;
    if (Poco::File(data_path).exists())
    {
        ReadBufferFromFile buffer(data_path);

        String ignore;
        readStringBinary(ignore, buffer);
        readIntText(entity.paxos_id, buffer);
        readStringBinary(ignore, buffer);
        readIntText(entity.entity_id, buffer);
        readStringBinary(ignore, buffer);
        readIntText(entity.local_paxos_id, buffer);
        readStringBinary(ignore, buffer);
        readIntText(entity.local_entity_id, buffer);
        readStringBinary(ignore, buffer);
        readStringBinary(entity.local_ddl_query_string, buffer);

        LOG_DEBUG(&Logger::get("QingCloudDDLSynchronism"), "loadCommitted " + toString(entity.local_paxos_id));
    }
    return entity;
}

void QingCloudDDLSynchronism::storeCommitted(const QingCloudDDLSynchronism::DDLEntity & entity)
{
    Poco::File{data_path}.createFile();

    {
        WriteBufferFromFile buffer(data_path);

        writeStringBinary("Paxos_id:", buffer);
        writeIntText(entity.paxos_id, buffer);
        writeStringBinary("\nEntity_id:", buffer);
        writeIntText(entity.entity_id, buffer);
        writeStringBinary("\nLocal_Paxos_id:", buffer);
        writeIntText(entity.local_paxos_id, buffer);
        writeStringBinary("\nLocal_Entity_id:", buffer);
        writeIntText(entity.local_entity_id, buffer);
        writeStringBinary("\nLocal_DDL_QUERY_String:", buffer);
        writeStringBinary(entity.local_ddl_query_string, buffer);

        buffer.sync();
    }
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

Block QingCloudDDLSynchronism::executeQueryWithConnections(const String & query_string, UInt64 offset, UInt64 limit)
{
    BlockInputStreams streams;
    Block header = storage->getSampleBlock();
    Settings settings = context.getSettingsRef();

    String inner_query = "SELECT * FROM system.ddl_queue WHERE proposer_id > " + toString(offset) + " ORDER BY proposer_id LIMIT " + toString(limit);

    for (size_t index = 0; index < connections.size(); ++index)
    {
        ConnectionPoolPtrs failover_connections;
        failover_connections.emplace_back(connections[index]);
        ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
            failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

        streams.emplace_back(std::make_shared<QingCloudErroneousBlockInputStream>(std::make_shared<RemoteBlockInputStream>(shard_pool, inner_query, header, context)));
    }

    ParserSelectQuery select_parser;
    ASTPtr node = parseQuery(select_parser, query_string.data(), query_string.data() + query_string.size(), "", 0);
    InterpreterSelectQuery interpreter(node, context, std::make_shared<UnionBlockInputStream<>>(streams, nullptr, streams.size()));

    if (BlockInputStreamPtr input = interpreter.execute().in)
        return std::make_shared<SquashingBlockInputStream>(interpreter.execute().in, std::numeric_limits<size_t>::max(),
                                                           std::numeric_limits<size_t>::max())->read();

    throw Exception("Cannot fetch query input stream", ErrorCodes::LOGICAL_ERROR);
}

}