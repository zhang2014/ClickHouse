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
#include <IO/WriteHelpers.h>
#include "QingCloudDDLSynchronism.h"
#include "QingCloudPaxosLearner.h"
#include "QingCloudPaxos.h"


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
        return std::make_shared<SquashingBlockInputStream>(input, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())->read();
    return {};
}

QingCloudDDLSynchronism::QingCloudDDLSynchronism(const Context & context, const String & /*node_id*/) :
    context(context), entity(context.getConfigRef().getString("path"))
{
    state_machine_storage = createDDLQueue(context);
    const String default_version = context.getMultiplexedVersion()->getCurrentWritingVersion();

    const ClusterPtr work_cluster = context.getCluster("Cluster_" + default_version);
    paxos = std::make_shared<QingCloudPaxos>(entity, work_cluster, context, state_machine_storage);
    learner = std::make_shared<QingCloudPaxosLearner>(entity, state_machine_storage, work_cluster, context);
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

bool QingCloudDDLSynchronism::enqueue(const String & query_string, std::function<bool()> quit_state)
{
    while (!quit_state())
    {
        std::unique_lock<std::mutex> lock{mutex};
//        learner->weakup();
        std::lock_guard<std::mutex> entity_lock(entity.mutex);
        if (paxos->sendPrepare(std::pair(entity.applied_entity_id + 1, query_string)) == QingCloudPaxos::SUCCESSFULLY)
            return true;
    }

    return false;
}

Block QingCloudDDLSynchronism::receivePrepare(const UInt64 & prepare_paxos_id)
{
    return paxos->receivePrepare(prepare_paxos_id);
}

Block QingCloudDDLSynchronism::acceptProposal(const String &from, const UInt64 &prepare_paxos_id, const LogEntity &value)
{
    return paxos->acceptProposal(from, prepare_paxos_id, value);
}

Block QingCloudDDLSynchronism::acceptedProposal(const String &from, const UInt64 &accepted_paxos_id, const LogEntity &accepted_entity)
{
    return paxos->acceptedProposal(from, accepted_paxos_id, accepted_entity);
}

QingCloudDDLSynchronism::~QingCloudDDLSynchronism() = default;

QingCloudDDLSynchronism::DDLEntity::DDLEntity(const String & data_path_)
        :dir(data_path_), data_path(data_path_ + "paxos.info")
{
    if (Poco::File(data_path).exists())
    {
        ReadBufferFromFile buffer(data_path);
        readBinary(applied_paxos_id, buffer);
        readBinary(applied_entity_id, buffer);
        readBinary(accepted_paxos_id, buffer);
        readBinary(accepted_entity_id, buffer);
        readStringBinary(accepted_entity_value, buffer);
    }

    store();
}

void QingCloudDDLSynchronism::DDLEntity::store()
{
    Poco::File(dir).createDirectories();
    WriteBufferFromFile buffer(data_path);
    writeBinary(applied_paxos_id, buffer);
    writeBinary(applied_entity_id, buffer);
    writeBinary(accepted_paxos_id, buffer);
    writeBinary(accepted_entity_id, buffer);
    writeStringBinary(accepted_entity_value, buffer);
    buffer.sync();
}

}