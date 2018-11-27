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
#include <QingCloud/Common/differentClusters.h>


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
    const String default_version = context.getMultiplexedVersion()->getCurrentVersion();

    const ClusterPtr work_cluster = context.getCluster("Cluster_" + default_version);

    paxos = std::make_shared<QingCloudPaxos>(entity, work_cluster, context, state_machine_storage);
    learner = std::make_shared<QingCloudPaxosLearner>(entity, state_machine_storage, work_cluster, context);
    current_cluster_node_size = getConnectionPoolsFromClusters({work_cluster}).size();
}

StoragePtr QingCloudDDLSynchronism::createDDLQueue(const Context & context)
{
    StoragePtr storage = context.tryGetTable("system", "ddl_queue");
    if (!storage)
    {
        String create_query = "CREATE TABLE system.ddl_queue(id UInt64, proposer_id UInt64, query_string String, from String)";
        String storage_declare = "ENGINE = MergeTree PARTITION BY proposer_id / 300000 ORDER BY (id, proposer_id)";
        executeLocalQuery(create_query + storage_declare, context);
        storage = context.getTable("system", "ddl_queue");
    }
    return storage;
}

UInt64 QingCloudDDLSynchronism::enqueue(const String & query_string, std::function<bool()> quit_state)
{
    std::unique_lock<std::mutex> lock(mutex);

    auto next_entity_id = [&, this] ()
    {
        entity.learning_cond.notify_one();
        std::lock_guard<std::recursive_mutex> entity_lock(entity.mutex);
        return entity.accepted_entity_id + 1;
    };

    while (!quit_state())
    {
        UInt64 entity_id = next_entity_id();

        {
            std::lock_guard<std::mutex> wait_lock(wait_apply_res_mutex);
            wait_apply_res[entity_id] = std::make_shared<WaitApplyRes>(current_cluster_node_size);
        }

        if (paxos->sendPrepare(std::pair(entity_id, query_string)) == QingCloudPaxos::SUCCESSFULLY)
            return entity_id;

        {
            std::lock_guard<std::mutex> wait_lock(wait_apply_res_mutex);
            wait_apply_res.erase(entity_id);
        }
    }

    return 0;
}

Block QingCloudDDLSynchronism::receivePrepare(const UInt64 & prepare_paxos_id)
{
    std::unique_lock<std::mutex> lock(prepare_mutex);
    return paxos->receivePrepare(prepare_paxos_id);
}

Block QingCloudDDLSynchronism::acceptProposal(const String &from, const UInt64 &prepare_paxos_id, const LogEntity &value)
{
    std::unique_lock<std::mutex> lock(acceptor_mutex);
    return paxos->acceptProposal(from, prepare_paxos_id, value);
}

Block QingCloudDDLSynchronism::acceptedProposal(const String &from, const String & origin_from, const UInt64 &accepted_paxos_id, const LogEntity & accepted_entity)
{
    std::unique_lock<std::mutex> lock(final_acceptor_mutex);
    return paxos->acceptedProposal(from, origin_from, accepted_paxos_id, accepted_entity);
}

void QingCloudDDLSynchronism::upgradeVersion(const String & /*origin_version*/, const String & upgrade_version)
{
    const ClusterPtr work_cluster = context.getCluster("Cluster_" + upgrade_version);
    paxos = std::make_shared<QingCloudPaxos>(entity, work_cluster, context, state_machine_storage);
    learner = std::make_shared<QingCloudPaxosLearner>(entity, state_machine_storage, work_cluster, context);
    current_cluster_node_size = getConnectionPoolsFromClusters({work_cluster}).size();
    std::unique_lock<std::recursive_mutex> lock(entity.mutex);
}

void QingCloudDDLSynchronism::wakeupLearner()
{
    entity.learning_cond.notify_one();
    std::lock_guard<std::recursive_mutex> entity_lock(entity.mutex);
}

void QingCloudDDLSynchronism::withLockPaxos(std::function<void()> fun_with_lock)
{
    std::unique_lock<std::mutex> lock(mutex);
    std::unique_lock<std::mutex> prepare_lock(prepare_mutex);
    std::unique_lock<std::mutex> acceptor_lock(acceptor_mutex);
    std::unique_lock<std::mutex> final_acceptor_lock(final_acceptor_mutex);

    for (size_t retries = 0; true; ++retries)
    {
        std::unique_lock<std::mutex> wait_apply_lock(wait_apply_res_mutex);

        if (!wait_apply_res.empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        else
            break;
    }

    fun_with_lock();
}

void QingCloudDDLSynchronism::WaitApplyRes::wait(const std::chrono::duration<long long int, std::milli> &timeout)
{
    std::unique_lock<std::mutex> lock(mutex);

    while (cond.wait_for(lock, timeout) != std::cv_status::timeout)
    {
        if (paxos_res.size() == current_cluster_node_size)
            return;
    }
}

void QingCloudDDLSynchronism::WaitApplyRes::notify_one(const UInt64 & res_state, const String & exception_message, const String & from)
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        paxos_res.emplace_back(std::tuple(res_state, exception_message, from));
    }

    cond.notify_one();
}

QingCloudDDLSynchronism::WaitApplyResPtr QingCloudDDLSynchronism::getWaitApplyRes(const UInt64 & entity_id, bool must_exists)
{
    std::lock_guard<std::mutex> entity_lock(wait_apply_res_mutex);

    if (!wait_apply_res.count(entity_id) && must_exists)
        throw Exception("LOGICAL ERROR: cannot wait for ddl query result, because " + toString(entity_id) + " owner is not self.",
                        ErrorCodes::LOGICAL_ERROR);

    return wait_apply_res.count(entity_id) ? wait_apply_res[entity_id] : std::make_shared<WaitApplyRes>(current_cluster_node_size);
}

void QingCloudDDLSynchronism::releaseApplyRes(const UInt64 & entity_id)
{
    std::lock_guard<std::mutex> entity_lock(wait_apply_res_mutex);

    if (!wait_apply_res.count(entity_id))
        throw Exception("LOGICAL ERROR: cannot wait for ddl query result, because " + toString(entity_id) + " owner is not self.",
                        ErrorCodes::LOGICAL_ERROR);

    wait_apply_res.erase(entity_id);
}

QingCloudDDLSynchronism::~QingCloudDDLSynchronism() = default;

}