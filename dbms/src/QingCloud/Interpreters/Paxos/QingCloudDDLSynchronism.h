#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>
#include <mutex>
#include <condition_variable>
#include <QingCloud/Interpreters/Paxos/PaxosDDLEntity.h>
#include <QingCloud/Interpreters/Paxos/QingCloudPaxos.h>
#include <QingCloud/Interpreters/Paxos/QingCloudPaxosLearner.h>

namespace DB
{

class Context;
class QingCloudDDLSynchronism;
using QingCloudDDLSynchronismPtr = std::shared_ptr<QingCloudDDLSynchronism>;

class QingCloudDDLSynchronism
{
public:

    struct WaitApplyRes
    {
    public:
        std::mutex mutex;
        std::condition_variable cond;
        size_t current_cluster_node_size;
        std::vector<std::tuple<UInt64, String, String>> paxos_res;

        WaitApplyRes(size_t current_cluster_node_size_) : current_cluster_node_size(current_cluster_node_size_)
        {}

        void wait(const std::chrono::duration<long long int, std::milli> &timeout);

        void notify_one(const UInt64 & entity_id, const String & exception_message, const String & from);
    };

    using WaitApplyResPtr = std::shared_ptr<WaitApplyRes>;

    ~QingCloudDDLSynchronism();

    QingCloudDDLSynchronism(const Context & context, const String & node_id);

    void wakeupLearner();

    RWLockFIFO::LockHandler lock();

    void upgradeVersion(const String & origin_version, const String & upgrade_version);

    UInt64 enqueue(const String & query_string, std::function<bool()> quit_state);

    Block receivePrepare(const UInt64 & prepare_paxos_id);

    void releaseApplyRes(const UInt64 & entity_id);

    WaitApplyResPtr getWaitApplyRes(const UInt64 & entity_id, bool must_exists = true);

    Block acceptProposal(const String &from, const UInt64 & prepare_paxos_id, const LogEntity & value);

    Block acceptedProposal(const String &from, const String & origin_from, const UInt64 & accepted_paxos_id, const LogEntity & accepted_entity);

private:
    StoragePtr state_machine_storage;
    std::shared_ptr<QingCloudPaxos> paxos;
    std::shared_ptr<QingCloudPaxosLearner> learner;

    const Context & context;
    DDLEntity entity;
    size_t current_cluster_node_size;

    RWLockFIFOPtr work_lock = RWLockFIFO::create();
    std::map<UInt64, std::shared_ptr<WaitApplyRes>> wait_apply_res;

    StoragePtr createDDLQueue(const Context & context);

};

}