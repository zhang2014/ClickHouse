#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>
#include <QingCloud/Interpreters/Paxos/PaxosDDLEntity.h>

namespace DB
{


class QingCloudPaxosLearner
{
public:
    QingCloudPaxosLearner(DDLEntity & entity_state, const StoragePtr & state_machine_storage,
                          const ClusterPtr &work_cluster, const Context &context);

    void work();

private:
    const Context & context;
    const StoragePtr & state_machine_storage;
    DDLEntity & entity_state;

    ClusterPtr work_cluster;
    std::chrono::milliseconds sleep_time;
    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> connections;

    std::mutex mutex;
    std::atomic<bool> quit{false};
    std::condition_variable cond;
    String self_address;
    std::thread thread = std::thread{&QingCloudPaxosLearner::work, this};

    void learning();

    void applyDDLQueries();

    Block queryWithTwoLevel(const String &first_query, const String &second_query);

    Block sendQueryToPaxosProxy(const String & send_query, const String & from);
};

}
