#pragma once

#include <Interpreters/Cluster.h>
#include "QingCloudDDLSynchronism.h"

namespace DB
{

class QingCloudPaxosLearner
{
public:
    QingCloudPaxosLearner(QingCloudDDLSynchronism::DDLEntity &entity_state, const StoragePtr &state_machine_storage,
                          const ClusterPtr &work_cluster, const Context &context);

    void work();

private:
    const Context & context;
    const StoragePtr & state_machine_storage;
    QingCloudDDLSynchronism::DDLEntity & entity_state;

    ClusterPtr work_cluster;
    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> connections;

    std::mutex mutex;
    std::atomic<bool> quit{false};
    std::condition_variable cond;
    std::chrono::milliseconds sleep_time;
    std::thread thread = std::thread{&QingCloudPaxosLearner::work, this};

    size_t learning();

    size_t applyDDLQueries();

    Block queryWithTwoLevel(const String &first_query, const String &second_query);
};

}
