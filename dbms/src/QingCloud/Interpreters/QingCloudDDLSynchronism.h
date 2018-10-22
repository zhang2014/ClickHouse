#pragma once

#include <Poco/File.h>
#include <Interpreters/Cluster.h>
#include <Storages/StorageMergeTree.h>
#include <QingCloud/Interpreters/Paxos/QingCloudPaxos.h>

namespace DB
{

class Context;

class QingCloudDDLSynchronism
{
private:
    using AddressesWithConnections = std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>>;
public:
    QingCloudDDLSynchronism(const Context & context);

    ~QingCloudDDLSynchronism();

    void updateAddressesAndConnections(const AddressesWithConnections & addresses_with_connections);

    void enqueue(const String & query_string, const Context & context);

    std::pair<UInt64, LogEntity> getLastQuery(const Context &context);
private:
    std::mutex mutex;
    StoragePtr storage;
    QingCloudPaxosPtr paxos;
    std::vector<ConnectionPoolPtr> connections;

    std::thread thread;
    const Context & context;

    std::atomic<bool> quit {false};
    std::condition_variable cond;
    std::chrono::milliseconds sleep_time;

    void work();

    StoragePtr createDDLQueue(const Context & context) const;

    void executeAndPersistenceQuery(const UInt64 &id, const UInt64 &proposer_id, const String &query_string);
};

using QingCloudDDLSynchronismPtr = std::shared_ptr<QingCloudDDLSynchronism>;

}