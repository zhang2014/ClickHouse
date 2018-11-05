#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>
#include <QingCloud/Interpreters/Paxos/QingCloudPaxos.h>

namespace DB
{

class Context;

class QingCloudDDLSynchronism;
using QingCloudDDLSynchronismPtr = std::shared_ptr<QingCloudDDLSynchronism>;

class QingCloudDDLSynchronism
{
    friend class InterpreterPaxosQuery;
private:
    using AddressesWithConnections = std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>>;

    struct DDLEntity
    {
        UInt64 paxos_id = 0;
        UInt64 entity_id = 0;

        UInt64 local_paxos_id = 0;
        UInt64 local_entity_id = 0;
        String local_ddl_query_string = "";
    };

public:
    ~QingCloudDDLSynchronism();

    QingCloudDDLSynchronism(const Context & context, const String & node_id);

    bool enqueue(const String & query_string, std::function<bool()> quit_state);

    void updateAddressesAndConnections(const String & node_id, const AddressesWithConnections & addresses_with_connections);

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

    String data_path;

    void work();

    DDLEntity loadCommitted();

    void storeCommitted(const DDLEntity &entity);

    void learningDDLEntities();

    StoragePtr createDDLQueue(const Context & context);

    Block executeQueryWithConnections(const String & query_string, UInt64 offset = 0, UInt64 limit = 10);

};

}