#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>
#include "QingCloudPaxosLearner.h"
#include "QingCloudPaxos.h"

namespace DB
{

class Context;

using LogEntity = std::pair<UInt64, String>;

class QingCloudDDLSynchronism;
using QingCloudDDLSynchronismPtr = std::shared_ptr<QingCloudDDLSynchronism>;

class QingCloudDDLSynchronism
{
public:

    struct DDLEntity
    {
        std::mutex mutex;
        UInt64 applied_paxos_id = 0;
        UInt64 applied_entity_id = 0;

        UInt64 accepted_paxos_id = 0;
        UInt64 accepted_entity_id = 0;
        String accepted_entity_value = "";

        DDLEntity(const String & data_path);

        void store();

    private:
        const String dir;
        const String data_path;
    };

    ~QingCloudDDLSynchronism();

    QingCloudDDLSynchronism(const Context & context, const String & node_id);

    bool enqueue(const String & query_string, std::function<bool()> quit_state);

    Block receivePrepare(const UInt64 & prepare_paxos_id);

    Block acceptProposal(const String &from, const UInt64 & prepare_paxos_id, const LogEntity & value);

    Block acceptedProposal(const String &from, const UInt64 & accepted_paxos_id, const LogEntity & accepted_entity);

private:
    std::mutex mutex;
    StoragePtr state_machine_storage;
    std::shared_ptr<QingCloudPaxos> paxos;
    std::shared_ptr<QingCloudPaxosLearner> learner;

    const Context & context;
    DDLEntity entity;

    StoragePtr createDDLQueue(const Context & context);

};

}