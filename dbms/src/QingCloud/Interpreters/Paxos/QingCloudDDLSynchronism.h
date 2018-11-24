#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>
#include <QingCloud/Interpreters/Paxos/PaxosDDLEntity.h>
#include <QingCloud/Interpreters/Paxos/QingCloudPaxos.h>
#include <QingCloud/Interpreters/Paxos/QingCloudPaxosLearner.h>

namespace DB
{

class Context;

using LogEntity = std::pair<UInt64, String>;

class QingCloudDDLSynchronism;
using QingCloudDDLSynchronismPtr = std::shared_ptr<QingCloudDDLSynchronism>;

class QingCloudDDLSynchronism
{
public:

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