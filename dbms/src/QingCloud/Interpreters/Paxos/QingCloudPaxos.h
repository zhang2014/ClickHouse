#pragma once

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Storages/IStorage.h>
#include <QingCloud/Interpreters/Paxos/PaxosDDLEntity.h>

namespace DB
{

class Context;

class QingCloudPaxos
{
public:
    QingCloudPaxos(DDLEntity & entity_state, const ClusterPtr & work_cluster, const Context & context, const StoragePtr & state_machine_storage);

    enum State
    {
        SUCCESSFULLY,
        FAILURE,
        NEED_LEARN
    };

    State sendPrepare(const LogEntity & value);

    Block receivePrepare(const UInt64 & prepare_paxos_id);

    Block acceptProposal(const String &from, const UInt64 & prepare_paxos_id, const LogEntity & value);

    Block acceptedProposal(const String &from, const String & origin_from, const UInt64 & accepted_paxos_id, const LogEntity & accepted_entity);

private:
    const Context & context;
    const StoragePtr & state_machine_storage;
    DDLEntity & entity_state;
    const ClusterPtr work_cluster;
    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> connections;
    String self_address;

    UInt64 promised_paxos_id;
    UInt64 prepared_paxos_id;
    std::map<UInt64, std::vector<String>> wait_commits;

    Block accepted_header = Block{{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "state"}};

    Block prepare_header = Block{{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "state"},
                                 {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "proposer_id"},
                                 {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "accepted_id"},
                                 {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "accepted_value_first"},
                                 {ColumnString::create(), std::make_shared<DataTypeString>(), "accepted_value_second"}};

    bool validateQuorumState(const Block &block, size_t total_size);

    Block validateQueryIsQuorum(const Block & block, size_t total_size);

    Block sendQueryToCluster(const Block &header, const String &query_string);
};

}