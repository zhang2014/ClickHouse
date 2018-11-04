#pragma once

#include <Core/Types.h>
#include <Client/ConnectionPool.h>
#include <Interpreters/Settings.h>

namespace DB
{

class Context;

using LogEntity = std::pair<UInt64, String>;


class QingCloudPaxos
{
public:

    enum State
    {
        SUCCESSFULLY,
        FAILURE,
        NEED_LEARN
    };

    QingCloudPaxos(const String & node_id, const UInt64 &last_accepted_id, const LogEntity &last_accepted_value,
                   const ConnectionPoolPtrs &connections, const Context &context, std::function<void(UInt64, LogEntity, String)> resolution_function);

    State sendPrepare(const LogEntity & value);

    Block receivePrepare(const UInt64 & proposal_id);

    Block acceptProposal(const String &from, const UInt64 & proposal_id, const LogEntity & value);

    Block acceptedProposal(const String &from, const UInt64 & proposal_id, const LogEntity & accepted_value);

    void setLastProposer(size_t proposer_id, LogEntity proposer_value_);

private:
    String node_id;
    UInt64 promised_id;
    UInt64 proposer_id;
    UInt64 accepted_id;
    LogEntity accepted_value;
    ConnectionPoolPtrs connections;


    const Context & context;
    Block accept_header;
    Block prepare_header;
    UInt64 higher_numbered;

    UInt64 final_id;
    LogEntity final_value;
    std::unordered_map<UInt64, std::vector<String>> proposals_with_accepted_nodes;
    std::function<void(UInt64, LogEntity, String)> resolution_function;

    Block sendQuery(const String &query_string, const Block &header);

    bool validateQuorumState(const Block &block, size_t total_size);

    Block validateQueryIsQuorum(const Block & block, size_t total_size);
};

using QingCloudPaxosPtr = std::shared_ptr<QingCloudPaxos>;

}