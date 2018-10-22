#pragma once

#include <Core/Types.h>
#include <Client/ConnectionPool.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>

namespace DB
{

using LogEntity = std::pair<UInt64, String>;

class QingCloudPaxos
{
public:
    QingCloudPaxos(const String & node_id, const UInt64 &last_accepted_id, const LogEntity &last_accepted_value,
                   const ConnectionPoolPtrs &connections, const Context &context, std::function<void(UInt64, LogEntity, String)> resolution_function);


    bool sendPrepare(const LogEntity & value);

    Block receivePrepare(const UInt64 & proposal_id);

    Block acceptProposal(const String &from, const UInt64 & proposal_id, const LogEntity & value);

    Block acceptedProposal(const String &from, const UInt64 & proposal_id, const LogEntity & accepted_value);

private:
    String node_id;
    UInt64 promised_id;
    UInt64 proposer_id;
    UInt64 accepted_id;
    LogEntity accepted_value;
    ConnectionPoolPtrs connections;


    Context context;
    Settings settings;
    Block accept_header;
    Block prepare_header;
    UInt64 higher_numbered;

    UInt64 final_id;
    LogEntity final_value;
    std::unordered_map<UInt64, std::vector<String>> proposals_with_accepted_nodes;
    std::function<void(UInt64, LogEntity, String)> resolution_function;

    Block sendQuery(const String &query_string, const Block &header);

    bool validateQueryIsQuorum(const Block &block, size_t quorum_size);
};

using QingCloudPaxosPtr = std::shared_ptr<QingCloudPaxos>;

}