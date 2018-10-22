#include <QingCloud/Interpreters/InterpreterPaxosQuery.h>
#include <QingCloud/Parsers/ASTPaxosQuery.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <DataStreams/OneBlockInputStream.h>
#include "QingCloudDDLSynchronism.h"


namespace DB
{

BlockIO InterpreterPaxosQuery::execute()
{
    ASTPaxosQuery * paxos_query = typeid_cast<ASTPaxosQuery *>(query_ptr.get());

    const auto multiplexed_context = context.getMultiplexedVersion();

    BlockIO res;
    QingCloudPaxosPtr paxos;

    if (paxos_query->kind == "PREPARE")
    {
        UInt64 proposal_number = paxos_query->names_and_values["proposal_number"].safeGet<UInt64>();
        res.in = std::make_shared<OneBlockInputStream>(paxos->receivePrepare(proposal_number));
    }
    else if (paxos_query->kind == "ACCEPT")
    {
        String from = paxos_query->names_and_values["from"].safeGet<String>();
        UInt64 proposal_number = paxos_query->names_and_values["proposal_number"].safeGet<UInt64>();
        UInt64 proposal_value_id = paxos_query->names_and_values["proposal_value_id"].safeGet<UInt64>();
        String proposal_value_query = paxos_query->names_and_values["proposal_value_query"].safeGet<String>();
        res.in = std::make_shared<OneBlockInputStream>(paxos->acceptProposal(from, proposal_number, std::pair(proposal_value_id, proposal_value_query)));
    }
    else if (paxos_query->kind == "ACCEPTED")
    {
        String from = paxos_query->names_and_values["from"].safeGet<String>();
        UInt64 proposal_number = paxos_query->names_and_values["proposal_number"].safeGet<UInt64>();
        UInt64 proposal_value_id = paxos_query->names_and_values["proposal_value_id"].safeGet<UInt64>();
        String proposal_value_query = paxos_query->names_and_values["proposal_value_query"].safeGet<String>();
        res.in = std::make_shared<OneBlockInputStream>(paxos->acceptedProposal(from, proposal_number, std::pair(proposal_value_id, proposal_value_query)));
    }
    return res;
}
}