#include <QingCloud/Interpreters/InterpreterPaxosQuery.h>
#include <QingCloud/Parsers/ASTPaxosQuery.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <DataStreams/OneBlockInputStream.h>
#include <QingCloud/Interpreters/Paxos/QingCloudDDLSynchronism.h>
#include <iostream>


namespace DB
{

BlockIO InterpreterPaxosQuery::execute()
{
    ASTPaxosQuery * paxos_query = typeid_cast<ASTPaxosQuery *>(query_ptr.get());

    BlockIO res;

    if (paxos_query->kind == "PREPARE")
    {
        UInt64 proposal_number = paxos_query->names_and_values["proposal_number"].safeGet<UInt64>();
        res.in = std::make_shared<OneBlockInputStream>(context.getDDLSynchronism()->receivePrepare(proposal_number));
    }
    else if (paxos_query->kind == "ACCEPT")
    {
        String from = paxos_query->names_and_values["from"].safeGet<String>();
        UInt64 proposal_number = paxos_query->names_and_values["proposal_number"].safeGet<UInt64>();
        UInt64 proposal_value_id = paxos_query->names_and_values["proposal_value_id"].safeGet<UInt64>();
        String proposal_value_query = paxos_query->names_and_values["proposal_value_query"].safeGet<String>();
        res.in = std::make_shared<OneBlockInputStream>(context.getDDLSynchronism()->acceptProposal(from, proposal_number, std::pair(proposal_value_id, proposal_value_query)));
    }
    else if (paxos_query->kind == "ACCEPTED")
    {
        String from = paxos_query->names_and_values["from"].safeGet<String>();
        UInt64 proposal_number = paxos_query->names_and_values["proposal_number"].safeGet<UInt64>();
        UInt64 proposal_value_id = paxos_query->names_and_values["proposal_value_id"].safeGet<UInt64>();
        String proposal_value_query = paxos_query->names_and_values["proposal_value_query"].safeGet<String>();
        res.in = std::make_shared<OneBlockInputStream>(context.getDDLSynchronism()->acceptedProposal(from, proposal_number, std::pair(proposal_value_id, proposal_value_query)));
    } else
        throw Exception("Unknow kind for paxos.", ErrorCodes::LOGICAL_ERROR);
    return res;
}

InterpreterPaxosQuery::InterpreterPaxosQuery(const ASTPtr &query_ptr_, const Context &context_)
    :query_ptr(query_ptr_), context(context_)
{

}
}