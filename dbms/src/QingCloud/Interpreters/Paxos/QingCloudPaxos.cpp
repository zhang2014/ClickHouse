#include <QingCloud/Interpreters/Paxos/QingCloudPaxos.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>


namespace DB
{

QingCloudPaxos::QingCloudPaxos(const String &node_id, const UInt64 &last_accepted_id, const LogEntity &last_accepted_value,
                               const ConnectionPoolPtrs &connections, const Context &context, std::function<void(UInt64, LogEntity, String)> resolution_function)
    : node_id(node_id), promised_id(last_accepted_id), proposer_id(last_accepted_id), accepted_id(last_accepted_id),
      accepted_value(last_accepted_value), connections(connections), context(context), higher_numbered(last_accepted_id),
      final_id(last_accepted_id), final_value(last_accepted_value), resolution_function(resolution_function)
{
    accept_header = Block{{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "state"}};

    prepare_header = Block{{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "state"},
                           {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "proposer_id"},
                           {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "accepted_id"},
                           {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "accepted_value_first"},
                           {ColumnString::create(), std::make_shared<DataTypeString>(), "accepted_value_second"}};

}

bool QingCloudPaxos::sendPrepare(const LogEntity &value)
{
    ++proposer_id;
    Block prepare_res = sendQuery("PAXOS PREPPARE proposal_number = " + toString(proposer_id), prepare_header);

    size_t promised_size = 0;
    for (size_t row = 0; row < prepare_res.rows(); ++row)
    {
        bool state = typeid_cast<const ColumnUInt64 &>(*prepare_res.safeGetByPosition(0).column).getBool(row);
        UInt64 proposer_id_ = typeid_cast<const ColumnUInt64 &>(*prepare_res.safeGetByPosition(1).column).getUInt(row);
        UInt64 accepted_id_ = typeid_cast<const ColumnUInt64 &>(*prepare_res.safeGetByPosition(2).column).getUInt(row);

        if (accepted_id_ > higher_numbered)
        {
            higher_numbered = accepted_id_;
            return false;
        }
        promised_size += proposer_id_ == proposer_id && state ? 1 : 0;
    }

    if (promised_size == connections.size() || promised_size >= connections.size() / 2 + 1)
    {
        Block block = sendQuery("PAXOS ACCEPT proposal_number=" + toString(proposer_id) + ",proposal_value_id=" + toString(value.first) +
                                ",proposal_value_query=" + value.second + ",from=" + node_id, accept_header);

        return validateQueryIsQuorum(block, connections.size() / 2 + 1);
    }

    return false;
}

Block QingCloudPaxos::receivePrepare(const UInt64 & proposal_id)
{
    if (proposal_id >= promised_id)
        promised_id = proposal_id;

    MutableColumnPtr state_column = ColumnUInt64::create();
    MutableColumnPtr proposer_id_column = ColumnUInt64::create();
    MutableColumnPtr accepted_id_column = ColumnUInt64::create();
    MutableColumnPtr accepted_value_first_column = ColumnUInt64::create();
    MutableColumnPtr accepted_value_second_column = ColumnString::create();
    state_column->insert(Field(UInt64(proposal_id >= promised_id)));
    proposer_id_column->insert(Field(UInt64(proposer_id)));
    accepted_id_column->insert(Field(UInt64(accepted_id)));
    accepted_value_first_column->insert(UInt64(accepted_value.first));
    accepted_value_second_column->insert(accepted_value.second);
    return Block{{std::move(state_column),                 std::make_shared<DataTypeUInt64>(), "state"},
                 {std::move(proposer_id_column),           std::make_shared<DataTypeUInt64>(), "proposer_id"},
                 {std::move(accepted_id_column),           std::make_shared<DataTypeUInt64>(), "accepted_id"},
                 {std::move(accepted_value_first_column),  std::make_shared<DataTypeUInt64>(), "accepted_value_first"},
                 {std::move(accepted_value_second_column), std::make_shared<DataTypeString>(), "accepted_value_second"}};
}

Block QingCloudPaxos::acceptProposal(const String & /*from*/, const UInt64 & proposal_id, const LogEntity & value)
{
    bool state = true;
    if (proposal_id >= promised_id)
    {
        accepted_value = value;
        accepted_id = proposal_id;
        promised_id = proposal_id;
        state &= validateQueryIsQuorum(
            sendQuery("PAXOS ACCEPT proposal_number=" + toString(proposer_id) + ",proposal_value_id=" + toString(value.first) +
                      ",proposal_value_query=" + value.second + ",from=" + node_id, accept_header), connections.size() / 2 + 1);
    }

    MutableColumnPtr column = ColumnUInt64::create();
    column->insert(Field(UInt64(proposal_id >= promised_id && state)));
    return Block{{std::move(column), std::make_shared<DataTypeUInt64>(), "state"}};
}

Block QingCloudPaxos::acceptedProposal(const String &from, const UInt64 &proposal_id, const LogEntity &accepted_value)
{
    if (proposal_id >= final_id)
    {
        if (!proposals_with_accepted_nodes.count(proposal_id))
        {
            std::vector<String> accepted_nodes;
            proposals_with_accepted_nodes.insert(std::pair(proposal_id, accepted_nodes));
        }

        std::vector<String> &accepted_nodes = proposals_with_accepted_nodes[proposal_id];
        if (!std::count(accepted_nodes.begin(), accepted_nodes.end(), from))
            accepted_nodes.emplace_back(from);

        if (accepted_nodes.size() == connections.size() || accepted_nodes.size() >= connections.size() / 2 + 1)
        {
            final_id = proposal_id;
            final_value = accepted_value;
            resolution_function(final_id, final_value, from);

            for (const auto & entry : proposals_with_accepted_nodes)
            {
                if (entry.first < final_id)
                    proposals_with_accepted_nodes.erase(entry.first);
            }
        }
    }
    MutableColumnPtr column = ColumnUInt64::create();
    column->insert(Field(UInt64(proposal_id >= final_id)));
    return Block{{std::move(column), std::make_shared<DataTypeUInt64>(), "state"}};
}

Block QingCloudPaxos::sendQuery(const String & query_string, const Block & header)
{
    BlockInputStreams res;
    for (size_t index = 0; index < connections.size(); ++index)
    {
        try
        {
            auto connection = connections[index]->get(&settings);
            res.emplace_back(std::make_shared<RemoteBlockInputStream>(connection, query_string, header, context, settings));
        }
        catch (const Exception &e)
        {
            tryLogCurrentException(&Logger::get("QingCloudPaxos"), "QingCloud Paxos broadcast exception :");
        }
    }
    return std::make_shared<SquashingBlockInputStream>(std::make_shared<UnionBlockInputStream<>>(res, nullptr, settings.max_threads),
                                                       std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())->read();
}

bool QingCloudPaxos::validateQueryIsQuorum(const Block & block, size_t quorum_size)
{
    for (size_t index = 0; index < block.rows(); ++index)
        if (typeid_cast<const ColumnUInt64 &>(*block.safeGetByPosition(0).column).getBool(row))
            --quorum_size;

    return quorum_size > 0;
}

}
