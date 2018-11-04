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
#include <Interpreters/Context.h>
#include <QingCloud/Datastream/QingCloudErroneousBlockInputStream.h>
#include "QingCloudPaxos.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int PAXOS_EXCEPTION;
}


QingCloudPaxos::QingCloudPaxos(const String &node_id, const UInt64 &last_accepted_id, const LogEntity &last_accepted_value,
                               const ConnectionPoolPtrs &connections, const Context &context,
                               std::function<void(UInt64, LogEntity, String)> resolution_function)
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

QingCloudPaxos::State QingCloudPaxos::sendPrepare(const LogEntity & value)
{
    ++proposer_id;
    const String & prepare_sql = "PAXOS PREPARE proposal_number = " + toString(proposer_id);
    Block res = validateQueryIsQuorum(sendQuery(prepare_sql, prepare_header), connections.size());

    const ColumnUInt64 & column_state = typeid_cast<const ColumnUInt64 &>(*res.getByName("state").column);
    const ColumnUInt64 & column_proposer_id = typeid_cast<const ColumnUInt64 &>(*res.getByName("proposer_id").column);
    const ColumnUInt64 & column_accepted_id = typeid_cast<const ColumnUInt64 &>(*res.getByName("accepted_id").column);

    for (size_t row = 0, promised = 0; row < res.rows(); ++row)
    {
        if (column_accepted_id.getUInt(row) > higher_numbered)
            return State::NEED_LEARN; /// has different paxos, we reach agreement through study.

        promised += column_proposer_id.getUInt(row) == proposer_id && column_state.getBool(row) ? 1 : 0;

        if (promised == connections.size() || promised >= connections.size() / 2 + 1)
        {
            Block block = validateQueryIsQuorum(sendQuery("PAXOS ACCEPT proposal_number=" + toString(proposer_id) + ",proposal_value_id=" +
                                                          toString(value.first) + ",proposal_value_query='" + value.second + "',from='" +
                                                          node_id + "'", accept_header), connections.size());

            return validateQuorumState(block, connections.size()) ? State::SUCCESSFULLY : State::FAILURE;
        }
    }

    return State::FAILURE;
}

Block QingCloudPaxos::receivePrepare(const UInt64 & proposal_id)
{
    if (proposal_id >= promised_id)
        promised_id = proposal_id;

    MutableColumns columns = prepare_header.cloneEmptyColumns();
    columns[0]->insert(Field(UInt64(proposal_id >= promised_id)));
    columns[1]->insert(Field(proposal_id));
    columns[2]->insert(Field(accepted_id));
    columns[3]->insert(Field(accepted_value.first));
    columns[4]->insert(accepted_value.second);
    return prepare_header.cloneWithColumns(std::move(columns));
}

Block QingCloudPaxos::acceptProposal(const String & /*from*/, const UInt64 & proposal_id, const LogEntity & value)
{
    if (proposal_id >= promised_id)
    {
        accepted_value = value;
        accepted_id = proposal_id;
        promised_id = proposal_id;
        Block current = validateQueryIsQuorum(
            sendQuery("PAXOS ACCEPTED proposal_number=" + toString(proposer_id) + ",proposal_value_id=" + toString(value.first) +
                      ",proposal_value_query='" + value.second + "',from='" + node_id + "'", accept_header), connections.size());

        if (validateQuorumState(current, connections.size()))
        {
            MutableColumns columns = accept_header.cloneEmptyColumns();
            columns[0]->insert(Field(UInt64(1)));
            return accept_header.cloneWithColumns(std::move(columns));
        }
    }

    MutableColumns columns = accept_header.cloneEmptyColumns();
    columns[0]->insert(Field(UInt64(0)));
    return accept_header.cloneWithColumns(std::move(columns));
}

Block QingCloudPaxos::acceptedProposal(const String & from, const UInt64 & proposal_id, const LogEntity & accepted_value)
{
    if (proposal_id >= final_id)
    {
        if (!proposals_with_accepted_nodes.count(proposal_id))
        {
            std::vector<String> accepted_nodes;
            proposals_with_accepted_nodes.insert(std::pair(proposal_id, accepted_nodes));
        }

        std::vector<String> & accepted_nodes = proposals_with_accepted_nodes[proposal_id];
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

    MutableColumns columns = accept_header.cloneEmptyColumns();
    columns[0]->insert(Field(UInt64(proposal_id >= final_id)));
    return accept_header.cloneWithColumns(std::move(columns));
}

Block QingCloudPaxos::sendQuery(const String & query_string, const Block & header)
{
    BlockInputStreams streams;
    Settings settings = context.getSettingsRef();
    /// TODO: use paxos timeout set query timeout; maybe settings.send_timeout and settings.receive_timeout

    for (size_t index = 0; index < connections.size(); ++index)
    {
        ConnectionPoolPtrs failover_connections;
        failover_connections.emplace_back(connections[index]);
        ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
            failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

        streams.emplace_back(std::make_shared<QingCloudErroneousBlockInputStream>(
            std::make_shared<RemoteBlockInputStream>(shard_pool, query_string, header, context)));
    }

    BlockInputStreamPtr union_stream = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, streams.size());
    return std::make_shared<SquashingBlockInputStream>(union_stream, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())->read();
}

bool QingCloudPaxos::validateQuorumState(const Block & block, size_t total_size)
{
    const ColumnUInt64 & column_state = typeid_cast<const ColumnUInt64 &>(*block.getByName("state").column);

    for (size_t row_index = 0, validated_size = 0; row_index < block.rows(); ++row_index)
    {
        UInt64 state_value = column_state.getUInt(row_index);
        if (state_value == 1)
            ++validated_size;

        if (validated_size == total_size || validated_size >= total_size / 2 + 1)
            return true;
    }

    return false;
}

Block QingCloudPaxos::validateQueryIsQuorum(const Block & block, size_t total_size)
{
    if (block.rows() != total_size && block.rows() < total_size / 2 + 1)
        throw Exception("QingCloud Paxos Protocol has no majority. ", ErrorCodes::PAXOS_EXCEPTION);

    ColumnWithTypeAndName code_column = block.getByName("_res_code");
    for (size_t row = 0, exception_size = 0; row < block.rows(); ++row)
    {
        if (typeid_cast<const ColumnUInt64 &>(*code_column.column).getUInt(row) == 1)
            ++exception_size;

        if (exception_size == total_size || exception_size >= total_size / 2 +1)
        {
            String exception_message;
            ColumnWithTypeAndName message_column = block.getByName("_exception_message");
            for (size_t exception_row = 0; exception_row < row; ++exception_row)
            {
                String current_exception_message = typeid_cast<const ColumnString&>(*message_column.column).getDataAt(exception_row);
                /// TODO: exception number or node_id prefix
                exception_message += current_exception_message + "\n";
            }
            throw Exception("QingCloud Paxos Protocol Exception " + exception_message, ErrorCodes::PAXOS_EXCEPTION);
        }
    }

    return block;
}

void QingCloudPaxos::setLastProposer(size_t proposer_id_, LogEntity proposer_value_)
{
    /// 在这里整个集群的状态将被收敛
    proposer_id = proposer_id_;
    promised_id = proposer_id_;
    accepted_id = proposer_id_;
    final_id = proposer_id_;
    accepted_value = proposer_value_;
    final_value = proposer_value_;
}

}
