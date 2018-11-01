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


namespace DB
{

QingCloudPaxos::QingCloudPaxos(const String &node_id, const UInt64 &last_accepted_id, const LogEntity &last_accepted_value,
                               const ConnectionPoolPtrs & connections, const Context &context, std::function<void(UInt64, LogEntity, String)> resolution_function)
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
    Block prepare_res = sendQuery("PAXOS PREPARE proposal_number = " + toString(proposer_id), prepare_header);

    size_t promised_size = 0;
    for (size_t row = 0; row < prepare_res.rows(); ++row)
    {
        bool state = typeid_cast<const ColumnUInt64 &>(*prepare_res.safeGetByPosition(0).column).getBool(row);
        UInt64 proposer_id_ = typeid_cast<const ColumnUInt64 &>(*prepare_res.safeGetByPosition(1).column).getUInt(row);
        UInt64 accepted_id_ = typeid_cast<const ColumnUInt64 &>(*prepare_res.safeGetByPosition(2).column).getUInt(row);

        if (accepted_id_ > higher_numbered)
        {
            /// TODO: 需要学习
            higher_numbered = accepted_id_;
            return false;
        }
        promised_size += proposer_id_ == proposer_id && state ? 1 : 0;
    }

    if (promised_size == connections.size() || promised_size >= connections.size() / 2 + 1)
    {
        Block block = sendQuery("PAXOS ACCEPT proposal_number=" + toString(proposer_id) + ",proposal_value_id=" + toString(value.first) +
                                ",proposal_value_query='" + value.second + "',from='" + node_id + "'", accept_header);

        return validateQueryIsQuorum(block, connections.size() / 2 + 1);
    }

    return false;
}

Block QingCloudPaxos::receivePrepare(const UInt64 & proposal_id)
{
    if (proposal_id >= promised_id)
        promised_id = proposal_id;

    MutableColumns columns = prepare_header.cloneEmptyColumns();
    columns[0]->insert(Field(UInt64(proposal_id >= promised_id)));
    columns[1]->insert(Field(proposer_id));
    columns[2]->insert(Field(accepted_id));
    columns[3]->insert(Field(accepted_value.first));
    columns[4]->insert(accepted_value.second);
    return prepare_header.cloneWithColumns(std::move(columns));
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
            sendQuery("PAXOS ACCEPTED proposal_number=" + toString(proposer_id) + ",proposal_value_id=" + toString(value.first) +
                              ",proposal_value_query='" + value.second + "',from='" + node_id + "'", accept_header),
            connections.size() / 2 + 1);
    }

    MutableColumns columns = accept_header.cloneEmptyColumns();
    columns[0]->insert(Field(UInt64(proposal_id >= promised_id && state)));
    return accept_header.cloneWithColumns(std::move(columns));
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

    MutableColumns columns = accept_header.cloneEmptyColumns();
    columns[0]->insert(Field(UInt64(proposal_id >= final_id)));
    return accept_header.cloneWithColumns(std::move(columns));
}

Block QingCloudPaxos::sendQuery(const String & query_string, const Block & header)
{
    Block block;
    BlockInputStreams res;
    Settings settings = context.getSettingsRef();

    try
    {
        /// TODO: 全部成功或多数的结果返回即可
        for (size_t index = 0; index < connections.size(); ++index)
        {
            try
            {
                auto connection = connections[index]->get(&settings);
                Block current_connection_block = std::make_shared<SquashingBlockInputStream>(
                    std::make_shared<RemoteBlockInputStream>(*connection, query_string, header, context, &settings),
                    std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())->read();

                //// TODO: 启动多个线程同时处理后聚合结果
                res.emplace_back(std::make_shared<OneBlockInputStream>(current_connection_block));
            }
            catch(...)
            {
                tryLogCurrentException(&Logger::get("QingCloudPaxos"), "QingCloud Paxos broadcast exception");
                res.emplace_back(std::make_shared<OneBlockInputStream>(header.cloneEmpty()));
            }

        }
        return std::make_shared<SquashingBlockInputStream>(std::make_shared<UnionBlockInputStream<>>(res, nullptr, res.size()),
                                                           std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())->read();
    }
    catch (...)
    {
        tryLogCurrentException(&Logger::get("QingCloudPaxos"), "QingCloud Paxos broadcast exception");
        return {};
    }
}

bool QingCloudPaxos::validateQueryIsQuorum(const Block &block, size_t quorum_size)
{
    bool all_successfully = true;
    for (size_t index = 0; index < block.rows(); ++index)
    {
        if (typeid_cast<const ColumnUInt64 &>(*block.safeGetByPosition(0).column).getBool(index))
            --quorum_size;
        else
            all_successfully = false;
    }

    return all_successfully || quorum_size <= 0;
}

}
