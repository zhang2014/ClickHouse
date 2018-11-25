#include <QingCloud/Interpreters/Paxos/QingCloudPaxos.h>
#include <Interpreters/Context.h>
#include <QingCloud/Datastream/QingCloudErroneousBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <QingCloud/Common/differentClusters.h>
#include <Common/typeid_cast.h>
#include <QingCloud/Parsers/ParserPaxosQuery.h>
#include <Parsers/parseQuery.h>
#include <QingCloud/Interpreters/InterpreterPaxosQuery.h>
#include <Common/getMultipleKeysFromConfig.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int PAXOS_EXCEPTION;
}

QingCloudPaxos::QingCloudPaxos(DDLEntity &entity_state, const ClusterPtr &work_cluster, const Context &context,
    const StoragePtr &state_machine_storage) :
    context(context), state_machine_storage(state_machine_storage), entity_state(entity_state),
    work_cluster(work_cluster)
{
    connections = getConnectionPoolsFromClusters({work_cluster});
    std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(context.getConfigRef(), "", "listen_host");
    self_address = listen_hosts.empty() ?  "localhost" : listen_hosts[0];
}

QingCloudPaxos::State QingCloudPaxos::sendPrepare(const LogEntity & value)
{
    std::cout << "QingCloudPaxos::sendPrepare -1 \n";
    std::lock_guard<std::recursive_mutex> lock(entity_state.mutex);
    std::cout << "QingCloudPaxos::sendPrepare -2 \n";
    prepared_paxos_id = std::max(prepared_paxos_id, entity_state.accepted_paxos_id);

    ++prepared_paxos_id;
    Block prepare_res = sendQueryToCluster(prepare_header, "PAXOS PREPARE proposal_number = " + toString(prepared_paxos_id));

    const ColumnUInt64 & column_state = typeid_cast<const ColumnUInt64 &>(*prepare_res.getByName("state").column);
    const ColumnUInt64 & column_proposer_id = typeid_cast<const ColumnUInt64 &>(*prepare_res.getByName("proposer_id").column);
    const ColumnUInt64 & column_accepted_id = typeid_cast<const ColumnUInt64 &>(*prepare_res.getByName("accepted_id").column);

    for (size_t row = 0, promised = 0; row < prepare_res.rows(); ++row)
    {
        if (column_accepted_id.getUInt(row) > entity_state.accepted_paxos_id)
            return State::NEED_LEARN; /// has different paxos, we reach agreement through study.

        promised += column_proposer_id.getUInt(row) == prepared_paxos_id && column_state.getBool(row) ? 1 : 0;
        if (promised == connections.size() || promised >= connections.size() / 2 + 1)
        {
            const String query_string = "PAXOS ACCEPT proposal_number=" + toString(prepared_paxos_id) + ",proposal_value_id=" +
                                        toString(value.first) + ",proposal_value_query='" + value.second + "',from='" + self_address + "'";

            Block accept_res = validateQueryIsQuorum(sendQueryToCluster(accepted_header, query_string), connections.size());

            return validateQuorumState(accept_res, connections.size()) ? State::SUCCESSFULLY : State::FAILURE;
        }
    }
    return State::FAILURE;
}

Block QingCloudPaxos::receivePrepare(const UInt64 & prepare_paxos_id)
{
    std::lock_guard<std::recursive_mutex> lock(entity_state.mutex);
    promised_paxos_id = std::max(promised_paxos_id, entity_state.accepted_paxos_id);
    promised_paxos_id = std::max(promised_paxos_id, prepare_paxos_id);

    MutableColumns columns = prepare_header.cloneEmptyColumns();
    columns[0]->insert(UInt64(prepare_paxos_id == promised_paxos_id));
    columns[1]->insert(prepare_paxos_id);
    columns[2]->insert(entity_state.accepted_paxos_id);
    columns[3]->insert(entity_state.accepted_entity_id);
    columns[4]->insert(entity_state.accepted_entity_value);
    return prepare_header.cloneWithColumns(std::move(columns));
}

Block QingCloudPaxos::acceptProposal(const String & /*from*/, const UInt64 & prepare_paxos_id, const LogEntity & value)
{
    std::lock_guard<std::recursive_mutex> lock(entity_state.mutex);
    promised_paxos_id = std::max(promised_paxos_id, entity_state.accepted_paxos_id);
    promised_paxos_id = std::max(promised_paxos_id, prepare_paxos_id);

    if (promised_paxos_id == prepare_paxos_id)
    {
        Block accepted_res = validateQueryIsQuorum(
            sendQueryToCluster(accepted_header, "PAXOS ACCEPTED proposal_number=" + toString(prepare_paxos_id) + ",proposal_value_id=" +
                                                toString(value.first) + ",proposal_value_query='" + value.second + "',from='" +
                                                self_address + "'"), connections.size());

        if (validateQuorumState(accepted_res, connections.size()))
        {
            MutableColumns columns = accepted_header.cloneEmptyColumns();
            columns[0]->insert(UInt64(1));
            return accepted_header.cloneWithColumns(std::move(columns));
        }
    }

    MutableColumns columns = accepted_header.cloneEmptyColumns();
    columns[0]->insert(UInt64(0));
    return accepted_header.cloneWithColumns(std::move(columns));
}

Block QingCloudPaxos::acceptedProposal(const String & from, const UInt64 & accepted_paxos_id, const LogEntity & accepted_entity)
{
    std::lock_guard<std::recursive_mutex> lock(entity_state.mutex);

    if (accepted_paxos_id >= entity_state.accepted_paxos_id)
    {
        /// TODO: 设置wait最大值, 准备清除无用的
        wait_commits[accepted_paxos_id].emplace_back(from);
        if (wait_commits[accepted_paxos_id].size() == connections.size() ||
            wait_commits[accepted_paxos_id].size() >= (connections.size() / 2 + 1))
        {

            /// TODO: 由于写入与更新paxos不是原子的, 应该在其中加入一个create_time, 如果存在两个一样的值 取更新更后面的
            BlockOutputStreamPtr output = state_machine_storage->write({}, context.getSettingsRef());
            Block header = state_machine_storage->getSampleBlock();

            MutableColumns mutable_columns = header.mutateColumns();
            mutable_columns[0]->insert(accepted_entity.first);
            mutable_columns[1]->insert(accepted_paxos_id);
            mutable_columns[2]->insert(accepted_entity.second);
            header.setColumns(std::move(mutable_columns));
            output->writePrefix(); output->write(header); output->writeSuffix();

            entity_state.accepted_paxos_id = accepted_paxos_id;
            entity_state.accepted_entity_id = accepted_entity.first;
            entity_state.accepted_entity_value = accepted_entity.second;
            entity_state.store();

            for (auto iter = wait_commits.begin(); iter != wait_commits.end(); ++iter) {
                if (iter->first < entity_state.accepted_paxos_id) {
                    wait_commits.erase(iter);
                }
            }
        }
    }

    MutableColumns columns = accepted_header.cloneEmptyColumns();
    columns[0]->insert(UInt64(accepted_paxos_id >= entity_state.accepted_paxos_id));
    return accepted_header.cloneWithColumns(std::move(columns));
}

Block QingCloudPaxos::sendQueryToCluster(const Block & header, const String & query_string)
{
    BlockInputStreams streams;
    Settings settings = context.getSettingsRef();
    const auto connections = getConnectionPoolsFromClusters({work_cluster});
    /// TODO: use paxos timeout set query timeout; maybe settings.send_timeout and settings.receive_timeout

    for (size_t index = 0; index < connections.size(); ++index)
    {
        if (connections[index].first.is_local)
        {
            ParserPaxos parser_paxos;
            ASTPtr query = parseQuery(parser_paxos, query_string.data(), query_string.data() + query_string.size(), "", 0 );
            InterpreterPaxosQuery interpreter(query, context);
            streams.emplace_back(std::make_shared<QingCloudErroneousBlockInputStream>(interpreter.execute().in));
        }
        else
        {
            ConnectionPoolPtrs failover_connections;
            failover_connections.emplace_back(connections[index].second);
            ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
                failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

            streams.emplace_back(std::make_shared<QingCloudErroneousBlockInputStream>(
                std::make_shared<RemoteBlockInputStream>(shard_pool, query_string, header, context)));
        }
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
                String current_exception_message = typeid_cast<const ColumnString&>(*message_column.column).getDataAt(exception_row).toString();
                /// TODO: exception number or node_id prefix
                exception_message += current_exception_message + "\n";
            }
            throw Exception("QingCloud Paxos Protocol Exception " + exception_message, ErrorCodes::PAXOS_EXCEPTION);
        }
    }

    return block;
}


}
