#include <QingCloud/Storages/StorageQingCloud.h>
#include <Common/escapeForFileName.h>
#include <Poco/File.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageDistributed.h>
#include <ext/range.h>
#include <QingCloud/Common/evaluateQueryInfo.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ENGINE_REQUIRED;
}

void StorageQingCloud::startup()
{
    for (const auto & storage : local_data_storage)
        storage.second->startup();

    for (const auto & storage : version_distributed)
        storage.second->startup();
}

void StorageQingCloud::shutdown()
{
    for (const auto & storage : version_distributed)
        storage.second->shutdown();

    for (const auto & storage : local_data_storage)
        storage.second->shutdown();
}

StorageQingCloud::StorageQingCloud(
    ASTCreateQuery & query, const String & data_path, const String & table_name, const String & database_name,
    Context &local_context, Context &context, const ColumnsDescription &columns, bool attach,
    bool has_force_restore_data_flag)
        : context(context), data_path(data_path), table_name(table_name), database_name(database_name),
        local_context(local_context), columns(columns), create_query(query)
{
    if (!create_query.storage)
        throw Exception("Incorrect CREATE query: ENGINE required", ErrorCodes::ENGINE_REQUIRED);

    String table_data_path = data_path + escapeForFileName(table_name) + '/';

    Poco::File(table_data_path).createDirectory();
    ASTPtr sharding_key = create_query.storage->distributed_by->ptr();
    MultiplexedClusterPtr multiplexed_cluster = context.getMultiplexedVersion();

    for (const auto & version_and_cluster : multiplexed_cluster->getAllVersionsCluster())
    {
        String version = version_and_cluster.first;
        ClusterPtr cluster = version_and_cluster.second;

        String version_table_data_path = table_data_path + version + "/";
        /// database_name, table_name, columns, remote_database, remote_table, cluster_name, context, sharding_key, data_path, attach
        version_distributed[version] = StorageDistributed::create(database_name, "Distributed_" + table_name, columns, database_name,
                                                                  table_name, "Cluster_" + version, context, sharding_key,
                                                                  version_table_data_path, attach);

        /// TODO: 有些存储不需要包装这个类型 如: view、null 等
        Cluster::AddressesWithFailover shards_addresses = cluster->getShardsAddresses();

        for (size_t shard_number : ext::range(0, shards_addresses.size()))
        {
            for (Cluster::Address & replica_address : shards_addresses[shard_number])
            {
                if (replica_address.is_local)
                    local_data_storage[std::pair(version, shard_number)] = StorageFactory::instance().get(
                        query, version_table_data_path, table_name, database_name, local_context, context, columns, attach,
                        has_force_restore_data_flag);
            }
        }
    }
}

BlockInputStreams StorageQingCloud::read(const Names &column_names, const SelectQueryInfo &query_info, const Context &context,
                                         QueryProcessingStage::Enum &processed_stage, size_t max_block_size, unsigned num_streams)
{
    std::pair<String, UInt64> version_and_shard_number = getQueryInfo(context);

    if (local_data_storage.count(version_and_shard_number))
        return local_data_storage[version_and_shard_number]->read(column_names, query_info, context, processed_stage, max_block_size,
                                                                  num_streams);
    else if (version_distributed.count(version_and_shard_number.first))
        return version_distributed[version_and_shard_number.first]->read(column_names, query_info, context, processed_stage, max_block_size,
                                                                         num_streams);
    else
    {
        BlockInputStreams streams;

        for (const auto & storage : version_distributed)
        {
            BlockInputStreams res = storage.second->read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
            streams.insert(streams.end(), res.begin(), res.end());
        }

        return streams;
    }
}

BlockOutputStreamPtr StorageQingCloud::write(const ASTPtr &query, const Settings &settings)
{
    MultiplexedClusterPtr multiplexed_version = context.getMultiplexedVersion();

    std::pair<String, UInt64> version_and_shard_number = getWritingInfo(settings, context);

    if (local_data_storage.count(version_and_shard_number))
        return local_data_storage[version_and_shard_number]->write(query, settings);

    if (!version_distributed.count(version_and_shard_number.first))
        throw Exception("Cannot find " + version_and_shard_number.first + " Storage.", ErrorCodes::LOGICAL_ERROR);

    return version_distributed[version_and_shard_number.first]->write(query, settings);
}

void StorageQingCloud::drop()
{
    for (const auto & storage : version_distributed)
        storage.second->drop();

    for (const auto & storage : local_data_storage)
        storage.second->drop();
}

}