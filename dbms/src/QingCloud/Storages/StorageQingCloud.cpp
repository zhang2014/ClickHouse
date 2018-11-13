#include <QingCloud/Storages/StorageQingCloud.h>
#include <Common/escapeForFileName.h>
#include <Poco/File.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageDistributed.h>
#include <ext/range.h>
#include <QingCloud/Common/evaluateQueryInfo.h>
#include <Storages/AlterCommands.h>
#include <Databases/IDatabase.h>
#include "StorageQingCloud.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ENGINE_REQUIRED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
        : IStorage{columns}, context(context), data_path(data_path), table_name(table_name), database_name(database_name),
        local_context(local_context), columns(columns), create_query(query)
{
    if (!create_query.storage)
        throw Exception("Incorrect CREATE query: ENGINE required", ErrorCodes::ENGINE_REQUIRED);


    String table_data_path = data_path + escapeForFileName(table_name) + '/';

    Poco::File(table_data_path).createDirectory();
//    if (Poco::File(table_data_path).createDirectory())
//    {
        MultiplexedClusterPtr multiplexed_cluster = context.getMultiplexedVersion();

        if (create_query.storage->distributed_by)
            sharding_key = create_query.storage->distributed_by->ptr();

        for (const auto & version_and_cluster : multiplexed_cluster->getAllVersionsCluster())
        {
            String version = version_and_cluster.first;
            ClusterPtr cluster = version_and_cluster.second;

            String version_table_data_path = table_data_path + version + "/";
            Poco::File(version_table_data_path).createDirectory();
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
                        local_data_storage[std::pair(version, shard_number + 1)] = StorageFactory::instance().get(
                            true, query, version_table_data_path, "Shard_" + toString(shard_number + 1) + "_" + table_name, database_name,
                            local_context, context, columns, attach, has_force_restore_data_flag);
                }
            }
        }
//    }

    /// TODO: 从磁盘恢复
}

BlockInputStreams StorageQingCloud::read(const Names &column_names, const SelectQueryInfo &query_info, const Context &context,
                                         QueryProcessingStage::Enum &processed_stage, size_t max_block_size, unsigned num_streams)
{
    std::pair<String, UInt64> version_and_shard_number = getQueryInfo(context);

    if (local_data_storage.count(version_and_shard_number))
        return local_data_storage[version_and_shard_number]->read(column_names, query_info, context, processed_stage, max_block_size,
                                                                  num_streams);
    else
    {
        if (version_and_shard_number.second)
            throw Exception("Illegal shard number " + toString(version_and_shard_number.second), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (version_distributed.count(version_and_shard_number.first))
            return version_distributed[version_and_shard_number.first]->read(
                column_names, query_info, context, processed_stage, max_block_size, num_streams);
        else
        {
            BlockInputStreams streams;

            MultiplexedClusterPtr multiplexed_cluster = context.getMultiplexedVersion();

            for (const auto & readable_version : multiplexed_cluster->getReadableVersions())
            {
                if (version_distributed.count(readable_version))
                {
                    Context query_context = context;
                    query_context.getSettingsRef().query_version = readable_version;
                    BlockInputStreams res = version_distributed[readable_version]->read(column_names, query_info, query_context,
                                                                                        processed_stage, max_block_size, num_streams);
                    streams.insert(streams.end(), res.begin(), res.end());
                }
            }

            return streams;
        }
    }
}

BlockOutputStreamPtr StorageQingCloud::write(const ASTPtr & query, const Settings & settings)
{
    std::pair<String, UInt64> version_and_shard_number = getWritingInfo(settings, context);

    if (local_data_storage.count(version_and_shard_number))
    {
        return local_data_storage[version_and_shard_number]->write(query, settings);
    }
    else
    {
        if (version_and_shard_number.second)
            throw Exception("Illegal shard number " + toString(version_and_shard_number.second), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!version_distributed.count(version_and_shard_number.first))
            throw Exception("Cannot find " + version_and_shard_number.first + " Storage.", ErrorCodes::LOGICAL_ERROR);

        Settings query_settings = settings;
        query_settings.writing_version = version_and_shard_number.first;
        return version_distributed[version_and_shard_number.first]->write(query, query_settings);
    }
}

void StorageQingCloud::drop()
{
    for (const auto & storage : version_distributed)
        storage.second->drop();

    for (const auto & storage : local_data_storage)
        storage.second->drop();
}

bool StorageQingCloud::checkData() const
{
    bool res = true;
    for (const auto & storage : local_data_storage)
        res &= storage.second->checkData();
    return res;
}

void StorageQingCloud::truncate(const ASTPtr & truncate_query)
{
    for (const auto & storage : version_distributed)
        storage.second->truncate(truncate_query);

    for (const auto & storage : local_data_storage)
        storage.second->truncate(truncate_query);
}

void StorageQingCloud::mutate(const MutationCommands & commands, const Context & context)
{
    for (const auto & storage : local_data_storage)
        storage.second->mutate(commands, context);
}

void StorageQingCloud::attachPartition(const ASTPtr & partition, bool part, const Context & context)
{
    for (const auto & storage : local_data_storage)
        storage.second->attachPartition(partition, part, context);
}

void StorageQingCloud::fetchPartition(const ASTPtr & partition, const String & from, const Context & context)
{
    for (const auto & storage : local_data_storage)
        storage.second->fetchPartition(partition, from, context);
}

void StorageQingCloud::freezePartition(const ASTPtr & partition, const String & with_name, const Context & context)
{
    for (const auto & storage : local_data_storage)
        storage.second->freezePartition(partition, with_name, context);
}

void StorageQingCloud::dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & context)
{
    for (const auto & storage : local_data_storage)
        storage.second->dropPartition(query, partition, detach, context);
}

void StorageQingCloud::clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context)
{
    for (const auto & storage : local_data_storage)
        storage.second->clearColumnInPartition(partition, column_name, context);
}

bool StorageQingCloud::optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    bool res = true;
    for (const auto & storage : local_data_storage)
        res &= storage.second->optimize(query, partition, final, deduplicate, context);
    return res;
}

void StorageQingCloud::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context)
{
    for (const auto & storage : local_data_storage)
        storage.second->replacePartitionFrom(source_table, partition, replace, context);
}

void StorageQingCloud::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
    auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    ColumnsDescription new_columns = getColumns();
    params.apply(new_columns);
    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, {});
    setColumns(std::move(new_columns));

    for (const auto & storage : version_distributed)
        storage.second->alter(params, database_name, table_name, context);

    for (const auto & storage : local_data_storage)
        storage.second->alter(params, database_name, table_name, context);
}

bool StorageQingCloud::hasColumn(const String & column_name) const
{
    bool initialize = false;
    bool has_column = false;

    for (const auto & storage : version_distributed)
    {
        bool current_has_column = storage.second->hasColumn(column_name);
        if (initialize && current_has_column != has_column)
            throw Exception("LOGICAL ERROR: different has column for multiplexed storage.", ErrorCodes::LOGICAL_ERROR);

        initialize |= true;
        has_column = current_has_column;
    }

    if (!has_column)
    {
        initialize = false;
        has_column = false;

        for (const auto & storage : local_data_storage)
        {
            bool current_has_column = storage.second->hasColumn(column_name);
            if (initialize && current_has_column != has_column)
                throw Exception("LOGICAL ERROR: different has column for multiplexed storage.", ErrorCodes::LOGICAL_ERROR);

            initialize |= true;
            has_column = current_has_column;
        }
    }

    return has_column;
}

void StorageQingCloud::attachVersion(const String & attach_version, const Context & context_)
{
    auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    /// TODO: 保证原子性, 利用storage的rename可以实现原子性
    if (!version_distributed.count(attach_version))
    {
        ClusterPtr cluster = context_.getCluster("Cluster_" + attach_version);
        String version_table_data_path = data_path + escapeForFileName(table_name) + '/' + attach_version + "/";

        version_distributed[attach_version] = StorageDistributed::create(database_name, "Distributed_" + table_name, columns, database_name,
                                                                         table_name, "Cluster_" + attach_version, context_, sharding_key,
                                                                         version_table_data_path, false);

        /// TODO: 有些存储不需要包装这个类型 如: view、null 等
        Cluster::AddressesWithFailover shards_addresses = cluster->getShardsAddresses();

        for (size_t shard_number : ext::range(0, shards_addresses.size()))
        {
            for (Cluster::Address & replica_address : shards_addresses[shard_number])
            {
                if (replica_address.is_local && !local_data_storage.count(std::pair(attach_version, shard_number + 1)))
                {
                    local_data_storage[std::pair(attach_version, shard_number + 1)] = StorageFactory::instance().get(
                        true, create_query, version_table_data_path, "Shard_" + toString(shard_number + 1) + "_" + table_name, database_name,
                        local_context, context, columns, false, false);
                }
            }
        }
    }

}

void StorageQingCloud::detachVersion(const String & detach_version, const Context & context_)
{
    auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    if (version_distributed.count(detach_version))
    {
        ClusterPtr cluster = context_.getCluster("Cluster_" + detach_version);

        StoragePtr distributed_storage = version_distributed[detach_version];
        distributed_storage->shutdown();
        distributed_storage->drop();
        distributed_storage->is_dropped = true;

        String version_table_data_path = data_path + escapeForFileName(table_name) + '/' + detach_version + "/";
        if (Poco::File(version_table_data_path + distributed_storage->getTableName()).exists())
            Poco::File(version_table_data_path + distributed_storage->getTableName()).remove(true);

        for (const auto & local_storage : local_data_storage)
        {
            if (local_storage.first.first == detach_version)
            {
                local_storage.second->shutdown();
                local_storage.second->drop();
                local_storage.second->is_dropped = true;

                if (Poco::File(version_table_data_path + local_storage.second->getTableName()).exists())
                    Poco::File(version_table_data_path + local_storage.second->getTableName()).remove(true);
            }
        }
    }
}

StorageQingCloud::~StorageQingCloud() = default;

}