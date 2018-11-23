#include <QingCloud/Storages/StorageQingCloud.h>
#include <Common/escapeForFileName.h>
#include <Poco/File.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageDistributed.h>
#include <ext/range.h>
#include <Storages/AlterCommands.h>
#include <Databases/IDatabase.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Poco/DirectoryIterator.h>
#include <IO/WriteBufferFromFile.h>
#include <QingCloud/Common/differentClusters.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <mutex>
#include <condition_variable>
#include <Storages/StorageMergeTree.h>
#include <Parsers/ASTPartition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <QingCloud/Interpreters/SafetyPointFactory.h>
#include "StorageQingCloud.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ENGINE_REQUIRED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

static String wrapVersionName(const String & version)
{
    if (!startsWith(version, "tmp_"))
        return "Cluster_" + version;
    return "Cluster_" + String(version.data() + 4);
}


StorageQingCloud::StorageQingCloud(
    ASTCreateQuery &query, const String &data_path, const String &table_name, const String &database_name,
    Context &local_context, Context &context, const ColumnsDescription &columns, bool attach,
    bool has_force_restore_data_flag) : StorageQingCloudBase{columns}, context(context), data_path(data_path), table_name(table_name),
                                        database_name(database_name), local_context(local_context), columns(columns), create_query(query),
                                        table_data_path(data_path + escapeForFileName(table_name) + '/'),
                                        version_info(table_data_path, context.getMultiplexedVersion())
{
    if (!create_query.storage)
        throw Exception("Incorrect CREATE query: ENGINE required", ErrorCodes::ENGINE_REQUIRED);

    if (create_query.storage->distributed_by)
        sharding_key = create_query.storage->distributed_by->ptr();

    for (const auto & version : version_info.retain_versions)
        createTablesWithCluster(version, context.getCluster(wrapVersionName(version)), attach, has_force_restore_data_flag);

    /// clear up
    for (Poco::DirectoryIterator it(table_data_path), end; it != end; ++it)
    {
        std::vector<String> versions = version_info.retain_versions;
        if (it->isDirectory() && std::find(versions.begin(), versions.end(), it.name()) == versions.end())
            it->remove(true);
    }
}

BlockOutputStreamPtr StorageQingCloud::write(const ASTPtr & query, const Settings & settings)
{
    const String writing_version = settings.writing_version;
    const UInt64 writing_shard_number = settings.writing_shard_index;
    const auto lock = version_lock->getLock(RWLockFIFO::Read, __PRETTY_FUNCTION__);

    if (!writing_version.empty() && writing_shard_number)
        return local_data_storage[std::pair(writing_version, writing_shard_number)]->write(query, settings);
    else if (!writing_version.empty())
        return version_distributed[writing_version]->write(query, settings);
    else
    {
        Settings query_settings = settings;
        query_settings.writing_version = version_info.write_version;
        return version_distributed[version_info.write_version]->write(query, query_settings);
    }
}

BlockInputStreams StorageQingCloud::read(const Names & column_names, const SelectQueryInfo &query_info, const Context &context,
                                         QueryProcessingStage::Enum processed_stage, size_t max_block_size, unsigned num_streams)
{
    Settings settings = context.getSettingsRef();
    const String query_version = settings.query_version;
    const UInt64 query_shard_number = settings.query_shard_index;
    const auto lock = version_lock->getLock(RWLockFIFO::Read, __PRETTY_FUNCTION__);

    if (!query_version.empty() && query_shard_number)
        return local_data_storage[std::pair(query_version, query_shard_number)]->read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
    else if (!query_version.empty())
        return version_distributed[query_version]->read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
    else
    {
        BlockInputStreams streams;

        /// TODO Read and write lock
        for (const auto & readable_version : version_info.read_versions)
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

void StorageQingCloud::flushVersionData(const String & version)
{
    dynamic_cast<StorageDistributed *>(version_distributed[version].get())->waitForFlushedOtherServer();
}

void StorageQingCloud::initializeVersions(std::initializer_list<String> versions)
{
    std::vector<String> & retain_version = version_info.retain_versions;
    for (auto iterator = versions.begin(); iterator != versions.end(); ++iterator)
    {
        ClusterPtr cluster = context.getCluster(wrapVersionName(*iterator));
        if (std::find(retain_version.begin(), retain_version.end(), *iterator) == retain_version.end())
        {
            if (Poco::File(table_data_path + *iterator).exists())
                Poco::File(table_data_path + *iterator).remove(true);

            createTablesWithCluster(*iterator, cluster);
            version_info.retain_versions.emplace_back(*iterator);
        }
    }

    version_info.store();
}

void StorageQingCloud::initializeVersionInfo(std::initializer_list<String> readable_versions, const String & writable_version)
{
    /// TODO: LOCK
    version_info.write_version = writable_version;

    for (auto iterator = readable_versions.begin(); iterator != readable_versions.end(); ++iterator)
    {
        std::vector<String> current_versions = version_info.read_versions;

        if (std::find(current_versions.begin(), current_versions.end(), *iterator) == current_versions.end())
            version_info.read_versions.emplace_back(*iterator);
    }

    version_info.store();
}

void StorageQingCloud::migrateDataBetweenVersions(const String &origin_version, const String &upgrade_version, bool move)
{
    const ClusterPtr origin_cluster = context.getCluster(wrapVersionName(origin_version));
    const ClusterPtr upgrade_cluster = context.getCluster(wrapVersionName(upgrade_version));

    /// shard_number -> leader_address
    const auto diff_shards = differentClusters(origin_cluster, upgrade_cluster);

    if (!diff_shards.empty() && move)
        throw Exception("LogicError: cannot copy data with diff cluster.", ErrorCodes::LOGICAL_ERROR);

    for (const auto & local_storage : local_data_storage)
    {
        if (local_storage.first.first == origin_version)
        {
            size_t shard_number = local_storage.first.second;

            if (diff_shards.count(shard_number) && diff_shards.at(shard_number).is_local)
                migrateDataInCluster(origin_version, upgrade_version, shard_number);
            else
                migrateDataInLocal(move, local_storage.second, local_data_storage[std::pair(upgrade_version, shard_number)]);
        }
    }
}

void StorageQingCloud::cleanupBeforeMigrate(const String &cleanup_version)
{
    for (const auto &local_storage : local_data_storage)
        if (local_storage.first.first == cleanup_version)
            local_storage.second->truncate({});     /// TODO: materialize view need query param
}

void StorageQingCloud::migrateDataInLocal(bool move, const StoragePtr &origin_, const StoragePtr &upgrade_storage_)
{
    if (dynamic_cast<StorageMergeTree *>(origin_.get()) && dynamic_cast<StorageMergeTree *>(upgrade_storage_.get()))
    {
        std::unordered_set<String> processed_partition_id;

        auto * origin = dynamic_cast<StorageMergeTree *>(origin_.get());
        auto * upgrade = dynamic_cast<StorageMergeTree *>(upgrade_storage_.get());
        MergeTreeData::DataPartsVector data_parts = origin->data.getDataPartsVector({MergeTreeDataPart::State::Committed});


        for (const MergeTreeData::DataPartPtr & part : data_parts)
        {
            auto partition_id = part->info.partition_id;
            if (!processed_partition_id.count(partition_id))
            {
                auto ast_partition = std::make_shared<ASTPartition>();

                ast_partition->id = partition_id;
                processed_partition_id.emplace(partition_id);
                upgrade->replacePartitionFrom(origin_, ast_partition, false, context);
                if (move) origin->dropPartition({}, ast_partition, false, context);
            }
        }
    }
}

void StorageQingCloud::migrateDataInCluster(const String &origin_version, const String &upgrade_version, size_t shard_number)
{
    Context query_context = context;
    query_context.getSettingsRef().query_version = origin_version;
    query_context.getSettingsRef().writing_version = upgrade_version;
    query_context.getSettingsRef().query_shard_index = shard_number;

    InterpreterSelectQuery(
        makeInsertQueryForSelf(database_name, table_name, version_distributed[origin_version]->getColumns().ordinary),
        query_context).execute();
}

void StorageQingCloud::createTablesWithCluster(const String & version, const ClusterPtr & cluster, bool attach, bool has_force_restore_data_flag)
{
    String version_table_data_path = table_data_path + version + "/";

    Poco::File(version_table_data_path).createDirectories();
    if (!version_distributed.count(version))
    {
        version_distributed[version] = StorageDistributed::create(database_name, "Distributed_" + table_name, columns, database_name,
                                                                  table_name, wrapVersionName(version), context, sharding_key,
                                                                  version_table_data_path, attach);
    }

    /// TODO: 有些存储是不存储数据的 所以我们可以只创建一个
    Cluster::AddressesWithFailover shards_addresses = cluster->getShardsAddresses();

    for (size_t shard_number : ext::range(0, shards_addresses.size()))
    {
        for (Cluster::Address & replica_address : shards_addresses[shard_number])
        {
            if (replica_address.is_local && !local_data_storage.count(std::pair(version, shard_number + 1)))
                local_data_storage[std::pair(version, shard_number + 1)] = StorageFactory::instance().get(
                    true, create_query, version_table_data_path, "Shard_" + toString(shard_number + 1) + "_" + table_name, database_name,
                    local_context, context, columns, attach, has_force_restore_data_flag);
        }
    }
}

void StorageQingCloud::deleteOutdatedVersions(std::initializer_list<String> delete_versions)
{
    const auto drop_storage = [&] (const StoragePtr & storage)
    {
        storage->shutdown();
        auto table_lock = storage->lockForAlter(__PRETTY_FUNCTION__);

        storage->drop();
        storage->is_dropped = true;
    };

    for (auto iterator = delete_versions.begin(); iterator != delete_versions.end(); ++iterator)
    {
        if (version_distributed.count(*iterator))
            drop_storage(version_distributed[*iterator]);

        for (const auto & local_storage : local_data_storage)
            if (local_storage.first.first == *iterator)
                drop_storage(local_storage.second);

        String version_table_data_path = table_data_path + *iterator + "/";

        if (Poco::File(version_table_data_path).exists())
            Poco::File(version_table_data_path).remove(true);
    }
}

bool StorageQingCloud::checkNeedUpgradeVersion(const String & upgrade_version)
{
    return version_info.write_version == upgrade_version;
}

StorageQingCloud::~StorageQingCloud() = default;

}