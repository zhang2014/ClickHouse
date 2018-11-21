#include <QingCloud/Storages/StorageQingCloud.h>
#include <Common/escapeForFileName.h>
#include <Poco/File.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageDistributed.h>
#include <ext/range.h>
#include <QingCloud/Common/evaluateQueryInfo.h>
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

    for (const auto & version : version_info.local_store_versions)
        createTablesWithCluster(version, context.getCluster(wrapVersionName(version)), attach, has_force_restore_data_flag);

    /// clear up
    for (Poco::DirectoryIterator it(table_data_path), end; it != end; ++it)
    {
        std::vector<String> versions = version_info.local_store_versions;
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
        query_settings.writing_version = version_info.writeable_version;
        return version_distributed[version_info.writeable_version]->write(query, query_settings);
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
        for (const auto & readable_version : version_info.readable_versions)
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
    std::cout << "StorageQingCloud get distributed lock \n";
    dynamic_cast<StorageDistributed *>(version_distributed[version].get())->waitForFlushedOtherServer();
    std::cout << "StorageQingCloud already get distributed lock \n";

    waitActionInCluster(version, "FLUSHED_DISTRIBUTED_DATA");
}

void StorageQingCloud::initializeVersions(std::initializer_list<String> versions)
{
    for (auto iterator = versions.begin(); iterator != versions.end(); ++iterator)
    {
        ClusterPtr cluster = context.getCluster(wrapVersionName(*iterator));
        createTablesWithCluster(*iterator, cluster);
        waitActionInCluster(*iterator, "INITIALIZE_VERSION");
    }
}

void StorageQingCloud::initializeVersionInfo(std::initializer_list<String> readable_versions, const String &writable_version)
{
    /// TODO: LOCK
    version_info.writeable_version = writable_version;

    for (auto iterator = readable_versions.begin(); iterator != readable_versions.end(); ++iterator)
    {
        /// TODO: check exists
        std::vector<String> current_r_versions = version_info.readable_versions;
        std::vector<String> current_l_versions = version_info.local_store_versions;

        if (std::find(current_r_versions.begin(), current_r_versions.end(), *iterator) == current_r_versions.end())
            version_info.readable_versions.emplace_back(*iterator);

        if (std::find(current_l_versions.begin(), current_l_versions.end(), *iterator) == current_l_versions.end())
            version_info.local_store_versions.emplace_back(*iterator);
    }

    version_info.storeVersionInfo();

    waitActionInCluster(writable_version, "INITIALIZE_WRITE_VERSION_INFO");
    for (auto iterator = readable_versions.begin(); iterator != readable_versions.end(); ++iterator)
        waitActionInCluster(*iterator, "INITIALIZE_READ_VERSION_INFO");

}

void StorageQingCloud::migrateDataBetweenVersions(const String & origin_version, const String & upgrade_version, bool drop, bool drop_data)
{
    const ClusterPtr origin_cluster = context.getCluster(wrapVersionName(origin_version));
    const ClusterPtr upgrade_cluster = context.getCluster(wrapVersionName(upgrade_version));

    if (drop_data)
        cleanupBeforeMigrate(upgrade_version);

    const auto diff_shards = differentClusters(origin_cluster, upgrade_cluster);

    for (const auto & local_storage : local_data_storage)
    {
        if (local_storage.first.first == origin_version)
        {
            size_t shard_number = local_storage.first.second;

            if (diff_shards.count(shard_number) && diff_shards.at(shard_number).is_local)
                rebalanceDataWithCluster(origin_version, upgrade_version, shard_number);
            else
                replaceDataWithLocal(drop, local_storage.second, local_data_storage[std::pair(upgrade_version, shard_number)]);
        }
    }
}

void StorageQingCloud::cleanupBeforeMigrate(const String &cleanup_version)
{
    for (const auto &local_storage : local_data_storage)
        if (local_storage.first.first == cleanup_version)
            local_storage.second->truncate({});     /// TODO: materialize view need query param

    waitActionInCluster(cleanup_version, "CLEANUP_VERSION_DATA");
}

void StorageQingCloud::replaceDataWithLocal(bool drop, const StoragePtr &origin_, const StoragePtr &upgrade_storage_)
{
    if (dynamic_cast<StorageMergeTree *>(origin_.get()) && dynamic_cast<StorageMergeTree *>(upgrade_storage_.get()))
    {
        std::unordered_set<String> partition_ids;

        StorageMergeTree * origin = dynamic_cast<StorageMergeTree *>(origin_.get());
        StorageMergeTree * upgrade = dynamic_cast<StorageMergeTree *>(upgrade_storage_.get());
        MergeTreeData::DataPartsVector data_parts = origin->data.getDataPartsVector({MergeTreeDataPart::State::Committed});

        for (const MergeTreeData::DataPartPtr & part : data_parts)
            partition_ids.emplace(part->info.partition_id);

        for (const auto & partition_id : partition_ids)
        {
            auto ast_partition = std::make_shared<ASTPartition>();

            ast_partition->id = partition_id;
            upgrade->replacePartitionFrom(origin_, ast_partition, false, context);
            if (drop) origin->dropPartition({}, ast_partition, false, context);
        }
    }
}

void StorageQingCloud::rebalanceDataWithCluster(const String &origin_version, const String &upgrade_version, size_t shard_number)
{
    Context query_context = context;
    query_context.getSettingsRef().query_version = origin_version;
    query_context.getSettingsRef().writing_version = upgrade_version;
    query_context.getSettingsRef().query_shard_index = shard_number;

    InterpreterSelectQuery(
        makeInsertQueryForSelf(database_name, table_name, version_distributed[origin_version]->getColumns().ordinary),
        query_context).execute();
}

void StorageQingCloud::waitActionInCluster(const String & action_version, const String &action_name)
{
    String version = wrapVersionName(action_version);
    ClusterPtr cluster = context.getCluster(version);

    if (cluster->getLocalShardCount())
    {
        std::cout << "Send Action Query: " << "ACTION NOTIFY '" + action_name + "' TABLE " + database_name + "." + table_name +
                                              " VERSION '" + action_version + "'" << "\n";

        String key = action_name + "_" + action_version;
        const auto addresses_with_connections = getConnectionPoolsFromClusters(cluster);
        sendQueryWithAddresses(addresses_with_connections, "ACTION NOTIFY '" + action_name + "' TABLE " + database_name + "." +
                                                           table_name + " VERSION '" + action_version + "'");

        std::unique_lock lock(receive_action_notify[key].mutex);

        for (const auto & addresses : addresses_with_connections)
        {
            if (!addresses.first.is_local)
                receive_action_notify[key].expected_addresses.emplace_back(addresses.first.toString());
        }

        if (!receive_action_notify[key].checkNotifyIsCompleted())
        {
            std::cv_status wait_res = receive_action_notify[key].cond.wait_for(lock, std::chrono::milliseconds(180000));
            if (wait_res == std::cv_status::timeout)
                throw Exception("", ErrorCodes::LOGICAL_ERROR);
        }


        std::cout << "Notfiy Action Query " << "ACTION NOTIFY '" + action_name + "' TABLE " + database_name + "." + table_name +
                                               " VERSION '" + action_version + "'" << "\n";
    }
}

void StorageQingCloud::sendQueryWithAddresses(const std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> &addresses_with_connections,
                                              const String &query_string) const
{
    BlockInputStreams streams;
    const Settings settings = context.getSettingsRef();
    for (const auto & address_with_connections : addresses_with_connections)
    {
        if (!address_with_connections.first.is_local)
        {
            ConnectionPoolPtrs failover_connections;
            failover_connections.emplace_back(address_with_connections.second);

            ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
                failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

            streams.emplace_back(std::make_shared<RemoteBlockInputStream>(shard_pool, query_string, Block{}, context));
        }
    }

    if (!streams.empty())
    {
        BlockInputStreamPtr union_stream = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, streams.size());
        std::make_shared<SquashingBlockInputStream>(union_stream, std::numeric_limits<size_t>::max(),
                                                    std::numeric_limits<size_t>::max())->read();
    }
}

void StorageQingCloud::receiveActionNotify(const String & action_name, const String & version)
{
    String key = action_name + "_" + version;
    receive_action_notify[key].mutex.lock();
    receive_action_notify[action_name + "_" + version].addresses.emplace_back(context.getClientInfo().client_hostname);
    if (receive_action_notify[key].checkNotifyIsCompleted())
    {
        receive_action_notify[key].mutex.unlock();
        receive_action_notify[action_name + "_" + version].cond.notify_one();
        return;
    }
    receive_action_notify[key].mutex.unlock();
}

void StorageQingCloud::createTablesWithCluster(const String & version, const ClusterPtr & cluster, bool attach, bool has_force_restore_data_flag)
{
    String version_table_data_path = table_data_path + version + "/";

    if (!version_distributed.count(version))
    {
        Poco::File(version_table_data_path).createDirectory();
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

StorageQingCloud::~StorageQingCloud() = default;

StorageQingCloud::VersionInfo::VersionInfo(const String & data_path, MultiplexedClusterPtr multiplexed_version_cluster)
    : data_path(data_path), version_path(data_path + "version.info")
{
    if (Poco::File(version_path).exists())
        loadVersionInfo();
    else
    {
        writeable_version = multiplexed_version_cluster->getCurrentWritingVersion();
        readable_versions.emplace_back(writeable_version);
        local_store_versions.emplace_back(writeable_version);
        storeVersionInfo();
    }
}

void StorageQingCloud::VersionInfo::loadVersionInfo()
{
    ReadBufferFromFile version_buffer(version_path);
    readBinary(readable_versions, version_buffer);
    readBinary(local_store_versions, version_buffer);
    readStringBinary(writeable_version, version_buffer);
}

void StorageQingCloud::VersionInfo::storeVersionInfo()
{
    Poco::File(data_path).createDirectories();
    WriteBufferFromFile version_buffer(version_path);
    writeBinary(readable_versions, version_buffer);
    writeBinary(local_store_versions, version_buffer);
    writeStringBinary(writeable_version, version_buffer);
}

bool StorageQingCloud::ActionNotifyer::checkNotifyIsCompleted()
{
    return addresses.size() == expected_addresses.size();
}

}