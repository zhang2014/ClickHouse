#include <QingCloud/Interpreters/MultiplexedVersionCluster.h>
#include <utility>
#include <IO/ReadHelpers.h>
#include <Storages/IStorage.h>
#include <Common/DNSResolver.h>
#include <Common/isLocalAddress.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <QingCloud/Common/addressesEquals.h>
#include <QingCloud/Common/getPropertyOrChildValue.h>
#include <QingCloud/Common/visitorConfigurationKey.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <IO/ReadBufferFromFile.h>
#include <QingCloud/Storages/StorageQingCloud.h>
#include "MultiplexedVersionCluster.h"
#include <QingCloud/Common/UpgradeStorageMacro.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ALREADY_UPGRADE_VERSION;
}

static String progressToString(ProgressEnum progress_enum)
{
    switch(progress_enum)
    {
        case NORMAL : return "NORMAL";
        case FLUSH_OLD_VERSION_DATA: return "FLUSH_OLD_VERSION_DATA";
        case CLEANUP_UPGRADE_VERSION: return "CLEANUP_UPGRADE_VERSION";
        case MIGRATE_OLD_VERSION_DATA: return "MIGRATE_OLD_VERSION_DATA";
        case MIGRATE_TMP_VERSION_DATA: return "MIGRATE_TMP_VERSION_DATA";
        case DELETE_OUTDATED_VERSIONS: return "DELETE_OUTDATED_VERSIONS";
        case FLUSH_UPGRADE_VERSION_DATA: return "FLUSH_UPGRADE_VERSION_DATA";
        case INITIALIZE_UPGRADE_VERSION: return "INITIALIZE_UPGRADE_VERSION";
        case REDIRECT_VERSIONS_AFTER_MIGRATE: return "REDIRECT_VERSIONS_AFTER_MIGRATE";
        case REDIRECT_VERSIONS_BEFORE_MIGRATE: return "REDIRECT_VERSIONS_BEFORE_MIGRATE";
        case REDIRECT_VERSION_AFTER_ALL_MIGRATE: return "REDIRECT_VERSION_AFTER_ALL_MIGRATE";
    }

    throw Exception("Cannot toString with " + toString(int(progress_enum)), ErrorCodes::LOGICAL_ERROR);
}

static ProgressEnum stringToProgress(const String & string)
{
    if (string == "NORMAL")
        return ProgressEnum::NORMAL;
    else if (string == "FLUSH_OLD_VERSION_DATA")
        return ProgressEnum::FLUSH_OLD_VERSION_DATA;
    else if (string == "CLEANUP_UPGRADE_VERSION")
        return ProgressEnum::CLEANUP_UPGRADE_VERSION;
    else if (string == "MIGRATE_OLD_VERSION_DATA")
        return ProgressEnum::MIGRATE_OLD_VERSION_DATA;
    else if (string == "MIGRATE_TMP_VERSION_DATA")
        return ProgressEnum::MIGRATE_TMP_VERSION_DATA;
    else if (string == "DELETE_OUTDATED_VERSIONS")
        return ProgressEnum::DELETE_OUTDATED_VERSIONS;
    else if (string == "FLUSH_UPGRADE_VERSION_DATA")
        return ProgressEnum::FLUSH_UPGRADE_VERSION_DATA;
    else if (string == "INITIALIZE_UPGRADE_VERSION")
        return ProgressEnum::INITIALIZE_UPGRADE_VERSION;
    else if (string == "REDIRECT_VERSIONS_AFTER_MIGRATE")
        return ProgressEnum::REDIRECT_VERSIONS_AFTER_MIGRATE;
    else if (string == "REDIRECT_VERSIONS_BEFORE_MIGRATE")
        return ProgressEnum::REDIRECT_VERSIONS_BEFORE_MIGRATE;
    else if (string == "REDIRECT_VERSION_AFTER_ALL_MIGRATE")
        return ProgressEnum::REDIRECT_VERSION_AFTER_ALL_MIGRATE;
}

MultiplexedVersionCluster::MultiplexedVersionCluster(
    const Poco::Util::AbstractConfiguration & configuration, const Settings & settings, const std::string & config_prefix)
    : upgrade_job_info(configuration.getString("path") + "/version.info",
                       getPropertyOrChildValue<String>(configuration, config_prefix + ".Version", "id"))
{
    updateMultiplexedVersionCluster(configuration, settings, config_prefix);
}


String MultiplexedVersionCluster::getCurrentVersion()
{
    std::unique_lock<std::mutex> lock(upgrade_job_info.mutex);
    return upgrade_job_info.version_info.second;
}

ClusterPtr MultiplexedVersionCluster::getCluster(const DB::String & cluster_name)
{
    if (startsWith(cluster_name, "Cluster_"))
    {
        std::unique_lock<std::mutex> lock(mutex);
        String version = String(cluster_name.data() + 8);
        if (all_version_and_cluster.count(version))
            return all_version_and_cluster[version];
    }

    return {};
}

ConnectionPoolPtr MultiplexedVersionCluster::getOrCreateConnectionPools(const Cluster::Address & address, const Settings & settings)
{
    for (const auto & address_and_connection_pool : address_and_connection_pool_cache)
        if (addressEquals(address_and_connection_pool.first, address))
            return address_and_connection_pool.second;

    ConnectionPoolPtr connections = std::make_shared<ConnectionPool>(
        settings.distributed_connections_pool_size, address.host_name, address.port, address.default_database, address.user, address.password,
        ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings).getSaturated(settings.max_execution_time), "server", address.compression, address.secure);

    address_and_connection_pool_cache.emplace_back(std::pair(address, connections));

    return connections;
}

void MultiplexedVersionCluster::updateMultiplexedVersionCluster(
    const Poco::Util::AbstractConfiguration & configuration, const Settings & settings, const std::string & config_prefix)
{
    std::unique_lock<std::mutex> lock(mutex);
    all_version_and_cluster.clear();

    visitorConfigurationKey(configuration, config_prefix, "Version", [&, this](const String & version_key) {
        const String version_id = getPropertyOrChildValue<String>(configuration, version_key, "id");
        all_version_and_cluster[version_id] = std::make_shared<DummyCluster>(configuration, version_key, this, settings);
    });
    /// TODO: clean up old connections
}

void MultiplexedVersionCluster::checkBeforeUpgrade(const String & origin_version, const String & upgrade_version)
{
    std::unique_lock<std::mutex> lock(upgrade_job_info.mutex);

    if (is_running || (upgrade_job_info.version_info.first != origin_version ||
                       (upgrade_job_info.version_info.second != origin_version && upgrade_job_info.version_info.second != upgrade_version)))
        throw Exception("In the process of upgrading, two upgrade tasks cannot be performed simultaneously",
                        ErrorCodes::ALREADY_UPGRADE_VERSION);
}

void MultiplexedVersionCluster::executionUpgradeVersion(const String &origin_version, const String &upgrade_version,
                                                        const std::vector<DatabaseAndTableName> &databases_and_tables_name,
                                                        const Context &context, const SafetyPointWithClusterPtr & safety_point_sync)
{
    if (upgrade_job_info.databases_with_progress.empty())
        upgrade_job_info.initializeDatabaseAndProgress(databases_and_tables_name);

    redirect(origin_version, upgrade_version, context, safety_point_sync);
    rebalance(origin_version, upgrade_version, context, safety_point_sync);

    upgrade_job_info.buffer = std::make_shared<WriteBufferFromFile>(upgrade_job_info.data_path);
    writeStringBinary(upgrade_version, *upgrade_job_info.buffer);
    writeStringBinary(upgrade_version, *upgrade_job_info.buffer);
    upgrade_job_info.buffer->sync();

    upgrade_job_info.databases_with_progress.clear();
    upgrade_job_info.version_info = std::pair(upgrade_version, upgrade_version);
}

void MultiplexedVersionCluster::redirect(const String &origin_version, const String &upgrade_version, const Context &context, const SafetyPointWithClusterPtr &safety_point_sync)
{
    const String tmp_upgrade_version = "tmp_" + upgrade_version;

    /// TODO: 排序
    for (const auto & table_and_progress : upgrade_job_info.databases_with_progress)
    {
        BEGIN_WITH(INITIALIZE_UPGRADE_VERSION, FLUSH_OLD_VERSION_DATA)
        CASE_WITH_ALTER_LOCK(INITIALIZE_UPGRADE_VERSION, REDIRECT_VERSIONS_BEFORE_MIGRATE, initializeVersions, {origin_version, tmp_upgrade_version, upgrade_version})
        CASE_WITH_ALTER_LOCK(REDIRECT_VERSIONS_BEFORE_MIGRATE, FLUSH_OLD_VERSION_DATA, initializeVersionInfo,  {origin_version, tmp_upgrade_version}, tmp_upgrade_version)
        END_WITH(INITIALIZE_UPGRADE_VERSION, FLUSH_OLD_VERSION_DATA)
    }
}

void MultiplexedVersionCluster::rebalance(const String &origin_version, const String &upgrade_version, const Context &context, const SafetyPointWithClusterPtr &safety_point_sync)
{
    const String tmp_upgrade_version = "tmp_" + upgrade_version;

    /// TODO: 排序
    for (const auto & table_and_progress : upgrade_job_info.databases_with_progress)
    {
        BEGIN_WITH(FLUSH_OLD_VERSION_DATA, NORMAL)
        CASE_WITH_ALTER_LOCK(DELETE_OUTDATED_VERSIONS, NORMAL, deleteOutdatedVersions, {origin_version, tmp_upgrade_version})
        CASE_WITH_ALTER_LOCK(REDIRECT_VERSION_AFTER_ALL_MIGRATE, DELETE_OUTDATED_VERSIONS, initializeVersionInfo, {upgrade_version}, upgrade_version)
        CASE_WITH_ALTER_LOCK(REDIRECT_VERSIONS_AFTER_MIGRATE, MIGRATE_TMP_VERSION_DATA, initializeVersionInfo, {tmp_upgrade_version, upgrade_version}, upgrade_version)

        CASE_WITH_STRUCT_LOCK(FLUSH_OLD_VERSION_DATA, CLEANUP_UPGRADE_VERSION, flushVersionData, origin_version)
        CASE_WITH_STRUCT_LOCK(CLEANUP_UPGRADE_VERSION, MIGRATE_OLD_VERSION_DATA, cleanupBeforeMigrate, upgrade_version)
        CASE_WITH_STRUCT_LOCK(FLUSH_UPGRADE_VERSION_DATA, REDIRECT_VERSIONS_AFTER_MIGRATE, flushVersionData, upgrade_version)
        CASE_WITH_STRUCT_LOCK(MIGRATE_OLD_VERSION_DATA, FLUSH_UPGRADE_VERSION_DATA, migrateDataBetweenVersions, origin_version, upgrade_version, true)
        CASE_WITH_STRUCT_LOCK(MIGRATE_TMP_VERSION_DATA, REDIRECT_VERSION_AFTER_ALL_MIGRATE, migrateDataBetweenVersions, tmp_upgrade_version, upgrade_version, false)
        END_WITH(FLUSH_OLD_VERSION_DATA, NORMAL)
    }
}

MultiplexedVersionCluster::UpgradeJobInfo::UpgradeJobInfo(const String & path, const String & default_version)
    : data_path(path), version_info(std::pair(default_version, default_version))
{
    if (!Poco::File(data_path).exists())
    {
        buffer = std::make_shared<WriteBufferFromFile>(data_path);
        writeStringBinary(default_version, *buffer);
        writeStringBinary(default_version, *buffer);
        buffer->sync();
    }

    if (Poco::File(data_path).exists())
    {
        buffer = std::make_shared<WriteBufferFromFile>(data_path);
        buffer->seek(0, SEEK_END);

        ReadBufferFromFile read_buffer = ReadBufferFromFile(data_path);
        readStringBinary(version_info.first, read_buffer);
        readStringBinary(version_info.second, read_buffer);

        while(!read_buffer.eof())
        {
            String progress_enum_string;
            std::pair<String, String> database_and_table_name;
            readStringBinary(database_and_table_name.first, read_buffer);
            readStringBinary(database_and_table_name.second, read_buffer);
            readStringBinary(progress_enum_string, read_buffer);

            databases_with_progress[database_and_table_name] = stringToProgress(progress_enum_string);
        }
    }
}

void MultiplexedVersionCluster::UpgradeJobInfo::initializeDatabaseAndProgress(const std::vector<DatabaseAndTableName> & databases_and_tables_name)
{
    std::unique_lock<std::mutex> lock(mutex);
    /// TODO: 写到临时文件中, 然后做替换, 防止中途宕机
    for (const auto & database_and_table_name : databases_and_tables_name)
        recordUpgradeStatus(database_and_table_name.first, database_and_table_name.second, INITIALIZE_UPGRADE_VERSION);
}

void MultiplexedVersionCluster::UpgradeJobInfo::recordUpgradeStatus(const String & database_name, const String & table_name, ProgressEnum progress_enum)
{
    std::unique_lock<std::mutex> lock(mutex);
    databases_with_progress[std::pair(database_name, table_name)] = progress_enum;
    writeStringBinary(database_name, *buffer);
    writeStringBinary(table_name, *buffer);
    writeStringBinary(progressToString(progress_enum), *buffer);
    buffer->sync();
}

DummyCluster::DummyCluster(const Poco::Util::AbstractConfiguration & configuration, const std::string & configuration_prefix,
                           MultiplexedVersionCluster * multiplexed_version_cluster, const Settings & settings)
{

    size_t shard_idx = 0;

    visitorConfigurationKey(configuration, configuration_prefix, "Shard", [&](const String & shared_key){
        Addresses shard_addresses;
        Addresses shard_local_addresses;
        ConnectionPoolPtrs shard_connections;

        visitorConfigurationKey(configuration, shared_key, "Replica", [&](const String & replica_key)
        {
            Cluster::Address replica_address = createAddresses(configuration, replica_key);
            ConnectionPoolPtr replica_connections = multiplexed_version_cluster->getOrCreateConnectionPools(replica_address, settings);

            if (replica_address.is_local)
                shard_local_addresses.emplace_back(replica_address);

            shard_addresses.emplace_back(replica_address);
            shard_connections.emplace_back(replica_connections);
        });

        auto shard_weight = getPropertyOrChildValue<UInt32>(configuration, shared_key, "weight", 1);
        ShardInfo shard_info = createShardInfo(++shard_idx, shard_addresses, shard_local_addresses, shard_connections, shard_weight, settings);
        shards_info.emplace_back(shard_info);
    });
}

Cluster::Address DummyCluster::createAddresses(const Poco::Util::AbstractConfiguration &configuration, const String &replica_key) const
{
    Address address;
    UInt16 clickhouse_port = static_cast<UInt16>(configuration.getInt("tcp_port", 0));
    address.host_name = getPropertyOrChildValue<String>(configuration, replica_key, "host");
    address.port = getPropertyOrChildValue<UInt16>(configuration, replica_key, "port");
    address.user = getPropertyOrChildValue<String>(configuration, replica_key, "user", "default");
    address.password = getPropertyOrChildValue<String>(configuration, replica_key, "password", "");
    address.secure = getPropertyOrChildValue<bool>(configuration, replica_key, "secure", false) ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    address.compression = getPropertyOrChildValue<bool>(configuration, replica_key, "compression", true) ? Protocol::Compression::Enable : Protocol::Compression::Disable;
    const auto initially_resolved_address = DNSResolver::instance().resolveAddress(address.host_name, address.port);
    address.is_local = isLocalAddress(initially_resolved_address, clickhouse_port);
    address.default_database = getPropertyOrChildValue<String>(configuration, replica_key, "default_database", "");
    return address;
}

Cluster::ShardInfo DummyCluster::createShardInfo(
    size_t shard_number, Addresses shard_addresses, Addresses shard_local_addresses, ConnectionPoolPtrs shard_connections, UInt32 weight,
    const Settings & settings)
{
    ShardInfo shard_info;
    shard_info.weight = weight;
    shard_info.shard_num = UInt32(shard_number);
    shard_info.has_internal_replication = false;
    shard_info.per_replica_pools.insert(shard_info.per_replica_pools.end(), shard_connections.begin(), shard_connections.end());
    shard_info.local_addresses.insert(shard_info.local_addresses.end(), shard_local_addresses.begin(), shard_local_addresses.end());
    shard_info.pool = std::make_shared<ConnectionPoolWithFailover>(shard_info.per_replica_pools, settings.load_balancing, settings.connections_with_failover_max_tries);

    addresses.emplace_back(shard_addresses);
    local_shard_count += shard_local_addresses.empty() ? 0 : 1;
    remote_shard_count += shard_local_addresses.empty() ? 1 : 0;
    if (shard_info.weight)
        slot_to_shard.insert(std::end(slot_to_shard), shard_info.weight, shards_info.size());

    return shard_info;
}

const Cluster::AddressesWithFailover & DummyCluster::getShardsAddresses() const
{
    return addresses;
}

size_t DummyCluster::getShardCount() const
{
    return shards_info.size();
}

const Cluster::ShardsInfo &DummyCluster::getShardsInfo() const
{
    return shards_info;
}

const Cluster::SlotToShard & DummyCluster::getSlotToShard() const
{
    return slot_to_shard;
}

size_t DummyCluster::getLocalShardCount() const
{
    return local_shard_count;
}

size_t DummyCluster::getRemoteShardCount() const
{
    return remote_shard_count;
}

}
