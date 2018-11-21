#include <QingCloud/Interpreters/MultiplexedVersionCluster.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/DNSResolver.h>
#include <Common/isLocalAddress.h>
#include <Storages/IStorage.h>
#include <IO/ReadHelpers.h>
#include "MultiplexedVersionCluster.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int SHARD_HAS_NO_CONNECTIONS;
}

static inline bool addressEquals(Cluster::Address expect, Cluster::Address actual)
{
    if (expect.host_name != actual.host_name)
        return false;
    if (expect.port != actual.port)
        return false;
    if (expect.user != actual.user)
        return false;
    if (expect.password != actual.password)
        return false;
    if (expect.secure != actual.secure)
        return false;

    return expect.compression == actual.compression;
}

std::vector<String> MultiplexedVersionCluster::getReadableVersions()
{
    std::vector<String> readable_versions;

    for (const auto & version_and_cluster : all_version_and_cluster)
    {
        ClusterPtr cluster = version_and_cluster.second;
        if (static_cast<DummyCluster *>(cluster.get())->is_readable)
            readable_versions.emplace_back(version_and_cluster.first);
    }

    return readable_versions;
}

std::map<String, ClusterPtr> MultiplexedVersionCluster::getAllVersionsCluster()
{
    return all_version_and_cluster;
}

String MultiplexedVersionCluster::getCurrentWritingVersion()
{
    for (const auto & version_and_cluster : all_version_and_cluster)
    {
        ClusterPtr cluster = version_and_cluster.second;
        if (static_cast<DummyCluster *>(cluster.get())->is_writeable)
            return version_and_cluster.first;
    }
    throw Exception("Cannot fount writing cluster.", ErrorCodes::BAD_GET);
}

std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> MultiplexedVersionCluster::getAddressesAndConnections()
{
    return address_and_connection_pool_cache;
}


MultiplexedVersionCluster::MultiplexedVersionCluster(const Poco::Util::AbstractConfiguration &configuration, const Settings &settings,
                                                     const std::string &config_prefix)
{
    updateMultiplexedVersionCluster(configuration, settings, config_prefix);
}

void MultiplexedVersionCluster::updateMultiplexedVersionCluster(
    const Poco::Util::AbstractConfiguration & configuration, const Settings & settings, const std::string & config_prefix)
{
    configuration_lock->getLock(RWLockFIFO::Write, "ConfigReload");
    Poco::Util::AbstractConfiguration::Keys versions_key;
    configuration.keys(config_prefix, versions_key);

    if (versions_key.empty())
        throw Exception("No cluster elements (version) specified in config at path " + config_prefix, ErrorCodes::SHARD_HAS_NO_CONNECTIONS);

    /// TODO: clear unuse connection
    std::map<String, ClusterPtr> new_all_version_and_cluster;

    for (const auto & version_key : versions_key)
    {
        String version_id = getPropertyOrChildValue<String>(configuration, config_prefix + "." + version_key, "id");
        bool readable = getPropertyOrChildValue<bool>(configuration, config_prefix + "." + version_key, "readable");
        bool writeable = getPropertyOrChildValue<bool>(configuration, config_prefix + "." + version_key, "writeable");

        new_all_version_and_cluster[version_id] = std::make_shared<DummyCluster>(configuration, config_prefix + "." + version_key, this,
                                                                                 settings, readable, writeable);
    }
    all_version_and_cluster = new_all_version_and_cluster;
}

void MultiplexedVersionCluster::setAddressAndConnections(
    Cluster::ShardInfo & shard_info, const Poco::Util::AbstractConfiguration & configuration,
    const std::string & replica_config_prefix, const Settings & settings, Cluster::Addresses & addresses)
{
    auto address = createAddress(configuration, replica_config_prefix);

    ConnectionPoolPtr connections;
    for (const auto & address_and_connection_pool : address_and_connection_pool_cache)
        if (addressEquals(address_and_connection_pool.first, address))
            connections = address_and_connection_pool.second;

    if (!connections)
    {
        /// CREATE new ConnectionPool
        connections = std::make_shared<ConnectionPool>(
            settings.distributed_connections_pool_size,
            address.host_name, address.port,
            address.default_database, address.user, address.password,
            ConnectionTimeouts::getTCPTimeoutsWithoutFailover(settings).getSaturated(settings.max_execution_time),
            "server", address.compression, address.secure);

        address_and_connection_pool_cache.emplace_back(std::pair(address, connections));
    }

    if (address.is_local)
        shard_info.local_addresses.emplace_back(address);

    addresses.emplace_back(address);
    shard_info.per_replica_pools.emplace_back(connections);
}

Cluster::Address MultiplexedVersionCluster::createAddress(const Poco::Util::AbstractConfiguration & configuration, const std::string & replica_config_prefix)
{
    Cluster::Address address;
    UInt16 clickhouse_port = static_cast<UInt16>(configuration.getInt("tcp_port", 0));
    address.host_name = getPropertyOrChildValue<String>(configuration, replica_config_prefix, "host");
    address.port = getPropertyOrChildValue<UInt16>(configuration, replica_config_prefix, "port");
    address.user = getPropertyOrChildValue<String>(configuration, replica_config_prefix, "user", "default");
    address.password = getPropertyOrChildValue<String>(configuration, replica_config_prefix, "password", "");
    address.secure = getPropertyOrChildValue<bool>(configuration, replica_config_prefix, "secure", false) ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    address.compression = getPropertyOrChildValue<bool>(configuration, replica_config_prefix, "compression", true) ? Protocol::Compression::Enable : Protocol::Compression::Disable;
    const auto initially_resolved_address = DNSResolver::instance().resolveAddress(address.host_name, address.port);
    address.is_local = isLocalAddress(initially_resolved_address, clickhouse_port);
    address.default_database = getPropertyOrChildValue<String>(configuration, replica_config_prefix, "default_database", "");

    return address;
}

template <typename T> T MultiplexedVersionCluster::getPropertyOrChildValue(
    const Poco::Util::AbstractConfiguration & configuration, const std::string & configuration_key, String property_or_child_name)
{
    if (configuration.has(configuration_key + "[@" + property_or_child_name + "]"))
        return MultiplexedVersionCluster::TypeToEnum<std::decay_t<T>>::getValue(configuration, configuration_key + "[@" + property_or_child_name + "]");

    return MultiplexedVersionCluster::TypeToEnum<std::decay_t<T>>::getValue(configuration, configuration_key + "." + property_or_child_name);
}

template <typename T> T MultiplexedVersionCluster::getPropertyOrChildValue(
    const Poco::Util::AbstractConfiguration & configuration, const std::string & configuration_key, String property_or_child_name,const T & default_value)
{
    if (configuration.has(configuration_key + "[@" + property_or_child_name + "]"))
        return MultiplexedVersionCluster::TypeToEnum<std::decay_t<T>>::getValue(configuration, configuration_key + "[@" + property_or_child_name + "]");

    if (configuration.has(configuration_key + "." + property_or_child_name))
        return MultiplexedVersionCluster::TypeToEnum<std::decay_t<T>>::getValue(configuration, configuration_key + "." + property_or_child_name);

    return default_value;
}

RWLockFIFO::LockHandler MultiplexedVersionCluster::getConfigurationLock()
{
    return configuration_lock->getLock(RWLockFIFO::Read, "getConfigurationLock");
}

ClusterPtr MultiplexedVersionCluster::getCluster(const DB::String &cluster_name)
{
    if (startsWith(cluster_name, "Cluster_"))
    {
        String version = String(cluster_name.data() + 8);
        if (all_version_and_cluster.count(version))
            return all_version_and_cluster[version];
    }

    return {};
}

DummyCluster::DummyCluster(const Poco::Util::AbstractConfiguration & configuration, const std::string & configuration_prefix,
                           MultiplexedVersionCluster *multiplexed_version_cluster, const Settings &settings, bool is_readable,
                           bool is_writeable)
    : is_readable(is_readable), is_writeable(is_writeable)
{
    Poco::Util::AbstractConfiguration::Keys shard_keys;
    configuration.keys(configuration_prefix, shard_keys);

    if (shard_keys.empty())
        throw Exception("No cluster elements (version) specified in config at path " + configuration_prefix, ErrorCodes::SHARD_HAS_NO_CONNECTIONS);

    size_t shard_idx = 0;
    for (const auto & shard_key : shard_keys)
    {
        if (!startsWith(shard_key, "Shard"))
            continue;

        shard_idx++;
        ShardInfo info;

        Poco::Util::AbstractConfiguration::Keys replica_keys;
        configuration.keys(configuration_prefix + "." + shard_key, replica_keys);

        Addresses addresses_pre_replica;
        if (replica_keys.empty())
            multiplexed_version_cluster->setAddressAndConnections(
                info, configuration, configuration_prefix + "." + shard_key, settings, addresses_pre_replica);
        else
        {
            for (const auto & replica_key : replica_keys)
            {
                if (startsWith(replica_key, "Replica"))
                    multiplexed_version_cluster->setAddressAndConnections(
                        info, configuration, configuration_prefix + "." + shard_key + "." + replica_key, settings, addresses_pre_replica);
            }
        }
        info.shard_num = UInt32(shard_idx);
        info.has_internal_replication = false;
        addresses.emplace_back(addresses_pre_replica);
        local_shard_count += info.isLocal() ? 1 : 0;
        remote_shard_count += info.isLocal() ? 0 : 1;
        info.weight = multiplexed_version_cluster->getPropertyOrChildValue<UInt32>(configuration, configuration_prefix + "." + shard_key, "weight", 1);
        info.pool = std::make_shared<ConnectionPoolWithFailover>(info.per_replica_pools, settings.load_balancing, settings.connections_with_failover_max_tries);
        shards_info.emplace_back(info);

        if (info.weight)
            slot_to_shard.insert(std::end(slot_to_shard), info.weight, shards_info.size() - 1);
    }

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
