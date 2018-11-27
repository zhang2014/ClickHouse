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


namespace DB
{

MultiplexedVersionCluster::MultiplexedVersionCluster(
    const Poco::Util::AbstractConfiguration & configuration, const Settings & settings, const std::string & config_prefix)
{
    updateMultiplexedVersionCluster(configuration, settings, config_prefix);
}


void MultiplexedVersionCluster::updateMultiplexedVersionCluster(
    const Poco::Util::AbstractConfiguration & configuration, const Settings & settings, const std::string & config_prefix)
{
    all_version_and_cluster.clear();

    visitorConfigurationKey(configuration, config_prefix, "VERSION", [&, this](const String & version_key) {
        const String version_id = getPropertyOrChildValue<String>(configuration, version_key, "id");
        all_version_and_cluster[version_id] = std::make_shared<DummyCluster>(configuration,  version_key, this, settings);
    });

    /// TODO: 从文件中加载
    default_version = !default_version.empty() ? default_version : getPropertyOrChildValue<String>(configuration, config_prefix + ".VERSION[0]", "id");
    /// TODO: clean up old connections
}

String MultiplexedVersionCluster::getCurrentVersion()
{
    return default_version;
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

ClusterPtr MultiplexedVersionCluster::getCluster(const DB::String & cluster_name)
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
    shard_info.per_replica_pools = std::move(shard_connections);
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
