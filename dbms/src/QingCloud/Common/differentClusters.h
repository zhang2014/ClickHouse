#pragma once

#include <Interpreters/Cluster.h>
#include <ext/range.h>

namespace DB
{

std::map<String, Cluster::Address> differentClusters(const ClusterPtr & c1, const ClusterPtr & c2)
{
    std::map<String, Cluster::Address> res;

    Cluster::ShardsInfo c1_shards_info = c1->getShardsInfo();
    Cluster::ShardsInfo c2_shards_info = c2->getShardsInfo();
    size_t max_shard_size = std::max(c1_shards_info.size(), c2_shards_info.size());

    Cluster::AddressesWithFailover c1_shards_addresses = c1->getShardsAddresses();
    Cluster::AddressesWithFailover c2_shards_addresses = c2->getShardsAddresses();

    for (size_t shard_index : ext::range(0, max_shard_size))
    {
        Cluster::Addresses c1_shard_addresses = c1_shards_addresses[shard_index];
        Cluster::Addresses c2_shard_addresses = c1_shards_addresses[shard_index];

        if (c1_shard_addresses.size() != c2_shard_addresses.size())
            throw Exception("Missing Replica factor.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        for (size_t replica_index : ext::range(0, c1_shard_addresses.size()))
        {
            Cluster::Address c1_replica_address = c1_shard_addresses[replica_index];
            Cluster::Address c2_replica_address = c2_shard_addresses[replica_index];

            if (c1_replica_address.host_name != c2_replica_address.host_name || c1_replica_address.port != c2_replica_address.port)
            {
                res[c1_replica_address.toString()] = c1_replica_address;
                res[c2_replica_address.toString()] = c2_replica_address;
            }
        }
    }

    return res;
}


template <typename... Args>
std::map<String, ConnectionPoolPtr> getConnectionPoolsFromClusters(Args &&... args)
{
    std::map<String, ConnectionPoolPtr> connections;
    std::vector<ClusterPtr> clusters = { std::forward<Args>(args)... };

    for (const auto & cluster : clusters)
    {
        Cluster::ShardsInfo shards_info = cluster->getShardsInfo();
        Cluster::AddressesWithFailover addresses_with_failover = cluster->getShardsAddresses();

        for (size_t shard_index : ext::range(0, shards_info.size()))
        {
            Cluster::Addresses shard_addresses = addresses_with_failover[shard_index];
            ConnectionPoolPtrs replicas_connection = shards_info[shard_index].per_replica_pools;
            for (size_t replica_index : ext::range(0, shard_addresses.size()))
            {
                Cluster::Address replica_address = shard_addresses[replica_index];
                if (!connections.count(replica_address.toString()))
                    connections[replica_address.toString()] = replicas_connection[replica_index];
            }
        }
    }
    return connections;
}

}