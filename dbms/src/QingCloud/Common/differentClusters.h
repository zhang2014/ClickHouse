#pragma once

#include <Interpreters/Cluster.h>
#include <ext/range.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

std::map<size_t, Cluster::Address> differentClusters(const ClusterPtr & c1, const ClusterPtr & c2)
{
    std::map<size_t, Cluster::Address> diff_res;

    Cluster::ShardsInfo c1_shards_info = c1->getShardsInfo();
    Cluster::ShardsInfo c2_shards_info = c2->getShardsInfo();

    Cluster::AddressesWithFailover c1_shards_addresses = c1->getShardsAddresses();
    Cluster::AddressesWithFailover c2_shards_addresses = c2->getShardsAddresses();

    for (size_t shard_index : ext::range(0, std::min(c1_shards_info.size(), c2_shards_info.size())))
    {
        Cluster::Addresses c1_shard_addresses = c1_shards_addresses[shard_index];
        Cluster::Addresses c2_shard_addresses = c1_shards_addresses[shard_index];

        if (c1_shard_addresses.size() != c2_shard_addresses.size())
            throw Exception("Missing match replica factor.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        for (size_t replica_index : ext::range(0, c1_shard_addresses.size()))
        {
            Cluster::Address c1_replica_address = c1_shard_addresses[replica_index];
            Cluster::Address c2_replica_address = c2_shard_addresses[replica_index];

            if (c1_replica_address.host_name != c2_replica_address.host_name || c1_replica_address.port != c2_replica_address.port)
                diff_res[shard_index + 1] = c1_shard_addresses[0];
        }
    }
    return diff_res;
}


std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> getConnectionPoolsFromClusters(const ClusterPtr & cluster)
{
    std::map<String, size_t> exists;
    std::vector<std::pair<Cluster::Address, ConnectionPoolPtr>> connections;

    Cluster::ShardsInfo shards_info = cluster->getShardsInfo();
    Cluster::AddressesWithFailover addresses_with_failover = cluster->getShardsAddresses();

    for (size_t shard_index : ext::range(0, shards_info.size()))
    {
        Cluster::Addresses shard_addresses = addresses_with_failover[shard_index];
        ConnectionPoolPtrs replicas_connection = shards_info[shard_index].per_replica_pools;
        for (size_t replica_index : ext::range(0, shard_addresses.size()))
        {
            Cluster::Address replica_address = shard_addresses[replica_index];
            if (!exists.count(replica_address.toString()))
                connections.emplace_back(std::pair(replica_address, replicas_connection[replica_index]));
        }
    }

    return connections;
}

ASTPtr makeInsertQueryForSelf(const String & database_name, const String & table_name, const NamesAndTypesList & columns)
{
    const auto insert_query = std::make_shared<ASTInsertQuery>();
    const auto select_query = std::make_shared<ASTSelectQuery>();
    const auto select_expression_list = std::make_shared<ASTExpressionList>();
    const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();

    insert_query->table = table_name;
    insert_query->database = database_name;
    insert_query->select = select_with_union_query;

    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->list_of_selects->children.push_back(select_query);

    select_query->select_expression_list = select_expression_list;
    select_query->replaceDatabaseAndTable(database_name, table_name);
    select_query->children.emplace_back(select_query->select_expression_list);

    /// manually substitute column names in place of asterisk
    for (const auto & column : columns)
        select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

    return insert_query;
}

}