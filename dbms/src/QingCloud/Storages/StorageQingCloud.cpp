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
#include "StorageQingCloud.h"
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


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ENGINE_REQUIRED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
        createTablesWithCluster(version, context.getCluster("Cluster_" + version), attach, has_force_restore_data_flag);

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

    if (unlikely(writing_version.empty() && writing_shard_number))
        throw Exception("Missing Version settings.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    else if (unlikely(!writing_version.empty() && !version_distributed.count(writing_version)))
        throw Exception("Illegal Version " + writing_version + ", because not found version.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (!writing_version.empty() && !writing_shard_number)
        return version_distributed[writing_version]->write(query, settings);
    else if (!writing_version.empty() && writing_shard_number)
        return local_data_storage[std::pair(writing_version, writing_shard_number)]->write(query, settings);
    else if (writing_version.empty() && !writing_shard_number)
    {
        /// TODO Read and write lock
        Settings query_settings = settings;
        query_settings.writing_version = version_info.writeable_version;
        return version_distributed[version_info.writeable_version]->write(query, query_settings);
    }
}

BlockInputStreams StorageQingCloud::read(const Names & column_names, const SelectQueryInfo &query_info, const Context &context,
                                         QueryProcessingStage::Enum &processed_stage, size_t max_block_size, unsigned num_streams)
{
    Settings settings = context.getSettingsRef();
    const String query_version = settings.query_version;
    const UInt64 query_shard_number = settings.query_shard_index;

    if (query_version.empty() && query_shard_number)
        throw Exception("Missing Version settings.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    else if (!query_version.empty() && !version_distributed.count(query_version))
        throw Exception("Illegal Version " + query_version + ", because not found version.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);


    if (!query_version.empty() && !query_shard_number)
        return version_distributed[query_version]->read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
    else if (!query_version.empty() && query_shard_number)
        return local_data_storage[std::pair(query_version, query_shard_number)]->read(column_names, query_info, context, processed_stage,
                                                                                      max_block_size, num_streams);
    else if (query_version.empty() && !query_shard_number)
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

void StorageQingCloud::upgradeVersion(const String & from_version, const String & upgrade_version)
{
    ClusterPtr from_cluster = context.getCluster("Cluster_" + from_version);
    ClusterPtr upgrade_cluster = context.getCluster("Cluster_" + upgrade_version);

    /// CREATE tmp_version and upgrade_version
    const String tmp_version = "tmp_" + upgrade_version;
    createTablesWithCluster(tmp_version, upgrade_cluster);
    createTablesWithCluster(upgrade_version, upgrade_cluster);
    waitActionInClusters(upgrade_version, "CREATE_UPGRADE_VERSION", from_cluster, upgrade_cluster);

    {
        /// TODO: LOCK
        version_info.writeable_version = tmp_version;
        version_info.readable_versions.emplace_back(tmp_version);
        version_info.local_store_versions.emplace_back(tmp_version);
        version_info.local_store_versions.emplace_back(upgrade_version);
        version_info.storeVersionInfo();
    }

    migrateDataForFromVersion(from_version, from_cluster, upgrade_version, upgrade_cluster);
}

template <typename... Args>
void StorageQingCloud::waitActionInClusters(const String & action_in_version, const String & action_name, Args &&... args)
{
    std::map<String, ConnectionPoolPtr> addresses_with_connections = getConnectionPoolsFromClusters(args...);

    const Settings settings = context.getSettingsRef();
    const String query_string = "QINGCLOUD ACTION NOTIFY " + action_name + " FROM VERSION  "+ action_in_version;

    /// TODO: 告诉其他节点我完成了这个Action
    BlockInputStreams streams;
    for (const auto & address_with_connections : addresses_with_connections)
    {
        ConnectionPoolPtrs failover_connections;
        failover_connections.emplace_back(address_with_connections.second);

        ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
            failover_connections, SettingLoadBalancing(LoadBalancing::RANDOM), settings.connections_with_failover_max_tries);

        streams.emplace_back(std::make_shared<RemoteBlockInputStream>(shard_pool, query_string, Block{}, context));
    }

    BlockInputStreamPtr union_stream = std::make_shared<UnionBlockInputStream<>>(streams, nullptr, streams.size());
    std::make_shared<SquashingBlockInputStream>(union_stream, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())->read();

    /// TODO: 先查看其他节点是否已经全部完成了
    /// TODO: 等待其他节点告诉我完成了这个Action, 如果超时报错 人工介入解决?
}

void StorageQingCloud::createTablesWithCluster(const String & version, const ClusterPtr & cluster, bool attach, bool has_force_restore_data_flag)
{
    String version_table_data_path = table_data_path + version + "/";

    Poco::File(version_table_data_path).createDirectory();
    version_distributed[version] = StorageDistributed::create(database_name, "Distributed_" + table_name, columns, database_name,
                                                              table_name, "Cluster_" + version, context, sharding_key,
                                                              version_table_data_path, attach);

    /// TODO: 有些存储是不存储数据的 所以我们可以只创建一个
    Cluster::AddressesWithFailover shards_addresses = cluster->getShardsAddresses();

    for (size_t shard_number : ext::range(0, shards_addresses.size()))
    {
        for (Cluster::Address & replica_address : shards_addresses[shard_number])
        {
            if (replica_address.is_local)
                local_data_storage[std::pair(version, shard_number + 1)] = StorageFactory::instance().get(
                    true, create_query, version_table_data_path, "Shard_" + toString(shard_number + 1) + "_" + table_name, database_name,
                    local_context, context, columns, attach, has_force_restore_data_flag);
        }
    }
}

void StorageQingCloud::migrateDataForFromVersion(
    const String &from_version, const ClusterPtr & from_cluster, const String & upgrade_version, const ClusterPtr & upgrade_cluster)
{
    /// TODO: 等待from_version的数据下刷完毕
    waitActionInClusters(from_version, "FLUSHED_DISTRIBUTED_DATA", from_cluster);
    std::map<String, Cluster::Address> diff_addresses = differentClusters(from_cluster, upgrade_cluster);

    auto rebalancing_data = [&, from_version, from_cluster](const Cluster::Address & address)
    {
        Cluster::ShardsInfo shards_info = from_cluster->getShardsInfo();
        Cluster::AddressesWithFailover shards_addresses = from_cluster->getShardsAddresses();

        for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
        {
            Cluster::Address leader_address = shards_addresses[shard_index][0];
            if (leader_address.host_name == address.host_name && leader_address.port == address.port)
            {
                Context query_context = context;

                query_context.getSettingsRef().query_version = from_version;
                query_context.getSettingsRef().writing_version = upgrade_version;
                query_context.getSettingsRef().query_shard_index = shards_info[shard_index].shard_num;

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
                for (const auto & column : version_distributed[from_version]->getColumns().ordinary)
                    select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

                InterpreterSelectQuery interpreter_insert(insert_query, query_context);
                interpreter_insert.execute();
            }
        }
    };

    for (const auto & str_addresses_with_address : diff_addresses)
        if (str_addresses_with_address.second.is_local)
            rebalancing_data(str_addresses_with_address.second);

    /// TODO: 等待upgrade_version的数据下刷完毕
    waitActionInClusters(upgrade_version, "FLUSHED_DISTRIBUTED_DATA", upgrade_cluster);

    {
        /// TODO: LOCK
        version_info.writeable_version = upgrade_version;
        version_info.readable_versions.clear();
        version_info.readable_versions.emplace_back(upgrade_version);
        version_info.readable_versions.emplace_back("tmp_" + upgrade_version);
//        version_info.local_store_versions.emplace_back(tmp_version);
//        version_info.local_store_versions.emplace_back(upgrade_version);
        version_info.storeVersionInfo();
    }
    /// TODO: 合并 from_version、tmp_upgrade_version、upgrade_version
    /// TODO: 等待集群的合并版本完毕
    /// TODO: 删除 from_version、tmp_upgrade_version
    /// TODO: 完成本次扩容工作
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

}