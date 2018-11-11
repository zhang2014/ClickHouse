//#include <Parsers/ASTCreateQuery.h>
//#include <QingCloud/Storages/StorageQingCloud.h>
//#include <Storages/StorageFactory.h>
//#include <Storages/StorageMergeTree.h>
//#include <ext/range.h>
//#include <Interpreters/ClusterProxy/executeQuery.h>
//#include <QingCloud/Interpreters/ClusterProxy/QingCloudSelectStreamFactory.h>
//#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
//#include <DataStreams/materializeBlock.h>
//#include <Interpreters/InterpreterSelectQuery.h>
//#include <IO/Operators.h>


//#include <Poco/DirectoryIterator.h>
//#include <Parsers/ASTPartition.h>
//#include <QingCloud/Datastream/QingCloudBlockOutputStream.h>
//
//
//namespace DB
//{
//
//namespace ErrorCodes
//{
//    extern const int BAD_ARGUMENTS;
//}
//
//static ASTPtr extractKeyExpressionList(IAST & node)
//{
//    const ASTFunction * expr_func = typeid_cast<const ASTFunction *>(&node);
//
//    if (expr_func && expr_func->name == "tuple")
//    {
//        /// Primary key is specified in tuple.
//        return expr_func->children.at(0);
//    }
//    else
//    {
//        /// Primary key consists of one column.
//        auto res = std::make_shared<ASTExpressionList>();
//        res->children.push_back(node.ptr());
//        return res;
//    }
//}
//
//static std::pair<String, UInt64> readVersionAndShardText(const String &full_text)
//{
//    const char *full_name = full_text.data();
//    const char *writing_version_end = strchr(full_name, '_');
//    const char *writing_shared_number_end = strchr(writing_version_end + 1, '_');
//    String version = parse<String>(full_name, writing_version_end - full_name);
//    UInt64 shared_number = parse<UInt64>(writing_version_end + 1, writing_shared_number_end - writing_version_end);
//    return std::pair(version, shared_number);
//};
//
//void StorageQingCloud::destroyLocalStorage(const StoragePtr & local_storage)
//{
//    local_storage->shutdown();
//    /// If table was already dropped by anyone, an exception will be thrown
//    auto table_lock = local_storage->lockDataForAlter(__PRETTY_FUNCTION__);
//    /// Delete table data
//    local_storage->drop();
//    local_storage->is_dropped = true;
//
//    String table_data_path = full_path + escapeForFileName(table_name) + '/' + escapeForFileName(local_storage->getTableName());
//
//    if (Poco::File(table_data_path).exists())
//        Poco::File(table_data_path).remove(true);
//}
//
//void StorageQingCloud::destroyLocalStorages(VersionAndShardsWithStorage &old_storages, VersionAndShardsWithStorage &new_storages)
//{
//    for (const auto & version_with_storage : old_storages)
//    {
//        if (!new_storages.count(version_with_storage.first))
//            destroyLocalStorage(version_with_storage.second);
//    }
//}
//
//StoragePtr StorageQingCloud::createLocalStorage(const String & local_table_name, bool attach)
//{
//    String date_column_name;
//    ASTPtr sampling_expression;
//    ASTPtr secondary_sorting_expr_list;
//    MergeTreeData::MergingParams merging_params;
//    merging_params.mode = MergeTreeData::MergingParams::Ordinary;
//    String table_data_path = full_path + escapeForFileName(table_name) + '/';
//
//    return StorageMergeTree::create(
//        table_data_path, database_name, local_table_name, columns, attach, context, primary_expr_list, secondary_sorting_expr_list,
//        date_column_name, partition_expr_list, sampling_expression, merging_params, settings, false
//    );
//}
//
//void StorageQingCloud::syncClusterConfig()
//{
//    VersionAndShardsWithStorage new_local_storages;
//    VersionAndShardsWithStorage old_local_storages = local_storages;
//
//    {
//        const auto lock = local_storage_lock->getLock(RWLockFIFO::Write);
//        const auto multiplexed_version = context.getMultiplexedVersion();
//
//        for (const auto version_and_shard : getLocalStoragesVersionAndShard(multiplexed_version))
//        {
//            if (local_storages.count(version_and_shard))
//                new_local_storages[version_and_shard] = local_storages[version_and_shard];
//            else
//            {
//                const auto version = version_and_shard.first;
//                const auto shard_index = version_and_shard.second;
//
//                WriteBufferFromOwnString local_table_name;
//                local_table_name << version << "_" << shard_index << "_" << table_name;
//                new_local_storages[version_and_shard] = createLocalStorage(local_table_name.str(), true);
//            }
//        }
//
//        local_storages = new_local_storages;
//    }
//
//    destroyLocalStorages(old_local_storages, new_local_storages);
//}
//
//std::string StorageQingCloud::getTableName() const
//{
//    return table_name;
//}
//
//void StorageQingCloud::startup()
//{
//    /// Prevent local_storages changes.
//    const auto lock = local_storage_lock->getLock(RWLockFIFO::Read);
//
//    for (auto & storage : local_storages)
//        storage.second->startup();
//}
//
//void StorageQingCloud::shutdown()
//{
//    /// Prevent local_storages changes.
//    const auto lock = local_storage_lock->getLock(RWLockFIFO::Read);
//
//    for (auto & storage : local_storages)
//        storage.second->shutdown();
//}
//
//BlockInputStreams StorageQingCloud::read(
//    const Names & column_names, const SelectQueryInfo & query_info, const Context & context,
//    QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned num_streams)
//{
//    BlockInputStreams res;
//
//    const Settings & settings = context.getSettingsRef();
//    const auto multiplexed_version_cluster = context.getMultiplexedVersion();
//
//    const auto lock = multiplexed_version_cluster->getConfigurationLock();
//    const auto lock2 = local_storage_lock->getLock(RWLockFIFO::Read);
//
//    UInt64 query_shard_number = UInt64(settings.query_shard_index);
//    auto all_versions_cluster = multiplexed_version_cluster->getAllVersionsCluster();
//    std::vector<String> query_versions = {settings.query_version.toString()};
//    if (settings.query_version.toString().empty())
//        query_versions = multiplexed_version_cluster->getReadableVersions();
//
//    for (String query_version : query_versions)
//    {
//        BlockInputStreams current_version_res;
//        const auto version_and_shard = std::pair(query_version, query_shard_number);
//
//        if (local_storages.count(version_and_shard))
//        {
//            current_version_res = readLocal(column_names, query_info, context, processed_stage, max_block_size, num_streams);
//        }
//        else
//        {
//            ClusterPtr query_cluster = all_versions_cluster.at(query_version);
//            ClusterProxy::QingCloudSelectStreamFactory stream_factory = ClusterProxy::QingCloudSelectStreamFactory(
//                processed_stage, QualifiedTableName{database_name, table_name}, context.getExternalTables(), query_version, query_shard_number);
//
//            current_version_res = ClusterProxy::executeQuery(stream_factory, query_cluster, query_info.query, context, settings);
//        }
//
//        if (!current_version_res.empty())
//            res.insert(res.end(), current_version_res.begin(), current_version_res.end());
//    }
//
//    return res;
//}
//
//BlockInputStreams StorageQingCloud::readLocal(
//    const Names & column_names, const SelectQueryInfo & query_info, const Context & context,
//    QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned int num_streams) const
//{
//    Settings settings = context.getSettingsRef();
//
//    const auto version_and_shard_index = std::pair(settings.query_version.toString(), UInt64(settings.query_shard_index));
//
//    return local_storages.at(version_and_shard_index)->read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
//}
//
//BlockOutputStreamPtr StorageQingCloud::write(const ASTPtr & query, const Settings & settings)
//{
//    const auto multiplexed_version = context.getMultiplexedVersion();
//
//    const auto lock = multiplexed_version->getConfigurationLock();
//    const auto lock2 = local_storage_lock->getLock(RWLockFIFO::Read);
//
//    auto writing_shard_number = UInt64(settings.writing_shard_index);
//    auto all_version_cluster = multiplexed_version->getAllVersionsCluster();
//    String writing_version = settings.writing_version.toString();
//    if (writing_version.empty())
//        writing_version = multiplexed_version->getCurrentWritingVersion();
//
//    if (likely(writing_shard_number))
//    {
//        if (likely(local_storages.count(std::pair(writing_version, writing_shard_number))))
//        {
//            StoragePtr storage = local_storages.at(std::pair(writing_version, writing_shard_number));
//            return storage->write(query, settings);
//        }
//    }
//
//    return std::make_shared<QingCloudBlockOutputStream>(context, asynchronism, query, all_version_cluster.at(writing_version),
//                                                        settings, settings.insert_distributed_sync, settings.insert_distributed_timeout,
//                                                        sharding_key_column_name, sharding_key_expr, getSampleBlock(), writing_version);
//}
//
//StorageQingCloud::~StorageQingCloud() = default;
//
//StorageQingCloud::StorageQingCloud(const String &data_path, const String &database_name, const String &table_name,
//                                   const ColumnsDescription &columns, bool /*attach*/, Context &context, ASTPtr &primary_expr_list,
//                                   ASTPtr &partition_expr_list, DB::MergeTreeSettings &settings, bool /*has_force_restore_data_flag*/,
//                                   ASTPtr &distribution_expr_list)
//    : IStorage{columns}, full_path(data_path), database_name(database_name), table_name(table_name), context(context),
//      primary_expr_list(primary_expr_list), partition_expr_list(partition_expr_list), settings(settings), columns(columns),
//      asynchronism(full_path + escapeForFileName(table_name) +"/" + "transmission/", context)
//{
//    String table_data_path = data_path + escapeForFileName(table_name) + '/';
//    Poco::File(table_data_path).createDirectory();
//
//    Poco::DirectoryIterator dir_end;
//    for (Poco::DirectoryIterator dir_it(table_data_path); dir_it != dir_end; ++dir_it)
//    {
//        if (dir_it->isDirectory() && dir_it.name() != "transmission")
//        {
//            String local_table_name = unescapeForFileName(dir_it.path().getFileName());
//            const auto version_and_shared_number = readVersionAndShardText(local_table_name);
//
//            local_storages[version_and_shared_number] = createLocalStorage(local_table_name, true);
//        }
//    }
//
//    syncClusterConfig();
//    sharding_key_column_name = distribution_expr_list ? distribution_expr_list->getColumnName() : String{};
//    sharding_key_expr = distribution_expr_list ? ExpressionAnalyzer(distribution_expr_list, context, nullptr, getColumns().getAllPhysical()).getActions(false) : nullptr;
//}
//
//std::vector<std::pair<String, UInt64>> StorageQingCloud::getLocalStoragesVersionAndShard(const MultiplexedClusterPtr & multiplexed_cluster)
//{
//    std::vector<std::pair<String, UInt64>> versions_and_shards;
//
//    for (const auto & version_and_cluster : multiplexed_cluster->getAllVersionsCluster())
//    {
//        String version = version_and_cluster.first;
//        ClusterPtr cluster = version_and_cluster.second;
//
//        Cluster::AddressesWithFailover shards_addresses = cluster->getShardsAddresses();
//
//        for (size_t shard_index : ext::range(0, shards_addresses.size()))
//        {
//            for (Cluster::Address & replica_address : shards_addresses[shard_index])
//            {
//                if (replica_address.is_local)
//                    versions_and_shards.emplace_back(version, shard_index + 1);
//            }
//        }
//    }
//    return versions_and_shards;
//}
//
//static std::vector<UInt64> getVersionShardsNumber(VersionAndShardsWithStorage storages, const String & version)
//{
//    std::vector<UInt64> shards_number;
//
//    for (const auto & version_and_storage : storages)
//    {
//        const auto version_and_shard_number = version_and_storage.first;
//
//        if (version_and_shard_number.first == version)
//            shards_number.emplace_back(version_and_shard_number.second);
//    }
//    return shards_number;
//}
//
//void StorageQingCloud::mergeVersions(std::vector<String> from_versions, const String & to_version)
//{
//    const auto multiplexed_version = context.getMultiplexedVersion();
//    const auto lock = local_storage_lock->getLock(RWLockFIFO::Read);
//    const auto configuration_lock = multiplexed_version->getConfigurationLock();
//
//    const auto dist_shards_number = getVersionShardsNumber(local_storages, to_version);
//
//    for (const auto source_version : from_versions)
//    {
//        if (source_version != to_version)
//        {
//            /// TODO check source_shards_number match dist_shards_number
//            const auto source_shards_number = getVersionShardsNumber(local_storages, source_version);
//
//            if (source_shards_number.size() != dist_shards_number.size())
//                throw Exception("Source Version miss match to version shards info.", ErrorCodes::BAD_ARGUMENTS);
//
//            for (const auto source_shard_number : source_shards_number)
//            {
//                StoragePtr dist_storage = local_storages[std::pair(to_version, source_shard_number)];
//                StoragePtr source_storage = local_storages[std::pair(source_version, source_shard_number)];
//
//                mergeStorage(source_storage, dist_storage);
//
//            }
//        }
//    }
//}
//
//void StorageQingCloud::mergeStorage(StoragePtr &source_storage, StoragePtr &dist_storage)
//{
//    std::unordered_set<String> partition_ids;
//
//    while(true)
//    {
//        partition_ids.clear();
//        StorageMergeTree * storage_merge_tree = static_cast<StorageMergeTree *>(source_storage.get());
//        MergeTreeData::DataPartsVector data_parts = storage_merge_tree->data.getDataPartsVector({MergeTreeDataPart::State::Committed});
//        for (const MergeTreeData::DataPartPtr & part : data_parts)
//            partition_ids.emplace(part->info.partition_id);
//
//
//        if (partition_ids.empty())
//            break;
//
//        for (const auto partition_id : partition_ids)
//        {
//            auto ast_partition = std::make_shared<ASTPartition>();
//            ast_partition->id = partition_id;
//
//            auto storage_lock = lockStructureForAlter(__PRETTY_FUNCTION__);
//            dist_storage->replacePartitionFrom(source_storage, ast_partition, false, context);
//            source_storage->dropPartition({}, ast_partition, false, context);
//        }
//    }
//}
//
//void registerStorageQingCloud(StorageFactory & factory)
//{
//    factory.registerStorage("QingCloud", [](const StorageFactory::Arguments & arguments)
//    {
//        if (!arguments.storage_def->partition_by || !arguments.storage_def->order_by)
//            throw Exception("Unable to create the QingCloud engine table.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
//
//        ASTPtr primary_expr_list;
//        ASTPtr partition_expr_list;
//        ASTPtr distributed_expr_list;
//        MergeTreeSettings storage_settings = arguments.context.getMergeTreeSettings();
//
//        if (arguments.storage_def->distributed_by)
//            distributed_expr_list = arguments.storage_def->distributed_by->ptr();
//
//        if (arguments.storage_def->partition_by)
//            partition_expr_list = extractKeyExpressionList(*arguments.storage_def->partition_by);
//
//        if (arguments.storage_def->order_by)
//            primary_expr_list = extractKeyExpressionList(*arguments.storage_def->order_by);
//
//        if (arguments.storage_def->settings)
//            storage_settings.loadFromQuery(*arguments.storage_def);
//
//        return StorageQingCloud::create(
//            arguments.data_path, arguments.database_name, arguments.table_name, arguments.columns, arguments.attach,
//            arguments.context, primary_expr_list, partition_expr_list, storage_settings, arguments.has_force_restore_data_flag,
//            distributed_expr_list
//        );
//    });
//}
//
//}
