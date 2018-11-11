//#pragma once
//
//#include <ext/shared_ptr_helper.h>
//
//#include <Storages/IStorage.h>
//#include <Common/SimpleIncrement.h>
//#include <Client/ConnectionPool.h>
//#include <Client/ConnectionPoolWithFailover.h>
//#include <Interpreters/Settings.h>
//#include <Interpreters/Cluster.h>
//#include <Interpreters/ExpressionActions.h>
//#include <Parsers/ASTFunction.h>
//#include <common/logger_useful.h>
//#include <Storages/MergeTree/MergeTreeSettings.h>
//#include <QingCloud/Interpreters/MultiplexedVersionCluster.h>
//#include <QingCloud/Datastream/QingCloudAsynchronism.h>
//
//namespace DB
//{
//
//class Context;
//class StorageDistributedDirectoryMonitor;
//
//using VersionWithReplica = std::pair<String, UInt64>;
//using VersionAndShardsWithStorage = std::map<VersionWithReplica, StoragePtr>;
//
//class StorageQingCloud : public ext::shared_ptr_helper<StorageQingCloud>, public IStorage
//{
//    friend class QingCloudBlockOutputStreamBck;
//
//public:
//    ~StorageQingCloud() override;
//
//    std::string getName() const override { return "QingCloud"; }
//
//    BlockInputStreams read(
//        const Names & column_names,
//        const SelectQueryInfo & query_info,
//        const Context & context,
//        QueryProcessingStage::Enum & processed_stage,
//        size_t max_block_size,
//        unsigned num_streams) override;
//
//    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;
//
//    void startup() override;
//    void shutdown() override;
//
//    std::string getTableName() const override;
//
//    void drop() override {}
//
//    void syncClusterConfig();
//
//    void mergeVersions(std::vector<String> from_versions, const String & to_version);
//
//private:
//    const String full_path;
//    const String database_name;
//    const String table_name;
//    Context & context;
//    const ASTPtr primary_expr_list;
//    const ASTPtr partition_expr_list;
//    const DB::MergeTreeSettings settings;
//    const ColumnsDescription columns;
//    QingCloudAsynchronism asynchronism;
//
//    VersionAndShardsWithStorage local_storages;
//    mutable RWLockFIFOPtr local_storage_lock = RWLockFIFO::create();
//
//    String sharding_key_column_name;
//    ExpressionActionsPtr sharding_key_expr;
//
//protected:
//    StorageQingCloud(const String &data_path, const String &database_name, const String &table_name, const ColumnsDescription &columns,
//                         bool attach, Context &context, ASTPtr &primary_expr_list, ASTPtr &partition_expr_list, DB::MergeTreeSettings &settings,
//                         bool has_force_restore_data_flag, ASTPtr &distribution_expr_list);
//
//    BlockInputStreams readLocal(const Names & column_names, const SelectQueryInfo & query_info, const Context & context,
//                                QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned int num_streams) const;
//
//    void destroyLocalStorage(const StoragePtr & local_storage);
//
//    StoragePtr createLocalStorage(const String & local_table_name, bool attach = false);
//
//    std::vector<std::pair<String, UInt64>> getLocalStoragesVersionAndShard(const MultiplexedClusterPtr &multiplexed_cluster);
//
//    void destroyLocalStorages(VersionAndShardsWithStorage &old_storages, VersionAndShardsWithStorage &new_storages);
//
//    void mergeStorage(StoragePtr &source_storage, StoragePtr &dist_storage);
//};
//
//}
