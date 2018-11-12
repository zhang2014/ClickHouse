#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Common/SimpleIncrement.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <QingCloud/Interpreters/MultiplexedVersionCluster.h>
#include <QingCloud/Datastream/QingCloudAsynchronism.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

class Context;
class StorageDistributedDirectoryMonitor;

using VersionAndStorage = std::map<String, StoragePtr>;
using VersionAndShardNumber = std::pair<String, UInt64>;
using VersionAndShardNumberWithStorage = std::map<VersionAndShardNumber, StoragePtr>;

class StorageQingCloud : public ext::shared_ptr_helper<StorageQingCloud>, public IStorage
{
public:
    ~StorageQingCloud() override;

    std::string getName() const override { return "QingCloud"; }

    bool isRemote() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return true; }

    StorageQingCloud(ASTCreateQuery & query, const String & data_path, const String & table_name, const String & database_name,
                     Context & local_context, Context & context, const ColumnsDescription & columns, bool attach, bool has_force_restore_data_flag);

    void startup() override;

    void shutdown() override;

    std::string getTableName() const override {return table_name;}

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    BlockInputStreams read(const Names & column_names, const SelectQueryInfo & query_info, const Context & context,
                           QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned num_streams) override;

    void attachVersion(const String & attach_version, const Context & context);

    void detachVersion(const String & detach_version, const Context & context);

    void mergeVersions(std::vector<String> /*from_versions*/, const String & /*to_version*/, std::vector<UInt64> /*shard_numbers*/){}

    void drop() override;

    bool checkData() const override;

    void truncate(const ASTPtr &truncate_query) override;

    bool hasColumn(const String &column_name) const override;

    void mutate(const MutationCommands &commands, const Context &context) override;

    void attachPartition(const ASTPtr & partition, bool part, const Context & context) override;

    void fetchPartition(const ASTPtr & partition, const String & from, const Context & context) override;

    void freezePartition(const ASTPtr & partition, const String & with_name, const Context & context) override;

    void dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & context) override;

    void clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context) override;

    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context) override;

private:
    Context & context;
    const String data_path;
    const String table_name;
    const String database_name;
    Context local_context;
    const ColumnsDescription columns;
    ASTCreateQuery create_query;

    VersionAndStorage version_distributed;
    VersionAndShardNumberWithStorage local_data_storage;

    ASTPtr sharding_key;
};
}
