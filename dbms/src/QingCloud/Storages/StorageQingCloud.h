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
#include "StorageQingCloudBase.h"

namespace DB
{

class Context;

class StorageQingCloud : public ext::shared_ptr_helper<StorageQingCloud>, public StorageQingCloudBase
{
public:
    StorageQingCloud(ASTCreateQuery &query, const String &data_path, const String &table_name, const String &database_name,
                     Context &local_context, Context &context, const ColumnsDescription &columns, bool attach,
                     bool has_force_restore_data_flag);

    ~StorageQingCloud() override;

    std::string getName() const override { return "QingCloud"; }

    std::string getTableName() const override {return table_name;}

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    BlockInputStreams read(const Names & column_names, const SelectQueryInfo & query_info, const Context & context,
                           QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned num_streams) override;

    void mergeVersions(std::vector<String> /*from_versions*/, const String & /*to_version*/, std::vector<UInt64> /*shard_numbers*/){}

    void upgradeVersion(const String & from_version, const String & upgrade_version);


private:
    Context & context;
    const String data_path;
    const String table_name;
    const String database_name;
    Context local_context;
    const ColumnsDescription columns;
    ASTCreateQuery create_query;

    struct VersionInfo
    {
        String data_path;
        String version_path;
        String writeable_version;
        std::vector<String> readable_versions;
        std::vector<String> local_store_versions;

        void loadVersionInfo();

        void storeVersionInfo();

        VersionInfo(const String &data_path, MultiplexedClusterPtr multiplexed_version_cluster);
    };

    String table_data_path;
    VersionInfo version_info;

    ASTPtr sharding_key;

    std::map<String, String> expect_addresses;
    std::map<String, String> complete_addresses;

    template <typename... Args>
    void waitActionInClusters(const String & action_in_version, const String & action_name, Args &&... args);

    void createTablesWithCluster(const String & version, const ClusterPtr & cluster, bool attach = false, bool has_force_restore_data_flag = false);

    void migrateDataForFromVersion(const String &from_version, const ClusterPtr &from_cluster, const String &upgrade_version, const ClusterPtr &upgrade_cluster);
};

}
