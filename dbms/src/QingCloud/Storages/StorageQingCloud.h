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
#include <Parsers/ASTCreateQuery.h>
#include "StorageQingCloudBase.h"
#include "VersionInfo.h"

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
                           QueryProcessingStage::Enum processed_stage, size_t max_block_size, unsigned num_streams) override;

    void flushVersionData(const String & version);

    void cleanupBeforeMigrate(const String & cleanup_version);

    void initializeVersions(std::initializer_list<String> versions);

    void deleteOutdatedVersions(std::initializer_list<String> delete_versions);

    void initializeVersionInfo(std::initializer_list<String> readable_versions, const String & writable_version);

    void migrateDataBetweenVersions(const String &origin_version, const String &upgrade_version, bool move);

private:
    Context & context;
    const String data_path;
    const String table_name;
    const String database_name;
    Context local_context;
    const ColumnsDescription columns;
    ASTCreateQuery create_query;

private:
    ASTPtr sharding_key;
    String table_data_path;
    VersionInfo version_info;

    void migrateDataInLocal(bool move, const StoragePtr & origin_, const StoragePtr & upgrade_storage_);

    void migrateDataInCluster(const String & origin_version, const String & upgrade_version, size_t shard_number);

    void createTablesWithCluster(const String & version, const ClusterPtr & cluster, bool attach = false, bool has_force_restore_data_flag = false);

};

}
