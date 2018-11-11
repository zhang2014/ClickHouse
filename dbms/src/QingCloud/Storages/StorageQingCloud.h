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
//    ~StorageQingCloud() override;

    std::string getName() const override { return "QingCloud"; }

    StorageQingCloud(ASTCreateQuery & query, const String & data_path, const String & table_name, const String & database_name,
                     Context & local_context, Context & context, const ColumnsDescription & columns, bool attach, bool has_force_restore_data_flag);

    void startup() override;

    void shutdown() override;

    BlockInputStreams read(const Names & column_names, const SelectQueryInfo & query_info, const Context & context,
                           QueryProcessingStage::Enum & processed_stage, size_t max_block_size, unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

//    std::string getTableName() const override;

    void drop() override;

//    void mergeVersions(std::vector<String> from_versions, const String & to_version);

private:
    Context & context;
    const String data_path;
    const String table_name;
    const String database_name;
    const Context local_context;
    const ColumnsDescription columns;
    ASTCreateQuery create_query;

    VersionAndStorage version_distributed;
    VersionAndShardNumberWithStorage local_data_storage;
//    mutable RWLockFIFOPtr local_storage_lock = RWLockFIFO::create();

protected:
//    void mergeStorage(StoragePtr &source_storage, StoragePtr &dist_storage);
};
}
