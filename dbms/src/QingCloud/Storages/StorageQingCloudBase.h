#pragma once

#include <Storages/IStorage.h>

namespace DB
{

using VersionAndStorage = std::map<String, StoragePtr>;
using VersionAndShardNumber = std::pair<String, UInt64>;
using VersionAndShardNumberWithStorage = std::map<VersionAndShardNumber, StoragePtr>;

class StorageQingCloudBase : public IStorage
{
public:
    StorageQingCloudBase(const ColumnsDescription &columns);

    bool isRemote() const override { return true; }
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }

    void drop() override;

    void truncate(const ASTPtr &truncate_query) override;

    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

    void clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context) override;

    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context) override;

    void dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & context) override;

    void attachPartition(const ASTPtr & partition, bool part, const Context & context) override;

    void fetchPartition(const ASTPtr & partition, const String & from, const Context & context) override;

    void freezePartition(const ASTPtr & partition, const String & with_name, const Context & context) override;

    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void mutate(const MutationCommands &commands, const Context &context) override;

    void startup() override;

    void shutdown() override;

    bool checkData() const override;

    bool hasColumn(const String &column_name) const override;


protected:
    VersionAndStorage version_distributed;
    VersionAndShardNumberWithStorage local_data_storage;
    RWLockFIFOPtr version_lock = RWLockFIFO::create();

};

}
