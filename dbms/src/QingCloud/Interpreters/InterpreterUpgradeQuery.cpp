#include <QingCloud/Interpreters/InterpreterUpgradeQuery.h>
#include <QingCloud/Parsers/ASTUpgradeQuery.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <QingCloud/Storages/StorageQingCloud.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/IDatabase.h>
#include <QingCloud/Interpreters/SafetyPointFactory.h>
#include <QingCloud/Common/differentClusters.h>


namespace DB
{

InterpreterUpgradeQuery::InterpreterUpgradeQuery(const ASTPtr & node, const Context &context) : node(node), context(context)
{
}

BlockIO InterpreterUpgradeQuery::execute()
{
    BlockIO res;
    ASTUpgradeQuery * upgrade_query = typeid_cast<ASTUpgradeQuery *>(node.get());

    String & origin_version = upgrade_query->origin_version;
    String & upgrade_version = upgrade_query->upgrade_version;
    const String sync_name = "UPGRADE_VERSION_" + origin_version + "_" + upgrade_version;

    const auto origin_cluster = context.getCluster("Cluster_" + origin_version);
    const auto upgrade_cluster = context.getCluster("Cluster_" + upgrade_version);
    const auto two_cluster_connections = getConnectionPoolsFromClusters({origin_cluster, upgrade_cluster});
    const auto two_cluster_safety_point_sync = SafetyPointFactory::instance().createSafetyPoint(sync_name, context, two_cluster_connections);

    try
    {
        upgradeVersionForPaxos(upgrade_query, two_cluster_safety_point_sync);
        const auto upgrade_storage = selectAllUpgradeStorage(origin_version, upgrade_version);
        res.in = std::make_shared<UpgradeQueryBlockInputStream>(two_cluster_safety_point_sync, upgrade_storage, origin_version, upgrade_version);
        return res;    
    }
    catch(...)
    {
        SafetyPointFactory::instance().releaseSafetyPoint(sync_name);
        throw;
    }
    
}

void InterpreterUpgradeQuery::upgradeVersionForPaxos(ASTUpgradeQuery * upgrade_query, const SafetyPointWithClusterPtr & safety_point_sync)
{
    context.getDDLSynchronism()->withLockPaxos([&safety_point_sync, this, &upgrade_query]()
    {
        safety_point_sync->broadcastSync("LOCK_NEW_PAXOS_QUERIES", 2);
        context.getDDLSynchronism()->wakeupLearner();
        safety_point_sync->broadcastSync("LOCK_PAXOS_STATUS_SYNC", 2);
        context.getDDLSynchronism()->upgradeVersion(upgrade_query->origin_version, upgrade_query->upgrade_version);
        safety_point_sync->broadcastSync("LOCK_UPGRADE_VERSION_FOR_PAXOS", 2);
    });
}

StorageWithLock InterpreterUpgradeQuery::selectAllUpgradeStorage(const String & origin_version, const String & /*upgrade_version*/)
{
    std::vector<std::pair<StoragePtr, TableStructureReadLockPtr>> res;
    for (auto & database_element : context.getDatabases())
    {
        const DatabasePtr & database = database_element.second;

        for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
        {
            if (auto storage = dynamic_cast<StorageQingCloud *>(iterator->table().get()))
            {
                /// TODO: add upgrade_version to check
                storage->checkNeedUpgradeVersion(origin_version);
                if (storage->checkNeedUpgradeVersion(origin_version))
                    res.emplace_back(std::pair(iterator->table(), iterator->table()->lockStructure(false, __PRETTY_FUNCTION__)));
            }
        }
    }
    return res;
}

UpgradeQueryBlockInputStream::UpgradeQueryBlockInputStream(
    const SafetyPointWithClusterPtr &safety_point_sync, const StorageWithLock &upgrade_storage, const String &origin_version,
    const String &upgrade_version) : origin_version(origin_version), upgrade_version(upgrade_version),
                                     tmp_upgrade_version("tmp_" + upgrade_version), upgrade_storage(upgrade_storage),
                                     safety_point_sync(safety_point_sync)
{
}

String UpgradeQueryBlockInputStream::getName() const
{
    return "UpgradeVersion";
}

Block UpgradeQueryBlockInputStream::getHeader() const
{
    return Block {
        {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "id"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "database_name"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "table_name"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "state"}
    };
}

#define DECLARE(ENUM, STAGE_BLOCK) \
    STAGE_BLOCK; \
    safety_point_sync->broadcastSync(progressToString(ENUM), 2); \

Block UpgradeQueryBlockInputStream::readImpl()
{
    try
    {
        for (const auto & storage_with_lock : upgrade_storage)
        {
            if (const auto storage = dynamic_cast<StorageQingCloud *>(storage_with_lock.first.get()))
            {
                DECLARE(ProgressEnum::INITIALIZE_UPGRADE_VERSION, storage->initializeVersions({origin_version, tmp_upgrade_version, upgrade_version}));
                DECLARE(ProgressEnum::REDIRECT_VERSIONS_BEFORE_MIGRATE, storage->initializeVersionInfo({origin_version, tmp_upgrade_version}, tmp_upgrade_version));
            }
        }

        for (const auto & storage_with_lock : upgrade_storage)
        {
            if (const auto storage = dynamic_cast<StorageQingCloud *>(storage_with_lock.first.get()))
            {
                DECLARE(FLUSH_OLD_VERSION_DATA, storage->flushVersionData(origin_version))
                DECLARE(CLEANUP_UPGRADE_VERSION, storage->cleanupBeforeMigrate(upgrade_version))
                DECLARE(MIGRATE_OLD_VERSION_DATA, storage->migrateDataBetweenVersions(origin_version, upgrade_version, true))
                DECLARE(FLUSH_UPGRADE_VERSION_DATA, storage->flushVersionData(upgrade_version))
                DECLARE(REDIRECT_VERSIONS_AFTER_MIGRATE, storage->initializeVersionInfo({tmp_upgrade_version, upgrade_version}, upgrade_version))
                DECLARE(MIGRATE_TMP_VERSION_DATA, storage->migrateDataBetweenVersions(tmp_upgrade_version, upgrade_version, false))
                DECLARE(REDIRECT_VERSION_AFTER_ALL_MIGRATE, storage->initializeVersionInfo({upgrade_version}, upgrade_version))
                DECLARE(NORMAL, storage->deleteOutdatedVersions({origin_version, tmp_upgrade_version}))
            }
        }

        SafetyPointFactory::instance().releaseSafetyPoint("UPGRADE_VERSION_" + origin_version + "_" + upgrade_version);
        return {};
    }
    catch(...)
    {
        SafetyPointFactory::instance().releaseSafetyPoint("UPGRADE_VERSION_" + origin_version + "_" + upgrade_version);
        throw;
    }
}

}
