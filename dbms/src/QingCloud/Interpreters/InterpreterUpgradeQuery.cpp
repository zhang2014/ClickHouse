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

static void changeProcessAndSync(ProgressEnum progress_enum, StorageQingCloud * /*storage*/, const SafetyPointWithClusterPtr & safety_point_sync)
{
    switch(progress_enum)
    {
        case NORMAL: safety_point_sync->broadcastSync("NORMAL"); break;
        case INITIALIZE_UPGRADE_VERSION : safety_point_sync->broadcastSync("INITIALIZE_UPGRADE_VERSION"); break;
        case REDIRECT_VERSIONS_BEFORE_MIGRATE: safety_point_sync->broadcastSync("REDIRECT_VERSIONS_BEFORE_MIGRATE"); break;
        case FLUSH_OLD_VERSION_DATA: safety_point_sync->broadcastSync("FLUSH_OLD_VERSION_DATA"); break;
        case CLEANUP_UPGRADE_VERSION: safety_point_sync->broadcastSync("CLEANUP_UPGRADE_VERSION"); break;
        case MIGRATE_OLD_VERSION_DATA: safety_point_sync->broadcastSync("MIGRATE_OLD_VERSION_DATA"); break;
        case FLUSH_UPGRADE_VERSION_DATA: safety_point_sync->broadcastSync("FLUSH_UPGRADE_VERSION_DATA"); break;
        case REDIRECT_VERSIONS_AFTER_MIGRATE: safety_point_sync->broadcastSync("REDIRECT_VERSIONS_AFTER_MIGRATE"); break;
        case MIGRATE_TMP_VERSION_DATA: safety_point_sync->broadcastSync("MIGRATE_TMP_VERSION_DATA"); break;
        case REDIRECT_VERSION_AFTER_ALL_MIGRATE: safety_point_sync->broadcastSync("REDIRECT_VERSION_AFTER_ALL_MIGRATE"); break;
        case DELETE_OUTDATED_VERSIONS: safety_point_sync->broadcastSync("DELETE_OUTDATED_VERSIONS"); break;
    }
}

InterpreterUpgradeQuery::InterpreterUpgradeQuery(const ASTPtr & node, const Context &context) : node(node), context(context)
{
}

BlockIO InterpreterUpgradeQuery::execute()
{
    ASTUpgradeQuery * upgrade_query = typeid_cast<ASTUpgradeQuery *>(node.get());
    const String sync_name = "UPGRADE_VERSION_" + upgrade_query->origin_version + "_" + upgrade_query->upgrade_version;

    const ClusterPtr origin_cluster = context.getCluster("Cluster_" + upgrade_query->origin_version);
    const ClusterPtr upgrade_cluster = context.getCluster("Cluster_" + upgrade_query->upgrade_version);

    const auto connections = getConnectionPoolsFromClusters({origin_cluster, upgrade_cluster});
    const auto safety_point_sync = SafetyPointFactory::instance().createSafetyPoint(sync_name, context, connections);

    const auto lock = context.getDDLSynchronism()->lock();
    safety_point_sync->broadcastSync("LOCK_NODE_DDL", 2);
    context.getDDLSynchronism()->wakeupLearner();
    safety_point_sync->broadcastSync("WAKEUP_PAXOS_LEARNER", 2);
    context.getDDLSynchronism()->upgradeVersion(upgrade_query->origin_version, upgrade_query->upgrade_version);
    safety_point_sync->broadcastSync("UPGRADE_PAXOS", 2);

    std::vector<StoragePtr> upgrade_storage;
    std::vector<TableStructureReadLockPtr> upgrade_storage_lock;
    for (auto & database_element : context.getDatabases())
    {
        const DatabasePtr & database = database_element.second;

        for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
        {
            if (auto storage = dynamic_cast<StorageQingCloud *>(iterator->table().get()))
            {
                if (storage->checkNeedUpgradeVersion(upgrade_query->origin_version))
                {
                    upgrade_storage.emplace_back(iterator->table());
                    upgrade_storage_lock.emplace_back(iterator->table()->lockStructure(false, __PRETTY_FUNCTION__));
                }
            }
        }
    }


    BlockIO res;
    res.in = std::make_shared<UpgradeQueryBlockInputStream>(
        safety_point_sync, upgrade_storage, upgrade_query->origin_version, upgrade_query->upgrade_version);
    for (const auto & storage_lock : upgrade_storage_lock)
        res.in->addTableLock(storage_lock);
    return res;
}

UpgradeQueryBlockInputStream::UpgradeQueryBlockInputStream(
    const SafetyPointWithClusterPtr &safety_point_sync, const std::vector<StoragePtr> &upgrade_storage, const String &origin_version,
    const String &upgrade_version) : origin_version(origin_version), upgrade_version(upgrade_version), upgrade_storages(upgrade_storage),
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

Block UpgradeQueryBlockInputStream::readImpl()
{
    try
    {
        for (const auto & upgrade_storage : upgrade_storages)
        {
            if (const auto storage = dynamic_cast<StorageQingCloud *>(upgrade_storage.get()))
            {
                storage->initializeVersions({origin_version, "tmp_" + upgrade_version, upgrade_version});
                changeProcessAndSync(ProgressEnum::INITIALIZE_UPGRADE_VERSION, storage, safety_point_sync);
                storage->initializeVersionInfo({origin_version, "tmp_" + upgrade_version}, "tmp_" + upgrade_version);
                changeProcessAndSync(ProgressEnum::REDIRECT_VERSIONS_BEFORE_MIGRATE, storage, safety_point_sync);
            }
        }

        for (const auto & upgrade_storage : upgrade_storages)
        {
            if (const auto storage = dynamic_cast<StorageQingCloud *>(upgrade_storage.get()))
            {

#define APPLY_FOR_PROGRESS(M)  \
                M(ProgressEnum::FLUSH_OLD_VERSION_DATA, storage, safety_point_sync, storage->flushVersionData(origin_version)) \
                M(ProgressEnum::CLEANUP_UPGRADE_VERSION, storage, safety_point_sync, storage->cleanupBeforeMigrate(upgrade_version)) \
                M(ProgressEnum::MIGRATE_OLD_VERSION_DATA, storage, safety_point_sync, storage->migrateDataBetweenVersions(origin_version, upgrade_version, true)) \
                M(ProgressEnum::FLUSH_UPGRADE_VERSION_DATA, storage, safety_point_sync, storage->flushVersionData(upgrade_version)) \
                M(ProgressEnum::REDIRECT_VERSIONS_AFTER_MIGRATE, storage, safety_point_sync, storage->initializeVersionInfo({"tmp_" + upgrade_version, upgrade_version}, upgrade_version)) \
                M(ProgressEnum::MIGRATE_TMP_VERSION_DATA, storage, safety_point_sync, storage->migrateDataBetweenVersions("tmp_" + upgrade_version, upgrade_version, false)) \
                M(ProgressEnum::REDIRECT_VERSION_AFTER_ALL_MIGRATE, storage, safety_point_sync,storage->initializeVersionInfo({upgrade_version}, upgrade_version)) \
                M(ProgressEnum::NORMAL, storage, safety_point_sync ,storage->deleteOutdatedVersions({origin_version, "tmp_" + upgrade_version}))

#define DECLARE(ENUM, STORAGE, SYNC, INVOKE) \
    INVOKE; \
    changeProcessAndSync(ENUM, STORAGE, SYNC);

    APPLY_FOR_PROGRESS(DECLARE)
#undef DECLARE
//                storage->flushVersionData(origin_version);
//                changeProcessAndSync(ProgressEnum::FLUSH_OLD_VERSION_DATA, storage, safety_point_sync);
//                storage->cleanupBeforeMigrate(upgrade_version);
//                changeProcessAndSync(ProgressEnum::CLEANUP_UPGRADE_VERSION, storage, safety_point_sync);
//                storage->migrateDataBetweenVersions(origin_version, upgrade_version, true);
//                changeProcessAndSync(ProgressEnum::MIGRATE_OLD_VERSION_DATA, storage, safety_point_sync);
//                storage->flushVersionData(upgrade_version);
//                changeProcessAndSync(ProgressEnum::FLUSH_UPGRADE_VERSION_DATA, storage, safety_point_sync);
//                storage->initializeVersionInfo({"tmp_" + upgrade_version, upgrade_version}, upgrade_version);
//                changeProcessAndSync(ProgressEnum::REDIRECT_VERSIONS_AFTER_MIGRATE, storage, safety_point_sync);
//                storage->migrateDataBetweenVersions("tmp_" + upgrade_version, upgrade_version, false);
//                changeProcessAndSync(ProgressEnum::MIGRATE_TMP_VERSION_DATA, storage, safety_point_sync);
//                storage->initializeVersionInfo({upgrade_version}, upgrade_version);
//                changeProcessAndSync(ProgressEnum::REDIRECT_VERSION_AFTER_ALL_MIGRATE, storage, safety_point_sync);
//                storage->deleteOutdatedVersions({origin_version, "tmp_" + upgrade_version});
//                changeProcessAndSync(ProgressEnum::NORMAL, storage, safety_point_sync);
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
