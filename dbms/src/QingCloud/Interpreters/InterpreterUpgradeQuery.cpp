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
    ASTUpgradeQuery * upgrade_query = typeid_cast<ASTUpgradeQuery *>(node.get());

    String & origin_version = upgrade_query->origin_version;
    String & upgrade_version = upgrade_query->upgrade_version;
    const String sync_name = "UPGRADE_VERSION_" + origin_version + "_" + upgrade_version;

    try
    {
        const auto origin_cluster = context.getCluster("Cluster_" + origin_version);
        const auto upgrade_cluster = context.getCluster("Cluster_" + upgrade_version);
        const auto two_cluster_connections = getConnectionPoolsFromClusters({origin_cluster, upgrade_cluster});
        const auto two_cluster_safety_point_sync = SafetyPointFactory::instance().createSafetyPoint(sync_name, context, two_cluster_connections);

        context.getMultiplexedVersion()->checkBeforeUpgrade(origin_version, upgrade_version);
        const auto databases_and_tables_name = upgradeVersionForPaxos(upgrade_query, two_cluster_safety_point_sync);
        context.getMultiplexedVersion()->executionUpgradeVersion(origin_version, upgrade_version, databases_and_tables_name, context,
                                                                 two_cluster_safety_point_sync);
        SafetyPointFactory::instance().releaseSafetyPoint(sync_name);
        return {};
    }
    catch(...)
    {
        SafetyPointFactory::instance().releaseSafetyPoint(sync_name);
        throw;
    }
}

std::vector<DatabaseAndTableName> InterpreterUpgradeQuery::upgradeVersionForPaxos(ASTUpgradeQuery * upgrade_query, const SafetyPointWithClusterPtr & safety_point_sync)
{
    return context.getDDLSynchronism()->withLockPaxos([&safety_point_sync, this, &upgrade_query]() -> std::vector<DatabaseAndTableName>
    {
        safety_point_sync->broadcastSync("LOCK_NEW_PAXOS_QUERIES", 2);
        context.getDDLSynchronism()->wakeupLearner();
        safety_point_sync->broadcastSync("LOCK_PAXOS_STATUS_SYNC", 2);
        context.getDDLSynchronism()->upgradeVersion(upgrade_query->origin_version, upgrade_query->upgrade_version);
        safety_point_sync->broadcastSync("LOCK_UPGRADE_VERSION_FOR_PAXOS", 2);
//        context.getMultiplexedVersion()->setCurrentVersion(upgrade_query->upgrade_version);
//        safety_point_sync->broadcastSync("LOCK_UPGRADE_CONTEXT_VERSION", 2);
        return selectAllUpgradeStorage(upgrade_query->origin_version, upgrade_query->upgrade_version);
    });
}

std::vector<DatabaseAndTableName> InterpreterUpgradeQuery::selectAllUpgradeStorage(const String & origin_version, const String & upgrade_version)
{
    std::vector<DatabaseAndTableName> databases_and_tables_name;
    for (auto & database_element : context.getDatabases())
    {
        const DatabasePtr & database = database_element.second;

        for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
        {
            if (auto storage = dynamic_cast<StorageQingCloud *>(iterator->table().get()))
            {
                if (storage->checkNeedUpgradeVersion(origin_version, upgrade_version))
                    databases_and_tables_name.emplace_back(std::pair(database->getDatabaseName(), iterator->name()));
            }
        }
    }
    return databases_and_tables_name;
}

#define DECLARE(ENUM, WRITE_LOCK, STAGE_BLOCK) \
    { \
        if (WRITE_LOCK) \
        { \
            const auto lock = storage->lockStructureForAlter(__PRETTY_FUNCTION__); \
            STAGE_BLOCK; \
            storage->updateUpgradeState(ENUM); \
        } \
        else \
        { \
            const auto lock = storage->lockStructure(false, __PRETTY_FUNCTION__); \
            STAGE_BLOCK; \
            storage->updateUpgradeState(ENUM); \
        } \
    } \
    safety_point_sync->broadcastSync(progressToString(ENUM), 2); \


}
