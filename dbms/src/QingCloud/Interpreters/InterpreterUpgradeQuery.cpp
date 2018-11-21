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
#include "InterpreterUpgradeQuery.h"


namespace DB
{

InterpreterUpgradeQuery::InterpreterUpgradeQuery(const ASTPtr &node, const Context &context) : node(node), context(context)
{
}


BlockIO InterpreterUpgradeQuery::execute()
{
    ASTUpgradeQuery * upgrade_query = typeid_cast<ASTUpgradeQuery *>(node.get());
    /// TODO: 锁住所有的DDL
    /// TODO: 确保之前的DDL已经被Paxos学习到了

//    context.getDDLSynchronism()->weakup();
//    SafetyPointFactory::instance().createSafetyPoint("Upgrade Version", context.getCluster());

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
    res.in = std::make_shared<UpgradeQueryBlockInputStream>(upgrade_storage);
    for (const auto & lock : upgrade_storage_lock)
        res.in->addTableLock(lock);
    return res;
}

UpgradeQueryBlockInputStream::UpgradeQueryBlockInputStream(const std::vector<StoragePtr> & upgrade_storage) : upgrade_storage(upgrade_storage)
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
    for (const auto & storage : upgrade_storage)
    {
        if (const auto qing_storage = dynamic_cast<StorageQingCloud *>(storage.get()))
        {
//            State::INITIALIZE_UPGRADE_VERSION;
            qing_storage->initializeVersions({origin_version, "tmp_" + upgrade_version, upgrade_version});
            qing_storage->initializeVersionInfo({origin_version, "tmp_" + upgrade_version}, "tmp_" + upgrade_version);
        }
    }

    for (const auto & storage : upgrade_storage)
    {
        if (const auto qing_storage = dynamic_cast<StorageQingCloud *>(storage.get()))
        {
            qing_storage->flushVersionData(origin_version);
            qing_storage->migrateDataBetweenVersions(origin_version, upgrade_version, true);
            qing_storage->flushVersionData(upgrade_version);
            qing_storage->initializeVersionInfo({"tmp_" + upgrade_version, upgrade_version}, upgrade_version);
            qing_storage->migrateDataBetweenVersions("tmp_" + upgrade_version, upgrade_version, false);
            qing_storage->initializeVersionInfo({upgrade_version}, upgrade_version);
            /// TODO: delete old version DELETE_OUTDATED_VERSIONS
        }
    }

    return {};
}

}
