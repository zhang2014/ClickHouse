#include <QingCloud/Interpreters/InterpreterUpgradeQuery.h>
#include <QingCloud/Parsers/ASTUpgradeQuery.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <QingCloud/Storages/StorageQingCloud.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include "InterpreterUpgradeQuery.h"


namespace DB
{

InterpreterUpgradeQuery::InterpreterUpgradeQuery(const ASTPtr &node, const Context &context) : node(node), context(context)
{
}


BlockIO InterpreterUpgradeQuery::execute()
{
    /// TODO: FROM BEGIN
    ASTUpgradeQuery * upgrade_query = typeid_cast<ASTUpgradeQuery *>(node.get());
    StoragePtr upgrade_storage = context.getTable(upgrade_query->database, upgrade_query->table);

    if (!dynamic_cast<StorageQingCloud *>(upgrade_storage.get()))
        throw Exception("Cannot run upgrade query for without QingCloud Storage.", ErrorCodes::LOGICAL_ERROR);

    BlockIO res;
    res.in = std::make_shared<UpgradeQueryBlockInputStream>(upgrade_storage, upgrade_query->origin_version, upgrade_query->upgrade_version);
    return res;
}

UpgradeQueryBlockInputStream::UpgradeQueryBlockInputStream(const StoragePtr &storage, const String &origin_version,
                                                           const String &upgrade_version) : storage(storage),
                                                                                            origin_version(origin_version),
                                                                                            upgrade_version(upgrade_version)
{
}

Block UpgradeQueryBlockInputStream::readImpl()
{
    StorageQingCloud * upgrade_storage = dynamic_cast<StorageQingCloud *>(storage.get());

    switch(state)
    {
        case FLUSH_OLD_VERSION_DATA:                   return flushOldVersionData(upgrade_storage);
        case MIGRATE_OLD_VERSION_DATA:                 return migrateOldVersionData(upgrade_storage);
        case MIGRATE_TMP_VERSION_DATA:                 return migrateTmpVersionData(upgrade_storage);
        case DELETE_OUTDATED_VERSIONS:                 return deleteOutdatedVersions(upgrade_storage);
        case FLUSH_UPGRADE_VERSION_DATA:               return flushUpgradeVersionData(upgrade_storage);
        case INITIALIZE_UPGRADE_VERSION:               return initializeUpgradeVersion(upgrade_storage);
        case REDIRECT_VERSIONS_AFTER_MIGRATE:          return redirectVersionAfterMigrate(upgrade_storage);
        case REDIRECT_VERSIONS_BEFORE_MIGRATE:         return redirectVersionBeforeMigrate(upgrade_storage);
        case REDIRECT_VERSION_AFTER_ALL_MIGRATE:       return redirectVersionAfterAllMigrate(upgrade_storage);

        default:
        case FAILURE:
        case SUCCESSFULLY: return {};
    }
}

Block UpgradeQueryBlockInputStream::initializeUpgradeVersion(StorageQingCloud * upgrade_storage)
{
    upgrade_storage->initializeVersions({origin_version, "tmp_" + upgrade_version, upgrade_version});
    state = REDIRECT_VERSIONS_BEFORE_MIGRATE;

    Block header = getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(String("Initialize upgrade version"));
    return header.cloneWithColumns(std::move(columns));
}

Block UpgradeQueryBlockInputStream::flushOldVersionData(StorageQingCloud * upgrade_storage)
{
    upgrade_storage->flushVersionData(origin_version);
    state = MIGRATE_OLD_VERSION_DATA;

    Block header = getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(String("Flush old version data"));
    return header.cloneWithColumns(std::move(columns));
}

Block UpgradeQueryBlockInputStream::flushUpgradeVersionData(StorageQingCloud *upgrade_storage)
{
    upgrade_storage->flushVersionData(upgrade_version);
    state = REDIRECT_VERSIONS_AFTER_MIGRATE;

    Block header = getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(String("Flush upgrade version data"));
    return header.cloneWithColumns(std::move(columns));
}

Block UpgradeQueryBlockInputStream::redirectVersionAfterMigrate(StorageQingCloud * upgrade_storage)
{
    upgrade_storage->initializeVersionInfo({"tmp_" + upgrade_version, upgrade_version}, upgrade_version);
    state = MIGRATE_TMP_VERSION_DATA;

    Block header = getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(String("Redirect Version After Migrate"));
    return header.cloneWithColumns(std::move(columns));
}

Block UpgradeQueryBlockInputStream::migrateOldVersionData(StorageQingCloud * upgrade_storage)
{
    upgrade_storage->migrateDataBetweenVersions(origin_version, upgrade_version, false, true);
    state = FLUSH_UPGRADE_VERSION_DATA;

    Block header = getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(String("Migrate Old Version Data"));
    return header.cloneWithColumns(std::move(columns));
}

Block UpgradeQueryBlockInputStream::migrateTmpVersionData(StorageQingCloud * upgrade_storage)
{
    upgrade_storage->migrateDataBetweenVersions(origin_version, upgrade_version, true, false);
    state = REDIRECT_VERSION_AFTER_ALL_MIGRATE;

    Block header = getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(String("Migrate Temp Version Data"));
    return header.cloneWithColumns(std::move(columns));
}

Block UpgradeQueryBlockInputStream::redirectVersionBeforeMigrate(StorageQingCloud * upgrade_storage)
{
    upgrade_storage->initializeVersionInfo({upgrade_version, "tmp_" + upgrade_version}, "tmp_" + upgrade_version);
    state = FLUSH_OLD_VERSION_DATA;

    Block header = getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(String("Redirect Version Before Migrate"));
    return header.cloneWithColumns(std::move(columns));
}

Block UpgradeQueryBlockInputStream::redirectVersionAfterAllMigrate(StorageQingCloud * upgrade_storage)
{
    upgrade_storage->initializeVersionInfo({upgrade_version}, upgrade_version);
    state = DELETE_OUTDATED_VERSIONS;

    Block header = getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(String("Redirect Version After All Migrate"));
    return header.cloneWithColumns(std::move(columns));
}

Block UpgradeQueryBlockInputStream::deleteOutdatedVersions(StorageQingCloud * /*upgrade_storage*/)
{
    /// TODO: 删除掉一些无用的版本数据, 释放物理空间
    Block header = getHeader();
    MutableColumns columns = header.cloneEmptyColumns();
    columns[0]->insert(String("Deleted Outdated Versions"));
    return header.cloneWithColumns(std::move(columns));
}

Block UpgradeQueryBlockInputStream::getHeader() const
{
    return Block{{
        ColumnString::create(), std::make_shared<DataTypeString>(), "stage_name",
    }};
}

String UpgradeQueryBlockInputStream::getName() const
{
    return "UpgradeVersion";
}

}
