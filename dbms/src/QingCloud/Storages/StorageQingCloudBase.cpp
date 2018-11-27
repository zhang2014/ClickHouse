#include <QingCloud/Storages/StorageQingCloudBase.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Storages/AlterCommands.h>
#include "StorageQingCloudBase.h"


namespace DB
{

void StorageQingCloudBase::drop()
{
    for (const auto & storage : version_distributed)
        storage.second->drop();

    for (const auto & storage : local_data_storage)
        storage.second->drop();
}

void StorageQingCloudBase::truncate(const ASTPtr & truncate_query)
{
    for (const auto & storage : version_distributed)
        storage.second->truncate(truncate_query);

    for (const auto & storage : local_data_storage)
        storage.second->truncate(truncate_query);
}

void StorageQingCloudBase::alter(const AlterCommands &params, const String &database_name, const String &table_name, const Context &context)
{
    auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    ColumnsDescription new_columns = getColumns();
    params.apply(new_columns);
    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, {});
    setColumns(std::move(new_columns));

    for (const auto & storage : version_distributed)
        storage.second->alter(params, database_name, table_name, context);

    for (const auto & storage : local_data_storage)
        storage.second->alter(params, database_name, table_name, context);
}

void StorageQingCloudBase::clearColumnInPartition(const ASTPtr &partition, const Field &column_name, const Context &context)
{
    for (const auto & storage : local_data_storage)
        storage.second->clearColumnInPartition(partition, column_name, context);
}

void StorageQingCloudBase::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context &context)
{
    for (const auto & storage : local_data_storage)
        storage.second->replacePartitionFrom(source_table, partition, replace, context);
}

void StorageQingCloudBase::dropPartition(const ASTPtr &query, const ASTPtr &partition, bool detach, const Context &context)
{
    for (const auto & storage : local_data_storage)
        storage.second->dropPartition(query, partition, detach, context);
}

void StorageQingCloudBase::attachPartition(const ASTPtr &partition, bool part, const Context &context)
{
    for (const auto & storage : local_data_storage)
        storage.second->attachPartition(partition, part, context);
}

void StorageQingCloudBase::fetchPartition(const ASTPtr &partition, const String &from, const Context &context)
{
    for (const auto & storage : local_data_storage)
        storage.second->fetchPartition(partition, from, context);
}

void StorageQingCloudBase::freezePartition(const ASTPtr &partition, const String &with_name, const Context &context)
{
    for (const auto & storage : local_data_storage)
        storage.second->freezePartition(partition, with_name, context);
}

bool StorageQingCloudBase::optimize(const ASTPtr &query, const ASTPtr &partition, bool final, bool deduplicate, const Context &context)
{
    for (const auto & storage : local_data_storage)
         if (!storage.second->optimize(query, partition, final, deduplicate, context))
             return false;
    return true;
}

void StorageQingCloudBase::mutate(const MutationCommands & commands, const Context & context)
{
    for (const auto & storage : local_data_storage)
        storage.second->mutate(commands, context);
}

void StorageQingCloudBase::startup()
{
    for (const auto & storage : local_data_storage)
        storage.second->startup();

    for (const auto & storage : version_distributed)
        storage.second->startup();
}

void StorageQingCloudBase::shutdown()
{
    for (const auto & storage : version_distributed)
        storage.second->shutdown();

    for (const auto & storage : local_data_storage)
        storage.second->shutdown();
}

bool StorageQingCloudBase::checkData() const
{
    for (const auto &storage : local_data_storage)
        if (!storage.second->checkData())
            return false;
    return true;
}

bool StorageQingCloudBase::hasColumn(const String &column_name) const
{
    for (const auto & storage : version_distributed)
        if (storage.second->hasColumn(column_name))
            return true;

    for (const auto & storage : local_data_storage)
        if (storage.second->hasColumn(column_name))
            return true;
    return false;
}

StorageQingCloudBase::StorageQingCloudBase(const ColumnsDescription & columns) :IStorage{columns}
{
}

}
