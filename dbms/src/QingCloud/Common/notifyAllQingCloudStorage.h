#pragma once

#include <QingCloud/Storages/StorageQingCloudbak.h>

namespace DB
{

void notifyAllQingCloudStorage(const Context & context, bool is_create)
{
    if (is_create)
        return;

    Databases databases = context.getDatabases();

    for (auto name_with_database : databases)
    {
        DatabasePtr & database = name_with_database.second;
        DatabaseIteratorPtr table_iterator = database->getIterator(context);

        while (table_iterator->isValid())
        {
            StoragePtr storage = table_iterator->table();
            StorageQingCloud * storage_qingcloud = dynamic_cast<StorageQingCloud *>(storage.get());

            if (storage_qingcloud)
                storage_qingcloud->syncClusterConfig();

            table_iterator->next();
        }
    }
}

}