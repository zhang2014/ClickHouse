#pragma once

#include <Storages/IStorage.h>
#include <ext/singleton.h>

namespace DB
{

class QingCloudStorageFactory : public ext::singleton<QingCloudStorageFactory>
{
public:
    StoragePtr get(
        ASTCreateQuery & query,
        const String & data_path,
        const String & table_name,
        const String & database_name,
        Context & local_context,
        Context & context,
        const ColumnsDescription & columns,
        bool attach,
        bool has_force_restore_data_flag) const;
};

}