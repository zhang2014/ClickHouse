#include <QingCloud/Storages/QingCloudStorageFactory.h>
#include "QingCloudStorageFactory.h"


namespace DB
{


StoragePtr QingCloudStorageFactory::get(
    ASTCreateQuery & query, const String & data_path, const String & table_name, const String & database_name,
    Context &local_context, Context &context, const ColumnsDescription & columns, bool attach, bool has_force_restore_data_flag) const
{
    /// TODO: 创建QingCloudStorage包装所有Engine
    return DB::StoragePtr();
}

}
