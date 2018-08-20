#include <QingCloud/Storages/QingCloudStorageFactory.h>
#include <QingCloud/Storages/StorageQingCloud.h>


namespace DB
{

StoragePtr QingCloudStorageFactory::get(
    ASTCreateQuery & query, const String & data_path, const String & table_name, const String & database_name,
    Context &local_context, Context & context, const ColumnsDescription & columns, bool attach, bool has_force_restore_data_flag) const
{
    return std::make_shared<StorageQingCloud>(query, data_path, table_name, database_name, local_context, context, columns, attach,
                                              has_force_restore_data_flag);
}

}
