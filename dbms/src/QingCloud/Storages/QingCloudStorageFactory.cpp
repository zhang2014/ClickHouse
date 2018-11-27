#include <QingCloud/Storages/QingCloudStorageFactory.h>
#include <QingCloud/Storages/StorageQingCloud.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
}

StoragePtr QingCloudStorageFactory::get(
    ASTCreateQuery & query, const String & data_path, const String & table_name, const String & database_name,
    Context &local_context, Context & context, const ColumnsDescription & columns, bool attach, bool has_force_restore_data_flag) const
{
    return std::make_shared<StorageQingCloud>(query, data_path, table_name, database_name, local_context, context, columns, attach,
                                              has_force_restore_data_flag);
}

bool QingCloudStorageFactory::checkSupportStorage(ASTCreateQuery &query, const String & /*data_path*/, const String &/*table_name*/,
                                                  const String &/*database_name*/, Context &/*local_context*/, Context &/*context*/,
                                                  const ColumnsDescription &/*columns*/, bool /*attach*/, bool /*has_force_restore_data_flag*/) const
{
    if (query.is_view || query.is_materialized_view)
        return false;

    ASTStorage * storage_def = query.storage;
    const ASTFunction & engine_def = *storage_def->engine;

    if (engine_def.parameters)
        throw Exception("Engine definition cannot take the form of a parametric function", ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);

    if (startsWith(engine_def.name, "Replicated") || engine_def.name == "Distributed" || engine_def.name == "Buffer" ||
        engine_def.name == "File" || engine_def.name == "Log" || engine_def.name == "Memory" || engine_def.name == "Set" ||
        engine_def.name == "StripeLog")
        throw Exception("Unknown table engine " + engine_def.name, ErrorCodes::UNKNOWN_STORAGE);

    return endsWith(engine_def.name, "MergeTree");
}

}
