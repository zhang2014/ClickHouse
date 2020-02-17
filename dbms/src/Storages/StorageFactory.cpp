#include <Storages/StorageFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageID.h>
#include "StorageFactory.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int ENGINE_REQUIRED;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int BAD_ARGUMENTS;
    extern const int DATA_TYPE_CANNOT_BE_USED_IN_TABLES;
}


/// Some types are only for intermediate values of expressions and cannot be used in tables.
static void checkAllTypesAreAllowedInTable(const NamesAndTypesList & names_and_types)
{
    for (const auto & elem : names_and_types)
        if (elem.type->cannotBeStoredInTables())
            throw Exception("Data type " + elem.type->getName() + " cannot be used in tables", ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_TABLES);
}

static void checkEngineTypeAllowedInCreateQuery(const String & engine_name)
{
    const auto & check_allow_engine_type = [&](const String & not_allow_engine_name, const String & exception_desc)
    {
        if (engine_name == not_allow_engine_name)
            throw Exception("Direct creation of tables with ENGINE " + not_allow_engine_name + " is not supported, use " + exception_desc + " statement",
                ErrorCodes::INCORRECT_QUERY);

        return true;
    };

    check_allow_engine_type("View", "CREATE VIEW");
    check_allow_engine_type("LiveView", "CREATE LIVE VIEW");
    check_allow_engine_type("MaterializedView", "CREATE MATERIALIZED VIEW");
}

static String getUnSupportFeatureMsg(
    const String & feature_description, const String & engine_name, const std::vector<String> & supporting_engines)
{
    String message = "Engine " + engine_name + " doesn't support " + feature_description +
        ". Currently only the following engines have support for the feature: [";

    for (size_t index = 0; index < supporting_engines.size(); ++index)
    {
        if (index)
            message += ", ";
        message += supporting_engines[index];
    }
    return message + "]";
}

static StorageFactory::CreatorFn checkStorageFeaturesCreator(
    const StorageFactory::CreatorFn & creator_fn, StorageFactory::StorageFeatures features)
{
    const auto & supporting_settings_engines = StorageFactory::instance().getAllRegisteredNamesByFeatureMatcherFn(
        [](StorageFactory::StorageFeatures f) { return f.supports_settings; });

    const auto & supporting_ttl_engines = StorageFactory::instance().getAllRegisteredNamesByFeatureMatcherFn(
        [](StorageFactory::StorageFeatures f) { return f.supports_ttl; });

    const auto & supporting_skipping_indices_engines = StorageFactory::instance().getAllRegisteredNamesByFeatureMatcherFn(
        [](StorageFactory::StorageFeatures f) { return f.supports_skipping_indices; });

    const auto & supporting_sort_order_engines = StorageFactory::instance().getAllRegisteredNamesByFeatureMatcherFn(
        [](StorageFactory::StorageFeatures f) { return f.supports_sort_order; });

    return [&](const StorageFactory::Arguments &arguments) -> StoragePtr
    {
        checkEngineTypeAllowedInCreateQuery(arguments.engine_name);

        if (arguments.storage_def->settings && !features.supports_settings)
            throw Exception(getUnSupportFeatureMsg("SETTINGS clause", arguments.engine_name, supporting_settings_engines),
                            ErrorCodes::BAD_ARGUMENTS);

        if ((arguments.storage_def->ttl_table || !arguments.columns.getColumnTTLs().empty()) && !features.supports_ttl)
            throw Exception(getUnSupportFeatureMsg("TTL clause", arguments.engine_name, supporting_ttl_engines), ErrorCodes::BAD_ARGUMENTS);

        if (arguments.query.columns_list && arguments.query.columns_list->indices &&
            !arguments.query.columns_list->indices->children.empty() && !features.supports_skipping_indices)
            throw Exception(getUnSupportFeatureMsg("skipping indices", arguments.engine_name, supporting_skipping_indices_engines),
                ErrorCodes::BAD_ARGUMENTS);

        if ((arguments.storage_def->partition_by || arguments.storage_def->primary_key || arguments.storage_def->order_by ||
             arguments.storage_def->sample_by) && !features.supports_sort_order)
            throw Exception(getUnSupportFeatureMsg("PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses", arguments.engine_name,
                supporting_sort_order_engines), ErrorCodes::BAD_ARGUMENTS);

        return creator_fn(arguments);
    };
}

void StorageFactory::registerStorage(const std::string & name, CreatorFn creator_fn, StorageFeatures features)
{
    if (!storages.emplace(name, Creator{checkStorageFeaturesCreator(creator_fn, features), features}).second)
        throw Exception("TableFunctionFactory: the table function name '" + name + "' is not unique", ErrorCodes::LOGICAL_ERROR);
}


StoragePtr StorageFactory::get(
    const ASTCreateQuery & query,
    const String & relative_data_path,
    Context & local_context,
    Context & context,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    bool has_force_restore_data_flag) const
{
    if (query.is_view)
    {
        if (query.storage)
            throw Exception("Specifying ENGINE is not allowed for a View", ErrorCodes::INCORRECT_QUERY);

        return getViewStorage("View", query, relative_data_path, local_context, context, columns, constraints, has_force_restore_data_flag);
    }
    else if (query.is_live_view)
    {

        if (query.storage)
            throw Exception("Specifying ENGINE is not allowed for a LiveView", ErrorCodes::INCORRECT_QUERY);

        return getViewStorage("LiveView", query, relative_data_path, local_context, context, columns, constraints, has_force_restore_data_flag);
    }
    else if (query.is_materialized_view)
    {
        /// Check for some special types, that are not allowed to be stored in tables. Example: NULL data type.
        /// Exception: any type is allowed in View, because plain (non-materialized) View does not store anything itself.
        checkAllTypesAreAllowedInTable(columns.getAll());

        return getViewStorage("MaterializedView", query, relative_data_path, local_context, context, columns, constraints, has_force_restore_data_flag);
    }
    else
    {
        /// Check for some special types, that are not allowed to be stored in tables. Example: NULL data type.
        /// Exception: any type is allowed in View, because plain (non-materialized) View does not store anything itself.
        checkAllTypesAreAllowedInTable(columns.getAll());

        if (!query.storage)
            throw Exception("Incorrect CREATE query: ENGINE required", ErrorCodes::ENGINE_REQUIRED);

        if (!query.storage->engine)
            throw Exception("Incorrect CREATE query: ENGINE required", ErrorCodes::ENGINE_REQUIRED);

        if (!query.storage->engine->arguments)
            throw Exception("LOGICAL_ERROR Incorrect CREATE query: engine argument is nullptr.", ErrorCodes::LOGICAL_ERROR);

        if (query.storage->engine->parameters)
            throw Exception("Engine definition cannot take the form of a parametric function", ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);

        findCreatorByName(query.storage->engine->name)(Arguments
        {
            .engine_name = query.storage->engine->name,
            .engine_args = query.storage->engine->arguments->children,
            .storage_def = query.storage,
            .query = query,
            .relative_data_path = relative_data_path,
            .table_id = StorageID(query.database, query.table, query.uuid),
            .local_context = local_context,
            .context = context,
            .columns = columns,
            .constraints = constraints,
            .attach = query.attach,
            .has_force_restore_data_flag = has_force_restore_data_flag
        });
    }
}

StorageFactory & StorageFactory::instance()
{
    static StorageFactory ret;
    return ret;
}

const StorageFactory::CreatorFn & StorageFactory::findCreatorByName(const String & engine_name) const
{
    auto it = storages.find(engine_name);
    if (it == storages.end())
    {
        auto hints = getHints(engine_name);
        if (!hints.empty())
            throw Exception("Unknown table engine " + engine_name + ". Maybe you meant: " + toString(hints), ErrorCodes::UNKNOWN_STORAGE);
        else
            throw Exception("Unknown table engine " + engine_name, ErrorCodes::UNKNOWN_STORAGE);
    }

    return it->second.creator_fn;
}

}
