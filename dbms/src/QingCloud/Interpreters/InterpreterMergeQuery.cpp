#include <QingCloud/Interpreters/InterpreterMergeQuery.h>
#include <Common/typeid_cast.h>
#include <QingCloud/Parsers/ASTMergeQuery.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <QingCloud/Storages/StorageQingCloudbak.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

InterpreterMergeQuery::InterpreterMergeQuery(const ASTPtr &query_ptr_, const Context &context_)
    :query_ptr(query_ptr_), context(context_)
{
}

BlockIO InterpreterMergeQuery::execute()
{
    auto & merge = typeid_cast<ASTMergeQuery &>(*query_ptr);
    const String & table_name = merge.table;
    String database_name = merge.database.empty() ? context.getCurrentDatabase() : merge.database;
    StoragePtr table = context.getTable(database_name, table_name);

    auto * storage = dynamic_cast<StorageQingCloud *>(table.get());

    if (!storage)
        throw Exception(database_name + "." + table_name + " is not QingCloud Engine Table.", ErrorCodes::BAD_ARGUMENTS);

    std::vector<String> source_versions;
    for (const auto ast_version : merge.source_versions->children)
        source_versions.emplace_back(typeid_cast<ASTLiteral *>(ast_version.get())->value.safeGet<String>());

    storage->mergeVersions(source_versions, static_cast<ASTLiteral *>(merge.dist_versions.get())->value.safeGet<String>());

    return {};
}


}
