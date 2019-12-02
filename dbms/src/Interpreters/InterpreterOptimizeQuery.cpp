#include <Storages/IStorage.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterOptimizeQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


BlockIO InterpreterOptimizeQuery::execute()
{
    const auto & ast = query_ptr->as<ASTOptimizeQuery &>();
    const auto & table_expression = ast.getChild(ASTOptimizeQuery::Children::TABLE_EXPRESSION);
    const auto & [database_name, table_name] = getDatabaseAndTable(table_expression);

    if (!ast.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, {database_name});

    StoragePtr table = context.getTable(database_name, table_name);
    table->optimize(query_ptr, ast.getChild(ASTOptimizeQuery::Children::OPTIMIZE_PARTITION), ast.final, ast.deduplicate, context);
    return {};
}

}
