#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/IStorage.h>
#include <Interpreters/DDLWorker.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

InterpreterRenameQuery::InterpreterRenameQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


struct RenameDescription
{
    RenameDescription(const ASTPtr & node, const String & current_database)
    {
        const auto & rename_expression = node->as<ASTRenameQueryExpression>();
        const auto & to_table_expression = rename_expression->getChild(ASTRenameQueryExpression::Children::TO_TABLE_EXPRESSION);
        const auto & from_table_expression = rename_expression->getChild(ASTRenameQueryExpression::Children::FROM_TABLE_EXPRESSION);

        std::tie(to_database_name, to_table_name) = getDatabaseAndTable(to_table_expression, current_database);
        std::tie(from_database_name, from_table_name) = getDatabaseAndTable(from_table_expression, current_database);
    }

    String from_database_name;
    String from_table_name;

    String to_database_name;
    String to_table_name;
};


BlockIO InterpreterRenameQuery::execute()
{
    const auto & rename = query_ptr->as<ASTRenameQuery &>();

    if (!rename.cluster.empty())
    {
        NameSet databases;
        const auto & expression_list = rename.getChild(ASTRenameQuery::Children::RENAME_EXPRESSION_LIST);

        for (const auto & rename_expression_node : expression_list->children)
        {
            const auto & rename_expression = rename_expression_node->as<ASTRenameQueryExpression>();
            const auto & to_table_expression = rename_expression->getChild(ASTRenameQueryExpression::Children::TO_TABLE_EXPRESSION);
            const auto & from_table_expression = rename_expression->getChild(ASTRenameQueryExpression::Children::FROM_TABLE_EXPRESSION);

            const auto & [to_database_name, to_table_name] = getDatabaseAndTable(to_table_expression);
            const auto & [from_database_name, from_table_name] = getDatabaseAndTable(from_table_expression);

            databases.emplace(from_database_name);
            databases.emplace(to_database_name);
        }

        return executeDDLQueryOnCluster(query_ptr, context, std::move(databases));
    }

    String path = context.getPath();
    String current_database = context.getCurrentDatabase();

    /// To avoid deadlocks, we must acquire locks for tables in same order in any different RENAMES.
    struct UniqueTableName
    {
        String database_name;
        String table_name;

        UniqueTableName(const String & database_name_, const String & table_name_)
            : database_name(database_name_), table_name(table_name_) {}

        bool operator< (const UniqueTableName & rhs) const
        {
            return std::tie(database_name, table_name) < std::tie(rhs.database_name, rhs.table_name);
        }
    };

    /// Don't allow to drop tables (that we are renaming); don't allow to create tables in places where tables will be renamed.
    std::map<UniqueTableName, std::unique_ptr<DDLGuard>> table_guards;

    /** In case of error while renaming, it is possible that only part of tables was renamed
      *  or we will be in inconsistent state. (It is worth to be fixed.)
      */

    std::vector<RenameDescription> descriptions;
    const auto & expression_list = rename.getChild(ASTRenameQuery::Children::RENAME_EXPRESSION_LIST);
    descriptions.reserve(expression_list->children.size());

    for (const auto & rename_expression : expression_list->children)
    {
        descriptions.emplace_back(rename_expression, current_database);

        UniqueTableName from(descriptions.back().from_database_name, descriptions.back().from_table_name);
        UniqueTableName to(descriptions.back().to_database_name, descriptions.back().to_table_name);

        table_guards[from];
        table_guards[to];
    }

    /// Must do it in consistent order.
    for (auto & table_guard : table_guards)
        table_guard.second = context.getDDLGuard(table_guard.first.database_name, table_guard.first.table_name);

    for (auto & elem : descriptions)
    {
        context.assertTableDoesntExist(elem.to_database_name, elem.to_table_name);
        auto from_table = context.getTable(elem.from_database_name, elem.from_table_name);
        auto from_table_lock = from_table->lockExclusively(context.getCurrentQueryId());

        context.getDatabase(elem.from_database_name)->renameTable(
            context,
            elem.from_table_name,
            *context.getDatabase(elem.to_database_name),
            elem.to_table_name,
            from_table_lock);
    }

    return {};
}


}
