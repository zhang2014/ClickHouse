#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    if (const auto & use_query = query_ptr->as<ASTUseQuery>())
    {
        const String & new_database = use_query->database;
        context.getSessionContext().setCurrentDatabase(new_database);
    }
    return {};
}

}
