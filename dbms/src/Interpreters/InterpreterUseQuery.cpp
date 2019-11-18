#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    const String & new_database = getIdentifierName(query_ptr->as<ASTUseQuery &>().database);
    context.getSessionContext().setCurrentDatabase(new_database);
    return {};
}

}
