#include <QingCloud/Interpreters/InterpreterActionQuery.h>
#include <QingCloud/Parsers/ASTActionQuery.h>
#include <Common/typeid_cast.h>
#include <QingCloud/Storages/StorageQingCloud.h>
#include <QingCloud/Interpreters/SafetyPointFactory.h>


namespace DB
{


InterpreterActionQuery::InterpreterActionQuery(const ASTPtr &query_ptr_, const Context &context_)
    : query_ptr(query_ptr_), context(context_)
{
}

BlockIO InterpreterActionQuery::execute()
{
    ASTActionQuery * action_query = typeid_cast<ASTActionQuery *>(query_ptr.get());
    SafetyPointFactory::instance().receiveActionNotify(action_query->sync_name, action_query->action_name, action_query->reentry, action_query->from);
    return {};
}

}
