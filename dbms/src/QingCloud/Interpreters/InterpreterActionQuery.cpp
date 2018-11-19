#include <QingCloud/Interpreters/InterpreterActionQuery.h>
#include <QingCloud/Parsers/ASTActionQuery.h>
#include <Common/typeid_cast.h>
#include <QingCloud/Storages/StorageQingCloud.h>


namespace DB
{


InterpreterActionQuery::InterpreterActionQuery(const ASTPtr &query_ptr_, const Context &context_)
    : query_ptr(query_ptr_), context(context_)
{
}

BlockIO InterpreterActionQuery::execute()
{
    ASTActionQuery * action_query = typeid_cast<ASTActionQuery *>(query_ptr.get());
    StoragePtr storage = context.getTable(action_query->database, action_query->table);

    if (StorageQingCloud * q_storage = dynamic_cast<StorageQingCloud *>(storage.get()))
    {
        q_storage->receiveActionNotify(action_query->action_name, action_query->version);
        return {};
    }

    throw Exception("Cannot run upgrade query for without QingCloud Storage.", ErrorCodes::LOGICAL_ERROR);
}

}
