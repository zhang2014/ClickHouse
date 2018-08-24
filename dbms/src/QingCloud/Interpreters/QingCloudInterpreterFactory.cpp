#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <QingCloud/Interpreters/InterpreterQingCloudDDLQuery.h>
#include <QingCloud/Interpreters/QingCloudInterpreterFactory.h>
#include <Common/typeid_cast.h>


namespace DB
{
std::unique_ptr<IInterpreter> QingCloudInterpreterFactory::get(ASTPtr & query, Context & context, QueryProcessingStage::Enum stage)
{
    std::unique_ptr<IInterpreter> local_interpreter = InterpreterFactory::get(query, context, stage);

    if (typeid_cast<ASTDropQuery *>(query.get()) || typeid_cast<ASTAlterQuery *>(query.get()) || typeid_cast<ASTRenameQuery *>(query.get())
        || typeid_cast<ASTCreateQuery *>(query.get()))
        return std::make_unique<InterpreterQingCloudDDLQuery>(local_interpreter, context, query);

    return local_interpreter;
}

}