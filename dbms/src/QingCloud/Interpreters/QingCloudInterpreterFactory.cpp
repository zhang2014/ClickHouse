#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <QingCloud/Interpreters/InterpreterQingCloudDDLQuery.h>
#include <QingCloud/Interpreters/QingCloudInterpreterFactory.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Context.h>
#include <QingCloud/Parsers/ASTMergeQuery.h>
#include <QingCloud/Interpreters/InterpreterMergeQuery.h>
#include <QingCloud/Parsers/ASTPaxosQuery.h>
#include <QingCloud/Interpreters/InterpreterPaxosQuery.h>
#include <QingCloud/Parsers/ASTUpgradeQuery.h>
#include "InterpreterUpgradeQuery.h"


namespace DB
{
std::unique_ptr<IInterpreter> QingCloudInterpreterFactory::get(ASTPtr & query, Context & context, QueryProcessingStage::Enum stage)
{
    if (typeid_cast<ASTMergeQuery *>(query.get()))
        return std::make_unique<InterpreterMergeQuery>(query, context);
    else if (typeid_cast<ASTPaxosQuery *>(query.get()))
        return std::make_unique<InterpreterPaxosQuery>(query, context);
    else if (typeid_cast<ASTUpgradeQuery *>(query.get()))
        return std::make_unique<InterpreterUpgradeQuery>(query.get(), context);

    std::unique_ptr<IInterpreter> local_interpreter = InterpreterFactory::get(query, context, stage);

    if (!context.getSettingsRef().internal_query)
    {
        if (typeid_cast<ASTDropQuery *>(query.get()) || typeid_cast<ASTAlterQuery *>(query.get()) || typeid_cast<ASTRenameQuery *>(query.get())
            || typeid_cast<ASTCreateQuery *>(query.get()))
            return std::make_unique<InterpreterQingCloudDDLQuery>(std::move(local_interpreter), context, query);
    }

    return local_interpreter;
}

}