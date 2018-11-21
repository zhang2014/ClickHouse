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
#include <QingCloud/Parsers/ASTPaxosQuery.h>
#include <QingCloud/Interpreters/InterpreterPaxosQuery.h>
#include <QingCloud/Parsers/ASTUpgradeQuery.h>
#include <QingCloud/Parsers/ASTActionQuery.h>
#include "InterpreterUpgradeQuery.h"
#include "InterpreterActionQuery.h"


namespace DB
{
std::unique_ptr<IInterpreter> QingCloudInterpreterFactory::get(ASTPtr & query, Context & context, QueryProcessingStage::Enum stage)
{
    if (typeid_cast<ASTPaxosQuery *>(query.get()))
        return std::make_unique<InterpreterPaxosQuery>(query, context);
    else if (typeid_cast<ASTActionQuery *>(query.get()))
        return std::make_unique<InterpreterActionQuery>(query, context);
    else if (typeid_cast<ASTUpgradeQuery *>(query.get()))
        return std::make_unique<InterpreterUpgradeQuery>(query, context);

    if (!context.getSettingsRef().internal_query)
    {
        if (typeid_cast<ASTDropQuery *>(query.get()) || typeid_cast<ASTAlterQuery *>(query.get())
            || typeid_cast<ASTRenameQuery *>(query.get()) || typeid_cast<ASTCreateQuery *>(query.get()))
            return std::make_unique<InterpreterQingCloudDDLQuery>(context, query);
    }

    return InterpreterFactory::get(query, context, stage);
}

}