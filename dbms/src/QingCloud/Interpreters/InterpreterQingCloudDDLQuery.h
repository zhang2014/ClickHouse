#pragma once

#include <Interpreters/IInterpreter.h>

namespace DB
{
class InterpreterQingCloudDDLQuery : public IInterpreter
{
public:
    InterpreterQingCloudDDLQuery(std::unique_ptr<IInterpreter> local_interpreter, Context & context, ASTPtr & query);

    BlockIO execute() override;

private:
    std::unique_ptr<IInterpreter> local_interpreter;
    Context & context;
    ASTPtr & query;
};

}