#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{

class InterpreterActionQuery : public IInterpreter
{
public:
    InterpreterActionQuery(const ASTPtr & query_ptr_, const Context & context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;

    const Context & context;
};

}