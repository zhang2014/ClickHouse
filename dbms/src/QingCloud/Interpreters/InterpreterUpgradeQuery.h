#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{

class InterpreterUpgradeQuery : public IInterpreter
{
public:
    InterpreterUpgradeQuery(const ASTPtr & node, const Context & context);

    BlockIO execute() override;

private:
    ASTPtr node;
    const Context & context;
};

}