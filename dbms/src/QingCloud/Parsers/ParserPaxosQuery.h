#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserPaxos : public IParserBase
{
protected:
    const char * getName() const { return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);

};

}