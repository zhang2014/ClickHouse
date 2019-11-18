#include <Parsers/ParserUseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTUseQuery.h>


namespace DB
{

bool ParserUseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_use("USE");
    ParserIdentifier name_p;

    if (!s_use.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTUseQuery>();

    if (!name_p.parse(pos, query->database, expected))
        return false;

    node = query;

    return true;
}

}
