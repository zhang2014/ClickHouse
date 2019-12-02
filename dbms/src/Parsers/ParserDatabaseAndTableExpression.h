#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>

namespace DB
{

class ParserDatabaseAndTableExpression : public IParserBase
{
protected:
    const char * getName() const { return "DatabaseAndTableExpression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        ParserIdentifier identifier_p;
        ParserToken s_dot(TokenType::Dot);

        ASTPtr table;
        ASTPtr database;

        if (!identifier_p.parse(pos, table, expected))
            return false;

        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!identifier_p.parse(pos, table, expected))
                return false;
        }

        auto query = std::make_shared<ASTDatabaseAndTableExpression>();

        if (table)
            query->setChild(ASTDatabaseAndTableExpression::Children::TABLE, std::move(table));

        if (database)
            query->setChild(ASTDatabaseAndTableExpression::Children::DATABASE, std::move(database));

        node = query;
        return true;
    }
};

}
