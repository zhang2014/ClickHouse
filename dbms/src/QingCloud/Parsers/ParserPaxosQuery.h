#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Core/Field.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTLiteral.h>
#include <QingCloud/Parsers/ASTPaxosQuery.h>
#include <iostream>

namespace DB
{

static bool parseNameValuePair(std::pair<String, Field> &name_value_pair, IParser::Pos &pos, Expected &expected)
{
    ParserIdentifier name_p;
    ParserLiteral value_p;
    ParserToken s_eq(TokenType::Equals);

    ASTPtr name;
    ASTPtr value;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_eq.ignore(pos, expected))
        return false;

    if (!value_p.parse(pos, value, expected))
        return false;

    name_value_pair.first = typeid_cast<const ASTIdentifier *>(name.get())->name;
    name_value_pair.second = typeid_cast<const ASTLiteral *>(value.get())->value;

    return true;
}

class ParserPaxos : public IParserBase
{
protected:
    const char * getName() const { return "ALTER query"; }

    bool parseImpl(Pos &pos, ASTPtr &node, Expected &expected)
    {
        ASTPtr kind;
        ParserToken s_comma(TokenType::Comma);
        std::unordered_map<String, Field> names_values;

        if (!ParserKeyword("PAXOS").ignore(pos))
            return false;

        if (!ParserIdentifier().parse(pos, kind, expected))
            return false;

        while (true)
        {
            if (!names_values.empty() && !s_comma.ignore(pos))
                break;

            std::pair<String, Field> name_value_pair;
            if (!parseNameValuePair(name_value_pair, pos, expected))
                return false;
            names_values.insert(name_value_pair);
        }

        auto paxos_query = std::make_shared<ASTPaxosQuery>();
        paxos_query->kind = typeid_cast<const ASTIdentifier *>(kind.get())->name;
        paxos_query->names_and_values = names_values;
        node = paxos_query;

        return true;
    }

};

}