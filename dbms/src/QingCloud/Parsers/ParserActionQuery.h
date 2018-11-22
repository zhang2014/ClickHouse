#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTIdentifier.h>
#include <QingCloud/Parsers/ASTUpgradeQuery.h>
#include <Parsers/ASTLiteral.h>
#include "ASTActionQuery.h"

namespace DB
{

class ParserActionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "Action Query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        /// "ACTION NOTIFY '" + name + " REENTRY " + toString(reentry) + " FROM '" + listen_hosts[0] + "'"
        ParserKeyword s_from("FROM");
        ParserKeyword s_reentry("REENTRY");
        ParserKeyword s_action("ACTION NOTIFY");

        ParserIdentifier name_p;
        ParserLiteral reentry_p;
        ParserStringLiteral string_p;
        ParserToken s_dot(TokenType::Dot);

        ASTPtr from;
        ASTPtr reentry;
        ASTPtr action_name;

        if (!s_action.ignore(pos, expected))
            return false;

        if (!string_p.parse(pos, action_name, expected))
            return false;

        if (!s_reentry.ignore(pos, expected))
            return false;

        if (!reentry_p.parse(pos, reentry, expected))
            return false;

        if (!s_from.ignore(pos, expected))
            return false;

        if (!string_p.parse(pos, from, expected))
            return false;

        auto query = std::make_shared<ASTActionQuery>();

        query->from = typeid_cast<ASTLiteral *>(from.get())->value.safeGet<String>();
        query->reentry = typeid_cast<ASTLiteral *>(reentry.get())->value.safeGet<UInt64>();
        query->action_name = typeid_cast<ASTLiteral *>(action_name.get())->value.safeGet<String>();
        node = query;

        return true;
    }
};

}
