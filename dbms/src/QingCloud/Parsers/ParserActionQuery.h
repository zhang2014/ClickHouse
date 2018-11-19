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
        /// "ACTION NOTIFY '" + action_name + "' FROM VERSION '"+ action_in_version + "'"
        ParserKeyword s_table("TABLE");
        ParserKeyword s_version("VERSION");
        ParserKeyword s_action("ACTION NOTIFY");

        ParserIdentifier name_p;
        ParserStringLiteral string_p;
        ParserToken s_dot(TokenType::Dot);

        ASTPtr table;
        ASTPtr version;
        ASTPtr database;
        ASTPtr action_name;

        if (!s_action.ignore(pos, expected))
            return false;

        if (!string_p.parse(pos, action_name, expected))
            return false;

        if (!s_table.parse(pos, expected))
            return false;

        if (!name_p.parse(pos, table, expected))
            return false;

        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }

        if (!s_version.ignore(pos, expected))
            return false;

        if (!string_p.parse(pos, version, expected))
            return false;

        auto query = std::make_shared<ASTActionQuery>();

        if (database)
            query->database = typeid_cast<ASTIdentifier *>(database.get())->name;

        query->table = typeid_cast<ASTIdentifier *>(table.get())->name;
        query->version = typeid_cast<ASTLiteral *>(version.get())->value.safeGet<String>();
        query->action_name = typeid_cast<ASTLiteral *>(action_name.get())->value.safeGet<String>();
        node = query;

        return true;
    }
};

}
