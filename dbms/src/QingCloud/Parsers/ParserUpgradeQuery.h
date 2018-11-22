#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTIdentifier.h>
#include <QingCloud/Parsers/ASTUpgradeQuery.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

class ParserUpgradeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "Cluster Query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserKeyword s_to("TO");
        ParserKeyword s_upgrade("UPGRADE VERSION");

        ASTPtr origin_version;
        ASTPtr upgrade_version;

        ParserToken s_dot(TokenType::Dot);
        ParserIdentifier name_p;
        ParserStringLiteral p_to_version;
        ParserStringLiteral p_from_version;

        auto query = std::make_shared<ASTUpgradeQuery>();

        if (!s_upgrade.ignore(pos, expected))
            return false;

        if (!p_from_version.parse(pos, origin_version, expected))
            return false;

        if (!s_to.ignore(pos, expected))
            return false;

        if (!p_to_version.parse(pos, upgrade_version, expected))
            return false;

        query->origin_version = typeid_cast<ASTLiteral *>(origin_version.get())->value.safeGet<String>();
        query->upgrade_version = typeid_cast<ASTLiteral *>(upgrade_version.get())->value.safeGet<String>();
        node = query;

        return true;
    }
};

}
