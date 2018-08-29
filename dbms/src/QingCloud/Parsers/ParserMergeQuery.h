#pragma once

#include <QingCloud/Parsers/ASTMergeQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{
/** Query of form
 * MERGE [TABLE] [database.]table VERSIONS ... TO ...
 */
class ParserMergeQuery : public IParserBase
{
protected:
    const char * getName() const { return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        auto query = std::make_shared<ASTMergeQuery>();
        node = query;

        ParserKeyword s_to("TO");
        ParserKeyword s_versions("VERSIONS");
        ParserKeyword s_merge_table("MERGE TABLE");
        ParserLiteral p_version;
        ParserList  p_versions(std::make_unique<ParserLiteral>(), std::make_unique<ParserToken>(TokenType::Comma));

        if (!s_merge_table.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
            return false;

        if (!s_versions.ignore(pos, expected))
            return false;

        if (!p_versions.parse(pos, query->source_versions, expected))
            return false;

        if (!s_to.ignore(pos, expected))
            return false;

        if (!p_version.parse(pos, query->dist_versions, expected))
            return false;

        if (!query->source_versions)
            query->children.push_back(query->source_versions);
        if (!query->dist_versions)
            query->children.push_back(query->dist_versions);

        return true;
    }
};

}
