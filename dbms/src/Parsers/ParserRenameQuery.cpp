#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTRenameQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserDatabaseAndTableExpression.h>


namespace DB
{

bool ParserRenameQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_rename_table("RENAME TABLE");
    ParserKeyword s_to("TO");
    ParserToken s_comma(TokenType::Comma);
    ParserDatabaseAndTableExpression simple_table_expression_p;

    if (!s_rename_table.ignore(pos, expected))
        return false;

    auto rename_expression_list = std::make_shared<ASTExpressionList>();

    while (true)
    {
        if (!rename_expression_list->children.empty() && !s_comma.ignore(pos))
            break;

        ASTPtr to_table_expression;
        ASTPtr from_table_expression;

        if (!simple_table_expression_p.parse(pos, from_table_expression, expected)
            || !s_to.ignore(pos)
            || !simple_table_expression_p.parse(pos, to_table_expression, expected))
            return false;

        auto rename_expression = std::make_shared<ASTRenameQueryExpression>();
        rename_expression->setChild(ASTRenameQueryExpression::Children::TO_TABLE_EXPRESSION, std::move(to_table_expression));
        rename_expression->setChild(ASTRenameQueryExpression::Children::FROM_TABLE_EXPRESSION, std::move(from_table_expression));
        rename_expression_list->children.push_back(rename_expression);
    }

    String cluster_str;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = std::make_shared<ASTRenameQuery>();
    query->cluster = cluster_str;
    query->setChild(ASTRenameQuery::Children::RENAME_EXPRESSION_LIST, rename_expression_list);
    node = query;
    return true;
}


}
