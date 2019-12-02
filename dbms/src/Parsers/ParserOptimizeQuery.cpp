#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDatabaseAndTableExpression.h>

#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{


bool ParserOptimizeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_optimize_table("OPTIMIZE TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_final("FINAL");
    ParserKeyword s_deduplicate("DEDUPLICATE");
    ParserToken s_dot(TokenType::Dot);
    ParserDatabaseAndTableExpression table_expression_p;
    ParserPartition partition_p;

    bool final = false;
    bool deduplicate = false;
    String cluster_str;

    ASTPtr table_expression;
    ASTPtr optimize_partition;

    if (!s_optimize_table.ignore(pos, expected))
        return false;

    if (!table_expression_p.parse(pos, table_expression, expected))
        return false;

    if (ParserKeyword{"ON"}.ignore(pos, expected) && !ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
        return false;

    if (s_partition.ignore(pos, expected))
    {
        if (!partition_p.parse(pos, optimize_partition, expected))
            return false;
    }

    if (s_final.ignore(pos, expected))
        final = true;

    if (s_deduplicate.ignore(pos, expected))
        deduplicate = true;

    auto query = std::make_shared<ASTOptimizeQuery>();
    query->final = final;
    query->cluster = cluster_str;
    query->deduplicate = deduplicate;
    query->setChild(ASTOptimizeQuery::Children::TABLE_EXPRESSION, std::move(table_expression));
    query->setChild(ASTOptimizeQuery::Children::OPTIMIZE_PARTITION, std::move(optimize_partition));
    node = query;

    return true;
}


}
