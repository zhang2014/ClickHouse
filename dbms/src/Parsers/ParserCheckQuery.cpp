#include <Parsers/ParserCheckQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserCheckQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check_table("CHECK TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserToken s_dot(TokenType::Dot);

    ParserPartition partition_parser;
    ParserIdentifier identifier_parser;

    ASTPtr table;
    ASTPtr database;
    ASTPtr partition;

    if (!s_check_table.ignore(pos, expected))
        return false;

    if (!identifier_parser.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos))
    {
        database = table;
        if (!identifier_parser.parse(pos, table, expected))
            return false;
    }

    if (s_partition.ignore(pos, expected))
    {
        if (!partition_parser.parse(pos, partition, expected))
            return false;
    }

    auto query = std::make_shared<ASTCheckQuery>();

    query->partition = partition;
    query->setTable(std::move(table));
    query->setDatabase(std::move(database));

    node = query;
    return true;
}

}
