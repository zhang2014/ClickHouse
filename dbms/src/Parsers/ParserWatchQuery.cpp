/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserWatchQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserDatabaseAndTableExpression.h>


namespace DB
{

bool ParserWatchQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_watch("WATCH");
    ParserKeyword s_limit("LIMIT");
    ParserKeyword s_events("EVENTS");
    ParserToken s_dot(TokenType::Dot);

    ParserDatabaseAndTableExpression simple_table_expression_p;

    ASTPtr limit_length;
    ASTPtr table_expression;

    auto query = std::make_shared<ASTWatchQuery>();

    if (!s_watch.ignore(pos, expected))
    {
        return false;
    }

    if (!simple_table_expression_p.parse(pos, table_expression, expected))
        return false;

    /// EVENTS
    if (s_events.ignore(pos, expected))
    {
        query->is_watch_events = true;
    }

    /// LIMIT length
    if (s_limit.ignore(pos, expected))
    {
        ParserNumber num;

        if (!num.parse(pos, limit_length, expected))
            return false;
    }

    query->setChild(ASTWatchQuery::Children::LIMIT_LENGTH, std::move(limit_length));
    query->setChild(ASTWatchQuery::Children::TABLE_EXPRESSION, std::move(table_expression));
    node = query;

    return true;
}


}
