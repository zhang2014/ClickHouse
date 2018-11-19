#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <QingCloud/Parsers/ParserPaxosQuery.h>
#include <QingCloud/Parsers/ParserUpgradeQuery.h>
#include "ParserActionQuery.h"

namespace DB
{

class ParserQingCloudQuery : public IParserBase
{
protected:
    const char *getName() const override { return "Query with output"; }

    bool parseImpl(Pos &pos, ASTPtr &node, Expected &expected) override
    {
        ParserPaxos paxos_p;
        ParserActionQuery action_query_p;
        ParserUpgradeQuery upgrade_query_p;

        return paxos_p.parse(pos, node, expected) || upgrade_query_p.parse(pos, node, expected) || action_query_p.parse(pos, node, expected);
    }
};

}
