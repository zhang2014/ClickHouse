#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

struct ExtractFunctionData
{
    using TypeToVisit = ASTFunction;

    const std::vector<ASTFunction *> functions;
    const std::vector<ASTFunction *> aggregate_functions;

    void visit(ASTFunction & identifier, ASTPtr &);
};

using ExtractFunctionMatcher = OneTypeMatcher<ExtractFunctionData>;
using ExtractFunctionVisitor = InDepthNodeVisitor<ExtractFunctionMatcher, true>;

}
