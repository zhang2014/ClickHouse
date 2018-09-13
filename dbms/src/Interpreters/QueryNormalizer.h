#pragma once

#include <Parsers/IAST.h>
#include "Settings.h"

namespace DB
{

class QueryNormalizer
{
public:
    void perform();

private:
    using SetOfASTs = std::set<const IAST *>;
    using MapOfASTs = std::map<ASTPtr, ASTPtr>;
    using Aliases = std::unordered_map<String, ASTPtr>;

    Aliases aliases;
    Settings & settings;

    void normalizeTreeImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias, size_t level);
};

}