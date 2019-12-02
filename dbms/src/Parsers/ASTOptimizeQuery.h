#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

/** OPTIMIZE query
  */
class ASTOptimizeTrait : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    /// A flag can be specified - perform optimization "to the end" instead of one step.
    bool final;
    /// Do deduplicate (default: false)
    bool deduplicate;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeTraitOnCluster<ASTOptimizeTrait>(clone(), new_database);
    }

protected:
    enum class Children : UInt8
    {
        TABLE_EXPRESSION,
        OPTIMIZE_PARTITION, /// The partition to optimize can be specified.
    };

    void reset(ASTOptimizeTrait & res) const { cloneOutputOptions(res); }

    /** Get the text that identifies this element. */
    void identifier(char delim, ASTTraitOStream<ASTOptimizeTrait> & out) const
    {
        out << "OptimizeQuery" << delim << Children::TABLE_EXPRESSION << (final ? "_final" : "") << (deduplicate ? "_deduplicate" : "");
    }

    void formatSyntax(ASTTraitOStream<ASTOptimizeTrait> & out, const FormatSettings & settings, FormatState &, FormatStateStacked) const
    {
        out << (settings.hilite ? hilite_keyword : "") << "OPTIMIZE TABLE " << (settings.hilite ? hilite_none : "")
            << Children::TABLE_EXPRESSION;

        formatOnCluster(settings);

        if (out.node->getChild(Children::OPTIMIZE_PARTITION))
            out << (settings.hilite ? hilite_keyword : "") << " PARTITION " << (settings.hilite ? hilite_none : "")
                << Children::OPTIMIZE_PARTITION;

        if (final)
            out << (settings.hilite ? hilite_keyword : "") << " FINAL" << (settings.hilite ? hilite_none : "");

        if (deduplicate)
            out << (settings.hilite ? hilite_keyword : "") << " DEDUPLICATE" << (settings.hilite ? hilite_none : "");
    }
};

using ASTOptimizeQuery = ASTTraitWithOutput<ASTOptimizeTrait>;

}
