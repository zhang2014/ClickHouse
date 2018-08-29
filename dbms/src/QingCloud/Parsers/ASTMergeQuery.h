#pragma once

#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

struct ASTMergeQuery : public ASTQueryWithOutput
{
    /** Get the text that identifies this element. */
    String getID() const override { return ("MergeQuery_" + database + "_" + table); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTMergeQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

    std::string database;
    std::string table;
    ASTPtr source_versions;
    ASTPtr dist_versions;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "MERGE TABLE " << (settings.hilite ? hilite_none : "");

        if (!table.empty())
        {
            if (!database.empty())
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(database) << (settings.hilite ? hilite_none : "");
                settings.ostr << ".";
            }
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(table) << (settings.hilite ? hilite_none : "");
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " VERSIONS " << (settings.hilite ? hilite_none : "");
        source_versions->formatImpl(settings, state, frame);
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " TO " << (settings.hilite ? hilite_none : "");
        dist_versions->formatImpl(settings, state, frame);
    }
};

}
