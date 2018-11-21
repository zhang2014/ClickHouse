#pragma once

#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <IO/WriteHelpers.h>

namespace DB
{

struct ASTActionQuery : public ASTQueryWithOutput
{
    /** Get the text that identifies this element. */
    String getID() const override { return ("ActionQuery_" + action_name); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTActionQuery>();
        res->from = from;
        res->reentry = reentry;
        res->action_name = action_name;
        return res;
    }

    String from;
    UInt64 reentry;
    String action_name;

protected:
    void formatQueryImpl(const FormatSettings &settings, FormatState & /*state*/, FormatStateStacked frame) const override
    {

        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " ACTION NOTIFY " << (settings.hilite ? hilite_none : "");
        settings.ostr << "'" << action_name << "'";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " REENTRY " << (settings.hilite ? hilite_none : "");
        settings.ostr << toString(reentry);

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " FROM " << (settings.hilite ? hilite_none : "");
        settings.ostr << "'" << from << "'";
    }
};

}
