#pragma once

#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

struct ASTActionQuery : public ASTQueryWithOutput
{
    /** Get the text that identifies this element. */
    String getID() const override
    { return ("ActionQuery_" + database + "_" + table); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTActionQuery>();
        res->table = table;
        res->version = version;
        res->database = database;
        res->action_name = action_name;
        return res;
    }

    String table;
    String version;
    String database;
    String action_name;

protected:
    void formatQueryImpl(const FormatSettings &settings, FormatState & /*state*/, FormatStateStacked frame) const override
    {

        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " ACTION NOTIFY " << (settings.hilite ? hilite_none : "");
        settings.ostr << "'" << action_name << "'";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " TABLE " << (settings.hilite ? hilite_none : "");
        if (!table.empty())
        {
            if (!database.empty())
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(database)
                              << (settings.hilite ? hilite_none : "");
                settings.ostr << ".";
            }
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(table)
                          << (settings.hilite ? hilite_none : "");
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " VERSION " << (settings.hilite ? hilite_none : "");
        settings.ostr << "'" << version << "'";
    }
};

}
