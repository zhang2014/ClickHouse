#pragma once

#include <Core/Field.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

struct ASTPaxosQuery : public ASTQueryWithOutput
{
    /** Get the text that identifies this element. */
    String getID() const override { return ("Paxos_" + kind); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTPaxosQuery>(*this);
        res->kind = kind;
        res->names_and_values = names_and_values;
        return res;
    }

    String kind;
    std::unordered_map<String, Field> names_and_values;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked frame) const override
    {

        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " PAXOS " << kind  << (settings.hilite ? hilite_none : "");

        size_t index = 0;
        for (const auto & name_and_value : names_and_values)
        {
            if (index == 0)
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " , " << (settings.hilite ? hilite_none : "");
            settings.ostr << (settings.hilite ? hilite_keyword : "") << name_and_value.first << (settings.hilite ? hilite_none : "");
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " = " << (settings.hilite ? hilite_none : "");
            settings.ostr << (settings.hilite ? hilite_keyword : "") << toString(name_and_value.second.get()) << (settings.hilite ? hilite_none : "");
            ++index;
        }
    }
};

}
