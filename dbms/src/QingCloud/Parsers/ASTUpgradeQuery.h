#pragma once

#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

struct ASTUpgradeQuery : public ASTQueryWithOutput
{
    String origin_version;
    String upgrade_version;

    String getID() const override { return ("UpgradeQuery_" + origin_version + "_" + upgrade_version); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTUpgradeQuery>();
        res->origin_version = origin_version;
        res->upgrade_version = upgrade_version;
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked frame) const override
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "UPGRADE VERSION " << (settings.hilite ? hilite_none : "");

        settings.ostr << origin_version;
        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " TO " << (settings.hilite ? hilite_none : "");
        settings.ostr << upgrade_version;
    }
};

}