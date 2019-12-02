#pragma once

#include <iomanip>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTTraitWithNamedChildren.h>


namespace DB
{

/** Query SHOW TABLES or SHOW DATABASES
  */
class ASTShowTablesQueryTrait : public ASTQueryWithOutput
{
public:
    bool databases{false};
    bool dictionaries{false};
    bool temporary{false};
    bool not_like{false};

protected:
    enum class Children : UInt8
    {
        FROM,
        LIKE,
        LIMIT_LENGTH,
    };

    void reset(ASTShowTablesQueryTrait & res) const { cloneOutputOptions(res); }

    /** Get the text that identifies this element. */
    void identifier(char /*delim*/, ASTTraitOStream<ASTShowTablesQueryTrait> & out) const { out << "ShowTables"; }

    void formatSyntax(ASTTraitOStream<ASTShowTablesQueryTrait> & out, const FormatSettings & settings, FormatState &, FormatStateStacked) const
    {
        if (databases)
        {
            out << (settings.hilite ? hilite_keyword : "") << "SHOW DATABASES" << (settings.hilite ? hilite_none : "");
        }
        else
        {
            out << (settings.hilite ? hilite_keyword : "") << "SHOW " << (temporary ? "TEMPORARY " : "")
                << (dictionaries ? "DICTIONARIES" : "TABLES") << (settings.hilite ? hilite_none : "");

            if (out.node->getChild(Children::FROM))
                out << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "") << Children::FROM;

            if (out.node->getChild(Children::LIKE))
                out << (settings.hilite ? hilite_keyword : "") << " LIKE " << (settings.hilite ? hilite_none : "") << Children::LIKE;

            if (out.node->getChild(Children::LIMIT_LENGTH))
                out << (settings.hilite ? hilite_keyword : "") << " LIMIT " << (settings.hilite ? hilite_none : "") << Children::LIMIT_LENGTH;
        }
    }
};

using ASTShowTablesQuery = ASTTraitWithOutput<ASTShowTablesQueryTrait>;

}
