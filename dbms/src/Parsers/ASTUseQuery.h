#pragma once

#include <Parsers/IAST.h>
#include <Common/quoteString.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{


/** USE query
  */
class ASTUseQuery : public IAST
{
public:
    ASTPtr database;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "UseQuery" + (delim + getIdentifierName(database)); }

    ASTPtr clone() const override { return std::make_shared<ASTUseQuery>(*this); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "USE " << (settings.hilite ? hilite_none : "")
                      << backQuoteIfNeed(getIdentifierName(database));
        return;
    }
};

}
