#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>
#include "ASTQueryWithTableAndOutput.h"


namespace DB
{

String ASTQueryWithTableAndOutput::getTableAndDatabaseID(char delim) const
{
    if (onlyDatabase())
        return databaseName();
    else
    {
        if (const auto & table = getTable())
        {
            if (const auto & database = getDatabase())
                return getIdentifierName(database) + delim + getIdentifierName(table);

            return getIdentifierName(table);
        }
    }

    throw Exception("LOGICAL ERROR: no define database and table in query.", ErrorCodes::LOGICAL_ERROR);
}

void ASTQueryWithTableAndOutput::formatTableAndDatabase(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked frame) const
{
    if (onlyDatabase())
        settings.ostr << backQuoteIfNeed(databaseName());
    else
    {
        std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

        if (const auto & table = getTable())
        {
            if (const auto & database = getDatabase())
            {
                settings.ostr << indent_str << backQuoteIfNeed(getIdentifierName(database));
                settings.ostr << ".";
            }
            settings.ostr << indent_str << backQuoteIfNeed(getIdentifierName(table));
        }
    }
}

static void setChildren(ASTs & children, size_t & pos, ASTPtr && set_node)
{
    if (set_node)
    {
        if (pos != ASTQueryWithTableAndOutput::npos)
            children[pos] = set_node;
        else
        {
            pos = children.size();
            children.emplace_back(set_node);
        }
    }
    else if (!set_node && pos != ASTQueryWithTableAndOutput::npos)
    {
        children.erase(children.begin() + pos);
    }
}

void ASTQueryWithTableAndOutput::setTable(ASTPtr && table)
{
    setChildren(children, table_pos, std::move(table));
}

void ASTQueryWithTableAndOutput::setDatabase(ASTPtr && database)
{
    setChildren(children, database_pos, std::move(database));
}

}

