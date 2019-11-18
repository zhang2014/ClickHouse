#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>
#include "ASTQueryWithTableAndOutput.h"


namespace DB
{

template <typename Base>
void ASTQueryWithTable<Base>::formatTableAndDatabase(
    const typename Base::FormatSettings & settings, typename Base::FormatState & /*state*/, typename Base::FormatStateStacked frame) const
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

template <typename Base>
static void setChildren(ASTs & children, size_t & pos, ASTPtr && set_node)
{
    if (set_node)
    {
        if (pos != ASTQueryWithTable<Base>::npos)
            children[pos] = set_node;
        else
        {
            pos = children.size();
            children.emplace_back(set_node);
        }
    }
    else if (!set_node && pos != ASTQueryWithTable<Base>::npos)
    {
        children.erase(children.begin() + pos);
    }
}

template <typename Base>
void ASTQueryWithTable<Base>::setTable(ASTPtr && table)
{
    setChildren<Base>(Base::children, table_pos, std::move(table));
}

template <typename Base>
void ASTQueryWithTable<Base>::setDatabase(ASTPtr && database)
{
    setChildren<Base>(Base::children, database_pos, std::move(database));
}

template class ASTQueryWithTable<IAST>;
template class ASTQueryWithTable<ASTQueryWithOutput>;

}

