#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>


namespace DB
{

/** RENAME query expression
 */
class ASTRenameQueryExpressionTrait : public IAST
{
protected:
    enum class Children : UInt8
    {
        FROM_TABLE_EXPRESSION,
        TO_TABLE_EXPRESSION,
    };

    void reset(ASTRenameQueryExpressionTrait & /*res*/) const {}

    /** Get the text that identifies this element. */
    void identifier(char /*delim*/, ASTTraitOStream<ASTRenameQueryExpressionTrait> & out) const { out << "RenameQueryExpression"; }

    void formatSyntax(ASTTraitOStream<ASTRenameQueryExpressionTrait> & out, const FormatSettings & settings, FormatState &, FormatStateStacked) const
    {
        out << Children::FROM_TABLE_EXPRESSION << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "")
            << Children::TO_TABLE_EXPRESSION;
    }
};

using ASTRenameQueryExpression = ASTTraitWithNonOutput<ASTRenameQueryExpressionTrait>;

/** RENAME query
  */
class ASTRenameQueryTrait : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    ASTs getExpressionList()
    {
        auto & query = this->as<ASTTraitWithNamedChildren<ASTRenameQueryTrait> &>();
        return query.getChildRef(Children::RENAME_EXPRESSION_LIST)->children;
    }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        auto query_ptr = clone();
        auto & query = query_ptr->as<ASTTraitWithNamedChildren<ASTRenameQueryTrait> &>();

        query.cluster.clear();
        auto & rename_expression_list = query.getChildRef(Children::RENAME_EXPRESSION_LIST);
        for (auto & rename_expression_node : rename_expression_list->children)
        {
            auto rename_expression = rename_expression_node->as<ASTRenameQueryExpression>();
            auto & to_description = rename_expression->getChildRef(ASTRenameQueryExpression::Children::TO_TABLE_EXPRESSION);
            auto & from_description = rename_expression->getChildRef(ASTRenameQueryExpression::Children::FROM_TABLE_EXPRESSION);

            auto to_table_expression = to_description->as<ASTDatabaseAndTableExpression>();
            auto from_table_expression = from_description->as<ASTDatabaseAndTableExpression>();

            if (to_table_expression && to_table_expression->getChild(ASTDatabaseAndTableExpression::Children::DATABASE))
                to_table_expression->setChild(ASTDatabaseAndTableExpression::Children::DATABASE, std::make_shared<ASTIdentifier>(new_database));

            if (from_table_expression && from_table_expression->getChild(ASTDatabaseAndTableExpression::Children::DATABASE))
                from_table_expression->setChild(ASTDatabaseAndTableExpression::Children::DATABASE, std::make_shared<ASTIdentifier>(new_database));
        }

        return query_ptr;
    }

protected:
    enum class Children : UInt8
    {
        RENAME_EXPRESSION_LIST  /// ExpressionList(ASTRenameQueryExpression)
    };

    void reset(ASTRenameQueryTrait & res) const { cloneOutputOptions(res); }

    /** Get the text that identifies this element. */
    void identifier(char /*delim*/, ASTTraitOStream<ASTRenameQueryTrait> & out) const { out << "RenameQuery"; }

    void formatSyntax(ASTTraitOStream<ASTRenameQueryTrait> & out, const FormatSettings & settings, FormatState &, FormatStateStacked) const
    {
        out << (settings.hilite ? hilite_keyword : "") << "RENAME TABLE " << (settings.hilite ? hilite_none : "")
            << Children::RENAME_EXPRESSION_LIST;

        formatOnCluster(settings);
    }
};

using ASTRenameQuery = ASTTraitWithOutput<ASTRenameQueryTrait>;

inline ASTPtr makeSimpleRenameQuery(const String & from_database, const String & from_table, const String & to_database, const String & to_table)
{
    auto rename_query = std::make_shared<ASTRenameQuery>();
    auto rename_expression_list = std::make_shared<ASTExpressionList>();
    auto rename_query_expression = std::make_shared<ASTRenameQueryExpression>();

    rename_query_expression->setChild(ASTRenameQueryExpression::Children::TO_TABLE_EXPRESSION, makeSimpleTableExpression(to_database, to_table));
    rename_query_expression->setChild(ASTRenameQueryExpression::Children::FROM_TABLE_EXPRESSION, makeSimpleTableExpression(from_database, from_table));

    rename_expression_list->children.push_back(rename_query_expression);
    rename_query->setChild(ASTRenameQuery::Children::RENAME_EXPRESSION_LIST, rename_expression_list);
    return rename_query;
}

}
