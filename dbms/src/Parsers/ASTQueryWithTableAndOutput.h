#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTTraitWithNamedChildren.h>

namespace DB
{

class ASTDatabaseAndTableExpressionTrait : public IAST
{
protected:
    enum class Children : UInt8
    {
        DATABASE,
        TABLE,
    };

    void reset(ASTDatabaseAndTableExpressionTrait &) const {}

    /** Get the text that identifies this element. */
    void identifier(char /*delim*/, ASTTraitOStream<ASTDatabaseAndTableExpressionTrait> & out) const { out << "DatabaseAndTableExpression"; }

    void formatSyntax(ASTTraitOStream<ASTDatabaseAndTableExpressionTrait> & out, const FormatSettings & settings, FormatState &, FormatStateStacked frame) const
    {
        std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

        const auto & table = out.node->getChild(Children::TABLE);
        const auto & database = out.node->getChild(Children::DATABASE);

        out << indent_str;

        if (database)
        {
            out << Children::DATABASE;

            if (table)
                out << ".";
        }

        if (table)
            out << Children::TABLE;
    }
};

using ASTDatabaseAndTableExpression = ASTTraitWithNonOutput<ASTDatabaseAndTableExpressionTrait>;

inline std::pair<String, String> getDatabaseAndTable(const ASTPtr & node, const String & default_database = "")
{
    if (const auto & table_expression = node->as<ASTDatabaseAndTableExpression>())
    {
        const auto & table = table_expression->getChild(ASTDatabaseAndTableExpression::Children::TABLE);
        const auto & database = table_expression->getChild(ASTDatabaseAndTableExpression::Children::DATABASE);

        const auto & table_name = table ? getIdentifierName(table) : "";
        const auto & database_name = database ? getIdentifierName(database) : default_database;

        return std::make_pair(database_name, table_name);
    }

    return {};
}

inline ASTPtr makeSimpleTableExpression(const String & database, const String & table)
{
    auto table_expression = std::make_shared<ASTDatabaseAndTableExpression>();
    table_expression->setChild(ASTDatabaseAndTableExpression::Children::TABLE, std::make_shared<ASTIdentifier>(table));
    table_expression->setChild(ASTDatabaseAndTableExpression::Children::DATABASE, std::make_shared<ASTIdentifier>(database));
    return table_expression;
}

inline void replaceDatabaseAndTable(ASTPtr & node, const String & new_database, const String & new_table)
{
    if (auto table_expression = node->as<ASTDatabaseAndTableExpression>())
    {
        table_expression->setChild(ASTDatabaseAndTableExpression::Children::TABLE, std::make_shared<ASTIdentifier>(new_table));
        table_expression->setChild(ASTDatabaseAndTableExpression::Children::DATABASE, std::make_shared<ASTIdentifier>(new_database));
    }
}

/** Query specifying table name and, possibly, the database and the FORMAT section.
  */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    ASTPtr database;
    ASTPtr table;
    bool temporary{false};

    String tableName() const { return getIdentifierName(table); }
    String databaseName(const String & default_name = "") const { return database ? getIdentifierName(database) : default_name ; }

    bool onlyDatabase() const { return !table && database; }

protected:
    String getTableAndDatabaseID(char delim) const;

    void formatHelper(const FormatSettings & settings, const char * name) const;

    void formatTableAndDatabase(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
};


template <typename AstIDAndQueryNames>
class ASTQueryWithTableAndOutputImpl : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const override { return AstIDAndQueryNames::ID + (delim + getTableAndDatabaseID(delim)); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTQueryWithTableAndOutputImpl<AstIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        formatHelper(settings, temporary ? AstIDAndQueryNames::QueryTemporary : AstIDAndQueryNames::Query);
    }
};

}
