#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query specifying table name and, possibly, the database and the FORMAT section.
  */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    static constexpr size_t npos = static_cast<size_t>(-1);

    bool isTemporary() const { return temporary; }
    bool onlyDatabase() const { return database_pos != npos && table_pos == npos; }

    ASTPtr getTable() const { return table_pos == npos ? ASTPtr{} : children[table_pos]; }
    ASTPtr getDatabase() const { return database_pos == npos ? ASTPtr{} : children[database_pos]; }

    String tableName() const { return getIdentifierName(getTable()); }
    String databaseName(const String & default_name = "") const { return database_pos == npos ? default_name : getIdentifierName(getDatabase()); }

    void setTable(ASTPtr && table);
    void setDatabase(ASTPtr && database);
    void setTemporary(bool is_temporary_table) { temporary = is_temporary_table; }
protected:

    bool temporary{false};
    size_t table_pos = npos;
    size_t database_pos = npos;

    String getTableAndDatabaseID(char delim) const;

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
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        const char * query_name = isTemporary() ? AstIDAndQueryNames::QueryTemporary : AstIDAndQueryNames::Query;

        settings.ostr << (settings.hilite ? hilite_keyword : "") << query_name << " " << (settings.hilite ? hilite_none : "");
        formatTableAndDatabase(settings, state, frame);
    }
};

}
