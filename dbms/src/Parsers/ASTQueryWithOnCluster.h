#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTTraitWithNamedChildren.h>

namespace DB
{

/// TODO: Quite messy.
class ASTQueryWithOnCluster
{
public:
    using Pos = IParser::Pos;

    /// Should be parsed from ON CLUSTER <cluster> clause
    String cluster;

    /// new_database should be used by queries that refer to default db
    ///  and default_database is specified for remote server
    virtual ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database = {}) const = 0;

    void formatOnCluster(const IAST::FormatSettings & settings) const;

    /// Parses " CLUSTER [cluster|'cluster'] " clause
    static bool parse(Pos & pos, std::string & cluster_str, Expected & expected);

    virtual ~ASTQueryWithOnCluster() = default;
    ASTQueryWithOnCluster() = default;
    ASTQueryWithOnCluster(const ASTQueryWithOnCluster &) = default;
    ASTQueryWithOnCluster & operator=(const ASTQueryWithOnCluster &) = default;

protected:
    template <typename T>
    static ASTPtr removeOnCluster(ASTPtr query_ptr, const std::string & new_database)
    {
        T & query = static_cast<T &>(*query_ptr);

        query.cluster.clear();
        if (!query.database)
            query.database = std::make_shared<ASTIdentifier>(new_database);

        return query_ptr;
    }

    template <typename Trait>
    static ASTPtr removeTraitOnCluster(ASTPtr query_ptr, const std::string & new_database)
    {
        auto & trait_query = query_ptr->as<ASTTraitWithOutput<Trait> &>();

        trait_query.cluster.clear();
        if (auto & table_expression = trait_query.getChildRef(ASTTraitWithOutput<Trait>::Children::TABLE_EXPRESSION))
        {
            const auto & [database_name, table_name] = getDatabaseAndTable(table_expression);
            if (!database_name.empty())
                replaceDatabaseAndTable(table_expression, new_database, table_name);
        }

        return query_ptr;
    }

    template <typename T>
    static ASTPtr removeOnCluster(ASTPtr query_ptr)
    {
        T & query = static_cast<T &>(*query_ptr);
        query.cluster.clear();
        return query_ptr;
    }
};

}
