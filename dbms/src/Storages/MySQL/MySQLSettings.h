#pragma once

#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/SettingsCommon.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

class Context;
class ASTStorage;
class ASTSetQuery;
using ASTSetQueryPtr = std::shared_ptr<ASTSetQuery>;

struct MySQLSettings
{
#define APPLY_FOR_MySQL_SETTINGS(M) \
    M(SettingString, remote_address, "", "Address of the MySQL server.") \
    M(SettingString, remote_database, "", "Database name on the MySQL server.") \
    M(SettingString, remote_table_name, "", "A Table name on the MySQL server.") \
    M(SettingString, user, "", "The MySQL User") \
    M(SettingString, password, "", "User password") \
    M(SettingBool, replace_query, 0, "Flag that sets query substitution INSERT INTO to REPLACE INTO. If replace_query=1, the query is replaced") \
    M(SettingString, on_duplicate_clause, "", "Adds the ON DUPLICATE KEY on_duplicate_clause expression to the INSERT query") \

#define DECLARE(TYPE, NAME, DEFAULT, DESCRIPTION) \
    TYPE NAME {DEFAULT};

    APPLY_FOR_MySQL_SETTINGS(DECLARE)

#undef DECLARE

    ASTSetQueryPtr set_variables_query = std::make_shared<ASTSetQuery>();

public:
    void loadFromQuery(const ASTStorage & storage_def);

    void loadFromEngineArguments(ASTs & arguments, const Context & context);

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & clickhouse_database,
                        const String & clickhouse_table_name);

private:
    void loadFromQuery(const ASTSetQuery * set_query);

    void loadSettingsFromConfig(const ASTSetQueryPtr &setting_query, const Poco::Util::AbstractConfiguration &config,
                                const String &config_prefix);
};

}