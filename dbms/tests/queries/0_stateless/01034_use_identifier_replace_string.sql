SET enable_debug_queries = 1;

ANALYZE CHECK TABLE database_name.table_name;

ANALYZE CREATE DATABASE database_name;

ANALYZE CREATE TABLE database_name.table_name(test UInt8) ENGINE = Memory;

ANALYZE CREATE DICTIONARY ordinary_db.dict1 ( key_column UInt64 DEFAULT 0, second_column UInt8 DEFAULT 1, third_column String DEFAULT 'qqq' ) PRIMARY KEY key_column SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict')) LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());

ANALYZE DROP DATABASE database_name;

ANALYZE DROP TABLE database_name.table_name;

ANALYZE DETACH DATABASE database_name;

ANALYZE DETACH TABLE database_name.table_name;

ANALYZE TRUNCATE TABLE database_name.table_name;

ANALYZE OPTIMIZE TABLE database_name.table_name;

-- TODO: alter query
-- TODO: watch query

ANALYZE SHOW TABLE FROM database_name;

ANALYZE USE database_name;

ANALYZE DESCRIBE TABLE database_name.table_name;

ANALYZE SHOW CREATE DATABASE database_name;

ANALYZE SHOW CREATE TABLE database_name.table_name;

ANALYZE SHOW CREATE DICTIONARY database_name.dictionary_name;

ANALYZE EXISTS TABLE database_name.table_name;

ANALYZE EXISTS DICTIONARY database_name.table_name;






