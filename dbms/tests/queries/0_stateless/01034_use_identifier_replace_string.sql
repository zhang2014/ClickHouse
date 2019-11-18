SET enable_debug_queries = 1;

ANALYZE DROP DATABASE `database_name`;
AST DROP DATABASE `database_name`;

ANALYZE DETACH DATABASE `database_name`;
AST DETACH DATABASE `database_name`;

ANALYZE CREATE DATABASE `database_name`;
AST CREATE DATABASE `database_name`;

ANALYZE CHECK TABLE `table_name`;
AST CHECK TABLE `table_name`;

ANALYZE CHECK TABLE `database_name`.`table_name`;
AST CHECK TABLE `database_name`.`table_name`;

ANALYZE DROP TABLE `table_name`;
AST DROP TABLE `table_name`;

ANALYZE DROP TABLE `database_name`.`table_name`;
AST DROP TABLE `database_name`.`table_name`;

ANALYZE DETACH TABLE `table_name`;
AST DETACH TABLE `table_name`;

ANALYZE DETACH TABLE `database_name`.`table_name`;
AST DETACH TABLE `database_name`.`table_name`;

ANALYZE TRUNCATE TABLE `table_name`;
AST TRUNCATE TABLE `table_name`;

ANALYZE TRUNCATE TABLE `database_name`.`table_name`;
AST TRUNCATE TABLE `database_name`.`table_name`;

ANALYZE CREATE TABLE `table_name`(test UInt8) ENGINE = Memory;
AST CREATE TABLE `table_name`(test UInt8) ENGINE = Memory;

ANALYZE CREATE TABLE `database_name`.`table_name`(test UInt8) ENGINE = Memory;
AST CREATE TABLE `database_name`.`table_name`(test UInt8) ENGINE = Memory;

ANALYZE OPTIMIZE TABLE `table_name`;
AST OPTIMIZE TABLE `table_name`;

ANALYZE OPTIMIZE TABLE `database_name`.`table_name`;
AST OPTIMIZE TABLE `database_name`.`table_name`;

ANALYZE SHOW TABLES FROM `database_name`;
AST SHOW TABLES FROM `database_name`;

ANALYZE DESCRIBE TABLE `database_name`.`table_name`;
AST DESCRIBE TABLE `database_name`.`table_name`;

ANALYZE SHOW CREATE DATABASE `database_name`;
AST SHOW CREATE DATABASE `database_name`;

ANALYZE SHOW CREATE TABLE `database_name`.`table_name`;
AST SHOW CREATE TABLE `database_name`.`table_name`;

ANALYZE SHOW CREATE DICTIONARY `database_name`.dictionary_name;
AST SHOW CREATE DICTIONARY `database_name`.dictionary_name;

ANALYZE EXISTS TABLE `database_name`.`table_name`;
AST EXISTS TABLE `database_name`.`table_name`;

ANALYZE EXISTS DICTIONARY `database_name`.`table_name`;
AST EXISTS DICTIONARY `database_name`.`table_name`;

-- ANALYZE CREATE DICTIONARY ordinary_db.dict1 ( key_column UInt64 DEFAULT 0, second_column UInt8 DEFAULT 1, third_column String DEFAULT 'qqq' ) PRIMARY KEY key_column SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict')) LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());
-- AST ANALYZE CREATE DICTIONARY ordinary_db.dict1 ( key_column UInt64 DEFAULT 0, second_column UInt8 DEFAULT 1, third_column String DEFAULT 'qqq' ) PRIMARY KEY key_column SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict')) LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT());
-- TODO: use query
-- TODO: alter query
-- TODO: watch query


