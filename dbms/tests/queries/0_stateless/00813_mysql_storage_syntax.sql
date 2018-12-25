DROP TABLE IF EXISTS test.test_mapping_database_and_table;
DROP TABLE IF EXISTS test.test_no_mapping_database_and_table;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_1;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_2;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_3;
CREATE TABLE test.test_bad_mysql_table(day Date, xxx String) ENGINE = MySQL(); -- { serverError 42 }
CREATE TABLE test.test_bad_mysql_table(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database_name', 'table_name', 'user_name', 'password', 1, 'xxx = 5'); -- { serverError 42 }
CREATE TABLE test.test_mapping_database_and_table(day Date, xxx String) ENGINE = MySQL() SETTINGS remote_address = '127.0.0.1', user = 'user_name', password = 'password';
CREATE TABLE test.test_no_mapping_database_and_table(day Date, xxx String) ENGINE = MySQL() SETTINGS remote_address = '127.0.0.1', remote_database = 'database', remote_table_name = 'table', user = 'user_name', password = 'password';
CREATE TABLE test.test_mysql_table_compatibility_1(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database', 'table', 'user_name', 'password');
CREATE TABLE test.test_mysql_table_compatibility_2(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database', 'table', 'user_name', 'password', 1);
CREATE TABLE test.test_mysql_table_compatibility_3(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database', 'table', 'user_name', 'password', 0, 'xxx = 5');

DROP TABLE IF EXISTS test.test_mapping_database_and_table;
DROP TABLE IF EXISTS test.test_no_mapping_database_and_table;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_1;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_2;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_3;