SET enable_debug_queries = 1;

ANALYZE SHOW TABLES FROM database_name LIKE '%match_database_name%' LIMIT 5

AST SHOW TABLES FROM database_name LIKE '%match_database_name%' LIMIT 5;

ANALYZE WATCH database_name.table_name EVENTS LIMIT 5;

AST WATCH database_name.table_name EVENTS LIMIT 5;

ANALYZE RENAME TABLE from_database_name_1.from_table_name_1 TO to_database_name_1.to_table_name_1, from_database_name_2.from_table_name_2 TO to_database_name_2.to_table_name_2;

AST RENAME TABLE from_database_name_1.from_table_name_1 TO to_database_name_1.to_table_name_1, from_database_name_2.from_table_name_2 TO to_database_name_2.to_table_name_2;
