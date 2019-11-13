DROP TABLE IF EXISTS test_order_by_fill;

CREATE TABLE test_order_by_fill (date Date, date_time DateTime, value UInt32) ENGINE = Memory;

SELECT '*** gaps fill and with expression ***';
WITH gaps_fill_begin = 0, gaps_fill_end = 3, gaps_step = 1 SELECT value FROM test_order_by_fill ORDER BY value WITH FILL FROM gaps_fill_begin TO gaps_fill_end STEP gaps_step;
