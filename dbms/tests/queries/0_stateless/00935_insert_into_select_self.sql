DROP TABLE IF EXISTS test.test_log;
CREATE TABLE test.test_log (`index` UInt8) ENGINE = Log;

INSERT INTO test.test_log SELECT * FROM test.test_log;

DROP TABLE test.test_log;
