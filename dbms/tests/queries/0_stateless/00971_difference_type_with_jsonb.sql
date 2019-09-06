DROP TABLE IF EXISTS test.test;

CREATE TABLE test.test(day Date, id UInt64, test JSONB) ENGINE = MergeTree PARTITION BY day ORDER BY (id, day);

INSERT INTO test.test VALUES('2019-01-01', 1, 'true');
INSERT INTO test.test VALUES('2019-01-01', 1, 'false');

INSERT INTO test.test VALUES('2019-01-01', 2, '2147483648'); -- Int32 max value
INSERT INTO test.test VALUES('2019-01-01', 2, '-2147483648'); -- Int32 min value
INSERT INTO test.test VALUES('2019-01-01', 2, '4294967296'); -- UInt32 max value
INSERT INTO test.test VALUES('2019-01-01', 2, '-4294967296'); -- Use Int64 type
INSERT INTO test.test VALUES('2019-01-01', 2, '9223372036854775807'); -- Int64 max value
INSERT INTO test.test VALUES('2019-01-01', 2, '-9223372036854775807'); -- Int64 min value
INSERT INTO test.test VALUES('2019-01-01', 2, '18446744073709552045'); -- UInt64 max value

INSERT INTO test.test VALUES('2019-01-01', 3, '"test_string_data"');

INSERT INTO test.test VALUES('2019-01-01', 4, '{"uid": 123456, "view_url": "http://yandex.ru", "is_first_view": true}');
INSERT INTO test.test VALUES('2019-01-01', 4, '{"uid": 123456, "view_url": "http://yandex.ru", "is_first_view": true, "session_info": {"cookies": "abcdefg", "country_code": 1, "is_male": true}}');

SELECT * FROM test.test;

DROP TABLE IF EXISTS test.test;
