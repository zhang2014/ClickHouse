SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.alter_compression_codec;

CREATE TABLE test.alter_compression_codec (
    somedate Date CODEC(LZ4),
    id UInt64 CODEC(NONE)
) ENGINE = MergeTree() PARTITION BY somedate ORDER BY id;

INSERT INTO test.alter_compression_codec VALUES('2018-01-01', 1);
INSERT INTO test.alter_compression_codec VALUES('2018-01-01', 2);
SELECT * FROM test.alter_compression_codec ORDER BY id;

ALTER TABLE test.alter_compression_codec ADD COLUMN alter_column String DEFAULT 'default_value' CODEC(ZSTD);

INSERT INTO test.alter_compression_codec VALUES('2018-01-01', 3, '3', '3');
INSERT INTO test.alter_compression_codec VALUES('2018-01-01', 4, '4', '4');
SELECT * FROM test.alter_compression_codec ORDER BY id;

ALTER TABLE test.alter_compression_codec data_one CODEC(NONE);

INSERT INTO test.alter_compression_codec VALUES('2018-01-01', 5, '5', '5');
INSERT INTO test.alter_compression_codec VALUES('2018-01-01', 6, '6', '6');
SELECT * FROM test.alter_compression_codec ORDER BY id;

OPTIMIZE TABLE test.alter_compression_codec FINAL;
SELECT * FROM test.alter_compression_codec ORDER BY id;

DROP TABLE IF EXISTS test.alter_compression_codec;
