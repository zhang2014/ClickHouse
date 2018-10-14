DROP TABLE IF EXISTS test.compression_codec;

-- 开启缓存于关闭缓存的情况下分别测试
CREATE TABLE test.compression_codec(day Date CODEC(ZSTD), its UInt32 CODEC(Delta(UInt32), LZ4HC(2)))

INSERT INTO test.compression_codec('2018-01-01', '')

SELECT * FROM test.compression_codec;

DROP TABLE IF EXISTS test.compression_codec;
