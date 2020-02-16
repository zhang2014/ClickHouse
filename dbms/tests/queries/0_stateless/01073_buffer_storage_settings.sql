DROP TABLE IF EXISTS buffer_01073_destination;
DROP TABLE IF EXISTS buffer_01073_use_default_settings;
DROP TABLE IF EXISTS buffer_01073_use_custom_user_settings;

CREATE TABLE buffer_01073_destination(a UInt32) ENGINE = Memory;
CREATE TABLE buffer_01073_use_default_settings(a UInt32) ENGINE = Buffer(currentDatabase(), 'buffer_01073_destination');
CREATE TABLE buffer_01073_use_custom_user_settings(a UInt32) ENGINE = Buffer(currentDatabase(), 'buffer_01073_destination') SETTINGS flusher_min_time = 10000, flusher_max_time = 10000, flusher_max_rows = 5;

INSERT INTO buffer_01073_use_default_settings SELECT number FROM numbers(1048577);

SELECT COUNT() FROM buffer_01073_destination;
SELECT COUNT() FROM buffer_01073_use_default_settings;

INSERT INTO buffer_01073_use_custom_user_settings SELECT number FROM numbers(5);

SELECT COUNT() FROM buffer_01073_destination;
SELECT COUNT() FROM buffer_01073_use_custom_user_settings;

INSERT INTO buffer_01073_use_custom_user_settings SELECT number FROM numbers(1);

SELECT COUNT() FROM buffer_01073_destination;
SELECT COUNT() FROM buffer_01073_use_custom_user_settings;

DROP TABLE IF EXISTS buffer_01073_destination;
DROP TABLE IF EXISTS buffer_01073_use_default_settings;
DROP TABLE IF EXISTS buffer_01073_use_custom_user_settings;
