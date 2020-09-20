set(CLICKHOUSE_DATA_PART_WRITER_LIB_SRC
    DataPartWriter.cpp
    LocalEnvironment.cpp
    MergeTreeDataPartOutputStream.cpp
)

add_library(clickhouse_data_part_writer_static STATIC ${CLICKHOUSE_DATA_PART_WRITER_LIB_SRC})
target_link_libraries(clickhouse_data_part_writer_static PRIVATE
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_dictionaries
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions)

install(TARGETS clickhouse_data_part_writer_static ARCHIVE DESTINATION lib)

add_custom_target (clickhouse-writer ALL DEPENDS clickhouse_data_part_writer_static clickhouse_common_io)

add_executable(test_data_writer test_data_writer.cpp)
target_link_libraries(test_data_writer PRIVATE clickhouse_data_part_writer_static dbms)
