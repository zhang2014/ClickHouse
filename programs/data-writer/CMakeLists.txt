set(CLICKHOUSE_DATA_PART_WRITER_LIB_SRC
    DataPartWriter.cpp
    LocalEnvironment.cpp
    MergeTreeDataPartOutputStream.cpp
)

add_library(clickhouse_data_part_writer_static STATIC ${CLICKHOUSE_DATA_PART_WRITER_LIB_SRC})
target_link_libraries(clickhouse_data_part_writer_static PUBLIC
        clickhouse_aggregate_functions clickhouse_common_config clickhouse_common_io
        clickhouse_dictionaries clickhouse_functions clickhouse_parsers clickhouse_storages_system clickhouse_table_functions)

#set(C_LIB ${CMAKE_BINARY_DIR}/libcombi.a)
#
#add_custom_target(data_writer
#        ALL COMMAND libtool -static -o ${C_LIB}
#        $<TARGET_FILE:re2>
#        $<TARGET_FILE:roaring>
#        $<TARGET_FILE:Poco::Foundation>
#        $<TARGET_FILE:re2_st>
#        $<TARGET_FILE:fmt>
#        $<TARGET_FILE:lz4>
#        $<TARGET_FILE:dbms>
#        $<TARGET_FILE:zstd>
#        $<TARGET_FILE:daemon>
#        $<TARGET_FILE:common>
#        $<TARGET_FILE:loggers>
#        $<TARGET_FILE:cityhash>
#        $<TARGET_FILE:string_utils>
#        $<TARGET_FILE:clickhouse_functions_url>
#        $<TARGET_FILE:clickhouse_functions_array>
#        $<TARGET_FILE:clickhouse_functions_gatherutils>
#        $<TARGET_FILE:consistent-hashing>
#        $<TARGET_FILE:consistent-hashing-sumbur>
#        $<TARGET_FILE:metrohash>
#        $<TARGET_FILE:murmurhash>
#        $<TARGET_FILE:clickhouse_parsers>
#        $<TARGET_FILE:clickhouse_common_io>
#        $<TARGET_FILE:clickhouse_functions>
#        $<TARGET_FILE:clickhouse_dictionaries>
#        $<TARGET_FILE:clickhouse_common_config>
#        $<TARGET_FILE:clickhouse_storages_system>
#        $<TARGET_FILE:clickhouse_table_functions>
#        $<TARGET_FILE:clickhouse_common_zookeeper>
#        $<TARGET_FILE:clickhouse_aggregate_functions>
#        $<TARGET_FILE:clickhouse_dictionaries_embedded>
#        $<TARGET_FILE:clickhouse_data_part_writer_static>
#        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/temporary_dir
#
#        DEPENDS
#        re2
#        roaring
#        re2_st
#        fmt
#        lz4
#        dbms
#        zstd
#        daemon
#        common
#        loggers
#        cityhash
#        string_utils
#        Poco::Foundation
#        clickhouse_common_io
#        clickhouse_functions
#        clickhouse_dictionaries
#        clickhouse_common_config
#        clickhouse_storages_system
#        clickhouse_table_functions
#        clickhouse_common_zookeeper
#        clickhouse_aggregate_functions
#        clickhouse_dictionaries_embedded
#        clickhouse_data_part_writer_static
#        clickhouse_functions_url
#        clickhouse_functions_array
#        clickhouse_functions_gatherutils
#        consistent-hashing
#        consistent-hashing-sumbur
#        metrohash
#        murmurhash
#)

#message(STATUS "new lib : ${CMAKE_BINARY_DIR}/libcombi.a")

#add_glob(io_headers ${CMAKE_CURRENT_SOURCE_DIR} ../../src/IO/*.h)
#add_glob(core_headers ${CMAKE_CURRENT_SOURCE_DIR} ../../src/Core/*.h)
#add_glob(common_headers ${CMAKE_CURRENT_SOURCE_DIR} ../../src/Common/*.h)
#add_glob(columns_headers ${CMAKE_CURRENT_SOURCE_DIR} ../../src/Columns/*.h)
#add_glob(base_common_headers ${CMAKE_CURRENT_SOURCE_DIR} ../../base/common/*.h)
#add_glob(hash_table_headers ${CMAKE_CURRENT_SOURCE_DIR} ../../src/Common/HashTable/*.h)
#
#list(REMOVE_ITEM common_headers ../../src/Common/OpenSSLHelpers.h)
#
#file(COPY ${io_headers} DESTINATION headers/IO)
#file(COPY ${core_headers} DESTINATION headers/Core)
#file(COPY ${common_headers} DESTINATION headers/Common)
#file(COPY ${columns_headers} DESTINATION headers/Columns)
#file(COPY ${base_common_headers} DESTINATION headers/base/common)
#file(COPY ${hash_table_headers} DESTINATION headers/Common/HashTable)
#
#file(COPY DataPartWriter.h DESTINATION headers)
#file(COPY IPartDataOutputStream.h DESTINATION headers)
#
#file(COPY ../../base/ext DESTINATION headers)
#file(COPY ../../contrib/boost/boost DESTINATION headers)
#file(COPY ../../contrib/fmtlib/include/fmt DESTINATION headers)
#file(COPY ../../base/pcg-random/pcg_extras.hpp DESTINATION headers)
#file(COPY ../../base/pcg-random/pcg_random.hpp DESTINATION headers)
#file(COPY ../../base/pcg-random/pcg_uint128.hpp DESTINATION headers)
#file(COPY ../../contrib/cityhash102/include/city.h DESTINATION headers)
#file(COPY ../../contrib/poco/Foundation/include/Poco DESTINATION headers)
#file(COPY ../../contrib/cityhash102/include/citycrc.h DESTINATION headers)
