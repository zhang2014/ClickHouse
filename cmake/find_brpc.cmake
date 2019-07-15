option (ENABLE_BRPC "Enable brpc (rpc subunit of braft)" ON)

if (ENABLE_BRPC AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/brpc/README.md")
    message (WARNING "submodule contrib/brpc is missing. to fix try run: \n git submodule update --init --recursive")
    set (ENABLE_BRPC  0)
endif ()

if (ENABLE_BRPC)
    set (USE_BRPC 1)
    set (BRPC_LIBRARY "brpc")
    set (BRPC_LIBRARY_INCLUDE "${ClickHouse_SOURCE_DIR}/contrib/brpc/src")
endif ()

message (STATUS "Using brpc=${ENABLE_BRPC}: ${BRPC_LIBRARY} : ${BRPC_LIBRARY_INCLUDE}")
