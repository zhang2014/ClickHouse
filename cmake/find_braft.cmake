include (CMakePushCheckState)
cmake_push_check_state ()

option (ENABLE_BRAFT "Enable braft (embedded distributed coordinator)" ON)

if (ENABLE_BRAFT AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft/raft.h")
    message (WARNING "submodule contrib/braft is missing. to fix try run: \n git submodule update --init --recursive")
    set (ENABLE_BRAFT  0)
endif ()

if (ENABLE_BRAFT)
    set (USE_BRAFT 1)
    set (BRAFT_LIBRARY "braft")
endif ()

message (STATUS "Using braft=${USE_BRAFT}: ${BRAFT_LIBRARY}")

cmake_pop_check_state ()
