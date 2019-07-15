if (NOT USE_INTERNAL_PROTOBUF_LIBRARY)
    # compatiable with protobuf which was compiled old C++ ABI
    set(CMAKE_CXX_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=0")
    set(CMAKE_C_FLAGS "")
    if (NOT (CMAKE_VERSION VERSION_LESS "3.8.0"))
        unset(CMAKE_CXX_STANDARD)
    endif ()
endif()

set (BRAFT_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/output/include")
set (BRAFT_SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft")

set (Protobuf_IMPORT_DIRS "${ClickHouse_SOURCE_DIR}/contrib/braft/src")
set(PROTO_FILES
    ${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft/builtin_service.proto
    ${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft/cli.proto
    ${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft/enum.proto
    ${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft/errno.proto
    ${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft/file_service.proto
    ${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft/local_file_meta.proto
    ${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft/local_storage.proto
    ${ClickHouse_SOURCE_DIR}/contrib/braft/src/braft/raft.proto
)

if(USE_PROTOBUF)
    PROTOBUF_GENERATE_CPP(PROTO_SOURCES PROTO_HEADERS ${PROTO_FILES})
endif()


set (BRAFT_SRC
    ${BRAFT_SRC_DIR}/ballot.cpp
    ${BRAFT_SRC_DIR}/ballot_box.cpp
    ${BRAFT_SRC_DIR}/builtin_service_impl.cpp
    ${BRAFT_SRC_DIR}/cli.cpp
    ${BRAFT_SRC_DIR}/cli_service.cpp
    ${BRAFT_SRC_DIR}/closure_queue.cpp
    ${BRAFT_SRC_DIR}/configuration.cpp
    ${BRAFT_SRC_DIR}/configuration_manager.cpp
    ${BRAFT_SRC_DIR}/file_reader.cpp
    ${BRAFT_SRC_DIR}/file_service.cpp
    ${BRAFT_SRC_DIR}/file_system_adaptor.cpp
    ${BRAFT_SRC_DIR}/fsm_caller.cpp
    ${BRAFT_SRC_DIR}/fsync.cpp
    ${BRAFT_SRC_DIR}/log.cpp
    ${BRAFT_SRC_DIR}/log_entry.cpp
    ${BRAFT_SRC_DIR}/log_manager.cpp
    ${BRAFT_SRC_DIR}/memory_log.cpp
    ${BRAFT_SRC_DIR}/node.cpp
    ${BRAFT_SRC_DIR}/node_manager.cpp
    ${BRAFT_SRC_DIR}/protobuf_file.cpp
    ${BRAFT_SRC_DIR}/raft.cpp
    ${BRAFT_SRC_DIR}/raft_meta.cpp
    ${BRAFT_SRC_DIR}/raft_service.cpp
    ${BRAFT_SRC_DIR}/remote_file_copier.cpp
    ${BRAFT_SRC_DIR}/repeated_timer_task.cpp
    ${BRAFT_SRC_DIR}/replicator.cpp
    ${BRAFT_SRC_DIR}/route_table.cpp
    ${BRAFT_SRC_DIR}/snapshot.cpp
    ${BRAFT_SRC_DIR}/snapshot_executor.cpp
    ${BRAFT_SRC_DIR}/snapshot_throttle.cpp
    ${BRAFT_SRC_DIR}/storage.cpp
    ${BRAFT_SRC_DIR}/util.cpp

    ${BRAFT_SRC_DIR}/closure_helper.h
    ${BRAFT_SRC_DIR}/builtin_service_impl.h
    ${BRAFT_SRC_DIR}/configuration.h
    ${BRAFT_SRC_DIR}/file_service.h
    ${BRAFT_SRC_DIR}/log.h
    ${BRAFT_SRC_DIR}/node_manager.h
    ${BRAFT_SRC_DIR}/fsync.h
    ${BRAFT_SRC_DIR}/node.h
    ${BRAFT_SRC_DIR}/log_manager.h
    ${BRAFT_SRC_DIR}/memory_log.h
    ${BRAFT_SRC_DIR}/ballot_box.h
    ${BRAFT_SRC_DIR}/macros.h
    ${BRAFT_SRC_DIR}/log_entry.h
    ${BRAFT_SRC_DIR}/fsm_caller.h
    ${BRAFT_SRC_DIR}/cli.h
    ${BRAFT_SRC_DIR}/cli_service.h
    ${BRAFT_SRC_DIR}/configuration_manager.h
    ${BRAFT_SRC_DIR}/ballot.h
    ${BRAFT_SRC_DIR}/closure_queue.h
    ${BRAFT_SRC_DIR}/file_system_adaptor.h
    ${BRAFT_SRC_DIR}/file_reader.h
    ${BRAFT_SRC_DIR}/protobuf_file.h
    ${BRAFT_SRC_DIR}/raft.h
    ${BRAFT_SRC_DIR}/raft_meta.h
    ${BRAFT_SRC_DIR}/raft_service.h
    ${BRAFT_SRC_DIR}/remote_file_copier.h
    ${BRAFT_SRC_DIR}/repeated_timer_task.h
    ${BRAFT_SRC_DIR}/replicator.h
    ${BRAFT_SRC_DIR}/route_table.h
    ${BRAFT_SRC_DIR}/snapshot.h
    ${BRAFT_SRC_DIR}/snapshot_executor.h
    ${BRAFT_SRC_DIR}/snapshot_throttle.h
    ${BRAFT_SRC_DIR}/storage.h
    ${BRAFT_SRC_DIR}/util.h
)

add_library(${BRAFT_LIBRARY} ${BRAFT_SRC} ${PROTO_SOURCES} ${PROTO_HEADERS})

if (USE_INTERNAL_PROTOBUF_LIBRARY)
    add_dependencies(${BRAFT_LIBRARY} protoc)
endif()

target_include_directories(${BRAFT_LIBRARY} PRIVATE ${BRPC_INCLUDE})
target_include_directories(${BRAFT_LIBRARY} PRIVATE ${PROTO_HEADERS})
target_include_directories(${BRAFT_LIBRARY} PRIVATE ${BRAFT_INCLUDE_DIR})
target_include_directories(${BRAFT_LIBRARY} PRIVATE ${Protobuf_INCLUDE_DIR})
target_include_directories(${BRAFT_LIBRARY} SYSTEM PUBLIC "${BRAFT_INCLUDE_DIR}")
target_include_directories(${BRAFT_LIBRARY} PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/braft/src)
