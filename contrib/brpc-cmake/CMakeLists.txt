if (NOT USE_INTERNAL_PROTOBUF_LIBRARY)
    # compatiable with protobuf which was compiled old C++ ABI
    set(CMAKE_CXX_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=0")
    set(CMAKE_C_FLAGS "")
    if (NOT (CMAKE_VERSION VERSION_LESS "3.8.0"))
        unset(CMAKE_CXX_STANDARD)
    endif ()
endif()

#find_package(GFLAGS REQUIRED)

set (BRPC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/brpc")

set(PROTO_FILES
    ${BRPC_SOURCE_DIR}/src/idl_options.proto
    ${BRPC_SOURCE_DIR}/src/brpc/rtmp.proto
    ${BRPC_SOURCE_DIR}/src/brpc/rpc_dump.proto
    ${BRPC_SOURCE_DIR}/src/brpc/get_favicon.proto
    ${BRPC_SOURCE_DIR}/src/brpc/span.proto
    ${BRPC_SOURCE_DIR}/src/brpc/builtin_service.proto
    ${BRPC_SOURCE_DIR}/src/brpc/get_js.proto
    ${BRPC_SOURCE_DIR}/src/brpc/errno.proto
    ${BRPC_SOURCE_DIR}/src/brpc/nshead_meta.proto
    ${BRPC_SOURCE_DIR}/src/brpc/options.proto
    ${BRPC_SOURCE_DIR}/src/brpc/policy/baidu_rpc_meta.proto
    ${BRPC_SOURCE_DIR}/src/brpc/policy/hulu_pbrpc_meta.proto
    ${BRPC_SOURCE_DIR}/src/brpc/policy/public_pbrpc_meta.proto
    ${BRPC_SOURCE_DIR}/src/brpc/policy/sofa_pbrpc_meta.proto
    ${BRPC_SOURCE_DIR}/src/brpc/policy/mongo.proto
    ${BRPC_SOURCE_DIR}/src/brpc/trackme.proto
    ${BRPC_SOURCE_DIR}/src/brpc/streaming_rpc_meta.proto
)

if(USE_PROTOBUF)
    PROTOBUF_GENERATE_CPP(PROTO_SOURCES PROTO_HEADERS ${PROTO_FILES})
endif()

#configure_file(${BRPC_SOURCE_DIR}/config.h.in ${PROJECT_SOURCE_DIR}/src/butil/config.h @ONLY)

set(BUTIL_SOURCES
        ${BRPC_SOURCE_DIR}/src/butil/third_party/dmg_fp/g_fmt.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/dmg_fp/dtoa_wrapper.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/dynamic_annotations/dynamic_annotations.c
        ${BRPC_SOURCE_DIR}/src/butil/third_party/icu/icu_utf.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/superfasthash/superfasthash.c
        ${BRPC_SOURCE_DIR}/src/butil/third_party/modp_b64/modp_b64.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/nspr/prtime.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/symbolize/demangle.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/symbolize/symbolize.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/snappy/snappy-sinksource.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/snappy/snappy-stubs-internal.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/snappy/snappy.cc
        ${BRPC_SOURCE_DIR}/src/butil/third_party/murmurhash3/murmurhash3.cpp
        ${BRPC_SOURCE_DIR}/src/butil/arena.cpp
        ${BRPC_SOURCE_DIR}/src/butil/at_exit.cc
        ${BRPC_SOURCE_DIR}/src/butil/atomicops_internals_x86_gcc.cc
        ${BRPC_SOURCE_DIR}/src/butil/base64.cc
        ${BRPC_SOURCE_DIR}/src/butil/big_endian.cc
        ${BRPC_SOURCE_DIR}/src/butil/cpu.cc
        ${BRPC_SOURCE_DIR}/src/butil/debug/alias.cc
        ${BRPC_SOURCE_DIR}/src/butil/debug/asan_invalid_access.cc
        ${BRPC_SOURCE_DIR}/src/butil/debug/crash_logging.cc
        ${BRPC_SOURCE_DIR}/src/butil/debug/debugger.cc
        ${BRPC_SOURCE_DIR}/src/butil/debug/debugger_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/debug/dump_without_crashing.cc
        ${BRPC_SOURCE_DIR}/src/butil/debug/proc_maps_linux.cc
        ${BRPC_SOURCE_DIR}/src/butil/debug/stack_trace.cc
        ${BRPC_SOURCE_DIR}/src/butil/debug/stack_trace_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/environment.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/file.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/file_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/file_enumerator.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/file_enumerator_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/file_path.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/file_path_constants.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/memory_mapped_file.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/memory_mapped_file_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/scoped_file.cc
        ${BRPC_SOURCE_DIR}/src/butil/files/scoped_temp_dir.cc
        ${BRPC_SOURCE_DIR}/src/butil/file_util.cc
        ${BRPC_SOURCE_DIR}/src/butil/file_util_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/guid.cc
        ${BRPC_SOURCE_DIR}/src/butil/guid_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/hash.cc
        ${BRPC_SOURCE_DIR}/src/butil/lazy_instance.cc
        ${BRPC_SOURCE_DIR}/src/butil/location.cc
        ${BRPC_SOURCE_DIR}/src/butil/md5.cc
        ${BRPC_SOURCE_DIR}/src/butil/memory/aligned_memory.cc
        ${BRPC_SOURCE_DIR}/src/butil/memory/ref_counted.cc
        ${BRPC_SOURCE_DIR}/src/butil/memory/ref_counted_memory.cc
        ${BRPC_SOURCE_DIR}/src/butil/memory/singleton.cc
        ${BRPC_SOURCE_DIR}/src/butil/memory/weak_ptr.cc
        ${BRPC_SOURCE_DIR}/src/butil/posix/file_descriptor_shuffle.cc
        ${BRPC_SOURCE_DIR}/src/butil/posix/global_descriptors.cc
        ${BRPC_SOURCE_DIR}/src/butil/process_util.cc
        ${BRPC_SOURCE_DIR}/src/butil/rand_util.cc
        ${BRPC_SOURCE_DIR}/src/butil/rand_util_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/fast_rand.cpp
        ${BRPC_SOURCE_DIR}/src/butil/safe_strerror_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/sha1_portable.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/latin1_string_conversions.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/nullable_string16.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/safe_sprintf.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/string16.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/string_number_conversions.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/string_split.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/string_piece.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/string_util.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/string_util_constants.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/stringprintf.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/utf_offset_string_conversions.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/utf_string_conversion_utils.cc
        ${BRPC_SOURCE_DIR}/src/butil/strings/utf_string_conversions.cc
        ${BRPC_SOURCE_DIR}/src/butil/synchronization/cancellation_flag.cc
        ${BRPC_SOURCE_DIR}/src/butil/synchronization/condition_variable_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/synchronization/waitable_event_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/non_thread_safe_impl.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/platform_thread_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/simple_thread.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/thread_checker_impl.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/thread_collision_warner.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/thread_id_name_manager.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/thread_local_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/thread_local_storage.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/thread_local_storage_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/thread_restrictions.cc
        ${BRPC_SOURCE_DIR}/src/butil/threading/watchdog.cc
        ${BRPC_SOURCE_DIR}/src/butil/time/clock.cc
        ${BRPC_SOURCE_DIR}/src/butil/time/default_clock.cc
        ${BRPC_SOURCE_DIR}/src/butil/time/default_tick_clock.cc
        ${BRPC_SOURCE_DIR}/src/butil/time/tick_clock.cc
        ${BRPC_SOURCE_DIR}/src/butil/time/time.cc
        ${BRPC_SOURCE_DIR}/src/butil/time/time_posix.cc
        ${BRPC_SOURCE_DIR}/src/butil/version.cc
        ${BRPC_SOURCE_DIR}/src/butil/logging.cc
        ${BRPC_SOURCE_DIR}/src/butil/class_name.cpp
        ${BRPC_SOURCE_DIR}/src/butil/errno.cpp
        ${BRPC_SOURCE_DIR}/src/butil/find_cstr.cpp
        ${BRPC_SOURCE_DIR}/src/butil/status.cpp
        ${BRPC_SOURCE_DIR}/src/butil/string_printf.cpp
        ${BRPC_SOURCE_DIR}/src/butil/thread_local.cpp
        ${BRPC_SOURCE_DIR}/src/butil/unix_socket.cpp
        ${BRPC_SOURCE_DIR}/src/butil/endpoint.cpp
        ${BRPC_SOURCE_DIR}/src/butil/fd_utility.cpp
        ${BRPC_SOURCE_DIR}/src/butil/files/temp_file.cpp
        ${BRPC_SOURCE_DIR}/src/butil/files/file_watcher.cpp
        ${BRPC_SOURCE_DIR}/src/butil/time.cpp
        ${BRPC_SOURCE_DIR}/src/butil/zero_copy_stream_as_streambuf.cpp
        ${BRPC_SOURCE_DIR}/src/butil/crc32c.cc
        ${BRPC_SOURCE_DIR}/src/butil/containers/case_ignored_flat_map.cpp
        ${BRPC_SOURCE_DIR}/src/butil/iobuf.cpp
        ${BRPC_SOURCE_DIR}/src/butil/binary_printer.cpp
        ${BRPC_SOURCE_DIR}/src/butil/popen.cpp
        )

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(BUTIL_SOURCES ${BUTIL_SOURCES}
            ${BRPC_SOURCE_DIR}/src/butil/file_util_linux.cc
            ${BRPC_SOURCE_DIR}/src/butil/threading/platform_thread_linux.cc
            ${BRPC_SOURCE_DIR}/src/butil/strings/sys_string_conversions_posix.cc)
endif()


file(GLOB_RECURSE BVAR_SOURCES "${BRPC_SOURCE_DIR}/src/bvar/*.cpp")
file(GLOB_RECURSE BTHREAD_SOURCES "${BRPC_SOURCE_DIR}/src/bthread/*.cpp")
file(GLOB_RECURSE JSON2PB_SOURCES "${BRPC_SOURCE_DIR}/src/json2pb/*.cpp")
file(GLOB_RECURSE BRPC_SOURCES "${BRPC_SOURCE_DIR}/src/brpc/*.cpp")
file(GLOB_RECURSE THRIFT_SOURCES "thrift*.cpp")

foreach(v ${THRIFT_SOURCES})
    list(REMOVE_ITEM BRPC_SOURCES ${v})
endforeach()

set(MCPACK2PB_SOURCES
        ${BRPC_SOURCE_DIR}/src/mcpack2pb/field_type.cpp
        ${BRPC_SOURCE_DIR}/src/mcpack2pb/mcpack2pb.cpp
        ${BRPC_SOURCE_DIR}/src/mcpack2pb/parser.cpp
        ${BRPC_SOURCE_DIR}/src/mcpack2pb/serializer.cpp
)

add_library(brpc ${BUTIL_SOURCES} ${BVAR_SOURCES} ${BTHREAD_SOURCES} ${JSON2PB_SOURCES} ${BRPC_SOURCES} ${PROTO_SOURCES} ${PROTO_HEADERS})
