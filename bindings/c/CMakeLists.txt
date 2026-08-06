cmake_minimum_required(VERSION 3.15)
project(confluent-kafka-c VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Path to the Rust project root
set(RUST_PROJECT_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH "Path to Rust project root")

# Rust FFI header directory
set(RUST_INCLUDE_DIR "${RUST_PROJECT_ROOT}/target/include")

# Link type: "static" or "shared"
set(LINK_TYPE "static" CACHE STRING "Link type: static or shared")

if(LINK_TYPE STREQUAL "shared")
    find_library(CONFLUENT_KAFKA_RUST
        NAMES confluent_kafka
        PATHS "${RUST_PROJECT_ROOT}/target/release" "${RUST_PROJECT_ROOT}/target/debug"
        NO_DEFAULT_PATH
    )
else()
    # For static linking, look for the .a file explicitly
    find_library(CONFLUENT_KAFKA_RUST
        NAMES libconfluent_kafka.a confluent_kafka
        PATHS "${RUST_PROJECT_ROOT}/target/release" "${RUST_PROJECT_ROOT}/target/debug"
        NO_DEFAULT_PATH
    )
endif()

if(NOT CONFLUENT_KAFKA_RUST)
    message(FATAL_ERROR
        "Could not find libconfluent_kafka. "
        "Run 'cargo build --features ffi' from the Rust project root first.")
endif()

message(STATUS "Using Rust library: ${CONFLUENT_KAFKA_RUST}")
message(STATUS "Using Rust headers: ${RUST_INCLUDE_DIR}")

# Unity test framework
add_subdirectory(tests/unity)

# System libraries for static linking
if(LINK_TYPE STREQUAL "shared")
    set(SYSTEM_LIBS "")
else()
    set(SYSTEM_LIBS pthread dl m)
endif()

# Enable CTest
enable_testing()

# Test executables
add_executable(test_mock_producer tests/test_mock_producer.c)
target_include_directories(test_mock_producer PRIVATE ${RUST_INCLUDE_DIR})
target_link_libraries(test_mock_producer unity::framework ${CONFLUENT_KAFKA_RUST} ${SYSTEM_LIBS})

add_executable(test_kafka_producer tests/test_kafka_producer.c)
target_include_directories(test_kafka_producer PRIVATE ${RUST_INCLUDE_DIR})
target_link_libraries(test_kafka_producer unity::framework ${CONFLUENT_KAFKA_RUST} ${SYSTEM_LIBS})

add_executable(test_mock_consumer tests/test_mock_consumer.c)
target_include_directories(test_mock_consumer PRIVATE ${RUST_INCLUDE_DIR})
target_link_libraries(test_mock_consumer unity::framework ${CONFLUENT_KAFKA_RUST} ${SYSTEM_LIBS})

add_executable(test_kafka_consumer tests/test_kafka_consumer.c)
target_include_directories(test_kafka_consumer PRIVATE ${RUST_INCLUDE_DIR})
# This test spawns a wakeup helper thread, so it needs pthread explicitly in
# addition to ${SYSTEM_LIBS} (which is empty under shared linking).
target_link_libraries(test_kafka_consumer unity::framework ${CONFLUENT_KAFKA_RUST} ${SYSTEM_LIBS} pthread)

add_executable(test_consumer_callbacks tests/test_consumer_callbacks.c)
target_include_directories(test_consumer_callbacks PRIVATE ${RUST_INCLUDE_DIR})
# This test compares callback / caller thread ids, so it needs pthread
# explicitly in addition to ${SYSTEM_LIBS} (which is empty under shared linking).
target_link_libraries(test_consumer_callbacks unity::framework ${CONFLUENT_KAFKA_RUST} ${SYSTEM_LIBS} pthread)

add_executable(test_mock_admin tests/test_mock_admin.c)
target_include_directories(test_mock_admin PRIVATE ${RUST_INCLUDE_DIR})
target_link_libraries(test_mock_admin unity::framework ${CONFLUENT_KAFKA_RUST} ${SYSTEM_LIBS})

# Register tests with CTest
add_test(NAME mock_producer COMMAND test_mock_producer)
add_test(NAME kafka_producer COMMAND test_kafka_producer)
add_test(NAME mock_consumer COMMAND test_mock_consumer)
add_test(NAME kafka_consumer COMMAND test_kafka_consumer)
add_test(NAME consumer_callbacks COMMAND test_consumer_callbacks)
add_test(NAME mock_admin COMMAND test_mock_admin)

# ---------------------------------------------------------------------------
# Producer performance test (opt-in; intentionally NOT registered with CTest).
#
# Mirrors tests/integration/producer_perf_test.rs. Drives two backends selected
# by the CLIENT_VERSION env var: the Rust client C bindings (v3, default) and
# librdkafka (v2, reference baseline). It needs a reachable broker and runs
# long, so it builds but is launched manually, not under `ctest`/`make test-c`.
#
# librdkafka is an optional dependency: if it is not found the target is
# skipped so the default `build-c` still succeeds on hosts without it.
# ---------------------------------------------------------------------------
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(RDKAFKA QUIET rdkafka)
endif()

if(RDKAFKA_FOUND)
    add_executable(producer_perf_test tests/producer_perf_test.c)
    target_include_directories(producer_perf_test
        PRIVATE ${RUST_INCLUDE_DIR} ${RDKAFKA_INCLUDE_DIRS})
    target_link_directories(producer_perf_test PRIVATE ${RDKAFKA_LIBRARY_DIRS})
    # pthread/m are needed by this test's own code (threads, isinf) regardless
    # of LINK_TYPE, so link them explicitly in addition to ${SYSTEM_LIBS}.
    target_link_libraries(producer_perf_test
        ${CONFLUENT_KAFKA_RUST} ${RDKAFKA_LIBRARIES} ${SYSTEM_LIBS} pthread m)
    message(STATUS "producer_perf_test enabled (librdkafka ${RDKAFKA_VERSION})")
else()
    message(STATUS "producer_perf_test skipped: librdkafka not found")
endif()
