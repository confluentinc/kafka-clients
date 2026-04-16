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

# Test executable
add_executable(test_mock_producer tests/test_mock_producer.c)
target_include_directories(test_mock_producer PRIVATE ${RUST_INCLUDE_DIR})

if(LINK_TYPE STREQUAL "shared")
    target_link_libraries(test_mock_producer ${CONFLUENT_KAFKA_RUST})
else()
    target_link_libraries(test_mock_producer ${CONFLUENT_KAFKA_RUST} pthread dl m)
endif()
