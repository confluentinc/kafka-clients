# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

# Standalone CMake project for the C++ gRPC server image. Kept separate
# from the parent bindings/c/CMakeLists.txt so the existing C unit-test
# build doesn't pull in gRPC / protobuf.
#
# Builds inside Dockerfile.grpc — see bindings/c/Dockerfile.grpc for the
# expected layout (libconfluent_kafka.a in /build/lib, header in
# /build/include, proto in /build/proto).

cmake_minimum_required(VERSION 3.16)
project(kafka_grpc_server LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Locations the Dockerfile stages.
set(RUST_LIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lib" CACHE PATH "libconfluent_kafka location")
set(RUST_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include" CACHE PATH "C FFI header dir")
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/proto/producer_service.proto" CACHE PATH "Proto source")

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Generate proto + grpc stubs at configure time.
get_filename_component(PROTO_DIR "${PROTO_FILE}" DIRECTORY)
get_filename_component(PROTO_NAME "${PROTO_FILE}" NAME_WE)
set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GENERATED_DIR}")

set(PROTO_SRCS "${GENERATED_DIR}/${PROTO_NAME}.pb.cc")
set(PROTO_HDRS "${GENERATED_DIR}/${PROTO_NAME}.pb.h")
set(GRPC_SRCS "${GENERATED_DIR}/${PROTO_NAME}.grpc.pb.cc")
set(GRPC_HDRS "${GENERATED_DIR}/${PROTO_NAME}.grpc.pb.h")

# Locate the gRPC C++ plugin (system protoc + the gRPC plugin from
# grpc::grpc_cpp_plugin imported target).
get_target_property(GRPC_CPP_PLUGIN_PATH gRPC::grpc_cpp_plugin LOCATION)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS
        --proto_path=${PROTO_DIR}
        --cpp_out=${GENERATED_DIR}
        --grpc_out=${GENERATED_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_PATH}
        ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating gRPC C++ stubs from ${PROTO_FILE}"
    VERBATIM
)

# Server executable.
add_executable(kafka_grpc_server
    server.cc
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

target_include_directories(kafka_grpc_server PRIVATE
    ${RUST_INCLUDE_DIR}
    ${GENERATED_DIR}
)

# Link the static Rust lib + grpc++ + protobuf + system libs.
target_link_libraries(kafka_grpc_server PRIVATE
    "${RUST_LIB_DIR}/libconfluent_kafka.a"
    gRPC::grpc++
    protobuf::libprotobuf
    pthread dl m
)
