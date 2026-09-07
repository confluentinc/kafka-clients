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
# All three protos are generated; consumer_service.proto imports
# producer_service.proto and admin_service.proto imports both, so they must
# share a proto_path.
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto" CACHE PATH "Proto source dir")
set(PROTO_NAMES producer_service consumer_service admin_service)

# Use module-mode find_package since Debian's libprotobuf-dev /
# libgrpc++-dev packages don't ship CMake config files.
find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPCPP REQUIRED grpc++)

# Generate proto + grpc stubs at configure time.
set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GENERATED_DIR}")

# The grpc_cpp_plugin binary is shipped alongside protoc by
# protobuf-compiler-grpc on Debian.
find_program(GRPC_CPP_PLUGIN_PATH grpc_cpp_plugin REQUIRED)

set(GENERATED_SRCS "")
foreach(PROTO_NAME ${PROTO_NAMES})
    set(_proto_file "${PROTO_DIR}/${PROTO_NAME}.proto")
    set(_pb_src "${GENERATED_DIR}/${PROTO_NAME}.pb.cc")
    set(_grpc_src "${GENERATED_DIR}/${PROTO_NAME}.grpc.pb.cc")
    add_custom_command(
        OUTPUT ${_pb_src} "${GENERATED_DIR}/${PROTO_NAME}.pb.h"
               ${_grpc_src} "${GENERATED_DIR}/${PROTO_NAME}.grpc.pb.h"
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS
            --proto_path=${PROTO_DIR}
            --cpp_out=${GENERATED_DIR}
            --grpc_out=${GENERATED_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_PATH}
            ${_proto_file}
        DEPENDS ${_proto_file}
        COMMENT "Generating gRPC C++ stubs from ${_proto_file}"
        VERBATIM
    )
    list(APPEND GENERATED_SRCS ${_pb_src} ${_grpc_src})
endforeach()

# Server executable.
add_executable(kafka_grpc_server
    server.cc
    ${GENERATED_SRCS}
)

target_include_directories(kafka_grpc_server PRIVATE
    ${RUST_INCLUDE_DIR}
    ${GENERATED_DIR}
)

# Link the static Rust lib + grpc++ + protobuf + system libs.
target_include_directories(kafka_grpc_server PRIVATE ${GRPCPP_INCLUDE_DIRS})
target_link_libraries(kafka_grpc_server PRIVATE
    "${RUST_LIB_DIR}/libconfluent_kafka.a"
    ${GRPCPP_LIBRARIES}
    ${Protobuf_LIBRARIES}
    pthread dl m
)
target_compile_options(kafka_grpc_server PRIVATE ${GRPCPP_CFLAGS_OTHER})
target_link_directories(kafka_grpc_server PRIVATE ${GRPCPP_LIBRARY_DIRS})
