cmake_minimum_required(VERSION 3.20)
project(confluent_kafka_cpp VERSION 0.1.0 LANGUAGES CXX)

# The C++ binding is header-only (cpp/CLAUDE.md §9). This project exports one
# INTERFACE target, ConfluentKafka::cpp, carrying the include directory, the
# C FFI header directory and the Rust core library to link.

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Where the Rust core was built. Same convention as c/CMakeLists.txt.
set(RUST_PROJECT_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rust" CACHE PATH
    "Path to the Rust project root (where `cargo build --features ffi` ran)")
# Overrides for a prebuilt core (a package, a CI cache): the directory holding
# the library, and the one holding confluent_kafka_bindings.h.
set(CONFLUENT_KAFKA_LIB_DIR "" CACHE PATH "Directory holding libconfluent_kafka (overrides RUST_PROJECT_ROOT)")
set(CONFLUENT_KAFKA_INCLUDE_DIR "" CACHE PATH "Directory holding confluent_kafka_bindings.h (overrides RUST_PROJECT_ROOT)")
set(LINK_TYPE "shared" CACHE STRING "Link the Rust core as 'static' or 'shared'")
set_property(CACHE LINK_TYPE PROPERTY STRINGS static shared)

if(CONFLUENT_KAFKA_INCLUDE_DIR)
    set(_ck_include_dir "${CONFLUENT_KAFKA_INCLUDE_DIR}")
else()
    set(_ck_include_dir "${RUST_PROJECT_ROOT}/target/include")
endif()
if(NOT EXISTS "${_ck_include_dir}/confluent_kafka_bindings.h")
    message(FATAL_ERROR
        "confluent_kafka_bindings.h not found in ${_ck_include_dir}. "
        "Run 'cargo build --features ffi' in ${RUST_PROJECT_ROOT}, or set CONFLUENT_KAFKA_INCLUDE_DIR.")
endif()

if(CONFLUENT_KAFKA_LIB_DIR)
    set(_ck_lib_paths "${CONFLUENT_KAFKA_LIB_DIR}")
else()
    set(_ck_lib_paths "${RUST_PROJECT_ROOT}/target/release" "${RUST_PROJECT_ROOT}/target/debug")
endif()
if(LINK_TYPE STREQUAL "static")
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES .so .dylib .dll.a)
endif()
find_library(CONFLUENT_KAFKA_RUST NAMES confluent_kafka PATHS ${_ck_lib_paths} NO_DEFAULT_PATH)
if(NOT CONFLUENT_KAFKA_RUST)
    message(FATAL_ERROR
        "libconfluent_kafka (${LINK_TYPE}) not found in: ${_ck_lib_paths}. "
        "Run 'cargo build --release --features ffi' in ${RUST_PROJECT_ROOT}, or set CONFLUENT_KAFKA_LIB_DIR.")
endif()
message(STATUS "Rust core: ${CONFLUENT_KAFKA_RUST}")
message(STATUS "C FFI header dir: ${_ck_include_dir}")

# version.hpp carries the same three numbers as project(); a drift fails the configure step.
file(STRINGS include/confluent/kafka/version.hpp _ck_version_line REGEX "std::string_view VERSION = \"")
string(REGEX REPLACE ".*\"([0-9.]+)\".*" "\\1" _ck_header_version "${_ck_version_line}")
if(NOT _ck_header_version STREQUAL PROJECT_VERSION)
    message(FATAL_ERROR "include/confluent/kafka/version.hpp says ${_ck_header_version}, CMakeLists.txt says ${PROJECT_VERSION}")
endif()

add_library(confluent_kafka_cpp INTERFACE)
add_library(ConfluentKafka::cpp ALIAS confluent_kafka_cpp)
target_include_directories(confluent_kafka_cpp INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
# SYSTEM: the C header declares opaque types as zero-length arrays, which
# -Wpedantic rejects in a C++ translation unit.
target_include_directories(confluent_kafka_cpp SYSTEM INTERFACE $<BUILD_INTERFACE:${_ck_include_dir}>)
target_compile_features(confluent_kafka_cpp INTERFACE cxx_std_20)
target_link_libraries(confluent_kafka_cpp INTERFACE ${CONFLUENT_KAFKA_RUST})
if(LINK_TYPE STREQUAL "static")
    find_package(Threads REQUIRED)
    target_link_libraries(confluent_kafka_cpp INTERFACE Threads::Threads ${CMAKE_DL_LIBS} m)
endif()

# Warnings every translation unit of this project (tests, probe) builds with.
# Users of the INTERFACE target are not forced into them.
set(CONFLUENT_KAFKA_CPP_WARNINGS
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic;-Werror>
    $<$<CXX_COMPILER_ID:MSVC>:/W4;/WX;/permissive->)

option(CONFLUENT_KAFKA_CPP_SANITIZE "Build the tests with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
if(CONFLUENT_KAFKA_CPP_SANITIZE AND NOT MSVC)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    option(CONFLUENT_KAFKA_CPP_BUILD_TESTS "Build the unit tests and the ABI probe" ON)
else()
    option(CONFLUENT_KAFKA_CPP_BUILD_TESTS "Build the unit tests and the ABI probe" OFF)
endif()

if(CONFLUENT_KAFKA_CPP_BUILD_TESTS)
    include(CTest)
    enable_testing()
    add_subdirectory(test/probe)
    add_subdirectory(test/unit)
endif()

# The translation linter (tools/lint): parses the Java sources and checks the
# headers against cpp/CLAUDE.md. Off by default, as it needs a Java runtime and
# downloads the pinned ANTLR tool and runtime on the first configure.
option(CONFLUENT_KAFKA_CPP_LINT "Build the translation linter" OFF)
if(CONFLUENT_KAFKA_CPP_LINT)
    add_subdirectory(tools/lint)
endif()

# Installation: headers plus a package config so `find_package(ConfluentKafkaCpp)`
# yields ConfluentKafka::cpp. The Rust core itself is installed by the core's
# own packaging; the config file locates it through the same cache variables.
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(TARGETS confluent_kafka_cpp EXPORT ConfluentKafkaCppTargets)
install(EXPORT ConfluentKafkaCppTargets
    NAMESPACE ConfluentKafka::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ConfluentKafkaCpp)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/ConfluentKafkaCppConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ConfluentKafkaCppConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/ConfluentKafkaCppConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ConfluentKafkaCpp)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/ConfluentKafkaCppConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/ConfluentKafkaCppConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ConfluentKafkaCpp)
