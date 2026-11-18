cmake_minimum_required(VERSION 3.20)
project(confluent_kafka_cpp VERSION 0.1.0 LANGUAGES CXX)

# The C++ binding is a compiled library over the C core (cpp/CLAUDE.md §3.2,
# §13.1): one object library built into ConfluentKafka::cpp (shared, with a
# soname, linking the shared core) and ConfluentKafka::cpp_static (linking the
# core's static library). The C header is needed to build them, never to use
# them.

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Where the Rust core was built. Same convention as c/CMakeLists.txt.
set(RUST_PROJECT_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rust" CACHE PATH
    "Path to the Rust project root (where `cargo build --features ffi` ran)")
# Overrides for a prebuilt core (a package, a CI cache): the directory holding
# the libraries, and the one holding confluent_kafka_bindings.h.
set(CONFLUENT_KAFKA_LIB_DIR "" CACHE PATH "Directory holding libconfluent_kafka (overrides RUST_PROJECT_ROOT)")
set(CONFLUENT_KAFKA_INCLUDE_DIR "" CACHE PATH "Directory holding confluent_kafka_bindings.h (overrides RUST_PROJECT_ROOT)")

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
# The core's two libraries, each from the first directory that has it. On
# Windows the shared one is linked through its import library.
set(_ck_saved_suffixes ${CMAKE_FIND_LIBRARY_SUFFIXES})
if(WIN32)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .dll.lib)
else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif()
find_library(CONFLUENT_KAFKA_CORE_SHARED NAMES confluent_kafka PATHS ${_ck_lib_paths} NO_DEFAULT_PATH)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_STATIC_LIBRARY_SUFFIX})
find_library(CONFLUENT_KAFKA_CORE_STATIC NAMES confluent_kafka PATHS ${_ck_lib_paths} NO_DEFAULT_PATH)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${_ck_saved_suffixes})
foreach(_ck_kind SHARED STATIC)
    if(NOT CONFLUENT_KAFKA_CORE_${_ck_kind})
        string(TOLOWER "${_ck_kind}" _ck_kind_text)
        message(FATAL_ERROR
            "the ${_ck_kind_text} libconfluent_kafka is not in: ${_ck_lib_paths}. "
            "Run 'cargo build --release --features ffi' in ${RUST_PROJECT_ROOT}, or set CONFLUENT_KAFKA_LIB_DIR.")
    endif()
endforeach()
message(STATUS "Rust core: ${CONFLUENT_KAFKA_CORE_SHARED}, ${CONFLUENT_KAFKA_CORE_STATIC}")
message(STATUS "C FFI header dir: ${_ck_include_dir}")

# The system libraries a program linking the core's static library needs
# (rustc's native-static-libs).
find_package(Threads REQUIRED)
set(CONFLUENT_KAFKA_NATIVE_STATIC_LIBS "Threads::Threads;${CMAKE_DL_LIBS};m" CACHE STRING
    "The system libraries a program linking libconfluent_kafka.a needs")

# The core's targets, named as the package config names them (cmake/), since the
# exported static target names the core's in its link interface.
# IMPORTED_NO_SONAME: a cargo build of the core has no soname (rust/build.rs
# sets one for packaging builds only), and a library without one linked by its
# path leaves that path in the binary; -lconfluent_kafka records the soname
# when there is one.
add_library(ConfluentKafka::core SHARED IMPORTED)
if(WIN32)
    set_target_properties(ConfluentKafka::core PROPERTIES IMPORTED_IMPLIB "${CONFLUENT_KAFKA_CORE_SHARED}")
else()
    set_target_properties(ConfluentKafka::core PROPERTIES
        IMPORTED_LOCATION "${CONFLUENT_KAFKA_CORE_SHARED}" IMPORTED_NO_SONAME TRUE)
endif()
add_library(ConfluentKafka::core_static STATIC IMPORTED)
set_target_properties(ConfluentKafka::core_static PROPERTIES
    IMPORTED_LOCATION "${CONFLUENT_KAFKA_CORE_STATIC}"
    INTERFACE_LINK_LIBRARIES "${CONFLUENT_KAFKA_NATIVE_STATIC_LIBS}")

# version.hpp carries the same three numbers as project(); a drift fails the configure step.
file(STRINGS include/confluent/kafka/version.hpp _ck_version_line REGEX "std::string_view VERSION = \"")
string(REGEX REPLACE ".*\"([0-9.]+)\".*" "\\1" _ck_header_version "${_ck_version_line}")
if(NOT _ck_header_version STREQUAL PROJECT_VERSION)
    message(FATAL_ERROR "include/confluent/kafka/version.hpp says ${_ck_header_version}, CMakeLists.txt says ${PROJECT_VERSION}")
endif()

# cpp/CLAUDE.md §12.4: the soname's version, 0.<minor> before 1.0 and the major after it.
if(PROJECT_VERSION_MAJOR EQUAL 0)
    set(CONFLUENT_KAFKA_CPP_SOVERSION "0.${PROJECT_VERSION_MINOR}")
else()
    set(CONFLUENT_KAFKA_CPP_SOVERSION "${PROJECT_VERSION_MAJOR}")
endif()

# Warnings every translation unit of this project (the library, tests, probe)
# builds with. Users of the targets are not forced into them.
set(CONFLUENT_KAFKA_CPP_WARNINGS
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic;-Werror>
    $<$<CXX_COMPILER_ID:MSVC>:/W4;/WX;/permissive->)

option(CONFLUENT_KAFKA_CPP_SANITIZE "Build the library and the tests with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
if(CONFLUENT_KAFKA_CPP_SANITIZE AND NOT MSVC)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

# The library's sources: one .cpp per public header under src/, and src/detail/.
file(GLOB_RECURSE _ck_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# What every compilation of the library's sources shares: hidden visibility, so
# only what CONFLUENT_KAFKA_CPP_API marks is exported, and the C header, which
# only the sources see (cpp/CLAUDE.md §1.2).
function(_ck_library_sources target)
    target_compile_definitions(${target} PRIVATE CONFLUENT_KAFKA_CPP_BUILDING)
    target_include_directories(${target}
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:include>
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    # SYSTEM: the C header declares opaque types as zero-length arrays, which
    # -Wpedantic rejects in a C++ translation unit.
    target_include_directories(${target} SYSTEM PRIVATE ${_ck_include_dir})
    target_compile_features(${target} PUBLIC cxx_std_20)
    target_compile_options(${target} PRIVATE ${CONFLUENT_KAFKA_CPP_WARNINGS})
    set_target_properties(${target} PROPERTIES
        CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN ON POSITION_INDEPENDENT_CODE ON)
endfunction()

add_library(confluent_kafka_cpp_objects OBJECT ${_ck_sources})
_ck_library_sources(confluent_kafka_cpp_objects)

add_library(confluent_kafka_cpp SHARED $<TARGET_OBJECTS:confluent_kafka_cpp_objects>)
add_library(ConfluentKafka::cpp ALIAS confluent_kafka_cpp)
_ck_library_sources(confluent_kafka_cpp)
target_link_libraries(confluent_kafka_cpp PRIVATE ConfluentKafka::core)
set_target_properties(confluent_kafka_cpp PROPERTIES
    VERSION ${PROJECT_VERSION} SOVERSION ${CONFLUENT_KAFKA_CPP_SOVERSION} EXPORT_NAME cpp)

# A DLL's objects carry __declspec(dllexport), so on Windows the static library
# compiles the sources again, without it; elsewhere it takes the same objects.
if(WIN32)
    add_library(confluent_kafka_cpp_static STATIC ${_ck_sources})
    set_target_properties(confluent_kafka_cpp_static PROPERTIES OUTPUT_NAME confluent_kafka_cpp_static)
else()
    add_library(confluent_kafka_cpp_static STATIC $<TARGET_OBJECTS:confluent_kafka_cpp_objects>)
    set_target_properties(confluent_kafka_cpp_static PROPERTIES OUTPUT_NAME confluent_kafka_cpp)
endif()
add_library(ConfluentKafka::cpp_static ALIAS confluent_kafka_cpp_static)
_ck_library_sources(confluent_kafka_cpp_static)
target_compile_definitions(confluent_kafka_cpp_static PUBLIC CONFLUENT_KAFKA_CPP_STATIC)
target_link_libraries(confluent_kafka_cpp_static PUBLIC ConfluentKafka::core_static)
set_target_properties(confluent_kafka_cpp_static PROPERTIES EXPORT_NAME cpp_static)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    option(CONFLUENT_KAFKA_CPP_BUILD_TESTS "Build the unit tests and the header probe" ON)
else()
    option(CONFLUENT_KAFKA_CPP_BUILD_TESTS "Build the unit tests and the header probe" OFF)
endif()

# Which library the unit tests link: the shared one, as most programs do, or the
# static one (make test-static).
set(CONFLUENT_KAFKA_CPP_TEST_LINK "shared" CACHE STRING "The library the unit tests link: shared or static")
set_property(CACHE CONFLUENT_KAFKA_CPP_TEST_LINK PROPERTY STRINGS shared static)

if(CONFLUENT_KAFKA_CPP_BUILD_TESTS)
    include(CTest)
    enable_testing()
    add_subdirectory(test/probe)
    add_subdirectory(test/unit)
    # A program built against this tree installed, with find_package (test/package);
    # not under the sanitizers, which the program would have to link too.
    if(NOT CONFLUENT_KAFKA_CPP_SANITIZE)
        get_filename_component(_ck_core_dir "${CONFLUENT_KAFKA_CORE_SHARED}" DIRECTORY)
        add_test(NAME package_config
            COMMAND ${CMAKE_COMMAND} -DBUILD_DIR=${CMAKE_BINARY_DIR} -DWORK_DIR=${CMAKE_BINARY_DIR}/package_test
                    -DCORE_LIB_DIR=${_ck_core_dir} "-DGENERATOR=${CMAKE_GENERATOR}" -DCXX=${CMAKE_CXX_COMPILER}
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/test/package/package_test.cmake)
    endif()
endif()

# The translation linter (tools/lint): parses the Java sources and checks the
# headers against cpp/CLAUDE.md. Off by default, as it needs a Java runtime and
# downloads the pinned ANTLR tool and runtime on the first configure.
option(CONFLUENT_KAFKA_CPP_LINT "Build the translation linter" OFF)
if(CONFLUENT_KAFKA_CPP_LINT)
    add_subdirectory(tools/lint)
endif()

# Installation, in two components a package maps to its runtime and -dev parts:
# `runtime` the shared library and its soname link; `development` the headers,
# the static library, the .so link, the package config and the pkg-config file.
# The core is installed by its own packaging (rust/xtask `install-c`).
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} COMPONENT development)
install(TARGETS confluent_kafka_cpp EXPORT ConfluentKafkaCppTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT runtime
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT runtime NAMELINK_COMPONENT development
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT development)
install(TARGETS confluent_kafka_cpp_static EXPORT ConfluentKafkaCppTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT development)
install(EXPORT ConfluentKafkaCppTargets
    NAMESPACE ConfluentKafka::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ConfluentKafkaCpp
    COMPONENT development)
# The version file follows the soname: before 1.0 a minor may break (§12.4).
if(PROJECT_VERSION_MAJOR EQUAL 0)
    set(_ck_compatibility SameMinorVersion)
else()
    set(_ck_compatibility SameMajorVersion)
endif()
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/ConfluentKafkaCppConfigVersion.cmake"
    COMPATIBILITY ${_ck_compatibility})
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ConfluentKafkaCppConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/ConfluentKafkaCppConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ConfluentKafkaCpp)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/ConfluentKafkaCppConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/ConfluentKafkaCppConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ConfluentKafkaCpp
    COMPONENT development)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/confluent-kafka-cpp.pc.in"
    "${CMAKE_CURRENT_BINARY_DIR}/confluent-kafka-cpp.pc" @ONLY)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/confluent-kafka-cpp.pc"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig COMPONENT development)
