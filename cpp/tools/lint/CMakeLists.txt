# The translation linter of the C++ binding: it parses the Java sources of the
# kafka/ submodule and checks the binding's public headers against the rules of
# cpp/CLAUDE.md.
#
# Built in two ways:
#   - with the binding: cmake -S cpp -B cpp/build -DCONFLUENT_KAFKA_CPP_LINT=ON
#   - on its own, without the Rust core: cmake -S cpp/tools/lint -B cpp/build-lint
# It needs a Java runtime (the ANTLR tool) and network access on the first
# configure (the pinned ANTLR jar and runtime sources).

cmake_minimum_required(VERSION 3.24)
project(confluent_kafka_cpp_lint LANGUAGES CXX)

if(PROJECT_IS_TOP_LEVEL)
    set(CMAKE_CXX_STANDARD 20)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
    if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
        set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    endif()
    include(CTest)
    enable_testing()
endif()

set(CK_LINT_KAFKA_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../kafka" CACHE PATH
    "The kafka/ submodule: the Java sources the binding translates")
if(NOT EXISTS "${CK_LINT_KAFKA_SOURCE_DIR}/clients/src/main/java/org/apache/kafka")
    message(FATAL_ERROR
        "No Java sources in ${CK_LINT_KAFKA_SOURCE_DIR}/clients/src/main/java. "
        "Run 'git submodule update --init kafka', or set CK_LINT_KAFKA_SOURCE_DIR.")
endif()

# The linter's own code builds with the binding's warnings.
set(CK_LINT_WARNINGS
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic;-Werror>
    $<$<CXX_COMPILER_ID:MSVC>:/W4;/WX;/permissive->)

include(cmake/Antlr4.cmake)

# The Java model. Its headers carry no ANTLR type (javamodel/java_model.hpp).
add_library(ck_java_model STATIC javamodel/java_syntax.cpp javamodel/java_model.cpp)
target_include_directories(ck_java_model PUBLIC javamodel)
target_link_libraries(ck_java_model PRIVATE ck_java_grammar)
target_compile_options(ck_java_model PRIVATE ${CK_LINT_WARNINGS})
set_target_properties(ck_java_model PROPERTIES POSITION_INDEPENDENT_CODE ON)

# cpp/CLAUDE.md as code: names, the §5 type table, the scope, the expected
# shape of a Java class, the data files. Plain C++, no ANTLR, no LLVM.
add_library(ck_rules STATIC
    rules/cpp_type.cpp rules/names.cpp rules/types.cpp rules/data_files.cpp rules/scope.cpp rules/expected.cpp)
target_include_directories(ck_rules PUBLIC rules)
target_link_libraries(ck_rules PUBLIC ck_java_model)
target_compile_options(ck_rules PRIVATE ${CK_LINT_WARNINGS})
set_target_properties(ck_rules PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_executable(ck_java_model_cli cli/ck_java_model.cpp)
set_target_properties(ck_java_model_cli PROPERTIES OUTPUT_NAME ck_java_model)
target_link_libraries(ck_java_model_cli PRIVATE ck_rules)
target_compile_definitions(ck_java_model_cli PRIVATE
    CK_LINT_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data")
target_compile_options(ck_java_model_cli PRIVATE ${CK_LINT_WARNINGS})

set(CK_LINT_DESIGN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../design/current" CACHE PATH
    "The repository's design/current: the public-audience and deprecated-API lists")

# ---- The clang-tidy plugin.
# A plugin loads only into the clang-tidy of the LLVM it was built against, so
# one major is pinned (LLVM_VERSION). LLVM's package version file accepts only
# its own major.minor, so the major is checked after loading it.
file(STRINGS LLVM_VERSION CK_LLVM_MAJOR LIMIT_COUNT 1)
set(CK_LLVM_ROOT "/usr/lib/llvm-${CK_LLVM_MAJOR}" CACHE PATH "The LLVM ${CK_LLVM_MAJOR} installation")
find_package(LLVM REQUIRED CONFIG PATHS "${CK_LLVM_ROOT}/lib/cmake/llvm" NO_DEFAULT_PATH)
if(NOT LLVM_VERSION_MAJOR EQUAL CK_LLVM_MAJOR)
    message(FATAL_ERROR "The linter needs LLVM ${CK_LLVM_MAJOR} (LLVM_VERSION); found ${LLVM_PACKAGE_VERSION}")
endif()
find_package(Clang REQUIRED CONFIG PATHS "${CK_LLVM_ROOT}/lib/cmake/clang" NO_DEFAULT_PATH)
find_program(CK_CLANG_TIDY clang-tidy HINTS "${LLVM_TOOLS_BINARY_DIR}" NO_DEFAULT_PATH REQUIRED)
message(STATUS "Translation linter: LLVM ${LLVM_PACKAGE_VERSION}, ${CK_CLANG_TIDY}")

set(CK_LINT_INCLUDE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../include" CACHE PATH "The binding's include directory")
# The C header, for running the checks on the binding's own headers.
if(DEFINED _ck_include_dir)
    set(_ck_lint_ffi_default "${_ck_include_dir}")
else()
    set(_ck_lint_ffi_default "${CMAKE_CURRENT_SOURCE_DIR}/../../../rust/target/include")
endif()
set(CK_LINT_FFI_INCLUDE_DIR "${_ck_lint_ffi_default}" CACHE PATH "The directory of confluent_kafka_bindings.h")

add_library(ConfluentKafkaTidy MODULE
    tidy/module.cpp tidy/session.cpp tidy/class_checks.cpp tidy/header_check.cpp)
target_include_directories(ConfluentKafkaTidy SYSTEM PRIVATE ${LLVM_INCLUDE_DIRS} ${CLANG_INCLUDE_DIRS})
separate_arguments(_ck_llvm_definitions NATIVE_COMMAND "${LLVM_DEFINITIONS}")
target_compile_definitions(ConfluentKafkaTidy PRIVATE
    CK_LINT_KAFKA_JAVA_ROOT="${CK_LINT_KAFKA_SOURCE_DIR}/clients/src/main/java"
    CK_LINT_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data"
    CK_LINT_INCLUDE_ROOT="${CK_LINT_INCLUDE_ROOT}")
target_compile_options(ConfluentKafkaTidy PRIVATE ${_ck_llvm_definitions} ${CK_LINT_WARNINGS})
if(NOT LLVM_ENABLE_RTTI)
    target_compile_options(ConfluentKafkaTidy PRIVATE -fno-rtti)
endif()
# Clang's symbols come from the clang-tidy that loads the plugin.
target_link_libraries(ConfluentKafkaTidy PRIVATE ck_rules)

if(BUILD_TESTING)
    # GoogleTest from the system, else the pin of cpp/test/unit/CMakeLists.txt.
    find_package(GTest CONFIG QUIET)
    if(NOT GTest_FOUND AND NOT TARGET GTest::gtest_main)
        include(FetchContent)
        FetchContent_Declare(googletest
            URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.tar.gz
            URL_HASH SHA256=65fab701d9829d38cb77c14acdc431d2108bfdbf8979e40eb8ae567edf10b27c
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
        set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(googletest)
    endif()
    include(GoogleTest)

    function(ck_lint_test name)
        add_executable(${name} test/${name}.cpp)
        target_link_libraries(${name} PRIVATE ck_rules GTest::gtest_main)
        target_compile_options(${name} PRIVATE ${CK_LINT_WARNINGS})
        target_compile_definitions(${name} PRIVATE
            CK_LINT_KAFKA_JAVA_ROOT="${CK_LINT_KAFKA_SOURCE_DIR}/clients/src/main/java"
            CK_LINT_DESIGN_DIR="${CK_LINT_DESIGN_DIR}"
            CK_LINT_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data")
    endfunction()

    # The model on small sources.
    ck_lint_test(javamodel_test)
    gtest_discover_tests(javamodel_test TEST_PREFIX "javamodel_test." DISCOVERY_MODE PRE_TEST)
    # The rules on cpp/CLAUDE.md's examples.
    ck_lint_test(rules_test)
    gtest_discover_tests(rules_test TEST_PREFIX "rules_test." DISCOVERY_MODE PRE_TEST)
    # The model of kafka/clients: every source parses and resolves, and it
    # agrees with design/current's public-audience and deprecated-API lists.
    # One process, so the tree is parsed once.
    ck_lint_test(javamodel_kafka_test)
    add_test(NAME javamodel_kafka_test COMMAND javamodel_kafka_test)

    # The plugin through the real clang-tidy: it loads, each fixture reports
    # exactly what its headers expect, and the binding's headers report nothing.
    add_executable(fixture_runner test/fixture_runner.cpp)
    target_compile_options(fixture_runner PRIVATE ${CK_LINT_WARNINGS})
    add_test(NAME lint_plugin_loads
        COMMAND fixture_runner checks ${CK_CLANG_TIDY} $<TARGET_FILE:ConfluentKafkaTidy>
                confluent-kafka-java-class confluent-kafka-java-inheritance confluent-kafka-java-enum
                confluent-kafka-header)
    file(GLOB _ck_fixtures LIST_DIRECTORIES true "${CMAKE_CURRENT_SOURCE_DIR}/test/fixtures/*")
    foreach(_fixture IN LISTS _ck_fixtures)
        if(IS_DIRECTORY "${_fixture}")
            get_filename_component(_name "${_fixture}" NAME)
            add_test(NAME lint_fixture_${_name}
                COMMAND fixture_runner run ${CK_CLANG_TIDY} $<TARGET_FILE:ConfluentKafkaTidy>
                        "${_fixture}/java" "${_fixture}/include" "${_fixture}/data"
                        "${CMAKE_CURRENT_BINARY_DIR}/fixtures/${_name}")
        endif()
    endforeach()
    if(EXISTS "${CK_LINT_FFI_INCLUDE_DIR}/confluent_kafka_bindings.h")
        # Each mutation edits one line of a real header in a copy of the include
        # tree; the checks must report it.
        file(GLOB _ck_mutations "${CMAKE_CURRENT_SOURCE_DIR}/test/mutations/*.mutation")
        foreach(_mutation IN LISTS _ck_mutations)
            get_filename_component(_name "${_mutation}" NAME_WE)
            add_test(NAME lint_mutation_${_name}
                COMMAND fixture_runner mutate ${CK_CLANG_TIDY} $<TARGET_FILE:ConfluentKafkaTidy>
                        "${CK_LINT_KAFKA_SOURCE_DIR}/clients/src/main/java" "${CK_LINT_INCLUDE_ROOT}"
                        "${CMAKE_CURRENT_SOURCE_DIR}/data" "${CMAKE_CURRENT_BINARY_DIR}/mutations/${_name}"
                        "${_mutation}" -isystem "${CK_LINT_FFI_INCLUDE_DIR}")
        endforeach()
        add_test(NAME lint_binding_headers
            COMMAND fixture_runner run ${CK_CLANG_TIDY} $<TARGET_FILE:ConfluentKafkaTidy>
                    "${CK_LINT_KAFKA_SOURCE_DIR}/clients/src/main/java" "${CK_LINT_INCLUDE_ROOT}"
                    "${CMAKE_CURRENT_SOURCE_DIR}/data" "${CMAKE_CURRENT_BINARY_DIR}/binding"
                    -isystem "${CK_LINT_FFI_INCLUDE_DIR}")
    endif()
endif()
