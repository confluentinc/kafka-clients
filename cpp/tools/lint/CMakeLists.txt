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

add_executable(ck_java_model_cli cli/ck_java_model.cpp)
set_target_properties(ck_java_model_cli PROPERTIES OUTPUT_NAME ck_java_model)
target_link_libraries(ck_java_model_cli PRIVATE ck_java_model)
target_compile_options(ck_java_model_cli PRIVATE ${CK_LINT_WARNINGS})

set(CK_LINT_DESIGN_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../design/current" CACHE PATH
    "The repository's design/current: the public-audience and deprecated-API lists")

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
        target_link_libraries(${name} PRIVATE ck_java_model GTest::gtest_main)
        target_compile_options(${name} PRIVATE ${CK_LINT_WARNINGS})
        target_compile_definitions(${name} PRIVATE
            CK_LINT_KAFKA_JAVA_ROOT="${CK_LINT_KAFKA_SOURCE_DIR}/clients/src/main/java"
            CK_LINT_DESIGN_DIR="${CK_LINT_DESIGN_DIR}")
    endfunction()

    # The model on small sources.
    ck_lint_test(javamodel_test)
    gtest_discover_tests(javamodel_test TEST_PREFIX "javamodel_test." DISCOVERY_MODE PRE_TEST)
    # The model of kafka/clients: every source parses and resolves, and it
    # agrees with design/current's public-audience and deprecated-API lists.
    # One process, so the tree is parsed once.
    ck_lint_test(javamodel_kafka_test)
    add_test(NAME javamodel_kafka_test COMMAND javamodel_kafka_test)
endif()
