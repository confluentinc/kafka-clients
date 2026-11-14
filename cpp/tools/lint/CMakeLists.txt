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

# The Java model. Its headers carry no ANTLR type (javamodel/java_syntax.hpp).
add_library(ck_java_model STATIC javamodel/java_syntax.cpp)
target_include_directories(ck_java_model PUBLIC javamodel)
target_link_libraries(ck_java_model PRIVATE ck_java_grammar)
target_compile_options(ck_java_model PRIVATE ${CK_LINT_WARNINGS})
set_target_properties(ck_java_model PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_executable(ck_java_model_cli cli/ck_java_model.cpp)
set_target_properties(ck_java_model_cli PROPERTIES OUTPUT_NAME ck_java_model)
target_link_libraries(ck_java_model_cli PRIVATE ck_java_model)
target_compile_options(ck_java_model_cli PRIVATE ${CK_LINT_WARNINGS})

if(BUILD_TESTING)
    # Every source of the Java client parses with no syntax error.
    add_test(NAME java_parse_kafka_clients
        COMMAND ck_java_model_cli parse "${CK_LINT_KAFKA_SOURCE_DIR}/clients/src/main/java")
endif()
