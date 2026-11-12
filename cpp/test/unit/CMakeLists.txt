# GoogleTest from the system (`find_package(GTest CONFIG)`), else fetched at
# a pinned release (cpp/CLAUDE.md §11).
find_package(GTest CONFIG QUIET)
if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.tar.gz
        URL_HASH SHA256=65fab701d9829d38cb77c14acdc431d2108bfdbf8979e40eb8ae567edf10b27c
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
endif()
include(GoogleTest)

# One executable per test file, named after it, as c/CMakeLists.txt does.
function(confluent_kafka_cpp_unit_test name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE ConfluentKafka::cpp GTest::gtest_main)
    target_compile_options(${name} PRIVATE ${CONFLUENT_KAFKA_CPP_WARNINGS})
    gtest_discover_tests(${name} TEST_PREFIX "${name}." DISCOVERY_MODE PRE_TEST)
endfunction()

confluent_kafka_cpp_unit_test(ffi_handle_test)

# The sync-only, header-only contract (cpp/CLAUDE.md §11): every `kafka_`
# name the headers use is declared in the C header, and none is a `_cb`
# function.
find_package(Python3 COMPONENTS Interpreter REQUIRED)
add_test(NAME ffi_symbols
    COMMAND ${Python3_EXECUTABLE} -I ${CMAKE_CURRENT_SOURCE_DIR}/../../tools/check_ffi_symbols.py
            --header ${_ck_include_dir}/confluent_kafka_bindings.h
            ${CMAKE_CURRENT_SOURCE_DIR}/../../include)
