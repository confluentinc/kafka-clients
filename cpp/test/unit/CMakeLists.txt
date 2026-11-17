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
confluent_kafka_cpp_unit_test(topic_partition_test)
confluent_kafka_cpp_unit_test(node_test)
confluent_kafka_cpp_unit_test(cluster_resource_test)
confluent_kafka_cpp_unit_test(pattern_type_test)
confluent_kafka_cpp_unit_test(subscription_pattern_test)
confluent_kafka_cpp_unit_test(acl_permission_type_test)
confluent_kafka_cpp_unit_test(config_resource_test)
