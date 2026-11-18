# The header probe (cpp/CLAUDE.md §3.1): one translation unit including every
# public header, compiled under the project warnings against the headers alone,
# as a program sees them.
file(GLOB_RECURSE _ck_public_headers RELATIVE "${CMAKE_SOURCE_DIR}/include"
     CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/include/confluent/kafka/*.hpp")
list(SORT _ck_public_headers)
set(_ck_probe_includes "")
foreach(_header IN LISTS _ck_public_headers)
    string(APPEND _ck_probe_includes "#include <${_header}>\n")
endforeach()
configure_file(probe_includes.hpp.in "${CMAKE_CURRENT_BINARY_DIR}/probe_includes.hpp" @ONLY)

add_library(confluent_kafka_cpp_probe OBJECT probe.cpp)
target_include_directories(confluent_kafka_cpp_probe PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(confluent_kafka_cpp_probe PRIVATE ConfluentKafka::cpp)
target_compile_options(confluent_kafka_cpp_probe PRIVATE ${CONFLUENT_KAFKA_CPP_WARNINGS})
