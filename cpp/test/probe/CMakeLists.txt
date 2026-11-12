# The ABI probe (cpp/CLAUDE.md §12): a shared library whose one translation
# unit includes every public header, so that (1) each header compiles on its
# own under the project warnings and (2) `abidiff` can compare two releases'
# exported symbols and type layouts.
file(GLOB_RECURSE _ck_public_headers RELATIVE "${CMAKE_SOURCE_DIR}/include"
     CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/include/confluent/kafka/*.hpp")
list(SORT _ck_public_headers)
set(_ck_probe_includes "")
foreach(_header IN LISTS _ck_public_headers)
    string(APPEND _ck_probe_includes "#include <${_header}>\n")
endforeach()
configure_file(probe_includes.hpp.in "${CMAKE_CURRENT_BINARY_DIR}/probe_includes.hpp" @ONLY)

add_library(confluent_kafka_cpp_probe SHARED probe.cpp)
target_include_directories(confluent_kafka_cpp_probe PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(confluent_kafka_cpp_probe PRIVATE ConfluentKafka::cpp)
target_compile_options(confluent_kafka_cpp_probe PRIVATE ${CONFLUENT_KAFKA_CPP_WARNINGS})
set_target_properties(confluent_kafka_cpp_probe PROPERTIES CXX_VISIBILITY_PRESET default)
