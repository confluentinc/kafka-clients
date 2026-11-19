# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The interface check of cpp/CLAUDE.md §12.4: the binding against a previous
# release taken from its git tag, three ways.
#   1. The API inventories (confluent-kafka-inventory) compared by
#      `ck_java_model api-diff`: a declaration removed or changed is breaking.
#   2. `abidiff` on the shared library of each tree, built with debug info, its
#      public headers given so only what they reach is compared; a break under
#      an unchanged SOVERSION is reported too. A header-only baseline (before
#      the compiled library) shipped no library a program loads: skipped.
#   3. The previous release's unit tests compiled against the new headers.
#
#   cmake -DREPO_ROOT=<repo> -DSOURCE_DIR=<cpp dir of the new tree>
#         -DWORK_DIR=<scratch dir> -DCLANG_TIDY=<clang-tidy-21> -DPLUGIN=<.so>
#         -DCK_JAVA_MODEL=<ck_java_model> -DFFI_INCLUDE=<dir of the C header>
#         -DCORE_LIB_DIR=<dir of libconfluent_kafka> [-DBASELINE=<rev>]
#         [-DGTEST_INCLUDE=<dir>] [-DCXX=g++] [-DSTRICT=ON] -P abi_check.cmake
#
# BASELINE defaults to the highest cpp/vX.Y.Z tag not above the new tree's
# version; with neither, there is nothing to compare. A break fails the run
# with STRICT=ON and is reported otherwise (development and preview, §12.4).

cmake_minimum_required(VERSION 3.20)

foreach(_var REPO_ROOT SOURCE_DIR WORK_DIR CLANG_TIDY PLUGIN CK_JAVA_MODEL FFI_INCLUDE CORE_LIB_DIR)
    if(NOT DEFINED ${_var} OR "${${_var}}" STREQUAL "")
        message(FATAL_ERROR "abi-check: ${_var} is not set")
    endif()
endforeach()
if(NOT DEFINED CXX OR CXX STREQUAL "")
    set(CXX g++)
endif()

# The new tree's version, from version.hpp.
file(STRINGS "${SOURCE_DIR}/include/confluent/kafka/version.hpp" _version_line
     REGEX "inline constexpr std::string_view VERSION = ")
string(REGEX MATCH "\"([0-9]+\\.[0-9]+\\.[0-9]+)\"" _ "${_version_line}")
set(_new_version "${CMAKE_MATCH_1}")

if(NOT DEFINED BASELINE OR BASELINE STREQUAL "")
    execute_process(COMMAND git -C "${REPO_ROOT}" tag -l "cpp/v*" --sort=-v:refname
                    OUTPUT_VARIABLE _tags OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REPLACE "\n" ";" _tags "${_tags}")
    foreach(_tag IN LISTS _tags)
        string(REGEX REPLACE "^cpp/v" "" _tag_version "${_tag}")
        if(_tag_version VERSION_LESS_EQUAL _new_version)
            set(BASELINE "${_tag}")
            break()
        endif()
    endforeach()
endif()
if(NOT DEFINED BASELINE OR BASELINE STREQUAL "")
    message(STATUS "abi-check: no cpp/v* tag at or below ${_new_version} and no BASELINE: nothing to compare")
    return()
endif()

execute_process(COMMAND git -C "${REPO_ROOT}" rev-parse --verify "${BASELINE}^{commit}"
                OUTPUT_VARIABLE _sha OUTPUT_STRIP_TRAILING_WHITESPACE RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "abi-check: ${BASELINE} is not a commit")
endif()

# The baseline's cpp/ tree, extracted once per commit.
set(_old_root "${WORK_DIR}/trees/${_sha}")
set(_old "${_old_root}/cpp")
if(NOT EXISTS "${_old}/CMakeLists.txt")
    file(REMOVE_RECURSE "${_old_root}")
    file(MAKE_DIRECTORY "${_old_root}")
    execute_process(COMMAND git -C "${REPO_ROOT}" archive -o "${_old_root}/tree.tar" "${_sha}" cpp
                    RESULT_VARIABLE _rc)
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "abi-check: git archive of ${BASELINE} failed")
    endif()
    execute_process(COMMAND "${CMAKE_COMMAND}" -E tar xf tree.tar WORKING_DIRECTORY "${_old_root}")
    file(REMOVE "${_old_root}/tree.tar")
endif()
message(STATUS "abi-check: ${SOURCE_DIR} against ${BASELINE} (${_sha})")

# The soname version of the tree at `tree` (a cpp directory), as its
# CMakeLists.txt derives it from project(): 0.<minor> before 1.0, the major after.
function(_ck_soversion tree out_var)
    file(STRINGS "${tree}/CMakeLists.txt" _project REGEX "^project\\(confluent_kafka_cpp VERSION ")
    string(REGEX MATCH "VERSION ([0-9]+)\\.([0-9]+)" _ "${_project}")
    if(CMAKE_MATCH_1 EQUAL 0)
        set(${out_var} "0.${CMAKE_MATCH_2}" PARENT_SCOPE)
    else()
        set(${out_var} "${CMAKE_MATCH_1}" PARENT_SCOPE)
    endif()
endfunction()

# Writes, for the tree at `tree` (a cpp directory), its probe source, its API
# inventory and, for a compiled library (a src/ tree), its shared library
# under `out`.
function(_ck_tree tree out)
    file(MAKE_DIRECTORY "${out}")
    file(GLOB_RECURSE _headers RELATIVE "${tree}/include" "${tree}/include/confluent/kafka/*.hpp")
    list(SORT _headers)
    set(_probe "// Generated by abi_check.cmake: every public header.\n")
    foreach(_h IN LISTS _headers)
        string(APPEND _probe "#include <${_h}>\n")
    endforeach()
    file(WRITE "${out}/probe.cpp" "${_probe}")
    set(_config "{Checks: '-*,confluent-kafka-inventory', CheckOptions: {ConfluentKafkaInventoryFile: '${out}/inventory.txt', ConfluentKafkaIncludeRoot: '${tree}/include'}}")
    execute_process(
        COMMAND "${CLANG_TIDY}" -load "${PLUGIN}" --quiet "-header-filter=.*" "-config=${_config}" "${out}/probe.cpp"
                -- -std=c++20 "-I${tree}/include" -isystem "${FFI_INCLUDE}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
    if(NOT EXISTS "${out}/inventory.txt")
        message(FATAL_ERROR "abi-check: no inventory for ${tree}:\n${_output}")
    endif()
    if(NOT EXISTS "${tree}/src")
        return()
    endif()
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -S "${tree}" -B "${out}/build" -DCMAKE_BUILD_TYPE=Debug
                "-DCMAKE_CXX_COMPILER=${CXX}" -DCONFLUENT_KAFKA_CPP_BUILD_TESTS=OFF
                "-DCONFLUENT_KAFKA_INCLUDE_DIR=${FFI_INCLUDE}" "-DCONFLUENT_KAFKA_LIB_DIR=${CORE_LIB_DIR}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
    if(_rc EQUAL 0)
        execute_process(COMMAND "${CMAKE_COMMAND}" --build "${out}/build" --target confluent_kafka_cpp
                        RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
    endif()
    if(NOT _rc EQUAL 0)
        message(FATAL_ERROR "abi-check: the library of ${tree} does not build:\n${_output}")
    endif()
endfunction()

_ck_tree("${_old}" "${WORK_DIR}/old")
_ck_tree("${SOURCE_DIR}" "${WORK_DIR}/new")

set(_breaks "")

# 1. The API inventories.
execute_process(COMMAND "${CK_JAVA_MODEL}" api-diff "${WORK_DIR}/old/inventory.txt" "${WORK_DIR}/new/inventory.txt"
                RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
message("-- API inventory\n${_output}")
if(NOT _rc EQUAL 0)
    list(APPEND _breaks "the API inventory")
endif()

# 2. The ABI of the shared libraries. abidiff's exit status is a bit set: 4 an
# ABI change, 8 an incompatible one.
# Script mode (-P) defines no CMAKE_SHARED_LIBRARY_SUFFIX.
if(CMAKE_HOST_APPLE)
    set(_ck_library libconfluent_kafka_cpp.dylib)
else()
    set(_ck_library libconfluent_kafka_cpp.so)
endif()
set(_old_library "")
set(_new_library "")
if(EXISTS "${WORK_DIR}/old/build/${_ck_library}")
    set(_old_library "${WORK_DIR}/old/build/${_ck_library}")
endif()
if(EXISTS "${WORK_DIR}/new/build/${_ck_library}")
    set(_new_library "${WORK_DIR}/new/build/${_ck_library}")
endif()
find_program(_abidiff abidiff)
if(NOT _old_library)
    message("-- abidiff: ${BASELINE} is header-only and shipped no library a program loads; nothing to compare")
elseif(NOT _new_library)
    message(FATAL_ERROR "abi-check: ${SOURCE_DIR} built no shared library")
elseif(NOT _abidiff)
    message(WARNING "abi-check: abidiff not found; the ABI comparison is skipped")
else()
    execute_process(COMMAND "${_abidiff}" --no-added-syms --headers-dir1 "${_old}/include"
                            --headers-dir2 "${SOURCE_DIR}/include" "${_old_library}" "${_new_library}"
                    RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
    message("-- abidiff (exit ${_rc})\n${_output}")
    math(EXPR _abi_bits "${_rc} & 12")
    math(EXPR _error_bits "${_rc} & 3")
    if(NOT _error_bits EQUAL 0)
        message(FATAL_ERROR "abi-check: abidiff failed")
    endif()
    if(NOT _abi_bits EQUAL 0)
        list(APPEND _breaks "the ABI of the shared library")
        # §12.4: a break needs a new soname, or programs built against the
        # baseline load a library they do not fit.
        _ck_soversion("${_old}" _old_soversion)
        _ck_soversion("${SOURCE_DIR}" _new_soversion)
        if(_old_soversion STREQUAL _new_soversion)
            message("-- the ABI breaks under the soname libconfluent_kafka_cpp.so.${_new_soversion} of ${BASELINE}: "
                    "raise the version in CMakeLists.txt (cpp/CLAUDE.md §12.4)")
            list(APPEND _breaks "the soname")
        endif()
    endif()
endif()

# 3. The previous release's unit tests against the new headers. A test of the
# library's private code (one including "detail/...", src/detail) is not part of
# the interface.
file(GLOB _old_tests "${_old}/test/unit/*_test.cpp")
foreach(_test IN LISTS _old_tests)
    file(STRINGS "${_test}" _private REGEX "^#include \"detail/")
    if(_private)
        list(REMOVE_ITEM _old_tests "${_test}")
    endif()
endforeach()
set(_gtest_flags "")
if(DEFINED GTEST_INCLUDE AND NOT GTEST_INCLUDE STREQUAL "")
    set(_gtest_flags -isystem "${GTEST_INCLUDE}")
endif()
set(_failed_tests "")
foreach(_test IN LISTS _old_tests)
    execute_process(
        COMMAND "${CXX}" -std=c++20 -fsyntax-only "-I${SOURCE_DIR}/include" -isystem "${FFI_INCLUDE}" ${_gtest_flags}
                "${_test}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
    if(NOT _rc EQUAL 0)
        get_filename_component(_name "${_test}" NAME)
        list(APPEND _failed_tests "${_name}")
        message("-- ${_name} of ${BASELINE} does not compile against the new headers:\n${_output}")
    endif()
endforeach()
list(LENGTH _old_tests _test_count)
if(_failed_tests)
    message("-- previous tests: ${_test_count} compiled, failing: ${_failed_tests}")
else()
    message("-- previous tests: ${_test_count} compiled, none failing")
endif()
if(_failed_tests)
    list(APPEND _breaks "the previous release's tests")
endif()

if(_breaks)
    string(REPLACE ";" ", " _breaks "${_breaks}")
    if(STRICT)
        message(FATAL_ERROR "abi-check: breaking changes against ${BASELINE} in ${_breaks}")
    endif()
    message(WARNING "abi-check: breaking changes against ${BASELINE} in ${_breaks} (reported, not failing: "
                    "development and preview, cpp/CLAUDE.md §12.4)")
else()
    message(STATUS "abi-check: compatible with ${BASELINE}")
endif()
