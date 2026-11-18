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

# Tests abi_check.cmake (cpp/CLAUDE.md §12.4) against HEAD with STRICT=ON: an
# unchanged tree and an appended enumerator pass; an inserted enumerator and a
# removed overload fail. Each case is HEAD's cpp/ tree, patched.
#
#   cmake -DREPO_ROOT=... -DWORK_DIR=... -DCLANG_TIDY=... -DPLUGIN=...
#         -DCK_JAVA_MODEL=... -DFFI_INCLUDE=... -DCORE_LIB_DIR=... -DABI_CHECK=<abi_check.cmake>
#         -P abi_check_test.cmake

file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}/head")
execute_process(COMMAND git -C "${REPO_ROOT}" archive -o "${WORK_DIR}/head.tar" HEAD cpp
                RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "git archive HEAD failed")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E tar xf "${WORK_DIR}/head.tar" WORKING_DIRECTORY "${WORK_DIR}/head")

set(_failures "")
# Runs the check on HEAD's cpp/ tree patched by each `<file> <from> <to>` that
# follows (files relative to cpp/, no `;` in a patch); `expect` is PASS or FAIL, and the check's
# output must contain `contains` (empty: anything).
function(_ck_case name expect contains)
    set(_tree "${WORK_DIR}/${name}")
    file(COPY "${WORK_DIR}/head/cpp" DESTINATION "${_tree}")
    set(_patches "${ARGN}")
    list(LENGTH _patches _count)
    set(_i 0)
    while(_i LESS _count)
        list(GET _patches ${_i} _file)
        math(EXPR _j "${_i} + 1")
        list(GET _patches ${_j} _from)
        math(EXPR _k "${_i} + 2")
        list(GET _patches ${_k} _to)
        file(READ "${_tree}/cpp/${_file}" _text)
        string(FIND "${_text}" "${_from}" _at)
        if(_at EQUAL -1)
            message(FATAL_ERROR "${name}: '${_from}' is not in ${_file}")
        endif()
        string(REPLACE "${_from}" "${_to}" _text "${_text}")
        file(WRITE "${_tree}/cpp/${_file}" "${_text}")
        math(EXPR _i "${_i} + 3")
    endwhile()
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -DREPO_ROOT=${REPO_ROOT} -DSOURCE_DIR=${_tree}/cpp -DWORK_DIR=${_tree}/work
                -DCLANG_TIDY=${CLANG_TIDY} -DPLUGIN=${PLUGIN} -DCK_JAVA_MODEL=${CK_JAVA_MODEL}
                -DFFI_INCLUDE=${FFI_INCLUDE} -DCORE_LIB_DIR=${CORE_LIB_DIR} -DBASELINE=HEAD -DSTRICT=ON
                -P "${ABI_CHECK}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
    string(FIND "${_output}" "${contains}" _found)
    if((expect STREQUAL "PASS" AND NOT _rc EQUAL 0) OR (expect STREQUAL "FAIL" AND _rc EQUAL 0) OR _found EQUAL -1)
        message("${name}: expected ${expect} with '${contains}', got exit ${_rc}:\n${_output}")
        set(_failures "${_failures};${name}" PARENT_SCOPE)
    else()
        message("${name}: ${expect} as expected")
    endif()
endfunction()

set(_pattern_type include/confluent/kafka/common/resource/pattern_type.hpp)
_ck_case(unchanged PASS "abi-check: compatible with HEAD")
_ck_case(appended_enumerator PASS "abi-check: compatible with HEAD" ${_pattern_type}
         "    PREFIXED = 4,\n" "    PREFIXED = 4,\n    /// Appended.\n    APPENDED = 5,\n")
# The values stay, so the library's ABI does too; the order is the API's.
_ck_case(inserted_enumerator FAIL "breaking changes against HEAD in the API inventory" ${_pattern_type}
         "    UNKNOWN = 0,\n" "    UNKNOWN = 0,\n    /// Inserted.\n    INSERTED = 9,\n")
# A removed overload is a removed symbol, under the baseline's soname. (A `;`
# would split CMake's argument list: the declaration is renamed up to it.)
_ck_case(removed_overload FAIL "breaks under the soname libconfluent_kafka_cpp.so.0.1"
         include/confluent/kafka/common/node.hpp
         "    Node(int32_t id, std::optional<std::string_view> host, int32_t port, std::optional<std::string_view> rack)"
         "    void removed()"
         src/common/node.cpp
         "Node::Node(int32_t id, std::optional<std::string_view> host, int32_t port, std::optional<std::string_view> rack)\n    : Node(id, host, port, rack, false) {}\n"
         "")
if(_failures)
    message(FATAL_ERROR "abi_check_test failed:${_failures}")
endif()
