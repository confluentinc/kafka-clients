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
# removed overload fail. Each case is HEAD's cpp/include and cpp/test, patched.
#
#   cmake -DREPO_ROOT=... -DWORK_DIR=... -DCLANG_TIDY=... -DPLUGIN=...
#         -DCK_JAVA_MODEL=... -DFFI_INCLUDE=... -DABI_CHECK=<abi_check.cmake>
#         -P abi_check_test.cmake

file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}/head")
execute_process(COMMAND git -C "${REPO_ROOT}" archive -o "${WORK_DIR}/head.tar" HEAD cpp/include cpp/test
                RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "git archive HEAD failed")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E tar xf "${WORK_DIR}/head.tar" WORKING_DIRECTORY "${WORK_DIR}/head")

set(_failures "")
# Runs the check on HEAD patched by replacing `from` with `to` in `file`
# (relative to cpp/include); `expect` is PASS or FAIL.
function(_ck_case name expect file from to)
    set(_tree "${WORK_DIR}/${name}")
    file(COPY "${WORK_DIR}/head/cpp" DESTINATION "${_tree}")
    if(NOT file STREQUAL "")
        file(READ "${_tree}/cpp/include/${file}" _text)
        string(FIND "${_text}" "${from}" _at)
        if(_at EQUAL -1)
            message(FATAL_ERROR "${name}: '${from}' is not in ${file}")
        endif()
        string(REPLACE "${from}" "${to}" _text "${_text}")
        file(WRITE "${_tree}/cpp/include/${file}" "${_text}")
    endif()
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -DREPO_ROOT=${REPO_ROOT} -DSOURCE_DIR=${_tree}/cpp -DWORK_DIR=${_tree}/work
                -DCLANG_TIDY=${CLANG_TIDY} -DPLUGIN=${PLUGIN} -DCK_JAVA_MODEL=${CK_JAVA_MODEL}
                -DFFI_INCLUDE=${FFI_INCLUDE} -DBASELINE=HEAD -DSTRICT=ON -P "${ABI_CHECK}"
        RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
    if((expect STREQUAL "PASS" AND NOT _rc EQUAL 0) OR (expect STREQUAL "FAIL" AND _rc EQUAL 0))
        message("${name}: expected ${expect}, got exit ${_rc}:\n${_output}")
        set(_failures "${_failures};${name}" PARENT_SCOPE)
    else()
        message("${name}: ${expect} as expected")
    endif()
endfunction()

_ck_case(unchanged PASS "" "" "")
_ck_case(appended_enumerator PASS confluent/kafka/common/resource/pattern_type.hpp
         "    PREFIXED = 4,\n" "    PREFIXED = 4,\n    /// Appended.\n    APPENDED = 5,\n")
_ck_case(inserted_enumerator FAIL confluent/kafka/common/resource/pattern_type.hpp
         "    UNKNOWN = 0,\n" "    UNKNOWN = 0,\n    /// Inserted.\n    INSERTED = 9,\n")
_ck_case(removed_overload FAIL confluent/kafka/common/node.hpp
         "    Node(int32_t id, std::optional<std::string_view> host, int32_t port) : Node(id, host, port, std::nullopt, false) {}\n"
         "")
if(_failures)
    message(FATAL_ERROR "abi_check_test failed:${_failures}")
endif()
