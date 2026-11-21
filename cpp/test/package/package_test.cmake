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

# Installs a build tree of the binding, builds test/package against it with
# find_package and runs its two programs (cpp/CLAUDE.md §13.1).
#
#   cmake -DBUILD_DIR=<cpp build tree> -DWORK_DIR=<scratch> -DCORE_LIB_DIR=<core libraries>
#         -DGENERATOR=<generator> -DCXX=<compiler> -P package_test.cmake

file(REMOVE_RECURSE "${WORK_DIR}")
execute_process(COMMAND "${CMAKE_COMMAND}" --install "${BUILD_DIR}" --prefix "${WORK_DIR}/prefix"
                RESULT_VARIABLE _rc OUTPUT_QUIET)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "installing ${BUILD_DIR} failed")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -S "${CMAKE_CURRENT_LIST_DIR}" -B "${WORK_DIR}/build" -G "${GENERATOR}"
            "-DCMAKE_CXX_COMPILER=${CXX}" "-DCMAKE_PREFIX_PATH=${WORK_DIR}/prefix"
            "-DConfluentKafkaCpp_CORE_LIB_DIR=${CORE_LIB_DIR}"
    RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "configuring the program against the package failed:\n${_output}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" --build "${WORK_DIR}/build"
                RESULT_VARIABLE _rc OUTPUT_VARIABLE _output ERROR_VARIABLE _output)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "building the program against the package failed:\n${_output}")
endif()

set(_expected "localhost:9092 (id: 1 rack: rack-a isFenced: false)")
foreach(_program shared_program static_program)
    execute_process(COMMAND "${WORK_DIR}/build/${_program}" RESULT_VARIABLE _rc OUTPUT_VARIABLE _output
                    ERROR_VARIABLE _output OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT _rc EQUAL 0 OR NOT _output STREQUAL _expected)
        message(FATAL_ERROR "${_program}: exit ${_rc}, printed '${_output}', expected '${_expected}'")
    endif()
    message(STATUS "${_program}: ${_output}")
endforeach()
