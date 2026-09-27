# Copyright 2025 Confluent Inc.
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

"""Staleness check of the generated ``_error_code.py``."""

import pytest

from tools.generate_error_code import (
    ERROR_CODE_PY, parse_error_codes, render,
)


def test_error_code_py_is_up_to_date():
    assert ERROR_CODE_PY.read_text(encoding="utf-8") == render(), (
        "Stale generated error-code constants in _error_code.py. "
        "Run: python tools/generate_error_code.py")


def test_parse_reads_negative_values_and_stops_at_the_enum_end():
    source = (
        "pub enum kafka_common_ErrorCode_t {\n"
        "    kafka_common_ErrorCode_NONE = 0,\n"
        "    kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR = -1,\n"
        "}\n"
        "kafka_common_ErrorCode_AFTER = 7,\n"
    )
    assert parse_error_codes(source) == [
        ("NONE", 0), ("UNKNOWN_SERVER_ERROR", -1)]


def test_parse_fails_without_the_enum():
    with pytest.raises(ValueError, match="kafka_common_ErrorCode_t not found"):
        parse_error_codes("pub enum Other {}\n")


def test_parse_fails_on_an_empty_enum():
    with pytest.raises(ValueError, match="has no enumerators"):
        parse_error_codes("pub enum kafka_common_ErrorCode_t {\n}\n")
