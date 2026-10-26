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

"""Tests of ``_common``: the properties reader, translated from the Rust
tool's ``parse_properties_handles_common_forms`` and extended with the
``java.util.Properties`` forms Kafka's system tests write, and ``print_json``
on a stdout that fails."""

from __future__ import annotations

import io
from typing import Any

import pytest

import _common
from _common import parse_properties, print_json


def test_parse_properties_handles_common_forms() -> None:
    contents = (
        "# a comment\n"
        "! another comment\n"
        "\n"
        "bootstrap.servers=localhost:9092\n"
        "linger.ms : 5\n"
        "compression.type snappy\n"
        "  padded.key = padded.value\n")
    assert parse_properties(contents) == {
        "bootstrap.servers": "localhost:9092",
        "linger.ms": "5",
        "compression.type": "snappy",
        "padded.key": "padded.value",
    }


def test_parse_properties_keeps_separators_inside_the_value() -> None:
    assert parse_properties("listeners=PLAINTEXT://host:9092\nkey:=value\n") == {
        "listeners": "PLAINTEXT://host:9092", "key": "=value"}


def test_parse_properties_later_line_wins() -> None:
    assert parse_properties("acks=1\nacks=all\n") == {"acks": "all"}


def test_parse_properties_joins_continued_lines() -> None:
    # What kafkatest's SecurityConfig writes for a SASL_PLAINTEXT client:
    # sasl.jaas.config is the JAAS file with each newline as " \\\n".
    jaas = ("/**\n"
            " * Licensed to the Apache Software Foundation (ASF)\n"
            " */\n"
            "\n"
            "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
            "    username=\"client\"\n"
            "    password=\"client-secret\";\n")
    contents = ("security.protocol=SASL_PLAINTEXT\n"
                "sasl.jaas.config=" + jaas.replace("\n", " \\\n") + "\n"
                "sasl.mechanism=PLAIN\n")
    props = parse_properties(contents)
    assert props == {
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.jaas.config": (
            "/** * Licensed to the Apache Software Foundation (ASF) */ "
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            "username=\"client\" password=\"client-secret\";"),
        "sasl.mechanism": "PLAIN",
    }


def test_parse_properties_comment_characters_on_a_continuation_are_text() -> None:
    assert parse_properties("key=a \\\n  # not a comment\n") == {"key": "a # not a comment"}


def test_parse_properties_even_backslashes_do_not_continue() -> None:
    assert parse_properties("path=C:\\\\\nnext=1\n") == {"path": "C:\\", "next": "1"}


def test_parse_properties_escapes() -> None:
    assert parse_properties("a\\=b=c\\td\nu=\\u0041\\x\n") == {"a=b": "c\td", "u": "Ax"}


def test_parse_properties_malformed_unicode_escape() -> None:
    with pytest.raises(ValueError, match="Malformed"):
        parse_properties("u=\\u00G1\n")


def test_parse_properties_handles_crlf() -> None:
    assert parse_properties("a=1\r\nb=2\rc=3") == {"a": "1", "b": "2", "c": "3"}


class _BrokenStdout(io.StringIO):
    def write(self, s: str) -> int:
        raise BrokenPipeError(32, "Broken pipe")


def test_print_json_drops_a_failed_write(monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's PrintStream only sets an error flag: the tools carry on.
    discarded: list[bool] = []
    monkeypatch.setattr("sys.stdout", _BrokenStdout())
    monkeypatch.setattr(_common, "_discard_stdout", lambda: discarded.append(True))
    print_json({"name": "startup_complete"})
    print_json({"name": "shutdown_complete"})
    assert discarded == [True, True]


def test_print_json_writes_one_compact_line(capsys: pytest.CaptureFixture[str]) -> None:
    data: dict[str, Any] = {"timestamp": 1, "name": "x", "key": None}
    print_json(data)
    assert capsys.readouterr().out == '{"timestamp":1,"name":"x","key":null}\n'
