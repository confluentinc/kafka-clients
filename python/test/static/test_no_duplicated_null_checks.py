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

"""The FFI-backed consumer and producer modules repeat no null check the core
makes: a ``None`` for a mandatory argument crosses as NULL and the core answers
with Java's error (CLAUDE.md §4 and Python Binding Conventions, Implementation
over the FFI). A null-check text left in their code is one of the residual
checks below, each for a value the FFI cannot carry as NULL; any other is a
duplicate that would hide the core's error.

The admin modules are not scanned: their hand-written per-RPC marshaling keeps
its checks (a deviation recorded in the change's report).

Reads source files only; needs neither the Rust library nor the C extension."""

from __future__ import annotations

import ast
import re
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[2] / "confluent_kafka"
SCANNED = [
    "consumer/_base.py",
    "consumer/_conversions.py",
    "consumer/consumer.py",
    "consumer/async_consumer.py",
    "producer/_base.py",
    "producer/_send.py",
    "producer/producer.py",
    "producer/async_producer.py",
]
NULL_TEXT = re.compile(r"\b(must not be|cannot be|could not be) None\b|must not contain None")
# Each residual text and why the FFI cannot take that None as NULL.
RESIDUAL = {
    # A Duration crosses as milliseconds, -1 meaning the untimed overload.
    "timeout must not be None",
    # A CloseOptions crosses as its fields.
    "option must not be None",
    # commit(offsets=None) reaches the FFI as "commit the consumed offsets".
    "offsets must not be None",
    # seek's partition, offset and offset_and_metadata cross as fields.
    "offset_and_metadata must not be None",
    "partition must not be None",
    "offset must not be None",
    # committed(partitions=None, timeout=None): Java dereferences the partitions
    # before the null Duration, which cannot cross the FFI.
    "partitions must not be None",
    # A SubscriptionPattern crosses as its pattern string, whose NULL is a NULL pattern.
    "SubscriptionPattern.pattern must not be None",
}


def _texts(path: Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and node.body and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    return [
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        and id(node) not in docstrings and NULL_TEXT.search(node.value)
    ]


def test_ffi_backed_modules_repeat_no_core_null_check() -> None:
    found = [(rel, line, text) for rel in SCANNED for line, text in _texts(PACKAGE / rel)]
    assert found, "no null-check text found at all: wrong files?"
    extra = [f"{rel}:{line}: {text!r}" for rel, line, text in found if text not in RESIDUAL]
    assert not extra, "null checks the core already makes:\n" + "\n".join(extra)


def test_the_scanner_skips_docstrings_and_finds_raises(tmp_path: Path) -> None:
    source = tmp_path / "m.py"
    source.write_text('def f(x):\n    """x must not be None."""\n'
                      '    if x is None:\n        raise TypeError("x must not be None")\n')
    assert _texts(source) == [(4, "x must not be None")]
