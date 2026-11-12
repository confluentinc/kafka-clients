#!/usr/bin/env python3
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
"""Check the binding's use of the C API (cpp/CLAUDE.md §1, §11).

Every ``kafka_`` identifier in the headers under the given directories must be
declared in ``confluent_kafka_bindings.h``, and no ``_cb`` function may be
called: the binding is synchronous. The counterpart of the Python binding's
``test_ffi_twins.py``.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

IDENTIFIER = re.compile(r"\bkafka_[A-Za-z0-9_]+\b")
# A function declaration: the name followed by `(`, not a function-pointer
# typedef, whose name follows `(*`.
DECLARATION = re.compile(r"(?<!\w)(?<!\(\*)(kafka_[A-Za-z0-9_]+)\s*\(")
CALL = re.compile(r"\b(kafka_[A-Za-z0-9_]+)\s*\(")
COMMENT = re.compile(r"//[^\n]*|/\*.*?\*/", re.DOTALL)


def strip_comments(text: str) -> str:
    return COMMENT.sub(" ", text)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--header", required=True, type=Path, help="confluent_kafka_bindings.h")
    parser.add_argument("roots", nargs="+", type=Path, help="directories holding the binding's headers")
    args = parser.parse_args()

    header = strip_comments(args.header.read_text(encoding="utf-8"))
    declared = set(IDENTIFIER.findall(header))
    functions = set(DECLARATION.findall(header))

    failures: list[str] = []
    checked = 0
    for root in args.roots:
        for path in sorted(root.rglob("*.hpp")):
            checked += 1
            text = strip_comments(path.read_text(encoding="utf-8"))
            for name in sorted(set(IDENTIFIER.findall(text))):
                if name not in declared:
                    failures.append(f"{path}: `{name}` is not declared in {args.header.name}")
            for name in sorted(set(CALL.findall(text))):
                if name in functions and name.endswith("_cb"):
                    failures.append(f"{path}: `{name}` is a `_cb` function; the binding is synchronous")
    for failure in failures:
        print(failure, file=sys.stderr)
    print(f"checked {checked} header(s) against {len(functions)} declared functions: "
          f"{'FAIL' if failures else 'ok'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
