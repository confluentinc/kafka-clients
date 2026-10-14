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

"""Static check that ``_confluentkafka.c`` calls only the C API functions that
``confluent_kafka_bindings.h`` declares.

That header declares every function that can fail, and every ``_cb``
function, under its own name, and every other function ``f`` only as its twin
``f_err``. Both take a trailing ``kafka_common_Error_t **out_err`` that
receives every failure, a Rust panic caught at the boundary included. The
plain ``f`` of a function that cannot fail reports a panic as a fallback value
(NULL, 0, -1) that is often a valid answer, so a binding calling it cannot
raise.

The extension includes ``confluent_kafka_bindings.h`` alone, so a call to a
name it does not declare is already an implicit declaration. This check also
catches what the compiler does not: a plain name taken as a function pointer
under a cast, a name built with ``##`` in a macro, and the C users' header
swapped back in.

Every ``kafka_*`` identifier outside comments and literals must be one of:

* a function the bindings header declares;
* a type, ending in ``_t`` or ``_e``;
* an enumerator, its last segment CONSTANT_CASE (``kafka_common_Error_WAKEUP``),
  not followed by ``(``;
* a function the file defines itself (``kafka_principal_to_py``).

A name assembled with ``##`` is judged as a pattern, its first piece and its
last pasted piece kept and anything between them standing for any name:
``kafka_admin_##Rpc##Options_new_err`` is fine because the header declares
``kafka_admin_CreateTopicsOptions_new_err``.

It reads the generated header, so the Rust library must have been built
(``cargo build --release --features ffi``); the C extension need not be::

    python tools/ffi_twins.py [path/to/file.c ...]
"""

import re
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    from tools.format_arity import DEFAULT_SOURCE, _strip_comments
except ImportError:  # run as a script, with tools/ as sys.path[0]
    from format_arity import DEFAULT_SOURCE, _strip_comments

DEFAULT_HEADER = (Path(__file__).resolve().parent.parent.parent
                  / "rust" / "target" / "include" / "confluent_kafka_bindings.h")

_KAFKA = re.compile(r"\bkafka_\w*")
_PASTE = re.compile(r"\s*##\s*([A-Za-z_]\w*)")
_ENUMERATOR = re.compile(r"_[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$")
_INCLUDE = re.compile(r'^\s*#\s*include\s*[<"]([^>"]*)[>"]', re.MULTILINE)
# `name(` in a header with its comments removed: a declared function. A
# function-pointer typedef, `(*name_fn_t)(`, ends in `_t` and is skipped.
_DECLARATION = re.compile(r"\b(kafka_\w+)\s*\(")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
# `static <type> name(` at the start of a line: a function the file defines.
_DEFINITION = re.compile(r"^static\b[^;={}()]*?\b(kafka_\w+)\s*\(", re.MULTILINE)


@dataclass(frozen=True)
class Finding:
    line: int
    name: str


def _code(source):
    """`source` with comments and string / character literals blanked,
    newlines kept so offsets map to the same lines."""
    code, in_literal = _strip_comments(source)
    return "".join(" " if lit and c != "\n" else c for c, lit in zip(code, in_literal))


def declared_functions(header_text):
    """The functions a C header declares."""
    code = _BLOCK_COMMENT.sub(" ", header_text)
    return {name for name in _DECLARATION.findall(code) if not name.endswith(("_t", "_e"))}


def load_declared(header=DEFAULT_HEADER):
    """The functions the generated `confluent_kafka_bindings.h` declares, or
    None when the header has not been generated."""
    header = Path(header)
    if not header.is_file():
        return None
    return declared_functions(header.read_text(encoding="utf-8"))


def scan(source, declared):
    """An include of the plain `confluent_kafka.h`, then the `kafka_*` names in
    `source` that are not functions in `declared`, types, enumerators or the
    file's own functions, in order of appearance."""
    findings = [Finding(source.count("\n", 0, m.start()) + 1, m.group(1))
                for m in _INCLUDE.finditer(source)
                if Path(m.group(1)).name == "confluent_kafka.h"]
    code = _code(source)
    defined = set(_DEFINITION.findall(code))
    for m in _KAFKA.finditer(code):
        name = m.group(0)
        last, end = name, m.end()
        while True:
            paste = _PASTE.match(code, end)
            if paste is None:
                break
            last, end = paste.group(1), paste.end()
        # The fragments after a `##` are visited again as plain identifiers
        # only when they start with `kafka_`, which a fragment never does.
        if last.endswith("_t") or last.endswith("_e"):
            continue
        if last == name:
            if name in declared or name in defined:
                continue
        else:
            pattern = re.compile(re.escape(name) + r"\w*" + re.escape(last))
            if any(pattern.fullmatch(d) for d in declared):
                continue
        called = code[end:end + 64].lstrip().startswith("(")
        if _ENUMERATOR.search(last) and not called:
            continue
        findings.append(Finding(code.count("\n", 0, m.start()) + 1, name if last == name else f"{name}##…{last}"))
    return findings


def count_names(source):
    """How many `kafka_*` identifiers the check looked at."""
    return len(_KAFKA.findall(_code(source)))


def check_file(path, declared):
    """Runs the check over one file, printing a summary and every finding.
    Returns the findings."""
    path = Path(path)
    source = path.read_text(encoding="utf-8")
    findings = scan(source, declared)
    print(f"   {path}: {count_names(source)} kafka_* identifier(s) inspected")
    for f in findings:
        what = ("the C users' header; include confluent_kafka_bindings.h"
                if f.name.endswith(".h") else "not declared in confluent_kafka_bindings.h")
        print(f"{path}:{f.line}: {f.name}: {what}", file=sys.stderr)
    return findings


def main(argv):
    declared = load_declared()
    if declared is None:
        print(f"{DEFAULT_HEADER} not found: build the Rust library first "
              "(cargo build --release --features ffi)", file=sys.stderr)
        return 2
    paths = argv or [DEFAULT_SOURCE]
    total = sum(len(check_file(p, declared)) for p in paths)
    if total:
        print(f"{total} C API name(s) that confluent_kafka_bindings.h does not declare", file=sys.stderr)
        return 1
    print("Every C API name is declared in confluent_kafka_bindings.h.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
