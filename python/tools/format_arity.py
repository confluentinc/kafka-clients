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

"""Static arity checker for the hand-written CPython extension module.

``Py_BuildValue`` and ``PyArg_ParseTuple`` / ``PyArg_ParseTupleAndKeywords``
are variadic: a format string with one unit too few (or too many) for the
arguments supplied compiles cleanly, is not diagnosed by any compiler warning,
and reads a garbage pointer at run time. Worse, the mismatch is frequently
*unreachable from the test suite*: for every admin RPC that Java's own
``MockAdminClient`` leaves unsupported, the success path of the corresponding
``*_drain`` is dead code in the unit tests, so only the error branch is ever
executed.

``PyObject_CallFunction`` takes the same ``Py_BuildValue`` grammar and is the
same defect class; it is scanned too, and it matters more than its call count
suggests, because every site is a **callback trampoline** -- the code path
hardest to reach from a unit test, which is this checker's whole rationale.

This checker parses every call site in ``_confluentkafka.c``, counts the format
units in the (string-literal) format argument, and compares that against the
number of top-level arguments actually passed. Any mismatch fails
``test/static/test_format_arity.py``.

What it does **not** check, so that the gate is not read as broader than it is:

* **Argument order.** A transposed pair of same-typed fields is statically
  undetectable here and has to be caught by a test or by review.
* **Argument types.** A ``long`` passed under ``i``, or an ``int64_t`` under
  ``i``, is an arity match and a run-time defect.
* **``N`` versus ``O`` reference stealing** in ``Py_BuildValue``. ``N`` steals
  the reference and ``O`` increments it; using the wrong one leaks or
  double-frees, and the counter cannot tell them apart.
* **``PyArg_ParseTupleAndKeywords`` kwlist length.** The kwlist must hold one
  ``NULL``-terminated name per non-positional unit; a short one reads past the
  array.
* ``PyErr_Format``, whose format is printf grammar rather than either of these
  two, so it needs a separate counter.

Run as a script to check other files, e.g. an older revision of the extension
extracted with ``git show``, to demonstrate that it detects a known-bad site::

    python tools/format_arity.py [path/to/file.c ...]
"""

import enum
import sys
from dataclasses import dataclass
from pathlib import Path

# Default subject of the check.
DEFAULT_SOURCE = Path(__file__).resolve().parent.parent / "_confluentkafka.c"


class Kind(enum.Enum):
    """The four variadic CPython entry points we can check.

    Each value is ``(name, format_index, fixed_args)``:

    * ``Py_BuildValue(format, ...)`` -- 1 fixed argument (the format itself).
    * ``PyObject_CallFunction(callable, format, ...)`` -- 2, and the same
      ``Py_BuildValue`` grammar for the rest.
    * ``PyArg_ParseTuple(args, format, ...)`` -- 2 fixed arguments.
    * ``PyArg_ParseTupleAndKeywords(args, kwargs, format, kwlist, ...)`` -- 4.
    """

    BUILD_VALUE = ("Py_BuildValue", 0, 1)
    CALL_FUNCTION = ("PyObject_CallFunction", 1, 2)
    PARSE_TUPLE = ("PyArg_ParseTuple", 1, 2)
    PARSE_TUPLE_AND_KEYWORDS = ("PyArg_ParseTupleAndKeywords", 2, 4)

    @property
    def function(self):
        return self.value[0]

    @property
    def format_index(self):
        """Zero-based index of the format string within the argument list."""
        return self.value[1]

    @property
    def fixed_args(self):
        """Number of arguments that are not consumed by format units."""
        return self.value[2]

    @property
    def is_parse(self):
        """``Py_BuildValue`` and ``PyObject_CallFunction`` read their arguments;
        ``PyArg_Parse*`` writes through pointers. The two format grammars differ
        (``|``, ``$``, ``:``, ``;``, ``O!``, ``es#``, ``s*`` are parse-only), so
        the unit counter needs to know which."""
        return self not in (Kind.BUILD_VALUE, Kind.CALL_FUNCTION)


# Scan order: the longer `PyArg_ParseTupleAndKeywords` before its prefix.
_SCAN_ORDER = (Kind.BUILD_VALUE, Kind.CALL_FUNCTION,
               Kind.PARSE_TUPLE_AND_KEYWORDS, Kind.PARSE_TUPLE)


@dataclass(frozen=True)
class Finding:
    """One problem found at one call site."""

    # 1-based line number of the call in the source file.
    line: int
    # Which CPython function.
    function: str
    # Human-readable description of the problem.
    detail: str


def scan(source):
    """Scans ``source`` (the full text of a C file) and returns every arity
    problem, as a list of :class:`Finding`.

    Comments are stripped first, and string literals are tracked, so a call
    mentioned inside a comment or a string is not scanned.
    """
    code, in_literal = _strip_comments(source)
    findings = []

    for kind, at, split in _call_sites(code, in_literal):
        line = _line_of(source, at)
        if split is None:
            findings.append(Finding(
                line, kind.function,
                "unterminated argument list (unbalanced parentheses?)"))
        else:
            args, _end = split
            findings.extend(_check_call(kind, line, code, in_literal, args))

    findings.sort(key=lambda f: (f.line, f.function))
    return findings


def _call_sites(code, in_literal):
    """Yields ``(kind, offset, split)`` for every call site, where ``split``
    is the :func:`_split_arguments` result for its argument list.

    After a call whose argument list is balanced, the search for the same
    function resumes past its closing ``)``.
    """
    for kind in _SCAN_ORDER:
        needle = kind.function
        start = 0
        while True:
            at = code.find(needle, start)
            if at < 0:
                break
            start = at + len(needle)

            # Skip matches inside a string literal, and matches that are part
            # of a longer identifier. `PyArg_ParseTuple` is a prefix of
            # `PyArg_ParseTupleAndKeywords`, so the right-boundary check is
            # what keeps the two from double-counting (the longer name is
            # scanned first, but each scan is independent).
            if in_literal[at]:
                continue
            if at > 0 and _is_ident_char(code[at - 1]):
                continue
            after = at + len(needle)
            if after < len(code) and _is_ident_char(code[after]):
                continue

            # Skip whitespace between the name and its `(`. A declaration or a
            # function-pointer mention with no call parenthesis is ignored.
            i = after
            while i < len(code) and code[i].isspace():
                i += 1
            if i >= len(code) or code[i] != "(":
                continue

            split = _split_arguments(code, in_literal, i)
            if split is not None:
                start = split[1]
            yield kind, at, split


def _check_call(kind, line, code, in_literal, args):
    """Checks one call site whose top-level arguments have already been
    split."""
    fmt_index = kind.format_index
    if fmt_index >= len(args):
        return [Finding(
            line, kind.function,
            f"call has {len(args)} argument(s) but the format string is "
            f"argument {fmt_index + 1}")]

    fmt_start, fmt_end = args[fmt_index]
    fmt = _string_literal(code[fmt_start:fmt_end], in_literal[fmt_start:fmt_end])
    if fmt is None:
        # A non-literal format cannot be checked. That is a hole in the gate,
        # so it is reported rather than silently skipped: either make the
        # format a literal, or the mismatch class this checker exists to
        # catch is unguarded at that site.
        return [Finding(
            line, kind.function,
            f"format argument is not a string literal "
            f"({code[fmt_start:fmt_end].strip()}) — arity cannot be "
            f"checked statically")]

    try:
        units = count_units(fmt, kind.is_parse)
    except ValueError as err:
        return [Finding(line, kind.function, f'format "{fmt}": {err}')]

    expected = kind.fixed_args + units
    if len(args) == expected:
        return []
    return [Finding(
        line, kind.function,
        f'format "{fmt}" needs {units} format unit(s), so the call takes '
        f"{expected} argument(s), but {len(args)} were passed")]


def count_units(fmt, parse):
    """Counts the arguments a CPython format string consumes.

    ``parse`` selects the ``PyArg_Parse*`` grammar (pointer targets, ``|``/``$``
    separators, ``:``/``;`` terminators) over the ``Py_BuildValue`` one.

    Structural characters (``()[]{}``, ``,``, whitespace, ``:`` in build mode
    for dict keys) consume nothing.

    Raises ``ValueError`` for a unit outside the grammar.
    """
    i = 0
    units = 0

    def peek(j):
        return fmt[j] if j < len(fmt) else None

    # `?` after a unit means "or None" in some third-party conventions but is
    # not CPython; every character below is from the documented grammar.
    while i < len(fmt):
        c = fmt[i]
        i += 1

        # Structural / separators -- no argument consumed.
        if c in "()[]{}, \t\n":
            pass
        elif parse and c in "|$":
            pass
        elif not parse and c == ":":
            pass
        # In PyArg_Parse* a `:` or `;` ends the format; what follows is a
        # function name (for error messages) or a custom error message.
        elif parse and c in ":;":
            break

        # Strings and buffers.
        elif c in "szy":
            if peek(i) == "#":
                i += 1
                units += 2
            elif parse and peek(i) == "*":
                i += 1
                units += 1
            else:
                units += 1
        elif c in "uZ":
            if peek(i) == "#":
                i += 1
                units += 2
            else:
                units += 1
        elif c == "U":
            # `U#` existed in old Py_BuildValue formats; harmless to accept.
            if not parse and peek(i) == "#":
                i += 1
                units += 2
            else:
                units += 1
        elif parse and c == "w":
            if peek(i) in ("*", "#"):
                two = peek(i) == "#"
                i += 1
                units += 2 if two else 1
            else:
                raise ValueError(
                    "`w` must be followed by `*` (or the deprecated `#`)")
        elif parse and c == "e":
            # `es`/`et` take (encoding, char **buffer); with `#` also a length.
            if peek(i) in ("s", "t"):
                i += 1
            else:
                raise ValueError("`e` must be followed by `s` or `t`")
            if peek(i) == "#":
                i += 1
                units += 3
            else:
                units += 2

        # Objects.
        elif c == "O":
            if peek(i) == "&" or (parse and peek(i) == "!"):
                i += 1
                units += 2
            else:
                units += 1
        elif c in "SN":
            units += 1
        elif parse and c == "Y":
            units += 1

        # Scalars.
        elif c in "bBhHiIlkLKncCdfDp":
            units += 1

        else:
            raise ValueError(f"unknown format unit `{c}`")

    return units


def _split_arguments(code, in_literal, open_paren):
    """Splits the argument list of a call whose ``(`` is at offset
    ``open_paren``.

    Returns the ``(start, end)`` range of each top-level argument and the
    offset just past the closing ``)``, or ``None`` when the list is
    unterminated. Nested calls, casts, array subscripts, braced initializers
    and string/char literals are all skipped, so only commas at nesting depth
    zero split.

    A call written ``f()`` yields zero arguments; ``f("")`` yields one.
    """
    assert code[open_paren] == "("
    depth = 0
    args = []
    start = open_paren + 1

    for i in range(open_paren, len(code)):
        if in_literal[i]:
            continue
        c = code[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
            if depth == 0:
                last = code[start:i]
                if not (not args and not last.strip()):
                    args.append((start, i))
                return args, i + 1
        elif c == "," and depth == 1:
            args.append((start, i))
            start = i + 1
    return None


def _string_literal(expr, in_literal):
    """Decodes a C expression that is one or more adjacent string literals
    into the concatenated string it denotes. Returns ``None`` when the
    expression is anything else (an identifier, a macro, a ternary, ...)."""
    out = []
    i = 0
    saw_one = False

    while i < len(expr):
        c = expr[i]
        if c.isspace():
            i += 1
            continue
        if c != '"' or not in_literal[i]:
            return None
        i += 1  # opening quote
        while i < len(expr) and expr[i] != '"':
            if expr[i] == "\\":
                # Keep the escape's *effect* on unit counting minimal: the
                # only escapes that could appear in a CPython format string
                # are `\"` and `\\`, neither of which is a format unit.
                i += 1
                if i < len(expr):
                    out.append(expr[i])
                    i += 1
                continue
            out.append(expr[i])
            i += 1
        if i >= len(expr):
            return None  # unterminated
        i += 1  # closing quote
        saw_one = True

    return "".join(out) if saw_one else None


def _strip_comments(source):
    """Replaces every comment character with a space (newlines preserved so
    line numbers do not move) and returns, alongside the rewritten text, a
    mask marking every character that lies inside a string or character
    literal (including the delimiting quotes)."""
    out = []
    mask = []
    i = 0
    n = len(source)

    while i < n:
        c = source[i]
        nxt = source[i + 1] if i + 1 < n else None

        if c == "/" and nxt == "/":
            while i < n and source[i] != "\n":
                out.append(" ")
                mask.append(False)
                i += 1
            continue
        if c == "/" and nxt == "*":
            j = i
            while j < n:
                end = source[j] == "*" and j + 1 < n and source[j + 1] == "/"
                out.append("\n" if source[j] == "\n" else " ")
                mask.append(False)
                j += 1
                if end:
                    out.append(" ")
                    mask.append(False)
                    j += 1
                    break
            i = j
            continue
        if c in "\"'":
            quote = c
            out.append(c)
            mask.append(True)
            i += 1
            while i < n:
                if source[i] == "\\":
                    out.append(source[i])
                    mask.append(True)
                    i += 1
                    if i < n:
                        out.append(source[i])
                        mask.append(True)
                        i += 1
                    continue
                closing = source[i] == quote
                out.append(source[i])
                mask.append(True)
                i += 1
                if closing:
                    break
            continue

        out.append(c)
        mask.append(False)
        i += 1

    return "".join(out), mask


def _is_ident_char(c):
    return c == "_" or (c.isascii() and c.isalnum())


def _line_of(source, offset):
    return source.count("\n", 0, offset) + 1


def count_sites(source):
    """Counts inspected call sites, for the summary line: ``Py_BuildValue``,
    ``PyObject_CallFunction`` and ``PyArg_Parse*`` respectively."""
    code, in_literal = _strip_comments(source)
    build = call = parse = 0
    for kind in _SCAN_ORDER:
        needle = kind.function
        start = 0
        while True:
            at = code.find(needle, start)
            if at < 0:
                break
            start = at + len(needle)
            if in_literal[at] or (at > 0 and _is_ident_char(code[at - 1])):
                continue
            after = at + len(needle)
            if after < len(code) and _is_ident_char(code[after]):
                continue
            i = after
            while i < len(code) and code[i].isspace():
                i += 1
            if i >= len(code) or code[i] != "(":
                continue
            if kind is Kind.BUILD_VALUE:
                build += 1
            elif kind is Kind.CALL_FUNCTION:
                call += 1
            else:
                parse += 1
    return build, call, parse


def check_file(path):
    """Runs the check over one file, printing a per-file summary and every
    finding. Returns the findings."""
    path = Path(path)
    source = path.read_text(encoding="utf-8")
    findings = scan(source)
    build, call, parse = count_sites(source)
    print(f"   {path}: {build} Py_BuildValue, {call} PyObject_CallFunction "
          f"and {parse} PyArg_Parse* call site(s) inspected")
    for f in findings:
        print(f"{path}:{f.line}: {f.function}: {f.detail}", file=sys.stderr)
    return findings


def main(argv):
    paths = argv or [DEFAULT_SOURCE]
    total = sum(len(check_file(p)) for p in paths)
    if total:
        print(f"{total} CPython format-arity problem(s)", file=sys.stderr)
        return 1
    print("No format-arity mismatches found!")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
