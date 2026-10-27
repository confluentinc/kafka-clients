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

"""JDK behaviour the package reproduces: ``String.valueOf``, the number
parsers and ``UUID.fromString``.

The Java ``toString()`` methods the package translates to ``__str__`` and the
messages its errors build concatenate values with ``+``, which calls
``String.valueOf``. Python's ``str`` renders the same values differently
(``None`` / ``null``, ``True`` / ``true``, ``{'a': 1}`` / ``{a=1}``,
``1e+20`` / ``1.0E20``), so every such concatenation goes through
:func:`java_str`. The config coercion parses numbers as Java's
``Long.parseLong`` / ``Integer.parseInt`` / ``Double.parseDouble`` do, and the
UUID deserializer as ``java.util.UUID.fromString`` does; each accepts and
rejects different text than Python's ``int`` / ``float`` / ``uuid.UUID``.
"""

from __future__ import annotations

import math
import re
import sys
import unicodedata
import uuid
from collections.abc import Mapping
from decimal import Decimal, localcontext

__all__ = ["java_str", "java_trim", "parse_double", "parse_int", "parse_long", "parse_long_string",
           "uuid_from_string"]


def _closest_of_two_digits(a: float, shortest: Decimal) -> Decimal:
    """The decimal Java's ``Double.toString`` takes when the shortest decimal that
    reads back as ``a`` has one digit: the one closest to ``a`` among those of one
    or two digits that read back as ``a``."""
    exact = Decimal(a)
    with localcontext() as ctx:
        ctx.prec = 800  # a double's exact value has at most 767 digits
        best, gap = shortest, abs(shortest - exact)
        # No two are equally close: the exact value has hundreds of digits.
        for significand in range(10, 100):
            candidate = Decimal(significand).scaleb(exact.adjusted() - 1)
            if float(candidate) == a and abs(candidate - exact) < gap:
                best, gap = candidate, abs(candidate - exact)
    return best


def _double_to_string(d: float) -> str:
    """Java's ``Double.toString``, as specified since JDK 19 (older JDKs print
    extra digits for some values: ``9.999999999999999E22`` for ``1.0E23``)."""
    if math.isnan(d):
        return "NaN"
    if math.isinf(d):
        return "Infinity" if d > 0 else "-Infinity"
    if d == 0.0:
        return "-0.0" if math.copysign(1.0, d) < 0 else "0.0"
    a = abs(d)
    if 1e-3 <= a < 1e7:
        # Python's repr is the shortest round-trip form and is positional in
        # this range; it keeps a ".0" on an integral value, as Java does.
        return repr(d)
    # Computerized scientific notation: one digit, a point, the rest, "E" exp.
    shortest = Decimal(repr(a))
    if a < sys.float_info.min and len(shortest.normalize().as_tuple().digits) == 1:
        # Only a subnormal can have a closer decimal of two digits that reads
        # back as it (4.9E-324, not 5.0E-324); a normal double's two-digit
        # neighbours are too far from it.
        shortest = _closest_of_two_digits(a, shortest)
    _, digits, exponent = shortest.normalize().as_tuple()
    text = "".join(str(x) for x in digits)
    exp10 = len(text) - 1 + int(exponent)
    mantissa = text[0] + "." + (text[1:] or "0")
    return ("-" if d < 0 else "") + f"{mantissa}E{exp10}"


def java_str(value: object) -> str:
    """``String.valueOf(value)``: the text Java's string concatenation gives, with
    Python's spelling of ``None``, ``True`` and ``False`` (CLAUDE.md, Python Binding
    Conventions, Idiom translations: ``toString()`` -> ``__str__``).

    ``None`` is ``None``, a ``bool`` is ``True`` / ``False``, a float follows
    ``Double.toString``, a collection is ``[a, b]`` and a mapping ``{k=v}`` with
    their elements converted the same way; everything else is ``str(value)``,
    which for the package's own types is Java's ``toString()``. The strings the
    core parses keep Java's ``null`` / ``true`` / ``false``
    (``_config.convert_to_string``).
    """
    if value is None:
        return "None"
    if value is True:
        return "True"
    if value is False:
        return "False"
    if isinstance(value, str):
        return value
    if isinstance(value, float):
        return _double_to_string(value)
    if isinstance(value, Mapping):
        return "{" + ", ".join(
            f"{java_str(k)}={java_str(v)}" for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple, set, frozenset)):
        return "[" + ", ".join(java_str(v) for v in value) + "]"
    if isinstance(value, BaseException):
        from ._throwable import to_string

        return to_string(value)
    return str(value)


_LONG_MIN = -(1 << 63)
_LONG_MAX = (1 << 63) - 1
_MASK64 = (1 << 64) - 1


def _digit(ch: str, radix: int) -> int:
    """Java's ``Character.digit(char, radix)`` for radix 10 or 16."""
    code = ord(ch)
    value = -1
    if code > 0xFFFF:
        return -1  # Java sees a surrogate half, which is no digit
    if "0" <= ch <= "9":
        value = code - 0x30
    elif "a" <= ch <= "z":
        value = code - 0x61 + 10
    elif "A" <= ch <= "Z":
        value = code - 0x41 + 10
    elif 0xFF21 <= code <= 0xFF3A:  # fullwidth A-Z
        value = code - 0xFF21 + 10
    elif 0xFF41 <= code <= 0xFF5A:  # fullwidth a-z
        value = code - 0xFF41 + 10
    elif ch.isdecimal():
        value = unicodedata.decimal(ch)
    return value if value < radix else -1


def parse_long(s: str, begin: int = 0, end: int | None = None, radix: int = 10) -> int:
    """Java's ``Long.parseLong(s, begin, end, radix)``, raising
    ``IllegalArgumentError`` (Java's ``NumberFormatException``, an
    ``IllegalArgumentException``) with its message."""
    from .illegal_argument_error import IllegalArgumentError

    if end is None:
        end = len(s)

    def error_at(index: int) -> IllegalArgumentError:
        return IllegalArgumentError(
            message=f'Error at index {index - begin} in: "{s[begin:end]}"')

    if begin == end:
        suffix = "" if radix == 10 else f" under radix {radix}"
        raise IllegalArgumentError(message=f'For input string: ""{suffix}')
    negative = False
    i = begin
    limit = -_LONG_MAX
    first = s[i]
    if first < "0":
        if first == "-":
            negative = True
            limit = _LONG_MIN
        elif first != "+":
            raise error_at(i)
        i += 1
        if i == end:
            raise error_at(i)
    multmin = -(-limit // radix)  # Java's limit / radix, truncated toward zero
    result = 0
    while i < end:
        digit = _digit(s[i], radix)
        if digit < 0 or result < multmin:
            raise error_at(i)
        result *= radix
        if result < limit + digit:
            raise error_at(i)
        i += 1
        result -= digit
    return result if negative else -result


def parse_long_string(s: str) -> int:
    """Java's ``Long.parseLong(String)``: the whole string, and
    ``NumberFormatException.forInputString``'s message (``For input string:
    "…"``), where the ``CharSequence`` overload (:func:`parse_long`) reports
    the index."""
    from .illegal_argument_error import IllegalArgumentError

    try:
        return parse_long(s)
    except IllegalArgumentError:
        raise IllegalArgumentError(message=f'For input string: "{s}"') from None


def parse_int(s: str, bits: int = 32) -> int:
    """Java's ``Integer.parseInt(String)`` (``bits=32``), with
    ``forInputString``'s message for bad text or a value out of range, or
    ``Short.parseShort(String)`` (``bits=16``), which parses an ``int`` and
    reports a value out of the ``short`` range as ``Value out of range.
    Value:"…" Radix:10``."""
    from .illegal_argument_error import IllegalArgumentError

    value = parse_long_string(s)
    if not -(1 << 31) <= value < (1 << 31):
        raise IllegalArgumentError(message=f'For input string: "{s}"')
    if bits == 16 and not -(1 << 15) <= value < (1 << 15):
        raise IllegalArgumentError(message=f'Value out of range. Value:"{s}" Radix:10')
    return value


_DOUBLE = re.compile(
    r"[+-]?(?:NaN|Infinity|(?:(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?|"
    r"0[xX](?:[0-9a-fA-F]+\.?|[0-9a-fA-F]*\.[0-9a-fA-F]+)[pP][+-]?\d+)[fFdD]?)",
    re.ASCII)


def java_trim(s: str) -> str:
    """Java's ``String.trim()``: strips the characters up to U+0020."""
    start, end = 0, len(s)
    while start < end and s[start] <= " ":
        start += 1
    while end > start and s[end - 1] <= " ":
        end -= 1
    return s[start:end]


def parse_double(s: str) -> float:
    """Java's ``Double.parseDouble(s)``: decimal or hexadecimal, an optional
    ``f`` / ``d`` suffix, ``NaN`` and ``Infinity`` (not Python's ``nan`` /
    ``inf``); raises ``IllegalArgumentError`` (Java's
    ``NumberFormatException``)."""
    from .illegal_argument_error import IllegalArgumentError

    text = java_trim(s)
    if not _DOUBLE.fullmatch(text):
        raise IllegalArgumentError(
            message="empty String" if not text else f'For input string: "{text}"')
    sign = -1.0 if text[0] == "-" else 1.0
    body = text.lstrip("+-")
    if body == "NaN":
        return math.nan
    if body == "Infinity":
        return sign * math.inf
    if body[-1] in "fFdD":
        body = body[:-1]
    if body[:2] in ("0x", "0X"):
        return sign * float.fromhex(body)
    return sign * float(body)


def uuid_from_string(name: str) -> uuid.UUID:
    """``java.util.UUID.fromString(name)``: five dash-separated hexadecimal
    groups, each masked to its width, so ``1-1-1-1-1`` is
    ``00000001-0001-0001-0001-000000000001``. Raises ``IllegalArgumentError``
    with Java's message."""
    from .illegal_argument_error import IllegalArgumentError

    if len(name.encode("utf_16_le", "surrogatepass")) // 2 > 36:
        raise IllegalArgumentError(message="UUID string too large")
    dash1 = name.find("-", 0)
    dash2 = name.find("-", dash1 + 1)
    dash3 = name.find("-", dash2 + 1)
    dash4 = name.find("-", dash3 + 1)
    dash5 = name.find("-", dash4 + 1)
    if dash4 < 0 or dash5 >= 0:
        raise IllegalArgumentError(message="Invalid UUID string: " + name)
    most = parse_long(name, 0, dash1, 16) & 0xFFFFFFFF
    most = (most << 16) | (parse_long(name, dash1 + 1, dash2, 16) & 0xFFFF)
    most = (most << 16) | (parse_long(name, dash2 + 1, dash3, 16) & 0xFFFF)
    least = parse_long(name, dash3 + 1, dash4, 16) & 0xFFFF
    least = (least << 48) | (parse_long(name, dash4 + 1, len(name), 16) & 0xFFFFFFFFFFFF)
    return uuid.UUID(int=((most & _MASK64) << 64) | (least & _MASK64))
