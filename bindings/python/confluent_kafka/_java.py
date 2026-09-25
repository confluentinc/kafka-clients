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
import unicodedata
import uuid
from collections.abc import Mapping
from decimal import Decimal

__all__ = ["java_str", "java_trim", "parse_double", "parse_int", "parse_long", "uuid_from_string"]


def _double_to_string(d: float) -> str:
    """Java's ``Double.toString``."""
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
    _, digits, exponent = Decimal(repr(a)).normalize().as_tuple()
    text = "".join(str(x) for x in digits)
    exp10 = len(text) - 1 + int(exponent)
    mantissa = text[0] + "." + (text[1:] or "0")
    return ("-" if d < 0 else "") + f"{mantissa}E{exp10}"


def java_str(value: object) -> str:
    """``String.valueOf(value)``: the text Java's string concatenation gives.

    ``None`` is ``null``, a ``bool`` is ``true`` / ``false``, a float follows
    ``Double.toString``, a collection is ``[a, b]`` and a mapping ``{k=v}`` with
    their elements converted the same way; everything else is ``str(value)``,
    which for the package's own types is Java's ``toString()``.
    """
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
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


def parse_int(s: str, bits: int = 32) -> int:
    """Java's ``Integer.parseInt(s)`` (``bits=32``) or ``Short.parseShort(s)``
    (``bits=16``): ``Long.parseLong`` limited to the width."""
    from .illegal_argument_error import IllegalArgumentError

    value = parse_long(s)
    if not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
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
