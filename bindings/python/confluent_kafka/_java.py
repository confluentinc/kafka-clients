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

"""JDK behaviour the package reproduces: ``String.valueOf`` and
``UUID.fromString``.

The Java ``toString()`` methods the package translates to ``__str__`` and the
messages its errors build concatenate values with ``+``, which calls
``String.valueOf``. Python's ``str`` renders the same values differently
(``None`` / ``null``, ``True`` / ``true``, ``{'a': 1}`` / ``{a=1}``,
``1e+20`` / ``1.0E20``), so every such concatenation goes through
:func:`java_str`. :func:`uuid_from_string` is ``java.util.UUID.fromString``,
which the UUID deserializer calls and which accepts more than Python's
``uuid.UUID`` parser (and rejects some of what it accepts).
"""

from __future__ import annotations

import math
import unicodedata
import uuid
from collections.abc import Mapping
from decimal import Decimal

__all__ = ["java_str", "uuid_from_string"]


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


def _digit16(ch: str) -> int:
    """Java's ``Character.digit(char, 16)``."""
    code = ord(ch)
    if code > 0xFFFF:
        return -1  # Java sees a surrogate half, which is no digit
    if "0" <= ch <= "9":
        return code - 0x30
    if "a" <= ch <= "f":
        return code - 0x61 + 10
    if "A" <= ch <= "F":
        return code - 0x41 + 10
    if 0xFF21 <= code <= 0xFF26:  # fullwidth A-F
        return code - 0xFF21 + 10
    if 0xFF41 <= code <= 0xFF46:  # fullwidth a-f
        return code - 0xFF41 + 10
    if ch.isdecimal():
        return unicodedata.decimal(ch)
    return -1


def _parse_long16(s: str, begin: int, end: int) -> int:
    """Java's ``Long.parseLong(s, begin, end, 16)``, raising
    ``IllegalArgumentError`` with ``NumberFormatException``'s message."""
    from .illegal_argument_error import IllegalArgumentError

    def error_at(index: int) -> IllegalArgumentError:
        return IllegalArgumentError(
            message=f'Error at index {index - begin} in: "{s[begin:end]}"')

    if begin == end:
        raise IllegalArgumentError(message='For input string: "" under radix 16')
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
    multmin = -(-limit // 16)  # Java's limit / radix, truncated toward zero
    result = 0
    while i < end:
        digit = _digit16(s[i])
        if digit < 0 or result < multmin:
            raise error_at(i)
        result *= 16
        if result < limit + digit:
            raise error_at(i)
        i += 1
        result -= digit
    return result if negative else -result


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
    most = _parse_long16(name, 0, dash1) & 0xFFFFFFFF
    most = (most << 16) | (_parse_long16(name, dash1 + 1, dash2) & 0xFFFF)
    most = (most << 16) | (_parse_long16(name, dash2 + 1, dash3) & 0xFFFF)
    least = _parse_long16(name, dash3 + 1, dash4) & 0xFFFF
    least = (least << 48) | (_parse_long16(name, dash4 + 1, len(name)) & 0xFFFFFFFFFFFF)
    return uuid.UUID(int=((most & _MASK64) << 64) | (least & _MASK64))
