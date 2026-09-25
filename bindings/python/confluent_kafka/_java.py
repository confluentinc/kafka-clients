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

"""Java's string conversion (``String.valueOf``) for ``__str__`` and messages.

The Java ``toString()`` methods the package translates to ``__str__`` and the
messages its errors build concatenate values with ``+``, which calls
``String.valueOf``. Python's ``str`` renders the same values differently
(``None`` / ``null``, ``True`` / ``true``, ``{'a': 1}`` / ``{a=1}``,
``1e+20`` / ``1.0E20``), so every such concatenation goes through
:func:`java_str`.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal

__all__ = ["java_str"]


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
