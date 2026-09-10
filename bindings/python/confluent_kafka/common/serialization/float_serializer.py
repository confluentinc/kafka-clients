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

"""``FloatSerializer`` — collapses Java's ``FloatSerializer`` (size 4) and
``DoubleSerializer`` (size 8).

Big-endian IEEE-754. Java's two serializers are **asymmetric** in their NaN
handling and this class reproduces that exactly:

- **size 4** — ``FloatSerializer.serialize`` uses ``Float.floatToRawIntBits``,
  which preserves the raw NaN bit pattern. ``struct.pack(">f", x)`` is the same
  raw pack, so the bytes match Java for every input a Python ``float`` can hold.
- **size 8** — ``DoubleSerializer.serialize`` uses ``Double.doubleToLongBits``,
  which **canonicalizes** every NaN to ``0x7ff8000000000000`` (unlike the *raw*
  ``doubleToRawLongBits``). ``struct.pack(">d", x)`` is a raw pack and would emit
  a non-canonical double NaN verbatim, so NaN is routed through the canonical
  constant to match Java's wire bytes for every input.

(For float32, a Python ``float`` is a C double and cannot carry a signaling
float32-NaN payload — see clarification C17. For float64 the value *is* a C
double, so a non-canonical double NaN is representable and must be canonicalized
to stay Java-faithful — clarification C20.) ``None`` maps to ``None``.
"""

from __future__ import annotations

import math
import struct

from confluent_kafka.common.headers import Headers

_FORMAT_BY_SIZE = {4: ">f", 8: ">d"}

# Java Double.doubleToLongBits canonical NaN, big-endian.
_CANONICAL_DOUBLE_NAN = b"\x7f\xf8\x00\x00\x00\x00\x00\x00"


class FloatSerializer:
    """Serializes a ``float`` to ``size`` big-endian IEEE-754 bytes."""

    def __init__(self, size: int = 8) -> None:
        self._size = size
        self._format = _FORMAT_BY_SIZE[size]

    def __call__(
        self,
        topic: str,
        value: float | None,
        headers: Headers | None = None,
    ) -> bytes | None:
        if value is None:
            return None
        if self._size == 8 and math.isnan(value):
            # Java DoubleSerializer uses doubleToLongBits, which canonicalizes
            # every NaN to 0x7ff8000000000000 (F2 / C20). struct.pack(">d") is a
            # raw pack and would leak a non-canonical NaN payload.
            return _CANONICAL_DOUBLE_NAN
        return struct.pack(self._format, value)
