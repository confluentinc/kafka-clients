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

"""``float_serializer(size=8)`` / ``float_serializer(size=4)``: Java's
``org.apache.kafka.common.serialization.DoubleSerializer`` /
``FloatSerializer``.

Big-endian IEEE 754. The two Java serializers treat NaN differently, and so do
these: ``DoubleSerializer`` uses ``Double.doubleToLongBits``, which writes every
NaN as ``0x7ff8000000000000``; ``FloatSerializer`` uses
``Float.floatToRawIntBits``, which keeps a NaN's bits. A Python ``float`` is a
double, so a 32-bit NaN read by ``float_deserializer(size=4)`` carries its
payload in the top bits of the double's mantissa (see ``_float_deserializer``)
and is written back with those bits. A value beyond the 32-bit range is written
as an infinity, as Java's ``(float)`` narrowing gives.
"""

from __future__ import annotations

import math
import struct

from confluent_kafka.common.headers import Headers

# Java Double.doubleToLongBits canonical NaN, big-endian.
_CANONICAL_DOUBLE_NAN = b"\x7f\xf8\x00\x00\x00\x00\x00\x00"
_FLOAT_QUIET_BIT = 0x00400000


def _float_bits(value: float) -> bytes:
    """``Float.floatToRawIntBits((float) value)``, big-endian."""
    if math.isnan(value):
        bits = int.from_bytes(struct.pack(">d", value), "big")
        payload = (bits >> 29) & 0x007FFFFF
        if payload == 0:
            payload = _FLOAT_QUIET_BIT  # a payload only in the low bits narrows to quiet NaN
        return ((bits >> 32) & 0x80000000 | 0x7F800000 | payload).to_bytes(4, "big")
    try:
        return struct.pack(">f", value)
    except OverflowError:
        return struct.pack(">f", math.copysign(math.inf, value))


class FloatSerializer:
    """Serializes a ``float`` to ``size`` big-endian IEEE 754 bytes."""

    __slots__ = ("_size",)

    def __init__(self, size: int = 8) -> None:
        self._size = size

    def __call__(self, topic: str, value: float | None,
                 headers: Headers | None = None) -> bytes | None:
        if value is None:
            return None
        if self._size == 4:
            return _float_bits(value)
        if math.isnan(value):
            return _CANONICAL_DOUBLE_NAN
        return struct.pack(">d", value)
