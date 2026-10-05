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

"""``float_deserializer(size=8)`` / ``float_deserializer(size=4)``: Java's
``org.apache.kafka.common.serialization.DoubleDeserializer`` /
``FloatDeserializer``.

A big-endian IEEE 754 value, raw bits kept. A 32-bit NaN keeps its payload in
the top bits of the double's mantissa, where the hardware conversion would set
the quiet bit, so ``float_serializer(size=4)`` writes back the bits Java's
``Float`` holds. The record data is a ``memoryview``, so the messages are those
of Java's ``deserialize(topic, headers, ByteBuffer)``, the overload Java's
consumer calls: ``"Size of data received by DoubleDeserializer is not 8"``,
``"Size of data received by Deserializer is not 4"``.
"""

from __future__ import annotations

import struct

from confluent_kafka.common.errors.serialization_error import SerializationError
from confluent_kafka.common.headers import Headers

_CLASS_NAME_BY_SIZE = {4: "Deserializer", 8: "DoubleDeserializer"}


def _from_float_bits(data: memoryview) -> float:
    """``Float.intBitsToFloat``, widened to a double without quieting a NaN."""
    bits = int.from_bytes(data, "big")
    if bits & 0x7F800000 == 0x7F800000 and bits & 0x007FFFFF:
        wide = (bits & 0x80000000) << 32 | 0x7FF << 52 | (bits & 0x007FFFFF) << 29
        nan: float = struct.unpack(">d", wide.to_bytes(8, "big"))[0]
        return nan
    value: float = struct.unpack(">f", data)[0]
    return value


class FloatDeserializer:
    """Deserializes ``size`` big-endian IEEE 754 bytes to a ``float``."""

    __slots__ = ("_size", "_message")

    def __init__(self, size: int = 8) -> None:
        self._size = size
        self._message = f"Size of data received by {_CLASS_NAME_BY_SIZE[size]} is not {size}"

    def __call__(self, topic: str, data: memoryview | None,
                 headers: Headers | None = None) -> float | None:
        if data is None:
            return None
        if len(data) != self._size:
            raise SerializationError(message=self._message)
        if self._size == 4:
            return _from_float_bits(data)
        value: float = struct.unpack(">d", data)[0]
        return value
