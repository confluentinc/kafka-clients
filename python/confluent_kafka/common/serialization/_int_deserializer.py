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

"""``int_deserializer(size=4)`` / ``int_deserializer(size=8)``: Java's
``org.apache.kafka.common.serialization.IntegerDeserializer`` /
``LongDeserializer``.

A signed big-endian integer, raising ``SerializationError`` with the message of
the Java class the size selects:
``"Size of data received by IntegerDeserializer is not 4"``,
``"Size of data received by LongDeserializer is not 8"``.
"""

from __future__ import annotations

from confluent_kafka.common.errors.serialization_error import SerializationError
from confluent_kafka.common.headers import Headers

_CLASS_NAME_BY_SIZE = {4: "IntegerDeserializer", 8: "LongDeserializer"}


class IntDeserializer:
    """Deserializes ``size`` big-endian bytes to a signed integer."""

    __slots__ = ("_size", "_message")

    def __init__(self, size: int = 4) -> None:
        self._size = size
        self._message = f"Size of data received by {_CLASS_NAME_BY_SIZE[size]} is not {size}"

    def __call__(self, topic: str, data: memoryview | None,
                 headers: Headers | None = None) -> int | None:
        if data is None:
            return None
        if len(data) != self._size:
            raise SerializationError(message=self._message)
        return int.from_bytes(data, "big", signed=True)
