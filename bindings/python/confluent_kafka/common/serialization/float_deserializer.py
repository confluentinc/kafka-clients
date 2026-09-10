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

"""``FloatDeserializer`` — collapses Java's ``FloatDeserializer`` (size 4) and
``DoubleDeserializer`` (size 8).

Reads a big-endian IEEE-754 value and raises
``SerializationException("Size of data received by Deserializer is not <n>")``
when the byte count does not match — the exact text of both Java array-path
deserializers. ``None`` maps to ``None``.
"""

from __future__ import annotations

import struct

from confluent_kafka.common.errors._generated import SerializationError
from confluent_kafka.common.headers import Headers

_FORMAT_BY_SIZE = {4: ">f", 8: ">d"}


class FloatDeserializer:
    """Deserializes ``size`` big-endian IEEE-754 bytes to a ``float``."""

    def __init__(self, size: int = 8) -> None:
        self._size = size
        self._format = _FORMAT_BY_SIZE[size]

    def __call__(
        self,
        topic: str,
        data: memoryview | None,
        headers: Headers | None = None,
    ) -> float | None:
        if data is None:
            return None
        if len(data) != self._size:
            # Both Java array-path deserializers use the literal "Deserializer".
            raise SerializationError(
                f"Size of data received by Deserializer is not {self._size}"
            )
        value: float = struct.unpack(self._format, bytes(data))[0]
        return value
