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

"""``IntSerializer`` — collapses Java's ``IntegerSerializer`` (size 4) and
``LongSerializer`` (size 8).

Both Java serializers emit big-endian two's-complement bytes of a signed fixed
width (``IntegerSerializer`` 32-bit, ``LongSerializer`` 64-bit). One
``size``-parameterized class covers both; the ``int_serializer(*, size=4)``
factory picks the width. ``None`` maps to ``None`` (Java's tombstone).
"""

from __future__ import annotations

from confluent_kafka.common.headers import Headers


class IntSerializer:
    """Serializes a signed integer to ``size`` big-endian bytes."""

    def __init__(self, size: int = 4) -> None:
        self._size = size

    def __call__(
        self,
        topic: str,
        value: int | None,
        headers: Headers | None = None,
    ) -> bytes | None:
        if value is None:
            return None
        # Big-endian, signed two's complement — matches Java's byte-shift emit
        # (``(byte)(data >>> 24)`` … ) which is a signed big-endian encoding.
        return int(value).to_bytes(self._size, "big", signed=True)
