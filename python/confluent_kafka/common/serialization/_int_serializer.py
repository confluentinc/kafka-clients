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

"""``int_serializer(size=4)`` / ``int_serializer(size=8)``: Java's
``org.apache.kafka.common.serialization.IntegerSerializer`` /
``LongSerializer``.

Both write a signed big-endian two's-complement integer of a fixed width. A
Python ``int`` outside the width raises ``OverflowError``, where Java's
``Integer`` / ``Long`` cannot hold such a value.
"""

from __future__ import annotations

from confluent_kafka.common.headers import Headers


class IntSerializer:
    """Serializes a signed integer to ``size`` big-endian bytes."""

    __slots__ = ("_size",)

    def __init__(self, size: int = 4) -> None:
        self._size = size

    def __call__(self, topic: str, value: int | None,
                 headers: Headers | None = None) -> bytes | None:
        if value is None:
            return None
        return value.to_bytes(self._size, "big", signed=True)
