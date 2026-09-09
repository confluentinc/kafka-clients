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

"""``IntDeserializer`` — collapses Java's ``IntegerDeserializer`` (size 4) and
``LongDeserializer`` (size 8).

Both read a signed big-endian integer of a fixed width and raise
``SerializationException("Size of data received by <Cls> is not <n>")`` when the
byte count does not match. This one class reproduces that per-size, keying the
message to the Java class the size selects so the message text matches Java
exactly.
"""

from __future__ import annotations

from confluent_kafka.common.errors._generated import SerializationError
from confluent_kafka.common.headers import Headers

# Java's per-class exception text, keyed by width so the collapsed class matches
# whichever Java deserializer the ``size`` selects.
_CLASS_NAME_BY_SIZE = {4: "IntegerDeserializer", 8: "LongDeserializer"}


class IntDeserializer:
    """Deserializes ``size`` big-endian bytes to a signed integer."""

    def __init__(self, size: int = 4) -> None:
        self._size = size

    def __call__(
        self,
        topic: str,
        data: memoryview | None,
        headers: Headers | None = None,
    ) -> int | None:
        if data is None:
            return None
        if len(data) != self._size:
            cls = _CLASS_NAME_BY_SIZE.get(self._size, "IntegerDeserializer")
            raise SerializationError(
                f"Size of data received by {cls} is not {self._size}"
            )
        return int.from_bytes(bytes(data), "big", signed=True)
