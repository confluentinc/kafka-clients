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

"""``BooleanDeserializer`` — Java's ``org.apache.kafka.common.serialization.BooleanDeserializer``.

Requires exactly one byte, ``0x01`` (true) or ``0x00`` (false); anything else
raises ``SerializationException``. ``None`` maps to ``None``.
"""

from __future__ import annotations

from confluent_kafka.common.errors._generated import SerializationError
from confluent_kafka.common.headers import Headers

_TRUE = 0x01
_FALSE = 0x00


class BooleanDeserializer:
    """Deserializes a single byte to a ``bool``; ``None`` maps to ``None``."""

    def __call__(
        self,
        topic: str,
        data: memoryview | None,
        headers: Headers | None = None,
    ) -> bool | None:
        if data is None:
            return None
        if len(data) != 1:
            raise SerializationError(
                "Size of data received by BooleanDeserializer is not 1"
            )
        b = data[0]
        if b == _TRUE:
            return True
        if b == _FALSE:
            return False
        # Java's byte is signed; its message prints the signed value (-1 for
        # 0xFF). Convert so the text matches Java exactly.
        signed = b - 256 if b > 127 else b
        raise SerializationError(
            f"Unexpected byte received by BooleanDeserializer: {signed}"
        )
