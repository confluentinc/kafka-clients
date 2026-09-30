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

"""``bytes_deserializer()``: Java's ``org.apache.kafka.common.serialization.ByteArrayDeserializer``.

Java hands out the record's own array; the record data here is a ``memoryview``
into the fetch batch, so an owned ``bytes`` copy is made when the deserializer
runs, and the result does not pin the batch (``memoryview_deserializer()`` is
the view). The consumer default.
"""

from __future__ import annotations

from confluent_kafka.common.headers import Headers


class ByteArrayDeserializer:
    """Copies the record data into owned ``bytes``."""

    __slots__ = ()

    def __call__(self, topic: str, data: memoryview | None,
                 headers: Headers | None = None) -> bytes | None:
        if data is None:
            return None
        return bytes(data)
