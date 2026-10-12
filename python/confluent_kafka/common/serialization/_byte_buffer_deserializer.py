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

"""``memoryview_deserializer()``: Java's ``org.apache.kafka.common.serialization.ByteBufferDeserializer``.

Returns the record data itself, a ``memoryview`` into the fetch batch, as Java's
``deserialize(topic, headers, ByteBuffer)`` returns its buffer. One held view
pins its whole batch; ``bytes_deserializer()`` copies instead.
"""

from __future__ import annotations

from confluent_kafka.common.headers import Headers


class ByteBufferDeserializer:
    """Returns the record data as the borrowing ``memoryview``."""

    __slots__ = ()

    def __call__(self, topic: str, data: memoryview | None,
                 headers: Headers | None = None) -> memoryview | None:
        return data
