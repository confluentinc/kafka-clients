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

"""``ByteArrayDeserializer`` — Java's ``org.apache.kafka.common.serialization.ByteArrayDeserializer``.

Returns an **owned ``bytes`` copy** of the record data — the consumer default
(D10). On the receive path ``data`` is a ``memoryview`` borrowing the fetch
batch; ``bytes(data)`` copies it out so the returned object does not pin the
batch. Zero-copy is the explicit ``memoryview_deserializer()`` opt-in instead.
``None`` maps to ``None``.
"""

from __future__ import annotations

from confluent_kafka.common.headers import Headers


class ByteArrayDeserializer:
    """Copies the record data into owned ``bytes``; ``None`` maps to ``None``."""

    def __call__(
        self,
        topic: str,
        data: memoryview | None,
        headers: Headers | None = None,
    ) -> bytes | None:
        if data is None:
            return None
        # Owned copy: the returned bytes must not borrow the fetch batch.
        return bytes(data)
