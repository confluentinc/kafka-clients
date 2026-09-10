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

"""``ByteBufferDeserializer`` — the zero-copy opt-in (Java's ``ByteBufferDeserializer``).

Hands out a ``memoryview`` that **borrows the fetch batch** — no copy. The
factory is ``memoryview_deserializer()``.

**Retention rule (D10):** one held view pins its *whole* batch until released.
For an owned copy that does not pin the batch, use ``bytes_deserializer()``
instead. ``None`` maps to ``None``.
"""

from __future__ import annotations

from confluent_kafka.common.headers import Headers


class ByteBufferDeserializer:
    """Returns the record data as a borrowing ``memoryview``; ``None`` -> ``None``."""

    def __call__(
        self,
        topic: str,
        data: memoryview | None,
        headers: Headers | None = None,
    ) -> memoryview | None:
        # Zero-copy: return the borrowed view unchanged. The caller owns the
        # retention consequence (one held view pins its batch).
        return data
