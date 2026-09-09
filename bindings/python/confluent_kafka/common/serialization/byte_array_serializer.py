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

"""``ByteArraySerializer`` — Java's ``org.apache.kafka.common.serialization.ByteArraySerializer``.

Passthrough: the bytes go to the wire unchanged. ``None`` maps to ``None``
(Java returns ``data``, which is ``null`` for ``null``). This is the producer
default (D10).
"""

from __future__ import annotations

from confluent_kafka.common.headers import Headers


class ByteArraySerializer:
    """Passes bytes through unchanged; ``None`` maps to ``None``."""

    def __call__(
        self,
        topic: str,
        value: bytes | None,
        headers: Headers | None = None,
    ) -> bytes | None:
        return value
