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

"""``FloatSerializer`` — collapses Java's ``FloatSerializer`` (size 4,
``Float.floatToRawIntBits``) and ``DoubleSerializer`` (size 8,
``Double.doubleToLongBits``).

Big-endian IEEE-754. ``struct``'s ``>f`` / ``>d`` use ``floatToRawIntBits`` /
``doubleToLongBits`` semantics — raw bit patterns, so NaN payloads round-trip
(Java's ``floatSerdeShouldPreserveNaNValues``). ``None`` maps to ``None``.
"""

from __future__ import annotations

import struct

from confluent_kafka.common.headers import Headers

_FORMAT_BY_SIZE = {4: ">f", 8: ">d"}


class FloatSerializer:
    """Serializes a ``float`` to ``size`` big-endian IEEE-754 bytes."""

    def __init__(self, size: int = 8) -> None:
        self._format = _FORMAT_BY_SIZE[size]

    def __call__(
        self,
        topic: str,
        value: float | None,
        headers: Headers | None = None,
    ) -> bytes | None:
        if value is None:
            return None
        return struct.pack(self._format, value)
