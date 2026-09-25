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

"""``bool_serializer()``: Java's ``org.apache.kafka.common.serialization.BooleanSerializer``."""

from __future__ import annotations

from confluent_kafka.common.headers import Headers

_TRUE = b"\x01"
_FALSE = b"\x00"


class BooleanSerializer:
    """One byte: ``0x01`` for true, ``0x00`` for false."""

    __slots__ = ()

    def __call__(self, topic: str, value: bool | None,
                 headers: Headers | None = None) -> bytes | None:
        if value is None:
            return None
        return _TRUE if value else _FALSE
