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

from typing import Any

from confluent_kafka.common.header.headers import Headers

_TRUE = b"\x01"
_FALSE = b"\x00"


class BooleanSerializer:
    """One byte: ``0x01`` for true, ``0x00`` for false."""

    __slots__ = ()

    def __call__(self, *, topic: str, data: bool | None,
                 headers: Headers | None = None, **kwargs: Any) -> bytes | None:
        return self._fast(topic, data, headers)

    def _fast(self, topic: str, data: bool | None,
              headers: Headers | None = None) -> bytes | None:
        """The positional entry the clients call, without the keyword
        binding and ``**kwargs`` dict of ``__call__`` (private)."""
        if data is None:
            return None
        return _TRUE if data else _FALSE
