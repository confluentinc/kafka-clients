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

"""``uuid_serializer()``: Java's ``org.apache.kafka.common.serialization.UUIDSerializer``.

Java's ``java.util.UUID`` is Python's ``uuid.UUID`` (CLAUDE.md, Python Binding
Conventions, Types); both render the lowercase dashed form, so the bytes match
Java's byte for byte.
"""

from __future__ import annotations

import uuid

from confluent_kafka.common.errors.serialization_error import SerializationError
from confluent_kafka.common.headers import Headers

from ._encoding import encode


class UUIDSerializer:
    """We are converting UUID to String before serializing. The encoding
    defaults to UTF8 and can be customized by setting the property
    ``key.serializer.encoding``, ``value.serializer.encoding`` or
    ``serializer.encoding``. The first two take precedence over the last."""

    __slots__ = ("_encoding",)

    def __init__(self) -> None:
        self._encoding = "UTF-8"

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        property_name = "key.serializer.encoding" if is_key else "value.serializer.encoding"
        encoding_value = configs.get(property_name)
        if encoding_value is None:
            encoding_value = configs.get("serializer.encoding")
        if isinstance(encoding_value, str):
            self._encoding = encoding_value

    def __call__(self, topic: str, value: uuid.UUID | None,
                 headers: Headers | None = None) -> bytes | None:
        if value is None:
            return None
        try:
            return encode(str(value), self._encoding)
        except LookupError:
            # Java passes no cause.
            raise SerializationError(
                message="Error when serializing UUID to byte[] due to unsupported encoding "
                + self._encoding) from None
