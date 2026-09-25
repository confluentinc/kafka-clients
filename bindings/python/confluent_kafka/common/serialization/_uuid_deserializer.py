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

"""``uuid_deserializer()``: Java's ``org.apache.kafka.common.serialization.UUIDDeserializer``.

The record data is a ``memoryview``, so the messages are those of Java's
``deserialize(topic, headers, ByteBuffer)``, the overload Java's consumer calls.
The text is parsed as ``java.util.UUID.fromString`` parses it.
"""

from __future__ import annotations

import uuid

from confluent_kafka._java import uuid_from_string
from confluent_kafka.common.errors.serialization_error import SerializationError
from confluent_kafka.common.headers import Headers
from confluent_kafka.illegal_argument_error import IllegalArgumentError

from ._encoding import decode


class UUIDDeserializer:
    """We are converting the byte array to String before deserializing to
    UUID. String encoding defaults to UTF8 and can be customized by setting the
    property ``key.deserializer.encoding``, ``value.deserializer.encoding`` or
    ``deserializer.encoding``. The first two take precedence over the last."""

    __slots__ = ("_encoding",)

    def __init__(self) -> None:
        self._encoding = "UTF-8"

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        property_name = "key.deserializer.encoding" if is_key else "value.deserializer.encoding"
        encoding_value = configs.get(property_name)
        if encoding_value is None:
            encoding_value = configs.get("deserializer.encoding")
        if isinstance(encoding_value, str):
            self._encoding = encoding_value

    def __call__(self, topic: str, data: memoryview | None,
                 headers: Headers | None = None) -> uuid.UUID | None:
        if data is None:
            return None
        try:
            text = decode(data, self._encoding)
        except LookupError as exc:
            raise SerializationError(
                message="Error when deserializing ByteBuffer to UUID due to unsupported "
                "encoding " + self._encoding) from exc
        try:
            return uuid_from_string(text)
        except IllegalArgumentError as exc:
            raise SerializationError(message="Error parsing data into UUID") from exc
