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

"""``UUIDDeserializer`` — Java's ``org.apache.kafka.common.serialization.UUIDDeserializer``.

Java decodes bytes to a string, then ``UUID.fromString``; encoding defaults to
UTF-8 and is customizable via ``key.deserializer.encoding`` /
``value.deserializer.encoding`` / ``deserializer.encoding``. An unparseable
string raises ``SerializationException("Error parsing data into UUID")`` — Java
wraps the ``IllegalArgumentException`` from ``UUID.fromString``.

Uses this binding's ``confluent_kafka.common.Uuid`` (base64 string form) rather
than ``java.util.UUID`` — see clarification C15.
"""

from __future__ import annotations

from confluent_kafka.common.errors._generated import SerializationError
from confluent_kafka.common.headers import Headers
from confluent_kafka.common.uuid import Uuid

from confluent_kafka import IllegalArgumentError


class UUIDDeserializer:
    """Deserializes bytes to a ``Uuid`` via its string form; ``None`` -> ``None``."""

    def __init__(self, encoding: str = "utf_8") -> None:
        self._encoding = encoding

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        property_name = (
            "key.deserializer.encoding" if is_key else "value.deserializer.encoding"
        )
        encoding_value = configs.get(property_name)
        if encoding_value is None:
            encoding_value = configs.get("deserializer.encoding")
        if isinstance(encoding_value, str):
            self._encoding = encoding_value

    def __call__(
        self,
        topic: str,
        data: memoryview | None,
        headers: Headers | None = None,
    ) -> Uuid | None:
        if data is None:
            return None
        try:
            text = bytes(data).decode(self._encoding)
        except LookupError as exc:
            raise SerializationError(
                "Error when deserializing byte[] to UUID due to unsupported "
                f"encoding {self._encoding}"
            ) from exc
        try:
            return Uuid.from_string(s=text)
        except IllegalArgumentError as exc:
            # Java wraps UUID.fromString's IllegalArgumentException.
            raise SerializationError("Error parsing data into UUID") from exc
