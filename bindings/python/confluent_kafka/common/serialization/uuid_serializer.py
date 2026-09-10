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

"""``UUIDSerializer`` — Java's ``org.apache.kafka.common.serialization.UUIDSerializer``.

Java converts a ``UUID`` to its string form (``UUID.toString()``) and serializes
those bytes, defaulting to UTF-8, customizable via ``key.serializer.encoding`` /
``value.serializer.encoding`` / ``serializer.encoding``.

Java's serde handles ``java.util.UUID``; this binding's UUID type is
``confluent_kafka.common.Uuid`` (``org.apache.kafka.common.Uuid``), whose string
form is a URL-safe base64 encoding rather than ``java.util.UUID``'s dashed form.
The *mechanism* is faithful (serialize the string form, parse it back on the
deserializer); the *wire bytes* are the base64 string, not the dashed string —
see clarification C15.
"""

from __future__ import annotations

from confluent_kafka.common.errors._generated import SerializationError
from confluent_kafka.common.headers import Headers
from confluent_kafka.common.uuid import Uuid


class UUIDSerializer:
    """Serializes a ``Uuid`` via its string form; ``None`` maps to ``None``."""

    def __init__(self, encoding: str = "utf_8") -> None:
        self._encoding = encoding

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        property_name = (
            "key.serializer.encoding" if is_key else "value.serializer.encoding"
        )
        encoding_value = configs.get(property_name)
        if encoding_value is None:
            encoding_value = configs.get("serializer.encoding")
        if isinstance(encoding_value, str):
            # Java's UUIDSerializer does NOT validate the charset in configure()
            # (unlike StringSerializer) — it stores the name and fails at
            # serialize time. Preserve that: store as-is.
            self._encoding = encoding_value

    def __call__(
        self,
        topic: str,
        value: Uuid | None,
        headers: Headers | None = None,
    ) -> bytes | None:
        if value is None:
            return None
        try:
            return str(value).encode(self._encoding)
        except LookupError as exc:
            raise SerializationError(
                "Error when serializing UUID to byte[] due to unsupported "
                f"encoding {self._encoding}"
            ) from exc
