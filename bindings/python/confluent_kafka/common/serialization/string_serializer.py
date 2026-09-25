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

"""``StringSerializer`` — Java's ``org.apache.kafka.common.serialization.StringSerializer``.

String encoding defaults to UTF-8 and can be customized either by the
``string_serializer(*, encoding=...)`` factory argument (the kwarg route, where
Java uses a constructor) or, on the config route, by setting the property
``key.serializer.encoding``, ``value.serializer.encoding`` or
``serializer.encoding`` — the first two take precedence over the last, exactly
as Java's ``configure`` does.
"""

from __future__ import annotations

from confluent_kafka.common.errors._generated import SerializationError
from confluent_kafka.common.headers import Headers

from ._encoding import normalize_encoding


class StringSerializer:
    """Serializes a ``str`` to its encoded bytes; ``None`` maps to ``None``."""

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
            # Java validates the charset name in configure() and raises
            # SerializationException on an unknown one — do the same here.
            self._encoding = normalize_encoding(encoding_value)

    def __call__(
        self,
        topic: str,
        value: str | None,
        headers: Headers | None = None,
    ) -> bytes | None:
        if value is None:
            return None
        try:
            return value.encode(self._encoding)
        except LookupError as exc:
            raise SerializationError(
                f"Unsupported encoding {self._encoding}"
            ) from exc
