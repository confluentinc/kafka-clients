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

"""``string_deserializer()``: Java's ``org.apache.kafka.common.serialization.StringDeserializer``."""

from __future__ import annotations

from confluent_kafka.common.errors.serialization_error import SerializationError
from confluent_kafka.common.headers import Headers

from ._encoding import decode, normalize_encoding


class StringDeserializer:
    """String encoding defaults to UTF8 and can be customized by setting the
    property ``key.deserializer.encoding``, ``value.deserializer.encoding`` or
    ``deserializer.encoding``. The first two take precedence over the last."""

    __slots__ = ("_encoding",)

    def __init__(self, encoding: str = "utf_8") -> None:
        self._encoding = normalize_encoding(encoding)

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        property_name = "key.deserializer.encoding" if is_key else "value.deserializer.encoding"
        encoding_value = configs.get(property_name)
        if encoding_value is None:
            encoding_value = configs.get("deserializer.encoding")
        if isinstance(encoding_value, str):
            self._encoding = normalize_encoding(encoding_value)

    def __call__(self, topic: str, data: memoryview | None,
                 headers: Headers | None = None) -> str | None:
        if data is None:
            return None
        try:
            return decode(data, self._encoding)
        except LookupError as exc:  # the name was validated; kept for safety
            raise SerializationError(message=f"Unsupported encoding {self._encoding}") from exc
