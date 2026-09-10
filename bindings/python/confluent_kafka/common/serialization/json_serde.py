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

"""JSON serde — a Python-natural addition with no Java counterpart.

Serializes via ``json.dumps(...).encode("utf_8")`` and deserializes via
``json.loads(bytes(data))``. A serde/deserde error is wrapped in
``SerializationError`` (its ``__cause__`` is the original ``json`` /
``UnicodeDecodeError``), matching how the Kafka serdes surface failures.
``None`` maps to ``None`` (Java's tombstone contract).
"""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka.common.errors._generated import SerializationError
from confluent_kafka.common.headers import Headers


class JsonSerializer:
    """Serializes any JSON-encodable object to UTF-8 JSON bytes."""

    def __call__(
        self,
        topic: str,
        value: Any | None,
        headers: Headers | None = None,
    ) -> bytes | None:
        if value is None:
            return None
        try:
            return json.dumps(value).encode("utf_8")
        except (TypeError, ValueError) as exc:
            raise SerializationError(
                "Error serializing value to JSON"
            ) from exc


class JsonDeserializer:
    """Deserializes UTF-8 JSON bytes to a Python object."""

    def __call__(
        self,
        topic: str,
        data: memoryview | None,
        headers: Headers | None = None,
    ) -> Any | None:
        if data is None:
            return None
        try:
            return json.loads(bytes(data))
        except (ValueError, UnicodeDecodeError) as exc:
            raise SerializationError(
                "Error deserializing JSON value"
            ) from exc
