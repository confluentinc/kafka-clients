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

"""``confluent_kafka.common.serialization``: Java's
``org.apache.kafka.common.serialization``.

The ``Serializer`` / ``Deserializer`` protocols (a serde is any callable of
their shape), the ``Configurable`` / ``Closeable`` lifecycle protocols, the
no-op ``SerdeBase``, and the built-in factories, each matching a Java serde
byte for byte (CLAUDE.md, Python Binding Conventions, Serialization). The
built-in serde classes are private: a factory is the way to get one.
"""

from __future__ import annotations

from ._factories import (
    bool_deserializer,
    bool_serializer,
    bytes_deserializer,
    bytes_serializer,
    float_deserializer,
    float_serializer,
    int_deserializer,
    int_serializer,
    json_deserializer,
    json_serializer,
    memoryview_deserializer,
    string_deserializer,
    string_serializer,
    uuid_deserializer,
    uuid_serializer,
)
from ._protocols import Closeable, Configurable, Deserializer, SerdeBase, Serializer

__all__ = [
    "Serializer",
    "Deserializer",
    "Configurable",
    "Closeable",
    "SerdeBase",
    "bytes_serializer",
    "bytes_deserializer",
    "memoryview_deserializer",
    "string_serializer",
    "string_deserializer",
    "int_serializer",
    "int_deserializer",
    "float_serializer",
    "float_deserializer",
    "bool_serializer",
    "bool_deserializer",
    "uuid_serializer",
    "uuid_deserializer",
    "json_serializer",
    "json_deserializer",
]
