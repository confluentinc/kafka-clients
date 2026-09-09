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

"""``confluent_kafka.common.serialization`` — mirror of
``org.apache.kafka.common.serialization``.

Public surface (spec §5.4):

* the ``Serializer`` / ``Deserializer`` protocols (a serde is any callable of
  the right shape; these are for type checking);
* the optional-lifecycle capability protocols ``Configurable`` / ``Closable``
  and the ``SerdeBase`` no-op base;
* the built-in serde **factories** (``bytes_serializer`` … ``json_deserializer``),
  each returning a typed callable so ``Generic[K, V]`` is inferred;
* the concrete built-in serde classes (``StringSerializer`` … ), for Java-style
  porting and for the config route's dotted-path resolution.

The client-facing lifecycle / supply helpers (``resolve_serde``,
``configure_if_defined``, ``close_if_defined``) are re-exported too; the clients
land in P4/P5.
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
from ._protocols import (
    Closable,
    Configurable,
    Deserializer,
    SerdeBase,
    Serializer,
)
from ._supply import close_if_defined, configure_if_defined, resolve_serde
from .bool_deserializer import BooleanDeserializer
from .bool_serializer import BooleanSerializer
from .byte_array_deserializer import ByteArrayDeserializer
from .byte_array_serializer import ByteArraySerializer
from .byte_buffer_deserializer import ByteBufferDeserializer
from .float_deserializer import FloatDeserializer
from .float_serializer import FloatSerializer
from .int_deserializer import IntDeserializer
from .int_serializer import IntSerializer
from .json_serde import JsonDeserializer, JsonSerializer
from .string_deserializer import StringDeserializer
from .string_serializer import StringSerializer
from .uuid_deserializer import UUIDDeserializer
from .uuid_serializer import UUIDSerializer

__all__ = [
    # protocols / lifecycle
    "Serializer",
    "Deserializer",
    "Configurable",
    "Closable",
    "SerdeBase",
    # factories
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
    # concrete classes
    "ByteArraySerializer",
    "ByteArrayDeserializer",
    "ByteBufferDeserializer",
    "StringSerializer",
    "StringDeserializer",
    "IntSerializer",
    "IntDeserializer",
    "FloatSerializer",
    "FloatDeserializer",
    "BooleanSerializer",
    "BooleanDeserializer",
    "UUIDSerializer",
    "UUIDDeserializer",
    "JsonSerializer",
    "JsonDeserializer",
    # supply helpers (used by the clients in P4/P5)
    "resolve_serde",
    "configure_if_defined",
    "close_if_defined",
]
