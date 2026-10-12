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

from ._factories import bool_deserializer as bool_deserializer
from ._factories import bool_serializer as bool_serializer
from ._factories import bytes_deserializer as bytes_deserializer
from ._factories import bytes_serializer as bytes_serializer
from ._factories import float_deserializer as float_deserializer
from ._factories import float_serializer as float_serializer
from ._factories import int_deserializer as int_deserializer
from ._factories import int_serializer as int_serializer
from ._factories import json_deserializer as json_deserializer
from ._factories import json_serializer as json_serializer
from ._factories import memoryview_deserializer as memoryview_deserializer
from ._factories import string_deserializer as string_deserializer
from ._factories import string_serializer as string_serializer
from ._factories import uuid_deserializer as uuid_deserializer
from ._factories import uuid_serializer as uuid_serializer
from .closeable import Closeable as Closeable
from .configurable import Configurable as Configurable
from .deserializer import Deserializer as Deserializer
from .serde_base import SerdeBase as SerdeBase
from .serializer import Serializer as Serializer

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
