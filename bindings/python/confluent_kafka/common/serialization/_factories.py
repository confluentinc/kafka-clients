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

"""The built-in serde factories *(deviation)*.

Each factory returns a typed Python callable matching Java's serde byte for
byte (CLAUDE.md, Python Binding Conventions, Serialization), so a client's key
and value types are inferred from it. Another ``size`` raises
``IllegalArgumentError``.
"""

from __future__ import annotations

import uuid
from typing import Any

from confluent_kafka.illegal_argument_error import IllegalArgumentError

from ._bool_deserializer import BooleanDeserializer
from ._bool_serializer import BooleanSerializer
from ._byte_array_deserializer import ByteArrayDeserializer
from ._byte_array_serializer import ByteArraySerializer
from ._byte_buffer_deserializer import ByteBufferDeserializer
from ._float_deserializer import FloatDeserializer
from ._float_serializer import FloatSerializer
from ._int_deserializer import IntDeserializer
from ._int_serializer import IntSerializer
from ._json_serde import JsonDeserializer, JsonSerializer
from ._string_deserializer import StringDeserializer
from ._string_serializer import StringSerializer
from ._uuid_deserializer import UUIDDeserializer
from ._uuid_serializer import UUIDSerializer
from .deserializer import Deserializer
from .serializer import Serializer

__all__ = [
    "bool_deserializer", "bool_serializer", "bytes_deserializer", "bytes_serializer",
    "float_deserializer", "float_serializer", "int_deserializer", "int_serializer",
    "json_deserializer", "json_serializer", "memoryview_deserializer",
    "string_deserializer", "string_serializer", "uuid_deserializer", "uuid_serializer",
]

_INT_SIZES = (4, 8)
_FLOAT_SIZES = (8, 4)


def _check_size(factory: str, size: int, sizes: tuple[int, int]) -> None:
    if size not in sizes:
        raise IllegalArgumentError(
            message=f"{factory}() size must be {sizes[0]} or {sizes[1]}; got {size}")


def bytes_serializer() -> Serializer[bytes]:
    """The bytes as they are: Java's ``ByteArraySerializer``, the producer
    default."""
    return ByteArraySerializer()


def bytes_deserializer() -> Deserializer[bytes]:
    """An owned copy of the record data: Java's ``ByteArrayDeserializer``, the
    consumer default."""
    return ByteArrayDeserializer()


def memoryview_deserializer() -> Deserializer[memoryview]:
    """A view into the fetch batch, which one held view pins whole: Java's
    ``ByteBufferDeserializer``."""
    return ByteBufferDeserializer()


def string_serializer(*, encoding: str = "utf_8") -> Serializer[str]:
    """A string in ``encoding``: Java's ``StringSerializer``. An unknown
    encoding raises ``SerializationError``."""
    return StringSerializer(encoding)


def string_deserializer(*, encoding: str = "utf_8") -> Deserializer[str]:
    """A string in ``encoding``: Java's ``StringDeserializer``. An unknown
    encoding raises ``SerializationError``."""
    return StringDeserializer(encoding)


def int_serializer(*, size: int = 4) -> Serializer[int]:
    """A signed big-endian integer of ``size`` bytes, 4 or 8: Java's
    ``IntegerSerializer`` / ``LongSerializer``."""
    _check_size("int_serializer", size, _INT_SIZES)
    return IntSerializer(size)


def int_deserializer(*, size: int = 4) -> Deserializer[int]:
    """A signed big-endian integer of ``size`` bytes, 4 or 8: Java's
    ``IntegerDeserializer`` / ``LongDeserializer``."""
    _check_size("int_deserializer", size, _INT_SIZES)
    return IntDeserializer(size)


def float_serializer(*, size: int = 8) -> Serializer[float]:
    """A big-endian IEEE 754 value of ``size`` bytes, 8 or 4: Java's
    ``DoubleSerializer`` (every NaN written as ``doubleToLongBits`` writes it)
    / ``FloatSerializer`` (raw bits)."""
    _check_size("float_serializer", size, _FLOAT_SIZES)
    return FloatSerializer(size)


def float_deserializer(*, size: int = 8) -> Deserializer[float]:
    """A big-endian IEEE 754 value of ``size`` bytes, 8 or 4: Java's
    ``DoubleDeserializer`` / ``FloatDeserializer``."""
    _check_size("float_deserializer", size, _FLOAT_SIZES)
    return FloatDeserializer(size)


def bool_serializer() -> Serializer[bool]:
    """One byte, ``0x01`` or ``0x00``: Java's ``BooleanSerializer``."""
    return BooleanSerializer()


def bool_deserializer() -> Deserializer[bool]:
    """One byte, ``0x01`` or ``0x00``: Java's ``BooleanDeserializer``."""
    return BooleanDeserializer()


def uuid_serializer() -> Serializer[uuid.UUID]:
    """A ``uuid.UUID`` in Java's dashed form, UTF-8: Java's
    ``UUIDSerializer``."""
    return UUIDSerializer()


def uuid_deserializer() -> Deserializer[uuid.UUID]:
    """A ``uuid.UUID`` parsed from its string form as
    ``java.util.UUID.fromString`` parses it, UTF-8: Java's
    ``UUIDDeserializer``."""
    return UUIDDeserializer()


def json_serializer() -> Serializer[Any]:
    """UTF-8 JSON through the ``json`` module (no Java counterpart)."""
    return JsonSerializer()


def json_deserializer() -> Deserializer[Any]:
    """UTF-8 JSON through the ``json`` module (no Java counterpart)."""
    return JsonDeserializer()
