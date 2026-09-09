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

"""The built-in serde factories (spec §5.4).

Each returns a **typed** callable (``Serializer[T]`` / ``Deserializer[T]``) so
the client's ``Generic[K, V]`` is inferred from the factory's return type (D11 /
§3 principle 7) — there are no hand-written type parameters anywhere. The factory
parameters are keyword-only per §3 principle 4, even though the factories are our
API rather than user-written callables, so a factory can grow a parameter without
breaking callers.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common.uuid import Uuid

from ._protocols import Deserializer, Serializer
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

_INT_SIZES = (4, 8)
_FLOAT_SIZES = (8, 4)


def bytes_serializer() -> Serializer[bytes]:
    """Passthrough serializer — the producer default."""
    return ByteArraySerializer()


def bytes_deserializer() -> Deserializer[bytes]:
    """Owned-``bytes`` deserializer (lazy copy) — the consumer default."""
    return ByteArrayDeserializer()


def memoryview_deserializer() -> Deserializer[memoryview]:
    """Zero-copy deserializer: views borrow the fetch batch (retention rule)."""
    return ByteBufferDeserializer()


def string_serializer(*, encoding: str = "utf_8") -> Serializer[str]:
    """``str`` -> bytes with the given ``encoding`` (default UTF-8)."""
    return StringSerializer(encoding)


def string_deserializer(*, encoding: str = "utf_8") -> Deserializer[str]:
    """bytes -> ``str`` with the given ``encoding`` (default UTF-8)."""
    return StringDeserializer(encoding)


def int_serializer(*, size: int = 4) -> Serializer[int]:
    """Signed big-endian integer serializer; ``size`` is 4 or 8 bytes."""
    if size not in _INT_SIZES:
        raise IllegalArgumentError(
            f"int_serializer size must be one of {_INT_SIZES}; got {size}"
        )
    return IntSerializer(size)


def int_deserializer(*, size: int = 4) -> Deserializer[int]:
    """Signed big-endian integer deserializer; ``size`` is 4 or 8 bytes."""
    if size not in _INT_SIZES:
        raise IllegalArgumentError(
            f"int_deserializer size must be one of {_INT_SIZES}; got {size}"
        )
    return IntDeserializer(size)


def float_serializer(*, size: int = 8) -> Serializer[float]:
    """Big-endian IEEE-754 serializer; ``size`` is 8 or 4 bytes."""
    if size not in _FLOAT_SIZES:
        raise IllegalArgumentError(
            f"float_serializer size must be one of {_FLOAT_SIZES}; got {size}"
        )
    return FloatSerializer(size)


def float_deserializer(*, size: int = 8) -> Deserializer[float]:
    """Big-endian IEEE-754 deserializer; ``size`` is 8 or 4 bytes."""
    if size not in _FLOAT_SIZES:
        raise IllegalArgumentError(
            f"float_deserializer size must be one of {_FLOAT_SIZES}; got {size}"
        )
    return FloatDeserializer(size)


def bool_serializer() -> Serializer[bool]:
    """Single-byte boolean serializer (``0x01`` / ``0x00``)."""
    return BooleanSerializer()


def bool_deserializer() -> Deserializer[bool]:
    """Single-byte boolean deserializer."""
    return BooleanDeserializer()


def uuid_serializer() -> Serializer[Uuid]:
    """``Uuid`` serializer via its string form (Java's UUID serde)."""
    return UUIDSerializer()


def uuid_deserializer() -> Deserializer[Uuid]:
    """``Uuid`` deserializer via its string form (Java's UUID serde)."""
    return UUIDDeserializer()


def json_serializer() -> Serializer[Any]:
    """JSON serializer via the ``json`` module (no Java counterpart)."""
    return JsonSerializer()


def json_deserializer() -> Deserializer[Any]:
    """JSON deserializer via the ``json`` module (no Java counterpart)."""
    return JsonDeserializer()
