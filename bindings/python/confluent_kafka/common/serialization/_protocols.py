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

"""The serde protocols and the optional lifecycle capabilities.

*(deviation)* Java's ``Serializer`` / ``Deserializer`` interfaces, with their
three ``serialize`` / ``deserialize`` overloads, are one callable shape whose
headers are always passed (CLAUDE.md, Python Binding Conventions,
Serialization). A serde is any callable of that shape: a function, a
``lambda``, a ``functools.partial`` or an instance with ``__call__``; the
protocols exist for the type checker. ``None`` in is Java's null, and the serde
decides what it maps to.

The lifecycle is duck-typed: ``configure(configs, is_key)`` runs once after
construction on the config route only, ``close()`` at client close (its
exceptions logged, never raised), and an absent method is a no-op.
``Configurable`` and ``Closeable`` are ``@runtime_checkable`` protocols for the
two methods, and ``SerdeBase`` is a no-op base with both.
"""

from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

from confluent_kafka.common.headers import Headers

__all__ = ["Closeable", "Configurable", "Deserializer", "SerdeBase", "Serializer"]

T_contra = TypeVar("T_contra", contravariant=True)
T_co = TypeVar("T_co", covariant=True)


class Serializer(Protocol[T_contra]):
    """An interface for converting objects to bytes.

    Java: ``org.apache.kafka.common.serialization.Serializer<T>``.
    """

    def __call__(self, topic: str, value: T_contra | None,
                 headers: Headers | None = None) -> bytes | None:
        """Convert ``value`` into bytes: ``topic`` is the topic associated
        with the data, ``headers`` the headers associated with the record, and
        ``value`` the typed data. Returns the serialized bytes."""
        ...


class Deserializer(Protocol[T_co]):
    """An interface for converting bytes to objects.

    Java: ``org.apache.kafka.common.serialization.Deserializer<T>``.
    """

    def __call__(self, topic: str, data: memoryview | None,
                 headers: Headers | None = None) -> T_co | None:
        """Deserialize a record value from a ``memoryview`` into a value or
        object: ``topic`` is the topic associated with the data, ``headers``
        the headers associated with the record, and ``data`` the serialized
        bytes, a view into the fetch batch. Returns the deserialized typed
        data; may be ``None``."""
        ...


@runtime_checkable
class Configurable(Protocol):
    """A serde configured after construction on the config route.

    Java: the ``configure(Map<String, ?> configs, boolean isKey)`` method of
    ``Serializer`` / ``Deserializer``.
    """

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        """Configure this class: ``configs`` are the configs in key/value
        pairs, ``is_key`` whether it is for the key or the value."""
        ...


@runtime_checkable
class Closeable(Protocol):
    """A serde holding resources to release at client close.

    Java: ``java.io.Closeable``, which ``Serializer`` / ``Deserializer``
    extend. This method must be idempotent as it may be called multiple times.
    """

    def close(self) -> None:
        """Close this serializer or deserializer."""
        ...


class SerdeBase:
    """A no-op base with both lifecycle methods, Java's ``default`` bodies,
    for a serde written as a class."""

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        """Configure this class; intentionally left blank."""

    def close(self) -> None:
        """Close this serde; intentionally left blank."""
