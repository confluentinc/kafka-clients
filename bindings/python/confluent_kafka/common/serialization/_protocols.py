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

Mirrors ``org.apache.kafka.common.serialization.Serializer`` /
``Deserializer`` (Apache Kafka 4.3.1). Java's three ``serialize`` /
``deserialize`` overloads collapse to one callable shape whose ``headers`` are
always passed (spec §5.4; Design Decisions D6 "one callable shape"). ``data`` /
``value`` may be ``None`` — Java's tombstone passthrough, where the serde is
*invoked* on null and decides what it maps to.

A serde is **any callable of the right shape** — a function, ``lambda``,
``functools.partial`` or an instance with ``__call__``. These ``Protocol``s
exist only for the type checker; nothing is required to inherit from them.

The optional lifecycle (``configure`` / ``close``) is duck-typed. ``Configurable``
and ``Closable`` name the two capabilities as ``@runtime_checkable`` protocols so
the client can ``isinstance``-check them; ``SerdeBase`` is a no-op convenience
base for Java-style porting (each method is Java's ``default {}``).
"""

from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

from confluent_kafka.common.headers import Headers

# ``T_contra`` on the serializer (it consumes ``T``); ``T_co`` on the
# deserializer (it produces ``T``) — so ``Serializer[bytes]`` accepts a
# ``bytes`` subtype and ``Deserializer[str]`` is usable where a supertype is
# expected, matching Java's ``Serializer<T>`` / ``Deserializer<T>`` variance.
T_contra = TypeVar("T_contra", contravariant=True)
T_co = TypeVar("T_co", covariant=True)


class Serializer(Protocol[T_contra]):
    """Converts an object to bytes — Java's ``Serializer<T>``.

    ``value`` may be ``None`` (Java's tombstone); every built-in returns
    ``None`` for ``None``. ``headers`` are always passed but may be empty.
    """

    def __call__(
        self,
        topic: str,
        value: T_contra | None,
        headers: Headers | None = None,
    ) -> bytes | None: ...


class Deserializer(Protocol[T_co]):
    """Converts bytes to an object — Java's ``Deserializer<T>``.

    ``data`` is a ``memoryview`` into the fetch batch on the receive path (a
    ``bytes``-like on other paths) and may be ``None`` (Java's tombstone).
    ``headers`` are always passed but may be empty.
    """

    def __call__(
        self,
        topic: str,
        data: memoryview | None,
        headers: Headers | None = None,
    ) -> T_co | None: ...


@runtime_checkable
class Configurable(Protocol):
    """Serdes that accept post-construction configuration on the config route.

    Java's ``Configurable`` / the ``configure(Map, boolean)`` default method:
    called once, after no-arg construction, with the whole client config plus
    which slot the serde occupies. Never called on the kwarg (instance) route.
    """

    def configure(self, configs: dict[str, object], is_key: bool) -> None: ...


@runtime_checkable
class Closable(Protocol):
    """Serdes that hold resources to release at client close.

    Java's ``Closeable.close()`` default method. Called at client close;
    exceptions are logged, never raised (Java's ``Utils.closeQuietly``). Must be
    idempotent — it may be called multiple times.
    """

    def close(self) -> None: ...


class SerdeBase:
    """A no-op lifecycle base for Java-style serde classes.

    Subclass and override only ``__call__`` (and, if needed, ``configure`` /
    ``close``). Each method here is Java's ``default {}`` — a bare function is
    already a complete serde, so inheriting this is a convenience, never a
    requirement.
    """

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        # Java's default {} — intentionally left blank.
        pass

    def close(self) -> None:
        # Java's default {} — intentionally left blank.
        pass
