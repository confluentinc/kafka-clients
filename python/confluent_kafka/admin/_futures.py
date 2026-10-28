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

"""The admin ``*Result`` classes' private client-kind marker.

A result class is shared by both clients, so it is generic over a private marker of
the client kind, :class:`Sync` or :class:`Async`, set by the client that returns it
(CLAUDE.md, Python Binding Conventions, Class family): ``CreateTopicsResult[Sync]``
holds :class:`~confluent_kafka.common.KafkaFuture`\\ s,
``CreateTopicsResult[Async]`` :class:`~confluent_kafka.common.AsyncKafkaFuture`\\ s.
No user code writes the marker.

The futures themselves are Java's ``KafkaFuture`` (Futures): a result composes its
``all()`` and refinements through their ``then_apply`` / ``when_complete`` /
``all_of`` and Java's ``new KafkaFutureImpl<>()`` is the private
``_KafkaFutureImpl``; the marker's ``_FUTURE`` is the future class of its kind, for
the static ``all_of`` and for a future over a ``_KafkaFutureImpl``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, ClassVar, SupportsIndex, TypeVar

from confluent_kafka._native_copy import not_copyable
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture, _KafkaFutureBase

__all__ = ["Async", "KindT", "Sync", "_Result", "_kind_of"]


class Sync:
    """Marker of the sync client kind: a result's futures are ``KafkaFuture``\\ s."""

    __slots__ = ()

    #: The future class of the kind.
    _FUTURE: ClassVar[Any] = KafkaFuture

    def __init__(self) -> None:
        raise TypeError("Sync is a type marker; it has no instances")


class Async:
    """Marker of the async client kind: a result's futures are
    ``AsyncKafkaFuture``\\ s."""

    __slots__ = ()

    #: The future class of the kind.
    _FUTURE: ClassVar[Any] = AsyncKafkaFuture

    def __init__(self) -> None:
        raise TypeError("Async is a type marker; it has no instances")


#: The client kind a result class is generic over.
KindT = TypeVar("KindT", Sync, Async)


def _kind_of(futures: Iterable[Any]) -> type[Sync] | type[Async]:
    """The kind of the given futures, for a result's public constructor:
    :class:`Async` when one is an ``AsyncKafkaFuture``, else :class:`Sync` (also
    for none)."""
    for future in futures:
        return Async if isinstance(future, AsyncKafkaFuture) else Sync
    return Sync


def _holds_native(value: Any, depth: int = 0) -> bool:
    """Whether ``value`` is, or holds within a few levels of containers, a future
    over a C handle."""
    if isinstance(value, _KafkaFutureBase):
        return value._native is not None
    if depth > 3:
        return False
    if isinstance(value, Mapping):
        return any(_holds_native(v, depth + 1) for v in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_holds_native(v, depth + 1) for v in value)
    return False


class _Result:
    """The base of every admin ``*Result`` class: ``copy``, ``deepcopy`` and
    ``pickle`` of a result built over the admin client's C result, whose futures
    are over C handles, are refused with ``TypeError``, as Java's results are
    neither ``Cloneable`` nor ``Serializable`` and a copy would share the native
    futures (CLAUDE.md, Python Binding Conventions, Types, the
    ``java.io.Serializable`` row); a result a mock builds, over Python state,
    keeps Python's default."""

    __slots__ = ()

    def __reduce_ex__(self, protocol: SupportsIndex) -> str | tuple[Any, ...]:
        values = list(getattr(self, "__dict__", {}).values())
        for cls in type(self).__mro__:
            slots = cls.__dict__.get("__slots__", ())
            for name in (slots,) if isinstance(slots, str) else slots:
                if name not in ("__dict__", "__weakref__"):
                    values.append(getattr(self, name, None))
        if _holds_native(values):
            raise not_copyable(type(self).__name__, "it holds the admin client's native result")
        return super().__reduce_ex__(protocol)
