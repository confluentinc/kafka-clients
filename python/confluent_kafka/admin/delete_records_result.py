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

"""``DeleteRecordsResult``: Java's ``org.apache.kafka.clients.admin.DeleteRecordsResult``.

Java's constructor is public, so it stays the keyword-only ``__init__``; the
client-kind marker is read from the futures given (``asyncio.Future``\\ s: the async
kind, else the sync one), and the clients build it through ``_new``, which sets the kind
explicitly, as for every result class (CLAUDE.md, Python Binding Conventions, Class
family). ``all()`` is Java's ``KafkaFuture.allOf``, kept through the private helper
``_all_of`` (Idiom translations).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import (
    AnyFuture,
    Async,
    KindT,
    Sync,
    _all_of,
    _kind_of,
)

if TYPE_CHECKING:
    from confluent_kafka.admin.deleted_records import DeletedRecords
    from confluent_kafka.common import TopicPartition

__all__ = ["DeleteRecordsResult"]


class DeleteRecordsResult(Generic[KindT]):
    """The result of the ``Admin.delete_records()`` call.

    Java: ``org.apache.kafka.clients.admin.DeleteRecordsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *, futures: Mapping[TopicPartition, AnyFuture[DeletedRecords]]) -> None:
        self._futures = dict(futures)
        self._kind: type[Sync] | type[Async] = _kind_of(self._futures.values())

    @classmethod
    def _new(cls, *, futures: Mapping[TopicPartition, AnyFuture[DeletedRecords]],
             kind: type[Sync] | type[Async]) -> DeleteRecordsResult[Any]:
        """The clients' builder: Java's public ``DeleteRecordsResult(futures)`` with the
        client kind set by the caller."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def low_watermarks(self) -> Mapping[TopicPartition, AnyFuture[DeletedRecords]]:
        """Return a map from topic partition to futures which can be used to check the
        status of individual deletions."""
        return self._futures

    def all(self) -> AnyFuture[None]:
        """Return a future which succeeds only if all the records deletions succeed."""
        return _all_of(self._futures.values(), self._kind)
