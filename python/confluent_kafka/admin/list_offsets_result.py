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

"""``ListOffsetsResult``: Java's ``org.apache.kafka.clients.admin.ListOffsetsResult``.

Java's constructor is public, so it stays the keyword-only ``__init__``; the
client-kind marker is read from the futures given (``AsyncKafkaFuture``\\ s: the async
kind, else the sync one), and the clients build it through ``_new``, which sets the kind
explicitly, as for every result class (CLAUDE.md, Python Binding Conventions, Class
family). ``partitionResult()`` raises ``IllegalArgumentError`` with Java's message for a
key that was not part of the call. ``all()`` is Java's ``allOf(...).thenApply(...)``,
through the futures' ``all_of`` and ``then_apply``. Java's public nested
``ListOffsetsResultInfo`` is the nested class
``ListOffsetsResult.ListOffsetsResultInfo`` (Idiom translations); it has no ``equals`` /
``hashCode`` in Java, so neither here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._java import java_str
from confluent_kafka.admin._futures import Async, KindT, Sync, _Result, _kind_of
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["ListOffsetsResult"]


class ListOffsetsResult(_Result, Generic[KindT]):
    """The result of the ``Admin.list_offsets()`` call.

    Java: ``org.apache.kafka.clients.admin.ListOffsetsResult``.
    """

    class ListOffsetsResultInfo:
        """The offset, timestamp and leader epoch listed for one partition.

        Java: ``org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo``.
        """

        __slots__ = ("_offset", "_timestamp", "_leader_epoch")

        def __init__(self, *, offset: int, timestamp: int, leader_epoch: int | None) -> None:
            self._offset = offset
            self._timestamp = timestamp
            self._leader_epoch = leader_epoch

        def offset(self) -> int:
            return self._offset

        def timestamp(self) -> int:
            return self._timestamp

        def leader_epoch(self) -> int | None:
            return self._leader_epoch

        def __str__(self) -> str:
            epoch = ("Optional.empty" if self._leader_epoch is None
                     else "Optional[" + java_str(self._leader_epoch) + "]")
            return ("ListOffsetsResultInfo(offset=" + java_str(self._offset)
                    + ", timestamp=" + java_str(self._timestamp)
                    + ", leaderEpoch=" + epoch + ")")

    __slots__ = ("_futures", "_kind")

    def __init__(
            self, *,
            futures: Mapping[TopicPartition, KafkaFuture[ListOffsetsResult.ListOffsetsResultInfo] | AsyncKafkaFuture[ListOffsetsResult.ListOffsetsResultInfo]]) -> None:
        if futures is None:
            raise TypeError("futures must not be None")
        self._futures = dict(futures)
        self._kind: type[Sync] | type[Async] = _kind_of(self._futures.values())

    @classmethod
    def _new(
            cls, *,
            futures: Mapping[TopicPartition, KafkaFuture[ListOffsetsResult.ListOffsetsResultInfo] | AsyncKafkaFuture[ListOffsetsResult.ListOffsetsResultInfo]],
            kind: type[Sync] | type[Async]) -> ListOffsetsResult[Any]:
        """The clients' builder: Java's public ``ListOffsetsResult(futures)`` with the
        client kind set by the caller."""
        result = cls.__new__(cls)
        result._futures = None if futures is None else dict(futures)
        result._kind = kind
        return result

    def partition_result(
            self, *,
            partition: TopicPartition) -> KafkaFuture[ListOffsetsResult.ListOffsetsResultInfo] | AsyncKafkaFuture[ListOffsetsResult.ListOffsetsResultInfo]:
        """Return a future which can be used to check the result for a given
        partition."""
        future = self._futures.get(partition)
        if future is None:
            raise IllegalArgumentError(
                message="List Offsets for partition \"" + java_str(partition) + "\" was not attempted")
        return future

    def all(
            self) -> KafkaFuture[dict[TopicPartition, ListOffsetsResult.ListOffsetsResultInfo]] | AsyncKafkaFuture[dict[TopicPartition, ListOffsetsResult.ListOffsetsResultInfo]]:
        """Return a future which succeeds only if offsets for all specified partitions
        have been successfully retrieved."""
        futures = self._futures

        def collect(_: None) -> dict[TopicPartition, ListOffsetsResult.ListOffsetsResultInfo]:
            # allOf ensured that all the futures completed successfully.
            return {partition: future.get_now(value_if_absent=None) for partition, future in futures.items()}

        return self._kind._FUTURE.all_of(futures=tuple(futures.values())).then_apply(function=collect)
