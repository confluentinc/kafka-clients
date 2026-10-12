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

"""``DescribeProducersResult``: Java's ``org.apache.kafka.clients.admin.DescribeProducersResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). ``partitionResult()``
raises ``IllegalArgumentError`` with Java's message for a key that was not part of the
call. ``all()`` is Java's ``allOf(...).thenApply(...)``, kept through the private
helpers ``_all_of`` / ``_then_apply``. Java's public nested ``PartitionProducerState``
is the nested class ``DescribeProducersResult.PartitionProducerState`` (Idiom
translations); it has no ``equals`` / ``hashCode`` in Java, so neither here.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._java import java_str
from confluent_kafka.admin._futures import (
    AnyFuture,
    Async,
    KindT,
    Sync,
    _all_of,
    _then_apply,
)
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.admin.producer_state import ProducerState
    from confluent_kafka.common import TopicPartition

__all__ = ["DescribeProducersResult"]


class DescribeProducersResult(Generic[KindT]):
    """The result of the ``Admin.describe_producers()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeProducersResult``.
    """

    class PartitionProducerState:
        """The active producers of one partition.

        Java: ``org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState``.
        """

        __slots__ = ("_active_producers",)

        def __init__(self, *, active_producers: Sequence[ProducerState]) -> None:
            self._active_producers = list(active_producers)

        def active_producers(self) -> list[ProducerState]:
            return self._active_producers

        def __str__(self) -> str:
            return ("PartitionProducerState(" + "activeProducers="
                    + java_str(self._active_producers) + ")")

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeProducersResult cannot be constructed directly; use the admin "
            "client's describe_producers()")

    @classmethod
    def _new(
            cls, *,
            futures: Mapping[TopicPartition, AnyFuture[DescribeProducersResult.PartitionProducerState]],
            kind: type[Sync] | type[Async]) -> DescribeProducersResult[Any]:
        """Java's package-private ``DescribeProducersResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def partition_result(
            self, *,
            partition: TopicPartition) -> AnyFuture[DescribeProducersResult.PartitionProducerState]:
        future = self._futures.get(partition)
        if future is None:
            raise IllegalArgumentError(
                message="Topic partition " + java_str(partition) + " was not included in the request")
        return future

    def all(
            self) -> AnyFuture[dict[TopicPartition, DescribeProducersResult.PartitionProducerState]]:
        futures = self._futures

        def collect(_: None) -> dict[TopicPartition, DescribeProducersResult.PartitionProducerState]:
            # allOf ensured that all the futures completed successfully.
            return {partition: future.result() for partition, future in futures.items()}

        return _then_apply(_all_of(futures.values(), self._kind), collect)
