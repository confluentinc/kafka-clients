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

"""``ListPartitionReassignmentsResult``: Java's
``org.apache.kafka.clients.admin.ListPartitionReassignmentsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync, _Result

if TYPE_CHECKING:
    from confluent_kafka.admin.partition_reassignment import PartitionReassignment
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["ListPartitionReassignmentsResult"]


class ListPartitionReassignmentsResult(_Result, Generic[KindT]):
    """The result of ``Admin.list_partition_reassignments()``.

    Java: ``org.apache.kafka.clients.admin.ListPartitionReassignmentsResult``.
    """

    __slots__ = ("_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ListPartitionReassignmentsResult cannot be constructed directly; use the admin "
            "client's list_partition_reassignments()")

    @classmethod
    def _new(
            cls, *,
            reassignments: KafkaFuture[dict[TopicPartition, PartitionReassignment]] | AsyncKafkaFuture[dict[TopicPartition, PartitionReassignment]],
            kind: type[Sync] | type[Async]) -> ListPartitionReassignmentsResult[Any]:
        """Java's package-private ``ListPartitionReassignmentsResult(reassignments)``."""
        result = cls.__new__(cls)
        result._future = reassignments
        result._kind = kind
        return result

    def reassignments(
            self) -> KafkaFuture[dict[TopicPartition, PartitionReassignment]] | AsyncKafkaFuture[dict[TopicPartition, PartitionReassignment]]:
        """Return a future which yields a map containing each partition's
        reassignments"""
        return self._future
