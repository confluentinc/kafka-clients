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

"""``AlterPartitionReassignmentsResult``: Java's
``org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). ``all()`` is Java's
``KafkaFuture.allOf``, kept through the private helper ``_all_of`` (Idiom
translations).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync, _all_of

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartition

__all__ = ["AlterPartitionReassignmentsResult"]


class AlterPartitionReassignmentsResult(Generic[KindT]):
    """The result of ``Admin.alter_partition_reassignments()``.

    Java: ``org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "AlterPartitionReassignmentsResult cannot be constructed directly; use the admin "
            "client's alter_partition_reassignments()")

    @classmethod
    def _new(cls, *, futures: Mapping[TopicPartition, AnyFuture[None]],
             kind: type[Sync] | type[Async]) -> AlterPartitionReassignmentsResult[Any]:
        """Java's package-private ``AlterPartitionReassignmentsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def values(self) -> Mapping[TopicPartition, AnyFuture[None]]:
        """Return a map from partitions to futures which can be used to check the status
        of the reassignment.

        Possible error codes:

        INVALID_REPLICA_ASSIGNMENT (39) - if the specified replica assignment was not
        valid -- for example, if it included negative numbers, repeated numbers, or
        specified a broker ID that the controller was not aware of.
        NO_REASSIGNMENT_IN_PROGRESS (85) - if the request wants to cancel reassignments
        but none exist UNKNOWN (-1)
        """
        return self._futures

    def all(self) -> AnyFuture[None]:
        """Return a future which succeeds only if all the reassignments were
        successfully initiated."""
        return _all_of(self._futures.values(), self._kind)
