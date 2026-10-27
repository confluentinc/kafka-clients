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

"""``AlterReplicaLogDirsResult``: Java's
``org.apache.kafka.clients.admin.AlterReplicaLogDirsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). ``all()`` is Java's
``KafkaFuture.allOf``, the ``all_of`` of the kind's future class (Futures).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartitionReplica
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["AlterReplicaLogDirsResult"]


class AlterReplicaLogDirsResult(Generic[KindT]):
    """The result of ``Admin.alter_replica_log_dirs()``.

    To retrieve the detailed result per specified ``TopicPartitionReplica``, use
    ``values()``. To retrieve the overall result only, use ``all()``.

    Java: ``org.apache.kafka.clients.admin.AlterReplicaLogDirsResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "AlterReplicaLogDirsResult cannot be constructed directly; use the admin "
            "client's alter_replica_log_dirs()")

    @classmethod
    def _new(cls, *,
             futures: Mapping[TopicPartitionReplica, KafkaFuture[None] | AsyncKafkaFuture[None]],
             kind: type[Sync] | type[Async]) -> AlterReplicaLogDirsResult[Any]:
        """Java's package-private ``AlterReplicaLogDirsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = None if futures is None else dict(futures)
        result._kind = kind
        return result

    def values(self) -> Mapping[TopicPartitionReplica, KafkaFuture[None] | AsyncKafkaFuture[None]]:
        """Return a map from ``TopicPartitionReplica`` to the future which holds the status of
        the individual replica movement.

        The future of a replica completes with ``None`` when its movement succeeded; else it
        fails with one of: ``ClusterAuthorizationError`` (CLUSTER_AUTHORIZATION_FAILED, 31),
        ``InvalidTopicError`` (INVALID_TOPIC_EXCEPTION, 17), ``LogDirNotFoundError``
        (LOG_DIR_NOT_FOUND, 57), ``ReplicaNotAvailableError`` (REPLICA_NOT_AVAILABLE, 9),
        ``KafkaStorageError`` (KAFKA_STORAGE_ERROR, 56), ``UnknownServerError``
        (UNKNOWN_SERVER_ERROR, -1).
        """
        return self._futures

    def all(self) -> KafkaFuture[None] | AsyncKafkaFuture[None]:
        """Return a future which succeeds if all the replica movements have succeeded; else
        it fails with an error described in ``values()``."""
        return self._kind._FUTURE.all_of(futures=tuple(self._futures.values()))
