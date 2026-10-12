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

"""``DescribeReplicaLogDirsResult``: Java's
``org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult``.

Hand-written: outside every generator pattern, as its public nested
``ReplicaLogDirInfo`` has only package-private, overloaded constructors (the short one
calls ``this(null, DescribeLogDirsResponse.INVALID_OFFSET_LAG, null,
INVALID_OFFSET_LAG)``). Java's constructors are package-private: each ``__init__``
raises ``TypeError`` and ``_new`` builds it (CLAUDE.md, Python Binding Conventions, Class
family); ``ReplicaLogDirInfo._new`` collapses its two constructors under ``java_forms``
with the Java-given defaults (Signatures). The getters drop Java's leading ``get``
(Idiom translations).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.admin._futures import (
    AnyFuture,
    Async,
    KindT,
    Sync,
    _all_of,
    _then_apply,
)

if TYPE_CHECKING:
    from confluent_kafka.common import TopicPartitionReplica

__all__ = ["DescribeReplicaLogDirsResult"]

# Java's ``DescribeLogDirsResponse.INVALID_OFFSET_LAG``.
_INVALID_OFFSET_LAG = -1


class DescribeReplicaLogDirsResult(Generic[KindT]):
    """The result of the ``Admin.describe_replica_log_dirs()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult``.
    """

    class ReplicaLogDirInfo:
        """The log directory information of a replica.

        Java: ``org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo``.
        """

        __slots__ = ("_current_replica_log_dir", "_current_replica_offset_lag",
                     "_future_replica_log_dir", "_future_replica_offset_lag")

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise TypeError(
                "ReplicaLogDirInfo cannot be constructed directly; use the admin client's "
                "describe_replica_log_dirs()")

        @classmethod
        @java_forms(
            Form(),
            Form("current_replica_log_dir", "current_replica_offset_lag",
                 "future_replica_log_dir", "future_replica_offset_lag",
                 defaults={"current_replica_log_dir": None,
                           "current_replica_offset_lag": _INVALID_OFFSET_LAG,
                           "future_replica_log_dir": None,
                           "future_replica_offset_lag": _INVALID_OFFSET_LAG}),
            name="ReplicaLogDirInfo",
        )
        def _new(cls, *, current_replica_log_dir: str | None = UNSET,
                 current_replica_offset_lag: int | None = None,
                 future_replica_log_dir: str | None = UNSET,
                 future_replica_offset_lag: int | None = None
                 ) -> DescribeReplicaLogDirsResult.ReplicaLogDirInfo:
            """Java's package-private ``ReplicaLogDirInfo()`` and
            ``ReplicaLogDirInfo(currentReplicaLogDir, currentReplicaOffsetLag,
            futureReplicaLogDir, futureReplicaOffsetLag)``."""
            info = cls.__new__(cls)
            info._current_replica_log_dir = current_replica_log_dir
            info._current_replica_offset_lag = current_replica_offset_lag
            info._future_replica_log_dir = future_replica_log_dir
            info._future_replica_offset_lag = future_replica_offset_lag
            return info

        def current_replica_log_dir(self) -> str | None:
            """The current log directory of the replica of this partition on the given
            broker. ``None`` if no replica is not found for this partition on the given
            broker."""
            return self._current_replica_log_dir

        def current_replica_offset_lag(self) -> int:
            """Defined as max(HW of partition - LEO of the replica, 0)."""
            return self._current_replica_offset_lag

        def future_replica_log_dir(self) -> str | None:
            """The future log directory of the replica of this partition on the given
            broker. ``None`` if the replica of this partition is not being moved to another
            log directory on the given broker."""
            return self._future_replica_log_dir

        def future_replica_offset_lag(self) -> int:
            """The LEO of the replica - LEO of the future log of this replica in the
            destination log directory. -1 if either there is not replica for this
            partition or the replica of this partition is not being moved to another log
            directory on the given broker."""
            return self._future_replica_offset_lag

        def __str__(self) -> str:
            if self._future_replica_log_dir is not None:
                return ("(currentReplicaLogDir=" + java_str(self._current_replica_log_dir)
                        + ", futureReplicaLogDir=" + java_str(self._future_replica_log_dir)
                        + ", futureReplicaOffsetLag="
                        + java_str(self._future_replica_offset_lag) + ")")
            return ("ReplicaLogDirInfo(currentReplicaLogDir="
                    + java_str(self._current_replica_log_dir) + ")")

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeReplicaLogDirsResult cannot be constructed directly; use the admin "
            "client's describe_replica_log_dirs()")

    @classmethod
    def _new(cls, *,
             futures: Mapping[TopicPartitionReplica,
                              AnyFuture[DescribeReplicaLogDirsResult.ReplicaLogDirInfo]],
             kind: type[Sync] | type[Async]) -> DescribeReplicaLogDirsResult[Any]:
        """Java's package-private ``DescribeReplicaLogDirsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def values(self) -> dict[TopicPartitionReplica,
                             AnyFuture[DescribeReplicaLogDirsResult.ReplicaLogDirInfo]]:
        """Return a map from replica to future which can be used to check the log
        directory information of individual replicas."""
        return self._futures

    def all(self) -> AnyFuture[dict[TopicPartitionReplica,
                                    DescribeReplicaLogDirsResult.ReplicaLogDirInfo]]:
        """Return a future which succeeds if log directory information of all replicas
        are available."""
        futures = self._futures

        def collect(_: None) -> dict[TopicPartitionReplica,
                                     DescribeReplicaLogDirsResult.ReplicaLogDirInfo]:
            # allOf ensured that all the futures completed successfully.
            return {key: future.result() for key, future in futures.items()}

        return _then_apply(_all_of(futures.values(), self._kind), collect)
