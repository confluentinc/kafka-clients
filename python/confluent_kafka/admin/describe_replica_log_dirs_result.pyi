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

# Stub for confluent_kafka.admin.DescribeReplicaLogDirsResult.

import asyncio
import concurrent.futures
from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.common import TopicPartitionReplica

__all__ = ["DescribeReplicaLogDirsResult"]

_INVALID_OFFSET_LAG: int

class DescribeReplicaLogDirsResult(Generic[KindT]):
    class ReplicaLogDirInfo:
        @overload
        @classmethod
        def _new(cls) -> DescribeReplicaLogDirsResult.ReplicaLogDirInfo: ...
        @overload
        @classmethod
        def _new(cls, *, current_replica_log_dir: str | None, current_replica_offset_lag: int,
                 future_replica_log_dir: str | None, future_replica_offset_lag: int
                 ) -> DescribeReplicaLogDirsResult.ReplicaLogDirInfo: ...
        def current_replica_log_dir(self) -> str | None: ...
        def current_replica_offset_lag(self) -> int: ...
        def future_replica_log_dir(self) -> str | None: ...
        def future_replica_offset_lag(self) -> int: ...

    @classmethod
    def _new(cls, *,
             futures: Mapping[TopicPartitionReplica,
                              AnyFuture[DescribeReplicaLogDirsResult.ReplicaLogDirInfo]],
             kind: type[KindT]) -> DescribeReplicaLogDirsResult[KindT]: ...
    @overload
    def values(self: DescribeReplicaLogDirsResult[Sync]
               ) -> dict[TopicPartitionReplica,
                         concurrent.futures.Future[DescribeReplicaLogDirsResult.ReplicaLogDirInfo]]: ...
    @overload
    def values(self: DescribeReplicaLogDirsResult[Async]
               ) -> dict[TopicPartitionReplica,
                         asyncio.Future[DescribeReplicaLogDirsResult.ReplicaLogDirInfo]]: ...
    @overload
    def all(self: DescribeReplicaLogDirsResult[Sync]
            ) -> concurrent.futures.Future[dict[TopicPartitionReplica,
                                                DescribeReplicaLogDirsResult.ReplicaLogDirInfo]]: ...
    @overload
    def all(self: DescribeReplicaLogDirsResult[Async]
            ) -> asyncio.Future[dict[TopicPartitionReplica,
                                     DescribeReplicaLogDirsResult.ReplicaLogDirInfo]]: ...
