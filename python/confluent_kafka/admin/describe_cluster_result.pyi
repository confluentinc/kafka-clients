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

# Stub for confluent_kafka.admin.DescribeClusterResult.

from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.common import Node
from confluent_kafka.common.acl import AclOperation
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeClusterResult"]

class DescribeClusterResult(Generic[KindT]):
    @classmethod
    def _new(cls, *, nodes: KafkaFuture[list[Node]] | AsyncKafkaFuture[list[Node]],
             controller: KafkaFuture[Node] | AsyncKafkaFuture[Node],
             cluster_id: KafkaFuture[str] | AsyncKafkaFuture[str],
             authorized_operations: KafkaFuture[set[AclOperation] | None] | AsyncKafkaFuture[set[AclOperation] | None],
             kind: type[KindT]) -> DescribeClusterResult[KindT]: ...
    @overload
    def nodes(self: DescribeClusterResult[Sync]) -> KafkaFuture[list[Node]]: ...
    @overload
    def nodes(self: DescribeClusterResult[Async]) -> AsyncKafkaFuture[list[Node]]: ...
    @overload
    def controller(self: DescribeClusterResult[Sync]) -> KafkaFuture[Node]: ...
    @overload
    def controller(self: DescribeClusterResult[Async]) -> AsyncKafkaFuture[Node]: ...
    @overload
    def cluster_id(self: DescribeClusterResult[Sync]) -> KafkaFuture[str]: ...
    @overload
    def cluster_id(self: DescribeClusterResult[Async]) -> AsyncKafkaFuture[str]: ...
    @overload
    def authorized_operations(self: DescribeClusterResult[Sync]
                              ) -> KafkaFuture[set[AclOperation] | None]: ...
    @overload
    def authorized_operations(self: DescribeClusterResult[Async]
                              ) -> AsyncKafkaFuture[set[AclOperation] | None]: ...
