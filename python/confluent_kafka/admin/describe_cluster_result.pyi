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

import asyncio
import concurrent.futures
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.common import AclOperation, Node

__all__ = ["DescribeClusterResult"]

class DescribeClusterResult(Generic[KindT]):
    @classmethod
    def _new(cls, *, nodes: AnyFuture[list[Node]], controller: AnyFuture[Node],
             cluster_id: AnyFuture[str],
             authorized_operations: AnyFuture[set[AclOperation] | None],
             kind: type[KindT]) -> DescribeClusterResult[KindT]: ...
    @overload
    def nodes(self: DescribeClusterResult[Sync]) -> concurrent.futures.Future[list[Node]]: ...
    @overload
    def nodes(self: DescribeClusterResult[Async]) -> asyncio.Future[list[Node]]: ...
    @overload
    def controller(self: DescribeClusterResult[Sync]) -> concurrent.futures.Future[Node]: ...
    @overload
    def controller(self: DescribeClusterResult[Async]) -> asyncio.Future[Node]: ...
    @overload
    def cluster_id(self: DescribeClusterResult[Sync]) -> concurrent.futures.Future[str]: ...
    @overload
    def cluster_id(self: DescribeClusterResult[Async]) -> asyncio.Future[str]: ...
    @overload
    def authorized_operations(self: DescribeClusterResult[Sync]
                              ) -> concurrent.futures.Future[set[AclOperation] | None]: ...
    @overload
    def authorized_operations(self: DescribeClusterResult[Async]
                              ) -> asyncio.Future[set[AclOperation] | None]: ...
