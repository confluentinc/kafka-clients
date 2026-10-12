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

# Stub for confluent_kafka.admin.ListPartitionReassignmentsResult.

import asyncio
import concurrent.futures
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.admin.partition_reassignment import PartitionReassignment
from confluent_kafka.common import TopicPartition

__all__ = ["ListPartitionReassignmentsResult"]

class ListPartitionReassignmentsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *,
             reassignments: AnyFuture[dict[TopicPartition, PartitionReassignment]],
             kind: type[KindT]) -> ListPartitionReassignmentsResult[KindT]: ...
    @overload
    def reassignments(self: ListPartitionReassignmentsResult[Sync]
                      ) -> concurrent.futures.Future[dict[TopicPartition, PartitionReassignment]]: ...
    @overload
    def reassignments(self: ListPartitionReassignmentsResult[Async]
                      ) -> asyncio.Future[dict[TopicPartition, PartitionReassignment]]: ...
