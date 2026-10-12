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

# Stub for confluent_kafka.admin.ListOffsetsResult.

import asyncio
import concurrent.futures
from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.common import TopicPartition

__all__ = ["ListOffsetsResult"]

class ListOffsetsResult(Generic[KindT]):
    class ListOffsetsResultInfo:
        def __init__(self, *, offset: int, timestamp: int, leader_epoch: int | None) -> None: ...
        def offset(self) -> int: ...
        def timestamp(self) -> int: ...
        def leader_epoch(self) -> int | None: ...

    @overload
    def __init__(self: ListOffsetsResult[Sync], *,
                 futures: Mapping[TopicPartition,
                                  concurrent.futures.Future[ListOffsetsResult.ListOffsetsResultInfo]]
                 ) -> None: ...
    @overload
    def __init__(self: ListOffsetsResult[Async], *,
                 futures: Mapping[TopicPartition,
                                  asyncio.Future[ListOffsetsResult.ListOffsetsResultInfo]]
                 ) -> None: ...
    @classmethod
    def _new(cls, *,
             futures: Mapping[TopicPartition, AnyFuture[ListOffsetsResult.ListOffsetsResultInfo]],
             kind: type[KindT]) -> ListOffsetsResult[KindT]: ...
    @overload
    def partition_result(self: ListOffsetsResult[Sync], *, partition: TopicPartition
                         ) -> concurrent.futures.Future[ListOffsetsResult.ListOffsetsResultInfo]: ...
    @overload
    def partition_result(self: ListOffsetsResult[Async], *, partition: TopicPartition
                         ) -> asyncio.Future[ListOffsetsResult.ListOffsetsResultInfo]: ...
    @overload
    def all(self: ListOffsetsResult[Sync]
            ) -> concurrent.futures.Future[dict[TopicPartition,
                                                ListOffsetsResult.ListOffsetsResultInfo]]: ...
    @overload
    def all(self: ListOffsetsResult[Async]
            ) -> asyncio.Future[dict[TopicPartition, ListOffsetsResult.ListOffsetsResultInfo]]: ...
