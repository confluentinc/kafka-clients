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

# Stub for confluent_kafka.admin.DescribeConsumerGroupsResult.

import asyncio
import concurrent.futures
from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.admin.consumer_group_description import ConsumerGroupDescription

__all__ = ["DescribeConsumerGroupsResult"]

class DescribeConsumerGroupsResult(Generic[KindT]):
    @overload
    def __init__(self: DescribeConsumerGroupsResult[Sync], *,
                 futures: Mapping[str, concurrent.futures.Future[ConsumerGroupDescription]]
                 ) -> None: ...
    @overload
    def __init__(self: DescribeConsumerGroupsResult[Async], *,
                 futures: Mapping[str, asyncio.Future[ConsumerGroupDescription]]) -> None: ...
    @classmethod
    def _new(cls, *, futures: Mapping[str, AnyFuture[ConsumerGroupDescription]],
             kind: type[KindT]) -> DescribeConsumerGroupsResult[KindT]: ...
    @overload
    def described_groups(self: DescribeConsumerGroupsResult[Sync]
                         ) -> dict[str, concurrent.futures.Future[ConsumerGroupDescription]]: ...
    @overload
    def described_groups(self: DescribeConsumerGroupsResult[Async]
                         ) -> dict[str, asyncio.Future[ConsumerGroupDescription]]: ...
    @overload
    def all(self: DescribeConsumerGroupsResult[Sync]
            ) -> concurrent.futures.Future[dict[str, ConsumerGroupDescription]]: ...
    @overload
    def all(self: DescribeConsumerGroupsResult[Async]
            ) -> asyncio.Future[dict[str, ConsumerGroupDescription]]: ...
