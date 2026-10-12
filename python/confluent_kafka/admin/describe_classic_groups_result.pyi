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

# Stub for confluent_kafka.admin.DescribeClassicGroupsResult.

import asyncio
import concurrent.futures
from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.admin.classic_group_description import ClassicGroupDescription

__all__ = ["DescribeClassicGroupsResult"]

class DescribeClassicGroupsResult(Generic[KindT]):
    @overload
    def __init__(self: DescribeClassicGroupsResult[Sync], *,
                 futures: Mapping[str, concurrent.futures.Future[ClassicGroupDescription]]
                 ) -> None: ...
    @overload
    def __init__(self: DescribeClassicGroupsResult[Async], *,
                 futures: Mapping[str, asyncio.Future[ClassicGroupDescription]]) -> None: ...
    @classmethod
    def _new(cls, *, futures: Mapping[str, AnyFuture[ClassicGroupDescription]],
             kind: type[KindT]) -> DescribeClassicGroupsResult[KindT]: ...
    @overload
    def described_groups(self: DescribeClassicGroupsResult[Sync]
                         ) -> dict[str, concurrent.futures.Future[ClassicGroupDescription]]: ...
    @overload
    def described_groups(self: DescribeClassicGroupsResult[Async]
                         ) -> dict[str, asyncio.Future[ClassicGroupDescription]]: ...
    @overload
    def all(self: DescribeClassicGroupsResult[Sync]
            ) -> concurrent.futures.Future[dict[str, ClassicGroupDescription]]: ...
    @overload
    def all(self: DescribeClassicGroupsResult[Async]
            ) -> asyncio.Future[dict[str, ClassicGroupDescription]]: ...
