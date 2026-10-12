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

# Stub for confluent_kafka.admin.DescribeLogDirsResult.

import asyncio
import concurrent.futures
from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.admin.log_dir_description import LogDirDescription

__all__ = ["DescribeLogDirsResult"]

class DescribeLogDirsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *, futures: Mapping[int, AnyFuture[dict[str, LogDirDescription]]],
             kind: type[KindT]) -> DescribeLogDirsResult[KindT]: ...
    @overload
    def descriptions(self: DescribeLogDirsResult[Sync]
                     ) -> dict[int, concurrent.futures.Future[dict[str, LogDirDescription]]]: ...
    @overload
    def descriptions(self: DescribeLogDirsResult[Async]
                     ) -> dict[int, asyncio.Future[dict[str, LogDirDescription]]]: ...
    @overload
    def all_descriptions(self: DescribeLogDirsResult[Sync]
                         ) -> concurrent.futures.Future[dict[int, dict[str, LogDirDescription]]]: ...
    @overload
    def all_descriptions(self: DescribeLogDirsResult[Async]
                         ) -> asyncio.Future[dict[int, dict[str, LogDirDescription]]]: ...
