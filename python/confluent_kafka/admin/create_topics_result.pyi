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

# Stub for confluent_kafka.admin.CreateTopicsResult.

import asyncio
import concurrent.futures
from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.admin.config import Config
from confluent_kafka.common import Uuid
from confluent_kafka.common.errors import ApiError

__all__ = ["CreateTopicsResult"]

_UNKNOWN: int

class _TopicMetadataAndConfig:
    @overload
    def __init__(self, *, topic_id: Uuid, num_partitions: int, replication_factor: int,
                 config: Config) -> None: ...
    @overload
    def __init__(self, *, exception: ApiError) -> None: ...
    def topic_id(self) -> Uuid: ...
    def num_partitions(self) -> int: ...
    def replication_factor(self) -> int: ...
    def config(self) -> Config: ...

class CreateTopicsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *, futures: Mapping[str, AnyFuture[_TopicMetadataAndConfig]],
             kind: type[KindT]) -> CreateTopicsResult[KindT]: ...
    @overload
    def values(self: CreateTopicsResult[Sync]) -> dict[str, concurrent.futures.Future[None]]: ...
    @overload
    def values(self: CreateTopicsResult[Async]) -> dict[str, asyncio.Future[None]]: ...
    @overload
    def all(self: CreateTopicsResult[Sync]) -> concurrent.futures.Future[None]: ...
    @overload
    def all(self: CreateTopicsResult[Async]) -> asyncio.Future[None]: ...
    @overload
    def config(self: CreateTopicsResult[Sync], *, topic: str) -> concurrent.futures.Future[Config]: ...
    @overload
    def config(self: CreateTopicsResult[Async], *, topic: str) -> asyncio.Future[Config]: ...
    @overload
    def topic_id(self: CreateTopicsResult[Sync], *, topic: str) -> concurrent.futures.Future[Uuid]: ...
    @overload
    def topic_id(self: CreateTopicsResult[Async], *, topic: str) -> asyncio.Future[Uuid]: ...
    @overload
    def num_partitions(self: CreateTopicsResult[Sync], *, topic: str
                       ) -> concurrent.futures.Future[int]: ...
    @overload
    def num_partitions(self: CreateTopicsResult[Async], *, topic: str) -> asyncio.Future[int]: ...
    @overload
    def replication_factor(self: CreateTopicsResult[Sync], *, topic: str
                           ) -> concurrent.futures.Future[int]: ...
    @overload
    def replication_factor(self: CreateTopicsResult[Async], *, topic: str
                           ) -> asyncio.Future[int]: ...
