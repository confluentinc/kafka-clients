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

# Stub for confluent_kafka.admin.AbortTransactionResult.

import asyncio
import concurrent.futures
from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.common import TopicPartition

__all__ = ["AbortTransactionResult"]

class AbortTransactionResult(Generic[KindT]):
    @classmethod
    def _new(cls, *, futures: Mapping[TopicPartition, AnyFuture[None]],
             kind: type[KindT]) -> AbortTransactionResult[KindT]: ...
    @overload
    def all(self: AbortTransactionResult[Sync]) -> concurrent.futures.Future[None]: ...
    @overload
    def all(self: AbortTransactionResult[Async]) -> asyncio.Future[None]: ...
