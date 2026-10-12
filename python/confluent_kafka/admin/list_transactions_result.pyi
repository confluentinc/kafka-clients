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

# Stub for confluent_kafka.admin.ListTransactionsResult.

import asyncio
import concurrent.futures
from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.admin.transaction_listing import TransactionListing

__all__ = ["ListTransactionsResult"]

class ListTransactionsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *,
             future: AnyFuture[Mapping[int, AnyFuture[list[TransactionListing]]]],
             kind: type[KindT]) -> ListTransactionsResult[KindT]: ...
    @overload
    def all(self: ListTransactionsResult[Sync]
            ) -> concurrent.futures.Future[list[TransactionListing]]: ...
    @overload
    def all(self: ListTransactionsResult[Async]) -> asyncio.Future[list[TransactionListing]]: ...
    @overload
    def by_broker_id(self: ListTransactionsResult[Sync]) -> concurrent.futures.Future[
        dict[int, concurrent.futures.Future[list[TransactionListing]]]]: ...
    @overload
    def by_broker_id(self: ListTransactionsResult[Async]) -> asyncio.Future[
        dict[int, asyncio.Future[list[TransactionListing]]]]: ...
    @overload
    def all_by_broker_id(self: ListTransactionsResult[Sync]
                         ) -> concurrent.futures.Future[dict[int, list[TransactionListing]]]: ...
    @overload
    def all_by_broker_id(self: ListTransactionsResult[Async]
                         ) -> asyncio.Future[dict[int, list[TransactionListing]]]: ...
