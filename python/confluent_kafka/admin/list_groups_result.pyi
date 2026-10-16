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

# Stub for confluent_kafka.admin.ListGroupsResult.

from collections.abc import Iterable
from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.admin.group_listing import GroupListing
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["ListGroupsResult"]

class ListGroupsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *, future: KafkaFuture[Iterable[object]] | AsyncKafkaFuture[Iterable[object]],
             kind: type[KindT]) -> ListGroupsResult[KindT]: ...
    @overload
    def all(self: ListGroupsResult[Sync]) -> KafkaFuture[list[GroupListing]]: ...
    @overload
    def all(self: ListGroupsResult[Async]) -> AsyncKafkaFuture[list[GroupListing]]: ...
    @overload
    def valid(self: ListGroupsResult[Sync]) -> KafkaFuture[list[GroupListing]]: ...
    @overload
    def valid(self: ListGroupsResult[Async]) -> AsyncKafkaFuture[list[GroupListing]]: ...
    @overload
    def errors(self: ListGroupsResult[Sync]
               ) -> KafkaFuture[list[BaseException]]: ...
    @overload
    def errors(self: ListGroupsResult[Async]) -> AsyncKafkaFuture[list[BaseException]]: ...
