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

# Stub for confluent_kafka.admin.ListTopicsResult.

from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.admin.topic_listing import TopicListing
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["ListTopicsResult"]

class ListTopicsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *,
             future: KafkaFuture[dict[str, TopicListing]] | AsyncKafkaFuture[dict[str, TopicListing]],
             kind: type[KindT]) -> ListTopicsResult[KindT]: ...
    @overload
    def names_to_listings(self: ListTopicsResult[Sync]) -> KafkaFuture[dict[str, TopicListing]]: ...
    @overload
    def names_to_listings(self: ListTopicsResult[Async]
                          ) -> AsyncKafkaFuture[dict[str, TopicListing]]: ...
    @overload
    def listings(self: ListTopicsResult[Sync]) -> KafkaFuture[list[TopicListing]]: ...
    @overload
    def listings(self: ListTopicsResult[Async]) -> AsyncKafkaFuture[list[TopicListing]]: ...
    @overload
    def names(self: ListTopicsResult[Sync]) -> KafkaFuture[set[str]]: ...
    @overload
    def names(self: ListTopicsResult[Async]) -> AsyncKafkaFuture[set[str]]: ...
