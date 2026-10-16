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

from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.admin.consumer_group_description import ConsumerGroupDescription
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeConsumerGroupsResult"]

class DescribeConsumerGroupsResult(Generic[KindT]):
    @overload
    def __init__(self: DescribeConsumerGroupsResult[Sync], *,
                 futures: Mapping[str, KafkaFuture[ConsumerGroupDescription]]) -> None: ...
    @overload
    def __init__(self: DescribeConsumerGroupsResult[Async], *,
                 futures: Mapping[str, AsyncKafkaFuture[ConsumerGroupDescription]]) -> None: ...
    @classmethod
    def _new(cls, *,
             futures: Mapping[str, KafkaFuture[ConsumerGroupDescription] | AsyncKafkaFuture[ConsumerGroupDescription]],
             kind: type[KindT]) -> DescribeConsumerGroupsResult[KindT]: ...
    @overload
    def described_groups(self: DescribeConsumerGroupsResult[Sync]
                         ) -> dict[str, KafkaFuture[ConsumerGroupDescription]]: ...
    @overload
    def described_groups(self: DescribeConsumerGroupsResult[Async]
                         ) -> dict[str, AsyncKafkaFuture[ConsumerGroupDescription]]: ...
    @overload
    def all(self: DescribeConsumerGroupsResult[Sync]
            ) -> KafkaFuture[dict[str, ConsumerGroupDescription]]: ...
    @overload
    def all(self: DescribeConsumerGroupsResult[Async]
            ) -> AsyncKafkaFuture[dict[str, ConsumerGroupDescription]]: ...
