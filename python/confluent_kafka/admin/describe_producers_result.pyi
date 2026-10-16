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

# Stub for confluent_kafka.admin.DescribeProducersResult.

from collections.abc import Mapping, Sequence
from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.admin.producer_state import ProducerState
from confluent_kafka.common import TopicPartition
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeProducersResult"]

class DescribeProducersResult(Generic[KindT]):
    class PartitionProducerState:
        def __init__(self, *, active_producers: Sequence[ProducerState]) -> None: ...
        def active_producers(self) -> list[ProducerState]: ...

    @classmethod
    def _new(cls, *,
             futures: Mapping[TopicPartition, KafkaFuture[DescribeProducersResult.PartitionProducerState] | AsyncKafkaFuture[DescribeProducersResult.PartitionProducerState]],
             kind: type[KindT]) -> DescribeProducersResult[KindT]: ...
    @overload
    def partition_result(self: DescribeProducersResult[Sync], *, partition: TopicPartition
                         ) -> KafkaFuture[DescribeProducersResult.PartitionProducerState]: ...
    @overload
    def partition_result(self: DescribeProducersResult[Async], *, partition: TopicPartition
                         ) -> AsyncKafkaFuture[DescribeProducersResult.PartitionProducerState]: ...
    @overload
    def all(self: DescribeProducersResult[Sync]
            ) -> KafkaFuture[dict[TopicPartition, DescribeProducersResult.PartitionProducerState]]: ...
    @overload
    def all(self: DescribeProducersResult[Async]
            ) -> AsyncKafkaFuture[dict[TopicPartition,
                                       DescribeProducersResult.PartitionProducerState]]: ...
