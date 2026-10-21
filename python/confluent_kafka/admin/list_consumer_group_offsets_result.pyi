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

# Stub for confluent_kafka.admin.ListConsumerGroupOffsetsResult.

from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.admin._internals import _CoordinatorKey
from confluent_kafka.common import TopicPartition
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture
from confluent_kafka.consumer import OffsetAndMetadata

__all__ = ["ListConsumerGroupOffsetsResult"]

class ListConsumerGroupOffsetsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *,
             futures: Mapping[_CoordinatorKey,
                              KafkaFuture[dict[TopicPartition, OffsetAndMetadata | None]] | AsyncKafkaFuture[dict[TopicPartition, OffsetAndMetadata | None]]],
             kind: type[KindT]) -> ListConsumerGroupOffsetsResult[KindT]: ...
    @overload
    def partitions_to_offset_and_metadata(
        self: ListConsumerGroupOffsetsResult[Sync], *, group_id: str | None = None
    ) -> KafkaFuture[dict[TopicPartition, OffsetAndMetadata | None]]: ...
    @overload
    def partitions_to_offset_and_metadata(
        self: ListConsumerGroupOffsetsResult[Async], *, group_id: str | None = None
    ) -> AsyncKafkaFuture[dict[TopicPartition, OffsetAndMetadata | None]]: ...
    @overload
    def all(self: ListConsumerGroupOffsetsResult[Sync]
            ) -> KafkaFuture[dict[str, dict[TopicPartition,
                                                          OffsetAndMetadata | None]]]: ...
    @overload
    def all(self: ListConsumerGroupOffsetsResult[Async]
            ) -> AsyncKafkaFuture[dict[str, dict[TopicPartition, OffsetAndMetadata | None]]]: ...
