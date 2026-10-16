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

# Stub for confluent_kafka.admin.DeleteTopicsResult.

from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.common import Uuid
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DeleteTopicsResult"]

class DeleteTopicsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *,
             topic_id_futures: Mapping[Uuid, KafkaFuture[None] | AsyncKafkaFuture[None]] | None,
             name_futures: Mapping[str, KafkaFuture[None] | AsyncKafkaFuture[None]] | None,
             kind: type[KindT]) -> DeleteTopicsResult[KindT]: ...
    @staticmethod
    def _of_topic_ids(*,
                      topic_id_futures: Mapping[Uuid, KafkaFuture[None] | AsyncKafkaFuture[None]],
                      kind: type[KindT]) -> DeleteTopicsResult[KindT]: ...
    @staticmethod
    def _of_topic_names(*,
                        name_futures: Mapping[str, KafkaFuture[None] | AsyncKafkaFuture[None]],
                        kind: type[KindT]) -> DeleteTopicsResult[KindT]: ...
    @overload
    def topic_id_values(self: DeleteTopicsResult[Sync]) -> dict[Uuid, KafkaFuture[None]] | None: ...
    @overload
    def topic_id_values(self: DeleteTopicsResult[Async]
                        ) -> dict[Uuid, AsyncKafkaFuture[None]] | None: ...
    @overload
    def topic_name_values(self: DeleteTopicsResult[Sync]) -> dict[str, KafkaFuture[None]] | None: ...
    @overload
    def topic_name_values(self: DeleteTopicsResult[Async]
                          ) -> dict[str, AsyncKafkaFuture[None]] | None: ...
    @overload
    def all(self: DeleteTopicsResult[Sync]) -> KafkaFuture[None]: ...
    @overload
    def all(self: DeleteTopicsResult[Async]) -> AsyncKafkaFuture[None]: ...
