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

# Stub for confluent_kafka.admin.DescribeConfigsResult.

from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.admin.config import Config
from confluent_kafka.common.config import ConfigResource
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeConfigsResult"]

class DescribeConfigsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *,
             futures: Mapping[ConfigResource, KafkaFuture[Config] | AsyncKafkaFuture[Config]],
             kind: type[KindT]) -> DescribeConfigsResult[KindT]: ...
    @overload
    def values(self: DescribeConfigsResult[Sync]) -> dict[ConfigResource, KafkaFuture[Config]]: ...
    @overload
    def values(self: DescribeConfigsResult[Async]
               ) -> dict[ConfigResource, AsyncKafkaFuture[Config]]: ...
    @overload
    def all(self: DescribeConfigsResult[Sync]) -> KafkaFuture[dict[ConfigResource, Config]]: ...
    @overload
    def all(self: DescribeConfigsResult[Async]
            ) -> AsyncKafkaFuture[dict[ConfigResource, Config]]: ...
