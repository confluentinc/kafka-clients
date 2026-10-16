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

# Stub for confluent_kafka.admin.AlterUserScramCredentialsResult.

from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["AlterUserScramCredentialsResult"]

class AlterUserScramCredentialsResult(Generic[KindT]):
    @overload
    def __init__(self: AlterUserScramCredentialsResult[Sync], *,
                 futures: Mapping[str, KafkaFuture[None]]) -> None: ...
    @overload
    def __init__(self: AlterUserScramCredentialsResult[Async], *,
                 futures: Mapping[str, AsyncKafkaFuture[None]]) -> None: ...
    @classmethod
    def _new(cls, *, futures: Mapping[str, KafkaFuture[None] | AsyncKafkaFuture[None]],
             kind: type[KindT]) -> AlterUserScramCredentialsResult[KindT]: ...
    @overload
    def values(self: AlterUserScramCredentialsResult[Sync]
               ) -> dict[str, KafkaFuture[None]]: ...
    @overload
    def values(self: AlterUserScramCredentialsResult[Async]
               ) -> dict[str, AsyncKafkaFuture[None]]: ...
    @overload
    def all(self: AlterUserScramCredentialsResult[Sync]) -> KafkaFuture[None]: ...
    @overload
    def all(self: AlterUserScramCredentialsResult[Async]) -> AsyncKafkaFuture[None]: ...
