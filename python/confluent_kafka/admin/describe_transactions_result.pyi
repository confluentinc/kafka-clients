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

# Stub for confluent_kafka.admin.DescribeTransactionsResult.

from collections.abc import Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.admin._internals import _CoordinatorKey
from confluent_kafka.admin.transaction_description import TransactionDescription
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeTransactionsResult"]

class DescribeTransactionsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *,
             futures: Mapping[_CoordinatorKey, KafkaFuture[TransactionDescription] | AsyncKafkaFuture[TransactionDescription]],
             kind: type[KindT]) -> DescribeTransactionsResult[KindT]: ...
    @overload
    def description(self: DescribeTransactionsResult[Sync], *, transactional_id: str
                    ) -> KafkaFuture[TransactionDescription]: ...
    @overload
    def description(self: DescribeTransactionsResult[Async], *, transactional_id: str
                    ) -> AsyncKafkaFuture[TransactionDescription]: ...
    @overload
    def all(self: DescribeTransactionsResult[Sync]
            ) -> KafkaFuture[dict[str, TransactionDescription]]: ...
    @overload
    def all(self: DescribeTransactionsResult[Async]
            ) -> AsyncKafkaFuture[dict[str, TransactionDescription]]: ...
