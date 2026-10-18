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

# Stub for confluent_kafka.admin.DescribeAclsResult.

from typing import Generic, overload

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.common import AclBinding
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeAclsResult"]

class DescribeAclsResult(Generic[KindT]):
    @classmethod
    def _new(cls, *,
             future: KafkaFuture[list[AclBinding]] | AsyncKafkaFuture[list[AclBinding]],
             kind: type[KindT]) -> DescribeAclsResult[KindT]: ...
    @overload
    def values(self: DescribeAclsResult[Sync]) -> KafkaFuture[list[AclBinding]]: ...
    @overload
    def values(self: DescribeAclsResult[Async]) -> AsyncKafkaFuture[list[AclBinding]]: ...
