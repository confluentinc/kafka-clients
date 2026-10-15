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

# GENERATED, DO NOT EDIT (stub for confluent_kafka.consumer.InvalidOffsetError).

from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["InvalidOffsetError"]

class InvalidOffsetError(KafkaError):
    def __init__(self, *, message: str) -> None: ...
    def partitions(self) -> set[TopicPartition]: ...
