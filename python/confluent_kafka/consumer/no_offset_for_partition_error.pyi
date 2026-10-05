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

# GENERATED, DO NOT EDIT (stub for confluent_kafka.consumer.NoOffsetForPartitionError).

from collections.abc import Iterable
from typing import ClassVar, overload

from confluent_kafka.common.topic_partition import TopicPartition
from confluent_kafka.consumer.invalid_offset_error import InvalidOffsetError

__all__ = ["NoOffsetForPartitionError"]

class NoOffsetForPartitionError(InvalidOffsetError):
    _ffi_id: ClassVar[int]
    @overload
    def __init__(self, *, partition: TopicPartition) -> None: ...
    @overload
    def __init__(self, *, partitions: Iterable[TopicPartition]) -> None: ...
    def partitions(self) -> set[TopicPartition]: ...
