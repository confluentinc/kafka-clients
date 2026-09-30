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

# GENERATED, DO NOT EDIT (stub for confluent_kafka.consumer.LogTruncationError).

from collections.abc import Mapping
from typing import ClassVar

from confluent_kafka.common.topic_partition import TopicPartition
from confluent_kafka.consumer.offset_and_metadata import OffsetAndMetadata
from confluent_kafka.consumer.offset_out_of_range_error import OffsetOutOfRangeError

__all__ = ["LogTruncationError"]

class LogTruncationError(OffsetOutOfRangeError):
    _ffi_id: ClassVar[int]
    def __init__(self, *, message: str | None = None, fetch_offsets: Mapping[TopicPartition, int], divergent_offsets: Mapping[TopicPartition, OffsetAndMetadata]) -> None: ...
    def divergent_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]: ...
