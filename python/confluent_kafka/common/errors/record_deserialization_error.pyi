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

# GENERATED, DO NOT EDIT (stub for confluent_kafka.common.errors.RecordDeserializationError).

import enum
from collections.abc import Iterable
from typing import ClassVar, overload

from confluent_kafka.common.errors.serialization_error import SerializationError
from confluent_kafka.common.headers import Headers
from confluent_kafka.common.timestamp_type import TimestampType
from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["RecordDeserializationError"]

class RecordDeserializationError(SerializationError):
    _ffi_id: ClassVar[int]
    class DeserializationExceptionOrigin(enum.Enum):
        KEY = "KEY"
        VALUE = "VALUE"
    @overload
    def __init__(self, *, origin: RecordDeserializationError.DeserializationExceptionOrigin | None, partition: TopicPartition, offset: int, timestamp: int, timestamp_type: TimestampType, key_buffer: bytes | None, value_buffer: bytes | None, headers: Iterable[tuple[str, bytes | bytearray | memoryview | None]], message: str | None, cause: BaseException | None = None) -> None: ...
    @overload
    def __init__(self, *, partition: TopicPartition, offset: int, message: str | None, cause: BaseException | None = None) -> None: ...
    def origin(self) -> RecordDeserializationError.DeserializationExceptionOrigin | None: ...
    def topic_partition(self) -> TopicPartition: ...
    def offset(self) -> int: ...
    def timestamp_type(self) -> TimestampType: ...
    def timestamp(self) -> int: ...
    def key_buffer(self) -> memoryview | None: ...
    def value_buffer(self) -> memoryview | None: ...
    def headers(self) -> Headers | None: ...
