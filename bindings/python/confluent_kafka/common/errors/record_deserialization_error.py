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

"""``RecordDeserializationError``: Java's ``org.apache.kafka.common.errors.RecordDeserializationException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

import enum
from collections.abc import Iterable
from typing import ClassVar, TYPE_CHECKING

from confluent_kafka import _throwable
from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka.common.errors.serialization_error import SerializationError
from confluent_kafka.common.timestamp_type import TimestampType

if TYPE_CHECKING:
    from confluent_kafka.common.headers import Headers
    from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["RecordDeserializationError"]


class RecordDeserializationError(SerializationError):
    """This exception is raised for any error that occurs while deserializing
    records received by the consumer using the configured
    ``org.apache.kafka.common.serialization.Deserializer``.

    Java: ``org.apache.kafka.common.errors.RecordDeserializationException``.

    Deprecated: the constructor form ``(partition, offset, message, cause)``.
    Since 3.9. Use
    ``RecordDeserializationException(DeserializationExceptionOrigin,
    TopicPartition, long, long, TimestampType, ByteBuffer, ByteBuffer, Headers,
    String, Throwable)`` instead.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = -27  # kafka_common_ErrorCode_RECORD_DESERIALIZATION

    class DeserializationExceptionOrigin(enum.Enum):
        """Java's nested enum ``RecordDeserializationException.DeserializationExceptionOrigin``."""

        KEY = "KEY"
        VALUE = "VALUE"

    @java_forms(
        Form("partition", "offset", "message", "cause", deprecated="Since 3.9. Use ``RecordDeserializationException(DeserializationExceptionOrigin, TopicPartition, long, long, TimestampType, ByteBuffer, ByteBuffer, Headers, String, Throwable)`` instead."),
        Form("origin", "partition", "offset", "timestamp", "timestamp_type", "key_buffer", "value_buffer", "headers", "message", "cause"),
    )
    def __init__(
        self,
        *,
        origin: RecordDeserializationError.DeserializationExceptionOrigin | None = UNSET,
        partition: TopicPartition,
        offset: int,
        timestamp: int = UNSET,
        timestamp_type: TimestampType = UNSET,
        key_buffer: bytes | None = UNSET,
        value_buffer: bytes | None = UNSET,
        headers: Iterable[tuple[str, bytes | bytearray | memoryview | None]] = UNSET,
        message: str,
        cause: BaseException | None = None,
        _java_form: int = -1,
    ) -> None:
        headers = _throwable.materialize(headers)
        if _java_form == 0:
            _throwable.init(self, message, cause)
            self._origin = None
            self._partition = partition
            self._offset = offset
            self._timestamp_type = TimestampType.NO_TIMESTAMP_TYPE
            self._timestamp = -1
            self._key_buffer = None
            self._value_buffer = None
            self._headers = None
        else:
            _throwable.init(self, message, cause)
            self._origin = origin
            self._partition = partition
            self._offset = offset
            self._timestamp_type = timestamp_type
            self._timestamp = timestamp
            self._key_buffer = _throwable.view(key_buffer)
            self._value_buffer = _throwable.view(value_buffer)
            self._headers = _throwable.headers(headers)
        self._java_kwargs = _throwable.kwargs(origin=origin, partition=partition, offset=offset, timestamp=timestamp, timestamp_type=timestamp_type, key_buffer=key_buffer, value_buffer=value_buffer, headers=headers, message=message, cause=cause)

    def origin(self) -> RecordDeserializationError.DeserializationExceptionOrigin | None:
        """Java's ``origin()``."""
        return self._origin

    def topic_partition(self) -> TopicPartition:
        """Java's ``topicPartition()``."""
        return self._partition

    def offset(self) -> int:
        """Java's ``offset()``."""
        return self._offset

    def timestamp_type(self) -> TimestampType:
        """Java's ``timestampType()``."""
        return self._timestamp_type

    def timestamp(self) -> int:
        """Java's ``timestamp()``."""
        return self._timestamp

    def key_buffer(self) -> memoryview | None:
        """Java's ``keyBuffer()``."""
        return self._key_buffer

    def value_buffer(self) -> memoryview | None:
        """Java's ``valueBuffer()``."""
        return self._value_buffer

    def headers(self) -> Headers | None:
        """Java's ``headers()``."""
        return self._headers
