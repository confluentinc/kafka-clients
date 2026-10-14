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

"""``RecordMetadata``: Java's ``org.apache.kafka.clients.producer.RecordMetadata``.

``offset()`` and ``timestamp()`` keep Java's ``-1`` sentinel beside
``has_offset()`` / ``has_timestamp()`` (CLAUDE.md, Python Binding Conventions,
Types). Java defines no ``equals`` / ``hashCode``, so neither does this.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["RecordMetadata"]

# ProduceResponse.INVALID_OFFSET and RecordBatch.NO_TIMESTAMP.
_INVALID_OFFSET = -1
_NO_TIMESTAMP = -1


class RecordMetadata:
    """The metadata for a record that has been acknowledged by the server.

    Java: ``org.apache.kafka.clients.producer.RecordMetadata`` (``final``).
    """

    #: Partition value for record without partition assigned.
    UNKNOWN_PARTITION: ClassVar[int] = -1

    __slots__ = ("_offset", "_timestamp", "_serialized_key_size",
                 "_serialized_value_size", "_topic_partition")

    def __init__(self, *, topic_partition: TopicPartition, base_offset: int,
                 batch_index: int, timestamp: int, serialized_key_size: int,
                 serialized_value_size: int) -> None:
        """Creates a new instance with the provided parameters."""
        # ignore the batchIndex if the base offset is -1, since this indicates
        # the offset is unknown
        self._offset = base_offset if base_offset == -1 else base_offset + batch_index
        self._timestamp = timestamp
        self._serialized_key_size = serialized_key_size
        self._serialized_value_size = serialized_value_size
        self._topic_partition = topic_partition

    @classmethod
    def _from_ffi(cls, *, topic: str, partition: int, offset: int, timestamp: int,
                  serialized_key_size: int, serialized_value_size: int) -> RecordMetadata:
        """The metadata the FFI reports: the core already combines
        ``baseOffset + batchIndex`` into ``offset``."""
        return cls(topic_partition=TopicPartition(topic=topic, partition=partition),
                   base_offset=offset, batch_index=0, timestamp=timestamp,
                   serialized_key_size=serialized_key_size,
                   serialized_value_size=serialized_value_size)

    def has_offset(self) -> bool:
        """Indicates whether the record metadata includes the offset."""
        return self._offset != _INVALID_OFFSET

    def offset(self) -> int:
        """The offset of the record in the topic/partition, or -1 if
        ``has_offset()`` returns ``False``."""
        return self._offset

    def has_timestamp(self) -> bool:
        """Indicates whether the record metadata includes the timestamp."""
        return self._timestamp != _NO_TIMESTAMP

    def timestamp(self) -> int:
        """The timestamp of the record in the topic/partition, or -1 if
        ``has_timestamp()`` returns ``False``."""
        return self._timestamp

    def serialized_key_size(self) -> int:
        """The size of the serialized, uncompressed key in bytes. If key is
        ``None``, the returned size is -1."""
        return self._serialized_key_size

    def serialized_value_size(self) -> int:
        """The size of the serialized, uncompressed value in bytes. If value is
        ``None``, the returned size is -1."""
        return self._serialized_value_size

    def topic(self) -> str:
        """The topic the record was appended to."""
        return self._topic_partition.topic()

    def partition(self) -> int:
        """The partition the record was sent to."""
        return self._topic_partition.partition()

    def __str__(self) -> str:
        return str(self._topic_partition) + "@" + str(self._offset)
