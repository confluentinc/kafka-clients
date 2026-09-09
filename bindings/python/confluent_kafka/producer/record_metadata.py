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

"""``RecordMetadata`` — the broker's acknowledgement of one published record.

Translated from ``org.apache.kafka.clients.producer.RecordMetadata`` (Apache
Kafka 4.3.1). Not on the send path, so pure Python. ``offset`` /
``timestamp`` use Java's ``-1`` sentinel with a paired ``has_offset()`` /
``has_timestamp()`` predicate (rule 3.8), not an ``Optional``. Java defines no
``equals``/``hashCode`` (identity), so neither does this.
"""

from __future__ import annotations

from confluent_kafka.common.topic_partition import TopicPartition

# Java: ProduceResponse.INVALID_OFFSET and RecordBatch.NO_TIMESTAMP are both -1.
_INVALID_OFFSET = -1
_NO_TIMESTAMP = -1


class RecordMetadata:
    """The broker's acknowledgement of one published record.

    Java: ``org.apache.kafka.clients.producer.RecordMetadata``
    (``RecordMetadata(TopicPartition topicPartition, long baseOffset,
    int batchIndex, long timestamp, int serializedKeySize,
    int serializedValueSize)``).
    """

    __slots__ = ("_topic_partition", "_offset", "_timestamp",
                 "_serialized_key_size", "_serialized_value_size")

    def __init__(self, *, topic_partition: TopicPartition, base_offset: int,
                 batch_index: int, timestamp: int,
                 serialized_key_size: int,
                 serialized_value_size: int) -> None:
        # Java: ignore batchIndex when baseOffset is -1 (offset unknown).
        self._offset = (base_offset if base_offset == _INVALID_OFFSET
                        else base_offset + batch_index)
        self._timestamp = timestamp
        self._serialized_key_size = serialized_key_size
        self._serialized_value_size = serialized_value_size
        self._topic_partition = topic_partition

    @classmethod
    def _from_ffi(cls, *, topic: str, partition: int, offset: int,
                  timestamp: int, serialized_key_size: int,
                  serialized_value_size: int) -> RecordMetadata:
        """Build a ``RecordMetadata`` from the fields the C FFI hands back.

        The core's ``RecordMetadata.offset()`` already combines ``baseOffset +
        batchIndex`` (or leaves the ``-1`` sentinel when the offset is unknown),
        so the FFI exposes the single combined ``offset``. We reproduce it as
        ``base_offset=offset, batch_index=0``: when ``offset == -1`` the ``__init__``
        keeps the sentinel; otherwise ``offset + 0 == offset``.
        """
        self = cls.__new__(cls)
        self._offset = offset
        self._timestamp = timestamp
        self._serialized_key_size = serialized_key_size
        self._serialized_value_size = serialized_value_size
        self._topic_partition = TopicPartition(topic=topic, partition=partition)
        return self

    def topic(self) -> str:
        return self._topic_partition.topic()

    def partition(self) -> int:
        return self._topic_partition.partition()

    def has_offset(self) -> bool:
        """Java ``hasOffset()`` — true iff the offset is included (``!= -1``)."""
        return self._offset != _INVALID_OFFSET

    def offset(self) -> int:
        """The record offset, or ``-1`` when ``has_offset()`` is False (Java's
        sentinel, e.g. ``acks=0``)."""
        return self._offset

    def has_timestamp(self) -> bool:
        """Java ``hasTimestamp()`` — true iff a valid timestamp exists."""
        return self._timestamp != _NO_TIMESTAMP

    def timestamp(self) -> int:
        """The record timestamp, or ``-1`` when ``has_timestamp()`` is False."""
        return self._timestamp

    def serialized_key_size(self) -> int:
        return self._serialized_key_size

    def serialized_value_size(self) -> int:
        return self._serialized_value_size

    def __repr__(self) -> str:
        # Java toString: topicPartition + "@" + offset
        return f"{self._topic_partition}@{self._offset}"
