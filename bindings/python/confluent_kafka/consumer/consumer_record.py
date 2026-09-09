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

"""``ConsumerRecord`` — one consumed record.

Translated from ``org.apache.kafka.clients.consumer.ConsumerRecord`` (Apache
Kafka 4.3.1). Java's three constructors collapse to one keyword-only
constructor whose defaults are the short overload's delegation values
(``timestamp=NO_TIMESTAMP=-1``, ``timestamp_type=NO_TIMESTAMP_TYPE``,
``serialized_key_size=serialized_value_size=NULL_SIZE=-1``, empty headers,
``leader_epoch=None``, ``delivery_count=None``). Java null-checks ``topic`` and
``headers`` with ``IllegalArgumentException`` — the binding raises
``IllegalArgumentError`` with the same messages. Immutable; accessor methods
(D16). Java defines no ``equals``/``hashCode`` (identity), so neither does this.
"""

from __future__ import annotations

from typing import Generic, TypeVar

from confluent_kafka import IllegalArgumentError

from confluent_kafka.common.headers import Headers, validate_written_headers
from confluent_kafka.common.timestamp_type import TimestampType

K = TypeVar("K")
V = TypeVar("V")

# Java: ConsumerRecord.NO_TIMESTAMP (== RecordBatch.NO_TIMESTAMP) and NULL_SIZE.
NO_TIMESTAMP = -1
NULL_SIZE = -1


class ConsumerRecord(Generic[K, V]):
    """One consumed record.

    Java: ``org.apache.kafka.clients.consumer.ConsumerRecord<K, V>``.
    ``key()``/``value()`` return deserializer output.
    """

    __slots__ = ("_topic", "_partition", "_offset", "_timestamp",
                 "_timestamp_type", "_serialized_key_size",
                 "_serialized_value_size", "_key", "_value", "_headers",
                 "_leader_epoch", "_delivery_count")

    def __init__(self, *, topic: str, partition: int, offset: int,
                 timestamp: int = NO_TIMESTAMP,
                 timestamp_type: TimestampType = TimestampType.NO_TIMESTAMP_TYPE,
                 serialized_key_size: int = NULL_SIZE,
                 serialized_value_size: int = NULL_SIZE,
                 key: K | None, value: V | None,
                 headers: Headers = (),
                 leader_epoch: int | None = None,
                 delivery_count: int | None = None) -> None:
        if topic is None:
            raise IllegalArgumentError("Topic cannot be null")
        if headers is None:
            raise IllegalArgumentError("Headers cannot be null")
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp
        self._timestamp_type = timestamp_type
        self._serialized_key_size = serialized_key_size
        self._serialized_value_size = serialized_value_size
        self._key = key
        self._value = value
        # Headers are validated/normalized to the owned tuple form; on the read
        # path the fetch layer supplies memoryview values (§5.2).
        self._headers: Headers = validate_written_headers(headers)  # type: ignore[assignment]
        self._leader_epoch = leader_epoch
        self._delivery_count = delivery_count

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> int:
        return self._timestamp

    def timestamp_type(self) -> TimestampType:
        return self._timestamp_type

    def key(self) -> K | None:
        return self._key

    def value(self) -> V | None:
        return self._value

    def headers(self) -> Headers:
        return self._headers

    def serialized_key_size(self) -> int:
        return self._serialized_key_size

    def serialized_value_size(self) -> int:
        return self._serialized_value_size

    def leader_epoch(self) -> int | None:
        return self._leader_epoch

    def delivery_count(self) -> int | None:
        """The delivery count for the record if available (share consumer
        only). Java ``Optional<Short> deliveryCount()``."""
        return self._delivery_count

    def __repr__(self) -> str:
        return ("ConsumerRecord(topic = " + self._topic
                + ", partition = " + str(self._partition)
                + ", leaderEpoch = " + str(self._leader_epoch)
                + ", offset = " + str(self._offset)
                + ", " + str(self._timestamp_type) + " = " + str(self._timestamp)
                + ", deliveryCount = " + str(self._delivery_count)
                + ", serialized key size = " + str(self._serialized_key_size)
                + ", serialized value size = " + str(self._serialized_value_size)
                + ", headers = " + str(self._headers)
                + ", key = " + str(self._key)
                + ", value = " + str(self._value) + ")")
