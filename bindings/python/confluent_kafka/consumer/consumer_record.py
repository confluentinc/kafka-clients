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

"""``ConsumerRecord``: Java's ``org.apache.kafka.clients.consumer.ConsumerRecord``.

Java's three constructors are one keyword-only constructor matched by
``java_forms`` (CLAUDE.md, Python Binding Conventions, Signatures). The short
``(topic, partition, offset, key, value)`` passes ``NO_TIMESTAMP``,
``NO_TIMESTAMP_TYPE``, ``NULL_SIZE``, empty headers and no leader epoch to the
11-argument form, which passes no delivery count to the full form. The record
is covariant in ``K`` and ``V``; the stubs bind a ``None`` key or value to
``Never``. Java defines no ``equals`` / ``hashCode``, so neither does this.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, overload

from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.common.headers import Headers, _headers_to_string, _read_headers
from confluent_kafka.common.timestamp_type import TimestampType
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Never
    else:
        from typing_extensions import Never

__all__ = ["ConsumerRecord"]

K_co = TypeVar("K_co", covariant=True)
V_co = TypeVar("V_co", covariant=True)
_K = TypeVar("_K")
_V = TypeVar("_V")

_WrittenHeaders = Iterable[tuple[str, "bytes | bytearray | memoryview | None"]]

_NO_TIMESTAMP = -1  # RecordBatch.NO_TIMESTAMP
_NULL_SIZE = -1


class ConsumerRecord(Generic[K_co, V_co]):
    """A key/value pair to be received from Kafka. This also consists of a
    topic name and a partition number from which the record is being received,
    an offset that points to the record in a Kafka partition, and a timestamp
    as marked by the corresponding ProducerRecord.

    Java: ``org.apache.kafka.clients.consumer.ConsumerRecord<K, V>``.
    """

    NO_TIMESTAMP: ClassVar[int] = _NO_TIMESTAMP
    NULL_SIZE: ClassVar[int] = _NULL_SIZE

    __slots__ = ("_topic", "_partition", "_offset", "_timestamp", "_timestamp_type",
                 "_serialized_key_size", "_serialized_value_size", "_headers", "_key",
                 "_value", "_leader_epoch", "_delivery_count")

    @overload
    def __init__(self: ConsumerRecord[Never, Never], *, topic: str, partition: int,
                 offset: int, key: None, value: None) -> None: ...
    @overload
    def __init__(self: ConsumerRecord[_K, Never], *, topic: str, partition: int,
                 offset: int, key: _K, value: None) -> None: ...
    @overload
    def __init__(self: ConsumerRecord[Never, _V], *, topic: str, partition: int,
                 offset: int, key: None, value: _V) -> None: ...
    @overload
    def __init__(self: ConsumerRecord[_K, _V], *, topic: str, partition: int,
                 offset: int, key: _K, value: _V) -> None: ...
    @overload
    def __init__(self: ConsumerRecord[Never, Never], *, topic: str, partition: int,
                 offset: int, timestamp: int, timestamp_type: TimestampType,
                 serialized_key_size: int, serialized_value_size: int, key: None,
                 value: None, headers: _WrittenHeaders, leader_epoch: int | None,
                 delivery_count: int | None = None) -> None: ...
    @overload
    def __init__(self: ConsumerRecord[_K, Never], *, topic: str, partition: int,
                 offset: int, timestamp: int, timestamp_type: TimestampType,
                 serialized_key_size: int, serialized_value_size: int, key: _K,
                 value: None, headers: _WrittenHeaders, leader_epoch: int | None,
                 delivery_count: int | None = None) -> None: ...
    @overload
    def __init__(self: ConsumerRecord[Never, _V], *, topic: str, partition: int,
                 offset: int, timestamp: int, timestamp_type: TimestampType,
                 serialized_key_size: int, serialized_value_size: int, key: None,
                 value: _V, headers: _WrittenHeaders, leader_epoch: int | None,
                 delivery_count: int | None = None) -> None: ...
    @overload
    def __init__(self: ConsumerRecord[_K, _V], *, topic: str, partition: int,
                 offset: int, timestamp: int, timestamp_type: TimestampType,
                 serialized_key_size: int, serialized_value_size: int, key: _K,
                 value: _V, headers: _WrittenHeaders, leader_epoch: int | None,
                 delivery_count: int | None = None) -> None: ...

    @java_forms(
        Form("topic", "partition", "offset", "key", "value"),
        Form("topic", "partition", "offset", "timestamp", "timestamp_type",
             "serialized_key_size", "serialized_value_size", "key", "value", "headers",
             "leader_epoch",
             defaults={"timestamp": _NO_TIMESTAMP,
                       "timestamp_type": TimestampType.NO_TIMESTAMP_TYPE,
                       "serialized_key_size": _NULL_SIZE,
                       "serialized_value_size": _NULL_SIZE,
                       "headers": (), "leader_epoch": None}),
        Form("topic", "partition", "offset", "timestamp", "timestamp_type",
             "serialized_key_size", "serialized_value_size", "key", "value", "headers",
             "leader_epoch", "delivery_count", defaults={"delivery_count": None}),
    )
    def __init__(self, *, topic: str, partition: int, offset: int,
                 timestamp: int = UNSET, timestamp_type: TimestampType = UNSET,
                 serialized_key_size: int = UNSET, serialized_value_size: int = UNSET,
                 key: Any, value: Any, headers: _WrittenHeaders = UNSET,
                 leader_epoch: int | None = UNSET,
                 delivery_count: int | None = None) -> None:
        """Creates a record to be received from a specified topic and
        partition: its offset, timestamp and timestamp type, the lengths of the
        serialized key and value, the key (``None`` is allowed) and value, the
        headers, the optional leader epoch (may be empty for legacy record
        formats) and the optional delivery count (may be empty when deliveries
        are not counted)."""
        if topic is None:
            raise IllegalArgumentError(message="Topic cannot be null")
        if headers is None:
            raise IllegalArgumentError(message="Headers cannot be null")
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp
        self._timestamp_type = timestamp_type
        self._serialized_key_size = serialized_key_size
        self._serialized_value_size = serialized_value_size
        self._key: K_co | None = key
        self._value: V_co | None = value
        self._headers: Headers = _read_headers(headers)
        self._leader_epoch = leader_epoch
        self._delivery_count = delivery_count

    def topic(self) -> str:
        """The topic this record is received from (never ``None``)."""
        return self._topic

    def partition(self) -> int:
        """The partition from which this record is received."""
        return self._partition

    def headers(self) -> Headers:
        """The headers (never ``None``)."""
        return self._headers

    def key(self) -> K_co | None:
        """The key (or ``None`` if no key is specified)."""
        return self._key

    def value(self) -> V_co | None:
        """The value."""
        return self._value

    def offset(self) -> int:
        """The position of this record in the corresponding Kafka partition."""
        return self._offset

    def timestamp(self) -> int:
        """The timestamp of this record, in milliseconds elapsed since unix
        epoch."""
        return self._timestamp

    def timestamp_type(self) -> TimestampType:
        """The timestamp type of this record."""
        return self._timestamp_type

    def serialized_key_size(self) -> int:
        """The size of the serialized, uncompressed key in bytes. If key is
        ``None``, the returned size is -1."""
        return self._serialized_key_size

    def serialized_value_size(self) -> int:
        """The size of the serialized, uncompressed value in bytes. If value is
        ``None``, the returned size is -1."""
        return self._serialized_value_size

    def leader_epoch(self) -> int | None:
        """Get the leader epoch for the record if available: ``None`` for
        legacy record formats."""
        return self._leader_epoch

    def delivery_count(self) -> int | None:
        """Get the delivery count for the record if available. Deliveries are
        counted for records delivered by share groups; ``None`` when deliveries
        are not counted."""
        return self._delivery_count

    def __str__(self) -> str:
        return ("ConsumerRecord(topic = " + self._topic
                + ", partition = " + str(self._partition)
                + ", leaderEpoch = " + java_str(self._leader_epoch)
                + ", offset = " + str(self._offset)
                + ", " + str(self._timestamp_type) + " = " + str(self._timestamp)
                + ", deliveryCount = " + java_str(self._delivery_count)
                + ", serialized key size = " + str(self._serialized_key_size)
                + ", serialized value size = " + str(self._serialized_value_size)
                + ", headers = " + _headers_to_string(self._headers)
                + ", key = " + java_str(self._key)
                + ", value = " + java_str(self._value) + ")")
