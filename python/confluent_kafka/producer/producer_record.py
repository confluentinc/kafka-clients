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

"""``ProducerRecord``: Java's ``org.apache.kafka.clients.producer.ProducerRecord``.

Java's six constructors are one keyword-only constructor matched by
``java_forms`` (CLAUDE.md, Python Binding Conventions, Signatures): the given
arguments must be one of them, and the shorter ones delegate to the longest,
passing ``null`` for what they leave out. ``partition`` and ``key`` default to
``UNSET``, so an explicit ``None`` is given: ``ProducerRecord(topic=…,
partition=0, key=None, value=…)`` is Java's ``(topic, partition, key, value)``
with a null key, and ``(topic, partition, value)`` without the key is rejected.
The record is covariant in ``K`` and ``V``; one stub per constructor and
key / value binding binds an omitted or ``None`` key or value to ``Never``, so
``ProducerRecord(topic="t", value="v")`` is a ``ProducerRecord[Never, str]``
and fits any producer with ``str`` values. Header values are viewed, not copied
(CLAUDE.md §12).
"""

from __future__ import annotations

import sys
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.common.headers import (
    Headers,
    _hand_out,
    _headers_hash,
    _headers_to_string,
    _read_headers,
)
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Never
    else:
        from typing_extensions import Never

__all__ = ["ProducerRecord"]

K_co = TypeVar("K_co", covariant=True)
V_co = TypeVar("V_co", covariant=True)
_K = TypeVar("_K")
_V = TypeVar("_V")

_WrittenHeaders = Iterable[tuple[str, "bytes | bytearray | memoryview | None"]]

# Java's six constructors, in Java's order; the shorter ones pass null to the
# first for what they leave out (a null header list is the empty ``()``).
_FORMS = (
    Form("topic", "partition", "timestamp", "key", "value", "headers",
         defaults={"partition": None, "timestamp": None, "key": None, "headers": ()}),
    Form("topic", "partition", "timestamp", "key", "value"),
    Form("topic", "partition", "key", "value", "headers"),
    Form("topic", "partition", "key", "value"),
    Form("topic", "key", "value"),
    Form("topic", "value"),
)


class ProducerRecord(Generic[K_co, V_co]):
    """A key/value pair to be sent to Kafka. This consists of a topic name to
    which the record is being sent, an optional partition number, and an
    optional key and value.

    If a valid partition number is specified that partition will be used when
    sending the record. If no partition is specified but a key is present a
    partition will be chosen using a hash of the key. If neither key nor
    partition is present a partition will be assigned in a round-robin fashion.
    Note that partition numbers are 0-indexed.

    The record also has an associated timestamp. If the user did not provide a
    timestamp, the producer will stamp the record with its current time. The
    timestamp eventually used by Kafka depends on the timestamp type configured
    for the topic: with ``CreateTime`` the timestamp in the producer record is
    used by the broker; with ``LogAppendTime`` it is overwritten by the broker
    with the broker local time when it appends the message to its log. In
    either case, the timestamp that has actually been used is returned in
    ``RecordMetadata``.

    Java: ``org.apache.kafka.clients.producer.ProducerRecord<K, V>``.
    """

    __slots__ = ("_topic", "_partition", "_headers", "_key", "_value", "_timestamp")

    @overload
    def __init__(self: ProducerRecord[Never, Never], *, topic: str, partition: int | None,
                 timestamp: int | None, key: None, value: None,
                 headers: _WrittenHeaders) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, Never], *, topic: str, partition: int | None,
                 timestamp: int | None, key: _K, value: None, headers: _WrittenHeaders) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, _V], *, topic: str, partition: int | None,
                 timestamp: int | None, key: None, value: _V, headers: _WrittenHeaders) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, _V], *, topic: str, partition: int | None,
                 timestamp: int | None, key: _K, value: _V, headers: _WrittenHeaders) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, Never], *, topic: str, partition: int | None,
                 timestamp: int | None, key: None, value: None) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, Never], *, topic: str, partition: int | None,
                 timestamp: int | None, key: _K, value: None) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, _V], *, topic: str, partition: int | None,
                 timestamp: int | None, key: None, value: _V) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, _V], *, topic: str, partition: int | None,
                 timestamp: int | None, key: _K, value: _V) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, Never], *, topic: str, partition: int | None,
                 key: None, value: None, headers: _WrittenHeaders) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, Never], *, topic: str, partition: int | None, key: _K,
                 value: None, headers: _WrittenHeaders) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, _V], *, topic: str, partition: int | None, key: None,
                 value: _V, headers: _WrittenHeaders) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, _V], *, topic: str, partition: int | None, key: _K,
                 value: _V, headers: _WrittenHeaders) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, Never], *, topic: str, partition: int | None,
                 key: None, value: None) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, Never], *, topic: str, partition: int | None, key: _K,
                 value: None) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, _V], *, topic: str, partition: int | None, key: None,
                 value: _V) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, _V], *, topic: str, partition: int | None, key: _K,
                 value: _V) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, Never], *, topic: str, key: None,
                 value: None) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, Never], *, topic: str, key: _K, value: None) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, _V], *, topic: str, key: None, value: _V) -> None: ...
    @overload
    def __init__(self: ProducerRecord[_K, _V], *, topic: str, key: _K, value: _V) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, Never], *, topic: str, value: None) -> None: ...
    @overload
    def __init__(self: ProducerRecord[Never, _V], *, topic: str, value: _V) -> None: ...

    @java_forms(*_FORMS)
    def __init__(self, *, topic: str, partition: int | None = UNSET,
                 timestamp: int | None = None, key: Any = UNSET, value: Any,
                 headers: _WrittenHeaders = ()) -> None:
        """Creates a record to be sent to a topic, and optionally to a
        partition, with a timestamp in milliseconds since epoch (if ``None``,
        the producer assigns it), a key, the value and the headers."""
        if topic is None:
            raise IllegalArgumentError(message="Topic cannot be null.")
        if timestamp is not None and timestamp < 0:
            raise IllegalArgumentError(
                message=f"Invalid timestamp: {timestamp}. Timestamp should always be "
                "non-negative or null.")
        if partition is not None and partition < 0:
            raise IllegalArgumentError(
                message=f"Invalid partition: {partition}. Partition number should always "
                "be non-negative or null.")
        self._topic = topic
        self._partition = partition
        self._key: K_co | None = key
        self._value: V_co | None = value
        self._timestamp = timestamp
        self._headers = _read_headers(headers)

    def topic(self) -> str:
        """The topic this record is being sent to."""
        return self._topic

    def headers(self) -> Headers:
        """The headers."""
        return _hand_out(self._headers)

    def key(self) -> K_co | None:
        """The key (or ``None`` if no key is specified)."""
        return self._key

    def value(self) -> V_co | None:
        """The value."""
        return self._value

    def timestamp(self) -> int | None:
        """The timestamp, which is in milliseconds since epoch."""
        return self._timestamp

    def partition(self) -> int | None:
        """The partition to which the record will be sent (or ``None`` if no
        partition was specified)."""
        return self._partition

    def __str__(self) -> str:
        return ("ProducerRecord(topic=" + self._topic + ", partition="
                + java_str(self._partition) + ", headers=" + _headers_to_string(self._headers)
                + ", key=" + java_str(self._key) + ", value=" + java_str(self._value)
                + ", timestamp=" + java_str(self._timestamp) + ")")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, ProducerRecord):
            return NotImplemented
        return (self._key == other._key and self._partition == other._partition
                and self._topic == other._topic and self._headers == other._headers
                and self._value == other._value and self._timestamp == other._timestamp)

    def __hash__(self) -> int:
        return hash((self._topic, self._partition, _headers_hash(self._headers), self._key,
                     self._value, self._timestamp))
