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

"""``ProducerRecord`` — a key/value pair to publish.

Translated from ``org.apache.kafka.clients.producer.ProducerRecord`` (Apache
Kafka 4.3.1). Java's six constructors collapse to one keyword-only constructor
(``partition=None``, ``timestamp=None``, ``key=None``, ``headers=()``). Java
null-checks ``topic`` and rejects negative ``partition`` / ``timestamp`` with
``IllegalArgumentException`` — the binding raises ``IllegalArgumentError`` with
the same messages. Value equality + hash over all fields (Java
``equals``/``hashCode``); immutable; accessor methods (D16).

This is the public value type users construct. The zero-copy send path
(implemented in P4) reads ``key()`` / ``value()`` — already ``bytes`` — and
writes them directly into the batch buffer with no intermediate copy, so
holding owned ``bytes`` here does not violate the DoD #10 hot-path rule (the
value type itself performs no per-record copy). See the P2 clarifications entry
for the native-``_confluentkafka.ProducerRecord`` reconciliation deferred to P4.
"""

from __future__ import annotations

from typing import Generic, TypeVar

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common.headers import Headers, validate_written_headers

K = TypeVar("K")
V = TypeVar("V")


class ProducerRecord(Generic[K, V]):
    """A key/value pair to be sent to Kafka.

    Java: ``org.apache.kafka.clients.producer.ProducerRecord<K, V>``.
    """

    __slots__ = ("_topic", "_partition", "_timestamp", "_key", "_value",
                 "_headers")

    def __init__(self, *, topic: str, partition: int | None = None,
                 timestamp: int | None = None, key: K | None = None,
                 value: V | None, headers: Headers = ()) -> None:
        if topic is None:
            raise IllegalArgumentError("Topic cannot be null.")
        if timestamp is not None and timestamp < 0:
            raise IllegalArgumentError(
                f"Invalid timestamp: {timestamp}. Timestamp should always be "
                f"non-negative or null."
            )
        if partition is not None and partition < 0:
            raise IllegalArgumentError(
                f"Invalid partition: {partition}. Partition number should "
                f"always be non-negative or null."
            )
        self._topic = topic
        self._partition = partition
        self._timestamp = timestamp
        self._key = key
        self._value = value
        # Headers normalized to the owned tuple form (Java new RecordHeaders(..)).
        self._headers: Headers = validate_written_headers(headers)  # type: ignore[assignment]

    def topic(self) -> str:
        return self._topic

    def value(self) -> V | None:
        return self._value

    def key(self) -> K | None:
        return self._key

    def partition(self) -> int | None:
        return self._partition

    def timestamp(self) -> int | None:
        return self._timestamp

    def headers(self) -> Headers:
        return self._headers

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, ProducerRecord):
            return NotImplemented
        return (self._key == other._key
                and self._partition == other._partition
                and self._topic == other._topic
                and self._headers == other._headers
                and self._value == other._value
                and self._timestamp == other._timestamp)

    def __hash__(self) -> int:
        return hash((self._topic, self._partition, self._headers, self._key,
                     self._value, self._timestamp))

    def __repr__(self) -> str:
        # Java toString.
        return (f"ProducerRecord(topic={self._topic}, "
                f"partition={self._partition}, headers={self._headers}, "
                f"key={self._key}, value={self._value}, "
                f"timestamp={self._timestamp})")
