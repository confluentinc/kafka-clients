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

"""Poll-result deserialization (private; CLAUDE.md, Python Binding Conventions,
Serialization; ``consumer-threading.md`` §27).

The native ``_confluentkafka.ConsumerRecords`` batch owns the fetched bytes; its
records hand out ``memoryview`` keys and values into it. This module runs the
user's deserializers over those views on the caller's thread and builds the
``ConsumerRecords``; ``bytes_deserializer()`` copies (its choice),
``memoryview_deserializer()`` borrows. A null key or value (a ``None`` view) is
not passed to its deserializer: the record's field is ``None``, as Java's
``CompletedFetch.parseRecord`` leaves it (Serialization).

A failing deserializer ends the batch where Java's ``FetchCollector`` ends it:
the records deserialized before the failing one are returned, and the
``RecordDeserializationError`` is raised when there are none (``collectFetch``
rethrows only for an empty fetch). The core has already moved the positions past
the whole batch, so the caller seeks each partition back to its first record
that is not returned, which leaves the failing record's position where Java
leaves it: ``seek(partition=e.topic_partition(), offset=e.offset() + 1)`` skips
it, and otherwise the next poll fetches and fails on it again. A poll that does
not return its records at all (Ctrl+C or a cancellation while it waited, or
during those seeks back) seeks each partition back to its first fetched record
instead (``fetched_positions``, ``Deserialized.unreturned``): Java's
``org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#poll`` never loses
records the positions have moved past.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable, NamedTuple, cast

from confluent_kafka.common.errors.record_deserialization_error import RecordDeserializationError
from confluent_kafka.common.header.headers import Headers
from confluent_kafka.common.record.timestamp_type import TimestampType
from confluent_kafka.common.topic_partition import TopicPartition

from .consumer_record import ConsumerRecord
from .consumer_records import ConsumerRecords
from .offset_and_metadata import OffsetAndMetadata

__all__ = ["Deserialized", "deserialize_batch", "fetched_positions"]


class Deserialized(NamedTuple):
    """A deserialized poll: the records to return, the error that ended the
    batch (raised when ``records`` is empty), and the ``(partition, offset)``
    positions to seek back to (each partition's first record not returned)."""

    records: ConsumerRecords[Any, Any]
    error: RecordDeserializationError | None
    rewind: list[tuple[TopicPartition, int]]

    def unreturned(self) -> list[tuple[TopicPartition, int]]:
        """The positions to seek back to when ``records`` is not returned
        either: each partition's first record in ``records``, else its
        ``rewind`` position."""
        positions: dict[TopicPartition, int] = {}
        for record in self.records:
            positions.setdefault(TopicPartition(topic=record.topic(), partition=record.partition()),
                                 record.offset())
        for tp, offset in self.rewind:
            positions.setdefault(tp, offset)
        return list(positions.items())


def fetched_positions(native_records: Any) -> list[tuple[TopicPartition, int]]:
    """Each partition's first record in a native ``ConsumerRecords`` batch
    (``None`` for an empty poll) that is not returned: the positions to seek
    back to, since the core has moved the positions past the whole batch."""
    if native_records is None:
        return []
    positions: dict[TopicPartition, int] = {}
    for i in range(native_records.count()):
        nr = native_records.get(i)
        positions.setdefault(TopicPartition(topic=nr.topic, partition=nr.partition), nr.offset)
    return list(positions.items())


def _deserialization_error(
    origin: RecordDeserializationError.DeserializationExceptionOrigin,
    tp: TopicPartition, nr: Any, key_buffer: memoryview | None,
    value_buffer: memoryview | None, headers: Headers, cause: Exception,
) -> RecordDeserializationError:
    """Java's ``CompletedFetch.newRecordDeserializationException``: the full
    constructor, with the record's position, timestamp, bytes and headers."""
    return RecordDeserializationError(
        origin=origin, partition=tp, offset=nr.offset, timestamp=nr.timestamp,
        timestamp_type=TimestampType(nr.timestamp_type),
        key_buffer=cast(bytes, key_buffer), value_buffer=cast(bytes, value_buffer),
        headers=headers,
        message=(f"Error deserializing {origin.name} for partition {tp} at offset "
                 f"{nr.offset}. If needed, please seek past the record to continue "
                 "consumption."),
        cause=cause,
    )


def _build(native_records: Any, grouped: dict[TopicPartition, list[ConsumerRecord[Any, Any]]],
           rewind: Mapping[TopicPartition, int]) -> ConsumerRecords[Any, Any]:
    """The ``ConsumerRecords`` of the returned records, with the core's next
    offsets (``kafka_consumer_ConsumerRecords_next_offsets``): Java's
    ``FetchCollector`` reports the fetch's next offset, past trailing control
    records, also for a partition whose poll only advanced past them. A
    partition in ``rewind`` was cut short by a failing deserializer: there, as
    Java's ``CompletedFetch.fetchRecords`` leaves ``nextFetchOffset``, the next
    offset is the last returned record's offset + 1 with its leader epoch, and
    a partition with no returned record has none."""
    next_offsets: dict[TopicPartition, OffsetAndMetadata] = {}
    for (topic, partition), (offset, metadata, epoch) in native_records.next_offsets().items():
        tp = TopicPartition(topic=topic, partition=partition)
        if tp not in rewind:
            next_offsets[tp] = OffsetAndMetadata(offset=offset, leader_epoch=epoch,
                                                 metadata=metadata)
    for tp in rewind:
        recs = grouped.get(tp)
        if recs:
            next_offsets[tp] = OffsetAndMetadata(offset=recs[-1].offset() + 1,
                                                 leader_epoch=recs[-1].leader_epoch(), metadata="")
    if not grouped and not next_offsets:
        return ConsumerRecords.empty()
    records = ConsumerRecords(records=grouped, next_offsets=next_offsets)
    # The core's fetch: not copied or pickled (ConsumerRecords.__reduce_ex__).
    records._fetched = True
    return records


def deserialize_batch(
    native_records: Any,
    *,
    key_deserializer: Callable[..., object],
    value_deserializer: Callable[..., object],
) -> Deserialized:
    """Deserialize a native ``ConsumerRecords`` batch (``None`` for an empty
    poll) on the caller's thread. The deserializers are the positional
    callers ``_kwargs.serde_caller`` chose, called as ``(topic, data,
    headers)``."""
    if native_records is None:
        return Deserialized(ConsumerRecords.empty(), None, [])

    grouped: dict[TopicPartition, list[ConsumerRecord[Any, Any]]] = {}
    origin = RecordDeserializationError.DeserializationExceptionOrigin
    count = native_records.count()
    for i in range(count):
        nr = native_records.get(i)
        topic = nr.topic
        tp = TopicPartition(topic=topic, partition=nr.partition)
        # Zero-copy: key/value, and the header values of `headers` (a
        # `Headers` the extension builds), are memoryviews borrowing the batch.
        key_buffer: memoryview | None = nr.key
        value_buffer: memoryview | None = nr.value
        headers: Headers = nr.headers
        error: RecordDeserializationError | None = None
        # Java's CompletedFetch.parseRecord: `keyBytes == null ? null :
        # deserialize(...)`, and the same for the value.
        key: object = None
        value: object = None
        if key_buffer is not None:
            try:
                key = key_deserializer(topic, key_buffer, headers)
            except Exception as exc:  # noqa: BLE001 - wrapped as Java does
                error = _deserialization_error(
                    origin.KEY, tp, nr, key_buffer, value_buffer, headers, exc)
        if error is None and value_buffer is not None:
            try:
                value = value_deserializer(topic, value_buffer, headers)
            except Exception as exc:  # noqa: BLE001 - wrapped as Java does
                error = _deserialization_error(
                    origin.VALUE, tp, nr, key_buffer, value_buffer, headers, exc)
        # The headers stay writable, as Java's CompletedFetch leaves them
        # (only KafkaProducer.doSend seals a record's headers).
        if error is not None:
            # Each partition's first record not returned: this one, and the
            # first of every partition after it in the batch.
            rewind: dict[TopicPartition, int] = {tp: nr.offset}
            for j in range(i + 1, count):
                later = native_records.get(j)
                later_tp = TopicPartition(topic=later.topic, partition=later.partition)
                rewind.setdefault(later_tp, later.offset)
            return Deserialized(_build(native_records, grouped, rewind), error,
                                list(rewind.items()))

        record: ConsumerRecord[Any, Any] = ConsumerRecord(
            topic=topic,
            partition=nr.partition,
            offset=nr.offset,
            timestamp=nr.timestamp,
            timestamp_type=TimestampType(nr.timestamp_type),
            serialized_key_size=nr.serialized_key_size,
            serialized_value_size=nr.serialized_value_size,
            key=key,
            value=value,
            headers=headers,
            leader_epoch=nr.leader_epoch,
        )
        grouped.setdefault(tp, []).append(record)

    return Deserialized(_build(native_records, grouped, {}), None, [])
