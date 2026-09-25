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
``memoryview_deserializer()`` borrows.

A failing deserializer ends the batch where Java's ``FetchCollector`` ends it:
the records deserialized before the failing one are returned, and the
``RecordDeserializationError`` is raised when there are none (``collectFetch``
rethrows only for an empty fetch). The core has already moved the positions past
the whole batch, so the caller seeks each partition back to its first record
that is not returned, which leaves the failing record's position where Java
leaves it: ``seek(partition=e.topic_partition(), offset=e.offset() + 1)`` skips
it, and otherwise the next poll fetches and fails on it again.
"""

from __future__ import annotations

from typing import Any, Callable, NamedTuple, cast

from confluent_kafka.common.errors.record_deserialization_error import RecordDeserializationError
from confluent_kafka.common.timestamp_type import TimestampType
from confluent_kafka.common.topic_partition import TopicPartition

from .consumer_record import ConsumerRecord
from .consumer_records import ConsumerRecords
from .offset_and_metadata import OffsetAndMetadata

__all__ = ["Deserialized", "deserialize_batch"]


class Deserialized(NamedTuple):
    """A deserialized poll: the records to return, the error that ended the
    batch (raised when ``records`` is empty), and the ``(partition, offset)``
    positions to seek back to (each partition's first record not returned)."""

    records: ConsumerRecords[Any, Any]
    error: RecordDeserializationError | None
    rewind: list[tuple[TopicPartition, int]]


def _deserialization_error(
    origin: RecordDeserializationError.DeserializationExceptionOrigin,
    tp: TopicPartition, nr: Any, key_buffer: memoryview | None,
    value_buffer: memoryview | None, headers: Any, cause: Exception,
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


def _build(grouped: dict[TopicPartition, list[ConsumerRecord[Any, Any]]]
           ) -> ConsumerRecords[Any, Any]:
    """The ``ConsumerRecords`` of the returned records, with next offsets
    recomputed as each partition's last returned offset + 1 and that record's
    leader epoch. Java's ``FetchCollector`` uses the fetch's next offset, which
    also skips trailing control records; the FFI's ``ConsumerRecords`` has no
    accessor for the core's value (``ffi-overload-gaps.md``), so a trailing
    transaction marker is not skipped here (``ConsumerRecords.next_offsets``)."""
    if not grouped:
        return ConsumerRecords.empty()
    next_offsets = {
        tp: OffsetAndMetadata(offset=recs[-1].offset() + 1, leader_epoch=recs[-1].leader_epoch(),
                              metadata="")
        for tp, recs in grouped.items()
    }
    return ConsumerRecords(records=grouped, next_offsets=next_offsets)


def deserialize_batch(
    native_records: Any,
    *,
    key_deserializer: Callable[..., object],
    value_deserializer: Callable[..., object],
) -> Deserialized:
    """Deserialize a native ``ConsumerRecords`` batch (``None`` for an empty
    poll) on the caller's thread."""
    if native_records is None:
        return Deserialized(ConsumerRecords.empty(), None, [])

    grouped: dict[TopicPartition, list[ConsumerRecord[Any, Any]]] = {}
    origin = RecordDeserializationError.DeserializationExceptionOrigin
    count = native_records.count()
    for i in range(count):
        nr = native_records.get(i)
        topic = nr.topic
        tp = TopicPartition(topic=topic, partition=nr.partition)
        # Zero-copy: key/value are memoryviews borrowing the batch.
        key_buffer: memoryview | None = nr.key
        value_buffer: memoryview | None = nr.value
        headers = nr.headers
        error: RecordDeserializationError | None = None
        try:
            key = key_deserializer(topic, key_buffer, headers)
        except Exception as exc:  # noqa: BLE001 - wrapped as Java does
            error = _deserialization_error(
                origin.KEY, tp, nr, key_buffer, value_buffer, headers, exc)
        if error is None:
            try:
                value = value_deserializer(topic, value_buffer, headers)
            except Exception as exc:  # noqa: BLE001 - wrapped as Java does
                error = _deserialization_error(
                    origin.VALUE, tp, nr, key_buffer, value_buffer, headers, exc)
        if error is not None:
            # Each partition's first record not returned: this one, and the
            # first of every partition after it in the batch.
            rewind: dict[TopicPartition, int] = {tp: nr.offset}
            for j in range(i + 1, count):
                later = native_records.get(j)
                later_tp = TopicPartition(topic=later.topic, partition=later.partition)
                rewind.setdefault(later_tp, later.offset)
            return Deserialized(_build(grouped), error, list(rewind.items()))

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
            headers=tuple(headers),
            leader_epoch=nr.leader_epoch,
        )
        grouped.setdefault(tp, []).append(record)

    return Deserialized(_build(grouped), None, [])
