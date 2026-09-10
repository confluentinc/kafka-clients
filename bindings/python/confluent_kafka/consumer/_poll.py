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

"""Poll-result deserialization (spec §5.4, consumer-threading.md §27).

The native ``_confluentkafka.ConsumerRecords`` batch owns the fetched bytes; its
records hand out ``memoryview`` key/value that borrow the batch (zero-copy). This
module turns that native batch into the pure-Python ``ConsumerRecords`` /
``ConsumerRecord`` value types (P2), running the user's key/value deserializers
on the ``memoryview`` data **without copying** the key/value bytes:
``bytes_deserializer()`` copies (its choice), ``memoryview_deserializer()``
borrows. Execution is eager, on the caller's thread; a failing deserializer
raises ``RecordDeserializationError`` and the position does not move (§5.4).
"""

from __future__ import annotations

from typing import Any, Callable

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka.common.errors._generated import RecordDeserializationError
from confluent_kafka.common.timestamp_type import TimestampType
from confluent_kafka.common.topic_partition import TopicPartition

from .consumer_record import ConsumerRecord
from .consumer_records import ConsumerRecords
from .offset_and_metadata import OffsetAndMetadata


def _attach_deserialization_payload(
    error: RecordDeserializationError, *,
    topic: str, partition: int, offset: int,
    key_buffer: memoryview | None, value_buffer: memoryview | None,
) -> RecordDeserializationError:
    """Attach the typed payload accessors Java's
    ``RecordDeserializationException`` exposes (``topicPartition()``,
    ``offset()``, ``keyBuffer()``, ``valueBuffer()``). The generated error class
    is a plain leaf (P1), so the payload is attached here at raise time (P1
    deferral C4); the accessors then let the poison-pill recovery
    ``seek(partition=e.topic_partition(), offset=e.offset() + 1)`` work."""
    tp = TopicPartition(topic=topic, partition=partition)
    error._topic_partition = tp  # type: ignore[attr-defined]
    error._offset = offset  # type: ignore[attr-defined]
    error._key_buffer = key_buffer  # type: ignore[attr-defined]
    error._value_buffer = value_buffer  # type: ignore[attr-defined]

    def topic_partition() -> TopicPartition:
        return tp

    def offset_accessor() -> int:
        return offset

    def key_buffer_accessor() -> memoryview | None:
        return key_buffer

    def value_buffer_accessor() -> memoryview | None:
        return value_buffer

    def origin() -> str:
        return topic

    error.topic_partition = topic_partition  # type: ignore[attr-defined]
    error.offset = offset_accessor  # type: ignore[attr-defined]
    error.key_buffer = key_buffer_accessor  # type: ignore[attr-defined]
    error.value_buffer = value_buffer_accessor  # type: ignore[attr-defined]
    error.origin = origin  # type: ignore[attr-defined]
    return error


def deserialize_batch(
    native_records: Any,
    *,
    key_deserializer: Callable[..., object],
    value_deserializer: Callable[..., object],
) -> ConsumerRecords[Any, Any]:
    """Turn a native ``ConsumerRecords`` batch into a pure-Python
    ``ConsumerRecords``, running the deserializers per record on the batch's
    ``memoryview`` data.

    ``native_records`` is ``None`` for an empty poll → the shared empty batch.
    """
    if native_records is None:
        return ConsumerRecords.empty()

    grouped: dict[TopicPartition, list[ConsumerRecord[Any, Any]]] = {}
    count = native_records.count()
    for i in range(count):
        nr = native_records.get(i)
        topic = nr.topic
        partition = nr.partition
        tp = TopicPartition(topic=topic, partition=partition)
        # Zero-copy: key/value are memoryviews borrowing the batch.
        key_buffer: memoryview | None = nr.key
        value_buffer: memoryview | None = nr.value
        headers = nr.headers
        try:
            key = key_deserializer(topic, key_buffer, headers)
        except RecordDeserializationError:
            raise
        except Exception as exc:  # noqa: BLE001 - wrapped like Java
            raise _attach_deserialization_payload(
                RecordDeserializationError(
                    f"Error deserializing key for partition {topic}-{partition} "
                    f"at offset {nr.offset}"
                ),
                topic=topic, partition=partition, offset=nr.offset,
                key_buffer=key_buffer, value_buffer=value_buffer,
            ) from exc
        try:
            value = value_deserializer(topic, value_buffer, headers)
        except RecordDeserializationError:
            raise
        except Exception as exc:  # noqa: BLE001 - wrapped like Java
            raise _attach_deserialization_payload(
                RecordDeserializationError(
                    f"Error deserializing value for partition "
                    f"{topic}-{partition} at offset {nr.offset}"
                ),
                topic=topic, partition=partition, offset=nr.offset,
                key_buffer=key_buffer, value_buffer=value_buffer,
            ) from exc

        record: ConsumerRecord[Any, Any] = ConsumerRecord(
            topic=topic,
            partition=partition,
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

    # next_offsets: the native batch does not carry KIP-1094 next offsets today,
    # so the batch is built with the (advanced) positions per partition — the
    # last record's offset + 1 for each partition with data.
    next_offsets: dict[TopicPartition, OffsetAndMetadata] = {}
    for tp, recs in grouped.items():
        if recs:
            next_offsets[tp] = OffsetAndMetadata(offset=recs[-1].offset() + 1)
    return ConsumerRecords(records=grouped, next_offsets=next_offsets)
