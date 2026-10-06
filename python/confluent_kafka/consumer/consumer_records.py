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

"""``ConsumerRecords``: Java's ``org.apache.kafka.clients.consumer.ConsumerRecords``.

Java's records-only constructor is ``@Deprecated`` (since 4.0), so it is not
generated (CLAUDE.md, Python Binding Conventions, Class family): the
constructor takes the records and their next offsets.
``records(partition)`` / ``records(topic)`` are one method matched by
``java_forms``.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, ClassVar, Generic, TypeVar, overload

from confluent_kafka._args import Form, java_forms
from confluent_kafka.common.topic_partition import TopicPartition

from .consumer_record import ConsumerRecord
from .offset_and_metadata import OffsetAndMetadata

__all__ = ["ConsumerRecords"]

K = TypeVar("K")
V = TypeVar("V")


class ConsumerRecords(Generic[K, V]):
    """A container that holds the list ``ConsumerRecord`` per partition for a
    particular topic. There is one ``ConsumerRecord`` list for every topic
    partition returned by a ``Consumer.poll()`` operation.

    Java: ``org.apache.kafka.clients.consumer.ConsumerRecords<K, V>``.
    """

    EMPTY: ClassVar[ConsumerRecords[Any, Any]]

    __slots__ = ("_records", "_next_offsets")

    def __init__(self, *,
                 records: Mapping[TopicPartition, Sequence[ConsumerRecord[K, V]]],
                 next_offsets: Mapping[TopicPartition, OffsetAndMetadata]) -> None:
        """The records by partition, and the next offsets and metadata of the
        partitions whose position the poll advanced."""
        self._records: dict[TopicPartition, tuple[ConsumerRecord[K, V], ...]] = {
            tp: tuple(recs) for tp, recs in records.items()}
        self._next_offsets: dict[TopicPartition, OffsetAndMetadata] = dict(next_offsets)

    @overload
    def records(self, *, partition: TopicPartition) -> list[ConsumerRecord[K, V]]: ...
    @overload
    def records(self, *, topic: str) -> list[ConsumerRecord[K, V]]: ...

    @java_forms(Form("partition"), Form("topic"))
    def records(self, *, partition: TopicPartition | None = None,
                topic: str | None = None) -> list[ConsumerRecord[K, V]]:
        """Get just the records for the given partition, or for the given
        topic."""
        if partition is not None:
            return list(self._records.get(partition, ()))
        result: list[ConsumerRecord[K, V]] = []
        for tp, recs in self._records.items():
            if tp.topic() == topic:
                result.extend(recs)
        return result

    def next_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """Get the next offsets and metadata corresponding to all topic
        partitions for which the position have been advanced in this poll
        call.

        On the records a ``KafkaConsumer`` / ``AsyncKafkaConsumer`` ``poll()``
        returns, a partition's next offset is its last returned record's offset
        + 1 (with that record's leader epoch), and a partition that returned no
        record is absent. Java's is the fetch's next offset, the position after
        the poll: past trailing control records (transaction markers) and a
        compacted tail, and present for a partition whose poll only advanced
        past them. The FFI does not expose the core's next offsets yet."""
        return dict(self._next_offsets)

    def partitions(self) -> set[TopicPartition]:
        """Get the partitions which have records contained in this record set:
        the set of partitions with data (may be empty if no data was
        returned)."""
        return set(self._records)

    def __iter__(self) -> Iterator[ConsumerRecord[K, V]]:
        for recs in self._records.values():
            yield from recs

    def __len__(self) -> int:
        """The number of records for all topics (Java's ``count()``)."""
        return sum(len(recs) for recs in self._records.values())

    def is_empty(self) -> bool:
        return not self._records

    @staticmethod
    def empty() -> ConsumerRecords[Any, Any]:
        return ConsumerRecords.EMPTY


ConsumerRecords.EMPTY = ConsumerRecords(records={}, next_offsets={})
