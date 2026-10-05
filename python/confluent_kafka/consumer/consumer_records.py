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

Java's two constructors are one keyword-only constructor. The records-only form
is ``@Deprecated`` (since 4.0) and warns; an instance built that way answers
``next_offsets()`` as Java 4.3.1 does: a rate-limited error log and ``{}``
(CLAUDE.md, Python Binding Conventions, Behaviour with no other home).
``records(partition)`` / ``records(topic)`` are one method matched by
``java_forms``.
"""

from __future__ import annotations

import logging
import threading
import time
import warnings
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, ClassVar, Generic, TypeVar, overload

from confluent_kafka._args import Form, java_forms
from confluent_kafka.common.topic_partition import TopicPartition

from .consumer_record import ConsumerRecord
from .offset_and_metadata import OffsetAndMetadata

__all__ = ["ConsumerRecords"]

K = TypeVar("K")
V = TypeVar("V")

log = logging.getLogger(__name__)

# Visible for testing (Java: TAINT_LOG_INTERVAL_NS, TAINTED_NEXT_OFFSETS_LAST_LOG_NS).
_TAINT_LOG_INTERVAL_S = 5 * 60.0
_tainted_next_offsets_last_log_s = time.monotonic() - _TAINT_LOG_INTERVAL_S
_taint_log_lock = threading.Lock()

_DEPRECATED = "Since 4.0. Use ``ConsumerRecords(records, next_offsets)`` instead."


class ConsumerRecords(Generic[K, V]):
    """A container that holds the list ``ConsumerRecord`` per partition for a
    particular topic. There is one ``ConsumerRecord`` list for every topic
    partition returned by a ``Consumer.poll()`` operation.

    Java: ``org.apache.kafka.clients.consumer.ConsumerRecords<K, V>``.
    """

    EMPTY: ClassVar[ConsumerRecords[Any, Any]]

    __slots__ = ("_records", "_next_offsets", "_tainted")

    def __init__(self, *,
                 records: Mapping[TopicPartition, Sequence[ConsumerRecord[K, V]]],
                 next_offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None) -> None:
        """The records by partition, and the next offsets and metadata of the
        partitions whose position the poll advanced. Deprecated without
        ``next_offsets``: since 4.0, use ``ConsumerRecords(records,
        next_offsets)`` instead."""
        if next_offsets is None:
            warnings.warn(f"ConsumerRecords(records) is deprecated. {_DEPRECATED}",
                          DeprecationWarning, stacklevel=2)
        self._init(records, next_offsets)

    def _init(self, records: Mapping[TopicPartition, Sequence[ConsumerRecord[K, V]]],
              next_offsets: Mapping[TopicPartition, OffsetAndMetadata] | None) -> None:
        self._records: dict[TopicPartition, tuple[ConsumerRecord[K, V], ...]] = {
            tp: tuple(recs) for tp, recs in records.items()}
        # Flag to detect if the legacy ConsumerRecords(Map) constructor is used.
        self._tainted = next_offsets is None
        self._next_offsets: dict[TopicPartition, OffsetAndMetadata] = (
            {} if next_offsets is None else dict(next_offsets))

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
        past them. The FFI does not expose the core's next offsets yet
        (``ffi-overload-gaps.md``)."""
        global _tainted_next_offsets_last_log_s
        if self._tainted:
            now = time.monotonic()
            with _taint_log_lock:
                due = now - _tainted_next_offsets_last_log_s >= _TAINT_LOG_INTERVAL_S
                if due:
                    _tainted_next_offsets_last_log_s = now
            if due:
                log.error(
                    "ConsumerRecords#nextOffsets() returned empty because this instance was "
                    "built with the deprecated ConsumerRecords(Map) constructor (see KIP-1094), "
                    "which does not supply next offsets. Downstream logic that relies on these "
                    "offsets to advance the consumer's committed position (for example, Kafka "
                    "Streams under exactly-once semantics) will be unable to commit, leading to "
                    "reprocessing. Update the interceptor or wrapper that constructed it to use "
                    "the ConsumerRecords(Map, Map) constructor that supplies next offsets.")
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
