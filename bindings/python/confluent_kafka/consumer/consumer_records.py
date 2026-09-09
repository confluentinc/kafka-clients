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

"""``ConsumerRecords`` — the batch returned by ``poll()``.

Translated from ``org.apache.kafka.clients.consumer.ConsumerRecords`` (Apache
Kafka 4.3.1). Iterable, sized, queryable per partition or topic.

Java's two constructors collapse to one keyword-only constructor. The
``next_offsets=None`` form is Java's records-only constructor, deprecated in
4.3.1 (KIP-1094 / KAFKA-20660): an instance built that way is "tainted" and
cannot supply next offsets — ``next_offsets()`` returns an empty ``dict`` and
logs a rate-limited error, exactly as Java does (it does NOT raise; see the P2
clarifications entry for the spec-text divergence).

``records(*, partition=None, topic=None)`` collapses Java's two ``records``
overloads with an ``@overload`` stub per form (D27); give exactly one.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Generic, TypeVar, overload

from confluent_kafka import IllegalArgumentError
from confluent_kafka._args import exactly_one
from confluent_kafka.common.topic_partition import TopicPartition

from .consumer_record import ConsumerRecord
from .offset_and_metadata import OffsetAndMetadata

K = TypeVar("K")
V = TypeVar("V")

log = logging.getLogger(__name__)

# Java: TAINT_LOG_INTERVAL_NS = 5 minutes. Kept in seconds here (time.monotonic).
_TAINT_LOG_INTERVAL_S = 5 * 60.0
# Java seeds the last-log time one interval in the past so the first tainted call
# logs. Module-global, mirroring Java's static AtomicLong.
_tainted_next_offsets_last_log_s = time.monotonic() - _TAINT_LOG_INTERVAL_S


class ConsumerRecords(Generic[K, V]):
    """The batch of records returned by ``poll()``.

    Java: ``org.apache.kafka.clients.consumer.ConsumerRecords<K, V>``.
    """

    __slots__ = ("_records", "_next_offsets", "_tainted")

    def __init__(
        self, *,
        records: Mapping[TopicPartition, Sequence[ConsumerRecord[K, V]]],
        next_offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
    ) -> None:
        # Preserve insertion order (Java uses the map's iteration order for
        # iterator()/records(topic)).
        self._records: dict[TopicPartition, tuple[ConsumerRecord[K, V], ...]] = {
            tp: tuple(recs) for tp, recs in records.items()
        }
        # tainted == the deprecated records-only constructor was used.
        self._tainted = next_offsets is None
        self._next_offsets: dict[TopicPartition, OffsetAndMetadata] = (
            {} if next_offsets is None else dict(next_offsets)
        )

    @staticmethod
    def empty() -> ConsumerRecords[Any, Any]:
        """Java ``empty()`` — the shared empty (non-tainted) batch."""
        return _EMPTY

    def __iter__(self) -> Iterator[ConsumerRecord[K, V]]:
        # Java concatenates the per-partition lists in map order.
        for recs in self._records.values():
            yield from recs

    def __len__(self) -> int:
        # Java count(): total records across all partitions.
        return sum(len(recs) for recs in self._records.values())

    def is_empty(self) -> bool:
        """Java ``isEmpty()`` — true iff there are no partitions with data."""
        return not self._records

    def partitions(self) -> set[TopicPartition]:
        """Java ``partitions()`` — the partitions with data in this batch."""
        return set(self._records.keys())

    @overload
    def records(self, *,
                partition: TopicPartition) -> list[ConsumerRecord[K, V]]: ...
    @overload
    def records(self, *, topic: str) -> list[ConsumerRecord[K, V]]: ...

    def records(self, *, partition: TopicPartition | None = None,
                topic: str | None = None) -> list[ConsumerRecord[K, V]]:
        """Records for one partition, or all records for one topic.

        Collapses Java's ``records(TopicPartition)`` and ``records(String)``;
        give exactly one of ``partition`` / ``topic``. ``records(topic=None)``
        would be Java's null-topic call, which raises ``IllegalArgumentError``.
        """
        chosen = exactly_one(
            "records", partition=partition, topic=topic,
        )
        if chosen == "partition":
            assert partition is not None
            return list(self._records.get(partition, ()))
        # topic form.
        assert topic is not None
        result: list[ConsumerRecord[K, V]] = []
        for tp, recs in self._records.items():
            if tp.topic() == topic:
                result.extend(recs)
        return result

    def next_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """The next offsets for partitions advanced in this poll.

        Java: on a tainted (records-only-constructed) instance this returns an
        empty map and logs a rate-limited deprecation error — it does NOT raise.
        """
        if self._tainted:
            self._maybe_log_tainted()
        return dict(self._next_offsets)

    @staticmethod
    def _maybe_log_tainted() -> None:
        global _tainted_next_offsets_last_log_s
        now = time.monotonic()
        last = _tainted_next_offsets_last_log_s
        if now - last >= _TAINT_LOG_INTERVAL_S:
            _tainted_next_offsets_last_log_s = now
            log.error(
                "ConsumerRecords.next_offsets() returned empty because this "
                "instance was built with the deprecated records-only "
                "constructor (see KIP-1094), which does not supply next "
                "offsets. Downstream logic that relies on these offsets to "
                "advance the consumer's committed position (for example, Kafka "
                "Streams under exactly-once semantics) will be unable to "
                "commit, leading to reprocessing. Update the interceptor or "
                "wrapper that constructed it to supply next offsets."
            )

    def __repr__(self) -> str:
        return f"ConsumerRecords(count={len(self)}, tainted={self._tainted})"


# Java EMPTY = new ConsumerRecords<>(Map.of(), Map.of()) — non-tainted.
_EMPTY: ConsumerRecords[Any, Any] = ConsumerRecords(records={}, next_offsets={})
