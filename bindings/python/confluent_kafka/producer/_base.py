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

"""State and helpers shared by the sync and async producer families.

The C extension (``_confluentkafka.c``) owns the asynchronous send work: two
background threads batch records and poll their completion futures, then invoke
a Python callback ``cb(result, error)`` with the GIL held. Both the sync
:class:`~confluent_kafka.producer.Producer` and the async
:class:`~confluent_kafka.producer.AsyncProducer` reuse the same C entry points
and differ only in the future type the callback resolves and how.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Protocol, TypeVar

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka import IllegalStateError
from confluent_kafka.common.metric import Metric
from confluent_kafka.common.metric_name import MetricName
from confluent_kafka.common.node import Node
from confluent_kafka.common.partition_info import PartitionInfo

if TYPE_CHECKING:
    from confluent_kafka.common.serialization import Serializer


_F = TypeVar("_F")

_log = logging.getLogger("confluent_kafka")


class _SnapshotMetric(Metric):
    """A point-in-time metric value returned from ``metrics()``.

    ``Metric`` is a ``Protocol`` (§5.1 — instances are produced by the client,
    never constructed by the user), so a concrete implementation is needed to
    materialise a snapshot. It carries the ``MetricName`` and the measured value
    read from the FFI (``metric_value()`` is Java's ``Object`` — a ``float`` /
    ``str`` / ``int`` per the metric's kind)."""

    __slots__ = ("_name", "_value")

    def __init__(self, name: MetricName, value: object) -> None:
        self._name = name
        self._value = value

    def metric_name(self) -> MetricName:
        return self._name

    def metric_value(self) -> object:
        return self._value


def _to_metrics_map(raw: list[dict[str, object]] | None) -> dict[MetricName, Metric]:
    """Build ``dict[MetricName, Metric]`` from the FFI metrics snapshot.

    ``Producer_metrics`` returns a list of ``{name, group, description, tags,
    value, kind}`` dicts (``kind`` distinguishes Rust's Long vs Int, which
    Python collapses to ``int``; the value is already coerced C-side). The key
    is the whole ``MetricName`` triple (name + group + tags), matching Java's
    ``Map<MetricName, Metric>``."""
    if not raw:
        return {}
    out: dict[MetricName, Metric] = {}
    for entry in raw:
        name = MetricName(
            name=entry["name"],           # type: ignore[arg-type]
            group=entry["group"],         # type: ignore[arg-type]
            description=entry["description"],  # type: ignore[arg-type]
            tags=entry["tags"],           # type: ignore[arg-type]
        )
        out[name] = _SnapshotMetric(name, entry["value"])
    return out


def _to_node(n: tuple[int, str, int, str | None] | None) -> Node | None:
    return None if n is None else Node(
        id=n[0], host=n[1], port=n[2], rack=n[3])


def _to_partition_info(t: tuple) -> PartitionInfo:  # type: ignore[type-arg]
    topic, partition, leader, replicas, isr, offline = t
    return PartitionInfo(
        topic=topic,
        partition=partition,
        leader=_to_node(leader),
        replicas=tuple(_to_node(x) for x in replicas),   # type: ignore[misc]
        in_sync_replicas=tuple(_to_node(x) for x in isr),  # type: ignore[misc]
        offline_replicas=tuple(_to_node(x) for x in offline),  # type: ignore[misc]
    )


def _offsets_to_spec(offsets: object) -> list[tuple[str, int, int, int, str]]:
    """Marshal ``{TopicPartition: OffsetAndMetadata}`` into the
    ``(topic, partition, offset, leader_epoch, metadata)`` tuple list the FFI
    expects: a missing ``leader_epoch`` becomes ``-1`` and a missing
    ``metadata`` becomes ``""``. An empty ``offsets`` maps to an empty list."""
    return [(tp.topic(), tp.partition(), oam.offset(),
             oam.leader_epoch() if oam.leader_epoch() is not None else -1,
             oam.metadata() if oam.metadata() is not None else "")
            for tp, oam in offsets.items()]  # type: ignore[attr-defined]


class _ProducerState:
    """The shared native handle and closed-state bookkeeping."""

    __slots__ = ("_c_producer", "_closed", "futures", "_key_serializer",
                 "_value_serializer", "__weakref__")

    def __init__(self) -> None:
        self.futures: set[object] = set()
        self._closed = False
        self._c_producer: int | None = None
        self._key_serializer: Serializer[object] | None = None
        self._value_serializer: Serializer[object] | None = None

    def _init_mock(self, auto_complete: bool) -> None:
        self._c_producer = _lib.Producer_new(auto_complete, self)

    def _init_kafka(self, config: dict[str, object]) -> None:
        self._c_producer = _lib.KafkaProducer_new(config, self)

    def _check_not_closed(self) -> None:
        # Java: IllegalStateException on use after close (§5.6).
        if self._closed:
            raise IllegalStateError("Cannot perform operation after "
                                    "producer has been closed")

    def _remove_future(self, future: object) -> None:
        self.futures.discard(future)

    def _add_future(self, future: _F) -> _F:
        self.futures.add(future)
        future.add_done_callback(self._remove_future)  # type: ignore[attr-defined]
        return future

    # ---- serializer plumbing (shared by the sync + async producers) ---------
    def _serialize(self, topic: str, value: object,
                   serializer: Serializer[object] | None) -> bytes | None:
        if value is None:
            return None
        assert serializer is not None
        return _as_bytes(serializer(topic, value))

    def _native_record(self, record: object) -> object:
        """Serialize the record's key/value on the caller's thread (spec §5.4)
        and build the native ``_confluentkafka.ProducerRecord`` that carries the
        serialized ``bytes`` (and the record's headers) into the send path with no
        further copy of the key/value bytes (C10)."""
        topic = record.topic()               # type: ignore[attr-defined]
        key = self._serialize(topic, record.key(),   # type: ignore[attr-defined]
                              self._key_serializer)
        value = self._serialize(topic, record.value(),  # type: ignore[attr-defined]
                                self._value_serializer)
        partition = record.partition()       # type: ignore[attr-defined]
        timestamp = record.timestamp()       # type: ignore[attr-defined]
        headers = record.headers()           # type: ignore[attr-defined]
        # A None value is a Java tombstone (the native ctor / FFI carry it as a
        # null value, value_len == -1); headers are passed through as the
        # already-owned (str, bytes|None) pairs the record holds.
        return _lib.ProducerRecord(
            topic,
            value,
            key,
            partition if partition is not None else -1,
            timestamp if timestamp is not None else -1,
            tuple(headers),
        )


def _as_bytes(value: object) -> bytes | None:
    """A serializer yields ``bytes`` (or ``None`` for a tombstone); narrow the
    ``Serializer[object]`` return for the native send call."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    from confluent_kafka import IllegalArgumentError as _IllegalArgumentError
    raise _IllegalArgumentError(
        f"serializer must return bytes or None, got {type(value).__name__}")
