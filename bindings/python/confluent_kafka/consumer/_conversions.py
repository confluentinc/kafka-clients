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

"""Conversions between the C extension's drain tuples/lists and the pure-Python
value types (P2).

The ``_confluentkafka`` natives return offsets, partition metadata, nodes and
metrics as plain tuples/lists/dicts (a marshaling layer only, spec §4). These
helpers turn them into the ``confluent_kafka.common`` / ``confluent_kafka.
consumer`` value types the public API returns, and the reverse for arguments
passed to the FFI. Kept out of the client modules so the FFI tuple shapes are
described in one place.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from confluent_kafka.common.metric import Metric
from confluent_kafka.common.metric_name import MetricName
from confluent_kafka.common.node import Node
from confluent_kafka.common.partition_info import PartitionInfo
from confluent_kafka.common.topic_partition import TopicPartition

from .offset_and_metadata import OffsetAndMetadata
from .offset_and_timestamp import OffsetAndTimestamp

__all__ = [
    "tp_to_spec",
    "offsets_to_spec",
    "timestamps_to_spec",
    "to_offset_map",
    "to_offset_and_timestamp_map",
    "to_long_map",
    "to_partition_info_list",
    "to_topics_map",
    "to_metrics_map",
    "MetricValue",
]


# --------------------------------------------------------------------------
# Python value types -> FFI argument tuples.
# --------------------------------------------------------------------------
def tp_to_spec(
    partitions: Iterable[TopicPartition],
) -> list[tuple[str, int]]:
    """``Iterable[TopicPartition]`` -> the FFI's ``(topic, partition)`` list."""
    return [(tp.topic(), tp.partition()) for tp in partitions]


def offsets_to_spec(
    offsets: Mapping[TopicPartition, OffsetAndMetadata],
) -> list[tuple[str, int, int, int, str]]:
    """``dict[TopicPartition, OffsetAndMetadata]`` -> the FFI's 5-tuple list.

    ``leader_epoch`` maps to ``-1`` when absent; ``metadata`` to ``""`` when
    absent — the FFI's sentinels for the two nullable fields.
    """
    result: list[tuple[str, int, int, int, str]] = []
    for tp, oam in offsets.items():
        epoch = oam.leader_epoch()
        result.append(
            (
                tp.topic(),
                tp.partition(),
                oam.offset(),
                epoch if epoch is not None else -1,
                oam.metadata(),
            )
        )
    return result


def timestamps_to_spec(
    timestamps: Mapping[TopicPartition, int],
) -> list[tuple[str, int, int]]:
    """``dict[TopicPartition, int]`` -> the FFI's ``(topic, partition,
    timestamp)`` list for ``offsets_for_times``."""
    return [
        (tp.topic(), tp.partition(), ts)
        for tp, ts in timestamps.items()
    ]


# --------------------------------------------------------------------------
# FFI drain tuples -> Python value types.
# --------------------------------------------------------------------------
def to_offset_map(
    raw: Mapping[tuple[str, int], tuple[int, str, int]],
) -> dict[TopicPartition, OffsetAndMetadata | None]:
    """FFI ``OffsetMap`` drain -> ``dict[TopicPartition, OffsetAndMetadata |
    None]``.

    Java's ``committed()`` maps a partition with no committed offset to a
    ``null`` value (D25 ruling C); the FFI signals that with ``offset == -1``,
    which becomes ``None`` here.
    """
    out: dict[TopicPartition, OffsetAndMetadata | None] = {}
    for (topic, partition), (offset, metadata, epoch) in raw.items():
        tp = TopicPartition(topic=topic, partition=partition)
        if offset < 0:
            out[tp] = None
        else:
            out[tp] = OffsetAndMetadata(
                offset=offset,
                leader_epoch=(epoch if epoch is not None and epoch >= 0
                              else None),
                metadata=metadata,
            )
    return out


def to_offset_and_timestamp_map(
    raw: Mapping[tuple[str, int], tuple[int, int, int]],
) -> dict[TopicPartition, OffsetAndTimestamp | None]:
    """FFI ``OffsetAndTimestampMap`` drain -> ``dict[TopicPartition,
    OffsetAndTimestamp | None]``.

    Java's ``offsetsForTimes`` maps a partition with no offset for the requested
    timestamp to ``null``; the FFI signals that by omitting the entry, so a
    partition the caller asked about but that is absent here yields ``None`` at
    the call site (handled by the caller, which knows the requested set).
    """
    out: dict[TopicPartition, OffsetAndTimestamp | None] = {}
    for (topic, partition), (offset, timestamp, epoch) in raw.items():
        tp = TopicPartition(topic=topic, partition=partition)
        out[tp] = OffsetAndTimestamp(
            offset=offset,
            timestamp=timestamp,
            leader_epoch=(epoch if epoch is not None and epoch >= 0
                          else None),
        )
    return out


def to_long_map(
    raw: Mapping[tuple[str, int], int],
) -> dict[TopicPartition, int]:
    """FFI ``LongOffsetMap`` drain -> ``dict[TopicPartition, int]``
    (``beginning_offsets`` / ``end_offsets``)."""
    return {
        TopicPartition(topic=topic, partition=partition): offset
        for (topic, partition), offset in raw.items()
    }


def _to_node(n: tuple[int, str, int, str | None] | None) -> Node | None:
    if n is None:
        return None
    node_id, host, port, rack = n
    return Node(id=node_id, host=host, port=port, rack=rack)


def _to_partition_info(
    t: tuple[
        str, int,
        tuple[int, str, int, str | None] | None,
        list[tuple[int, str, int, str | None] | None],
        list[tuple[int, str, int, str | None] | None],
        list[tuple[int, str, int, str | None] | None],
    ],
) -> PartitionInfo:
    topic, partition, leader, replicas, isr, offline = t
    return PartitionInfo(
        topic=topic,
        partition=partition,
        leader=_to_node(leader),
        replicas=tuple(_to_node(x) for x in replicas),  # type: ignore[misc]
        in_sync_replicas=tuple(_to_node(x) for x in isr),  # type: ignore[misc]
        offline_replicas=tuple(_to_node(x) for x in offline),  # type: ignore[misc]
    )


def to_partition_info_list(
    raw: list[Any],
) -> list[PartitionInfo]:
    """FFI ``PartitionInfoList`` drain -> ``list[PartitionInfo]``."""
    return [_to_partition_info(t) for t in raw]


def to_topics_map(
    raw: Mapping[str, list[Any]],
) -> dict[str, list[PartitionInfo]]:
    """FFI ``TopicPartitionInfoMap`` drain -> ``dict[str,
    list[PartitionInfo]]`` (``list_topics``)."""
    return {
        topic: [_to_partition_info(t) for t in infos]
        for topic, infos in raw.items()
    }


class MetricValue:
    """A concrete ``Metric`` produced by ``metrics()``.

    Not user-constructed and not on the Java surface as a class — Java's
    ``metrics()`` returns ``Map<MetricName, ? extends Metric>`` whose values are
    ``KafkaMetric`` instances. The binding materialises each FFI metric snapshot
    as this small value satisfying the ``Metric`` protocol
    (``metric_name()`` / ``metric_value()``). Immutable.
    """

    __slots__ = ("_metric_name", "_metric_value")

    def __init__(self, *, metric_name: MetricName, metric_value: Any) -> None:
        self._metric_name = metric_name
        self._metric_value = metric_value

    def metric_name(self) -> MetricName:
        return self._metric_name

    def metric_value(self) -> Any:
        return self._metric_value

    def __repr__(self) -> str:
        return (
            f"MetricValue(metric_name={self._metric_name!r}, "
            f"metric_value={self._metric_value!r})"
        )


def to_metrics_map(
    raw: list[Mapping[str, Any]],
) -> dict[MetricName, Metric]:
    """FFI ``Consumer_metrics`` drain (``list[dict]`` of name/group/description/
    tags/value/kind) -> ``dict[MetricName, Metric]``."""
    out: dict[MetricName, Metric] = {}
    for entry in raw:
        name = MetricName(
            name=entry["name"],
            group=entry["group"],
            description=entry["description"],
            tags=dict(entry["tags"]),
        )
        out[name] = MetricValue(metric_name=name, metric_value=entry["value"])
    return out
