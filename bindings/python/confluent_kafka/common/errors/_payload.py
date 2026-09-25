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

"""Typed payload for the error classes that carry Java getters (rule 5).

Several Kafka exceptions expose typed payload getters — e.g.
``TopicAuthorizationException.unauthorizedTopics()`` (``Set<String>``),
``LogTruncationException.divergentOffsets()`` (``Map<TopicPartition,
OffsetAndMetadata>``). The Python mirror reproduces those getters as accessor
methods on the generated error classes (``…Error``); the methods read a typed
payload attached to the instance at construction time.

The core owns the values behind the FFI two-step accessor pattern
(``kafka_common_Error_<type>`` → sub-handle → ``…_<getter>``). The C extension's
``KafkaError_payload`` reads the sub-handle before the error handle is destroyed
and returns the raw payload as a ``dict`` keyed by accessor name (raw tuples for
``TopicPartition`` / ``OffsetAndMetadata``). :func:`build_payload` wraps those
raw values into the public ``confluent_kafka`` types, so the generated accessor
methods can return them directly.

The mapping of Java collection return types to Python containers follows R2.8:
``Set`` → ``set``, ``Map`` → ``dict``, ``List`` → ``list``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from confluent_kafka.common.topic_partition import TopicPartition
    from confluent_kafka.consumer.offset_and_metadata import OffsetAndMetadata


def _to_tp(raw: tuple[str, int]) -> TopicPartition:
    from confluent_kafka.common.topic_partition import TopicPartition

    topic, partition = raw
    return TopicPartition(topic=topic, partition=partition)


def _to_oam(raw: tuple[int, str, int | None]) -> OffsetAndMetadata:
    from confluent_kafka.consumer.offset_and_metadata import OffsetAndMetadata

    offset, metadata, epoch = raw
    return OffsetAndMetadata(
        offset=offset,
        leader_epoch=(epoch if epoch is not None and epoch >= 0 else None),
        metadata=metadata,
    )


def _tp_set(raw: list[tuple[str, int]] | None) -> set[TopicPartition] | None:
    if raw is None:
        return None
    return {_to_tp(t) for t in raw}


def _long_map(
    raw: dict[tuple[str, int], int] | None,
) -> dict[TopicPartition, int] | None:
    if raw is None:
        return None
    return {_to_tp(k): v for k, v in raw.items()}


def _oam_map(
    raw: dict[tuple[str, int], tuple[int, str, int | None]] | None,
) -> dict[TopicPartition, OffsetAndMetadata] | None:
    if raw is None:
        return None
    return {_to_tp(k): _to_oam(v) for k, v in raw.items()}


def build_payload(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    """Wrap the C extension's raw payload dict into public ``confluent_kafka``
    types, keyed by the Python accessor-method name.

    Returns ``None`` when the error carries no typed payload. Only the keys the
    C extension provided for this error variant are present; the generated
    accessor methods read exactly those keys.
    """
    if not raw:
        return None
    out: dict[str, Any] = {}
    for key, value in raw.items():
        if key in ("unauthorized_topics", "invalid_topics"):
            # Java ``Set<String>``.
            out[key] = set(value)
        elif key == "partitions":
            # Java ``Set<TopicPartition>``.
            out[key] = _tp_set(value)
        elif key in (
            "offset_out_of_range_partitions",
            "record_too_large_partitions",
        ):
            # Java ``Map<TopicPartition, Long>``.
            out[key] = _long_map(value)
        elif key == "divergent_offsets":
            # Java ``Map<TopicPartition, OffsetAndMetadata>``.
            out[key] = _oam_map(value)
        else:
            # Scalars (str | None, int, float) pass through unchanged.
            out[key] = value
    return out
