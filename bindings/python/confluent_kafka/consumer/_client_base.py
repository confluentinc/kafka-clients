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

"""Shared FFI op-spec builders and synchronous state reads for the consumer
clients.

Each blocking op is expressed as a ``(submit, resolve, free)`` triple the engine
drives (``_run_sync`` / ``_run_async``), mirroring the legacy binding. The pure
state reads (``assignment`` / ``subscription`` / ``paused`` / ``group_metadata``
/ ``metrics`` / ``current_lag`` / ``wakeup``) are synchronous on both the sync
and async clients (they never block, spec §6.2), so they live here directly.
"""

from __future__ import annotations

from typing import Any, Callable, TypeVar

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka import ConcurrentModificationError
from confluent_kafka.common.errors import from_ffi_error
from confluent_kafka.common.metric import Metric
from confluent_kafka.common.metric_name import MetricName
from confluent_kafka.common.partition_info import PartitionInfo
from confluent_kafka.common.topic_partition import TopicPartition

from ._conversions import (
    offsets_to_spec, timestamps_to_spec, to_long_map, to_metrics_map,
    to_offset_and_timestamp_map, to_offset_map, to_partition_info_list,
    to_topics_map, tp_to_spec,
)
from ._engine import _ConsumerEngine
from ._poll import deserialize_batch
from .consumer_group_metadata import ConsumerGroupMetadata
from .consumer_records import ConsumerRecords
from .offset_and_metadata import OffsetAndMetadata
from .offset_and_timestamp import OffsetAndTimestamp

_R = TypeVar("_R")

# One FFI op expressed as (submit, resolve, free); ``_run_sync`` / ``_run_async``
# drive it and return the resolve callable's result.
_Spec = tuple[
    Callable[[Callable[..., None]], None],
    Callable[[tuple[Any, ...]], _R],
    Callable[[tuple[Any, ...]], None],
]


def _concurrent_error() -> ConcurrentModificationError:
    # Java: ConcurrentModificationException "KafkaConsumer is not safe for
    # multi-threaded access." The FFI's single-owner guard rejects a concurrent
    # op; a null return from a sync state read means the guard was rejected.
    return ConcurrentModificationError(
        "KafkaConsumer is not safe for multi-threaded access."
    )


def _raise_if_error(error_handle: int) -> None:
    if error_handle:
        raise from_ffi_error(error_handle)


class _ConsumerClientBase(_ConsumerEngine):
    """Op-spec builders + synchronous state reads shared by the clients."""

    __slots__ = ()

    # ---- resolve/free payload handlers ---------------------------------
    @staticmethod
    def _resolve_void(payload: tuple[Any, ...]) -> None:
        _raise_if_error(payload[0])

    @staticmethod
    def _free_void(payload: tuple[Any, ...]) -> None:
        if payload and payload[0]:
            _lib.KafkaError_destroy(payload[0])

    def _resolve_poll(self, payload: tuple[Any, ...]) -> ConsumerRecords[Any, Any]:
        records, error = payload
        if error:
            raise from_ffi_error(error)
        # ConsumerRecords_wrap returns the native batch (owns the fetched bytes)
        # or None for an empty poll. Deserialize eagerly on the caller's thread
        # (spec §5.4), borrowing the batch's memoryviews (§27).
        native = _lib.ConsumerRecords_wrap(records) if records else None
        return deserialize_batch(
            native,
            key_deserializer=self._key_deserializer,
            value_deserializer=self._value_deserializer,
        )

    @staticmethod
    def _free_poll(payload: tuple[Any, ...]) -> None:
        records, error = payload
        if error:
            _lib.KafkaError_destroy(error)
        if records:
            # Wrap + drop to destroy the batch handle.
            _lib.ConsumerRecords_wrap(records)

    @staticmethod
    def _resolve_position(payload: tuple[Any, ...]) -> int:
        position, error = payload
        if error:
            raise from_ffi_error(error)
        return int(position)

    @staticmethod
    def _free_position(payload: tuple[Any, ...]) -> None:
        if payload[1]:
            _lib.KafkaError_destroy(payload[1])

    @staticmethod
    def _resolve_map(drain: Callable[[int], Any], convert: Callable[[Any], Any]) -> Callable[[tuple[Any, ...]], Any]:
        def resolve(payload: tuple[Any, ...]) -> Any:
            handle, error = payload
            if error:
                raise from_ffi_error(error)
            return convert(drain(handle)) if handle else {}
        return resolve

    @staticmethod
    def _free_map(drain: Callable[[int], Any]) -> Callable[[tuple[Any, ...]], None]:
        def free(payload: tuple[Any, ...]) -> None:
            handle, error = payload
            if error:
                _lib.KafkaError_destroy(error)
            if handle:
                drain(handle)
        return free

    # ---- op-spec builders ----------------------------------------------
    def _poll_spec(self, timeout_ms: int) -> _Spec[ConsumerRecords[Any, Any]]:
        return (
            lambda cb: _lib.Consumer_poll_async(self._h, timeout_ms, cb),
            self._resolve_poll,
            self._free_poll,
        )

    def _subscribe_topics_spec(self, topics: list[str], has_listener: bool) -> _Spec[None]:
        if has_listener:
            return (
                lambda cb: _lib.Consumer_subscribe_caller_thread_listener_async(self._h, topics, cb),
                self._resolve_void, self._free_void,
            )
        return (
            lambda cb: _lib.Consumer_subscribe_async(self._h, topics, cb),
            self._resolve_void, self._free_void,
        )

    def _subscribe_pattern_spec(self, pattern: str, has_listener: bool) -> _Spec[None]:
        if has_listener:
            return (
                lambda cb: _lib.Consumer_subscribe_pattern_caller_thread_listener_async(self._h, pattern, cb),
                self._resolve_void, self._free_void,
            )
        return (
            lambda cb: _lib.Consumer_subscribe_pattern_async(self._h, pattern, cb),
            self._resolve_void, self._free_void,
        )

    def _unsubscribe_spec(self) -> _Spec[None]:
        return (
            lambda cb: _lib.Consumer_unsubscribe_async(self._h, cb),
            self._resolve_void, self._free_void,
        )

    def _tp_op_spec(self, fn: Any, partitions: Any) -> _Spec[None]:
        spec = tp_to_spec(partitions)
        return (lambda cb: fn(self._h, spec, cb), self._resolve_void, self._free_void)

    def _seek_spec(self, partition: TopicPartition, offset: int | None,
                   offset_and_metadata: Any) -> tuple[Any, Any, Any]:
        if offset_and_metadata is not None:
            oam = offset_and_metadata
            epoch = oam.leader_epoch()
            return (
                lambda cb: _lib.Consumer_seek_with_metadata_async(
                    self._h, partition.topic(), partition.partition(),
                    oam.offset(), epoch if epoch is not None else -1,
                    oam.metadata(), cb,
                ),
                self._resolve_void, self._free_void,
            )
        return (
            lambda cb: _lib.Consumer_seek_async(
                self._h, partition.topic(), partition.partition(), offset, cb,
            ),
            self._resolve_void, self._free_void,
        )

    def _commit_spec(self, offsets: Any) -> _Spec[None]:
        if offsets is None:
            return (
                lambda cb: _lib.Consumer_commit_sync_async(self._h, cb),
                self._resolve_void, self._free_void,
            )
        spec = offsets_to_spec(offsets)
        return (
            lambda cb: _lib.Consumer_commit_sync_offsets_async(self._h, spec, cb),
            self._resolve_void, self._free_void,
        )

    def _position_spec(self, partition: TopicPartition) -> _Spec[int]:
        return (
            lambda cb: _lib.Consumer_position_async(
                self._h, partition.topic(), partition.partition(), cb,
            ),
            self._resolve_position, self._free_position,
        )

    def _committed_spec(self, partitions: Any) -> _Spec[dict[TopicPartition, OffsetAndMetadata | None]]:
        spec = tp_to_spec(partitions)
        # Java maps an unfetched partition to a null value; ensure every
        # requested partition is present.
        requested = list(partitions)

        def resolve(payload: tuple[Any, ...]) -> Any:
            handle, error = payload
            if error:
                raise from_ffi_error(error)
            result = to_offset_map(_lib.OffsetMap_drain(handle)) if handle else {}
            for tp in requested:
                result.setdefault(tp, None)
            return result

        return (
            lambda cb: _lib.Consumer_committed_async(self._h, spec, cb),
            resolve, self._free_map(_lib.OffsetMap_drain),
        )

    def _offsets_for_times_spec(self, timestamps: Any) -> _Spec[dict[TopicPartition, OffsetAndTimestamp | None]]:
        spec = timestamps_to_spec(timestamps)
        requested = list(timestamps.keys())

        def resolve(payload: tuple[Any, ...]) -> Any:
            handle, error = payload
            if error:
                raise from_ffi_error(error)
            result = to_offset_and_timestamp_map(
                _lib.OffsetAndTimestampMap_drain(handle)
            ) if handle else {}
            # Java maps a partition with no offset for the timestamp to null.
            for tp in requested:
                result.setdefault(tp, None)
            return result

        return (
            lambda cb: _lib.Consumer_offsets_for_times_async(self._h, spec, cb),
            resolve, self._free_map(_lib.OffsetAndTimestampMap_drain),
        )

    def _long_offsets_spec(self, fn: Any, partitions: Any) -> _Spec[dict[TopicPartition, int]]:
        spec = tp_to_spec(partitions)
        return (
            lambda cb: fn(self._h, spec, cb),
            self._resolve_map(_lib.LongOffsetMap_drain, to_long_map),
            self._free_map(_lib.LongOffsetMap_drain),
        )

    def _partitions_for_spec(self, topic: str) -> _Spec[list[PartitionInfo]]:
        return (
            lambda cb: _lib.Consumer_partitions_for_async(self._h, topic, cb),
            self._resolve_map(_lib.PartitionInfoList_drain, to_partition_info_list),
            self._free_map(_lib.PartitionInfoList_drain),
        )

    def _list_topics_spec(self) -> _Spec[dict[str, list[PartitionInfo]]]:
        return (
            lambda cb: _lib.Consumer_list_topics_async(self._h, cb),
            self._resolve_map(_lib.TopicPartitionInfoMap_drain, to_topics_map),
            self._free_map(_lib.TopicPartitionInfoMap_drain),
        )

    def _close_spec(self, timeout_ms: int, operation_code: int) -> _Spec[None]:
        return (
            lambda cb: _lib.Consumer_close_options_async(self._h, timeout_ms, operation_code, cb),
            self._resolve_void, self._free_void,
        )

    # ---- synchronous state reads (both classes) ------------------------
    def assignment(self) -> set[TopicPartition]:
        """Java ``assignment()`` — the currently assigned partitions."""
        self._check_closed()
        raw = _lib.Consumer_assignment(self._h)
        if raw is None:
            raise _concurrent_error()
        return {TopicPartition(topic=t, partition=p) for (t, p) in raw}

    def subscription(self) -> set[str]:
        """Java ``subscription()`` — the currently subscribed topics."""
        self._check_closed()
        raw = _lib.Consumer_subscription(self._h)
        if raw is None:
            raise _concurrent_error()
        return set(raw)

    def paused(self) -> set[TopicPartition]:
        """Java ``paused()`` — the currently paused partitions."""
        self._check_closed()
        raw = _lib.Consumer_paused(self._h)
        if raw is None:
            raise _concurrent_error()
        return {TopicPartition(topic=t, partition=p) for (t, p) in raw}

    def current_lag(self, *, partition: TopicPartition) -> int | None:
        """Java ``currentLag(TopicPartition)`` — the consumer's current lag, or
        ``None`` if unknown (``OptionalLong.empty()``)."""
        self._check_closed()
        lag: int | None = _lib.Consumer_current_lag(
            self._h, partition.topic(), partition.partition()
        )
        return lag

    def group_metadata(self) -> ConsumerGroupMetadata:
        """Java ``groupMetadata()`` — the group membership metadata."""
        self._check_closed()
        native = _lib.Consumer_group_metadata(self._h)
        if native is None:
            raise _concurrent_error()
        return ConsumerGroupMetadata(
            group_id=native.group_id,
            generation_id=native.generation_id,
            member_id=native.member_id,
            group_instance_id=native.group_instance_id,
        )

    def metrics(self) -> dict[MetricName, Metric]:
        """Java ``metrics()`` — the consumer metrics keyed by ``MetricName``."""
        self._check_closed()
        raw = _lib.Consumer_metrics(self._h)
        if raw is None:
            raise _concurrent_error()
        return to_metrics_map(raw)

    def wakeup(self) -> None:
        """Java ``wakeup()`` — interrupt the in-flight blocking call, which
        raises ``WakeupError``. Callable from any thread (the one method that
        is, spec §6.6)."""
        if self._h is not None:
            _lib.Consumer_wakeup(self._h)
