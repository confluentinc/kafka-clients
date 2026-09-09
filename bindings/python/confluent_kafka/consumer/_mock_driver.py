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

"""The ``MockConsumer`` driver surface, shared by the sync and async mocks.

Everything except ``rebalance`` (which fires the listener and so is sync on the
sync mock but a coroutine on the async mock — spec §3 principle 5) lives here.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka.common.errors import from_ffi_error, to_ffi_id
from confluent_kafka.common.errors._base import KafkaError
from confluent_kafka.common.metric import KafkaMetric
from confluent_kafka.common.partition_info import PartitionInfo
from confluent_kafka.common.topic_partition import TopicPartition
from confluent_kafka.common.uuid import Uuid

from ._unsupported import raise_unsupported
from .consumer_record import ConsumerRecord


def _raise_if_error(error_handle: int) -> None:
    if error_handle:
        raise from_ffi_error(error_handle)


def _as_bytes(value: object) -> bytes | None:
    """The serialized bytes for a mock record key/value: ``None`` passes through
    (tombstone), ``bytes``-like is used as-is."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError(
        "MockConsumer.add_record key/value must be bytes-like (the serialized "
        f"form); got {type(value).__name__}"
    )


class _MockDriverMixin:
    """Java ``MockConsumer``'s public test-helper surface (sans ``rebalance``).

    Each method is inherent (not on the ``Consumer`` trait). ``self._h`` /
    ``self._check_closed`` come from the engine.
    """

    __slots__ = ()

    def add_record(self, *, record: ConsumerRecord[Any, Any]) -> None:
        """Java ``addRecord(ConsumerRecord)`` — buffer a record for ``poll()``.
        ``record.key()`` / ``record.value()`` carry the serialized bytes (the
        mock deserializes on ``poll``, spec §6.2)."""
        self._check_closed()  # type: ignore[attr-defined]
        _raise_if_error(_lib.MockConsumer_add_record(
            self._h, record.topic(), record.partition(), record.offset(),  # type: ignore[attr-defined]
            _as_bytes(record.key()), _as_bytes(record.value()),
        ))

    def update_beginning_offsets(self, *,
                                 offsets: Mapping[TopicPartition, int]) -> None:
        """Java ``updateBeginningOffsets(Map)``."""
        self._check_closed()  # type: ignore[attr-defined]
        for tp, offset in offsets.items():
            _raise_if_error(_lib.MockConsumer_update_beginning_offsets(
                self._h, tp.topic(), tp.partition(), offset))  # type: ignore[attr-defined]

    def update_end_offsets(self, *,
                           offsets: Mapping[TopicPartition, int]) -> None:
        """Java ``updateEndOffsets(Map)``."""
        self._check_closed()  # type: ignore[attr-defined]
        for tp, offset in offsets.items():
            _raise_if_error(_lib.MockConsumer_update_end_offsets(
                self._h, tp.topic(), tp.partition(), offset))  # type: ignore[attr-defined]

    def update_duration_offsets(self, *,
                                offsets: Mapping[TopicPartition, int]) -> None:
        """Java ``updateDurationOffsets(Map)``."""
        self._check_closed()  # type: ignore[attr-defined]
        for tp, offset in offsets.items():
            _raise_if_error(_lib.MockConsumer_update_duration_offsets(
                self._h, tp.topic(), tp.partition(), offset))  # type: ignore[attr-defined]

    def update_partitions(self, *, topic: str,
                          partitions: list[PartitionInfo]) -> None:
        """Java ``updatePartitions(String, List<PartitionInfo>)``. Simplified to
        the leader's id/host/port (the mock keeps only that)."""
        self._check_closed()  # type: ignore[attr-defined]
        leader_id, leader_host, leader_port = 0, "", 0
        if partitions:
            first_leader = partitions[0].leader()
            if first_leader is not None:
                leader_id = first_leader.id()
                leader_host = first_leader.host()
                leader_port = first_leader.port()
        _raise_if_error(_lib.MockConsumer_update_partitions(
            self._h, topic, len(partitions), leader_id, leader_host, leader_port))  # type: ignore[attr-defined]

    def set_poll_exception(self, *, error: KafkaError | None) -> None:
        """Java ``setPollException(KafkaException)`` — the next ``poll`` raises
        this once. ``None`` is a no-op (the FFI has no clear)."""
        self._check_closed()  # type: ignore[attr-defined]
        if error is None:
            return
        _raise_if_error(_lib.MockConsumer_set_poll_exception(
            self._h, to_ffi_id(error), str(error)))  # type: ignore[attr-defined]

    def set_offsets_exception(self, *, error: KafkaError | None) -> None:
        """Java ``setOffsetsException(KafkaException)``."""
        self._check_closed()  # type: ignore[attr-defined]
        if error is None:
            return
        _raise_if_error(_lib.MockConsumer_set_offsets_exception(
            self._h, to_ffi_id(error), str(error)))  # type: ignore[attr-defined]

    def set_max_poll_records(self, *, max_poll_records: int) -> None:
        """Java ``setMaxPollRecords(long)`` — ``IllegalArgumentError`` when
        ``< 1``."""
        self._check_closed()  # type: ignore[attr-defined]
        _raise_if_error(_lib.MockConsumer_set_max_poll_records(
            self._h, max_poll_records))  # type: ignore[attr-defined]

    def should_rebalance(self) -> bool:
        """Java ``shouldRebalance()``."""
        self._check_closed()  # type: ignore[attr-defined]
        result: bool = _lib.MockConsumer_should_rebalance(self._h)  # type: ignore[attr-defined]
        return result

    def reset_should_rebalance(self) -> None:
        """Java ``resetShouldRebalance()``."""
        self._check_closed()  # type: ignore[attr-defined]
        _lib.MockConsumer_reset_should_rebalance(self._h)  # type: ignore[attr-defined]

    def schedule_poll_task(self, *, task: Callable[[], None]) -> None:
        """Java ``schedulePollTask(Runnable)``. The general task form is not
        wired (the core's task takes ``&mut MockConsumer``); use
        ``schedule_nop_poll_task`` for the no-op case."""
        raise_unsupported("schedule_poll_task")

    def schedule_nop_poll_task(self) -> None:
        """Java ``scheduleNopPollTask()``."""
        self._check_closed()  # type: ignore[attr-defined]
        _raise_if_error(_lib.MockConsumer_schedule_nop_poll_task(self._h))  # type: ignore[attr-defined]

    def last_poll_timeout(self) -> float | None:
        """Java ``lastPollTimeout()`` — the timeout (seconds) of the most recent
        ``poll``, or ``None`` if never polled."""
        self._check_closed()  # type: ignore[attr-defined]
        result: float | None = _lib.MockConsumer_last_poll_timeout(self._h)  # type: ignore[attr-defined]
        return result

    def closed(self) -> bool:
        """Java ``closed()`` — whether the consumer has been closed."""
        if self._h is None:  # type: ignore[attr-defined]
            return True
        result: bool = _lib.MockConsumer_closed(self._h)  # type: ignore[attr-defined]
        return result

    def set_client_instance_id(self, *, instance_id: Uuid) -> None:
        """Java ``setClientInstanceId(Uuid)`` (KIP-714)."""
        raise_unsupported("set_client_instance_id")

    def inject_timeout_exception(self, *, counter: int) -> None:
        """Java ``injectTimeoutException(int)`` (KIP-714)."""
        raise_unsupported("inject_timeout_exception")

    def disable_telemetry(self) -> None:
        """Java ``disableTelemetry()`` (KIP-714)."""
        raise_unsupported("disable_telemetry")

    def added_metrics(self) -> list[KafkaMetric]:
        """Java ``addedMetrics()`` (KIP-714)."""
        raise_unsupported("added_metrics")
