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

"""``Consumer`` — the non-instantiable base of the synchronous consumer family.

Translated from ``org.apache.kafka.clients.consumer.Consumer`` (Apache Kafka
4.3.1). Every consumer method lives here; ``KafkaConsumer`` and ``MockConsumer``
inherit and add only a constructor (spec §3 principle 6). The base ``__init__``
raises ``TypeError`` naming the concrete classes.

Blocking-in-Java methods drive the FFI through ``_run_sync`` and return their
result on the caller's thread (spec §1). The pure state reads that never block
(``assignment`` / ``subscription`` / ``paused`` / ``current_lag`` /
``group_metadata`` / ``metrics`` / ``wakeup``) come from ``_ConsumerClientBase``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Generic, TypeVar, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka import Duration
from confluent_kafka._args import at_most_one, exactly_one
from confluent_kafka._config import duration_to_ms
from confluent_kafka.common.metric import KafkaMetric
from confluent_kafka.common.partition_info import PartitionInfo
from confluent_kafka.common.uuid import Uuid
from confluent_kafka.common.topic_partition import TopicPartition

from ._client_base import _ConsumerClientBase
from .close_options import CloseOptions
from .consumer_records import ConsumerRecords
from .consumer_rebalance_listener import CommitCallback, ConsumerRebalanceListener
from .offset_and_metadata import OffsetAndMetadata
from .offset_and_timestamp import OffsetAndTimestamp
from .subscription_pattern import SubscriptionPattern
from ._unsupported import raise_unsupported

K = TypeVar("K")
V = TypeVar("V")

# Java's default.api.timeout.ms fallback for the Duration-less overloads.
_DEFAULT_API_TIMEOUT_MS = 60_000

_GROUP_OP_CODE = {
    CloseOptions.GroupMembershipOperation.DEFAULT: 0,
    CloseOptions.GroupMembershipOperation.LEAVE_GROUP: 1,
    CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP: 2,
}


class Consumer(_ConsumerClientBase, Generic[K, V]):
    """Non-instantiable base — the synchronous ``Consumer`` interface.

    Java: ``interface Consumer<K, V>``. Construct a ``KafkaConsumer`` or
    ``MockConsumer``.
    """

    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if type(self) is Consumer:
            raise TypeError(
                "Consumer is a non-instantiable base; use KafkaConsumer or "
                "MockConsumer"
            )

    # ---- subscription & assignment -------------------------------------
    @overload
    def subscribe(self, *, topics: Iterable[str],
                  listener: ConsumerRebalanceListener | None = None) -> None: ...
    @overload
    def subscribe(self, *, pattern: SubscriptionPattern,
                  listener: ConsumerRebalanceListener | None = None) -> None: ...

    def subscribe(self, *, topics: Iterable[str] | None = None,
                  pattern: SubscriptionPattern | None = None,
                  listener: ConsumerRebalanceListener | None = None) -> None:
        """Subscribe to ``topics`` or a broker-side RE2/J ``pattern``, optionally
        with a rebalance ``listener`` (invoked on the caller's thread during
        ``poll()`` etc., §31)."""
        self._check_closed()
        chosen = exactly_one("subscribe", topics=topics, pattern=pattern)
        self._listener = listener
        if chosen == "topics":
            topic_list = list(topics)  # type: ignore[arg-type]
            self._run_sync(*self._subscribe_topics_spec(topic_list, listener is not None))
        else:
            assert pattern is not None
            self._run_sync(*self._subscribe_pattern_spec(pattern.pattern(), listener is not None))

    def unsubscribe(self) -> None:
        """Java ``unsubscribe()``."""
        self._check_closed()
        self._run_sync(*self._unsubscribe_spec())

    def assign(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Java ``assign(Collection<TopicPartition>)`` — manual assignment."""
        self._check_closed()
        if self._in_callback:
            self._reentrant_tp_op("ConsumerHandle_assign", partitions)
            return
        self._run_sync(*self._tp_op_spec(_lib.Consumer_assign_async, partitions))

    def pause(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Java ``pause(Collection<TopicPartition>)``."""
        self._check_closed()
        if self._in_callback:
            self._reentrant_tp_op("ConsumerHandle_pause", partitions)
            return
        self._run_sync(*self._tp_op_spec(_lib.Consumer_pause_async, partitions))

    def resume(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Java ``resume(Collection<TopicPartition>)``."""
        self._check_closed()
        if self._in_callback:
            self._reentrant_tp_op("ConsumerHandle_resume", partitions)
            return
        self._run_sync(*self._tp_op_spec(_lib.Consumer_resume_async, partitions))

    # ---- consume -------------------------------------------------------
    def poll(self, *, timeout: Duration) -> ConsumerRecords[K, V]:
        """Java ``poll(Duration)`` — fetch records, running the deserializers on
        the caller's thread; a failing deserializer raises
        ``RecordDeserializationError`` and the position does not move (§5.4)."""
        self._check_closed()
        ms = duration_to_ms(timeout, default_ms=0)
        return self._run_sync(*self._poll_spec(ms))

    # ---- offsets -------------------------------------------------------
    def commit(self, *,
               offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
               timeout: Duration | None = None) -> None:
        """Java ``commitSync`` — commit and wait for the ack. ``commit_nowait``
        is Java ``commitAsync`` (returns immediately)."""
        self._check_closed()
        # timeout has no timed FFI form on commit_sync yet; unwired (D7, C36).
        if self._in_callback:
            self._reentrant_commit(offsets)
            return
        self._run_sync(*self._commit_spec(offsets))

    def commit_nowait(self, *,
                      offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
                      on_commit: CommitCallback | None = None) -> None:
        """Java ``commitAsync`` — commit without waiting, optional callback."""
        self._check_closed()
        if self._in_callback and on_commit is None:
            self._reentrant_commit_nowait(offsets)
            return
        adapter = self._wrap_commit_callback(on_commit)
        if offsets is None:
            if adapter is None:
                error = _lib.Consumer_commit_async(self._h)
            else:
                error = _lib.Consumer_commit_async(self._h, adapter)
        else:
            from ._conversions import offsets_to_spec
            spec = offsets_to_spec(offsets)
            if adapter is None:
                error = _lib.Consumer_commit_async_offsets(self._h, spec)
            else:
                error = _lib.Consumer_commit_async_offsets(self._h, spec, adapter)
        if error:
            from confluent_kafka.common.errors import from_ffi_error
            raise from_ffi_error(error)

    def committed(self, *, partitions: Iterable[TopicPartition],
                  timeout: Duration | None = None
                  ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        """Java ``committed(Set)`` — the last committed offsets; a partition with
        no committed offset maps to ``None`` (D25 ruling C)."""
        self._check_closed()
        if self._in_callback:
            return self._reentrant_committed(list(partitions))
        return self._run_sync(*self._committed_spec(partitions))

    def position(self, *, partition: TopicPartition,
                 timeout: Duration | None = None) -> int:
        """Java ``position(TopicPartition)`` — the next offset to be fetched."""
        self._check_closed()
        if self._in_callback:
            return self._reentrant_position(partition)
        return self._run_sync(*self._position_spec(partition))

    @overload
    def seek(self, *, partition: TopicPartition, offset: int) -> None: ...
    @overload
    def seek(self, *, partition: TopicPartition,
             offset_and_metadata: OffsetAndMetadata) -> None: ...

    def seek(self, *, partition: TopicPartition, offset: int | None = None,
             offset_and_metadata: OffsetAndMetadata | None = None) -> None:
        """Java ``seek(TopicPartition, long)`` / ``seek(TopicPartition,
        OffsetAndMetadata)``. Also the poison-pill recovery device (§5.4)."""
        self._check_closed()
        exactly_one("seek", offset=offset, offset_and_metadata=offset_and_metadata)
        if self._in_callback:
            self._reentrant_seek(partition, offset, offset_and_metadata)
            return
        self._run_sync(*self._seek_spec(partition, offset, offset_and_metadata))

    def seek_to_beginning(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Java ``seekToBeginning(Collection)``."""
        self._check_closed()
        if self._in_callback:
            self._reentrant_tp_op("ConsumerHandle_seek_to_beginning", partitions)
            return
        self._run_sync(*self._tp_op_spec(_lib.Consumer_seek_to_beginning_async, partitions))

    def seek_to_end(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Java ``seekToEnd(Collection)``."""
        self._check_closed()
        if self._in_callback:
            self._reentrant_tp_op("ConsumerHandle_seek_to_end", partitions)
            return
        self._run_sync(*self._tp_op_spec(_lib.Consumer_seek_to_end_async, partitions))

    def beginning_offsets(self, *, partitions: Iterable[TopicPartition],
                          timeout: Duration | None = None
                          ) -> dict[TopicPartition, int]:
        """Java ``beginningOffsets(Collection)``."""
        self._check_closed()
        if self._in_callback:
            return self._reentrant_long_offsets(
                "ConsumerHandle_beginning_offsets", partitions)
        return self._run_sync(*self._long_offsets_spec(
            _lib.Consumer_beginning_offsets_async, partitions))

    def end_offsets(self, *, partitions: Iterable[TopicPartition],
                    timeout: Duration | None = None
                    ) -> dict[TopicPartition, int]:
        """Java ``endOffsets(Collection)``."""
        self._check_closed()
        if self._in_callback:
            return self._reentrant_long_offsets(
                "ConsumerHandle_end_offsets", partitions)
        return self._run_sync(*self._long_offsets_spec(
            _lib.Consumer_end_offsets_async, partitions))

    def offsets_for_times(self, *, timestamps: Mapping[TopicPartition, int],
                          timeout: Duration | None = None
                          ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        """Java ``offsetsForTimes(Map)`` — the earliest offset whose timestamp is
        ≥ the requested one, or ``None`` if none (Java null)."""
        self._check_closed()
        if self._in_callback:
            return self._reentrant_offsets_for_times(timestamps)
        return self._run_sync(*self._offsets_for_times_spec(timestamps))

    # ---- metadata & observability --------------------------------------
    def partitions_for(self, *, topic: str,
                       timeout: Duration | None = None) -> list[PartitionInfo]:
        """Java ``partitionsFor(String)``."""
        self._check_closed()
        return self._run_sync(*self._partitions_for_spec(topic))

    def list_topics(self, *, timeout: Duration | None = None
                    ) -> dict[str, list[PartitionInfo]]:
        """Java ``listTopics()``."""
        self._check_closed()
        return self._run_sync(*self._list_topics_spec())

    def register_metric_for_subscription(self, *, metric: KafkaMetric) -> None:
        """Java ``registerMetricForSubscription(KafkaMetric)`` (KIP-1076)."""
        raise_unsupported("register_metric_for_subscription")

    def unregister_metric_from_subscription(self, *, metric: KafkaMetric) -> None:
        """Java ``unregisterMetricFromSubscription(KafkaMetric)`` (KIP-1076)."""
        raise_unsupported("unregister_metric_from_subscription")

    def client_instance_id(self, *, timeout: Duration | None = None) -> Uuid:
        """Java ``clientInstanceId(Duration)`` (KIP-714 telemetry)."""
        # Java validates a negative timeout first (IllegalArgumentException).
        if timeout is not None:
            duration_to_ms(timeout, default_ms=_DEFAULT_API_TIMEOUT_MS)
        raise_unsupported("client_instance_id")

    # ---- lifecycle -----------------------------------------------------
    @overload
    def close(self, *, timeout: Duration | None = None) -> None: ...
    @overload
    def close(self, *, option: CloseOptions) -> None: ...

    def close(self, *, timeout: Duration | None = None,
              option: CloseOptions | None = None) -> None:
        """Java ``close()`` / ``close(Duration)`` / ``close(CloseOptions)``.
        Idempotent; leaves the group per the ``option`` (``DEFAULT`` matches
        Java: static members remain, dynamic members leave)."""
        at_most_one("close", timeout=timeout, option=option)
        if self._closed or self._h is None:
            return
        self._closed = True
        try:
            timeout_ms, op_code = _close_args(timeout, option)
            self._run_sync(*self._close_spec(timeout_ms, op_code))
        finally:
            self._destroy()

    def __enter__(self) -> Consumer[K, V]:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def _close_args(timeout: Duration | None,
                option: CloseOptions | None) -> tuple[int, int]:
    """Resolve (timeout_ms, group-membership-operation-code) for close. A
    negative ``timeout_ms`` (``-1``) tells the FFI to use the default close
    timeout."""
    if option is not None:
        opt_timeout = option._timeout_getter()
        op = option._group_membership_operation_getter()
        timeout_ms = duration_to_ms(opt_timeout, default_ms=-1) if opt_timeout is not None else -1
        return timeout_ms, _GROUP_OP_CODE[op]
    timeout_ms = duration_to_ms(timeout, default_ms=-1) if timeout is not None else -1
    return timeout_ms, 0
