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

"""The body of ``MockConsumer`` and ``AsyncMockConsumer`` (private): a direct
Python translation of Java's ``org.apache.kafka.clients.consumer.MockConsumer``.

*(deviation, logged for the post-phase review)* The mock is not FFI-backed,
although the Implementation-over-the-FFI rule lists ``MockConsumer`` with the
clients: the FFI's ``kafka_consumer_MockConsumer_add_record`` carries only a
topic, partition, offset and serialized key/value, while Java's mock buffers
the very ``ConsumerRecord`` objects it is given and returns them from
``poll()`` undeserialized, with their timestamps, headers and leader epochs
(which it also records in the positions ``commit()`` commits); the core's
``ConsumerHandle`` rejects the blocking calls a listener makes on a mock; and
``schedulePollTask(Runnable)`` has no entry point. Translated from Java, the
mock keeps all of that, as ``MockProducer`` does.

Java's ``synchronized`` methods take one reentrant lock, so a listener or a poll
task may call back into the mock from the thread holding it, and another thread
waits instead of failing (Java's mock is not thread-safe but serializes).
``wakeup()`` only sets its flag, as Java's ``AtomicBoolean``.

``AsyncMockConsumer`` awaits the same body; ``rebalance`` awaits a coroutine
listener method there.
"""

from __future__ import annotations

import inspect
import logging
import threading
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from confluent_kafka.common.errors.unsupported_version_error import UnsupportedVersionError
from confluent_kafka.common.errors.wakeup_error import WakeupError
from confluent_kafka.common.topic_partition import TopicPartition
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError

from ._auto_offset_reset_strategy import AutoOffsetResetStrategy
from ._subscription_state import FetchPosition, SubscriptionState
from .consumer_group_metadata import ConsumerGroupMetadata
from .consumer_records import ConsumerRecords
from .no_offset_for_partition_error import NoOffsetForPartitionError
from .offset_and_metadata import OffsetAndMetadata
from .offset_out_of_range_error import OffsetOutOfRangeError

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common.kafka_error import KafkaError
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.metric_name import MetricName
    from confluent_kafka.common.partition_info import PartitionInfo

    from .close_options import CloseOptions
    from .consumer_rebalance_listener import ConsumerRebalanceListener
    from .consumer_record import ConsumerRecord
    from .offset_and_timestamp import OffsetAndTimestamp
    from .offset_commit_callback import OffsetCommitCallback
    from .subscription_pattern import SubscriptionPattern

__all__ = ["MockConsumerCore"]

K = TypeVar("K")
V = TypeVar("V")

_LOG = logging.getLogger("confluent_kafka.consumer")

_LONG_MAX = (1 << 63) - 1


def _seconds(timeout: Duration) -> float:
    return timeout.total_seconds() if isinstance(timeout, timedelta) else float(timeout)


def _no_coroutine(result: object, what: str) -> None:
    """A synchronous mock cannot await a coroutine a callback returned."""
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if callable(close):
            close()
        raise TypeError(f"a coroutine {what} requires an AsyncMockConsumer")


class MockConsumerCore(Generic[K, V]):
    """Java's ``MockConsumer`` state and methods, shared by the sync and async
    mocks. The interface methods are defined once, on ``Consumer`` /
    ``AsyncConsumer``, which call these hooks (``_c_*`` / ``_a_*``) in place of
    the FFI ones."""

    def _init_mock(self, offset_reset_strategy: AutoOffsetResetStrategy) -> None:
        """Java's private ``MockConsumer(AutoOffsetResetStrategy)``."""
        self._lock = threading.RLock()
        self._subscriptions = SubscriptionState(offset_reset_strategy)
        self._partitions: dict[str, list[PartitionInfo]] = {}
        self._records: dict[TopicPartition, list[ConsumerRecord[K, V]]] = {}
        self._paused_partitions: set[TopicPartition] = set()
        self._mock_closed = False
        self._beginning_offsets: dict[TopicPartition, int] = {}
        self._end_offsets: dict[TopicPartition, int] = {}
        self._duration_reset_offsets: dict[TopicPartition, int] = {}
        self._poll_tasks: deque[Callable[[], None]] = deque()
        self._poll_tasks_lock = threading.RLock()
        self._poll_exception: KafkaError | None = None
        self._offsets_exception: KafkaError | None = None
        self._wakeup_requested = False
        self._committed_offsets: dict[TopicPartition, OffsetAndMetadata] = {}
        self._last_poll_timeout: float | None = None
        self._max_poll_records = _LONG_MAX

    # ---- the interface methods ----------------------------------------------
    def _c_assignment(self) -> set[TopicPartition]:
        with self._lock:
            return self._subscriptions.assigned_partitions()

    def _rebalance_begin(self, new_assignment: Iterable[TopicPartition]
                         ) -> tuple[list[TopicPartition], list[TopicPartition],
                                    list[TopicPartition]]:
        """``rebalance``'s first half: the new assignment, the added and the
        removed partitions; the buffered records are cleared."""
        new_list = list(new_assignment)
        old_assignment = self._subscriptions.assigned_partitions()
        new_set = set(new_list)
        added = [x for x in new_list if x not in old_assignment]
        removed = [x for x in old_assignment if x not in new_set]
        self._records.clear()
        return new_list, added, removed

    def _c_rebalance(self, new_assignment: Iterable[TopicPartition]) -> None:
        """Java's ``rebalance(Collection)``: the listener runs on this thread,
        under the mock's lock, and the rebalance waits for it."""
        with self._lock:
            new_list, added, removed = self._rebalance_begin(new_assignment)
            listener = self._subscriptions.rebalance_listener()
            if removed and listener is not None:
                _no_coroutine(listener.on_partitions_revoked(set(removed)),  # type: ignore[func-returns-value]
                              "rebalance listener")
            self._subscriptions.assign_from_subscribed(new_list)
            listener = self._subscriptions.rebalance_listener()
            if listener is not None:
                _no_coroutine(listener.on_partitions_assigned(set(added)),  # type: ignore[func-returns-value]
                              "rebalance listener")

    async def _a_rebalance(self, new_assignment: Iterable[TopicPartition]) -> None:
        """Async ``rebalance``: a coroutine listener method is awaited."""
        with self._lock:
            new_list, added, removed = self._rebalance_begin(new_assignment)
            listener = self._subscriptions.rebalance_listener()
        if removed and listener is not None:
            result = listener.on_partitions_revoked(set(removed))  # type: ignore[func-returns-value]
            if inspect.isawaitable(result):
                await result
        with self._lock:
            self._subscriptions.assign_from_subscribed(new_list)
            listener = self._subscriptions.rebalance_listener()
        if listener is not None:
            result = listener.on_partitions_assigned(set(added))  # type: ignore[func-returns-value]
            if inspect.isawaitable(result):
                await result

    def _c_subscription(self) -> set[str]:
        with self._lock:
            return self._subscriptions.subscription()

    def _c_subscribe_topics(self, topics: Iterable[str],
                          callback: ConsumerRebalanceListener | None) -> None:
        """Java's private ``subscribe(Collection, Optional)``."""
        with self._lock:
            self._ensure_not_closed()
            self._committed_offsets.clear()
            self._subscriptions.subscribe(set(topics), callback)

    def _c_subscribe_pattern(self, pattern: SubscriptionPattern | None,
                           callback: ConsumerRebalanceListener | None) -> None:
        """Java's private ``subscribe(SubscriptionPattern, Optional)``."""
        if pattern is None or str(pattern) == "":
            raise IllegalArgumentError(
                message="Topic pattern cannot be " + ("null" if pattern is None else "empty"))
        with self._lock:
            self._ensure_not_closed()
            self._committed_offsets.clear()
            self._subscriptions.subscribe_pattern(pattern, callback)

    def _c_assign(self, partitions: Iterable[TopicPartition]) -> None:
        with self._lock:
            self._ensure_not_closed()
            self._committed_offsets.clear()
            self._subscriptions.assign_from_user(set(partitions))

    def _c_unsubscribe(self) -> None:
        with self._lock:
            self._ensure_not_closed()
            self._committed_offsets.clear()
            self._subscriptions.unsubscribe()

    def _c_poll(self, timeout: Duration) -> ConsumerRecords[K, V]:
        with self._lock:
            self._ensure_not_closed()
            self._last_poll_timeout = _seconds(timeout)
            # Lock around the task, so a task may schedule the next one.
            with self._poll_tasks_lock:
                task = self._poll_tasks.popleft() if self._poll_tasks else None
                if task is not None:
                    task()
            if self._wakeup_requested:
                self._wakeup_requested = False
                raise WakeupError()
            if self._poll_exception is not None:
                exception, self._poll_exception = self._poll_exception, None
                raise exception
            # Handle seeks that need to wait for a poll() call to be processed.
            for tp in self._subscriptions.assigned_partitions():
                if not self._subscriptions.has_valid_position(tp):
                    self._update_fetch_position(tp)
            # Update the consumed offset.
            results: dict[TopicPartition, list[ConsumerRecord[K, V]]] = {}
            next_offset_and_metadata: dict[TopicPartition, OffsetAndMetadata] = {}
            num_poll_records = 0
            for tp in list(self._records):
                if num_poll_records >= self._max_poll_records:
                    break
                recs = self._records[tp]
                if self._subscriptions.is_paused(tp) or not self._subscriptions.is_assigned(tp):
                    continue
                index = 0
                while index < len(recs):
                    if num_poll_records >= self._max_poll_records:
                        break
                    position = self._fetch_position(tp).offset
                    rec = recs[index]
                    beginning = self._beginning_offsets.get(tp)
                    if beginning is not None and beginning > position:
                        raise OffsetOutOfRangeError(offset_out_of_range_partitions={tp: position})
                    if rec.offset() >= position:
                        results.setdefault(tp, []).append(rec)
                        self._subscriptions.set_position(
                            tp, FetchPosition(rec.offset() + 1, rec.leader_epoch()))
                        next_offset_and_metadata[tp] = OffsetAndMetadata(
                            offset=rec.offset() + 1, leader_epoch=rec.leader_epoch(), metadata="")
                        num_poll_records += 1
                        del recs[index]
                    else:
                        index += 1
                if not recs:
                    del self._records[tp]
            return ConsumerRecords(records=results, next_offsets=next_offset_and_metadata)

    def _fetch_position(self, tp: TopicPartition) -> FetchPosition:
        position = self._subscriptions.position(tp)
        assert position is not None  # a valid position after _update_fetch_position
        return position

    def _c_commit_async(self, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                      callback: OffsetCommitCallback | None) -> None:
        """Java's ``commitAsync(Map, OffsetCommitCallback)``: the callback runs
        inline, on this thread; an exception it raises propagates."""
        with self._lock:
            self._ensure_not_closed()
            self._committed_offsets.update(offsets)
            if callback is not None:
                _no_coroutine(callback(dict(offsets), None), "OffsetCommitCallback")

    def _c_commit(self, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None) -> None:
        """Java's ``commitSync()`` / ``commitSync(Map)``."""
        with self._lock:
            self._c_commit_async(self._subscriptions.all_consumed() if offsets is None else offsets,
                               None)

    def _c_commit_nowait(self, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None,
                       callback: OffsetCommitCallback | None) -> None:
        """Java's ``commitAsync()`` / ``commitAsync(callback)`` /
        ``commitAsync(offsets, callback)``."""
        with self._lock:
            if offsets is None:
                self._ensure_not_closed()
                offsets = self._subscriptions.all_consumed()
            self._c_commit_async(offsets, callback)

    def _c_seek(self, partition: TopicPartition, offset: int | None,
              offset_and_metadata: OffsetAndMetadata | None) -> None:
        with self._lock:
            self._ensure_not_closed()
            if offset_and_metadata is not None:
                offset = offset_and_metadata.offset()
            assert offset is not None
            self._subscriptions.seek(partition, offset)

    def _c_committed(self, partitions: Iterable[TopicPartition]
                           ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        """Java's ``committed(Set)``: only the partitions with a committed offset;
        an unassigned one reports offset 0."""
        with self._lock:
            self._ensure_not_closed()
            return {tp: (self._committed_offsets[tp] if self._subscriptions.is_assigned(tp)
                         else OffsetAndMetadata(offset=0))
                    for tp in partitions if tp in self._committed_offsets}

    def _c_position(self, partition: TopicPartition) -> int:
        with self._lock:
            self._ensure_not_closed()
            if not self._subscriptions.is_assigned(partition):
                raise IllegalArgumentError(
                    message="You can only check the position for partitions assigned to this "
                            "consumer.")
            position = self._subscriptions.position(partition)
            if position is None:
                self._update_fetch_position(partition)
                position = self._fetch_position(partition)
            return position.offset

    def _c_seek_to_beginning(self, partitions: Iterable[TopicPartition]) -> None:
        with self._lock:
            self._ensure_not_closed()
            self._subscriptions.request_offset_reset_partitions(
                list(partitions), AutoOffsetResetStrategy.EARLIEST)

    def _c_seek_to_end(self, partitions: Iterable[TopicPartition]) -> None:
        with self._lock:
            self._ensure_not_closed()
            self._subscriptions.request_offset_reset_partitions(
                list(partitions), AutoOffsetResetStrategy.LATEST)

    def _c_metrics(self) -> dict[MetricName, Metric]:
        with self._lock:
            self._ensure_not_closed()
            return {}

    def _c_partitions_for(self, topic: str) -> list[PartitionInfo]:
        with self._lock:
            self._ensure_not_closed()
            return list(self._partitions.get(topic, []))

    def _c_list_topics(self) -> dict[str, list[PartitionInfo]]:
        with self._lock:
            self._ensure_not_closed()
            return {topic: list(partitions) for topic, partitions in self._partitions.items()}

    def _c_paused(self) -> set[TopicPartition]:
        with self._lock:
            return set(self._paused_partitions)

    def _c_pause(self, partitions: Iterable[TopicPartition]) -> None:
        with self._lock:
            for partition in partitions:
                self._subscriptions.pause(partition)
                self._paused_partitions.add(partition)

    def _c_resume(self, partitions: Iterable[TopicPartition]) -> None:
        with self._lock:
            for partition in partitions:
                self._subscriptions.resume(partition)
                self._paused_partitions.discard(partition)

    def _c_offsets_for_times(self, timestamps_to_search: Mapping[TopicPartition, int]
                           ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        # Java's mock throws UnsupportedOperationException("Not implemented
        # yet.") (MockConsumer.java:536).
        raise UnsupportedVersionError(message="Not implemented yet.")

    def _offsets_of(self, partitions: Iterable[TopicPartition],
                    offsets: dict[TopicPartition, int], which: str) -> dict[TopicPartition, int]:
        with self._lock:
            if self._offsets_exception is not None:
                exception, self._offsets_exception = self._offsets_exception, None
                raise exception
            result: dict[TopicPartition, int] = {}
            for tp in partitions:
                offset = offsets.get(tp)
                if offset is None:
                    raise IllegalStateError(
                        message=f"The partition {tp} does not have {which} offset.")
                result[tp] = offset
            return result

    def _c_beginning_offsets(self, partitions: Iterable[TopicPartition]) -> dict[TopicPartition, int]:
        return self._offsets_of(partitions, self._beginning_offsets, "a beginning")

    def _c_end_offsets(self, partitions: Iterable[TopicPartition]) -> dict[TopicPartition, int]:
        return self._offsets_of(partitions, self._end_offsets, "an end")

    def _c_current_lag(self, topic_partition: TopicPartition) -> int | None:
        if topic_partition in self._end_offsets:
            return self._end_offsets[topic_partition] - self._c_position(topic_partition)
        # If the test doesn't bother to set an end offset, we assume it wants to
        # model being caught up.
        return 0

    def _c_group_metadata(self) -> ConsumerGroupMetadata:
        return ConsumerGroupMetadata._of(group_id="dummy.group.id", generation_id=1,
                                         member_id="1", group_instance_id=None)

    def _c_close(self, option: CloseOptions | None) -> None:
        """Java's ``close()`` / ``close(CloseOptions)``, both of which only mark
        the mock closed; the callbacks it holds are released (Threads and
        callbacks)."""
        with self._lock:
            self._mock_closed = True
            self._subscriptions.release_rebalance_listener()
            with self._poll_tasks_lock:
                self._poll_tasks.clear()

    def _c_wakeup(self) -> None:
        self._wakeup_requested = True

    # ---- the async peer's waiting methods: the same body ----------------------
    async def _a_subscribe_topics(self, topics: Iterable[str],
                                  callback: ConsumerRebalanceListener | None) -> None:
        self._c_subscribe_topics(topics, callback)

    async def _a_subscribe_pattern(self, pattern: SubscriptionPattern | None,
                                   callback: ConsumerRebalanceListener | None) -> None:
        self._c_subscribe_pattern(pattern, callback)

    async def _a_assign(self, partitions: Iterable[TopicPartition]) -> None:
        self._c_assign(partitions)

    async def _a_unsubscribe(self) -> None:
        self._c_unsubscribe()

    async def _a_poll(self, timeout: Duration) -> ConsumerRecords[K, V]:
        return self._c_poll(timeout)

    async def _a_commit(self, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None) -> None:
        self._c_commit(offsets)

    async def _a_seek(self, partition: TopicPartition, offset: int | None,
                      offset_and_metadata: OffsetAndMetadata | None) -> None:
        self._c_seek(partition, offset, offset_and_metadata)

    async def _a_seek_to_beginning(self, partitions: Iterable[TopicPartition]) -> None:
        self._c_seek_to_beginning(partitions)

    async def _a_seek_to_end(self, partitions: Iterable[TopicPartition]) -> None:
        self._c_seek_to_end(partitions)

    async def _a_position(self, partition: TopicPartition) -> int:
        return self._c_position(partition)

    async def _a_committed(self, partitions: Iterable[TopicPartition]
                           ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        return self._c_committed(partitions)

    async def _a_partitions_for(self, topic: str) -> list[PartitionInfo]:
        return self._c_partitions_for(topic)

    async def _a_list_topics(self) -> dict[str, list[PartitionInfo]]:
        return self._c_list_topics()

    async def _a_pause(self, partitions: Iterable[TopicPartition]) -> None:
        self._c_pause(partitions)

    async def _a_resume(self, partitions: Iterable[TopicPartition]) -> None:
        self._c_resume(partitions)

    async def _a_offsets_for_times(self, timestamps_to_search: Mapping[TopicPartition, int]
                                   ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        return self._c_offsets_for_times(timestamps_to_search)

    async def _a_beginning_offsets(self, partitions: Iterable[TopicPartition]
                                   ) -> dict[TopicPartition, int]:
        return self._c_beginning_offsets(partitions)

    async def _a_end_offsets(self, partitions: Iterable[TopicPartition]
                             ) -> dict[TopicPartition, int]:
        return self._c_end_offsets(partitions)

    async def _a_close(self, option: CloseOptions | None) -> None:
        self._c_close(option)

    # ---- the Java mock's own methods ------------------------------------------
    def add_record(self, *, record: ConsumerRecord[K, V]) -> None:
        """Adds a record to be returned by a later ``poll()``, as given (it is
        not deserialized). Raises ``IllegalStateError`` when its partition is not
        assigned to the consumer."""
        with self._lock:
            self._ensure_not_closed()
            tp = TopicPartition(topic=record.topic(), partition=record.partition())
            if tp not in self._subscriptions.assigned_partitions():
                raise IllegalStateError(
                    message="Cannot add records for a partition that is not assigned to the "
                            "consumer")
            self._records.setdefault(tp, []).append(record)

    def set_max_poll_records(self, *, max_poll_records: int) -> None:
        """Sets the maximum number of records returned in a single call to
        ``poll()``."""
        with self._lock:
            if max_poll_records < 1:
                raise IllegalArgumentError(
                    message="MaxPollRecords must be strictly superior to 0")
            self._max_poll_records = max_poll_records

    def set_poll_exception(self, *, exception: KafkaError | None) -> None:
        """The next ``poll()`` raises ``exception``, the very instance; ``None``
        clears a pending one."""
        with self._lock:
            self._poll_exception = exception

    def set_offsets_exception(self, *, exception: KafkaError | None) -> None:
        """The next ``beginning_offsets()`` / ``end_offsets()`` raises
        ``exception``, the very instance; ``None`` clears a pending one."""
        with self._lock:
            self._offsets_exception = exception

    def update_beginning_offsets(self, *, new_offsets: Mapping[TopicPartition, int]) -> None:
        with self._lock:
            self._beginning_offsets.update(new_offsets)

    def update_end_offsets(self, *, new_offsets: Mapping[TopicPartition, int]) -> None:
        with self._lock:
            self._end_offsets.update(new_offsets)

    def update_duration_offsets(self, *, new_offsets: Mapping[TopicPartition, int]) -> None:
        with self._lock:
            self._duration_reset_offsets.update(new_offsets)

    def update_partitions(self, *, topic: str, partitions: Sequence[PartitionInfo]) -> None:
        with self._lock:
            self._ensure_not_closed()
            self._partitions[topic] = list(partitions)

    def closed(self) -> bool:
        with self._lock:
            return self._mock_closed

    def schedule_poll_task(self, *, task: Callable[[], None]) -> None:
        """Schedule a task to be executed during a ``poll()``. One enqueued task
        will be executed per ``poll()`` invocation. You can use this repeatedly
        to mock out multiple responses to poll invocations."""
        with self._lock, self._poll_tasks_lock:
            self._poll_tasks.append(task)

    def schedule_nop_poll_task(self) -> None:
        with self._lock:
            self.schedule_poll_task(task=lambda: None)

    def last_poll_timeout(self) -> float | None:
        """The timeout (seconds) of the most recent ``poll()``, or ``None``
        before the first."""
        return self._last_poll_timeout

    # ---- Java's private helpers ------------------------------------------------
    def _ensure_not_closed(self) -> None:
        if self._mock_closed:
            raise IllegalStateError(message="This consumer has already been closed.")

    def _update_fetch_position(self, tp: TopicPartition) -> None:
        if self._subscriptions.is_offset_reset_needed(tp):
            self._reset_offset_position(tp)
        elif tp not in self._committed_offsets:
            self._subscriptions.request_offset_reset(tp)
            self._reset_offset_position(tp)
        else:
            self._subscriptions.seek(tp, self._committed_offsets[tp].offset())

    def _reset_offset_position(self, tp: TopicPartition) -> None:
        strategy = self._subscriptions.reset_strategy(tp)
        offset: int | None
        if strategy is AutoOffsetResetStrategy.EARLIEST:
            offset = self._beginning_offsets.get(tp)
            if offset is None:
                raise IllegalStateError(
                    message="MockConsumer didn't have beginning offset specified, but tried to "
                            "seek to beginning")
        elif strategy is AutoOffsetResetStrategy.LATEST:
            offset = self._end_offsets.get(tp)
            if offset is None:
                raise IllegalStateError(
                    message="MockConsumer didn't have end offset specified, but tried to seek "
                            "to end")
        elif (strategy is not None
              and strategy.type() is AutoOffsetResetStrategy.StrategyType.BY_DURATION):
            offset = self._duration_reset_offsets.get(tp)
            if offset is None:
                raise IllegalStateError(
                    message="MockConsumer didn't have duration offset specified, but tried to "
                            "seek to timestamp")
        else:
            raise NoOffsetForPartitionError(partition=tp)
        self._c_seek(tp, offset, None)
