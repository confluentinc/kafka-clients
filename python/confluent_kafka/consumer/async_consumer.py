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

"""``AsyncConsumer``: the asyncio peer of :class:`Consumer` (CLAUDE.md, Python
Binding Conventions, Class family).

The same methods, in the same order. A method is ``async def`` iff Java waits
in it — on the background thread (``addAndGet``), on the network or on a
listener it runs: ``subscribe``, ``assign``, ``unsubscribe``, ``poll``,
``commit``, ``seek``, ``seek_to_beginning`` / ``seek_to_end``, ``position``,
``committed``, ``partitions_for``, ``list_topics``, ``pause`` / ``resume``,
``offsets_for_times``, ``beginning_offsets`` / ``end_offsets`` and ``close``;
every other method is a plain ``def`` (``assignment()``, ``subscription()``,
``commit_nowait()``, ``metrics()``, ``paused()``, ``group_metadata()``,
``wakeup()``). A waiting method awaits the entry point's ``_async`` form; the
listener runs on the event loop and may be ``async def``. A coroutine listener
method that ``commit_nowait()`` delivers is awaited in a task that holds the
consumer until it ends: meanwhile ``metrics()`` / ``group_metadata()`` raise
``ConcurrentModificationError`` (see ``commit_nowait``).

Not generated, besides :class:`Consumer`'s omissions: ``current_lag()``, which
waits in Java (``AsyncKafkaConsumer.currentLag`` uses ``addAndGet``) but whose
``_async`` entry point, ``kafka_consumer_Consumer_current_lag_async``, is
missing (``ffi-overload-gaps.md``).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import UNSET, java_forms
from confluent_kafka._async import await_to_end
from confluent_kafka.concurrent_modification_error import ConcurrentModificationError

from ._base import (
    _ConsumerState, _Forward, _ListenerErrors, blank_null_topics, close_args, poll_timeout_ms,
)
from ._conversions import offsets_to_spec, tp_to_spec
from .close_options import CloseOptions
from .consumer import COMMIT_NOWAIT_FORMS, SEEK_FORMS, SUBSCRIBE_FORMS

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.metric_name import MetricName
    from confluent_kafka.common.partition_info import PartitionInfo
    from confluent_kafka.common.topic_partition import TopicPartition

    from ._poll import Deserialized
    from .consumer_group_metadata import ConsumerGroupMetadata
    from .consumer_rebalance_listener import ConsumerRebalanceListener
    from .consumer_records import ConsumerRecords
    from .offset_and_metadata import OffsetAndMetadata
    from .offset_and_timestamp import OffsetAndTimestamp
    from .offset_commit_callback import OffsetCommitCallback
    from .subscription_pattern import SubscriptionPattern

__all__ = ["AsyncConsumer"]

K = TypeVar("K")
V = TypeVar("V")

_LOG = logging.getLogger("confluent_kafka.consumer")


class AsyncConsumer(Generic[K, V], _ConsumerState):
    """The asyncio peer of the ``Consumer`` interface.

    Construct an ``AsyncKafkaConsumer`` or ``AsyncMockConsumer``; an async
    context manager whose exit closes. The async classes follow the sync
    thread rule per event loop: a call while another task is inside the
    consumer raises ``ConcurrentModificationError``, ``wakeup()`` excepted.
    """

    def __init__(self) -> None:
        if type(self) is AsyncConsumer:
            raise TypeError(
                "AsyncConsumer is a non-instantiable base; use AsyncKafkaConsumer or "
                "AsyncMockConsumer")
        _ConsumerState.__init__(self)

    def assignment(self) -> set[TopicPartition]:
        """See :meth:`Consumer.assignment`."""
        return self._c_assignment()

    def subscription(self) -> set[str]:
        """See :meth:`Consumer.subscription`."""
        return self._c_subscription()

    @overload
    async def subscribe(self, *, topics: Iterable[str],
                        callback: ConsumerRebalanceListener | None = None) -> None: ...
    @overload
    async def subscribe(self, *, pattern: SubscriptionPattern,
                        callback: ConsumerRebalanceListener | None = None) -> None: ...

    @java_forms(*SUBSCRIBE_FORMS)
    async def subscribe(self, *, topics: Iterable[str] | None = None,
                        pattern: SubscriptionPattern | None = None,
                        callback: ConsumerRebalanceListener | None = None) -> None:
        """See :meth:`Consumer.subscribe`. The listener's methods may be
        ``async def``; they are awaited on the event loop. A call a listener
        makes back into the consumer (``await consumer.commit()``, ``seek()``,
        ``position()``, …) runs synchronously through the core's
        ``ConsumerHandle``, which has no ``_async`` forms, so it blocks the
        event loop for its duration."""
        if topics is not None:
            await self._a_subscribe_topics(topics, callback)
        else:
            await self._a_subscribe_pattern(pattern, callback)

    async def assign(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.assign`."""
        await self._a_assign(partitions)

    async def unsubscribe(self) -> None:
        """See :meth:`Consumer.unsubscribe`."""
        await self._a_unsubscribe()

    async def poll(self, *, timeout: Duration) -> ConsumerRecords[K, V]:
        """See :meth:`Consumer.poll`."""
        return await self._a_poll(timeout)

    async def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None
                     ) -> None:
        """See :meth:`Consumer.commit` (Java's ``commitSync``)."""
        await self._a_commit(offsets)

    @overload
    def commit_nowait(self, *, callback: OffsetCommitCallback | None = None) -> None: ...
    @overload
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                      callback: OffsetCommitCallback | None) -> None: ...

    @java_forms(*COMMIT_NOWAIT_FORMS)
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
                      callback: OffsetCommitCallback | None = UNSET) -> None:
        """See :meth:`Consumer.commit_nowait` (Java's ``commitAsync``, which
        does not wait: a plain ``def``). The callback runs on the event loop,
        inside a later call on this consumer.

        While the commit waits for its offsets, the core may deliver a queued
        listener callback (``consumer-threading.md`` §31). A plain method runs
        inside this call; an ``async def`` one cannot be awaited inside a plain
        ``def``, so the rest of the commit then continues in a task on this
        loop, the listener awaited there, and the next awaited call on the
        consumer waits for it (and raises its failure). Until that task ends,
        ``assignment()`` / ``subscription()`` / ``paused()`` read through the
        core's ``ConsumerHandle``, a further ``commit_nowait()`` follows it, and
        ``metrics()`` / ``group_metadata()``, which the handle lacks, raise
        ``ConcurrentModificationError``."""
        self._c_commit_nowait(offsets, callback)

    @overload
    async def seek(self, *, partition: TopicPartition, offset: int) -> None: ...
    @overload
    async def seek(self, *, partition: TopicPartition,
                   offset_and_metadata: OffsetAndMetadata) -> None: ...

    @java_forms(*SEEK_FORMS)
    async def seek(self, *, partition: TopicPartition, offset: int | None = None,
                   offset_and_metadata: OffsetAndMetadata | None = None) -> None:
        """See :meth:`Consumer.seek` (Java waits for the background thread to
        apply it, ``addAndGet``)."""
        await self._a_seek(partition, offset, offset_and_metadata)

    async def seek_to_beginning(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.seek_to_beginning`."""
        await self._a_seek_to_beginning(partitions)

    async def seek_to_end(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.seek_to_end`."""
        await self._a_seek_to_end(partitions)

    async def position(self, *, partition: TopicPartition) -> int:
        """See :meth:`Consumer.position`."""
        return await self._a_position(partition)

    async def committed(self, *, partitions: Iterable[TopicPartition]
                        ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        """See :meth:`Consumer.committed`."""
        return await self._a_committed(partitions)

    def metrics(self) -> dict[MetricName, Metric]:
        """See :meth:`Consumer.metrics`.

        While a ``commit_nowait()`` still finishes in a task (a coroutine listener
        it delivered is being awaited), this raises ``ConcurrentModificationError``:
        that commit holds the consumer, and the core's ``ConsumerHandle`` has no
        ``metrics`` (``ffi-overload-gaps.md``)."""
        return self._c_metrics()

    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """See :meth:`Consumer.partitions_for`."""
        return await self._a_partitions_for(topic)

    async def list_topics(self) -> dict[str, list[PartitionInfo]]:
        """See :meth:`Consumer.list_topics`."""
        return await self._a_list_topics()

    def paused(self) -> set[TopicPartition]:
        """See :meth:`Consumer.paused`."""
        return self._c_paused()

    async def pause(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.pause`."""
        await self._a_pause(partitions)

    async def resume(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.resume`."""
        await self._a_resume(partitions)

    async def offsets_for_times(self, *, timestamps_to_search: Mapping[TopicPartition, int]
                                ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        """See :meth:`Consumer.offsets_for_times`."""
        return await self._a_offsets_for_times(timestamps_to_search)

    async def beginning_offsets(self, *, partitions: Iterable[TopicPartition]
                                ) -> dict[TopicPartition, int]:
        """See :meth:`Consumer.beginning_offsets`."""
        return await self._a_beginning_offsets(partitions)

    async def end_offsets(self, *, partitions: Iterable[TopicPartition]
                          ) -> dict[TopicPartition, int]:
        """See :meth:`Consumer.end_offsets`."""
        return await self._a_end_offsets(partitions)

    def group_metadata(self) -> ConsumerGroupMetadata:
        """See :meth:`Consumer.group_metadata`.

        While a ``commit_nowait()`` still finishes in a task (a coroutine listener
        it delivered is being awaited), this raises ``ConcurrentModificationError``:
        that commit holds the consumer, and the core's ``ConsumerHandle`` has no
        ``groupMetadata`` (``ffi-overload-gaps.md``)."""
        return self._c_group_metadata()

    async def close(self, *, option: CloseOptions | None = None) -> None:
        """See :meth:`Consumer.close`."""
        await self._a_close(option)

    async def __aenter__(self) -> AsyncConsumer[K, V]:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def wakeup(self) -> None:
        """See :meth:`Consumer.wakeup`."""
        self._c_wakeup()

    # ---- the FFI implementations of the waiting calls ------------------------
    async def _a_subscribe_topics(self, topics: Iterable[str],
                                  callback: ConsumerRebalanceListener | None) -> None:
        topic_list = blank_null_topics(topics)
        previous, self._listener = self._listener, callback
        try:
            await self._run_async(*self._subscribe_topics_spec(topic_list, callback is not None))
        except BaseException:
            self._listener = previous
            raise

    async def _a_subscribe_pattern(self, pattern: SubscriptionPattern | None,
                                   callback: ConsumerRebalanceListener | None) -> None:
        assert pattern is not None
        previous, self._listener = self._listener, callback
        try:
            await self._run_async(*self._subscribe_pattern_spec(pattern.pattern(),
                                                                callback is not None))
        except BaseException:
            self._listener = previous
            raise

    async def _tp_op(self, op: str, fn: Any, partitions: Iterable[TopicPartition]) -> None:
        self._check_not_null(op, partitions)
        partition_list = list(partitions)
        if self._in_callback():
            self._reentrant_use(op, partition_list)
            return
        await self._run_async(*self._void_spec(fn, tp_to_spec(partition_list)))

    async def _a_assign(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("assign", _lib.Consumer_assign_async,
                          self._assign_partitions(partitions))

    async def _a_unsubscribe(self) -> None:
        await self._run_async(*self._void_spec(_lib.Consumer_unsubscribe_async))

    async def _a_poll(self, timeout: Duration) -> ConsumerRecords[K, V]:
        # The closed check first, then Java's Timer check (Order).
        self._check_open()
        timeout_ms = poll_timeout_ms(timeout)
        result: Deserialized = await self._run_async(*self._poll_spec(timeout_ms))
        for partition, offset in result.rewind:
            try:
                await self._run_async(*self._seek_spec(partition, offset, None))
            except Exception:  # noqa: BLE001 - the deserialization error matters
                _LOG.exception("Could not seek %s back to offset %d", partition, offset)
        if result.error is not None and result.records.is_empty():
            raise result.error
        return result.records

    async def _a_commit(self, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None) -> None:
        if self._in_callback():
            self._reentrant_use("commit", offsets)
            return
        await self._run_async(*self._commit_spec(offsets))

    async def _a_seek(self, partition: TopicPartition, offset: int | None,
                      offset_and_metadata: OffsetAndMetadata | None) -> None:
        if self._in_callback():
            self._reentrant_use("seek", partition, offset, offset_and_metadata)
            return
        await self._run_async(*self._seek_spec(partition, offset, offset_and_metadata))

    async def _a_seek_to_beginning(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("seek_to_beginning", _lib.Consumer_seek_to_beginning_async, partitions)

    async def _a_seek_to_end(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("seek_to_end", _lib.Consumer_seek_to_end_async, partitions)

    async def _a_position(self, partition: TopicPartition) -> int:
        if self._in_callback():
            position: int = self._reentrant_use("position", partition)
            return position
        return await self._run_async(*self._position_spec(partition))

    async def _a_committed(self, partitions: Iterable[TopicPartition]
                           ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        partition_list = list(partitions)
        if self._in_callback():
            committed: dict[TopicPartition, OffsetAndMetadata | None] = self._reentrant_use(
                "committed", partition_list)
            return committed
        return await self._run_async(*self._committed_spec(partition_list))

    async def _a_partitions_for(self, topic: str) -> list[PartitionInfo]:
        return await self._run_async(*self._partitions_for_spec(topic))

    async def _a_list_topics(self) -> dict[str, list[PartitionInfo]]:
        return await self._run_async(*self._list_topics_spec())

    async def _a_pause(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("pause", _lib.Consumer_pause_async, partitions)

    async def _a_resume(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("resume", _lib.Consumer_resume_async, partitions)

    async def _a_offsets_for_times(self, timestamps_to_search: Mapping[TopicPartition, int]
                                   ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        self._check_not_null("offsets_for_times", timestamps_to_search)
        timestamps = dict(timestamps_to_search)
        if self._in_callback():
            found: dict[TopicPartition, OffsetAndTimestamp | None] = self._reentrant_use(
                "offsets_for_times", timestamps)
            return found
        return await self._run_async(*self._offsets_for_times_spec(timestamps))

    async def _long_offsets(self, op: str, fn: Any, partitions: Iterable[TopicPartition]
                            ) -> dict[TopicPartition, int]:
        self._check_not_null(op, partitions)
        partition_list = list(partitions)
        if self._in_callback():
            offsets: dict[TopicPartition, int] = self._reentrant_use(op, partition_list)
            return offsets
        return await self._run_async(*self._long_offsets_spec(fn, partition_list))

    async def _a_beginning_offsets(self, partitions: Iterable[TopicPartition]
                                   ) -> dict[TopicPartition, int]:
        return await self._long_offsets("beginning_offsets",
                                        _lib.Consumer_beginning_offsets_async, partitions)

    async def _a_end_offsets(self, partitions: Iterable[TopicPartition]
                             ) -> dict[TopicPartition, int]:
        return await self._long_offsets("end_offsets", _lib.Consumer_end_offsets_async,
                                        partitions)

    async def _a_close(self, option: CloseOptions | None) -> None:
        if self._is_closed():
            return
        timeout_ms, operation = close_args(option)
        # A commit_nowait() still awaiting its listener finishes first; its
        # failure is raised once the consumer is closed.
        deferred: Exception | None = None
        if not self._in_callback():
            try:
                await self._await_commit_continuation()
            except Exception as exc:  # noqa: BLE001 - raised after the close
                deferred = exc
        if not self._begin_close():
            return
        try:
            await self._run_async(*self._close_spec(timeout_ms, operation))
        except ConcurrentModificationError:
            self._abort_close()
            raise
        except BaseException:
            await self._finish_close_off_loop()
            raise
        await self._finish_close_off_loop()
        if deferred is not None:
            raise deferred

    # ---- commit_nowait (Java's commitAsync) on the event loop ----------------
    def _c_commit_nowait(self, offsets: Any, callback: OffsetCommitCallback | None) -> None:
        """``commit_nowait()`` on the loop's thread (see ``commit_nowait``).

        Without a listener, or inside a callback, it is ``Consumer``'s. With a
        listener, the synchronous FFI call runs on the helper thread while this
        call waits and runs the queued callbacks (``_commit_nowait_on_loop``).
        While a previous ``commit_nowait()`` still finishes in a task, this one
        follows it in another (``_follow_commit_nowait``)."""
        if self._in_callback() or (self._listener is None and not self._commit_in_flight()):
            _ConsumerState._c_commit_nowait(self, offsets, callback)
            return
        with self._use():
            spec = None if offsets is None else offsets_to_spec(offsets)
        adapter = self._wrap_commit_callback(
            callback, empty_offsets=offsets is not None and not offsets)
        if self._commit_in_flight():
            self._follow_commit_nowait(spec, adapter)
            return
        self._commit_nowait_on_loop(spec, adapter)

    def _commit_nowait_on_loop(self, spec: Any, adapter: Any) -> None:
        """Wait on this thread for the helper's FFI call, running the queued
        callbacks; a coroutine listener hands the rest to a task on the loop
        (``_continue_commit_nowait``), which then holds the use."""
        try:
            loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        forward = _Forward(self._on_pending_notify)
        submit, resolve, free = self._commit_on_helper_spec(spec, adapter, forward)
        box: dict[str, tuple[Any, ...]] = {}
        done = threading.Event()
        errors: _ListenerErrors = []

        def cb(*payload: Any) -> None:
            box["payload"] = payload
            done.set()
            self._on_pending_notify()

        use = self._use()
        h = use.__enter__()
        handed_off = False
        interrupted: KeyboardInterrupt | None = None
        try:
            self._pending_event.clear()
            submit(h, cb)
            while True:
                finished = done.is_set()
                try:
                    handoff = self._drain_pending(h, errors, handoff=loop is not None,
                                                  forward=forward)
                    if handoff is not None and loop is not None:
                        self._commit_continuation = loop.create_task(
                            self._continue_commit_nowait(use, h, handoff, done, box, resolve,
                                                         errors, forward))
                        handed_off = True
                        return
                    if finished:
                        break
                    # Short slices keep KeyboardInterrupt deliverable.
                    self._pending_event.wait(0.1)
                    self._pending_event.clear()
                except KeyboardInterrupt as exc:
                    if interrupted is None:
                        interrupted = exc
                        _lib.Consumer_wakeup(h)
        finally:
            if not handed_off:
                use.__exit__(None, None, None)
        payload = box["payload"]
        if interrupted is not None:
            free(payload)
            raise interrupted
        self._resolve_reporting(resolve, payload, errors)

    async def _continue_commit_nowait(self, use: Any, h: int, handoff: tuple[int, Any],
                                      done: threading.Event, box: dict[str, tuple[Any, ...]],
                                      resolve: Any, errors: _ListenerErrors,
                                      forward: _Forward) -> None:
        """The rest of a ``commit_nowait()`` whose coroutine listener is awaited
        on this loop: await it, ack it, wait for the helper's FFI call (draining
        on the loop), and keep the commit's failure for the next awaited call.
        Cancelled, it finishes synchronously (a coroutine listener then fails
        with ``TypeError``), so the use is not released while the FFI call runs."""
        loop = asyncio.get_running_loop()
        pending = asyncio.Event()
        waiter = (loop, pending)
        self._async_waiters.add(waiter)
        try:
            try:
                await self._await_listener(*handoff, errors)
                while not done.is_set():
                    await pending.wait()
                    pending.clear()
                    await self._drain_pending_async(h, errors, forward)
                await self._drain_pending_async(h, errors, forward)
            except asyncio.CancelledError:
                while True:
                    finished = done.is_set()
                    self._drain_pending(h, errors, forward=forward)
                    if finished:
                        break
                    done.wait(0.1)
                raise
            try:
                self._resolve_reporting(resolve, box["payload"], errors)
            except Exception as exc:  # noqa: BLE001 - raised by the next awaited call
                self._deferred_error = exc
        finally:
            self._async_waiters.discard(waiter)
            use.__exit__(None, None, None)

    def _follow_commit_nowait(self, spec: Any, adapter: Any) -> None:
        """A ``commit_nowait()`` while the previous one still finishes in a task:
        a task running this one after it, its failure kept for the next awaited
        call."""
        previous = self._commit_continuation
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Not on the loop that holds the consumer: another thread.
            raise ConcurrentModificationError(
                message="KafkaConsumer is not safe for multi-threaded access.") from None

        async def follow() -> None:
            if previous is not None:
                await asyncio.wait({previous})
            forward = _Forward(self._on_pending_notify)
            try:
                await self._run_async(*self._commit_on_helper_spec(spec, adapter, forward),
                                      after_commit_nowait=False, forward=forward)
            except Exception as exc:  # noqa: BLE001 - raised by the next awaited call
                if self._deferred_error is None:
                    self._deferred_error = exc

        self._commit_continuation = loop.create_task(follow())

    async def _finish_close_off_loop(self) -> None:
        """``_finish_close`` waits for the uses in flight and joins the core's
        tasks: off the event loop, and to its end even if the task is cancelled
        meanwhile, so ``close()`` does not return before it."""
        await await_to_end(asyncio.get_running_loop().run_in_executor(None, self._finish_close))
