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

The same methods, in the same order. A method is ``async def`` iff the entry
point it calls has a ``_cb`` completion form in the FFI header:
``subscribe``, ``assign``, ``unsubscribe``, ``poll``, ``commit``, ``seek``,
``seek_to_beginning`` / ``seek_to_end``, ``position``, ``committed``,
``partitions_for``, ``list_topics``, ``pause`` / ``resume``,
``offsets_for_times``, ``beginning_offsets`` / ``end_offsets`` and ``close``;
every other method is a plain ``def`` (``assignment()``, ``subscription()``,
``commit_nowait()``, ``metrics()``, ``paused()``, ``current_lag()``,
``group_metadata()``, ``wakeup()``). A waiting method awaits the entry point's
``_cb`` form, running the queued callbacks on the event loop; the listener runs
there and may be ``async def``.
A coroutine listener method that ``commit_nowait()`` delivers is awaited in a
task that holds the consumer until it ends: meanwhile ``group_metadata()``
and ``current_lag()`` raise ``ConcurrentModificationError`` (see
``commit_nowait``).

Not generated: :class:`Consumer`'s omissions.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import UNSET, java_forms
from confluent_kafka._async import await_to_end
from confluent_kafka.concurrent_modification_error import ConcurrentModificationError

from ._base import (
    _AT_CLOSE, _AT_LATER, _AT_POLL, _ConsumerState, blank_null_topics, close_args,
    poll_timeout_ms,
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
        ``ConsumerHandle``'s plain entry points, inside the call delivering the
        listener, so it blocks the event loop for its duration."""
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
    def commit_nowait(self, *, callback: OffsetCommitCallback
                      | Callable[[dict[TopicPartition, OffsetAndMetadata] | None,
                                  Exception | None], Awaitable[None]]
                      | None = None) -> None: ...
    @overload
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                      callback: OffsetCommitCallback
                      | Callable[[dict[TopicPartition, OffsetAndMetadata] | None,
                                  Exception | None], Awaitable[None]]
                      | None) -> None: ...

    @java_forms(*COMMIT_NOWAIT_FORMS)
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
                      callback: OffsetCommitCallback
                      | Callable[[dict[TopicPartition, OffsetAndMetadata] | None,
                                  Exception | None], Awaitable[None]]
                      | None = UNSET) -> None:
        """See :meth:`Consumer.commit_nowait` (Java's ``commitAsync``, which
        does not wait: a plain ``def``). The callback runs on the event loop,
        inside a later call on this consumer.

        The callback may be ``async def``: it is awaited on the event loop,
        inside the call that delivers it (``poll()``, ``commit()``,
        ``close()``, …), which returns only after it ended. Callbacks still
        start one at a time in the order the commits complete, each after the
        previous one ended. An exception it raises, ``CancelledError``
        included, propagates out of the call that delivers it, as for a plain
        callback (see :meth:`Consumer.commit_nowait`). On the synchronous
        ``Consumer`` an ``async def`` callback is rejected: its coroutine is
        closed, not awaited, and the delivering call raises ``TypeError``.
        ``AsyncMockConsumer`` awaits it after this call returns (Java's mock
        runs the callback inside ``commitAsync``), before its next awaited call
        goes on, which raises the exception it raised (a deferred error).
        On ``AsyncKafkaConsumer`` one delivered inside a ``commit()`` that
        another callback issued, which blocks the loop, is awaited once that
        commit returned (*deviation*: Java runs it inside that commit): the
        delivering call raises its exception once it ended, wrapped as that
        call wraps a callback's; a ``poll()`` returns its records first, and
        its next awaited call raises it.

        While the commit waits for its offsets, the core may deliver a queued
        listener callback (``consumer-threading.md`` §31). A plain method runs
        inside this call; an ``async def`` one cannot be awaited inside a plain
        ``def``, so the rest of the commit then continues in a task on this
        loop, the listener awaited there, and the next awaited call on the
        consumer waits for it (and raises its failure). Until that task ends,
        ``assignment()`` / ``subscription()`` / ``paused()`` read through the
        core's ``ConsumerHandle``, a further ``commit_nowait()`` follows it,
        ``metrics()`` answers (Java's takes no ``acquire()``), and
        ``group_metadata()`` / ``current_lag()``, which the handle lacks, raise
        ``ConcurrentModificationError``.

        Called from inside a callback this consumer delivers, it commits through
        the core's ``ConsumerHandle`` and first runs the commit callbacks queued
        behind the running one, inside this call, as Java's ``commitAsync``
        does; it blocks the event loop meanwhile, so an ``async def`` callback
        among them is scheduled there and awaited after this call returns, by
        the call delivering the outer callback, and the callbacks after it may
        start before it ends (*deviation*). ``await commit()`` from inside a
        callback does the same."""
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
        """See :meth:`Consumer.metrics`. Like Java's ``metrics()`` it takes no
        ``acquire()``, so it answers while a call is in flight."""
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

    def current_lag(self, *, topic_partition: TopicPartition) -> int | None:
        """See :meth:`Consumer.current_lag`. Its entry point,
        ``kafka_consumer_Consumer_current_lag``, has no ``_cb`` form, so it
        is a plain ``def``."""
        return self._c_current_lag(topic_partition)

    def group_metadata(self) -> ConsumerGroupMetadata:
        """See :meth:`Consumer.group_metadata`.

        While a ``commit_nowait()`` still finishes in a task (a coroutine listener
        it delivered is being awaited), this raises ``ConcurrentModificationError``:
        that commit holds the consumer, and the core's ``ConsumerHandle`` has no
        ``groupMetadata``."""
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
            await self._run_async(*self._subscribe_topics_spec(topic_list, callback))
        except BaseException:
            self._listener = previous
            raise

    async def _a_subscribe_pattern(self, pattern: SubscriptionPattern | None,
                                   callback: ConsumerRebalanceListener | None) -> None:
        assert pattern is not None
        previous, self._listener = self._listener, callback
        try:
            await self._run_async(*self._subscribe_pattern_spec(pattern.pattern(), callback))
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
        await self._tp_op("assign", _lib.Consumer_assign_cb,
                          self._assign_partitions(partitions))

    async def _a_unsubscribe(self) -> None:
        await self._run_async(*self._void_spec(_lib.Consumer_unsubscribe_cb))

    async def _a_poll(self, timeout: Duration) -> ConsumerRecords[K, V]:
        # The closed check first, then Java's Timer check (Order).
        self._check_open()
        timeout_ms = poll_timeout_ms(timeout)
        # A deferred error the poll keeps (see _run_async) is not raised by the
        # seeks back: the records go to the user first.
        result: Deserialized = await self._run_async(*self._poll_spec(timeout_ms), site=_AT_POLL)
        for partition, offset in result.rewind:
            try:
                await self._run_async(*self._seek_spec(partition, offset, None),
                                      after_commit_nowait=False)
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
        await self._tp_op("seek_to_beginning", _lib.Consumer_seek_to_beginning_cb, partitions)

    async def _a_seek_to_end(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("seek_to_end", _lib.Consumer_seek_to_end_cb, partitions)

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
        await self._tp_op("pause", _lib.Consumer_pause_cb, partitions)

    async def _a_resume(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("resume", _lib.Consumer_resume_cb, partitions)

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
                                        _lib.Consumer_beginning_offsets_cb, partitions)

    async def _a_end_offsets(self, partitions: Iterable[TopicPartition]
                             ) -> dict[TopicPartition, int]:
        return await self._long_offsets("end_offsets", _lib.Consumer_end_offsets_cb,
                                        partitions)

    async def _a_close(self, option: CloseOptions | None) -> None:
        if self._is_closed():
            return
        timeout_ms, operation = close_args(option)
        # A commit_nowait() still awaiting its listener finishes first; its
        # failure, or a deferred error, is raised once the consumer is closed
        # (a callback's exception in Java's "Failed to close kafka consumer").
        deferred: Exception | None = None
        if not self._in_callback():
            try:
                await self._await_commit_continuation(_AT_CLOSE)
            except Exception as exc:  # noqa: BLE001 - raised after the close
                deferred = exc
        # A call left on a closed event loop does not hold the consumer.
        self._release_abandoned()
        if not self._begin_close():
            return
        try:
            await self._run_async(*self._close_spec(timeout_ms, operation), site=_AT_CLOSE)
        except ConcurrentModificationError:
            self._abort_close()
            raise
        except BaseException:
            await self._finish_close_off_loop()
            raise
        await self._finish_close_off_loop()
        if deferred is not None:
            raise deferred

    # ---- coroutine callbacks (listener methods, commit callbacks) -----------
    def _defer_callback(self, awaitable: Awaitable[object], callback_id: int) -> bool:
        """Await a coroutine callback -- a listener method, or an
        ``OffsetCommitCallback`` -- in a task on the running loop, then report
        its result to the core (``kafka_consumer_Consumer__set_callback_result``):
        the exception it raised, ``CancelledError`` included, as a foreign
        callback error owning it, which the delivering call raises
        (``_raise_error``). The call delivering it waits for the task before going on
        (``_wait_on_loop``, or ``_commit_nowait_on_loop``'s hand-off), and the
        core waits for the report before it delivers the next callback, so the
        callback still runs inside that call, callbacks start in the order the
        core delivers them, and the rebalance does not advance before a
        listener returns (``consumer-threading.md`` §31). The task is created
        inside the callback window, so a consumer operation the callback issues
        goes through the ConsumerHandle.

        A commit callback a blocking ConsumerHandle commit runs inside it
        (``_handle_commit``: a callback committing again, as Java's ``commit()``
        runs ``executeCallbacks()`` first) is delivered while that commit blocks
        the loop, so its task cannot run before the commit returns: it is
        reported to the core at once, and the task only awaits it; the callbacks
        after it may start before it ends, and the delivering call still waits
        for it, then raises the exception it raised, instead of its own result
        (``_take_callback_error``), wrapped as that call wraps a callback's
        exception -- a deferred error (*deviation*: Java runs it to its end
        inside that commit, which raises the exception). A ``poll`` returns its
        records instead, as the core has advanced the positions past them, and
        the next awaited call raises the exception (``_run_async``)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        report_now = self._handle_commit_running()

        async def run() -> None:
            error: BaseException | None = None
            try:
                await awaitable
            except BaseException as exc:  # noqa: BLE001 - reported to the core
                error = exc
            if report_now:
                if error is not None and self._callback_error is None:
                    self._callback_error = error
                return
            # Under the lifecycle lock: close() clears the handle under it
            # before it frees the handle, so a live handle is not freed meanwhile.
            with self._lifecycle:
                if self._h:
                    _lib.Consumer__set_callback_result(self._h, callback_id, error)

        task = loop.create_task(run())
        self._listener_tasks.add(task)
        task.add_done_callback(self._listener_tasks.discard)
        if report_now:
            with self._lifecycle:
                if self._h:
                    _lib.Consumer__set_callback_result(self._h, callback_id, None)
        return True

    # ---- commit_nowait (Java's commitAsync) on the event loop ----------------
    def _c_commit_nowait(self, offsets: Any, callback: Callable[..., object] | None) -> None:
        """``commit_nowait()`` on the loop's thread (see ``commit_nowait``).

        Inside a callback, it is ``Consumer``'s. Otherwise it submits the
        ``_cb`` form and waits on this thread, running the queued callbacks
        (``_commit_nowait_on_loop``); a coroutine listener method they deliver
        hands the rest of the commit to a task on the loop. While a previous
        ``commit_nowait()`` still finishes in a task, this one follows it in
        another (``_follow_commit_nowait``)."""
        if self._in_callback():
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
        """Wait on this thread for the commit's completion, running the queued
        callbacks, in 100 ms slices so a ``KeyboardInterrupt`` is delivered; a
        coroutine listener method or commit callback they deliver (a task
        ``_defer_callback`` created) hands the rest to a task on the loop
        (``_continue_commit_nowait``), which then holds the use."""
        submit, resolve, free = self._commit_nowait_spec(spec, adapter)
        box: list[tuple[Any, ...]] = []
        event = self._pending_event

        def cb(*payload: Any) -> None:
            box.append(payload)
            event.set()

        use = self._use()
        h = use.__enter__()
        handed_off = False
        interrupted: KeyboardInterrupt | None = None
        try:
            event.clear()
            self._drain(h)
            submit(h, cb)
            while True:
                try:
                    if self._listener_tasks and interrupted is None:
                        loop = asyncio.get_running_loop()  # it created the tasks
                        self._commit_continuation = loop.create_task(
                            self._continue_commit_nowait(use, h, box, resolve, free))
                        handed_off = True
                        return
                    if box:
                        break
                    event.wait(0.1)
                    event.clear()
                    self._drain(h)
                except KeyboardInterrupt as exc:
                    if interrupted is None:
                        interrupted = exc
                        _lib.Consumer_wakeup(h)
        finally:
            if not handed_off:
                use.release()
        payload = box[0]
        if interrupted is not None:
            free(payload)
            raise interrupted
        resolve(payload)

    async def _continue_commit_nowait(self, use: Any, h: int, box: list[tuple[Any, ...]],
                                      resolve: Any, free: Any) -> None:
        """The rest of a ``commit_nowait()`` whose coroutine listener is awaited
        on this loop: wait for the commit's completion on the loop
        (``_wait_on_loop``, which awaits the listener first), and keep the
        commit's failure for the next awaited call. Cancelled, it wakes the
        consumer and still waits for the completion, so the use is not released
        while the FFI call runs."""
        loop = asyncio.get_running_loop()
        pending = asyncio.Event()
        waiter = (loop, pending)
        self._async_waiters[waiter] = use
        try:
            cancelled = await self._wait_on_loop(h, lambda: bool(box), pending)
            payload = box[0]
            if cancelled is not None:
                free(payload)
                raise cancelled
            self._finish_async(payload, resolve, free, _AT_LATER)
        finally:
            self._async_waiters.pop(waiter, None)
            use.release()

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
            try:
                await self._run_async(*self._commit_nowait_spec(spec, adapter),
                                      after_commit_nowait=False, site=_AT_LATER)
            except Exception as exc:  # noqa: BLE001 - raised by the next awaited call
                self._keep_error(exc, from_callback=False)

        self._commit_continuation = loop.create_task(follow())

    async def _finish_close_off_loop(self) -> None:
        """``_finish_close`` waits for the uses in flight and joins the core's
        tasks: off the event loop, and to its end even if the task is cancelled
        meanwhile, so ``close()`` does not return before it."""
        await await_to_end(asyncio.get_running_loop().run_in_executor(None, self._finish_close))
