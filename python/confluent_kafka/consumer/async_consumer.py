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
``subscribe``, ``assign``, ``unsubscribe``, ``poll``, ``commit``,
``commit_nowait``, ``seek``, ``seek_to_beginning`` / ``seek_to_end``,
``position``, ``committed``, ``partitions_for``, ``list_topics``, ``pause`` /
``resume``, ``offsets_for_times``, ``beginning_offsets`` / ``end_offsets``,
``current_lag`` and ``close``; every other method is a plain ``def``
(``assignment()``, ``subscription()``, ``metrics()``, ``paused()``,
``group_metadata()``, ``wakeup()``). A waiting method awaits the entry point's
``_cb`` form, running the queued callbacks on the event loop; the listener runs
there and may be ``async def``.

Not generated: :class:`Consumer`'s omissions.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import MISSING, java_forms, reject_str
from confluent_kafka._async import await_to_end
from confluent_kafka.concurrent_modification_error import ConcurrentModificationError

from ._base import (
    _AT_CLOSE, _AT_POLL, _ConsumerState, abandoned_call_wait_s, blank_null_topics, close_args,
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
    async def subscribe(self, *, topics: Iterable[str]) -> None: ...
    @overload
    async def subscribe(self, *, topics: Iterable[str],
                        callback: ConsumerRebalanceListener) -> None: ...
    @overload
    async def subscribe(self, *, pattern: SubscriptionPattern,
                        callback: ConsumerRebalanceListener) -> None: ...
    @overload
    async def subscribe(self, *, pattern: SubscriptionPattern) -> None: ...

    @java_forms(*SUBSCRIBE_FORMS)
    async def subscribe(self, *, topics: Iterable[str] = MISSING,
                        pattern: SubscriptionPattern = MISSING,
                        callback: ConsumerRebalanceListener = MISSING) -> None:
        """See :meth:`Consumer.subscribe`. The listener's methods may be
        ``async def``; they are awaited on the event loop. A call a listener
        makes back into the consumer (``await consumer.commit()``, ``seek()``,
        ``position()``, …) runs synchronously through the core's
        ``ConsumerHandle``'s plain entry points, inside the call delivering the
        listener, so it blocks the event loop for its duration."""
        if topics is not MISSING:
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

    async def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] = MISSING,
                     timeout: Duration = MISSING) -> None:
        """See :meth:`Consumer.commit` (Java's ``commitSync``)."""
        await self._a_commit(offsets, timeout)

    @overload
    async def commit_nowait(self, *, callback: OffsetCommitCallback
                            | Callable[[dict[TopicPartition, OffsetAndMetadata] | None,
                                        Exception | None], Awaitable[None]]
                            | None = None) -> None: ...
    @overload
    async def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                            callback: OffsetCommitCallback
                            | Callable[[dict[TopicPartition, OffsetAndMetadata] | None,
                                        Exception | None], Awaitable[None]]
                            | None) -> None: ...

    @java_forms(*COMMIT_NOWAIT_FORMS)
    async def commit_nowait(self, *,
                            offsets: Mapping[TopicPartition, OffsetAndMetadata] = MISSING,
                            callback: OffsetCommitCallback
                            | Callable[[dict[TopicPartition, OffsetAndMetadata] | None,
                                        Exception | None], Awaitable[None]]
                            | None = MISSING) -> None:
        """See :meth:`Consumer.commit_nowait` (Java's ``commitAsync``).

        It is ``async def`` because its entry point,
        ``kafka_consumer_Consumer_commit_async``, completes through its ``_cb``
        form: like Java's ``commitAsync``, which first runs the queued commit
        callbacks and then waits for the background thread to take the offsets
        to commit (``offsetsReady``), the call waits until the commit is sent,
        not for its result, which goes to the callback inside a later call on
        this consumer. A failure of the call itself (``InvalidGroupIdError``, a
        timeout waiting for the offsets, the exception of a queued commit
        callback it runs) is raised from it, as Java's ``commitAsync`` throws
        it. While it waits, the core may deliver a queued listener callback
        (``consumer-threading.md`` §31), which runs on the event loop inside
        this call; an ``async def`` method is awaited there before it returns.

        The callback may be ``async def``: it is awaited on the event loop,
        inside the call that delivers it (``poll()``, ``commit()``,
        ``commit_nowait()``, ``close()``, …), which returns only after it ended.
        Callbacks still start one at a time in the order the commits complete,
        each after the previous one ended. An exception it raises,
        ``CancelledError`` included, propagates out of the call that delivers
        it, as for a plain callback (see :meth:`Consumer.commit_nowait`). On the
        synchronous ``Consumer`` an ``async def`` callback is rejected: its
        coroutine is closed, not awaited, and the delivering call raises
        ``TypeError``. ``AsyncMockConsumer`` awaits it inside this call, as
        Java's mock runs the callback inside ``commitAsync``.

        Called from inside a callback this consumer delivers, it commits through
        the core's ``ConsumerHandle`` and first runs the commit callbacks queued
        behind the running one, inside this call, as Java's ``commitAsync``
        does; it blocks the event loop meanwhile, so an ``async def`` callback
        among them is scheduled there and awaited after this call returns, by
        the call delivering the outer callback, and the callbacks after it may
        start before it ends (*deviation*: Java runs it to its end inside this
        call): the delivering call raises its exception once it ended, wrapped
        as that call wraps a callback's; a ``poll()`` returns its records first,
        and its next awaited call raises it. ``await commit()`` from inside a
        callback does the same."""
        await self._a_commit_nowait(offsets, callback)

    @overload
    async def seek(self, *, partition: TopicPartition, offset: int) -> None: ...
    @overload
    async def seek(self, *, partition: TopicPartition,
                   offset_and_metadata: OffsetAndMetadata) -> None: ...

    @java_forms(*SEEK_FORMS)
    async def seek(self, *, partition: TopicPartition, offset: int = MISSING,
                   offset_and_metadata: OffsetAndMetadata = MISSING) -> None:
        """See :meth:`Consumer.seek` (Java waits for the background thread to
        apply it, ``addAndGet``)."""
        await self._a_seek(partition, offset, offset_and_metadata)

    async def seek_to_beginning(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.seek_to_beginning`."""
        await self._a_seek_to_beginning(partitions)

    async def seek_to_end(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.seek_to_end`."""
        await self._a_seek_to_end(partitions)

    async def position(self, *, partition: TopicPartition,
                       timeout: Duration = MISSING) -> int:
        """See :meth:`Consumer.position`."""
        return await self._a_position(partition, timeout)

    async def committed(self, *, partitions: Iterable[TopicPartition],
                        timeout: Duration = MISSING
                        ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        """See :meth:`Consumer.committed`."""
        return await self._a_committed(partitions, timeout)

    def metrics(self) -> dict[MetricName, Metric]:
        """See :meth:`Consumer.metrics`. Like Java's ``metrics()`` it takes no
        ``acquire()``, so it answers while a call is in flight."""
        return self._c_metrics()

    async def partitions_for(self, *, topic: str, timeout: Duration = MISSING
                             ) -> list[PartitionInfo]:
        """See :meth:`Consumer.partitions_for`."""
        return await self._a_partitions_for(topic, timeout)

    async def list_topics(self, *, timeout: Duration = MISSING
                          ) -> dict[str, list[PartitionInfo]]:
        """See :meth:`Consumer.list_topics`."""
        return await self._a_list_topics(timeout)

    def paused(self) -> set[TopicPartition]:
        """See :meth:`Consumer.paused`."""
        return self._c_paused()

    async def pause(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.pause`."""
        await self._a_pause(partitions)

    async def resume(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.resume`."""
        await self._a_resume(partitions)

    async def offsets_for_times(self, *, timestamps_to_search: Mapping[TopicPartition, int],
                                timeout: Duration = MISSING
                                ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        """See :meth:`Consumer.offsets_for_times`."""
        return await self._a_offsets_for_times(timestamps_to_search, timeout)

    async def beginning_offsets(self, *, partitions: Iterable[TopicPartition],
                                timeout: Duration = MISSING) -> dict[TopicPartition, int]:
        """See :meth:`Consumer.beginning_offsets`."""
        return await self._a_beginning_offsets(partitions, timeout)

    async def end_offsets(self, *, partitions: Iterable[TopicPartition],
                          timeout: Duration = MISSING) -> dict[TopicPartition, int]:
        """See :meth:`Consumer.end_offsets`."""
        return await self._a_end_offsets(partitions, timeout)

    async def current_lag(self, *, topic_partition: TopicPartition) -> int | None:
        """See :meth:`Consumer.current_lag`. Its entry point has a ``_cb``
        form, ``kafka_consumer_Consumer_current_lag_cb``, so it is awaited."""
        return await self._a_current_lag(topic_partition)

    def group_metadata(self) -> ConsumerGroupMetadata:
        """See :meth:`Consumer.group_metadata`."""
        return self._c_group_metadata()

    async def close(self, *, option: CloseOptions = MISSING) -> None:
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
                                  callback: ConsumerRebalanceListener) -> None:
        reject_str("topics", topics)  # the shape check precedes the others
        listener = self._subscribe_listener(callback)
        self._check_not_null("subscribe_topics", topics)
        topic_list = blank_null_topics(topics)
        previous, self._listener = self._listener, listener
        try:
            await self._run_async(*self._subscribe_topics_spec(topic_list, listener))
        except BaseException:
            self._listener = previous
            raise

    async def _a_subscribe_pattern(self, pattern: SubscriptionPattern,
                                   callback: ConsumerRebalanceListener) -> None:
        listener = self._subscribe_listener(callback)
        self._check_not_null("subscribe_pattern", pattern)
        previous, self._listener = self._listener, listener
        try:
            await self._run_async(*self._subscribe_pattern_spec(pattern.pattern(), listener))
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
        # Not on the ConsumerHandle (KIP-1306's RebalanceConsumer has no
        # assign): from inside a callback it reaches the busy consumer and
        # reports ConcurrentModificationError, as Java's acquire() does from
        # another thread.
        partition_list = list(self._assign_partitions(partitions))
        await self._run_async(*self._void_spec(_lib.Consumer_assign_cb, tp_to_spec(partition_list)))

    async def _a_unsubscribe(self) -> None:
        await self._run_async(*self._void_spec(_lib.Consumer_unsubscribe_cb))

    async def _a_poll(self, timeout: Duration) -> ConsumerRecords[K, V]:
        # The closed check first, then Java's Timer check (Order).
        self._check_open()
        timeout_ms = poll_timeout_ms(timeout)
        # A deferred error the poll keeps (see _run_async) is not raised by the
        # seeks back: the records go to the user first.
        result: Deserialized = await self._run_async(*self._poll_spec(timeout_ms), site=_AT_POLL,
                                                     abandoned=self._fetched_positions)
        cancelled = await self._rewind_async(result.rewind)
        if cancelled is not None:
            # Cancelled during the seeks back: the records are not returned either.
            await self._rewind_async(result.unreturned())
            raise cancelled
        if result.error is not None and result.records.is_empty():
            raise result.error
        return result.records

    async def _a_commit(self, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                        timeout: Duration = MISSING) -> None:
        offsets = self._offsets_arg(offsets)
        timeout_ms = self._timeout_ms(timeout, timer=True, group=True)
        if self._in_callback():
            self._reentrant_use("commit", offsets)
            return
        await self._run_async(*self._commit_spec(offsets, timeout_ms))

    async def _a_seek(self, partition: TopicPartition, offset: int,
                      offset_and_metadata: OffsetAndMetadata) -> None:
        to, oam = self._seek_args(partition, offset, offset_and_metadata)
        if self._in_callback():
            self._reentrant_use("seek", partition, to, oam)
            return
        await self._run_async(*self._seek_spec(partition, to, oam))

    async def _a_seek_to_beginning(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("seek_to_beginning", _lib.Consumer_seek_to_beginning_cb, partitions)

    async def _a_seek_to_end(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("seek_to_end", _lib.Consumer_seek_to_end_cb, partitions)

    async def _a_position(self, partition: TopicPartition,
                          timeout: Duration = MISSING) -> int:
        self._check_position_partition(partition)
        timeout_ms = self._timeout_ms(timeout, timer=True, handle_timed=True)
        if self._in_callback():
            position: int = self._reentrant_use("position", partition, timeout_ms)
            return position
        return await self._run_async(*self._position_spec(partition, timeout_ms))

    async def _a_current_lag(self, topic_partition: TopicPartition) -> int | None:
        self._check_not_null("current_lag", topic_partition)
        if self._in_callback():
            in_callback: int | None = self._reentrant_use("current_lag", topic_partition)
            return in_callback
        lag: int | None = await self._run_async(*self._current_lag_spec(topic_partition))
        return lag

    async def _a_committed(self, partitions: Iterable[TopicPartition],
                           timeout: Duration = MISSING
                           ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        self._check_committed_timeout(partitions, timeout)
        timeout_ms = self._timeout_ms(timeout, group=True)
        self._check_committed_partitions(partitions)
        partition_list = list(partitions)
        if self._in_callback():
            committed: dict[TopicPartition, OffsetAndMetadata | None] = self._reentrant_use(
                "committed", partition_list)
            return committed
        return await self._run_async(*self._committed_spec(partition_list, timeout_ms))

    async def _a_partitions_for(self, topic: str, timeout: Duration = MISSING
                                ) -> list[PartitionInfo]:
        timeout_ms = self._timeout_ms(timeout)
        self._check_partitions_for_topic(topic, timeout_ms)
        return await self._run_async(*self._partitions_for_spec(topic, timeout_ms))

    async def _a_list_topics(self, timeout: Duration = MISSING
                             ) -> dict[str, list[PartitionInfo]]:
        return await self._run_async(*self._list_topics_spec(self._timeout_ms(timeout)))

    async def _a_pause(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("pause", _lib.Consumer_pause_cb, partitions)

    async def _a_resume(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("resume", _lib.Consumer_resume_cb, partitions)

    async def _a_offsets_for_times(self, timestamps_to_search: Mapping[TopicPartition, int],
                                   timeout: Duration = MISSING
                                   ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        self._check_not_null("offsets_for_times", timestamps_to_search)
        timeout_ms = self._timeout_ms(timeout)
        timestamps = dict(timestamps_to_search)
        if self._in_callback():
            found: dict[TopicPartition, OffsetAndTimestamp | None] = self._reentrant_use(
                "offsets_for_times", timestamps)
            return found
        return await self._run_async(*self._offsets_for_times_spec(timestamps, timeout_ms))

    async def _long_offsets(self, op: str, fn: Any, partitions: Iterable[TopicPartition],
                            timeout: Duration) -> dict[TopicPartition, int]:
        self._check_not_null(op, partitions)
        timeout_ms = self._timeout_ms(timeout)
        partition_list = list(partitions)
        if self._in_callback():
            offsets: dict[TopicPartition, int] = self._reentrant_use(op, partition_list)
            return offsets
        return await self._run_async(*self._long_offsets_spec(fn, partition_list, timeout_ms))

    async def _a_beginning_offsets(self, partitions: Iterable[TopicPartition],
                                   timeout: Duration = MISSING
                                   ) -> dict[TopicPartition, int]:
        return await self._long_offsets("beginning_offsets",
                                        _lib.Consumer_beginning_offsets_cb, partitions, timeout)

    async def _a_end_offsets(self, partitions: Iterable[TopicPartition],
                             timeout: Duration = MISSING
                             ) -> dict[TopicPartition, int]:
        return await self._long_offsets("end_offsets", _lib.Consumer_end_offsets_cb,
                                        partitions, timeout)

    async def _a_close(self, option: CloseOptions) -> None:
        if self._is_closed():
            return
        timeout_ms, operation = close_args(option)
        # A deferred error (a coroutine commit callback's exception a poll kept,
        # see _finish_async) is raised once the consumer is closed, as Java's
        # close() raises a callback's exception: wrapped in "Failed to close
        # kafka consumer", a KeyboardInterrupt as it is (_raise_at). It is the
        # first exception, so it wins over a failure of the close itself.
        deferred = None if self._in_callback() else self._take_deferred_error()
        # A call left on a closed event loop does not hold the consumer. A
        # coroutine callback it delivered is reported failed (it can never
        # end); the core then ends the call and releases its guard before it
        # queues the completion and fires the notify, which close() waits for,
        # within its own timeout (abandoned_call_wait_s), so the core does not
        # refuse the close as a call in flight. A call that goes on waiting
        # after the callback (a poll), or one the wait does not see end, keeps
        # the guard, and close() is refused as for any call in flight.
        self._pending_event.clear()
        if self._release_abandoned():
            wait_s = abandoned_call_wait_s(timeout_ms)
            if wait_s > 0:
                await asyncio.get_running_loop().run_in_executor(
                    None, self._pending_event.wait, wait_s)
        try:
            began = self._begin_close()
        except BaseException:
            self._keep_error(deferred)
            raise
        if not began:
            self._keep_error(deferred)
            return
        try:
            await self._run_async(*self._close_spec(timeout_ms, operation),
                                  raise_deferred=False, site=_AT_CLOSE)
        except ConcurrentModificationError:
            # Still open: the deferred error is the next awaited call's again.
            self._abort_close()
            self._keep_error(deferred)
            raise
        except Exception:
            await self._finish_close_off_loop()
            if deferred is not None:
                self._raise_at(deferred, _AT_CLOSE)
            raise
        except BaseException:
            await self._finish_close_off_loop()
            raise
        await self._finish_close_off_loop()
        if deferred is not None:
            self._raise_at(deferred, _AT_CLOSE)

    # ---- coroutine callbacks (listener methods, commit callbacks) -----------
    def _defer_callback(self, awaitable: Awaitable[object], callback_id: int) -> bool:
        """Await a coroutine callback -- a listener method, or an
        ``OffsetCommitCallback`` -- in a task on the running loop, then report
        its result to the core (``kafka_consumer_Consumer__set_callback_result``):
        the exception it raised, ``CancelledError`` included, as a foreign
        callback error owning it, which the delivering call raises
        (``_raise_error``). The call delivering it waits for the task before going on
        (``_wait_on_loop``), and the core waits for the report before it delivers the next callback, so the
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
            interrupted: KeyboardInterrupt | None = None
            try:
                await awaitable
            except BaseException as exc:  # noqa: BLE001 - reported to the core
                error = exc
            finally:
                # Reached whatever the except clause raises (a Ctrl+C there).
                if report_now:
                    if error is not None and self._callback_error is None:
                        self._callback_error = error
                else:
                    interrupted = self._report_deferred(callback_id, error)
            if interrupted is not None:
                raise interrupted

        if not report_now:
            # Reported by the task, or, if its loop closes first, by
            # _release_abandoned.
            self._unacked[callback_id] = loop
        task = loop.create_task(run())
        self._listener_tasks.add(task)
        task.add_done_callback(self._listener_tasks.discard)
        if report_now:
            with self._lifecycle:
                if self._h:
                    _lib.Consumer__set_callback_result(self._h, callback_id, None)
        return True

    def _report_deferred(self, callback_id: int, error: BaseException | None
                         ) -> KeyboardInterrupt | None:
        """Report a deferred callback's outcome to the core once, whatever is
        raised meanwhile: the core parks the delivering call, and ``destroy``,
        until it comes. A Ctrl+C landing while it is reported (under
        ``asyncio.run`` the second one raises ``KeyboardInterrupt`` in the
        running code) reports it again; the core drops a report for an id
        already reported, so one landing after the report is harmless. The
        ``KeyboardInterrupt`` is returned for the task to raise.

        Under the lifecycle lock: ``close()`` clears the handle under it before
        it frees the handle, so a live handle is not freed meanwhile."""
        interrupted: KeyboardInterrupt | None = None
        while True:
            try:
                with self._lifecycle:
                    self._unacked.pop(callback_id, None)
                    if self._h:
                        _lib.Consumer__set_callback_result(self._h, callback_id, error)
                return interrupted
            except KeyboardInterrupt as exc:
                interrupted = exc

    # ---- commit_nowait (Java's commitAsync) on the event loop ----------------
    async def _a_commit_nowait(self, offsets: Any, callback: Callable[..., object] | None) -> None:
        """``commit_nowait()`` on the event loop (see ``commit_nowait``).

        Inside a callback, it is ``Consumer``'s, through the ConsumerHandle.
        Otherwise it awaits the ``_cb`` form's completion (``_run_async``), the
        same completion the synchronous ``Consumer`` waits on, running the queued
        callbacks on the loop and awaiting a coroutine one there; its failure is
        raised as it is, as Java's ``commitAsync`` throws it."""
        if self._in_callback():
            _ConsumerState._c_commit_nowait(self, offsets, callback)
            return
        offsets = self._offsets_arg(offsets)
        with self._use():
            spec = None if offsets is None else offsets_to_spec(offsets)
            adapter = self._wrap_commit_callback(
                callback, empty_offsets=offsets is not None and not offsets)
        await self._run_async(*self._commit_nowait_spec(spec, adapter))

    async def _finish_close_off_loop(self) -> None:
        """``_finish_close`` waits for the uses in flight and joins the core's
        tasks: off the event loop, and to its end even if the task is cancelled
        meanwhile, so ``close()`` does not return before it."""
        await await_to_end(asyncio.get_running_loop().run_in_executor(None, self._finish_close))
