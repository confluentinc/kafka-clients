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

"""``AsyncProducer``: the asyncio peer of :class:`~confluent_kafka.producer.Producer`.

By rule 3, the same methods as ``Producer`` (CLAUDE.md, Python Binding
Conventions, Class family), each ``async def`` iff the entry point it calls has
a queued ``_cb`` completion form in the FFI header: ``init_transactions``,
``begin_transaction``, ``send_offsets_to_transaction``, ``commit_transaction``,
``abort_transaction``, ``send``, ``flush``, ``partitions_for`` and ``close``.
``metrics`` has none, so it is a plain ``def``. ``send`` returns an
``asyncio.Future``, so a round trip is ``md = await (await p.send(record=r))``.

The producer's queued completions -- the ``send()`` callbacks and the results
of the waiting calls -- are run by ``kafka_producer_Producer__execute_callbacks``
on an event loop: the C API's notify hook schedules that pump with
``loop.call_soon_threadsafe`` on the running loops this producer was used
from, and each async method pumps once on entry. Each result then reaches the
loop awaiting it through ``call_soon_threadsafe``, and a ``send()`` callback
runs on its record's loop, before its future completes (Implementation over
the FFI, Threads and callbacks). When none of those loops is running, the pump
runs on a short-lived helper thread instead, so a completion whose loop has
closed is dropped promptly (see :meth:`AsyncProducer.send`).

By rule 3, a ``send()`` callback may be an ``async def``: it is awaited in a task
on its record's loop, and the callbacks of a loop still run one at a time in
completion order, a later one waiting for the awaited one to end. The C API
completes the record's future, and lets ``flush()``, ``commit_transaction()``,
``abort_transaction()`` and ``close()`` return, once the callback reported
(``kafka_producer_Producer__set_callback_result``): a callback the pump runs on
its own loop reports by returning, any other once it ran on its loop, or once
its task ended (see ``_send``).

*(deviation)* Backpressure is the core's ``max.block.ms`` wait on the
submission task, not a wait in ``send()`` (see ``_base``).
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import logging
import threading
import weakref
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import UNSET
from confluent_kafka._async import Undelivered, await_to_end
from confluent_kafka._errors import from_ffi_error
from confluent_kafka.common.errors.timeout_error import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.null_pointer_error import NullPointerError

from ._base import (
    FLUSH_IN_CALLBACK_MESSAGE,
    LONG_MAX_VALUE,
    _ProducerState,
    check_group_metadata,
    close_timeout_ms,
    closed_while_sending,
    free_error,
    free_error_payload,
    group_metadata_handle,
    offsets_to_spec,
    raise_if_error,
    to_metrics_map,
    to_partition_info,
)
from ._send import (
    MetadataTuple,
    completion_to_python,
    failure_metadata,
    log_unreported_callback_error,
    run_callback,
)
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import MetricName, PartitionInfo, TopicPartition
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from .producer_record import ProducerRecord

__all__ = ["AsyncProducer"]

K = TypeVar("K")
V = TypeVar("V")

_LOG = logging.getLogger("confluent_kafka.producer")

#: The ``send()`` callback of the asyncio producer: Java's ``Callback``, which by
#: rule 3 may also be an ``async def``, awaited on the record's event loop.
_AsyncCallback = Callable[[RecordMetadata, Exception | None], Awaitable[None] | None]

# A completion the callback pump buffered for the event loop: (future, callback,
# metadata, exception, callback id). The id is 0 when there is nothing to
# report: a record the send itself refused, completed from its handover.
_Pending = tuple["asyncio.Future[RecordMetadata]", "_AsyncCallback | None", RecordMetadata,
                 "Exception | None", int]

# The producers (by id) whose ``async def`` send() callback the current task
# awaits, Java's ``Thread.currentThread() == this.ioThread``: the C API waits
# for that callback's report before flush(), commit_transaction(),
# abort_transaction() and close() complete, so the callback must not wait for
# them.
_in_async_callback: contextvars.ContextVar[frozenset[int]] = contextvars.ContextVar(
    "confluent_kafka_producer_in_async_callback", default=frozenset())

# Java's warning for a close() with a timeout from a callback
# (KafkaProducer.close(Duration, boolean)).
_CLOSE_IN_CALLBACK_MESSAGE = (
    "Overriding close timeout %d ms to 0 ms in order to prevent useless blocking due to "
    "self-join. This means you have incorrectly invoked close with a non-zero timeout from "
    "the producer call-back.")

# The message of a transaction-end call from an async send() callback that ran
# out of max.block.ms: it waits for the callback's own report.
_ASYNC_CALLBACK_WAIT_TIMEOUT_MESSAGE = (
    "Timeout expired after {ms}ms while awaiting {operation} from an async send() callback, "
    "whose own report it waits for")

# How often a wait for the callbacks' reports looks for completions stranded
# on an event loop that has closed (see AsyncProducer._await_payload).
_STRANDED_POLL_S = 0.1


def _running_loop() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def _settle(future: asyncio.Future[RecordMetadata], metadata: RecordMetadata,
            exception: Exception | None) -> None:
    """Complete a record's future (on its loop) unless its awaiter cancelled it."""
    if future.done():
        return
    if exception is not None:
        future.set_exception(exception)
    else:
        future.set_result(metadata)


def _set_result(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


def _drop_completion(metadata: RecordMetadata) -> None:
    """A completion whose event loop has closed: no loop can run its callback
    (it runs on the event loop), or finish one it awaits, or complete its
    future any more, so it is dropped and one warning is logged."""
    _LOG.warning("The completion of a record sent to topic-partition '%s' was dropped: its "
                 "event loop had closed, so its callback did not run and its future does not "
                 "complete", f"{metadata.topic()}-{metadata.partition()}")


class _AsyncCompletion:
    """The completions of a send that is not waited for: called by the callback
    pump (any thread) as the record's delivery callback, and :meth:`resolve` at
    its handover; both hand the completion to the record's event loop (see
    AsyncProducer._deliver), once. One object rather than closures over its
    values, so a record in flight keeps fewer objects alive for the garbage
    collector to scan."""

    __slots__ = ("_producer", "_callback", "_future", "_topic", "_partition", "_loop",
                 "_fired")

    def __init__(self, producer: AsyncProducer[Any, Any], callback: _AsyncCallback | None,
                 future: asyncio.Future[RecordMetadata], topic: str, partition: int | None,
                 loop: asyncio.AbstractEventLoop) -> None:
        self._producer = producer
        self._callback = callback
        self._future = future
        self._topic = topic
        self._partition = partition
        self._loop = loop
        # Whether the record completed: both the delivery callback and a failed
        # handover can complete it, and pumps may run on several threads.
        self._fired = False

    def _complete(self, metadata: RecordMetadata, exception: Exception | None,
                  callback_id: int) -> BaseException | bool | None:
        # Once per record: the callback and the future are resolved on the
        # record's loop (see _deliver).
        producer = self._producer
        with producer._fire_lock:
            if self._fired:
                return None
            self._fired = True
        return producer._deliver(self._loop, (self._future, self._callback, metadata, exception,
                                              callback_id))

    def __call__(self, metadata_tuple: MetadataTuple | None, error: int,
                 callback_id: int) -> bool | None:
        # The error handle is borrowed: converted during the call. Returns True
        # when the report comes later, raises the callback's exception for the C
        # extension to report (see _send).
        metadata, exception = completion_to_python(metadata_tuple, error, self._topic,
                                                   self._partition)
        outcome = self._complete(metadata, exception, callback_id)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def resolve(self, error: int) -> None:
        """The handover: the error the Rust send() returned, if any. Nothing to
        report then: the delivery callback does not run for this record, so an
        exception its callback raised is logged here (see _report)."""
        if error:
            exception = cast("Exception", from_ffi_error(error))
            metadata = failure_metadata(self._topic, self._partition)
            outcome = self._complete(metadata, exception, 0)
            if isinstance(outcome, BaseException):
                log_unreported_callback_error(metadata, outcome)


class AsyncProducer(Generic[K, V], _ProducerState):
    """The asyncio peer of ``Producer``, the interface for the
    ``AsyncKafkaProducer``: an async context manager whose exit flushes, then
    closes."""

    def __init__(self) -> None:
        if type(self) is AsyncProducer:
            raise TypeError("AsyncProducer is a non-instantiable base; use "
                            "AsyncKafkaProducer or AsyncMockProducer")
        _ProducerState.__init__(self)
        # Completions the callback pump buffered, per event loop, and resolved
        # on that loop in completion order, one callback at a time: a loop is
        # in _busy while a _complete_pending call is scheduled on it or an
        # async callback runs there (_awaiting), and later completions for it
        # wait in _pending meanwhile. _pending holds a loop only while it is
        # busy.
        self._pending: dict[asyncio.AbstractEventLoop, list[_Pending]] = {}
        self._busy: set[asyncio.AbstractEventLoop] = set()
        self._awaiting: dict[asyncio.AbstractEventLoop, _Pending] = {}
        self._callback_tasks: set[asyncio.Task[None]] = set()
        self._pending_lock = threading.Lock()
        # Keeps each record's completion to once (see _AsyncCompletion): one lock
        # for the producer, not one per record.
        self._fire_lock = threading.Lock()
        # The results of the other async operations on their way to their loop.
        self._undelivered = Undelivered()
        # The event loops this producer was used from: the notify hook
        # schedules the callback pump on the running ones.
        self._loops: weakref.WeakSet[asyncio.AbstractEventLoop] = weakref.WeakSet()
        self._loops_lock = threading.Lock()

    # ---- callback pump -------------------------------------------------------

    def _notify_hook(self) -> Callable[[], None]:
        # A weak reference: the native producer keeps the hook until it is
        # destroyed, and must not keep an unclosed producer alive.
        ref = weakref.ref(self)

        def notify() -> None:
            producer = ref()
            if producer is not None:
                producer._schedule_pump()

        return notify

    def _schedule_pump(self) -> None:
        """The notify hook (any thread, the GIL held): schedule :meth:`_pump`
        on every running loop this producer was used from, or, with none
        running, run it on a helper thread. It only schedules, never runs a
        callback (CLAUDE.md §4)."""
        with self._loops_lock:
            loops = list(self._loops)
        scheduled = False
        for loop in loops:
            if loop.is_closed() or not loop.is_running():
                continue
            try:
                loop.call_soon_threadsafe(self._pump)
                scheduled = True
            except RuntimeError:  # the loop closed since the check
                continue
        if not scheduled:
            threading.Thread(target=self._pump, name="confluent-kafka-producer-pump",
                             daemon=True).start()

    def _pump(self) -> None:
        """Run the producer's queued callbacks on this thread, once the native
        producer exists and until it is destroyed; callable from any thread."""
        if not self._c_producer or not self._pump_use():
            return
        try:
            _lib.Producer__execute_callbacks(self._c_producer)
        finally:
            self._end_use()

    def _enter(self) -> asyncio.AbstractEventLoop:
        """The start of each async method: register the running loop with the
        notify hook, run what is already queued, and free what is left for a
        loop that has closed."""
        loop = asyncio.get_running_loop()
        with self._loops_lock:
            self._loops.add(loop)
        self._pump()
        self._undelivered.free_closed()
        return loop

    def _deliver(self, loop: asyncio.AbstractEventLoop,
                 item: _Pending) -> BaseException | bool | None:
        """Hand one record's completion to its ``loop`` (called by the pump on
        any thread). Run inline when the pump runs on ``loop`` with nothing
        before it there: ``None``, or the exception the callback raised, which
        the caller reports by returning or raising (see ``_send``). Otherwise
        ``True``, the report coming later: buffered, then resolved on the loop
        by one scheduled call per batch, or queued behind the async callback
        the loop awaits; dropped, and reported, with :func:`_drop_completion`'s
        warning when the loop has closed."""
        if loop.is_closed():
            # The completion is dropped, then the future untracked, so a later
            # flush() does not wait for it (see _completable).
            self._drop([item])
            return True
        with self._pending_lock:
            inline = loop not in self._busy and _running_loop() is loop
            schedule = False
            if not inline:
                self._pending.setdefault(loop, []).append(item)
                schedule = loop not in self._busy
                self._busy.add(loop)
        if inline:
            return self._run_inline(loop, item)
        if schedule:
            try:
                loop.call_soon_threadsafe(self._complete_pending, loop)
            except RuntimeError:  # the loop closed since the check
                with self._pending_lock:
                    refused = self._pending.pop(loop, [])
                    self._busy.discard(loop)
                self._drop(refused)
        return True

    def _start_callback(self, loop: asyncio.AbstractEventLoop,
                        item: _Pending) -> tuple[bool, BaseException | None]:
        """Run ``item``'s callback on ``loop``'s thread: ``(True, None)`` when
        it is an ``async def``, whose task now finishes the completion (see
        :meth:`_await_callback`), else ``(False, the exception it raised)``,
        which the caller reports to the core (see ``_send``)."""
        _future, callback, metadata, exception, _callback_id = item
        if callback is None:
            return False, None
        try:
            result = run_callback(self, callback, metadata, exception)
        except Exception as exc:  # noqa: BLE001 - reported to the core
            return False, exc
        if inspect.isawaitable(result):
            self._await_callback(loop, item, result)
            return True, None
        return False, None

    def _run_inline(self, loop: asyncio.AbstractEventLoop,
                    item: _Pending) -> BaseException | bool | None:
        """:meth:`_deliver` inline, inside the pump on ``loop``."""
        deferred, failure = self._start_callback(loop, item)
        if deferred:
            return True
        future, _callback, metadata, exception, _callback_id = item
        _settle(future, metadata, exception)
        self._discard_future(future)
        return failure

    def _await_callback(self, loop: asyncio.AbstractEventLoop, item: _Pending,
                        awaitable: Awaitable[object]) -> None:
        """Await an ``async def`` callback in a task on ``loop`` (by rule 3),
        the loop staying busy meanwhile: once it ends, the callback is
        reported with its exception (``CancelledError`` included), which the
        core logs, the future completed, and the loop's next completion run."""
        producer_id = id(self)

        async def run() -> None:
            # The task's own copy of the context: a flush() or close() the
            # callback awaits sees it is inside the callback.
            _in_async_callback.set(_in_async_callback.get() | {producer_id})
            await awaitable

        with self._pending_lock:
            self._busy.add(loop)
            self._awaiting[loop] = item
        task = loop.create_task(run())
        self._callback_tasks.add(task)

        def done(task: asyncio.Task[None]) -> None:
            self._callback_tasks.discard(task)
            with self._pending_lock:
                if self._awaiting.get(loop) is not item:
                    return  # dropped meanwhile (see _drop_stranded)
                del self._awaiting[loop]
            failure: BaseException | None
            if task.cancelled():
                failure = asyncio.CancelledError()
            else:
                failure = task.exception()
            future, _callback, metadata, exception, callback_id = item
            self._report(callback_id, failure, metadata)
            _settle(future, metadata, exception)
            self._discard_future(future)
            self._complete_pending(loop)

        task.add_done_callback(done)

    def _report(self, callback_id: int, failure: BaseException | None,
                metadata: RecordMetadata | None = None) -> None:
        """Report a callback to the C API (``Producer__set_callback_result``),
        with the exception it raised, if any, which the core logs. Under the
        lifecycle lock and only while the handle is alive: :meth:`_destroy` sets
        ``_destroyed`` under it before it frees the handle, which completes what
        is left unreported. A completion the core does not deliver (the callback
        id 0) has no report: an exception its callback raised is logged here
        instead (see ``_send``)."""
        if not callback_id:
            if failure is not None and metadata is not None:
                log_unreported_callback_error(metadata, failure)
            return
        with self._lifecycle:
            if self._destroyed or not self._c_producer:
                return
            _lib.Producer__set_callback_result(self._c_producer, callback_id, failure)

    def _in_own_callback(self) -> bool:
        """Whether the current task is this producer's ``async def`` callback."""
        return id(self) in _in_async_callback.get()

    async def init_transactions(self) -> None:
        """See :meth:`Producer.init_transactions`."""
        self._check_not_closed()
        await self._run_async(_lib.Producer_init_transactions_cb)

    async def begin_transaction(self) -> None:
        """See :meth:`Producer.begin_transaction`."""
        self._check_not_closed()
        await self._run_async(_lib.Producer_begin_transaction_cb)
        self._in_transaction = True

    async def send_offsets_to_transaction(
            self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        """See :meth:`Producer.send_offsets_to_transaction`."""
        self._check_not_closed()
        check_group_metadata(group_metadata)
        native = group_metadata_handle(group_metadata)
        spec = offsets_to_spec(offsets)
        await self._run_async(_lib.Producer_send_offsets_to_transaction_cb, spec, native)

    async def commit_transaction(self) -> None:
        """See :meth:`Producer.commit_transaction`: when it returns, the
        callbacks of the transaction's records have run, ``async def`` ones
        awaited, but for those of a record whose event loop has closed, which
        are dropped (see :meth:`send`). From an ``async def`` callback, which
        it would wait for, it raises ``TimeoutError`` after ``max.block.ms``,
        as Java's does from a callback."""
        self._check_not_closed()
        sent = list(self._futures)
        # Sends from now on wait for their handover again (see _ProducerState).
        self._in_transaction = False
        await self._run_async(_lib.Producer_commit_transaction_cb, reports="EndTxn(true)")
        sent = self._completable(sent)
        if sent:
            await asyncio.wait(sent)

    async def abort_transaction(self) -> None:
        """See :meth:`Producer.abort_transaction`. From an ``async def``
        callback, which it would wait for, it raises ``TimeoutError`` after
        ``max.block.ms``, as Java's does from a callback."""
        self._check_not_closed()
        # Sends from now on wait for their handover again (see _ProducerState).
        self._in_transaction = False
        await self._run_async(_lib.Producer_abort_transaction_cb, reports="EndTxn(false)")

    async def send(self, *, record: ProducerRecord[K, V],
                   callback: _AsyncCallback | None = UNSET) -> asyncio.Future[RecordMetadata]:
        """See :meth:`Producer.send`. With a ``transactional.id`` and no
        transaction started, awaiting it waits for the record to reach the
        producer, without blocking the loop, and raises what
        :meth:`Producer.send` raises; cancelling the awaiting task then lets the
        wait end, then raises ``CancelledError``. The returned
        ``asyncio.Future`` resolves with the record's metadata. The
        ``callback`` runs on the event loop, before the future completes, and
        must not block it. By rule 3 it may be an ``async def``: it is then
        awaited in a task on the loop before the future completes, the loop's
        later callbacks waiting for it, and ``flush()``,
        ``commit_transaction()``, ``abort_transaction()`` and ``close()`` wait
        for it too. Awaiting, from inside such a callback, the future of a
        record that completes after it never returns, that record's callback
        waiting behind this one (Java's blocking ``get()`` in a callback). A
        raising callback, ``async def`` or not (``CancelledError`` included),
        fails nothing: its exception is reported to the core, which logs it
        once with Java's message and the exception's text. A record that completes
        after its event loop has closed is dropped with a warning on the
        ``confluent_kafka.producer`` logger: its callback does not run (or an
        awaited one does not finish) and its future does not complete."""
        if callback is UNSET:
            callback = None  # Java's send(record) passes null
        self._check_not_closed()
        native, topic, partition = self._native_parts(record)
        loop = self._enter()
        future: asyncio.Future[RecordMetadata] = loop.create_future()
        if self._transactional and not self._in_transaction:
            return await self._send_waiting(native, topic, partition, callback, loop, future)

        # Not waited for: no transactional.id, or inside a started transaction.
        cb = _AsyncCompletion(self, callback, future, topic, partition, loop)
        # A close() that began since the check above (the serializers ran in
        # between) refuses the record as Java's RecordAccumulator.append does.
        with self._send_use as c_producer:
            raise_if_error(_lib.Producer_send_cb(c_producer, native, cb, cb.resolve))
            self._track(future)
        return future

    async def _send_waiting(self, native: object, topic: str, partition: int | None,
                            callback: _AsyncCallback | None, loop: asyncio.AbstractEventLoop,
                            future: asyncio.Future[RecordMetadata]
                            ) -> asyncio.Future[RecordMetadata]:
        """``send()`` of a transactional producer with no transaction started.

        See Producer.send: it awaits the handover and raises the error the Rust
        send() returns.
        """
        handed_over: asyncio.Future[None] = loop.create_future()
        lock = threading.Lock()
        completed = False
        rethrown: Exception | None = None

        def complete(metadata: RecordMetadata, exception: Exception | None,
                     callback_id: int) -> BaseException | bool | None:
            # Once per record, from the pump (any thread): the callback and the
            # future are resolved on the record's loop (see _deliver).
            nonlocal completed
            with lock:
                if completed:
                    return None
                completed = True
            return self._deliver(loop, (future, callback, metadata, exception, callback_id))

        def on_completion(metadata_tuple: MetadataTuple | None, error: int,
                          callback_id: int) -> bool | None:
            # The error handle is borrowed: converted during the call. Returns
            # True when the report comes later, raises the callback's exception
            # for the C extension to report (see _send).
            metadata, exception = completion_to_python(metadata_tuple, error, topic, partition)
            outcome = complete(metadata, exception, callback_id)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        def resolve(error: int) -> None:
            # The handover: the error the Rust send() returned, if any,
            # rethrown by send(); the record's Callback may still run, once, if
            # the record had reached the accumulator (see Producer.send).
            nonlocal rethrown
            if error:
                rethrown = cast("Exception", from_ffi_error(error))
            if not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(_set_result, handed_over)
                except RuntimeError:  # the loop closed since the check
                    pass

        with self._send_use as c_producer:
            raise_if_error(_lib.Producer_send_cb(c_producer, native, on_completion, resolve))
        try:
            await await_to_end(handed_over)
        finally:
            # Tracked only once handed over without an error (see
            # Producer.send). Otherwise send() does not return the future:
            # cancelled, a later completion of the record runs only its
            # callback (_complete_pending skips a done future), and an
            # error there is not logged as never retrieved.
            if rethrown is None:
                self._track(future)
            elif not future.cancel():
                future.exception()  # completed already: its error is retrieved
        if rethrown is not None:
            raise rethrown
        return future

    def _complete_pending(self, loop: asyncio.AbstractEventLoop) -> None:
        """Resolve the completions buffered for ``loop``, on ``loop``, in
        completion order: the callback runs and is reported, then the future
        completes and is untracked (see _ProducerState._track), also when its
        awaiter cancelled it. An ``async def`` callback stops the run: its task
        resumes it once it ends (see :meth:`_await_callback`)."""
        while True:
            with self._pending_lock:
                items = self._pending.get(loop)
                if not items:
                    self._pending.pop(loop, None)
                    self._busy.discard(loop)
                    return
                item = items.pop(0)
            deferred, failure = self._start_callback(loop, item)
            if deferred:
                return
            future, _callback, metadata, exception, callback_id = item
            self._report(callback_id, failure, metadata)
            _settle(future, metadata, exception)
            self._discard_future(future)

    async def flush(self) -> None:
        """See :meth:`Producer.flush`: when it returns, the callbacks of the
        records sent before it have run, ``async def`` ones awaited, but for
        those of a record whose event loop has closed, which are dropped (see
        :meth:`send`). From an ``async def`` callback it raises ``KafkaError``,
        as Java's does from a callback: it would wait for that callback."""
        self._check_not_closed()
        if self._in_own_callback():
            _LOG.error(FLUSH_IN_CALLBACK_MESSAGE)
            raise KafkaError(message=FLUSH_IN_CALLBACK_MESSAGE)
        sent = list(self._futures)
        # The C API drains the submission queue first (see Producer.flush).
        await self._run_async(_lib.Producer_flush_cb, reports="flush")
        sent = self._completable(sent)
        if sent:
            await asyncio.wait(sent)

    def _completable(self, sent: list[Any]) -> list[Any]:
        """The futures of ``sent`` a wait can still see complete. One whose event
        loop has closed never completes: it is untracked instead of waited for,
        and its completion is dropped with :func:`_drop_completion`'s warning,
        when it arrives (see ``send``) or, if it arrived while the loop was
        open but was left for the loop to run, here (and in ``close()``)."""
        self._drop_stranded()
        completable: list[Any] = []
        for future in sent:
            if future.get_loop().is_closed():
                self._futures.discard(future)
            else:
                completable.append(future)
        return completable

    def _drop_stranded(self) -> None:
        """Drop the completions left for event loops that have closed: the
        scheduled call that would resolve them on the loop never runs, nor does
        the end of an ``async def`` callback the loop was awaiting."""
        with self._pending_lock:
            closed_loops = [loop for loop in self._busy if loop.is_closed()]
            stranded: list[_Pending] = []
            for loop in closed_loops:
                self._busy.discard(loop)
                awaited = self._awaiting.pop(loop, None)
                if awaited is not None:
                    stranded.append(awaited)
                stranded.extend(self._pending.pop(loop, []))
        self._drop(stranded)

    def _drop(self, items: list[_Pending]) -> None:
        """Drop completions whose event loop has closed, untracking their
        futures (see :func:`_drop_completion`), and report their callbacks, so
        the C API's waits for the reports end."""
        for future, _callback, metadata, _exception, callback_id in items:
            _drop_completion(metadata)
            self._futures.discard(future)
            self._report(callback_id, None)

    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """See :meth:`Producer.partitions_for`."""
        self._check_not_closed()
        if topic is None:
            raise NullPointerError(message="topic cannot be null")
        partitions, error = await self._await_payload(_lib.Producer_partitions_for_cb, topic)
        raise_if_error(error)
        return [to_partition_info(t) for t in partitions]

    def metrics(self) -> dict[MetricName, Metric]:
        """See :meth:`Producer.metrics`. Its entry point has no queued ``_cb``
        form, so it is a plain ``def``."""
        self._check_not_closed()
        return to_metrics_map(self._call(_lib.Producer_metrics))

    async def close(self, *, timeout: Duration | None = None) -> None:
        """See :meth:`Producer.close`: it waits for the callbacks of the
        records sent before it, ``async def`` ones included. From an ``async
        def`` callback it does not wait and is equivalent to
        ``close(timeout=0)``, as Java's from a callback: the producer is freed
        once the callback has ended."""
        if self._closed:
            return
        timeout_ms = close_timeout_ms(timeout)
        # Check and set at once: exactly one close() tears the producer down.
        if not self._begin_close():
            return
        if self._in_own_callback():
            if timeout_ms is None or timeout_ms > 0:
                _LOG.warning(_CLOSE_IN_CALLBACK_MESSAGE,
                             LONG_MAX_VALUE if timeout_ms is None else timeout_ms)
            self._close_from_callback()
            return
        c_producer = self._c_producer
        loop = self._enter()
        # Whether the Rust close ran to its end; if the task was cancelled
        # first, the close is forced before the handle is freed (see
        # _teardown).
        closed = False
        try:
            # close() is Java's close(Duration.ofMillis(Long.MAX_VALUE)). The C
            # API drains the submission queue first (as flush), so every record
            # sent before is included; a timed close spends at most its timeout on
            # that drain and the close together, failing what it did not reach.
            if timeout_ms is None:
                (error,) = await self._await_payload(_lib.Producer_close_cb, owner=True,
                                                     reports="close")
            else:
                (error,) = await self._await_payload(
                    _lib.Producer_close_with_timeout_cb, timeout_ms, owner=True,
                    reports="close")
            closed = True
            raise_if_error(error)
        finally:
            # Forces an unfinished close, waits for the calls still in flight,
            # then frees the handle (running the callbacks still pending there):
            # off the loop, and to its end even if this task is cancelled again
            # meanwhile, so the serializers close after it.
            teardown = loop.run_in_executor(None, self._teardown, c_producer, not closed)
            try:
                await await_to_end(teardown)
            finally:
                self._close_serializers()
                # What is left for an event loop that has closed is freed.
                self._undelivered.free_closed()
                self._drop_stranded()

    def _close_from_callback(self) -> None:
        """Java's ``close(0)`` from the I/O thread, which does not join itself:
        queue a close with a zero timeout and return. Its completion arrives
        once the callback calling it has ended and reported, in the pump, which
        holds a use of the handle: the teardown then runs on a helper
        thread."""

        def closed(error: int) -> None:
            free_error(error)
            threading.Thread(target=self._finish_close_from_callback,
                             name="confluent-kafka-producer-close", daemon=True).start()

        _lib.Producer_close_with_timeout_cb(self._c_producer, 0, closed)

    def _finish_close_from_callback(self) -> None:
        self._destroy()
        self._close_serializers()
        self._undelivered.free_closed()
        self._drop_stranded()

    def _teardown(self, c_producer: int, force: bool) -> None:
        if force:
            # Java's sender.forceClose() for a close() the cancellation stopped
            # (KafkaProducer.close(Duration, boolean), KafkaProducer.java:1506-1524):
            # a close with a zero timeout fails the records still pending.
            _lib.Producer_close_with_timeout_cb(c_producer, 0, free_error)
        self._destroy()

    async def __aenter__(self) -> AsyncProducer[K, V]:
        return self

    async def __aexit__(self, *exc: object) -> None:
        # Closeable: flush, then close (the close runs even if the flush fails).
        try:
            if not self._closed:
                await self.flush()
        finally:
            await self.close()

    # ---- completion primitives ----------------------------------------------

    async def _await_payload(self, entry: Callable[..., None], *args: Any,
                             owner: bool = False, reports: str | None = None
                             ) -> tuple[Any, ...]:
        """Submit ``entry(handle, *args, cb)`` (a queued ``_cb`` entry point) and
        await its completion payload on the loop. The completion runs in the
        callback pump and hops onto the loop via ``call_soon_threadsafe``; a
        payload nobody awaits any more (a cancelled task, a closed loop) has its
        error handle freed (see :class:`~confluent_kafka._async.Undelivered`).

        The submission is one use of the handle, which raises Java's closed
        ``IllegalStateError`` once a ``close()`` has begun; only ``close()``,
        which owns the handle, passes ``owner``.

        ``reports`` names an operation whose completion the C API delivers only
        once the callbacks of the records sent before it reported: the wait
        drops, and so reports, the completions left on an event loop that
        closes meanwhile (:meth:`_drop_stranded`), which would otherwise never
        report; from this producer's ``async def`` callback it ends with
        ``TimeoutError`` after ``max.block.ms``, the callback's own report
        being among those waited for."""
        loop = self._enter()
        fut: asyncio.Future[tuple[Any, ...]] = loop.create_future()
        free = free_error_payload

        def deliver(payload: tuple[Any, ...]) -> None:
            if fut.done():
                free(payload)
                return
            fut.set_result(payload)

        def cb(*payload: Any) -> None:
            self._undelivered.hand_over(loop, deliver, payload, free)

        if owner:
            entry(self._c_producer, *args, cb)
        else:
            with self._use() as c_producer:
                entry(c_producer, *args, cb)
        if reports is None:
            return await fut
        if self._in_own_callback():
            try:
                return await asyncio.wait_for(fut, self._max_block_ms / 1000.0)
            except asyncio.TimeoutError:
                raise KafkaTimeoutError(message=_ASYNC_CALLBACK_WAIT_TIMEOUT_MESSAGE.format(
                    ms=self._max_block_ms, operation=reports)) from None
        try:
            while not fut.done():
                await asyncio.wait((fut,), timeout=_STRANDED_POLL_S)
                self._drop_stranded()
        except BaseException:
            # A cancelled wait: the payload, when it arrives, is freed.
            fut.cancel()
            raise
        return fut.result()

    async def _run_async(self, entry: Callable[..., None], *args: Any,
                         reports: str | None = None) -> None:
        """A void queued FFI operation, awaited; raises its typed error. See
        :meth:`_await_payload` for ``reports``."""
        (error,) = await self._await_payload(entry, *args, reports=reports)
        raise_if_error(error)
