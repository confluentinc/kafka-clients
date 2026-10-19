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
``AsyncKafkaFuture``, so a round trip is
``md = await (await p.send(record=r)).get()``.

The producer's queued completions -- the ``send()`` callbacks and the results
of the waiting calls -- are run by ``kafka_producer_Producer__execute_callbacks``
on the producer's event loop, with the GIL held from the first callback to the
last. By rule 3 the producer is bound to one loop: the one running when it is
constructed, or else the loop of its first async call; a call from another
loop raises ``RuntimeError``, as asyncio's own primitives do. The C API's
notify hook wakes that loop through a socket pair it watches (one byte written
from the Rust task's thread, no GIL taken there; see
:meth:`AsyncProducer._install_wake`), or, before the loop is bound and where
the loop cannot watch a socket, schedules the pump on it with ``call_soon_on``,
and each async method pumps on entry (``send()`` on every 64th send in a row
that the producer took at once, the sends in between only counting; see
``AsyncProducer._PUMP_EVERY``), so the callbacks have a single drainer. A ``send()`` callback runs on the loop before its record's future
completes (Implementation over the FFI, Threads and callbacks). Once the loop
has closed, the pump runs on one helper thread instead, so a completion left
for it is dropped promptly (see :meth:`AsyncProducer.send`).

By rule 3, a ``send()`` callback may be an ``async def``: it is awaited in a task
on the producer's event loop, and the callbacks still run one at a time in
completion order, a later one waiting for the awaited one to end. The C API
completes the record's future, and lets ``flush()``, ``commit_transaction()``,
``abort_transaction()`` and ``close()`` return, once the callback reported
(``kafka_producer_Producer__set_callback_result``): a callback the pump runs
inline reports by returning, one queued behind an awaited callback once it ran,
an ``async def`` one once its task ended, and one whose loop has closed at once,
dropped (see ``_send``).

Awaiting ``send()`` waits for the record to reach the producer, as Java's
``send()`` does, without blocking the loop, and so gives Java's backpressure
(see ``_base``): the record is taken on the loop's thread when the producer can
take it at once, and waits on a core task otherwise. The future ``send()``
returns, that future's state and the record's completions are one object,
allocated once per send (see ``_AsyncRecord``).
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import logging
import socket
import threading
import weakref
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import UNSET
from confluent_kafka._async import (
    LoopBinding,
    OffLoopPump,
    Undelivered,
    await_to_end,
    call_soon_on,
)
from confluent_kafka._errors import from_ffi_error
from confluent_kafka.common.errors.timeout_error import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.common.kafka_future import _PENDING, AsyncKafkaFuture, _FutureState

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
    run_callback,
)
from ._record_future import AsyncRecordFuture, complete_record
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
R = TypeVar("R")

_LOG = logging.getLogger("confluent_kafka.producer")

#: The ``send()`` callback of the asyncio producer: Java's ``Callback``, which by
#: rule 3 may also be an ``async def``, awaited on the record's event loop.
_AsyncCallback = Callable[[RecordMetadata, Exception | None], Awaitable[None] | None]

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


# A record's handover not yet resolved (see _AsyncRecord.outcome).
_HANDOVER_PENDING: Any = object()


async def _await_all(loop: asyncio.AbstractEventLoop,
                     futures: list[_FutureState[RecordMetadata]]) -> None:
    """Wait on ``loop`` until every record future of ``futures`` is done: the
    internal wait of ``flush()`` and ``commit_transaction()``, never a public
    ``get()``. The records complete one at a time, on ``loop`` (the producer's
    single drainer), so the count needs no lock; were the last to complete
    elsewhere, the wait would still end on the loop."""
    if not futures:
        return
    all_done: asyncio.Future[None] = loop.create_future()
    remaining = len(futures)

    def finish() -> None:
        if not all_done.done():
            all_done.set_result(None)

    def one_done(_future: _FutureState[RecordMetadata]) -> None:
        nonlocal remaining
        remaining -= 1
        if remaining:
            return
        if _running_loop() is loop:
            finish()
        else:
            try:
                call_soon_on(loop, finish)
            except RuntimeError:  # the loop closed: nobody awaits any more
                pass

    for future in futures:
        future._add_listener(one_done)
    await all_done


def _drop_completion(metadata: RecordMetadata) -> None:
    """A completion whose event loop has closed: no loop can run its callback
    (it runs on the event loop), or finish one it awaits, or complete its
    future any more, so it is dropped and one warning is logged."""
    _LOG.warning("The completion of a record sent to topic-partition '%s' was dropped: its "
                 "event loop had closed, so its callback did not run and its future does not "
                 "complete", f"{metadata.topic()}-{metadata.partition()}")


class _AsyncRecord(AsyncRecordFuture, _FutureState[RecordMetadata]):
    """One record of the asyncio producer's ``send()``, as one object: the
    ``AsyncKafkaFuture`` ``send()`` returns (an :class:`AsyncRecordFuture`:
    ``cancel()`` answers ``False``, as Java's ``FutureRecordMetadata``), that
    future's own Python state (:class:`_FutureState`, its lock the producer's
    ``_fire_lock``, so no lock is made per record) and the completions of the
    send, which the C send gets: the record's delivery callback (``__call__``),
    called by the callback pump (the loop's thread, or a helper once the loop
    closed), which hands the completion to the producer's event loop (see
    AsyncProducer._deliver), once, and :meth:`resolve` at its handover, called
    by the pump too, or on the loop's thread inside ``Producer_send_cb`` for a
    record the producer takes or refuses at once, which records the handover
    in :attr:`outcome`: ``send()`` reads it once the call returns and awaits it
    (:meth:`waiter`) only for a record that was queued; ``None`` for a record
    handed over, else the error to rethrow. One object rather than a future,
    its state and a completion each, and the handover a slot rather than an
    ``asyncio.Future`` per record, so a send allocates once and a record in
    flight keeps fewer objects alive for the garbage collector to scan.

    The future's state (``_impl``) is the record itself, through a property
    rather than a reference stored on it, so a completed record is freed by
    its reference count, not by a cycle collection. A dependant built from it
    (``then_apply``, ``when_complete``) is an ordinary future over its own
    state."""

    __slots__ = ("_lock", "_state", "_outcome", "_listeners",
                 "_producer", "_callback", "_topic", "_partition",
                 "outcome", "_handover", "_fired",
                 "_metadata", "_exception", "_callback_id")

    # A record always has its loop (the base future may lack one until used).
    _loop: asyncio.AbstractEventLoop

    # The completion's payload, set once the record completed (see _complete).
    _metadata: RecordMetadata
    _exception: Exception | None
    _callback_id: int

    # No C source: the class attribute stands in for the base's slot, which is
    # never written for a record.
    _native = None

    @property
    def _impl(self) -> _AsyncRecord:  # type: ignore[override]
        return self

    def __init__(self, producer: AsyncProducer[Any, Any], callback: _AsyncCallback | None,
                 topic: str, partition: int | None, loop: asyncio.AbstractEventLoop) -> None:
        # The future (AsyncKafkaFuture's slots; _native is the class attribute).
        self._inner = None
        self._loop = loop
        # Its state (_FutureState's attributes).
        self._lock = producer._fire_lock
        self._state = _PENDING
        self._outcome = None
        self._listeners = None
        # The completions.
        self._producer = producer
        self._callback = callback
        self._topic = topic
        self._partition = partition
        # The handover: _HANDOVER_PENDING until resolve() ran, then None for a
        # record handed over, or the error the Rust send() returned. Written
        # and read on the loop's thread only (resolve() hops there from any
        # other thread), so send() needs no lock: it reads the slot once the C
        # call returns and, for a record still pending, creates _handover and
        # suspends on it; the resolve then sets both, on the loop.
        self.outcome: Any = _HANDOVER_PENDING
        # The future send() awaits, created only for a queued record.
        self._handover: asyncio.Future[None] | None = None
        # Whether the record completed, on the loop's thread or, once the loop
        # closed, on the helper pump's or close()'s.
        self._fired = False

    # -- the future ------------------------------------------------------------

    def is_done(self) -> bool:
        return self._state != _PENDING

    def cancel(self, *, may_interrupt_if_running: bool) -> bool:  # type: ignore[override]
        """Java's ``FutureRecordMetadata.cancel``: always ``False``; the record
        is not cancelled, nor is any future built from this one."""
        return False

    def then_apply(self, *, function: Callable[[RecordMetadata], R]  # type: ignore[override]
                   ) -> AsyncKafkaFuture[R]:
        """See :meth:`AsyncKafkaFuture.then_apply`."""
        return AsyncKafkaFuture._of_impl(_FutureState.then_apply(self, function), self._loop)

    def when_complete(self, *, action: Callable[[RecordMetadata | None, BaseException | None], None]  # type: ignore[override]
                      ) -> AsyncKafkaFuture[RecordMetadata]:
        """See :meth:`AsyncKafkaFuture.when_complete`."""
        return AsyncKafkaFuture._of_impl(_FutureState.when_complete(self, action), self._loop)

    # -- the completions -------------------------------------------------------

    def _complete(self, metadata: RecordMetadata, exception: Exception | None,
                  callback_id: int) -> BaseException | bool | None:
        # Once per record: the callback and the future are resolved on the
        # producer's loop (see _deliver), from the payload kept here.
        with self._lock:
            if self._fired:
                return None
            self._fired = True
        self._metadata = metadata
        self._exception = exception
        self._callback_id = callback_id
        return self._producer._deliver(self._loop, self)

    def _complete_future(self) -> None:
        """Complete the record's future, on its loop, once its callback has
        returned: its dependants and the inner ``asyncio.Future`` of a waiting
        ``get()`` are released there. Then untrack it (see
        _ProducerState._track)."""
        complete_record(self, self._metadata, self._exception)
        self._producer._discard_future(self)

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
        """The handover: the error the Rust send() returned, if any, which
        send() rethrows, as Java's doSend does; the record's Callback may still
        run, once, if the record had reached the accumulator (see
        Producer.send). Resolved on the loop: inline when the pump runs there,
        the common case, else through :func:`call_soon_on`; not at all once the
        loop has closed, which no send() awaits on any more."""
        loop = self._loop
        if loop.is_closed():
            return
        outcome: BaseException | None = (
            cast("Exception", from_ffi_error(error)) if error else None)
        if _running_loop() is loop:
            self._resolve_on_loop(outcome)
            return
        try:
            call_soon_on(loop, self._resolve_on_loop, outcome)
        except RuntimeError:  # the loop closed since the check
            pass

    def _resolve_on_loop(self, outcome: BaseException | None) -> None:
        """Record the handover, on the loop, and wake the send() awaiting it,
        if it suspended (a queued record)."""
        if self.outcome is not _HANDOVER_PENDING:
            return
        self.outcome = outcome
        waiter = self._handover
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def waiter(self) -> asyncio.Future[None]:
        """The future ``send()`` awaits for a queued record's handover, created
        here, on the loop, right before ``send()`` suspends on it."""
        waiter = self._handover = self._loop.create_future()
        return waiter


class AsyncProducer(Generic[K, V], _ProducerState):
    """The asyncio peer of ``Producer``, the interface for the
    ``AsyncKafkaProducer``: an async context manager whose exit flushes, then
    closes."""

    # By rule 3 (asyncio's cooperative scheduling): a send() the producer takes
    # at once never suspends, so a loop that only sends never runs what others
    # scheduled on it (the callback pump the notify hook queued, the hand-over
    # of a queued record, other tasks). After this many such sends in a row,
    # the next send() first gives the loop one iteration (`asyncio.sleep(0)`).
    _FAST_PATH_YIELD_EVERY = 1000

    # Between those yields, the completions of the earlier records are the
    # notify hook's to deliver, but its wake-up reaches the loop only when the
    # loop selects, which a run of sends taken at once never lets it do. So
    # every this many such sends in a row, send() runs the callback pump
    # itself on entry (the housekeeping of _enter); the sends in between only
    # count. The first send after a suspension pumps too, the count restarting
    # at 0 there. The callbacks then run in batches of at most this many
    # sends' worth, within the same millisecond at full rate, and a caller
    # that waits between sends gets each from the hook, as soon as the loop
    # wakes for it.
    _PUMP_EVERY = 64

    # How the native producer wakes the loop when its callback queue turns
    # non-empty: through a socket pair the loop watches (one byte written by
    # the C notify hook, no GIL taken on the Rust task's thread; see
    # _install_wake), or, when False or when the loop has no ``add_reader``,
    # through the notify callable (_schedule_pump), which takes the GIL there.
    _WAKE_BY_FD = True

    def __init__(self) -> None:
        if type(self) is AsyncProducer:
            raise TypeError("AsyncProducer is a non-instantiable base; use "
                            "AsyncKafkaProducer or AsyncMockProducer")
        _ProducerState.__init__(self)
        # send() calls in a row that the producer took at once, without
        # suspending (see _FAST_PATH_YIELD_EVERY).
        self._fast_path_sends = 0
        # Completions the callback pump buffered, per event loop, and resolved
        # on that loop in completion order, one callback at a time: a loop is
        # in _busy while a _complete_pending call is scheduled on it or an
        # async callback runs there (_awaiting), and later completions for it
        # wait in _pending meanwhile. _pending holds a loop only while it is
        # busy.
        self._pending: dict[asyncio.AbstractEventLoop, list[_AsyncRecord]] = {}
        self._busy: set[asyncio.AbstractEventLoop] = set()
        self._awaiting: dict[asyncio.AbstractEventLoop, _AsyncRecord] = {}
        self._callback_tasks: set[asyncio.Task[None]] = set()
        self._pending_lock = threading.Lock()
        # Keeps each record's completion to once and guards each record's
        # future state (see _AsyncRecord): one lock for the producer, not one
        # per record.
        self._fire_lock = threading.Lock()
        # The results of the other async operations on their way to their loop.
        self._undelivered = Undelivered()
        # The one event loop this producer runs on, which runs its callback
        # pump; once it has closed, a helper thread pumps instead.
        self._binding = LoopBinding(type(self).__name__)
        self._off_loop_pump = OffLoopPump(self._pump, "confluent-kafka-producer-pump")
        # The socket pair the loop watches for the native producer's wake-ups
        # (see _install_wake), set once the loop is bound; None before, once the
        # producer is destroyed, and when the loop cannot watch a socket.
        self._wake_r: socket.socket | None = None
        self._wake_w: socket.socket | None = None
        self._wake_tried = False

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
        """The notify hook as a callable (any thread, the GIL held), the
        wake-up before the loop is bound and once the socket pair is released
        (see :meth:`_install_wake`): schedule :meth:`_pump` on the producer's
        event loop, or, once that loop has closed, run it on the helper thread.
        Before the first async call there is no loop yet, and that call pumps
        on entry. It only schedules, never runs a callback (CLAUDE.md §4)."""
        loop = self._binding.loop
        if loop is None:
            return
        if not loop.is_closed():
            try:
                call_soon_on(loop, self._pump)
                return
            except RuntimeError:  # the loop closed since the check
                pass
        self._off_loop_pump.start()

    def _pump(self) -> None:
        """Run the producer's queued callbacks on this thread, once the native
        producer exists and until it is destroyed: on the producer's loop, or
        on the helper thread once that loop has closed (see
        :meth:`_schedule_pump`), which also drops what the loop left."""
        if not self._c_producer or not self._pump_use():
            return
        try:
            _lib.Producer__execute_callbacks(self._c_producer)
        finally:
            self._end_use()
        loop = self._binding.loop
        if loop is not None and loop.is_closed():
            self._undelivered.free_closed()
            self._drop_stranded()

    def _destroy(self) -> None:
        """Free the native producer (see :meth:`_ProducerState._destroy`), then
        the results left for an event loop that has closed: a result the pump
        handed to the loop before it closed waits here, not in the C API, and
        no later operation frees it once the producer is gone. The socket pair
        goes last: no hook fires once the native producer is gone."""
        super()._destroy()
        self._undelivered.free_closed()
        self._release_wake()

    def _enter(self, housekeeping: bool = True) -> asyncio.AbstractEventLoop:
        """The start of each async method: check the running loop is the
        producer's (binding it on the first call; another raises
        ``RuntimeError``), give the loop the socket pair to watch on that first
        call, and, with ``housekeeping``, run what is already queued and free
        what is left for a loop that has closed. Only :meth:`send` passes
        ``False``, for the sends between two of its entry pumps (see
        ``_PUMP_EVERY``)."""
        loop = self._binding.bind()
        if not self._wake_tried and self._c_producer and not self._closed:
            self._install_wake(loop)
        if housekeeping:
            self._pump()
            self._undelivered.free_closed()
        return loop

    def _install_wake(self, loop: asyncio.AbstractEventLoop) -> None:
        """Once, on the loop's thread, when the loop is bound: a socket pair
        whose read end the loop watches (``add_reader``) and whose write end
        the native producer gets. Its notify hook then writes one byte there
        from the Rust task's thread, without taking the GIL, and the loop runs
        :meth:`_on_wake` when it next selects; the callable hook
        (:meth:`_schedule_pump`) stays the fallback. A loop without
        ``add_reader`` (Windows' Proactor), or a platform without the socket
        wake, keeps the callable."""
        self._wake_tried = True
        if not self._WAKE_BY_FD:
            return
        try:
            reader, writer = socket.socketpair()
        except OSError:
            return
        reader.setblocking(False)
        writer.setblocking(False)
        try:
            loop.add_reader(reader.fileno(), self._on_wake, reader)
        except (NotImplementedError, RuntimeError, OSError):
            reader.close()
            writer.close()
            return
        self._wake_r, self._wake_w = reader, writer
        # Counted like a pump run, not a _use(): a close() begun meanwhile
        # must not make the first async call raise here; it clears the fd
        # again in _destroy.
        if not self._pump_use():
            self._release_wake()
            return
        try:
            _lib.Producer__set_wake_fd(self._c_producer, writer.fileno())
        finally:
            self._end_use()

    def _on_wake(self, reader: socket.socket) -> None:
        """The loop's reader callback for the socket pair: drop the bytes the
        hook wrote (many wake-ups, one read), then run the pump."""
        try:
            reader.recv(4096)
        except OSError:  # nothing to read, or the end closed meanwhile
            pass
        self._pump()

    def _release_wake(self) -> None:
        """Stop the wake-ups through the socket pair: the native producer falls
        back to the callable hook (unless it is destroyed already, after which
        no hook fires), then the loop stops watching the read end and both
        ends are closed, on the loop's thread while the loop still runs."""
        reader, writer = self._wake_r, self._wake_w
        if reader is None or writer is None:
            return
        self._wake_r = self._wake_w = None
        if self._pump_use():
            try:
                _lib.Producer__set_wake_fd(self._c_producer, -1)
            finally:
                self._end_use()
        loop = self._binding.loop

        def unregister(bound: asyncio.AbstractEventLoop) -> None:
            try:
                bound.remove_reader(reader.fileno())
            except (OSError, ValueError, RuntimeError):  # the loop closed meanwhile
                pass
            reader.close()
            writer.close()

        if loop is None or loop.is_closed():
            reader.close()
            writer.close()
        elif _running_loop() is loop:
            unregister(loop)
        else:
            try:
                call_soon_on(loop, unregister, loop)
            except RuntimeError:  # the loop closed since the check
                reader.close()
                writer.close()

    def _deliver(self, loop: asyncio.AbstractEventLoop,
                 item: _AsyncRecord) -> BaseException | bool | None:
        """Hand one record's completion to its ``loop``, the producer's
        (called by the pump). Run inline when the pump runs on ``loop`` with
        nothing before it there: ``None``, or the exception the callback raised, which
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
                call_soon_on(loop, self._complete_pending, loop)
            except RuntimeError:  # the loop closed since the check
                with self._pending_lock:
                    refused = self._pending.pop(loop, [])
                    self._busy.discard(loop)
                self._drop(refused)
        return True

    def _start_callback(self, loop: asyncio.AbstractEventLoop,
                        item: _AsyncRecord) -> tuple[bool, BaseException | None]:
        """Run ``item``'s callback on ``loop``'s thread: ``(True, None)`` when
        it is an ``async def``, whose task now finishes the completion (see
        :meth:`_await_callback`), else ``(False, the exception it raised)``,
        which the caller reports to the core (see ``_send``)."""
        callback = item._callback
        if callback is None:
            return False, None
        try:
            result = run_callback(self, callback, item._metadata, item._exception)
        except Exception as exc:  # noqa: BLE001 - reported to the core
            return False, exc
        if inspect.isawaitable(result):
            self._await_callback(loop, item, result)
            return True, None
        return False, None

    def _run_inline(self, loop: asyncio.AbstractEventLoop,
                    item: _AsyncRecord) -> BaseException | bool | None:
        """:meth:`_deliver` inline, inside the pump on ``loop``."""
        deferred, failure = self._start_callback(loop, item)
        if deferred:
            return True
        item._complete_future()
        return failure

    def _await_callback(self, loop: asyncio.AbstractEventLoop, item: _AsyncRecord,
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
            self._report(item._callback_id, failure)
            item._complete_future()
            self._complete_pending(loop)

        task.add_done_callback(done)

    def _report(self, callback_id: int, failure: BaseException | None) -> None:
        """Report a callback to the C API (``Producer__set_callback_result``),
        with the exception it raised, if any, which the core logs. Under the
        lifecycle lock and only while the handle is alive: :meth:`_destroy` sets
        ``_destroyed`` under it before it frees the handle, which completes what
        is left unreported."""
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
        await self._run_async(_lib.Producer_commit_transaction_cb, reports="EndTxn(true)")
        await _await_all(asyncio.get_running_loop(), self._completable(sent))

    async def abort_transaction(self) -> None:
        """See :meth:`Producer.abort_transaction`. From an ``async def``
        callback, which it would wait for, it raises ``TimeoutError`` after
        ``max.block.ms``, as Java's does from a callback."""
        self._check_not_closed()
        await self._run_async(_lib.Producer_abort_transaction_cb, reports="EndTxn(false)")

    async def send(self, *, record: ProducerRecord[K, V],
                   callback: _AsyncCallback | None = UNSET
                   ) -> AsyncKafkaFuture[RecordMetadata]:
        """See :meth:`Producer.send`. Awaiting it waits for the record to reach
        the producer, without blocking the loop, and raises what
        :meth:`Producer.send` raises; cancelling the awaiting task lets the wait
        end, then raises ``CancelledError``, the record staying sent. By rule 3,
        after 1000 sends in a row that the producer took at once, the next
        ``send()`` gives the event loop one iteration before touching its
        record, so the loop's other callbacks and tasks run even when the
        caller only sends; a task cancelled there has sent nothing. Between
        those yields every 64th such send runs the producer's queued callbacks
        on entry, so a caller that only sends gets the earlier records'
        callbacks in small batches, within the same millisecond at full rate,
        while a caller that waits between sends gets each callback as soon as
        the loop wakes for it. The returned
        ``AsyncKafkaFuture`` completes with the record's metadata, so a round
        trip is ``md = await (await p.send(record=r)).get()``; a failed record
        makes ``get()`` raise ``ExecutionError`` from the error. ``cancel()``
        returns ``False`` and cancels nothing, as Java's ``FutureRecordMetadata``;
        the future's ``then_apply`` functions and ``when_complete`` actions run
        on the event loop, once the record's callback has returned. The
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
        # Before the serializers, so a call from another loop has no effect.
        # The pump runs on every _PUMP_EVERY-th send taken at once in a row
        # and on the first after a suspension; the others only count.
        loop = self._enter(self._fast_path_sends % self._PUMP_EVERY == 0)
        if self._fast_path_sends >= self._FAST_PATH_YIELD_EVERY:
            # Before the record is touched: a task cancelled here has sent
            # nothing (see _FAST_PATH_YIELD_EVERY).
            self._fast_path_sends = 0
            await asyncio.sleep(0)
        native, topic, partition = self._native_parts(record)
        # The record: the AsyncKafkaFuture returned (AsyncRecordFuture: cancel()
        # answers False, as Java's FutureRecordMetadata), its own state, which
        # the completion completes on the loop, and the send's completions,
        # one object (see _AsyncRecord).
        sent = _AsyncRecord(self, callback, topic, partition, loop)
        # A close() that began since the check above (the serializers ran in
        # between) refuses the record as Java's RecordAccumulator.append does.
        with self._send_use as c_producer:
            # The record is tried on the loop's thread first and queued to the
            # submission task only when the producer would have to wait for
            # it: sent.resolve runs inside the call for a record taken or
            # refused at once (sent.outcome is then set before it is read),
            # from the pump otherwise.
            raise_if_error(_lib.Producer_send_cb(c_producer, native, sent, sent.resolve))
        outcome = sent.outcome
        # Taken or refused at once: one more send without a suspension. Queued:
        # the await below suspends, so the loop gets its turn anyway.
        self._fast_path_sends = 0 if outcome is _HANDOVER_PENDING else self._fast_path_sends + 1
        try:
            if outcome is _HANDOVER_PENDING:
                # Queued: wait for the handover, on the loop.
                await await_to_end(sent.waiter())
                outcome = sent.outcome
            if outcome is not None:
                raise outcome
            return sent
        finally:
            # Tracked only once handed over without an error (see
            # Producer.send). Otherwise send() does not return the future, and
            # a later completion of the record runs its callback and completes
            # a future nobody holds. Not handed over yet when the coroutine is
            # closed with its loop (a task the garbage collector finds on a
            # closed loop): the record is still sent, and its completion,
            # dropped for the closed loop, untracks it.
            outcome = sent.outcome
            if outcome is None or outcome is _HANDOVER_PENDING:
                self._track(sent)

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
            self._report(item._callback_id, failure)
            item._complete_future()

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
        await _await_all(asyncio.get_running_loop(), self._completable(sent))

    def _completable(self, sent: list[_FutureState[RecordMetadata]]
                     ) -> list[_FutureState[RecordMetadata]]:
        """The futures of ``sent`` a wait can still see complete. Those of a
        producer whose event loop has closed never complete: they are untracked
        instead of waited for, and their completions are dropped with
        :func:`_drop_completion`'s warning, when they arrive (see ``send``) or,
        if one arrived while the loop was open but was left for the loop to
        run, here (and in ``close()``)."""
        self._drop_stranded()
        loop = self._binding.loop
        if loop is not None and loop.is_closed():
            for future in sent:
                self._futures.discard(future)
            return []
        return sent

    def _drop_stranded(self) -> None:
        """Drop the completions left for event loops that have closed: the
        scheduled call that would resolve them on the loop never runs, nor does
        the end of an ``async def`` callback the loop was awaiting."""
        with self._pending_lock:
            closed_loops = [loop for loop in self._busy if loop.is_closed()]
            stranded: list[_AsyncRecord] = []
            for loop in closed_loops:
                self._busy.discard(loop)
                awaited = self._awaiting.pop(loop, None)
                if awaited is not None:
                    stranded.append(awaited)
                stranded.extend(self._pending.pop(loop, []))
        self._drop(stranded)

    def _drop(self, items: list[_AsyncRecord]) -> None:
        """Drop completions whose event loop has closed, untracking their
        futures (see :func:`_drop_completion`), and report their callbacks, so
        the C API's waits for the reports end."""
        for item in items:
            _drop_completion(item._metadata)
            self._futures.discard(item)
            self._report(item._callback_id, None)

    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """See :meth:`Producer.partitions_for`."""
        self._check_not_closed()
        if topic is None:
            raise TypeError("topic must not be None")
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
        # Another loop's close() raises before it marks the producer closing.
        self._binding.bind()
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
        callback pump and hops onto the loop via ``call_soon_on``; a
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
        try:
            if reports is None:
                return await fut
            if self._in_own_callback():
                try:
                    return await asyncio.wait_for(fut, self._max_block_ms / 1000.0)
                except asyncio.TimeoutError:
                    raise KafkaTimeoutError(message=_ASYNC_CALLBACK_WAIT_TIMEOUT_MESSAGE.format(
                        ms=self._max_block_ms, operation=reports)) from None
            while not fut.done():
                await asyncio.wait((fut,), timeout=_STRANDED_POLL_S)
                self._drop_stranded()
            return fut.result()
        except BaseException:
            # A wait that ends without taking the payload: a cancelled task, or
            # one left on a closed loop, whose coroutine is closed when the task
            # goes away. A payload delivered already is freed now; one still
            # on its way is freed when it arrives: by deliver, or, the loop
            # closed, by its hand-over, so the future of a closed loop, whose
            # cancel would schedule on that loop, is left alone.
            if fut.done():
                if not fut.cancelled():
                    free(fut.result())
            elif not loop.is_closed():
                fut.cancel()
            raise

    async def _run_async(self, entry: Callable[..., None], *args: Any,
                         reports: str | None = None) -> None:
        """A void queued FFI operation, awaited; raises its typed error. See
        :meth:`_await_payload` for ``reports``."""
        (error,) = await self._await_payload(entry, *args, reports=reports)
        raise_if_error(error)
