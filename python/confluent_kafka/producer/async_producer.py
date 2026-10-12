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
``send_offsets_to_transaction``, ``commit_transaction``, ``abort_transaction``,
``send``, ``flush``, ``partitions_for`` and ``close``. ``metrics`` has none, so
it is a plain ``def``. ``send`` returns an ``asyncio.Future``, so a round trip
is ``md = await (await p.send(record=r))``.

*(deviation)* ``begin_transaction`` has no ``_cb`` form either, but it is an
``async def`` too, as in ``Producer`` it waits: like every transaction-control
operation it first drains the records already sent (producer-transactions.md
§13), so its blocking entry point runs in the loop's default executor.

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

*(deviation)* Backpressure is the core's ``max.block.ms`` wait on the
submission task, not a wait in ``send()`` (see ``_base``).
"""

from __future__ import annotations

import asyncio
import logging
import threading
import weakref
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import UNSET
from confluent_kafka._async import Undelivered, await_to_end
from confluent_kafka._errors import from_ffi_error
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.null_pointer_error import NullPointerError

from ._base import (
    CLOSED_WHILE_SENDING_MESSAGE,
    _ProducerState,
    check_group_metadata,
    close_timeout_ms,
    free_error,
    free_error_payload,
    group_metadata_handle,
    offsets_to_spec,
    raise_if_error,
    to_metrics_map,
    to_partition_info,
)
from ._send import MetadataTuple, completion_to_python, failure_metadata, invoke_callback
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import MetricName, PartitionInfo, TopicPartition
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from .callback import Callback
    from .producer_record import ProducerRecord

__all__ = ["AsyncProducer"]

K = TypeVar("K")
V = TypeVar("V")

_LOG = logging.getLogger("confluent_kafka.producer")

# A completion the callback pump buffered for the event loop:
# (future, callback, metadata, exception).
_Pending = tuple["asyncio.Future[RecordMetadata]", "Callback | None", RecordMetadata,
                 "Exception | None"]


def _set_result(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


def _drop_completion(metadata: RecordMetadata) -> None:
    """A completion whose event loop has closed: no loop can run its callback
    (it runs on the event loop) or complete its future any more, so it is
    dropped and one warning is logged."""
    _LOG.warning("The completion of a record sent to topic-partition '%s' was dropped: its "
                 "event loop had closed, so its callback did not run and its future does not "
                 "complete", f"{metadata.topic()}-{metadata.partition()}")


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
        # on that loop in one scheduled call.
        self._pending: dict[asyncio.AbstractEventLoop, list[_Pending]] = {}
        self._pending_lock = threading.Lock()
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

    def _deliver(self, loop: asyncio.AbstractEventLoop, item: _Pending) -> None:
        """Hand one record's completion to its ``loop`` (called by the pump on
        any thread): buffered, then resolved on the loop in one scheduled call
        per batch; dropped with :func:`_drop_completion`'s warning when the
        loop has closed."""
        if loop.is_closed():
            # The completion is dropped, then the future untracked, so a later
            # flush() does not wait for it (see _completable).
            self._drop([item])
            return
        with self._pending_lock:
            pending = self._pending.get(loop)
            schedule = pending is None
            if pending is None:
                pending = self._pending[loop] = []
            pending.append(item)
        if schedule:
            try:
                loop.call_soon_threadsafe(self._complete_pending, loop)
            except RuntimeError:  # the loop closed since the check
                with self._pending_lock:
                    refused = self._pending.pop(loop, [])
                self._drop(refused)

    async def init_transactions(self) -> None:
        """See :meth:`Producer.init_transactions`."""
        self._check_not_closed()
        await self._run_async(_lib.Producer_init_transactions_cb)

    async def begin_transaction(self) -> None:
        """See :meth:`Producer.begin_transaction`. Its entry point has no
        queued form, so it runs in the loop's default executor, as one use of
        the handle, and is awaited without blocking the loop (see the module
        docstring); cancelling the awaiting task lets it end, then raises
        ``CancelledError``."""
        self._check_not_closed()
        loop = self._enter()
        use = self._use()
        c_producer = use.__enter__()

        def begin() -> Exception | None:
            try:
                error = _lib.Producer_begin_transaction(c_producer)
            finally:
                use.__exit__(None, None, None)
            if error:
                return from_ffi_error(error)
            self._in_transaction = True
            return None

        try:
            began = loop.run_in_executor(None, begin)
        except BaseException:
            use.__exit__(None, None, None)
            raise
        exception = await await_to_end(began)
        if exception is not None:
            raise exception

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
        callbacks of the transaction's records have run, but for those of a
        record whose event loop has closed, which are dropped (see
        :meth:`send`)."""
        self._check_not_closed()
        sent = list(self._futures)
        # Sends from now on wait for their handover again (see _ProducerState).
        self._in_transaction = False
        await self._run_async(_lib.Producer_commit_transaction_cb)
        sent = self._completable(sent)
        if sent:
            await asyncio.wait(sent)

    async def abort_transaction(self) -> None:
        """See :meth:`Producer.abort_transaction`."""
        self._check_not_closed()
        # Sends from now on wait for their handover again (see _ProducerState).
        self._in_transaction = False
        await self._run_async(_lib.Producer_abort_transaction_cb)

    async def send(self, *, record: ProducerRecord[K, V],
                   callback: Callback | None = UNSET) -> asyncio.Future[RecordMetadata]:
        """See :meth:`Producer.send`. With a ``transactional.id`` and no
        transaction started, awaiting it waits for the record to reach the
        producer, without blocking the loop, and raises what
        :meth:`Producer.send` raises; cancelling the awaiting task then lets the
        wait end, then raises ``CancelledError``. The returned
        ``asyncio.Future`` resolves with the record's metadata. The
        ``callback`` runs on the event loop, before the future completes, and
        must not block it. A record that completes after its event loop has
        closed is dropped with a warning on the ``confluent_kafka.producer``
        logger: its callback does not run and its future does not complete."""
        if callback is UNSET:
            callback = None  # Java's send(record) passes null
        self._check_not_closed()
        native = self._native_record(record)
        topic = record.topic()
        partition = record.partition()
        loop = self._enter()
        future: asyncio.Future[RecordMetadata] = loop.create_future()

        # See Producer.send: with no transaction started, a transactional
        # producer's send awaits the handover and raises the error the Rust
        # send() returns.
        waiting = self._transactional and not self._in_transaction
        handed_over: asyncio.Future[None] = loop.create_future()
        lock = threading.Lock()
        completed = False
        rethrown: Exception | None = None

        def complete(metadata: RecordMetadata, exception: Exception | None) -> None:
            # Once per record, from the pump (any thread): the callback and the
            # future are resolved on the record's loop.
            nonlocal completed
            with lock:
                if completed:
                    return
                completed = True
            self._deliver(loop, (future, callback, metadata, exception))

        def on_completion(metadata_tuple: MetadataTuple | None, error: int) -> None:
            # The error handle is borrowed: converted during the call.
            complete(*completion_to_python(metadata_tuple, error, topic, partition))

        def resolve(error: int) -> None:
            # The handover: the error the Rust send() returned, after which the
            # record's Callback never runs.
            nonlocal completed, rethrown
            if error:
                exception = from_ffi_error(error)
                if waiting:
                    with lock:
                        completed = True
                    rethrown = exception
                    # The record never reached the producer (Java's doSend
                    # rethrows the error), so flush() has nothing to wait for.
                    self._futures.discard(future)
                else:
                    complete(failure_metadata(topic, partition), exception)
            if waiting and not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(_set_result, handed_over)
                except RuntimeError:  # the loop closed since the check
                    pass

        # A close() that began since the check above (the serializers ran in
        # between) refuses the record as Java's RecordAccumulator.append does.
        with self._use(KafkaError(message=CLOSED_WHILE_SENDING_MESSAGE)) as c_producer:
            raise_if_error(_lib.Producer_send_cb(c_producer, native, on_completion, resolve))
            self._track(future)
        if waiting:
            await await_to_end(handed_over)
            if rethrown is not None:
                raise rethrown
        return future

    def _complete_pending(self, loop: asyncio.AbstractEventLoop) -> None:
        """Resolve the completions buffered for ``loop``, on ``loop``, in
        completion order: the callback runs, then the future completes."""
        with self._pending_lock:
            items = self._pending.pop(loop, [])
        for future, callback, metadata, exception in items:
            invoke_callback(self, callback, metadata, exception)
            if future.done():
                continue  # cancelled by its awaiter
            if exception is not None:
                future.set_exception(exception)
            else:
                future.set_result(metadata)

    async def flush(self) -> None:
        """See :meth:`Producer.flush`: when it returns, the callbacks of the
        records sent before it have run, but for those of a record whose event
        loop has closed, which are dropped (see :meth:`send`)."""
        self._check_not_closed()
        sent = list(self._futures)
        # The C API drains the submission queue first (see Producer.flush).
        await self._run_async(_lib.Producer_flush_cb)
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
        scheduled call that would resolve them on the loop never runs."""
        with self._pending_lock:
            closed_loops = [loop for loop in self._pending if loop.is_closed()]
            stranded = [item for loop in closed_loops for item in self._pending.pop(loop)]
        self._drop(stranded)

    def _drop(self, items: list[_Pending]) -> None:
        """Drop completions whose event loop has closed, untracking their
        futures (see :func:`_drop_completion`)."""
        for future, _callback, metadata, _exception in items:
            _drop_completion(metadata)
            self._futures.discard(future)

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
        """See :meth:`Producer.close`."""
        if self._closed:
            return
        timeout_ms = close_timeout_ms(timeout)
        # Check and set at once: exactly one close() tears the producer down.
        if not self._begin_close():
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
            # sent before is included.
            if timeout_ms is None:
                (error,) = await self._await_payload(_lib.Producer_close_cb, owner=True)
            else:
                (error,) = await self._await_payload(
                    _lib.Producer_close_with_timeout_cb, timeout_ms, owner=True)
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

    def _teardown(self, c_producer: int, force: bool) -> None:
        if force:
            # Java's sender.forceClose() for a close() the cancellation stopped
            # (KafkaProducer.close(Duration, boolean), KafkaProducer.java:1419-1437):
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
                             owner: bool = False) -> tuple[Any, ...]:
        """Submit ``entry(handle, *args, cb)`` (a queued ``_cb`` entry point) and
        await its completion payload on the loop. The completion runs in the
        callback pump and hops onto the loop via ``call_soon_threadsafe``; a
        payload nobody awaits any more (a cancelled task, a closed loop) has its
        error handle freed (see :class:`~confluent_kafka._async.Undelivered`).

        The submission is one use of the handle, which raises Java's closed
        ``IllegalStateError`` once a ``close()`` has begun; only ``close()``,
        which owns the handle, passes ``owner``."""
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
        return await fut

    async def _run_async(self, entry: Callable[..., None], *args: Any) -> None:
        """A void queued FFI operation, awaited; raises its typed error."""
        (error,) = await self._await_payload(entry, *args)
        raise_if_error(error)
