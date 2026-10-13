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
an ``_async`` completion form in the FFI header: ``init_transactions``,
``begin_transaction``, ``send_offsets_to_transaction``, ``commit_transaction``,
``abort_transaction``, ``send``, ``flush``, ``partitions_for`` and ``close``.
``metrics`` has none, so it is a plain ``def``. ``send`` returns an
``asyncio.Future``, so a round trip is ``md = await (await p.send(record=r))``.

Every waiting call awaits an ``asyncio.Future`` completed through
``loop.call_soon_threadsafe``; a ``send()`` callback runs on the event loop
(Implementation over the FFI, Threads and callbacks). A ``send()`` whose record
the producer takes at once does not wait, so after ``_YIELD_EVERY`` such sends
in a row while completions are pending it yields the loop once, letting their
callbacks run (rule 3: Java has no event loop to share).
"""

from __future__ import annotations

import asyncio
import logging
import threading
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
    force_close,
    free_payload,
    group_metadata_handle,
    offsets_to_spec,
    raise_if_error,
    to_metrics_map,
    to_partition_info,
)
from ._send import completion_to_python, invoke_callback
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

# A completion the producer's callback thread buffered for the event loop:
# (future, callback, topic, partition, metadata handle, error handle).
_Pending = tuple["asyncio.Future[RecordMetadata]", "Callback | None", str, "int | None", int, int]


def _drop_completion(topic: str, partition: int | None, result: int, error: int) -> None:
    """A completion whose event loop has closed: no loop can run its callback
    (it runs on the event loop) or complete its future any more, so its
    handles are freed and one warning is logged."""
    metadata, _ = completion_to_python(result, error, topic, partition)
    _LOG.warning("The completion of a record sent to topic-partition '%s' was dropped: its "
                 "event loop had closed, so its callback did not run and its future does not "
                 "complete", f"{metadata.topic()}-{metadata.partition()}")


class AsyncProducer(Generic[K, V], _ProducerState):
    """The asyncio peer of ``Producer``, the interface for the
    ``AsyncKafkaProducer``: an async context manager whose exit flushes, then
    closes."""

    # After this many sends in a row that the producer took at once, send()
    # yields the loop once if completions are pending (see the module).
    _YIELD_EVERY = 1000

    def __init__(self) -> None:
        if type(self) is AsyncProducer:
            raise TypeError("AsyncProducer is a non-instantiable base; use "
                            "AsyncKafkaProducer or AsyncMockProducer")
        _ProducerState.__init__(self)
        # Completions the producer's callback thread buffered, per event loop,
        # and resolved on that loop in one scheduled call.
        self._pending: dict[asyncio.AbstractEventLoop, list[_Pending]] = {}
        self._pending_lock = threading.Lock()
        # Sends in a row the producer took at once (see _YIELD_EVERY).
        self._sends_since_yield = 0
        # The results of the other async operations on their way to their loop.
        self._undelivered = Undelivered()

    async def init_transactions(self) -> None:
        """See :meth:`Producer.init_transactions`."""
        self._check_not_closed()
        await self._run_async(
            lambda cb: self._call(_lib.Producer_init_transactions_async, cb))

    async def begin_transaction(self) -> None:
        """See :meth:`Producer.begin_transaction`. Its entry point has an
        ``_async`` form, so it is an ``async def`` awaiting it."""
        self._check_not_closed()
        await self._run_async(
            lambda cb: self._call(_lib.Producer_begin_transaction_async, cb))

    async def send_offsets_to_transaction(
            self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        """See :meth:`Producer.send_offsets_to_transaction`."""
        self._check_not_closed()
        check_group_metadata(group_metadata)
        native = group_metadata_handle(group_metadata)
        spec = offsets_to_spec(offsets)
        await self._run_async(lambda cb: self._call(
            _lib.Producer_send_offsets_to_transaction_async, spec, native, cb))

    async def commit_transaction(self) -> None:
        """See :meth:`Producer.commit_transaction`: when it returns, the
        callbacks of the transaction's records have run, but for those of a
        record whose event loop has closed, which are dropped (see
        :meth:`send`)."""
        self._check_not_closed()
        sent = list(self._futures)
        await self._run_async(
            lambda cb: self._call(_lib.Producer_commit_transaction_async, cb))
        sent = self._completable(sent)
        if sent:
            await asyncio.wait(sent)

    async def abort_transaction(self) -> None:
        """See :meth:`Producer.abort_transaction`."""
        self._check_not_closed()
        await self._run_async(
            lambda cb: self._call(_lib.Producer_abort_transaction_async, cb))

    async def send(self, *, record: ProducerRecord[K, V],
                   callback: Callback | None = UNSET) -> asyncio.Future[RecordMetadata]:
        """See :meth:`Producer.send`. Where Java's ``send()`` blocks (the
        topic's metadata up to ``max.block.ms``, buffer space), awaiting it
        waits without blocking the loop, when the producer could not take the
        record at once; it raises what :meth:`Producer.send` raises. The
        returned ``asyncio.Future`` resolves with the record's metadata. The
        ``callback`` runs on the event loop, before the future completes, and
        must not block it. A record that completes after its event loop has
        closed is dropped with a warning on the ``confluent_kafka.producer``
        logger: its callback does not run and its future does not complete."""
        if callback is UNSET:
            callback = None  # Java's send(record) passes null
        if self._sends_since_yield >= self._YIELD_EVERY and self._pending:
            # Sends the producer takes at once never wait, so the completions
            # the callback thread buffered would wait for the caller's next
            # await: give the loop one turn (a cancellation here hands nothing
            # over).
            self._sends_since_yield = 0
            await asyncio.sleep(0)
        self._check_not_closed()
        native = self._native_record(record)
        topic = record.topic()
        partition = record.partition()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[RecordMetadata] = loop.create_future()
        # The producer's answer for a record it could not take at once (see
        # Producer.send), awaited on the loop.
        waiter: asyncio.Future[int] | None = None

        def settle(error: int) -> None:
            # On the loop. The awaiter may have been cancelled while the record
            # was queued: then the answer is handled as Producer.send's cb does.
            if waiter is None or waiter.done():
                if error:
                    _lib.KafkaError_destroy(error)
                else:
                    self._track(future)
                return
            waiter.set_result(error)

        def cb(result: int | None, error: int) -> None:
            if result is None:
                # The producer's answer, from its dispatcher thread: handed to
                # the loop, which the send awaits on.
                if not loop.is_closed():
                    try:
                        loop.call_soon_threadsafe(settle, error)
                    except RuntimeError:  # the loop closed since the check
                        pass
                    else:
                        return
                if error:
                    _lib.KafkaError_destroy(error)
                return
            # A completion: runs on the producer's callback thread. asyncio
            # futures may only be completed on their loop: buffer, and schedule
            # one drain per batch.
            if loop.is_closed():
                # The completion is dropped, then the future untracked, so a
                # later flush() does not wait for it (see _completable).
                _drop_completion(topic, partition, result, error)
                self._futures.discard(future)
                return
            with self._pending_lock:
                pending = self._pending.get(loop)
                schedule = pending is None
                if pending is None:
                    pending = self._pending[loop] = []
                pending.append((future, callback, topic, partition, result, error))
            if schedule:
                try:
                    loop.call_soon_threadsafe(self._complete_pending, loop)
                except RuntimeError:  # the loop closed since the check
                    with self._pending_lock:
                        refused = self._pending.pop(loop, [])
                    self._drop(refused)

        # A close() that began since the check above (the serializers ran in
        # between) refuses the record as Java's RecordAccumulator.append does.
        closed_while_sending = KafkaError(message=CLOSED_WHILE_SENDING_MESSAGE)
        with self._use(closed_while_sending) as c_producer:
            taken = _lib.Producer_send(c_producer, native, cb)
        if taken is True:
            self._sends_since_yield += 1
            self._track(future)
            return future
        if taken is False:
            raise closed_while_sending
        if taken is None:
            # Queued: await the producer's answer (see Producer.send). A
            # cancellation here leaves the answer to `settle`.
            self._sends_since_yield = 0
            waiter = loop.create_future()
            taken = await waiter
            if not taken:
                self._track(future)
                return future
        raise from_ffi_error(taken)

    def _complete_pending(self, loop: asyncio.AbstractEventLoop) -> None:
        """Resolve the completions buffered for ``loop``, on ``loop``, in
        completion order: the callback runs, then the future completes."""
        with self._pending_lock:
            items = self._pending.pop(loop, [])
        for future, callback, topic, partition, result, error in items:
            metadata, exception = completion_to_python(result, error, topic, partition)
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
        await self._run_async(lambda cb: self._call(_lib.Producer_flush_async, cb))
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
        for future, _callback, topic, partition, result, error in items:
            _drop_completion(topic, partition, result, error)
            self._futures.discard(future)

    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """See :meth:`Producer.partitions_for`."""
        self._check_not_closed()
        if topic is None:
            raise NullPointerError(message="topic cannot be null")
        list_handle, error = await self._await_payload(
            lambda cb: self._call(_lib.Producer_partitions_for_async, topic, cb), True)
        if error:
            if list_handle:
                _lib.PartitionInfoList_drain(list_handle)
            raise_if_error(error)
        return [to_partition_info(t) for t in _lib.PartitionInfoList_drain(list_handle)]

    def metrics(self) -> dict[MetricName, Metric]:
        """See :meth:`Producer.metrics`. Its entry point has no ``_async``
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
        loop = asyncio.get_running_loop()
        # Refuse further records; the Rust close first hands the records still
        # queued to the producer (producer-transactions.md §13), then waits for
        # the records handed over, within the close timeout.
        _lib.Producer_shutdown(c_producer)
        # Whether the Rust close ran to its end; if the task was cancelled
        # first, the close is forced before the handle is freed (see
        # force_close).
        closed = False
        try:
            if timeout_ms is None:
                (error,) = await self._await_payload(
                    lambda cb: _lib.Producer_close_async(c_producer, cb), False)
            else:
                close_ms = timeout_ms
                (error,) = await self._await_payload(
                    lambda cb: _lib.Producer_close_with_timeout_async(
                        c_producer, close_ms, cb), False)
            closed = True
            raise_if_error(error)
        finally:
            # Forces an unfinished close, waits for the calls still in flight,
            # then joins the callback thread: off the loop, and to its end even
            # if this task is cancelled again meanwhile, so the serializers
            # close after it.
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
            force_close(c_producer)
        self._wait_for_uses()
        _lib.Producer_destroy(c_producer)

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

    async def _await_payload(self, submit: Callable[[Callable[..., None]], None],
                             partitions: bool) -> tuple[Any, ...]:
        """Submit an ``_async`` FFI operation and await its completion payload
        on the loop. The completion runs on the producer's dispatcher thread and
        hops onto the loop via ``call_soon_threadsafe``; a payload nobody awaits
        any more (a cancelled task, a closed loop) has its handles freed (see
        :class:`~confluent_kafka._async.Undelivered`)."""
        self._undelivered.free_closed()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[Any, ...]] = loop.create_future()

        def free(payload: tuple[Any, ...]) -> None:
            free_payload(payload, partitions)

        def deliver(payload: tuple[Any, ...]) -> None:
            if fut.done():
                free(payload)
                return
            fut.set_result(payload)

        def cb(*payload: Any) -> None:
            self._undelivered.hand_over(loop, deliver, payload, free)

        submit(cb)
        return await fut

    async def _run_async(self, submit: Callable[[Callable[..., None]], None]) -> None:
        """A void ``_async`` FFI operation, awaited; raises its typed error."""
        (error,) = await self._await_payload(submit, False)
        raise_if_error(error)
