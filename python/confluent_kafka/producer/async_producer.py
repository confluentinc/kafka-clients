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
Conventions, Class family), each ``async def`` iff Java waits in it:
``init_transactions``, ``send_offsets_to_transaction``, ``commit_transaction``,
``abort_transaction``, ``send`` (Java blocks on metadata and buffer space),
``flush``, ``partitions_for`` and ``close``. ``begin_transaction`` and
``metrics`` do not wait, so they are plain ``def``. ``send`` returns an
``asyncio.Future``, so a round trip is ``md = await (await p.send(record=r))``.

Every waiting call awaits an ``asyncio.Future`` completed through
``loop.call_soon_threadsafe``; a ``send()`` callback runs on the event loop
(Implementation over the FFI, Threads and callbacks).
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._async import await_to_end
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

# A completion the C poll thread buffered for the event loop:
# (future, callback, topic, partition, metadata handle, error handle).
_Pending = tuple["asyncio.Future[RecordMetadata]", "Callback | None", str, "int | None", int, int]


def _set_result(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


class AsyncProducer(Generic[K, V], _ProducerState):
    """The asyncio peer of ``Producer``, the interface for the
    ``AsyncKafkaProducer``: an async context manager whose exit flushes, then
    closes."""

    def __init__(self) -> None:
        if type(self) is AsyncProducer:
            raise TypeError("AsyncProducer is a non-instantiable base; use "
                            "AsyncKafkaProducer or AsyncMockProducer")
        _ProducerState.__init__(self)
        # Completions the C poll thread buffered, per event loop, and resolved
        # on that loop in one scheduled call.
        self._pending: dict[asyncio.AbstractEventLoop, list[_Pending]] = {}
        self._pending_lock = threading.Lock()

    async def init_transactions(self) -> None:
        """See :meth:`Producer.init_transactions`."""
        self._check_transaction_manager()
        self._check_not_closed()
        await self._drain_async()
        await self._run_async(
            lambda cb: self._call(_lib.Producer_init_transactions_async, cb))

    def begin_transaction(self) -> None:
        """See :meth:`Producer.begin_transaction`. Java does not wait in it, so
        it is a plain ``def``; it does not wait for earlier sends either, so it
        never blocks the loop (see :meth:`Producer.begin_transaction` for why it
        needs no drain)."""
        self._check_transaction_manager()
        self._check_not_closed()
        raise_if_error(self._call(_lib.Producer_begin_transaction))
        self._in_transaction = True

    async def send_offsets_to_transaction(
            self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        """See :meth:`Producer.send_offsets_to_transaction`."""
        check_group_metadata(group_metadata)
        self._check_transaction_manager()
        self._check_not_closed()
        native = group_metadata_handle(group_metadata)
        await self._drain_async()
        spec = offsets_to_spec(offsets)
        await self._run_async(lambda cb: self._call(
            _lib.Producer_send_offsets_to_transaction_async, spec, native, cb))

    async def commit_transaction(self) -> None:
        """See :meth:`Producer.commit_transaction`: when it returns, the
        callbacks of the transaction's records have run."""
        self._check_transaction_manager()
        self._check_not_closed()
        sent = list(self._futures)
        # Sends from now on wait for their handover again (see _ProducerState).
        self._in_transaction = False
        await self._drain_async()
        await self._run_async(
            lambda cb: self._call(_lib.Producer_commit_transaction_async, cb))
        if sent:
            await asyncio.wait(sent)

    async def abort_transaction(self) -> None:
        """See :meth:`Producer.abort_transaction`."""
        self._check_transaction_manager()
        self._check_not_closed()
        # Sends from now on wait for their handover again (see _ProducerState).
        self._in_transaction = False
        await self._drain_async()
        await self._run_async(
            lambda cb: self._call(_lib.Producer_abort_transaction_async, cb))

    async def send(self, *, record: ProducerRecord[K, V],
                   callback: Callback | None = None) -> asyncio.Future[RecordMetadata]:
        """See :meth:`Producer.send`. Awaiting it waits for buffer space (Java's
        ``send()`` blocks on ``buffer.memory``) and, with a ``transactional.id``
        and no transaction started, for the record to reach the producer,
        without blocking the loop; it raises what :meth:`Producer.send` raises. The returned
        ``asyncio.Future`` resolves with the record's metadata. The
        ``callback`` runs on the event loop, before the future completes, and
        must not block it."""
        self._check_not_closed()
        native = self._native_record(record)
        topic = record.topic()
        partition = record.partition()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[RecordMetadata] = loop.create_future()

        # See Producer.send: with no transaction started, a transactional
        # producer's send awaits the handover and raises the error the Rust
        # send() returns.
        waiting = self._transactional and not self._in_transaction
        rethrown: Exception | None = None
        lock = threading.Lock()

        def cb(result: int, error: int, immediate: bool) -> None:
            # Runs on the C completion thread. asyncio futures may only be
            # completed on their loop: buffer, and schedule one drain per batch.
            nonlocal rethrown
            if immediate and waiting:
                with lock:
                    if waiting:
                        rethrown = completion_to_python(result, error, topic, partition)[1]
                        return
            if loop.is_closed():
                metadata, exception = completion_to_python(result, error, topic, partition)
                invoke_callback(self, callback, metadata, exception)
                return
            with self._pending_lock:
                pending = self._pending.get(loop)
                schedule = pending is None
                if pending is None:
                    pending = self._pending[loop] = []
                pending.append((future, callback, topic, partition, result, error))
            if schedule:
                loop.call_soon_threadsafe(self._complete_pending, loop)

        # A close() that began since the check above (the serializers ran in
        # between) refuses the record as Java's RecordAccumulator.append does.
        closed_while_sending = KafkaError(message=CLOSED_WHILE_SENDING_MESSAGE)
        space: asyncio.Future[None] | None = None
        handed_over: asyncio.Future[None] | None = None
        with self._use(closed_while_sending) as c_producer:
            full = _lib.Producer_send(c_producer, native, cb)
            if full is None:
                raise closed_while_sending
            self._track(future)
            if waiting:
                handed_over = loop.create_future()
                handover = handed_over

                def ready() -> None:
                    if not loop.is_closed():
                        loop.call_soon_threadsafe(_set_result, handover)

                if _lib.Producer_drain(c_producer, ready):
                    handed_over = None
            if full:
                waiter: asyncio.Future[None] = loop.create_future()

                def space_cb() -> None:
                    if not loop.is_closed():
                        loop.call_soon_threadsafe(_set_result, waiter)

                if not _lib.Producer_on_space_available(c_producer, space_cb):
                    space = waiter
        # Both waits clear `waiting` when they end, a cancellation included:
        # left set, it would keep taking the immediate error from the callback
        # and the future, and the tracked future would hang a later flush().
        try:
            if space is not None:
                await space
            if handed_over is not None:
                await handed_over
        finally:
            if waiting:
                with lock:
                    waiting = False
                if rethrown is not None:
                    # The record never reached the producer (Java's doSend
                    # rethrows the error), so flush() has nothing to wait for.
                    self._futures.discard(future)
        if rethrown is not None:
            raise rethrown
        return future

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
        records sent before it have run."""
        self._check_not_closed()
        sent = list(self._futures)
        await self._drain_async()
        await self._run_async(lambda cb: self._call(_lib.Producer_flush_async, cb))
        if sent:
            await asyncio.wait(sent)

    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """See :meth:`Producer.partitions_for`."""
        if topic is None:
            raise NullPointerError(message="topic cannot be null")
        self._check_not_closed()
        list_handle, error = await self._await_payload(
            lambda cb: self._call(_lib.Producer_partitions_for_async, topic, cb), True)
        if error:
            if list_handle:
                _lib.PartitionInfoList_drain(list_handle)
            raise_if_error(error)
        return [to_partition_info(t) for t in _lib.PartitionInfoList_drain(list_handle)]

    def metrics(self) -> dict[MetricName, Metric]:
        """See :meth:`Producer.metrics`. Java does not wait in it, so it is a
        plain ``def``."""
        self._check_not_closed()
        return to_metrics_map(self._call(_lib.Producer_metrics))

    async def close(self, *, timeout: Duration | None = None) -> None:
        """See :meth:`Producer.close`."""
        timeout_ms = close_timeout_ms(timeout)
        # Check and set at once: exactly one close() tears the producer down.
        if not self._begin_close():
            return
        c_producer = self._c_producer
        loop = asyncio.get_running_loop()
        # Refuse further records and hand the accumulated ones to the Rust
        # producer, waiting for that within the close timeout (Java's close
        # timer covers the whole close).
        start = loop.time()
        _lib.Producer_shutdown(c_producer)
        # Whether the Rust close ran to its end; if the task was cancelled
        # first, the close is forced before the handle is freed (see
        # force_close).
        closed = False
        try:
            if timeout_ms is None:
                await self._drain_async(closing=True)
                (error,) = await self._await_payload(
                    lambda cb: _lib.Producer_close_async(c_producer, cb), False)
            else:
                await self._drain_async(timeout_ms / 1000.0, closing=True)
                remaining_ms = max(0, timeout_ms - int((loop.time() - start) * 1000))
                (error,) = await self._await_payload(
                    lambda cb: _lib.Producer_close_with_timeout_async(
                        c_producer, remaining_ms, cb), False)
            closed = True
            raise_if_error(error)
        finally:
            # Forces an unfinished close, waits for the calls still in flight,
            # then joins the send and poll threads: off the loop, and to its end
            # even if this task is cancelled again meanwhile, so the serializers
            # close after it.
            teardown = loop.run_in_executor(None, self._teardown, c_producer, not closed)
            try:
                await await_to_end(teardown)
            finally:
                self._close_serializers()

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

    async def _drain_async(self, timeout_s: float | None = None, *,
                           closing: bool = False) -> None:
        """Await until every record sent so far is with the Rust producer (see
        ``_ProducerState._drain_sync``), at most ``timeout_s`` seconds when
        given, without blocking the loop; ``closing`` is passed by ``close()``
        alone (see ``_ProducerState._drain_registered``)."""
        loop = asyncio.get_running_loop()
        drained: asyncio.Future[None] = loop.create_future()

        def ready() -> None:
            if not loop.is_closed():
                loop.call_soon_threadsafe(_set_result, drained)

        if self._drain_registered(ready, closing=closing):
            return
        if timeout_s is None:
            await drained
            return
        try:
            await asyncio.wait_for(asyncio.shield(drained), timeout_s)
        except asyncio.TimeoutError:
            pass

    async def _await_payload(self, submit: Callable[[Callable[..., None]], None],
                             partitions: bool) -> tuple[Any, ...]:
        """Submit an ``_async`` FFI operation and await its completion payload
        on the loop. The completion runs on the producer's dispatcher thread and
        hops onto the loop via ``call_soon_threadsafe``; a payload nobody awaits
        any more (a cancelled task) has its handles freed."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[Any, ...]] = loop.create_future()

        def deliver(payload: tuple[Any, ...]) -> None:
            if fut.done():
                free_payload(payload, partitions)
                return
            fut.set_result(payload)

        def cb(*payload: Any) -> None:
            if loop.is_closed():
                free_payload(payload, partitions)
                return
            loop.call_soon_threadsafe(deliver, payload)

        submit(cb)
        return await fut

    async def _run_async(self, submit: Callable[[Callable[..., None]], None]) -> None:
        """A void ``_async`` FFI operation, awaited; raises its typed error."""
        (error,) = await self._await_payload(submit, False)
        raise_if_error(error)
