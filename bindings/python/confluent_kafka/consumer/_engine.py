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

"""The FFI-driven engine shared by the sync and async consumer classes.

Holds the native ``_confluentkafka`` consumer handle and the two blocking-op
drivers, ``_run_sync`` / ``_run_async``, plus the caller-thread rebalance /
commit callback machinery (consumer-threading.md §31 / §41).

**Callback thread contract.** Java runs ``ConsumerRebalanceListener`` methods and
``OffsetCommitCallback`` on the thread calling ``poll()`` / ``commit*()`` /
``unsubscribe()`` / ``close()`` (§31); the rebalance does not complete until the
listener returns. This engine reproduces that: it installs a *caller-thread*
listener in the core (via ``Consumer_subscribe_caller_thread_listener_async``),
which, when the core needs a callback, enqueues it on the consumer handle and
parks the driving op on an ack. A one-shot notification wakes this engine's wait
loop, which drains the queue **on the thread running the blocking op**, invokes
the user's listener, and acks — so the listener runs on the caller's thread, and
a reentrant op the listener submits (through the ``ConsumerHandle``, §41) is not
gated behind a parked dispatcher (closing the D25 gap-1/8 deadlock).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka import IllegalStateError
from confluent_kafka.common.errors import from_ffi_error, to_ffi_id
from confluent_kafka.common.errors._base import KafkaError
from confluent_kafka.common.topic_partition import TopicPartition

from .consumer_rebalance_listener import ConsumerRebalanceListener
from .offset_and_metadata import OffsetAndMetadata
from .offset_and_timestamp import OffsetAndTimestamp
from ._conversions import to_offset_map

_T = TypeVar("_T")

log = logging.getLogger(__name__)

# The pending-callback method discriminants (must match the Rust FFI constants
# PENDING_METHOD_REVOKED / _ASSIGNED / _LOST in src/ffi/consumer.rs).
_PENDING_REVOKED = 0
_PENDING_ASSIGNED = 1
_PENDING_LOST = 2


def _error_from_exception(exc: BaseException) -> int:
    """Build a C ``kafka_common_Error_t`` handle from a Python exception raised
    by a user listener, so the rebalance fails with it (Java: a listener that
    throws fails the rebalance). A ``KafkaError`` maps by its ``_ffi_id``;
    anything else becomes the catch-all wire error via its message."""
    try:
        code = to_ffi_id(exc)
    except TypeError:
        # Not a mapped Kafka/JDK error — surface as UnknownServerError (-1) with
        # the message, exactly as the FFI does for an unmapped listener throw.
        code = -1
    handle: int = _lib.KafkaError_new(code, str(exc))
    return handle


class _ConsumerEngine:
    """Owns the native consumer handle and the blocking-op drivers.

    Subclassed indirectly: the ``Consumer`` / ``AsyncConsumer`` bases mix this in
    and add the public API surface. Not a public class.
    """

    __slots__ = (
        "_h", "_closed", "_listener", "_key_deserializer", "_value_deserializer",
        "_loop", "_pending_notify_ref", "_pending_event",
        "_reentrant_handle", "_callback_depth",
    )

    def _engine_init(
        self, *,
        handle: int,
        key_deserializer: Callable[..., object],
        value_deserializer: Callable[..., object],
    ) -> None:
        self._h: int | None = handle
        self._closed = False
        self._listener: ConsumerRebalanceListener | None = None
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        # A guard-free reentrancy handle (§41), created lazily; used to route a
        # consumer op the listener issues from inside a callback through the
        # core's `&self` ConsumerHandle instead of the `&mut` consumer path
        # (whose single-owner guard the outer op holds, and whose `&mut` access
        # would alias the in-flight op).
        self._reentrant_handle: int | None = None
        # Depth of caller-thread callback delivery, THREAD-LOCAL: only the thread
        # currently running a listener sees `_in_callback` (a different thread
        # touching the consumer meanwhile must still hit the single-owner guard
        # and get ConcurrentModificationError). > 0 means this thread's consumer
        # op must go through `_reentrant_handle`.
        self._callback_depth = threading.local()
        # Event loop remembered for async coroutine listener/callback delivery.
        self._loop: asyncio.AbstractEventLoop | None = None
        # A threading.Event set (from the dispatcher thread) when a caller-thread
        # rebalance callback is enqueued, so the wait loop wakes and drains it.
        self._pending_event = threading.Event()
        # Keep a strong ref to the registered notify closure alive for the life
        # of the consumer; register it up front so any subscribe delivers here.
        self._pending_notify_ref: Callable[[], None] | None = (
            self._pending_event.set
        )
        _lib.Consumer_set_pending_callback_notify(self._h, self._pending_notify_ref)

    # ---- lifecycle ------------------------------------------------------
    def _check_closed(self) -> None:
        if self._closed or self._h is None:
            raise IllegalStateError("This consumer has already been closed.")

    def _destroy(self) -> None:
        if self._reentrant_handle is not None:
            _lib.ConsumerHandle_destroy(self._reentrant_handle)
            self._reentrant_handle = None
        if self._h is not None:
            _lib.Consumer_destroy(self._h)
            self._h = None
        self._listener = None
        self._pending_notify_ref = None

    # ---- reentrancy (a consumer op issued from inside a callback) --------
    @property
    def _in_callback(self) -> bool:
        """Whether THIS thread is currently delivering a rebalance callback, so a
        consumer op it issues must be routed through the ConsumerHandle (§41).
        Thread-local: a different thread is unaffected."""
        return getattr(self._callback_depth, "value", 0) > 0

    def _enter_callback(self) -> None:
        self._callback_depth.value = getattr(self._callback_depth, "value", 0) + 1

    def _exit_callback(self) -> None:
        self._callback_depth.value = getattr(self._callback_depth, "value", 0) - 1

    def _handle(self) -> int:
        """The lazily-created guard-free ConsumerHandle (§41)."""
        if self._reentrant_handle is None:
            self._reentrant_handle = _lib.Consumer_handle(self._h)
        return self._reentrant_handle

    # A consumer op the listener issues from inside a callback runs here, through
    # the ConsumerHandle's `&self` FFI (guard-free, no `&mut` aliasing). Each
    # returns/raises exactly as the consumer method would.
    def _reentrant_commit(self, offsets: Any) -> None:
        from ._conversions import offsets_to_spec
        h = self._handle()
        if offsets is None:
            error = _lib.ConsumerHandle_commit_sync(h)
        else:
            error = _lib.ConsumerHandle_commit_sync_offsets(
                h, offsets_to_spec(offsets))
        self._raise_handle_error(error)

    def _reentrant_commit_nowait(self, offsets: Any) -> None:
        from ._conversions import offsets_to_spec
        h = self._handle()
        if offsets is None:
            error = _lib.ConsumerHandle_commit_async(h)
        else:
            error = _lib.ConsumerHandle_commit_async_offsets(
                h, offsets_to_spec(offsets))
        self._raise_handle_error(error)

    def _reentrant_committed(
        self, partitions: Any
    ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        from ._conversions import tp_to_spec, to_offset_map
        handle, error = _lib.ConsumerHandle_committed(
            self._handle(), tp_to_spec(partitions))
        self._raise_handle_error(error)
        result = to_offset_map(_lib.OffsetMap_drain(handle)) if handle else {}
        for tp in partitions:
            result.setdefault(tp, None)
        return result

    def _reentrant_position(self, partition: Any) -> int:
        pos, error = _lib.ConsumerHandle_position(
            self._handle(), partition.topic(), partition.partition())
        self._raise_handle_error(error)
        return int(pos)

    def _reentrant_seek(self, partition: Any, offset: Any,
                        offset_and_metadata: Any) -> None:
        h = self._handle()
        if offset_and_metadata is not None:
            oam = offset_and_metadata
            epoch = oam.leader_epoch()
            error = _lib.ConsumerHandle_seek_with_metadata(
                h, partition.topic(), partition.partition(), oam.offset(),
                epoch if epoch is not None else -1, oam.metadata())
        else:
            error = _lib.ConsumerHandle_seek(
                h, partition.topic(), partition.partition(), offset)
        self._raise_handle_error(error)

    def _reentrant_tp_op(self, fn_name: str, partitions: Any) -> None:
        from ._conversions import tp_to_spec
        fn = getattr(_lib, fn_name)
        self._raise_handle_error(fn(self._handle(), tp_to_spec(partitions)))

    def _reentrant_long_offsets(self, fn_name: str, partitions: Any) -> "dict[TopicPartition, int]":
        from ._conversions import tp_to_spec, to_long_map
        fn = getattr(_lib, fn_name)
        handle, error = fn(self._handle(), tp_to_spec(partitions))
        self._raise_handle_error(error)
        return to_long_map(_lib.LongOffsetMap_drain(handle)) if handle else {}

    def _reentrant_offsets_for_times(
        self, timestamps: Any
    ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        from ._conversions import (
            timestamps_to_spec, to_offset_and_timestamp_map,
        )
        handle, error = _lib.ConsumerHandle_offsets_for_times(
            self._handle(), timestamps_to_spec(timestamps))
        self._raise_handle_error(error)
        result = (to_offset_and_timestamp_map(
            _lib.OffsetAndTimestampMap_drain(handle)) if handle else {})
        for tp in timestamps:
            result.setdefault(tp, None)
        return result

    @staticmethod
    def _raise_handle_error(error_handle: int) -> None:
        if error_handle:
            raise from_ffi_error(error_handle)

    # ---- caller-thread rebalance-callback drain -------------------------
    def _drain_pending_callbacks(self) -> None:
        """Run every enqueued caller-thread rebalance callback on THIS thread,
        then ack it (unblocking the parked op in the core). Synchronous —
        used by ``_run_sync``. Java: the listener runs on the poll caller's
        thread and the rebalance blocks on it (§31)."""
        while True:
            pending = _lib.Consumer_next_pending_callback(self._h)
            if pending is None:
                return
            method = _lib.PendingCallback_method(pending)
            raw = _lib.PendingCallback_partitions(pending)
            partitions = {self._to_tp(t, p) for (t, p) in raw}
            error_handle = 0
            # Mark the callback window so a reentrant consumer op the listener
            # issues routes through the ConsumerHandle (§41).
            self._enter_callback()
            try:
                result = self._invoke_listener(method, partitions)
                if inspect.isawaitable(result):
                    error_handle = self._run_awaitable_sync(result)
            except BaseException as exc:  # noqa: BLE001 - reported to the core
                error_handle = _error_from_exception(exc)
            finally:
                self._exit_callback()
            _lib.Consumer_ack_pending_callback(pending, error_handle)

    async def _drain_pending_callbacks_async(self) -> None:
        """Async peer of ``_drain_pending_callbacks``: awaits a coroutine
        listener on this event loop, so ``await consumer.commit()`` inside an
        async listener works (§41)."""
        while True:
            pending = _lib.Consumer_next_pending_callback(self._h)
            if pending is None:
                return
            method = _lib.PendingCallback_method(pending)
            raw = _lib.PendingCallback_partitions(pending)
            partitions = {self._to_tp(t, p) for (t, p) in raw}
            error_handle = 0
            self._enter_callback()
            try:
                result = self._invoke_listener(method, partitions)
                if inspect.isawaitable(result):
                    await result
            except BaseException as exc:  # noqa: BLE001 - reported to the core
                error_handle = _error_from_exception(exc)
            finally:
                self._exit_callback()
            _lib.Consumer_ack_pending_callback(pending, error_handle)

    def _invoke_listener(
        self, method: int, partitions: set[Any]
    ) -> object:
        """Dispatch one enqueued callback to the user's listener method.

        The base ``ConsumerRebalanceListener`` methods are typed ``-> None``
        (Java-identical), but a user override may be an ``async def`` returning a
        coroutine (spec §6.2 / decision F) — the ``# type: ignore`` on each
        return acknowledges that runtime-only shape."""
        listener = self._listener
        if listener is None:
            return None
        if method == _PENDING_REVOKED:
            return listener.on_partitions_revoked(partitions)  # type: ignore[func-returns-value]
        if method == _PENDING_ASSIGNED:
            return listener.on_partitions_assigned(partitions)  # type: ignore[func-returns-value]
        # _PENDING_LOST
        return listener.on_partitions_lost(partitions)  # type: ignore[func-returns-value]

    def _run_awaitable_sync(self, awaitable: Awaitable[Any]) -> int:
        """Run a coroutine listener result to completion from a synchronous
        drain. A coroutine listener needs an event loop; the sync ``Consumer``
        has none, so this rejects it (mirrors the legacy binding)."""
        loop = self._loop
        if loop is None:
            close = getattr(awaitable, "close", None)
            if callable(close):
                close()
            raise TypeError(
                "a coroutine rebalance listener requires an AsyncConsumer"
            )
        coro = cast("Coroutine[Any, Any, None]", awaitable)
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            fut.result()
        except BaseException as exc:  # noqa: BLE001
            return _error_from_exception(exc)
        return 0

    @staticmethod
    def _to_tp(topic: str, partition: int) -> TopicPartition:
        return TopicPartition(topic=topic, partition=partition)

    # ---- blocking-op drivers -------------------------------------------
    def _run_sync(
        self,
        submit: Callable[[Callable[..., None]], None],
        resolve: Callable[[tuple[Any, ...]], _T],
        free: Callable[[tuple[Any, ...]], None],
    ) -> _T:
        """Submit an async FFI op and wait for its completion on this thread,
        draining caller-thread rebalance callbacks meanwhile.

        ``KeyboardInterrupt`` aborts the in-flight op via ``wakeup``, mirroring
        Java's interrupt handling."""
        box: dict[str, tuple[Any, ...]] = {}
        done = threading.Event()

        def cb(*payload: Any) -> None:
            box["payload"] = payload
            done.set()
            # Wake the wait loop even if it is parked on the pending-callback
            # signal, so completion is observed promptly.
            self._pending_event.set()

        # Clear any stale pending-signal before submitting.
        self._pending_event.clear()
        submit(cb)
        interrupted: KeyboardInterrupt | None = None
        while not done.is_set():
            try:
                # Wake on either the op completion or a pending-callback signal;
                # short slices keep KeyboardInterrupt reachable on the main
                # thread. Then drain any pending rebalance callbacks on THIS
                # thread and ack them (unblocking the parked op).
                self._pending_event.wait(0.1)
                self._pending_event.clear()
                self._drain_pending_callbacks()
            except KeyboardInterrupt as exc:  # a signal during the wait/callback
                interrupted = exc
                self.wakeup()  # type: ignore[attr-defined]
        # Final drain to clear any callback enqueued right before completion.
        try:
            self._drain_pending_callbacks()
        except BaseException:  # noqa: BLE001 - already reported to the core
            pass
        payload = box["payload"]
        if interrupted is not None:
            free(payload)
            raise interrupted
        return resolve(payload)

    async def _run_async(
        self,
        submit: Callable[[Callable[..., None]], None],
        resolve: Callable[[tuple[Any, ...]], _T],
        free: Callable[[tuple[Any, ...]], None],
    ) -> _T:
        """Async peer of ``_run_sync``: awaits completion on the event loop,
        draining caller-thread rebalance callbacks on the loop between wakeups so
        a coroutine listener may ``await`` consumer methods (§41)."""
        loop = asyncio.get_running_loop()
        self._loop = loop
        fut: asyncio.Future[tuple[Any, ...]] = loop.create_future()

        def deliver(payload: tuple[Any, ...]) -> None:
            if fut.cancelled() or fut.done():
                free(payload)
                return
            fut.set_result(payload)

        def cb(*payload: Any) -> None:
            if loop.is_closed():
                free(payload)
                return
            loop.call_soon_threadsafe(deliver, payload)

        # An asyncio.Event to bridge the threading notify onto the loop.
        pending_async = asyncio.Event()

        def on_pending() -> None:
            # Runs on the dispatcher thread; hop onto the loop.
            if not loop.is_closed():
                loop.call_soon_threadsafe(pending_async.set)

        self._pending_notify_ref = on_pending
        _lib.Consumer_set_pending_callback_notify(self._h, on_pending)

        submit(cb)
        try:
            while not fut.done():
                waiter = asyncio.ensure_future(pending_async.wait())
                pending_set: set[asyncio.Future[Any]] = {fut, waiter}
                await asyncio.wait(
                    pending_set, return_when=asyncio.FIRST_COMPLETED,
                )
                if not waiter.done():
                    waiter.cancel()
                pending_async.clear()
                await self._drain_pending_callbacks_async()
            await self._drain_pending_callbacks_async()
            payload = fut.result()
        except asyncio.CancelledError:
            self.wakeup()  # type: ignore[attr-defined]
            raise
        return resolve(payload)

    # ---- commit callback ------------------------------------------------
    def _wrap_commit_callback(
        self, callback: Callable[..., Any] | None,
    ) -> Callable[[Any, Any], None] | None:
        """Adapt a user ``on_commit(offsets, exception)`` to the C commit
        trampoline. Runs on the caller's thread (the commit driver drains it);
        an exception raised by it is logged and swallowed (Java ``onComplete``
        returns ``void``)."""
        if callback is None:
            return None
        if not callable(callback):
            raise TypeError("on_commit must be callable")
        loop = self._loop

        def adapter(offsets_handle: Any, error_handle: Any) -> None:
            offsets: dict[Any, OffsetAndMetadata] | None
            if offsets_handle:
                raw = _lib.OffsetMap_drain(offsets_handle)
                converted = to_offset_map(raw)
                offsets = {tp: oam for tp, oam in converted.items() if oam is not None}
            else:
                offsets = None
            exception = from_ffi_error(error_handle) if error_handle else None
            try:
                result = callback(offsets, exception)
                if inspect.isawaitable(result):
                    if loop is None:
                        close = getattr(result, "close", None)
                        if callable(close):
                            close()
                        raise TypeError(
                            "a coroutine commit callback requires an "
                            "AsyncConsumer"
                        )
                    coro = cast("Coroutine[Any, Any, None]", result)
                    asyncio.run_coroutine_threadsafe(coro, loop).result()
            except Exception:  # noqa: BLE001 - must not escape into C
                log.exception("Error in commit callback")

        return adapter
