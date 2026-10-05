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

"""State and helpers shared by the FFI-backed consumers (private).

``Consumer`` / ``KafkaConsumer`` and ``AsyncConsumer`` / ``AsyncKafkaConsumer``
call the C FFI through the C extension (``_confluentkafka``) (CLAUDE.md, Python
Binding Conventions, Implementation over the FFI). A waiting call submits the
``_async`` form of its entry point and waits for the completion on the shared
dispatcher, draining the caller-thread callback queue meanwhile, so the
``ConsumerRebalanceListener`` and the ``commit_nowait()`` callback run on the
waiting thread, inside the call that delivers them (Threads and callbacks,
``consumer-threading.md`` §31):

- the core parks a rebalance on the ack of a queued listener callback
  (``kafka_consumer_Consumer_subscribe_caller_thread_listener_async``);
- a commit callback that completes inside an ``_async`` operation is queued the
  same way and runs when this thread acks it; one that completes inside a
  synchronous call (``commit_nowait()`` running earlier commits' callbacks) runs
  inside that call, on this thread.

``commit_nowait()`` has no ``_async`` entry point, and the core's commit
processes the background events while it waits for the offsets; with a listener
registered, its synchronous FFI call runs on a helper thread while the calling
thread waits and drains the queue, and a commit callback the core runs on the
helper is handed back to that call's own caller. The pending-callback notify is
registered once per consumer and wakes every waiting call, so a call the guard
rejects cannot take it from the call in flight.

A consumer operation the callback issues goes through the core's guard-free
``ConsumerHandle`` (``kafka_consumer_Consumer_handle``), since the outer call
holds the consumer's single-owner guard.

``KafkaConsumer`` is not thread-safe: a call while another thread is inside the
consumer raises ``ConcurrentModificationError``, ``wakeup()`` excepted — refused by
the per-thread use count, as Java's ``acquire()``, before it reaches the FFI's
single-owner guard (which still rejects a call from another task of the same
thread). Every native call runs inside a counted use of the handle,
and ``close()`` frees it only once no use is left, so a call racing ``close()``
never touches a freed handle.
"""

from __future__ import annotations

import asyncio
import collections
import concurrent.futures
import contextvars
import inspect
import logging
import math
import threading
from collections.abc import Awaitable, Callable, Iterable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeVar, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._config import duration_to_ms, log_unused, prepare
from confluent_kafka._errors import from_ffi_error, to_ffi_id
from confluent_kafka.common.errors.invalid_group_id_error import InvalidGroupIdError
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.common.serialization import bytes_deserializer
from confluent_kafka.common.serialization._supply import close_if_defined, resolve_serde
from confluent_kafka.common.topic_partition import TopicPartition
from confluent_kafka.concurrent_modification_error import ConcurrentModificationError
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError

from ._conversions import (
    offsets_to_spec, timestamps_to_spec, to_long_map, to_metrics_map,
    to_offset_and_timestamp_map, to_offset_map, to_partition_info_list,
    to_topics_map, tp_to_spec,
)
from ._poll import Deserialized, deserialize_batch
from .close_options import CloseOptions
from .consumer_group_metadata import ConsumerGroupMetadata

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.metric_name import MetricName
    from confluent_kafka.common.partition_info import PartitionInfo
    from confluent_kafka.common.serialization import Deserializer

    from .consumer_rebalance_listener import ConsumerRebalanceListener
    from .offset_and_metadata import OffsetAndMetadata
    from .offset_and_timestamp import OffsetAndTimestamp
    from .offset_commit_callback import OffsetCommitCallback

_T = TypeVar("_T")

_LOG = logging.getLogger("confluent_kafka.consumer")

# Java's AsyncKafkaConsumer.acquireAndEnsureOpen() / acquire() messages.
CLOSED_MESSAGE = "This consumer has already been closed."
CONCURRENT_MESSAGE = "KafkaConsumer is not safe for multi-threaded access."

# Java's AsyncKafkaConsumer.throwIfGroupIdNotDefined() message.
GROUP_ID_NOT_DEFINED_MESSAGE = ("To use the group management or offset commit APIs, you must "
                                "provide a valid group.id in the consumer configuration.")

_GROUP_ID = "group.id"

# Java's ConsumerConfig keys of a deserializer given through the config route.
_KEY_DESERIALIZER = "key.deserializer"
_VALUE_DESERIALIZER = "value.deserializer"

# The pending-callback methods (the Rust FFI's PENDING_METHOD_* discriminants,
# src/ffi/consumer.rs).
_PENDING_REVOKED = 0
_PENDING_ASSIGNED = 1
_PENDING_LOST = 2
_PENDING_COMMIT = 3

# The C close-with-option group-membership-operation discriminants.
_GROUP_OP_CODE = {
    CloseOptions.GroupMembershipOperation.DEFAULT: 0,
    CloseOptions.GroupMembershipOperation.LEAVE_GROUP: 1,
    CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP: 2,
}

# The ids of the consumers whose callback this context is delivering. A
# ContextVar rather than a thread-local: the listener of an async consumer is
# awaited inside the task that delivers it, and another task on the same loop
# is not inside the callback (it must hit the single-owner guard).
_IN_CALLBACK: contextvars.ContextVar[frozenset[int]] = contextvars.ContextVar(
    "confluent_kafka_consumer_in_callback", default=frozenset())

_OPEN = "open"
_CLOSING = "closing"
_CLOSED = "closed"

# Java's AsyncKafkaConsumer.invokeRebalanceCallbacks wrapping message.
_LISTENER_ERROR_MESSAGE = "User rebalance callback throws an error"

# The errors this call's listeners raised: (the message the core got, the
# object to raise).
_ListenerErrors = list[tuple[str, BaseException]]


def raise_if_error(error: int) -> None:
    """Raise the typed error of a plain FFI entry point's result handle."""
    if error:
        raise from_ffi_error(error)


def _error_handle_for(exc: BaseException) -> int:
    """The C error handle reporting ``exc``, a listener's exception, to the
    core, which fails the rebalance with it (Java: a listener that throws). The
    class is chosen by its FFI id (``kafka_common_Error_new``)."""
    handle: int = _lib.KafkaError_new(to_ffi_id(exc), str(exc))
    return handle


def _listener_error_for(failure: BaseException, errors: _ListenerErrors) -> BaseException | None:
    """The listener's error object a call's ``failure`` reports, if any: the
    core fails the delivering call with the first listener error, carrying its
    class and message (an empty message may come back as the class's
    default)."""
    message = str(failure)
    for reported, error in errors:
        if reported == message or not reported:
            return error
    return None


def poll_timeout_ms(timeout: Duration) -> int:
    """``poll(Duration)``'s timeout in milliseconds, as Java's
    ``Duration.toMillis()`` (rounded down); a negative one raises Java's
    ``Timer`` message, ``Invalid negative timeout -N``."""
    seconds = timeout.total_seconds() if isinstance(timeout, timedelta) else float(timeout)
    millis = math.floor(seconds * 1000.0)
    if millis < 0:
        raise IllegalArgumentError(message=f"Invalid negative timeout {millis}")
    return millis


# A None topic crosses to the FFI as the blank "": Java's Utils.isBlank(null) is
# true, so the core's own check raises Java's IllegalArgumentException for it,
# in Java's order (after the open and group.id checks).
_BLANK_TOPIC_PARTITION = TopicPartition(topic="", partition=0)


def blank_null_topics(topics: Iterable[str]) -> list[str]:
    """``subscribe(topics)``'s topics for the FFI, a ``None`` topic as ``""``
    (Java: ``isBlank(topic)`` → "Topic collection to subscribe to cannot
    contain null or empty topic")."""
    return ["" if topic is None else topic for topic in topics]


def blank_null_topic_partitions(partitions: Iterable[TopicPartition]) -> list[TopicPartition]:
    """``assign(partitions)``'s partitions for the FFI, a ``None`` partition or
    topic as a blank topic (Java: ``isBlank(tp != null ? tp.topic() : null)`` →
    "Topic partitions to assign to cannot have null or empty topic")."""
    return [_BLANK_TOPIC_PARTITION if tp is None or tp.topic() is None else tp
            for tp in partitions]


def close_args(timeout: Duration | None, option: CloseOptions | None) -> tuple[int, int]:
    """``(timeout_ms, group-membership-operation code)`` for
    ``kafka_consumer_Consumer_close_with_option``: Java's ``close(Duration)`` is
    ``close(CloseOptions.timeout(timeout))``, and ``close(CloseOptions)`` rejects
    a negative timeout (``AsyncKafkaConsumer.close(CloseOptions)``). ``-1`` is
    the FFI's "no timeout set", the default close timeout."""
    if option is not None:
        timeout = option._timeout_getter()
        operation = option._group_membership_operation_getter()
    else:
        operation = CloseOptions.GroupMembershipOperation.DEFAULT
    if timeout is None:
        return -1, _GROUP_OP_CODE[operation]
    return duration_to_ms(timeout, default_ms=-1), _GROUP_OP_CODE[operation]


class _Forward:
    """The commit callbacks a helper-hosted ``commit_nowait()`` hands back to its
    own caller's thread; the helper waits on each until it has run."""

    __slots__ = ("_queue", "_wake", "thread")

    def __init__(self, wake: Callable[[], None]) -> None:
        self.thread = threading.get_ident()
        self._wake = wake
        self._queue: collections.deque[tuple[Callable[[], None], threading.Event]] = (
            collections.deque())

    def hand(self, run: Callable[[], None]) -> None:
        """On the helper thread: queue ``run`` for the caller and wait for it."""
        ran = threading.Event()
        self._queue.append((run, ran))
        self._wake()
        ran.wait()

    def run_pending(self) -> None:
        """On the caller's thread: run what the helper handed over."""
        while self._queue:
            run, ran = self._queue.popleft()
            try:
                run()
            finally:
                ran.set()


# The _Forward of the commit_nowait() call the helper thread is running.
_HELPER = threading.local()


class _Use:
    """One use of the native consumer handle: while it runs, ``close()`` does not
    free the handle (see ``_ConsumerState``). It counts per thread, as Java's
    ``acquire()``: a use while another thread holds one raises
    ``ConcurrentModificationError`` (the same thread may nest), so a call is
    refused before it reaches the FFI — or the helper thread of
    ``commit_nowait()``, where it would otherwise queue behind the other call."""

    __slots__ = ("_state", "_thread")

    def __init__(self, state: _ConsumerState) -> None:
        self._state = state
        self._thread = 0

    def __enter__(self) -> int:
        state = self._state
        thread = threading.get_ident()
        with state._lifecycle:
            if state._state == _CLOSED or state._h == 0:
                raise IllegalStateError(message=CLOSED_MESSAGE)
            if state._state == _CLOSING and state._closing_thread != thread:
                # Java's close() holds the consumer's lock (acquire()).
                raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
            if any(user != thread for user in state._use_threads):
                # Java's acquire(): another thread is inside the consumer.
                raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
            state._uses += 1
            state._use_threads[thread] = state._use_threads.get(thread, 0) + 1
            self._thread = thread
        return state._h

    def __exit__(self, *exc: object) -> None:
        state = self._state
        with state._lifecycle:
            state._uses -= 1
            remaining = state._use_threads[self._thread] - 1
            if remaining:
                state._use_threads[self._thread] = remaining
            else:
                del state._use_threads[self._thread]
            if state._uses == 0:
                state._lifecycle.notify_all()


class _ConsumerState:
    """The native consumer handle, its deserializers, its listener and its
    lifecycle.

    The handle lives until ``close()`` frees it. Every native call runs inside
    ``_use()``, which counts it per thread under ``_lifecycle`` and refuses to
    start once the consumer is closed (``IllegalStateError``), while another
    thread holds a use, or, on another thread, while it is closing
    (``ConcurrentModificationError``, as Java's ``acquire()``). ``close()`` sets the closing state under the same lock, so exactly
    one close runs, and only when no other thread is inside the consumer: Java's
    ``close()`` calls ``acquire()`` before it changes any state, so a ``close()``
    from another thread raises ``ConcurrentModificationError`` and leaves the
    consumer as it was (and when its FFI close still fails because another thread
    is inside the consumer, the consumer stays open, as in Java). Once closed, it
    waits for the uses still counted before it frees the handle. A waiting call is
    one use, covering its submission, its wait and its callbacks.
    """

    def __init__(self) -> None:
        self._h: int = 0
        self._state = _OPEN
        self._closing_thread: int | None = None
        self._lifecycle = threading.Condition()
        self._uses = 0
        # The threads holding a use, with their counts (close() refuses while
        # another thread holds one, as Java's acquire()).
        self._use_threads: dict[int, int] = {}
        self._listener: ConsumerRebalanceListener | None = None
        self._key_deserializer: Deserializer[Any] = bytes_deserializer()
        self._value_deserializer: Deserializer[Any] = bytes_deserializer()
        self._reentrant_handle = 0
        self._group_id_defined = False
        # Set by the pending-callback notify (on the dispatcher thread) and by
        # an operation's completion, so the waiting call wakes and drains.
        self._pending_event = threading.Event()
        self._pending_notify_ref: Callable[[], None] | None = None
        # The awaiting calls' (loop, event) pairs the notify wakes. The notify
        # is registered once per consumer, so a call the guard rejects cannot
        # take it from the call in flight.
        self._async_waiters: set[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = set()
        # The thread running commit_nowait()'s synchronous FFI call while a
        # listener is registered (each call hands its commit callbacks back to
        # its own caller through a _Forward).
        self._commit_helper: concurrent.futures.ThreadPoolExecutor | None = None
        # AsyncConsumer.commit_nowait(): the task finishing a commit whose
        # coroutine listener is awaited on the loop, and its failure, raised by
        # the next awaited call.
        self._commit_continuation: asyncio.Task[None] | None = None
        self._deferred_error: BaseException | None = None

    # ---- construction ---------------------------------------------------
    def _start(self, configs: dict[str, Any], key_deserializer: Deserializer[Any] | None,
               value_deserializer: Deserializer[Any] | None) -> None:
        """Java's ``KafkaConsumer(configs, keyDeserializer, valueDeserializer)``:
        parse ``configs``, take the deserializers (an argument wins over the
        config key; neither means ``bytes_deserializer()``), build the core
        consumer, then log the unused configs. A given deserializer argument
        replaces its config key, which is then not parsed
        (``ConsumerConfig.appendDeserializerToConfig``). A construction failure
        raises its typed error after closing the deserializers built so far."""
        given = [key for key, argument in ((_KEY_DESERIALIZER, key_deserializer),
                                           (_VALUE_DESERIALIZER, value_deserializer))
                 if argument is not None]
        originals, native = prepare(configs, client="consumer", given_serdes=given)
        key = cast("Deserializer[Any]", resolve_serde(
            key_deserializer, originals, _KEY_DESERIALIZER, is_key=True,
            default=bytes_deserializer()))
        value: Deserializer[Any] | None = None
        try:
            value = cast("Deserializer[Any]", resolve_serde(
                value_deserializer, originals, _VALUE_DESERIALIZER, is_key=False,
                default=bytes_deserializer()))
            handle, error = _lib.Consumer_KafkaConsumer_new_typed(native)
        except BaseException:
            close_if_defined(key)
            if value is not None:
                close_if_defined(value)
            raise
        if error:
            close_if_defined(key)
            close_if_defined(value)
            raise from_ffi_error(error)
        self._key_deserializer = key
        self._value_deserializer = value
        self._h = handle
        self._group_id_defined = configs.get(_GROUP_ID) is not None
        # Registered once, before any operation: it also moves commit callbacks
        # onto the caller's thread (kafka_consumer_Consumer_set_pending_callback_notify).
        self._pending_notify_ref = self._on_pending_notify
        _lib.Consumer_set_pending_callback_notify(self._h, self._pending_notify_ref)
        log_unused(originals, client="consumer")

    # ---- lifecycle ------------------------------------------------------
    def _use(self) -> _Use:
        """A use of the native handle: ``with self._use() as h:``."""
        return _Use(self)

    def _call(self, fn: Callable[..., _T], *args: Any) -> _T:
        """``fn(handle, *args)`` as one use of the handle."""
        with self._use() as h:
            return fn(h, *args)

    def _begin_close(self) -> bool:
        """Enter the closing state; whether this call did (and so must close).
        A second ``close()`` — after it, or from inside the closing call's own
        callbacks — does nothing, as Java's ``if (!closed)``. While another
        thread is inside the consumer it raises ``ConcurrentModificationError``
        and changes nothing: Java's ``close()`` calls ``acquire()`` first."""
        thread = threading.get_ident()
        with self._lifecycle:
            if self._state == _CLOSED or self._h == 0:
                return False
            if self._state == _CLOSING:
                if self._closing_thread == thread:
                    return False
                raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
            if any(user != thread for user in self._use_threads):
                raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
            self._state = _CLOSING
            self._closing_thread = thread
            return True

    def _abort_close(self) -> None:
        """Back to open: the FFI close was refused (another thread is inside the
        consumer), so the consumer is still usable, as in Java."""
        with self._lifecycle:
            self._state = _OPEN
            self._closing_thread = None

    def _finish_close(self) -> None:
        """Closed: wait until no use is left (none can start), then free the
        native handle, release every callback the consumer holds, and close the
        deserializers (Java closes them last, ``AsyncKafkaConsumer.close``)."""
        with self._lifecycle:
            self._state = _CLOSED
            while self._uses:
                self._lifecycle.wait()
            handle, self._h = self._h, 0
            reentrant, self._reentrant_handle = self._reentrant_handle, 0
            helper, self._commit_helper = self._commit_helper, None
        if helper is not None:
            helper.shutdown(wait=False)
        if reentrant:
            _lib.ConsumerHandle_destroy(reentrant)
        if handle:
            _lib.Consumer_destroy(handle)
        self._listener = None
        self._pending_notify_ref = None
        close_if_defined(self._key_deserializer)
        close_if_defined(self._value_deserializer)

    # ---- callbacks: the caller-thread window ------------------------------
    def _in_callback(self) -> bool:
        """Whether this context is delivering one of this consumer's callbacks,
        so a consumer operation it issues must go through the ConsumerHandle."""
        return id(self) in _IN_CALLBACK.get()

    def _enter_callback(self) -> contextvars.Token[frozenset[int]]:
        return _IN_CALLBACK.set(_IN_CALLBACK.get() | {id(self)})

    @staticmethod
    def _exit_callback(token: contextvars.Token[frozenset[int]]) -> None:
        _IN_CALLBACK.reset(token)

    def _inside_own_call(self) -> bool:
        """Whether a call on this consumer holds its FFI guard for this context:
        a callback it is delivering, or a ``commit_nowait()`` still finishing on
        the loop; a read then goes through the ConsumerHandle."""
        return self._in_callback() or self._commit_in_flight()

    def _handle(self) -> int:
        """The core's guard-free ``ConsumerHandle`` (created lazily)."""
        with self._lifecycle:
            if not self._reentrant_handle:
                self._reentrant_handle = _lib.Consumer_handle(self._h)
            return self._reentrant_handle

    def _on_pending_notify(self) -> None:
        """The pending-callback notify, registered once per consumer; it runs on
        the dispatcher thread. It wakes every call waiting on this consumer: a
        synchronous one through ``_pending_event``, an awaiting one on its own
        loop."""
        self._pending_event.set()
        for loop, event in list(self._async_waiters):
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:  # its loop is closed
                pass

    @staticmethod
    def _on_caller_thread(run: Callable[[], None]) -> None:
        """Run a commit callback on the thread of the call delivering it. The
        core runs the callbacks of earlier commits inside ``commit_nowait()``'s
        synchronous FFI call, which runs on the helper thread when a listener is
        registered: the helper hands the callback to that call's own caller
        (its ``_Forward``) and waits until it has run, so the core continues
        after it, as after Java's ``offsetCommitCallbackInvoker.executeCallbacks()``."""
        forward: _Forward | None = getattr(_HELPER, "forward", None)
        if forward is None or forward.thread == threading.get_ident():
            run()
            return
        forward.hand(run)

    def _wrap_commit_callback(self, callback: OffsetCommitCallback | None, *,
                              empty_offsets: bool = False
                              ) -> Callable[[int, int], None] | None:
        """Adapt ``callback(offsets, exception)`` to the C commit trampoline.

        ``offsets`` is ``None`` where Java passes ``null``: when the commit
        failed (``whenComplete`` on an exceptionally completed future), and for
        an explicit empty ``offsets`` (``empty_offsets``), whose Java
        ``commit()`` returns ``completedFuture(null)``
        (``AsyncKafkaConsumer.commitAsync``).

        It runs on the caller's thread, inside the call that delivers it (the
        waiting call that acks its queued entry, or ``commit_nowait()`` running
        the callbacks of earlier commits), with the callback window open, so a
        consumer operation it issues goes through the ConsumerHandle. Java lets
        an exception from ``onComplete`` propagate out of the delivering call;
        across the FFI it cannot, so it is logged. A commit callback is a plain
        function: an awaitable it returns is closed and reported."""
        if callback is None:
            return None
        if not callable(callback):
            raise TypeError("callback must be callable")

        def adapter(offsets_handle: int, error_handle: int) -> None:
            offsets: dict[TopicPartition, OffsetAndMetadata] | None = None
            if offsets_handle:
                drained = {tp: oam for tp, oam in to_offset_map(
                    _lib.OffsetMap_drain(offsets_handle)).items() if oam is not None}
                if not error_handle and not empty_offsets:
                    offsets = drained
            exception = from_ffi_error(error_handle) if error_handle else None

            def run() -> None:
                token = self._enter_callback()
                try:
                    result = callback(offsets, cast(Exception, exception))
                    if inspect.isawaitable(result):
                        close = getattr(result, "close", None)
                        if callable(close):
                            close()
                        _LOG.error("An OffsetCommitCallback returned an awaitable; commit "
                                   "callbacks are plain functions and it was not awaited")
                except Exception:  # noqa: BLE001 - it cannot cross the FFI
                    _LOG.exception("Error in OffsetCommitCallback")
                finally:
                    self._exit_callback(token)

            self._on_caller_thread(run)

        return adapter

    def _invoke_listener(self, method: int, partitions: set[TopicPartition]) -> object:
        """Call the listener method a queued entry names; the result is
        awaitable when the method is an ``async def``."""
        listener = self._listener
        if listener is None:
            return None
        if method == _PENDING_REVOKED:
            return listener.on_partitions_revoked(partitions)  # type: ignore[func-returns-value]
        if method == _PENDING_ASSIGNED:
            return listener.on_partitions_assigned(partitions)  # type: ignore[func-returns-value]
        return listener.on_partitions_lost(partitions)  # type: ignore[func-returns-value]

    def _next_pending(self, h: int) -> tuple[int, int, set[TopicPartition]] | None:
        pending = _lib.Consumer_next_pending_callback(h)
        if pending is None:
            return None
        method = _lib.PendingCallback_method(pending)
        partitions = {TopicPartition(topic=t, partition=p)
                      for (t, p) in _lib.PendingCallback_partitions(pending)}
        return pending, method, partitions

    @staticmethod
    def _listener_failure(exc: BaseException, errors: _ListenerErrors) -> int:
        """The error handle reporting a listener's exception to the core, which
        fails the delivering call with it.

        Java's ``invokeRebalanceCallbacks`` keeps a ``KafkaException`` as it is
        and wraps any other exception as ``KafkaException("User rebalance
        callback throws an error", e)`` (``maybeWrapAsKafkaException``). The
        core only carries the class and the message, so the error object is kept
        in ``errors`` for the delivering call to raise (``_resolve_reporting``).
        ``KeyboardInterrupt`` / ``CancelledError`` are re-raised by the drain."""
        if not isinstance(exc, Exception):
            return _error_handle_for(exc)
        error: BaseException = exc
        if not isinstance(exc, KafkaError):
            error = KafkaError(message=_LISTENER_ERROR_MESSAGE, cause=exc)
        errors.append((str(error), error))
        return _error_handle_for(error)

    @staticmethod
    def _resolve_reporting(resolve: Callable[[tuple[Any, ...]], _T], payload: tuple[Any, ...],
                           errors: _ListenerErrors) -> _T:
        """``resolve(payload)``; when the call failed with the error a listener
        of this call raised, raise that very object (with its class, payload and
        cause) rather than the one rebuilt from its FFI id."""
        try:
            return resolve(payload)
        except KafkaError as exc:
            original = _listener_error_for(exc, errors)
            if original is None:
                raise
        raise original

    def _drain_pending(self, h: int, errors: _ListenerErrors, *, handoff: bool = False,
                       forward: _Forward | None = None
                       ) -> tuple[int, Awaitable[object]] | None:
        """Run the queued callbacks on this thread and ack each (unparking the
        core), after the commit callbacks the helper thread handed to this call
        (``forward``).

        A coroutine listener needs an event loop: with ``handoff`` the drain
        stops and returns the entry with its awaitable, unacked (the caller
        awaits it on the loop); otherwise it is closed and the rebalance fails
        with a ``TypeError``."""
        while True:
            if forward is not None:
                forward.run_pending()
            entry = self._next_pending(h)
            if entry is None:
                return None
            pending, method, partitions = entry
            if method == _PENDING_COMMIT:
                # The ack runs the commit callback (the adapter) on this thread.
                _lib.Consumer_ack_pending_callback(pending, 0)
                continue
            error_handle = 0
            interrupted: KeyboardInterrupt | None = None
            token = self._enter_callback()
            try:
                result = self._invoke_listener(method, partitions)
                if inspect.isawaitable(result):
                    if handoff:
                        return pending, cast("Awaitable[object]", result)
                    close = getattr(result, "close", None)
                    if callable(close):
                        close()
                    raise TypeError("a coroutine rebalance listener requires an AsyncConsumer")
            except BaseException as exc:  # noqa: BLE001 - reported to the core
                error_handle = self._listener_failure(exc, errors)
                if isinstance(exc, KeyboardInterrupt):
                    interrupted = exc
            finally:
                self._exit_callback(token)
            _lib.Consumer_ack_pending_callback(pending, error_handle)
            if interrupted is not None:
                raise interrupted

    async def _await_listener(self, pending: int, awaitable: Awaitable[object],
                              errors: _ListenerErrors) -> None:
        """Await a coroutine listener's result on this loop, inside the callback
        window, and ack its entry."""
        error_handle = 0
        cancelled: BaseException | None = None
        token = self._enter_callback()
        try:
            await awaitable
        except BaseException as exc:  # noqa: BLE001 - reported to the core
            error_handle = self._listener_failure(exc, errors)
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
                cancelled = exc
        finally:
            self._exit_callback(token)
        _lib.Consumer_ack_pending_callback(pending, error_handle)
        if cancelled is not None:
            raise cancelled

    async def _drain_pending_async(self, h: int, errors: _ListenerErrors,
                                   forward: _Forward | None = None) -> None:
        """Async peer of ``_drain_pending``: a coroutine listener is awaited on
        this event loop, so ``await consumer.commit()`` inside it works."""
        while True:
            handoff = self._drain_pending(h, errors, handoff=True, forward=forward)
            if handoff is None:
                return
            await self._await_listener(*handoff, errors)

    # ---- waiting calls ----------------------------------------------------
    def _run_sync(self, submit: Callable[[int, Callable[..., None]], None],
                  resolve: Callable[[tuple[Any, ...]], _T],
                  free: Callable[[tuple[Any, ...]], None], *,
                  forward: _Forward | None = None) -> _T:
        """Submit an ``_async`` FFI operation (``submit(handle, cb)``) and wait
        for its completion on this thread, draining the caller-thread callbacks
        meanwhile. Ctrl+C wakes the consumer, lets the call end, frees its
        result and re-raises ``KeyboardInterrupt``."""
        box: dict[str, tuple[Any, ...]] = {}
        done = threading.Event()
        errors: _ListenerErrors = []

        def cb(*payload: Any) -> None:
            box["payload"] = payload
            done.set()
            self._pending_event.set()

        with self._use() as h:
            self._pending_event.clear()
            submit(h, cb)
            interrupted: KeyboardInterrupt | None = None
            while not done.is_set():
                try:
                    # Short slices keep KeyboardInterrupt deliverable.
                    self._pending_event.wait(0.1)
                    self._pending_event.clear()
                    self._drain_pending(h, errors, forward=forward)
                except KeyboardInterrupt as exc:
                    if interrupted is None:
                        interrupted = exc
                        _lib.Consumer_wakeup(h)
            self._drain_pending(h, errors, forward=forward)
        payload = box["payload"]
        if interrupted is not None:
            free(payload)
            raise interrupted
        return self._resolve_reporting(resolve, payload, errors)

    async def _run_async(self, submit: Callable[[int, Callable[..., None]], None],
                         resolve: Callable[[tuple[Any, ...]], _T],
                         free: Callable[[tuple[Any, ...]], None], *,
                         after_commit_nowait: bool = True,
                         forward: _Forward | None = None) -> _T:
        """Async peer of ``_run_sync``: awaits the completion on the event loop,
        draining the caller-thread callbacks on the loop. Cancelling the
        awaiting task wakes the consumer, lets the call end, frees its result
        and re-raises ``CancelledError``.

        It first lets a ``commit_nowait()`` still awaiting a coroutine listener
        finish (``after_commit_nowait``), and raises that commit's failure."""
        if after_commit_nowait and not self._in_callback():
            await self._await_commit_continuation()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[Any, ...]] = loop.create_future()
        pending = asyncio.Event()
        # Drain once first: an entry may be queued already.
        pending.set()
        waiter = (loop, pending)
        errors: _ListenerErrors = []

        def deliver(payload: tuple[Any, ...]) -> None:
            if not fut.done():
                fut.set_result(payload)
            pending.set()

        def cb(*payload: Any) -> None:
            if loop.is_closed():
                free(payload)
                return
            loop.call_soon_threadsafe(deliver, payload)

        cancelled: asyncio.CancelledError | None = None
        with self._use() as h:
            self._async_waiters.add(waiter)
            try:
                submit(h, cb)
                while not fut.done():
                    try:
                        await pending.wait()
                        pending.clear()
                        await self._drain_pending_async(h, errors, forward)
                    except asyncio.CancelledError as exc:
                        if cancelled is None:
                            cancelled = exc
                            _lib.Consumer_wakeup(h)
                await self._drain_pending_async(h, errors, forward)
            finally:
                self._async_waiters.discard(waiter)
        payload = fut.result()
        if cancelled is not None:
            free(payload)
            raise cancelled
        return self._resolve_reporting(resolve, payload, errors)

    async def _await_commit_continuation(self) -> None:
        """Wait for the ``commit_nowait()`` still awaiting a coroutine listener
        (``AsyncConsumer``), then raise its failure, as Java raises a
        listener's error from the next call that runs it."""
        task = self._commit_continuation
        if task is None:
            return
        if not task.done():
            await asyncio.wait({task})
        if self._commit_continuation is task:
            self._commit_continuation = None
        error, self._deferred_error = self._deferred_error, None
        if error is not None:
            raise error

    def _commit_in_flight(self) -> bool:
        """Whether a ``commit_nowait()`` is still finishing on the loop, holding
        the consumer (``AsyncConsumer``)."""
        task = self._commit_continuation
        return task is not None and not task.done()

    # ---- commit_nowait (Java's commitAsync) -----------------------------------
    @staticmethod
    def _commit_async_ffi(h: int, spec: Any, adapter: Any) -> int:
        """The synchronous ``kafka_consumer_Consumer_commit_async*`` call of
        ``commit_nowait()``'s form; its error handle (0 on success)."""
        if spec is None:
            error: int = (_lib.Consumer_commit_async(h) if adapter is None
                          else _lib.Consumer_commit_async(h, adapter))
            return error
        error = _lib.Consumer_commit_async_offsets(h, spec, adapter)
        return error

    def _helper(self) -> concurrent.futures.ThreadPoolExecutor:
        """The thread running ``commit_nowait()``'s synchronous FFI call while a
        listener is registered (created lazily, shut down by ``close()``)."""
        with self._lifecycle:
            if self._commit_helper is None:
                self._commit_helper = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="confluent-kafka-commit-nowait")
            return self._commit_helper

    def _commit_on_helper_spec(self, spec: Any, adapter: Any,
                               forward: _Forward) -> tuple[Any, Any, Any]:
        """``commit_nowait()`` as a waiting operation: its synchronous FFI call
        runs on the helper thread, and the calling thread waits for it, draining
        the queue. There is no ``_async`` form of
        ``kafka_consumer_Consumer_commit_async*``, and the core's commit
        processes the background events while it waits for the offsets
        (``consumer-threading.md`` §31): a listener callback queued then can
        only run on the calling thread, which must not be blocked in the call."""
        def submit(h: int, cb: Callable[..., None]) -> None:
            def run() -> None:
                # The commit callbacks the core runs inside this call go back to
                # this call's caller.
                _HELPER.forward = forward
                try:
                    error = self._commit_async_ffi(h, spec, adapter)
                except BaseException as exc:  # noqa: BLE001 - raised by the waiting call
                    cb(0, exc)
                    return
                finally:
                    _HELPER.forward = None
                cb(error, None)

            self._helper().submit(run)

        def resolve(payload: tuple[Any, ...]) -> None:
            error, exc = payload
            if exc is not None:
                raise exc
            raise_if_error(error)

        def free(payload: tuple[Any, ...]) -> None:
            if payload[0]:
                _lib.KafkaError_destroy(payload[0])

        return submit, resolve, free

    # ---- payload handlers ---------------------------------------------------
    @staticmethod
    def _resolve_void(payload: tuple[Any, ...]) -> None:
        raise_if_error(payload[0])

    @staticmethod
    def _free_void(payload: tuple[Any, ...]) -> None:
        if payload and payload[0]:
            _lib.KafkaError_destroy(payload[0])

    @staticmethod
    def _resolve_map(drain: Callable[[int], Any], convert: Callable[[Any], Any]
                     ) -> Callable[[tuple[Any, ...]], Any]:
        def resolve(payload: tuple[Any, ...]) -> Any:
            handle, error = payload
            if error:
                if handle:
                    drain(handle)
                raise from_ffi_error(error)
            return convert(drain(handle)) if handle else convert({})
        return resolve

    @staticmethod
    def _free_map(drain: Callable[[int], Any]) -> Callable[[tuple[Any, ...]], None]:
        def free(payload: tuple[Any, ...]) -> None:
            handle, error = payload
            if error:
                _lib.KafkaError_destroy(error)
            if handle:
                drain(handle)
        return free

    # ---- operation specs: (submit, resolve, free) ---------------------------
    def _void_spec(self, fn: Callable[..., None], *args: Any) -> tuple[Any, Any, Any]:
        return (lambda h, cb: fn(h, *args, cb), self._resolve_void, self._free_void)

    def _poll_spec(self, timeout_ms: int) -> tuple[Any, Any, Any]:
        def resolve(payload: tuple[Any, ...]) -> Deserialized:
            records, error = payload
            if error:
                raise from_ffi_error(error)
            # The native batch owns the fetched bytes; the deserializers run on
            # this thread over memoryviews into it (consumer-threading.md §27).
            native = _lib.ConsumerRecords_wrap(records) if records else None
            return deserialize_batch(native, key_deserializer=self._key_deserializer,
                                     value_deserializer=self._value_deserializer)

        def free(payload: tuple[Any, ...]) -> None:
            records, error = payload
            if error:
                _lib.KafkaError_destroy(error)
            if records:
                _lib.ConsumerRecords_wrap(records)

        return (lambda h, cb: _lib.Consumer_poll_async(h, timeout_ms, cb), resolve, free)

    def _subscribe_topics_spec(self, topics: list[str], has_listener: bool) -> tuple[Any, Any, Any]:
        if has_listener:
            return self._void_spec(_lib.Consumer_subscribe_caller_thread_listener_async, topics)
        return self._void_spec(_lib.Consumer_subscribe_async, topics)

    def _subscribe_pattern_spec(self, pattern: str, has_listener: bool) -> tuple[Any, Any, Any]:
        if has_listener:
            return self._void_spec(
                _lib.Consumer_subscribe_pattern_caller_thread_listener_async, pattern)
        return self._void_spec(_lib.Consumer_subscribe_pattern_async, pattern)

    def _seek_spec(self, partition: TopicPartition, offset: int | None,
                   offset_and_metadata: OffsetAndMetadata | None) -> tuple[Any, Any, Any]:
        if offset_and_metadata is not None:
            epoch = offset_and_metadata.leader_epoch()
            return self._void_spec(
                _lib.Consumer_seek_with_offset_and_metadata_async, partition.topic(),
                partition.partition(), offset_and_metadata.offset(),
                -1 if epoch is None else epoch, offset_and_metadata.metadata())
        return self._void_spec(_lib.Consumer_seek_async, partition.topic(),
                               partition.partition(), offset)

    def _commit_spec(self, offsets: Any) -> tuple[Any, Any, Any]:
        if offsets is None:
            return self._void_spec(_lib.Consumer_commit_sync_async)
        return self._void_spec(_lib.Consumer_commit_sync_with_offsets_async,
                               offsets_to_spec(offsets))

    def _position_spec(self, partition: TopicPartition) -> tuple[Any, Any, Any]:
        def resolve(payload: tuple[Any, ...]) -> int:
            position, error = payload
            if error:
                raise from_ffi_error(error)
            return int(position)

        def free(payload: tuple[Any, ...]) -> None:
            if payload[1]:
                _lib.KafkaError_destroy(payload[1])

        return (lambda h, cb: _lib.Consumer_position_async(
            h, partition.topic(), partition.partition(), cb), resolve, free)

    def _committed_spec(self, partitions: list[TopicPartition]) -> tuple[Any, Any, Any]:
        def convert(raw: Any) -> dict[TopicPartition, OffsetAndMetadata | None]:
            result = to_offset_map(raw)
            # Java maps a partition without a committed offset to null.
            for tp in partitions:
                result.setdefault(tp, None)
            return result

        spec = tp_to_spec(partitions)
        return (lambda h, cb: _lib.Consumer_committed_async(h, spec, cb),
                self._resolve_map(_lib.OffsetMap_drain, convert),
                self._free_map(_lib.OffsetMap_drain))

    def _offsets_for_times_spec(self, timestamps_to_search: dict[TopicPartition, int]
                                ) -> tuple[Any, Any, Any]:
        def convert(raw: Any) -> dict[TopicPartition, OffsetAndTimestamp | None]:
            result = to_offset_and_timestamp_map(raw)
            # Java maps a partition without an offset for its timestamp to null.
            for tp in timestamps_to_search:
                result.setdefault(tp, None)
            return result

        spec = timestamps_to_spec(timestamps_to_search)
        return (lambda h, cb: _lib.Consumer_offsets_for_times_async(h, spec, cb),
                self._resolve_map(_lib.OffsetAndTimestampMap_drain, convert),
                self._free_map(_lib.OffsetAndTimestampMap_drain))

    def _long_offsets_spec(self, fn: Callable[..., None], partitions: list[TopicPartition]
                           ) -> tuple[Any, Any, Any]:
        spec = tp_to_spec(partitions)
        return (lambda h, cb: fn(h, spec, cb),
                self._resolve_map(_lib.LongOffsetMap_drain, to_long_map),
                self._free_map(_lib.LongOffsetMap_drain))

    def _partitions_for_spec(self, topic: str) -> tuple[Any, Any, Any]:
        return (lambda h, cb: _lib.Consumer_partitions_for_async(h, topic, cb),
                self._resolve_map(_lib.PartitionInfoList_drain, to_partition_info_list),
                self._free_map(_lib.PartitionInfoList_drain))

    def _list_topics_spec(self) -> tuple[Any, Any, Any]:
        return (lambda h, cb: _lib.Consumer_list_topics_async(h, cb),
                self._resolve_map(_lib.TopicPartitionInfoMap_drain, to_topics_map),
                self._free_map(_lib.TopicPartitionInfoMap_drain))

    def _close_spec(self, timeout_ms: int, operation_code: int) -> tuple[Any, Any, Any]:
        return self._void_spec(_lib.Consumer_close_with_option_async, timeout_ms, operation_code)

    # ---- calls that do not wait (both families) ---------------------------
    def _c_assignment(self) -> set[TopicPartition]:
        with self._use() as h:
            raw = (_lib.ConsumerHandle_assignment(self._handle()) if self._inside_own_call()
                   else _lib.Consumer_assignment(h))
        if raw is None:
            raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
        return {TopicPartition(topic=t, partition=p) for (t, p) in raw}

    def _c_subscription(self) -> set[str]:
        with self._use() as h:
            raw = (_lib.ConsumerHandle_subscription(self._handle()) if self._inside_own_call()
                   else _lib.Consumer_subscription(h))
        if raw is None:
            raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
        return set(raw)

    def _c_paused(self) -> set[TopicPartition]:
        with self._use() as h:
            raw = (_lib.ConsumerHandle_paused(self._handle()) if self._inside_own_call()
                   else _lib.Consumer_paused(h))
        if raw is None:
            raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
        return {TopicPartition(topic=t, partition=p) for (t, p) in raw}

    def _c_metrics(self) -> dict[MetricName, Metric]:
        raw = self._call(_lib.Consumer_metrics)
        if raw is None:
            raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
        return to_metrics_map(raw)

    def _c_current_lag(self, topic_partition: TopicPartition) -> int | None:
        lag: int | None = self._call(_lib.Consumer_current_lag, topic_partition.topic(),
                                     topic_partition.partition())
        return lag

    def _assign_partitions(self, partitions: Iterable[TopicPartition]) -> list[TopicPartition]:
        """Java's ``assign()`` argument checks: a null collection raises after
        the open check (the FFI takes a list, so the binding checks it); a null
        partition or topic is left to the core as a blank topic."""
        if partitions is None:
            with self._use():
                raise IllegalArgumentError(
                    message="Topic partitions collection to assign to cannot be null")
        return blank_null_topic_partitions(partitions)

    def _c_group_metadata(self) -> ConsumerGroupMetadata:
        # Java's groupMetadata() calls throwIfGroupIdNotDefined(); the core's
        # returns a stub without a group.id, so the binding checks it.
        if not self._group_id_defined:
            with self._use():
                pass
            raise InvalidGroupIdError(message=GROUP_ID_NOT_DEFINED_MESSAGE)
        native = self._call(_lib.Consumer_group_metadata)
        if native is None:
            raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
        return ConsumerGroupMetadata._of(
            group_id=native.group_id, generation_id=native.generation_id,
            member_id=native.member_id, group_instance_id=native.group_instance_id,
            native=native)

    def _c_commit_nowait(self, offsets: Any, callback: OffsetCommitCallback | None) -> None:
        """Java's ``commitAsync()`` / ``commitAsync(callback)`` /
        ``commitAsync(offsets, callback)``: returns once the commit is sent; the
        callback runs inside a later call on this consumer, on its thread.

        The core's commit waits for the offsets to commit and processes the
        background events meanwhile (``consumer-threading.md`` §31; Java's
        ``commitAsync`` does not), so with a listener registered a listener
        callback can be queued during it for this thread to run: the
        synchronous FFI call then runs on the helper thread while this thread
        waits and drains the queue (``_commit_on_helper``)."""
        with self._use() as h:
            spec = None if offsets is None else offsets_to_spec(offsets)
            if self._in_callback() and callback is None:
                handle = self._handle()
                raise_if_error(_lib.ConsumerHandle_commit_async(handle) if spec is None
                               else _lib.ConsumerHandle_commit_async_offsets(handle, spec))
                return
            adapter = self._wrap_commit_callback(
                callback, empty_offsets=offsets is not None and not offsets)
            if self._in_callback() or self._listener is None:
                # Nothing can be queued for this thread: no caller-thread listener
                # (or, inside a callback, the FFI guard rejects the call).
                raise_if_error(self._commit_async_ffi(h, spec, adapter))
                return
        self._commit_on_helper(spec, adapter)

    def _commit_on_helper(self, spec: Any, adapter: Any) -> None:
        """``commit_nowait()``'s FFI call on the helper thread, this thread
        waiting and draining (``_commit_on_helper_spec``)."""
        forward = _Forward(self._on_pending_notify)
        self._run_sync(*self._commit_on_helper_spec(spec, adapter, forward), forward=forward)

    def _c_wakeup(self) -> None:
        """Java's ``wakeup()``: callable from any thread; a no-op once closed."""
        with self._lifecycle:
            if not self._h:
                return
            self._uses += 1
            h = self._h
        try:
            _lib.Consumer_wakeup(h)
        finally:
            with self._lifecycle:
                self._uses -= 1
                if self._uses == 0:
                    self._lifecycle.notify_all()

    # ---- a consumer operation from inside a callback (the ConsumerHandle) -----
    def _reentrant(self, op: str, *args: Any) -> Any:
        """Run ``op`` through the guard-free ConsumerHandle."""
        handle = self._handle()
        if op == "commit":
            (offsets,) = args
            raise_if_error(_lib.ConsumerHandle_commit_sync(handle) if offsets is None
                           else _lib.ConsumerHandle_commit_sync_offsets(
                               handle, offsets_to_spec(offsets)))
            return None
        if op == "seek":
            partition, offset, oam = args
            if oam is not None:
                epoch = oam.leader_epoch()
                raise_if_error(_lib.ConsumerHandle_seek_with_metadata(
                    handle, partition.topic(), partition.partition(), oam.offset(),
                    -1 if epoch is None else epoch, oam.metadata()))
            else:
                raise_if_error(_lib.ConsumerHandle_seek(
                    handle, partition.topic(), partition.partition(), offset))
            return None
        if op in ("assign", "pause", "resume", "seek_to_beginning", "seek_to_end"):
            (partitions,) = args
            raise_if_error(getattr(_lib, "ConsumerHandle_" + op)(handle, tp_to_spec(partitions)))
            return None
        if op == "position":
            (partition,) = args
            position, error = _lib.ConsumerHandle_position(
                handle, partition.topic(), partition.partition())
            raise_if_error(error)
            return int(position)
        if op == "committed":
            (partitions,) = args
            raw, error = _lib.ConsumerHandle_committed(handle, tp_to_spec(partitions))
            raise_if_error(error)
            result = to_offset_map(_lib.OffsetMap_drain(raw)) if raw else {}
            for tp in partitions:
                result.setdefault(tp, None)
            return result
        if op in ("beginning_offsets", "end_offsets"):
            (partitions,) = args
            raw, error = getattr(_lib, "ConsumerHandle_" + op)(handle, tp_to_spec(partitions))
            raise_if_error(error)
            return to_long_map(_lib.LongOffsetMap_drain(raw)) if raw else {}
        if op == "offsets_for_times":
            (timestamps,) = args
            raw, error = _lib.ConsumerHandle_offsets_for_times(handle, timestamps_to_spec(timestamps))
            raise_if_error(error)
            result_t = to_offset_and_timestamp_map(
                _lib.OffsetAndTimestampMap_drain(raw)) if raw else {}
            for tp in timestamps:
                result_t.setdefault(tp, None)
            return result_t
        raise AssertionError(op)

    def _reentrant_use(self, op: str, *args: Any) -> Any:
        """``_reentrant`` as one use of the handle."""
        with self._use():
            return self._reentrant(op, *args)

