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
``_cb`` form of its entry point and waits for its completion, never calling the
blocking form. The core runs nothing on a thread of its own: every completion,
``ConsumerRebalanceListener`` method and ``OffsetCommitCallback`` is queued on
the consumer's callbacks vector, and the waiting call runs them on its own thread
(or event loop) through ``kafka_consumer_Consumer__execute_callbacks``, woken by
the notify hook (``kafka_consumer_Consumer__set_callbacks_notify``). So the
listener and the commit callback run on the waiting thread, inside the call that
delivers them, and the rebalance does not advance before the listener returns
(Threads and callbacks, ``consumer-threading.md`` §31). The notify is registered
once per consumer and wakes every waiting call.

A consumer operation a callback issues goes through the core's guard-free
``ConsumerHandle`` (``kafka_consumer_Consumer_handle``), since the outer call
holds the consumer's single-owner guard. An operation the handle lacks
(``poll``, ``subscribe``, ``close``, ...) raises ``ConcurrentModificationError``
at once: the core's guard would refuse it too, but its answer would be queued
behind the running callback, where a nested drain (which runs nothing) cannot
reach it.

A listener's or an ``OffsetCommitCallback``'s exception reaches the core as a
foreign callback error owning a
reference to the exception object (``kafka_common_Error_local_callback``), which
it releases when its last copy drops; the call it fails reads the object back
from its error and raises it, as Java surfaces the callback's own ``Throwable``
(``_raise_error``).

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
import contextlib
import contextvars
import inspect
import logging
import math
import threading
import weakref
from collections.abc import Awaitable, Callable, Iterable, Iterator
from datetime import timedelta
from typing import TYPE_CHECKING, Any, NoReturn, TypeVar, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._async import Undelivered, call_soon_on
from confluent_kafka._config import duration_to_ms, prepare
from confluent_kafka._errors import from_ffi_error
from confluent_kafka.common.errors.invalid_group_id_error import InvalidGroupIdError
from confluent_kafka.common.errors.wakeup_error import WakeupError
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.common.serialization import bytes_deserializer
from confluent_kafka.common.config.config_error import ConfigError
from confluent_kafka.common.serialization._supply import _resolve_class as resolve_class
from confluent_kafka.common.serialization._supply import close_if_defined, resolve_serde
from confluent_kafka.common.topic_partition import TopicPartition
from confluent_kafka.concurrent_modification_error import ConcurrentModificationError
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError
from confluent_kafka.null_pointer_error import NullPointerError

from ._conversions import (
    offsets_to_spec, timestamps_to_spec, to_long_map, to_metrics_map,
    to_offset_and_timestamp_map, to_offset_map, to_partition_info_list,
    to_topics_map, tp_to_spec,
)
from ._poll import Deserialized, deserialize_batch, fetched_positions
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

# The ``abandoned`` of a waiting call whose result carries records (``poll``):
# the ``(partition, offset)`` positions to seek back to when the call does not
# return its result (Ctrl+C, cancellation).
_Abandoned = Callable[[tuple[Any, ...]], list[tuple[TopicPartition, int]]]


# Java's Long.MAX_VALUE, the largest Duration.toMillis().
_LONG_MAX = (1 << 63) - 1

# Java's AsyncKafkaConsumer.acquireAndEnsureOpen() / acquire() messages.
CLOSED_MESSAGE = "This consumer has already been closed."
CONCURRENT_MESSAGE = "KafkaConsumer is not safe for multi-threaded access."

# The null checks of AsyncKafkaConsumer on an argument the FFI takes as a list
# or a map, so a None cannot cross it (CLAUDE.md, Python Binding Conventions,
# Implementation over the FFI, exception 4): pause / resume
# (Objects.requireNonNull), offsetsForTimes, the private
# seek(Collection, AutoOffsetResetStrategy) of seekToBeginning / seekToEnd, and
# the private beginningOrEndOffset; with whether Java checks before acquire()
# (only the private seek does). assign's is _assign_partitions.
_NULL_ARGUMENT: dict[str, tuple[type[NullPointerError] | type[IllegalArgumentError], str, bool]] = {
    "pause": (NullPointerError, "The partitions to pause must be nonnull", False),
    "resume": (NullPointerError, "The partitions to resume must be nonnull", False),
    "offsets_for_times": (NullPointerError, "Timestamps to search cannot be null", False),
    "seek_to_beginning": (IllegalArgumentError, "Partitions collection cannot be null", True),
    "seek_to_end": (IllegalArgumentError, "Partitions collection cannot be null", True),
    "beginning_offsets": (NullPointerError, "Partitions cannot be null", False),
    "end_offsets": (NullPointerError, "Partitions cannot be null", False),
}

# Java's AsyncKafkaConsumer.throwIfGroupIdNotDefined() message.
GROUP_ID_NOT_DEFINED_MESSAGE = ("To use the group management or offset commit APIs, you must "
                                "provide a valid group.id in the consumer configuration.")

_GROUP_ID = "group.id"
_CLIENT_ID = "client.id"

# Java's ConsumerConfig keys of a deserializer given through the config route.
_KEY_DESERIALIZER = "key.deserializer"
_VALUE_DESERIALIZER = "value.deserializer"

# The listener methods the C trampolines name (the ListenerAdapter's _on_*).
_REVOKED = 0
_ASSIGNED = 1
_LOST = 2

# The C close group-membership-operation discriminants (Consumer_close_cb).
_GROUP_OP_CODE = {
    CloseOptions.GroupMembershipOperation.DEFAULT: 0,
    CloseOptions.GroupMembershipOperation.LEAVE_GROUP: 1,
    CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP: 2,
}

# kafka_common_Error_LOCAL_CALLBACK: a foreign callback error the client
# reported as it is, without wrapping it.
_LOCAL_CALLBACK = -30

# Where a waiting call raises a callback's exception (``_raise_error``,
# ``_raise_at``), since Java's sites wrap it differently: as it is, or as the
# client wrapped it (commit, commit_nowait, subscribe, ...); poll's
# maybeWrapAsKafkaException, which leaves a KafkaException unwrapped; close's
# new KafkaException("Failed to close kafka consumer", e), which wraps every
# exception but an InterruptException.
_AT_CALL = 0
_AT_POLL = 1
_AT_CLOSE = 2

# Java's AsyncKafkaConsumer.close() message.
CLOSE_FAILED_MESSAGE = "Failed to close kafka consumer"

# The FFI's "no timeout given": the entry point then calls Java's overload
# without the Duration.
_NO_TIMEOUT = -1

# The core's message for a negative timeout given to a `_with_timeout` entry
# point (the FFI's `ms`, `Error::local_illegal_argument`), used where Java's
# Duration overload makes no negative check of its own (overload_timeout_ms).
TIMEOUT_NEGATIVE_MESSAGE = "Timeout must not be negative"

# The ids of the consumers whose callback this context is delivering. A
# ContextVar rather than a thread-local: the listener of an async consumer is
# awaited inside the task that delivers it, and another task on the same loop
# is not inside the callback (it must hit the single-owner guard).
_IN_CALLBACK: contextvars.ContextVar[frozenset[int]] = contextvars.ContextVar(
    "confluent_kafka_consumer_in_callback", default=frozenset())

# How often a close() waiting for the uses in flight looks for an awaiting
# call whose event loop has closed meanwhile (_finish_close).
_ABANDONED_RECHECK_S = 0.1

# How long close() waits at most, once it reported a coroutine callback left on
# a closed loop, for the core to end the call that delivered it
# (AsyncConsumer._a_close, abandoned_call_wait_s, capped by the close's own
# timeout): the core completes that call once it has the report, and releases
# its guard before it queues the completion.
ABANDONED_CALL_END_S = 5.0

_OPEN = "open"
_CLOSING = "closing"
_CLOSED = "closed"


def raise_if_error(error: int) -> None:
    """Raise the typed error of a plain FFI entry point's result handle."""
    if error:
        raise from_ffi_error(error)


def _wraps_its_source(error: int) -> bool:
    """Whether the error is Java's ``new KafkaException(t)`` around a foreign
    callback error: its source is that error and its message the source's
    ``toString()``, which the core renders ``"LocalCallbackError: <message>"``
    (``common::LocalCallbackError``'s ``Display``)."""
    source: int = _lib.KafkaError_source(error)
    return bool(source) and _lib.KafkaError_code(source) == _LOCAL_CALLBACK and (
        _lib.KafkaError_message(error)
        == "LocalCallbackError: " + _lib.KafkaError_message(source))


def _parse_class_key(originals: dict[str, Any], key: str) -> None:
    """The checks Java's ``ConfigDef`` makes on a deserializer's ``CLASS`` key
    while it parses the configs, before the consumer is built: an unresolvable
    name, or a value that is neither a name nor a class, raises
    ``ConfigError`` (``resolve_serde`` constructs the class later)."""
    value = originals.get(key)
    if value is None or inspect.isclass(value):
        return
    if isinstance(value, str):
        resolve_class(key, value)
        return
    raise ConfigError(name=key, value=value, message="Expected a Class instance or class name.")


class _ListenerAdapter:
    """What the C listener trampolines call: ``_on_revoked`` / ``_on_assigned``
    / ``_on_lost(partitions, callback_id)``, the partitions as
    ``list[(topic, partition)]``. Each runs the listener's method through the
    consumer (``_ConsumerState._run_listener``) and returns ``None`` when it
    returned, ``True`` when the consumer deferred it (a coroutine method on an
    ``AsyncConsumer``, reported later through
    ``kafka_consumer_Consumer__set_callback_result``), or raises its exception,
    which the trampoline reports to the core.

    The C registration owns the adapter, so it holds the consumer weakly: a
    strong reference would keep the consumer alive until it is closed."""

    __slots__ = ("_listener", "_state")

    def __init__(self, listener: ConsumerRebalanceListener, state: _ConsumerState) -> None:
        self._listener = listener
        self._state = weakref.ref(state)

    def _on_revoked(self, raw: list[tuple[str, int]], callback_id: int) -> bool | None:
        return self._invoke(_REVOKED, raw, callback_id)

    def _on_assigned(self, raw: list[tuple[str, int]], callback_id: int) -> bool | None:
        return self._invoke(_ASSIGNED, raw, callback_id)

    def _on_lost(self, raw: list[tuple[str, int]], callback_id: int) -> bool | None:
        return self._invoke(_LOST, raw, callback_id)

    def _invoke(self, method: int, raw: list[tuple[str, int]], callback_id: int) -> bool | None:
        partitions = {TopicPartition(topic=t, partition=p) for (t, p) in raw}
        state = self._state()
        if state is None:  # the consumer is gone: nothing can wait for it
            return None
        return state._run_listener(self._listener, method, partitions, callback_id)


def _duration_millis(timeout: Duration) -> int:
    """A ``Duration`` in whole milliseconds, rounded down as Java's
    ``Duration.toMillis()``: a ``timedelta`` exactly, from its integer fields, a
    number through ``float()``. One that does not fit a signed 64-bit
    millisecond count raises ``OverflowError``, infinity included, and ``NaN``
    ``ValueError`` (see ``duration_to_ms``); a negative one is returned."""
    if isinstance(timeout, timedelta):
        millis = ((timeout.days * 86_400 + timeout.seconds) * 1_000
                  + timeout.microseconds // 1_000)
    else:
        millis = math.floor(float(timeout) * 1000.0)
    if millis > _LONG_MAX:
        raise OverflowError(f"timeout of {millis} ms does not fit a signed 64-bit integer")
    return millis


def poll_timeout_ms(timeout: Duration) -> int:
    """``poll(Duration)``'s timeout in whole milliseconds (``_duration_millis``).
    A negative one raises Java's ``Timer`` message, ``Invalid negative timeout
    -N``."""
    millis = _duration_millis(timeout)
    if millis < 0:
        raise IllegalArgumentError(message=f"Invalid negative timeout {millis}")
    return millis


def overload_timeout_ms(timeout: Duration | None, *, timer: bool) -> int:
    """The ``timeout`` of a ``Duration`` overload in whole milliseconds
    (``_duration_millis``), for its ``_with_timeout`` entry point; ``None``
    (not given) is ``_NO_TIMEOUT``, the overload without it.

    A negative one cannot cross the C extension, where a negative count selects
    the overload without the timeout (CLAUDE.md, Python Binding Conventions,
    Implementation over the FFI, exception 1), so it raises
    ``IllegalArgumentError`` here: with Java's ``Timer`` message (``Invalid
    negative timeout -N``) where Java builds a ``Timer`` from it (``timer``:
    ``commitSync``, ``position``), else with the message of the core's
    ``_with_timeout`` entry points, ``TIMEOUT_NEGATIVE_MESSAGE``, as Java's
    method makes no negative check of its own."""
    if timeout is None:
        return _NO_TIMEOUT
    millis = _duration_millis(timeout)
    if millis < 0:
        raise IllegalArgumentError(message=f"Invalid negative timeout {millis}" if timer
                                   else TIMEOUT_NEGATIVE_MESSAGE)
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


def abandoned_call_wait_s(close_timeout_ms: int) -> float:
    """How long ``close()`` waits for the core to end a call whose coroutine
    callback it reported for a closed loop (``AsyncConsumer._a_close``):
    ``ABANDONED_CALL_END_S``, capped by the close's own timeout
    (``close_args``' milliseconds, ``-1`` for the default close timeout, which
    is longer), so ``close(timeout=0)`` does not wait."""
    if close_timeout_ms < 0:
        return ABANDONED_CALL_END_S
    return min(ABANDONED_CALL_END_S, close_timeout_ms / 1000.0)


def close_args(option: CloseOptions | None) -> tuple[int, int]:
    """``(timeout_ms, group-membership-operation code)`` for
    ``kafka_consumer_Consumer_close_with_options``: ``close(CloseOptions)``
    rejects a negative timeout (``AsyncKafkaConsumer.close(CloseOptions)``).
    ``-1`` is the FFI's "no timeout set", the default close timeout."""
    timeout: Duration | None = None
    if option is not None:
        timeout = option._timeout_getter()
        operation = option._group_membership_operation_getter()
    else:
        operation = CloseOptions.GroupMembershipOperation.DEFAULT
    if timeout is None:
        return -1, _GROUP_OP_CODE[operation]
    return duration_to_ms(timeout, default_ms=-1), _GROUP_OP_CODE[operation]



class _Use:
    """One use of the native consumer handle: while it runs, ``close()`` does not
    free the handle (see ``_ConsumerState``). It counts per thread, as Java's
    ``acquire()``: a use while another thread holds one raises
    ``ConcurrentModificationError`` (the same thread may nest), so a call is
    refused before it reaches the FFI's single-owner guard."""

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
                # An awaiting call left on a closed event loop can never end
                # its use: end it first, so it does not hold the consumer.
                state._release_abandoned()
            if any(user != thread for user in state._use_threads):
                # Java's acquire(): another thread is inside the consumer.
                raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
            state._uses += 1
            state._use_threads[thread] = state._use_threads.get(thread, 0) + 1
            self._thread = thread
        return state._h

    def __exit__(self, *exc: object) -> None:
        self.release()

    def release(self) -> None:
        """End this use; once ended, a second call does nothing (see
        ``_ConsumerState._release_abandoned``)."""
        state = self._state
        with state._lifecycle:
            if not self._thread:
                return
            state._uses -= 1
            remaining = state._use_threads[self._thread] - 1
            if remaining:
                state._use_threads[self._thread] = remaining
            else:
                del state._use_threads[self._thread]
            self._thread = 0
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
    one use, covering its submission, its wait and its callbacks; an awaiting call
    whose event loop has closed can never resume, so its use is ended by the next
    call that meets it, by the next awaiting call and in ``close()``
    (``_release_abandoned``).
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
        # The wakeup the consumer sent itself to end a wakeable call sooner on
        # Ctrl+C or cancellation (_run_sync, _wait_on_loop), left armed because
        # the call ended without consuming it (_own_wake_outcome). The next
        # wakeable call that the core ends with WakeupError for it is submitted
        # again, so the wakeup never outlives the call that sent it. A user's
        # wakeup() clears it (_c_wakeup): the wakeup armed is then the user's.
        self._own_wake_left = False
        # The user's wakeup() calls, so a call can tell whether the wakeup it
        # sent itself is still the only one armed.
        self._user_wakeups = 0
        self._group_id_defined = False
        # Set by the notify hook (on a core task) and by an operation's
        # completion, so the waiting call wakes and drains.
        self._pending_event = threading.Event()
        self._pending_notify_ref: Callable[[], None] | None = None
        # The awaiting calls' (loop, event) pairs the notify wakes, with the use
        # each holds. The notify is registered once per consumer, so a call the
        # guard rejects cannot take it from the call in flight.
        self._async_waiters: dict[tuple[asyncio.AbstractEventLoop, asyncio.Event], _Use] = {}
        # The awaiting calls' results on their way to their loop (AsyncConsumer).
        self._undelivered = Undelivered()
        # AsyncConsumer: the tasks awaiting a coroutine listener method, which
        # the delivering call waits for before it returns.
        self._listener_tasks: set[asyncio.Task[None]] = set()
        # AsyncConsumer: the coroutine callbacks deferred to a task and not yet
        # reported to the core, by callback id, with the task's loop. One whose
        # loop has closed can never report: _release_abandoned reports it.
        self._unacked: dict[int, asyncio.AbstractEventLoop] = {}
        # AsyncConsumer: a coroutine commit callback's exception a poll could
        # not raise without losing its records, raised by the next awaited call
        # (_keep_error, _raise_deferred_error).
        self._deferred_error: BaseException | None = None
        # AsyncConsumer: the exception a coroutine commit callback raised after
        # its invocation was reported (see _take_callback_error).
        self._callback_error: BaseException | None = None
        # The threads inside a blocking ConsumerHandle commit, with their
        # counts: the commit callbacks it runs inside it run on that thread
        # while it waits (AsyncConsumer's _defer_callback reads it).
        self._handle_commit_threads: dict[int, int] = {}

    # ---- construction ---------------------------------------------------
    def _start(self, configs: dict[str, Any], key_deserializer: Deserializer[Any] | None,
               value_deserializer: Deserializer[Any] | None) -> None:
        """Java's ``KafkaConsumer(configs, keyDeserializer, valueDeserializer)``:
        parse ``configs``, build the core consumer (which reports the keys it
        does not know), then take the deserializers (an argument wins over the
        config key; neither means ``bytes_deserializer()``). A given
        deserializer argument replaces its config key, which is then not parsed
        (``ConsumerConfig.appendDeserializerToConfig``).

        A deserializer the config route builds is configured with the user's
        configs and the consumer's ``client.id``, the one the core generated
        when none is set (``consumer-<group.id>-<n>``), as Java's
        ``Deserializers`` passes ``config.originals(singletonMap(CLIENT_ID_CONFIG,
        clientId))``: so it is constructed once the core consumer exists, the
        core holding the id *(deviation: Java builds it before the bootstrap
        metadata, the core resolves the bootstrap hosts first)*. Its checks
        that Java's ``ConfigDef`` makes while it parses the configs (an
        unresolvable name, an instance) and the arguments' checks run first.

        A construction failure raises its typed error after closing the
        deserializers built so far, and the core consumer when it was built."""
        given = [key for key, argument in ((_KEY_DESERIALIZER, key_deserializer),
                                           (_VALUE_DESERIALIZER, value_deserializer))
                 if argument is not None]
        originals, native = prepare(configs, client="consumer", given_serdes=given)
        arguments: list[Deserializer[Any]] = []
        try:
            for argument, config_key, is_key in ((key_deserializer, _KEY_DESERIALIZER, True),
                                                 (value_deserializer, _VALUE_DESERIALIZER,
                                                  False)):
                if argument is not None:
                    arguments.append(cast("Deserializer[Any]", resolve_serde(
                        argument, originals, config_key, is_key=is_key,
                        default=bytes_deserializer())))
                else:
                    _parse_class_key(originals, config_key)
            handle, error = _lib.Consumer_KafkaConsumer_new(native)
        except BaseException:
            for argument in arguments:
                close_if_defined(argument)
            raise
        if error:
            for argument in arguments:
                close_if_defined(argument)
            raise from_ffi_error(error)
        key: Deserializer[Any] | None = None
        try:
            serde_configs = dict(originals)
            serde_configs[_CLIENT_ID] = _lib.Consumer_client_id(handle)
            key = cast("Deserializer[Any]", resolve_serde(
                key_deserializer, serde_configs, _KEY_DESERIALIZER, is_key=True,
                default=bytes_deserializer()))
            value = cast("Deserializer[Any]", resolve_serde(
                value_deserializer, serde_configs, _VALUE_DESERIALIZER, is_key=False,
                default=bytes_deserializer()))
        except BaseException:
            # The key deserializer built or given, and a given value one (one
            # the config route builds is built last, so nothing fails after it).
            for built in (key if key is not None else key_deserializer, value_deserializer):
                if built is not None:
                    close_if_defined(built)
            _lib.Consumer_destroy(handle)
            raise
        self._key_deserializer = key
        self._value_deserializer = value
        self._h = handle
        self._group_id_defined = configs.get(_GROUP_ID) is not None
        # Registered once, before any operation can queue a callback
        # (kafka_consumer_Consumer__set_callbacks_notify).
        self._pending_notify_ref = self._on_pending_notify
        _lib.Consumer__set_callbacks_notify(self._h, self._pending_notify_ref)

    # ---- lifecycle ------------------------------------------------------
    def _use(self) -> _Use:
        """A use of the native handle: ``with self._use() as h:``."""
        return _Use(self)

    def _call(self, fn: Callable[..., _T], *args: Any) -> _T:
        """``fn(handle, *args)`` as one use of the handle."""
        with self._use() as h:
            return fn(h, *args)

    def _is_closed(self) -> bool:
        """Whether ``close()`` has run: a second ``close()`` returns before its
        own argument checks (CLAUDE.md, Python Binding Conventions,
        Implementation over the FFI, Order)."""
        with self._lifecycle:
            return self._state == _CLOSED or self._h == 0

    def _check_open(self) -> None:
        """The closed check alone, ahead of a check Java makes before
        ``acquire()`` (Order); ``_use()`` then makes the concurrency check."""
        if self._is_closed():
            raise IllegalStateError(message=CLOSED_MESSAGE)

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
                # A call left on a closed event loop does not hold the consumer.
                self._release_abandoned()
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
            self._release_abandoned()
            while self._uses:
                # An awaiting call whose loop closes while this waits can never
                # end its use: look for one again at each short wait.
                self._lifecycle.wait(_ABANDONED_RECHECK_S)
                self._release_abandoned()
            handle, self._h = self._h, 0
            reentrant, self._reentrant_handle = self._reentrant_handle, 0
        # What is left for an event loop that has closed is freed.
        self._undelivered.free_closed()
        if reentrant:
            _lib.ConsumerHandle_destroy(reentrant)
        if handle:
            _lib.Consumer_destroy(handle)
        self._listener = None
        self._pending_notify_ref = None
        close_if_defined(self._key_deserializer)
        close_if_defined(self._value_deserializer)

    def _release_abandoned(self) -> bool:
        """End the use of every awaiting call whose event loop has closed: its
        task can never resume, so its use would never end and ``close()`` would
        wait for it for good. Its result is freed through ``_undelivered``, or
        on arrival if it has not arrived. While its native call still runs, the
        core refuses a close (``ConcurrentModificationError``), so the handle is
        not freed under it.

        A coroutine callback deferred to a task on such a loop (``_unacked``)
        can never report either, and the core waits for its report before it
        completes the call that delivered it, refusing ``close()`` meanwhile:
        it is reported failed with ``CancelledError``, as ``asyncio.run``'s
        shutdown would have cancelled its task.

        Called by the guard of every call when another thread holds a use
        (``_Use``), by every awaiting call, by ``close()`` before it begins and
        while it waits for the uses in flight. Whether it reported a callback."""
        for waiter, use in list(self._async_waiters.items()):
            if waiter[0].is_closed() and self._async_waiters.pop(waiter, None) is use:
                use.release()
        # Its task never ends either: the calls on other loops must not wait
        # for it (_wait_on_loop).
        for task in [t for t in self._listener_tasks if t.get_loop().is_closed()]:
            self._listener_tasks.discard(task)
        reported = False
        for callback_id, loop in list(self._unacked.items()):
            if loop.is_closed() and self._unacked.pop(callback_id, None) is loop:
                with self._lifecycle:
                    if self._h:
                        _lib.Consumer__set_callback_result(
                            self._h, callback_id,
                            asyncio.CancelledError("the event loop awaiting the callback closed"))
                        reported = True
        return reported

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

    def _handle(self) -> int:
        """The core's guard-free ``ConsumerHandle`` (created lazily)."""
        with self._lifecycle:
            if not self._reentrant_handle:
                self._reentrant_handle = _lib.Consumer_handle(self._h)
            return self._reentrant_handle

    @contextlib.contextmanager
    def _handle_commit(self) -> Iterator[None]:
        """A blocking ConsumerHandle commit on this thread. Like Java's
        ``commit()``, which runs ``executeCallbacks()`` first, it runs the
        commit callbacks queued behind the running one on this thread while it
        waits (``_handle_commit_running`` tells them so)."""
        thread = threading.get_ident()
        with self._lifecycle:
            self._handle_commit_threads[thread] = self._handle_commit_threads.get(thread, 0) + 1
        try:
            yield
        finally:
            with self._lifecycle:
                count = self._handle_commit_threads.pop(thread) - 1
                if count:
                    self._handle_commit_threads[thread] = count

    def _handle_commit_running(self) -> bool:
        """Whether this thread is inside a blocking ConsumerHandle commit
        (``_handle_commit``), so it cannot run its event loop meanwhile."""
        with self._lifecycle:
            return threading.get_ident() in self._handle_commit_threads


    def _on_pending_notify(self) -> None:
        """The notify hook, registered once per consumer; it runs on a core task
        and only schedules. It wakes every call waiting on this consumer: a
        synchronous one through ``_pending_event``, an awaiting one on its own
        loop."""
        self._pending_event.set()
        for loop, event in list(self._async_waiters):
            try:
                call_soon_on(loop, event.set)
            except RuntimeError:  # its loop is closed
                pass

    def _drain(self, h: int) -> None:
        """Run the queued callbacks on this thread, until none is left
        (``kafka_consumer_Consumer__execute_callbacks``; a nested drain, from
        inside one of them, runs nothing)."""
        while _lib.Consumer__execute_callbacks(h) > 0:
            pass

    # ---- the listener and the commit callback ------------------------------
    def _listener_adapter(self, listener: ConsumerRebalanceListener | None
                          ) -> _ListenerAdapter | None:
        return None if listener is None else _ListenerAdapter(listener, self)

    def _run_listener(self, listener: ConsumerRebalanceListener, method: int,
                      partitions: set[TopicPartition], callback_id: int) -> bool | None:
        """Call the listener's method for a queued invocation, inside the
        callback window, so a consumer operation it issues goes through the
        ConsumerHandle; ``True`` when its awaitable was deferred (see
        ``_ListenerAdapter``). An exception is raised to the trampoline, which
        reports it as a foreign callback error owning the object, read back by
        the swap-back (``_raise_error``).

        On the synchronous consumer an ``async def`` method cannot be awaited:
        its coroutine is closed and the rebalance fails with ``TypeError``."""
        token = self._enter_callback()
        try:
            if method == _REVOKED:
                result: object = listener.on_partitions_revoked(partitions)  # type: ignore[func-returns-value]
            elif method == _ASSIGNED:
                result = listener.on_partitions_assigned(partitions)  # type: ignore[func-returns-value]
            else:
                result = listener.on_partitions_lost(partitions)  # type: ignore[func-returns-value]
            if inspect.isawaitable(result):
                if self._defer_callback(cast("Awaitable[object]", result), callback_id):
                    return True
                close = getattr(result, "close", None)
                if callable(close):
                    close()
                raise TypeError("a coroutine rebalance listener requires an AsyncConsumer")
        finally:
            self._exit_callback(token)
        return None

    def _defer_callback(self, awaitable: Awaitable[object], callback_id: int) -> bool:
        """Await the awaitable a coroutine callback (a listener method or an
        ``OffsetCommitCallback``) returned later, then report it, or the
        exception it raised, to the core under ``callback_id``; whether it was
        deferred. The synchronous consumer cannot (``AsyncConsumer`` can)."""
        return False

    def _take_callback_error(self) -> BaseException | None:
        """The exception a coroutine commit callback raised after its
        invocation was already reported to the core (``AsyncConsumer``'s
        ``_defer_callback`` inside a blocking ConsumerHandle commit), cleared;
        the delivering call raises it once it has ended, or, a ``poll``, keeps
        it for the next awaited call (``_finish_async``)."""
        error, self._callback_error = self._callback_error, None
        return error

    def _raise_error(self, error: int, site: int = _AT_CALL) -> NoReturn:
        """Raise the typed error of an OWNED error handle, a waiting call's
        result, swapping a callback's own exception back in.

        When the error, or its cause chain, is the foreign callback error
        reporting an exception a listener method or an ``OffsetCommitCallback``
        raised during the call, Java would surface that exception, wrapped as
        the call's ``site`` wraps it:

        - ``close`` (``_AT_CLOSE``) throws ``new KafkaException("Failed to close
          kafka consumer", e)`` for every exception but an
          ``InterruptException`` (``AsyncKafkaConsumer.java:1669-1675``): the
          core's wrap, with the original as its ``__cause__``, or a
          ``KeyboardInterrupt`` as it is;
        - elsewhere, as it is when the client reported it unwrapped (code
          ``LOCAL_CALLBACK``: a commit callback's from ``commit`` and
          ``commit_nowait``), or when it is a ``KafkaError`` or a
          ``KeyboardInterrupt``, which Java's ``maybeWrapAsKafkaException``
          leaves unwrapped (``KafkaException``, ``InterruptException``);
          otherwise as the wrapping ``KafkaError`` (a listener's "User
          rebalance callback throws an error"), with the original as its
          ``__cause__``. ``poll``'s wrap of a commit callback's exception,
          Java's ``new KafkaException(t)``, is a ``KafkaError`` whose message
          is that exception's ``toString()``, as Java's."""
        original: BaseException | None = _lib.KafkaError_callback_exception(error)
        code = _lib.KafkaError_code(error)
        if site == _AT_CLOSE:
            typed = from_ffi_error(error)
            if original is None:
                raise typed
            if isinstance(original, KeyboardInterrupt):
                raise original
            raise typed from original
        if (original is not None and code != _LOCAL_CALLBACK
                and not isinstance(original, (KafkaError, KeyboardInterrupt))
                and _wraps_its_source(error)):
            # Java's `new KafkaException(t)` (poll's maybeWrapAsKafkaException):
            # its message is t.toString(), the callback's own exception's.
            _lib.KafkaError_destroy(error)
            raise KafkaError(cause=original) from original
        typed = from_ffi_error(error)
        if original is None:
            raise typed
        if code == _LOCAL_CALLBACK or isinstance(original, (KafkaError, KeyboardInterrupt)):
            raise original
        raise typed from original

    @staticmethod
    def _raise_at(error: BaseException, site: int) -> NoReturn:
        """Raise a callback's exception the core never saw (see
        ``_take_callback_error`` and ``_keep_error``) as the call at
        ``site`` raises one it delivered (``_raise_error``): a
        ``KeyboardInterrupt`` as it is (Java's ``InterruptException``); at
        ``poll`` Java's ``maybeWrapAsKafkaException`` (a ``KafkaError`` as it
        is, else ``new KafkaException(t)``); at ``close`` Java's
        ``new KafkaException("Failed to close kafka consumer", e)``; elsewhere
        as it is, as ``commit`` raises a commit callback's."""
        if isinstance(error, KeyboardInterrupt):
            raise error
        if site == _AT_POLL and not isinstance(error, KafkaError):
            raise KafkaError(cause=error) from error
        if site == _AT_CLOSE:
            raise KafkaError(message=CLOSE_FAILED_MESSAGE, cause=error) from error
        raise error

    def _keep_error(self, error: BaseException | None) -> None:
        """Keep a coroutine commit callback's exception a ``poll`` could not
        raise for the next awaited call (``_raise_deferred_error``), unless an
        older one is kept: Java's first exception wins. ``None`` keeps nothing."""
        if error is not None and self._deferred_error is None:
            self._deferred_error = error

    def _take_deferred_error(self) -> BaseException | None:
        """The kept exception (``_keep_error``), cleared."""
        error, self._deferred_error = self._deferred_error, None
        return error

    def _raise_deferred_error(self, site: int) -> None:
        """Raise the kept exception, if any (``_keep_error``), as the call at
        ``site`` raises a callback's exception it delivers (``_raise_at``), as
        Java raises it from the call running the callback."""
        error = self._take_deferred_error()
        if error is not None:
            self._raise_at(error, site)

    def _wrap_commit_callback(self, callback: Callable[..., object] | None, *,
                              empty_offsets: bool = False
                              ) -> Callable[[dict[Any, Any], int, int], bool | None] | None:
        """Adapt ``callback(offsets, exception)`` to the C commit trampoline,
        which calls it with the committed offsets, a BORROWED error handle and
        the invocation's callback id.

        ``offsets`` is ``None`` where Java passes ``null``: when the commit
        failed (``whenComplete`` on an exceptionally completed future), and for
        an explicit empty ``offsets`` (``empty_offsets``), whose Java
        ``commit()`` returns ``completedFuture(null)``
        (``AsyncKafkaConsumer.commitAsync``).

        It runs on the caller's thread, inside the waiting call that drains it,
        with the callback window open, so a consumer operation it issues goes
        through the ConsumerHandle. It returns ``None`` once the callback
        returned, and the trampoline reports the invocation; ``True`` when the
        consumer deferred the awaitable an ``async def`` callback returned
        (``AsyncConsumer``, see ``_defer_callback``), which reports it once
        awaited. An exception the callback raises, or the awaitable raises once
        awaited, is raised to the trampoline, which reports it to the core as a
        foreign callback error owning the exception: as in Java, where
        ``onComplete`` is called without a ``try``, it propagates out of the
        delivering call (``poll`` wraps it in a ``KafkaError`` unless it is one,
        as Java's ``maybeWrapAsKafkaException``; ``close`` wraps it in
        "Failed to close kafka consumer" unless it is a ``KeyboardInterrupt``;
        ``commit`` and ``commit_nowait`` raise it as it is, see
        ``_raise_error``), and the
        callbacks queued behind it run in a later call. On the synchronous
        consumer an awaitable cannot be awaited: it is closed and the delivering
        call fails with ``TypeError``, as for a coroutine listener."""
        if callback is None:
            return None
        if not callable(callback):
            raise TypeError("callback must be callable")

        def adapter(raw_offsets: dict[Any, Any], error_handle: int,
                    callback_id: int) -> bool | None:
            offsets: dict[TopicPartition, OffsetAndMetadata] | None = None
            if not error_handle and not empty_offsets:
                offsets = {tp: oam for tp, oam in to_offset_map(raw_offsets).items()
                           if oam is not None}
            exception = from_ffi_error(error_handle, owned=False) if error_handle else None
            token = self._enter_callback()
            try:
                result: object = callback(offsets, cast(Exception, exception))
                if inspect.isawaitable(result):
                    if self._defer_callback(cast("Awaitable[object]", result), callback_id):
                        return True
                    close = getattr(result, "close", None)
                    if callable(close):
                        close()
                    raise TypeError("a coroutine OffsetCommitCallback requires an AsyncConsumer")
            finally:
                self._exit_callback(token)
            return None

        return adapter

    # ---- waiting calls ----------------------------------------------------
    def _refuse_inside_callback(self) -> None:
        """A waiting call from inside a callback this consumer is delivering:
        the outer call holds the core's single-owner guard, and the guard's
        refusal of the ``_cb`` submission would be queued behind the running
        callback, where the nested drain never reaches it. Raise the guard's
        error at once (after the closed check)."""
        if self._in_callback():
            with self._use():
                raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)

    def _run_sync(self, submit: Callable[[int, Callable[..., None]], None],
                  resolve: Callable[[tuple[Any, ...]], _T],
                  free: Callable[[tuple[Any, ...]], None], wakeable: bool, *,
                  abandoned: _Abandoned | None = None) -> _T:
        """Submit a ``_cb`` FFI operation (``submit(handle, cb)``) and wait for
        its completion on this thread, running the queued callbacks meanwhile,
        in 100 ms slices so a ``KeyboardInterrupt`` is delivered. Ctrl+C lets
        the call end, frees its result and re-raises ``KeyboardInterrupt``;
        when the operation is one Java's ``wakeup()`` interrupts
        (``wakeable``, see ``_void_spec``) it first wakes the consumer once, so
        the call ends sooner. A result whose records the call then does not
        return (``abandoned``, ``poll``'s ``_fetched_positions``) first seeks
        each of their partitions back to its first one (``_rewind_sync``): the
        core has moved the positions past them, and a later commit would
        commit past records the user never received. So does a Ctrl+C while
        the result is resolved (``poll``'s deserializers run here) before it
        propagates.

        The wake waits for a drain to run to its end, and is sent only if the
        completion has still not arrived: an interrupted drain may have left
        it queued, or it may have run just before the interrupt. A wake the
        core operation still does not consume (Java's ``WakeupTrigger`` leaves
        a wakeup pending when the operation's future is already done) stays
        armed; the binding never leaves its own wakeup behind, so the next
        wakeable call the core ends with ``WakeupError`` for it is submitted
        again (``_own_wake_outcome``)."""
        self._refuse_inside_callback()
        event = self._pending_event
        interrupted: KeyboardInterrupt | None = None
        with self._use() as h:
            # What an earlier call left queued runs first, so the notify of this
            # call's completion is not lost behind it.
            event.clear()
            self._drain(h)
            while True:
                box: list[tuple[Any, ...]] = []

                def cb(*payload: Any, box: list[tuple[Any, ...]] = box) -> None:
                    box.append(payload)
                    event.set()

                submit(h, cb)
                wake_due = False
                mark: int | None = None
                while not box:
                    try:
                        if wake_due:
                            self._drain(h)
                            wake_due = False
                            if not box and wakeable:
                                mark = self._own_wake(h)
                            continue
                        event.wait(0.1)
                        event.clear()
                        self._drain(h)
                    except KeyboardInterrupt as exc:
                        if interrupted is None:
                            interrupted = exc
                            wake_due = True
                payload = box[0]
                if not self._own_wake_outcome(payload, wakeable, mark, interrupted is not None):
                    break
                free(payload)
        if interrupted is not None:
            positions = abandoned(payload) if abandoned is not None else []
            free(payload)
            self._rewind_sync(positions)
            raise interrupted
        if abandoned is None:
            return resolve(payload)
        try:
            return resolve(payload)
        except KeyboardInterrupt:
            # Ctrl+C while the result is resolved (poll's deserializers run on
            # this thread): nothing of it is returned, so its records are
            # sought back as when Ctrl+C lands in the wait.
            self._rewind_sync(abandoned(payload))
            raise

    async def _run_async(self, submit: Callable[[int, Callable[..., None]], None],
                         resolve: Callable[[tuple[Any, ...]], _T],
                         free: Callable[[tuple[Any, ...]], None], wakeable: bool, *,
                         raise_deferred: bool = True, site: int = _AT_CALL,
                         abandoned: _Abandoned | None = None) -> _T:
        """Async peer of ``_run_sync``: awaits the completion on the event loop,
        running the queued callbacks on the loop, and awaiting a coroutine
        listener there. Cancelling the awaiting task lets the call end, frees
        its result and re-raises ``CancelledError``, first waking the consumer
        when the operation is ``wakeable``, as ``_run_sync`` does on Ctrl+C,
        and first awaiting the seeks back of the records it does not return
        (``abandoned``, ``_rewind_async``); so does a ``KeyboardInterrupt`` or
        ``CancelledError`` raised while the result is resolved (``poll``'s
        deserializers).

        It first raises the exception of a coroutine commit callback an
        earlier ``poll`` kept (``raise_deferred``), wrapped as the call at
        ``site`` wraps a callback's exception (``_raise_deferred_error``).

        A coroutine commit callback a blocking ConsumerHandle commit delivered
        during the call may raise after the call ended (``_take_callback_error``):
        the call then raises that exception, wrapped for its ``site``, instead
        of its own result; but ``poll`` (``_AT_POLL``) returns its records, whose
        positions the core has already advanced, and keeps the exception for the
        next awaited call, as a cancelled call does.

        The results of earlier calls left for an event loop that has closed are
        freed first (see :class:`~confluent_kafka._async.Undelivered`)."""
        self._undelivered.free_closed()
        self._release_abandoned()
        if raise_deferred and not self._in_callback():
            self._raise_deferred_error(site)
        self._refuse_inside_callback()
        loop = asyncio.get_running_loop()
        pending = asyncio.Event()
        waiter = (loop, pending)

        def attempt() -> tuple[asyncio.Future[tuple[Any, ...]], Callable[[], bool],
                               Callable[..., None]]:
            fut: asyncio.Future[tuple[Any, ...]] = loop.create_future()
            arrived: list[bool] = []

            def deliver(payload: tuple[Any, ...]) -> None:
                if not fut.done():
                    fut.set_result(payload)
                pending.set()

            def cb(*payload: Any) -> None:
                # The completion has arrived: ``fut`` gets it one loop iteration
                # later (``hand_over``), a wakeup now would find no operation.
                arrived.append(True)
                self._undelivered.hand_over(loop, deliver, payload, free)

            return fut, lambda: bool(arrived), cb

        use = self._use()
        with use as h:
            self._async_waiters[waiter] = use
            try:
                # What an earlier call left queued runs first, so the notify of
                # this call's completion is not lost behind it.
                self._drain(h)
                while True:
                    fut, has_arrived, cb = attempt()
                    submit(h, cb)
                    cancelled, mark = await self._wait_on_loop(
                        h, fut.done, has_arrived, pending, wakeable)
                    payload = fut.result()
                    if not self._own_wake_outcome(payload, wakeable, mark,
                                                  cancelled is not None):
                        break
                    free(payload)
            finally:
                self._async_waiters.pop(waiter, None)
        if cancelled is not None:
            # The call cannot raise a coroutine commit callback's exception it
            # delivered: it is kept for the next awaited call, as a poll keeps
            # it (_finish_async), before the seeks back would take it.
            self._keep_error(self._take_callback_error())
            positions = abandoned(payload) if abandoned is not None else []
            free(payload)
            await self._rewind_async(positions)
            raise cancelled
        if abandoned is None:
            return self._finish_async(payload, resolve, free, site)
        try:
            return self._finish_async(payload, resolve, free, site)
        except (KeyboardInterrupt, asyncio.CancelledError):
            # Raised while the result is resolved (poll's deserializers run on
            # the loop): nothing of it is returned, so its records are sought
            # back as when the wait is cancelled.
            await self._rewind_async(abandoned(payload))
            raise

    def _finish_async(self, payload: tuple[Any, ...], resolve: Callable[[tuple[Any, ...]], _T],
                      free: Callable[[tuple[Any, ...]], None], site: int) -> _T:
        """The result of an awaited call that has ended (see ``_run_async``):
        ``resolve(payload)``, unless a coroutine commit callback raised after
        its invocation was reported."""
        callback_error = self._take_callback_error()
        if callback_error is not None:
            if site == _AT_POLL:
                # Never lose the fetched records: the core has advanced the
                # positions past them.
                self._keep_error(callback_error)
            else:
                free(payload)
                self._raise_at(callback_error, site)
        return resolve(payload)

    async def _wait_on_loop(self, h: int, done: Callable[[], bool],
                            arrived: Callable[[], bool], pending: asyncio.Event,
                            wakeable: bool) -> tuple[asyncio.CancelledError | None, int | None]:
        """Wait on the loop until ``done()``, running the queued callbacks each
        time the notify sets ``pending``, and awaiting the coroutine listener
        methods they defer before going on. The first cancellation is returned
        once the call has ended; it wakes the consumer when the operation is
        ``wakeable``, and only if its completion has not ``arrived()``. Returned
        with it is the mark of that wake (``_own_wake``), ``None`` when none was
        sent: a wake the core operation does not consume stays armed, and the
        binding never leaves its own wakeup behind (``_own_wake_outcome``).

        A cancellation while a deferred callback is awaited wakes at once: the
        core operation that delivered it waits for its report, and checks for a
        wakeup after it unless its own result is ready by then (Java's
        ``pollForFetches`` ``setFetchAction``, also for an expired ``poll``;
        ``commitSync``'s ``setActiveTask`` after the commit callbacks, which
        leaves the wakeup pending when the commit's future is already done, as
        ``commit(offsets={})``'s is). Any other cancellation first drains: the
        completion may already be queued, and a wake sent then would find no
        operation to end."""
        cancelled: asyncio.CancelledError | None = None
        wake_due = False
        mark: int | None = None

        def wake() -> None:
            nonlocal mark
            if wakeable and not arrived():
                mark = self._own_wake(h)

        while True:
            awaiting_callbacks = False
            try:
                if self._listener_tasks:
                    awaiting_callbacks = True
                    await asyncio.wait(set(self._listener_tasks))
                    continue
                if wake_due:
                    wake_due = False
                    self._drain(h)
                    wake()
                    continue
                if done():
                    return cancelled, mark
                await pending.wait()
                pending.clear()
                self._drain(h)
            except asyncio.CancelledError as exc:
                if cancelled is None:
                    cancelled = exc
                    if awaiting_callbacks:
                        wake()
                    else:
                        wake_due = True

    # ---- seeking back records that are not returned -------------------------
    @staticmethod
    def _fetched_positions(payload: tuple[Any, ...]) -> list[tuple[TopicPartition, int]]:
        """The ``abandoned`` of a ``poll`` (``_run_sync``): each partition's
        first fetched record (``fetched_positions``)."""
        return fetched_positions(payload[0])

    def _rewind_sync(self, positions: list[tuple[TopicPartition, int]]
                     ) -> KeyboardInterrupt | None:
        """Seek each partition back to its ``(partition, offset)`` position:
        records a ``poll`` does not return, whose positions the core has moved
        past. Every seek runs to its end: a Ctrl+C during one lets it end, as
        ``_run_sync`` does, and the next ones still run; the first such
        ``KeyboardInterrupt`` is returned, for the caller to raise once the
        positions are back. A seek that fails is logged."""
        interrupted: KeyboardInterrupt | None = None
        for partition, offset in positions:
            try:
                self._run_sync(*self._seek_spec(partition, offset, None))
            except KeyboardInterrupt as exc:
                if interrupted is None:
                    interrupted = exc
            except Exception:  # noqa: BLE001 - the caller's outcome matters
                _LOG.exception("Could not seek %s back to offset %d", partition, offset)
        return interrupted

    async def _rewind_async(self, positions: list[tuple[TopicPartition, int]]
                            ) -> asyncio.CancelledError | None:
        """Async peer of ``_rewind_sync``: a cancellation during a seek lets it
        end, the next ones still run, and the first ``CancelledError`` is
        returned. A deferred commit-callback error is not raised by the seeks
        (``raise_deferred=False``): the outcome of the poll goes first."""
        cancelled: asyncio.CancelledError | None = None
        for partition, offset in positions:
            try:
                await self._run_async(*self._seek_spec(partition, offset, None),
                                      raise_deferred=False)
            except asyncio.CancelledError as exc:
                if cancelled is None:
                    cancelled = exc
            except Exception:  # noqa: BLE001 - the caller's outcome matters
                _LOG.exception("Could not seek %s back to offset %d", partition, offset)
        return cancelled

    # ---- the consumer's own wakeup ------------------------------------------
    def _own_wake(self, h: int) -> int:
        """Wake the consumer to end the call in progress sooner (Ctrl+C,
        cancellation), returning the count of the user's ``wakeup()`` calls at
        that moment, the mark ``_own_wake_outcome`` checks."""
        mark = self._user_wakeups
        _lib.Consumer_wakeup(h)
        return mark

    def _own_wake_outcome(self, payload: tuple[Any, ...], wakeable: bool, mark: int | None,
                          ending: bool) -> bool:
        """Account for the consumer's own wakeup once a wakeable call has ended
        with ``payload``: whether the call must be submitted again (its error
        is the ``WakeupError`` only the consumer's own leftover wakeup raised).

        The core ends a wakeable call with ``WakeupError`` when it consumes a
        wakeup, and rotates it: none is armed after that. The consumer's own
        wake (``mark``, see ``_own_wake``) that the call ended without
        consuming stays armed, as Java's ``WakeupTrigger`` keeps a wakeup that
        found the operation's future already done; unless the user called
        ``wakeup()`` since, it is the consumer's alone, and the next wakeable
        call it ends is submitted again, as if the wakeup had not been sent.
        A call ``ending`` on Ctrl+C or cancellation is never submitted again.

        Submitting again repeats the operation from the start: the core raised
        ``WakeupError`` before the operation's result, and before any callback
        or interceptor it runs for it. A lookup is sent again. A commit has
        taken its offsets by then: its wakeup is checked only after the
        offsets-ready wait, as Java's ``commitSync`` registers it
        (``AsyncKafkaConsumer.java:1136-1138``, ``:1801-1804``), at the wait
        for the pending async commits (before their callbacks run) or for its
        own result (before ``onCommit``). Its offsets are committed again, the
        same ones since nothing is polled in between, and the first commit
        still completes, as Java's woken ``commitSync``'s does."""
        if not wakeable:
            return False
        error = payload[-1] if payload else 0
        woken = bool(error) and _lib.KafkaError_code(error) == WakeupError._ffi_id
        # Under _lifecycle, as _c_wakeup counts the user's wakeup and clears
        # the record: a user wakeup() can never fall between the compare and
        # the store.
        with self._lifecycle:
            if woken:
                left, self._own_wake_left = self._own_wake_left, False
                return left and not ending
            if mark is not None and mark == self._user_wakeups:
                self._own_wake_left = True
        return False

    def _own_wake_raised(self, error: int) -> bool:
        """``_own_wake_outcome`` for a ConsumerHandle call inside a callback,
        which never sends a wake itself: whether ``error``, an OWNED error
        handle, is the ``WakeupError`` only the consumer's own leftover wakeup
        raised; it is then freed and the call is made again."""
        if not self._own_wake_outcome((error,), True, None, False):
            return False
        _lib.KafkaError_destroy(error)
        return True

    # ---- payload handlers ---------------------------------------------------
    def _resolve_void(self, payload: tuple[Any, ...]) -> None:
        if payload[0]:
            self._raise_error(payload[0])

    @staticmethod
    def _free_void(payload: tuple[Any, ...]) -> None:
        if payload and payload[0]:
            _lib.KafkaError_destroy(payload[0])

    def _resolve_value(self, convert: Callable[[Any], Any]
                       ) -> Callable[[tuple[Any, ...]], Any]:
        """The resolve of a ``cb(value, error)`` completion: ``convert(value)``,
        or the error raised."""
        def resolve(payload: tuple[Any, ...]) -> Any:
            value, error = payload
            if error:
                self._raise_error(error)
            return convert(value)
        return resolve

    @staticmethod
    def _free_value(payload: tuple[Any, ...]) -> None:
        if payload[1]:
            _lib.KafkaError_destroy(payload[1])

    # ---- operation specs: (submit, resolve, free, wakeable) -----------------
    # ``wakeable``: the core operation is one Java's ``wakeup()`` interrupts,
    # because ``AsyncKafkaConsumer`` registers its future with the
    # ``WakeupTrigger`` (``poll``, ``commitSync``, ``position``, ``committed``,
    # ``partitionsFor``, ``listTopics``). Only those are woken on Ctrl+C or
    # cancellation. Any other call (``commitAsync``, ``subscribe``, ``assign``,
    # ``seek``, ``pause``, ``offsetsForTimes``, ``close``, ...) never consumes a
    # wakeup, so waking the consumer during it would not end it sooner and would
    # leave the wakeup armed: the next wakeable call would raise ``WakeupError``
    # although the user never called ``wakeup()``.
    #
    # A wakeable operation consumes a wakeup sent at any time before it ends,
    # its expired ``poll`` included: Java's ``pollForFetches`` calls
    # ``wakeupTrigger.setFetchAction`` after an empty ``collectFetch()`` also
    # when no time is left (``AsyncKafkaConsumer.java:1964-2016``), so a wakeup
    # sent while a commit callback the poll delivers runs is raised by that
    # poll. A wake does not end the operation, and stays armed, as a user's
    # ``wakeup()`` there does in Java, when the operation's result is ready
    # before the core checks for it: Java's ``setActiveTask`` leaves a wakeup
    # pending for a future already done (``commit(offsets={})``, a ``commit``
    # whose earlier async commits have completed), a ``poll`` returns records
    # already buffered when its callbacks end (Java's first ``collectFetch()``
    # returns them before any wakeup check), and the core may complete just
    # after its last check. The consumer never leaves its own wake behind: the
    # next wakeable call the core ends with ``WakeupError`` for it is submitted
    # again (``_own_wake_outcome``).
    def _void_spec(self, fn: Callable[..., None], *args: Any,
                   wakeable: bool = False) -> tuple[Any, Any, Any, bool]:
        return (lambda h, cb: fn(h, *args, cb), self._resolve_void, self._free_void, wakeable)

    def _value_spec(self, fn: Callable[..., None], convert: Callable[[Any], Any],
                    *args: Any, wakeable: bool = False) -> tuple[Any, Any, Any, bool]:
        return (lambda h, cb: fn(h, *args, cb), self._resolve_value(convert), self._free_value,
                wakeable)

    def _poll_spec(self, timeout_ms: int) -> tuple[Any, Any, Any, bool]:
        def convert(records: Any) -> Deserialized:
            # The native batch owns the fetched bytes; the deserializers run on
            # this thread over memoryviews into it (consumer-threading.md §27).
            return deserialize_batch(records, key_deserializer=self._key_deserializer,
                                     value_deserializer=self._value_deserializer)

        return self._value_spec(_lib.Consumer_poll_cb, convert, timeout_ms, wakeable=True)

    def _subscribe_topics_spec(self, topics: list[str],
                               listener: ConsumerRebalanceListener | None
                               ) -> tuple[Any, Any, Any, bool]:
        return self._void_spec(_lib.Consumer_subscribe_cb, topics,
                               self._listener_adapter(listener))

    def _subscribe_pattern_spec(self, pattern: str, listener: ConsumerRebalanceListener | None
                                ) -> tuple[Any, Any, Any, bool]:
        return self._void_spec(_lib.Consumer_subscribe_pattern_cb, pattern,
                               self._listener_adapter(listener))

    def _seek_spec(self, partition: TopicPartition, offset: int | None,
                   offset_and_metadata: OffsetAndMetadata | None) -> tuple[Any, Any, Any, bool]:
        if offset_and_metadata is not None:
            epoch = offset_and_metadata.leader_epoch()
            return self._void_spec(
                _lib.Consumer_seek_with_metadata_cb, partition.topic(),
                partition.partition(), offset_and_metadata.offset(),
                -1 if epoch is None else epoch, offset_and_metadata.metadata())
        return self._void_spec(_lib.Consumer_seek_cb, partition.topic(),
                               partition.partition(), offset)

    def _commit_spec(self, offsets: Any, timeout_ms: int = _NO_TIMEOUT
                     ) -> tuple[Any, Any, Any, bool]:
        spec = None if offsets is None else offsets_to_spec(offsets)
        return self._void_spec(_lib.Consumer_commit_sync_cb, spec, timeout_ms, wakeable=True)

    def _commit_nowait_spec(self, spec: Any, adapter: Any) -> tuple[Any, Any, Any, bool]:
        # Not wakeable: Java's commitAsync waits for its offsets through
        # ConsumerUtils.getResult, without the WakeupTrigger
        # (AsyncKafkaConsumer.java:1126-1140), so it never throws WakeupException.
        return self._void_spec(_lib.Consumer_commit_async_cb, spec, adapter)

    def _position_spec(self, partition: TopicPartition, timeout_ms: int = _NO_TIMEOUT
                       ) -> tuple[Any, Any, Any, bool]:
        return self._value_spec(_lib.Consumer_position_cb, int, partition.topic(),
                                partition.partition(), timeout_ms, wakeable=True)

    def _committed_spec(self, partitions: list[TopicPartition], timeout_ms: int = _NO_TIMEOUT
                        ) -> tuple[Any, Any, Any, bool]:
        def convert(raw: Any) -> dict[TopicPartition, OffsetAndMetadata | None]:
            result = to_offset_map(raw or {})
            # Java maps a partition without a committed offset to null.
            for tp in partitions:
                result.setdefault(tp, None)
            return result

        return self._value_spec(_lib.Consumer_committed_cb, convert, tp_to_spec(partitions),
                                timeout_ms, wakeable=True)

    def _offsets_for_times_spec(self, timestamps_to_search: dict[TopicPartition, int],
                                timeout_ms: int = _NO_TIMEOUT) -> tuple[Any, Any, Any, bool]:
        def convert(raw: Any) -> dict[TopicPartition, OffsetAndTimestamp | None]:
            result = to_offset_and_timestamp_map(raw or {})
            # Java maps a partition without an offset for its timestamp to null.
            for tp in timestamps_to_search:
                result.setdefault(tp, None)
            return result

        return self._value_spec(_lib.Consumer_offsets_for_times_cb, convert,
                                timestamps_to_spec(timestamps_to_search), timeout_ms)

    def _long_offsets_spec(self, fn: Callable[..., None], partitions: list[TopicPartition],
                           timeout_ms: int = _NO_TIMEOUT) -> tuple[Any, Any, Any, bool]:
        return self._value_spec(fn, lambda raw: to_long_map(raw or {}), tp_to_spec(partitions),
                                timeout_ms)

    def _partitions_for_spec(self, topic: str, timeout_ms: int = _NO_TIMEOUT
                             ) -> tuple[Any, Any, Any, bool]:
        return self._value_spec(_lib.Consumer_partitions_for_cb,
                                lambda raw: to_partition_info_list(raw or []), topic, timeout_ms,
                                wakeable=True)

    def _list_topics_spec(self, timeout_ms: int = _NO_TIMEOUT) -> tuple[Any, Any, Any, bool]:
        return self._value_spec(_lib.Consumer_list_topics_cb,
                                lambda raw: to_topics_map(raw or {}), timeout_ms, wakeable=True)

    def _close_spec(self, timeout_ms: int, operation_code: int) -> tuple[Any, Any, Any, bool]:
        def resolve(payload: tuple[Any, ...]) -> None:
            if payload[0]:
                self._raise_error(payload[0], _AT_CLOSE)

        # Not wakeable: Java's close disables the wakeups.
        return (lambda h, cb: _lib.Consumer_close_cb(h, timeout_ms, operation_code, cb),
                resolve, self._free_void, False)

    # ---- calls that do not wait (both families) ---------------------------
    @staticmethod
    def _read(pair: tuple[Any, int]) -> Any:
        """The value of a state read's ``(value, error)`` pair. The core's
        single-owner guard refuses the read while a call on this consumer is in
        flight, as Java's ``acquire()`` does, and the error raised here is its
        ``ConcurrentModificationError`` (a read from inside a callback goes
        through the ConsumerHandle instead)."""
        value, error = pair
        raise_if_error(error)
        return value

    def _c_assignment(self) -> set[TopicPartition]:
        with self._use() as h:
            raw = (_lib.ConsumerHandle_assignment(self._handle()) if self._in_callback()
                   else self._read(_lib.Consumer_assignment(h)))
        return {TopicPartition(topic=t, partition=p) for (t, p) in raw}

    def _c_subscription(self) -> set[str]:
        # Not on the ConsumerHandle (KIP-1306's RebalanceConsumer does not
        # carry subscription()): from inside a callback the read reports the
        # busy consumer, as Java's acquire() does from another thread.
        with self._use() as h:
            raw = self._read(_lib.Consumer_subscription(h))
        return set(raw)

    def _c_paused(self) -> set[TopicPartition]:
        with self._use() as h:
            raw = (_lib.ConsumerHandle_paused(self._handle()) if self._in_callback()
                   else self._read(_lib.Consumer_paused(h)))
        return {TopicPartition(topic=t, partition=p) for (t, p) in raw}

    def _c_metrics(self) -> dict[MetricName, Metric]:
        # Java's metrics() takes no acquire(): the core answers while a call is
        # in flight.
        raw = self._call(_lib.Consumer_metrics)
        return to_metrics_map(raw)

    def _c_current_lag(self, topic_partition: TopicPartition) -> int | None:
        with self._use() as h:
            lag: int | None = self._read(_lib.Consumer_current_lag(
                h, topic_partition.topic(), topic_partition.partition()))
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

    def _timeout_ms(self, timeout: Duration | None, *, timer: bool = False,
                    group: bool = False, handle_timed: bool = False) -> int:
        """``overload_timeout_ms`` for a ``Duration`` overload, in Java's order
        (CLAUDE.md, Python Binding Conventions, Implementation over the FFI,
        Order); ``_NO_TIMEOUT`` when ``timeout`` is not given.

        The closed check comes first. Inside a callback, an overload whose
        ``ConsumerHandle`` form has no timeout (every one but ``position``,
        ``handle_timed``) is refused with ``ConcurrentModificationError``, as
        ``partitions_for`` and ``list_topics`` are refused without one
        (``_refuse_inside_callback``) *(deviation: inside a listener the core's
        handle has no timed form; goes when it has)*. Then, inside one use of
        the handle, as Java checks the timeout after ``acquireAndEnsureOpen()``:
        another thread inside the consumer raises
        ``ConcurrentModificationError``; for a negative timeout, a consumer
        without a ``group.id`` raises ``InvalidGroupIdError`` where Java's body
        checks it before the timeout (``group``: ``commitSync``, ``committed``),
        then the negative timeout ``IllegalArgumentError``."""
        if timeout is None:
            return _NO_TIMEOUT
        self._check_open()
        if not handle_timed and self._in_callback():
            with self._use():
                raise ConcurrentModificationError(message=CONCURRENT_MESSAGE)
        with self._use():
            millis = _duration_millis(timeout)
            if millis < 0 and group and not self._group_id_defined:
                raise InvalidGroupIdError(message=GROUP_ID_NOT_DEFINED_MESSAGE)
            return overload_timeout_ms(timeout, timer=timer)

    def _check_not_null(self, op: str, value: object) -> None:
        """Java's null check of ``op``'s argument (``_NULL_ARGUMENT``), after
        the closed check (CLAUDE.md, Python Binding Conventions, Implementation
        over the FFI, Order) and, where Java makes it after ``acquire()``, the
        concurrency check."""
        checked = _NULL_ARGUMENT.get(op)
        if value is None and checked is not None:
            error, message, before_acquire = checked
            if before_acquire:
                self._check_open()
                raise error(message=message)
            with self._use():
                raise error(message=message)

    def _c_group_metadata(self) -> ConsumerGroupMetadata:
        # Java's groupMetadata() calls throwIfGroupIdNotDefined(); the core's
        # returns a stub without a group.id, so the binding checks it.
        if not self._group_id_defined:
            with self._use():
                pass
            raise InvalidGroupIdError(message=GROUP_ID_NOT_DEFINED_MESSAGE)
        with self._use() as h:
            native = self._read(_lib.Consumer_group_metadata(h))
        return ConsumerGroupMetadata._of(
            group_id=native.group_id, generation_id=native.generation_id,
            member_id=native.member_id, group_instance_id=native.group_instance_id,
            native=native)


    def _c_commit_nowait(self, offsets: Any, callback: Callable[..., object] | None) -> None:
        """Java's ``commitAsync()`` / ``commitAsync(callback)`` /
        ``commitAsync(offsets, callback)``: returns once the commit is sent; the
        callback runs inside a later call on this consumer, on its thread.

        The core's commit waits for the offsets to commit and processes the
        background events meanwhile (``consumer-threading.md`` §31; Java's
        ``commitAsync`` does not), so a listener callback can be queued during
        it: like every waiting call it submits the ``_cb`` form and runs the
        queued callbacks on this thread while it waits.

        Inside a callback, the commit goes through the ConsumerHandle
        (``kafka_consumer_ConsumerHandle_commit_async``, or its
        ``_with_callback`` / ``_with_offsets_callback`` forms), whose plain
        entry point first runs the commit callbacks queued behind the running
        one on this thread, as Java's ``commit()`` runs ``executeCallbacks()``
        first; this commit's own callback runs inside a later call. Java has
        no ``commitAsync(Map)``: given offsets without a callback are
        ``commitAsync(offsets, null)``, the ``_with_offsets_callback`` form
        with a no-op registration."""
        if self._in_callback():
            with self._use() as h:
                spec = None if offsets is None else offsets_to_spec(offsets)
                adapter = self._wrap_commit_callback(
                    callback, empty_offsets=offsets is not None and not offsets)
                with self._handle_commit():
                    if adapter is None and spec is None:
                        error = _lib.ConsumerHandle_commit_async(self._handle())
                    else:
                        error = _lib.ConsumerHandle_commit_async_callback(
                            h, self._handle(), spec, adapter)
                if error:
                    self._raise_error(error)
                return
        with self._use():
            spec = None if offsets is None else offsets_to_spec(offsets)
            adapter = self._wrap_commit_callback(
                callback, empty_offsets=offsets is not None and not offsets)
        self._run_sync(*self._commit_nowait_spec(spec, adapter))

    def _c_wakeup(self) -> None:
        """Java's ``wakeup()``: callable from any thread; a no-op once closed."""
        with self._lifecycle:
            if not self._h:
                return
            self._uses += 1
            h = self._h
            # The wakeup armed from now on is the user's (see _own_wake_left).
            self._user_wakeups += 1
            self._own_wake_left = False
        try:
            _lib.Consumer_wakeup(h)
        finally:
            with self._lifecycle:
                self._uses -= 1
                if self._uses == 0:
                    self._lifecycle.notify_all()

    # ---- a consumer operation from inside a callback (the ConsumerHandle) -----
    def _reentrant(self, op: str, *args: Any) -> Any:
        """Run ``op`` through the guard-free ConsumerHandle. Its plain forms
        are called here, inside the drain that runs the callback: they wait on
        the consumer's background task, which keeps running while a callback
        is pending (``consumer-threading.md`` §31).

        Of the ``Duration`` overloads the handle has only ``position``'s
        (``kafka_consumer_ConsumerHandle_position_with_timeout``); the others
        are refused inside a callback when a ``timeout`` is given
        (``_timeout_ms``), as ``partitions_for`` and ``list_topics``, which
        have no handle form, are refused."""
        handle = self._handle()
        if op == "commit":
            (offsets,) = args
            spec = None if offsets is None else offsets_to_spec(offsets)
            with self._handle_commit():
                error = _lib.ConsumerHandle_commit_sync(handle, spec)
                while self._own_wake_raised(error):
                    error = _lib.ConsumerHandle_commit_sync(handle, spec)
            if error:
                self._raise_error(error)
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
        if op in ("pause", "resume", "seek_to_beginning", "seek_to_end"):
            (partitions,) = args
            raise_if_error(getattr(_lib, "ConsumerHandle_" + op)(handle, tp_to_spec(partitions)))
            return None
        if op == "position":
            partition, timeout_ms = args
            position, error = _lib.ConsumerHandle_position(
                handle, partition.topic(), partition.partition(), timeout_ms)
            while self._own_wake_raised(error):
                position, error = _lib.ConsumerHandle_position(
                    handle, partition.topic(), partition.partition(), timeout_ms)
            raise_if_error(error)
            return int(position)
        if op == "committed":
            (partitions,) = args
            raw, error = _lib.ConsumerHandle_committed(handle, tp_to_spec(partitions))
            while self._own_wake_raised(error):
                raw, error = _lib.ConsumerHandle_committed(handle, tp_to_spec(partitions))
            raise_if_error(error)
            result = to_offset_map(raw or {})
            for tp in partitions:
                result.setdefault(tp, None)
            return result
        if op in ("beginning_offsets", "end_offsets"):
            (partitions,) = args
            raw, error = getattr(_lib, "ConsumerHandle_" + op)(handle, tp_to_spec(partitions))
            raise_if_error(error)
            return to_long_map(raw or {})
        if op == "offsets_for_times":
            (timestamps,) = args
            raw, error = _lib.ConsumerHandle_offsets_for_times(handle, timestamps_to_spec(timestamps))
            raise_if_error(error)
            result_t = to_offset_and_timestamp_map(raw or {})
            for tp in timestamps:
                result_t.setdefault(tp, None)
            return result_t
        raise AssertionError(op)

    def _reentrant_use(self, op: str, *args: Any) -> Any:
        """``_reentrant`` as one use of the handle."""
        with self._use():
            return self._reentrant(op, *args)
