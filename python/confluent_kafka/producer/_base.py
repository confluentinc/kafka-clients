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

"""State and helpers shared by the FFI-backed producers (private).

``Producer`` / ``KafkaProducer`` and ``AsyncProducer`` / ``AsyncKafkaProducer``
call the C FFI through the C extension (``_confluentkafka``). Every operation
uses the C API's queued ``_cb`` form (CLAUDE.md §4, "Async variants of blocking
methods"): a send is ``kafka_producer_Producer_send_with_callback_cb``,
which hands the record to the producer on the calling thread when it can take
it at once and otherwise queues it on the core's submission task, which gives a
record it still cannot hand over a task of its own to wait on; a flush, close
or transaction-control call is its ``_cb`` twin. The completions -- the
``Callback`` of each record, the handover of each record that waited and the
result of each operation -- are queued in the client's callback vector and run
by whoever calls ``kafka_producer_Producer__execute_callbacks``: the sync
producer's completion thread, or the event loop of the async producer, which
the ``__set_callbacks_notify`` hook wakes (the hook only schedules, never runs a
callback). The handover of a record taken, or refused, at once is reported on
the calling thread instead, inside the send. Both families reuse the same
entry points: a record's completion completes the same Python state (a
``_FutureState``: a ``_KafkaFutureImpl`` on the sync producer, the record's
own future on the asyncio one), and they differ only in the future ``send()``
hands out over it (``KafkaFuture`` / ``AsyncKafkaFuture``) and where the
user's callback
runs (CLAUDE.md, Python Binding Conventions, Futures, Implementation over the
FFI and Threads and callbacks).

Backpressure is Java's: ``send()`` returns only once its record is with the
producer, as Java's returns once ``doSend`` appended it. The C API's send
runs the producer's ``try_send`` on the calling thread first and takes the
record at once in the common case (its topic's metadata known, the buffer with
room). Otherwise the record is queued for the submission task, which hands it
to the producer on a task of its own, where the metadata wait and the wait for
``buffer.memory`` space are bounded by ``max.block.ms``, as in Java. ``send()``
waits for that handover on an internal future, which the handover completion
resolves, then returns the record's own future, which the record's delivery
callback completes, or raises what Java's ``doSend`` rethrows. So one caller's
records reach the producer in sending order, a record not yet in the buffer
has a caller waiting for it, at most one per sending thread or task, and each
waits at most ``max.block.ms`` whatever the number of callers, as with Java's
blocking ``send()``. The wait is on a core task, not in a blocking native
call: a sync ``send()`` sees Ctrl+C, raising it once the record is handed
over, and an async one does not block its loop.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._config import duration_to_ms, prepare
from confluent_kafka._errors import from_ffi_error
# The slice waits live in the shared ``_wait`` module; re-exported for the producers.
from confluent_kafka._wait import WAIT_SLICE_S as WAIT_SLICE_S
from confluent_kafka._wait import result_sliced as result_sliced
from confluent_kafka._wait import wait_sliced as wait_sliced
from confluent_kafka.common.errors.timeout_error import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.errors.unsupported_version_error import UnsupportedVersionError
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.common.metric_name import MetricName
from confluent_kafka.common.node import Node
from confluent_kafka.common.partition_info import PartitionInfo
from confluent_kafka.common.serialization import bytes_serializer
from confluent_kafka.common.serialization._supply import close_if_defined, resolve_serde
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.common.kafka_future import _FutureState
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.serialization import Serializer
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from .producer_record import ProducerRecord

# Java's KafkaProducer.throwIfProducerClosed() message.
CLOSED_MESSAGE = "Cannot perform operation after producer has been closed"

# Java's KafkaProducer.flush() message for a flush from a delivery callback.
FLUSH_IN_CALLBACK_MESSAGE = ("KafkaProducer.flush() invocation inside a callback is not "
                             "permitted because it may lead to deadlock.")

# Java's KafkaProducer.throwIfInvalidGroupMetadata() message for a null argument;
# the core cannot receive a null, so the binding checks it.
NULL_GROUP_METADATA_MESSAGE = "Consumer group metadata could not be null"

# A ConsumerGroupMetadata without the core's handle (a mock consumer's): the
# FFI takes only the handle, so the binding cannot send it.
NO_GROUP_METADATA_HANDLE_MESSAGE = ("Only a KafkaConsumer's group_metadata() can be sent to "
                                    "a KafkaProducer; this one comes from a mock consumer")

# Java's ProducerConfig keys of a serializer given through the config route.
_KEY_SERIALIZER = "key.serializer"
_VALUE_SERIALIZER = "value.serializer"

# Java's close() is close(Duration.ofMillis(Long.MAX_VALUE)).
LONG_MAX_VALUE = (1 << 63) - 1

# Java's RecordAccumulator.append() message for a send that races close.
CLOSED_WHILE_SENDING_MESSAGE = "Producer closed while send in progress"


def closed_while_sending() -> KafkaError:
    """What a send raises when ``close()`` began while it ran, as Java's
    ``RecordAccumulator.append`` throws ``KafkaException("Producer closed while
    send in progress")``. Built only then, so a send that goes through
    allocates no error."""
    return KafkaError(message=CLOSED_WHILE_SENDING_MESSAGE)

# ProducerConfig.MAX_BLOCK_MS_CONFIG and its default (60 s).
_MAX_BLOCK_MS = "max.block.ms"
_DEFAULT_MAX_BLOCK_MS = 60000

# The message of a wait from a delivery callback that ran out of max.block.ms:
# the completion it waits for can only run on the thread the callback blocks.
CALLBACK_WAIT_TIMEOUT_MESSAGE = ("Timeout expired after {ms}ms while awaiting {operation} from "
                                 "a send() callback, which blocks the producer's completion "
                                 "thread")


class _SnapshotMetric:
    """A point-in-time metric value returned from ``metrics()``: the
    ``Metric`` protocol over the ``MetricName`` and the value the FFI read
    (``metric_value()`` is Java's ``Object``)."""

    __slots__ = ("_name", "_value")

    def __init__(self, name: MetricName, value: object) -> None:
        self._name = name
        self._value = value

    def metric_name(self) -> MetricName:
        return self._name

    def metric_value(self) -> Any:
        return self._value


def to_metrics_map(raw: list[dict[str, Any]] | None) -> dict[MetricName, Metric]:
    """``dict[MetricName, Metric]`` from the FFI metrics snapshot, a list of
    ``{name, group, description, tags, value, kind}`` dicts (the key is the
    whole ``MetricName``, as Java's ``Map<MetricName, Metric>``)."""
    out: dict[MetricName, Metric] = {}
    for entry in raw or ():
        name = MetricName(name=entry["name"], group=entry["group"],
                          description=entry["description"], tags=entry["tags"])
        out[name] = _SnapshotMetric(name, entry["value"])
    return out


def _to_node(raw: tuple[int, str, int, str | None]) -> Node:
    return Node(id=raw[0], host=raw[1], port=raw[2], rack=raw[3])


def to_partition_info(raw: tuple[Any, ...]) -> PartitionInfo:
    topic, partition, leader, replicas, isr, offline = raw
    return PartitionInfo(
        topic=topic,
        partition=partition,
        leader=None if leader is None else _to_node(leader),
        replicas=tuple(_to_node(n) for n in replicas),
        in_sync_replicas=tuple(_to_node(n) for n in isr),
        offline_replicas=tuple(_to_node(n) for n in offline),
    )


def offsets_to_spec(
        offsets: Mapping[TopicPartition, OffsetAndMetadata]) -> list[tuple[str, int, int, int, str]]:
    """``{TopicPartition: OffsetAndMetadata}`` as the ``(topic, partition,
    offset, leader_epoch, metadata)`` tuples the FFI takes (a missing leader
    epoch is -1)."""
    return [(tp.topic(), tp.partition(), oam.offset(),
             -1 if oam.leader_epoch() is None else cast(int, oam.leader_epoch()),
             oam.metadata())
            for tp, oam in offsets.items()]


def group_metadata_handle(group_metadata: ConsumerGroupMetadata) -> object:
    """The core's handle a ``KafkaConsumer``'s ``group_metadata()`` carries,
    which the FFI takes in place of the four fields. Metadata without one (a
    mock consumer's) raises ``UnsupportedVersionError``, as the FFI has no
    constructor to build a handle from the fields; Java sends any
    ``ConsumerGroupMetadata``."""
    native = group_metadata._native
    if native is None:
        raise UnsupportedVersionError(message=NO_GROUP_METADATA_HANDLE_MESSAGE)
    return native


def close_timeout_ms(timeout: Duration | None) -> int | None:
    """``close(Duration)``'s timeout in milliseconds (``None`` for ``close()``);
    a negative one raises ``IllegalArgumentError`` with Java's message before
    anything else, as ``KafkaProducer.close(Duration, boolean)`` does."""
    return None if timeout is None else duration_to_ms(timeout, default_ms=0)


def serialize(serializer: Serializer[Any], topic: str, data: object,
              headers: object) -> bytes | None:
    """One serializer call, Java's ``serialize(topic, headers, data)``: a
    ``None`` ``data`` is passed too (the serializer decides what Java's null
    maps to), and a ``None`` result is a null key or value."""
    out = serializer(topic, data, cast(Any, headers))
    if out is None or type(out) is bytes:
        return out
    if isinstance(out, (bytes, bytearray, memoryview)):
        return bytes(out)
    raise TypeError(f"a serializer must return bytes or None, not {type(out).__name__}")


def raise_if_error(error: int) -> None:
    """Raise the typed error of an OWNED FFI error handle (0 is success)."""
    if error:
        raise from_ffi_error(error)


def free_error(error: int) -> None:
    """Free an OWNED FFI error handle nobody raises (0 is none)."""
    if error:
        _lib.KafkaError_destroy(error)


def wait_futures(futures: list[_FutureState[Any]]) -> None:
    """Wait (as :func:`wait_sliced`) until every record future of ``futures``
    (the Python state under each ``KafkaFuture`` a ``send()`` returned) is
    done: the internal wait of ``flush()`` and ``commit_transaction()``, never
    a public ``get()``."""
    if not futures:
        return
    all_done = threading.Event()
    lock = threading.Lock()
    remaining = len(futures)

    def one_done(_future: _FutureState[Any]) -> None:
        nonlocal remaining
        with lock:
            remaining -= 1
            last = remaining == 0
        if last:
            all_done.set()

    for future in futures:
        future._add_listener(one_done)
    wait_sliced(all_done)


class Completion:
    """The completion of one queued (``_cb``) operation, waited for on the
    calling thread: the extension calls it with the operation's payload (one
    OWNED error handle, or a list and one) from the thread that runs the
    producer's callbacks, and :meth:`wait` returns that payload.

    ``free`` releases a payload nobody takes: one that arrives after the wait
    gave up, or that an interrupted wait leaves behind."""

    __slots__ = ("_done", "_lock", "_payload", "_abandoned", "_free")

    def __init__(self, free: Callable[[tuple[Any, ...]], None]) -> None:
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._payload: tuple[Any, ...] | None = None
        self._abandoned = False
        self._free = free

    def __call__(self, *payload: Any) -> None:
        with self._lock:
            if self._abandoned:
                abandoned = True
            else:
                abandoned = False
                self._payload = payload
        if abandoned:
            self._free(payload)
        else:
            self._done.set()

    def wait(self, deadline: float | None = None,
             on_interrupt: Callable[[], None] | None = None) -> tuple[Any, ...] | None:
        """The payload, waited for in slices (see :func:`wait_sliced`); ``None``
        when ``deadline`` passed first, the late payload then being freed. A
        Ctrl+C is raised once the payload is in, which is then freed."""
        try:
            arrived = wait_sliced(self._done, deadline, on_interrupt)
        except KeyboardInterrupt:
            with self._lock:
                payload, self._payload = self._payload, None
            if payload is not None:
                self._free(payload)
            raise
        with self._lock:
            if not arrived and self._payload is None:
                self._abandoned = True
                return None
            payload, self._payload = self._payload, None
        return payload


def free_error_payload(payload: tuple[Any, ...]) -> None:
    """Free the ``(error,)`` payload of a void operation."""
    free_error(payload[-1])


class _Use:
    """One call into the native producer: while it runs, ``close()`` does not
    free the handle (see ``_ProducerState``)."""

    __slots__ = ("_state", "_error")

    def __init__(self, state: _ProducerState,
                 error: Callable[[], BaseException] | None) -> None:
        self._state = state
        self._error = error

    def __enter__(self) -> int:
        state = self._state
        with state._lifecycle_lock:
            if state._closed:
                raise self._error() if self._error is not None else IllegalStateError(
                    message=CLOSED_MESSAGE)
            state._uses += 1
        return state._c_producer

    def __exit__(self, *exc: object) -> None:
        state = self._state
        with state._lifecycle_lock:
            state._uses -= 1
            # Only a close() waits for the uses to end, and it sets _closed
            # under this lock before it waits: with none begun, nobody waits.
            if state._uses == 0 and state._closed:
                state._lifecycle.notify_all()


class _ProducerState:
    """The native producer handle, its serializers and the closed flag.

    The handle lives until ``close()`` frees it, and the producer is
    thread-safe: every native call runs inside ``_use()``, which counts it under
    ``_lifecycle`` and refuses to start once ``_closed`` is set. ``close()`` sets
    ``_closed`` under the same lock (so exactly one ``close()`` tears the
    producer down) and waits for the calls in flight to end before it frees the
    handle. A use covers only the native call itself, never a wait for a
    completion, so a ``close()`` from a delivery callback waits for nothing that
    the callback's thread must first run. The one native call that waits in the
    core is a ``send()`` from a delivery callback (see ``Producer.send``), which
    the core's close wakes.
    """

    def __init__(self) -> None:
        self._c_producer: int = 0
        self._closed = False
        # The Condition's own lock, taken directly on the per-send path (see
        # _Use): the same lock, without the Condition's Python-level wrappers.
        self._lifecycle_lock = threading.RLock()
        self._lifecycle = threading.Condition(self._lifecycle_lock)
        self._uses = 0
        # Futures of the records sent and not yet completed: flush() waits for
        # the ones sent before it (their callbacks have run by then, as in Java).
        self._futures: set[_FutureState[Any]] = set()
        # Bound once: every completion untracks its record's future with it.
        self._discard_future = self._futures.discard
        self._key_serializer: Serializer[Any] = bytes_serializer()
        self._value_serializer: Serializer[Any] = bytes_serializer()
        # The use of every send(), built once: a _Use holds no per-use state.
        self._send_use = _Use(self, closed_while_sending)
        # ProducerConfig max.block.ms: what bounds a wait that cannot be
        # serviced (see CALLBACK_WAIT_TIMEOUT_MESSAGE).
        self._max_block_ms = _DEFAULT_MAX_BLOCK_MS
        # Set (under _lifecycle) once Producer_destroy has run: the callback
        # pump must not touch the handle after that.
        self._destroyed = False

    def _notify_hook(self) -> Callable[[], None] | None:
        """The callable the extension calls from the C API's
        ``__set_callbacks_notify`` hook (on any thread, with the GIL), or
        ``None`` for a hook that only signals the condition ``Producer_poll``
        waits on."""
        return None

    def _after_start(self) -> None:
        """Run once the native producer is built (the sync producer starts its
        completion thread here)."""

    def _use(self, error: Callable[[], BaseException] | None = None) -> _Use:
        """A call into the native producer: ``with self._use() as c:`` gives
        the handle, or raises what ``error()`` builds (by default Java's closed
        ``IllegalStateError``) once ``close()`` has begun. ``error`` builds the
        error only then, so a call that goes through allocates none."""
        return _Use(self, error)

    def _call(self, fn: Callable[..., Any], *args: Any) -> Any:
        """``fn(handle, *args)`` as one use of the handle."""
        with self._use() as c_producer:
            return fn(c_producer, *args)

    def _begin_close(self) -> bool:
        """Set ``_closed``; whether this call did (and so must tear down)."""
        with self._lifecycle:
            if self._closed:
                return False
            self._closed = True
            return True

    def _wait_for_uses(self) -> None:
        """Wait until no call into the native producer is in flight (none can
        start: ``_closed`` is set)."""
        with self._lifecycle:
            while self._uses:
                self._lifecycle.wait()

    def _start(self, configs: dict[str, Any], key_serializer: Serializer[Any] | None,
               value_serializer: Serializer[Any] | None) -> None:
        """Java's ``KafkaProducer(configs, keySerializer, valueSerializer)``:
        parse ``configs``, take the serializers (an argument wins over the
        config key; neither means ``bytes_serializer()``), build the core
        producer (which reports the keys it does not know). A given serializer
        argument replaces its config key, which is then not parsed
        (``ProducerConfig.appendSerializerToConfig``). A construction failure
        raises its typed error (Java's ``KafkaException("Failed to construct
        kafka producer", cause)``) after closing the serializers built so far."""
        given = [key for key, argument in ((_KEY_SERIALIZER, key_serializer),
                                           (_VALUE_SERIALIZER, value_serializer))
                 if argument is not None]
        originals, native = prepare(configs, client="producer", given_serdes=given)
        key = cast("Serializer[Any]", resolve_serde(
            key_serializer, originals, _KEY_SERIALIZER, is_key=True, default=bytes_serializer()))
        value: Serializer[Any] | None = None
        try:
            value = cast("Serializer[Any]", resolve_serde(
                value_serializer, originals, _VALUE_SERIALIZER, is_key=False,
                default=bytes_serializer()))
            handle, error = _lib.KafkaProducer_new(native, self._notify_hook())
        except BaseException:
            close_if_defined(key)
            if value is not None:
                close_if_defined(value)
            raise
        if error:
            close_if_defined(key)
            close_if_defined(value)
            raise from_ffi_error(error)
        self._key_serializer = key
        self._value_serializer = value
        self._c_producer = handle
        max_block_ms = native.get(_MAX_BLOCK_MS)
        if max_block_ms is not None:
            self._max_block_ms = int(max_block_ms)
        self._after_start()

    def _callback_deadline(self) -> float:
        """The ``time.monotonic()`` deadline of a wait from a delivery
        callback: ``max.block.ms`` from now, the bound Java's own wait has
        there (its completion would need the blocked I/O thread)."""
        return time.monotonic() + self._max_block_ms / 1000.0

    def _callback_wait_timeout(self, operation: str) -> KafkaTimeoutError:
        return KafkaTimeoutError(message=CALLBACK_WAIT_TIMEOUT_MESSAGE.format(
            ms=self._max_block_ms, operation=operation))

    def _destroy(self) -> None:
        """Wait until no call into the native producer is in flight, then
        ``Producer_destroy`` it (which runs the callbacks still pending, on
        this thread), once. ``_destroyed`` is set under ``_lifecycle`` with no
        use in flight, so a callback pump that has not started by then never
        will (see :meth:`_pump_use`)."""
        with self._lifecycle:
            while self._uses:
                self._lifecycle.wait()
            if self._destroyed:
                return
            self._destroyed = True
        _lib.Producer_destroy(self._c_producer)

    def _pump_use(self) -> bool:
        """Count a run of the callback pump as a use of the handle, unlike
        :meth:`_use` also once ``close()`` has begun (its completions still
        need the pump); ``False`` once the handle is destroyed. The caller
        ends the use with :meth:`_end_use`."""
        with self._lifecycle:
            if self._destroyed:
                return False
            self._uses += 1
            return True

    def _end_use(self) -> None:
        with self._lifecycle:
            self._uses -= 1
            if self._uses == 0:
                self._lifecycle.notify_all()

    def _check_not_closed(self) -> None:
        """Java's ``throwIfProducerClosed()``."""
        if self._closed:
            raise IllegalStateError(message=CLOSED_MESSAGE)

    def _native_parts(self, record: ProducerRecord[Any, Any]) -> tuple[Any, str, int | None]:
        """Serialize the record on the caller's thread and build the native
        ``_confluentkafka.ProducerRecord`` the send path reads: it holds the
        serialized ``bytes`` and the record's header values without copying
        them (CLAUDE.md §12). Returns it with the record's topic and partition,
        which the completion needs, so they are read once."""
        topic = record.topic()
        headers = record.headers()
        key = serialize(self._key_serializer, topic, record.key(), headers)
        value = serialize(self._value_serializer, topic, record.value(), headers)
        partition = record.partition()
        timestamp = record.timestamp()
        native = _lib.ProducerRecord(topic, value, key,
                                     -1 if partition is None else partition,
                                     -1 if timestamp is None else timestamp,
                                     headers)
        return native, topic, partition

    def _track(self, future: _FutureState[Any]) -> None:
        """Track a sent record's future until its completion untracks it, right
        after completing it (``_discard_future``), so a flush() or
        commit_transaction() waits for the records sent before it. Untracking
        in the completion, rather than by a done callback, saves the asyncio
        loop a scheduled callback per record. A record can complete before
        ``send()`` gets here (on the sync producer's completion thread): its
        future is then already done, and untracked at once. The sync ``send()``
        does not call this: it checks its completion's own flag."""
        self._futures.add(future)
        if future.is_done():
            self._futures.discard(future)

    def _close_serializers(self) -> None:
        """Java closes both serializers at producer close; a failing
        ``close()`` is logged, never raised."""
        close_if_defined(self._key_serializer)
        close_if_defined(self._value_serializer)


def check_group_metadata(group_metadata: ConsumerGroupMetadata | None) -> None:
    """The null arm of Java's ``throwIfInvalidGroupMetadata``
    (KafkaProducer.java:1578-1585), which the core cannot receive; the
    generation / member id arm is the core's."""
    if group_metadata is None:
        raise IllegalArgumentError(message=NULL_GROUP_METADATA_MESSAGE)
