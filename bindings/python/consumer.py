"""Pythonic Kafka consumer over the Rust consumer C FFI.

Mirrors the Java ``Consumer<K, V>`` interface with Python naming, in both a
synchronous (:class:`Consumer`, :class:`KafkaConsumer`, :class:`MockConsumer`)
and an asyncio-native (:class:`AsyncConsumer`, :class:`AsyncKafkaConsumer`,
:class:`AsyncMockConsumer`) form.

Design notes
------------
* The C extension (``_confluentkafka``) is a marshaling layer only: it converts
  Python objects to/from the C FFI and bridges the FFI's async callbacks back
  into Python. All orchestration lives here in pure Python.
* **Both APIs drive the async C bindings.** Even the synchronous methods submit
  an async FFI op and then wait on an interruptible Python primitive, so a long
  ``poll`` never parks the calling thread inside a native ``block_on`` where
  Python signal handlers cannot run. On ``KeyboardInterrupt`` the sync waiter
  calls :meth:`wakeup`, drains the in-flight op (releasing the access guard),
  and re-raises; the async waiter does the same on ``CancelledError``.
* The Rust consumer is single-owner (one operation in flight). Concurrent use
  surfaces as a ``KafkaError`` (ConcurrentModification) or, for the
  non-blocking state reads, a ``RuntimeError``.
* Key/value/header bytes are exposed as zero-copy ``memoryview`` objects backed
  by the record batch; they stay valid while the owning record (and its batch)
  is alive.
* **User callbacks are bridged.** A rebalance listener
  (:meth:`Consumer.subscribe` ``listener=``) and a ``commitAsync`` completion
  callback (:meth:`Consumer.commit_async` ``callback=``) are invoked on the Rust
  dispatcher thread with the GIL held, and the operation that triggered them
  does not complete until the callback returns — the same ordering guarantee
  Java gives by running them on the polling thread. Because that is *not* the
  thread that owns the consumer, a callback must reach back into the consumer
  through :meth:`Consumer.handle` (see :class:`ConsumerHandle`); the plain
  consumer methods would be rejected as concurrent access.

Out of scope: pattern subscription, ``metrics()`` / ``clientInstanceId()``.
"""

import asyncio
import datetime as _dt
import inspect
import logging
import threading

import _confluentkafka as _lib
from producer import KafkaError  # shared error type

_log = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Supporting value types (mirror the Java consumer types).
# --------------------------------------------------------------------------
class TopicPartition:
    """A (topic, partition) pair."""

    __slots__ = ("topic", "partition")

    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition

    def __eq__(self, other):
        return (isinstance(other, TopicPartition)
                and self.topic == other.topic
                and self.partition == other.partition)

    def __hash__(self):
        return hash((self.topic, self.partition))

    def __repr__(self):
        return f"TopicPartition(topic={self.topic!r}, partition={self.partition})"


class OffsetAndMetadata:
    """A committed offset with optional metadata and leader epoch."""

    __slots__ = ("offset", "metadata", "leader_epoch")

    def __init__(self, offset, metadata="", leader_epoch=None):
        self.offset = offset
        self.metadata = metadata
        self.leader_epoch = leader_epoch

    def __eq__(self, other):
        return (isinstance(other, OffsetAndMetadata)
                and self.offset == other.offset
                and self.metadata == other.metadata
                and self.leader_epoch == other.leader_epoch)

    def __repr__(self):
        return (f"OffsetAndMetadata(offset={self.offset}, metadata={self.metadata!r}, "
                f"leader_epoch={self.leader_epoch})")


class OffsetAndTimestamp:
    """An offset looked up by timestamp."""

    __slots__ = ("offset", "timestamp", "leader_epoch")

    def __init__(self, offset, timestamp, leader_epoch=None):
        self.offset = offset
        self.timestamp = timestamp
        self.leader_epoch = leader_epoch

    def __repr__(self):
        return (f"OffsetAndTimestamp(offset={self.offset}, timestamp={self.timestamp}, "
                f"leader_epoch={self.leader_epoch})")


class ConsumerGroupMetadata:
    """Group membership metadata."""

    __slots__ = ("group_id", "generation_id", "member_id", "group_instance_id")

    def __init__(self, group_id, generation_id, member_id, group_instance_id):
        self.group_id = group_id
        self.generation_id = generation_id
        self.member_id = member_id
        self.group_instance_id = group_instance_id

    def __repr__(self):
        return (f"ConsumerGroupMetadata(group_id={self.group_id!r}, "
                f"generation_id={self.generation_id}, member_id={self.member_id!r}, "
                f"group_instance_id={self.group_instance_id!r})")


class Node:
    """A Kafka broker node."""

    __slots__ = ("id", "host", "port", "rack")

    def __init__(self, id, host, port, rack=None):
        self.id = id
        self.host = host
        self.port = port
        self.rack = rack

    def __repr__(self):
        return f"Node(id={self.id}, host={self.host!r}, port={self.port}, rack={self.rack!r})"


class PartitionInfo:
    """Metadata about a single partition."""

    __slots__ = ("topic", "partition", "leader", "replicas",
                 "in_sync_replicas", "offline_replicas")

    def __init__(self, topic, partition, leader, replicas,
                 in_sync_replicas, offline_replicas):
        self.topic = topic
        self.partition = partition
        self.leader = leader
        self.replicas = replicas
        self.in_sync_replicas = in_sync_replicas
        self.offline_replicas = offline_replicas

    def __repr__(self):
        return (f"PartitionInfo(topic={self.topic!r}, partition={self.partition}, "
                f"leader={self.leader!r})")


class ConsumerRecords:
    """An iterable batch of :class:`ConsumerRecord` returned by ``poll``.

    Wraps the owning C records handle. Iterating yields the C ``ConsumerRecord``
    objects directly (no per-record Python wrapper allocation); ``record.key`` /
    ``record.value`` are zero-copy ``memoryview`` objects valid while the record
    is alive.
    """

    __slots__ = ("_c",)

    def __init__(self, c_records):
        self._c = c_records  # _confluentkafka.ConsumerRecords or None

    def __len__(self):
        return self._c.count() if self._c is not None else 0

    def is_empty(self):
        return self._c is None or self._c.is_empty()

    def __iter__(self):
        if self._c is None:
            return
        for i in range(self._c.count()):
            yield self._c.get(i)


# --------------------------------------------------------------------------
# Conversions from the C drain dicts/lists to the Python value types.
# --------------------------------------------------------------------------
def _to_offset_map(raw):
    return {TopicPartition(t, p): OffsetAndMetadata(o, m, e)
            for (t, p), (o, m, e) in raw.items()}


def _to_offset_and_timestamp_map(raw):
    return {TopicPartition(t, p): OffsetAndTimestamp(o, ts, e)
            for (t, p), (o, ts, e) in raw.items()}


def _to_long_map(raw):
    return {TopicPartition(t, p): off for (t, p), off in raw.items()}


def _to_node(n):
    return None if n is None else Node(n[0], n[1], n[2], n[3])


def _to_partition_info(t):
    topic, partition, leader, replicas, isr, offline = t
    return PartitionInfo(topic, partition, _to_node(leader),
                         [_to_node(x) for x in replicas],
                         [_to_node(x) for x in isr],
                         [_to_node(x) for x in offline])


def _to_partition_info_list(raw):
    return [_to_partition_info(t) for t in raw]


def _to_topics_map(raw):
    return {topic: [_to_partition_info(t) for t in infos]
            for topic, infos in raw.items()}


def _ms(timeout):
    """Convert a timeout (seconds float, or ``timedelta``) to int64 ms."""
    if isinstance(timeout, _dt.timedelta):
        return int(timeout.total_seconds() * 1000)
    return int(float(timeout) * 1000)


def _offsets_to_spec(offsets):
    """dict[TopicPartition, OffsetAndMetadata] -> the FFI's 5-tuple list shape."""
    return [(tp.topic, tp.partition, oam.offset,
             oam.leader_epoch if oam.leader_epoch is not None else -1,
             oam.metadata if oam.metadata is not None else "")
            for tp, oam in offsets.items()]


def _tp_to_spec(partitions):
    """Iterable[TopicPartition] -> the FFI's (topic, partition) list shape."""
    return [(tp.topic, tp.partition) for tp in partitions]


def _concurrent_error():
    return RuntimeError("KafkaConsumer is not safe for multi-threaded access.")


def _raise_if_error(error_handle):
    if error_handle:
        raise KafkaError._from_c(error_handle)


# --------------------------------------------------------------------------
# Callback adapters. These sit between the C trampolines and the user's
# duck-typed callback objects, and are what the trampolines actually invoke.
# --------------------------------------------------------------------------
class _ListenerAdapter:
    """Bridges a user rebalance listener to the three C trampolines.

    Mirrors Java's ``ConsumerRebalanceListener``: the listener object supplies
    ``on_partitions_revoked(partitions)`` and ``on_partitions_assigned(partitions)``
    (both required) and optionally ``on_partitions_lost(partitions)``. When
    ``on_partitions_lost`` is absent it delegates to ``on_partitions_revoked``,
    exactly like the Java interface's default method. Each method receives a
    ``list[TopicPartition]``.

    The ``_on_*`` methods are called from C on the Rust dispatcher thread with
    the GIL held, and receive the partitions as a ``list[(topic, partition)]``
    already converted C-side (the owned FFI list handle is destroyed there, so no
    handle ownership crosses into Python and none can leak). Exceptions propagate
    back into the trampoline, which turns them into a ``KafkaError`` returned to
    Rust — the rebalance, and the operation that drove it, then fails with that
    message, like a Java listener that throws.
    """

    __slots__ = ("_listener", "_loop")

    def __init__(self, listener, loop=None):
        for name in ("on_partitions_revoked", "on_partitions_assigned"):
            if not callable(getattr(listener, name, None)):
                raise TypeError(
                    f"listener must define a callable {name}(partitions)")
        self._listener = listener
        # Event loop to run coroutine listener methods on (AsyncConsumer only).
        self._loop = loop

    # ---- called from the C trampolines ------------------------------------
    def _on_revoked(self, raw_partitions):
        self._invoke(self._listener.on_partitions_revoked, raw_partitions)

    def _on_assigned(self, raw_partitions):
        self._invoke(self._listener.on_partitions_assigned, raw_partitions)

    def _on_lost(self, raw_partitions):
        method = getattr(self._listener, "on_partitions_lost", None)
        if not callable(method):
            # Java's ConsumerRebalanceListener.onPartitionsLost default body.
            method = self._listener.on_partitions_revoked
        self._invoke(method, raw_partitions)

    def _invoke(self, method, raw_partitions):
        partitions = [TopicPartition(t, p) for (t, p) in raw_partitions]
        result = method(partitions)
        if inspect.isawaitable(result):
            if self._loop is None:
                raise RuntimeError(
                    "a coroutine rebalance listener requires an AsyncConsumer")
            # Run it on the consumer's event loop and block this dispatcher
            # thread until it finishes, so the rebalance still does not complete
            # before the callback does.
            asyncio.run_coroutine_threadsafe(result, self._loop).result()


class _CommitCallbackAdapter:
    """Adapts a user ``callback(offsets, exception)`` to the C commit trampoline.

    Mirrors Java's ``OffsetCommitCallback.onComplete(Map, Exception)``:
    ``offsets`` is a ``dict[TopicPartition, OffsetAndMetadata]`` and
    ``exception`` is a :class:`KafkaError` or ``None``. Java's ``onComplete``
    returns ``void`` and has nowhere to report a failure of its own, so an
    exception raised here is logged and swallowed.

    Like :class:`_ListenerAdapter`, a callback that returns an awaitable is
    supported when a ``loop`` is available (:class:`AsyncConsumer`): it is
    scheduled onto that loop and awaited, so the operation delivering the
    completion still does not return before the callback body has run. The
    synchronous :class:`Consumer` has no loop, so a coroutine callback is
    rejected — up front in :meth:`__init__` when it is recognizable as one
    (unlike a rebalance listener, this adapter is built by the very call that
    will use it, so the rejection can surface to the caller).
    """

    __slots__ = ("_callback", "_loop")

    def __init__(self, callback, loop=None):
        if not callable(callback):
            raise TypeError("callback must be callable")
        if loop is None and inspect.iscoroutinefunction(callback):
            raise TypeError(
                "a coroutine commit callback requires an AsyncConsumer; the "
                "synchronous Consumer has no event loop to run it on")
        self._callback = callback
        # Event loop to run a coroutine callback on (AsyncConsumer only).
        self._loop = loop

    def __call__(self, offsets_handle, error_handle):
        # Both handles are owned by this call; draining / converting frees them.
        offsets = (_to_offset_map(_lib.OffsetMap_drain(offsets_handle))
                   if offsets_handle else {})
        exception = KafkaError._from_c(error_handle) if error_handle else None
        try:
            result = self._callback(offsets, exception)
            if inspect.isawaitable(result):
                if self._loop is None:
                    close = getattr(result, "close", None)
                    if callable(close):
                        close()  # suppress the unhelpful "never awaited" warning
                    raise TypeError(
                        "a coroutine commit callback requires an AsyncConsumer; "
                        "the synchronous Consumer has no event loop to run it on")
                # Run it on the consumer's event loop and block this dispatcher
                # thread until it finishes, so the delivering operation still
                # does not return before the callback has.
                asyncio.run_coroutine_threadsafe(result, self._loop).result()
        except Exception:  # noqa: BLE001 - must not escape into the C caller
            _log.exception("Error in commit_async callback")


# --------------------------------------------------------------------------
# Reentrancy handle.
# --------------------------------------------------------------------------
class ConsumerHandle:
    """A reentrancy handle onto a live consumer.

    This is the Python equivalent of Java capturing the ``consumer`` variable
    inside a ``ConsumerRebalanceListener`` or ``OffsetCommitCallback``: it is how
    a callback reaches back into the consumer it belongs to. Obtain one with
    :meth:`Consumer.handle`.

    :meth:`wakeup` and the three state getters return immediately; every other
    method is a **blocking** C call that releases the GIL while it runs. The same
    class therefore serves the synchronous and the asyncio consumer. Only the
    operations the core handle exposes are available — notably there is no
    ``poll``, ``subscribe``, ``close`` or callback-taking commit.

    .. warning::
       * Callbacks are the intended caller. Calling the *consumer's* own methods
         from inside a callback is rejected with a ``KafkaError``
         (ConcurrentModification); handle methods bypass that guard by design.
       * Because the blocking methods block, an :class:`AsyncConsumer`
         application must not call them on the event loop thread — use them from
         inside callbacks (which run on the dispatcher thread) or from an
         executor.
       * A coroutine rebalance listener must use this handle rather than
         ``await``-ing :class:`AsyncConsumer` methods: those need the dispatcher
         thread to deliver their completion, and it is parked waiting for the
         listener — a deadlock.
       * :meth:`destroy` the handle **before** closing the owning consumer.
         Handles are not valid afterwards. Using the handle as a context manager
         does this for you.
       * On a :class:`MockConsumer`-derived handle the state getters return empty
         collections and every other operation fails with an
         ``unsupported_version`` ``KafkaError`` (core behavior).
    """

    __slots__ = ("_h",)

    def __init__(self, h):
        self._h = h

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destroy()

    def destroy(self):
        """Free the handle. Idempotent; must happen before the consumer closes."""
        if self._h is not None:
            _lib.ConsumerHandle_destroy(self._h)
            self._h = None

    def _check(self):
        if self._h is None:
            raise RuntimeError("ConsumerHandle is already destroyed")
        return self._h

    # ---- non-blocking ----------------------------------------------------
    def wakeup(self):
        """Wake up the owning consumer's in-flight operation."""
        _lib.ConsumerHandle_wakeup(self._check())

    def assignment(self):
        return {TopicPartition(t, p)
                for (t, p) in _lib.ConsumerHandle_assignment(self._check())}

    def subscription(self):
        return set(_lib.ConsumerHandle_subscription(self._check()))

    def paused(self):
        return {TopicPartition(t, p)
                for (t, p) in _lib.ConsumerHandle_paused(self._check())}

    # ---- blocking --------------------------------------------------------
    def assign(self, partitions):
        """Assign partitions. An empty collection is rejected: on the consumer
        ``assign([])`` leaves the group, which this handle does not expose."""
        _raise_if_error(
            _lib.ConsumerHandle_assign(self._check(), _tp_to_spec(partitions)))

    def seek(self, partition, offset):
        """Seek a partition. ``offset`` is an int, or an :class:`OffsetAndMetadata`."""
        if isinstance(offset, OffsetAndMetadata):
            epoch = offset.leader_epoch if offset.leader_epoch is not None else -1
            e = _lib.ConsumerHandle_seek_with_metadata(
                self._check(), partition.topic, partition.partition,
                offset.offset, epoch, offset.metadata or "")
        else:
            e = _lib.ConsumerHandle_seek(
                self._check(), partition.topic, partition.partition, offset)
        _raise_if_error(e)

    def seek_to_beginning(self, partitions):
        _raise_if_error(_lib.ConsumerHandle_seek_to_beginning(
            self._check(), _tp_to_spec(partitions)))

    def seek_to_end(self, partitions):
        _raise_if_error(_lib.ConsumerHandle_seek_to_end(
            self._check(), _tp_to_spec(partitions)))

    def pause(self, partitions):
        _raise_if_error(_lib.ConsumerHandle_pause(
            self._check(), _tp_to_spec(partitions)))

    def resume(self, partitions):
        _raise_if_error(_lib.ConsumerHandle_resume(
            self._check(), _tp_to_spec(partitions)))

    def position(self, partition, timeout=None):
        if timeout is None:
            position, e = _lib.ConsumerHandle_position(
                self._check(), partition.topic, partition.partition)
        else:
            position, e = _lib.ConsumerHandle_position_timeout(
                self._check(), partition.topic, partition.partition, _ms(timeout))
        _raise_if_error(e)
        return position

    @staticmethod
    def _value(payload, drain, convert):
        handle, e = payload
        if e:
            if handle:
                drain(handle)  # drain destroys the handle
            raise KafkaError._from_c(e)
        return convert(drain(handle))

    def committed(self, partitions):
        return self._value(
            _lib.ConsumerHandle_committed(self._check(), _tp_to_spec(partitions)),
            _lib.OffsetMap_drain, _to_offset_map)

    def beginning_offsets(self, partitions):
        return self._value(
            _lib.ConsumerHandle_beginning_offsets(
                self._check(), _tp_to_spec(partitions)),
            _lib.LongOffsetMap_drain, _to_long_map)

    def end_offsets(self, partitions):
        return self._value(
            _lib.ConsumerHandle_end_offsets(self._check(), _tp_to_spec(partitions)),
            _lib.LongOffsetMap_drain, _to_long_map)

    def offsets_for_times(self, timestamps):
        spec = [(tp.topic, tp.partition, ts) for tp, ts in timestamps.items()]
        return self._value(
            _lib.ConsumerHandle_offsets_for_times(self._check(), spec),
            _lib.OffsetAndTimestampMap_drain, _to_offset_and_timestamp_map)

    def commit_sync(self, offsets=None):
        """Commit synchronously — Java ``commitSync()`` / ``commitSync(Map)``."""
        if offsets is None:
            e = _lib.ConsumerHandle_commit_sync(self._check())
        else:
            e = _lib.ConsumerHandle_commit_sync_offsets(
                self._check(), _offsets_to_spec(offsets))
        _raise_if_error(e)

    def commit_async(self, offsets=None):
        """Initiate an asynchronous commit — Java ``commitAsync()`` /
        ``commitAsync(Map)``. The handle exposes no completion-callback variant
        (matching the core handle); use
        :meth:`Consumer.commit_async` for that."""
        if offsets is None:
            e = _lib.ConsumerHandle_commit_async(self._check())
        else:
            e = _lib.ConsumerHandle_commit_async_offsets(
                self._check(), _offsets_to_spec(offsets))
        _raise_if_error(e)


# --------------------------------------------------------------------------
# Shared base: handle ownership, non-blocking state reads, and the per-method
# (submit, resolve, free) specs driven by _run_sync / _run_async.
# --------------------------------------------------------------------------
class _ConsumerBase:
    def __init__(self):
        self._h = None
        self.closed = False
        # Event loop coroutine callbacks are scheduled onto; stays None for the
        # synchronous consumer (see _listener_loop).
        self._loop = None
        # Strong reference to the currently registered rebalance-listener
        # adapter, mirroring the reference the Rust adapter holds (see
        # subscribe()). Kept in sync with the registration: replaced by a
        # subsequent subscribe*, dropped when the consumer is destroyed.
        self._listener_adapter = None

    def _init_mock(self, auto_offset_reset="earliest"):
        self._h = _lib.Consumer_MockConsumer_new(auto_offset_reset)

    def _init_kafka(self, config):
        self._h = _lib.Consumer_KafkaConsumer_new(config)

    def _check_closed(self):
        if self.closed:
            raise RuntimeError("Consumer is already closed")

    def _destroy(self):
        if self._h is not None:
            _lib.Consumer_destroy(self._h)
            self._h = None
        # Destroying the consumer drops the Rust listener adapter, which fires
        # the C destroy hook; release our reference too so the registration is
        # not kept alive by a closed consumer.
        self._listener_adapter = None

    def handle(self):
        """Return a :class:`ConsumerHandle` for reentrant access.

        This is what a rebalance listener or commit callback uses to call back
        into the consumer — the consumer's own methods would be rejected as
        concurrent access while the triggering operation is still in flight.
        Destroy the handle (or use it as a context manager) before closing the
        consumer."""
        self._check_closed()
        return ConsumerHandle(_lib.Consumer_handle(self._h))

    def _listener_loop(self):
        """Event loop that coroutine callbacks are scheduled on.

        Used for coroutine rebalance-listener methods and coroutine
        ``commit_async`` callbacks. ``None`` for the synchronous consumer, whose
        callbacks are plain callables invoked directly on the dispatcher
        thread."""
        return None

    # ---- non-blocking state reads (sync in Java; shared by both APIs) ------
    def assignment(self):
        raw = _lib.Consumer_assignment(self._h)
        if raw is None:
            raise _concurrent_error()
        return {TopicPartition(t, p) for (t, p) in raw}

    def subscription(self):
        raw = _lib.Consumer_subscription(self._h)
        if raw is None:
            raise _concurrent_error()
        return set(raw)

    def paused(self):
        raw = _lib.Consumer_paused(self._h)
        if raw is None:
            raise _concurrent_error()
        return {TopicPartition(t, p) for (t, p) in raw}

    def group_metadata(self):
        g = _lib.Consumer_group_metadata(self._h)
        if g is None:
            raise _concurrent_error()
        return ConsumerGroupMetadata(*g)

    def client_id(self):
        return _lib.Consumer_client_id(self._h)

    def current_lag(self, partition):
        return _lib.Consumer_current_lag(self._h, partition.topic, partition.partition)

    def wakeup(self):
        _lib.Consumer_wakeup(self._h)

    # ---- local, non-blocking ops (sync in Java; shared by both APIs) -------
    def enforce_rebalance(self, reason=None):
        e = _lib.Consumer_enforce_rebalance(self._h, reason)
        if e:
            raise KafkaError._from_c(e)

    def commit_async(self, offsets=None, callback=None):
        """Initiate an asynchronous commit and return immediately.

        Covers all three Java overloads: ``commitAsync()``,
        ``commitAsync(callback)`` and ``commitAsync(offsets, callback)``.

        Args:
            offsets: optional ``dict[TopicPartition, OffsetAndMetadata]`` to
                commit; the current positions are committed when omitted.
            callback: optional ``callback(offsets, exception)`` — Java's
                ``OffsetCommitCallback``. ``offsets`` is a
                ``dict[TopicPartition, OffsetAndMetadata]`` and ``exception`` a
                :class:`KafkaError` or ``None``. On an :class:`AsyncConsumer` it
                may be a coroutine function; on the synchronous
                :class:`Consumer` it must not be (there is no event loop to run
                it on, so one is rejected with ``TypeError`` here).

        .. warning::
           ``callback`` runs on the Rust dispatcher thread, not the caller's, and
           the operation that delivers it (a later ``poll``/``commit``/``close``)
           does not return until it does — matching Java, which runs
           ``onComplete`` on the polling thread. To touch the consumer from
           inside it, use :meth:`handle`. A coroutine callback is scheduled onto
           the :class:`AsyncConsumer`'s loop and awaited there, so this call must
           not itself occupy that loop while the completion is delivered — i.e.
           do not call ``commit_async`` with a coroutine callback on the loop
           thread if the completion can be delivered inline (a
           :class:`AsyncMockConsumer` does exactly that); drive it from a worker
           thread instead.
        """
        cb = (None if callback is None
              else _CommitCallbackAdapter(callback, self._listener_loop()))
        if offsets is None:
            e = _lib.Consumer_commit_async(self._h, cb)
        else:
            e = _lib.Consumer_commit_async_offsets(
                self._h, _offsets_to_spec(offsets), cb)
        _raise_if_error(e)

    # ---- resolve / free pairs (by callback payload shape) ------------------
    @staticmethod
    def _resolve_void(payload):
        (error,) = payload
        if error:
            raise KafkaError._from_c(error)
        return None

    @staticmethod
    def _free_void(payload):
        if payload[0]:
            _lib.KafkaError_destroy(payload[0])

    @staticmethod
    def _resolve_poll(payload):
        records, error = payload
        if error:
            raise KafkaError._from_c(error)
        return ConsumerRecords(_lib.ConsumerRecords_wrap(records))

    @staticmethod
    def _free_poll(payload):
        records, error = payload
        if error:
            _lib.KafkaError_destroy(error)
        if records:
            # Wrap so the resulting object's destructor frees the batch.
            _lib.ConsumerRecords_wrap(records)

    @staticmethod
    def _resolve_position(payload):
        position, error = payload
        if error:
            raise KafkaError._from_c(error)
        return position

    @staticmethod
    def _free_position(payload):
        if payload[1]:
            _lib.KafkaError_destroy(payload[1])

    @staticmethod
    def _resolve_value(drain, convert):
        def resolve(payload):
            handle, error = payload
            if error:
                raise KafkaError._from_c(error)
            return convert(drain(handle))
        return resolve

    @staticmethod
    def _free_value(drain):
        def free(payload):
            handle, error = payload
            if error:
                _lib.KafkaError_destroy(error)
            if handle:
                drain(handle)  # drain destroys the handle
        return free

    # ---- per-method specs: (submit, resolve, free) -------------------------
    def _poll_spec(self, timeout):
        ms = _ms(timeout)
        return (lambda cb: _lib.Consumer_poll_async(self._h, ms, cb),
                self._resolve_poll, self._free_poll)

    def _subscribe_spec(self, topics, listener=None):
        topics = list(topics)
        if listener is None:
            # Java's subscribe(Collection) clears any previously registered
            # listener, so drop our reference to match.
            self._listener_adapter = None
            return (lambda cb: _lib.Consumer_subscribe_async(self._h, topics, cb),
                    self._resolve_void, self._free_void)
        adapter = _ListenerAdapter(listener, self._listener_loop())
        # Replacing the registration releases the previous adapter: Rust drops
        # its adapter (firing the C destroy hook, which DECREFs) and we drop our
        # reference here.
        self._listener_adapter = adapter
        return (lambda cb: _lib.Consumer_subscribe_with_listener_async(
                    self._h, topics, adapter, cb),
                self._resolve_void, self._free_void)

    def _seek_spec(self, partition, offset):
        """Spec for ``seek``; ``offset`` is an int or an :class:`OffsetAndMetadata`.

        ``seek`` does not block in Java, but the Rust ``AsyncKafkaConsumer``'s does
        (it submits a ``SeekUnvalidatedEvent`` and drains background events, which
        can invoke the rebalance listener), so it goes through the async FFI entry
        point like every other blocking op rather than a sync call that would hold
        the GIL — and the event loop — across a callback dispatch.
        """
        if isinstance(offset, OffsetAndMetadata):
            epoch = offset.leader_epoch if offset.leader_epoch is not None else -1
            return (lambda cb: _lib.Consumer_seek_with_metadata_async(
                        self._h, partition.topic, partition.partition,
                        offset.offset, epoch, offset.metadata or "", cb),
                    self._resolve_void, self._free_void)
        return (lambda cb: _lib.Consumer_seek_async(
                    self._h, partition.topic, partition.partition, offset, cb),
                self._resolve_void, self._free_void)

    def _unsubscribe_spec(self):
        return (lambda cb: _lib.Consumer_unsubscribe_async(self._h, cb),
                self._resolve_void, self._free_void)

    def _tp_op_spec(self, fn, partitions):
        tps = _tp_to_spec(partitions)
        return (lambda cb: fn(self._h, tps, cb), self._resolve_void, self._free_void)

    def _commit_spec(self, offsets):
        if offsets is None:
            return (lambda cb: _lib.Consumer_commit_sync_async(self._h, cb),
                    self._resolve_void, self._free_void)
        spec = _offsets_to_spec(offsets)
        return (lambda cb: _lib.Consumer_commit_sync_offsets_async(self._h, spec, cb),
                self._resolve_void, self._free_void)

    def _close_spec(self):
        return (lambda cb: _lib.Consumer_close_async(self._h, cb),
                self._resolve_void, self._free_void)

    def _position_spec(self, partition):
        return (lambda cb: _lib.Consumer_position_async(
                    self._h, partition.topic, partition.partition, cb),
                self._resolve_position, self._free_position)

    def _committed_spec(self, partitions):
        tps = [(tp.topic, tp.partition) for tp in partitions]
        drain = _lib.OffsetMap_drain
        return (lambda cb: _lib.Consumer_committed_async(self._h, tps, cb),
                self._resolve_value(drain, _to_offset_map),
                self._free_value(drain))

    def _offsets_for_times_spec(self, timestamps):
        spec = [(tp.topic, tp.partition, ts) for tp, ts in timestamps.items()]
        drain = _lib.OffsetAndTimestampMap_drain
        return (lambda cb: _lib.Consumer_offsets_for_times_async(self._h, spec, cb),
                self._resolve_value(drain, _to_offset_and_timestamp_map),
                self._free_value(drain))

    def _long_offsets_spec(self, fn, partitions):
        tps = [(tp.topic, tp.partition) for tp in partitions]
        drain = _lib.LongOffsetMap_drain
        return (lambda cb: fn(self._h, tps, cb),
                self._resolve_value(drain, _to_long_map),
                self._free_value(drain))

    def _partitions_for_spec(self, topic):
        drain = _lib.PartitionInfoList_drain
        return (lambda cb: _lib.Consumer_partitions_for_async(self._h, topic, cb),
                self._resolve_value(drain, _to_partition_info_list),
                self._free_value(drain))

    def _list_topics_spec(self):
        drain = _lib.TopicPartitionInfoMap_drain
        return (lambda cb: _lib.Consumer_list_topics_async(self._h, cb),
                self._resolve_value(drain, _to_topics_map),
                self._free_value(drain))


class _MockConsumerMixin:
    """Mock-only operations (test helper)."""

    def rebalance(self, partitions):
        """Drive a rebalance to ``partitions`` (Java ``MockConsumer.rebalance``).

        Invokes the registered rebalance listener inline and does not return
        until its callbacks have: ``on_partitions_revoked`` with the removed
        partitions (only when something was removed), then
        ``on_partitions_assigned`` with the *added* partitions — which fires even
        when nothing was added, as long as a listener is registered.
        ``on_partitions_lost`` is never fired by the mock.

        Requires a topic subscription; a manually assigned consumer fails with
        "manual assignment in use". A listener exception surfaces here as a
        :class:`KafkaError` carrying its message.
        """
        _raise_if_error(
            _lib.MockConsumer_rebalance(self._h, _tp_to_spec(partitions)))

    def add_record(self, topic, partition, offset, key=None, value=None):
        e = _lib.MockConsumer_add_record(self._h, topic, partition, offset, key, value)
        if e:
            raise KafkaError._from_c(e)

    def update_beginning_offsets(self, topic, partition, offset):
        e = _lib.MockConsumer_update_beginning_offsets(self._h, topic, partition, offset)
        if e:
            raise KafkaError._from_c(e)

    def update_end_offsets(self, topic, partition, offset):
        e = _lib.MockConsumer_update_end_offsets(self._h, topic, partition, offset)
        if e:
            raise KafkaError._from_c(e)

    def update_partitions(self, topic, partition_count, leader_id=0,
                          leader_host="localhost", leader_port=9092):
        e = _lib.MockConsumer_update_partitions(
            self._h, topic, partition_count, leader_id, leader_host, leader_port)
        if e:
            raise KafkaError._from_c(e)

    def set_poll_error(self, message):
        e = _lib.MockConsumer_set_poll_error(self._h, message)
        if e:
            raise KafkaError._from_c(e)


# --------------------------------------------------------------------------
# Synchronous API.
# --------------------------------------------------------------------------
class Consumer(_ConsumerBase):
    """A synchronous Kafka consumer. Blocking methods submit an async FFI op
    and wait on an interruptible event, so ``KeyboardInterrupt`` is honored
    promptly (translated into a Rust-side ``wakeup``)."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _run_sync(self, submit, resolve, free):
        box = {}
        done = threading.Event()

        def cb(*payload):
            box["payload"] = payload
            done.set()

        submit(cb)
        interrupted = None
        while True:
            try:
                # Short slices keep the main-thread eval loop reachable so a
                # pending signal raises KeyboardInterrupt here rather than after
                # the whole op completes.
                while not done.wait(0.1):
                    pass
                break
            except KeyboardInterrupt as exc:
                interrupted = exc
                self.wakeup()  # abort the in-flight op; idempotent
                # keep waiting for the callback so the guard is released
        payload = box["payload"]
        if interrupted is not None:
            free(payload)
            raise interrupted
        return resolve(payload)

    def poll(self, timeout):
        self._check_closed()
        return self._run_sync(*self._poll_spec(timeout))

    def subscribe(self, topics, listener=None):
        """Subscribe to ``topics``, optionally with a rebalance ``listener``.

        ``listener`` is any object with ``on_partitions_revoked(partitions)`` and
        ``on_partitions_assigned(partitions)`` methods, plus an optional
        ``on_partitions_lost(partitions)`` (which otherwise delegates to
        ``on_partitions_revoked``, as in Java). Each receives a
        ``list[TopicPartition]``.

        The listener registration is released when a later ``subscribe`` replaces
        it, or when the consumer is closed/destroyed — **not** by
        :meth:`unsubscribe`, matching Java's
        ``SubscriptionState.unsubscribe()``, which leaves the listener in place.

        .. warning::
           Listener methods run on the Rust dispatcher thread, not the caller's,
           and the rebalance does not complete until they return (Java's
           guarantee). To call back into the consumer from a listener — to flush
           offsets with ``commit_sync`` before partitions are taken away, say —
           use :meth:`handle`; the consumer's own methods would be rejected as
           concurrent access.
        """
        self._check_closed()
        return self._run_sync(*self._subscribe_spec(topics, listener))

    def unsubscribe(self):
        self._check_closed()
        return self._run_sync(*self._unsubscribe_spec())

    def assign(self, partitions):
        self._check_closed()
        return self._run_sync(*self._tp_op_spec(_lib.Consumer_assign_async, partitions))

    def pause(self, partitions):
        self._check_closed()
        return self._run_sync(*self._tp_op_spec(_lib.Consumer_pause_async, partitions))

    def resume(self, partitions):
        self._check_closed()
        return self._run_sync(*self._tp_op_spec(_lib.Consumer_resume_async, partitions))

    def seek(self, partition, offset):
        """Seek a partition. ``offset`` is an int, or an :class:`OffsetAndMetadata`."""
        self._check_closed()
        return self._run_sync(*self._seek_spec(partition, offset))

    def seek_to_beginning(self, partitions):
        self._check_closed()
        return self._run_sync(*self._tp_op_spec(
            _lib.Consumer_seek_to_beginning_async, partitions))

    def seek_to_end(self, partitions):
        self._check_closed()
        return self._run_sync(*self._tp_op_spec(
            _lib.Consumer_seek_to_end_async, partitions))

    def commit(self, offsets=None, timeout=None):
        self._check_closed()
        return self._run_sync(*self._commit_spec(offsets))

    def position(self, partition, timeout=None):
        self._check_closed()
        return self._run_sync(*self._position_spec(partition))

    def committed(self, partitions, timeout=None):
        self._check_closed()
        return self._run_sync(*self._committed_spec(partitions))

    def offsets_for_times(self, timestamps, timeout=None):
        self._check_closed()
        return self._run_sync(*self._offsets_for_times_spec(timestamps))

    def beginning_offsets(self, partitions, timeout=None):
        self._check_closed()
        return self._run_sync(*self._long_offsets_spec(
            _lib.Consumer_beginning_offsets_async, partitions))

    def end_offsets(self, partitions, timeout=None):
        self._check_closed()
        return self._run_sync(*self._long_offsets_spec(
            _lib.Consumer_end_offsets_async, partitions))

    def partitions_for(self, topic, timeout=None):
        self._check_closed()
        return self._run_sync(*self._partitions_for_spec(topic))

    def list_topics(self, timeout=None):
        self._check_closed()
        return self._run_sync(*self._list_topics_spec())

    def close(self, timeout=None):
        if self.closed:
            return
        self.closed = True
        try:
            self._run_sync(*self._close_spec())
        finally:
            self._destroy()


# --------------------------------------------------------------------------
# Asyncio-native API.
# --------------------------------------------------------------------------
class AsyncConsumer(_ConsumerBase):
    """An asyncio-native Kafka consumer. Blocking methods are coroutines that
    submit an async FFI op and ``await`` its completion on the event loop;
    cancelling the await aborts the in-flight op via ``wakeup``."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @staticmethod
    def _deliver(fut, payload, free):
        # Runs on the event loop thread.
        if fut.cancelled() or fut.done():
            free(payload)
            return
        fut.set_result(payload)

    async def _run_async(self, submit, resolve, free):
        loop = asyncio.get_running_loop()
        self._loop = loop  # remember it for off-loop coroutine callbacks
        fut = loop.create_future()

        def cb(*payload):
            # Runs on the Rust dispatcher thread with the GIL held. asyncio
            # futures must be touched only on the loop thread.
            if loop.is_closed():
                free(payload)
                return
            loop.call_soon_threadsafe(self._deliver, fut, payload, free)

        submit(cb)
        try:
            payload = await fut
        except asyncio.CancelledError:
            # Abort the in-flight op; its late callback will free the handles
            # via _deliver (the future is now cancelled) and release the guard.
            self.wakeup()
            raise
        return resolve(payload)

    async def poll(self, timeout):
        self._check_closed()
        return await self._run_async(*self._poll_spec(timeout))

    def _listener_loop(self):
        # Coroutine callbacks are scheduled back onto this consumer's loop (the
        # dispatcher thread cannot run them itself). Re-observed here and in
        # _run_async, and cached, because commit_async is a plain method that may
        # legitimately be called from a worker thread — where there is no running
        # loop, but the consumer's loop is still the right target.
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        return self._loop

    async def subscribe(self, topics, listener=None):
        """Subscribe to ``topics``, optionally with a rebalance ``listener``.

        Same contract as :meth:`Consumer.subscribe`, and the listener methods may
        additionally be coroutines: they are scheduled onto this consumer's event
        loop and awaited before the rebalance proceeds.

        .. warning::
           A coroutine listener method must NOT ``await`` other
           :class:`AsyncConsumer` methods — their completion is delivered by the
           dispatcher thread, which is parked waiting for the listener, so it
           would deadlock. Use :meth:`handle` for reentrant access instead.
        """
        self._check_closed()
        return await self._run_async(*self._subscribe_spec(topics, listener))

    async def unsubscribe(self):
        self._check_closed()
        return await self._run_async(*self._unsubscribe_spec())

    async def assign(self, partitions):
        self._check_closed()
        return await self._run_async(*self._tp_op_spec(_lib.Consumer_assign_async, partitions))

    async def pause(self, partitions):
        self._check_closed()
        return await self._run_async(*self._tp_op_spec(_lib.Consumer_pause_async, partitions))

    async def resume(self, partitions):
        self._check_closed()
        return await self._run_async(*self._tp_op_spec(_lib.Consumer_resume_async, partitions))

    async def seek(self, partition, offset):
        """Seek a partition. ``offset`` is an int, or an :class:`OffsetAndMetadata`.

        A coroutine (unlike Java's non-blocking ``seek``) because the Rust
        consumer's ``seek`` awaits the background task, which may run a rebalance
        listener on the way — see :meth:`_ConsumerBase._seek_spec`."""
        self._check_closed()
        return await self._run_async(*self._seek_spec(partition, offset))

    async def seek_to_beginning(self, partitions):
        self._check_closed()
        return await self._run_async(*self._tp_op_spec(
            _lib.Consumer_seek_to_beginning_async, partitions))

    async def seek_to_end(self, partitions):
        self._check_closed()
        return await self._run_async(*self._tp_op_spec(
            _lib.Consumer_seek_to_end_async, partitions))

    async def commit(self, offsets=None, timeout=None):
        self._check_closed()
        return await self._run_async(*self._commit_spec(offsets))

    async def position(self, partition, timeout=None):
        self._check_closed()
        return await self._run_async(*self._position_spec(partition))

    async def committed(self, partitions, timeout=None):
        self._check_closed()
        return await self._run_async(*self._committed_spec(partitions))

    async def offsets_for_times(self, timestamps, timeout=None):
        self._check_closed()
        return await self._run_async(*self._offsets_for_times_spec(timestamps))

    async def beginning_offsets(self, partitions, timeout=None):
        self._check_closed()
        return await self._run_async(*self._long_offsets_spec(
            _lib.Consumer_beginning_offsets_async, partitions))

    async def end_offsets(self, partitions, timeout=None):
        self._check_closed()
        return await self._run_async(*self._long_offsets_spec(
            _lib.Consumer_end_offsets_async, partitions))

    async def partitions_for(self, topic, timeout=None):
        self._check_closed()
        return await self._run_async(*self._partitions_for_spec(topic))

    async def list_topics(self, timeout=None):
        self._check_closed()
        return await self._run_async(*self._list_topics_spec())

    async def close(self, timeout=None):
        if self.closed:
            return
        self.closed = True
        try:
            await self._run_async(*self._close_spec())
        finally:
            self._destroy()


# --------------------------------------------------------------------------
# Concrete variants.
# --------------------------------------------------------------------------
class KafkaConsumer(Consumer):
    """A synchronous consumer connected to a real cluster.

    Args:
        config: dict of configuration properties (e.g. ``bootstrap.servers``,
            ``group.id``, ``group.protocol=consumer``).
    """

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class MockConsumer(_MockConsumerMixin, Consumer):
    def __init__(self, auto_offset_reset="earliest"):
        super().__init__()
        self._init_mock(auto_offset_reset)


class AsyncKafkaConsumer(AsyncConsumer):
    """An asyncio-native consumer connected to a real cluster."""

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class AsyncMockConsumer(_MockConsumerMixin, AsyncConsumer):
    def __init__(self, auto_offset_reset="earliest"):
        super().__init__()
        self._init_mock(auto_offset_reset)
