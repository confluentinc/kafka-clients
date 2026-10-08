"""Pythonic Kafka consumer over the Rust consumer C FFI.

Mirrors the Java ``Consumer<K, V>`` interface with Python naming, in both a
synchronous (:class:`Consumer`, :class:`KafkaConsumer`, :class:`MockConsumer`)
and an asyncio-native (:class:`AsyncConsumer`, :class:`AsyncKafkaConsumer`,
:class:`AsyncMockConsumer`) form.

Design notes
------------
* The C extension (``_confluentkafka``) is a marshaling layer only: it converts
  Python objects to/from the C FFI and bridges the FFI's callbacks back into
  Python. All orchestration lives here in pure Python.
* **The synchronous consumer calls the blocking C entry points** with the GIL
  released. A rebalance listener (:meth:`Consumer.subscribe` ``listener=``) or a
  ``commitAsync`` completion callback (:meth:`Consumer.commit_async`
  ``callback=``) is invoked by the Rust client *on the calling thread*, inside
  the operation that triggered it, and the operation does not complete until
  the callback has returned -- exactly Java's guarantee of running them on the
  polling thread. A listener that needs to reach back into the consumer uses
  :meth:`Consumer.handle` (see :class:`ConsumerHandle`): the consumer's own
  methods are rejected while the triggering operation is in flight.
* **The asyncio consumer calls the ``_cb`` twins**: the operation runs on the
  Rust runtime and queues its completion -- and every listener / commit-callback
  invocation it triggers -- on the client's callback queue. The client's notify
  hook fires once each time that queue goes from empty to non-empty and
  schedules the pump (``Consumer_execute_callbacks``) on the event loop with
  ``call_soon_threadsafe``, so every callback runs on the loop thread. Listener
  methods may therefore be coroutines: the pumped invocation schedules the
  coroutine on the loop and reports its outcome to the client when it
  completes, and the rebalance does not advance before then.
* The Rust consumer is single-owner (one operation in flight). Concurrent use
  surfaces as a ``KafkaError`` (LocalConcurrentModification); the non-blocking
  state reads return empty collections meanwhile.
* Record keys and values are ``bytes`` (``None`` for a Java ``null``), copied
  out of the record batch when the record object is created.
* ``wakeup()`` is the only way to interrupt a blocking call: the synchronous
  consumer sits inside a native call that Python signal handlers cannot
  interrupt, so ``Ctrl-C`` takes effect when the call returns. Call
  :meth:`Consumer.wakeup` from another thread (or a signal handler) to abort the
  in-flight operation promptly.

Out of scope: ``clientInstanceId()``.

``metrics()`` IS supported (a one-shot snapshot read); see
:meth:`Consumer.metrics`.
"""

import asyncio
import datetime as _dt
import inspect
import logging
import weakref

import _confluentkafka as _lib
from producer import KafkaError  # shared error type
# ConsumerGroupMetadata is a C extension type that owns a live Rust
# group-metadata handle (freed in its tp_dealloc on GC). It exposes the same
# .group_id / .generation_id / .member_id / .group_instance_id + repr surface as
# the former pure-Python dataclass, and additionally carries the handle that
# Producer.send_offsets_to_transaction feeds back into the FFI (see
# producer-transactions-python-plan.md §6.1). Consumer.group_metadata() is the
# only way to get one: Java deprecated the ConsumerGroupMetadata constructors in
# 4.2 (the class becomes an interface in 5.0), so the type cannot be
# instantiated from Python.
from _confluentkafka import ConsumerGroupMetadata  # noqa: F401  (re-exported)

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


# ConsumerGroupMetadata is re-exported from _confluentkafka (see the import
# above); it is a handle-owning C extension type, not a pure-Python dataclass,
# because Producer.send_offsets_to_transaction must feed the live handle back
# into the FFI. Its field/repr surface is unchanged.


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
    """An iterable batch of ``ConsumerRecord`` returned by ``poll``.

    Wraps the owning C records handle. Iterating yields the C ``ConsumerRecord``
    objects (``topic``, ``partition``, ``offset``, ``timestamp``,
    ``timestamp_type``, ``key``, ``value``, ``headers``, ``leader_epoch``,
    ``delivery_count``, ...). ``record.key`` / ``record.value`` are ``bytes``
    (or ``None``), ``record.headers`` a ``list[(str, bytes | None)]``.
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
            return iter(())
        return iter(self._c.records())

    def partitions(self):
        """The partitions with records in this batch -- Java ``partitions()``."""
        if self._c is None:
            return set()
        return {TopicPartition(t, p) for (t, p) in self._c.partitions()}

    def records(self, partition_or_topic):
        """Records of one partition (``TopicPartition``) or topic (``str``) --
        Java's two ``records(...)`` overloads."""
        if self._c is None:
            return []
        if isinstance(partition_or_topic, str):
            return self._c.records_with_topic(partition_or_topic)
        tp = partition_or_topic
        return self._c.records_with_partition(tp.topic, tp.partition)

    def next_offsets(self):
        """The position to resume from per partition -- Java ``nextOffsets()``."""
        if self._c is None:
            return {}
        return _to_offset_map(self._c.next_offsets())


# --------------------------------------------------------------------------
# Conversions from the C-side dicts/lists to the Python value types.
# --------------------------------------------------------------------------
def _to_offset_map(raw):
    # A None value is Java's null entry of committed(): no committed offset.
    return {TopicPartition(t, p): (None if v is None else OffsetAndMetadata(*v))
            for (t, p), v in raw.items()}


def _to_offset_and_timestamp_map(raw):
    return {TopicPartition(t, p): (None if v is None else OffsetAndTimestamp(*v))
            for (t, p), v in raw.items()}


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
    """Timeout (seconds float, ``timedelta``, or ``None``) -> int64 ms.

    ``None`` (or a negative value) maps to ``-1``, which selects the Java
    overload *without* a ``Duration`` where one exists (``close()``,
    ``position(tp)``, ...)."""
    if timeout is None:
        return -1
    if isinstance(timeout, _dt.timedelta):
        timeout = timeout.total_seconds()
    if timeout < 0:
        return -1
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


def _timestamps_to_spec(timestamps):
    """dict[TopicPartition, int] -> the FFI's (topic, partition, int64) shape."""
    return [(tp.topic, tp.partition, ts) for tp, ts in timestamps.items()]


def _concurrent_error():
    return RuntimeError("KafkaConsumer is not safe for multi-threaded access.")


def _raise_if_error(err_tuple):
    if err_tuple is not None:
        raise KafkaError._from_tuple(err_tuple)


def _running_loop():
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def _schedule(awaitable, loop, on_done):
    """Run ``awaitable`` as a task on ``loop`` and call
    ``on_done(exception | None)`` on the loop thread when it finishes.

    The pump runs on the loop thread, so this is normally called there; the
    one off-loop caller is ``Consumer_destroy`` running still-pending callbacks
    on the destroying thread, hence the ``call_soon_threadsafe`` hop."""

    def start():
        task = asyncio.ensure_future(awaitable, loop=loop)

        def done(t):
            if t.cancelled():
                on_done(asyncio.CancelledError("callback task was cancelled"))
            else:
                on_done(t.exception())

        task.add_done_callback(done)

    if _running_loop() is loop:
        start()
        return
    try:
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
        loop.call_soon_threadsafe(start)
    except RuntimeError:
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()  # suppress the "never awaited" warning
        # Still report, so the client never waits on a callback_id forever.
        on_done(RuntimeError("event loop closed before the callback could run"))


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

    The ``_on_*`` methods are what the C trampolines call, with the partitions
    already converted to a ``list[(topic, partition)]`` and the client's
    ``callback_id`` for the invocation. They return:

    * ``None`` -- the listener returned; the trampoline reports success to the
      client right away;
    * ``True`` -- the listener method returned an awaitable (a coroutine
      listener on an :class:`AsyncConsumer`): it has been scheduled on the
      consumer's event loop and ``report(callback_id, message | None)`` will be
      called when it finishes. The rebalance does not advance before that.
    * raise -- the trampoline reports the exception's message as a
      ``KafkaException``; the rebalance, and the operation that drove it, fail
      with that message, like a Java listener that throws.

    On the synchronous :class:`Consumer` there is no event loop, so a listener
    method returning an awaitable is an error (``RuntimeError``, reported like
    any other exception).
    """

    __slots__ = ("_listener", "_loop", "_report")

    def __init__(self, listener, loop=None, report=None):
        for name in ("on_partitions_revoked", "on_partitions_assigned"):
            if not callable(getattr(listener, name, None)):
                raise TypeError(
                    f"listener must define a callable {name}(partitions)")
        self._listener = listener
        # Event loop to run coroutine listener methods on (AsyncConsumer only),
        # and a weak reference to the consumer method that reports a deferred
        # result to the client. Weak, because the C registration owns this
        # adapter: a strong reference would keep the consumer alive until it
        # is closed.
        self._loop = loop
        self._report = weakref.WeakMethod(report) if report is not None else None

    # ---- called from the C trampolines ------------------------------------
    def _on_revoked(self, raw_partitions, callback_id):
        return self._invoke(self._listener.on_partitions_revoked, raw_partitions, callback_id)

    def _on_assigned(self, raw_partitions, callback_id):
        return self._invoke(self._listener.on_partitions_assigned, raw_partitions, callback_id)

    def _on_lost(self, raw_partitions, callback_id):
        method = getattr(self._listener, "on_partitions_lost", None)
        if not callable(method):
            # Java's ConsumerRebalanceListener.onPartitionsLost default body.
            method = self._listener.on_partitions_revoked
        return self._invoke(method, raw_partitions, callback_id)

    def _invoke(self, method, raw_partitions, callback_id):
        partitions = [TopicPartition(t, p) for (t, p) in raw_partitions]
        result = method(partitions)
        if not inspect.isawaitable(result):
            return None
        if self._loop is None or self._report is None:
            close = getattr(result, "close", None)
            if callable(close):
                close()  # suppress the unhelpful "never awaited" warning
            raise RuntimeError(
                "a coroutine rebalance listener requires an AsyncConsumer")
        report_ref = self._report

        def on_done(exc):
            report = report_ref()
            if report is not None:  # the consumer may be gone meanwhile
                report(callback_id, None if exc is None else (str(exc) or repr(exc)))

        _schedule(result, self._loop, on_done)
        return True


class _CommitCallbackAdapter:
    """Adapts a user ``callback(offsets, exception)`` to the C commit trampoline.

    Mirrors Java's ``OffsetCommitCallback.onComplete(Map, Exception)``:
    ``offsets`` is a ``dict[TopicPartition, OffsetAndMetadata]`` and
    ``exception`` is a :class:`KafkaError` or ``None``. Java's ``onComplete``
    returns ``void`` and has nowhere to report a failure of its own, so an
    exception raised here is logged and swallowed, and the trampoline reports
    the invocation to the client as soon as this returns.

    A callback that returns an awaitable is supported on an
    :class:`AsyncConsumer` (the invocation is pumped on the event loop, and the
    awaitable is scheduled there -- not awaited, since ``onComplete`` is void).
    The synchronous :class:`Consumer` has no loop, so a coroutine callback is
    rejected -- up front in :meth:`__init__` when it is recognizable as one
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

    def __call__(self, raw_offsets, err_tuple):
        offsets = _to_offset_map(raw_offsets)
        exception = KafkaError._from_tuple(err_tuple)
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

                def on_done(exc):
                    if exc is not None:
                        _log.error("Error in commit_async callback", exc_info=exc)

                _schedule(result, self._loop, on_done)
        except Exception:  # noqa: BLE001 - must not escape into the C caller
            _log.exception("Error in commit_async callback")


# --------------------------------------------------------------------------
# Re-entrancy handles.
# --------------------------------------------------------------------------
class ConsumerHandle:
    """A re-entrancy handle onto a live consumer.

    This is the Python equivalent of Java capturing the ``consumer`` variable
    inside a ``ConsumerRebalanceListener`` or ``OffsetCommitCallback``: it is how
    a callback reaches back into the consumer it belongs to. Obtain one with
    :meth:`Consumer.handle`.

    :meth:`wakeup` and the three state getters return immediately; every other
    method is a **blocking** call (GIL released) that runs the operation on the
    client's runtime and waits for it -- safe from inside a listener, whose
    consumer is busy with the operation that drove the callback. Only the
    operations the Rust handle exposes are available: there is no ``poll``,
    ``subscribe``, ``close`` or callback-taking commit.

    .. note::
       * Calling the *consumer's* own methods from inside a callback is
         rejected with a ``KafkaError`` (ConcurrentModification); handle methods
         bypass that guard by design.
       * A handle that outlives its consumer fails every operation with a
         ``KafkaError`` (IllegalState, "consumer destroyed"); the getters return
         empty collections. :meth:`destroy` frees the handle itself (the context
         manager form does it for you).
       * On a :class:`MockConsumer`-derived handle only :meth:`wakeup` works:
         the getters return empty collections and every other operation fails
         with an ``unsupported_version`` ``KafkaError`` (core behavior) -- drive
         the mock directly.
    """

    __slots__ = ("_h",)

    def __init__(self, h):
        self._h = h

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destroy()

    def destroy(self):
        """Free the handle. Idempotent."""
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
    def _void(self, fn, *args):
        _raise_if_error(fn(self._check(), *args))

    def _value(self, fn, *args):
        value, err = fn(self._check(), *args)
        _raise_if_error(err)
        return value

    def assign(self, partitions):
        """Assign partitions. An empty collection is rejected: on the consumer
        ``assign([])`` leaves the group, which this handle does not expose."""
        self._void(_lib.ConsumerHandle_assign, _tp_to_spec(partitions))

    def seek(self, partition, offset):
        """Seek a partition. ``offset`` is an int, or an :class:`OffsetAndMetadata`."""
        if isinstance(offset, OffsetAndMetadata):
            epoch = offset.leader_epoch if offset.leader_epoch is not None else -1
            self._void(_lib.ConsumerHandle_seek_with_metadata, partition.topic,
                       partition.partition, offset.offset, epoch, offset.metadata)
        else:
            self._void(_lib.ConsumerHandle_seek, partition.topic, partition.partition, offset)

    def seek_to_beginning(self, partitions):
        self._void(_lib.ConsumerHandle_seek_to_beginning, _tp_to_spec(partitions))

    def seek_to_end(self, partitions):
        self._void(_lib.ConsumerHandle_seek_to_end, _tp_to_spec(partitions))

    def pause(self, partitions):
        self._void(_lib.ConsumerHandle_pause, _tp_to_spec(partitions))

    def resume(self, partitions):
        self._void(_lib.ConsumerHandle_resume, _tp_to_spec(partitions))

    def position(self, partition, timeout=None):
        return self._value(_lib.ConsumerHandle_position, partition.topic,
                           partition.partition, _ms(timeout))

    def committed(self, partitions):
        return _to_offset_map(
            self._value(_lib.ConsumerHandle_committed, _tp_to_spec(partitions)))

    def beginning_offsets(self, partitions):
        return _to_long_map(
            self._value(_lib.ConsumerHandle_beginning_offsets, _tp_to_spec(partitions)))

    def end_offsets(self, partitions):
        return _to_long_map(
            self._value(_lib.ConsumerHandle_end_offsets, _tp_to_spec(partitions)))

    def offsets_for_times(self, timestamps):
        return _to_offset_and_timestamp_map(
            self._value(_lib.ConsumerHandle_offsets_for_times, _timestamps_to_spec(timestamps)))

    def commit_sync(self, offsets=None):
        """Commit synchronously -- Java ``commitSync()`` / ``commitSync(Map)``."""
        spec = None if offsets is None else _offsets_to_spec(offsets)
        self._void(_lib.ConsumerHandle_commit_sync, spec)

    def commit_async(self, offsets=None):
        """Initiate an asynchronous commit -- Java ``commitAsync()`` /
        ``commitAsync(Map)``. The handle exposes no completion-callback variant
        (matching the Rust handle); use :meth:`Consumer.commit_async` for that."""
        spec = None if offsets is None else _offsets_to_spec(offsets)
        self._void(_lib.ConsumerHandle_commit_async, spec)


class AsyncConsumerHandle(ConsumerHandle):
    """The :class:`AsyncConsumer` form of :class:`ConsumerHandle`: the blocking
    operations are coroutines driving the handle's ``_cb`` twins, whose
    completions the owning consumer's pump delivers on the event loop. Obtain
    one with :meth:`AsyncConsumer.handle`; it is what a coroutine rebalance
    listener uses to call back into the consumer.

    ``wakeup`` and the state getters are the inherited sync methods."""

    __slots__ = ()

    async def _run_cb(self, submit):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def deliver(payload):
            if not fut.done():
                fut.set_result(payload)

        def cb(*payload):
            # The pump normally runs on the loop thread, but a handle whose
            # consumer is already destroyed fires the callback inline, and
            # Consumer_destroy runs pending ones on the destroying thread:
            # always hop through call_soon_threadsafe.
            if not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(deliver, payload)
                except RuntimeError:
                    pass

        submit(cb)
        return await fut

    async def _void(self, fn, *args):  # noqa: D102  (async twin of the base helper)
        (err,) = await self._run_cb(lambda cb: fn(self._check(), *args, cb))
        _raise_if_error(err)

    async def _value(self, fn, *args):  # noqa: D102
        value, err = await self._run_cb(lambda cb: fn(self._check(), *args, cb))
        _raise_if_error(err)
        return value

    async def assign(self, partitions):
        await self._void(_lib.ConsumerHandle_assign_cb, _tp_to_spec(partitions))

    async def seek(self, partition, offset):
        if isinstance(offset, OffsetAndMetadata):
            epoch = offset.leader_epoch if offset.leader_epoch is not None else -1
            await self._void(_lib.ConsumerHandle_seek_with_metadata_cb, partition.topic,
                             partition.partition, offset.offset, epoch, offset.metadata)
        else:
            await self._void(_lib.ConsumerHandle_seek_cb, partition.topic,
                             partition.partition, offset)

    async def seek_to_beginning(self, partitions):
        await self._void(_lib.ConsumerHandle_seek_to_beginning_cb, _tp_to_spec(partitions))

    async def seek_to_end(self, partitions):
        await self._void(_lib.ConsumerHandle_seek_to_end_cb, _tp_to_spec(partitions))

    async def pause(self, partitions):
        await self._void(_lib.ConsumerHandle_pause_cb, _tp_to_spec(partitions))

    async def resume(self, partitions):
        await self._void(_lib.ConsumerHandle_resume_cb, _tp_to_spec(partitions))

    async def position(self, partition, timeout=None):
        return await self._value(_lib.ConsumerHandle_position_cb, partition.topic,
                                 partition.partition, _ms(timeout))

    async def committed(self, partitions):
        return _to_offset_map(await self._value(
            _lib.ConsumerHandle_committed_cb, _tp_to_spec(partitions)))

    async def beginning_offsets(self, partitions):
        return _to_long_map(await self._value(
            _lib.ConsumerHandle_beginning_offsets_cb, _tp_to_spec(partitions)))

    async def end_offsets(self, partitions):
        return _to_long_map(await self._value(
            _lib.ConsumerHandle_end_offsets_cb, _tp_to_spec(partitions)))

    async def offsets_for_times(self, timestamps):
        return _to_offset_and_timestamp_map(await self._value(
            _lib.ConsumerHandle_offsets_for_times_cb, _timestamps_to_spec(timestamps)))

    async def commit_sync(self, offsets=None):
        spec = None if offsets is None else _offsets_to_spec(offsets)
        await self._void(_lib.ConsumerHandle_commit_sync_cb, spec)

    async def commit_async(self, offsets=None):
        spec = None if offsets is None else _offsets_to_spec(offsets)
        await self._void(_lib.ConsumerHandle_commit_async_cb, spec)


# --------------------------------------------------------------------------
# Shared base: handle ownership and the non-blocking state reads.
# --------------------------------------------------------------------------
class _ConsumerBase:
    def __init__(self):
        self._h = None
        self.closed = False

    def _init_mock(self, auto_offset_reset="earliest"):
        self._h = _lib.Consumer_MockConsumer_new(auto_offset_reset)
        _lib.Consumer_set_callbacks_notify(self._h, self._notify_callable())

    def _init_kafka(self, config):
        self._h = _lib.Consumer_KafkaConsumer_new(config)
        _lib.Consumer_set_callbacks_notify(self._h, self._notify_callable())

    def _notify_callable(self):
        """The Python callable the client's notify hook invokes (``None`` for
        the synchronous consumer, which never queues a callback)."""
        return None

    def _listener_loop(self):
        """Event loop coroutine callbacks are scheduled on (``None`` for the
        synchronous consumer)."""
        return None

    def _check_closed(self):
        if self.closed:
            raise RuntimeError("Consumer is already closed")

    def _destroy(self):
        if self._h is not None:
            # Runs the still pending callbacks, then releases the registered
            # listener (and with it our reference to the user's listener).
            _lib.Consumer_destroy(self._h)
            self._h = None

    def _report_listener_result(self, callback_id, message):
        """Deliver a deferred listener outcome to the client (``None`` =
        success). A consumer destroyed meanwhile has no one to report to."""
        if self._h is not None:
            _lib.Consumer_set_callback_result(self._h, callback_id, message)

    def _listener_adapter(self, listener):
        if listener is None:
            return None
        loop = self._listener_loop()
        report = None if loop is None else self._report_listener_result
        return _ListenerAdapter(listener, loop, report)

    def _commit_adapter(self, callback):
        if callback is None:
            return None
        return _CommitCallbackAdapter(callback, self._listener_loop())

    def _handle_type(self):
        return ConsumerHandle

    def handle(self):
        """Return a re-entrancy handle for callback use.

        This is what a rebalance listener or commit callback uses to call back
        into the consumer -- the consumer's own methods would be rejected as
        concurrent access while the triggering operation is still in flight.
        :class:`Consumer` returns a :class:`ConsumerHandle`,
        :class:`AsyncConsumer` an :class:`AsyncConsumerHandle`."""
        self._check_closed()
        return self._handle_type()(_lib.Consumer_handle(self._h))

    # ---- non-blocking state reads (sync in Java; shared by both APIs) ------
    def assignment(self):
        self._check_closed()
        return {TopicPartition(t, p) for (t, p) in _lib.Consumer_assignment(self._h)}

    def subscription(self):
        self._check_closed()
        return set(_lib.Consumer_subscription(self._h))

    def paused(self):
        self._check_closed()
        return {TopicPartition(t, p) for (t, p) in _lib.Consumer_paused(self._h)}

    def metrics(self):
        """Point-in-time snapshot of the consumer's metrics.

        Returns a list of dicts with keys ``name``, ``group``, ``description``,
        ``tags`` (dict[str, str]), ``value`` (float / str / int depending on the
        metric) and ``kind`` (0=double, 1=string, 2=long, 3=int).

        ``kind`` is redundant for double/string but not for the integer cases:
        Rust distinguishes ``Long`` from ``Int`` while Python has a single
        ``int``, so ``kind`` is the only way to round-trip that faithfully.

        A list rather than a dict keyed by name: ``MetricName`` identity is
        (name, group, tags), so per-partition metrics share a name and differ
        only by tags. Callers that want a mapping should key on the whole
        triple.

        Values are measured once, when this is called -- the entries are not
        live handles. Empty while another operation is in flight.
        """
        self._check_closed()
        return _lib.Consumer_metrics(self._h)

    def group_metadata(self):
        # Returns a ConsumerGroupMetadata object that owns a fresh Rust handle
        # (freed on GC via its tp_dealloc). None means another operation is in
        # flight (the single-owner guard).
        self._check_closed()
        g = _lib.Consumer_group_metadata(self._h)
        if g is None:
            raise _concurrent_error()
        return g

    def client_id(self):
        self._check_closed()
        return _lib.Consumer_client_id(self._h)

    def current_lag(self, partition):
        """The lag of ``partition`` as an ``int``, or ``None`` when unknown
        (Java's ``OptionalLong.empty()``)."""
        self._check_closed()
        return _lib.Consumer_current_lag(self._h, partition.topic, partition.partition)

    def wakeup(self):
        """Abort the in-flight blocking operation with a ``Wakeup`` error, or
        the next one if none is in flight. Safe from any thread (including a
        signal handler); a no-op once the consumer is closed."""
        h = self._h
        if h is not None:
            _lib.Consumer_wakeup(h)


class _MockConsumerMixin:
    """Mock-only operations (test helper), shared by the sync and async mocks.
    :meth:`rebalance` is overridden by the async mock (it invokes the listener)."""

    def rebalance(self, partitions):
        """Drive a rebalance to ``partitions`` (Java ``MockConsumer.rebalance``).

        Invokes the registered rebalance listener on this thread and does not
        return until its callbacks have: ``on_partitions_revoked`` with the
        removed partitions (only when something was removed), then
        ``on_partitions_assigned`` with the *added* partitions -- which fires even
        when nothing was added, as long as a listener is registered.
        ``on_partitions_lost`` is never fired by the mock.

        Requires a topic subscription; a manually assigned consumer fails with
        "manual assignment in use". A listener exception surfaces here as a
        :class:`KafkaError` carrying its message.
        """
        self._check_closed()
        _raise_if_error(_lib.MockConsumer_rebalance(self._h, _tp_to_spec(partitions)))

    def add_record(self, topic, partition, offset, key=None, value=None):
        """Queue a record for the next ``poll`` (Java ``addRecord``). ``key`` and
        ``value`` are bytes-like or ``None``."""
        self._check_closed()
        _raise_if_error(_lib.MockConsumer_add_record(
            self._h, topic, partition, offset, key, value))

    def update_beginning_offsets(self, topic, partition, offset):
        self._check_closed()
        _lib.MockConsumer_update_beginning_offsets(self._h, [(topic, partition, offset)])

    def update_end_offsets(self, topic, partition, offset):
        self._check_closed()
        _lib.MockConsumer_update_end_offsets(self._h, [(topic, partition, offset)])

    def update_duration_offsets(self, topic, partition, offset):
        """The offset ``offsets_for_times`` answers for the partition
        (Java ``updateDurationOffsets``)."""
        self._check_closed()
        _lib.MockConsumer_update_duration_offsets(self._h, [(topic, partition, offset)])

    def update_partitions(self, topic, partition_count, leader_id=0,
                          leader_host="localhost", leader_port=9092):
        """Register ``partition_count`` partitions of ``topic`` led by one node
        (Java ``updatePartitions``)."""
        self._check_closed()
        _raise_if_error(_lib.MockConsumer_update_partitions(
            self._h, topic,
            [(p, leader_id, leader_host, leader_port) for p in range(partition_count)]))

    def set_poll_error(self, message, code=None):
        """Make the next ``poll`` fail with a ``KafkaError`` carrying
        ``message`` (Java ``setPollException``). ``code`` selects a specific
        error class (an ``_error_code`` value such as
        ``LOCAL_TIMEOUT``); by default the error is a plain ``KafkaException``."""
        self._check_closed()
        _lib.MockConsumer_set_poll_error(self._h, message, code)

    def set_offsets_error(self, message, code=None):
        """Make the next ``beginning_offsets`` / ``end_offsets`` /
        ``offsets_for_times`` fail (Java ``setOffsetsException``)."""
        self._check_closed()
        _lib.MockConsumer_set_offsets_error(self._h, message, code)

    def set_max_poll_records(self, max_poll_records):
        self._check_closed()
        _raise_if_error(_lib.MockConsumer_set_max_poll_records(self._h, max_poll_records))

    def should_rebalance(self):
        """Whether ``enforce_rebalance`` was called (Java ``shouldRebalance``)."""
        self._check_closed()
        return _lib.MockConsumer_should_rebalance(self._h)

    def reset_should_rebalance(self):
        self._check_closed()
        _lib.MockConsumer_reset_should_rebalance(self._h)

    def last_poll_timeout(self):
        """The timeout of the last ``poll`` in milliseconds (Java
        ``lastPollTimeout``)."""
        self._check_closed()
        return _lib.MockConsumer_last_poll_timeout(self._h)


# --------------------------------------------------------------------------
# Synchronous API.
# --------------------------------------------------------------------------
class Consumer(_ConsumerBase):
    """A synchronous Kafka consumer. Every method that blocks in Java is a
    blocking call here, made with the GIL released. Rebalance listeners and
    commit callbacks run on the calling thread, inside the call that triggers
    them. Use :meth:`wakeup` from another thread to interrupt a blocking call."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _void(self, fn, *args):
        self._check_closed()
        _raise_if_error(fn(self._h, *args))

    def _value(self, fn, *args):
        self._check_closed()
        value, err = fn(self._h, *args)
        _raise_if_error(err)
        return value

    def poll(self, timeout):
        """Fetch records, waiting up to ``timeout`` seconds -- Java ``poll(Duration)``."""
        return ConsumerRecords(self._value(_lib.Consumer_poll, _ms(timeout)))

    def subscribe(self, topics, listener=None):
        """Subscribe to ``topics``, optionally with a rebalance ``listener``.

        ``listener`` is any object with ``on_partitions_revoked(partitions)`` and
        ``on_partitions_assigned(partitions)`` methods, plus an optional
        ``on_partitions_lost(partitions)`` (which otherwise delegates to
        ``on_partitions_revoked``, as in Java). Each receives a
        ``list[TopicPartition]``.

        The listener registration is released when a later ``subscribe`` replaces
        it, or when the consumer is closed/destroyed -- **not** by
        :meth:`unsubscribe`, matching Java's
        ``SubscriptionState.unsubscribe()``, which leaves the listener in place.

        Listener methods run on the thread calling ``poll`` (or any other
        operation that drives a rebalance), and the rebalance does not complete
        until they return (Java's guarantee). To call back into the consumer
        from a listener -- to flush offsets with ``commit_sync`` before
        partitions are taken away, say -- use :meth:`handle`; the consumer's own
        methods would be rejected as concurrent access.
        """
        self._void(_lib.Consumer_subscribe, list(topics), self._listener_adapter(listener))

    def subscribe_pattern(self, pattern, listener=None):
        """Subscribe to every topic matching the RE2 ``pattern`` -- Java's
        ``subscribe(SubscriptionPattern[, listener])``, evaluated by the group
        coordinator (KIP-848)."""
        self._void(_lib.Consumer_subscribe_pattern, pattern, self._listener_adapter(listener))

    def unsubscribe(self):
        self._void(_lib.Consumer_unsubscribe)

    def assign(self, partitions):
        self._void(_lib.Consumer_assign, _tp_to_spec(partitions))

    def pause(self, partitions):
        self._void(_lib.Consumer_pause, _tp_to_spec(partitions))

    def resume(self, partitions):
        self._void(_lib.Consumer_resume, _tp_to_spec(partitions))

    def seek(self, partition, offset):
        """Seek a partition. ``offset`` is an int, or an :class:`OffsetAndMetadata`.

        Java's ``seek`` does not block, but the Rust consumer's awaits its
        background task (which may run the rebalance listener on the way), so it
        is a blocking call like the others."""
        if isinstance(offset, OffsetAndMetadata):
            epoch = offset.leader_epoch if offset.leader_epoch is not None else -1
            self._void(_lib.Consumer_seek_with_metadata, partition.topic,
                       partition.partition, offset.offset, epoch, offset.metadata)
        else:
            self._void(_lib.Consumer_seek, partition.topic, partition.partition, offset)

    def seek_to_beginning(self, partitions):
        self._void(_lib.Consumer_seek_to_beginning, _tp_to_spec(partitions))

    def seek_to_end(self, partitions):
        self._void(_lib.Consumer_seek_to_end, _tp_to_spec(partitions))

    def commit(self, offsets=None, timeout=None):
        """Commit synchronously -- Java ``commitSync`` in all four forms
        (``()``, ``(Duration)``, ``(Map)``, ``(Map, Duration)``)."""
        spec = None if offsets is None else _offsets_to_spec(offsets)
        self._void(_lib.Consumer_commit_sync, spec, _ms(timeout))

    commit_sync = commit

    def commit_async(self, offsets=None, callback=None):
        """Initiate an asynchronous commit.

        Covers all three Java overloads: ``commitAsync()``,
        ``commitAsync(callback)`` and ``commitAsync(offsets, callback)``.

        Args:
            offsets: optional ``dict[TopicPartition, OffsetAndMetadata]`` to
                commit; the current positions are committed when omitted.
            callback: optional ``callback(offsets, exception)`` -- Java's
                ``OffsetCommitCallback``. ``offsets`` is a
                ``dict[TopicPartition, OffsetAndMetadata]`` and ``exception`` a
                :class:`KafkaError` or ``None``. It must not be a coroutine
                function on this synchronous consumer (there is no event loop to
                run it on, so one is rejected with ``TypeError`` here); use an
                :class:`AsyncConsumer` for that.

        ``callback`` runs on the calling thread of the operation that delivers
        it (a later ``poll`` / ``commit`` / ``close``; a :class:`MockConsumer`
        delivers it inside this very call), and that operation does not return
        until it does -- matching Java, which runs ``onComplete`` on the polling
        thread. To touch the consumer from inside it, use :meth:`handle`.
        """
        spec = None if offsets is None else _offsets_to_spec(offsets)
        self._void(_lib.Consumer_commit_async, spec, self._commit_adapter(callback))

    def position(self, partition, timeout=None):
        return self._value(_lib.Consumer_position, partition.topic, partition.partition,
                           _ms(timeout))

    def committed(self, partitions, timeout=None):
        return _to_offset_map(self._value(
            _lib.Consumer_committed, _tp_to_spec(partitions), _ms(timeout)))

    def offsets_for_times(self, timestamps, timeout=None):
        return _to_offset_and_timestamp_map(self._value(
            _lib.Consumer_offsets_for_times, _timestamps_to_spec(timestamps), _ms(timeout)))

    def beginning_offsets(self, partitions, timeout=None):
        return _to_long_map(self._value(
            _lib.Consumer_beginning_offsets, _tp_to_spec(partitions), _ms(timeout)))

    def end_offsets(self, partitions, timeout=None):
        return _to_long_map(self._value(
            _lib.Consumer_end_offsets, _tp_to_spec(partitions), _ms(timeout)))

    def partitions_for(self, topic, timeout=None):
        return _to_partition_info_list(self._value(
            _lib.Consumer_partitions_for, topic, _ms(timeout)))

    def list_topics(self, timeout=None):
        return _to_topics_map(self._value(_lib.Consumer_list_topics, _ms(timeout)))

    def enforce_rebalance(self, reason=None):
        """Request a rebalance -- Java ``enforceRebalance([reason])``."""
        if reason is None:
            self._void(_lib.Consumer_enforce_rebalance)
        else:
            self._void(_lib.Consumer_enforce_rebalance_with_reason, reason)

    def close(self, timeout=None):
        """Close the consumer -- Java ``close()`` or ``close(CloseOptions.timeout(..))``
        when ``timeout`` (seconds) is given. Idempotent."""
        if self.closed:
            return
        self.closed = True
        try:
            _raise_if_error(_lib.Consumer_close(self._h, _ms(timeout)))
        finally:
            self._destroy()


# --------------------------------------------------------------------------
# Asyncio-native API.
# --------------------------------------------------------------------------
class AsyncConsumer(_ConsumerBase):
    """An asyncio-native Kafka consumer. Every method that blocks in Java is a
    coroutine driving the client's ``_cb`` entry point; the completion -- and
    every rebalance-listener or commit-callback invocation on the way -- is
    queued by the client and run by this consumer's callback pump on the event
    loop thread. Listener methods and commit callbacks may be coroutines.

    Cancelling an awaiting coroutine (``asyncio.wait_for`` etc.) does not abort
    the Rust-side operation: call :meth:`wakeup` to do that, and the operation
    completes with a ``Wakeup`` error."""

    def __init__(self):
        super().__init__()
        self._loop = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def _notify_callable(self):
        return self._on_notify

    def _on_notify(self):
        """The C notify hook: runs on whichever thread queued the first
        callback (a Rust task). It only schedules; never runs callbacks."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(self._pump)
        except RuntimeError:
            pass  # loop closed between the check and the call

    def _pump(self):
        """Run every queued callback on the event loop thread."""
        if self._h is None:
            return
        while _lib.Consumer_execute_callbacks(self._h) > 0:
            pass

    def _bind_loop(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        return loop

    def _listener_loop(self):
        # Coroutine callbacks are scheduled back onto this consumer's loop. The
        # loop is bound by the first coroutine (subscribe / commit_async are
        # coroutines themselves), so it is known by the time an adapter is built.
        if self._loop is None:
            self._loop = _running_loop()
        return self._loop

    def _handle_type(self):
        return AsyncConsumerHandle

    def _deliver(self, loop, fn, *args):
        """Run ``fn(*args)`` on the loop thread. Callbacks normally already run
        there (the pump is scheduled on the loop) but ``Consumer_destroy`` runs
        the still-pending ones on the thread that destroys the handle, so
        always hop through ``call_soon_threadsafe``."""
        if loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(fn, *args)
        except RuntimeError:
            pass

    async def _run_cb(self, submit):
        """Submit a ``_cb`` op and ``await`` its completion.

        ``submit(cb)`` registers ``cb(payload...)``, which the pump runs on the
        loop thread once the op completes; the payload is handed to the
        awaiting coroutine through an asyncio future. A late completion for an
        awaiter that was cancelled is simply dropped -- the payload holds no
        native handles, only copied Python values."""
        loop = self._bind_loop()
        fut = loop.create_future()

        def deliver(payload):
            if not fut.done():
                fut.set_result(payload)

        def cb(*payload):
            self._deliver(loop, deliver, payload)

        submit(cb)
        return await fut

    async def _void(self, fn, *args):
        self._check_closed()
        (err,) = await self._run_cb(lambda cb: fn(self._h, *args, cb))
        _raise_if_error(err)

    async def _value(self, fn, *args):
        self._check_closed()
        value, err = await self._run_cb(lambda cb: fn(self._h, *args, cb))
        _raise_if_error(err)
        return value

    async def poll(self, timeout):
        """Fetch records, waiting up to ``timeout`` seconds -- Java ``poll(Duration)``."""
        return ConsumerRecords(await self._value(_lib.Consumer_poll_cb, _ms(timeout)))

    async def subscribe(self, topics, listener=None):
        """Subscribe to ``topics``, optionally with a rebalance ``listener``.

        Same contract as :meth:`Consumer.subscribe`; the listener methods may
        additionally be coroutines. Every listener invocation is pumped on the
        event loop: a plain method runs there directly, a coroutine method is
        scheduled as a task and the rebalance waits for it to finish. A
        coroutine listener that needs the consumer uses :meth:`handle` (an
        :class:`AsyncConsumerHandle`); awaiting this consumer's own methods from
        inside it is rejected as concurrent access, like in Java.
        """
        await self._void(_lib.Consumer_subscribe_cb, list(topics),
                         self._listener_adapter(listener))

    async def subscribe_pattern(self, pattern, listener=None):
        """Subscribe to every topic matching the RE2 ``pattern`` -- Java's
        ``subscribe(SubscriptionPattern[, listener])``."""
        await self._void(_lib.Consumer_subscribe_pattern_cb, pattern,
                         self._listener_adapter(listener))

    async def unsubscribe(self):
        await self._void(_lib.Consumer_unsubscribe_cb)

    async def assign(self, partitions):
        await self._void(_lib.Consumer_assign_cb, _tp_to_spec(partitions))

    async def pause(self, partitions):
        await self._void(_lib.Consumer_pause_cb, _tp_to_spec(partitions))

    async def resume(self, partitions):
        await self._void(_lib.Consumer_resume_cb, _tp_to_spec(partitions))

    async def seek(self, partition, offset):
        """Seek a partition. ``offset`` is an int, or an :class:`OffsetAndMetadata`.

        A coroutine (unlike Java's non-blocking ``seek``) because the Rust
        consumer's ``seek`` awaits the background task, which may run a rebalance
        listener on the way."""
        if isinstance(offset, OffsetAndMetadata):
            epoch = offset.leader_epoch if offset.leader_epoch is not None else -1
            await self._void(_lib.Consumer_seek_with_metadata_cb, partition.topic,
                             partition.partition, offset.offset, epoch, offset.metadata)
        else:
            await self._void(_lib.Consumer_seek_cb, partition.topic,
                             partition.partition, offset)

    async def seek_to_beginning(self, partitions):
        await self._void(_lib.Consumer_seek_to_beginning_cb, _tp_to_spec(partitions))

    async def seek_to_end(self, partitions):
        await self._void(_lib.Consumer_seek_to_end_cb, _tp_to_spec(partitions))

    async def commit(self, offsets=None, timeout=None):
        """Commit synchronously -- Java ``commitSync`` in all four forms."""
        spec = None if offsets is None else _offsets_to_spec(offsets)
        await self._void(_lib.Consumer_commit_sync_cb, spec, _ms(timeout))

    commit_sync = commit

    async def commit_async(self, offsets=None, callback=None):
        """Initiate an asynchronous commit -- Java ``commitAsync()`` /
        ``commitAsync(callback)`` / ``commitAsync(offsets, callback)``.

        A coroutine because the Rust consumer's ``commitAsync`` awaits the
        background task that enqueues the request; it still completes as soon
        as the commit is *initiated*. ``callback(offsets, exception)`` fires on
        the event loop when the commit completes (delivered by a later ``poll``
        / ``commit`` / ``close``; a :class:`AsyncMockConsumer` delivers it
        within this call) and may be a coroutine function, in which case the
        coroutine is scheduled on the loop (Java's ``onComplete`` is void, so
        nothing waits for it).
        """
        spec = None if offsets is None else _offsets_to_spec(offsets)
        await self._void(_lib.Consumer_commit_async_cb, spec, self._commit_adapter(callback))

    async def position(self, partition, timeout=None):
        return await self._value(_lib.Consumer_position_cb, partition.topic,
                                 partition.partition, _ms(timeout))

    async def committed(self, partitions, timeout=None):
        return _to_offset_map(await self._value(
            _lib.Consumer_committed_cb, _tp_to_spec(partitions), _ms(timeout)))

    async def offsets_for_times(self, timestamps, timeout=None):
        return _to_offset_and_timestamp_map(await self._value(
            _lib.Consumer_offsets_for_times_cb, _timestamps_to_spec(timestamps), _ms(timeout)))

    async def beginning_offsets(self, partitions, timeout=None):
        return _to_long_map(await self._value(
            _lib.Consumer_beginning_offsets_cb, _tp_to_spec(partitions), _ms(timeout)))

    async def end_offsets(self, partitions, timeout=None):
        return _to_long_map(await self._value(
            _lib.Consumer_end_offsets_cb, _tp_to_spec(partitions), _ms(timeout)))

    async def partitions_for(self, topic, timeout=None):
        return _to_partition_info_list(await self._value(
            _lib.Consumer_partitions_for_cb, topic, _ms(timeout)))

    async def list_topics(self, timeout=None):
        return _to_topics_map(await self._value(_lib.Consumer_list_topics_cb, _ms(timeout)))

    async def enforce_rebalance(self, reason=None):
        """Request a rebalance -- Java ``enforceRebalance([reason])``."""
        if reason is None:
            await self._void(_lib.Consumer_enforce_rebalance_cb)
        else:
            await self._void(_lib.Consumer_enforce_rebalance_with_reason_cb, reason)

    async def close(self, timeout=None):
        """Close the consumer -- Java ``close()`` or ``close(CloseOptions.timeout(..))``
        when ``timeout`` (seconds) is given. Idempotent."""
        if self.closed:
            return
        self.closed = True
        try:
            (err,) = await self._run_cb(
                lambda cb: _lib.Consumer_close_cb(self._h, _ms(timeout), cb))
            _raise_if_error(err)
        finally:
            self._destroy()


class _AsyncMockConsumerMixin(_MockConsumerMixin):
    async def rebalance(self, partitions):
        """Drive a rebalance to ``partitions`` (Java ``MockConsumer.rebalance``).

        The async twin of :meth:`_MockConsumerMixin.rebalance`: the listener
        invocations are pumped on the event loop (coroutine listener methods are
        scheduled there), and this coroutine completes once the rebalance --
        including every listener callback -- has.
        """
        self._check_closed()
        (err,) = await self._run_cb(
            lambda cb: _lib.MockConsumer_rebalance_cb(self._h, _tp_to_spec(partitions), cb))
        _raise_if_error(err)


# --------------------------------------------------------------------------
# Concrete variants.
# --------------------------------------------------------------------------
class KafkaConsumer(Consumer):
    """A synchronous consumer connected to a real cluster.

    Args:
        config: dict of configuration properties (e.g. ``bootstrap.servers``,
            ``group.id``, ``group.protocol=consumer``). Keys and values are
            strings. A rejected configuration raises ``RuntimeError`` with the
            client's message. Records carry raw ``bytes`` keys and values.
    """

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class MockConsumer(_MockConsumerMixin, Consumer):
    """The synchronous in-memory test double (Java ``MockConsumer``).

    Args:
        auto_offset_reset: ``"earliest"``, ``"latest"`` or ``"none"``.
    """

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


class AsyncMockConsumer(_AsyncMockConsumerMixin, AsyncConsumer):
    """The asyncio-native in-memory test double."""

    def __init__(self, auto_offset_reset="earliest"):
        super().__init__()
        self._init_mock(auto_offset_reset)
