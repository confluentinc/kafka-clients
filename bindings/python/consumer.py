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

Out of scope (the FFI bridges no callbacks into the embedding language):
rebalance listeners, ``commitAsync`` completion callbacks, pattern
subscription, ``metrics()`` / ``clientInstanceId()``.
"""

import asyncio
import datetime as _dt
import threading

import _confluentkafka as _lib
from producer import KafkaError  # shared error type


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


def _concurrent_error():
    return RuntimeError("KafkaConsumer is not safe for multi-threaded access.")


# --------------------------------------------------------------------------
# Shared base: handle ownership, non-blocking state reads, and the per-method
# (submit, resolve, free) specs driven by _run_sync / _run_async.
# --------------------------------------------------------------------------
class _ConsumerBase:
    def __init__(self):
        self._h = None
        self.closed = False

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
    def seek(self, partition, offset):
        """Seek a partition. ``offset`` is an int, or an :class:`OffsetAndMetadata`."""
        if isinstance(offset, OffsetAndMetadata):
            epoch = offset.leader_epoch if offset.leader_epoch is not None else -1
            e = _lib.Consumer_seek_with_metadata(
                self._h, partition.topic, partition.partition,
                offset.offset, epoch, offset.metadata or "")
        else:
            e = _lib.Consumer_seek(self._h, partition.topic, partition.partition, offset)
        if e:
            raise KafkaError._from_c(e)

    def enforce_rebalance(self, reason=None):
        e = _lib.Consumer_enforce_rebalance(self._h, reason)
        if e:
            raise KafkaError._from_c(e)

    def commit_async(self):
        """Initiate an asynchronous commit of the current positions and return
        immediately (Java ``commitAsync()``; the completion callback variant is
        not bridged — see module docstring)."""
        e = _lib.Consumer_commit_async(self._h)
        if e:
            raise KafkaError._from_c(e)

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

    def _subscribe_spec(self, topics):
        topics = list(topics)
        return (lambda cb: _lib.Consumer_subscribe_async(self._h, topics, cb),
                self._resolve_void, self._free_void)

    def _unsubscribe_spec(self):
        return (lambda cb: _lib.Consumer_unsubscribe_async(self._h, cb),
                self._resolve_void, self._free_void)

    def _tp_op_spec(self, fn, partitions):
        tps = [(tp.topic, tp.partition) for tp in partitions]
        return (lambda cb: fn(self._h, tps, cb), self._resolve_void, self._free_void)

    def _commit_spec(self, offsets):
        if offsets is None:
            return (lambda cb: _lib.Consumer_commit_sync_async(self._h, cb),
                    self._resolve_void, self._free_void)
        spec = [(tp.topic, tp.partition, oam.offset,
                 oam.leader_epoch if oam.leader_epoch is not None else -1,
                 oam.metadata if oam.metadata is not None else "")
                for tp, oam in offsets.items()]
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

    def subscribe(self, topics):
        self._check_closed()
        return self._run_sync(*self._subscribe_spec(topics))

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

    @staticmethod
    def _fail(fut, err):
        # Event-loop thread. Fail the awaiter unless it's already resolved.
        if fut.cancelled() or fut.done():
            return
        fut.set_exception(err)

    async def _run_async(self, submit, resolve, free):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def cb(*payload):
            # Dispatcher thread, GIL held. INVARIANT (rules 1.2/1.3): never leave
            # `fut` unresolved and never drop the C handles — on every path.
            try:
                if loop.is_closed():
                    free(payload)
                    return
                loop.call_soon_threadsafe(self._deliver, fut, payload, free)
            except BaseException as err:
                # call_soon_threadsafe can raise if the loop closed after the
                # is_closed() check. Release the handles, then wake the awaiter
                # with the error so `await fut` doesn't hang forever.
                free(payload)
                try:
                    loop.call_soon_threadsafe(self._fail, fut, err)
                except BaseException:
                    pass  # loop truly gone -> the awaiter is gone too

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

    async def subscribe(self, topics):
        self._check_closed()
        return await self._run_async(*self._subscribe_spec(topics))

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
