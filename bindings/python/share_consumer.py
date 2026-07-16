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

"""Pythonic KIP-932 share consumer over the Rust share-consumer C FFI.

Mirrors Java's blocking ``KafkaShareConsumer`` with Python naming. This phase
ships the synchronous API (:class:`KafkaShareConsumer`, :class:`MockShareConsumer`);
an asyncio-native variant is a straightforward additive follow-on.

Design notes
------------
* The C extension (``_confluentkafka``) is a marshaling layer only: it converts
  Python objects to/from the C FFI and bridges the FFI's async callbacks back
  into Python. All orchestration lives here in pure Python.
* Even the synchronous methods submit an async FFI op and then wait on an
  interruptible Python primitive, so a long ``poll`` never parks the calling
  thread inside a native ``block_on`` where Python signal handlers cannot run.
  On ``KeyboardInterrupt`` the waiter calls :meth:`wakeup`, drains the in-flight
  op (releasing the access guard), and re-raises.
* Key/value/header bytes are exposed as zero-copy ``memoryview`` objects backed
  by the record batch; they stay valid while the owning record (and its batch)
  is alive.
* ``acknowledge`` borrows the live poll batch through the record it is given —
  the batch-keepalive chain guarantees the underlying record stays valid.
"""

import datetime as _dt
import enum
import threading

import _confluentkafka as _lib
from producer import KafkaError  # shared error type


# --------------------------------------------------------------------------
# Value types.
# --------------------------------------------------------------------------
class AcknowledgeType(enum.IntEnum):
    """How a delivered record was handled (KIP-932)."""

    ACCEPT = _lib.AcknowledgeType_ACCEPT
    RELEASE = _lib.AcknowledgeType_RELEASE
    REJECT = _lib.AcknowledgeType_REJECT
    RENEW = _lib.AcknowledgeType_RENEW


class TopicIdPartition:
    """A (topic, topic-id, partition) triple. ``topic_id`` is the 16 raw
    big-endian UUID bytes of the topic (share-group specific)."""

    __slots__ = ("topic", "topic_id", "partition")

    def __init__(self, topic, topic_id, partition):
        self.topic = topic
        self.topic_id = topic_id
        self.partition = partition

    def __eq__(self, other):
        return (isinstance(other, TopicIdPartition)
                and self.topic == other.topic
                and self.topic_id == other.topic_id
                and self.partition == other.partition)

    def __hash__(self):
        return hash((self.topic, self.topic_id, self.partition))

    def __repr__(self):
        return (f"TopicIdPartition(topic={self.topic!r}, "
                f"topic_id={self.topic_id!r}, partition={self.partition})")


class ConsumerRecords:
    """An iterable batch of records returned by ``poll``.

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
# Conversions from the C drain structures to the Python value types.
# --------------------------------------------------------------------------
def _make_kafka_error(code, message, is_retriable, is_fatal):
    """Build a :class:`KafkaError` from fields extracted C-side. Used where the
    underlying error handle is borrowed (``ShareCommitResult``) or already
    consumed (the ack-commit callback), so ``KafkaError._from_c`` — which owns
    and destroys a handle — cannot apply."""
    e = KafkaError.__new__(KafkaError)
    e._code = code
    e._message = message
    e._is_retriable = is_retriable
    e._is_fatal = is_fatal
    return e


def _to_topic_id_partition(raw_key):
    topic, topic_id, partition = raw_key
    return TopicIdPartition(topic, topic_id, partition)


def _to_share_commit_map(raw):
    return {_to_topic_id_partition(k): (_make_kafka_error(*v) if v is not None else None)
            for k, v in raw.items()}


def _to_share_ack_offsets(raw):
    return {_to_topic_id_partition(k): offsets for k, offsets in raw.items()}


def _ms(timeout):
    """Convert a timeout (seconds float, or ``timedelta``) to int64 ms."""
    if isinstance(timeout, _dt.timedelta):
        return int(timeout.total_seconds() * 1000)
    return int(float(timeout) * 1000)


# --------------------------------------------------------------------------
# Shared base: handle ownership, the interruptible sync waiter, and the
# per-method (submit, resolve, free) specs.
# --------------------------------------------------------------------------
class _ShareConsumerBase:
    def __init__(self):
        self._h = None
        self.closed = False
        # The currently-registered ack-commit bridge (or None). The extension
        # holds an INCREF on it too; the wrapper tracks it so it can be passed
        # as `old_cb` for the extension's DECREF on replace/clear.
        self._ack_commit_bridge = None

    def _init_mock(self):
        self._h = _lib.MockShareConsumer_new()

    def _init_kafka(self, config):
        self._h = _lib.KafkaShareConsumer_new(config)

    def _check_closed(self):
        if self.closed:
            raise RuntimeError("ShareConsumer is already closed")

    def _destroy(self):
        if self._h is not None:
            _lib.ShareConsumer_destroy(self._h)
            self._h = None

    # ---- non-blocking ops (sync in Java) -----------------------------------
    def wakeup(self):
        if self._h is not None:
            _lib.ShareConsumer_wakeup(self._h)

    def subscription(self):
        self._check_closed()
        subs, err = _lib.ShareConsumer_subscription(self._h)
        if err:
            raise KafkaError._from_c(err)
        return set(subs)

    def acquisition_lock_timeout_ms(self):
        """Acquisition-lock timeout (ms) for the last fetched records, or None
        when the broker did not report one (e.g. the mock)."""
        self._check_closed()
        value, err = _lib.ShareConsumer_acquisition_lock_timeout_ms(self._h)
        if err:
            raise KafkaError._from_c(err)
        return value

    def acknowledge(self, record_or_topic, *rest):
        """Acknowledge a delivered record.

        Two forms:
          * ``acknowledge(record[, type])`` — acknowledge a record from the last
            poll batch (defaults to ``ACCEPT``).
          * ``acknowledge(topic, partition, offset[, type])`` — acknowledge by
            coordinates without borrowing a batch (defaults to ``ACCEPT``).
        """
        self._check_closed()
        if isinstance(record_or_topic, str):
            partition, offset = rest[0], rest[1]
            ack_type = int(rest[2]) if len(rest) > 2 else int(AcknowledgeType.ACCEPT)
            err = _lib.ShareConsumer_acknowledge_by_offset(
                self._h, record_or_topic, partition, offset, ack_type)
        elif rest:
            err = _lib.ShareConsumer_acknowledge_with_type(
                self._h, record_or_topic, int(rest[0]))
        else:
            err = _lib.ShareConsumer_acknowledge(self._h, record_or_topic)
        if err:
            raise KafkaError._from_c(err)

    def commit_async(self):
        """Commit the acknowledgements for the last poll without waiting for the
        broker (Java ``commitAsync()``); the registered ack-commit callback, if
        any, fires when the commit completes."""
        self._check_closed()
        err = _lib.ShareConsumer_commit_async(self._h)
        if err:
            raise KafkaError._from_c(err)

    def set_acknowledgement_commit_callback(self, callback):
        """Register (or, with ``None``, clear) the callback invoked when an
        acknowledgement commit completes. The callback receives
        ``(offsets, error)`` where ``offsets`` is a
        ``dict[TopicIdPartition, set[int]]`` and ``error`` is a
        :class:`KafkaError` or ``None``."""
        old = self._ack_commit_bridge
        new = None
        if callback is not None:
            def bridge(raw_offsets, err_fields):
                error = _make_kafka_error(*err_fields) if err_fields is not None else None
                callback(_to_share_ack_offsets(raw_offsets), error)
            new = bridge
        err = _lib.ShareConsumer_set_acknowledgement_commit_callback(self._h, new, old)
        if err:
            raise KafkaError._from_c(err)
        # Only now that Rust accepted the registration does the extension's
        # INCREF/DECREF bookkeeping match this tracked reference.
        self._ack_commit_bridge = new

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
        return (lambda cb: _lib.ShareConsumer_poll_async(self._h, ms, cb),
                self._resolve_poll, self._free_poll)

    def _subscribe_spec(self, topics):
        topics = list(topics)
        return (lambda cb: _lib.ShareConsumer_subscribe_async(self._h, topics, cb),
                self._resolve_void, self._free_void)

    def _unsubscribe_spec(self):
        return (lambda cb: _lib.ShareConsumer_unsubscribe_async(self._h, cb),
                self._resolve_void, self._free_void)

    def _close_spec(self):
        return (lambda cb: _lib.ShareConsumer_close_async(self._h, cb),
                self._resolve_void, self._free_void)

    def _commit_spec(self, timeout_ms):
        drain = _lib.ShareCommitResult_drain
        if timeout_ms is None:
            submit = lambda cb: _lib.ShareConsumer_commit_sync_async(self._h, cb)
        else:
            submit = lambda cb: _lib.ShareConsumer_commit_sync_timeout_async(
                self._h, timeout_ms, cb)
        return (submit, self._resolve_value(drain, _to_share_commit_map),
                self._free_value(drain))


# --------------------------------------------------------------------------
# Synchronous API.
# --------------------------------------------------------------------------
class ShareConsumer(_ShareConsumerBase):
    """A synchronous share consumer. Blocking methods submit an async FFI op and
    wait on an interruptible event, so ``KeyboardInterrupt`` is honored promptly
    (translated into a Rust-side ``wakeup``)."""

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

    def subscribe(self, topics):
        self._check_closed()
        return self._run_sync(*self._subscribe_spec(topics))

    def unsubscribe(self):
        self._check_closed()
        return self._run_sync(*self._unsubscribe_spec())

    def poll(self, timeout):
        self._check_closed()
        return self._run_sync(*self._poll_spec(timeout))

    def commit_sync(self):
        """Commit the acknowledgements for the last poll, waiting up to the
        default API timeout; returns ``dict[TopicIdPartition, KafkaError | None]``
        (a null per-partition error means that partition committed)."""
        self._check_closed()
        return self._run_sync(*self._commit_spec(None))

    def commit_sync_timeout(self, timeout):
        """Like :meth:`commit_sync` but bounded by ``timeout`` (seconds float or
        ``timedelta``)."""
        self._check_closed()
        return self._run_sync(*self._commit_spec(_ms(timeout)))

    def close(self, timeout=None):
        if self.closed:
            return
        # Release the persistent ack-commit callback (dropping the extension's
        # INCREF) while the consumer is still open; the mock never fires it, so
        # this is race-free here.
        if self._ack_commit_bridge is not None:
            try:
                self.set_acknowledgement_commit_callback(None)
            except KafkaError:
                pass
        self.closed = True
        try:
            self._run_sync(*self._close_spec())
        finally:
            self._destroy()


# --------------------------------------------------------------------------
# Concrete variants.
# --------------------------------------------------------------------------
class KafkaShareConsumer(ShareConsumer):
    """A synchronous share consumer connected to a real cluster.

    Args:
        config: dict of configuration properties (e.g. ``bootstrap.servers``,
            ``group.id``).
    """

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class MockShareConsumer(ShareConsumer):
    """A broker-less mock share consumer for tests."""

    def __init__(self):
        super().__init__()
        self._init_mock()

    def add_record(self, topic, partition, offset, key=None, value=None):
        """Enqueue a record on a subscribed topic-partition (mock only)."""
        # The FFI takes key/value before offset; the reorder lives here so the C
        # entry point stays a straight pass-through.
        err = _lib.MockShareConsumer_add_record(
            self._h, topic, partition, key, value, offset)
        if err:
            raise KafkaError._from_c(err)

    def set_client_instance_id(self, instance_id):
        """Set the client instance id returned by the mock. ``instance_id`` must
        be exactly 16 raw UUID bytes."""
        _lib.MockShareConsumer_set_client_instance_id(self._h, instance_id)
