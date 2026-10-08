import asyncio
import logging
import time
import _confluentkafka as _lib
from _confluentkafka import ProducerRecord
from _sync_wait import SyncWaiter, _SLICE_SECONDS
from concurrent.futures import CancelledError, TimeoutError as FutureTimeoutError

_log = logging.getLogger(__name__)

# The sync producer waits in slices of this many milliseconds (one
# ``Producer_poll`` call per slice, GIL released inside) so a pending
# ``KeyboardInterrupt`` is raised between two slices.
_SLICE_MS = int(_SLICE_SECONDS * 1000)


# ProducerRecord is a C extension type imported from _confluentkafka module.
# It holds the topic, partition, timestamp and the key / value bytes objects;
# the C extension builds the kafka_producer_ProducerRecord_t over those bytes
# (zero-copy) for the duration of each send.


class KafkaError(Exception):
    """Kafka error with code, message, and retriable/fatal flags."""

    def __init__(self):
        raise NotImplementedError()

    def __str__(self):
        return self._message if self._message is not None else ""

    @staticmethod
    def _from_c(_id: int):
        """Build a KafkaError from an OWNED C error handle and destroy it.

        Used by the admin binding, whose completion trampolines still hand
        error handles to Python as ints; the producer and consumer bindings
        hand error tuples (see :meth:`_from_tuple`)."""
        ret = KafkaError.__new__(KafkaError)
        ret._code = _lib.KafkaError_code(_id)
        ret._message = _lib.KafkaError_message(_id)
        ret._is_retriable = _lib.KafkaError_is_retriable(_id)
        ret._is_fatal = _lib.KafkaError_is_fatal(_id)
        ret._txn_requires_abort = _lib.KafkaError_txn_requires_abort(_id)
        _lib.KafkaError_destroy(_id)
        return ret

    @staticmethod
    def _from_parts(code: int, message, is_retriable: int, is_fatal: int,
                    txn_requires_abort: int = False):
        """Build a KafkaError from already-copied fields.

        The producer binding never hands an error handle to Python: the C layer
        copies the fields out of the (owned or borrowed) ``kafka_common_Error_t``
        into a ``(code, message, is_retriable, is_fatal, txn_requires_abort)``
        tuple and frees / leaves the handle itself. The admin binding does the
        same for the *borrowed* per-key errors inside a result handle (those die
        with their parent handle), passing only the first four fields.
        """
        ret = KafkaError.__new__(KafkaError)
        ret._code = code
        ret._message = message
        ret._is_retriable = bool(is_retriable)
        ret._is_fatal = bool(is_fatal)
        ret._txn_requires_abort = bool(txn_requires_abort)
        return ret

    @staticmethod
    def _from_tuple(parts):
        """``None`` -> ``None``; an error tuple from the C layer -> KafkaError."""
        if parts is None:
            return None
        return KafkaError._from_parts(*parts)

    @property
    def code(self):
        return self._code

    @property
    def message(self):
        return self._message

    @property
    def is_retriable(self):
        return self._is_retriable

    @property
    def is_fatal(self):
        return self._is_fatal

    @property
    def txn_requires_abort(self):
        """Whether this error requires the current transaction to be aborted.

        Mirrors librdkafka's ``rd_kafka_error_txn_requires_abort()``: when this
        is ``True`` (e.g. a ``TRANSACTION_ABORTABLE`` error), the transaction
        can no longer commit and must be aborted with
        :meth:`Producer.abort_transaction` / :meth:`AsyncProducer.abort_transaction`.

        A ``ConcurrentModification`` error is a caller-sequencing bug, NOT an
        abortable transaction failure: it reports ``False`` here, the
        transaction is untouched, and it must not trigger an abort or retry."""
        return self._txn_requires_abort


class RecordMetadata:
    """The metadata of a record acknowledged by the server (Java
    ``RecordMetadata``).

    Plain, eagerly-copied fields: the C layer copies them out of the
    ``kafka_producer_RecordMetadata_t`` -- which it only ever *borrows* from the
    future or the delivery callback -- so there is no native handle to manage.
    ``offset()`` / ``timestamp()`` are ``-1`` when the broker reported none
    (Java ``hasOffset()`` / ``hasTimestamp()`` false), and the whole object is
    the Java "null metadata" placeholder (partition and offset ``-1``) when it
    accompanies a delivery error.
    """

    def __init__(self, topic, partition, offset, timestamp,
                 serialized_key_size=-1, serialized_value_size=-1):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp
        self._serialized_key_size = serialized_key_size
        self._serialized_value_size = serialized_value_size

    @staticmethod
    def _from_tuple(t):
        """``None`` -> ``None``; the C layer's ``(topic, partition, offset,
        timestamp, serialized_key_size, serialized_value_size)`` -> metadata."""
        if t is None:
            return None
        return RecordMetadata(*t)

    def offset(self):
        return self._offset

    def has_offset(self):
        return self._offset >= 0

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def timestamp(self):
        return self._timestamp

    def has_timestamp(self):
        return self._timestamp >= 0

    def serialized_key_size(self):
        return self._serialized_key_size

    def serialized_value_size(self):
        return self._serialized_value_size

    def __repr__(self):
        return (f"RecordMetadata(topic={self._topic!r}, partition={self._partition}, "
                f"offset={self._offset}, timestamp={self._timestamp})")


def _invoke_on_delivery(on_delivery, metadata, exception):
    """Invoke a user delivery callback, shielding the C caller from it.

    Mirrors Java's ``Callback.onCompletion(RecordMetadata, Exception)``: on
    success ``metadata`` is set and ``exception`` is ``None``; on failure
    ``exception`` is a :class:`KafkaError` and ``metadata`` is Java's "null
    metadata" placeholder (partition and offset ``-1``) or ``None``. Java's
    contract is that the callback fires exactly once per record, so it is
    invoked here on *every* completion path — including one whose future was
    already cancelled or resolved, where the future itself is left untouched.

    An exception raised by the callback is logged and swallowed. It must not
    propagate: the caller is the C callback pump, where the only handling
    available is ``PyErr_WriteUnraisable``."""
    if on_delivery is None:
        return
    try:
        on_delivery(metadata, exception)
    except Exception:  # noqa: BLE001 - user callback, must not escape into C
        _log.exception("Error in on_delivery callback")


def _completion_to_python(meta_tuple, err_tuple):
    """Convert a C completion ``(meta_tuple | None, err_tuple | None)`` into
    ``(RecordMetadata | None, KafkaError | None)``."""
    return RecordMetadata._from_tuple(meta_tuple), KafkaError._from_tuple(err_tuple)


def _ms(timeout):
    """Seconds (float, ``None`` or negative = forever) -> int64 milliseconds
    (``-1`` = forever) for the C pump."""
    if timeout is None or timeout < 0:
        return -1
    return int(float(timeout) * 1000)


class SendFuture:
    """The future returned by the sync :meth:`Producer.send` -- Java's
    ``Future<RecordMetadata>``.

    Wraps the owned ``KafkaFuture<RecordMetadata>`` the Rust producer returned
    for the record, with the ``concurrent.futures.Future`` surface the previous
    implementation exposed: ``result(timeout)``, ``exception(timeout)``,
    ``done()``, ``cancel()`` and ``cancelled()``.

    :meth:`result` / :meth:`exception` never block in a native ``get``: the
    first wait registers the queued ``KafkaFuture.get_cb`` on the handle, whose
    completion the producer's callback pump delivers on the *waiting* thread,
    and then waits in 100 ms slices of ``Producer_poll`` (see
    :class:`_ProducerWaiter`), so

    * a ``KeyboardInterrupt`` is raised between two slices (``Ctrl-C`` is
      honoured promptly; the registration stays, the future remains pending
      and a later ``result()`` picks the completion up);
    * the delivery callbacks the client queued meanwhile -- including this
      record's own ``on_delivery`` once it completed -- run on the waiting
      thread, like from :meth:`Producer.poll`. This is a deliberate
      divergence from Java, where ``get()`` never runs the I/O thread's
      callbacks: in this binding the calling thread is the only thread that
      ever runs them.

    ``cancel()`` cannot un-send the record (Java's ``FutureRecordMetadata``
    does not support cancellation either): it only marks the future cancelled
    so that :meth:`result` raises :class:`concurrent.futures.CancelledError`
    instead of waiting. ``on_delivery`` still fires for the record.
    """

    __slots__ = ("_handle", "_waiter", "_box", "_outcome", "_cancelled")

    def __init__(self, handle, waiter):
        self._handle = handle          # _lib.KafkaFuture
        self._waiter = waiter          # the producer's _ProducerWaiter
        self._box = None               # [(meta_tuple, err_tuple)] once get_cb ran
        self._outcome = None           # (RecordMetadata | None, KafkaError | None)
        self._cancelled = False

    def _wait(self, timeout):
        if self._cancelled:
            raise CancelledError()
        if self._outcome is None:
            if self._box is None:
                box = []
                # get_cb's completion is queued on the producer's callbacks
                # vector; the slices below drain it on this thread.
                self._handle.get_cb(lambda meta, err: box.append((meta, err)))
                self._box = box
            deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
            while not self._box:
                if deadline is None:
                    slice_s = _SLICE_SECONDS
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise FutureTimeoutError()
                    slice_s = min(_SLICE_SECONDS, remaining)
                self._waiter.wait_slice(slice_s)
            self._outcome = _completion_to_python(*self._box[0])
        return self._outcome

    def result(self, timeout=None):
        """Wait up to ``timeout`` seconds (``None`` = forever) and return the
        :class:`RecordMetadata`; raises the :class:`KafkaError` the record
        failed with, :class:`concurrent.futures.TimeoutError` when the wait
        elapses first and :class:`concurrent.futures.CancelledError` after
        :meth:`cancel`."""
        metadata, exception = self._wait(timeout)
        if exception is not None:
            raise exception
        return metadata

    def exception(self, timeout=None):
        """Wait like :meth:`result` and return the record's :class:`KafkaError`
        (``None`` on success) instead of raising it."""
        return self._wait(timeout)[1]

    def done(self):
        return (self._cancelled or self._outcome is not None or bool(self._box)
                or self._handle.is_done())

    def cancel(self):
        if self.done():
            return False
        self._cancelled = True
        return True

    def cancelled(self):
        return self._cancelled


class _ProducerBase:
    """State and helpers shared by the sync and async producers.

    The C extension (`_confluentkafka.c`) is a thin layer over the C API of
    the Rust client and owns no threads of its own. Both producers drive the
    C API's queued (``_cb``) entry points: delivery callbacks and the
    completions of the queued operations are *queued* by Rust on the
    producer's callback vector and run only when the producer's callback pump
    is driven -- on the calling thread, inside the sliced waits of
    :class:`Producer` (see :class:`_ProducerWaiter`), and on the event loop
    for :class:`AsyncProducer`, whose Python notify hook schedules the pump
    once each time the vector goes from empty to non-empty.
    """

    def __init__(self):
        self.closed = False
        self.c_producer = None

    def _init_mock(self, auto_complete=True):
        self.c_producer = _lib.MockProducer_new(auto_complete, self._notify_callable())

    def _init_kafka(self, config):
        self.c_producer = _lib.KafkaProducer_new(config, self._notify_callable())

    def _notify_callable(self):
        """The Python callable the C notify hook invokes (``None`` for the sync
        producer, which waits on the C condition variable instead)."""
        return None

    def _check_closed(self):
        if self.closed:
            raise RuntimeError("Producer is already closed")

    def metrics(self):
        """Point-in-time snapshot of the producer's metrics.

        Returns a list of dicts with keys ``name``, ``group``, ``description``,
        ``tags`` (dict[str, str]), ``value`` (float / str / int depending on the
        metric) and ``kind`` (0=double, 1=string, 2=long, 3=int).

        ``kind`` is redundant for double/string but not for the integer cases:
        Rust distinguishes ``Long`` from ``Int`` while Python has a single
        ``int``, so ``kind`` is the only way to round-trip that faithfully.

        A list rather than a dict keyed by name: ``MetricName`` identity is
        (name, group, tags), so per-topic metrics share a name and differ only
        by tags. Callers that want a mapping should key on the whole triple.

        ``metrics()`` does not block in Java, so this is a plain sync method on
        both the sync and async producers. Values are measured once, when this
        is called -- the entries are not live handles.
        """
        self._check_closed()
        raw = _lib.Producer_metrics(self.c_producer)
        if raw is None:
            return []
        return raw

    @staticmethod
    def _validate_record(producer_record):
        if producer_record is None:
            raise ValueError("producer_record cannot be None")
        if not isinstance(producer_record, ProducerRecord):
            raise TypeError(
                "producer_record must be an instance of ProducerRecord")

    @staticmethod
    def _delivery_callable(on_delivery):
        """Wrap a user ``on_delivery`` into the ``callable(meta_tuple | None,
        err_tuple | None)`` the C delivery callback invokes, or ``None``."""
        if on_delivery is None:
            return None

        def cb(meta_tuple, err_tuple):
            metadata, exception = _completion_to_python(meta_tuple, err_tuple)
            _invoke_on_delivery(on_delivery, metadata, exception)
        return cb

    @staticmethod
    def _raise_if_error(err_tuple):
        if err_tuple is not None:
            raise KafkaError._from_parts(*err_tuple)

    @staticmethod
    def _partitions_to_python(raw):
        import consumer as _kc
        return [_kc._to_partition_info(t) for t in raw]

    # ---- transaction helpers (shared by the sync + async families) ----------
    @staticmethod
    def _offsets_to_spec(offsets):
        """Marshal ``{TopicPartition: OffsetAndMetadata}`` into the
        ``(topic, partition, offset, leader_epoch, metadata)`` tuple list the C
        wrapper expects. Mirrors the consumer commit path (``_commit_spec``) so
        the two are consistent: a missing ``leader_epoch`` becomes ``-1`` and a
        missing ``metadata`` becomes ``""``. An empty ``offsets`` maps to an
        empty list (a legitimate ``count == 0``)."""
        return [(tp.topic, tp.partition, oam.offset,
                 oam.leader_epoch if oam.leader_epoch is not None else -1,
                 oam.metadata if oam.metadata is not None else "")
                for tp, oam in offsets.items()]


class _MockProducerMixin:
    """Mock-only operations shared by :class:`MockProducer` and
    :class:`AsyncMockProducer`."""

    def complete_next(self):
        """Complete the next pending send successfully.

        Returns:
            True if there was a pending completion, False otherwise.
        """
        return _lib.MockProducer_complete_next(self.c_producer)

    def error_next(self, error_code, error_message=None):
        """Complete the next pending send with an error.

        The error is built with the C API's typed ``kafka_common_Error_*``
        factory for ``error_code``, so only the codes the binding maps are
        accepted: -5 (local timeout), -4 (local illegal state), -3 (local
        illegal argument), -2 (local concurrent modification), 7
        (REQUEST_TIMED_OUT), 10 (MESSAGE_TOO_LARGE), 18 (RECORD_LIST_TOO_LARGE),
        24 (INVALID_GROUP_ID), 35 (UNSUPPORTED_VERSION) and 89
        (THROTTLING_QUOTA_EXCEEDED). Any other code raises ``ValueError``.

        Args:
            error_code: Kafka error code (one of the mapped codes above)
            error_message: Optional error message

        Returns:
            True if there was a pending completion, False otherwise.
        """
        return _lib.MockProducer_error_next(
            self.c_producer, error_code, error_message)

    def history_count(self):
        """Returns the number of successfully sent records."""
        return _lib.MockProducer_history_count(self.c_producer)

    def clear(self):
        """Clear the sent history and pending completions."""
        _lib.MockProducer_clear(self.c_producer)


class _ProducerWaiter(SyncWaiter):
    """The sync :class:`Producer`'s :class:`SyncWaiter`.

    The producer's C layer has its own wake-up primitive: the notify hook
    installed by the extension broadcasts a condition variable that
    ``Producer_poll`` waits on (GIL released). So the sync producer registers
    NO Python notify callable -- the inherited :meth:`notify` is never
    installed and never fires for this client -- and :meth:`wait_slice` is one
    bounded ``Producer_poll`` call instead of a wait on the Python event:
    drain, wait up to the slice for the condvar, drain again. The 100 ms slice
    keeps the wait interruptible (``Ctrl-C`` is raised between two slices);
    :meth:`drain` is ``Producer_execute_callbacks``.
    """

    __slots__ = ("_producer",)

    def __init__(self, producer):
        super().__init__(producer._execute_callbacks)
        self._producer = producer

    def wait_slice(self, timeout=_SLICE_SECONDS):
        """One ``Producer_poll`` slice (``timeout`` seconds at most); returns how
        many callbacks ran. Returns ``0`` once the native producer is gone."""
        return self._producer._poll_slice(int(timeout * 1000))


class Producer(_ProducerBase):
    """The synchronous producer.

    Every operation that blocks in Java (:meth:`send` while the record is
    buffered, :meth:`flush`, :meth:`partitions_for`, the transaction control
    ops, :meth:`close`) is submitted through the C API's queued ``_cb`` entry
    point and waited for in Python, in 100 ms slices driven by the C condvar
    pump ``Producer_poll`` (see :class:`_ProducerWaiter`), never through a
    blocking C entry point. Hence

    * ``Ctrl-C`` is honoured promptly: a ``KeyboardInterrupt`` is raised
      between two slices. The in-flight operation is let finish first (so the
      client stays consistent and usable), then the interrupt is re-raised
      and the operation's result is dropped.
    * Delivery callbacks (``on_delivery``) are queued by the client and run
      on the *calling* thread only -- inside :meth:`poll`, :meth:`flush`,
      :meth:`commit_transaction`, :meth:`abort_transaction`, :meth:`close`,
      and inside the waits of :meth:`send` and :meth:`SendFuture.result`.
      This is Java's "sender thread callbacks" model moved onto the caller.
      An application that registers callbacks and only ever calls
      :meth:`send` should still call :meth:`poll` regularly (``poll(0)`` in
      its produce loop is typical) to run them promptly.

    Do not call these blocking methods from inside an ``on_delivery``
    callback: the callback already runs inside the pump of this thread, so a
    nested wait could never drain its own completion (Java's ``Callback``
    contract likewise forbids blocking in the callback).
    """

    def __init__(self):
        super().__init__()
        self._pending = set()  # SendFutures not yet known to be done
        self._waiter = _ProducerWaiter(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _track(self, future):
        self._pending.add(future)
        if len(self._pending) > 1024:
            self._pending = {f for f in self._pending if not f.done()}
        return future

    def _cancel(self):
        for future in self._pending:
            if not future.done():
                future.cancel()
        self._pending.clear()

    # ---- the callback pump (all on the calling thread) ----------------------

    def _execute_callbacks(self):
        """One ``Producer_execute_callbacks`` pass; ``0`` once destroyed."""
        if self.c_producer is None:
            return 0
        return _lib.Producer_execute_callbacks(self.c_producer)

    def _pump(self):
        """Run every queued callback on this thread; returns how many ran."""
        return self._waiter.drain()

    def _poll_slice(self, timeout_ms):
        """One ``Producer_poll`` slice: drain, wait up to ``timeout_ms`` on the
        notify condvar when nothing was queued, drain again. Returns how many
        callbacks ran; ``0`` once the native producer is gone."""
        if self.c_producer is None:
            return 0
        n = _lib.Producer_poll(self.c_producer, timeout_ms)
        if n > 0:
            n += self._waiter.drain()
        return n

    def _run_void(self, submit):
        """Submit a void ``_cb`` op (``cb(err_tuple | None)``), wait for its
        completion in slices and raise its error, if any. A
        ``KeyboardInterrupt`` while waiting lets the op finish first."""
        (err_tuple,) = self._waiter.run(submit)
        self._raise_if_error(err_tuple)

    def send(self, producer_record: ProducerRecord,
             on_delivery=None) -> SendFuture:
        """Send a record; returns a :class:`SendFuture` resolving to its metadata.

        Blocks only as long as Java's ``send()`` does -- until the record is
        appended to the client's buffer (up to ``max.block.ms`` when the buffer
        is full or metadata is missing) -- then returns a future for its
        acknowledgement. The wait is sliced (``Producer_send_cb`` + the
        waiter), so ``Ctrl-C`` is honoured while a full buffer blocks the
        send. The key and value bytes are passed to the client without
        copying.

        Args:
            producer_record: the :class:`ProducerRecord` to send.
            on_delivery: optional ``callback(metadata, exception)`` invoked once
                the record completes — Java's ``Callback`` argument to
                ``send(record, callback)``. On success ``metadata`` is a
                :class:`RecordMetadata` and ``exception`` is ``None``; on
                failure ``exception`` is a :class:`KafkaError` and
                ``metadata`` is the placeholder Java passes beside it
                (partition and offset ``-1``). It is invoked exactly once per
                record, even if the returned future was cancelled or already
                resolved; exceptions it raises are logged and swallowed.

        .. note::
           ``on_delivery`` does not run on a background thread: the client
           queues it and it runs on the calling thread inside :meth:`poll`,
           :meth:`flush`, :meth:`commit_transaction`, :meth:`abort_transaction`,
           :meth:`close` or the wait of a later :meth:`send` /
           :meth:`SendFuture.result`.

        Raises:
            KafkaError: when the client rejects the record before buffering
                it (Java's ``send()`` throws for these; a failure of a
                buffered record is reported through the future instead).
        """
        self._check_closed()
        self._validate_record(producer_record)
        delivery = self._delivery_callable(on_delivery)
        # resolve(KafkaFuture | None, err_tuple | None) runs from the pump once
        # the record is registered (or rejected).
        handle, err_tuple = self._waiter.run(
            lambda cb: _lib.Producer_send_cb(
                self.c_producer, producer_record, delivery, cb))
        self._raise_if_error(err_tuple)
        return self._track(SendFuture(handle, self._waiter))

    def poll(self, timeout=0.0):
        """Run the pending delivery callbacks on the calling thread.

        Waits up to ``timeout`` seconds (``0`` = run what is queued and return
        at once; ``None`` or negative = until at least one callback ran) for a
        completion when none is queued. The wait is sliced (100 ms
        ``Producer_poll`` calls), so ``Ctrl-C`` interrupts an unbounded wait
        promptly. Returns the number of callbacks run.
        """
        self._check_closed()
        ms = _ms(timeout)
        if ms == 0:
            return self._poll_slice(0)
        deadline = None if ms < 0 else time.monotonic() + ms / 1000.0
        while True:
            if deadline is None:
                slice_ms = _SLICE_MS
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return 0
                slice_ms = max(1, min(_SLICE_MS, int(remaining * 1000)))
            n = self._poll_slice(slice_ms)
            if n > 0:
                return n

    def flush(self):
        """Flush all pending records (Java ``flush()``): waits until every
        previously sent record is acknowledged, running their delivery
        callbacks on the calling thread as they complete."""
        self._check_closed()
        try:
            self._run_void(lambda cb: _lib.Producer_flush_cb(self.c_producer, cb))
        finally:
            self._pump()

    def partitions_for(self, topic):
        """Return partition metadata for ``topic`` as a list of PartitionInfo.

        Reuses the consumer binding's PartitionInfo conversion (the C API
        returns the same ``kafka_common_PartitionInfo_t`` type)."""
        self._check_closed()
        raw, err_tuple = self._waiter.run(
            lambda cb: _lib.Producer_partitions_for_cb(self.c_producer, topic, cb))
        self._raise_if_error(err_tuple)
        return self._partitions_to_python(raw)

    # ---- transaction control (sync; Java KafkaProducer transaction API) -----
    #
    # Each op submits its `_cb` twin and waits for the completion in slices
    # (KafkaError raised on a non-NULL error); begin_transaction has no `_cb`
    # twin in Rust (a non-waiting state transition) and stays the direct call.
    # The queued sends of the C API (`send_async`, used by the AsyncProducer)
    # that had RETURNED before a control call are drained into it -- committed
    # on commit, discarded on abort -- exactly as flush/close drain them
    # (producer-transactions.md §13); the sync send() returns only once the
    # record is registered, so it is always part of the open transaction.

    def init_transactions(self):
        """Initialize transactions (Java ``initTransactions()``).

        Call exactly once, before any other transactional method, when
        ``transactional.id`` is configured. Waits until the transaction
        coordinator is ready; a timeout error is safe to retry.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        self._run_void(
            lambda cb: _lib.Producer_init_transactions_cb(self.c_producer, cb))

    def begin_transaction(self):
        """Begin a new transaction (Java ``beginTransaction()``).

        A state transition that does not wait; :meth:`init_transactions` must
        have completed successfully first. Records sent with :meth:`send`
        between this call and :meth:`commit_transaction` /
        :meth:`abort_transaction` are part of the transaction.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        self._raise_if_error(_lib.Producer_begin_transaction(self.c_producer))

    def send_offsets_to_transaction(self, offsets, group_metadata):
        """Send consumer-group offsets to the coordinator as part of the ongoing
        transaction (Java ``sendOffsetsToTransaction(offsets, groupMetadata)``).

        The producer half of consume-transform-produce: the offsets commit only
        if the transaction commits. Waits until the coordinator acknowledges.

        Args:
            offsets: a ``{TopicPartition: OffsetAndMetadata}`` mapping (each
                offset is the offset of the *next* record to consume). An empty
                mapping stages nothing.
            group_metadata: the :class:`ConsumerGroupMetadata` from
                ``consumer.group_metadata()`` (it owns the live handle the C
                API needs).

        Raises:
            KafkaError: if the call fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`.
        """
        self._check_closed()
        spec = self._offsets_to_spec(offsets)
        self._run_void(
            lambda cb: _lib.Producer_send_offsets_to_transaction_cb(
                self.c_producer, spec, group_metadata, cb))

    def commit_transaction(self):
        """Commit the ongoing transaction (Java ``commitTransaction()``).

        Flushes the transaction's records (every record whose :meth:`send`
        returned, plus any queued send that had returned), waits until the
        transaction is committed, running the delivery callbacks of the
        flushed records on the calling thread as they complete.

        Raises:
            KafkaError: if the commit fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`; a timeout error is safe to retry.
        """
        self._check_closed()
        try:
            self._run_void(
                lambda cb: _lib.Producer_commit_transaction_cb(self.c_producer, cb))
        finally:
            self._pump()

    def abort_transaction(self):
        """Abort the ongoing transaction (Java ``abortTransaction()``).

        Discards the transaction's records (including queued sends that had
        returned) and staged offsets, waits until the abort completes, running
        the delivery callbacks of the discarded records on the calling thread
        as they complete.

        Raises:
            KafkaError: if the abort fails.
        """
        self._check_closed()
        try:
            self._run_void(
                lambda cb: _lib.Producer_abort_transaction_cb(self.c_producer, cb))
        finally:
            self._pump()

    def close(self):
        """Close the producer (Java ``close()``): waits until the in-flight
        records are delivered, running their delivery callbacks on the calling
        thread, then frees the native producer. Futures still pending are
        cancelled. Idempotent.

        A ``KeyboardInterrupt`` while closing cannot abort the close: the
        operation is let finish, the native producer is freed and the
        interrupt is then re-raised."""
        if self.closed:
            return
        self.closed = True
        self._cancel()
        try:
            self._run_void(lambda cb: _lib.Producer_close_cb(self.c_producer, cb))
        finally:
            self._pump()
            # Destroying the native handle runs every still-pending callback
            # exactly once (on this thread) before freeing. The handle is
            # dropped first so a pump racing the destroy sees it gone.
            handle, self.c_producer = self.c_producer, None
            _lib.Producer_destroy(handle)


class AsyncProducer(_ProducerBase):
    """An asyncio-native producer.

    ``send`` is a coroutine that returns an :class:`asyncio.Future` resolving
    to a :class:`RecordMetadata` (``fut = await producer.send(rec)``; then
    ``await fut`` for the result). It drives the C API's *queued* send: the
    record is handed to the client's submission task and the coroutine
    suspends until the client reports it registered (or rejected), so the
    event loop is never blocked by a full buffer -- Java's ``send()`` would
    block the caller there.

    Every completion -- registration of a queued send, delivery of a record,
    the end of a queued ``flush`` / transaction op / ``close`` -- is queued by
    the client and run by the producer's callback pump. The client's notify
    hook fires once each time that queue goes from empty to non-empty and
    schedules the pump on the event loop (``call_soon_threadsafe``), which
    runs every queued callback on the loop thread. So ``on_delivery`` always
    runs on the event loop and may touch loop state; it must not block.
    """

    def __init__(self):
        super().__init__()
        self._loop = None
        self._pending = set()

    def _notify_callable(self):
        return self._on_notify

    def _on_notify(self):
        """The C notify hook: runs on whichever thread queued the first
        callback (a Rust task, or the loop thread for a synchronous
        completion). It only schedules; never runs callbacks."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(self._pump)
        except RuntimeError:
            pass  # loop closed between the check and the call

    def _pump(self):
        """Run every queued callback on the event loop thread."""
        if self.c_producer is None:
            return
        while _lib.Producer_execute_callbacks(self.c_producer) > 0:
            pass

    def _bind_loop(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        return loop

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def _track(self, future):
        self._pending.add(future)
        future.add_done_callback(self._pending.discard)
        return future

    def _cancel(self):
        for future in list(self._pending):
            if not future.done():
                future.cancel()
        self._pending.clear()

    def _deliver(self, loop, fn, *args):
        """Run ``fn(*args)`` on the loop thread. Callbacks normally already run
        there (the pump is scheduled on the loop) but ``Producer_destroy`` runs
        the still-pending ones on the executor thread that destroys the
        handle, so always hop through ``call_soon_threadsafe``."""
        if loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(fn, *args)
        except RuntimeError:
            pass

    async def send(self, producer_record: ProducerRecord,
                   on_delivery=None) -> "asyncio.Future[RecordMetadata]":
        """Send a record; returns an ``asyncio.Future`` resolving to its metadata.

        Suspends until the client has registered the record (the queued send's
        completion), then returns the future for its acknowledgement.

        ``on_delivery`` is the asyncio counterpart of the sync
        :meth:`Producer.send` argument — a plain (non-coroutine)
        ``callback(metadata, exception)`` invoked exactly once per record, on
        the event loop thread, after the returned future is resolved. On
        failure ``metadata`` is the placeholder Java passes beside the error
        (partition and offset ``-1``).

        Raises:
            KafkaError: when the client rejects the record before buffering it.
        """
        self._check_closed()
        self._validate_record(producer_record)
        loop = self._bind_loop()
        ret = loop.create_future()
        registered = loop.create_future()

        def resolve_registered(kafka_future, err_tuple):
            # kafka_future (the owned KafkaFuture handle) is not needed: the
            # record's completion arrives through on_completion below.
            self._deliver(loop, self._set_registered, registered, err_tuple)

        def on_completion(meta_tuple, err_tuple):
            self._deliver(loop, self._resolve_future, ret, on_delivery,
                          meta_tuple, err_tuple)

        _lib.Producer_send_cb(self.c_producer, producer_record, on_completion,
                              resolve_registered)
        self._track(ret)
        err_tuple = await registered
        if err_tuple is not None:
            self._pending.discard(ret)
            raise KafkaError._from_parts(*err_tuple)
        return ret

    @staticmethod
    def _set_registered(registered, err_tuple):
        if not registered.done():
            registered.set_result(err_tuple)

    @staticmethod
    def _resolve_future(ret, on_delivery, meta_tuple, err_tuple):
        """Resolve a single record future. Runs on the event loop thread.
        ``on_delivery`` (if given) is invoked here too — on every path, per
        the callback obligation."""
        metadata, exception = _completion_to_python(meta_tuple, err_tuple)
        if not ret.cancelled() and not ret.done():
            if exception is not None:
                ret.set_exception(exception)
            else:
                ret.set_result(metadata)
        _invoke_on_delivery(on_delivery, metadata, exception)

    async def _run_cb(self, submit):
        """Submit a queued (``_cb``) op and ``await`` its completion.

        ``submit(cb)`` registers ``cb(payload...)``, which the pump runs on the
        loop thread once the op completes; the payload is handed to the
        awaiting coroutine through an asyncio future. A late completion for an
        awaiter that was cancelled (e.g. under ``asyncio.wait_for``) is simply
        dropped -- the payload holds no native handles, only copied tuples."""
        loop = self._bind_loop()
        fut = loop.create_future()

        def deliver(payload):
            if not fut.done():
                fut.set_result(payload)

        def cb(*payload):
            self._deliver(loop, deliver, payload)

        submit(cb)
        return await fut

    async def _run_void_cb(self, submit):
        (err_tuple,) = await self._run_cb(submit)
        self._raise_if_error(err_tuple)

    async def flush(self):
        """Flush all pending records (Java ``flush()``): awaits the
        acknowledgement of every record sent so far. Their delivery callbacks
        run on the loop as the completions arrive."""
        self._check_closed()
        await self._run_void_cb(
            lambda cb: _lib.Producer_flush_cb(self.c_producer, cb))

    async def partitions_for(self, topic):
        """Return partition metadata for ``topic`` as a list of PartitionInfo."""
        self._check_closed()
        raw, err_tuple = await self._run_cb(
            lambda cb: _lib.Producer_partitions_for_cb(self.c_producer, topic, cb))
        self._raise_if_error(err_tuple)
        return self._partitions_to_python(raw)

    # ---- transaction control (async; Java KafkaProducer transaction API) ----
    #
    # init / send_offsets / commit / abort drive the C API's queued `_cb`
    # variants and await their completion on the loop; begin_transaction is a
    # non-waiting state transition with no `_cb` twin and runs on the default
    # executor so a slow lock never blocks the loop. The queued sends this
    # producer issues (`send_async` in the C API) that had RETURNED -- i.e.
    # whose ``await producer.send(...)`` completed -- before a control call are
    # drained into it: committed on commit, discarded on abort, exactly as
    # flush / close drain them (producer-transactions.md §13). A send still
    # suspended in ``await producer.send(...)`` when the control call starts is
    # not ordered against it.

    async def init_transactions(self):
        """Initialize transactions (Java ``initTransactions()``).

        Call exactly once, before any other transactional method, when
        ``transactional.id`` is configured. Awaits the coordinator handshake on
        the event loop; a timeout error is safe to retry.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        await self._run_void_cb(
            lambda cb: _lib.Producer_init_transactions_cb(self.c_producer, cb))

    async def begin_transaction(self):
        """Begin a new transaction (Java ``beginTransaction()``).

        A state transition that does not wait; :meth:`init_transactions` must
        have completed first. Records whose ``await producer.send(...)``
        returned between this call and :meth:`commit_transaction` /
        :meth:`abort_transaction` are part of the transaction.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        loop = self._bind_loop()
        err_tuple = await loop.run_in_executor(
            None, _lib.Producer_begin_transaction, self.c_producer)
        self._raise_if_error(err_tuple)

    async def send_offsets_to_transaction(self, offsets, group_metadata):
        """Send consumer-group offsets to the coordinator as part of the ongoing
        transaction (Java ``sendOffsetsToTransaction(offsets, groupMetadata)``).

        The producer half of consume-transform-produce: the offsets commit only
        if the transaction commits. Awaits the coordinator call on the event
        loop.

        Args:
            offsets: a ``{TopicPartition: OffsetAndMetadata}`` mapping (each
                offset is the offset of the *next* record to consume). An empty
                mapping stages nothing.
            group_metadata: the :class:`ConsumerGroupMetadata` from
                ``consumer.group_metadata()`` (it owns the live handle the C
                API needs).

        Raises:
            KafkaError: if the call fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`.
        """
        self._check_closed()
        spec = self._offsets_to_spec(offsets)
        await self._run_void_cb(
            lambda cb: _lib.Producer_send_offsets_to_transaction_cb(
                self.c_producer, spec, group_metadata, cb))

    async def commit_transaction(self):
        """Commit the ongoing transaction (Java ``commitTransaction()``).

        Flushes the transaction's records -- every send that had returned,
        including queued ones -- then awaits the commit on the event loop.

        Raises:
            KafkaError: if the commit fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`; a timeout error is safe to retry.
        """
        self._check_closed()
        await self._run_void_cb(
            lambda cb: _lib.Producer_commit_transaction_cb(self.c_producer, cb))

    async def abort_transaction(self):
        """Abort the ongoing transaction (Java ``abortTransaction()``).

        Discards the transaction's records (every send that had returned,
        including queued ones) and staged offsets, then awaits the abort on the
        event loop.

        Raises:
            KafkaError: if the abort fails.
        """
        self._check_closed()
        await self._run_void_cb(
            lambda cb: _lib.Producer_abort_transaction_cb(self.c_producer, cb))

    async def close(self):
        """Close the producer (Java ``close()``): awaits the delivery of the
        in-flight records (their callbacks run on the loop), then frees the
        native producer off the loop. Pending futures are cancelled.
        Idempotent."""
        if self.closed:
            return
        self.closed = True
        self._cancel()
        loop = self._bind_loop()
        (err_tuple,) = await self._run_cb(
            lambda cb: _lib.Producer_close_cb(self.c_producer, cb))
        # Destroying the native handle blocks (it waits for the client's
        # tasks and runs every still-pending callback), so run it off the
        # loop; the callbacks it runs hop back onto the loop via _deliver.
        handle, self.c_producer = self.c_producer, None
        await loop.run_in_executor(None, _lib.Producer_destroy, handle)
        self._raise_if_error(err_tuple)


class KafkaProducer(Producer):
    """A Kafka producer connected to a real cluster.

    Args:
        config: A dict of configuration properties (string keys and values).
            At minimum, ``bootstrap.servers`` must be provided.
    """

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class MockProducer(_MockProducerMixin, Producer):

    def __init__(self, auto_complete=True):
        super().__init__()
        self._init_mock(auto_complete)


class AsyncKafkaProducer(AsyncProducer):
    """An asyncio-native Kafka producer connected to a real cluster.

    Args:
        config: A dict of configuration properties (string keys and values).
            At minimum, ``bootstrap.servers`` must be provided.
    """

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class AsyncMockProducer(_MockProducerMixin, AsyncProducer):

    def __init__(self, auto_complete=True):
        super().__init__()
        self._init_mock(auto_complete)
