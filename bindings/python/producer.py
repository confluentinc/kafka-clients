import asyncio
import logging
import os
import sys
import threading
import time
import _confluentkafka as _lib
from _confluentkafka import ProducerRecord
from concurrent.futures import (Future)

_log = logging.getLogger(__name__)


# ProducerRecord is a C extension type imported from _confluentkafka module
# It stores kafka_producer_ProducerRecord_t internally for optimized performance


class KafkaError(Exception):
    """Kafka error with code, message, and retriable/fatal flags."""

    def __init__(self):
        raise NotImplementedError()

    def __str__(self):
        return self._message

    @staticmethod
    def _from_c(_id: int):
        ret = KafkaError.__new__(KafkaError)
        ret._code = _lib.KafkaError_code(_id)
        ret._message = _lib.KafkaError_message(_id)
        ret._is_retriable = _lib.KafkaError_is_retriable(_id)
        ret._is_fatal = _lib.KafkaError_is_fatal(_id)
        ret._txn_requires_abort = _lib.KafkaError_txn_requires_abort(_id)
        _lib.KafkaError_destroy(_id)
        return ret

    @staticmethod
    def _from_parts(code: int, message: str, is_retriable: int, is_fatal: int):
        """Build a KafkaError from already-copied fields.

        Used for *borrowed* per-key errors inside an admin result handle: those
        die with their parent handle, so the C layer copies their fields out
        before destroying it and there is nothing left to ``KafkaError_destroy``.
        """
        ret = KafkaError.__new__(KafkaError)
        ret._code = code
        ret._message = message
        ret._is_retriable = bool(is_retriable)
        ret._is_fatal = bool(is_fatal)
        return ret

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
    def __init__(self):
        raise NotImplementedError()

    def _set_attributes_from_c(self, offset: int, partition: int,
                               topic: str, timestamp: int):
        self._offset = offset
        self._partition = partition
        self._topic = topic
        self._timestamp = timestamp

    def __del__(self):
        if not self._populated and self._id != 0:
            _lib.RecordMetadata_destroy(self._id)

    def _get_record_metadata(self):
        if not self._populated:
            self._populated = True
            _lib.RecordMetadata_copy(
                self._id,
                self._set_attributes_from_c)
        return self

    @staticmethod
    def _from_c(_id: int):
        self = RecordMetadata.__new__(RecordMetadata)
        self._id = _id
        self._populated = False
        self._topic = None
        self._offset = None
        self._partition = None
        self._timestamp = None
        return self

    def offset(self):
        return self._get_record_metadata()._offset

    def topic(self):
        return self._get_record_metadata()._topic

    def partition(self):
        return self._get_record_metadata()._partition

    def timestamp(self):
        return self._get_record_metadata()._timestamp


def _invoke_on_delivery(on_delivery, metadata, exception):
    """Invoke a user delivery callback, shielding the C caller from it.

    Mirrors Java's ``Callback.onCompletion(RecordMetadata, Exception)``: exactly
    one of the two arguments is meaningful (``metadata`` on success,
    ``exception`` on failure) and the callback returns nothing. Java's contract
    is that the callback fires exactly once per record, so it is invoked here on
    *every* completion path — including one whose ``Future`` was already
    cancelled or resolved, where the future itself is left untouched.

    An exception raised by the callback is logged and swallowed. It must not
    propagate: the caller is a C completion thread, where the only handling
    available is ``PyErr_Print``, and the record's ``Future`` has already been
    resolved by then."""
    if on_delivery is None:
        return
    try:
        on_delivery(metadata, exception)
    except Exception:  # noqa: BLE001 - user callback, must not escape into C
        _log.exception("Error in on_delivery callback")


def _completion_to_python(result, error):
    """Convert the raw C completion handles into owned Python objects.

    Called exactly once per completion, before any branching, so ownership of
    both handles is transferred into Python objects that free themselves —
    :meth:`KafkaError._from_c` destroys the error handle immediately, and
    :class:`RecordMetadata` owns the metadata handle until it is copied or
    garbage collected. Every downstream branch (future cancelled, future
    already done, ``on_delivery`` invocation) can then simply use or ignore the
    objects with no double-free or leak to reason about."""
    metadata = RecordMetadata._from_c(result) if result != 0 else None
    exception = KafkaError._from_c(error) if error != 0 else None
    return metadata, exception


def _latency_probe_enabled():
    value = os.environ.get("CK_LATENCY_PROBE", "")
    return value != "" and value != "0"


class _LatencyProbe:
    """Per-record latency breakdown of the send path (``CK_LATENCY_PROBE=1``).

    Diagnostic only; nothing here runs unless the environment variable is set
    at producer construction. Every record is stamped at four points and, at
    ``close()``, the mean time spent in each section and its share of the
    send()-to-callback total are printed to stderr::

        t0  Python send() entered                        (caller thread)
        t1  the C send entry point returned              (caller thread)
        t2  record appended to the Rust accumulator: its CreateTime, which
            the Rust producer assigns at append time (1 ms resolution)
        t4  Rust reported the record complete            (C, see the .c file)
        t6  Python completion callback entered           (callback thread)

    Sections: A = t0->t1, B = t1->t2, C = t2->t4, D = t4->t6. Sums are
    additive, so the section means partition the mean end-to-end latency.
    The 1 ms floor of t2 is corrected by +0.5 ms on B and -0.5 ms on C.
    """

    def __init__(self):
        self.sends = 0
        self.space_waits = 0
        self.space_wait_ns = 0
        self.n = 0
        self.sum_a = 0
        self.sum_b = 0
        self.sum_c = 0
        self.sum_d = 0
        self.sum_e2e = 0
        self.missing_t2 = 0
        self.missing_t4 = 0

    # -- caller thread ------------------------------------------------------
    def start(self):
        self.sends += 1
        return [time.time_ns(), 0]

    def returned(self, st):
        st[1] = time.time_ns()

    def space_wait(self, ns):
        self.space_waits += 1
        self.space_wait_ns += ns

    # -- completion thread --------------------------------------------------
    def complete(self, st, metadata, completed_ns, t6):
        t0 = st[0]
        t1 = st[1] or t0
        self.n += 1
        self.sum_e2e += t6 - t0
        self.sum_a += t1 - t0
        if metadata is None:
            self.missing_t2 += 1
            t2 = t1
        else:
            t2 = metadata.timestamp() * 1_000_000
        if not completed_ns:
            self.missing_t4 += 1
        t4 = completed_ns or t6
        self.sum_b += t2 - t1
        self.sum_c += t4 - t2
        self.sum_d += t6 - t4

    def report(self, metrics_rows=None, out=None):
        out = out or sys.stderr
        if self.n == 0:
            print("[CK_LATENCY_PROBE] no completions recorded", file=out)
            return
        n = self.n
        e2e = self.sum_e2e / n / 1e6
        a = self.sum_a / n / 1e6
        b = self.sum_b / n / 1e6 + 0.5
        c = self.sum_c / n / 1e6 - 0.5
        d = self.sum_d / n / 1e6
        rows = [
            ("A send(): Python -> C entry -> return", a),
            ("B send() returned -> in Rust accumulator", b),
            ("C Rust core: accumulator -> completion", c),
            ("D Rust completion -> Python callback", d),
        ]
        lines = [f"[CK_LATENCY_PROBE] {n} completions, {self.sends} sends, "
                 f"mean send()->callback {e2e:.3f} ms",
                 f"  {'section':44s} {'mean ms':>9s} {'share':>7s}"]
        for label, val in rows:
            share = (val / e2e * 100.0) if e2e > 0 else 0.0
            lines.append(f"  {label:44s} {val:9.3f} {share:6.1f}%")
        if self.sends:
            blocked = self.space_waits / self.sends * 100.0
            per_send = self.space_wait_ns / self.sends / 1e6
            lines.append(f"  caller blocked for buffer space on {blocked:.1f}% of "
                         f"sends, {per_send:.3f} ms per send on average")
        if self.missing_t2 or self.missing_t4:
            lines.append(f"  records without CreateTime: {self.missing_t2}, "
                         f"without C completion stamp: {self.missing_t4}")
        wanted = ("record-queue-time-avg", "request-latency-avg",
                  "records-per-request-avg", "batch-size-avg")
        for row in metrics_rows or []:
            try:
                if row.get("group") == "producer-metrics" and row.get("name") in wanted:
                    lines.append(f"  Rust metric {row['name']:26s} {float(row['value']):9.3f}")
            except (TypeError, ValueError, AttributeError):
                pass
        lines.append("  (t2 is the record CreateTime, 1 ms resolution; "
                     "B/C carry a +/-0.5 ms floor correction)")
        print("\n".join(lines), file=out, flush=True)


class _ProducerBase:
    """State and helpers shared by the sync and async producers.

    ``send`` hands each record straight to the Rust producer on the calling
    thread (`kafka_producer_Producer_send_with_callback`, GIL released for the
    call): there is no Python- or C-side buffering, and the record is in the
    Rust accumulator when ``send`` returns, as in Java. Delivery reports come
    back on the Rust dispatcher thread; the C extension batches them and
    invokes the Python callback ``cb(result, error)`` from one C callback
    thread per producer, taking the GIL once per burst. Both the sync
    :class:`Producer` and the async :class:`AsyncProducer` reuse the same C
    entry points and differ only in the future type the callback resolves and
    how (see their respective ``send``).
    """

    def __init__(self):
        self.futures = set()
        self.closed = False
        self.c_producer = None
        self._probe = _LatencyProbe() if _latency_probe_enabled() else None

    def _init_mock(self, auto_complete=True):
        self.c_producer = _lib.Producer_new(auto_complete, self)

    def _init_kafka(self, config):
        self.c_producer = _lib.KafkaProducer_new(config, self)

    def _remove_future(self, future):
        if future in self.futures:
            self.futures.remove(future)

    def _add_future(self, future):
        self.futures.add(future)
        future.add_done_callback(self._remove_future)
        return future

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

    # ---- resolve / free pairs for the async-FFI ops (flush, partitions_for) --
    # These drive Producer_flush_async / Producer_partitions_for_async, whose
    # trampolines deliver the raw C handles as Python ints (see the consumer's
    # _ConsumerBase for the same pattern). ``resolve`` runs on completion and
    # consumes the handles (drain frees the PartitionInfoList, ``_from_c`` frees
    # the error); ``free`` runs only when the event loop is gone before delivery.
    @staticmethod
    def _resolve_void(payload):
        (error,) = payload
        if error:
            raise KafkaError._from_c(error)
        return None

    @staticmethod
    def _free_void(payload):
        (error,) = payload
        if error:
            _lib.KafkaError_destroy(error)

    @staticmethod
    def _resolve_partitions(payload):
        list_handle, error = payload
        if error:
            if list_handle:
                _lib.PartitionInfoList_drain(list_handle)  # drain frees the handle
            raise KafkaError._from_c(error)
        import consumer as _kc
        raw = _lib.PartitionInfoList_drain(list_handle)
        return [_kc._to_partition_info(t) for t in raw]

    @staticmethod
    def _free_partitions(payload):
        list_handle, error = payload
        if error:
            _lib.KafkaError_destroy(error)
        if list_handle:
            _lib.PartitionInfoList_drain(list_handle)


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

        Args:
            error_code: Kafka error code
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


class Producer(_ProducerBase):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _cancel(self):
        while len(self.futures) > 0:
            for future in list(self.futures):
                self._remove_future(future)
                future.cancel()

    def send(self, producer_record: ProducerRecord,
             on_delivery=None) -> Future[RecordMetadata]:
        """Send a record; returns a ``Future`` resolving to its metadata.

        Args:
            producer_record: the :class:`ProducerRecord` to send.
            on_delivery: optional ``callback(metadata, exception)`` invoked once
                the record completes — Java's ``Callback`` argument to
                ``send(record, callback)``. Exactly one argument is meaningful:
                ``metadata`` is a :class:`RecordMetadata` on success and
                ``exception`` a :class:`KafkaError` on failure. It is invoked
                exactly once per record, even if the returned ``Future`` was
                cancelled or already resolved; exceptions it raises are logged
                and swallowed.

        Like Java's ``send()``, the call returns once the record is in the
        producer's buffer. It blocks only while the producer is out of
        ``buffer.memory`` or waiting for topic metadata, for at most
        ``max.block.ms``; the GIL is released meanwhile.

        .. warning::
           ``on_delivery`` runs on the producer's completion thread, not on the
           caller's — the same contract as Java, where the callback executes on
           the producer's I/O thread. (This differs from
           ``confluent-kafka-python``, which defers callbacks until the
           application calls ``poll()``.) Keep it short and do not block in it.
        """
        self._check_closed()
        self._validate_record(producer_record)
        ret = Future()
        probe = self._probe
        st = probe.start() if probe is not None else None

        def cb(result, error, completed_ns=0):
            t6 = time.time_ns() if probe is not None else 0
            # Runs on the C completion thread with the GIL held. Convert the
            # handles once up front, then resolve the future (unless the caller
            # cancelled it or it is already done) and honor the callback
            # obligation on every path.
            metadata, exception = _completion_to_python(result, error)
            if probe is not None:
                # Before the future is published: reading the CreateTime
                # here copies the metadata out of its C handle on this
                # thread, so a waiter on the future cannot race that lazy
                # copy from another thread.
                probe.complete(st, metadata, completed_ns, t6)
            if not ret.cancelled() and not ret.done():
                if exception is not None:
                    ret.set_exception(exception)
                else:
                    ret.set_result(metadata)
            _invoke_on_delivery(on_delivery, metadata, exception)

        # Direct send on the calling thread (GIL released inside the C call):
        # the record is in the Rust accumulator when this returns. Blocks only
        # where Java's send() blocks (buffer.memory exhausted / metadata
        # missing), for at most max.block.ms.
        _lib.Producer_send(self.c_producer, producer_record, cb)
        if probe is not None:
            probe.returned(st)
        return self._add_future(ret)

    def _run_sync(self, submit, resolve):
        """Submit an async FFI op and wait on an interruptible event.

        Mirrors the sync Consumer's ``_run_sync``: the calling thread never
        parks inside a native ``block_on`` — it waits on a ``threading.Event``
        (which releases the GIL so the producer's dispatcher thread can run the
        completion callback). The producer has no ``wakeup``, so there is no
        abort path; we simply wait for the callback to fire."""
        box = {}
        done = threading.Event()

        def cb(*payload):
            box["payload"] = payload
            done.set()

        submit(cb)
        done.wait()
        return resolve(box["payload"])

    def flush(self):
        """Flush all pending records."""
        self._run_sync(
            lambda cb: _lib.Producer_flush_async(self.c_producer, cb),
            self._resolve_void,
        )

    def partitions_for(self, topic):
        """Return partition metadata for ``topic`` as a list of PartitionInfo.

        Reuses the consumer binding's PartitionInfoList drain + conversion
        (the FFI returns the same shared handle type)."""
        return self._run_sync(
            lambda cb: _lib.Producer_partitions_for_async(self.c_producer, topic, cb),
            self._resolve_partitions,
        )

    # ---- transaction control (sync; Java KafkaProducer transaction API) -----
    #
    # Async-first: every op drives the *_async FFI variant through _run_sync
    # (like flush() / close()), so the calling thread waits on an interruptible
    # threading.Event with the GIL released -- it never parks inside a native
    # block_on, and a stuck transaction op stays responsive to SIGTERM /
    # KeyboardInterrupt on the main thread. _resolve_void raises KafkaError on a
    # non-null completion error (0 = success).
    #
    # Records produced inside a transaction use send(): Python's send() calls
    # the synchronous send FFI, which registers the record before it returns, so
    # every record produced between begin_transaction() and commit/abort is part
    # of the transaction (committed on commit, discarded on abort). Python does
    # not expose an async/outbox send path.

    def init_transactions(self):
        """Initialize transactions (Java ``initTransactions()``).

        Call exactly once, before any other transactional method, when
        ``transactional.id`` is configured. Waits until the transaction
        coordinator is ready; a timeout error is safe to retry. Driven through
        the async FFI so the wait stays interruptible on the main thread.

        Produce records inside a transaction with :meth:`send`, which registers
        each record before it returns, so the record is part of the transaction.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        self._run_sync(
            lambda cb: _lib.Producer_init_transactions_async(self.c_producer, cb),
            self._resolve_void,
        )

    def begin_transaction(self):
        """Begin a new transaction (Java ``beginTransaction()``).

        A state transition that does not wait; :meth:`init_transactions` must
        have completed successfully first. Routed through the async FFI (like
        the other four control ops) for a uniform, interruptible path.

        Produce records into this transaction with :meth:`send`, which registers
        each record before it returns, so the record is part of the transaction.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        self._run_sync(
            lambda cb: _lib.Producer_begin_transaction_async(self.c_producer, cb),
            self._resolve_void,
        )

    def send_offsets_to_transaction(self, offsets, group_metadata):
        """Send consumer-group offsets to the coordinator as part of the ongoing
        transaction (Java ``sendOffsetsToTransaction(offsets, groupMetadata)``).

        The producer half of consume-transform-produce: the offsets commit only
        if the transaction commits. Waits until the coordinator acknowledges,
        driven through the async FFI so the wait stays interruptible.

        Args:
            offsets: a ``{TopicPartition: OffsetAndMetadata}`` mapping (each
                offset is the offset of the *next* record to consume). An empty
                mapping stages nothing.
            group_metadata: the :class:`ConsumerGroupMetadata` from
                ``consumer.group_metadata()`` (it owns the live handle the FFI
                needs).

        Produce records inside the transaction with :meth:`send`, which
        registers each record before it returns, so the record is part of the
        transaction.

        Raises:
            KafkaError: if the call fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`.
        """
        self._check_closed()
        spec = self._offsets_to_spec(offsets)
        self._run_sync(
            lambda cb: _lib.Producer_send_offsets_to_transaction_async(
                self.c_producer, spec, group_metadata, cb),
            self._resolve_void,
        )

    def commit_transaction(self):
        """Commit the ongoing transaction (Java ``commitTransaction()``).

        Flushes any pending records, then waits until the transaction is
        committed. Driven through the async FFI so the wait stays interruptible
        on the main thread.

        Produce records inside the transaction with :meth:`send`, which
        registers each record before it returns, so the record is part of the
        transaction.

        Raises:
            KafkaError: if the commit fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`; a timeout error is safe to retry.
        """
        self._check_closed()
        self._run_sync(
            lambda cb: _lib.Producer_commit_transaction_async(self.c_producer, cb),
            self._resolve_void,
        )

    def abort_transaction(self):
        """Abort the ongoing transaction (Java ``abortTransaction()``).

        Discards the transaction's records and staged offsets, then waits until
        the abort completes. Driven through the async FFI so the wait stays
        interruptible on the main thread.

        Produce records inside a transaction with :meth:`send`, which registers
        each record before it returns, so the record is part of the transaction.

        Raises:
            KafkaError: if the abort fails.
        """
        self._check_closed()
        self._run_sync(
            lambda cb: _lib.Producer_abort_transaction_async(self.c_producer, cb),
            self._resolve_void,
        )

    def close(self):
        if self.closed:
            return
        probe_metrics = None
        if self._probe is not None:
            try:
                probe_metrics = self.metrics()
            except Exception:  # noqa: BLE001 - diagnostics only
                probe_metrics = None
        self.closed = True
        self._cancel()
        # Split teardown (see _confluentkafka.c): reject further sends, then
        # drive the Rust-side close through the interruptible _run_sync path
        # (same as flush), then free the handle and join the C callback thread.
        # Keeping the Rust close in _run_sync means a stuck close stays
        # responsive to KeyboardInterrupt on the main thread rather than
        # blocking in a native wait.
        _lib.Producer_shutdown(self.c_producer)
        self._run_sync(
            lambda cb: _lib.Producer_close_async(self.c_producer, cb),
            self._resolve_void,
        )
        _lib.Producer_destroy(self.c_producer)
        if self._probe is not None:
            self._probe.report(probe_metrics)


class AsyncProducer(_ProducerBase):
    """An asyncio-native producer.

    ``send`` is a coroutine that returns an :class:`asyncio.Future` resolving
    to a :class:`RecordMetadata` (``fut = await producer.send(rec)``; then
    ``await fut`` for the result). The produce itself runs synchronously on
    the loop thread — the record goes straight to the Rust producer, exactly
    as in the sync :class:`Producer` — and blocks the loop only where Java's
    ``send()`` blocks: while the producer is out of ``buffer.memory`` or
    waiting for topic metadata, for at most ``max.block.ms``. ``send`` stays a
    coroutine for API symmetry but never suspends on its own, so a flooding
    ``await producer.send(...)`` loop should yield (``await asyncio.sleep(0)``)
    now and then to let the completion drain run.

    Completions from the C callback thread are marshalled back onto the event
    loop (an ``asyncio.Future`` is not thread-safe).

    Completions are *coalesced*: the C callback thread invokes the callback once per
    record, but rather than waking the event loop once per record (one
    ``call_soon_threadsafe`` each), each callback buffers its
    ``(future, result, error)`` and schedules a single drain only when one is
    not already pending. The drain then resolves the whole accumulated batch in
    one event-loop wakeup. This keeps the per-record cross-thread signalling
    cost — the bottleneck under high produce rates — off the hot path.
    """

    def __init__(self):
        super().__init__()
        # Completions buffered by the C callback thread, drained on the event
        # loop. Guarded by a lock since the two run on different
        # threads; the critical sections are tiny (append / list swap).
        self._pending = []
        self._drain_scheduled = False
        self._pending_lock = threading.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @staticmethod
    def _resolve_future(ret, on_delivery, result, error):
        """Resolve a single future from C completion handles. Runs on the event
        loop thread, so it is safe to mutate the asyncio.Future. Ownership of
        the ``result`` / ``error`` C handles transfers here and is always
        freed. ``on_delivery`` (if given) is invoked here too — on the loop
        thread, and on every path, per the callback obligation."""
        metadata, exception = _completion_to_python(result, error)
        if not ret.cancelled() and not ret.done():
            if exception is not None:
                ret.set_exception(exception)
            else:
                ret.set_result(metadata)
        _invoke_on_delivery(on_delivery, metadata, exception)

    def _drain(self):
        """Resolve all buffered completions. Runs on the event loop thread."""
        with self._pending_lock:
            items = self._pending
            self._pending = []
            self._drain_scheduled = False
        for ret, on_delivery, result, error in items:
            self._resolve_future(ret, on_delivery, result, error)

    def _cancel(self):
        # asyncio.Future done-callbacks are scheduled, not run inline, so
        # `_remove_future` will not shrink `self.futures` synchronously here.
        # Cancel each future once and clear the set ourselves — the sync
        # producer's `while len(...)` loop would spin forever on asyncio
        # futures.
        for future in list(self.futures):
            if not future.done():
                future.cancel()
        self.futures.clear()

    async def send(self, producer_record: ProducerRecord,
                   on_delivery=None) -> "asyncio.Future[RecordMetadata]":
        """Send a record; returns an ``asyncio.Future`` resolving to its metadata.

        ``on_delivery`` is the asyncio counterpart of the sync
        :meth:`Producer.send` argument — a plain (non-coroutine)
        ``callback(metadata, exception)`` invoked exactly once per record. It
        runs **on the event loop thread** (inside the completion drain), not on
        the C completion thread, so it may safely touch loop state; it must not
        block the loop.

        The produce itself runs synchronously on the loop thread and the
        coroutine never suspends by itself; see the class docstring for when
        it blocks.
        """
        self._check_closed()
        self._validate_record(producer_record)
        loop = asyncio.get_running_loop()
        ret = loop.create_future()

        # Runs on the C callback thread with the GIL held. asyncio
        # futures must only be mutated on the loop thread, so buffer the
        # completion and wake the loop once per drain (coalescing) rather than
        # once per record. If the loop is already closed we can't schedule
        # anything — convert (and thereby free) the C handles here, and still
        # honor the callback obligation, noting that in this teardown case
        # on_delivery necessarily runs on the completion thread.
        def cb(result, error, completed_ns=0):
            if loop.is_closed():
                metadata, exception = _completion_to_python(result, error)
                _invoke_on_delivery(on_delivery, metadata, exception)
                return
            with self._pending_lock:
                self._pending.append((ret, on_delivery, result, error))
                if self._drain_scheduled:
                    return
                self._drain_scheduled = True
                loop.call_soon_threadsafe(self._drain)

        # Direct send on the loop thread (GIL released inside the C call): the
        # record is in the Rust accumulator when this returns. See the class
        # docstring for when it blocks.
        _lib.Producer_send(self.c_producer, producer_record, cb)
        self._add_future(ret)
        return ret

    async def _run_async(self, submit, resolve, free):
        """Submit an async FFI op and ``await`` its completion on the event loop.

        Mirrors the async Consumer's ``_run_async``: the completion callback runs
        on the producer's dispatcher thread and hops onto the loop via
        ``call_soon_threadsafe`` (asyncio futures are not thread-safe). The C
        handles the payload carries are owned here and freed on every path --
        ``resolve`` consumes them on normal completion; ``free`` consumes them
        when we cannot deliver: the loop is already closed, or the awaiting task
        was cancelled (e.g. under ``asyncio.wait_for``) so ``fut`` is already done
        by the time the late callback lands. Dropping a payload that carries a
        non-null ``KafkaError`` handle would leak it, unbounded under a
        retry/cancel loop."""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def deliver(payload):
            # Runs on the event loop thread.
            if fut.cancelled() or fut.done():
                free(payload)
                return
            fut.set_result(payload)

        def cb(*payload):
            if loop.is_closed():
                free(payload)
                return
            loop.call_soon_threadsafe(deliver, payload)

        submit(cb)
        payload = await fut
        return resolve(payload)

    async def flush(self):
        """Flush all pending records."""
        await self._run_async(
            lambda cb: _lib.Producer_flush_async(self.c_producer, cb),
            self._resolve_void,
            self._free_void,
        )

    async def partitions_for(self, topic):
        """Return partition metadata for ``topic`` as a list of PartitionInfo."""
        return await self._run_async(
            lambda cb: _lib.Producer_partitions_for_async(self.c_producer, topic, cb),
            self._resolve_partitions,
            self._free_partitions,
        )

    # ---- transaction control (async; Java KafkaProducer transaction API) ----
    #
    # Genuinely async: all five ops drive the *_async FFI variant through
    # _run_async (like flush() / close()). The completion callback runs on the
    # producer's dispatcher thread and hops onto the loop via
    # call_soon_threadsafe; the coroutine simply ``await``s it. Nothing runs on a
    # run_in_executor thread and nothing parks inside a native block_on, so the
    # op is cancellable and the event loop is never frozen -- a strict
    # improvement over the old executor façade (DoD #11). _resolve_void raises
    # KafkaError on a non-null completion error; _free_void frees the error
    # handle if the loop is gone before delivery.
    #
    # Records produced inside a transaction use send() (``await
    # producer.send(rec)``): Python's send() calls the synchronous send FFI,
    # which registers the record before it returns, so every record produced
    # between begin_transaction() and commit/abort is part of the transaction
    # (committed on commit, discarded on abort). Python does not expose an
    # async/outbox send path.

    async def init_transactions(self):
        """Initialize transactions (Java ``initTransactions()``).

        Call exactly once, before any other transactional method, when
        ``transactional.id`` is configured. Awaits the coordinator handshake on
        the event loop (no executor thread); a timeout error is safe to retry.

        Produce records inside a transaction with :meth:`send`, which registers
        each record before it returns, so the record is part of the transaction.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        await self._run_async(
            lambda cb: _lib.Producer_init_transactions_async(self.c_producer, cb),
            self._resolve_void,
            self._free_void,
        )

    async def begin_transaction(self):
        """Begin a new transaction (Java ``beginTransaction()``).

        A state transition that does not wait; :meth:`init_transactions` must
        have completed first. Awaited through the async FFI (like the other four
        control ops) for a uniform, cancellable path.

        Produce records into this transaction with :meth:`send`, which registers
        each record before it returns, so the record is part of the transaction.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        await self._run_async(
            lambda cb: _lib.Producer_begin_transaction_async(self.c_producer, cb),
            self._resolve_void,
            self._free_void,
        )

    async def send_offsets_to_transaction(self, offsets, group_metadata):
        """Send consumer-group offsets to the coordinator as part of the ongoing
        transaction (Java ``sendOffsetsToTransaction(offsets, groupMetadata)``).

        The producer half of consume-transform-produce: the offsets commit only
        if the transaction commits. Awaits the coordinator call on the event
        loop (no executor thread).

        Args:
            offsets: a ``{TopicPartition: OffsetAndMetadata}`` mapping (each
                offset is the offset of the *next* record to consume). An empty
                mapping stages nothing.
            group_metadata: the :class:`ConsumerGroupMetadata` from
                ``consumer.group_metadata()`` (it owns the live handle the FFI
                needs).

        Produce records inside the transaction with :meth:`send`, which
        registers each record before it returns, so the record is part of the
        transaction.

        Raises:
            KafkaError: if the call fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`.
        """
        self._check_closed()
        spec = self._offsets_to_spec(offsets)
        await self._run_async(
            lambda cb: _lib.Producer_send_offsets_to_transaction_async(
                self.c_producer, spec, group_metadata, cb),
            self._resolve_void,
            self._free_void,
        )

    async def commit_transaction(self):
        """Commit the ongoing transaction (Java ``commitTransaction()``).

        Flushes any pending records, then awaits the commit on the event loop
        (no executor thread), so the coroutine is cancellable and the loop is
        never frozen.

        Produce records inside the transaction with :meth:`send`, which
        registers each record before it returns, so the record is part of the
        transaction.

        Raises:
            KafkaError: if the commit fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`; a timeout error is safe to retry.
        """
        self._check_closed()
        await self._run_async(
            lambda cb: _lib.Producer_commit_transaction_async(self.c_producer, cb),
            self._resolve_void,
            self._free_void,
        )

    async def abort_transaction(self):
        """Abort the ongoing transaction (Java ``abortTransaction()``).

        Discards the transaction's records and staged offsets, then awaits the
        abort on the event loop (no executor thread).

        Produce records inside a transaction with :meth:`send`, which registers
        each record before it returns, so the record is part of the transaction.

        Raises:
            KafkaError: if the abort fails.
        """
        self._check_closed()
        await self._run_async(
            lambda cb: _lib.Producer_abort_transaction_async(self.c_producer, cb),
            self._resolve_void,
            self._free_void,
        )

    async def close(self):
        if self.closed:
            return
        self.closed = True
        self._cancel()
        loop = asyncio.get_running_loop()
        # Split teardown (see _confluentkafka.c): reject further sends (a flag
        # flip, inline); await the Rust-side close via the async FFI
        # (_run_async) so it is cooperative with the loop and cancellable, like
        # flush; then free the handle and join the C callback thread off the
        # loop, since that is a blocking C call.
        _lib.Producer_shutdown(self.c_producer)
        await self._run_async(
            lambda cb: _lib.Producer_close_async(self.c_producer, cb),
            self._resolve_void,
            self._free_void,
        )
        await loop.run_in_executor(None, _lib.Producer_destroy, self.c_producer)


class KafkaProducer(Producer):
    """A Kafka producer connected to a real cluster.

    Args:
        config: A dict of configuration properties. At minimum,
            ``bootstrap.servers`` must be provided.
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
        config: A dict of configuration properties. At minimum,
            ``bootstrap.servers`` must be provided.
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
