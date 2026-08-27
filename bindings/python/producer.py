import asyncio
import threading
import _confluentkafka as _lib
from _confluentkafka import ProducerRecord
from concurrent.futures import (Future)


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


class _ProducerBase:
    """State and helpers shared by the sync and async producers.

    The C extension (`_confluentkafka.c`) owns all the asynchronous work:
    two background threads batch records and poll their completion futures,
    then invoke a Python callback ``cb(result, error)`` with the GIL held.
    Both the sync :class:`Producer` and the async :class:`AsyncProducer`
    reuse the same C entry points and differ only in the future type the
    callback resolves and how (see their respective ``send``).
    """

    def __init__(self):
        self.futures = set()
        self.closed = False
        self.c_producer = None

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
    def _raise_if_error(error):
        """Raise a :class:`KafkaError` if a txn C wrapper returned a non-null
        error handle (0 = success). The FFI txn ops return the error pointer
        directly, so this replaces the callback-based ``_resolve_void``."""
        if error:
            raise KafkaError._from_c(error)

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

    def send(self, producer_record: ProducerRecord) -> Future[RecordMetadata]:
        self._check_closed()
        self._validate_record(producer_record)
        ret = Future()

        def cb(result, error):
            if ret.cancelled():
                if error != 0:
                    _lib.KafkaError_destroy(error)
                if result != 0:
                    _lib.RecordMetadata_destroy(result)
                return
            if error != 0:
                if ret.done():
                    _lib.KafkaError_destroy(error)
                    if result != 0:
                        _lib.RecordMetadata_destroy(result)
                    return
                ret.set_exception(
                    KafkaError._from_c(error)
                )
                if result != 0:
                    _lib.RecordMetadata_destroy(result)
            else:
                if ret.done():
                    if result != 0:
                        _lib.RecordMetadata_destroy(result)
                    return
                if result != 0:
                    ret.set_result(RecordMetadata._from_c(result))
                else:
                    ret.set_result(None)

        full = _lib.Producer_send(self.c_producer, producer_record, cb)
        fut = self._add_future(ret)
        if full:
            # Buffer is full: block until the send task frees capacity so a
            # fast producer cannot accumulate records without bound. Mirrors
            # Java's send() blocking when buffer.memory is exhausted.
            # concurrent.futures.Future.result() releases the GIL while waiting,
            # so the C send task can still run the space callback.
            space = Future()
            if not _lib.Producer_on_space_available(
                    self.c_producer, lambda: space.set_result(None)):
                space.result()
        return fut

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
    # §13: records produced inside a transaction MUST use the synchronous
    # send() above. The asynchronous / outbox send path is NOT supported inside
    # a transaction and using it there is undefined behavior — a record queued
    # between begin_transaction() and commit/abort may be published despite an
    # abort, or lost/rejected despite a commit, because the queued send is not
    # ordered against the transaction-control calls. This is documented, not
    # enforced: there is deliberately no runtime guard. Each method's docstring
    # repeats the warning so it shows up in help().

    def init_transactions(self):
        """Initialize transactions (Java ``initTransactions()``).

        Call exactly once, before any other transactional method, when
        ``transactional.id`` is configured. Blocks until the transaction
        coordinator is ready; a timeout error is safe to retry.

        Inside a transaction, produce records only with the synchronous
        :meth:`send`. The async/outbox send path is unsupported and undefined
        inside a transaction: a record queued between :meth:`begin_transaction`
        and commit/abort may be published despite an abort, or lost/rejected
        despite a commit.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        self._raise_if_error(_lib.Producer_init_transactions(self.c_producer))

    def begin_transaction(self):
        """Begin a new transaction (Java ``beginTransaction()``).

        A pure, non-blocking state transition; :meth:`init_transactions` must
        have completed successfully first.

        Produce records into this transaction only with the synchronous
        :meth:`send`. The async/outbox send path is unsupported and undefined
        inside a transaction: a record queued between this call and
        commit/abort may be published despite an abort, or lost/rejected despite
        a commit.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        self._raise_if_error(_lib.Producer_begin_transaction(self.c_producer))

    def send_offsets_to_transaction(self, offsets, group_metadata):
        """Send consumer-group offsets to the coordinator as part of the ongoing
        transaction (Java ``sendOffsetsToTransaction(offsets, groupMetadata)``).

        The producer half of consume-transform-produce: the offsets commit only
        if the transaction commits. Blocks until the coordinator acknowledges.

        Args:
            offsets: a ``{TopicPartition: OffsetAndMetadata}`` mapping (each
                offset is the offset of the *next* record to consume). An empty
                mapping stages nothing.
            group_metadata: the :class:`ConsumerGroupMetadata` from
                ``consumer.group_metadata()`` (it owns the live handle the FFI
                needs).

        Inside a transaction, produce records only with the synchronous
        :meth:`send`; the async/outbox send path is unsupported and undefined
        there (a record queued between :meth:`begin_transaction` and
        commit/abort may be published-despite-abort or lost-despite-commit).

        Raises:
            KafkaError: if the call fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`.
        """
        self._check_closed()
        spec = self._offsets_to_spec(offsets)
        self._raise_if_error(_lib.Producer_send_offsets_to_transaction(
            self.c_producer, spec, group_metadata))

    def commit_transaction(self):
        """Commit the ongoing transaction (Java ``commitTransaction()``).

        Flushes any pending records, then blocks until the transaction is
        committed.

        Produce records only with the synchronous :meth:`send`; the async/outbox
        send path is unsupported and undefined inside a transaction (a record
        queued between :meth:`begin_transaction` and commit/abort may be
        published-despite-abort or lost-despite-commit).

        Raises:
            KafkaError: if the commit fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`; a timeout error is safe to retry.
        """
        self._check_closed()
        self._raise_if_error(_lib.Producer_commit_transaction(self.c_producer))

    def abort_transaction(self):
        """Abort the ongoing transaction (Java ``abortTransaction()``).

        Discards the transaction's records and staged offsets, then blocks until
        the abort completes.

        Produce records only with the synchronous :meth:`send`; the async/outbox
        send path is unsupported and undefined inside a transaction (a record
        queued between :meth:`begin_transaction` and commit/abort may be
        published-despite-abort or lost-despite-commit).

        Raises:
            KafkaError: if the abort fails.
        """
        self._check_closed()
        self._raise_if_error(_lib.Producer_abort_transaction(self.c_producer))

    def close(self):
        if self.closed:
            return
        self.closed = True
        self._cancel()
        # Split teardown (see _confluentkafka.c): join the C batching threads,
        # then drive the Rust-side close through the interruptible _run_sync
        # path (same as flush), then free. Keeping the Rust close in _run_sync
        # means a stuck close stays responsive to KeyboardInterrupt on the main
        # thread rather than blocking in a native wait.
        _lib.Producer_shutdown(self.c_producer)
        self._run_sync(
            lambda cb: _lib.Producer_close_async(self.c_producer, cb),
            self._resolve_void,
        )
        _lib.Producer_destroy(self.c_producer)


class AsyncProducer(_ProducerBase):
    """An asyncio-native producer.

    ``send`` is a coroutine that returns an :class:`asyncio.Future` resolving
    to a :class:`RecordMetadata` (``fut = await producer.send(rec)``; then
    ``await fut`` for the result). It is a coroutine — rather than a plain
    method like the sync :class:`Producer` — so it can suspend on backpressure
    (``await``-ing buffer capacity when the producer is full); the produce
    itself is non-blocking. That same suspension yields the event loop to the
    completion drain, so a flooding ``await producer.send(...)`` loop does not
    starve completions.

    Completions from the C background thread are marshalled back onto the event
    loop (an ``asyncio.Future`` is not thread-safe).

    Completions are *coalesced*: the C poll task invokes the callback once per
    record, but rather than waking the event loop once per record (one
    ``call_soon_threadsafe`` each), each callback buffers its
    ``(future, result, error)`` and schedules a single drain only when one is
    not already pending. The drain then resolves the whole accumulated batch in
    one event-loop wakeup. This keeps the per-record cross-thread signalling
    cost — the bottleneck under high produce rates — off the hot path.
    """

    def __init__(self):
        super().__init__()
        # Completions buffered by the C poll task (producer thread), drained on
        # the event loop. Guarded by a lock since the two run on different
        # threads; the critical sections are tiny (append / list swap).
        self._pending = []
        self._drain_scheduled = False
        self._pending_lock = threading.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @staticmethod
    def _resolve_future(ret, result, error):
        """Resolve a single future from C completion handles. Runs on the event
        loop thread, so it is safe to mutate the asyncio.Future. Ownership of
        the ``result`` / ``error`` C handles transfers here and is always
        freed."""
        if ret.cancelled():
            if error != 0:
                _lib.KafkaError_destroy(error)
            if result != 0:
                _lib.RecordMetadata_destroy(result)
            return
        if error != 0:
            if ret.done():
                _lib.KafkaError_destroy(error)
                if result != 0:
                    _lib.RecordMetadata_destroy(result)
                return
            ret.set_exception(KafkaError._from_c(error))
            if result != 0:
                _lib.RecordMetadata_destroy(result)
        else:
            if ret.done():
                if result != 0:
                    _lib.RecordMetadata_destroy(result)
                return
            if result != 0:
                ret.set_result(RecordMetadata._from_c(result))
            else:
                ret.set_result(None)

    @staticmethod
    def _resolve_space(space):
        """Resolve a space-available future. Runs on the event loop thread
        (scheduled via call_soon_threadsafe from the C send task)."""
        if not space.done():
            space.set_result(None)

    def _drain(self):
        """Resolve all buffered completions. Runs on the event loop thread."""
        with self._pending_lock:
            items = self._pending
            self._pending = []
            self._drain_scheduled = False
        for ret, result, error in items:
            self._resolve_future(ret, result, error)

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

    async def send(self, producer_record: ProducerRecord) \
            -> "asyncio.Future[RecordMetadata]":
        self._check_closed()
        self._validate_record(producer_record)
        loop = asyncio.get_running_loop()
        ret = loop.create_future()

        # Runs on the C background (poll) thread with the GIL held. asyncio
        # futures must only be mutated on the loop thread, so buffer the
        # completion and wake the loop once per drain (coalescing) rather than
        # once per record. If the loop is already closed we can't schedule
        # anything — free the C handles here to avoid leaking them.
        def cb(result, error):
            if loop.is_closed():
                if error != 0:
                    _lib.KafkaError_destroy(error)
                if result != 0:
                    _lib.RecordMetadata_destroy(result)
                return
            with self._pending_lock:
                self._pending.append((ret, result, error))
                if self._drain_scheduled:
                    return
                self._drain_scheduled = True
                loop.call_soon_threadsafe(self._drain)

        full = _lib.Producer_send(self.c_producer, producer_record, cb)
        self._add_future(ret)
        if full:
            # Buffer is full: await (yielding the loop, non-blocking) until the
            # send task frees capacity, bounding accumulation — Java's send()
            # blocks on buffer.memory here. Awaiting also yields to the
            # completion drain. The space callback runs on the C send task, so
            # it hops onto the loop via call_soon_threadsafe.
            space = loop.create_future()

            def space_cb():
                if not loop.is_closed():
                    loop.call_soon_threadsafe(self._resolve_space, space)

            if not _lib.Producer_on_space_available(self.c_producer, space_cb):
                await space
        return ret

    async def _run_async(self, submit, resolve, free):
        """Submit an async FFI op and ``await`` its completion on the event loop.

        Mirrors the async Consumer's ``_run_async``: the completion callback runs
        on the producer's dispatcher thread and hops onto the loop via
        ``call_soon_threadsafe`` (asyncio futures are not thread-safe). If the
        loop is already closed we can't schedule, so the C handles are freed
        inline via ``free`` to avoid leaking them."""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def deliver(payload):
            if not fut.done():
                fut.set_result(payload)

        def cb(*payload):
            if loop.is_closed():
                free(payload)
                return
            loop.call_soon_threadsafe(deliver, payload)

        submit(cb)
        payload = await fut
        return resolve(payload)

    async def _run_blocking(self, fn, *args):
        """Run a blocking transaction C op on the default executor and await it,
        raising a :class:`KafkaError` if it returns a non-null error handle.

        ``run_in_executor`` cannot cancel the C call once the executor thread has
        picked it up. If this coroutine is cancelled (e.g. under
        ``asyncio.wait_for``) after the executor has produced a non-null
        ``KafkaError`` handle but before :meth:`_raise_if_error` consumes it, that
        handle would leak — unbounded under a retry/cancel loop. To prevent that
        we ``shield`` the executor future so cancelling the *await* does not
        cancel the still-running C call, and on cancellation we attach a
        done-callback that frees the handle once the executor thread finishes,
        then re-raise. The normal (non-cancelled) path is unchanged: the returned
        handle flows to :meth:`_raise_if_error` exactly as before.

        The cancellation window is a genuine thread/loop race (the executor thread
        must produce the handle in the instant between the await being cancelled
        and this frame resuming), so there is no deterministic way to hit it from
        a unit test; the freeing logic is exercised indirectly by every async txn
        op routed through here.
        """
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(None, fn, *args)
        try:
            error = await asyncio.shield(fut)
        except asyncio.CancelledError:
            def _free_when_done(f):
                # `shield` kept `fut` running, so it completes with the executor's
                # result and is never cancelled here; free any non-null handle it
                # produced so the cancellation cannot strand a KafkaError.
                if not f.cancelled():
                    err = f.result()
                    if err:
                        _lib.KafkaError_destroy(err)
            fut.add_done_callback(_free_when_done)
            raise
        self._raise_if_error(error)

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
    # The blocking ops (init / commit / abort / send_offsets) run the blocking C
    # call on the default executor and ``await`` it, so the event loop is never
    # frozen (the C wrapper also releases the GIL). begin_transaction is a pure
    # non-blocking state transition and is called directly (no executor), which
    # is genuinely async — not a block_on façade (DoD #11).
    #
    # §13: records produced inside a transaction MUST use the synchronous
    # send() (``await producer.send(rec)``). The async/outbox send path is NOT
    # supported inside a transaction and using it there is undefined behavior — a
    # record queued between begin_transaction() and commit/abort may be published
    # despite an abort, or lost/rejected despite a commit. Documented, not
    # enforced: no runtime guard.

    async def init_transactions(self):
        """Initialize transactions (Java ``initTransactions()``).

        Call exactly once, before any other transactional method, when
        ``transactional.id`` is configured. Runs the blocking coordinator
        handshake on the executor; a timeout error is safe to retry.

        Inside a transaction, produce records only with the synchronous
        :meth:`send`. The async/outbox send path is unsupported and undefined
        inside a transaction: a record queued between :meth:`begin_transaction`
        and commit/abort may be published despite an abort, or lost/rejected
        despite a commit.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        await self._run_blocking(_lib.Producer_init_transactions, self.c_producer)

    async def begin_transaction(self):
        """Begin a new transaction (Java ``beginTransaction()``).

        A pure, non-blocking state transition, so it is called directly (not on
        the executor); :meth:`init_transactions` must have completed first.

        Produce records into this transaction only with the synchronous
        :meth:`send`. The async/outbox send path is unsupported and undefined
        inside a transaction: a record queued between this call and commit/abort
        may be published despite an abort, or lost/rejected despite a commit.

        Raises:
            KafkaError: if the call fails.
        """
        self._check_closed()
        self._raise_if_error(_lib.Producer_begin_transaction(self.c_producer))

    async def send_offsets_to_transaction(self, offsets, group_metadata):
        """Send consumer-group offsets to the coordinator as part of the ongoing
        transaction (Java ``sendOffsetsToTransaction(offsets, groupMetadata)``).

        The producer half of consume-transform-produce: the offsets commit only
        if the transaction commits. Runs the blocking coordinator call on the
        executor.

        Args:
            offsets: a ``{TopicPartition: OffsetAndMetadata}`` mapping (each
                offset is the offset of the *next* record to consume). An empty
                mapping stages nothing.
            group_metadata: the :class:`ConsumerGroupMetadata` from
                ``consumer.group_metadata()`` (it owns the live handle the FFI
                needs).

        Inside a transaction, produce records only with the synchronous
        :meth:`send`; the async/outbox send path is unsupported and undefined
        there (a record queued between :meth:`begin_transaction` and
        commit/abort may be published-despite-abort or lost-despite-commit).

        Raises:
            KafkaError: if the call fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`.
        """
        self._check_closed()
        spec = self._offsets_to_spec(offsets)
        await self._run_blocking(
            _lib.Producer_send_offsets_to_transaction,
            self.c_producer, spec, group_metadata)

    async def commit_transaction(self):
        """Commit the ongoing transaction (Java ``commitTransaction()``).

        Flushes any pending records, then runs the blocking commit on the
        executor.

        Produce records only with the synchronous :meth:`send`; the async/outbox
        send path is unsupported and undefined inside a transaction (a record
        queued between :meth:`begin_transaction` and commit/abort may be
        published-despite-abort or lost-despite-commit).

        Raises:
            KafkaError: if the commit fails. If ``err.txn_requires_abort`` is
                ``True`` the transaction must be aborted with
                :meth:`abort_transaction`; a timeout error is safe to retry.
        """
        self._check_closed()
        await self._run_blocking(_lib.Producer_commit_transaction, self.c_producer)

    async def abort_transaction(self):
        """Abort the ongoing transaction (Java ``abortTransaction()``).

        Discards the transaction's records and staged offsets, then runs the
        blocking abort on the executor.

        Produce records only with the synchronous :meth:`send`; the async/outbox
        send path is unsupported and undefined inside a transaction (a record
        queued between :meth:`begin_transaction` and commit/abort may be
        published-despite-abort or lost-despite-commit).

        Raises:
            KafkaError: if the abort fails.
        """
        self._check_closed()
        await self._run_blocking(_lib.Producer_abort_transaction, self.c_producer)

    async def close(self):
        if self.closed:
            return
        self.closed = True
        self._cancel()
        loop = asyncio.get_running_loop()
        # Split teardown (see _confluentkafka.c): the C batching-thread join and
        # the final free are blocking C calls, so run them off the event loop;
        # the Rust-side close is awaited via the async FFI (_run_async) so it is
        # cooperative with the loop and cancellable, like flush.
        await loop.run_in_executor(None, _lib.Producer_shutdown, self.c_producer)
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
