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

"""``Producer``: Java's ``org.apache.kafka.clients.producer.Producer``.

A Java client interface, so a non-instantiable base class with all its methods
(CLAUDE.md, Python Binding Conventions, Class family): ``KafkaProducer`` adds
only Java's constructors, ``MockProducer`` its constructors and the Java mock's
methods. The methods here call the C FFI (Implementation over the FFI); the
Javadoc of each is ``KafkaProducer``'s, which the interface's points to.

Not generated, their FFI entry point being missing:
``registerMetricForSubscription`` / ``unregisterMetricFromSubscription``
(``kafka_producer_Producer_register_metric_for_subscription`` / ``..._unregister_...``)
and ``clientInstanceId(Duration)`` (``kafka_producer_Producer_client_instance_id``).

Each method waits on the entry point's queued ``_cb`` form in 100 ms slices,
never in a blocking native call, so Ctrl+C is seen: the operation is let finish
and ``KeyboardInterrupt`` is raised after it, the producer staying usable (a
``close()`` is forced instead). The completions -- the ``send()`` callbacks
and the results of the waiting calls -- run on one background completion
thread per producer, which runs ``kafka_producer_Producer__execute_callbacks``
whenever the C API's notify hook signals it (CLAUDE.md, Python Binding
Conventions, Threads and callbacks).

*(deviation)* Backpressure is the core's ``max.block.ms`` wait on the
submission task, not a wait in ``send()`` (see ``_base``).

*(deviation)* A call from a ``send()`` callback cannot have its completion run,
since that runs on the thread the callback blocks: ``send()`` there does not
wait for its record to reach the producer, and ``init_transactions``,
``send_offsets_to_transaction``, ``commit_transaction``, ``abort_transaction``
and ``partitions_for`` raise ``TimeoutError`` after ``max.block.ms``, which is
when Java's waits, blocking the I/O thread its completion needs, time out; the
operation itself may still complete. ``begin_transaction`` does too, although
Java's does not wait: its queued form first drains the records already sent
(producer-transactions.md §13), and its completion needs that same thread.
The core may then have begun the transaction while the producer still counts
none as started, since the result arrives only once the callback returns: a
retried ``begin_transaction`` raises Java's ``IllegalStateError`` for a
transaction already in progress, and sends from outside a callback keep
waiting for their record's handover, as with no transaction started, until
``commit_transaction`` or ``abort_transaction`` ends it.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import UNSET
from confluent_kafka._errors import from_ffi_error
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.null_pointer_error import NullPointerError

from ._base import (
    CLOSED_WHILE_SENDING_MESSAGE,
    FLUSH_IN_CALLBACK_MESSAGE,
    LONG_MAX_VALUE,
    Completion,
    _ProducerState,
    check_group_metadata,
    close_timeout_ms,
    free_error,
    free_error_payload,
    group_metadata_handle,
    offsets_to_spec,
    raise_if_error,
    to_metrics_map,
    to_partition_info,
    wait_futures,
    wait_sliced,
)
from ._send import (
    MetadataTuple,
    completion_to_python,
    failure_metadata,
    in_callback,
    invoke_callback,
    log_unreported_callback_error,
)
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import MetricName, PartitionInfo, TopicPartition
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from .callback import Callback
    from .producer_record import ProducerRecord

__all__ = ["Producer"]

K = TypeVar("K")
V = TypeVar("V")

_LOG = logging.getLogger("confluent_kafka.producer")

# The completion thread's name, and how long one run of its pump waits for the
# notify hook before it looks at its stop flag again.
_COMPLETION_THREAD_NAME = "confluent-kafka-producer-completion"
_PUMP_WAIT_MS = 100


def _free_partitions_payload(payload: tuple[Any, ...]) -> None:
    """Free the ``(list, error)`` payload of ``partitions_for``: the list is
    plain Python, only the error is a handle."""
    free_error(payload[-1])


class Producer(Generic[K, V], _ProducerState):
    """The interface for the ``KafkaProducer``.

    Java: ``org.apache.kafka.clients.producer.Producer<K, V>`` (``Closeable``:
    a context manager whose exit flushes, then closes).
    """

    def __init__(self) -> None:
        if type(self) is Producer:
            raise TypeError(
                "Producer is a non-instantiable base; use KafkaProducer or MockProducer")
        _ProducerState.__init__(self)
        self._completion_thread: threading.Thread | None = None
        # Set by close(): the completion thread ends after its current pump.
        self._stop_completions = False
        # Set by the completion of a close() from a send() callback: the
        # completion thread tears the producer down and ends.
        self._teardown_on_completion_thread = False

    def _after_start(self) -> None:
        thread = threading.Thread(target=self._run_completions, name=_COMPLETION_THREAD_NAME,
                                  daemon=True)
        self._completion_thread = thread
        thread.start()

    def _run_completions(self) -> None:
        """The completion thread: run the producer's queued callbacks
        (``Producer_poll`` waits for the notify hook between runs) until
        ``close()`` stops it, one at a time in completion order."""
        c_producer = self._c_producer
        while not self._stop_completions:
            if not self._pump_use():
                return
            try:
                _lib.Producer_poll(c_producer, _PUMP_WAIT_MS)
            finally:
                self._end_use()
            if self._teardown_on_completion_thread:
                # The close() a callback asked for has completed (see close).
                self._destroy()
                self._close_serializers()
                return

    def _stop_completion_thread(self) -> None:
        self._stop_completions = True
        thread = self._completion_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()

    def _run_queued(self, operation: str, entry: Callable[..., None], *args: Any,
                    free: Callable[[tuple[Any, ...]], None] = free_error_payload
                    ) -> tuple[Any, ...]:
        """Submit ``entry(handle, *args, completion)`` (a queued ``_cb`` entry
        point) as one use of the handle and wait for its payload. From a
        ``send()`` callback the wait is bounded by ``max.block.ms`` (see the
        module docstring)."""
        completion = Completion(free)
        deadline = self._callback_deadline() if in_callback(self) else None
        with self._use() as c_producer:
            entry(c_producer, *args, completion)
        payload = completion.wait(deadline)
        if payload is None:
            raise self._callback_wait_timeout(operation)
        return payload

    def _run_void(self, operation: str, entry: Callable[..., None], *args: Any) -> None:
        """:meth:`_run_queued` for a void operation: raises its typed error."""
        (error,) = self._run_queued(operation, entry, *args)
        raise_if_error(error)

    def init_transactions(self) -> None:
        """Needs to be called before any other methods when the
        ``transactional.id`` is set in the configuration. It ensures any
        transactions initiated by previous instances of the producer with the
        same ``transactional.id`` are completed (a transaction in progress is
        aborted; one that had begun completion is awaited), and gets the
        internal producer id and epoch used in all future transactional
        messages issued by the producer.

        Raises ``TimeoutError`` if the transactional state cannot be initialized
        before expiration of ``max.block.ms``; it is safe to retry, but once the
        transactional state has been successfully initialized, this method
        should no longer be used. Raises ``IllegalStateError`` if no
        ``transactional.id`` has been configured, ``UnsupportedVersionError`` if
        the broker does not support transactions, ``AuthorizationError`` if the
        configured ``transactional.id`` is not authorized, and ``KafkaError`` if
        the producer has encountered a previous fatal error.
        """
        self._check_not_closed()
        # The C API drains the submission queue first (producer-transactions.md
        # §13), as for every transaction-control operation.
        self._run_void("InitProducerId", _lib.Producer_init_transactions_cb)

    def begin_transaction(self) -> None:
        """Should be called before the start of each new transaction. Note that
        prior to the first invocation of this method, you must invoke
        ``init_transactions()`` exactly one time.

        Raises ``IllegalStateError`` if no ``transactional.id`` has been
        configured or if ``init_transactions()`` has not yet been invoked,
        ``ProducerFencedError`` if another producer with the same
        ``transactional.id`` is active, and ``KafkaError`` if the producer has
        encountered a previous fatal error.
        """
        self._check_not_closed()
        # Like every transaction-control operation the C API drains the
        # submission queue first (producer-transactions.md §13).
        self._run_void("beginTransaction", _lib.Producer_begin_transaction_cb)
        self._in_transaction = True

    def send_offsets_to_transaction(
            self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        """Sends a list of specified offsets to the consumer group coordinator,
        and also marks those offsets as part of the current transaction. These
        offsets will be considered committed only if the transaction is
        committed successfully. The committed offset should be the next message
        your application will consume, i.e. ``next_record_to_be_processed.offset()``
        (or ``ConsumerRecords.next_offsets()``).

        This method should be used when you need to batch consumed and produced
        messages together, typically in a consume-transform-produce pattern; the
        ``group_metadata`` should be extracted from the used consumer via
        ``KafkaConsumer.group_metadata()``. It waits until the request has been
        received and acknowledged by the consumer group coordinator, and raises
        ``TimeoutError`` if the producer cannot send offsets before expiration of
        ``max.block.ms``.

        Raises ``IllegalArgumentError`` if ``group_metadata`` is ``None`` or has
        a generation id above 0 with an unknown member id,
        ``UnsupportedVersionError`` if ``group_metadata`` comes from a mock
        consumer, ``IllegalStateError`` if no ``transactional.id`` has been
        configured or no transaction has been started, ``ProducerFencedError``,
        ``CommitFailedError`` if the commit failed and cannot be retried, and
        ``KafkaError`` if the producer has encountered a previous fatal or
        abortable error.
        """
        self._check_not_closed()
        check_group_metadata(group_metadata)
        native = group_metadata_handle(group_metadata)
        spec = offsets_to_spec(offsets)
        self._run_void("AddOffsetsToTxn", _lib.Producer_send_offsets_to_transaction_cb,
                       spec, native)

    def commit_transaction(self) -> None:
        """Commits the ongoing transaction. This method will flush any unsent
        records before actually committing the transaction.

        If any of the ``send()`` calls which were part of the transaction hit
        irrecoverable errors, this method will raise the last received error
        immediately and the transaction will not be committed. If the
        transaction is committed successfully and this method returns without
        raising, it is guaranteed that all callbacks for records in the
        transaction will have been invoked and completed; exceptions raised by
        callbacks are ignored.

        Raises ``TimeoutError`` if the transaction cannot be committed before
        expiration of ``max.block.ms``: it is safe to retry, but not to attempt
        a different operation such as ``abort_transaction()``. Raises
        ``IllegalStateError`` if no ``transactional.id`` has been configured or
        no transaction has been started, ``ProducerFencedError``, and
        ``KafkaError`` if the producer has encountered a previous fatal or
        abortable error.
        """
        self._check_not_closed()
        sent = list(self._futures)
        # Sends from now on wait for their handover again (see _ProducerState).
        self._in_transaction = False
        self._run_void("EndTxn(true)", _lib.Producer_commit_transaction_cb)
        if not in_callback(self):
            # Their callbacks run on the completion thread, which a callback
            # calling this method is blocking.
            wait_futures(sent)

    def abort_transaction(self) -> None:
        """Aborts the ongoing transaction. Any unflushed produce messages will
        be aborted when this call is made. This call will raise an error
        immediately if any prior ``send()`` calls failed with a
        ``ProducerFencedError`` or an ``AuthorizationError``.

        Raises ``TimeoutError`` if the transaction cannot be aborted before
        expiration of ``max.block.ms``: it is safe to retry, but not to attempt
        a different operation such as ``commit_transaction()``. Raises
        ``IllegalStateError`` if no ``transactional.id`` has been configured or
        no transaction has been started, ``ProducerFencedError``, and
        ``KafkaError`` if the producer has encountered a previous fatal error.
        """
        self._check_not_closed()
        # Sends from now on wait for their handover again (see _ProducerState).
        self._in_transaction = False
        self._run_void("EndTxn(false)", _lib.Producer_abort_transaction_cb)

    def send(self, *, record: ProducerRecord[K, V],
             callback: Callback | None = UNSET) -> Future[RecordMetadata]:
        """Asynchronously send a record to a topic and invoke the provided
        ``callback`` when the send has been acknowledged (``None`` indicates no
        callback). Java's ``send(record)`` passes ``null`` to it, so
        ``callback`` defaults to ``UNSET`` (Signatures) and is filled here.

        The send is asynchronous and this method will return immediately
        (except for rare cases described below) once the record has been stored
        in the buffer of records waiting to be sent. It can block 1) for the
        first record sent to a topic, up to ``max.block.ms`` while waiting for
        the topic's metadata, and 2) while the buffer is full. The key and value
        are serialized on the calling thread, and a serializer's error is raised
        here.

        The result of the send is a ``RecordMetadata`` specifying the partition
        the record was sent to, the offset it was assigned and the timestamp of
        the record (offset -1 with ``acks=0``). ``future.result()`` waits until
        the request completes and returns the metadata or raises the error that
        occurred while sending the record. The future cannot be cancelled, as
        Java's.

        The ``callback`` runs on the producer's background completion thread,
        never on the calling thread, after the record completes and before its
        future does. Callbacks for records being sent to the same partition are
        guaranteed to execute in order. A raising callback fails nothing: its
        exception is reported to the core, which logs it once with Java's
        message, "Error executing user-provided callback on message for
        topic-partition '<topic>-<partition>'", and the exception's text. It
        should be reasonably fast, or it will delay the completion of other
        records. An ``async def`` callback cannot be awaited there: its
        coroutine is closed and a ``TypeError`` is reported the same way
        (``AsyncProducer`` awaits it).

        When used as part of a transaction, it is not necessary to define a
        callback or check the result of the future in order to detect errors
        from ``send``: if any of the send calls failed with an irrecoverable
        error, the final ``commit_transaction()`` call will fail and raise the
        error from the last failed send.

        Raises ``IllegalStateError`` when send is invoked after the producer has
        been closed, and ``KafkaError("Producer closed while send in
        progress")`` when the producer is closed while it runs. With a
        ``transactional.id`` configured and no transaction started (no
        ``begin_transaction()`` since ``init_transactions()`` or the last
        ``commit_transaction()`` / ``abort_transaction()``), send returns once
        the record is with the producer (after the metadata wait, as Java's) and
        raises ``IllegalStateError`` as Java's does, e.g. "Cannot add partition
        … to transaction while in state  READY". As in Java, such a record has
        already reached the producer's buffer: it stays there until a
        transaction adds its partition (it is then committed or aborted with
        that transaction) or it fails (a forced close aborts it, say), and its
        ``callback`` runs then, once. A record whose ``send()`` raises before
        it reaches the buffer (a producer closed meanwhile, say) never runs its
        callback, as in Java. The other errors of a send
        (``TimeoutError`` for the metadata wait, ``RecordTooLargeError``,
        ``ProducerFencedError``, …) are given to the callback and the future, as
        in Java. So are, unlike Java, the errors its ``send()`` raises once the
        record reaches the producer inside a started transaction or without a
        ``transactional.id``: the ``KafkaError`` of a producer in a previous
        fatal or abortable error state, which ``commit_transaction()`` then
        raises.
        """
        if callback is UNSET:
            callback = None
        self._check_not_closed()
        native = self._native_record(record)
        topic = record.topic()
        partition = record.partition()
        future: Future[RecordMetadata] = Future()
        # Java's FutureRecordMetadata.cancel() returns false: a running future
        # cannot be cancelled.
        future.set_running_or_notify_cancel()

        # A transactional producer's send with no transaction started waits
        # for the handover (below) and raises the error the Rust send() returns:
        # Java's doSend rethrows those (KafkaProducer.java:1150-1162), where an
        # ApiException reaches the callback and the future (:1056-1068). Inside
        # a started transaction the record is not waited for (see
        # _ProducerState._in_transaction), nor from a send() callback, whose
        # thread runs the handover (see the module docstring).
        waiting = self._transactional and not self._in_transaction and not in_callback(self)
        handed_over = threading.Event()
        lock = threading.Lock()
        completed = False
        rethrown: Exception | None = None

        def complete(metadata: RecordMetadata, exception: Exception | None) -> Exception | None:
            # Once per record. As in Java's
            # ProducerBatch.completeFutureAndFireCallbacks, the callback runs
            # before the future completes. Returns the callback's exception.
            nonlocal completed
            with lock:
                if completed:
                    return None
                completed = True
            failure = invoke_callback(self, callback, metadata, exception)
            if exception is not None:
                future.set_exception(exception)
            else:
                future.set_result(metadata)
            return failure

        def on_completion(metadata_tuple: MetadataTuple | None, error: int,
                          callback_id: int) -> None:
            # The record's Callback, on the completion thread. Returning
            # reports the callback to the C API, raising reports its exception
            # (see _send); the C future, a flush() and the transaction-end calls
            # wait for that report.
            failure = complete(*completion_to_python(metadata_tuple, error, topic, partition))
            if failure is not None:
                raise failure

        def resolve(error: int) -> None:
            # The handover, on the completion thread: the error the Rust send()
            # returned, if any.
            nonlocal rethrown
            if error:
                exception = cast("Exception", from_ffi_error(error))
                if waiting:
                    # Rethrown by send(), as Java's doSend does. The record's
                    # Callback may still run: doSend appends the record, its
                    # appendCallbacks carrying the Callback, before
                    # maybeAddPartition throws (KafkaProducer.java:1027-1039),
                    # so a record that had reached the accumulator completes
                    # with its batch (on_completion, once); one rejected before
                    # the append never does. The future is left to that
                    # completion: send() does not return it.
                    rethrown = exception
                else:
                    # No delivery callback reports this completion, so an
                    # exception the callback raises has no core report to go
                    # to: it is logged here instead (see _send).
                    metadata = failure_metadata(topic, partition)
                    failure = complete(metadata, exception)
                    if failure is not None:
                        log_unreported_callback_error(metadata, failure)
            handed_over.set()

        # A close() that began since the check above (the serializers ran in
        # between) refuses the record as Java's RecordAccumulator.append does.
        with self._use(KafkaError(message=CLOSED_WHILE_SENDING_MESSAGE)) as c_producer:
            raise_if_error(_lib.Producer_send_cb(c_producer, native, on_completion, resolve))
            if not waiting:
                self._track(future)
        if waiting:
            try:
                # A Ctrl+C is raised once the record is handed over.
                wait_sliced(handed_over)
            finally:
                # Tracked only once handed over without an error: flush() and
                # commit_transaction() must not wait for a rethrown record,
                # whose Callback may never run (the core's own flush covers
                # it if it reached the accumulator).
                if rethrown is None:
                    self._track(future)
            if rethrown is not None:
                raise rethrown
        return future

    def flush(self) -> None:
        """Invoking this method makes all buffered records immediately available
        to send (even if ``linger.ms`` is greater than 0) and waits on the
        completion of the requests associated with these records. A request is
        considered completed when it is successful according to the ``acks``
        configuration you have specified or else it results in an error; when
        ``flush()`` returns, the callbacks of those records have run.

        Other threads can continue sending records while one thread is blocked
        waiting for a flush call to complete, however no guarantee is made about
        the completion of records sent after the flush call begins.
        Applications don't need to call this method for transactional producers,
        since ``commit_transaction()`` flushes all buffered records first.

        This method must not be called from within a ``send()`` callback: it
        raises ``KafkaError``, as it would cause a deadlock.
        """
        self._check_not_closed()
        if in_callback(self):
            _LOG.error(FLUSH_IN_CALLBACK_MESSAGE)
            raise KafkaError(message=FLUSH_IN_CALLBACK_MESSAGE)
        sent = list(self._futures)
        # The C API drains the submission queue first, so every record sent
        # before this call is flushed (producer-transactions.md §13).
        self._run_void("flush", _lib.Producer_flush_cb)
        wait_futures(sent)

    def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """Get the partition metadata for the given topic. This can be used for
        custom partitioning. This will attempt to refresh metadata until it
        finds the topic in it, or the configured ``max.block.ms`` expires
        (``TimeoutError``). Raises ``NullPointerError`` for a ``None`` topic.
        """
        self._check_not_closed()
        if topic is None:
            raise NullPointerError(message="topic cannot be null")
        partitions, error = self._run_queued(
            f"the metadata of topic {topic}", _lib.Producer_partitions_for_cb, topic,
            free=_free_partitions_payload)
        raise_if_error(error)
        return [to_partition_info(t) for t in partitions]

    def metrics(self) -> dict[MetricName, Metric]:
        """Get the full set of internal metrics maintained by the producer."""
        self._check_not_closed()
        return to_metrics_map(self._call(_lib.Producer_metrics))

    def close(self, *, timeout: Duration | None = None) -> None:
        """Close this producer: ``close()`` waits until all previously sent
        requests complete; ``close(timeout=…)`` waits up to ``timeout`` for the
        producer to complete the sending of all incomplete requests, and if it
        cannot, fails any unsent and unacknowledged records immediately (and
        aborts the ongoing transaction if it is not already completing). A
        timeout of zero means do not wait for pending send requests to complete.

        If invoked from within a ``send()`` callback this method does not wait
        and is equivalent to ``close(timeout=0)``: no further sending would
        happen while it blocks the producer's completion thread.

        Closing twice is harmless; any other call after it raises
        ``IllegalStateError``. A negative ``timeout`` raises
        ``IllegalArgumentError``, except that a second ``close()`` returns
        before checking it *(deviation)*.
        """
        if self._closed:
            return
        timeout_ms = close_timeout_ms(timeout)
        # Check and set at once: exactly one close() tears the producer down.
        if not self._begin_close():
            return
        if in_callback(self):
            if timeout_ms is None or timeout_ms > 0:
                _LOG.warning(
                    "Overriding close timeout %d ms to 0 ms in order to prevent useless "
                    "blocking due to self-join. This means you have incorrectly invoked "
                    "close with a non-zero timeout from the producer call-back.",
                    LONG_MAX_VALUE if timeout_ms is None else timeout_ms)
            self._close_from_callback()
            return
        c_producer = self._c_producer
        completion = Completion(free_error_payload)

        def force() -> None:
            # Java's sender.forceClose() for a close() an interrupt stopped
            # (KafkaProducer.close(Duration, boolean), KafkaProducer.java:1419-1437:
            # the interrupt is kept and the close proceeds to force-close and
            # join): a close with a zero timeout fails the records still pending.
            _lib.Producer_close_with_timeout_cb(c_producer, 0, free_error)

        payload: tuple[Any, ...] | None = None
        try:
            # close() is Java's close(Duration.ofMillis(Long.MAX_VALUE)). The C
            # API drains the submission queue first (as flush), so every record
            # sent before is included; a timed close spends at most its timeout on
            # that drain and the close together, failing what it did not reach.
            if timeout_ms is None:
                _lib.Producer_close_cb(c_producer, completion)
            else:
                _lib.Producer_close_with_timeout_cb(c_producer, timeout_ms, completion)
            payload = completion.wait(on_interrupt=force)
        finally:
            # The completion thread ends, the calls in flight end, then the
            # handle is freed (running the callbacks still pending here).
            self._stop_completion_thread()
            self._destroy()
            self._close_serializers()
        assert payload is not None
        raise_if_error(payload[0])

    def _close_from_callback(self) -> None:
        """Java's ``close(0)`` from the I/O thread, which does not join itself:
        queue a close with a zero timeout and return; once it completes, the
        completion thread -- the one this callback blocks -- frees the producer
        and ends (see :meth:`_run_completions`)."""

        def closed(error: int) -> None:
            free_error(error)
            self._teardown_on_completion_thread = True

        _lib.Producer_close_with_timeout_cb(self._c_producer, 0, closed)

    def __enter__(self) -> Producer[K, V]:
        return self

    def __exit__(self, *exc: object) -> None:
        # Closeable: flush, then close (the close runs even if the flush fails).
        try:
            if not self._closed:
                self.flush()
        finally:
            self.close()

