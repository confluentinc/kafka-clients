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

Not generated, their FFI entry point being missing (``ffi-overload-gaps.md``):
``registerMetricForSubscription`` / ``unregisterMetricFromSubscription``
(``kafka_producer_Producer_register_metric_for_subscription`` / ``..._unregister_...``)
and ``clientInstanceId(Duration)`` (``kafka_producer_Producer_client_instance_id``).
"""

from __future__ import annotations

import concurrent.futures
import logging
import time
from collections.abc import Mapping
from concurrent.futures import Future
from typing import TYPE_CHECKING, Generic, TypeVar

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.null_pointer_error import NullPointerError

from ._base import (
    CLOSED_WHILE_SENDING_MESSAGE,
    FLUSH_IN_CALLBACK_MESSAGE,
    LONG_MAX_VALUE,
    _ProducerState,
    await_payload,
    check_group_metadata,
    close_timeout_ms,
    group_metadata_fields,
    offsets_to_spec,
    raise_if_error,
    run_sync,
    to_metrics_map,
    to_partition_info,
)
from ._send import completion_to_python, in_callback, invoke_callback
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
        self._drain_sync()
        run_sync(lambda cb: self._call(_lib.Producer_init_transactions_async, cb))

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
        self._drain_sync()
        raise_if_error(self._call(_lib.Producer_begin_transaction))

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
        ``IllegalStateError`` if no ``transactional.id`` has been configured or
        no transaction has been started, ``ProducerFencedError``,
        ``CommitFailedError`` if the commit failed and cannot be retried, and
        ``KafkaError`` if the producer has encountered a previous fatal or
        abortable error.
        """
        check_group_metadata(group_metadata)
        self._check_not_closed()
        self._drain_sync()
        spec = offsets_to_spec(offsets)
        fields = group_metadata_fields(group_metadata)
        run_sync(lambda cb: self._call(_lib.Producer_send_offsets_to_transaction_fields_async,
                                       spec, fields, cb))

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
        self._drain_sync()
        run_sync(lambda cb: self._call(_lib.Producer_commit_transaction_async, cb))
        if not in_callback(self):
            # Their callbacks run on the completion thread, which a callback
            # calling this method is blocking.
            concurrent.futures.wait(sent)

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
        self._drain_sync()
        run_sync(lambda cb: self._call(_lib.Producer_abort_transaction_async, cb))

    def send(self, *, record: ProducerRecord[K, V],
             callback: Callback | None = None) -> Future[RecordMetadata]:
        """Asynchronously send a record to a topic and invoke the provided
        ``callback`` when the send has been acknowledged (``None`` indicates no
        callback).

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
        guaranteed to execute in order. A raising callback is logged. It should
        be reasonably fast, or it will delay the completion of other records.

        When used as part of a transaction, it is not necessary to define a
        callback or check the result of the future in order to detect errors
        from ``send``: if any of the send calls failed with an irrecoverable
        error, the final ``commit_transaction()`` call will fail and raise the
        error from the last failed send.

        Raises ``IllegalStateError`` if a ``transactional.id`` has been
        configured and no transaction has been started, or when send is invoked
        after the producer has been closed.
        """
        self._check_not_closed()
        native = self._native_record(record)
        topic = record.topic()
        partition = record.partition()
        future: Future[RecordMetadata] = Future()
        # Java's FutureRecordMetadata.cancel() returns false: a running future
        # cannot be cancelled.
        future.set_running_or_notify_cancel()

        def cb(result: int, error: int) -> None:
            # Runs on the C completion thread with the GIL held. As in Java's
            # ProducerBatch.completeFutureAndFireCallbacks, the callback runs
            # before the future completes.
            metadata, exception = completion_to_python(result, error, topic, partition)
            invoke_callback(self, callback, metadata, exception)
            if exception is not None:
                future.set_exception(exception)
            else:
                future.set_result(metadata)

        # A close() that began since the check above (the serializers ran in
        # between) refuses the record as Java's RecordAccumulator.append does.
        closed_while_sending = KafkaError(message=CLOSED_WHILE_SENDING_MESSAGE)
        space: Future[None] | None = None
        with self._use(closed_while_sending) as c_producer:
            full = _lib.Producer_send(c_producer, native, cb)
            if full is None:
                raise closed_while_sending
            self._track(future)
            if full:
                # The buffer is full: wait (below, not as a use) until the send
                # task frees capacity, as Java's send() blocks on buffer.memory.
                waiter: Future[None] = Future()
                if not _lib.Producer_on_space_available(
                        c_producer, lambda: waiter.set_result(None)):
                    space = waiter
        if space is not None:
            space.result()
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
        if in_callback(self):
            _LOG.error(FLUSH_IN_CALLBACK_MESSAGE)
            raise KafkaError(message=FLUSH_IN_CALLBACK_MESSAGE)
        self._check_not_closed()
        sent = list(self._futures)
        self._drain_sync()
        run_sync(lambda cb: self._call(_lib.Producer_flush_async, cb))
        concurrent.futures.wait(sent)

    def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """Get the partition metadata for the given topic. This can be used for
        custom partitioning. This will attempt to refresh metadata until it
        finds the topic in it, or the configured ``max.block.ms`` expires
        (``TimeoutError``). Raises ``NullPointerError`` for a ``None`` topic.
        """
        if topic is None:
            raise NullPointerError(message="topic cannot be null")
        self._check_not_closed()
        list_handle, error = await_payload(
            lambda cb: self._call(_lib.Producer_partitions_for_async, topic, cb))
        if error:
            if list_handle:
                _lib.PartitionInfoList_drain(list_handle)
            raise_if_error(error)
        return [to_partition_info(t) for t in _lib.PartitionInfoList_drain(list_handle)]

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
        ``IllegalArgumentError``.
        """
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
            timeout_ms = 0
        c_producer = self._c_producer
        # Refuse further records and hand the accumulated ones to the Rust
        # producer, waiting for that within the close timeout (Java's close
        # timer covers the whole close).
        start = time.monotonic()
        _lib.Producer_shutdown(c_producer)
        try:
            if timeout_ms is None:
                self._drain_sync()
                run_sync(lambda cb: _lib.Producer_close_async(c_producer, cb))
            else:
                self._drain_sync(timeout_ms / 1000.0)
                remaining_ms = max(0, timeout_ms - int((time.monotonic() - start) * 1000))
                run_sync(lambda cb: _lib.Producer_close_with_timeout_async(
                    c_producer, remaining_ms, cb))
        finally:
            # No call that could still touch the handle is in flight once this
            # returns; then it is freed.
            self._wait_for_uses()
            _lib.Producer_destroy(c_producer)
            self._close_serializers()

    def __enter__(self) -> Producer[K, V]:
        return self

    def __exit__(self, *exc: object) -> None:
        # Closeable: flush, then close (the close runs even if the flush fails).
        try:
            if not self._closed:
                self.flush()
        finally:
            self.close()

