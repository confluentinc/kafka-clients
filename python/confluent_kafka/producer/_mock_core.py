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

"""The state and logic of Java's ``MockProducer``, shared by ``MockProducer``
and ``AsyncMockProducer`` (private).

``MockProducer`` is a direct Python translation of its Java source, not an FFI
client: Java's sends complete in the caller, which the FFI's completion
dispatcher cannot give (CLAUDE.md, Python Binding Conventions, Implementation
over the FFI). This mixin holds every ``MockProducer.java`` field and method:
the Java mock's own public methods (plain ``def`` on both classes, as none
waits), the setters of its public mutable exception fields, and the bodies of
the interface methods, which each class wraps with its own ``def`` /
``async def``. Java's ``synchronized`` methods take one reentrant lock.

Left out with what they serve (Class family, Dropped): ``disableTelemetry``,
``injectTimeoutException``, ``setClientInstanceId`` (state of
``clientInstanceId``) and ``addedMetrics`` (state of
``registerMetricForSubscription``), whose base methods are not generated.
"""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from confluent_kafka.common.errors.producer_fenced_error import ProducerFencedError
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.common.serialization import bytes_serializer
from confluent_kafka.common.topic_partition import TopicPartition
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError
from confluent_kafka.null_pointer_error import NullPointerError

from ._send import invoke_callback
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka.common import Cluster, MetricName, PartitionInfo
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.serialization import Serializer
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from .callback import Callback
    from .partitioner import Partitioner
    from .producer_record import ProducerRecord

__all__ = ["MockProducerCore"]

K = TypeVar("K")
V = TypeVar("V")

# RecordBatch.NO_TIMESTAMP; Integer.MAX_VALUE.
_NO_TIMESTAMP = -1
_INT_MAX = 0x7FFFFFFF


class _Completion:
    """Java's ``MockProducer.Completion``: a send the mock completes later."""

    __slots__ = ("_offset", "_metadata", "_future", "_callback", "_tp", "_producer")

    def __init__(self, offset: int, metadata: RecordMetadata, future: Any,
                 callback: Callback | None, tp: TopicPartition, producer: object) -> None:
        self._offset = offset
        self._metadata = metadata
        self._future = future
        self._callback = callback
        self._tp = tp
        self._producer = producer

    def complete(self, e: RuntimeError | None) -> None:
        """Java's ``complete(RuntimeException e)``: the callback gets the
        metadata, or ``RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)`` and
        ``e``; then the future completes (``result.done()``) with
        ``FutureRecordMetadata.value()``'s metadata, or ``e``."""
        if self._callback is not None:
            if e is None:
                invoke_callback(self._producer, self._callback, self._metadata, None)
            else:
                failed = RecordMetadata(topic_partition=self._tp, base_offset=-1, batch_index=-1,
                                        timestamp=_NO_TIMESTAMP, serialized_key_size=-1,
                                        serialized_value_size=-1)
                invoke_callback(self._producer, self._callback, failed, e)
        if self._future.done():
            return  # an asyncio future its awaiter cancelled
        if e is None:
            self._future.set_result(RecordMetadata(
                topic_partition=self._tp, base_offset=self._offset, batch_index=0,
                timestamp=_NO_TIMESTAMP, serialized_key_size=0, serialized_value_size=0))
        else:
            self._future.set_exception(e)


class MockProducerCore(Generic[K, V]):
    """Every field and method of Java's ``MockProducer``; see the module doc."""

    def _init_mock(self, cluster: Cluster | None, auto_complete: bool,
                   partitioner: Partitioner | None, key_serializer: Serializer[Any] | None,
                   value_serializer: Serializer[Any] | None) -> None:
        """Java's ``MockProducer(cluster, autoComplete, partitioner,
        keySerializer, valueSerializer)``. A ``None`` cluster is Java's
        ``Cluster.empty()``; a ``None`` serializer is ``bytes_serializer()``
        *(deviation: Java requires one)*."""
        self._lock = threading.RLock()
        self._cluster = cluster
        self._auto_complete = auto_complete
        self._partitioner = partitioner
        self._key_serializer: Serializer[Any] = (
            bytes_serializer() if key_serializer is None else key_serializer)
        self._value_serializer: Serializer[Any] = (
            bytes_serializer() if value_serializer is None else value_serializer)
        self._offsets: dict[TopicPartition, int] = {}
        self._sent: list[ProducerRecord[K, V]] = []
        self._uncommitted_sends: list[ProducerRecord[K, V]] = []
        self._consumer_group_offsets: list[dict[str, dict[TopicPartition, OffsetAndMetadata]]] = []
        self._uncommitted_consumer_group_offsets: dict[
            str, dict[TopicPartition, OffsetAndMetadata]] = {}
        self._completions: deque[_Completion] = deque()
        self._mock_metrics: dict[MetricName, Metric] = {}
        self._closed = False
        self._transaction_initialized = False
        self._transaction_in_flight = False
        self._transaction_committed = False
        self._transaction_aborted = False
        self._producer_fenced = False
        self._sent_offsets = False
        self._commit_count = 0
        # Java's public mutable exception fields (MockProducer.java:79-96).
        self._init_transaction_exception: RuntimeError | None = None
        self._begin_transaction_exception: RuntimeError | None = None
        self._send_offsets_to_transaction_exception: RuntimeError | None = None
        self._commit_transaction_exception: RuntimeError | None = None
        self._abort_transaction_exception: RuntimeError | None = None
        self._send_exception: RuntimeError | None = None
        self._flush_exception: RuntimeError | None = None
        self._partitions_for_exception: RuntimeError | None = None
        self._close_exception: RuntimeError | None = None

    # ---- Java's public mutable fields -> setters (Class family) --------------

    def set_init_transaction_exception(self, *,
                                       init_transaction_exception: RuntimeError | None) -> None:
        """Error to raise when ``init_transactions()`` is called *(deviation:
        Java's public field ``initTransactionException``)*."""
        self._init_transaction_exception = init_transaction_exception

    def set_begin_transaction_exception(self, *,
                                        begin_transaction_exception: RuntimeError | None) -> None:
        """Error to raise when ``begin_transaction()`` is called *(deviation:
        Java's public field ``beginTransactionException``)*."""
        self._begin_transaction_exception = begin_transaction_exception

    def set_send_offsets_to_transaction_exception(
            self, *, send_offsets_to_transaction_exception: RuntimeError | None) -> None:
        """Error to raise when ``send_offsets_to_transaction()`` is called
        *(deviation: Java's public field ``sendOffsetsToTransactionException``)*."""
        self._send_offsets_to_transaction_exception = send_offsets_to_transaction_exception

    def set_commit_transaction_exception(
            self, *, commit_transaction_exception: RuntimeError | None) -> None:
        """Error to raise when ``commit_transaction()`` is called *(deviation:
        Java's public field ``commitTransactionException``)*."""
        self._commit_transaction_exception = commit_transaction_exception

    def set_abort_transaction_exception(
            self, *, abort_transaction_exception: RuntimeError | None) -> None:
        """Error to raise when ``abort_transaction()`` is called *(deviation:
        Java's public field ``abortTransactionException``)*."""
        self._abort_transaction_exception = abort_transaction_exception

    def set_send_exception(self, *, send_exception: RuntimeError | None) -> None:
        """Error to raise when ``send()`` is called *(deviation: Java's public
        field ``sendException``)*."""
        self._send_exception = send_exception

    def set_flush_exception(self, *, flush_exception: RuntimeError | None) -> None:
        """Error to raise when ``flush()`` is called *(deviation: Java's public
        field ``flushException``)*."""
        self._flush_exception = flush_exception

    def set_partitions_for_exception(self, *,
                                     partitions_for_exception: RuntimeError | None) -> None:
        """Error to raise when ``partitions_for()`` is called *(deviation:
        Java's public field ``partitionsForException``)*."""
        self._partitions_for_exception = partitions_for_exception

    def set_close_exception(self, *, close_exception: RuntimeError | None) -> None:
        """Error to raise when ``close()`` is called *(deviation: Java's public
        field ``closeException``)*."""
        self._close_exception = close_exception

    # ---- the Java mock's own public methods ----------------------------------

    def set_mock_metrics(self, *, name: MetricName, metric: Metric) -> None:
        """Set a mock metric for testing purpose."""
        self._mock_metrics[name] = metric

    def closed(self) -> bool:
        """Checks whether this mock producer has been closed."""
        return self._closed

    def fence_producer(self) -> None:
        """Fences this mock producer, causing it to raise
        ``ProducerFencedError`` on subsequent transactional operations."""
        with self._lock:
            self._verify_not_closed()
            self._verify_not_fenced()
            self._verify_transactions_initialized()
            self._producer_fenced = True

    def transaction_initialized(self) -> bool:
        """Checks whether transactions have been initialized for this mock
        producer."""
        return self._transaction_initialized

    def transaction_in_flight(self) -> bool:
        """Checks whether a transaction is currently in progress."""
        return self._transaction_in_flight

    def transaction_committed(self) -> bool:
        """Checks whether the current transaction has been committed."""
        return self._transaction_committed

    def transaction_aborted(self) -> bool:
        """Checks whether the current transaction has been aborted."""
        return self._transaction_aborted

    def flushed(self) -> bool:
        """Checks whether all sent records have been completed (no pending
        completions)."""
        return len(self._completions) == 0

    def sent_offsets(self) -> bool:
        """Checks whether offsets have been sent to the current transaction."""
        return self._sent_offsets

    def commit_count(self) -> int:
        """Gets the total number of transactions committed by this mock
        producer."""
        return self._commit_count

    def history(self) -> list[ProducerRecord[K, V]]:
        """Get the list of sent records since the last call to ``clear()``."""
        with self._lock:
            return list(self._sent)

    def uncommitted_records(self) -> list[ProducerRecord[K, V]]:
        """Gets the list of records sent in the current transaction that have
        not yet been committed."""
        with self._lock:
            return list(self._uncommitted_sends)

    def consumer_group_offsets_history(
            self) -> list[dict[str, dict[TopicPartition, OffsetAndMetadata]]]:
        """Get the list of committed consumer group offsets since the last call
        to ``clear()``."""
        with self._lock:
            return list(self._consumer_group_offsets)

    def uncommitted_offsets(self) -> dict[str, dict[TopicPartition, OffsetAndMetadata]]:
        """Gets the consumer group offsets sent in the current transaction that
        have not yet been committed: a map of consumer group ids to their
        uncommitted offsets."""
        with self._lock:
            return self._uncommitted_consumer_group_offsets

    def clear(self) -> None:
        """Clear the stored history of sent records, consumer group offsets."""
        with self._lock:
            self._sent.clear()
            self._uncommitted_sends.clear()
            self._sent_offsets = False
            self._completions.clear()
            self._consumer_group_offsets.clear()
            self._uncommitted_consumer_group_offsets.clear()

    def complete_next(self) -> bool:
        """Complete the earliest uncompleted call successfully; returns whether
        there was an uncompleted call to complete."""
        with self._lock:
            return self._complete_earliest(None)

    def error_next(self, *, e: RuntimeError) -> bool:
        """Complete the earliest uncompleted call with the given error;
        returns whether there was an uncompleted call to complete."""
        with self._lock:
            return self._complete_earliest(e)

    # ---- the interface methods' bodies ---------------------------------------

    def _init_transactions(self) -> None:
        self._verify_not_closed()
        self._verify_not_fenced()
        if self._transaction_initialized:
            raise IllegalStateError(
                message="MockProducer has already been initialized for transactions.")
        if self._init_transaction_exception is not None:
            raise self._init_transaction_exception
        self._transaction_initialized = True
        self._transaction_in_flight = False
        self._transaction_committed = False
        self._transaction_aborted = False
        self._sent_offsets = False

    def _begin_transaction(self) -> None:
        self._verify_not_closed()
        self._verify_not_fenced()
        self._verify_transactions_initialized()
        if self._begin_transaction_exception is not None:
            raise self._begin_transaction_exception
        if self._transaction_in_flight:
            raise IllegalStateError(message="Transaction already started")
        self._transaction_in_flight = True
        self._transaction_committed = False
        self._transaction_aborted = False
        self._sent_offsets = False

    def _send_offsets_to_transaction(self, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                                     group_metadata: ConsumerGroupMetadata) -> None:
        if group_metadata is None:
            raise NullPointerError()  # Objects.requireNonNull(groupMetadata)
        self._verify_not_closed()
        self._verify_not_fenced()
        self._verify_transactions_initialized()
        self._verify_transaction_in_flight()
        if self._send_offsets_to_transaction_exception is not None:
            raise self._send_offsets_to_transaction_exception
        if len(offsets) == 0:
            return
        uncommitted = self._uncommitted_consumer_group_offsets.setdefault(
            group_metadata.group_id(), {})
        uncommitted.update(offsets)
        self._sent_offsets = True

    def _commit_transaction(self) -> None:
        self._verify_not_closed()
        self._verify_not_fenced()
        self._verify_transactions_initialized()
        self._verify_transaction_in_flight()
        if self._commit_transaction_exception is not None:
            raise self._commit_transaction_exception
        self._flush()
        self._sent.extend(self._uncommitted_sends)
        if self._uncommitted_consumer_group_offsets:
            self._consumer_group_offsets.append(self._uncommitted_consumer_group_offsets)
        self._uncommitted_sends.clear()
        self._uncommitted_consumer_group_offsets = {}
        self._transaction_committed = True
        self._transaction_aborted = False
        self._transaction_in_flight = False
        self._commit_count += 1

    def _abort_transaction(self) -> None:
        self._verify_not_closed()
        self._verify_not_fenced()
        self._verify_transactions_initialized()
        self._verify_transaction_in_flight()
        if self._abort_transaction_exception is not None:
            raise self._abort_transaction_exception
        self._flush()
        self._uncommitted_sends.clear()
        self._uncommitted_consumer_group_offsets.clear()
        self._transaction_committed = False
        self._transaction_aborted = True
        self._transaction_in_flight = False

    def _send(self, record: ProducerRecord[K, V], callback: Callback | None, future: Any) -> Any:
        """Java's ``send(record, callback)``: adds the record to the sent
        records (or to the transaction's), completing it now when
        ``auto_complete``, else on ``complete_next`` / ``error_next`` /
        ``flush``. ``future`` is the (not yet completed) future to return."""
        with self._lock:
            if self._closed:
                raise IllegalStateError(message="MockProducer is already closed.")
            if self._producer_fenced:
                raise KafkaError(message="MockProducer is fenced.",
                                 cause=ProducerFencedError(message="Fenced"))
            if self._send_exception is not None:
                raise self._send_exception
            partition = 0
            if self._partitions_for_topic(record.topic()):
                partition = self._partition(record)
            else:
                # just to raise the serializer's error if the serializers are not
                # the proper ones to serialize the key / value
                self._key_serializer(record.topic(), record.key(), ())
                self._value_serializer(record.topic(), record.value(), ())
            topic_partition = TopicPartition(topic=record.topic(), partition=partition)
            offset = self._next_offset(topic_partition)
            base_offset = max(0, offset - _INT_MAX)
            batch_index = min(_INT_MAX, offset)
            completion = _Completion(
                offset,
                RecordMetadata(topic_partition=topic_partition, base_offset=base_offset,
                               batch_index=batch_index, timestamp=_NO_TIMESTAMP,
                               serialized_key_size=0, serialized_value_size=0),
                future, callback, topic_partition, self)
            if not self._transaction_in_flight:
                self._sent.append(record)
            else:
                self._uncommitted_sends.append(record)
            if self._auto_complete:
                completion.complete(None)
            else:
                self._completions.append(completion)
            return future

    def _flush(self) -> None:
        with self._lock:
            self._verify_not_closed()
            if self._flush_exception is not None:
                raise self._flush_exception
            while self._completions:
                self._complete_earliest(None)

    def _partitions_for(self, topic: str) -> list[PartitionInfo]:
        if self._partitions_for_exception is not None:
            raise self._partitions_for_exception
        return list(self._partitions_for_topic(topic))

    def _metrics(self) -> dict[MetricName, Metric]:
        return self._mock_metrics

    def _close(self) -> None:
        """Java's ``close()`` is ``close(Duration.ofMillis(0))``, and
        ``close(Duration)`` never reads its timeout."""
        if self._close_exception is not None:
            raise self._close_exception
        self._closed = True

    # ---- private helpers (MockProducer.java) ---------------------------------

    def _verify_not_closed(self) -> None:
        if self._closed:
            raise IllegalStateError(message="MockProducer is already closed.")

    def _verify_not_fenced(self) -> None:
        if self._producer_fenced:
            raise ProducerFencedError(message="MockProducer is fenced.")

    def _verify_transactions_initialized(self) -> None:
        if not self._transaction_initialized:
            raise IllegalStateError(
                message="MockProducer hasn't been initialized for transactions.")

    def _verify_transaction_in_flight(self) -> None:
        if not self._transaction_in_flight:
            raise IllegalStateError(message="There is no open transaction.")

    def _next_offset(self, tp: TopicPartition) -> int:
        """Get the next offset for this topic/partition."""
        offset = self._offsets.get(tp)
        if offset is None:
            self._offsets[tp] = 1
            return 0
        self._offsets[tp] = offset + 1
        return offset

    def _partitions_for_topic(self, topic: str) -> list[PartitionInfo]:
        """``cluster.partitionsForTopic(topic)``; ``Cluster.empty()`` has none."""
        if self._cluster is None:
            return []
        partitions: list[PartitionInfo] = self._cluster.partitions_for_topic(topic)  # type: ignore[attr-defined]
        return partitions

    def _partition(self, record: ProducerRecord[K, V]) -> int:
        """Computes partition for given record."""
        partition = record.partition()
        topic = record.topic()
        if partition is not None:
            num_partitions = len(self._partitions_for_topic(topic))
            # they have given us a partition, use it
            if partition < 0 or partition >= num_partitions:
                raise IllegalArgumentError(
                    message=f"Invalid partition given with record: {partition} is not in the "
                    f"range [0...{num_partitions}].")
            return partition
        key_bytes = self._key_serializer(topic, record.key(), record.headers())
        value_bytes = self._value_serializer(topic, record.value(), record.headers())
        if self._partitioner is None:
            return self._partitions_for_topic(record.topic())[0].partition()
        chosen: int = self._partitioner.partition(  # type: ignore[attr-defined]
            topic, record.key(), key_bytes, record.value(), value_bytes, self._cluster)
        return chosen

    def _complete_earliest(self, e: RuntimeError | None) -> bool:
        """Java's ``errorNext(e)``, which ``completeNext()`` calls with
        ``null``."""
        if not self._completions:
            return False
        self._completions.popleft().complete(e)
        return True
