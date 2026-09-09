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

"""``MockProducer`` / ``AsyncMockProducer`` — the in-memory test double.

Translated directly from ``org.apache.kafka.clients.producer.MockProducer``
(Apache Kafka 4.3.1). Java's ``MockProducer`` is a **self-contained, synchronous**
in-memory mock: it keeps records, offsets and the transaction state machine in
its own fields and completes sends synchronously (or on ``completeNext`` /
``flush``). It has no network and no I/O thread, so — unlike the real
:class:`KafkaProducer`, which is backed by the async Rust core over the FFI — the
faithful translation is a pure-Python one that mirrors ``MockProducer.java``
method-for-method. See clarification C22.

Java's three constructors collapse to one keyword-only constructor (spec §6.1).
Java's public mutable exception FIELDS (`MockProducer.java:80-96`) become
``set_<field>_exception`` methods (rule 3.12 / D16): a bare public attribute is
not allowed on this surface.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka.common import PartitionInfo, TopicPartition
from confluent_kafka.common.errors import KafkaError
from confluent_kafka.common.errors._generated import ProducerFencedError
from confluent_kafka.common.serialization import bytes_serializer, resolve_serde

from .async_producer import AsyncProducer
from .kafka_producer import _reject_partitioner
from .producer import Producer
from .producer_record import ProducerRecord
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import MetricName
    from confluent_kafka.common.metric import KafkaMetric, Metric
    from confluent_kafka.common.serialization import Serializer
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from ._send import DeliveryCallback

K = TypeVar("K")
V = TypeVar("V")

# Java: RecordBatch.NO_TIMESTAMP and ProduceResponse.INVALID_OFFSET are both -1.
_NO_TIMESTAMP = -1


class _Completion:
    """Java ``MockProducer.Completion`` — a pending send's future + its metadata.

    On success the real metadata is delivered; on error a metadata with all
    fields ``-1`` is handed to the callback alongside the exception
    (`MockProducer.java:639-659` / ``testMetadataOnException``)."""

    __slots__ = ("_metadata", "_future", "_callback", "_error_metadata")

    def __init__(self, metadata: RecordMetadata,
                 future: Future[RecordMetadata],
                 callback: DeliveryCallback | None,
                 error_metadata: RecordMetadata) -> None:
        self._metadata = metadata
        self._future = future
        self._callback = callback
        self._error_metadata = error_metadata

    def complete(self, exception: BaseException | None) -> None:
        if exception is None:
            if not self._future.done():
                self._future.set_result(self._metadata)
            _invoke(self._callback, self._metadata, None)
        else:
            if not self._future.done():
                self._future.set_exception(exception)
            _invoke(self._callback, self._error_metadata,
                    cast("KafkaError", exception))


def _invoke(callback: DeliveryCallback | None,
            metadata: RecordMetadata | None,
            exception: KafkaError | None) -> None:
    if callback is None:
        return
    import logging
    try:
        callback(metadata, exception)
    except Exception:  # noqa: BLE001 - user callback must not escape
        logging.getLogger("confluent_kafka").exception(
            "Error in on_delivery callback")


class _MockCore(Generic[K, V]):
    """The synchronous in-memory state Java's ``MockProducer`` keeps. Every
    method is a direct translation of the matching ``MockProducer.java``
    method."""

    def __init__(self, auto_complete: bool,
                 key_serializer: Serializer[Any],
                 value_serializer: Serializer[Any]) -> None:
        self._auto_complete = auto_complete
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._sent: list[ProducerRecord[K, V]] = []
        self._uncommitted_sends: list[ProducerRecord[K, V]] = []
        self._completions: list[_Completion] = []
        self._offsets: dict[TopicPartition, int] = {}
        self._consumer_group_offsets: list[
            dict[str, dict[TopicPartition, OffsetAndMetadata]]] = []
        self._uncommitted_offsets: dict[
            str, dict[TopicPartition, OffsetAndMetadata]] = {}
        self._mock_metrics: dict[MetricName, Metric] = {}
        self._added_metrics: list[KafkaMetric] = []

        self._closed = False
        self._producer_fenced = False
        self._transaction_initialized = False
        self._transaction_in_flight = False
        self._transaction_committed = False
        self._transaction_aborted = False
        self._sent_offsets = False
        self._commit_count = 0

        # Java public mutable exception fields (MockProducer.java:80-98).
        self.init_transaction_exception: BaseException | None = None
        self.begin_transaction_exception: BaseException | None = None
        self.send_offsets_to_transaction_exception: BaseException | None = None
        self.commit_transaction_exception: BaseException | None = None
        self.abort_transaction_exception: BaseException | None = None
        self.send_exception: BaseException | None = None
        self.flush_exception: BaseException | None = None
        self.partitions_for_exception: BaseException | None = None
        self.close_exception: BaseException | None = None

        self._client_instance_id: Any | None = None
        self._telemetry_disabled = False
        self._inject_timeout_counter = 0

    # ---- verify helpers (MockProducer.java:248-282) -------------------------
    def _verify_not_closed(self) -> None:
        if self._closed:
            raise IllegalStateError("MockProducer is already closed.")

    def _verify_not_fenced(self) -> None:
        if self._producer_fenced:
            raise ProducerFencedError("MockProducer is fenced.")

    def _verify_transactions_initialized(self) -> None:
        if not self._transaction_initialized:
            raise IllegalStateError(
                "MockProducer hasn't been initialized for transactions.")

    def _verify_transaction_in_flight(self) -> None:
        if not self._transaction_in_flight:
            raise IllegalStateError("There is no open transaction.")

    # ---- transactions (MockProducer.java:154-260) ---------------------------
    def init_transactions(self) -> None:
        self._verify_not_closed()
        self._verify_not_fenced()
        if self._transaction_initialized:
            raise IllegalStateError(
                "MockProducer has already been initialized for transactions.")
        if self.init_transaction_exception is not None:
            raise self.init_transaction_exception
        self._transaction_initialized = True
        self._transaction_in_flight = False
        self._transaction_committed = False
        self._transaction_aborted = False
        self._sent_offsets = False

    def begin_transaction(self) -> None:
        self._verify_not_closed()
        self._verify_not_fenced()
        self._verify_transactions_initialized()
        if self.begin_transaction_exception is not None:
            raise self.begin_transaction_exception
        if self._transaction_in_flight:
            raise IllegalStateError("Transaction already started")
        self._transaction_in_flight = True
        self._transaction_committed = False
        self._transaction_aborted = False
        self._sent_offsets = False

    def send_offsets_to_transaction(
            self, offsets: dict[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        if group_metadata is None:
            raise IllegalArgumentError("groupMetadata must not be null")
        self._verify_not_closed()
        self._verify_not_fenced()
        self._verify_transactions_initialized()
        self._verify_transaction_in_flight()
        if self.send_offsets_to_transaction_exception is not None:
            raise self.send_offsets_to_transaction_exception
        if not offsets:
            return
        group = group_metadata.group_id()
        self._uncommitted_offsets.setdefault(group, {}).update(offsets)
        self._sent_offsets = True

    def commit_transaction(self) -> None:
        self._verify_not_closed()
        self._verify_not_fenced()
        self._verify_transactions_initialized()
        self._verify_transaction_in_flight()
        if self.commit_transaction_exception is not None:
            raise self.commit_transaction_exception
        self.flush()
        self._sent.extend(self._uncommitted_sends)
        if self._uncommitted_offsets:
            self._consumer_group_offsets.append(self._uncommitted_offsets)
        self._uncommitted_sends = []
        self._uncommitted_offsets = {}
        self._transaction_committed = True
        self._transaction_aborted = False
        self._transaction_in_flight = False
        self._commit_count += 1

    def abort_transaction(self) -> None:
        self._verify_not_closed()
        self._verify_not_fenced()
        self._verify_transactions_initialized()
        self._verify_transaction_in_flight()
        if self.abort_transaction_exception is not None:
            raise self.abort_transaction_exception
        self.flush()
        self._uncommitted_sends = []
        self._uncommitted_offsets = {}
        self._transaction_committed = False
        self._transaction_aborted = True
        self._transaction_in_flight = False

    # ---- send (MockProducer.java:287-338) -----------------------------------
    def _next_offset(self, tp: TopicPartition) -> int:
        offset = self._offsets.get(tp)
        if offset is None:
            self._offsets[tp] = 1
            return 0
        self._offsets[tp] = offset + 1
        return offset

    def send(self, record: ProducerRecord[K, V],
             callback: DeliveryCallback | None) -> Future[RecordMetadata]:
        if self._closed:
            raise IllegalStateError("MockProducer is already closed.")
        if self._producer_fenced:
            # Java: KafkaException wrapping ProducerFencedException.
            err = KafkaError("MockProducer is fenced.")
            err.__cause__ = ProducerFencedError("Fenced")
            raise err
        if self.send_exception is not None:
            raise self.send_exception
        # Java calls the serializers even on the no-partition path, so a bad
        # serializer surfaces here; the defaults accept bytes.
        topic = record.topic()
        self._serialize(topic, record.key(), self._key_serializer)
        self._serialize(topic, record.value(), self._value_serializer)

        rp = record.partition()
        partition = rp if rp is not None else 0
        tp = TopicPartition(topic=topic, partition=partition)
        future: Future[RecordMetadata] = Future()
        offset = self._next_offset(tp)
        base_offset = max(0, offset - 0x7FFFFFFF)
        batch_index = min(0x7FFFFFFF, offset)
        metadata = RecordMetadata(
            topic_partition=tp, base_offset=base_offset, batch_index=batch_index,
            timestamp=_NO_TIMESTAMP, serialized_key_size=0,
            serialized_value_size=0)
        error_metadata = RecordMetadata(
            topic_partition=tp, base_offset=-1, batch_index=0,
            timestamp=_NO_TIMESTAMP, serialized_key_size=-1,
            serialized_value_size=-1)
        completion = _Completion(metadata, future, callback, error_metadata)

        if self._transaction_in_flight:
            self._uncommitted_sends.append(record)
        else:
            self._sent.append(record)

        if self._auto_complete:
            completion.complete(None)
        else:
            self._completions.append(completion)
        return future

    @staticmethod
    def _serialize(topic: str, value: object,
                   serializer: Serializer[Any]) -> None:
        if value is None:
            return
        serializer(topic, value)

    def flush(self) -> None:
        self._verify_not_closed()
        if self.flush_exception is not None:
            raise self.flush_exception
        while self._completions:
            self.complete_next()

    def complete_next(self) -> bool:
        if not self._completions:
            return False
        self._completions.pop(0).complete(None)
        return True

    def error_next(self, exception: BaseException) -> bool:
        if not self._completions:
            return False
        self._completions.pop(0).complete(exception)
        return True

    def fence_producer(self) -> None:
        self._verify_not_closed()
        self._verify_not_fenced()
        self._verify_transactions_initialized()
        self._producer_fenced = True

    def close(self) -> None:
        if self.close_exception is not None:
            raise self.close_exception
        self._closed = True

    def clear(self) -> None:
        self._sent.clear()
        self._uncommitted_sends.clear()
        self._sent_offsets = False
        self._completions.clear()
        self._consumer_group_offsets.clear()
        self._uncommitted_offsets.clear()

    def flushed(self) -> bool:
        return len(self._completions) == 0

    def client_instance_id(self) -> Any:
        if self._telemetry_disabled:
            raise IllegalStateError()
        if self._client_instance_id is None:
            raise KafkaError("clientInstanceId not set")
        if self._inject_timeout_counter != 0:
            if self._inject_timeout_counter > 0:
                self._inject_timeout_counter -= 1
            from confluent_kafka import TimeoutError as _TimeoutError
            raise _TimeoutError(
                "TimeoutExceptions are successfully injected for test.")
        return self._client_instance_id


class _MockSurfaceMixin(Generic[K, V]):
    """The Java ``MockProducer`` observation + injection surface, shared by the
    sync and async mocks. The concrete classes supply ``_core``."""

    _core: _MockCore[K, V]

    # ---- send-completion control --------------------------------------------
    def complete_next(self) -> bool:
        """Java ``completeNext()`` — resolve the next pending send."""
        return self._core.complete_next()

    def error_next(self, *, error: BaseException) -> bool:
        """Java ``errorNext(e)`` — fail the next pending send with ``error``."""
        if error is None:
            raise IllegalArgumentError("error must not be None")
        return self._core.error_next(error)

    def flushed(self) -> bool:
        return self._core.flushed()

    def closed(self) -> bool:
        return self._core._closed

    def history(self) -> list[object]:
        return list(self._core._sent)

    def history_count(self) -> int:
        """Convenience; Java exposes ``history()`` only."""
        return len(self._core._sent)

    def clear(self) -> None:
        self._core.clear()

    # ---- transaction observation --------------------------------------------
    def fence_producer(self) -> None:
        self._core.fence_producer()

    def transaction_initialized(self) -> bool:
        return self._core._transaction_initialized

    def transaction_in_flight(self) -> bool:
        return self._core._transaction_in_flight

    def transaction_committed(self) -> bool:
        return self._core._transaction_committed

    def transaction_aborted(self) -> bool:
        return self._core._transaction_aborted

    def sent_offsets(self) -> bool:
        return self._core._sent_offsets

    def commit_count(self) -> int:
        return self._core._commit_count

    def uncommitted_records(self) -> list[object]:
        return list(self._core._uncommitted_sends)

    def uncommitted_offsets(self) -> dict:  # type: ignore[type-arg]
        return {g: dict(m) for g, m in self._core._uncommitted_offsets.items()}

    def consumer_group_offsets_history(self) -> list[dict]:  # type: ignore[type-arg]
        return [{g: dict(m) for g, m in txn.items()}
                for txn in self._core._consumer_group_offsets]

    # ---- failure injection (Java public exception fields → setters) ---------
    def set_send_exception(self, *, error: BaseException | None) -> None:
        self._core.send_exception = error

    def set_flush_exception(self, *, error: BaseException | None) -> None:
        self._core.flush_exception = error

    def set_partitions_for_exception(self, *, error: BaseException | None) -> None:
        self._core.partitions_for_exception = error

    def set_close_exception(self, *, error: BaseException | None) -> None:
        self._core.close_exception = error

    def set_init_transaction_exception(self, *, error: BaseException | None) -> None:
        self._core.init_transaction_exception = error

    def set_begin_transaction_exception(self, *, error: BaseException | None) -> None:
        self._core.begin_transaction_exception = error

    def set_send_offsets_to_transaction_exception(
            self, *, error: BaseException | None) -> None:
        self._core.send_offsets_to_transaction_exception = error

    def set_commit_transaction_exception(self, *, error: BaseException | None) -> None:
        self._core.commit_transaction_exception = error

    def set_abort_transaction_exception(self, *, error: BaseException | None) -> None:
        self._core.abort_transaction_exception = error

    # ---- telemetry / metrics hooks ------------------------------------------
    def inject_timeout_exception(self, *, counter: int) -> None:
        """Java ``injectTimeoutException(int)`` — inject timeout errors into
        subsequent ``client_instance_id`` calls (``-1`` = infinite)."""
        self._core._inject_timeout_counter = counter

    def set_client_instance_id(self, *, instance_id: object) -> None:
        self._core._client_instance_id = instance_id

    def set_mock_metrics(self, *, name: MetricName, metric: Metric) -> None:
        """Java ``setMockMetrics(name, metric)`` — seed a metric returned by
        ``metrics()``."""
        self._core._mock_metrics[name] = metric

    def disable_telemetry(self) -> None:
        self._core._telemetry_disabled = True

    def added_metrics(self) -> list[KafkaMetric]:
        return list(self._core._added_metrics)

    def metrics(self) -> dict[MetricName, Metric]:
        return dict(self._core._mock_metrics)

    def _partitions_for(self) -> list[PartitionInfo]:
        """Java ``partitionsFor(topic)`` — the mock has no ``Cluster`` on this
        surface (custom partitioner deferred, spec §6.1), so it returns an empty
        list unless a ``partitionsForException`` was injected. The sync/async
        wrappers live on the concrete classes (Java's is not blocking, but the
        real async peer's is a coroutine, so the surface must match per class)."""
        if self._core.partitions_for_exception is not None:
            raise self._core.partitions_for_exception
        return []

    def register_metric_for_subscription(self, *, metric: KafkaMetric) -> None:
        """Java ``registerMetricForSubscription(metric)`` — records the metric in
        ``addedMetrics()``."""
        self._core._added_metrics.append(metric)

    def unregister_metric_from_subscription(
            self, *, metric: KafkaMetric) -> None:
        """Java ``unregisterMetricFromSubscription(metric)``."""
        if metric in self._core._added_metrics:
            self._core._added_metrics.remove(metric)


def _resolve_mock_serdes(
        key_serializer: Serializer[Any] | None,
        value_serializer: Serializer[Any] | None,
) -> tuple[Serializer[Any], Serializer[Any]]:
    ks = resolve_serde(
        key_serializer, {}, "key.serializer",
        is_key=True, default=bytes_serializer())
    vs = resolve_serde(
        value_serializer, {}, "value.serializer",
        is_key=False, default=bytes_serializer())
    return cast("Serializer[Any]", ks), cast("Serializer[Any]", vs)


def _reject_cluster(cluster: object | None) -> None:
    """Java's ``MockProducer(Cluster, …)`` seeds a partition layout used only by
    the partitioner. With no custom-partitioner surface yet a ``Cluster`` is
    meaningless here and there is no Python ``Cluster`` type, so a non-None value
    is rejected (spec §6.1)."""
    if cluster is not None:
        raise IllegalArgumentError(
            "cluster is not supported: partition layout is only used by a "
            "custom partitioner, which is not yet available (spec §6.1)")


def _client_instance_id_with_timeout(
        core: _MockCore[Any, Any], timeout: Duration | None) -> Any:
    from .producer import _timeout_seconds
    if timeout is not None and _timeout_seconds(timeout) < 0:
        raise IllegalArgumentError("The timeout cannot be negative.")
    return core.client_instance_id()


class MockProducer(_MockSurfaceMixin[K, V], Producer[K, V]):
    """In-memory test double. No broker. Mirrors Java's ``MockProducer``."""

    def __init__(self, *, cluster: object | None = None,
                 auto_complete: bool = False,
                 partitioner: object | None = None,
                 key_serializer: Serializer[Any] = bytes_serializer(),
                 value_serializer: Serializer[Any] = bytes_serializer()) -> None:
        # The mock is a pure-Python synchronous double (C22) — no native
        # producer handle; the base guard is not invoked (its state is unused).
        _reject_partitioner(partitioner)
        _reject_cluster(cluster)
        ks, vs = _resolve_mock_serdes(key_serializer, value_serializer)
        self._core: _MockCore[K, V] = _MockCore(auto_complete, ks, vs)

    def send(self, *, record: ProducerRecord[K, V],
             on_delivery: DeliveryCallback | None = None
             ) -> Future[RecordMetadata]:
        return self._core.send(record, on_delivery)

    def flush(self) -> None:
        self._core.flush()

    def init_transactions(self) -> None:
        self._core.init_transactions()

    def begin_transaction(self) -> None:
        self._core.begin_transaction()

    def send_offsets_to_transaction(
            self, *,
            offsets: dict[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        self._core.send_offsets_to_transaction(offsets, group_metadata)

    def commit_transaction(self) -> None:
        self._core.commit_transaction()

    def abort_transaction(self) -> None:
        self._core.abort_transaction()

    def client_instance_id(self, *, timeout: Duration | None = None) -> Any:
        return _client_instance_id_with_timeout(self._core, timeout)

    def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        return self._partitions_for()

    def close(self, *, timeout: Duration | None = None) -> None:
        from .producer import _validate_timeout
        _validate_timeout(timeout)
        self._core.close()

    def __enter__(self) -> MockProducer[K, V]:
        return self

    def __exit__(self, *exc: object) -> None:
        if not self._core._closed:
            try:
                self.flush()
            except Exception:  # noqa: BLE001 - close still proceeds
                pass
        self.close()


class AsyncMockProducer(_MockSurfaceMixin[K, V], AsyncProducer[K, V]):
    """The asyncio-native in-memory test double."""

    def __init__(self, *, cluster: object | None = None,
                 auto_complete: bool = False,
                 partitioner: object | None = None,
                 key_serializer: Serializer[Any] = bytes_serializer(),
                 value_serializer: Serializer[Any] = bytes_serializer()) -> None:
        _reject_partitioner(partitioner)
        _reject_cluster(cluster)
        ks, vs = _resolve_mock_serdes(key_serializer, value_serializer)
        self._core: _MockCore[K, V] = _MockCore(auto_complete, ks, vs)

    async def send(self, *, record: ProducerRecord[K, V],
                   on_delivery: DeliveryCallback | None = None
                   ) -> asyncio.Future[RecordMetadata]:
        # The mock completes synchronously (or on complete_next/flush), producing
        # a concurrent.futures.Future; wrap it as an asyncio.Future so the async
        # surface matches the real AsyncKafkaProducer (spec §6.1).
        return asyncio.wrap_future(self._core.send(record, on_delivery))

    async def flush(self) -> None:
        self._core.flush()

    async def init_transactions(self) -> None:
        self._core.init_transactions()

    def begin_transaction(self) -> None:
        self._core.begin_transaction()

    async def send_offsets_to_transaction(
            self, *,
            offsets: dict[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        self._core.send_offsets_to_transaction(offsets, group_metadata)

    async def commit_transaction(self) -> None:
        self._core.commit_transaction()

    async def abort_transaction(self) -> None:
        self._core.abort_transaction()

    async def client_instance_id(
            self, *, timeout: Duration | None = None) -> Any:
        return _client_instance_id_with_timeout(self._core, timeout)

    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        return self._partitions_for()

    async def close(self, *, timeout: Duration | None = None) -> None:
        from .producer import _validate_timeout
        _validate_timeout(timeout)
        self._core.close()

    async def __aenter__(self) -> AsyncMockProducer[K, V]:
        return self

    async def __aexit__(self, *exc: object) -> None:
        if not self._core._closed:
            try:
                await self.flush()
            except Exception:  # noqa: BLE001
                pass
        await self.close()
