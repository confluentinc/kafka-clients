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

"""``Producer`` — the sync producer interface, non-instantiable base.

Translated from ``org.apache.kafka.clients.producer.Producer`` (Apache Kafka
4.3.1). Java's ``Producer<K, V>`` is an interface implemented by ``KafkaProducer``
and ``MockProducer``; principle 6 makes it a non-instantiable base class whose
``__init__`` guard raises ``TypeError`` naming the concrete classes. Every
producer method lives here; the concrete classes add only a constructor (and, for
``MockProducer``, the mock helpers).

The async peer ``AsyncProducer`` lives in ``async_producer.py`` (rule 6: async
classes live in the same module family, ``Async``-prefixed).
"""

from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import TYPE_CHECKING, Callable, Generic, TypeVar, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common.errors import from_ffi_error

from ._base import (
    _ProducerState,
    _offsets_to_spec,
    _to_metrics_map,
    _to_partition_info,
)
from ._send import _completion_to_python, _invoke_on_delivery
from .producer_record import ProducerRecord
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import (
        MetricName,
        PartitionInfo,
        TopicPartition,
        Uuid,
    )
    from confluent_kafka.common.metric import KafkaMetric, Metric
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from ._send import DeliveryCallback

K = TypeVar("K")
V = TypeVar("V")

# The subclasses a user should construct instead of the abstract base.
_CONCRETE = "KafkaProducer or MockProducer"


class Producer(Generic[K, V], _ProducerState):
    """Non-instantiable base for the sync producer family (Java: interface
    ``Producer<K, V>``). Every producer method lives here.

    ``KafkaProducer`` and ``MockProducer`` inherit and add a constructor.
    Instantiating ``Producer`` directly raises ``TypeError`` (principle 6)."""

    def __init__(self) -> None:
        if type(self) is Producer:
            raise TypeError(
                f"Producer is not instantiable; use {_CONCRETE}")
        _ProducerState.__init__(self)

    # ---- publish ------------------------------------------------------------
    def send(self, *, record: ProducerRecord[K, V],
             on_delivery: DeliveryCallback | None = None
             ) -> Future[RecordMetadata]:
        """Send a record; returns a ``Future`` resolving to its ``RecordMetadata``.

        Java: ``Producer.send(record)`` / ``send(record, Callback)``. The
        serializers run eagerly on the caller's thread (spec §5.4); the returned
        future resolves with the broker acknowledgement.

        ``on_delivery(metadata, exception)`` is Java's ``Callback`` — exactly one
        argument is meaningful (``metadata`` on success, ``exception`` on
        failure). It runs on the producer's **background completion thread**
        (spec §7.1 / D25 D), never on the caller's; exceptions it raises are
        logged and swallowed."""
        self._check_not_closed()
        native = self._native_record(record)
        ret: Future[RecordMetadata] = Future()

        def cb(result: int, error: int) -> None:
            # Runs on the C completion thread with the GIL held.
            metadata, exception = _completion_to_python(result, error)
            if not ret.cancelled() and not ret.done():
                if exception is not None:
                    ret.set_exception(exception)
                elif metadata is not None:
                    ret.set_result(metadata)
            _invoke_on_delivery(on_delivery, metadata, exception)

        full = _lib.Producer_send(self._c_producer, native, cb)
        fut = self._add_future(ret)
        if full:
            # Buffer full: block until the send task frees capacity, bounding
            # accumulation — Java's send() blocks on buffer.memory here.
            space: Future[None] = Future()
            if not _lib.Producer_on_space_available(
                    self._c_producer, lambda: space.set_result(None)):
                space.result()
        return fut

    def flush(self) -> None:
        """Flush all pending records (Java ``flush()``)."""
        self._check_not_closed()
        self._run_sync(
            lambda cb: _lib.Producer_flush_async(self._c_producer, cb))

    # ---- transactions (no timeout: Java takes none — D26 round 2) -----------
    def init_transactions(self) -> None:
        """Java ``initTransactions()``."""
        self._check_not_closed()
        self._run_sync(
            lambda cb: _lib.Producer_init_transactions_async(
                self._c_producer, cb))

    def begin_transaction(self) -> None:
        """Java ``beginTransaction()``."""
        self._check_not_closed()
        self._run_sync(
            lambda cb: _lib.Producer_begin_transaction_async(
                self._c_producer, cb))

    def send_offsets_to_transaction(
            self, *,
            offsets: dict[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        """Java ``sendOffsetsToTransaction(offsets, groupMetadata)``."""
        self._check_not_closed()
        spec = _offsets_to_spec(offsets)
        gm = (group_metadata.group_id(), group_metadata.generation_id(),
              group_metadata.member_id(), group_metadata.group_instance_id())
        self._run_sync(
            lambda cb: _lib.Producer_send_offsets_to_transaction_fields_async(
                self._c_producer, spec, gm, cb))

    def commit_transaction(self) -> None:
        """Java ``commitTransaction()``."""
        self._check_not_closed()
        self._run_sync(
            lambda cb: _lib.Producer_commit_transaction_async(
                self._c_producer, cb))

    def abort_transaction(self) -> None:
        """Java ``abortTransaction()``."""
        self._check_not_closed()
        self._run_sync(
            lambda cb: _lib.Producer_abort_transaction_async(
                self._c_producer, cb))

    # ---- metadata & observability -------------------------------------------
    def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """Java ``partitionsFor(topic)`` — partition metadata for ``topic``."""
        self._check_not_closed()
        raw = self._run_sync_partitions(
            lambda cb: _lib.Producer_partitions_for_async(
                self._c_producer, topic, cb))
        return [_to_partition_info(t) for t in raw]

    def metrics(self) -> dict[MetricName, Metric]:
        """Java ``metrics()`` — a point-in-time snapshot. Does not block, so it
        is a plain sync method on both the sync and async producers."""
        self._check_not_closed()
        return _to_metrics_map(_lib.Producer_metrics(self._c_producer))

    def register_metric_for_subscription(self, *, metric: KafkaMetric) -> None:
        """Java ``registerMetricForSubscription(metric)``.

        The Rust core's ``Producer`` trait does not expose metric subscription
        (KIP-714 telemetry), so this raises the mapped Java error (rule 10);
        the gap is logged in the clarifications file."""
        self._check_not_closed()
        raise self._telemetry_unsupported("registerMetricForSubscription")

    def unregister_metric_from_subscription(
            self, *, metric: KafkaMetric) -> None:
        """Java ``unregisterMetricFromSubscription(metric)`` — see
        :meth:`register_metric_for_subscription` for the core-gap note."""
        self._check_not_closed()
        raise self._telemetry_unsupported("unregisterMetricFromSubscription")

    def client_instance_id(self, *, timeout: Duration | None = None) -> Uuid:
        """Java ``clientInstanceId(timeout)`` — the client's telemetry instance
        id (KIP-714).

        A negative ``timeout`` raises ``IllegalArgumentError`` with Java's exact
        message (validated before any I/O). The Rust core does not implement
        client telemetry, so a valid call raises the mapped Java error (rule
        10); the gap is logged in the clarifications file."""
        self._check_not_closed()
        # Java's exact message for a negative clientInstanceId timeout
        # (KafkaProducerTest.testClientInstanceIdInvalidTimeout).
        if timeout is not None and _timeout_seconds(timeout) < 0:
            raise IllegalArgumentError("The timeout cannot be negative.")
        raise self._telemetry_unsupported("clientInstanceId")

    @staticmethod
    def _telemetry_unsupported(method: str) -> BaseException:
        from confluent_kafka.common.errors import KafkaError as _KafkaError
        return _KafkaError(
            f"{method} is not supported: the Rust core does not implement "
            f"client telemetry (KIP-714)")

    # ---- lifecycle ----------------------------------------------------------
    def close(self, *, timeout: Duration | None = None) -> None:
        """Java ``close()`` / ``close(Duration)`` — idempotent; use after close
        raises ``IllegalStateError`` (spec §5.6). A negative ``timeout`` raises
        ``IllegalArgumentError``."""
        _validate_timeout(timeout)
        if self._closed:
            return
        self._closed = True
        self._cancel()
        _lib.Producer_shutdown(self._c_producer)
        timeout_ms = _timeout_to_ms(timeout)
        self._run_sync(
            lambda cb: _lib.Producer_close_async(
                self._c_producer, cb, timeout_ms))
        _lib.Producer_destroy(self._c_producer)

    def _cancel(self) -> None:
        while len(self.futures) > 0:
            for future in list(self.futures):
                self._remove_future(future)
                future.cancel()  # type: ignore[attr-defined]

    def __enter__(self) -> Producer[K, V]:
        return self

    def __exit__(self, *exc: object) -> None:
        # Java's try-with-resources / spec §5.6: flush then close.
        if not self._closed:
            self.flush()
        self.close()

    # ---- sync completion primitives -----------------------------------------
    def _await_payload(
            self, submit: Callable[[Callable[..., None]], None]
    ) -> tuple[object, ...]:
        """Submit an async FFI op and wait on an interruptible event for the
        completion payload.

        The calling thread never parks inside a native ``block_on`` — it waits on
        a ``threading.Event`` (which releases the GIL so the producer's
        dispatcher thread can run the completion callback)."""
        box: dict[str, tuple[object, ...]] = {}
        done = threading.Event()

        def cb(*payload: object) -> None:
            box["payload"] = payload
            done.set()

        submit(cb)
        done.wait()
        return box["payload"]

    def _run_sync(
            self, submit: Callable[[Callable[..., None]], None]) -> None:
        """A void async FFI op; raises the typed error on a completion error."""
        (error,) = self._await_payload(submit)
        if error:
            raise from_ffi_error(cast(int, error))

    def _run_sync_partitions(
            self, submit: Callable[[Callable[..., None]], None]
    ) -> list[tuple]:  # type: ignore[type-arg]
        """A ``partitions_for`` async FFI op; drains the PartitionInfoList and
        returns its raw tuples (or raises the typed error)."""
        list_handle, error = self._await_payload(submit)
        if error:
            if list_handle:
                _lib.PartitionInfoList_drain(list_handle)
            raise from_ffi_error(cast(int, error))
        raw: list[tuple] = _lib.PartitionInfoList_drain(list_handle)  # type: ignore[type-arg]
        return raw


def _timeout_seconds(timeout: Duration) -> float:
    from datetime import timedelta
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return timeout


def _validate_timeout(timeout: Duration | None) -> None:
    """Java: a negative ``Duration`` raises ``IllegalArgumentException``
    (D7 addendum / D25 A). ``None`` is allowed (default api timeout)."""
    if timeout is not None and _timeout_seconds(timeout) < 0:
        raise IllegalArgumentError("The timeout cannot be negative.")


def _timeout_to_ms(timeout: Duration | None) -> int:
    """Milliseconds for the FFI close, or ``-1`` for the default (no timeout)."""
    if timeout is None:
        return -1
    return int(_timeout_seconds(timeout) * 1000)
