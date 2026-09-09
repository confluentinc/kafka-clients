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

"""``AsyncProducer`` — the asyncio-native peer of :class:`Producer`.

Same surface as ``Producer`` (spec §6.1), but the methods that block in Java are
coroutines (principle 5): ``send``, ``flush``, the four blocking transaction ops,
``partitions_for``, ``client_instance_id`` and ``close``. ``begin_transaction``,
``metrics`` and the metric-subscription hooks do not block, so they stay plain
``def``.

``send`` is a coroutine because Java's ``send()`` blocks when the buffer is full;
awaiting it suspends on capacity instead of blocking the loop, and returns an
``asyncio.Future`` for the broker acknowledgement.
"""

from __future__ import annotations

import asyncio
import threading
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
from .producer import _timeout_seconds, _timeout_to_ms, _validate_timeout
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
    from .producer_record import ProducerRecord

K = TypeVar("K")
V = TypeVar("V")

_CONCRETE = "AsyncKafkaProducer or AsyncMockProducer"


class AsyncProducer(Generic[K, V], _ProducerState):
    """Non-instantiable base for the async producer family. Methods that perform
    I/O are coroutines (principle 5). Instantiating ``AsyncProducer`` directly
    raises ``TypeError``."""

    def __init__(self) -> None:
        if type(self) is AsyncProducer:
            raise TypeError(
                f"AsyncProducer is not instantiable; use {_CONCRETE}")
        _ProducerState.__init__(self)
        # Completions buffered by the C poll task (producer thread), drained on
        # the event loop. Guarded by a lock (two threads); the critical sections
        # are tiny (append / list swap).
        self._pending: list[
            tuple[asyncio.Future[RecordMetadata],
                  DeliveryCallback | None, int, int]] = []
        self._drain_scheduled = False
        self._pending_lock = threading.Lock()

    # ---- publish ------------------------------------------------------------
    async def send(self, *, record: ProducerRecord[K, V],
                   on_delivery: DeliveryCallback | None = None
                   ) -> asyncio.Future[RecordMetadata]:
        """Send a record; returns an ``asyncio.Future`` resolving to its metadata.

        ``on_delivery`` runs on the **event loop thread** (inside the completion
        drain), not the C completion thread (spec §7.1); it must not block the
        loop. A full round trip is ``md = await (await p.send(record=rec))``."""
        self._check_not_closed()
        native = self._native_record(record)
        loop = asyncio.get_running_loop()
        ret: asyncio.Future[RecordMetadata] = loop.create_future()

        def cb(result: int, error: int) -> None:
            # Runs on the C completion thread. asyncio futures may only be
            # mutated on the loop thread, so buffer + coalesce a drain.
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

        full = _lib.Producer_send(self._c_producer, native, cb)
        self._add_future(ret)
        if full:
            space: asyncio.Future[None] = loop.create_future()

            def space_cb() -> None:
                if not loop.is_closed():
                    loop.call_soon_threadsafe(self._resolve_space, space)

            if not _lib.Producer_on_space_available(self._c_producer, space_cb):
                await space
        return ret

    @staticmethod
    def _resolve_space(space: asyncio.Future[None]) -> None:
        if not space.done():
            space.set_result(None)

    def _drain(self) -> None:
        """Resolve all buffered completions. Runs on the event loop thread."""
        with self._pending_lock:
            items = self._pending
            self._pending = []
            self._drain_scheduled = False
        for ret, on_delivery, result, error in items:
            metadata, exception = _completion_to_python(result, error)
            if not ret.cancelled() and not ret.done():
                if exception is not None:
                    ret.set_exception(exception)
                elif metadata is not None:
                    ret.set_result(metadata)
            _invoke_on_delivery(on_delivery, metadata, exception)

    async def flush(self) -> None:
        """Flush all pending records."""
        self._check_not_closed()
        await self._run_async(
            lambda cb: _lib.Producer_flush_async(self._c_producer, cb))

    # ---- transactions (no timeout: Java takes none) -------------------------
    async def init_transactions(self) -> None:
        self._check_not_closed()
        await self._run_async(
            lambda cb: _lib.Producer_init_transactions_async(
                self._c_producer, cb))

    def begin_transaction(self) -> None:
        """A state transition that does not block, so a plain ``def`` on both
        classes (principle 5). Routed through the async FFI for a uniform path."""
        self._check_not_closed()
        self._run_sync(
            lambda cb: _lib.Producer_begin_transaction_async(
                self._c_producer, cb))

    async def send_offsets_to_transaction(
            self, *,
            offsets: dict[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        self._check_not_closed()
        spec = _offsets_to_spec(offsets)
        gm = (group_metadata.group_id(), group_metadata.generation_id(),
              group_metadata.member_id(), group_metadata.group_instance_id())
        await self._run_async(
            lambda cb: _lib.Producer_send_offsets_to_transaction_fields_async(
                self._c_producer, spec, gm, cb))

    async def commit_transaction(self) -> None:
        self._check_not_closed()
        await self._run_async(
            lambda cb: _lib.Producer_commit_transaction_async(
                self._c_producer, cb))

    async def abort_transaction(self) -> None:
        self._check_not_closed()
        await self._run_async(
            lambda cb: _lib.Producer_abort_transaction_async(
                self._c_producer, cb))

    # ---- metadata & observability -------------------------------------------
    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        self._check_not_closed()
        raw = await self._run_async_partitions(
            lambda cb: _lib.Producer_partitions_for_async(
                self._c_producer, topic, cb))
        return [_to_partition_info(t) for t in raw]

    def metrics(self) -> dict[MetricName, Metric]:
        """Does not block in Java, so a plain ``def`` on the async class too."""
        self._check_not_closed()
        return _to_metrics_map(_lib.Producer_metrics(self._c_producer))

    def register_metric_for_subscription(self, *, metric: KafkaMetric) -> None:
        self._check_not_closed()
        raise _telemetry_unsupported("registerMetricForSubscription")

    def unregister_metric_from_subscription(
            self, *, metric: KafkaMetric) -> None:
        self._check_not_closed()
        raise _telemetry_unsupported("unregisterMetricFromSubscription")

    async def client_instance_id(
            self, *, timeout: Duration | None = None) -> Uuid:
        self._check_not_closed()
        if timeout is not None and _timeout_seconds(timeout) < 0:
            raise IllegalArgumentError("The timeout cannot be negative.")
        raise _telemetry_unsupported("clientInstanceId")

    # ---- lifecycle ----------------------------------------------------------
    async def close(self, *, timeout: Duration | None = None) -> None:
        _validate_timeout(timeout)
        if self._closed:
            return
        self._closed = True
        self._cancel()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _lib.Producer_shutdown, self._c_producer)
        timeout_ms = _timeout_to_ms(timeout)
        await self._run_async(
            lambda cb: _lib.Producer_close_timeout_async(
                self._c_producer, timeout_ms, cb))
        await loop.run_in_executor(None, _lib.Producer_destroy, self._c_producer)

    def _cancel(self) -> None:
        # asyncio.Future done-callbacks are scheduled, not run inline, so
        # cancel each once and clear the set ourselves.
        for future in list(self.futures):
            if not future.done():  # type: ignore[attr-defined]
                future.cancel()  # type: ignore[attr-defined]
        self.futures.clear()

    async def __aenter__(self) -> AsyncProducer[K, V]:
        return self

    async def __aexit__(self, *exc: object) -> None:
        if not self._closed:
            await self.flush()
        await self.close()

    # ---- completion primitives ----------------------------------------------
    async def _await_payload(
            self, submit: Callable[[Callable[..., None]], None],
            partitions: bool) -> tuple[object, ...]:
        """Submit an async FFI op and ``await`` its completion payload on the
        event loop. The completion callback runs on the producer's dispatcher
        thread and hops onto the loop via ``call_soon_threadsafe``; a dropped
        payload that carries a non-null error handle is freed to avoid a leak."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[object, ...]] = loop.create_future()

        def deliver(payload: tuple[object, ...]) -> None:
            if fut.cancelled() or fut.done():
                _free_payload(payload, partitions)
                return
            fut.set_result(payload)

        def cb(*payload: object) -> None:
            if loop.is_closed():
                _free_payload(payload, partitions)
                return
            loop.call_soon_threadsafe(deliver, payload)

        submit(cb)
        return await fut

    async def _run_async(
            self, submit: Callable[[Callable[..., None]], None]) -> None:
        """A void async FFI op; raises the typed error on a completion error."""
        (error,) = await self._await_payload(submit, False)
        if error:
            raise from_ffi_error(cast(int, error))

    async def _run_async_partitions(
            self, submit: Callable[[Callable[..., None]], None]
    ) -> list[tuple]:  # type: ignore[type-arg]
        """A ``partitions_for`` async FFI op; drains the PartitionInfoList."""
        list_handle, error = await self._await_payload(submit, True)
        if error:
            if list_handle:
                _lib.PartitionInfoList_drain(list_handle)
            raise from_ffi_error(cast(int, error))
        raw: list[tuple] = _lib.PartitionInfoList_drain(list_handle)  # type: ignore[type-arg]
        return raw

    def _run_sync(
            self, submit: Callable[[Callable[..., None]], None]) -> None:
        """A non-blocking op (begin_transaction) driven synchronously through the
        async FFI; waits on a ``threading.Event`` since it does not await."""
        box: dict[str, tuple[object, ...]] = {}
        done = threading.Event()

        def cb(*payload: object) -> None:
            box["payload"] = payload
            done.set()

        submit(cb)
        done.wait()
        (error,) = box["payload"]
        if error:
            raise from_ffi_error(cast(int, error))


def _free_payload(payload: tuple[object, ...], partitions: bool) -> None:
    if partitions:
        list_handle, error = payload
        if error:
            _lib.KafkaError_destroy(error)
        if list_handle:
            _lib.PartitionInfoList_drain(list_handle)
    else:
        (error,) = payload
        if error:
            _lib.KafkaError_destroy(error)


def _telemetry_unsupported(method: str) -> BaseException:
    from confluent_kafka.common.errors import KafkaError as _KafkaError
    return _KafkaError(
        f"{method} is not supported: the Rust core does not implement "
        f"client telemetry (KIP-714)")
