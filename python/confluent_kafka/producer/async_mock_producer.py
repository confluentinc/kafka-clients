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

"""``AsyncMockProducer``: the asyncio peer of
:class:`~confluent_kafka.producer.MockProducer`.

By rule 3, ``MockProducer``'s constructors and methods (CLAUDE.md, Python
Binding Conventions, Class family): the interface methods are ``async def``
where :class:`AsyncProducer`'s are, and the Java mock's own methods are plain
``def``, as none of them waits. Sends complete, and their callbacks run, on the
event loop that awaits ``send()``.

As on ``AsyncKafkaProducer``, a ``send()`` callback may be an ``async def``
(rule 3): run on the loop completing the record, it is awaited in a task before
the record's future completes, a later callback of the mock waiting for it, and
``flush()``, ``commit_transaction()``, ``abort_transaction()`` and ``close()``
wait for the callbacks awaited when they are called. A completion outside a
running loop (``complete_next()`` from plain code) cannot await one: its
coroutine is closed and a ``TypeError`` raised, as on ``MockProducer``.

As on ``MockProducer`` (Java's ``Completion.complete`` has no try/catch), an
exception a callback raises while the completing call runs propagates out of
that call, and the record's future never completes. An awaited ``async def``
callback, or one run in a task behind it, ends after that call returned, so
its exception (``CancelledError`` included) is a deferred error: it is kept and
raised by the next awaited call of the mock, at its start, except ``close()``,
which closes the mock first; ``flush()``, ``commit_transaction()``,
``abort_transaction()`` and ``close()`` raise it once the callbacks they wait
for ended. The record's future then never completes either, and the
completions the call had already started behind it still run, unlike Java's,
whose throwing callback stops the completing call.
"""

from __future__ import annotations

import asyncio
import inspect
from collections import deque
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, TypeVar, overload

from confluent_kafka._args import MISSING, java_forms
from confluent_kafka.common.kafka_future import _KafkaFutureImpl

from ._mock_core import MockProducerCore
from ._record_future import AsyncRecordFuture
from ._send import check_callback, run_callback
from .async_producer import AsyncProducer, _AsyncCallback, _in_async_callback, _running_loop
from .mock_producer import _FORMS
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import (
        AsyncKafkaFuture,
        Cluster,
        MetricName,
        PartitionInfo,
        TopicPartition,
    )
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.serialization import Serializer
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from .partitioner import Partitioner
    from .producer_record import ProducerRecord

__all__ = ["AsyncMockProducer"]

K = TypeVar("K")
V = TypeVar("V")


class AsyncMockProducer(MockProducerCore[K, V], AsyncProducer[K, V]):
    """The asyncio peer of ``MockProducer``, a mock of the producer interface
    you can use for testing code that uses Kafka: see ``MockProducer``."""

    @overload
    def __init__(self: AsyncMockProducer[bytes, bytes], *, cluster: Cluster,
                 auto_complete: bool, partitioner: Partitioner | None) -> None: ...
    @overload
    def __init__(self: AsyncMockProducer[K, bytes], *, cluster: Cluster, auto_complete: bool,
                 partitioner: Partitioner | None, key_serializer: Serializer[K]) -> None: ...
    @overload
    def __init__(self: AsyncMockProducer[bytes, V], *, cluster: Cluster, auto_complete: bool,
                 partitioner: Partitioner | None, value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self, *, cluster: Cluster, auto_complete: bool,
                 partitioner: Partitioner | None, key_serializer: Serializer[K],
                 value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self: AsyncMockProducer[bytes, bytes], *, auto_complete: bool,
                 partitioner: Partitioner | None) -> None: ...
    @overload
    def __init__(self: AsyncMockProducer[K, bytes], *, auto_complete: bool,
                 partitioner: Partitioner | None, key_serializer: Serializer[K]) -> None: ...
    @overload
    def __init__(self: AsyncMockProducer[bytes, V], *, auto_complete: bool,
                 partitioner: Partitioner | None, value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self, *, auto_complete: bool, partitioner: Partitioner | None,
                 key_serializer: Serializer[K], value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self: AsyncMockProducer[bytes, bytes]) -> None: ...

    @java_forms(*_FORMS)
    def __init__(self, *, cluster: Cluster = MISSING, auto_complete: bool = MISSING,
                 partitioner: Partitioner | None = MISSING,
                 key_serializer: Serializer[Any] | None = MISSING,
                 value_serializer: Serializer[Any] | None = MISSING) -> None:
        """See ``MockProducer``."""
        AsyncProducer.__init__(self)
        if auto_complete is MISSING:
            # MockProducer(): this(Cluster.empty(), false, null, null, null).
            auto_complete, partitioner = False, None
        self._init_mock(cluster, auto_complete, partitioner,
                        None if key_serializer is MISSING else key_serializer,
                        None if value_serializer is MISSING else value_serializer)
        # The last callback task of the mock: a later completion runs after it.
        self._mock_tail: asyncio.Task[None] | None = None
        # The exceptions the callback tasks raised, oldest first, that no call
        # has raised yet (see the module docstring).
        self._mock_callback_errors: deque[BaseException] = deque()

    def _fire(self, callback: Callable[..., object] | None, metadata: RecordMetadata,
              e: RuntimeError | None, future: Any, settle: Callable[[], None]) -> None:
        """Run a completion's callback, then settle its future. On a running
        loop, an ``async def`` callback is awaited in a task first, and while
        one is awaited a later completion runs after it, in a task too. An
        exception the callback raises leaves the future pending: raised here
        when the callback ran here, else kept as a deferred error (see the
        module docstring)."""
        loop = _running_loop()
        tail = self._mock_tail
        if tail is not None and (tail.done() or tail.get_loop() is not loop):
            tail = None
        if loop is None or (tail is None and callback is None):
            MockProducerCore._fire(self, callback, metadata, e, future, settle)
            return
        result: object = None
        if tail is None:
            assert callback is not None
            result = run_callback(self, callback, metadata, e)
            if not inspect.isawaitable(result):
                settle()
                return
        awaited = result if inspect.isawaitable(result) else None
        producer_id = id(self)

        async def run() -> None:
            if tail is not None:
                await asyncio.wait((tail,))
            _in_async_callback.set(_in_async_callback.get() | {producer_id})
            try:
                if awaited is not None:
                    await awaited
                elif callback is not None:
                    pending = run_callback(self, callback, metadata, e)
                    if inspect.isawaitable(pending):
                        await pending
            except BaseException as exc:  # noqa: BLE001 - raised by the next awaited call
                self._mock_callback_errors.append(exc)
                return
            settle()

        self._mock_tail = loop.create_task(run())

    async def _await_callbacks(self) -> None:
        """Wait for the callbacks the mock awaits, unless called from one,
        then raise the oldest deferred error."""
        tail = self._mock_tail
        if (tail is not None and not tail.done() and id(self) not in _in_async_callback.get()
                and tail.get_loop() is _running_loop()):
            await asyncio.wait((tail,))
        self._raise_deferred_callback_error()

    def _raise_deferred_callback_error(self) -> None:
        """Raise the oldest exception a callback task raised that no call has
        raised yet, unless called from a callback of the mock (see the module
        docstring)."""
        if self._mock_callback_errors and id(self) not in _in_async_callback.get():
            raise self._mock_callback_errors.popleft()

    async def init_transactions(self) -> None:
        self._raise_deferred_callback_error()
        self._init_transactions()

    async def begin_transaction(self) -> None:
        self._raise_deferred_callback_error()
        self._begin_transaction()

    async def send_offsets_to_transaction(
            self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        self._raise_deferred_callback_error()
        self._send_offsets_to_transaction(offsets, group_metadata)

    async def commit_transaction(self) -> None:
        self._raise_deferred_callback_error()
        self._commit_transaction()
        await self._await_callbacks()

    async def abort_transaction(self) -> None:
        self._raise_deferred_callback_error()
        self._abort_transaction()
        await self._await_callbacks()

    @overload
    async def send(self, *, record: ProducerRecord[K, V]) -> AsyncKafkaFuture[RecordMetadata]: ...
    @overload
    async def send(self, *, record: ProducerRecord[K, V], callback: _AsyncCallback | None
                   ) -> AsyncKafkaFuture[RecordMetadata]: ...
    async def send(self, *, record: ProducerRecord[K, V],
                   callback: _AsyncCallback | None = MISSING
                   ) -> AsyncKafkaFuture[RecordMetadata]:
        """See ``MockProducer.send``; the returned ``AsyncKafkaFuture`` is
        complete when this returns if ``auto_complete`` (and the callback is
        not an ``async def`` still awaited). ``cancel()`` on it returns
        ``False``, as Java's."""
        self._raise_deferred_callback_error()
        if callback is MISSING:
            callback = None  # Java's send(record) passes null
        if callback is not None:
            check_callback(callback)
        future: _KafkaFutureImpl[RecordMetadata] = _KafkaFutureImpl()
        self._send(record, callback, future)
        return AsyncRecordFuture._of_impl(future, asyncio.get_running_loop())

    async def flush(self) -> None:
        self._raise_deferred_callback_error()
        self._flush()
        await self._await_callbacks()

    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        self._raise_deferred_callback_error()
        return self._partitions_for(topic)

    def metrics(self) -> dict[MetricName, Metric]:
        return self._metrics()

    @overload
    async def close(self) -> None: ...
    @overload
    async def close(self, *, timeout: Duration) -> None: ...
    async def close(self, *, timeout: Duration = MISSING) -> None:
        """Java's mock never reads the timeout, a negative or a ``None`` one
        included."""
        self._close()
        await self._await_callbacks()

    async def __aenter__(self) -> AsyncMockProducer[K, V]:
        return self

    async def __aexit__(self, *exc: object) -> None:
        try:
            if not self._closed:
                await self.flush()
        finally:
            await self.close()
