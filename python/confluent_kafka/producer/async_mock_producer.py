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
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, overload

from confluent_kafka._args import UNSET, java_forms

from ._mock_core import MockProducerCore
from .async_producer import AsyncProducer
from .mock_producer import _FORMS
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import Cluster, MetricName, PartitionInfo, TopicPartition
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.serialization import Serializer
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from .callback import Callback
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
    def __init__(self, *, cluster: Cluster | None = None, auto_complete: bool = UNSET,
                 partitioner: Partitioner | None = UNSET,
                 key_serializer: Serializer[Any] | None = None,
                 value_serializer: Serializer[Any] | None = None) -> None:
        """See ``MockProducer``."""
        AsyncProducer.__init__(self)
        if auto_complete is UNSET:
            # MockProducer(): this(Cluster.empty(), false, null, null, null).
            auto_complete, partitioner, key_serializer, value_serializer = False, None, None, None
        self._init_mock(cluster, auto_complete, partitioner, key_serializer, value_serializer)

    async def init_transactions(self) -> None:
        self._init_transactions()

    def begin_transaction(self) -> None:
        self._begin_transaction()

    async def send_offsets_to_transaction(
            self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        self._send_offsets_to_transaction(offsets, group_metadata)

    async def commit_transaction(self) -> None:
        self._commit_transaction()

    async def abort_transaction(self) -> None:
        self._abort_transaction()

    async def send(self, *, record: ProducerRecord[K, V],
                   callback: Callback | None = None) -> asyncio.Future[RecordMetadata]:
        """See ``MockProducer.send``; the returned ``asyncio.Future`` is complete
        when this returns if ``auto_complete``."""
        future: asyncio.Future[RecordMetadata] = asyncio.get_running_loop().create_future()
        result: asyncio.Future[RecordMetadata] = self._send(record, callback, future)
        return result

    async def flush(self) -> None:
        self._flush()

    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        return self._partitions_for(topic)

    def metrics(self) -> dict[MetricName, Metric]:
        return self._metrics()

    async def close(self, *, timeout: Duration | None = None) -> None:
        """Java's mock never reads the timeout, a negative one included."""
        self._close()

    async def __aenter__(self) -> AsyncMockProducer[K, V]:
        return self

    async def __aexit__(self, *exc: object) -> None:
        try:
            if not self._closed:
                await self.flush()
        finally:
            await self.close()
