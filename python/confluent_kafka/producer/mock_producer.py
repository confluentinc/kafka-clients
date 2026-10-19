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

"""``MockProducer``: Java's ``org.apache.kafka.clients.producer.MockProducer``.

A direct Python translation of its Java source (CLAUDE.md, Python Binding
Conventions, Implementation over the FFI), with Java's constructors and the Java
mock's methods beyond the interface (Class family); its public mutable exception
fields are ``set_<field>`` setters. It uses the ``cluster`` and ``partitioner``
it is given as Java's mock does: ``partitions_for()`` returns
``cluster.partitions_for_topic(topic)``, and ``send()`` chooses the partition
with ``partitioner.partition(topic, key, key_bytes, value, value_bytes,
cluster)``.

Java's three constructors, ``(Cluster, boolean autoComplete, Partitioner,
Serializer, Serializer)``, ``(boolean autoComplete, Partitioner, Serializer,
Serializer)`` and ``()``, are one keyword-only ``__init__`` checked by
``java_forms`` (Signatures): the given arguments must be one of them, a
serializer excepted, which may be left out (then ``bytes_serializer()``), so a
cluster alone or ``auto_complete`` alone matches none of them. ``()`` passes
``(Cluster.empty(), false, null, null, null)`` to the first.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, overload

from confluent_kafka._args import MISSING, Form, java_forms
from confluent_kafka.common.kafka_future import _KafkaFutureImpl

from ._mock_core import MockProducerCore
from ._record_future import RecordFuture
from ._send import check_callback, reject_coroutine_callback
from .producer import Producer
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import (
        Cluster,
        KafkaFuture,
        MetricName,
        PartitionInfo,
        TopicPartition,
    )
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.serialization import Serializer
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from .callback import Callback
    from .partitioner import Partitioner
    from .producer_record import ProducerRecord

__all__ = ["MockProducer"]

K = TypeVar("K")
V = TypeVar("V")

_SERDES = ("key_serializer", "value_serializer")
_FORMS = (
    Form("cluster", "auto_complete", "partitioner", "key_serializer", "value_serializer",
         defaults={"auto_complete": False, "partitioner": None, "key_serializer": None,
                   "value_serializer": None}, serdes=_SERDES),
    Form("auto_complete", "partitioner", "key_serializer", "value_serializer", serdes=_SERDES),
    Form(),
)


class MockProducer(MockProducerCore[K, V], Producer[K, V]):
    """A mock of the producer interface you can use for testing code that uses
    Kafka.

    By default this mock will synchronously complete each send call
    successfully. However it can be configured to allow the user to control the
    completion of the call and supply an optional error for the producer to
    raise. Sends complete, and their callbacks run, on the calling thread. An
    exception a callback raises propagates out of the call that completed the
    record (``send()``, ``complete_next()``, ``error_next()``, ``flush()``,
    ``commit_transaction()``, ``abort_transaction()``), whose future then never
    completes, as from Java's mock.

    Java: ``org.apache.kafka.clients.producer.MockProducer<K, V>``.
    """

    @overload
    def __init__(self: MockProducer[bytes, bytes], *, cluster: Cluster, auto_complete: bool,
                 partitioner: Partitioner | None) -> None: ...
    @overload
    def __init__(self: MockProducer[K, bytes], *, cluster: Cluster, auto_complete: bool,
                 partitioner: Partitioner | None, key_serializer: Serializer[K]) -> None: ...
    @overload
    def __init__(self: MockProducer[bytes, V], *, cluster: Cluster, auto_complete: bool,
                 partitioner: Partitioner | None, value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self, *, cluster: Cluster, auto_complete: bool,
                 partitioner: Partitioner | None, key_serializer: Serializer[K],
                 value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self: MockProducer[bytes, bytes], *, auto_complete: bool,
                 partitioner: Partitioner | None) -> None: ...
    @overload
    def __init__(self: MockProducer[K, bytes], *, auto_complete: bool,
                 partitioner: Partitioner | None, key_serializer: Serializer[K]) -> None: ...
    @overload
    def __init__(self: MockProducer[bytes, V], *, auto_complete: bool,
                 partitioner: Partitioner | None, value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self, *, auto_complete: bool, partitioner: Partitioner | None,
                 key_serializer: Serializer[K], value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self: MockProducer[bytes, bytes]) -> None: ...

    @java_forms(*_FORMS)
    def __init__(self, *, cluster: Cluster = MISSING, auto_complete: bool = MISSING,
                 partitioner: Partitioner | None = MISSING,
                 key_serializer: Serializer[Any] | None = MISSING,
                 value_serializer: Serializer[Any] | None = MISSING) -> None:
        """Create a mock producer: ``cluster`` holds the metadata for this
        producer (none by default); with ``auto_complete``, all requests
        complete successfully and run the callback at once, otherwise the user
        must call ``complete_next()`` or ``error_next()`` after ``send()`` to
        complete the call and resolve the returned future; ``partitioner`` is
        the partition strategy; and the key and value serializers default to
        ``bytes_serializer()``."""
        Producer.__init__(self)
        if auto_complete is MISSING:
            # MockProducer(): this(Cluster.empty(), false, null, null, null).
            auto_complete, partitioner = False, None
        self._init_mock(cluster, auto_complete, partitioner,
                        None if key_serializer is MISSING else key_serializer,
                        None if value_serializer is MISSING else value_serializer)

    def init_transactions(self) -> None:
        self._init_transactions()

    def begin_transaction(self) -> None:
        self._begin_transaction()

    def send_offsets_to_transaction(
            self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
            group_metadata: ConsumerGroupMetadata) -> None:
        self._send_offsets_to_transaction(offsets, group_metadata)

    def commit_transaction(self) -> None:
        self._commit_transaction()

    def abort_transaction(self) -> None:
        self._abort_transaction()

    @overload
    def send(self, *, record: ProducerRecord[K, V]) -> KafkaFuture[RecordMetadata]: ...
    @overload
    def send(self, *, record: ProducerRecord[K, V], callback: Callback | None
             ) -> KafkaFuture[RecordMetadata]: ...
    def send(self, *, record: ProducerRecord[K, V],
             callback: Callback | None = MISSING) -> KafkaFuture[RecordMetadata]:
        """Adds the record to the list of sent records (``history()``). With
        ``auto_complete`` the returned future is complete, and the callback has
        run, when this returns. ``cancel()`` on the future returns ``False``,
        as Java's; its ``then_apply`` functions and ``when_complete`` actions run
        in the call that completes the record. A coroutine function given as
        ``callback`` raises ``TypeError`` (``AsyncMockProducer`` awaits it)."""
        if callback is MISSING:
            callback = None  # Java's send(record) passes null
        if callback is not None:
            reject_coroutine_callback(callback)
            check_callback(callback)
        future: _KafkaFutureImpl[RecordMetadata] = _KafkaFutureImpl()
        self._send(record, callback, future)
        return RecordFuture._of_impl(future)

    def flush(self) -> None:
        self._flush()

    def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        return self._partitions_for(topic)

    def metrics(self) -> dict[MetricName, Metric]:
        return self._metrics()

    @overload
    def close(self) -> None: ...
    @overload
    def close(self, *, timeout: Duration) -> None: ...
    def close(self, *, timeout: Duration = MISSING) -> None:
        """Java's mock never reads the timeout, a negative or a ``None`` one
        included."""
        self._close()

    def __enter__(self) -> MockProducer[K, V]:
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            if not self._closed:
                self.flush()
        finally:
            self.close()
