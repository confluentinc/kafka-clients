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
``java_forms`` (Signatures): ``()`` passes ``(Cluster.empty(), false, null,
null, null)`` to the first, so a cluster alone is accepted, but
``auto_complete`` alone matches none of them.
"""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, TypeVar, overload

from confluent_kafka._args import UNSET, Form, java_forms

from ._mock_core import MockProducerCore
from .producer import Producer
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

__all__ = ["MockProducer"]

K = TypeVar("K")
V = TypeVar("V")

_FORMS = (
    Form("cluster", "auto_complete", "partitioner", "key_serializer", "value_serializer",
         defaults={"auto_complete": False, "partitioner": None, "key_serializer": None,
                   "value_serializer": None}),
    Form("auto_complete", "partitioner", "key_serializer", "value_serializer"),
    Form(),
)


class MockProducer(MockProducerCore[K, V], Producer[K, V]):
    """A mock of the producer interface you can use for testing code that uses
    Kafka.

    By default this mock will synchronously complete each send call
    successfully. However it can be configured to allow the user to control the
    completion of the call and supply an optional error for the producer to
    raise. Sends complete, and their callbacks run, on the calling thread.

    Java: ``org.apache.kafka.clients.producer.MockProducer<K, V>``.
    """

    @overload
    def __init__(self: MockProducer[bytes, bytes], *, cluster: Cluster | None = None,
                 auto_complete: bool = False, partitioner: Partitioner | None = None) -> None: ...
    @overload
    def __init__(self: MockProducer[K, bytes], *, cluster: Cluster | None = None,
                 auto_complete: bool = False, partitioner: Partitioner | None = None,
                 key_serializer: Serializer[K]) -> None: ...
    @overload
    def __init__(self: MockProducer[bytes, V], *, cluster: Cluster | None = None,
                 auto_complete: bool = False, partitioner: Partitioner | None = None,
                 value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self, *, cluster: Cluster | None = None, auto_complete: bool = False,
                 partitioner: Partitioner | None = None, key_serializer: Serializer[K],
                 value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self: MockProducer[bytes, bytes], *, auto_complete: bool,
                 partitioner: Partitioner | None, key_serializer: None,
                 value_serializer: None) -> None: ...
    @overload
    def __init__(self: MockProducer[K, bytes], *, auto_complete: bool,
                 partitioner: Partitioner | None, key_serializer: Serializer[K],
                 value_serializer: None) -> None: ...
    @overload
    def __init__(self: MockProducer[bytes, V], *, auto_complete: bool,
                 partitioner: Partitioner | None, key_serializer: None,
                 value_serializer: Serializer[V]) -> None: ...

    @java_forms(*_FORMS)
    def __init__(self, *, cluster: Cluster | None = None, auto_complete: bool = UNSET,
                 partitioner: Partitioner | None = UNSET,
                 key_serializer: Serializer[Any] | None = UNSET,
                 value_serializer: Serializer[Any] | None = UNSET) -> None:
        """Create a mock producer: ``cluster`` holds the metadata for this
        producer (none by default); with ``auto_complete``, all requests
        complete successfully and run the callback at once, otherwise the user
        must call ``complete_next()`` or ``error_next()`` after ``send()`` to
        complete the call and resolve the returned future; ``partitioner`` is
        the partition strategy; and the key and value serializers default to
        ``bytes_serializer()``."""
        Producer.__init__(self)
        if auto_complete is UNSET:
            # MockProducer(): this(Cluster.empty(), false, null, null, null).
            auto_complete, partitioner, key_serializer, value_serializer = False, None, None, None
        self._init_mock(cluster, auto_complete, partitioner, key_serializer, value_serializer)

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

    def send(self, *, record: ProducerRecord[K, V],
             callback: Callback | None = None) -> Future[RecordMetadata]:
        """Adds the record to the list of sent records (``history()``). With
        ``auto_complete`` the returned future is complete, and the callback has
        run, when this returns. The future cannot be cancelled, as Java's."""
        future: Future[RecordMetadata] = Future()
        future.set_running_or_notify_cancel()
        result: Future[RecordMetadata] = self._send(record, callback, future)
        return result

    def flush(self) -> None:
        self._flush()

    def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        return self._partitions_for(topic)

    def metrics(self) -> dict[MetricName, Metric]:
        return self._metrics()

    def close(self, *, timeout: Duration | None = None) -> None:
        """Java's mock never reads the timeout, a negative one included."""
        self._close()

    def __enter__(self) -> MockProducer[K, V]:
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            if not self._closed:
                self.flush()
        finally:
            self.close()
