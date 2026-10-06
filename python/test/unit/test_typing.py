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

"""Static-typing assertions for the value types, records, serdes and clients.

Run by ``mypy --strict`` (the module is in the ``confluent_kafka`` test tree and
also imported at runtime as a smoke check). ``typing.assert_type`` fails the
type check if the inferred type of an expression is not exactly the asserted
one, which is how the ``@overload`` stubs on ``TopicIdPartition``,
``ConsumerRecords.records`` and the generics inference on the record types
(an omitted or ``None`` key or value binds ``Never``) are machine-verified.
"""

from __future__ import annotations

import sys
import uuid
from typing import TYPE_CHECKING

if sys.version_info >= (3, 11):
    from typing import assert_type
else:
    from typing_extensions import assert_type

from confluent_kafka.common import Node, TimestampType, TopicIdPartition, TopicPartition, Uuid
from confluent_kafka.common.serialization import (
    Deserializer,
    Serializer,
    bool_deserializer,
    bool_serializer,
    bytes_deserializer,
    bytes_serializer,
    float_deserializer,
    float_serializer,
    int_deserializer,
    int_serializer,
    json_deserializer,
    json_serializer,
    memoryview_deserializer,
    string_deserializer,
    string_serializer,
    uuid_deserializer,
    uuid_serializer,
)
from confluent_kafka.common import KafkaError
from confluent_kafka.common.errors import TopicAuthorizationError
from confluent_kafka.consumer import (
    ConsumerRecord,
    ConsumerRecords,
    OffsetAndMetadata,
)
from confluent_kafka.producer import ProducerRecord, RecordMetadata

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Never
    else:
        from typing_extensions import Never


def _topic_id_partition_overloads() -> None:
    uid = Uuid.random_uuid()
    a = TopicIdPartition(topic_id=uid, partition=0, topic="t")
    assert_type(a, TopicIdPartition)
    b = TopicIdPartition(topic_id=uid,
                         topic_partition=TopicPartition(topic="t", partition=0))
    assert_type(b, TopicIdPartition)
    assert_type(TopicIdPartition(topic_id=uid, partition=0, topic=None), TopicIdPartition)
    assert_type(a.topic_id(), Uuid)
    assert_type(a.topic_partition(), TopicPartition)


def _node_and_offset_and_metadata_overloads() -> None:
    # One stub per Java constructor of Node; OffsetAndMetadata's (offset) and
    # (offset, metadata) share one.
    assert_type(Node(id=1, host="h", port=9092), Node)
    assert_type(Node(id=1, host="h", port=9092, rack=None), Node)
    assert_type(Node(id=1, host="h", port=9092, rack="r", is_fenced=True), Node)
    assert_type(OffsetAndMetadata(offset=1), OffsetAndMetadata)
    assert_type(OffsetAndMetadata(offset=1, metadata="m"), OffsetAndMetadata)
    assert_type(OffsetAndMetadata(offset=1, leader_epoch=None, metadata=""), OffsetAndMetadata)


def _consumer_records_overloads() -> None:
    cr: ConsumerRecords[int, str] = ConsumerRecords.empty()
    by_partition = cr.records(partition=TopicPartition(topic="t", partition=0))
    assert_type(by_partition, "list[ConsumerRecord[int, str]]")
    by_topic = cr.records(topic="t")
    assert_type(by_topic, "list[ConsumerRecord[int, str]]")


def _record_generics_inference() -> None:
    # K/V are inferred from the constructor arguments.
    cr = ConsumerRecord(topic="t", partition=0, offset=0, key=1, value="v")
    assert_type(cr.key(), "int | None")
    assert_type(cr.value(), "str | None")

    pr = ProducerRecord(topic="t", key=b"k", value=b"v")
    assert_type(pr.key(), "bytes | None")
    assert_type(pr.value(), "bytes | None")


def _record_never_binding() -> None:
    # An omitted or None key or value binds its type variable to Never, so a
    # record needs no written type parameters.
    assert_type(ProducerRecord(topic="t", value=None), "ProducerRecord[Never, Never]")
    assert_type(ProducerRecord(topic="t", key="k", value=None), "ProducerRecord[str, Never]")
    assert_type(ProducerRecord(topic="t", value=1), "ProducerRecord[Never, int]")
    assert_type(ProducerRecord(topic="t", key=None, value=1), "ProducerRecord[Never, int]")
    assert_type(ProducerRecord(topic="t", partition=0, timestamp=5, key="k", value=1.0,
                               headers=[("h", b"v")]), "ProducerRecord[str, float]")
    # One stub per Java constructor and binding: a None key is Java's null key.
    assert_type(ProducerRecord(topic="t", partition=0, key=None, value=1),
                "ProducerRecord[Never, int]")
    assert_type(ProducerRecord(topic="t", partition=None, key="k", value=None, headers=()),
                "ProducerRecord[str, Never]")
    assert_type(ProducerRecord(topic="t", partition=0, timestamp=None, key=b"k", value=b"v"),
                "ProducerRecord[bytes, bytes]")
    assert_type(ConsumerRecord(topic="t", partition=0, offset=0, key=None, value=None),
                "ConsumerRecord[Never, Never]")
    assert_type(ConsumerRecord(topic="t", partition=0, offset=0, key=1, value=None),
                "ConsumerRecord[int, Never]")
    assert_type(ConsumerRecord(topic="t", partition=0, offset=0, key=None, value="v"),
                "ConsumerRecord[Never, str]")
    full = ConsumerRecord(topic="t", partition=0, offset=0, timestamp=0,
                          timestamp_type=TimestampType.CREATE_TIME, serialized_key_size=0,
                          serialized_value_size=1, key=None, value=b"v", headers=(),
                          leader_epoch=None, delivery_count=1)
    assert_type(full, "ConsumerRecord[Never, bytes]")
    # Covariance: a record of Never keys is a record of any key type.
    widened: ConsumerRecord[str, bytes] = full
    assert_type(widened.key(), "str | None")


def _sentinel_return_types() -> None:
    md = RecordMetadata(
        topic_partition=TopicPartition(topic="t", partition=0),
        base_offset=0, batch_index=0, timestamp=0,
        serialized_key_size=0, serialized_value_size=0)
    # A long+hasX() pair is int, not int | None.
    assert_type(md.offset(), int)
    assert_type(md.has_offset(), bool)

    om = OffsetAndMetadata(offset=1)
    assert_type(om.leader_epoch(), "int | None")


def _serde_factory_types() -> None:
    # Each factory returns a typed callable, so K/V infer from it.
    assert_type(bytes_serializer(), "Serializer[bytes]")
    assert_type(bytes_deserializer(), "Deserializer[bytes]")
    assert_type(memoryview_deserializer(), "Deserializer[memoryview]")
    assert_type(string_serializer(), "Serializer[str]")
    assert_type(string_deserializer(), "Deserializer[str]")
    assert_type(int_serializer(), "Serializer[int]")
    assert_type(int_deserializer(), "Deserializer[int]")
    assert_type(float_serializer(), "Serializer[float]")
    assert_type(float_deserializer(), "Deserializer[float]")
    assert_type(bool_serializer(), "Serializer[bool]")
    assert_type(bool_deserializer(), "Deserializer[bool]")
    assert_type(uuid_serializer(), "Serializer[uuid.UUID]")
    assert_type(uuid_deserializer(), "Deserializer[uuid.UUID]")
    # json_* are Object-typed (Java Object), so they are Serializer[Any] /
    # Deserializer[Any]; assign to a concrete-typed name to confirm they satisfy
    # the protocol (assert_type on Any is a no-op).
    _js: Serializer[object] = json_serializer()
    _jd: Deserializer[object] = json_deserializer()
    del _js, _jd

    # A serde is just a callable of the right shape — a bare function type-checks.
    def value_deser(topic: str, data: memoryview | None,
                    headers: object = None) -> str | None:
        return None if data is None else bytes(data).decode()

    d: Deserializer[str] = value_deser
    assert_type(d, "Deserializer[str]")


def _error_reexport_is_typed() -> None:
    # C19: importing a generated error class from the *package* path
    # (confluent_kafka.common.errors) must resolve to the class, not ``object``.
    # Without the generated ``common/errors/__init__.pyi`` the star re-export left
    # mypy inferring ``object`` here, which this assert_type would then fail.
    assert_type(TopicAuthorizationError, type[TopicAuthorizationError])
    err = TopicAuthorizationError(message="nope")
    assert_type(err, TopicAuthorizationError)
    # A KafkaError-typed slot accepts it (the re-export carries the base relation).
    base: KafkaError = err
    assert_type(base, KafkaError)


def test_error_reexport_derives_from_kafka_error() -> None:
    """Runtime side of C19: the package-path re-export is a real ``KafkaError``
    subclass (its static typing is enforced by ``_error_reexport_is_typed`` under
    ``mypy --strict``)."""
    assert issubclass(TopicAuthorizationError, KafkaError)
    assert isinstance(TopicAuthorizationError(message="x"), KafkaError)
def _consumer_family_types(c: object = None) -> None:
    """P5: the consumer clients' constructor binding stubs, method return types
    and overload stubs.

    Type-check-only: the body is statically analysed by mypy --strict but never
    executed at runtime (an early return guards the FFI calls)."""
    if c is None:
        return
    from typing import Any

    from confluent_kafka.common import MetricName, PartitionInfo, TopicPartition
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.serialization import (
        bytes_deserializer, int_deserializer, json_deserializer, string_deserializer,
    )
    from confluent_kafka.consumer import (
        AsyncConsumer, AsyncKafkaConsumer, AsyncMockConsumer, CloseOptions, Consumer,
        ConsumerGroupMetadata, ConsumerRecord, ConsumerRecords, KafkaConsumer, MockConsumer,
        OffsetAndMetadata, OffsetAndTimestamp, OffsetCommitCallback, SubscriptionPattern,
    )

    configs: dict[str, Any] = {"bootstrap.servers": "localhost:9092"}
    tp = TopicPartition(topic="t", partition=0)
    # An omitted deserializer binds bytes; a given one binds its type.
    assert_type(KafkaConsumer(configs=configs), "KafkaConsumer[bytes, bytes]")
    assert_type(KafkaConsumer(configs=configs, key_deserializer=string_deserializer()),
                "KafkaConsumer[str, bytes]")
    assert_type(KafkaConsumer(configs=configs, value_deserializer=int_deserializer()),
                "KafkaConsumer[bytes, int]")
    assert_type(KafkaConsumer(configs=configs, key_deserializer=string_deserializer(),
                              value_deserializer=json_deserializer()),
                "KafkaConsumer[str, Any]")
    assert_type(AsyncKafkaConsumer(configs=configs), "AsyncKafkaConsumer[bytes, bytes]")
    assert_type(AsyncKafkaConsumer(configs=configs, key_deserializer=bytes_deserializer(),
                                   value_deserializer=string_deserializer()),
                "AsyncKafkaConsumer[bytes, str]")

    kc = KafkaConsumer(configs=configs, value_deserializer=string_deserializer())
    assert_type(kc.poll(timeout=1.0), "ConsumerRecords[bytes, str]")
    assert_type(kc.committed(partitions=[tp]), "dict[TopicPartition, OffsetAndMetadata | None]")
    assert_type(kc.position(partition=tp), int)
    assert_type(kc.assignment(), "set[TopicPartition]")
    assert_type(kc.subscription(), "set[str]")
    assert_type(kc.paused(), "set[TopicPartition]")
    assert_type(kc.current_lag(topic_partition=tp), "int | None")
    assert_type(kc.list_topics(), "dict[str, list[PartitionInfo]]")
    assert_type(kc.partitions_for(topic="t"), "list[PartitionInfo]")
    assert_type(kc.beginning_offsets(partitions=[tp]), "dict[TopicPartition, int]")
    assert_type(kc.end_offsets(partitions=[tp]), "dict[TopicPartition, int]")
    assert_type(kc.offsets_for_times(timestamps_to_search={tp: 0}),
                "dict[TopicPartition, OffsetAndTimestamp | None]")
    assert_type(kc.metrics(), "dict[MetricName, Metric]")
    assert_type(kc.group_metadata(), ConsumerGroupMetadata)
    # The overload stubs accept each Java form.
    kc.subscribe(topics=["t"])
    kc.subscribe(pattern=SubscriptionPattern(pattern="t.*"))
    kc.seek(partition=tp, offset=5)
    kc.seek(partition=tp, offset_and_metadata=OffsetAndMetadata(offset=5))
    callback: OffsetCommitCallback = lambda offsets, exception: None  # noqa: E731
    kc.commit_nowait()
    kc.commit_nowait(callback=callback)
    kc.commit_nowait(offsets={tp: OffsetAndMetadata(offset=5)}, callback=callback)
    kc.commit_nowait(offsets={tp: OffsetAndMetadata(offset=5)}, callback=None)
    kc.commit()
    kc.commit(offsets={tp: OffsetAndMetadata(offset=5)})
    kc.close()
    kc.close(option=CloseOptions.timeout(1.0))
    assert_type(CloseOptions.timeout(1.0).timeout(), "float | None")
    base: Consumer[bytes, str] = kc
    assert_type(base.poll(timeout=0), "ConsumerRecords[bytes, str]")

    # Nothing binds MockConsumer's type parameters: the caller writes them, as
    # Java does (`new MockConsumer<String, String>("earliest")`).
    mc: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    mc.add_record(record=ConsumerRecord(topic="t", partition=0, offset=0, key="k", value="v"))
    assert_type(mc.poll(timeout=0), "ConsumerRecords[str, str]")
    assert_type(mc.last_poll_timeout(), "float | None")
    assert_type(mc.closed(), bool)
    mc.rebalance(new_assignment=[tp])
    mc.schedule_poll_task(task=lambda: None)

    async def _async() -> None:
        ac = AsyncKafkaConsumer(configs=configs, key_deserializer=string_deserializer())
        assert_type(await ac.poll(timeout=1.0), "ConsumerRecords[str, bytes]")
        assert_type(await ac.position(partition=tp), int)
        assert_type(ac.assignment(), "set[TopicPartition]")
        ac.commit_nowait(callback=callback)
        await ac.commit()
        await ac.seek(partition=tp, offset=5)
        await ac.close(option=CloseOptions.timeout(1.0))
        abase: AsyncConsumer[str, bytes] = ac
        assert_type(await abase.poll(timeout=0), "ConsumerRecords[str, bytes]")
        am: AsyncMockConsumer[int, int] = AsyncMockConsumer(offset_reset_strategy="latest")
        await am.rebalance(new_assignment=[tp])
        assert_type(await am.poll(timeout=0), "ConsumerRecords[int, int]")

    del _async


def _producer_family_types(p: object = None) -> None:
    """The producer clients' constructor binding stubs, ``send`` and the async
    double await.

    Type-check-only (guarded early return; never runs the FFI)."""
    if p is None:
        return
    import asyncio
    from concurrent.futures import Future
    from typing import Any

    from confluent_kafka.common import MetricName, PartitionInfo
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.producer import (
        AsyncKafkaProducer, AsyncMockProducer, Callback, KafkaProducer, MockProducer,
        Producer, ProducerRecord, RecordMetadata,
    )

    configs: dict[str, Any] = {"bootstrap.servers": "localhost:9092"}
    # An omitted serializer binds bytes; a given one binds its type.
    assert_type(KafkaProducer(configs=configs), "KafkaProducer[bytes, bytes]")
    assert_type(KafkaProducer(configs=configs, key_serializer=string_serializer()),
                "KafkaProducer[str, bytes]")
    assert_type(KafkaProducer(configs=configs, value_serializer=int_serializer()),
                "KafkaProducer[bytes, int]")
    assert_type(KafkaProducer(configs=configs, key_serializer=string_serializer(),
                              value_serializer=json_serializer()),
                "KafkaProducer[str, Any]")
    assert_type(AsyncKafkaProducer(configs=configs), "AsyncKafkaProducer[bytes, bytes]")
    assert_type(AsyncKafkaProducer(configs=configs, key_serializer=string_serializer(),
                                   value_serializer=float_serializer()),
                "AsyncKafkaProducer[str, float]")

    # MockProducer's forms: (cluster, auto_complete, partitioner, key_serializer,
    # value_serializer), (auto_complete, partitioner, key_serializer,
    # value_serializer) and (); a serializer left out binds bytes.
    assert_type(MockProducer(), "MockProducer[bytes, bytes]")
    assert_type(MockProducer(cluster=object(), auto_complete=True, partitioner=None),
                "MockProducer[bytes, bytes]")
    assert_type(MockProducer(cluster=object(), auto_complete=True, partitioner=None,
                             key_serializer=string_serializer()), "MockProducer[str, bytes]")
    assert_type(MockProducer(cluster=object(), auto_complete=True, partitioner=None,
                             value_serializer=string_serializer()), "MockProducer[bytes, str]")
    assert_type(MockProducer(cluster=object(), auto_complete=True, partitioner=None,
                             key_serializer=string_serializer(),
                             value_serializer=int_serializer()), "MockProducer[str, int]")
    assert_type(MockProducer(auto_complete=True, partitioner=None,
                             key_serializer=string_serializer(),
                             value_serializer=int_serializer()), "MockProducer[str, int]")
    assert_type(MockProducer(auto_complete=True, partitioner=None), "MockProducer[bytes, bytes]")
    assert_type(MockProducer(auto_complete=True, partitioner=None,
                             key_serializer=string_serializer()), "MockProducer[str, bytes]")
    assert_type(MockProducer(auto_complete=True, partitioner=None,
                             value_serializer=string_serializer()), "MockProducer[bytes, str]")
    assert_type(AsyncMockProducer(auto_complete=True, partitioner=None,
                                  value_serializer=string_serializer()),
                "AsyncMockProducer[bytes, str]")

    kp = KafkaProducer(configs=configs, key_serializer=string_serializer(),
                       value_serializer=string_serializer())
    callback: Callback = lambda metadata, exception: None  # noqa: E731
    # A record's omitted key binds Never, which fits any producer (covariance).
    assert_type(kp.send(record=ProducerRecord(topic="t", value="v"), callback=callback),
                "Future[RecordMetadata]")
    assert_type(kp.partitions_for(topic="t"), "list[PartitionInfo]")
    assert_type(kp.metrics(), "dict[MetricName, Metric]")
    base: Producer[str, str] = kp
    assert_type(base.send(record=ProducerRecord(topic="t", key="k", value="v")),
                "Future[RecordMetadata]")
    mp = MockProducer(auto_complete=True, partitioner=None, key_serializer=string_serializer(),
                      value_serializer=string_serializer())
    assert_type(mp.history(), "list[ProducerRecord[str, str]]")

    async def _async() -> None:
        ap = AsyncKafkaProducer(configs=configs, value_serializer=string_serializer())
        future = await ap.send(record=ProducerRecord(topic="t", value="v"))
        assert_type(future, "asyncio.Future[RecordMetadata]")
        assert_type(await future, RecordMetadata)
        assert_type(await (await ap.send(record=ProducerRecord(topic="t", value="v"))),
                    RecordMetadata)

    del _async, Future, asyncio


def test_typing_module_imports() -> None:
    """Runtime smoke check that the typing module imports cleanly. The real
    assertions above are enforced statically by mypy --strict."""
    _topic_id_partition_overloads()
    _node_and_offset_and_metadata_overloads()
    _consumer_records_overloads()
    _record_generics_inference()
    _sentinel_return_types()
    _serde_factory_types()
    _error_reexport_is_typed()
    _consumer_family_types()
    _producer_family_types()
