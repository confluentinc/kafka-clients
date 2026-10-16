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
from datetime import timedelta
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
    # The Duration overloads: an optional timeout, seconds or a timedelta.
    assert_type(kc.commit(timeout=1.0), None)
    assert_type(kc.commit(offsets={}, timeout=timedelta(seconds=1)), None)
    assert_type(kc.position(partition=tp, timeout=1.0), int)
    assert_type(kc.committed(partitions=[tp], timeout=1.0),
                "dict[TopicPartition, OffsetAndMetadata | None]")
    assert_type(kc.partitions_for(topic="t", timeout=1.0), "list[PartitionInfo]")
    assert_type(kc.list_topics(timeout=1.0), "dict[str, list[PartitionInfo]]")
    assert_type(kc.offsets_for_times(timestamps_to_search={tp: 0}, timeout=1.0),
                "dict[TopicPartition, OffsetAndTimestamp | None]")
    assert_type(kc.beginning_offsets(partitions=[tp], timeout=1.0), "dict[TopicPartition, int]")
    assert_type(kc.end_offsets(partitions=[tp], timeout=None), "dict[TopicPartition, int]")
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

    async def async_callback(offsets: dict[TopicPartition, OffsetAndMetadata] | None,
                             exception: Exception | None) -> None:
        pass

    # Only an async consumer awaits an `async def` commit callback; the sync
    # client rejects one (strict mode's warn_unused_ignores checks the ignores).
    kc.commit_nowait(callback=async_callback)  # type: ignore[arg-type]
    kc.commit_nowait(offsets={tp: OffsetAndMetadata(offset=5)},
                     callback=async_callback)  # type: ignore[arg-type]
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
    mc.lose_partitions(partitions_lost=[tp])
    mc.schedule_poll_task(task=lambda: None)

    async def _async() -> None:
        ac = AsyncKafkaConsumer(configs=configs, key_deserializer=string_deserializer())
        assert_type(await ac.poll(timeout=1.0), "ConsumerRecords[str, bytes]")
        assert_type(await ac.position(partition=tp), int)
        assert_type(ac.assignment(), "set[TopicPartition]")
        assert_type(ac.current_lag(topic_partition=tp), "int | None")
        # commit_nowait is async def on the async peers: awaited, it yields None.
        assert_type(await ac.commit_nowait(), None)
        assert_type(await ac.commit_nowait(callback=callback), None)
        assert_type(await ac.commit_nowait(callback=async_callback), None)
        assert_type(await ac.commit_nowait(offsets={tp: OffsetAndMetadata(offset=5)},
                                           callback=async_callback), None)
        assert_type(await ac.commit_nowait(offsets={tp: OffsetAndMetadata(offset=5)},
                                           callback=None), None)
        await ac.commit()
        await ac.seek(partition=tp, offset=5)
        await ac.close(option=CloseOptions.timeout(1.0))
        abase: AsyncConsumer[str, bytes] = ac
        assert_type(await abase.poll(timeout=0), "ConsumerRecords[str, bytes]")
        am: AsyncMockConsumer[int, int] = AsyncMockConsumer(offset_reset_strategy="latest")
        await am.rebalance(new_assignment=[tp])
        assert_type(await am.commit_nowait(callback=async_callback), None)
        assert_type(await am.commit_nowait(offsets={tp: OffsetAndMetadata(offset=5)},
                                           callback=async_callback), None)
        assert_type(await abase.commit_nowait(callback=async_callback), None)
        await am.lose_partitions(partitions_lost=[tp])
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

        async def async_callback(metadata: RecordMetadata,
                                 exception: Exception | None) -> None:
            del metadata, exception
        # Only the asyncio producers await an `async def` send callback; the
        # sync producers take the plain Callback alias.
        assert_type(await ap.send(record=ProducerRecord(topic="t", value="v"),
                                  callback=async_callback),
                    "asyncio.Future[RecordMetadata]")
        await ap.send(record=ProducerRecord(topic="t", value="v"), callback=callback)
        amp = AsyncMockProducer(auto_complete=True, partitioner=None)
        await amp.send(record=ProducerRecord(topic="t", value=b"v"), callback=async_callback)
        kp.send(record=ProducerRecord(topic="t", value="v"),
                callback=async_callback)  # type: ignore[arg-type]

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


def _admin_reference_set_types(c: object = None) -> None:
    """The admin reference set: the Options setters return the subclass
    (``typing.Self`` / re-declared), the keyword constructors, and the result
    classes' futures per client kind, bound from the futures a public constructor
    is given (no code writes the kind marker).

    Type-check-only: the body is statically analysed by mypy --strict but never
    executed at runtime."""
    if c is None:
        return
    import asyncio
    import concurrent.futures

    from confluent_kafka.admin.create_topics_options import CreateTopicsOptions
    from confluent_kafka.admin.delete_records_options import DeleteRecordsOptions
    from confluent_kafka.admin.describe_features_options import DescribeFeaturesOptions
    from confluent_kafka.admin.list_offsets_result import ListOffsetsResult

    # AbstractOptions.timeout_ms returns Self; a re-declaration its own class.
    assert_type(DeleteRecordsOptions().timeout_ms(timeout_ms=1), DeleteRecordsOptions)
    assert_type(DeleteRecordsOptions().timeout_ms(), "int | None")
    assert_type(CreateTopicsOptions(timeout_ms=1, validate_only=True)
                .timeout_ms(timeout_ms=None).validate_only(validate_only=False),
                CreateTopicsOptions)
    assert_type(DescribeFeaturesOptions(node_id=1).node_id(), "int | None")
    assert_type(DescribeFeaturesOptions().node_id(node_id=2), DescribeFeaturesOptions)

    Info = ListOffsetsResult.ListOffsetsResultInfo
    tp = TopicPartition(topic="t", partition=0)
    sync_future: concurrent.futures.Future[ListOffsetsResult.ListOffsetsResultInfo] = (
        concurrent.futures.Future())
    sync_result = ListOffsetsResult(futures={tp: sync_future})
    assert_type(sync_result.partition_result(partition=tp),
                "concurrent.futures.Future[ListOffsetsResult.ListOffsetsResultInfo]")
    assert_type(sync_result.all(),
                "concurrent.futures.Future[dict[TopicPartition, ListOffsetsResult.ListOffsetsResultInfo]]")

    async def _async() -> None:
        async_future: asyncio.Future[ListOffsetsResult.ListOffsetsResultInfo] = (
            asyncio.get_running_loop().create_future())
        async_result = ListOffsetsResult(futures={tp: async_future})
        assert_type(async_result.all(),
                    "asyncio.Future[dict[TopicPartition, ListOffsetsResult.ListOffsetsResultInfo]]")
        assert_type((await async_result.partition_result(partition=tp)).offset(), int)

    del _async, Info


def test_admin_reference_set_typing_imports() -> None:
    """Runtime smoke check of the admin reference set's typing function."""
    _admin_reference_set_types()


def _admin_client_types(c: object = None) -> None:
    """The admin clients: ``Admin.create`` returns ``Admin``, ``AsyncAdmin.create``
    its async peer, and a result's futures follow the client that returned it, the
    kind bound without any written type argument.

    Type-check-only: the body is statically analysed by mypy --strict but never
    executed at runtime."""
    if c is None:
        return
    import asyncio
    import concurrent.futures

    from confluent_kafka.admin import Admin, AsyncAdmin, NewTopic, TopicListing

    admin = Admin.create(configs={"bootstrap.servers": "b:9092"})
    assert_type(admin, Admin)
    created = admin.create_topics(new_topics=[NewTopic(name="t", num_partitions=1,
                                                       replication_factor=1)])
    assert_type(created.all(), "concurrent.futures.Future[None]")
    assert_type(created.values(), "dict[str, concurrent.futures.Future[None]]")
    assert_type(admin.list_topics().names(), "concurrent.futures.Future[set[str]]")
    with admin as same:
        assert_type(same, Admin)

    async def _async() -> None:
        async_admin = AsyncAdmin.create(configs={"bootstrap.servers": "b:9092"})
        assert_type(async_admin, AsyncAdmin)
        async_created = await async_admin.create_topics(new_topics=[])
        assert_type(async_created.all(), "asyncio.Future[None]")
        listings = await (await async_admin.list_topics()).listings()
        assert_type(listings, list[TopicListing])
        async with async_admin as same_async:
            assert_type(same_async, AsyncAdmin)

    del _async, asyncio, concurrent


def test_admin_client_typing_imports() -> None:
    """Runtime smoke check of the admin clients' typing function."""
    _admin_client_types()


# Typing assertions of the admin RPCs.
def _admin_topic_rpc_types(c: object = None) -> None:
    """The topic, cluster, config, log-dir, partition, record, election, reassignment
    and offset RPCs: each result's futures follow the client that returned it, the
    kind bound without any written type argument, and every stub of a form-checked
    method types.

    Type-check-only: the body is statically analysed by mypy --strict but never
    executed at runtime."""
    if c is None:
        return
    import asyncio
    import concurrent.futures

    from confluent_kafka.admin import (
        Admin,
        AlterConfigOp,
        AsyncAdmin,
        Config,
        ConfigEntry,
        DeletedRecords,
        DescribeReplicaLogDirsResult,
        ListConfigResourcesOptions,
        ListOffsetsResult,
        LogDirDescription,
        NewPartitionReassignment,
        NewPartitions,
        OffsetSpec,
        PartitionReassignment,
        RecordsToDelete,
        TopicDescription,
    )
    from confluent_kafka.common import (
        AclOperation,
        ElectionType,
        Node,
        TopicCollection,
        TopicPartition,
        TopicPartitionReplica,
        Uuid,
    )
    from confluent_kafka.common.config import ConfigResource

    admin = Admin.create(configs={"bootstrap.servers": "b:9092"})
    tp = TopicPartition(topic="t", partition=0)
    resource = ConfigResource(type=ConfigResource.Type.TOPIC, name="t")
    replica = TopicPartitionReplica(topic="t", partition=0, broker_id=1)
    op = AlterConfigOp(config_entry=ConfigEntry(name="k", value="v"),
                       operation_type=AlterConfigOp.OpType.SET)

    deleted = admin.delete_topics(topics=["t"])
    assert_type(deleted.all(), "concurrent.futures.Future[None]")
    by_ids = admin.delete_topics(topics=TopicCollection.of_topic_ids(topics=[]))
    assert_type(by_ids.topic_id_values(), "dict[Uuid, concurrent.futures.Future[None]] | None")
    assert_type(admin.describe_topics(topic_names=["t"]).topic_name_values(),
                "dict[str, concurrent.futures.Future[TopicDescription]] | None")
    assert_type(admin.describe_topics(
        topics=TopicCollection.of_topic_names(topics=[])).all_topic_names(),
        "concurrent.futures.Future[dict[str, TopicDescription]] | None")
    cluster = admin.describe_cluster()
    assert_type(cluster.nodes(), "concurrent.futures.Future[list[Node]]")
    assert_type(cluster.authorized_operations(),
                "concurrent.futures.Future[set[AclOperation] | None]")
    assert_type(admin.describe_configs(resources=[resource]).all(),
                "concurrent.futures.Future[dict[ConfigResource, Config]]")
    assert_type(admin.incremental_alter_configs(configs={resource: [op]}).values(),
                "dict[ConfigResource, concurrent.futures.Future[None]]")
    assert_type(admin.alter_replica_log_dirs(replica_assignment={replica: "/d"}).all(),
                "concurrent.futures.Future[None]")
    assert_type(admin.describe_log_dirs(brokers=[1]).all_descriptions(),
                "concurrent.futures.Future[dict[int, dict[str, LogDirDescription]]]")
    assert_type(admin.describe_replica_log_dirs(replicas=[replica]).values(),
                "dict[TopicPartitionReplica, "
                "concurrent.futures.Future[DescribeReplicaLogDirsResult.ReplicaLogDirInfo]]")
    assert_type(admin.create_partitions(
        new_partitions={"t": NewPartitions.increase_to(total_count=2)}).values(),
        "dict[str, concurrent.futures.Future[None]]")
    assert_type(admin.delete_records(
        records_to_delete={tp: RecordsToDelete.before_offset(offset=1)}).low_watermarks(),
        "dict[TopicPartition, concurrent.futures.Future[DeletedRecords]]")
    assert_type(admin.elect_leaders(election_type=ElectionType.PREFERRED,
                                    partitions=None).partitions(),
                "concurrent.futures.Future[dict[TopicPartition, BaseException | None]]")
    assert_type(admin.alter_partition_reassignments(
        reassignments={tp: NewPartitionReassignment(target_replicas=[1]), tp: None}).all(),
        "concurrent.futures.Future[None]")
    assert_type(admin.list_partition_reassignments().reassignments(),
                "concurrent.futures.Future[dict[TopicPartition, PartitionReassignment]]")
    assert_type(admin.list_offsets(topic_partition_offsets={tp: OffsetSpec.latest()}).all(),
                "concurrent.futures.Future[dict[TopicPartition, "
                "ListOffsetsResult.ListOffsetsResultInfo]]")
    assert_type(admin.list_config_resources().all(),
                "concurrent.futures.Future[list[ConfigResource]]")
    assert_type(admin.list_config_resources(
        config_resource_types=[ConfigResource.Type.TOPIC],
        options=ListConfigResourcesOptions()).all(),
        "concurrent.futures.Future[list[ConfigResource]]")

    async def _async() -> None:
        async_admin = AsyncAdmin.create(configs={"bootstrap.servers": "b:9092"})
        async_deleted = await async_admin.delete_topics(topics=["t"])
        assert_type(async_deleted.all(), "asyncio.Future[None]")
        async_described = await async_admin.describe_topics(
            topics=TopicCollection.of_topic_ids(topics=[Uuid.random_uuid()]))
        assert_type(async_described.all_topic_ids(),
                    "asyncio.Future[dict[Uuid, TopicDescription]] | None")
        nodes = await (await async_admin.describe_cluster()).nodes()
        assert_type(nodes, list[Node])
        assert_type((await async_admin.describe_configs(resources=[])).all(),
                    "asyncio.Future[dict[ConfigResource, Config]]")
        assert_type((await async_admin.incremental_alter_configs(configs={})).all(),
                    "asyncio.Future[None]")
        assert_type((await async_admin.alter_replica_log_dirs(replica_assignment={})).all(),
                    "asyncio.Future[None]")
        assert_type((await async_admin.describe_log_dirs(brokers=[])).all_descriptions(),
                    "asyncio.Future[dict[int, dict[str, LogDirDescription]]]")
        assert_type((await async_admin.describe_replica_log_dirs(replicas=[])).all(),
                    "asyncio.Future[dict[TopicPartitionReplica, "
                    "DescribeReplicaLogDirsResult.ReplicaLogDirInfo]]")
        assert_type((await async_admin.create_partitions(new_partitions={})).all(),
                    "asyncio.Future[None]")
        assert_type((await async_admin.delete_records(records_to_delete={})).all(),
                    "asyncio.Future[None]")
        assert_type((await async_admin.elect_leaders(election_type=ElectionType.UNCLEAN,
                                                     partitions=[tp])).all(),
                    "asyncio.Future[None]")
        assert_type((await async_admin.alter_partition_reassignments(reassignments={})).all(),
                    "asyncio.Future[None]")
        listed = await (await async_admin.list_partition_reassignments(
            partitions=[tp])).reassignments()
        assert_type(listed, dict[TopicPartition, PartitionReassignment])
        assert_type((await async_admin.list_offsets(topic_partition_offsets={})).all(),
                    "asyncio.Future[dict[TopicPartition, "
                    "ListOffsetsResult.ListOffsetsResultInfo]]")
        resources = await (await async_admin.list_config_resources()).all()
        assert_type(resources, list[ConfigResource])

    del _async, asyncio, concurrent


def test_admin_topic_rpc_typing_imports() -> None:
    """Runtime smoke check of the topic, cluster, config and offset RPCs' typing function."""
    _admin_topic_rpc_types()


def _admin_group_rpc_types(c: object = None) -> None:
    """The consumer-group RPCs: each result's futures follow the client that
    returned it, and ``list_consumer_group_offsets`` takes either of its Java
    forms, with no written type argument.

    Type-check-only: the body is statically analysed by mypy --strict but never
    executed at runtime."""
    if c is None:
        return
    import asyncio
    import concurrent.futures

    from confluent_kafka.admin import (
        Admin,
        AsyncAdmin,
        ClassicGroupDescription,
        ConsumerGroupDescription,
        GroupListing,
        ListConsumerGroupOffsetsSpec,
        MemberToRemove,
        RemoveMembersFromConsumerGroupOptions,
    )
    from confluent_kafka.consumer import OffsetAndMetadata

    admin = Admin.create(configs={"bootstrap.servers": "b:9092"})
    tp = TopicPartition(topic="t", partition=0)
    assert_type(admin.describe_consumer_groups(group_ids=["g"]).described_groups(),
                "dict[str, concurrent.futures.Future[ConsumerGroupDescription]]")
    assert_type(admin.list_consumer_group_offsets(group_id="g").all(),
                "concurrent.futures.Future[dict[str, dict[TopicPartition, "
                "OffsetAndMetadata | None]]]")
    by_spec = admin.list_consumer_group_offsets(
        group_specs={"g": ListConsumerGroupOffsetsSpec(topic_partitions=[tp])})
    assert_type(by_spec.partitions_to_offset_and_metadata(group_id="g"),
                "concurrent.futures.Future[dict[TopicPartition, OffsetAndMetadata | None]]")
    assert_type(admin.delete_consumer_groups(group_ids=["g"]).deleted_groups(),
                "dict[str, concurrent.futures.Future[None]]")
    assert_type(admin.delete_consumer_group_offsets(group_id="g", partitions={tp})
                .partition_result(partition=tp), "concurrent.futures.Future[None]")
    assert_type(admin.list_groups().valid(), "concurrent.futures.Future[list[GroupListing]]")
    options = RemoveMembersFromConsumerGroupOptions(
        members=[MemberToRemove(group_instance_id="i")])
    assert_type(admin.remove_members_from_consumer_group(group_id="g", options=options).all(),
                "concurrent.futures.Future[None]")
    assert_type(admin.alter_consumer_group_offsets(
        group_id="g", offsets={tp: OffsetAndMetadata(offset=1)}).all(),
        "concurrent.futures.Future[None]")
    assert_type(admin.describe_classic_groups(group_ids=["g"]).all(),
                "concurrent.futures.Future[dict[str, ClassicGroupDescription]]")

    async def _async() -> None:
        async_admin = AsyncAdmin.create(configs={"bootstrap.servers": "b:9092"})
        described = await async_admin.describe_consumer_groups(group_ids=["g"])
        assert_type(described.all(), "asyncio.Future[dict[str, ConsumerGroupDescription]]")
        offsets = await (await async_admin.list_consumer_group_offsets(
            group_id="g")).partitions_to_offset_and_metadata()
        assert_type(offsets, dict[TopicPartition, OffsetAndMetadata | None])
        assert_type(await (await async_admin.list_groups()).errors(), list[BaseException])
        removed = await async_admin.remove_members_from_consumer_group(group_id="g",
                                                                       options=options)
        assert_type(removed.member_result(member=MemberToRemove(group_instance_id="i")),
                    "asyncio.Future[None]")
        altered = await async_admin.alter_consumer_group_offsets(group_id="g", offsets={})
        assert_type(altered.partition_result(partition=tp), "asyncio.Future[None]")
        deleted = await async_admin.delete_consumer_group_offsets(group_id="g", partitions=[])
        assert_type(deleted.all(), "asyncio.Future[None]")
        assert_type(await (await async_admin.delete_consumer_groups(group_ids=[])).all(), None)
        classic = await async_admin.describe_classic_groups(group_ids=["g"])
        assert_type(classic.described_groups(),
                    "dict[str, asyncio.Future[ClassicGroupDescription]]")

    del _async, asyncio, concurrent


def test_admin_group_rpc_typing_imports() -> None:
    """Runtime smoke check of the consumer-group RPCs' typing function."""
    _admin_group_rpc_types()


def _admin_security_rpc_types(c: object = None) -> None:
    """The ACL, delegation-token, quota, SCRAM, feature, producer and transaction
    RPCs: each result's futures follow the client that returned it, and
    ``describe_user_scram_credentials`` takes each of its Java forms, with no written
    type argument.

    Type-check-only: the body is statically analysed by mypy --strict but never
    executed at runtime."""
    if c is None:
        return
    import asyncio
    import concurrent.futures

    from confluent_kafka.admin import (
        AbortTransactionSpec,
        Admin,
        AsyncAdmin,
        DescribeProducersResult,
        DescribeUserScramCredentialsOptions,
        FeatureMetadata,
        FeatureUpdate,
        ScramMechanism,
        TransactionDescription,
        TransactionListing,
        UserScramCredentialDeletion,
        UserScramCredentialsDescription,
    )
    from confluent_kafka.common import (
        AclBinding,
        AclBindingFilter,
        ClientQuotaAlteration,
        ClientQuotaEntity,
        ClientQuotaFilter,
        DelegationToken,
    )

    admin = Admin.create(configs={"bootstrap.servers": "b:9092"})
    tp = TopicPartition(topic="t", partition=0)
    assert_type(admin.describe_acls(filter=AclBindingFilter.ANY).values(),
                "concurrent.futures.Future[list[AclBinding]]")
    created = admin.create_acls(acls=[])
    assert_type(created.all(), "concurrent.futures.Future[None]")
    assert_type(admin.delete_acls(filters=[AclBindingFilter.ANY]).all(),
                "concurrent.futures.Future[list[AclBinding]]")
    assert_type(admin.create_delegation_token().delegation_token(),
                "concurrent.futures.Future[DelegationToken]")
    assert_type(admin.renew_delegation_token(hmac=b"h").expiry_timestamp(),
                "concurrent.futures.Future[int]")
    assert_type(admin.expire_delegation_token(hmac=b"h").expiry_timestamp(),
                "concurrent.futures.Future[int]")
    assert_type(admin.describe_delegation_token().delegation_tokens(),
                "concurrent.futures.Future[list[DelegationToken]]")
    assert_type(admin.describe_client_quotas(filter=ClientQuotaFilter.all()).entities(),
                "concurrent.futures.Future[dict[ClientQuotaEntity, dict[str, float]]]")
    altered = admin.alter_client_quotas(entries=[ClientQuotaAlteration(
        entity=ClientQuotaEntity(entries={"user": "u"}), ops=[])])
    assert_type(altered.all(), "concurrent.futures.Future[None]")
    assert_type(admin.describe_user_scram_credentials().all(),
                "concurrent.futures.Future[dict[str, UserScramCredentialsDescription]]")
    assert_type(admin.describe_user_scram_credentials(users=["u"]).users(),
                "concurrent.futures.Future[list[str]]")
    assert_type(admin.describe_user_scram_credentials(
        users=None, options=DescribeUserScramCredentialsOptions()).description(user_name="u"),
        "concurrent.futures.Future[UserScramCredentialsDescription]")
    assert_type(admin.alter_user_scram_credentials(alterations=[UserScramCredentialDeletion(
        user="u", mechanism=ScramMechanism.SCRAM_SHA_256)]).all(),
        "concurrent.futures.Future[None]")
    assert_type(admin.describe_features().feature_metadata(),
                "concurrent.futures.Future[FeatureMetadata]")
    assert_type(admin.update_features(feature_updates={"f": FeatureUpdate(
        max_version_level=1, upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)}).all(),
        "concurrent.futures.Future[None]")
    assert_type(admin.describe_producers(partitions=[tp]).partition_result(partition=tp),
                "concurrent.futures.Future[DescribeProducersResult.PartitionProducerState]")
    assert_type(admin.describe_transactions(transactional_ids=["x"]).description(
        transactional_id="x"), "concurrent.futures.Future[TransactionDescription]")
    assert_type(admin.abort_transaction(spec=AbortTransactionSpec(
        topic_partition=tp, producer_id=1, producer_epoch=0, coordinator_epoch=0)).all(),
        "concurrent.futures.Future[None]")
    assert_type(admin.list_transactions().all(),
                "concurrent.futures.Future[list[TransactionListing]]")
    assert_type(admin.fence_producers(transactional_ids=["x"]).producer_id(
        transactional_id="x"), "concurrent.futures.Future[int]")
    assert_type(admin.force_terminate_transaction(transactional_id="x").result(),
                "concurrent.futures.Future[None]")

    async def _async() -> None:
        async_admin = AsyncAdmin.create(configs={"bootstrap.servers": "b:9092"})
        acls = await async_admin.describe_acls(filter=AclBindingFilter.ANY)
        assert_type(acls.values(), "asyncio.Future[list[AclBinding]]")
        scram = await async_admin.describe_user_scram_credentials(users=["u"])
        assert_type(await scram.users(), list[str])
        features = await async_admin.describe_features()
        assert_type(await features.feature_metadata(), FeatureMetadata)
        listed = await async_admin.list_transactions()
        assert_type(listed.all(), "asyncio.Future[list[TransactionListing]]")
        terminated = await async_admin.force_terminate_transaction(transactional_id="x")
        assert_type(terminated.result(), "asyncio.Future[None]")

    del _async, asyncio, concurrent


def test_admin_security_rpc_typing_imports() -> None:
    """Runtime smoke check of the security RPCs' typing function."""
    _admin_security_rpc_types()


# ---------------------------------------------------------------------------
# KafkaFuture / AsyncKafkaFuture (CLAUDE.md, Futures)
# ---------------------------------------------------------------------------


def _kafka_future_types() -> None:
    import concurrent.futures

    from confluent_kafka.common import KafkaFuture

    f = KafkaFuture.completed_future(value=1)
    assert_type(f, KafkaFuture[int])
    assert_type(f.get(), int)
    assert_type(f.get(timeout=1.0), int)
    assert_type(f.get(timeout=timedelta(seconds=1)), int)
    assert_type(f.get_now(value_if_absent=0), int)
    assert_type(f.then_apply(function=str), KafkaFuture[str])
    assert_type(f.when_complete(action=lambda value, error: None), KafkaFuture[int])
    assert_type(KafkaFuture.all_of(futures=(f, f.then_apply(function=str))), KafkaFuture[None])
    assert_type(f.to_completion_stage(), concurrent.futures.Future[int])
    assert_type(f.cancel(may_interrupt_if_running=False), bool)
    assert_type(f.is_done(), bool)


async def _async_kafka_future_types() -> None:
    import asyncio

    from confluent_kafka.common import AsyncKafkaFuture

    a = AsyncKafkaFuture.completed_future(value="x")
    assert_type(a, AsyncKafkaFuture[str])
    assert_type(await a.get(), str)
    assert_type(await a.get(timeout=0.5), str)
    assert_type(a.get_now(value_if_absent="y"), str)
    assert_type(a.then_apply(function=len), AsyncKafkaFuture[int])
    assert_type(AsyncKafkaFuture.all_of(futures=(a,)), AsyncKafkaFuture[None])
    assert_type(a.to_completion_stage(), asyncio.Future[str])


def test_kafka_future_typing() -> None:
    import asyncio

    _kafka_future_types()
    asyncio.run(_async_kafka_future_types())
