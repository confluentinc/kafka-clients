# Copyright 2026 Confluent Inc.
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

"""An explicit ``None`` is Java's ``null`` on the producer family and the common
value classes (CLAUDE.md, Python Binding Conventions, Signatures; Implementation
over the FFI, exception 4): left out (``MISSING``) is the only "not given"; a
``None`` Java dereferences raises ``TypeError("<param> must not be None")`` at
Java's point; one Java gives a meaning keeps it."""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka._args import MISSING
from confluent_kafka.common import (
    KafkaFuture,
    Node,
    TopicIdPartition,
    TopicPartition,
    TopicPartitionInfo,
    Uuid,
)
from confluent_kafka.common.security.auth import KafkaPrincipal
from confluent_kafka.common.security.token.delegation.token_information import TokenInformation
from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata, OffsetAndTimestamp
from confluent_kafka.producer import (
    AsyncKafkaProducer,
    AsyncMockProducer,
    KafkaProducer,
    MockProducer,
    Producer,
    ProducerRecord,
)

UNREACHABLE = {"bootstrap.servers": "127.0.0.1:59999", "max.block.ms": 100}
RECORD: ProducerRecord[bytes, bytes] = ProducerRecord(topic="t", value=b"v")


def _group_metadata() -> ConsumerGroupMetadata:
    return ConsumerGroupMetadata._of(group_id="g", generation_id=-1, member_id="",
                                     group_instance_id=None)


def _raises(error: type[BaseException], message: str) -> Any:
    class _Check:
        def __enter__(self) -> None:
            self._ctx = pytest.raises(error)
            self._info = self._ctx.__enter__()

        def __exit__(self, *exc: Any) -> bool:
            handled = self._ctx.__exit__(*exc)
            assert type(self._info.value) is error, self._info.value
            assert str(self._info.value) == message
            return bool(handled)
    return _Check()


# ---- KafkaProducer / AsyncKafkaProducer ------------------------------------------
def test_kafka_producer_send_null_record() -> None:
    # Java's doSend reads record.topic() after throwIfProducerClosed().
    with KafkaProducer(configs=UNREACHABLE) as p:
        with _raises(TypeError, "record must not be None"):
            p.send(record=None)  # type: ignore[arg-type]
        with _raises(TypeError, "record must not be None"):
            p.send(record=None, callback=None)  # type: ignore[arg-type]
    with _raises(IllegalStateError, "Cannot perform operation after producer has been closed"):
        p.send(record=None)  # type: ignore[arg-type]


def test_kafka_producer_close_null_timeout() -> None:
    # Java's timeout.toMillis() on a null: the producer stays open.
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        with _raises(TypeError, "timeout must not be None"):
            p.close(timeout=None)  # type: ignore[arg-type]
        assert p.metrics() is not None  # still open
    finally:
        p.close(timeout=0)
    # A second close() returns before its argument checks (Order).
    p.close(timeout=None)  # type: ignore[arg-type]


def test_kafka_producer_close_missing_is_close() -> None:
    p = KafkaProducer(configs=UNREACHABLE)
    p.close(timeout=MISSING)
    with _raises(IllegalStateError, "Cannot perform operation after producer has been closed"):
        p.flush()


def test_kafka_producer_send_offsets_null_offsets() -> None:
    # throwIfInvalidGroupMetadata first, then offsets.isEmpty().
    with KafkaProducer(configs=UNREACHABLE) as p:
        with _raises(IllegalArgumentError, "Consumer group metadata could not be None"):
            p.send_offsets_to_transaction(offsets=None, group_metadata=None)  # type: ignore[arg-type]
        with _raises(TypeError, "offsets must not be None"):
            p.send_offsets_to_transaction(offsets=None,  # type: ignore[arg-type]
                                          group_metadata=_group_metadata())


async def test_async_kafka_producer_null_arguments() -> None:
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        with _raises(TypeError, "record must not be None"):
            await p.send(record=None)  # type: ignore[arg-type]
        with _raises(TypeError, "offsets must not be None"):
            await p.send_offsets_to_transaction(offsets=None,  # type: ignore[arg-type]
                                                group_metadata=_group_metadata())
        with _raises(TypeError, "timeout must not be None"):
            await p.close(timeout=None)  # type: ignore[arg-type]
    finally:
        await p.close(timeout=0)


@pytest.mark.parametrize("cls", [KafkaProducer, AsyncKafkaProducer])
def test_kafka_producer_serde_none_is_the_config_route(cls: type) -> None:
    # Java's KafkaProducer reads a null serializer as "use the config key"
    # (Signatures, the serde exception): left out and None are the same.
    sig = inspect.signature(cls.__init__)
    assert sig.parameters["key_serializer"].default is MISSING
    assert sig.parameters["value_serializer"].default is MISSING


def test_kafka_producer_serde_none_constructs() -> None:
    with KafkaProducer(configs=UNREACHABLE, key_serializer=None, value_serializer=None) as p:
        assert p.metrics() is not None


# ---- MockProducer / AsyncMockProducer ---------------------------------------------
def test_mock_producer_null_cluster_fails_at_the_first_send() -> None:
    # Java stores the null cluster; its first send() dereferences it
    # (MockProducer.java:312), as does partitionsFor (374).
    p: MockProducer[bytes, bytes] = MockProducer(cluster=None, auto_complete=True,  # type: ignore[call-overload]
                                                 partitioner=None)
    with _raises(TypeError, "cluster must not be None"):
        p.send(record=RECORD)
    with _raises(TypeError, "cluster must not be None"):
        p.partitions_for(topic="t")
    # A null record is dereferenced first (record.topic() is the argument).
    with _raises(TypeError, "record must not be None"):
        p.send(record=None)  # type: ignore[arg-type]


def test_mock_producer_left_out_cluster_is_empty() -> None:
    p: MockProducer[bytes, bytes] = MockProducer(auto_complete=True, partitioner=None)
    assert p.partitions_for(topic="t") == []
    assert p.send(record=RECORD).get().partition() == 0
    q: MockProducer[bytes, bytes] = MockProducer()
    assert q.partitions_for(topic=None) == []  # type: ignore[arg-type]


def test_mock_producer_null_record_and_offsets() -> None:
    p: MockProducer[bytes, bytes] = MockProducer(auto_complete=True, partitioner=None)
    with _raises(TypeError, "record must not be None"):
        p.send(record=None)  # type: ignore[arg-type]
    p.init_transactions()
    p.begin_transaction()
    with _raises(TypeError, "group_metadata must not be None"):
        p.send_offsets_to_transaction(offsets=None, group_metadata=None)  # type: ignore[arg-type]
    with _raises(TypeError, "offsets must not be None"):
        p.send_offsets_to_transaction(offsets=None,  # type: ignore[arg-type]
                                      group_metadata=_group_metadata())
    # Java's mock never reads the close timeout, a null one included.
    p.close(timeout=None)  # type: ignore[arg-type]
    assert p.closed()
    with _raises(IllegalStateError, "MockProducer is already closed."):
        p.send(record=None)  # type: ignore[arg-type]


async def test_async_mock_producer_null_arguments() -> None:
    p: AsyncMockProducer[bytes, bytes] = AsyncMockProducer(  # type: ignore[call-overload]
        cluster=None, auto_complete=True, partitioner=None)
    with _raises(TypeError, "cluster must not be None"):
        await p.send(record=RECORD)
    with _raises(TypeError, "record must not be None"):
        await p.send(record=None)  # type: ignore[arg-type]
    await p.close(timeout=None)  # type: ignore[arg-type]
    assert p.closed()


def test_producer_signatures_show_missing() -> None:
    assert inspect.signature(Producer.close).parameters["timeout"].default is MISSING
    assert inspect.signature(Producer.send).parameters["callback"].default is MISSING
    params = inspect.signature(MockProducer.__init__).parameters
    assert [params[n].default for n in ("cluster", "auto_complete", "partitioner",
                                        "key_serializer", "value_serializer")] == [MISSING] * 5


# ---- KafkaFuture ------------------------------------------------------------------
def test_kafka_future_get_null_timeout() -> None:
    # Java's get(timeout, null) dereferences the TimeUnit, a done future too.
    f = KafkaFuture.completed_future(value=1)
    with _raises(TypeError, "timeout must not be None"):
        f.get(timeout=None)  # type: ignore[arg-type]
    assert f.get(timeout=MISSING) == 1
    assert f.get() == 1


async def test_async_kafka_future_get_null_timeout() -> None:
    from confluent_kafka.common import AsyncKafkaFuture

    f = AsyncKafkaFuture.completed_future(value=1)
    with _raises(TypeError, "timeout must not be None"):
        await f.get(timeout=None)  # type: ignore[arg-type]
    assert await f.get() == 1


# ---- the common value classes -------------------------------------------------------
def test_topic_id_partition_null_arguments() -> None:
    uuid = Uuid.random_uuid()
    # Java: Objects.requireNonNull(topicPartition, "topicPartition can not be null").
    with _raises(TypeError, "topic_partition must not be None"):
        TopicIdPartition(topic_id=uuid, topic_partition=None)  # type: ignore[arg-type]
    with _raises(TypeError, "topic_id must not be None"):
        TopicIdPartition(topic_id=None, topic_partition=None)  # type: ignore[arg-type]
    # A null topic is Java's (topicId, partition, null).
    assert TopicIdPartition(topic_id=uuid, partition=0, topic=None).topic() is None
    with _raises(TypeError, "partition must not be None"):
        TopicIdPartition(topic_id=uuid, partition=None, topic="t")  # type: ignore[arg-type]
    with _raises(IllegalArgumentError,
                 "TopicIdPartition() takes one of (topic_id, topic_partition), "
                 "(topic_id, partition, topic); got (topic_id, partition)"):
        TopicIdPartition(topic_id=uuid, partition=0)  # type: ignore[call-overload]


def test_topic_partition_info_null_lists() -> None:
    node = Node(id=1, host="h", port=1)
    four = TopicPartitionInfo(partition=0, leader=node, replicas=[node], isr=[node])
    assert four.elr() is None and four.last_known_elr() is None
    # Java's Collections.unmodifiableList(null).
    for name in ("replicas", "isr", "elr", "last_known_elr"):
        kwargs: dict[str, Any] = {"partition": 0, "leader": node, "replicas": [node],
                                  "isr": [node], "elr": [], "last_known_elr": []}
        kwargs[name] = None
        with _raises(TypeError, f"{name} must not be None"):
            TopicPartitionInfo(**kwargs)


def test_uuid_from_string_null() -> None:
    with _raises(TypeError, "str must not be None"):
        Uuid.from_string(str=None)  # type: ignore[arg-type]


def test_kafka_principal_token_authenticated() -> None:
    p = KafkaPrincipal(principal_type="User", name="n")
    assert p.token_authenticated() is False
    assert KafkaPrincipal(principal_type="User", name="n",
                          token_authenticated=True).token_authenticated() is True
    p.token_authenticated(token_authenticated=True)
    assert p.token_authenticated() is True
    with _raises(TypeError, "token_authenticated must not be None"):
        p.token_authenticated(token_authenticated=None)  # type: ignore[call-overload]


def test_token_information_requester() -> None:
    owner = KafkaPrincipal(principal_type="User", name="o")
    args: dict[str, Any] = {"token_id": "i", "owner": owner, "renewers": [],
                            "issue_timestamp": 0, "max_timestamp": 0, "expiry_timestamp": 0}
    # The shorter constructor passes the owner; Java's null is stored as given.
    assert TokenInformation(**args).token_requester() is owner
    assert TokenInformation(**args, token_requester=None).token_requester() is None


def test_offset_and_metadata_null_metadata_and_epoch() -> None:
    # Java's null metadata is stored as ""; a None leader epoch is Optional.empty().
    o = OffsetAndMetadata(offset=1, leader_epoch=None, metadata=None)
    assert o.metadata() == "" and o.leader_epoch() is None
    assert OffsetAndMetadata(offset=1, metadata=MISSING).metadata() == ""
    assert OffsetAndTimestamp(offset=1, timestamp=2, leader_epoch=None).leader_epoch() is None
    assert OffsetAndTimestamp(offset=1, timestamp=2).leader_epoch() is None


def test_topic_partition_is_untouched() -> None:
    assert TopicPartition(topic="t", partition=0).partition() == 0


# ---- Critic 99: Java stores the null, or fails with its own NPE ---------------------
def test_consumer_records_stores_a_null_records_map() -> None:
    from confluent_kafka.consumer import ConsumerRecords

    # Java stores records as given (ConsumerRecords.java:77) and dereferences
    # it at first use; next_offsets() does not read it.
    r: Any = ConsumerRecords(records=None, next_offsets={})  # type: ignore[arg-type]
    assert r.next_offsets() == {}
    for use in (lambda: r.records(partition=TopicPartition(topic="t", partition=0)),
                lambda: r.records(topic="t"), lambda: len(r), lambda: r.partitions(),
                lambda: iter(r), lambda: r.is_empty()):
        with _raises(TypeError, "records must not be None"):
            use()
    with _raises(IllegalArgumentError, "Topic must be non-None."):
        r.records(topic=None)


def test_partition_info_stores_null_arrays() -> None:
    from confluent_kafka.common import PartitionInfo

    p: Any = PartitionInfo(topic="t", partition=0, leader=None, replicas=None,  # type: ignore[arg-type]
                           in_sync_replicas=None, offline_replicas=None)  # type: ignore[arg-type]
    assert p.replicas() is None and p.in_sync_replicas() is None and p.offline_replicas() is None
    # Java's formatNodeIds prints a null array as "[]".
    assert str(p) == ("Partition(topic = t, partition = 0, leader = none, replicas = [], "
                      "isr = [], offlineReplicas = [])")
    assert p == PartitionInfo(topic="t", partition=0, leader=None, replicas=None,  # type: ignore[arg-type]
                              in_sync_replicas=None, offline_replicas=None)  # type: ignore[arg-type]


def test_token_information_stores_null_renewers() -> None:
    owner = KafkaPrincipal(principal_type="User", name="o")
    other = KafkaPrincipal(principal_type="User", name="x")
    t: Any = TokenInformation(token_id="i", owner=owner, renewers=None,  # type: ignore[arg-type]
                              issue_timestamp=0, max_timestamp=0, expiry_timestamp=0)
    assert t.renewers() is None
    assert "renewers=None" in str(t)
    assert t.owner_or_renewer(principal=owner) is True  # short-circuits before the renewers
    with _raises(TypeError, "renewers must not be None"):
        t.owner_or_renewer(principal=other)
    with _raises(TypeError, "renewers must not be None"):
        t.renewers_as_string()


def test_kafka_future_all_of_null_futures() -> None:
    # Arrays.stream(null) and a null element's toCompletionStage() (KafkaFuture.java:76, 80).
    with _raises(TypeError, "futures must not be None"):
        KafkaFuture.all_of(futures=None)  # type: ignore[arg-type]
    with _raises(TypeError, "futures must not contain None"):
        KafkaFuture.all_of(futures=(KafkaFuture.completed_future(value=1), None))  # type: ignore[arg-type]


async def test_async_kafka_future_all_of_null_futures() -> None:
    from confluent_kafka.common import AsyncKafkaFuture

    with _raises(TypeError, "futures must not be None"):
        AsyncKafkaFuture.all_of(futures=None)  # type: ignore[arg-type]
    with _raises(TypeError, "futures must not contain None"):
        AsyncKafkaFuture.all_of(futures=(None,))  # type: ignore[arg-type]


def test_clients_null_configs() -> None:
    # Java's new HashMap<>(configs) (ProducerConfig.java:717, ConsumerConfig.java:794,
    # Utils.castToStringObjectMap).
    from confluent_kafka.admin import Admin
    from confluent_kafka.consumer import KafkaConsumer

    for build in (lambda: KafkaProducer(configs=None), lambda: KafkaConsumer(configs=None),
                  lambda: Admin.create(configs=None)):
        with _raises(TypeError, "configs must not be None"):
            build()  # type: ignore[arg-type]
