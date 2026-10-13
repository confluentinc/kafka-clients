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

"""``MockProducerTest.java`` (Apache Kafka 4.3.1), every ``@Test``, in Java order.

Java asserts the class of most errors; these tests also assert the message
``MockProducer.java`` throws (CLAUDE.md, Python Binding Conventions, Tests and
typing). Java's ``Cluster`` and ``RoundRobinPartitioner`` are not translated
(placeholder alias, partitioner implementation), so ``_Cluster`` and
``_RoundRobinPartitioner`` below stand in for them with the methods the mock
calls. Java's fixture serializer ``MockSerializer`` is a UTF-8 string
serializer, so ``string_serializer()`` replaces it.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.common import KafkaError, PartitionInfo, TopicPartition
from confluent_kafka.common.errors import ProducerFencedError
from confluent_kafka.common.serialization import int_serializer, string_serializer
from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.producer import MockProducer, ProducerRecord, RecordMetadata

TOPIC = "topic"
GROUP_ID = "group"
RECORD1 = ProducerRecord(topic=TOPIC, key="key1", value="value1")
RECORD2 = ProducerRecord(topic=TOPIC, key="key2", value="value2")

NOT_INITIALIZED = "MockProducer hasn't been initialized for transactions."
NO_TRANSACTION = "There is no open transaction."
ALREADY_CLOSED = "MockProducer is already closed."
FENCED = "MockProducer is fenced."


class _Cluster:
    """Stands in for Java's ``Cluster`` (not translated): the methods
    ``MockProducer`` and ``RoundRobinPartitioner`` call on it."""

    def __init__(self, partitions: list[PartitionInfo]) -> None:
        self._by_topic: dict[str, list[PartitionInfo]] = {}
        for p in partitions:
            self._by_topic.setdefault(p.topic(), []).append(p)

    def partitions_for_topic(self, topic: str) -> list[PartitionInfo]:
        return list(self._by_topic.get(topic, []))

    def available_partitions_for_topic(self, topic: str) -> list[PartitionInfo]:
        return [p for p in self._by_topic.get(topic, []) if p.leader() is not None]


def _empty_cluster() -> _Cluster:
    """Java's ``Cluster.empty()``."""
    return _Cluster([])


class _RoundRobinPartitioner:
    """Stands in for Java's ``RoundRobinPartitioner`` (a partitioner
    implementation, not translated): ``partition()`` as Java writes it."""

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}

    def partition(self, topic: str, key: object, key_bytes: bytes | None, value: object,
                  value_bytes: bytes | None, cluster: _Cluster) -> int:
        partitions = cluster.partitions_for_topic(topic)
        num_partitions = len(partitions)
        next_value = self._counters.get(topic, 0)
        self._counters[topic] = next_value + 1
        available = cluster.available_partitions_for_topic(topic)
        if available:
            return available[(next_value & 0x7FFFFFFF) % len(available)].partition()
        # no partitions are available, give a non-available partition
        return (next_value & 0x7FFFFFFF) % num_partitions


_created: list[MockProducer[Any, Any]] = []


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    """Java's ``@AfterEach cleanup``: close the producer if still open."""
    yield
    while _created:
        producer = _created.pop()
        if not producer.closed():
            producer.close()


def build_mock_producer(auto_complete: bool) -> MockProducer[str, str]:
    """Java's ``buildMockProducer(autoComplete)``: ``new
    MockProducer<>(Cluster.empty(), autoComplete, null, new MockSerializer(),
    new MockSerializer())``."""
    producer = MockProducer(cluster=_empty_cluster(), auto_complete=auto_complete,
                            partitioner=None, key_serializer=string_serializer(),
                            value_serializer=string_serializer())
    _created.append(producer)
    return producer


def is_error(future: Any) -> bool:
    try:
        future.result()
        return False
    except Exception:  # noqa: BLE001 - Java catches Exception
        return True


def test_auto_complete_mock() -> None:
    producer = build_mock_producer(True)
    metadata = producer.send(record=RECORD1)
    assert metadata.done(), "Send should be immediately complete"
    assert not is_error(metadata), "Send should be successful"
    assert metadata.result().offset() == 0, "Offset should be 0"
    assert metadata.result().topic() == TOPIC
    assert producer.history() == [RECORD1], "We should have the record in our history"
    producer.clear()
    assert len(producer.history()) == 0, "Clear should erase our history"


def test_partitioner() -> None:
    partition_info0 = PartitionInfo(topic=TOPIC, partition=0, leader=None, replicas=(),
                                    in_sync_replicas=())
    partition_info1 = PartitionInfo(topic=TOPIC, partition=1, leader=None, replicas=(),
                                    in_sync_replicas=())
    cluster = _Cluster([partition_info0, partition_info1])
    producer = MockProducer(cluster=cluster, auto_complete=True,
                            partitioner=_RoundRobinPartitioner(),
                            key_serializer=string_serializer(),
                            value_serializer=string_serializer())
    record = ProducerRecord(topic=TOPIC, key="key", value="value")
    metadata = producer.send(record=record)
    assert metadata.result().partition() == 0, "Partition should be correct"
    producer.clear()
    assert len(producer.history()) == 0, "Clear should erase our history"
    producer.close()


def test_manual_completion() -> None:
    producer = build_mock_producer(False)
    md1 = producer.send(record=RECORD1)
    assert not md1.done(), "Send shouldn't have completed"
    md2 = producer.send(record=RECORD2)
    assert not md2.done(), "Send shouldn't have completed"
    assert producer.complete_next(), "Complete the first request"
    assert not is_error(md1), "Request should be successful"
    assert not md2.done(), "Second request still incomplete"
    e = IllegalArgumentError(message="blah")
    assert producer.error_next(e=e), "Complete the second request with an error"
    with pytest.raises(IllegalArgumentError) as err:
        md2.result()
    assert err.value is e
    assert not producer.complete_next(), "No more requests to complete"

    md3 = producer.send(record=RECORD1)
    md4 = producer.send(record=RECORD2)
    assert not md3.done() and not md4.done(), "Requests should not be completed."
    producer.flush()
    assert md3.done() and md4.done(), "Requests should be completed."


def test_should_init_transactions() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    assert producer.transaction_initialized()


def test_should_throw_on_init_transaction_if_producer_already_initialized_for_transactions() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    with pytest.raises(IllegalStateError) as err:
        producer.init_transactions()
    assert str(err.value) == "MockProducer has already been initialized for transactions."


def test_should_throw_on_begin_transaction_if_transactions_not_initialized() -> None:
    producer = build_mock_producer(True)
    with pytest.raises(IllegalStateError) as err:
        producer.begin_transaction()
    assert str(err.value) == NOT_INITIALIZED


def test_should_begin_transactions() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()
    assert producer.transaction_in_flight()


def test_should_throw_on_begin_transactions_if_transaction_inflight() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()
    with pytest.raises(IllegalStateError) as err:
        producer.begin_transaction()
    assert str(err.value) == "Transaction already started"


def test_should_throw_on_send_offsets_to_transaction_if_transactions_not_initialized() -> None:
    producer = build_mock_producer(True)
    with pytest.raises(IllegalStateError) as err:
        producer.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata(GROUP_ID))
    assert str(err.value) == NOT_INITIALIZED


def test_should_throw_on_send_offsets_to_transaction_transaction_if_no_transaction_got_started() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    with pytest.raises(IllegalStateError) as err:
        producer.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata(GROUP_ID))
    assert str(err.value) == NO_TRANSACTION


def test_should_throw_on_commit_if_transactions_not_initialized() -> None:
    producer = build_mock_producer(True)
    with pytest.raises(IllegalStateError) as err:
        producer.commit_transaction()
    assert str(err.value) == NOT_INITIALIZED


def test_should_throw_on_commit_transaction_if_no_transaction_got_started() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    with pytest.raises(IllegalStateError) as err:
        producer.commit_transaction()
    assert str(err.value) == NO_TRANSACTION


def test_should_commit_empty_transaction() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()
    producer.commit_transaction()
    assert not producer.transaction_in_flight()
    assert producer.transaction_committed()
    assert not producer.transaction_aborted()


def test_should_count_committed_transaction() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()
    assert producer.commit_count() == 0
    producer.commit_transaction()
    assert producer.commit_count() == 1


def test_should_not_count_aborted_transaction() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()
    producer.abort_transaction()
    producer.begin_transaction()
    producer.commit_transaction()
    assert producer.commit_count() == 1


def test_should_throw_on_abort_if_transactions_not_initialized() -> None:
    producer = build_mock_producer(True)
    with pytest.raises(IllegalStateError) as err:
        producer.abort_transaction()
    assert str(err.value) == NOT_INITIALIZED


def test_should_throw_on_abort_transaction_if_no_transaction_got_started() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    with pytest.raises(IllegalStateError) as err:
        producer.abort_transaction()
    assert str(err.value) == NO_TRANSACTION


def test_should_abort_empty_transaction() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()
    producer.abort_transaction()
    assert not producer.transaction_in_flight()
    assert producer.transaction_aborted()
    assert not producer.transaction_committed()


def test_should_throw_fence_producer_if_transactions_not_initialized() -> None:
    producer = build_mock_producer(True)
    with pytest.raises(IllegalStateError) as err:
        producer.fence_producer()
    assert str(err.value) == NOT_INITIALIZED


def test_should_throw_on_begin_transactions_if_producer_got_fenced() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.fence_producer()
    with pytest.raises(ProducerFencedError) as err:
        producer.begin_transaction()
    assert str(err.value) == FENCED


def test_should_throw_on_send_if_producer_got_fenced() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.fence_producer()
    with pytest.raises(KafkaError) as err:
        producer.send(record=None)  # type: ignore[arg-type]
    assert type(err.value) is KafkaError
    assert str(err.value) == FENCED
    assert isinstance(err.value.__cause__, ProducerFencedError), (
        "The root cause of the exception should be ProducerFenced")
    assert str(err.value.__cause__) == "Fenced"


def test_should_throw_on_send_offsets_to_transaction_by_group_id_if_producer_got_fenced() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.fence_producer()
    with pytest.raises(ProducerFencedError) as err:
        producer.send_offsets_to_transaction(offsets=None,  # type: ignore[arg-type]
                                             group_metadata=_group_metadata(GROUP_ID))
    assert str(err.value) == FENCED


def test_should_throw_on_send_offsets_to_transaction_by_group_metadata_if_producer_got_fenced() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.fence_producer()
    with pytest.raises(ProducerFencedError) as err:
        producer.send_offsets_to_transaction(offsets=None,  # type: ignore[arg-type]
                                             group_metadata=_group_metadata(GROUP_ID))
    assert str(err.value) == FENCED


def test_should_throw_on_commit_transaction_if_producer_got_fenced() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.fence_producer()
    with pytest.raises(ProducerFencedError) as err:
        producer.commit_transaction()
    assert str(err.value) == FENCED


def test_should_throw_on_abort_transaction_if_producer_got_fenced() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.fence_producer()
    with pytest.raises(ProducerFencedError) as err:
        producer.abort_transaction()
    assert str(err.value) == FENCED


def test_should_publish_messages_only_after_commit_if_transactions_are_enabled() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()

    producer.send(record=RECORD1)
    producer.send(record=RECORD2)

    assert producer.history() == []

    producer.commit_transaction()

    assert producer.history() == [RECORD1, RECORD2]


def test_should_flush_on_commit_for_non_auto_complete_if_transactions_are_enabled() -> None:
    producer = build_mock_producer(False)
    producer.init_transactions()
    producer.begin_transaction()

    md1 = producer.send(record=RECORD1)
    md2 = producer.send(record=RECORD2)

    assert not md1.done()
    assert not md2.done()

    producer.commit_transaction()

    assert md1.done()
    assert md2.done()


def test_should_drop_messages_on_abort_if_transactions_are_enabled() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()

    producer.begin_transaction()
    producer.send(record=RECORD1)
    producer.send(record=RECORD2)
    producer.abort_transaction()
    assert producer.history() == []

    producer.begin_transaction()
    producer.commit_transaction()
    assert producer.history() == []


def test_should_throw_on_abort_for_non_auto_complete_if_transactions_are_enabled() -> None:
    producer = build_mock_producer(False)
    producer.init_transactions()
    producer.begin_transaction()

    md1 = producer.send(record=RECORD1)
    assert not md1.done()
    producer.abort_transaction()
    assert md1.done()


def test_should_preserve_committed_messages_on_abort_if_transactions_are_enabled() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()

    producer.begin_transaction()
    producer.send(record=RECORD1)
    producer.send(record=RECORD2)
    producer.commit_transaction()

    producer.begin_transaction()
    producer.abort_transaction()

    assert producer.history() == [RECORD1, RECORD2]


def test_should_publish_consumer_group_offsets_only_after_commit_if_transactions_are_enabled() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()

    group1 = "g1"
    group1_commit = {TopicPartition(topic=TOPIC, partition=0): OffsetAndMetadata(offset=42),
                     TopicPartition(topic=TOPIC, partition=1): OffsetAndMetadata(offset=73)}
    group2 = "g2"
    group2_commit = {TopicPartition(topic=TOPIC, partition=0): OffsetAndMetadata(offset=101),
                     TopicPartition(topic=TOPIC, partition=1): OffsetAndMetadata(offset=21)}
    producer.send_offsets_to_transaction(offsets=group1_commit,
                                         group_metadata=_group_metadata(group1))
    producer.send_offsets_to_transaction(offsets=group2_commit,
                                         group_metadata=_group_metadata(group2))

    assert producer.consumer_group_offsets_history() == []

    expected_result = {group1: group1_commit, group2: group2_commit}

    producer.commit_transaction()
    assert producer.consumer_group_offsets_history() == [expected_result]


# Not translated: shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction
# gets its NullPointerException from the deprecated new ConsumerGroupMetadata(null),
# which is not offered.


def test_should_ignore_empty_offsets_when_send_offsets_to_transaction_by_group_metadata() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()
    producer.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata("groupId"))
    assert not producer.sent_offsets()


def test_should_add_offsets_when_send_offsets_to_transaction_by_group_metadata() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()

    assert not producer.sent_offsets()

    group_commit = {TopicPartition(topic=TOPIC, partition=0):
                    OffsetAndMetadata(offset=42, leader_epoch=None, metadata="")}
    producer.send_offsets_to_transaction(offsets=group_commit,
                                         group_metadata=_group_metadata("groupId"))
    assert producer.sent_offsets()


def test_should_reset_sent_offsets_flag_only_when_beginning_new_transaction() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()

    assert not producer.sent_offsets()

    group_commit = {TopicPartition(topic=TOPIC, partition=0):
                    OffsetAndMetadata(offset=42, leader_epoch=None, metadata="")}
    producer.send_offsets_to_transaction(offsets=group_commit,
                                         group_metadata=_group_metadata("groupId"))
    producer.commit_transaction()  # commit should not reset "sentOffsets" flag
    assert producer.sent_offsets()

    producer.begin_transaction()
    assert not producer.sent_offsets()

    producer.send_offsets_to_transaction(offsets=group_commit,
                                         group_metadata=_group_metadata("groupId"))
    producer.commit_transaction()  # commit should not reset "sentOffsets" flag
    assert producer.sent_offsets()

    producer.begin_transaction()
    assert not producer.sent_offsets()


def test_should_publish_latest_and_cumulative_consumer_group_offsets_only_after_commit_if_transactions_are_enabled() -> None:  # noqa: E501
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()

    group = "g"
    group_commit1 = {TopicPartition(topic=TOPIC, partition=0): OffsetAndMetadata(offset=42),
                     TopicPartition(topic=TOPIC, partition=1): OffsetAndMetadata(offset=73)}
    group_commit2 = {TopicPartition(topic=TOPIC, partition=1): OffsetAndMetadata(offset=101),
                     TopicPartition(topic=TOPIC, partition=2): OffsetAndMetadata(offset=21)}
    producer.send_offsets_to_transaction(offsets=group_commit1,
                                         group_metadata=_group_metadata(group))
    producer.send_offsets_to_transaction(offsets=group_commit2,
                                         group_metadata=_group_metadata(group))

    assert producer.consumer_group_offsets_history() == []

    expected_result = {group: {
        TopicPartition(topic=TOPIC, partition=0): OffsetAndMetadata(offset=42),
        TopicPartition(topic=TOPIC, partition=1): OffsetAndMetadata(offset=101),
        TopicPartition(topic=TOPIC, partition=2): OffsetAndMetadata(offset=21),
    }}

    producer.commit_transaction()
    assert producer.consumer_group_offsets_history() == [expected_result]


def test_should_drop_consumer_group_offsets_on_abort_if_transactions_are_enabled() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()

    group = "g"
    group_commit = {TopicPartition(topic=TOPIC, partition=0): OffsetAndMetadata(offset=42),
                    TopicPartition(topic=TOPIC, partition=1): OffsetAndMetadata(offset=73)}
    producer.send_offsets_to_transaction(offsets=group_commit,
                                         group_metadata=_group_metadata(group))
    producer.abort_transaction()

    producer.begin_transaction()
    producer.commit_transaction()
    assert producer.consumer_group_offsets_history() == []

    producer.begin_transaction()
    producer.send_offsets_to_transaction(offsets=group_commit,
                                         group_metadata=_group_metadata(group))
    producer.abort_transaction()

    producer.begin_transaction()
    producer.commit_transaction()
    assert producer.consumer_group_offsets_history() == []


def test_should_preserve_offsets_from_commit_by_group_id_on_abort_if_transactions_are_enabled() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()

    group = "g"
    group_commit = {TopicPartition(topic=TOPIC, partition=0): OffsetAndMetadata(offset=42),
                    TopicPartition(topic=TOPIC, partition=1): OffsetAndMetadata(offset=73)}
    producer.send_offsets_to_transaction(offsets=group_commit,
                                         group_metadata=_group_metadata(group))
    producer.commit_transaction()

    producer.begin_transaction()
    producer.abort_transaction()

    assert producer.consumer_group_offsets_history() == [{group: group_commit}]


def test_should_preserve_offsets_from_commit_by_group_metadata_on_abort_if_transactions_are_enabled() -> None:  # noqa: E501
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.begin_transaction()

    group = "g"
    group_commit = {TopicPartition(topic=TOPIC, partition=0): OffsetAndMetadata(offset=42),
                    TopicPartition(topic=TOPIC, partition=1): OffsetAndMetadata(offset=73)}
    producer.send_offsets_to_transaction(offsets=group_commit,
                                         group_metadata=_group_metadata(group))
    producer.commit_transaction()

    producer.begin_transaction()

    group2 = "g2"
    group_commit2 = {TopicPartition(topic=TOPIC, partition=2): OffsetAndMetadata(offset=53),
                     TopicPartition(topic=TOPIC, partition=3): OffsetAndMetadata(offset=84)}
    producer.send_offsets_to_transaction(offsets=group_commit2,
                                         group_metadata=_group_metadata(group2))
    producer.abort_transaction()

    assert producer.consumer_group_offsets_history() == [{group: group_commit}]


def test_should_throw_on_init_transaction_if_producer_is_closed() -> None:
    producer = build_mock_producer(True)
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.init_transactions()
    assert str(err.value) == ALREADY_CLOSED


def test_should_throw_on_send_if_producer_is_closed() -> None:
    producer = build_mock_producer(True)
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.send(record=None)  # type: ignore[arg-type]
    assert str(err.value) == ALREADY_CLOSED


def test_should_throw_on_begin_transaction_if_producer_is_closed() -> None:
    producer = build_mock_producer(True)
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.begin_transaction()
    assert str(err.value) == ALREADY_CLOSED


def test_should_throw_send_offsets_to_transaction_by_group_id_if_producer_is_closed() -> None:
    producer = build_mock_producer(True)
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.send_offsets_to_transaction(offsets=None,  # type: ignore[arg-type]
                                             group_metadata=_group_metadata(GROUP_ID))
    assert str(err.value) == ALREADY_CLOSED


def test_should_throw_send_offsets_to_transaction_by_group_metadata_if_producer_is_closed() -> None:
    producer = build_mock_producer(True)
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.send_offsets_to_transaction(offsets=None,  # type: ignore[arg-type]
                                             group_metadata=_group_metadata(GROUP_ID))
    assert str(err.value) == ALREADY_CLOSED


def test_should_throw_on_commit_transaction_if_producer_is_closed() -> None:
    producer = build_mock_producer(True)
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.commit_transaction()
    assert str(err.value) == ALREADY_CLOSED


def test_should_throw_on_abort_transaction_if_producer_is_closed() -> None:
    producer = build_mock_producer(True)
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.abort_transaction()
    assert str(err.value) == ALREADY_CLOSED


def test_should_throw_on_fence_producer_if_producer_is_closed() -> None:
    producer = build_mock_producer(True)
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.fence_producer()
    assert str(err.value) == ALREADY_CLOSED


def test_should_throw_on_flush_producer_if_producer_is_closed() -> None:
    producer = build_mock_producer(True)
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.flush()
    assert str(err.value) == ALREADY_CLOSED


def test_should_not_throw_on_flush_producer_if_producer_is_fenced() -> None:
    producer = build_mock_producer(True)
    producer.init_transactions()
    producer.fence_producer()
    producer.flush()


def test_should_throw_class_cast_exception() -> None:
    # Java: an IntegerSerializer given a String key throws ClassCastException
    # from send(). The mock calls the serializer, so send() raises the error the
    # serializer raises for that value.
    with pytest.raises(Exception) as direct:
        int_serializer()(TOPIC, "key1", ())
    with MockProducer(cluster=_empty_cluster(), auto_complete=True, partitioner=None,
                      key_serializer=int_serializer(),
                      value_serializer=string_serializer()) as custom_producer:
        with pytest.raises(type(direct.value)) as err:
            custom_producer.send(record=ProducerRecord(topic=TOPIC, key="key1",  # type: ignore[arg-type]
                                                       value="value1"))
        assert str(err.value) == str(direct.value)


def test_should_be_flushed_if_no_buffered_records() -> None:
    producer = build_mock_producer(True)
    assert producer.flushed()


def test_should_be_flushed_with_auto_complete_if_buffered_records() -> None:
    producer = build_mock_producer(True)
    producer.send(record=RECORD1)
    assert producer.flushed()


def test_should_not_be_flushed_with_no_auto_complete_if_buffered_records() -> None:
    producer = build_mock_producer(False)
    producer.send(record=RECORD1)
    assert not producer.flushed()


def test_should_not_be_flushed_after_flush() -> None:
    producer = build_mock_producer(False)
    producer.send(record=RECORD1)
    producer.flush()
    assert producer.flushed()


def test_metadata_on_exception() -> None:
    producer = build_mock_producer(False)
    seen: list[tuple[RecordMetadata, Exception | None]] = []

    def callback(md: RecordMetadata, exception: Exception | None) -> None:
        assert md is not None
        assert md.offset() == -1, "Invalid offset"
        assert md.timestamp() == -1, "Invalid timestamp"
        assert md.serialized_key_size() == -1, "Invalid Serialized Key size"
        assert md.serialized_value_size() == -1, "Invalid Serialized value size"
        seen.append((md, exception))

    metadata = producer.send(record=RECORD2, callback=callback)
    e = IllegalArgumentError(message="dummy exception")
    assert producer.error_next(e=e), "Complete the second request with an error"
    with pytest.raises(IllegalArgumentError) as err:
        metadata.result()
    assert err.value is e
    assert len(seen) == 1 and seen[0][1] is e


def _group_metadata(group_id: str) -> ConsumerGroupMetadata:
    """What Java's deprecated ``new ConsumerGroupMetadata(groupId)`` builds; the
    mock reads only the group id."""
    return ConsumerGroupMetadata._of(group_id=group_id, generation_id=-1, member_id="",
                                     group_instance_id=None)
