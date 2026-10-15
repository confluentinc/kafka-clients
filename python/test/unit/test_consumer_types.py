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

"""Tests of the ``confluent_kafka.consumer`` value types and records.

Java's tests translated: ``OffsetAndMetadataTest``, ``CloseOptionsTest``,
``ConsumerRecordTest`` and ``ConsumerRecordsTest`` (all but one).
Skipped: ``OffsetAndMetadataTest``'s two deserialization-compatibility tests
read checked-in Java-serialized files (no Python meaning); its round-trip test
uses pickle for Java's serialization. ``ConsumerGroupMetadataTest`` tests only
the two constructors Java deprecates for removal, which are not offered.
``ConsumerRecordsTest.testNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecatedConstructor``
tests the records-only constructor Java deprecates, which is not generated
(CLAUDE.md, Python Binding Conventions, Class family). No Java test exists for
``OffsetAndTimestamp`` or ``SubscriptionPattern``.
"""

from __future__ import annotations

import copy
import pickle
from datetime import timedelta
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError, NullPointerError
from confluent_kafka.common import TimestampType, TopicPartition
from confluent_kafka.consumer import (
    CloseOptions,
    ConsumerGroupMetadata,
    ConsumerRecord,
    ConsumerRecords,
    KafkaConsumer,
    OffsetAndMetadata,
    OffsetAndTimestamp,
    SubscriptionPattern,
)

_DEFAULT = CloseOptions.GroupMembershipOperation.DEFAULT
_LOGGER = "confluent_kafka.consumer.consumer_records"

# --------------------------------------------------------------------------- #
# OffsetAndMetadata: OffsetAndMetadataTest
# --------------------------------------------------------------------------- #


def test_invalid_negative_offset() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        OffsetAndMetadata(offset=-239, leader_epoch=15, metadata="")
    assert str(exc.value) == "Invalid negative offset"


def test_offset_and_metadata_serialization_roundtrip() -> None:
    for oam in (OffsetAndMetadata(offset=239, leader_epoch=15, metadata="blah"),
                OffsetAndMetadata(offset=239, metadata="blah"),
                OffsetAndMetadata(offset=239)):
        assert pickle.loads(pickle.dumps(oam)) == oam


def test_equals_with_null_and_negative_leader_epoch() -> None:
    with_null = OffsetAndMetadata(offset=100, leader_epoch=None, metadata="metadata")
    with_negative = OffsetAndMetadata(offset=100, leader_epoch=-1, metadata="metadata")
    assert with_null == with_negative
    assert hash(with_null) == hash(with_negative)


def test_equals_with_null_and_empty_metadata() -> None:
    with_null = OffsetAndMetadata(offset=100, leader_epoch=1, metadata=None)  # type: ignore[arg-type]
    with_empty = OffsetAndMetadata(offset=100, leader_epoch=1, metadata="")
    assert with_null == with_empty
    assert hash(with_null) == hash(with_empty)


def test_offset_and_metadata_forms() -> None:
    assert (OffsetAndMetadata(offset=5).metadata(), OffsetAndMetadata(offset=5).leader_epoch()) == (
        "", None)
    assert OffsetAndMetadata(offset=5, metadata="m").leader_epoch() is None
    # metadata is UNSET: an explicit "" is given, so (offset, leaderEpoch, metadata).
    assert OffsetAndMetadata(offset=5, leader_epoch=3, metadata="").leader_epoch() == 3
    with pytest.raises(IllegalArgumentError) as exc:
        OffsetAndMetadata(offset=5, leader_epoch=3)
    assert str(exc.value) == (
        "OffsetAndMetadata() takes one of (offset, leader_epoch, metadata), "
        "(offset, metadata), (offset); got (offset, leader_epoch)")
    # leader_epoch is UNSET too ((offset, metadata) passes Optional.empty()): a
    # None leader epoch is given, so it selects the three-argument constructor,
    # and without metadata it is refused, as Java has no (offset, leaderEpoch).
    assert OffsetAndMetadata(offset=5, leader_epoch=None, metadata="m").metadata() == "m"
    with pytest.raises(IllegalArgumentError) as exc:
        OffsetAndMetadata(offset=5, leader_epoch=None)  # type: ignore[call-overload]
    assert str(exc.value).endswith("; got (offset, leader_epoch)")
    assert str(OffsetAndMetadata(offset=5, leader_epoch=3, metadata="m")) == (
        "OffsetAndMetadata{offset=5, leaderEpoch=3, metadata='m'}")
    assert str(OffsetAndMetadata(offset=5)) == (
        "OffsetAndMetadata{offset=5, leaderEpoch=null, metadata=''}")
    with pytest.raises(TypeError):
        OffsetAndMetadata(5)  # type: ignore[call-arg]


# --------------------------------------------------------------------------- #
# OffsetAndTimestamp (no Java test)
# --------------------------------------------------------------------------- #


def test_offset_and_timestamp() -> None:
    o = OffsetAndTimestamp(offset=1, timestamp=2)
    assert (o.offset(), o.timestamp(), o.leader_epoch()) == (1, 2, None)
    assert OffsetAndTimestamp(offset=1, timestamp=2, leader_epoch=-1).leader_epoch() == -1
    assert o == OffsetAndTimestamp(offset=1, timestamp=2)
    assert hash(o) == hash(OffsetAndTimestamp(offset=1, timestamp=2))
    assert o != OffsetAndTimestamp(offset=1, timestamp=2, leader_epoch=4)
    # leader_epoch is UNSET ((offset, timestamp) passes Optional.empty()); both
    # sets are Java constructors, so a None one is the three-argument form.
    assert o == OffsetAndTimestamp(offset=1, timestamp=2, leader_epoch=None)
    assert OffsetAndTimestamp(offset=1, timestamp=2, leader_epoch=None).leader_epoch() is None
    assert str(o) == "(timestamp=2, leaderEpoch=null, offset=1)"
    with pytest.raises(IllegalArgumentError) as exc:
        OffsetAndTimestamp(offset=-1, timestamp=0)
    assert str(exc.value) == "Invalid negative offset"
    with pytest.raises(IllegalArgumentError) as exc:
        OffsetAndTimestamp(offset=0, timestamp=-1)
    assert str(exc.value) == "Invalid negative timestamp"
    with pytest.raises(TypeError):
        OffsetAndTimestamp(1, 2)  # type: ignore[call-arg]


# --------------------------------------------------------------------------- #
# ConsumerGroupMetadata (no constructor: both Java constructors are
# @Deprecated(forRemoval = true), so ConsumerGroupMetadataTest, which tests only
# them, is not translated)
# --------------------------------------------------------------------------- #


def test_group_metadata_has_no_constructor() -> None:
    for call in (lambda: ConsumerGroupMetadata(),  # type: ignore[call-arg]
                 lambda: ConsumerGroupMetadata(group_id="group")):  # type: ignore[call-arg]
        with pytest.raises(TypeError) as exc:
            call()
        assert str(exc.value) == ("ConsumerGroupMetadata cannot be constructed directly; "
                                  "use the consumer's group_metadata()")


def test_group_metadata_value_semantics() -> None:
    a = ConsumerGroupMetadata._of(group_id="g", generation_id=1, member_id="m",
                                  group_instance_id=None)
    b = ConsumerGroupMetadata._of(group_id="g", generation_id=1, member_id="m",
                                  group_instance_id=None)
    assert (a.group_id(), a.generation_id(), a.member_id(), a.group_instance_id()) == (
        "g", 1, "m", None)
    assert a == b and hash(a) == hash(b)
    assert a != ConsumerGroupMetadata._of(group_id="g", generation_id=1, member_id="m",
                                          group_instance_id="i")
    assert str(a) == "GroupMetadata(groupId = g, generationId = 1, memberId = m, groupInstanceId = )"


def test_kafka_consumer_group_metadata_deep_copies_to_itself() -> None:
    # A KafkaConsumer's metadata holds the core's handle, which cannot be
    # copied; the value is immutable, so a deep copy is the object itself.
    with KafkaConsumer(configs={"bootstrap.servers": "localhost:1",
                                "group.protocol": "consumer", "group.id": "g"}) as consumer:
        metadata = consumer.group_metadata()
    assert copy.deepcopy(metadata) is metadata
    assert copy.copy(metadata) == metadata


# --------------------------------------------------------------------------- #
# CloseOptions: CloseOptionsTest
# --------------------------------------------------------------------------- #


def test_operation_should_not_be_null() -> None:
    with pytest.raises(NullPointerError) as exc:
        CloseOptions.group_membership_operation(None)  # type: ignore[arg-type]
    assert str(exc.value) == "operation should not be null"
    with pytest.raises(NullPointerError):
        CloseOptions.timeout(0).with_group_membership_operation(None)  # type: ignore[arg-type]


def test_operation_should_have_default_value() -> None:
    assert CloseOptions.timeout(0).group_membership_operation() is _DEFAULT


def test_timeout_could_be_null() -> None:
    options = CloseOptions.timeout(None)
    assert options.timeout() is None


def test_timeout_should_be_default_empty() -> None:
    assert CloseOptions.group_membership_operation(_DEFAULT).timeout() is None


def test_close_options_builder_shape() -> None:
    with pytest.raises(TypeError) as exc:
        CloseOptions()
    assert str(exc.value) == (
        "CloseOptions cannot be constructed directly; use CloseOptions.timeout(...) or "
        "CloseOptions.group_membership_operation(...)")
    leave = CloseOptions.GroupMembershipOperation.LEAVE_GROUP
    options = CloseOptions.timeout(timedelta(seconds=3)).with_group_membership_operation(leave)
    # A returned Duration is a float of seconds.
    assert options.timeout() == 3.0
    assert options.group_membership_operation() is leave
    assert options.with_timeout(1.5) is options and options.timeout() == 1.5
    assert [m.value for m in CloseOptions.GroupMembershipOperation] == [
        "LEAVE_GROUP", "REMAIN_IN_GROUP", "DEFAULT"]


# --------------------------------------------------------------------------- #
# SubscriptionPattern (no Java test)
# --------------------------------------------------------------------------- #


def test_subscription_pattern() -> None:
    p = SubscriptionPattern(pattern="t.*")
    assert p.pattern() == "t.*" and str(p) == "t.*"
    assert p == SubscriptionPattern(pattern="t.*") and hash(p) == hash(SubscriptionPattern(pattern="t.*"))
    with pytest.raises(TypeError):
        SubscriptionPattern("t.*")  # type: ignore[call-arg]


def test_offset_reset_strategy_is_not_generated() -> None:
    # Java deprecates the enum (CLAUDE.md, Python Binding Conventions, Class
    # family), and with it the MockConsumer constructor that takes it.
    import confluent_kafka.consumer as consumer_module

    assert not hasattr(consumer_module, "OffsetResetStrategy")


# --------------------------------------------------------------------------- #
# ConsumerRecord: ConsumerRecordTest
# --------------------------------------------------------------------------- #


def test_short_constructor() -> None:
    record = ConsumerRecord(topic="topic", partition=0, offset=23, key="key", value="value")
    assert record.topic() == "topic"
    assert record.partition() == 0
    assert record.offset() == 23
    assert record.key() == "key"
    assert record.value() == "value"
    assert record.timestamp_type() is TimestampType.NO_TIMESTAMP_TYPE
    assert record.timestamp() == ConsumerRecord.NO_TIMESTAMP == -1
    assert record.serialized_key_size() == ConsumerRecord.NULL_SIZE == -1
    assert record.serialized_value_size() == ConsumerRecord.NULL_SIZE
    assert record.leader_epoch() is None
    assert record.delivery_count() is None
    assert record.headers() == ()


def test_long_constructor() -> None:
    headers = [("header key", "header value".encode("utf-8"))]
    record = ConsumerRecord(topic="topic", partition=0, offset=23, timestamp=23434217432432,
                            timestamp_type=TimestampType.CREATE_TIME, serialized_key_size=100,
                            serialized_value_size=1142, key="key", value="value",
                            headers=headers, leader_epoch=None)
    assert (record.topic(), record.partition(), record.offset()) == ("topic", 0, 23)
    assert (record.key(), record.value()) == ("key", "value")
    assert record.timestamp_type() is TimestampType.CREATE_TIME
    assert record.timestamp() == 23434217432432
    assert (record.serialized_key_size(), record.serialized_value_size()) == (100, 1142)
    assert record.leader_epoch() is None and record.delivery_count() is None
    assert [(k, bytes(v or b"")) for k, v in record.headers()] == [
        ("header key", b"header value")]
    record = ConsumerRecord(topic="topic", partition=0, offset=23, timestamp=23434217432432,
                            timestamp_type=TimestampType.CREATE_TIME, serialized_key_size=100,
                            serialized_value_size=1142, key="key", value="value",
                            headers=headers, leader_epoch=10, delivery_count=1)
    assert record.leader_epoch() == 10
    assert record.delivery_count() == 1


def test_consumer_record_null_checks_and_forms() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        ConsumerRecord(topic=None, partition=0, offset=0, key=None, value=None)  # type: ignore[call-overload]
    assert str(exc.value) == "Topic cannot be null"
    with pytest.raises(IllegalArgumentError) as exc:
        ConsumerRecord(topic="t", partition=0, offset=0, timestamp=0,  # type: ignore[call-overload]
                       timestamp_type=TimestampType.CREATE_TIME, serialized_key_size=0,
                       serialized_value_size=0, key=None, value=None, headers=None,
                       leader_epoch=None)
    assert str(exc.value) == "Headers cannot be null"
    # A delivery count needs the full form (Java has no shorter one taking it).
    with pytest.raises(IllegalArgumentError) as exc:
        ConsumerRecord(topic="t", partition=0, offset=0, key=None, value=None,  # type: ignore[call-overload]
                       delivery_count=2)
    assert str(exc.value).endswith("got (topic, partition, offset, key, value, delivery_count)")
    # delivery_count is UNSET (the 11-argument form passes Optional.empty()): a
    # None one is given, so it is refused with the short form too, selects the
    # full form with the long one, and the short form is filled with no count.
    with pytest.raises(IllegalArgumentError) as exc:
        ConsumerRecord(topic="t", partition=0, offset=0, key=None, value=None,  # type: ignore[call-overload]
                       delivery_count=None)
    assert str(exc.value).endswith("got (topic, partition, offset, key, value, delivery_count)")
    assert ConsumerRecord(topic="t", partition=0, offset=0, timestamp=-1,
                          timestamp_type=TimestampType.NO_TIMESTAMP_TYPE, serialized_key_size=-1,
                          serialized_value_size=-1, key=None, value=None, headers=(),
                          leader_epoch=None, delivery_count=None).delivery_count() is None
    assert ConsumerRecord(topic="t", partition=0, offset=0, key=None,
                          value=None).delivery_count() is None
    # UNSET: Java's default values given explicitly still select the full form.
    full = ConsumerRecord(topic="t", partition=0, offset=0, timestamp=-1,
                          timestamp_type=TimestampType.NO_TIMESTAMP_TYPE, serialized_key_size=-1,
                          serialized_value_size=-1, key=None, value=None, headers=(),
                          leader_epoch=None, delivery_count=2)
    assert full.delivery_count() == 2
    assert str(full) == (
        "ConsumerRecord(topic = t, partition = 0, leaderEpoch = null, offset = 0, "
        "NoTimestampType = -1, deliveryCount = 2, serialized key size = -1, "
        "serialized value size = -1, headers = RecordHeaders(headers = [], isReadOnly = false), "
        "key = null, value = null)")
    with pytest.raises(TypeError):
        ConsumerRecord("t", 0, 0, None, None)  # type: ignore[call-overload]


# --------------------------------------------------------------------------- #
# ConsumerRecords: ConsumerRecordsTest
# --------------------------------------------------------------------------- #


def _build_topic_test_records(record_size: int, partition_size: int, empty_partition_index: int,
                              topics: list[str]) -> ConsumerRecords[int, str]:
    partition_to_records: dict[TopicPartition, list[ConsumerRecord[int, str]]] = {}
    next_offsets: dict[TopicPartition, OffsetAndMetadata] = {}
    for topic in topics:
        for i in range(partition_size):
            records: list[ConsumerRecord[int, str]] = []
            if i != empty_partition_index:
                for j in range(record_size):
                    records.append(ConsumerRecord(
                        topic=topic, partition=i, offset=j, timestamp=0,
                        timestamp_type=TimestampType.CREATE_TIME, serialized_key_size=0,
                        serialized_value_size=0, key=j, value=str(j), headers=(),
                        leader_epoch=None))
            tp = TopicPartition(topic=topic, partition=i)
            partition_to_records[tp] = records
            next_offsets[tp] = OffsetAndMetadata(offset=record_size, leader_epoch=None, metadata="")
    return ConsumerRecords(records=partition_to_records, next_offsets=next_offsets)


def _validate_record_payload(topic: str, record: ConsumerRecord[int, str], current_partition: int,
                             record_count: int, record_size: int) -> None:
    assert record.topic() == topic
    assert record.partition() == current_partition
    assert record.offset() == record_count % record_size
    assert record.key() == record_count % record_size
    assert record.value() == str(record_count % record_size)


def test_iterator() -> None:
    topic, record_size, partition_size, empty = "topic", 10, 15, 3
    records = _build_topic_test_records(record_size, partition_size, empty, [topic])
    record_count = partition_count = 0
    current_partition = -1
    for record in records:
        assert record.partition() != empty, f"Partition {record.partition()} is not empty"
        if current_partition != record.partition():
            partition_count += 1
            current_partition = record.partition()
        _validate_record_payload(topic, record, current_partition, record_count, record_size)
        record_count += 1
    assert partition_count + 1 == partition_size


def test_records_by_partition() -> None:
    topics, record_size, partition_size, empty = ["topic1", "topic2"], 3, 5, 2
    consumer_records = _build_topic_test_records(record_size, partition_size, empty, topics)
    assert len(consumer_records.next_offsets()) == partition_size * len(topics)
    for topic in topics:
        for partition in range(partition_size):
            tp = TopicPartition(topic=topic, partition=partition)
            records = consumer_records.records(partition=tp)
            if partition == empty:
                assert records == []
            else:
                assert len(records) == record_size
                last = records[record_size - 1]
                assert consumer_records.next_offsets()[tp] == OffsetAndMetadata(
                    offset=last.offset() + 1, leader_epoch=last.leader_epoch(), metadata="")
                for i, record in enumerate(records):
                    _validate_record_payload(topic, record, partition, i, record_size)


def test_records_by_null_topic() -> None:
    # Java's records(null) throws "Topic must be non-null."; in Python a None
    # topic is not given, so java_forms rejects the call naming both forms.
    with pytest.raises(IllegalArgumentError) as exc:
        ConsumerRecords.empty().records(topic=None)  # type: ignore[call-overload]
    assert str(exc.value) == "records() takes one of (partition), (topic); got ()"


def test_records_by_topic() -> None:
    topics, record_size, partition_size, empty = ["topic1", "topic2", "topic3", "topic4"], 3, 10, 6
    consumer_records = _build_topic_test_records(record_size, partition_size, empty, topics)
    assert len(consumer_records.next_offsets()) == partition_size * len(topics)
    for topic in topics:
        record_count = partition_count = 0
        current_partition = -1
        for record in consumer_records.records(topic=topic):
            assert record.partition() != empty
            if current_partition != record.partition():
                partition_count += 1
                current_partition = record.partition()
            _validate_record_payload(topic, record, current_partition, record_count, record_size)
            record_count += 1
        assert partition_count + 1 == partition_size
        assert record_count == record_size * (partition_size - 1)


def test_records_are_immutable() -> None:
    topic, record_size, partition_size, empty = "topic", 3, 6, 2
    tp = TopicPartition(topic=topic, partition=0)
    new_record: ConsumerRecord[int, str] = ConsumerRecord(
        topic=topic, partition=0, offset=0, timestamp=0, timestamp_type=TimestampType.CREATE_TIME,
        serialized_key_size=0, serialized_value_size=0, key=0, value="0", headers=(),
        leader_epoch=None)
    records = _build_topic_test_records(record_size, partition_size, empty, [topic])
    empty_records: ConsumerRecords[int, str] = ConsumerRecords.empty()
    assert len(records.next_offsets()) == partition_size
    # Java's views throw UnsupportedOperationException; Python hands out
    # copies, so a change to one leaves the records as they were.
    for batch, expected in ((records, record_size * (partition_size - 1)), (empty_records, 0)):
        batch.records(partition=tp).append(new_record)
        batch.partitions().add(tp)
        batch.records(topic=topic).clear()
        batch.next_offsets().clear()
        assert len(batch) == expected
    assert len(records.next_offsets()) == partition_size


def test_the_deprecated_records_only_constructor_is_not_generated() -> None:
    # Java's @Deprecated ConsumerRecords(Map) (CLAUDE.md, Class family):
    # next_offsets is required.
    records: dict[TopicPartition, list[ConsumerRecord[int, str]]] = {}
    with pytest.raises(TypeError):
        ConsumerRecords(records=records)  # type: ignore[call-arg]


def test_next_offsets_does_not_log_error_when_constructed_with_next_offsets(
        caplog: pytest.LogCaptureFixture) -> None:
    tp = TopicPartition(topic="topic", partition=0)
    records = {tp: [ConsumerRecord(topic="topic", partition=0, offset=0, key=0, value="value")]}
    next_offsets = {tp: OffsetAndMetadata(offset=1)}
    with caplog.at_level("ERROR", logger=_LOGGER):
        consumer_records = ConsumerRecords(records=records, next_offsets=next_offsets)
        assert consumer_records.next_offsets() == next_offsets
        assert [r for r in caplog.records if r.levelname == "ERROR"] == []


def test_next_offsets_does_not_log_error_for_empty_records(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("ERROR", logger=_LOGGER):
        assert ConsumerRecords.empty().next_offsets() == {}
        assert [r for r in caplog.records if r.levelname == "ERROR"] == []


def test_consumer_records_surface() -> None:
    assert ConsumerRecords.EMPTY is ConsumerRecords.empty()
    assert ConsumerRecords.EMPTY.is_empty() and len(ConsumerRecords.EMPTY) == 0
    with pytest.raises(IllegalArgumentError) as exc:
        ConsumerRecords.empty().records(partition=TopicPartition(topic="t", partition=0),  # type: ignore[call-overload]
                                        topic="t")
    assert str(exc.value) == "records() takes one of (partition), (topic); got (partition, topic)"
    records: Any = {}
    with pytest.raises(TypeError):
        ConsumerRecords(records)  # type: ignore[call-arg]
