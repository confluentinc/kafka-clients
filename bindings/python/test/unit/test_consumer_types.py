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

"""Unit tests for ``confluent_kafka.consumer`` value types (P2).

Translates the Java tests where they exist:

- ``OffsetAndMetadataTest`` — all cases except the three that exercise Java
  ``Serializable`` round-trips / a checked-in serialized blob (not relevant to
  Python; noted below). The equality-semantics cases are translated.
- ``ConsumerGroupMetadataTest`` — all four cases (the null-argument cases become
  ``TypeError``, Python's analog of Java's ``NullPointerException``).
- ``CloseOptionsTest`` — all four cases.
- ``ConsumerRecordTest`` — ``testShortConstructor`` / ``testLongConstructor``.
- ``ConsumerRecordsTest`` — iterator / records-by-partition / records-by-topic /
  null-topic / immutability / next-offsets tainted-vs-supplied behaviour.

No Java test exists for ``OffsetAndTimestamp``, ``SubscriptionPattern`` or
``OffsetResetStrategy``; behavioural tests are added for parity.
"""

from __future__ import annotations

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common import TimestampType, TopicPartition
from confluent_kafka.consumer import (
    CloseOptions,
    ConsumerGroupMetadata,
    ConsumerRecord,
    ConsumerRecords,
    OffsetAndMetadata,
    OffsetAndTimestamp,
    OffsetResetStrategy,
    SubscriptionPattern,
)

_GMO = CloseOptions.GroupMembershipOperation

# --------------------------------------------------------------------------- #
# OffsetAndMetadata — translated from OffsetAndMetadataTest
# --------------------------------------------------------------------------- #


def test_offset_and_metadata_invalid_negative_offset() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        OffsetAndMetadata(offset=-239, leader_epoch=15, metadata="")
    assert "Invalid negative offset" in str(exc.value)


def test_offset_and_metadata_accessors_and_defaults() -> None:
    om = OffsetAndMetadata(offset=239)
    assert om.offset() == 239
    assert om.metadata() == ""
    assert om.leader_epoch() is None

    om2 = OffsetAndMetadata(offset=239, leader_epoch=15, metadata="blah")
    assert om2.leader_epoch() == 15
    assert om2.metadata() == "blah"


def test_offset_and_metadata_equals_null_and_negative_leader_epoch() -> None:
    # Java testEqualsWithNullAndNegativeLeaderEpoch.
    with_null = OffsetAndMetadata(offset=100, leader_epoch=None,
                                  metadata="metadata")
    with_negative = OffsetAndMetadata(offset=100, leader_epoch=-1,
                                      metadata="metadata")
    assert with_null == with_negative
    assert hash(with_null) == hash(with_negative)
    # And both read back as absent.
    assert with_null.leader_epoch() is None
    assert with_negative.leader_epoch() is None


def test_offset_and_metadata_equals_null_and_empty_metadata() -> None:
    # Java testEqualsWithNullAndEmptyMetadata: null metadata -> "".
    with_null = OffsetAndMetadata(offset=100, leader_epoch=1,
                                  metadata=None)  # type: ignore[arg-type]
    with_empty = OffsetAndMetadata(offset=100, leader_epoch=1, metadata="")
    assert with_null == with_empty
    assert hash(with_null) == hash(with_empty)
    assert with_null.metadata() == ""


def test_offset_and_metadata_keyword_only() -> None:
    with pytest.raises(TypeError):
        OffsetAndMetadata(1)  # type: ignore[misc, call-arg]


# Skipped: OffsetAndMetadataTest.testSerializationRoundtrip /
# testDeserializationCompatibilityBeforeLeaderEpoch /
# testDeserializationCompatibilityWithLeaderEpoch exercise Java ``Serializable``
# round-trips and checked-in serialized blobs, which have no Python analog.


# --------------------------------------------------------------------------- #
# OffsetAndTimestamp
# --------------------------------------------------------------------------- #


def test_offset_and_timestamp_accessors_and_validation() -> None:
    ot = OffsetAndTimestamp(offset=5, timestamp=100)
    assert ot.offset() == 5
    assert ot.timestamp() == 100
    assert ot.leader_epoch() is None

    ot2 = OffsetAndTimestamp(offset=5, timestamp=100, leader_epoch=7)
    assert ot2.leader_epoch() == 7

    with pytest.raises(IllegalArgumentError) as exc:
        OffsetAndTimestamp(offset=-1, timestamp=1)
    assert "Invalid negative offset" in str(exc.value)
    with pytest.raises(IllegalArgumentError) as exc:
        OffsetAndTimestamp(offset=1, timestamp=-1)
    assert "Invalid negative timestamp" in str(exc.value)


def test_offset_and_timestamp_equality_uses_raw_leader_epoch() -> None:
    # Unlike OffsetAndMetadata, equality here uses the stored epoch verbatim.
    a = OffsetAndTimestamp(offset=1, timestamp=2, leader_epoch=None)
    b = OffsetAndTimestamp(offset=1, timestamp=2, leader_epoch=-1)
    assert a != b
    c = OffsetAndTimestamp(offset=1, timestamp=2, leader_epoch=-1)
    assert b == c
    assert hash(b) == hash(c)


# --------------------------------------------------------------------------- #
# ConsumerGroupMetadata — translated from ConsumerGroupMetadataTest
# --------------------------------------------------------------------------- #


def test_group_metadata_assignment_constructor() -> None:
    gm = ConsumerGroupMetadata(group_id="group", generation_id=2,
                               member_id="member", group_instance_id="instance")
    assert gm.group_id() == "group"
    assert gm.generation_id() == 2
    assert gm.member_id() == "member"
    assert gm.group_instance_id() == "instance"


def test_group_metadata_group_id_constructor_defaults() -> None:
    gm = ConsumerGroupMetadata(group_id="group")
    assert gm.group_id() == "group"
    assert gm.generation_id() == -1  # JoinGroupRequest.UNKNOWN_GENERATION_ID
    assert gm.member_id() == ""      # JoinGroupRequest.UNKNOWN_MEMBER_ID
    assert gm.group_instance_id() is None


def test_group_metadata_invalid_group_id() -> None:
    with pytest.raises(TypeError):
        ConsumerGroupMetadata(group_id=None, generation_id=2,  # type: ignore[arg-type]
                              member_id="member")


def test_group_metadata_invalid_member_id() -> None:
    with pytest.raises(TypeError):
        ConsumerGroupMetadata(group_id="group", generation_id=2,
                              member_id=None)  # type: ignore[arg-type]


def test_group_metadata_equality_and_repr() -> None:
    a = ConsumerGroupMetadata(group_id="g", generation_id=1, member_id="m")
    b = ConsumerGroupMetadata(group_id="g", generation_id=1, member_id="m")
    assert a == b
    assert hash(a) == hash(b)
    assert repr(a) == (
        "GroupMetadata(groupId = g, generationId = 1, memberId = m, "
        "groupInstanceId = )"
    )


# --------------------------------------------------------------------------- #
# CloseOptions — translated from CloseOptionsTest
# --------------------------------------------------------------------------- #


def test_close_options_operation_should_not_be_null() -> None:
    with pytest.raises(TypeError):
        CloseOptions.group_membership_operation(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        CloseOptions.timeout(0.0).with_group_membership_operation(
            None)  # type: ignore[arg-type]


def test_close_options_operation_default_value() -> None:
    assert (CloseOptions.timeout(0.0).group_membership_operation()
            is _GMO.DEFAULT)


def test_close_options_timeout_could_be_null() -> None:
    opts = CloseOptions.timeout(None)
    assert opts.timeout() is None


def test_close_options_timeout_default_empty() -> None:
    assert CloseOptions.group_membership_operation(_GMO.DEFAULT).timeout() is None


def test_close_options_constructor_raises() -> None:
    with pytest.raises(TypeError):
        CloseOptions()


def test_close_options_fluent_chain() -> None:
    opts = (CloseOptions.timeout(1.5)
            .with_group_membership_operation(_GMO.LEAVE_GROUP))
    assert opts.timeout() == 1.5
    assert opts.group_membership_operation() is _GMO.LEAVE_GROUP


# --------------------------------------------------------------------------- #
# SubscriptionPattern
# --------------------------------------------------------------------------- #


def test_subscription_pattern() -> None:
    sp = SubscriptionPattern(pattern="topic-.*")
    assert sp.pattern() == "topic-.*"
    assert str(sp) == "topic-.*"
    assert sp == SubscriptionPattern(pattern="topic-.*")
    assert hash(sp) == hash(SubscriptionPattern(pattern="topic-.*"))
    assert sp != SubscriptionPattern(pattern="other")


def test_subscription_pattern_keyword_only() -> None:
    with pytest.raises(TypeError):
        SubscriptionPattern("x")  # type: ignore[misc, call-arg]


# --------------------------------------------------------------------------- #
# OffsetResetStrategy
# --------------------------------------------------------------------------- #


def test_offset_reset_strategy_members_and_str() -> None:
    assert list(OffsetResetStrategy) == [
        OffsetResetStrategy.LATEST,
        OffsetResetStrategy.EARLIEST,
        OffsetResetStrategy.NONE,
    ]
    assert str(OffsetResetStrategy.EARLIEST) == "earliest"
    assert str(OffsetResetStrategy.LATEST) == "latest"
    assert str(OffsetResetStrategy.NONE) == "none"


# --------------------------------------------------------------------------- #
# ConsumerRecord — translated from ConsumerRecordTest
# --------------------------------------------------------------------------- #


def test_consumer_record_short_constructor() -> None:
    r: ConsumerRecord[str, str] = ConsumerRecord(
        topic="topic", partition=0, offset=23, key="key", value="value")
    assert r.topic() == "topic"
    assert r.partition() == 0
    assert r.offset() == 23
    assert r.key() == "key"
    assert r.value() == "value"
    assert r.timestamp_type() is TimestampType.NO_TIMESTAMP_TYPE
    assert r.timestamp() == -1                # ConsumerRecord.NO_TIMESTAMP
    assert r.serialized_key_size() == -1      # NULL_SIZE
    assert r.serialized_value_size() == -1
    assert r.leader_epoch() is None
    assert r.delivery_count() is None
    assert r.headers() == ()


def test_consumer_record_long_constructor() -> None:
    headers = [("header key", b"header value")]
    r: ConsumerRecord[str, str] = ConsumerRecord(
        topic="topic", partition=0, offset=23, timestamp=23434217432432,
        timestamp_type=TimestampType.CREATE_TIME, serialized_key_size=100,
        serialized_value_size=1142, key="key", value="value", headers=headers)
    assert r.topic() == "topic"
    assert r.offset() == 23
    assert r.timestamp_type() is TimestampType.CREATE_TIME
    assert r.timestamp() == 23434217432432
    assert r.serialized_key_size() == 100
    assert r.serialized_value_size() == 1142
    assert r.leader_epoch() is None
    assert r.delivery_count() is None
    assert r.headers() == (("header key", b"header value"),)

    r2: ConsumerRecord[str, str] = ConsumerRecord(
        topic="topic", partition=0, offset=23, timestamp=23434217432432,
        timestamp_type=TimestampType.CREATE_TIME, serialized_key_size=100,
        serialized_value_size=1142, key="key", value="value", headers=headers,
        leader_epoch=10, delivery_count=1)
    assert r2.leader_epoch() == 10
    assert r2.delivery_count() == 1


def test_consumer_record_null_topic_and_headers() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        ConsumerRecord(topic=None, partition=0, offset=0,  # type: ignore[arg-type]
                       key=None, value=None)
    assert "Topic cannot be null" in str(exc.value)
    with pytest.raises(IllegalArgumentError) as exc:
        ConsumerRecord(topic="t", partition=0, offset=0, key=None, value=None,
                       headers=None)  # type: ignore[arg-type]
    assert "Headers cannot be null" in str(exc.value)


# --------------------------------------------------------------------------- #
# ConsumerRecords — translated from ConsumerRecordsTest
# --------------------------------------------------------------------------- #


def _build_test_records(record_size: int, partition_size: int,
                        empty_partition_index: int,
                        topics: list[str]) -> ConsumerRecords[int, str]:
    partition_to_records: dict[
        TopicPartition, list[ConsumerRecord[int, str]]] = {}
    next_offsets: dict[TopicPartition, OffsetAndMetadata] = {}
    for topic in topics:
        for i in range(partition_size):
            recs: list[ConsumerRecord[int, str]] = []
            if i != empty_partition_index:
                for j in range(record_size):
                    recs.append(ConsumerRecord(
                        topic=topic, partition=i, offset=j, timestamp=0,
                        timestamp_type=TimestampType.CREATE_TIME,
                        serialized_key_size=0, serialized_value_size=0,
                        key=j, value=str(j)))
            tp = TopicPartition(topic=topic, partition=i)
            partition_to_records[tp] = recs
            next_offsets[tp] = OffsetAndMetadata(offset=record_size,
                                                 metadata="")
    return ConsumerRecords(records=partition_to_records,
                           next_offsets=next_offsets)


def test_consumer_records_iterator() -> None:
    topic = "topic"
    record_size, partition_size, empty_idx = 10, 15, 3
    records = _build_test_records(record_size, partition_size, empty_idx,
                                  [topic])
    record_count = 0
    partition_count = 0
    current_partition = -1
    for record in records:
        assert record.partition() != empty_idx
        if current_partition != record.partition():
            partition_count += 1
            current_partition = record.partition()
        assert record.topic() == topic
        assert record.offset() == record_count % record_size
        assert record.key() == record_count % record_size
        assert record.value() == str(record_count % record_size)
        record_count += 1
    assert partition_size == partition_count + 1  # including empty partition


def test_consumer_records_by_partition() -> None:
    topics = ["topic1", "topic2"]
    record_size, partition_size, empty_idx = 3, 5, 2
    cr = _build_test_records(record_size, partition_size, empty_idx, topics)
    assert len(cr.next_offsets()) == partition_size * len(topics)
    for topic in topics:
        for partition in range(partition_size):
            tp = TopicPartition(topic=topic, partition=partition)
            recs = cr.records(partition=tp)
            if partition == empty_idx:
                assert recs == []
            else:
                assert len(recs) == record_size
                last = recs[record_size - 1]
                assert cr.next_offsets()[tp] == OffsetAndMetadata(
                    offset=last.offset() + 1, leader_epoch=last.leader_epoch(),
                    metadata="")


def test_consumer_records_by_null_topic_raises() -> None:
    # Java records(null) raises IllegalArgumentException "Topic must be
    # non-null.". In the collapsed Python API, records(topic=None) is
    # indistinguishable from "no argument given", so the combination check
    # (_args.exactly_one) raises IllegalArgumentError naming both alternatives.
    cr: ConsumerRecords[int, str] = ConsumerRecords.empty()
    with pytest.raises(IllegalArgumentError) as exc:
        cr.records(topic=None)  # type: ignore[arg-type]
    assert "takes exactly one of partition, topic" in str(exc.value)


def test_consumer_records_by_topic() -> None:
    topics = ["topic1", "topic2", "topic3", "topic4"]
    record_size, partition_size, empty_idx = 3, 10, 6
    cr = _build_test_records(record_size, partition_size, empty_idx, topics)
    expected_total = record_size * (partition_size - 1)
    assert len(cr.next_offsets()) == partition_size * len(topics)
    for topic in topics:
        recs = cr.records(topic=topic)
        record_count = 0
        partition_count = 0
        current_partition = -1
        for record in recs:
            assert record.partition() != empty_idx
            if current_partition != record.partition():
                partition_count += 1
                current_partition = record.partition()
            record_count += 1
        assert partition_size == partition_count + 1
        assert record_count == expected_total


def test_consumer_records_requires_exactly_one_query() -> None:
    tp = TopicPartition(topic="t", partition=0)
    cr: ConsumerRecords[int, str] = ConsumerRecords.empty()
    with pytest.raises(IllegalArgumentError):
        cr.records(partition=tp, topic="t")  # both
    with pytest.raises(IllegalArgumentError):
        cr.records()  # neither


def test_consumer_records_are_immutable() -> None:
    # The Java test asserts the returned collections are unmodifiable. In Python
    # records() returns a fresh list and partitions() a fresh set, so mutating
    # them cannot affect the ConsumerRecords; count is unchanged.
    topic = "topic"
    record_size, partition_size, empty_idx = 3, 6, 2
    tp = TopicPartition(topic=topic, partition=0)
    records = _build_test_records(record_size, partition_size, empty_idx,
                                  [topic])
    recs = records.records(partition=tp)
    recs.append(ConsumerRecord(topic=topic, partition=0, offset=0, key=0,
                               value="0"))
    parts = records.partitions()
    parts.add(TopicPartition(topic=topic, partition=99))
    assert len(records) == record_size * (partition_size - 1)
    assert TopicPartition(topic=topic, partition=99) not in records.partitions()


def test_consumer_records_empty() -> None:
    empty: ConsumerRecords[int, str] = ConsumerRecords.empty()
    assert len(empty) == 0
    assert empty.is_empty() is True
    assert empty.partitions() == set()
    assert empty.next_offsets() == {}


_LOGGER_NAME = "confluent_kafka.consumer.consumer_records"


def test_next_offsets_logs_error_periodically_when_tainted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Java testNextOffsetsLogsErrorPeriodicallyWhenConstructedWithDeprecated
    # Constructor: the tainted (records-only) instance returns an empty map and
    # logs a rate-limited ERROR — exactly once per interval, not once per call.
    import confluent_kafka.consumer.consumer_records as cr_mod

    tp = TopicPartition(topic="topic", partition=0)
    rec = ConsumerRecord(topic="topic", partition=0, offset=0, key=0,
                         value="value")
    records = {tp: [rec]}

    previous = cr_mod._tainted_next_offsets_last_log_s
    try:
        # Force the rate-limit window to have elapsed so the next tainted call
        # logs (Java sets the AtomicLong one interval into the past).
        cr_mod._tainted_next_offsets_last_log_s = (
            cr_mod._TAINT_LOG_INTERVAL_S * -2)

        with caplog.at_level("ERROR", logger=_LOGGER_NAME):
            cr: ConsumerRecords[int, str] = ConsumerRecords(records=records)
            # Deprecated constructor supplies no next offsets -> empty map.
            assert cr.next_offsets() == {}

            errors = [r for r in caplog.records if r.levelname == "ERROR"]
            assert len(errors) == 1
            assert "deprecated records-only" in errors[0].getMessage()

            # Within the window, neither repeated calls nor new tainted
            # instances log again.
            assert cr.next_offsets() == {}
            assert ConsumerRecords(records=records).next_offsets() == {}
            assert len([r for r in caplog.records
                        if r.levelname == "ERROR"]) == 1

            # Once the window has elapsed, the error is logged again.
            cr_mod._tainted_next_offsets_last_log_s = (
                cr_mod._TAINT_LOG_INTERVAL_S * -2)
            assert cr.next_offsets() == {}
            assert len([r for r in caplog.records
                        if r.levelname == "ERROR"]) == 2
    finally:
        cr_mod._tainted_next_offsets_last_log_s = previous


def test_next_offsets_does_not_log_when_supplied(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Java testNextOffsetsDoesNotLogErrorWhenConstructedWithNextOffsets.
    tp = TopicPartition(topic="topic", partition=0)
    rec = ConsumerRecord(topic="topic", partition=0, offset=0, key=0,
                         value="value")
    next_offsets = {tp: OffsetAndMetadata(offset=1)}
    with caplog.at_level("ERROR", logger=_LOGGER_NAME):
        cr: ConsumerRecords[int, str] = ConsumerRecords(
            records={tp: [rec]}, next_offsets=next_offsets)
        assert cr.next_offsets() == next_offsets
        assert [r for r in caplog.records if r.levelname == "ERROR"] == []


def test_next_offsets_does_not_log_for_empty_records(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Java testNextOffsetsDoesNotLogErrorForEmptyRecords.
    with caplog.at_level("ERROR", logger=_LOGGER_NAME):
        assert ConsumerRecords.empty().next_offsets() == {}
        assert [r for r in caplog.records if r.levelname == "ERROR"] == []
