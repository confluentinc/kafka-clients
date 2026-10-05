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

"""``MockConsumer``: Java's ``MockConsumerTest`` in full (8 tests), then the
Java mock's other documented behaviours with Java's messages
(``MockConsumer.java``), each also on ``AsyncMockConsumer`` where the method
differs by async-ness, and the two ``consumer-threading.md`` §31 regression
tests on the mocks (a ``commit()`` inside ``on_partitions_revoked``; the
rebalance not advancing until the listener returns), sync and async."""

from __future__ import annotations

import asyncio
import threading
import warnings
from datetime import timedelta
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka.common import KafkaError, Node, PartitionInfo, TimestampType, TopicPartition
from confluent_kafka.common.errors import UnsupportedVersionError, WakeupError
from confluent_kafka.consumer import (
    AsyncMockConsumer, CloseOptions, ConsumerRebalanceListener, ConsumerRecord, MockConsumer,
    NoOffsetForPartitionError, OffsetAndMetadata, OffsetOutOfRangeError, SubscriptionPattern,
)

WAIT = 5.0


def tp(topic: str, partition: int) -> TopicPartition:
    return TopicPartition(topic=topic, partition=partition)


def record(topic: str, partition: int, offset: int, key: Any = None,
           value: Any = None, leader_epoch: int | None = None) -> ConsumerRecord[Any, Any]:
    return ConsumerRecord(topic=topic, partition=partition, offset=offset, timestamp=0,
                          timestamp_type=TimestampType.CREATE_TIME, serialized_key_size=0,
                          serialized_value_size=0, key=key, value=value, headers=(),
                          leader_epoch=leader_epoch)


def earliest() -> MockConsumer[str, str]:
    # Java: new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name())
    c: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    return c


# ---------------------------------------------------------------------------
# MockConsumerTest
# ---------------------------------------------------------------------------
def test_simple_mock() -> None:
    consumer = earliest()
    consumer.subscribe(topics=["test"])
    assert len(consumer.poll(timeout=0)) == 0
    consumer.rebalance(new_assignment=[tp("test", 0), tp("test", 1)])
    # Mock consumers need to seek manually since they cannot automatically reset offsets
    consumer.update_beginning_offsets(new_offsets={tp("test", 0): 0, tp("test", 1): 0})
    consumer.seek(partition=tp("test", 0), offset=0)
    rec1 = record("test", 0, 0, "key1", "value1")
    rec2 = record("test", 0, 1, "key2", "value2")
    consumer.add_record(record=rec1)
    consumer.add_record(record=rec2)
    recs = consumer.poll(timeout=timedelta(milliseconds=1))
    it = iter(recs)
    assert next(it) is rec1
    assert next(it) is rec2
    assert next(it, None) is None
    partition = tp("test", 0)
    assert consumer.position(partition=partition) == 2
    assert len(recs.next_offsets()) == 1
    assert recs.next_offsets()[partition] == OffsetAndMetadata(offset=2, leader_epoch=None,
                                                               metadata="")
    consumer.commit()
    committed = consumer.committed(partitions={partition})[partition]
    assert committed is not None and committed.offset() == 2


def test_consumer_records_is_empty_when_returning_no_records() -> None:
    consumer = earliest()
    partition = tp("test", 0)
    consumer.assign(partitions={partition})
    consumer.add_record(record=ConsumerRecord(topic="test", partition=0, offset=0, key=None,
                                              value=None))
    consumer.update_end_offsets(new_offsets={partition: 1})
    consumer.seek_to_end(partitions={partition})
    records = consumer.poll(timeout=timedelta(milliseconds=1))
    assert len(records) == 0
    assert records.is_empty()


def test_should_not_clear_records_for_paused_partitions() -> None:
    consumer = earliest()
    partition0 = tp("test", 0)
    test_partition_list = [partition0]
    consumer.assign(partitions=test_partition_list)
    consumer.add_record(record=ConsumerRecord(topic="test", partition=0, offset=0, key=None,
                                              value=None))
    consumer.update_beginning_offsets(new_offsets={partition0: 0})
    consumer.seek_to_beginning(partitions=test_partition_list)

    consumer.pause(partitions=test_partition_list)
    consumer.poll(timeout=timedelta(milliseconds=1))
    consumer.resume(partitions=test_partition_list)
    records_second_poll = consumer.poll(timeout=timedelta(milliseconds=1))
    assert len(records_second_poll) == 1
    assert len(records_second_poll.next_offsets()) == 1
    assert records_second_poll.next_offsets()[tp("test", 0)] == OffsetAndMetadata(
        offset=1, leader_epoch=None, metadata="")


def test_end_offsets_should_be_idempotent() -> None:
    consumer = earliest()
    partition = tp("test", 0)
    consumer.update_end_offsets(new_offsets={partition: 10})
    # consumer.end_offsets should NOT change the value of end offsets
    for _ in range(3):
        assert consumer.end_offsets(partitions={partition})[partition] == 10
    consumer.update_end_offsets(new_offsets={partition: 11})
    for _ in range(3):
        assert consumer.end_offsets(partitions={partition})[partition] == 11


def test_duration_based_offset_reset() -> None:
    consumer: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="by_duration:PT1H")
    consumer.subscribe(topics=["test"])
    consumer.rebalance(new_assignment=[tp("test", 0), tp("test", 1)])
    consumer.update_duration_offsets(new_offsets={tp("test", 0): 10, tp("test", 1): 11})
    rec1 = record("test", 0, 10, "key1", "value1")
    rec2 = record("test", 0, 11, "key2", "value2")
    consumer.add_record(record=rec1)
    consumer.add_record(record=rec2)
    records = consumer.poll(timeout=timedelta(milliseconds=1))
    it = iter(records)
    assert next(it) is rec1
    assert next(it) is rec2
    assert next(it, None) is None


def test_rebalance_listener() -> None:
    consumer = earliest()
    revoked: list[TopicPartition] = []
    assigned: list[TopicPartition] = []

    class Listener(ConsumerRebalanceListener):
        def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
            revoked.clear()
            revoked.extend(partitions)

        def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
            if not partitions:
                return
            assigned.clear()
            assigned.extend(partitions)

    consumer.subscribe(topics=["test"], callback=Listener())
    assert len(consumer.poll(timeout=0)) == 0
    topic_partition_list = [tp("test", 0), tp("test", 1)]
    consumer.rebalance(new_assignment=topic_partition_list)

    assert revoked == []
    assert len(assigned) == 2
    assert topic_partition_list[0] in assigned
    assert topic_partition_list[1] in assigned

    consumer.rebalance(new_assignment=[])
    assert len(assigned) == 2
    assert topic_partition_list[0] in revoked
    assert topic_partition_list[1] in revoked

    consumer.rebalance(new_assignment=[topic_partition_list[0]])
    assert len(assigned) == 1
    assert topic_partition_list[0] in assigned

    consumer.rebalance(new_assignment=[topic_partition_list[1]])
    assert len(assigned) == 1
    assert topic_partition_list[1] in assigned
    assert len(revoked) == 1
    assert topic_partition_list[0] in revoked


def test_re2j_pattern_subscription() -> None:
    consumer = earliest()
    # Java: subscribe((SubscriptionPattern) null) throws IllegalArgumentException;
    # an omitted pattern matches no Java overload here (java_forms).
    with pytest.raises(IllegalArgumentError):
        consumer.subscribe(pattern=None)
    with pytest.raises(IllegalArgumentError) as empty:
        consumer.subscribe(pattern=SubscriptionPattern(pattern=""))
    assert str(empty.value) == "Topic pattern cannot be empty"

    pattern = SubscriptionPattern(pattern="t.*")
    # Java's subscribe(pattern, null) throws "RebalanceListener cannot be null";
    # a None callback reads as not given, which is Java's subscribe(pattern).

    consumer.subscribe(pattern=pattern)
    assert consumer.subscription() == set()
    # Check that the subscription to pattern was successfully applied in the
    # mock consumer (using a different subscription type should fail)
    with pytest.raises(IllegalStateError) as mixed:
        consumer.subscribe(topics=["topic1"])
    assert str(mixed.value) == "Subscription to topics, partitions and pattern are mutually exclusive"


def test_should_return_max_poll_records() -> None:
    consumer = earliest()
    partition = tp("test", 0)
    consumer.assign(partitions={partition})
    consumer.update_beginning_offsets(new_offsets={partition: 0})
    for offset in range(10):
        consumer.add_record(record=ConsumerRecord(topic="test", partition=0, offset=offset,
                                                  key=None, value=None))
    consumer.set_max_poll_records(max_poll_records=2)
    assert len(consumer.poll(timeout=timedelta(milliseconds=1))) == 2
    assert len(consumer.poll(timeout=timedelta(milliseconds=1))) == 2
    consumer.set_max_poll_records(max_poll_records=(1 << 63) - 1)
    assert len(consumer.poll(timeout=timedelta(milliseconds=1))) == 6
    assert consumer.poll(timeout=timedelta(milliseconds=1)).is_empty()


# ---------------------------------------------------------------------------
# The Java mock's other behaviours
# ---------------------------------------------------------------------------
def test_constructor_takes_the_strategy_name() -> None:
    # Java's MockConsumer(String); the @Deprecated MockConsumer(OffsetResetStrategy)
    # is not generated (CLAUDE.md, Python Binding Conventions, Class family).
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        MockConsumer(offset_reset_strategy="latest")
        consumer: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy="earliest")
    consumer.assign(partitions=[tp("t", 0)])
    consumer.update_beginning_offsets(new_offsets={tp("t", 0): 3})
    assert consumer.position(partition=tp("t", 0)) == 3


@pytest.mark.parametrize("strategy, message", [
    (None, "Auto offset reset strategy is null"),
    ("by_duration", "<:duration> part is missing in by_duration auto offset reset strategy."),
    ("EARLIEST", "Unknown auto offset reset strategy: EARLIEST"),
    ("by_duration:1H", "Unable to parse duration string in by_duration offset reset strategy."),
    ("by_duration:-PT1H", "Unable to parse duration string in by_duration offset reset strategy."),
    ("by_duration:PT", "Unable to parse duration string in by_duration offset reset strategy."),
])
def test_constructor_rejects_an_invalid_strategy(strategy: Any, message: str) -> None:
    # AutoOffsetResetStrategy.fromString's messages.
    with pytest.raises(IllegalArgumentError) as e:
        MockConsumer(offset_reset_strategy=strategy)
    assert str(e.value) == message


def test_by_duration_accepts_java_duration_forms() -> None:
    for text in ("by_duration:PT1H", "by_duration:P1DT2H3M4.5S", "by_duration:pt0.000000001s",
                 "by_duration:P2D", "by_duration:PT-0S"):
        MockConsumer(offset_reset_strategy=text)


def test_add_record_needs_an_assigned_partition() -> None:
    consumer = earliest()
    with pytest.raises(IllegalStateError) as e:
        consumer.add_record(record=record("test", 0, 0))
    assert str(e.value) == "Cannot add records for a partition that is not assigned to the consumer"


def test_poll_returns_the_records_added_undeserialized_with_their_fields() -> None:
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    consumer.update_beginning_offsets(new_offsets={partition: 0})
    rec = ConsumerRecord(topic="t", partition=0, offset=0, timestamp=123,
                         timestamp_type=TimestampType.LOG_APPEND_TIME, serialized_key_size=3,
                         serialized_value_size=5, key={"k": 1}, value=[1, 2],
                         headers=(("h", b"v"),), leader_epoch=7)
    consumer.add_record(record=rec)
    polled = list(consumer.poll(timeout=0))
    assert polled == [rec] and polled[0] is rec
    # The record's leader epoch reaches the position, as in Java.
    assert consumer.poll(timeout=0).is_empty()
    consumer.commit()
    committed = consumer.committed(partitions=[partition])[partition]
    assert committed == OffsetAndMetadata(offset=1, leader_epoch=7, metadata="")


def test_poll_skips_records_below_the_position_and_keeps_them() -> None:
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    consumer.update_beginning_offsets(new_offsets={partition: 0})
    consumer.seek(partition=partition, offset=5)
    stale, fresh = record("t", 0, 3), record("t", 0, 5)
    consumer.add_record(record=stale)
    consumer.add_record(record=fresh)
    assert list(consumer.poll(timeout=0)) == [fresh]
    consumer.seek(partition=partition, offset=0)
    # The stale record stayed buffered (Java only removes what it returns).
    assert list(consumer.poll(timeout=0)) == [stale]


def test_poll_out_of_range_when_beginning_is_past_the_position() -> None:
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    consumer.update_beginning_offsets(new_offsets={partition: 0})
    consumer.seek(partition=partition, offset=0)
    consumer.update_beginning_offsets(new_offsets={partition: 5})
    consumer.add_record(record=record("t", 0, 6))
    with pytest.raises(OffsetOutOfRangeError) as e:
        consumer.poll(timeout=0)
    assert e.value.offset_out_of_range_partitions() == {partition: 0}
    assert str(e.value) == ("Offsets out of range with no configured reset policy for "
                            "partitions: {t-0=0}")


@pytest.mark.parametrize("strategy, message", [
    ("earliest", "MockConsumer didn't have beginning offset specified, but tried to seek to "
                 "beginning"),
    ("latest", "MockConsumer didn't have end offset specified, but tried to seek to end"),
    ("by_duration:PT1M", "MockConsumer didn't have duration offset specified, but tried to seek "
                         "to timestamp"),
])
def test_reset_without_an_offset(strategy: str, message: str) -> None:
    consumer: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy=strategy)
    consumer.assign(partitions=[tp("t", 0)])
    with pytest.raises(IllegalStateError) as e:
        consumer.poll(timeout=0)
    assert str(e.value) == message


def test_reset_strategy_none_raises_no_offset_for_partition() -> None:
    consumer: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy="none")
    consumer.assign(partitions=[tp("t", 0)])
    with pytest.raises(NoOffsetForPartitionError) as e:
        consumer.position(partition=tp("t", 0))
    assert e.value.partitions() == {tp("t", 0)}


def test_committed_offset_is_the_reset_position() -> None:
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    consumer.commit(offsets={partition: OffsetAndMetadata(offset=4)})
    # updateFetchPosition seeks to the committed offset.
    assert consumer.position(partition=partition) == 4


def test_committed_reports_only_committed_partitions_and_zero_when_unassigned() -> None:
    consumer = earliest()
    p0, p1 = tp("t", 0), tp("t", 1)
    consumer.assign(partitions=[p0, p1])
    consumer.commit(offsets={p0: OffsetAndMetadata(offset=4)})
    assert consumer.committed(partitions=[p0, p1]) == {p0: OffsetAndMetadata(offset=4)}
    # A reassignment clears the committed offsets (Java's committed.clear()).
    consumer.assign(partitions=[p0])
    assert consumer.committed(partitions=[p0]) == {}


def test_position_of_an_unassigned_partition() -> None:
    consumer = earliest()
    with pytest.raises(IllegalArgumentError) as e:
        consumer.position(partition=tp("t", 0))
    assert str(e.value) == ("You can only check the position for partitions assigned to this "
                            "consumer.")


def test_seek_of_an_unassigned_partition() -> None:
    consumer = earliest()
    with pytest.raises(IllegalStateError) as e:
        consumer.seek(partition=tp("t", 0), offset=1)
    assert str(e.value) == "No current assignment for partition t-0"
    with pytest.raises(IllegalStateError):
        consumer.seek_to_beginning(partitions=[tp("t", 0)])
    with pytest.raises(IllegalStateError):
        consumer.pause(partitions=[tp("t", 0)])


def test_seek_with_offset_and_metadata_uses_its_offset() -> None:
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    consumer.seek(partition=partition, offset_and_metadata=OffsetAndMetadata(offset=9))
    assert consumer.position(partition=partition) == 9


def test_missing_beginning_and_end_offsets() -> None:
    consumer = earliest()
    with pytest.raises(IllegalStateError) as b:
        consumer.beginning_offsets(partitions=[tp("t", 0)])
    assert str(b.value) == "The partition t-0 does not have a beginning offset."
    with pytest.raises(IllegalStateError) as e:
        consumer.end_offsets(partitions=[tp("t", 0)])
    assert str(e.value) == "The partition t-0 does not have an end offset."


def test_set_max_poll_records_rejects_non_positive() -> None:
    consumer = earliest()
    with pytest.raises(IllegalArgumentError) as e:
        consumer.set_max_poll_records(max_poll_records=0)
    assert str(e.value) == "MaxPollRecords must be strictly superior to 0"


def test_set_poll_exception_raises_the_injected_instance_once() -> None:
    consumer = earliest()
    injected = KafkaError(message="boom")
    consumer.set_poll_exception(exception=injected)
    with pytest.raises(KafkaError) as e:
        consumer.poll(timeout=0)
    assert e.value is injected
    assert consumer.poll(timeout=0).is_empty()


def test_set_poll_exception_none_clears_a_pending_one() -> None:
    # Java: setPollException(null) clears the field (MockConsumer.java:344-346).
    consumer = earliest()
    consumer.set_poll_exception(exception=IllegalStateError(message="boom"))  # type: ignore[arg-type]
    consumer.set_poll_exception(exception=None)
    assert consumer.poll(timeout=0).is_empty()


def test_set_offsets_exception_raises_the_injected_instance_and_clears() -> None:
    consumer = earliest()
    partition = tp("t", 0)
    consumer.update_end_offsets(new_offsets={partition: 3})
    injected = KafkaError(message="offsets")
    consumer.set_offsets_exception(exception=injected)
    with pytest.raises(KafkaError) as e:
        consumer.beginning_offsets(partitions=[partition])
    assert e.value is injected
    assert consumer.end_offsets(partitions=[partition]) == {partition: 3}
    consumer.set_offsets_exception(exception=injected)
    consumer.set_offsets_exception(exception=None)
    assert consumer.end_offsets(partitions=[partition]) == {partition: 3}


def test_offsets_for_times_is_not_implemented() -> None:
    consumer = earliest()
    with pytest.raises(UnsupportedVersionError) as e:
        consumer.offsets_for_times(timestamps_to_search={tp("t", 0): 0})
    assert str(e.value) == "Not implemented yet."


def test_partitions_for_and_list_topics() -> None:
    consumer = earliest()
    node = Node(id=1, host="h", port=9092)
    infos = [PartitionInfo(topic="t", partition=0, leader=node, replicas=(node,),
                           in_sync_replicas=(node,))]
    assert consumer.partitions_for(topic="t") == []
    consumer.update_partitions(topic="t", partitions=infos)
    assert consumer.partitions_for(topic="t") == infos
    assert consumer.list_topics() == {"t": infos}


def test_current_lag() -> None:
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    consumer.update_beginning_offsets(new_offsets={partition: 0})
    # No end offset: the test models being caught up.
    assert consumer.current_lag(topic_partition=partition) == 0
    consumer.update_end_offsets(new_offsets={partition: 10})
    consumer.seek(partition=partition, offset=4)
    assert consumer.current_lag(topic_partition=partition) == 6


def test_group_metadata_and_metrics() -> None:
    consumer = earliest()
    metadata = consumer.group_metadata()
    assert (metadata.group_id(), metadata.generation_id(), metadata.member_id(),
            metadata.group_instance_id()) == ("dummy.group.id", 1, "1", None)
    assert consumer.metrics() == {}


def test_paused_keeps_the_mocks_own_set() -> None:
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    consumer.pause(partitions=[partition])
    assert consumer.paused() == {partition}
    consumer.resume(partitions=[partition])
    assert consumer.paused() == set()


def test_wakeup_is_consumed_by_the_next_poll() -> None:
    consumer = earliest()
    consumer.assign(partitions=[tp("t", 0)])
    consumer.update_beginning_offsets(new_offsets={tp("t", 0): 0})
    consumer.wakeup()
    with pytest.raises(WakeupError):
        consumer.poll(timeout=0)
    assert consumer.poll(timeout=0).is_empty()


def test_schedule_poll_task_runs_one_task_per_poll() -> None:
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    consumer.update_beginning_offsets(new_offsets={partition: 0})
    added = record("t", 0, 0, "k", "v")
    runs: list[str] = []

    def add() -> None:
        runs.append("add")
        consumer.add_record(record=added)
        # A task may schedule the next one.
        consumer.schedule_poll_task(task=lambda: runs.append("next"))

    consumer.schedule_poll_task(task=add)
    consumer.schedule_nop_poll_task()
    assert list(consumer.poll(timeout=0)) == [added]
    assert runs == ["add"]
    consumer.poll(timeout=0)  # the no-op task
    assert runs == ["add"]
    consumer.poll(timeout=0)
    assert runs == ["add", "next"]


def test_schedule_poll_task_from_another_thread() -> None:
    # The Java class comment: a driver thread schedules tasks that run inside
    # a poll() on another thread.
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    consumer.update_beginning_offsets(new_offsets={partition: 0})
    ran = threading.Event()
    consumer.schedule_poll_task(task=ran.set)
    worker = threading.Thread(target=lambda: consumer.poll(timeout=0))
    worker.start()
    worker.join(WAIT)
    assert ran.is_set()


def test_last_poll_timeout() -> None:
    consumer = earliest()
    consumer.assign(partitions=[tp("t", 0)])
    consumer.update_beginning_offsets(new_offsets={tp("t", 0): 0})
    assert consumer.last_poll_timeout() is None
    consumer.poll(timeout=timedelta(milliseconds=250))
    assert consumer.last_poll_timeout() == 0.25
    consumer.poll(timeout=3)
    assert consumer.last_poll_timeout() == 3.0


def test_close_marks_closed_and_later_calls_raise() -> None:
    consumer = earliest()
    assert consumer.closed() is False
    consumer.close()
    assert consumer.closed() is True
    consumer.close()
    with pytest.raises(IllegalStateError) as e:
        consumer.poll(timeout=0)
    assert str(e.value) == "This consumer has already been closed."
    for call in (lambda: consumer.subscribe(topics=["t"]), consumer.unsubscribe,
                 lambda: consumer.assign(partitions=[]), consumer.metrics, consumer.list_topics,
                 lambda: consumer.committed(partitions=[]), lambda: consumer.commit(),
                 lambda: consumer.add_record(record=record("t", 0, 0))):
        with pytest.raises(IllegalStateError):
            call()


def test_close_with_a_negative_timeout_closes_the_mock() -> None:
    # Java's MockConsumer.close(CloseOptions) never reads the timeout.
    consumer = earliest()
    consumer.close(option=CloseOptions.timeout(-1))
    assert consumer.closed()


def test_close_releases_the_listener() -> None:
    import gc
    import weakref

    consumer = earliest()
    listener = ConsumerRebalanceListener()
    ref = weakref.ref(listener)
    consumer.subscribe(topics=["t"], callback=listener)
    del listener
    gc.collect()
    assert ref() is not None
    consumer.close()
    gc.collect()
    assert ref() is None


def test_rebalance_needs_a_subscription() -> None:
    consumer = earliest()
    consumer.assign(partitions=[tp("t", 0)])
    with pytest.raises(IllegalArgumentError) as e:
        consumer.rebalance(new_assignment=[tp("t", 0)])
    assert str(e.value) == ("Attempt to dynamically assign partitions while manual assignment "
                            "in use")


def test_rebalance_clears_the_buffered_records() -> None:
    consumer = earliest()
    consumer.subscribe(topics=["t"])
    consumer.rebalance(new_assignment=[tp("t", 0)])
    consumer.update_beginning_offsets(new_offsets={tp("t", 0): 0})
    consumer.add_record(record=record("t", 0, 0))
    consumer.rebalance(new_assignment=[tp("t", 0)])
    assert consumer.poll(timeout=0).is_empty()


def test_commit_nowait_callback_runs_inline_and_propagates() -> None:
    # Java's mock calls callback.onComplete(offsets, null) inside commitAsync,
    # and lets an exception it throws propagate.
    consumer = earliest()
    partition = tp("t", 0)
    consumer.assign(partitions=[partition])
    seen: list[Any] = []
    offsets = {partition: OffsetAndMetadata(offset=3)}
    consumer.commit_nowait(offsets=offsets,
                           callback=lambda o, e: seen.append((o, e, threading.get_ident())))
    assert seen == [(offsets, None, threading.get_ident())]
    assert consumer.committed(partitions=[partition]) == offsets

    def boom(o: Any, e: Any) -> None:
        raise ValueError("callback")

    with pytest.raises(ValueError):
        consumer.commit_nowait(callback=boom)


def test_listener_may_call_back_into_the_mock() -> None:
    # Java's synchronized methods are reentrant: a listener commits, seeks and
    # reads the assignment from inside the rebalance.
    consumer = earliest()
    seen: dict[str, Any] = {}

    class Listener(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
            seen["assignment"] = consumer.assignment()
            consumer.seek(partition=tp("t", 0), offset=4)
            consumer.commit(offsets={tp("t", 0): OffsetAndMetadata(offset=4)})
            seen["position"] = consumer.position(partition=tp("t", 0))

    consumer.subscribe(topics=["t"], callback=Listener())
    consumer.rebalance(new_assignment=[tp("t", 0)])
    assert seen == {"assignment": {tp("t", 0)}, "position": 4}
    assert consumer.committed(partitions=[tp("t", 0)]) == {tp("t", 0): OffsetAndMetadata(offset=4)}


def test_listener_exception_propagates_from_rebalance() -> None:
    consumer = earliest()

    class Boom(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
            raise ValueError("boom in assigned")

    consumer.subscribe(topics=["t"], callback=Boom())
    with pytest.raises(ValueError, match="boom in assigned"):
        consumer.rebalance(new_assignment=[tp("t", 0)])


def test_a_coroutine_listener_needs_the_async_mock() -> None:
    consumer = earliest()

    class Coroutine(ConsumerRebalanceListener):
        async def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:  # type: ignore[override]
            pass

    consumer.subscribe(topics=["t"], callback=Coroutine())
    with pytest.raises(TypeError) as e:
        consumer.rebalance(new_assignment=[tp("t", 0)])
    assert str(e.value) == "a coroutine rebalance listener requires an AsyncMockConsumer"


# ---------------------------------------------------------------------------
# consumer-threading.md §31 regression tests, on the mocks
# ---------------------------------------------------------------------------
def test_commit_inside_on_partitions_revoked_succeeds() -> None:
    consumer = earliest()
    partition = tp("t", 0)

    class CommitOnRevoke(ConsumerRebalanceListener):
        def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
            consumer.commit(offsets={partition: OffsetAndMetadata(offset=7)})

    consumer.subscribe(topics=["t"], callback=CommitOnRevoke())
    consumer.rebalance(new_assignment=[partition])
    consumer.rebalance(new_assignment=[])
    consumer.rebalance(new_assignment=[partition])
    assert consumer.committed(partitions=[partition]) == {partition: OffsetAndMetadata(offset=7)}


def test_rebalance_does_not_advance_until_the_listener_returns() -> None:
    consumer = earliest()
    entered, release, returned = threading.Event(), threading.Event(), threading.Event()

    class Blocking(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
            entered.set()
            assert release.wait(WAIT)

    consumer.subscribe(topics=["t"], callback=Blocking())

    def drive() -> None:
        consumer.rebalance(new_assignment=[tp("t", 0)])
        returned.set()

    worker = threading.Thread(target=drive)
    worker.start()
    try:
        assert entered.wait(WAIT)
        assert not returned.wait(0.3), "the rebalance returned while the listener ran"
        release.set()
        assert returned.wait(WAIT)
    finally:
        release.set()
        worker.join(WAIT)
    assert consumer.assignment() == {tp("t", 0)}


def test_async_commit_inside_an_async_on_partitions_revoked_succeeds() -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(offset_reset_strategy="earliest")
        partition = tp("t", 0)

        class CommitOnRevoke(ConsumerRebalanceListener):
            async def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:  # type: ignore[override]
                await asyncio.sleep(0)
                await consumer.commit(offsets={partition: OffsetAndMetadata(offset=7)})

        await consumer.subscribe(topics=["t"], callback=CommitOnRevoke())
        await consumer.rebalance(new_assignment=[partition])
        await consumer.rebalance(new_assignment=[])
        await consumer.rebalance(new_assignment=[partition])
        assert await consumer.committed(partitions=[partition]) == {
            partition: OffsetAndMetadata(offset=7)}
        await consumer.close()

    asyncio.run(main())


def test_async_rebalance_does_not_advance_until_the_listener_returns() -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(offset_reset_strategy="earliest")
        entered, release = asyncio.Event(), asyncio.Event()

        class Blocking(ConsumerRebalanceListener):
            async def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:  # type: ignore[override]
                entered.set()
                await release.wait()

        await consumer.subscribe(topics=["t"], callback=Blocking())
        task = asyncio.ensure_future(consumer.rebalance(new_assignment=[tp("t", 0)]))
        await asyncio.wait_for(entered.wait(), WAIT)
        await asyncio.sleep(0.2)
        assert not task.done(), "the rebalance returned while the listener ran"
        release.set()
        await asyncio.wait_for(task, WAIT)
        assert consumer.assignment() == {tp("t", 0)}

    asyncio.run(main())


def test_async_mock_mirrors_the_sync_surface() -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(offset_reset_strategy="earliest")
        partition = tp("t", 0)
        await consumer.assign(partitions=[partition])
        consumer.update_beginning_offsets(new_offsets={partition: 0})
        rec = record("t", 0, 0, "k", "v")
        consumer.add_record(record=rec)
        assert list(await consumer.poll(timeout=0)) == [rec]
        assert await consumer.position(partition=partition) == 1
        consumer.commit_nowait()
        assert await consumer.committed(partitions=[partition]) == {
            partition: OffsetAndMetadata(offset=1, leader_epoch=None, metadata="")}
        await consumer.pause(partitions=[partition])
        assert consumer.paused() == {partition}
        await consumer.resume(partitions=[partition])
        consumer.wakeup()
        with pytest.raises(WakeupError):
            await consumer.poll(timeout=0)
        with pytest.raises(UnsupportedVersionError):
            await consumer.offsets_for_times(timestamps_to_search={partition: 0})
        assert not hasattr(consumer, "current_lag")
        async with consumer:
            pass
        assert consumer.closed()

    asyncio.run(main())
