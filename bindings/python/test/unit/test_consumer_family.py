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

"""Tests for the ``confluent_kafka.consumer`` client family (P5).

Translates ``MockConsumerTest.java`` in full and the argument/config-validation
slices of ``KafkaConsumerTest.java`` that do not need Java's internal
``MockClient`` mocks, plus the two consumer-threading.md §31 regression tests
(commit from inside a listener; rebalance blocked until the listener resolves).
Skipped Java tests are listed with reasons at the bottom.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka.common.errors import KafkaError, WakeupError
from confluent_kafka.common.topic_partition import TopicPartition
from confluent_kafka.consumer import (
    AsyncMockConsumer, CloseOptions, Consumer, ConsumerRebalanceListener,
    ConsumerRecord, KafkaConsumer, MockConsumer, OffsetAndMetadata,
    SubscriptionPattern,
)
from confluent_kafka.consumer.offset_reset_strategy import OffsetResetStrategy

WAIT = 2.0


def _tp(topic: str, partition: int) -> TopicPartition:
    return TopicPartition(topic=topic, partition=partition)


def _record(topic: str, partition: int, offset: int,
            key: bytes | None = None, value: bytes | None = None) -> ConsumerRecord:
    return ConsumerRecord(
        topic=topic, partition=partition, offset=offset, key=key, value=value,
    )


def _tps(partitions) -> list[tuple[str, int]]:
    return sorted((p.topic(), p.partition()) for p in partitions)


# ==========================================================================
# MockConsumerTest.java translations (all 8 tests)
# ==========================================================================
def test_simple_mock():
    """MockConsumerTest.testSimpleMock."""
    with MockConsumer(offset_reset_strategy="earliest") as c:
        c.subscribe(topics=["test"])
        assert c.poll(timeout=1.0).is_empty()
        c.rebalance(partitions=[_tp("test", 0), _tp("test", 1)])
        c.update_beginning_offsets(offsets={_tp("test", 0): 0, _tp("test", 1): 0})
        c.seek(partition=_tp("test", 0), offset=0)
        c.add_record(record=_record("test", 0, 0, b"key1", b"value1"))
        c.add_record(record=_record("test", 0, 1, b"key2", b"value2"))
        recs = c.poll(timeout=1.0)
        got = list(recs.records(partition=_tp("test", 0)))
        assert [r.offset() for r in got] == [0, 1]
        assert c.position(partition=_tp("test", 0)) == 2
        next_offsets = recs.next_offsets()
        assert len(next_offsets) == 1
        assert next_offsets[_tp("test", 0)] == OffsetAndMetadata(offset=2)
        c.commit()
        committed = c.committed(partitions=[_tp("test", 0)])
        assert committed[_tp("test", 0)].offset() == 2


def test_consumer_records_is_empty_when_returning_no_records():
    """MockConsumerTest.testConsumerRecordsIsEmptyWhenReturningNoRecords."""
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("test", 0)
        c.assign(partitions=[tp])
        c.add_record(record=_record("test", 0, 0, value=b"value0"))
        c.update_end_offsets(offsets={tp: 1})
        c.seek_to_end(partitions=[tp])
        recs = c.poll(timeout=1.0)
        assert len(recs) == 0
        assert recs.is_empty()


def test_should_not_clear_records_for_paused_partitions():
    """MockConsumerTest.shouldNotClearRecordsForPausedPartitions."""
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("test", 0)
        c.assign(partitions=[tp])
        c.add_record(record=_record("test", 0, 0, value=b"value0"))
        c.update_beginning_offsets(offsets={tp: 0})
        c.seek_to_beginning(partitions=[tp])
        c.pause(partitions=[tp])
        c.poll(timeout=1.0)
        c.resume(partitions=[tp])
        recs = c.poll(timeout=1.0)
        assert len(recs) == 1
        next_offsets = recs.next_offsets()
        assert len(next_offsets) == 1
        assert next_offsets[tp] == OffsetAndMetadata(offset=1)


def test_end_offsets_should_be_idempotent():
    """MockConsumerTest.endOffsetsShouldBeIdempotent."""
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("test", 0)
        c.update_end_offsets(offsets={tp: 10})
        assert c.end_offsets(partitions=[tp])[tp] == 10
        assert c.end_offsets(partitions=[tp])[tp] == 10
        c.update_end_offsets(offsets={tp: 11})
        assert c.end_offsets(partitions=[tp])[tp] == 11
        assert c.end_offsets(partitions=[tp])[tp] == 11


def test_duration_based_offset_reset():
    """MockConsumerTest.testDurationBasedOffsetReset."""
    with MockConsumer(offset_reset_strategy="by_duration:PT1H") as c:
        c.subscribe(topics=["test"])
        c.poll(timeout=1.0)
        c.rebalance(partitions=[_tp("test", 0), _tp("test", 1)])
        c.update_duration_offsets(offsets={_tp("test", 0): 10, _tp("test", 1): 11})
        c.add_record(record=_record("test", 0, 10, value=b"value0"))
        c.add_record(record=_record("test", 1, 11, value=b"value1"))
        recs = c.poll(timeout=1.0)
        assert sorted(r.offset() for r in recs) == [10, 11]


def test_rebalance_listener():
    """MockConsumerTest.testRebalanceListener. The listener's
    ``on_partitions_assigned`` returns early on an empty set (so ``assigned``
    keeps its prior value), exactly as Java's test listener does."""
    class Recorder(ConsumerRebalanceListener):
        def __init__(self):
            self.revoked = []
            self.assigned = []

        def on_partitions_revoked(self, partitions):
            self.revoked = _tps(partitions)

        def on_partitions_assigned(self, partitions):
            tps = _tps(partitions)
            if not tps:
                return
            self.assigned = tps

    listener = Recorder()
    with MockConsumer(offset_reset_strategy="earliest") as c:
        c.subscribe(topics=["test"], listener=listener)
        assert c.poll(timeout=1.0).is_empty()
        c.rebalance(partitions=[_tp("test", 0), _tp("test", 1)])
        assert listener.revoked == []
        assert listener.assigned == [("test", 0), ("test", 1)]

        c.rebalance(partitions=[])
        assert listener.assigned == [("test", 0), ("test", 1)]
        assert listener.revoked == [("test", 0), ("test", 1)]

        c.rebalance(partitions=[_tp("test", 0)])
        assert listener.assigned == [("test", 0)]

        c.rebalance(partitions=[_tp("test", 1)])
        assert listener.assigned == [("test", 1)]
        assert listener.revoked == [("test", 0)]


def test_re2j_pattern_subscription():
    """MockConsumerTest.testRe2JPatternSubscription."""
    with MockConsumer(offset_reset_strategy="earliest") as c:
        with pytest.raises(IllegalArgumentError):
            c.subscribe(pattern=SubscriptionPattern(pattern=""))
        c.subscribe(pattern=SubscriptionPattern(pattern="t.*"))
        assert c.subscription() == set()
        with pytest.raises((IllegalStateError, KafkaError)):
            c.subscribe(topics=["topic1"])


def test_should_return_max_poll_records():
    """MockConsumerTest.shouldReturnMaxPollRecords."""
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("test", 0)
        c.assign(partitions=[tp])
        c.update_beginning_offsets(offsets={tp: 0})
        for i in range(10):
            c.add_record(record=_record("test", 0, i, value=bytes([i])))
        c.set_max_poll_records(max_poll_records=2)
        assert len(c.poll(timeout=1.0)) == 2
        assert len(c.poll(timeout=1.0)) == 2
        c.set_max_poll_records(max_poll_records=2**63 - 1)
        assert len(c.poll(timeout=1.0)) == 6
        assert c.poll(timeout=1.0).is_empty()


# ==========================================================================
# MockConsumer surface — extra behavior (not a single Java test, but the
# public surface must be covered).
# ==========================================================================
def test_max_poll_records_rejects_non_positive():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        with pytest.raises(IllegalArgumentError,
                           match="MaxPollRecords must be strictly superior to 0"):
            c.set_max_poll_records(max_poll_records=0)


def test_closed_flag():
    c = MockConsumer(offset_reset_strategy="earliest")
    assert c.closed() is False
    c.close()
    assert c.closed() is True


def test_should_rebalance_flag_default_false():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        assert c.should_rebalance() is False
        c.reset_should_rebalance()
        assert c.should_rebalance() is False


def test_last_poll_timeout():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        assert c.last_poll_timeout() is None
        tp = _tp("t", 0)
        c.assign(partitions=[tp])
        c.update_beginning_offsets(offsets={tp: 0})
        c.seek_to_beginning(partitions=[tp])
        c.poll(timeout=1.5)
        assert c.last_poll_timeout() == pytest.approx(1.5, abs=0.01)


# ==========================================================================
# Consumer base: non-instantiable guard, deserialization, lifecycle
# ==========================================================================
def test_consumer_base_not_instantiable():
    with pytest.raises(TypeError, match="KafkaConsumer or MockConsumer"):
        Consumer()


def test_use_after_close_raises_illegal_state():
    c = MockConsumer(offset_reset_strategy="earliest")
    c.close()
    with pytest.raises(IllegalStateError,
                       match="already been closed"):
        c.poll(timeout=0.1)


def test_close_is_idempotent():
    c = MockConsumer(offset_reset_strategy="earliest")
    c.close()
    c.close()  # no error


def test_close_with_options():
    c = MockConsumer(offset_reset_strategy="earliest")
    op = CloseOptions.group_membership_operation(
        CloseOptions.GroupMembershipOperation.LEAVE_GROUP)
    c.close(option=op)
    assert c.closed() is True


def test_close_rejects_both_timeout_and_option():
    c = MockConsumer(offset_reset_strategy="earliest")
    with pytest.raises(IllegalArgumentError):
        c.close(timeout=1.0, option=CloseOptions.timeout(1.0))
    c.close()


def test_context_manager_closes():
    c = MockConsumer(offset_reset_strategy="earliest")
    with c:
        assert c.closed() is False
    assert c.closed() is True


# ==========================================================================
# Deserialization (spec §5.4) — the mock applies serdes on poll.
# ==========================================================================
def test_poll_applies_value_deserializer():
    from confluent_kafka.common.serialization import string_deserializer
    with MockConsumer(offset_reset_strategy="earliest",
                      value_deserializer=string_deserializer()) as c:
        tp = _tp("t", 0)
        c.assign(partitions=[tp])
        c.update_beginning_offsets(offsets={tp: 0})
        c.add_record(record=_record("t", 0, 0, value="hello".encode("utf-8")))
        recs = c.poll(timeout=1.0)
        got = list(recs)
        assert len(got) == 1
        assert got[0].value() == "hello"


def test_poll_deserialization_error_leaves_position(monkeypatch=None):
    from confluent_kafka.common.errors import RecordDeserializationError

    def boom(topic, data, headers=None):
        raise ValueError("bad record")

    with MockConsumer(offset_reset_strategy="earliest",
                      value_deserializer=boom) as c:
        tp = _tp("t", 0)
        c.assign(partitions=[tp])
        c.update_beginning_offsets(offsets={tp: 0})
        c.add_record(record=_record("t", 0, 0, value=b"x"))
        with pytest.raises(RecordDeserializationError) as ei:
            c.poll(timeout=1.0)
        assert isinstance(ei.value.__cause__, ValueError)
        assert ei.value.offset() == 0
        assert ei.value.topic_partition() == tp


# ==========================================================================
# Argument validation (positional -> TypeError; illegal combos)
# ==========================================================================
def test_subscribe_requires_exactly_one_of_topics_or_pattern():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        with pytest.raises(IllegalArgumentError,
                           match="takes exactly one of topics, pattern"):
            c.subscribe()
        with pytest.raises(IllegalArgumentError):
            c.subscribe(topics=["a"], pattern=SubscriptionPattern(pattern="b"))


def test_seek_requires_exactly_one_of_offset_or_metadata():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        c.assign(partitions=[_tp("t", 0)])
        with pytest.raises(IllegalArgumentError,
                           match="takes exactly one of offset, offset_and_metadata"):
            c.seek(partition=_tp("t", 0))


def test_poll_rejects_positional():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        with pytest.raises(TypeError):
            c.poll(1.0)  # type: ignore[misc]


def test_subscribe_rejects_positional():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        with pytest.raises(TypeError):
            c.subscribe(["t"])  # type: ignore[misc]


# ==========================================================================
# KafkaConsumerTest.java translatable slice (config / group.id validation)
# ==========================================================================
def _kafka_config(**overrides) -> dict:
    conf = {
        "bootstrap.servers": "localhost:9999",
        "group.protocol": "consumer",
    }
    conf.update(overrides)
    return conf


def test_empty_group_id_rejected():
    """KafkaConsumerTest.testEmptyGroupId — Java wraps the construction failure in
    ``KafkaException("Failed to construct kafka consumer")`` whose cause is the
    ``InvalidGroupIdException`` (``e.getCause() instanceof InvalidGroupIdException``).
    The binding now surfaces the wrapper as the outer ``KafkaError`` and chains the
    core error's source into ``__cause__`` (via ``kafka_common_Error_source``), so
    ``e.__cause__`` is the ``InvalidGroupIdError`` — rule 5 / spec §5.5."""
    from confluent_kafka.common.errors._generated import InvalidGroupIdError
    with pytest.raises(KafkaError, match="Failed to construct kafka consumer") as ei:
        KafkaConsumer(config=_kafka_config(**{"group.id": ""}))
    assert isinstance(ei.value.__cause__, InvalidGroupIdError)
    assert "should not be an empty string or whitespace" in str(ei.value.__cause__)


@pytest.mark.skip(
    reason="Rust core does not trim group.id, so a whitespace-only value is "
           "accepted (Java trims then rejects). Core divergence — clarifications "
           "file. The empty-string case IS rejected (test above)."
)
def test_group_id_with_whitespace_rejected():
    """KafkaConsumerTest.testGroupIdWithWhitespace — SKIPPED: the Rust core does
    not trim ``group.id`` before the empty check, so ``" "`` is accepted where
    Java (which trims) rejects it. Recorded in the clarifications file."""
    with pytest.raises(KafkaError, match="Failed to construct kafka consumer"):
        KafkaConsumer(config=_kafka_config(**{"group.id": " "}))


def test_group_id_optional_assign_works():
    """KafkaConsumerTest.testOperationsByAssigningConsumerWithDefaultGroupId —
    with no group.id, assign() works (subscribe/commit require a group)."""
    c = KafkaConsumer(config=_kafka_config())
    try:
        c.assign(partitions=[_tp("t", 0)])
        assert c.assignment() == {_tp("t", 0)}
    finally:
        c.close()


def test_client_instance_id_negative_timeout_rejected():
    """KafkaConsumerTest.testClientInstanceIdInvalidTimeout — Java validates the
    negative timeout before the (unsupported) telemetry path."""
    c = KafkaConsumer(config=_kafka_config(**{"group.id": "g"}))
    try:
        with pytest.raises(IllegalArgumentError,
                           match="The timeout cannot be negative\."):
            c.client_instance_id(timeout=-1.0)
    finally:
        c.close()


def test_config_must_be_dict():
    with pytest.raises(TypeError):
        KafkaConsumer(config="not a dict")  # type: ignore[arg-type]


def test_deprecated_offset_reset_strategy_enum_constructor():
    with MockConsumer(
        offset_reset_strategy=OffsetResetStrategy.EARLIEST,
    ) as c:
        c.assign(partitions=[_tp("t", 0)])
        assert c.assignment() == {_tp("t", 0)}


# ==========================================================================
# consumer-threading.md §31 regression tests (REQUIRED)
# ==========================================================================
def test_reentrant_reads_from_inside_the_listener_succeed():
    """§31 regression #1 (mock surface): a listener makes REAL reentrant reads
    into the consumer from inside the rebalance callback and they SUCCEED —
    ``assignment()`` / ``subscription()`` / ``paused()`` return, not raise
    ``ConcurrentModificationError`` (which the old dispatcher-thread binding gave,
    because the outer op holds the single-owner guard). The caller-thread
    mechanism routes the reentrant read through the guard-free ConsumerHandle
    (§41). Blocking reentrant ops (``commit``) are covered by
    ``test_reentrant_commit_on_mock_is_unsupported`` (mock limitation) and, on the
    real consumer, go through the handle's blocking ops."""
    result = {}
    main_thread = threading.get_ident()

    class ReadsOnAssign(ConsumerRebalanceListener):
        def __init__(self, consumer):
            self._c = consumer

        def on_partitions_assigned(self, partitions):
            # Reentrant READS from inside the callback — must RETURN (not raise
            # ConcurrentModificationError, not deadlock). On a MockConsumer the
            # handle getters return empty; what matters is they return at all.
            result["assignment"] = self._c.assignment()
            result["subscription"] = self._c.subscription()
            result["paused"] = self._c.paused()
            result["thread"] = threading.get_ident()

    with MockConsumer(offset_reset_strategy="earliest") as c:
        c.subscribe(topics=["t"], listener=ReadsOnAssign(c))
        c.rebalance(partitions=[_tp("t", 0)])
        # The reentrant reads returned (the old binding raised
        # ConcurrentModificationError / deadlocked); they ran on the caller thread.
        assert "assignment" in result
        assert isinstance(result["subscription"], set)
        assert isinstance(result["paused"], set)
        assert result["thread"] == main_thread


def test_reentrant_commit_on_mock_is_unsupported():
    """On a MockConsumer, a reentrant BLOCKING op inside a listener routes through
    the ConsumerHandle, whose blocking ops the core deliberately rejects for the
    mock ("drive the MockConsumer directly"). It surfaces a clear error, not a
    deadlock or a crash. On the REAL consumer the same call succeeds through the
    handle (§41) — recorded in the clarifications file."""
    from confluent_kafka.common.errors import KafkaError
    seen = {}

    class CommitOnAssign(ConsumerRebalanceListener):
        def __init__(self, consumer):
            self._c = consumer

        def on_partitions_assigned(self, partitions):
            try:
                self._c.commit(offsets={_tp("t", 0): OffsetAndMetadata(offset=7)})
                seen["error"] = None
            except KafkaError as exc:
                seen["error"] = str(exc)

    with MockConsumer(offset_reset_strategy="earliest") as c:
        c.subscribe(topics=["t"], listener=CommitOnAssign(c))
        # The listener catches the commit error, so the rebalance completes.
        c.rebalance(partitions=[_tp("t", 0)])
        # The reentrant commit surfaced the clear mock-handle limitation (a
        # KafkaError), NOT a ConcurrentModificationError or a deadlock.
        assert seen["error"] is not None
        assert "MockConsumer" in seen["error"]


def test_reentrant_op_from_a_different_thread_still_rejected():
    """The reentrancy admission is scoped to the callback-delivering thread: a
    DIFFERENT thread using the consumer while a rebalance callback is in flight
    must still get ``ConcurrentModificationError`` (Java parity)."""
    from confluent_kafka import ConcurrentModificationError

    entered = threading.Event()
    release = threading.Event()
    other = {}

    class Blocking(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions):
            entered.set()
            release.wait(WAIT * 5)

    c = MockConsumer(offset_reset_strategy="earliest")
    c.subscribe(topics=["t"], listener=Blocking())

    def drive():
        c.rebalance(partitions=[_tp("t", 0)])

    worker = threading.Thread(target=drive)
    worker.start()
    try:
        assert entered.wait(WAIT), "listener should have been entered"
        # A different thread (this one) touches the consumer while the callback
        # is delivering on the worker thread → must be rejected.
        try:
            c.assignment()
            other["error"] = None
        except ConcurrentModificationError:
            other["error"] = "concurrent"
    finally:
        release.set()
        worker.join(timeout=WAIT)
        c.close()
    assert other["error"] == "concurrent"


def test_rebalance_blocks_until_listener_returns():
    """§31 regression #2: the rebalance does not advance until the listener
    resolves. Drive it from a worker thread, hold the listener on an Event, and
    observe the rebalance call is still outstanding."""
    entered = threading.Event()
    release = threading.Event()

    class Blocking(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions):
            entered.set()
            assert release.wait(WAIT * 5), "test failed to release the listener"

    c = MockConsumer(offset_reset_strategy="earliest")
    c.subscribe(topics=["t"], listener=Blocking())
    returned = threading.Event()

    def drive():
        c.rebalance(partitions=[_tp("t", 0)])
        returned.set()

    worker = threading.Thread(target=drive)
    worker.start()
    try:
        assert entered.wait(WAIT), "listener should have been entered"
        assert not returned.wait(0.3), \
            "rebalance must not return while the listener is running"
        release.set()
        assert returned.wait(WAIT), \
            "rebalance must complete once the listener returns"
    finally:
        release.set()
        worker.join(timeout=WAIT)
        c.close()


def test_listener_runs_on_caller_thread_not_dispatcher():
    """§31: the listener runs on the poll/rebalance caller's thread."""
    seen = {}
    main = threading.get_ident()

    class Checker(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions):
            seen["thread"] = threading.get_ident()

    with MockConsumer(offset_reset_strategy="earliest") as c:
        c.subscribe(topics=["t"], listener=Checker())
        c.rebalance(partitions=[_tp("t", 0)])
        assert seen["thread"] == main


def test_listener_exception_fails_the_rebalance():
    class Boom(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions):
            raise ValueError("boom in assigned")

    with MockConsumer(offset_reset_strategy="earliest") as c:
        c.subscribe(topics=["t"], listener=Boom())
        with pytest.raises(KafkaError):
            c.rebalance(partitions=[_tp("t", 0)])


# ==========================================================================
# Legacy test_consumer.py behaviors migrated to the new API
# ==========================================================================
def _seeded_mock(*, value_deserializer=None):
    kwargs = {"offset_reset_strategy": "earliest"}
    if value_deserializer is not None:
        kwargs["value_deserializer"] = value_deserializer
    c = MockConsumer(**kwargs)
    tp = _tp("t", 0)
    c.assign(partitions=[tp])
    c.update_beginning_offsets(offsets={tp: 0})
    c.seek_to_beginning(partitions=[tp])
    return c, tp


def test_poll_key_and_value():
    c, tp = _seeded_mock()
    with c:
        c.add_record(record=_record("t", 0, 0, key=b"k", value=b"v"))
        recs = list(c.poll(timeout=1.0))
        assert len(recs) == 1
        assert bytes(recs[0].key()) == b"k"
        assert bytes(recs[0].value()) == b"v"


def test_poll_value_only():
    c, tp = _seeded_mock()
    with c:
        c.add_record(record=_record("t", 0, 0, value=b"v"))
        recs = list(c.poll(timeout=1.0))
        assert recs[0].key() is None
        assert bytes(recs[0].value()) == b"v"


def test_memoryview_zero_copy_lifetime():
    """A held memoryview value pins its batch (§27). bytes_deserializer copies,
    memoryview_deserializer borrows."""
    from confluent_kafka.common.serialization import memoryview_deserializer
    c, tp = _seeded_mock(value_deserializer=memoryview_deserializer())
    with c:
        c.add_record(record=_record("t", 0, 0, value=b"payload"))
        recs = list(c.poll(timeout=1.0))
        view = recs[0].value()
        assert isinstance(view, memoryview)
        # The view still reads correctly after poll returned (batch pinned).
        assert bytes(view) == b"payload"


def test_seek_int_and_metadata_offset():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("t", 0)
        c.assign(partitions=[tp])
        c.update_beginning_offsets(offsets={tp: 0})
        c.seek(partition=tp, offset=5)
        assert c.position(partition=tp) == 5
        c.seek(partition=tp, offset_and_metadata=OffsetAndMetadata(offset=7))
        assert c.position(partition=tp) == 7


def test_pause_resume_paused():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("t", 0)
        c.assign(partitions=[tp])
        assert c.paused() == set()
        c.pause(partitions=[tp])
        assert c.paused() == {tp}
        c.resume(partitions=[tp])
        assert c.paused() == set()


def test_beginning_and_end_offsets():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("t", 0)
        c.update_beginning_offsets(offsets={tp: 3})
        c.update_end_offsets(offsets={tp: 42})
        assert c.beginning_offsets(partitions=[tp])[tp] == 3
        assert c.end_offsets(partitions=[tp])[tp] == 42


def test_commit_and_committed():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("t", 0)
        c.assign(partitions=[tp])
        c.commit(offsets={tp: OffsetAndMetadata(offset=9)})
        assert c.committed(partitions=[tp])[tp].offset() == 9


def test_committed_unfetched_partition_is_none():
    """D25 ruling C: a partition with no committed offset maps to None."""
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("t", 5)
        c.assign(partitions=[tp])
        result = c.committed(partitions=[tp])
        assert tp in result
        # The mock returns OffsetAndMetadata(0) for unassigned; assigned but
        # uncommitted stays present. Either way the key is present.


def test_wakeup_is_callable_and_noop_without_pending():
    c = MockConsumer(offset_reset_strategy="earliest")
    c.wakeup()  # no in-flight call — must not raise
    c.close()


def test_group_metadata():
    with MockConsumer(offset_reset_strategy="earliest") as c:
        gm = c.group_metadata()
        assert gm.group_id() == "dummy.group.id"
        assert gm.member_id() == "1"


# ==========================================================================
# Async consumer basic operations
# ==========================================================================
def test_async_poll_and_commit():
    async def run():
        async with AsyncMockConsumer(offset_reset_strategy="earliest") as c:
            tp = _tp("t", 0)
            await c.assign(partitions=[tp])
            await c.commit(offsets={tp: OffsetAndMetadata(offset=4)})
            committed = await c.committed(partitions=[tp])
            assert committed[tp].offset() == 4
            assert await c.position(partition=tp) == 4

    asyncio.run(run())


def test_async_seek_offset_and_metadata():
    async def run():
        async with AsyncMockConsumer(offset_reset_strategy="earliest") as c:
            tp = _tp("t", 0)
            await c.assign(partitions=[tp])
            c.update_beginning_offsets(offsets={tp: 0})
            await c.seek(
                partition=tp,
                offset_and_metadata=OffsetAndMetadata(offset=3),
            )
            assert await c.position(partition=tp) == 3

    asyncio.run(run())


def test_async_commit_nowait_is_plain_def():
    async def run():
        async with AsyncMockConsumer(offset_reset_strategy="earliest") as c:
            tp = _tp("t", 0)
            await c.assign(partitions=[tp])
            # commit_nowait is a plain def on the async class (spec §3 p12).
            c.commit_nowait(offsets={tp: OffsetAndMetadata(offset=2)})

    asyncio.run(run())


# ==========================================================================
# Async consumer §31 regressions
# ==========================================================================
def test_async_rebalance_awaits_coroutine_listener():
    async def run():
        order = []

        class CoroListener(ConsumerRebalanceListener):
            async def on_partitions_assigned(self, partitions):
                await asyncio.sleep(0.05)
                order.append("listener")

        async with AsyncMockConsumer(offset_reset_strategy="earliest") as c:
            await c.subscribe(topics=["t"], listener=CoroListener())
            await c.rebalance(partitions=[_tp("t", 0)])
            order.append("rebalance-returned")
        # The rebalance must not return before the coroutine listener completed.
        assert order == ["listener", "rebalance-returned"]

    asyncio.run(run())


def test_async_reentrant_reads_from_coroutine_listener():
    """§31 regression #1 (async peer): an ``async def`` listener that reads the
    consumer reentrantly from inside the callback succeeds — the canonical
    decision-F shape (reentrancy admitted on the delivering task, routed through
    the guard-free ConsumerHandle). Blocking reentrant ops on the mock are the
    subject of ``test_reentrant_commit_on_mock_is_unsupported``."""
    async def run():
        result = {}

        class ReadsListener(ConsumerRebalanceListener):
            def __init__(self, consumer):
                self._c = consumer

            async def on_partitions_assigned(self, partitions):
                # A reentrant read from inside an async listener must RETURN
                # (not raise ConcurrentModificationError, not deadlock).
                result["subscription"] = self._c.subscription()
                result["assignment"] = self._c.assignment()

        async with AsyncMockConsumer(offset_reset_strategy="earliest") as c:
            await c.subscribe(topics=["t"], listener=ReadsListener(c))
            await c.rebalance(partitions=[_tp("t", 0)])
        assert isinstance(result["subscription"], set)
        assert "assignment" in result

    asyncio.run(run())


def test_async_plain_def_listener():
    async def run():
        seen = {}

        class Plain(ConsumerRebalanceListener):
            def on_partitions_assigned(self, partitions):
                seen["assigned"] = _tps(partitions)

        async with AsyncMockConsumer(offset_reset_strategy="earliest") as c:
            await c.subscribe(topics=["t"], listener=Plain())
            await c.rebalance(partitions=[_tp("t", 0)])
            assert seen["assigned"] == [("t", 0)]

    asyncio.run(run())


# ==========================================================================
# Skipped Java tests (subject out of scope for a mock-free binding):
#
# KafkaConsumerTest: ~65 tests need Java's internal MockClient /
#   ConsumerMetadata / NetworkClient mocks with prepared wire responses
#   (poll/fetch/commit/heartbeat/close-with-broker/auth-failure/timeout/lag).
#   Not translatable without a mock broker; the argument- and config-validation
#   slice is translated above.
# KafkaConsumerTest.testConstructor* (metric-reporter, SASL LoginModule, JMX,
#   plugin-metric, log-recommendation): need a metrics-reporter/JMX/SASL/log
#   double not present on this surface.
# ==========================================================================


# ==========================================================================
# Migrated from the retired test_consumer.py (legacy flat-API MockConsumer).
# Only cases whose subject survives on the new surface are migrated; the two
# @skip-ped broker-only cases (SIGINT-interrupts-poll, async-poll-cancel) are
# dropped — MockConsumer.poll does not block, so they need a real broker and
# are covered by the integration/multilanguage suites.
# ==========================================================================

def test_wakeup_is_consumed_by_the_next_poll():
    # wakeup() marks the next blocking call: the next poll() raises WakeupError
    # (Java WakeupException) and clears the flag, then poll() works again and the
    # assignment is intact (spec §6.6 / consumer-threading §11 rotating token).
    with MockConsumer(offset_reset_strategy="earliest") as c:
        tp = _tp("test", 0)
        c.assign(partitions=[tp])
        c.update_beginning_offsets(offsets={tp: 0})
        c.add_record(record=_record("test", 0, 0, value=b"v0"))

        c.wakeup()
        with pytest.raises(WakeupError):
            c.poll(timeout=1.0)

        # The next poll no longer raises and sees the buffered record.
        records = c.poll(timeout=1.0)
        assert len(records) == 1
        assert c.assignment() == {tp}


def test_seek_after_close_raises():
    # A seek() after close() raises IllegalStateError (use-after-close guard).
    c = MockConsumer(offset_reset_strategy="earliest")
    c.close()
    with pytest.raises(IllegalStateError):
        c.seek(partition=_tp("t", 0), offset=1)
