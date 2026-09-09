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

"""Unit tests for the ``confluent_kafka.producer`` family (P4).

Translates:

- ``MockProducerTest.java`` — every ``@Test`` against ``MockProducer``. Skipped
  cases are marked with the reason (Java-only constructs: ``testPartitioner``
  needs ``Cluster``/``RoundRobinPartitioner``, ``shouldThrowClassCastException``
  needs Java generics + a mismatched serializer — neither has a Python surface).
- the broker-independent slice of ``KafkaProducerTest.java`` (constructor / config
  validation, close semantics, ``partitionsFor``-null, ``clientInstanceId`` timeout,
  transaction use-after-close). The mock-dependent majority is skipped (see the
  per-test notes and the P4 session report).
- the rule-11 per-method surface checks (positional call → ``TypeError``;
  base-class instantiation → ``TypeError``; ``on_delivery`` thread identity;
  double-await shape; context-manager flush-then-close; use-after-close →
  ``IllegalStateError``; error mapping through ``from_ffi_error``).

Thread-contract coverage boundary: the pure-Python ``MockProducer`` completes
sends synchronously on the caller thread (Java-faithful,
``test_mock_on_delivery_fires_synchronously_on_caller_thread``). The **real**
``KafkaProducer``'s background-completion-thread contract (spec §7.1 / D25 D) is
only partially checkable without a broker — ``test_kafka_on_delivery_not_on_caller_thread``
asserts it on a fail-fast delivery to an unresolvable bootstrap; the full
success-path contract is covered by P7 integration (the gRPC multilanguage
harness), not by a unit test.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka.common import TopicPartition
from confluent_kafka.common.errors import KafkaError
from confluent_kafka.common.errors._generated import (
    ProducerFencedError,
    RecordTooLargeError,
    TimeoutError as WireTimeoutError,
    TransactionAbortableError,
)
from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata
from confluent_kafka.producer import (
    AsyncKafkaProducer,
    AsyncMockProducer,
    AsyncProducer,
    KafkaProducer,
    MockProducer,
    Producer,
    ProducerRecord,
    RecordMetadata,
)

TOPIC = "topic"


def _record(key: bytes, value: bytes, *, partition=None, timestamp=None):
    return ProducerRecord(topic=TOPIC, key=key, value=value,
                          partition=partition, timestamp=timestamp)


RECORD1 = _record(b"key1", b"value1")
RECORD2 = _record(b"key2", b"value2")


def build_mock(auto_complete: bool) -> MockProducer:
    """Java's buildMockProducer(autoComplete). The Java fixture uses
    ``MockSerializer`` (string→bytes) with a ``String`` record; the Python fixture
    uses the default ``bytes_serializer`` with ``bytes`` records, so the stored
    record compares equal to the original (see C22)."""
    return MockProducer(auto_complete=auto_complete)


def is_error(future) -> bool:
    """Java's isError(future): did the future fail?"""
    try:
        future.result()
        return False
    except Exception:
        return True


# ===========================================================================
# MockProducerTest.java
# ===========================================================================

class TestMockProducer:
    def test_auto_complete_mock(self):
        p = build_mock(True)
        md = p.send(record=RECORD1)
        assert md.done()
        assert not is_error(md)
        assert md.result().offset() == 0
        assert md.result().topic() == TOPIC
        assert p.history() == [RECORD1]
        p.clear()
        assert len(p.history()) == 0
        p.close()

    # testPartitioner — SKIPPED: needs Cluster + RoundRobinPartitioner +
    # PartitionInfo, none of which exist on the Python surface (custom
    # partitioner is deferred to the plugin pass, spec §6.1). See C22/§6.1.

    def test_manual_completion(self):
        p = build_mock(False)
        md1 = p.send(record=RECORD1)
        assert not md1.done()
        md2 = p.send(record=RECORD2)
        assert not md2.done()

        assert p.complete_next()
        assert not is_error(md1)
        assert not md2.done()

        # errorNext with an arbitrary error; the completion carries the same
        # class + message (identity is not preserved across the FFI — C23).
        assert p.error_next(error=IllegalArgumentError("blah"))
        with pytest.raises(IllegalArgumentError) as exc:
            md2.result()
        assert str(exc.value) == "blah"

        assert not p.complete_next()

        md3 = p.send(record=RECORD1)
        md4 = p.send(record=RECORD2)
        assert not md3.done() and not md4.done()
        p.flush()
        assert md3.done() and md4.done()
        p.close()

    # ---- transaction state machine -----------------------------------------
    def test_should_init_transactions(self):
        p = build_mock(True)
        p.init_transactions()
        assert p.transaction_initialized()

    def test_should_throw_on_init_if_already_initialized(self):
        p = build_mock(True)
        p.init_transactions()
        with pytest.raises(IllegalStateError):
            p.init_transactions()

    def test_should_throw_on_begin_if_not_initialized(self):
        p = build_mock(True)
        with pytest.raises(IllegalStateError):
            p.begin_transaction()

    def test_should_begin_transactions(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        assert p.transaction_in_flight()

    def test_should_throw_on_begin_if_transaction_in_flight(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        with pytest.raises(IllegalStateError):
            p.begin_transaction()

    def test_should_throw_on_send_offsets_if_not_initialized(self):
        p = build_mock(True)
        with pytest.raises(IllegalStateError):
            p.send_offsets_to_transaction(
                offsets={}, group_metadata=ConsumerGroupMetadata(group_id="g"))

    def test_should_throw_on_send_offsets_if_no_transaction_started(self):
        p = build_mock(True)
        p.init_transactions()
        with pytest.raises(IllegalStateError):
            p.send_offsets_to_transaction(
                offsets={}, group_metadata=ConsumerGroupMetadata(group_id="g"))

    def test_should_throw_on_commit_if_not_initialized(self):
        p = build_mock(True)
        with pytest.raises(IllegalStateError):
            p.commit_transaction()

    def test_should_throw_on_commit_if_no_transaction_started(self):
        p = build_mock(True)
        p.init_transactions()
        with pytest.raises(IllegalStateError):
            p.commit_transaction()

    def test_should_commit_empty_transaction(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        p.commit_transaction()
        assert not p.transaction_in_flight()
        assert p.transaction_committed()
        assert not p.transaction_aborted()

    def test_should_count_committed_transaction(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        assert p.commit_count() == 0
        p.commit_transaction()
        assert p.commit_count() == 1

    def test_should_not_count_aborted_transaction(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        p.abort_transaction()
        p.begin_transaction()
        p.commit_transaction()
        assert p.commit_count() == 1

    def test_should_throw_on_abort_if_not_initialized(self):
        p = build_mock(True)
        with pytest.raises(IllegalStateError):
            p.abort_transaction()

    def test_should_throw_on_abort_if_no_transaction_started(self):
        p = build_mock(True)
        p.init_transactions()
        with pytest.raises(IllegalStateError):
            p.abort_transaction()

    def test_should_abort_empty_transaction(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        p.abort_transaction()
        assert not p.transaction_in_flight()
        assert p.transaction_aborted()
        assert not p.transaction_committed()

    def test_should_throw_fence_if_not_initialized(self):
        p = build_mock(True)
        with pytest.raises(IllegalStateError):
            p.fence_producer()

    def test_should_throw_on_begin_if_fenced(self):
        p = build_mock(True)
        p.init_transactions()
        p.fence_producer()
        with pytest.raises(ProducerFencedError):
            p.begin_transaction()

    def test_should_throw_on_send_if_fenced(self):
        p = build_mock(True)
        p.init_transactions()
        p.fence_producer()
        # Java: KafkaException wrapping ProducerFencedException. The core builds a
        # bare KafkaError with ProducerFenced as the cause (see C22 note).
        with pytest.raises(KafkaError) as exc:
            p.send(record=RECORD1)
        assert isinstance(exc.value.__cause__, ProducerFencedError)

    def test_should_throw_on_send_offsets_by_group_metadata_if_fenced(self):
        p = build_mock(True)
        p.init_transactions()
        p.fence_producer()
        with pytest.raises(ProducerFencedError):
            p.send_offsets_to_transaction(
                offsets={}, group_metadata=ConsumerGroupMetadata(group_id="g"))

    def test_should_throw_on_commit_if_fenced(self):
        p = build_mock(True)
        p.init_transactions()
        p.fence_producer()
        with pytest.raises(ProducerFencedError):
            p.commit_transaction()

    def test_should_throw_on_abort_if_fenced(self):
        p = build_mock(True)
        p.init_transactions()
        p.fence_producer()
        with pytest.raises(ProducerFencedError):
            p.abort_transaction()

    def test_should_publish_only_after_commit(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        p.send(record=RECORD1)
        p.send(record=RECORD2)
        assert p.history() == []
        p.commit_transaction()
        assert p.history() == [RECORD1, RECORD2]

    def test_should_flush_on_commit_for_non_auto_complete(self):
        p = build_mock(False)
        p.init_transactions()
        p.begin_transaction()
        md1 = p.send(record=RECORD1)
        md2 = p.send(record=RECORD2)
        assert not md1.done()
        assert not md2.done()
        p.commit_transaction()
        assert md1.done()
        assert md2.done()

    def test_should_drop_messages_on_abort(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        p.send(record=RECORD1)
        p.send(record=RECORD2)
        p.abort_transaction()
        assert p.history() == []
        p.begin_transaction()
        p.commit_transaction()
        assert p.history() == []

    def test_should_throw_on_abort_for_non_auto_complete(self):
        p = build_mock(False)
        p.init_transactions()
        p.begin_transaction()
        md1 = p.send(record=RECORD1)
        assert not md1.done()
        p.abort_transaction()
        assert md1.done()

    def test_should_preserve_committed_messages_on_abort(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        p.send(record=RECORD1)
        p.send(record=RECORD2)
        p.commit_transaction()
        p.begin_transaction()
        p.abort_transaction()
        assert p.history() == [RECORD1, RECORD2]

    # ---- consumer-group offsets --------------------------------------------
    def test_should_publish_offsets_only_after_commit(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        tp0 = TopicPartition(topic=TOPIC, partition=0)
        tp1 = TopicPartition(topic=TOPIC, partition=1)
        g1 = {tp0: OffsetAndMetadata(offset=42),
              tp1: OffsetAndMetadata(offset=73)}
        g2 = {tp0: OffsetAndMetadata(offset=101),
              tp1: OffsetAndMetadata(offset=21)}
        p.send_offsets_to_transaction(
            offsets=g1, group_metadata=ConsumerGroupMetadata(group_id="g1"))
        p.send_offsets_to_transaction(
            offsets=g2, group_metadata=ConsumerGroupMetadata(group_id="g2"))
        assert p.consumer_group_offsets_history() == []
        p.commit_transaction()
        assert p.consumer_group_offsets_history() == [{"g1": g1, "g2": g2}]

    def test_should_throw_on_null_group_metadata(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        # Java: NullPointerException from ConsumerGroupMetadata(null). Our
        # ConsumerGroupMetadata requires group_id; None is a TypeError-ish reject.
        with pytest.raises((TypeError, ValueError, IllegalArgumentError)):
            p.send_offsets_to_transaction(
                offsets={},
                group_metadata=ConsumerGroupMetadata(group_id=None))  # type: ignore[arg-type]

    def test_should_ignore_empty_offsets(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        p.send_offsets_to_transaction(
            offsets={},
            group_metadata=ConsumerGroupMetadata(group_id="groupId"))
        assert not p.sent_offsets()

    def test_should_add_offsets(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        assert not p.sent_offsets()
        tp0 = TopicPartition(topic=TOPIC, partition=0)
        commit = {tp0: OffsetAndMetadata(offset=42)}
        p.send_offsets_to_transaction(
            offsets=commit,
            group_metadata=ConsumerGroupMetadata(group_id="groupId"))
        assert p.sent_offsets()

    def test_should_reset_sent_offsets_only_on_begin(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        assert not p.sent_offsets()
        tp0 = TopicPartition(topic=TOPIC, partition=0)
        commit = {tp0: OffsetAndMetadata(offset=42)}
        gm = ConsumerGroupMetadata(group_id="groupId")
        p.send_offsets_to_transaction(offsets=commit, group_metadata=gm)
        p.commit_transaction()
        assert p.sent_offsets()  # commit does NOT reset
        p.begin_transaction()
        assert not p.sent_offsets()  # begin resets
        p.send_offsets_to_transaction(offsets=commit, group_metadata=gm)
        p.commit_transaction()
        assert p.sent_offsets()
        p.begin_transaction()
        assert not p.sent_offsets()

    def test_should_publish_latest_and_cumulative_offsets(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        tp0 = TopicPartition(topic=TOPIC, partition=0)
        tp1 = TopicPartition(topic=TOPIC, partition=1)
        tp2 = TopicPartition(topic=TOPIC, partition=2)
        c1 = {tp0: OffsetAndMetadata(offset=42),
              tp1: OffsetAndMetadata(offset=73)}
        c2 = {tp1: OffsetAndMetadata(offset=101),
              tp2: OffsetAndMetadata(offset=21)}
        gm = ConsumerGroupMetadata(group_id="g")
        p.send_offsets_to_transaction(offsets=c1, group_metadata=gm)
        p.send_offsets_to_transaction(offsets=c2, group_metadata=gm)
        assert p.consumer_group_offsets_history() == []
        expected = {"g": {tp0: OffsetAndMetadata(offset=42),
                          tp1: OffsetAndMetadata(offset=101),
                          tp2: OffsetAndMetadata(offset=21)}}
        p.commit_transaction()
        assert p.consumer_group_offsets_history() == [expected]

    def test_should_drop_offsets_on_abort(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        tp0 = TopicPartition(topic=TOPIC, partition=0)
        tp1 = TopicPartition(topic=TOPIC, partition=1)
        commit = {tp0: OffsetAndMetadata(offset=42),
                  tp1: OffsetAndMetadata(offset=73)}
        gm = ConsumerGroupMetadata(group_id="g")
        p.send_offsets_to_transaction(offsets=commit, group_metadata=gm)
        p.abort_transaction()
        p.begin_transaction()
        p.commit_transaction()
        assert p.consumer_group_offsets_history() == []
        p.begin_transaction()
        p.send_offsets_to_transaction(offsets=commit, group_metadata=gm)
        p.abort_transaction()
        p.begin_transaction()
        p.commit_transaction()
        assert p.consumer_group_offsets_history() == []

    def test_should_preserve_offsets_on_abort(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        tp0 = TopicPartition(topic=TOPIC, partition=0)
        tp1 = TopicPartition(topic=TOPIC, partition=1)
        commit = {tp0: OffsetAndMetadata(offset=42),
                  tp1: OffsetAndMetadata(offset=73)}
        gm = ConsumerGroupMetadata(group_id="g")
        p.send_offsets_to_transaction(offsets=commit, group_metadata=gm)
        p.commit_transaction()
        p.begin_transaction()
        p.abort_transaction()
        assert p.consumer_group_offsets_history() == [{"g": commit}]

    def test_should_preserve_committed_offsets_only(self):
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        tp0 = TopicPartition(topic=TOPIC, partition=0)
        tp1 = TopicPartition(topic=TOPIC, partition=1)
        tp2 = TopicPartition(topic=TOPIC, partition=2)
        tp3 = TopicPartition(topic=TOPIC, partition=3)
        c1 = {tp0: OffsetAndMetadata(offset=42),
              tp1: OffsetAndMetadata(offset=73)}
        p.send_offsets_to_transaction(
            offsets=c1, group_metadata=ConsumerGroupMetadata(group_id="g"))
        p.commit_transaction()
        p.begin_transaction()
        c2 = {tp2: OffsetAndMetadata(offset=53),
              tp3: OffsetAndMetadata(offset=84)}
        p.send_offsets_to_transaction(
            offsets=c2, group_metadata=ConsumerGroupMetadata(group_id="g2"))
        p.abort_transaction()
        assert p.consumer_group_offsets_history() == [{"g": c1}]

    # ---- closed-state errors -----------------------------------------------
    def test_should_throw_on_init_if_closed(self):
        p = build_mock(True)
        p.close()
        with pytest.raises(IllegalStateError):
            p.init_transactions()

    def test_should_throw_on_send_if_closed(self):
        p = build_mock(True)
        p.close()
        with pytest.raises(IllegalStateError):
            p.send(record=RECORD1)

    def test_should_throw_on_begin_if_closed(self):
        p = build_mock(True)
        p.close()
        with pytest.raises(IllegalStateError):
            p.begin_transaction()

    def test_should_throw_send_offsets_if_closed(self):
        p = build_mock(True)
        p.close()
        with pytest.raises(IllegalStateError):
            p.send_offsets_to_transaction(
                offsets={}, group_metadata=ConsumerGroupMetadata(group_id="g"))

    def test_should_throw_on_commit_if_closed(self):
        p = build_mock(True)
        p.close()
        with pytest.raises(IllegalStateError):
            p.commit_transaction()

    def test_should_throw_on_abort_if_closed(self):
        p = build_mock(True)
        p.close()
        with pytest.raises(IllegalStateError):
            p.abort_transaction()

    def test_should_throw_on_fence_if_closed(self):
        p = build_mock(True)
        p.close()
        with pytest.raises(IllegalStateError):
            p.fence_producer()

    def test_should_throw_on_flush_if_closed(self):
        p = build_mock(True)
        p.close()
        with pytest.raises(IllegalStateError):
            p.flush()

    def test_should_not_throw_on_flush_if_fenced(self):
        p = build_mock(True)
        p.init_transactions()
        p.fence_producer()
        p.flush()  # assertDoesNotThrow
        p.close()

    # shouldThrowClassCastException — SKIPPED: relies on Java generics letting a
    # String key past an IntegerSerializer to force ClassCastException at
    # serialize time. Python has no generics enforcement and the default
    # bytes serializer accepts any bytes; no faithful equivalent.

    # ---- flushed() ----------------------------------------------------------
    def test_should_be_flushed_if_no_buffered_records(self):
        p = build_mock(True)
        assert p.flushed()
        p.close()

    def test_should_be_flushed_with_auto_complete(self):
        p = build_mock(True)
        p.send(record=RECORD1)
        assert p.flushed()
        p.close()

    def test_should_not_be_flushed_with_no_auto_complete(self):
        p = build_mock(False)
        p.send(record=RECORD1)
        assert not p.flushed()
        p.close()

    def test_should_be_flushed_after_flush(self):
        p = build_mock(False)
        p.send(record=RECORD1)
        p.flush()
        assert p.flushed()
        p.close()

    def test_metadata_on_exception(self):
        # Java testMetadataOnException: errorNext delivers a callback with a
        # non-null RecordMetadata whose fields are all -1, plus the error.
        p = build_mock(False)
        captured = {}

        def on_delivery(md, exc):
            captured["md"] = md
            captured["exc"] = exc

        md = p.send(record=RECORD2, on_delivery=on_delivery)
        assert p.error_next(error=IllegalArgumentError("dummy exception"))
        with pytest.raises(IllegalArgumentError) as exc:
            md.result()
        assert str(exc.value) == "dummy exception"
        # The callback saw the error-path metadata (all fields -1) and the error.
        meta = captured["md"]
        assert meta is not None
        assert meta.offset() == -1
        assert meta.timestamp() == -1
        assert meta.serialized_key_size() == -1
        assert meta.serialized_value_size() == -1
        assert isinstance(captured["exc"], IllegalArgumentError)
        p.close()

    def test_client_instance_id_unset_raises(self):
        # Java MockProducer.clientInstanceId throws
        # UnsupportedOperationException("clientInstanceId not set") when the id is
        # unset (MockProducer.java:406). No UnsupportedOperationError JDK analog
        # exists on this surface, so the semantic counterpart NotImplementedError
        # carries Java's exact message (C25 addendum / Critic 67 F1).
        p = build_mock(True)
        with pytest.raises(NotImplementedError) as exc:
            p.client_instance_id()
        assert str(exc.value) == "clientInstanceId not set"
        p.close()

    def test_client_instance_id_returns_set_id(self):
        # After set_client_instance_id, the mock returns it (Java-faithful).
        from confluent_kafka.common import Uuid
        p = build_mock(True)
        uid = Uuid.random_uuid()
        p.set_client_instance_id(instance_id=uid)
        assert p.client_instance_id() == uid
        p.close()


# ===========================================================================
# KafkaProducerTest.java — broker-independent slice
# ===========================================================================

# Skipped (require a MockClient / mocked ProducerMetadata / Sender / metrics
# internals — no Python surface): every transaction happy-path test, all
# metadata/metrics/interceptor/telemetry tests, and the "close forced on pending
# X" tests. See the P4 session report for the full per-method triage.

BOOTSTRAP = {"bootstrap.servers": "localhost:9000"}


class TestKafkaProducerConstruction:
    def test_constructor_with_serializers(self):
        # testConstructorWithSerializers: construct + close, no exception.
        from confluent_kafka.common.serialization import bytes_serializer
        p = KafkaProducer(config=dict(BOOTSTRAP),
                          key_serializer=bytes_serializer(),
                          value_serializer=bytes_serializer())
        p.close()

    def test_config_must_be_dict(self):
        with pytest.raises(IllegalArgumentError):
            KafkaProducer(config="not-a-dict")  # type: ignore[arg-type]

    def test_close_should_be_idempotent(self):
        # closeShouldBeIdempotent: close() twice, no error.
        p = KafkaProducer(config=dict(BOOTSTRAP))
        p.close()
        p.close()

    def test_close_with_negative_timeout_should_raise(self):
        # closeWithNegativeTimestampShouldThrow.
        p = KafkaProducer(config=dict(BOOTSTRAP))
        with pytest.raises(IllegalArgumentError):
            p.close(timeout=-0.1)
        p.close()

    def test_context_manager_flushes_then_closes(self):
        with KafkaProducer(config=dict(BOOTSTRAP)) as p:
            assert p is not None
        # After the block the producer is closed; a further op raises.
        with pytest.raises(IllegalStateError):
            p.flush()

    def test_use_after_close_raises_illegal_state(self):
        p = KafkaProducer(config=dict(BOOTSTRAP))
        p.close()
        with pytest.raises(IllegalStateError):
            p.flush()
        with pytest.raises(IllegalStateError):
            p.init_transactions()

    def test_callback_config_key_rejected(self):
        # The old-client callback keys are rejected with a ConfigError (§11.1).
        from confluent_kafka.common.config import ConfigError
        cfg = dict(BOOTSTRAP)
        cfg["on_delivery"] = lambda *a: None
        with pytest.raises(ConfigError):
            KafkaProducer(config=cfg)

    def test_client_instance_id_invalid_timeout(self):
        # testClientInstanceIdInvalidTimeout: exact Java message.
        p = KafkaProducer(config={"bootstrap.servers": "localhost:9999"})
        with pytest.raises(IllegalArgumentError) as exc:
            p.client_instance_id(timeout=-0.001)
        assert str(exc.value) == "The timeout cannot be negative."
        p.close()

    def test_partitioner_rejected(self):
        with pytest.raises(IllegalArgumentError):
            KafkaProducer(config=dict(BOOTSTRAP), partitioner=object())


class TestProducerRecordNullTopic:
    def test_null_topic_name(self):
        # testNullTopicName (a ProducerRecord ctor test).
        with pytest.raises(IllegalArgumentError):
            ProducerRecord(topic=None, value=b"v")  # type: ignore[arg-type]


# ===========================================================================
# Rule-11 per-method surface checks
# ===========================================================================

class TestSurfaceContracts:
    def test_base_producer_not_instantiable(self):
        with pytest.raises(TypeError):
            Producer()

    def test_base_async_producer_not_instantiable(self):
        with pytest.raises(TypeError):
            AsyncProducer()

    def test_send_positional_is_type_error(self):
        p = build_mock(True)
        with pytest.raises(TypeError):
            p.send(RECORD1)  # type: ignore[misc]
        p.close()

    def test_transaction_methods_have_no_timeout(self):
        # D26 round 2 / Java Producer.java:45/50/55/61/66 — none of the five
        # transaction methods take a Duration/timeout, on BOTH the sync Producer
        # and the async AsyncProducer.
        import inspect
        for cls in (Producer, AsyncProducer):
            for name in ("init_transactions", "begin_transaction",
                         "send_offsets_to_transaction", "commit_transaction",
                         "abort_transaction"):
                sig = inspect.signature(getattr(cls, name))
                assert "timeout" not in sig.parameters, f"{cls.__name__}.{name}"

    def test_mock_on_delivery_fires_synchronously_on_caller_thread(self):
        # The pure-Python MockProducer with auto_complete completes the send
        # SYNCHRONOUSLY on the caller thread — Java-faithful (Java's MockProducer
        # completes inline in send()). This asserts that faithful behaviour.
        #
        # The REAL KafkaProducer fires on_delivery on the background completion
        # thread, never the caller's (spec §7.1 / D25 D); that contract needs a
        # broker and is covered by P7 integration (see the module docstring and
        # test_kafka_on_delivery_not_on_caller_thread below for the broker-free
        # partial check).
        caller = threading.get_ident()
        p = build_mock(True)
        seen = {}

        def cb(md, exc):
            seen["thread"] = threading.get_ident()

        p.send(record=RECORD1, on_delivery=cb)
        assert "thread" in seen
        assert seen["thread"] == caller
        p.close()

    # The real KafkaProducer's background-completion-thread contract (spec §7.1 /
    # D25 D) is NOT unit-tested: a broker-free attempt (send to an unresolvable
    # bootstrap, assert the error callback fires off-thread) proved unreliable —
    # the delivery does not fail fast deterministically and the subsequent close
    # can block. The contract is covered by P7 integration (the gRPC multilanguage
    # harness). See the module docstring "Thread-contract coverage boundary".

    def test_error_mapping_through_from_ffi_error(self):
        # error_next injects a typed error; the future surfaces the same class.
        p = build_mock(False)
        md = p.send(record=RECORD1)
        p.error_next(error=RecordTooLargeError("too big"))
        with pytest.raises(RecordTooLargeError):
            md.result()
        p.close()


class TestAsyncProducerFamily:
    async def test_async_mock_send_and_history(self):
        p = AsyncMockProducer(auto_complete=True)
        # Double await: first the send (buffer capacity), then the ack future.
        md = await (await p.send(record=RECORD1))
        assert md.offset() == 0
        assert p.history() == [RECORD1]
        await p.close()

    async def test_async_double_await_shape(self):
        # md = await (await p.send(...)) — the first await suspends on capacity,
        # the returned future resolves with the metadata.
        p = AsyncMockProducer(auto_complete=True)
        md = await (await p.send(record=RECORD1))
        assert isinstance(md, RecordMetadata)
        await p.close()

    async def test_async_begin_transaction_is_plain_def(self):
        # begin_transaction does not block, so it is a plain def on the async
        # class (principle 5) — calling it does not return a coroutine.
        import inspect
        assert not inspect.iscoroutinefunction(AsyncProducer.begin_transaction)
        p = AsyncMockProducer(auto_complete=True)
        await p.init_transactions()
        p.begin_transaction()
        assert p.transaction_in_flight()
        await p.close()

    async def test_async_context_manager(self):
        async with AsyncMockProducer(auto_complete=True) as p:
            await p.send(record=RECORD1)
        assert p.closed()


# ===========================================================================
# Migrated from the retired test_producer.py (legacy flat-API MockProducer).
#
# Only cases whose subject survives on the new pure-Python MockProducer surface
# (C22/C38) are migrated here. The legacy backpressure suite ran against the
# retired FFI-backed mock's send-task, which the pure-Python mock does not have —
# but backpressure is a live contract of the REAL producer (the reason
# `send` is `async`, spec principle 5), so it is re-expressed against
# `KafkaProducer` / `AsyncKafkaProducer` further down (TestProducerBackpressure),
# not dropped. The async-FFI-routing / GIL / handle-lifecycle regressions
# targeted the retired mock's `_lib.Producer_*_async` plumbing and have no
# public-surface subject, so they are dropped (not skipped). The flat-error
# assertions (`err.code` / `err.is_retriable` / `err.txn_requires_abort`) are
# re-expressed here as the typed error class the new hierarchy uses.
# ===========================================================================

class TestMockProducerCallbacks:
    def test_on_delivery_fires_when_future_cancelled(self):
        # Callback obligation (CLAUDE.md §9.5): on_delivery must fire even when the
        # caller cancelled the returned future before completion.
        p = build_mock(False)
        seen = []
        md = p.send(record=RECORD1, on_delivery=lambda meta, exc: seen.append((meta, exc)))
        assert md.cancel()
        assert p.complete_next()
        assert len(seen) == 1
        meta, exc = seen[0]
        assert exc is None
        assert meta.offset() == 0
        p.close()

    def test_on_delivery_exception_does_not_break_future_or_producer(self):
        # A raising on_delivery is logged, not propagated (rule 7): the producer
        # keeps working and the next send still completes.
        p = build_mock(False)

        def boom(meta, exc):
            raise RuntimeError("callback blew up")

        md1 = p.send(record=RECORD1, on_delivery=boom)
        assert p.complete_next()
        assert not is_error(md1)  # the future itself still resolved
        md2 = p.send(record=RECORD2)
        assert p.complete_next()
        assert md2.result().offset() == 1
        p.close()

    def test_on_delivery_none_is_the_default(self):
        # on_delivery=None is a valid no-op.
        p = build_mock(True)
        md = p.send(record=RECORD1, on_delivery=None)
        assert md.result().offset() == 0
        p.close()


class TestMockProducerLifecycle:
    def test_partitions_for_on_mock_is_empty(self):
        # MockProducer.partitions_for(*, topic=) returns [] (no cluster metadata).
        p = build_mock(True)
        assert p.partitions_for(topic=TOPIC) == []
        p.close()

    def test_close_with_send_in_flight(self):
        # Regression: close() must not deadlock with an un-completed in-flight
        # send on a manual-completion mock.
        p = build_mock(False)
        p.send(record=RECORD1)
        p.close()
        assert p.closed()


class TestMockProducerTransactionFailure:
    def test_commit_failure_abortable_surfaces_abortable_error(self):
        # Legacy asserted err.txn_requires_abort is True (flat API, removed); the
        # new hierarchy expresses "abortable" as the typed TransactionAbortableError.
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        p.send(record=RECORD1)
        p.set_commit_transaction_exception(error=TransactionAbortableError("abort me"))
        with pytest.raises(TransactionAbortableError):
            p.commit_transaction()
        p.close()

    def test_commit_failure_non_abortable_surfaces_its_own_error(self):
        # A non-abortable commit failure (e.g. a wire timeout) surfaces as its own
        # typed class, distinct from TransactionAbortableError.
        p = build_mock(True)
        p.init_transactions()
        p.begin_transaction()
        p.send(record=RECORD1)
        p.set_commit_transaction_exception(error=WireTimeoutError("timed out"))
        with pytest.raises(WireTimeoutError):
            p.commit_transaction()
        p.close()


class TestAsyncMockProducerCallbacks:
    async def test_async_on_delivery_fires_on_loop_thread(self):
        # On AsyncKafkaProducer the on_delivery runs on the event-loop thread, not
        # a background completion thread (rule 7).
        import asyncio
        p = AsyncMockProducer(auto_complete=True)
        loop_ident = threading.get_ident()
        seen = []

        def cb(meta, exc):
            seen.append(threading.get_ident())

        await (await p.send(record=RECORD1, on_delivery=cb))
        # Let any scheduled callback run on the loop.
        await asyncio.sleep(0)
        assert seen == [loop_ident]
        await p.close()

    async def test_async_close_with_send_in_flight(self):
        p = AsyncMockProducer(auto_complete=False)
        await p.send(record=RECORD1)
        await p.close()
        assert p.closed()


# ===========================================================================
# Producer backpressure — a live contract of the REAL producer.
#
# `AsyncKafkaProducer.send` / `KafkaProducer.send` are `async` / thread-blocking
# precisely because Java's `send` blocks on buffer space (spec principle 5):
# `Producer_send` returns `full` once PRODUCER_MAX_ACCUMULATED_RECORDS (1000)
# records are accumulated un-taken, and the send then waits on
# `Producer_on_space_available` (async_producer.py / producer.py). This is
# re-expressed here from the retired FFI-mock backpressure suite; the subject is
# the real producer's send path, not the retired mock.
#
# The `Producer_test_set_paused` hook (`_confluentkafka.c:884`) stalls the send
# task's drain so the buffer fills deterministically WITHOUT a broker — records
# accumulate regardless of broker reachability. The producer is pointed at an
# UNREACHABLE bootstrap: with the task paused, filling to the bound and crossing
# it exercises the full/space-available path, and `close()` (called while still
# paused) fires the pending space waiters and releases the blocked/suspended
# sender. The pure "resume-on-drain" leg (un-pause, delivery actually completes)
# needs the send task to deliver to a broker, so it belongs to the integration
# arm — see the module note below and C44.
# ===========================================================================

import _confluentkafka as _lib  # noqa: E402  (test-only Producer_test_set_paused)

# PRODUCER_MAX_ACCUMULATED_RECORDS in _confluentkafka.c: the producer is "full"
# once this many records are accumulated un-taken by the (paused) send task.
BACKPRESSURE_BOUND = 1000
_UNREACHABLE = {"bootstrap.servers": "127.0.0.1:59999"}


class TestProducerBackpressure:
    def test_sync_send_blocks_on_full_and_close_unblocks(self):
        # KafkaProducer.send blocks the calling thread once the buffer is full
        # (Java send() on a full buffer); close() must release it, not hang.
        p = KafkaProducer(config=_UNREACHABLE)
        _lib.Producer_test_set_paused(p._c_producer, True)
        for _ in range(BACKPRESSURE_BOUND - 1):
            p.send(record=RECORD1)  # below the bound: none block

        done = threading.Event()

        def crossing_send():
            p.send(record=RECORD1)  # crosses the bound -> blocks on full
            done.set()

        t = threading.Thread(target=crossing_send)
        t.start()
        try:
            assert not done.wait(timeout=0.4), "send should block while the buffer is full"
            # close() while still paused fires the pending space waiter and
            # releases the blocked sender (and tears down cleanly — the send task
            # never tries to deliver to the unreachable broker).
            p.close(timeout=2.0)
            assert done.wait(timeout=10.0), "close must release the blocked sender"
        finally:
            t.join(timeout=10.0)

    async def test_async_send_suspends_on_full_and_close_unblocks(self):
        # AsyncKafkaProducer.send suspends (yields the loop) on a full buffer,
        # never blocks it; close() releases the suspended sender.
        p = AsyncKafkaProducer(config=_UNREACHABLE)
        _lib.Producer_test_set_paused(p._c_producer, True)
        for _ in range(BACKPRESSURE_BOUND - 1):
            await p.send(record=RECORD1)

        task = asyncio.ensure_future(p.send(record=RECORD1))
        await asyncio.sleep(0.3)
        assert not task.done(), "crossing send should suspend on backpressure"
        # The loop stays responsive while the send is suspended.
        assert await asyncio.sleep(0, result=True)

        # close() while still paused releases the suspended sender cleanly.
        await asyncio.wait_for(p.close(timeout=2.0), timeout=10.0)
        await asyncio.wait_for(task, timeout=5.0)
        assert task.done()

    async def test_async_below_bound_never_suspends(self):
        # When accumulation stays below the bound, no send suspends — backpressure
        # is invisible. Paused so the task cannot drain, isolating the bound check.
        p = AsyncKafkaProducer(config=_UNREACHABLE)
        _lib.Producer_test_set_paused(p._c_producer, True)
        for _ in range(50):
            fut = p.send(record=RECORD1)
            # Each send resolves promptly (no space wait) below the bound.
            await asyncio.wait_for(fut, timeout=2.0)
        # Teardown while paused (records never delivered to the unreachable broker).
        await asyncio.wait_for(p.close(timeout=2.0), timeout=10.0)
