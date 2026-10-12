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

"""``KafkaConsumerTest.java`` (Apache Kafka 4.3.1) for ``KafkaConsumer`` without a
broker, in Java order, with Java's messages; and the binding's own contracts that
need no broker — the commit callback on the caller's thread (C47), the lifetime of
the native handle across a racing ``close()``, the deserializers' configuration
route and their close, the negative timeouts, the GIL released while the
constructor resolves the bootstrap hosts. Where Java drives a ``MockClient``
only to make an operation time out, an unreachable bootstrap address and a short
``default.api.timeout.ms`` stand for it. ``test/integration/test_kafka_consumer_broker.py``
runs, against a broker, the cases Java scripts through a ``MockClient``.

Of Java's 126 tests, 31 are translated here and 14 in the broker module; the
other 81, each named, are not, with the reason:

- the classic protocol: Java runs these with ``group.protocol=classic`` only, and
  the core implements KIP-848 only (``consumer-threading.md`` §20):
  ``testPollReturnsRecords``,
  ``testSecondPollWithDeserializationErrorThrowsRecordDeserializationException``,
  ``testSubscriptionWithEmptyPartitionAssignment``, ``verifyHeartbeatSent``,
  ``verifyHeartbeatSentWhenFetchedDataReady``,
  ``verifyNoCoordinatorLookupForManualAssignmentWithOffsetCommit``,
  ``testAutoCommitSentBeforePositionUpdate``, ``testRegexSubscription``,
  ``testChangingRegexSubscription``, ``testWakeupWithFetchDataAvailable``,
  ``testPollThrowsInterruptExceptionIfInterrupted``,
  ``testSubscriptionChangesWithAutoCommitEnabled``,
  ``testSubscriptionChangesWithAutoCommitDisabled``,
  ``testUnsubscribeShouldTriggerPartitionsRevokedWithValidGeneration``,
  ``testUnsubscribeShouldTriggerPartitionsLostWithNoGeneration``,
  ``testGracefulClose``, ``testCloseTimeoutDueToNoResponseForCloseFetchRequest``,
  ``testCloseTimeout``, ``testLeaveGroupTimeout``, ``testCloseNoWait``,
  ``testCloseInterrupt``, ``testShouldAttemptToRejoinGroupAfterSyncGroupFailed``,
  ``testPartitionsForNonExistingTopic``, ``testPartitionsForAuthenticationFailure``,
  ``testBeginningOffsetsAuthenticationFailure``,
  ``testEndOffsetsAuthenticationFailure``,
  ``testOffsetsForTimesAuthenticationFailure``,
  ``testCommitSyncAuthenticationFailure``, ``testCommittedAuthenticationFailure``,
  ``testRebalanceException``, ``testReturnRecordsDuringRebalance``,
  ``testGetGroupMetadata``, ``testCurrentLagPreventsMultipleInFlightRequests``,
  ``testCurrentLagClearsFlagOnFatalPartitionError``,
  ``testCurrentLagClearsFlagOnRetriablePartitionError``,
  ``testEnforceRebalanceWithManualAssignment``,
  ``testEnforceRebalanceTriggersRebalanceOnNextPoll``,
  ``testEnforceRebalanceReason``, ``testAssignorNameConflict`` and
  ``testSubscribeToRe2jPatternNotSupportedForClassicConsumer``. The broker module
  covers what three of them check for the consumer protocol (a ``wakeup()``
  breaking a waiting ``poll()``, a failing deserializer leaving the position at
  the record, a listener's exception);
- ``ClassicKafkaConsumer``'s KIP-848 recommendation log, which only the classic
  consumer writes: ``testClassicProtocolLogsRecommendationToTryConsumerProtocol``,
  ``testDefaultProtocolLogsRecommendationToTryConsumerProtocol``,
  ``testNoGroupIdDoesNotLogGroupProtocolMessage`` and
  ``testConsumerProtocolDoesNotLogRecommendation``;
- a ``MockClient`` answer a broker does not give, or a mocked collaborator:
  ``testFetchProgressWithMissingPartitionPosition`` (a
  ``NOT_LEADER_OR_FOLLOWER`` list-offsets answer for one partition),
  ``fetchResponseWithUnexpectedPartitionIsIgnored`` (a fetch response naming an
  unassigned partition), ``testFetchStableOffsetThrowInCommitted``,
  ``testFetchStableOffsetThrowInPoll`` and
  ``testFetchStableOffsetThrowInPosition`` (an ``OffsetFetch`` version without
  ``requireStable``: an old broker), ``testPollAuthenticationFailure`` (a SASL
  authentication failure; the test broker has no SASL listener) and
  ``testConstructorFailsOnNetworkClientConstructorFailure`` (a mocked
  ``NetworkClient`` constructor);
- metrics: the KIP-1076 metric subscription is not generated
  (``testSubscribingCustomMetricsDoesntAffectConsumerMetrics``,
  ``testSubscribingCustomMetricsWithSameNameDoesntAffectConsumerMetrics``,
  ``testUnsubscribingCustomMetricsWithSameNameDoesntAffectConsumerMetrics``,
  ``testUnSubscribingNonExisingMetricsDoesntCauseError``); the core loads no
  metric reporter, JMX or telemetry reporter
  (``testShouldOnlyCallMetricReporterMetricChangeOnceWithExistingConsumerMetric``,
  ``testShouldNotCallMetricReporterMetricRemovalWithExistingConsumerMetric``,
  ``testMetricsReporterAutoGeneratedClientId``,
  ``testDisableJmxAndClientTelemetryReporter``,
  ``testExplicitlyOnlyEnableJmxReporter``,
  ``testExplicitlyOnlyEnableClientTelemetryReporter``,
  ``testConstructorInvalidMetricReporters`` — ``metric.reporters`` is not loaded,
  so an invalid class is not detected — and ``testConsumerJmxPrefix``);
  ``testMetricConfigRecordingLevelInfo`` reads the private
  ``metricsRegistry().config()``; ``testPollTimeMetrics``, ``testPollIdleRatio``,
  ``testMeasureCommitSyncDurationOnFailure``, ``testMeasureCommitSyncDuration``,
  ``testMeasureCommittedDurationOnFailure`` and ``testMeasureCommittedDuration``
  assert values a ``MockTime`` makes exact, and the core's clock cannot be
  injected;
- not generated or dropped: ``testClientInstanceId``,
  ``testClientInstanceIdInvalidTimeout`` and
  ``testClientInstanceIdNoTelemetryReporterRegistered`` (``clientInstanceId`` is
  not generated, its entry point being missing); ``testCurrentLag``
  (``current_lag()`` always returns ``None``, Rust-core gap 3); and
  ``testSubscriptionOnNullPattern`` for the dropped ``java.util.regex.Pattern``
  overloads (``testSubscriptionOnEmptyPattern`` runs as its ``SubscriptionPattern``
  counterpart);
- configuration: ``testInvalidSocketSendBufferSize`` /
  ``testInvalidSocketReceiveBufferSize`` (the core does not validate the
  ``send.buffer.bytes`` / ``receive.buffer.bytes`` ranges, Rust-core gap 11);
  ``testUnusedConfigs`` (only the core reports the keys it does not know, in its
  own log);
  ``testInterceptorConstructorClose``,
  ``testInterceptorConstructorConfigurationWithExceptionShouldCloseRemainingInstances``
  and ``configurableObjectsShouldSeeGeneratedClientId`` (the client runs no
  interceptors: ``interceptor.classes`` is a key the core does not know; the
  last also needs the generated ``client.id`` in a
  config-route deserializer's ``configure``, which the binding does not have when
  it builds the deserializers, before the core consumer, in Java's order).

``testOperationsBySubscribingConsumerWithDefaultGroupId``'s
``enable.auto.commit=true`` half is not asserted (the core does not reject it,
Rust-core gap 11); ``testAssignedPartitionsMetrics``' group-assignment half needs
a broker, and ``testClosingConsumerUnregistersConsumerMetrics``' after-close half
reads ``metrics()`` after ``close()``, which raises ``IllegalStateError`` here.
"""

from __future__ import annotations

import asyncio
import gc
import math
import sys
import threading
import time
import uuid
import warnings
import weakref
from datetime import timedelta
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import (
    ConcurrentModificationError, IllegalArgumentError, IllegalStateError, NullPointerError,
)
from confluent_kafka.common import KafkaError, TopicPartition
from confluent_kafka.common.errors import InvalidGroupIdError, UnsupportedVersionError, WakeupError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.serialization import string_deserializer
from confluent_kafka.consumer import (
    AsyncKafkaConsumer, CloseOptions, ConsumerRebalanceListener, KafkaConsumer,
    OffsetAndMetadata, SubscriptionPattern,
)
from confluent_kafka.consumer._base import poll_timeout_ms

WAIT = 10.0
TOPIC = "test"
GROUP_ID = "mock-group"
TP0 = TopicPartition(topic=TOPIC, partition=0)

# The message of Java's AsyncKafkaConsumer.throwIfGroupIdNotDefined().
NO_GROUP_ID = ("To use the group management or offset commit APIs, you must provide a valid "
               "group.id in the consumer configuration.")
CLOSED = "This consumer has already been closed."
CONCURRENT = "KafkaConsumer is not safe for multi-threaded access."


def configs(group_id: str | None = GROUP_ID, **extra: Any) -> dict[str, Any]:
    config: dict[str, Any] = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer"}
    if group_id is not None:
        config["group.id"] = group_id
    config.update(extra)
    return config


def new_consumer(group_id: str | None = GROUP_ID, **extra: Any) -> KafkaConsumer[bytes, bytes]:
    return KafkaConsumer(configs=configs(group_id, **extra))


# ---------------------------------------------------------------------------
# KafkaConsumerTest
# ---------------------------------------------------------------------------
def test_subscription() -> None:
    with new_consumer() as consumer:
        consumer.subscribe(topics=[TOPIC])
        assert consumer.subscription() == {TOPIC}
        assert consumer.assignment() == set()

        consumer.subscribe(topics=[])
        assert consumer.subscription() == set()
        assert consumer.assignment() == set()

        consumer.assign(partitions=[TP0])
        assert consumer.subscription() == set()
        assert consumer.assignment() == {TP0}

        consumer.unsubscribe()
        assert consumer.subscription() == set()
        assert consumer.assignment() == set()


def test_subscription_on_null_topic_collection() -> None:
    # Java's subscribe((List) null) throws IllegalArgumentException; an omitted
    # (None) topics matches no Java overload.
    with new_consumer() as consumer, pytest.raises(IllegalArgumentError) as e:
        consumer.subscribe(topics=None)
    assert str(e.value) == ("subscribe() takes one of (topics), (topics, callback), "
                            "(pattern, callback), (pattern); got ()")


def test_subscription_on_empty_topic() -> None:
    with new_consumer() as consumer, pytest.raises(IllegalArgumentError) as e:
        consumer.subscribe(topics=["  "])
    assert str(e.value) == "Topic collection to subscribe to cannot contain null or empty topic"


def test_subscription_on_null_topic() -> None:
    with new_consumer() as consumer, pytest.raises(IllegalArgumentError) as e:
        consumer.subscribe(topics=[None])  # type: ignore[list-item]
    assert str(e.value) == "Topic collection to subscribe to cannot contain null or empty topic"


def test_subscription_on_null_topic_without_a_group_id() -> None:
    # Java's subscribe() checks the group.id before the topics.
    with new_consumer(None) as consumer, pytest.raises(InvalidGroupIdError) as e:
        consumer.subscribe(topics=[None])  # type: ignore[list-item]
    assert str(e.value) == NO_GROUP_ID


def test_subscription_on_empty_subscription_pattern() -> None:
    with new_consumer() as consumer, pytest.raises(IllegalArgumentError) as e:
        consumer.subscribe(pattern=SubscriptionPattern(pattern=""))
    assert str(e.value) == "Topic pattern to subscribe to cannot be empty"


def test_seek_negative() -> None:
    with new_consumer(None) as consumer:
        partition = TopicPartition(topic="nonExistTopic", partition=0)
        consumer.assign(partitions={partition})
        with pytest.raises(IllegalArgumentError) as e:
            consumer.seek(partition=partition, offset=-1)
        assert str(e.value) == "seek offset must not be a negative number"


def test_assign_on_empty_topic_partition() -> None:
    with new_consumer() as consumer:
        consumer.assign(partitions=[])
        assert consumer.subscription() == set()
        assert consumer.assignment() == set()


def test_assign_on_null_topic_partition() -> None:
    with new_consumer(None) as consumer, pytest.raises(IllegalArgumentError) as e:
        consumer.assign(partitions=None)  # type: ignore[arg-type]
    assert str(e.value) == "Topic partitions collection to assign to cannot be null"


def test_assign_on_null_topic_partition_after_close() -> None:
    # Java's assign() checks that the consumer is open before its argument.
    consumer = new_consumer(None)
    consumer.close()
    with pytest.raises(IllegalStateError) as e:
        consumer.assign(partitions=None)  # type: ignore[arg-type]
    assert str(e.value) == CLOSED


# AsyncKafkaConsumer's null checks of an argument the FFI cannot take as None
# (CLAUDE.md, Python Binding Conventions, Implementation over the FFI,
# exception 4): the method, its keyword, Java's error class and message.
NULL_ARGUMENT_CASES: list[tuple[str, str, type[Exception], str]] = [
    ("pause", "partitions", NullPointerError, "The partitions to pause must be nonnull"),
    ("resume", "partitions", NullPointerError, "The partitions to resume must be nonnull"),
    ("offsets_for_times", "timestamps_to_search", NullPointerError,
     "Timestamps to search cannot be null"),
    ("seek_to_beginning", "partitions", IllegalArgumentError,
     "Partitions collection cannot be null"),
    ("seek_to_end", "partitions", IllegalArgumentError, "Partitions collection cannot be null"),
    ("beginning_offsets", "partitions", NullPointerError, "Partitions cannot be null"),
    ("end_offsets", "partitions", NullPointerError, "Partitions cannot be null"),
]


@pytest.mark.parametrize("method, keyword, error, message", NULL_ARGUMENT_CASES)
def test_a_null_argument_raises_java_s_error(method: str, keyword: str,
                                             error: type[Exception], message: str) -> None:
    with new_consumer(None) as consumer, pytest.raises(error) as e:
        getattr(consumer, method)(**{keyword: None})
    assert type(e.value) is error
    assert str(e.value) == message


@pytest.mark.parametrize("method, keyword, error, message", NULL_ARGUMENT_CASES)
def test_a_null_argument_after_close_reports_the_closed_consumer(
        method: str, keyword: str, error: type[Exception], message: str) -> None:
    # The closed check comes first (Order), seek_to_* included, whose Java
    # null check precedes acquireAndEnsureOpen().
    consumer = new_consumer(None)
    consumer.close()
    with pytest.raises(IllegalStateError) as e:
        getattr(consumer, method)(**{keyword: None})
    assert str(e.value) == CLOSED


@pytest.mark.parametrize("method, keyword, error, message", NULL_ARGUMENT_CASES)
def test_the_async_consumer_makes_the_same_null_checks(
        method: str, keyword: str, error: type[Exception], message: str) -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs(None))
        with pytest.raises(error) as e:
            await getattr(consumer, method)(**{keyword: None})
        assert type(e.value) is error
        assert str(e.value) == message
        await consumer.close(option=CloseOptions.timeout(0))
        with pytest.raises(IllegalStateError) as e:
            await getattr(consumer, method)(**{keyword: None})
        assert str(e.value) == CLOSED

    asyncio.run(main())


def test_assign_on_null_topic_in_partition() -> None:
    with new_consumer(None) as consumer:
        with pytest.raises(IllegalArgumentError) as e:
            null_topic = TopicPartition(topic=None, partition=0)  # type: ignore[arg-type]
            consumer.assign(partitions={null_topic})
        assert str(e.value) == "Topic partitions to assign to cannot have null or empty topic"
        # Java: isBlank(tp != null ? tp.topic() : null).
        with pytest.raises(IllegalArgumentError) as e:
            consumer.assign(partitions=[None])  # type: ignore[list-item]
        assert str(e.value) == "Topic partitions to assign to cannot have null or empty topic"


def test_assign_on_empty_topic_in_partition() -> None:
    with new_consumer(None) as consumer, pytest.raises(IllegalArgumentError) as e:
        consumer.assign(partitions={TopicPartition(topic="  ", partition=0)})
    assert str(e.value) == "Topic partitions to assign to cannot have null or empty topic"


def test_pause() -> None:
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        assert consumer.assignment() == {TP0}
        assert consumer.paused() == set()

        consumer.pause(partitions={TP0})
        assert consumer.paused() == {TP0}

        consumer.resume(partitions={TP0})
        assert consumer.paused() == set()

        consumer.unsubscribe()
        assert consumer.paused() == set()


@pytest.mark.parametrize("setup", ["no-subscription", "empty-subscription", "empty-assignment"],
                         ids=["testPollWithNoSubscription", "testPollWithEmptySubscription",
                              "testPollWithEmptyUserAssignment"])
def test_poll_without_subscription(setup: str) -> None:
    with new_consumer(None if setup == "no-subscription" else GROUP_ID) as consumer:
        if setup == "empty-subscription":
            consumer.subscribe(topics=[])
        elif setup == "empty-assignment":
            consumer.assign(partitions=set())
        with pytest.raises(IllegalStateError) as e:
            consumer.poll(timeout=0)
        assert str(e.value) == "Consumer is not subscribed to any topics or assigned any partitions"


def test_verify_poll_times_out_during_metadata_update() -> None:
    # Java asserts no FETCH is sent while the metadata is not updated; with an
    # unreachable broker there is none, and poll(timeout=0) returns at once.
    with new_consumer() as consumer:
        consumer.subscribe(topics=[TOPIC])
        started = time.monotonic()
        assert consumer.poll(timeout=0).is_empty()
        assert time.monotonic() - started < WAIT


def test_committed_throws_timeout_error_for_no_response() -> None:
    # testCommittedThrowsTimeoutExceptionForNoResponse: Java's
    # committed(partitions, Duration.ofMillis(1000)); the Duration form is
    # not generated, its entry point being missing, so default.api.timeout.ms
    # bounds it.
    with new_consumer(**{"default.api.timeout.ms": 1000}) as consumer:
        consumer.assign(partitions=[TP0])
        with pytest.raises(KafkaTimeoutError) as e:
            consumer.committed(partitions={TP0})
        assert str(e.value) == ("Timeout of 1000ms expired before the last committed offset for "
                                "partitions [test-0] could be determined. Try tuning "
                                "default.api.timeout.ms larger to relax the threshold.")


# Java's consumerForCheckingTimeoutException leaves default.api.timeout.ms at its
# 60000 ms default; 500 ms keeps the test short, and Java's message carries it.
OFFSETS_TIMEOUT = "Failed to get offsets by times in 500ms"


def test_offsets_for_times_timeout() -> None:
    consumer = new_consumer(**{"default.api.timeout.ms": 500})
    with pytest.raises(KafkaTimeoutError) as e:
        consumer.offsets_for_times(timestamps_to_search={TP0: 0})
    assert str(e.value) == OFFSETS_TIMEOUT
    consumer.close(option=CloseOptions.timeout(0))


def test_beginning_offsets_timeout() -> None:
    consumer = new_consumer(**{"default.api.timeout.ms": 500})
    with pytest.raises(KafkaTimeoutError) as e:
        consumer.beginning_offsets(partitions=[TP0])
    assert str(e.value) == OFFSETS_TIMEOUT
    consumer.close(option=CloseOptions.timeout(0))


def test_end_offsets_timeout() -> None:
    consumer = new_consumer(**{"default.api.timeout.ms": 500})
    with pytest.raises(KafkaTimeoutError) as e:
        consumer.end_offsets(partitions=[TP0])
    assert str(e.value) == OFFSETS_TIMEOUT
    consumer.close(option=CloseOptions.timeout(0))


def _metric_value(consumer: KafkaConsumer[Any, Any], name: str) -> Any:
    for metric_name, metric in consumer.metrics().items():
        if metric_name.name() == name:
            return metric.metric_value()
    return None


def test_assigned_partitions_metrics() -> None:
    # Java also moves the assignment through subscribe + assignFromSubscribed on
    # the SubscriptionState it injects; a group assignment needs a broker here.
    with new_consumer() as consumer:
        deadline = time.monotonic() + WAIT
        while _metric_value(consumer, "assigned-partitions") is None:
            assert time.monotonic() < deadline, "no assigned-partitions metric"
            time.sleep(0.01)
        assert _metric_value(consumer, "assigned-partitions") == 0.0
        consumer.assign(partitions={TP0})
        assert _metric_value(consumer, "assigned-partitions") == 1.0
        consumer.assign(partitions={TP0, TopicPartition(topic=TOPIC, partition=1)})
        assert _metric_value(consumer, "assigned-partitions") == 2.0


def test_closing_consumer_unregisters_consumer_metrics() -> None:
    # Java then checks the metrics are gone after close(); here metrics() after
    # close() raises IllegalStateError (use after close raises), so only the
    # registration is checked.
    consumer = new_consumer()
    consumer.subscribe(topics=[TOPIC])
    names = {metric_name.name() for metric_name in consumer.metrics()}
    assert {"last-poll-seconds-ago", "time-between-poll-avg", "time-between-poll-max"} <= names
    consumer.close()
    with pytest.raises(IllegalStateError):
        consumer.metrics()


def test_close_should_be_idempotent() -> None:
    consumer = new_consumer()
    consumer.close(option=CloseOptions.timeout(timedelta(0)))
    consumer.close(option=CloseOptions.timeout(timedelta(0)))
    with pytest.raises(IllegalStateError) as e:
        consumer.assignment()
    assert str(e.value) == CLOSED


def test_operations_by_subscribing_consumer_with_default_group_id() -> None:
    with new_consumer(None) as consumer, pytest.raises(InvalidGroupIdError) as e:
        consumer.subscribe(topics={TOPIC})
    assert str(e.value) == NO_GROUP_ID
    with new_consumer(None) as consumer, pytest.raises(InvalidGroupIdError):
        consumer.committed(partitions={TP0})
    with new_consumer(None) as consumer, pytest.raises(InvalidGroupIdError):
        consumer.commit_nowait()
    with new_consumer(None) as consumer, pytest.raises(InvalidGroupIdError):
        consumer.commit()


def test_operations_by_assigning_consumer_with_default_group_id() -> None:
    with new_consumer(None) as consumer:
        consumer.assign(partitions={TP0})
        with pytest.raises(InvalidGroupIdError):
            consumer.committed(partitions={TP0})
        with pytest.raises(InvalidGroupIdError):
            consumer.commit_nowait()
        with pytest.raises(InvalidGroupIdError):
            consumer.commit()


def test_group_metadata_needs_a_group_id() -> None:
    # Java's groupMetadata() calls throwIfGroupIdNotDefined().
    with new_consumer(None) as consumer, pytest.raises(InvalidGroupIdError) as e:
        consumer.group_metadata()
    assert str(e.value) == NO_GROUP_ID
    with new_consumer() as consumer:
        metadata = consumer.group_metadata()
        assert (metadata.group_id(), metadata.generation_id(), metadata.member_id(),
                metadata.group_instance_id()) == (GROUP_ID, -1, "", None)


@pytest.mark.parametrize("group_id", ["", " "],
                         ids=["testEmptyGroupId", "testGroupIdWithWhitespace"])
def test_group_id_with_whitespace(group_id: str) -> None:
    with pytest.raises(KafkaError) as e:
        new_consumer(group_id)
    assert str(e.value) == "Failed to construct kafka consumer"
    assert isinstance(e.value.__cause__, InvalidGroupIdError)
    assert str(e.value.__cause__) == ("The configured group.id should not be an empty string or "
                                      "whitespace.")


def test_constructor_close() -> None:
    with pytest.raises(KafkaError) as e:
        KafkaConsumer(configs={"bootstrap.servers": "invalid-23-8409-adsfsdj",
                               "group.protocol": "consumer", "client.id": "testConstructorClose"})
    assert str(e.value) == "Failed to construct kafka consumer"


def test_os_default_socket_buffer_sizes() -> None:
    # Selectable.USE_DEFAULT_BUFFER_SIZE is -1.
    new_consumer(None, **{"send.buffer.bytes": -1, "receive.buffer.bytes": -1}).close()


def test_should_ignore_group_instance_id_for_empty_group_id() -> None:
    new_consumer(None, **{"group.instance.id": "instance_id"}).close()


def test_classic_protocol_is_not_supported() -> None:
    with pytest.raises(UnsupportedVersionError) as e:
        KafkaConsumer(configs={"bootstrap.servers": "localhost:1", "group.id": GROUP_ID})
    assert str(e.value) == ("Classic group protocol is not yet supported in this client; set "
                            "group.protocol=consumer (KIP-848).")


def _poll_in_background(consumer: KafkaConsumer[Any, Any]) -> tuple[threading.Thread, dict[str, Any]]:
    """Start a poll(timeout=5) on another thread (it waits: no broker)."""
    outcome: dict[str, Any] = {}

    def run() -> None:
        try:
            outcome["records"] = consumer.poll(timeout=5)
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    worker = threading.Thread(target=run)
    worker.start()
    time.sleep(0.5)
    return worker, outcome


def test_prevent_multi_thread() -> None:
    consumer = new_consumer()
    consumer.subscribe(topics=[TOPIC])
    worker, outcome = _poll_in_background(consumer)
    try:
        with pytest.raises(ConcurrentModificationError) as e:
            consumer.poll(timeout=0)
        assert str(e.value) == CONCURRENT
        consumer.wakeup()
    finally:
        worker.join(WAIT)
    assert isinstance(outcome.get("error"), WakeupError)
    consumer.close(option=CloseOptions.timeout(0))


def test_invalid_group_metadata() -> None:
    consumer = new_consumer()
    consumer.subscribe(topics=[TOPIC])
    worker, _ = _poll_in_background(consumer)
    try:
        # concurrent access is illegal
        with pytest.raises(ConcurrentModificationError):
            consumer.group_metadata()
        consumer.wakeup()
    finally:
        worker.join(WAIT)
    # accessing closed consumer is illegal
    consumer.close(option=CloseOptions.timeout(0))
    with pytest.raises(IllegalStateError) as e:
        consumer.group_metadata()
    assert str(e.value) == CLOSED


# ---------------------------------------------------------------------------
# Construction releases the GIL
# ---------------------------------------------------------------------------
def test_construction_releases_the_gil_while_it_resolves_the_bootstrap_hosts(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The core resolves the bootstrap hosts while it builds the consumer
    # (ClientUtils.parseAndValidateAddresses), a blocking name-service call, so
    # the C extension releases the GIL around the native constructor. Nothing
    # observes the GIL directly: a second thread records when it runs, and the
    # native constructor alone is timed, as the Python code around it hands the
    # GIL over anyway. With the GIL held throughout the call, that thread could
    # run only within a switch interval (1 ms here) of either end of it, never
    # in its middle. A fresh host name defeats a resolver's negative cache.
    # Best effort: a resolver that answers in under 20 ms leaves no middle to
    # observe, and the test skips.
    margin = 0.005
    window: dict[str, float] = {}
    native_new = _lib.Consumer_KafkaConsumer_new_typed

    def timed_new(native: dict[str, str]) -> Any:
        window["start"] = time.perf_counter()
        try:
            return native_new(native)
        finally:
            window["end"] = time.perf_counter()

    monkeypatch.setattr(_lib, "Consumer_KafkaConsumer_new_typed", timed_new)
    ran: list[float] = []
    stop = threading.Event()

    def other_thread() -> None:
        last = 0.0
        while not stop.is_set():
            now = time.perf_counter()
            if now - last >= 0.0005:
                ran.append(now)
                last = now

    host = f"{uuid.uuid4().hex}.invalid:9092"
    interval = sys.getswitchinterval()
    sys.setswitchinterval(0.001)
    worker = threading.Thread(target=other_thread)
    worker.start()
    try:
        with pytest.raises(KafkaError) as e:
            KafkaConsumer(configs=configs(**{
                "bootstrap.servers": host,
                "client.dns.lookup": "resolve_canonical_bootstrap_servers_only"}))
    finally:
        stop.set()
        worker.join(WAIT)
        sys.setswitchinterval(interval)
    assert str(e.value) == "Failed to construct kafka consumer"
    assert str(e.value.__cause__) == f"Unknown host in bootstrap.servers: {host}"
    start, end = window["start"] + margin, window["end"] - margin
    if end - start < 2 * margin:
        pytest.skip(f"the resolver answered in {(window['end'] - window['start']) * 1000:.1f} "
                    "ms, too fast to observe the GIL released")
    assert any(start < at < end for at in ran), (
        "no other thread ran in the middle of the native constructor: it holds the GIL")


# ---------------------------------------------------------------------------
# Timeouts and close
# ---------------------------------------------------------------------------
def test_poll_rejects_a_negative_timeout() -> None:
    # Java's Timer: "Invalid negative timeout " + timeoutMs.
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        with pytest.raises(IllegalArgumentError) as e:
            consumer.poll(timeout=-1)
        assert str(e.value) == "Invalid negative timeout -1000"
        with pytest.raises(IllegalArgumentError) as e:
            consumer.poll(timeout=timedelta(microseconds=-500))
        assert str(e.value) == "Invalid negative timeout -1"


def test_closed_first_then_the_argument_checks() -> None:
    # Java's poll checks the timeout (Timer) before acquireAndEnsureOpen(), and
    # close(CloseOptions) its timeout before `closed`; here the closed state
    # comes first (CLAUDE.md, Python Binding Conventions, Implementation over
    # the FFI, Order), so a second close() returns silently.
    consumer = new_consumer()
    consumer.close()
    with pytest.raises(IllegalStateError) as e:
        consumer.poll(timeout=-1)
    assert str(e.value) == CLOSED
    consumer.close(option=CloseOptions.timeout(-1))

    async def main() -> None:
        async_consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await async_consumer.close()
        with pytest.raises(IllegalStateError) as e:
            await async_consumer.poll(timeout=-1)
        assert str(e.value) == CLOSED
        await async_consumer.close(option=CloseOptions.timeout(-1))

    asyncio.run(main())


def test_close_rejects_a_negative_timeout_and_stays_open() -> None:
    consumer = new_consumer()
    with pytest.raises(IllegalArgumentError) as e:
        consumer.close(option=CloseOptions.timeout(-1))
    assert str(e.value) == "The timeout cannot be negative."
    with pytest.raises(IllegalArgumentError):
        consumer.close(option=CloseOptions.timeout(timedelta(seconds=-1)))
    assert consumer.subscription() == set()
    consumer.close(option=CloseOptions.timeout(0))


def test_poll_reads_a_timedelta_exactly() -> None:
    # Java's Duration.toMillis() is integer arithmetic, rounding down;
    # timedelta.total_seconds() goes through a float, one millisecond too long
    # for timedelta.max. A poll that long cannot be waited out, so the
    # conversion is checked on its own.
    td = timedelta.max
    assert poll_timeout_ms(td) == (td.days * 86_400 + td.seconds) * 1_000 + td.microseconds // 1_000
    assert poll_timeout_ms(timedelta(microseconds=1_999)) == 1
    assert poll_timeout_ms(timedelta(microseconds=999)) == 0
    assert poll_timeout_ms(0.0019999) == 1


# A timeout beyond Long.MAX_VALUE milliseconds (CLAUDE.md, Python Binding
# Conventions, Signatures, Timeouts): OverflowError, and NaN ValueError, before
# the FFI.
TOO_LONG = f"timeout of {math.floor(1e300 * 1000.0)} ms does not fit a signed 64-bit integer"


def test_a_timeout_beyond_a_long_raises_before_the_ffi_and_the_consumer_stays_open() -> None:
    consumer = new_consumer()
    consumer.assign(partitions=[TP0])
    with pytest.raises(OverflowError) as e:
        consumer.poll(timeout=1e300)
    assert str(e.value) == TOO_LONG
    with pytest.raises(OverflowError):
        consumer.poll(timeout=float("inf"))
    with pytest.raises(ValueError):
        consumer.poll(timeout=float("nan"))
    with pytest.raises(OverflowError) as e:
        consumer.close(option=CloseOptions.timeout(1e300))
    assert str(e.value) == TOO_LONG
    with pytest.raises(OverflowError):
        consumer.close(option=CloseOptions.timeout(float("inf")))
    with pytest.raises(ValueError):
        consumer.close(option=CloseOptions.timeout(float("nan")))
    # Still open: nothing reached the FFI.
    assert consumer.assignment() == {TP0}
    assert consumer.poll(timeout=0).is_empty()
    consumer.close(option=CloseOptions.timeout(0))
    with pytest.raises(IllegalStateError) as e:
        consumer.assignment()
    assert str(e.value) == CLOSED


def test_an_async_timeout_beyond_a_long_raises_before_the_ffi_and_the_consumer_stays_open(
        ) -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.assign(partitions=[TP0])
        with pytest.raises(OverflowError) as e:
            await consumer.poll(timeout=1e300)
        assert str(e.value) == TOO_LONG
        with pytest.raises(OverflowError) as e:
            await consumer.close(option=CloseOptions.timeout(1e300))
        assert str(e.value) == TOO_LONG
        with pytest.raises(ValueError):
            await consumer.close(option=CloseOptions.timeout(float("nan")))
        assert consumer.assignment() == {TP0}
        await consumer.close(option=CloseOptions.timeout(0))
        with pytest.raises(IllegalStateError) as e:
            consumer.assignment()
        assert str(e.value) == CLOSED

    asyncio.run(main())


def test_close_with_a_timeout_is_not_generated() -> None:
    # Java's @Deprecated close(Duration) (CLAUDE.md, Python Binding Conventions,
    # Class family); the consumer stays open.
    consumer = new_consumer()
    with pytest.raises(TypeError):
        consumer.close(timeout=0)  # type: ignore[call-arg]
    assert consumer.subscription() == set()
    consumer.close(option=CloseOptions.timeout(0))


def test_close_twice_is_harmless_and_calls_after_it_raise() -> None:
    consumer = new_consumer()
    consumer.close(option=CloseOptions.timeout(0))
    consumer.close()
    consumer.wakeup()  # a no-op once closed
    for call in (consumer.assignment, consumer.subscription, consumer.paused, consumer.metrics,
                 lambda: consumer.poll(timeout=0), lambda: consumer.commit(),
                 lambda: consumer.commit_nowait(), lambda: consumer.assign(partitions=[TP0]),
                 lambda: consumer.position(partition=TP0),
                 lambda: consumer.current_lag(topic_partition=TP0)):
        with pytest.raises(IllegalStateError) as e:
            call()
        assert str(e.value) == CLOSED


def test_close_from_another_thread_while_polling_fails_and_stays_open() -> None:
    # Java's close() takes the consumer's lock (acquire()): another thread
    # inside poll() makes it throw ConcurrentModificationException, and the
    # consumer is still usable.
    consumer = new_consumer()
    consumer.subscribe(topics=[TOPIC])
    worker, outcome = _poll_in_background(consumer)
    try:
        with pytest.raises(ConcurrentModificationError):
            consumer.close(option=CloseOptions.timeout(0))
        consumer.wakeup()
    finally:
        worker.join(WAIT)
    assert isinstance(outcome.get("error"), WakeupError)
    assert consumer.subscription() == {TOPIC}
    consumer.close(option=CloseOptions.timeout(0))


def _spy_close(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    import _confluentkafka as lib  # type: ignore[import-not-found]

    calls: list[Any] = []
    original = lib.Consumer_close_with_option_async

    def spy(*args: Any) -> Any:
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(lib, "Consumer_close_with_option_async", spy)
    return calls


def test_close_from_another_thread_fails_before_changing_anything(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's close(CloseOptions) calls acquire() before it changes any state: a
    # close() from another thread while one is inside the consumer throws
    # ConcurrentModificationException to the closer only, and the owning
    # thread's calls go on (its own later calls, not a window of refusals).
    calls = _spy_close(monkeypatch)
    consumer = new_consumer()
    consumer.assign(partitions=[TP0])
    inside, outcome = threading.Event(), {}

    def owner() -> None:
        inside.set()
        try:
            consumer.poll(timeout=1.0)
            outcome["calls"] = (consumer.assignment(), consumer.poll(timeout=0).is_empty())
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    worker = threading.Thread(target=owner)
    worker.start()
    try:
        assert inside.wait(WAIT)
        deadline = time.monotonic() + WAIT
        while not consumer._use_threads:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        for _ in range(20):
            with pytest.raises(ConcurrentModificationError) as e:
                consumer.close(option=CloseOptions.timeout(0))
            assert str(e.value) == CONCURRENT
        # The FFI close was never reached: nothing changed.
        assert calls == []
    finally:
        worker.join(WAIT)
    assert outcome == {"calls": ({TP0}, True)}
    consumer.close(option=CloseOptions.timeout(0))
    assert len(calls) == 1


def test_async_close_from_another_thread_fails_before_changing_anything(
        monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _spy_close(monkeypatch)
    polling, release = threading.Event(), threading.Event()
    outcome: dict[str, Any] = {}

    async def main(consumer: AsyncKafkaConsumer[bytes, bytes]) -> None:
        await consumer.assign(partitions=[TP0])
        task = asyncio.ensure_future(consumer.poll(timeout=1.0))
        await asyncio.sleep(0.1)
        polling.set()
        await task
        outcome["assignment"] = consumer.assignment()
        await asyncio.get_running_loop().run_in_executor(None, release.wait, WAIT)
        await consumer.close(option=CloseOptions.timeout(0))

    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    worker = threading.Thread(target=asyncio.run, args=(main(consumer),))
    worker.start()
    try:
        assert polling.wait(WAIT)
        with pytest.raises(ConcurrentModificationError) as e:
            asyncio.run(consumer.close(option=CloseOptions.timeout(0)))
        assert str(e.value) == CONCURRENT
        assert calls == []
    finally:
        release.set()
        worker.join(WAIT)
    assert outcome == {"assignment": {TP0}}
    assert len(calls) == 1


def test_close_races_calls_from_other_threads_without_touching_a_freed_handle() -> None:
    # Every native call is a counted use; close() frees the handle only once
    # none is left. Four threads hammer the consumer while it closes: each call
    # either returns, meets the single-owner guard or finds it closed.
    for _ in range(20):
        consumer = new_consumer()
        consumer.assign(partitions=[TP0])
        stop = threading.Event()
        unexpected: list[BaseException] = []

        def hammer() -> None:
            while not stop.is_set():
                try:
                    consumer.assignment()
                    consumer.paused()
                    consumer.wakeup()
                except (ConcurrentModificationError, IllegalStateError):
                    pass
                except BaseException as exc:  # noqa: BLE001
                    unexpected.append(exc)
                    return

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for t in threads:
            t.start()
        closed = False
        while not closed:
            try:
                consumer.close(option=CloseOptions.timeout(0))
                closed = True
            except ConcurrentModificationError:
                pass
        stop.set()
        for t in threads:
            t.join(WAIT)
        assert unexpected == []


def test_two_threads_closing_at_once_tear_down_once() -> None:
    for _ in range(20):
        consumer = new_consumer()
        barrier = threading.Barrier(2)
        errors: list[BaseException] = []

        def close() -> None:
            barrier.wait()
            try:
                consumer.close(option=CloseOptions.timeout(0))
            except ConcurrentModificationError:
                pass  # the other close is inside the consumer
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=close) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(WAIT)
        assert errors == []
        # Closed exactly once, whichever won.
        consumer.close()
        with pytest.raises(IllegalStateError):
            consumer.assignment()


def test_close_releases_the_listener() -> None:
    consumer = new_consumer()
    listener = ConsumerRebalanceListener()
    ref = weakref.ref(listener)
    consumer.subscribe(topics=[TOPIC], callback=listener)
    del listener
    gc.collect()
    assert ref() is not None
    consumer.close(option=CloseOptions.timeout(0))
    gc.collect()
    assert ref() is None


def test_a_listenerless_subscribe_releases_the_listener() -> None:
    with new_consumer() as consumer:
        listener = ConsumerRebalanceListener()
        ref = weakref.ref(listener)
        consumer.subscribe(topics=[TOPIC], callback=listener)
        del listener
        consumer.subscribe(topics=[TOPIC])
        gc.collect()
        assert ref() is None


# ---------------------------------------------------------------------------
# The commit callback runs on the caller's thread (C47)
# ---------------------------------------------------------------------------
def test_commit_callback_runs_on_the_thread_of_a_later_waiting_call() -> None:
    # An empty commit completes at once (Java's completedFuture(null), so the
    # callback's offsets are None); its callback runs in the next call that
    # executes the callbacks — here commit(), whose _async operation queues it,
    # and this thread runs it while waiting.
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        seen: list[tuple[Any, Any, int]] = []
        consumer.commit_nowait(offsets={}, callback=lambda o, e: seen.append(
            (o, e, threading.get_ident())))
        consumer.commit(offsets={})
        assert seen == [(None, None, threading.get_ident())]


def test_commit_callback_runs_inside_a_later_commit_nowait() -> None:
    # A synchronous call running the callbacks of earlier commits runs them on
    # its own thread.
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        seen: list[int] = []
        consumer.commit_nowait(offsets={}, callback=lambda o, e: seen.append(threading.get_ident()))
        assert seen == []
        # The core queues the completed commit's callback from a task of its
        # own; let it run, then a later commit_nowait() delivers it.
        time.sleep(0.2)
        consumer.commit_nowait(offsets={}, callback=lambda o, e: None)
        assert seen == [threading.get_ident()]


def test_commit_callback_may_call_back_into_the_consumer() -> None:
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        seen: dict[str, Any] = {}

        def callback(offsets: Any, exception: Any) -> None:
            seen["assignment"] = consumer.assignment()
            seen["paused"] = consumer.paused()

        consumer.commit_nowait(offsets={}, callback=callback)
        consumer.commit(offsets={})
        assert seen == {"assignment": {TP0}, "paused": set()}


def test_another_thread_during_the_commit_callback_meets_the_guard() -> None:
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        seen: dict[str, Any] = {}

        def callback(offsets: Any, exception: Any) -> None:
            def other() -> None:
                try:
                    consumer.assignment()
                    seen["other"] = "returned"
                except ConcurrentModificationError:
                    seen["other"] = "concurrent"

            t = threading.Thread(target=other)
            t.start()
            t.join(WAIT)

        consumer.commit_nowait(offsets={}, callback=callback)
        consumer.commit(offsets={})
        assert seen == {"other": "concurrent"}


def _listener_consumer() -> KafkaConsumer[bytes, bytes]:
    """A consumer subscribed with a listener: commit_nowait() then runs its FFI
    call on the helper thread (the broker is unreachable; empty-offsets commits
    complete at once)."""
    consumer = new_consumer()
    consumer.subscribe(topics=[TOPIC], callback=ConsumerRebalanceListener())
    return consumer


def test_commit_nowait_from_another_thread_while_one_runs_meets_the_guard() -> None:
    # Java's commitAsync() calls acquire(): a second thread's commitAsync()
    # while the first thread's is inside the consumer raises
    # ConcurrentModificationException, rather than waiting for it.
    consumer = _listener_consumer()
    started, release = threading.Event(), threading.Event()
    ran_on: list[int] = []

    def slow(offsets: Any, exception: Any) -> None:
        ran_on.append(threading.get_ident())
        started.set()
        release.wait(WAIT)

    try:
        consumer.commit_nowait(offsets={}, callback=slow)
        # The core queues the completed commit's callback from a task of its
        # own; let it, so the next commit_nowait() runs it inside its FFI call.
        time.sleep(0.2)
        outcome: dict[str, Any] = {}

        def first() -> None:
            try:
                consumer.commit_nowait(offsets={}, callback=lambda o, e: None)
                outcome["first"] = "returned"
            except BaseException as exc:  # noqa: BLE001
                outcome["first"] = exc

        worker = threading.Thread(target=first)
        worker.start()
        assert started.wait(WAIT)
        started_at = time.monotonic()
        with pytest.raises(ConcurrentModificationError) as e:
            consumer.commit_nowait(offsets={}, callback=lambda o, e: None)
        assert str(e.value) == CONCURRENT
        # Refused at once, not queued behind the first call.
        assert time.monotonic() - started_at < 1.0
        release.set()
        worker.join(WAIT)
        assert outcome == {"first": "returned"}
        # The earlier commit's callback ran inside the first call, on its thread.
        assert ran_on == [worker.ident]
    finally:
        release.set()
        consumer.close()


def test_commit_callbacks_run_on_the_thread_of_the_call_delivering_them() -> None:
    # Two threads in turn: each commit_nowait() runs the callbacks of the
    # earlier commits inside its FFI call on the helper thread, and hands them
    # back to its own caller (threads by name: an ident is reused).
    consumer = _listener_consumer()
    seen: dict[str, str] = {}

    def record(name: str) -> Any:
        return lambda o, e: seen.__setitem__(name, threading.current_thread().name)

    def commit_nowait_on(thread_name: str, callback_name: str) -> None:
        worker = threading.Thread(
            target=lambda: consumer.commit_nowait(offsets={}, callback=record(callback_name)),
            name=thread_name)
        worker.start()
        worker.join(WAIT)

    try:
        commit_nowait_on("thread-a", "first")
        time.sleep(0.2)
        commit_nowait_on("thread-b", "second")
        # The first commit's callback ran inside thread B's call, on thread B.
        assert seen == {"first": "thread-b"}
        time.sleep(0.2)
        # And the second's inside this thread's call.
        consumer.commit_nowait(offsets={}, callback=lambda o, e: None)
        assert seen == {"first": "thread-b", "second": threading.current_thread().name}
    finally:
        consumer.close()


def test_a_raising_commit_callback_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])

        def boom(offsets: Any, exception: Any) -> None:
            raise ValueError("callback boom")

        consumer.commit_nowait(offsets={}, callback=boom)
        with caplog.at_level("ERROR", logger="confluent_kafka.consumer"):
            consumer.commit(offsets={})
        assert any("Error in OffsetCommitCallback" in r.getMessage() for r in caplog.records)


def test_commit_callback_must_be_callable() -> None:
    with new_consumer() as consumer, pytest.raises(TypeError) as e:
        consumer.commit_nowait(callback=42)  # type: ignore[arg-type]
    assert str(e.value) == "callback must be callable"


def test_async_commit_callback_runs_on_the_event_loop() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.assign(partitions=[TP0])
        seen: list[int] = []
        consumer.commit_nowait(offsets={}, callback=lambda o, e: seen.append(
            threading.get_ident()))
        await consumer.commit(offsets={})
        assert seen == [threading.get_ident()]
        await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())


# ---------------------------------------------------------------------------
# Deserializers: the config route, and their close
# ---------------------------------------------------------------------------
class RecordingDeserializer:
    """A deserializer the config route builds: records configure and close."""

    instances: list[RecordingDeserializer] = []

    def __init__(self) -> None:
        self.configured: tuple[dict[str, Any], bool] | None = None
        self.closed = 0
        RecordingDeserializer.instances.append(self)

    def configure(self, configs: dict[str, Any], is_key: bool) -> None:
        self.configured = (dict(configs), is_key)

    def close(self) -> None:
        self.closed += 1

    def __call__(self, topic: str, data: memoryview | None, headers: Any = None) -> Any:
        return None if data is None else bytes(data).decode()


def test_the_config_route_builds_and_configures_the_deserializers() -> None:
    RecordingDeserializer.instances.clear()
    path = f"{__name__}.RecordingDeserializer"
    consumer = KafkaConsumer(configs=configs(**{"key.deserializer": path,
                                                "value.deserializer": RecordingDeserializer}))
    key, value = RecordingDeserializer.instances
    assert key.configured is not None and key.configured[1] is True
    assert value.configured is not None and value.configured[1] is False
    assert key.configured[0]["key.deserializer"] == path
    consumer.close(option=CloseOptions.timeout(0))
    # Java closes the deserializers at consumer close.
    assert (key.closed, value.closed) == (1, 1)


def test_a_deserializer_argument_wins_over_its_config_key() -> None:
    RecordingDeserializer.instances.clear()
    given = string_deserializer()
    consumer = KafkaConsumer(configs=configs(**{"value.deserializer": RecordingDeserializer}),
                             value_deserializer=given)
    # The config key is not used: nothing was built from it.
    assert RecordingDeserializer.instances == []
    assert consumer._value_deserializer is given
    consumer.close(option=CloseOptions.timeout(0))


def test_given_deserializers_are_closed_at_close_but_not_configured() -> None:
    RecordingDeserializer.instances.clear()
    key, value = RecordingDeserializer(), RecordingDeserializer()
    consumer = KafkaConsumer(configs=configs(), key_deserializer=key, value_deserializer=value)
    assert key.configured is None and value.configured is None
    consumer.close(option=CloseOptions.timeout(0))
    consumer.close()
    assert (key.closed, value.closed) == (1, 1)


def test_a_failed_construction_closes_the_deserializers_built() -> None:
    RecordingDeserializer.instances.clear()
    with pytest.raises(KafkaError):
        KafkaConsumer(configs=configs("", **{"key.deserializer": RecordingDeserializer,
                                             "value.deserializer": RecordingDeserializer}))
    assert [d.closed for d in RecordingDeserializer.instances] == [1, 1]


def test_no_deserializer_means_bytes() -> None:
    with new_consumer() as consumer:
        from confluent_kafka.common.serialization import bytes_deserializer
        assert type(consumer._key_deserializer) is type(bytes_deserializer())
        assert type(consumer._value_deserializer) is type(bytes_deserializer())


def test_async_consumer_closes_its_deserializers() -> None:
    async def main() -> None:
        key, value = RecordingDeserializer(), RecordingDeserializer()
        consumer = AsyncKafkaConsumer(configs=configs(), key_deserializer=key,
                                      value_deserializer=value)
        await consumer.close(option=CloseOptions.timeout(0))
        assert (key.closed, value.closed) == (1, 1)

    asyncio.run(main())


# ---------------------------------------------------------------------------
# The async consumer
# ---------------------------------------------------------------------------
def test_async_consumer_surface_without_a_broker() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.subscribe(topics=[TOPIC])
        assert consumer.subscription() == {TOPIC}
        await consumer.unsubscribe()
        await consumer.assign(partitions=[TP0])
        assert consumer.assignment() == {TP0}
        await consumer.pause(partitions=[TP0])
        assert consumer.paused() == {TP0}
        await consumer.resume(partitions=[TP0])
        with pytest.raises(IllegalArgumentError) as e:
            await consumer.seek(partition=TP0, offset=-1)
        assert str(e.value) == "seek offset must not be a negative number"
        await consumer.seek(partition=TP0, offset_and_metadata=OffsetAndMetadata(offset=3))
        # A plain def (its entry point has no _async form); nothing fetched,
        # so the lag is not known.
        assert consumer.current_lag(topic_partition=TP0) is None
        with pytest.raises(IllegalArgumentError):
            await consumer.poll(timeout=-1)
        with pytest.raises(IllegalArgumentError) as e:
            await consumer.subscribe(topics=[None])  # type: ignore[list-item]
        assert str(e.value) == ("Topic collection to subscribe to cannot contain null or "
                                "empty topic")
        with pytest.raises(IllegalArgumentError) as e:
            await consumer.assign(partitions=None)  # type: ignore[arg-type]
        assert str(e.value) == "Topic partitions collection to assign to cannot be null"
        with pytest.raises(IllegalArgumentError) as e:
            await consumer.assign(partitions=[None])  # type: ignore[list-item]
        assert str(e.value) == "Topic partitions to assign to cannot have null or empty topic"
        await consumer.close(option=CloseOptions.timeout(0))
        await consumer.close()
        with pytest.raises(IllegalStateError):
            consumer.assignment()
        with pytest.raises(IllegalStateError) as e:
            consumer.current_lag(topic_partition=TP0)
        assert str(e.value) == CLOSED

    asyncio.run(main())


def test_async_concurrent_use_on_one_loop_meets_the_guard() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.subscribe(topics=[TOPIC])
        poll = asyncio.ensure_future(consumer.poll(timeout=5))
        await asyncio.sleep(0.5)
        with pytest.raises(ConcurrentModificationError):
            consumer.assignment()
        consumer.wakeup()
        with pytest.raises(WakeupError):
            await poll
        await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())


def test_cancelling_an_async_poll_wakes_the_consumer_and_lets_it_end() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.subscribe(topics=[TOPIC])
        poll = asyncio.ensure_future(consumer.poll(timeout=30))
        await asyncio.sleep(0.5)
        started = time.monotonic()
        poll.cancel()
        with pytest.raises(asyncio.CancelledError):
            await poll
        assert time.monotonic() - started < 5
        # The call ended: the consumer is usable again.
        assert consumer.subscription() == {TOPIC}
        await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())


def test_an_async_result_its_closing_loop_refuses_is_freed(
        hold_completion: Any, freed_errors: list[int], stopped_loop: Any,
        capfd: pytest.CaptureFixture[str]) -> None:
    # The awaiting call's task waits on a loop that stopped; the loop is open
    # when the call's result (position()'s error for a partition not assigned)
    # checks it, then closes before call_soon_threadsafe. The result is freed
    # on the dispatcher thread instead of the RuntimeError reaching the C
    # trampoline, which printed it and left the result's handles allocated.
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    held = hold_completion("Consumer_position_async")
    stopped_loop.start(consumer.position(partition=TP0))
    assert held.submitted.is_set()
    loop = stopped_loop.loop
    loop.close()
    loop.is_closed = lambda: False  # type: ignore[method-assign] # the check saw it open
    capfd.readouterr()
    held.release.set()
    assert held.delivered.wait(WAIT)
    del loop.is_closed
    ((_, error),) = held.payloads
    assert error
    assert held.raised == []
    assert capfd.readouterr().err == ""
    assert freed_errors == [error]
    _close_bounded(consumer, stopped_loop)  # the call's task still pending


def test_an_async_result_left_for_a_closed_loop_is_freed_by_the_next_call(
        hold_completion: Any, freed_errors: list[int], stopped_loop: Any) -> None:
    # The result reaches its loop while the loop is stopped, and the loop then
    # closes, which discards the queued delivery: the consumer's next awaiting
    # call, from another loop, frees it.
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    held = hold_completion("Consumer_position_async")
    stopped_loop.start(consumer.position(partition=TP0))
    held.release.set()
    assert held.delivered.wait(WAIT)
    stopped_loop.loop.close()
    ((_, error),) = held.payloads
    assert error
    assert freed_errors == []
    asyncio.run(consumer.assign(partitions=[TP0]))
    assert freed_errors == [error]
    _close_bounded(consumer, stopped_loop)  # the call's task still pending


@pytest.mark.parametrize("same_thread", [True, False], ids=["same thread", "another thread"])
def test_close_does_not_wait_for_a_call_left_on_a_closed_loop(
        same_thread: bool, hold_completion: Any, freed_errors: list[int],
        stopped_loop: Any) -> None:
    # An awaiting call holds a use of the consumer until it ends. Its task is
    # left pending on a loop that stops, with its result queued there, and the
    # loop closes: the call can never resume. close() from a fresh loop, on the
    # call's thread or another one, returns and frees the stranded result; it
    # used to wait for that use for good (from another thread, to raise
    # ConcurrentModificationError).
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    held = hold_completion("Consumer_position_async")

    def leave_a_call() -> None:
        stopped_loop.start(consumer.position(partition=TP0))
        held.release.set()
        held.delivered.wait(WAIT)
        stopped_loop.loop.close()

    if same_thread:
        leave_a_call()
    else:
        caller = threading.Thread(target=leave_a_call)
        caller.start()
        caller.join(WAIT)
    assert held.delivered.is_set() and stopped_loop.loop.is_closed()
    ((_, error),) = held.payloads
    assert freed_errors == []
    _close_bounded(consumer, stopped_loop)
    assert freed_errors == [error]


def _close_bounded(consumer: AsyncKafkaConsumer[Any, Any], stopped_loop: Any) -> None:
    """``close()`` from a fresh loop on this thread, which must not wait for the
    call left on ``stopped_loop``. Should it wait, a timer ends that call's use
    after WAIT, so the test fails instead of hanging the suite."""
    fired = threading.Event()

    def end_the_call() -> None:
        fired.set()
        stopped_loop.abandon()

    watchdog = threading.Timer(WAIT, end_the_call)
    watchdog.start()
    try:
        asyncio.run(consumer.close(option=CloseOptions.timeout(0)))
    finally:
        watchdog.cancel()
    assert not fired.is_set(), "close() waited for a call whose event loop had closed"
    assert consumer._is_closed()  # noqa: SLF001


def test_wakeup_before_poll_raises_once() -> None:
    with new_consumer() as consumer:
        consumer.subscribe(topics=[TOPIC])
        consumer.wakeup()
        with pytest.raises(WakeupError):
            consumer.poll(timeout=1)
        # The next call proceeds normally.
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert consumer.poll(timeout=0).is_empty()
