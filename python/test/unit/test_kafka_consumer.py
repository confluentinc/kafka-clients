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

"""``KafkaConsumerTest.java`` (as of Apache Kafka 4.4.0) for ``KafkaConsumer`` without a
broker, in Java order, with Java's messages; and the binding's own contracts that
need no broker — the commit callback on the caller's thread (C47), the lifetime of
the native handle across a racing ``close()``, the deserializers' configuration
route and their close, the negative timeouts, the GIL released while the
constructor resolves the bootstrap hosts. Where Java drives a ``MockClient``
only to make an operation time out, an unreachable bootstrap address and a short
``default.api.timeout.ms`` stand for it. ``test/integration/test_kafka_consumer_broker.py``
runs, against a broker, the cases Java scripts through a ``MockClient``.

Of Java's 135 tests, 36 are translated here and 14 in the broker module; the
other 85, each named, are not, with the reason:

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
  ``testEnforceRebalanceReason``, ``testAssignorNameConflict``,
  ``testSubscribeToRe2jPatternNotSupportedForClassicConsumer``,
  ``testPauseFlagPreservedForRetainedPartitionAcrossRebalance``,
  ``testUnsubscribeDoesNotCommitOffsetsEvenWithAutoCommitEnabled`` and
  ``testClassicConsumerCloseRunsRevocationCallbackAndAttemptsLeaveGroupWhenInterrupted``. The broker module
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
  ``testFetchResponseWithUnexpectedPartitionIsIgnored`` (a fetch response naming an
  unassigned partition), ``testFetchStableOffsetThrowInCommitted``,
  ``testFetchStableOffsetThrowInPoll`` and
  ``testFetchStableOffsetThrowInPosition`` (an ``OffsetFetch`` version without
  ``requireStable``: an old broker), ``testPollAuthenticationFailure`` (a SASL
  authentication failure; the test broker has no SASL listener) and
  ``testConstructorFailsOnNetworkClientConstructorFailure`` (a mocked
  ``NetworkClient`` constructor), and ``testCurrentLag`` (a ``ListOffsets``
  answer held back to observe the lag before and after it; the core's
  translation is ``test_current_lag`` in ``rust/src/consumer/kafka_consumer.rs``,
  and the broker module checks the lag ``end_offsets()`` leaves);
- metrics: the KIP-1076 metric subscription is not generated
  (``testSubscribingCustomMetricsDoesntAffectConsumerMetrics``,
  ``testSubscribingCustomMetricsWithSameNameDoesntAffectConsumerMetrics``,
  ``testUnsubscribingCustomMetricsWithSameNameDoesntAffectConsumerMetrics``,
  ``testUnsubscribingNonExistingMetricsDoesntCauseError``); the core loads no
  metric reporter, JMX or telemetry reporter
  (``testShouldOnlyCallMetricReporterMetricChangeOnceWithExistingConsumerMetric``,
  ``testShouldNotCallMetricReporterMetricRemovalWithExistingConsumerMetric``,
  ``testMetricsReporterAutoGeneratedClientId``,
  ``testDisableJmxAndClientTelemetryReporter``,
  ``testExplicitlyOnlyEnableJmxReporter``,
  ``testExplicitlyOnlyEnableClientTelemetryReporter``,
  ``testConstructorInvalidMetricReporters`` — ``metric.reporters`` is not loaded,
  so an invalid class is not detected — and ``testConsumerJmxPrefix``);
  ``testMonitorablePlugins`` checks the ``Monitorable`` metrics of a
  deserializer and interceptor plugin, and the client runs no interceptors and
  registers no plugin metrics;
  ``testMetricConfigRecordingLevelInfo`` reads the private
  ``metricsRegistry().config()``; ``testPollTimeMetrics``, ``testPollIdleRatio``,
  ``testMeasureCommitSyncDurationOnFailure``, ``testMeasureCommitSyncDuration``,
  ``testMeasureCommittedDurationOnFailure`` and ``testMeasureCommittedDuration``
  assert values a ``MockTime`` makes exact, and the core's clock cannot be
  injected;
- not generated or dropped: ``testClientInstanceId``,
  ``testClientInstanceIdInvalidTimeout`` and
  ``testClientInstanceIdNoTelemetryReporterRegistered`` (``clientInstanceId`` is
  not generated, its entry point being missing); and
  ``testSubscriptionOnNullPattern`` for the dropped ``java.util.regex.Pattern``
  overloads (``testSubscriptionOnEmptyPattern`` runs as its ``SubscriptionPattern``
  counterpart);
- configuration: ``testInvalidSocketSendBufferSize`` /
  ``testInvalidSocketReceiveBufferSize`` (the core does not validate the
  ``send.buffer.bytes`` / ``receive.buffer.bytes`` ranges, Rust-core gap 11);
  ``testUnusedConfigs`` (only the core reports the keys it does not know, in its
  own log);
  ``testInterceptorConstructorClose`` and
  ``testInterceptorConstructorConfigurationWithExceptionShouldCloseRemainingInstances``
  (the client runs no interceptors: ``interceptor.classes`` is a key the core
  does not know).

``configurableObjectsShouldSeeGeneratedClientId`` runs without its interceptor,
for the two deserializers.

``testOperationsBySubscribingConsumerWithDefaultGroupId``'s
``enable.auto.commit=true`` half is not asserted (the core does not reject it,
Rust-core gap 11); ``testAssignedPartitionsMetrics``' group-assignment half needs
a broker, and ``testClosingConsumerUnregistersConsumerMetrics``' after-close half
reads ``metrics()`` after ``close()``, which raises ``IllegalStateError`` here,
as does ``testMetricsRemovedOnClose``' after-close half; ``testPollIdleRatioZero``
checks only the default value, its zero-length poll being recorded through the
package-private ``kafkaConsumerMetrics()`` on a ``MockTime``. Java runs most tests
for both group protocols; the translations run the consumer protocol's arm.
"""

from __future__ import annotations

import asyncio
import logging
import gc
import inspect
import math
import os
import signal
import sys
import threading
import time
import uuid
import warnings
import weakref
from datetime import timedelta
from typing import Any, Callable

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import (
    ConcurrentModificationError, IllegalArgumentError, IllegalStateError,
    UnsupportedOperationError,
)
from confluent_kafka.common.header import Headers
from confluent_kafka.common import KafkaError, TopicPartition
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.errors import BootstrapResolutionError
from confluent_kafka.common.errors import InvalidGroupIdError, WakeupError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.serialization import string_deserializer
from confluent_kafka.consumer import (
    AsyncConsumer, AsyncKafkaConsumer, AsyncMockConsumer, CloseOptions, CommitFailedError,
    ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata, SubscriptionPattern,
)
from confluent_kafka.consumer import _base as consumer_base
from confluent_kafka.consumer._base import poll_timeout_ms


class _NoOpListener(ConsumerRebalanceListener):
    """A listener whose callbacks do nothing, as Java's tests'
    ``NoOpConsumerRebalanceListener``."""

    def on_partitions_revoked(self, partitions: object) -> None:
        pass

    def on_partitions_assigned(self, partitions: object) -> None:
        pass



def _caller(deserializer: Any) -> Any:
    """The positional caller the consumer calls ``deserializer`` through
    (``_kwargs.serde_caller``), as ``deserialize_batch`` expects."""
    from confluent_kafka._kwargs import DESERIALIZER_NAMES, serde_caller

    return serde_caller(deserializer, DESERIALIZER_NAMES)

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
    # Java's subscribe((List) null) throws IllegalArgumentException: a None is
    # Java's null, so (topics) with a null collection (Signatures).
    with new_consumer() as consumer, pytest.raises(IllegalArgumentError) as e:
        consumer.subscribe(topics=None)  # type: ignore[arg-type]
    assert str(e.value) == "Topic collection to subscribe to cannot be None"


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
    assert str(e.value) == "Topic partitions collection to assign to cannot be None"


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
    ("pause", "partitions", TypeError, "partitions must not be None"),
    ("resume", "partitions", TypeError, "partitions must not be None"),
    ("offsets_for_times", "timestamps_to_search", TypeError,
     "timestamps_to_search must not be None"),
    ("seek_to_beginning", "partitions", IllegalArgumentError,
     "Partitions collection cannot be None"),
    ("seek_to_end", "partitions", IllegalArgumentError, "Partitions collection cannot be None"),
    ("beginning_offsets", "partitions", TypeError, "partitions must not be None"),
    ("end_offsets", "partitions", TypeError, "partitions must not be None"),
    # Java dereferences the null with no check of its own (TopicMetadataEvent's
    # Objects.requireNonNull(topic)): the same TypeError.
    ("partitions_for", "topic", TypeError, "topic must not be None"),
]


@pytest.mark.parametrize("method, keyword, error, message", NULL_ARGUMENT_CASES)
def test_a_null_argument_raises_java_s_error(method: str, keyword: str,
                                             error: type[Exception], message: str) -> None:
    with new_consumer(None) as consumer, pytest.raises(error) as e:
        getattr(consumer, method)(**{keyword: None})
    assert type(e.value) is error
    assert str(e.value) == message


def test_committed_with_null_partitions() -> None:
    # Java: throwIfGroupIdNotDefined(), then partitions.isEmpty() dereferences the
    # null with no check of its own; the group-less consumer reports the group first.
    with new_consumer() as consumer, pytest.raises(TypeError) as e:
        consumer.committed(partitions=None)  # type: ignore[arg-type]
    assert str(e.value) == "partitions must not be None"
    with new_consumer(None) as consumer, pytest.raises(InvalidGroupIdError) as g:
        consumer.committed(partitions=None)  # type: ignore[arg-type]
    assert str(g.value) == NO_GROUP_ID


def test_async_committed_with_null_partitions() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        with pytest.raises(TypeError) as e:
            await consumer.committed(partitions=None)  # type: ignore[arg-type]
        assert str(e.value) == "partitions must not be None"
        await consumer.close(option=CloseOptions.timeout(timeout=0))
        groupless: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs(None))
        with pytest.raises(InvalidGroupIdError) as g:
            await groupless.committed(partitions=None)  # type: ignore[arg-type]
        assert str(g.value) == NO_GROUP_ID
        await groupless.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


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
        await consumer.close(option=CloseOptions.timeout(timeout=0))
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


def test_pause_flag_preserved_for_retained_partition_on_manual_assignment_change() -> None:
    # Java runs it for both protocols; the consumer protocol's arm.
    tp1 = TopicPartition(topic=TOPIC, partition=1)
    with new_consumer() as consumer:
        # Manually assign two partitions and pause one of them.
        consumer.assign(partitions=[TP0, tp1])
        consumer.pause(partitions={TP0})
        assert consumer.paused() == {TP0}

        # Change the assignment while keeping tp0 assigned.
        consumer.assign(partitions=[TP0])
        assert consumer.assignment() == {TP0}
        # "The pause state of the partition that remains assigned must be preserved"
        assert consumer.paused() == {TP0}

        # A partition that is (re)added to the assignment starts unpaused.
        consumer.assign(partitions=[TP0, tp1])
        assert consumer.paused() == {TP0}


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
    # committed(partitions, Duration.ofMillis(1000)).
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        with pytest.raises(KafkaTimeoutError) as e:
            consumer.committed(partitions={TP0}, timeout=1.0)
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
    consumer.close(option=CloseOptions.timeout(timeout=0))


def test_beginning_offsets_timeout() -> None:
    consumer = new_consumer(**{"default.api.timeout.ms": 500})
    with pytest.raises(KafkaTimeoutError) as e:
        consumer.beginning_offsets(partitions=[TP0])
    assert str(e.value) == OFFSETS_TIMEOUT
    consumer.close(option=CloseOptions.timeout(timeout=0))


def test_end_offsets_timeout() -> None:
    consumer = new_consumer(**{"default.api.timeout.ms": 500})
    with pytest.raises(KafkaTimeoutError) as e:
        consumer.end_offsets(partitions=[TP0])
    assert str(e.value) == OFFSETS_TIMEOUT
    consumer.close(option=CloseOptions.timeout(timeout=0))


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


def test_metrics_removed_on_close() -> None:
    # Java runs it for both protocols; the consumer protocol's arm. Java then
    # checks that close() removed every metric; here metrics() after close()
    # raises IllegalStateError (use after close raises), so only Java's
    # assertMetricsMap(true) half is checked.
    consumer = KafkaConsumer(configs={"group.protocol": "consumer",
                                      "bootstrap.servers": "localhost:9999"},
                             key_deserializer=string_deserializer(),
                             value_deserializer=string_deserializer())
    # Copy the map because we're going to modify it.
    metrics = dict(consumer.metrics())
    # There's a meta-metric named "count" that is automatically added to the metrics map.
    count_names = [name for name in metrics
                   if name.name() == "count" and name.group() == "kafka-metrics-count"]
    # Make sure the meta-metric is present and has an entry.
    assert count_names, 'The "count" meta-metric was unexpectedly missing from the Consumer metrics'
    assert metrics.pop(count_names[0]) is not None, (
        'The "count" meta-metric key was removed from the Consumer metrics map, but it '
        "unexpectedly had no entry")
    assert metrics, "The consumer should have created metrics, but they are unexpectedly empty"
    consumer.close(option=CloseOptions.timeout(timeout=timedelta(0)))
    with pytest.raises(IllegalStateError) as e:
        consumer.metrics()
    assert str(e.value) == CLOSED


def test_poll_idle_ratio_zero() -> None:
    # Java runs it for both protocols; the consumer protocol's arm. Java then
    # records a poll that starts and ends within one MockTime millisecond through
    # the package-private kafkaConsumerMetrics() and expects 0.0; the core's
    # clock cannot be injected and that method is not generated, so only the
    # default value is checked.
    with new_consumer() as consumer:
        values = [metric.metric_value() for name, metric in consumer.metrics().items()
                  if name.name() == "poll-idle-ratio-avg" and name.group() == "consumer-metrics"]
        # Test default value
        assert len(values) == 1 and math.isnan(values[0])


def test_close_should_be_idempotent() -> None:
    consumer = new_consumer()
    consumer.close(option=CloseOptions.timeout(timeout=timedelta(0)))
    consumer.close(option=CloseOptions.timeout(timeout=timedelta(0)))
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
    with pytest.raises(UnsupportedOperationError) as e:
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
    consumer.close(option=CloseOptions.timeout(timeout=0))


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
    consumer.close(option=CloseOptions.timeout(timeout=0))
    with pytest.raises(IllegalStateError) as e:
        consumer.group_metadata()
    assert str(e.value) == CLOSED


def test_metrics_from_another_thread_while_polling() -> None:
    # Java's metrics() takes no acquire() (AsyncKafkaConsumer.metrics()), so a
    # metrics-reporter thread reads while the owner thread is inside poll().
    consumer = new_consumer()
    consumer.subscribe(topics=[TOPIC])
    worker, outcome = _poll_in_background(consumer)
    try:
        names = {metric_name.name() for metric_name in consumer.metrics()}
        assert "last-poll-seconds-ago" in names
        consumer.wakeup()
    finally:
        worker.join(WAIT)
    assert isinstance(outcome.get("error"), WakeupError)
    consumer.close(option=CloseOptions.timeout(timeout=0))


# Java's BootstrapResolutionException message (KIP-909), for a 3000 ms budget.
BOOTSTRAP_RESOLUTION_FAILED = ("Failed to resolve bootstrap servers after 3000ms. Please check "
                               "your bootstrap.servers configuration and DNS settings.")
# An RFC 6761 reserved .invalid name: DNS resolution fails.
UNRESOLVABLE_HOST = "unresolvable.invalid:9092"


def test_consumer_bootstrap_resolution_error_propagated_to_poll() -> None:
    # testConsumerBootstrapResolutionExceptionPropagatedToPoll (KIP-909). Java
    # runs it for both protocols; the consumer protocol's arm.
    with KafkaConsumer(configs={
            "bootstrap.servers": UNRESOLVABLE_HOST,
            # Set a short bootstrap timeout so the test doesn't take too long
            "bootstrap.resolve.timeout.ms": "3000",
            "group.protocol": "consumer",
            "group.id": "test-group"},
            key_deserializer=string_deserializer(),
            value_deserializer=string_deserializer()) as consumer:
        # Subscribe to a topic to trigger metadata fetch
        consumer.subscribe(topics={"test-topic"})

        # Poll continuously until we get the BootstrapResolutionError; it is
        # raised after bootstrap.resolve.timeout.ms expires.
        start = time.monotonic()
        max_wait = 15.0  # 15 seconds max to prevent test hanging
        with pytest.raises(BootstrapResolutionError) as e:
            while time.monotonic() - start < max_wait:
                consumer.poll(timeout=timedelta(milliseconds=100))
            pytest.fail(f"Expected BootstrapResolutionError to be thrown within {max_wait * 1000:.0f}ms")
        # Java accepts either of two phrasings; the core has one.
        assert str(e.value) == BOOTSTRAP_RESOLUTION_FAILED

        # After the first failure, any further API call must also raise. This
        # guards against accidentally clearing the bootstrap error from the
        # metadata layer.
        with pytest.raises(BootstrapResolutionError) as again:
            consumer.poll(timeout=timedelta(milliseconds=100))
        assert str(again.value) == BOOTSTRAP_RESOLUTION_FAILED


def test_consumer_constructor_fails_with_config_error_on_unresolvable_bootstrap_when_timeout_zero(
) -> None:
    # testConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero.
    # Java runs it for both protocols; the consumer protocol's arm. The default
    # bootstrap.resolve.timeout.ms=0 resolves DNS synchronously in the
    # constructor; the failure surfaces as ConfigError, wrapped in the
    # constructor's KafkaError, so no consumer is created.
    with pytest.raises(KafkaError) as e:
        KafkaConsumer(configs={"bootstrap.servers": UNRESOLVABLE_HOST,
                               "group.protocol": "consumer", "group.id": "test-group"},
                      key_deserializer=string_deserializer(),
                      value_deserializer=string_deserializer())
    assert type(e.value) is KafkaError
    assert str(e.value) == "Failed to construct kafka consumer"
    assert isinstance(e.value.__cause__, ConfigError)
    assert str(e.value.__cause__) == "No resolvable bootstrap urls given in bootstrap.servers"


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
    # The branch C API names the typed-config constructor Consumer_KafkaConsumer_new.
    native_new = _lib.Consumer_KafkaConsumer_new

    def timed_new(native: dict[str, str]) -> Any:
        window["start"] = time.perf_counter()
        try:
            return native_new(native)
        finally:
            window["end"] = time.perf_counter()

    monkeypatch.setattr(_lib, "Consumer_KafkaConsumer_new", timed_new)
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
    consumer.close(option=CloseOptions.timeout(timeout=-1))

    async def main() -> None:
        async_consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await async_consumer.close()
        with pytest.raises(IllegalStateError) as e:
            await async_consumer.poll(timeout=-1)
        assert str(e.value) == CLOSED
        await async_consumer.close(option=CloseOptions.timeout(timeout=-1))

    asyncio.run(main())


def test_close_rejects_a_negative_timeout_and_stays_open() -> None:
    consumer = new_consumer()
    with pytest.raises(IllegalArgumentError) as e:
        consumer.close(option=CloseOptions.timeout(timeout=-1))
    assert str(e.value) == "The timeout cannot be negative."
    with pytest.raises(IllegalArgumentError):
        consumer.close(option=CloseOptions.timeout(timeout=timedelta(seconds=-1)))
    assert consumer.subscription() == set()
    consumer.close(option=CloseOptions.timeout(timeout=0))


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
        consumer.close(option=CloseOptions.timeout(timeout=1e300))
    assert str(e.value) == TOO_LONG
    with pytest.raises(OverflowError):
        consumer.close(option=CloseOptions.timeout(timeout=float("inf")))
    with pytest.raises(ValueError):
        consumer.close(option=CloseOptions.timeout(timeout=float("nan")))
    # Still open: nothing reached the FFI.
    assert consumer.assignment() == {TP0}
    assert consumer.poll(timeout=0).is_empty()
    consumer.close(option=CloseOptions.timeout(timeout=0))
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
            await consumer.close(option=CloseOptions.timeout(timeout=1e300))
        assert str(e.value) == TOO_LONG
        with pytest.raises(ValueError):
            await consumer.close(option=CloseOptions.timeout(timeout=float("nan")))
        assert consumer.assignment() == {TP0}
        await consumer.close(option=CloseOptions.timeout(timeout=0))
        with pytest.raises(IllegalStateError) as e:
            consumer.assignment()
        assert str(e.value) == CLOSED

    asyncio.run(main())


# ---------------------------------------------------------------------------
# The Duration overloads (Signatures, Timeouts): the optional ``timeout``
# ---------------------------------------------------------------------------
# A given timeout calls the `_with_timeout` entry point, None the overload
# without it, bounded by default.api.timeout.ms; Java's own TimeoutException
# messages carry the bound that applied.
COMMITTED_TIMEOUT = ("Timeout of {}ms expired before the last committed offset for partitions "
                     "[test-0] could be determined. Try tuning default.api.timeout.ms larger to "
                     "relax the threshold.")
TIMED: dict[str, tuple[Callable[..., Any], str]] = {
    "commit": (lambda c, **t: c.commit(offsets={TP0: OffsetAndMetadata(offset=1)}, **t),
               "Timeout of {}ms expired before successfully committing offsets"),
    "position": (lambda c, **t: c.position(partition=TP0, **t),
                 "Timeout of {}ms expired before the position for partition test-0 could be "
                 "determined"),
    "committed": (lambda c, **t: c.committed(partitions={TP0}, **t), COMMITTED_TIMEOUT),
    "offsets_for_times": (lambda c, **t: c.offsets_for_times(timestamps_to_search={TP0: 0}, **t),
                          "Failed to get offsets by times in {}ms"),
    "beginning_offsets": (lambda c, **t: c.beginning_offsets(partitions=[TP0], **t),
                          "Failed to get offsets by times in {}ms"),
    "end_offsets": (lambda c, **t: c.end_offsets(partitions=[TP0], **t),
                    "Failed to get offsets by times in {}ms"),
}
# partitionsFor / listTopics: Java throws at once for a zero timeout; the
# untimed overload waits out default.api.timeout.ms. An expired lookup has
# either of two messages, depending on which side the deadline reaches first:
# the request manager's (Java's TopicMetadataRequestManager) or the
# application side's wait for the event.
METADATA_EXPIRED_BY_MANAGER = "Timeout expired while fetching topic metadata"
METADATA: dict[str, tuple[Callable[..., Any], str, set[str]]] = {
    "partitions_for": (lambda c, **t: c.partitions_for(topic=TOPIC, **t),
                       "Timeout of 0ms expired before partitions for topic test could be "
                       "determined",
                       {METADATA_EXPIRED_BY_MANAGER,
                        "Timeout expired while waiting for TopicMetadata"}),
    "list_topics": (lambda c, **t: c.list_topics(**t),
                    "Timeout of 0ms expired before all topics' metadata could be listed",
                    {METADATA_EXPIRED_BY_MANAGER,
                     "Timeout expired while waiting for AllTopicsMetadata"}),
}
# Every Duration overload, with Java's negative-timeout message: a Timer's where
# Java builds one from the timeout (commitSync, position), else the core's.
NEGATIVE: dict[str, tuple[Callable[..., Any], str]] = {
    **{name: (call, "Invalid negative timeout {}") for name, (call, _) in TIMED.items()
       if name in ("commit", "position")},
    **{name: (call, "Timeout must not be negative") for name, (call, _) in TIMED.items()
       if name not in ("commit", "position")},
    **{name: (call, "Timeout must not be negative") for name, (call, _, _) in METADATA.items()},
}


@pytest.mark.parametrize("method", sorted(TIMED))
def test_a_duration_overload_waits_up_to_its_timeout(method: str) -> None:
    call, message = TIMED[method]
    # default.api.timeout.ms bounds the untimed overload, the timeout the timed one.
    with new_consumer(**{"default.api.timeout.ms": 300}) as consumer:
        consumer.assign(partitions=[TP0])
        with pytest.raises(KafkaTimeoutError) as e:
            call(consumer)
        assert str(e.value) == message.format(300)
        with pytest.raises(KafkaTimeoutError) as e:
            call(consumer, timeout=0.2)
        assert str(e.value) == message.format(200)
        with pytest.raises(KafkaTimeoutError) as e:
            call(consumer, timeout=timedelta(milliseconds=150))
        assert str(e.value) == message.format(150)


@pytest.mark.parametrize("method", sorted(METADATA))
def test_a_metadata_duration_overload_waits_up_to_its_timeout(method: str) -> None:
    call, zero, expired = METADATA[method]
    with new_consumer(**{"default.api.timeout.ms": 300}) as consumer:
        started = time.monotonic()
        with pytest.raises(KafkaTimeoutError) as e:
            call(consumer, timeout=0)
        assert str(e.value) == zero
        with pytest.raises(KafkaTimeoutError) as e:
            call(consumer, timeout=0.2)
        assert str(e.value) in expired
        with pytest.raises(KafkaTimeoutError) as e:
            call(consumer)
        assert str(e.value) in expired
        assert time.monotonic() - started < WAIT


def test_a_zero_timeout_returns_the_offsets_lookups_at_once() -> None:
    # AsyncKafkaConsumer: offsetsForTimes returns the empty results,
    # beginningOffsets / endOffsets an empty map, without waiting.
    with new_consumer(**{"default.api.timeout.ms": 60_000}) as consumer:
        started = time.monotonic()
        assert consumer.offsets_for_times(timestamps_to_search={TP0: 0}, timeout=0) == {TP0: None}
        assert consumer.beginning_offsets(partitions=[TP0], timeout=0) == {}
        assert consumer.end_offsets(partitions=[TP0], timeout=timedelta(0)) == {}
        assert time.monotonic() - started < WAIT


@pytest.mark.parametrize("method", sorted(NEGATIVE))
def test_a_duration_overload_rejects_a_negative_timeout(method: str) -> None:
    call, message = NEGATIVE[method]
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        with pytest.raises(IllegalArgumentError) as e:
            call(consumer, timeout=-1)
        assert str(e.value) == message.format(-1000)
        with pytest.raises(IllegalArgumentError) as e:
            call(consumer, timeout=timedelta(microseconds=-500))
        assert str(e.value) == message.format(-1)
        assert consumer.assignment() == {TP0}


@pytest.mark.parametrize("method", sorted(NEGATIVE))
def test_a_duration_overload_raises_overflow_and_nan_before_the_ffi(method: str) -> None:
    call, _ = NEGATIVE[method]
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        with pytest.raises(OverflowError) as e:
            call(consumer, timeout=1e300)
        assert str(e.value) == TOO_LONG
        with pytest.raises(OverflowError):
            call(consumer, timeout=float("inf"))
        with pytest.raises(ValueError):
            call(consumer, timeout=float("nan"))
        assert consumer.assignment() == {TP0}


@pytest.mark.parametrize("method", sorted(NEGATIVE))
def test_a_duration_overload_checks_the_closed_state_first(method: str) -> None:
    call, _ = NEGATIVE[method]
    consumer = new_consumer()
    consumer.close(option=CloseOptions.timeout(timeout=0))
    for timeout in (-1, float("nan")):
        with pytest.raises(IllegalStateError) as e:
            call(consumer, timeout=timeout)
        assert str(e.value) == CLOSED


@pytest.mark.parametrize("method", sorted(TIMED))
def test_a_timed_call_inside_a_callback_passes_its_timeout(method: str) -> None:
    # Inside a callback the call goes through the core's ConsumerHandle,
    # whose Duration overloads (kafka_consumer_ConsumerHandle_<method>
    # _with_timeout) take the timeout, as the consumer's do: it bounds the
    # wait, not default.api.timeout.ms. Updated for the handle's timed forms
    # (was: refused with ConcurrentModificationError, run decision 18b).
    call, message = TIMED[method]
    with new_consumer(**{"default.api.timeout.ms": 60_000}) as consumer:
        consumer.assign(partitions=[TP0])
        raised: list[BaseException] = []

        def callback(**kwargs: Any) -> None:
            for timeout in (0.2, -1):
                try:
                    call(consumer, timeout=timeout)
                except BaseException as exc:  # noqa: BLE001 - asserted below
                    raised.append(exc)

        consumer.commit_nowait(offsets={}, callback=callback)
        started = time.monotonic()
        consumer.commit(offsets={})
        assert time.monotonic() - started < WAIT
        timed_out, negative = raised
        assert isinstance(timed_out, KafkaTimeoutError)
        assert str(timed_out) == message.format(200)
        assert isinstance(negative, IllegalArgumentError)
        assert str(negative) == NEGATIVE[method][1].format(-1000)


@pytest.mark.parametrize("method", sorted(METADATA))
def test_a_metadata_call_inside_a_callback_goes_through_the_handle(method: str) -> None:
    # partitions_for / list_topics inside a callback: the handle's forms
    # (kafka_consumer_ConsumerHandle_partitions_for / _list_topics, and their
    # _with_timeout forms) answer as the consumer's do. Updated (was: refused
    # with ConcurrentModificationError, the handle having no such form).
    call, zero, expired = METADATA[method]
    with new_consumer(**{"default.api.timeout.ms": 300}) as consumer:
        consumer.assign(partitions=[TP0])
        raised: list[BaseException] = []

        def callback(**kwargs: Any) -> None:
            for timeout in (0, 0.2, None):
                try:
                    if timeout is None:  # the untimed overload
                        call(consumer)
                    else:
                        call(consumer, timeout=timeout)
                except BaseException as exc:  # noqa: BLE001 - asserted below
                    raised.append(exc)

        consumer.commit_nowait(offsets={}, callback=callback)
        consumer.commit(offsets={})
        assert [type(e) for e in raised] == [KafkaTimeoutError] * 3
        assert str(raised[0]) == zero
        assert str(raised[1]) in expired and str(raised[2]) in expired


def test_group_metadata_inside_a_callback_goes_through_the_handle() -> None:
    # group_metadata() inside a callback reads the handle's shared slot
    # (kafka_consumer_ConsumerHandle_group_metadata), the delivering call
    # holding the consumer's guard.
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        seen: list[Any] = []

        def callback(**kwargs: Any) -> None:
            seen.append(consumer.group_metadata())

        consumer.commit_nowait(offsets={}, callback=callback)
        consumer.commit(offsets={})
        (metadata,) = seen
        outside = consumer.group_metadata()
        assert metadata.group_id() == outside.group_id() == GROUP_ID
        assert metadata.generation_id() == outside.generation_id()
        assert metadata.member_id() == outside.member_id()


@pytest.mark.parametrize("method", ["assign", "subscribe", "subscription", "unsubscribe", "poll",
                                    "close"])
def test_what_the_handle_has_no_form_of_is_refused_inside_a_callback(method: str) -> None:
    # assign, subscribe, subscription, unsubscribe, poll and close have no
    # ConsumerHandle form (KIP-1306's RebalanceConsumer has none of them):
    # inside a callback they reach the busy consumer and raise
    # ConcurrentModificationError, as Java's acquire() does from another thread.
    calls: dict[str, Callable[[Any], Any]] = {
        "assign": lambda c: c.assign(partitions=[TP0]),
        "subscription": lambda c: c.subscription(),
        "subscribe": lambda c: c.subscribe(topics=[TOPIC]),
        "unsubscribe": lambda c: c.unsubscribe(),
        "poll": lambda c: c.poll(timeout=0),
        "close": lambda c: c.close(option=CloseOptions.timeout(timeout=0)),
    }
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        raised: list[BaseException] = []

        def callback(**kwargs: Any) -> None:
            try:
                calls[method](consumer)
            except BaseException as exc:  # noqa: BLE001 - asserted below
                raised.append(exc)

        consumer.commit_nowait(offsets={}, callback=callback)
        consumer.commit(offsets={})
        assert [type(e) for e in raised] == [ConcurrentModificationError]
        assert str(raised[0]) == CONCURRENT


def test_a_timed_position_inside_a_callback_passes_its_timeout() -> None:
    with new_consumer(**{"default.api.timeout.ms": 60_000}) as consumer:
        consumer.assign(partitions=[TP0])
        raised: list[BaseException] = []

        def callback(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
            try:
                consumer.position(partition=TP0, timeout=0.2)
            except BaseException as exc:  # noqa: BLE001 - asserted below
                raised.append(exc)

        consumer.commit_nowait(offsets={}, callback=callback)
        started = time.monotonic()
        consumer.commit(offsets={})
        assert time.monotonic() - started < WAIT
        (error,) = raised
        assert isinstance(error, KafkaTimeoutError)
        assert str(error) == TIMED["position"][1].format(200)


def test_current_lag_waits_for_the_background_task() -> None:
    # Java's currentLag sends a CurrentLagEvent and waits for its answer
    # (addAndGet): the sync class waits on the _cb form. No end offset is known,
    # so the lag is not either; an unassigned partition is the IllegalStateError
    # SubscriptionState raises.
    unassigned = TopicPartition(topic=TOPIC, partition=7)
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        assert consumer.current_lag(topic_partition=TP0) is None
        with pytest.raises(IllegalStateError) as e:
            consumer.current_lag(topic_partition=unassigned)
        assert str(e.value) == f"No current assignment for partition {unassigned}"


def test_current_lag_reads_the_ffi_presence_flag() -> None:
    # The FFI carries Java's OptionalLong as (present, lag): a present lag may
    # be negative (a position past the log end) and is kept, and only the flag
    # marks the empty one, whatever the lag beside it.
    from confluent_kafka.consumer._base import _lag_or_none
    assert _lag_or_none((1, -5)) == -5
    assert _lag_or_none((1, 0)) == 0
    assert _lag_or_none((1, 6)) == 6
    assert _lag_or_none((0, 0)) is None


def test_current_lag_inside_a_callback_goes_through_the_handle(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Inside a callback the consumer is held by the call delivering it, so
    # current_lag reaches it through the ConsumerHandle's blocking form, as
    # Java's callback calls the consumer on the same thread.
    calls: list[tuple[Any, ...]] = []
    handle_current_lag = _lib.ConsumerHandle_current_lag

    def recording(*args: Any) -> Any:
        calls.append(args[1:])
        return handle_current_lag(*args)

    monkeypatch.setattr(_lib, "ConsumerHandle_current_lag", recording)
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        answered: list[object] = []

        def callback(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
            try:
                answered.append(consumer.current_lag(topic_partition=TP0))
            except BaseException as exc:  # noqa: BLE001 - asserted below
                answered.append(exc)

        consumer.commit_nowait(offsets={}, callback=callback)
        consumer.commit(offsets={})
        assert answered == [None]
        assert calls == [(TOPIC, 0)]


@pytest.mark.parametrize("method", sorted(NEGATIVE))
def test_a_negative_timeout_from_another_thread_meets_the_guard_first(method: str) -> None:
    # Java checks the timeout after acquireAndEnsureOpen(): a call while
    # another thread is inside the consumer raises
    # ConcurrentModificationException, whatever its timeout.
    call, _ = NEGATIVE[method]
    with new_consumer() as consumer:
        inside, leave = threading.Event(), threading.Event()

        def hold() -> None:
            with consumer._use():  # noqa: SLF001
                inside.set()
                leave.wait(WAIT)

        holder = threading.Thread(target=hold)
        holder.start()
        try:
            assert inside.wait(WAIT)
            with pytest.raises(ConcurrentModificationError) as e:
                call(consumer, timeout=-1)
            assert str(e.value) == CONCURRENT
        finally:
            leave.set()
            holder.join(WAIT)


@pytest.mark.parametrize("method", sorted(NEGATIVE))
def test_a_negative_timeout_without_a_group_id(method: str) -> None:
    # Java's commitSync and committed check the group.id before the timeout
    # (throwIfGroupIdNotDefined); the others reach the timeout check.
    call, message = NEGATIVE[method]
    with new_consumer(group_id=None) as consumer:
        consumer.assign(partitions=[TP0])
        if method in ("commit", "committed"):
            with pytest.raises(InvalidGroupIdError) as e:
                call(consumer, timeout=-1)
            assert str(e.value) == NO_GROUP_ID
        else:
            with pytest.raises(IllegalArgumentError) as e:
                call(consumer, timeout=-1)
            assert str(e.value) == message.format(-1000)


def test_the_duration_overloads_are_keyword_only() -> None:
    with new_consumer() as consumer:
        c: Any = consumer
        for positional in (lambda: c.commit(None, 1.0), lambda: c.position(TP0, 1.0),
                           lambda: c.committed({TP0}, 1.0), lambda: c.partitions_for(TOPIC, 1.0),
                           lambda: c.list_topics(1.0),
                           lambda: c.offsets_for_times({TP0: 0}, 1.0),
                           lambda: c.beginning_offsets([TP0], 1.0),
                           lambda: c.end_offsets([TP0], 1.0)):
            with pytest.raises(TypeError):
                positional()


def test_the_async_duration_overloads() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(
            configs=configs(**{"default.api.timeout.ms": 300}))
        await consumer.assign(partitions=[TP0])
        cases: list[tuple[Callable[..., Any], str]] = [
            (lambda **t: consumer.commit(offsets={TP0: OffsetAndMetadata(offset=1)}, **t),
             TIMED["commit"][1]),
            (lambda **t: consumer.position(partition=TP0, **t), TIMED["position"][1]),
            (lambda **t: consumer.committed(partitions={TP0}, **t), COMMITTED_TIMEOUT),
            (lambda **t: consumer.offsets_for_times(timestamps_to_search={TP0: 0}, **t),
             TIMED["offsets_for_times"][1]),
            (lambda **t: consumer.beginning_offsets(partitions=[TP0], **t),
             TIMED["beginning_offsets"][1]),
            (lambda **t: consumer.end_offsets(partitions=[TP0], **t), TIMED["end_offsets"][1]),
        ]
        for call, message in cases:
            with pytest.raises(KafkaTimeoutError) as e:
                await call()
            assert str(e.value) == message.format(300)
            with pytest.raises(KafkaTimeoutError) as e:
                await call(timeout=0.2)
            assert str(e.value) == message.format(200)
            with pytest.raises(IllegalArgumentError):
                await call(timeout=-1)
            with pytest.raises(OverflowError):
                await call(timeout=float("inf"))
        with pytest.raises(KafkaTimeoutError) as e:
            await consumer.partitions_for(topic=TOPIC, timeout=0)
        assert str(e.value) == METADATA["partitions_for"][1]
        with pytest.raises(KafkaTimeoutError) as e:
            await consumer.list_topics(timeout=0)
        assert str(e.value) == METADATA["list_topics"][1]
        with pytest.raises(IllegalArgumentError) as e:
            await consumer.position(partition=TP0, timeout=-1)
        assert str(e.value) == "Invalid negative timeout -1000"
        with pytest.raises(IllegalArgumentError) as e:
            await consumer.list_topics(timeout=-1)
        assert str(e.value) == "Timeout must not be negative"
        with pytest.raises(TypeError):
            await consumer.position(TP0, 1.0)  # type: ignore[misc]
        assert await consumer.end_offsets(partitions=[TP0], timeout=0) == {}
        await consumer.close(option=CloseOptions.timeout(timeout=0))
        with pytest.raises(IllegalStateError) as e:
            await consumer.committed(partitions={TP0}, timeout=-1)
        assert str(e.value) == CLOSED

    asyncio.run(main())


def test_close_with_a_timeout_is_not_generated() -> None:
    # Java's @Deprecated close(Duration) (CLAUDE.md, Python Binding Conventions,
    # Class family); the consumer stays open.
    consumer = new_consumer()
    with pytest.raises(TypeError):
        consumer.close(timeout=0)  # type: ignore[call-arg]
    assert consumer.subscription() == set()
    consumer.close(option=CloseOptions.timeout(timeout=0))


def test_close_twice_is_harmless_and_calls_after_it_raise() -> None:
    consumer = new_consumer()
    consumer.close(option=CloseOptions.timeout(timeout=0))
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
            consumer.close(option=CloseOptions.timeout(timeout=0))
        consumer.wakeup()
    finally:
        worker.join(WAIT)
    assert isinstance(outcome.get("error"), WakeupError)
    assert consumer.subscription() == {TOPIC}
    consumer.close(option=CloseOptions.timeout(timeout=0))


def _spy_close(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    import _confluentkafka as lib  # type: ignore[import-not-found]

    calls: list[Any] = []
    # The branch C API's non-blocking close is Consumer_close_cb.
    original = lib.Consumer_close_cb

    def spy(*args: Any) -> Any:
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(lib, "Consumer_close_cb", spy)
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
                consumer.close(option=CloseOptions.timeout(timeout=0))
            assert str(e.value) == CONCURRENT
        # The FFI close was never reached: nothing changed.
        assert calls == []
    finally:
        worker.join(WAIT)
    assert outcome == {"calls": ({TP0}, True)}
    consumer.close(option=CloseOptions.timeout(timeout=0))
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
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    worker = threading.Thread(target=asyncio.run, args=(main(consumer),))
    worker.start()
    try:
        assert polling.wait(WAIT)
        # Updated for the one-loop binding (was: the guard's
        # ConcurrentModificationError): a close() from another thread's loop
        # is refused before it changes anything, as the consumer is bound to
        # the worker's loop.
        with pytest.raises(RuntimeError) as e:
            asyncio.run(consumer.close(option=CloseOptions.timeout(timeout=0)))
        assert str(e.value) == BOUND_ELSEWHERE
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
                consumer.close(option=CloseOptions.timeout(timeout=0))
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
                consumer.close(option=CloseOptions.timeout(timeout=0))
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
    listener = _NoOpListener()
    ref = weakref.ref(listener)
    consumer.subscribe(topics=[TOPIC], callback=listener)
    del listener
    gc.collect()
    assert ref() is not None
    consumer.close(option=CloseOptions.timeout(timeout=0))
    gc.collect()
    assert ref() is None


def test_a_listenerless_subscribe_releases_the_listener() -> None:
    with new_consumer() as consumer:
        listener = _NoOpListener()
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
        consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_:
                                                        seen.append((offsets, exception, threading.get_ident())))
        consumer.commit(offsets={})
        assert seen == [(None, None, threading.get_ident())]


def test_commit_callback_runs_inside_a_later_commit_nowait() -> None:
    # A synchronous call running the callbacks of earlier commits runs them on
    # its own thread.
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        seen: list[int] = []
        consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_:
                                                        seen.append(threading.get_ident()))
        assert seen == []
        # The core queues the completed commit's callback from a task of its
        # own; let it run, then a later commit_nowait() delivers it.
        time.sleep(0.2)
        consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_: None)
        assert seen == [threading.get_ident()]


def test_commit_callback_may_call_back_into_the_consumer() -> None:
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        seen: dict[str, Any] = {}

        def callback(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
            seen["assignment"] = consumer.assignment()
            seen["paused"] = consumer.paused()

        consumer.commit_nowait(offsets={}, callback=callback)
        consumer.commit(offsets={})
        assert seen == {"assignment": {TP0}, "paused": set()}


def test_another_thread_during_the_commit_callback_meets_the_guard() -> None:
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        seen: dict[str, Any] = {}

        def callback(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
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
    """A consumer subscribed with a listener (the broker is unreachable;
    empty-offsets commits complete at once). As in Java, commit_nowait()
    delivers no listener callback, only poll() and unsubscribe() do; the
    callbacks the branch C API does run go through the waiting call's drain on
    its caller's thread, so there is no helper thread."""
    consumer = new_consumer()
    consumer.subscribe(topics=[TOPIC], callback=_NoOpListener())
    return consumer


def test_commit_nowait_from_another_thread_while_one_runs_meets_the_guard() -> None:
    # Java's commitAsync() calls acquire(): a second thread's commitAsync()
    # while the first thread's is inside the consumer raises
    # ConcurrentModificationException, rather than waiting for it.
    consumer = _listener_consumer()
    started, release = threading.Event(), threading.Event()
    ran_on: list[int] = []

    def slow(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
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
                consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_: None)
                outcome["first"] = "returned"
            except BaseException as exc:  # noqa: BLE001
                outcome["first"] = exc

        worker = threading.Thread(target=first)
        worker.start()
        assert started.wait(WAIT)
        started_at = time.monotonic()
        with pytest.raises(ConcurrentModificationError) as e:
            consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_: None)
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
    # earlier commits on its own caller's thread, in the drain of its wait
    # (threads by name: an ident is reused). The branch C API has no helper
    # thread; the guarantee tested is the same.
    consumer = _listener_consumer()
    seen: dict[str, str] = {}

    def record(name: str) -> Any:
        return lambda *, offsets, exception, **_: seen.__setitem__(
            name, threading.current_thread().name)

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
        consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_: None)
        assert seen == {"first": "thread-b", "second": threading.current_thread().name}
    finally:
        consumer.close()


class CommitCallbackError(ValueError):
    pass


def queue_a_raising_commit_callback(consumer: KafkaConsumer[bytes, bytes], raised: BaseException,
                                    events: list[str]) -> None:
    """Queue a commit callback that raises ``raised``, with one more callback
    queued behind it: the raising one queues it (an empty commit completes at
    once) before it raises."""
    def boom(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
        events.append("boom")
        consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_:
                                                        events.append('next'))
        raise raised

    consumer.commit_nowait(offsets={}, callback=boom)


# Java's OffsetCommitCallbackInvoker.executeCallbacks calls onComplete without a
# try: the exception propagates out of the call that runs it -- commitSync and
# commitAsync as it is (AsyncKafkaConsumer.commit runs executeCallbacks first),
# poll wrapped by maybeWrapAsKafkaException after clearing the inflight poll --
# and the callbacks queued behind it stay queued for the next call.
@pytest.mark.parametrize("delivering", ["commit", "commit_nowait", "poll"])
def test_a_raising_commit_callback_propagates_from_the_delivering_call(delivering: str) -> None:
    raised = CommitCallbackError("callback boom")
    events: list[str] = []
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        queue_a_raising_commit_callback(consumer, raised, events)
        if delivering == "poll":
            with pytest.raises(KafkaError) as wrapped:
                consumer.poll(timeout=0)
            # Java's new KafkaException(t): the message is t.toString().
            assert type(wrapped.value) is KafkaError
            assert str(wrapped.value) == f"{__name__}.CommitCallbackError: callback boom"
            assert wrapped.value.__cause__ is raised
        else:
            with pytest.raises(CommitCallbackError) as e:
                if delivering == "commit":
                    consumer.commit(offsets={})
                else:
                    consumer.commit_nowait(offsets={}, callback=None)
            assert e.value is raised
            assert str(e.value) == "callback boom"
        assert events == ["boom"]
        # The callback queued behind it runs inside the next call, and the
        # consumer goes on (the inflight poll was cleared).
        consumer.commit(offsets={})
        assert events == ["boom", "next"]
        assert len(consumer.poll(timeout=0)) == 0


def test_a_raising_commit_callback_propagates_from_a_commit_inside_a_callback() -> None:
    # A commit through the ConsumerHandle runs the queued callbacks first, as
    # Java's commit() does: the exception propagates out of it, into the
    # callback that issued it.
    raised = CommitCallbackError("callback boom")
    seen: list[BaseException] = []
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])

        def raising(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
            raise raised

        def first(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
            consumer.commit_nowait(offsets={}, callback=raising)
            try:
                consumer.commit(offsets={})
            except CommitCallbackError as exc:
                seen.append(exc)

        consumer.commit_nowait(offsets={}, callback=first)
        consumer.commit(offsets={})
        assert len(seen) == 1 and seen[0] is raised


@pytest.mark.parametrize("raised", [CommitCallbackError("callback boom"), CommitFailedError()],
                         ids=["value-error", "kafka-error"])
def test_close_wraps_a_raising_commit_callback(raised: BaseException) -> None:
    # Java's close(): the error of awaitPendingAsyncCommitsAndExecuteCommitCallbacks
    # is kept and thrown as KafkaException("Failed to close kafka consumer", e),
    # a KafkaException too (AsyncKafkaConsumer.java:1669-1675; unlike poll's
    # maybeWrapAsKafkaException, which leaves one unwrapped).
    events: list[str] = []
    consumer = new_consumer(**{"enable.auto.commit": False})
    consumer.assign(partitions=[TP0])
    queue_a_raising_commit_callback(consumer, raised, events)
    with pytest.raises(KafkaError) as e:
        consumer.close(option=CloseOptions.timeout(timeout=timedelta(seconds=5)))
    assert type(e.value) is KafkaError
    assert str(e.value) == "Failed to close kafka consumer"
    assert e.value.__cause__ is raised
    # Closed all the same.
    with pytest.raises(IllegalStateError):
        consumer.assignment()


def test_close_raises_a_keyboard_interrupt_of_a_commit_callback_as_it_is() -> None:
    # Java's close() throws an InterruptException as it is.
    raised = KeyboardInterrupt()
    events: list[str] = []
    consumer = new_consumer(**{"enable.auto.commit": False})
    consumer.assign(partitions=[TP0])
    queue_a_raising_commit_callback(consumer, raised, events)
    with pytest.raises(KeyboardInterrupt) as e:
        consumer.close(option=CloseOptions.timeout(timeout=timedelta(seconds=5)))
    assert e.value is raised
    with pytest.raises(IllegalStateError):
        consumer.assignment()


def test_close_swallows_a_raising_commit_callback_of_the_auto_commit() -> None:
    # Java's autoCommitOnClose: commitSyncAllConsumed logs a failed commit
    # ("Synchronous auto-commit failed") and close goes on.
    raised = CommitCallbackError("callback boom")
    events: list[str] = []
    consumer = new_consumer(**{"enable.auto.commit": True})
    consumer.assign(partitions=[TP0])
    queue_a_raising_commit_callback(consumer, raised, events)
    consumer.close(option=CloseOptions.timeout(timeout=timedelta(seconds=5)))
    assert events == ["boom", "next"]


def test_commit_callback_must_be_callable() -> None:
    with new_consumer() as consumer, pytest.raises(TypeError) as e:
        consumer.commit_nowait(callback=42)  # type: ignore[arg-type]
    assert str(e.value) == "callback must be callable"


def test_async_commit_callback_runs_on_the_event_loop() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.assign(partitions=[TP0])
        seen: list[int] = []
        await consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_:
                                                              seen.append(threading.get_ident()))
        await consumer.commit(offsets={})
        assert seen == [threading.get_ident()]
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


# Java's commitAsync throws its own failures (throwIfGroupIdNotDefined, the
# offsetsReady wait, a queued callback executeCallbacks runs) from the call: on
# the async consumer they raise from `await commit_nowait()`, and close() does
# not raise them again.
def test_async_commit_nowait_raises_its_own_error_and_close_does_not() -> None:
    # Java's commitAsync waits for its offsets (ConsumerUtils.getResult(
    # commitEvent.offsetsReady(), ...), AsyncKafkaConsumer.java:1136): on the
    # async consumers commit_nowait is a coroutine, and its error comes out of
    # the await, not out of the call that creates it.
    for cls in (AsyncConsumer, AsyncKafkaConsumer, AsyncMockConsumer):
        assert inspect.iscoroutinefunction(cls.commit_nowait), cls

    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs(None))
        await consumer.assign(partitions=[TP0])
        call = consumer.commit_nowait()
        assert inspect.iscoroutine(call)
        with pytest.raises(InvalidGroupIdError) as e:
            await call
        assert str(e.value) == NO_GROUP_ID
        with pytest.raises(InvalidGroupIdError) as e:
            await consumer.commit_nowait(offsets={TP0: OffsetAndMetadata(offset=1)},
                                         callback=lambda *, offsets, exception, **_: None)
        assert str(e.value) == NO_GROUP_ID
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


@pytest.mark.parametrize("coroutine", [False, True], ids=["plain", "coroutine"])
def test_async_a_raising_commit_callback_propagates_from_commit_nowait(coroutine: bool) -> None:
    raised = CommitCallbackError("callback boom")
    events: list[str] = []

    def plain(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
        events.append("boom")
        raise raised

    async def awaited(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
        await asyncio.sleep(0)
        events.append("boom")
        raise raised

    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.assign(partitions=[TP0])
        # An empty commit completes at once: its callback is queued for the
        # next call, whose executeCallbacks runs it.
        await consumer.commit_nowait(offsets={}, callback=awaited if coroutine else plain)
        assert events == []
        with pytest.raises(CommitCallbackError) as e:
            await consumer.commit_nowait(offsets={}, callback=None)
        assert e.value is raised
        assert events == ["boom"]
        # The consumer goes on, and close() does not raise it again.
        await consumer.commit(offsets={})
        assert events == ["boom"]
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


# ---------------------------------------------------------------------------
# A listener invocation is reported once, whatever is raised
# ---------------------------------------------------------------------------
# The C trampoline (_confluentkafka.c's listener_fire) reports every
# invocation the adapter does not defer: whatever the adapter raises,
# KeyboardInterrupt included, is reported as the listener's failure, so the
# core never parks the delivering call, or destroy, on a report that never
# comes. These tests drive the adapter as the trampoline does and check it
# raises; Java's wrapper message is the core's. A Ctrl+C while the sync
# failure is reported cannot land: that report is C code, running no Python.
# A deferred (coroutine) listener reports from Python, on the loop, and is
# covered by the last test.
CALLBACK_ID = 7


class AssignedRecorder(ConsumerRebalanceListener):
    """Records each ``on_partitions_assigned`` call, then raises ``failure``."""

    def __init__(self, failure: BaseException | None = None) -> None:
        self.assigned: list[set[TopicPartition]] = []
        self.failure = failure

    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        self.assigned.append(set(partitions))
        if self.failure is not None:
            raise self.failure

    def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        pass


def test_ctrl_c_before_the_listener_runs_is_raised_to_the_trampoline() -> None:
    # Ctrl+C landing while the partitions are converted, before the listener
    # runs: the adapter raises it, so the trampoline fails the invocation.
    listener = AssignedRecorder()
    with new_consumer() as consumer:
        adapter = consumer._listener_adapter(listener)  # noqa: SLF001
        assert adapter is not None

        def interrupted(**_: Any) -> TopicPartition:
            raise KeyboardInterrupt

        with pytest.MonkeyPatch.context() as m:
            m.setattr(consumer_base, "TopicPartition", interrupted)
            with pytest.raises(KeyboardInterrupt):
                adapter._on_assigned([(TOPIC, 0)], CALLBACK_ID)  # noqa: SLF001
        assert not consumer._in_callback()  # noqa: SLF001
    assert listener.assigned == []


def test_a_listener_failure_is_raised_to_the_trampoline_with_the_window_closed() -> None:
    # The listener's own exception reaches the trampoline as it is; the core
    # wraps it ("User rebalance callback throws an error").
    boom = RuntimeError("boom")
    listener = AssignedRecorder(failure=boom)
    with new_consumer() as consumer:
        adapter = consumer._listener_adapter(listener)  # noqa: SLF001
        assert adapter is not None
        with pytest.raises(RuntimeError) as raised:
            adapter._on_assigned([(TOPIC, 0)], CALLBACK_ID)  # noqa: SLF001
        assert raised.value is boom
        assert not consumer._in_callback()  # noqa: SLF001
    assert listener.assigned == [{TP0}]


def test_ctrl_c_as_the_listener_window_closes_is_raised_to_the_trampoline() -> None:
    # Ctrl+C right after the listener returned, as the callback window closes:
    # the adapter raises it, so the invocation is still reported, as failed.
    listener = AssignedRecorder()
    with new_consumer() as consumer:
        adapter = consumer._listener_adapter(listener)  # noqa: SLF001
        assert adapter is not None
        close_window = consumer._exit_callback  # noqa: SLF001

        def exit_callback(token: Any) -> None:
            close_window(token)
            raise KeyboardInterrupt

        with pytest.MonkeyPatch.context() as m:
            m.setattr(consumer, "_exit_callback", exit_callback)
            with pytest.raises(KeyboardInterrupt):
                adapter._on_assigned([(TOPIC, 0)], CALLBACK_ID)  # noqa: SLF001
        assert not consumer._in_callback()  # noqa: SLF001
    assert listener.assigned == [{TP0}]


def test_ctrl_c_while_a_coroutine_listener_failure_is_reported_still_reports_it() -> None:
    # A coroutine listener is awaited on the loop and reported from there:
    # under asyncio.run a first Ctrl+C cancels the listener, and a second one
    # raises KeyboardInterrupt while that cancellation is reported. The report
    # is made again, and the interrupt raised once it is.
    loop = asyncio.new_event_loop()
    try:
        async def build() -> AsyncKafkaConsumer[bytes, bytes]:
            return AsyncKafkaConsumer(configs=configs())

        consumer = loop.run_until_complete(build())
        reports: list[tuple[int, type[BaseException] | None]] = []

        def report(h: int, callback_id: int, error: BaseException | None) -> None:
            reports.append((callback_id, None if error is None else type(error)))
            if len(reports) == 1:
                raise KeyboardInterrupt

        async def on_partitions_assigned() -> None:
            raise asyncio.CancelledError

        deferred: list[bool] = []

        def deliver() -> None:  # a callback on the loop, as the trampoline's
            deferred.append(consumer._defer_callback(  # noqa: SLF001
                on_partitions_assigned(), CALLBACK_ID))

        with pytest.MonkeyPatch.context() as m:
            m.setattr(_lib, "Consumer__set_callback_result", report)
            loop.call_soon(deliver)
            give_up = loop.call_later(WAIT, loop.stop)  # no interrupt: fail, not hang
            # Only the listener task's interrupt ends the loop before WAIT.
            with pytest.raises(KeyboardInterrupt):
                loop.run_forever()
            give_up.cancel()
        assert deferred == [True]
        assert reports == [(CALLBACK_ID, asyncio.CancelledError)] * 2
        loop.run_until_complete(consumer.close(option=CloseOptions.timeout(timeout=0)))
    finally:
        loop.close()


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

    def __call__(self, *, topic: str, data: memoryview | None, headers: Any = None, **kwargs: Any) -> Any:
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
    consumer.close(option=CloseOptions.timeout(timeout=0))
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
    consumer.close(option=CloseOptions.timeout(timeout=0))


def test_given_deserializers_are_closed_at_close_but_not_configured() -> None:
    RecordingDeserializer.instances.clear()
    key, value = RecordingDeserializer(), RecordingDeserializer()
    consumer = KafkaConsumer(configs=configs(), key_deserializer=key, value_deserializer=value)
    assert key.configured is None and value.configured is None
    consumer.close(option=CloseOptions.timeout(timeout=0))
    consumer.close()
    assert (key.closed, value.closed) == (1, 1)


def test_a_failed_construction_closes_the_deserializers_built() -> None:
    # The core consumer fails: the config route has built nothing yet, a given
    # deserializer is closed.
    RecordingDeserializer.instances.clear()
    with pytest.raises(KafkaError):
        KafkaConsumer(configs=configs("", **{"key.deserializer": RecordingDeserializer,
                                             "value.deserializer": RecordingDeserializer}))
    assert RecordingDeserializer.instances == []
    given = RecordingDeserializer()
    with pytest.raises(KafkaError):
        KafkaConsumer(configs=configs("", **{"key.deserializer": RecordingDeserializer}),
                      value_deserializer=given)
    assert RecordingDeserializer.instances == [given] and given.closed == 1


class FailingConfigureDeserializer(RecordingDeserializer):
    def configure(self, configs: dict[str, Any], is_key: bool) -> None:
        raise ValueError("cannot configure")


def test_a_failing_configure_closes_the_key_deserializer_and_the_core_consumer(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The value deserializer is built last: its configure fails once the core
    # consumer exists, which is destroyed, and the key deserializer is closed.
    destroyed: list[int] = []
    real_destroy = _lib.Consumer_destroy

    def destroy(handle: int) -> None:
        destroyed.append(handle)
        real_destroy(handle)

    monkeypatch.setattr(_lib, "Consumer_destroy", destroy)
    RecordingDeserializer.instances.clear()
    with pytest.raises(KafkaError) as failed:
        KafkaConsumer(configs=configs(**{"key.deserializer": RecordingDeserializer,
                                         "value.deserializer": FailingConfigureDeserializer}))
    # Java's constructor wraps it: KafkaException("Failed to construct kafka consumer", t).
    assert str(failed.value) == "Failed to construct kafka consumer"
    assert isinstance(failed.value.__cause__, ValueError)
    assert str(failed.value.__cause__) == "cannot configure"
    key, value = RecordingDeserializer.instances
    assert (key.closed, value.closed) == (1, 0)
    assert len(destroyed) == 1


@pytest.mark.parametrize("key, value, message", [
    ("key.deserializer", "no.such.Deserializer", "Class no.such.Deserializer could not be found."),
    ("value.deserializer", RecordingDeserializer(), "Expected a Class instance or class name."),
])
def test_a_config_route_class_is_parsed_before_the_core_consumer(
        key: str, value: Any, message: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's ConfigDef parses a CLASS key with the other configs, before the
    # consumer is built; the core is not called.
    def unexpected(native: Any) -> Any:
        raise AssertionError("the core consumer was built")

    monkeypatch.setattr(_lib, "Consumer_KafkaConsumer_new", unexpected)
    with pytest.raises(ConfigError) as e:
        KafkaConsumer(configs=configs(**{key: value}))
    assert str(e.value) == f"Invalid value {value} for configuration {key}: {message}"


class DeserializerForClientId(RecordingDeserializer):
    """Java's ``KafkaConsumerTest.DeserializerForClientId``: records the
    ``client.id`` its ``configure`` sees."""

    client_ids: list[str] = []

    def configure(self, configs: dict[str, Any], is_key: bool) -> None:
        DeserializerForClientId.client_ids.append(configs["client.id"])


def test_configurable_objects_should_see_generated_client_id() -> None:
    # configurableObjectsShouldSeeGeneratedClientId, without its interceptor
    # (the client runs no interceptors): the two config-route deserializers see
    # the client.id the consumer generated.
    DeserializerForClientId.client_ids.clear()
    with KafkaConsumer(configs=configs(**{
            "key.deserializer": DeserializerForClientId,
            "value.deserializer": f"{__name__}.DeserializerForClientId"})) as consumer:
        client_id = _lib.Consumer_client_id(consumer._h)
        assert client_id
        assert client_id.startswith(f"consumer-{GROUP_ID}-")
        assert DeserializerForClientId.client_ids == [client_id, client_id]
    # A client.id the user sets is the one they see.
    DeserializerForClientId.client_ids.clear()
    with KafkaConsumer(configs=configs(**{"client.id": "mine",
                                          "key.deserializer": DeserializerForClientId})):
        assert DeserializerForClientId.client_ids == ["mine"]


def test_the_async_consumer_configures_with_the_generated_client_id() -> None:
    async def main() -> None:
        DeserializerForClientId.client_ids.clear()
        consumer = AsyncKafkaConsumer(configs=configs(
            **{"value.deserializer": DeserializerForClientId}))
        client_id = _lib.Consumer_client_id(consumer._h)
        assert DeserializerForClientId.client_ids == [client_id]
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


def test_no_deserializer_means_bytes() -> None:
    with new_consumer() as consumer:
        from confluent_kafka.common.serialization import bytes_deserializer
        assert type(consumer._key_deserializer) is type(bytes_deserializer())
        assert type(consumer._value_deserializer) is type(bytes_deserializer())


class RaisingCloseDeserializer(RecordingDeserializer):
    def close(self) -> None:
        super().close()
        raise RuntimeError("close failed")


def _closing_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage().startswith("Exception while closing serde")]


def test_a_deserializer_close_that_raises_is_logged_not_raised(
        caplog: pytest.LogCaptureFixture) -> None:
    # Java's Utils.closeQuietly: the exception is logged, the other deserializer
    # is still closed, close() returns normally.
    key, value = RaisingCloseDeserializer(), RecordingDeserializer()
    consumer = KafkaConsumer(configs=configs(), key_deserializer=key, value_deserializer=value)
    with caplog.at_level(logging.WARNING, logger="confluent_kafka"):
        consumer.close(option=CloseOptions.timeout(timeout=0))
    assert (key.closed, value.closed) == (1, 1)
    warnings = _closing_warnings(caplog)
    assert len(warnings) == 1 and warnings[0].exc_info is not None
    assert isinstance(warnings[0].exc_info[1], RuntimeError)


def test_an_async_consumer_deserializer_close_that_raises_is_logged_not_raised(
        caplog: pytest.LogCaptureFixture) -> None:
    async def main() -> None:
        key, value = RecordingDeserializer(), RaisingCloseDeserializer()
        consumer = AsyncKafkaConsumer(configs=configs(), key_deserializer=key,
                                      value_deserializer=value)
        with caplog.at_level(logging.WARNING, logger="confluent_kafka"):
            await consumer.close(option=CloseOptions.timeout(timeout=0))
        assert (key.closed, value.closed) == (1, 1)

    asyncio.run(main())
    warnings = _closing_warnings(caplog)
    assert len(warnings) == 1 and warnings[0].exc_info is not None
    assert isinstance(warnings[0].exc_info[1], RuntimeError)


def test_async_consumer_closes_its_deserializers() -> None:
    async def main() -> None:
        key, value = RecordingDeserializer(), RecordingDeserializer()
        consumer = AsyncKafkaConsumer(configs=configs(), key_deserializer=key,
                                      value_deserializer=value)
        await consumer.close(option=CloseOptions.timeout(timeout=0))
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
        # Awaited (its entry point has a _cb form); no end offset is known
        # yet, so the lag is not either.
        assert await consumer.current_lag(topic_partition=TP0) is None
        with pytest.raises(IllegalArgumentError):
            await consumer.poll(timeout=-1)
        with pytest.raises(IllegalArgumentError) as e:
            await consumer.subscribe(topics=[None])  # type: ignore[list-item]
        assert str(e.value) == ("Topic collection to subscribe to cannot contain null or "
                                "empty topic")
        with pytest.raises(IllegalArgumentError) as e:
            await consumer.assign(partitions=None)  # type: ignore[arg-type]
        assert str(e.value) == "Topic partitions collection to assign to cannot be None"
        with pytest.raises(IllegalArgumentError) as e:
            await consumer.assign(partitions=[None])  # type: ignore[list-item]
        assert str(e.value) == "Topic partitions to assign to cannot have null or empty topic"
        await consumer.close(option=CloseOptions.timeout(timeout=0))
        await consumer.close()
        with pytest.raises(IllegalStateError):
            consumer.assignment()
        with pytest.raises(IllegalStateError) as e:
            await consumer.current_lag(topic_partition=TP0)
        assert str(e.value) == CLOSED

    asyncio.run(main())


def test_async_concurrent_use_on_one_loop_meets_the_guard() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.subscribe(topics=[TOPIC])
        poll = asyncio.ensure_future(consumer.poll(timeout=5))
        await asyncio.sleep(0.5)
        # The core's single-owner guard refuses every read Java's
        # acquireAndEnsureOpen() guards, with Java's message.
        reads: list[Callable[[], object]] = [
            consumer.assignment, consumer.subscription, consumer.paused,
            consumer.group_metadata]
        for read in reads:
            with pytest.raises(ConcurrentModificationError) as e:
                read()
            assert str(e.value) == "KafkaConsumer is not safe for multi-threaded access."
        # current_lag waits on its _cb form, which the guard refuses the same way.
        with pytest.raises(ConcurrentModificationError) as e:
            await consumer.current_lag(topic_partition=TP0)
        assert str(e.value) == "KafkaConsumer is not safe for multi-threaded access."
        # Java's metrics() takes no acquire(): it answers.
        assert isinstance(consumer.metrics(), dict)
        consumer.wakeup()
        with pytest.raises(WakeupError) as woken:
            await poll
        assert str(woken.value) == ""  # Java's WakeupException has no message
        await consumer.close(option=CloseOptions.timeout(timeout=0))

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
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


def test_cancelling_an_async_commit_nowait_leaves_no_wakeup_armed(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Only the calls Java's wakeup() interrupts (poll, commitSync, position,
    # committed, partitionsFor, listTopics) are woken on cancellation. Any other
    # one never consumes a wakeup (commitAsync waits through
    # ConsumerUtils.getResult without the WakeupTrigger,
    # AsyncKafkaConsumer.java:1126-1140): waking it would leave the wakeup armed,
    # and the next poll would raise WakeupError although wakeup() was never
    # called. The call is cancelled deterministically while it awaits a
    # coroutine commit callback an empty commit queued, which it delivers first
    # (Java's commitAsync runs executeCallbacks). The other calls Java cannot
    # wake do not deliver commit callbacks; their flag is checked by
    # test_only_the_calls_java_wakes_are_woken.
    wakeups: list[int] = []
    real_wakeup = _lib.Consumer_wakeup

    def counting_wakeup(h: int) -> None:
        wakeups.append(h)
        real_wakeup(h)

    monkeypatch.setattr(_lib, "Consumer_wakeup", counting_wakeup)

    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.assign(partitions=[TP0])
        started = asyncio.Event()
        release = asyncio.Event()
        ended: list[bool] = []

        async def slow(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
            started.set()
            await release.wait()
            ended.append(True)

        await consumer.commit_nowait(offsets={}, callback=slow)
        running = asyncio.ensure_future(consumer.commit_nowait(offsets={}, callback=None))
        await started.wait()
        running.cancel()
        asyncio.get_running_loop().call_later(0.05, release.set)
        with pytest.raises(asyncio.CancelledError):
            await running
        # The call waited for the callback it delivered, and did not wake.
        assert ended == [True]
        assert wakeups == []
        assert (await consumer.poll(timeout=0)).is_empty()
        await consumer.commit(offsets={})
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


def _counting_wakeups(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    wakeups: list[int] = []
    real_wakeup = _lib.Consumer_wakeup

    def counting_wakeup(h: int) -> None:
        wakeups.append(h)
        real_wakeup(h)

    monkeypatch.setattr(_lib, "Consumer_wakeup", counting_wakeup)
    return wakeups


# call -> (the poll timeout, or None for commit(); the seconds the callback
# runs on after the cancellation).
_CANCELLED_DURING_CALLBACK = {
    "expired_poll": (0.0, 0.05),
    "deadline_passes_in_callback": (0.3, 0.6),
    "poll": (30.0, 0.05),
    "commit": (None, 0.05),
}


@pytest.mark.parametrize("call", list(_CANCELLED_DURING_CALLBACK))
def test_cancelling_a_call_awaiting_a_coroutine_commit_callback_it_delivered(
        call: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # A wakeable call delivers a coroutine commit callback (Java's poll and
    # commitSync run executeCallbacks) and waits for its report; the call is
    # cancelled while the callback runs, and the consumer wakes it. A poll
    # checks for a wakeup after the report (setFetchAction after an empty
    # collectFetch(), AsyncKafkaConsumer.java:1964-2016, which Java runs also
    # when the poll's timeout has expired) and consumes the wake. That holds
    # for a poll(0), and for a poll cancelled before its deadline whose
    # callback ends after it. commit(offsets={}) does not consume it: its
    # futures are already done, and Java's setActiveTask leaves a wakeup
    # pending for a done future (AsyncKafkaConsumer.java:1801-1804,
    # WakeupTrigger.setActiveTask), so the wake stays armed in the core. Either
    # way the next poll returns: a wakeup the consumer sent itself never
    # outlives the call that sent it (the next call the core ends with
    # WakeupError for it is submitted again), and the next poll would otherwise
    # raise WakeupError although wakeup() was never called.
    timeout, release_after = _CANCELLED_DURING_CALLBACK[call]
    wakeups = _counting_wakeups(monkeypatch)

    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.assign(partitions=[TP0])
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
            started.set()
            await release.wait()

        await consumer.commit_nowait(offsets={}, callback=slow)
        running: asyncio.Future[Any]
        if timeout is None:
            running = asyncio.ensure_future(consumer.commit(offsets={}))
        else:
            running = asyncio.ensure_future(consumer.poll(timeout=timeout))
        await started.wait()
        running.cancel()
        asyncio.get_running_loop().call_later(release_after, release.set)
        start = time.monotonic()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert time.monotonic() - start < 5
        assert len(wakeups) == 1
        assert (await consumer.poll(timeout=0)).is_empty()
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


def test_a_wakeup_before_commit_with_empty_offsets_is_raised_by_the_next_poll() -> None:
    # Java's commitSync(emptyMap) registers futures that are already done, and
    # WakeupTrigger.setActiveTask leaves a pending wakeup for a done future
    # (WakeupTriggerTest.testExceptionTriggeredWhenTaskAsynchronouslyCompleted
    # BeforeSet): the commit returns, and the next wakeable call, here poll,
    # raises WakeupException. The poll after it returns.
    consumer = new_consumer()
    try:
        consumer.assign(partitions=[TP0])
        consumer.wakeup()
        consumer.commit(offsets={})
        with pytest.raises(WakeupError) as woken:
            consumer.poll(timeout=0)
        assert str(woken.value) == ""  # Java's WakeupException has no message
        assert consumer.poll(timeout=0).is_empty()
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


def test_async_a_wakeup_before_commit_with_empty_offsets_is_raised_by_the_next_poll() -> None:
    # The AsyncKafkaConsumer peer of the test above.
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        try:
            await consumer.assign(partitions=[TP0])
            consumer.wakeup()
            await consumer.commit(offsets={})
            with pytest.raises(WakeupError):
                await consumer.poll(timeout=0)
            assert (await consumer.poll(timeout=0)).is_empty()
        finally:
            await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


@pytest.mark.parametrize("then", ["poll", "user_wakeup", "handle_committed"])
def test_a_wake_on_ctrl_c_the_call_did_not_consume_never_outlives_it(
        then: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Ctrl+C lands in a commit() (Java's commitSync, wakeable) before the drain
    # that runs its queued completion, and the drain after it does not see the
    # completion yet, as when the core completes just after the drain: the
    # consumer wakes itself, and the core, whose operation has ended, leaves
    # the wakeup armed. The consumer's own wakeup never outlives the call that
    # sent it: the next wakeable call -- a poll, or a ConsumerHandle
    # committed() inside a commit callback -- that the core ends with
    # WakeupError for it is submitted again. A user's wakeup() after it is the
    # user's, and the next poll raises it, as Java's.
    wakeups = _counting_wakeups(monkeypatch)
    consumer = new_consumer(**{"default.api.timeout.ms": 500})
    event = _NotifiedEvent()
    consumer._pending_event = event  # type: ignore[attr-defined]
    state = {"armed": False, "interrupted": False, "held": False, "completed": False}
    real_submit = _lib.Consumer_commit_sync_cb
    real_execute = _lib.Consumer__execute_callbacks

    def submit(h: int, spec: Any, timeout: int, cb: Callable[..., None]) -> None:
        def completed(*payload: Any) -> None:
            state["completed"] = True
            cb(*payload)

        event.notified = False
        state["armed"] = True
        real_submit(h, spec, timeout, completed)

    def execute(h: int) -> int:
        if state["armed"] and event.notified:
            if not state["interrupted"]:
                state["interrupted"] = True
                raise KeyboardInterrupt
            if not state["held"]:
                state["held"] = True
                return 0
            state["armed"] = False
        ran: int = real_execute(h)
        return ran

    try:
        consumer.assign(partitions=[TP0])
        monkeypatch.setattr(_lib, "Consumer_commit_sync_cb", submit)
        monkeypatch.setattr(_lib, "Consumer__execute_callbacks", execute)
        with pytest.raises(KeyboardInterrupt):
            consumer.commit(offsets={})
        monkeypatch.setattr(_lib, "Consumer_commit_sync_cb", real_submit)
        monkeypatch.setattr(_lib, "Consumer__execute_callbacks", real_execute)
        assert state["completed"] and state["held"]
        assert len(wakeups) == 1
        if then == "user_wakeup":
            consumer.wakeup()
            with pytest.raises(WakeupError):
                consumer.poll(timeout=0)
        elif then == "handle_committed":
            raised: list[BaseException] = []

            def committed(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
                try:
                    consumer.committed(partitions=[TP0])
                except Exception as exc:
                    raised.append(exc)

            # The first commit_nowait queues the callback, the second delivers
            # it; the ConsumerHandle committed() it calls waits for the
            # unreachable coordinator until default.api.timeout.ms.
            consumer.commit_nowait(offsets={}, callback=committed)
            consumer.commit_nowait(offsets={}, callback=None)
            assert [type(e) for e in raised] == [KafkaTimeoutError]
        assert consumer.poll(timeout=0).is_empty()
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


def test_a_wakeup_from_a_commit_callback_of_an_expired_poll_is_raised_by_that_poll() -> None:
    # Java's testWakeupAfterEmptyFetch with no time left: a wakeup() sent while
    # poll(0) runs a commit callback is raised by that poll, since Java's
    # pollForFetches calls setFetchAction after the empty collectFetch()
    # whatever the remaining time (AsyncKafkaConsumer.java:1964-2016). The
    # next poll returns.
    consumer = new_consumer()
    try:
        consumer.assign(partitions=[TP0])
        consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_:
                                                        consumer.wakeup())
        with pytest.raises(WakeupError):
            consumer.poll(timeout=0)
        assert consumer.poll(timeout=0).is_empty()
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


def test_cancelling_a_call_whose_completion_is_queued_leaves_no_wakeup_armed(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The core commitSync has ended and its completion is queued, not yet
    # drained, when the awaiting task is cancelled. The consumer drains before
    # it decides to wake: the completion is there, so it does not wake, and the
    # next poll returns. (Waking would leave the wakeup armed, and the next poll
    # would raise WakeupError although wakeup() was never called.)
    wakeups = _counting_wakeups(monkeypatch)

    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        c: Any = consumer
        await consumer.assign(partitions=[TP0])
        real_drain = c._drain
        gate = {"open": True, "held": 0}

        def gated_drain(h: int) -> None:
            if gate["open"]:
                real_drain(h)
            else:
                gate["held"] += 1

        real_submit = _lib.Consumer_commit_sync_cb

        def submit(*args: Any) -> None:
            gate["open"] = False  # after the drain before the submission
            real_submit(*args)

        monkeypatch.setattr(c, "_drain", gated_drain)
        monkeypatch.setattr(_lib, "Consumer_commit_sync_cb", submit)
        running = asyncio.ensure_future(consumer.commit(offsets={}))
        # The notify of the completion woke the call, whose drain held it back.
        deadline = time.monotonic() + 5
        while not gate["held"]:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.01)
        gate["open"] = True
        running.cancel()
        # Have the call drain again even where the cancellation does not, so a
        # wake that comes first is seen, not a hang.
        for _, pending in list(c._async_waiters):
            pending.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(running), 5)
        assert wakeups == []
        assert (await consumer.poll(timeout=0)).is_empty()
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


def test_an_async_result_its_closing_loop_refuses_is_freed(
        hold_completion: Any, freed_errors: list[int], stopped_loop: Any,
        capfd: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    # The awaiting call's task waits on a loop that stopped; the loop is open
    # when the call's result (position()'s error for a partition not assigned)
    # checks it, then closes before call_soon_threadsafe. The result is freed
    # on the dispatcher thread instead of the RuntimeError reaching the C
    # trampoline, which printed it and left the result's handles allocated.
    #
    # On the branch C API a completion is delivered only by a drain of the
    # consumer's callbacks vector, which the stopped loop never runs. Under the
    # one-loop binding the only call another loop may make is close(), once
    # the bound loop is closed (LoopBinding.bind_for_close): it drains the
    # vector (and delivers the result to the closed loop), so the test makes
    # that call; any other call from there is refused first.
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    held = hold_completion("Consumer_position_cb")
    _start_without_draining(monkeypatch, stopped_loop, consumer.position(partition=TP0))
    assert held.submitted.is_set()
    loop = stopped_loop.loop
    loop.close()

    def is_closed() -> bool:
        # The delivery's check (Undelivered.hand_over) saw the loop open;
        # everything else (the binding, the abandoned-use release) sees it closed.
        return sys._getframe(1).f_code.co_name != "hand_over"

    loop.is_closed = is_closed  # type: ignore[method-assign]
    capfd.readouterr()
    held.release.set()
    _await_queued_completion(consumer)
    _refused_from_another_loop(lambda: consumer.assign(partitions=[TP0]))
    assert not held.delivered.is_set()
    _close_bounded(consumer, stopped_loop)  # the call's task still pending
    assert held.delivered.is_set()
    del loop.is_closed
    ((_, error),) = held.payloads
    assert error
    assert held.raised == []
    assert capfd.readouterr().err == ""
    assert freed_errors == [error]


def test_an_async_result_left_for_a_closed_loop_is_freed_by_the_next_call(
        hold_completion: Any, freed_errors: list[int], stopped_loop: Any,
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The result is left for its loop while the loop is stopped, and the loop
    # then closes, so it can never be delivered there: close() from another
    # loop, the one call the binding then allows (LoopBinding.bind_for_close),
    # frees it; any other call from there is refused first. (On the branch C
    # API that close is also the drain that delivers the result, the stopped
    # loop draining nothing.)
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    held = hold_completion("Consumer_position_cb")
    _start_without_draining(monkeypatch, stopped_loop, consumer.position(partition=TP0))
    held.release.set()
    stopped_loop.loop.close()
    _await_queued_completion(consumer)
    assert freed_errors == []
    _refused_from_another_loop(lambda: consumer.assign(partitions=[TP0]))
    assert freed_errors == []
    _close_bounded(consumer, stopped_loop)  # the call's task still pending
    assert held.delivered.is_set()
    ((_, error),) = held.payloads
    assert error
    assert freed_errors == [error]


@pytest.mark.parametrize("same_thread", [True, False], ids=["same thread", "another thread"])
def test_close_does_not_wait_for_a_call_left_on_a_closed_loop(
        same_thread: bool, hold_completion: Any, freed_errors: list[int],
        stopped_loop: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # An awaiting call holds a use of the consumer until it ends. Its task is
    # left pending on a loop that stops, with its result queued there, and the
    # loop closes: the call can never resume. close() from a fresh loop, on the
    # call's thread or another one, returns and frees the stranded result; it
    # used to wait for that use for good (from another thread, to raise
    # ConcurrentModificationError).
    # On the branch C API the result stays queued in the consumer's callbacks
    # vector (the stopped loop drains nothing) until close() drains it.
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    held = hold_completion("Consumer_position_cb")

    def leave_a_call() -> None:
        _start_without_draining(monkeypatch, stopped_loop, consumer.position(partition=TP0))
        held.release.set()
        stopped_loop.loop.close()
        _await_queued_completion(consumer)

    if same_thread:
        leave_a_call()
    else:
        caller = threading.Thread(target=leave_a_call)
        caller.start()
        caller.join(WAIT)
    assert held.submitted.is_set() and stopped_loop.loop.is_closed()
    assert freed_errors == []
    _close_bounded(consumer, stopped_loop)
    assert held.delivered.is_set()
    ((_, error),) = held.payloads
    assert freed_errors == [error]


def test_a_call_left_on_a_closed_loop_does_not_hold_the_consumer_for_other_threads(
        hold_completion: Any, stopped_loop: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Gap 39: an awaiting call left on another thread's loop, which closes,
    # holds its use of the consumer for that thread. A call that does not wait
    # (assignment(), subscription(), paused(), metrics(), …)
    # from this thread used to raise ConcurrentModificationError until a
    # waiting call or close() ended that use; the guard every call passes
    # (_Use) now ends it first, as _run_async and close() do.
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    held = hold_completion("Consumer_position_cb")

    def leave_a_call() -> None:
        _start_without_draining(monkeypatch, stopped_loop, consumer.position(partition=TP0))
        held.release.set()
        stopped_loop.loop.close()
        _await_queued_completion(consumer)

    caller = threading.Thread(target=leave_a_call)
    caller.start()
    caller.join(WAIT)
    assert held.submitted.is_set() and stopped_loop.loop.is_closed()
    assert consumer.assignment() == set()
    assert consumer.subscription() == set()
    assert consumer.paused() == set()
    _close_bounded(consumer, stopped_loop)


def test_a_sync_close_does_not_wait_for_a_call_left_on_a_closed_loop(
        hold_completion: Any, stopped_loop: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Gap 39's guard also covers close() entered from a thread outside any
    # event loop: _begin_close ends the abandoned use before it checks for
    # another thread inside the consumer.
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    held = hold_completion("Consumer_position_cb")

    def leave_a_call() -> None:
        _start_without_draining(monkeypatch, stopped_loop, consumer.position(partition=TP0))
        held.release.set()
        stopped_loop.loop.close()
        _await_queued_completion(consumer)

    caller = threading.Thread(target=leave_a_call)
    caller.start()
    caller.join(WAIT)
    assert consumer._begin_close()  # noqa: SLF001
    consumer._abort_close()  # noqa: SLF001
    _close_bounded(consumer, stopped_loop)


def test_a_close_already_waiting_ends_a_call_its_loop_leaves_meanwhile(
        hold_completion: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Gap 40: close() waits in _finish_close for the uses in flight. An
    # awaiting call whose loop closes only after that wait began can never end
    # its use; the wait now re-checks for such calls, so close() returns.
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
    held = hold_completion("Consumer_position_cb")
    loop = asyncio.new_event_loop()
    task = loop.create_task(consumer.position(partition=TP0))
    loop.run_until_complete(asyncio.sleep(0))
    assert held.submitted.wait(WAIT)
    # close() from another thread: the guard refuses it while the call runs
    # (Java's acquire()), so the closing state is entered as the call's own
    # thread would, and the wait for the uses starts with the call in flight.
    assert consumer._begin_close()  # noqa: SLF001
    finished = threading.Event()

    def finish() -> None:
        consumer._finish_close()  # noqa: SLF001
        finished.set()

    closer = threading.Thread(target=finish)
    closer.start()
    try:
        assert not finished.wait(0.3), "close() did not wait for the call in flight"
        held.release.set()
        loop.close()
        assert finished.wait(WAIT), "close() waited for a call whose event loop had closed"
    finally:
        held.release.set()
        if not loop.is_closed():
            loop.close()
        closer.join(WAIT)
        task._log_destroy_pending = False  # noqa: SLF001
        task.get_coro().close()
    assert consumer._is_closed()  # noqa: SLF001


class _StrandedCallback:
    """A consumer whose call delivered a coroutine commit callback, awaited in
    a task on a loop that then closed with the task pending: the callback
    never ends and never reports. ``discard()`` closes what is left."""

    def __init__(self) -> None:
        self.consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        self.loop = asyncio.new_event_loop()
        started = asyncio.Event()
        never = asyncio.Event()
        consumer = self.consumer

        async def stuck(*, offsets: Any, exception: Any, **kwargs: Any) -> None:
            started.set()
            await never.wait()

        async def commit_twice() -> None:
            await consumer.assign(partitions=[TP0])
            await consumer.commit_nowait(offsets={}, callback=stuck)
            # The second commit delivers the first one's callback.
            await consumer.commit_nowait(offsets={}, callback=None)

        self._tasks = {self.loop.create_task(commit_twice())}
        deadline = time.monotonic() + WAIT
        while not started.is_set():
            assert time.monotonic() < deadline, "the commit callback was not delivered"
            self.loop.run_until_complete(asyncio.sleep(0.01))
        self._tasks |= set(consumer._listener_tasks)  # noqa: SLF001
        self.loop.close()

    def close_on_another_thread(self, option: CloseOptions) -> BaseException | None:
        """``close(option)`` from a fresh loop, bounded by WAIT; what it raised."""
        outcome: list[BaseException | None] = []

        def close() -> None:
            try:
                asyncio.run(self.consumer.close(option=option))
                outcome.append(None)
            except BaseException as exc:  # noqa: BLE001 - returned
                outcome.append(exc)

        closer = threading.Thread(target=close, daemon=True)
        closer.start()
        closer.join(WAIT)
        assert outcome, "close() waited for a callback whose loop had closed"
        return outcome[0]

    def discard(self) -> None:
        if not self.consumer._is_closed():  # noqa: SLF001
            asyncio.run(self.consumer.close(option=CloseOptions.timeout(timeout=1.0)))
        for pending in self._tasks:
            pending._log_destroy_pending = False  # noqa: SLF001
            pending.get_coro().close()


def test_a_coroutine_commit_callback_left_on_a_closed_loop_does_not_block_close(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Gap 41: a call delivers a coroutine commit callback and awaits it in a
    # task on its loop; the core waits for that callback's report before it
    # completes the call, and refuses close() meanwhile. The loop closes with
    # the task pending, so the callback never ends and never reports. The
    # consumer now reports it, failed with CancelledError (what asyncio.run's
    # shutdown would deliver to the task), once it finds its loop closed, so
    # close() from a fresh loop returns.
    stranded = _StrandedCallback()
    reported: list[tuple[int, BaseException | None]] = []
    real_report = _lib.Consumer__set_callback_result

    def report(handle: int, callback_id: int, error: BaseException | None) -> None:
        reported.append((callback_id, error))
        real_report(handle, callback_id, error)

    try:
        monkeypatch.setattr(_lib, "Consumer__set_callback_result", report)
        assert stranded.close_on_another_thread(CloseOptions.timeout(timeout=1.0)) is None
        ((_, error),) = reported
        assert isinstance(error, asyncio.CancelledError)
        assert stranded.consumer._is_closed()  # noqa: SLF001
    finally:
        stranded.discard()


class _RecordingEvent(threading.Event):
    """The consumer's pending-callback event, recording the timeouts it is
    waited with."""

    def __init__(self) -> None:
        super().__init__()
        self.waits: list[float | None] = []

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout)
        return super().wait(timeout)


@pytest.mark.parametrize("timeout, waited", [(0, []), (0.3, [0.3]), (None, [5.0])],
                         ids=["zero", "short", "default"])
def test_close_waits_for_a_stranded_callback_s_call_within_its_own_timeout(
        timeout: float | None, waited: list[float]) -> None:
    # Critic 79 issue 4: once close() reported a stranded coroutine callback,
    # it waits for the core to end the call that delivered it at most
    # ABANDONED_CALL_END_S, capped by the close's own timeout: not at all for
    # close(timeout=0), which may then be refused as a call in flight is.
    stranded = _StrandedCallback()
    event = _RecordingEvent()
    stranded.consumer._pending_event = event  # noqa: SLF001
    try:
        option = CloseOptions.timeout(timeout=timeout)
        started = time.monotonic()
        outcome = stranded.close_on_another_thread(option)
        assert outcome is None or isinstance(outcome, ConcurrentModificationError)
        assert event.waits == waited
        if timeout == 0:
            assert time.monotonic() - started < 1.0
    finally:
        stranded.discard()


def _start_without_draining(monkeypatch: pytest.MonkeyPatch, stopped_loop: Any,
                            call: Any) -> None:
    """``stopped_loop.start(call)`` with the consumer's drain gated off, so the
    stopped loop drains nothing, as these tests assume: the call's completion
    stays in the consumer's callbacks vector whether the core queues it before
    the loop stops or after. Without the gate, a core answering within the
    loop's one iteration had the loop drain it there, running the held
    completion on the loop thread, which then waited for a release only that
    thread sets."""
    execute = _lib.Consumer__execute_callbacks
    monkeypatch.setattr(_lib, "Consumer__execute_callbacks", lambda h: 0)
    try:
        stopped_loop.start(call)
    finally:
        monkeypatch.setattr(_lib, "Consumer__execute_callbacks", execute)


def _await_queued_completion(consumer: AsyncKafkaConsumer[Any, Any]) -> None:
    """Wait until the core has queued the held call's completion (its notify
    hook fired). The core releases the call's single-owner guard before it
    queues the completion, so the next call is not refused."""
    assert consumer._pending_event.wait(WAIT)  # noqa: SLF001


BOUND_ELSEWHERE = "AsyncKafkaConsumer is bound to a different event loop"


def _refused_from_another_loop(call: Any) -> None:
    """The one-loop binding (``_async.LoopBinding``): a waiting call other than
    ``close()`` from a fresh loop, after the consumer's own loop closed, is
    refused with ``RuntimeError`` and no effect."""
    with pytest.raises(RuntimeError) as e:
        asyncio.run(call())
    assert str(e.value) == BOUND_ELSEWHERE


def _close_bounded(consumer: AsyncKafkaConsumer[Any, Any], stopped_loop: Any) -> None:
    """``close()`` from a fresh loop on this thread, which must not wait for the
    call left on ``stopped_loop``. Should it wait, a timer ends that call's use
    after WAIT, so the test fails instead of hanging the suite. The bound loop
    being closed, the fresh loop's close() is allowed (LoopBinding.bind_for_close)."""
    fired = threading.Event()

    def end_the_call() -> None:
        fired.set()
        stopped_loop.abandon()

    watchdog = threading.Timer(WAIT, end_the_call)
    watchdog.start()
    try:
        asyncio.run(consumer.close(option=CloseOptions.timeout(timeout=0)))
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


# ---------------------------------------------------------------------------
# The sync waiting model: `_cb` entry points, Ctrl+C
# ---------------------------------------------------------------------------
# The synchronous KafkaConsumer never calls a blocking C entry point: every
# waiting method submits the `_cb` form and waits in Python, in 100 ms slices,
# running the consumer's queued callbacks on the calling thread (CLAUDE.md,
# Python Binding Conventions, Implementation over the FFI).

_BLOCKING_ENTRY_POINTS = (
    "Consumer_poll", "Consumer_subscribe", "Consumer_subscribe_pattern", "Consumer_unsubscribe",
    "Consumer_assign", "Consumer_pause", "Consumer_resume", "Consumer_seek",
    "Consumer_seek_with_metadata", "Consumer_seek_to_beginning", "Consumer_seek_to_end",
    "Consumer_commit_sync", "Consumer_commit_async", "Consumer_position", "Consumer_committed",
    "Consumer_beginning_offsets", "Consumer_end_offsets", "Consumer_offsets_for_times",
    "Consumer_partitions_for", "Consumer_list_topics", "Consumer_close",
)


def test_sync_consumer_never_calls_a_blocking_entry_point(
        monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(name: str) -> Any:
        def entry(*args: Any) -> Any:
            raise AssertionError(f"the sync KafkaConsumer called the blocking {name}")
        return entry

    for name in _BLOCKING_ENTRY_POINTS:
        monkeypatch.setattr(_lib, name, forbidden(name))
    committed: list[Any] = []
    consumer = new_consumer(**{"default.api.timeout.ms": 200})
    try:
        consumer.subscribe(topics=[TOPIC], callback=_NoOpListener())
        assert consumer.subscription() == {TOPIC}
        consumer.unsubscribe()
        consumer.subscribe(pattern=SubscriptionPattern(pattern="te.*"))
        consumer.unsubscribe()
        consumer.assign(partitions=[TP0])
        consumer.pause(partitions=[TP0])
        assert consumer.paused() == {TP0}
        consumer.resume(partitions=[TP0])
        consumer.seek(partition=TP0, offset=1)
        consumer.seek(partition=TP0, offset_and_metadata=OffsetAndMetadata(offset=2))
        consumer.seek_to_beginning(partitions=[TP0])
        consumer.seek_to_end(partitions=[TP0])
        assert consumer.poll(timeout=0).is_empty()
        consumer.commit_nowait(offsets={}, callback=lambda *, offsets, exception, **_:
                                                        committed.append((offsets, exception)))
        consumer.commit(offsets={})
        # The commit callback ran inside the later waiting call, on this thread.
        assert committed == [(None, None)]
        with pytest.raises(IllegalStateError):
            consumer.position(partition=TopicPartition(topic=TOPIC, partition=1))
        for call in (lambda: consumer.committed(partitions=[TP0]),
                     lambda: consumer.beginning_offsets(partitions=[TP0]),
                     lambda: consumer.end_offsets(partitions=[TP0]),
                     lambda: consumer.offsets_for_times(timestamps_to_search={TP0: 0}),
                     lambda: consumer.partitions_for(topic=TOPIC),
                     lambda: consumer.list_topics()):
            with pytest.raises(KafkaTimeoutError):
                call()
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


def test_only_the_calls_java_wakes_are_woken() -> None:
    # The operation specs shared by KafkaConsumer and AsyncKafkaConsumer: Ctrl+C
    # or a cancellation wakes the consumer only during an operation whose future
    # Java's AsyncKafkaConsumer registers with the WakeupTrigger (poll,
    # commitSync, position, committed, partitionsFor, listTopics).
    with new_consumer() as consumer:
        c: Any = consumer
        specs = {
            "poll": c._poll_spec(0),
            "commit": c._commit_spec(None),
            "position": c._position_spec(TP0),
            "committed": c._committed_spec([TP0]),
            "partitions_for": c._partitions_for_spec(TOPIC),
            "list_topics": c._list_topics_spec(),
            "commit_nowait": c._commit_nowait_spec(None, None),
            "subscribe_topics": c._subscribe_topics_spec([TOPIC], None),
            "subscribe_pattern": c._subscribe_pattern_spec("t.*", None),
            "seek": c._seek_spec(TP0, 0, None),
            "seek_with_metadata": c._seek_spec(TP0, None, OffsetAndMetadata(offset=0)),
            "assign_and_partition_ops": c._void_spec(_lib.Consumer_assign_cb, []),
            "unsubscribe": c._void_spec(_lib.Consumer_unsubscribe_cb),
            "offsets_for_times": c._offsets_for_times_spec({}),
            "beginning_offsets": c._long_offsets_spec(_lib.Consumer_beginning_offsets_cb, []),
            "close": c._close_spec(0, 0),
        }
        woken = {name for name, (_, _, _, wakeable) in specs.items() if wakeable}
        assert woken == {"poll", "commit", "position", "committed", "partitions_for",
                         "list_topics"}


class _InterruptedOnce(threading.Event):
    """The consumer's pending-callback event: once ``armed``, its next ``wait``
    raises ``KeyboardInterrupt``, where Ctrl+C lands in ``_run_sync``'s wait."""

    armed = False

    def wait(self, timeout: float | None = None) -> bool:
        if self.armed:
            self.armed = False
            raise KeyboardInterrupt
        return super().wait(timeout)


@pytest.mark.parametrize("call", ["poll", "commit_nowait", "seek", "pause"])
def test_ctrl_c_wakes_only_a_call_java_can_wake(
        call: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Ctrl+C during a waiting call lets the call end and re-raises
    # KeyboardInterrupt. The consumer wakes itself first only for a call Java's
    # wakeup() interrupts, here poll, which then ends at once. A call that never
    # consumes a wakeup, such as commitAsync (ConsumerUtils.getResult without
    # the WakeupTrigger, AsyncKafkaConsumer.java:1126-1140), is not woken: the
    # wakeup would stay armed and the next poll would raise WakeupError.
    wakeups: list[int] = []
    real_wakeup = _lib.Consumer_wakeup

    def counting_wakeup(h: int) -> None:
        wakeups.append(h)
        real_wakeup(h)

    monkeypatch.setattr(_lib, "Consumer_wakeup", counting_wakeup)
    consumer = new_consumer()
    event = _InterruptedOnce()
    consumer._pending_event = event  # type: ignore[attr-defined]
    try:
        consumer.assign(partitions=[TP0])
        event.armed = True
        start = time.monotonic()
        with pytest.raises(KeyboardInterrupt):
            if call == "poll":
                consumer.poll(timeout=30)
            elif call == "commit_nowait":
                consumer.commit_nowait(offsets={}, callback=None)
            elif call == "seek":
                consumer.seek(partition=TP0, offset=0)
            else:
                consumer.pause(partitions=[TP0])
        assert time.monotonic() - start < 5
        assert not event.armed
        assert len(wakeups) == (1 if call == "poll" else 0)
        assert consumer.poll(timeout=0).is_empty()
        consumer.commit(offsets={})
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


class _NativeRecord:
    """A record of the native batch ``Consumer_poll_cb`` hands over; its value
    is ``b"<partition>:<offset>"`` unless given, ``None`` bytes for a null key
    or value."""

    def __init__(self, tp: TopicPartition, offset: int, *, key: bytes | None = None,
                 value: bytes | None | str = "") -> None:
        if value == "":
            value = f"{tp.partition()}:{offset}".encode()
        assert not isinstance(value, str)
        self.topic = tp.topic()
        self.partition = tp.partition()
        self.offset = offset
        self.timestamp = 0
        self.timestamp_type = 0
        # A null key or value: no bytes, Java's ConsumerRecord.NULL_SIZE.
        self.serialized_key_size = -1 if key is None else len(key)
        self.serialized_value_size = -1 if value is None else len(value)
        self.key = None if key is None else memoryview(key)
        self.value = None if value is None else memoryview(value)
        self.headers = Headers()  # what the extension builds: writable, empty
        self.leader_epoch = None


class _NativeBatch:
    """The native ``ConsumerRecords`` batch: the records, then the next offsets."""

    def __init__(self, records: list[_NativeRecord]) -> None:
        self._records = records

    def count(self) -> int:
        return len(self._records)

    def get(self, i: int) -> _NativeRecord:
        return self._records[i]

    def next_offsets(self) -> dict[tuple[str, int], tuple[int, str, int | None]]:
        return {(r.topic, r.partition): (r.offset + 1, "", None) for r in self._records}


# A null key or value is not deserialized (Serialization; Java's
# CompletedFetch.parseRecord: `keyBytes == null ? null : deserialize(...)`).
def _refuses_null(*, topic: str, data: memoryview | None, headers: Any = None, **kwargs: Any) -> str:
    """A deserializer that fails on Java's null, as one assuming non-null
    bytes would."""
    if data is None:
        raise AssertionError("the deserializer was called with a null")
    return bytes(data).decode()


def test_a_null_key_is_not_deserialized() -> None:
    from confluent_kafka.consumer._poll import deserialize_batch
    result = deserialize_batch(_NativeBatch([_NativeRecord(TP0, 0, key=None, value=b"v")]),
                               key_deserializer=_caller(_refuses_null),
                               value_deserializer=_caller(_refuses_null))
    assert result.error is None and result.rewind == []
    (record,) = list(result.records)
    assert record.key() is None
    assert record.value() == "v"
    assert record.serialized_key_size() == -1


def test_a_tombstone_is_delivered_with_a_null_value() -> None:
    from confluent_kafka.consumer._poll import deserialize_batch
    result = deserialize_batch(
        _NativeBatch([_NativeRecord(TP0, 0, key=b"k", value=None),
                      _NativeRecord(TP0, 1, key=None, value=None)]),
        key_deserializer=_caller(_refuses_null), value_deserializer=_caller(_refuses_null))
    assert result.error is None
    tombstone, empty = list(result.records)
    assert (tombstone.key(), tombstone.value()) == ("k", None)
    assert tombstone.serialized_value_size() == -1
    assert (empty.key(), empty.value()) == (None, None)
    assert result.records.next_offsets() == {TP0: OffsetAndMetadata(offset=2)}


def test_a_failing_deserializer_still_fails_on_non_null_bytes() -> None:
    from confluent_kafka.common.errors import RecordDeserializationError
    from confluent_kafka.consumer._poll import deserialize_batch

    def failing(*, topic: str, data: memoryview | None, headers: Any = None, **kwargs: Any) -> str:
        assert data is not None
        raise ValueError("bad value")

    result = deserialize_batch(
        _NativeBatch([_NativeRecord(TP0, 0, key=None, value=None),
                      _NativeRecord(TP0, 1, key=None, value=b"x")]),
        key_deserializer=_caller(_refuses_null), value_deserializer=_caller(failing))
    (record,) = list(result.records)
    assert record.offset() == 0 and record.value() is None
    assert isinstance(result.error, RecordDeserializationError)
    assert result.error.offset() == 1
    assert result.error.key_buffer() is None
    assert result.rewind == [(TP0, 1)]


TP1 = TopicPartition(topic=TOPIC, partition=1)


class _FetchingCore:
    """Stands for the core's fetch path: ``Consumer_poll_cb`` returns the
    records of ``LOG`` (in its order, interleaving the partitions) from each
    partition's position on, and moves the positions past them, as
    ``FetchCollector`` sets the position while it collects; ``Consumer_seek_cb``
    moves a position back (and seeks the core too). The poll's completion is
    queued for a drain, which the notify of an async consumer's waiting call
    has run, as the core's does. The first drain after a poll (``interrupt``)
    or a seek (``interrupt_seek``) is submitted raises ``KeyboardInterrupt``, as
    Ctrl+C landing while the call waits; the completion is held back, and not
    notified, until ``release``, for a cancellation to land first."""

    LOG = [(TP0, 3), (TP1, 7), (TP0, 4)]

    def __init__(self, monkeypatch: pytest.MonkeyPatch, consumer: Any) -> None:
        self.positions = {TP0: 3, TP1: 7}
        self.seeks: list[tuple[str, int, int]] = []
        self.interrupt = False
        self.interrupt_seek = False
        self.release = True
        self._pending: Callable[..., None] | None = None
        self._interrupt_drain = False
        real_execute = _lib.Consumer__execute_callbacks
        real_seek = _lib.Consumer_seek_cb

        def poll_cb(h: int, timeout_ms: int, cb: Callable[..., None]) -> None:
            self._pending = cb
            self._interrupt_drain = self.interrupt
            if self.release:
                for loop, pending in list(consumer._async_waiters):
                    loop.call_soon_threadsafe(pending.set)

        def seek_cb(h: int, topic: str, partition: int, offset: int,
                    cb: Callable[..., None]) -> None:
            self.seeks.append((topic, partition, offset))
            self.positions[TopicPartition(topic=topic, partition=partition)] = offset
            self._interrupt_drain, self.interrupt_seek = self.interrupt_seek, False
            real_seek(h, topic, partition, offset, cb)

        def execute(h: int) -> int:
            if self._interrupt_drain:
                self._interrupt_drain = False
                raise KeyboardInterrupt
            cb = self._pending
            if cb is not None and self.release:
                self._pending = None
                cb(self._fetch(), 0)
                return 1
            return int(real_execute(h))

        monkeypatch.setattr(_lib, "Consumer_poll_cb", poll_cb)
        monkeypatch.setattr(_lib, "Consumer_seek_cb", seek_cb)
        monkeypatch.setattr(_lib, "Consumer__execute_callbacks", execute)

    def _fetch(self) -> _NativeBatch:
        records = [_NativeRecord(tp, offset) for tp, offset in self.LOG
                   if offset >= self.positions[tp]]
        for record in records:
            tp = TopicPartition(topic=record.topic, partition=record.partition)
            self.positions[tp] = record.offset + 1
        return _NativeBatch(records)


def _fetched(records: Any) -> list[tuple[int, int]]:
    return sorted((r.partition(), r.offset()) for r in records)


def test_ctrl_c_during_a_poll_returning_records_seeks_them_back(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Ctrl+C while a poll waits lets the core call end; when that call returns
    # records (records already buffered come back before any wakeup check), the
    # poll does not return them and raises KeyboardInterrupt. The core has moved
    # the positions past them, so a commit after Ctrl+C would commit records the
    # user never received: each partition is first sought back to its first
    # fetched record, and the next poll returns them. Java's poll never loses
    # records the positions moved past (AsyncKafkaConsumer.java:940-960).
    consumer = new_consumer()
    try:
        consumer.assign(partitions=[TP0, TP1])
        core = _FetchingCore(monkeypatch, consumer)
        core.interrupt = True
        with pytest.raises(KeyboardInterrupt):
            consumer.poll(timeout=30)
        assert core.seeks == [(TOPIC, 0, 3), (TOPIC, 1, 7)]
        assert core.positions == {TP0: 3, TP1: 7}
        core.interrupt = False
        assert _fetched(consumer.poll(timeout=30)) == [(0, 3), (0, 4), (1, 7)]
        assert core.positions == {TP0: 5, TP1: 8}
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


def test_ctrl_c_during_the_seek_back_of_a_failed_deserialization_seeks_every_record_back(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The value deserializer fails on TP0's offset 4: the poll would return
    # TP0's 3 and TP1's 7 and seek TP0 back to 4. Ctrl+C during that seek lets
    # it end; the poll then raises KeyboardInterrupt instead of returning the
    # records, so each partition goes back to its first record not returned.
    def value_deserializer(*, topic: str, data: Any, headers: Any = None, **kwargs: Any) -> bytes:
        if bytes(data) == b"0:4":
            raise ValueError("bad value")
        return bytes(data)

    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(
        configs=configs(), value_deserializer=value_deserializer)
    try:
        consumer.assign(partitions=[TP0, TP1])
        core = _FetchingCore(monkeypatch, consumer)
        core.interrupt_seek = True
        with pytest.raises(KeyboardInterrupt):
            consumer.poll(timeout=30)
        assert core.seeks == [(TOPIC, 0, 4), (TOPIC, 0, 3), (TOPIC, 1, 7)]
        assert core.positions == {TP0: 3, TP1: 7}
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


def test_cancelling_an_async_poll_returning_records_seeks_them_back(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The async peer of the Ctrl+C case: the awaiting task is cancelled while
    # the poll waits, and the core call then returns records. The seeks back
    # are awaited before the CancelledError propagates, and the next poll
    # returns the records.
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.assign(partitions=[TP0, TP1])
        core = _FetchingCore(monkeypatch, consumer)
        core.release = False
        poll = asyncio.ensure_future(consumer.poll(timeout=30))
        await asyncio.sleep(0.2)
        core.release = True
        poll.cancel()
        with pytest.raises(asyncio.CancelledError):
            await poll
        assert core.seeks == [(TOPIC, 0, 3), (TOPIC, 1, 7)]
        assert core.positions == {TP0: 3, TP1: 7}
        assert _fetched(await consumer.poll(timeout=30)) == [(0, 3), (0, 4), (1, 7)]
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


def _interrupting_value_deserializer(raised: BaseException) -> Callable[..., bytes]:
    """A value deserializer raising ``raised`` on the second record: Ctrl+C (or
    a cancellation) landing while the poll deserializes its batch."""
    calls = {"n": 0}

    def value_deserializer(*, topic: str, data: Any, headers: Any = None, **kwargs: Any) -> bytes:
        calls["n"] += 1
        if calls["n"] == 2:
            raise raised
        return bytes(data)

    return value_deserializer


def test_ctrl_c_while_a_poll_deserializes_its_records_seeks_them_back(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Ctrl+C once the poll's completion has arrived, while its deserializers run
    # on the caller's thread: the poll does not return the records either, and
    # the core has moved the positions past the whole batch. Each partition is
    # sought back to its first fetched record before KeyboardInterrupt
    # propagates, and the next poll returns them, as when Ctrl+C lands in the
    # wait (AsyncKafkaConsumer.java:940-960 never loses records the positions
    # moved past).
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(
        configs=configs(), value_deserializer=_interrupting_value_deserializer(
            KeyboardInterrupt()))
    try:
        consumer.assign(partitions=[TP0, TP1])
        core = _FetchingCore(monkeypatch, consumer)
        with pytest.raises(KeyboardInterrupt):
            consumer.poll(timeout=30)
        assert core.seeks == [(TOPIC, 0, 3), (TOPIC, 1, 7)]
        assert core.positions == {TP0: 3, TP1: 7}
        assert _fetched(consumer.poll(timeout=30)) == [(0, 3), (0, 4), (1, 7)]
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


@pytest.mark.parametrize("raised", [KeyboardInterrupt, asyncio.CancelledError])
def test_an_async_poll_interrupted_while_it_deserializes_seeks_its_records_back(
        raised: type[BaseException], monkeypatch: pytest.MonkeyPatch) -> None:
    # The async peer: a KeyboardInterrupt or CancelledError raised while the
    # poll deserializes its batch on the loop awaits the seeks back before it
    # propagates, and the next poll returns the records.
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(
            configs=configs(), value_deserializer=_interrupting_value_deserializer(raised()))
        await consumer.assign(partitions=[TP0, TP1])
        core = _FetchingCore(monkeypatch, consumer)
        with pytest.raises(raised):
            await consumer.poll(timeout=30)
        assert core.seeks == [(TOPIC, 0, 3), (TOPIC, 1, 7)]
        assert core.positions == {TP0: 3, TP1: 7}
        assert _fetched(await consumer.poll(timeout=30)) == [(0, 3), (0, 4), (1, 7)]
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


@pytest.mark.parametrize("raised", [ValueError("bad"), KeyboardInterrupt()],
                         ids=["value-error", "keyboard-interrupt"])
def test_a_cancelled_async_poll_keeps_a_coroutine_commit_callback_error_for_the_next_call(
        raised: BaseException, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture) -> None:
    # A coroutine commit callback a blocking ConsumerHandle commit delivered
    # during the poll raises after its invocation was reported (stood for by
    # setting the error the consumer then stores), and the poll is cancelled.
    # The cancelled poll cannot raise it: it keeps it for the next awaited call,
    # as a poll that returns keeps it. The seeks back do not take it: none is
    # logged as failed, and every one runs, a KeyboardInterrupt too.
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())
        await consumer.assign(partitions=[TP0, TP1])
        core = _FetchingCore(monkeypatch, consumer)
        core.release = False
        poll = asyncio.ensure_future(consumer.poll(timeout=30))
        await asyncio.sleep(0.2)
        consumer._callback_error = raised  # type: ignore[attr-defined]
        core.release = True
        poll.cancel()
        with pytest.raises(asyncio.CancelledError):
            await poll
        assert core.seeks == [(TOPIC, 0, 3), (TOPIC, 1, 7)]
        assert core.positions == {TP0: 3, TP1: 7}
        assert "Could not seek" not in caplog.text
        if isinstance(raised, KeyboardInterrupt):
            with pytest.raises(KeyboardInterrupt) as interrupted:
                await consumer.poll(timeout=30)
            assert interrupted.value is raised
        else:
            # Java's maybeWrapAsKafkaException at poll.
            with pytest.raises(KafkaError) as wrapped:
                await consumer.poll(timeout=30)
            assert wrapped.value.__cause__ is raised
        assert _fetched(await consumer.poll(timeout=30)) == [(0, 3), (0, 4), (1, 7)]
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


class _NotifiedEvent(threading.Event):
    """The consumer's pending-callback event, recording a ``set`` (the notify
    hook's, or a completion's) since ``notified`` was last cleared."""

    notified = False

    def set(self) -> None:
        self.notified = True
        super().set()


@pytest.mark.parametrize("lands", ["after_the_completion", "before_the_completion"])
def test_ctrl_c_once_the_completion_is_queued_does_not_wake(
        lands: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Ctrl+C lands in a drain of a commit() (Java's commitSync, wakeable) whose
    # core operation has ended: right after the completion ran, or before the
    # drain ran the queued completion. The consumer finishes the drain before it
    # decides to wake, finds the completion and does not wake: a wakeup would
    # stay armed, and the next poll would raise WakeupError although wakeup()
    # was never called.
    wakeups = _counting_wakeups(monkeypatch)
    consumer = new_consumer()
    event = _NotifiedEvent()
    consumer._pending_event = event  # type: ignore[attr-defined]
    state = {"armed": False, "completed": False}
    real_submit = _lib.Consumer_commit_sync_cb
    real_execute = _lib.Consumer__execute_callbacks

    def submit(h: int, spec: Any, timeout: int, cb: Callable[..., None]) -> None:
        def completed(*payload: Any) -> None:
            state["completed"] = True
            cb(*payload)

        event.notified = False
        state["armed"] = True
        real_submit(h, spec, timeout, completed)

    def execute(h: int) -> int:
        if state["armed"] and lands == "before_the_completion" and event.notified:
            state["armed"] = False
            raise KeyboardInterrupt
        ran: int = real_execute(h)
        if state["armed"] and lands == "after_the_completion" and state["completed"]:
            state["armed"] = False
            raise KeyboardInterrupt
        return ran

    try:
        consumer.assign(partitions=[TP0])
        monkeypatch.setattr(_lib, "Consumer_commit_sync_cb", submit)
        monkeypatch.setattr(_lib, "Consumer__execute_callbacks", execute)
        with pytest.raises(KeyboardInterrupt):
            consumer.commit(offsets={})
        assert not state["armed"]
        assert state["completed"]
        assert wakeups == []
        assert consumer.poll(timeout=0).is_empty()
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


def test_sigint_while_polling_wakes_up_once_then_reraises_and_consumer_reusable(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Ctrl+C during a waiting call (CLAUDE.md, Python Binding Conventions,
    # Threads and callbacks): the consumer calls wakeup() once, waits for the
    # in-flight operation to end with the WakeupError it caused (the guard
    # released, the result freed), then re-raises KeyboardInterrupt. The
    # wakeup is consumed by that operation, so the consumer is usable and the
    # next call does not see it. pytest runs the test on the main thread, the
    # only one that runs Python signal handlers.
    wakeups: list[int] = []
    real_wakeup = _lib.Consumer_wakeup

    def counting_wakeup(h: int) -> None:
        wakeups.append(h)
        real_wakeup(h)

    monkeypatch.setattr(_lib, "Consumer_wakeup", counting_wakeup)
    consumer = new_consumer()
    try:
        consumer.subscribe(topics=[TOPIC])
        timer = threading.Timer(0.3, os.kill, (os.getpid(), signal.SIGINT))
        start = time.monotonic()
        timer.start()
        try:
            with pytest.raises(KeyboardInterrupt):
                consumer.poll(timeout=30)
            elapsed = time.monotonic() - start
        finally:
            timer.cancel()
            timer.join()
        assert 0.25 <= elapsed < 5, elapsed
        assert len(wakeups) == 1
        assert consumer.subscription() == {TOPIC}
        assert consumer.poll(timeout=0).is_empty()
    finally:
        consumer.close(option=CloseOptions.timeout(timeout=0))


# ---- the one-loop binding (N7; as AsyncKafkaProducer's and AsyncKafkaAdminClient's) ----
def test_an_async_call_from_another_loop_is_refused_before_it_has_any_effect() -> None:
    # By rule 3 the async consumer is bound to one event loop
    # (_async.LoopBinding): the loop of its first waiting call here, as it
    # was built outside any loop. A waiting call from another loop raises
    # RuntimeError before anything else, and the consumer stays usable on its
    # own loop.
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())

    async def main() -> None:
        await consumer.assign(partitions=[TP0])
        loop = asyncio.get_running_loop()
        for call in (lambda: consumer.assign(partitions=[TP1]),
                     lambda: consumer.position(partition=TP0, timeout=0),
                     lambda: consumer.poll(timeout=0),
                     lambda: consumer.close()):
            with pytest.raises(RuntimeError) as e:
                await loop.run_in_executor(None, lambda c=call: asyncio.run(c()))
            assert str(e.value) == BOUND_ELSEWHERE
        assert consumer.assignment() == {TP0}
        assert not consumer._is_closed()  # noqa: SLF001
        await consumer.assign(partitions=[TP1])
        assert consumer.assignment() == {TP1}
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


def test_an_async_consumer_built_on_a_loop_is_bound_to_it() -> None:
    # Built while a loop runs, the consumer is bound to that loop before any
    # call, so even its first waiting call from another loop is refused.
    async def build() -> AsyncKafkaConsumer[bytes, bytes]:
        return AsyncKafkaConsumer(configs=configs())

    loop = asyncio.new_event_loop()
    try:
        consumer = loop.run_until_complete(build())
        with pytest.raises(RuntimeError) as e:
            asyncio.run(consumer.assign(partitions=[TP0]))
        assert str(e.value) == BOUND_ELSEWHERE
        assert consumer.assignment() == set()
        loop.run_until_complete(consumer.close(option=CloseOptions.timeout(timeout=0)))
    finally:
        loop.close()


def test_the_async_mock_consumer_is_not_bound_to_a_loop() -> None:
    # AsyncMockConsumer is pure Python, as AsyncMockProducer: no callback
    # queue to drain, so no binding.
    consumer: AsyncMockConsumer[bytes, bytes] = AsyncMockConsumer(offset_reset_strategy="earliest")
    asyncio.run(consumer.assign(partitions=[TP0]))
    asyncio.run(consumer.assign(partitions=[TP1]))
    assert consumer.assignment() == {TP1}
    asyncio.run(consumer.close())


def test_an_async_timed_call_inside_a_callback_goes_through_the_handle() -> None:
    # AsyncKafkaConsumer runs the callback on its loop, inside the delivering
    # call; a timed call and partitions_for there go through the handle's
    # forms, as on the sync consumer.
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(
            configs=configs(**{"default.api.timeout.ms": 300}))
        await consumer.assign(partitions=[TP0])
        raised: list[BaseException] = []

        async def callback(**kwargs: Any) -> None:
            for call in (lambda: consumer.end_offsets(partitions=[TP0], timeout=0.2),
                         lambda: consumer.partitions_for(topic=TOPIC, timeout=0)):
                try:
                    await call()
                except BaseException as exc:  # noqa: BLE001 - asserted below
                    raised.append(exc)

        await consumer.commit_nowait(offsets={}, callback=callback)
        await consumer.commit(offsets={})
        timed_out, zero = raised
        assert isinstance(timed_out, KafkaTimeoutError)
        assert str(timed_out) == TIMED["end_offsets"][1].format(200)
        assert isinstance(zero, KafkaTimeoutError)
        assert str(zero) == METADATA["partitions_for"][1]
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


@pytest.mark.parametrize("method", ["committed", "position", "partitions_for", "list_topics"])
def test_the_consumers_own_leftover_wakeup_is_not_raised_inside_a_callback(method: str) -> None:
    # Critic 103 item 4: the consumer may leave its own wakeup armed after a
    # call it woke (Ctrl+C, cancellation; _own_wake_left). Inside a callback a
    # wakeable handle call that meets it is made again, so the user sees no
    # WakeupError they did not ask for. partitions_for and list_topics are
    # wakeable, as Java's (AsyncKafkaConsumer.java:1324, :1352).
    calls: dict[str, Callable[[Any], Any]] = {
        "committed": lambda c: c.committed(partitions={TP0}, timeout=0.3),
        "position": lambda c: c.position(partition=TP0, timeout=0.3),
        "partitions_for": lambda c: c.partitions_for(topic="other", timeout=0.3),
        "list_topics": lambda c: c.list_topics(timeout=0.3),
    }
    with new_consumer(**{"default.api.timeout.ms": 60_000}) as consumer:
        consumer.assign(partitions=[TP0])
        raised: list[BaseException] = []

        def callback(**kwargs: Any) -> None:
            with consumer._use() as h:  # noqa: SLF001
                consumer._own_wake_left = True  # noqa: SLF001
                _lib.Consumer_wakeup(h)  # what _own_wake sends
            try:
                calls[method](consumer)
            except BaseException as exc:  # noqa: BLE001 - asserted below
                raised.append(exc)

        consumer.commit_nowait(offsets={}, callback=callback)
        consumer.commit(offsets={})
        assert [type(e) for e in raised] == [KafkaTimeoutError]


def test_an_async_consumer_whose_loop_ended_can_still_be_closed() -> None:
    # The real path the gap 39-41 recovery serves: use the consumer on a loop
    # that ends, then close() it from another loop, which the binding allows
    # once the bound loop is closed (LoopBinding.bind_for_close), as Java's
    # close() always works. Any other call from there stays refused.
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs())

    async def use() -> None:
        await consumer.assign(partitions=[TP0])

    asyncio.run(use())
    _refused_from_another_loop(lambda: consumer.poll(timeout=0))
    asyncio.run(consumer.close(option=CloseOptions.timeout(timeout=0)))
    assert consumer._is_closed()  # noqa: SLF001
