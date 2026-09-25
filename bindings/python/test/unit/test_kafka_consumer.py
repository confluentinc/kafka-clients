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

"""``KafkaConsumer`` without a broker: the ``KafkaConsumerTest`` cases that run
against an unreachable bootstrap server, with Java's messages, and the
binding's own contracts that need no broker — the commit callback on the
caller's thread (C47), the lifetime of the native handle across a racing
``close()``, the deserializers' configuration route and their close, the
negative timeouts. The broker-backed cases are in
``test/integration/test_kafka_consumer_broker.py``.

Not translated (the subject is not generated, or needs Java's ``MockClient`` /
``MockTime`` / ``MockMetricsReporter`` to script the network, which the binding
cannot inject; the §31 and wakeup cases run against a broker instead):
the ``*Metrics*`` / ``*MetricReporter*`` / ``testConsumerJmxPrefix`` /
``testMetricConfigRecordingLevelInfo`` / ``testPollTimeMetrics`` /
``testPollIdleRatio`` / ``testMeasure*`` cases (metric registration is not
generated and the core's metrics are not reporter-backed);
``testInterceptorConstructor*`` (``interceptor.classes`` raises ``ConfigError``);
``testClientInstanceId*`` (not generated); ``testEnforceRebalance*`` (dropped);
``testSubscriptionWithEmptyPartitionAssignment``, ``testEmptyGroupId``,
``testGracefulClose``, ``testClassicProtocol*``, ``testSubscribeToRe2jPattern
NotSupportedForClassicConsumer``, ``testAssignorNameConflict`` (the classic
protocol, which the core does not implement); the pattern-subscription
``testSubscriptionOnNullPattern`` / ``OnEmptyPattern`` / ``testRegexSubscription``
/ ``testChangingRegexSubscription`` for ``java.util.regex.Pattern`` (dropped;
the ``SubscriptionPattern`` counterparts are below); ``testUnusedConfigs``
(``ssl.protocol`` is a key ``ConsumerConfig`` defines, and the binding can only
log keys the ``ConfigDef`` does not define); ``testInvalidSocketSendBufferSize``
/ ``ReceiveBufferSize`` (the core does not validate ``send.buffer.bytes`` /
``receive.buffer.bytes`` ranges, ``ffi-overload-gaps.md``);
``testOperationsBySubscribingConsumerWithDefaultGroupId``'s
``enable.auto.commit=true`` half (the core does not reject it,
``ffi-overload-gaps.md``); and every case that scripts fetch, heartbeat,
coordinator, offset or rebalance responses through ``MockClient``.
"""

from __future__ import annotations

import asyncio
import gc
import threading
import time
import warnings
import weakref
from datetime import timedelta
from typing import Any

import pytest

from confluent_kafka import ConcurrentModificationError, IllegalArgumentError, IllegalStateError
from confluent_kafka.common import KafkaError, TopicPartition
from confluent_kafka.common.errors import InvalidGroupIdError, UnsupportedVersionError, WakeupError
from confluent_kafka.common.serialization import string_deserializer
from confluent_kafka.consumer import (
    AsyncKafkaConsumer, CloseOptions, ConsumerRebalanceListener, KafkaConsumer,
    OffsetAndMetadata, SubscriptionPattern,
)

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


@pytest.mark.parametrize("setup", ["no-subscription", "empty-subscription", "empty-assignment"])
def test_poll_without_subscription(setup: str) -> None:
    # testPollWithNoSubscription / WithEmptySubscription / WithEmptyUserAssignment.
    with new_consumer(None if setup == "no-subscription" else GROUP_ID) as consumer:
        if setup == "empty-subscription":
            consumer.subscribe(topics=[])
        elif setup == "empty-assignment":
            consumer.assign(partitions=set())
        with pytest.raises(IllegalStateError) as e:
            consumer.poll(timeout=0)
        assert str(e.value) == "Consumer is not subscribed to any topics or assigned any partitions"


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


@pytest.mark.parametrize("group_id", ["", " "])
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


def test_close_rejects_a_negative_timeout_and_stays_open() -> None:
    consumer = new_consumer()
    with pytest.raises(IllegalArgumentError) as e:
        consumer.close(option=CloseOptions.timeout(-1))
    assert str(e.value) == "The timeout cannot be negative."
    with pytest.warns(DeprecationWarning), pytest.raises(IllegalArgumentError):
        consumer.close(timeout=timedelta(seconds=-1))
    assert consumer.subscription() == set()
    consumer.close(option=CloseOptions.timeout(0))


def test_close_with_a_timeout_is_deprecated() -> None:
    consumer = new_consumer()
    with pytest.warns(DeprecationWarning) as caught:
        consumer.close(timeout=0)
    assert str(caught[0].message) == (
        "close(timeout) is deprecated. This method has been deprecated since Kafka 4.1 and "
        "should use close(option=...) instead.")


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
    # An empty commit completes at once (Java's completedFuture); its callback
    # runs in the next call that executes the callbacks — here commit(), whose
    # _async operation queues it, and this thread runs it while waiting.
    with new_consumer() as consumer:
        consumer.assign(partitions=[TP0])
        seen: list[tuple[Any, Any, int]] = []
        consumer.commit_nowait(offsets={}, callback=lambda o, e: seen.append(
            (o, e, threading.get_ident())))
        consumer.commit(offsets={})
        assert seen == [({}, None, threading.get_ident())]


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
        assert not hasattr(consumer, "current_lag")
        with pytest.raises(IllegalArgumentError):
            await consumer.poll(timeout=-1)
        await consumer.close(option=CloseOptions.timeout(0))
        await consumer.close()
        with pytest.raises(IllegalStateError):
            consumer.assignment()

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
