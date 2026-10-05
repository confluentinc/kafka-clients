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

"""``KafkaConsumer`` / ``AsyncKafkaConsumer`` against a broker, where Java's
``KafkaConsumerTest`` scripts a ``MockClient`` the binding cannot inject:

- the two ``consumer-threading.md`` §31 regression tests, sync and async: a
  ``commit()`` inside ``on_partitions_revoked`` succeeds, and the rebalance does
  not advance until the listener returns (``testRebalanceException`` /
  ``testSubscriptionChangesWith*``'s listener assertions);
- the ``commit_nowait()`` callback running on the polling thread or event loop
  (C47);
- ``wakeup()`` breaking a waiting ``poll()`` (``testWakeupWithFetchDataAvailable``);
- a failing deserializer leaving the position at the record
  (``testSecondPollWithDeserializationErrorThrowsRecordDeserializationException``);
- ``commit_nowait()`` while a rebalance is delivered, the async notify, a
  listener's exception as Java raises it (``testRebalanceException``'s
  consumer-protocol counterpart), a failed commit's callback;
- the ``KafkaConsumerTest`` cases Java scripts through a ``MockClient``, with the
  broker's answers standing for the prepared responses:
  ``testMissingOffsetNoResetPolicy``, ``testResetToCommittedOffset``,
  ``testResetUsingAutoResetPolicy``, ``testResetUsingDurationBasedAutoResetPolicy``,
  ``testOffsetIsValidAfterSeek``, ``testCommitsFetchedDuringAssign``,
  ``testNoCommittedOffsets``, ``testManualAssignmentChangeWithAutoCommitEnabled``,
  ``testManualAssignmentChangeWithAutoCommitDisabled``,
  ``testOffsetOfPausedPartitions``, ``testListOffsetShouldUpdateSubscriptions``,
  ``testSubscriptionOnInvalidTopic``,
  ``verifyNoCoordinatorLookupForManualAssignmentWithSeek`` and
  ``testPollSendsRequestToJoin`` (the rest are named in
  ``test/unit/test_kafka_consumer.py``).

Skips without Docker (``kafka_broker``).
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from typing import Any

import pytest

from confluent_kafka import ConcurrentModificationError
from confluent_kafka.common import KafkaError, TopicPartition
from confluent_kafka.common.errors import (
    InvalidTopicError, RecordDeserializationError, TopicAuthorizationError, WakeupError,
)
from confluent_kafka.common.serialization import string_deserializer, string_serializer
from confluent_kafka.consumer import (
    AsyncKafkaConsumer, CloseOptions, CommitFailedError, ConsumerRebalanceListener, KafkaConsumer,
    NoOffsetForPartitionError, OffsetAndMetadata, RetriableCommitFailedError,
)
from confluent_kafka.producer import KafkaProducer, ProducerRecord

from .conftest import create_topic

DEADLINE = 60.0


def _topic(broker: Any, values: list[str]) -> str:
    topic = f"py-consumer-{uuid.uuid4().hex[:12]}"
    create_topic(broker, topic)
    producer: KafkaProducer[str, str] = KafkaProducer(
        configs={"bootstrap.servers": broker.external_bootstrap},
        key_serializer=string_serializer(), value_serializer=string_serializer())
    try:
        for value in values:
            producer.send(record=ProducerRecord(topic=topic, partition=0, value=value))
        producer.flush()
    finally:
        producer.close()
    return topic


def _configs(broker: Any, **extra: Any) -> dict[str, Any]:
    configs: dict[str, Any] = {
        "bootstrap.servers": broker.external_bootstrap, "group.protocol": "consumer",
        "group.id": f"py-group-{uuid.uuid4().hex[:12]}", "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }
    configs.update(extra)
    return configs


def _poll_until(consumer: KafkaConsumer[Any, Any], done: Any) -> list[Any]:
    values: list[Any] = []
    deadline = time.monotonic() + DEADLINE
    while not done(values):
        assert time.monotonic() < deadline, "timed out polling"
        values.extend(r.value() for r in consumer.poll(timeout=0.5))
    return values


async def _poll_until_async(consumer: AsyncKafkaConsumer[Any, Any], done: Any) -> list[Any]:
    values: list[Any] = []
    deadline = time.monotonic() + DEADLINE
    while not done(values):
        assert time.monotonic() < deadline, "timed out polling"
        values.extend(r.value() for r in await consumer.poll(timeout=0.5))
    return values


# ---------------------------------------------------------------------------
# consumer-threading.md §31
# ---------------------------------------------------------------------------
def test_commit_inside_on_partitions_revoked_succeeds(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a", "b", "c"])
    tp = TopicPartition(topic=topic, partition=0)
    consumer: KafkaConsumer[str, str] = KafkaConsumer(
        configs=_configs(kafka_broker), value_deserializer=string_deserializer())
    seen: dict[str, Any] = {}

    class CommitOnRevoke(ConsumerRebalanceListener):
        def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
            seen["thread"] = threading.get_ident()
            consumer.commit(offsets={tp: OffsetAndMetadata(offset=3)})
            seen["committed"] = True

    try:
        consumer.subscribe(topics=[topic], callback=CommitOnRevoke())
        assert _poll_until(consumer, lambda v: len(v) >= 3) == ["a", "b", "c"]
        consumer.unsubscribe()
        assert seen == {"thread": threading.get_ident(), "committed": True}
        committed = consumer.committed(partitions=[tp])[tp]
        assert committed is not None and committed.offset() == 3
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_rebalance_does_not_advance_until_the_listener_returns(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a"])
    tp = TopicPartition(topic=topic, partition=0)
    consumer: KafkaConsumer[str, str] = KafkaConsumer(
        configs=_configs(kafka_broker), value_deserializer=string_deserializer())
    entered, release, returned = threading.Event(), threading.Event(), threading.Event()
    inside: dict[str, Any] = {}

    class Blocking(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
            if not partitions:
                return
            # The listener may call back into its consumer.
            inside["assignment"] = consumer.assignment()
            inside["position"] = consumer.position(partition=tp)
            entered.set()
            assert release.wait(DEADLINE)

    consumer.subscribe(topics=[topic], callback=Blocking())
    outcome: dict[str, Any] = {}

    def drive() -> None:
        try:
            outcome["values"] = _poll_until(consumer, lambda v: len(v) >= 1)
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc
        returned.set()

    worker = threading.Thread(target=drive)
    worker.start()
    try:
        assert entered.wait(DEADLINE), "the listener was not called"
        assert not returned.wait(0.5), "poll returned while the listener ran"
        # The waiting poll still holds the consumer.
        with pytest.raises(ConcurrentModificationError):
            consumer.assignment()
        release.set()
        assert returned.wait(DEADLINE)
    finally:
        release.set()
        worker.join(DEADLINE)
    assert outcome == {"values": ["a"]}
    assert inside == {"assignment": {tp}, "position": 0}
    assert consumer.assignment() == {tp}
    consumer.close(option=CloseOptions.timeout(0))


def test_async_commit_inside_an_async_on_partitions_revoked_succeeds(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a", "b"])
    tp = TopicPartition(topic=topic, partition=0)

    async def main() -> None:
        consumer: AsyncKafkaConsumer[str, str] = AsyncKafkaConsumer(
            configs=_configs(kafka_broker), value_deserializer=string_deserializer())
        seen: dict[str, Any] = {}

        class CommitOnRevoke(ConsumerRebalanceListener):
            async def on_partitions_revoked(  # type: ignore[override]
                    self, partitions: set[TopicPartition]) -> None:
                await consumer.commit(offsets={tp: OffsetAndMetadata(offset=2)})
                seen["committed"] = threading.get_ident()

        try:
            await consumer.subscribe(topics=[topic], callback=CommitOnRevoke())
            assert await _poll_until_async(consumer, lambda v: len(v) >= 2) == ["a", "b"]
            await consumer.unsubscribe()
            assert seen == {"committed": threading.get_ident()}
            committed = (await consumer.committed(partitions=[tp]))[tp]
            assert committed is not None and committed.offset() == 2
        finally:
            await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())


def test_async_rebalance_does_not_advance_until_the_listener_returns(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a"])
    tp = TopicPartition(topic=topic, partition=0)

    async def main() -> None:
        consumer: AsyncKafkaConsumer[str, str] = AsyncKafkaConsumer(
            configs=_configs(kafka_broker), value_deserializer=string_deserializer())
        entered, release = asyncio.Event(), asyncio.Event()

        class Blocking(ConsumerRebalanceListener):
            async def on_partitions_assigned(  # type: ignore[override]
                    self, partitions: set[TopicPartition]) -> None:
                if not partitions:
                    return
                entered.set()
                await release.wait()

        await consumer.subscribe(topics=[topic], callback=Blocking())
        polling = asyncio.ensure_future(_poll_until_async(consumer, lambda v: len(v) >= 1))
        await asyncio.wait_for(entered.wait(), DEADLINE)
        await asyncio.sleep(0.5)
        assert not polling.done(), "poll returned while the listener ran"
        # Another task on the loop meets the single-owner guard.
        with pytest.raises(ConcurrentModificationError):
            consumer.assignment()
        release.set()
        assert await asyncio.wait_for(polling, DEADLINE) == ["a"]
        assert consumer.assignment() == {tp}
        await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())


# ---------------------------------------------------------------------------
# The commit_nowait() callback runs on the polling thread (C47)
# ---------------------------------------------------------------------------
def test_commit_callback_runs_on_the_polling_thread(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a"])
    tp = TopicPartition(topic=topic, partition=0)
    consumer: KafkaConsumer[str, str] = KafkaConsumer(
        configs=_configs(kafka_broker), value_deserializer=string_deserializer())
    seen: list[Any] = []
    try:
        consumer.assign(partitions=[tp])
        assert _poll_until(consumer, lambda v: len(v) >= 1) == ["a"]
        consumer.commit_nowait(offsets={tp: OffsetAndMetadata(offset=1)},
                               callback=lambda o, e: seen.append((o, e, threading.get_ident())))
        deadline = time.monotonic() + DEADLINE
        while not seen:
            assert time.monotonic() < deadline, "the commit callback did not run"
            consumer.poll(timeout=0.2)
        offsets, error, thread = seen[0]
        assert error is None and thread == threading.get_ident()
        assert offsets == {tp: OffsetAndMetadata(offset=1)}
    finally:
        consumer.close()


def test_async_commit_callback_runs_on_the_event_loop(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a"])
    tp = TopicPartition(topic=topic, partition=0)

    async def main() -> None:
        consumer: AsyncKafkaConsumer[str, str] = AsyncKafkaConsumer(
            configs=_configs(kafka_broker), value_deserializer=string_deserializer())
        seen: list[int] = []
        try:
            await consumer.assign(partitions=[tp])
            assert await _poll_until_async(consumer, lambda v: len(v) >= 1) == ["a"]
            consumer.commit_nowait(offsets={tp: OffsetAndMetadata(offset=1)},
                                   callback=lambda o, e: seen.append(threading.get_ident()))
            await consumer.commit()  # waits for the pending async commit
            assert seen == [threading.get_ident()]
        finally:
            await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())


# ---------------------------------------------------------------------------
# wakeup() and a failing deserializer
# ---------------------------------------------------------------------------
def test_wakeup_breaks_a_waiting_poll(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, [])
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=_configs(kafka_broker))
    consumer.subscribe(topics=[topic])
    outcome: dict[str, Any] = {}

    def run() -> None:
        started = time.monotonic()
        try:
            consumer.poll(timeout=30)
            outcome["result"] = "returned"
        except WakeupError:
            outcome["result"] = "wakeup"
        outcome["elapsed"] = time.monotonic() - started

    worker = threading.Thread(target=run)
    worker.start()
    time.sleep(1.0)
    consumer.wakeup()
    worker.join(DEADLINE)
    assert outcome["result"] == "wakeup" and outcome["elapsed"] < 15
    # The call after proceeds normally.
    consumer.poll(timeout=0)
    consumer.close(option=CloseOptions.timeout(0))


def test_a_failing_deserializer_leaves_the_position_at_the_record(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a", "bad", "c"])
    tp = TopicPartition(topic=topic, partition=0)

    def value_deserializer(topic: str, data: memoryview | None, headers: Any = None) -> str:
        assert data is not None
        text = bytes(data).decode()
        if text == "bad":
            raise ValueError("cannot deserialize")
        return text

    consumer: KafkaConsumer[bytes, str] = KafkaConsumer(
        configs=_configs(kafka_broker), value_deserializer=value_deserializer)
    try:
        consumer.assign(partitions=[tp])
        values: list[str | None] = []
        deadline = time.monotonic() + DEADLINE
        error: RecordDeserializationError | None = None
        while error is None:
            assert time.monotonic() < deadline, "no deserialization error"
            try:
                values.extend(r.value() for r in consumer.poll(timeout=0.5))
            except RecordDeserializationError as e:
                error = e
        # The records before the failing one were returned first.
        assert values == ["a"]
        assert error.topic_partition() == tp and error.offset() == 1
        assert isinstance(error.__cause__, ValueError)
        assert consumer.position(partition=tp) == 1
        # Unmoved: the next poll fails on it again.
        with pytest.raises(RecordDeserializationError):
            deadline = time.monotonic() + DEADLINE
            while time.monotonic() < deadline:
                assert consumer.poll(timeout=0.5).is_empty()
        consumer.seek(partition=error.topic_partition(), offset=error.offset() + 1)
        assert _poll_until(consumer, lambda v: len(v) >= 1) == ["c"]
    finally:
        consumer.close(option=CloseOptions.timeout(0))


# ---------------------------------------------------------------------------
# commit_nowait() while a rebalance is delivered: the core's commit processes
# the background events while it waits for its offsets (consumer-threading.md
# §31), so a listener callback queued then runs on the calling thread (or loop)
# inside commit_nowait() — it must never block (Critic 76 B1)
# ---------------------------------------------------------------------------
class _Producing:
    """Produce to every partition of ``topic`` until stopped."""

    def __init__(self, broker: Any, topic: str, partitions: int) -> None:
        self._stop = threading.Event()
        self._producer: KafkaProducer[bytes, bytes] = KafkaProducer(
            configs={"bootstrap.servers": broker.external_bootstrap, "linger.ms": 5})
        self._thread = threading.Thread(target=self._run, args=(topic, partitions), daemon=True)
        self._thread.start()

    def _run(self, topic: str, partitions: int) -> None:
        sent = 0
        while not self._stop.is_set():
            self._producer.send(record=ProducerRecord(topic=topic, partition=sent % partitions,
                                                      value=b"x" * 10))
            sent += 1
            if sent % 50 == 0:
                self._producer.flush()
                time.sleep(0.01)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(DEADLINE)
        self._producer.close()


class _Member:
    """A second group member, joining when started and polling until stopped."""

    def __init__(self, configs: dict[str, Any], topic: str) -> None:
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, args=(configs, topic), daemon=True)
        self._thread.start()

    def _run(self, configs: dict[str, Any], topic: str) -> None:
        member: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=configs)
        member.subscribe(topics=[topic])
        while not self._stop.is_set():
            member.poll(timeout=0.2)
        member.close(option=CloseOptions.timeout(0))

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(DEADLINE)


def _rebalance_topic(broker: Any) -> str:
    topic = f"py-consumer-rebalance-{uuid.uuid4().hex[:12]}"
    create_topic(broker, topic, partitions=4)
    return topic


@pytest.mark.parametrize("with_callback", [False, True])
def test_commit_nowait_during_a_rebalance_runs_the_listener_on_this_thread(
        kafka_broker: Any, with_callback: bool) -> None:
    topic = _rebalance_topic(kafka_broker)
    configs = _configs(kafka_broker, **{"max.poll.records": "5"})
    events: list[tuple[str, int, int]] = []
    state: dict[str, Any] = {"after_revoke": 0, "callbacks": []}

    class Recording(ConsumerRebalanceListener):
        def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
            events.append(("revoked", len(partitions), threading.get_ident()))

        def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
            events.append(("assigned", len(partitions), threading.get_ident()))

    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=configs)
    stop = threading.Event()

    def loop() -> None:
        try:
            consumer.subscribe(topics=[topic], callback=Recording())
            while not stop.is_set():
                if not consumer.poll(timeout=0.5).is_empty():
                    time.sleep(0.05)  # processing the records
                    if with_callback:
                        consumer.commit_nowait(
                            callback=lambda o, e: state["callbacks"].append(threading.get_ident()))
                    else:
                        consumer.commit_nowait()
                if any(event[0] == "revoked" for event in events):
                    state["after_revoke"] += 1
        except BaseException as exc:  # noqa: BLE001 - asserted below
            state["error"] = exc

    producing = _Producing(kafka_broker, topic, 4)
    worker = threading.Thread(target=loop, daemon=True)
    worker.start()
    member: _Member | None = None
    try:
        deadline = time.monotonic() + DEADLINE
        while ("assigned", 4, worker.ident) not in events:
            assert time.monotonic() < deadline, f"no assignment: {events}"
            time.sleep(0.1)
        member = _Member(configs, topic)
        deadline = time.monotonic() + DEADLINE
        while state["after_revoke"] < 20 and "error" not in state:
            assert time.monotonic() < deadline, (
                f"the poll/commit_nowait loop stopped: {events}, {state}")
            time.sleep(0.1)
        stop.set()
        worker.join(DEADLINE)
        assert not worker.is_alive()
        assert "error" not in state, state
        assert [thread for name, _, thread in events if name == "revoked"][:1] == [worker.ident]
        if with_callback:
            assert state["callbacks"] and set(state["callbacks"]) == {worker.ident}
    finally:
        stop.set()
        if member is not None:
            member.stop()
        producing.stop()
        if not worker.is_alive():
            # The default close timeout: pending async commits are awaited.
            consumer.close()


@pytest.mark.parametrize("coroutine_listener", [True, False])
def test_async_commit_nowait_during_a_rebalance_runs_the_listener_on_the_loop(
        kafka_broker: Any, coroutine_listener: bool) -> None:
    topic = _rebalance_topic(kafka_broker)
    configs = _configs(kafka_broker, **{"max.poll.records": "5"})
    events: list[tuple[str, int, int]] = []
    state: dict[str, Any] = {"after_revoke": 0}

    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs)

        def record(name: str, partitions: set[TopicPartition]) -> None:
            events.append((name, len(partitions), threading.get_ident()))

        class Plain(ConsumerRebalanceListener):
            def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
                record("revoked", partitions)

            def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
                record("assigned", partitions)

        class Coroutine(ConsumerRebalanceListener):
            async def on_partitions_revoked(  # type: ignore[override]
                    self, partitions: set[TopicPartition]) -> None:
                await asyncio.sleep(0.01)
                # A reentrant call from the listener still works.
                consumer.assignment()
                record("revoked", partitions)

            async def on_partitions_assigned(  # type: ignore[override]
                    self, partitions: set[TopicPartition]) -> None:
                await asyncio.sleep(0.01)
                record("assigned", partitions)

        member: _Member | None = None
        try:
            await consumer.subscribe(topics=[topic],
                                     callback=Coroutine() if coroutine_listener else Plain())
            deadline = time.monotonic() + 2 * DEADLINE
            while state["after_revoke"] < 20:
                assert time.monotonic() < deadline, f"no progress: {events}"
                if not (await consumer.poll(timeout=0.5)).is_empty():
                    await asyncio.sleep(0.05)
                    consumer.commit_nowait()
                if member is None and any(e[:2] == ("assigned", 4) for e in events):
                    member = _Member(configs, topic)
                if any(event[0] == "revoked" for event in events):
                    state["after_revoke"] += 1
            state["loop_thread"] = threading.get_ident()
        finally:
            if member is not None:
                member.stop()
            # The default close timeout: pending async commits are awaited.
            await consumer.close()

    def run() -> None:
        try:
            asyncio.run(main())
        except BaseException as exc:  # noqa: BLE001 - asserted below
            state["error"] = exc

    producing = _Producing(kafka_broker, topic, 4)
    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        worker.join(3 * DEADLINE)
        assert not worker.is_alive(), f"the event loop is blocked: {events}, {state}"
        assert "error" not in state, state
        revoked = [thread for name, _, thread in events if name == "revoked"]
        assert revoked and set(revoked) == {state["loop_thread"]}
    finally:
        producing.stop()


# ---------------------------------------------------------------------------
# AsyncKafkaConsumer: a call the guard rejects does not take the pending-callback
# notify from the call in flight (Critic 76 F1)
# ---------------------------------------------------------------------------
def test_async_rejected_call_does_not_stall_the_polling_task(kafka_broker: Any) -> None:
    topic = f"py-consumer-notify-{uuid.uuid4().hex[:12]}"
    create_topic(kafka_broker, topic, partitions=2)
    configs = _configs(kafka_broker)

    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=configs)
        loop = asyncio.get_running_loop()
        revoked = asyncio.Event()

        class Recording(ConsumerRebalanceListener):
            def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
                if partitions:
                    revoked.set()

        member: _Member | None = None
        stop = asyncio.Event()
        try:
            await consumer.subscribe(topics=[topic], callback=Recording())
            deadline = loop.time() + DEADLINE
            while len(consumer.assignment()) < 2:
                assert loop.time() < deadline, "no assignment"
                await consumer.poll(timeout=0.2)

            async def poller() -> None:
                while not stop.is_set():
                    await consumer.poll(timeout=30)

            polling = asyncio.ensure_future(poller())
            await asyncio.sleep(0.2)
            with pytest.raises(ConcurrentModificationError):
                await consumer.position(partition=TopicPartition(topic=topic, partition=0))
            member = _Member(configs, topic)
            # Delivered by the in-flight poll(timeout=30), well before it times out.
            await asyncio.wait_for(revoked.wait(), 20)
            stop.set()
            await asyncio.wait_for(polling, DEADLINE)
        finally:
            if member is not None:
                member.stop()
            await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())


# ---------------------------------------------------------------------------
# A listener's exception comes back as Java raises it (Critic 76 F3):
# AsyncKafkaConsumer.invokeRebalanceCallbacks → maybeWrapAsKafkaException(e,
# "User rebalance callback throws an error")
# ---------------------------------------------------------------------------
class _MyCommitFailed(CommitFailedError):
    pass


def _raising_listener(raised: BaseException) -> ConsumerRebalanceListener:
    class Raising(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
            if partitions:
                raise raised

    return Raising()


def _poll_for_error(consumer: KafkaConsumer[Any, Any]) -> BaseException:
    deadline = time.monotonic() + DEADLINE
    while True:
        assert time.monotonic() < deadline, "poll() never raised the listener's error"
        try:
            consumer.poll(timeout=0.5)
        except Exception as exc:  # noqa: BLE001 - returned for the assertions
            return exc


def test_listener_value_error_is_wrapped_as_java_does(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, [])
    raised = ValueError("listener boom")
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=_configs(kafka_broker))
    try:
        consumer.subscribe(topics=[topic], callback=_raising_listener(raised))
        error = _poll_for_error(consumer)
        assert type(error) is KafkaError
        assert str(error) == "User rebalance callback throws an error"
        assert error.__cause__ is raised
    finally:
        consumer.close(option=CloseOptions.timeout(0))


@pytest.mark.parametrize("raised", [
    _MyCommitFailed(message="sub boom"),
    TopicAuthorizationError(unauthorized_topics={"t1"}),
], ids=["kafka-error-subclass", "payload-error"])
def test_listener_kafka_error_is_raised_as_the_same_instance(kafka_broker: Any,
                                                             raised: KafkaError) -> None:
    topic = _topic(kafka_broker, [])
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=_configs(kafka_broker))
    try:
        consumer.subscribe(topics=[topic], callback=_raising_listener(raised))
        error = _poll_for_error(consumer)
        assert error is raised
        if isinstance(raised, TopicAuthorizationError):
            assert error.unauthorized_topics() == {"t1"}  # type: ignore[attr-defined]
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_async_listener_error_is_wrapped_as_java_does(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, [])
    raised = ValueError("listener boom")

    class Raising(ConsumerRebalanceListener):
        async def on_partitions_assigned(  # type: ignore[override]
                self, partitions: set[TopicPartition]) -> None:
            if partitions:
                raise raised

    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(
            configs=_configs(kafka_broker))
        try:
            await consumer.subscribe(topics=[topic], callback=Raising())
            deadline = time.monotonic() + DEADLINE
            while True:
                assert time.monotonic() < deadline, "poll() never raised the listener's error"
                try:
                    await consumer.poll(timeout=0.5)
                except KafkaError as error:
                    assert type(error) is KafkaError
                    assert str(error) == "User rebalance callback throws an error"
                    assert error.__cause__ is raised
                    return
        finally:
            await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())


# ---------------------------------------------------------------------------
# The commit callback of a failed commit gets offsets=None, as Java's
# whenComplete on an exceptionally completed future (Critic 76 F4)
# ---------------------------------------------------------------------------
def test_commit_callback_of_a_failed_commit_gets_null_offsets(kafka_broker: Any) -> None:
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=_configs(kafka_broker))
    missing = TopicPartition(topic=f"py-missing-{uuid.uuid4().hex[:12]}", partition=0)
    seen: list[tuple[Any, Any]] = []
    try:
        consumer.commit_nowait(offsets={missing: OffsetAndMetadata(offset=5)},
                               callback=lambda o, e: seen.append((o, e)))
        deadline = time.monotonic() + DEADLINE
        while not seen:
            assert time.monotonic() < deadline, "the commit callback did not run"
            try:
                consumer.commit(offsets={})
            except KafkaError:
                pass
            time.sleep(0.2)
        offsets, error = seen[0]
        assert offsets is None
        assert isinstance(error, RetriableCommitFailedError)
    finally:
        consumer.close(option=CloseOptions.timeout(0))


# ---------------------------------------------------------------------------
# ConsumerRecords.next_offsets() past a transaction marker (Critic 76 F5):
# Java's FetchCollector reports the fetch's next offset, the position after the
# poll. The FFI's ConsumerRecords has no next-offsets accessor, so the binding
# recomputes last offset + 1 (ffi-overload-gaps.md).
# ---------------------------------------------------------------------------
def _transactional_topic(broker: Any, values: list[str]) -> str:
    topic = f"py-consumer-txn-{uuid.uuid4().hex[:12]}"
    create_topic(broker, topic)
    producer: KafkaProducer[str, str] = KafkaProducer(
        configs={"bootstrap.servers": broker.external_bootstrap,
                 "transactional.id": f"py-txn-{uuid.uuid4().hex[:12]}"},
        key_serializer=string_serializer(), value_serializer=string_serializer())
    try:
        producer.init_transactions()
        producer.begin_transaction()
        for value in values:
            producer.send(record=ProducerRecord(topic=topic, partition=0, value=value))
        producer.commit_transaction()
    finally:
        producer.close()
    return topic


@pytest.mark.skip(reason=(
    "The FFI's ConsumerRecords has no next-offsets accessor (ffi-overload-gaps.md): "
    "next_offsets() is the last offset + 1 (3), Java's is past the commit marker (4)"))
def test_next_offsets_skip_the_transaction_marker(kafka_broker: Any) -> None:
    # Offsets 0-2 are the records, 3 the commit marker.
    topic = _transactional_topic(kafka_broker, ["a", "b", "c"])
    tp = TopicPartition(topic=topic, partition=0)
    consumer: KafkaConsumer[str, str] = KafkaConsumer(
        configs=_configs(kafka_broker, **{"isolation.level": "read_committed"}),
        value_deserializer=string_deserializer())
    try:
        consumer.assign(partitions=[tp])
        deadline = time.monotonic() + DEADLINE
        while True:
            assert time.monotonic() < deadline, "no records"
            records = consumer.poll(timeout=0.5)
            if not records.is_empty():
                break
        assert [r.value() for r in records] == ["a", "b", "c"]
        assert consumer.position(partition=tp) == 4
        assert records.next_offsets()[tp].offset() == 4
    finally:
        consumer.close(option=CloseOptions.timeout(0))


# ---------------------------------------------------------------------------
# KafkaConsumerTest cases Java scripts through a MockClient, run against the
# broker: the broker's answers stand for the prepared responses (the offsets
# differ: Java's MockClient invents 50 / 539 / 10000, here they are the real
# log's)
# ---------------------------------------------------------------------------
def _produce(broker: Any, topic: str, partition: int, values: list[str]) -> None:
    producer: KafkaProducer[str, str] = KafkaProducer(
        configs={"bootstrap.servers": broker.external_bootstrap},
        key_serializer=string_serializer(), value_serializer=string_serializer())
    try:
        for value in values:
            producer.send(record=ProducerRecord(topic=topic, partition=partition, value=value))
        producer.flush()
    finally:
        producer.close()


def _two_partition_topic(broker: Any, values: list[str]) -> str:
    topic = f"py-consumer-2p-{uuid.uuid4().hex[:12]}"
    create_topic(broker, topic, partitions=2)
    for partition in (0, 1):
        _produce(broker, topic, partition, values)
    return topic


def _commit(configs: dict[str, Any], offsets: dict[TopicPartition, OffsetAndMetadata]) -> None:
    """Commit ``offsets`` for the group of ``configs`` (Java's prepared OffsetFetch)."""
    committer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=configs)
    try:
        committer.assign(partitions=list(offsets))
        committer.commit(offsets=offsets)
    finally:
        committer.close(option=CloseOptions.timeout(0))


def _poll_eventually_raises(consumer: KafkaConsumer[Any, Any]) -> BaseException:
    """Java's assertPollEventuallyThrows: poll(ZERO) until it raises."""
    deadline = time.monotonic() + DEADLINE
    while True:
        assert time.monotonic() < deadline, "poll() never raised"
        try:
            consumer.poll(timeout=0)
        except Exception as exc:  # noqa: BLE001 - returned for the assertions
            return exc
        time.sleep(0.05)


def test_missing_offset_no_reset_policy(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a"])
    tp = TopicPartition(topic=topic, partition=0)
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(
        configs=_configs(kafka_broker, **{"auto.offset.reset": "none"}))
    try:
        consumer.assign(partitions=[tp])
        error = _poll_eventually_raises(consumer)
        assert isinstance(error, NoOffsetForPartitionError)
        assert error.partitions() == {tp}  # type: ignore[attr-defined]
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_reset_to_committed_offset(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a", "b", "c"])
    tp = TopicPartition(topic=topic, partition=0)
    configs = _configs(kafka_broker, **{"auto.offset.reset": "none"})
    _commit(configs, {tp: OffsetAndMetadata(offset=2)})
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=configs)
    try:
        consumer.assign(partitions=[tp])
        consumer.poll(timeout=0)
        assert consumer.position(partition=tp) == 2
    finally:
        consumer.close(option=CloseOptions.timeout(0))


@pytest.mark.parametrize("strategy, expected", [("latest", 3), ("by_duration:PT1H", 0)],
                         ids=["testResetUsingAutoResetPolicy",
                              "testResetUsingDurationBasedAutoResetPolicy"])
def test_reset_using_auto_reset_policy(kafka_broker: Any, strategy: str, expected: int) -> None:
    # by_duration:PT1H: the three records were produced just now, so the first
    # offset at or after one hour ago is the first record's.
    topic = _topic(kafka_broker, ["a", "b", "c"])
    tp = TopicPartition(topic=topic, partition=0)
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(
        configs=_configs(kafka_broker, **{"auto.offset.reset": strategy}))
    try:
        consumer.assign(partitions=[tp])
        consumer.poll(timeout=0)
        assert consumer.position(partition=tp) == expected
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_offset_is_valid_after_seek(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker, ["a", "b", "c"])
    tp = TopicPartition(topic=topic, partition=0)
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(
        configs=_configs(kafka_broker, **{"auto.offset.reset": "latest"}))
    try:
        consumer.assign(partitions=[tp])
        consumer.seek(partition=tp, offset=1)
        consumer.poll(timeout=0)
        assert consumer.position(partition=tp) == 1
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_commits_fetched_during_assign(kafka_broker: Any) -> None:
    topic = _two_partition_topic(kafka_broker, ["a", "b", "c"])
    tp0, tp1 = (TopicPartition(topic=topic, partition=p) for p in (0, 1))
    configs = _configs(kafka_broker)
    _commit(configs, {tp0: OffsetAndMetadata(offset=1), tp1: OffsetAndMetadata(offset=2)})
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=configs)
    try:
        consumer.assign(partitions=[tp0])
        committed = consumer.committed(partitions={tp0})[tp0]
        assert committed is not None and committed.offset() == 1
        consumer.assign(partitions=[tp0, tp1])
        committed = consumer.committed(partitions={tp0})[tp0]
        assert committed is not None and committed.offset() == 1
        committed = consumer.committed(partitions={tp1})[tp1]
        assert committed is not None and committed.offset() == 2
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_no_committed_offsets(kafka_broker: Any) -> None:
    topic = _two_partition_topic(kafka_broker, ["a"])
    tp0, tp1 = (TopicPartition(topic=topic, partition=p) for p in (0, 1))
    configs = _configs(kafka_broker)
    _commit(configs, {tp0: OffsetAndMetadata(offset=1)})
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=configs)
    try:
        consumer.assign(partitions=[tp0, tp1])
        committed = consumer.committed(partitions={tp0, tp1})
        assert len(committed) == 2
        tp0_committed = committed[tp0]
        assert tp0_committed is not None and tp0_committed.offset() == 1
        assert committed[tp1] is None
    finally:
        consumer.close(option=CloseOptions.timeout(0))


@pytest.mark.parametrize("auto_commit", [True, False],
                         ids=["testManualAssignmentChangeWithAutoCommitEnabled",
                              "testManualAssignmentChangeWithAutoCommitDisabled"])
def test_manual_assignment_change(kafka_broker: Any, auto_commit: bool) -> None:
    topic, topic2 = _topic(kafka_broker, ["a"]), _topic(kafka_broker, ["x"])
    tp0, t2p0 = TopicPartition(topic=topic, partition=0), TopicPartition(topic=topic2, partition=0)
    # Java's assign() auto-commits the consumed positions once the auto-commit
    # interval has elapsed (CommitRequestManager.updateTimerAndMaybeCommit), as
    # Java's MockTime has it; a short interval and a wait stand for that here.
    configs = _configs(kafka_broker, **{"enable.auto.commit": str(auto_commit).lower(),
                                        "auto.commit.interval.ms": 500})
    _commit(configs, {tp0: OffsetAndMetadata(offset=0)})
    consumer: KafkaConsumer[str, str] = KafkaConsumer(
        configs=configs, value_deserializer=string_deserializer())
    try:
        consumer.assign(partitions={tp0})
        consumer.seek_to_beginning(partitions={tp0})
        committed = consumer.committed(partitions={tp0})[tp0]
        assert committed is not None and committed.offset() == 0
        assert consumer.assignment() == {tp0}
        deadline = time.monotonic() + DEADLINE
        while True:
            assert time.monotonic() < deadline, "no record"
            records = consumer.poll(timeout=0.5)
            if not records.is_empty():
                break
        assert len(records) == 1
        assert consumer.position(partition=tp0) == 1
        assert {tp: oam.offset() for tp, oam in records.next_offsets().items()} == {tp0: 1}
        # Several auto-commit intervals, so an auto-commit (or the one assign()
        # makes once the interval has elapsed) has the consumed position.
        time.sleep(1.6)
        consumer.assign(partitions={t2p0})
        assert consumer.assignment() == {t2p0}
        # With auto-commit, the revoked partition's position is committed; without
        # it, no commit is sent.
        expected = 1 if auto_commit else 0
        deadline = time.monotonic() + (DEADLINE if auto_commit else 2.0)
        while True:
            committed = consumer.committed(partitions={tp0})[tp0]
            assert committed is not None
            if committed.offset() == expected and auto_commit:
                break
            if time.monotonic() >= deadline:
                assert committed.offset() == expected
                break
            time.sleep(0.1)
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_offset_of_paused_partitions(kafka_broker: Any) -> None:
    topic = _two_partition_topic(kafka_broker, ["a", "b", "c"])
    tp0, tp1 = (TopicPartition(topic=topic, partition=p) for p in (0, 1))
    configs = _configs(kafka_broker)
    _commit(configs, {tp0: OffsetAndMetadata(offset=0), tp1: OffsetAndMetadata(offset=0)})
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=configs)
    try:
        partitions = {tp0, tp1}
        consumer.assign(partitions=partitions)
        assert consumer.assignment() == partitions
        consumer.pause(partitions=partitions)
        consumer.seek_to_end(partitions=partitions)
        committed = consumer.committed(partitions={tp0})[tp0]
        assert committed is not None and committed.offset() == 0
        committed = consumer.committed(partitions={tp1})[tp1]
        assert committed is not None and committed.offset() == 0
        assert consumer.position(partition=tp0) == 3
        assert consumer.position(partition=tp1) == 3
        consumer.unsubscribe()
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_list_offset_should_update_subscriptions(kafka_broker: Any) -> None:
    # Java also asserts currentLag(tp0) == 40; current_lag() always returns None
    # (Rust-core gap 3), so that half is not asserted.
    topic = _topic(kafka_broker, ["a", "b", "c"])
    tp = TopicPartition(topic=topic, partition=0)
    configs = _configs(kafka_broker)
    del configs["group.id"]
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=configs)
    try:
        consumer.assign(partitions={tp})
        consumer.seek(partition=tp, offset=1)
        assert consumer.end_offsets(partitions={tp}) == {tp: 3}
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_subscription_on_invalid_topic(kafka_broker: Any) -> None:
    invalid_topic_name = "topic abc"  # Invalid topic name due to space
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=_configs(kafka_broker))
    try:
        consumer.subscribe(topics={invalid_topic_name}, callback=ConsumerRebalanceListener())
        error = _poll_eventually_raises(consumer)
        assert isinstance(error, InvalidTopicError)
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_verify_no_coordinator_lookup_for_manual_assignment_with_seek(kafka_broker: Any) -> None:
    # Without a group.id there is no coordinator to look up: the records come
    # from the list-offsets and fetch alone.
    topic = _topic(kafka_broker, ["a", "b", "c"])
    tp = TopicPartition(topic=topic, partition=0)
    configs = _configs(kafka_broker)
    del configs["group.id"]
    consumer: KafkaConsumer[str, str] = KafkaConsumer(
        configs=configs, value_deserializer=string_deserializer())
    try:
        consumer.assign(partitions={tp})
        consumer.seek_to_beginning(partitions={tp})
        deadline = time.monotonic() + DEADLINE
        while True:
            assert time.monotonic() < deadline, "no records"
            records = consumer.poll(timeout=0.5)
            if not records.is_empty():
                break
        assert len(records) == 3
        assert consumer.position(partition=tp) == 3
        assert {p: oam.offset() for p, oam in records.next_offsets().items()} == {tp: 3}
    finally:
        consumer.close(option=CloseOptions.timeout(0))


def test_poll_sends_request_to_join(kafka_broker: Any) -> None:
    # Java checks subscribe() sends no heartbeat and poll() does; the requests
    # are not observable here, so the join poll() starts is: the member gets
    # its assignment only by polling.
    topic = _topic(kafka_broker, ["a"])
    tp = TopicPartition(topic=topic, partition=0)
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=_configs(kafka_broker))
    try:
        consumer.subscribe(topics=[topic])
        time.sleep(1.0)
        assert consumer.assignment() == set()
        deadline = time.monotonic() + DEADLINE
        while consumer.assignment() != {tp}:
            assert time.monotonic() < deadline, "the member did not join"
            consumer.poll(timeout=0)
            time.sleep(0.05)
    finally:
        consumer.close(option=CloseOptions.timeout(0))
