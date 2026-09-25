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
  (``testSecondPollWithDeserializationErrorThrowsRecordDeserializationException``).

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
from confluent_kafka.common import TopicPartition
from confluent_kafka.common.errors import RecordDeserializationError, WakeupError
from confluent_kafka.common.serialization import string_deserializer, string_serializer
from confluent_kafka.consumer import (
    AsyncKafkaConsumer, CloseOptions, ConsumerRebalanceListener, KafkaConsumer,
    OffsetAndMetadata,
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
