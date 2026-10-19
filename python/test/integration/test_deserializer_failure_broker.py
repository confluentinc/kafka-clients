# Copyright 2026 Confluent Inc.
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

"""A failing value deserializer across two partitions against a broker, sync
and asyncio: ``poll()`` raises ``RecordDeserializationError`` with Java's
message (``CompletedFetch.java:347-349``), the failing record's
``topic_partition()``, ``offset()``, ``origin()`` and ``value_buffer()``, and
the deserializer's exception as its cause, leaving that partition's position
at the record; ``seek(partition=e.topic_partition(), offset=e.offset() + 1)``
then skips the record.

The other partition's records, produced after the first error: Java's
``FetchCollector`` keeps the failing partition's fetch next in line, so every
``poll()`` raises again and the other partition's records wait behind it until
the failing partition is paused or sought past (``FetchCollector.java:98-143``,
``CompletedFetch.java:271-305``). The binding instead seeks the failing
partition back after each failure (``confluent_kafka/consumer/_poll.py:176-185``)
and returns the other partition's records between the errors. The first test
asserts what the client does; the second, a strict xfail, Java's behaviour, a
deviation for the owner to decide. Skips without Docker (``kafka_broker``)."""

from __future__ import annotations

import time
from typing import Any

import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.common.errors import RecordDeserializationError
from confluent_kafka.common.serialization import string_serializer

from .broker_helpers import (
    DEADLINE, MODE, MODES, Consumer, aw, close_consumer, consumer_configs, make_consumer,
    new_topic, produce, run,
)

Origin = RecordDeserializationError.DeserializationExceptionOrigin


def _value_deserializer(topic: str, data: memoryview | None, headers: Any = None) -> str:
    assert data is not None
    text = bytes(data).decode()
    if text == "bad":
        raise ValueError("cannot deserialize")
    return text


class _Polling:
    """A consumer over both partitions of a topic whose partition 0 holds
    a, bad, c; the values it returns per partition, and the errors it raised."""

    def __init__(self, consumer: Consumer, topic: str) -> None:
        self.consumer = consumer
        self.tp0, self.tp1 = (TopicPartition(topic=topic, partition=p) for p in (0, 1))
        self.values: dict[int, list[str]] = {0: [], 1: []}
        self.errors: list[RecordDeserializationError] = []

    async def poll_once(self) -> None:
        try:
            for record in await aw(self.consumer.poll(timeout=0.5)):
                self.values[record.partition()].append(record.value())
        except RecordDeserializationError as e:
            self.errors.append(e)

    async def until_first_error(self) -> RecordDeserializationError:
        await aw(self.consumer.assign(partitions=[self.tp0, self.tp1]))
        end = time.monotonic() + DEADLINE
        while not self.errors:
            assert time.monotonic() < end, f"no deserialization error: {self.values}"
            await self.poll_once()
        error = self.errors[0]
        assert str(error) == (
            f"Error deserializing VALUE for partition {self.tp0} at offset 1. If needed, "
            "please seek past the record to continue consumption.")
        assert error.topic_partition() == self.tp0 and error.offset() == 1
        assert error.origin() is Origin.VALUE
        value_buffer = error.value_buffer()
        assert value_buffer is not None and bytes(value_buffer) == b"bad"
        assert isinstance(error.__cause__, ValueError)
        assert self.values[0] == ["a"]
        assert await aw(self.consumer.position(partition=self.tp0, timeout=10)) == 1
        return error

    async def skip(self, error: RecordDeserializationError) -> None:
        """Seek past the failing record and read the rest of partition 0."""
        await aw(self.consumer.seek(partition=error.topic_partition(),
                                    offset=error.offset() + 1))
        end = time.monotonic() + DEADLINE
        while len(self.values[0]) < 2:
            assert time.monotonic() < end, f"partition 0 stuck: {self.values}"
            await self.poll_once()
        assert await aw(self.consumer.position(partition=self.tp0, timeout=10)) == 3
        assert await aw(self.consumer.position(partition=self.tp1, timeout=10)) == 3


def _topic(broker: Any) -> str:
    topic = new_topic(broker, "py-deser-2p", partitions=2)
    produce(broker, topic, ["a", "bad", "c"], partition=0, value_serializer=string_serializer())
    return topic


def _produce_tp1(broker: Any, topic: str) -> None:
    produce(broker, topic, ["x", "y", "z"], partition=1, value_serializer=string_serializer())


@MODE
def test_a_failing_record_does_not_hold_back_the_other_partition(kafka_broker: Any,
                                                                  mode: str) -> None:
    # What the client does: partition 1's records come through, unpaused,
    # while partition 0 stays at its failing record (raising again whenever a
    # poll reaches it).
    topic = _topic(kafka_broker)

    async def body() -> dict[int, list[str]]:
        consumer = make_consumer(mode, consumer_configs(kafka_broker),
                                 value_deserializer=_value_deserializer)
        polling = _Polling(consumer, topic)
        try:
            error = await polling.until_first_error()
            _produce_tp1(kafka_broker, topic)
            end = time.monotonic() + DEADLINE
            while len(polling.values[1]) < 3:
                assert time.monotonic() < end, f"partition 1 held back: {polling.values}"
                await polling.poll_once()
            assert {(e.topic_partition(), e.offset()) for e in polling.errors} == \
                {(polling.tp0, 1)}
            assert await aw(consumer.position(partition=polling.tp0, timeout=10)) == 1
            await polling.skip(error)
        finally:
            await close_consumer(consumer)
        return polling.values

    assert run(body) == {0: ["a", "c"], 1: ["x", "y", "z"]}


@pytest.mark.xfail(strict=True, raises=AssertionError, reason=(
    "deviation for the owner to decide: Java keeps the failing partition's fetch next in "
    "line, so every poll() raises and the other partitions' records wait until it is "
    "paused or sought past (FetchCollector.java:98-143); the binding seeks partition 0 "
    "back after each failure (confluent_kafka/consumer/_poll.py:176-185) and lets "
    "partition 1's records through between the errors"))
@pytest.mark.parametrize("mode", MODES)
def test_a_failing_record_holds_back_the_other_partition_as_java(kafka_broker: Any,
                                                                 mode: str) -> None:
    topic = _topic(kafka_broker)

    async def body() -> dict[int, list[str]]:
        consumer = make_consumer(mode, consumer_configs(kafka_broker),
                                 value_deserializer=_value_deserializer)
        polling = _Polling(consumer, topic)
        try:
            error = await polling.until_first_error()
            _produce_tp1(kafka_broker, topic)
            # Unpaused: every poll raises, partition 1 returns nothing.
            end = time.monotonic() + 4
            polls = 0
            while time.monotonic() < end:
                await polling.poll_once()
                polls += 1
            assert polling.values[1] == [], f"partition 1 not held back: {polling.values}"
            assert len(polling.errors) == polls + 1
            # Paused, partition 1's records come through.
            await aw(consumer.pause(partitions=[polling.tp0]))
            end = time.monotonic() + DEADLINE
            while len(polling.values[1]) < 3:
                assert time.monotonic() < end, f"partition 1 held back: {polling.values}"
                await polling.poll_once()
            await aw(consumer.resume(partitions=[polling.tp0]))
            await polling.skip(error)
        finally:
            await close_consumer(consumer)
        return polling.values

    assert run(body) == {0: ["a", "c"], 1: ["x", "y", "z"]}
