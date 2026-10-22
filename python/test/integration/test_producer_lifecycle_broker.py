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

"""The producer's lifecycle against a broker, sync and asyncio:

- ``flush()`` under load: every record sent before it is acknowledged, its
  callback run and its future done when it returns, the offsets of each
  partition in send order;
- ``close()`` under load waits for the records in flight
  (``close(Duration.ofMillis(Long.MAX_VALUE))``);
- ``close()`` from inside a ``send()`` callback, records still in flight, is
  ``close(0)`` (``KafkaProducer.close``'s self-join guard): it does not
  deadlock, and every record the producer took still gets its callback, once;
- Ctrl+C while ``send()`` waits for a partition's metadata (up to
  ``max.block.ms``) re-raises ``KeyboardInterrupt`` (``CancelledError`` in the
  asyncio task, ``asyncio.run`` then raising ``KeyboardInterrupt``), and the
  producer still closes.

Skips without Docker (``kafka_broker``).
"""

from __future__ import annotations

import asyncio
import os
import signal
import threading
import time
from typing import Any

import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.common import KafkaError, TopicPartition
from confluent_kafka.consumer import CloseOptions, KafkaConsumer
from confluent_kafka.producer import AsyncKafkaProducer, KafkaProducer, ProducerRecord, RecordMetadata

from .broker_helpers import (
    DEADLINE, MODE, aw, consumer_configs, make_producer, new_topic, producer_configs, run,
)

#: The records of the load tests, over two partitions.
LOAD = 4000
VALUE = b"v" * 100

_Outcome = tuple[int, int, Exception | None]


def _recorder(outcomes: list[_Outcome]) -> Any:
    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        outcomes.append((metadata.partition(), metadata.offset(), exception))
    return callback


def _end_offsets(broker: Any, topic: str) -> dict[int, int]:
    consumer: KafkaConsumer[Any, Any] = KafkaConsumer(configs=consumer_configs(broker))
    try:
        tps = [TopicPartition(topic=topic, partition=p) for p in (0, 1)]
        return {tp.partition(): o
                for tp, o in consumer.end_offsets(partitions=tps, timeout=30).items()}
    finally:
        consumer.close(option=CloseOptions(timeout=0))


def _assert_in_send_order(outcomes: list[_Outcome], count: int) -> None:
    """Every record acknowledged, each partition's offsets ``0..`` in the order
    the callbacks ran (callbacks of one partition run in send order)."""
    assert len(outcomes) == count
    assert [e for _, _, e in outcomes if e is not None] == []
    for partition in (0, 1):
        offsets = [o for p, o, _ in outcomes if p == partition]
        assert offsets == list(range(count // 2))


@MODE
def test_flush_under_load_completes_every_record(kafka_broker: Any, mode: str) -> None:
    topic = new_topic(kafka_broker, "py-flush-load", partitions=2)
    outcomes: list[_Outcome] = []

    async def body() -> None:
        producer = make_producer(mode, producer_configs(kafka_broker, linger_ms=20))
        try:
            futures = [await aw(producer.send(
                record=ProducerRecord(topic=topic, partition=i % 2, key=None, value=VALUE),
                callback=_recorder(outcomes))) for i in range(LOAD)]
            await aw(producer.flush())
            assert all(f.is_done() for f in futures)
            assert not any(f.is_completed_exceptionally() for f in futures)
            _assert_in_send_order(outcomes, LOAD)
            last = await aw(futures[-1].get(timeout=0))
            assert (last.partition(), last.offset()) == (1, LOAD // 2 - 1)
        finally:
            await aw(producer.close())

    run(body)
    assert _end_offsets(kafka_broker, topic) == {0: LOAD // 2, 1: LOAD // 2}


@MODE
def test_close_under_load_waits_for_the_records_in_flight(kafka_broker: Any,
                                                          mode: str) -> None:
    topic = new_topic(kafka_broker, "py-close-load", partitions=2)
    outcomes: list[_Outcome] = []

    async def body() -> None:
        producer = make_producer(mode, producer_configs(kafka_broker, linger_ms=20))
        futures = [await aw(producer.send(
            record=ProducerRecord(topic=topic, partition=i % 2, key=None, value=VALUE),
            callback=_recorder(outcomes))) for i in range(LOAD)]
        await aw(producer.close())
        assert all(f.is_done() for f in futures)
        assert not any(f.is_completed_exceptionally() for f in futures)
        _assert_in_send_order(outcomes, LOAD)

    run(body)
    assert _end_offsets(kafka_broker, topic) == {0: LOAD // 2, 1: LOAD // 2}


@MODE
def test_close_from_a_send_callback_does_not_deadlock(kafka_broker: Any, mode: str) -> None:
    topic = new_topic(kafka_broker, "py-close-in-callback", partitions=2)
    # Small batches, one in flight per connection: records stay in flight
    # after the first one completes.
    configs = producer_configs(kafka_broker, batch_size=512,
                               max_in_flight_requests_per_connection=1)
    outcomes: list[_Outcome] = []
    closes: list[float] = []

    async def body() -> int:
        producer = make_producer(mode, configs)

        def record(metadata: RecordMetadata, exception: Exception | None) -> bool:
            outcomes.append((metadata.partition(), metadata.offset(), exception))
            return len(outcomes) == 1

        if isinstance(producer, KafkaProducer):
            sync_producer = producer

            def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> Any:
                if record(metadata, exception):
                    started = time.monotonic()
                    sync_producer.close()  # close(0) from the completion thread
                    closes.append(time.monotonic() - started)
        else:
            async_producer: AsyncKafkaProducer[Any, Any] = producer

            async def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> Any:
                if record(metadata, exception):
                    started = time.monotonic()
                    await async_producer.close()
                    closes.append(time.monotonic() - started)

        futures = []
        for i in range(2000):
            try:
                futures.append(await aw(producer.send(record=ProducerRecord(
                    topic=topic, partition=i % 2, key=None, value=VALUE), callback=callback)))
            except (IllegalStateError, KafkaError):
                # Refused once the callback has closed the producer: no callback.
                break
        end = time.monotonic() + DEADLINE
        while len(outcomes) < len(futures):
            assert time.monotonic() < end, \
                f"{len(outcomes)} of {len(futures)} callbacks ran: deadlocked?"
            await asyncio.sleep(0.1)
        assert all(f.is_done() for f in futures)
        await aw(producer.close())  # a second close() is harmless
        return len(futures)

    taken = run(body)
    # Every record taken ran its callback once; the callback's close() did not
    # wait for the records still in flight.
    assert len(outcomes) == taken and closes and closes[0] < 10
    assert outcomes[0][2] is None
    # Records were still in flight: the forced close failed them with Java's
    # error (RecordAccumulator.abortIncompleteBatches).
    errors = [error for _, _, error in outcomes if error is not None]
    assert errors, "no record was in flight at the close"
    assert {(type(e), str(e)) for e in errors} == {(KafkaError, "Producer is closed forcefully.")}


def _interruptible() -> None:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("SIGINT reaches only the main thread")
    if signal.getsignal(signal.SIGINT) is not signal.default_int_handler:
        pytest.skip("SIGINT does not raise KeyboardInterrupt here")


def _sigint_after(delay: float) -> threading.Timer:
    timer = threading.Timer(delay, os.kill, (os.getpid(), signal.SIGINT))
    timer.start()
    return timer


def test_keyboard_interrupt_in_a_blocking_send(kafka_broker: Any) -> None:
    _interruptible()
    topic = new_topic(kafka_broker, "py-sigint-send")  # one partition
    producer: KafkaProducer[Any, Any] = KafkaProducer(
        configs=producer_configs(kafka_broker, max_block_ms=5000))
    try:
        # The topic's metadata cached: only partition 3, which the topic does
        # not have, makes send() wait.
        assert producer.send(record=ProducerRecord(
            topic=topic, partition=0, key=None, value=b"a")).get(timeout=30).offset() == 0
        timer = _sigint_after(1.0)
        started = time.monotonic()
        try:
            with pytest.raises(KeyboardInterrupt):
                producer.send(record=ProducerRecord(topic=topic, partition=3, key=None,
                                                    value=b"b"))
        finally:
            timer.cancel()
        # Raised once the send's wait ended (max.block.ms at the latest).
        assert time.monotonic() - started < 15
        # The producer stays usable.
        assert producer.send(record=ProducerRecord(
            topic=topic, partition=0, key=None, value=b"c")).get(timeout=30).offset() == 1
    finally:
        started = time.monotonic()
        producer.close(timeout=10)
        assert time.monotonic() - started < 15


def test_async_keyboard_interrupt_in_a_blocking_send(kafka_broker: Any) -> None:
    _interruptible()
    topic = new_topic(kafka_broker, "py-sigint-async-send")
    state: dict[str, Any] = {}

    async def main() -> None:
        producer: AsyncKafkaProducer[Any, Any] = AsyncKafkaProducer(
            configs=producer_configs(kafka_broker, max_block_ms=5000))
        try:
            first = await producer.send(record=ProducerRecord(
                topic=topic, partition=0, key=None, value=b"a"))
            assert (await first.get(timeout=30)).offset() == 0
            timer = _sigint_after(1.0)
            try:
                # asyncio.run's SIGINT handler cancels this task.
                await producer.send(record=ProducerRecord(topic=topic, partition=3, key=None,
                                                          value=b"b"))
            except asyncio.CancelledError:
                state["cancelled"] = True
                raise
            finally:
                timer.cancel()
        finally:
            started = time.monotonic()
            await producer.close(timeout=10)
            state["close_s"] = time.monotonic() - started

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(main())
    assert state["cancelled"] is True and state["close_s"] < 15
