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

"""The asyncio clients against a broker, where the event loop is the subject
(CLAUDE.md, Python Binding Conventions, Threads and callbacks):

- a ``poll()`` cancelled after the core answered it with records leaves the
  position unchanged: the records are sought back and returned by the next
  polls, none lost and none twice (the sync form: a Ctrl+C while ``poll()``
  resolves its records);
- an ``async def`` ``ConsumerRebalanceListener`` is awaited on the loop across
  a rebalance, the loop's other tasks running meanwhile;
- an event loop ending with a ``poll()`` still pending does not hang, leaves
  no "Task was destroyed but it is pending!" behind, and the consumer then
  closes on another loop;
- backpressure: concurrent awaited sends through a small ``buffer.memory``
  complete, each partition's records in send order (the sync form: concurrent
  sending threads).

Skips without Docker (``kafka_broker``).
"""

from __future__ import annotations

import asyncio
import gc
import logging
import threading
import time
import warnings
from typing import Any

import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.common.serialization import string_deserializer, string_serializer
from confluent_kafka.consumer import (
    AsyncKafkaConsumer, CloseOptions, ConsumerRebalanceListener, KafkaConsumer,
)
from confluent_kafka.consumer._base import _ConsumerState
from confluent_kafka.producer import ProducerRecord

from .broker_helpers import (
    DEADLINE, MODE, aw, close_consumer, consumer_configs, make_producer, new_topic, produce,
    producer_configs, read_all, run,
)

STR_IN: dict[str, Any] = {"value_deserializer": string_deserializer()}

#: The records the rewind tests fetch.
REWIND_COUNT = 8


def _spy_rewinds(monkeypatch: pytest.MonkeyPatch) -> list[list[tuple[TopicPartition, int]]]:
    """The non-empty position lists the consumer seeks back to
    (``_rewind_sync`` / ``_rewind_async``): proof that the abandoned poll had
    records."""
    rewinds: list[list[tuple[TopicPartition, int]]] = []
    real_sync, real_async = _ConsumerState._rewind_sync, _ConsumerState._rewind_async

    def rewind_sync(self: Any, positions: list[tuple[TopicPartition, int]]) -> Any:
        if positions:
            rewinds.append(list(positions))
        return real_sync(self, positions)

    async def rewind_async(self: Any, positions: list[tuple[TopicPartition, int]]) -> Any:
        if positions:
            rewinds.append(list(positions))
        return await real_async(self, positions)

    monkeypatch.setattr(_ConsumerState, "_rewind_sync", rewind_sync)
    monkeypatch.setattr(_ConsumerState, "_rewind_async", rewind_async)
    return rewinds


def test_async_cancelled_poll_rewinds(kafka_broker: Any,
                                      monkeypatch: pytest.MonkeyPatch) -> None:
    """A poll cancelled after the core answered it with records, before the
    loop delivered the answer: the records are sought back, and the next
    polls return each of them once."""
    topic = new_topic(kafka_broker, "py-cancel-poll")
    expected = [f"v{i}" for i in range(REWIND_COUNT)]
    tp = TopicPartition(topic=topic, partition=0)
    rewinds = _spy_rewinds(monkeypatch)

    async def main() -> list[str]:
        consumer: AsyncKafkaConsumer[bytes, str] = AsyncKafkaConsumer(
            configs=consumer_configs(kafka_broker), **STR_IN)
        try:
            await consumer.assign(partitions=[tp])
            polling = asyncio.ensure_future(consumer.poll(timeout=10))
            await asyncio.sleep(0.5)  # the poll waits on the empty partition
            # The notify hook sets the event from a core task once the poll's
            # answer, with the records, is queued; a fixed sleep could cancel
            # before it, leaving nothing to rewind.
            consumer._pending_event.clear()
            produce(kafka_broker, topic, expected, value_serializer=string_serializer())
            # The loop blocked: the core's answer stays queued for the loop
            # while the task is cancelled.
            assert consumer._pending_event.wait(DEADLINE), "the poll got no answer"
            polling.cancel()
            with pytest.raises(asyncio.CancelledError):
                await polling
            assert rewinds == [[(tp, 0)]]
            assert await consumer.position(partition=tp, timeout=10) == 0
            returned: list[str] = []
            end = time.monotonic() + DEADLINE
            while len(returned) < REWIND_COUNT:
                assert time.monotonic() < end, f"returned {returned}"
                returned.extend(str(r.value()) for r in await consumer.poll(timeout=0.5))
            # None twice: nothing more arrives.
            assert (await consumer.poll(timeout=1)).is_empty()
            return returned
        finally:
            await consumer.close(option=CloseOptions(timeout=0))

    assert asyncio.run(main()) == expected


def test_keyboard_interrupt_in_poll_rewinds(kafka_broker: Any,
                                            monkeypatch: pytest.MonkeyPatch) -> None:
    """The sync form of the test above: a Ctrl+C landing while ``poll()``
    resolves its records (a deserializer raising ``KeyboardInterrupt`` on the
    fourth record, as Ctrl+C would there) re-raises ``KeyboardInterrupt``, the
    records are sought back, and the next polls return each of them once."""
    topic = new_topic(kafka_broker, "py-sigint-poll")
    expected = [f"v{i}" for i in range(REWIND_COUNT)]
    produce(kafka_broker, topic, expected, value_serializer=string_serializer())
    tp = TopicPartition(topic=topic, partition=0)
    rewinds = _spy_rewinds(monkeypatch)
    interrupted: list[bool] = []

    def value_deserializer(*, topic: str, data: memoryview | None, headers: Any = None, **kwargs: Any) -> str:
        assert data is not None
        text = bytes(data).decode()
        if text == "v3" and not interrupted:
            interrupted.append(True)
            raise KeyboardInterrupt
        return text

    consumer: KafkaConsumer[bytes, str] = KafkaConsumer(
        configs=consumer_configs(kafka_broker), value_deserializer=value_deserializer)
    try:
        consumer.assign(partitions=[tp])
        returned: list[str] = []
        end = time.monotonic() + DEADLINE
        while not interrupted:
            assert time.monotonic() < end, "no record reached the deserializer"
            try:
                returned.extend(str(r.value()) for r in consumer.poll(timeout=0.5))
            except KeyboardInterrupt:
                assert interrupted
        assert returned == [] and rewinds == [[(tp, 0)]]
        assert consumer.position(partition=tp, timeout=10) == 0
        end = time.monotonic() + DEADLINE
        while len(returned) < REWIND_COUNT:
            assert time.monotonic() < end, f"returned {returned}"
            returned.extend(str(r.value()) for r in consumer.poll(timeout=0.5))
        assert consumer.poll(timeout=1).is_empty()
        assert returned == expected
    finally:
        consumer.close(option=CloseOptions(timeout=0))


def test_async_def_listener_is_awaited_across_a_rebalance(kafka_broker: Any) -> None:
    topic = new_topic(kafka_broker, "py-async-listener")
    produce(kafka_broker, topic, ["a"], value_serializer=string_serializer())
    tp = TopicPartition(topic=topic, partition=0)
    events: list[tuple[str, frozenset[TopicPartition], int]] = []

    async def main() -> None:
        ticks = 0
        stop = asyncio.Event()

        async def ticker() -> None:
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0.05)

        class Awaiting(ConsumerRebalanceListener):
            async def on_partitions_assigned(  # type: ignore[override]
                    self, partitions: set[TopicPartition]) -> None:
                before = ticks
                await asyncio.sleep(1.0)
                # The loop ran the ticker while this listener waited.
                events.append(("assigned", frozenset(partitions), ticks - before))

            async def on_partitions_revoked(  # type: ignore[override]
                    self, partitions: set[TopicPartition]) -> None:
                before = ticks
                await asyncio.sleep(1.0)
                events.append(("revoked", frozenset(partitions), ticks - before))

        consumer: AsyncKafkaConsumer[bytes, str] = AsyncKafkaConsumer(
            configs=consumer_configs(kafka_broker), **STR_IN)
        ticking = asyncio.ensure_future(ticker())
        try:
            await consumer.subscribe(topics=[topic], callback=Awaiting())
            values: list[str | None] = []
            end = time.monotonic() + DEADLINE
            while not values:
                assert time.monotonic() < end
                values.extend(r.value() for r in await consumer.poll(timeout=0.5))
            # The poll returned the record after the listener had returned.
            assert values == ["a"]
            assert [(e, p) for e, p, _ in events if p] == [("assigned", frozenset({tp}))]
            await consumer.unsubscribe()
            # unsubscribe() returned after the awaited revocation.
            assert [(e, p) for e, p, _ in events if p] == [
                ("assigned", frozenset({tp})), ("revoked", frozenset({tp}))]
            assert all(t >= 5 for e, p, t in events if p)
        finally:
            stop.set()
            await ticking
            await consumer.close(option=CloseOptions(timeout=0))

    asyncio.run(main())


def test_async_loop_ending_with_a_poll_pending(kafka_broker: Any,
                                               caplog: pytest.LogCaptureFixture) -> None:
    topic = new_topic(kafka_broker, "py-loop-end")
    tp = TopicPartition(topic=topic, partition=0)
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(
        configs=consumer_configs(kafka_broker))
    pending: list[asyncio.Future[Any]] = []

    async def start() -> None:
        await consumer.assign(partitions=[tp])
        # A poll left waiting when the main coroutine returns: asyncio.run
        # cancels it before closing the loop.
        pending.append(asyncio.ensure_future(consumer.poll(timeout=30)))
        await asyncio.sleep(0.5)

    caplog.set_level(logging.WARNING)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        started = time.monotonic()
        asyncio.run(start())
        assert time.monotonic() - started < 10, "the loop's shutdown waited for the poll"
        (poll,) = pending
        assert poll.cancelled()
        del poll, pending[:]
        gc.collect()
        # The consumer is bound to the loop that ended (_async.LoopBinding);
        # close() alone may run on another loop once that loop is closed, and
        # the poll the ended loop left does not hold it.
        started = time.monotonic()
        asyncio.run(consumer.close(option=CloseOptions(timeout=5)))
        assert time.monotonic() - started < 10
        gc.collect()
    messages = [r.getMessage() for r in caplog.records] + [str(w.message) for w in caught]
    assert not [m for m in messages
                if "Task was destroyed" in m or "was never awaited" in m], messages


#: Records per partition of the backpressure tests, and a value of 1 KiB: 2 MiB
#: through a 64 KiB buffer.
PRESSURE_COUNT = 1000
PRESSURE_VALUE = "x" * 1024


async def _assert_partition_order(broker: Any, topic: str,
                                  offsets: dict[int, list[tuple[int, int]]]) -> None:
    """Each partition's records acknowledged and stored in send order."""
    for partition in (0, 1):
        sent = offsets[partition]
        assert [seq for seq, _ in sent] == list(range(PRESSURE_COUNT))
        assert [offset for _, offset in sent] == list(range(PRESSURE_COUNT))
    stored = read_all(broker, topic, 2, 2 * PRESSURE_COUNT, **STR_IN)
    for partition in (0, 1):
        seqs = [int(str(r.value()).split(":")[0]) for r in stored if r.partition() == partition]
        assert seqs == list(range(PRESSURE_COUNT))


@MODE
def test_backpressure_keeps_each_partitions_order(kafka_broker: Any, mode: str) -> None:
    topic = new_topic(kafka_broker, "py-backpressure", partitions=2)
    configs = producer_configs(kafka_broker, buffer_memory=65536, batch_size=16384,
                               linger_ms=5, max_block_ms=60000)

    async def body() -> None:
        producer = make_producer(mode, configs, value_serializer=string_serializer())
        offsets: dict[int, list[tuple[int, int]]] = {0: [], 1: []}

        async def send_all(partition: int) -> None:
            futures = [await aw(producer.send(record=ProducerRecord(
                topic=topic, partition=partition, key=None,
                value=f"{seq}:{PRESSURE_VALUE}"))) for seq in range(PRESSURE_COUNT)]
            for seq, future in enumerate(futures):
                offsets[partition].append((seq, (await aw(future.get(timeout=60))).offset()))

        try:
            if mode == "async":
                # Two tasks sending at once, each awaiting its sends in turn.
                await asyncio.wait_for(asyncio.gather(send_all(0), send_all(1)), DEADLINE)
            else:
                threads = [threading.Thread(target=lambda p=p: asyncio.run(send_all(p)))
                           for p in (0, 1)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(DEADLINE)
                assert not any(t.is_alive() for t in threads)
        finally:
            await aw(producer.close())
        await _assert_partition_order(kafka_broker, topic, offsets)

    run(body)
