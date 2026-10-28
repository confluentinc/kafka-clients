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

"""The asyncio forms of the callback interfaces the async classes take
(CLAUDE.md, Python Binding Conventions, Class family, rule 3):
``AsyncCallback`` (``confluent_kafka.producer``), ``AsyncOffsetCommitCallback``
and ``AsyncConsumerRebalanceListener`` (``confluent_kafka.consumer``), and the
plain or ``async def`` task ``AsyncMockConsumer.schedule_poll_task`` takes.

Java has no asyncio forms, so there is no Java test to translate; these tests
follow the Java forms' own tests where they apply (the listener's default
``onPartitionsLost`` calling ``onPartitionsRevoked``)."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any

import pytest

from confluent_kafka import ConcurrentModificationError
from confluent_kafka.common import TopicPartition
from confluent_kafka.common.errors import WakeupError
from confluent_kafka.consumer import (
    AsyncConsumerRebalanceListener, AsyncKafkaConsumer, AsyncMockConsumer,
    AsyncOffsetCommitCallback, ConsumerRebalanceListener, MockConsumer, OffsetAndMetadata,
)
from confluent_kafka.producer import (
    AsyncCallback, AsyncMockProducer, Callback, ProducerRecord, RecordMetadata,
)

CONCURRENT = "KafkaConsumer is not safe for multi-threaded access."
TP0 = TopicPartition(topic="t", partition=0)
TP1 = TopicPartition(topic="t", partition=1)


class _Recording(AsyncConsumerRebalanceListener):
    def __init__(self) -> None:
        self.events: list[tuple[str, set[TopicPartition]]] = []

    async def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        await asyncio.sleep(0)
        self.events.append(("revoked", set(partitions)))

    async def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        await asyncio.sleep(0)
        self.events.append(("assigned", set(partitions)))


# --------------------------------------------------------------------------- #
# The public names
# --------------------------------------------------------------------------- #


def test_the_asyncio_forms_live_beside_the_java_forms() -> None:
    import confluent_kafka.consumer as consumer
    import confluent_kafka.producer as producer

    assert AsyncCallback.__module__ == "confluent_kafka.producer.async_callback"
    assert "AsyncCallback" in producer.__all__ and "Callback" in producer.__all__
    for name in ("AsyncOffsetCommitCallback", "AsyncConsumerRebalanceListener"):
        assert name in consumer.__all__
    assert inspect.iscoroutinefunction(AsyncCallback.__call__)
    assert inspect.iscoroutinefunction(AsyncOffsetCommitCallback.__call__)
    assert not inspect.iscoroutinefunction(Callback.__call__)


def test_the_async_listener_is_an_abc_with_javas_abstract_methods() -> None:
    assert AsyncConsumerRebalanceListener.__abstractmethods__ == frozenset(
        {"on_partitions_revoked", "on_partitions_assigned"})
    assert not issubclass(AsyncConsumerRebalanceListener, ConsumerRebalanceListener)
    for name in ("on_partitions_revoked", "on_partitions_assigned", "on_partitions_lost"):
        assert inspect.iscoroutinefunction(getattr(AsyncConsumerRebalanceListener, name))

    class OnlyRevoked(AsyncConsumerRebalanceListener):
        async def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
            pass

    with pytest.raises(TypeError) as e:
        OnlyRevoked()  # type: ignore[abstract]
    assert "on_partitions_assigned" in str(e.value)


def test_the_async_listeners_on_partitions_lost_awaits_on_partitions_revoked() -> None:
    # Java's default onPartitionsLost calls onPartitionsRevoked.
    listener = _Recording()
    asyncio.run(listener.on_partitions_lost({TP0}))
    assert listener.events == [("revoked", {TP0})]


# --------------------------------------------------------------------------- #
# AsyncMockConsumer takes the asyncio listener and awaits it
# --------------------------------------------------------------------------- #


def test_async_mock_rebalance_and_lose_partitions_await_the_async_listener() -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(
            offset_reset_strategy="earliest")
        listener = _Recording()
        await consumer.subscribe(topics=["t"], callback=listener)
        await consumer.rebalance(new_assignment=[TP0, TP1])
        await consumer.rebalance(new_assignment=[TP1])
        await consumer.lose_partitions(partitions_lost=[TP1])
        assert listener.events == [
            ("assigned", {TP0, TP1}), ("revoked", {TP0}), ("assigned", set()),
            # The default on_partitions_lost awaits on_partitions_revoked.
            ("revoked", {TP1})]
        assert consumer.assignment() == set()

    asyncio.run(main())


def test_the_sync_mock_rejects_the_async_listeners_coroutines() -> None:
    consumer: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    consumer.subscribe(topics=["t"], callback=_Recording())  # type: ignore[arg-type]
    with pytest.raises(TypeError) as e:
        consumer.rebalance(new_assignment=[TP0])
    assert str(e.value) == "a coroutine rebalance listener requires an AsyncMockConsumer"


def test_async_kafka_consumer_subscribe_takes_the_async_listener() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(
            configs={"bootstrap.servers": "localhost:1", "group.protocol": "consumer",
                     "group.id": "g"})
        try:
            await consumer.subscribe(topics=["t"], callback=_Recording())
            assert consumer.subscription() == {"t"}
        finally:
            await consumer.close(option=_zero_close())

    asyncio.run(main())


def _zero_close() -> Any:
    from confluent_kafka.consumer import CloseOptions

    return CloseOptions.timeout(timeout=0)


# --------------------------------------------------------------------------- #
# AsyncOffsetCommitCallback
# --------------------------------------------------------------------------- #


def test_async_mock_commit_nowait_awaits_an_async_offset_commit_callback() -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(
            offset_reset_strategy="earliest")
        await consumer.assign(partitions=[TP0])
        seen: list[dict[TopicPartition, OffsetAndMetadata] | None] = []

        async def on_complete(*, offsets: dict[TopicPartition, OffsetAndMetadata] | None,
                              exception: Exception | None, **kwargs: Any) -> None:
            await asyncio.sleep(0)
            seen.append(offsets)

        callback: AsyncOffsetCommitCallback = on_complete
        await consumer.commit_nowait(offsets={TP0: OffsetAndMetadata(offset=3)},
                                     callback=callback)
        assert seen == [{TP0: OffsetAndMetadata(offset=3)}]

    asyncio.run(main())


# --------------------------------------------------------------------------- #
# AsyncCallback
# --------------------------------------------------------------------------- #


def test_async_mock_producer_awaits_an_async_callback() -> None:
    async def main() -> None:
        producer: AsyncMockProducer[bytes, bytes] = AsyncMockProducer(
            auto_complete=True, partitioner=None, key_serializer=None, value_serializer=None)
        seen: list[int] = []

        async def on_done(*, metadata: RecordMetadata, exception: Exception | None,
                          **kwargs: Any) -> None:
            await asyncio.sleep(0)
            seen.append(metadata.offset())

        callback: AsyncCallback = on_done
        future = await producer.send(record=ProducerRecord(topic="t", partition=0, key=None,
                                                           value=b"v"), callback=callback)
        await producer.flush()
        assert seen == [0]
        assert (await future.get()).offset() == 0

    asyncio.run(main())


# --------------------------------------------------------------------------- #
# AsyncMockConsumer.schedule_poll_task: a plain or an async def task
# --------------------------------------------------------------------------- #


def test_async_mock_poll_awaits_an_async_poll_task() -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(
            offset_reset_strategy="earliest")
        ran: list[str] = []

        async def task() -> None:
            await asyncio.sleep(0)
            # The task's own calls into the consumer pass.
            consumer.schedule_nop_poll_task()
            ran.append("async")

        consumer.schedule_poll_task(task=task)
        consumer.schedule_poll_task(task=lambda: ran.append("plain"))
        await consumer.assign(partitions=[TP0])
        consumer.update_beginning_offsets(new_offsets={TP0: 0})
        assert (await consumer.poll(timeout=0)).is_empty()
        assert ran == ["async"]
        assert (await consumer.poll(timeout=0)).is_empty()
        assert ran == ["async", "plain"]
        # The nop task the async one scheduled runs on the next poll.
        await consumer.poll(timeout=0)
        assert not consumer._poll_tasks

    asyncio.run(main())


def test_another_task_is_refused_while_the_poll_awaits_its_task() -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(
            offset_reset_strategy="earliest")
        entered = asyncio.Event()
        release = asyncio.Event()

        async def task() -> None:
            entered.set()
            await release.wait()

        consumer.schedule_poll_task(task=task)
        await consumer.assign(partitions=[TP0])
        consumer.update_beginning_offsets(new_offsets={TP0: 0})
        polling = asyncio.ensure_future(consumer.poll(timeout=0))
        await entered.wait()
        with pytest.raises(ConcurrentModificationError) as e:
            consumer.assignment()
        assert str(e.value) == CONCURRENT
        # A call from another thread is refused the same way.
        with pytest.raises(ConcurrentModificationError) as e:
            await asyncio.to_thread(consumer.closed)
        assert str(e.value) == CONCURRENT
        release.set()
        assert (await polling).is_empty()
        assert consumer.assignment() == {TP0}

    asyncio.run(main())


def test_wakeup_passes_while_the_poll_awaits_its_task() -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(
            offset_reset_strategy="earliest")
        entered = asyncio.Event()
        release = asyncio.Event()

        async def task() -> None:
            entered.set()
            await release.wait()

        consumer.schedule_poll_task(task=task)
        await consumer.assign(partitions=[TP0])
        consumer.update_beginning_offsets(new_offsets={TP0: 0})
        polling = asyncio.ensure_future(consumer.poll(timeout=0))
        await entered.wait()
        consumer.wakeup()  # passes, as the deviation says
        release.set()
        with pytest.raises(WakeupError):
            await polling
        assert (await consumer.poll(timeout=0)).is_empty()

    asyncio.run(main())


def test_the_sync_mock_rejects_an_async_poll_task() -> None:
    consumer: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")

    async def task() -> None:
        pass

    consumer.schedule_poll_task(task=task)  # type: ignore[arg-type]
    consumer.assign(partitions=[TP0])
    with pytest.raises(TypeError) as e:
        consumer.poll(timeout=0)
    assert str(e.value) == "a coroutine poll task requires an AsyncMockConsumer"
