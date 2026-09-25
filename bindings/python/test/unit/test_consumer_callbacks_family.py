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

"""The ``OffsetCommitCallback`` and ``ConsumerRebalanceListener`` contracts on
the consumer family, without a broker (CLAUDE.md, Python Binding Conventions,
Threads and callbacks): the callback's payload and thread, a coroutine
callback, the offsets marshaling, and the listener's registration lifetime on
both a ``KafkaConsumer`` and a ``MockConsumer``. The broker-backed §31 cases are
in ``test/integration/test_kafka_consumer_broker.py``."""

from __future__ import annotations

import asyncio
import gc
import threading
import weakref
from typing import Any

import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.consumer import (
    AsyncMockConsumer, CloseOptions, Consumer, ConsumerRebalanceListener, ConsumerRecord,
    KafkaConsumer, MockConsumer, OffsetAndMetadata,
)

TP = TopicPartition(topic="t", partition=0)
CONFIGS = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer", "group.id": "g"}


def kafka_consumer() -> KafkaConsumer[bytes, bytes]:
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=CONFIGS)
    consumer.assign(partitions=[TP])
    return consumer


def mock_consumer() -> MockConsumer[bytes, bytes]:
    consumer: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy="earliest")
    consumer.assign(partitions=[TP])
    consumer.update_beginning_offsets(new_offsets={TP: 0})
    return consumer


def close(consumer: Consumer[Any, Any]) -> None:
    consumer.close(option=CloseOptions.timeout(0))


# ---------------------------------------------------------------------------
# The commit callback
# ---------------------------------------------------------------------------
def test_mock_callback_receives_the_consumed_positions() -> None:
    consumer = mock_consumer()
    consumer.add_record(record=ConsumerRecord(topic="t", partition=0, offset=0, key=b"k",
                                              value=b"v"))
    consumer.poll(timeout=0)
    seen: list[Any] = []
    consumer.commit_nowait(callback=lambda offsets, exception: seen.append((offsets, exception)))
    assert seen == [({TP: OffsetAndMetadata(offset=1, leader_epoch=None, metadata="")}, None)]
    close(consumer)


def test_mock_callback_receives_the_explicit_offsets() -> None:
    consumer = mock_consumer()
    seen: list[Any] = []
    offsets = {TP: OffsetAndMetadata(offset=3, metadata="m")}
    consumer.commit_nowait(offsets=offsets,
                           callback=lambda o, exception: seen.append((o, exception)))
    assert seen == [(offsets, None)]
    assert seen[0][0][TP].metadata() == "m"
    close(consumer)


def test_kafka_consumer_callback_receives_offsets_and_no_error() -> None:
    consumer = kafka_consumer()
    seen: list[Any] = []
    consumer.commit_nowait(offsets={}, callback=lambda o, e: seen.append((o, e)))
    consumer.commit(offsets={})
    assert seen == [({}, None)]
    close(consumer)


@pytest.mark.parametrize("make", [kafka_consumer, mock_consumer])
def test_a_coroutine_callback_is_closed_and_reported(make: Any,
                                                     caplog: pytest.LogCaptureFixture) -> None:
    consumer = make()
    closed = threading.Event()

    async def callback(offsets: Any, exception: Any) -> None:
        try:
            await asyncio.sleep(0)
        finally:
            closed.set()

    if isinstance(consumer, KafkaConsumer):
        consumer.commit_nowait(offsets={}, callback=callback)  # type: ignore[arg-type]
        with caplog.at_level("ERROR", logger="confluent_kafka.consumer"):
            consumer.commit(offsets={})
        assert any("returned an awaitable" in r.getMessage() for r in caplog.records)
    else:
        # The mock calls the callback inline and lets its failure propagate.
        with pytest.raises(TypeError) as e:
            consumer.commit_nowait(callback=callback)  # type: ignore[arg-type]
        assert str(e.value) == "a coroutine OffsetCommitCallback requires an AsyncMockConsumer"
    close(consumer)


@pytest.mark.parametrize("make", [kafka_consumer])
@pytest.mark.parametrize("entry_point", ["commit", "commit_nowait"])
def test_non_str_offset_metadata_is_rejected(make: Any, entry_point: str) -> None:
    consumer = make()
    offsets = {TP: OffsetAndMetadata(offset=5, metadata=123)}  # type: ignore[arg-type]
    kwargs: dict[str, Any] = {"offsets": offsets}
    if entry_point == "commit_nowait":
        kwargs["callback"] = lambda o, e: None
    with pytest.raises(TypeError) as e:
        getattr(consumer, entry_point)(**kwargs)
    assert str(e.value) == "offset metadata must be str or None, not int"
    close(consumer)


def test_async_mock_callback_runs_inline_on_the_loop() -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[bytes, bytes] = AsyncMockConsumer(
            offset_reset_strategy="earliest")
        await consumer.assign(partitions=[TP])
        seen: list[Any] = []
        consumer.commit_nowait(offsets={TP: OffsetAndMetadata(offset=3, metadata="m")},
                               callback=lambda o, e: seen.append((o, e, threading.get_ident())))
        assert seen == [({TP: OffsetAndMetadata(offset=3, metadata="m")}, None,
                         threading.get_ident())]
        await consumer.close()

    asyncio.run(main())


# ---------------------------------------------------------------------------
# The listener's registration lifetime (retained while subscribed and across
# unsubscribe; released by a replacing or listener-less subscribe, and by close)
# ---------------------------------------------------------------------------
def listener() -> ConsumerRebalanceListener:
    return ConsumerRebalanceListener()


@pytest.mark.parametrize("make", [kafka_consumer, mock_consumer])
def test_listener_retained_while_subscribed(make: Any) -> None:
    consumer = make()
    consumer.unsubscribe()
    held = listener()
    ref = weakref.ref(held)
    consumer.subscribe(topics=["t"], callback=held)
    del held
    gc.collect()
    assert ref() is not None
    consumer.unsubscribe()
    gc.collect()
    assert ref() is not None
    close(consumer)
    gc.collect()
    assert ref() is None


@pytest.mark.parametrize("make", [kafka_consumer, mock_consumer])
def test_listener_released_by_a_listenerless_or_replacing_subscribe(make: Any) -> None:
    consumer = make()
    consumer.unsubscribe()
    first = listener()
    ref = weakref.ref(first)
    consumer.subscribe(topics=["t"], callback=first)
    del first
    consumer.subscribe(topics=["t"], callback=listener())
    gc.collect()
    assert ref() is None
    second = listener()
    ref = weakref.ref(second)
    consumer.subscribe(topics=["t"], callback=second)
    del second
    consumer.subscribe(topics=["t"])
    gc.collect()
    assert ref() is None
    close(consumer)
