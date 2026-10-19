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

"""The keyword contract of serdes and single-method callbacks (CLAUDE.md,
Python Binding Conventions, Signatures and Serialization): they are called
with keyword arguments named after Java's parameters and must accept
``**kwargs``; one without is rejected with ``IllegalArgumentError("<param>
must accept **kwargs")`` where it is given. Java has no counterpart test: its
interfaces grow by default methods instead."""

from __future__ import annotations

import asyncio
import functools
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common import TopicPartition
from confluent_kafka.common.serialization import (
    bytes_deserializer,
    bytes_serializer,
    string_deserializer,
    string_serializer,
)
from confluent_kafka.consumer import (
    AsyncKafkaConsumer,
    AsyncMockConsumer,
    KafkaConsumer,
    MockConsumer,
    OffsetAndMetadata,
)
from confluent_kafka.producer import (
    AsyncKafkaProducer,
    AsyncMockProducer,
    KafkaProducer,
    MockProducer,
    ProducerRecord,
)

PRODUCER = {"bootstrap.servers": "127.0.0.1:59999", "max.block.ms": 100}
CONSUMER = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer", "group.id": "g"}
RECORD = ProducerRecord(topic="t", key=b"k", value=b"v")


def positional_serializer(topic: str, value: Any, headers: Any = None) -> bytes | None:
    return value  # type: ignore[no-any-return]


def positional_deserializer(topic: str, data: Any, headers: Any = None) -> Any:
    return data


def positional_callback(metadata: Any, exception: Any) -> None:
    return None


def positional_commit_callback(offsets: Any, exception: Any) -> None:
    return None


class _CallableNoKwargs:
    def __call__(self, *, topic: str, value: Any, headers: Any = None) -> bytes | None:
        return None


class NoKwargsSerializer:
    """A config-route serializer without ``**kwargs``; records its close."""

    closed = 0

    def __call__(self, *, topic: str, value: Any, headers: Any = None) -> bytes | None:
        return None

    def close(self) -> None:
        NoKwargsSerializer.closed += 1


def _message(e: pytest.ExceptionInfo[IllegalArgumentError]) -> str:
    return str(e.value)


# ---- serdes -------------------------------------------------------------------
@pytest.mark.parametrize("argument", ["key_serializer", "value_serializer"])
def test_a_constructor_serializer_without_kwargs_is_rejected(argument: str) -> None:
    for serde in (positional_serializer, _CallableNoKwargs(),
                  lambda *, topic, value, headers=None: None):
        with pytest.raises(IllegalArgumentError) as e:
            KafkaProducer(configs=PRODUCER, **{argument: serde})
        assert _message(e) == f"{argument} must accept **kwargs"
        serdes: dict[str, Any] = {"key_serializer": bytes_serializer(),
                                  "value_serializer": bytes_serializer(), argument: serde}
        with pytest.raises(IllegalArgumentError) as e:
            MockProducer(auto_complete=True, partitioner=None, **serdes)
        assert _message(e) == f"{argument} must accept **kwargs"


@pytest.mark.parametrize("argument", ["key_deserializer", "value_deserializer"])
def test_a_constructor_deserializer_without_kwargs_is_rejected(argument: str) -> None:
    with pytest.raises(IllegalArgumentError) as e:
        KafkaConsumer(configs=CONSUMER, **{argument: positional_deserializer})
    assert _message(e) == f"{argument} must accept **kwargs"


def test_a_config_route_serializer_without_kwargs_is_rejected_and_closed() -> None:
    NoKwargsSerializer.closed = 0
    configs = dict(PRODUCER)
    configs["value.serializer"] = f"{__name__}.NoKwargsSerializer"
    with pytest.raises(IllegalArgumentError) as e:
        KafkaProducer(configs=configs)
    assert _message(e) == "value.serializer must accept **kwargs"
    assert NoKwargsSerializer.closed == 1


def test_the_serdes_are_called_by_keyword_with_javas_names() -> None:
    seen: list[dict[str, Any]] = []

    def key_serializer(**kwargs: Any) -> bytes | None:
        seen.append(kwargs)
        return b"k"

    with MockProducer(auto_complete=True, partitioner=None, key_serializer=key_serializer,
                      value_serializer=bytes_serializer()) as producer:
        producer.send(record=RECORD)
    assert len(seen) == 1 and set(seen[0]) == {"topic", "value", "headers"}
    assert seen[0]["topic"] == "t" and seen[0]["value"] == b"k"


def test_the_built_in_serdes_take_the_keyword_form() -> None:
    assert string_serializer()(topic="t", value="a", headers=None) == b"a"
    assert string_deserializer()(topic="t", data=memoryview(b"a"), headers=None) == "a"
    assert bytes_deserializer()(topic="t", data=memoryview(b"a"), extra=1) == b"a"
    with pytest.raises(TypeError):
        string_serializer()("t", "a")  # type: ignore[misc]


# ---- the send() callback ---------------------------------------------------------
def test_a_send_callback_without_kwargs_is_rejected() -> None:
    with MockProducer() as mock:
        with pytest.raises(IllegalArgumentError) as e:
            mock.send(record=RECORD, callback=positional_callback)  # type: ignore[call-overload]
        assert _message(e) == "callback must accept **kwargs"
        assert mock.history() == []
    with KafkaProducer(configs=PRODUCER) as producer:
        with pytest.raises(IllegalArgumentError) as e:
            producer.send(record=RECORD, callback=positional_callback)  # type: ignore[call-overload]
        assert _message(e) == "callback must accept **kwargs"


def test_a_partial_callback_with_kwargs_is_accepted_and_called_by_keyword() -> None:
    seen: list[tuple[str, Any, Any]] = []

    def on_done(tag: str, *, metadata: Any, exception: Any, **kwargs: Any) -> None:
        seen.append((tag, metadata, exception))

    with MockProducer() as mock:
        mock.send(record=RECORD, callback=functools.partial(on_done, "a"))
    assert len(seen) == 1 and seen[0][0] == "a" and seen[0][2] is None
    assert seen[0][1].topic() == "t"


def test_an_async_send_callback_without_kwargs_is_rejected() -> None:
    async def positional(metadata: Any, exception: Any) -> None:
        return None

    async def run() -> None:
        async with AsyncMockProducer() as mock:
            with pytest.raises(IllegalArgumentError) as e:
                await mock.send(record=RECORD, callback=positional)  # type: ignore[call-overload]
            assert _message(e) == "callback must accept **kwargs"
        async with AsyncKafkaProducer(configs=PRODUCER) as producer:
            with pytest.raises(IllegalArgumentError) as e:
                await producer.send(record=RECORD,  # type: ignore[call-overload]
                                    callback=positional)
            assert _message(e) == "callback must accept **kwargs"

    asyncio.run(run())


# ---- the commit_nowait() callback ------------------------------------------------
def test_a_commit_callback_without_kwargs_is_rejected() -> None:
    tp = TopicPartition(topic="t", partition=0)
    offsets = {tp: OffsetAndMetadata(offset=1)}
    mock: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy="earliest")
    mock.assign(partitions=[tp])
    with pytest.raises(IllegalArgumentError) as e:
        mock.commit_nowait(offsets=offsets,  # type: ignore[call-overload]
                           callback=positional_commit_callback)
    assert _message(e) == "callback must accept **kwargs"
    assert mock.committed(partitions={tp}) == {}
    with KafkaConsumer(configs=CONSUMER) as consumer:
        consumer.assign(partitions=[tp])
        with pytest.raises(IllegalArgumentError) as e:
            consumer.commit_nowait(callback=positional_commit_callback)  # type: ignore[call-overload]
        assert _message(e) == "callback must accept **kwargs"


def test_an_async_commit_callback_without_kwargs_is_rejected() -> None:
    async def run() -> None:
        mock: AsyncMockConsumer[bytes, bytes] = AsyncMockConsumer(
            offset_reset_strategy="earliest")
        with pytest.raises(IllegalArgumentError) as e:
            await mock.commit_nowait(callback=positional_commit_callback)  # type: ignore[call-overload]
        assert _message(e) == "callback must accept **kwargs"
        async with AsyncKafkaConsumer(configs=CONSUMER) as consumer:
            with pytest.raises(IllegalArgumentError) as e:
                await consumer.commit_nowait(  # type: ignore[call-overload]
                    callback=positional_commit_callback)
            assert _message(e) == "callback must accept **kwargs"

    asyncio.run(run())


def test_a_commit_callback_is_called_by_keyword_with_javas_names() -> None:
    tp = TopicPartition(topic="t", partition=0)
    seen: list[dict[str, Any]] = []
    mock: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy="earliest")
    mock.assign(partitions=[tp])
    mock.commit_nowait(offsets={tp: OffsetAndMetadata(offset=1)},
                       callback=lambda **kwargs: seen.append(kwargs))
    assert seen == [{"offsets": {tp: OffsetAndMetadata(offset=1)}, "exception": None}]
