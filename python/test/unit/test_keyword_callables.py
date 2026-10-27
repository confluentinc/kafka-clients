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


def positional_serializer(topic: str, data: Any, headers: Any = None) -> bytes | None:
    return data  # type: ignore[no-any-return]


def positional_deserializer(topic: str, data: Any, headers: Any = None) -> Any:
    return data


def positional_callback(metadata: Any, exception: Any) -> None:
    return None


def positional_commit_callback(offsets: Any, exception: Any) -> None:
    return None


class _CallableNoKwargs:
    def __call__(self, *, topic: str, data: Any, headers: Any = None) -> bytes | None:
        return None


class NoKwargsSerializer:
    """A config-route serializer without ``**kwargs``; records its close."""

    closed = 0
    configured = 0

    def __call__(self, *, topic: str, data: Any, headers: Any = None) -> bytes | None:
        return None

    def configure(self, configs: Any, is_key: bool) -> None:
        NoKwargsSerializer.configured += 1

    def close(self) -> None:
        NoKwargsSerializer.closed += 1


def _message(e: pytest.ExceptionInfo[IllegalArgumentError]) -> str:
    return str(e.value)


# ---- serdes -------------------------------------------------------------------
@pytest.mark.parametrize("argument", ["key_serializer", "value_serializer"])
def test_a_constructor_serializer_without_kwargs_is_rejected(argument: str) -> None:
    for serde in (positional_serializer, _CallableNoKwargs(),
                  lambda *, topic, data, headers=None: None):
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
    # Checked before configure(), as Java's
    # org.apache.kafka.common.config.AbstractConfig#getConfiguredInstance(Object,Class,Map)
    # checks the type before it configures, then closed.
    NoKwargsSerializer.closed = NoKwargsSerializer.configured = 0
    configs = dict(PRODUCER)
    configs["value.serializer"] = f"{__name__}.NoKwargsSerializer"
    with pytest.raises(IllegalArgumentError) as e:
        KafkaProducer(configs=configs)
    assert _message(e) == "value.serializer must accept **kwargs"
    assert NoKwargsSerializer.closed == 1
    assert NoKwargsSerializer.configured == 0


def test_the_serdes_are_called_by_keyword_with_javas_names() -> None:
    seen: list[dict[str, Any]] = []

    def key_serializer(**kwargs: Any) -> bytes | None:
        seen.append(kwargs)
        return b"k"

    with MockProducer(auto_complete=True, partitioner=None, key_serializer=key_serializer,
                      value_serializer=bytes_serializer()) as producer:
        producer.send(record=RECORD)
    # Java's serialize(String topic, Headers headers, T data).
    assert len(seen) == 1 and set(seen[0]) == {"topic", "data", "headers"}
    assert seen[0]["topic"] == "t" and seen[0]["data"] == b"k"


def test_the_built_in_serdes_take_the_keyword_form() -> None:
    assert string_serializer()(topic="t", data="a", headers=None) == b"a"
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


# ---- how the clients call a serde (an implementation detail, same contract) ------
def test_serde_caller_picks_the_cheapest_call_that_keeps_the_contract() -> None:
    from confluent_kafka._kwargs import DESERIALIZER_NAMES, SERIALIZER_NAMES, serde_caller

    built_in = bytes_deserializer()
    fast = serde_caller(built_in, DESERIALIZER_NAMES)
    assert getattr(fast, "__func__", None) is type(built_in)._fast  # type: ignore[attr-defined]
    assert fast("t", memoryview(b"a"), None) == b"a"

    def positional(topic: str, data: Any, headers: Any = None, **kwargs: Any) -> Any:
        return (topic, bytes(data))

    assert serde_caller(positional, DESERIALIZER_NAMES) is positional

    seen: list[dict[str, Any]] = []

    def keyword_only(*, topic: str, data: Any, headers: Any = None, **kwargs: Any) -> bytes:
        seen.append({"topic": topic, "data": data, "headers": headers, **kwargs})
        return b"x"

    def other_order(data: Any, topic: str, headers: Any = None, **kwargs: Any) -> bytes:
        seen.append({"topic": topic, "data": data})
        return b"y"

    for serde, out in ((keyword_only, b"x"), (other_order, b"y")):
        call = serde_caller(serde, SERIALIZER_NAMES)
        assert call is not serde
        assert call("t", "v", None) == out
    assert seen == [{"topic": "t", "data": "v", "headers": None}, {"topic": "t", "data": "v"}]


def test_a_keyword_only_serializer_reaches_the_real_producer_by_keyword() -> None:
    seen: list[dict[str, Any]] = []

    def serializer(*, topic: str, data: Any, headers: Any = None, **kwargs: Any) -> bytes:
        seen.append({"topic": topic, "data": data, **kwargs})
        return b"s"

    with KafkaProducer(configs=PRODUCER, value_serializer=serializer) as producer:
        producer.send(record=RECORD)
    assert seen == [{"topic": "t", "data": b"v"}]


# ---- what the fast check reads (Critic 103 items 1 and 5) -------------------------
def _no_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``inspect.signature`` fail, so a check that reaches it shows."""
    import inspect

    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("inspect.signature was called")

    monkeypatch.setattr(inspect, "signature", refuse)


def test_a_partial_is_answered_from_its_function_without_inspect_signature(
        monkeypatch: pytest.MonkeyPatch) -> None:
    from confluent_kafka._kwargs import accepts_kwargs

    def with_kwargs(tag: str, *, metadata: Any, exception: Any, **kwargs: Any) -> None:
        return None

    _no_signature(monkeypatch)
    assert accepts_kwargs(functools.partial(with_kwargs, "a"))
    assert accepts_kwargs(functools.partial(functools.partial(with_kwargs), "a"))
    assert not accepts_kwargs(functools.partial(positional_callback, None))
    seen: list[str] = []

    def on_done(tag: str, *, metadata: Any, exception: Any, **kwargs: Any) -> None:
        seen.append(tag)

    with MockProducer() as mock:
        mock.send(record=RECORD, callback=functools.partial(on_done, "sent"))
        with pytest.raises(IllegalArgumentError) as e:
            mock.send(record=RECORD,  # type: ignore[call-overload]
                      callback=functools.partial(positional_callback, None))
        assert _message(e) == "callback must accept **kwargs"
    assert seen == ["sent"]


def test_a_decorated_callable_is_judged_by_the_function_it_wraps(
        monkeypatch: pytest.MonkeyPatch) -> None:
    from confluent_kafka._kwargs import accepts_kwargs

    def logged(fn: Any) -> Any:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)
        return wrapper

    @logged
    def without(*, metadata: Any, exception: Any) -> None:
        return None

    @logged
    def with_kwargs(*, metadata: Any, exception: Any, **kwargs: Any) -> None:
        return None

    _no_signature(monkeypatch)
    assert not accepts_kwargs(without)
    assert accepts_kwargs(with_kwargs)
    with MockProducer() as mock:
        with pytest.raises(IllegalArgumentError) as e:
            mock.send(record=RECORD, callback=without)
        assert _message(e) == "callback must accept **kwargs"
        mock.send(record=RECORD, callback=with_kwargs)


def test_a_self_wrapping_callable_does_not_loop_the_check() -> None:
    # Critic 103 item 11: functools.wraps(f)(f) sets f.__wrapped__ = f; the
    # walk stops on the cycle, as inspect.unwrap does, and answers from f.
    from confluent_kafka._kwargs import accepts_kwargs

    def without(*, metadata: Any, exception: Any) -> None:
        return None

    def with_kwargs(*, metadata: Any, exception: Any, **kwargs: Any) -> None:
        return None

    def serializer(*, topic: str, data: Any, headers: Any = None) -> bytes | None:
        return None

    for fn in (without, with_kwargs, serializer):
        functools.wraps(fn)(fn)
        assert fn.__wrapped__ is fn  # type: ignore[attr-defined]
    assert not accepts_kwargs(without)
    assert accepts_kwargs(with_kwargs)
    with MockProducer() as mock:
        with pytest.raises(IllegalArgumentError) as e:
            mock.send(record=RECORD, callback=without)
        assert _message(e) == "callback must accept **kwargs"
        mock.send(record=RECORD, callback=with_kwargs)
    with pytest.raises(IllegalArgumentError) as e:
        KafkaProducer(configs=PRODUCER, value_serializer=serializer)
    assert _message(e) == "value_serializer must accept **kwargs"
