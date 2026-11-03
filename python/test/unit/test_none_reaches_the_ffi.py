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

"""A ``None`` for a mandatory argument reaches the FFI as NULL, and the core
answers with Java's error and message (CLAUDE.md §4: a NULL mandatory argument
is a precondition panic carrying Java's error; Python Binding Conventions,
Types: ``LOCAL_NULL_POINTER`` is a ``TypeError``, its "null" spelled "None").

Each test wraps the extension's entry point with a recorder, so it proves the
binding made no check of its own: the wrapper saw the ``None``, and the error
the call raised is the one the core returned."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import _confluentkafka  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka.consumer import KafkaConsumer
from confluent_kafka.producer import KafkaProducer

CONSUMER_CONFIGS = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer", "group.id": "g"}
PRODUCER_CONFIGS = {"bootstrap.servers": "localhost:1", "max.block.ms": 200}


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str], list[tuple[Any, ...]]]]:
    """Replaces an extension entry point by one recording its arguments and
    calling the original; returns the list the calls are recorded in."""
    def wrap(name: str) -> list[tuple[Any, ...]]:
        calls: list[tuple[Any, ...]] = []
        original = getattr(_confluentkafka, name)

        def recording(*args: Any) -> Any:
            calls.append(args)
            return original(*args)
        monkeypatch.setattr(_confluentkafka, name, recording)
        return calls
    yield wrap


def test_a_null_pointer_error_from_a_cb_twin(recorder: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # AsyncKafkaConsumer#pause's Objects.requireNonNull, through Consumer_pause_cb.
    calls = recorder("Consumer_pause_cb")
    with KafkaConsumer(configs=CONSUMER_CONFIGS) as consumer:
        with pytest.raises(TypeError) as e:
            consumer.pause(partitions=None)  # type: ignore[arg-type]
    assert type(e.value) is TypeError
    assert str(e.value) == "The partitions to pause must be nonnull"
    assert len(calls) == 1 and calls[0][1] is None


def test_an_illegal_argument_from_the_core(recorder: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # AsyncKafkaConsumer#subscribeInternal's IllegalArgumentException keeps Java's "null".
    calls = recorder("Consumer_subscribe_cb")
    with KafkaConsumer(configs=CONSUMER_CONFIGS) as consumer:
        with pytest.raises(IllegalArgumentError) as e:
            consumer.subscribe(topics=None)  # type: ignore[call-overload]
    assert str(e.value) == "Topic collection to subscribe to cannot be null"
    assert len(calls) == 1 and calls[0][1] is None


def test_send_of_a_null_record_raises_from_the_call(recorder: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # KafkaProducer#doSend dereferences the null record: a NullPointerException
    # the call rethrows, never handed to the callback.
    calls = recorder("Producer_send_cb")
    fired: list[Any] = []
    with KafkaProducer(configs=PRODUCER_CONFIGS) as producer:
        with pytest.raises(TypeError) as e:
            producer.send(record=None, callback=lambda **kw: fired.append(kw))  # type: ignore[arg-type]
    assert str(e.value) == "record must not be None"
    assert len(calls) == 1 and calls[0][1] is None
    assert fired == []
