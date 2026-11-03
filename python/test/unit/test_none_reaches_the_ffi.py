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
from confluent_kafka.admin import Admin, NewTopic
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


ADMIN_CONFIGS = {"bootstrap.servers": "localhost:1"}


def test_admin_a_null_collection_reaches_the_ffi(recorder: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # KafkaAdminClient#createTopics dereferences the null collection (newTopics.size()).
    calls = recorder("Admin_create_topics")
    with Admin.create(configs=ADMIN_CONFIGS) as admin:
        with pytest.raises(TypeError) as e:
            admin.create_topics(new_topics=None)  # type: ignore[call-overload]
    assert type(e.value) is TypeError
    assert str(e.value) == "new_topics must not be None"
    assert len(calls) == 1 and calls[0][1] is None


def test_admin_null_options_reach_the_ffi(recorder: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # KafkaAdminClient#createTopics reads options.timeoutMs() of the null options.
    calls = recorder("Admin_create_topics")
    with Admin.create(configs=ADMIN_CONFIGS) as admin:
        with pytest.raises(TypeError) as e:
            admin.create_topics(new_topics=[NewTopic(name="t", num_partitions=1, replication_factor=1)],
                                options=None)  # type: ignore[call-overload]
    assert str(e.value) == "options must not be None"
    assert len(calls) == 1 and calls[0][-1] is None


def test_admin_a_null_string_reaches_the_ffi(recorder: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # KafkaAdminClient#alterConsumerGroupOffsets: the null group id is the C API's to report.
    calls = recorder("Admin_alter_consumer_group_offsets")
    with Admin.create(configs=ADMIN_CONFIGS) as admin:
        with pytest.raises(TypeError) as e:
            admin.alter_consumer_group_offsets(group_id=None, offsets={})  # type: ignore[call-overload]
    assert str(e.value) == "group_id must not be None"
    assert len(calls) == 1 and calls[0][1] is None


def test_admin_a_null_enum_reaches_the_ffi(recorder: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # KafkaAdminClient#electLeaders: the null election type is the C API's to report.
    calls = recorder("Admin_elect_leaders")
    with Admin.create(configs=ADMIN_CONFIGS) as admin:
        with pytest.raises(TypeError) as e:
            admin.elect_leaders(election_type=None, partitions=None)  # type: ignore[call-overload]
    assert str(e.value) == "election_type must not be None"
    assert len(calls) == 1 and calls[0][1] is None


def test_admin_null_options_come_before_a_later_null(
        recorder: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # KafkaAdminClient#forceTerminateTransaction reads options before the null id,
    # which the C API checks first: Python keeps Java's order.
    calls = recorder("Admin_force_terminate_transaction")
    with Admin.create(configs=ADMIN_CONFIGS) as admin:
        with pytest.raises(TypeError) as e:
            admin.force_terminate_transaction(transactional_id=None, options=None)  # type: ignore[call-overload]
    assert str(e.value) == "options must not be None"
    assert calls == []
