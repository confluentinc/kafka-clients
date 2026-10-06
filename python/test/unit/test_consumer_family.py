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

"""The consumer family's surface, against the Python Binding Conventions
(CLAUDE.md): the non-instantiable bases, Java's member order, the
keyword-only parameters (a positional call is a ``TypeError``), the
``java_forms`` checks with their exact messages, every stub form, the
``@Deprecated`` warnings, the async-ness of each method, and what is not
generated or dropped. Java's ``MockConsumerTest`` is in
``test_mock_consumer.py``, the broker-less ``KafkaConsumerTest`` cases in
``test_kafka_consumer.py``."""

from __future__ import annotations

import asyncio
import inspect
import threading
from datetime import timedelta
from typing import Any

import pytest

import confluent_kafka.consumer as consumer_module
from confluent_kafka import IllegalArgumentError
from confluent_kafka.common import TopicPartition
from confluent_kafka.consumer import (
    AsyncConsumer, AsyncKafkaConsumer, AsyncMockConsumer, CloseOptions, Consumer,
    ConsumerRebalanceListener, KafkaConsumer, MockConsumer, OffsetAndMetadata,
    SubscriptionPattern,
)
from confluent_kafka.consumer._mock_core import MockConsumerCore

TP = TopicPartition(topic="t", partition=0)
CONFIGS = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer", "group.id": "g"}

# Java's Consumer interface, in declaration order, as the binding names it
# (commitSync -> commit, commitAsync -> commit_nowait; enforceRebalance dropped;
# clientInstanceId and the metric subscription methods not generated).
CONSUMER_METHODS = [
    "assignment", "subscription", "subscribe", "assign", "unsubscribe", "poll", "commit",
    "commit_nowait", "seek", "seek_to_beginning", "seek_to_end", "position", "committed",
    "metrics", "partitions_for", "list_topics", "paused", "pause", "resume",
    "offsets_for_times", "beginning_offsets", "end_offsets", "current_lag", "group_metadata",
    "close", "__enter__", "__exit__", "wakeup",
]
ASYNC_METHODS = list(CONSUMER_METHODS)
ASYNC_METHODS[ASYNC_METHODS.index("__enter__")] = "__aenter__"
ASYNC_METHODS[ASYNC_METHODS.index("__exit__")] = "__aexit__"

# Java's MockConsumer's own public methods, in declaration order, less the
# dropped ones (shouldRebalance / resetShouldRebalance, and the telemetry-only
# setClientInstanceId / injectTimeoutException / disableTelemetry /
# addedMetrics).
MOCK_METHODS = [
    "rebalance", "add_record", "set_max_poll_records", "set_poll_exception",
    "set_offsets_exception", "update_beginning_offsets", "update_end_offsets",
    "update_duration_offsets", "update_partitions", "closed", "schedule_poll_task",
    "schedule_nop_poll_task", "last_poll_timeout",
]

# The methods whose entry point has an _async completion form in the FFI header
# (Class family): not current_lag, although Java's waits (addAndGet).
WAITING = {"subscribe", "assign", "unsubscribe", "poll", "commit", "seek", "seek_to_beginning",
           "seek_to_end", "position", "committed", "partitions_for", "list_topics", "pause",
           "resume", "offsets_for_times", "beginning_offsets", "end_offsets", "close",
           "__aenter__", "__aexit__"}

DUNDERS = ("__enter__", "__exit__", "__aenter__", "__aexit__")


def public_methods(cls: type) -> list[str]:
    return [n for n in vars(cls) if (not n.startswith("_") or n in DUNDERS)
            and callable(getattr(cls, n))]


def test_bases_are_not_instantiable() -> None:
    with pytest.raises(TypeError) as e:
        Consumer()
    assert str(e.value) == "Consumer is a non-instantiable base; use KafkaConsumer or MockConsumer"
    with pytest.raises(TypeError) as e:
        AsyncConsumer()
    assert str(e.value) == ("AsyncConsumer is a non-instantiable base; use AsyncKafkaConsumer or "
                            "AsyncMockConsumer")


def test_members_keep_javas_declaration_order() -> None:
    assert public_methods(Consumer) == CONSUMER_METHODS
    assert public_methods(AsyncConsumer) == ASYNC_METHODS
    assert public_methods(KafkaConsumer) == []
    assert public_methods(AsyncKafkaConsumer) == []
    assert ["rebalance"] + public_methods(MockConsumerCore) == MOCK_METHODS
    assert public_methods(MockConsumer) == ["rebalance"]
    assert public_methods(AsyncMockConsumer) == ["rebalance"]


def test_async_def_iff_java_waits() -> None:
    for name in ASYNC_METHODS:
        assert inspect.iscoroutinefunction(getattr(AsyncConsumer, name)) is (name in WAITING), name
    for name in CONSUMER_METHODS:
        assert not inspect.iscoroutinefunction(getattr(Consumer, name)), name
    assert inspect.iscoroutinefunction(AsyncMockConsumer.rebalance)
    assert not inspect.iscoroutinefunction(MockConsumer.rebalance)
    for name in MOCK_METHODS[1:]:
        assert not inspect.iscoroutinefunction(getattr(AsyncMockConsumer, name)), name


@pytest.mark.parametrize("name", [
    "client_instance_id", "register_metric_for_subscription",
    "unregister_metric_from_subscription", "enforce_rebalance", "should_rebalance",
    "reset_should_rebalance", "set_client_instance_id", "inject_timeout_exception",
    "disable_telemetry", "added_metrics", "client_id",
])
def test_not_generated_or_dropped(name: str) -> None:
    for cls in (Consumer, KafkaConsumer, MockConsumer, AsyncConsumer, AsyncKafkaConsumer,
                AsyncMockConsumer):
        assert not hasattr(cls, name), (cls.__name__, name)


def test_current_lag_is_a_plain_def_on_every_class() -> None:
    # Its entry point, kafka_consumer_Consumer_current_lag, has no _async
    # form, so it is a plain def on both classes (Class family), the mocks
    # included.
    for cls in (Consumer, KafkaConsumer, MockConsumer, AsyncConsumer, AsyncKafkaConsumer,
                AsyncMockConsumer):
        assert not inspect.iscoroutinefunction(cls.current_lag), cls.__name__


def test_module_exports() -> None:
    assert "OffsetCommitCallback" in consumer_module.__all__
    assert "CommitCallback" not in consumer_module.__all__
    assert not hasattr(consumer_module, "CommitCallback")
    for name in ("Consumer", "KafkaConsumer", "MockConsumer", "AsyncConsumer",
                 "AsyncKafkaConsumer", "AsyncMockConsumer", "ConsumerRebalanceListener"):
        assert name in consumer_module.__all__


def test_rebalance_listener_defaults() -> None:
    calls: list[tuple[str, set[TopicPartition]]] = []

    class Revokes(ConsumerRebalanceListener):
        def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
            calls.append(("revoked", partitions))

    # The abstract methods are no-ops; the default onPartitionsLost calls
    # onPartitionsRevoked. They are called positionally.
    ConsumerRebalanceListener().on_partitions_revoked({TP})
    ConsumerRebalanceListener().on_partitions_assigned({TP})
    ConsumerRebalanceListener().on_partitions_lost({TP})
    Revokes().on_partitions_lost({TP})
    assert calls == [("revoked", {TP})]


# ---------------------------------------------------------------------------
# Keyword-only parameters, java_forms, stubs
# ---------------------------------------------------------------------------
def mock() -> MockConsumer[Any, Any]:
    c: MockConsumer[Any, Any] = MockConsumer(offset_reset_strategy="earliest")
    return c


POSITIONAL_CALLS = [
    ("subscribe", (["t"],)), ("assign", ([TP],)), ("poll", (0,)), ("commit", ({},)),
    ("commit_nowait", ({},)), ("seek", (TP, 0)), ("seek_to_beginning", ([TP],)),
    ("seek_to_end", ([TP],)), ("position", (TP,)), ("committed", ([TP],)),
    ("partitions_for", ("t",)), ("pause", ([TP],)), ("resume", ([TP],)),
    ("offsets_for_times", ({TP: 0},)), ("beginning_offsets", ([TP],)),
    ("end_offsets", ([TP],)), ("current_lag", (TP,)), ("close", (1,)),
]
MOCK_POSITIONAL_CALLS = [
    ("rebalance", ([TP],)), ("add_record", (None,)), ("set_max_poll_records", (1,)),
    ("set_poll_exception", (None,)), ("set_offsets_exception", (None,)),
    ("update_beginning_offsets", ({},)), ("update_end_offsets", ({},)),
    ("update_duration_offsets", ({},)), ("update_partitions", ("t", [])),
    ("schedule_poll_task", (lambda: None,)),
]


@pytest.mark.parametrize("name, args", POSITIONAL_CALLS + MOCK_POSITIONAL_CALLS)
def test_a_positional_call_is_a_type_error(name: str, args: tuple[Any, ...]) -> None:
    with pytest.raises(TypeError):
        getattr(mock(), name)(*args)


@pytest.mark.parametrize("name, args", POSITIONAL_CALLS)
def test_a_positional_call_on_kafka_consumer_is_a_type_error(name: str,
                                                              args: tuple[Any, ...]) -> None:
    with KafkaConsumer(configs=CONFIGS) as consumer, pytest.raises(TypeError):
        getattr(consumer, name)(*args)


@pytest.mark.parametrize("name, args", POSITIONAL_CALLS)
def test_a_positional_call_on_the_async_mock_is_a_type_error(name: str,
                                                              args: tuple[Any, ...]) -> None:
    async def main() -> None:
        c: AsyncMockConsumer[Any, Any] = AsyncMockConsumer(offset_reset_strategy="earliest")
        with pytest.raises(TypeError):
            result = getattr(c, name)(*args)
            if inspect.isawaitable(result):
                await result

    asyncio.run(main())


def test_constructors_are_keyword_only() -> None:
    for call in (lambda: MockConsumer("earliest"), lambda: KafkaConsumer(CONFIGS),
                 lambda: AsyncMockConsumer("earliest"), lambda: AsyncKafkaConsumer(CONFIGS)):
        with pytest.raises(TypeError):
            call()  # type: ignore[no-untyped-call]


SUBSCRIBE_MESSAGE = ("subscribe() takes one of (topics), (topics, callback), (pattern, callback), "
                     "(pattern); got ")
SEEK_MESSAGE = "seek() takes one of (partition, offset), (partition, offset_and_metadata); got "
COMMIT_NOWAIT_MESSAGE = "commit_nowait() takes one of (), (callback), (offsets, callback); got "


@pytest.mark.parametrize("kwargs, given", [
    ({}, "()"),
    ({"topics": ["t"], "pattern": SubscriptionPattern(pattern="t")}, "(topics, pattern)"),
    ({"topics": ["t"], "pattern": SubscriptionPattern(pattern="t"),
      "callback": ConsumerRebalanceListener()}, "(topics, pattern, callback)"),
    ({"callback": ConsumerRebalanceListener()}, "(callback)"),
])
def test_subscribe_rejects_a_non_java_combination(kwargs: dict[str, Any], given: str) -> None:
    with pytest.raises(IllegalArgumentError) as e:
        mock().subscribe(**kwargs)
    assert str(e.value) == SUBSCRIBE_MESSAGE + given


@pytest.mark.parametrize("kwargs, given", [
    ({"partition": TP}, "(partition)"),
    ({"partition": TP, "offset": 1, "offset_and_metadata": OffsetAndMetadata(offset=1)},
     "(partition, offset, offset_and_metadata)"),
])
def test_seek_rejects_a_non_java_combination(kwargs: dict[str, Any], given: str) -> None:
    with pytest.raises(IllegalArgumentError) as e:
        mock().seek(**kwargs)
    assert str(e.value) == SEEK_MESSAGE + given


def test_commit_nowait_rejects_offsets_without_a_callback() -> None:
    # Java has no commitAsync(Map) overload.
    with pytest.raises(IllegalArgumentError) as e:
        mock().commit_nowait(offsets={TP: OffsetAndMetadata(offset=1)})
    assert str(e.value) == COMMIT_NOWAIT_MESSAGE + "(offsets)"


def test_commit_nowait_takes_offsets_with_a_null_callback() -> None:
    # callback defaults to UNSET, so callback=None is given: Java's
    # commitAsync(offsets, null) (CLAUDE.md, Python Binding Conventions,
    # Signatures).
    c = mock()
    c.assign(partitions=[TP])
    c.commit_nowait(offsets={TP: OffsetAndMetadata(offset=4)}, callback=None)
    assert c.committed(partitions=[TP]) == {TP: OffsetAndMetadata(offset=4)}
    with KafkaConsumer(configs=CONFIGS) as kc:
        # Empty offsets complete at once, without a broker (Java's commit()).
        kc.commit_nowait(offsets={}, callback=None)


def test_close_with_a_timeout_is_not_generated() -> None:
    # Java's @Deprecated close(Duration) (CLAUDE.md, Class family): close()
    # and close(option) are every combination, so close has no decorator.
    with pytest.raises(TypeError):
        mock().close(timeout=1)  # type: ignore[call-arg]


def test_the_kafka_consumer_checks_the_same_forms() -> None:
    with KafkaConsumer(configs=CONFIGS) as c:
        with pytest.raises(IllegalArgumentError) as e:
            c.subscribe()
        assert str(e.value) == SUBSCRIBE_MESSAGE + "()"
        with pytest.raises(IllegalArgumentError) as e:
            c.seek(partition=TP)
        assert str(e.value) == SEEK_MESSAGE + "(partition)"
        with pytest.raises(IllegalArgumentError) as e:
            c.commit_nowait(offsets={})
        assert str(e.value) == COMMIT_NOWAIT_MESSAGE + "(offsets)"


def test_the_async_classes_check_the_same_forms() -> None:
    async def main() -> None:
        c: AsyncMockConsumer[Any, Any] = AsyncMockConsumer(offset_reset_strategy="earliest")
        with pytest.raises(IllegalArgumentError) as e:
            await c.subscribe()
        assert str(e.value) == SUBSCRIBE_MESSAGE + "()"
        with pytest.raises(IllegalArgumentError) as e:
            await c.seek(partition=TP)
        assert str(e.value) == SEEK_MESSAGE + "(partition)"
        with pytest.raises(IllegalArgumentError) as e:
            c.commit_nowait(offsets={})
        assert str(e.value) == COMMIT_NOWAIT_MESSAGE + "(offsets)"
        with pytest.raises(TypeError):
            await c.close(timeout=1)  # type: ignore[call-arg]

    asyncio.run(main())


def test_every_stub_form_works() -> None:
    c = mock()
    listener = ConsumerRebalanceListener()
    c.subscribe(topics=["t"])
    c.subscribe(topics=["t"], callback=listener)
    c.unsubscribe()
    c.subscribe(pattern=SubscriptionPattern(pattern="t.*"))
    c.unsubscribe()
    c.subscribe(pattern=SubscriptionPattern(pattern="t.*"), callback=listener)
    c.unsubscribe()
    c.assign(partitions=[TP])
    c.seek(partition=TP, offset=1)
    c.seek(partition=TP, offset_and_metadata=OffsetAndMetadata(offset=2))
    c.commit()
    c.commit(offsets={TP: OffsetAndMetadata(offset=2)})
    c.commit_nowait()
    c.commit_nowait(callback=lambda offsets, exception: None)
    c.commit_nowait(offsets={TP: OffsetAndMetadata(offset=2)},
                    callback=lambda offsets, exception: None)
    c.close()
    c.close(option=CloseOptions.timeout(timedelta(seconds=1)))


async def test_cancelling_close_waits_for_its_teardown() -> None:
    # Cancelling the task awaiting close() lets the close end, then raises
    # CancelledError: close() returns only after _finish_close (waiting for the
    # uses in flight, freeing the handle, closing the deserializers) has run,
    # however often the task is cancelled meanwhile (Copilot review, PR #187).
    # The mock's close() runs on the loop, so the real consumer: without a
    # group.id it has no group to leave, and its close ends at once.
    c = AsyncKafkaConsumer(configs={"bootstrap.servers": "localhost:1",
                                    "group.protocol": "consumer"})
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    real_finish_close = c._finish_close  # noqa: SLF001

    def finish_close() -> None:
        started.set()
        release.wait(10)
        real_finish_close()
        finished.set()

    c._finish_close = finish_close  # type: ignore[method-assign]  # noqa: SLF001
    task = asyncio.ensure_future(c.close())
    assert await asyncio.to_thread(started.wait, 10)
    task.cancel()
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.sleep(0.1)
    assert not task.done(), "close() returned while its teardown still runs"
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
