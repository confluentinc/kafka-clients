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

"""An explicit ``None`` is Java's ``null`` on the consumer family (CLAUDE.md,
Python Binding Conventions, Signatures; Implementation over the FFI, exception 4
and Order): left out (``MISSING``) is the only "not given".

Each row is one call on the four consumer classes, with what Java does for it:
``AsyncKafkaConsumer`` for ``KafkaConsumer`` / ``AsyncKafkaConsumer`` (an
unreachable bootstrap; no call here reaches the broker), and Java's
``MockConsumer`` for ``MockConsumer`` / ``AsyncMockConsumer``, which checks only
where Java's mock checks (it ignores every ``Duration``, a null one included)."""

from __future__ import annotations

import asyncio
import inspect
import pydoc
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka._args import MISSING
from confluent_kafka.common import TopicPartition
from confluent_kafka.common.errors import InvalidGroupIdError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka import UnsupportedOperationError
from confluent_kafka.consumer import (
    AsyncConsumer,
    AsyncKafkaConsumer,
    AsyncMockConsumer,
    CloseOptions,
    Consumer,
    ConsumerRebalanceListener,
    KafkaConsumer,
    MockConsumer,
    OffsetAndMetadata,
    SubscriptionPattern,
)

TP = TopicPartition(topic="t", partition=0)
CONFIGS = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer", "group.id": "g",
           "default.api.timeout.ms": 200}
NO_GROUP = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer"}
POSITION_MESSAGE = "You can only check the position for partitions assigned to this consumer."

KINDS = ["kafka", "async_kafka", "mock", "async_mock"]
FFI = {"kafka", "async_kafka"}


def _make(kind: str, configs: dict[str, Any] | None = None) -> Any:
    if kind == "kafka":
        return KafkaConsumer(configs=dict(configs or CONFIGS))
    if kind == "async_kafka":
        return AsyncKafkaConsumer(configs=dict(configs or CONFIGS))
    if kind == "mock":
        return MockConsumer(offset_reset_strategy="earliest")
    return AsyncMockConsumer(offset_reset_strategy="earliest")


async def _invoke(consumer: Any, name: str, kwargs: dict[str, Any]) -> Any:
    result = getattr(consumer, name)(**kwargs)
    if inspect.isawaitable(result):
        result = await result
    return result


def run(kind: str, body: Callable[[Any, Callable[..., Any]], Awaitable[None]],
        configs: dict[str, Any] | None = None) -> None:
    """Run ``await body(consumer, call)`` on a fresh consumer of ``kind``, on an
    event loop (the async classes are bound to it); ``call(name, **kwargs)``
    calls the method, awaiting it on the async classes. Closed afterwards."""
    async def main() -> None:
        consumer = _make(kind, configs)

        def call(name: str, **kwargs: Any) -> Awaitable[Any]:
            return _invoke(consumer, name, kwargs)

        try:
            await body(consumer, call)
        finally:
            await _invoke(consumer, "close", {})

    asyncio.run(main())


async def raises(error: type[BaseException], message: str, fn: Callable[[], Any]) -> None:
    with pytest.raises(error) as exc:
        result = fn()
        if inspect.isawaitable(result):
            await result
    assert type(exc.value) is error, exc.value
    assert str(exc.value) == message


class _Listener(ConsumerRebalanceListener):
    pass

    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        pass

    def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        pass


# ---- subscribe ---------------------------------------------------------------
@pytest.mark.parametrize("kind", KINDS)
def test_subscribe_null_listener(kind: str) -> None:
    # Java: "RebalanceListener cannot be null"
    # (org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#subscribe(Collection,ConsumerRebalanceListener),
    # #subscribe(SubscriptionPattern,ConsumerRebalanceListener);
    # org.apache.kafka.clients.consumer.MockConsumer#subscribe(SubscriptionPattern,ConsumerRebalanceListener),
    # #subscribe(Collection,ConsumerRebalanceListener)).
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await raises(IllegalArgumentError, "RebalanceListener cannot be None",
               lambda: call("subscribe", topics=["t"], callback=None))
        await raises(IllegalArgumentError, "RebalanceListener cannot be None",
               lambda: call("subscribe", pattern=SubscriptionPattern(pattern="t.*"),
                            callback=None))
        assert c.subscription() == set()
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_subscribe_null_topics(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        # org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#subscribeInternal(Collection,Optional)'s check
        if kind in FFI:
            await raises(IllegalArgumentError, "Topic collection to subscribe to cannot be None",
                   lambda: call("subscribe", topics=None))
            await raises(IllegalArgumentError, "Topic collection to subscribe to cannot be None",
                   lambda: call("subscribe", topics=None, callback=_Listener()))
        else:  # the mock's new HashSet<>(null)
            await raises(TypeError, "topics must not be None", lambda: call("subscribe", topics=None))
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_subscribe_null_pattern(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        message = ("Topic pattern to subscribe to cannot be None" if kind in FFI
                   # org.apache.kafka.clients.consumer.MockConsumer#subscribe(SubscriptionPattern)
                   else "Topic pattern cannot be None")
        await raises(IllegalArgumentError, message, lambda: call("subscribe", pattern=None))
        await raises(IllegalArgumentError, message,
               lambda: call("subscribe", pattern=None, callback=_Listener()))
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_subscribe_listener_check_precedes_the_null_collection(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await raises(IllegalArgumentError, "RebalanceListener cannot be None",
               lambda: call("subscribe", topics=None, callback=None))
    run(kind, body)


@pytest.mark.parametrize("kind", ["kafka", "async_kafka"])
def test_subscribe_null_collection_before_the_group_check(kind: str) -> None:
    # Order: the Python null check runs before the core's group check, where
    # Java's subscribeInternal checks the group first (accepted, CLAUDE.md Order).
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await raises(IllegalArgumentError, "Topic collection to subscribe to cannot be None",
               lambda: call("subscribe", topics=None))
        await raises(InvalidGroupIdError, str(_group_message()), lambda: call("subscribe", topics=["t"]))
    run(kind, body, NO_GROUP)


def _group_message() -> str:
    return ("To use the group management or offset commit APIs, you must provide a valid "
            "group.id in the consumer configuration.")


@pytest.mark.parametrize("kind", KINDS)
def test_subscribe_forms(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await call("subscribe", topics=["t"])
        assert c.subscription() == {"t"}
        await call("subscribe", topics=["u"], callback=_Listener())
        assert c.subscription() == {"u"}
        for kwargs in ({}, {"topics": ["t"], "pattern": SubscriptionPattern(pattern="t")},
                       {"callback": None}):
            await raises(IllegalArgumentError,
                   "subscribe() takes one of (topics), (topics, callback), (pattern, callback), "
                   "(pattern); got (" + ", ".join(kwargs) + ")",
                   lambda kwargs=kwargs: call("subscribe", **kwargs))
        # MISSING passed explicitly is the same as left out.
        await call("subscribe", topics=["v"], callback=MISSING)
        assert c.subscription() == {"v"}
    run(kind, body)


# ---- commit / commit_nowait ------------------------------------------------------
@pytest.mark.parametrize("kind", KINDS)
def test_commit_null_offsets(kind: str) -> None:
    # Java: new HashMap<>(null) (org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#commitSync(Map)); the
    # mock's committed.putAll(null) (org.apache.kafka.clients.consumer.MockConsumer#commitAsync(Map,OffsetCommitCallback)).
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await raises(TypeError, "offsets must not be None", lambda: call("commit", offsets=None))
        await raises(TypeError, "offsets must not be None",
               lambda: call("commit", offsets=None, timeout=1.0))
        await raises(TypeError, "offsets must not be None",
               lambda: call("commit_nowait", offsets=None, callback=None))
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_commit_null_timeout(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        if kind in FFI:  # calculateDeadlineMs(time, null)
            await raises(TypeError, "timeout must not be None", lambda: call("commit", timeout=None))
            await raises(TypeError, "timeout must not be None",
                   lambda: call("commit", offsets={}, timeout=None))
        else:  # Java's mock ignores the Duration
            await call("commit", timeout=None)
            await call("commit", offsets={}, timeout=None)
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_commit_with_missing_is_left_out(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await call("assign", partitions=[TP])
        await call("commit", offsets={TP: OffsetAndMetadata(offset=1)}, timeout=MISSING)
        if kind not in FFI:
            assert await call("committed", partitions=[TP]) == {TP: OffsetAndMetadata(offset=1)}
            await call("commit", offsets=MISSING)
    if kind in FFI:
        return  # an unreachable bootstrap: no commit completes without a coordinator
    run(kind, body)


# ---- seek ----------------------------------------------------------------------
@pytest.mark.parametrize("kind", KINDS)
def test_seek_null_arguments(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await call("assign", partitions=[TP])
        # Java's offsetAndMetadata.offset()
        # (org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#seek(TopicPartition,OffsetAndMetadata);
        # org.apache.kafka.clients.consumer.MockConsumer#seek(TopicPartition,OffsetAndMetadata)).
        await raises(TypeError, "offset_and_metadata must not be None",
               lambda: call("seek", partition=TP, offset_and_metadata=None))
        # A long cannot be null: Python's TypeError.
        await raises(TypeError, "offset must not be None",
               lambda: call("seek", partition=TP, offset=None))
        # subscriptions.seekUnvalidated(null, …) / seek(null, …): no assignment.
        await raises(IllegalStateError, "No current assignment for partition None",
               lambda: call("seek", partition=None, offset=5))
        await raises(IllegalStateError, "No current assignment for partition None",
               lambda: call("seek", partition=None, offset_and_metadata=OffsetAndMetadata(
                   offset=5)))
        await raises(IllegalArgumentError,
               "seek() takes one of (partition, offset), (partition, offset_and_metadata); "
               "got (partition)", lambda: call("seek", partition=TP))
    run(kind, body)


@pytest.mark.parametrize("kind", ["kafka", "async_kafka"])
def test_seek_null_partition_checks_the_offset_first(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await raises(IllegalArgumentError, "seek offset must not be a negative number",
               lambda: call("seek", partition=None, offset=-1))
        # With a leader epoch: Metadata.updateLastSeenEpochIfNewer's requireNonNull,
        # wrapped by ConsumerUtils.getResult (test_seek_null_partition_with_an_epoch_…).
        from confluent_kafka.common import KafkaError

        await raises(KafkaError, "builtins.TypeError: partition must not be None",
                     lambda: call("seek", partition=None, offset_and_metadata=OffsetAndMetadata(
                         offset=5, leader_epoch=1, metadata="")))
    run(kind, body)


# ---- position / committed ---------------------------------------------------------
@pytest.mark.parametrize("kind", KINDS)
def test_position_null_partition(kind: str) -> None:
    # Java's subscriptions.isAssigned(null) is false: IllegalStateException
    # (org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#position(TopicPartition,Duration)); Java's mock
    # throws IllegalArgumentException with the same text (org.apache.kafka.clients.consumer.MockConsumer#position(TopicPartition)).
    error = IllegalStateError if kind in FFI else IllegalArgumentError

    async def body(c: Any, call: Callable[..., Any]) -> None:
        await raises(error, POSITION_MESSAGE, lambda: call("position", partition=None))
        await raises(error, POSITION_MESSAGE, lambda: call("position", partition=None, timeout=None))
        await raises(error, POSITION_MESSAGE, lambda: call("position", partition=None, timeout=-1))
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_position_null_timeout(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await call("assign", partitions=[TP])
        if kind in FFI:
            await raises(TypeError, "timeout must not be None",
                   lambda: call("position", partition=TP, timeout=None))
        else:
            c.update_beginning_offsets(new_offsets={TP: 3})
            assert await call("position", partition=TP, timeout=None) == 3
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_committed_null_arguments(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await raises(TypeError, "partitions must not be None",
               lambda: call("committed", partitions=None))
        if kind in FFI:
            await raises(TypeError, "timeout must not be None",
                   lambda: call("committed", partitions=[TP], timeout=None))
            # partitions.isEmpty() precedes the Duration's use.
            await raises(TypeError, "partitions must not be None",
                   lambda: call("committed", partitions=None, timeout=None))
        else:
            assert await call("committed", partitions=[TP], timeout=None) == {}
    run(kind, body)


@pytest.mark.parametrize("kind", ["kafka", "async_kafka"])
def test_committed_group_check_comes_first(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await raises(InvalidGroupIdError, _group_message(), lambda: call("committed", partitions=None))
        await raises(InvalidGroupIdError, _group_message(),
               lambda: call("committed", partitions=[TP], timeout=None))
    run(kind, body, NO_GROUP)


# ---- the metadata and offset lookups --------------------------------------------------
@pytest.mark.parametrize("kind", KINDS)
def test_partitions_for_null_arguments(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        if kind in FFI:
            # TopicMetadataEvent's Objects.requireNonNull(topic); a zero timeout
            # throws new TimeoutException() first (point n,
            # org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#partitionsFor(String,Duration)).
            await raises(TypeError, "topic must not be None", lambda: call("partitions_for", topic=None))
            await raises(TypeError, "topic must not be None",
                   lambda: call("partitions_for", topic=None, timeout=1.0))
            await raises(KafkaTimeoutError, "", lambda: call("partitions_for", topic=None, timeout=0))
            await raises(TypeError, "timeout must not be None",
                   lambda: call("partitions_for", topic="t", timeout=None))
            await raises(TypeError, "timeout must not be None",
                   lambda: call("partitions_for", topic=None, timeout=None))
            await raises(TypeError, "timeout must not be None", lambda: call("list_topics", timeout=None))
        else:  # the mock's partitions.getOrDefault(null, emptyList())
            assert await call("partitions_for", topic=None) == []
            assert await call("partitions_for", topic="t", timeout=None) == []
            assert await call("list_topics", timeout=None) == {}
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_offset_lookups_null_arguments(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        for name in ("beginning_offsets", "end_offsets"):
            await raises(TypeError, "partitions must not be None", lambda name=name: call(
                name, partitions=None))
        if kind in FFI:
            await raises(TypeError, "timestamps_to_search must not be None",
                   lambda: call("offsets_for_times", timestamps_to_search=None))
            for name in ("beginning_offsets", "end_offsets"):
                await raises(TypeError, "timeout must not be None", lambda name=name: call(
                    name, partitions=[TP], timeout=None))
            await raises(TypeError, "timeout must not be None",
                   lambda: call("offsets_for_times", timestamps_to_search={TP: 0}, timeout=None))
        else:
            c.update_beginning_offsets(new_offsets={TP: 1})
            c.update_end_offsets(new_offsets={TP: 9})
            assert await call("beginning_offsets", partitions=[TP], timeout=None) == {TP: 1}
            assert await call("end_offsets", partitions=[TP], timeout=None) == {TP: 9}
            await raises(UnsupportedOperationError, "Not implemented yet.",
                   lambda: call("offsets_for_times", timestamps_to_search=None))
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_current_lag_null_partition(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        if kind in FFI:  # CurrentLagEvent's Objects.requireNonNull(partition)
            await raises(TypeError, "topic_partition must not be None",
                   lambda: c.current_lag(topic_partition=None))
        else:  # the mock's endOffsets.containsKey(null) is false: caught up
            assert await call("current_lag", topic_partition=None) == 0
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_partition_collections_null(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        if kind in FFI:
            await raises(IllegalArgumentError, "Topic partitions collection to assign to cannot be None",
                   lambda: call("assign", partitions=None))
            await raises(IllegalArgumentError, "Partitions collection cannot be None",
                   lambda: call("seek_to_beginning", partitions=None))
        else:  # the mock's new HashSet<>(null), requestOffsetReset(null)
            await raises(TypeError, "partitions must not be None", lambda: call("assign", partitions=None))
            await raises(TypeError, "partitions must not be None",
                   lambda: call("seek_to_beginning", partitions=None))
            await raises(TypeError, "partitions must not be None",
                   lambda: call("seek_to_end", partitions=None))
        await raises(TypeError, "partitions must not be None", lambda: call("pause", partitions=None))
        await raises(TypeError, "partitions must not be None", lambda: call("resume", partitions=None))
    run(kind, body)


# ---- poll and close -------------------------------------------------------------------
@pytest.mark.parametrize("kind", KINDS)
def test_poll_null_timeout(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        if kind in FFI:  # time.timer(null)
            await raises(TypeError, "timeout must not be None", lambda: call("poll", timeout=None))
        else:  # Java's mock stores the Duration as given
            assert (await call("poll", timeout=None)).is_empty()
            assert c.last_poll_timeout() is None
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_close_null_option(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        if kind in FFI:  # option.timeout() on a null: the consumer stays open
            await raises(TypeError, "option must not be None", lambda: call("close", option=None))
            assert c.assignment() == set()
        else:  # Java's mock never reads the option
            await call("close", option=None)
            assert c.closed()
    run(kind, body)


@pytest.mark.parametrize("kind", KINDS)
def test_close_with_missing_is_close(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await call("close", option=MISSING)
        await raises(IllegalStateError, "This consumer has already been closed.",
               lambda: call("assign", partitions=[TP]))
    run(kind, body)


@pytest.mark.parametrize("kind", ["kafka", "async_kafka"])
def test_the_closed_check_comes_first(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await call("close")
        closed = "This consumer has already been closed."
        for name, kwargs in (("subscribe", {"topics": ["t"], "callback": None}),
                             ("subscribe", {"topics": None}),
                             ("commit", {"offsets": None}),
                             ("commit", {"timeout": None}),
                             ("seek", {"partition": TP, "offset_and_metadata": None}),
                             ("position", {"partition": None}),
                             ("partitions_for", {"topic": None}),
                             ("poll", {"timeout": None})):
            await raises(IllegalStateError, closed, lambda name=name, kwargs=kwargs: call(name, **kwargs))
        # A second close() returns before its argument checks (Order).
        await call("close", option=None)
    run(kind, body)


def test_mock_listener_check_precedes_the_closed_check() -> None:
    # org.apache.kafka.clients.consumer.MockConsumer#subscribe(Collection,ConsumerRebalanceListener) with a null
    # listener checks it before ensureNotClosed().
    c: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy="earliest")
    c.close()

    async def check() -> None:
        await raises(IllegalArgumentError, "RebalanceListener cannot be None",
                     lambda: c.subscribe(topics=["t"], callback=None))  # type: ignore[call-overload]
        await raises(IllegalStateError, "This consumer has already been closed.",
                     lambda: c.subscribe(topics=None))  # type: ignore[call-overload]

    asyncio.run(check())


# ---- the mock's own methods -----------------------------------------------------------
@pytest.mark.parametrize("kind", ["mock", "async_mock"])
def test_mock_helpers_null_arguments(kind: str) -> None:
    async def body(c: Any, call: Callable[..., Any]) -> None:
        await raises(TypeError, "new_assignment must not be None",
               lambda: call("rebalance", new_assignment=None))
        await raises(TypeError, "partitions_lost must not be None",
               lambda: call("lose_partitions", partitions_lost=None))
        await raises(TypeError, "record must not be None", lambda: c.add_record(record=None))
        for name in ("update_beginning_offsets", "update_end_offsets", "update_duration_offsets"):
            await raises(TypeError, "new_offsets must not be None",
                   lambda name=name: getattr(c, name)(new_offsets=None))
    run(kind, body)


# ---- MISSING in the signatures ---------------------------------------------------------
@pytest.mark.parametrize("cls", [Consumer, AsyncConsumer])
def test_signatures_show_missing(cls: type) -> None:
    sig = inspect.signature(cls.close)
    assert sig.parameters["option"].default is MISSING
    assert "option: 'CloseOptions' = MISSING" in str(sig)
    for name in ("commit", "position", "committed", "partitions_for", "list_topics",
                 "offsets_for_times", "beginning_offsets", "end_offsets"):
        params = inspect.signature(getattr(cls, name)).parameters
        assert params["timeout"].default is MISSING, name
    params = inspect.signature(cls.subscribe).parameters
    assert [params[n].default for n in ("topics", "pattern", "callback")] == [MISSING] * 3
    assert "= MISSING" in pydoc.render_doc(cls.close, renderer=pydoc.plaintext)
    assert CloseOptions is not None


# ---- Critic 99: Java's empty-input return and the background NPE --------------------
@pytest.mark.parametrize("kind", ["kafka", "async_kafka"])
def test_an_empty_input_returns_before_the_null_timeout(kind: str) -> None:
    # committed / beginningOffsets / endOffsets / offsetsForTimes return the empty
    # map before they read the Duration (org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#committed(Set,Duration),
    # #offsetsForTimes(Map,Duration), #beginningOrEndOffset), so a None timeout is never
    # dereferenced there.
    async def body(c: Any, call: Callable[..., Any]) -> None:
        assert await call("committed", partitions=[], timeout=None) == {}
        assert await call("beginning_offsets", partitions=[], timeout=None) == {}
        assert await call("end_offsets", partitions=[], timeout=None) == {}
        assert await call("offsets_for_times", timestamps_to_search={}, timeout=None) == {}
        # A one-shot iterable is read once, so an empty one is an empty input.
        assert await call("committed", partitions=iter([]), timeout=None) == {}
        assert await call("committed", partitions=(tp for tp in []), timeout=None) == {}
        assert await call("beginning_offsets", partitions=iter([]), timeout=None) == {}
        await raises(TypeError, "timeout must not be None",
                     lambda: call("committed", partitions=[TP], timeout=None))
    run(kind, body)


@pytest.mark.parametrize("kind", ["kafka", "async_kafka"])
def test_seek_null_partition_with_an_epoch_is_a_wrapped_npe(kind: str) -> None:
    # Metadata.updateLastSeenEpochIfNewer's requireNonNull throws on the
    # background thread; ConsumerUtils.getResult wraps it: KafkaException(npe).
    from confluent_kafka.common import KafkaError

    async def body(c: Any, call: Callable[..., Any]) -> None:
        with pytest.raises(KafkaError) as exc:
            await call("seek", partition=None, offset_and_metadata=OffsetAndMetadata(
                offset=5, leader_epoch=3, metadata=""))
        assert type(exc.value) is KafkaError
        assert type(exc.value.__cause__) is TypeError
        assert str(exc.value.__cause__) == "partition must not be None"
        assert str(exc.value) == "builtins.TypeError: partition must not be None"
    run(kind, body)


@pytest.mark.parametrize("kind", ["mock", "async_mock"])
def test_mock_update_partitions_stores_a_null_list(kind: str) -> None:
    # org.apache.kafka.clients.consumer.MockConsumer#updatePartitions stores the list as given.
    async def body(c: Any, call: Callable[..., Any]) -> None:
        c.update_partitions(topic="t", partitions=None)
        assert await call("partitions_for", topic="t") is None
        assert await call("list_topics") == {"t": None}
    run(kind, body)
