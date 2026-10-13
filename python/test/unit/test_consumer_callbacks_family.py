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
import sys
import threading
import weakref
from typing import Any, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.consumer import (
    AsyncConsumer, AsyncKafkaConsumer, AsyncMockConsumer, CloseOptions, Consumer,
    ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, MockConsumer, OffsetAndMetadata,
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


def test_kafka_consumer_callback_of_an_empty_commit_gets_null_offsets() -> None:
    # Java's commitAsync(emptyMap, cb): commit() returns completedFuture(null),
    # so whenComplete hands the callback (null, null).
    consumer = kafka_consumer()
    seen: list[Any] = []
    consumer.commit_nowait(offsets={}, callback=lambda o, e: seen.append((o, e)))
    consumer.commit(offsets={})
    assert seen == [(None, None)]
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
        assert [r.getMessage() for r in caplog.records] == [
            "An OffsetCommitCallback returned an awaitable; only an AsyncConsumer on its "
            "event loop awaits it, and it was not awaited"]
        # Closed, not awaited: its body never ran.
        assert not closed.is_set()
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
# An ``async def`` commit callback on the async consumers (rule 3, as for a
# listener method): awaited on the loop, inside the call that delivers it,
# one at a time in commit order
# ---------------------------------------------------------------------------
async def async_kafka_consumer() -> AsyncKafkaConsumer[bytes, bytes]:
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=CONFIGS)
    await consumer.assign(partitions=[TP])
    return consumer


async def async_mock_consumer() -> AsyncMockConsumer[bytes, bytes]:
    consumer: AsyncMockConsumer[bytes, bytes] = AsyncMockConsumer(
        offset_reset_strategy="earliest")
    await consumer.assign(partitions=[TP])
    consumer.update_beginning_offsets(new_offsets={TP: 0})
    return consumer


ASYNC_MAKERS = [async_kafka_consumer, async_mock_consumer]


async def deliver(consumer: AsyncConsumer[Any, Any], call: str) -> None:
    """The awaited call that delivers the queued commit callbacks."""
    if call == "poll":
        await consumer.poll(timeout=0)
    elif call == "commit":
        await consumer.commit(offsets={})
    else:
        await consumer.close(option=CloseOptions.timeout(5))


@pytest.mark.parametrize("make", ASYNC_MAKERS)
@pytest.mark.parametrize("call", ["poll", "commit", "close"])
def test_async_coroutine_callback_is_awaited_before_the_delivering_call_returns(
        make: Any, call: str) -> None:
    async def main() -> None:
        consumer = await make()
        seen: list[Any] = []
        released = asyncio.Event()

        async def callback(offsets: Any, exception: Any) -> None:
            seen.append(("start", threading.get_ident()))
            # Suspends until another task runs: the delivering call must keep
            # waiting through it.
            asyncio.get_running_loop().call_later(0.05, released.set)
            await released.wait()
            seen.append(("end", exception))

        consumer.commit_nowait(offsets={TP: OffsetAndMetadata(offset=1)} if
                               isinstance(consumer, AsyncMockConsumer) else {},
                               callback=callback)
        await deliver(consumer, call)
        assert seen == [("start", threading.get_ident()), ("end", None)]
        if call != "close":
            await consumer.close(option=CloseOptions.timeout(5))

    asyncio.run(main())


@pytest.mark.parametrize("make", ASYNC_MAKERS)
def test_async_coroutine_callbacks_run_one_after_another(make: Any) -> None:
    async def main() -> None:
        consumer = await make()
        events: list[str] = []

        def callback(name: str, delay: float) -> Any:
            async def run(offsets: Any, exception: Any) -> None:
                events.append(f"start {name}")
                await asyncio.sleep(delay)
                events.append(f"end {name}")
            return run

        def plain(offsets: Any, exception: Any) -> None:
            events.append("plain")

        offsets: dict[TopicPartition, OffsetAndMetadata] = (
            {TP: OffsetAndMetadata(offset=1)} if isinstance(consumer, AsyncMockConsumer) else {})
        # The first sleeps longest: the second still starts only after it ended.
        consumer.commit_nowait(offsets=offsets, callback=callback("first", 0.1))
        consumer.commit_nowait(offsets=offsets, callback=callback("second", 0.0))
        consumer.commit_nowait(offsets=offsets, callback=plain)
        await consumer.commit(offsets={})
        assert events == ["start first", "end first", "start second", "end second", "plain"]
        await consumer.close(option=CloseOptions.timeout(5))

    asyncio.run(main())


def test_async_core_delivers_the_next_callback_only_after_the_coroutine_reported() -> None:
    # The trampoline does not report a deferred invocation; the task does once
    # the coroutine ended, and only then does the core deliver the next
    # callback (OffsetCommitCallbackInvoker awaits each report).
    async def main() -> None:
        consumer = await async_kafka_consumer()
        queued_meanwhile: list[int] = []

        async def first(offsets: Any, exception: Any) -> None:
            await asyncio.sleep(0.1)  # time for the core to deliver the next one
            queued_meanwhile.append(_lib.Consumer__execute_callbacks(consumer._h))

        consumer.commit_nowait(offsets={}, callback=first)
        consumer.commit_nowait(offsets={}, callback=lambda o, e: None)
        await consumer.commit(offsets={})
        assert queued_meanwhile == [0]
        await consumer.close(option=CloseOptions.timeout(5))

    asyncio.run(main())


class CallbackError(ValueError):
    pass


@pytest.mark.parametrize("make", ASYNC_MAKERS)
@pytest.mark.parametrize("raised", [CallbackError("bad"), asyncio.CancelledError()],
                         ids=["error", "cancelled"])
def test_async_raising_coroutine_callback_is_logged(make: Any, raised: BaseException,
                                                    caplog: pytest.LogCaptureFixture) -> None:
    async def main() -> None:
        consumer = await make()

        async def callback(offsets: Any, exception: Any) -> None:
            await asyncio.sleep(0)
            raise raised

        consumer.commit_nowait(offsets={TP: OffsetAndMetadata(offset=1)} if
                               isinstance(consumer, AsyncMockConsumer) else {},
                               callback=callback)
        with caplog.at_level("ERROR", logger="confluent_kafka.consumer"):
            # Logged, not raised: the call that delivered it goes on.
            await consumer.commit(offsets={})
        records = [r for r in caplog.records if r.name == "confluent_kafka.consumer"]
        assert [r.getMessage() for r in records] == ["Error in OffsetCommitCallback"]
        assert records[0].exc_info is not None and records[0].exc_info[1] is raised
        await consumer.close(option=CloseOptions.timeout(5))

    asyncio.run(main())


@pytest.mark.parametrize("make", ASYNC_MAKERS)
def test_async_coroutine_callback_calls_back_into_the_consumer(make: Any) -> None:
    async def main() -> None:
        consumer = await make()
        seen: list[Any] = []

        async def callback(offsets: Any, exception: Any) -> None:
            await asyncio.sleep(0)
            seen.append(consumer.assignment())
            consumer.commit_nowait(offsets={}, callback=None)
            await consumer.pause(partitions=[TP])
            seen.append(consumer.paused())

        consumer.commit_nowait(offsets={TP: OffsetAndMetadata(offset=1)} if
                               isinstance(consumer, AsyncMockConsumer) else {},
                               callback=callback)
        await consumer.commit(offsets={})
        assert seen == [{TP}, {TP}]
        await consumer.close(option=CloseOptions.timeout(5))

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


# ---------------------------------------------------------------------------
# A listener's exception is owned by the foreign callback error reporting it
# (kafka_common_Error_local_callback with a release function): it survives with
# no Python-side reference until the operation's error is raised, and is
# released once that error is dropped
# ---------------------------------------------------------------------------
def ffi_mock_backed(consumer: Any) -> Any:
    """``consumer`` (a bare ``KafkaConsumer`` or ``AsyncKafkaConsumer``) over the
    FFI's ``kafka_consumer_MockConsumer_t``, so a rebalance runs the listener
    through the C trampolines without a broker."""
    consumer._h = _lib.Consumer_MockConsumer_new("earliest")
    consumer._group_id_defined = True
    consumer._pending_notify_ref = consumer._on_pending_notify
    _lib.Consumer__set_callbacks_notify(consumer._h, consumer._pending_notify_ref)
    return consumer


def sync_ffi_mock() -> KafkaConsumer[bytes, bytes]:
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer.__new__(KafkaConsumer)
    Consumer.__init__(consumer)
    return cast("KafkaConsumer[bytes, bytes]", ffi_mock_backed(consumer))


def async_ffi_mock() -> AsyncKafkaConsumer[bytes, bytes]:
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer.__new__(AsyncKafkaConsumer)
    AsyncConsumer.__init__(consumer)
    return cast("AsyncKafkaConsumer[bytes, bytes]", ffi_mock_backed(consumer))


def rebalance_spec(consumer: Any) -> tuple[Any, Any, Any]:
    """Drive ``MockConsumer.rebalance([TP])`` as a waiting call, collecting the
    garbage before the error is read back, so only the error keeps the
    listener's exception alive."""
    submit, resolve, free = consumer._void_spec(_lib.MockConsumer_rebalance_cb,
                                                [(TP.topic(), TP.partition())])

    def collected_then_resolved(payload: tuple[Any, ...]) -> None:
        gc.collect()
        resolve(payload)

    return submit, collected_then_resolved, free


class ListenerError(ValueError):
    """A listener's own exception (a subclass, so it takes a weak reference).
    Java's ``MockConsumer.rebalance`` lets it propagate unwrapped."""


class FreshErrorListener(ConsumerRebalanceListener):
    """Raises a new exception, remembered only through a weak reference."""

    def __init__(self) -> None:
        self.raised: list[weakref.ref[ListenerError]] = []

    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        error = ListenerError("bad")
        self.raised.append(weakref.ref(error))
        raise error


class AsyncFreshErrorListener(ConsumerRebalanceListener):
    def __init__(self) -> None:
        self.raised: list[weakref.ref[ListenerError]] = []

    async def on_partitions_assigned(  # type: ignore[override]
            self, partitions: set[TopicPartition]) -> None:
        error = ListenerError("bad")
        self.raised.append(weakref.ref(error))
        raise error


class HeldErrorListener(ConsumerRebalanceListener):
    """Raises the exception the test holds, to count its references."""

    def __init__(self, error: BaseException) -> None:
        self.error = error

    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        raise self.error


class AsyncHeldErrorListener(ConsumerRebalanceListener):
    def __init__(self, error: BaseException) -> None:
        self.error = error

    async def on_partitions_assigned(  # type: ignore[override]
            self, partitions: set[TopicPartition]) -> None:
        raise self.error


def test_the_listener_exception_lives_in_its_error_until_it_is_dropped() -> None:
    consumer = sync_ffi_mock()
    listener = FreshErrorListener()
    consumer.subscribe(topics=["t"], callback=listener)
    with pytest.raises(ListenerError) as e:
        consumer._run_sync(*rebalance_spec(consumer))
    [ref] = listener.raised
    assert e.value is ref()
    assert ref() is not None
    del e
    gc.collect()
    assert ref() is None
    close(consumer)


def test_the_listener_exception_reference_is_released_once() -> None:
    consumer = sync_ffi_mock()
    error = ListenerError("bad")
    consumer.subscribe(topics=["t"], callback=HeldErrorListener(error))
    gc.collect()
    baseline = sys.getrefcount(error)
    with pytest.raises(ListenerError) as e:
        consumer._run_sync(*rebalance_spec(consumer))
    assert e.value is error
    del e
    error.__traceback__ = None
    gc.collect()
    assert sys.getrefcount(error) == baseline
    close(consumer)


def test_async_the_listener_exception_lives_in_its_error_until_it_is_dropped() -> None:
    async def main() -> None:
        consumer = async_ffi_mock()
        listener = AsyncFreshErrorListener()
        await consumer.subscribe(topics=["t"], callback=listener)
        with pytest.raises(ListenerError) as e:
            await consumer._run_async(*rebalance_spec(consumer))
        [ref] = listener.raised
        assert e.value is ref()
        assert ref() is not None
        del e
        gc.collect()
        assert ref() is None
        await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())


def test_async_the_listener_exception_reference_is_released_once() -> None:
    async def main() -> None:
        consumer = async_ffi_mock()
        error = ListenerError("bad")
        await consumer.subscribe(topics=["t"], callback=AsyncHeldErrorListener(error))
        gc.collect()
        baseline = sys.getrefcount(error)
        with pytest.raises(ListenerError) as e:
            await consumer._run_async(*rebalance_spec(consumer))
        assert e.value is error
        del e
        error.__traceback__ = None
        gc.collect()
        assert sys.getrefcount(error) == baseline
        await consumer.close(option=CloseOptions.timeout(0))

    asyncio.run(main())
