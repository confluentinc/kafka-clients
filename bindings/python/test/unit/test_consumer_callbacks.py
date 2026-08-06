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

"""Consumer callback bridging: rebalance listeners, ``commitAsync`` completion
callbacks, and the :class:`ConsumerHandle` reentrancy path.

Driven by ``MockConsumer.rebalance``, which invokes the registered listener
inline — the deterministic, broker-free equivalent of a real rebalance. Its
semantics constrain what can be asserted here:

* it requires a **topic subscription** (a manually assigned consumer fails);
* it fires ``on_partitions_revoked`` only when something was removed;
* it fires ``on_partitions_assigned`` unconditionally while a listener is
  registered, with the *added* partitions (possibly an empty list);
* it never fires ``on_partitions_lost``, so the Java default
  (``onPartitionsLost`` delegating to ``onPartitionsRevoked``) is asserted
  against the adapter directly.
"""

import asyncio
import gc
import threading
import weakref

import pytest
from consumer import (
    AsyncMockConsumer, ConsumerHandle, KafkaConsumer, MockConsumer,
    OffsetAndMetadata, TopicPartition,
)
from producer import KafkaError

WAIT = 2.0

# ConcurrentModification (and any other unmapped error) surfaces in the bindings
# as UnknownServerError.
UNKNOWN_SERVER_ERROR = -1


def _tps(partitions):
    """Normalize a list[TopicPartition] to a sorted list of tuples."""
    return sorted((p.topic, p.partition) for p in partitions)


class RecordingListener:
    """Records every callback with its partitions, in order."""

    def __init__(self):
        self.events = []

    def on_partitions_revoked(self, partitions):
        self.events.append(("revoked", _tps(partitions)))

    def on_partitions_assigned(self, partitions):
        self.events.append(("assigned", _tps(partitions)))


class RecordingListenerWithLost(RecordingListener):
    def on_partitions_lost(self, partitions):
        self.events.append(("lost", _tps(partitions)))


# -- rebalance listener: invocation and partitions ---------------------------

def test_listener_assigned_on_first_rebalance():
    with MockConsumer("earliest") as c:
        listener = RecordingListener()
        c.subscribe(["t"], listener)
        assert c.subscription() == {"t"}
        c.rebalance([TopicPartition("t", 0), TopicPartition("t", 1)])
        assert listener.events == [("assigned", [("t", 0), ("t", 1)])]
        assert c.assignment() == {TopicPartition("t", 0), TopicPartition("t", 1)}


def test_listener_revoked_then_added_only_on_second_rebalance():
    # Java hands onPartitionsAssigned only the *newly added* partitions, and
    # onPartitionsRevoked only the removed ones — not the whole assignment.
    with MockConsumer("earliest") as c:
        listener = RecordingListener()
        c.subscribe(["t"], listener)
        c.rebalance([TopicPartition("t", 0), TopicPartition("t", 1)])
        listener.events.clear()
        c.rebalance([TopicPartition("t", 1), TopicPartition("t", 2)])
        assert listener.events == [
            ("revoked", [("t", 0)]),
            ("assigned", [("t", 2)]),
        ]


def test_rebalance_fires_assigned_even_when_nothing_was_added():
    with MockConsumer("earliest") as c:
        listener = RecordingListener()
        c.subscribe(["t"], listener)
        c.rebalance([TopicPartition("t", 0)])
        listener.events.clear()
        c.rebalance([TopicPartition("t", 0)])
        assert listener.events == [("assigned", [])]


def test_rebalance_requires_a_topic_subscription():
    with MockConsumer("earliest") as c:
        c.assign([TopicPartition("t", 0)])
        with pytest.raises(KafkaError) as exc_info:
            c.rebalance([TopicPartition("t", 0)])
        assert "manual assignment in use" in str(exc_info.value)


def test_rebalance_without_a_listener_is_a_no_op_but_reassigns():
    with MockConsumer("earliest") as c:
        c.subscribe(["t"])
        c.rebalance([TopicPartition("t", 5)])
        assert c.assignment() == {TopicPartition("t", 5)}


def test_listener_missing_a_required_method_is_rejected():
    class Partial:
        def on_partitions_revoked(self, partitions):
            pass

    with MockConsumer("earliest") as c:
        with pytest.raises(TypeError) as exc_info:
            c.subscribe(["t"], Partial())
        assert "on_partitions_assigned" in str(exc_info.value)


def test_listener_lost_defaults_to_revoked():
    """Java's ``ConsumerRebalanceListener.onPartitionsLost`` default delegates to
    ``onPartitionsRevoked``. The FFI always passes all three trampolines, but
    MockConsumer never fires ``lost``, so the delegation is asserted against the
    adapter directly — ``_on_lost`` is exactly what the C trampoline calls, with
    the same already-converted ``list[(topic, partition)]`` payload."""
    import consumer as _cons

    without_lost = RecordingListener()
    _cons._ListenerAdapter(without_lost)._on_lost([("t", 3)])
    assert without_lost.events == [("revoked", [("t", 3)])]

    with_lost = RecordingListenerWithLost()
    _cons._ListenerAdapter(with_lost)._on_lost([("t", 3)])
    assert with_lost.events == [("lost", [("t", 3)])]


# -- rebalance listener: error propagation -----------------------------------

def test_listener_exception_propagates_as_kafka_error():
    class Boom:
        def on_partitions_revoked(self, partitions):
            pass

        def on_partitions_assigned(self, partitions):
            raise ValueError("boom-from-listener")

    with MockConsumer("earliest") as c:
        c.subscribe(["t"], Boom())
        with pytest.raises(KafkaError) as exc_info:
            c.rebalance([TopicPartition("t", 0)])
        # The exception text propagates verbatim, like a throwing Java listener.
        assert str(exc_info.value) == "boom-from-listener"
        assert exc_info.value.code == UNKNOWN_SERVER_ERROR


def test_listener_exception_in_revoked_propagates():
    class BoomOnRevoke(RecordingListener):
        def on_partitions_revoked(self, partitions):
            raise ValueError("revoke-boom")

    with MockConsumer("earliest") as c:
        c.subscribe(["t"], BoomOnRevoke())
        c.rebalance([TopicPartition("t", 0)])  # nothing revoked yet
        with pytest.raises(KafkaError) as exc_info:
            c.rebalance([TopicPartition("t", 1)])
        assert str(exc_info.value) == "revoke-boom"


# -- rebalance listener: ordering guarantee (consumer-threading.md §31 #2) ----

def test_rebalance_blocks_until_listener_returns():
    """The rebalance must not complete before the listener has. Drive it from a
    worker thread and hold the listener on an Event: the rebalance call is still
    outstanding, and only completes once the listener is released."""
    entered = threading.Event()
    release = threading.Event()

    class Blocking:
        def on_partitions_revoked(self, partitions):
            pass

        def on_partitions_assigned(self, partitions):
            entered.set()
            assert release.wait(WAIT * 5), "test failed to release the listener"

    c = MockConsumer("earliest")
    c.subscribe(["t"], Blocking())
    returned = threading.Event()

    def drive():
        c.rebalance([TopicPartition("t", 0)])
        returned.set()

    worker = threading.Thread(target=drive)
    worker.start()
    try:
        assert entered.wait(WAIT), "listener should have been entered"
        assert not returned.wait(0.3), \
            "rebalance must not return while the listener is still running"
        release.set()
        assert returned.wait(WAIT), \
            "rebalance must complete once the listener returns"
    finally:
        release.set()
        worker.join(timeout=WAIT)
        c.close()


# -- reentrancy: ConsumerHandle from inside a listener (§31 #1) ---------------

def test_listener_can_use_handle_without_deadlock():
    """A listener reaching back into the consumer through ``handle()`` must not
    deadlock: the callback runs on the dispatcher thread, whose handle ops bypass
    the consumer's access guard. On a MockConsumer the getters are empty and the
    commit is unsupported — what matters is that the calls return at all."""
    with MockConsumer("earliest") as c:
        handle = c.handle()
        seen = {}

        class UsesHandle:
            def on_partitions_revoked(self, partitions):
                pass

            def on_partitions_assigned(self, partitions):
                seen["assignment"] = handle.assignment()
                seen["subscription"] = handle.subscription()
                seen["paused"] = handle.paused()
                try:
                    handle.commit_sync()
                    seen["commit_error"] = None
                except KafkaError as exc:
                    seen["commit_error"] = str(exc)

        try:
            c.subscribe(["t"], UsesHandle())
            c.rebalance([TopicPartition("t", 0)])
        finally:
            handle.destroy()

        assert seen["assignment"] == set()
        assert seen["subscription"] == set()
        assert seen["paused"] == set()
        assert "not supported on a MockConsumer handle" in seen["commit_error"]


def test_consumer_method_from_listener_is_rejected_as_concurrent():
    """Documents why ``handle()`` exists: the consumer's own methods are rejected
    while the operation that drove the callback still owns the access guard."""
    c = MockConsumer("earliest")
    seen = {}

    class UsesConsumer:
        def on_partitions_revoked(self, partitions):
            pass

        def on_partitions_assigned(self, partitions):
            try:
                c.seek(TopicPartition("t", 0), 0)
                seen["error"] = None
            except KafkaError as exc:
                seen["error"] = exc

    c.subscribe(["t"], UsesConsumer())
    c.rebalance([TopicPartition("t", 0)])
    assert seen["error"] is not None, \
        "a plain consumer call from a callback must be rejected"
    assert seen["error"].code == UNKNOWN_SERVER_ERROR
    c.close()


# -- listener registration lifetime ------------------------------------------

def test_listener_released_on_replacing_subscribe():
    c = MockConsumer("earliest")
    listener = RecordingListener()
    ref = weakref.ref(listener)
    c.subscribe(["t"], listener)
    del listener
    gc.collect()
    assert ref() is not None, "a registered listener must stay alive"

    c.subscribe(["t"], RecordingListener())
    gc.collect()
    assert ref() is None, "a replaced listener must be released"
    c.close()


def test_listener_released_by_a_listenerless_subscribe():
    # Java's subscribe(Collection) clears any registered listener.
    c = MockConsumer("earliest")
    listener = RecordingListener()
    ref = weakref.ref(listener)
    c.subscribe(["t"], listener)
    del listener
    gc.collect()
    c.subscribe(["t"])
    gc.collect()
    assert ref() is None
    c.close()


def test_listener_survives_unsubscribe_but_not_close():
    """``SubscriptionState.unsubscribe()`` leaves the listener registered in Java,
    so the Python binding must not release it there either. Destroying the
    consumer (which ``close()`` does) is what releases it."""
    c = MockConsumer("earliest")
    listener = RecordingListener()
    ref = weakref.ref(listener)
    c.subscribe(["t"], listener)
    del listener
    gc.collect()

    c.unsubscribe()
    gc.collect()
    assert ref() is not None, "unsubscribe must NOT release the listener"

    c.close()
    gc.collect()
    assert ref() is None, "closing the consumer releases the listener"


def test_listener_released_when_consumer_is_garbage_collected():
    c = MockConsumer("earliest")
    listener = RecordingListener()
    ref = weakref.ref(listener)
    c.subscribe(["t"], listener)
    del listener
    gc.collect()
    c.close()
    del c
    gc.collect()
    assert ref() is None


# -- commitAsync completion callback -----------------------------------------

def _seeded(c, topic="t", partition=0, count=1):
    """Assign one partition with ``count`` records and consume them, so there is
    a position to commit."""
    tp = TopicPartition(topic, partition)
    c.assign([tp])
    for i in range(count):
        c.add_record(topic, partition, i, b"k", b"v")
    c.update_beginning_offsets(topic, partition, 0)
    c.poll(1.0)
    return tp


def test_commit_async_callback_receives_current_positions():
    with MockConsumer("earliest") as c:
        tp = _seeded(c)
        seen = []
        c.commit_async(callback=lambda offsets, exc: seen.append((offsets, exc)))
        (offsets, exc), = seen
        assert exc is None
        assert offsets == {tp: OffsetAndMetadata(1, "", None)}


def test_commit_async_callback_receives_explicit_offsets():
    with MockConsumer("earliest") as c:
        tp = _seeded(c)
        seen = []
        c.commit_async({tp: OffsetAndMetadata(7, "meta")},
                       callback=lambda offsets, exc: seen.append((offsets, exc)))
        (offsets, exc), = seen
        assert exc is None
        assert offsets == {tp: OffsetAndMetadata(7, "meta", None)}
        assert c.committed([tp]) == {tp: OffsetAndMetadata(7, "meta", None)}


def test_commit_async_without_a_callback():
    # All three Java overloads: commitAsync(), commitAsync(cb), commitAsync(map, cb).
    with MockConsumer("earliest") as c:
        tp = _seeded(c)
        c.commit_async()
        c.commit_async({tp: OffsetAndMetadata(4)})
        assert c.committed([tp]) == {tp: OffsetAndMetadata(4, "", None)}


def test_commit_async_callback_exception_is_swallowed():
    # Java's OffsetCommitCallback.onComplete returns void — there is nowhere to
    # report a failure of the callback itself, so it must not surface.
    with MockConsumer("earliest") as c:
        _seeded(c)
        fired = []

        def callback(offsets, exception):
            fired.append(offsets)
            raise RuntimeError("callback blew up")

        c.commit_async(callback=callback)  # must not raise
        assert len(fired) == 1
        c.commit_async(callback=lambda o, e: fired.append(o))
        assert len(fired) == 2


def test_commit_async_callback_must_be_callable():
    with MockConsumer("earliest") as c:
        with pytest.raises(TypeError):
            c.commit_async(callback="not callable")


def test_commit_async_callback_can_use_handle():
    with MockConsumer("earliest") as c:
        _seeded(c)
        handle = c.handle()
        seen = {}

        def callback(offsets, exception):
            seen["assignment"] = handle.assignment()

        try:
            c.commit_async(callback=callback)
        finally:
            handle.destroy()
        assert seen["assignment"] == set()


# -- ConsumerHandle ----------------------------------------------------------

def test_handle_getters_are_empty_on_a_mock():
    with MockConsumer("earliest") as c:
        with c.handle() as handle:
            assert isinstance(handle, ConsumerHandle)
            assert handle.assignment() == set()
            assert handle.subscription() == set()
            assert handle.paused() == set()
            handle.wakeup()  # no-op, must not raise


def test_handle_blocking_ops_are_unsupported_on_a_mock():
    tp = TopicPartition("t", 0)
    with MockConsumer("earliest") as c:
        with c.handle() as handle:
            for name, call in [
                ("assign", lambda: handle.assign([tp])),
                ("seek", lambda: handle.seek(tp, 0)),
                ("seek_with_metadata", lambda: handle.seek(tp, OffsetAndMetadata(1, "m"))),
                ("seek_to_beginning", lambda: handle.seek_to_beginning([tp])),
                ("seek_to_end", lambda: handle.seek_to_end([tp])),
                ("pause", lambda: handle.pause([tp])),
                ("resume", lambda: handle.resume([tp])),
                ("position", lambda: handle.position(tp)),
                ("position_timeout", lambda: handle.position(tp, timeout=1.0)),
                ("committed", lambda: handle.committed([tp])),
                ("beginning_offsets", lambda: handle.beginning_offsets([tp])),
                ("end_offsets", lambda: handle.end_offsets([tp])),
                ("offsets_for_times", lambda: handle.offsets_for_times({tp: 0})),
                ("commit_sync", lambda: handle.commit_sync()),
                ("commit_sync_offsets",
                 lambda: handle.commit_sync({tp: OffsetAndMetadata(1)})),
                ("commit_async", lambda: handle.commit_async()),
                ("commit_async_offsets",
                 lambda: handle.commit_async({tp: OffsetAndMetadata(1)})),
            ]:
                with pytest.raises(KafkaError) as exc_info:
                    call()
                assert "not supported on a MockConsumer handle" in str(exc_info.value), \
                    f"{name} reported an unexpected error"


def test_handle_destroy_is_idempotent_and_use_after_destroy_raises():
    with MockConsumer("earliest") as c:
        handle = c.handle()
        handle.destroy()
        handle.destroy()  # no error
        with pytest.raises(RuntimeError):
            handle.assignment()
        with pytest.raises(RuntimeError):
            handle.commit_sync()


def test_handle_shares_state_with_a_real_consumer():
    """Exercises the non-mock arm without a broker: the handle shares the
    consumer's SubscriptionState, so a subscribe on the consumer is visible
    through the handle, and position() on an unassigned partition fails
    immediately (no broker round trip)."""
    c = KafkaConsumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "handle-test",
        "group.protocol": "consumer",
    })
    try:
        c.subscribe(["handle-topic"])
        with c.handle() as handle:
            assert handle.subscription() == {"handle-topic"}
            assert handle.assignment() == set()
            with pytest.raises(KafkaError) as exc_info:
                handle.position(TopicPartition("handle-topic", 0))
            assert "partitions assigned to this consumer" in str(exc_info.value)
            handle.wakeup()
    finally:
        c.close()


def test_handle_after_consumer_closed_raises():
    c = MockConsumer("earliest")
    c.close()
    with pytest.raises(RuntimeError):
        c.handle()


# -- async consumer ----------------------------------------------------------

async def _rebalance_off_loop(consumer, partitions):
    """Drive MockConsumer.rebalance from an executor: it is a blocking call that
    parks until the listener returns, so it must not run on the event loop (a
    coroutine listener has to be scheduled onto that loop)."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, consumer.rebalance, partitions)


async def test_async_coroutine_listener():
    c = AsyncMockConsumer("earliest")
    events = []

    class CoroListener:
        async def on_partitions_revoked(self, partitions):
            events.append(("revoked", _tps(partitions)))

        async def on_partitions_assigned(self, partitions):
            events.append(("assigned", _tps(partitions)))

    await c.subscribe(["t"], CoroListener())
    await _rebalance_off_loop(c, [TopicPartition("t", 0)])
    assert events == [("assigned", [("t", 0)])]
    await _rebalance_off_loop(c, [TopicPartition("t", 1)])
    assert events[1:] == [("revoked", [("t", 0)]), ("assigned", [("t", 1)])]
    await c.close()


async def test_async_plain_callable_listener():
    # A non-coroutine listener works on the async consumer too; it just runs
    # directly on the dispatcher thread.
    c = AsyncMockConsumer("earliest")
    listener = RecordingListener()
    await c.subscribe(["t"], listener)
    await _rebalance_off_loop(c, [TopicPartition("t", 0)])
    assert listener.events == [("assigned", [("t", 0)])]
    await c.close()


async def test_async_coroutine_listener_exception_propagates():
    c = AsyncMockConsumer("earliest")

    class CoroBoom:
        async def on_partitions_revoked(self, partitions):
            pass

        async def on_partitions_assigned(self, partitions):
            raise ValueError("coro-boom")

    await c.subscribe(["t"], CoroBoom())
    with pytest.raises(KafkaError) as exc_info:
        await _rebalance_off_loop(c, [TopicPartition("t", 0)])
    assert str(exc_info.value) == "coro-boom"
    await c.close()


async def test_async_coroutine_listener_can_use_handle():
    c = AsyncMockConsumer("earliest")
    handle = c.handle()
    seen = {}

    class CoroUsesHandle:
        async def on_partitions_revoked(self, partitions):
            pass

        async def on_partitions_assigned(self, partitions):
            seen["subscription"] = handle.subscription()

    try:
        await c.subscribe(["t"], CoroUsesHandle())
        await _rebalance_off_loop(c, [TopicPartition("t", 0)])
    finally:
        handle.destroy()
    assert seen["subscription"] == set()
    await c.close()


async def test_async_commit_async_callback():
    c = AsyncMockConsumer("earliest")
    tp = TopicPartition("t", 0)
    await c.assign([tp])
    c.add_record("t", 0, 0, b"k", b"v")
    c.update_beginning_offsets("t", 0, 0)
    await c.poll(1.0)
    seen = []
    # commitAsync does not block in Java, so it stays a plain method here.
    c.commit_async(callback=lambda offsets, exc: seen.append((offsets, exc)))
    (offsets, exc), = seen
    assert exc is None
    assert offsets == {tp: OffsetAndMetadata(1, "", None)}
    await c.close()
