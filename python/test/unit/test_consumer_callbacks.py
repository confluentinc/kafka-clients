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
inline — the deterministic, broker-free equivalent of a real rebalance. On the
synchronous consumer the listener runs on the thread calling ``rebalance``,
inside that call (the sync mock's ``rebalance`` is the one operation still on
a blocking entry point; a listener driven by ``poll`` & co. runs on the calling
thread too, inside the waiter's drain of the client's callbacks vector); on
the asyncio consumer ``rebalance`` is a coroutine over the ``_cb`` twin and the
listener invocation is pumped on the event loop. Its semantics constrain what
can be asserted here:

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
import inspect
import threading
import weakref

import _confluentkafka as _lib
import pytest
import consumer as kc
from consumer import (
    AsyncMockConsumer, ConsumerHandle, KafkaConsumer, MockConsumer,
    OffsetAndMetadata, TopicPartition,
)
from producer import KafkaError

WAIT = 2.0

# A generic Python exception raised from a listener (not a KafkaError subtype)
# is genuinely unmapped and surfaces in the bindings as UnknownServerError.
UNKNOWN_SERVER_ERROR = -1

# The access guard rejects a concurrent op with LocalConcurrentModificationError,
# which has its own dedicated FFI code rather than folding into
# UnknownServerError -- see kafka_common_ErrorCode_LOCAL_CONCURRENT_MODIFICATION
# in src/ffi/common.rs.
LOCAL_CONCURRENT_MODIFICATION = -2


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
    the same already-converted ``list[(topic, partition)]`` payload and the
    client's ``callback_id``. A ``None`` return means "report success now"."""
    import consumer as _cons

    without_lost = RecordingListener()
    assert _cons._ListenerAdapter(without_lost)._on_lost([("t", 3)], 0) is None
    assert without_lost.events == [("revoked", [("t", 3)])]

    with_lost = RecordingListenerWithLost()
    assert _cons._ListenerAdapter(with_lost)._on_lost([("t", 3)], 0) is None
    assert with_lost.events == [("lost", [("t", 3)])]


def test_coroutine_listener_is_rejected_on_the_sync_consumer():
    """The synchronous consumer has no event loop to run a coroutine listener
    on, so an ``async def`` listener method fails the rebalance (reported to the
    client like any listener exception) instead of being silently dropped."""

    class CoroListener:
        async def on_partitions_revoked(self, partitions):
            pass  # pragma: no cover

        async def on_partitions_assigned(self, partitions):
            pass  # pragma: no cover

    with MockConsumer("earliest") as c:
        c.subscribe(["t"], CoroListener())
        with pytest.raises(KafkaError, match="requires an AsyncConsumer"):
            c.rebalance([TopicPartition("t", 0)])


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
    deadlock: the callback runs on the thread driving the rebalance, inside the
    blocking ``rebalance`` call, and the handle ops bypass the consumer's access
    guard. On a MockConsumer the getters are empty and the commit is
    unsupported — what matters is that the calls return at all."""
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


# -- seek is a waited-for `_cb` op (sync) / a `_cb` coroutine (async) ---------

def test_seek_uses_the_cb_entry_points_on_both_classes():
    """``seek`` goes through the ``_cb`` twin on both classes, like every other
    operation that blocks in Rust: waited for in slices on ``Consumer``,
    awaited on ``AsyncConsumer``.

    Java's ``seek`` does not block, but ``AsyncKafkaConsumer::seek`` submits a
    ``SeekUnvalidatedEvent`` and drains background events, so it can invoke the
    rebalance listener (`consumer-threading.md` §31). The listener invocation is
    queued and run by whoever drains the client's callbacks vector: the calling
    thread on the synchronous consumer, the event loop on the asyncio one (a
    coroutine listener has to run there). Hence ``seek`` is per-class (plain on
    ``Consumer``, a coroutine on ``AsyncConsumer``) rather than a shared method
    on ``_ConsumerBase``, and the old ``*_async`` C entry points are gone. The
    blocking entry points stay exported for the C / gRPC path and for
    :class:`ConsumerHandle`, which must not wait on the pump it runs inside of.
    """
    for name in ("Consumer_seek_cb", "Consumer_seek_with_metadata_cb",
                 "ConsumerHandle_seek", "ConsumerHandle_seek_with_metadata"):
        assert hasattr(_lib, name), name
    assert not hasattr(_lib, "Consumer_seek_async")
    assert not hasattr(_lib, "Consumer_seek_with_metadata_async")
    assert "seek" not in vars(kc._ConsumerBase)
    assert not inspect.iscoroutinefunction(kc.Consumer.seek)
    assert inspect.iscoroutinefunction(kc.AsyncConsumer.seek)


def test_seek_while_a_listener_callback_is_being_dispatched():
    """Seek from a third thread while a listener callback is parked on the
    thread driving the rebalance.

    This is as close to Issue 1's deadlock as ``MockConsumer`` can get, and it is
    a **liveness check, not a discriminator**: the real reproduction needs the §31
    background-event machinery — a pending ``RebalanceListenerCallbackNeeded``
    drained by ``seek`` itself — which only the real ``AsyncKafkaConsumer`` has.
    On the mock the single-owner guard rejects the concurrent seek, so the
    broker-backed reproduction lives in the integration / multilanguage suites.

    What this does pin is that the sync routing stays live in that window:
    the parked listener has released the GIL (``Event.wait``), the seek submits
    its ``_cb`` twin and waits in Python slices, and the guard rejection --
    queued on the client's callbacks vector -- is drained by the seeking thread
    and raised as a ``KafkaError``, neither hanging nor losing the error, while
    Python keeps running on both other threads. That drain is free because the
    mock's ``rebalance`` invokes the listener directly (blocking entry point)
    rather than from the pump.
    """
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
    rebalanced = threading.Event()
    seeked = threading.Event()
    outcome = {}

    def drive_rebalance():
        c.rebalance([TopicPartition("t", 0)])
        rebalanced.set()

    def drive_seek():
        try:
            c.seek(TopicPartition("t", 0), 3)
            outcome["error"] = None
        except KafkaError as exc:
            outcome["error"] = exc
        seeked.set()

    rebalancer = threading.Thread(target=drive_rebalance)
    seeker = threading.Thread(target=drive_seek)
    rebalancer.start()
    try:
        assert entered.wait(WAIT), "listener should have been entered"
        seeker.start()
        assert seeked.wait(WAIT), "seek must not block the interpreter"
        # The listener is still parked, so the seek was rejected rather than
        # silently applied behind the in-flight rebalance.
        assert not rebalanced.is_set()
        assert outcome["error"] is not None
        assert outcome["error"].code == LOCAL_CONCURRENT_MODIFICATION
    finally:
        release.set()
        seeker.join(timeout=WAIT)
        rebalancer.join(timeout=WAIT)
        c.close()
    assert rebalanced.is_set(), "rebalance must complete once the listener returns"


def test_consumer_method_from_listener_is_rejected_as_concurrent():
    """Documents why ``handle()`` exists: the consumer's own methods are rejected
    while the operation that drove the callback still owns the access guard.
    Here the listener runs inside the mock's blocking ``rebalance``, so the
    rejection is the Rust guard's, delivered through the callbacks vector and
    drained by the seek's own wait (the pump is not busy)."""
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
    assert seen["error"].code == LOCAL_CONCURRENT_MODIFICATION
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


def test_coroutine_commit_callback_is_rejected_on_the_sync_consumer():
    """The synchronous consumer has no event loop to run a coroutine on, so an
    ``async def`` callback is rejected up front — before the commit is submitted.
    Silently creating and dropping the coroutine (the pre-fix behavior) loses the
    completion notification that Java's ``onComplete`` guarantees."""
    with MockConsumer("earliest") as c:
        tp = _seeded(c)

        async def callback(offsets, exception):
            pass  # pragma: no cover - must never run

        with pytest.raises(TypeError, match="requires an AsyncConsumer"):
            c.commit_async(callback=callback)
        with pytest.raises(TypeError, match="requires an AsyncConsumer"):
            c.commit_async({tp: OffsetAndMetadata(3)}, callback=callback)
        # Rejected before anything was committed.
        assert c.committed([tp]) == {}


def test_callable_returning_an_awaitable_is_reported_on_the_sync_consumer(caplog):
    """A plain callable that *returns* a coroutine is not recognizable up front
    (``iscoroutinefunction`` is False), so it is caught when it fires. There is
    nowhere to report it — Java's ``onComplete`` returns void — but it must be
    logged rather than silently discarded."""
    with MockConsumer("earliest") as c:
        tp = _seeded(c)

        async def body():
            pass  # pragma: no cover - must never run

        def callback(offsets, exception):
            return body()

        with caplog.at_level("ERROR", logger="consumer"):
            c.commit_async(callback=callback)  # must not raise
        assert "requires an AsyncConsumer" in caplog.text
        # The commit itself still happened; only the callback body could not run.
        assert c.committed([tp]) == {tp: OffsetAndMetadata(1, "", None)}


# -- malformed offsets: rejected, and nothing is committed -------------------
#
# All four entry points that take the (topic, partition, offset, epoch, metadata)
# shape share one marshaling helper. A metadata value that is not str/None must
# fail the whole call: converting it silently to "no metadata" would commit a
# different map than the caller passed, and would return success with a live
# exception set, which CPython later reports as an unrelated SystemError.

def _offsets_entry_points(consumer, handle):
    return {
        "commit_async_offsets": lambda offsets: consumer.commit_async(offsets),
        "commit_sync_offsets": lambda offsets: consumer.commit(offsets),
        "handle_commit_sync_offsets": lambda offsets: handle.commit_sync(offsets),
        "handle_commit_async_offsets": lambda offsets: handle.commit_async(offsets),
    }


@pytest.mark.parametrize("entry_point", [
    "commit_async_offsets", "commit_sync_offsets",
    "handle_commit_sync_offsets", "handle_commit_async_offsets",
])
def test_non_str_offset_metadata_is_rejected(entry_point):
    with MockConsumer("earliest") as c:
        tp = _seeded(c)
        handle = c.handle()
        try:
            commit = _offsets_entry_points(c, handle)[entry_point]
            with pytest.raises(TypeError,
                               match="metadata must be str or None, not int"):
                commit({tp: OffsetAndMetadata(5, 123)})
        finally:
            handle.destroy()
        # Nothing was committed, and no exception was left set for a later call
        # to trip over (pre-fix: the commit went through with metadata="" and the
        # next unrelated C call raised SystemError).
        assert c.committed([tp]) == {}
        c.commit_async({tp: OffsetAndMetadata(5, "ok")})
        assert c.committed([tp]) == {tp: OffsetAndMetadata(5, "ok", None)}


def test_none_offset_metadata_is_still_accepted():
    # metadata=None means "no metadata" and must keep working.
    with MockConsumer("earliest") as c:
        tp = _seeded(c)
        c.commit_async({tp: OffsetAndMetadata(5, None)})
        assert c.committed([tp]) == {tp: OffsetAndMetadata(5, "", None)}


def test_consumer_method_from_a_pumped_callback_is_rejected_without_hanging():
    """A commit callback is delivered by the waiter's drain of the client's
    callbacks vector (a `_cb` op queues the interface methods it triggers), so
    it runs *inside* the pump. A consumer method called from there cannot wait
    for the pump -- the Rust guard's rejection would be queued behind the very
    callback that is running, and a nested ``Consumer_execute_callbacks``
    returns 0 -- so the consumer raises the guard's error itself, up front,
    rather than waiting forever. ``close`` is rejected the same way and must
    leave the consumer open (destroying the handle from inside a callback
    would await the operation that is waiting for the callback)."""
    with MockConsumer("earliest") as c:
        tp = _seeded(c)
        seen = {}

        def callback(offsets, exception):
            seen["in_drain"] = c._waiter.in_drain()
            for name, call in [
                ("commit", lambda: c.commit()),
                ("position", lambda: c.position(tp)),
                ("close", lambda: c.close()),
            ]:
                try:
                    call()
                    seen[name] = None
                except KafkaError as exc:
                    seen[name] = exc

        c.commit_async(callback=callback)
        assert seen["in_drain"] is True
        for name in ("commit", "position", "close"):
            assert seen[name] is not None, f"{name} must be rejected from a callback"
            assert seen[name].code == LOCAL_CONCURRENT_MODIFICATION, name
            assert str(seen[name]) == "KafkaConsumer is not safe for multi-threaded access."
        # Rejected cleanly: the consumer is open, the pump idle, and it works.
        assert not c.closed
        assert not c._waiter.in_drain()
        assert c.committed([tp]) == {tp: OffsetAndMetadata(1, "", None)}


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
#
# AsyncMockConsumer.rebalance is a coroutine over the `_cb` twin: the mock
# invokes the listener on the Rust runtime, the invocation is queued on the
# client's callback queue, and the consumer's notify hook schedules the pump on
# the event loop. A coroutine listener method is scheduled there as a task and
# the rebalance completes only after its result has been reported back.

async def _settle(predicate, rounds=50):
    """Yield to the loop until ``predicate()`` holds (bounded)."""
    for _ in range(rounds):
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return predicate()


async def test_async_coroutine_listener():
    c = AsyncMockConsumer("earliest")
    events = []

    class CoroListener:
        async def on_partitions_revoked(self, partitions):
            events.append(("revoked", _tps(partitions)))

        async def on_partitions_assigned(self, partitions):
            events.append(("assigned", _tps(partitions)))

    await c.subscribe(["t"], CoroListener())
    await c.rebalance([TopicPartition("t", 0)])
    assert events == [("assigned", [("t", 0)])]
    await c.rebalance([TopicPartition("t", 1)])
    assert events[1:] == [("revoked", [("t", 0)]), ("assigned", [("t", 1)])]
    await c.close()


async def test_async_plain_callable_listener():
    # A non-coroutine listener works on the async consumer too; the pump just
    # runs it directly on the event loop thread.
    c = AsyncMockConsumer("earliest")
    listener = RecordingListener()
    await c.subscribe(["t"], listener)
    await c.rebalance([TopicPartition("t", 0)])
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
        await c.rebalance([TopicPartition("t", 0)])
    assert str(exc_info.value) == "coro-boom"
    assert exc_info.value.code == UNKNOWN_SERVER_ERROR
    await c.close()


async def test_async_rebalance_waits_for_the_coroutine_listener():
    """The rebalance must not complete before the coroutine listener has
    (consumer-threading.md §31 #2): hold the listener on an asyncio.Event and
    check the rebalance coroutine is still pending until it is released."""
    entered = asyncio.Event()
    release = asyncio.Event()
    c = AsyncMockConsumer("earliest")

    class Blocking:
        async def on_partitions_revoked(self, partitions):
            pass

        async def on_partitions_assigned(self, partitions):
            entered.set()
            await asyncio.wait_for(release.wait(), WAIT * 5)

    await c.subscribe(["t"], Blocking())
    task = asyncio.ensure_future(c.rebalance([TopicPartition("t", 0)]))
    try:
        await asyncio.wait_for(entered.wait(), WAIT)
        await asyncio.sleep(0.2)
        assert not task.done(), \
            "rebalance must not complete while the listener is still running"
        release.set()
        await asyncio.wait_for(task, WAIT)
        assert c.assignment() == {TopicPartition("t", 0)}
    finally:
        release.set()
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
                await handle.commit_sync()
                seen["commit_error"] = None
            except KafkaError as exc:
                seen["commit_error"] = str(exc)

    try:
        await c.subscribe(["t"], CoroUsesHandle())
        await c.rebalance([TopicPartition("t", 0)])
    finally:
        handle.destroy()
    assert isinstance(handle, kc.AsyncConsumerHandle)
    assert seen["subscription"] == set()
    assert "not supported on a MockConsumer handle" in seen["commit_error"]
    await c.close()


async def test_async_coroutine_commit_callback():
    """A coroutine commit callback is scheduled onto the consumer's loop by the
    pumped invocation -- the completion must not be silently dropped (CLAUDE.md
    §11.5). Java's ``onComplete`` is void, so nothing waits for the coroutine:
    yield to the loop until it has run."""
    c = AsyncMockConsumer("earliest")
    tp = TopicPartition("t", 0)
    await c.assign([tp])
    c.add_record("t", 0, 0, b"k", b"v")
    c.update_beginning_offsets("t", 0, 0)
    await c.poll(1.0)
    seen = []

    async def callback(offsets, exception):
        seen.append((offsets, exception))

    await c.commit_async(callback=callback)
    assert await _settle(lambda: seen), "the coroutine callback never ran"
    (offsets, exc), = seen
    assert exc is None
    assert offsets == {tp: OffsetAndMetadata(1, "", None)}
    await c.close()


async def test_async_commit_async_callback():
    c = AsyncMockConsumer("earliest")
    tp = TopicPartition("t", 0)
    await c.assign([tp])
    c.add_record("t", 0, 0, b"k", b"v")
    c.update_beginning_offsets("t", 0, 0)
    await c.poll(1.0)
    seen = []
    # A coroutine (the Rust commitAsync awaits the background task), completing
    # once the commit is initiated. The mock delivers the completion callback
    # within the same call, ahead of the completion itself, so the pump has run
    # it by the time the await returns.
    await c.commit_async(callback=lambda offsets, exc: seen.append((offsets, exc)))
    (offsets, exc), = seen
    assert exc is None
    assert offsets == {tp: OffsetAndMetadata(1, "", None)}
    await c.close()


async def test_async_callbacks_run_on_the_event_loop_thread():
    """Every pumped callback -- listener methods and commit callbacks alike --
    runs on the event loop thread, never on a Rust task."""
    c = AsyncMockConsumer("earliest")
    main = threading.get_ident()
    threads = []

    class Recording:
        def on_partitions_revoked(self, partitions):
            threads.append(threading.get_ident())

        def on_partitions_assigned(self, partitions):
            threads.append(threading.get_ident())

    await c.subscribe(["t"], Recording())
    await c.rebalance([TopicPartition("t", 0)])
    tp = TopicPartition("t", 0)
    await c.commit_async({tp: OffsetAndMetadata(1)},
                         callback=lambda o, e: threads.append(threading.get_ident()))
    assert len(threads) == 2
    assert set(threads) == {main}
    await c.close()
