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

"""Test suite for the Python Kafka consumer bindings (MockConsumer-driven)."""

import asyncio
import gc
import signal
import threading
import time

import _confluentkafka as _lib
import pytest
from _sync_wait import SyncWaiter
from consumer import (
    MockConsumer, AsyncMockConsumer, TopicPartition, OffsetAndMetadata,
    ConsumerGroupMetadata,
)
from producer import KafkaError

# Generated from kafka_common_ErrorCode_e (python tools/generate_error_code.py).
# Private plumbing, not public API -- imported here because asserting the error
# code is the point: it identifies the class, which message text only hinted at.
import _error_code as ec

POLL_TIMEOUT = 1.0


def _seed(c, topic="t", partition=0, records=None):
    """Assign one partition, add records, set its beginning offset to 0."""
    tp = TopicPartition(topic, partition)
    c.assign([tp])
    for i, (k, v) in enumerate(records or []):
        c.add_record(topic, partition, i, k, v)
    c.update_beginning_offsets(topic, partition, 0)
    return tp


# -- lifecycle ---------------------------------------------------------------

def test_create_and_close():
    c = MockConsumer("earliest")
    c.close()
    assert c.closed


def test_close_idempotent():
    c = MockConsumer("earliest")
    c.close()
    c.close()  # no error


def test_context_manager():
    with MockConsumer("earliest") as c:
        assert not c.closed
    assert c.closed


def test_poll_after_close_raises():
    c = MockConsumer("earliest")
    c.close()
    with pytest.raises(RuntimeError):
        c.poll(POLL_TIMEOUT)


# -- sync poll ---------------------------------------------------------------

def test_poll_key_and_value():
    with MockConsumer("earliest") as c:
        _seed(c, records=[(b"k1", b"v1")])
        recs = c.poll(POLL_TIMEOUT)
        assert len(recs) == 1
        assert not recs.is_empty()
        (r,) = list(recs)
        assert r.topic == "t"
        assert r.partition == 0
        assert r.offset == 0
        assert bytes(r.key) == b"k1"
        assert bytes(r.value) == b"v1"


def test_poll_value_only():
    with MockConsumer("earliest") as c:
        _seed(c, records=[(None, b"v")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        assert r.key is None
        assert bytes(r.value) == b"v"


def test_poll_multiple_records_offsets():
    with MockConsumer("earliest") as c:
        _seed(c, records=[(b"k0", b"v0"), (b"k1", b"v1"), (b"k2", b"v2")])
        recs = list(c.poll(POLL_TIMEOUT))
        assert [r.offset for r in recs] == [0, 1, 2]
        assert [bytes(r.value) for r in recs] == [b"v0", b"v1", b"v2"]


def test_value_is_bytes():
    # Records come out of the FFI as kafka_Bytes_t views owned by the record
    # batch; the binding copies them into immutable bytes when the record
    # object is created, so a record never dangles into a freed batch.
    with MockConsumer("earliest") as c:
        _seed(c, records=[(b"k", b"value")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        assert isinstance(r.value, bytes)
        assert isinstance(r.key, bytes)
        assert r.headers == []


def test_bytes_lifetime_independent_of_batch():
    """The key/value bytes stay valid after the ConsumerRecords /
    ConsumerRecord that produced them are dropped."""
    with MockConsumer("earliest") as c:
        _seed(c, records=[(b"k", b"the-value")])
        recs = c.poll(POLL_TIMEOUT)
        rec = next(iter(recs))
        value = rec.value
        assert value == b"the-value"
        # Drop every Python-visible owner except the bytes themselves.
        del rec
        del recs
        gc.collect()
        # The batch is gone, the copied bytes remain valid.
        assert value == b"the-value"


def test_records_batch_views():
    with MockConsumer("earliest") as c:
        tp = _seed(c, records=[(b"k0", b"v0"), (b"k1", b"v1")])
        recs = c.poll(POLL_TIMEOUT)
        assert recs.partitions() == {tp}
        assert [r.offset for r in recs.records(tp)] == [0, 1]
        assert [r.offset for r in recs.records("t")] == [0, 1]
        assert recs.records(TopicPartition("other", 0)) == []
        # nextOffsets(): the position to resume from after this batch.
        assert recs.next_offsets()[tp].offset == 2


# -- commit / position / committed -------------------------------------------

def test_commit_and_committed():
    with MockConsumer("earliest") as c:
        tp = _seed(c, records=[(b"k", b"v")])
        list(c.poll(POLL_TIMEOUT))
        c.commit({tp: OffsetAndMetadata(5, "meta")})
        committed = c.committed([tp])
        assert committed[tp].offset == 5
        assert committed[tp].metadata == "meta"


def test_position():
    with MockConsumer("earliest") as c:
        tp = _seed(c, records=[(b"k", b"v")])
        list(c.poll(POLL_TIMEOUT))
        assert c.position(tp) == 1  # advanced past the single record


def test_commit_current_positions():
    with MockConsumer("earliest") as c:
        _seed(c, records=[(b"k", b"v")])
        list(c.poll(POLL_TIMEOUT))
        c.commit()  # commit current positions, no error


# -- seek --------------------------------------------------------------------
#
# seek awaits the consumer's background task in Rust like every other operation
# that blocks there, so it goes through the `_cb` twin on both classes: waited
# for in slices on Consumer, awaited as a coroutine on AsyncConsumer. See
# test_consumer_callbacks.test_seek_uses_the_cb_entry_points_on_both_classes.

def test_seek_int_offset():
    with MockConsumer("earliest") as c:
        tp = _seed(c, records=[(b"k", b"v")])
        c.seek(tp, 5)
        assert c.position(tp) == 5


def test_seek_offset_and_metadata():
    with MockConsumer("earliest") as c:
        tp = _seed(c, records=[(b"k", b"v")])
        c.seek(tp, OffsetAndMetadata(9, "m", 3))
        assert c.position(tp) == 9


def test_seek_after_close_raises():
    c = MockConsumer("earliest")
    tp = _seed(c)
    c.close()
    with pytest.raises(RuntimeError):
        c.seek(tp, 1)


async def test_async_seek_int_offset():
    async with AsyncMockConsumer("earliest") as c:
        tp = TopicPartition("t", 0)
        await c.assign([tp])
        c.update_beginning_offsets("t", 0, 0)
        await c.seek(tp, 5)
        assert await c.position(tp) == 5


async def test_async_seek_offset_and_metadata():
    async with AsyncMockConsumer("earliest") as c:
        tp = TopicPartition("t", 0)
        await c.assign([tp])
        c.update_beginning_offsets("t", 0, 0)
        await c.seek(tp, OffsetAndMetadata(9, "m", 3))
        assert await c.position(tp) == 9


# -- state reads -------------------------------------------------------------

def test_assignment_and_subscription():
    with MockConsumer("earliest") as c:
        tp = _seed(c)
        assert c.assignment() == {tp}
        # An assigned (not subscribed) consumer has an empty subscription.
        assert c.subscription() == set()


def test_beginning_and_end_offsets():
    with MockConsumer("earliest") as c:
        tp = _seed(c, records=[(b"k", b"v")])
        c.update_end_offsets("t", 0, 1)
        assert c.beginning_offsets([tp]) == {tp: 0}
        assert c.end_offsets([tp]) == {tp: 1}


def test_pause_resume():
    with MockConsumer("earliest") as c:
        tp = _seed(c)
        c.pause([tp])
        assert tp in c.paused()
        c.resume([tp])
        assert tp not in c.paused()


# -- the sync waiting model: `_cb` twins + SyncWaiter -------------------------
#
# The synchronous Consumer never calls a blocking C entry point: every method
# submits the `_cb` twin and waits in Python slices (SyncWaiter), draining the
# client's callbacks vector on the calling thread. These tests drive the two
# shared helpers (`_void` / `_value`) with stand-in `_cb` functions, so the
# waiting itself is exercised without a broker: MockConsumer completes every
# real operation instantly, leaving no window to observe a wait.

def test_sync_consumer_registers_the_waiter_notify_hook():
    with MockConsumer("earliest") as c:
        assert isinstance(c._waiter, SyncWaiter)
        assert c._notify_callable() == c._waiter.notify


def test_sync_value_op_waits_for_a_queued_completion():
    with MockConsumer("earliest") as c:
        calls = []

        def fake_value_cb(h, arg, cb):
            # Shape of every `_cb` twin: (h, *args, cb). Complete later, from
            # another thread, the way a Rust task does -- the waiter must sleep
            # until then and return the payload the completion carried.
            calls.append((h, arg))

            def fire():
                time.sleep(0.2)
                cb(42, None)
            threading.Thread(target=fire, daemon=True).start()

        t0 = time.monotonic()
        assert c._value(fake_value_cb, "arg") == 42
        assert 0.15 <= time.monotonic() - t0 < 2.0
        assert calls == [(c._h, "arg")]


def test_sync_void_op_accepts_an_inline_completion_and_raises_its_error():
    with MockConsumer("earliest") as c:
        c._void(lambda h, cb: cb(None))  # completed inline: no wait at all
        err_tuple = (ec.LOCAL_TIMEOUT, "took too long", 1, 0, 0)
        with pytest.raises(KafkaError) as exc:
            c._void(lambda h, cb: cb(err_tuple))
        assert exc.value.code == ec.LOCAL_TIMEOUT
        assert str(exc.value) == "took too long"


def test_sync_ops_are_rejected_once_closed_before_anything_is_submitted():
    c = MockConsumer("earliest")
    c.close()
    submitted = []
    with pytest.raises(RuntimeError):
        c._void(lambda h, cb: submitted.append(h))
    assert submitted == []


# -- wakeup / signal interruption --------------------------------------------
#
# MockConsumer.poll returns immediately (it never blocks for the timeout, just
# like Java's MockConsumer), so a *real* poll leaves no window for a signal to
# land inside the wait; the SIGINT test below therefore delays the completion
# of a real `Consumer_poll_cb` by hand. The wakeup flag semantics are the
# mock's own and are asserted directly.

def test_wakeup_sets_flag_consumed_by_next_poll():
    # wakeup() sets a pending flag; the next poll raises a Wakeup error (Java
    # WakeupException semantics) and clears it, after which poll works again.
    with MockConsumer("earliest") as c:
        tp = _seed(c, records=[(b"k", b"v")])
        c.wakeup()
        with pytest.raises(KafkaError) as exc:
            c.poll(POLL_TIMEOUT)
        assert exc.value.code == ec.WAKEUP
        recs = c.poll(POLL_TIMEOUT)
        assert len(recs) == 1
        assert c.assignment() == {tp}


def test_sigint_while_polling_wakes_up_once_then_reraises_and_consumer_reusable():
    """Ctrl-C while a sync call is waiting: the consumer calls ``wakeup()`` once,
    the in-flight operation completes with the Wakeup error (swallowed), and
    only then is ``KeyboardInterrupt`` raised -- so the single-owner guard is
    released and the payload freed. The consumer stays usable.

    The mock's poll never blocks, so the wait is staged: the submit hands the
    completion to the *real* ``Consumer_poll_cb`` only after 0.5 s, from a
    thread, while SIGINT is raised after 0.15 s. That late poll observes the
    wakeup the interrupt issued -- exactly what a blocking poll would do -- and
    completes with ``Wakeup``, which the following ``poll(0)`` must not see.
    """
    with MockConsumer("earliest") as c:
        tp = _seed(c, records=[(b"k", b"v")])
        completion = []
        callback_fired = threading.Event()

        def submit(cb):
            def fire():
                time.sleep(0.5)

                def record_then_deliver(value, err):
                    completion.append((value, err))
                    callback_fired.set()
                    cb(value, err)
                _lib.Consumer_poll_cb(c._h, 0, record_then_deliver)
            threading.Thread(target=fire, daemon=True).start()

        def raise_sigint():
            time.sleep(0.15)
            signal.raise_signal(signal.SIGINT)

        th = threading.Thread(target=raise_sigint)
        th.start()
        t0 = time.monotonic()
        with pytest.raises(KeyboardInterrupt):
            c._run(submit, c.wakeup)
        elapsed = time.monotonic() - t0
        th.join()

        # The interrupt was deferred until the completion had been drained ...
        assert callback_fired.is_set()
        assert 0.4 < elapsed < 1.5, elapsed
        # ... and the operation ended with the Wakeup the interrupt issued.
        (value, err), = completion
        assert value is None
        assert err is not None and err[0] == ec.WAKEUP

        # The consumer is still usable: the wakeup was consumed by the
        # interrupted operation, not left pending for the next one.
        assert c.assignment() == {tp}
        recs = c.poll(0)
        assert len(recs) == 1


# -- async API ---------------------------------------------------------------

async def test_async_poll_and_fields():
    async with AsyncMockConsumer("earliest") as c:
        await c.assign([TopicPartition("t", 0)])
        c.add_record("t", 0, 0, b"k", b"v")
        c.update_beginning_offsets("t", 0, 0)
        recs = await c.poll(POLL_TIMEOUT)
        assert len(recs) == 1
        (r,) = list(recs)
        assert bytes(r.value) == b"v"


async def test_async_commit_and_committed():
    async with AsyncMockConsumer("earliest") as c:
        tp = TopicPartition("t", 0)
        await c.assign([tp])
        c.add_record("t", 0, 0, b"k", b"v")
        c.update_beginning_offsets("t", 0, 0)
        list(await c.poll(POLL_TIMEOUT))
        await c.commit({tp: OffsetAndMetadata(3)})
        committed = await c.committed([tp])
        assert committed[tp].offset == 3


@pytest.mark.skip(reason="MockConsumer.poll does not block (matches Java); "
                         "cancellation requires a blocking KafkaConsumer poll")
async def test_async_poll_cancel_then_reusable():
    async with AsyncMockConsumer("earliest") as c:
        await c.assign([TopicPartition("t", 0)])
        c.update_beginning_offsets("t", 0, 0)
        task = asyncio.create_task(c.poll(5.0))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # Let the aborted poll's callback fire and release the guard.
        await asyncio.sleep(0.3)
        c.add_record("t", 0, 0, b"k", b"v")
        recs = await c.poll(POLL_TIMEOUT)
        assert len(recs) == 1


# -- ConsumerGroupMetadata ----------------------------------------------------
#
# ConsumerGroupMetadata comes only from Consumer.group_metadata(): Java
# deprecated its constructors in 4.2, so the type has none. It owns a live Rust
# handle freed in tp_dealloc; the tests below also assert it survives GC.

def test_group_metadata_is_not_constructible():
    with pytest.raises(TypeError):
        ConsumerGroupMetadata("g1", 7, "member-42", "instance-a")
    with pytest.raises(TypeError):
        ConsumerGroupMetadata()


def test_group_metadata_from_mock_consumer():
    # Java's MockConsumer.groupMetadata(): a dynamic member, so the absent
    # group_instance_id maps to None (Java's Optional.empty()).
    with MockConsumer("earliest") as c:
        gm = c.group_metadata()
    assert gm.group_id == "dummy.group.id"
    assert gm.generation_id == 1
    assert gm.member_id == "1"
    assert gm.group_instance_id is None


def test_group_metadata_repr_and_destruction():
    with MockConsumer("earliest") as c:
        gm = c.group_metadata()
    # repr matches the surface the former pure-Python dataclass produced, and
    # the handle outlives the consumer that handed it out.
    assert repr(gm) == (
        "ConsumerGroupMetadata(group_id='dummy.group.id', generation_id=1, "
        "member_id='1', group_instance_id=None)")
    # Dropping the only reference must free the owned handle without crashing.
    del gm
    gc.collect()
