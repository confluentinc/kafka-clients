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

import pytest
from consumer import (
    MockConsumer, AsyncMockConsumer, TopicPartition, OffsetAndMetadata,
)
from producer import KafkaError

# Generated from kafka_common_ErrorCode_t (cargo xtask generate-error-codes).
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


def test_value_is_memoryview():
    with MockConsumer("earliest") as c:
        _seed(c, records=[(b"k", b"value")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        assert isinstance(r.value, memoryview)
        assert isinstance(r.key, memoryview)


def test_memoryview_zero_copy_lifetime():
    """A memoryview must keep the underlying batch alive after the
    ConsumerRecords / ConsumerRecord that produced it are dropped."""
    with MockConsumer("earliest") as c:
        _seed(c, records=[(b"k", b"the-value")])
        recs = c.poll(POLL_TIMEOUT)
        rec = next(iter(recs))
        mv = rec.value
        assert bytes(mv) == b"the-value"
        # Drop every Python-visible owner except the memoryview itself.
        del rec
        del recs
        gc.collect()
        # The exporter still holds the batch alive: bytes remain valid.
        assert bytes(mv) == b"the-value"


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


# -- wakeup / signal interruption --------------------------------------------
#
# MockConsumer.poll returns immediately (it never blocks for the timeout, just
# like Java's MockConsumer). The wakeup / KeyboardInterrupt / cancellation
# paths can only be *triggered* when poll actually blocks, which requires a
# real KafkaConsumer + broker. So here we only assert wakeup is safe and
# non-disruptive; the blocking-interruption behavior is covered by integration
# tests against a live broker.

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


@pytest.mark.skip(reason="MockConsumer.poll does not block (matches Java); "
                         "Ctrl-C interruption requires a blocking KafkaConsumer poll")
def test_sigint_interrupts_sync_poll_and_consumer_reusable():
    with MockConsumer("earliest") as c:
        c.assign([TopicPartition("t", 0)])
        c.update_beginning_offsets("t", 0, 0)

        def fire():
            time.sleep(0.2)
            signal.raise_signal(signal.SIGINT)

        th = threading.Thread(target=fire)
        th.start()
        with pytest.raises(KeyboardInterrupt):
            c.poll(5.0)
        th.join()
        c.add_record("t", 0, 0, b"k", b"v")
        recs = c.poll(POLL_TIMEOUT)
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
