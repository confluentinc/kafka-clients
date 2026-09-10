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

"""Consumer callback / rebalance-listener contracts on the new
``confluent_kafka.consumer`` surface.

Migrated from the retired ``test_consumer_callbacks.py`` (legacy flat API with a
public ``ConsumerHandle`` / ``handle()`` object). The rebalance-listener core and
the §31 reentrancy / rebalance-blocking regressions already live in
``test_consumer_family.py``; this module carries the remaining
``OffsetCommitCallback`` / listener contracts whose subject survives:

- the ``commit_nowait`` / ``on_commit`` payload contract (Java
  ``OffsetCommitCallback.onComplete``), sync and async;
- a raising ``on_commit`` is swallowed (``onComplete`` returns void);
- ``on_commit`` must be callable;
- a coroutine ``on_commit`` on the **sync** consumer is reported (logged), not
  silently dropped;
- non-``str`` offset metadata fails the whole commit and commits nothing, while
  the default (empty-string) metadata is accepted;
- ``ConsumerRebalanceListener.on_partitions_lost`` defaults to
  ``on_partitions_revoked`` (Java default);
- the listener's registration lifetime (retained across ``unsubscribe``,
  released by a replacing / listener-less ``subscribe`` or by ``close``).

Dropped (subject removed by the spec): every test targeting the public
``ConsumerHandle`` / ``c.handle()`` object API (getters, ``destroy()``,
use-after-destroy, blocking-ops-unsupported), and the two seek FFI-routing /
liveness plumbing tests — the user-visible reentrancy behavior is covered by
``test_consumer_family.py``'s reentrancy suite. The listener-missing-a-method
rejection is also dropped: the new ``ConsumerRebalanceListener`` is a
subclassable class with no-op defaults (not a Java abstract interface), so a
partial subclass is legal and inherits the defaults.
"""

from __future__ import annotations

import asyncio
import gc
import weakref

import pytest

from confluent_kafka.consumer import (
    ConsumerRebalanceListener,
    ConsumerRecord,
    MockConsumer,
    OffsetAndMetadata,
)
from confluent_kafka.common.topic_partition import TopicPartition


def _tp(topic: str, partition: int) -> TopicPartition:
    return TopicPartition(topic=topic, partition=partition)


def _assigned_mock(*, seeded: int = 0) -> MockConsumer:
    """A MockConsumer assigned to ``t-0``. With ``seeded=N`` it also adds and
    consumes ``N`` records, so there is a current position (N) to commit — the
    legacy ``_seeded`` helper."""
    c = MockConsumer(offset_reset_strategy="earliest")
    tp = _tp("t", 0)
    c.assign(partitions=[tp])
    c.update_beginning_offsets(offsets={tp: 0})
    for i in range(seeded):
        c.add_record(record=ConsumerRecord(
            topic="t", partition=0, offset=i, key=b"k", value=b"v"))
    if seeded:
        c.poll(timeout=1.0)
    return c


# ---------------------------------------------------------------------------
# OffsetCommitCallback payload contract (commit_nowait / on_commit)
# ---------------------------------------------------------------------------

def test_commit_nowait_callback_receives_current_positions():
    c = _assigned_mock(seeded=1)  # consumed 1 record -> current position is 1
    tp = _tp("t", 0)
    seen = []
    c.commit_nowait(on_commit=lambda offsets, exc: seen.append((offsets, exc)))
    assert len(seen) == 1
    offsets, exc = seen[0]
    assert exc is None
    assert offsets[tp].offset() == 1
    c.close()


def test_commit_nowait_callback_receives_explicit_offsets():
    c = _assigned_mock()
    tp = _tp("t", 0)
    seen = []
    c.commit_nowait(offsets={tp: OffsetAndMetadata(offset=7, metadata="meta")},
                    on_commit=lambda offsets, exc: seen.append((offsets, exc)))
    assert len(seen) == 1
    offsets, exc = seen[0]
    assert exc is None
    assert offsets[tp].offset() == 7
    assert offsets[tp].metadata() == "meta"
    assert c.committed(partitions=[tp])[tp].offset() == 7
    c.close()


def test_commit_nowait_without_a_callback():
    c = _assigned_mock()
    tp = _tp("t", 0)
    c.commit_nowait(offsets={tp: OffsetAndMetadata(offset=4)})
    assert c.committed(partitions=[tp])[tp].offset() == 4
    c.close()


def test_commit_nowait_callback_exception_is_swallowed():
    # Java onComplete returns void; a raising callback must not surface, and the
    # next commit still fires its own callback.
    c = _assigned_mock()

    def boom(offsets, exc):
        raise RuntimeError("callback blew up")

    c.commit_nowait(on_commit=boom)  # must not raise

    seen = []
    c.commit_nowait(on_commit=lambda offsets, exc: seen.append(exc))
    assert seen == [None]
    c.close()


def test_commit_nowait_callback_must_be_callable():
    c = _assigned_mock()
    with pytest.raises(TypeError):
        c.commit_nowait(on_commit="not callable")
    c.close()


def test_coroutine_commit_callback_on_sync_consumer_is_reported(caplog):
    # A coroutine on_commit on the SYNC consumer cannot be awaited; it is caught
    # when it fires and logged (not silently dropped), while the commit lands.
    c = _assigned_mock()
    tp = _tp("t", 0)

    async def acb(offsets, exc):
        pass

    with caplog.at_level("ERROR"):
        c.commit_nowait(offsets={tp: OffsetAndMetadata(offset=9)}, on_commit=acb)
    # The failure is logged (message + the "requires an AsyncConsumer" cause in
    # the traceback), not raised.
    assert any(
        "commit callback" in rec.getMessage()
        or "AsyncConsumer" in (rec.exc_text or "")
        or (rec.exc_info and "AsyncConsumer" in str(rec.exc_info[1]))
        for rec in caplog.records
    )
    # the commit itself still happened
    assert c.committed(partitions=[tp])[tp].offset() == 9
    c.close()


# ---------------------------------------------------------------------------
# Offset metadata type validation at commit time
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("entry_point", ["commit", "commit_nowait"])
def test_non_str_offset_metadata_is_rejected(entry_point):
    c = _assigned_mock()
    tp = _tp("t", 0)
    offsets = {tp: OffsetAndMetadata(offset=5, metadata=123)}  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="metadata must be str or None"):
        getattr(c, entry_point)(offsets=offsets)
    # nothing was committed; a subsequent valid commit works.
    c.commit(offsets={tp: OffsetAndMetadata(offset=5)})
    assert c.committed(partitions=[tp])[tp].offset() == 5
    c.close()


def test_default_offset_metadata_is_accepted():
    c = _assigned_mock()
    tp = _tp("t", 0)
    c.commit(offsets={tp: OffsetAndMetadata(offset=6)})  # metadata "" default
    assert c.committed(partitions=[tp])[tp].offset() == 6
    c.close()


# ---------------------------------------------------------------------------
# ConsumerRebalanceListener.on_partitions_lost default
# ---------------------------------------------------------------------------

def test_on_partitions_lost_defaults_to_revoked():
    class Recorder(ConsumerRebalanceListener):
        def __init__(self):
            self.events = []

        def on_partitions_revoked(self, partitions):
            self.events.append(("revoked", [(p.topic(), p.partition()) for p in partitions]))

        def on_partitions_assigned(self, partitions):
            self.events.append(("assigned", [(p.topic(), p.partition()) for p in partitions]))

    listener = Recorder()
    result = listener.on_partitions_lost([_tp("t", 0)])
    # The default may be sync or return a coroutine (the base is Java-faithful);
    # drive it either way.
    if asyncio.iscoroutine(result):
        asyncio.new_event_loop().run_until_complete(result)
    assert listener.events == [("revoked", [("t", 0)])]


# ---------------------------------------------------------------------------
# Listener registration lifetime (retained across unsubscribe; released by a
# replacing / listener-less subscribe or by close)
# ---------------------------------------------------------------------------

def _listener():
    class L(ConsumerRebalanceListener):
        def on_partitions_revoked(self, partitions):
            pass

        def on_partitions_assigned(self, partitions):
            pass

    return L()


def test_listener_retained_while_subscribed():
    c = MockConsumer(offset_reset_strategy="earliest")
    listener = _listener()
    ref = weakref.ref(listener)
    c.subscribe(topics=["t"], listener=listener)
    del listener
    gc.collect()
    assert ref() is not None  # the consumer keeps it alive
    c.close()


def test_listener_released_by_a_listenerless_subscribe():
    c = MockConsumer(offset_reset_strategy="earliest")
    listener = _listener()
    ref = weakref.ref(listener)
    c.subscribe(topics=["t"], listener=listener)
    del listener
    c.subscribe(topics=["t"])  # listener-less: releases the old one
    gc.collect()
    assert ref() is None
    c.close()


def test_listener_released_by_a_replacing_subscribe():
    c = MockConsumer(offset_reset_strategy="earliest")
    first = _listener()
    ref = weakref.ref(first)
    c.subscribe(topics=["t"], listener=first)
    del first
    c.subscribe(topics=["t"], listener=_listener())  # replace: releases the old
    gc.collect()
    assert ref() is None
    c.close()


def test_listener_survives_unsubscribe_but_is_released_by_close():
    c = MockConsumer(offset_reset_strategy="earliest")
    listener = _listener()
    ref = weakref.ref(listener)
    c.subscribe(topics=["t"], listener=listener)
    del listener
    c.unsubscribe()
    gc.collect()
    assert ref() is not None  # unsubscribe retains the listener (Java-faithful)
    c.close()
    gc.collect()
    assert ref() is None  # close releases it


# ---------------------------------------------------------------------------
# Async peer: commit_nowait / on_commit on the AsyncMockConsumer
# ---------------------------------------------------------------------------

async def test_async_commit_nowait_callback_receives_explicit_offsets():
    from confluent_kafka.consumer import AsyncMockConsumer

    c = AsyncMockConsumer(offset_reset_strategy="earliest")
    tp = _tp("t", 0)
    await c.assign(partitions=[tp])
    c.update_beginning_offsets(offsets={tp: 0})
    seen = []
    # commit_nowait is a plain def on both classes (rule 4.12), so no await.
    c.commit_nowait(offsets={tp: OffsetAndMetadata(offset=3, metadata="m")},
                    on_commit=lambda offsets, exc: seen.append((offsets, exc)))
    await asyncio.sleep(0.1)
    assert len(seen) == 1
    offsets, exc = seen[0]
    assert exc is None
    assert offsets[tp].offset() == 3
    assert offsets[tp].metadata() == "m"
    await c.close()
