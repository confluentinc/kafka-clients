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

"""Group rebalances against a broker (KIP-848), sync and asyncio:

- two members of one group over a two-partition topic split the partitions,
  the listener seeing the assignment and the revocation, and the partition a
  member revokes reaching the other member only once its
  ``on_partitions_revoked`` has returned;
- ``close()`` with ``CloseOptions`` ``LEAVE_GROUP`` hands the leaving member's
  partition to the other member at once, while a dynamic member closing with
  ``REMAIN_IN_GROUP`` sends no leave heartbeat
  (``ConsumerHeartbeatRequestManager.java:220-222``), so its partition stays
  assigned to it until the session times out;
- a static member (``group.instance.id``) closing with ``REMAIN_IN_GROUP``
  leaves temporarily (``LEAVE_GROUP_STATIC_MEMBER_EPOCH``,
  ``ConsumerMembershipManager.java:510-515``) and rejoining keeps its
  partitions.

Skips without Docker (``kafka_broker``).
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.consumer import CloseOptions, ConsumerRebalanceListener

from .broker_helpers import (
    DEADLINE, MODE, MODES, Consumer, aw, close_consumer, consumer_configs, make_consumer,
    new_topic, produce, unique,
)

Op = CloseOptions.GroupMembershipOperation

#: How long a revoking listener holds the rebalance.
HOLD_S = 2.0


class _Recording(ConsumerRebalanceListener):
    """Records ``(event, partitions, entered, returned)``, holding each
    non-empty revocation ``hold_s`` seconds once ``hold`` is set."""

    def __init__(self, hold_s: float = 0.0) -> None:
        self.events: list[tuple[str, frozenset[TopicPartition], float, float]] = []
        self.hold = threading.Event()
        self._hold_s = hold_s

    def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        entered = time.monotonic()
        if partitions and self.hold.is_set():
            time.sleep(self._hold_s)
        self.events.append(("revoked", frozenset(partitions), entered, time.monotonic()))

    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        now = time.monotonic()
        self.events.append(("assigned", frozenset(partitions), now, now))

    def non_empty(self, event: str) -> list[tuple[frozenset[TopicPartition], float, float]]:
        return [(p, a, b) for e, p, a, b in self.events if e == event and p]


class _BackgroundMember:
    """A group member polling on a thread of its own (an event loop of its own
    for the asyncio form), publishing its assignment after every poll."""

    def __init__(self, mode: str, configs: dict[str, Any], topic: str,
                 listener: ConsumerRebalanceListener) -> None:
        self.assignment: set[TopicPartition] = set()
        self.error: BaseException | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=lambda: asyncio.run(self._run(mode, configs, topic, listener)), daemon=True)
        self._thread.start()

    async def _run(self, mode: str, configs: dict[str, Any], topic: str,
                   listener: ConsumerRebalanceListener) -> None:
        consumer = make_consumer(mode, configs)
        try:
            await aw(consumer.subscribe(topics=[topic], callback=listener))
            while not self._stop.is_set():
                await aw(consumer.poll(timeout=0.2))
                self.assignment = consumer.assignment()
        except BaseException as exc:  # noqa: BLE001 - reported by stop()
            self.error = exc
        finally:
            await close_consumer(consumer)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(DEADLINE)
        assert not self._thread.is_alive(), "the member did not stop"
        assert self.error is None, f"the member failed: {self.error!r}"


def _partitions(topic: str) -> set[TopicPartition]:
    return {TopicPartition(topic=topic, partition=p) for p in (0, 1)}


async def _poll_both_until_split(a: Consumer, b: Consumer, topic: str) -> None:
    """Poll the two members in turn until each holds one partition."""
    end = time.monotonic() + DEADLINE
    while True:
        await aw(a.poll(timeout=0.2))
        await aw(b.poll(timeout=0.2))
        mine, theirs = a.assignment(), b.assignment()
        if len(mine) == 1 and len(theirs) == 1 and mine | theirs == _partitions(topic):
            return
        assert time.monotonic() < end, f"no split: {mine} / {theirs}"


async def _poll_until_assigned(consumer: Consumer, wanted: set[TopicPartition],
                               deadline: float = DEADLINE) -> float:
    """Poll until ``consumer`` holds ``wanted``; the seconds it took."""
    start = time.monotonic()
    while consumer.assignment() != wanted:
        assert time.monotonic() - start < deadline, f"assignment {consumer.assignment()}"
        await aw(consumer.poll(timeout=0.2))
    return time.monotonic() - start


@MODE
def test_two_members_split_the_partitions(kafka_broker: Any, mode: str) -> None:
    topic = new_topic(kafka_broker, "py-rebalance", partitions=2)
    configs = consumer_configs(kafka_broker)
    first_listener = _Recording(hold_s=HOLD_S)
    second_listener = _Recording()

    async def body() -> None:
        first = make_consumer(mode, configs)
        second: _BackgroundMember | None = None
        try:
            await aw(first.subscribe(topics=[topic], callback=first_listener))
            await _poll_until_assigned(first, _partitions(topic))
            # From now on a revocation holds the rebalance HOLD_S seconds.
            first_listener.hold.set()
            second = _BackgroundMember(mode, configs, topic, second_listener)
            end = time.monotonic() + DEADLINE
            while len(first.assignment()) != 1 or len(second.assignment) != 1:
                assert time.monotonic() < end, \
                    f"no split: {first.assignment()} / {second.assignment}"
                await aw(first.poll(timeout=0.2))
            assert first.assignment() | second.assignment == _partitions(topic)
        finally:
            if second is not None:
                second.stop()
            await close_consumer(first)

    asyncio.run(body())
    assigned = first_listener.non_empty("assigned")
    assert [p for p, _, _ in assigned] == [frozenset(_partitions(topic))]
    ((revoked, _entered, returned),) = first_listener.non_empty("revoked")[:1]
    assert len(revoked) == 1
    # The revoked partition reached the second member, and only once the first
    # member's listener had returned (consumer-threading.md §31).
    ((given, at, _),) = second_listener.non_empty("assigned")[:1]
    assert given == revoked
    assert at >= returned, f"assigned {returned - at:.3f}s before the revocation ended"


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("operation", [Op.LEAVE_GROUP, Op.REMAIN_IN_GROUP],
                         ids=["leave", "remain"])
def test_close_with_group_membership_operation(kafka_broker: Any, mode: str,
                                               operation: Op) -> None:
    topic = new_topic(kafka_broker, "py-close-op", partitions=2)
    configs = consumer_configs(kafka_broker)

    async def body() -> None:
        stays = make_consumer(mode, configs)
        leaves = make_consumer(mode, configs)
        try:
            await aw(stays.subscribe(topics=[topic]))
            await aw(leaves.subscribe(topics=[topic]))
            await _poll_both_until_split(stays, leaves, topic)
        finally:
            await close_consumer(leaves, timeout=10, operation=operation)
        try:
            # Both operations hand the partition over well inside the 45 s
            # session timeout (about 4 s here: the next heartbeat of the member
            # that stays). These are dynamic members, and a dynamic member's
            # REMAIN_IN_GROUP close leaves the group too, as in Java 4.4:
            # leaveGroupOnClose moves it to LEAVING with the leave epoch (-1),
            # and although shouldSendLeaveHeartbeatNow() skips the immediate
            # leave heartbeat, AbstractHeartbeatRequestManager.poll still sends
            # one because shouldHeartbeatNow() includes LEAVING.
            await _poll_until_assigned(stays, _partitions(topic), deadline=20)
        finally:
            await close_consumer(stays)

    asyncio.run(body())


@MODE
def test_static_member_rejoining_keeps_its_partitions(kafka_broker: Any, mode: str) -> None:
    topic = new_topic(kafka_broker, "py-static", partitions=2)
    group = unique("py-static-group")
    instance = unique("py-instance")

    def configs(instance_id: str) -> dict[str, Any]:
        return consumer_configs(kafka_broker, group_id=group, group_instance_id=instance_id)

    stays_listener = _Recording()

    async def body() -> None:
        stays = make_consumer(mode, configs(unique("py-instance")))
        try:
            await aw(stays.subscribe(topics=[topic], callback=stays_listener))
            member = make_consumer(mode, configs(instance))
            try:
                await aw(member.subscribe(topics=[topic]))
                await _poll_both_until_split(stays, member, topic)
                kept, held = stays.assignment(), member.assignment()
            finally:
                await close_consumer(member, timeout=10, operation=Op.REMAIN_IN_GROUP)
            revocations = len(stays_listener.non_empty("revoked"))
            # The static member's partition is held for it. Longer than one
            # heartbeat interval (5 s): a member that left for good would see
            # its partition reach the other member at its next heartbeat.
            end = time.monotonic() + 6.5
            while time.monotonic() < end:
                await aw(stays.poll(timeout=0.2))
                assert stays.assignment() == kept
            rejoined = make_consumer(mode, configs(instance))
            try:
                await aw(rejoined.subscribe(topics=[topic]))
                end = time.monotonic() + DEADLINE
                while rejoined.assignment() != held:
                    assert time.monotonic() < end, f"rejoined with {rejoined.assignment()}"
                    await aw(rejoined.poll(timeout=0.2))
                    await aw(stays.poll(timeout=0.2))
                assert stays.assignment() == kept
                assert len(stays_listener.non_empty("revoked")) == revocations
            finally:
                await close_consumer(rejoined, timeout=10, operation=Op.LEAVE_GROUP)
        finally:
            await close_consumer(stays)

    asyncio.run(body())


@MODE
def test_a_listener_reads_metadata_and_timed_offsets_through_the_handle(
        kafka_broker: Any, mode: str) -> None:
    # Inside on_partitions_assigned the consumer is called back through the
    # core's ConsumerHandle: partitions_for and a timed end_offsets answer
    # there (kafka_consumer_ConsumerHandle_partitions_for,
    # ..._end_offsets_with_timeout), as Java's listener calls the consumer.
    topic = new_topic(kafka_broker, "py-rebalance-handle", partitions=2)
    produce(kafka_broker, topic, [b"a", b"b", b"c"], partition=0)
    configs = consumer_configs(kafka_broker)
    seen: dict[str, Any] = {}
    consumer = make_consumer(mode, configs)

    class SyncListener(ConsumerRebalanceListener):
        def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
            if partitions:
                seen["partitions_for"] = consumer.partitions_for(topic=topic)
                seen["end_offsets"] = consumer.end_offsets(partitions=partitions, timeout=10.0)

        def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
            pass

    class AsyncListener(ConsumerRebalanceListener):
        async def on_partitions_assigned(  # type: ignore[override]
                self, partitions: set[TopicPartition]) -> None:
            if partitions:
                seen["partitions_for"] = await consumer.partitions_for(topic=topic)
                seen["end_offsets"] = await consumer.end_offsets(partitions=partitions,
                                                                 timeout=10.0)

        def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
            pass

    async def body() -> None:
        try:
            listener = SyncListener() if mode == "sync" else AsyncListener()
            await aw(consumer.subscribe(topics=[topic], callback=listener))
            await _poll_until_assigned(consumer, _partitions(topic))
        finally:
            await close_consumer(consumer)

    asyncio.run(body())
    assert sorted(p.partition() for p in seen["partitions_for"]) == [0, 1]
    assert seen["end_offsets"] == {TopicPartition(topic=topic, partition=0): 3,
                                   TopicPartition(topic=topic, partition=1): 0}
