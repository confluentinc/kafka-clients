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

"""A Ctrl+C while ``AsyncMockConsumer`` hands its monitor to the awaiting task.

``rebalance``, ``lose_partitions``, ``poll`` with an ``async def`` poll task and
``commit_nowait`` with an ``async def`` callback hold the mock's monitor for
the task while they await (``_Monitor``). A ``KeyboardInterrupt`` landing right
after the hand-over (here, as the monitor's ``__exit__`` returns) or at it
(before the owner is set) must leave the monitor with no owner; otherwise every
later call from another task raises ``ConcurrentModificationError`` for good."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.consumer import AsyncMockConsumer, ConsumerRebalanceListener
from confluent_kafka.consumer import _mock_core

_TP = TopicPartition(topic="t", partition=0)


class _AsyncListener(ConsumerRebalanceListener):
    async def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:  # type: ignore[override]
        await asyncio.sleep(0)

    async def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:  # type: ignore[override]
        await asyncio.sleep(0)

    async def on_partitions_lost(self, partitions: set[TopicPartition]) -> None:  # type: ignore[override]
        await asyncio.sleep(0)


async def _ready(consumer: AsyncMockConsumer[str, str], site: str
                 ) -> Callable[[], Awaitable[object]]:
    """Prepare ``consumer`` for ``site``; the call that hands the monitor over."""
    await consumer.subscribe(topics=["t"], callback=_AsyncListener())
    if site == "rebalance":
        return lambda: consumer.rebalance(new_assignment=[_TP])
    await consumer.rebalance(new_assignment=[_TP])
    if site == "lose_partitions":
        return lambda: consumer.lose_partitions(partitions_lost=[_TP])
    if site == "poll":
        consumer.update_beginning_offsets(new_offsets={_TP: 0})

        async def task() -> None:
            await asyncio.sleep(0)

        consumer.schedule_poll_task(task=task)
        return lambda: consumer.poll(timeout=0)
    assert site == "commit_nowait"

    async def callback(**_kwargs: Any) -> None:
        await asyncio.sleep(0)

    return lambda: consumer.commit_nowait(callback=callback)


@pytest.mark.parametrize("at", ["after", "before"])
@pytest.mark.parametrize("site", ["rebalance", "lose_partitions", "poll", "commit_nowait"])
def test_an_interrupt_at_the_hand_over_leaves_the_monitor_free(
        site: str, at: str, monkeypatch: pytest.MonkeyPatch) -> None:
    async def main() -> None:
        consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(
            offset_reset_strategy="earliest")
        call = await _ready(consumer, site)
        armed = [True]
        if at == "after":
            real_exit = _mock_core._Monitor.__exit__

            def exit_then_interrupt(self: Any, *exc: object) -> None:
                real_exit(self, *exc)
                if armed[0] and self.owner is not None and self.owner is asyncio.current_task():
                    armed[0] = False
                    raise KeyboardInterrupt

            monkeypatch.setattr(_mock_core._Monitor, "__exit__", exit_then_interrupt)
        else:
            real_current = _mock_core._current_task

            def interrupt_once() -> asyncio.Task[Any] | None:
                if armed[0]:
                    armed[0] = False
                    raise KeyboardInterrupt
                return real_current()

            monkeypatch.setattr(_mock_core, "_current_task", interrupt_once)
        with pytest.raises(KeyboardInterrupt):
            await call()
        monkeypatch.undo()
        assert not armed[0], "the interrupt was not injected"
        assert consumer._lock.owner is None

        async def from_another_task() -> set[TopicPartition]:
            return consumer.assignment()

        # Another task enters the monitor: no ConcurrentModificationError.
        await asyncio.create_task(from_another_task())
        await consumer.close()

    asyncio.run(main())
