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

"""A Ctrl+C while a mock consumer holds its monitor, or ``AsyncMockConsumer``
hands it to the awaiting task.

``rebalance``, ``lose_partitions``, ``poll`` with an ``async def`` poll task and
``commit_nowait`` with an ``async def`` callback hold the mock's monitor for
the task while they await (``_Monitor``). A ``KeyboardInterrupt`` landing right
after the hand-over (here, as the block that hands it over releases the lock) or at it
(before the owner is set) must leave the monitor with no owner; otherwise every
later call from another task raises ``ConcurrentModificationError`` for good."""

from __future__ import annotations

import asyncio
import random
import signal
import sys
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.consumer import AsyncMockConsumer, ConsumerRebalanceListener, MockConsumer
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
            real_rlock = consumer._lock.rlock

            class InterruptAfterHandOver:
                """The monitor's lock, raising once right after it is released by
                the block that handed the monitor over (the owner is then this
                task), as a signal handler does at the next check point."""

                def __enter__(self) -> bool:
                    return bool(real_rlock.__enter__())

                def __exit__(self, *exc: object) -> None:
                    real_rlock.__exit__(*exc)
                    if armed[0] and consumer._lock.owner is asyncio.current_task():
                        armed[0] = False
                        raise KeyboardInterrupt

                def __getattr__(self, name: str) -> Any:
                    return getattr(real_rlock, name)

            consumer._lock.rlock = InterruptAfterHandOver()  # type: ignore[assignment]
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
        if at == "after":
            consumer._lock.rlock = real_rlock
        assert not armed[0], "the interrupt was not injected"
        assert consumer._lock.owner is None

        async def from_another_task() -> set[TopicPartition]:
            return consumer.assignment()

        # Another task enters the monitor: no ConcurrentModificationError.
        await asyncio.create_task(from_another_task())
        await consumer.close()

    asyncio.run(main())


# --------------------------------------------------------------------------- #
# The sync MockConsumer: a Ctrl+C while a method holds the monitor's lock must
# not leave the lock held; Java's monitor cannot stay held after the method.
# --------------------------------------------------------------------------- #

WAIT = 10.0


def _lock_is_free(lock: Any) -> bool:
    """Whether another thread can take ``lock`` (a held RLock blocks it)."""
    taken: list[bool] = []

    def take() -> None:
        ok = lock.acquire(timeout=WAIT)
        taken.append(ok)
        if ok:
            lock.release()

    thread = threading.Thread(target=take, daemon=True)
    thread.start()
    thread.join(WAIT + 1)
    return taken == [True]


def _returns_from_another_thread(call: Callable[[], object]) -> bool:
    done = threading.Event()
    thread = threading.Thread(target=lambda: (call(), done.set()), daemon=True)
    thread.start()
    return done.wait(WAIT)


class _InterruptingRLock:
    """The monitor's lock, raising ``KeyboardInterrupt`` once on the test's
    thread: before its acquire (``acquire``) or after its release
    (``release``)."""

    def __init__(self, inner: Any, at: str) -> None:
        self._inner = inner
        self._at = at
        self._thread = threading.get_ident()
        self.fired = False

    def _fire(self, at: str) -> None:
        if self._at == at and not self.fired and threading.get_ident() == self._thread:
            self.fired = True
            raise KeyboardInterrupt

    def __enter__(self) -> bool:
        self._fire("acquire")
        return bool(self._inner.__enter__())

    def __exit__(self, *exc: object) -> None:
        self._inner.__exit__(*exc)
        self._fire("release")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _assigned_mock() -> MockConsumer[str, str]:
    consumer: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    consumer.assign(partitions=[_TP])
    return consumer


@pytest.mark.parametrize("at", ["acquire", "check", "release"])
@pytest.mark.parametrize("method", ["assignment", "paused"])
def test_an_interrupt_in_a_sync_mock_call_leaves_the_monitor_free(
        method: str, at: str, monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = _assigned_mock()
    real = consumer._lock.rlock
    fired = [False]
    if at == "check":
        real_check = _mock_core._Monitor.check

        def interrupt_once(self: Any) -> None:
            if not fired[0]:
                fired[0] = True
                raise KeyboardInterrupt
            real_check(self)

        monkeypatch.setattr(_mock_core._Monitor, "check", interrupt_once)
    else:
        wrapper = _InterruptingRLock(real, at)
        consumer._lock.rlock = wrapper  # type: ignore[assignment]
    try:
        with pytest.raises(KeyboardInterrupt):
            getattr(consumer, method)()
    finally:
        consumer._lock.rlock = real
        monkeypatch.undo()
    assert fired[0] or wrapper.fired
    assert _lock_is_free(real)
    assert _returns_from_another_thread(getattr(consumer, method))


STRESS_S = 3.0


# CPython 3.14 checks for signals after a `with` statement's `__enter__` returns
# and before its block starts, so a KeyboardInterrupt there skips `__exit__` and
# leaves the lock held (https://github.com/python/cpython/issues/148874, fixed
# in 3.15). These signals land there often enough to fail; a run that misses
# that point passes.
@pytest.mark.xfail(sys.version_info[:2] == (3, 14), strict=False,
                   reason="CPython gh-148874: on 3.14 a signal right after __enter__ skips __exit__")
@pytest.mark.skipif(not hasattr(signal, "pthread_kill"), reason="needs signal.pthread_kill")
@pytest.mark.parametrize("method", ["assignment", "paused"])
def test_real_sigint_leaves_the_sync_mock_monitor_free(method: str) -> None:
    """Real ``SIGINT`` signals while a sync mock call loops: the handler raises
    ``KeyboardInterrupt`` only while the call runs, at the interpreter's own
    check points; the lock is free afterwards and another thread's call
    returns."""
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    consumer = _assigned_mock()
    call = getattr(consumer, method)
    in_call = [False]

    def handler(signum: int, frame: object) -> None:
        if in_call[0]:
            raise KeyboardInterrupt

    previous = signal.signal(signal.SIGINT, handler)
    stop = threading.Event()
    main = threading.main_thread().ident
    assert main is not None

    def send() -> None:
        while not stop.is_set():
            signal.pthread_kill(main, signal.SIGINT)
            time.sleep(random.uniform(20e-6, 500e-6))

    sender = threading.Thread(target=send, daemon=True)
    interrupted = 0
    try:
        sender.start()
        deadline = time.monotonic() + STRESS_S
        while time.monotonic() < deadline:
            try:
                in_call[0] = True
                call()
            except KeyboardInterrupt:
                interrupted += 1
            finally:
                in_call[0] = False
    finally:
        stop.set()
        sender.join(WAIT)
        signal.signal(signal.SIGINT, previous)
    assert interrupted > 0, "no call was interrupted; the test did not run its case"
    assert _lock_is_free(consumer._lock.rlock)
    assert _returns_from_another_thread(call)
