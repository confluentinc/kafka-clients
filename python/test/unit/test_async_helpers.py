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

"""Tests for ``confluent_kafka._async``'s single-drainer helpers: the event
loop an async client is bound to, the helper thread pumping once that loop has
closed, and the hop onto the loop."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from confluent_kafka._async import LoopBinding, OffLoopPump, call_soon_on


def test_a_binding_takes_the_loop_running_at_construction() -> None:
    async def main() -> None:
        binding = LoopBinding("Client")
        assert binding.loop is asyncio.get_running_loop()
        assert binding.bind() is asyncio.get_running_loop()

    asyncio.run(main())


def test_a_binding_made_off_loop_takes_the_loop_of_its_first_call() -> None:
    binding = LoopBinding("Client")
    assert binding.loop is None

    async def bind() -> asyncio.AbstractEventLoop:
        return binding.bind()

    loop = asyncio.new_event_loop()
    try:
        assert loop.run_until_complete(bind()) is loop
        assert binding.loop is loop
        assert loop.run_until_complete(bind()) is loop
        with pytest.raises(RuntimeError) as err:
            asyncio.run(bind())
        assert str(err.value) == "Client is bound to a different event loop"
        assert binding.loop is loop
    finally:
        loop.close()


def test_a_binding_outside_a_running_loop_raises() -> None:
    with pytest.raises(RuntimeError):
        LoopBinding("Client").bind()


def test_the_off_loop_pump_runs_one_pump_at_a_time_and_misses_no_request() -> None:
    # A request while the helper pumps makes it pump once more instead of
    # starting a second thread, so the drain keeps a single drainer.
    lock = threading.Lock()
    running = 0
    overlaps = 0
    runs: list[str] = []
    entered = threading.Event()
    proceed = threading.Event()

    def pump() -> None:
        nonlocal running, overlaps
        with lock:
            running += 1
            overlaps += running > 1
        runs.append(threading.current_thread().name)
        if len(runs) == 1:
            entered.set()
            proceed.wait()
        with lock:
            running -= 1

    helper = OffLoopPump(pump, "test-pump")
    helper.start()
    assert entered.wait(5)
    helper.start()
    helper.start()
    proceed.set()
    deadline = time.monotonic() + 5
    while len(runs) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.1)
    # The two requests made while the first pump ran are served by one more.
    assert runs == ["test-pump", "test-pump"]
    assert overlaps == 0
    helper.start()
    deadline = time.monotonic() + 5
    while len(runs) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(runs) == 3


def test_the_off_loop_pump_logs_a_raising_pump_and_goes_on(
        caplog: pytest.LogCaptureFixture) -> None:
    done = threading.Event()
    calls = 0

    def pump() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("boom")
        done.set()

    helper = OffLoopPump(pump, "test-pump")
    with caplog.at_level("ERROR", logger="confluent_kafka"):
        helper.start()
        deadline = time.monotonic() + 5
        while not caplog.records and time.monotonic() < deadline:
            time.sleep(0.01)
        helper.start()
        assert done.wait(5)
    assert [r.getMessage() for r in caplog.records] == ["Error running the callback pump"]


def test_call_soon_on_schedules_from_the_loop_and_from_another_thread() -> None:
    async def main() -> list[str]:
        loop = asyncio.get_running_loop()
        ran: list[str] = []
        done = loop.create_future()

        def record(name: str) -> None:
            ran.append(name)
            if len(ran) == 2:
                done.set_result(None)

        call_soon_on(loop, record, "on the loop")
        thread = threading.Thread(target=call_soon_on, args=(loop, record, "elsewhere"))
        thread.start()
        thread.join()
        await done
        return ran

    assert sorted(asyncio.run(main())) == ["elsewhere", "on the loop"]


def test_call_soon_on_a_closed_loop_raises() -> None:
    loop = asyncio.new_event_loop()
    loop.close()
    with pytest.raises(RuntimeError):
        call_soon_on(loop, lambda: None)
