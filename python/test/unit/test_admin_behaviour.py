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

"""The admin client family's Python-side behaviour that no Java test covers: the
callback pump under load, ``close()`` with calls in flight, a closing event loop,
the off-loop pump, Ctrl+C and task cancellation, silent unread failures, thread
safety and the release of every thread and object at ``close()`` (CLAUDE.md, Python
Binding Conventions, Threads and callbacks, Implementation over the FFI and
Behaviour with no other home).

Each scenario has a sync and an asyncio form. The real clients run against the
unreachable ``localhost:1``, so each call fails with the core's ``TimeoutError``
after its ``timeout_ms`` (``Timed out waiting for a node assignment``), or with
``The AdminClient thread has exited`` when ``close()`` aborts it; an empty
``create_topics`` batch is the one real call that succeeds, done at once in the
core, so its dependants still run where its delivery runs (the pump thread, the
loop). The mocks complete their futures inside the call. No test sleeps to
synchronise: waits are bounded joins, events and future reads.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import signal
import sys
import threading
import time
import warnings
import weakref
from collections.abc import Callable, Iterator
from types import FrameType
from typing import Any

import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.admin import (
    Admin,
    AsyncAdmin,
    AsyncMockAdminClient,
    CreateTopicsOptions,
    ListTopicsOptions,
    MockAdminClient,
    NewTopic,
)
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError

from .admin_futures import (
    WAIT,
    assert_future_throws,
    async_assert_future_throws,
    async_value,
    value,
)

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CLOSED = "Cannot accept new calls when AdminClient is closing."
LIST_TIMED_OUT = "Timed out waiting for a node assignment. Call: listTopics"
CREATE_TIMED_OUT = "Timed out waiting for a node assignment. Call: createTopics"
PUMP_THREAD = "kafka-admin-client-thread"
OFF_LOOP_PUMP = "confluent-kafka-admin-pump"
ADMIN_THREADS = (PUMP_THREAD, OFF_LOOP_PUMP, "kafka-admin-client-close")
SHORT = 300
# The call timeout of a call a waiting close() lets end, in ms.
GRACE = 800
LONG = 20000


def _exited(call: str) -> str:
    """The message of a call ``close()`` aborts (Java's
    ``KafkaAdminClient.AdminClientRunnable`` ``TimeoutException``)."""
    return f"The AdminClient thread has exited. Call: {call}"


def _admin() -> Admin:
    return Admin.create(configs=UNREACHABLE)


def _list(admin: Any, timeout_ms: int) -> Any:
    return admin.list_topics(options=ListTopicsOptions(timeout_ms=timeout_ms))


def _topic(name: str) -> NewTopic:
    return NewTopic(name=name, num_partitions=1, replication_factor=1)


def _admin_threads() -> set[threading.Thread]:
    return {t for t in threading.enumerate() if t.name in ADMIN_THREADS}


def _join_new_admin_threads(before: set[threading.Thread]) -> set[threading.Thread]:
    """Join the admin threads started since ``before`` (bounded); those still alive."""
    new = _admin_threads() - before
    for thread in new:
        thread.join(WAIT)
    return {t for t in new if t.is_alive()}


class _Calls:
    """Thread-safe record of callback invocations: ``(key, thread name)``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.seen: list[tuple[Any, str]] = []

    def add(self, key: Any, result: Any = None) -> Any:
        """Record ``key`` on the current thread; ``result`` is returned, so a
        ``then_apply`` function can record and answer at once."""
        with self._lock:
            self.seen.append((key, threading.current_thread().name))
        return result

    def function(self, key: Any, result: Any) -> Callable[[Any], Any]:
        """A ``then_apply`` function recording ``key`` and answering ``result``."""
        return lambda _value: self.add(key, result)

    def action(self, *key: Any) -> Callable[[Any, BaseException | None], None]:
        """A ``when_complete`` action recording ``(*key, value, error)``."""
        return lambda value, error: self.add((*key, value, error))

    def keys(self) -> list[Any]:
        with self._lock:
            return [k for k, _ in self.seen]

    def threads(self) -> set[str]:
        with self._lock:
            return {t for _, t in self.seen}


# ---- 1. the pump under load -------------------------------------------------------


def test_mock_many_futures_chains_fire_once_on_the_calling_thread() -> None:
    """``MockAdminClient`` completes its futures inside the call, from many threads
    at once: each ``then_apply`` function and ``when_complete`` action of a done
    future runs once, inside the call that adds it, on the thread adding it."""
    client = MockAdminClient.create().num_brokers(num_brokers=1).build()
    calls = _Calls()
    errors: list[BaseException] = []

    def work(worker: int) -> None:
        try:
            for i in range(25):
                name = f"t{worker}-{i}"
                future = client.create_topics(new_topics=[_topic(name)]).values()[name]
                applied = future.then_apply(function=calls.function(("apply", name), name))
                assert applied.is_done()
                assert applied.when_complete(action=calls.action("done", name)).is_done()
        except BaseException as error:  # noqa: BLE001 - collected for the assertion
            errors.append(error)

    threads = [threading.Thread(target=work, args=(w,), name=f"worker-{w}") for w in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(WAIT)
    assert errors == []
    keys = calls.keys()
    names = {f"t{w}-{i}" for w in range(8) for i in range(25)}
    assert sorted(k[1] for k in keys if k[0] == "apply") == sorted(names)
    assert sorted(k[1] for k in keys if k[0] == "done") == sorted(names)
    assert all(k[2:] == (k[1], None) for k in keys if k[0] == "done")
    # Each chain ran on the worker that built it, its function before its action.
    for key, thread_name in calls.seen:
        assert thread_name == "worker-" + key[1].split("-")[0].removeprefix("t")
    for name in names:
        assert keys.index(("apply", name)) < keys.index(("done", name, name, None))
    assert value(client.list_topics().names()) == names


def test_pump_runs_many_chains_once_each_in_completion_order() -> None:
    """The real client's futures complete on its one pump thread, many at once:
    every ``then_apply`` / ``when_complete`` of a successful call (an empty
    batch, done at once) and of failing calls with staggered timeouts runs once,
    a dependant after its source and the failing calls on the pump thread, in
    the order their deadlines pass."""
    calls = _Calls()
    with _admin() as admin:
        empty = [admin.create_topics(new_topics=[]).all() for _ in range(20)]
        for i, future in enumerate(empty):
            future.then_apply(function=calls.function(("apply", i), i)) \
                .when_complete(action=calls.action("chained", i))
        failing = []
        for group in range(4):
            for j in range(5):
                names = _list(admin, SHORT + 150 * group).names()
                names.when_complete(action=lambda _v, e, g=group, j=j: calls.add(("fail", g, j, e)))
                failing.append(names)
        for future in failing:
            assert str(assert_future_throws(future, KafkaTimeoutError)) == LIST_TIMED_OUT
        for future in empty:
            assert value(future) is None
    keys = calls.keys()
    # A failing call's action runs on the pump thread; an empty batch's future
    # may be done when the chain is added, which then runs inside the call.
    assert {t for k, t in calls.seen if k[0] == "fail"} == {PUMP_THREAD}
    assert {t for k, t in calls.seen if k[0] != "fail"} <= {PUMP_THREAD, "MainThread"}
    assert sorted(k[1] for k in keys if k[0] == "apply") == list(range(20))
    assert sorted(k[1] for k in keys if k[0] == "chained") == list(range(20))
    for i in range(20):
        assert keys.index(("apply", i)) < keys.index(("chained", i, i, None))
    fails = [k for k in keys if k[0] == "fail"]
    assert sorted((g, j) for _, g, j, _e in fails) == [(g, j) for g in range(4) for j in range(5)]
    assert all(type(e) is KafkaTimeoutError for *_, e in fails)
    groups = [g for _, g, _j, _e in fails]
    assert groups == sorted(groups)


async def test_async_mock_many_futures_chains_fire_once_on_the_loop() -> None:
    """``AsyncMockAdminClient``: many concurrent tasks; each chain of a done future
    runs once, on the loop, inside the call that adds it."""
    client = AsyncMockAdminClient.create().num_brokers(num_brokers=1).build()
    loop_thread = threading.current_thread().name
    calls = _Calls()

    async def work(worker: int) -> None:
        for i in range(25):
            name = f"t{worker}-{i}"
            future = client.create_topics(new_topics=[_topic(name)]).values()[name]
            chained = future.then_apply(function=calls.function(("apply", name), name)) \
                .when_complete(action=calls.action("done", name))
            assert await async_value(chained) == name
            await asyncio.sleep(0)

    await asyncio.gather(*(work(w) for w in range(8)))
    names = {f"t{w}-{i}" for w in range(8) for i in range(25)}
    keys = calls.keys()
    assert sorted(k[1] for k in keys if k[0] == "apply") == sorted(names)
    assert sorted(k[1] for k in keys if k[0] == "done") == sorted(names)
    assert calls.threads() == {loop_thread}
    assert await async_value(client.list_topics().names()) == names


async def test_async_loop_runs_many_chains_once_each_in_completion_order() -> None:
    """The asyncio client's dependants run once each on its loop, in the order the
    calls complete."""
    loop_thread = threading.current_thread().name
    calls = _Calls()
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        empty = [admin.create_topics(new_topics=[]).all() for _ in range(20)]
        chained = [future.then_apply(function=calls.function(("apply", i), i))
                   .when_complete(action=calls.action("chained", i))
                   for i, future in enumerate(empty)]
        failing = []
        for group in range(4):
            for j in range(5):
                names = _list(admin, SHORT + 150 * group).names()
                failing.append(names.when_complete(
                    action=lambda _v, e, g=group, j=j: calls.add(("fail", g, j, e))))
        assert await asyncio.gather(*(async_value(f) for f in chained)) == list(range(20))
        causes = await asyncio.gather(
            *(async_assert_future_throws(f, KafkaTimeoutError) for f in failing))
        assert {str(c) for c in causes} == {LIST_TIMED_OUT}
    keys = calls.keys()
    assert calls.threads() == {loop_thread}
    assert sorted(k[1] for k in keys if k[0] == "apply") == list(range(20))
    for i in range(20):
        assert keys.index(("apply", i)) < keys.index(("chained", i, i, None))
    groups = [k[1] for k in keys if k[0] == "fail"]
    assert len(groups) == 20 and groups == sorted(groups)


# ---- 2. close() with calls in flight ------------------------------------------------


@pytest.mark.parametrize("timeout", [0, 0.3, None], ids=["zero", "short", "none"])
def test_close_with_calls_in_flight(timeout: float | None) -> None:
    """``close(timeout=…)`` with calls in flight: zero and a short timeout abort
    them once the grace period ends, each with the core's ``TimeoutError`` naming
    its call, and return within the timeout; ``close()`` waits for them, so they
    end with their own call timeout."""
    admin = _admin()
    call_timeout_ms = GRACE if timeout is None else LONG
    listed = _list(admin, call_timeout_ms)
    created = admin.create_topics(new_topics=[_topic("a")],
                                  options=CreateTopicsOptions(timeout_ms=call_timeout_ms))
    started = time.monotonic()
    if timeout is None:  # left out: Java's close(), which waits without limit
        admin.close()
    else:
        admin.close(timeout=timeout)
    elapsed = time.monotonic() - started
    names, all_created = listed.names(), created.all()
    assert names.is_done() and all_created.is_done()
    list_cause = assert_future_throws(names, KafkaTimeoutError)
    create_cause = assert_future_throws(all_created, KafkaTimeoutError)
    if timeout is None:
        assert str(list_cause) == LIST_TIMED_OUT
        assert str(create_cause) == CREATE_TIMED_OUT
        assert GRACE / 1000 - 0.1 < elapsed < 10
    else:
        assert str(list_cause) == _exited("listTopics")
        assert str(create_cause) == _exited("createTopics")
        assert timeout <= elapsed + 0.01 and elapsed < timeout + 2
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_topics()


@pytest.mark.parametrize("timeout", [0, 0.3, None], ids=["zero", "short", "none"])
async def test_async_close_with_calls_in_flight(timeout: float | None) -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    listed = _list(admin, GRACE if timeout is None else LONG)
    started = time.monotonic()
    if timeout is None:  # left out: Java's close()
        await admin.close()
    else:
        await admin.close(timeout=timeout)
    elapsed = time.monotonic() - started
    cause = await async_assert_future_throws(listed.names(), KafkaTimeoutError)
    if timeout is None:
        assert str(cause) == LIST_TIMED_OUT
        assert GRACE / 1000 - 0.1 < elapsed < 10
    else:
        assert str(cause) == _exited("listTopics")
        assert timeout <= elapsed + 0.01 and elapsed < timeout + 2
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_topics()


# ---- 3. close() from a callback -------------------------------------------------------


def test_close_from_a_when_complete_with_another_call_in_flight() -> None:
    """``close(timeout=0)`` from a ``when_complete`` action, on the pump thread,
    while another call is in flight: it returns, the other call is aborted with
    its call name, and the pump thread ends."""
    before = _admin_threads()
    admin = _admin()
    other = _list(admin, LONG).names()
    returned = threading.Event()

    def action(_v: Any, _e: BaseException | None) -> None:
        admin.close(timeout=0)
        returned.set()

    _list(admin, SHORT).names().when_complete(action=action)
    assert returned.wait(WAIT)
    assert str(assert_future_throws(other, KafkaTimeoutError)) == _exited("listTopics")
    assert _join_new_admin_threads(before) == set()
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_topics()


async def test_async_close_from_a_when_complete() -> None:
    """The asyncio form: the action, on the loop, starts ``close()`` as a task
    (it cannot await); the close completes and aborts the other call."""
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    other = _list(admin, LONG).names()
    tasks: list[asyncio.Task[None]] = []
    _list(admin, SHORT).names().when_complete(
        action=lambda _v, _e: tasks.append(asyncio.ensure_future(admin.close(timeout=0))))
    assert str(await async_assert_future_throws(other, KafkaTimeoutError)) == (
        _exited("listTopics"))
    assert len(tasks) == 1
    await asyncio.wait_for(tasks[0], WAIT)
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_topics()


# ---- 4. a closing event loop ---------------------------------------------------------


class _Reports:
    """What the test reported so far: warnings, unraisable errors and
    ``asyncio`` / ``confluent_kafka`` log records."""

    def __init__(self, caught: list[warnings.WarningMessage], unraisable: list[Any],
                 caplog: pytest.LogCaptureFixture) -> None:
        self._caught = caught
        self._unraisable = unraisable
        self._caplog = caplog

    def assert_silent(self) -> None:
        """Collect the garbage, then assert nothing was reported."""
        gc.collect()
        reports = [str(w.message) for w in self._caught]
        reports += [str(u.exc_value) for u in self._unraisable]
        reports += [r.getMessage() for r in self._caplog.records
                    if r.name == "asyncio" or r.name.startswith("confluent_kafka")]
        assert reports == []


@pytest.fixture
def reports(caplog: pytest.LogCaptureFixture) -> Iterator[_Reports]:
    """A :class:`_Reports` of the test."""
    caplog.set_level(logging.DEBUG, logger="confluent_kafka")
    caplog.set_level(logging.WARNING, logger="asyncio")
    unraisable: list[Any] = []
    previous = sys.unraisablehook
    sys.unraisablehook = unraisable.append
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            yield _Reports(caught, unraisable, caplog)
    finally:
        sys.unraisablehook = previous


def test_closing_the_loop_with_admin_futures_pending(reports: _Reports) -> None:
    """A loop closed with admin futures pending (a dependant and an inner
    ``asyncio.Future`` among them) does not hang and leaves no warning: the
    off-loop pump delivers the failures, the inner future is dropped silently.

    The client cannot be closed afterwards
    (``test_async_client_closes_after_its_loop_closed``), so it is left open."""
    held: dict[str, Any] = {}
    done = threading.Event()

    def action(_v: Any, _e: BaseException | None) -> None:
        held["thread"] = threading.current_thread().name
        done.set()

    async def start() -> None:
        admin = AsyncAdmin.create(configs=UNREACHABLE)
        result = _list(admin, SHORT)
        held["names"] = result.names()
        held["dependant"] = result.names().when_complete(action=action)
        held["stage"] = result.listings().to_completion_stage()
        held["admin"] = admin

    loop = asyncio.new_event_loop()
    loop.run_until_complete(start())
    loop.close()
    dependant = held["dependant"]
    # The off-loop pump completes the futures (an await cannot run on the
    # closed loop, so the action's event is the wait).
    assert done.wait(WAIT)
    assert held["thread"] == OFF_LOOP_PUMP
    assert held["names"].is_done() and held["names"].is_completed_exceptionally()
    assert dependant.is_completed_exceptionally()
    held.clear()
    reports.assert_silent()


def test_asyncio_run_ends_with_a_task_awaiting_get(reports: _Reports) -> None:
    """``asyncio.run`` returning with a task still awaiting an admin future's
    ``get()``: asyncio cancels the task, nothing is reported as destroyed while
    pending, and the future itself fails later through the off-loop pump."""
    held: dict[str, Any] = {}
    done = threading.Event()

    async def main() -> None:
        admin = AsyncAdmin.create(configs=UNREACHABLE)
        names = _list(admin, SHORT).names()
        names.when_complete(action=lambda _v, _e: done.set())
        held["task"] = asyncio.ensure_future(names.get())
        held["names"] = names
        await asyncio.sleep(0)
        held["admin"] = admin

    asyncio.run(main())
    assert held["task"].cancelled()
    assert done.wait(WAIT)
    assert held["names"].is_completed_exceptionally()
    held.clear()
    reports.assert_silent()


def test_async_client_closes_after_its_loop_closed() -> None:
    # Owner decision (2026-10-20): close() alone may run on another loop once
    # the bound loop is closed (LoopBinding.bind_for_close), so a client whose
    # loop ended can still be closed, as Java's close() always can be.
    loop = asyncio.new_event_loop()
    admin = loop.run_until_complete(_create_async_on_loop())
    loop.close()
    asyncio.run(admin.close(timeout=0))


async def _create_async_on_loop() -> AsyncAdmin:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    _list(admin, SHORT)
    return admin


# ---- 5. the off-loop pump --------------------------------------------------------------


async def test_sync_client_in_a_worker_thread_while_a_loop_runs() -> None:
    """A sync client used from a worker thread while the loop runs: its callbacks
    run on its pump thread, never on the loop or the worker, and the loop keeps
    running meanwhile."""
    loop_thread = threading.current_thread().name
    calls = _Calls()
    ticks = 0

    def work() -> str:
        with _admin() as admin:
            names = _list(admin, SHORT).names()
            names.when_complete(action=lambda _v, e: calls.add(type(e).__name__))
            return str(assert_future_throws(names, KafkaTimeoutError))

    worker = asyncio.ensure_future(asyncio.to_thread(work))
    while not worker.done():
        ticks += 1
        await asyncio.wait({worker}, timeout=0.01)
    assert worker.result() == LIST_TIMED_OUT
    assert ticks > 1
    assert calls.seen == [("TimeoutError", PUMP_THREAD)]
    assert loop_thread not in calls.threads()


async def test_async_client_on_its_loop_while_a_sync_client_runs_in_a_thread() -> None:
    loop_thread = threading.current_thread().name
    sync_calls, async_calls = _Calls(), _Calls()
    sync_result: list[str] = []

    def work() -> None:
        with _admin() as admin:
            names = _list(admin, SHORT).names()
            names.when_complete(action=lambda _v, _e: sync_calls.add("sync"))
            sync_result.append(str(assert_future_throws(names, KafkaTimeoutError)))

    thread = threading.Thread(target=work, name="sync-worker")
    thread.start()
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        names = _list(admin, SHORT).names().when_complete(
            action=lambda _v, _e: async_calls.add("async"))
        assert str(await async_assert_future_throws(names, KafkaTimeoutError)) == LIST_TIMED_OUT
    await asyncio.to_thread(thread.join, WAIT)
    assert not thread.is_alive()
    assert sync_result == [LIST_TIMED_OUT]
    assert sync_calls.seen == [("sync", PUMP_THREAD)]
    assert async_calls.seen == [("async", loop_thread)]


def test_async_client_without_a_loop_completes_on_the_off_loop_pump() -> None:
    """An asyncio client used from a thread with no loop: its callbacks run on the
    off-loop pump thread; a later loop reads its futures and closes it."""
    calls = _Calls()
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    names = _list(admin, SHORT).names()
    done = threading.Event()

    def action(_v: Any, _e: BaseException | None) -> None:
        calls.add("done")
        done.set()

    names.when_complete(action=action)
    assert done.wait(WAIT)
    assert calls.seen == [("done", OFF_LOOP_PUMP)]

    async def main() -> None:
        assert str(await async_assert_future_throws(names, KafkaTimeoutError)) == (
            LIST_TIMED_OUT)
        await admin.close()

    asyncio.run(main())


# ---- 6. Ctrl+C / cancellation inside get() ----------------------------------------


def _with_sigint(after: float, run: Callable[[], Any]) -> float:
    """Run ``run()`` with one SIGINT sent to this (the main) thread after
    ``after`` seconds and raised as ``KeyboardInterrupt``; how long until it
    raised. The handler runs once, so no signal is left pending."""
    assert threading.current_thread() is threading.main_thread()

    def on_sigint(signum: int, frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGINT, on_sigint)
    timer = threading.Timer(after, lambda: os.kill(os.getpid(), signal.SIGINT))
    started = time.monotonic()
    try:
        timer.start()
        with pytest.raises(KeyboardInterrupt):
            run()
        return time.monotonic() - started
    finally:
        timer.join()
        signal.signal(signal.SIGINT, previous)


def test_ctrl_c_in_get_leaves_the_client_usable() -> None:
    with _admin() as admin:
        names = _list(admin, LONG).names()
        assert _with_sigint(0.2, names.get) < 5
        assert not names.is_done()
        assert str(assert_future_throws(_list(admin, SHORT).names(), KafkaTimeoutError)) == (
            LIST_TIMED_OUT)
        admin.close(timeout=0)
        assert str(assert_future_throws(names, KafkaTimeoutError)) == _exited("listTopics")


async def test_async_cancelled_get_leaves_the_client_usable() -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    names = _list(admin, LONG).names()
    started = asyncio.Event()

    async def wait() -> Any:
        started.set()
        return await names.get()

    task = asyncio.ensure_future(wait())
    await started.wait()
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not names.is_done() and not names.is_cancelled()
    assert str(await async_assert_future_throws(_list(admin, SHORT).names(),
                                                KafkaTimeoutError)) == LIST_TIMED_OUT
    await admin.close(timeout=0)
    assert str(await async_assert_future_throws(names, KafkaTimeoutError)) == (
        _exited("listTopics"))


# ---- 7. Ctrl+C in close() ------------------------------------------------------------


def test_ctrl_c_in_close_then_a_second_close_is_harmless() -> None:
    """SIGINT during a ``close()`` that waits for a call in flight re-raises
    ``KeyboardInterrupt``; a second ``close()`` returns at once, the close goes on
    and the call ends with its own timeout."""
    before = _admin_threads()
    admin = _admin()
    names = _list(admin, GRACE).names()
    assert _with_sigint(0.2, admin.close) < GRACE / 1000 - 0.1
    started = time.monotonic()
    admin.close()
    admin.close(timeout=0)
    assert time.monotonic() - started < 0.5
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_topics()
    assert str(assert_future_throws(names, KafkaTimeoutError)) == LIST_TIMED_OUT
    assert _join_new_admin_threads(before) == set()


async def test_async_cancelled_close_then_a_second_close_is_harmless() -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    names = _list(admin, GRACE).names()
    closing = asyncio.ensure_future(admin.close())
    await asyncio.sleep(0)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    started = time.monotonic()
    await admin.close()
    await admin.close(timeout=0)
    assert time.monotonic() - started < 0.5
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_topics()
    assert str(await async_assert_future_throws(names, KafkaTimeoutError)) == LIST_TIMED_OUT


# ---- 8. unread failed futures are silent -------------------------------------------


def test_unread_failed_futures_are_silent(reports: _Reports) -> None:
    """Failed futures nobody reads (a plain one, a dependant, the inner future of
    ``to_completion_stage``) report nothing when collected."""
    admin = _admin()
    result = _list(admin, SHORT)
    futures = [result.names(), result.listings().then_apply(function=list),
               result.names_to_listings().when_complete(action=lambda _v, _e: None)]
    stage = result.listings().to_completion_stage()
    admin.close()
    assert all(f.is_done() for f in futures) and stage.done()
    del result, futures, stage, admin
    reports.assert_silent()


async def test_async_unread_failed_futures_are_silent(reports: _Reports) -> None:
    """The asyncio form: in particular asyncio's "Future exception was never
    retrieved" is not logged for an inner ``asyncio.Future`` nobody reads."""
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    result = _list(admin, SHORT)
    futures = [result.names(), result.listings().then_apply(function=list),
               result.names_to_listings().when_complete(action=lambda _v, _e: None)]
    stage = result.listings().to_completion_stage()
    await admin.close()
    await asyncio.wait({stage}, timeout=WAIT)
    assert all(f.is_done() for f in futures) and stage.done()
    del result, futures, stage, admin
    reports.assert_silent()


# ---- 9. thread safety ----------------------------------------------------------------


def test_many_threads_on_one_client_each_callback_once() -> None:
    """N threads each issuing RPCs on one client: every future fails with its
    call's timeout, no error escapes a thread and each action runs exactly once."""
    calls = _Calls()
    errors: list[BaseException] = []
    admin = _admin()

    def work(worker: int) -> None:
        try:
            futures = []
            for i in range(5):
                key = (worker, i)
                if i % 2:
                    future = _list(admin, SHORT).names()
                else:
                    future = admin.create_topics(new_topics=[_topic(f"t{worker}-{i}")],
                                                 options=CreateTopicsOptions(timeout_ms=SHORT)).all()
                futures.append((future.when_complete(action=lambda _v, e, k=key: calls.add(
                    (k, type(e).__name__))), LIST_TIMED_OUT if i % 2 else CREATE_TIMED_OUT))
            for future, message in futures:
                assert str(assert_future_throws(future, KafkaTimeoutError)) == message
        except BaseException as error:  # noqa: BLE001 - collected for the assertion
            errors.append(error)

    threads = [threading.Thread(target=work, args=(w,)) for w in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(WAIT)
    assert not any(t.is_alive() for t in threads)
    admin.close()
    assert errors == []
    keys = calls.keys()
    assert sorted(k for k, _ in keys) == [(w, i) for w in range(10) for i in range(5)]
    assert {name for _, name in keys} == {"TimeoutError"}
    assert calls.threads() == {PUMP_THREAD}


async def test_async_many_tasks_on_one_client_each_callback_once() -> None:
    calls = _Calls()
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:

        async def work(worker: int) -> None:
            futures = []
            for i in range(5):
                key = (worker, i)
                futures.append(_list(admin, SHORT).names().when_complete(
                    action=lambda _v, e, k=key: calls.add((k, type(e).__name__))))
                await asyncio.sleep(0)
            causes = await asyncio.gather(
                *(async_assert_future_throws(f, KafkaTimeoutError) for f in futures))
            assert {str(c) for c in causes} == {LIST_TIMED_OUT}

        await asyncio.gather(*(work(w) for w in range(10)))
    keys = calls.keys()
    assert sorted(k for k, _ in keys) == [(w, i) for w in range(10) for i in range(5)]
    assert {name for _, name in keys} == {"TimeoutError"}


# ---- 10. nothing left after close() --------------------------------------------------


def test_close_leaves_no_thread_and_no_client() -> None:
    """After ``close()``: no admin thread of the client is alive, and the client is
    collected, even while its futures are still held."""
    before = _admin_threads()
    admin = _admin()
    names = _list(admin, SHORT).names()
    names.when_complete(action=lambda _v, _e: None)
    assert len(_admin_threads() - before) == 1
    admin.close()
    assert _join_new_admin_threads(before) == set()
    ref = weakref.ref(admin)
    del admin
    gc.collect()
    assert ref() is None
    assert names.is_done()


async def test_async_close_leaves_no_thread_and_no_client() -> None:
    before = _admin_threads()
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    names = _list(admin, SHORT).names()
    names.when_complete(action=lambda _v, _e: None)
    await admin.close()
    assert await asyncio.to_thread(_join_new_admin_threads, before) == set()
    ref = weakref.ref(admin)
    del admin
    gc.collect()
    assert ref() is None
    assert names.is_done()
