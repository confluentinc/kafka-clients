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

"""A Ctrl+C while a client takes or ends a use of its native handle.

``close()`` waits for the counted uses to end before it frees the handle, so a
``KeyboardInterrupt`` must neither leave a use counted nor the lifecycle lock
held; otherwise ``close()`` waits forever (CLAUDE.md, Python Binding
Conventions, Implementation over the FFI). The use is taken and ended in the C
extension (``_confluentkafka.Use``), under the C lock.

The interrupt is injected at each point where Python code could still run:

- ``enter``: the start path's acquire of the lifecycle lock raises (a signal
  handler running in a contended acquire), before anything is counted;
- ``count``: the start path's release of the lock raises right after the count;
- ``body``: the native call inside the use raises, as a signal handler does at
  the check point after it returns;
- ``end``: the end path's acquire of the lock raises (the C code retries it);
- ``notify``: the waiters' wake-up at the end raises (the C code retries it).

The last test sends real ``SIGINT`` signals while a call loops."""

from __future__ import annotations

import asyncio
import os
import random
import signal
import sys
import threading
import time
from collections.abc import Callable
from typing import Any

import pytest

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka.admin import Admin
from confluent_kafka.consumer import CloseOptions, KafkaConsumer
from confluent_kafka.producer import KafkaProducer, ProducerRecord

CLOSE_BOUND_S = 10.0


class _InterruptingLock:
    """The client's lifecycle lock, raising ``KeyboardInterrupt`` once, on the
    test's thread only (the clients' own threads use it too): before its first
    acquire (``enter``), after its first release (``count``), or before the
    acquire after that (``end``). Everything else goes to the real lock."""

    def __init__(self, inner: Any, at: str) -> None:
        self._inner = inner
        self._at = at
        self._thread = threading.get_ident()
        self._acquires = 0
        self._releases = 0
        self.fired = False

    def _mine(self) -> bool:
        return threading.get_ident() == self._thread and not self.fired

    def acquire(self, *args: Any, **kwargs: Any) -> bool:
        if self._mine():
            self._acquires += 1
            if (self._at == "enter" and self._acquires == 1) or (
                    self._at == "end" and self._acquires == 2):
                self.fired = True
                raise KeyboardInterrupt
        return bool(self._inner.acquire(*args, **kwargs))

    def release(self) -> None:
        self._inner.release()
        if self._mine():
            self._releases += 1
            if self._at == "count" and self._releases == 1:
                self.fired = True
                raise KeyboardInterrupt

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _InterruptingCondition:
    """The client's lifecycle condition, whose ``notify_all`` raises
    ``KeyboardInterrupt`` once before waking the waiters."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.fired = False

    def notify_all(self) -> None:
        if not self.fired:
            self.fired = True
            raise KeyboardInterrupt
        self._inner.notify_all()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _lock_is_free(lock: Any) -> bool:
    """Whether another thread can take ``lock`` (a held RLock blocks it)."""
    taken: list[bool] = []

    def take() -> None:
        ok = lock.acquire(timeout=CLOSE_BOUND_S)
        taken.append(ok)
        if ok:
            lock.release()

    thread = threading.Thread(target=take, daemon=True)
    thread.start()
    thread.join(CLOSE_BOUND_S + 1)
    return taken == [True]


def _closes_within_bound(close: Callable[[], None]) -> None:
    done = threading.Event()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            close()
        except BaseException as e:  # noqa: BLE001 - reported below
            errors.append(e)
        finally:
            done.set()

    threading.Thread(target=run, daemon=True).start()
    assert done.wait(CLOSE_BOUND_S), "close() waited for a use nobody holds"
    assert errors == []


def _interrupted(owner: Any, at: str, call: Callable[[], object],
                 monkeypatch: pytest.MonkeyPatch, native: str | None = None,
                 idle: bool = True) -> None:
    """Run ``call`` with the interrupt injected at ``at``: it must raise
    ``KeyboardInterrupt``, leave the lock free and, for a client with no
    background use (``idle``), the count at 0. ``native`` names the extension
    function the ``body`` injection replaces."""
    real_lock = owner._lifecycle_lock
    real_cond = owner._lifecycle
    wrapper: Any = None
    if at in ("enter", "count", "end"):
        wrapper = _InterruptingLock(real_lock, at)
        owner._lifecycle_lock = wrapper
    elif at == "notify":
        wrapper = _InterruptingCondition(real_cond)
        owner._lifecycle = wrapper
    else:
        assert at == "body" and native is not None

        def raiser(*_args: object) -> object:
            raise KeyboardInterrupt

        monkeypatch.setattr(_lib, native, raiser)
    try:
        with pytest.raises(KeyboardInterrupt):
            call()
    finally:
        owner._lifecycle_lock = real_lock
        owner._lifecycle = real_cond
        monkeypatch.undo()
    if wrapper is not None:
        assert wrapper.fired
    assert _lock_is_free(real_lock)
    if idle:
        assert owner._uses == 0


def _consumer() -> KafkaConsumer[bytes, bytes]:
    return KafkaConsumer(configs={"bootstrap.servers": "localhost:1",
                                  "group.protocol": "consumer"})


_CONSUMER_SITES = {
    # site: (the call, the native function inside its use)
    "use": ("assignment", "Consumer_assignment"),  # Java's acquire()
    "hold": ("metrics", "Consumer_metrics"),       # outside acquire()
    "wakeup": ("wakeup", "Consumer_wakeup"),       # wakeup()'s own use
}


@pytest.mark.parametrize("at", ["enter", "count", "body", "end", "notify"])
@pytest.mark.parametrize("site", sorted(_CONSUMER_SITES))
def test_consumer_use_survives_an_interrupt(site: str, at: str,
                                            monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = _consumer()
    method, native = _CONSUMER_SITES[site]
    _interrupted(consumer, at, getattr(consumer, method), monkeypatch, native)
    assert consumer._use_threads == {}
    # The consumer still works, and close() returns.
    assert consumer.assignment() == set()
    consumer.wakeup()
    _closes_within_bound(lambda: consumer.close(option=CloseOptions.timeout(timeout=0)))
    assert consumer._uses == 0


# The producer's and the admin client's calls wake a waiter only once close()
# has begun, so their notify point is covered by the pumps below.
@pytest.mark.parametrize("at", ["enter", "count", "body", "end"])
def test_producer_use_survives_an_interrupt(at: str, monkeypatch: pytest.MonkeyPatch) -> None:
    producer: KafkaProducer[bytes, bytes] = KafkaProducer(
        configs={"bootstrap.servers": "localhost:1"})
    # The completion thread counts its own uses; after close() none is left.
    _interrupted(producer, at, producer.metrics, monkeypatch, "Producer_metrics", idle=False)
    assert isinstance(producer.metrics(), dict)
    _closes_within_bound(lambda: producer.close(timeout=0))
    assert producer._uses == 0


@pytest.mark.parametrize("at", ["enter", "count", "end"])
def test_producer_shared_send_use_survives_an_interrupt(at: str) -> None:
    """``send()``'s use is one object every send shares; an interrupt in one
    send leaves the others' counts intact."""
    producer: KafkaProducer[bytes, bytes] = KafkaProducer(
        configs={"bootstrap.servers": "localhost:1", "max.block.ms": 0})
    stop = threading.Event()
    sent: list[int] = []

    def sender() -> None:
        while not stop.is_set():
            producer.send(record=ProducerRecord(topic="t", value=b"v"))
            sent.append(1)

    others = [threading.Thread(target=sender, daemon=True) for _ in range(4)]
    for thread in others:
        thread.start()
    try:
        record = ProducerRecord(topic="t", value=b"v")
        _interrupted(producer, at, lambda: producer.send(record=record),
                     pytest.MonkeyPatch(), idle=False)
    finally:
        stop.set()
        for thread in others:
            thread.join(CLOSE_BOUND_S)
    assert sent
    _closes_within_bound(lambda: producer.close(timeout=0))
    assert producer._uses == 0


@pytest.mark.parametrize("at", ["enter", "count", "body", "end"])
def test_admin_use_survives_an_interrupt(at: str, monkeypatch: pytest.MonkeyPatch) -> None:
    admin = Admin.create(configs={"bootstrap.servers": "localhost:1"})
    # The pump thread counts its own uses; after close() none is left.
    _interrupted(admin, at, admin.list_topics, monkeypatch, "Admin_list_topics", idle=False)
    admin.list_topics()
    _closes_within_bound(lambda: admin.close(timeout=0))
    assert admin._uses == 0


# --------------------------------------------------------------------------- #
# The asyncio clients count their pump runs on the event loop, which may run
# on the main thread, so the same points apply there.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("at", ["enter", "count", "end", "notify"])
@pytest.mark.parametrize("site", ["pump", "install_wake", "release_wake", "move_wake_off"])
def test_async_producer_pump_survives_an_interrupt(site: str, at: str,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    from confluent_kafka.producer import AsyncKafkaProducer

    async def main() -> None:
        loop = asyncio.get_running_loop()
        producer: AsyncKafkaProducer[bytes, bytes] = AsyncKafkaProducer(
            configs={"bootstrap.servers": "localhost:1"})
        if site == "install_wake" and not producer._WAKE_BY_FD:
            pytest.skip("no socket wake-up on this platform")
        if site != "install_wake":
            producer._enter()  # binds the loop and installs the socket wake-up
        call: Callable[[], object] = {
            "pump": producer._pump,
            "install_wake": lambda: producer._install_wake(loop),
            "release_wake": producer._release_wake,
            "move_wake_off": lambda: producer._move_wake_off(loop),
        }[site]
        _interrupted(producer, at, call, monkeypatch)
        await asyncio.wait_for(producer.close(timeout=0), CLOSE_BOUND_S)
        assert producer._uses == 0

    asyncio.run(main())


@pytest.mark.parametrize("at", ["enter", "count", "body", "end", "notify"])
def test_async_admin_pump_survives_an_interrupt(at: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from confluent_kafka.admin import AsyncAdmin

    async def main() -> None:
        admin = AsyncAdmin.create(configs={"bootstrap.servers": "localhost:1"})
        _interrupted(admin, at, admin._run_pump, monkeypatch, "Admin__execute_callbacks")
        await asyncio.wait_for(admin.close(timeout=0), CLOSE_BOUND_S)
        assert admin._uses == 0

    asyncio.run(main())


# --------------------------------------------------------------------------- #
# The one test with real signals: a thread sends SIGINT to the main thread
# again and again while a call loops; the handler raises KeyboardInterrupt only
# while the call runs, at the interpreter's own check points.
# --------------------------------------------------------------------------- #

STRESS_S = 3.0


# CPython 3.14 checks for signals after a `with` statement's `__enter__` returns
# and before its block starts, so a KeyboardInterrupt there skips `__exit__` and
# leaves the use counted (https://github.com/python/cpython/issues/148874, fixed
# in 3.15). These signals land there often enough to fail; a run that misses
# that point passes.
@pytest.mark.xfail(sys.version_info[:2] == (3, 14), strict=False,
                   reason="CPython gh-148874: on 3.14 a signal right after __enter__ skips __exit__")
@pytest.mark.skipif(not hasattr(signal, "pthread_kill"), reason="needs signal.pthread_kill")
@pytest.mark.parametrize("client", ["producer", "consumer", "admin"])
def test_real_sigint_leaves_no_use_counted(client: str) -> None:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    owner: Any
    calls: list[Callable[[], object]]
    close: Callable[[], None]
    if client == "producer":
        producer: KafkaProducer[bytes, bytes] = KafkaProducer(
            configs={"bootstrap.servers": "localhost:1"})
        owner, calls = producer, [producer.metrics]
        close = lambda: producer.close(timeout=0)  # noqa: E731
    elif client == "consumer":
        consumer = _consumer()
        owner, calls = consumer, [consumer.metrics, consumer.assignment, consumer.wakeup]
        close = lambda: consumer.close(option=CloseOptions.timeout(timeout=0))  # noqa: E731
    else:
        admin = Admin.create(configs={"bootstrap.servers": "localhost:1"})
        owner, calls = admin, [admin.describe_cluster]
        close = lambda: admin.close(timeout=0)  # noqa: E731

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
    made = 0
    try:
        sender.start()
        deadline = time.monotonic() + STRESS_S
        while time.monotonic() < deadline:
            for call in calls:
                try:
                    in_call[0] = True
                    call()
                    made += 1
                except KeyboardInterrupt:
                    interrupted += 1
                finally:
                    in_call[0] = False
    finally:
        stop.set()
        sender.join(CLOSE_BOUND_S)
        signal.signal(signal.SIGINT, previous)
    print(f"{client}: {made} calls, {interrupted} interrupted, pid {os.getpid()}")
    assert interrupted > 0, "no call was interrupted; the test did not run its case"
    assert _lock_is_free(owner._lifecycle_lock)
    if client == "consumer":
        assert owner._uses == 0
        assert owner._use_threads == {}
    _closes_within_bound(close)
    assert owner._uses == 0
