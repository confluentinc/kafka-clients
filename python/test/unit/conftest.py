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

"""Fixtures and helpers shared by the unit test modules."""

import asyncio
import sys
import threading
import time
from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.common import KafkaError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.producer import AsyncKafkaProducer, KafkaProducer, ProducerRecord
from confluent_kafka.producer._base import await_payload, raise_if_error

# How long a held completion waits for its release.
_HOLD_TIMEOUT = 10.0


@pytest.fixture
def two_gib_bytes():
    """A zero-filled ``bytes`` of exactly 2 GiB (``2**31`` bytes): one byte more
    than the ``int32_t`` lengths of the C API can carry.

    ``bytes(n)`` is allocated with ``calloc``, so its pages are mapped lazily and
    the buffer costs almost no resident memory. It needs a 64-bit interpreter,
    and a test using it is skipped rather than failed where it cannot be
    allocated.
    """
    if sys.maxsize <= 2**32:
        pytest.skip("a 2 GiB buffer needs a 64-bit Python")
    try:
        return bytes(2**31)
    except MemoryError:
        pytest.skip("could not allocate a 2 GiB buffer")


class HeldCompletion:
    """The completion of one entry point of the C extension, held on the native
    thread that delivers it until :attr:`release` is set: the test decides when
    the result reaches the client's callback (and so its event loop).

    ``payloads`` holds the results as the C trampoline passes them, ``raised``
    what the client's callback raised back to the trampoline (which prints it
    and drops it), and ``delivered`` is set once the callback has returned. A
    payload ``holds`` rejects is passed straight through, unrecorded: a
    ``Producer_send`` callback first carries the producer's answer for a queued
    record (``(None, error)``), which the send waits for, and holding it would
    hold the send."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, name: str,
                 holds: Callable[[tuple[Any, ...]], bool] | None = None) -> None:
        self.submitted = threading.Event()
        self.release = threading.Event()
        self.delivered = threading.Event()
        self.payloads: list[tuple[Any, ...]] = []
        self.raised: list[BaseException] = []
        real = getattr(_lib, name)

        def entry(*args: Any) -> Any:
            *rest, cb = args

            def held(*payload: Any) -> None:
                if holds is not None and not holds(payload):
                    cb(*payload)
                    return
                self.payloads.append(payload)
                self.release.wait(_HOLD_TIMEOUT)
                try:
                    cb(*payload)
                except BaseException as exc:
                    self.raised.append(exc)
                    raise
                finally:
                    self.delivered.set()

            result = real(*rest, held)
            self.submitted.set()
            return result

        monkeypatch.setattr(_lib, name, entry)


@pytest.fixture
def hold_completion(monkeypatch: pytest.MonkeyPatch) -> Callable[..., HeldCompletion]:
    """``hold_completion(name, holds=...)`` holds the completions of
    ``_lib.<name>``, whose last argument is the completion callback (see
    :class:`HeldCompletion`)."""
    return lambda name, **kwargs: HeldCompletion(monkeypatch, name, **kwargs)


class PendingCall:
    """A call of one ``_async`` entry point of the C extension that never
    reaches the native producer: its arguments are recorded in ``calls``,
    ``submitted`` is set, and its completion stays pending for good.

    This is how a test keeps a ``close()`` pending until the client forces it
    (an interrupt, or a cancellation) without a broker to hold the native
    close. Holding the close's *completion* instead (:class:`HeldCompletion`)
    would hold the producer's dispatcher thread, and with it the forced
    close's own completion."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
        self.submitted = threading.Event()
        self.calls: list[tuple[Any, ...]] = []

        def entry(*args: Any) -> None:
            self.calls.append(args)
            self.submitted.set()

        monkeypatch.setattr(_lib, name, entry)


@pytest.fixture
def pending_call(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], PendingCall]:
    """``pending_call(name)`` leaves every call of ``_lib.<name>`` pending for
    good (see :class:`PendingCall`)."""
    return lambda name: PendingCall(monkeypatch, name)


@pytest.fixture
def freed_errors(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The error handles freed with ``KafkaError_destroy`` during the test, in
    order (each is still freed)."""
    freed: list[int] = []
    real = _lib.KafkaError_destroy

    def destroy(handle: int) -> None:
        freed.append(handle)
        real(handle)

    monkeypatch.setattr(_lib, "KafkaError_destroy", destroy)
    return freed


class StoppedLoop:
    """A fresh event loop that runs a call until it waits, then stops, as
    ``loop.run_until_complete(...)`` returning while another task of the loop
    still waits: the call's task is left pending on the stopped loop."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._task: asyncio.Task[Any] | None = None

    def start(self, call: Coroutine[Any, Any, Any]) -> None:
        self._task = self.loop.create_task(call)
        self.loop.run_until_complete(asyncio.sleep(0))

    def abandon(self) -> None:
        """Close the loop, then do to the task what the garbage collector does
        to a task left on a closed loop: close its coroutine, which runs its
        ``finally`` blocks (an awaiting consumer call gives its use of the
        consumer back), without the "destroyed but pending" report."""
        if not self.loop.is_closed():
            self.loop.close()
        task, self._task = self._task, None
        if task is not None:
            task._log_destroy_pending = False  # noqa: SLF001
            task.get_coro().close()


@pytest.fixture
def stopped_loop() -> Iterator[StoppedLoop]:
    """A :class:`StoppedLoop`, abandoned at the end of the test."""
    loop = StoppedLoop()
    yield loop
    loop.abandon()


# ===========================================================================
# The real producer without a broker: constants and fakes of the send path,
# shared by test_producer_family.py and test_producer_send_path.py
# ===========================================================================

TOPIC = "topic"
RECORD = ProducerRecord(topic=TOPIC, key=b"k", value=b"v")
# Sends fail on the metadata wait after max.block.ms.
UNREACHABLE: dict[str, Any] = {"bootstrap.servers": "127.0.0.1:59999", "max.block.ms": 100}
# Java's KafkaProducer.throwIfProducerClosed() message.
CLOSED = "Cannot perform operation after producer has been closed"
# What a send() still waiting inside the producer raises once close() ends its
# wait: the FFI's refusal of the waiting record, or the core's closed check.
CLOSED_PRODUCER_REFUSALS = ("Producer closed while send in progress", CLOSED)
# Java's message for a record sent with no transaction started
# (TransactionManager.java:465, with Java's double space).
NOT_IN_A_TRANSACTION = "Cannot add partition topic-0 to transaction while in state  READY"


def is_closed_producer_refusal(error: object) -> bool:
    return (isinstance(error, (KafkaError, IllegalStateError))
            and str(error) in CLOSED_PRODUCER_REFUSALS)


def refusal() -> int:
    """An owned handle of the error Java's doSend rethrows for a record sent
    with no transaction started, as the core reports a refusal."""
    handle: int = _lib.KafkaError_new(IllegalStateError._ffi_id, NOT_IN_A_TRANSACTION)
    return handle


def failed_delivery() -> int:
    """An owned handle of a record's failure, as a delivery report carries it."""
    handle: int = _lib.KafkaError_new(KafkaTimeoutError._ffi_id, "boom")
    return handle


def wait_until(predicate: Callable[[], bool], timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        time.sleep(0.005)


async def await_until(predicate: Callable[[], bool], timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        await asyncio.sleep(0.005)


def spy_on(monkeypatch: pytest.MonkeyPatch, name: str) -> list[Any]:
    """The return values of ``_lib.<name>`` during the test, in order (each
    call still goes through). For ``Producer_send``: True for a record taken
    at once, None for one queued, an error handle for one refused."""
    calls: list[Any] = []
    real = getattr(_lib, name)

    def wrapper(*args: Any) -> Any:
        result = real(*args)
        calls.append(result)
        return result

    monkeypatch.setattr(_lib, name, wrapper)
    return calls


def fake_queued_send(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fakes a Producer_send whose record the producer could not take at once:
    queued (None), the answer still to come. The callback send() passed is
    kept, for the test to answer with: ``cb(None, 0)`` for an accepted record,
    ``cb(None, handle)`` for a refused one, ``cb(metadata, error)`` for a
    delivery report."""
    captured: dict[str, Any] = {}

    def producer_send(c_producer: int, native: Any, cb: Any) -> None:
        captured["cb"] = cb

    monkeypatch.setattr(_lib, "Producer_send", producer_send)
    return captured


def fake_taken_send(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Fakes a Producer_send that takes every record at once (True), keeping
    the callbacks, so the test completes the records itself."""
    cbs: list[Any] = []

    def producer_send(c_producer: int, native: Any, cb: Any) -> bool:
        cbs.append(cb)
        return True

    monkeypatch.setattr(_lib, "Producer_send", producer_send)
    return cbs


def send_on_a_thread(p: KafkaProducer[Any, Any],
                     record: ProducerRecord[Any, Any] = RECORD) -> tuple[threading.Thread, list[Any]]:
    """Run ``p.send(record=record)`` on a thread, where it blocks for the
    producer's answer; its outcome (the future, or the error it raised) lands
    in the returned list."""
    outcome: list[Any] = []

    def run() -> None:
        try:
            outcome.append(p.send(record=record))
        except Exception as error:  # noqa: BLE001 - the test reads which one
            outcome.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


def close_the_rust_producer_behind_the_wrapper(
        p: KafkaProducer[Any, Any] | AsyncKafkaProducer[Any, Any]) -> None:
    """Close the Rust producer while the Python wrapper still believes it is
    open, so the next send reaches the producer and is refused there, on the
    calling thread, with Java's "Cannot perform operation after producer has
    been closed": a stand-in for every refusal Java's send() throws (a
    transaction state, a fatal error), which the broker tests in
    test/integration drive for real."""
    (error,) = await_payload(lambda cb: _lib.Producer_close_async(p._c_producer, cb))  # noqa: SLF001
    raise_if_error(error)
