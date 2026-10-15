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

"""The send path of ``KafkaProducer`` and ``AsyncKafkaProducer`` around the
answers of ``Producer_try_send_cb``, the C extension's non-blocking send, each
faked here without a broker (test/integration/test_kafka_producer_broker.py
drives the same paths against one). The entry point's contract::

    _lib.Producer_try_send_cb(handle, record, on_completion, resolve) -> owned_error_int

    taken at once     resolve(0) runs inside the call, on the calling thread;
                      on_completion runs later, from the completion thread
    refused at once   resolve(owned_error_int) runs inside the call, on the
                      calling thread; on_completion never runs
    queued            neither runs inside the call: resolve runs from the
                      callback pump once the record is handed to the producer
    rejected          the C API refuses the record before either: its OWNED
                      error is returned and neither runs

and what ``send()`` makes of each answer:

- taken: the future is tracked and returned pending, as for a queued record;
- refused, with no handover to wait for: the callback runs inside ``send()``,
  on the calling thread (the event loop for the async producer), the future
  fails, and a raising callback is logged once, there being no core report
  for it (``Producer.send``'s last paragraph, a deviation from Java, which
  rethrows such an error);
- refused while ``send()`` waits for the handover (a ``transactional.id`` and
  no transaction started): ``send()`` raises the error at once, the callback
  does not run and nothing is tracked, as Java's ``doSend`` rethrows it;
- rejected: ``send()`` raises the error, as for ``Producer_send_cb``.

The queued answer is the one every other test takes against an unreachable
broker, so it is covered there; here only the entry point in use is checked.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.common import KafkaError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.producer import (
    AsyncKafkaProducer, KafkaProducer, ProducerRecord, RecordMetadata,
)
from confluent_kafka.producer._base import CLOSED_WHILE_SENDING_MESSAGE
from confluent_kafka.producer._send import MetadataTuple

TOPIC = "topic"
RECORD = ProducerRecord(topic=TOPIC, key=b"k", value=b"v")
UNREACHABLE = {"bootstrap.servers": "127.0.0.1:59999", "max.block.ms": 100}
TRANSACTIONAL = {**UNREACHABLE, "transactional.id": "txn"}

# The transaction manager's refusal of a send outside a transaction, which the
# Rust send() returns (Java's doSend rethrows it) -- with Java's double space.
NOT_IN_A_TRANSACTION = "Cannot add partition topic-0 to transaction while in state  READY"
# What the C API returns for a record it refuses before queuing it.
REJECTED = "Cannot perform operation after producer has been closed"
# The error of a delivery that fails.
BOOM = "boom"
# The core's message for a raising callback, which Python logs for a
# completion the core does not deliver (the record has no partition yet).
CALLBACK_ERROR = "Error executing user-provided callback on message for topic-partition 'topic--1'"
# The delivery of a record the broker acknowledged at offset 7.
ACKNOWLEDGED: MetadataTuple = (TOPIC, 0, 7, 1234, 1, 1)

Seen = list[tuple[int, bool, int, Exception | None]]


class FakeTrySend:
    """A fake of ``_lib.Producer_try_send_cb`` giving the answer the C extension
    gives for a record the producer takes at once, refuses at once (with
    ``refusal``, a class and a message), or queues, and for one the C API
    rejects (see the module docstring). ``calls`` keeps each record's
    callbacks and the thread that sent it, ``handles`` every error handle made,
    all OWNED by the receiver, who frees them."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, answer: str,
                 refusal: tuple[type, str] = (KafkaError, CLOSED_WHILE_SENDING_MESSAGE)) -> None:
        self.answer = answer
        self.refusal = refusal
        self.calls: list[dict[str, Any]] = []
        self.handles: list[int] = []
        monkeypatch.setattr(_lib, "Producer_try_send_cb", self)

    def _error(self, cls: type, message: str) -> int:
        handle: int = _lib.KafkaError_new(cls._ffi_id, message)  # noqa: SLF001
        self.handles.append(handle)
        return handle

    def __call__(self, c_producer: int, native: Any, on_completion: Any, resolve: Any) -> int:
        self.calls.append({"on_completion": on_completion, "resolve": resolve,
                           "thread": threading.get_ident()})
        if self.answer == "taken":
            resolve(0)
        elif self.answer == "refused":
            resolve(self._error(*self.refusal))
        elif self.answer == "rejected":
            return self._error(KafkaError, REJECTED)
        else:
            assert self.answer == "queued"
        return 0


def _deliver(call: dict[str, Any], *, failed: bool) -> None:
    """The core fires the record's delivery callback as the completion thread
    does: a BORROWED error handle, freed after the call, and callback id 0, as
    the faked send registered no report with the core."""
    error = _lib.KafkaError_new(KafkaTimeoutError._ffi_id, BOOM) if failed else 0  # noqa: SLF001
    try:
        call["on_completion"](None if failed else ACKNOWLEDGED, error, 0)
    finally:
        if error:
            _lib.KafkaError_destroy(error)


def _deliver_on_a_thread(call: dict[str, Any], *, failed: bool) -> int:
    """:func:`_deliver` on a thread of its own, joined; the thread's ident."""
    thread = threading.Thread(target=_deliver, args=(call,), kwargs={"failed": failed})
    thread.start()
    thread.join(10)
    assert not thread.is_alive()
    assert thread.ident is not None
    return thread.ident


def _recorder(seen: Seen, returned: list[bool], raising: BaseException | None = None) -> Any:
    """A callback recording the thread it runs on, whether ``send()`` had
    returned by then (``returned`` is set by the test once it has), the
    record's partition and its exception; raising ``raising`` when given."""
    def callback(metadata: RecordMetadata, exception: Exception | None) -> None:
        seen.append((threading.get_ident(), bool(returned), metadata.partition(), exception))
        if raising is not None:
            raise raising
    return callback


def _core_callback_errors(capfd: pytest.CaptureFixture[str]) -> list[str]:
    """The core's log lines for a raising send() callback, from stderr."""
    return [line.split("] ", 1)[1] for line in capfd.readouterr().err.splitlines()
            if "Error executing user-provided callback" in line]


def _python_callback_errors(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == "confluent_kafka.producer"]


def _freed_once(freed: list[int], fake: FakeTrySend) -> bool:
    """Every error handle the fake made was freed, each exactly once."""
    return bool(fake.handles) and all(freed.count(handle) == 1 for handle in fake.handles)


# ===========================================================================
# KafkaProducer
# ===========================================================================


@pytest.mark.parametrize("failed", [False, True])
def test_a_record_taken_at_once_is_returned_pending_and_completes_on_the_completion_thread(
        monkeypatch: pytest.MonkeyPatch, failed: bool) -> None:
    # Java's send() appends the record to its batch on the calling thread and
    # returns its future, which the I/O thread completes (KafkaProducer.java:
    # 1029-1030, Sender). The producer taking the record at once is the same
    # shape: resolve(0) runs inside Producer_try_send_cb, send() returns the
    # pending future, tracked for flush(), and the delivery callback runs on
    # the completion thread, before the future completes.
    fake = FakeTrySend(monkeypatch, "taken")
    seen: Seen = []
    returned: list[bool] = []
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        started = time.monotonic()
        future = p.send(record=RECORD, callback=_recorder(seen, returned))
        returned.append(True)
        assert time.monotonic() - started < 1.0
        assert len(fake.calls) == 1
        assert not future.done()
        assert future in p._futures  # noqa: SLF001
        assert seen == []
        deliverer = _deliver_on_a_thread(fake.calls[0], failed=failed)
        assert deliverer != threading.get_ident()
        if failed:
            exception = future.exception(timeout=5)
            assert type(exception) is KafkaTimeoutError
            assert str(exception) == BOOM
            assert seen == [(deliverer, True, -1, exception)]
        else:
            assert future.result(timeout=5).offset() == 7
            assert seen == [(deliverer, True, 0, None)]
        assert future not in p._futures  # noqa: SLF001
    finally:
        p.close(timeout=0)


@pytest.mark.parametrize("raising", [False, True])
def test_a_record_refused_at_once_runs_its_callback_inside_send_on_the_calling_thread(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
        capfd: pytest.CaptureFixture[str], freed_errors: list[int], raising: bool) -> None:
    # With no handover to wait for, a record the Rust send() returns an error
    # for completes with it, its callback first (see Producer.send: Java
    # rethrows it, Python gives it to the callback and the future, a
    # deviation). Refused as send() hands it over, that happens inside
    # send(), on the calling thread, where the completion thread does it for
    # a record refused after being queued. A raising callback is logged by
    # Python, once, as no core report exists for this completion.
    fake = FakeTrySend(monkeypatch, "refused")
    blown = RuntimeError("callback blew up")
    seen: Seen = []
    returned: list[bool] = []
    p = KafkaProducer(configs=UNREACHABLE)
    capfd.readouterr()
    try:
        with caplog.at_level(logging.ERROR, logger="confluent_kafka.producer"):
            future = p.send(record=RECORD, callback=_recorder(seen, returned,
                                                               blown if raising else None))
            returned.append(True)
        assert len(seen) == 1
        thread, after_return, partition, exception = seen[0]
        assert (thread, after_return, partition) == (threading.get_ident(), False, -1)
        assert type(exception) is KafkaError
        assert str(exception) == CLOSED_WHILE_SENDING_MESSAGE
        assert future.exception(timeout=0) is exception
        assert future not in p._futures  # noqa: SLF001
        assert _freed_once(freed_errors, fake)
    finally:
        p.close(timeout=0)
    records = _python_callback_errors(caplog)
    if raising:
        assert [r.getMessage() for r in records] == [CALLBACK_ERROR]
        assert records[0].exc_info is not None
        assert records[0].exc_info[1] is blown
    else:
        assert records == []
    assert _core_callback_errors(capfd) == []


def test_a_refused_transactional_send_raises_at_once_and_runs_no_callback(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int]) -> None:
    # Java's doSend rethrows the transaction manager's IllegalStateException
    # for a send outside a transaction (KafkaProducer.java:1151-1154) and the
    # callback does not run then. Python's send() waits for the handover of
    # such a record to raise it (see Producer.send); refused as it is handed
    # over, there is nothing to wait for: send() raises inside the call, and
    # the future, which it does not return, is not tracked for flush().
    fake = FakeTrySend(monkeypatch, "refused", (IllegalStateError, NOT_IN_A_TRANSACTION))
    seen: Seen = []
    p = KafkaProducer(configs=TRANSACTIONAL)
    try:
        started = time.monotonic()
        with pytest.raises(IllegalStateError) as raised:
            p.send(record=RECORD, callback=_recorder(seen, []))
        assert time.monotonic() - started < 1.0
        assert str(raised.value) == NOT_IN_A_TRANSACTION
        assert len(fake.calls) == 1
        assert seen == []
        assert p._futures == set()  # noqa: SLF001
        assert _freed_once(freed_errors, fake)
    finally:
        p.close(timeout=0)


def test_a_record_the_c_api_rejects_raises_and_calls_neither_callback(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int]) -> None:
    # The C API's own refusal, before the record is queued or tried: the OWNED
    # error is returned, raised by send(), and neither callback runs, as for
    # Producer_send_cb.
    fake = FakeTrySend(monkeypatch, "rejected")
    seen: Seen = []
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        with pytest.raises(KafkaError) as raised:
            p.send(record=RECORD, callback=_recorder(seen, []))
        assert str(raised.value) == REJECTED
        assert seen == []
        assert p._futures == set()  # noqa: SLF001
        assert _freed_once(freed_errors, fake)
    finally:
        p.close(timeout=0)


# ===========================================================================
# AsyncKafkaProducer
# ===========================================================================


@pytest.mark.parametrize("failed", [False, True])
async def test_an_async_record_taken_at_once_is_returned_pending_and_completes_on_the_loop(
        monkeypatch: pytest.MonkeyPatch, failed: bool) -> None:
    # As for KafkaProducer, with the callback run on the event loop (never on
    # the completion thread, which only hands the completion to the loop).
    fake = FakeTrySend(monkeypatch, "taken")
    seen: Seen = []
    returned: list[bool] = []
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        future = await p.send(record=RECORD, callback=_recorder(seen, returned))
        returned.append(True)
        assert len(fake.calls) == 1
        assert not future.done()
        assert future in p._futures  # noqa: SLF001
        assert seen == []
        deliverer = _deliver_on_a_thread(fake.calls[0], failed=failed)
        assert deliverer != threading.get_ident()
        if failed:
            with pytest.raises(KafkaTimeoutError, match=BOOM) as raised:
                await asyncio.wait_for(future, 5)
            assert seen == [(threading.get_ident(), True, -1, raised.value)]
        else:
            assert (await asyncio.wait_for(future, 5)).offset() == 7
            assert seen == [(threading.get_ident(), True, 0, None)]
        assert future not in p._futures  # noqa: SLF001
    finally:
        await p.close(timeout=0)


@pytest.mark.parametrize("raising", [False, True])
async def test_an_async_record_refused_at_once_runs_its_callback_inside_send_on_the_loop(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
        capfd: pytest.CaptureFixture[str], freed_errors: list[int], raising: bool) -> None:
    # As for KafkaProducer: the callback runs inside send(), on the loop, with
    # no hop through the loop's queue (a callback scheduled before send() has
    # not run when it returns), and the future is done when send() returns.
    fake = FakeTrySend(monkeypatch, "refused")
    blown = RuntimeError("callback blew up")
    seen: Seen = []
    returned: list[bool] = []
    hopped: list[int] = []
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    capfd.readouterr()
    try:
        with caplog.at_level(logging.ERROR, logger="confluent_kafka.producer"):
            asyncio.get_running_loop().call_soon(hopped.append, 1)
            future = await p.send(record=RECORD, callback=_recorder(seen, returned,
                                                                     blown if raising else None))
            returned.append(True)
        assert hopped == []
        assert len(seen) == 1
        thread, after_return, partition, exception = seen[0]
        assert (thread, after_return, partition) == (threading.get_ident(), False, -1)
        assert type(exception) is KafkaError
        assert str(exception) == CLOSED_WHILE_SENDING_MESSAGE
        assert future.done()
        assert future.exception() is exception
        assert _freed_once(freed_errors, fake)
        # An asyncio future runs its done callbacks on the loop's next turn:
        # the one untracking it has not run yet.
        await asyncio.sleep(0)
        assert future not in p._futures  # noqa: SLF001
    finally:
        await p.close(timeout=0)
    records = _python_callback_errors(caplog)
    if raising:
        assert [r.getMessage() for r in records] == [CALLBACK_ERROR]
        assert records[0].exc_info is not None
        assert records[0].exc_info[1] is blown
    else:
        assert records == []
    assert _core_callback_errors(capfd) == []


async def test_a_refused_async_transactional_send_raises_at_once_without_a_hop_to_the_loop(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int]) -> None:
    # As for KafkaProducer. The handover future is resolved on the loop
    # directly, inside Producer_try_send_cb, and awaiting a done future does
    # not yield: send() raises before anything scheduled on the loop runs.
    fake = FakeTrySend(monkeypatch, "refused", (IllegalStateError, NOT_IN_A_TRANSACTION))
    seen: Seen = []
    hopped: list[int] = []
    p = AsyncKafkaProducer(configs=TRANSACTIONAL)
    try:
        asyncio.get_running_loop().call_soon(hopped.append, 1)
        with pytest.raises(IllegalStateError) as raised:
            await p.send(record=RECORD, callback=_recorder(seen, []))
        assert hopped == []
        assert str(raised.value) == NOT_IN_A_TRANSACTION
        assert len(fake.calls) == 1
        assert seen == []
        assert p._futures == set()  # noqa: SLF001
        assert _freed_once(freed_errors, fake)
    finally:
        await p.close(timeout=0)


async def test_an_async_record_the_c_api_rejects_raises_and_calls_neither_callback(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int]) -> None:
    fake = FakeTrySend(monkeypatch, "rejected")
    seen: Seen = []
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        with pytest.raises(KafkaError) as raised:
            await p.send(record=RECORD, callback=_recorder(seen, []))
        assert str(raised.value) == REJECTED
        assert seen == []
        assert p._futures == set()  # noqa: SLF001
        assert _freed_once(freed_errors, fake)
    finally:
        await p.close(timeout=0)


# ===========================================================================
# The entry point in use
# ===========================================================================


def _spied(monkeypatch: pytest.MonkeyPatch, name: str) -> list[int]:
    """The calls of ``_lib.<name>`` (each still made), as a growing list."""
    calls: list[int] = []
    real = getattr(_lib, name)

    def spy(*args: Any) -> Any:
        calls.append(1)
        return real(*args)

    monkeypatch.setattr(_lib, name, spy)
    return calls


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_send_goes_through_the_non_blocking_entry_point_only(
        monkeypatch: pytest.MonkeyPatch, asynchronous: bool) -> None:
    # Both families send through Producer_try_send_cb; Producer_send_cb, the
    # queued-only twin, serves the C binding and is never called from Python.
    # Against an unreachable broker the record cannot be taken at once (its
    # topic's metadata is missing), so it is queued, and the core fails it
    # after max.block.ms as every other test here sees.
    send_cb = _spied(monkeypatch, "Producer_send_cb")
    try_send_cb = _spied(monkeypatch, "Producer_try_send_cb")
    if asynchronous:
        ap = AsyncKafkaProducer(configs=UNREACHABLE)
        try:
            with pytest.raises(KafkaTimeoutError):
                await asyncio.wait_for(await ap.send(record=RECORD), 30)
        finally:
            await ap.close()
    else:
        with KafkaProducer(configs=UNREACHABLE) as p:
            assert type(p.send(record=RECORD).exception(timeout=30)) is KafkaTimeoutError
    assert (len(send_cb), len(try_send_cb)) == (0, 1)
