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
answers of ``Producer_send_cb``, the C extension's send, each
faked here without a broker (test/integration/test_kafka_producer_broker.py
drives the same paths against one). The entry point's contract::

    _lib.Producer_send_cb(handle, record, on_completion, resolve) -> owned_error_int

    taken at once     resolve(0) runs inside the call, on the calling thread;
                      on_completion runs later, from the completion thread
    refused at once   resolve(owned_error_int) runs inside the call, on the
                      calling thread; on_completion runs later only for a
                      record that had reached the accumulator, once it
                      completes
    queued            neither runs inside the call: resolve runs from the
                      callback pump once the record is handed to the producer
    rejected          the C API refuses the record before either: its OWNED
                      error is returned and neither runs

and what ``send()`` makes of each answer:

- taken: the future is tracked and returned pending;
- refused, at once or once queued: ``send()`` raises the error, the callback
  does not run and nothing is tracked, as Java's
  ``org.apache.kafka.clients.producer.KafkaProducer#doSend`` rethrows it;
- queued: ``send()`` waits for the handover, as Java's waits in ``doSend``;
- rejected: ``send()`` raises the C API's error.

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
from confluent_kafka._preview import PREVIEW_WARNING
from confluent_kafka.common import KafkaError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.producer import (
    AsyncKafkaProducer, KafkaProducer, ProducerRecord, RecordMetadata,
)
from confluent_kafka.producer._base import CLOSED_WHILE_SENDING_MESSAGE
from confluent_kafka.producer._send import MetadataTuple

from .producer_futures import (
    assert_send_throws,
    async_assert_send_throws,
    async_failure,
    failure,
)

TOPIC = "topic"
RECORD = ProducerRecord(topic=TOPIC, key=b"k", value=b"v")
UNREACHABLE = {"bootstrap.servers": "127.0.0.1:59999", "max.block.ms": 100}
TRANSACTIONAL = {**UNREACHABLE, "transactional.id": "txn"}

# The transaction manager's refusal of a send outside a transaction, which the
# Rust send() returns (Java's doSend rethrows it) -- with Java's double space.
NOT_IN_A_TRANSACTION = "Cannot add partition topic-0 to transaction while in state  READY"
# The two refusals: a KafkaException and an IllegalStateException, both
# rethrown by Java's doSend.
REFUSALS = [
    pytest.param(UNREACHABLE, (KafkaError, CLOSED_WHILE_SENDING_MESSAGE),
                 id="closed-while-sending"),
    pytest.param(TRANSACTIONAL, (IllegalStateError, NOT_IN_A_TRANSACTION),
                 id="outside-a-transaction"),
]
# What the C API returns for a record it refuses before queuing it.
REJECTED = "Cannot perform operation after producer has been closed"
# The error of a delivery that fails.
BOOM = "boom"
# The delivery of a record the broker acknowledged at offset 7.
ACKNOWLEDGED: MetadataTuple = (TOPIC, 0, 7, 1234, 1, 1)

Seen = list[tuple[int, bool, int, Exception | None]]


class FakeSendCb:
    """A fake of ``_lib.Producer_send_cb`` giving the answer the C extension
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
        monkeypatch.setattr(_lib, "Producer_send_cb", self)

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


def _recorder(seen: Seen, returned: list[bool]) -> Any:
    """A callback recording the thread it runs on, whether ``send()`` had
    returned by then (``returned`` is set by the test once it has), the
    record's partition and its exception."""
    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        seen.append((threading.get_ident(), bool(returned), metadata.partition(), exception))
    return callback


def _core_callback_errors(capfd: pytest.CaptureFixture[str]) -> list[str]:
    """The core's log lines for a raising send() callback, from stderr."""
    return [line.split("] ", 1)[1] for line in capfd.readouterr().err.splitlines()
            if "Error executing user-provided callback" in line]


def _producer_log(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """What the producer logged, but for the preview warning of its construction."""
    return [r for r in caplog.records if r.name == "confluent_kafka.producer"
            and r.getMessage() != PREVIEW_WARNING]


def _freed_once(freed: list[int], fake: FakeSendCb) -> bool:
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
    # 1110-1111, Sender). The producer taking the record at once is the same
    # shape: resolve(0) runs inside Producer_send_cb, send() returns the
    # pending future, tracked for flush(), and the delivery callback runs on
    # the completion thread, before the future completes.
    fake = FakeSendCb(monkeypatch, "taken")
    seen: Seen = []
    returned: list[bool] = []
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        started = time.monotonic()
        future = p.send(record=RECORD, callback=_recorder(seen, returned))
        returned.append(True)
        assert time.monotonic() - started < 1.0
        assert len(fake.calls) == 1
        assert not future.is_done()
        assert future._impl in p._futures  # noqa: SLF001
        assert seen == []
        deliverer = _deliver_on_a_thread(fake.calls[0], failed=failed)
        assert deliverer != threading.get_ident()
        if failed:
            exception = failure(future, 5)
            assert type(exception) is KafkaTimeoutError
            assert str(exception) == BOOM
            assert seen == [(deliverer, True, -1, exception)]
        else:
            assert future.get(timeout=5).offset() == 7
            assert seen == [(deliverer, True, 0, None)]
        assert future._impl not in p._futures  # noqa: SLF001
    finally:
        p.close(timeout=0)


@pytest.mark.parametrize(("configs", "refusal"), REFUSALS)
def test_a_record_refused_at_once_raises_inside_send_and_runs_no_callback(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
        capfd: pytest.CaptureFixture[str], freed_errors: list[int],
        configs: dict[str, Any], refusal: tuple[type[Exception], str]) -> None:
    # Java's org.apache.kafka.clients.producer.KafkaProducer#doSend rethrows an
    # error that is not an ApiException, as the
    # closing metadata's KafkaException or the transaction manager's
    # IllegalStateException for a send outside a transaction, and the callback
    # does not run then. Refused as it is handed
    # over, inside Producer_send_cb, the record leaves send() nothing to
    # wait for: send() raises inside the call, the future, which it does not
    # return, is not tracked for flush(), and nothing is logged.
    fake = FakeSendCb(monkeypatch, "refused", refusal)
    cls, message = refusal
    seen: Seen = []
    p = KafkaProducer(configs=configs)
    capfd.readouterr()
    try:
        started = time.monotonic()
        with caplog.at_level(logging.ERROR, logger="confluent_kafka.producer"), \
                pytest.raises(cls) as raised:
            p.send(record=RECORD, callback=_recorder(seen, []))
        assert time.monotonic() - started < 1.0
        assert type(raised.value) is cls
        assert str(raised.value) == message
        assert len(fake.calls) == 1
        assert seen == []
        assert p._futures == set()  # noqa: SLF001
        assert _freed_once(freed_errors, fake)
    finally:
        p.close(timeout=0)
    assert _producer_log(caplog) == []
    assert _core_callback_errors(capfd) == []


def test_a_record_the_c_api_rejects_raises_and_calls_neither_callback(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int]) -> None:
    # The C API's own refusal, before the record is queued or tried: the OWNED
    # error is returned, send() raises it, and neither callback runs.
    fake = FakeSendCb(monkeypatch, "rejected")
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
    fake = FakeSendCb(monkeypatch, "taken")
    seen: Seen = []
    returned: list[bool] = []
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        future = await p.send(record=RECORD, callback=_recorder(seen, returned))
        returned.append(True)
        assert len(fake.calls) == 1
        assert not future.is_done()
        assert future._impl in p._futures  # noqa: SLF001
        assert seen == []
        deliverer = _deliver_on_a_thread(fake.calls[0], failed=failed)
        assert deliverer != threading.get_ident()
        if failed:
            exception = await async_failure(future, 5)
            assert type(exception) is KafkaTimeoutError
            assert str(exception) == BOOM
            assert seen == [(threading.get_ident(), True, -1, exception)]
        else:
            assert (await future.get(timeout=5)).offset() == 7
            assert seen == [(threading.get_ident(), True, 0, None)]
        assert future._impl not in p._futures  # noqa: SLF001
    finally:
        await p.close(timeout=0)


@pytest.mark.parametrize(("configs", "refusal"), REFUSALS)
async def test_an_async_record_refused_at_once_raises_inside_send_without_a_hop_to_the_loop(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
        capfd: pytest.CaptureFixture[str], freed_errors: list[int],
        configs: dict[str, Any], refusal: tuple[type[Exception], str]) -> None:
    # As for KafkaProducer. The handover future is resolved on the loop
    # directly, inside Producer_send_cb, and send() does not await a done
    # future: it raises before anything scheduled on the loop runs.
    fake = FakeSendCb(monkeypatch, "refused", refusal)
    cls, message = refusal
    seen: Seen = []
    hopped: list[int] = []
    p = AsyncKafkaProducer(configs=configs)
    capfd.readouterr()
    try:
        with caplog.at_level(logging.ERROR, logger="confluent_kafka.producer"):
            asyncio.get_running_loop().call_soon(hopped.append, 1)
            with pytest.raises(cls) as raised:
                await p.send(record=RECORD, callback=_recorder(seen, []))
            assert hopped == []
        assert type(raised.value) is cls
        assert str(raised.value) == message
        assert len(fake.calls) == 1
        assert seen == []
        assert p._futures == set()  # noqa: SLF001
        assert _freed_once(freed_errors, fake)
    finally:
        await p.close(timeout=0)
    assert _producer_log(caplog) == []
    assert _core_callback_errors(capfd) == []


async def test_an_async_record_the_c_api_rejects_raises_and_calls_neither_callback(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int]) -> None:
    fake = FakeSendCb(monkeypatch, "rejected")
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
async def test_send_goes_through_producer_send_cb_only(
        monkeypatch: pytest.MonkeyPatch, asynchronous: bool) -> None:
    # Both families send through Producer_send_cb, which tries the producer on
    # the calling thread first; the blocking Producer_send is called only from
    # a delivery callback (see test_producer_family.py,
    # test_a_send_from_a_callback_calls_the_blocking_send).
    # Against an unreachable broker the record cannot be taken at once (its
    # topic's metadata is missing), so it is queued, and send() returns once
    # the core failed it after max.block.ms, as every other test here sees.
    send = _spied(monkeypatch, "Producer_send")
    send_cb = _spied(monkeypatch, "Producer_send_cb")
    if asynchronous:
        ap = AsyncKafkaProducer(configs=UNREACHABLE)
        try:
            await async_assert_send_throws(await ap.send(record=RECORD), KafkaTimeoutError, 30)
        finally:
            await ap.close()
    else:
        with KafkaProducer(configs=UNREACHABLE) as p:
            assert_send_throws(p.send(record=RECORD), KafkaTimeoutError, 30)
    assert (len(send), len(send_cb)) == (0, 1)
