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

"""The send path of the real producer around the producer's answers, without a
broker (test_producer_family.py has the answers themselves; the broker tests in
test/integration drive the same paths against a real broker).

``send()`` hands the record to the Rust producer on the calling thread
(``Producer_send``): taken at once (True), queued until the producer takes or
refuses it (None, the answer coming through the same callback as ``cb(None,
error)``), or refused on the calling thread (the error handle). A taken
record's delivery report comes later, as ``cb(metadata, error)`` on the
producer's completion thread, or on the event loop for the async class.

Covered here: a refusal whose delivery report still comes, a serializer that
raises, a ``close()`` from a signal handler or from a delivery callback while
another ``send()`` waits, a completion for a cancelled asyncio future, two
event loops sending through one producer, the lifetime of the (record,
callback, producer) pin ``Producer_send`` holds, and the one free of every
refusal's error handle.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import signal
import threading
import time
import weakref
from types import FrameType
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.common import KafkaError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.producer import (
    AsyncKafkaProducer,
    KafkaProducer,
    ProducerRecord,
    RecordMetadata,
)

from .conftest import (
    CLOSED,
    CLOSED_PRODUCER_REFUSALS,
    NOT_IN_A_TRANSACTION,
    RECORD,
    TOPIC,
    UNREACHABLE,
    await_until,
    close_the_rust_producer_behind_the_wrapper,
    fake_taken_send,
    failed_delivery,
    is_closed_producer_refusal,
    refusal,
    send_on_a_thread,
    spy_on,
    wait_until,
)

# A record with a partition of its own: a failure's metadata carries it
# (KafkaProducer.AppendCallbacks.topicPartition()).
PARTITIONED = ProducerRecord(topic=TOPIC, partition=3, key=b"k", value=b"v")
# Long enough for a queued record to still wait inside the producer while the
# test acts on it.
PATIENT: dict[str, Any] = {**UNREACHABLE, "max.block.ms": 10000}

REFUSAL_PATHS = ["refused on the calling thread", "queued, then refused"]


def _chain(error: BaseException) -> list[BaseException]:
    """``error`` and its causes: one FFI handle each (from_ffi_error turns the
    core's ``kafka_common_Error_source`` chain into ``__cause__``, freeing
    every handle of the chain)."""
    chain: list[BaseException] = []
    cause: BaseException | None = error
    while cause is not None:
        chain.append(cause)
        cause = cause.__cause__
    return chain


def _refusing_send(monkeypatch: pytest.MonkeyPatch, path: str, handle: int) -> dict[str, Any]:
    """Fakes a Producer_send that refuses the record with ``handle``, on the
    calling thread or (``path`` "queued, then refused") after queueing it, the
    answer coming from another thread as the dispatcher's would; the callback
    is kept, for the delivery report the test makes later."""
    captured: dict[str, Any] = {}

    def producer_send(c_producer: int, native: Any, cb: Any) -> int | None:
        captured["cb"] = cb
        if path == "queued, then refused":
            threading.Timer(0.05, cb, (None, handle)).start()
            return None
        return handle

    monkeypatch.setattr(_lib, "Producer_send", producer_send)
    return captured


# ===========================================================================
# A refusal whose delivery report still comes
# ===========================================================================
#
# Java 4.x finds some transaction-state errors only once the record is in a
# batch (KafkaProducer.java:1029-1038 appends before maybeAddPartition): send()
# throws, and the batch's completion still runs the callback once, with the
# record's partition in its metadata. The FFI reports such a refusal with the
# delivery callback pending, and Producer_send keeps the callback for it.

@pytest.mark.parametrize("path", REFUSAL_PATHS)
def test_a_refusal_whose_delivery_report_still_comes_runs_the_callback_once(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int], path: str) -> None:
    handle = refusal()
    captured = _refusing_send(monkeypatch, path, handle)
    p = KafkaProducer(configs=UNREACHABLE)
    seen: list[tuple[int, RecordMetadata, Exception | None]] = []
    ran = threading.Event()

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        seen.append((threading.get_ident(), md, e))
        ran.set()

    try:
        with pytest.raises(IllegalStateError) as err:
            p.send(record=PARTITIONED, callback=callback)
        assert str(err.value) == NOT_IN_A_TRANSACTION
        assert freed_errors == [handle]
        # The record is not tracked: flush() has nothing to wait for, and the
        # callback has not run.
        assert not p._futures  # noqa: SLF001
        start = time.monotonic()
        p.flush()
        assert time.monotonic() - start < 1
        assert not ran.is_set()
        # The batch completes: the delivery report, from the completion
        # thread, runs the callback once, with the record's partition.
        delivery = failed_delivery()
        completion_thread = threading.Thread(target=captured["cb"], args=(0, delivery))
        completion_thread.start()
        completion_thread.join(10)
        assert ran.wait(10)
        ((thread, md, e),) = seen
        assert thread == completion_thread.ident
        assert (md.topic(), md.partition(), md.offset(), md.timestamp()) == (TOPIC, 3, -1, -1)
        assert not md.has_offset()
        assert type(e) is KafkaTimeoutError and str(e) == "boom"
        assert freed_errors == [handle, delivery]
        time.sleep(0.05)
        assert len(seen) == 1
    finally:
        p.close(timeout=0)


@pytest.mark.parametrize("path", REFUSAL_PATHS)
async def test_an_async_refusal_whose_delivery_report_still_comes_runs_the_callback_once(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int], path: str) -> None:
    handle = refusal()
    captured = _refusing_send(monkeypatch, path, handle)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    loop_thread = threading.get_ident()
    seen: list[tuple[int, RecordMetadata, Exception | None]] = []

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        seen.append((threading.get_ident(), md, e))

    try:
        with pytest.raises(IllegalStateError) as err:
            await p.send(record=PARTITIONED, callback=callback)
        assert str(err.value) == NOT_IN_A_TRANSACTION
        assert freed_errors == [handle]
        assert not p._futures  # noqa: SLF001
        await asyncio.wait_for(p.flush(), 5)
        assert seen == []
        # The delivery report, from the completion thread, reaches the
        # callback on the loop.
        delivery = failed_delivery()
        threading.Thread(target=captured["cb"], args=(0, delivery)).start()
        await await_until(lambda: bool(seen))
        ((thread, md, e),) = seen
        assert thread == loop_thread
        assert (md.topic(), md.partition(), md.offset()) == (TOPIC, 3, -1)
        assert type(e) is KafkaTimeoutError and str(e) == "boom"
        assert freed_errors == [handle, delivery]
        await asyncio.sleep(0.05)
        assert len(seen) == 1
    finally:
        await p.close(timeout=0)


# ===========================================================================
# A serializer that raises
# ===========================================================================
#
# Java's doSend serializes the key and the value before the append and throws
# the serializer's exception from send() (a ClassCastException wrapped as
# SerializationException, any other as itself). Nothing reaches the producer:
# no record to deliver, no future, nothing for flush() or close() to wait for.

def _raising_serializer(topic: str, data: object, headers: object = None) -> bytes:
    raise ValueError(f"cannot serialize {data!r} for {topic}")


def test_a_raising_serializer_fails_the_send_before_the_hand_over(
        monkeypatch: pytest.MonkeyPatch) -> None:
    answers = spy_on(monkeypatch, "Producer_send")
    p = KafkaProducer(configs=UNREACHABLE, value_serializer=_raising_serializer)
    seen: list[Exception | None] = []
    try:
        with pytest.raises(ValueError) as err:
            p.send(record=RECORD, callback=lambda md, e: seen.append(e))
        assert str(err.value) == "cannot serialize b'v' for topic"
        assert answers == []
        assert not p._futures  # noqa: SLF001
        start = time.monotonic()
        p.flush()
        assert time.monotonic() - start < 1
    finally:
        start = time.monotonic()
        p.close()
        assert time.monotonic() - start < 5
    assert seen == []


async def test_a_raising_serializer_fails_the_async_send_before_the_hand_over(
        monkeypatch: pytest.MonkeyPatch) -> None:
    answers = spy_on(monkeypatch, "Producer_send")
    p = AsyncKafkaProducer(configs=UNREACHABLE, value_serializer=_raising_serializer)
    seen: list[Exception | None] = []
    try:
        with pytest.raises(ValueError) as err:
            await p.send(record=RECORD, callback=lambda md, e: seen.append(e))
        assert str(err.value) == "cannot serialize b'v' for topic"
        assert answers == []
        assert not p._futures  # noqa: SLF001
        await asyncio.wait_for(p.flush(), 1)
    finally:
        await asyncio.wait_for(p.close(), 5)
    assert seen == []


# ===========================================================================
# close() while a send() waits: from a signal handler, from a callback
# ===========================================================================

def test_a_close_from_the_signal_handler_ends_the_waiting_send(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int]) -> None:
    # A Ctrl+C handler that closes the producer instead of raising (a shutdown
    # hook, as Java's Runtime.addShutdownHook calling producer.close()):
    # close() runs on the main thread, inside the send()'s wait for the
    # producer's answer. The Rust close fails the record the send() waits for,
    # so the send() raises the closed refusal, as Java's send() throws when
    # close() ends its metadata wait; its handle is freed once, the native
    # producer destroyed once, and every later call reports the closed state.
    answers = spy_on(monkeypatch, "Producer_send")
    destroys = spy_on(monkeypatch, "Producer_destroy")
    p = KafkaProducer(configs=PATIENT)
    closes: list[float] = []
    armed = True

    def on_sigint(signum: int, frame: FrameType | None) -> None:
        if armed:
            start = time.monotonic()
            p.close()
            closes.append(time.monotonic() - start)

    def interrupt_once_queued() -> None:
        wait_until(lambda: answers == [None])
        os.kill(os.getpid(), signal.SIGINT)

    previous = signal.signal(signal.SIGINT, on_sigint)
    interrupter = threading.Thread(target=interrupt_once_queued)
    try:
        interrupter.start()
        with pytest.raises((KafkaError, IllegalStateError)) as err:
            p.send(record=RECORD)
    finally:
        interrupter.join(30)
        armed = False
        time.sleep(0.05)  # a signal still pending runs the disarmed handler
        signal.signal(signal.SIGINT, previous)
    assert is_closed_producer_refusal(err.value), str(err.value)
    assert len(closes) == 1 and closes[0] < 5, closes
    assert p._closed  # noqa: SLF001
    # The refusal's handles (the error and its causes), each freed once.
    assert len(freed_errors) == len(_chain(err.value)) == len(set(freed_errors)), freed_errors
    assert len(destroys) == 1
    with pytest.raises(IllegalStateError) as again:
        p.send(record=RECORD)
    assert str(again.value) == CLOSED
    p.close()  # a second close is harmless
    assert len(destroys) == 1


def test_a_close_from_a_delivery_callback_ends_another_threads_waiting_send(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    # A delivery callback that closes the producer (Java: close(0) from the
    # I/O thread, with the "Overriding close timeout" warning) while another
    # thread's send() waits inside the producer: the Rust close fails that
    # record, the waiting send() raises the closed refusal, and nothing
    # deadlocks, since the close waits for no completion the callback's own
    # thread would have to run. The first record is a fake the test completes
    # itself, standing in for the completion thread; the second is real and
    # queued at the unreachable broker.
    real = _lib.Producer_send
    captured: dict[str, Any] = {}
    answers: list[Any] = []

    def producer_send(c_producer: int, native: Any, cb: Any) -> Any:
        if "cb" not in captured:
            captured["cb"] = cb
            threading.Timer(0.01, cb, (None, 0)).start()  # accepted after a short wait
            return None
        result = real(c_producer, native, cb)
        answers.append(result)
        return result

    monkeypatch.setattr(_lib, "Producer_send", producer_send)
    p = KafkaProducer(configs=PATIENT)
    closed_in_callback = threading.Event()

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        p.close()
        closed_in_callback.set()

    with caplog.at_level(logging.WARNING, logger="confluent_kafka.producer"):
        first = p.send(record=RECORD, callback=callback)
        assert first in p._futures  # noqa: SLF001
        blocked, outcome = send_on_a_thread(p)
        try:
            wait_until(lambda: answers == [None])  # queued: send() waits for the answer
            delivery = failed_delivery()
            completion_thread = threading.Thread(target=captured["cb"], args=(0, delivery))
            completion_thread.start()
            completion_thread.join(10)
            assert not completion_thread.is_alive(), "the callback's close() did not return"
            blocked.join(10)
            assert not blocked.is_alive(), "the waiting send() did not return after the close"
        finally:
            p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
            p.close(timeout=0)
    assert closed_in_callback.is_set() and p._closed  # noqa: SLF001
    (error,) = outcome
    assert is_closed_producer_refusal(error), outcome
    assert type(first.exception(timeout=0)) is KafkaTimeoutError
    assert str(first.exception(timeout=0)) == "boom"
    assert [r.getMessage() for r in caplog.records if r.name == "confluent_kafka.producer"] == [
        "Overriding close timeout 9223372036854775807 ms to 0 ms in order to prevent useless "
        "blocking due to self-join. This means you have incorrectly invoked close with a "
        "non-zero timeout from the producer call-back."]
    with pytest.raises(IllegalStateError) as err:
        p.send(record=RECORD)
    assert str(err.value) == CLOSED


# ===========================================================================
# Completions and the event loops
# ===========================================================================

async def test_a_completion_for_a_cancelled_future_runs_the_callback_and_frees_its_error(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int]) -> None:
    # Java's FutureRecordMetadata.cancel() returns false; an asyncio future
    # can be cancelled by its awaiter, and the record is in the producer all
    # the same. Its completion runs the callback (Java runs the callbacks of
    # every record of the batch), leaves the cancelled future alone (no
    # InvalidStateError on the loop) and frees its error handle once; the
    # next record's completion is unaffected.
    cbs = fake_taken_send(monkeypatch)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda loop, context: loop_errors.append(context))
    seen: list[tuple[str, Exception | None]] = []
    try:
        first = await p.send(record=RECORD, callback=lambda md, e: seen.append(("first", e)))
        assert first.cancel()
        await asyncio.sleep(0)  # the future's done callback untracks it
        assert not p._futures  # noqa: SLF001
        delivery = failed_delivery()
        threading.Thread(target=cbs[0], args=(0, delivery)).start()
        await await_until(lambda: len(seen) == 1)
        assert first.cancelled()
        name, e = seen[0]
        assert name == "first" and type(e) is KafkaTimeoutError and str(e) == "boom"
        assert freed_errors == [delivery]
        second = await p.send(record=RECORD, callback=lambda md, e: seen.append(("second", e)))
        threading.Thread(target=cbs[1], args=(0, 0)).start()
        md = await asyncio.wait_for(second, 10)
        assert seen[1] == ("second", None)
        assert (md.topic(), md.partition(), md.offset()) == (TOPIC, -1, -1)
        await asyncio.sleep(0.05)
        assert loop_errors == []
    finally:
        loop.set_exception_handler(None)
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        await p.close(timeout=0)


def test_completions_resolve_on_the_loop_that_sent_the_record(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # One AsyncKafkaProducer, two event loops on two threads (the producer is
    # thread-safe; an asyncio future may only be completed on its own loop):
    # each record's callback runs and its future resolves on the loop that
    # sent it, nothing stays pending, and a third loop closes the producer.
    cbs = fake_taken_send(monkeypatch)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    sent = [threading.Event() for _ in range(2)]
    loop_threads: dict[int, int] = {}
    callback_threads: dict[int, int] = {}
    results: dict[int, RecordMetadata] = {}

    def on_a_loop(i: int) -> None:
        async def main() -> None:
            loop_threads[i] = threading.get_ident()

            def callback(md: RecordMetadata, e: Exception | None) -> None:
                callback_threads[i] = threading.get_ident()

            future = await p.send(record=RECORD, callback=callback)
            sent[i].set()
            results[i] = await asyncio.wait_for(future, 10)

        asyncio.run(main())

    loops = [threading.Thread(target=on_a_loop, args=(i,)) for i in range(2)]
    for thread in loops:
        thread.start()
    try:
        for event in sent:
            assert event.wait(10)
        for cb in cbs:  # the completion thread reports both records
            cb(0, 0)
        for thread in loops:
            thread.join(10)
        assert not any(thread.is_alive() for thread in loops)
        assert sorted(results) == [0, 1]
        assert callback_threads == loop_threads
        assert len(set(loop_threads.values())) == 2
        assert not p._pending  # noqa: SLF001
    finally:
        asyncio.run(p.close(timeout=0))
    assert p._closed  # noqa: SLF001


# ===========================================================================
# The pin: (record, callback, producer), held until the completion is reported
# ===========================================================================
#
# Producer_send pins the record, the callback and the producer in a tuple until
# the completion is reported: a queued record's key and value buffers are
# borrowed until the producer takes them. The callback closure send() builds
# is referenced by that pin and by send()'s own frame only, so a weak reference
# to it dies with the pin once send() has returned.

def _pins(monkeypatch: pytest.MonkeyPatch) -> list[weakref.ref[Any]]:
    """Weak references to the callback closure each send() hands
    Producer_send (the call goes through)."""
    refs: list[weakref.ref[Any]] = []
    real = _lib.Producer_send

    def producer_send(c_producer: int, native: Any, cb: Any) -> Any:
        refs.append(weakref.ref(cb))
        return real(c_producer, native, cb)

    monkeypatch.setattr(_lib, "Producer_send", producer_send)
    return refs


def _send_keeping_no_frames(p: KafkaProducer[Any, Any],
                            record: ProducerRecord[Any, Any]) -> tuple[type, str] | None:
    """``p.send(record)``, keeping neither the error it raises nor its
    traceback (which would keep send()'s frame, and so its callback closure,
    alive): the error's type and message, or None for a future."""
    try:
        p.send(record=record)
    except Exception as error:  # noqa: BLE001 - the test reads which one
        return type(error), str(error)
    return None


def test_the_pin_of_a_record_refused_on_the_calling_thread_is_released_at_once(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # No completion is coming for a record refused on the calling thread: the
    # refusal releases the pin, so the closure dies as send() raises.
    refs = _pins(monkeypatch)
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        close_the_rust_producer_behind_the_wrapper(p)
        assert _send_keeping_no_frames(p, RECORD) == (IllegalStateError, CLOSED)
        gc.collect()
        assert [ref() for ref in refs] == [None]
    finally:
        p.close(timeout=0)


def test_the_pin_of_a_queued_record_is_released_when_close_refuses_it(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # A queued record is pinned while it waits inside the producer; the close
    # that refuses it releases the pin, as no completion is coming.
    refs = _pins(monkeypatch)
    answers = spy_on(monkeypatch, "Producer_send")
    p = KafkaProducer(configs=PATIENT)
    outcome: list[tuple[type, str] | None] = []
    sender = threading.Thread(target=lambda: outcome.append(_send_keeping_no_frames(p, RECORD)))
    sender.start()
    try:
        wait_until(lambda: answers == [None])  # queued: send() waits for the answer
        gc.collect()
        assert [ref() is not None for ref in refs] == [True]
        p.close(timeout=0)
        sender.join(10)
        assert not sender.is_alive(), "the waiting send() did not return after the close"
        (raised,) = outcome
        assert raised is not None and raised[0] in (KafkaError, IllegalStateError), raised
        assert raised[1] in CLOSED_PRODUCER_REFUSALS, raised
        gc.collect()
        assert [ref() for ref in refs] == [None]
    finally:
        sender.join(10)
        p.close(timeout=0)


def test_the_pin_of_a_record_the_producer_took_lives_until_its_delivery_report(
        monkeypatch: pytest.MonkeyPatch, hold_completion: Any) -> None:
    # A record the producer took has a completion coming: the pin lives until
    # the delivery report has run (here held on the completion thread), then
    # the report's trampoline releases it.
    held = hold_completion("Producer_send", holds=lambda payload: payload[0] is not None)
    refs = _pins(monkeypatch)  # on top: sees send()'s own closure, which the hold then wraps
    p = KafkaProducer(configs=UNREACHABLE)
    seen: list[Exception | None] = []
    try:
        # Taken after its metadata wait failed (an ApiException: the future
        # fails, the callback runs); the report is held.
        future = p.send(record=RECORD, callback=lambda md, e: seen.append(e))
        assert future in p._futures  # noqa: SLF001
        wait_until(lambda: bool(held.payloads))
        gc.collect()
        assert [ref() is not None for ref in refs] == [True]
        assert not future.done() and seen == []
        held.release.set()
        assert held.delivered.wait(10)
        assert type(future.exception(timeout=10)) is KafkaTimeoutError
        assert len(seen) == 1
        wait_until(lambda: refs[0]() is None, timeout=10)
    finally:
        held.release.set()
        p.close(timeout=0)


# ===========================================================================
# Every refusal's error handle is freed exactly once
# ===========================================================================
#
# The handle of the error the producer refuses a record with is owned by
# whoever raises it: from_ffi_error frees it as send() raises, after the
# handles of its causes. One free per handle, whether Producer_send returned
# it (a refusal on the calling thread) or the producer answered with it later
# (cb(None, handle)).

def _refusals(monkeypatch: pytest.MonkeyPatch) -> tuple[list[Any], list[int]]:
    """The results of Producer_send and the error handles the producer refused
    records with, on the calling thread or in its later answer."""
    results: list[Any] = []
    refused: list[int] = []
    real = _lib.Producer_send

    def producer_send(c_producer: int, native: Any, cb: Any) -> Any:
        def answering(result: int | None, error: int) -> None:
            if result is None and error:
                refused.append(error)
            cb(result, error)

        result = real(c_producer, native, answering)
        results.append(result)
        if type(result) is int:
            refused.append(result)
        return result

    monkeypatch.setattr(_lib, "Producer_send", producer_send)
    return results, refused


@pytest.mark.parametrize("path", REFUSAL_PATHS)
def test_a_refusals_error_handle_is_freed_exactly_once(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int], path: str) -> None:
    results, refused = _refusals(monkeypatch)
    p = KafkaProducer(configs=PATIENT)
    try:
        if path == "refused on the calling thread":
            close_the_rust_producer_behind_the_wrapper(p)
            with pytest.raises(IllegalStateError) as err:
                p.send(record=RECORD)
            error: BaseException = err.value
            assert str(error) == CLOSED
            assert results == refused
        else:
            sender, outcome = send_on_a_thread(p)
            wait_until(lambda: results == [None])
            p.close(timeout=0)
            sender.join(10)
            assert not sender.is_alive(), "the waiting send() did not return after the close"
            (error,) = outcome
            assert is_closed_producer_refusal(error), outcome
        assert len(refused) == 1
        assert freed_errors[-1:] == refused
        assert len(freed_errors) == len(_chain(error)) == len(set(freed_errors)), freed_errors
    finally:
        p.close(timeout=0)


@pytest.mark.parametrize("path", REFUSAL_PATHS)
async def test_an_async_refusals_error_handle_is_freed_exactly_once(
        monkeypatch: pytest.MonkeyPatch, freed_errors: list[int], path: str) -> None:
    results, refused = _refusals(monkeypatch)
    p = AsyncKafkaProducer(configs=PATIENT)
    try:
        if path == "refused on the calling thread":
            close_the_rust_producer_behind_the_wrapper(p)
            with pytest.raises(IllegalStateError) as err:
                await p.send(record=RECORD)
            error: BaseException = err.value
            assert str(error) == CLOSED
            assert results == refused
        else:
            blocked = asyncio.ensure_future(p.send(record=RECORD))
            await await_until(lambda: results == [None])
            await p.close(timeout=0)
            with pytest.raises((KafkaError, IllegalStateError)) as refusal_err:
                await asyncio.wait_for(blocked, 10)
            error = refusal_err.value
            assert is_closed_producer_refusal(error), str(error)
        assert len(refused) == 1
        assert freed_errors[-1:] == refused
        assert len(freed_errors) == len(_chain(error)) == len(set(freed_errors)), freed_errors
    finally:
        await p.close(timeout=0)
