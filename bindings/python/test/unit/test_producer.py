# Copyright 2025 Confluent Inc.
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

"""Test suite for the Confluent Kafka Rust Python bindings."""

import asyncio
import gc
import os
import threading
import time
import pytest
import _confluentkafka as _lib
from producer import (
    KafkaProducer, MockProducer, ProducerRecord, RecordMetadata, KafkaError,
    AsyncKafkaProducer, AsyncMockProducer
)
from consumer import MockConsumer, TopicPartition, OffsetAndMetadata

# Errors::TransactionAbortable (code 120): the one code whose txn_requires_abort()
# is True, used to exercise the abortable-commit path.
TXN_ABORTABLE_CODE = 120
# A non-abortable error code (RequestTimedOut) for the contrast assertion.
NON_ABORTABLE_CODE = 7

# Timeout in seconds for future.result() calls. Not derived from any
# production timeout value -- purely how long the test waits before
# declaring a future broken. Overridable so CI can widen it on a
# resource-constrained runner without touching the assertions.
FUTURE_TIMEOUT = float(os.environ.get("CONFLUENT_KAFKA_TEST_FUTURE_TIMEOUT", "2"))

# Time for the batch thread to dispatch records (batch interval is 10ms)
BATCH_DISPATCH = 0.02

# Backpressure bound (PRODUCER_MAX_ACCUMULATED_RECORDS in _confluentkafka.c):
# the producer blocks once this many records are accumulated un-taken.
BACKPRESSURE_BOUND = 1000


# -- MockProducer lifecycle ---------------------------------------------------

def test_create_mock_producer_auto_complete():
    p = MockProducer(auto_complete=True)
    assert p.c_producer is not None
    p.close()


def test_create_mock_producer_manual():
    p = MockProducer(auto_complete=False)
    assert p.c_producer is not None
    p.close()


# -- Send with auto-complete --------------------------------------------------

def test_send_with_key_and_value():
    with MockProducer(auto_complete=True) as p:
        record = ProducerRecord("test-topic", b"value", b"key")
        future = p.send(record)
        meta = future.result(timeout=FUTURE_TIMEOUT)
        assert future.done()
        assert isinstance(meta, RecordMetadata)
        assert meta.topic() == "test-topic"
        assert meta.offset() == 0
        assert meta.partition() == 0


def test_send_value_only():
    with MockProducer(auto_complete=True) as p:
        record = ProducerRecord("test-topic", b"value")
        future = p.send(record)
        meta = future.result(timeout=FUTURE_TIMEOUT)
        assert meta.topic() == "test-topic"


def test_send_with_key_none():
    with MockProducer(auto_complete=True) as p:
        record = ProducerRecord("test-topic", b"value", None)
        future = p.send(record)
        meta = future.result(timeout=FUTURE_TIMEOUT)
        assert meta.offset() == 0


# -- RecordMetadata -----------------------------------------------------------

def test_metadata_fields():
    with MockProducer(auto_complete=True) as p:
        future = p.send(ProducerRecord("my-topic", b"v", b"k"))
        meta = future.result(timeout=FUTURE_TIMEOUT)
        assert meta.topic() == "my-topic"
        assert meta.offset() == 0
        assert meta.partition() == 0
        assert meta.timestamp() == -1


def test_multiple_sends_incrementing_offsets():
    with MockProducer(auto_complete=True) as p:
        futures = []
        for i in range(3):
            f = p.send(ProducerRecord("test-topic", f"v{i}".encode()))
            futures.append(f)
        offsets = [f.result(timeout=FUTURE_TIMEOUT).offset() for f in futures]
        assert offsets == [0, 1, 2]


# -- Manual completion --------------------------------------------------------

def _sync_complete_next_when_ready(p, timeout=FUTURE_TIMEOUT):
    """Retry complete_next() until the C batching thread (10ms interval) has
    queued the record. A single fixed sleep races that thread: on a loaded
    machine it can elapse before the record is queued, so complete_next()
    silently returns False and the future then hangs until FUTURE_TIMEOUT.
    (Sync counterpart of the async _complete_next_when_ready below.)"""
    deadline = time.monotonic() + timeout
    while not p.complete_next():
        assert time.monotonic() < deadline, "complete_next() never found a pending completion"
        time.sleep(0.005)


def _sync_error_next_when_ready(p, error_code, error_message, timeout=FUTURE_TIMEOUT):
    """Error-path counterpart of _sync_complete_next_when_ready."""
    deadline = time.monotonic() + timeout
    while not p.error_next(error_code, error_message):
        assert time.monotonic() < deadline, "error_next() never found a pending completion"
        time.sleep(0.005)


def test_manual_complete_next():
    p = MockProducer(auto_complete=False)
    future = p.send(ProducerRecord("test-topic", b"v"))
    assert not future.done()
    _sync_complete_next_when_ready(p)
    meta = future.result(timeout=FUTURE_TIMEOUT)
    assert future.done()
    assert isinstance(meta, RecordMetadata)
    assert meta.offset() == 0
    p.close()


def test_manual_error_next():
    p = MockProducer(auto_complete=False)
    future = p.send(ProducerRecord("test-topic", b"v"))
    _sync_error_next_when_ready(p, 2, "test error")
    with pytest.raises(KafkaError) as exc_info:
        future.result(timeout=FUTURE_TIMEOUT)
    err = exc_info.value
    assert err.code == 2
    assert err.message == "test error"
    assert isinstance(err.is_retriable, bool)
    assert isinstance(err.is_fatal, bool)
    p.close()


def test_manual_error_next_null_message():
    p = MockProducer(auto_complete=False)
    future = p.send(ProducerRecord("test-topic", b"v"))
    _sync_error_next_when_ready(p, 2, None)
    with pytest.raises(KafkaError) as exc_info:
        future.result(timeout=FUTURE_TIMEOUT)
    assert exc_info.value.code == 2
    p.close()


# -- Delivery callback (on_delivery) ------------------------------------------
#
# Java's send(record, Callback) fires the callback exactly once per record on the
# producer's I/O thread. Here it fires on the C completion thread for the sync
# producer, and on the event loop (inside the completion drain) for the async
# one. The obligation holds on every path — including a cancelled or already
# resolved Future — and a raising callback must not escape into the C caller.

def test_on_delivery_success():
    with MockProducer(auto_complete=True) as p:
        got = []
        fired = threading.Event()

        def on_delivery(metadata, exception):
            got.append((metadata, exception))
            fired.set()

        future = p.send(ProducerRecord("cb-topic", b"v", b"k"),
                        on_delivery=on_delivery)
        future.result(timeout=FUTURE_TIMEOUT)
        # The callback runs after the future is resolved, on the same thread.
        assert fired.wait(FUTURE_TIMEOUT)
        (meta, err), = got
        assert err is None
        assert isinstance(meta, RecordMetadata)
        assert meta.topic() == "cb-topic"
        assert meta.offset() == 0
        assert meta.partition() == 0


def test_on_delivery_error():
    p = MockProducer(auto_complete=False)
    got = []
    fired = threading.Event()

    def on_delivery(metadata, exception):
        got.append((metadata, exception))
        fired.set()

    future = p.send(ProducerRecord("test-topic", b"v"), on_delivery=on_delivery)
    _sync_error_next_when_ready(p, 2, "delivery failed")
    with pytest.raises(KafkaError):
        future.result(timeout=FUTURE_TIMEOUT)
    assert fired.wait(FUTURE_TIMEOUT)
    (meta, err), = got
    assert meta is None
    assert isinstance(err, KafkaError)
    assert err.code == 2
    assert err.message == "delivery failed"
    p.close()


def test_on_delivery_fires_when_future_cancelled():
    # Callback obligation: Java fires the callback regardless of what the caller
    # did with the returned future, so a cancelled future must not suppress it.
    p = MockProducer(auto_complete=False)
    got = []
    fired = threading.Event()

    def on_delivery(metadata, exception):
        got.append((metadata, exception))
        fired.set()

    future = p.send(ProducerRecord("test-topic", b"v"), on_delivery=on_delivery)
    assert future.cancel()
    _sync_complete_next_when_ready(p)
    assert fired.wait(FUTURE_TIMEOUT), "on_delivery must fire for a cancelled future"
    (meta, err), = got
    assert err is None
    assert meta is not None and meta.offset() == 0
    assert future.cancelled()
    p.close()


def test_on_delivery_exception_does_not_break_future_or_producer():
    with MockProducer(auto_complete=True) as p:
        fired = threading.Event()

        def on_delivery(metadata, exception):
            fired.set()
            raise RuntimeError("callback blew up")

        future = p.send(ProducerRecord("test-topic", b"v"), on_delivery=on_delivery)
        # The future is resolved before the callback runs, so it is unaffected.
        assert future.result(timeout=FUTURE_TIMEOUT).offset() == 0
        assert fired.wait(FUTURE_TIMEOUT)
        # And the completion thread survived: the next send still completes.
        assert p.send(ProducerRecord("test-topic", b"v2")).result(
            timeout=FUTURE_TIMEOUT).offset() == 1


def test_on_delivery_none_is_the_default():
    # No callback: unchanged behavior (the pre-existing tests cover this, but
    # assert the keyword is genuinely optional).
    with MockProducer(auto_complete=True) as p:
        assert p.send(ProducerRecord("test-topic", b"v"),
                      on_delivery=None).result(timeout=FUTURE_TIMEOUT).offset() == 0


# -- Flush and close ----------------------------------------------------------

def test_flush():
    with MockProducer(auto_complete=True) as p:
        p.send(ProducerRecord("test-topic", b"v"))
        p.flush()  # Should not raise


def test_partitions_for():
    # Exercises the async-FFI path (Producer_partitions_for_async) waited on by
    # the sync Producer's threading.Event. The mock has no topics, so the result
    # is an empty list rather than an error.
    with MockProducer(auto_complete=True) as p:
        assert p.partitions_for("test-topic") == []


def test_close():
    p = MockProducer(auto_complete=True)
    future = p.send(ProducerRecord("test-topic", b"v"))
    future.result(timeout=FUTURE_TIMEOUT)
    p.close()
    with pytest.raises(RuntimeError):
        p.send(ProducerRecord("test-topic", b"v2"))


def test_close_idempotent():
    p = MockProducer(auto_complete=True)
    p.close()
    p.close()  # Should not raise


def test_close_with_send_in_flight():
    # Regression: close() must not deadlock when a send is still in flight and
    # has not completed on its own (auto_complete=False). The C close path
    # flushes outstanding records before joining the poll-futures thread, so
    # the never-completing future resolves instead of blocking the join.
    p = MockProducer(auto_complete=False)
    p.send(ProducerRecord("test-topic", b"v"))
    time.sleep(BATCH_DISPATCH)  # ensure the record is in flight at close time
    p.close()
    assert p.closed


# -- Mock operations ----------------------------------------------------------

def test_history_count():
    with MockProducer(auto_complete=True) as p:
        futures = []
        for i in range(3):
            futures.append(p.send(ProducerRecord("test-topic", f"v{i}".encode())))
        for f in futures:
            f.result(timeout=FUTURE_TIMEOUT)
        assert p.history_count() == 3


def test_clear():
    with MockProducer(auto_complete=True) as p:
        futures = []
        for i in range(3):
            futures.append(p.send(ProducerRecord("test-topic", f"v{i}".encode())))
        for f in futures:
            f.result(timeout=FUTURE_TIMEOUT)
        p.clear()
        assert p.history_count() == 0


# -- Error handling -----------------------------------------------------------

def test_kafka_error_properties():
    p = MockProducer(auto_complete=False)
    future = p.send(ProducerRecord("test-topic", b"v"))
    _sync_error_next_when_ready(p, 2, "corrupt message")
    with pytest.raises(KafkaError) as exc_info:
        future.result(timeout=FUTURE_TIMEOUT)
    err = exc_info.value
    assert err.code == 2
    assert err.message == "corrupt message"
    assert isinstance(err.is_retriable, bool)
    assert isinstance(err.is_fatal, bool)
    p.close()


def test_send_after_close_raises():
    p = MockProducer(auto_complete=True)
    p.close()
    with pytest.raises(RuntimeError):
        p.send(ProducerRecord("test-topic", b"v"))


def test_metrics_after_close_raises():
    p = MockProducer(auto_complete=True)
    p.close()
    with pytest.raises(RuntimeError):
        p.metrics()


def _c_entered_after_close(*_args):
    # Stub swapped in for an FFI entry point AFTER close(): close() frees the C
    # producer struct, so any FFI call reaching it would read freed memory. A
    # method must raise the Python "closed" RuntimeError before getting here.
    # (Raising AssertionError, not RuntimeError, so pytest.raises(RuntimeError)
    # cannot mistake an FFI entry for the expected guard.)
    raise AssertionError("FFI entered after close()")


def test_flush_after_close_raises(monkeypatch):
    # A bare pytest.raises(RuntimeError) has no teeth here: flush() also
    # re-checks closed AFTER the drain wait. The stub proves the guard fires
    # BEFORE the drain wait touches the (freed) C producer.
    p = MockProducer(auto_complete=True)
    p.close()
    monkeypatch.setattr(_lib, "Producer_on_drained", _c_entered_after_close)
    with pytest.raises(RuntimeError, match="closed"):
        p.flush()


def test_partitions_for_after_close_raises(monkeypatch):
    p = MockProducer(auto_complete=True)
    p.close()
    monkeypatch.setattr(_lib, "Producer_partitions_for_async", _c_entered_after_close)
    with pytest.raises(RuntimeError, match="closed"):
        p.partitions_for("test-topic")


def test_mock_helpers_after_close_raise(monkeypatch):
    # The MockProducer test helpers hand the C pointer to the FFI too.
    p = MockProducer(auto_complete=True)
    p.close()
    for symbol in ("MockProducer_complete_next", "MockProducer_error_next",
                   "MockProducer_history_count", "MockProducer_clear"):
        monkeypatch.setattr(_lib, symbol, _c_entered_after_close)
    with pytest.raises(RuntimeError, match="closed"):
        p.complete_next()
    with pytest.raises(RuntimeError, match="closed"):
        p.error_next(1)
    with pytest.raises(RuntimeError, match="closed"):
        p.history_count()
    with pytest.raises(RuntimeError, match="closed"):
        p.clear()


# -- Context manager ----------------------------------------------------------

def test_context_manager():
    with MockProducer(auto_complete=True) as p:
        future = p.send(ProducerRecord("test-topic", b"v"))
        meta = future.result(timeout=FUTURE_TIMEOUT)
        assert isinstance(meta, RecordMetadata)
    # After with block, producer is closed
    assert p.closed


# -- ProducerRecord -----------------------------------------------------------

def test_producer_record_all_fields():
    r = ProducerRecord("t", b"v", b"k", 2, 1000)
    assert r.topic == "t"
    assert r.value == b"v"
    assert r.key == b"k"
    assert r.partition == 2
    assert r.timestamp == 1000


def test_producer_record_defaults():
    r = ProducerRecord("t", b"v")
    assert r.topic == "t"
    assert r.value == b"v"
    assert r.partition is None
    assert r.timestamp is None


def test_producer_record_invalid_topic_type():
    with pytest.raises(TypeError):
        ProducerRecord(123, b"v")


def test_producer_record_invalid_value_type():
    with pytest.raises(TypeError):
        ProducerRecord("t", "not bytes")


# -- KafkaProducer lifecycle ---------------------------------------------------

def test_create_kafka_producer():
    p = KafkaProducer({"bootstrap.servers": "localhost:9092"})
    assert p.c_producer is not None
    p.close()


def test_create_kafka_producer_context_manager():
    with KafkaProducer({"bootstrap.servers": "localhost:9092"}) as p:
        assert p.c_producer is not None
    assert p.closed


def test_kafka_producer_close_idempotent():
    p = KafkaProducer({"bootstrap.servers": "localhost:9092"})
    p.close()
    p.close()


def test_kafka_producer_send_after_close_raises():
    p = KafkaProducer({"bootstrap.servers": "localhost:9092"})
    p.close()
    with pytest.raises(RuntimeError):
        p.send(ProducerRecord("test-topic", b"v"))


def test_kafka_producer_invalid_config():
    with pytest.raises(RuntimeError):
        KafkaProducer({"batch.size": "not-a-number"})


def test_kafka_producer_config_not_dict():
    with pytest.raises(TypeError):
        KafkaProducer("bootstrap.servers=localhost:9092")


def test_kafka_producer_multiple_configs():
    p = KafkaProducer({
        "bootstrap.servers": "localhost:9092",
        "client.id": "python-test",
        "batch.size": "32768",
    })
    p.close()


# =============================================================================
# AsyncProducer tests
#
# These mirror the sync MockProducer tests above, exercising the asyncio-native
# producer. The C machinery is identical; only the completion callback differs
# (it marshals results back onto the event loop via call_soon_threadsafe).
# `asyncio_mode = "auto"` (pyproject.toml) runs `async def test_*` directly.
# =============================================================================


# -- AsyncMockProducer lifecycle ----------------------------------------------

async def test_async_create_mock_producer_auto_complete():
    p = AsyncMockProducer(auto_complete=True)
    assert p.c_producer is not None
    await p.close()


async def test_async_create_mock_producer_manual():
    p = AsyncMockProducer(auto_complete=False)
    assert p.c_producer is not None
    await p.close()


# -- Send with auto-complete --------------------------------------------------

async def test_async_send_with_key_and_value():
    async with AsyncMockProducer(auto_complete=True) as p:
        record = ProducerRecord("test-topic", b"value", b"key")
        future = await p.send(record)
        meta = await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
        assert future.done()
        assert isinstance(meta, RecordMetadata)
        assert meta.topic() == "test-topic"
        assert meta.offset() == 0
        assert meta.partition() == 0


async def test_async_send_value_only():
    async with AsyncMockProducer(auto_complete=True) as p:
        record = ProducerRecord("test-topic", b"value")
        future = await p.send(record)
        meta = await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
        assert meta.topic() == "test-topic"


async def test_async_send_with_key_none():
    async with AsyncMockProducer(auto_complete=True) as p:
        record = ProducerRecord("test-topic", b"value", None)
        future = await p.send(record)
        meta = await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
        assert meta.offset() == 0


# -- RecordMetadata -----------------------------------------------------------

async def test_async_metadata_fields():
    async with AsyncMockProducer(auto_complete=True) as p:
        future = await p.send(ProducerRecord("my-topic", b"v", b"k"))
        meta = await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
        assert meta.topic() == "my-topic"
        assert meta.offset() == 0
        assert meta.partition() == 0
        assert meta.timestamp() == -1


async def test_async_multiple_sends_incrementing_offsets():
    async with AsyncMockProducer(auto_complete=True) as p:
        futures = [
            await p.send(ProducerRecord("test-topic", f"v{i}".encode()))
            for i in range(3)
        ]
        metas = await asyncio.wait_for(
            asyncio.gather(*futures), timeout=FUTURE_TIMEOUT)
        assert [m.offset() for m in metas] == [0, 1, 2]


# -- Manual completion --------------------------------------------------------

async def _complete_next_when_ready(p, timeout=FUTURE_TIMEOUT):
    """Retry ``complete_next()`` until it finds the queued record.

    ``complete_next()`` only succeeds once the C batching thread (10ms batch
    interval) has picked up the send and queued a completion on the mock
    producer. A single fixed sleep before calling it races that thread: on a
    loaded CI machine the sleep can elapse before the record is queued, so
    ``complete_next()`` silently returns ``False``, the record is queued a
    moment later with nobody left to complete it, and the future then hangs
    until ``FUTURE_TIMEOUT``. Retrying removes the race outright instead of
    widening the margin.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not p.complete_next():
        assert loop.time() < deadline, "complete_next() never found a pending completion"
        await asyncio.sleep(0.005)


async def _error_next_when_ready(p, error_code, error_message, timeout=FUTURE_TIMEOUT):
    """Same retry as :func:`_complete_next_when_ready`, for the error path."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not p.error_next(error_code, error_message):
        assert loop.time() < deadline, "error_next() never found a pending completion"
        await asyncio.sleep(0.005)


async def test_async_manual_complete_next():
    p = AsyncMockProducer(auto_complete=False)
    future = await p.send(ProducerRecord("test-topic", b"v"))
    await asyncio.sleep(BATCH_DISPATCH)
    assert not future.done()
    await _complete_next_when_ready(p)
    meta = await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
    assert future.done()
    assert isinstance(meta, RecordMetadata)
    assert meta.offset() == 0
    await p.close()


async def test_async_manual_error_next():
    p = AsyncMockProducer(auto_complete=False)
    future = await p.send(ProducerRecord("test-topic", b"v"))
    await asyncio.sleep(BATCH_DISPATCH)
    await _error_next_when_ready(p, 2, "test error")
    with pytest.raises(KafkaError) as exc_info:
        await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
    err = exc_info.value
    assert err.code == 2
    assert err.message == "test error"
    assert isinstance(err.is_retriable, bool)
    assert isinstance(err.is_fatal, bool)
    await p.close()


# -- Flush and close ----------------------------------------------------------

async def test_async_flush():
    async with AsyncMockProducer(auto_complete=True) as p:
        await p.send(ProducerRecord("test-topic", b"v"))
        await p.flush()  # Should not raise


async def test_async_partitions_for():
    # Exercises the async-FFI path awaited on the event loop
    # (Producer_partitions_for_async + _run_async). Empty mock -> empty list.
    async with AsyncMockProducer(auto_complete=True) as p:
        assert await p.partitions_for("test-topic") == []


async def test_async_close():
    p = AsyncMockProducer(auto_complete=True)
    future = await p.send(ProducerRecord("test-topic", b"v"))
    await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
    await p.close()
    with pytest.raises(RuntimeError):
        await p.send(ProducerRecord("test-topic", b"v2"))


async def test_async_close_idempotent():
    p = AsyncMockProducer(auto_complete=True)
    await p.close()
    await p.close()  # Should not raise


async def test_async_close_with_send_in_flight():
    # close() while a send has not yet completed must not raise or deadlock.
    # The C close path flushes outstanding records before joining the
    # poll-futures thread (see test_close_with_send_in_flight).
    p = AsyncMockProducer(auto_complete=False)
    await p.send(ProducerRecord("test-topic", b"v"))
    await asyncio.sleep(BATCH_DISPATCH)  # record is in flight at close time
    await p.close()
    assert p.closed


async def test_async_cancel_before_completion():
    # A future cancelled before the C callback fires is handled cleanly:
    # the completion path frees the C handles and does not error.
    p = AsyncMockProducer(auto_complete=False)
    future = await p.send(ProducerRecord("test-topic", b"v"))
    assert future.cancel()
    await _complete_next_when_ready(p)
    # Give the loop a chance to run the (no-op) scheduled completion.
    await asyncio.sleep(BATCH_DISPATCH)
    assert future.cancelled()
    await p.close()


# -- Delivery callback (async) -------------------------------------------------

async def test_async_on_delivery_success_on_loop_thread():
    async with AsyncMockProducer(auto_complete=True) as p:
        got = []
        loop_thread = threading.get_ident()

        def on_delivery(metadata, exception):
            got.append((metadata, exception, threading.get_ident()))

        future = await p.send(ProducerRecord("cb-topic", b"v", b"k"),
                              on_delivery=on_delivery)
        await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
        # The callback runs inside the completion drain, which is what resolves
        # the future — so it has already fired by the time the await resumes.
        (meta, err, thread), = got
        assert err is None
        assert meta.topic() == "cb-topic" and meta.offset() == 0
        assert thread == loop_thread, "on_delivery must run on the event loop"


async def test_async_on_delivery_error():
    p = AsyncMockProducer(auto_complete=False)
    got = []
    future = await p.send(ProducerRecord("test-topic", b"v"),
                          on_delivery=lambda m, e: got.append((m, e)))
    await _error_next_when_ready(p, 2, "async delivery failed")
    with pytest.raises(KafkaError):
        await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
    (meta, err), = got
    assert meta is None
    assert err.code == 2 and err.message == "async delivery failed"
    await p.close()


async def test_async_on_delivery_fires_when_future_cancelled():
    p = AsyncMockProducer(auto_complete=False)
    got = []
    future = await p.send(ProducerRecord("test-topic", b"v"),
                          on_delivery=lambda m, e: got.append((m, e)))
    assert future.cancel()
    await _complete_next_when_ready(p)
    for _ in range(200):
        if got:
            break
        await asyncio.sleep(0.01)
    assert len(got) == 1, "on_delivery must fire for a cancelled future"
    assert got[0][1] is None
    assert future.cancelled()
    await p.close()


async def test_async_on_delivery_exception_does_not_break_the_drain():
    async with AsyncMockProducer(auto_complete=True) as p:
        fired = []

        def on_delivery(metadata, exception):
            fired.append(metadata)
            raise RuntimeError("callback blew up")

        first = await p.send(ProducerRecord("test-topic", b"v"),
                             on_delivery=on_delivery)
        meta = await asyncio.wait_for(first, timeout=FUTURE_TIMEOUT)
        assert meta.offset() == 0
        assert len(fired) == 1
        # The drain survived, so subsequent completions still resolve.
        second = await p.send(ProducerRecord("test-topic", b"v2"))
        assert (await asyncio.wait_for(second, timeout=FUTURE_TIMEOUT)).offset() == 1


# -- Mock operations ----------------------------------------------------------

async def test_async_history_count():
    async with AsyncMockProducer(auto_complete=True) as p:
        futures = [
            await p.send(ProducerRecord("test-topic", f"v{i}".encode()))
            for i in range(3)
        ]
        await asyncio.wait_for(
            asyncio.gather(*futures), timeout=FUTURE_TIMEOUT)
        assert p.history_count() == 3


async def test_async_clear():
    async with AsyncMockProducer(auto_complete=True) as p:
        futures = [
            await p.send(ProducerRecord("test-topic", f"v{i}".encode()))
            for i in range(3)
        ]
        await asyncio.wait_for(
            asyncio.gather(*futures), timeout=FUTURE_TIMEOUT)
        p.clear()
        assert p.history_count() == 0


# -- Context manager ----------------------------------------------------------

async def test_async_context_manager():
    async with AsyncMockProducer(auto_complete=True) as p:
        future = await p.send(ProducerRecord("test-topic", b"v"))
        meta = await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
        assert isinstance(meta, RecordMetadata)
    # After the async with block, producer is closed
    assert p.closed


# -- AsyncKafkaProducer lifecycle ---------------------------------------------

async def test_async_create_kafka_producer():
    p = AsyncKafkaProducer({"bootstrap.servers": "localhost:9092"})
    assert p.c_producer is not None
    await p.close()


async def test_async_kafka_producer_context_manager():
    async with AsyncKafkaProducer(
            {"bootstrap.servers": "localhost:9092"}) as p:
        assert p.c_producer is not None
    assert p.closed


async def test_async_kafka_producer_close_idempotent():
    p = AsyncKafkaProducer({"bootstrap.servers": "localhost:9092"})
    await p.close()
    await p.close()


async def test_async_kafka_producer_send_after_close_raises():
    p = AsyncKafkaProducer({"bootstrap.servers": "localhost:9092"})
    await p.close()
    with pytest.raises(RuntimeError):
        await p.send(ProducerRecord("test-topic", b"v"))


async def test_async_kafka_producer_metrics_after_close_raises():
    p = AsyncKafkaProducer({"bootstrap.servers": "localhost:9092"})
    await p.close()
    with pytest.raises(RuntimeError):
        p.metrics()


async def test_async_flush_after_close_raises(monkeypatch):
    # See test_flush_after_close_raises: the stub proves the guard fires before
    # the drain wait touches the freed C producer.
    p = AsyncMockProducer(auto_complete=True)
    await p.close()
    monkeypatch.setattr(_lib, "Producer_on_drained", _c_entered_after_close)
    with pytest.raises(RuntimeError, match="closed"):
        await p.flush()


async def test_async_partitions_for_after_close_raises(monkeypatch):
    p = AsyncMockProducer(auto_complete=True)
    await p.close()
    monkeypatch.setattr(_lib, "Producer_partitions_for_async", _c_entered_after_close)
    with pytest.raises(RuntimeError, match="closed"):
        await p.partitions_for("test-topic")


async def test_async_kafka_producer_invalid_config():
    with pytest.raises(RuntimeError):
        AsyncKafkaProducer({"batch.size": "not-a-number"})


async def test_async_kafka_producer_config_not_dict():
    with pytest.raises(TypeError):
        AsyncKafkaProducer("bootstrap.servers=localhost:9092")


# =============================================================================
# Backpressure tests
#
# Once BACKPRESSURE_BOUND records are accumulated but not yet taken by the send
# task, the producer is "full" and further enqueuing waits for capacity. The
# mock accepts instantly and would never fill, so a test-only hook
# (`Producer_test_set_paused`) stalls the send task to build accumulation.
# =============================================================================


def _fill_to_bound_sync(p):
    """Send BACKPRESSURE_BOUND-1 records (none cross the bound, none block)."""
    for _ in range(BACKPRESSURE_BOUND - 1):
        p.send(ProducerRecord("test-topic", b"v"))


def test_backpressure_sync_blocks_until_drained():
    # With the send task paused, the send that crosses the bound blocks the
    # calling thread until capacity frees (mirrors Java send() on a full buffer).
    p = MockProducer(auto_complete=True)
    _lib.Producer_test_set_paused(p.c_producer, True)
    _fill_to_bound_sync(p)

    done = threading.Event()

    def crossing_send():
        p.send(ProducerRecord("test-topic", b"v"))
        done.set()

    t = threading.Thread(target=crossing_send)
    t.start()
    try:
        assert not done.wait(timeout=0.4), "send should block while paused"
        _lib.Producer_test_set_paused(p.c_producer, False)
        assert done.wait(timeout=FUTURE_TIMEOUT), "send should unblock on drain"
    finally:
        t.join(timeout=FUTURE_TIMEOUT)
        p.close()


def test_backpressure_sync_close_unblocks():
    # close() must release a sender blocked on backpressure rather than hang.
    p = MockProducer(auto_complete=True)
    _lib.Producer_test_set_paused(p.c_producer, True)
    _fill_to_bound_sync(p)

    done = threading.Event()
    t = threading.Thread(
        target=lambda: (p.send(ProducerRecord("test-topic", b"v")), done.set()))
    t.start()
    assert not done.wait(timeout=0.4)
    p.close()  # fires pending space waiters
    assert done.wait(timeout=FUTURE_TIMEOUT), "close must unblock the sender"
    t.join(timeout=FUTURE_TIMEOUT)


async def test_async_backpressure_suspends_until_drained():
    # The send that crosses the bound suspends (yields the loop) until the
    # send task drains; it must not block the loop.
    async with AsyncMockProducer(auto_complete=True) as p:
        _lib.Producer_test_set_paused(p.c_producer, True)
        for _ in range(BACKPRESSURE_BOUND - 1):
            await p.send(ProducerRecord("test-topic", b"v"))

        task = asyncio.ensure_future(p.send(ProducerRecord("test-topic", b"v")))
        await asyncio.sleep(0.3)
        assert not task.done(), "crossing send should suspend on backpressure"
        # The loop is still responsive while the send is suspended.
        assert await asyncio.sleep(0, result=True)

        _lib.Producer_test_set_paused(p.c_producer, False)
        await asyncio.wait_for(task, timeout=FUTURE_TIMEOUT)
        assert task.done()


async def test_async_backpressure_close_unblocks():
    # close() must release a suspended async sender, not hang.
    p = AsyncMockProducer(auto_complete=True)
    _lib.Producer_test_set_paused(p.c_producer, True)
    for _ in range(BACKPRESSURE_BOUND - 1):
        await p.send(ProducerRecord("test-topic", b"v"))

    task = asyncio.ensure_future(p.send(ProducerRecord("test-topic", b"v")))
    await asyncio.sleep(0.3)
    assert not task.done()
    await p.close()  # fires pending space waiters onto the loop
    await asyncio.wait_for(task, timeout=FUTURE_TIMEOUT)
    assert task.done()


async def test_backpressure_does_not_trigger_when_draining():
    # When the send task keeps up (not paused), sends below the bound never
    # block — backpressure is invisible.
    async with AsyncMockProducer(auto_complete=True) as p:
        futures = [
            await p.send(ProducerRecord("test-topic", b"v"))
            for _ in range(50)
        ]
        metas = await asyncio.wait_for(
            asyncio.gather(*futures), timeout=FUTURE_TIMEOUT)
        assert len(metas) == 50


# =============================================================================
# Producer transaction tests (mock-backed)
#
# Translated from Java MockProducerTest transaction tests. Each transactional
# test below hands its records over before the control op -- most by awaiting the
# send's .result(), the send/commit-race tests by relying on the control op's own
# _wait_drained(). Python's send() does NOT register synchronously: it appends the
# record to a C-side batch (the tray) that the background task later hands to Rust,
# so the control ops and flush() drain that tray first, reconstructing Java's
# synchronous doSend guarantee (see rules producer-transactions.md §13 and the
# send/commit-race tests below).
# (The transaction-control ops themselves are async-first -- they drive the
# *_async FFI variants -- but that is invisible to the public API.)
#
# The offset-lifecycle behaviour (sent-offsets flag, publish-on-commit,
# drop-on-abort) IS observable through the exposed
# `MockProducer_sent_offsets` / `MockProducer_committed_offset` hooks and is
# translated faithfully below. Only two Java categories are NOT translatable
# through the exposed C FFI surface:
#   * The state introspectors `commitCount()` and
#     `transactionInFlight/Committed/Aborted()` are not FFI-exposed, so tests
#     asserting *only* on them are covered via the exposed observables instead —
#     `history_count()` reflects committed-vs-aborted records (0 before commit,
#     1 after commit, 0 after abort).
#   * `fenceProducer()` has no FFI symbol, so the fenced-producer tests are not
#     translated.
# =============================================================================


# -- Sync happy / abort / empty -----------------------------------------------

def test_txn_init_begin_send_commit():
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        # §13: inside a transaction, produce with send() and await its result so
        # the record is handed to the producer before the commit.
        future = p.send(ProducerRecord("test-topic", b"value", b"key"))
        meta = future.result(timeout=FUTURE_TIMEOUT)
        assert isinstance(meta, RecordMetadata)
        assert meta.offset() == 0
        p.commit_transaction()  # must not raise


def test_txn_commit_empty():
    # Java MockProducerTest.shouldCommitEmptyTransaction (behavioral core; the
    # transactionCommitted()/transactionInFlight() introspectors are not
    # FFI-exposed).
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.commit_transaction()  # must not raise


def test_txn_abort_empty():
    # Java MockProducerTest.shouldAbortEmptyTransaction (behavioral core).
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.abort_transaction()  # must not raise


def test_txn_committed_records_in_history():
    # Java MockProducerTest.shouldCountCommittedTransaction, asserted via the
    # exposed history_count() (commitCount() is not FFI-exposed): a record sent
    # in a transaction only enters the sent history once the transaction commits.
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.send(ProducerRecord("test-topic", b"v")).result(timeout=FUTURE_TIMEOUT)
        assert p.history_count() == 0  # uncommitted before commit
        p.commit_transaction()
        assert p.history_count() == 1


def test_txn_aborted_records_discarded():
    # Java MockProducerTest.shouldNotCountAbortedTransaction: an aborted
    # transaction's records are discarded; only a committed transaction's
    # records reach the history.
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.send(ProducerRecord("test-topic", b"discarded")).result(timeout=FUTURE_TIMEOUT)
        p.abort_transaction()
        assert p.history_count() == 0

        p.begin_transaction()
        p.send(ProducerRecord("test-topic", b"kept")).result(timeout=FUTURE_TIMEOUT)
        p.commit_transaction()
        assert p.history_count() == 1


def test_txn_abort_then_reuse():
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.send(ProducerRecord("test-topic", b"v")).result(timeout=FUTURE_TIMEOUT)
        p.abort_transaction()
        # a fresh transaction can be started and committed after an abort
        p.begin_transaction()
        p.commit_transaction()


# -- Sync commit failure (txn_requires_abort / is_fatal) ----------------------

def test_txn_commit_failure_requires_abort():
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        assert _lib.MockProducer_set_commit_transaction_error(
            p.c_producer, False, TXN_ABORTABLE_CODE, "commit failed abortably")
        with pytest.raises(KafkaError) as exc_info:
            p.commit_transaction()
        err = exc_info.value
        assert err.code == TXN_ABORTABLE_CODE
        assert err.message == "commit failed abortably"
        assert err.txn_requires_abort is True
        assert err.is_fatal is False
        p.abort_transaction()  # recover


def test_txn_commit_failure_non_abortable():
    # Contrast with the abortable case: a non-abortable error reports
    # txn_requires_abort False, so the property is not vacuously always-True.
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        assert _lib.MockProducer_set_commit_transaction_error(
            p.c_producer, False, NON_ABORTABLE_CODE, "commit timed out")
        with pytest.raises(KafkaError) as exc_info:
            p.commit_transaction()
        err = exc_info.value
        assert err.code == NON_ABORTABLE_CODE
        assert err.message == "commit timed out"
        assert err.txn_requires_abort is False
        assert err.is_fatal is False
        p.abort_transaction()


# -- Sync send_offsets_to_transaction -----------------------------------------

def test_txn_send_offsets_to_transaction():
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    group_id = group_metadata.group_id
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        # Java shouldAddOffsetsWhenSendOffsetsToTransactionByGroupMetadata (:447):
        # the flag is False before offsets are staged, proving the False->True
        # transition rather than only the True-after state.
        assert _lib.MockProducer_sent_offsets(p.c_producer) is False
        offsets = {TopicPartition("t", 0): OffsetAndMetadata(5, metadata="m")}
        p.send_offsets_to_transaction(offsets, group_metadata)
        assert _lib.MockProducer_sent_offsets(p.c_producer) is True
        p.commit_transaction()
        # Round-trip: the offset staged for (group, topic, partition) is
        # recorded after the commit.
        committed = _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 0)
        assert committed is not None
        assert committed[0] == 5
        assert committed[2] == "m"
    consumer.close()


def test_txn_send_offsets_non_str_metadata_raises_type_error():
    # OffsetAndMetadata does no type validation and _offsets_to_spec forwards
    # oam.metadata verbatim (only None -> ""), so a non-str metadata reaches the
    # C marshaling loop. It must surface as a clean TypeError (NOT a SystemError
    # from the wrapper returning with an exception still pending) AND must not
    # stage the offsets: the loop bails before the FFI call, so the mock's
    # sent-offsets flag stays False.
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        assert _lib.MockProducer_sent_offsets(p.c_producer) is False
        offsets = {TopicPartition("t", 0): OffsetAndMetadata(5, metadata=b"x")}
        with pytest.raises(TypeError):
            p.send_offsets_to_transaction(offsets, group_metadata)
        # Rejected before the FFI call -> nothing staged.
        assert _lib.MockProducer_sent_offsets(p.c_producer) is False
        p.abort_transaction()  # leave the transaction in a clean state
    consumer.close()


def test_txn_send_offsets_empty_stages_nothing():
    # An empty offsets map is a legitimate count == 0 and stages nothing
    # (Java MockProducer.sendOffsetsToTransaction ignores empty maps).
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.send_offsets_to_transaction({}, group_metadata)
        assert _lib.MockProducer_sent_offsets(p.c_producer) is False
        p.commit_transaction()
    consumer.close()


# -- Offset lifecycle: sent-offsets flag / publish-on-commit / drop-on-abort --
# Translated from the Java MockProducerTest offset-lifecycle cases, asserted
# through the exposed MockProducer_sent_offsets / MockProducer_committed_offset
# hooks (the FFI analog of Java's consumerGroupOffsetsHistory()).


def test_txn_reset_sent_offsets_flag_only_when_beginning_new_transaction():
    # Java shouldResetSentOffsetsFlagOnlyWhenBeginningNewTransaction (:464):
    # commit() must NOT reset the sentOffsets flag; only begin_transaction() does.
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        assert _lib.MockProducer_sent_offsets(p.c_producer) is False

        group_commit = {TopicPartition("t", 0): OffsetAndMetadata(42)}
        p.send_offsets_to_transaction(group_commit, group_metadata)
        p.commit_transaction()  # commit must not reset the flag
        assert _lib.MockProducer_sent_offsets(p.c_producer) is True

        p.begin_transaction()  # begin resets it
        assert _lib.MockProducer_sent_offsets(p.c_producer) is False

        p.send_offsets_to_transaction(group_commit, group_metadata)
        p.commit_transaction()  # commit must not reset the flag
        assert _lib.MockProducer_sent_offsets(p.c_producer) is True

        p.begin_transaction()  # begin resets it
        assert _lib.MockProducer_sent_offsets(p.c_producer) is False
    consumer.close()


def test_txn_publish_latest_and_cumulative_offsets_only_after_commit():
    # Java shouldPublishLatestAndCumulativeConsumerGroupOffsetsOnlyAfterCommit...
    # (:492): two send_offsets calls for the same group merge cumulatively, and a
    # later offset for the same partition wins (partition 1: 73 -> 101). Nothing
    # is published until the transaction commits.
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    group_id = group_metadata.group_id
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.send_offsets_to_transaction(
            {TopicPartition("t", 0): OffsetAndMetadata(42),
             TopicPartition("t", 1): OffsetAndMetadata(73)}, group_metadata)
        p.send_offsets_to_transaction(
            {TopicPartition("t", 1): OffsetAndMetadata(101),
             TopicPartition("t", 2): OffsetAndMetadata(21)}, group_metadata)

        # Nothing is published before commit.
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 0) is None
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 1) is None
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 2) is None

        p.commit_transaction()
        # Cumulative merge across the two calls, latest-wins for partition 1.
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 0)[0] == 42
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 1)[0] == 101
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 2)[0] == 21
    consumer.close()


def test_txn_drop_consumer_group_offsets_on_abort():
    # Java shouldDropConsumerGroupOffsetsOnAbortIfTransactionsAreEnabled (:529):
    # offsets staged inside a transaction that is ABORTED are discarded — they
    # never reach the committed history, so a later empty commit publishes
    # nothing. Asserted via committed_offset(...) being None for the staged
    # (group, topic, partition) tuples.
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    group_id = group_metadata.group_id
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        group_commit = {
            TopicPartition("t", 0): OffsetAndMetadata(42),
            TopicPartition("t", 1): OffsetAndMetadata(73),
        }
        p.send_offsets_to_transaction(group_commit, group_metadata)
        p.abort_transaction()

        p.begin_transaction()
        p.commit_transaction()
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 0) is None
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 1) is None

        # Java repeats the abort cycle a second time; the outcome is unchanged.
        p.begin_transaction()
        p.send_offsets_to_transaction(group_commit, group_metadata)
        p.abort_transaction()

        p.begin_transaction()
        p.commit_transaction()
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 0) is None
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 1) is None
    consumer.close()


def test_txn_preserve_committed_offsets_on_later_abort():
    # Java shouldPreserveOffsetsFromCommitByGroupMetadataOnAbortIfTransactions...
    # (:583): offsets committed by one transaction survive a LATER transaction's
    # abort, while the aborted transaction's freshly staged offsets are dropped.
    #
    # Deviation: Java stages the second (aborted) transaction's offsets under a
    # *different* group ("g2") to show per-group isolation. The binding cannot
    # express two groups — MockConsumer.group_metadata() is fixed to
    # "dummy.group.id" and ConsumerGroupMetadata has no Python constructor — so
    # the second transaction stages additional partitions under the SAME group.
    # The observable behaviour (committed offsets preserved; the aborted staging
    # dropped) is identical and exercises the same commit/abort staging split.
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    group_id = group_metadata.group_id
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.send_offsets_to_transaction(
            {TopicPartition("t", 0): OffsetAndMetadata(42),
             TopicPartition("t", 1): OffsetAndMetadata(73)}, group_metadata)
        p.commit_transaction()

        p.begin_transaction()
        p.send_offsets_to_transaction(
            {TopicPartition("t", 2): OffsetAndMetadata(53),
             TopicPartition("t", 3): OffsetAndMetadata(84)}, group_metadata)
        p.abort_transaction()

        # Offsets committed by the first transaction are preserved ...
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 0)[0] == 42
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 1)[0] == 73
        # ... and the aborted transaction's staged offsets are dropped.
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 2) is None
        assert _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 3) is None
    consumer.close()


# -- Sync IllegalState paths (Java MockProducerTest; error-message asserted) ---

def test_txn_begin_before_init_raises():
    # shouldThrowOnBeginTransactionIfTransactionsNotInitialized
    with MockProducer(auto_complete=True) as p:
        with pytest.raises(KafkaError) as exc_info:
            p.begin_transaction()
        assert exc_info.value.message == \
            "MockProducer hasn't been initialized for transactions."


def test_txn_double_init_raises():
    # shouldThrowOnInitTransactionIfProducerAlreadyInitializedForTransactions
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        with pytest.raises(KafkaError) as exc_info:
            p.init_transactions()
        assert exc_info.value.message == \
            "MockProducer has already been initialized for transactions."


def test_txn_begin_twice_raises():
    # shouldThrowOnBeginTransactionsIfTransactionInflight
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        with pytest.raises(KafkaError) as exc_info:
            p.begin_transaction()
        assert exc_info.value.message == "Transaction already started"
        p.abort_transaction()


def test_txn_commit_before_init_raises():
    # shouldThrowOnCommitIfTransactionsNotInitialized
    with MockProducer(auto_complete=True) as p:
        with pytest.raises(KafkaError) as exc_info:
            p.commit_transaction()
        assert exc_info.value.message == \
            "MockProducer hasn't been initialized for transactions."


def test_txn_commit_without_begin_raises():
    # shouldThrowOnCommitTransactionIfNoTransactionGotStarted
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        with pytest.raises(KafkaError) as exc_info:
            p.commit_transaction()
        assert exc_info.value.message == "There is no open transaction."


def test_txn_abort_before_init_raises():
    # shouldThrowOnAbortIfTransactionsNotInitialized
    with MockProducer(auto_complete=True) as p:
        with pytest.raises(KafkaError) as exc_info:
            p.abort_transaction()
        assert exc_info.value.message == \
            "MockProducer hasn't been initialized for transactions."


def test_txn_abort_without_begin_raises():
    # shouldThrowOnAbortTransactionIfNoTransactionGotStarted
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        with pytest.raises(KafkaError) as exc_info:
            p.abort_transaction()
        assert exc_info.value.message == "There is no open transaction."


def test_txn_send_offsets_before_init_raises():
    # shouldThrowOnSendOffsetsToTransactionIfTransactionsNotInitialized
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    with MockProducer(auto_complete=True) as p:
        with pytest.raises(KafkaError) as exc_info:
            p.send_offsets_to_transaction(
                {TopicPartition("t", 0): OffsetAndMetadata(1)}, group_metadata)
        assert exc_info.value.message == \
            "MockProducer hasn't been initialized for transactions."
    consumer.close()


def test_txn_send_offsets_without_begin_raises():
    # shouldThrowOnSendOffsetsToTransactionTransactionIfNoTransactionGotStarted
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        with pytest.raises(KafkaError) as exc_info:
            p.send_offsets_to_transaction(
                {TopicPartition("t", 0): OffsetAndMetadata(1)}, group_metadata)
        assert exc_info.value.message == "There is no open transaction."
    consumer.close()


# -- Producer-closed transaction paths ----------------------------------------
# Java's shouldThrowOn{Init,Begin,Commit,Abort}TransactionIfProducerIsClosed and
# shouldThrowSendOffsetsToTransaction...IfProducerIsClosed throw
# IllegalStateException. The binding raises RuntimeError("Producer is already
# closed") instead: every txn method calls _check_closed() before reaching the
# FFI. This is an intentional, pre-existing, binding-wide divergence from Java's
# exception type (the same _check_closed guards send/flush/partitions_for), so
# these assert the binding's actual RuntimeError.


def test_txn_init_after_close_raises():
    # Java shouldThrowOnInitTransactionIfProducerIsClosed (:617).
    p = MockProducer(auto_complete=True)
    p.close()
    with pytest.raises(RuntimeError):
        p.init_transactions()


def test_txn_begin_after_close_raises():
    # Java shouldThrowOnBeginTransactionIfProducerIsClosed (:631).
    p = MockProducer(auto_complete=True)
    p.close()
    with pytest.raises(RuntimeError):
        p.begin_transaction()


def test_txn_commit_after_close_raises():
    # Java shouldThrowOnCommitTransactionIfProducerIsClosed (:652).
    p = MockProducer(auto_complete=True)
    p.close()
    with pytest.raises(RuntimeError):
        p.commit_transaction()


def test_txn_abort_after_close_raises():
    # Java shouldThrowOnAbortTransactionIfProducerIsClosed (:659).
    p = MockProducer(auto_complete=True)
    p.close()
    with pytest.raises(RuntimeError):
        p.abort_transaction()


def test_txn_send_offsets_after_close_raises():
    # Java shouldThrowSendOffsetsToTransactionBy{GroupId,GroupMetadata}...
    # IfProducerIsClosed (:638, :645) — one Python send_offsets form covers both.
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    p = MockProducer(auto_complete=True)
    p.close()
    with pytest.raises(RuntimeError):
        p.send_offsets_to_transaction(
            {TopicPartition("t", 0): OffsetAndMetadata(1)}, group_metadata)
    consumer.close()


# -- Un-awaited send drained before control ops (send/commit race) ------------
#
# Python's send() only appends the record to a C-side batch (the tray) that the
# background send task later hands to the Rust producer. Without draining, a
# returned-but-not-awaited send could reach Rust *after* a commit/abort/flush,
# landing in the wrong transaction (or surviving an abort). The control ops and
# flush() now wait via Producer_on_drained first, so a returned send is part of
# the operation -- Java's synchronous doSend guarantee. These tests send WITHOUT
# .result() and assert via the deterministic Rust-side observables
# (history_count / on_drained), which is exactly what the fix restores.


def test_txn_unawaited_send_is_committed():
    # The record is NOT awaited before commit. commit_transaction() drains it to
    # the producer first, so it is committed (history_count == 1). Before the fix
    # the record was still in the tray at commit time -> history_count == 0.
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.send(ProducerRecord("test-topic", b"v"))  # no .result()
        p.commit_transaction()
        assert p.history_count() == 1


def test_txn_unawaited_send_is_discarded_by_abort():
    # The un-awaited record is drained to the producer inside the aborting
    # transaction, then discarded by the abort -- it must not leak into history
    # later (before the fix it was handed to Rust ~10ms after the abort, landing
    # outside the transaction and leaking in).
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.begin_transaction()
        p.send(ProducerRecord("test-topic", b"discarded"))  # no .result()
        p.abort_transaction()
        assert p.history_count() == 0
        time.sleep(0.05)  # a stale tray hand-off would land in this window
        assert p.history_count() == 0
        # A fresh transaction with another un-awaited send still commits it.
        p.begin_transaction()
        p.send(ProducerRecord("test-topic", b"kept"))  # no .result()
        p.commit_transaction()
        assert p.history_count() == 1


def test_flush_completes_unawaited_send():
    # Java flush() completes every returned send. send() only appends to the
    # tray, so without the fix flush() returns before the record is even handed
    # to the producer (history_count == 0). With the fix flush() drains the tray
    # first, so the record is handed AND completed by the flush.
    with MockProducer(auto_complete=True) as p:
        fut = p.send(ProducerRecord("test-topic", b"v"))  # no .result()
        p.flush()
        # Deterministic Rust-side observable: 0 without the fix (still in tray),
        # 1 with it (handed + completed by the flush).
        assert p.history_count() == 1
        # The record's Python future is resolved by the poll-futures task once
        # the flush completes it; result() blocks (bounded) until then.
        meta = fut.result(timeout=FUTURE_TIMEOUT)
        assert isinstance(meta, RecordMetadata)
        assert fut.done()


def test_on_drained_fast_path():
    # After a record has completed (its send task hand-off is done), on_drained
    # reports already-drained (True) and never registers/fires the callback.
    with MockProducer(auto_complete=True) as p:
        p.send(ProducerRecord("test-topic", b"v")).result(timeout=FUTURE_TIMEOUT)
        called = []
        assert _lib.Producer_on_drained(
            p.c_producer, lambda: called.append(1)) is True
        time.sleep(0.02)  # a spuriously-registered cb would fire here
        assert called == []


def test_on_drained_parks_waiter_when_paused():
    # P2.3: while the send task is paused it will not take the tray, so on_drained
    # does NOT short-circuit -- it registers the callback (returns False) and the
    # waiter parks. Unpausing lets the send task drain and fire the parked
    # callback. (Before P2.3 the `|| test_paused` clause short-circuited to True,
    # which protected nothing -- no paused test waits on a drain -- and blocked
    # any deterministic parked-waiter test.)
    p = MockProducer(auto_complete=True)
    fired = threading.Event()
    try:
        _lib.Producer_test_set_paused(p.c_producer, True)
        p.send(ProducerRecord("test-topic", b"v"))  # accepted, not handed (paused)
        # Registered, not satisfied: on_drained returns False and the cb parks.
        assert _lib.Producer_on_drained(p.c_producer, fired.set) is False
        assert not fired.wait(timeout=0.1), "cb must not fire while paused"
        # Unpause: the send task drains the tray and fires the parked cb.
        _lib.Producer_test_set_paused(p.c_producer, False)
        assert fired.wait(timeout=FUTURE_TIMEOUT), "cb must fire once unpaused"
    finally:
        p.close()


# -- ConsumerGroupMetadata handle lifecycle -----------------------------------

def test_group_metadata_handle_lifecycle():
    # Each group_metadata() returns a fresh handle-owning object freed on GC in
    # its tp_dealloc; creating and dropping many must not leak or crash, and two
    # objects never alias one handle (the FFI clones internally).
    consumer = MockConsumer("earliest")
    for _ in range(5000):
        gm = consumer.group_metadata()
        assert gm.group_id == "dummy.group.id"
        assert gm.generation_id == 1
        del gm
    gc.collect()
    # A handle still works after many others were freed.
    gm = consumer.group_metadata()
    assert gm.group_id == "dummy.group.id"
    consumer.close()


# =============================================================================
# AsyncProducer transaction tests
# =============================================================================


async def test_async_txn_init_begin_send_commit():
    async with AsyncMockProducer(auto_complete=True) as p:
        await p.init_transactions()
        await p.begin_transaction()
        future = await p.send(ProducerRecord("test-topic", b"v"))
        meta = await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
        assert meta.offset() == 0
        await p.commit_transaction()


async def test_async_txn_abort_then_reuse():
    async with AsyncMockProducer(auto_complete=True) as p:
        await p.init_transactions()
        await p.begin_transaction()
        future = await p.send(ProducerRecord("test-topic", b"v"))
        await asyncio.wait_for(future, timeout=FUTURE_TIMEOUT)
        await p.abort_transaction()
        await p.begin_transaction()
        await p.commit_transaction()


async def test_async_txn_commit_failure_requires_abort():
    async with AsyncMockProducer(auto_complete=True) as p:
        await p.init_transactions()
        await p.begin_transaction()
        assert _lib.MockProducer_set_commit_transaction_error(
            p.c_producer, False, TXN_ABORTABLE_CODE, "async commit abortable")
        with pytest.raises(KafkaError) as exc_info:
            await p.commit_transaction()
        err = exc_info.value
        assert err.code == TXN_ABORTABLE_CODE
        assert err.message == "async commit abortable"
        assert err.txn_requires_abort is True
        assert err.is_fatal is False
        await p.abort_transaction()


async def test_async_txn_send_offsets_to_transaction():
    consumer = MockConsumer("earliest")
    group_metadata = consumer.group_metadata()
    group_id = group_metadata.group_id
    async with AsyncMockProducer(auto_complete=True) as p:
        await p.init_transactions()
        await p.begin_transaction()
        await p.send_offsets_to_transaction(
            {TopicPartition("t", 1): OffsetAndMetadata(9)}, group_metadata)
        assert _lib.MockProducer_sent_offsets(p.c_producer) is True
        await p.commit_transaction()
        committed = _lib.MockProducer_committed_offset(
            p.c_producer, group_id, "t", 1)
        assert committed is not None
        assert committed[0] == 9
    consumer.close()


async def test_async_txn_begin_before_init_raises():
    async with AsyncMockProducer(auto_complete=True) as p:
        with pytest.raises(KafkaError) as exc_info:
            await p.begin_transaction()
        assert exc_info.value.message == \
            "MockProducer hasn't been initialized for transactions."


async def test_async_txn_commit_without_begin_raises():
    # Also exercises KafkaError propagation back from the async completion
    # callback (_run_async -> _resolve_void raises).
    async with AsyncMockProducer(auto_complete=True) as p:
        await p.init_transactions()
        with pytest.raises(KafkaError) as exc_info:
            await p.commit_transaction()
        assert exc_info.value.message == "There is no open transaction."


# =============================================================================
# Async-first routing regression
#
# All five transaction-control ops on BOTH producers drive the *_async FFI
# variants (kafka_producer_Producer_<op>_async) and wait on the completion
# callback -- sync via _run_sync (threading.Event, GIL released so the wait
# stays interruptible on the main thread, like flush/close), async via
# _run_async (call_soon_threadsafe onto the loop, awaited/cancellable). These
# pin that routing so it cannot silently revert to the old blocking sync FFI /
# run_in_executor block_on facade. Each fake replaces the real FFI, records the
# call, and fires the success callback (error handle 0) from a BACKGROUND thread
# -- exactly as the real dispatcher thread does. That the sync test does not
# hang is itself proof the GIL is released during the wait: a native block_on
# would have held it, and the background thread could never have run the
# callback.
# =============================================================================

# The completion callback is always the LAST positional arg each wrapper hands
# the FFI (the four no-arg ops pass (producer, cb); send_offsets passes
# (producer, spec, group_metadata, cb)), so one fake covers all five.
_TXN_ASYNC_SYMBOLS = [
    "Producer_init_transactions_async",
    "Producer_begin_transaction_async",
    "Producer_send_offsets_to_transaction_async",
    "Producer_commit_transaction_async",
    "Producer_abort_transaction_async",
]


def _fire_success_from_background_thread(*args, _calls=None):
    _calls.append(1)
    cb = args[-1]  # completion callback is always the last positional arg
    # error handle 0 == success; fire from a bg thread like the dispatcher does.
    threading.Thread(target=lambda: cb(0)).start()


def test_txn_sync_ops_route_through_async_ffi_and_release_gil():
    consumer = MockConsumer("earliest")
    gm = consumer.group_metadata()
    offsets = {TopicPartition("t", 0): OffsetAndMetadata(1)}
    p = MockProducer(auto_complete=True)
    ops = [
        p.init_transactions,
        p.begin_transaction,
        lambda: p.send_offsets_to_transaction(offsets, gm),
        p.commit_transaction,
        p.abort_transaction,
    ]
    try:
        for sym, op in zip(_TXN_ASYNC_SYMBOLS, ops):
            real = getattr(_lib, sym)
            calls = []
            setattr(_lib, sym,
                    lambda *a, _c=calls: _fire_success_from_background_thread(*a, _calls=_c))
            try:
                # Returns only if _run_sync released the GIL, the bg thread ran
                # the callback, and _run_sync resolved it. Would hang (or, on the
                # old routing, never touch the *_async symbol) otherwise.
                op()
            finally:
                setattr(_lib, sym, real)
            assert calls == [1], f"{sym} was not driven exactly once"
    finally:
        p.close()
        consumer.close()


async def test_async_txn_ops_route_through_async_ffi():
    consumer = MockConsumer("earliest")
    gm = consumer.group_metadata()
    offsets = {TopicPartition("t", 0): OffsetAndMetadata(1)}
    p = AsyncMockProducer(auto_complete=True)
    ops = [
        p.init_transactions,
        p.begin_transaction,
        lambda: p.send_offsets_to_transaction(offsets, gm),
        p.commit_transaction,
        p.abort_transaction,
    ]
    try:
        for sym, op in zip(_TXN_ASYNC_SYMBOLS, ops):
            real = getattr(_lib, sym)
            calls = []
            setattr(_lib, sym,
                    lambda *a, _c=calls: _fire_success_from_background_thread(*a, _calls=_c))
            try:
                # Awaitable and completes only if _run_async hopped the callback
                # onto the loop and resolved the future.
                await op()
            finally:
                setattr(_lib, sym, real)
            assert calls == [1], f"{sym} was not driven exactly once"
    finally:
        await p.close()
        consumer.close()


async def test_async_txn_cancelled_await_frees_late_error_handle():
    # Cancellation-safety regression (Critic 56, Finding 1): when an async txn op
    # is cancelled (e.g. asyncio.wait_for(commit_transaction(), timeout=T) then
    # retry -- the pattern the docstrings invite) and its completion callback
    # fires LATE with a non-null KafkaError handle, _run_async's deliver must
    # FREE that handle, not drop it. Dropping it leaks the Box<KafkaError>,
    # unbounded under a retry/cancel loop. Before the fix, deliver was
    # `if not fut.done(): fut.set_result(payload)` with no else, so the handle of
    # a cancelled-then-failed op reached neither _resolve_void nor _free_void.
    #
    # Deterministic: fake the *_async FFI to CAPTURE the callback without firing
    # it (an op that outlives the cancel), cancel the await, then fire the late
    # callback with a non-null handle and assert KafkaError_destroy freed it.
    captured = {}
    freed = []
    real_async = _lib.Producer_commit_transaction_async
    real_destroy = _lib.KafkaError_destroy
    p = AsyncMockProducer(auto_complete=True)
    try:
        _lib.Producer_commit_transaction_async = \
            lambda _producer_ptr, cb: captured.__setitem__("cb", cb)
        _lib.KafkaError_destroy = lambda h: freed.append(h)

        task = asyncio.ensure_future(p.commit_transaction())
        # One yield runs the task to its `await fut` suspension (submit(cb) has
        # captured the callback by then).
        await asyncio.sleep(0)
        assert "cb" in captured, "op did not submit its callback"

        task.cancel()  # equivalent to a wait_for timeout cancelling the await
        with pytest.raises(asyncio.CancelledError):
            await task

        # The op completes LATE with a non-null error handle (fake KafkaError*).
        HANDLE = 0xDEAD
        captured["cb"](HANDLE)
        await asyncio.sleep(0)  # let call_soon_threadsafe(deliver, ...) run

        # deliver saw fut.cancelled() and routed the payload to free(); without
        # the fix it would have been dropped and freed == [].
        assert freed == [HANDLE], \
            f"late error handle leaked (not freed); freed={freed!r}"
    finally:
        _lib.Producer_commit_transaction_async = real_async
        _lib.KafkaError_destroy = real_destroy
        await p.close()


# =============================================================================
# AsyncProducer un-awaited send drained before control ops (async twins)
#
# Async counterparts of the sync send/commit-race tests: the record's send() is
# awaited (send is a coroutine) but the returned Future is NOT, so the record is
# still in the C tray when the control op / flush runs. _wait_drained() (awaited
# first by each) hands it to the producer before the op, matching Java.
# =============================================================================


async def test_async_txn_unawaited_send_is_committed():
    async with AsyncMockProducer(auto_complete=True) as p:
        await p.init_transactions()
        await p.begin_transaction()
        await p.send(ProducerRecord("test-topic", b"v"))  # Future not awaited
        await p.commit_transaction()
        assert p.history_count() == 1


async def test_async_txn_unawaited_send_is_discarded_by_abort():
    async with AsyncMockProducer(auto_complete=True) as p:
        await p.init_transactions()
        await p.begin_transaction()
        await p.send(ProducerRecord("test-topic", b"discarded"))  # not awaited
        await p.abort_transaction()
        assert p.history_count() == 0
        await asyncio.sleep(0.05)  # a stale tray hand-off would land here
        assert p.history_count() == 0
        await p.begin_transaction()
        await p.send(ProducerRecord("test-topic", b"kept"))  # not awaited
        await p.commit_transaction()
        assert p.history_count() == 1


async def test_async_flush_completes_unawaited_send():
    async with AsyncMockProducer(auto_complete=True) as p:
        fut = await p.send(ProducerRecord("test-topic", b"v"))  # Future not awaited
        await p.flush()
        # Deterministic: 0 without the fix (still in the tray), 1 with it.
        assert p.history_count() == 1
        meta = await asyncio.wait_for(fut, timeout=FUTURE_TIMEOUT)
        assert isinstance(meta, RecordMetadata)


# =============================================================================
# Pass-2 drain hardening: racing senders, ordering, close-release, cancellation,
# and the sync/async drain-wait timeout (P2.4, T1-T7).
#
# Every wait below is bounded (a hang fails the test, it does not hang the run);
# the close-release / cancellation / timeout tests genuinely park a waiter first
# (they assert on_drained returned False, or the op did not complete before the
# release), so they exercise the parked-waiter path rather than the fast path.
#
# T3 (an immediately-erroring record still counted as handed) is intentionally
# NOT translated: it cannot be provoked through the mock. send_batch's only
# synchronous-error paths are a null topic/key/value pointer (unreachable from the
# Python ProducerRecord constructor -- topic is required and non-null; key/value
# pointers are non-null whenever their length is >= 0) and mock.send() returning
# Err, which happens only on closed / producer_fenced / a preset send_error --
# none of which the Python MockProducer FFI exposes (fenceProducer / set-send-error
# have no symbols; error_next is an async completion and set_commit_transaction_error
# is commit-time). Faking it would test the fake, not the drain, so it is skipped.
# =============================================================================


def test_txn_racing_senders_no_hang():
    # T1: a background thread floods un-awaited sends while the main-thread cycles
    # run begin/send/commit-or-abort. The drain must never deadlock, nothing may be
    # lost or stuck, and every committed in-txn record must reach history.
    p = MockProducer(auto_complete=True)
    p.init_transactions()

    stop = threading.Event()

    def racer():
        while not stop.is_set():
            try:
                p.send(ProducerRecord("test-topic", b"race"))
            except Exception:  # noqa: BLE001 - transient state / teardown; ignore
                return

    committed = [0]
    txn_futures = []
    cycles_done = threading.Event()

    def cycles():
        try:
            for i in range(50):
                p.begin_transaction()
                txn_futures.append(p.send(ProducerRecord("test-topic", b"txn")))
                if i % 2 == 0:
                    p.commit_transaction()
                    committed[0] += 1
                else:
                    p.abort_transaction()
        finally:
            cycles_done.set()

    racer_t = threading.Thread(target=racer, daemon=True)
    cycles_t = threading.Thread(target=cycles, daemon=True)
    racer_t.start()
    cycles_t.start()
    try:
        # A drain deadlock would hang here; the bound turns it into a failure.
        assert cycles_done.wait(timeout=30), "begin/send/commit cycles hung"
    finally:
        stop.set()
        racer_t.join(timeout=FUTURE_TIMEOUT)
        cycles_t.join(timeout=FUTURE_TIMEOUT)
    assert not cycles_t.is_alive()
    assert not racer_t.is_alive()

    p.flush()  # hand over anything still in the tray
    # Nothing stuck: every in-txn record future resolved (auto_complete resolves
    # the send regardless of the transaction's later commit/abort).
    for f in txn_futures:
        f.result(timeout=FUTURE_TIMEOUT)
    # Nothing lost: at least the committed in-txn records reached history. The
    # exact total also includes racer records the mock accepted out-of-txn, which
    # is nondeterministic under the race -- hence the >= bound.
    assert p.history_count() >= committed[0]
    p.close()


def test_txn_drain_before_begin_ordering():
    # T2: init; un-awaited out-of-txn send; begin. begin's _wait_drained hands the
    # record to the mock BEFORE begin runs, and the mock puts an out-of-txn send
    # straight into history -- so history_count is already 1 when begin returns,
    # proving the record was drained over before begin executed.
    with MockProducer(auto_complete=True) as p:
        p.init_transactions()
        p.send(ProducerRecord("test-topic", b"pre"))  # no .result(), out-of-txn
        p.begin_transaction()
        assert p.history_count() == 1  # handed over before begin ran
        p.commit_transaction()
        assert p.history_count() == 1  # empty txn added nothing


def test_close_releases_parked_drain_waiter():
    # T4 (sync): a parked commit is released by close(); the P2.2 re-check then
    # raises the closed error instead of submitting into the torn-down producer.
    p = MockProducer(auto_complete=True)
    p.init_transactions()
    p.begin_transaction()
    _lib.Producer_test_set_paused(p.c_producer, True)
    p.send(ProducerRecord("test-topic", b"v"))  # accepted, not handed (paused)

    result = {}
    started = threading.Event()

    def run_commit():
        started.set()
        try:
            p.commit_transaction()
            result["ok"] = True
        except Exception as e:  # noqa: BLE001
            result["exc"] = e

    t = threading.Thread(target=run_commit, daemon=True)
    t.start()
    assert started.wait(timeout=FUTURE_TIMEOUT)
    # Genuinely parked: paused -> record undrained -> _wait_drained is waiting.
    time.sleep(0.1)
    assert not result, "commit must park while the record is undrained"

    p.close()  # releases the parked waiter; must not raise
    t.join(timeout=FUTURE_TIMEOUT)
    assert not t.is_alive()
    assert "ok" not in result, "commit must not submit into a closing producer"
    assert isinstance(result.get("exc"), RuntimeError)
    assert "closed" in str(result["exc"]).lower()


async def test_async_close_releases_parked_drain_waiter():
    # T4 (async twin): same, on the event loop with a task + await close().
    p = AsyncMockProducer(auto_complete=True)
    await p.init_transactions()
    await p.begin_transaction()
    _lib.Producer_test_set_paused(p.c_producer, True)
    await p.send(ProducerRecord("test-topic", b"v"))  # Future not awaited

    task = asyncio.ensure_future(p.commit_transaction())
    await asyncio.sleep(0.1)
    assert not task.done(), "commit must park while the record is undrained"

    await p.close()  # releases the parked waiter; must not raise
    with pytest.raises(RuntimeError, match="closed"):
        await asyncio.wait_for(task, timeout=FUTURE_TIMEOUT)


async def test_async_drain_wait_cancellation_never_submits():
    # T5: cancelling the awaiting task (via an outer wait_for timeout) while the
    # commit is parked must propagate as a timeout, never submit the commit, and --
    # after unpause fires the stale drain cb onto the cancelled fut -- raise NO
    # InvalidStateError (the fut.done() guard in _resolve_drained absorbs it).
    p = AsyncMockProducer(auto_complete=True)
    loop = asyncio.get_running_loop()
    loop_errors = []
    loop.set_exception_handler(lambda _loop, ctx: loop_errors.append(ctx))
    try:
        await p.init_transactions()
        await p.begin_transaction()
        _lib.Producer_test_set_paused(p.c_producer, True)
        await p.send(ProducerRecord("test-topic", b"v"))  # Future not awaited

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(p.commit_transaction(), timeout=0.1)

        # Unpause: the stale drain cb from the cancelled _wait_drained fires onto a
        # cancelled fut -> guarded, no error; the record is now handed over.
        _lib.Producer_test_set_paused(p.c_producer, False)
        await asyncio.sleep(0.1)  # let the late cb land on the loop
        assert loop_errors == [], f"loop exception after cancellation: {loop_errors}"
        assert p.history_count() == 0, "commit must not have been submitted"

        # The transaction is still open; a fresh commit succeeds.
        await p.commit_transaction()
        assert p.history_count() == 1
    finally:
        await p.close()


def test_sync_drain_wait_timeout(monkeypatch):
    # T6: a parked sync commit whose drain never completes (paused) times out with
    # a retriable, non-abortable KafkaError; after unpause a fresh commit succeeds
    # and the stale drain entry firing later causes no error.
    import producer as producer_module
    p = MockProducer(auto_complete=True)
    try:
        p.init_transactions()
        p.begin_transaction()
        _lib.Producer_test_set_paused(p.c_producer, True)
        p.send(ProducerRecord("test-topic", b"v"))  # accepted, not handed (paused)

        monkeypatch.setattr(producer_module, "_DRAIN_WAIT_TIMEOUT_S", 0.1)
        with pytest.raises(KafkaError) as ei:
            p.commit_transaction()
        err = ei.value
        assert err.is_retriable is True
        assert err.txn_requires_abort is False

        _lib.Producer_test_set_paused(p.c_producer, False)
        p.commit_transaction()  # record now drains; commit succeeds
        assert p.history_count() == 1
        time.sleep(0.05)  # the stale drain entry firing later must cause no error
        assert p.history_count() == 1
    finally:
        p.close()


async def test_async_drain_wait_timeout(monkeypatch):
    # T7: async twin of T6 (relies on P2.1 bounding the async wait). asyncio.wait_for
    # trips _DRAIN_WAIT_TIMEOUT_S and the op raises the same retriable KafkaError.
    import producer as producer_module
    p = AsyncMockProducer(auto_complete=True)
    try:
        await p.init_transactions()
        await p.begin_transaction()
        _lib.Producer_test_set_paused(p.c_producer, True)
        await p.send(ProducerRecord("test-topic", b"v"))  # Future not awaited

        monkeypatch.setattr(producer_module, "_DRAIN_WAIT_TIMEOUT_S", 0.1)
        with pytest.raises(KafkaError) as ei:
            await p.commit_transaction()
        err = ei.value
        assert err.is_retriable is True
        assert err.txn_requires_abort is False

        _lib.Producer_test_set_paused(p.c_producer, False)
        await p.commit_transaction()  # record now drains; commit succeeds
        assert p.history_count() == 1
        await asyncio.sleep(0.05)  # a stale drain entry firing later: no error
        assert p.history_count() == 1
    finally:
        await p.close()
