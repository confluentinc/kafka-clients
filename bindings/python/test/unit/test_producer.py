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
    p.complete_next()
    # Give the loop a chance to run the (no-op) scheduled completion.
    await asyncio.sleep(BATCH_DISPATCH)
    assert future.cancelled()
    await p.close()


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
# Translated from Java MockProducerTest transaction tests. §13: every
# transactional test produces with the SYNCHRONOUS send() only; the
# async/outbox send path is unsupported inside a transaction.
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
        # §13: inside a transaction, produce with the synchronous send().
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
    # Also exercises KafkaError propagation back through run_in_executor.
    async with AsyncMockProducer(auto_complete=True) as p:
        await p.init_transactions()
        with pytest.raises(KafkaError) as exc_info:
            await p.commit_transaction()
        assert exc_info.value.message == "There is no open transaction."
