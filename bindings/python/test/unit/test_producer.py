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
import os
import threading
import time
import pytest
import _confluentkafka as _lib
from producer import (
    KafkaProducer, MockProducer, ProducerRecord, RecordMetadata, KafkaError,
    AsyncKafkaProducer, AsyncMockProducer
)

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
