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

import time
import pytest
from producer import (
    MockProducer, ProducerRecord, RecordMetadata, KafkaError
)

# Timeout in seconds for future.result() calls
FUTURE_TIMEOUT = 2

# Time for the batch thread to dispatch records (batch interval is 10ms)
BATCH_DISPATCH = 0.02


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

def test_manual_complete_next():
    p = MockProducer(auto_complete=False)
    future = p.send(ProducerRecord("test-topic", b"v"))
    time.sleep(BATCH_DISPATCH)
    assert not future.done()
    p.complete_next()
    meta = future.result(timeout=FUTURE_TIMEOUT)
    assert future.done()
    assert isinstance(meta, RecordMetadata)
    assert meta.offset() == 0
    p.close()


def test_manual_error_next():
    p = MockProducer(auto_complete=False)
    future = p.send(ProducerRecord("test-topic", b"v"))
    time.sleep(BATCH_DISPATCH)
    p.error_next(2, "test error")
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
    time.sleep(BATCH_DISPATCH)
    p.error_next(2, None)
    with pytest.raises(KafkaError) as exc_info:
        future.result(timeout=FUTURE_TIMEOUT)
    assert exc_info.value.code == 2
    p.close()


# -- Flush and close ----------------------------------------------------------

def test_flush():
    with MockProducer(auto_complete=True) as p:
        p.send(ProducerRecord("test-topic", b"v"))
        p.flush()  # Should not raise


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
    time.sleep(BATCH_DISPATCH)
    p.error_next(2, "corrupt message")
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
