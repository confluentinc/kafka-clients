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

"""Tests of ``ProducerRecord`` and ``RecordMetadata``.

Java's tests translated: ``ProducerRecordTest`` and ``RecordMetadataTest``
(all), with the exact messages Java raises.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from confluent_kafka import ExecutionError, IllegalArgumentError, TimeoutError
from confluent_kafka.common import Headers, TopicPartition
from confluent_kafka.common.errors import CorruptRecordError
from confluent_kafka.producer import AsyncMockProducer, MockProducer, ProducerRecord, RecordMetadata

# --------------------------------------------------------------------------- #
# ProducerRecord: ProducerRecordTest
# --------------------------------------------------------------------------- #


def test_equals_and_hash_code() -> None:
    producer_record = ProducerRecord(topic="test", partition=1, key="key", value=1)
    assert producer_record == producer_record
    assert hash(producer_record) == hash(producer_record)
    equal_record = ProducerRecord(topic="test", partition=1, key="key", value=1)
    assert producer_record == equal_record
    assert hash(producer_record) == hash(equal_record)
    assert producer_record != ProducerRecord(topic="test-1", partition=1, key="key", value=1)
    assert producer_record != ProducerRecord(topic="test", partition=2, key="key", value=1)
    assert producer_record != ProducerRecord(topic="test", partition=1, key="key-1", value=1)
    assert producer_record != ProducerRecord(topic="test", partition=1, key="key", value=2)
    null_fields = ProducerRecord(topic="topic", partition=None, timestamp=None, key=None,
                                 value=None, headers=None)  # type: ignore[call-overload]
    assert null_fields == null_fields
    assert hash(null_fields) == hash(null_fields)


def test_invalid_records() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        ProducerRecord(topic=None, partition=0, key="key", value=1)  # type: ignore[call-overload]
    assert str(exc.value) == "Topic cannot be None."
    with pytest.raises(IllegalArgumentError) as exc:
        ProducerRecord(topic="test", partition=0, timestamp=-1, key="key", value=1)
    assert str(exc.value) == (
        "Invalid timestamp: -1. Timestamp should always be non-negative or None.")
    with pytest.raises(IllegalArgumentError) as exc:
        ProducerRecord(topic="test", partition=-1, key="key", value=1)
    assert str(exc.value) == (
        "Invalid partition: -1. Partition number should always be non-negative or None.")


def test_producer_record_forms_and_headers() -> None:
    # Each Java constructor delegates to the longest; (topic, value) is one.
    r = ProducerRecord(topic="t", value=b"v")
    assert (r.topic(), r.partition(), r.timestamp(), r.key(), r.value()) == (
        "t", None, None, None, b"v")
    assert r.headers() == Headers() and r.headers().to_array() == ()
    raw = bytearray(b"h")
    tombstone = ProducerRecord(topic="t", partition=None, key=b"k", value=None,
                               headers=[("a", raw), ("b", None)])
    assert tombstone.value() is None
    ha, hb = tombstone.headers()
    a, va, b, vb = ha.key(), ha.value(), hb.key(), hb.value()
    assert (a, b, vb) == ("a", "b", None)
    assert isinstance(va, memoryview) and va.tobytes() == b"h"
    raw[0] = ord("H")  # the header value is viewed, not copied
    assert va.tobytes() == b"H"
    assert str(ProducerRecord(topic="t", partition=1, key="k", value="v")) == (
        "ProducerRecord(topic=t, partition=1, headers=RecordHeaders(headers = [], "
        "isReadOnly = False), key=k, value=v, timestamp=None)")
    # partition and key default to MISSING, so an explicit None is given: Java's
    # (topic, partition, key, value) with a null key, or a null partition.
    null_key = ProducerRecord(topic="t", partition=0, key=None, value=b"v")
    assert (null_key.partition(), null_key.key(), null_key.value()) == (0, None, b"v")
    assert ProducerRecord(topic="t", partition=None, key=b"k", value=b"v").partition() is None
    # timestamp and headers default to MISSING as well (constructors 3-6 pass a
    # null to a longer one): a null timestamp selects (topic, partition,
    # timestamp, key, value), an empty header list (topic, partition, key,
    # value, headers).
    null_timestamp = ProducerRecord(topic="t", partition=0, timestamp=None, key=b"k", value=b"v")
    assert (null_timestamp.timestamp(), null_timestamp.headers()) == (None, Headers())
    empty_headers = ProducerRecord(topic="t", partition=None, key=None, value=b"v", headers=())
    assert (empty_headers.timestamp(), empty_headers.headers()) == (None, Headers())
    # Only Java's six constructors are accepted (CLAUDE.md, Signatures).
    forms = ("ProducerRecord() takes one of (topic, partition, timestamp, key, value, headers), "
             "(topic, partition, timestamp, key, value), (topic, partition, key, value, headers), "
             "(topic, partition, key, value), (topic, key, value), (topic, value); got ")
    for kwargs, got in [({"partition": 0}, "(topic, partition, value)"),
                        ({"timestamp": 5}, "(topic, timestamp, value)"),
                        ({"timestamp": None}, "(topic, timestamp, value)"),
                        ({"headers": [("h", b"1")]}, "(topic, value, headers)"),
                        ({"headers": ()}, "(topic, value, headers)"),
                        ({"key": b"k", "headers": [("h", b"1")]},
                         "(topic, key, value, headers)"),
                        ({"key": None, "headers": ()}, "(topic, key, value, headers)")]:
        with pytest.raises(IllegalArgumentError) as exc:
            ProducerRecord(topic="t", value=b"v", **kwargs)
        assert str(exc.value) == forms + got
    with pytest.raises(TypeError) as npe:
        ProducerRecord(topic="t", partition=None, key=None, value=1,
                       headers=[(None, b"x")])  # type: ignore[list-item]
    assert str(npe.value) == "headers must not contain a None key"
    with pytest.raises(TypeError):
        ProducerRecord("t", None, None, None, 1)  # type: ignore[call-overload]


# --------------------------------------------------------------------------- #
# RecordMetadata: RecordMetadataTest
# --------------------------------------------------------------------------- #


def test_construction_with_missing_batch_index() -> None:
    tp = TopicPartition(topic="foo", partition=0)
    metadata = RecordMetadata(topic_partition=tp, base_offset=-1, batch_index=-1,
                              timestamp=2340234, serialized_key_size=3, serialized_value_size=5)
    assert metadata.topic() == tp.topic()
    assert metadata.partition() == tp.partition()
    assert metadata.timestamp() == 2340234
    assert not metadata.has_offset()
    assert metadata.offset() == -1
    assert metadata.serialized_key_size() == 3
    assert metadata.serialized_value_size() == 5


def test_construction_with_batch_index_offset() -> None:
    tp = TopicPartition(topic="foo", partition=0)
    metadata = RecordMetadata(topic_partition=tp, base_offset=15, batch_index=3,
                              timestamp=2340234, serialized_key_size=3, serialized_value_size=5)
    assert metadata.topic() == tp.topic()
    assert metadata.partition() == tp.partition()
    assert metadata.timestamp() == 2340234
    assert metadata.offset() == 18
    assert metadata.serialized_key_size() == 3
    assert metadata.serialized_value_size() == 5


def test_record_metadata_surface() -> None:
    tp = TopicPartition(topic="foo", partition=2)
    metadata = RecordMetadata(topic_partition=tp, base_offset=7, batch_index=0, timestamp=-1,
                              serialized_key_size=-1, serialized_value_size=-1)
    assert RecordMetadata.UNKNOWN_PARTITION == -1
    assert metadata.has_offset() and not metadata.has_timestamp()
    assert str(metadata) == "foo-2@7"
    with pytest.raises(TypeError):
        RecordMetadata(tp, 0, 0, 0, 0, 0)  # type: ignore[call-arg]


# --------------------------------------------------------------------------- #
# Header values: the record's own views, and the native record's buffer
# exports (the send path borrows the bytes, never reads a released buffer)
# --------------------------------------------------------------------------- #


def test_header_value_survives_the_caller_releasing_its_view() -> None:
    with memoryview(bytearray(b"hdr")) as mv:
        record = ProducerRecord(topic="t", partition=None, key=None, value=b"v",
                                headers=[("h", mv)])
    (header,) = record.headers()
    value = header.value()
    assert value is not None and value.tobytes() == b"hdr"
    # A caller releasing a handed-out view leaves the record's own intact.
    with record.headers().to_array()[0].value() as handed_out:  # type: ignore[union-attr]
        assert handed_out.tobytes() == b"hdr"
    assert record.headers().to_array()[0].value().tobytes() == b"hdr"  # type: ignore[union-attr]


def test_released_or_non_contiguous_header_value_is_rejected() -> None:
    released = memoryview(b"x")
    released.release()
    with pytest.raises(ValueError) as exc:
        ProducerRecord(topic="t", partition=None, key=None, value=b"v",
                       headers=[("h", released)])
    assert str(exc.value) == "operation forbidden on released memoryview object"
    with pytest.raises(TypeError) as type_exc:
        ProducerRecord(topic="t", partition=None, key=None, value=b"v",
                       headers=[("h", memoryview(b"abcdef")[::2])])
    assert str(type_exc.value) == "headers[0] value must be a C-contiguous buffer"
    # A non-byte format is carried as its bytes.
    import array

    record = ProducerRecord(topic="t", partition=None, key=None, value=b"v",
                            headers=[("h", memoryview(array.array("H", [1])))])
    assert record.headers().to_array()[0].value().tobytes() == array.array("H", [1]).tobytes()  # type: ignore[union-attr]


def test_native_record_holds_an_export_of_each_header_value() -> None:
    import _confluentkafka as lib  # type: ignore[import-not-found]

    released = memoryview(bytes(bytearray(b"Z" * 4096)))
    released.release()
    with pytest.raises(ValueError) as exc:
        lib.ProducerRecord("t", b"v", None, -1, -1, (("k", released),))
    assert str(exc.value) == "operation forbidden on released memoryview object"

    buffer = bytearray(b"hdr")
    view = memoryview(buffer)
    native = lib.ProducerRecord("t", b"v", None, -1, -1,
                                (("k", view), ("n", None), ("b", b"xy")))
    assert native.headers == [("k", b"hdr"), ("n", None), ("b", b"xy")]
    # The export pins the bytes: the view cannot be released, nor the
    # bytearray resized, while the native record lives.
    with pytest.raises(BufferError):
        view.release()
    with pytest.raises(BufferError):
        buffer.extend(b"!")
    del native
    view.release()

    with pytest.raises(BufferError) as buffer_exc:
        lib.ProducerRecord("t", b"v", None, -1, -1, (("k", memoryview(b"abcdef")[::2]),))
    assert str(buffer_exc.value) == "memoryview: underlying buffer is not C-contiguous"
    with pytest.raises(TypeError) as type_exc:
        lib.ProducerRecord("t", b"v", None, -1, -1, (("k", 5),))
    assert str(type_exc.value) == "header value must be bytes, a contiguous memoryview or None"


def test_send_path_record_from_a_released_caller_view() -> None:
    # The native record the send path builds from a record whose caller has
    # released its header view carries the header bytes, not freed memory.
    import _confluentkafka as lib  # type: ignore[import-not-found]

    with memoryview(bytearray(b"A" * 4096)) as mv:
        record = ProducerRecord(topic="t", partition=None, key=None, value=b"v",
                                headers=[("h", mv)])
    junk = [bytes(bytearray(b"Q" * 4096)) for _ in range(50)]
    native = lib.ProducerRecord("t", b"v", None, -1, -1, record.headers()._pairs())  # noqa: SLF001
    assert native.headers == [("h", b"A" * 4096)]
    del junk


def test_record_metadata_from_ffi_is_what_the_constructor_builds() -> None:
    # The core reports the combined offset (baseOffset + batchIndex); _from_ffi
    # fills the slots directly, with what the constructor builds for that
    # offset as base_offset and batch_index 0.
    for topic, partition, offset, timestamp, key_size, value_size in (
            ("t", 0, 5, 123, -1, 100), ("orders", 7, -1, -1, 3, 0), ("a", 2147483647, 0, 0, 0, 0)):
        made = RecordMetadata._from_ffi(  # noqa: SLF001
            topic=topic, partition=partition, offset=offset, timestamp=timestamp,
            serialized_key_size=key_size, serialized_value_size=value_size)
        built = RecordMetadata(
            topic_partition=TopicPartition(topic=topic, partition=partition), base_offset=offset,
            batch_index=0, timestamp=timestamp, serialized_key_size=key_size,
            serialized_value_size=value_size)
        assert type(made) is RecordMetadata
        for getter in ("topic", "partition", "offset", "has_offset", "timestamp",
                       "has_timestamp", "serialized_key_size", "serialized_value_size"):
            assert getattr(made, getter)() == getattr(built, getter)(), getter
        assert str(made) == str(built)


# --------------------------------------------------------------------------- #
# RecordSendTest: the future a send returns. Java drives a ProduceRequestResult
# and a FutureRecordMetadata directly (internals the binding does not have);
# here the MockProducer stands in, with auto_complete=False so a send's future
# completes only when complete_next() / error_next() completes the request, as
# ProduceRequestResult.done() does. The offset is the mock's (0 for the first
# record of the partition) where Java checks baseOffset + relOffset.
# --------------------------------------------------------------------------- #

_SEND_TP = TopicPartition(topic="test", partition=0)


def _record() -> ProducerRecord[bytes, bytes]:
    return ProducerRecord(topic=_SEND_TP.topic(), partition=_SEND_TP.partition(), key=None,
                          value=b"v")


def _complete_later(producer: MockProducer[bytes, bytes], error: Exception | None,
                    delay: float) -> threading.Thread:
    """Java's ``asyncRequest``: a thread completes the request after ``delay``."""
    def run() -> None:
        import time
        time.sleep(delay)
        if error is None:
            producer.complete_next()
        else:
            producer.error_next(e=error)
    thread = threading.Thread(target=run)
    thread.start()
    return thread


def test_record_send_timeout() -> None:
    """``RecordSendTest.testTimeout``: waiting on a request that never completes
    times out; once completed, ``get()`` answers with the offset."""
    producer: MockProducer[bytes, bytes] = MockProducer(auto_complete=False, partitioner=None)
    future = producer.send(record=_record())
    assert not future.is_done(), "Request is not completed"
    with pytest.raises(TimeoutError):
        future.get(timeout=0.005)
    assert producer.complete_next()
    assert future.is_done()
    assert future.get().offset() == 0
    producer.close()


def test_record_send_error() -> None:
    """``RecordSendTest.testError``: an asynchronous request eventually throws the
    right exception, boxed in ``ExecutionError``."""
    producer: MockProducer[bytes, bytes] = MockProducer(auto_complete=False, partitioner=None)
    future = producer.send(record=_record())
    error = CorruptRecordError()
    thread = _complete_later(producer, error, 0.05)
    with pytest.raises(ExecutionError) as info:
        future.get()
    assert info.value.__cause__ is error
    thread.join()
    producer.close()


def test_record_send_blocking() -> None:
    """``RecordSendTest.testBlocking``: an asynchronous request eventually returns
    the right offset."""
    producer: MockProducer[bytes, bytes] = MockProducer(auto_complete=False, partitioner=None)
    future = producer.send(record=_record())
    thread = _complete_later(producer, None, 0.05)
    assert future.get().offset() == 0
    thread.join()
    producer.close()


async def test_async_record_send_timeout() -> None:
    """``testTimeout`` on the asyncio mock: the awaited ``get()`` times out, then
    answers once the request completes."""
    producer: AsyncMockProducer[bytes, bytes] = AsyncMockProducer(auto_complete=False,
                                                                  partitioner=None)
    future = await producer.send(record=_record())
    assert not future.is_done(), "Request is not completed"
    with pytest.raises(TimeoutError):
        await future.get(timeout=0.005)
    assert producer.complete_next()
    assert future.is_done()
    assert (await future.get()).offset() == 0
    await producer.close()


async def test_async_record_send_error() -> None:
    """``testError`` on the asyncio mock: the request is completed with an error
    after 50 ms, on the loop (Java's thread)."""
    producer: AsyncMockProducer[bytes, bytes] = AsyncMockProducer(auto_complete=False,
                                                                  partitioner=None)
    future = await producer.send(record=_record())
    error = CorruptRecordError()
    asyncio.get_running_loop().call_later(0.05, lambda: producer.error_next(e=error))
    with pytest.raises(ExecutionError) as info:
        await future.get()
    assert info.value.__cause__ is error
    await producer.close()


async def test_async_record_send_blocking() -> None:
    """``testBlocking`` on the asyncio mock: the awaited ``get()`` returns the offset
    once the request completes, 50 ms later."""
    producer: AsyncMockProducer[bytes, bytes] = AsyncMockProducer(auto_complete=False,
                                                                  partitioner=None)
    future = await producer.send(record=_record())
    asyncio.get_running_loop().call_later(0.05, producer.complete_next)
    assert (await future.get()).offset() == 0
    await producer.close()
