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

"""Record metadata edges against a broker, sync and asyncio:

- ``acks=0``: the broker answers nothing, so the ``RecordMetadata`` has no
  offset (``-1``, ``has_offset()`` false);
- a topic with ``message.timestamp.type=LogAppendTime``: the broker's append
  time replaces the record's timestamp, in the ``RecordMetadata`` and in the
  consumed record, whose ``timestamp_type()`` is ``LOG_APPEND_TIME``;
- a tombstone (``value=None``) is consumed with a ``None`` value and a ``-1``
  serialized value size, never as ``b""``;
- ``bytes`` and ``memoryview`` values and header values through ``gzip`` and
  ``zstd`` compression come back unchanged, header values as ``memoryview``.

Skips without Docker (``kafka_broker``).
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from confluent_kafka.common import Header, TimestampType
from confluent_kafka.producer import ProducerRecord

from .broker_helpers import (
    MODE, MODES, aw, make_producer, new_topic, producer_configs, read_all, run,
)


async def _send(mode: str, configs: dict[str, Any], record: ProducerRecord[Any, Any]) -> Any:
    """Send ``record`` and return its ``RecordMetadata``."""
    producer = make_producer(mode, configs)
    try:
        future = await aw(producer.send(record=record))
        return await aw(future.get(timeout=30))
    finally:
        await aw(producer.close())


@MODE
def test_acks_zero_metadata_has_no_offset(kafka_broker: Any, mode: str) -> None:
    topic = new_topic(kafka_broker, "py-acks0")

    async def body() -> Any:
        return await _send(mode, producer_configs(kafka_broker, acks="0"),
                           ProducerRecord(topic=topic, partition=0, key=None, value=b"v"))

    metadata = run(body)
    assert metadata.offset() == -1 and not metadata.has_offset()
    assert (metadata.topic(), metadata.partition()) == (topic, 0)
    # The record still reached the log.
    assert [r.value() for r in read_all(kafka_broker, topic, 1, 1)] == [b"v"]


@MODE
def test_log_append_time_replaces_the_record_timestamp(kafka_broker: Any, mode: str) -> None:
    topic = new_topic(kafka_broker, "py-log-append",
                      configs={"message.timestamp.type": "LogAppendTime"})
    # An hour ago: within retention, and far from the append time.
    given = int(time.time() * 1000) - 3_600_000

    async def body() -> tuple[Any, int, int]:
        before = int(time.time() * 1000)
        metadata = await _send(mode, producer_configs(kafka_broker), ProducerRecord(
            topic=topic, partition=0, timestamp=given, key=None, value=b"v"))
        return metadata, before, int(time.time() * 1000)

    metadata, before, after = run(body)
    assert metadata.has_timestamp()
    assert before - 1000 <= metadata.timestamp() <= after + 1000
    (record,) = read_all(kafka_broker, topic, 1, 1)
    assert record.timestamp_type() is TimestampType.LOG_APPEND_TIME
    assert record.timestamp() == metadata.timestamp()


@MODE
def test_tombstone_round_trip(kafka_broker: Any, mode: str) -> None:
    topic = new_topic(kafka_broker, "py-tombstone")

    async def body() -> Any:
        return await _send(mode, producer_configs(kafka_broker),
                           ProducerRecord(topic=topic, partition=0, key=b"k", value=None))

    metadata = run(body)
    assert metadata.serialized_value_size() == -1 and metadata.serialized_key_size() == 1
    (record,) = read_all(kafka_broker, topic, 1, 1)
    assert record.key() == b"k"
    assert record.value() is None and record.serialized_value_size() == -1


#: Header values as given, and as they come back.
HEADERS_IN: list[tuple[str, bytes | memoryview | None]] = [
    ("h-bytes", b"hb"), ("h-view", memoryview(b"xhvx")[1:3]), ("h-null", None)]
HEADERS_OUT: list[tuple[str, bytes | None]] = [
    ("h-bytes", b"hb"), ("h-view", b"hv"), ("h-null", None)]


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("compression", ["gzip", "zstd"])
def test_compressed_bytes_and_memoryview_round_trip(kafka_broker: Any, mode: str,
                                                    compression: str) -> None:
    topic = new_topic(kafka_broker, f"py-{compression}")
    big = bytes(range(256)) * 64  # compressible, several KiB
    values: list[bytes | memoryview] = [b"plain", memoryview(big)[3:-3], memoryview(b"view")]

    async def body() -> None:
        producer = make_producer(mode, producer_configs(
            kafka_broker, compression_type=compression, linger_ms=50))
        try:
            futures = [await aw(producer.send(record=ProducerRecord(
                topic=topic, partition=0, key=memoryview(b"key"), value=value,
                headers=HEADERS_IN))) for value in values]
            await aw(producer.flush())
            assert [(await aw(f.get(timeout=30))).offset() for f in futures] == [0, 1, 2]
        finally:
            await aw(producer.close())

    run(body)
    records = read_all(kafka_broker, topic, 1, len(values))
    assert [r.value() for r in records] == [bytes(v) for v in values]
    for record in records:
        assert record.key() == b"key"
        headers = [(h.key(), h.value()) for h in record.headers()]
        assert all(isinstance(h, Header) for h in record.headers())
        assert all(v is None or isinstance(v, memoryview) for _, v in headers)
        # Each value views the fetch batch (CLAUDE.md §14), never a copy.
        assert all(v is None or type(v.obj).__name__ == "_BorrowedBytes" for _, v in headers)
        assert [(k, None if v is None else bytes(v)) for k, v in headers] == HEADERS_OUT
        # A polled record's headers stay writable, as Java's CompletedFetch
        # leaves them.
        record.headers().add(key="k", value=None)
        assert record.headers().last_header(key="k") is not None
