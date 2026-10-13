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

"""The producer against a broker, where Java's ``KafkaProducerTest`` scripts a
``MockClient`` the binding cannot inject: the errors ``KafkaProducer.doSend``
rethrows out of ``send()`` (``KafkaProducer.java:1069-1081``) rather than
giving to the callback and the future, which need the topic's metadata first;
the record size check, which runs after the metadata wait too
(``KafkaProducer.java:1022-1024``); an open transaction, for
``begin_transaction()`` not waiting; and a record the producer takes on the
calling thread, which needs the topic's metadata cached. Then the send path
around the producer's answers against a real broker: the metadata wait and the
buffer-memory wait of ``send()``, several callers waiting at once, a record
queued when Ctrl+C ended its ``send()``, sends from a delivery callback, the
completion thread, the event loop while sends wait, transaction control and
the context manager beside a blocked ``send()``, and the lifetime of the pin
``Producer_send`` holds (the unit tests in test/unit/test_producer_send_path.py
drive the same paths without a broker). Skips without Docker
(``kafka_broker``)."""

from __future__ import annotations

import asyncio
import gc
import os
import signal
import threading
import time
import uuid
import weakref
from collections.abc import Callable
from concurrent.futures import Future
from types import FrameType
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.common import KafkaError, TopicPartition
from confluent_kafka.common.errors import RecordTooLargeError, TransactionAbortedError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.serialization import string_serializer
from confluent_kafka.consumer import KafkaConsumer
from confluent_kafka.producer import (
    AsyncKafkaProducer, KafkaProducer, ProducerRecord, RecordMetadata,
)

from .conftest import KAFKA_BIN, create_topic


def _topic(broker: Any, partitions: int = 1) -> str:
    topic = f"py-producer-{uuid.uuid4().hex[:12]}"
    create_topic(broker, topic, partitions)
    return topic


def _add_partitions(broker: Any, topic: str, partitions: int) -> None:
    """Grow ``topic`` to ``partitions`` partitions via kafka-topics.sh inside
    the container."""
    rc, out = broker.exec([
        f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", broker.internal_bootstrap,
        "--alter", "--topic", topic, "--partitions", str(partitions),
    ])
    if rc not in (0, None):
        raise RuntimeError(f"adding partitions failed (rc={rc}): {out!r}")


def _configs(broker: Any) -> dict[str, Any]:
    return {"bootstrap.servers": broker.external_bootstrap,
            "transactional.id": f"py-txn-{uuid.uuid4().hex[:12]}"}


def test_send_before_init_transactions_raises(kafka_broker: Any) -> None:
    # TransactionManager.maybeAddPartition's IllegalStateException
    # (TransactionManager.java:462), rethrown by doSend's catch (Exception e)
    # (KafkaProducer.java:1070-1073): send() raises it, and the callback does
    # not run then. doSend appends the record before maybeAddPartition
    # (KafkaProducer.java:1029-1038), so the record stays in its batch, which
    # the sender does not drain until the partition is in a transaction
    # (RecordAccumulator.java:815-818). The forced close aborts the batch with
    # "Producer is closed forcefully." (Sender.java:287-295,
    # RecordAccumulator.java:1146), and the callback runs once, then.
    topic = _topic(kafka_broker)
    calls: list[Exception | None] = []
    producer = KafkaProducer(configs=_configs(kafka_broker), key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    try:
        try:
            producer.send(record=ProducerRecord(topic=topic, key="key", value="value"),
                          callback=lambda metadata, exception: calls.append(exception))
        except IllegalStateError as error:
            assert str(error) == (f"Cannot add partition {topic}-0 to transaction before "
                                  "completing a call to initTransactions")
        else:
            raise AssertionError("send() did not raise")
        assert calls == []
    finally:
        producer.close(timeout=0)
    assert len(calls) == 1
    assert type(calls[0]) is KafkaError
    assert str(calls[0]) == "Producer is closed forcefully."


def test_send_outside_a_transaction_raises(kafka_broker: Any) -> None:
    # After initTransactions and before beginTransaction the transaction
    # manager is READY (TransactionManager.java:465, with Java's double space).
    # As doSend appends before maybeAddPartition (KafkaProducer.java:1029-1038),
    # the refused record stays in its batch. The next send to the partition,
    # inside a transaction, adds the partition to it, and the batch is
    # committed with both records: the second record's offset is 1 and both
    # callbacks run, each once.
    topic = _topic(kafka_broker)
    calls: list[Exception | None] = []
    producer = KafkaProducer(configs=_configs(kafka_broker), key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    try:
        producer.init_transactions()
        try:
            producer.send(record=ProducerRecord(topic=topic, key="key", value="value"),
                          callback=lambda metadata, exception: calls.append(exception))
        except IllegalStateError as error:
            assert str(error) == (f"Cannot add partition {topic}-0 to transaction while in "
                                  "state  READY")
        else:
            raise AssertionError("send() did not raise")
        assert calls == []

        # Inside a transaction the same send goes through; the refused record
        # is in the same batch, ahead of it.
        producer.begin_transaction()
        future = producer.send(record=ProducerRecord(topic=topic, key="key", value="value"),
                               callback=lambda metadata, exception: calls.append(exception))
        producer.commit_transaction()
        assert future.result(timeout=30).offset() == 1
        assert calls == [None, None]
    finally:
        producer.close(timeout=0)


def test_async_send_outside_a_transaction_raises(kafka_broker: Any) -> None:
    # The async peer of the test above: the same batch, offsets and callbacks.
    topic = _topic(kafka_broker)

    async def main() -> None:
        producer = AsyncKafkaProducer(configs=_configs(kafka_broker),
                                      key_serializer=string_serializer(),
                                      value_serializer=string_serializer())
        calls: list[Exception | None] = []
        try:
            await producer.init_transactions()
            try:
                await producer.send(
                    record=ProducerRecord(topic=topic, key="key", value="value"),
                    callback=lambda metadata, exception: calls.append(exception))
            except IllegalStateError as error:
                assert str(error) == (f"Cannot add partition {topic}-0 to transaction while "
                                      "in state  READY")
            else:
                raise AssertionError("send() did not raise")
            assert calls == []

            await producer.begin_transaction()
            future = await producer.send(
                record=ProducerRecord(topic=topic, key="key", value="value"),
                callback=lambda metadata, exception: calls.append(exception))
            await producer.commit_transaction()
            assert (await asyncio.wait_for(future, 30)).offset() == 1
            assert calls == [None, None]
        finally:
            await producer.close(timeout=0)

    asyncio.run(main())


# A record of a null key and VALUE_SIZE bytes of value, and no headers.
VALUE_SIZE = 1000


def _size_upper_bound(value_size: int) -> int:
    """Java's ``AbstractRecords.estimateSizeInBytesUpperBound`` of a record with
    a null key, ``value_size`` bytes of value and no headers, as ``doSend``
    computes it (``DefaultRecordBatch.estimateBatchSizeUpperBound``):
    ``RECORD_BATCH_OVERHEAD`` (61) and ``DefaultRecord.MAX_RECORD_OVERHEAD``
    (21), then ``DefaultRecord.sizeOf``: the null key's varint (1), the value's
    zigzag varint length and its bytes, and the header count's varint (1)."""
    varint, rest = 1, value_size << 1
    while rest >= 0x80:
        varint, rest = varint + 1, rest >> 7
    return 61 + 21 + 1 + varint + value_size + 1


# Java's KafkaProducer.ensureValidRecordSize (KafkaProducer.java:1162-1171):
# each limit, the producer configs that make the record exceed it, and its
# message.
RECORD_TOO_LARGE = [
    pytest.param({"max.request.size": VALUE_SIZE},
                 f"The message is {_size_upper_bound(VALUE_SIZE)} bytes when serialized which "
                 f"is larger than {VALUE_SIZE}, which is the value of the max.request.size "
                 "configuration.", id="max.request.size"),
    pytest.param({"buffer.memory": VALUE_SIZE},
                 f"The message is {_size_upper_bound(VALUE_SIZE)} bytes when serialized which "
                 "is larger than the total memory buffer you have configured with the "
                 "buffer.memory configuration.", id="buffer.memory"),
]


def _assert_record_too_large(topic: str, calls: list[tuple[RecordMetadata, Exception | None]],
                             error: BaseException, message: str) -> None:
    # doSend's catch (ApiException e) (KafkaProducer.java:1056-1068) gives the
    # same exception to the callback, with metadata of no offset for the
    # record's topic-partition (-1, the record naming none), and to the failed
    # future.
    assert type(error) is RecordTooLargeError
    assert str(error) == message
    ((metadata, exception),) = calls
    assert exception is error
    assert (metadata.topic(), metadata.partition()) == (topic, -1)
    assert not metadata.has_offset()
    assert not metadata.has_timestamp()
    assert (metadata.serialized_key_size(), metadata.serialized_value_size()) == (-1, -1)


@pytest.mark.parametrize(("limit", "message"), RECORD_TOO_LARGE)
def test_an_oversized_record_fails_with_record_too_large(
        kafka_broker: Any, limit: dict[str, Any], message: str) -> None:
    topic = _topic(kafka_broker)
    calls: list[tuple[RecordMetadata, Exception | None]] = []
    producer = KafkaProducer(configs={"bootstrap.servers": kafka_broker.external_bootstrap,
                                      **limit})
    try:
        future = producer.send(record=ProducerRecord(topic=topic, value=bytes(VALUE_SIZE)),
                               callback=lambda metadata, exception: calls.append(
                                   (metadata, exception)))
        error = future.exception(timeout=30)
    finally:
        producer.close()
    assert error is not None
    _assert_record_too_large(topic, calls, error, message)


@pytest.mark.parametrize(("limit", "message"), RECORD_TOO_LARGE)
def test_an_oversized_async_record_fails_with_record_too_large(
        kafka_broker: Any, limit: dict[str, Any], message: str) -> None:
    topic = _topic(kafka_broker)

    async def main() -> BaseException | None:
        calls.clear()
        producer = AsyncKafkaProducer(
            configs={"bootstrap.servers": kafka_broker.external_bootstrap, **limit})
        try:
            future = await producer.send(
                record=ProducerRecord(topic=topic, value=bytes(VALUE_SIZE)),
                callback=lambda metadata, exception: calls.append((metadata, exception)))
            await asyncio.wait((future,), timeout=30)
            return future.exception()
        finally:
            await producer.close()

    calls: list[tuple[RecordMetadata, Exception | None]] = []
    error = asyncio.run(main())
    assert error is not None
    _assert_record_too_large(topic, calls, error, message)


def _invalid_begin(transactional_id: str) -> str:
    return (f"TransactionalId {transactional_id}: Invalid transition attempted from state "
            "IN_TRANSACTION to state IN_TRANSACTION")


def test_begin_transaction_does_not_wait_and_an_open_transaction_keeps_its_records(
        kafka_broker: Any) -> None:
    # Critic 75 N1: Java's beginTransaction does not wait. A record sent in the
    # open transaction stays in that transaction: a second begin_transaction()
    # fails at once with the transaction manager's invalid transition, and the
    # commit_transaction() after it commits the record
    # (producer-transactions.md §13).
    topic = _topic(kafka_broker)
    configs = _configs(kafka_broker)
    producer = KafkaProducer(configs=configs, key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    try:
        producer.init_transactions()
        producer.begin_transaction()
        future = producer.send(record=ProducerRecord(topic=topic, key="key", value="value"))
        start = time.monotonic()
        with pytest.raises(IllegalStateError) as err:
            producer.begin_transaction()
        assert time.monotonic() - start < 1
        assert str(err.value) == _invalid_begin(configs["transactional.id"])
        producer.commit_transaction()
        assert future.result(timeout=30).offset() >= 0
    finally:
        producer.close(timeout=0)


def test_async_begin_transaction_does_not_wait_and_an_open_transaction_keeps_its_records(
        kafka_broker: Any) -> None:
    topic = _topic(kafka_broker)
    configs = _configs(kafka_broker)

    async def main() -> None:
        producer = AsyncKafkaProducer(configs=configs, key_serializer=string_serializer(),
                                      value_serializer=string_serializer())
        try:
            await producer.init_transactions()
            await producer.begin_transaction()
            future = await producer.send(
                record=ProducerRecord(topic=topic, key="key", value="value"))
            start = time.monotonic()
            with pytest.raises(IllegalStateError) as err:
                await producer.begin_transaction()
            assert time.monotonic() - start < 1
            assert str(err.value) == _invalid_begin(configs["transactional.id"])
            await producer.commit_transaction()
            assert (await asyncio.wait_for(future, 30)).offset() >= 0
        finally:
            await producer.close(timeout=0)

    asyncio.run(main())


def _answers(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """What ``_lib.Producer_send`` returned for each record: True for one the
    producer took on the calling thread, None for one it queued and answered
    for later, an error handle for one it refused."""
    answers: list[Any] = []
    real = _lib.Producer_send

    def spy(*args: Any) -> Any:
        answer = real(*args)
        answers.append(answer)
        return answer

    monkeypatch.setattr(_lib, "Producer_send", spy)
    return answers


def test_a_send_with_cached_metadata_is_taken_on_the_calling_thread(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # The first record of a topic needs its metadata, so the producer cannot
    # take it at once: it is queued and send() waits for the producer's
    # answer, as Java's send() blocks in waitOnMetadata. With the metadata
    # cached, the next record is appended on the calling thread, as Java's
    # is, and send() returns without waiting.
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    producer = KafkaProducer(configs={"bootstrap.servers": kafka_broker.external_bootstrap},
                             key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    try:
        first = producer.send(record=ProducerRecord(topic=topic, partition=0, key="k", value="a"))
        second = producer.send(record=ProducerRecord(topic=topic, partition=0, key="k", value="b"))
        assert answers == [None, True]
        assert first.result(timeout=30).offset() == 0
        assert second.result(timeout=30).offset() == 1
    finally:
        producer.close()


def test_an_async_send_with_cached_metadata_is_taken_on_the_calling_thread(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)

    async def main() -> None:
        producer = AsyncKafkaProducer(
            configs={"bootstrap.servers": kafka_broker.external_bootstrap},
            key_serializer=string_serializer(), value_serializer=string_serializer())
        try:
            first = await producer.send(
                record=ProducerRecord(topic=topic, partition=0, key="k", value="a"))
            second = await producer.send(
                record=ProducerRecord(topic=topic, partition=0, key="k", value="b"))
            assert answers == [None, True]
            assert (await asyncio.wait_for(first, 30)).offset() == 0
            assert (await asyncio.wait_for(second, 30)).offset() == 1
        finally:
            await producer.close()

    asyncio.run(main())


# Written as the Headers input: an empty value and a null value kept apart.
HEADERS = [("trace", b"abc"), ("empty", b""), ("null", None)]


# A consumed record: its key, value, headers (values copied out of the fetch
# buffer) and offset.
_Consumed = tuple[Any, Any, list[tuple[str, bytes | None]], int]


def _consumed(broker: Any, topic: str, *, partition: int = 0, at_least: int = 1,
              settle: float = 0.0, timeout: float = 60.0, **configs: Any) -> list[_Consumed]:
    """The records of ``topic``-``partition`` from offset 0, read back with the
    binding's consumer: at least ``at_least`` of them within ``timeout``, plus
    whatever arrives in the ``settle`` seconds after that."""
    consumer: KafkaConsumer[Any, Any] = KafkaConsumer(configs={
        "bootstrap.servers": broker.external_bootstrap, "group.protocol": "consumer",
        "group.id": f"py-read-{uuid.uuid4().hex[:12]}", "enable.auto.commit": False, **configs})
    records: list[_Consumed] = []
    try:
        tp = TopicPartition(topic=topic, partition=partition)
        consumer.assign(partitions=[tp])
        consumer.seek(partition=tp, offset=0)
        deadline = time.monotonic() + timeout
        settled: float | None = None
        while True:
            now = time.monotonic()
            if len(records) >= at_least:
                if settled is None:
                    settled = now + settle
                if now >= settled:
                    return records
            elif now >= deadline:
                raise AssertionError(
                    f"{len(records)} of {at_least} records consumed from {topic}-{partition}")
            for record in consumer.poll(timeout=0.5):
                records.append((record.key(), record.value(),
                                [(key, None if value is None else bytes(value))
                                 for key, value in record.headers()],
                                record.offset()))
    finally:
        consumer.close()


def _consumed_headers(broker: Any, topic: str) -> list[tuple[str, bytes | None]]:
    """The headers of the record at offset 0 of ``topic``-0."""
    return _consumed(broker, topic)[0][2]


def test_send_delivers_the_record_headers(kafka_broker: Any) -> None:
    # Critic 75 R2-B2: the core's borrowed send, which send_batch calls for
    # every record of the C engine, appended no headers; Java's doSend appends
    # record.headers().toArray() (KafkaProducer.java:1020, 1029-1030).
    topic = _topic(kafka_broker)
    producer = KafkaProducer(configs={"bootstrap.servers": kafka_broker.external_bootstrap})
    try:
        future = producer.send(record=ProducerRecord(topic=topic, partition=0, key=b"k",
                                                     value=b"v", headers=HEADERS))
        assert future.result(timeout=30).offset() == 0
    finally:
        producer.close()
    assert _consumed_headers(kafka_broker, topic) == HEADERS


def test_async_send_delivers_the_record_headers(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker)

    async def main() -> None:
        producer = AsyncKafkaProducer(
            configs={"bootstrap.servers": kafka_broker.external_bootstrap})
        try:
            future = await producer.send(record=ProducerRecord(
                topic=topic, partition=0, key=b"k", value=b"v", headers=HEADERS))
            assert (await asyncio.wait_for(future, 30)).offset() == 0
        finally:
            await producer.close()

    asyncio.run(main())
    assert _consumed_headers(kafka_broker, topic) == HEADERS


# ===========================================================================
# The send path around the producer's answers, against the broker
# ===========================================================================

# What a send() still waiting inside the producer raises once close() ends its
# wait: the FFI's refusal of the waiting record, or the core's closed check
# (Java's own race: KafkaProducer.throwIfProducerClosed vs. the accumulator's
# "Producer closed while send in progress").
_CLOSED_REFUSALS = ("Producer closed while send in progress",
                    "Cannot perform operation after producer has been closed")


def _is_closed_refusal(error: object) -> bool:
    return isinstance(error, (KafkaError, IllegalStateError)) and str(error) in _CLOSED_REFUSALS


def _partition_timeout(topic: str, partition: int, max_block_ms: int) -> str:
    """Java's waitOnMetadata TimeoutException for a partition the topic does
    not have (KafkaProducer.java:1144-1148)."""
    return (f"Partition {partition} of topic {topic} with partition count 1 is not present in "
            f"metadata after {max_block_ms} ms.")


def _wait_until(predicate: Callable[[], bool], timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        time.sleep(0.005)


def _record(topic: str, partition: int, value: bytes) -> ProducerRecord[Any, Any]:
    """Java's ``ProducerRecord(topic, partition, key, value)`` with a null key."""
    return ProducerRecord(topic=topic, partition=partition, key=None, value=value)


def _send_on_a_thread(producer: KafkaProducer[Any, Any],
                      record: ProducerRecord[Any, Any]) -> tuple[threading.Thread, list[Any]]:
    """``producer.send(record)`` on a thread, where it waits for the producer's
    answer; the future or the error it raised lands in the returned list."""
    outcome: list[Any] = []

    def run() -> None:
        try:
            outcome.append(producer.send(record=record))
        except Exception as error:  # noqa: BLE001 - the test reads which one
            outcome.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


def _bootstrap(broker: Any, **configs: Any) -> dict[str, Any]:
    return {"bootstrap.servers": broker.external_bootstrap, **configs}


# -- The metadata wait --------------------------------------------------------

def test_a_send_blocked_on_metadata_returns_a_failed_future_after_max_block_ms(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's waitOnMetadata throws TimeoutException once max.block.ms has
    # passed without the partition appearing in the metadata
    # (KafkaProducer.java:1127-1148), and doSend's catch (ApiException e)
    # gives it to the callback, with the record's partition in the metadata,
    # and to the future (KafkaProducer.java:1056-1068): send() returns the
    # failed future after waiting, without raising.
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    calls: list[tuple[RecordMetadata, Exception | None]] = []
    producer = KafkaProducer(configs=_bootstrap(kafka_broker, **{"max.block.ms": 500}))
    try:
        start = time.monotonic()
        future = producer.send(record=_record(topic, 3, b"v"),
                               callback=lambda metadata, exception: calls.append(
                                   (metadata, exception)))
        elapsed = time.monotonic() - start
        error = future.exception(timeout=30)
    finally:
        producer.close(timeout=0)
    assert answers == [None]
    assert 0.5 <= elapsed < 5, elapsed
    assert type(error) is KafkaTimeoutError and str(error) == _partition_timeout(topic, 3, 500)
    ((metadata, exception),) = calls
    assert exception is error
    assert (metadata.topic(), metadata.partition(), metadata.offset()) == (topic, 3, -1)
    assert not metadata.has_offset()


def test_an_async_send_blocked_on_metadata_returns_a_failed_future_after_max_block_ms(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    calls: list[tuple[RecordMetadata, Exception | None]] = []

    async def main() -> tuple[float, BaseException | None]:
        producer = AsyncKafkaProducer(configs=_bootstrap(kafka_broker, **{"max.block.ms": 500}))
        try:
            start = time.monotonic()
            future = await producer.send(
                record=_record(topic, 3, b"v"),
                callback=lambda metadata, exception: calls.append((metadata, exception)))
            elapsed = time.monotonic() - start
            await asyncio.wait((future,), timeout=30)
            return elapsed, future.exception()
        finally:
            await producer.close(timeout=0)

    elapsed, error = asyncio.run(main())
    assert answers == [None]
    assert 0.5 <= elapsed < 5, elapsed
    assert type(error) is KafkaTimeoutError and str(error) == _partition_timeout(topic, 3, 500)
    ((metadata, exception),) = calls
    assert exception is error
    assert (metadata.topic(), metadata.partition(), metadata.offset()) == (topic, 3, -1)


def test_blocked_sends_on_several_threads_wait_in_parallel(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Each send() waits for its own record, as each Java thread blocks in its
    # own waitOnMetadata: eight threads blocked on a missing partition all
    # return after one max.block.ms, not eight, each with its failed future.
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    producer = KafkaProducer(configs=_bootstrap(kafka_broker, **{"max.block.ms": 1000}))
    outcomes: list[Any] = [None] * 8

    def run(i: int) -> None:
        try:
            outcomes[i] = producer.send(record=_record(topic, 7, f"{i}".encode()))
        except Exception as error:  # noqa: BLE001 - the test reads which one
            outcomes[i] = error

    threads = [threading.Thread(target=run, args=(i,)) for i in range(8)]
    try:
        start = time.monotonic()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        elapsed = time.monotonic() - start
        assert not any(thread.is_alive() for thread in threads)
        assert 1.0 <= elapsed < 4, elapsed
        assert answers == [None] * 8
        for outcome in outcomes:
            assert isinstance(outcome, Future), outcome
            error = outcome.exception(timeout=30)
            assert type(error) is KafkaTimeoutError
            assert str(error) == _partition_timeout(topic, 7, 1000)
    finally:
        producer.close(timeout=0)


def test_a_send_blocked_on_metadata_does_not_hold_up_other_sends(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's send() blocks only its calling thread (waitOnMetadata): another
    # thread's send() to a topic with cached metadata is appended on that
    # thread at once and completes while the first still waits.
    cold = _topic(kafka_broker)
    warm = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    producer = KafkaProducer(configs=_bootstrap(kafka_broker, **{"max.block.ms": 3000}))
    try:
        assert producer.send(record=_record(warm, 0, b"warm-up")).result(timeout=30).offset() == 0
        assert answers == [None]
        blocked, outcome = _send_on_a_thread(producer, _record(cold, 7, b"blocked"))
        _wait_until(lambda: answers == [None, None])  # queued: its send() waits
        start = time.monotonic()
        future = producer.send(record=_record(warm, 0, b"not held up"))
        elapsed = time.monotonic() - start
        assert blocked.is_alive(), "the blocked send() returned early"
        assert answers == [None, None, True]
        assert elapsed < 0.5, elapsed
        assert future.result(timeout=30).offset() == 1
        blocked.join(30)
        assert not blocked.is_alive()
        (blocked_future,) = outcome
        assert isinstance(blocked_future, Future), outcome
        error = blocked_future.exception(timeout=30)
        assert type(error) is KafkaTimeoutError and str(error) == _partition_timeout(cold, 7, 3000)
    finally:
        producer.close(timeout=0)


def test_async_sends_waiting_for_metadata_do_not_block_the_loop(kafka_broker: Any) -> None:
    # Where Java's send() blocks its thread, the async send() awaits: a
    # hundred sends waiting out max.block.ms on a missing partition leave the
    # loop to its other tasks, and each gets its failed future.
    topic = _topic(kafka_broker)

    async def main() -> None:
        producer = AsyncKafkaProducer(configs=_bootstrap(kafka_broker, **{"max.block.ms": 1000}))
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.01)
                ticks += 1

        tick_task = asyncio.ensure_future(ticker())
        try:
            start = time.monotonic()
            sends = [asyncio.ensure_future(producer.send(record=_record(topic, 7, f"{i}".encode())))
                     for i in range(100)]
            futures = await asyncio.gather(*sends)
            elapsed = time.monotonic() - start
            tick_task.cancel()
            assert 1.0 <= elapsed < 5, elapsed
            assert ticks >= 50, ticks
            await asyncio.wait(futures, timeout=30)
            for future in futures:
                error = future.exception()
                assert type(error) is KafkaTimeoutError
                assert str(error) == _partition_timeout(topic, 7, 1000)
        finally:
            tick_task.cancel()
            await producer.close(timeout=0)

    asyncio.run(main())


# -- The buffer-memory wait ---------------------------------------------------

def test_a_send_waiting_for_buffer_memory_is_queued_until_a_batch_completes(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's send() blocks in BufferPool.allocate while all of buffer.memory
    # is in use (RecordAccumulator.append, up to max.block.ms): the producer
    # cannot take such a record at once, so it is queued and send() waits for
    # it. A waiter makes the batches holding the memory sendable the next
    # time the Sender looks (RecordAccumulator.ready: exhausted), whatever
    # linger.ms says; the Sender looks when it is woken, here as in Java by
    # a metadata request (waitOnMetadata: sender.wakeup()). The batch
    # completes, frees its memory, and the wait ends with the record taken.
    topic = _topic(kafka_broker, partitions=2)
    other = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    producer = KafkaProducer(configs=_bootstrap(kafka_broker, **{
        "buffer.memory": 16384, "batch.size": 16384, "linger.ms": 60000, "max.block.ms": 10000}))
    try:
        producer.partitions_for(topic=topic)  # the metadata, cached
        first = producer.send(record=_record(topic, 0, b"a"))  # the batch takes all the memory
        # doSend woke the Sender for the new batch: let it look (nothing is
        # sendable yet: linger.ms) and go back to its poll before the pool
        # is exhausted, so that only the wake below makes it look again.
        time.sleep(0.2)
        waiting, outcome = _send_on_a_thread(producer, _record(topic, 1, b"b"))  # no memory left
        _wait_until(lambda: answers == [True, None])
        assert waiting.is_alive(), "the send() waiting for memory returned early"
        start = time.monotonic()
        producer.partitions_for(topic=other)  # wakes the Sender, which finds the pool exhausted
        waiting.join(5)
        elapsed = time.monotonic() - start
        assert not waiting.is_alive(), "the send() waiting for memory did not return"
        assert elapsed < 5, elapsed
        # The first batch was sent to free its memory, long before linger.ms.
        assert first.result(timeout=5).offset() == 0
        (second,) = outcome
        assert isinstance(second, Future), outcome
        producer.flush()
        assert second.result(timeout=30).offset() == 0
    finally:
        producer.close(timeout=0)


# -- Records queued when their send() ended early -----------------------------

def test_queued_records_are_produced_once_and_in_order_when_their_partition_appears(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # A send() waiting for its partition's metadata (Java: blocked in
    # waitOnMetadata, which refreshes the metadata until the partition
    # appears or max.block.ms passes) returns once the partition is created.
    # Four threads, each sending a hundred records in a row to a partition
    # that appears while their first record waits: every record is produced
    # exactly once, in each thread's send order.
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    producer = KafkaProducer(configs=_bootstrap(kafka_broker, **{"max.block.ms": 30000}))
    per_thread = 100
    futures: dict[int, list[Future[RecordMetadata]]] = {i: [] for i in range(4)}
    errors: list[Exception] = []

    def run(i: int) -> None:
        try:
            for j in range(per_thread):
                futures[i].append(producer.send(record=_record(topic, 1, f"t{i}-{j}".encode())))
        except Exception as error:  # noqa: BLE001 - the test reads which one
            errors.append(error)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(4)]
    try:
        for thread in threads:
            thread.start()
        _wait_until(lambda: answers.count(None) == 4)  # each thread's first record waits
        time.sleep(0.2)
        assert all(thread.is_alive() for thread in threads)
        _add_partitions(kafka_broker, topic, 2)
        for thread in threads:
            thread.join(60)
        assert not any(thread.is_alive() for thread in threads)
        assert errors == []
        offsets = {i: [future.result(timeout=60).offset() for future in futures[i]]
                   for i in range(4)}
    finally:
        producer.close(timeout=0)
    for i in range(4):
        assert offsets[i] == sorted(offsets[i]), f"thread {i}'s records are out of order"
    assert answers.count(None) == 4 and answers.count(True) == 4 * per_thread - 4
    values = [value for _, value, _, _ in _consumed(kafka_broker, topic, partition=1,
                                                      at_least=4 * per_thread, settle=1.0)]
    assert len(values) == 4 * per_thread and len(set(values)) == 4 * per_thread
    for i in range(4):
        assert [v for v in values if v.startswith(f"t{i}-".encode())] == [
            f"t{i}-{j}".encode() for j in range(per_thread)]


def test_a_record_queued_when_ctrl_c_ended_its_send_is_produced_when_it_can_be(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Ctrl+C ends the send()'s wait (Java's InterruptException). The record
    # stays queued inside the producer and is produced once its partition
    # appears (where Java drops it: a retry after the interrupt duplicates
    # it). Its future is tracked when the producer takes it, so flush() waits
    # for it, and its callback runs with its offset.
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    producer = KafkaProducer(configs=_bootstrap(kafka_broker, **{
        "max.block.ms": 30000, "linger.ms": 2000}))
    calls: list[tuple[RecordMetadata, Exception | None]] = []
    armed = True

    def on_sigint(signum: int, frame: FrameType | None) -> None:
        if armed:
            raise KeyboardInterrupt

    def interrupt_once_queued() -> None:
        _wait_until(lambda: answers == [None])
        os.kill(os.getpid(), signal.SIGINT)

    previous = signal.signal(signal.SIGINT, on_sigint)
    interrupter = threading.Thread(target=interrupt_once_queued)
    try:
        interrupter.start()
        with pytest.raises(KeyboardInterrupt):
            producer.send(record=_record(topic, 1, b"interrupted"),
                          callback=lambda metadata, exception: calls.append(
                              (metadata, exception)))
    finally:
        interrupter.join(30)
        armed = False
        time.sleep(0.05)  # a signal still pending runs the disarmed handler
        signal.signal(signal.SIGINT, previous)
    try:
        assert not producer._futures  # noqa: SLF001
        _add_partitions(kafka_broker, topic, 2)
        # Taken: tracked for flush(), and in its batch for linger.ms.
        _wait_until(lambda: len(producer._futures) == 1)  # noqa: SLF001
        assert calls == []
        producer.flush()
        assert not producer._futures  # noqa: SLF001
        ((metadata, exception),) = calls
        assert exception is None
        assert (metadata.partition(), metadata.offset()) == (1, 0)
    finally:
        producer.close(timeout=0)
    ((key, value, headers, offset),) = _consumed(kafka_broker, topic, partition=1)
    assert (key, value, headers, offset) == (None, b"interrupted", [], 0)


# -- What reaches the broker --------------------------------------------------

def test_a_record_built_inline_reaches_the_broker_intact_after_its_metadata_wait(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # The record is built inside the send() call and referenced by nothing
    # else: while it is queued (the topic's metadata is not cached), the pin
    # Producer_send holds keeps its key, value and header buffers alive for
    # the producer, which borrows them (CLAUDE.md §14). The key, the value, a
    # header, and a tombstone with a null header value arrive as sent.
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    producer = KafkaProducer(configs=_bootstrap(kafka_broker))
    try:
        first = producer.send(record=ProducerRecord(
            topic=topic, partition=0, key=b"key-1", value=b"value-1",
            headers=[("h", b"x" * 1000)]))
        gc.collect()
        second = producer.send(record=ProducerRecord(
            topic=topic, partition=0, key=b"key-2", value=None,
            headers=[("tomb", None), ("why", b"gone")]))
        assert answers == [None, True]
        assert first.result(timeout=30).offset() == 0
        assert second.result(timeout=30).offset() == 1
    finally:
        producer.close()
    assert _consumed(kafka_broker, topic, at_least=2) == [
        (b"key-1", b"value-1", [("h", b"x" * 1000)], 0),
        (b"key-2", None, [("tomb", None), ("why", b"gone")], 1)]


# -- The threads --------------------------------------------------------------

def test_a_send_from_a_delivery_callback_reaches_the_broker(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Java allows send() inside a callback (only flush() and a waiting close()
    # are ruled out). A record to a topic with cached metadata is appended at
    # once; one to a cold topic makes the callback wait for the producer's
    # answer, which comes from the dispatcher thread, not from the completion
    # thread the callback runs on, so nothing deadlocks. Both arrive, and a
    # later close() returns.
    warm = _topic(kafka_broker)
    cold = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    producer = KafkaProducer(configs=_bootstrap(kafka_broker))
    inner: dict[str, Future[RecordMetadata]] = {}
    done = threading.Event()

    def callback(metadata: RecordMetadata, exception: Exception | None) -> None:
        inner["warm"] = producer.send(record=_record(warm, 0, b"from the callback, warm"))
        inner["cold"] = producer.send(record=_record(cold, 0, b"from the callback, cold"))
        done.set()

    try:
        outer = producer.send(record=_record(warm, 0, b"outer"), callback=callback)
        assert outer.result(timeout=30).offset() == 0
        assert done.wait(30)
        assert answers == [None, True, None]
        assert inner["warm"].result(timeout=30).offset() == 1
        assert inner["cold"].result(timeout=30).offset() == 0
        start = time.monotonic()
        producer.close()
        assert time.monotonic() - start < 10
    finally:
        producer.close(timeout=0)


def test_an_async_send_from_a_delivery_callback_reaches_the_broker(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # The async callback runs on the loop and must not block it: it starts
    # the sends as tasks, which await the producer's answers.
    warm = _topic(kafka_broker)
    cold = _topic(kafka_broker)
    answers = _answers(monkeypatch)

    async def main() -> None:
        producer = AsyncKafkaProducer(configs=_bootstrap(kafka_broker))
        inner: dict[str, asyncio.Task[asyncio.Future[RecordMetadata]]] = {}

        def callback(metadata: RecordMetadata, exception: Exception | None) -> None:
            inner["warm"] = asyncio.ensure_future(
                producer.send(record=_record(warm, 0, b"from the callback, warm")))
            inner["cold"] = asyncio.ensure_future(
                producer.send(record=_record(cold, 0, b"from the callback, cold")))

        try:
            outer = await producer.send(record=_record(warm, 0, b"outer"), callback=callback)
            assert (await asyncio.wait_for(outer, 30)).offset() == 0
            assert set(inner) == {"warm", "cold"}
            warm_future = await asyncio.wait_for(inner["warm"], 30)
            cold_future = await asyncio.wait_for(inner["cold"], 30)
            assert answers == [None, True, None]
            assert (await asyncio.wait_for(warm_future, 30)).offset() == 1
            assert (await asyncio.wait_for(cold_future, 30)).offset() == 0
            start = time.monotonic()
            await producer.close()
            assert time.monotonic() - start < 10
        finally:
            await producer.close(timeout=0)

    asyncio.run(main())


def _answer_threads(monkeypatch: pytest.MonkeyPatch) -> set[int]:
    """The threads the producer's answers for queued records (``cb(None,
    error)``) arrive on: the dispatcher's."""
    threads: set[int] = set()
    real = _lib.Producer_send

    def spy(c_producer: int, native: Any, cb: Any) -> Any:
        def answering(result: int | None, error: int) -> None:
            if result is None:
                threads.add(threading.get_ident())
            cb(result, error)

        return real(c_producer, native, answering)

    monkeypatch.setattr(_lib, "Producer_send", spy)
    return threads


def test_delivery_callbacks_run_on_one_completion_thread(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Java runs the callbacks on the producer's I/O thread, one at a time;
    # here on the extension's one completion thread per producer, for a
    # record taken at once as for one queued and taken later: never on the
    # calling thread, and not on the dispatcher thread that answers for the
    # queued records.
    topic = _topic(kafka_broker)
    answer_threads = _answer_threads(monkeypatch)
    producer = KafkaProducer(configs=_bootstrap(kafka_broker))
    callback_threads: list[int] = []
    try:
        futures = [producer.send(record=_record(topic, 0, f"{i}".encode()),
                                 callback=lambda metadata, exception: callback_threads.append(
                                     threading.get_ident()))
                   for i in range(5)]
        for future in futures:
            future.result(timeout=30)
    finally:
        producer.close(timeout=0)
    caller = threading.get_ident()
    assert len(callback_threads) == 5 and len(set(callback_threads)) == 1
    assert caller not in callback_threads
    assert answer_threads and caller not in answer_threads
    assert not set(callback_threads) & answer_threads


def test_async_delivery_callbacks_run_on_the_loop(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    topic = _topic(kafka_broker)
    answer_threads = _answer_threads(monkeypatch)
    callback_threads: list[int] = []

    async def main() -> int:
        producer = AsyncKafkaProducer(configs=_bootstrap(kafka_broker))
        try:
            futures = [await producer.send(
                record=_record(topic, 0, f"{i}".encode()),
                callback=lambda metadata, exception: callback_threads.append(
                    threading.get_ident()))
                for i in range(5)]
            await asyncio.wait_for(asyncio.gather(*futures), 30)
        finally:
            await producer.close(timeout=0)
        return threading.get_ident()

    loop_thread = asyncio.run(main())
    assert callback_threads == [loop_thread] * 5
    assert answer_threads and loop_thread not in answer_threads


# -- Transaction control and the context manager beside a blocked send() ------

def test_transaction_control_does_not_wait_for_a_send_blocked_on_metadata(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # A send() blocked in its metadata wait holds no batch (Java: the thread
    # is in waitOnMetadata, before the append), so flush(),
    # commit_transaction() and abort_transaction() do not wait for it, as in
    # Java 4.2: the transaction's other records are committed or aborted
    # meanwhile, and close(0) ends the blocked send. linger.ms keeps C's
    # batch undrained when the abort comes: a batch the Sender had already
    # sent would complete instead, in Java too.
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    producer = KafkaProducer(configs={**_configs(kafka_broker),
                                      "max.block.ms": 20000, "linger.ms": 5000})
    try:
        producer.init_transactions()
        producer.begin_transaction()
        blocked, outcome = _send_on_a_thread(producer, _record(topic, 7, b"blocked"))
        _wait_until(lambda: answers == [None])
        committed = producer.send(record=_record(topic, 0, b"A"))
        start = time.monotonic()
        producer.flush()
        assert time.monotonic() - start < 2
        assert committed.result(timeout=30).offset() == 0
        start = time.monotonic()
        producer.commit_transaction()
        assert time.monotonic() - start < 2
        producer.begin_transaction()
        aborted = producer.send(record=_record(topic, 0, b"C"))
        start = time.monotonic()
        producer.abort_transaction()
        assert time.monotonic() - start < 2
        # Sender.maybeSendAndPollTransactionalRequest aborts the undrained
        # batches with a TransactionAbortedException, whose default message
        # this is (TransactionAbortedException.java:36).
        error = aborted.exception(timeout=30)
        assert type(error) is TransactionAbortedError
        assert str(error) == "Failing batch since transaction was aborted"
        assert blocked.is_alive(), "the blocked send() returned early"
        start = time.monotonic()
        producer.close(timeout=0)
        assert time.monotonic() - start < 5
        blocked.join(30)
        assert not blocked.is_alive(), "the blocked send() did not return after the close"
        (blocked_outcome,) = outcome
        assert _is_closed_refusal(blocked_outcome), outcome
    finally:
        producer.close(timeout=0)
    values = [value for _, value, _, _ in _consumed(
        kafka_broker, topic, settle=2.0, **{"isolation.level": "read_committed"})]
    assert values == [b"A"]


def test_the_context_manager_exit_does_not_wait_for_a_send_blocked_on_metadata(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Closeable: the exit flushes, then closes. Neither waits for another
    # thread's send() blocked in its metadata wait, whose record no batch
    # holds; the close fails that record, and its send() raises the closed
    # refusal, as Java's does when close() ends its wait.
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)
    with KafkaProducer(configs=_bootstrap(kafka_broker, **{"max.block.ms": 20000})) as producer:
        delivered = producer.send(record=_record(topic, 0, b"delivered"))
        blocked, outcome = _send_on_a_thread(producer, _record(topic, 7, b"blocked"))
        _wait_until(lambda: answers == [None, None])
        start = time.monotonic()
    assert time.monotonic() - start < 5
    assert delivered.result(timeout=0).offset() == 0  # flushed by the exit
    blocked.join(30)
    assert not blocked.is_alive(), "the blocked send() did not return after the exit"
    (error,) = outcome
    assert _is_closed_refusal(error), outcome


def test_the_async_context_manager_exit_does_not_wait_for_a_send_blocked_on_metadata(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    topic = _topic(kafka_broker)
    answers = _answers(monkeypatch)

    async def main() -> None:
        async with AsyncKafkaProducer(
                configs=_bootstrap(kafka_broker, **{"max.block.ms": 20000})) as producer:
            delivered = await producer.send(record=_record(topic, 0, b"delivered"))
            blocked = asyncio.ensure_future(producer.send(record=_record(topic, 7, b"blocked")))
            while answers != [None, None]:
                await asyncio.sleep(0.005)
            start = time.monotonic()
        assert time.monotonic() - start < 5
        assert delivered.done() and delivered.result().offset() == 0  # flushed by the exit
        with pytest.raises((KafkaError, IllegalStateError)) as err:
            await asyncio.wait_for(blocked, 30)
        assert _is_closed_refusal(err.value), str(err.value)

    asyncio.run(main())


# -- The pin: (record, callback, producer), held until the completion ---------

def _pins(monkeypatch: pytest.MonkeyPatch) -> list[weakref.ref[Any]]:
    """Weak references to the callback closure each send() hands
    Producer_send: the pin and send()'s own frame are its only holders, so it
    dies with the pin once send() has returned."""
    refs: list[weakref.ref[Any]] = []
    real = _lib.Producer_send

    def spy(c_producer: int, native: Any, cb: Any) -> Any:
        refs.append(weakref.ref(cb))
        return real(c_producer, native, cb)

    monkeypatch.setattr(_lib, "Producer_send", spy)
    return refs


def test_the_pin_of_a_record_taken_at_once_lives_until_its_delivery_report(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # A record appended on the calling thread sits in its batch for
    # linger.ms: the pin lives through that, and is released when the
    # delivery report has run.
    topic = _topic(kafka_broker)
    producer = KafkaProducer(configs=_bootstrap(kafka_broker, **{"linger.ms": 1000}))
    try:
        producer.partitions_for(topic=topic)  # the metadata, cached
        refs = _pins(monkeypatch)
        future = producer.send(record=_record(topic, 0, b"v"))
        gc.collect()
        assert [ref() is not None for ref in refs] == [True]
        assert future.result(timeout=30).offset() == 0
        _wait_until(lambda: refs[0]() is None, timeout=10)
    finally:
        producer.close(timeout=0)


def test_the_pin_of_a_refused_record_in_a_batch_is_released_by_its_callback(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Java appends before maybeAddPartition (KafkaProducer.java:1029-1038): a
    # transactional send() outside a transaction raises with its record in a
    # batch and its callback still to come. The pin stays with the record
    # until that callback has run: here when the forced close fails the
    # batch.
    topic = _topic(kafka_broker)
    refs = _pins(monkeypatch)
    producer = KafkaProducer(configs=_configs(kafka_broker))
    calls: list[Exception | None] = []
    try:
        producer.init_transactions()
        raised: tuple[type, str] | None = None
        try:
            producer.send(record=_record(topic, 0, b"v"),
                          callback=lambda metadata, exception: calls.append(exception))
        except IllegalStateError as error:
            raised = (type(error), str(error))  # not the error: its traceback keeps send()'s frame
        assert raised == (IllegalStateError,
                          f"Cannot add partition {topic}-0 to transaction while in state  READY")
        gc.collect()
        assert [ref() is not None for ref in refs] == [True]
        assert calls == []
    finally:
        producer.close(timeout=0)
    assert len(calls) == 1 and str(calls[0]) == "Producer is closed forcefully."
    gc.collect()
    assert [ref() for ref in refs] == [None]
