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
rethrows out of ``send()`` (``KafkaProducer.java:1143-1155``) rather than
giving to the callback and the future, which need the topic's metadata first
(the record then stays in the accumulator, so its callback runs later, with
the batch's outcome);

the record size check, which runs after the metadata wait too
(``KafkaProducer.java:1103-1105``); an open transaction, which a second
``begin_transaction()`` needs; and the non-blocking send, whose record taken on
the calling thread needs the topic's metadata cached, and whose send waiting
for a partition that appears later must hold back no other thread's record.
Skips without Docker (``kafka_broker``)."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.common import KafkaError, KafkaFuture, TopicPartition
from confluent_kafka.common.errors import RecordTooLargeError
from confluent_kafka.common.serialization import string_deserializer, string_serializer
from confluent_kafka.consumer import KafkaConsumer
from confluent_kafka.producer import (
    AsyncKafkaProducer, KafkaProducer, ProducerRecord, RecordMetadata,
)

from ..unit.producer_futures import async_failure, failure
from .conftest import KAFKA_BIN, create_topic


def _topic(broker: Any) -> str:
    topic = f"py-producer-{uuid.uuid4().hex[:12]}"
    create_topic(broker, topic)
    return topic


def _configs(broker: Any) -> dict[str, Any]:
    return {"bootstrap.servers": broker.external_bootstrap,
            "transactional.id": f"py-txn-{uuid.uuid4().hex[:12]}"}


def _committed(broker: Any, topic: str,
               count: int = 1) -> list[tuple[int, str | None, str | None]]:
    """The ``(offset, key, value)`` of the first ``count`` records of
    ``topic``-0 that a ``read_committed`` consumer reads: a record of an aborted
    or still open transaction is never among them, so this proves the commit
    included it where its acknowledged offset cannot."""
    consumer: KafkaConsumer[str, str] = KafkaConsumer(
        configs={"bootstrap.servers": broker.external_bootstrap,
                 "group.protocol": "consumer",
                 "group.id": f"py-committed-{uuid.uuid4().hex[:12]}",
                 "enable.auto.commit": False,
                 "isolation.level": "read_committed"},
        key_deserializer=string_deserializer(), value_deserializer=string_deserializer())
    try:
        tp = TopicPartition(topic=topic, partition=0)
        consumer.assign(partitions=[tp])
        consumer.seek(partition=tp, offset=0)
        consumed: list[tuple[int, str | None, str | None]] = []
        deadline = time.monotonic() + 60
        while len(consumed) < count and time.monotonic() < deadline:
            consumed.extend((r.offset(), r.key(), r.value())
                            for r in consumer.poll(timeout=0.5))
        if not consumed:
            raise AssertionError("no committed record was consumed")
        return consumed
    finally:
        consumer.close()


_Call = tuple[str, int, int, Exception | None]


def _recorder(calls: list[_Call], label: str) -> Any:
    """A callback appending ``(label, partition, offset, exception)``."""
    def callback(metadata: RecordMetadata, exception: Exception | None) -> None:
        calls.append((label, metadata.partition(), metadata.offset(), exception))
    return callback


def test_send_before_init_transactions_raises(kafka_broker: Any) -> None:
    # TransactionManager.maybeAddPartition's IllegalStateException
    # (TransactionManager.java:469), rethrown by doSend's catch (Exception e):
    # send() raises it, and the callback does not run yet. doSend appended the
    # record before (KafkaProducer.java:1108-1120), and the Sender holds its
    # batch, which no transaction adds. close(Duration.ZERO) force-closes:
    # abortIncompleteBatches fails it with "Producer is closed forcefully."
    # (RecordAccumulator.java:1194), and the callback runs then, with the -1
    # metadata of KafkaProducer.AppendCallbacks.onCompletion (:1676-1679).

    topic = _topic(kafka_broker)
    calls: list[_Call] = []
    producer = KafkaProducer(configs=_configs(kafka_broker), key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    try:
        try:
            producer.send(record=ProducerRecord(topic=topic, key="key", value="value"),
                          callback=_recorder(calls, "value"))
        except IllegalStateError as error:
            assert str(error) == (f"Cannot add partition {topic}-0 to transaction before "
                                  "completing a call to initTransactions")
        else:
            raise AssertionError("send() did not raise")
        assert calls == []
    finally:
        producer.close(timeout=0)
    assert len(calls) == 1
    label, partition, offset, exception = calls[0]
    assert (label, partition, offset) == ("value", 0, -1)
    assert type(exception) is KafkaError
    assert str(exception) == "Producer is closed forcefully."


def _outside_then_inside(inside_offset: int) -> list[tuple[int, str, str]]:
    """What a read_committed consumer reads after the two sends below and the
    commit: both records. Java's doSend appends the record to the accumulator
    before maybeAddPartition throws (KafkaProducer.java:1110-1120), and the
    Sender holds the batch until its partition joins a transaction, so the send
    that raised is committed with the transaction, ahead of the one inside it."""
    return [(0, "key", "outside"), (inside_offset, "key", "inside")]


def _callbacks_outside_then_inside(inside_offset: int) -> list[_Call]:
    """The callbacks Java runs for the two sends below, once the commit has
    flushed their batch: the record whose send() raised completes with the
    batch too, its appendCallbacks carrying its Callback, and callbacks of one
    partition run in send order (ProducerBatch.completeFutureAndFireCallbacks),
    each with its record's metadata and no error."""
    return [("outside", 0, 0, None), ("inside", 0, inside_offset, None)]


def test_send_outside_a_transaction_raises(kafka_broker: Any) -> None:
    # After initTransactions and before beginTransaction the transaction
    # manager is READY (TransactionManager.java:472, with Java's double space).
    topic = _topic(kafka_broker)
    calls: list[_Call] = []
    producer = KafkaProducer(configs=_configs(kafka_broker), key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    try:
        producer.init_transactions()
        try:
            producer.send(record=ProducerRecord(topic=topic, key="key", value="outside"),
                          callback=_recorder(calls, "outside"))
        except IllegalStateError as error:
            assert str(error) == (f"Cannot add partition {topic}-0 to transaction while in "
                                  "state  READY")
        else:
            raise AssertionError("send() did not raise")
        assert calls == []

        # Inside a transaction the same send goes through. The commit flushes
        # the batch holding both records, and both callbacks have run when it
        # returns.
        producer.begin_transaction()
        future = producer.send(record=ProducerRecord(topic=topic, key="key", value="inside"),
                               callback=_recorder(calls, "inside"))
        producer.commit_transaction()
        offset = future.get(timeout=30).offset()
        assert calls == _callbacks_outside_then_inside(offset)
    finally:
        producer.close(timeout=0)
    assert _committed(kafka_broker, topic, 2) == _outside_then_inside(offset)


def test_async_send_outside_a_transaction_raises(kafka_broker: Any) -> None:
    topic = _topic(kafka_broker)

    async def main() -> int:
        producer = AsyncKafkaProducer(configs=_configs(kafka_broker),
                                      key_serializer=string_serializer(),
                                      value_serializer=string_serializer())
        calls: list[_Call] = []
        try:
            await producer.init_transactions()
            try:
                await producer.send(
                    record=ProducerRecord(topic=topic, key="key", value="outside"),
                    callback=_recorder(calls, "outside"))
            except IllegalStateError as error:
                assert str(error) == (f"Cannot add partition {topic}-0 to transaction while "
                                      "in state  READY")
            else:
                raise AssertionError("send() did not raise")
            assert calls == []

            await producer.begin_transaction()
            future = await producer.send(
                record=ProducerRecord(topic=topic, key="key", value="inside"),
                callback=_recorder(calls, "inside"))
            await producer.commit_transaction()
            offset = (await future.get(timeout=30)).offset()
            assert calls == _callbacks_outside_then_inside(offset)
            return offset
        finally:
            await producer.close(timeout=0)

    offset = asyncio.run(main())
    assert _committed(kafka_broker, topic, 2) == _outside_then_inside(offset)


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


# Java's KafkaProducer.ensureValidRecordSize (KafkaProducer.java:1243-1252):
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
    # doSend's catch (ApiException e) (KafkaProducer.java:1130-1142) gives the
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
        error = failure(future, 30)
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
            return await async_failure(future, 30)
        finally:
            await producer.close()

    calls: list[tuple[RecordMetadata, Exception | None]] = []
    error = asyncio.run(main())
    assert error is not None
    _assert_record_too_large(topic, calls, error, message)


def _invalid_begin(transactional_id: str) -> str:
    return (f"TransactionalId {transactional_id}: Invalid transition attempted from state "
            "IN_TRANSACTION to state IN_TRANSACTION")


def test_begin_transaction_fails_at_once_and_an_open_transaction_keeps_its_records(
        kafka_broker: Any) -> None:
    # Critic 75 N1: a record sent in the open transaction stays in that
    # transaction: a second begin_transaction() fails at once with the
    # transaction manager's invalid transition, and the commit_transaction()
    # after it commits the record (producer-transactions.md §13). send()
    # returned once the record reached the producer, as Java's returns once
    # doSend appended it, so the second begin has nothing to wait for. The
    # branch C API has no hook to hold a record's delivery (master's
    # Producer_test_set_paused), and the record may be acknowledged before the
    # commit, so whether its future is still pending at the second begin is
    # not checked.
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
        assert time.monotonic() - start < 5
        assert str(err.value) == _invalid_begin(configs["transactional.id"])
        producer.commit_transaction()
        offset = future.get(timeout=30).offset()
    finally:
        producer.close(timeout=0)
    assert _committed(kafka_broker, topic) == [(offset, "key", "value")]


def test_async_begin_transaction_fails_at_once_and_an_open_transaction_keeps_its_records(
        kafka_broker: Any) -> None:
    # The async peer of the test above, updated for the branch C API the same way.
    topic = _topic(kafka_broker)
    configs = _configs(kafka_broker)

    async def main() -> int:
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
            assert time.monotonic() - start < 5
            assert str(err.value) == _invalid_begin(configs["transactional.id"])
            await producer.commit_transaction()
            return (await future.get(timeout=30)).offset()
        finally:
            await producer.close(timeout=0)

    offset = asyncio.run(main())
    assert _committed(kafka_broker, topic) == [(offset, "key", "value")]


# Written as the Headers input: an empty value and a null value kept apart.
HEADERS = [("trace", b"abc"), ("empty", b""), ("null", None)]


def _consumed_headers(broker: Any, topic: str) -> list[tuple[str, bytes | None]]:
    """The headers of the record at offset 0 of ``topic``-0, read back with the
    binding's consumer."""
    consumer = KafkaConsumer(configs={"bootstrap.servers": broker.external_bootstrap,
                                      "group.protocol": "consumer",
                                      "group.id": f"py-headers-{uuid.uuid4().hex[:12]}",
                                      "enable.auto.commit": False})
    try:
        tp = TopicPartition(topic=topic, partition=0)
        consumer.assign(partitions=[tp])
        consumer.seek(partition=tp, offset=0)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            for record in consumer.poll(timeout=0.5):
                return [(key, None if value is None else bytes(value))
                        for key, value in record.headers()]
        raise AssertionError("the record was not consumed")
    finally:
        consumer.close()


def test_send_delivers_the_record_headers(kafka_broker: Any) -> None:
    # Critic 75 R2-B2: the core's borrowed send, which send_batch calls for
    # every record of the C engine, appended no headers; Java's doSend appends
    # record.headers().toArray() (KafkaProducer.java:1101, 1110-1111).
    topic = _topic(kafka_broker)
    producer = KafkaProducer(configs={"bootstrap.servers": kafka_broker.external_bootstrap})
    try:
        future = producer.send(record=ProducerRecord(topic=topic, partition=0, key=b"k",
                                                     value=b"v", headers=HEADERS))
        assert future.get(timeout=30).offset() == 0
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
            assert (await future.get(timeout=30)).offset() == 0
        finally:
            await producer.close()

    asyncio.run(main())
    assert _consumed_headers(kafka_broker, topic) == HEADERS


def test_async_commit_transaction_waits_for_an_async_def_callback(kafka_broker: Any) -> None:
    # commitTransaction flushes the transaction's records, their callbacks
    # fired first (ProducerBatch.completeFutureAndFireCallbacks before the
    # EndTxn result completes): an awaited `async def` callback holds the
    # commit until it returned and reported.
    topic = _topic(kafka_broker)
    configs = _configs(kafka_broker)

    async def main() -> int:
        release = asyncio.Event()
        started = asyncio.Event()
        finished: list[bool] = []

        async def callback(metadata: RecordMetadata, exception: Exception | None) -> None:
            assert exception is None
            started.set()
            await release.wait()
            finished.append(True)

        producer = AsyncKafkaProducer(configs=configs, key_serializer=string_serializer(),
                                      value_serializer=string_serializer())
        try:
            await producer.init_transactions()
            await producer.begin_transaction()
            future = await producer.send(
                record=ProducerRecord(topic=topic, key="key", value="value"),
                callback=callback)
            commit = asyncio.ensure_future(producer.commit_transaction())
            await asyncio.wait_for(started.wait(), 60)
            await asyncio.sleep(0.5)
            assert not commit.done() and not future.is_done() and finished == []
            release.set()
            await asyncio.wait_for(commit, 60)
            assert finished == [True]
            return (await future.get(timeout=30)).offset()
        finally:
            await producer.close(timeout=0)

    offset = asyncio.run(main())
    assert _committed(kafka_broker, topic) == [(offset, "key", "value")]


# ===========================================================================
# The send's fast path: a record taken on the calling thread, and a send
# waiting on another thread
# ===========================================================================


def _handovers(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Where each record's handover completed, from the ``resolve`` of
    ``Producer_send_cb``: ``"inline"`` inside the call, on the calling
    thread, for a record the producer took (or refused) at once; ``"queued"``
    later, from the callback pump, for one queued on the submission task."""
    handovers: list[str] = []
    real = _lib.Producer_send_cb

    def spy(c_producer: int, native: Any, on_completion: Any, resolve: Any) -> Any:
        caller = threading.get_ident()
        inside = True

        def resolved(error: int) -> None:
            handovers.append("inline" if inside and threading.get_ident() == caller else "queued")
            resolve(error)

        try:
            return real(c_producer, native, on_completion, resolved)
        finally:
            inside = False

    monkeypatch.setattr(_lib, "Producer_send_cb", spy)
    return handovers


def _add_partitions(broker: Any, topic: str, partitions: int) -> None:
    """Grow ``topic`` to ``partitions`` partitions via kafka-topics.sh inside
    the container."""
    rc, out = broker.exec([
        f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", broker.internal_bootstrap,
        "--alter", "--topic", topic, "--partitions", str(partitions),
    ])
    if rc not in (0, None):
        raise RuntimeError(f"adding partitions failed (rc={rc}): {out!r}")


def _record(topic: str, partition: int, value: bytes) -> ProducerRecord[Any, Any]:
    """Java's ``ProducerRecord(topic, partition, key, value)`` with a null key."""
    return ProducerRecord(topic=topic, partition=partition, key=None, value=value)


def _records_of(broker: Any, topic: str, partition: int, *, from_offset: int,
                window: float) -> list[tuple[int, bytes | None]]:
    """The ``(offset, value)`` of the records of ``topic``-``partition`` from
    ``from_offset`` that arrive within ``window`` seconds, read back with the
    binding's consumer."""
    consumer: KafkaConsumer[Any, Any] = KafkaConsumer(configs={
        "bootstrap.servers": broker.external_bootstrap, "group.protocol": "consumer",
        "group.id": f"py-records-{uuid.uuid4().hex[:12]}", "enable.auto.commit": False})
    try:
        tp = TopicPartition(topic=topic, partition=partition)
        consumer.assign(partitions=[tp])
        consumer.seek(partition=tp, offset=from_offset)
        records: list[tuple[int, bytes | None]] = []
        deadline = time.monotonic() + window
        while time.monotonic() < deadline:
            records.extend((r.offset(), None if r.value() is None else bytes(r.value()))
                           for r in consumer.poll(timeout=0.5))
        return records
    finally:
        consumer.close()


def test_a_send_with_cached_metadata_is_taken_on_the_calling_thread(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's send() appends the record on the calling thread, after blocking
    # in waitOnMetadata for the first record of a topic (KafkaProducer.java:
    # 1065-1111). Here the producer takes a record at once only with its
    # topic's metadata cached: the first record is queued on the submission
    # task, whose send() waits for the metadata, and its handover completes
    # from the callback pump, the caller's send() waiting for it; the next
    # one, with the metadata cached, is appended on the calling thread and its
    # handover completes inside send().
    topic = _topic(kafka_broker)
    handovers = _handovers(monkeypatch)
    producer = KafkaProducer(configs={"bootstrap.servers": kafka_broker.external_bootstrap})
    try:
        first = producer.send(record=_record(topic, 0, b"a"))
        assert first.get(timeout=30).offset() == 0
        second = producer.send(record=_record(topic, 0, b"b"))
        assert handovers == ["queued", "inline"]
        assert second.get(timeout=30).offset() == 1
    finally:
        producer.close()


def test_an_async_send_with_cached_metadata_is_taken_on_the_calling_thread(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # The async peer of the test above: the second record's handover
    # completes inside send(), on the loop.
    topic = _topic(kafka_broker)
    handovers = _handovers(monkeypatch)

    async def main() -> None:
        producer = AsyncKafkaProducer(
            configs={"bootstrap.servers": kafka_broker.external_bootstrap})
        try:
            first = await producer.send(record=_record(topic, 0, b"a"))
            assert (await first.get(timeout=30)).offset() == 0
            second = await producer.send(record=_record(topic, 0, b"b"))
            assert handovers == ["queued", "inline"]
            assert (await second.get(timeout=30)).offset() == 1
        finally:
            await producer.close()

    asyncio.run(main())


def test_a_record_taken_at_once_is_not_held_behind_one_waiting_on_another_thread(
        kafka_broker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's send() blocks in waitOnMetadata, up to max.block.ms, for a
    # partition the topic does not have yet (KafkaProducer.java:1195-1226),
    # which holds back its caller's next record but no other thread's. Here
    # such a record is queued on the submission task, which leaves it to wait
    # for the partition on a task of its own, and its send() waits for that
    # handover; another thread's record, to a partition the producer can take
    # at once, is appended on that thread and reaches the broker while the
    # first still waits (the C API's send tries the producer first,
    # whatever the submission queue holds). Once the partition appears, the
    # first record is handed over and its send() returns.
    topic = _topic(kafka_broker)  # one partition
    handovers = _handovers(monkeypatch)
    tried = threading.Event()
    spied = _lib.Producer_send_cb

    def signalling(*args: Any) -> Any:
        try:
            return spied(*args)
        finally:
            tried.set()

    monkeypatch.setattr(_lib, "Producer_send_cb", signalling)
    producer = KafkaProducer(configs={"bootstrap.servers": kafka_broker.external_bootstrap,
                                      "max.block.ms": 60000})
    try:
        # With the topic's metadata cached, a record to partition 0 is taken
        # at once (the test above).
        warm_up = producer.send(record=_record(topic, 0, b"warm-up"))
        assert warm_up.get(timeout=30).offset() == 0
        tried.clear()
        firsts: list[KafkaFuture[RecordMetadata]] = []
        waiting = threading.Thread(
            target=lambda: firsts.append(producer.send(record=_record(topic, 1, b"a"))))
        waiting.start()
        assert tried.wait(30)  # the first record is queued, its send() waiting
        second = producer.send(record=_record(topic, 0, b"b"))
        # The second was taken inside send(); the first is not handed over yet.
        assert handovers == ["queued", "inline"]
        assert second.get(timeout=30).offset() == 1
        assert _records_of(kafka_broker, topic, 0, from_offset=1, window=3.0) == [(1, b"b")]
        assert waiting.is_alive(), "the first send() still waits for its partition"
        _add_partitions(kafka_broker, topic, 2)
        waiting.join(60)
        assert not waiting.is_alive()
        (first,) = firsts
        assert first.get(timeout=60).partition() == 1
        assert handovers == ["queued", "inline", "queued"]
    finally:
        producer.close()
