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

"""Transactions against a broker, sync and asyncio: the exactly-once
consume-transform-produce round trip (``initTransactions``,
``beginTransaction``, ``send``, ``sendOffsetsToTransaction`` with the
consumer's ``groupMetadata()``, ``commitTransaction``), and an aborted
transaction, whose records a ``read_committed`` consumer never returns while a
``read_uncommitted`` one does. Skips without Docker (``kafka_broker``)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from confluent_kafka.common import TopicPartition
from confluent_kafka.common.serialization import string_deserializer, string_serializer
from confluent_kafka.consumer import OffsetAndMetadata
from confluent_kafka.producer import ProducerRecord

from .broker_helpers import (
    DEADLINE, MODE, aw, close_consumer, consumer_configs, make_consumer, make_producer,
    new_topic, poll_records, produce, producer_configs, read_all, run, unique,
)

STR_IN: dict[str, Any] = {"value_deserializer": string_deserializer()}
STR_OUT: dict[str, Any] = {"key_serializer": string_serializer(), "value_serializer": string_serializer()}


@MODE
def test_exactly_once_round_trip(kafka_broker: Any, mode: str) -> None:
    source = new_topic(kafka_broker, "py-eos-in")
    sink = new_topic(kafka_broker, "py-eos-out")
    produce(kafka_broker, source, ["a", "b", "c"], value_serializer=string_serializer())
    tp = TopicPartition(topic=source, partition=0)
    group = consumer_configs(kafka_broker, isolation_level="read_committed")

    async def body() -> None:
        consumer = make_consumer(mode, group, **STR_IN)
        producer = make_producer(mode, producer_configs(
            kafka_broker, transactional_id=unique("py-eos-txn")), **STR_OUT)
        try:
            await aw(consumer.subscribe(topics=[source]))
            records = await poll_records(consumer, lambda rs: len(rs) >= 3)
            assert [r.value() for r in records] == ["a", "b", "c"]
            await aw(producer.init_transactions())
            await aw(producer.begin_transaction())
            for record in records:
                await aw(producer.send(record=ProducerRecord(
                    topic=sink, partition=0, key=record.value(),
                    value=str(record.value()).upper())))
            await aw(producer.send_offsets_to_transaction(
                offsets={tp: OffsetAndMetadata(offset=records[-1].offset() + 1)},
                group_metadata=consumer.group_metadata()))
            await aw(producer.commit_transaction())
            # The offsets are the group's once the transaction committed.
            end = time.monotonic() + DEADLINE
            while True:
                committed = (await aw(consumer.committed(partitions=[tp], timeout=10)))[tp]
                if committed is not None and committed.offset() == 3:
                    break
                assert time.monotonic() < end, f"committed offset is {committed}"
                await asyncio.sleep(0.2)
        finally:
            await aw(producer.close())
            await close_consumer(consumer)

    run(body)
    out = read_all(kafka_broker, sink, 1, 3, isolation_level="read_committed", **STR_IN)
    assert [r.value() for r in out] == ["A", "B", "C"]


@MODE
def test_aborted_transaction_is_hidden_from_read_committed(kafka_broker: Any,
                                                           mode: str) -> None:
    topic = new_topic(kafka_broker, "py-abort")

    async def body() -> list[int]:
        producer = make_producer(mode, producer_configs(
            kafka_broker, transactional_id=unique("py-abort-txn")), **STR_OUT)
        try:
            await aw(producer.init_transactions())
            await aw(producer.begin_transaction())
            futures = [await aw(producer.send(record=ProducerRecord(
                topic=topic, partition=0, key=None, value=v))) for v in ("x1", "x2")]
            # The records reach the log before the abort marks them aborted.
            await aw(producer.flush())
            offsets = [(await aw(f.get(timeout=30))).offset() for f in futures]
            await aw(producer.abort_transaction())
            await aw(producer.begin_transaction())
            await aw(producer.send(record=ProducerRecord(topic=topic, partition=0, key=None,
                                                         value="kept")))
            await aw(producer.commit_transaction())
            return offsets
        finally:
            await aw(producer.close())

    assert run(body) == [0, 1]
    # Offset 2 is the abort marker, 3 the committed record, 4 its marker.
    uncommitted = read_all(kafka_broker, topic, 1, 3, **STR_IN)
    assert [(r.offset(), r.value()) for r in uncommitted] == [(0, "x1"), (1, "x2"), (3, "kept")]
    committed = read_all(kafka_broker, topic, 1, 1, isolation_level="read_committed",
                         window=1.0, **STR_IN)
    assert [(r.offset(), r.value()) for r in committed] == [(3, "kept")]
