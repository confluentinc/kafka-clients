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

"""The consumer's timed overloads against a broker, sync and asyncio:
``poll(timeout)``, ``commit(offsets, timeout)``, ``position(partition,
timeout)``, ``committed(partitions, timeout)``, ``beginning_offsets`` /
``end_offsets(partitions, timeout)`` and ``offsets_for_times(timestamps,
timeout)``, ``partitions_for`` / ``list_topics(timeout)`` and ``commit(timeout)``
without offsets return the broker's values; a zero timeout on a fresh consumer
answers as ``org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer``
does: ``TimeoutError`` with Java's message from ``position``
(``#position(TopicPartition,Duration)``), ``committed``
(``#committed(Set,Duration)``), ``partitions_for``
(``#partitionsFor(String,Duration)``) and ``list_topics``
(``#listTopics(Duration)``), the partitions mapped to ``None`` from
``offsets_for_times`` (``#offsetsForTimes(Map,Duration)``) and an empty map
from ``beginning_offsets`` / ``end_offsets`` (``#beginningOrEndOffset``). Skips
without Docker (``kafka_broker``)."""

from __future__ import annotations

import time
from typing import Any

import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.common.errors import TimeoutError
from confluent_kafka.common.serialization import string_deserializer, string_serializer
from confluent_kafka.consumer import (
    CloseOptions, KafkaConsumer, OffsetAndMetadata, OffsetAndTimestamp,
)
from confluent_kafka.producer import KafkaProducer, ProducerRecord

from .broker_helpers import (
    MODE, aw, close_consumer, consumer_configs, make_consumer, new_topic, poll_records,
    producer_configs, run,
)

STR_IN: dict[str, Any] = {"value_deserializer": string_deserializer()}

#: ``(partition, timestamp, value)`` of the records, in send order, the
#: timestamps in ms after :data:`BASE`.
RECORDS = [(0, 1000, "a"), (0, 2000, "b"), (0, 3000, "c"), (1, 1500, "x"), (1, 2500, "y")]

#: A minute ago: recent, or the broker's retention (7 days) would delete the
#: records during the test, moving the log start offset to the end.
BASE = int(time.time() * 1000) - 60_000


def _timestamped_topic(broker: Any) -> str:
    topic = new_topic(broker, "py-timed", partitions=2)
    producer: KafkaProducer[Any, str] = KafkaProducer(
        configs=producer_configs(broker), value_serializer=string_serializer())
    try:
        for partition, timestamp, value in RECORDS:
            producer.send(record=ProducerRecord(topic=topic, partition=partition,
                                                timestamp=BASE + timestamp, key=None,
                                                value=value))
        producer.flush()
    finally:
        producer.close()
    return topic


@MODE
def test_timed_overloads_return_the_brokers_values(kafka_broker: Any, mode: str) -> None:
    topic = _timestamped_topic(kafka_broker)
    tp0, tp1 = (TopicPartition(topic=topic, partition=p) for p in (0, 1))

    async def body() -> None:
        consumer = make_consumer(mode, consumer_configs(kafka_broker), **STR_IN)
        try:
            await aw(consumer.assign(partitions=[tp0, tp1]))
            records = await poll_records(consumer, lambda rs: len(rs) >= len(RECORDS))
            assert sorted((r.partition(), r.timestamp() - BASE, r.value()) for r in records) \
                == sorted(RECORDS)
            assert await aw(consumer.position(partition=tp0, timeout=10)) == 3
            assert await aw(consumer.position(partition=tp1, timeout=10)) == 2
            await aw(consumer.commit(offsets={tp0: OffsetAndMetadata(offset=2, metadata="m")},
                                     timeout=10))
            assert await aw(consumer.committed(partitions=[tp0, tp1], timeout=10)) == {
                tp0: OffsetAndMetadata(offset=2, metadata="m"), tp1: None}
            assert await aw(consumer.beginning_offsets(partitions=[tp0, tp1], timeout=10)) \
                == {tp0: 0, tp1: 0}
            assert await aw(consumer.end_offsets(partitions=[tp0, tp1], timeout=10)) == {
                tp0: 3, tp1: 2}
            found = await aw(consumer.offsets_for_times(
                timestamps_to_search={tp0: BASE + 1500, tp1: BASE + 1500}, timeout=10))
            assert found == {
                tp0: OffsetAndTimestamp(offset=1, timestamp=BASE + 2000, leader_epoch=0),
                tp1: OffsetAndTimestamp(offset=0, timestamp=BASE + 1500, leader_epoch=0)}
            # Past the last timestamp: no such message.
            assert await aw(consumer.offsets_for_times(
                timestamps_to_search={tp0: BASE + 1500, tp1: BASE + 9999}, timeout=10)) == {
                tp0: OffsetAndTimestamp(offset=1, timestamp=BASE + 2000, leader_epoch=0),
                tp1: None}
            # A timed poll with nothing left returns empty.
            assert (await aw(consumer.poll(timeout=0.5))).is_empty()
            # The metadata overloads.
            partitions = await aw(consumer.partitions_for(topic=topic, timeout=10))
            assert sorted(p.partition() for p in partitions) == [0, 1]
            assert topic in await aw(consumer.list_topics(timeout=10))
            # commit(timeout) without offsets commits the consumed positions.
            await aw(consumer.commit(timeout=10))
            assert {tp: o.offset() if o is not None else None for tp, o in (await aw(
                consumer.committed(partitions=[tp0, tp1], timeout=10))).items()} == {
                tp0: 3, tp1: 2}
        finally:
            await close_consumer(consumer)
    run(body)


@MODE
def test_zero_timeouts_on_a_fresh_consumer(kafka_broker: Any, mode: str) -> None:
    topic = _timestamped_topic(kafka_broker)
    tp = TopicPartition(topic=topic, partition=0)

    async def body() -> None:
        consumer = make_consumer(mode, consumer_configs(kafka_broker), **STR_IN)
        try:
            await aw(consumer.assign(partitions=[tp]))
            with pytest.raises(TimeoutError) as position_error:
                await aw(consumer.position(partition=tp, timeout=0))
            assert str(position_error.value) == (
                f"Timeout of 0ms expired before the position for partition {topic}-0 could "
                "be determined")
            with pytest.raises(TimeoutError) as committed_error:
                await aw(consumer.committed(partitions=[tp], timeout=0))
            assert str(committed_error.value) == (
                f"Timeout of 0ms expired before the last committed offset for partitions "
                f"[{topic}-0] could be determined. Try tuning default.api.timeout.ms larger "
                "to relax the threshold.")
            assert await aw(consumer.offsets_for_times(timestamps_to_search={tp: 0},
                                                       timeout=0)) == {tp: None}
            assert await aw(consumer.beginning_offsets(partitions=[tp], timeout=0)) == {}
            assert await aw(consumer.end_offsets(partitions=[tp], timeout=0)) == {}
        finally:
            await close_consumer(consumer)

    run(body)


@pytest.mark.xfail(strict=True, raises=AssertionError, reason=(
    "recorded core deviation, for the owner to decide: list_topics(timeout=0) raises "
    "TimeoutError(\"Timeout of 0ms expired before all topics' metadata could be "
    "listed\"), where Java's AsyncKafkaConsumer.listTopics throws new TimeoutException() "
    "without a message, which the Errors rule renders "
    "as str(e) == ''; the core adds the text on purpose, citing the RECORDED DEVIATION of "
    "rust/src/consumer/internals/async_kafka_consumer.rs:6241-6255 (repeated at "
    ":6289-6297), on the ground that Error cannot carry Java's null message -- but "
    "84e3a6e2 gives the message-less WakeupError an empty message, as Error::timeout('') "
    "could here"))
@MODE
def test_zero_timeout_list_topics(kafka_broker: Any, mode: str) -> None:
    # org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#listTopics(Duration):
    # TimeoutException() without a message.
    async def body() -> None:
        consumer = make_consumer(mode, consumer_configs(kafka_broker))
        try:
            with pytest.raises(TimeoutError) as error:
                await aw(consumer.list_topics(timeout=0))
            assert str(error.value) == ""
        finally:
            await close_consumer(consumer)

    run(body)


@pytest.mark.xfail(strict=True, raises=AssertionError, reason=(
    "recorded core deviation, for the owner to decide: partitions_for(timeout=0) of an "
    "uncached topic raises TimeoutError('Timeout of 0ms expired before partitions for "
    "topic <t> could be determined'), where Java's AsyncKafkaConsumer.partitionsFor throws "
    "new TimeoutException() without a message; the "
    "core adds the text on purpose, its RECORDED DEVIATION at "
    "rust/src/consumer/internals/async_kafka_consumer.rs:6241-6255, on the ground that "
    "Error cannot carry Java's null message -- but 84e3a6e2 gives the message-less "
    "WakeupError an empty message, as Error::timeout('') could here"))
@MODE
def test_zero_timeout_partitions_for(kafka_broker: Any, mode: str) -> None:
    topic = new_topic(kafka_broker, "py-timed-meta")

    async def body() -> None:
        consumer = make_consumer(mode, consumer_configs(kafka_broker))
        try:
            with pytest.raises(TimeoutError) as error:
                await aw(consumer.partitions_for(topic=topic, timeout=0))
            assert str(error.value) == ""
        finally:
            await close_consumer(consumer)

    run(body)


def test_offset_lookups_answer_within_a_round_trip(kafka_broker: Any) -> None:
    """A ListOffsets-backed lookup answers within a round trip, as Java's, which
    completes it inside client.poll
    (org.apache.kafka.clients.consumer.internals.OffsetsRequestManager#buildListOffsetRequestToNode): the core
    wakes its background loop when the response is queued (#264), so a 2 s
    timeout is ample after the first lookup. One mode only: the sync and asyncio
    consumers reach the same core path."""
    topic = _timestamped_topic(kafka_broker)
    tps = [TopicPartition(topic=topic, partition=p) for p in (0, 1)]
    consumer: KafkaConsumer[Any, Any] = KafkaConsumer(configs=consumer_configs(kafka_broker))
    try:
        # The first lookup with room for the metadata, then two that Java
        # answers in milliseconds.
        assert consumer.end_offsets(partitions=tps, timeout=30) == {tps[0]: 3, tps[1]: 2}
        for _ in range(2):
            assert consumer.end_offsets(partitions=tps, timeout=2) == {tps[0]: 3, tps[1]: 2}
    finally:
        consumer.close(option=CloseOptions(timeout=0))
