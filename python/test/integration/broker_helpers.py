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

"""Helpers shared by the broker-backed scenario tests: unique topics and
groups, client configs, and the two client forms one test body drives.

A scenario written once runs on the sync clients (``KafkaProducer`` /
``KafkaConsumer``) and on the asyncio ones (``AsyncKafkaProducer`` /
``AsyncKafkaConsumer``): the body is an ``async def`` that wraps every client
call in :func:`aw`, which awaits the result of an asyncio client and returns
the sync client's as it is. The sync clients then run on the event loop's
thread, which blocks while they wait; nothing else runs on that loop.
"""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.consumer import (
    AsyncKafkaConsumer, CloseOptions, ConsumerRecord, KafkaConsumer,
)
from confluent_kafka.producer import AsyncKafkaProducer, KafkaProducer, ProducerRecord

from .conftest import KAFKA_BIN

#: Every wait of these tests is bounded by this many seconds.
DEADLINE = 45.0

#: The two forms of every scenario.
MODES = ("sync", "async")

#: ``@MODE`` parametrises a test over :data:`MODES`.
MODE = pytest.mark.parametrize("mode", MODES)

_T = TypeVar("_T")

Consumer = KafkaConsumer[Any, Any] | AsyncKafkaConsumer[Any, Any]
Producer = KafkaProducer[Any, Any] | AsyncKafkaProducer[Any, Any]


async def aw(value: Any) -> Any:
    """``await value`` for an asyncio client's call, ``value`` itself for a
    sync client's (typed ``Any``: the two forms return different types, a
    ``KafkaFuture`` or an ``AsyncKafkaFuture`` say)."""
    if inspect.isawaitable(value):
        return await value
    return value


def run(body: Callable[[], Coroutine[Any, Any, _T]]) -> _T:
    """Run a scenario's ``async def`` body on a fresh event loop."""
    return asyncio.run(body())


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def new_topic(broker: Any, prefix: str, partitions: int = 1,
              configs: dict[str, str] | None = None) -> str:
    """A new topic of ``partitions`` partitions and the topic ``configs``,
    created with kafka-topics.sh inside the broker container."""
    topic = unique(prefix)
    cmd = [f"{KAFKA_BIN}/kafka-topics.sh", "--bootstrap-server", broker.internal_bootstrap,
           "--create", "--topic", topic, "--partitions", str(partitions),
           "--replication-factor", "1"]
    for key, value in (configs or {}).items():
        cmd += ["--config", f"{key}={value}"]
    rc, out = broker.exec(cmd)
    if rc not in (0, None):
        raise RuntimeError(f"creating {topic} failed (rc={rc}): {out!r}")
    return topic


def consumer_configs(broker: Any, **extra: Any) -> dict[str, Any]:
    """A KIP-848 consumer of a new group, reading from the earliest offset,
    without auto-commit; ``extra`` keys use ``_`` for ``.``."""
    configs: dict[str, Any] = {
        "bootstrap.servers": broker.external_bootstrap, "group.protocol": "consumer",
        "group.id": unique("py-group"), "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }
    configs.update({k.replace("_", "."): v for k, v in extra.items()})
    return configs


def producer_configs(broker: Any, **extra: Any) -> dict[str, Any]:
    """A producer of the broker; ``extra`` keys use ``_`` for ``.``."""
    configs: dict[str, Any] = {"bootstrap.servers": broker.external_bootstrap}
    configs.update({k.replace("_", "."): v for k, v in extra.items()})
    return configs


def make_consumer(mode: str, configs: dict[str, Any], **serdes: Any) -> Consumer:
    if mode == "sync":
        return KafkaConsumer(configs=configs, **serdes)
    return AsyncKafkaConsumer(configs=configs, **serdes)


def make_producer(mode: str, configs: dict[str, Any], **serdes: Any) -> Producer:
    if mode == "sync":
        return KafkaProducer(configs=configs, **serdes)
    return AsyncKafkaProducer(configs=configs, **serdes)


async def close_consumer(consumer: Consumer, timeout: float = 0,
                         operation: CloseOptions.GroupMembershipOperation | None = None
                         ) -> None:
    """Close ``consumer`` with ``timeout`` and, when given, ``operation``."""
    if operation is None:
        option = CloseOptions(timeout=timeout)
    else:
        option = CloseOptions(timeout=timeout, operation=operation)
    await aw(consumer.close(option=option))


async def poll_records(consumer: Consumer, done: Callable[[list[ConsumerRecord[Any, Any]]], bool],
                       deadline: float = DEADLINE, timeout: float = 0.5
                       ) -> list[ConsumerRecord[Any, Any]]:
    """Poll until ``done(records)`` holds for the records polled so far."""
    records: list[ConsumerRecord[Any, Any]] = []
    end = time.monotonic() + deadline
    while not done(records):
        assert time.monotonic() < end, f"timed out polling: {len(records)} records"
        records.extend(await aw(consumer.poll(timeout=timeout)))
    return records


async def poll_values(consumer: Consumer, count: int, deadline: float = DEADLINE
                      ) -> list[Any]:
    """The values of the first ``count`` records polled."""
    return [r.value() for r in await poll_records(consumer, lambda rs: len(rs) >= count,
                                                   deadline)]


def produce(broker: Any, topic: str, values: list[Any], partition: int | None = 0,
            **serdes: Any) -> None:
    """Send ``values`` to ``topic``-``partition`` with a sync producer and flush."""
    producer: KafkaProducer[Any, Any] = KafkaProducer(configs=producer_configs(broker), **serdes)
    try:
        for value in values:
            producer.send(record=ProducerRecord(topic=topic, partition=partition, key=None,
                                                value=value))
        producer.flush()
    finally:
        producer.close()


def read_all(broker: Any, topic: str, partitions: int, count: int,
             isolation_level: str = "read_uncommitted", window: float = 0.0,
             **serdes: Any) -> list[ConsumerRecord[Any, Any]]:
    """The records of ``topic`` from offset 0, read with an assigned sync
    consumer: at least ``count`` of them, plus those arriving ``window``
    seconds after that."""
    consumer: KafkaConsumer[Any, Any] = KafkaConsumer(
        configs=consumer_configs(broker, isolation_level=isolation_level), **serdes)
    try:
        tps = [TopicPartition(topic=topic, partition=p) for p in range(partitions)]
        consumer.assign(partitions=tps)
        consumer.seek_to_beginning(partitions=tps)
        records: list[ConsumerRecord[Any, Any]] = []
        end = time.monotonic() + DEADLINE
        while len(records) < count:
            assert time.monotonic() < end, f"read {len(records)} of {count} records"
            records.extend(consumer.poll(timeout=0.5))
        end = time.monotonic() + window
        while time.monotonic() < end:
            records.extend(consumer.poll(timeout=0.2))
        return records
    finally:
        consumer.close(option=CloseOptions(timeout=0))
