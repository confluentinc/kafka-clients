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

"""An object holding a native core object refuses ``copy``, ``deepcopy`` and
``pickle`` with ``TypeError``: a copy would share the native object, which the
original's ``close()`` frees under it (CLAUDE.md, Python Binding Conventions,
Types, the ``java.io.Serializable`` row). The mocks, and the futures, Results
and records built in Python, keep Python's default."""

from __future__ import annotations

import asyncio
import copy
import pickle
from collections.abc import Callable
from typing import Any

import pytest

from confluent_kafka.admin import (
    Admin,
    AsyncAdmin,
    ListConfigResourcesOptions,
    MockAdminClient,
    NewTopic,
)
from confluent_kafka.common import KafkaFuture, Node, TopicPartition
from confluent_kafka.common.kafka_future import AsyncKafkaFuture
from confluent_kafka.consumer import (
    AsyncKafkaConsumer,
    CloseOptions,
    ConsumerRecord,
    ConsumerRecords,
    KafkaConsumer,
    MockConsumer,
)
from confluent_kafka.consumer import _poll
from confluent_kafka.producer import (
    AsyncKafkaProducer,
    KafkaProducer,
    MockProducer,
    ProducerRecord,
)

from .test_admin_none_arguments import RPCS

_COPIES: list[Callable[[Any], object]] = [copy.copy, copy.deepcopy, pickle.dumps]
_CONSUMER_CONFIGS = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer"}
_CONFIGS = {"bootstrap.servers": "localhost:1"}


def _refused(value: object, message: str) -> None:
    for copier in _COPIES:
        with pytest.raises(TypeError) as e:
            copier(value)
        assert str(e.value) == message, copier


def _client_message(name: str) -> str:
    return f"{name} cannot be copied or pickled; it holds the client's native handle"


# --------------------------------------------------------------------------- #
# The six FFI-backed clients
# --------------------------------------------------------------------------- #


def test_kafka_producer_refuses_copy_and_stays_usable() -> None:
    producer: KafkaProducer[bytes, bytes] = KafkaProducer(configs=_CONFIGS)
    _refused(producer, _client_message("KafkaProducer"))
    assert isinstance(producer.metrics(), dict)
    producer.close(timeout=0)


def test_kafka_consumer_refuses_copy_and_stays_usable() -> None:
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=_CONSUMER_CONFIGS)
    _refused(consumer, _client_message("KafkaConsumer"))
    assert consumer.assignment() == set()
    consumer.close(option=CloseOptions.timeout(timeout=0))


def test_kafka_admin_client_refuses_copy_and_stays_usable() -> None:
    admin = Admin.create(configs=_CONFIGS)
    _refused(admin, _client_message("KafkaAdminClient"))
    admin.list_topics()
    admin.close(timeout=0)


def test_async_kafka_producer_refuses_copy_and_stays_usable() -> None:
    async def main() -> None:
        producer: AsyncKafkaProducer[bytes, bytes] = AsyncKafkaProducer(configs=_CONFIGS)
        _refused(producer, _client_message("AsyncKafkaProducer"))
        assert isinstance(producer.metrics(), dict)
        await producer.close(timeout=0)

    asyncio.run(main())


def test_async_kafka_consumer_refuses_copy_and_stays_usable() -> None:
    async def main() -> None:
        consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(
            configs=_CONSUMER_CONFIGS)
        _refused(consumer, _client_message("AsyncKafkaConsumer"))
        assert consumer.assignment() == set()
        await consumer.close(option=CloseOptions.timeout(timeout=0))

    asyncio.run(main())


def test_async_kafka_admin_client_refuses_copy_and_stays_usable() -> None:
    async def main() -> None:
        admin = AsyncAdmin.create(configs=_CONFIGS)
        _refused(admin, _client_message("AsyncKafkaAdminClient"))
        admin.list_topics()
        await admin.close(timeout=0)

    asyncio.run(main())


def test_mocks_keep_pythons_default_copy() -> None:
    for mock in (MockProducer(), MockConsumer(offset_reset_strategy="earliest"),
                 MockAdminClient(brokers=[Node(id=0, host="localhost", port=9092)], controller=Node(
                     id=0, host="localhost", port=9092))):
        assert type(copy.copy(mock)) is type(mock)


# --------------------------------------------------------------------------- #
# Futures and admin Results over a C handle
# --------------------------------------------------------------------------- #

_FUTURE_MESSAGE = "{} cannot be copied or pickled; it holds the client's native future"
_RESULT_MESSAGE = "{} cannot be copied or pickled; it holds the admin client's native result"


def test_a_real_admin_clients_results_and_futures_refuse_copy() -> None:
    admin = Admin.create(configs=_CONFIGS)
    try:
        result = admin.create_topics(new_topics=[NewTopic(name="t", num_partitions=1,
                                                          replication_factor=1)])
        _refused(result, _RESULT_MESSAGE.format("CreateTopicsResult"))
        _refused(admin.list_topics(), _RESULT_MESSAGE.format("ListTopicsResult"))
        cluster = admin.describe_cluster()
        _refused(cluster, _RESULT_MESSAGE.format("DescribeClusterResult"))
        _refused(cluster.cluster_id(), _FUTURE_MESSAGE.format("KafkaFuture"))
        # A future composed in Python over a C future is Python state: it copies.
        assert isinstance(copy.copy(result.values()["t"]), KafkaFuture)
        assert isinstance(copy.copy(result.num_partitions(topic="t")), KafkaFuture)
    finally:
        admin.close(timeout=0)


def test_an_async_admin_clients_results_and_futures_refuse_copy() -> None:
    async def main() -> None:
        admin = AsyncAdmin.create(configs=_CONFIGS)
        result = admin.create_topics(new_topics=[NewTopic(name="t", num_partitions=1,
                                                          replication_factor=1)])
        _refused(result, _RESULT_MESSAGE.format("CreateTopicsResult"))
        _refused(admin.describe_cluster().cluster_id(), _FUTURE_MESSAGE.format("AsyncKafkaFuture"))
        await admin.close(timeout=0)

    asyncio.run(main())


def _call(admin: Any, form: str) -> Any:
    """The Result of one RPC call form (``RPCS``: every RPC, ``#`` a second form)."""
    kwargs = dict(RPCS[form][0])
    if form == "list_config_resources":
        # Java's only overload with the types takes the options too.
        kwargs["options"] = ListConfigResourcesOptions()
    return getattr(admin, form.split("#")[0])(**kwargs)


@pytest.mark.parametrize("form", sorted(RPCS))
def test_every_result_a_real_admin_client_builds_refuses_copy(form: str) -> None:
    """Every Result the client builds over its C result refuses, whether it holds
    the C result in its futures, in futures composed over them or in the closures
    of its accessors."""
    admin = Admin.create(configs=_CONFIGS)
    try:
        result = _call(admin, form)
        _refused(result, _RESULT_MESSAGE.format(type(result).__name__))
    finally:
        admin.close(timeout=0)


@pytest.mark.parametrize("form", sorted(RPCS))
def test_every_result_an_async_admin_client_builds_refuses_copy(form: str) -> None:
    async def main() -> None:
        admin = AsyncAdmin.create(configs=_CONFIGS)
        try:
            result = _call(admin, form)
            _refused(result, _RESULT_MESSAGE.format(type(result).__name__))
        finally:
            await admin.close(timeout=0)

    asyncio.run(main())


@pytest.mark.parametrize("form", sorted(RPCS))
def test_a_result_the_mock_builds_keeps_pythons_default_copy(form: str) -> None:
    mock = MockAdminClient(brokers=[Node(id=0, host="localhost", port=9092)],
                           controller=Node(id=0, host="localhost", port=9092))
    try:
        result = _call(mock, form)
    except Exception:  # noqa: BLE001 - a call Java's mock does not support, or refuses
        pytest.skip("the mock does not build this Result")
    assert type(copy.copy(result)) is type(result)


def test_the_async_producers_record_future_refuses_copy() -> None:
    async def main() -> None:
        producer: AsyncKafkaProducer[bytes, bytes] = AsyncKafkaProducer(
            configs={**_CONFIGS, "max.block.ms": 0})
        future = await producer.send(record=ProducerRecord(topic="t", value=b"v"))
        assert isinstance(future, AsyncKafkaFuture)
        _refused(future, "AsyncKafkaFuture cannot be copied or pickled; it is the record the "
                         "producer's send completes")
        await producer.close(timeout=0)

    asyncio.run(main())


def test_futures_and_results_built_in_python_keep_pythons_default() -> None:
    done = KafkaFuture.completed_future(value=1)
    assert copy.copy(done).get() == 1
    mock = MockAdminClient(brokers=[Node(id=0, host="localhost", port=9092)],
                           controller=Node(id=0, host="localhost", port=9092))
    result = mock.create_topics(new_topics=[NewTopic(name="t", num_partitions=1,
                                                     replication_factor=1)])
    assert type(copy.copy(result)) is type(result)
    record_future = MockProducer().send(record=ProducerRecord(topic="t", value=b"v"))
    assert type(copy.copy(record_future)) is type(record_future)


# --------------------------------------------------------------------------- #
# ConsumerRecords
# --------------------------------------------------------------------------- #


class _NativeRecords:
    """Stands in for the extension's ConsumerRecords of a fetch."""

    def next_offsets(self) -> dict[tuple[str, int], tuple[int, str, int]]:
        return {("t", 0): (6, "", -1)}


def test_the_records_a_real_poll_returns_refuse_copy() -> None:
    tp = TopicPartition(topic="t", partition=0)
    record: ConsumerRecord[bytes, bytes] = ConsumerRecord(topic="t", partition=0, offset=5,
                                                          key=None, value=b"v")
    fetched = _poll._build(_NativeRecords(), {tp: [record]}, {})
    _refused(fetched, "ConsumerRecords cannot be copied or pickled; it holds the consumer's "
                      "fetched records")
    built = ConsumerRecords(records={tp: [record]}, next_offsets={})
    assert len(copy.copy(built)) == 1
    assert len(pickle.loads(pickle.dumps(built))) == 1
    assert copy.copy(ConsumerRecords.empty()).is_empty()
