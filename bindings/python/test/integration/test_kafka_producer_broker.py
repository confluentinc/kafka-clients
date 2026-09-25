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
and an open transaction, which ``begin_transaction()`` without a drain needs.
Skips without Docker (``kafka_broker``)."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.common.serialization import string_serializer
from confluent_kafka.producer import AsyncKafkaProducer, KafkaProducer, ProducerRecord

from .conftest import create_topic


def _topic(broker: Any) -> str:
    topic = f"py-producer-{uuid.uuid4().hex[:12]}"
    create_topic(broker, topic)
    return topic


def _configs(broker: Any) -> dict[str, Any]:
    return {"bootstrap.servers": broker.external_bootstrap,
            "transactional.id": f"py-txn-{uuid.uuid4().hex[:12]}"}


def test_send_before_init_transactions_raises(kafka_broker: Any) -> None:
    # TransactionManager.maybeAddPartition's IllegalStateException
    # (TransactionManager.java:443), rethrown by doSend's catch (Exception e):
    # send() raises it, the callback does not run.
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
    finally:
        producer.close(timeout=0)
    assert calls == []


def test_send_outside_a_transaction_raises(kafka_broker: Any) -> None:
    # After initTransactions and before beginTransaction the transaction
    # manager is READY (TransactionManager.java:446, with Java's double space).
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

        # Inside a transaction the same send goes through, and its callback runs.
        producer.begin_transaction()
        future = producer.send(record=ProducerRecord(topic=topic, key="key", value="value"),
                               callback=lambda metadata, exception: calls.append(exception))
        producer.commit_transaction()
        assert future.result(timeout=30).offset() >= 0
        assert calls == [None]
    finally:
        producer.close(timeout=0)


def test_async_send_outside_a_transaction_raises(kafka_broker: Any) -> None:
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

            producer.begin_transaction()
            future = await producer.send(
                record=ProducerRecord(topic=topic, key="key", value="value"),
                callback=lambda metadata, exception: calls.append(exception))
            await producer.commit_transaction()
            assert (await asyncio.wait_for(future, 30)).offset() >= 0
            assert calls == [None]
        finally:
            await producer.close(timeout=0)

    asyncio.run(main())


def _invalid_begin(transactional_id: str) -> str:
    return (f"TransactionalId {transactional_id}: Invalid transition attempted from state "
            "IN_TRANSACTION to state IN_TRANSACTION")


def test_begin_transaction_does_not_wait_and_an_open_transaction_keeps_its_records(
        kafka_broker: Any) -> None:
    # Critic 75 N1: Java's beginTransaction does not wait. A record sent in the
    # open transaction and still in the batching engine (its handover held)
    # stays in that transaction: a second begin_transaction() fails at once
    # with the transaction manager's invalid transition, and the
    # commit_transaction() after it drains and commits the record
    # (producer-transactions.md §13).
    topic = _topic(kafka_broker)
    configs = _configs(kafka_broker)
    producer = KafkaProducer(configs=configs, key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    try:
        producer.init_transactions()
        producer.begin_transaction()
        _lib.Producer_test_set_paused(producer._c_producer, True)  # noqa: SLF001
        future = producer.send(record=ProducerRecord(topic=topic, key="key", value="value"))
        start = time.monotonic()
        with pytest.raises(IllegalStateError) as err:
            producer.begin_transaction()
        assert time.monotonic() - start < 1
        assert str(err.value) == _invalid_begin(configs["transactional.id"])
        assert not future.done()
        _lib.Producer_test_set_paused(producer._c_producer, False)  # noqa: SLF001
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
            producer.begin_transaction()
            _lib.Producer_test_set_paused(producer._c_producer, True)  # noqa: SLF001
            future = await producer.send(
                record=ProducerRecord(topic=topic, key="key", value="value"))
            start = time.monotonic()
            with pytest.raises(IllegalStateError) as err:
                producer.begin_transaction()
            assert time.monotonic() - start < 1
            assert str(err.value) == _invalid_begin(configs["transactional.id"])
            assert not future.done()
            _lib.Producer_test_set_paused(producer._c_producer, False)  # noqa: SLF001
            await producer.commit_transaction()
            assert (await asyncio.wait_for(future, 30)).offset() >= 0
        finally:
            await producer.close(timeout=0)

    asyncio.run(main())
