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

"""A ``str``, ``bytes`` or ``bytearray`` given for a parameter whose Java type is a
collection of ``String`` raises ``TypeError`` (CLAUDE.md, Python Binding Conventions,
Signatures). Java cannot receive one there; Python would iterate it by character, so
``subscribe(topics="orders")`` would subscribe to "o", "r", "d", "e", "s" and
``delete_topics(topics="orders")`` would delete them. The check comes with the
keyword and form checks, before the body's own, so it wins over a closed client."""

from __future__ import annotations

from typing import Any

import pytest

from confluent_kafka.admin import (
    Admin,
    AsyncAdmin,
    AsyncMockAdminClient,
    ListGroupsOptions,
    MockAdminClient,
)
from confluent_kafka.common import TopicCollection
from confluent_kafka.common.errors import InvalidTopicError, TopicAuthorizationError
from confluent_kafka.consumer import (
    AsyncKafkaConsumer,
    AsyncMockConsumer,
    ConsumerRebalanceListener,
    KafkaConsumer,
    MockConsumer,
)

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CONSUMER = {"bootstrap.servers": "localhost:1", "group.protocol": "consumer", "group.id": "g"}
STRINGS: list[Any] = ["orders", b"orders", bytearray(b"orders")]

# (method, parameter) for every admin method taking a Collection<String>.
ADMIN_CALLS = [
    ("delete_topics", "topics"),
    ("describe_topics", "topic_names"),
    ("describe_consumer_groups", "group_ids"),
    ("delete_consumer_groups", "group_ids"),
    ("describe_classic_groups", "group_ids"),
    ("describe_transactions", "transactional_ids"),
    ("fence_producers", "transactional_ids"),
    ("describe_user_scram_credentials", "users"),
]


def message(name: str, value: Any) -> str:
    return f"{name} must be an iterable of str, not {type(value).__name__}"


def rejects(call: Any, name: str, value: Any) -> None:
    with pytest.raises(TypeError) as info:
        call(**{name: value})
    assert str(info.value) == message(name, value)


# ---------------------------------------------------------------------------
# Consumers: subscribe(topics)


@pytest.mark.parametrize("value", STRINGS)
def test_kafka_consumer_subscribe_rejects_str(value: Any) -> None:
    with KafkaConsumer(configs=CONSUMER) as consumer:
        rejects(consumer.subscribe, "topics", value)
        with pytest.raises(TypeError):
            consumer.subscribe(topics=value, callback=ConsumerRebalanceListener())


def test_kafka_consumer_subscribe_rejects_str_when_closed() -> None:
    consumer: KafkaConsumer[bytes, bytes] = KafkaConsumer(configs=CONSUMER)
    consumer.close()
    rejects(consumer.subscribe, "topics", "orders")


@pytest.mark.parametrize("value", STRINGS)
async def test_async_kafka_consumer_subscribe_rejects_str(value: Any) -> None:
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=CONSUMER)
    try:
        with pytest.raises(TypeError) as info:
            await consumer.subscribe(topics=value)
        assert str(info.value) == message("topics", value)
    finally:
        await consumer.close()
    with pytest.raises(TypeError) as info:  # before the closed check too
        await consumer.subscribe(topics=value)
    assert str(info.value) == message("topics", value)


@pytest.mark.parametrize("value", STRINGS)
def test_mock_consumer_subscribe_rejects_str(value: Any) -> None:
    consumer: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    rejects(consumer.subscribe, "topics", value)
    assert consumer.subscription() == set()
    consumer.close()
    rejects(consumer.subscribe, "topics", value)


def test_mock_consumer_subscribe_takes_any_other_iterable() -> None:
    consumer: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    consumer.subscribe(topics=["orders"])
    assert consumer.subscription() == {"orders"}
    consumer.subscribe(topics=("a", "b"))
    assert consumer.subscription() == {"a", "b"}
    consumer.subscribe(topics=iter(["c"]))
    assert consumer.subscription() == {"c"}
    consumer.subscribe(topics={"d"}, callback=ConsumerRebalanceListener())
    assert consumer.subscription() == {"d"}
    consumer.close()


@pytest.mark.parametrize("value", STRINGS)
async def test_async_mock_consumer_subscribe_rejects_str(value: Any) -> None:
    consumer: AsyncMockConsumer[str, str] = AsyncMockConsumer(offset_reset_strategy="earliest")
    with pytest.raises(TypeError) as info:
        await consumer.subscribe(topics=value)
    assert str(info.value) == message("topics", value)
    assert consumer.subscription() == set()
    await consumer.subscribe(topics=["orders"])
    assert consumer.subscription() == {"orders"}
    await consumer.close()


# ---------------------------------------------------------------------------
# Admin clients: every Collection<String> parameter, real and mock, sync and asyncio


@pytest.mark.parametrize("method,name", ADMIN_CALLS)
@pytest.mark.parametrize("value", STRINGS)
def test_admin_rejects_str(method: str, name: str, value: Any) -> None:
    admin = Admin.create(configs=UNREACHABLE)
    try:
        rejects(getattr(admin, method), name, value)
    finally:
        admin.close()
    # The shape check precedes the closed check (Implementation over the FFI, Order).
    rejects(getattr(admin, method), name, value)


@pytest.mark.parametrize("method,name", ADMIN_CALLS)
@pytest.mark.parametrize("value", STRINGS)
async def test_async_admin_rejects_str(method: str, name: str, value: Any) -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        rejects(getattr(admin, method), name, value)
    finally:
        await admin.close()
    rejects(getattr(admin, method), name, value)


@pytest.mark.parametrize("method,name", ADMIN_CALLS)
@pytest.mark.parametrize("value", STRINGS)
def test_mock_admin_rejects_str(method: str, name: str, value: Any) -> None:
    client = MockAdminClient()
    rejects(getattr(client, method), name, value)
    client.close()
    rejects(getattr(client, method), name, value)


@pytest.mark.parametrize("method,name", ADMIN_CALLS)
@pytest.mark.parametrize("value", STRINGS)
async def test_async_mock_admin_rejects_str(method: str, name: str, value: Any) -> None:
    client = AsyncMockAdminClient()
    rejects(getattr(client, method), name, value)
    await client.close()
    rejects(getattr(client, method), name, value)


def test_mock_admin_takes_any_other_iterable() -> None:
    client = MockAdminClient()
    result = client.delete_topics(topics=iter(["orders"]))
    assert set(result.topic_name_values() or {}) == {"orders"}
    # Java's mock implements describeTopics (describeConsumerGroups is "Not implemented yet").
    described = client.describe_topics(topic_names=("a", "b"))
    assert set(described.topic_name_values() or {}) == {"a", "b"}
    client.close()


# ---------------------------------------------------------------------------
# Value types and options reached from the admin methods


@pytest.mark.parametrize("value", STRINGS)
def test_topic_collection_of_topic_names_rejects_str(value: Any) -> None:
    rejects(TopicCollection.of_topic_names, "topics", value)
    assert TopicCollection.of_topic_names(topics=iter(["a", "b"])).topic_names() == ["a", "b"]


@pytest.mark.parametrize("value", STRINGS)
def test_list_groups_options_protocol_types_rejects_str(value: Any) -> None:
    rejects(ListGroupsOptions().with_protocol_types, "protocol_types", value)
    assert ListGroupsOptions().with_protocol_types(protocol_types=None).protocol_types() == set()
    options = ListGroupsOptions().with_protocol_types(protocol_types=iter(["consumer"]))
    assert options.protocol_types() == {"consumer"}


@pytest.mark.parametrize("value", STRINGS)
def test_mock_admin_builder_broker_log_dirs_rejects_str(value: Any) -> None:
    # A List<List<String>>: the outer message names its element kind.
    with pytest.raises(TypeError) as info:
        MockAdminClient.Builder().broker_log_dirs(broker_log_dirs=value)
    assert str(info.value) == (f"broker_log_dirs must be an iterable of lists of str, not "
                               f"{type(value).__name__}")
    # The inner collections are checked too: one str per broker is the same mistake.
    with pytest.raises(TypeError) as info:
        MockAdminClient.Builder().broker_log_dirs(broker_log_dirs=[value])
    assert str(info.value) == message("broker_log_dirs", value)
    client = MockAdminClient.Builder().broker_log_dirs(broker_log_dirs=[["/d1", "/d2"]]).build()
    client.close()


def test_mock_admin_builder_broker_log_dirs_keeps_a_one_shot_iterable() -> None:
    builder = MockAdminClient.Builder().broker_log_dirs(
        broker_log_dirs=iter([["/d1"], ["/d2"]]))  # type: ignore[arg-type]  # a one-shot iterable
    assert builder._broker_log_dirs == [["/d1"], ["/d2"]]  # noqa: SLF001 - the stored dirs
    builder.build().close()


@pytest.mark.parametrize("value", STRINGS)
def test_error_set_of_string_constructors_reject_str(value: Any) -> None:
    """Java's TopicAuthorizationException(Set<String>) and InvalidTopicException(Set<String>)."""
    rejects(TopicAuthorizationError, "unauthorized_topics", value)
    rejects(InvalidTopicError, "invalid_topics", value)
    with pytest.raises(TypeError) as info:
        TopicAuthorizationError(message="m", unauthorized_topics=value)
    assert str(info.value) == message("unauthorized_topics", value)
    assert TopicAuthorizationError(unauthorized_topics=["orders"]).unauthorized_topics() == {"orders"}
    assert InvalidTopicError(invalid_topics=iter(["a", "b"])).invalid_topics() == {"a", "b"}
