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

"""A ``str`` argument holding a NUL character where the FFI takes a C string: it
raises ``ValueError("embedded null character")`` where it reaches the FFI, as
``PyArg_Parse``'s ``"s"`` unit does and as a record's topic and header keys and
the producer's and the consumer's configs already did. A C string ends at its
first NUL, so the rest of the name must not be dropped on the way: the core
would act on another name (``subscribe(topics=["orders\\0-test"])`` subscribing
to ``orders``, ``delete_topics`` deleting it). Java sends such a name and the
broker rejects it; the .NET binding throws at the call too (``dotnet/CLAUDE.md``,
Strings). The check comes where the value is converted for the FFI, as a
non-``str``'s ``TypeError`` does: after the closed check, before the core's own
checks. The value types and the mocks do not check, as Java's do not. Java has
no test of its own: a Java ``String`` crosses no C boundary."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from confluent_kafka import IllegalStateError
from confluent_kafka.admin import (
    Admin,
    ListGroupsOptions,
    ListTransactionsOptions,
    MemberToRemove,
    MockAdminClient,
    NewTopic,
    RemoveMembersFromConsumerGroupOptions,
)
from confluent_kafka.common import TopicCollection, TopicPartition
from confluent_kafka.consumer import (
    AsyncKafkaConsumer,
    ConsumerRebalanceListener,
    KafkaConsumer,
    MockConsumer,
    OffsetAndMetadata,
)
from confluent_kafka.producer import KafkaProducer

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CONSUMER = {**UNREACHABLE, "group.protocol": "consumer", "group.id": "g"}
NAME = "orders\0-test"
EMBEDDED_NUL = "embedded null character"


class _NoOpListener(ConsumerRebalanceListener):
    def on_partitions_revoked(self, partitions: object) -> None:
        pass

    def on_partitions_assigned(self, partitions: object) -> None:
        pass


def test_the_mocks_and_value_types_keep_a_nul() -> None:
    consumer: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy="earliest")
    consumer.subscribe(topics=[NAME])
    assert consumer.subscription() == {NAME}
    consumer.close()
    client = MockAdminClient()
    assert set(client.delete_topics(topics=[NAME]).topic_name_values() or {}) == {NAME}
    client.close()
    assert OffsetAndMetadata(offset=1, metadata=NAME).metadata() == NAME


@pytest.mark.parametrize("build", [
    lambda configs: KafkaProducer(configs=configs),
    lambda configs: KafkaConsumer(configs={**CONSUMER, **configs}),
    lambda configs: Admin.create(configs=configs),
], ids=["producer", "consumer", "admin"])
@pytest.mark.parametrize("configs", [
    {**UNREACHABLE, "client.id": "a\0b"},
    # Cut at the NUL, the key would be bootstrap.servers itself.
    {"bootstrap.servers\0-ignored": "localhost:1"},
], ids=["value", "key"])
def test_a_client_refuses_a_nul_in_its_configs(build: Callable[[dict[str, str]], Any],
                                               configs: dict[str, str]) -> None:
    with pytest.raises(ValueError, match=EMBEDDED_NUL):
        build(configs).close()


@pytest.mark.parametrize("listener", [False, True], ids=["topics", "topics-callback"])
def test_the_consumer_refuses_a_nul_in_a_topic(listener: bool) -> None:
    with KafkaConsumer(configs=CONSUMER) as consumer:
        with pytest.raises(ValueError, match=EMBEDDED_NUL):
            if listener:
                consumer.subscribe(topics=[NAME], callback=_NoOpListener())
            else:
                consumer.subscribe(topics=[NAME])
        assert consumer.subscription() == set()
        consumer.subscribe(topics=["orders"])
        assert consumer.subscription() == {"orders"}


async def test_the_async_consumer_refuses_a_nul_in_a_topic() -> None:
    consumer: AsyncKafkaConsumer[bytes, bytes] = AsyncKafkaConsumer(configs=CONSUMER)
    try:
        with pytest.raises(ValueError, match=EMBEDDED_NUL):
            await consumer.subscribe(topics=[NAME])
        assert consumer.subscription() == set()
    finally:
        await consumer.close()


OFFSETS = {TopicPartition(topic="t", partition=0): OffsetAndMetadata(offset=1, metadata=NAME)}


def test_the_consumer_refuses_a_nul_in_committed_metadata() -> None:
    # Refused before the commit is sent: cut at the NUL, the metadata committed
    # would have been "orders".
    with KafkaConsumer(configs=CONSUMER) as consumer:
        with pytest.raises(ValueError, match=EMBEDDED_NUL):
            consumer.commit(offsets=OFFSETS, timeout=0.5)


def test_the_producer_refuses_a_nul_in_the_metadata_of_offsets_sent_to_a_transaction() -> None:
    with KafkaConsumer(configs=CONSUMER) as consumer:
        group_metadata = consumer.group_metadata()
    producer = KafkaProducer(configs={**UNREACHABLE, "max.block.ms": "500"})
    try:
        # The check precedes the core's: for this producer, which has no
        # transactional.id, Java raises IllegalStateException("Transactional
        # method invoked on a non-transactional producer.") once it has waited
        # up to max.block.ms for the topics' metadata.
        with pytest.raises(ValueError, match=EMBEDDED_NUL):
            producer.send_offsets_to_transaction(offsets=OFFSETS, group_metadata=group_metadata)
    finally:
        producer.close(timeout=0)


def test_the_check_precedes_the_cores_own_and_follows_the_closed_check() -> None:
    # Java's throwIfGroupIdNotDefined() comes first, but the core makes that
    # check inside the call the converted topics are handed to, so the
    # conversion's ValueError comes first, as a non-str topic's TypeError does.
    without_group_id = {**UNREACHABLE, "group.protocol": "consumer"}
    with KafkaConsumer(configs=without_group_id) as consumer:
        with pytest.raises(TypeError, match="topics must be str"):
            consumer.subscribe(topics=[1])  # type: ignore[list-item]
        with pytest.raises(ValueError, match=EMBEDDED_NUL):
            consumer.subscribe(topics=[NAME])
    with pytest.raises(IllegalStateError, match="This consumer has already been closed."):
        consumer.subscribe(topics=[NAME])


def _remove_members_with_reason(admin: Admin) -> Any:
    options = RemoveMembersFromConsumerGroupOptions()
    options.reason(reason=NAME)
    return admin.remove_members_from_consumer_group(group_id="g", options=options)


# Every admin call handing a str to the FFI as a C string, named after what it
# takes.
ADMIN_CALLS: list[tuple[str, Callable[[Admin], Any]]] = [
    ("delete_topics", lambda admin: admin.delete_topics(topics=[NAME])),
    ("delete_topics-collection", lambda admin: admin.delete_topics(
        topics=TopicCollection.of_topic_names(topics=[NAME]))),
    ("describe_topics", lambda admin: admin.describe_topics(topic_names=[NAME])),
    ("describe_topics-collection", lambda admin: admin.describe_topics(
        topics=TopicCollection.of_topic_names(topics=[NAME]))),
    ("describe_consumer_groups", lambda admin: admin.describe_consumer_groups(group_ids=[NAME])),
    ("delete_consumer_groups", lambda admin: admin.delete_consumer_groups(group_ids=[NAME])),
    ("describe_classic_groups", lambda admin: admin.describe_classic_groups(group_ids=[NAME])),
    ("describe_transactions", lambda admin: admin.describe_transactions(transactional_ids=[NAME])),
    ("fence_producers", lambda admin: admin.fence_producers(transactional_ids=[NAME])),
    ("describe_user_scram_credentials",
     lambda admin: admin.describe_user_scram_credentials(users=[NAME])),
    ("remove_members_from_consumer_group-member", lambda admin: admin.remove_members_from_consumer_group(
        group_id="g", options=RemoveMembersFromConsumerGroupOptions(
            members=[MemberToRemove(group_instance_id=NAME)]))),
    ("remove_members_from_consumer_group-reason", _remove_members_with_reason),
    ("list_groups-protocol_types", lambda admin: admin.list_groups(
        options=ListGroupsOptions().with_protocol_types(protocol_types=[NAME]))),
    ("list_transactions-pattern", lambda admin: admin.list_transactions(
        options=ListTransactionsOptions().filter_on_transactional_id_pattern(pattern=NAME))),
    ("create_topics-result-num_partitions", lambda admin: admin.create_topics(
        new_topics=[NewTopic(name="orders", num_partitions=1, replication_factor=1)]
    ).num_partitions(topic=NAME)),
]


@pytest.fixture(scope="module")
def admin() -> Any:
    client = Admin.create(configs=UNREACHABLE)
    yield client
    client.close(timeout=0)


@pytest.mark.parametrize("call", [c[1] for c in ADMIN_CALLS], ids=[c[0] for c in ADMIN_CALLS])
def test_the_admin_client_refuses_a_nul_in_a_name(admin: Admin,
                                                 call: Callable[[Admin], Any]) -> None:
    with pytest.raises(ValueError, match=EMBEDDED_NUL):
        call(admin)
