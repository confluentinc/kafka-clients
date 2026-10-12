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

"""``AsyncMockAdminClient``, the asyncio peer of ``MockAdminClient``: it shares the
mock's private core, so ``test_mock_admin_client.py`` covers the bodies. These check
the peer's own surface: one representative call per method group (with its
``asyncio.Future``\\ s), the ``Builder`` swap, the methods that are
``async def`` exactly where ``AsyncAdmin``'s are, the forms and keyword-only calls,
and that Java's ``synchronized`` lock is not held once a call returns to its task.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
from typing import Any

import pytest

from confluent_kafka.admin import (
    AlterConfigOp,
    AsyncAdmin,
    AsyncMockAdminClient,
    ConfigEntry,
    CreateDelegationTokenOptions,
    DescribeDelegationTokenOptions,
    FeatureUpdate,
    FinalizedVersionRange,
    ListConsumerGroupOffsetsSpec,
    MockAdminClient,
    NewPartitionReassignment,
    NewTopic,
    OffsetSpec,
)
from confluent_kafka.common import (
    AclBindingFilter,
    KafkaPrincipal,
    Node,
    TopicCollection,
    TopicPartition,
    TopicPartitionReplica,
    Uuid,
)
from confluent_kafka.common.config import ConfigResource
from confluent_kafka.common.errors.invalid_request_error import InvalidRequestError
from confluent_kafka.common.errors.timeout_error import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.errors.topic_exists_error import TopicExistsError
from confluent_kafka.common.errors.unsupported_version_error import UnsupportedVersionError
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError
from confluent_kafka.null_pointer_error import NullPointerError


def admin(num_brokers: int = 3) -> AsyncMockAdminClient:
    return AsyncMockAdminClient.create().num_brokers(num_brokers=num_brokers).build()


def test_builder_swap_builds_the_async_peer() -> None:
    # AsyncMockAdminClient.Builder.build() returns an AsyncMockAdminClient.
    builder = AsyncMockAdminClient.create()
    assert type(builder) is AsyncMockAdminClient.Builder
    assert not issubclass(AsyncMockAdminClient.Builder, MockAdminClient.Builder)
    client = builder.num_brokers(num_brokers=2).build()
    assert type(client) is AsyncMockAdminClient
    assert isinstance(client, AsyncAdmin)
    assert type(AsyncMockAdminClient.Builder(cluster_id="c").build()) is AsyncMockAdminClient
    assert type(MockAdminClient.create().build()) is MockAdminClient


def test_constructors_and_constants() -> None:
    assert AsyncMockAdminClient.DEFAULT_CLUSTER_ID == "I4ZmrWqfT2e-upky_4fdPA"
    assert AsyncMockAdminClient.DEFAULT_LOG_DIRS == ("/tmp/kafka-logs",)
    assert AsyncMockAdminClient().brokers() == [Node.no_node()]
    broker = Node(id=0, host="h", port=1)
    assert AsyncMockAdminClient(brokers=[broker], controller=broker).brokers() == [broker]
    with pytest.raises(IllegalArgumentError) as e:
        AsyncMockAdminClient(brokers=[broker])  # type: ignore[call-overload]
    assert str(e.value) == "AsyncMockAdminClient() takes one of (), (brokers, controller); got (brokers)"


def test_async_def_exactly_where_async_admin_has_it_and_every_method_overridden() -> None:
    for name, member in vars(AsyncAdmin).items():
        if not callable(member) or (name.startswith("_") and name not in (
                "__aenter__", "__aexit__")) or name == "create":
            continue
        own = vars(AsyncMockAdminClient).get(name)
        assert own is not None, name
        assert (inspect.iscoroutinefunction(inspect.unwrap(own))
                == inspect.iscoroutinefunction(inspect.unwrap(member))), name
    # The Java mock's own methods stay plain def: none of them waits.
    for name in ("controller", "add_topic", "mark_topic_for_deletion", "timeout_next_request",
                 "handle_describe_topics_using_ids", "update_beginning_offsets",
                 "update_end_offsets", "update_consumer_group_offsets",
                 "set_fetches_remaining_until_visible", "brokers", "broker"):
        assert not inspect.iscoroutinefunction(getattr(AsyncMockAdminClient, name)), name


async def test_topics_and_cluster() -> None:
    client = admin()
    created = await client.create_topics(new_topics=[
        NewTopic(name="t", num_partitions=2, replication_factor=1)])
    future = created.values()["t"]
    assert isinstance(future, asyncio.Future) and future.done()
    assert await created.num_partitions(topic="t") == 2
    again = await client.create_topics(new_topics=[
        NewTopic(name="t", num_partitions=1, replication_factor=1)])
    with pytest.raises(TopicExistsError):
        await again.all()
    assert await (await client.list_topics()).names() == {"t"}
    described = await client.describe_topics(topic_names=["t"])
    names = described.all_topic_names()
    assert names is not None
    assert len((await names)["t"].partitions()) == 2
    cluster = await client.describe_cluster()
    assert await cluster.cluster_id() == AsyncMockAdminClient.DEFAULT_CLUSTER_ID
    assert len(await cluster.nodes()) == 3
    deleted = await client.delete_topics(topics=TopicCollection.of_topic_names(topics=["t"]))
    await deleted.all()
    assert await (await client.list_topics()).names() == set()


async def test_timeout_next_request_and_handle_describe_topics_using_ids() -> None:
    client = admin()
    client.timeout_next_request(number_of_request=1)
    timed_out = await client.create_topics(new_topics=[
        NewTopic(name="t", num_partitions=1, replication_factor=1)])
    with pytest.raises(KafkaTimeoutError):
        await timed_out.all()
    created = await client.create_topics(new_topics=[
        NewTopic(name="t", num_partitions=1, replication_factor=1)])
    topic_id = await created.topic_id(topic="t")
    futures = client.handle_describe_topics_using_ids(topic_ids=[topic_id])
    assert isinstance(futures[topic_id], asyncio.Future)
    assert (await futures[topic_id]).name() == "t"


def test_handle_describe_topics_using_ids_needs_a_running_loop() -> None:
    with pytest.raises(IllegalStateError):
        admin().handle_describe_topics_using_ids(topic_ids=[Uuid.random_uuid()])


async def test_configs() -> None:
    client = admin()
    resource = ConfigResource(type=ConfigResource.Type.GROUP, name="g")
    altered = await client.incremental_alter_configs(configs={resource: [AlterConfigOp(
        config_entry=ConfigEntry(name="k", value="v"), operation_type=AlterConfigOp.OpType.SET)]})
    await altered.all()
    described = await client.describe_configs(resources=[resource])
    entry = (await described.all())[resource].get(name="k")
    assert entry is not None and entry.value() == "v"
    listed = await client.list_config_resources()
    assert resource in await listed.all()
    groups = await client.list_groups()
    assert [g.group_id() for g in await groups.all()] == ["g"]


async def test_partitions_records_log_dirs_and_offsets() -> None:
    client = admin()
    await (await client.create_topics(new_topics=[
        NewTopic(name="t", num_partitions=1, replication_factor=2)])).all()
    tp = TopicPartition(topic="t", partition=0)
    with pytest.raises(UnsupportedVersionError) as e:
        await client.create_partitions(new_partitions={})
    assert str(e.value) == "Not implemented yet"
    assert (await client.delete_records(records_to_delete={})).low_watermarks() == {}
    reassigned = await client.alter_partition_reassignments(
        reassignments={tp: NewPartitionReassignment(target_replicas=[1, 2])})
    await reassigned.all()
    listed = await (await client.list_partition_reassignments()).reassignments()
    assert listed[tp].adding_replicas() == [2]
    log_dirs = await client.describe_log_dirs(brokers=[0, 1, 2])
    assert set(await log_dirs.all_descriptions()) == {0, 1, 2}
    replica = TopicPartitionReplica(topic="t", partition=0, broker_id=0)
    replica_dirs = await client.describe_replica_log_dirs(replicas=[replica])
    info = (await replica_dirs.all())[replica]
    assert info.current_replica_log_dir() == "/tmp/kafka-logs"
    client.update_end_offsets(new_offsets={tp: 9})
    offsets = await client.list_offsets(topic_partition_offsets={tp: OffsetSpec.latest()})
    assert (await offsets.partition_result(partition=tp)).offset() == 9


async def test_consumer_group_offsets() -> None:
    client = admin()
    tp = TopicPartition(topic="t", partition=0)
    client.update_consumer_group_offsets(new_offsets={tp: 4})
    result = await client.list_consumer_group_offsets(
        group_specs={"g": ListConsumerGroupOffsetsSpec(topic_partitions=[])})
    listed = (await result.partitions_to_offset_and_metadata(group_id="g"))[tp]
    assert listed is not None and listed.offset() == 4
    with pytest.raises(NullPointerError):  # Java's default spec
        await client.list_consumer_group_offsets(group_id="g")
    with pytest.raises(UnsupportedVersionError) as e:
        await client.describe_consumer_groups(group_ids=["g"])
    assert str(e.value) == "Not implemented yet"


async def test_delegation_tokens() -> None:
    client = admin()
    alice = KafkaPrincipal(principal_type=KafkaPrincipal.USER_TYPE, name="alice")
    created = await client.create_delegation_token(
        options=CreateDelegationTokenOptions(renewers=[alice]))
    token = await created.delegation_token()
    renewed = await client.renew_delegation_token(hmac=token.hmac())
    assert await renewed.expiry_timestamp() == -1
    described = await client.describe_delegation_token(
        options=DescribeDelegationTokenOptions(owners=[alice]))
    assert await described.delegation_tokens() == [token]
    with pytest.raises(NullPointerError):  # Java's default null owners
        await client.describe_delegation_token()
    expired = await client.expire_delegation_token(hmac=token.hmac())
    assert await expired.expiry_timestamp() == -1


async def test_features() -> None:
    client = (AsyncMockAdminClient.create()
              .feature_levels(feature_levels={"f": 3})
              .min_supported_feature_levels(min_supported_feature_levels={"f": 1})
              .max_supported_feature_levels(max_supported_feature_levels={"f": 5})
              .build())
    updated = await client.update_features(feature_updates={"f": FeatureUpdate(
        max_version_level=4, upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)})
    await updated.all()
    metadata = await (await client.describe_features()).feature_metadata()
    assert metadata.finalized_features()["f"] == FinalizedVersionRange(
        min_version_level=4, max_version_level=4)
    rejected = await client.update_features(feature_updates={"f": FeatureUpdate(
        max_version_level=6, upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)})
    with pytest.raises(InvalidRequestError) as e:
        await rejected.all()
    assert str(e.value) == "Invalid update version 6 for feature f. Can't upgrade above 5"


async def test_unimplemented_groups_raise_java_messages() -> None:
    client = admin()
    with pytest.raises(UnsupportedVersionError) as e:
        await client.describe_acls(filter=AclBindingFilter.ANY)
    assert str(e.value) == "Not implemented yet"
    with pytest.raises(UnsupportedVersionError) as e:
        await client.alter_client_quotas(entries=[])
    assert str(e.value) == "Not implement yet"
    with pytest.raises(UnsupportedVersionError) as e:
        await client.describe_user_scram_credentials()
    assert str(e.value) == "Not implemented yet"
    with pytest.raises(UnsupportedVersionError) as e:
        await client.list_transactions()
    assert str(e.value) == "Not implemented yet"


async def test_close_does_nothing_and_context_manager() -> None:
    async with admin() as client:
        await client.close(timeout=-1.0)
        assert await (await client.list_topics()).names() == set()
    assert await (await client.list_topics()).names() == set()


async def test_positional_calls_are_type_errors() -> None:
    client = admin()
    with pytest.raises(TypeError):
        await client.create_topics([])  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        await client.list_consumer_group_offsets("g")  # type: ignore[call-overload]
    with pytest.raises(TypeError):
        client.timeout_next_request(1)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        AsyncMockAdminClient.Builder().num_brokers(1)  # type: ignore[call-arg]


async def test_lock_is_released_before_the_call_returns_to_its_task() -> None:
    # Java's synchronized is the core's RLock; on the async peer it is never held
    # across an await, so another thread can take it while the task is suspended.
    client = admin()

    def held_elsewhere() -> bool:
        acquired = client._lock.acquire(blocking=False)
        if acquired:
            client._lock.release()
        return not acquired

    calls: list[Any] = [
        client.create_topics(new_topics=[NewTopic(name="t", num_partitions=1,
                                                  replication_factor=1)]),
        client.describe_cluster(),
        client.list_groups(),
    ]
    for call in calls:
        result = await call
        assert result is not None
        holder = await asyncio.to_thread(held_elsewhere)
        assert holder is False
    # A thread holding the lock blocks the mock's synchronized bodies, as in Java.
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with client._lock:
            entered.set()
            release.wait(5)

    thread = threading.Thread(target=hold)
    thread.start()
    entered.wait(5)
    assert await asyncio.to_thread(held_elsewhere) is True
    release.set()
    thread.join(5)
    assert await asyncio.to_thread(held_elsewhere) is False
