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

"""A ``None`` where Java dereferences the argument raises
``TypeError("<param> must not be None")``, where Java dereferences it and in
Java's order (CLAUDE.md, Python Binding Conventions, Types: an explicit check or
a bare dereference of the argument in the method's body)."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any

import pytest

from confluent_kafka import (
    ConcurrentModificationError,
    ExecutionError,
    IllegalArgumentError,
    IllegalStateError,
)
from confluent_kafka.admin import (
    Admin,
    Config,
    NewPartitionReassignment,
    NewTopic,
    PartitionReassignment,
)
from confluent_kafka.common import ClassicGroupState, GroupState, TopicCollection, TopicPartition
from confluent_kafka.common.acl import (
    AccessControlEntry,
    AccessControlEntryFilter,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
)
from confluent_kafka.common.errors import ResourceNotFoundError
from confluent_kafka.common.resource import PatternType, ResourcePatternFilter, ResourceType
from confluent_kafka.consumer import OffsetAndMetadata

N: Any = None

SITES: list[tuple[str, Callable[[], object], str]] = [
    # GroupState.parse / ClassicGroupState.parse: name.toUpperCase(Locale.ROOT)
    ("GroupState.parse", lambda: GroupState.parse(name=N), "name"),
    ("ClassicGroupState.parse", lambda: ClassicGroupState.parse(name=N), "name"),
    # AclOperation / AclPermissionType / ResourceType.fromString: str.toUpperCase(Locale.ROOT)
    ("AclOperation.from_string", lambda: AclOperation.from_string(str=N), "str"),
    ("AclPermissionType.from_string", lambda: AclPermissionType.from_string(str=N), "str"),
    ("ResourceType.from_string", lambda: ResourceType.from_string(str=N), "str"),
    # AclBindingFilter.matches: binding.pattern()
    ("AclBindingFilter.matches", lambda: AclBindingFilter.ANY.matches(binding=N), "binding"),
    # AccessControlEntryFilter.matches: other.principal() once the filter has a principal
    ("AccessControlEntryFilter.matches",
     lambda: AccessControlEntryFilter(principal="User:a", host=None, operation=AclOperation.ANY,
                                      permission_type=AclPermissionType.ANY).matches(other=N),
     "other"),
    ("AccessControlEntryFilter.matches (operation)",
     lambda: AccessControlEntryFilter(principal=None, host=None, operation=AclOperation.READ,
                                      permission_type=AclPermissionType.ANY).matches(other=N),
     "other"),
    # ResourcePatternFilter.matches: pattern.resourceType() once the type is not ANY
    ("ResourcePatternFilter.matches",
     lambda: ResourcePatternFilter(resource_type=ResourceType.TOPIC, name=None,
                                   pattern_type=PatternType.ANY).matches(pattern=N),
     "pattern"),
    ("ResourcePatternFilter.matches (name)",
     lambda: ResourcePatternFilter(resource_type=ResourceType.ANY, name="t",
                                   pattern_type=PatternType.ANY).matches(pattern=N),
     "pattern"),
    # TopicCollection.ofTopicNames / ofTopicIds: new ArrayList<>(topics)
    ("TopicCollection.of_topic_names", lambda: TopicCollection.of_topic_names(topics=N), "topics"),
    ("TopicCollection.of_topic_ids", lambda: TopicCollection.of_topic_ids(topics=N), "topics"),
    # Config(entries): for (ConfigEntry entry : entries)
    ("Config", lambda: Config(entries=N), "entries"),
    # PartitionReassignment: Collections.unmodifiableList, in this order
    ("PartitionReassignment replicas",
     lambda: PartitionReassignment(replicas=N, adding_replicas=N, removing_replicas=N), "replicas"),
    ("PartitionReassignment adding_replicas",
     lambda: PartitionReassignment(replicas=[1], adding_replicas=N, removing_replicas=N),
     "adding_replicas"),
    ("PartitionReassignment removing_replicas",
     lambda: PartitionReassignment(replicas=[1], adding_replicas=[], removing_replicas=N),
     "removing_replicas"),
]


@pytest.mark.parametrize("label,call,param", SITES, ids=[s[0] for s in SITES])
def test_a_dereferenced_none_is_a_type_error(label: str, call: Callable[[], object],
                                             param: str) -> None:
    with pytest.raises(TypeError) as e:
        call()
    assert str(e.value) == f"{param} must not be None"


def test_a_none_element_java_dereferences() -> None:
    # Config: entry.name(); NewPartitionReassignment: List.copyOf.
    with pytest.raises(TypeError) as c:
        Config(entries=[N])
    assert str(c.value) == "entries must not contain None"
    with pytest.raises(TypeError) as r:
        NewPartitionReassignment(target_replicas=[1, N])
    assert str(r.value) == "target_replicas must not contain None"


def test_java_does_not_dereference_there() -> None:
    # ANY filters touch no field of the argument, as Java's.
    assert AccessControlEntryFilter.ANY.matches(other=N) is True
    assert ResourcePatternFilter.ANY.matches(pattern=N) is True
    # Java checks null itself here.
    assert PatternType.from_string(name=N) is PatternType.UNKNOWN
    with pytest.raises(IllegalArgumentError) as e:
        NewPartitionReassignment(target_replicas=N)
    assert str(e.value) == "Cannot create a new partition reassignment without any replicas"


def test_admin_results_given_none() -> None:
    """The client's Results: a null key is never in the request."""
    entry = AccessControlEntry(principal="User:a", host="*", operation=AclOperation.READ,
                               permission_type=AclPermissionType.ALLOW)
    assert entry is not None
    admin = Admin.create(configs={"bootstrap.servers": "localhost:1",
                                  "default.api.timeout.ms": 500, "request.timeout.ms": 250})
    try:
        # FenceProducersResult.findAndApply: "TransactionalId `" + null + "` ..."
        with pytest.raises(IllegalArgumentError) as f:
            admin.fence_producers(transactional_ids=["x"]).producer_id(transactional_id=N)
        assert str(f.value) == "TransactionalId `None` was not included in the request"
        # RemoveMembersFromConsumerGroupResult.memberResult: memberInfos.contains(null) fails.
        from confluent_kafka.admin import MemberToRemove, RemoveMembersFromConsumerGroupOptions
        removed = admin.remove_members_from_consumer_group(
            group_id="g", options=RemoveMembersFromConsumerGroupOptions(
                members=[MemberToRemove(group_instance_id="i")]))
        with pytest.raises(IllegalArgumentError) as m:
            removed.member_result(member=N)
        assert str(m.value) == "Member None was not included in the original request"
        # CreateTopicsResult.config(null): the core's answer for a topic not in the request.
        created = admin.create_topics(
            new_topics=[NewTopic(name="t", num_partitions=1, replication_factor=1)])
        with pytest.raises(ExecutionError) as c:
            created.config(topic=N).get()
        assert type(c.value.__cause__) is IllegalArgumentError
        assert str(c.value.__cause__) == "Topic None was not part of the createTopics request"
        # AlterConsumerGroupOffsetsResult.partitionResult(null) (deviation, exception 4).
        altered = admin.alter_consumer_group_offsets(
            group_id="g", offsets={TopicPartition(topic="t", partition=0): OffsetAndMetadata(offset=1)})
        with pytest.raises(TypeError) as a:
            altered.partition_result(partition=N)
        assert str(a.value) == "partition must not be None"
        # DescribeUserScramCredentialsResult.description(null): the request's error, else
        # ResourceNotFoundError("No such user: None"); here the request times out.
        described = admin.describe_user_scram_credentials(users=["u"])
        with pytest.raises(ExecutionError) as d:
            described.description(user_name=N).get()
        users_error: BaseException | None = None
        try:
            described.users().get()
        except ExecutionError as e:
            users_error = e.__cause__
        assert type(d.value.__cause__) is type(users_error)
        assert not isinstance(d.value.__cause__, ResourceNotFoundError)
        # When the request succeeds, no described user is null.
        from confluent_kafka.common import KafkaFuture
        no_user = described._no_such_user(KafkaFuture.completed_future(value=["u"]), None)
        with pytest.raises(ExecutionError) as r:
            no_user.get()
        assert type(r.value.__cause__) is ResourceNotFoundError
        assert str(r.value.__cause__) == "No such user: None"
    finally:
        admin.close(timeout=0)


# ---- Results, copied collections and the consumer and producer value classes ----


def test_describe_transactions_result_message_reads_none() -> None:
    # DescribeTransactionsResult.description: "TransactionalId `" + null + "` ...".
    from confluent_kafka.admin import DescribeTransactionsResult
    from confluent_kafka.admin._futures import Sync
    result = DescribeTransactionsResult._new(futures={}, kind=Sync)
    with pytest.raises(IllegalArgumentError) as e:
        result.description(transactional_id=N)
    assert str(e.value) == "TransactionalId `None` was not included in the request"


def test_a_none_element_java_copies_reject() -> None:
    from confluent_kafka.admin import (
        ConsumerGroupDescription,
        MemberAssignment,
        UserScramCredentialsDescription,
    )
    from confluent_kafka.common import GroupState, GroupType
    cases = [
        # List.copyOf(members) / Set.copyOf(topicPartitions) / List.copyOf(credentialInfos)
        (lambda: ConsumerGroupDescription(group_id="g", is_simple_consumer_group=False,
                                          members=[N], partition_assignor="a",
                                          type=GroupType.CONSUMER,
                                          group_state=GroupState.STABLE, coordinator=None,
                                          authorized_operations=None, group_epoch=None,
                                          target_assignment_epoch=None), "members"),
        (lambda: MemberAssignment(topic_partitions=[N]), "topic_partitions"),
        (lambda: UserScramCredentialsDescription(name="u", credential_infos=[N]),
         "credential_infos"),
    ]
    for call, param in cases:
        with pytest.raises(TypeError) as e:
            call()
        assert str(e.value) == f"{param} must not contain None"


def test_kafka_consumer_subscribe_to_a_none_pattern() -> None:
    # subscribeToRegex: subscriptionPattern.pattern().isEmpty().
    from confluent_kafka.consumer import KafkaConsumer, SubscriptionPattern
    consumer: Any = KafkaConsumer(configs={"bootstrap.servers": "localhost:1", "group.id": "g",
                                           "group.protocol": "consumer"})
    try:
        with pytest.raises(TypeError) as e:
            consumer.subscribe(pattern=SubscriptionPattern(pattern=N))
        assert str(e.value) == "SubscriptionPattern.pattern must not be None"
    finally:
        consumer.close()


@pytest.mark.parametrize("with_callback", [False, True], ids=["plain", "callback"])
async def test_async_consumers_subscribe_to_a_none_pattern(with_callback: bool) -> None:
    # AsyncKafkaConsumer: subscriptionPattern.pattern().isEmpty(), after the
    # listener's own check (AsyncMockConsumer: test_none_stored.py).
    from confluent_kafka.consumer import (
        AsyncKafkaConsumer,
        ConsumerRebalanceListener,
        SubscriptionPattern,
    )

    class Listener(ConsumerRebalanceListener):
        def on_partitions_revoked(self, partitions: object) -> None:
            pass

        def on_partitions_assigned(self, partitions: object) -> None:
            pass
    kwargs: dict[str, Any] = {"pattern": SubscriptionPattern(pattern=N)}
    if with_callback:
        kwargs["callback"] = Listener()
    consumer: Any = AsyncKafkaConsumer(configs={"bootstrap.servers": "localhost:1",
                                                "group.id": "g", "group.protocol": "consumer"})
    try:
        with pytest.raises(TypeError) as e:
            await consumer.subscribe(**kwargs)
        assert str(e.value) == "SubscriptionPattern.pattern must not be None"
    finally:
        await consumer.close()


def _holding_another_thread(consumer: Any) -> tuple[threading.Thread, threading.Event]:
    """Another thread inside the consumer: it holds a use (Java's acquire())
    until the returned event is set."""
    entered, release = threading.Event(), threading.Event()

    def hold() -> None:
        with consumer._use():
            entered.set()
            release.wait(10)

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert entered.wait(10)
    return thread, release


@pytest.mark.parametrize("kind", ["sync", "async"])
def test_a_none_pattern_is_checked_after_the_closed_and_thread_checks(kind: str) -> None:
    """Java's subscribeToRegex calls acquireAndEnsureOpen() before it reads the
    pattern: a closed consumer, and a call while another thread is inside it,
    report that first (Order)."""
    from confluent_kafka.consumer import AsyncKafkaConsumer, KafkaConsumer, SubscriptionPattern

    configs = {"bootstrap.servers": "localhost:1", "group.id": "g", "group.protocol": "consumer"}
    pattern = SubscriptionPattern(pattern=N)

    async def main() -> None:
        consumer: Any = (KafkaConsumer if kind == "sync" else AsyncKafkaConsumer)(configs=configs)

        async def subscribe() -> None:
            if kind == "sync":
                consumer.subscribe(pattern=pattern)
            else:
                await consumer.subscribe(pattern=pattern)

        thread, release = _holding_another_thread(consumer)
        try:
            with pytest.raises(ConcurrentModificationError) as e:
                await subscribe()
            assert str(e.value) == "KafkaConsumer is not safe for multi-threaded access."
        finally:
            release.set()
            thread.join(10)
        if kind == "sync":
            consumer.close()
        else:
            await consumer.close()
        with pytest.raises(IllegalStateError) as closed:
            await subscribe()
        assert str(closed.value) == "This consumer has already been closed."

    asyncio.run(main())
