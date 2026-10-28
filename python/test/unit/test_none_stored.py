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

"""A constructor that stores an argument as Java's does stores a ``None`` too;
the ``TypeError`` comes where Java uses the field, or from the call that hands
it to the FFI (CLAUDE.md, Python Binding Conventions, Class family)."""

from __future__ import annotations

from typing import Any

import pytest

from confluent_kafka.admin import (
    Admin,
    AlterUserScramCredentialsResult,
    AlterClientQuotasResult,
    DeleteRecordsResult,
    DescribeClassicGroupsResult,
    DescribeClientQuotasResult,
    DescribeConsumerGroupsResult,
    DescribeProducersResult,
    ListOffsetsResult,
    TransactionDescription,
    TransactionState,
)
from confluent_kafka.admin._futures import Sync
from confluent_kafka.common import ClassicGroupState, TopicPartition
from confluent_kafka.common.quota import (
    ClientQuotaAlteration,
    ClientQuotaEntity,
    ClientQuotaFilter,
)
from confluent_kafka.common.security.auth import KafkaPrincipal
from confluent_kafka.common.security.token.delegation import DelegationToken, TokenInformation
from confluent_kafka.consumer import OffsetAndMetadata

N: Any = None
ENTITY = ClientQuotaEntity(entries={"user": "a"})
OWNER = KafkaPrincipal(principal_type="User", name="a")
TOKEN_INFO = TokenInformation(token_id="t", owner=OWNER, renewers=[], issue_timestamp=0,
                              max_timestamp=0, expiry_timestamp=0)


def test_client_quota_alteration_stores_none_ops() -> None:
    alteration = ClientQuotaAlteration(entity=ENTITY, ops=N)
    assert alteration.ops() is None
    assert str(alteration) == "ClientQuotaAlteration(entity=ClientQuotaEntity(entries={user=a}), ops=None)"


def test_client_quota_entity_stores_none_entries() -> None:
    entity = ClientQuotaEntity(entries=N)
    assert entity.entries() is None
    assert str(entity) == "ClientQuotaEntity(entries=None)"
    # Java's equals and hashCode are null-safe (Objects.equals, Objects.hash).
    assert entity == ClientQuotaEntity(entries=N)
    assert hash(entity) == hash(ClientQuotaEntity(entries=N))
    assert entity != ENTITY


@pytest.mark.parametrize("factory", [ClientQuotaFilter.contains, ClientQuotaFilter.contains_only])
def test_client_quota_filter_stores_none_components(factory: Any) -> None:
    quota_filter = factory(components=N)
    assert quota_filter.components() is None
    assert str(quota_filter) == f"ClientQuotaFilter(components=None, strict={quota_filter.strict()})"
    assert quota_filter == factory(components=N)
    assert hash(quota_filter) == hash(factory(components=N))


def test_delegation_token_stores_none_hmac() -> None:
    token = DelegationToken(token_information=TOKEN_INFO, hmac=N)
    assert token.hmac() is None
    # MessageDigest.isEqual and Arrays.hashCode are null-safe.
    assert token == DelegationToken(token_information=TOKEN_INFO, hmac=N)
    assert token != DelegationToken(token_information=TOKEN_INFO, hmac=b"x")
    assert hash(token) == hash(DelegationToken(token_information=TOKEN_INFO, hmac=N))
    # hmacAsBase64String: Base64.getEncoder().encodeToString(null).
    with pytest.raises(TypeError) as e:
        token.hmac_as_base64_string()
    assert str(e.value) == "hmac must not be None"


def test_partition_producer_state_stores_none() -> None:
    state = DescribeProducersResult.PartitionProducerState(active_producers=N)
    assert state.active_producers() is None
    assert str(state) == "PartitionProducerState(activeProducers=None)"


def test_transaction_description_stores_none_topic_partitions() -> None:
    def make() -> TransactionDescription:
        return TransactionDescription(coordinator_id=1, state=TransactionState.ONGOING,
                                      producer_id=2, producer_epoch=0, transaction_timeout_ms=3,
                                      transaction_start_time_ms=None, topic_partitions=N)
    description = make()
    assert description.topic_partitions() is None
    assert str(description).endswith(", topicPartitions=None)")
    assert description == make() and hash(description) == hash(make())


@pytest.mark.parametrize("cls,param", [
    (ListOffsetsResult, "futures"),
    (AlterClientQuotasResult, "futures"),
    (DeleteRecordsResult, "futures"),
    (DescribeClassicGroupsResult, "futures"),
    (DescribeConsumerGroupsResult, "futures"),
    (DescribeClientQuotasResult, "entities"),
    (AlterUserScramCredentialsResult, "futures"),
])
def test_a_result_reading_its_kind_from_its_futures_requires_them(cls: Any, param: str) -> None:
    with pytest.raises(TypeError) as e:
        cls(**{param: None})
    assert str(e.value) == f"{param} must not be None"


def test_a_client_built_result_stores_none() -> None:
    from confluent_kafka.admin import DeleteConsumerGroupsResult
    result = DeleteConsumerGroupsResult._new(futures=N, kind=Sync)
    assert result._futures is None


def test_the_admin_client_reports_a_none_inside_its_quota_arguments() -> None:
    admin = Admin.create(configs={"bootstrap.servers": "localhost:1"})
    try:
        cases = [
            (lambda: admin.alter_client_quotas(
                entries=[ClientQuotaAlteration(entity=ENTITY, ops=N)]),
             "entries must not contain a ClientQuotaAlteration with a None ops"),
            (lambda: admin.alter_client_quotas(
                entries=[ClientQuotaAlteration(entity=N, ops=[])]),
             "entries must not contain a ClientQuotaAlteration with a None entity"),
            (lambda: admin.alter_client_quotas(
                entries=[ClientQuotaAlteration(entity=ClientQuotaEntity(entries=N), ops=[])]),
             "entries must not contain a ClientQuotaEntity with a None entries"),
            (lambda: admin.alter_client_quotas(
                entries=[ClientQuotaAlteration(entity=ENTITY, ops=[N])]),
             "ClientQuotaAlteration.ops must not contain None"),
            (lambda: admin.describe_client_quotas(filter=ClientQuotaFilter.contains(components=N)),
             "ClientQuotaFilter.components must not be None"),
            (lambda: admin.describe_client_quotas(
                filter=ClientQuotaFilter.contains_only(components=[N])),
             "ClientQuotaFilter.components must not contain None"),
        ]
        for call, message in cases:
            with pytest.raises(TypeError) as e:
                call()
            assert str(e.value) == message
    finally:
        admin.close(timeout=0)


# ---- Nested copies keep a None ----------------------------------------------------


def test_new_topic_and_new_partitions_store_a_none_assignment() -> None:
    from confluent_kafka.admin import NewPartitions, NewTopic
    topic = NewTopic(name="t", replicas_assignments={0: N, 1: [1]})
    assert topic.replicas_assignments() == {0: None, 1: [1]}
    partitions = NewPartitions.increase_to(total_count=2, new_assignments=[N])
    assert partitions.assignments() == [None]
    admin = Admin.create(configs={"bootstrap.servers": "localhost:1"})
    try:
        with pytest.raises(TypeError) as t:
            admin.create_topics(new_topics=[topic])
        assert str(t.value) == "new_topics must not contain a NewTopic with a None replica assignment"
        with pytest.raises(TypeError) as p:
            admin.create_partitions(new_partitions={"t": partitions})
        assert str(p.value) == "new_partitions must not contain a NewPartitions with a None assignment"
    finally:
        admin.close(timeout=0)


def test_consumer_records_stores_a_none_list() -> None:
    from confluent_kafka.consumer import ConsumerRecords
    tp = TopicPartition(topic="t", partition=0)
    records: ConsumerRecords[Any, Any] = ConsumerRecords(records={tp: N}, next_offsets={})
    # records(partition): Java's `if (recs == null) return Collections.emptyList()`.
    assert records.records(partition=tp) == []
    assert records.partitions() == {tp} and not records.is_empty()
    # count(), the iterator and records(topic) dereference the list.
    for call in (lambda: len(records), lambda: list(records),
                 lambda: records.records(topic="t")):
        with pytest.raises(TypeError) as e:
            call()
        assert str(e.value) == "records must not contain None"
    # records(topic): entry.getKey().topic() dereferences a null key.
    keyless: ConsumerRecords[Any, Any] = ConsumerRecords(records={N: []}, next_offsets={})
    with pytest.raises(TypeError) as k:
        keyless.records(topic="t")
    assert str(k.value) == "records must not contain a None key"

def test_classic_group_description_members_copy() -> None:
    from confluent_kafka.admin import ClassicGroupDescription
    # List.copyOf(members) rejects a null element.
    with pytest.raises(TypeError) as e:
        ClassicGroupDescription(group_id="g", protocol="p", protocol_data="d", members=[N],
                                state=ClassicGroupState.STABLE, coordinator=None)
    assert str(e.value) == "members must not contain None"


def test_methods_dereferencing_a_stored_none() -> None:
    from confluent_kafka.admin import ClassicGroupDescription, FeatureUpdate, MemberToRemove
    from confluent_kafka.common.security.auth import KafkaPrincipal
    from confluent_kafka.common.security.token.delegation import TokenInformation
    owner = KafkaPrincipal(principal_type="User", name="a")

    def token(o: Any, requester: Any) -> TokenInformation:
        return TokenInformation(token_id="t", owner=o, token_requester=requester, renewers=[],
                                issue_timestamp=0, max_timestamp=0, expiry_timestamp=0)
    cases = [
        # TokenInformation.ownerAsString / tokenRequesterAsString: x.toString()
        (lambda: token(N, owner).owner_as_string(), "owner"),
        (lambda: token(owner, N).token_requester_as_string(), "token_requester"),
        # ClassicGroupDescription.isSimpleConsumerGroup: protocol.isEmpty()
        (lambda: ClassicGroupDescription(group_id="g", protocol=N, protocol_data="d", members=[],
                                         state=ClassicGroupState.STABLE,
                                         coordinator=None).is_simple_consumer_group(),
         "protocol"),
        # FeatureUpdate.equals: this.upgradeType.equals(that.upgradeType), past the level test
        (lambda: FeatureUpdate(max_version_level=1, upgrade_type=N)
         == FeatureUpdate(max_version_level=1, upgrade_type=N), "upgrade_type"),
        # MemberToRemove.equals: this.groupInstanceId.equals(...)
        (lambda: MemberToRemove(group_instance_id=N) == MemberToRemove(group_instance_id=N),
         "group_instance_id"),
    ]
    for call, field in cases:
        with pytest.raises(TypeError) as e:
            call()
        assert str(e.value) == f"{field} must not be None"
    # Where Java does not reach the field: a different level, a null-safe hashCode.
    assert FeatureUpdate(max_version_level=1, upgrade_type=N) != FeatureUpdate(
        max_version_level=2, upgrade_type=N)
    assert hash(MemberToRemove(group_instance_id=N)) == hash(MemberToRemove(group_instance_id=N))


def test_kafka_metric_without_time() -> None:
    import threading

    from confluent_kafka.common.metrics import KafkaMetric

    class Provider:
        def value(self, config: Any, now: int) -> float:
            return 1.0
    metric = KafkaMetric(lock=threading.Lock(),
                         metric_name=KafkaMetric._snapshot(name="n", group="g", value=1).metric_name(),
                         value_provider=Provider(), config=None, time=N)
    # KafkaMetric.metricValue: time.milliseconds()
    with pytest.raises(TypeError) as e:
        metric.metric_value()
    assert str(e.value) == "time must not be None"


def test_consumer_and_producer_value_classes() -> None:
    from confluent_kafka.consumer import ConsumerRecords, MockConsumer, SubscriptionPattern
    from confluent_kafka.producer import RecordMetadata
    tp = TopicPartition(topic="t", partition=0)
    # ConsumerRecords: Map.copyOf(nextOffsets) rejects a null key or value.
    for offsets in ({tp: N}, {N: OffsetAndMetadata(offset=1)}):
        with pytest.raises(TypeError) as e:
            ConsumerRecords(records={}, next_offsets=offsets)
        assert str(e.value) == "next_offsets must not contain None"
    # RecordMetadata stores a null partition; topic(), partition(), toString() dereference it.
    metadata = RecordMetadata(topic_partition=N, base_offset=0, batch_index=0, timestamp=0,
                              serialized_key_size=0, serialized_value_size=0)
    for call in (metadata.topic, metadata.partition, lambda: str(metadata)):
        with pytest.raises(TypeError) as e:
            call()
        assert str(e.value) == "topic_partition must not be None"
    # SubscriptionPattern stores a null pattern: toString() returns it (None), hashCode()
    # dereferences it, equals is null-safe.
    pattern = SubscriptionPattern(pattern=N)
    assert pattern.pattern() is None and str(pattern) == "None"
    assert pattern == SubscriptionPattern(pattern=N)
    with pytest.raises(TypeError) as h:
        hash(pattern)
    assert str(h.value) == "pattern must not be None"
    # MockConsumer.subscribe: pattern.toString().isEmpty() dereferences the null pattern.
    mock: MockConsumer[bytes, bytes] = MockConsumer(offset_reset_strategy="earliest")
    with pytest.raises(TypeError) as m:
        mock.subscribe(pattern=pattern)
    assert str(m.value) == "SubscriptionPattern.pattern must not be None"


@pytest.mark.parametrize("with_callback", [False, True], ids=["plain", "callback"])
async def test_async_mock_consumer_subscribe_to_a_none_pattern(with_callback: bool) -> None:
    # AsyncMockConsumer: pattern.toString().isEmpty() dereferences the null pattern,
    # after the listener's own check.
    from confluent_kafka.consumer import (
        AsyncMockConsumer,
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
    mock: Any = AsyncMockConsumer(offset_reset_strategy="earliest")
    with pytest.raises(TypeError) as m:
        await mock.subscribe(**kwargs)
    assert str(m.value) == "SubscriptionPattern.pattern must not be None"
