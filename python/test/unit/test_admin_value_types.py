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

"""Tests of the ``confluent_kafka.admin`` value classes and enums.

Java's tests translated: ``ConfigTest``, ``ConsumerGroupDescriptionTest``,
``GroupListingTest``, ``MemberDescriptionTest``, ``NewTopicTest`` and
``ScramMechanismTest`` (all). Skipped, with the reason at the test's place:
``NewTopicTest``'s two ``convertToCreatableTopic`` tests (package-private, so
not generated). ``ConsumerGroupListingTest`` tests a deprecated class, which is
not generated. The other classes have no Java tests; they get construction,
getter, ``__eq__`` / ``__hash__`` / ``__str__`` and error-message checks against
their Java source, plus, per public method, a positional call raising
``TypeError`` and each ``java_forms`` rejection's exact message (CLAUDE.md,
Python Binding Conventions, Tests and typing).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka.admin import (
    AbortTransactionSpec,
    AlterConfigOp,
    ClassicGroupDescription,
    Config,
    ConfigEntry,
    ConsumerGroupDescription,
    DeletedRecords,
    FeatureMetadata,
    FeatureUpdate,
    FinalizedVersionRange,
    GroupListing,
    ListConsumerGroupOffsetsSpec,
    LogDirDescription,
    MemberAssignment,
    MemberDescription,
    MemberToRemove,
    NewPartitionReassignment,
    NewPartitions,
    NewTopic,
    OffsetSpec,
    PartitionReassignment,
    ProducerState,
    RecordsToDelete,
    ReplicaInfo,
    ScramCredentialInfo,
    ScramMechanism,
    SupportedVersionRange,
    TopicDescription,
    TopicListing,
    TransactionDescription,
    TransactionListing,
    TransactionState,
    UserScramCredentialAlteration,
    UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
    UserScramCredentialsDescription,
)
from confluent_kafka.common import GroupState, GroupType, Node, TopicPartition, Uuid

# --------------------------------------------------------------------------
# ConfigTest

E1 = ConfigEntry(name="a", value="b")
E2 = ConfigEntry(name="c", value="d")


@pytest.fixture
def config() -> Config:
    return Config(entries=[E1, E2])


def test_should_get_entry(config: Config) -> None:
    assert config.get(name="a") == E1
    assert config.get(name="c") == E2


def test_should_return_null_on_get_unknown_entry(config: Config) -> None:
    assert config.get(name="unknown") is None


def test_should_get_all_entries(config: Config) -> None:
    assert len(config.entries()) == 2
    assert E1 in config.entries()
    assert E2 in config.entries()


def test_should_implement_equals_properly(config: Config) -> None:
    assert config == config
    assert config == Config(entries=config.entries())
    assert Config(entries=[E1]) != config
    assert "this" != config


def test_should_implement_hash_code_properly(config: Config) -> None:
    assert hash(config) == hash(config)
    assert hash(config) == hash(Config(entries=config.entries()))
    assert hash(Config(entries=[E1])) != hash(config)


def test_should_implement_to_string_properly(config: Config) -> None:
    assert str(E1) in str(config)
    assert str(E2) in str(config)


def test_hash_code_and_equals_with_null() -> None:
    # Java's test passes null where the types say non-null; the entry stores it.
    ce0 = ConfigEntry(name="abc", value=None, source=None,  # type: ignore[call-overload]
                      is_sensitive=False, is_read_only=False, synonyms=None, type=None,
                      documentation=None)
    ce1 = ConfigEntry(name="abc", value=None, source=None,  # type: ignore[call-overload]
                      is_sensitive=False, is_read_only=False, synonyms=None, type=None,
                      documentation=None)
    assert ce0 == ce1
    assert hash(ce0) == hash(ce1)


def test_equals() -> None:
    ce0 = ConfigEntry(name="abc", value=None,  # type: ignore[call-overload]
                      source=ConfigEntry.ConfigSource.DEFAULT_CONFIG,
                      is_sensitive=False, is_read_only=False, synonyms=None, type=None,
                      documentation=None)
    ce1 = ConfigEntry(name="abc", value=None,  # type: ignore[call-overload]
                      source=ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
                      is_sensitive=False, is_read_only=False, synonyms=None, type=None,
                      documentation=None)
    assert ce0 != ce1


# --------------------------------------------------------------------------
# ConsumerGroupDescriptionTest


def test_group_state() -> None:
    for group_state in GroupState:
        description = ConsumerGroupDescription(
            group_id="groupId", is_simple_consumer_group=False, members=set(),
            partition_assignor="assignor", type=GroupType.CONSUMER, group_state=group_state,
            coordinator=None, authorized_operations=set(), group_epoch=None,
            target_assignment_epoch=None)
        assert description.group_state() is group_state


# --------------------------------------------------------------------------
# GroupListingTest

GROUP_ID = "mygroup"


def test_simple_consumer_group() -> None:
    gl = GroupListing(group_id=GROUP_ID, type=GroupType.CLASSIC, protocol="",
                      group_state=GroupState.EMPTY)
    assert gl.is_simple_consumer_group()
    # ConsumerProtocol.PROTOCOL_TYPE
    gl = GroupListing(group_id=GROUP_ID, type=GroupType.CLASSIC, protocol="consumer",
                      group_state=GroupState.STABLE)
    assert not gl.is_simple_consumer_group()
    gl = GroupListing(group_id=GROUP_ID, type=GroupType.CONSUMER, protocol="",
                      group_state=GroupState.EMPTY)
    assert not gl.is_simple_consumer_group()
    gl = GroupListing(group_id=GROUP_ID, type=None, protocol="", group_state=None)
    assert not gl.is_simple_consumer_group()


# --------------------------------------------------------------------------
# MemberDescriptionTest

MEMBER_ID = "member_id"
INSTANCE_ID = "instanceId"
RACK_ID = "rackId"
CLIENT_ID = "client_id"
HOST = "host"
ASSIGNMENT = MemberAssignment(topic_partitions={TopicPartition(topic="topic", partition=1)})


def _member(**overrides: Any) -> MemberDescription:
    kwargs: dict[str, Any] = dict(
        member_id=MEMBER_ID, group_instance_id=INSTANCE_ID, rack_id=RACK_ID,
        client_id=CLIENT_ID, host=HOST, assignment=ASSIGNMENT, target_assignment=None,
        member_epoch=None, upgraded=None)
    kwargs.update(overrides)
    return MemberDescription(**kwargs)


STATIC_MEMBER_DESCRIPTION = _member()


def test_equals_without_group_instance_id() -> None:
    dynamic_member_description = _member(group_instance_id=None, rack_id=None)
    identity_description = _member(group_instance_id=None, rack_id=None)
    assert STATIC_MEMBER_DESCRIPTION != dynamic_member_description
    assert hash(STATIC_MEMBER_DESCRIPTION) != hash(dynamic_member_description)
    # Check self equality.
    assert dynamic_member_description == dynamic_member_description
    assert dynamic_member_description == identity_description
    assert hash(dynamic_member_description) == hash(identity_description)


def test_equals_with_group_instance_id() -> None:
    # Check self equality.
    assert STATIC_MEMBER_DESCRIPTION == STATIC_MEMBER_DESCRIPTION
    identity_description = _member()
    assert STATIC_MEMBER_DESCRIPTION == identity_description
    assert hash(STATIC_MEMBER_DESCRIPTION) == hash(identity_description)


def test_non_equal() -> None:
    for overrides in (dict(member_id="new_member"), dict(group_instance_id="new_instance"),
                      dict(target_assignment=ASSIGNMENT), dict(member_epoch=1),
                      dict(upgraded=False)):
        other = _member(**overrides)
        assert STATIC_MEMBER_DESCRIPTION != other
        assert hash(STATIC_MEMBER_DESCRIPTION) != hash(other)


def test_member_description_null_defaults_and_str() -> None:
    m = MemberDescription(member_id=None, group_instance_id=None, rack_id=None, client_id=None,
                          host=None, assignment=None, target_assignment=None,
                          member_epoch=None, upgraded=None)
    assert (m.consumer_id(), m.client_id(), m.host()) == ("", "", "")
    assert m.assignment() == MemberAssignment(topic_partitions=set())
    assert str(m) == ("(memberId=, groupInstanceId=None, rackId=None, clientId=, host=, "
                      "assignment=(topicPartitions=), targetAssignment=Optional.empty, "
                      "memberEpoch=None, upgraded=None)")
    m = _member(target_assignment=ASSIGNMENT, member_epoch=3, upgraded=True)
    assert str(m) == ("(memberId=member_id, groupInstanceId=instanceId, rackId=rackId, "
                      "clientId=client_id, host=host, assignment=(topicPartitions=topic-1), "
                      "targetAssignment=Optional[(topicPartitions=topic-1)], memberEpoch=3, "
                      "upgraded=True)")


# --------------------------------------------------------------------------
# NewTopicTest

TEST_TOPIC = "testtopic"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1
CLEANUP_POLICY_CONFIG_KEY = "cleanup.policy"
CLEANUP_POLICY_CONFIG_VALUE = "compact"
BROKER_IDS = [1, 2]
NO_NUM_PARTITIONS = -1  # CreateTopicsRequest.NO_NUM_PARTITIONS
NO_REPLICATION_FACTOR = -1  # CreateTopicsRequest.NO_REPLICATION_FACTOR


def test_constructor_with_partitions_and_replication_factor() -> None:
    topic = NewTopic(name=TEST_TOPIC, num_partitions=NUM_PARTITIONS,
                     replication_factor=REPLICATION_FACTOR)
    assert topic.name() == TEST_TOPIC
    assert topic.num_partitions() == NUM_PARTITIONS
    assert topic.replication_factor() == REPLICATION_FACTOR
    assert topic.replicas_assignments() is None


def test_constructor_with_optional_values() -> None:
    topic = NewTopic(name=TEST_TOPIC, num_partitions=None, replication_factor=None)
    assert topic.name() == TEST_TOPIC
    assert topic.num_partitions() == NO_NUM_PARTITIONS
    assert topic.replication_factor() == NO_REPLICATION_FACTOR
    assert topic.replicas_assignments() is None


def test_constructor_with_replicas_assignments() -> None:
    replicas_assignments = {0: BROKER_IDS}
    new_topic = NewTopic(name=TEST_TOPIC, replicas_assignments=replicas_assignments)
    assert new_topic.name() == TEST_TOPIC
    assert new_topic.num_partitions() == NO_NUM_PARTITIONS
    assert new_topic.replication_factor() == NO_REPLICATION_FACTOR
    assert new_topic.replicas_assignments() == replicas_assignments


def test_configs_not_null() -> None:
    new_topic = NewTopic(name=TEST_TOPIC, num_partitions=NUM_PARTITIONS,
                         replication_factor=REPLICATION_FACTOR)
    configs = {CLEANUP_POLICY_CONFIG_KEY: CLEANUP_POLICY_CONFIG_VALUE}
    assert new_topic.configs(configs=configs) is new_topic
    assert new_topic.configs() == configs
    new_topic.configs(configs=None)
    assert new_topic.configs() is None


def test_configs_null() -> None:
    new_topic = NewTopic(name=TEST_TOPIC, num_partitions=NUM_PARTITIONS,
                         replication_factor=REPLICATION_FACTOR)
    assert new_topic.configs() is None


def test_unmodifiable_replicas_assignments() -> None:
    # Java's view throws UnsupportedOperationException on put; the Python getter
    # returns a copy, so changing it leaves the topic unchanged.
    new_topic = NewTopic(name=TEST_TOPIC, replicas_assignments={0: BROKER_IDS})
    returned_assignments = new_topic.replicas_assignments()
    assert returned_assignments is not None
    returned_assignments[1] = [3, 4]
    assert new_topic.replicas_assignments() == {0: BROKER_IDS}


@pytest.mark.skip(reason="convertToCreatableTopic() is package-private, so not generated")
def test_convert_to_creatable_topic_with_partitions_and_replication_factor() -> None:
    pass


@pytest.mark.skip(reason="convertToCreatableTopic() is package-private, so not generated")
def test_convert_to_creatable_topic_with_replicas_assignments() -> None:
    pass


def test_to_string() -> None:
    topic1 = NewTopic(name=TEST_TOPIC, num_partitions=NUM_PARTITIONS,
                      replication_factor=REPLICATION_FACTOR)
    expected1 = ("(name=" + TEST_TOPIC + ", numPartitions=" + str(NUM_PARTITIONS)
                 + ", replicationFactor=" + str(REPLICATION_FACTOR)
                 + ", replicasAssignments=None, configs=None)")
    assert str(topic1) == expected1
    topic1.configs(configs={CLEANUP_POLICY_CONFIG_KEY: CLEANUP_POLICY_CONFIG_VALUE})
    expected2 = ("(name=" + TEST_TOPIC + ", numPartitions=" + str(NUM_PARTITIONS)
                 + ", replicationFactor=" + str(REPLICATION_FACTOR)
                 + ", replicasAssignments=None, configs="
                 + "{" + CLEANUP_POLICY_CONFIG_KEY + "=" + CLEANUP_POLICY_CONFIG_VALUE + "})")
    assert str(topic1) == expected2
    topic2 = NewTopic(name=TEST_TOPIC, replicas_assignments={0: BROKER_IDS})
    expected3 = ("(name=" + TEST_TOPIC + ", numPartitions=default"
                 + ", replicationFactor=default, replicasAssignments="
                 + "{0=[1, 2]}" + ", configs=None)")
    assert str(topic2) == expected3


def test_equals_and_hash_code() -> None:
    topic1 = NewTopic(name=TEST_TOPIC, num_partitions=NUM_PARTITIONS,
                      replication_factor=REPLICATION_FACTOR)
    topic2 = NewTopic(name=TEST_TOPIC, num_partitions=NUM_PARTITIONS,
                      replication_factor=REPLICATION_FACTOR)
    topic3 = NewTopic(name="another-topic", num_partitions=NUM_PARTITIONS,
                      replication_factor=REPLICATION_FACTOR)
    assert topic1 == topic2
    assert topic1 != topic3
    assert hash(topic1) == hash(topic2)
    assert hash(topic1) != hash(topic3)


def test_new_topic_forms() -> None:
    with pytest.raises(IllegalArgumentError) as e:
        NewTopic(name=TEST_TOPIC, num_partitions=1)  # type: ignore[call-overload]
    assert str(e.value) == (
        "NewTopic() takes one of (name, num_partitions, replication_factor), "
        "(name, replicas_assignments); got (name, num_partitions)")
    with pytest.raises(TypeError):
        NewTopic(TEST_TOPIC, 1, 1)  # type: ignore[call-overload]


# --------------------------------------------------------------------------
# ScramMechanismTest


def test_from_mechanism_name() -> None:
    assert ScramMechanism.from_mechanism_name(mechanism_name="UNKNOWN") is ScramMechanism.UNKNOWN
    assert (ScramMechanism.from_mechanism_name(mechanism_name="SCRAM-SHA-256")
            is ScramMechanism.SCRAM_SHA_256)
    assert (ScramMechanism.from_mechanism_name(mechanism_name="SCRAM-SHA-512")
            is ScramMechanism.SCRAM_SHA_512)
    assert (ScramMechanism.from_mechanism_name(mechanism_name="some string")
            is ScramMechanism.UNKNOWN)
    assert (ScramMechanism.from_mechanism_name(mechanism_name="scram-sha-256")
            is ScramMechanism.UNKNOWN)


# --------------------------------------------------------------------------
# Classes without Java tests: checks against their Java source.


def test_enums() -> None:
    assert [int(m) for m in ScramMechanism] == [0, 1, 2]
    assert ScramMechanism.from_type(type=2) is ScramMechanism.SCRAM_SHA_512
    assert ScramMechanism.from_type(type=9) is ScramMechanism.UNKNOWN
    assert ScramMechanism.SCRAM_SHA_256.type() == 1
    assert str(ScramMechanism.SCRAM_SHA_256) == "SCRAM_SHA_256"
    assert [int(op) for op in AlterConfigOp.OpType] == [0, 1, 2, 3]
    assert AlterConfigOp.OpType.for_id(id=3) is AlterConfigOp.OpType.SUBTRACT
    assert AlterConfigOp.OpType.for_id(id=7) is None
    assert AlterConfigOp.OpType.APPEND.id() == 2
    assert str(AlterConfigOp.OpType.DELETE) == "DELETE"
    assert [int(t) for t in FeatureUpdate.UpgradeType] == [0, 1, 2, 3]
    assert FeatureUpdate.UpgradeType.from_code(code=2) is FeatureUpdate.UpgradeType.SAFE_DOWNGRADE
    assert FeatureUpdate.UpgradeType.from_code(code=0) is FeatureUpdate.UpgradeType.UNKNOWN
    assert FeatureUpdate.UpgradeType.UPGRADE.code() == 1
    assert str(TransactionState.PREPARE_EPOCH_FENCE) == "PrepareEpochFence"
    assert TransactionState.parse(name="CompleteCommit") is TransactionState.COMPLETE_COMMIT
    assert TransactionState.parse(name="nope") is TransactionState.UNKNOWN
    assert TransactionState.ONGOING.value == "ONGOING"
    assert [s.name for s in ConfigEntry.ConfigSource][-1] == "UNKNOWN"
    assert len(ConfigEntry.ConfigType) == 10
    assert str(ConfigEntry.ConfigType.PASSWORD) == "PASSWORD"


def test_config_entry() -> None:
    entry = ConfigEntry(name="n", value="v")
    assert entry.source() is ConfigEntry.ConfigSource.UNKNOWN
    assert entry.type() is ConfigEntry.ConfigType.UNKNOWN
    assert entry.synonyms() == []
    assert (entry.is_sensitive(), entry.is_read_only(), entry.is_default()) == (
        False, False, False)
    assert entry.documentation() is None
    assert str(entry) == ("ConfigEntry(name=n, value=v, source=UNKNOWN, isSensitive=False, "
                          "isReadOnly=False, synonyms=[], type=UNKNOWN, documentation=None)")
    synonym = ConfigEntry.ConfigSynonym._new(
        name="s", value=None, source=ConfigEntry.ConfigSource.DEFAULT_CONFIG)
    secret = ConfigEntry(name="n", value="v", source=ConfigEntry.ConfigSource.DEFAULT_CONFIG,
                         is_sensitive=True, is_read_only=True, synonyms=[synonym],
                         type=ConfigEntry.ConfigType.PASSWORD, documentation="d")
    assert secret.is_default()
    assert secret.synonyms() == [synonym]
    assert str(secret) == (
        "ConfigEntry(name=n, value=Redacted, source=DEFAULT_CONFIG, isSensitive=True, "
        "isReadOnly=True, synonyms=[ConfigSynonym(name=s, value=None, "
        "source=DEFAULT_CONFIG)], type=PASSWORD, documentation=d)")
    assert synonym == ConfigEntry.ConfigSynonym._new(
        name="s", value=None, source=ConfigEntry.ConfigSource.DEFAULT_CONFIG)
    with pytest.raises(TypeError) as t:
        ConfigEntry.ConfigSynonym()
    assert str(t.value) == ("ConfigEntry.ConfigSynonym cannot be constructed directly; use "
                            "ConfigEntry.synonyms()")
    with pytest.raises(TypeError) as n:
        ConfigEntry(name=None, value="v")  # type: ignore[call-overload]
    assert str(n.value) == "name must not be None"
    with pytest.raises(IllegalArgumentError) as e:
        ConfigEntry(name="n", value="v", is_sensitive=True)  # type: ignore[call-overload]
    assert str(e.value) == (
        "ConfigEntry() takes one of (name, value), (name, value, source, is_sensitive, "
        "is_read_only, synonyms, type, documentation); got (name, value, is_sensitive)")
    with pytest.raises(TypeError):
        ConfigEntry("n", "v")  # type: ignore[call-overload]


def test_alter_config_op() -> None:
    op = AlterConfigOp(config_entry=E1, operation_type=AlterConfigOp.OpType.SET)
    assert op.config_entry() is E1
    assert op.op_type() is AlterConfigOp.OpType.SET
    assert op == AlterConfigOp(config_entry=E1, operation_type=AlterConfigOp.OpType.SET)
    assert hash(op) == hash(AlterConfigOp(config_entry=E1,
                                          operation_type=AlterConfigOp.OpType.SET))
    assert op != AlterConfigOp(config_entry=E1, operation_type=AlterConfigOp.OpType.DELETE)
    assert str(op) == "AlterConfigOp{opType=SET, configEntry=" + str(E1) + "}"


def test_version_ranges_and_feature_update() -> None:
    r = FinalizedVersionRange(min_version_level=1, max_version_level=2)
    assert (r.min_version_level(), r.max_version_level()) == (1, 2)
    assert r == FinalizedVersionRange(min_version_level=1, max_version_level=2)
    assert str(r) == "FinalizedVersionRange[min_version_level:1, max_version_level:2]"
    with pytest.raises(IllegalArgumentError) as e:
        FinalizedVersionRange(min_version_level=2, max_version_level=1)
    assert str(e.value) == ("Expected minVersionLevel >= 0, maxVersionLevel >= 0 and "
                            "maxVersionLevel >= minVersionLevel, but received "
                            "minVersionLevel: 2, maxVersionLevel: 1")
    s = SupportedVersionRange(min_version=0, max_version=3)
    assert (s.min_version(), s.max_version()) == (0, 3)
    assert hash(s) == hash(SupportedVersionRange(min_version=0, max_version=3))
    assert str(s) == "SupportedVersionRange[min_version:0, max_version:3]"
    with pytest.raises(IllegalArgumentError) as e:
        SupportedVersionRange(min_version=-1, max_version=3)
    assert str(e.value) == ("Expected 0 <= minVersion <= maxVersion but received "
                            "minVersion:-1, maxVersion:3.")
    u = FeatureUpdate(max_version_level=0, upgrade_type=FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)
    assert u.upgrade_type() is FeatureUpdate.UpgradeType.SAFE_DOWNGRADE
    assert str(u) == "FeatureUpdate{maxVersionLevel:0, upgradeType:SAFE_DOWNGRADE}"
    assert u == FeatureUpdate(max_version_level=0,
                              upgrade_type=FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)
    with pytest.raises(IllegalArgumentError) as e:
        FeatureUpdate(max_version_level=0, upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)
    assert str(e.value) == ("The upgradeType flag should be set to SAFE_DOWNGRADE or "
                            "UNSAFE_DOWNGRADE when the provided maxVersionLevel:0 is < 1.")
    with pytest.raises(IllegalArgumentError) as e:
        FeatureUpdate(max_version_level=-1, upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)
    assert str(e.value) == "Cannot specify a negative version level."
    m = FeatureMetadata._new(finalized_features={"f": r}, finalized_features_epoch=None,
                             supported_features={"f": s})
    assert m.finalized_features() == {"f": r}
    assert m.finalized_features_epoch() is None
    assert m.supported_features() == {"f": s}
    assert str(m) == ("FeatureMetadata{finalizedFeatures:{(f -> " + str(r) + ")}, "
                      "finalizedFeaturesEpoch:<none>, supportedFeatures:{(f -> " + str(s)
                      + ")}}")
    assert hash(m) == hash(FeatureMetadata._new(
        finalized_features={"f": r}, finalized_features_epoch=None, supported_features={"f": s}))
    with pytest.raises(TypeError) as t:
        FeatureMetadata()
    assert str(t.value) == ("FeatureMetadata cannot be constructed directly; use "
                            "DescribeFeaturesResult.feature_metadata()")


def test_list_consumer_group_offsets_spec() -> None:
    tp = TopicPartition(topic="t", partition=0)
    spec = ListConsumerGroupOffsetsSpec()
    assert spec.topic_partitions() is None
    assert spec.topic_partitions(topic_partitions=[tp]) is spec
    assert spec.topic_partitions() == [tp]
    assert spec == ListConsumerGroupOffsetsSpec(topic_partitions=[tp])
    assert hash(spec) == hash(ListConsumerGroupOffsetsSpec(topic_partitions=[tp]))
    assert str(spec) == "ListConsumerGroupOffsetsSpec(topicPartitions=[t-0])"
    spec.topic_partitions(topic_partitions=None)
    assert spec.topic_partitions() is None
    stub = (Path(__file__).parents[2] / "confluent_kafka" / "admin"
            / "list_consumer_group_offsets_spec.pyi").read_text()
    # The stub shows the field's Java starting value (null).
    assert "topic_partitions: Iterable[TopicPartition] | None = None) -> None" in stub
    assert ListConsumerGroupOffsetsSpec(topic_partitions=None) == ListConsumerGroupOffsetsSpec()
    with pytest.raises(TypeError):
        spec.topic_partitions([tp])  # type: ignore[call-overload]


def test_static_factories_and_private_constructors() -> None:
    r = RecordsToDelete.before_offset(offset=5)
    assert r.before_offset() == 5
    assert r == RecordsToDelete.before_offset(offset=5)
    assert hash(r) == hash(RecordsToDelete.before_offset(offset=5))
    assert str(r) == "(beforeOffset = 5)"
    with pytest.raises(TypeError) as t:
        RecordsToDelete()
    assert str(t.value) == ("RecordsToDelete cannot be constructed directly; use "
                            "RecordsToDelete.before_offset(...)")
    p = NewPartitions.increase_to(total_count=6)
    assert (p.total_count(), p.assignments()) == (6, None)
    assert str(p) == "(totalCount=6, newAssignments=None)"
    p = NewPartitions.increase_to(total_count=6, new_assignments=[[1, 2], [2, 3], [3, 1]])
    assert p.assignments() == [[1, 2], [2, 3], [3, 1]]
    assert str(p) == "(totalCount=6, newAssignments=[[1, 2], [2, 3], [3, 1]])"
    with pytest.raises(TypeError) as t:
        NewPartitions()
    assert str(t.value) == ("NewPartitions cannot be constructed directly; use "
                            "NewPartitions.increase_to(...)")
    with pytest.raises(TypeError):
        NewPartitions.increase_to(6)  # type: ignore[call-arg]


def test_offset_spec() -> None:
    cases = [(OffsetSpec.latest(), OffsetSpec.LatestSpec),
             (OffsetSpec.earliest(), OffsetSpec.EarliestSpec),
             (OffsetSpec.max_timestamp(), OffsetSpec.MaxTimestampSpec),
             (OffsetSpec.earliest_local(), OffsetSpec.EarliestLocalSpec),
             (OffsetSpec.latest_tiered(), OffsetSpec.LatestTieredSpec),
             (OffsetSpec.earliest_pending_upload(), OffsetSpec.EarliestPendingUploadSpec)]
    for spec, cls in cases:
        assert type(spec) is cls
        assert isinstance(spec, OffsetSpec)
        assert cls.__qualname__ == "OffsetSpec." + cls.__name__
        assert isinstance(cls(), OffsetSpec)
    ts = OffsetSpec.for_timestamp(timestamp=1234)
    assert isinstance(ts, OffsetSpec.TimestampSpec)
    assert ts._timestamp == 1234
    assert isinstance(OffsetSpec(), OffsetSpec)
    with pytest.raises(TypeError) as t:
        OffsetSpec.TimestampSpec()
    assert str(t.value) == ("OffsetSpec.TimestampSpec cannot be constructed directly; use "
                            "OffsetSpec.for_timestamp(...)")
    assert OffsetSpec.latest() != OffsetSpec.latest()  # no Java equals: identity


def test_scram_credentials() -> None:
    info = ScramCredentialInfo(mechanism=ScramMechanism.SCRAM_SHA_256, iterations=4096)
    assert (info.mechanism(), info.iterations()) == (ScramMechanism.SCRAM_SHA_256, 4096)
    assert info == ScramCredentialInfo(mechanism=ScramMechanism.SCRAM_SHA_256, iterations=4096)
    assert str(info) == "ScramCredentialInfo{mechanism=SCRAM_SHA_256, iterations=4096}"
    with pytest.raises(TypeError) as none:
        ScramCredentialInfo(mechanism=None, iterations=1)  # type: ignore[arg-type]
    assert str(none.value) == "mechanism must not be None"
    with pytest.raises(TypeError) as t:
        UserScramCredentialAlteration(user="u")
    assert str(t.value) == ("UserScramCredentialAlteration cannot be constructed directly; use "
                            "one of its subclasses")
    d = UserScramCredentialDeletion(user="u", mechanism=ScramMechanism.SCRAM_SHA_512)
    assert (d.user(), d.mechanism()) == ("u", ScramMechanism.SCRAM_SHA_512)
    assert isinstance(d, UserScramCredentialAlteration)
    with pytest.raises(TypeError) as none:
        UserScramCredentialDeletion(user=None, mechanism=ScramMechanism.UNKNOWN)  # type: ignore[arg-type]
    assert str(none.value) == "user must not be None"
    up = UserScramCredentialUpsertion(user="u", credential_info=info, password="pässword")
    assert up.password() == "pässword".encode()
    salt = up.salt()
    assert salt and all(c in b"0123456789abcdefghijklmnopqrstuvwxyz" for c in salt)
    assert up.salt() != UserScramCredentialUpsertion(
        user="u", credential_info=info, password=b"p").salt()
    up = UserScramCredentialUpsertion(user="u", credential_info=info, password=b"p", salt=b"s")
    assert (up.user(), up.credential_info(), up.password(), up.salt()) == ("u", info, b"p", b"s")
    with pytest.raises(TypeError):
        UserScramCredentialUpsertion(user="u", credential_info=info, password="p",  # type: ignore[call-overload]
                                     salt=b"s")
    desc = UserScramCredentialsDescription(name="u", credential_infos=[info])
    assert (desc.name(), desc.credential_infos()) == ("u", [info])
    assert desc == UserScramCredentialsDescription(name="u", credential_infos=[info])
    assert hash(desc) == hash(UserScramCredentialsDescription(name="u", credential_infos=[info]))
    assert str(desc) == ("UserScramCredentialsDescription{name='u', credentialInfos=["
                         + str(info) + "]}")


def test_transactions_and_producers() -> None:
    tp = TopicPartition(topic="t", partition=1)
    spec = AbortTransactionSpec(topic_partition=tp, producer_id=5, producer_epoch=1,
                                coordinator_epoch=2)
    assert (spec.topic_partition(), spec.producer_id(), spec.producer_epoch(),
            spec.coordinator_epoch()) == (tp, 5, 1, 2)
    assert spec == AbortTransactionSpec(topic_partition=tp, producer_id=5, producer_epoch=1,
                                        coordinator_epoch=2)
    assert str(spec) == ("AbortTransactionSpec(topicPartition=t-1, producerId=5, "
                         "producerEpoch=1, coordinatorEpoch=2)")
    listing = TransactionListing(transactional_id="x", producer_id=7,
                                 transaction_state=TransactionState.ONGOING)
    assert (listing.transactional_id(), listing.producer_id(), listing.state()) == (
        "x", 7, TransactionState.ONGOING)
    assert str(listing) == ("TransactionListing(transactionalId='x', producerId=7, "
                            "transactionState=Ongoing)")
    desc = TransactionDescription(coordinator_id=1, state=TransactionState.EMPTY, producer_id=2,
                                  producer_epoch=3, transaction_timeout_ms=4,
                                  transaction_start_time_ms=None, topic_partitions={tp})
    assert desc.topic_partitions() == {tp}
    assert desc.transaction_start_time_ms() is None
    assert str(desc) == ("TransactionDescription(coordinatorId=1, state=Empty, producerId=2, "
                         "producerEpoch=3, transactionTimeoutMs=4, "
                         "transactionStartTimeMs=OptionalLong.empty, topicPartitions=[t-1])")
    assert hash(desc) == hash(TransactionDescription(
        coordinator_id=1, state=TransactionState.EMPTY, producer_id=2, producer_epoch=3,
        transaction_timeout_ms=4, transaction_start_time_ms=None, topic_partitions=[tp]))
    ps = ProducerState(producer_id=1, producer_epoch=2, last_sequence=3, last_timestamp=4,
                       coordinator_epoch=5, current_transaction_start_offset=None)
    assert (ps.coordinator_epoch(), ps.current_transaction_start_offset()) == (5, None)
    assert str(ps) == ("ProducerState(producerId=1, producerEpoch=2, lastSequence=3, "
                       "lastTimestamp=4, coordinatorEpoch=OptionalInt[5], "
                       "currentTransactionStartOffset=OptionalLong.empty)")
    assert ps == ProducerState(producer_id=1, producer_epoch=2, last_sequence=3,
                               last_timestamp=4, coordinator_epoch=5,
                               current_transaction_start_offset=None)


def test_topics_partitions_and_log_dirs() -> None:
    uuid = Uuid(most_sig_bits=1, least_sig_bits=2)
    listing = TopicListing(name="t", topic_id=uuid, internal=False)
    assert (listing.name(), listing.topic_id(), listing.is_internal()) == ("t", uuid, False)
    assert str(listing) == "(name=t, topicId=" + str(uuid) + ", internal=False)"
    d = TopicDescription(name="t", internal=True, partitions=[])
    assert d.authorized_operations() == set()
    assert d.topic_id() == Uuid.ZERO_UUID
    assert str(d) == "(name=t, internal=True, partitions=, authorizedOperations=[])"
    d2 = TopicDescription(name="t", internal=True, partitions=[], authorized_operations=None,
                          topic_id=uuid)
    assert d2.authorized_operations() is None
    assert d == TopicDescription(name="t", internal=True, partitions=[],
                                 authorized_operations=set(), topic_id=uuid)  # no topicId
    with pytest.raises(IllegalArgumentError) as e:
        TopicDescription(name="t", internal=True, partitions=[],  # type: ignore[call-overload]
                         topic_id=uuid)
    assert str(e.value) == (
        "TopicDescription() takes one of (name, internal, partitions), (name, internal, "
        "partitions, authorized_operations), (name, internal, partitions, "
        "authorized_operations, topic_id); got (name, internal, partitions, topic_id)")
    reassignment = NewPartitionReassignment(target_replicas=[1, 2])
    assert reassignment.target_replicas() == [1, 2]
    with pytest.raises(IllegalArgumentError) as e:
        NewPartitionReassignment(target_replicas=[])
    assert str(e.value) == "Cannot create a new partition reassignment without any replicas"
    pr = PartitionReassignment(replicas=[1, 2], adding_replicas=[2], removing_replicas=[])
    assert (pr.replicas(), pr.adding_replicas(), pr.removing_replicas()) == ([1, 2], [2], [])
    assert str(pr) == ("PartitionReassignment(replicas=[1, 2], addingReplicas=[2], "
                       "removingReplicas=[])")
    assert DeletedRecords(low_watermark=9).low_watermark() == 9
    info = ReplicaInfo(size=10, offset_lag=1, is_future=True)
    assert (info.size(), info.offset_lag(), info.is_future()) == (10, 1, True)
    assert str(info) == "ReplicaInfo(size=10, offsetLag=1, isFuture=True)"
    tp = TopicPartition(topic="t", partition=0)
    ld = LogDirDescription(error=None, replica_infos={tp: info})
    assert (ld.error(), ld.total_bytes(), ld.usable_bytes()) == (None, None, None)
    assert ld.replica_infos() == {tp: info}
    ld = LogDirDescription(error=None, replica_infos={}, total_bytes=100, usable_bytes=-1)
    assert (ld.total_bytes(), ld.usable_bytes()) == (100, None)
    assert str(ld) == ("LogDirDescription(replicaInfos={}, error=None, "
                       "totalBytes=OptionalLong[100], usableBytes=OptionalLong.empty, "
                       "isCordoned=False)")
    assert ld.is_cordoned() is False
    ld = LogDirDescription(error=None, replica_infos={}, total_bytes=1, usable_bytes=2,
                           is_cordoned=True)
    assert ld.is_cordoned() is True
    assert str(ld).endswith("isCordoned=True)")
    with pytest.raises(IllegalArgumentError) as e:
        LogDirDescription(error=None, replica_infos={}, total_bytes=1)  # type: ignore[call-overload]
    assert str(e.value) == (
        "LogDirDescription() takes one of (error, replica_infos), (error, replica_infos, "
        "total_bytes, usable_bytes), (error, replica_infos, total_bytes, usable_bytes, "
        "is_cordoned); got (error, replica_infos, total_bytes)")


def test_groups() -> None:
    tp = TopicPartition(topic="t", partition=0)
    node = Node(id=1, host="h", port=9092)
    member = _member()
    a = MemberAssignment(topic_partitions=[tp])
    assert a.topic_partitions() == {tp}
    assert MemberAssignment(topic_partitions=None).topic_partitions() == set()
    assert str(a) == "(topicPartitions=t-0)"
    assert MemberToRemove(group_instance_id="i").group_instance_id() == "i"
    assert MemberToRemove(group_instance_id="i") == MemberToRemove(group_instance_id="i")
    assert hash(MemberToRemove(group_instance_id="i")) == hash(
        MemberToRemove(group_instance_id="i"))
    c = ConsumerGroupDescription(
        group_id=None, is_simple_consumer_group=True, members=[member], partition_assignor=None,
        type=GroupType.CLASSIC, group_state=GroupState.STABLE, coordinator=node,
        authorized_operations=None, group_epoch=3, target_assignment_epoch=None)
    assert (c.group_id(), c.partition_assignor(), c.members()) == ("", "", [member])
    assert c.authorized_operations() is None
    assert c.coordinator() == node
    assert (c.group_epoch(), c.target_assignment_epoch()) == (3, None)
    assert str(c) == ("(groupId=, isSimpleConsumerGroup=True, members=" + str(member)
                      + ", partitionAssignor=, type=" + str(GroupType.CLASSIC) + ", groupState="
                      + str(GroupState.STABLE) + ", coordinator=" + str(node)
                      + ", authorizedOperations=None, groupEpoch=3, targetAssignmentEpoch=None)")
    assert hash(c) == hash(ConsumerGroupDescription(
        group_id="", is_simple_consumer_group=True, members=[member], partition_assignor="",
        type=GroupType.CLASSIC, group_state=GroupState.STABLE, coordinator=node,
        authorized_operations=None, group_epoch=3, target_assignment_epoch=None))
    from confluent_kafka.common import ClassicGroupState

    cg = ClassicGroupDescription(group_id="g", protocol="", protocol_data=None, members=None,
                                 state=ClassicGroupState.EMPTY, coordinator=None)
    assert cg.is_simple_consumer_group()
    assert (cg.protocol_data(), cg.members(), cg.authorized_operations()) == ("", [], set())
    assert cg == ClassicGroupDescription(group_id="g", protocol="", protocol_data="",
                                         members=[], state=ClassicGroupState.EMPTY,
                                         coordinator=None, authorized_operations=set())
    assert str(cg) == ("(groupId=g, protocol='', protocolData=, members=, state="
                       + str(ClassicGroupState.EMPTY)
                       + ", coordinator=None, authorizedOperations=[])")
    assert ClassicGroupDescription(
        group_id="g", protocol="", protocol_data="", members=[], state=ClassicGroupState.EMPTY,
        coordinator=None, authorized_operations=None).authorized_operations() == set()
    unknown = ClassicGroupDescription._new(
        group_id=None, protocol="", protocol_data=None, members=None,
        state=ClassicGroupState.EMPTY, coordinator=None, authorized_operations=None)
    assert unknown.authorized_operations() is None
    assert (unknown.group_id(), unknown.protocol_data(), unknown.members()) == ("", "", [])
    assert str(unknown).endswith("authorizedOperations=None)")
    assert unknown != cg
    known = ClassicGroupDescription._new(
        group_id="g", protocol="", protocol_data="", members=[], state=ClassicGroupState.EMPTY,
        coordinator=None, authorized_operations=set())
    assert known == cg and hash(known) == hash(cg)
    gl = GroupListing(group_id="g", type=None, protocol="p", group_state=None)
    assert str(gl) == "(groupId='g', type=none, protocol='p', groupState=none)"
    assert gl == GroupListing(group_id="g", type=None, protocol="p", group_state=None)


@pytest.mark.parametrize("call", [
    lambda: Config([E1]),  # type: ignore[call-arg]
    lambda: DeletedRecords(1),  # type: ignore[call-arg]
    lambda: MemberToRemove("i"),  # type: ignore[call-arg]
    lambda: ReplicaInfo(1, 2, False),  # type: ignore[call-arg]
    lambda: Config(entries=[E1]).get("a"),  # type: ignore[call-arg]
    lambda: TransactionState.parse("Empty"),  # type: ignore[call-arg]
    lambda: ScramMechanism.from_type(1),  # type: ignore[call-arg]
    lambda: OffsetSpec.for_timestamp(1),  # type: ignore[call-arg]
])
def test_positional_calls_raise_type_error(call: Any) -> None:
    with pytest.raises(TypeError):
        call()
