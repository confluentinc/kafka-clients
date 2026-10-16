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

"""``MockAdminClient``: Java ships no tests of it, so these translate the Rust core's
tests of its own translation, ``rust/src/admin/mock_admin_client.rs`` (its
``#[cfg(test)]`` module), for the methods of the topics, cluster, configs,
partitions / records, log dirs and close groups, in the Rust file's
order, asserting Java's messages; then the ``Builder``'s keyword forms,
the constructors' forms and the binding's own checks of the other methods
of these groups.

The Rust helper ``admin()`` is ``MockAdminClient::create(3)``, Java's
``MockAdminClient.create().numBrokers(3).build()``. Where the Rust mock reports an
error Java's mock does not (Rust cannot panic across the FFI), the test asserts
Java's behaviour instead and says so:
``create_rejects_zero_brokers_rather_than_fabricating_a_controller`` and
``create_rejects_a_negative_broker_count`` (Java: ``IndexOutOfBoundsException`` /
``IllegalArgumentException`` from ``List.get`` / ``subList``), the two "leader with
no log directories" tests (Java: ``IndexOutOfBoundsException`` thrown by the call,
the empty log-dir list being seeded through ``Builder.brokerLogDirs`` as the Rust
``set_broker_log_dirs`` has no Java counterpart), and the two
``list_partition_reassignments_after_topic_*`` tests (Java: a ``RuntimeException``
and an ``IndexOutOfBoundsException`` thrown by the call, not a failed future).

Skipped: ``mock_set_broker_log_dirs_rejects_an_unknown_broker`` (``set_broker_log_dirs``
is a Rust-only helper, not in Java's mock).

Then the features, delegation-token and ``list_consumer_group_offsets`` tests (the
rest of the 53), with the same adaptations: the Rust ``set_feature_levels`` (not in
Java's mock) is the ``Builder``'s three feature-level setters; the five
``mock_update_features_rejects_*`` tests are one parametrized test; a describe of
every token passes ``owners=[]``, as Java's default ``null`` owners raise
``NullPointerError()`` (its own test); with no renewer
``create_delegation_token`` raises ``IndexError`` from the call (Java's
``renewers().get(0)``), and a negative seeded offset raises ``IllegalArgumentError``
from the call (Java's ``new OffsetAndMetadata(-1)``), where the Rust core fails the
future; that test's spec lists ``[]`` partitions, as Java's default spec (``null``)
raises ``NullPointerError()`` once an offset is stored. Last, the binding's own checks
of the other methods.
"""

from __future__ import annotations

from typing import Any

import pytest

from confluent_kafka.admin import (
    AbortTransactionSpec,
    AlterConfigOp,
    ConfigEntry,
    CreateDelegationTokenOptions,
    DescribeDelegationTokenOptions,
    DescribeFeaturesOptions,
    DescribeUserScramCredentialsOptions,
    ExpireDelegationTokenOptions,
    FeatureUpdate,
    FinalizedVersionRange,
    ListConfigResourcesOptions,
    ListConsumerGroupOffsetsOptions,
    ListConsumerGroupOffsetsSpec,
    ListGroupsOptions,
    MockAdminClient,
    NewPartitionReassignment,
    NewTopic,
    OffsetSpec,
    RecordsToDelete,
    RemoveMembersFromConsumerGroupOptions,
    RenewDelegationTokenOptions,
    SupportedVersionRange,
    UpdateFeaturesOptions,
)
from confluent_kafka.common import (
    AclBindingFilter,
    ClientQuotaFilter,
    DelegationToken,
    ElectionType,
    GroupState,
    GroupType,
    KafkaPrincipal,
    Node,
    TopicCollection,
    TopicPartition,
    TopicPartitionInfo,
    TopicPartitionReplica,
    Uuid,
)
from confluent_kafka.common.config import ConfigResource
from confluent_kafka.common.errors.delegation_token_not_found_error import (
    DelegationTokenNotFoundError,
)
from confluent_kafka.common.errors.invalid_principal_type_error import InvalidPrincipalTypeError
from confluent_kafka.common.errors.invalid_replication_factor_error import (
    InvalidReplicationFactorError,
)
from confluent_kafka.common.errors.invalid_request_error import InvalidRequestError
from confluent_kafka.common.errors.kafka_storage_error import KafkaStorageError
from confluent_kafka.common.errors.replica_not_available_error import ReplicaNotAvailableError
from confluent_kafka.common.errors.timeout_error import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.errors.topic_exists_error import TopicExistsError
from confluent_kafka.common.errors.unknown_topic_id_error import UnknownTopicIdError
from confluent_kafka.common.errors.unknown_topic_or_partition_error import (
    UnknownTopicOrPartitionError,
)
from confluent_kafka.common.errors.unsupported_version_error import UnsupportedVersionError
from confluent_kafka.consumer import OffsetAndMetadata
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.null_pointer_error import NullPointerError


def admin(num_brokers: int = 3) -> MockAdminClient:
    return MockAdminClient.create().num_brokers(num_brokers=num_brokers).build()


def new_topic(name: str, partitions: int, replication_factor: int) -> NewTopic:
    return NewTopic(name=name, num_partitions=partitions, replication_factor=replication_factor)


def error_of(future: Any) -> BaseException:
    error: BaseException | None = future.exception()
    assert error is not None
    return error


def seeded_broker(id_: int) -> Node:
    return Node(id=id_, host="localhost", port=1000 + id_)


def unknown_broker() -> Node:
    return Node(id=99, host="elsewhere", port=9999)


def partition_info(partition: int, leader: Node | None, replicas: list[Node],
                   isr: list[Node]) -> TopicPartitionInfo:
    return TopicPartitionInfo(partition=partition, leader=leader, replicas=replicas, isr=isr,
                              elr=(), last_known_elr=())


# ---- Builder broker count (MockAdminClient.java:152, :210) --------------------

def test_create_rejects_zero_brokers_rather_than_fabricating_a_controller() -> None:
    # Java's build() reads brokers.get(0) on an empty list (the Rust mock reports
    # "num_brokers must be at least 1, was 0" instead).
    with pytest.raises(IndexError) as e:
        admin(0)
    assert str(e.value) == "Index 0 out of bounds for length 0"


def test_create_rejects_a_negative_broker_count() -> None:
    # Java's numBrokers(-1) fails in brokers.subList(0, -1).
    with pytest.raises(IllegalArgumentError) as e:
        admin(-1)
    assert str(e.value) == "fromIndex(0) > toIndex(-1)"


def test_controller_is_always_one_of_the_seeded_nodes() -> None:
    for num_brokers in range(1, 4):
        described = admin(num_brokers).describe_cluster()
        nodes = described.nodes().result()
        controller = described.controller().result()
        assert controller in nodes
        assert controller.id() == 0
        assert len(nodes) == num_brokers


# ---- Topics -------------------------------------------------------------------

def test_create_then_list_and_describe() -> None:
    client = admin()
    result = client.create_topics(new_topics=[new_topic("t", 2, 2)])
    assert result.all().result() is None
    assert result.num_partitions(topic="t").result() == 2
    assert result.replication_factor(topic="t").result() == 2
    assert "t" in client.list_topics().names().result()
    described = client.describe_topics(
        topics=TopicCollection.of_topic_names(topics=["t"])).all_topic_names()
    assert described is not None
    assert len(described.result()["t"].partitions()) == 2


def test_create_existing_topic_fails_with_topic_exists() -> None:
    client = admin()
    client.create_topics(new_topics=[new_topic("t", 1, 1)]).all().result()
    result = client.create_topics(new_topics=[new_topic("t", 1, 1)])
    error = error_of(result.values()["t"])
    assert isinstance(error, TopicExistsError)
    assert str(error) == "Topic t exists already."


def test_create_with_replication_factor_too_large_fails() -> None:
    client = admin(1)
    result = client.create_topics(new_topics=[new_topic("t", 1, 5)])
    error = error_of(result.values()["t"])
    assert isinstance(error, InvalidReplicationFactorError)
    assert str(error) == "Replication factor: 5 is larger than brokers: 1"


def test_mock_create_topics_rejects_a_leader_with_no_log_directories() -> None:
    # Java: brokerLogDirs.get(0).get(0) throws out of createTopics (the Rust mock
    # fails the future with "Broker 0 has no log directories.").
    client = MockAdminClient.Builder(
        brokers=[seeded_broker(0), seeded_broker(1), seeded_broker(2)],
        broker_log_dirs=[[], ["/a"], ["/b"]]).build()
    with pytest.raises(IndexError) as e:
        client.create_topics(new_topics=[new_topic("t", 1, 1)])
    assert str(e.value) == "Index 0 out of bounds for length 0"


def test_describe_nonexistent_topic_is_unknown_topic() -> None:
    result = admin().describe_topics(topics=TopicCollection.of_topic_names(topics=["missing"]))
    values = result.topic_name_values()
    assert values is not None
    error = error_of(values["missing"])
    assert isinstance(error, UnknownTopicOrPartitionError)
    assert str(error) == "Topic missing not found."


def test_delete_then_gone() -> None:
    client = admin()
    client.create_topics(new_topics=[new_topic("t", 1, 1)]).all().result()
    client.delete_topics(topics=TopicCollection.of_topic_names(topics=["t"])).all().result()
    assert "t" not in client.list_topics().names().result()


def test_delete_missing_topic_fails() -> None:
    result = admin().delete_topics(topics=TopicCollection.of_topic_names(topics=["nope"]))
    values = result.topic_name_values()
    assert values is not None
    error = error_of(values["nope"])
    assert isinstance(error, UnknownTopicOrPartitionError)
    assert str(error) == "Topic nope does not exist."


def test_timeout_next_request_times_out_create() -> None:
    client = admin()
    client.timeout_next_request(number_of_request=1)
    result = client.create_topics(new_topics=[new_topic("t", 1, 1)])
    assert isinstance(error_of(result.values()["t"]), KafkaTimeoutError)
    # Next request succeeds.
    client.create_topics(new_topics=[new_topic("t2", 1, 1)]).all().result()


# ---- Cluster ------------------------------------------------------------------

def test_describe_cluster_returns_brokers_and_controller() -> None:
    result = admin().describe_cluster()
    assert len(result.nodes().result()) == 3
    assert result.controller().result().id() == 0
    assert result.cluster_id().result() == MockAdminClient.DEFAULT_CLUSTER_ID
    assert result.authorized_operations().result() == set()


def test_describe_cluster_timeout_recovers_on_next_call() -> None:
    client = admin()
    client.timeout_next_request(number_of_request=1)
    timed_out = client.describe_cluster()
    assert isinstance(error_of(timed_out.nodes()), KafkaTimeoutError)
    assert isinstance(error_of(timed_out.controller()), KafkaTimeoutError)
    assert isinstance(error_of(timed_out.cluster_id()), KafkaTimeoutError)
    # The counter is decremented, so the next call succeeds.
    assert len(client.describe_cluster().nodes().result()) == 3


# ---- Configs ------------------------------------------------------------------

def topic_resource(name: str) -> ConfigResource:
    return ConfigResource(type=ConfigResource.Type.TOPIC, name=name)


def test_describe_configs_topic_returns_stored_configs() -> None:
    client = admin()
    topic = new_topic("t", 1, 1).configs(configs={"retention.ms": "1000"})
    client.create_topics(new_topics=[topic]).all().result()
    resource = topic_resource("t")
    config = client.describe_configs(resources=[resource]).values()[resource].result()
    entry = config.get(name="retention.ms")
    assert entry is not None and entry.value() == "1000"


def test_describe_configs_broker_returns_default_replication_factor() -> None:
    resource = ConfigResource(type=ConfigResource.Type.BROKER, name="0")
    config = admin().describe_configs(resources=[resource]).values()[resource].result()
    entry = config.get(name="default.replication.factor")
    assert entry is not None and entry.value() == "3"


def test_describe_configs_unknown_topic_is_unknown_topic_error() -> None:
    resource = topic_resource("missing")
    error = error_of(admin().describe_configs(resources=[resource]).values()[resource])
    assert isinstance(error, UnknownTopicOrPartitionError)
    assert str(error) == "Resource ConfigResource(type=TOPIC, name='missing') not found."


def test_describe_configs_unknown_broker_is_invalid_request() -> None:
    resource = ConfigResource(type=ConfigResource.Type.BROKER, name="99")
    error = error_of(admin().describe_configs(resources=[resource]).values()[resource])
    assert isinstance(error, InvalidRequestError)
    assert str(error) == "Broker 99 not found."


def test_describe_configs_timeout_recovers_on_next_call() -> None:
    client = admin()
    client.timeout_next_request(number_of_request=1)
    resource = ConfigResource(type=ConfigResource.Type.BROKER, name="0")
    timed_out = client.describe_configs(resources=[resource])
    assert isinstance(error_of(timed_out.values()[resource]), KafkaTimeoutError)
    client.describe_configs(resources=[resource]).values()[resource].result()


def set_op(name: str, value: str | None) -> AlterConfigOp:
    return AlterConfigOp(config_entry=ConfigEntry(name=name, value=value),
                         operation_type=AlterConfigOp.OpType.SET)


def test_incremental_alter_configs_topic_set_and_delete() -> None:
    client = admin()
    client.create_topics(new_topics=[new_topic("t", 1, 1)]).all().result()
    resource = topic_resource("t")
    # SET.
    client.incremental_alter_configs(configs={resource: [set_op("retention.ms", "42")]}) \
        .all().result()
    config = client.describe_configs(resources=[resource]).values()[resource].result()
    entry = config.get(name="retention.ms")
    assert entry is not None and entry.value() == "42"
    # DELETE.
    delete_op = AlterConfigOp(config_entry=ConfigEntry(name="retention.ms", value=None),
                              operation_type=AlterConfigOp.OpType.DELETE)
    client.incremental_alter_configs(configs={resource: [delete_op]}).all().result()
    config = client.describe_configs(resources=[resource]).values()[resource].result()
    assert config.get(name="retention.ms") is None


def test_incremental_alter_configs_unknown_topic_is_unknown_topic_error() -> None:
    resource = topic_resource("missing")
    result = admin().incremental_alter_configs(configs={resource: [set_op("k", "v")]})
    error = error_of(result.values()[resource])
    assert isinstance(error, UnknownTopicOrPartitionError)
    assert str(error) == "No such topic as missing"


def test_incremental_alter_configs_client_metrics_creates_resource() -> None:
    client = admin()
    resource = ConfigResource(type=ConfigResource.Type.CLIENT_METRICS, name="cm")
    client.incremental_alter_configs(configs={resource: [set_op("interval.ms", "5000")]}) \
        .all().result()
    # The new client-metrics resource now shows up in list_config_resources.
    listed = client.list_config_resources(
        config_resource_types={ConfigResource.Type.CLIENT_METRICS},
        options=ListConfigResourcesOptions()).all().result()
    assert resource in listed


def test_incremental_alter_configs_empty_client_metrics_name_is_invalid_request() -> None:
    resource = ConfigResource(type=ConfigResource.Type.CLIENT_METRICS, name="")
    result = admin().incremental_alter_configs(configs={resource: [set_op("k", "v")]})
    error = error_of(result.values()[resource])
    assert isinstance(error, InvalidRequestError)
    assert str(error) == "Empty resource name"


def test_list_config_resources_all_types_when_empty() -> None:
    client = admin()
    client.create_topics(new_topics=[new_topic("t", 1, 1)]).all().result()
    listed = set(client.list_config_resources(
        config_resource_types=set(), options=ListConfigResourcesOptions()).all().result())
    assert topic_resource("t") in listed
    # 3 brokers -> broker 0..2 and broker-logger 0..2.
    for i in range(3):
        assert ConfigResource(type=ConfigResource.Type.BROKER, name=str(i)) in listed
        assert ConfigResource(type=ConfigResource.Type.BROKER_LOGGER, name=str(i)) in listed


def test_list_config_resources_filters_by_type() -> None:
    client = admin()
    client.create_topics(new_topics=[new_topic("t", 1, 1)]).all().result()
    listed = client.list_config_resources(
        config_resource_types={ConfigResource.Type.TOPIC},
        options=ListConfigResourcesOptions()).all().result()
    assert list(listed) == [topic_resource("t")]


# ---- Partition reassignments --------------------------------------------------

def admin_with_reassignment() -> tuple[MockAdminClient, TopicPartition]:
    """Seeds one topic and reassigns its only partition."""
    client = admin()
    client.create_topics(new_topics=[new_topic("rt", 1, 3)]).all().result()
    tp = TopicPartition(topic="rt", partition=0)
    target = NewPartitionReassignment(target_replicas=[1, 2])
    client.alter_partition_reassignments(reassignments={tp: target}).all().result()
    return client, tp


def test_alter_then_list_partition_reassignments_reports_adding_and_removing() -> None:
    client, tp = admin_with_reassignment()
    listed = client.list_partition_reassignments().reassignments().result()
    reassignment = listed[tp]
    # The mock seeds every partition with all three brokers as replicas, so
    # targeting {1, 2} removes broker 0 and adds nothing.
    assert reassignment.replicas() == [0, 1, 2]
    assert reassignment.adding_replicas() == []
    assert reassignment.removing_replicas() == [0]
    # None (Java's empty Optional) cancels (MockAdminClient.java:1160-1162).
    client.alter_partition_reassignments(reassignments={tp: None}).all().result()
    assert client.list_partition_reassignments().reassignments().result() == {}


def test_list_partition_reassignments_after_topic_deletion_fails_the_future() -> None:
    # delete_topics leaves the reassignment behind, as Java's does; Java's
    # findPartitionReassignment then throws its RuntimeException out of the call
    # (the Rust mock fails the future instead).
    client, _tp = admin_with_reassignment()
    client.delete_topics(topics=TopicCollection.of_topic_names(topics=["rt"])).all().result()
    with pytest.raises(RuntimeError) as e:
        client.list_partition_reassignments()
    assert str(e.value) == (
        "Internal MockAdminClient logic error: found reassignment for rt-0, but no "
        "TopicMetadata")


def test_list_partition_reassignments_after_topic_shrink_fails_the_future() -> None:
    # Java's metadata.partitions.get(1) throws IndexOutOfBoundsException on the
    # recreated one-partition topic (its "no TopicPartitionInfo" branch is dead; the
    # Rust mock reports that message instead).
    client = admin()
    client.create_topics(new_topics=[new_topic("rt2", 2, 3)]).all().result()
    tp = TopicPartition(topic="rt2", partition=1)
    target = NewPartitionReassignment(target_replicas=[1, 2])
    client.alter_partition_reassignments(reassignments={tp: target}).all().result()
    client.delete_topics(topics=TopicCollection.of_topic_names(topics=["rt2"])).all().result()
    client.create_topics(new_topics=[new_topic("rt2", 1, 3)]).all().result()
    with pytest.raises(IndexError) as e:
        client.list_partition_reassignments()
    assert str(e.value) == "Index 1 out of bounds for length 1"


# ---- add_topic broker validation (MockAdminClient.java:296-309) ---------------

def test_mock_add_topic_accepts_partitions_whose_brokers_are_all_known() -> None:
    mock = admin()
    replicas = [seeded_broker(0), seeded_broker(1)]
    mock.add_topic(internal=False, name="topic",
                   partitions=[partition_info(0, seeded_broker(0), replicas, replicas)],
                   configs={})


def test_mock_add_topic_rejects_a_duplicate_topic() -> None:
    mock = admin()
    partitions = [partition_info(0, seeded_broker(0), [seeded_broker(0)], [seeded_broker(0)])]
    mock.add_topic(internal=False, name="topic", partitions=partitions, configs={})
    with pytest.raises(IllegalArgumentError) as e:
        mock.add_topic(internal=False, name="topic", partitions=partitions, configs={})
    assert str(e.value) == "Topic topic was already added."


def test_mock_add_topic_rejects_an_unknown_leader() -> None:
    mock = admin()
    partitions = [partition_info(0, unknown_broker(), [seeded_broker(0)], [])]
    with pytest.raises(IllegalArgumentError) as e:
        mock.add_topic(internal=False, name="topic", partitions=partitions, configs={})
    assert str(e.value) == "Leader broker unknown"
    # Java's brokers.contains(null) is false for a leaderless partition, so it takes
    # this same branch rather than reaching the log-dir loop.
    leaderless = [partition_info(0, None, [seeded_broker(0)], [])]
    with pytest.raises(IllegalArgumentError) as e:
        mock.add_topic(internal=False, name="other", partitions=leaderless, configs={})
    assert str(e.value) == "Leader broker unknown"
    # Neither rejected topic was recorded.
    assert mock.list_topics().names().result() == set()


def test_mock_add_topic_rejects_unknown_brokers_in_the_replica_list() -> None:
    partitions = [partition_info(0, seeded_broker(0), [seeded_broker(0), unknown_broker()], [])]
    with pytest.raises(IllegalArgumentError) as e:
        admin().add_topic(internal=False, name="topic", partitions=partitions, configs={})
    assert str(e.value) == "Unknown brokers in replica list"


def test_mock_add_topic_rejects_unknown_brokers_in_the_isr_list() -> None:
    # The replica list is fine here, so only the ISR check can fire.
    partitions = [partition_info(0, seeded_broker(0), [seeded_broker(0)], [unknown_broker()])]
    with pytest.raises(IllegalArgumentError) as e:
        admin().add_topic(internal=False, name="topic", partitions=partitions, configs={})
    assert str(e.value) == "Unknown brokers in isr list"


def test_mock_add_topic_rejects_a_leader_with_no_log_directories() -> None:
    # Java: brokerLogDirs.get(0).get(0) throws (the Rust mock reports "Broker 0 has
    # no log directories.").
    mock = MockAdminClient.Builder(broker_log_dirs=[[]]).build()
    partitions = [partition_info(0, seeded_broker(0), [seeded_broker(0)], [])]
    with pytest.raises(IndexError) as e:
        mock.add_topic(internal=False, name="topic", partitions=partitions, configs={})
    assert str(e.value) == "Index 0 out of bounds for length 0"


def test_mock_mark_topic_for_deletion_rejects_an_unknown_topic() -> None:
    with pytest.raises(IllegalArgumentError) as e:
        admin().mark_topic_for_deletion(name="nope")
    assert str(e.value) == "Topic nope did not exist."


# ---- Builder keyword forms --------------------------------------------------------

def test_builder_defaults_match_java() -> None:
    builder = MockAdminClient.Builder()
    client = builder.build()
    assert client.brokers() == [seeded_broker(0)]
    assert client.describe_cluster().controller().result() == seeded_broker(0)
    assert client.describe_cluster().cluster_id().result() == "I4ZmrWqfT2e-upky_4fdPA"
    assert MockAdminClient.DEFAULT_LOG_DIRS == ("/tmp/kafka-logs",)
    # One partition, min(brokers, 3) replicas.
    result = client.create_topics(new_topics=[NewTopic(name="t", num_partitions=None,
                                                       replication_factor=None)])
    assert result.num_partitions(topic="t").result() == 1
    assert result.replication_factor(topic="t").result() == 1


def test_builder_keywords_equal_the_setters() -> None:
    brokers = [seeded_broker(0), seeded_broker(1), seeded_broker(2)]
    by_keyword = MockAdminClient.Builder(
        cluster_id="c", brokers=brokers, controller=2, default_partitions=4,
        default_replication_factor=2).build()
    by_setters = (MockAdminClient.create().cluster_id(cluster_id="c").brokers(brokers=brokers)
                  .controller(index=2).default_partitions(num_partitions=4)
                  .default_replication_factor(default_replication_factor=2).build())
    for client in (by_keyword, by_setters):
        cluster = client.describe_cluster()
        assert cluster.cluster_id().result() == "c"
        assert cluster.controller().result() == seeded_broker(2)
        assert client.brokers() == brokers
        result = client.create_topics(new_topics=[NewTopic(name="t", num_partitions=None,
                                                           replication_factor=None)])
        assert result.num_partitions(topic="t").result() == 4
        assert result.replication_factor(topic="t").result() == 2


def test_builder_runs_controller_after_brokers() -> None:
    # controller(int) reads brokers: keyword order alone (controller first) would
    # fail with the one default broker.
    brokers = [seeded_broker(0), seeded_broker(1)]
    client = MockAdminClient.Builder(controller=1, brokers=brokers).build()
    assert client.describe_cluster().controller().result() == seeded_broker(1)


def test_builder_runs_broker_log_dirs_after_brokers() -> None:
    # brokers(...) rewrites brokerLogDirs through numBrokers; the given log dirs
    # survive because broker_log_dirs runs after it.
    brokers = [seeded_broker(0), seeded_broker(1)]
    client = MockAdminClient.Builder(broker_log_dirs=[["/a"], ["/b"]], brokers=brokers).build()
    client.create_topics(new_topics=[new_topic("t", 1, 1)]).all().result()
    replica = TopicPartitionReplica(topic="t", partition=0, broker_id=0)
    info = client.describe_replica_log_dirs(replicas=[replica]).values()[replica].result()
    assert info.current_replica_log_dir() == "/a"


def test_builder_num_brokers_shrinks_and_grows() -> None:
    builder = MockAdminClient.create().num_brokers(num_brokers=3).num_brokers(num_brokers=2)
    assert builder.build().brokers() == [seeded_broker(0), seeded_broker(1)]
    assert builder.num_brokers(num_brokers=4).build().brokers() == [
        seeded_broker(i) for i in range(4)]


def test_builder_controller_out_of_range_raises_index_error() -> None:
    with pytest.raises(IndexError) as e:
        MockAdminClient.Builder(controller=5)
    assert str(e.value) == "Index 5 out of bounds for length 1"


def test_builder_default_replication_factor_is_short_value() -> None:
    # Java's build() passes defaultReplicationFactor.shortValue().
    client = MockAdminClient.Builder(default_replication_factor=65538).build()
    resource = ConfigResource(type=ConfigResource.Type.BROKER, name="0")
    config = client.describe_configs(resources=[resource]).values()[resource].result()
    entry = config.get(name="default.replication.factor")
    assert entry is not None and entry.value() == "2"


def test_builder_num_brokers_is_no_keyword_and_calls_are_keyword_only() -> None:
    with pytest.raises(TypeError):
        MockAdminClient.Builder(num_brokers=2)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        MockAdminClient.Builder().cluster_id("c")  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        MockAdminClient.Builder("c")  # type: ignore[call-arg]


def test_builder_keyword_none_calls_the_setter() -> None:
    # None calls the setter; Java's brokers(null) dereferences it.
    with pytest.raises(TypeError):
        MockAdminClient.Builder(brokers=None)  # type: ignore[arg-type]


# ---- Constructors -------------------------------------------------------------

def test_no_argument_constructor_has_one_no_node_broker() -> None:
    client = MockAdminClient()
    assert client.brokers() == [Node.no_node()]
    assert client.describe_cluster().controller().result() == Node.no_node()


def test_brokers_controller_constructor() -> None:
    brokers = [seeded_broker(0), seeded_broker(1)]
    client = MockAdminClient(brokers=brokers, controller=seeded_broker(1))
    assert client.brokers() == brokers
    # MockAdminClient(brokers, controller) uses brokers.size() as replication factor.
    result = client.create_topics(new_topics=[NewTopic(name="t", num_partitions=None,
                                                       replication_factor=None)])
    assert result.replication_factor(topic="t").result() == 2


def test_constructor_forms() -> None:
    with pytest.raises(IllegalArgumentError) as e:
        MockAdminClient(brokers=[seeded_broker(0)])  # type: ignore[call-overload]
    assert str(e.value) == "MockAdminClient() takes one of (), (brokers, controller); got (brokers)"
    with pytest.raises(IllegalArgumentError) as e:
        MockAdminClient(brokers=[seeded_broker(0)], controller=seeded_broker(1))
    assert str(e.value) == "The controller node must be in the list of brokers"
    with pytest.raises(TypeError):
        MockAdminClient([seeded_broker(0)], seeded_broker(0))  # type: ignore[call-overload]


def test_close_does_nothing_and_the_mock_keeps_answering() -> None:
    with admin() as client:
        client.close()
        client.close(timeout=-1.0)
    assert len(client.describe_cluster().nodes().result()) == 3


def test_create_hides_admin_create() -> None:
    assert isinstance(MockAdminClient.create(), MockAdminClient.Builder)


# ---- The other methods of these groups ----------------------------------------

def test_describe_and_delete_topics_by_id() -> None:
    client = admin()
    result = client.create_topics(new_topics=[new_topic("t", 1, 1)])
    topic_id = result.topic_id(topic="t").result()
    by_id = client.describe_topics(topics=TopicCollection.of_topic_ids(topics=[topic_id]))
    values = by_id.topic_id_values()
    assert values is not None and values[topic_id].result().name() == "t"
    missing = Uuid.random_uuid()
    error = error_of(client.handle_describe_topics_using_ids(topic_ids=[missing])[missing])
    assert isinstance(error, UnknownTopicIdError)
    assert str(error) == "Topic id" + str(missing) + " not found."
    deleted = client.delete_topics(topics=TopicCollection.of_topic_ids(topics=[topic_id, missing]))
    id_values = deleted.topic_id_values()
    assert id_values is not None
    assert id_values[topic_id].result() is None
    assert str(error_of(id_values[missing])) == f"Topic {missing} does not exist."


def test_delete_topics_by_names_iterable() -> None:
    client = admin()
    client.create_topics(new_topics=[new_topic("t", 1, 1)]).all().result()
    client.delete_topics(topics=["t"]).all().result()
    assert client.list_topics().names().result() == set()


def test_describe_topics_forms() -> None:
    client = admin()
    with pytest.raises(IllegalArgumentError) as e:
        client.describe_topics(topic_names=["t"],  # type: ignore[call-overload]
                               topics=TopicCollection.of_topic_names(topics=["t"]))
    assert str(e.value) == ("describe_topics() takes one of (topic_names), (topic_names, "
                            "options), (topics), (topics, options); got (topic_names, topics)")
    values = client.describe_topics(topic_names=["x"]).topic_name_values()
    assert values is not None and isinstance(error_of(values["x"]), UnknownTopicOrPartitionError)


def test_marked_for_deletion_and_fetches_until_visible() -> None:
    client = admin()
    client.create_topics(new_topics=[new_topic("t", 1, 1)]).all().result()
    client.set_fetches_remaining_until_visible(topic_name="t", fetches_remaining_until_visible=1)
    assert client.list_topics().names().result() == set()
    assert client.list_topics().names().result() == {"t"}
    client.mark_topic_for_deletion(name="t")
    values = client.describe_topics(topic_names=["t"]).topic_name_values()
    assert values is not None and isinstance(error_of(values["t"]), UnknownTopicOrPartitionError)
    with pytest.raises(RuntimeError) as e:
        client.set_fetches_remaining_until_visible(topic_name="x",
                                                   fetches_remaining_until_visible=1)
    assert str(e.value) == "No such topic as x"


def test_broker_accessors() -> None:
    client = admin()
    copy = client.brokers()
    copy.clear()
    assert len(client.brokers()) == 3
    assert client.broker(index=2) == seeded_broker(2)
    with pytest.raises(IndexError):
        client.broker(index=3)


def test_describe_configs_other_resources() -> None:
    client = MockAdminClient.Builder(default_group_configs={"g.k": "d"}).build()
    bad_broker = ConfigResource(type=ConfigResource.Type.BROKER, name="x")
    unknown_metrics = ConfigResource(type=ConfigResource.Type.CLIENT_METRICS, name="cm")
    group = ConfigResource(type=ConfigResource.Type.GROUP, name="g")
    logger = ConfigResource(type=ConfigResource.Type.BROKER_LOGGER, name="0")
    values = client.describe_configs(
        resources=[bad_broker, unknown_metrics, group, logger]).values()
    error = error_of(values[bad_broker])
    assert isinstance(error, IllegalArgumentError) and str(error) == 'For input string: "x"'
    assert isinstance(error_of(values[unknown_metrics]), NullPointerError)
    entry = values[group].result().get(name="g.k")
    assert entry is not None and entry.value() == "d"
    error = error_of(values[logger])
    assert isinstance(error, UnsupportedVersionError) and str(error) == "Not implemented yet"


def test_incremental_alter_configs_unsupported_op_and_type() -> None:
    client = admin()
    broker = ConfigResource(type=ConfigResource.Type.BROKER, name="0")
    logger = ConfigResource(type=ConfigResource.Type.BROKER_LOGGER, name="0")
    append = AlterConfigOp(config_entry=ConfigEntry(name="k", value="v"),
                           operation_type=AlterConfigOp.OpType.APPEND)
    values = client.incremental_alter_configs(
        configs={broker: [append], logger: [set_op("k", "v")]}).values()
    error = error_of(values[broker])
    assert isinstance(error, InvalidRequestError) and str(error) == "Unsupported op type APPEND"
    assert isinstance(error_of(values[logger]), UnsupportedVersionError)


def test_replica_log_dirs() -> None:
    client = MockAdminClient.Builder(broker_log_dirs=[["/a", "/b"]]).build()
    client.create_topics(new_topics=[new_topic("t", 1, 1)]).all().result()
    replica = TopicPartitionReplica(topic="t", partition=0, broker_id=0)
    beyond = TopicPartitionReplica(topic="t", partition=5, broker_id=0)
    unknown = TopicPartitionReplica(topic="u", partition=0, broker_id=0)
    values = client.alter_replica_log_dirs(
        replica_assignment={replica: "/b", beyond: "/b"}).values()
    assert values[replica].result() is None
    error = error_of(values[beyond])
    assert isinstance(error, ReplicaNotAvailableError) and str(error) == "Can't find t-5-0"
    offline = client.alter_replica_log_dirs(replica_assignment={replica: "/c"}).values()
    error = error_of(offline[replica])
    assert isinstance(error, KafkaStorageError) and str(error) == "Log directory /c is offline"
    infos = client.describe_replica_log_dirs(replicas=[replica, beyond, unknown]).values()
    assert unknown not in infos
    moved = infos[replica].result()
    assert (moved.current_replica_log_dir(), moved.future_replica_log_dir()) == ("/a", "/b")
    none = infos[beyond].result()
    assert (none.current_replica_log_dir(), none.current_replica_offset_lag()) == (None, -1)
    with pytest.raises(IndexError):
        client.alter_replica_log_dirs(
            replica_assignment={TopicPartitionReplica(topic="t", partition=0, broker_id=4): "/a"})


def test_describe_log_dirs() -> None:
    client = admin()
    client.create_topics(new_topics=[new_topic("t", 1, 2)]).all().result()
    values = client.describe_log_dirs(brokers=[0, 1, 2]).descriptions()
    description = values[0].result()["/tmp/kafka-logs"]
    assert list(description.replica_infos()) == [TopicPartition(topic="t", partition=0)]
    assert description.total_bytes() is None
    assert values[2].result() == {}
    # A replica on a broker not asked for: Java's NullPointerException.
    with pytest.raises(NullPointerError):
        client.describe_log_dirs(brokers=[0])


def test_unimplemented_methods_raise_java_messages() -> None:
    client = admin()
    with pytest.raises(UnsupportedVersionError) as e:
        client.create_partitions(new_partitions={})
    assert str(e.value) == "Not implemented yet"
    with pytest.raises(UnsupportedVersionError) as e:
        client.elect_leaders(election_type=ElectionType.PREFERRED, partitions=None)
    assert str(e.value) == "Not implemented yet"
    assert client.delete_records(records_to_delete={}).low_watermarks() == {}
    tp = TopicPartition(topic="t", partition=0)
    with pytest.raises(UnsupportedVersionError) as e:
        client.delete_records(records_to_delete={tp: RecordsToDelete.before_offset(offset=1)})
    assert str(e.value) == "Not implemented yet"


def test_list_offsets() -> None:
    client = admin()
    tp = TopicPartition(topic="t", partition=0)
    client.update_beginning_offsets(new_offsets={tp: 3})
    client.update_end_offsets(new_offsets={tp: 9})
    earliest = client.list_offsets(topic_partition_offsets={tp: OffsetSpec.earliest()})
    assert earliest.partition_result(partition=tp).result().offset() == 3
    latest = client.list_offsets(topic_partition_offsets={tp: OffsetSpec.latest()})
    assert latest.partition_result(partition=tp).result().offset() == 9
    with pytest.raises(UnsupportedVersionError) as e:
        client.list_offsets(topic_partition_offsets={tp: OffsetSpec.for_timestamp(timestamp=1)})
    assert str(e.value) == "Not implement yet"
    other = TopicPartition(topic="t", partition=1)
    with pytest.raises(NullPointerError):
        client.list_offsets(topic_partition_offsets={other: OffsetSpec.latest()})


def test_list_config_resources_forms() -> None:
    client = admin()
    assert len(client.list_config_resources().all().result()) == 6
    with pytest.raises(IllegalArgumentError) as e:
        client.list_config_resources(  # type: ignore[call-overload]
            config_resource_types={ConfigResource.Type.TOPIC})
    assert str(e.value) == ("list_config_resources() takes one of (config_resource_types, "
                            "options), (); got (config_resource_types)")
    # Java's mock completes with a HashSet; the result's future holds a list.
    assert isinstance(client.list_config_resources().all().result(), list)


def test_create_topics_negative_counts_are_illegal_capacity() -> None:
    # Java's new ArrayList<>(replicationFactor) / new ArrayList<>(numberOfPartitions)
    # throw IllegalArgumentException out of the call; topics handled before stay.
    client = admin()
    with pytest.raises(IllegalArgumentError) as e:
        client.create_topics(new_topics=[new_topic("a", 1, 1), new_topic("t", 1, -2)])
    assert str(e.value) == "Illegal Capacity: -2"
    with pytest.raises(IllegalArgumentError) as e:
        client.create_topics(new_topics=[new_topic("u", -2, 1)])
    assert str(e.value) == "Illegal Capacity: -2"
    assert client.list_topics().names().result() == {"a"}
    defaults = MockAdminClient.Builder(default_partitions=-3).build()
    with pytest.raises(IllegalArgumentError) as e:
        defaults.create_topics(new_topics=[NewTopic(name="t", num_partitions=None,
                                                    replication_factor=None)])
    assert str(e.value) == "Illegal Capacity: -3"


def test_public_constructors_keep_java_collections_index_messages() -> None:
    # MockAdminClient() uses singletonList(Node.noNode()) and
    # nCopies(1, DEFAULT_LOG_DIRS): their get says "Index: i, Size: n". The leader
    # Node.no_node() has id -1.
    client = MockAdminClient()
    with pytest.raises(IndexError) as e:
        client.create_topics(new_topics=[new_topic("t", 1, 1)])
    assert str(e.value) == "Index: -1, Size: 1"
    with pytest.raises(IndexError) as e:
        client.broker(index=1)
    assert str(e.value) == "Index: 1, Size: 1"
    two = MockAdminClient(brokers=[seeded_broker(0), seeded_broker(1)], controller=seeded_broker(0))
    with pytest.raises(IndexError) as e:
        two.alter_replica_log_dirs(replica_assignment={
            TopicPartitionReplica(topic="t", partition=0, broker_id=2): "/a"})
    assert str(e.value) == "Index: 2, Size: 2"


def test_positional_calls_are_type_errors_on_the_mock() -> None:
    client = admin()
    builder = MockAdminClient.Builder()
    tp = TopicPartition(topic="t", partition=0)
    calls: list[tuple[Any, tuple[Any, ...]]] = [
        (client.create_topics, ([],)),
        (client.delete_topics, (["t"],)),
        (client.list_topics, (None,)),
        (client.describe_topics, (["t"],)),
        (client.describe_cluster, (None,)),
        (client.describe_configs, ([],)),
        (client.incremental_alter_configs, ({},)),
        (client.alter_replica_log_dirs, ({},)),
        (client.describe_log_dirs, ([],)),
        (client.describe_replica_log_dirs, ([],)),
        (client.create_partitions, ({},)),
        (client.delete_records, ({},)),
        (client.elect_leaders, (ElectionType.PREFERRED, None)),
        (client.alter_partition_reassignments, ({},)),
        (client.list_partition_reassignments, (None,)),
        (client.list_offsets, ({},)),
        (client.list_config_resources, (set(), ListConfigResourcesOptions())),
        (client.close, (1.0,)),
        (client.controller, (seeded_broker(0),)),
        (client.add_topic, (False, "x", [], {})),
        (client.mark_topic_for_deletion, ("t",)),
        (client.timeout_next_request, (1,)),
        (client.handle_describe_topics_using_ids, ([],)),
        (client.update_beginning_offsets, ({tp: 1},)),
        (client.update_end_offsets, ({tp: 1},)),
        (client.update_consumer_group_offsets, ({tp: 1},)),
        (client.set_fetches_remaining_until_visible, ("t", 1)),
        (client.broker, (0,)),
        (builder.cluster_id, ("c",)),
        (builder.brokers, ([seeded_broker(0)],)),
        (builder.num_brokers, (1,)),
        (builder.controller, (0,)),
        (builder.broker_log_dirs, ([["/a"]],)),
        (builder.default_replication_factor, (1,)),
        (builder.using_raft_controller, (True,)),
        (builder.default_partitions, (1,)),
        (builder.feature_levels, ({},)),
        (builder.min_supported_feature_levels, ({},)),
        (builder.max_supported_feature_levels, ({},)),
        (builder.default_group_configs, ({},)),
    ]
    for method, args in calls:
        with pytest.raises(TypeError):
            method(*args)
    # Nothing a rejected call would have done happened.
    assert client.list_topics().names().result() == set()


# ---- features (Rust: admin_with_features, update_one, mock_*_features_*) -----

def cause_of(future: Any) -> BaseException:
    """``get()`` on a failed future: Java's ``ExecutionException``, the stored error
    its cause."""
    from confluent_kafka import ExecutionError

    with pytest.raises(ExecutionError) as info:
        future.get()
    assert info.value.__cause__ is not None
    return info.value.__cause__


def admin_with_features() -> MockAdminClient:
    # The Rust helper set_feature_levels has no Java counterpart: the
    # Builder's three feature-level setters seed the same maps.
    return (MockAdminClient.create().num_brokers(num_brokers=1)
            .feature_levels(feature_levels={"feature": 3})
            .min_supported_feature_levels(min_supported_feature_levels={"feature": 1})
            .max_supported_feature_levels(max_supported_feature_levels={"feature": 5})
            .build())


def update_one(mock: MockAdminClient, next_: int,
               upgrade_type: FeatureUpdate.UpgradeType) -> Any:
    updates = {"feature": FeatureUpdate(max_version_level=next_, upgrade_type=upgrade_type)}
    result = mock.update_features(feature_updates=updates, options=UpdateFeaturesOptions())
    return result.values()["feature"]


def finalized(mock: MockAdminClient) -> FinalizedVersionRange:
    metadata = mock.describe_features(options=DescribeFeaturesOptions()).feature_metadata()
    return metadata.get().finalized_features()["feature"]


def test_mock_describe_features_returns_seeded_ranges() -> None:
    metadata = admin_with_features().describe_features().feature_metadata().get()
    assert metadata.finalized_features_epoch() == 123
    assert metadata.finalized_features()["feature"] == FinalizedVersionRange(
        min_version_level=3, max_version_level=3)
    assert metadata.supported_features()["feature"] == SupportedVersionRange(
        min_version=1, max_version=5)


def test_mock_update_features_upgrade_succeeds_and_mutates_level() -> None:
    mock = admin_with_features()
    assert update_one(mock, 4, FeatureUpdate.UpgradeType.UPGRADE).get() is None
    # The finalized level is now 4.
    assert finalized(mock) == FinalizedVersionRange(min_version_level=4, max_version_level=4)


def test_mock_update_features_validate_only_does_not_mutate() -> None:
    mock = admin_with_features()
    updates = {"feature": FeatureUpdate(max_version_level=4,
                                        upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)}
    result = mock.update_features(feature_updates=updates,
                                  options=UpdateFeaturesOptions(validate_only=True))
    assert result.values()["feature"].get() is None
    # Level unchanged because validate_only was set.
    assert finalized(mock) == FinalizedVersionRange(min_version_level=3, max_version_level=3)


@pytest.mark.parametrize(("next_", "upgrade_type", "message"), [
    (2, FeatureUpdate.UpgradeType.UPGRADE, "Can't upgrade to lower version."),
    (4, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE, "Can't downgrade to newer version."),
    (6, FeatureUpdate.UpgradeType.UPGRADE, "Can't upgrade above 5"),
    # next = 0 (a deletion), with SAFE_DOWNGRADE; min is 1.
    (0, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE, "Can't downgrade below 1"),
    (2, FeatureUpdate.UpgradeType.UNKNOWN, "Invalid upgrade type."),
], ids=["rejects_upgrade_to_lower_version", "rejects_safe_downgrade_to_newer_version",
        "rejects_upgrade_above_max", "rejects_downgrade_below_min",
        "rejects_unknown_upgrade_type"])
def test_mock_update_features_rejects(next_: int, upgrade_type: FeatureUpdate.UpgradeType,
                                      message: str) -> None:
    # The five Rust mock_update_features_rejects_* tests.
    error = cause_of(update_one(admin_with_features(), next_, upgrade_type))
    assert type(error) is InvalidRequestError
    assert str(error) == f"Invalid update version {next_} for feature feature. {message}"


def test_mock_update_features_unsafe_downgrade_succeeds() -> None:
    mock = admin_with_features()
    # cur = 3, next = 2, UNSAFE_DOWNGRADE: allowed (the mock's even-level guard is
    # dead code, mirroring Java).
    assert update_one(mock, 2, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE).get() is None
    assert finalized(mock) == FinalizedVersionRange(min_version_level=2, max_version_level=2)


def test_update_features_first_failure_fails_every_future_and_sets_nothing() -> None:
    mock = (MockAdminClient.create()
            .feature_levels(feature_levels={"a": 1, "b": 1})
            .min_supported_feature_levels(min_supported_feature_levels={"a": 1, "b": 1})
            .max_supported_feature_levels(max_supported_feature_levels={"a": 5, "b": 2})
            .build())
    upgrade = FeatureUpdate.UpgradeType.UPGRADE
    result = mock.update_features(feature_updates={
        "a": FeatureUpdate(max_version_level=2, upgrade_type=upgrade),
        "b": FeatureUpdate(max_version_level=3, upgrade_type=upgrade)})
    errors = [cause_of(result.values()[f]) for f in ("a", "b")]
    assert errors[0] is errors[1]
    assert str(errors[0]) == "Invalid update version 3 for feature b. Can't upgrade above 2"
    assert type(cause_of(result.all())) is InvalidRequestError
    metadata = mock.describe_features().feature_metadata().get()
    assert metadata.finalized_features()["a"].max_version_level() == 1


def test_describe_features_missing_supported_range_is_null_pointer() -> None:
    # Java unboxes minSupportedFeatureLevels.get(feature): null for a feature with
    # no supported range.
    mock = MockAdminClient.create().feature_levels(feature_levels={"f": 1}).build()
    with pytest.raises(NullPointerError):
        mock.describe_features()


# ---- delegation tokens (Rust: user, *_delegation_token_*) --------------------

def user(name: str) -> KafkaPrincipal:
    return KafkaPrincipal(principal_type=KafkaPrincipal.USER_TYPE, name=name)


def all_tokens(client: MockAdminClient) -> list[DelegationToken]:
    # Java's describeDelegationToken() with the default options dereferences the
    # null owners: an empty owners list lists every token.
    options = DescribeDelegationTokenOptions(owners=[])
    return client.describe_delegation_token(options=options).delegation_tokens().get()


def test_create_delegation_token_rejects_non_user_renewer() -> None:
    options = CreateDelegationTokenOptions(
        renewers=[KafkaPrincipal(principal_type="Group", name="admins")])
    error = cause_of(admin().create_delegation_token(options=options).delegation_token())
    assert type(error) is InvalidPrincipalTypeError
    assert str(error) == ""


def test_create_delegation_token_without_a_renewer_reports_an_error() -> None:
    # Java's options.renewers().get(0) (MockAdminClient.java:652) throws
    # IndexOutOfBoundsException out of the call on the options' default LinkedList;
    # the Rust core fails the future instead, as it cannot panic across the FFI.
    client = admin()
    with pytest.raises(IndexError) as e:
        client.create_delegation_token(options=CreateDelegationTokenOptions())
    assert str(e.value) == "Index: 0, Size: 0"
    with pytest.raises(IndexError):
        client.create_delegation_token()
    # Nothing was stored, so a describe still finds no tokens.
    assert all_tokens(client) == []


def test_create_then_describe_lists_token() -> None:
    client = admin()
    options = CreateDelegationTokenOptions(renewers=[user("alice")], max_lifetime_ms=1000)
    token = client.create_delegation_token(options=options).delegation_token().get()
    assert token.token_info().owner() == user("alice")
    assert token.token_info().renewers() == [user("alice")]
    assert token.token_info().max_timestamp() == 1000
    assert token.token_info().expiry_timestamp() == -1
    assert token.hmac() == token.token_info().token_id().encode()
    assert all_tokens(client) == [token]


def test_renew_delegation_token_found_and_not_found() -> None:
    client = admin()
    unknown = cause_of(client.renew_delegation_token(
        hmac=b"nope",
        options=RenewDelegationTokenOptions(renew_time_period_ms=10)).expiry_timestamp())
    assert type(unknown) is DelegationTokenNotFoundError
    assert str(unknown) == ""

    token = client.create_delegation_token(
        options=CreateDelegationTokenOptions(renewers=[user("alice")])).delegation_token().get()
    expiry = client.renew_delegation_token(
        hmac=token.hmac(),
        options=RenewDelegationTokenOptions(renew_time_period_ms=4242)).expiry_timestamp()
    assert expiry.get() == 4242
    assert token.token_info().expiry_timestamp() == 4242


def test_expire_delegation_token_not_found() -> None:
    error = cause_of(admin().expire_delegation_token(
        hmac=b"nope", options=ExpireDelegationTokenOptions()).expiry_timestamp())
    assert type(error) is DelegationTokenNotFoundError
    assert str(error) == ""


def test_expire_delegation_token_negative_one_removes_token() -> None:
    client = admin()
    token = client.create_delegation_token(
        options=CreateDelegationTokenOptions(renewers=[user("alice")])).delegation_token().get()
    listed = all_tokens(client)
    expiry = client.expire_delegation_token(
        hmac=token.hmac(),
        options=ExpireDelegationTokenOptions(expiry_time_period_ms=-1)).expiry_timestamp()
    assert expiry.get() == -1
    assert all_tokens(client) == []
    # Java completes describe with its live list, which removeAll then empties.
    assert listed == []


def test_expire_delegation_token_in_the_future_keeps_token() -> None:
    client = admin()
    token = client.create_delegation_token(
        options=CreateDelegationTokenOptions(renewers=[user("alice")])).delegation_token().get()
    later = 2 ** 62
    expiry = client.expire_delegation_token(
        hmac=token.hmac(),
        options=ExpireDelegationTokenOptions(expiry_time_period_ms=later)).expiry_timestamp()
    assert expiry.get() == later
    assert all_tokens(client) == [token]


def test_describe_delegation_token_owners_filter() -> None:
    client = admin()
    token_alice = client.create_delegation_token(
        options=CreateDelegationTokenOptions(renewers=[user("alice")])).delegation_token().get()
    client.create_delegation_token(
        options=CreateDelegationTokenOptions(renewers=[user("bob")])).delegation_token().get()
    listed = client.describe_delegation_token(
        options=DescribeDelegationTokenOptions(owners=[user("alice")])).delegation_tokens()
    assert listed.get() == [token_alice]


def test_describe_delegation_token_default_owners_is_null_pointer() -> None:
    # Java's options.owners().isEmpty() on the default null owners.
    client = admin()
    with pytest.raises(NullPointerError):
        client.describe_delegation_token()
    with pytest.raises(NullPointerError):
        client.describe_delegation_token(options=DescribeDelegationTokenOptions(owners=None))


# ---- consumer groups ---------------------------------------------------------

def test_mock_list_consumer_group_offsets_rejects_a_negative_seeded_offset() -> None:
    # updateConsumerGroupOffsets is an unvalidated putAll in Java, so -1 is seedable;
    # Java then throws IllegalArgumentException("Invalid negative offset") from new
    # OffsetAndMetadata(...) while building the map, out of the call (the Rust core
    # fails the future instead). The spec lists its partitions: Java's default spec
    # has null partitions, which the filter dereferences (see the next test).
    mock = admin()
    tp = TopicPartition(topic="topic", partition=0)
    mock.update_consumer_group_offsets(new_offsets={tp: -1})
    specs = {"group": ListConsumerGroupOffsetsSpec(topic_partitions=[])}
    with pytest.raises(IllegalArgumentError) as e:
        mock.list_consumer_group_offsets(group_specs=specs,
                                         options=ListConsumerGroupOffsetsOptions())
    assert str(e.value) == "Invalid negative offset"

    # The mock is still usable afterwards.
    mock.update_consumer_group_offsets(new_offsets={tp: 7})
    offsets = mock.list_consumer_group_offsets(group_specs=specs)
    listed = offsets.partitions_to_offset_and_metadata().result()[tp]
    assert listed is not None and listed.offset() == 7


def test_mock_list_consumer_group_offsets_negative_offset_outside_the_selection_is_ignored(
) -> None:
    mock = admin()
    selected = TopicPartition(topic="topic", partition=0)
    other = TopicPartition(topic="topic", partition=1)
    mock.update_consumer_group_offsets(new_offsets={selected: 5, other: -1})
    specs = {"group": ListConsumerGroupOffsetsSpec(topic_partitions=[selected])}
    offsets = mock.list_consumer_group_offsets(
        group_specs=specs).partitions_to_offset_and_metadata(group_id="group").result()
    assert len(offsets) == 1
    listed = offsets[selected]
    assert listed is not None and listed.offset() == 5


def test_list_consumer_group_offsets_java_mock_quirks() -> None:
    mock = admin()
    # No offsets stored: the filter never runs, so the default spec lists nothing.
    result = mock.list_consumer_group_offsets(group_id="g")
    assert result.partitions_to_offset_and_metadata(group_id="g").result() == {}
    assert result.all().result() == {"g": {}}
    tp = TopicPartition(topic="t", partition=0)
    mock.update_consumer_group_offsets(new_offsets={tp: 3})
    # Java's listConsumerGroupOffsets(groupId) passes new ListConsumerGroupOffsetsSpec(),
    # whose null partitions the filter dereferences.
    with pytest.raises(NullPointerError):
        mock.list_consumer_group_offsets(group_id="g")
    with pytest.raises(UnsupportedVersionError) as e:
        mock.list_consumer_group_offsets(group_specs={})
    assert str(e.value) == "Not implemented yet"
    with pytest.raises(UnsupportedVersionError):
        mock.list_consumer_group_offsets(group_specs={
            "a": ListConsumerGroupOffsetsSpec(), "b": ListConsumerGroupOffsetsSpec()})
    with pytest.raises(IllegalArgumentError) as iae:
        mock.list_consumer_group_offsets(  # type: ignore[call-overload]
            group_id="g", group_specs={})
    assert str(iae.value) == (
        "list_consumer_group_offsets() takes one of (group_id, options), (group_id), "
        "(group_specs, options), (group_specs); got (group_id, group_specs)")


def test_list_groups_lists_the_groups_with_configs() -> None:
    mock = admin()
    assert mock.list_groups().all().result() == []
    resource = ConfigResource(type=ConfigResource.Type.GROUP, name="g1")
    mock.incremental_alter_configs(configs={resource: [AlterConfigOp(
        config_entry=ConfigEntry(name="k", value="v"), operation_type=AlterConfigOp.OpType.SET)]})
    (listing,) = mock.list_groups(options=ListGroupsOptions()).all().result()
    assert listing.group_id() == "g1"
    assert listing.type() is GroupType.CONSUMER
    assert listing.protocol() == "consumer"
    assert listing.group_state() is GroupState.STABLE


def test_unimplemented_g3b_methods_raise_java_messages() -> None:
    client = admin()
    tp = TopicPartition(topic="t", partition=0)
    implemented = "Not implemented yet"
    typo = "Not implement yet"  # Java's own typo
    calls: list[tuple[Any, dict[str, Any], str]] = [
        (client.describe_acls, {"filter": AclBindingFilter.ANY}, implemented),
        (client.create_acls, {"acls": []}, implemented),
        (client.delete_acls, {"filters": []}, implemented),
        (client.describe_consumer_groups, {"group_ids": ["g"]}, implemented),
        (client.delete_consumer_groups, {"group_ids": ["g"]}, implemented),
        (client.delete_consumer_group_offsets, {"group_id": "g", "partitions": [tp]},
         implemented),
        (client.remove_members_from_consumer_group,
         {"group_id": "g", "options": RemoveMembersFromConsumerGroupOptions()}, implemented),
        (client.alter_consumer_group_offsets,
         {"group_id": "g", "offsets": {tp: OffsetAndMetadata(offset=1)}}, typo),
        (client.describe_client_quotas, {"filter": ClientQuotaFilter.all()}, typo),
        (client.alter_client_quotas, {"entries": []}, typo),
        (client.describe_user_scram_credentials, {}, implemented),
        (client.describe_user_scram_credentials, {"users": None}, implemented),
        (client.describe_user_scram_credentials,
         {"users": ["u"], "options": DescribeUserScramCredentialsOptions()}, implemented),
        (client.alter_user_scram_credentials, {"alterations": []}, implemented),
        (client.describe_producers, {"partitions": [tp]}, implemented),
        (client.describe_transactions, {"transactional_ids": ["t"]}, implemented),
        (client.abort_transaction, {"spec": AbortTransactionSpec(
            topic_partition=tp, producer_id=1, producer_epoch=0, coordinator_epoch=0)},
         implemented),
        (client.list_transactions, {}, implemented),
        (client.fence_producers, {"transactional_ids": ["t"]}, implemented),
        (client.describe_classic_groups, {"group_ids": ["g"]}, implemented),
        (client.force_terminate_transaction, {"transactional_id": "t"}, implemented),
    ]
    for method, kwargs, message in calls:
        with pytest.raises(UnsupportedVersionError) as e:
            method(**kwargs)
        assert str(e.value) == message, method


def test_positional_calls_are_type_errors_on_the_g3b_methods() -> None:
    client = admin()
    calls: list[tuple[Any, tuple[Any, ...]]] = [
        (client.describe_acls, (AclBindingFilter.ANY,)),
        (client.create_acls, ([],)),
        (client.delete_acls, ([],)),
        (client.create_delegation_token, (None,)),
        (client.renew_delegation_token, (b"x",)),
        (client.expire_delegation_token, (b"x",)),
        (client.describe_delegation_token, (None,)),
        (client.describe_consumer_groups, ([],)),
        (client.list_consumer_group_offsets, ("g",)),
        (client.delete_consumer_groups, ([],)),
        (client.delete_consumer_group_offsets, ("g", [])),
        (client.list_groups, (None,)),
        (client.remove_members_from_consumer_group, ("g", None)),
        (client.alter_consumer_group_offsets, ("g", {})),
        (client.describe_client_quotas, (ClientQuotaFilter.all(),)),
        (client.alter_client_quotas, ([],)),
        (client.describe_user_scram_credentials, (None,)),
        (client.alter_user_scram_credentials, ([],)),
        (client.describe_features, (None,)),
        (client.update_features, ({},)),
        (client.describe_producers, ([],)),
        (client.describe_transactions, ([],)),
        (client.abort_transaction, (None,)),
        (client.list_transactions, (None,)),
        (client.fence_producers, ([],)),
        (client.describe_classic_groups, ([],)),
        (client.force_terminate_transaction, ("t",)),
    ]
    for method, args in calls:
        with pytest.raises(TypeError):
            method(*args)


def test_the_mock_overrides_every_admin_method() -> None:
    from confluent_kafka.admin import Admin

    for name, member in vars(Admin).items():
        if callable(member) and not name.startswith("_") and name != "create":
            assert name in vars(MockAdminClient), name
