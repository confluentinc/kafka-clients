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

"""The admin client's topic, cluster, config, log-dir, partition, record, election,
reassignment, offset and controller RPCs, without a broker:
``deleteTopics``, ``describeTopics``, ``describeCluster``, ``describeConfigs``,
``incrementalAlterConfigs``, ``alterReplicaLogDirs``, ``describeLogDirs``,
``describeReplicaLogDirs``, ``createPartitions``, ``deleteRecords``,
``electLeaders``, ``alterPartitionReassignments``, ``listPartitionReassignments``,
``listOffsets``, ``listConfigResources`` and ``unregisterController``, sync and
async.

The ``KafkaAdminClientTest.java`` (Apache Kafka 4.4.0) tests of these RPCs, ported
or skipped with the reason, are listed in the inventory of all its tests in
``test_admin_java_ports.py``'s module docstring, which also holds the ports that need
no scripted broker (``testInvalidTopicNames``' ``deleteTopics`` / ``describeTopics``
parts, ``testDescribeTopicsTimeoutWhenNoBrokerResponds``); the ``MockClient``-scripted
ones are translated in the core, ``rust/src/admin/kafka_admin_client.rs``.

The binding's own part is tested here: keyword-only signatures and the
``java_forms`` messages, the closed client, the plain values each RPC hands the
extension (read from the conversions, not from a broker), the setters each
Options records and the extension runs, the value classes built from the plain
values the extension's converters deliver, and every RPC returning at once with
futures that fail later with the call timeout and the core's message.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka.admin import (
    Admin,
    AlterConfigOp,
    AlterConfigsOptions,
    AlterPartitionReassignmentsOptions,
    AlterReplicaLogDirsOptions,
    AsyncAdmin,
    Config,
    ConfigEntry,
    CreatePartitionsOptions,
    DeletedRecords,
    DeleteRecordsOptions,
    DeleteTopicsOptions,
    DescribeClusterOptions,
    DescribeConfigsOptions,
    DescribeLogDirsOptions,
    DescribeReplicaLogDirsOptions,
    DescribeReplicaLogDirsResult,
    DescribeTopicsOptions,
    ElectLeadersOptions,
    ListConfigResourcesOptions,
    ListOffsetsOptions,
    ListOffsetsResult,
    ListPartitionReassignmentsOptions,
    LogDirDescription,
    NewPartitionReassignment,
    NewPartitions,
    OffsetSpec,
    PartitionReassignment,
    RecordsToDelete,
    ReplicaInfo,
    TopicDescription,
    UnregisterControllerOptions,
)
from confluent_kafka.admin import _conversions
from confluent_kafka.admin._futures import Async, Sync
from confluent_kafka.common.acl import AclOperation
from confluent_kafka.common import (
    AsyncKafkaFuture,
    ElectionType,
    IsolationLevel,
    KafkaFuture,
    Node,
    TopicCollection,
    TopicPartition,
    TopicPartitionInfo,
    TopicPartitionReplica,
    Uuid,
)
from confluent_kafka.common.config import ConfigResource
from confluent_kafka.common.errors import (
    KafkaStorageError,
    UnknownServerError,
    UnknownTopicOrPartitionError,
)
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError

from .admin_futures import assert_future_throws, async_assert_future_throws

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CLOSED = "Cannot accept new calls when AdminClient is closing."
SHORT = 300

TP = TopicPartition(topic="t", partition=0)
RESOURCE = ConfigResource(type=ConfigResource.Type.TOPIC, name="t")
REPLICA = TopicPartitionReplica(topic="t", partition=0, broker_id=1)
SET_OP = AlterConfigOp(config_entry=ConfigEntry(name="retention.ms", value="1000"),
                       operation_type=AlterConfigOp.OpType.SET)


def _admin(**extra: Any) -> Admin:
    return Admin.create(configs={**UNREACHABLE, **extra})


def _timed_out(call: str) -> str:
    return "Timed out waiting for a node assignment. Call: " + call


def _raw_error(error_class: type[BaseException], message: str) -> tuple[Any, ...]:
    """An error embedded in a delivered value (a log dir's, an election's), as the
    extension's ``borrowed_error_to_py`` gives it."""
    return (error_class._ffi_id, message, 0, 0, None, 1, None)  # type: ignore[attr-defined]


# Each RPC once, with a short call timeout: (name, Java call name in the core's
# message, the call, the future to read). The sync and async tests share it.
_CALLS: list[tuple[str, str, Callable[[Any], Any], Callable[[Any], Any]]] = [
    ("delete_topics", "deleteTopics",
     lambda a: a.delete_topics(topics=["t"], options=DeleteTopicsOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("delete_topics_by_id", "deleteTopics",
     lambda a: a.delete_topics(topics=TopicCollection.of_topic_ids(topics=[Uuid.random_uuid()]),
                               options=DeleteTopicsOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("describe_topics_by_id", "describeTopicsWithIds",
     lambda a: a.describe_topics(
         topics=TopicCollection.of_topic_ids(topics=[Uuid.random_uuid()]),
         options=DescribeTopicsOptions(timeout_ms=SHORT)),
     lambda r: r.all_topic_ids()),
    ("describe_cluster", "listNodes",
     lambda a: a.describe_cluster(options=DescribeClusterOptions(timeout_ms=SHORT)),
     lambda r: r.nodes()),
    ("describe_configs", "describeConfigs",
     lambda a: a.describe_configs(resources=[RESOURCE],
                                  options=DescribeConfigsOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("incremental_alter_configs", "incrementalAlterConfigs",
     lambda a: a.incremental_alter_configs(configs={RESOURCE: [SET_OP]},
                                           options=AlterConfigsOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("alter_replica_log_dirs", "alterReplicaLogDirs",
     lambda a: a.alter_replica_log_dirs(replica_assignment={REPLICA: "/d"},
                                        options=AlterReplicaLogDirsOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("describe_log_dirs", "describeLogDirs",
     lambda a: a.describe_log_dirs(brokers=[1], options=DescribeLogDirsOptions(timeout_ms=SHORT)),
     lambda r: r.all_descriptions()),
    ("describe_replica_log_dirs", "describeReplicaLogDirs",
     lambda a: a.describe_replica_log_dirs(
         replicas=[REPLICA], options=DescribeReplicaLogDirsOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("create_partitions", "createPartitions",
     lambda a: a.create_partitions(new_partitions={"t": NewPartitions.increase_to(total_count=3)},
                                   options=CreatePartitionsOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("delete_records", "deleteRecords(api=METADATA)",
     lambda a: a.delete_records(records_to_delete={TP: RecordsToDelete.before_offset(offset=5)},
                                options=DeleteRecordsOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("elect_leaders", "electLeaders",
     lambda a: a.elect_leaders(election_type=ElectionType.PREFERRED, partitions={TP},
                               options=ElectLeadersOptions(timeout_ms=SHORT)),
     lambda r: r.partitions()),
    ("alter_partition_reassignments", "alterPartitionReassignments",
     lambda a: a.alter_partition_reassignments(
         reassignments={TP: NewPartitionReassignment(target_replicas=[1])},
         options=AlterPartitionReassignmentsOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("list_partition_reassignments", "listPartitionReassignments",
     lambda a: a.list_partition_reassignments(
         options=ListPartitionReassignmentsOptions(timeout_ms=SHORT)),
     lambda r: r.reassignments()),
    ("list_offsets", "listOffsets(api=METADATA)",
     lambda a: a.list_offsets(topic_partition_offsets={TP: OffsetSpec.latest()},
                              options=ListOffsetsOptions().timeout_ms(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("list_config_resources", "listConfigResources",
     lambda a: a.list_config_resources(config_resource_types=(),
                                       options=ListConfigResourcesOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
    ("unregister_controller", "unregisterController",
     lambda a: a.unregister_controller(controller_id=3,
                                       options=UnregisterControllerOptions(timeout_ms=SHORT)),
     lambda r: r.all()),
]


# ---- core gaps ---------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason=(
    "core gap: the Rust core's by-name describeTopics times out as 'listNodes' "
    "(its first step); Java's KafkaAdminClient sends it as "
    "'describeTopicPartitions' "
    "(KafkaAdminClient#generateDescribeTopicsCallWithDescribeTopicPartitionsApi)"))
def test_describe_topics_by_name_times_out_with_java_call_name() -> None:
    with _admin() as admin:
        result = admin.describe_topics(topic_names=["t"],
                                       options=DescribeTopicsOptions(timeout_ms=SHORT))
        all_names = result.all_topic_names()
        assert all_names is not None
        cause = assert_future_throws(all_names, KafkaTimeoutError)
        assert str(cause) == _timed_out("describeTopicPartitions")


# ---- signatures and forms ---------------------------------------------------


def test_positional_calls_raise_type_error() -> None:
    with _admin() as admin:
        calls: list[Callable[[], Any]] = [
            lambda: admin.delete_topics(["t"]),  # type: ignore[call-overload]
            lambda: admin.describe_topics(["t"]),  # type: ignore[call-overload]
            lambda: admin.describe_cluster(DescribeClusterOptions()),  # type: ignore[misc]
            lambda: admin.describe_configs([RESOURCE]),  # type: ignore[misc]
            lambda: admin.incremental_alter_configs({}),  # type: ignore[misc]
            lambda: admin.alter_replica_log_dirs({}),  # type: ignore[misc]
            lambda: admin.describe_log_dirs([1]),  # type: ignore[misc]
            lambda: admin.describe_replica_log_dirs([]),  # type: ignore[misc]
            lambda: admin.create_partitions({}),  # type: ignore[misc]
            lambda: admin.delete_records({}),  # type: ignore[misc]
            lambda: admin.elect_leaders(ElectionType.PREFERRED, None),  # type: ignore[misc]
            lambda: admin.alter_partition_reassignments({}),  # type: ignore[misc]
            lambda: admin.list_partition_reassignments(None),  # type: ignore[misc]
            lambda: admin.list_offsets({}),  # type: ignore[misc]
            lambda: admin.list_config_resources((), ListConfigResourcesOptions()),  # type: ignore[call-overload]
            lambda: admin.unregister_controller(1),  # type: ignore[misc]
        ]
        for call in calls:
            with pytest.raises(TypeError):
                call()


def test_describe_topics_forms() -> None:
    """Java has ``describeTopics(Collection<String>[, options])`` and
    ``describeTopics(TopicCollection[, options])``: one of ``topic_names`` /
    ``topics`` must be given, never both."""
    expected = ("describe_topics() takes one of (topic_names), (topic_names, options), "
                "(topics), (topics, options); got ")
    with _admin() as admin:
        with pytest.raises(IllegalArgumentError) as info:
            admin.describe_topics()  # type: ignore[call-overload]
        assert str(info.value) == expected + "()"
        with pytest.raises(IllegalArgumentError) as info:
            admin.describe_topics(topic_names=["a"],  # type: ignore[call-overload]
                                  topics=TopicCollection.of_topic_names(topics=["a"]))
        assert str(info.value) == expected + "(topic_names, topics)"
        with pytest.raises(IllegalArgumentError) as info:
            admin.describe_topics(options=DescribeTopicsOptions())  # type: ignore[call-overload]
        assert str(info.value) == expected + "(options)"


def test_list_config_resources_forms() -> None:
    """Java has ``listConfigResources(Set, options)`` and ``listConfigResources()``
    (which passes ``Set.of()``) only."""
    expected = ("list_config_resources() takes one of (config_resource_types, options), (); "
                "got ")
    with _admin() as admin:
        with pytest.raises(IllegalArgumentError) as info:
            admin.list_config_resources(  # type: ignore[call-overload]
                config_resource_types=[ConfigResource.Type.TOPIC])
        assert str(info.value) == expected + "(config_resource_types)"
        with pytest.raises(IllegalArgumentError) as info:
            admin.list_config_resources(  # type: ignore[call-overload]
                options=ListConfigResourcesOptions())
        assert str(info.value) == expected + "(options)"


def test_topic_collection_of_no_supported_class() -> None:
    """``KafkaAdminClient.deleteTopics`` / ``describeTopics`` reject a
    ``TopicCollection`` that is neither by name nor by id, with Java's message."""
    bare = TopicCollection.__new__(TopicCollection)
    with _admin() as admin:
        with pytest.raises(IllegalArgumentError) as info:
            admin.delete_topics(topics=bare)
        assert str(info.value) == ("The TopicCollection: " + str(bare) + " provided did not "
                                   "match any supported classes for deleteTopics.")
        with pytest.raises(IllegalArgumentError) as info:
            admin.describe_topics(topics=bare)
        assert str(info.value) == ("The TopicCollection: " + str(bare) + " provided did not "
                                   "match any supported classes for describeTopics.")


def test_calls_after_close_raise_illegal_state() -> None:
    admin = _admin()
    admin.close()
    for _name, _call, run, _future in _CALLS:
        with pytest.raises(IllegalStateError) as info:
            run(admin)
        assert str(info.value) == CLOSED
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.describe_topics(topic_names=["t"])
    # The closed check comes first, before Java's TopicCollection check.
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.delete_topics(topics=TopicCollection.__new__(TopicCollection))


# ---- the plain values each RPC hands the extension ---------------------------


def test_delete_and_describe_topics_args() -> None:
    by_ids, keys = _conversions.topic_collection_target(["a", "b"], "deleteTopics")
    assert (by_ids, keys) == (False, ["a", "b"])
    assert _conversions.delete_topics_args(["a", "b"], False, keys) == (
        _conversions.DELETE_TOPIC_NAMES, ["a", "b"])
    assert _conversions.delete_topics_args(
        TopicCollection.of_topic_names(topics=["a", "b"]), False, keys) == (
        _conversions.DELETE_TOPIC_NAME_COLLECTION, ["a", "b"])
    topic_id = Uuid.random_uuid()
    by_ids, keys = _conversions.topic_collection_target(
        TopicCollection.of_topic_ids(topics=[topic_id]), "deleteTopics")
    assert (by_ids, keys) == (True, [topic_id])
    assert _conversions.delete_topics_args(
        TopicCollection.of_topic_ids(topics=[topic_id]), True, keys) == (
        _conversions.DELETE_TOPIC_ID_COLLECTION, [str(topic_id)])
    names = TopicCollection.of_topic_names(topics=["x"])
    assert _conversions.topic_collection_target(names, "describeTopics") == (False, ["x"])
    assert _conversions.describe_topics_args(_conversions.DESCRIBE_TOPIC_ID_COLLECTION,
                                             [topic_id]) == (2, [str(topic_id)])
    assert _conversions.describe_topics_args(_conversions.DESCRIBE_TOPIC_NAMES, ["x"]) == (
        0, ["x"])
    assert _conversions.describe_topics_args(_conversions.DESCRIBE_TOPIC_NAME_COLLECTION,
                                             ["x"]) == (1, ["x"])


def test_cluster_and_config_args() -> None:
    broker = ConfigResource(type=ConfigResource.Type.BROKER, name="1")
    assert _conversions.describe_configs_args([RESOURCE, broker]) == ([(2, "t"), (4, "1")],)
    delete = AlterConfigOp(config_entry=ConfigEntry(name="cleanup.policy", value=None),
                           operation_type=AlterConfigOp.OpType.DELETE)
    # A resource with no op crosses too, as Java sends it with no config.
    args = _conversions.incremental_alter_configs_args({RESOURCE: [SET_OP, delete], broker: []})
    assert args == ([(2, "t", [("retention.ms", "1000", 0), ("cleanup.policy", None, 1)]),
                     (4, "1", [])],)
    assert _conversions.list_config_resources_args(
        [ConfigResource.Type.GROUP, ConfigResource.Type.TOPIC]) == ([32, 2],)
    assert _conversions.list_config_resources_args(()) == ([],)


def test_log_dir_and_partition_args() -> None:
    assert _conversions.alter_replica_log_dirs_args({REPLICA: "/data"}) == (
        [("t", 0, 1, "/data")],)
    assert _conversions.describe_log_dirs_args([1, 2]) == ([1, 2],)
    assert _conversions.describe_replica_log_dirs_args([REPLICA]) == ([("t", 0, 1)],)
    args = _conversions.create_partitions_args(
        {"a": NewPartitions.increase_to(total_count=3),
         "b": NewPartitions.increase_to(total_count=2, new_assignments=[[1, 2]]),
         "c": NewPartitions.increase_to(total_count=4, new_assignments=[])})
    assert args == ([("a", 3, None), ("b", 2, [[1, 2]]), ("c", 4, [])],)
    assert _conversions.delete_records_args({TP: RecordsToDelete.before_offset(offset=-1)}) == (
        [("t", 0, -1)],)


def test_election_reassignment_and_offset_args() -> None:
    tp1 = TopicPartition(topic="u", partition=3)
    assert _conversions.elect_leaders_args(ElectionType.UNCLEAN, None) == (1, None)
    assert _conversions.elect_leaders_args(ElectionType.PREFERRED, [tp1]) == (0, [("u", 3)])
    # Java's electLeaders(type, emptySet) is not its null: no partition, not all.
    assert _conversions.elect_leaders_args(ElectionType.PREFERRED, []) == (0, [])
    assert _conversions.alter_partition_reassignments_args(
        {TP: NewPartitionReassignment(target_replicas=[1, 2]), tp1: None}) == (
        [("t", 0, [1, 2]), ("u", 3, None)],)
    assert _conversions.list_partition_reassignments_args(None) == (None,)
    assert _conversions.list_partition_reassignments_args([tp1]) == ([("u", 3)],)
    args = _conversions.list_offsets_args(
        {TP: OffsetSpec.for_timestamp(timestamp=-2), tp1: OffsetSpec.earliest()})
    assert args == ([("t", 0, True, -2), ("u", 3, False, -2)],)
    # The caller adds the isolation level once it has read the options.
    assert ListOffsetsOptions(isolation_level=IsolationLevel.READ_COMMITTED
                              ).isolation_level().id() == 1


def test_offset_spec_values() -> None:
    """``KafkaAdminClient.getOffsetFromSpec``'s sentinels, the timestamp flagged."""
    assert _conversions.offset_spec_value(OffsetSpec.latest()) == (False, -1)
    assert _conversions.offset_spec_value(OffsetSpec.earliest()) == (False, -2)
    assert _conversions.offset_spec_value(OffsetSpec.max_timestamp()) == (False, -3)
    assert _conversions.offset_spec_value(OffsetSpec.earliest_local()) == (False, -4)
    assert _conversions.offset_spec_value(OffsetSpec.latest_tiered()) == (False, -5)
    assert _conversions.offset_spec_value(OffsetSpec.earliest_pending_upload()) == (False, -6)
    assert _conversions.offset_spec_value(OffsetSpec.for_timestamp(timestamp=42)) == (True, 42)
    # Java's public no-argument OffsetSpec() falls through to LATEST_TIMESTAMP.
    assert _conversions.offset_spec_value(OffsetSpec()) == (False, -1)


# ---- the setters that ran reach the extension ---------------------------------


_OPTIONS_WITH_EVERY_SETTER: list[tuple[str, Callable[[Any], Any], list[tuple[str, Any]]]] = [
    ("delete_topics",
     lambda a: a.delete_topics(topics=["t"], options=DeleteTopicsOptions(
         timeout_ms=1, retry_on_quota_violation=False)),
     [("timeout_ms", 1), ("retry_on_quota_violation", False)]),
    ("describe_topics",
     lambda a: a.describe_topics(topic_names=["t"], options=DescribeTopicsOptions(
         timeout_ms=1, include_authorized_operations=True, partition_size_limit_per_response=5)),
     [("timeout_ms", 1), ("include_authorized_operations", True),
      ("partition_size_limit_per_response", 5)]),
    ("describe_cluster",
     lambda a: a.describe_cluster(options=DescribeClusterOptions(
         timeout_ms=1, include_authorized_operations=True, include_fenced_brokers=True)),
     [("timeout_ms", 1), ("include_authorized_operations", True),
      ("include_fenced_brokers", True)]),
    ("describe_configs",
     lambda a: a.describe_configs(resources=[RESOURCE], options=DescribeConfigsOptions(
         timeout_ms=1, include_synonyms=True, include_documentation=True)),
     [("timeout_ms", 1), ("include_synonyms", True), ("include_documentation", True)]),
    ("incremental_alter_configs",
     lambda a: a.incremental_alter_configs(configs={RESOURCE: [SET_OP]},
                                           options=AlterConfigsOptions(timeout_ms=1,
                                                                       validate_only=True)),
     [("timeout_ms", 1), ("validate_only", True)]),
    ("create_partitions",
     lambda a: a.create_partitions(
         new_partitions={"t": NewPartitions.increase_to(total_count=3)},
         options=CreatePartitionsOptions(timeout_ms=1, validate_only=True,
                                         retry_on_quota_violation=False)),
     [("timeout_ms", 1), ("validate_only", True), ("retry_on_quota_violation", False)]),
    ("alter_partition_reassignments",
     lambda a: a.alter_partition_reassignments(
         reassignments={TP: None},
         options=AlterPartitionReassignmentsOptions(timeout_ms=1,
                                                    allow_replication_factor_change=False)),
     [("timeout_ms", 1), ("allow_replication_factor_change", False)]),
    ("list_offsets",
     lambda a: a.list_offsets(
         topic_partition_offsets={TP: OffsetSpec.latest()},
         options=ListOffsetsOptions(isolation_level=IsolationLevel.READ_COMMITTED).timeout_ms(
             timeout_ms=1)),
     [("timeout_ms", 1)]),
    ("unregister_controller",
     lambda a: a.unregister_controller(controller_id=1,
                                       options=UnregisterControllerOptions(timeout_ms=1)),
     [("timeout_ms", 1)]),
]


@pytest.mark.parametrize("name, run, ran", _OPTIONS_WITH_EVERY_SETTER,
                         ids=[c[0] for c in _OPTIONS_WITH_EVERY_SETTER])
def test_every_setter_reaches_the_extension(name: str, run: Callable[[Any], Any],
                                            ran: list[tuple[str, Any]]) -> None:
    """Each Options records the setters that ran, and the extension runs each on
    the C options (an unknown one would raise ``RuntimeError``)."""
    seen: list[list[tuple[str, Any]]] = []
    with _admin() as admin:
        call = admin._call

        def recording(entry: Any, *args: Any) -> Any:
            seen.append(args[-1])
            return call(entry, *args)

        admin._call = recording  # type: ignore[method-assign]
        try:
            run(admin)
        finally:
            del admin._call
    assert seen == [ran]


# ---- the value classes from the extension's plain values ----------------------


def test_topic_description_from_core_data() -> None:
    topic_id = Uuid.random_uuid()
    leader = (1, "h1", 9092, "r1")
    follower = (2, "h2", 9093, None)
    partition = (0, leader, [leader, follower], [leader], [follower], [])
    description = _conversions._topic_description(
        ("t", str(topic_id), 1, [partition, (1, None, [], [], None, None)], [3, 4]))
    assert description == TopicDescription(
        name="t", internal=True,
        partitions=[TopicPartitionInfo(
            partition=0, leader=Node(id=1, host="h1", port=9092, rack="r1"),
            replicas=[Node(id=1, host="h1", port=9092, rack="r1"),
                      Node(id=2, host="h2", port=9093, rack=None)],
            isr=[Node(id=1, host="h1", port=9092, rack="r1")],
            elr=[Node(id=2, host="h2", port=9093, rack=None)], last_known_elr=[]),
            TopicPartitionInfo(partition=1, leader=None, replicas=[], isr=[])],
        authorized_operations={AclOperation.READ, AclOperation.WRITE}, topic_id=topic_id)
    assert description.topic_id() == topic_id
    assert description.partitions()[1].elr() is None
    # Java's null authorizedOperations (not requested) stays None.
    unrequested = _conversions._topic_description(("t", str(topic_id), 0, [], None))
    assert unrequested.authorized_operations() is None


def test_cluster_nodes_carry_the_fenced_flag() -> None:
    """``KafkaAdminClient.describeCluster`` builds ``new Node(id, host, port, rack,
    isFenced)``."""
    nodes = _conversions._cluster_nodes([(1, "h", 9092, None, True), (2, "g", 9093, "r", False)])
    assert nodes == [Node(id=1, host="h", port=9092, rack=None, is_fenced=True),
                     Node(id=2, host="g", port=9093, rack="r", is_fenced=False)]
    assert nodes[0].is_fenced() and not nodes[1].is_fenced()
    assert _conversions._cluster_controller(None) is None


def test_config_from_core_data() -> None:
    config = _conversions.config_from_rows([
        ("retention.ms", "1000", 0, 0, 1, "DYNAMIC_TOPIC_CONFIG", "LONG", "how long",
         [("retention.ms", "1000", "DYNAMIC_TOPIC_CONFIG"),
          ("log.retention.ms", None, "DEFAULT_CONFIG")]),
        ("password", None, 1, 1, 0, "DEFAULT_CONFIG", "PASSWORD", None, [])])
    assert isinstance(config, Config)
    entry = config.get(name="retention.ms")
    assert entry is not None
    assert entry.value() == "1000"
    assert entry.source() is ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG
    assert entry.type() is ConfigEntry.ConfigType.LONG
    assert entry.is_read_only() and not entry.is_sensitive() and not entry.is_default()
    assert entry.documentation() == "how long"
    assert [(s.name(), s.value(), s.source()) for s in entry.synonyms()] == [
        ("retention.ms", "1000", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG),
        ("log.retention.ms", None, ConfigEntry.ConfigSource.DEFAULT_CONFIG)]
    password = config.get(name="password")
    assert password is not None and password.is_default() and password.is_sensitive()
    assert password.value() is None and password.documentation() is None
    assert _conversions.config_resource_from_tuple((4, "1")) == ConfigResource(
        type=ConfigResource.Type.BROKER, name="1")
    assert _conversions.config_resource_from_tuple((99, "x")).type() is (
        ConfigResource.Type.UNKNOWN)


def test_config_from_core_data_keeps_a_null_type() -> None:
    """A ``createTopics`` entry has Java's null type (``configEntry`` in ``KafkaAdminClient#getCreateTopicsCall``):
    the extension hands it over as ``None`` and the entry keeps it, printing ``type=None``."""
    config = _conversions.config_from_rows([
        ("cleanup.policy", "compact", 0, 0, 1, "DYNAMIC_TOPIC_CONFIG", None, None, [])])
    entry = config.get(name="cleanup.policy")
    assert entry is not None
    assert entry.type() is None
    assert entry != ConfigEntry(name="cleanup.policy", value="compact",
                                source=ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
                                is_sensitive=False, is_read_only=True, synonyms=[],
                                type=ConfigEntry.ConfigType.UNKNOWN, documentation=None)
    assert str(entry) == (
        "ConfigEntry(name=cleanup.policy, value=compact, source=DYNAMIC_TOPIC_CONFIG, "
        "isSensitive=False, isReadOnly=True, synonyms=[], type=None, documentation=None)")


#: ``new ConfigEntry(name, value)`` rows of a core ``Config`` (Admin__config_future).
CORE_CONFIG = [("retention.ms", "4242"), ("cleanup.policy", None)]


def _assert_core_config(config: Config) -> None:
    """The entries of :data:`CORE_CONFIG` as Java's two-argument ``ConfigEntry``
    holds them: source and type ``UNKNOWN``, no documentation, no synonyms."""
    assert isinstance(config, Config)
    assert sorted(entry.name() for entry in config.entries()) == [
        "cleanup.policy", "retention.ms"]
    entry = config.get(name="retention.ms")
    assert entry is not None and entry.value() == "4242"
    assert entry.source() is ConfigEntry.ConfigSource.UNKNOWN
    assert entry.type() is ConfigEntry.ConfigType.UNKNOWN
    assert entry.documentation() is None and entry.synonyms() == []
    assert not entry.is_default() and not entry.is_sensitive() and not entry.is_read_only()
    policy = config.get(name="cleanup.policy")
    assert policy is not None and policy.value() is None


class _ConfigNative:
    """An extension ``AdminResult`` stand-in whose ``config`` / ``values`` futures
    are completed core ``Config`` futures converted by the C converter the real
    result's table names for that accessor (``Admin__config_future``); the other
    futures are completed with ``None``."""

    def __init__(self, result: str) -> None:
        self._result = result

    def futures(self, name: str, post: Any = None) -> dict[Any, Any]:
        if self._result == "DescribeConfigsResult":
            return {(2, "t"): _lib.Admin__config_future(self._result, name, CORE_CONFIG, post)}
        return {"t": _lib.KafkaFuture.completed_future(None)}

    def future(self, name: str, key: Any = None, post: Any = None) -> Any:
        return _lib.Admin__config_future(self._result, name, CORE_CONFIG, post)


@pytest.mark.parametrize("kind", [Sync, Async], ids=["sync", "async"])
def test_create_topics_config_reads_the_rows_of_its_c_converter(kind: Any) -> None:
    """``CreateTopicsResult.config(topic)`` through its real delivery: the converter
    the C table gives the ``config`` accessor makes the rows its Python ``post``
    reads (they once disagreed, five fields against nine, and every created topic's
    ``config()`` failed with ``ValueError``)."""
    async def read() -> Config:
        result = _conversions.create_topics_result(
            _ConfigNative("CreateTopicsResult"), kind, asyncio.get_running_loop())
        config: Config = await result.config(topic="t").get(timeout=5)
        return config

    if kind is Sync:
        result = _conversions.create_topics_result(_ConfigNative("CreateTopicsResult"), kind, None)
        _assert_core_config(result.config(topic="t").get(timeout=5))
    else:
        _assert_core_config(asyncio.run(read()))


def test_describe_configs_values_read_the_rows_of_their_c_converter() -> None:
    """``DescribeConfigsResult.values()`` through its real delivery, as above."""
    result = _conversions.describe_configs_result(
        _ConfigNative("DescribeConfigsResult"), Sync, None)
    resource = ConfigResource(type=ConfigResource.Type.TOPIC, name="t")
    _assert_core_config(result.values()[resource].get(timeout=5))


def test_config_future_hook_names_only_config_accessors() -> None:
    """``Admin__config_future`` reads a Result table it knows, by accessor name."""
    with pytest.raises(ValueError, match="ListTopicsResult has no Config accessor"):
        _lib.Admin__config_future("ListTopicsResult", "names", [], None)
    with pytest.raises(AttributeError, match="CreateTopicsResult has no accessor nope"):
        _lib.Admin__config_future("CreateTopicsResult", "nope", [], None)


def test_log_dirs_from_core_data() -> None:
    raw = {"/a": (None, 100, 40, [("t", 0, 10, 0, 0), ("t", 1, 20, 5, 1)], False),
           "/b": (_raw_error(KafkaStorageError, "offline"), -1, -1, [], True)}
    dirs = _conversions._log_dir_descriptions(raw)
    assert set(dirs) == {"/a", "/b"}
    first: LogDirDescription = dirs["/a"]
    assert first.error() is None
    assert first.total_bytes() == 100 and first.usable_bytes() == 40
    assert first.is_cordoned() is False
    infos = first.replica_infos()
    assert {tp: (i.size(), i.offset_lag(), i.is_future()) for tp, i in infos.items()} == {
        TopicPartition(topic="t", partition=0): (10, 0, False),
        TopicPartition(topic="t", partition=1): (20, 5, True)}
    assert all(type(i) is ReplicaInfo for i in infos.values())
    offline: LogDirDescription = dirs["/b"]
    assert type(offline.error()) is KafkaStorageError
    assert offline.total_bytes() is None and offline.usable_bytes() is None
    # ``kafka_admin_LogDirDescription_is_cordoned``: Java's five-argument form.
    assert offline.is_cordoned() is True
    # A log-dir error that is no ApiError cannot sit in LogDirDescription.error(),
    # so the broker's future fails with it.
    not_api = (UnknownServerError._ffi_id, "bare", 0, 0, None, 0, None)
    with pytest.raises(Exception) as info:
        _conversions._log_dir_descriptions({"/c": (not_api, 0, 0, [], False)})
    assert type(info.value).__name__ == "KafkaError"


def test_replica_log_dir_info_offsets_and_records_from_core_data() -> None:
    info = _conversions._replica_log_dir_info(("/a", 0, "/b", 7))
    assert isinstance(info, DescribeReplicaLogDirsResult.ReplicaLogDirInfo)
    assert (info.current_replica_log_dir(), info.current_replica_offset_lag(),
            info.future_replica_log_dir(), info.future_replica_offset_lag()) == ("/a", 0, "/b", 7)
    missing = _conversions._replica_log_dir_info((None, -1, None, -1))
    assert missing.current_replica_log_dir() is None
    listed = _conversions._listed_offset((12, 34, None))
    assert isinstance(listed, ListOffsetsResult.ListOffsetsResultInfo)
    assert (listed.offset(), listed.timestamp(), listed.leader_epoch()) == (12, 34, None)
    assert _conversions._listed_offset((12, 34, 5)).leader_epoch() == 5
    deleted = _conversions._deleted_records(9)
    assert type(deleted) is DeletedRecords and deleted.low_watermark() == 9
    reassignments = _conversions._reassignments({("t", 0): ([1, 2, 3], [3], [1])})
    assert set(reassignments) == {TP}
    assert type(reassignments[TP]) is PartitionReassignment
    assert (reassignments[TP].replicas(), reassignments[TP].adding_replicas(),
            reassignments[TP].removing_replicas()) == ([1, 2, 3], [3], [1])
    elections = _conversions._elections({("t", 0): None,
                                         ("u", 1): _raw_error(UnknownTopicOrPartitionError, "x")})
    assert elections[TP] is None
    assert type(elections[TopicPartition(topic="u", partition=1)]) is (
        UnknownTopicOrPartitionError)
    resources = _conversions._config_resources([(2, "t"), (4, "1")])
    assert resources == [RESOURCE, ConfigResource(type=ConfigResource.Type.BROKER, name="1")]


# ---- returned at once, failing later with the call timeout --------------------


@pytest.mark.parametrize("name, call, run, future", _CALLS, ids=[c[0] for c in _CALLS])
def test_call_fails_with_the_call_timeout(name: str, call: str, run: Callable[[Any], Any],
                                          future: Callable[[Any], Any]) -> None:
    with _admin() as admin:
        started = time.monotonic()
        result = run(admin)
        assert time.monotonic() - started < 1
        outcome = future(result)
        assert isinstance(outcome, KafkaFuture)
        cause = assert_future_throws(outcome, KafkaTimeoutError)
        assert str(cause) == _timed_out(call)


def test_per_key_futures_are_keyed_as_requested() -> None:
    topic_id = Uuid.random_uuid()
    with _admin() as admin:
        deleted = admin.delete_topics(topics=TopicCollection.of_topic_ids(topics=[topic_id]),
                                      options=DeleteTopicsOptions(timeout_ms=SHORT))
        assert deleted.topic_name_values() is None
        assert set(deleted.topic_id_values() or {}) == {topic_id}
        by_name = admin.delete_topics(topics=["a"], options=DeleteTopicsOptions(timeout_ms=SHORT))
        assert by_name.topic_id_values() is None
        assert set(by_name.topic_name_values() or {}) == {"a"}
        described = admin.describe_topics(
            topics=TopicCollection.of_topic_names(topics=["a", "b"]),
            options=DescribeTopicsOptions(timeout_ms=SHORT))
        assert described.topic_id_values() is None
        assert set(described.topic_name_values() or {}) == {"a", "b"}
        configs = admin.describe_configs(resources=[RESOURCE],
                                         options=DescribeConfigsOptions(timeout_ms=SHORT))
        assert set(configs.values()) == {RESOURCE}
        altered = admin.incremental_alter_configs(configs={RESOURCE: []},
                                                  options=AlterConfigsOptions(timeout_ms=SHORT))
        assert set(altered.values()) == {RESOURCE}
        replicas = admin.describe_replica_log_dirs(
            replicas=[REPLICA], options=DescribeReplicaLogDirsOptions(timeout_ms=SHORT))
        assert set(replicas.values()) == {REPLICA}
        log_dirs = admin.describe_log_dirs(brokers=[1, 2],
                                           options=DescribeLogDirsOptions(timeout_ms=SHORT))
        assert set(log_dirs.descriptions()) == {1, 2}
        records = admin.delete_records(records_to_delete={TP: RecordsToDelete.before_offset(
            offset=1)}, options=DeleteRecordsOptions(timeout_ms=SHORT))
        assert set(records.low_watermarks()) == {TP}
        offsets = admin.list_offsets(topic_partition_offsets={TP: OffsetSpec.earliest()},
                                     options=ListOffsetsOptions().timeout_ms(timeout_ms=SHORT))
        assert_future_throws(offsets.partition_result(partition=TP), KafkaTimeoutError)


def test_default_options_use_default_api_timeout() -> None:
    """Every short form passes a fresh Options, so the call takes
    ``default.api.timeout.ms``."""
    with _admin(**{"default.api.timeout.ms": str(SHORT), "request.timeout.ms": "100"}) as admin:
        short_forms: list[Callable[[], Any]] = [
            lambda: admin.delete_topics(topics=["t"]).all(),
            lambda: admin.describe_cluster().cluster_id(),
            lambda: admin.describe_configs(resources=[RESOURCE]).all(),
            lambda: admin.describe_log_dirs(brokers=[1]).all_descriptions(),
            lambda: admin.elect_leaders(election_type=ElectionType.PREFERRED,
                                        partitions=None).partitions(),
            lambda: admin.list_partition_reassignments(partitions=[TP]).reassignments(),
            lambda: admin.list_config_resources().all(),
            lambda: admin.unregister_controller(controller_id=1).all(),
        ]
        for short_form in short_forms:
            assert_future_throws(short_form(), KafkaTimeoutError)


# ---- the async peers --------------------------------------------------------


async def test_async_calls_fail_with_the_call_timeout() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        for name, call, run, future in _CALLS:
            result = run(admin)
            outcome = future(result)
            assert isinstance(outcome, AsyncKafkaFuture), name
            cause = await async_assert_future_throws(outcome, KafkaTimeoutError)
            assert str(cause) == _timed_out(call), name
        described = admin.describe_topics(topic_names=["t"],
                                          options=DescribeTopicsOptions(timeout_ms=SHORT))
        names = described.topic_name_values()
        assert names is not None
        await async_assert_future_throws(names["t"], KafkaTimeoutError)


async def test_async_signatures_forms_and_closed() -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    with pytest.raises(TypeError):
        admin.delete_topics(["t"])  # type: ignore[call-overload]
    with pytest.raises(TypeError):
        admin.list_offsets({})  # type: ignore[misc]
    with pytest.raises(IllegalArgumentError) as info:
        admin.describe_topics()  # type: ignore[call-overload]
    assert str(info.value).endswith("; got ()")
    with pytest.raises(IllegalArgumentError) as info:
        admin.list_config_resources(  # type: ignore[call-overload]
            options=ListConfigResourcesOptions())
    assert str(info.value) == ("list_config_resources() takes one of "
                               "(config_resource_types, options), (); got (options)")
    await admin.close()
    for _name, _call, run, _future in _CALLS:
        with pytest.raises(IllegalStateError) as closed:
            run(admin)
        assert str(closed.value) == CLOSED


async def test_async_cluster_futures() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        cluster = admin.describe_cluster(options=DescribeClusterOptions(timeout_ms=SHORT))
        for future in (cluster.nodes(), cluster.controller(), cluster.cluster_id(),
                       cluster.authorized_operations()):
            assert isinstance(future, AsyncKafkaFuture)
        results = await asyncio.gather(
            *(async_assert_future_throws(f, KafkaTimeoutError)
              for f in (cluster.nodes(), cluster.controller(), cluster.cluster_id(),
                        cluster.authorized_operations())))
        assert all(str(r) == _timed_out("listNodes") for r in results)
