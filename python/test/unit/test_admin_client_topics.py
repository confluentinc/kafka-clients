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
reassignment and offset RPCs, without a broker:
``deleteTopics``, ``describeTopics``, ``describeCluster``, ``describeConfigs``,
``incrementalAlterConfigs``, ``alterReplicaLogDirs``, ``describeLogDirs``,
``describeReplicaLogDirs``, ``createPartitions``, ``deleteRecords``,
``electLeaders``, ``alterPartitionReassignments``, ``listPartitionReassignments``,
``listOffsets`` and ``listConfigResources``, sync and async.

The ``KafkaAdminClientTest.java`` (Apache Kafka 4.3.1) tests of these RPCs, ported
or skipped with the reason, are listed in the inventory of all its tests in
``test_admin_java_ports.py``'s module docstring, which also holds the ports that need
no scripted broker (``testInvalidTopicNames``' ``deleteTopics`` / ``describeTopics``
parts, ``testDescribeTopicsTimeoutWhenNoBrokerResponds``); the ``MockClient``-scripted
ones are translated in the core, ``rust/src/admin/kafka_admin_client.rs``.

The binding's own part is tested here: keyword-only signatures and the
``java_forms`` messages, the closed client, the plain values each RPC hands the
extension for its options (read from the conversions, not from a broker), the
results built from the core's per-key and whole-call errors, and the call timeout
with the core's message.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

import _confluentkafka as _lib  # type: ignore[import-not-found]
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
)
from confluent_kafka.admin import _conversions
from confluent_kafka.admin._futures import Async, Sync
from confluent_kafka.common import (
    AclOperation,
    ElectionType,
    IsolationLevel,
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
    TopicAuthorizationError,
    UnknownServerError,
    UnknownTopicOrPartitionError,
)
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError

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


def _whole_call_error(message: str = "boom") -> tuple[int, int]:
    """A ``(result_int, error_int)`` payload failed as a whole (an owned handle)."""
    return 0, _lib.KafkaError_new(UnknownServerError._ffi_id, message)


def _raw_error(error_class: type[BaseException], message: str) -> tuple[Any, ...]:
    """A per-key error as a drain gives it (the borrowed form of ``from_ffi_error``)."""
    return (error_class._ffi_id, message, 0, 0, None, 1, None)  # type: ignore[attr-defined]


# Each RPC once, with a short call timeout: (Java call name in the core's message,
# the call, the future to read). The sync and async tests share it.
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
]


# ---- core gaps ---------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason=(
    "core gap: the Rust core names the by-name describeTopics call 'describeTopics'; "
    "Java's KafkaAdminClient sends it as 'describeTopicPartitions' "
    "(KafkaAdminClient.java:2231)"))
def test_describe_topics_by_name_times_out_with_java_call_name() -> None:
    with _admin() as admin:
        result = admin.describe_topics(topic_names=["t"],
                                       options=DescribeTopicsOptions(timeout_ms=SHORT))
        all_names = result.all_topic_names()
        assert all_names is not None
        with pytest.raises(KafkaTimeoutError) as info:
            all_names.result()
        assert str(info.value) == _timed_out("describeTopicPartitions")


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


# ---- options reach the extension --------------------------------------------


def test_delete_and_describe_topics_args() -> None:
    by_ids, keys = _conversions.topic_collection_target(["a", "b"], "deleteTopics")
    assert (by_ids, keys) == (False, ["a", "b"])
    options = DeleteTopicsOptions(timeout_ms=7, retry_on_quota_violation=False)
    assert _conversions.delete_topics_args(False, keys, options) == (["a", "b"], 7, False)
    assert _conversions.delete_topics_args(False, keys, DeleteTopicsOptions()) == (
        ["a", "b"], -1, True)
    topic_id = Uuid.random_uuid()
    by_ids, keys = _conversions.topic_collection_target(
        TopicCollection.of_topic_ids(topics=[topic_id]), "deleteTopics")
    assert (by_ids, keys) == (True, [topic_id])
    assert _conversions.delete_topics_args(True, keys, options) == ([str(topic_id)], 7, False)
    names = TopicCollection.of_topic_names(topics=["x"])
    assert _conversions.topic_collection_target(names, "describeTopics") == (False, ["x"])
    described = DescribeTopicsOptions(timeout_ms=9, include_authorized_operations=True,
                                      partition_size_limit_per_response=5)
    assert _conversions.describe_topics_args(True, [topic_id], described) == (
        [str(topic_id)], 9, True, 5)
    assert _conversions.describe_topics_args(False, ["x"], DescribeTopicsOptions()) == (
        ["x"], -1, False, 2000)


def test_cluster_and_config_args() -> None:
    assert _conversions.describe_cluster_args(DescribeClusterOptions(
        timeout_ms=3, include_authorized_operations=True, include_fenced_brokers=True)) == (
        3, True, True)
    assert _conversions.describe_cluster_args(DescribeClusterOptions()) == (-1, False, False)
    broker = ConfigResource(type=ConfigResource.Type.BROKER, name="1")
    assert _conversions.describe_configs_args([RESOURCE, broker], DescribeConfigsOptions(
        include_synonyms=True, include_documentation=True)) == (
        [(2, "t"), (4, "1")], -1, True, True)
    delete = AlterConfigOp(config_entry=ConfigEntry(name="cleanup.policy", value=None),
                           operation_type=AlterConfigOp.OpType.DELETE)
    args = _conversions.incremental_alter_configs_args(
        {RESOURCE: [SET_OP, delete], broker: []}, AlterConfigsOptions(validate_only=True))
    assert args == ([(2, "t", "retention.ms", "1000", 0), (2, "t", "cleanup.policy", None, 1)],
                    -1, True)
    assert _conversions.list_config_resources_args(
        [ConfigResource.Type.GROUP, ConfigResource.Type.TOPIC],
        ListConfigResourcesOptions(timeout_ms=4)) == ([32, 2], 4)
    assert _conversions.list_config_resources_args((), ListConfigResourcesOptions()) == ([], -1)


def test_log_dir_and_partition_args() -> None:
    assert _conversions.alter_replica_log_dirs_args(
        {REPLICA: "/data"}, AlterReplicaLogDirsOptions(timeout_ms=8)) == (
        [("t", 0, 1, "/data")], 8)
    assert _conversions.describe_log_dirs_args([1, 2], DescribeLogDirsOptions()) == ([1, 2], -1)
    assert _conversions.describe_replica_log_dirs_args(
        [REPLICA], DescribeReplicaLogDirsOptions(timeout_ms=0)) == ([("t", 0, 1)], 0)
    args = _conversions.create_partitions_args(
        {"a": NewPartitions.increase_to(total_count=3),
         "b": NewPartitions.increase_to(total_count=2, new_assignments=[[1, 2]]),
         "c": NewPartitions.increase_to(total_count=4, new_assignments=[])},
        CreatePartitionsOptions(validate_only=True, retry_on_quota_violation=False))
    assert args == ([("a", 3, None), ("b", 2, [[1, 2]]), ("c", 4, [])], -1, True, False)
    assert _conversions.delete_records_args(
        {TP: RecordsToDelete.before_offset(offset=-1)}, DeleteRecordsOptions(timeout_ms=1)) == (
        [("t", 0, -1)], 1)


def test_election_reassignment_and_offset_args() -> None:
    tp1 = TopicPartition(topic="u", partition=3)
    assert _conversions.elect_leaders_args(ElectionType.UNCLEAN, None, ElectLeadersOptions()) == (
        1, True, [], -1)
    assert _conversions.elect_leaders_args(ElectionType.PREFERRED, [tp1],
                                           ElectLeadersOptions(timeout_ms=5)) == (
        0, False, [("u", 3)], 5)
    # Java's electLeaders(type, emptySet) is not its null: no partition, not all.
    assert _conversions.elect_leaders_args(ElectionType.PREFERRED, [],
                                           ElectLeadersOptions())[1:3] == (False, [])
    assert _conversions.alter_partition_reassignments_args(
        {TP: NewPartitionReassignment(target_replicas=[1, 2]), tp1: None},
        AlterPartitionReassignmentsOptions(allow_replication_factor_change=False)) == (
        [("t", 0, False, [1, 2]), ("u", 3, True, [])], -1, False)
    assert _conversions.list_partition_reassignments_args(
        None, ListPartitionReassignmentsOptions()) == (True, [], -1)
    assert _conversions.list_partition_reassignments_args(
        [tp1], ListPartitionReassignmentsOptions(timeout_ms=2)) == (False, [("u", 3)], 2)
    args = _conversions.list_offsets_args(
        {TP: OffsetSpec.for_timestamp(timestamp=-2), tp1: OffsetSpec.earliest()},
        ListOffsetsOptions(isolation_level=IsolationLevel.READ_COMMITTED))
    assert args == ([("t", 0, True, -2), ("u", 3, False, -2)], -1, 1)


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


# ---- results from core data -------------------------------------------------


def test_topic_description_from_core_data() -> None:
    topic_id = Uuid.random_uuid()
    leader = (1, "h1", 9092, "r1")
    follower = (2, "h2", 9093, None)
    partition = (0, leader, [leader, follower], [leader], [follower], [])
    error, description = _conversions._topic_description(
        (None, ("t", str(topic_id), 1, [partition, (1, None, [], [], None, None)], [3, 4])))
    assert error is None
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
    _, unrequested = _conversions._topic_description((None, ("t", str(topic_id), 0, [], None)))
    assert unrequested.authorized_operations() is None
    raw = _raw_error(UnknownTopicOrPartitionError, "unknown")
    error, value = _conversions._topic_description((raw, None))
    assert type(error) is UnknownTopicOrPartitionError and value is None


def test_config_from_core_data() -> None:
    error, config = _conversions._described_config((None, [
        ("retention.ms", "1000", 0, 0, 1, "DYNAMIC_TOPIC_CONFIG", "LONG", "how long",
         [("retention.ms", "1000", "DYNAMIC_TOPIC_CONFIG"),
          ("log.retention.ms", None, "DEFAULT_CONFIG")]),
        ("password", None, 1, 1, 0, "DEFAULT_CONFIG", "PASSWORD", None, [])]))
    assert error is None
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


def test_log_dirs_from_core_data() -> None:
    raw = {"/a": (None, 100, 40, [("t", 0, 10, 0, 0), ("t", 1, 20, 5, 1)]),
           "/b": (_raw_error(KafkaStorageError, "offline"), -1, -1, [])}
    error, dirs = _conversions._log_dir_descriptions((None, raw))
    assert error is None
    assert set(dirs) == {"/a", "/b"}
    first: LogDirDescription = dirs["/a"]
    assert first.error() is None
    assert first.total_bytes() == 100 and first.usable_bytes() == 40
    infos = first.replica_infos()
    assert {tp: (i.size(), i.offset_lag(), i.is_future()) for tp, i in infos.items()} == {
        TopicPartition(topic="t", partition=0): (10, 0, False),
        TopicPartition(topic="t", partition=1): (20, 5, True)}
    assert all(type(i) is ReplicaInfo for i in infos.values())
    offline: LogDirDescription = dirs["/b"]
    assert type(offline.error()) is KafkaStorageError
    assert offline.total_bytes() is None and offline.usable_bytes() is None
    # A log-dir error that is no ApiError cannot sit in LogDirDescription.error()
    # so the broker's future fails with it.
    not_api = (UnknownServerError._ffi_id, "bare", 0, 0, None, 0, None)
    error, dirs = _conversions._log_dir_descriptions((None, {"/c": (not_api, 0, 0, [])}))
    assert type(error).__name__ == "KafkaError" and dirs is None


def test_replica_log_dir_info_offsets_and_records_from_core_data() -> None:
    error, info = _conversions._replica_log_dir_info((None, ("/a", 0, "/b", 7)))
    assert error is None
    assert isinstance(info, DescribeReplicaLogDirsResult.ReplicaLogDirInfo)
    assert (info.current_replica_log_dir(), info.current_replica_offset_lag(),
            info.future_replica_log_dir(), info.future_replica_offset_lag()) == ("/a", 0, "/b", 7)
    error, missing = _conversions._replica_log_dir_info((None, (None, -1, None, -1)))
    assert missing.current_replica_log_dir() is None
    error, listed = _conversions._listed_offset((None, (12, 34, None)))
    assert isinstance(listed, ListOffsetsResult.ListOffsetsResultInfo)
    assert (listed.offset(), listed.timestamp(), listed.leader_epoch()) == (12, 34, None)
    error, listed = _conversions._listed_offset((None, (12, 34, 5)))
    assert listed.leader_epoch() == 5
    error, deleted = _conversions._deleted_records((None, 9))
    assert error is None and type(deleted) is DeletedRecords
    assert deleted.low_watermark() == 9
    error, deleted = _conversions._deleted_records(
        (_raw_error(TopicAuthorizationError, "Not authorized to access topics: [t]"), -1))
    assert type(error) is TopicAuthorizationError and deleted is None
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


def test_whole_call_error_fails_every_requested_key() -> None:
    """A call the core rejects as a whole (``error_int`` set) fails every future of its
    result with that error, keyed as requested (Java's ``completeAllExceptionally``)."""
    topic_id = Uuid.random_uuid()
    deleted = _conversions.delete_topics_result(True, [topic_id], _whole_call_error(), Sync)
    assert deleted.topic_name_values() is None
    by_id = deleted.topic_id_values()
    assert by_id is not None and set(by_id) == {topic_id}
    with pytest.raises(UnknownServerError, match="boom"):
        by_id[topic_id].result()
    described = _conversions.describe_topics_result(False, ["t"], _whole_call_error(), Sync)
    assert described.topic_id_values() is None
    configs = _conversions.describe_configs_result([RESOURCE], _whole_call_error(), Sync)
    assert set(configs.values()) == {RESOURCE}
    altered = _conversions.incremental_alter_configs_result([RESOURCE], _whole_call_error(),
                                                            Sync)
    assert set(altered.values()) == {RESOURCE}
    replicas = _conversions.alter_replica_log_dirs_result([REPLICA], _whole_call_error(), Sync)
    assert set(replicas.values()) == {REPLICA}
    log_dirs = _conversions.describe_log_dirs_result([1, 2], _whole_call_error(), Sync)
    assert set(log_dirs.descriptions()) == {1, 2}
    replica_dirs = _conversions.describe_replica_log_dirs_result([REPLICA], _whole_call_error(),
                                                                 Sync)
    assert set(replica_dirs.values()) == {REPLICA}
    partitions = _conversions.create_partitions_result(["t"], _whole_call_error(), Sync)
    assert set(partitions.values()) == {"t"}
    records = _conversions.delete_records_result([TP], _whole_call_error(), Sync)
    assert set(records.low_watermarks()) == {TP}
    reassigned = _conversions.alter_partition_reassignments_result([TP], _whole_call_error(),
                                                                   Sync)
    assert set(reassigned.values()) == {TP}
    offsets = _conversions.list_offsets_result([TP], _whole_call_error(), Sync)
    with pytest.raises(UnknownServerError, match="boom"):
        offsets.partition_result(partition=TP).result()
    for single in (_conversions.elect_leaders_result(_whole_call_error(), Sync).partitions(),
                   _conversions.list_partition_reassignments_result(
                       _whole_call_error(), Sync).reassignments(),
                   _conversions.list_config_resources_result(_whole_call_error(), Sync).all()):
        with pytest.raises(UnknownServerError, match="boom"):
            single.result()
    cluster = _conversions.describe_cluster_result(_whole_call_error(), Sync)
    for future in (cluster.nodes(), cluster.controller(), cluster.cluster_id(),
                   cluster.authorized_operations()):
        with pytest.raises(UnknownServerError, match="boom"):
            future.result()


# ---- the call timeout, sync and async ---------------------------------------


@pytest.mark.parametrize("name, call, run, future", _CALLS, ids=[c[0] for c in _CALLS])
def test_call_fails_with_the_call_timeout(name: str, call: str, run: Callable[[Any], Any],
                                          future: Callable[[Any], Any]) -> None:
    with _admin() as admin:
        result = run(admin)
        outcome = future(result)
        assert outcome.done()
        with pytest.raises(KafkaTimeoutError) as info:
            outcome.result()
        assert str(info.value) == _timed_out(call)


def test_per_key_futures_are_keyed_as_requested() -> None:
    topic_id = Uuid.random_uuid()
    with _admin() as admin:
        deleted = admin.delete_topics(topics=TopicCollection.of_topic_ids(topics=[topic_id]),
                                      options=DeleteTopicsOptions(timeout_ms=SHORT))
        assert deleted.topic_name_values() is None
        assert set(deleted.topic_id_values() or {}) == {topic_id}
        described = admin.describe_topics(
            topics=TopicCollection.of_topic_names(topics=["a", "b"]),
            options=DescribeTopicsOptions(timeout_ms=SHORT))
        assert described.topic_id_values() is None
        assert set(described.topic_name_values() or {}) == {"a", "b"}
        configs = admin.describe_configs(resources=[RESOURCE],
                                         options=DescribeConfigsOptions(timeout_ms=SHORT))
        assert set(configs.values()) == {RESOURCE}
        replicas = admin.describe_replica_log_dirs(
            replicas=[REPLICA], options=DescribeReplicaLogDirsOptions(timeout_ms=SHORT))
        assert set(replicas.values()) == {REPLICA}
        offsets = admin.list_offsets(topic_partition_offsets={TP: OffsetSpec.earliest()},
                                     options=ListOffsetsOptions().timeout_ms(timeout_ms=SHORT))
        with pytest.raises(KafkaTimeoutError):
            offsets.partition_result(partition=TP).result()


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
        ]
        for short_form in short_forms:
            with pytest.raises(KafkaTimeoutError):
                short_form().result()


def test_free_functions_free_their_own_payloads() -> None:
    """Each ``free_x`` frees a real payload of its own RPC (the late result of an
    interrupted call), through that RPC's drain."""
    frees = {
        "delete_topics": _conversions.free_delete_topics,
        "delete_topics_by_id": _conversions.free_delete_topics,
        "describe_topics_by_id": _conversions.free_describe_topics,
        "describe_cluster": _conversions.free_describe_cluster,
        "describe_configs": _conversions.free_describe_configs,
        "incremental_alter_configs": _conversions.free_incremental_alter_configs,
        "alter_replica_log_dirs": _conversions.free_alter_replica_log_dirs,
        "describe_log_dirs": _conversions.free_describe_log_dirs,
        "describe_replica_log_dirs": _conversions.free_describe_replica_log_dirs,
        "create_partitions": _conversions.free_create_partitions,
        "delete_records": _conversions.free_delete_records,
        "elect_leaders": _conversions.free_elect_leaders,
        "alter_partition_reassignments": _conversions.free_alter_partition_reassignments,
        "list_partition_reassignments": _conversions.free_list_partition_reassignments,
        "list_offsets": _conversions.free_list_offsets,
        "list_config_resources": _conversions.free_list_config_resources,
    }
    assert set(frees) == {c[0] for c in _CALLS}
    with _admin() as admin:
        real_wait_for = admin._wait_for
        for name, _call, run, _future in _CALLS:
            raw: list[Any] = []

            def keep(submit: Any, free: Any) -> Any:
                raw.append(real_wait_for(submit, lambda payload: None))
                raise _Stop

            admin._wait_for = keep  # type: ignore[method-assign]
            try:
                with pytest.raises(_Stop):
                    run(admin)
            finally:
                del admin._wait_for
            frees[name](raw[0])


class _Stop(Exception):
    pass


async def test_async_calls_fail_with_the_call_timeout() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        for name, call, run, future in _CALLS:
            result = await run(admin)
            outcome = future(result)
            assert isinstance(outcome, asyncio.Future) and outcome.done(), name
            with pytest.raises(KafkaTimeoutError) as info:
                await outcome
            assert str(info.value) == _timed_out(call), name
        described = await admin.describe_topics(topic_names=["t"],
                                                options=DescribeTopicsOptions(timeout_ms=SHORT))
        names = described.topic_name_values()
        assert names is not None
        with pytest.raises(KafkaTimeoutError):
            await names["t"]


async def test_async_signatures_forms_and_closed() -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    with pytest.raises(TypeError):
        await admin.delete_topics(["t"])  # type: ignore[call-overload]
    with pytest.raises(TypeError):
        await admin.list_offsets({})  # type: ignore[misc]
    with pytest.raises(IllegalArgumentError) as info:
        await admin.describe_topics()  # type: ignore[call-overload]
    assert str(info.value).endswith("; got ()")
    with pytest.raises(IllegalArgumentError) as info:
        await admin.list_config_resources(  # type: ignore[call-overload]
            options=ListConfigResourcesOptions())
    assert str(info.value) == ("list_config_resources() takes one of "
                               "(config_resource_types, options), (); got (options)")
    await admin.close()
    for _name, _call, run, _future in _CALLS:
        with pytest.raises(IllegalStateError) as closed:
            await run(admin)
        assert str(closed.value) == CLOSED


async def test_async_results_hold_asyncio_futures() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        cluster = await admin.describe_cluster(options=DescribeClusterOptions(timeout_ms=SHORT))
        for future in (cluster.nodes(), cluster.controller(), cluster.cluster_id(),
                       cluster.authorized_operations()):
            assert isinstance(future, asyncio.Future)
            assert isinstance(future.exception(), KafkaTimeoutError)
    whole = _conversions.describe_configs_result([RESOURCE], _whole_call_error(), Async)
    with pytest.raises(UnknownServerError):
        await whole.values()[RESOURCE]
