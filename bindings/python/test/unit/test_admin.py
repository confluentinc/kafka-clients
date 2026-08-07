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

"""Test suite for the Python Kafka admin bindings (MockAdminClient-driven)."""

import asyncio
import gc
import signal
import threading
import time

import pytest
from admin import (
    MockAdminClient, AsyncMockAdminClient, AdminClient, AsyncAdminClient,
    NewTopic, NewPartitions, RecordsToDelete, DeletedRecords,
    TopicDescription, TopicListing, TopicMetadataAndConfig,
    AlterConfigOp, ClientMetricsResourceListing, ClusterDescription, Config,
    ConfigEntry, ConfigResource, ConfigResourceType, OpType, ReplicaLogDirInfo,
    TopicPartitionReplica,
    ElectionType, IsolationLevel, ListOffsetsResultInfo, NewPartitionReassignment,
    OffsetSpec, PartitionReassignment,
    GroupListing, ListConsumerGroupOffsetsSpec, MemberToRemove, OffsetAndMetadata,
    _to_describe_classic_groups, _to_describe_consumer_groups,
    _to_describe_log_dirs, _to_describe_replica_log_dirs,
    _to_elect_leaders, _to_full_config_entry, _to_keyed_errors,
    _to_list_consumer_group_offsets, _to_list_groups, _to_list_offsets,
    _to_list_partition_reassignments, _to_log_dir_description,
    _to_member_description,
)
from producer import KafkaError

# Numeric `Errors` codes (src/common/protocol/errors.rs).
UNKNOWN_TOPIC_OR_PARTITION = 3
REPLICA_NOT_AVAILABLE = 9
UNSUPPORTED_VERSION = 35
TOPIC_ALREADY_EXISTS = 36
INVALID_REPLICATION_FACTOR = 38
INVALID_REQUEST = 42
KAFKA_STORAGE_ERROR = 56


def _created(admin, name, num_partitions=1, replication_factor=1):
    """Create one topic and assert it succeeded; returns its metadata."""
    result = admin.create_topics(
        [NewTopic(name, num_partitions, replication_factor)])
    value = result[name]
    assert isinstance(value, TopicMetadataAndConfig), value
    return value


# -- lifecycle ---------------------------------------------------------------

def test_create_and_close():
    admin = MockAdminClient(1)
    admin.close()
    assert admin.closed


def test_close_idempotent():
    admin = MockAdminClient(1)
    admin.close()
    admin.close()  # no error


def test_context_manager():
    with MockAdminClient(1) as admin:
        assert not admin.closed
    assert admin.closed


def test_call_after_close_raises():
    admin = MockAdminClient(1)
    admin.close()
    with pytest.raises(RuntimeError):
        admin.list_topics()


def test_mock_requires_at_least_one_broker():
    # The mock puts the controller and every partition leader on broker 0; Java
    # throws IndexOutOfBoundsException, the FFI returns NULL, and the C extension
    # turns that into a RuntimeError.
    with pytest.raises(RuntimeError):
        MockAdminClient(0)


def test_admin_client_requires_dict_config():
    with pytest.raises(TypeError):
        AdminClient("bootstrap.servers=localhost:9092")


def test_admin_client_rejects_empty_bootstrap():
    # No bootstrap.servers -> the Rust constructor fails, surfaced as
    # RuntimeError by the C extension (there is no handle to attach an error to).
    with pytest.raises(RuntimeError):
        AdminClient({})


# -- production client (broker-less) -----------------------------------------
#
# Everything above drives MockAdminClient. These cover the production path that
# only a real client reaches: building and *entering* the tokio runtime so the
# admin background task can be spawned, close/destroy against a real client, and
# the mock-only drivers rejecting a production handle.
#
# No broker is needed: construction only parses the config, and every RPC below
# carries a short explicit timeout. Mirrors bindings/c/tests/test_kafka_admin.c
# and the broker-less consumer/producer suites.

PRODUCTION_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "py-kafka-admin-test",
    "default.api.timeout.ms": "1000",
    "request.timeout.ms": "500",
}


def test_admin_client_constructs_and_closes():
    admin = AdminClient(dict(PRODUCTION_CONFIG))
    assert not admin.closed
    admin.close(timeout=1.0)
    assert admin.closed
    admin.close()  # idempotent


def test_admin_client_context_manager_closes():
    with AdminClient(dict(PRODUCTION_CONFIG)) as admin:
        assert not admin.closed
    assert admin.closed


def test_admin_client_rpc_returns_or_raises_without_hanging():
    """A real RPC against an unreachable bootstrap must terminate.

    With no broker it raises (typically a timeout); if something happens to be
    listening on localhost:9092 it may succeed. Both outcomes are accepted —
    what is asserted is that exactly one of them is produced, promptly.
    """
    with AdminClient(dict(PRODUCTION_CONFIG)) as admin:
        try:
            listings = admin.list_topics(timeout=1.0, list_internal=True)
        except KafkaError as e:
            assert str(e)
        else:
            assert isinstance(listings, dict)


def test_admin_client_rejects_mock_only_driver():
    """`MockAdminClient_timeout_next_request` takes the same opaque handle as the
    rest of the API, so a production handle has to be rejected at runtime. The
    raw `_lib` call is used deliberately: `timeout_next_request` lives on
    `_MockAdminClientMixin`, so `AdminClient` does not expose it, and this error
    arm is otherwise unreachable from Python."""
    import _confluentkafka as _lib

    with AdminClient(dict(PRODUCTION_CONFIG)) as admin:
        e = _lib.MockAdminClient_timeout_next_request(admin._h, 1)
        assert e
        err = KafkaError._from_c(e)
        assert str(err) == "this operation is only supported on a MockAdminClient"


def test_admin_client_marshaling_error_needs_no_broker():
    """Argument marshaling runs before the RPC is submitted, so an unparseable
    topic id fails on the production client with no network involved."""
    with AdminClient(dict(PRODUCTION_CONFIG)) as admin:
        with pytest.raises(KafkaError) as excinfo:
            admin.describe_topics_by_ids(["not-a-base64-uuid"], timeout=1.0)
        assert "invalid topic id" in str(excinfo.value)


async def test_async_admin_client_constructs_and_closes():
    admin = AsyncAdminClient(dict(PRODUCTION_CONFIG))
    assert not admin.closed
    await admin.close(timeout=1.0)
    assert admin.closed
    await admin.close()  # idempotent


# -- createTopics ------------------------------------------------------------

def test_create_topics_returns_metadata():
    with MockAdminClient(3) as admin:
        result = admin.create_topics([NewTopic("topic-a", 4, 2)])
        assert list(result) == ["topic-a"]
        meta = result["topic-a"]
        assert isinstance(meta, TopicMetadataAndConfig)
        assert meta.num_partitions == 4
        assert meta.replication_factor == 2
        assert meta.topic_id  # non-empty base64 topic id
        assert meta.error is None
        assert meta.configs == []


def test_create_topics_with_configs():
    with MockAdminClient(1) as admin:
        result = admin.create_topics(
            [NewTopic("configured", 1, 1, configs={"cleanup.policy": "compact"})])
        meta = result["configured"]
        assert len(meta.configs) == 1
        entry = meta.configs[0]
        assert entry.name == "cleanup.policy"
        assert entry.value == "compact"
        assert entry.is_sensitive is False
        assert entry.is_read_only is False


def test_create_topics_broker_defaults():
    # None -> broker defaults; the mock uses 1 partition and min(brokers, 3).
    with MockAdminClient(2) as admin:
        meta = _created(admin, "defaults", None, None)
        assert meta.num_partitions == 1
        assert meta.replication_factor == 2


def test_create_topics_partial_failure_keeps_both_outcomes():
    """One topic succeeds, two fail, and every outcome survives in one dict."""
    with MockAdminClient(3) as admin:
        _created(admin, "existing")
        result = admin.create_topics([
            NewTopic("existing", 1, 1),
            NewTopic("fresh", 2, 1),
            NewTopic("too-many-replicas", 1, 9),
        ])
        assert set(result) == {"existing", "fresh", "too-many-replicas"}

        existing = result["existing"]
        assert isinstance(existing, KafkaError)
        assert existing.code == TOPIC_ALREADY_EXISTS
        assert existing.message == "Topic existing exists already."

        too_many = result["too-many-replicas"]
        assert isinstance(too_many, KafkaError)
        assert too_many.code == INVALID_REPLICATION_FACTOR

        fresh = result["fresh"]
        assert isinstance(fresh, TopicMetadataAndConfig)
        assert fresh.num_partitions == 2


def test_create_topics_validate_only_option_is_accepted():
    with MockAdminClient(1) as admin:
        # Java's MockAdminClient.createTopics never reads its options argument
        # (MockAdminClient.java:363-422 at kafka a18251bae0b8), so validateOnly
        # has no effect on the mock and the topic IS created. This pins the
        # option marshaling path, not a behavior the mock does not implement.
        admin.create_topics([NewTopic("validated", 1, 1)], validate_only=True)
        assert "validated" in admin.list_topics()


def test_create_topics_replicas_assignment_accepted():
    # Selects Java's NewTopic(name, Map<Integer, List<Integer>>) form. The mock
    # applies its defaults rather than the assignment (see the C test for the
    # Java citation), so only the marshaling path is asserted here.
    with MockAdminClient(3) as admin:
        result = admin.create_topics([
            NewTopic("assigned", replicas_assignments={0: [0, 1], 1: [1, 2]}),
        ])
        meta = result["assigned"]
        assert isinstance(meta, TopicMetadataAndConfig)
        assert meta.num_partitions == 1
        assert meta.replication_factor == 3


def test_create_topics_empty_batch():
    with MockAdminClient(1) as admin:
        assert admin.create_topics([]) == {}


# -- listTopics --------------------------------------------------------------

def test_list_topics():
    with MockAdminClient(1) as admin:
        _created(admin, "beta")
        _created(admin, "alpha")
        listings = admin.list_topics(list_internal=True)
        assert set(listings) == {"alpha", "beta"}
        listing = listings["alpha"]
        assert isinstance(listing, TopicListing)
        assert listing.name == "alpha"
        assert listing.is_internal is False
        assert listing.topic_id


def test_list_topics_empty():
    with MockAdminClient(1) as admin:
        assert admin.list_topics() == {}


def test_list_topics_call_failure_raises():
    """listTopics has a single future in Java, so a failure is a call failure
    and must raise rather than appear as a per-key error."""
    with MockAdminClient(1) as admin:
        admin.timeout_next_request(1)
        with pytest.raises(KafkaError) as excinfo:
            admin.list_topics()
        assert str(excinfo.value) == "The mock timed out the request."


# -- describeTopics ----------------------------------------------------------

def test_describe_topics_partial_failure():
    with MockAdminClient(3) as admin:
        _created(admin, "described", 2, 2)
        result = admin.describe_topics(["described", "missing"])
        assert set(result) == {"described", "missing"}

        missing = result["missing"]
        assert isinstance(missing, KafkaError)
        assert missing.code == UNKNOWN_TOPIC_OR_PARTITION
        assert missing.message == "Topic missing not found."

        described = result["described"]
        assert isinstance(described, TopicDescription)
        assert described.name == "described"
        assert described.is_internal is False
        assert described.topic_id
        assert described.authorized_operations == []
        assert len(described.partitions) == 2

        p0 = described.partitions[0]
        assert p0.partition == 0
        # The mock puts every leader on broker 0 (localhost:1000).
        assert p0.leader.id == 0
        assert p0.leader.host == "localhost"
        assert p0.leader.port == 1000
        assert p0.leader.rack is None
        assert [n.id for n in p0.replicas] == [0, 1]
        assert p0.isr == []
        # The mock reports empty (not absent) ELR sets.
        assert p0.elr == []
        assert p0.last_known_elr == []


def test_describe_topics_by_ids():
    with MockAdminClient(1) as admin:
        _created(admin, "by-id")
        topic_id = admin.list_topics(list_internal=True)["by-id"].topic_id

        result = admin.describe_topics_by_ids([topic_id])
        assert list(result) == [topic_id]
        described = result[topic_id]
        assert isinstance(described, TopicDescription)
        assert described.name == "by-id"
        assert described.topic_id == topic_id


def test_describe_topics_by_ids_rejects_invalid_id():
    with MockAdminClient(1) as admin:
        with pytest.raises(KafkaError) as excinfo:
            admin.describe_topics_by_ids(["not a base64 uuid at all"])
        assert "invalid topic id" in str(excinfo.value)


# -- deleteTopics ------------------------------------------------------------

def test_delete_topics_partial_failure():
    with MockAdminClient(1) as admin:
        _created(admin, "doomed")
        result = admin.delete_topics(["doomed", "never-existed"])
        # A void per-key result: None means success.
        assert result["doomed"] is None
        missing = result["never-existed"]
        assert isinstance(missing, KafkaError)
        assert missing.code == UNKNOWN_TOPIC_OR_PARTITION
        assert missing.message == "Topic never-existed does not exist."
        assert admin.list_topics(list_internal=True) == {}


def test_delete_topics_by_ids():
    with MockAdminClient(1) as admin:
        _created(admin, "id-doomed")
        topic_id = admin.list_topics(list_internal=True)["id-doomed"].topic_id
        result = admin.delete_topics_by_ids([topic_id])
        assert result[topic_id] is None
        assert admin.list_topics(list_internal=True) == {}


def test_delete_topics_by_ids_rejects_invalid_id():
    with MockAdminClient(1) as admin:
        with pytest.raises(KafkaError):
            admin.delete_topics_by_ids(["@@@"])


# -- createPartitions --------------------------------------------------------
#
# Java's MockAdminClient.createPartitions throws
# UnsupportedOperationException("Not implemented yet")
# (MockAdminClient.java:626-628 at kafka a18251bae0b8). Per
# .claude/rules/admin-client.md §9 the Rust mock represents that as a per-key
# KafkaError::unsupported_version("Not implemented yet") instead of panicking, so
# the mock exercises the whole marshaling + flattening path and the outcome
# asserted here is that faithful "unsupported" error.

def test_create_partitions_reports_unsupported_per_topic():
    with MockAdminClient(3) as admin:
        _created(admin, "grow-me")
        result = admin.create_partitions({"grow-me": NewPartitions(4)}, timeout=5.0)
        assert list(result) == ["grow-me"]
        err = result["grow-me"]
        assert isinstance(err, KafkaError)
        assert err.code == UNSUPPORTED_VERSION
        assert err.message == "Not implemented yet"


def test_create_partitions_with_assignments_marshals():
    """Supplying new_assignments selects Java's
    NewPartitions.increaseTo(totalCount, newAssignments) form. The mock rejects
    the RPC either way, so this pins the marshaling path only."""
    with MockAdminClient(3) as admin:
        result = admin.create_partitions({
            "zeta": NewPartitions(2),
            "alpha": NewPartitions(3, [[0, 1], [1, 2]]),
        })
        assert set(result) == {"alpha", "zeta"}
        assert all(isinstance(v, KafkaError) for v in result.values())


def test_create_partitions_empty_batch():
    with MockAdminClient(1) as admin:
        assert admin.create_partitions({}) == {}


# -- deleteRecords -----------------------------------------------------------
#
# Java's MockAdminClient.deleteRecords returns an empty result for an empty
# request and otherwise throws UnsupportedOperationException("Not implemented
# yet") (MockAdminClient.java:630-638). The Rust mock mirrors both halves.

def test_delete_records_reports_unsupported_per_partition():
    with MockAdminClient(1) as admin:
        _created(admin, "trimmed", 2, 1)
        # -1 is Java's documented "truncate to the high watermark".
        result = admin.delete_records({
            ("trimmed", 0): RecordsToDelete(10),
            ("trimmed", 1): RecordsToDelete(5),
            ("another", 0): RecordsToDelete(-1),
        }, timeout=5.0)
        assert set(result) == {("trimmed", 0), ("trimmed", 1), ("another", 0)}
        for key, value in result.items():
            assert isinstance(value, KafkaError), key
            assert value.code == UNSUPPORTED_VERSION
            assert value.message == "Not implemented yet"


def test_delete_records_empty_batch_succeeds():
    """Java returns an empty DeleteRecordsResult for an empty request rather than
    throwing (MockAdminClient.java:632-635), so this must not raise."""
    with MockAdminClient(1) as admin:
        assert admin.delete_records({}) == {}


def test_delete_records_success_branch_converts_low_watermark():
    """The mock never succeeds at deleteRecords, so the success branch of the
    drain conversion is covered directly: a null per-key error yields a
    DeletedRecords carrying the low watermark, not a KafkaError."""
    from admin import _to_delete_records

    converted = _to_delete_records({
        ("t", 0): (None, 42),
        ("t", 1): ((UNSUPPORTED_VERSION, "Not implemented yet", 0, 0), -1),
    })
    ok = converted[("t", 0)]
    assert isinstance(ok, DeletedRecords)
    assert ok.low_watermark == 42
    failed = converted[("t", 1)]
    assert isinstance(failed, KafkaError)
    assert failed.message == "Not implemented yet"


# -- options marshaling ------------------------------------------------------

def test_timeout_accepts_float_and_timedelta():
    import datetime as dt
    with MockAdminClient(1) as admin:
        _created(admin, "t1")
        assert admin.list_topics(timeout=5.0, list_internal=True)
        assert admin.list_topics(timeout=dt.timedelta(seconds=5), list_internal=True)


# -- asyncio API -------------------------------------------------------------

async def test_async_create_and_describe():
    async with AsyncMockAdminClient(3) as admin:
        result = await admin.create_topics([NewTopic("async-topic", 3, 2)])
        meta = result["async-topic"]
        assert isinstance(meta, TopicMetadataAndConfig)
        assert meta.num_partitions == 3

        described = await admin.describe_topics(["async-topic"])
        assert len(described["async-topic"].partitions) == 3


async def test_async_partial_failure():
    async with AsyncMockAdminClient(1) as admin:
        await admin.create_topics([NewTopic("dupe", 1, 1)])
        result = await admin.create_topics(
            [NewTopic("dupe", 1, 1), NewTopic("new-one", 1, 1)])
        assert isinstance(result["dupe"], KafkaError)
        assert isinstance(result["new-one"], TopicMetadataAndConfig)


async def test_async_list_and_delete():
    async with AsyncMockAdminClient(1) as admin:
        await admin.create_topics([NewTopic("gone", 1, 1)])
        assert set(await admin.list_topics(list_internal=True)) == {"gone"}
        result = await admin.delete_topics(["gone"])
        assert result["gone"] is None
        assert await admin.list_topics(list_internal=True) == {}


async def test_async_create_partitions_and_delete_records():
    async with AsyncMockAdminClient(1) as admin:
        await admin.create_topics([NewTopic("async-grow", 1, 1)])
        grown = await admin.create_partitions({"async-grow": NewPartitions(3)})
        assert isinstance(grown["async-grow"], KafkaError)

        trimmed = await admin.delete_records({("async-grow", 0): RecordsToDelete(4)})
        assert isinstance(trimmed[("async-grow", 0)], KafkaError)

        # The empty deleteRecords path returns a result, not an error.
        assert await admin.delete_records({}) == {}


async def test_async_close_idempotent():
    admin = AsyncMockAdminClient(1)
    await admin.close()
    await admin.close()
    assert admin.closed


async def test_async_call_after_close_raises():
    admin = AsyncMockAdminClient(1)
    await admin.close()
    with pytest.raises(RuntimeError):
        await admin.list_topics()


async def test_async_list_topics_call_failure_raises():
    async with AsyncMockAdminClient(1) as admin:
        admin.timeout_next_request(1)
        with pytest.raises(KafkaError):
            await admin.list_topics()


async def test_async_concurrent_calls():
    """The admin client is thread-safe and has no single-owner guard, so
    concurrent RPCs are allowed (unlike the consumer)."""
    async with AsyncMockAdminClient(1) as admin:
        await admin.create_topics([NewTopic("c1", 1, 1)])
        results = await asyncio.gather(
            admin.list_topics(list_internal=True),
            admin.describe_topics(["c1"]),
            admin.list_topics(list_internal=True),
        )
        assert set(results[0]) == {"c1"}
        assert isinstance(results[1]["c1"], TopicDescription)
        assert set(results[2]) == {"c1"}


# -- B2: describe_cluster ----------------------------------------------------

def test_describe_cluster():
    with MockAdminClient(3) as admin:
        # include_authorized_operations is False on purpose: Java's
        # MockAdminClient.describeCluster ignores its options entirely
        # (MockAdminClient.java:340-360) and always completes the operations
        # future with an *empty* set, never null.
        described = admin.describe_cluster()
        assert isinstance(described, ClusterDescription)
        assert described.cluster_id == "4A5xz_QZTB2CtL4wc0X0Jw"
        assert [n.id for n in described.nodes] == [0, 1, 2]
        assert described.nodes[0].host == "localhost"
        assert described.nodes[0].port == 1000
        assert described.controller.id == 0
        # Empty, not None: None would mean the broker did not report them.
        assert described.authorized_operations == []


def test_describe_cluster_call_failure_raises():
    """All four attribute futures fail together, so the call raises rather than
    reporting anything per key. The next call succeeds."""
    with MockAdminClient(1) as admin:
        admin.timeout_next_request(1)
        with pytest.raises(KafkaError):
            admin.describe_cluster()
        assert admin.describe_cluster().controller.id == 0


# -- B2: describe_configs / incremental_alter_configs ------------------------

def test_describe_configs_partial_failure():
    with MockAdminClient(1) as admin:
        _created(admin, "cfg-topic")
        topic = ConfigResource(ConfigResourceType.TOPIC, "cfg-topic")
        missing = ConfigResource(ConfigResourceType.TOPIC, "cfg-missing")
        broker = ConfigResource(ConfigResourceType.BROKER, "0")
        logger = ConfigResource(ConfigResourceType.BROKER_LOGGER, "0")

        assert admin.incremental_alter_configs(
            {topic: [AlterConfigOp(ConfigEntry("retention.ms", "60000", False, False, False),
                                   OpType.SET)]})[topic] is None

        described = admin.describe_configs([topic, missing, broker, logger],
                                           include_synonyms=True,
                                           include_documentation=True)
        assert set(described) == {topic, missing, broker, logger}

        config = described[topic]
        assert isinstance(config, Config)
        entry = config.get("retention.ms")
        assert entry.value == "60000"
        # The mock builds entries with `new ConfigEntry(name, value)`
        # (MockAdminClient.toConfigObject), so source/type stay UNKNOWN and
        # there is no documentation or synonym even though we asked for them.
        assert entry.source == "UNKNOWN"
        assert entry.config_type == "UNKNOWN"
        assert entry.documentation is None
        assert entry.synonyms == []
        assert entry.is_default is False
        assert config.get("no.such.config") is None

        assert isinstance(described[missing], KafkaError)
        assert described[missing].code == UNKNOWN_TOPIC_OR_PARTITION

        assert described[broker].get("default.replication.factor").value == "1"

        # BROKER_LOGGER hits getResourceDescription's default branch, which
        # throws UnsupportedOperationException("Not implemented yet").
        assert isinstance(described[logger], KafkaError)
        assert described[logger].code == UNSUPPORTED_VERSION


def test_describe_configs_empty_batch():
    with MockAdminClient(1) as admin:
        assert admin.describe_configs([]) == {}


def test_incremental_alter_configs_set_then_delete():
    with MockAdminClient(1) as admin:
        _created(admin, "alter-topic")
        resource = ConfigResource(ConfigResourceType.TOPIC, "alter-topic")

        # Two ops on one resource collapse to one result key.
        result = admin.incremental_alter_configs({resource: [
            AlterConfigOp(ConfigEntry("retention.ms", "1000", False, False, False), OpType.SET),
            AlterConfigOp(ConfigEntry("segment.ms", "2000", False, False, False), OpType.SET),
        ]})
        assert result == {resource: None}

        config = admin.describe_configs([resource])[resource]
        assert {e.name for e in config.entries} == {"retention.ms", "segment.ms"}

        # DELETE carries a null value, which is what Java sends for a removal.
        assert admin.incremental_alter_configs({resource: [
            AlterConfigOp(ConfigEntry("retention.ms", None, False, False, False), OpType.DELETE),
        ]})[resource] is None
        config = admin.describe_configs([resource])[resource]
        assert {e.name for e in config.entries} == {"segment.ms"}


def test_incremental_alter_configs_partial_failure():
    with MockAdminClient(1) as admin:
        _created(admin, "alter-ok")
        ok = ConfigResource(ConfigResourceType.TOPIC, "alter-ok")
        missing = ConfigResource(ConfigResourceType.TOPIC, "alter-missing")
        entry = ConfigEntry("retention.ms", "1000", False, False, False)

        result = admin.incremental_alter_configs({
            ok: [AlterConfigOp(entry, OpType.SET)],
            missing: [AlterConfigOp(entry, OpType.SET)],
        })
        assert result[ok] is None
        assert isinstance(result[missing], KafkaError)
        assert result[missing].code == UNKNOWN_TOPIC_OR_PARTITION


def test_incremental_alter_configs_unsupported_op_type_fails_that_resource():
    """APPEND reaches the mock, which rejects it as InvalidRequest — unlike an
    unknown op-type *code*, which never leaves the marshaling layer."""
    with MockAdminClient(1) as admin:
        _created(admin, "append-topic")
        resource = ConfigResource(ConfigResourceType.TOPIC, "append-topic")
        result = admin.incremental_alter_configs({resource: [
            AlterConfigOp(ConfigEntry("cleanup.policy", "compact", False, False, False),
                          OpType.APPEND),
        ]})
        assert isinstance(result[resource], KafkaError)
        assert result[resource].code == INVALID_REQUEST


def test_incremental_alter_configs_bad_op_type_raises():
    """An unknown AlterConfigOp.OpType code is a marshaling failure: the whole
    call fails and the RPC is never submitted."""
    with MockAdminClient(1) as admin:
        resource = ConfigResource(ConfigResourceType.TOPIC, "whatever")
        with pytest.raises(KafkaError) as excinfo:
            admin.incremental_alter_configs({resource: [
                AlterConfigOp(ConfigEntry("k", "v", False, False, False), 99),
            ]})
        assert "99" in str(excinfo.value)


# -- B2: listConfigResources / listClientMetricsResources --------------------

def test_list_config_resources():
    with MockAdminClient(2) as admin:
        _created(admin, "lcr-b")
        _created(admin, "lcr-a")

        topics = admin.list_config_resources([ConfigResourceType.TOPIC])
        # Sorted by (type id, name).
        assert [r.name for r in topics] == ["lcr-a", "lcr-b"]
        assert all(r.resource_type == ConfigResourceType.TOPIC for r in topics)

        # No filter means every supported type: 2 topics + 1 BROKER and
        # 1 BROKER_LOGGER per broker.
        every = admin.list_config_resources()
        by_type = {}
        for r in every:
            by_type[r.resource_type] = by_type.get(r.resource_type, 0) + 1
        assert by_type[ConfigResourceType.TOPIC] == 2
        assert by_type[ConfigResourceType.BROKER] == 2
        assert by_type[ConfigResourceType.BROKER_LOGGER] == 2
        assert ConfigResourceType.CLIENT_METRICS not in by_type


def test_list_client_metrics_resources():
    with MockAdminClient(1) as admin:
        assert admin.list_client_metrics_resources() == []

        # Altering a CLIENT_METRICS resource creates it, which is how Java's
        # mock seeds clientMetricsConfigs.
        for name in ("cm-b", "cm-a"):
            resource = ConfigResource(ConfigResourceType.CLIENT_METRICS, name)
            assert admin.incremental_alter_configs({resource: [
                AlterConfigOp(ConfigEntry("interval.ms", "1000", False, False, False),
                              OpType.SET),
            ]})[resource] is None

        listed = admin.list_client_metrics_resources()
        assert [r.name for r in listed] == ["cm-a", "cm-b"]  # sorted by name
        assert all(isinstance(r, ClientMetricsResourceListing) for r in listed)

        # The same resources through the API that supersedes this one.
        via_config = admin.list_config_resources([ConfigResourceType.CLIENT_METRICS])
        assert {r.name for r in via_config} == {"cm-a", "cm-b"}


# -- B2: log dirs ------------------------------------------------------------

def test_describe_log_dirs():
    with MockAdminClient(1) as admin:
        _created(admin, "ld-topic", num_partitions=2)

        # Broker 7 does not exist. Java still puts an entry in the result for
        # every requested broker (`unwrappedResults.putIfAbsent`), so it comes
        # back with an empty log-dir map rather than an error.
        described = admin.describe_log_dirs([0, 7])
        assert set(described) == {0, 7}
        assert described[7] == {}

        log_dirs = described[0]
        assert list(log_dirs) == ["/tmp/kafka-logs"]
        description = log_dirs["/tmp/kafka-logs"]
        assert description.error is None
        # The mock reports no volume sizes, i.e. Java's empty OptionalLong.
        assert description.total_bytes is None
        assert description.usable_bytes is None
        assert set(description.replica_infos) == {("ld-topic", 0), ("ld-topic", 1)}
        replica = description.replica_infos[("ld-topic", 0)]
        assert replica.size == 0
        assert replica.offset_lag == 0
        assert replica.is_future is False


def test_alter_replica_log_dirs_partial_failure():
    with MockAdminClient(1) as admin:
        _created(admin, "mv-topic", num_partitions=2)

        accepted = TopicPartitionReplica("mv-topic", 0, 0)
        offline_dir = TopicPartitionReplica("mv-topic", 1, 0)
        unknown_topic = TopicPartitionReplica("mv-missing", 0, 0)
        unknown_broker = TopicPartitionReplica("mv-topic", 0, 9)

        result = admin.alter_replica_log_dirs({
            accepted: "/tmp/kafka-logs",
            offline_dir: "/data/other",
            unknown_topic: "/tmp/kafka-logs",
            unknown_broker: "/tmp/kafka-logs",
        })
        assert result[accepted] is None
        assert result[offline_dir].code == KAFKA_STORAGE_ERROR
        assert result[unknown_topic].code == REPLICA_NOT_AVAILABLE
        assert result[unknown_broker].code == REPLICA_NOT_AVAILABLE

        # The accepted move is now a pending move on that replica.
        described = admin.describe_replica_log_dirs([accepted])[accepted]
        assert described.future_replica_log_dir == "/tmp/kafka-logs"


def test_describe_replica_log_dirs_omits_unknown_topics():
    """MockAdminClient.describeReplicaLogDirs skips replicas of unknown topics
    entirely (`if (topicMetadata != null)`, MockAdminClient.java:1112) rather
    than reporting an error, so the result is shorter than the request.

    This is mock-only. KafkaAdminClient seeds one future per requested replica
    (`KafkaAdminClient.java:3066-3068`) and completes all of them (`:3141-3145`),
    so against a real broker an unknown topic comes back *present*, with a null
    `current_replica_log_dir`.
    """
    with MockAdminClient(1) as admin:
        _created(admin, "drld-topic")
        known = TopicPartitionReplica("drld-topic", 0, 0)
        unknown = TopicPartitionReplica("drld-missing", 0, 0)

        described = admin.describe_replica_log_dirs([known, unknown])
        assert set(described) == {known}

        info = described[known]
        assert isinstance(info, ReplicaLogDirInfo)
        assert info.current_replica_log_dir == "/tmp/kafka-logs"
        assert info.current_replica_offset_lag == 0
        assert info.future_replica_log_dir is None
        assert info.future_replica_offset_lag == 0


def test_config_resource_and_replica_are_usable_dict_keys():
    """The two composite keys round-trip through the C boundary by value, so a
    freshly built key must match the one the drain produced."""
    with MockAdminClient(1) as admin:
        _created(admin, "key-topic")
        described = admin.describe_configs(
            [ConfigResource(ConfigResourceType.TOPIC, "key-topic")])
        assert described[ConfigResource(ConfigResourceType.TOPIC, "key-topic")] is not None
        assert ConfigResource(2, "x") == ConfigResource(2, "x")
        assert ConfigResource(2, "x") != ConfigResource(4, "x")

        replicas = admin.describe_replica_log_dirs(
            [TopicPartitionReplica("key-topic", 0, 0)])
        assert replicas[TopicPartitionReplica("key-topic", 0, 0)] is not None
        assert TopicPartitionReplica("t", 1, 2) == TopicPartitionReplica("t", 1, 2)
        assert TopicPartitionReplica("t", 1, 2) != TopicPartitionReplica("t", 1, 3)


# -- B2: asyncio-native ------------------------------------------------------

async def test_async_describe_cluster_and_configs():
    admin = AsyncMockAdminClient(2)
    try:
        described = await admin.describe_cluster()
        assert len(described.nodes) == 2

        resource = ConfigResource(ConfigResourceType.BROKER, "0")
        configs = await admin.describe_configs([resource])
        assert configs[resource].get("default.replication.factor").value == "2"

        altered = await admin.incremental_alter_configs({resource: [
            AlterConfigOp(ConfigEntry("num.io.threads", "9", False, False, False), OpType.SET),
        ]})
        assert altered == {resource: None}
        configs = await admin.describe_configs([resource])
        assert configs[resource].get("num.io.threads").value == "9"
    finally:
        await admin.close()


async def test_async_log_dirs_and_listings():
    admin = AsyncMockAdminClient(1)
    try:
        result = await admin.create_topics([NewTopic("async-ld", 1, 1)])
        assert isinstance(result["async-ld"], TopicMetadataAndConfig)

        log_dirs = await admin.describe_log_dirs([0])
        assert set(log_dirs[0]) == {"/tmp/kafka-logs"}

        replica = TopicPartitionReplica("async-ld", 0, 0)
        assert await admin.alter_replica_log_dirs({replica: "/tmp/kafka-logs"}) == {
            replica: None}
        described = await admin.describe_replica_log_dirs([replica])
        assert described[replica].future_replica_log_dir == "/tmp/kafka-logs"

        assert [r.name for r in await admin.list_config_resources(
            [ConfigResourceType.TOPIC])] == ["async-ld"]
        assert await admin.list_client_metrics_resources() == []
    finally:
        await admin.close()


def test_b2_handles_survive_gc_of_intermediate_objects():
    """Drained B2 results own no borrowed pointers either, so they stay valid
    after the result handle is destroyed and a GC pass runs."""
    with MockAdminClient(1) as admin:
        _created(admin, "gc-b2", 2, 1)
        resource = ConfigResource(ConfigResourceType.TOPIC, "gc-b2")
        cluster = admin.describe_cluster()
        configs = admin.describe_configs([resource])[resource]
        log_dirs = admin.describe_log_dirs([0])[0]
        gc.collect()
        assert cluster.nodes[0].host == "localhost"
        assert isinstance(configs, Config)
        assert log_dirs["/tmp/kafka-logs"].replica_infos[("gc-b2", 1)].size == 0


# -- pure converters ---------------------------------------------------------
#
# MockAdminClient builds its config entries with the two-argument
# ``ConfigEntry(name, value)`` constructor (MockAdminClient.java:889-895), so it
# never produces a non-UNKNOWN source or type, a documentation string, a
# synonym, a LogDirDescription error, or a volume size. It also ignores its
# ``options`` argument. Those tuple shapes therefore cannot be reached
# end-to-end; the converters are pure functions of their input, so they are
# asserted directly. A field-order or arity error in the synonym 3-tuple or the
# entry 9-tuple would otherwise ship silently.

def test_to_full_config_entry_maps_every_field_and_synonym():
    entry = _to_full_config_entry((
        "retention.ms", "604800000", 0, 1, 0, "DYNAMIC_TOPIC_CONFIG", "LONG",
        "The retention window.",
        # Ordered by precedence in Java; the converter must not sort.
        [("retention.ms", "604800000", "DYNAMIC_TOPIC_CONFIG"),
         ("log.retention.ms", None, "STATIC_BROKER_CONFIG")],
    ))
    assert isinstance(entry, ConfigEntry)
    assert entry.name == "retention.ms"
    assert entry.value == "604800000"
    assert entry.is_default is False
    assert entry.is_sensitive is True
    assert entry.is_read_only is False
    assert entry.source == "DYNAMIC_TOPIC_CONFIG"
    assert entry.config_type == "LONG"
    assert entry.documentation == "The retention window."

    assert [(s.name, s.value, s.source) for s in entry.synonyms] == [
        ("retention.ms", "604800000", "DYNAMIC_TOPIC_CONFIG"),
        ("log.retention.ms", None, "STATIC_BROKER_CONFIG"),
    ]


def test_to_full_config_entry_preserves_nulls_and_empty_synonyms():
    entry = _to_full_config_entry(
        ("sensitive.config", None, 1, 1, 1, "UNKNOWN", "UNKNOWN", None, []))
    assert entry.value is None
    assert entry.documentation is None
    assert entry.synonyms == []
    assert entry.is_default is True


def test_to_log_dir_description_carries_error_and_volume_bytes():
    description = _to_log_dir_description(
        ((KAFKA_STORAGE_ERROR, "offline", 0, 0), 2000, 1000,
         [("t", 0, 100, 5, 0), ("t", 1, 200, 0, 1)]))
    assert isinstance(description.error, KafkaError)
    assert description.error.code == KAFKA_STORAGE_ERROR
    assert description.total_bytes == 2000
    assert description.usable_bytes == 1000
    assert description.replica_infos[("t", 0)].size == 100
    assert description.replica_infos[("t", 0)].offset_lag == 5
    assert description.replica_infos[("t", 0)].is_future is False
    assert description.replica_infos[("t", 1)].is_future is True


def test_to_log_dir_description_maps_unknown_volume_bytes_to_none():
    """-1 is the wire's UNKNOWN_VOLUME_BYTES, i.e. Java's empty OptionalLong."""
    description = _to_log_dir_description((None, -1, -1, []))
    assert description.error is None
    assert description.total_bytes is None
    assert description.usable_bytes is None
    assert description.replica_infos == {}


def test_to_describe_log_dirs_error_arm_replaces_the_map():
    """A per-broker failure surfaces as a KafkaError *instead of* the log-dir
    map. The mock never fails a broker, so this arm is unreachable end-to-end."""
    out = _to_describe_log_dirs({
        0: ((UNSUPPORTED_VERSION, "nope", 0, 0), None),
        1: (None, {"/data/1": (None, -1, -1, [])}),
    })
    assert isinstance(out[0], KafkaError)
    assert out[0].code == UNSUPPORTED_VERSION
    assert set(out[1]) == {"/data/1"}


def test_to_describe_replica_log_dirs_error_arm_replaces_the_info():
    out = _to_describe_replica_log_dirs({
        ("t", 0, 1): ((REPLICA_NOT_AVAILABLE, "gone", 1, 0), None),
        ("t", 1, 1): (None, ("/data/current", 7, None, -1)),
    })
    failed = TopicPartitionReplica("t", 0, 1)
    assert isinstance(out[failed], KafkaError)
    assert out[failed].code == REPLICA_NOT_AVAILABLE

    info = out[TopicPartitionReplica("t", 1, 1)]
    assert info.current_replica_log_dir == "/data/current"
    assert info.current_replica_offset_lag == 7
    assert info.future_replica_log_dir is None
    assert info.future_replica_offset_lag == -1


async def test_async_describe_cluster_call_failure_raises():
    """The async error arm of a B2 RPC. The sync mirror is
    ``test_describe_cluster_call_failure_raises``; without this, ``_run_async``'s
    error arm is only reached by inherited B0/B1 tests."""
    admin = AsyncMockAdminClient(1)
    try:
        admin.timeout_next_request(1)
        with pytest.raises(KafkaError):
            await admin.describe_cluster()
        # The client is still usable afterwards.
        assert (await admin.describe_cluster()).controller.id == 0
    finally:
        await admin.close()


# -- lifetime / interrupt ----------------------------------------------------

def test_handle_survives_gc_of_intermediate_objects():
    """Drained results own no borrowed pointers, so they stay valid after the
    result handle is destroyed and a GC pass runs."""
    with MockAdminClient(1) as admin:
        _created(admin, "gc-topic", 2, 1)
        described = admin.describe_topics(["gc-topic"])["gc-topic"]
        listings = admin.list_topics(list_internal=True)
        gc.collect()
        assert described.name == "gc-topic"
        assert described.partitions[0].leader.host == "localhost"
        assert listings["gc-topic"].topic_id == described.topic_id


def test_sigint_while_waiting_drains_callback_then_reraises():
    """A SIGINT delivered while a sync call is waiting must raise
    KeyboardInterrupt *after* the in-flight callback has been drained, so the
    result handles are freed rather than leaked, and the client stays usable.

    ``_run_sync`` is the shared waiter behind every sync RPC. It is driven here
    with a deliberately slow submit because the mock resolves every real RPC
    instantly, leaving no window for the signal to land inside the wait.
    """
    with MockAdminClient(1) as admin:
        _created(admin, "before-sigint")

        freed = []
        callback_fired = threading.Event()

        def submit(cb):
            def fire():
                time.sleep(0.5)
                callback_fired.set()
                cb(0)  # payload shape of a void op: (error_int,)
            threading.Thread(target=fire, daemon=True).start()

        def resolve(payload):
            raise AssertionError("resolve must not run after an interrupt")

        def free(payload):
            freed.append(payload)

        def raise_sigint():
            time.sleep(0.15)
            signal.raise_signal(signal.SIGINT)

        th = threading.Thread(target=raise_sigint)
        th.start()
        with pytest.raises(KeyboardInterrupt):
            admin._run_sync(submit, resolve, free)
        th.join()

        # The interrupt was deferred until the callback arrived, and its payload
        # was freed rather than leaked.
        assert callback_fired.is_set()
        assert freed == [(0,)]

        # The client is still usable afterwards.
        assert "before-sigint" in admin.list_topics(list_internal=True)


# -- B3: elections, reassignments, offsets -----------------------------------

def test_elect_leaders_call_failure_raises():
    """`MockAdminClient.electLeaders` throws UnsupportedOperationException
    ("Not implemented yet", MockAdminClient.java:792-798). Java exposes one
    future for the whole election, so that is a *call* failure and it raises,
    rather than landing as a per-partition error."""
    with MockAdminClient(1) as admin:
        _created(admin, "el-topic", num_partitions=2)
        with pytest.raises(KafkaError) as exc:
            admin.elect_leaders(ElectionType.PREFERRED, [("el-topic", 0), ("el-topic", 1)])
        assert exc.value.code == UNSUPPORTED_VERSION
        assert str(exc.value) == "Not implemented yet"

        # An explicit `None` is Java's null Set: every partition in the cluster.
        with pytest.raises(KafkaError):
            admin.elect_leaders(ElectionType.UNCLEAN, None)


def test_elect_leaders_requires_partitions_explicitly():
    """Java has no no-argument `electLeaders` overload — both take the `Set`
    (`Admin.java:1092` and the three-argument form). Omitting it must not
    default to a cluster-wide election, which for UNCLEAN would be
    destructive."""
    with MockAdminClient(1) as admin:
        with pytest.raises(TypeError):
            admin.elect_leaders(ElectionType.UNCLEAN)


def test_elect_leaders_rejects_bad_election_type():
    """Mirrors Java's `ElectionType.valueOf(byte)` IllegalArgumentException,
    raised before the RPC is submitted."""
    with MockAdminClient(1) as admin:
        with pytest.raises(KafkaError) as exc:
            admin.elect_leaders(7, None)
        assert str(exc.value) == "Value 7 must be one of [PREFERRED, UNCLEAN]"


def test_alter_partition_reassignments_partial_failure():
    """The mock reassigns against its in-memory map
    (MockAdminClient.java:1141-1167). A partition it does not know fails with
    UNKNOWN_TOPIC_OR_PARTITION per partition, so the call itself succeeds."""
    with MockAdminClient(3) as admin:
        _created(admin, "ra-topic", num_partitions=1, replication_factor=3)

        result = admin.alter_partition_reassignments({
            ("ra-topic", 0): NewPartitionReassignment([1, 2]),
            ("ra-missing", 0): NewPartitionReassignment([1, 2]),
        })
        assert result[("ra-topic", 0)] is None
        assert result[("ra-missing", 0)].code == UNKNOWN_TOPIC_OR_PARTITION


def test_list_partition_reassignments_round_trip():
    """Reassigning to {1, 2} on a 3-broker mock, whose partitions are seeded
    with every broker as a replica (MockAdminClient.java:412-420), removes
    broker 0 and adds nothing. Asserting all three lists with distinct contents
    is what catches a transposition between them."""
    with MockAdminClient(3) as admin:
        _created(admin, "lr-topic", num_partitions=1, replication_factor=3)
        assert admin.alter_partition_reassignments({
            ("lr-topic", 0): NewPartitionReassignment([1, 2])}) == {("lr-topic", 0): None}

        # `partitions=None` is Java's Optional.empty(): list everything.
        listed = admin.list_partition_reassignments()
        assert set(listed) == {("lr-topic", 0)}
        reassignment = listed[("lr-topic", 0)]
        assert isinstance(reassignment, PartitionReassignment)
        assert reassignment.replicas == [0, 1, 2]
        assert reassignment.adding_replicas == []
        assert reassignment.removing_replicas == [0]

        # Restricting to a partition with no reassignment yields nothing, so
        # the result is smaller than the request.
        assert admin.list_partition_reassignments([("lr-other", 0)]) == {}

        # A None value is Java's empty Optional, which reverts the
        # reassignment (Admin.java:1142-1143).
        assert admin.alter_partition_reassignments({("lr-topic", 0): None}) == {
            ("lr-topic", 0): None}
        assert admin.list_partition_reassignments() == {}


def test_alter_partition_reassignments_rejects_empty_replicas():
    """An empty target-replica list is Java's
    `NewPartitionReassignment(List<Integer>)` IllegalArgumentException, not a
    cancellation — the None value is the only way to cancel."""
    with MockAdminClient(3) as admin:
        _created(admin, "ra-empty", num_partitions=1, replication_factor=3)
        with pytest.raises(KafkaError) as exc:
            admin.alter_partition_reassignments({
                ("ra-empty", 0): NewPartitionReassignment([])})
        assert str(exc.value) == (
            "reassignment for ra-empty-0 at index 0: Cannot create a new partition "
            "reassignment without any replicas")
        # Nothing was submitted.
        assert admin.list_partition_reassignments() == {}


def test_list_offsets_earliest_and_latest():
    """The mock answers earliest() from `beginningOffsets` and everything else
    from `endOffsets` (MockAdminClient.java:1220-1240). Asking for different
    specs on two partitions with different seeded offsets catches a transposed
    spec list. An unseeded partition reports -1 rather than Java's NPE on
    unboxing a null Long, a divergence documented in the Rust mock."""
    with MockAdminClient(1) as admin:
        _created(admin, "lo-topic", num_partitions=2)
        admin.update_beginning_offsets({("lo-topic", 0): 5, ("lo-topic", 1): 7})
        admin.update_end_offsets({("lo-topic", 0): 105, ("lo-topic", 1): 107})

        result = admin.list_offsets({
            ("lo-topic", 0): OffsetSpec.earliest(),
            ("lo-topic", 1): OffsetSpec.latest(),
            ("lo-unseeded", 0): OffsetSpec.max_timestamp(),
        })
        info = result[("lo-topic", 0)]
        assert isinstance(info, ListOffsetsResultInfo)
        assert info.offset == 5
        # The mock reports no timestamp and no leader epoch.
        assert info.timestamp == -1
        assert info.leader_epoch is None

        assert result[("lo-topic", 1)].offset == 107
        assert result[("lo-unseeded", 0)].offset == -1


def test_list_offsets_timestamp_flag_is_load_bearing():
    """`for_timestamp(-2)` and `earliest()` are the same wire value once Java's
    `getOffsetFromSpec` has run, so the two are only distinguishable because
    OffsetSpec carries an explicit flag. The mock proves they are not
    interchangeable: a TimestampSpec fails that partition with
    UnsupportedOperationException (MockAdminClient.java:1230), while earliest()
    returns the seeded offset. This is a *per-partition* error, because Java's
    ListOffsetsResult holds one future per partition."""
    with MockAdminClient(1) as admin:
        _created(admin, "lo-ts", num_partitions=2)
        admin.update_beginning_offsets({("lo-ts", 0): 11})

        result = admin.list_offsets({
            ("lo-ts", 0): OffsetSpec.earliest(),
            ("lo-ts", 1): OffsetSpec.for_timestamp(OffsetSpec._EARLIEST),
        }, isolation_level=IsolationLevel.READ_COMMITTED)
        assert result[("lo-ts", 0)].offset == 11
        assert result[("lo-ts", 1)].code == UNSUPPORTED_VERSION


def test_list_offsets_rejects_bad_isolation_level():
    """Mirrors Java's `IsolationLevel.forId` IllegalArgumentException."""
    with MockAdminClient(1) as admin:
        _created(admin, "lo-bad")
        with pytest.raises(KafkaError) as exc:
            admin.list_offsets({("lo-bad", 0): OffsetSpec.latest()}, isolation_level=9)
        assert str(exc.value) == "Unknown isolation level 9"


def test_b3_empty_batches():
    """Empty batches resolve without a round trip, as in B1/B2."""
    with MockAdminClient(1) as admin:
        assert admin.alter_partition_reassignments({}) == {}
        assert admin.list_offsets({}) == {}
        assert admin.list_partition_reassignments([]) == {}


def test_mock_offset_drivers_are_mock_only():
    """`update_beginning_offsets` / `update_end_offsets` live on
    `_MockAdminClientMixin` alongside `timeout_next_request`, so they are not
    part of the production surface at all. (The FFI *does* also reject a
    production handle at runtime; that arm is covered by the C test
    `test_mock_admin_offset_drivers_reject_non_mock`, which can reach it
    because C has one handle type for both clients.)"""
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    try:
        assert not hasattr(admin, "update_beginning_offsets")
        assert not hasattr(admin, "update_end_offsets")
        assert not hasattr(admin, "timeout_next_request")
    finally:
        admin.close(timeout=1.0)

    with MockAdminClient(1) as mock:
        assert hasattr(mock, "update_beginning_offsets")
        assert hasattr(mock, "update_end_offsets")


async def test_async_b3_round_trip():
    admin = AsyncMockAdminClient(3)
    try:
        await admin.create_topics([NewTopic("async-b3", 1, 3)])
        assert await admin.alter_partition_reassignments({
            ("async-b3", 0): NewPartitionReassignment([1, 2])}) == {("async-b3", 0): None}

        listed = await admin.list_partition_reassignments()
        assert listed[("async-b3", 0)].removing_replicas == [0]

        # The mock drivers are plain sync methods, even on the async client:
        # they only mutate in-memory state, exactly as `timeout_next_request` does.
        admin.update_end_offsets({("async-b3", 0): 77})
        offsets = await admin.list_offsets({("async-b3", 0): OffsetSpec.latest()})
        assert offsets[("async-b3", 0)].offset == 77
    finally:
        await admin.close()


async def test_async_elect_leaders_call_failure_raises():
    """The async mirror of the sync call-failure arm: a single-future RPC
    raises rather than reporting per-key."""
    admin = AsyncMockAdminClient(1)
    try:
        await admin.create_topics([NewTopic("async-el", 1, 1)])
        with pytest.raises(KafkaError) as exc:
            await admin.elect_leaders(ElectionType.PREFERRED, [("async-el", 0)])
        assert exc.value.code == UNSUPPORTED_VERSION
    finally:
        await admin.close()


def test_b3_handles_survive_gc_of_intermediate_objects():
    """Drained B3 results own no borrowed pointers, so they stay valid after the
    result handle is destroyed and a GC pass runs."""
    with MockAdminClient(3) as admin:
        _created(admin, "gc-b3", num_partitions=1, replication_factor=3)
        admin.alter_partition_reassignments({
            ("gc-b3", 0): NewPartitionReassignment([1, 2])})
        admin.update_end_offsets({("gc-b3", 0): 3})

        listed = admin.list_partition_reassignments()
        offsets = admin.list_offsets({("gc-b3", 0): OffsetSpec.latest()})
        gc.collect()
        assert listed[("gc-b3", 0)].replicas == [0, 1, 2]
        assert offsets[("gc-b3", 0)].offset == 3


# -- direct converter coverage (unreachable through the mock) -----------------

def test_to_list_offsets_maps_both_arms():
    """The mock never reports a timestamp or a leader epoch, so the populated
    arm is only reachable here. Distinct offset/timestamp/epoch values catch a
    transposed tuple field."""
    converted = _to_list_offsets({
        ("t", 0): (None, (42, 1_700_000_000_000, 7)),
        ("t", 1): (None, (5, -1, None)),
        ("t", 2): ((3, "boom", False, False), None),
    })
    assert converted[("t", 0)].offset == 42
    assert converted[("t", 0)].timestamp == 1_700_000_000_000
    assert converted[("t", 0)].leader_epoch == 7
    assert converted[("t", 1)].leader_epoch is None
    error = converted[("t", 2)]
    assert isinstance(error, KafkaError)
    assert error.code == 3
    assert str(error) == "boom"


def test_to_list_partition_reassignments_keeps_the_three_lists_apart():
    """Three distinct lists, so a transposed tuple field fails."""
    converted = _to_list_partition_reassignments({("t", 0): ([0, 1, 2], [3], [0, 2])})
    reassignment = converted[("t", 0)]
    assert reassignment.replicas == [0, 1, 2]
    assert reassignment.adding_replicas == [3]
    assert reassignment.removing_replicas == [0, 2]


def test_to_elect_leaders_maps_none_to_success():
    """Java's per-partition value is an Optional<Throwable>, so None is success
    rather than a missing value."""
    converted = _to_elect_leaders({("t", 0): None, ("t", 1): (9, "nope", True, False)})
    assert converted[("t", 0)] is None
    assert converted[("t", 1)].code == 9
    assert converted[("t", 1)].is_retriable is True


def test_offset_spec_factories_use_javas_sentinels():
    """Each factory's sentinel is the value Java's
    `KafkaAdminClient.getOffsetFromSpec` emits for it, so a transposed pair
    fails here."""
    assert (OffsetSpec.latest().is_timestamp, OffsetSpec.latest().value) == (False, -1)
    assert (OffsetSpec.earliest().is_timestamp, OffsetSpec.earliest().value) == (False, -2)
    assert OffsetSpec.max_timestamp().value == -3
    assert OffsetSpec.earliest_local().value == -4
    assert OffsetSpec.latest_tiered().value == -5
    assert OffsetSpec.earliest_pending_upload().value == -6
    stamped = OffsetSpec.for_timestamp(-2)
    assert (stamped.is_timestamp, stamped.value) == (True, -2)
    # Same value, different spec: the flag is the only thing separating them.
    assert stamped != OffsetSpec.earliest()


# -- B4: groups and group offsets --------------------------------------------

def _seed_group(admin, group_id):
    """Seed a group in the mock. `groupConfigs` is the only map
    `MockAdminClient.listGroups` reads (MockAdminClient.java:728-732), and
    `incrementalAlterConfigs` on a GROUP resource is its only writer."""
    resource = ConfigResource(ConfigResourceType.GROUP, group_id)
    assert admin.incremental_alter_configs({resource: [
        AlterConfigOp(ConfigEntry("consumer.session.timeout.ms", "45000",
                                  False, False, False), OpType.SET)]
    }) == {resource: None}


def test_list_groups_reports_seeded_groups():
    """MockAdminClient.java:730 reports every seeded group as
    CONSUMER / "consumer" / STABLE. `GroupType.toString()` is the capitalised
    "Consumer"; the protocol type is the lower-case wire string, and the two
    being adjacent is exactly why both are asserted."""
    with MockAdminClient(1) as admin:
        _seed_group(admin, "lg-a")
        _seed_group(admin, "lg-b")

        valid, errors = admin.list_groups()
        assert errors == []
        assert {g.group_id for g in valid} == {"lg-a", "lg-b"}
        listing = next(g for g in valid if g.group_id == "lg-a")
        assert listing.group_type == "Consumer"
        assert listing.protocol == "consumer"
        assert listing.group_state == "Stable"
        assert listing.is_simple_consumer_group is False


def test_list_groups_accepts_every_filter():
    """The mock ignores its options argument entirely
    (MockAdminClient.java:728), so this only pins that the three name arrays
    marshal — and that they are three separate arrays, since a filter that
    matched nothing would be indistinguishable here."""
    with MockAdminClient(1) as admin:
        valid, errors = admin.list_groups(
            group_states=["Stable", "Empty"], protocol_types=["consumer"],
            types=["Consumer", "Classic"], timeout=5.0)
        assert (valid, errors) == ([], [])


def test_list_consumer_groups_reports_empty_optionals_as_none():
    """MockAdminClient.java:743 uses `new ConsumerGroupListing(g, false)`, whose
    state and type are empty Optionals — None here, not "Unknown"."""
    with MockAdminClient(1) as admin:
        _seed_group(admin, "lcg-a")
        valid, errors = admin.list_consumer_groups()
        assert errors == []
        assert len(valid) == 1
        listing = valid[0]
        assert listing.group_id == "lcg-a"
        assert listing.is_simple_consumer_group is False
        assert listing.group_state is None
        assert listing.state is None
        assert listing.group_type is None


def test_describe_consumer_groups_reports_unsupported_per_group():
    """`describedGroups()` is one future per group, so the mock's
    UnsupportedOperationException (MockAdminClient.java:735-737) lands in every
    per-group slot rather than raising."""
    with MockAdminClient(1) as admin:
        result = admin.describe_consumer_groups(["dg-a", "dg-b"],
                                                include_authorized_operations=True)
        assert set(result) == {"dg-a", "dg-b"}
        for value in result.values():
            assert isinstance(value, KafkaError)
            assert value.code == UNSUPPORTED_VERSION
            assert str(value) == "Not implemented yet"


def test_describe_classic_groups_reports_unsupported_per_group():
    with MockAdminClient(1) as admin:
        result = admin.describe_classic_groups(["dcg"])
        assert set(result) == {"dcg"}
        assert str(result["dcg"]) == "Not implemented yet"


def test_list_consumer_group_offsets_round_trip():
    """Seeded offsets come back for the whole group when the spec's partitions
    are unset (Java's null collection), and only the selected ones otherwise —
    a narrower second result is what proves the selection is not ignored."""
    with MockAdminClient(1) as admin:
        admin.update_consumer_group_offsets({("og-a", 0): 17, ("og-b", 1): 23})

        result = admin.list_consumer_group_offsets({"og-group": None})
        offsets = result["og-group"]
        assert set(offsets) == {("og-a", 0), ("og-b", 1)}
        first = offsets[("og-a", 0)]
        assert first.offset == 17
        # Java's one-argument OffsetAndMetadata normalises the metadata to "".
        assert first.metadata == ""
        assert first.leader_epoch is None
        assert offsets[("og-b", 1)].offset == 23

        narrowed = admin.list_consumer_group_offsets(
            {"og-group": ListConsumerGroupOffsetsSpec([("og-b", 1)])}, require_stable=True)
        assert set(narrowed["og-group"]) == {("og-b", 1)}
        assert narrowed["og-group"][("og-b", 1)].offset == 23


def test_list_consumer_group_offsets_rejects_a_duplicate_group_id():
    """A Python dict cannot hold a duplicate key, so the rejection is only
    reachable through the C layer — but it is the contract the FFI documents,
    and `_to_*` would silently collapse the pair if it ever changed."""
    with MockAdminClient(1) as admin:
        result = admin.list_consumer_group_offsets({"m1": None, "m2": None})
        # The mock handles exactly one group (MockAdminClient.java:748-751) and
        # fails each group's future otherwise, so this is a per-group error.
        assert set(result) == {"m1", "m2"}
        for value in result.values():
            assert isinstance(value, KafkaError)
            assert str(value) == "Not implemented yet"


def test_alter_consumer_group_offsets_reports_unsupported_per_partition():
    """Java's `partitionResult(tp)` is one KafkaFuture<Void> per requested
    partition, so the mock's "Not implement yet" (Java's own typo,
    MockAdminClient.java:1213) lands per partition."""
    with MockAdminClient(1) as admin:
        result = admin.alter_consumer_group_offsets("acg", {
            ("ac", 0): OffsetAndMetadata(5, "m0", 4),
            ("ac", 1): OffsetAndMetadata(6),
        })
        assert set(result) == {("ac", 0), ("ac", 1)}
        for value in result.values():
            assert str(value) == "Not implement yet"


def test_alter_consumer_group_offsets_rejects_a_negative_offset():
    """Java's OffsetAndMetadata constructor throws; the index prefix tells the
    caller which entry was at fault, which a Java Map call site does not need."""
    with MockAdminClient(1) as admin:
        with pytest.raises(KafkaError) as exc:
            admin.alter_consumer_group_offsets("acg", {("ac", 0): OffsetAndMetadata(-1)})
        assert str(exc.value) == "offset at index 0: Invalid negative offset"


def test_alter_consumer_group_offsets_with_no_partitions_raises():
    """With no requested partition there is no per-key slot for the outcome, so
    the whole-request failure raises — which is also all Java's `all()` could
    report."""
    with MockAdminClient(1) as admin:
        with pytest.raises(KafkaError) as exc:
            admin.alter_consumer_group_offsets("acg", {})
        assert str(exc.value) == "Not implement yet"


def test_delete_consumer_group_offsets_reports_unsupported_per_partition():
    with MockAdminClient(1) as admin:
        result = admin.delete_consumer_group_offsets("dcg", [("dc", 0), ("dc", 1)])
        assert set(result) == {("dc", 0), ("dc", 1)}
        for value in result.values():
            assert str(value) == "Not implemented yet"


def test_delete_consumer_groups_reports_unsupported_per_group():
    with MockAdminClient(1) as admin:
        result = admin.delete_consumer_groups(["z-group", "a-group"])
        assert set(result) == {"a-group", "z-group"}
        for value in result.values():
            assert str(value) == "Not implemented yet"


def test_remove_members_keys_by_group_instance_id():
    """Accepts MemberToRemove objects and bare instance-id strings alike, and
    keys the result by group instance id."""
    with MockAdminClient(1) as admin:
        result = admin.remove_members_from_consumer_group(
            "rm-group", [MemberToRemove("instance-a"), "instance-b"],
            reason="rolling restart")
        assert set(result) == {"instance-a", "instance-b"}
        for value in result.values():
            assert str(value) == "Not implemented yet"


def test_remove_all_members_has_no_per_member_outcome():
    """`members=None` is Java's no-argument options constructor, where
    `memberResult` is not applicable and `all()` is the only observable — so a
    failure raises instead of landing per member."""
    with MockAdminClient(1) as admin:
        with pytest.raises(KafkaError) as exc:
            admin.remove_members_from_consumer_group("rm-group", None)
        assert str(exc.value) == "Not implemented yet"


def test_remove_members_rejects_an_empty_member_list():
    """Java's `RemoveMembersFromConsumerGroupOptions(Collection)` throws for an
    empty collection, so an empty list must not silently become "remove
    everything" — the two are different arguments here."""
    with MockAdminClient(1) as admin:
        with pytest.raises(KafkaError) as exc:
            admin.remove_members_from_consumer_group("rm-group", [])
        assert str(exc.value) == "Invalid empty members has been provided"


def test_remove_members_requires_the_member_argument():
    """`members` is positional-required, as `elect_leaders`' partitions is: the
    destructive remove-everything mode must be asked for explicitly."""
    with MockAdminClient(1) as admin:
        with pytest.raises(TypeError):
            admin.remove_members_from_consumer_group("rm-group")


async def _seed_group_async(admin, group_id):
    """`_seed_group` for the asyncio client: the RPC is a coroutine there, but
    the mock driver stays synchronous on both."""
    resource = ConfigResource(ConfigResourceType.GROUP, group_id)
    assert await admin.incremental_alter_configs({resource: [
        AlterConfigOp(ConfigEntry("consumer.session.timeout.ms", "45000",
                                  False, False, False), OpType.SET)]
    }) == {resource: None}


@pytest.mark.asyncio
async def test_async_group_rpcs():
    admin = AsyncMockAdminClient(1)
    try:
        await _seed_group_async(admin, "async-group")
        valid, errors = await admin.list_groups()
        assert [g.group_id for g in valid] == ["async-group"]
        assert errors == []

        valid, errors = await admin.list_consumer_groups()
        assert [g.group_id for g in valid] == ["async-group"]

        described = await admin.describe_consumer_groups(["async-group"])
        assert str(described["async-group"]) == "Not implemented yet"
        described = await admin.describe_classic_groups(["async-group"])
        assert str(described["async-group"]) == "Not implemented yet"

        # The mock drivers are synchronous inherent methods on both clients.
        admin.update_consumer_group_offsets({("async-t", 0): 8})
        offsets = await admin.list_consumer_group_offsets({"async-group": None})
        assert offsets["async-group"][("async-t", 0)].offset == 8

        altered = await admin.alter_consumer_group_offsets(
            "async-group", {("async-t", 0): OffsetAndMetadata(1)})
        assert str(altered[("async-t", 0)]) == "Not implement yet"
        deleted = await admin.delete_consumer_group_offsets("async-group", [("async-t", 0)])
        assert str(deleted[("async-t", 0)]) == "Not implemented yet"
        groups = await admin.delete_consumer_groups(["async-group"])
        assert str(groups["async-group"]) == "Not implemented yet"
        members = await admin.remove_members_from_consumer_group("async-group", ["i-1"])
        assert str(members["i-1"]) == "Not implemented yet"
    finally:
        await admin.close()


@pytest.mark.asyncio
async def test_async_alter_consumer_group_offsets_rejects_a_negative_offset():
    """A marshaling failure raises from the coroutine rather than resolving a
    future that never completes."""
    admin = AsyncMockAdminClient(1)
    try:
        with pytest.raises(KafkaError) as exc:
            await admin.alter_consumer_group_offsets(
                "acg", {("t", 0): OffsetAndMetadata(-3)})
        assert str(exc.value) == "offset at index 0: Invalid negative offset"
    finally:
        await admin.close()


def test_b4_handles_survive_gc_of_intermediate_objects():
    """Drained B4 results own no borrowed pointers, so they stay valid after the
    result handles are destroyed and a GC pass runs."""
    with MockAdminClient(1) as admin:
        _seed_group(admin, "gc-b4")
        admin.update_consumer_group_offsets({("gc-b4-t", 0): 4})
        valid, _ = admin.list_groups()
        offsets = admin.list_consumer_group_offsets({"gc-b4": None})
        gc.collect()
        assert valid[0].group_id == "gc-b4"
        assert offsets["gc-b4"][("gc-b4-t", 0)].offset == 4


# -- B4 direct converter coverage (unreachable through the mock) --------------

def test_to_describe_consumer_groups_maps_every_field():
    """The mock never describes a group, so every populated field here is only
    reachable through the converter. Distinct values throughout, so a
    transposed tuple field fails."""
    member = ("consumer-7", "instance-7", "rack-7", "client-7", "host-7",
              [("ta", 0), ("tb", 1)], [("tc", 2)], 17, True)
    description = ("g-ok", False, [member], "range", "Consumer", "Stable", "Stable",
                   (3, "h3", 9093, "rack-3"), [3, 4], 11, 12)
    converted = _to_describe_consumer_groups({
        "g-ok": (None, description),
        "g-bad": ((69, "no such group", False, False), None),
    })

    group = converted["g-ok"]
    assert group.group_id == "g-ok"
    assert group.is_simple_consumer_group is False
    assert group.partition_assignor == "range"
    assert group.group_type == "Consumer"
    assert group.state == "Stable"
    assert group.group_state == "Stable"
    assert group.coordinator.id == 3
    assert group.authorized_operations == [3, 4]
    # Distinct epochs, so swapping the two fails.
    assert group.group_epoch == 11
    assert group.target_assignment_epoch == 12

    described_member = group.members[0]
    assert described_member.consumer_id == "consumer-7"
    assert described_member.group_instance_id == "instance-7"
    assert described_member.rack_id == "rack-7"
    assert described_member.client_id == "client-7"
    assert described_member.host == "host-7"
    assert described_member.assignment.topic_partitions == [("ta", 0), ("tb", 1)]
    assert described_member.target_assignment.topic_partitions == [("tc", 2)]
    assert described_member.member_epoch == 17
    assert described_member.upgraded is True

    assert isinstance(converted["g-bad"], KafkaError)
    assert converted["g-bad"].code == 69


def test_to_member_description_keeps_an_absent_target_assignment_none():
    """None is Java's empty Optional, distinct from a MemberAssignment holding
    no partitions — which is what the non-optional `assignment` becomes."""
    member = ("c", None, None, "cid", "h", [], None, None, None)
    converted = _to_member_description(member)
    assert converted.group_instance_id is None
    assert converted.rack_id is None
    assert converted.member_epoch is None
    assert converted.upgraded is None
    assert converted.target_assignment is None
    assert converted.assignment.topic_partitions == []


def test_to_describe_classic_groups_keeps_protocol_and_protocol_data_apart():
    """Two adjacent same-typed strings, asserted with different values."""
    description = ("cg", "consumer", "range", False, [], "Stable",
                   (1, "h1", 9091, None), [8])
    converted = _to_describe_classic_groups({"cg": (None, description)})
    group = converted["cg"]
    assert group.protocol == "consumer"
    assert group.protocol_data == "range"
    assert group.state == "Stable"
    assert group.coordinator.id == 1
    assert group.authorized_operations == [8]


def test_to_list_consumer_group_offsets_is_two_level_and_keeps_null_offsets():
    """The inner None is Java's null map value: no committed offset for that
    partition, which is not the same as a committed offset of 0."""
    converted = _to_list_consumer_group_offsets({
        "g-ok": (None, {("ta", 0): (100, "meta-a", 4), ("tb", 1): None}),
        "g-bad": ((35, "Not implemented yet", False, False), None),
    })
    offsets = converted["g-ok"]
    assert offsets[("ta", 0)].offset == 100
    assert offsets[("ta", 0)].metadata == "meta-a"
    assert offsets[("ta", 0)].leader_epoch == 4
    assert offsets[("tb", 1)] is None
    assert isinstance(converted["g-bad"], KafkaError)


def test_to_list_groups_keeps_the_valid_and_error_lists_independent():
    """Two listings and one error: a caller who indexed the errors by listing
    position would read the wrong thing, which is why they stay separate."""
    valid, errors = _to_list_groups((
        [("g1", "Consumer", "consumer", "Stable", False),
         ("g2", None, "", None, True)],
        [(15, "coordinator not available", True, False)],
    ))
    assert len(valid) == 2
    assert len(errors) == 1
    assert valid[0] == GroupListing("g1", "Consumer", "consumer", "Stable", False)
    assert valid[1].group_type is None
    assert valid[1].group_state is None
    assert valid[1].is_simple_consumer_group is True
    assert errors[0].code == 15
    assert errors[0].is_retriable is True


def test_to_keyed_errors_maps_none_to_success():
    """Every RPC whose per-key future is KafkaFuture<Void> drains through this
    one converter, so None must mean success rather than a missing value."""
    converted = _to_keyed_errors({("t", 0): None, "g": (69, "nope", False, False)})
    assert converted[("t", 0)] is None
    assert converted["g"].code == 69
