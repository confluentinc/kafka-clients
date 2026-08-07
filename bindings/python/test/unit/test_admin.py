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
    _to_describe_log_dirs, _to_describe_replica_log_dirs,
    _to_full_config_entry, _to_log_dir_description,
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
