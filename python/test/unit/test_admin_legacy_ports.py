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

"""Behaviour the legacy ``test_admin.py`` (of the removed ``admin.py`` binding) pinned that
the new ``confluent_kafka.admin`` tests did not, ported to the new API and asserting
Java's behaviour, not the legacy surface. The legacy file is deleted; this docstring is
the record of where each of its 182 tests went.

Ported here (legacy test -> test below):
``create_topics_with_configs`` -> ``test_create_topics_with_configs``;
``create_topics_broker_defaults`` -> ``test_create_topics_broker_defaults``;
``create_topics_validate_only_option_is_accepted`` ->
``test_create_topics_validate_only_has_no_effect_on_the_mock``;
``create_topics_replicas_assignment_accepted`` ->
``test_create_topics_replicas_assignments_form``;
``create_topics_empty_batch`` -> ``test_create_topics_empty_batch``;
``list_topics`` / ``list_topics_empty`` / ``list_topics_call_failure_raises`` ->
``test_list_topics_*`` (three tests); ``describe_topics_partial_failure`` ->
``test_describe_topics_partial_failure``; ``describe_topics_by_ids`` ->
``test_describe_topics_by_ids``; ``delete_topics_partial_failure`` ->
``test_delete_topics_partial_failure``; ``describe_configs_partial_failure`` ->
``test_describe_configs_entry_fields_and_logger_resource``;
``describe_configs_empty_batch`` -> ``test_describe_configs_empty_batch``;
``incremental_alter_configs_set_then_delete`` ->
``test_incremental_alter_configs_two_ops_on_one_resource``;
``config_resource_and_replica_are_usable_dict_keys`` ->
``test_config_resource_and_replica_are_usable_dict_keys``;
``list_groups_reports_seeded_groups`` / ``list_groups_accepts_every_filter`` ->
``test_list_groups_*``; ``timeout_accepts_float_and_timedelta`` ->
``test_option_timeout_ms_is_an_int_of_milliseconds``;
``byte_arguments_of_2_gib_raise_overflow_error`` ->
``test_scram_byte_arguments_of_2_gib_raise_overflow_error`` and
``test_delegation_token_hmac_of_2_gib_raises_overflow_error`` (the live
``checked_len_to_int32`` calls, through ``Admin.create``);
``alter_user_scram_credentials_passes_an_empty_password_through`` /
``..._accepts_an_explicitly_empty_salt`` -> ``test_scram_alteration_rows`` (empty salt) and
``test_scram_empty_password_reaches_the_mock`` (empty password).

Covered by the new tests (same behaviour, new API):
* lifecycle, close, context manager, closed-client calls, config validation, the
  no-broker marshaling error, the real-client constructors, sync / asyncio lifecycle
  (``test_create_and_close`` .. ``test_async_create_and_close``, the ``test_admin_client_*``
  tests, ``test_async_*_constructs_and_closes``, ``test_async_close_idempotent``,
  ``test_async_call_after_close_raises``, ``test_*_after_close*``) ->
  ``test_admin_client.py`` (``test_close_admin_client``, ``test_admin_client_failure_when_closed``,
  ``test_config_*``, ``test_create_returns_kafka_admin_client``, ``test_async_create_*``),
  ``test_admin_client_topics.py`` / ``_groups.py`` / ``_security.py``
  (``test_calls_after_close_raise_illegal_state``, ``test_async_calls_after_close_raise_illegal_state``)
  and ``test_mock_admin_client.py`` (``test_close_does_nothing_and_the_mock_keeps_answering``,
  ``test_create_rejects_*``);
* ``create_topics_partial_failure_keeps_both_outcomes`` ->
  ``test_create_existing_topic_fails_with_topic_exists``,
  ``test_create_with_replication_factor_too_large_fails``;
* ``describe_topics_by_ids_rejects_invalid_id``, ``delete_topics_by_ids*`` ->
  ``test_describe_and_delete_topics_by_id`` (an invalid base64 id cannot be built:
  ``Uuid`` is a value type, ``test_admin_common_types.py``);
* ``create_partitions_*``, ``delete_records_*``, ``elect_leaders_call_failure_raises``,
  ``describe_*_reports_unsupported_per_*``, ``alter_*_reports_unsupported_per_*``,
  ``delete_consumer_group*_reports_unsupported_*``, ``create_acls`` / ``describe_acls`` /
  ``delete_acls`` / client quotas / SCRAM / producers / transactions "unsupported" tests ->
  ``test_unimplemented_methods_raise_java_messages`` and
  ``test_unimplemented_g3b_methods_raise_java_messages`` (Java's mock throws
  ``UnsupportedOperationException`` out of the call; the legacy per-key error was the Rust
  core's stand-in);
* ``describe_cluster*`` -> ``test_describe_cluster_returns_brokers_and_controller``,
  ``test_describe_cluster_timeout_recovers_on_next_call``;
* ``incremental_alter_configs_partial_failure`` / ``_unsupported_op_type_*`` /
  ``list_config_resources*`` -> ``test_incremental_alter_configs_unknown_topic_is_*``,
  ``test_incremental_alter_configs_unsupported_op_and_type``,
  ``test_incremental_alter_configs_client_metrics_creates_resource``,
  ``test_list_config_resources_*``; ``describe_log_dirs`` /
  ``alter_replica_log_dirs_partial_failure`` / ``describe_replica_log_dirs_omits_unknown_topics`` ->
  ``test_describe_log_dirs``, ``test_replica_log_dirs``;
* ``alter_partition_reassignments_partial_failure``, ``list_partition_reassignments_round_trip``,
  ``alter_partition_reassignments_rejects_empty_replicas`` ->
  ``test_alter_then_list_partition_reassignments_reports_adding_and_removing``,
  ``test_admin_value_types.py`` (``NewPartitionReassignment``);
* ``elect_leaders_requires_partitions_explicitly`` / ``_rejects_bad_election_type``,
  ``list_offsets_rejects_bad_isolation_level``, ``offset_spec_factories_use_javas_sentinels``,
  ``list_offsets_*`` -> ``test_admin_client_topics.py`` (``test_election_reassignment_and_offset_args``,
  ``test_offset_spec_values``), ``test_admin_common_types.py`` (enums), ``test_list_offsets``;
* groups: ``list_consumer_group_offsets_*`` (negative seeded offset, duplicate group id,
  empty selection), ``alter_consumer_group_offsets_rejects_*``, ``remove_members_*`` ->
  ``test_mock_list_consumer_group_offsets_*``, ``test_list_consumer_group_offsets_java_mock_quirks``,
  ``test_admin_client_groups.py`` (``test_remove_members_requires_its_options``,
  ``test_list_consumer_group_offsets_rejects_what_java_has_no_overload_for``), and the
  ``OffsetAndMetadata`` checks in the consumer tests;
* ACL / quota / SCRAM / feature / token / producer / transaction argument validation
  (``create_acls_rejects_what_javas_constructors_reject``, ``acl_binding_filter_defaults_*``,
  ``client_quota_*``, ``alter_client_quotas_rejects_a_duplicate_entity``,
  ``update_features_rejects_what_javas_feature_update_constructor_rejects``,
  ``create_delegation_token_*``, ``delegation_token_lifecycle``, ``describe_features_*``,
  ``update_features_applies_and_validates``) -> ``test_admin_client_security.py``
  (``test_acl_arguments``, ``test_client_quota_arguments``, ``test_feature_arguments``,
  ``test_delegation_token_arguments``, ``test_producer_and_transaction_arguments``),
  ``test_admin_common_types.py`` and the mock's features / token tests;
* ``sigint_while_waiting_drains_callback_then_reraises`` ->
  ``test_ctrl_c_re_raises_at_once``; async tests (``async_create_and_describe``,
  ``async_partial_failure``, ``async_concurrent_calls``, ``async_b3_*``, ``async_group_rpcs``,
  ``async_b5a_rpcs``, ``async_b5b_rpcs``, ``async_b6_rpcs``) ->
  ``test_async_calls_at_once``, ``test_async_create_topics_and_list_topics``,
  ``test_async_mock_admin_client.py``; ``authorized_operations_none_stays_distinct_from_empty`` /
  ``partition_info_elr_none_stays_distinct_from_empty`` -> ``test_admin_value_types.py`` /
  ``test_topic_description_from_core_data``.

Further legacy tests, by name:
* ``mock_requires_at_least_one_broker`` -> ``test_create_rejects_zero_brokers_rather_than_fabricating_a_controller``;
  ``create_topics_returns_metadata`` -> ``test_create_then_list_and_describe``;
  ``async_list_and_delete``, ``async_log_dirs_and_listings`` -> ``test_async_mock_admin_client.py``
  (``test_topics_and_cluster``, ``test_partitions_records_log_dirs_and_offsets``);
  ``async_elect_leaders_call_failure_raises`` -> ``test_unimplemented_methods_raise_java_messages``
  (Java's mock throws) and ``test_election_reassignment_and_offset_args``;
  ``incremental_alter_configs_bad_op_type_raises`` -> ``test_incremental_alter_configs_unsupported_op_and_type``
  (an unknown op type cannot be built: ``AlterConfigOp.OpType`` is an enum);
  ``alter_consumer_group_offsets_with_no_partitions_raises``,
  ``remove_all_members_has_no_per_member_outcome`` -> ``test_admin_client_groups.py``
  (``test_alter_consumer_group_offsets_passes_its_offsets``, ``test_remove_all_members_fails_to_describe_the_group``);
  ``create_acls_round_trips_every_field_through_the_key``,
  ``describe_acls_accepts_the_any_and_match_values_create_rejects``,
  ``delete_acls_keeps_a_null_name_distinct_from_an_empty_one`` -> ``test_acl_arguments``,
  ``test_acl_values_from_the_drains`` (``test_admin_client_security.py``);
  ``describe_client_quotas_raises_and_accepts_every_match_type``,
  ``alter_client_quotas_keeps_the_default_entity_distinct`` -> ``test_client_quota_arguments``;
  ``describe_delegation_token_keeps_no_filter_apart_from_a_filter`` ->
  ``test_describe_delegation_token_owners_filter`` and ``test_delegation_token_arguments``;
  ``list_transactions_filters_keep_each_column_apart`` -> ``test_producer_and_transaction_arguments``.

Legacy-only surface, dropped (the thing tested no longer exists):
* ``*_handles_survive_gc_of_intermediate_objects`` (``b2``, ``b3``, ``b4``, ``handle``): the
  legacy code kept raw C result handles alive across Python objects; the new package frees
  its payloads itself (``test_free_functions_free_their_own_payloads``,
  ``test_per_key_error_crosses_in_the_borrowed_form``);
* every ``test_to_*`` (``to_full_config_entry*``, ``to_log_dir_description*``,
  ``to_describe_log_dirs_error_arm*``, ``to_list_offsets*``, ``to_list_partition_reassignments*``,
  ``to_elect_leaders*``, ``to_describe_consumer_groups*``, ``to_member_description*``,
  ``to_describe_classic_groups*``, ``to_list_consumer_group_offsets*``, ``to_list_groups*``,
  ``to_keyed_errors*``, ``to_acl_binding*``, ``to_delete_acls*``, ``to_describe_client_quotas*``,
  ``to_alter_client_quotas*``, ``to_create_acls*``, ``to_describe_acls*``, ``to_delegation_token*``,
  ``to_describe_user_scram*``, ``to_feature_metadata*``, ``to_describe_producers*``,
  ``to_describe_transactions*``, ``to_fence_producers*``, ``to_list_transactions*``) and every
  ``*_rows_*`` test: the private conversion helpers and the positional row tuples of the old
  binding; the new equivalents are the ``*_from_core_data`` / ``*_from_the_drain`` tests of
  ``test_admin_client_topics.py``, ``_groups.py`` and ``_security.py``;
* ``*_returns_or_raises_without_hanging``, ``admin_client_rejects_mock_only_driver``,
  ``mock_offset_drivers_are_mock_only``, ``b3_empty_batches``: the legacy flat ``AdminClient``
  had mock-only drivers on one class; the new ``MockAdminClient`` is its own class;
* ``describe_acls_raises_because_it_has_one_future_for_the_whole_call``,
  ``describe_user_scram_credentials_raises``, ``list_transactions_fails_the_whole_call``,
  ``describe_acls_requires_the_filter_argument``, ``create_delegation_token_rejects_a_non_user_renewer``:
  the legacy binding raised from the call because it had no futures; the new results hold
  futures (``test_describe_acls_unknown_filter_fails_the_future``) and the mock's Java behaviour
  is in ``test_create_delegation_token_rejects_non_user_renewer``;
* ``timeout_conversion_is_exact_for_whole_milliseconds``: the legacy seconds-to-ms helper (new:
  ``timeout_ms`` is an int, ``test_negative_option_timeout_is_sent_as_zero``);
* ``call_after_close_raises`` for the legacy ``closed`` property and ``test_create_and_close``
  checks of ``admin.closed``: Java's ``Admin`` has no ``closed`` accessor (strict parity).
"""

from __future__ import annotations

from typing import Any

import pytest

from confluent_kafka.admin import (
    Admin,
    AlterConfigOp,
    ConfigEntry,
    CreateTopicsOptions,
    ListGroupsOptions,
    ListTopicsOptions,
    MockAdminClient,
    NewTopic,
    ScramCredentialInfo,
    ScramMechanism,
    UserScramCredentialUpsertion,
)
from confluent_kafka.common import TopicCollection, TopicPartitionReplica
from confluent_kafka.common.config import ConfigResource
from confluent_kafka.common.errors.timeout_error import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.errors.topic_exists_error import TopicExistsError
from confluent_kafka.common.errors.unknown_topic_or_partition_error import (
    UnknownTopicOrPartitionError,
)
from confluent_kafka.common.errors.unsupported_version_error import UnsupportedVersionError


def admin(num_brokers: int = 3) -> MockAdminClient:
    return MockAdminClient.create().num_brokers(num_brokers=num_brokers).build()


def created(client: MockAdminClient, name: str, partitions: int = 1,
            replication_factor: int = 1) -> None:
    client.create_topics(new_topics=[NewTopic(
        name=name, num_partitions=partitions, replication_factor=replication_factor)]
    ).all().result()


def error_of(future: Any) -> BaseException:
    error: BaseException | None = future.exception()
    assert error is not None
    return error


# ---- createTopics -------------------------------------------------------------

def test_create_topics_with_configs() -> None:
    client = admin(1)
    result = client.create_topics(new_topics=[NewTopic(
        name="configured", num_partitions=1, replication_factor=1
    ).configs(configs={"cleanup.policy": "compact"})])
    result.all().result()
    config = client.describe_configs(resources=[ConfigResource(
        type=ConfigResource.Type.TOPIC, name="configured")]).all().result()
    (entry,) = next(iter(config.values())).entries()
    assert (entry.name(), entry.value()) == ("cleanup.policy", "compact")
    assert entry.is_sensitive() is False and entry.is_read_only() is False


def test_create_topics_broker_defaults() -> None:
    # None -> broker defaults; Java's mock uses its default partition count (1) and
    # min(brokers, default replication factor 3) replicas.
    client = admin(2)
    result = client.create_topics(new_topics=[NewTopic(
        name="defaults", num_partitions=None, replication_factor=None)])
    assert result.num_partitions(topic="defaults").result() == 1
    assert result.replication_factor(topic="defaults").result() == 2


def test_create_topics_validate_only_has_no_effect_on_the_mock() -> None:
    # Java's MockAdminClient.createTopics never reads its options, so validateOnly
    # does not stop the topic being created.
    client = admin(1)
    client.create_topics(
        new_topics=[NewTopic(name="validated", num_partitions=1, replication_factor=1)],
        options=CreateTopicsOptions().validate_only(validate_only=True)).all().result()
    assert "validated" in client.list_topics().names().result()


def test_create_topics_replicas_assignments_form() -> None:
    # Java's NewTopic(name, Map<Integer, List<Integer>>); the mock applies its
    # defaults rather than the assignment, so only the form is exercised.
    client = admin(3)
    topic = NewTopic(name="assigned", replicas_assignments={0: [0, 1], 1: [1, 2]})
    result = client.create_topics(new_topics=[topic])
    result.all().result()
    assert result.num_partitions(topic="assigned").result() == 1
    assert result.replication_factor(topic="assigned").result() == 3


def test_create_topics_empty_batch() -> None:
    result = admin(1).create_topics(new_topics=[])
    assert result.values() == {}
    result.all().result()


# ---- listTopics ---------------------------------------------------------------

def test_list_topics_lists_listings_by_name() -> None:
    client = admin(1)
    created(client, "beta")
    created(client, "alpha")
    listings = client.list_topics(
        options=ListTopicsOptions().list_internal(list_internal=True)).listings().result()
    assert {listing.name() for listing in listings} == {"alpha", "beta"}
    assert all(listing.is_internal() is False for listing in listings)
    assert all(listing.topic_id() for listing in listings)


def test_list_topics_empty() -> None:
    assert admin(1).list_topics().names().result() == set()


def test_list_topics_call_failure_fails_the_one_future() -> None:
    # listTopics has a single future in Java, so a failure is that future's.
    client = admin(1)
    client.timeout_next_request(number_of_request=1)
    result = client.list_topics()
    assert isinstance(error_of(result.names()), KafkaTimeoutError)
    assert isinstance(error_of(result.listings()), KafkaTimeoutError)
    assert client.list_topics().names().result() == set()


# ---- describeTopics / deleteTopics --------------------------------------------

def test_describe_topics_partial_failure() -> None:
    client = admin(3)
    created(client, "described", 2, 2)
    result = client.describe_topics(
        topics=TopicCollection.of_topic_names(topics=["described", "missing"]))
    values = result.topic_name_values()
    assert values is not None and set(values) == {"described", "missing"}
    missing = error_of(values["missing"])
    assert isinstance(missing, UnknownTopicOrPartitionError)
    assert str(missing) == "Topic missing not found."
    described = values["described"].result()
    assert described.name() == "described"
    assert described.is_internal() is False
    # Java's mock completes with an empty ISR and no authorized operations reported.
    assert described.authorized_operations() == set()
    assert len(described.partitions()) == 2
    p0 = described.partitions()[0]
    assert p0.partition() == 0
    leader = p0.leader()
    assert leader is not None
    assert (leader.id(), leader.host(), leader.port()) == (0, "localhost", 1000)
    assert [node.id() for node in p0.replicas()] == [0, 1]
    assert p0.isr() == []
    assert p0.elr() == [] and p0.last_known_elr() == []


def test_describe_topics_by_ids() -> None:
    client = admin(1)
    topic_id = client.create_topics(new_topics=[NewTopic(
        name="by-id", num_partitions=1, replication_factor=1)]).topic_id(topic="by-id").result()
    values = client.describe_topics(
        topics=TopicCollection.of_topic_ids(topics=[topic_id])).topic_id_values()
    assert values is not None and list(values) == [topic_id]
    described = values[topic_id].result()
    assert described.name() == "by-id" and described.topic_id() == topic_id


def test_delete_topics_partial_failure() -> None:
    client = admin(1)
    created(client, "doomed")
    values = client.delete_topics(
        topics=TopicCollection.of_topic_names(topics=["doomed", "never-existed"])
    ).topic_name_values()
    assert values is not None
    assert values["doomed"].result() is None
    missing = error_of(values["never-existed"])
    assert isinstance(missing, UnknownTopicOrPartitionError)
    assert str(missing) == "Topic never-existed does not exist."
    assert client.list_topics().names().result() == set()


def test_create_existing_topic_is_that_topics_failure_only() -> None:
    client = admin(3)
    created(client, "existing")
    result = client.create_topics(new_topics=[
        NewTopic(name="existing", num_partitions=1, replication_factor=1),
        NewTopic(name="fresh", num_partitions=2, replication_factor=1)])
    values = result.values()
    assert isinstance(error_of(values["existing"]), TopicExistsError)
    assert values["fresh"].result() is None
    assert result.num_partitions(topic="fresh").result() == 2


# ---- configs ------------------------------------------------------------------

def test_describe_configs_entry_fields_and_logger_resource() -> None:
    client = admin(1)
    created(client, "cfg-topic")
    topic = ConfigResource(type=ConfigResource.Type.TOPIC, name="cfg-topic")
    logger = ConfigResource(type=ConfigResource.Type.BROKER_LOGGER, name="0")
    client.incremental_alter_configs(configs={topic: [AlterConfigOp(
        config_entry=ConfigEntry(name="retention.ms", value="60000"),
        operation_type=AlterConfigOp.OpType.SET)]}).all().result()
    described = client.describe_configs(resources=[topic, logger]).values()
    config = described[topic].result()
    entry = config.get(name="retention.ms")
    assert entry is not None and entry.value() == "60000"
    # Java's mock builds entries with new ConfigEntry(name, value): source and type
    # stay UNKNOWN, no documentation, no synonyms.
    assert entry.source() is ConfigEntry.ConfigSource.UNKNOWN
    assert entry.type() is ConfigEntry.ConfigType.UNKNOWN
    assert entry.documentation() is None
    assert entry.synonyms() == []
    assert entry.is_default() is False
    assert config.get(name="no.such.config") is None
    # BROKER_LOGGER hits getResourceDescription's default branch.
    error = error_of(described[logger])
    assert isinstance(error, UnsupportedVersionError)
    assert str(error) == "Not implemented yet"


def test_describe_configs_empty_batch() -> None:
    result = admin(1).describe_configs(resources=[])
    assert result.values() == {}
    assert result.all().result() == {}


def test_incremental_alter_configs_two_ops_on_one_resource() -> None:
    client = admin(1)
    created(client, "alter-topic")
    resource = ConfigResource(type=ConfigResource.Type.TOPIC, name="alter-topic")
    result = client.incremental_alter_configs(configs={resource: [
        AlterConfigOp(config_entry=ConfigEntry(name="retention.ms", value="1000"),
                      operation_type=AlterConfigOp.OpType.SET),
        AlterConfigOp(config_entry=ConfigEntry(name="segment.ms", value="2000"),
                      operation_type=AlterConfigOp.OpType.SET)]})
    assert list(result.values()) == [resource]  # two ops, one result key
    result.all().result()
    config = client.describe_configs(resources=[resource]).values()[resource].result()
    assert {entry.name() for entry in config.entries()} == {"retention.ms", "segment.ms"}
    client.incremental_alter_configs(configs={resource: [AlterConfigOp(
        config_entry=ConfigEntry(name="retention.ms", value=None),
        operation_type=AlterConfigOp.OpType.DELETE)]}).all().result()
    config = client.describe_configs(resources=[resource]).values()[resource].result()
    assert {entry.name() for entry in config.entries()} == {"segment.ms"}


def test_config_resource_and_replica_are_usable_dict_keys() -> None:
    client = admin(1)
    created(client, "key-topic")
    described = client.describe_configs(resources=[ConfigResource(
        type=ConfigResource.Type.TOPIC, name="key-topic")]).values()
    assert ConfigResource(type=ConfigResource.Type.TOPIC, name="key-topic") in described
    assert (ConfigResource(type=ConfigResource.Type.TOPIC, name="x")
            == ConfigResource(type=ConfigResource.Type.TOPIC, name="x"))
    assert (ConfigResource(type=ConfigResource.Type.TOPIC, name="x")
            != ConfigResource(type=ConfigResource.Type.BROKER, name="x"))
    replica = TopicPartitionReplica(topic="key-topic", partition=0, broker_id=0)
    assert replica in client.describe_replica_log_dirs(replicas=[replica]).values()
    assert replica == TopicPartitionReplica(topic="key-topic", partition=0, broker_id=0)
    assert replica != TopicPartitionReplica(topic="key-topic", partition=0, broker_id=1)
    assert hash(replica) == hash(
        TopicPartitionReplica(topic="key-topic", partition=0, broker_id=0))


# ---- groups and options -------------------------------------------------------

def test_list_groups_reports_every_seeded_group_with_the_consumer_defaults() -> None:
    from confluent_kafka.common import GroupState, GroupType

    client = admin(1)
    for name in ("lg-a", "lg-b"):
        client.incremental_alter_configs(configs={
            ConfigResource(type=ConfigResource.Type.GROUP, name=name): [AlterConfigOp(
                config_entry=ConfigEntry(name="k", value="v"),
                operation_type=AlterConfigOp.OpType.SET)]}).all().result()
    listings = client.list_groups().all().result()
    assert {listing.group_id() for listing in listings} == {"lg-a", "lg-b"}
    listing = next(entry for entry in listings if entry.group_id() == "lg-a")
    assert listing.type() is GroupType.CONSUMER
    assert listing.protocol() == "consumer"
    assert listing.group_state() is GroupState.STABLE
    assert listing.is_simple_consumer_group() is False


def test_list_groups_ignores_its_filters_on_the_mock() -> None:
    # MockAdminClient.listGroups ignores its options (Java :728), so a filter that
    # matches nothing still lists the group.
    from confluent_kafka.common import GroupState

    client = admin(1)
    client.incremental_alter_configs(configs={
        ConfigResource(type=ConfigResource.Type.GROUP, name="g"): [AlterConfigOp(
            config_entry=ConfigEntry(name="k", value="v"),
            operation_type=AlterConfigOp.OpType.SET)]}).all().result()
    options = ListGroupsOptions.for_consumer_groups().in_group_states(
        group_states={GroupState.EMPTY})
    assert [listing.group_id() for listing in
            client.list_groups(options=options).all().result()] == ["g"]


def test_option_timeout_ms_is_an_int_of_milliseconds() -> None:
    options = CreateTopicsOptions().timeout_ms(timeout_ms=1500)
    assert options.timeout_ms() == 1500
    assert CreateTopicsOptions().timeout_ms() is None


# ---- 2 GiB byte arguments (the C API's int32_t lengths) -----------------------

UNREACHABLE = {"bootstrap.servers": "localhost:1"}


@pytest.mark.parametrize("field", ["password", "salt"])
def test_scram_byte_arguments_of_2_gib_raise_overflow_error(
        field: str, two_gib_bytes: bytes) -> None:
    # checked_len_to_int32 runs before anything is submitted, so no broker is needed.
    kwargs: dict[str, Any] = {"password": b"p"}
    kwargs[field] = two_gib_bytes
    upsertion = UserScramCredentialUpsertion(
        user="u", credential_info=ScramCredentialInfo(
            mechanism=ScramMechanism.SCRAM_SHA_256, iterations=4096), **kwargs)
    with Admin.create(configs=UNREACHABLE) as client:
        with pytest.raises(OverflowError, match=rf"^{field} exceeds 2 GiB$"):
            client.alter_user_scram_credentials(alterations=[upsertion])


@pytest.mark.parametrize("method", ["renew_delegation_token", "expire_delegation_token"])
def test_delegation_token_hmac_of_2_gib_raises_overflow_error(
        method: str, two_gib_bytes: bytes) -> None:
    with Admin.create(configs=UNREACHABLE) as client:
        with pytest.raises(OverflowError, match=r"^hmac exceeds 2 GiB$"):
            getattr(client, method)(hmac=two_gib_bytes)


def test_scram_empty_password_reaches_the_mock() -> None:
    # An empty password is a value, not an absent one: it passes the length check and the
    # call reaches Java's mock, which does not implement it.
    upsertion = UserScramCredentialUpsertion(
        user="u", credential_info=ScramCredentialInfo(
            mechanism=ScramMechanism.SCRAM_SHA_256, iterations=4096), password=b"")
    with pytest.raises(UnsupportedVersionError) as e:
        admin(1).alter_user_scram_credentials(alterations=[upsertion])
    assert str(e.value) == "Not implemented yet"
