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

"""A ``None`` argument of every admin RPC is Java's ``null`` (CLAUDE.md, Python
Binding Conventions, Signatures and Implementation over the FFI, exception 4):
where Java dereferences it, ``TypeError("<param> must not be None")`` (or "must not
contain None" for a null element), in Java's order; where Java gives it a meaning,
that meaning; an ``options`` left out is Java's ``new XxxOptions()``, an explicit
``None`` raises on the clients (``KafkaAdminClient`` dereferences it) and is
accepted by the mocks wherever Java's ``MockAdminClient`` ignores ``options``.

Each row: ``(rpc, parameter, the whole argument is None (else one element),
expected error class name or "OK", expected message)``, checked on ``Admin`` and
``AsyncAdmin`` (``ADMIN_ROWS``) and on ``MockAdminClient`` and
``AsyncMockAdminClient`` (``MOCK_ROWS``).
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka._args import MISSING
from confluent_kafka.admin import (
    AbortTransactionSpec,
    Admin,
    AlterConfigOp,
    AsyncAdmin,
    AsyncMockAdminClient,
    ConfigEntry,
    FeatureUpdate,
    ListConsumerGroupOffsetsSpec,
    MemberToRemove,
    MockAdminClient,
    NewPartitionReassignment,
    NewPartitions,
    NewTopic,
    OffsetSpec,
    RecordsToDelete,
    RemoveMembersFromConsumerGroupOptions,
    RenewDelegationTokenOptions,
    ScramMechanism,
    UserScramCredentialDeletion,
)
from confluent_kafka.common import (
    AccessControlEntry,
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    ClientQuotaAlteration,
    ClientQuotaEntity,
    ClientQuotaFilter,
    ElectionType,
    Node,
    PatternType,
    ResourcePattern,
    ResourceType,
    TopicPartition,
    TopicPartitionReplica,
)
from confluent_kafka.common.config.config_resource import ConfigResource
from confluent_kafka.consumer import OffsetAndMetadata

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CLOSED = "Cannot accept new calls when AdminClient is closing."
NODE = Node(id=0, host="h", port=1)

Row = tuple[str, str, bool, str, "str | None"]

TP = TopicPartition(topic="t", partition=0)
BINDING = AclBinding(
    pattern=ResourcePattern(resource_type=ResourceType.TOPIC, name="t",
                            pattern_type=PatternType.LITERAL),
    entry=AccessControlEntry(principal="User:a", host="*", operation=AclOperation.READ,
                             permission_type=AclPermissionType.ALLOW))
SPEC = AbortTransactionSpec(topic_partition=TP, producer_id=7, producer_epoch=1,
                            coordinator_epoch=2)
RES = ConfigResource(type=ConfigResource.Type.TOPIC, name="t")
ENTITY = ClientQuotaEntity(entries={"user": "u"})
OP = AlterConfigOp(config_entry=ConfigEntry(name="a", value="b"),
                   operation_type=AlterConfigOp.OpType.SET)
REPLICA = TopicPartitionReplica(topic="t", partition=0, broker_id=0)

#: rpc (``#`` a second form) -> (the arguments of a valid call, and per parameter a
#: value holding a ``None`` element).
RPCS: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
 "create_topics": ({"new_topics": [NewTopic(name="t", num_partitions=1, replication_factor=1)]}, {"new_topics": [None]}),
 "delete_topics": ({"topics": ["t"]}, {"topics": [None]}),
 "list_topics": ({}, {}),
 "describe_topics": ({"topic_names": ["t"]}, {"topic_names": [None]}),
 "describe_cluster": ({}, {}),
 "describe_acls": ({"filter": AclBindingFilter.ANY}, {}),
 "create_acls": ({"acls": [BINDING]}, {"acls": [None]}),
 "delete_acls": ({"filters": [AclBindingFilter.ANY]}, {"filters": [None]}),
 "describe_configs": ({"resources": [RES]}, {"resources": [None]}),
 "incremental_alter_configs": ({"configs": {RES: [OP]}},
                               {"configs": {RES: None}}),
 "alter_replica_log_dirs": ({"replica_assignment": {REPLICA: "/d"}},
                            {"replica_assignment": {None: "/d"}}),
 "describe_log_dirs": ({"brokers": [0]}, {"brokers": [None]}),
 "describe_replica_log_dirs": ({"replicas": [REPLICA]}, {"replicas": [None]}),
 "create_partitions": ({"new_partitions": {"t": NewPartitions.increase_to(total_count=3)}}, {"new_partitions": {"t": None}}),
 "delete_records": ({"records_to_delete": {TP: RecordsToDelete.before_offset(offset=1)}}, {"records_to_delete": {TP: None}}),
 "create_delegation_token": ({}, {}),
 "renew_delegation_token": ({"hmac": b"m"}, {}),
 "expire_delegation_token": ({"hmac": b"m"}, {}),
 "describe_delegation_token": ({}, {}),
 "describe_consumer_groups": ({"group_ids": ["g"]}, {"group_ids": [None]}),
 "list_consumer_group_offsets": ({"group_specs": {"g": ListConsumerGroupOffsetsSpec()}}, {"group_specs": {"g": None}}),
 "list_consumer_group_offsets#id": ({"group_id": "g"}, {}),
 "delete_consumer_groups": ({"group_ids": ["g"]}, {"group_ids": [None]}),
 "delete_consumer_group_offsets": ({"group_id": "g", "partitions": {TP}}, {"partitions": [None]}),
 "list_groups": ({}, {}),
 "elect_leaders": ({"election_type": ElectionType.PREFERRED, "partitions": {TP}}, {"partitions": [None]}),
 "alter_partition_reassignments": ({"reassignments": {TP: NewPartitionReassignment(target_replicas=[1])}}, {"reassignments": {None: None}}),
 "list_partition_reassignments": ({"partitions": {TP}}, {"partitions": [None]}),
 "remove_members_from_consumer_group": ({"group_id": "g", "options": RemoveMembersFromConsumerGroupOptions(members=[MemberToRemove(group_instance_id="i")])}, {}),
 "alter_consumer_group_offsets": ({"group_id": "g", "offsets": {TP: OffsetAndMetadata(offset=1)}}, {"offsets": {TP: None}}),
 "list_offsets": ({"topic_partition_offsets": {TP: OffsetSpec.latest()}}, {"topic_partition_offsets": {TP: None}}),
 "describe_client_quotas": ({"filter": ClientQuotaFilter.all()}, {}),
 "alter_client_quotas": ({"entries": [ClientQuotaAlteration(entity=ENTITY, ops=[])]}, {"entries": [None]}),
 "describe_user_scram_credentials": ({"users": ["u"]}, {"users": [None]}),
 "alter_user_scram_credentials": ({"alterations": [UserScramCredentialDeletion(user="u", mechanism=ScramMechanism.SCRAM_SHA_256)]}, {"alterations": [None]}),
 "describe_features": ({}, {}),
 "update_features": ({"feature_updates": {"f": FeatureUpdate(max_version_level=1, upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)}}, {"feature_updates": {"f": None}}),
 "unregister_controller": ({"controller_id": 1}, {}),
 "describe_producers": ({"partitions": [TP]}, {"partitions": [None]}),
 "describe_transactions": ({"transactional_ids": ["x"]}, {"transactional_ids": [None]}),
 "abort_transaction": ({"spec": SPEC}, {}),
 "list_transactions": ({}, {}),
 "fence_producers": ({"transactional_ids": ["x"]}, {"transactional_ids": [None]}),
 "list_config_resources": ({"config_resource_types": [ConfigResource.Type.TOPIC]}, {"config_resource_types": [None]}),
 "describe_classic_groups": ({"group_ids": ["g"]}, {"group_ids": [None]}),
 "force_terminate_transaction": ({"transactional_id": "x"}, {}),
}


ADMIN_ROWS: list[Row] = [
    ('create_topics', 'new_topics', True, 'TypeError', 'new_topics must not be None'),
    ('create_topics', 'options', True, 'TypeError', 'options must not be None'),
    ('create_topics', 'new_topics', False, 'TypeError', 'new_topics must not contain None'),
    ('delete_topics', 'topics', True, 'IllegalArgumentError', 'delete_topics(): topics=None matches more than one overload: (topics: Iterable[str]), (topics: TopicCollection)'),
    ('delete_topics', 'options', True, 'TypeError', 'options must not be None'),
    ('list_topics', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_topics', 'topic_names', True, 'TypeError', 'topic_names must not be None'),
    ('describe_topics', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_cluster', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_acls', 'filter', True, 'TypeError', 'filter must not be None'),
    ('describe_acls', 'options', True, 'TypeError', 'options must not be None'),
    ('create_acls', 'acls', True, 'TypeError', 'acls must not be None'),
    ('create_acls', 'options', True, 'TypeError', 'options must not be None'),
    ('create_acls', 'acls', False, 'TypeError', 'acls must not contain None'),
    ('delete_acls', 'filters', True, 'TypeError', 'filters must not be None'),
    ('delete_acls', 'options', True, 'TypeError', 'options must not be None'),
    ('delete_acls', 'filters', False, 'TypeError', 'filters must not contain None'),
    ('describe_configs', 'resources', True, 'TypeError', 'resources must not be None'),
    ('describe_configs', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_configs', 'resources', False, 'TypeError', 'resources must not contain None'),
    ('incremental_alter_configs', 'configs', True, 'TypeError', 'configs must not be None'),
    ('incremental_alter_configs', 'options', True, 'TypeError', 'options must not be None'),
    ('incremental_alter_configs', 'configs', False, 'TypeError', 'configs must not contain None'),
    ('alter_replica_log_dirs', 'replica_assignment', True, 'TypeError', 'replica_assignment must not be None'),
    ('alter_replica_log_dirs', 'options', True, 'TypeError', 'options must not be None'),
    ('alter_replica_log_dirs', 'replica_assignment', False, 'TypeError', 'replica_assignment must not contain None'),
    ('describe_log_dirs', 'brokers', True, 'TypeError', 'brokers must not be None'),
    ('describe_log_dirs', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_log_dirs', 'brokers', False, 'TypeError', 'brokers must not contain None'),
    ('describe_replica_log_dirs', 'replicas', True, 'TypeError', 'replicas must not be None'),
    ('describe_replica_log_dirs', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_replica_log_dirs', 'replicas', False, 'TypeError', 'replicas must not contain None'),
    ('create_partitions', 'new_partitions', True, 'TypeError', 'new_partitions must not be None'),
    ('create_partitions', 'options', True, 'TypeError', 'options must not be None'),
    ('create_partitions', 'new_partitions', False, 'TypeError', 'new_partitions must not contain None'),
    ('delete_records', 'records_to_delete', True, 'TypeError', 'records_to_delete must not be None'),
    ('delete_records', 'options', True, 'TypeError', 'options must not be None'),
    ('delete_records', 'records_to_delete', False, 'TypeError', 'records_to_delete must not contain None'),
    ('create_delegation_token', 'options', True, 'TypeError', 'options must not be None'),
    ('renew_delegation_token', 'hmac', True, 'TypeError', 'hmac must not be None'),
    ('renew_delegation_token', 'options', True, 'TypeError', 'options must not be None'),
    ('expire_delegation_token', 'hmac', True, 'TypeError', 'hmac must not be None'),
    ('expire_delegation_token', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_delegation_token', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_consumer_groups', 'group_ids', True, 'TypeError', 'group_ids must not be None'),
    ('describe_consumer_groups', 'options', True, 'TypeError', 'options must not be None'),
    ('list_consumer_group_offsets', 'group_specs', True, 'TypeError', 'group_specs must not be None'),
    ('list_consumer_group_offsets', 'options', True, 'TypeError', 'options must not be None'),
    ('list_consumer_group_offsets', 'group_specs', False, 'TypeError', 'group_specs must not contain None'),
    ('list_consumer_group_offsets#id', 'group_id', True, 'TypeError', 'group_id must not be None'),
    ('list_consumer_group_offsets#id', 'options', True, 'TypeError', 'options must not be None'),
    ('delete_consumer_groups', 'group_ids', True, 'TypeError', 'group_ids must not be None'),
    ('delete_consumer_groups', 'options', True, 'TypeError', 'options must not be None'),
    ('delete_consumer_group_offsets', 'group_id', True, 'TypeError', 'group_id must not be None'),
    ('delete_consumer_group_offsets', 'partitions', True, 'TypeError', 'partitions must not be None'),
    ('delete_consumer_group_offsets', 'options', True, 'TypeError', 'options must not be None'),
    ('delete_consumer_group_offsets', 'partitions', False, 'TypeError', 'partitions must not contain None'),
    ('list_groups', 'options', True, 'TypeError', 'options must not be None'),
    ('elect_leaders', 'election_type', True, 'TypeError', 'election_type must not be None'),
    ('elect_leaders', 'partitions', True, 'OK', None),
    ('elect_leaders', 'options', True, 'TypeError', 'options must not be None'),
    ('elect_leaders', 'partitions', False, 'TypeError', 'partitions must not contain None'),
    ('alter_partition_reassignments', 'reassignments', True, 'TypeError', 'reassignments must not be None'),
    ('alter_partition_reassignments', 'options', True, 'TypeError', 'options must not be None'),
    ('alter_partition_reassignments', 'reassignments', False, 'TypeError', 'reassignments must not contain a None key'),
    ('list_partition_reassignments', 'partitions', True, 'TypeError', 'partitions must not be None'),
    ('list_partition_reassignments', 'options', True, 'TypeError', 'options must not be None'),
    ('list_partition_reassignments', 'partitions', False, 'TypeError', 'partitions must not contain None'),
    ('remove_members_from_consumer_group', 'group_id', True, 'TypeError', 'group_id must not be None'),
    ('remove_members_from_consumer_group', 'options', True, 'TypeError', 'options must not be None'),
    ('alter_consumer_group_offsets', 'group_id', True, 'TypeError', 'group_id must not be None'),
    ('alter_consumer_group_offsets', 'offsets', True, 'TypeError', 'offsets must not be None'),
    ('alter_consumer_group_offsets', 'options', True, 'TypeError', 'options must not be None'),
    ('alter_consumer_group_offsets', 'offsets', False, 'TypeError', 'offsets must not contain None'),
    ('list_offsets', 'topic_partition_offsets', True, 'TypeError', 'topic_partition_offsets must not be None'),
    ('list_offsets', 'options', True, 'TypeError', 'options must not be None'),
    ('list_offsets', 'topic_partition_offsets', False, 'OK', None),
    ('describe_client_quotas', 'filter', True, 'TypeError', 'filter must not be None'),
    ('describe_client_quotas', 'options', True, 'TypeError', 'options must not be None'),
    ('alter_client_quotas', 'entries', True, 'TypeError', 'entries must not be None'),
    ('alter_client_quotas', 'options', True, 'TypeError', 'options must not be None'),
    ('alter_client_quotas', 'entries', False, 'TypeError', 'entries must not contain None'),
    ('describe_user_scram_credentials', 'users', True, 'OK', None),
    ('describe_user_scram_credentials', 'options', True, 'TypeError', 'options must not be None'),
    ('alter_user_scram_credentials', 'alterations', True, 'TypeError', 'alterations must not be None'),
    ('alter_user_scram_credentials', 'options', True, 'TypeError', 'options must not be None'),
    ('alter_user_scram_credentials', 'alterations', False, 'TypeError', 'alterations must not contain None'),
    ('describe_features', 'options', True, 'TypeError', 'options must not be None'),
    ('update_features', 'feature_updates', True, 'TypeError', 'feature_updates must not be None'),
    ('update_features', 'options', True, 'TypeError', 'options must not be None'),
    ('update_features', 'feature_updates', False, 'TypeError', 'feature_updates must not contain None'),
    ('unregister_controller', 'controller_id', True, 'TypeError', 'controller_id must not be None'),
    ('unregister_controller', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_producers', 'partitions', True, 'TypeError', 'partitions must not be None'),
    ('describe_producers', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_producers', 'partitions', False, 'TypeError', 'partitions must not contain None'),
    ('describe_transactions', 'transactional_ids', True, 'TypeError', 'transactional_ids must not be None'),
    ('describe_transactions', 'options', True, 'TypeError', 'options must not be None'),
    ('abort_transaction', 'spec', True, 'TypeError', 'spec must not be None'),
    ('abort_transaction', 'options', True, 'TypeError', 'options must not be None'),
    ('list_transactions', 'options', True, 'TypeError', 'options must not be None'),
    ('fence_producers', 'transactional_ids', True, 'TypeError', 'transactional_ids must not be None'),
    ('fence_producers', 'options', True, 'TypeError', 'options must not be None'),
    ('list_config_resources', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_classic_groups', 'group_ids', True, 'TypeError', 'group_ids must not be None'),
    ('describe_classic_groups', 'options', True, 'TypeError', 'options must not be None'),
    ('force_terminate_transaction', 'transactional_id', True, 'TypeError', 'transactional_id must not be None'),
    ('force_terminate_transaction', 'options', True, 'TypeError', 'options must not be None'),
]

MOCK_ROWS: list[Row] = [
    ('create_topics', 'new_topics', True, 'TypeError', 'new_topics must not be None'),
    ('create_topics', 'options', True, 'OK', None),
    ('create_topics', 'new_topics', False, 'TypeError', 'new_topics must not contain None'),
    ('delete_topics', 'topics', True, 'IllegalArgumentError', 'delete_topics(): topics=None matches more than one overload: (topics: Iterable[str]), (topics: TopicCollection)'),
    ('delete_topics', 'options', True, 'OK', None),
    ('delete_topics', 'topics', False, 'OK', None),
    ('list_topics', 'options', True, 'OK', None),
    ('describe_topics', 'topic_names', True, 'TypeError', 'topic_names must not be None'),
    ('describe_topics', 'options', True, 'OK', None),
    ('describe_topics', 'topic_names', False, 'OK', None),
    ('describe_cluster', 'options', True, 'OK', None),
    ('describe_acls', 'filter', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_acls', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('create_acls', 'acls', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('create_acls', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('create_acls', 'acls', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_acls', 'filters', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_acls', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_acls', 'filters', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_configs', 'resources', True, 'TypeError', 'resources must not be None'),
    ('describe_configs', 'options', True, 'OK', None),
    ('describe_configs', 'resources', False, 'OK', None),
    ('incremental_alter_configs', 'configs', True, 'TypeError', 'configs must not be None'),
    ('incremental_alter_configs', 'options', True, 'OK', None),
    ('incremental_alter_configs', 'configs', False, 'OK', None),
    ('alter_replica_log_dirs', 'replica_assignment', True, 'TypeError', 'replica_assignment must not be None'),
    ('alter_replica_log_dirs', 'options', True, 'OK', None),
    ('alter_replica_log_dirs', 'replica_assignment', False, 'TypeError', 'replica_assignment must not contain None'),
    ('describe_log_dirs', 'brokers', True, 'TypeError', 'brokers must not be None'),
    ('describe_log_dirs', 'options', True, 'OK', None),
    ('describe_log_dirs', 'brokers', False, 'OK', None),
    ('describe_replica_log_dirs', 'replicas', True, 'TypeError', 'replicas must not be None'),
    ('describe_replica_log_dirs', 'options', True, 'OK', None),
    ('describe_replica_log_dirs', 'replicas', False, 'TypeError', 'replicas must not contain None'),
    ('create_partitions', 'new_partitions', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('create_partitions', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('create_partitions', 'new_partitions', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_records', 'records_to_delete', True, 'TypeError', 'records_to_delete must not be None'),
    ('delete_records', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_records', 'records_to_delete', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('create_delegation_token', 'options', True, 'TypeError', 'options must not be None'),
    ('renew_delegation_token', 'hmac', True, 'OK', None),
    ('renew_delegation_token', 'options', True, 'TypeError', 'options must not be None'),
    ('expire_delegation_token', 'hmac', True, 'OK', None),
    ('expire_delegation_token', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_delegation_token', 'options', True, 'TypeError', 'options must not be None'),
    ('describe_consumer_groups', 'group_ids', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_consumer_groups', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_consumer_groups', 'group_ids', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('list_consumer_group_offsets', 'group_specs', True, 'TypeError', 'group_specs must not be None'),
    ('list_consumer_group_offsets', 'options', True, 'OK', None),
    ('list_consumer_group_offsets', 'group_specs', False, 'TypeError', 'group_specs must not contain None'),
    ('list_consumer_group_offsets#id', 'group_id', True, 'OK', None),
    ('list_consumer_group_offsets#id', 'options', True, 'OK', None),
    ('delete_consumer_groups', 'group_ids', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_consumer_groups', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_consumer_groups', 'group_ids', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_consumer_group_offsets', 'group_id', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_consumer_group_offsets', 'partitions', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_consumer_group_offsets', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('delete_consumer_group_offsets', 'partitions', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('list_groups', 'options', True, 'OK', None),
    ('elect_leaders', 'election_type', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('elect_leaders', 'partitions', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('elect_leaders', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('elect_leaders', 'partitions', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('alter_partition_reassignments', 'reassignments', True, 'TypeError', 'reassignments must not be None'),
    ('alter_partition_reassignments', 'options', True, 'OK', None),
    ('alter_partition_reassignments', 'reassignments', False, 'TypeError', 'reassignments must not contain a None key'),
    ('list_partition_reassignments', 'partitions', True, 'TypeError', 'partitions must not be None'),
    ('list_partition_reassignments', 'options', True, 'OK', None),
    ('list_partition_reassignments', 'partitions', False, 'OK', None),
    ('remove_members_from_consumer_group', 'group_id', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('remove_members_from_consumer_group', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('alter_consumer_group_offsets', 'group_id', True, 'UnsupportedVersionError', 'Not implement yet'),
    ('alter_consumer_group_offsets', 'offsets', True, 'UnsupportedVersionError', 'Not implement yet'),
    ('alter_consumer_group_offsets', 'options', True, 'UnsupportedVersionError', 'Not implement yet'),
    ('alter_consumer_group_offsets', 'offsets', False, 'UnsupportedVersionError', 'Not implement yet'),
    ('list_offsets', 'topic_partition_offsets', True, 'TypeError', 'topic_partition_offsets must not be None'),
    ('list_offsets', 'options', True, 'IllegalStateError', 'MockAdminClient has no end offset for t-0'),
    ('list_offsets', 'topic_partition_offsets', False, 'IllegalStateError', 'MockAdminClient has no end offset for t-0'),
    ('describe_client_quotas', 'filter', True, 'UnsupportedVersionError', 'Not implement yet'),
    ('describe_client_quotas', 'options', True, 'UnsupportedVersionError', 'Not implement yet'),
    ('alter_client_quotas', 'entries', True, 'UnsupportedVersionError', 'Not implement yet'),
    ('alter_client_quotas', 'options', True, 'UnsupportedVersionError', 'Not implement yet'),
    ('alter_client_quotas', 'entries', False, 'UnsupportedVersionError', 'Not implement yet'),
    ('describe_user_scram_credentials', 'users', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_user_scram_credentials', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_user_scram_credentials', 'users', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('alter_user_scram_credentials', 'alterations', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('alter_user_scram_credentials', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('alter_user_scram_credentials', 'alterations', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_features', 'options', True, 'OK', None),
    ('update_features', 'feature_updates', True, 'TypeError', 'feature_updates must not be None'),
    ('update_features', 'options', True, 'OK', None),
    ('update_features', 'feature_updates', False, 'TypeError', 'feature_updates must not contain None'),
    ('unregister_controller', 'controller_id', True, 'TypeError', 'controller_id must not be None'),
    ('unregister_controller', 'options', True, 'OK', None),
    ('describe_producers', 'partitions', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_producers', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_producers', 'partitions', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_transactions', 'transactional_ids', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_transactions', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_transactions', 'transactional_ids', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('abort_transaction', 'spec', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('abort_transaction', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('list_transactions', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('fence_producers', 'transactional_ids', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('fence_producers', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('fence_producers', 'transactional_ids', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('list_config_resources', 'options', True, 'OK', None),
    ('describe_classic_groups', 'group_ids', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_classic_groups', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('describe_classic_groups', 'group_ids', False, 'UnsupportedVersionError', 'Not implemented yet'),
    ('force_terminate_transaction', 'transactional_id', True, 'UnsupportedVersionError', 'Not implemented yet'),
    ('force_terminate_transaction', 'options', True, 'UnsupportedVersionError', 'Not implemented yet'),
]


def _call(client: Any, row: Row) -> tuple[str, str | None]:
    key, param, whole, _, _ = row
    kwargs, elements = RPCS[key]
    call = dict(kwargs)
    call[param] = None if whole else elements[param]
    try:
        getattr(client, key.split("#")[0])(**call)
    except Exception as e:  # noqa: BLE001 - the row names the class
        return type(e).__name__, str(e)
    return "OK", None


def _ids(rows: list[Row]) -> list[str]:
    return [f"{r[0]}-{r[1]}-{'none' if r[2] else 'element'}" for r in rows]


@pytest.fixture(scope="module")
def admin() -> Any:
    client = Admin.create(configs=UNREACHABLE)
    yield client
    client.close(timeout=0)


@pytest.mark.parametrize("row", ADMIN_ROWS, ids=_ids(ADMIN_ROWS))
def test_admin_none_argument(admin: Admin, row: Row) -> None:
    assert _call(admin, row) == (row[3], row[4])


@pytest.mark.parametrize("row", ADMIN_ROWS, ids=_ids(ADMIN_ROWS))
async def test_async_admin_none_argument(row: Row) -> None:
    client = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        assert _call(client, row) == (row[3], row[4])
    finally:
        await client.close(timeout=0)


@pytest.mark.parametrize("row", MOCK_ROWS, ids=_ids(MOCK_ROWS))
def test_mock_none_argument(row: Row) -> None:
    assert _call(MockAdminClient(brokers=[NODE], controller=NODE), row) == (row[3], row[4])


@pytest.mark.parametrize("row", MOCK_ROWS, ids=_ids(MOCK_ROWS))
async def test_async_mock_none_argument(row: Row) -> None:
    client = AsyncMockAdminClient(brokers=[NODE], controller=NODE)
    assert _call(client, row) == (row[3], row[4])


# ---- Java's order within a method ------------------------------------------------


def test_java_order_of_the_null_checks(admin: Admin) -> None:
    """``createTopics`` dereferences ``newTopics.size()`` before
    ``options.timeoutMs()``; ``renewDelegationToken`` and ``describeClientQuotas``
    read ``options`` first and the other argument only in the request; ``describeAcls``
    reads ``filter.isUnknown()`` first."""
    cases: list[tuple[Any, str]] = [
        (lambda: admin.create_topics(new_topics=None, options=None), "new_topics"),
        (lambda: admin.renew_delegation_token(hmac=None, options=None), "options"),
        (lambda: admin.describe_client_quotas(filter=None, options=None), "options"),
        (lambda: admin.describe_acls(filter=None, options=None), "filter"),
        (lambda: admin.remove_members_from_consumer_group(group_id=None, options=None),
         "options"),
        (lambda: admin.delete_consumer_group_offsets(group_id=None, partitions=None,
                                                     options=None), "options"),
    ]
    for call, name in cases:
        with pytest.raises(TypeError) as info:
            call()
        assert str(info.value) == f"{name} must not be None"


def test_describe_topics_with_a_none_topic_collection(admin: Admin) -> None:
    """Java's ``describeTopics((TopicCollection) null, options)``: neither subclass."""
    with pytest.raises(IllegalArgumentError) as info:
        admin.describe_topics(topics=None)  # type: ignore[call-overload]
    assert str(info.value) == ("The TopicCollection: None provided did not match any "
                               "supported classes for describeTopics.")
    with pytest.raises(IllegalArgumentError) as info:
        MockAdminClient().describe_topics(topics=None)  # type: ignore[call-overload]
    assert str(info.value) == ("The TopicCollection provided did not match any "
                               "supported classes for describeTopics.")


def test_closed_check_precedes_the_null_checks_and_follows_the_shape_checks() -> None:
    """The Order paragraph: on a closed client the closed state is reported before a
    ``None`` argument, the shape checks (``java_forms``, the ambiguous ``None``) before
    it."""
    client = Admin.create(configs=UNREACHABLE)
    client.close(timeout=0)
    with pytest.raises(IllegalStateError) as info:
        client.create_topics(new_topics=None)  # type: ignore[arg-type]
    assert str(info.value) == CLOSED
    with pytest.raises(IllegalStateError):
        client.list_topics(options=None)
    with pytest.raises(IllegalArgumentError, match="matches more than one overload"):
        client.delete_topics(topics=None)  # type: ignore[call-overload]


def test_close_timeout_none_is_a_null_duration() -> None:
    """Java's ``close(Duration)`` dereferences ``timeout.toMillis()``; a second
    ``close()`` returns before reading it *(deviation)*; the mocks' ``close`` is an
    empty body, as Java's."""
    client = Admin.create(configs=UNREACHABLE)
    with pytest.raises(TypeError) as info:
        client.close(timeout=None)
    assert str(info.value) == "timeout must not be None"
    client.close(timeout=0)
    client.close(timeout=None)
    MockAdminClient().close(timeout=None)


async def test_async_close_timeout_none_is_a_null_duration() -> None:
    client = AsyncAdmin.create(configs=UNREACHABLE)
    with pytest.raises(TypeError) as info:
        await client.close(timeout=None)
    assert str(info.value) == "timeout must not be None"
    await client.close(timeout=0)
    await AsyncMockAdminClient().close(timeout=None)


# ---- MISSING ------------------------------------------------------------------------


def test_signatures_show_missing() -> None:
    for cls in (Admin, AsyncAdmin, MockAdminClient, AsyncMockAdminClient):
        assert inspect.signature(cls.create_topics).parameters["options"].default is MISSING
        assert inspect.signature(cls.close).parameters["timeout"].default is MISSING
    assert "options: 'CreateTopicsOptions | None' = MISSING" in str(
        inspect.signature(Admin.create_topics))


def test_missing_passed_explicitly_is_left_out(admin: Admin) -> None:
    """``MISSING`` passed explicitly is the parameter left out (Signatures), on each
    class."""
    assert type(admin.list_topics(options=MISSING)) is type(admin.list_topics())
    mock = MockAdminClient(brokers=[NODE], controller=NODE)
    mock.create_topics(new_topics=[NewTopic(name="t", num_partitions=1,
                                            replication_factor=1)], options=MISSING)
    assert mock.list_topics(options=MISSING).names().get() == {"t"}
    with pytest.raises(TypeError, match="^hmac must not be None$"):
        admin.renew_delegation_token(hmac=None, options=MISSING)
    # The mock reads options.renewTimePeriodMs() of a new RenewDelegationTokenOptions.
    assert type(mock.renew_delegation_token(hmac=b"x", options=MISSING)) is type(
        mock.renew_delegation_token(hmac=b"x", options=RenewDelegationTokenOptions()))


async def test_async_missing_passed_explicitly_is_left_out() -> None:
    client = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        assert client.list_topics(options=MISSING) is not None
    finally:
        await client.close(timeout=0)
    mock = AsyncMockAdminClient(brokers=[NODE], controller=NODE)
    await mock.create_topics(
        new_topics=[NewTopic(name="t", num_partitions=1, replication_factor=1)],
        options=MISSING).all().get()
    assert await mock.list_topics(options=MISSING).names().get() == {"t"}


# ---- the mocks, where Java's mock behaves otherwise --------------------------------


def test_mock_delete_records_none() -> None:
    """Java's mock dereferences ``recordsToDelete.isEmpty()``; an empty map returns an
    empty result."""
    mock = MockAdminClient()
    with pytest.raises(TypeError) as info:
        mock.delete_records(records_to_delete=None)  # type: ignore[arg-type]
    assert str(info.value) == "records_to_delete must not be None"
    assert mock.delete_records(records_to_delete={}).low_watermarks() == {}


def test_mock_list_consumer_group_offsets_none_group_id() -> None:
    """Java's ``listConsumerGroupOffsets(null)`` builds ``singletonMap(null, spec)``,
    which the mock answers."""
    result = MockAdminClient().list_consumer_group_offsets(group_id=None)  # type: ignore[call-overload]
    assert result.partitions_to_offset_and_metadata(group_id=None).get() == {}


def test_mock_update_features_none_options_read_only_when_valid() -> None:
    """Java's mock reads ``options.validateOnly()`` only once every update is valid."""
    mock = MockAdminClient.create().feature_levels(feature_levels={"f": 1}) \
        .min_supported_feature_levels(min_supported_feature_levels={"f": 0}) \
        .max_supported_feature_levels(max_supported_feature_levels={"f": 3}).build()
    upgrade = FeatureUpdate(max_version_level=2, upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)
    with pytest.raises(TypeError) as info:
        mock.update_features(feature_updates={"f": upgrade}, options=None)
    assert str(info.value) == "options must not be None"
    too_high = FeatureUpdate(max_version_level=9,
                             upgrade_type=FeatureUpdate.UpgradeType.UPGRADE)
    result = mock.update_features(feature_updates={"f": too_high}, options=None)
    assert result.values()["f"].is_completed_exceptionally()


def test_mock_constructor_none_brokers() -> None:
    """Java's ``MockAdminClient(brokers, controller)`` reads ``brokers.size()``."""
    with pytest.raises(TypeError) as info:
        MockAdminClient(brokers=None, controller=NODE)  # type: ignore[arg-type]
    assert str(info.value) == "brokers must not be None"
    with pytest.raises(TypeError) as info:
        MockAdminClient.create().brokers(brokers=None)  # type: ignore[arg-type]
    assert str(info.value) == "brokers must not be None"


# ---- the hand-written value classes -------------------------------------------------


def test_value_classes_none_is_java_null() -> None:
    from confluent_kafka.admin import ScramCredentialInfo, UserScramCredentialUpsertion

    # NewTopic(name, null): Collections.unmodifiableMap(null).
    with pytest.raises(TypeError) as info:
        NewTopic(name="t", replicas_assignments=None)  # type: ignore[call-overload]
    assert str(info.value) == "replicas_assignments must not be None"
    # NewPartitions.increaseTo(n, null) is increaseTo(n).
    assert str(NewPartitions.increase_to(total_count=3, new_assignments=None)) \
        == str(NewPartitions.increase_to(total_count=3)) == "(totalCount=3, newAssignments=None)"
    assert NewPartitions.increase_to(total_count=3, new_assignments=MISSING).assignments() is None
    credential = ScramCredentialInfo(mechanism=ScramMechanism.SCRAM_SHA_256, iterations=4096)
    # requireNonNull(salt), after user, credentialInfo and password.
    with pytest.raises(TypeError) as info:
        UserScramCredentialUpsertion(user="u", credential_info=credential, password=b"p",
                                     salt=None)  # type: ignore[arg-type]
    assert str(info.value) == "salt must not be None"
    with pytest.raises(TypeError) as info:
        UserScramCredentialUpsertion(user=None, credential_info=credential,  # type: ignore[arg-type]
                                     password=b"p", salt=None)  # type: ignore[arg-type]
    assert str(info.value) == "user must not be None"
    # (user, info, null) matches both the String and the byte[] constructor.
    with pytest.raises(IllegalArgumentError) as info2:
        UserScramCredentialUpsertion(user="u", credential_info=credential,
                                     password=None)  # type: ignore[arg-type]
    assert str(info2.value) == ("UserScramCredentialUpsertion(): password=None matches more "
                                "than one overload: (password: str), (password: bytes)")
    assert len(UserScramCredentialUpsertion(user="u", credential_info=credential,
                                            password="p", salt=MISSING).salt()) > 0
    spec = ListConsumerGroupOffsetsSpec(topic_partitions=MISSING)
    assert spec == ListConsumerGroupOffsetsSpec() and spec.topic_partitions() is None


def test_mock_add_topic_uses_topic_id_left_out() -> None:
    from confluent_kafka.common import TopicPartitionInfo, Uuid

    mock = MockAdminClient(brokers=[NODE], controller=NODE)
    info = [TopicPartitionInfo(partition=0, leader=NODE, replicas=[NODE], isr=[NODE])]
    mock.add_topic(internal=False, name="a", partitions=info, configs={})
    mock.add_topic(internal=False, name="b", partitions=info, configs={}, uses_topic_id=False)
    ids = {n: listing.topic_id()
           for n, listing in mock.list_topics().names_to_listings().get().items()}
    assert ids["a"] != Uuid.ZERO_UUID and ids["b"] == Uuid.ZERO_UUID
