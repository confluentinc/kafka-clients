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

"""``confluent_kafka.admin``: Java's ``org.apache.kafka.clients.admin``.

The admin client, its options and results, and the value classes and enums its
calls take and return (CLAUDE.md, Python Binding Conventions, Modules). Users
import from this module, never from the files.

Imports are grouped by kind, one section each, so that each kind can be
regenerated on its own: value classes and enums, Options, Results, clients.
"""

from __future__ import annotations

__all__: list[str] = []

# BEGIN VALUE CLASSES AND ENUMS
from .abort_transaction_spec import AbortTransactionSpec as AbortTransactionSpec
from .alter_config_op import AlterConfigOp as AlterConfigOp
from .classic_group_description import ClassicGroupDescription as ClassicGroupDescription
from .config import Config as Config
from .config_entry import ConfigEntry as ConfigEntry
from .consumer_group_description import ConsumerGroupDescription as ConsumerGroupDescription
from .deleted_records import DeletedRecords as DeletedRecords
from .feature_metadata import FeatureMetadata as FeatureMetadata
from .feature_update import FeatureUpdate as FeatureUpdate
from .finalized_version_range import FinalizedVersionRange as FinalizedVersionRange
from .group_listing import GroupListing as GroupListing
from .list_consumer_group_offsets_spec import (
    ListConsumerGroupOffsetsSpec as ListConsumerGroupOffsetsSpec,
)
from .log_dir_description import LogDirDescription as LogDirDescription
from .member_assignment import MemberAssignment as MemberAssignment
from .member_description import MemberDescription as MemberDescription
from .member_to_remove import MemberToRemove as MemberToRemove
from .new_partition_reassignment import NewPartitionReassignment as NewPartitionReassignment
from .new_partitions import NewPartitions as NewPartitions
from .new_topic import NewTopic as NewTopic
from .offset_spec import OffsetSpec as OffsetSpec
from .partition_reassignment import PartitionReassignment as PartitionReassignment
from .producer_state import ProducerState as ProducerState
from .records_to_delete import RecordsToDelete as RecordsToDelete
from .replica_info import ReplicaInfo as ReplicaInfo
from .scram_credential_info import ScramCredentialInfo as ScramCredentialInfo
from .scram_mechanism import ScramMechanism as ScramMechanism
from .supported_version_range import SupportedVersionRange as SupportedVersionRange
from .topic_description import TopicDescription as TopicDescription
from .topic_listing import TopicListing as TopicListing
from .transaction_description import TransactionDescription as TransactionDescription
from .transaction_listing import TransactionListing as TransactionListing
from .transaction_state import TransactionState as TransactionState
from .user_scram_credential_alteration import (
    UserScramCredentialAlteration as UserScramCredentialAlteration,
)
from .user_scram_credential_deletion import (
    UserScramCredentialDeletion as UserScramCredentialDeletion,
)
from .user_scram_credential_upsertion import (
    UserScramCredentialUpsertion as UserScramCredentialUpsertion,
)
from .user_scram_credentials_description import (
    UserScramCredentialsDescription as UserScramCredentialsDescription,
)

__all__ += [
    "AbortTransactionSpec",
    "AlterConfigOp",
    "ClassicGroupDescription",
    "Config",
    "ConfigEntry",
    "ConsumerGroupDescription",
    "DeletedRecords",
    "FeatureMetadata",
    "FeatureUpdate",
    "FinalizedVersionRange",
    "GroupListing",
    "ListConsumerGroupOffsetsSpec",
    "LogDirDescription",
    "MemberAssignment",
    "MemberDescription",
    "MemberToRemove",
    "NewPartitionReassignment",
    "NewPartitions",
    "NewTopic",
    "OffsetSpec",
    "PartitionReassignment",
    "ProducerState",
    "RecordsToDelete",
    "ReplicaInfo",
    "ScramCredentialInfo",
    "ScramMechanism",
    "SupportedVersionRange",
    "TopicDescription",
    "TopicListing",
    "TransactionDescription",
    "TransactionListing",
    "TransactionState",
    "UserScramCredentialAlteration",
    "UserScramCredentialDeletion",
    "UserScramCredentialUpsertion",
    "UserScramCredentialsDescription",
]
# END VALUE CLASSES AND ENUMS

# BEGIN OPTIONS
from .abort_transaction_options import AbortTransactionOptions as AbortTransactionOptions
from .abstract_options import AbstractOptions as AbstractOptions
from .alter_client_quotas_options import AlterClientQuotasOptions as AlterClientQuotasOptions
from .alter_configs_options import AlterConfigsOptions as AlterConfigsOptions
from .alter_consumer_group_offsets_options import (
    AlterConsumerGroupOffsetsOptions as AlterConsumerGroupOffsetsOptions,
)
from .alter_partition_reassignments_options import (
    AlterPartitionReassignmentsOptions as AlterPartitionReassignmentsOptions,
)
from .alter_replica_log_dirs_options import AlterReplicaLogDirsOptions as AlterReplicaLogDirsOptions
from .alter_user_scram_credentials_options import (
    AlterUserScramCredentialsOptions as AlterUserScramCredentialsOptions,
)
from .create_acls_options import CreateAclsOptions as CreateAclsOptions
from .create_delegation_token_options import (
    CreateDelegationTokenOptions as CreateDelegationTokenOptions,
)
from .create_partitions_options import CreatePartitionsOptions as CreatePartitionsOptions
from .create_topics_options import CreateTopicsOptions as CreateTopicsOptions
from .delete_acls_options import DeleteAclsOptions as DeleteAclsOptions
from .delete_consumer_group_offsets_options import (
    DeleteConsumerGroupOffsetsOptions as DeleteConsumerGroupOffsetsOptions,
)
from .delete_consumer_groups_options import (
    DeleteConsumerGroupsOptions as DeleteConsumerGroupsOptions,
)
from .delete_records_options import DeleteRecordsOptions as DeleteRecordsOptions
from .delete_topics_options import DeleteTopicsOptions as DeleteTopicsOptions
from .describe_acls_options import DescribeAclsOptions as DescribeAclsOptions
from .describe_classic_groups_options import (
    DescribeClassicGroupsOptions as DescribeClassicGroupsOptions,
)
from .describe_client_quotas_options import (
    DescribeClientQuotasOptions as DescribeClientQuotasOptions,
)
from .describe_cluster_options import DescribeClusterOptions as DescribeClusterOptions
from .describe_configs_options import DescribeConfigsOptions as DescribeConfigsOptions
from .describe_consumer_groups_options import (
    DescribeConsumerGroupsOptions as DescribeConsumerGroupsOptions,
)
from .describe_delegation_token_options import (
    DescribeDelegationTokenOptions as DescribeDelegationTokenOptions,
)
from .describe_features_options import DescribeFeaturesOptions as DescribeFeaturesOptions
from .describe_log_dirs_options import DescribeLogDirsOptions as DescribeLogDirsOptions
from .describe_producers_options import DescribeProducersOptions as DescribeProducersOptions
from .describe_replica_log_dirs_options import (
    DescribeReplicaLogDirsOptions as DescribeReplicaLogDirsOptions,
)
from .describe_topics_options import DescribeTopicsOptions as DescribeTopicsOptions
from .describe_transactions_options import (
    DescribeTransactionsOptions as DescribeTransactionsOptions,
)
from .describe_user_scram_credentials_options import (
    DescribeUserScramCredentialsOptions as DescribeUserScramCredentialsOptions,
)
from .elect_leaders_options import ElectLeadersOptions as ElectLeadersOptions
from .expire_delegation_token_options import (
    ExpireDelegationTokenOptions as ExpireDelegationTokenOptions,
)
from .fence_producers_options import FenceProducersOptions as FenceProducersOptions
from .list_config_resources_options import ListConfigResourcesOptions as ListConfigResourcesOptions
from .list_consumer_group_offsets_options import (
    ListConsumerGroupOffsetsOptions as ListConsumerGroupOffsetsOptions,
)
from .list_groups_options import ListGroupsOptions as ListGroupsOptions
from .list_offsets_options import ListOffsetsOptions as ListOffsetsOptions
from .list_partition_reassignments_options import (
    ListPartitionReassignmentsOptions as ListPartitionReassignmentsOptions,
)
from .list_topics_options import ListTopicsOptions as ListTopicsOptions
from .list_transactions_options import ListTransactionsOptions as ListTransactionsOptions
from .remove_members_from_consumer_group_options import (
    RemoveMembersFromConsumerGroupOptions as RemoveMembersFromConsumerGroupOptions,
)
from .renew_delegation_token_options import (
    RenewDelegationTokenOptions as RenewDelegationTokenOptions,
)
from .terminate_transaction_options import (
    TerminateTransactionOptions as TerminateTransactionOptions,
)
from .update_features_options import UpdateFeaturesOptions as UpdateFeaturesOptions

__all__ += [
    "AbortTransactionOptions",
    "AbstractOptions",
    "AlterClientQuotasOptions",
    "AlterConfigsOptions",
    "AlterConsumerGroupOffsetsOptions",
    "AlterPartitionReassignmentsOptions",
    "AlterReplicaLogDirsOptions",
    "AlterUserScramCredentialsOptions",
    "CreateAclsOptions",
    "CreateDelegationTokenOptions",
    "CreatePartitionsOptions",
    "CreateTopicsOptions",
    "DeleteAclsOptions",
    "DeleteConsumerGroupOffsetsOptions",
    "DeleteConsumerGroupsOptions",
    "DeleteRecordsOptions",
    "DeleteTopicsOptions",
    "DescribeAclsOptions",
    "DescribeClassicGroupsOptions",
    "DescribeClientQuotasOptions",
    "DescribeClusterOptions",
    "DescribeConfigsOptions",
    "DescribeConsumerGroupsOptions",
    "DescribeDelegationTokenOptions",
    "DescribeFeaturesOptions",
    "DescribeLogDirsOptions",
    "DescribeProducersOptions",
    "DescribeReplicaLogDirsOptions",
    "DescribeTopicsOptions",
    "DescribeTransactionsOptions",
    "DescribeUserScramCredentialsOptions",
    "ElectLeadersOptions",
    "ExpireDelegationTokenOptions",
    "FenceProducersOptions",
    "ListConfigResourcesOptions",
    "ListConsumerGroupOffsetsOptions",
    "ListGroupsOptions",
    "ListOffsetsOptions",
    "ListPartitionReassignmentsOptions",
    "ListTopicsOptions",
    "ListTransactionsOptions",
    "RemoveMembersFromConsumerGroupOptions",
    "RenewDelegationTokenOptions",
    "TerminateTransactionOptions",
    "UpdateFeaturesOptions",
]
# END OPTIONS

# BEGIN RESULTS
from .abort_transaction_result import AbortTransactionResult as AbortTransactionResult
from .alter_client_quotas_result import AlterClientQuotasResult as AlterClientQuotasResult
from .alter_configs_result import AlterConfigsResult as AlterConfigsResult
from .alter_consumer_group_offsets_result import (
    AlterConsumerGroupOffsetsResult as AlterConsumerGroupOffsetsResult,
)
from .alter_partition_reassignments_result import (
    AlterPartitionReassignmentsResult as AlterPartitionReassignmentsResult,
)
from .alter_replica_log_dirs_result import AlterReplicaLogDirsResult as AlterReplicaLogDirsResult
from .alter_user_scram_credentials_result import (
    AlterUserScramCredentialsResult as AlterUserScramCredentialsResult,
)
from .create_acls_result import CreateAclsResult as CreateAclsResult
from .create_delegation_token_result import (
    CreateDelegationTokenResult as CreateDelegationTokenResult,
)
from .create_partitions_result import CreatePartitionsResult as CreatePartitionsResult
from .create_topics_result import CreateTopicsResult as CreateTopicsResult
from .delete_acls_result import DeleteAclsResult as DeleteAclsResult
from .delete_consumer_group_offsets_result import (
    DeleteConsumerGroupOffsetsResult as DeleteConsumerGroupOffsetsResult,
)
from .delete_consumer_groups_result import DeleteConsumerGroupsResult as DeleteConsumerGroupsResult
from .delete_records_result import DeleteRecordsResult as DeleteRecordsResult
from .delete_topics_result import DeleteTopicsResult as DeleteTopicsResult
from .describe_acls_result import DescribeAclsResult as DescribeAclsResult
from .describe_classic_groups_result import (
    DescribeClassicGroupsResult as DescribeClassicGroupsResult,
)
from .describe_client_quotas_result import DescribeClientQuotasResult as DescribeClientQuotasResult
from .describe_cluster_result import DescribeClusterResult as DescribeClusterResult
from .describe_configs_result import DescribeConfigsResult as DescribeConfigsResult
from .describe_consumer_groups_result import (
    DescribeConsumerGroupsResult as DescribeConsumerGroupsResult,
)
from .describe_delegation_token_result import (
    DescribeDelegationTokenResult as DescribeDelegationTokenResult,
)
from .describe_features_result import DescribeFeaturesResult as DescribeFeaturesResult
from .describe_log_dirs_result import DescribeLogDirsResult as DescribeLogDirsResult
from .describe_producers_result import DescribeProducersResult as DescribeProducersResult
from .describe_replica_log_dirs_result import (
    DescribeReplicaLogDirsResult as DescribeReplicaLogDirsResult,
)
from .describe_topics_result import DescribeTopicsResult as DescribeTopicsResult
from .describe_transactions_result import DescribeTransactionsResult as DescribeTransactionsResult
from .describe_user_scram_credentials_result import (
    DescribeUserScramCredentialsResult as DescribeUserScramCredentialsResult,
)
from .elect_leaders_result import ElectLeadersResult as ElectLeadersResult
from .expire_delegation_token_result import (
    ExpireDelegationTokenResult as ExpireDelegationTokenResult,
)
from .fence_producers_result import FenceProducersResult as FenceProducersResult
from .list_config_resources_result import ListConfigResourcesResult as ListConfigResourcesResult
from .list_consumer_group_offsets_result import (
    ListConsumerGroupOffsetsResult as ListConsumerGroupOffsetsResult,
)
from .list_groups_result import ListGroupsResult as ListGroupsResult
from .list_offsets_result import ListOffsetsResult as ListOffsetsResult
from .list_partition_reassignments_result import (
    ListPartitionReassignmentsResult as ListPartitionReassignmentsResult,
)
from .list_topics_result import ListTopicsResult as ListTopicsResult
from .list_transactions_result import ListTransactionsResult as ListTransactionsResult
from .remove_members_from_consumer_group_result import (
    RemoveMembersFromConsumerGroupResult as RemoveMembersFromConsumerGroupResult,
)
from .renew_delegation_token_result import RenewDelegationTokenResult as RenewDelegationTokenResult
from .terminate_transaction_result import TerminateTransactionResult as TerminateTransactionResult
from .update_features_result import UpdateFeaturesResult as UpdateFeaturesResult

__all__ += [
    "AbortTransactionResult",
    "AlterClientQuotasResult",
    "AlterConfigsResult",
    "AlterConsumerGroupOffsetsResult",
    "AlterPartitionReassignmentsResult",
    "AlterReplicaLogDirsResult",
    "AlterUserScramCredentialsResult",
    "CreateAclsResult",
    "CreateDelegationTokenResult",
    "CreatePartitionsResult",
    "CreateTopicsResult",
    "DeleteAclsResult",
    "DeleteConsumerGroupOffsetsResult",
    "DeleteConsumerGroupsResult",
    "DeleteRecordsResult",
    "DeleteTopicsResult",
    "DescribeAclsResult",
    "DescribeClassicGroupsResult",
    "DescribeClientQuotasResult",
    "DescribeClusterResult",
    "DescribeConfigsResult",
    "DescribeConsumerGroupsResult",
    "DescribeDelegationTokenResult",
    "DescribeFeaturesResult",
    "DescribeLogDirsResult",
    "DescribeProducersResult",
    "DescribeReplicaLogDirsResult",
    "DescribeTopicsResult",
    "DescribeTransactionsResult",
    "DescribeUserScramCredentialsResult",
    "ElectLeadersResult",
    "ExpireDelegationTokenResult",
    "FenceProducersResult",
    "ListConfigResourcesResult",
    "ListConsumerGroupOffsetsResult",
    "ListGroupsResult",
    "ListOffsetsResult",
    "ListPartitionReassignmentsResult",
    "ListTopicsResult",
    "ListTransactionsResult",
    "RemoveMembersFromConsumerGroupResult",
    "RenewDelegationTokenResult",
    "TerminateTransactionResult",
    "UpdateFeaturesResult",
]
# END RESULTS

# BEGIN CLIENTS
from .admin import Admin as Admin
from .async_admin import AsyncAdmin as AsyncAdmin
from .async_kafka_admin_client import AsyncKafkaAdminClient as AsyncKafkaAdminClient
from .kafka_admin_client import KafkaAdminClient as KafkaAdminClient

__all__ += [
    "Admin",
    "AsyncAdmin",
    "AsyncKafkaAdminClient",
    "KafkaAdminClient",
]

from .async_mock_admin_client import AsyncMockAdminClient as AsyncMockAdminClient
from .mock_admin_client import MockAdminClient as MockAdminClient

__all__ += ["AsyncMockAdminClient", "MockAdminClient"]
# END CLIENTS
