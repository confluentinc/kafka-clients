// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Options classes for admin operations.
//!
//! Corresponds to the `*Options` classes in `org.apache.kafka.clients.admin`.
//! Each extends the Java `AbstractOptions` base (a `timeout_ms` field); in Rust
//! each options struct simply carries its own optional timeout plus operation
//! specific fields.

pub mod alter_client_quotas_options;
pub mod alter_configs_options;
pub mod alter_consumer_group_offsets_options;
pub mod alter_partition_reassignments_options;
pub mod alter_replica_log_dirs_options;
pub mod create_acls_options;
pub mod create_delegation_token_options;
pub mod create_partitions_options;
pub mod create_topics_options;
pub mod delete_acls_options;
pub mod delete_consumer_group_offsets_options;
pub mod delete_consumer_groups_options;
pub mod delete_records_options;
pub mod delete_topics_options;
pub mod describe_acls_options;
pub mod describe_classic_groups_options;
pub mod describe_client_quotas_options;
pub mod describe_cluster_options;
pub mod describe_configs_options;
pub mod describe_consumer_groups_options;
pub mod describe_delegation_token_options;
pub mod describe_log_dirs_options;
pub mod describe_replica_log_dirs_options;
pub mod describe_topics_options;
pub mod elect_leaders_options;
pub mod expire_delegation_token_options;
pub mod list_config_resources_options;
pub mod list_consumer_group_offsets_options;
pub mod list_consumer_groups_options;
pub mod list_groups_options;
pub mod list_offsets_options;
pub mod list_partition_reassignments_options;
pub mod list_topics_options;
pub mod remove_members_from_consumer_group_options;
pub mod renew_delegation_token_options;

pub use alter_client_quotas_options::AlterClientQuotasOptions;
pub use alter_configs_options::AlterConfigsOptions;
pub use alter_consumer_group_offsets_options::AlterConsumerGroupOffsetsOptions;
pub use alter_partition_reassignments_options::AlterPartitionReassignmentsOptions;
pub use alter_replica_log_dirs_options::AlterReplicaLogDirsOptions;
pub use create_acls_options::CreateAclsOptions;
pub use create_delegation_token_options::CreateDelegationTokenOptions;
pub use create_partitions_options::CreatePartitionsOptions;
pub use create_topics_options::CreateTopicsOptions;
pub use delete_acls_options::DeleteAclsOptions;
pub use delete_consumer_group_offsets_options::DeleteConsumerGroupOffsetsOptions;
pub use delete_consumer_groups_options::DeleteConsumerGroupsOptions;
pub use delete_records_options::DeleteRecordsOptions;
pub use delete_topics_options::DeleteTopicsOptions;
pub use describe_acls_options::DescribeAclsOptions;
pub use describe_classic_groups_options::DescribeClassicGroupsOptions;
pub use describe_client_quotas_options::DescribeClientQuotasOptions;
pub use describe_cluster_options::DescribeClusterOptions;
pub use describe_configs_options::DescribeConfigsOptions;
pub use describe_consumer_groups_options::DescribeConsumerGroupsOptions;
pub use describe_delegation_token_options::DescribeDelegationTokenOptions;
pub use describe_log_dirs_options::DescribeLogDirsOptions;
pub use describe_replica_log_dirs_options::DescribeReplicaLogDirsOptions;
pub use describe_topics_options::DescribeTopicsOptions;
pub use elect_leaders_options::ElectLeadersOptions;
pub use expire_delegation_token_options::ExpireDelegationTokenOptions;
pub use list_config_resources_options::ListConfigResourcesOptions;
pub use list_consumer_group_offsets_options::ListConsumerGroupOffsetsOptions;
#[allow(deprecated)]
pub use list_consumer_groups_options::ListConsumerGroupsOptions;
pub use list_groups_options::ListGroupsOptions;
pub use list_offsets_options::ListOffsetsOptions;
pub use list_partition_reassignments_options::ListPartitionReassignmentsOptions;
pub use list_topics_options::ListTopicsOptions;
pub use remove_members_from_consumer_group_options::RemoveMembersFromConsumerGroupOptions;
pub use renew_delegation_token_options::RenewDelegationTokenOptions;
