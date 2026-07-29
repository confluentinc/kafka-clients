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

//! Kafka admin client.
//!
//! Corresponds to the `org.apache.kafka.clients.admin` package (the `clients`
//! segment is dropped per the project naming conventions). See
//! `.claude/rules/admin-client.md` for the design decisions that govern this
//! module.

pub mod admin_client_config;
pub mod alter_client_quotas_result;
pub mod alter_config_op;
pub mod alter_configs_result;
pub mod alter_consumer_group_offsets_result;
pub mod alter_partition_reassignments_result;
pub mod alter_replica_log_dirs_result;
pub mod classic_group_description;
pub mod config;
pub mod config_entry;
pub mod consumer_group_description;
pub mod consumer_group_listing;
pub mod create_acls_result;
pub mod create_partitions_result;
pub mod create_topics_result;
pub mod delete_acls_result;
pub mod delete_consumer_group_offsets_result;
pub mod delete_consumer_groups_result;
pub mod delete_records_result;
pub mod delete_topics_result;
pub mod deleted_records;
pub mod describe_acls_result;
pub mod describe_classic_groups_result;
pub mod describe_client_quotas_result;
pub mod describe_cluster_result;
pub mod describe_configs_result;
pub mod describe_consumer_groups_result;
pub mod describe_log_dirs_result;
pub mod describe_replica_log_dirs_result;
pub mod describe_topics_result;
pub mod elect_leaders_result;
pub mod group_listing;
pub mod kafka_admin_client;
pub mod list_config_resources_result;
pub mod list_consumer_group_offsets_result;
pub mod list_consumer_group_offsets_spec;
pub mod list_consumer_groups_result;
pub mod list_groups_result;
pub mod list_offsets_result;
pub mod list_partition_reassignments_result;
pub mod list_topics_result;
pub mod log_dir_description;
pub mod member_assignment;
pub mod member_description;
pub mod member_to_remove;
pub mod mock_admin_client;
pub mod new_partition_reassignment;
pub mod new_partitions;
pub mod new_topic;
pub mod offset_spec;
pub mod options;
pub mod partition_reassignment;
pub mod records_to_delete;
pub mod remove_members_from_consumer_group_result;
pub mod replica_info;
pub mod topic_description;
pub mod topic_listing;

pub(crate) mod internals;

pub use admin_client_config::AdminClientConfig;
pub use alter_client_quotas_result::AlterClientQuotasResult;
pub use alter_config_op::{AlterConfigOp, OpType};
pub use alter_configs_result::AlterConfigsResult;
pub use alter_consumer_group_offsets_result::AlterConsumerGroupOffsetsResult;
pub use alter_partition_reassignments_result::AlterPartitionReassignmentsResult;
pub use alter_replica_log_dirs_result::AlterReplicaLogDirsResult;
pub use classic_group_description::ClassicGroupDescription;
pub use config::Config;
pub use config_entry::{ConfigEntry, ConfigSource, ConfigSynonym, ConfigType};
pub use consumer_group_description::ConsumerGroupDescription;
#[allow(deprecated)]
pub use consumer_group_listing::ConsumerGroupListing;
use std::collections::HashMap;

pub use create_acls_result::CreateAclsResult;
pub use create_partitions_result::CreatePartitionsResult;
pub use create_topics_result::{CreateTopicsResult, TopicMetadataAndConfig};
pub use delete_acls_result::{DeleteAclsResult, FilterResult, FilterResults};
pub use delete_consumer_group_offsets_result::DeleteConsumerGroupOffsetsResult;
pub use delete_consumer_groups_result::DeleteConsumerGroupsResult;
pub use delete_records_result::DeleteRecordsResult;
pub use delete_topics_result::DeleteTopicsResult;
pub use deleted_records::DeletedRecords;
pub use describe_acls_result::DescribeAclsResult;
pub use describe_classic_groups_result::DescribeClassicGroupsResult;
pub use describe_client_quotas_result::DescribeClientQuotasResult;
pub use describe_cluster_result::DescribeClusterResult;
pub use describe_configs_result::DescribeConfigsResult;
pub use describe_consumer_groups_result::DescribeConsumerGroupsResult;
pub use describe_log_dirs_result::DescribeLogDirsResult;
pub use describe_replica_log_dirs_result::{DescribeReplicaLogDirsResult, ReplicaLogDirInfo};
pub use describe_topics_result::DescribeTopicsResult;
pub use elect_leaders_result::ElectLeadersResult;
pub use group_listing::GroupListing;
pub use kafka_admin_client::KafkaAdminClient;
pub use list_config_resources_result::ListConfigResourcesResult;
pub use list_consumer_group_offsets_result::{GroupOffsets, ListConsumerGroupOffsetsResult};
pub use list_consumer_group_offsets_spec::ListConsumerGroupOffsetsSpec;
#[allow(deprecated)]
pub use list_consumer_groups_result::ListConsumerGroupsResult;
pub use list_groups_result::ListGroupsResult;
pub use list_offsets_result::{ListOffsetsResult, ListOffsetsResultInfo};
pub use list_partition_reassignments_result::ListPartitionReassignmentsResult;
pub use list_topics_result::ListTopicsResult;
pub use log_dir_description::LogDirDescription;
pub use member_assignment::MemberAssignment;
pub use member_description::MemberDescription;
pub use member_to_remove::MemberToRemove;
pub use mock_admin_client::MockAdminClient;
pub use new_partition_reassignment::NewPartitionReassignment;
pub use new_partitions::NewPartitions;
pub use new_topic::NewTopic;
pub use offset_spec::OffsetSpec;
#[allow(deprecated)]
pub use options::ListConsumerGroupsOptions;
pub use options::{
    AlterClientQuotasOptions, AlterConfigsOptions, AlterConsumerGroupOffsetsOptions,
    AlterPartitionReassignmentsOptions, AlterReplicaLogDirsOptions, CreateAclsOptions, CreatePartitionsOptions,
    CreateTopicsOptions, DeleteAclsOptions, DeleteConsumerGroupOffsetsOptions, DeleteConsumerGroupsOptions,
    DeleteRecordsOptions, DeleteTopicsOptions, DescribeAclsOptions, DescribeClassicGroupsOptions,
    DescribeClientQuotasOptions, DescribeClusterOptions, DescribeConfigsOptions, DescribeConsumerGroupsOptions,
    DescribeLogDirsOptions, DescribeReplicaLogDirsOptions, DescribeTopicsOptions, ElectLeadersOptions,
    ListConfigResourcesOptions, ListConsumerGroupOffsetsOptions, ListGroupsOptions, ListOffsetsOptions,
    ListPartitionReassignmentsOptions, ListTopicsOptions, RemoveMembersFromConsumerGroupOptions,
};
pub use partition_reassignment::PartitionReassignment;
pub use records_to_delete::RecordsToDelete;
pub use remove_members_from_consumer_group_result::RemoveMembersFromConsumerGroupResult;
pub use replica_info::ReplicaInfo;
pub use topic_description::TopicDescription;
pub use topic_listing::TopicListing;

use std::time::Duration;

use async_trait::async_trait;

use crate::common::acl::{AclBinding, AclBindingFilter};
use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::quota::{ClientQuotaAlteration, ClientQuotaFilter};
use crate::common::{ElectionType, KafkaError, TopicCollection, TopicPartition, TopicPartitionReplica};
use crate::consumer::OffsetAndMetadata;

use std::collections::HashSet;

/// The administrative client for Kafka, which supports managing and inspecting
/// topics, brokers, configurations and records.
///
/// Corresponds to `org.apache.kafka.clients.admin.Admin`.
///
/// Per `.claude/rules/admin-client.md` §1, every RPC method is a **plain sync
/// `fn`** that returns immediately with a `*Result` holding one
/// [`KafkaFuture`](crate::common::KafkaFuture) per key — the network I/O happens
/// later on the background task, and the caller opts into blocking by awaiting
/// the returned future(s). The only `async fn` is [`close`](Admin::close),
/// which (like Java's `close(Duration)`) joins the background task.
#[async_trait]
pub trait Admin: Send + Sync {
    /// Create a batch of new topics.
    ///
    /// Corresponds to `Admin.createTopics(Collection<NewTopic>, CreateTopicsOptions)`.
    fn create_topics(&self, new_topics: &[NewTopic], options: CreateTopicsOptions) -> CreateTopicsResult;

    /// Delete a batch of topics (by name or by id, per the `TopicCollection`).
    ///
    /// Corresponds to `Admin.deleteTopics(TopicCollection, DeleteTopicsOptions)`.
    fn delete_topics(&self, topics: TopicCollection, options: DeleteTopicsOptions) -> DeleteTopicsResult;

    /// List the topics available in the cluster.
    ///
    /// Corresponds to `Admin.listTopics(ListTopicsOptions)`.
    fn list_topics(&self, options: ListTopicsOptions) -> ListTopicsResult;

    /// Describe some topics in the cluster (by name or by id, per the
    /// `TopicCollection`).
    ///
    /// Corresponds to `Admin.describeTopics(TopicCollection, DescribeTopicsOptions)`.
    fn describe_topics(&self, topics: TopicCollection, options: DescribeTopicsOptions) -> DescribeTopicsResult;

    /// Increase the number of partitions of the given topics.
    ///
    /// Corresponds to `Admin.createPartitions(Map<String, NewPartitions>, CreatePartitionsOptions)`.
    fn create_partitions(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> CreatePartitionsResult;

    /// Delete records whose offset is smaller than the given offset of the
    /// corresponding partition.
    ///
    /// Corresponds to `Admin.deleteRecords(Map<TopicPartition, RecordsToDelete>, DeleteRecordsOptions)`.
    fn delete_records(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> DeleteRecordsResult;

    /// Get information about the nodes in the cluster.
    ///
    /// Corresponds to `Admin.describeCluster(DescribeClusterOptions)`.
    fn describe_cluster(&self, options: DescribeClusterOptions) -> DescribeClusterResult;

    /// Get the configuration for the specified resources.
    ///
    /// Corresponds to `Admin.describeConfigs(Collection<ConfigResource>, DescribeConfigsOptions)`.
    fn describe_configs(
        &self,
        config_resources: &[ConfigResource],
        options: DescribeConfigsOptions,
    ) -> DescribeConfigsResult;

    /// Incrementally update the configuration for the specified resources.
    ///
    /// Corresponds to `Admin.incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>, AlterConfigsOptions)`.
    fn incremental_alter_configs(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ) -> AlterConfigsResult;

    /// List the config resources available in the cluster whose type is in the
    /// given set (an empty set means all supported types).
    ///
    /// Corresponds to `Admin.listConfigResources(Set<ConfigResource.Type>, ListConfigResourcesOptions)`.
    fn list_config_resources(
        &self,
        config_resource_types: &HashSet<ConfigResourceType>,
        options: ListConfigResourcesOptions,
    ) -> ListConfigResourcesResult;

    /// Query the information of all log directories on the given set of
    /// brokers.
    ///
    /// Corresponds to `Admin.describeLogDirs(Collection<Integer>, DescribeLogDirsOptions)`.
    fn describe_log_dirs(&self, brokers: &[i32], options: DescribeLogDirsOptions) -> DescribeLogDirsResult;

    /// Change the log directory for the specified replicas.
    ///
    /// Corresponds to `Admin.alterReplicaLogDirs(Map<TopicPartitionReplica, String>, AlterReplicaLogDirsOptions)`.
    fn alter_replica_log_dirs(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ) -> AlterReplicaLogDirsResult;

    /// Query the replica log directory information for the specified replicas.
    ///
    /// Corresponds to `Admin.describeReplicaLogDirs(Collection<TopicPartitionReplica>, DescribeReplicaLogDirsOptions)`.
    fn describe_replica_log_dirs(
        &self,
        replicas: &[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> DescribeReplicaLogDirsResult;

    /// Elect a replica as leader for the given partitions, or for all
    /// partitions if `partitions` is `None`.
    ///
    /// Corresponds to `Admin.electLeaders(ElectionType, Set<TopicPartition>, ElectLeadersOptions)`.
    fn elect_leaders(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
        options: ElectLeadersOptions,
    ) -> ElectLeadersResult;

    /// Change the partition reassignments for the given partitions.
    ///
    /// A `None` value for a partition cancels an ongoing reassignment.
    ///
    /// Corresponds to
    /// `Admin.alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>>, AlterPartitionReassignmentsOptions)`.
    fn alter_partition_reassignments(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
        options: AlterPartitionReassignmentsOptions,
    ) -> AlterPartitionReassignmentsResult;

    /// List the current partition reassignments, optionally restricted to a set
    /// of partitions (`None` lists all ongoing reassignments).
    ///
    /// Corresponds to
    /// `Admin.listPartitionReassignments(Optional<Set<TopicPartition>>, ListPartitionReassignmentsOptions)`.
    fn list_partition_reassignments(
        &self,
        partitions: Option<HashSet<TopicPartition>>,
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult;

    /// List the offsets for the given partitions and offset specifications.
    ///
    /// Corresponds to `Admin.listOffsets(Map<TopicPartition, OffsetSpec>, ListOffsetsOptions)`.
    fn list_offsets(
        &self,
        topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ) -> ListOffsetsResult;

    /// List the groups available in the cluster.
    ///
    /// Corresponds to `Admin.listGroups(ListGroupsOptions)`.
    fn list_groups(&self, options: ListGroupsOptions) -> ListGroupsResult;

    /// List the consumer groups available in the cluster.
    ///
    /// Corresponds to `Admin.listConsumerGroups(ListConsumerGroupsOptions)`
    /// (deprecated since 4.1 in favor of [`list_groups`](Admin::list_groups)).
    #[allow(deprecated)]
    fn list_consumer_groups(&self, options: ListConsumerGroupsOptions) -> ListConsumerGroupsResult;

    /// Describe some consumer groups in the cluster.
    ///
    /// Corresponds to `Admin.describeConsumerGroups(Collection<String>, DescribeConsumerGroupsOptions)`.
    fn describe_consumer_groups(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> DescribeConsumerGroupsResult;

    /// Describe some classic groups in the cluster.
    ///
    /// Corresponds to `Admin.describeClassicGroups(Collection<String>, DescribeClassicGroupsOptions)`.
    fn describe_classic_groups(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> DescribeClassicGroupsResult;

    /// List the consumer group offsets available in the cluster for the given
    /// group specifications.
    ///
    /// Corresponds to
    /// `Admin.listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>, ListConsumerGroupOffsetsOptions)`.
    fn list_consumer_group_offsets(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult;

    /// Alter offsets for a consumer group.
    ///
    /// Corresponds to
    /// `Admin.alterConsumerGroupOffsets(String, Map<TopicPartition, OffsetAndMetadata>, AlterConsumerGroupOffsetsOptions)`.
    fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ) -> AlterConsumerGroupOffsetsResult;

    /// Delete offsets for a set of partitions in a consumer group.
    ///
    /// Corresponds to
    /// `Admin.deleteConsumerGroupOffsets(String, Set<TopicPartition>, DeleteConsumerGroupOffsetsOptions)`.
    fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> DeleteConsumerGroupOffsetsResult;

    /// Delete consumer groups from the cluster.
    ///
    /// Corresponds to
    /// `Admin.deleteConsumerGroups(Collection<String>, DeleteConsumerGroupsOptions)`.
    fn delete_consumer_groups(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> DeleteConsumerGroupsResult;

    /// Remove members from a consumer group by given member identities.
    ///
    /// Corresponds to
    /// `Admin.removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)`.
    fn remove_members_from_consumer_group(
        &self,
        group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> RemoveMembersFromConsumerGroupResult;

    /// Create ACLs.
    ///
    /// Corresponds to `Admin.createAcls(Collection<AclBinding>, CreateAclsOptions)`.
    fn create_acls(&self, acls: &[AclBinding], options: CreateAclsOptions) -> CreateAclsResult;

    /// Describe ACLs matching the provided filter.
    ///
    /// Corresponds to `Admin.describeAcls(AclBindingFilter, DescribeAclsOptions)`.
    fn describe_acls(&self, filter: &AclBindingFilter, options: DescribeAclsOptions) -> DescribeAclsResult;

    /// Delete ACLs matching the provided filters.
    ///
    /// Corresponds to `Admin.deleteAcls(Collection<AclBindingFilter>, DeleteAclsOptions)`.
    fn delete_acls(&self, filters: &[AclBindingFilter], options: DeleteAclsOptions) -> DeleteAclsResult;

    /// Describe the client quotas matching the provided filter.
    ///
    /// Corresponds to
    /// `Admin.describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)`.
    fn describe_client_quotas(
        &self,
        filter: &ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> DescribeClientQuotasResult;

    /// Alter the client quotas of one or more quota entities.
    ///
    /// Corresponds to
    /// `Admin.alterClientQuotas(Collection<ClientQuotaAlteration>, AlterClientQuotasOptions)`.
    fn alter_client_quotas(
        &self,
        entries: &[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> AlterClientQuotasResult;

    /// Close the admin client, awaiting the background task to finish
    /// in-flight work up to `timeout`.
    ///
    /// Corresponds to `Admin.close(Duration)`; blocking in Java, so `async` in
    /// Rust (CLAUDE.md §9.4).
    async fn close(&self, timeout: Duration);
}

/// Creates a network-backed [`Admin`] client from the given configuration.
///
/// Corresponds to `Admin.create(Properties)` / `AdminClient.create`. Spawns the
/// single background I/O task. Phase 1 supports the PLAINTEXT security protocol
/// only.
///
/// # Errors
///
/// Returns an error if the bootstrap addresses cannot be resolved or the
/// network client cannot be constructed.
pub fn new_admin_client(config: AdminClientConfig) -> Result<Box<dyn Admin>, KafkaError> {
    Ok(Box::new(KafkaAdminClient::from_config(config)?))
}
