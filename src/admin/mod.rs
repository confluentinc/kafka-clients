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

pub mod abort_transaction_result;
pub mod abort_transaction_spec;
pub mod admin_client_config;
pub mod alter_client_quotas_result;
pub mod alter_config_op;
pub mod alter_configs_result;
pub mod alter_consumer_group_offsets_result;
pub mod alter_partition_reassignments_result;
pub mod alter_replica_log_dirs_result;
pub mod alter_user_scram_credentials_result;
pub mod classic_group_description;
pub mod client_metrics_resource_listing;
pub mod config;
pub mod config_entry;
pub mod consumer_group_description;
pub mod consumer_group_listing;
pub mod create_acls_result;
pub mod create_delegation_token_result;
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
pub mod describe_delegation_token_result;
pub mod describe_features_result;
pub mod describe_log_dirs_result;
pub mod describe_producers_result;
pub mod describe_replica_log_dirs_result;
pub mod describe_topics_result;
pub mod describe_transactions_result;
pub mod describe_user_scram_credentials_result;
pub mod elect_leaders_result;
pub mod expire_delegation_token_result;
pub mod feature_metadata;
pub mod feature_update;
pub mod fence_producers_result;
pub mod finalized_version_range;
pub mod group_listing;
pub mod kafka_admin_client;
pub mod list_client_metrics_resources_result;
pub mod list_config_resources_result;
pub mod list_consumer_group_offsets_result;
pub mod list_consumer_group_offsets_spec;
pub mod list_consumer_groups_result;
pub mod list_groups_result;
pub mod list_offsets_result;
pub mod list_partition_reassignments_result;
pub mod list_topics_result;
pub mod list_transactions_result;
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
pub mod producer_state;
pub mod records_to_delete;
pub mod remove_members_from_consumer_group_result;
pub mod renew_delegation_token_result;
pub mod replica_info;
pub mod scram_credential_info;
pub mod scram_mechanism;
pub mod supported_version_range;
pub mod terminate_transaction_result;
pub mod topic_description;
pub mod topic_listing;
pub mod transaction_description;
pub mod transaction_listing;
pub mod transaction_state;
pub mod update_features_result;
pub mod user_scram_credential_alteration;
pub mod user_scram_credential_deletion;
pub mod user_scram_credential_upsertion;
pub mod user_scram_credentials_description;

pub(crate) mod internals;

pub use abort_transaction_result::AbortTransactionResult;
pub use abort_transaction_spec::AbortTransactionSpec;
pub use admin_client_config::AdminClientConfig;
pub use alter_client_quotas_result::AlterClientQuotasResult;
pub use alter_config_op::{AlterConfigOp, OpType};
pub use alter_configs_result::AlterConfigsResult;
pub use alter_consumer_group_offsets_result::AlterConsumerGroupOffsetsResult;
pub use alter_partition_reassignments_result::AlterPartitionReassignmentsResult;
pub use alter_replica_log_dirs_result::AlterReplicaLogDirsResult;
pub use alter_user_scram_credentials_result::AlterUserScramCredentialsResult;
pub use classic_group_description::ClassicGroupDescription;
#[allow(deprecated)]
pub use client_metrics_resource_listing::ClientMetricsResourceListing;
pub use config::Config;
pub use config_entry::{ConfigEntry, ConfigEntryOptions, ConfigSource, ConfigSynonym, ConfigType};
pub use consumer_group_description::ConsumerGroupDescription;
#[allow(deprecated)]
pub use consumer_group_listing::ConsumerGroupListing;
use std::collections::HashMap;

pub use create_acls_result::CreateAclsResult;
pub use create_delegation_token_result::CreateDelegationTokenResult;
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
pub use describe_delegation_token_result::DescribeDelegationTokenResult;
pub use describe_features_result::DescribeFeaturesResult;
pub use describe_log_dirs_result::DescribeLogDirsResult;
pub use describe_producers_result::{DescribeProducersResult, PartitionProducerState};
pub use describe_replica_log_dirs_result::{DescribeReplicaLogDirsResult, ReplicaLogDirInfo};
pub use describe_topics_result::DescribeTopicsResult;
pub use describe_transactions_result::DescribeTransactionsResult;
pub use describe_user_scram_credentials_result::DescribeUserScramCredentialsResult;
pub use elect_leaders_result::ElectLeadersResult;
pub use expire_delegation_token_result::ExpireDelegationTokenResult;
pub use feature_metadata::FeatureMetadata;
pub use feature_update::{FeatureUpdate, UpgradeType};
pub use fence_producers_result::FenceProducersResult;
pub use finalized_version_range::FinalizedVersionRange;
pub use group_listing::GroupListing;
pub use kafka_admin_client::KafkaAdminClient;
#[allow(deprecated)]
pub use list_client_metrics_resources_result::ListClientMetricsResourcesResult;
pub use list_config_resources_result::ListConfigResourcesResult;
pub use list_consumer_group_offsets_result::{GroupOffsets, ListConsumerGroupOffsetsResult};
pub use list_consumer_group_offsets_spec::ListConsumerGroupOffsetsSpec;
#[allow(deprecated)]
pub use list_consumer_groups_result::ListConsumerGroupsResult;
pub use list_groups_result::ListGroupsResult;
pub use list_offsets_result::{ListOffsetsResult, ListOffsetsResultInfo};
pub use list_partition_reassignments_result::ListPartitionReassignmentsResult;
pub use list_topics_result::ListTopicsResult;
pub use list_transactions_result::ListTransactionsResult;
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
pub use options::ListClientMetricsResourcesOptions;
#[allow(deprecated)]
pub use options::ListConsumerGroupsOptions;
pub use options::{
    AbortTransactionOptions, AlterClientQuotasOptions, AlterConfigsOptions, AlterConsumerGroupOffsetsOptions,
    AlterPartitionReassignmentsOptions, AlterReplicaLogDirsOptions, AlterUserScramCredentialsOptions,
    CreateAclsOptions, CreateDelegationTokenOptions, CreatePartitionsOptions, CreateTopicsOptions, DeleteAclsOptions,
    DeleteConsumerGroupOffsetsOptions, DeleteConsumerGroupsOptions, DeleteRecordsOptions, DeleteTopicsOptions,
    DescribeAclsOptions, DescribeClassicGroupsOptions, DescribeClientQuotasOptions, DescribeClusterOptions,
    DescribeConfigsOptions, DescribeConsumerGroupsOptions, DescribeDelegationTokenOptions, DescribeFeaturesOptions,
    DescribeLogDirsOptions, DescribeProducersOptions, DescribeReplicaLogDirsOptions, DescribeTopicsOptions,
    DescribeTransactionsOptions, DescribeUserScramCredentialsOptions, ElectLeadersOptions,
    ExpireDelegationTokenOptions, FenceProducersOptions, ListConfigResourcesOptions, ListConsumerGroupOffsetsOptions,
    ListGroupsOptions, ListOffsetsOptions, ListPartitionReassignmentsOptions, ListTopicsOptions,
    ListTransactionsOptions, RemoveMembersFromConsumerGroupOptions, RenewDelegationTokenOptions,
    TerminateTransactionOptions, UpdateFeaturesOptions,
};
pub use partition_reassignment::PartitionReassignment;
pub use producer_state::ProducerState;
pub use records_to_delete::RecordsToDelete;
pub use remove_members_from_consumer_group_result::RemoveMembersFromConsumerGroupResult;
pub use renew_delegation_token_result::RenewDelegationTokenResult;
pub use replica_info::ReplicaInfo;
pub use scram_credential_info::ScramCredentialInfo;
pub use scram_mechanism::ScramMechanism;
pub use supported_version_range::SupportedVersionRange;
pub use terminate_transaction_result::TerminateTransactionResult;
pub use topic_description::TopicDescription;
pub use topic_listing::TopicListing;
pub use transaction_description::TransactionDescription;
pub use transaction_listing::TransactionListing;
pub use transaction_state::TransactionState;
pub use update_features_result::UpdateFeaturesResult;
pub use user_scram_credential_alteration::UserScramCredentialAlteration;
pub use user_scram_credential_deletion::UserScramCredentialDeletion;
pub use user_scram_credential_upsertion::UserScramCredentialUpsertion;
pub use user_scram_credentials_description::UserScramCredentialsDescription;

use std::time::Duration;

use async_trait::async_trait;

use crate::common::acl::{AclBinding, AclBindingFilter};
use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::quota::{ClientQuotaAlteration, ClientQuotaFilter};
use crate::common::{ElectionType, Error, TopicCollection, TopicPartition, TopicPartitionReplica};
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
    /// Corresponds to `Admin.createTopics(Collection<NewTopic>)`.
    fn create_topics(&self, new_topics: &[NewTopic]) -> CreateTopicsResult {
        self.create_topics_options(new_topics, CreateTopicsOptions::default())
    }

    /// Create a batch of new topics.
    ///
    /// Corresponds to `Admin.createTopics(Collection<NewTopic>, CreateTopicsOptions)`.
    fn create_topics_options(&self, new_topics: &[NewTopic], options: CreateTopicsOptions) -> CreateTopicsResult;

    /// Delete a batch of topics (by name or by id, per the `TopicCollection`).
    ///
    /// Corresponds to `Admin.deleteTopics(TopicCollection)`.
    fn delete_topics(&self, topics: TopicCollection) -> DeleteTopicsResult {
        self.delete_topics_options(topics, DeleteTopicsOptions::default())
    }

    /// Delete a batch of topics (by name or by id, per the `TopicCollection`).
    ///
    /// Corresponds to `Admin.deleteTopics(TopicCollection, DeleteTopicsOptions)`.
    fn delete_topics_options(&self, topics: TopicCollection, options: DeleteTopicsOptions) -> DeleteTopicsResult;

    /// List the topics available in the cluster.
    ///
    /// Corresponds to `Admin.listTopics()`.
    fn list_topics(&self) -> ListTopicsResult {
        self.list_topics_options(ListTopicsOptions::default())
    }

    /// List the topics available in the cluster.
    ///
    /// Corresponds to `Admin.listTopics(ListTopicsOptions)`.
    fn list_topics_options(&self, options: ListTopicsOptions) -> ListTopicsResult;

    /// Describe some topics in the cluster, by name.
    ///
    /// Corresponds to `Admin.describeTopics(Collection<String>)`.
    fn describe_topics_topic_names(&self, topic_names: &[String]) -> DescribeTopicsResult {
        self.describe_topics_topic_names_options(topic_names, DescribeTopicsOptions::default())
    }

    /// Describe some topics in the cluster, by name.
    ///
    /// Corresponds to `Admin.describeTopics(Collection<String>, DescribeTopicsOptions)`,
    /// whose body wraps the names in a `TopicCollection`.
    fn describe_topics_topic_names_options(
        &self,
        topic_names: &[String],
        options: DescribeTopicsOptions,
    ) -> DescribeTopicsResult {
        self.describe_topics_options(TopicCollection::of_topic_names(topic_names.to_vec()), options)
    }

    /// Describe some topics in the cluster (by name or by id, per the
    /// `TopicCollection`).
    ///
    /// Corresponds to `Admin.describeTopics(TopicCollection)`.
    fn describe_topics(&self, topics: TopicCollection) -> DescribeTopicsResult {
        self.describe_topics_options(topics, DescribeTopicsOptions::default())
    }

    /// Describe some topics in the cluster (by name or by id, per the
    /// `TopicCollection`).
    ///
    /// Corresponds to `Admin.describeTopics(TopicCollection, DescribeTopicsOptions)`.
    fn describe_topics_options(&self, topics: TopicCollection, options: DescribeTopicsOptions) -> DescribeTopicsResult;

    /// Increase the number of partitions of the given topics.
    ///
    /// The returned per-topic futures may complete exceptionally with, among
    /// others, `InvalidPartitionsException` if the requested partition count is
    /// less than or equal to the current partition count.
    ///
    /// Corresponds to `Admin.createPartitions(Map<String, NewPartitions>)`.
    fn create_partitions(&self, new_partitions: &HashMap<String, NewPartitions>) -> CreatePartitionsResult {
        self.create_partitions_options(new_partitions, CreatePartitionsOptions::default())
    }

    /// Increase the number of partitions of the given topics.
    ///
    /// The returned per-topic futures may complete exceptionally with, among
    /// others, `InvalidPartitionsException` if the requested partition count is
    /// less than or equal to the current partition count.
    ///
    /// Corresponds to `Admin.createPartitions(Map<String, NewPartitions>, CreatePartitionsOptions)`.
    fn create_partitions_options(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> CreatePartitionsResult;

    /// Delete records whose offset is smaller than the given offset of the
    /// corresponding partition.
    ///
    /// Corresponds to `Admin.deleteRecords(Map<TopicPartition, RecordsToDelete>)`.
    fn delete_records(&self, records_to_delete: &HashMap<TopicPartition, RecordsToDelete>) -> DeleteRecordsResult {
        self.delete_records_options(records_to_delete, DeleteRecordsOptions::default())
    }

    /// Delete records whose offset is smaller than the given offset of the
    /// corresponding partition.
    ///
    /// Corresponds to `Admin.deleteRecords(Map<TopicPartition, RecordsToDelete>, DeleteRecordsOptions)`.
    fn delete_records_options(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> DeleteRecordsResult;

    /// Describe the active producers for a set of topic partitions.
    ///
    /// Corresponds to `Admin.describeProducers(Collection<TopicPartition>)`.
    fn describe_producers(&self, partitions: &[TopicPartition]) -> DescribeProducersResult {
        self.describe_producers_options(partitions, DescribeProducersOptions::default())
    }

    /// Describe the active producers for a set of topic partitions.
    ///
    /// Corresponds to `Admin.describeProducers(Collection<TopicPartition>, DescribeProducersOptions)`.
    fn describe_producers_options(
        &self,
        partitions: &[TopicPartition],
        options: DescribeProducersOptions,
    ) -> DescribeProducersResult;

    /// Forcefully abort a transaction which is open on a topic partition.
    ///
    /// Corresponds to `Admin.abortTransaction(AbortTransactionSpec)`.
    fn abort_transaction(&self, spec: AbortTransactionSpec) -> AbortTransactionResult {
        self.abort_transaction_options(spec, AbortTransactionOptions::default())
    }

    /// Forcefully abort a transaction which is open on a topic partition.
    ///
    /// Corresponds to `Admin.abortTransaction(AbortTransactionSpec, AbortTransactionOptions)`.
    fn abort_transaction_options(
        &self,
        spec: AbortTransactionSpec,
        options: AbortTransactionOptions,
    ) -> AbortTransactionResult;

    /// List the transaction states of the given transactional ids.
    ///
    /// Corresponds to `Admin.describeTransactions(Collection<String>)`.
    fn describe_transactions(&self, transactional_ids: &[String]) -> DescribeTransactionsResult {
        self.describe_transactions_options(transactional_ids, DescribeTransactionsOptions::default())
    }

    /// List the transaction states of the given transactional ids.
    ///
    /// Corresponds to `Admin.describeTransactions(Collection<String>, DescribeTransactionsOptions)`.
    fn describe_transactions_options(
        &self,
        transactional_ids: &[String],
        options: DescribeTransactionsOptions,
    ) -> DescribeTransactionsResult;

    /// Fence out all active producers that use any of the provided transactional ids.
    ///
    /// Corresponds to `Admin.fenceProducers(Collection<String>)`.
    fn fence_producers(&self, transactional_ids: &[String]) -> FenceProducersResult {
        self.fence_producers_options(transactional_ids, FenceProducersOptions::default())
    }

    /// Fence out all active producers that use any of the provided transactional ids.
    ///
    /// Corresponds to `Admin.fenceProducers(Collection<String>, FenceProducersOptions)`.
    fn fence_producers_options(
        &self,
        transactional_ids: &[String],
        options: FenceProducersOptions,
    ) -> FenceProducersResult;

    /// List the transactions in the cluster (fans out to all brokers).
    ///
    /// Corresponds to `Admin.listTransactions()`.
    fn list_transactions(&self) -> ListTransactionsResult {
        self.list_transactions_options(ListTransactionsOptions::default())
    }

    /// List the transactions in the cluster (fans out to all brokers).
    ///
    /// Corresponds to `Admin.listTransactions(ListTransactionsOptions)`.
    fn list_transactions_options(&self, options: ListTransactionsOptions) -> ListTransactionsResult;

    /// Forcefully terminate an ongoing transaction for a given transactional id.
    ///
    /// Corresponds to `Admin.forceTerminateTransaction(String)`.
    fn force_terminate_transaction(&self, transactional_id: &str) -> TerminateTransactionResult {
        self.force_terminate_transaction_options(transactional_id, TerminateTransactionOptions::default())
    }

    /// Forcefully terminate an ongoing transaction for a given transactional id.
    ///
    /// Corresponds to `Admin.forceTerminateTransaction(String, TerminateTransactionOptions)`.
    fn force_terminate_transaction_options(
        &self,
        transactional_id: &str,
        options: TerminateTransactionOptions,
    ) -> TerminateTransactionResult;

    /// Get information about the nodes in the cluster.
    ///
    /// Corresponds to `Admin.describeCluster()`.
    fn describe_cluster(&self) -> DescribeClusterResult {
        self.describe_cluster_options(DescribeClusterOptions::default())
    }

    /// Get information about the nodes in the cluster.
    ///
    /// Corresponds to `Admin.describeCluster(DescribeClusterOptions)`.
    fn describe_cluster_options(&self, options: DescribeClusterOptions) -> DescribeClusterResult;

    /// Get the configuration for the specified resources.
    ///
    /// Corresponds to `Admin.describeConfigs(Collection<ConfigResource>)`.
    fn describe_configs(&self, config_resources: &[ConfigResource]) -> DescribeConfigsResult {
        self.describe_configs_options(config_resources, DescribeConfigsOptions::default())
    }

    /// Get the configuration for the specified resources.
    ///
    /// Corresponds to `Admin.describeConfigs(Collection<ConfigResource>, DescribeConfigsOptions)`.
    fn describe_configs_options(
        &self,
        config_resources: &[ConfigResource],
        options: DescribeConfigsOptions,
    ) -> DescribeConfigsResult;

    /// Incrementally update the configuration for the specified resources.
    ///
    /// Corresponds to `Admin.incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>)`.
    fn incremental_alter_configs(&self, configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>) -> AlterConfigsResult {
        self.incremental_alter_configs_options(configs, AlterConfigsOptions::default())
    }

    /// Incrementally update the configuration for the specified resources.
    ///
    /// Corresponds to `Admin.incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>, AlterConfigsOptions)`.
    fn incremental_alter_configs_options(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ) -> AlterConfigsResult;

    /// List all the config resources available in the cluster.
    ///
    /// Corresponds to `Admin.listConfigResources()`. Java's body passes an
    /// empty type set — meaning "all supported types" — alongside the fresh
    /// options instance, so this is not a plain no-options forward.
    fn list_config_resources(&self) -> ListConfigResourcesResult {
        self.list_config_resources_options(&HashSet::new(), ListConfigResourcesOptions::default())
    }

    /// List the config resources available in the cluster whose type is in the
    /// given set (an empty set means all supported types).
    ///
    /// Corresponds to `Admin.listConfigResources(Set<ConfigResource.Type>, ListConfigResourcesOptions)`.
    fn list_config_resources_options(
        &self,
        config_resource_types: &HashSet<ConfigResourceType>,
        options: ListConfigResourcesOptions,
    ) -> ListConfigResourcesResult;

    /// List the client metrics resources available in the cluster.
    ///
    /// Corresponds to `Admin.listClientMetricsResources()`
    /// (deprecated since 4.1 in favor of
    /// [`list_config_resources`](Admin::list_config_resources)).
    #[allow(deprecated)]
    fn list_client_metrics_resources(&self) -> ListClientMetricsResourcesResult {
        self.list_client_metrics_resources_options(ListClientMetricsResourcesOptions::default())
    }

    /// List the client metrics resources available in the cluster.
    ///
    /// Corresponds to `Admin.listClientMetricsResources(ListClientMetricsResourcesOptions)`
    /// (deprecated since 4.1 in favor of
    /// [`list_config_resources`](Admin::list_config_resources)).
    #[allow(deprecated)]
    fn list_client_metrics_resources_options(
        &self,
        options: ListClientMetricsResourcesOptions,
    ) -> ListClientMetricsResourcesResult;

    /// Query the information of all log directories on the given set of
    /// brokers.
    ///
    /// Corresponds to `Admin.describeLogDirs(Collection<Integer>)`.
    fn describe_log_dirs(&self, brokers: &[i32]) -> DescribeLogDirsResult {
        self.describe_log_dirs_options(brokers, DescribeLogDirsOptions::default())
    }

    /// Query the information of all log directories on the given set of
    /// brokers.
    ///
    /// Corresponds to `Admin.describeLogDirs(Collection<Integer>, DescribeLogDirsOptions)`.
    fn describe_log_dirs_options(&self, brokers: &[i32], options: DescribeLogDirsOptions) -> DescribeLogDirsResult;

    /// Change the log directory for the specified replicas.
    ///
    /// Corresponds to `Admin.alterReplicaLogDirs(Map<TopicPartitionReplica, String>)`.
    fn alter_replica_log_dirs(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
    ) -> AlterReplicaLogDirsResult {
        self.alter_replica_log_dirs_options(replica_assignment, AlterReplicaLogDirsOptions::default())
    }

    /// Change the log directory for the specified replicas.
    ///
    /// Corresponds to `Admin.alterReplicaLogDirs(Map<TopicPartitionReplica, String>, AlterReplicaLogDirsOptions)`.
    fn alter_replica_log_dirs_options(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ) -> AlterReplicaLogDirsResult;

    /// Query the replica log directory information for the specified replicas.
    ///
    /// Corresponds to `Admin.describeReplicaLogDirs(Collection<TopicPartitionReplica>)`.
    fn describe_replica_log_dirs(&self, replicas: &[TopicPartitionReplica]) -> DescribeReplicaLogDirsResult {
        self.describe_replica_log_dirs_options(replicas, DescribeReplicaLogDirsOptions::default())
    }

    /// Query the replica log directory information for the specified replicas.
    ///
    /// Corresponds to `Admin.describeReplicaLogDirs(Collection<TopicPartitionReplica>, DescribeReplicaLogDirsOptions)`.
    fn describe_replica_log_dirs_options(
        &self,
        replicas: &[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> DescribeReplicaLogDirsResult;

    /// Elect a replica as leader for the given partitions, or for all
    /// partitions if `partitions` is `None`.
    ///
    /// Corresponds to `Admin.electLeaders(ElectionType, Set<TopicPartition>)`.
    fn elect_leaders(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
    ) -> ElectLeadersResult {
        self.elect_leaders_options(election_type, partitions, ElectLeadersOptions::default())
    }

    /// Elect a replica as leader for the given partitions, or for all
    /// partitions if `partitions` is `None`.
    ///
    /// Corresponds to `Admin.electLeaders(ElectionType, Set<TopicPartition>, ElectLeadersOptions)`.
    fn elect_leaders_options(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
        options: ElectLeadersOptions,
    ) -> ElectLeadersResult;

    /// Change the partition reassignments for the given partitions.
    ///
    /// A `None` value for a partition cancels an ongoing reassignment.
    ///
    /// Corresponds to `Admin.alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>>)`.
    fn alter_partition_reassignments(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
    ) -> AlterPartitionReassignmentsResult {
        self.alter_partition_reassignments_options(reassignments, AlterPartitionReassignmentsOptions::default())
    }

    /// Change the partition reassignments for the given partitions.
    ///
    /// A `None` value for a partition cancels an ongoing reassignment.
    ///
    /// Corresponds to
    /// `Admin.alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>>, AlterPartitionReassignmentsOptions)`.
    fn alter_partition_reassignments_options(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
        options: AlterPartitionReassignmentsOptions,
    ) -> AlterPartitionReassignmentsResult;

    /// List all the current partition reassignments.
    ///
    /// Corresponds to `Admin.listPartitionReassignments()`.
    fn list_partition_reassignments(&self) -> ListPartitionReassignmentsResult {
        self.list_partition_reassignments_options(ListPartitionReassignmentsOptions::default())
    }

    /// List the current partition reassignments for the given partitions.
    ///
    /// Corresponds to `Admin.listPartitionReassignments(Set<TopicPartition>)`.
    fn list_partition_reassignments_partitions(
        &self,
        partitions: HashSet<TopicPartition>,
    ) -> ListPartitionReassignmentsResult {
        self.list_partition_reassignments_partitions_options(
            Some(partitions),
            ListPartitionReassignmentsOptions::default(),
        )
    }

    /// List all the current partition reassignments, with options.
    ///
    /// Corresponds to `Admin.listPartitionReassignments(ListPartitionReassignmentsOptions)`,
    /// whose body passes `Optional.empty()` for the partition set.
    fn list_partition_reassignments_options(
        &self,
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult {
        self.list_partition_reassignments_partitions_options(None, options)
    }

    /// List the current partition reassignments, optionally restricted to a set
    /// of partitions (`None` lists all ongoing reassignments).
    ///
    /// Corresponds to
    /// `Admin.listPartitionReassignments(Optional<Set<TopicPartition>>, ListPartitionReassignmentsOptions)`.
    fn list_partition_reassignments_partitions_options(
        &self,
        partitions: Option<HashSet<TopicPartition>>,
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult;

    /// List the offsets for the given partitions and offset specifications.
    ///
    /// Corresponds to `Admin.listOffsets(Map<TopicPartition, OffsetSpec>)`.
    fn list_offsets(&self, topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>) -> ListOffsetsResult {
        self.list_offsets_options(topic_partition_offsets, ListOffsetsOptions::default())
    }

    /// List the offsets for the given partitions and offset specifications.
    ///
    /// Corresponds to `Admin.listOffsets(Map<TopicPartition, OffsetSpec>, ListOffsetsOptions)`.
    fn list_offsets_options(
        &self,
        topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ) -> ListOffsetsResult;

    /// List the groups available in the cluster.
    ///
    /// Corresponds to `Admin.listGroups()`.
    fn list_groups(&self) -> ListGroupsResult {
        self.list_groups_options(ListGroupsOptions::default())
    }

    /// List the groups available in the cluster.
    ///
    /// Corresponds to `Admin.listGroups(ListGroupsOptions)`.
    fn list_groups_options(&self, options: ListGroupsOptions) -> ListGroupsResult;

    /// List the consumer groups available in the cluster.
    ///
    /// Corresponds to `Admin.listConsumerGroups()`
    /// (deprecated since 4.1 in favor of [`list_groups`](Admin::list_groups)).
    #[allow(deprecated)]
    fn list_consumer_groups(&self) -> ListConsumerGroupsResult {
        self.list_consumer_groups_options(ListConsumerGroupsOptions::default())
    }

    /// List the consumer groups available in the cluster.
    ///
    /// Corresponds to `Admin.listConsumerGroups(ListConsumerGroupsOptions)`
    /// (deprecated since 4.1 in favor of [`list_groups`](Admin::list_groups)).
    #[allow(deprecated)]
    fn list_consumer_groups_options(&self, options: ListConsumerGroupsOptions) -> ListConsumerGroupsResult;

    /// Describe some consumer groups in the cluster.
    ///
    /// Corresponds to `Admin.describeConsumerGroups(Collection<String>)`.
    fn describe_consumer_groups(&self, group_ids: &[String]) -> DescribeConsumerGroupsResult {
        self.describe_consumer_groups_options(group_ids, DescribeConsumerGroupsOptions::default())
    }

    /// Describe some consumer groups in the cluster.
    ///
    /// Corresponds to `Admin.describeConsumerGroups(Collection<String>, DescribeConsumerGroupsOptions)`.
    fn describe_consumer_groups_options(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> DescribeConsumerGroupsResult;

    /// Describe some classic groups in the cluster.
    ///
    /// Corresponds to `Admin.describeClassicGroups(Collection<String>)`.
    fn describe_classic_groups(&self, group_ids: &[String]) -> DescribeClassicGroupsResult {
        self.describe_classic_groups_options(group_ids, DescribeClassicGroupsOptions::default())
    }

    /// Describe some classic groups in the cluster.
    ///
    /// Corresponds to `Admin.describeClassicGroups(Collection<String>, DescribeClassicGroupsOptions)`.
    fn describe_classic_groups_options(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> DescribeClassicGroupsResult;

    /// List the consumer group offsets available in the cluster for the given
    /// group.
    ///
    /// Corresponds to `Admin.listConsumerGroupOffsets(String)`.
    fn list_consumer_group_offsets_group_id(&self, group_id: &str) -> ListConsumerGroupOffsetsResult {
        self.list_consumer_group_offsets_group_id_options(group_id, ListConsumerGroupOffsetsOptions::default())
    }

    /// List the consumer group offsets available in the cluster for the given
    /// group, with options.
    ///
    /// Corresponds to `Admin.listConsumerGroupOffsets(String, ListConsumerGroupOffsetsOptions)`.
    fn list_consumer_group_offsets_group_id_options(
        &self,
        group_id: &str,
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult {
        let group_spec = ListConsumerGroupOffsetsSpec::new();

        // We can use the provided options with the batched API, which uses topic partitions from
        // the group spec and ignores any topic partitions set in the options.
        self.list_consumer_group_offsets_options(&HashMap::from([(group_id.to_string(), group_spec)]), options)
    }

    /// List the consumer group offsets available in the cluster for the given
    /// group specifications.
    ///
    /// Corresponds to `Admin.listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>)`.
    fn list_consumer_group_offsets(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
    ) -> ListConsumerGroupOffsetsResult {
        self.list_consumer_group_offsets_options(group_specs, ListConsumerGroupOffsetsOptions::default())
    }

    /// List the consumer group offsets available in the cluster for the given
    /// group specifications.
    ///
    /// Corresponds to
    /// `Admin.listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>, ListConsumerGroupOffsetsOptions)`.
    fn list_consumer_group_offsets_options(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult;

    /// Alter offsets for a consumer group.
    ///
    /// Corresponds to `Admin.alterConsumerGroupOffsets(String, Map<TopicPartition, OffsetAndMetadata>)`.
    fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> AlterConsumerGroupOffsetsResult {
        self.alter_consumer_group_offsets_options(group_id, offsets, AlterConsumerGroupOffsetsOptions::default())
    }

    /// Alter offsets for a consumer group.
    ///
    /// Corresponds to
    /// `Admin.alterConsumerGroupOffsets(String, Map<TopicPartition, OffsetAndMetadata>, AlterConsumerGroupOffsetsOptions)`.
    fn alter_consumer_group_offsets_options(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ) -> AlterConsumerGroupOffsetsResult;

    /// Delete offsets for a set of partitions in a consumer group.
    ///
    /// Corresponds to `Admin.deleteConsumerGroupOffsets(String, Set<TopicPartition>)`.
    fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
    ) -> DeleteConsumerGroupOffsetsResult {
        self.delete_consumer_group_offsets_options(group_id, partitions, DeleteConsumerGroupOffsetsOptions::default())
    }

    /// Delete offsets for a set of partitions in a consumer group.
    ///
    /// Corresponds to
    /// `Admin.deleteConsumerGroupOffsets(String, Set<TopicPartition>, DeleteConsumerGroupOffsetsOptions)`.
    fn delete_consumer_group_offsets_options(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> DeleteConsumerGroupOffsetsResult;

    /// Delete consumer groups from the cluster.
    ///
    /// Corresponds to `Admin.deleteConsumerGroups(Collection<String>)`.
    fn delete_consumer_groups(&self, group_ids: &[String]) -> DeleteConsumerGroupsResult {
        self.delete_consumer_groups_options(group_ids, DeleteConsumerGroupsOptions::default())
    }

    /// Delete consumer groups from the cluster.
    ///
    /// Corresponds to
    /// `Admin.deleteConsumerGroups(Collection<String>, DeleteConsumerGroupsOptions)`.
    fn delete_consumer_groups_options(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> DeleteConsumerGroupsResult;

    /// Remove members from a consumer group by given member identities.
    ///
    /// Corresponds to
    /// `Admin.removeMembersFromConsumerGroup(String, RemoveMembersFromConsumerGroupOptions)`.
    fn remove_members_from_consumer_group_options(
        &self,
        group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> RemoveMembersFromConsumerGroupResult;

    /// Create ACLs.
    ///
    /// Corresponds to `Admin.createAcls(Collection<AclBinding>)`.
    fn create_acls(&self, acls: &[AclBinding]) -> CreateAclsResult {
        self.create_acls_options(acls, CreateAclsOptions::default())
    }

    /// Create ACLs.
    ///
    /// Corresponds to `Admin.createAcls(Collection<AclBinding>, CreateAclsOptions)`.
    fn create_acls_options(&self, acls: &[AclBinding], options: CreateAclsOptions) -> CreateAclsResult;

    /// Describe ACLs matching the provided filter.
    ///
    /// Corresponds to `Admin.describeAcls(AclBindingFilter)`.
    fn describe_acls(&self, filter: &AclBindingFilter) -> DescribeAclsResult {
        self.describe_acls_options(filter, DescribeAclsOptions::default())
    }

    /// Describe ACLs matching the provided filter.
    ///
    /// Corresponds to `Admin.describeAcls(AclBindingFilter, DescribeAclsOptions)`.
    fn describe_acls_options(&self, filter: &AclBindingFilter, options: DescribeAclsOptions) -> DescribeAclsResult;

    /// Delete ACLs matching the provided filters.
    ///
    /// Corresponds to `Admin.deleteAcls(Collection<AclBindingFilter>)`.
    fn delete_acls(&self, filters: &[AclBindingFilter]) -> DeleteAclsResult {
        self.delete_acls_options(filters, DeleteAclsOptions::default())
    }

    /// Delete ACLs matching the provided filters.
    ///
    /// Corresponds to `Admin.deleteAcls(Collection<AclBindingFilter>, DeleteAclsOptions)`.
    fn delete_acls_options(&self, filters: &[AclBindingFilter], options: DeleteAclsOptions) -> DeleteAclsResult;

    /// Describe the client quotas matching the provided filter.
    ///
    /// Corresponds to `Admin.describeClientQuotas(ClientQuotaFilter)`.
    fn describe_client_quotas(&self, filter: &ClientQuotaFilter) -> DescribeClientQuotasResult {
        self.describe_client_quotas_options(filter, DescribeClientQuotasOptions::default())
    }

    /// Describe the client quotas matching the provided filter.
    ///
    /// Corresponds to
    /// `Admin.describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)`.
    fn describe_client_quotas_options(
        &self,
        filter: &ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> DescribeClientQuotasResult;

    /// Alter the client quotas of one or more quota entities.
    ///
    /// Corresponds to `Admin.alterClientQuotas(Collection<ClientQuotaAlteration>)`.
    fn alter_client_quotas(&self, entries: &[ClientQuotaAlteration]) -> AlterClientQuotasResult {
        self.alter_client_quotas_options(entries, AlterClientQuotasOptions::default())
    }

    /// Alter the client quotas of one or more quota entities.
    ///
    /// Corresponds to
    /// `Admin.alterClientQuotas(Collection<ClientQuotaAlteration>, AlterClientQuotasOptions)`.
    fn alter_client_quotas_options(
        &self,
        entries: &[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> AlterClientQuotasResult;

    /// Describe the SASL/SCRAM credentials for all users.
    ///
    /// Corresponds to `Admin.describeUserScramCredentials()`. Java's body
    /// passes a `null` user list; Rust spells "all users" as an empty slice —
    /// see [`describe_user_scram_credentials_options`](Admin::describe_user_scram_credentials_options).
    fn describe_user_scram_credentials(&self) -> DescribeUserScramCredentialsResult {
        self.describe_user_scram_credentials_options(&[], DescribeUserScramCredentialsOptions::default())
    }

    /// Describe the SASL/SCRAM credentials for the given users.
    ///
    /// Corresponds to `Admin.describeUserScramCredentials(List<String>)`.
    fn describe_user_scram_credentials_users(&self, users: &[String]) -> DescribeUserScramCredentialsResult {
        self.describe_user_scram_credentials_options(users, DescribeUserScramCredentialsOptions::default())
    }

    /// Describe all SASL/SCRAM credentials for the given users, or all users if
    /// `users` is empty.
    ///
    /// Corresponds to
    /// `Admin.describeUserScramCredentials(List<String>, DescribeUserScramCredentialsOptions)`.
    fn describe_user_scram_credentials_options(
        &self,
        users: &[String],
        options: DescribeUserScramCredentialsOptions,
    ) -> DescribeUserScramCredentialsResult;

    /// Alter (upsert / delete) SASL/SCRAM credentials for one or more users.
    ///
    /// Corresponds to `Admin.alterUserScramCredentials(List<UserScramCredentialAlteration>)`.
    fn alter_user_scram_credentials(
        &self,
        alterations: &[UserScramCredentialAlteration],
    ) -> AlterUserScramCredentialsResult {
        self.alter_user_scram_credentials_options(alterations, AlterUserScramCredentialsOptions::default())
    }

    /// Alter (upsert / delete) SASL/SCRAM credentials for one or more users.
    ///
    /// Corresponds to
    /// `Admin.alterUserScramCredentials(List<UserScramCredentialAlteration>, AlterUserScramCredentialsOptions)`.
    fn alter_user_scram_credentials_options(
        &self,
        alterations: &[UserScramCredentialAlteration],
        options: AlterUserScramCredentialsOptions,
    ) -> AlterUserScramCredentialsResult;

    /// Create a delegation token.
    ///
    /// Corresponds to `Admin.createDelegationToken()`.
    fn create_delegation_token(&self) -> CreateDelegationTokenResult {
        self.create_delegation_token_options(CreateDelegationTokenOptions::default())
    }

    /// Create a delegation token.
    ///
    /// Corresponds to
    /// `Admin.createDelegationToken(CreateDelegationTokenOptions)`.
    fn create_delegation_token_options(&self, options: CreateDelegationTokenOptions) -> CreateDelegationTokenResult;

    /// Renew a delegation token identified by its HMAC.
    ///
    /// Corresponds to `Admin.renewDelegationToken(byte[])`.
    fn renew_delegation_token(&self, hmac: &[u8]) -> RenewDelegationTokenResult {
        self.renew_delegation_token_options(hmac, RenewDelegationTokenOptions::default())
    }

    /// Renew a delegation token identified by its HMAC.
    ///
    /// Corresponds to
    /// `Admin.renewDelegationToken(byte[], RenewDelegationTokenOptions)`.
    fn renew_delegation_token_options(
        &self,
        hmac: &[u8],
        options: RenewDelegationTokenOptions,
    ) -> RenewDelegationTokenResult;

    /// Expire a delegation token identified by its HMAC.
    ///
    /// Corresponds to `Admin.expireDelegationToken(byte[])`.
    fn expire_delegation_token(&self, hmac: &[u8]) -> ExpireDelegationTokenResult {
        self.expire_delegation_token_options(hmac, ExpireDelegationTokenOptions::default())
    }

    /// Expire a delegation token identified by its HMAC.
    ///
    /// Corresponds to
    /// `Admin.expireDelegationToken(byte[], ExpireDelegationTokenOptions)`.
    fn expire_delegation_token_options(
        &self,
        hmac: &[u8],
        options: ExpireDelegationTokenOptions,
    ) -> ExpireDelegationTokenResult;

    /// Describe the delegation tokens matching the provided owners filter.
    ///
    /// Corresponds to `Admin.describeDelegationToken()`.
    fn describe_delegation_token(&self) -> DescribeDelegationTokenResult {
        self.describe_delegation_token_options(DescribeDelegationTokenOptions::default())
    }

    /// Describe the delegation tokens matching the provided owners filter.
    ///
    /// Corresponds to
    /// `Admin.describeDelegationToken(DescribeDelegationTokenOptions)`.
    fn describe_delegation_token_options(
        &self,
        options: DescribeDelegationTokenOptions,
    ) -> DescribeDelegationTokenResult;

    /// Describe the finalized and supported features of the cluster.
    ///
    /// Corresponds to `Admin.describeFeatures()`.
    fn describe_features(&self) -> DescribeFeaturesResult {
        self.describe_features_options(DescribeFeaturesOptions::default())
    }

    /// Describe the finalized and supported features of the cluster.
    ///
    /// Corresponds to `Admin.describeFeatures(DescribeFeaturesOptions)`.
    fn describe_features_options(&self, options: DescribeFeaturesOptions) -> DescribeFeaturesResult;

    /// Apply the given feature updates.
    ///
    /// Corresponds to `Admin.updateFeatures(Map<String, FeatureUpdate>)`.
    fn update_features(&self, feature_updates: &HashMap<String, FeatureUpdate>) -> Result<UpdateFeaturesResult, Error> {
        self.update_features_options(feature_updates, UpdateFeaturesOptions::default())
    }

    /// Apply the given feature updates.
    ///
    /// Corresponds to
    /// `Admin.updateFeatures(Map<String, FeatureUpdate>, UpdateFeaturesOptions)`.
    ///
    /// Java throws `IllegalArgumentException` synchronously when the update map
    /// is empty or contains a blank feature name; per CLAUDE.md §10.2, that
    /// unchecked-but-recoverable throw becomes an `Err` here (the only admin
    /// RPC whose client-side validation can fail before the `Call` is
    /// enqueued).
    ///
    /// # Errors
    ///
    /// Returns [`Error::local_illegal_argument`] if `feature_updates` is empty or
    /// any feature name is blank.
    fn update_features_options(
        &self,
        feature_updates: &HashMap<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ) -> Result<UpdateFeaturesResult, Error>;

    /// Close the admin client, awaiting the background task to finish its
    /// in-flight work.
    ///
    /// Corresponds to `Admin.close()`, whose body waits `Long.MAX_VALUE`
    /// milliseconds; [`close_timeout`](Admin::close_timeout) clamps that to the
    /// same 365-day ceiling Java's `close(Duration)` applies.
    async fn close(&self) {
        self.close_timeout(Duration::from_millis(i64::MAX as u64)).await
    }

    /// Close the admin client, awaiting the background task to finish
    /// in-flight work up to `timeout`.
    ///
    /// The bound is a guarantee, not a hint: this returns once `timeout` has
    /// elapsed whatever the background task is doing, mirroring the timed
    /// `thread.join(waitTimeMs)` that ends Java's `close`. A task that has not
    /// finished by then is left running.
    ///
    /// Corresponds to `Admin.close(Duration)`; blocking in Java, so `async` in
    /// Rust (CLAUDE.md §9.4).
    async fn close_timeout(&self, timeout: Duration);
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
pub fn new_admin_client(config: AdminClientConfig) -> Result<Box<dyn Admin>, Error> {
    Ok(Box::new(KafkaAdminClient::from_config(config)?))
}

#[cfg(test)]
mod tests {
    //! Coverage for the `Admin` trait's `default` bodies — the Rust
    //! translations of Java's `default` convenience overloads
    //! (`Admin.java`). Each test asserts a convenience form agrees with the
    //! form it forwards to, exactly as the Java one-line body does.

    use std::collections::{HashMap, HashSet};
    use std::time::Duration;

    use super::*;
    use crate::admin::MockAdminClient;
    use crate::common::{Node, TopicPartition, TopicPartitionInfo};

    fn admin() -> MockAdminClient {
        let mock = MockAdminClient::create(1).expect("num_brokers is at least 1");
        // The mock seeds brokers as `localhost:1000 + id` (MockAdminClient.java:143).
        let leader = Node::new(0, "localhost".to_string(), 1000);
        mock.add_topic(
            false,
            "topic",
            vec![TopicPartitionInfo::new_elr_last_known_elr(
                0,
                Some(leader.clone()),
                vec![leader.clone()],
                vec![leader],
                Vec::new(),
                Vec::new(),
            )],
            None,
        )
        .expect("the seeded broker is known");
        mock
    }

    /// Representative of the 42 generated no-options forwards: the convenience
    /// form must behave as `create_topics_options(.., CreateTopicsOptions::default())`
    /// does, mirroring `Admin.java:180`.
    #[tokio::test]
    async fn create_topics_forwards_to_the_options_form() {
        let mock = admin();
        mock.create_topics(&[NewTopic::new_num_partitions_replication_factor(
            "created",
            Some(1),
            Some(1),
        )])
        .all()
        .get()
        .await
        .expect("the topic is created");

        let via_options = admin();
        via_options
            .create_topics_options(
                &[NewTopic::new_num_partitions_replication_factor(
                    "created",
                    Some(1),
                    Some(1),
                )],
                CreateTopicsOptions::default(),
            )
            .all()
            .get()
            .await
            .expect("the topic is created");

        assert_eq!(
            mock.list_topics().names().get().await.unwrap(),
            via_options
                .list_topics_options(ListTopicsOptions::default())
                .names()
                .get()
                .await
                .unwrap()
        );
    }

    /// `Admin.listTopics()` — the zero-argument shape of the same forward.
    #[tokio::test]
    async fn list_topics_forwards_to_the_options_form() {
        let mock = admin();
        assert_eq!(
            mock.list_topics().names().get().await.unwrap(),
            mock.list_topics_options(ListTopicsOptions::default())
                .names()
                .get()
                .await
                .unwrap()
        );
        assert_eq!(
            mock.list_topics().names().get().await.unwrap(),
            HashSet::from(["topic".to_string()])
        );
    }

    /// `Admin.describeTopics(Collection<String>)` `:295` and
    /// `describeTopics(Collection<String>, DescribeTopicsOptions)` `:306` — the
    /// latter wraps the names in a `TopicCollection`, so both must agree with
    /// the `TopicCollection` form.
    #[tokio::test]
    async fn describe_topics_by_name_wraps_the_names_in_a_topic_collection() {
        let mock = admin();
        let names = ["topic".to_string()];

        let expected = mock
            .describe_topics(TopicCollection::of_topic_names(names.to_vec()))
            .all_topic_names()
            .expect("described by name")
            .get()
            .await
            .expect("the topic exists");

        for described in [
            mock.describe_topics_topic_names(&names),
            mock.describe_topics_topic_names_options(&names, DescribeTopicsOptions::default()),
        ] {
            let actual = described
                .all_topic_names()
                .expect("described by name")
                .get()
                .await
                .expect("the topic exists");
            assert_eq!(actual, expected);
        }
    }

    /// `Admin.listConfigResources()` `:1812` passes `Set.of()` — "every
    /// supported type" — not an empty result.
    #[tokio::test]
    async fn list_config_resources_asks_for_every_type() {
        let mock = admin();
        let mut convenience = mock.list_config_resources().all().get().await.unwrap();
        let mut explicit = mock
            .list_config_resources_options(&HashSet::new(), ListConfigResourcesOptions::default())
            .all()
            .get()
            .await
            .unwrap();
        convenience.sort_by_key(|resource| (resource.resource_type() as i32, resource.name().to_string()));
        explicit.sort_by_key(|resource| (resource.resource_type() as i32, resource.name().to_string()));

        assert_eq!(convenience, explicit);
        assert!(
            convenience
                .iter()
                .any(|resource| resource.resource_type() == ConfigResourceType::Topic),
            "an empty type set must not mean an empty result: {convenience:?}"
        );
    }

    /// `Admin.describeUserScramCredentials()` `:1434` passes a `null` user list,
    /// which Rust spells as an empty slice; `:1447` passes the caller's list.
    /// The mock rejects both alike (`MockAdminClient.java:1251`), so the
    /// assertion is on the error message the forward propagates.
    #[tokio::test]
    async fn describe_user_scram_credentials_forwards_both_shapes() {
        let mock = admin();
        for described in [
            mock.describe_user_scram_credentials(),
            mock.describe_user_scram_credentials_users(&["user".to_string()]),
            mock.describe_user_scram_credentials_options(&[], DescribeUserScramCredentialsOptions::default()),
        ] {
            let error = described.all().get().await.expect_err("the mock does not implement this");
            assert_eq!(error.message(), "Not implemented yet");
        }
    }

    /// `Admin.listPartitionReassignments()` `:1193`, `(Set)` `:1203` and
    /// `(options)` `:1248`. The last passes `Optional.empty()`, so it must agree
    /// with the no-argument form rather than with an empty partition set.
    #[tokio::test]
    async fn list_partition_reassignments_forwards_every_shape() {
        let mock = admin();
        let partition = TopicPartition::new("topic", 0);

        let all = mock.list_partition_reassignments().reassignments().get().await.unwrap();
        let all_with_options = mock
            .list_partition_reassignments_options(ListPartitionReassignmentsOptions::default())
            .reassignments()
            .get()
            .await
            .unwrap();
        let selected = mock
            .list_partition_reassignments_partitions(HashSet::from([partition.clone()]))
            .reassignments()
            .get()
            .await
            .unwrap();
        let selected_with_options = mock
            .list_partition_reassignments_partitions_options(
                Some(HashSet::from([partition])),
                ListPartitionReassignmentsOptions::default(),
            )
            .reassignments()
            .get()
            .await
            .unwrap();

        assert_eq!(all, all_with_options);
        assert_eq!(selected, selected_with_options);
        // Nothing is reassigning in a freshly seeded mock, so both are empty —
        // what matters is that `(options)` took the `None` branch, i.e. did not
        // request the empty *set*, which the mock would answer identically here
        // but which diverges as soon as a reassignment exists.
        assert!(all.is_empty());
    }

    /// `Admin.listConsumerGroupOffsets(String)` `:912` and
    /// `(String, options)` `:928` — the latter builds a singleton map with a
    /// default `ListConsumerGroupOffsetsSpec`, i.e. "all partitions".
    #[tokio::test]
    async fn list_consumer_group_offsets_by_group_id_builds_a_singleton_spec() {
        let mock = admin();
        let partition = TopicPartition::new("topic", 0);
        mock.update_consumer_group_offsets(HashMap::from([(partition.clone(), 17)]));

        let expected = mock
            .list_consumer_group_offsets(&HashMap::from([("group".to_string(), ListConsumerGroupOffsetsSpec::new())]))
            .partitions_to_offset_and_metadata()
            .expect("exactly one group")
            .get()
            .await
            .expect("offsets");

        for listed in [
            mock.list_consumer_group_offsets_group_id("group"),
            mock.list_consumer_group_offsets_group_id_options("group", ListConsumerGroupOffsetsOptions::default()),
        ] {
            let actual = listed
                .partitions_to_offset_and_metadata()
                .expect("exactly one group")
                .get()
                .await
                .expect("offsets");
            assert_eq!(actual, expected);
            assert_eq!(actual[&partition].as_ref().expect("committed").offset(), 17);
        }
    }

    /// `Admin.close()` `:153` waits `Long.MAX_VALUE` milliseconds; the forward
    /// must not overflow the `Duration` conversion.
    #[tokio::test]
    async fn close_forwards_to_the_timeout_form() {
        admin().close().await;
        admin().close_timeout(Duration::from_millis(i64::MAX as u64)).await;
    }
}
