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
pub mod alter_config_op;
pub mod alter_configs_result;
pub mod alter_replica_log_dirs_result;
pub mod config;
pub mod config_entry;
pub mod create_partitions_result;
pub mod create_topics_result;
pub mod delete_records_result;
pub mod delete_topics_result;
pub mod deleted_records;
pub mod describe_cluster_result;
pub mod describe_configs_result;
pub mod describe_log_dirs_result;
pub mod describe_replica_log_dirs_result;
pub mod describe_topics_result;
pub mod kafka_admin_client;
pub mod list_config_resources_result;
pub mod list_topics_result;
pub mod log_dir_description;
pub mod mock_admin_client;
pub mod new_partitions;
pub mod new_topic;
pub mod options;
pub mod records_to_delete;
pub mod replica_info;
pub mod topic_description;
pub mod topic_listing;

pub(crate) mod internals;

pub use admin_client_config::AdminClientConfig;
pub use alter_config_op::{AlterConfigOp, OpType};
pub use alter_configs_result::AlterConfigsResult;
pub use alter_replica_log_dirs_result::AlterReplicaLogDirsResult;
pub use config::Config;
pub use config_entry::{ConfigEntry, ConfigSource, ConfigSynonym, ConfigType};
use std::collections::HashMap;

pub use create_partitions_result::CreatePartitionsResult;
pub use create_topics_result::{CreateTopicsResult, TopicMetadataAndConfig};
pub use delete_records_result::DeleteRecordsResult;
pub use delete_topics_result::DeleteTopicsResult;
pub use deleted_records::DeletedRecords;
pub use describe_cluster_result::DescribeClusterResult;
pub use describe_configs_result::DescribeConfigsResult;
pub use describe_log_dirs_result::DescribeLogDirsResult;
pub use describe_replica_log_dirs_result::{DescribeReplicaLogDirsResult, ReplicaLogDirInfo};
pub use describe_topics_result::DescribeTopicsResult;
pub use kafka_admin_client::KafkaAdminClient;
pub use list_config_resources_result::ListConfigResourcesResult;
pub use list_topics_result::ListTopicsResult;
pub use log_dir_description::LogDirDescription;
pub use mock_admin_client::MockAdminClient;
pub use new_partitions::NewPartitions;
pub use new_topic::NewTopic;
pub use options::{
    AlterConfigsOptions, AlterReplicaLogDirsOptions, CreatePartitionsOptions, CreateTopicsOptions,
    DeleteRecordsOptions, DeleteTopicsOptions, DescribeClusterOptions, DescribeConfigsOptions, DescribeLogDirsOptions,
    DescribeReplicaLogDirsOptions, DescribeTopicsOptions, ListConfigResourcesOptions, ListTopicsOptions,
};
pub use records_to_delete::RecordsToDelete;
pub use replica_info::ReplicaInfo;
pub use topic_description::TopicDescription;
pub use topic_listing::TopicListing;

use std::time::Duration;

use async_trait::async_trait;

use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::{KafkaError, TopicCollection, TopicPartition, TopicPartitionReplica};

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
