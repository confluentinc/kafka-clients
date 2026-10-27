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

mod abort_transaction_result;
mod abort_transaction_spec;
mod admin_client;
mod admin_client_config;
mod alter_client_quotas_result;
pub mod alter_config_op;
mod alter_configs_result;
mod alter_consumer_group_offsets_result;
mod alter_partition_reassignments_result;
mod alter_replica_log_dirs_result;
mod alter_user_scram_credentials_result;
mod classic_group_description;
mod config;
pub mod config_entry;
mod consumer_group_description;
mod create_acls_result;
mod create_delegation_token_result;
mod create_partitions_result;
pub mod create_topics_result;
pub mod delete_acls_result;
mod delete_consumer_group_offsets_result;
mod delete_consumer_groups_result;
mod delete_records_result;
mod delete_topics_result;
mod deleted_records;
mod describe_acls_result;
mod describe_classic_groups_result;
mod describe_client_quotas_result;
mod describe_cluster_result;
mod describe_configs_result;
mod describe_consumer_groups_result;
mod describe_delegation_token_result;
mod describe_features_result;
mod describe_log_dirs_result;
pub mod describe_producers_result;
pub mod describe_replica_log_dirs_result;
mod describe_topics_result;
mod describe_transactions_result;
mod describe_user_scram_credentials_result;
mod elect_leaders_result;
mod endpoint_type;
mod expire_delegation_token_result;
mod feature_metadata;
pub mod feature_update;
mod fence_producers_result;
mod finalized_version_range;
mod group_listing;
mod kafka_admin_client;
mod list_config_resources_result;
mod list_consumer_group_offsets_result;
mod list_consumer_group_offsets_spec;
mod list_groups_result;
pub mod list_offsets_result;
mod list_partition_reassignments_result;
mod list_topics_result;
mod list_transactions_result;
mod log_dir_description;
mod member_assignment;
mod member_description;
mod member_to_remove;
pub mod mock_admin_client;
mod new_partition_reassignment;
mod new_partitions;
mod new_topic;
pub mod offset_spec;
pub mod options;
mod partition_reassignment;
mod producer_state;
mod records_to_delete;
mod remove_members_from_consumer_group_result;
mod renew_delegation_token_result;
mod replica_info;
mod scram_credential_info;
mod scram_mechanism;
mod supported_version_range;
mod terminate_transaction_result;
mod topic_description;
mod topic_listing;
mod transaction_description;
mod transaction_listing;
mod transaction_state;
mod unregister_controller_result;
mod update_features_result;
mod user_scram_credential_alteration;
mod user_scram_credential_deletion;
mod user_scram_credential_upsertion;
mod user_scram_credentials_description;

pub(crate) mod internals;

pub use abort_transaction_result::AbortTransactionResult;
pub use abort_transaction_spec::AbortTransactionSpec;
pub use admin_client::AdminClient;
pub use admin_client_config::AdminClientConfig;
pub use alter_client_quotas_result::AlterClientQuotasResult;
pub use alter_config_op::{AlterConfigOp, OpType};
pub use alter_configs_result::AlterConfigsResult;
pub use alter_consumer_group_offsets_result::AlterConsumerGroupOffsetsResult;
pub use alter_partition_reassignments_result::AlterPartitionReassignmentsResult;
pub use alter_replica_log_dirs_result::AlterReplicaLogDirsResult;
pub use alter_user_scram_credentials_result::AlterUserScramCredentialsResult;
pub use classic_group_description::ClassicGroupDescription;
pub use config::Config;
// `ConfigSource`, `ConfigSynonym` and `ConfigType` are NOT re-exported here:
// they are nested classes of Java's `ConfigEntry`,
// so per CLAUDE.md §2 they are reached through the file module that plays the
// outer-class qualifier — `admin::config_entry::ConfigSource`, mirroring Java's
// `ConfigEntry.ConfigSource`. `ConfigEntryOptions` / `ConfigEntryOptionsBuilder`
// are CLAUDE.md-invented, not Java nested classes, so they stay flat.
pub use config_entry::{ConfigEntry, ConfigEntryOptions, ConfigEntryOptionsBuilder};
pub use consumer_group_description::ConsumerGroupDescription;
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
pub use endpoint_type::EndpointType;
pub use expire_delegation_token_result::ExpireDelegationTokenResult;
pub use feature_metadata::FeatureMetadata;
pub use feature_update::{FeatureUpdate, UpgradeType};
pub use fence_producers_result::FenceProducersResult;
pub use finalized_version_range::FinalizedVersionRange;
pub use group_listing::GroupListing;
pub use kafka_admin_client::KafkaAdminClient;
pub use list_config_resources_result::ListConfigResourcesResult;
pub(crate) use list_consumer_group_offsets_result::GroupOffsets;
pub use list_consumer_group_offsets_result::ListConsumerGroupOffsetsResult;
pub use list_consumer_group_offsets_spec::ListConsumerGroupOffsetsSpec;
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
    TerminateTransactionOptions, UnregisterControllerOptions, UpdateFeaturesOptions,
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
pub use unregister_controller_result::UnregisterControllerResult;
pub use update_features_result::UpdateFeaturesResult;
pub use user_scram_credential_alteration::UserScramCredentialAlteration;
pub use user_scram_credential_deletion::UserScramCredentialDeletion;
pub use user_scram_credential_upsertion::UserScramCredentialUpsertion;
pub use user_scram_credentials_description::UserScramCredentialsDescription;

use std::time::Duration;

use async_trait::async_trait;

use crate::common::acl::{AclBinding, AclBindingFilter};
use crate::common::config::{ConfigResource, config_resource};
use crate::common::quota::{ClientQuotaAlteration, ClientQuotaFilter};
use crate::common::{ElectionType, Error, TopicCollection, TopicPartition, TopicPartitionReplica};
use crate::consumer::OffsetAndMetadata;

use std::collections::HashSet;

/// The administrative client for Kafka, which supports managing and inspecting topics, brokers,
/// configurations and ACLs.
///
/// Instances returned from `AdminClient::create` are guaranteed to be thread safe. However, the
/// `KafkaFuture`s returned from request methods are completed by a single task so it is important
/// that any code which executes on that task when they complete (using `KafkaFuture::then_apply`,
/// for example) doesn't block for too long. If necessary, processing of results should be passed
/// to another task.
///
/// The operations exposed by Admin follow a consistent pattern:
///
/// - Admin instances should be created using `AdminClient::create`.
/// - Each operation typically has two overloaded methods, one which uses a default set of options
///   and an overloaded method where the last parameter is an explicit options object.
/// - The operation method's first parameter is a collection of items to perform the operation on.
///   Batching multiple requests into a single call is more efficient and should be preferred over
///   multiple calls to the same method.
/// - The operation methods execute asynchronously.
/// - Each `xxx` operation method returns an `XxxResult` class with methods which expose
///   `KafkaFuture` for accessing the result(s) of the operation.
/// - Typically an `all()` method is provided for getting the overall success/failure of the batch
///   and a `values()` method provided access to each item in a request batch. Other methods may
///   also be provided.
/// - For synchronous behavior, await `KafkaFuture::get`.
///
/// Here is a simple example of using an Admin client instance to create a new topic:
///
/// ```no_run
/// use std::collections::{BTreeMap, HashMap};
///
/// use confluent_kafka::admin::{AdminClient, AdminClientConfig, NewTopic};
///
/// # async fn example() -> Result<(), confluent_kafka::common::Error> {
/// let props = HashMap::from([(
///     AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
///     "localhost:9092".to_string(),
/// )]);
/// let admin = AdminClient::create(AdminClientConfig::new(&props)?)?;
/// let topic_name = "my-topic";
/// // Create a compacted topic
/// let result = admin.create_topics(&[NewTopic::with_num_partitions_replication_factor(
///     topic_name,
///     Some(12),
///     Some(3),
/// )
/// .set_configs(BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]))]);
///
/// // Call values() to get the result for a specific topic
/// let future = result.values()[topic_name].clone();
///
/// // Await get() until the topic creation is complete or has failed
/// future.get().await?;
/// admin.close().await;
/// # Ok(())
/// # }
/// ```
///
/// # Bootstrap and balancing
///
/// The `bootstrap.servers` config passed to `AdminClient::create` is only used for discovering
/// the brokers in the cluster, which the client will then connect to as needed. As such, it is
/// sufficient to include only two or three broker addresses to cope with the possibility of
/// brokers being unavailable.
///
/// Different operations necessitate requests being sent to different nodes in the cluster. For
/// example `Self::create_topics` communicates with the controller, but
/// `Self::describe_topics_with_topic_names` can talk to any broker. When the recipient does not
/// matter the instance will try to use the broker with the fewest outstanding requests.
///
/// The client will transparently retry certain errors which are usually transient. For example
/// if the request for `create_topics()` get sent to a node which was not the controller the
/// metadata would be refreshed and the request re-sent to the controller.
///
/// # Broker Compatibility
///
/// The minimum broker version required is 0.10.0.0. Methods with stricter requirements will
/// specify the minimum broker version required.
///
/// # Translation notes
///
/// The request methods are plain `fn`s that return at once; only `close` and
/// `close_with_timeout` are `async`, as Java's `close` joins the admin thread.
#[async_trait]
#[doc(alias = "org.apache.kafka.clients.admin.Admin")]
pub trait Admin: Send + Sync {
    /// Create a batch of new topics with the default options.
    ///
    /// This is a convenience method for `Self::create_topics_with_options` with default options.
    /// See the overload for more details.
    ///
    /// This operation is supported by brokers with version 0.10.1.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `new_topics` - The new topics to create.
    ///
    /// # Returns
    ///
    /// The CreateTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#createTopics")]
    fn create_topics(&self, new_topics: &[NewTopic]) -> CreateTopicsResult {
        self.create_topics_with_options(new_topics, CreateTopicsOptions::default())
    }

    /// Create a batch of new topics.
    ///
    /// This operation is not transactional so it may succeed for some topics while fail for others.
    ///
    /// It may take several seconds after `CreateTopicsResult` returns success for all the brokers
    /// to become aware that the topics have been created. During this time, `Self::list_topics` and
    /// `Self::describe_topics_with_topic_names` may not return information about the new topics.
    ///
    /// This operation is supported by brokers with version 0.10.1.0 or higher. The validateOnly
    /// option is supported from version 0.10.2.0.
    ///
    /// # Arguments
    ///
    /// - `new_topics` - The new topics to create.
    /// - `options` - The options to use when creating the new topics.
    ///
    /// # Returns
    ///
    /// The CreateTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#createTopics")]
    fn create_topics_with_options(&self, new_topics: &[NewTopic], options: CreateTopicsOptions) -> CreateTopicsResult;

    /// This is a convenience method for `Self::delete_topics_with_options` with default options.
    /// See the overload for more details.
    ///
    /// When using topic IDs, this operation is supported by brokers with inter-broker protocol 2.8
    /// or higher. When using topic names, this operation is supported by brokers with version
    /// 0.10.1.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `topics` - The topics to delete.
    ///
    /// # Returns
    ///
    /// The DeleteTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteTopics")]
    fn delete_topics(&self, topics: TopicCollection) -> DeleteTopicsResult {
        self.delete_topics_with_options(topics, DeleteTopicsOptions::default())
    }

    /// Delete a batch of topics.
    ///
    /// This operation is not transactional so it may succeed for some topics while fail for others.
    ///
    /// It may take several seconds after the `DeleteTopicsResult` returns success for all the
    /// brokers to become aware that the topics are gone. During this time, `Self::list_topics` and
    /// `Self::describe_topics_with_topic_names` may continue to return information about the
    /// deleted topics.
    ///
    /// If delete.topic.enable is set to false on the brokers, an error will be returned to the
    /// client indicating that topic deletion is disabled.
    ///
    /// When using topic IDs, this operation is supported by brokers with inter-broker protocol 2.8
    /// or higher. When using topic names, this operation is supported by brokers with version
    /// 0.10.1.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `topics` - The topics to delete.
    /// - `options` - The options to use when deleting the topics.
    ///
    /// # Returns
    ///
    /// The DeleteTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteTopics")]
    fn delete_topics_with_options(&self, topics: TopicCollection, options: DeleteTopicsOptions) -> DeleteTopicsResult;

    /// List the topics available in the cluster with the default options.
    ///
    /// This is a convenience method for `Self::list_topics_with_options` with default options. See
    /// the overload for more details.
    ///
    /// # Returns
    ///
    /// The ListTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listTopics")]
    fn list_topics(&self) -> ListTopicsResult {
        self.list_topics_with_options(ListTopicsOptions::default())
    }

    /// List the topics available in the cluster.
    ///
    /// # Arguments
    ///
    /// - `options` - The options to use when listing the topics.
    ///
    /// # Returns
    ///
    /// The ListTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listTopics")]
    fn list_topics_with_options(&self, options: ListTopicsOptions) -> ListTopicsResult;

    /// Describe some topics in the cluster, with the default options.
    ///
    /// This is a convenience method for `Self::describe_topics_with_topic_names_options` with
    /// default options. See the overload for more details.
    ///
    /// # Arguments
    ///
    /// - `topic_names` - The names of the topics to describe.
    ///
    /// # Returns
    ///
    /// The DescribeTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeTopics")]
    fn describe_topics_with_topic_names(&self, topic_names: &[String]) -> DescribeTopicsResult {
        self.describe_topics_with_topic_names_options(topic_names, DescribeTopicsOptions::default())
    }

    /// Describe some topics in the cluster.
    ///
    /// # Arguments
    ///
    /// - `topic_names` - The names of the topics to describe.
    /// - `options` - The options to use when describing the topic.
    ///
    /// # Returns
    ///
    /// The DescribeTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeTopics")]
    fn describe_topics_with_topic_names_options(
        &self,
        topic_names: &[String],
        options: DescribeTopicsOptions,
    ) -> DescribeTopicsResult {
        self.describe_topics_with_topics_options(TopicCollection::of_topic_names(topic_names.to_vec()), options)
    }

    /// This is a convenience method for `Self::describe_topics_with_topics_options` with default
    /// options. See the overload for more details.
    ///
    /// When using topic IDs, this operation is supported by brokers with version 3.1.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `topics` - The topics to describe.
    ///
    /// # Returns
    ///
    /// The DescribeTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeTopics")]
    fn describe_topics_with_topics(&self, topics: TopicCollection) -> DescribeTopicsResult {
        self.describe_topics_with_topics_options(topics, DescribeTopicsOptions::default())
    }

    /// Describe some topics in the cluster. When using topic IDs, this operation is supported by
    /// brokers with version 3.1.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `topics` - The topics to describe.
    /// - `options` - The options to use when describing the topics.
    ///
    /// # Returns
    ///
    /// The DescribeTopicsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeTopics")]
    fn describe_topics_with_topics_options(
        &self,
        topics: TopicCollection,
        options: DescribeTopicsOptions,
    ) -> DescribeTopicsResult;

    /// Increase the number of partitions of the topics given as the keys of `new_partitions`
    /// according to the corresponding values. **If partitions are increased for a topic that has a
    /// key, the partition logic or ordering of the messages will be affected.**
    ///
    /// This is a convenience method for `Self::create_partitions_with_options` with default
    /// options. See the overload for more details.
    ///
    /// # Arguments
    ///
    /// - `new_partitions` - The topics which should have new partitions created, and corresponding
    ///   parameters for the created partitions.
    ///
    /// # Returns
    ///
    /// The CreatePartitionsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#createPartitions")]
    fn create_partitions(&self, new_partitions: &HashMap<String, NewPartitions>) -> CreatePartitionsResult {
        self.create_partitions_with_options(new_partitions, CreatePartitionsOptions::default())
    }

    /// Increase the number of partitions of the topics given as the keys of `new_partitions`
    /// according to the corresponding values. **If partitions are increased for a topic that has a
    /// key, the partition logic or ordering of the messages will be affected.**
    ///
    /// This operation is not transactional so it may succeed for some topics while fail for others.
    ///
    /// It may take several seconds after this method returns success for all the brokers to become
    /// aware that the partitions have been created. During this time,
    /// `Self::describe_topics_with_topic_names` may not return information about the new
    /// partitions.
    ///
    /// This operation is supported by brokers with version 1.0.0 or higher.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the values() method of the returned `CreatePartitionsResult`
    ///
    /// - `Error::is_authorization_error` if the authenticated user is not authorized to alter the
    ///   topic
    /// - `Error::is_timeout_error` if the request was not completed in within the given
    ///   `CreatePartitionsOptions::timeout_ms`.
    /// - `Error::is_reassignment_in_progress_error` if a partition reassignment is currently in
    ///   progress
    /// - `Error::is_broker_not_available_error` if the requested `NewPartitions::assignments`
    ///   contain a broker that is currently unavailable.
    /// - `Error::is_invalid_replication_factor_error` if no `NewPartitions::assignments` are given
    ///   and it is impossible for the broker to assign replicas with the topics replication factor.
    /// - Subclasses of `Error::is_kafka_error` if the request is invalid in some way.
    /// - `Error::is_invalid_partitions_error` if the requested partition count is less than or
    ///   equal to the current partition count.
    ///
    /// # Arguments
    ///
    /// - `new_partitions` - The topics which should have new partitions created, and corresponding
    ///   parameters for the created partitions.
    /// - `options` - The options to use when creating the new partitions.
    ///
    /// # Returns
    ///
    /// The CreatePartitionsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#createPartitions")]
    fn create_partitions_with_options(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> CreatePartitionsResult;

    /// Delete records whose offset is smaller than the given offset of the corresponding partition.
    ///
    /// This is a convenience method for `Self::delete_records_with_options` with default options.
    /// See the overload for more details.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `records_to_delete` - The topic partitions and related offsets from which records deletion
    ///   starts.
    ///
    /// # Returns
    ///
    /// The DeleteRecordsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteRecords")]
    fn delete_records(&self, records_to_delete: &HashMap<TopicPartition, RecordsToDelete>) -> DeleteRecordsResult {
        self.delete_records_with_options(records_to_delete, DeleteRecordsOptions::default())
    }

    /// Delete records whose offset is smaller than the given offset of the corresponding partition.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `records_to_delete` - The topic partitions and related offsets from which records deletion
    ///   starts.
    /// - `options` - The options to use when deleting records.
    ///
    /// # Returns
    ///
    /// The DeleteRecordsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteRecords")]
    fn delete_records_with_options(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> DeleteRecordsResult;

    /// Unregister a controller. This is a convenience method for
    /// `Self::unregister_controller_with_options`
    ///
    /// # Arguments
    ///
    /// - `controller_id` - the controller id to unregister.
    ///
    /// # Returns
    ///
    /// the `UnregisterControllerResult` containing the result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#unregisterController")]
    fn unregister_controller(&self, controller_id: i32) -> UnregisterControllerResult {
        self.unregister_controller_with_options(controller_id, UnregisterControllerOptions::default())
    }

    /// Unregister a controller. The following errors can be anticipated when calling `get()` on the
    /// future from the returned `UnregisterControllerResult`:
    ///
    /// - `Error::is_timeout_error` If the request timed out before the unregister operation could
    ///   finish.
    /// - `Error::is_unsupported_version_error` If the software is too old to support the
    ///   unregistration API.
    /// - `Error::is_controller_id_not_registered_error` If the requested controller id is not
    ///   currently registered.
    /// - `Error::is_not_controller_error` If the request does not arrive at the active controller.
    /// - `Error::is_invalid_request_error` If the request tries to unregister the current active
    ///   controller id.
    ///
    /// # Arguments
    ///
    /// - `controller_id` - the controller id to unregister.
    /// - `options` - the options to use.
    ///
    /// # Returns
    ///
    /// the `UnregisterControllerResult` containing the result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#unregisterController")]
    fn unregister_controller_with_options(
        &self,
        controller_id: i32,
        options: UnregisterControllerOptions,
    ) -> UnregisterControllerResult;

    /// Describe producer state on a set of topic partitions. See
    /// `Self::describe_producers_with_options` for more details.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The set of partitions to query
    ///
    /// # Returns
    ///
    /// The result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeProducers")]
    fn describe_producers(&self, partitions: &[TopicPartition]) -> DescribeProducersResult {
        self.describe_producers_with_options(partitions, DescribeProducersOptions::default())
    }

    /// Describe active producer state on a set of topic partitions. Unless a specific broker is
    /// requested through `DescribeProducersOptions::broker_id`, this will query the partition
    /// leader to find the producer state.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The set of partitions to query
    /// - `options` - Options to control the method behavior
    ///
    /// # Returns
    ///
    /// The result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeProducers")]
    fn describe_producers_with_options(
        &self,
        partitions: &[TopicPartition],
        options: DescribeProducersOptions,
    ) -> DescribeProducersResult;

    /// Forcefully abort a transaction which is open on a topic partition. See
    /// `Self::abort_transaction_with_options` for more details.
    ///
    /// # Arguments
    ///
    /// - `spec` - The transaction specification including topic partition and producer details
    ///
    /// # Returns
    ///
    /// The result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#abortTransaction")]
    fn abort_transaction(&self, spec: AbortTransactionSpec) -> AbortTransactionResult {
        self.abort_transaction_with_options(spec, AbortTransactionOptions::default())
    }

    /// Forcefully abort a transaction which is open on a topic partition. This will send a
    /// `WriteTxnMarkers` request to the partition leader in order to abort the transaction. This
    /// requires administrative privileges.
    ///
    /// # Arguments
    ///
    /// - `spec` - The transaction specification including topic partition and producer details
    /// - `options` - Options to control the method behavior (including filters)
    ///
    /// # Returns
    ///
    /// The result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#abortTransaction")]
    fn abort_transaction_with_options(
        &self,
        spec: AbortTransactionSpec,
        options: AbortTransactionOptions,
    ) -> AbortTransactionResult;

    /// Describe the state of a set of transactional IDs. See
    /// `Self::describe_transactions_with_options` for more details.
    ///
    /// # Arguments
    ///
    /// - `transactional_ids` - The set of transactional IDs to query
    ///
    /// # Returns
    ///
    /// The result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeTransactions")]
    fn describe_transactions(&self, transactional_ids: &[String]) -> DescribeTransactionsResult {
        self.describe_transactions_with_options(transactional_ids, DescribeTransactionsOptions::default())
    }

    /// Describe the state of a set of transactional IDs from the respective transaction
    /// coordinators, which are dynamically discovered.
    ///
    /// # Arguments
    ///
    /// - `transactional_ids` - The set of transactional IDs to query
    /// - `options` - Options to control the method behavior
    ///
    /// # Returns
    ///
    /// The result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeTransactions")]
    fn describe_transactions_with_options(
        &self,
        transactional_ids: &[String],
        options: DescribeTransactionsOptions,
    ) -> DescribeTransactionsResult;

    /// Fence out all active producers that use any of the provided transactional IDs, with the
    /// default options.
    ///
    /// This is a convenience method for `Self::fence_producers_with_options` with default options.
    /// See the overload for more details.
    ///
    /// # Arguments
    ///
    /// - `transactional_ids` - The IDs of the producers to fence.
    ///
    /// # Returns
    ///
    /// The FenceProducersResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#fenceProducers")]
    fn fence_producers(&self, transactional_ids: &[String]) -> FenceProducersResult {
        self.fence_producers_with_options(transactional_ids, FenceProducersOptions::default())
    }

    /// Fence out all active producers that use any of the provided transactional IDs.
    ///
    /// # Arguments
    ///
    /// - `transactional_ids` - The IDs of the producers to fence.
    /// - `options` - The options to use when fencing the producers.
    ///
    /// # Returns
    ///
    /// The FenceProducersResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#fenceProducers")]
    fn fence_producers_with_options(
        &self,
        transactional_ids: &[String],
        options: FenceProducersOptions,
    ) -> FenceProducersResult;

    /// List active transactions in the cluster. See `Self::list_transactions_with_options` for more
    /// details.
    ///
    /// # Returns
    ///
    /// The result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listTransactions")]
    fn list_transactions(&self) -> ListTransactionsResult {
        self.list_transactions_with_options(ListTransactionsOptions::default())
    }

    /// List active transactions in the cluster. This will query all potential transaction
    /// coordinators in the cluster and collect the state of all transactions. Users should
    /// typically attempt to reduce the size of the result set using
    /// `ListTransactionsOptions::filter_producer_ids` or `ListTransactionsOptions::filter_states`
    /// or `ListTransactionsOptions::filter_on_duration`.
    ///
    /// # Arguments
    ///
    /// - `options` - Options to control the method behavior (including filters)
    ///
    /// # Returns
    ///
    /// The result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listTransactions")]
    fn list_transactions_with_options(&self, options: ListTransactionsOptions) -> ListTransactionsResult;

    /// Force terminate a transaction for the given transactional ID with the default options.
    ///
    /// This is a convenience method for `Self::force_terminate_transaction_with_options` with
    /// default options.
    ///
    /// # Arguments
    ///
    /// - `transactional_id` - The ID of the transaction to terminate.
    ///
    /// # Returns
    ///
    /// The TerminateTransactionResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#forceTerminateTransaction")]
    fn force_terminate_transaction(&self, transactional_id: &str) -> TerminateTransactionResult {
        self.force_terminate_transaction_with_options(transactional_id, TerminateTransactionOptions::default())
    }

    /// Force terminate a transaction for the given transactional ID. This operation aborts any
    /// ongoing transaction associated with the transactional ID. It's similar to fenceProducers but
    /// only targets a single transactional ID to handle long-running transactions when 2PC is
    /// enabled.
    ///
    /// # Arguments
    ///
    /// - `transactional_id` - The ID of the transaction to terminate.
    /// - `options` - The options to use when terminating the transaction.
    ///
    /// # Returns
    ///
    /// The TerminateTransactionResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#forceTerminateTransaction")]
    fn force_terminate_transaction_with_options(
        &self,
        transactional_id: &str,
        options: TerminateTransactionOptions,
    ) -> TerminateTransactionResult;

    /// Get information about the nodes in the cluster, using the default options.
    ///
    /// This is a convenience method for `Self::describe_cluster_with_options` with default options.
    /// See the overload for more details.
    ///
    /// # Returns
    ///
    /// The DescribeClusterResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeCluster")]
    fn describe_cluster(&self) -> DescribeClusterResult {
        self.describe_cluster_with_options(DescribeClusterOptions::default())
    }

    /// Get information about the nodes in the cluster.
    ///
    /// To obtain broker cluster information, you must configure
    /// `AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG`. To obtain controller cluster information, you
    /// must configure `AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG`.
    ///
    /// # Arguments
    ///
    /// - `options` - The options to use when getting information about the cluster.
    ///
    /// # Returns
    ///
    /// The DescribeClusterResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeCluster")]
    fn describe_cluster_with_options(&self, options: DescribeClusterOptions) -> DescribeClusterResult;

    /// Get the configuration for the specified resources with the default options.
    ///
    /// This is a convenience method for `Self::describe_configs_with_options` with default options.
    /// See the overload for more details.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `config_resources` - The resources (topic and broker resource types are currently
    ///   supported)
    ///
    /// # Returns
    ///
    /// The DescribeConfigsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeConfigs")]
    fn describe_configs(&self, config_resources: &[ConfigResource]) -> DescribeConfigsResult {
        self.describe_configs_with_options(config_resources, DescribeConfigsOptions::default())
    }

    /// Get the configuration for the specified resources.
    ///
    /// The returned configuration includes default values and the `ConfigEntry::is_default` method
    /// can be used to distinguish them from user supplied values.
    ///
    /// The value of config entries where `ConfigEntry::is_sensitive` is true is always `None` so
    /// that sensitive information is not disclosed.
    ///
    /// Config entries where `ConfigEntry::is_read_only` is true cannot be updated.
    ///
    /// The different behavior of nonexistent resource:
    ///
    /// - `config_resource::Type::Broker`: fails with `Error::is_timeout_error`
    /// - `config_resource::Type::Topic`: fails with `Error::is_unknown_topic_or_partition_error`
    /// - `config_resource::Type::Group`: just return default configs even if the target group is
    ///   nonexistent
    /// - `config_resource::Type::BrokerLogger`: fails with `Error::is_timeout_error`
    /// - `config_resource::Type::ClientMetrics`: will return empty configs
    ///
    /// Note that you cannot describe broker configs or broker logger using
    /// `AdminClientConfig::BOOTSTRAP_CONTROLLERS_CONFIG`, and you cannot describe controller
    /// configs or controller logger using `AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG`.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `config_resources` - The resources (topic and broker resource types are currently
    ///   supported)
    /// - `options` - The options to use when describing configs
    ///
    /// # Returns
    ///
    /// The DescribeConfigsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeConfigs")]
    fn describe_configs_with_options(
        &self,
        config_resources: &[ConfigResource],
        options: DescribeConfigsOptions,
    ) -> DescribeConfigsResult;

    /// Incrementally updates the configuration for the specified resources with default options.
    ///
    /// This is a convenience method for `Self::incremental_alter_configs_with_options` with default
    /// options. See the overload for more details.
    ///
    /// This operation is supported by brokers with version 2.3.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `configs` - The resources with their configs
    ///
    /// # Returns
    ///
    /// The AlterConfigsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#incrementalAlterConfigs")]
    fn incremental_alter_configs(&self, configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>) -> AlterConfigsResult {
        self.incremental_alter_configs_with_options(configs, AlterConfigsOptions::default())
    }

    /// Incrementally update the configuration for the specified resources.
    ///
    /// Updates are not transactional so they may succeed for some resources while fail for others.
    /// The configs for a particular resource are updated atomically.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the returned `AlterConfigsResult`:
    ///
    /// - `Error::is_cluster_authorization_error` if the authenticated user didn't have alter access
    ///   to the cluster.
    /// - `Error::is_topic_authorization_error` if the authenticated user didn't have alter access
    ///   to the Topic.
    /// - `Error::is_unknown_topic_or_partition_error` if the Topic doesn't exist.
    /// - `Error::is_invalid_request_error` if the request details are invalid. e.g., a
    ///   configuration key was specified more than once for a resource
    ///
    /// This operation is supported by brokers with version 2.3.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `configs` - The resources with their configs
    /// - `options` - The options to use when altering configs
    ///
    /// # Returns
    ///
    /// The AlterConfigsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#incrementalAlterConfigs")]
    fn incremental_alter_configs_with_options(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ) -> AlterConfigsResult;

    /// List all configuration resources available in the cluster with the default options.
    ///
    /// This is a convenience method for `Self::list_config_resources_with_options` with default
    /// options. See the overload for more details.
    ///
    /// # Returns
    ///
    /// The ListConfigurationResourcesResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listConfigResources")]
    fn list_config_resources(&self) -> ListConfigResourcesResult {
        self.list_config_resources_with_options(&HashSet::new(), ListConfigResourcesOptions::default())
    }

    /// List the configuration resources available in the cluster which matches config resource
    /// type. If no config resource types are specified, all configuration resources will be listed.
    ///
    /// # Arguments
    ///
    /// - `config_resource_types` - The set of configuration resource types to list.
    /// - `options` - The options to use when listing the configuration resources.
    ///
    /// # Returns
    ///
    /// The ListConfigurationResourcesResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listConfigResources")]
    fn list_config_resources_with_options(
        &self,
        config_resource_types: &HashSet<config_resource::Type>,
        options: ListConfigResourcesOptions,
    ) -> ListConfigResourcesResult;

    /// Query the information of all log directories on the given set of brokers
    ///
    /// This is a convenience method for `Self::describe_log_dirs_with_options` with default
    /// options. See the overload for more details.
    ///
    /// This operation is supported by brokers with version 1.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `brokers` - A list of brokers
    ///
    /// # Returns
    ///
    /// The DescribeLogDirsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeLogDirs")]
    fn describe_log_dirs(&self, brokers: &[i32]) -> DescribeLogDirsResult {
        self.describe_log_dirs_with_options(brokers, DescribeLogDirsOptions::default())
    }

    /// Query the information of all log directories on the given set of brokers
    ///
    /// This operation is supported by brokers with version 1.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `brokers` - A list of brokers
    /// - `options` - The options to use when querying log dir info
    ///
    /// # Returns
    ///
    /// The DescribeLogDirsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeLogDirs")]
    fn describe_log_dirs_with_options(&self, brokers: &[i32], options: DescribeLogDirsOptions)
    -> DescribeLogDirsResult;

    /// Change the log directory for the specified replicas. If the replica does not exist on the
    /// broker, the result shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be
    /// created in the given log directory on the broker when it is created later. If the replica
    /// already exists on the broker, the replica will be moved to the given log directory if it is
    /// not already there. For detailed result, inspect the returned `AlterReplicaLogDirsResult`
    /// instance.
    ///
    /// This operation is not transactional so it may succeed for some replicas while fail for
    /// others.
    ///
    /// This is a convenience method for `Self::alter_replica_log_dirs_with_options` with default
    /// options. See the overload for more details.
    ///
    /// This operation is supported by brokers with version 1.1.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `replica_assignment` - The replicas with their log directory absolute path
    ///
    /// # Returns
    ///
    /// The AlterReplicaLogDirsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterReplicaLogDirs")]
    fn alter_replica_log_dirs(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
    ) -> AlterReplicaLogDirsResult {
        self.alter_replica_log_dirs_with_options(replica_assignment, AlterReplicaLogDirsOptions::default())
    }

    /// Change the log directory for the specified replicas. If the replica does not exist on the
    /// broker, the result shows REPLICA_NOT_AVAILABLE for the given replica and the replica will be
    /// created in the given log directory on the broker when it is created later. If the replica
    /// already exists on the broker, the replica will be moved to the given log directory if it is
    /// not already there. For detailed result, inspect the returned `AlterReplicaLogDirsResult`
    /// instance.
    ///
    /// This operation is not transactional so it may succeed for some replicas while fail for
    /// others.
    ///
    /// This operation is supported by brokers with version 1.1.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `replica_assignment` - The replicas with their log directory absolute path
    /// - `options` - The options to use when changing replica dir
    ///
    /// # Returns
    ///
    /// The AlterReplicaLogDirsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterReplicaLogDirs")]
    fn alter_replica_log_dirs_with_options(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ) -> AlterReplicaLogDirsResult;

    /// Query the replica log directory information for the specified replicas.
    ///
    /// This is a convenience method for `Self::describe_replica_log_dirs_with_options` with default
    /// options. See the overload for more details.
    ///
    /// This operation is supported by brokers with version 1.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `replicas` - The replicas to query
    ///
    /// # Returns
    ///
    /// The DescribeReplicaLogDirsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeReplicaLogDirs")]
    fn describe_replica_log_dirs(&self, replicas: &[TopicPartitionReplica]) -> DescribeReplicaLogDirsResult {
        self.describe_replica_log_dirs_with_options(replicas, DescribeReplicaLogDirsOptions::default())
    }

    /// Query the replica log directory information for the specified replicas.
    ///
    /// This operation is supported by brokers with version 1.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `replicas` - The replicas to query
    /// - `options` - The options to use when querying replica log dir info
    ///
    /// # Returns
    ///
    /// The DescribeReplicaLogDirsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeReplicaLogDirs")]
    fn describe_replica_log_dirs_with_options(
        &self,
        replicas: &[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> DescribeReplicaLogDirsResult;

    /// Elect a replica as leader for topic partitions.
    ///
    /// This is a convenience method for `Self::elect_leaders_with_options` with default options.
    ///
    /// # Arguments
    ///
    /// - `election_type` - The type of election to conduct.
    /// - `partitions` - The topics and partitions for which to conduct elections.
    ///
    /// # Returns
    ///
    /// The ElectLeadersResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#electLeaders")]
    fn elect_leaders(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
    ) -> ElectLeadersResult {
        self.elect_leaders_with_options(election_type, partitions, ElectLeadersOptions::default())
    }

    /// Elect a replica as leader for the given `partitions`, or for all partitions if the argument
    /// to `partitions` is None.
    ///
    /// This operation is not transactional so it may succeed for some partitions while fail for
    /// others.
    ///
    /// It may take several seconds after this method returns success for all the brokers in the
    /// cluster to become aware that the partitions have new leaders. During this time,
    /// `Self::describe_topics_with_topic_names` may not return information about the partitions'
    /// new leaders.
    ///
    /// This operation is supported by brokers with version 2.2.0 or later if preferred election is
    /// use; otherwise the brokers most be 2.4.0 or higher.
    ///
    /// The following errors can be anticipated when calling `get()` on the future obtained from the
    /// returned `ElectLeadersResult`:
    ///
    /// - `Error::is_cluster_authorization_error` if the authenticated user didn't have alter access
    ///   to the cluster.
    /// - `Error::is_unknown_topic_or_partition_error` if the topic or partition did not exist
    ///   within the cluster.
    /// - `Error::is_invalid_topic_error` if the topic was already queued for deletion.
    /// - `Error::is_not_controller_error` if the request was sent to a broker that was not the
    ///   controller for the cluster.
    /// - `Error::is_timeout_error` if the request timed out before the election was complete.
    /// - `Error::is_leader_not_available_error` if the preferred leader was not alive or not in the
    ///   ISR.
    ///
    /// # Arguments
    ///
    /// - `election_type` - The type of election to conduct.
    /// - `partitions` - The topics and partitions for which to conduct elections.
    /// - `options` - The options to use when electing the leaders.
    ///
    /// # Returns
    ///
    /// The ElectLeadersResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#electLeaders")]
    fn elect_leaders_with_options(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
        options: ElectLeadersOptions,
    ) -> ElectLeadersResult;

    /// Change the reassignments for one or more partitions. Providing `None` will revert the
    /// reassignment for the associated partition. This is a convenience method for
    /// `Self::alter_partition_reassignments_with_options` with default options. See the overload
    /// for more details.
    ///
    /// # Arguments
    ///
    /// - `reassignments` - The reassignments to add, modify, or remove. See
    ///   `NewPartitionReassignment`.
    ///
    /// # Returns
    ///
    /// The result.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterPartitionReassignments")]
    fn alter_partition_reassignments(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
    ) -> AlterPartitionReassignmentsResult {
        self.alter_partition_reassignments_with_options(reassignments, AlterPartitionReassignmentsOptions::default())
    }

    /// Change the reassignments for one or more partitions. Providing `None` will revert the
    /// reassignment for the associated partition.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the returned `AlterPartitionReassignmentsResult`:
    ///
    /// - `Error::is_cluster_authorization_error` If the authenticated user didn't have alter access
    ///   to the cluster.
    /// - `Error::is_unknown_topic_or_partition_error` If the topic or partition does not exist
    ///   within the cluster.
    /// - `Error::is_timeout_error` if the request timed out before the controller could record the
    ///   new assignments.
    /// - `Error::is_invalid_replica_assignment_error` If the specified assignment was not valid.
    /// - `Error::is_invalid_replication_factor_error` If the replication factor was changed in an
    ///   invalid way. Only thrown when
    ///   `AlterPartitionReassignmentsOptions::allow_replication_factor_change` is set to false and
    ///   the request is attempting to alter reassignments (not cancel)
    /// - `Error::is_unsupported_version_error` If
    ///   `AlterPartitionReassignmentsOptions::allow_replication_factor_change` was changed outside
    ///   the default and the server does not support the option (e.g due to an old Kafka version).
    /// - `Error::is_no_reassignment_in_progress_error` If there was an attempt to cancel a
    ///   reassignment for a partition which was not being reassigned.
    ///
    /// # Arguments
    ///
    /// - `reassignments` - The reassignments to add, modify, or remove. See
    ///   `NewPartitionReassignment`.
    /// - `options` - The options to use.
    ///
    /// # Returns
    ///
    /// The result.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterPartitionReassignments")]
    fn alter_partition_reassignments_with_options(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
        options: AlterPartitionReassignmentsOptions,
    ) -> AlterPartitionReassignmentsResult;

    /// List all of the current partition reassignments.
    ///
    /// This is a convenience method for `Self::list_partition_reassignments_with_options` with
    /// default options. See the overload for more details.
    ///
    /// # Returns
    ///
    /// The result.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listPartitionReassignments")]
    fn list_partition_reassignments(&self) -> ListPartitionReassignmentsResult {
        self.list_partition_reassignments_with_options(ListPartitionReassignmentsOptions::default())
    }

    /// List the current reassignments for the given partitions.
    ///
    /// This is a convenience method for
    /// `Self::list_partition_reassignments_with_partitions_options` with default options. See the
    /// overload for more details.
    ///
    /// # Arguments
    ///
    /// - `partitions` - The topic partitions to list reassignments for.
    ///
    /// # Returns
    ///
    /// The result.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listPartitionReassignments")]
    fn list_partition_reassignments_with_partitions(
        &self,
        partitions: HashSet<TopicPartition>,
    ) -> ListPartitionReassignmentsResult {
        self.list_partition_reassignments_with_partitions_options(
            Some(partitions),
            ListPartitionReassignmentsOptions::default(),
        )
    }

    /// List all of the current partition reassignments.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the returned `ListPartitionReassignmentsResult`:
    ///
    /// - `Error::is_cluster_authorization_error` If the authenticated user doesn't have alter
    ///   access to the cluster.
    /// - `Error::is_unknown_topic_or_partition_error` If a given topic or partition does not exist.
    /// - `Error::is_timeout_error` If the request timed out before the controller could list the
    ///   current reassignments.
    ///
    /// # Arguments
    ///
    /// - `options` - The options to use.
    ///
    /// # Returns
    ///
    /// The result.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listPartitionReassignments")]
    fn list_partition_reassignments_with_options(
        &self,
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult {
        self.list_partition_reassignments_with_partitions_options(None, options)
    }

    /// List the current reassignments for the given partitions.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the returned `ListPartitionReassignmentsResult`:
    ///
    /// - `Error::is_cluster_authorization_error` If the authenticated user doesn't have alter
    ///   access to the cluster.
    /// - `Error::is_unknown_topic_or_partition_error` If a given topic or partition does not exist.
    /// - `Error::is_timeout_error` If the request timed out before the controller could list the
    ///   current reassignments.
    ///
    /// # Arguments
    ///
    /// - `partitions` - the partitions we want to get reassignment for, or `None` if we want to get
    ///   the reassignments for all partitions in the cluster
    /// - `options` - The options to use.
    ///
    /// # Returns
    ///
    /// The result.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listPartitionReassignments")]
    fn list_partition_reassignments_with_partitions_options(
        &self,
        partitions: Option<HashSet<TopicPartition>>,
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult;

    /// List offset for the specified partitions and OffsetSpec. This operation enables to find the
    /// beginning offset, end offset as well as the offset matching a timestamp in partitions.
    ///
    /// This is a convenience method for `Self::list_offsets_with_options`
    ///
    /// # Arguments
    ///
    /// - `topic_partition_offsets` - The mapping from partition to the OffsetSpec to look up.
    ///
    /// # Returns
    ///
    /// The ListOffsetsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listOffsets")]
    fn list_offsets(&self, topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>) -> ListOffsetsResult {
        self.list_offsets_with_options(topic_partition_offsets, ListOffsetsOptions::default())
    }

    /// List offset for the specified partitions. This operation enables to find the beginning
    /// offset, end offset as well as the offset matching a timestamp in partitions.
    ///
    /// # Arguments
    ///
    /// - `topic_partition_offsets` - The mapping from partition to the OffsetSpec to look up.
    /// - `options` - The options to use when retrieving the offsets
    ///
    /// # Returns
    ///
    /// The ListOffsetsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listOffsets")]
    fn list_offsets_with_options(
        &self,
        topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ) -> ListOffsetsResult;

    /// List the groups available in the cluster with the default options.
    ///
    /// This is a convenience method for `Self::list_groups_with_options` with default options. See
    /// the overload for more details.
    ///
    /// # Returns
    ///
    /// The ListGroupsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listGroups")]
    fn list_groups(&self) -> ListGroupsResult {
        self.list_groups_with_options(ListGroupsOptions::default())
    }

    /// List the groups available in the cluster.
    ///
    /// # Arguments
    ///
    /// - `options` - The options to use when listing the groups.
    ///
    /// # Returns
    ///
    /// The ListGroupsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listGroups")]
    fn list_groups_with_options(&self, options: ListGroupsOptions) -> ListGroupsResult;

    /// Describe some consumer groups in the cluster, with the default options.
    ///
    /// This is a convenience method for `Self::describe_consumer_groups_with_options` with default
    /// options. See the overload for more details.
    ///
    /// # Arguments
    ///
    /// - `group_ids` - The IDs of the groups to describe.
    ///
    /// # Returns
    ///
    /// The DescribeConsumerGroupsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeConsumerGroups")]
    fn describe_consumer_groups(&self, group_ids: &[String]) -> DescribeConsumerGroupsResult {
        self.describe_consumer_groups_with_options(group_ids, DescribeConsumerGroupsOptions::default())
    }

    /// Describe some consumer groups in the cluster.
    ///
    /// # Arguments
    ///
    /// - `group_ids` - The IDs of the groups to describe.
    /// - `options` - The options to use when describing the groups.
    ///
    /// # Returns
    ///
    /// The DescribeConsumerGroupsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeConsumerGroups")]
    fn describe_consumer_groups_with_options(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> DescribeConsumerGroupsResult;

    /// Describe some classic groups in the cluster, with the default options.
    ///
    /// This is a convenience method for `Self::describe_classic_groups_with_options` with default
    /// options. See the overload for more details.
    ///
    /// # Arguments
    ///
    /// - `group_ids` - The IDs of the groups to describe.
    ///
    /// # Returns
    ///
    /// The DescribeClassicGroupsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeClassicGroups")]
    fn describe_classic_groups(&self, group_ids: &[String]) -> DescribeClassicGroupsResult {
        self.describe_classic_groups_with_options(group_ids, DescribeClassicGroupsOptions::default())
    }

    /// Describe some classic groups in the cluster.
    ///
    /// # Arguments
    ///
    /// - `group_ids` - The IDs of the groups to describe.
    /// - `options` - The options to use when describing the groups.
    ///
    /// # Returns
    ///
    /// The DescribeClassicGroupsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeClassicGroups")]
    fn describe_classic_groups_with_options(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> DescribeClassicGroupsResult;

    /// List the consumer group offsets available in the cluster with the default options.
    ///
    /// This is a convenience method for `Self::list_consumer_group_offsets_with_group_id_options`
    /// to list offsets of all partitions of one group with default options.
    ///
    /// # Arguments
    ///
    /// - `group_id` - The group to list the offsets of.
    ///
    /// # Returns
    ///
    /// The ListConsumerGroupOffsetsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listConsumerGroupOffsets")]
    fn list_consumer_group_offsets_with_group_id(&self, group_id: &str) -> ListConsumerGroupOffsetsResult {
        self.list_consumer_group_offsets_with_group_id_options(group_id, ListConsumerGroupOffsetsOptions::default())
    }

    /// List the consumer group offsets available in the cluster.
    ///
    /// # Arguments
    ///
    /// - `group_id` - The group to list the offsets of.
    /// - `options` - The options to use when listing the consumer group offsets.
    ///
    /// # Returns
    ///
    /// The ListConsumerGroupOffsetsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listConsumerGroupOffsets")]
    fn list_consumer_group_offsets_with_group_id_options(
        &self,
        group_id: &str,
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult {
        let group_spec = ListConsumerGroupOffsetsSpec::new();

        // We can use the provided options with the batched API, which uses topic partitions from
        // the group spec and ignores any topic partitions set in the options.
        self.list_consumer_group_offsets_with_group_specs_options(
            &HashMap::from([(group_id.to_string(), group_spec)]),
            options,
        )
    }

    /// List the consumer group offsets available in the cluster for the specified groups with the
    /// default options.
    ///
    /// This is a convenience method for
    /// `Self::list_consumer_group_offsets_with_group_specs_options` with default options.
    ///
    /// # Arguments
    ///
    /// - `group_specs` - Map of consumer group ids to a spec that specifies the topic partitions of
    ///   the group to list offsets for.
    ///
    /// # Returns
    ///
    /// The ListConsumerGroupOffsetsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listConsumerGroupOffsets")]
    fn list_consumer_group_offsets_with_group_specs(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
    ) -> ListConsumerGroupOffsetsResult {
        self.list_consumer_group_offsets_with_group_specs_options(
            group_specs,
            ListConsumerGroupOffsetsOptions::default(),
        )
    }

    /// List the consumer group offsets available in the cluster for the specified consumer groups.
    ///
    /// # Arguments
    ///
    /// - `group_specs` - Map of consumer group ids to a spec that specifies the topic partitions of
    ///   the group to list offsets for.
    /// - `options` - The options to use when listing the consumer group offsets.
    ///
    /// # Returns
    ///
    /// The ListConsumerGroupOffsetsResult
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#listConsumerGroupOffsets")]
    fn list_consumer_group_offsets_with_group_specs_options(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult;

    /// Alters offsets for the specified group. In order to succeed, the group must be empty.
    ///
    /// This is a convenience method for `Self::alter_consumer_group_offsets_with_options` with
    /// default options. See the overload for more details.
    ///
    /// # Arguments
    ///
    /// - `group_id` - The group for which to alter offsets.
    /// - `offsets` - A map of offsets by partition with associated metadata.
    ///
    /// # Returns
    ///
    /// The AlterOffsetsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterConsumerGroupOffsets")]
    fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> AlterConsumerGroupOffsetsResult {
        self.alter_consumer_group_offsets_with_options(group_id, offsets, AlterConsumerGroupOffsetsOptions::default())
    }

    /// Alters offsets for the specified group. In order to succeed, the group must be empty.
    ///
    /// This operation is not transactional so it may succeed for some partitions while fail for
    /// others.
    ///
    /// # Arguments
    ///
    /// - `group_id` - The group for which to alter offsets.
    /// - `offsets` - A map of offsets by partition with associated metadata. Partitions not
    ///   specified in the map are ignored.
    /// - `options` - The options to use when altering the offsets.
    ///
    /// # Returns
    ///
    /// The AlterOffsetsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterConsumerGroupOffsets")]
    fn alter_consumer_group_offsets_with_options(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ) -> AlterConsumerGroupOffsetsResult;

    /// Delete committed offsets for a set of partitions in a consumer group with the default
    /// options. This will succeed at the partition level only if the group is not actively
    /// subscribed to the corresponding topic.
    ///
    /// # Arguments
    ///
    /// - `group_id` - The group to delete the offsets of.
    /// - `partitions` - The partitions whose committed offsets to delete.
    ///
    /// # Returns
    ///
    /// The DeleteConsumerGroupOffsetsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteConsumerGroupOffsets")]
    fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
    ) -> DeleteConsumerGroupOffsetsResult {
        self.delete_consumer_group_offsets_with_options(
            group_id,
            partitions,
            DeleteConsumerGroupOffsetsOptions::default(),
        )
    }

    /// Delete committed offsets for a set of partitions in a consumer group. This will succeed at
    /// the partition level only if the group is not actively subscribed to the corresponding topic.
    ///
    /// # Arguments
    ///
    /// - `group_id` - The group to delete the offsets of.
    /// - `partitions` - The partitions whose committed offsets to delete.
    /// - `options` - The options to use when deleting offsets in a consumer group.
    ///
    /// # Returns
    ///
    /// The DeleteConsumerGroupOffsetsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteConsumerGroupOffsets")]
    fn delete_consumer_group_offsets_with_options(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> DeleteConsumerGroupOffsetsResult;

    /// Delete consumer groups from the cluster with the default options.
    ///
    /// # Arguments
    ///
    /// - `group_ids` - The ids of the groups to delete.
    ///
    /// # Returns
    ///
    /// The DeleteConsumerGroupResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteConsumerGroups")]
    fn delete_consumer_groups(&self, group_ids: &[String]) -> DeleteConsumerGroupsResult {
        self.delete_consumer_groups_with_options(group_ids, DeleteConsumerGroupsOptions::default())
    }

    /// Delete consumer groups from the cluster.
    ///
    /// # Arguments
    ///
    /// - `group_ids` - The ids of the groups to delete.
    /// - `options` - The options to use when deleting a consumer group.
    ///
    /// # Returns
    ///
    /// The DeleteConsumerGroupsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteConsumerGroups")]
    fn delete_consumer_groups_with_options(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> DeleteConsumerGroupsResult;

    /// Remove members from the consumer group by given member identities.
    ///
    /// For possible error codes, refer to `LeaveGroupResponse`.
    ///
    /// # Arguments
    ///
    /// - `group_id` - The ID of the group to remove member from.
    /// - `options` - The options to carry removing members' information.
    ///
    /// # Returns
    ///
    /// The MembershipChangeResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#removeMembersFromConsumerGroup")]
    fn remove_members_from_consumer_group_with_options(
        &self,
        group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> RemoveMembersFromConsumerGroupResult;

    /// This is a convenience method for `Self::create_acls_with_options` with default options. See
    /// the overload for more details.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `acls` - The ACLs to create
    ///
    /// # Returns
    ///
    /// The CreateAclsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#createAcls")]
    fn create_acls(&self, acls: &[AclBinding]) -> CreateAclsResult {
        self.create_acls_with_options(acls, CreateAclsOptions::default())
    }

    /// Creates access control lists (ACLs) which are bound to specific resources.
    ///
    /// This operation is not transactional so it may succeed for some ACLs while fail for others.
    ///
    /// If you attempt to add an ACL that duplicates an existing ACL, no error will be raised, but
    /// no changes will be made.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `acls` - The ACLs to create
    /// - `options` - The options to use when creating the ACLs.
    ///
    /// # Returns
    ///
    /// The CreateAclsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#createAcls")]
    fn create_acls_with_options(&self, acls: &[AclBinding], options: CreateAclsOptions) -> CreateAclsResult;

    /// This is a convenience method for `Self::describe_acls_with_options` with default options.
    /// See the overload for more details.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `filter` - The filter to use.
    ///
    /// # Returns
    ///
    /// The DescribeAclsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeAcls")]
    fn describe_acls(&self, filter: &AclBindingFilter) -> DescribeAclsResult {
        self.describe_acls_with_options(filter, DescribeAclsOptions::default())
    }

    /// Lists access control lists (ACLs) according to the supplied filter.
    ///
    /// Note: it may take some time for changes made by `createAcls` or `deleteAcls` to be reflected
    /// in the output of `describeAcls`.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `filter` - The filter to use.
    /// - `options` - The options to use when listing the ACLs.
    ///
    /// # Returns
    ///
    /// The DescribeAclsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeAcls")]
    fn describe_acls_with_options(&self, filter: &AclBindingFilter, options: DescribeAclsOptions)
    -> DescribeAclsResult;

    /// This is a convenience method for `Self::delete_acls_with_options` with default options. See
    /// the overload for more details.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `filters` - The filters to use.
    ///
    /// # Returns
    ///
    /// The DeleteAclsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteAcls")]
    fn delete_acls(&self, filters: &[AclBindingFilter]) -> DeleteAclsResult {
        self.delete_acls_with_options(filters, DeleteAclsOptions::default())
    }

    /// Deletes access control lists (ACLs) according to the supplied filters.
    ///
    /// This operation is not transactional so it may succeed for some ACLs while fail for others.
    ///
    /// This operation is supported by brokers with version 0.11.0.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `filters` - The filters to use.
    /// - `options` - The options to use when deleting the ACLs.
    ///
    /// # Returns
    ///
    /// The DeleteAclsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#deleteAcls")]
    fn delete_acls_with_options(&self, filters: &[AclBindingFilter], options: DeleteAclsOptions) -> DeleteAclsResult;

    /// Describes all entities matching the provided filter that have at least one client quota
    /// configuration value defined.
    ///
    /// This is a convenience method for `Self::describe_client_quotas_with_options` with default
    /// options. See the overload for more details.
    ///
    /// This operation is supported by brokers with version 2.6.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `filter` - the filter to apply to match entities
    ///
    /// # Returns
    ///
    /// the DescribeClientQuotasResult containing the result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeClientQuotas")]
    fn describe_client_quotas(&self, filter: &ClientQuotaFilter) -> DescribeClientQuotasResult {
        self.describe_client_quotas_with_options(filter, DescribeClientQuotasOptions::default())
    }

    /// Describes all entities matching the provided filter that have at least one client quota
    /// configuration value defined.
    ///
    /// The following errors can be anticipated when calling `get()` on the future from the returned
    /// `DescribeClientQuotasResult`:
    ///
    /// - `Error::is_cluster_authorization_error` If the authenticated user didn't have describe
    ///   access to the cluster.
    /// - `Error::is_invalid_request_error` If the request details are invalid. e.g., an invalid
    ///   entity type was specified.
    /// - `Error::is_timeout_error` If the request timed out before the describe could finish.
    ///
    /// This operation is supported by brokers with version 2.6.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `filter` - the filter to apply to match entities
    /// - `options` - the options to use
    ///
    /// # Returns
    ///
    /// the DescribeClientQuotasResult containing the result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeClientQuotas")]
    fn describe_client_quotas_with_options(
        &self,
        filter: &ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> DescribeClientQuotasResult;

    /// Alters client quota configurations with the specified alterations.
    ///
    /// This is a convenience method for `Self::alter_client_quotas_with_options` with default
    /// options. See the overload for more details.
    ///
    /// This operation is supported by brokers with version 2.6.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `entries` - the alterations to perform
    ///
    /// # Returns
    ///
    /// the AlterClientQuotasResult containing the result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterClientQuotas")]
    fn alter_client_quotas(&self, entries: &[ClientQuotaAlteration]) -> AlterClientQuotasResult {
        self.alter_client_quotas_with_options(entries, AlterClientQuotasOptions::default())
    }

    /// Alters client quota configurations with the specified alterations.
    ///
    /// Alterations for a single entity are atomic, but across entities is not guaranteed. The
    /// resulting per-entity error code should be evaluated to resolve the success or failure of all
    /// updates.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the returned `AlterClientQuotasResult`:
    ///
    /// - `Error::is_cluster_authorization_error` If the authenticated user didn't have alter access
    ///   to the cluster.
    /// - `Error::is_invalid_request_error` If the request details are invalid. e.g., a
    ///   configuration key was specified more than once for an entity.
    /// - `Error::is_timeout_error` If the request timed out before the alterations could finish. It
    ///   cannot be guaranteed whether the update succeed or not.
    ///
    /// This operation is supported by brokers with version 2.6.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `entries` - the alterations to perform
    /// - `options` - The options to use when altering the quotas.
    ///
    /// # Returns
    ///
    /// the AlterClientQuotasResult containing the result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterClientQuotas")]
    fn alter_client_quotas_with_options(
        &self,
        entries: &[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> AlterClientQuotasResult;

    /// Describe all SASL/SCRAM credentials.
    ///
    /// This is a convenience method for `Self::describe_user_scram_credentials_with_users_options`
    ///
    /// # Returns
    ///
    /// The DescribeUserScramCredentialsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeUserScramCredentials")]
    fn describe_user_scram_credentials(&self) -> DescribeUserScramCredentialsResult {
        self.describe_user_scram_credentials_with_users_options(&[], DescribeUserScramCredentialsOptions::default())
    }

    /// Describe SASL/SCRAM credentials for the given users.
    ///
    /// This is a convenience method for `Self::describe_user_scram_credentials_with_users_options`
    ///
    /// # Arguments
    ///
    /// - `users` - the users for which credentials are to be described; all users' credentials are
    ///   described if empty.
    ///
    /// # Returns
    ///
    /// The DescribeUserScramCredentialsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeUserScramCredentials")]
    fn describe_user_scram_credentials_with_users(&self, users: &[String]) -> DescribeUserScramCredentialsResult {
        self.describe_user_scram_credentials_with_users_options(users, DescribeUserScramCredentialsOptions::default())
    }

    /// Describe SASL/SCRAM credentials.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures from the
    /// returned `DescribeUserScramCredentialsResult`:
    ///
    /// - `Error::is_cluster_authorization_error` If the authenticated user didn't have describe
    ///   access to the cluster.
    /// - `Error::is_resource_not_found_error` If the user did not exist/had no SCRAM credentials.
    /// - `Error::is_duplicate_resource_error` If the user was requested to be described more than
    ///   once in the original request.
    /// - `Error::is_timeout_error` If the request timed out before the describe operation could
    ///   finish.
    ///
    /// This operation is supported by brokers with version 2.7.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `users` - the users for which credentials are to be described; all users' credentials are
    ///   described if empty.
    /// - `options` - The options to use when describing the credentials
    ///
    /// # Returns
    ///
    /// The DescribeUserScramCredentialsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeUserScramCredentials")]
    fn describe_user_scram_credentials_with_users_options(
        &self,
        users: &[String],
        options: DescribeUserScramCredentialsOptions,
    ) -> DescribeUserScramCredentialsResult;

    /// Alter SASL/SCRAM credentials for the given users.
    ///
    /// This is a convenience method for `Self::alter_user_scram_credentials_with_options`
    ///
    /// # Arguments
    ///
    /// - `alterations` - the alterations to be applied
    ///
    /// # Returns
    ///
    /// The AlterUserScramCredentialsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterUserScramCredentials")]
    fn alter_user_scram_credentials(
        &self,
        alterations: &[UserScramCredentialAlteration],
    ) -> AlterUserScramCredentialsResult {
        self.alter_user_scram_credentials_with_options(alterations, AlterUserScramCredentialsOptions::default())
    }

    /// Alter SASL/SCRAM credentials.
    ///
    /// The following errors can be anticipated when calling `get()` any of the futures from the
    /// returned `AlterUserScramCredentialsResult`:
    ///
    /// - `Error::is_not_controller_error` If the request is not sent to the Controller broker.
    /// - `Error::is_cluster_authorization_error` If the authenticated user didn't have alter access
    ///   to the cluster.
    /// - `Error::is_unsupported_by_authentication_error` If the user authenticated with a
    ///   delegation token.
    /// - `Error::is_unsupported_sasl_mechanism_error` If the requested SCRAM mechanism is
    ///   unrecognized or otherwise unsupported.
    /// - `Error::is_unacceptable_credential_error` If the username is empty or the requested number
    ///   of iterations is too small or too large.
    /// - `Error::is_timeout_error` If the request timed out before the describe could finish.
    ///
    /// This operation is supported by brokers with version 2.7.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `alterations` - the alterations to be applied
    /// - `options` - The options to use when altering the credentials
    ///
    /// # Returns
    ///
    /// The AlterUserScramCredentialsResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#alterUserScramCredentials")]
    fn alter_user_scram_credentials_with_options(
        &self,
        alterations: &[UserScramCredentialAlteration],
        options: AlterUserScramCredentialsOptions,
    ) -> AlterUserScramCredentialsResult;

    /// Create a Delegation Token.
    ///
    /// This is a convenience method for `Self::create_delegation_token_with_options` with default
    /// options. See the overload for more details.
    ///
    /// # Returns
    ///
    /// The CreateDelegationTokenResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#createDelegationToken")]
    fn create_delegation_token(&self) -> CreateDelegationTokenResult {
        self.create_delegation_token_with_options(CreateDelegationTokenOptions::default())
    }

    /// Create a Delegation Token.
    ///
    /// This operation is supported by brokers with version 1.1.0 or higher.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the delegationToken() method of the returned `CreateDelegationTokenResult`
    ///
    /// - `Error::is_unsupported_by_authentication_error` If the request sent on PLAINTEXT/1-way SSL
    ///   channels or delegation token authenticated channels.
    /// - `Error::is_invalid_principal_type_error` if the renewers principal type is not supported.
    /// - `Error::is_delegation_token_disabled_error` if the delegation token feature is disabled.
    /// - `Error::is_timeout_error` if the request was not completed in within the given
    ///   `CreateDelegationTokenOptions::timeout_ms`.
    ///
    /// # Arguments
    ///
    /// - `options` - The options to use when creating delegation token.
    ///
    /// # Returns
    ///
    /// The CreateDelegationTokenResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#createDelegationToken")]
    fn create_delegation_token_with_options(
        &self,
        options: CreateDelegationTokenOptions,
    ) -> CreateDelegationTokenResult;

    /// Renew a Delegation Token.
    ///
    /// This is a convenience method for `Self::renew_delegation_token_with_options` with default
    /// options. See the overload for more details.
    ///
    /// # Arguments
    ///
    /// - `hmac` - HMAC of the Delegation token
    ///
    /// # Returns
    ///
    /// The RenewDelegationTokenResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#renewDelegationToken")]
    fn renew_delegation_token(&self, hmac: &[u8]) -> RenewDelegationTokenResult {
        self.renew_delegation_token_with_options(hmac, RenewDelegationTokenOptions::default())
    }

    /// Renew a Delegation Token.
    ///
    /// This operation is supported by brokers with version 1.1.0 or higher.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the expiryTimestamp() method of the returned `RenewDelegationTokenResult`
    ///
    /// - `Error::is_unsupported_by_authentication_error` If the request sent on PLAINTEXT/1-way SSL
    ///   channels or delegation token authenticated channels.
    /// - `Error::is_delegation_token_disabled_error` if the delegation token feature is disabled.
    /// - `Error::is_delegation_token_not_found_error` if the delegation token is not found on
    ///   server.
    /// - `Error::is_delegation_token_owner_mismatch_error` if the authenticated user is not
    ///   owner/renewer of the token.
    /// - `Error::is_delegation_token_expired_error` if the delegation token is expired.
    /// - `Error::is_timeout_error` if the request was not completed in within the given
    ///   `RenewDelegationTokenOptions::timeout_ms`.
    ///
    /// # Arguments
    ///
    /// - `hmac` - HMAC of the Delegation token
    /// - `options` - The options to use when renewing delegation token.
    ///
    /// # Returns
    ///
    /// The RenewDelegationTokenResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#renewDelegationToken")]
    fn renew_delegation_token_with_options(
        &self,
        hmac: &[u8],
        options: RenewDelegationTokenOptions,
    ) -> RenewDelegationTokenResult;

    /// Expire a Delegation Token.
    ///
    /// This is a convenience method for `Self::expire_delegation_token_with_options` with default
    /// options. This will expire the token immediately. See the overload for more details.
    ///
    /// # Arguments
    ///
    /// - `hmac` - HMAC of the Delegation token
    ///
    /// # Returns
    ///
    /// The ExpireDelegationTokenResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#expireDelegationToken")]
    fn expire_delegation_token(&self, hmac: &[u8]) -> ExpireDelegationTokenResult {
        self.expire_delegation_token_with_options(hmac, ExpireDelegationTokenOptions::default())
    }

    /// Expire a Delegation Token.
    ///
    /// This operation is supported by brokers with version 1.1.0 or higher.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the expiryTimestamp() method of the returned `ExpireDelegationTokenResult`
    ///
    /// - `Error::is_unsupported_by_authentication_error` If the request sent on PLAINTEXT/1-way SSL
    ///   channels or delegation token authenticated channels.
    /// - `Error::is_delegation_token_disabled_error` if the delegation token feature is disabled.
    /// - `Error::is_delegation_token_not_found_error` if the delegation token is not found on
    ///   server.
    /// - `Error::is_delegation_token_owner_mismatch_error` if the authenticated user is not
    ///   owner/renewer of the requested token.
    /// - `Error::is_delegation_token_expired_error` if the delegation token is expired.
    /// - `Error::is_timeout_error` if the request was not completed in within the given
    ///   `ExpireDelegationTokenOptions::timeout_ms`.
    ///
    /// # Arguments
    ///
    /// - `hmac` - HMAC of the Delegation token
    /// - `options` - The options to use when expiring delegation token.
    ///
    /// # Returns
    ///
    /// The ExpireDelegationTokenResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#expireDelegationToken")]
    fn expire_delegation_token_with_options(
        &self,
        hmac: &[u8],
        options: ExpireDelegationTokenOptions,
    ) -> ExpireDelegationTokenResult;

    /// Describe the Delegation Tokens.
    ///
    /// This is a convenience method for `Self::describe_delegation_token_with_options` with default
    /// options. This will return all the user owned tokens and tokens where user have Describe
    /// permission. See the overload for more details.
    ///
    /// # Returns
    ///
    /// The DescribeDelegationTokenResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeDelegationToken")]
    fn describe_delegation_token(&self) -> DescribeDelegationTokenResult {
        self.describe_delegation_token_with_options(DescribeDelegationTokenOptions::default())
    }

    /// Describe the Delegation Tokens.
    ///
    /// This operation is supported by brokers with version 1.1.0 or higher.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the delegationTokens() method of the returned `DescribeDelegationTokenResult`
    ///
    /// - `Error::is_unsupported_by_authentication_error` If the request sent on PLAINTEXT/1-way SSL
    ///   channels or delegation token authenticated channels.
    /// - `Error::is_delegation_token_disabled_error` if the delegation token feature is disabled.
    /// - `Error::is_timeout_error` if the request was not completed in within the given
    ///   `DescribeDelegationTokenOptions::timeout_ms`.
    ///
    /// # Arguments
    ///
    /// - `options` - The options to use when describing delegation tokens.
    ///
    /// # Returns
    ///
    /// The DescribeDelegationTokenResult.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeDelegationToken")]
    fn describe_delegation_token_with_options(
        &self,
        options: DescribeDelegationTokenOptions,
    ) -> DescribeDelegationTokenResult;

    /// Describes finalized as well as supported features.
    ///
    /// This is a convenience method for `Self::describe_features_with_options` with default
    /// options. See the overload for more details.
    ///
    /// # Returns
    ///
    /// the `DescribeFeaturesResult` containing the result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeFeatures")]
    fn describe_features(&self) -> DescribeFeaturesResult {
        self.describe_features_with_options(DescribeFeaturesOptions::default())
    }

    /// Describes finalized as well as supported features. The request is issued to any random
    /// broker.
    ///
    /// The following errors can be anticipated when calling `get()` on the future from the returned
    /// `DescribeFeaturesResult`:
    ///
    /// - `Error::is_timeout_error` If the request timed out before the describe operation could
    ///   finish.
    ///
    /// # Arguments
    ///
    /// - `options` - the options to use
    ///
    /// # Returns
    ///
    /// the `DescribeFeaturesResult` containing the result
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#describeFeatures")]
    fn describe_features_with_options(&self, options: DescribeFeaturesOptions) -> DescribeFeaturesResult;

    /// Applies specified updates to finalized features.
    ///
    /// This is a convenience method for `Self::update_features_with_options` with default options.
    /// See the overload for more details.
    ///
    /// # Arguments
    ///
    /// - `feature_updates` - the map of finalized feature name to `FeatureUpdate`
    ///
    /// # Returns
    ///
    /// the `UpdateFeaturesResult` containing the result
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `feature_updates` is empty or any feature name
    ///   is blank.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#updateFeatures")]
    fn update_features(&self, feature_updates: &HashMap<String, FeatureUpdate>) -> Result<UpdateFeaturesResult, Error> {
        self.update_features_with_options(feature_updates, UpdateFeaturesOptions::default())
    }

    /// Applies specified updates to finalized features. This operation is not transactional so some
    /// updates may succeed while the rest may fail.
    ///
    /// The API takes in a map of finalized feature names to `FeatureUpdate` that needs to be
    /// applied. Each entry in the map specifies the finalized feature to be added or updated or
    /// deleted, along with the new max feature version level value. This request is issued only to
    /// the controller since the API is only served by the controller. The return value contains an
    /// error code for each supplied `FeatureUpdate`, and the code indicates if the update succeeded
    /// or failed in the controller.
    ///
    /// - Downgrading a feature version level is not a common operation and should only be performed
    ///   when necessary. It is permitted only if the `FeatureUpdate` specifies the `upgradeType` as
    ///   either `UpgradeType::SAFE_DOWNGRADE` or `UpgradeType::UNSAFE_DOWNGRADE`.
    /// - `SAFE_DOWNGRADE`: Allows downgrades that do not lead to metadata loss.
    /// - `UNSAFE_DOWNGRADE`: Permits downgrades that might result in metadata loss.
    ///
    /// Note that even with these settings, certain downgrades may still be rejected by the
    /// controller if they are considered unsafe or impossible.
    ///
    /// - Deleting a finalized feature version is also not a common operation. To delete a feature,
    ///   set the `maxVersionLevel` to zero and specify the `upgradeType` as either
    ///   `UpgradeType::SAFE_DOWNGRADE` or `UpgradeType::UNSAFE_DOWNGRADE`.
    /// - The `UpgradeType::UPGRADE` type cannot be used when the `maxVersionLevel` is zero.
    ///   Attempting to do so will result in an `Error::is_local_illegal_argument_error`.
    ///
    /// The following errors can be anticipated when calling `get()` on the futures obtained from
    /// the returned `UpdateFeaturesResult`:
    ///
    /// - `Error::is_cluster_authorization_error` If the authenticated user didn't have alter access
    ///   to the cluster.
    /// - `Error::is_invalid_request_error` If the request details are invalid. e.g., a non-existing
    ///   finalized feature is attempted to be deleted or downgraded.
    /// - `Error::is_timeout_error` If the request timed out before the updates could finish. It
    ///   cannot be guaranteed whether the updates succeeded or not.
    /// - `Error::is_feature_update_failed_error` This means there was an unexpected error
    ///   encountered when the update was applied on the controller. There is no guarantee on
    ///   whether the update succeeded or failed. The best way to find out is to issue a
    ///   `Admin::describe_features` request.
    ///
    /// This operation is supported by brokers with version 2.7.0 or higher.
    ///
    /// # Arguments
    ///
    /// - `feature_updates` - the map of finalized feature name to `FeatureUpdate`
    /// - `options` - the options to use
    ///
    /// # Returns
    ///
    /// the `UpdateFeaturesResult` containing the result
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `feature_updates` is empty or any feature name
    ///   is blank.
    ///
    /// # Translation notes
    ///
    /// Java throws that `IllegalArgumentException` from the call; it is returned here as an `Err`.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#updateFeatures")]
    fn update_features_with_options(
        &self,
        feature_updates: &HashMap<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ) -> Result<UpdateFeaturesResult, Error>;

    /// Close the Admin and release all associated resources.
    ///
    /// See `Self::close_with_timeout`
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#close")]
    async fn close(&self) {
        self.close_with_timeout(Duration::from_millis(i64::MAX as u64)).await
    }

    /// Close the Admin client and release all associated resources.
    ///
    /// The close operation has a grace period during which current operations will be allowed to
    /// complete, specified by the given duration. New operations will not be accepted during the
    /// grace period. Once the grace period is over, all operations that have not yet been completed
    /// will be aborted with a `Error::is_timeout_error`.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The time to use for the wait time.
    ///
    /// # Translation notes
    ///
    /// Returns once `timeout` has elapsed, as Java's timed `join` does; a background task still
    /// running then is left running.
    #[doc(alias = "org.apache.kafka.clients.admin.Admin#close")]
    async fn close_with_timeout(&self, timeout: Duration);
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
    use crate::admin::mock_admin_client;
    use crate::common::{Node, TopicPartition, TopicPartitionInfo};

    fn admin() -> MockAdminClient {
        let mock = mock_admin_client::Builder::new()
            .build()
            .expect("a fresh builder has one broker");
        // The mock seeds brokers as `localhost:1000 + id`.
        let leader = Node::new(0, "localhost".to_string(), 1000);
        mock.add_topic(
            false,
            "topic",
            vec![TopicPartitionInfo::with_elr_last_known_elr(
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
    /// form must behave as `create_topics_with_options(.., CreateTopicsOptions::default())`
    /// does, mirroring `Admin#createTopics(Collection)`.
    #[tokio::test]
    async fn create_topics_forwards_to_the_options_form() {
        let mock = admin();
        mock.create_topics(&[NewTopic::with_num_partitions_replication_factor(
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
            .create_topics_with_options(
                &[NewTopic::with_num_partitions_replication_factor(
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
                .list_topics_with_options(ListTopicsOptions::default())
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
            mock.list_topics_with_options(ListTopicsOptions::default())
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

    /// `Admin.describeTopics(Collection<String>)` and
    /// `describeTopics(Collection<String>, DescribeTopicsOptions)` — the
    /// latter wraps the names in a `TopicCollection`, so both must agree with
    /// the `TopicCollection` form.
    #[tokio::test]
    async fn describe_topics_by_name_wraps_the_names_in_a_topic_collection() {
        let mock = admin();
        let names = ["topic".to_string()];

        let expected = mock
            .describe_topics_with_topics(TopicCollection::of_topic_names(names.to_vec()))
            .all_topic_names()
            .expect("described by name")
            .get()
            .await
            .expect("the topic exists");

        for described in [
            mock.describe_topics_with_topic_names(&names),
            mock.describe_topics_with_topic_names_options(&names, DescribeTopicsOptions::default()),
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

    /// `Admin.listConfigResources()` passes `Set.of()` — "every
    /// supported type" — not an empty result.
    #[tokio::test]
    async fn list_config_resources_asks_for_every_type() {
        let mock = admin();
        let mut convenience = mock.list_config_resources().all().get().await.unwrap();
        let mut explicit = mock
            .list_config_resources_with_options(&HashSet::new(), ListConfigResourcesOptions::default())
            .all()
            .get()
            .await
            .unwrap();
        convenience.sort_by_key(|resource| (resource.r#type() as i32, resource.name().to_string()));
        explicit.sort_by_key(|resource| (resource.r#type() as i32, resource.name().to_string()));

        assert_eq!(convenience, explicit);
        assert!(
            convenience
                .iter()
                .any(|resource| resource.r#type() == config_resource::Type::Topic),
            "an empty type set must not mean an empty result: {convenience:?}"
        );
    }

    /// `Admin.describeUserScramCredentials()` passes a `null` user list,
    /// which Rust spells as an empty slice; `(List)` passes the caller's list.
    /// The mock rejects both alike, so the
    /// assertion is on the error message the forward propagates.
    #[tokio::test]
    async fn describe_user_scram_credentials_forwards_both_shapes() {
        let mock = admin();
        for described in [
            mock.describe_user_scram_credentials(),
            mock.describe_user_scram_credentials_with_users(&["user".to_string()]),
            mock.describe_user_scram_credentials_with_users_options(
                &[],
                DescribeUserScramCredentialsOptions::default(),
            ),
        ] {
            let error = described.all().get().await.expect_err("the mock does not implement this");
            assert!(error.is_local_unsupported_operation_error(), "{error:?}");
            assert_eq!(error.message(), "Not implemented yet");
        }
    }

    /// `Admin.listPartitionReassignments()`, `(Set)` and
    /// `(options)`. The last passes `Optional.empty()`, so it must agree
    /// with the no-argument form rather than with an empty partition set.
    #[tokio::test]
    async fn list_partition_reassignments_forwards_every_shape() {
        let mock = admin();
        let partition = TopicPartition::new("topic", 0);

        let all = mock.list_partition_reassignments().reassignments().get().await.unwrap();
        let all_with_options = mock
            .list_partition_reassignments_with_options(ListPartitionReassignmentsOptions::default())
            .reassignments()
            .get()
            .await
            .unwrap();
        let selected = mock
            .list_partition_reassignments_with_partitions(HashSet::from([partition.clone()]))
            .reassignments()
            .get()
            .await
            .unwrap();
        let selected_with_options = mock
            .list_partition_reassignments_with_partitions_options(
                Some(HashSet::from([partition])),
                ListPartitionReassignmentsOptions::default(),
            )
            .reassignments()
            .get()
            .await
            .unwrap();

        // `PartitionReassignment` has no `PartialEq` (Java's does not override
        // `equals`), so the maps are compared by their replica lists.
        let view = |reassignments: &HashMap<TopicPartition, PartitionReassignment>| {
            reassignments
                .iter()
                .map(|(partition, r)| {
                    let lists = (
                        r.replicas().to_vec(),
                        r.adding_replicas().to_vec(),
                        r.removing_replicas().to_vec(),
                    );
                    (partition.clone(), lists)
                })
                .collect::<HashMap<_, _>>()
        };
        assert_eq!(view(&all), view(&all_with_options));
        assert_eq!(view(&selected), view(&selected_with_options));
        // Nothing is reassigning in a freshly seeded mock, so both are empty —
        // what matters is that `(options)` took the `None` branch, i.e. did not
        // request the empty *set*, which the mock would answer identically here
        // but which diverges as soon as a reassignment exists.
        assert!(all.is_empty());
    }

    /// `Admin.listConsumerGroupOffsets(String)` and
    /// `(String, options)` — the latter builds a singleton map with a
    /// default `ListConsumerGroupOffsetsSpec`, i.e. "all partitions".
    #[tokio::test]
    async fn list_consumer_group_offsets_by_group_id_builds_a_singleton_spec() {
        let mock = admin();
        let partition = TopicPartition::new("topic", 0);
        mock.update_consumer_group_offsets(HashMap::from([(partition.clone(), 17)]));

        let expected = mock
            .list_consumer_group_offsets_with_group_specs(&HashMap::from([(
                "group".to_string(),
                ListConsumerGroupOffsetsSpec::new(),
            )]))
            .partitions_to_offset_and_metadata()
            .expect("exactly one group")
            .get()
            .await
            .expect("offsets");

        for listed in [
            mock.list_consumer_group_offsets_with_group_id("group"),
            mock.list_consumer_group_offsets_with_group_id_options("group", ListConsumerGroupOffsetsOptions::default()),
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

    /// `Admin.close()` waits `Long.MAX_VALUE` milliseconds; the forward
    /// must not overflow the `Duration` conversion.
    #[tokio::test]
    async fn close_forwards_to_the_timeout_form() {
        admin().close().await;
        admin().close_with_timeout(Duration::from_millis(i64::MAX as u64)).await;
    }
}
