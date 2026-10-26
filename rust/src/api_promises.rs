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

//! The auto traits every public type promises (CLAUDE.md §3 "Super and auto
//! traits"): `Send + Sync`, except the consumers, which are `Send + !Sync`
//! (`consumer-threading.md` §2).
//!
//! Losing `Sync` (or `Send`) is a breaking change that no compile error inside
//! the crate reveals: a `Cell` cache or an `Rc` field compiles, and only a
//! downstream `tokio::spawn` or `Arc<T>` fails. One assertion per public struct
//! and enum makes it a failing test here instead. `cargo xtask lint-custom`
//! (`check-thread-safety-probe`) checks that every public type has its line;
//! a generic type is probed with `String` / `Vec<u8>` arguments.

#![expect(
    unused_imports,
    reason = "each test globs every public module, and uses the types of one package"
)]

use crate::admin::alter_config_op::*;
use crate::admin::config_entry::*;
use crate::admin::create_topics_result::*;
use crate::admin::delete_acls_result::*;
use crate::admin::describe_producers_result::*;
use crate::admin::describe_replica_log_dirs_result::*;
use crate::admin::feature_update::*;
use crate::admin::list_offsets_result::*;
use crate::admin::mock_admin_client::*;
use crate::admin::offset_spec::*;
use crate::admin::options::*;
use crate::admin::*;
use crate::common::acl::*;
use crate::common::config::config_resource::*;
use crate::common::config::*;
use crate::common::errors::record_deserialization_error::*;
use crate::common::errors::*;
use crate::common::header::*;
use crate::common::kafka_future::*;
use crate::common::metrics::gauge::*;
use crate::common::metrics::metric_value_provider::*;
use crate::common::metrics::sensor::*;
use crate::common::metrics::stats::sampled_stat::*;
use crate::common::metrics::stats::*;
use crate::common::metrics::*;
use crate::common::quota::*;
use crate::common::record::*;
use crate::common::resource::*;
use crate::common::security::auth::*;
use crate::common::security::scram::*;
use crate::common::security::token::delegation::*;
use crate::common::security::token::*;
use crate::common::security::*;
use crate::common::serialization::*;
use crate::common::topic_collection::*;
use crate::common::*;
use crate::consumer::close_options::*;
use crate::consumer::consumer_partition_assignor::*;
use crate::consumer::offset_commit_callback::*;
use crate::consumer::*;
use crate::producer::callback::*;
use crate::producer::*;
use crate::test_thread_safety::{is_send, is_sync};

#[test]
fn common_types_are_send_and_sync() {
    assert!(
        is_send!(AccessControlEntry) && is_sync!(AccessControlEntry),
        "AccessControlEntry"
    );
    assert!(
        is_send!(AccessControlEntryFilter) && is_sync!(AccessControlEntryFilter),
        "AccessControlEntryFilter"
    );
    assert!(is_send!(AclBinding) && is_sync!(AclBinding), "AclBinding");
    assert!(is_send!(AclBindingFilter) && is_sync!(AclBindingFilter), "AclBindingFilter");
    assert!(is_send!(AclOperation) && is_sync!(AclOperation), "AclOperation");
    assert!(is_send!(AclPermissionType) && is_sync!(AclPermissionType), "AclPermissionType");
    assert!(is_send!(Avg) && is_sync!(Avg), "Avg");
    assert!(
        is_send!(ByteArrayDeserializer) && is_sync!(ByteArrayDeserializer),
        "ByteArrayDeserializer"
    );
    assert!(
        is_send!(ByteArraySerializer) && is_sync!(ByteArraySerializer),
        "ByteArraySerializer"
    );
    assert!(is_send!(BytesDeserializer) && is_sync!(BytesDeserializer), "BytesDeserializer");
    assert!(is_send!(ClassicGroupState) && is_sync!(ClassicGroupState), "ClassicGroupState");
    assert!(
        is_send!(ClientQuotaAlteration) && is_sync!(ClientQuotaAlteration),
        "ClientQuotaAlteration"
    );
    assert!(is_send!(ClientQuotaEntity) && is_sync!(ClientQuotaEntity), "ClientQuotaEntity");
    assert!(is_send!(ClientQuotaFilter) && is_sync!(ClientQuotaFilter), "ClientQuotaFilter");
    assert!(
        is_send!(ClientQuotaFilterComponent) && is_sync!(ClientQuotaFilterComponent),
        "ClientQuotaFilterComponent"
    );
    assert!(is_send!(ClientQuotaMatch) && is_sync!(ClientQuotaMatch), "ClientQuotaMatch");
    assert!(
        is_send!(ClosureMeasurable<fn(&MetricConfig, i64) -> f64>)
            && is_sync!(ClosureMeasurable<fn(&MetricConfig, i64) -> f64>),
        "ClosureMeasurable<fn(&MetricConfig, i64) -> f64>"
    );
    assert!(is_send!(Cluster) && is_sync!(Cluster), "Cluster");
    assert!(is_send!(ClusterResource) && is_sync!(ClusterResource), "ClusterResource");
    assert!(is_send!(ConfigResource) && is_sync!(ConfigResource), "ConfigResource");
    assert!(is_send!(CumulativeCount) && is_sync!(CumulativeCount), "CumulativeCount");
    assert!(is_send!(CumulativeSum) && is_sync!(CumulativeSum), "CumulativeSum");
    assert!(is_send!(DelegationToken) && is_sync!(DelegationToken), "DelegationToken");
    assert!(
        is_send!(DeserializationErrorOrigin) && is_sync!(DeserializationErrorOrigin),
        "DeserializationErrorOrigin"
    );
    assert!(
        is_send!(DuplicateResourceError) && is_sync!(DuplicateResourceError),
        "DuplicateResourceError"
    );
    assert!(is_send!(ElectionType) && is_sync!(ElectionType), "ElectionType");
    assert!(is_send!(Error) && is_sync!(Error), "Error");
    assert!(
        is_send!(GroupAuthorizationError) && is_sync!(GroupAuthorizationError),
        "GroupAuthorizationError"
    );
    assert!(is_send!(GroupState) && is_sync!(GroupState), "GroupState");
    assert!(is_send!(GroupType) && is_sync!(GroupType), "GroupType");
    assert!(is_send!(InvalidTopicError) && is_sync!(InvalidTopicError), "InvalidTopicError");
    assert!(is_send!(IsolationLevel) && is_sync!(IsolationLevel), "IsolationLevel");
    assert!(is_send!(KafkaError) && is_sync!(KafkaError), "KafkaError");
    assert!(
        is_send!(KafkaFuture<String>) && is_sync!(KafkaFuture<String>),
        "KafkaFuture<String>"
    );
    assert!(is_send!(KafkaMetric) && is_sync!(KafkaMetric), "KafkaMetric");
    assert!(is_send!(KafkaPrincipal) && is_sync!(KafkaPrincipal), "KafkaPrincipal");
    assert!(
        is_send!(LocalCallbackError) && is_sync!(LocalCallbackError),
        "LocalCallbackError"
    );
    assert!(is_send!(Max) && is_sync!(Max), "Max");
    assert!(is_send!(Meter) && is_sync!(Meter), "Meter");
    assert!(is_send!(MetricConfig) && is_sync!(MetricConfig), "MetricConfig");
    assert!(is_send!(MetricName) && is_sync!(MetricName), "MetricName");
    assert!(
        is_send!(MetricNameTemplate) && is_sync!(MetricNameTemplate),
        "MetricNameTemplate"
    );
    assert!(is_send!(MetricValue) && is_sync!(MetricValue), "MetricValue");
    assert!(is_send!(Metrics) && is_sync!(Metrics), "Metrics");
    assert!(is_send!(Min) && is_sync!(Min), "Min");
    assert!(is_send!(NamedMeasurable) && is_sync!(NamedMeasurable), "NamedMeasurable");
    assert!(is_send!(Node) && is_sync!(Node), "Node");
    assert!(is_send!(Op) && is_sync!(Op), "Op");
    assert!(is_send!(PartitionInfo) && is_sync!(PartitionInfo), "PartitionInfo");
    assert!(is_send!(PatternType) && is_sync!(PatternType), "PatternType");
    assert!(is_send!(Quota) && is_sync!(Quota), "Quota");
    assert!(
        is_send!(QuotaViolationError) && is_sync!(QuotaViolationError),
        "QuotaViolationError"
    );
    assert!(is_send!(Rate) && is_sync!(Rate), "Rate");
    assert!(
        is_send!(RecordDeserializationError) && is_sync!(RecordDeserializationError),
        "RecordDeserializationError"
    );
    assert!(is_send!(RecordHeader) && is_sync!(RecordHeader), "RecordHeader");
    assert!(is_send!(RecordHeaders) && is_sync!(RecordHeaders), "RecordHeaders");
    assert!(
        is_send!(RecordTooLargeError) && is_sync!(RecordTooLargeError),
        "RecordTooLargeError"
    );
    assert!(is_send!(RecordingLevel) && is_sync!(RecordingLevel), "RecordingLevel");
    assert!(is_send!(Resource) && is_sync!(Resource), "Resource");
    assert!(
        is_send!(ResourceNotFoundError) && is_sync!(ResourceNotFoundError),
        "ResourceNotFoundError"
    );
    assert!(is_send!(ResourcePattern) && is_sync!(ResourcePattern), "ResourcePattern");
    assert!(
        is_send!(ResourcePatternFilter) && is_sync!(ResourcePatternFilter),
        "ResourcePatternFilter"
    );
    assert!(is_send!(ResourceType) && is_sync!(ResourceType), "ResourceType");
    assert!(is_send!(Sample) && is_sync!(Sample), "Sample");
    assert!(is_send!(SampledStat) && is_sync!(SampledStat), "SampledStat");
    assert!(is_send!(SaslConfigs) && is_sync!(SaslConfigs), "SaslConfigs");
    assert!(is_send!(SecurityProtocol) && is_sync!(SecurityProtocol), "SecurityProtocol");
    assert!(is_send!(Sensor) && is_sync!(Sensor), "Sensor");
    assert!(
        is_send!(SensorOptions<'static>) && is_sync!(SensorOptions<'static>),
        "SensorOptions<'static>"
    );
    assert!(
        is_send!(SensorOptionsBuilder<'static>) && is_sync!(SensorOptionsBuilder<'static>),
        "SensorOptionsBuilder<'static>"
    );
    assert!(is_send!(SimpleRate) && is_sync!(SimpleRate), "SimpleRate");
    assert!(is_send!(SslClientAuth) && is_sync!(SslClientAuth), "SslClientAuth");
    assert!(is_send!(SslConfigs) && is_sync!(SslConfigs), "SslConfigs");
    assert!(
        is_send!(StringDeserializer) && is_sync!(StringDeserializer),
        "StringDeserializer"
    );
    assert!(is_send!(StringSerializer) && is_sync!(StringSerializer), "StringSerializer");
    assert!(
        is_send!(ThrottlingQuotaExceededError) && is_sync!(ThrottlingQuotaExceededError),
        "ThrottlingQuotaExceededError"
    );
    assert!(is_send!(TimeUnit) && is_sync!(TimeUnit), "TimeUnit");
    assert!(is_send!(TimestampType) && is_sync!(TimestampType), "TimestampType");
    assert!(is_send!(TokenInformation) && is_sync!(TokenInformation), "TokenInformation");
    assert!(
        is_send!(TopicAuthorizationError) && is_sync!(TopicAuthorizationError),
        "TopicAuthorizationError"
    );
    assert!(is_send!(TopicCollection) && is_sync!(TopicCollection), "TopicCollection");
    assert!(is_send!(TopicIdCollection) && is_sync!(TopicIdCollection), "TopicIdCollection");
    assert!(is_send!(TopicIdPartition) && is_sync!(TopicIdPartition), "TopicIdPartition");
    assert!(
        is_send!(TopicNameCollection) && is_sync!(TopicNameCollection),
        "TopicNameCollection"
    );
    assert!(is_send!(TopicPartition) && is_sync!(TopicPartition), "TopicPartition");
    assert!(
        is_send!(TopicPartitionInfo) && is_sync!(TopicPartitionInfo),
        "TopicPartitionInfo"
    );
    assert!(
        is_send!(TopicPartitionReplica) && is_sync!(TopicPartitionReplica),
        "TopicPartitionReplica"
    );
    assert!(is_send!(Type) && is_sync!(Type), "Type");
    assert!(is_send!(Uuid) && is_sync!(Uuid), "Uuid");
    assert!(is_send!(Value) && is_sync!(Value), "Value");
    assert!(is_send!(WindowedCount) && is_sync!(WindowedCount), "WindowedCount");
    assert!(is_send!(WindowedSum) && is_sync!(WindowedSum), "WindowedSum");
}

#[test]
fn admin_types_are_send_and_sync() {
    assert!(
        is_send!(AbortTransactionOptions) && is_sync!(AbortTransactionOptions),
        "AbortTransactionOptions"
    );
    assert!(
        is_send!(AbortTransactionResult) && is_sync!(AbortTransactionResult),
        "AbortTransactionResult"
    );
    assert!(
        is_send!(AbortTransactionSpec) && is_sync!(AbortTransactionSpec),
        "AbortTransactionSpec"
    );
    assert!(is_send!(AdminClient) && is_sync!(AdminClient), "AdminClient");
    assert!(is_send!(AdminClientConfig) && is_sync!(AdminClientConfig), "AdminClientConfig");
    assert!(
        is_send!(AlterClientQuotasOptions) && is_sync!(AlterClientQuotasOptions),
        "AlterClientQuotasOptions"
    );
    assert!(
        is_send!(AlterClientQuotasResult) && is_sync!(AlterClientQuotasResult),
        "AlterClientQuotasResult"
    );
    assert!(is_send!(AlterConfigOp) && is_sync!(AlterConfigOp), "AlterConfigOp");
    assert!(
        is_send!(AlterConfigsOptions) && is_sync!(AlterConfigsOptions),
        "AlterConfigsOptions"
    );
    assert!(
        is_send!(AlterConfigsResult) && is_sync!(AlterConfigsResult),
        "AlterConfigsResult"
    );
    assert!(
        is_send!(AlterConsumerGroupOffsetsOptions) && is_sync!(AlterConsumerGroupOffsetsOptions),
        "AlterConsumerGroupOffsetsOptions"
    );
    assert!(
        is_send!(AlterConsumerGroupOffsetsResult) && is_sync!(AlterConsumerGroupOffsetsResult),
        "AlterConsumerGroupOffsetsResult"
    );
    assert!(
        is_send!(AlterPartitionReassignmentsOptions) && is_sync!(AlterPartitionReassignmentsOptions),
        "AlterPartitionReassignmentsOptions"
    );
    assert!(
        is_send!(AlterPartitionReassignmentsResult) && is_sync!(AlterPartitionReassignmentsResult),
        "AlterPartitionReassignmentsResult"
    );
    assert!(
        is_send!(AlterReplicaLogDirsOptions) && is_sync!(AlterReplicaLogDirsOptions),
        "AlterReplicaLogDirsOptions"
    );
    assert!(
        is_send!(AlterReplicaLogDirsResult) && is_sync!(AlterReplicaLogDirsResult),
        "AlterReplicaLogDirsResult"
    );
    assert!(
        is_send!(AlterUserScramCredentialsOptions) && is_sync!(AlterUserScramCredentialsOptions),
        "AlterUserScramCredentialsOptions"
    );
    assert!(
        is_send!(AlterUserScramCredentialsResult) && is_sync!(AlterUserScramCredentialsResult),
        "AlterUserScramCredentialsResult"
    );
    assert!(is_send!(Builder) && is_sync!(Builder), "Builder");
    assert!(
        is_send!(ClassicGroupDescription) && is_sync!(ClassicGroupDescription),
        "ClassicGroupDescription"
    );
    assert!(is_send!(Config) && is_sync!(Config), "Config");
    assert!(is_send!(ConfigEntry) && is_sync!(ConfigEntry), "ConfigEntry");
    assert!(
        is_send!(ConfigEntryOptions) && is_sync!(ConfigEntryOptions),
        "ConfigEntryOptions"
    );
    assert!(
        is_send!(ConfigEntryOptionsBuilder) && is_sync!(ConfigEntryOptionsBuilder),
        "ConfigEntryOptionsBuilder"
    );
    assert!(is_send!(ConfigSource) && is_sync!(ConfigSource), "ConfigSource");
    assert!(is_send!(ConfigSynonym) && is_sync!(ConfigSynonym), "ConfigSynonym");
    assert!(is_send!(ConfigType) && is_sync!(ConfigType), "ConfigType");
    assert!(
        is_send!(ConsumerGroupDescription) && is_sync!(ConsumerGroupDescription),
        "ConsumerGroupDescription"
    );
    assert!(is_send!(CreateAclsOptions) && is_sync!(CreateAclsOptions), "CreateAclsOptions");
    assert!(is_send!(CreateAclsResult) && is_sync!(CreateAclsResult), "CreateAclsResult");
    assert!(
        is_send!(CreateDelegationTokenOptions) && is_sync!(CreateDelegationTokenOptions),
        "CreateDelegationTokenOptions"
    );
    assert!(
        is_send!(CreateDelegationTokenResult) && is_sync!(CreateDelegationTokenResult),
        "CreateDelegationTokenResult"
    );
    assert!(
        is_send!(CreatePartitionsOptions) && is_sync!(CreatePartitionsOptions),
        "CreatePartitionsOptions"
    );
    assert!(
        is_send!(CreatePartitionsResult) && is_sync!(CreatePartitionsResult),
        "CreatePartitionsResult"
    );
    assert!(
        is_send!(CreateTopicsOptions) && is_sync!(CreateTopicsOptions),
        "CreateTopicsOptions"
    );
    assert!(
        is_send!(CreateTopicsResult) && is_sync!(CreateTopicsResult),
        "CreateTopicsResult"
    );
    assert!(is_send!(DeleteAclsOptions) && is_sync!(DeleteAclsOptions), "DeleteAclsOptions");
    assert!(is_send!(DeleteAclsResult) && is_sync!(DeleteAclsResult), "DeleteAclsResult");
    assert!(
        is_send!(DeleteConsumerGroupOffsetsOptions) && is_sync!(DeleteConsumerGroupOffsetsOptions),
        "DeleteConsumerGroupOffsetsOptions"
    );
    assert!(
        is_send!(DeleteConsumerGroupOffsetsResult) && is_sync!(DeleteConsumerGroupOffsetsResult),
        "DeleteConsumerGroupOffsetsResult"
    );
    assert!(
        is_send!(DeleteConsumerGroupsOptions) && is_sync!(DeleteConsumerGroupsOptions),
        "DeleteConsumerGroupsOptions"
    );
    assert!(
        is_send!(DeleteConsumerGroupsResult) && is_sync!(DeleteConsumerGroupsResult),
        "DeleteConsumerGroupsResult"
    );
    assert!(
        is_send!(DeleteRecordsOptions) && is_sync!(DeleteRecordsOptions),
        "DeleteRecordsOptions"
    );
    assert!(
        is_send!(DeleteRecordsResult) && is_sync!(DeleteRecordsResult),
        "DeleteRecordsResult"
    );
    assert!(
        is_send!(DeleteTopicsOptions) && is_sync!(DeleteTopicsOptions),
        "DeleteTopicsOptions"
    );
    assert!(
        is_send!(DeleteTopicsResult) && is_sync!(DeleteTopicsResult),
        "DeleteTopicsResult"
    );
    assert!(is_send!(DeletedRecords) && is_sync!(DeletedRecords), "DeletedRecords");
    assert!(
        is_send!(DescribeAclsOptions) && is_sync!(DescribeAclsOptions),
        "DescribeAclsOptions"
    );
    assert!(
        is_send!(DescribeAclsResult) && is_sync!(DescribeAclsResult),
        "DescribeAclsResult"
    );
    assert!(
        is_send!(DescribeClassicGroupsOptions) && is_sync!(DescribeClassicGroupsOptions),
        "DescribeClassicGroupsOptions"
    );
    assert!(
        is_send!(DescribeClassicGroupsResult) && is_sync!(DescribeClassicGroupsResult),
        "DescribeClassicGroupsResult"
    );
    assert!(
        is_send!(DescribeClientQuotasOptions) && is_sync!(DescribeClientQuotasOptions),
        "DescribeClientQuotasOptions"
    );
    assert!(
        is_send!(DescribeClientQuotasResult) && is_sync!(DescribeClientQuotasResult),
        "DescribeClientQuotasResult"
    );
    assert!(
        is_send!(DescribeClusterOptions) && is_sync!(DescribeClusterOptions),
        "DescribeClusterOptions"
    );
    assert!(
        is_send!(DescribeClusterResult) && is_sync!(DescribeClusterResult),
        "DescribeClusterResult"
    );
    assert!(
        is_send!(DescribeConfigsOptions) && is_sync!(DescribeConfigsOptions),
        "DescribeConfigsOptions"
    );
    assert!(
        is_send!(DescribeConfigsResult) && is_sync!(DescribeConfigsResult),
        "DescribeConfigsResult"
    );
    assert!(
        is_send!(DescribeConsumerGroupsOptions) && is_sync!(DescribeConsumerGroupsOptions),
        "DescribeConsumerGroupsOptions"
    );
    assert!(
        is_send!(DescribeConsumerGroupsResult) && is_sync!(DescribeConsumerGroupsResult),
        "DescribeConsumerGroupsResult"
    );
    assert!(
        is_send!(DescribeDelegationTokenOptions) && is_sync!(DescribeDelegationTokenOptions),
        "DescribeDelegationTokenOptions"
    );
    assert!(
        is_send!(DescribeDelegationTokenResult) && is_sync!(DescribeDelegationTokenResult),
        "DescribeDelegationTokenResult"
    );
    assert!(
        is_send!(DescribeFeaturesOptions) && is_sync!(DescribeFeaturesOptions),
        "DescribeFeaturesOptions"
    );
    assert!(
        is_send!(DescribeFeaturesResult) && is_sync!(DescribeFeaturesResult),
        "DescribeFeaturesResult"
    );
    assert!(
        is_send!(DescribeLogDirsOptions) && is_sync!(DescribeLogDirsOptions),
        "DescribeLogDirsOptions"
    );
    assert!(
        is_send!(DescribeLogDirsResult) && is_sync!(DescribeLogDirsResult),
        "DescribeLogDirsResult"
    );
    assert!(
        is_send!(DescribeProducersOptions) && is_sync!(DescribeProducersOptions),
        "DescribeProducersOptions"
    );
    assert!(
        is_send!(DescribeProducersResult) && is_sync!(DescribeProducersResult),
        "DescribeProducersResult"
    );
    assert!(
        is_send!(DescribeReplicaLogDirsOptions) && is_sync!(DescribeReplicaLogDirsOptions),
        "DescribeReplicaLogDirsOptions"
    );
    assert!(
        is_send!(DescribeReplicaLogDirsResult) && is_sync!(DescribeReplicaLogDirsResult),
        "DescribeReplicaLogDirsResult"
    );
    assert!(
        is_send!(DescribeTopicsOptions) && is_sync!(DescribeTopicsOptions),
        "DescribeTopicsOptions"
    );
    assert!(
        is_send!(DescribeTopicsResult) && is_sync!(DescribeTopicsResult),
        "DescribeTopicsResult"
    );
    assert!(
        is_send!(DescribeTransactionsOptions) && is_sync!(DescribeTransactionsOptions),
        "DescribeTransactionsOptions"
    );
    assert!(
        is_send!(DescribeTransactionsResult) && is_sync!(DescribeTransactionsResult),
        "DescribeTransactionsResult"
    );
    assert!(
        is_send!(DescribeUserScramCredentialsOptions) && is_sync!(DescribeUserScramCredentialsOptions),
        "DescribeUserScramCredentialsOptions"
    );
    assert!(
        is_send!(DescribeUserScramCredentialsResult) && is_sync!(DescribeUserScramCredentialsResult),
        "DescribeUserScramCredentialsResult"
    );
    assert!(
        is_send!(ElectLeadersOptions) && is_sync!(ElectLeadersOptions),
        "ElectLeadersOptions"
    );
    assert!(
        is_send!(ElectLeadersResult) && is_sync!(ElectLeadersResult),
        "ElectLeadersResult"
    );
    assert!(is_send!(EndpointType) && is_sync!(EndpointType), "EndpointType");
    assert!(
        is_send!(ExpireDelegationTokenOptions) && is_sync!(ExpireDelegationTokenOptions),
        "ExpireDelegationTokenOptions"
    );
    assert!(
        is_send!(ExpireDelegationTokenResult) && is_sync!(ExpireDelegationTokenResult),
        "ExpireDelegationTokenResult"
    );
    assert!(is_send!(FeatureMetadata) && is_sync!(FeatureMetadata), "FeatureMetadata");
    assert!(is_send!(FeatureUpdate) && is_sync!(FeatureUpdate), "FeatureUpdate");
    assert!(
        is_send!(FenceProducersOptions) && is_sync!(FenceProducersOptions),
        "FenceProducersOptions"
    );
    assert!(
        is_send!(FenceProducersResult) && is_sync!(FenceProducersResult),
        "FenceProducersResult"
    );
    assert!(is_send!(FilterResult) && is_sync!(FilterResult), "FilterResult");
    assert!(is_send!(FilterResults) && is_sync!(FilterResults), "FilterResults");
    assert!(
        is_send!(FinalizedVersionRange) && is_sync!(FinalizedVersionRange),
        "FinalizedVersionRange"
    );
    assert!(is_send!(GroupListing) && is_sync!(GroupListing), "GroupListing");
    assert!(is_send!(KafkaAdminClient) && is_sync!(KafkaAdminClient), "KafkaAdminClient");
    assert!(
        is_send!(ListConfigResourcesOptions) && is_sync!(ListConfigResourcesOptions),
        "ListConfigResourcesOptions"
    );
    assert!(
        is_send!(ListConfigResourcesResult) && is_sync!(ListConfigResourcesResult),
        "ListConfigResourcesResult"
    );
    assert!(
        is_send!(ListConsumerGroupOffsetsOptions) && is_sync!(ListConsumerGroupOffsetsOptions),
        "ListConsumerGroupOffsetsOptions"
    );
    assert!(
        is_send!(ListConsumerGroupOffsetsResult) && is_sync!(ListConsumerGroupOffsetsResult),
        "ListConsumerGroupOffsetsResult"
    );
    assert!(
        is_send!(ListConsumerGroupOffsetsSpec) && is_sync!(ListConsumerGroupOffsetsSpec),
        "ListConsumerGroupOffsetsSpec"
    );
    assert!(is_send!(ListGroupsOptions) && is_sync!(ListGroupsOptions), "ListGroupsOptions");
    assert!(is_send!(ListGroupsResult) && is_sync!(ListGroupsResult), "ListGroupsResult");
    assert!(
        is_send!(ListOffsetsOptions) && is_sync!(ListOffsetsOptions),
        "ListOffsetsOptions"
    );
    assert!(is_send!(ListOffsetsResult) && is_sync!(ListOffsetsResult), "ListOffsetsResult");
    assert!(
        is_send!(ListOffsetsResultInfo) && is_sync!(ListOffsetsResultInfo),
        "ListOffsetsResultInfo"
    );
    assert!(
        is_send!(ListPartitionReassignmentsOptions) && is_sync!(ListPartitionReassignmentsOptions),
        "ListPartitionReassignmentsOptions"
    );
    assert!(
        is_send!(ListPartitionReassignmentsResult) && is_sync!(ListPartitionReassignmentsResult),
        "ListPartitionReassignmentsResult"
    );
    assert!(is_send!(ListTopicsOptions) && is_sync!(ListTopicsOptions), "ListTopicsOptions");
    assert!(is_send!(ListTopicsResult) && is_sync!(ListTopicsResult), "ListTopicsResult");
    assert!(
        is_send!(ListTransactionsOptions) && is_sync!(ListTransactionsOptions),
        "ListTransactionsOptions"
    );
    assert!(
        is_send!(ListTransactionsResult) && is_sync!(ListTransactionsResult),
        "ListTransactionsResult"
    );
    assert!(is_send!(LogDirDescription) && is_sync!(LogDirDescription), "LogDirDescription");
    assert!(is_send!(MemberAssignment) && is_sync!(MemberAssignment), "MemberAssignment");
    assert!(is_send!(MemberDescription) && is_sync!(MemberDescription), "MemberDescription");
    assert!(is_send!(MemberToRemove) && is_sync!(MemberToRemove), "MemberToRemove");
    assert!(is_send!(MockAdminClient) && is_sync!(MockAdminClient), "MockAdminClient");
    assert!(
        is_send!(NewPartitionReassignment) && is_sync!(NewPartitionReassignment),
        "NewPartitionReassignment"
    );
    assert!(is_send!(NewPartitions) && is_sync!(NewPartitions), "NewPartitions");
    assert!(is_send!(NewTopic) && is_sync!(NewTopic), "NewTopic");
    assert!(is_send!(OffsetSpec) && is_sync!(OffsetSpec), "OffsetSpec");
    assert!(is_send!(OpType) && is_sync!(OpType), "OpType");
    assert!(
        is_send!(PartitionProducerState) && is_sync!(PartitionProducerState),
        "PartitionProducerState"
    );
    assert!(
        is_send!(PartitionReassignment) && is_sync!(PartitionReassignment),
        "PartitionReassignment"
    );
    assert!(is_send!(ProducerState) && is_sync!(ProducerState), "ProducerState");
    assert!(is_send!(RecordsToDelete) && is_sync!(RecordsToDelete), "RecordsToDelete");
    assert!(
        is_send!(RemoveMembersFromConsumerGroupOptions) && is_sync!(RemoveMembersFromConsumerGroupOptions),
        "RemoveMembersFromConsumerGroupOptions"
    );
    assert!(
        is_send!(RemoveMembersFromConsumerGroupResult) && is_sync!(RemoveMembersFromConsumerGroupResult),
        "RemoveMembersFromConsumerGroupResult"
    );
    assert!(
        is_send!(RenewDelegationTokenOptions) && is_sync!(RenewDelegationTokenOptions),
        "RenewDelegationTokenOptions"
    );
    assert!(
        is_send!(RenewDelegationTokenResult) && is_sync!(RenewDelegationTokenResult),
        "RenewDelegationTokenResult"
    );
    assert!(is_send!(ReplicaInfo) && is_sync!(ReplicaInfo), "ReplicaInfo");
    assert!(is_send!(ReplicaLogDirInfo) && is_sync!(ReplicaLogDirInfo), "ReplicaLogDirInfo");
    assert!(
        is_send!(ScramCredentialInfo) && is_sync!(ScramCredentialInfo),
        "ScramCredentialInfo"
    );
    assert!(is_send!(ScramMechanism) && is_sync!(ScramMechanism), "ScramMechanism");
    assert!(
        is_send!(SupportedVersionRange) && is_sync!(SupportedVersionRange),
        "SupportedVersionRange"
    );
    assert!(
        is_send!(TerminateTransactionOptions) && is_sync!(TerminateTransactionOptions),
        "TerminateTransactionOptions"
    );
    assert!(
        is_send!(TerminateTransactionResult) && is_sync!(TerminateTransactionResult),
        "TerminateTransactionResult"
    );
    assert!(is_send!(TimestampSpec) && is_sync!(TimestampSpec), "TimestampSpec");
    assert!(is_send!(TopicDescription) && is_sync!(TopicDescription), "TopicDescription");
    assert!(is_send!(TopicListing) && is_sync!(TopicListing), "TopicListing");
    assert!(
        is_send!(TopicMetadataAndConfig) && is_sync!(TopicMetadataAndConfig),
        "TopicMetadataAndConfig"
    );
    assert!(
        is_send!(TransactionDescription) && is_sync!(TransactionDescription),
        "TransactionDescription"
    );
    assert!(
        is_send!(TransactionListing) && is_sync!(TransactionListing),
        "TransactionListing"
    );
    assert!(is_send!(TransactionState) && is_sync!(TransactionState), "TransactionState");
    assert!(
        is_send!(UnregisterControllerOptions) && is_sync!(UnregisterControllerOptions),
        "UnregisterControllerOptions"
    );
    assert!(
        is_send!(UnregisterControllerResult) && is_sync!(UnregisterControllerResult),
        "UnregisterControllerResult"
    );
    assert!(
        is_send!(UpdateFeaturesOptions) && is_sync!(UpdateFeaturesOptions),
        "UpdateFeaturesOptions"
    );
    assert!(
        is_send!(UpdateFeaturesResult) && is_sync!(UpdateFeaturesResult),
        "UpdateFeaturesResult"
    );
    assert!(is_send!(UpgradeType) && is_sync!(UpgradeType), "UpgradeType");
    assert!(
        is_send!(UserScramCredentialAlteration) && is_sync!(UserScramCredentialAlteration),
        "UserScramCredentialAlteration"
    );
    assert!(
        is_send!(UserScramCredentialDeletion) && is_sync!(UserScramCredentialDeletion),
        "UserScramCredentialDeletion"
    );
    assert!(
        is_send!(UserScramCredentialUpsertion) && is_sync!(UserScramCredentialUpsertion),
        "UserScramCredentialUpsertion"
    );
    assert!(
        is_send!(UserScramCredentialsDescription) && is_sync!(UserScramCredentialsDescription),
        "UserScramCredentialsDescription"
    );
}

#[test]
fn producer_types_are_send_and_sync() {
    assert!(
        is_send!(KafkaProducer<String, Vec<u8>>) && is_sync!(KafkaProducer<String, Vec<u8>>),
        "KafkaProducer<String, Vec<u8>>"
    );
    assert!(
        is_send!(MockProducer<String, Vec<u8>>) && is_sync!(MockProducer<String, Vec<u8>>),
        "MockProducer<String, Vec<u8>>"
    );
    assert!(
        is_send!(MockProducerOptions<String, Vec<u8>>) && is_sync!(MockProducerOptions<String, Vec<u8>>),
        "MockProducerOptions<String, Vec<u8>>"
    );
    assert!(
        is_send!(MockProducerOptionsBuilder<String, Vec<u8>>) && is_sync!(MockProducerOptionsBuilder<String, Vec<u8>>),
        "MockProducerOptionsBuilder<String, Vec<u8>>"
    );
    assert!(is_send!(ProducerConfig) && is_sync!(ProducerConfig), "ProducerConfig");
    assert!(
        is_send!(ProducerRecord<String, Vec<u8>>) && is_sync!(ProducerRecord<String, Vec<u8>>),
        "ProducerRecord<String, Vec<u8>>"
    );
    assert!(
        is_send!(ProducerRecordOptions<String, Vec<u8>>) && is_sync!(ProducerRecordOptions<String, Vec<u8>>),
        "ProducerRecordOptions<String, Vec<u8>>"
    );
    assert!(
        is_send!(ProducerRecordOptionsBuilder<String, Vec<u8>>)
            && is_sync!(ProducerRecordOptionsBuilder<String, Vec<u8>>),
        "ProducerRecordOptionsBuilder<String, Vec<u8>>"
    );
    assert!(is_send!(RecordMetadata) && is_sync!(RecordMetadata), "RecordMetadata");
    assert!(
        is_send!(RoundRobinPartitioner) && is_sync!(RoundRobinPartitioner),
        "RoundRobinPartitioner"
    );
}

#[test]
fn consumer_types_are_send_and_sync() {
    assert!(is_send!(Assignment) && is_sync!(Assignment), "Assignment");
    assert!(is_send!(CloseOptions) && is_sync!(CloseOptions), "CloseOptions");
    assert!(
        is_send!(ConsumerCommitFailedError) && is_sync!(ConsumerCommitFailedError),
        "ConsumerCommitFailedError"
    );
    assert!(is_send!(ConsumerConfig) && is_sync!(ConsumerConfig), "ConsumerConfig");
    assert!(is_send!(ConsumerHandle) && is_sync!(ConsumerHandle), "ConsumerHandle");
    assert!(
        is_send!(ConsumerLogTruncationError) && is_sync!(ConsumerLogTruncationError),
        "ConsumerLogTruncationError"
    );
    assert!(
        is_send!(ConsumerNoOffsetForPartitionError) && is_sync!(ConsumerNoOffsetForPartitionError),
        "ConsumerNoOffsetForPartitionError"
    );
    assert!(
        is_send!(ConsumerOffsetOutOfRangeError) && is_sync!(ConsumerOffsetOutOfRangeError),
        "ConsumerOffsetOutOfRangeError"
    );
    assert!(
        is_send!(ConsumerRecord<String, Vec<u8>>) && is_sync!(ConsumerRecord<String, Vec<u8>>),
        "ConsumerRecord<String, Vec<u8>>"
    );
    assert!(
        is_send!(ConsumerRecordOptions<String, Vec<u8>>) && is_sync!(ConsumerRecordOptions<String, Vec<u8>>),
        "ConsumerRecordOptions<String, Vec<u8>>"
    );
    assert!(
        is_send!(ConsumerRecordOptionsBuilder<String, Vec<u8>>)
            && is_sync!(ConsumerRecordOptionsBuilder<String, Vec<u8>>),
        "ConsumerRecordOptionsBuilder<String, Vec<u8>>"
    );
    assert!(
        is_send!(ConsumerRecords<String, Vec<u8>>) && is_sync!(ConsumerRecords<String, Vec<u8>>),
        "ConsumerRecords<String, Vec<u8>>"
    );
    assert!(
        is_send!(ConsumerRetriableCommitFailedError) && is_sync!(ConsumerRetriableCommitFailedError),
        "ConsumerRetriableCommitFailedError"
    );
    assert!(
        is_send!(GroupMembershipOperation) && is_sync!(GroupMembershipOperation),
        "GroupMembershipOperation"
    );
    assert!(is_send!(GroupProtocol) && is_sync!(GroupProtocol), "GroupProtocol");
    assert!(is_send!(OffsetAndMetadata) && is_sync!(OffsetAndMetadata), "OffsetAndMetadata");
    assert!(
        is_send!(OffsetAndTimestamp) && is_sync!(OffsetAndTimestamp),
        "OffsetAndTimestamp"
    );
    assert!(is_send!(Subscription) && is_sync!(Subscription), "Subscription");
    assert!(
        is_send!(SubscriptionOptions) && is_sync!(SubscriptionOptions),
        "SubscriptionOptions"
    );
    assert!(
        is_send!(SubscriptionOptionsBuilder) && is_sync!(SubscriptionOptionsBuilder),
        "SubscriptionOptionsBuilder"
    );
    assert!(
        is_send!(SubscriptionPattern) && is_sync!(SubscriptionPattern),
        "SubscriptionPattern"
    );
}

/// The consumers are `Send + !Sync`: Java's consumer is single-threaded and
/// enforces it at runtime with `acquire()`, Rust at compile time, through the
/// `PhantomData<Cell<()>>` marker each consumer holds and `&mut self` on the
/// methods Java guards (`consumer-threading.md` §2). `ConsumerHandle` is their
/// `Sync` cross-thread view, probed above.
#[test]
fn consumers_are_send_but_not_sync() {
    assert!(is_send!(KafkaConsumer<String, Vec<u8>>) && !is_sync!(KafkaConsumer<String, Vec<u8>>));
    assert!(is_send!(MockConsumer<String, Vec<u8>>) && !is_sync!(MockConsumer<String, Vec<u8>>));
    assert!(is_send!(Box<dyn DynConsumer<String, Vec<u8>>>) && !is_sync!(Box<dyn DynConsumer<String, Vec<u8>>>));
}
