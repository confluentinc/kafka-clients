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

//! Request/response framework for Kafka RPCs (org.apache.kafka.common.requests)
//!
//! This module provides the request and response header types, the abstract
//! request/response framework, and concrete implementations for ApiVersions
//! and Metadata RPCs.
//!
//! # Dead-code lint
//!
//! Java marks this package "not a supported API", so it is crate-private. It
//! is translated in full (DoD #2), but the client uses only part of it; the
//! rest has no caller yet, or only the translated tests. Nothing outside the
//! crate can reach it, so the module allows dead code rather than dropping
//! Java methods.

#![expect(dead_code, unused_imports)]

mod abstract_request;
mod abstract_response;
pub mod add_offsets_to_txn_request;
mod add_offsets_to_txn_response;
pub mod add_partitions_to_txn_request;
mod add_partitions_to_txn_response;
pub mod alter_client_quotas_request;
mod alter_client_quotas_response;
pub mod alter_partition_reassignments_request;
mod alter_partition_reassignments_response;
pub mod alter_replica_log_dirs_request;
mod alter_replica_log_dirs_response;
pub mod alter_user_scram_credentials_request;
mod alter_user_scram_credentials_response;
pub mod api_versions_request;
pub mod api_versions_response;
pub mod consumer_group_describe_request;
mod consumer_group_describe_response;
pub mod consumer_group_heartbeat_request;
mod consumer_group_heartbeat_response;
mod correlation_id_mismatch_error;
pub mod create_acls_request;
mod create_acls_response;
pub mod create_delegation_token_request;
mod create_delegation_token_response;
pub mod create_partitions_request;
mod create_partitions_response;
pub mod create_topics_request;
mod create_topics_response;
pub mod delete_acls_request;
mod delete_acls_response;
pub mod delete_groups_request;
mod delete_groups_response;
pub mod delete_records_request;
mod delete_records_response;
pub mod delete_topics_request;
mod delete_topics_response;
pub mod describe_acls_request;
mod describe_acls_response;
pub mod describe_client_quotas_request;
mod describe_client_quotas_response;
pub mod describe_cluster_request;
mod describe_cluster_response;
pub mod describe_configs_request;
mod describe_configs_response;
pub mod describe_delegation_token_request;
mod describe_delegation_token_response;
pub mod describe_groups_request;
mod describe_groups_response;
pub mod describe_log_dirs_request;
mod describe_log_dirs_response;
pub mod describe_producers_request;
mod describe_producers_response;
pub mod describe_transactions_request;
mod describe_transactions_response;
pub mod describe_user_scram_credentials_request;
mod describe_user_scram_credentials_response;
pub mod elect_leaders_request;
mod elect_leaders_response;
pub mod end_txn_request;
mod end_txn_response;
pub mod expire_delegation_token_request;
mod expire_delegation_token_response;
mod fetch_metadata;
pub use fetch_metadata::FetchMetadata;
pub mod fetch_request;
mod fetch_response;
pub mod find_coordinator_request;
mod find_coordinator_response;
pub mod incremental_alter_configs_request;
mod incremental_alter_configs_response;
pub mod init_producer_id_request;
mod init_producer_id_response;
mod join_group_request;
pub mod leave_group_request;
mod leave_group_response;
pub mod list_config_resources_request;
mod list_config_resources_response;
pub mod list_groups_request;
mod list_groups_response;
pub mod list_offsets_request;
mod list_offsets_response;
pub mod list_partition_reassignments_request;
mod list_partition_reassignments_response;
pub mod list_transactions_request;
mod list_transactions_response;
pub mod metadata_request;
pub mod metadata_response;
pub mod offset_commit_request;
mod offset_commit_response;
pub mod offset_delete_request;
mod offset_delete_response;
pub mod offset_fetch_request;
pub mod offset_fetch_response;
pub mod offsets_for_leader_epoch_request;
mod offsets_for_leader_epoch_response;
pub mod produce_request;
pub mod produce_response;
pub mod renew_delegation_token_request;
mod renew_delegation_token_response;
mod request_and_size;
mod request_header;
mod request_test_utils;
mod request_utils;
mod response_header;
pub mod sasl_authenticate_request;
mod sasl_authenticate_response;
pub mod sasl_handshake_request;
mod sasl_handshake_response;
mod send_builder;
mod transaction_result;
pub mod txn_offset_commit_request;
mod txn_offset_commit_response;

pub use abstract_request::{AbstractRequest, RequestBuilder};
pub use abstract_response::{AbstractResponse, ConcreteResponse};
pub use add_offsets_to_txn_request::AddOffsetsToTxnRequest;
pub use add_offsets_to_txn_response::AddOffsetsToTxnResponse;
pub use add_partitions_to_txn_request::AddPartitionsToTxnRequest;
pub use add_partitions_to_txn_response::AddPartitionsToTxnResponse;
pub mod update_features_request;
mod update_features_response;
pub mod write_txn_markers_request;
mod write_txn_markers_response;

pub use alter_client_quotas_request::AlterClientQuotasRequest;
pub use alter_client_quotas_response::AlterClientQuotasResponse;
pub use alter_partition_reassignments_request::AlterPartitionReassignmentsRequest;
pub use alter_partition_reassignments_response::AlterPartitionReassignmentsResponse;
pub use alter_replica_log_dirs_request::AlterReplicaLogDirsRequest;
pub use alter_replica_log_dirs_response::AlterReplicaLogDirsResponse;
pub use alter_user_scram_credentials_request::AlterUserScramCredentialsRequest;
pub use alter_user_scram_credentials_response::AlterUserScramCredentialsResponse;
pub use api_versions_request::ApiVersionsRequest;
pub use api_versions_response::ApiVersionsResponse;
pub use consumer_group_describe_request::ConsumerGroupDescribeRequest;
pub use consumer_group_describe_response::ConsumerGroupDescribeResponse;
pub use consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
pub use consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
pub use correlation_id_mismatch_error::CorrelationIdMismatchError;
pub use create_acls_request::CreateAclsRequest;
pub use create_acls_response::CreateAclsResponse;
pub use create_delegation_token_request::CreateDelegationTokenRequest;
pub use create_delegation_token_response::{
    CreateDelegationTokenResponse, CreateDelegationTokenResponseOptions, CreateDelegationTokenResponseOptionsBuilder,
};
pub use create_partitions_request::CreatePartitionsRequest;
pub use create_partitions_response::CreatePartitionsResponse;
pub use create_topics_request::CreateTopicsRequest;
pub use create_topics_response::CreateTopicsResponse;
pub use delete_acls_request::DeleteAclsRequest;
pub use delete_acls_response::DeleteAclsResponse;
pub use delete_groups_request::DeleteGroupsRequest;
pub use delete_groups_response::DeleteGroupsResponse;
pub use delete_records_request::DeleteRecordsRequest;
pub use delete_records_response::DeleteRecordsResponse;
pub use delete_topics_request::DeleteTopicsRequest;
pub use delete_topics_response::DeleteTopicsResponse;
pub use describe_acls_request::DescribeAclsRequest;
pub use describe_acls_response::DescribeAclsResponse;
pub use describe_client_quotas_request::DescribeClientQuotasRequest;
pub use describe_client_quotas_response::DescribeClientQuotasResponse;
pub use describe_cluster_request::DescribeClusterRequest;
pub use describe_cluster_response::DescribeClusterResponse;
pub use describe_configs_request::DescribeConfigsRequest;
pub use describe_configs_response::DescribeConfigsResponse;
pub use describe_delegation_token_request::DescribeDelegationTokenRequest;
pub use describe_delegation_token_response::{
    DescribeDelegationTokenResponse, DescribeDelegationTokenResponseOptions,
    DescribeDelegationTokenResponseOptionsBuilder,
};
pub use describe_groups_request::DescribeGroupsRequest;
pub use describe_groups_response::DescribeGroupsResponse;
pub use describe_log_dirs_request::DescribeLogDirsRequest;
pub use describe_log_dirs_response::DescribeLogDirsResponse;
pub use describe_producers_request::DescribeProducersRequest;
pub use describe_producers_response::DescribeProducersResponse;
pub use describe_transactions_request::DescribeTransactionsRequest;
pub use describe_transactions_response::DescribeTransactionsResponse;
pub use describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest;
pub use describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse;
pub use elect_leaders_request::ElectLeadersRequest;
pub use elect_leaders_response::{
    ElectLeadersResponse, ElectLeadersResponseOptions, ElectLeadersResponseOptionsBuilder,
};
pub use end_txn_request::EndTxnRequest;
pub use end_txn_response::EndTxnResponse;
pub use expire_delegation_token_request::ExpireDelegationTokenRequest;
pub use expire_delegation_token_response::ExpireDelegationTokenResponse;
pub use fetch_request::FetchRequest;
pub use fetch_response::FetchResponse;
pub use find_coordinator_request::{CoordinatorType, FindCoordinatorRequest};
pub use find_coordinator_response::FindCoordinatorResponse;
pub use incremental_alter_configs_request::IncrementalAlterConfigsRequest;
pub use incremental_alter_configs_response::IncrementalAlterConfigsResponse;
pub use init_producer_id_request::InitProducerIdRequest;
pub use init_producer_id_response::InitProducerIdResponse;
pub use join_group_request::JoinGroupRequest;
pub use leave_group_request::LeaveGroupRequest;
pub use leave_group_response::LeaveGroupResponse;
pub use list_config_resources_request::ListConfigResourcesRequest;
pub use list_config_resources_response::ListConfigResourcesResponse;
pub use list_groups_request::ListGroupsRequest;
pub use list_groups_response::ListGroupsResponse;
pub use list_offsets_request::{
    ListOffsetsRequest, ListOffsetsRequestBuilderOptions, ListOffsetsRequestBuilderOptionsBuilder,
};
pub use list_offsets_response::ListOffsetsResponse;
pub use list_partition_reassignments_request::ListPartitionReassignmentsRequest;
pub use list_partition_reassignments_response::ListPartitionReassignmentsResponse;
pub use list_transactions_request::ListTransactionsRequest;
pub use list_transactions_response::ListTransactionsResponse;
pub use metadata_request::{MetadataRequest, MetadataRequestBuilderOptions, MetadataRequestBuilderOptionsBuilder};
pub use metadata_response::{MetadataResponse, PartitionMetadata, TopicMetadata};
pub use offset_commit_request::OffsetCommitRequest;
pub use offset_commit_response::OffsetCommitResponse;
pub use offset_delete_request::OffsetDeleteRequest;
pub use offset_delete_response::OffsetDeleteResponse;
pub use offset_fetch_request::OffsetFetchRequest;
pub use offset_fetch_response::OffsetFetchResponse;
pub use offsets_for_leader_epoch_request::OffsetsForLeaderEpochRequest;
pub use offsets_for_leader_epoch_response::OffsetsForLeaderEpochResponse;
pub use produce_request::ProduceRequest;
pub use produce_response::{
    PartitionResponse, PartitionResponseOptions, PartitionResponseOptionsBuilder, ProduceResponse, RecordError,
};
pub use renew_delegation_token_request::RenewDelegationTokenRequest;
pub use renew_delegation_token_response::RenewDelegationTokenResponse;
pub use request_and_size::RequestAndSize;
pub use request_header::{RequestHeader, RequestHeaderOptions, RequestHeaderOptionsBuilder};
pub use request_test_utils::{PartitionMetadataSupplier, PartitionSupplier, RequestTestUtils};
pub use request_utils::RequestUtils;
pub use response_header::ResponseHeader;
pub use sasl_authenticate_request::SaslAuthenticateRequest;
pub use sasl_authenticate_response::SaslAuthenticateResponse;
pub use sasl_handshake_request::SaslHandshakeRequest;
pub use sasl_handshake_response::SaslHandshakeResponse;
pub use send_builder::SendBuilder;
pub use transaction_result::TransactionResult;
pub use txn_offset_commit_request::{
    CommittedOffset, TxnOffsetCommitRequest, TxnOffsetCommitRequestBuilderOptions,
    TxnOffsetCommitRequestBuilderOptionsBuilder,
};
pub use txn_offset_commit_response::TxnOffsetCommitResponse;
pub use update_features_request::{FeatureUpdateItem, UpdateFeaturesRequest};
pub use update_features_response::UpdateFeaturesResponse;
pub use write_txn_markers_request::WriteTxnMarkersRequest;
pub use write_txn_markers_response::WriteTxnMarkersResponse;

/// Sentinel value indicating that the partition leader epoch is unknown or not set.
///
/// Corresponds to `RecordBatch.NO_PARTITION_LEADER_EPOCH` in Java.
///
/// Stays a module-level constant rather than folding into
/// `RecordBatch::NO_PARTITION_LEADER_EPOCH` — deliberately written without a
/// rustdoc link, since this is public documentation and the target is not.
/// `common::requests` is a *directory* module, which keeps its own namespace, and
/// `RecordBatch` lives under `common::record::internal` and so is only
/// `pub(crate)`. Folding would therefore drop a public constant from the API
/// (CLAUDE.md §6).
pub const RECORD_BATCH_NO_PARTITION_LEADER_EPOCH: i32 = -1;
