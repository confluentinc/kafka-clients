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

mod abstract_request;
mod abstract_response;
mod add_offsets_to_txn_request;
mod add_offsets_to_txn_response;
mod add_partitions_to_txn_request;
mod add_partitions_to_txn_response;
mod alter_client_quotas_request;
mod alter_client_quotas_response;
mod alter_partition_reassignments_request;
mod alter_partition_reassignments_response;
mod alter_replica_log_dirs_request;
mod alter_replica_log_dirs_response;
mod alter_user_scram_credentials_request;
mod alter_user_scram_credentials_response;
mod api_versions_request;
mod api_versions_response;
mod consumer_group_describe_request;
mod consumer_group_describe_response;
mod consumer_group_heartbeat_request;
mod consumer_group_heartbeat_response;
mod correlation_id_mismatch_error;
mod create_acls_request;
mod create_acls_response;
mod create_delegation_token_request;
mod create_delegation_token_response;
mod create_partitions_request;
mod create_partitions_response;
mod create_topics_request;
mod create_topics_response;
mod delete_acls_request;
mod delete_acls_response;
mod delete_groups_request;
mod delete_groups_response;
mod delete_records_request;
mod delete_records_response;
mod delete_topics_request;
mod delete_topics_response;
mod describe_acls_request;
mod describe_acls_response;
mod describe_client_quotas_request;
mod describe_client_quotas_response;
mod describe_cluster_request;
mod describe_cluster_response;
mod describe_configs_request;
mod describe_configs_response;
mod describe_delegation_token_request;
mod describe_delegation_token_response;
mod describe_groups_request;
mod describe_groups_response;
mod describe_log_dirs_request;
mod describe_log_dirs_response;
mod describe_producers_request;
mod describe_producers_response;
mod describe_transactions_request;
mod describe_transactions_response;
mod describe_user_scram_credentials_request;
mod describe_user_scram_credentials_response;
mod elect_leaders_request;
mod elect_leaders_response;
mod end_txn_request;
mod end_txn_response;
mod expire_delegation_token_request;
mod expire_delegation_token_response;
mod fetch_metadata;
pub use fetch_metadata::FetchMetadata;
pub mod fetch_request;
mod fetch_response;
pub mod find_coordinator_request;
mod find_coordinator_response;
mod incremental_alter_configs_request;
mod incremental_alter_configs_response;
mod init_producer_id_request;
mod init_producer_id_response;
mod join_group_request;
mod leave_group_request;
mod leave_group_response;
mod list_config_resources_request;
mod list_config_resources_response;
mod list_groups_request;
mod list_groups_response;
mod list_offsets_request;
mod list_offsets_response;
mod list_partition_reassignments_request;
mod list_partition_reassignments_response;
mod list_transactions_request;
mod list_transactions_response;
mod metadata_request;
pub mod metadata_response;
mod offset_commit_request;
mod offset_commit_response;
mod offset_delete_request;
mod offset_delete_response;
mod offset_fetch_request;
mod offset_fetch_response;
mod offsets_for_leader_epoch_request;
mod offsets_for_leader_epoch_response;
mod produce_request;
pub mod produce_response;
mod renew_delegation_token_request;
mod renew_delegation_token_response;
mod request_and_size;
mod request_header;
mod request_test_utils;
mod request_utils;
mod response_header;
mod sasl_authenticate_request;
mod sasl_authenticate_response;
mod sasl_handshake_request;
mod sasl_handshake_response;
mod send_builder;
mod transaction_result;
pub mod txn_offset_commit_request;
mod txn_offset_commit_response;

pub use abstract_request::{ConcreteRequest, RequestBuilder};
pub use abstract_response::{AbstractResponse, ConcreteResponse};
pub use add_offsets_to_txn_request::{AddOffsetsToTxnRequest, AddOffsetsToTxnRequestBuilder};
pub use add_offsets_to_txn_response::AddOffsetsToTxnResponse;
pub use add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnRequestBuilder};
pub use add_partitions_to_txn_response::AddPartitionsToTxnResponse;
pub mod update_features_request;
mod update_features_response;
mod write_txn_markers_request;
mod write_txn_markers_response;

pub use alter_client_quotas_request::{AlterClientQuotasRequest, AlterClientQuotasRequestBuilder};
pub use alter_client_quotas_response::AlterClientQuotasResponse;
pub use alter_partition_reassignments_request::{
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsRequestBuilder,
};
pub use alter_partition_reassignments_response::AlterPartitionReassignmentsResponse;
pub use alter_replica_log_dirs_request::{AlterReplicaLogDirsRequest, AlterReplicaLogDirsRequestBuilder};
pub use alter_replica_log_dirs_response::AlterReplicaLogDirsResponse;
pub use alter_user_scram_credentials_request::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsRequestBuilder,
};
pub use alter_user_scram_credentials_response::AlterUserScramCredentialsResponse;
pub use api_versions_request::{ApiVersionsRequest, ApiVersionsRequestBuilder};
pub use api_versions_response::{ApiVersionsResponse, ApiVersionsResponseBuilder};
pub use consumer_group_describe_request::{ConsumerGroupDescribeRequest, ConsumerGroupDescribeRequestBuilder};
pub use consumer_group_describe_response::ConsumerGroupDescribeResponse;
pub use consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatRequestBuilder};
pub use consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
pub use correlation_id_mismatch_error::CorrelationIdMismatchError;
pub use create_acls_request::{CreateAclsRequest, CreateAclsRequestBuilder};
pub use create_acls_response::CreateAclsResponse;
pub use create_delegation_token_request::{CreateDelegationTokenRequest, CreateDelegationTokenRequestBuilder};
pub use create_delegation_token_response::{
    CreateDelegationTokenResponse, CreateDelegationTokenResponseOptions, CreateDelegationTokenResponseOptionsBuilder,
};
pub use create_partitions_request::{CreatePartitionsRequest, CreatePartitionsRequestBuilder};
pub use create_partitions_response::CreatePartitionsResponse;
pub use create_topics_request::{CreateTopicsRequest, CreateTopicsRequestBuilder};
pub use create_topics_response::CreateTopicsResponse;
pub use delete_acls_request::{DeleteAclsRequest, DeleteAclsRequestBuilder};
pub use delete_acls_response::DeleteAclsResponse;
pub use delete_groups_request::{DeleteGroupsRequest, DeleteGroupsRequestBuilder};
pub use delete_groups_response::DeleteGroupsResponse;
pub use delete_records_request::{DeleteRecordsRequest, DeleteRecordsRequestBuilder};
pub use delete_records_response::DeleteRecordsResponse;
pub use delete_topics_request::{DeleteTopicsRequest, DeleteTopicsRequestBuilder};
pub use delete_topics_response::DeleteTopicsResponse;
pub use describe_acls_request::{DescribeAclsRequest, DescribeAclsRequestBuilder};
pub use describe_acls_response::DescribeAclsResponse;
pub use describe_client_quotas_request::{DescribeClientQuotasRequest, DescribeClientQuotasRequestBuilder};
pub use describe_client_quotas_response::DescribeClientQuotasResponse;
pub use describe_cluster_request::{DescribeClusterRequest, DescribeClusterRequestBuilder};
pub use describe_cluster_response::DescribeClusterResponse;
pub use describe_configs_request::{DescribeConfigsRequest, DescribeConfigsRequestBuilder};
pub use describe_configs_response::DescribeConfigsResponse;
pub use describe_delegation_token_request::{DescribeDelegationTokenRequest, DescribeDelegationTokenRequestBuilder};
pub use describe_delegation_token_response::{
    DescribeDelegationTokenResponse, DescribeDelegationTokenResponseOptions,
    DescribeDelegationTokenResponseOptionsBuilder,
};
pub use describe_groups_request::{DescribeGroupsRequest, DescribeGroupsRequestBuilder};
pub use describe_groups_response::DescribeGroupsResponse;
pub use describe_log_dirs_request::{DescribeLogDirsRequest, DescribeLogDirsRequestBuilder};
pub use describe_log_dirs_response::DescribeLogDirsResponse;
pub use describe_producers_request::{DescribeProducersRequest, DescribeProducersRequestBuilder};
pub use describe_producers_response::DescribeProducersResponse;
pub use describe_transactions_request::{DescribeTransactionsRequest, DescribeTransactionsRequestBuilder};
pub use describe_transactions_response::DescribeTransactionsResponse;
pub use describe_user_scram_credentials_request::{
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsRequestBuilder,
};
pub use describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse;
pub use elect_leaders_request::{ElectLeadersRequest, ElectLeadersRequestBuilder};
pub use elect_leaders_response::{
    ElectLeadersResponse, ElectLeadersResponseOptions, ElectLeadersResponseOptionsBuilder,
};
pub use end_txn_request::{EndTxnRequest, EndTxnRequestBuilder};
pub use end_txn_response::EndTxnResponse;
pub use expire_delegation_token_request::{ExpireDelegationTokenRequest, ExpireDelegationTokenRequestBuilder};
pub use expire_delegation_token_response::ExpireDelegationTokenResponse;
pub use fetch_request::{FetchRequest, FetchRequestBuilder};
pub use fetch_response::FetchResponse;
pub use find_coordinator_request::{CoordinatorType, FindCoordinatorRequest, FindCoordinatorRequestBuilder};
pub use find_coordinator_response::FindCoordinatorResponse;
pub use incremental_alter_configs_request::{IncrementalAlterConfigsRequest, IncrementalAlterConfigsRequestBuilder};
pub use incremental_alter_configs_response::IncrementalAlterConfigsResponse;
pub use init_producer_id_request::{InitProducerIdRequest, InitProducerIdRequestBuilder};
pub use init_producer_id_response::InitProducerIdResponse;
pub use join_group_request::JoinGroupRequest;
pub use leave_group_request::{LeaveGroupRequest, LeaveGroupRequestBuilder};
pub use leave_group_response::LeaveGroupResponse;
pub use list_config_resources_request::{ListConfigResourcesRequest, ListConfigResourcesRequestBuilder};
pub use list_config_resources_response::ListConfigResourcesResponse;
pub use list_groups_request::{ListGroupsRequest, ListGroupsRequestBuilder};
pub use list_groups_response::ListGroupsResponse;
pub use list_offsets_request::{
    ListOffsetsRequest, ListOffsetsRequestBuilder, ListOffsetsRequestBuilderOptions,
    ListOffsetsRequestBuilderOptionsBuilder,
};
pub use list_offsets_response::ListOffsetsResponse;
pub use list_partition_reassignments_request::{
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsRequestBuilder,
};
pub use list_partition_reassignments_response::ListPartitionReassignmentsResponse;
pub use list_transactions_request::{ListTransactionsRequest, ListTransactionsRequestBuilder};
pub use list_transactions_response::ListTransactionsResponse;
pub use metadata_request::{
    MetadataRequest, MetadataRequestBuilder, MetadataRequestBuilderOptions, MetadataRequestBuilderOptionsBuilder,
};
pub use metadata_response::{MetadataResponse, PartitionMetadata, TopicMetadata};
pub use offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestBuilder};
pub use offset_commit_response::OffsetCommitResponse;
pub use offset_delete_request::{OffsetDeleteRequest, OffsetDeleteRequestBuilder};
pub use offset_delete_response::OffsetDeleteResponse;
pub use offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestBuilder};
pub use offset_fetch_response::{OffsetFetchResponse, OffsetFetchResponseBuilder};
pub use offsets_for_leader_epoch_request::{OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochRequestBuilder};
pub use offsets_for_leader_epoch_response::OffsetsForLeaderEpochResponse;
pub use produce_request::{ProduceRequest, ProduceRequestBuilder};
pub use produce_response::{
    PartitionResponse, PartitionResponseOptions, PartitionResponseOptionsBuilder, ProduceResponse, RecordError,
};
pub use renew_delegation_token_request::{RenewDelegationTokenRequest, RenewDelegationTokenRequestBuilder};
pub use renew_delegation_token_response::RenewDelegationTokenResponse;
pub use request_and_size::RequestAndSize;
pub use request_header::{RequestHeader, RequestHeaderOptions, RequestHeaderOptionsBuilder};
pub use request_test_utils::{PartitionMetadataSupplier, PartitionSupplier, RequestTestUtils};
pub use request_utils::RequestUtils;
pub use response_header::ResponseHeader;
pub use sasl_authenticate_request::{SaslAuthenticateRequest, SaslAuthenticateRequestBuilder};
pub use sasl_authenticate_response::SaslAuthenticateResponse;
pub use sasl_handshake_request::{SaslHandshakeRequest, SaslHandshakeRequestBuilder};
pub use sasl_handshake_response::SaslHandshakeResponse;
pub use send_builder::SendBuilder;
pub use transaction_result::TransactionResult;
pub use txn_offset_commit_request::{
    CommittedOffset, TxnOffsetCommitRequest, TxnOffsetCommitRequestBuilder, TxnOffsetCommitRequestBuilderOptions,
    TxnOffsetCommitRequestBuilderOptionsBuilder,
};
pub use txn_offset_commit_response::TxnOffsetCommitResponse;
pub use update_features_request::{FeatureUpdateItem, UpdateFeaturesRequest, UpdateFeaturesRequestBuilder};
pub use update_features_response::UpdateFeaturesResponse;
pub use write_txn_markers_request::{WriteTxnMarkersRequest, WriteTxnMarkersRequestBuilder};
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
/// (CLAUDE.md §4).
pub const RECORD_BATCH_NO_PARTITION_LEADER_EPOCH: i32 = -1;
