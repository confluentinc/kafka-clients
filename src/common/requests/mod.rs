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

pub mod abstract_request;
pub mod abstract_response;
pub mod alter_client_quotas_request;
pub mod alter_client_quotas_response;
pub mod alter_partition_reassignments_request;
pub mod alter_partition_reassignments_response;
pub mod alter_replica_log_dirs_request;
pub mod alter_replica_log_dirs_response;
pub mod alter_user_scram_credentials_request;
pub mod alter_user_scram_credentials_response;
pub mod api_versions_request;
pub mod api_versions_response;
pub mod consumer_group_describe_request;
pub mod consumer_group_describe_response;
pub mod consumer_group_heartbeat_request;
pub mod consumer_group_heartbeat_response;
pub mod create_acls_request;
pub mod create_acls_response;
pub mod create_delegation_token_request;
pub mod create_delegation_token_response;
pub mod create_partitions_request;
pub mod create_partitions_response;
pub mod create_topics_request;
pub mod create_topics_response;
pub mod delete_acls_request;
pub mod delete_acls_response;
pub mod delete_groups_request;
pub mod delete_groups_response;
pub mod delete_records_request;
pub mod delete_records_response;
pub mod delete_topics_request;
pub mod delete_topics_response;
pub mod describe_acls_request;
pub mod describe_acls_response;
pub mod describe_client_quotas_request;
pub mod describe_client_quotas_response;
pub mod describe_cluster_request;
pub mod describe_cluster_response;
pub mod describe_configs_request;
pub mod describe_configs_response;
pub mod describe_delegation_token_request;
pub mod describe_delegation_token_response;
pub mod describe_groups_request;
pub mod describe_groups_response;
pub mod describe_log_dirs_request;
pub mod describe_log_dirs_response;
pub mod describe_producers_request;
pub mod describe_producers_response;
pub mod describe_transactions_request;
pub mod describe_transactions_response;
pub mod describe_user_scram_credentials_request;
pub mod describe_user_scram_credentials_response;
pub mod elect_leaders_request;
pub mod elect_leaders_response;
pub mod expire_delegation_token_request;
pub mod expire_delegation_token_response;
pub mod fetch_metadata;
pub mod fetch_request;
pub mod fetch_response;
pub mod find_coordinator_request;
pub mod find_coordinator_response;
pub mod incremental_alter_configs_request;
pub mod incremental_alter_configs_response;
pub mod init_producer_id_request;
pub mod init_producer_id_response;
pub mod join_group_request;
pub mod leave_group_request;
pub mod leave_group_response;
pub mod list_config_resources_request;
pub mod list_config_resources_response;
pub mod list_groups_request;
pub mod list_groups_response;
pub mod list_offsets_request;
pub mod list_offsets_response;
pub mod list_partition_reassignments_request;
pub mod list_partition_reassignments_response;
pub mod list_transactions_request;
pub mod list_transactions_response;
pub mod metadata_request;
pub mod metadata_response;
pub mod offset_commit_request;
pub mod offset_commit_response;
pub mod offset_delete_request;
pub mod offset_delete_response;
pub mod offset_fetch_request;
pub mod offset_fetch_response;
pub mod offsets_for_leader_epoch_request;
pub mod offsets_for_leader_epoch_response;
pub mod produce_request;
pub mod produce_response;
pub mod renew_delegation_token_request;
pub mod renew_delegation_token_response;
pub mod request_and_size;
pub mod request_header;
pub(crate) mod request_test_utils;
pub(crate) mod request_utils;
pub mod response_header;
pub mod sasl_authenticate_request;
pub mod sasl_authenticate_response;
pub mod sasl_handshake_request;
pub mod sasl_handshake_response;
pub mod send_builder;
pub mod update_features_request;
pub mod update_features_response;
pub mod write_txn_markers_request;
pub mod write_txn_markers_response;

pub use abstract_request::{ConcreteRequest, RequestBuilder};
pub use abstract_response::ConcreteResponse;
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
pub use consumer_group_heartbeat_request::{
    CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatRequestBuilder,
    JOIN_GROUP_MEMBER_EPOCH, LEAVE_GROUP_MEMBER_EPOCH, LEAVE_GROUP_STATIC_MEMBER_EPOCH,
    REGEX_RESOLUTION_NOT_SUPPORTED_MSG,
};
pub use consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
pub use create_acls_request::{CreateAclsRequest, CreateAclsRequestBuilder};
pub use create_acls_response::CreateAclsResponse;
pub use create_delegation_token_request::{CreateDelegationTokenRequest, CreateDelegationTokenRequestBuilder};
pub use create_delegation_token_response::CreateDelegationTokenResponse;
pub use create_partitions_request::{CreatePartitionsRequest, CreatePartitionsRequestBuilder};
pub use create_partitions_response::CreatePartitionsResponse;
pub use create_topics_request::{
    CreateTopicsRequest, CreateTopicsRequestBuilder, NO_NUM_PARTITIONS, NO_REPLICATION_FACTOR,
};
pub use create_topics_response::CreateTopicsResponse;
pub use delete_acls_request::{DeleteAclsRequest, DeleteAclsRequestBuilder};
pub use delete_acls_response::DeleteAclsResponse;
pub use delete_groups_request::{DeleteGroupsRequest, DeleteGroupsRequestBuilder};
pub use delete_groups_response::DeleteGroupsResponse;
pub use delete_records_request::{DeleteRecordsRequest, DeleteRecordsRequestBuilder};
pub use delete_records_response::{DeleteRecordsResponse, INVALID_LOW_WATERMARK};
pub use delete_topics_request::{DeleteTopicsRequest, DeleteTopicsRequestBuilder};
pub use delete_topics_response::DeleteTopicsResponse;
pub use describe_acls_request::{DescribeAclsRequest, DescribeAclsRequestBuilder};
pub use describe_acls_response::DescribeAclsResponse;
pub use describe_client_quotas_request::{
    DescribeClientQuotasRequest, DescribeClientQuotasRequestBuilder, MATCH_TYPE_DEFAULT, MATCH_TYPE_EXACT,
    MATCH_TYPE_SPECIFIED,
};
pub use describe_client_quotas_response::DescribeClientQuotasResponse;
pub use describe_cluster_request::{
    DescribeClusterRequest, DescribeClusterRequestBuilder, ENDPOINT_TYPE_BROKER, ENDPOINT_TYPE_CONTROLLER,
};
pub use describe_cluster_response::DescribeClusterResponse;
pub use describe_configs_request::{DescribeConfigsRequest, DescribeConfigsRequestBuilder};
pub use describe_configs_response::DescribeConfigsResponse;
pub use describe_delegation_token_request::{DescribeDelegationTokenRequest, DescribeDelegationTokenRequestBuilder};
pub use describe_delegation_token_response::DescribeDelegationTokenResponse;
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
pub use elect_leaders_response::ElectLeadersResponse;
pub use expire_delegation_token_request::{ExpireDelegationTokenRequest, ExpireDelegationTokenRequestBuilder};
pub use expire_delegation_token_response::ExpireDelegationTokenResponse;
pub use fetch_request::{FetchRequest, FetchRequestBuilder};
pub use fetch_response::FetchResponse;
pub use find_coordinator_request::{
    CoordinatorType, FindCoordinatorRequest, FindCoordinatorRequestBuilder, MIN_BATCHED_VERSION,
};
pub use find_coordinator_response::FindCoordinatorResponse;
pub use incremental_alter_configs_request::{IncrementalAlterConfigsRequest, IncrementalAlterConfigsRequestBuilder};
pub use incremental_alter_configs_response::IncrementalAlterConfigsResponse;
pub use init_producer_id_request::{InitProducerIdRequest, InitProducerIdRequestBuilder};
pub use init_producer_id_response::InitProducerIdResponse;
pub use join_group_request::{MAX_REASON_LENGTH, UNKNOWN_MEMBER_ID, maybe_truncate_reason};
pub use leave_group_request::{LeaveGroupRequest, LeaveGroupRequestBuilder};
pub use leave_group_response::LeaveGroupResponse;
pub use list_config_resources_request::{ListConfigResourcesRequest, ListConfigResourcesRequestBuilder};
pub use list_config_resources_response::ListConfigResourcesResponse;
pub use list_groups_request::{ListGroupsRequest, ListGroupsRequestBuilder};
pub use list_groups_response::ListGroupsResponse;
pub use list_offsets_request::{ListOffsetsRequest, ListOffsetsRequestBuilder};
pub use list_offsets_response::ListOffsetsResponse;
pub use list_partition_reassignments_request::{
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsRequestBuilder,
};
pub use list_partition_reassignments_response::ListPartitionReassignmentsResponse;
pub use list_transactions_request::{ListTransactionsRequest, ListTransactionsRequestBuilder};
pub use list_transactions_response::ListTransactionsResponse;
pub use metadata_request::{MetadataRequest, MetadataRequestBuilder};
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
pub use produce_response::{PartitionResponse, ProduceResponse, RecordError};
pub use renew_delegation_token_request::{RenewDelegationTokenRequest, RenewDelegationTokenRequestBuilder};
pub use renew_delegation_token_response::RenewDelegationTokenResponse;
pub use request_and_size::RequestAndSize;
pub use request_header::RequestHeader;
pub use response_header::ResponseHeader;
pub use sasl_authenticate_request::{SaslAuthenticateRequest, SaslAuthenticateRequestBuilder};
pub use sasl_authenticate_response::SaslAuthenticateResponse;
pub use sasl_handshake_request::{SaslHandshakeRequest, SaslHandshakeRequestBuilder};
pub use sasl_handshake_response::SaslHandshakeResponse;
pub use send_builder::SendBuilder;
pub use update_features_request::{FeatureUpdateItem, UpdateFeaturesRequest, UpdateFeaturesRequestBuilder};
pub use update_features_response::UpdateFeaturesResponse;
pub use write_txn_markers_request::{WriteTxnMarkersRequest, WriteTxnMarkersRequestBuilder};
pub use write_txn_markers_response::WriteTxnMarkersResponse;

/// Sentinel value indicating that the partition leader epoch is unknown or not set.
///
/// Corresponds to `RecordBatch.RECORD_BATCH_NO_PARTITION_LEADER_EPOCH` in Java.
pub const RECORD_BATCH_NO_PARTITION_LEADER_EPOCH: i32 = -1;
