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
pub mod add_partitions_to_txn_request;
pub mod add_partitions_to_txn_response;
pub mod api_versions_request;
pub mod api_versions_response;
pub mod consumer_group_heartbeat_request;
pub mod consumer_group_heartbeat_response;
pub mod fetch_metadata;
pub mod fetch_request;
pub mod fetch_response;
pub mod find_coordinator_request;
pub mod find_coordinator_response;
pub mod init_producer_id_request;
pub mod init_producer_id_response;
pub mod list_offsets_request;
pub mod list_offsets_response;
pub mod metadata_request;
pub mod metadata_response;
pub mod offset_commit_request;
pub mod offset_commit_response;
pub mod offset_fetch_request;
pub mod offset_fetch_response;
pub mod offsets_for_leader_epoch_request;
pub mod offsets_for_leader_epoch_response;
pub mod produce_request;
pub mod produce_response;
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
pub mod transaction_result;

pub use abstract_request::{ConcreteRequest, RequestBuilder};
pub use abstract_response::ConcreteResponse;
pub use add_partitions_to_txn_request::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnRequestBuilder, EARLIEST_BROKER_VERSION, LAST_CLIENT_VERSION,
};
pub use add_partitions_to_txn_response::{AddPartitionsToTxnResponse, V3_AND_BELOW_TXN_ID};
pub use api_versions_request::{ApiVersionsRequest, ApiVersionsRequestBuilder};
pub use api_versions_response::{ApiVersionsResponse, ApiVersionsResponseBuilder};
pub use consumer_group_heartbeat_request::{
    CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatRequestBuilder,
    JOIN_GROUP_MEMBER_EPOCH, LEAVE_GROUP_MEMBER_EPOCH, LEAVE_GROUP_STATIC_MEMBER_EPOCH,
    REGEX_RESOLUTION_NOT_SUPPORTED_MSG,
};
pub use consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
pub use fetch_request::{FetchRequest, FetchRequestBuilder};
pub use fetch_response::FetchResponse;
pub use find_coordinator_request::{
    CoordinatorType, FindCoordinatorRequest, FindCoordinatorRequestBuilder, MIN_BATCHED_VERSION,
};
pub use find_coordinator_response::FindCoordinatorResponse;
pub use init_producer_id_request::{InitProducerIdRequest, InitProducerIdRequestBuilder};
pub use init_producer_id_response::InitProducerIdResponse;
pub use list_offsets_request::{ListOffsetsRequest, ListOffsetsRequestBuilder};
pub use list_offsets_response::ListOffsetsResponse;
pub use metadata_request::{MetadataRequest, MetadataRequestBuilder};
pub use metadata_response::{MetadataResponse, PartitionMetadata, TopicMetadata};
pub use offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestBuilder};
pub use offset_commit_response::OffsetCommitResponse;
pub use offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestBuilder};
pub use offset_fetch_response::{OffsetFetchResponse, OffsetFetchResponseBuilder};
pub use offsets_for_leader_epoch_request::{OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochRequestBuilder};
pub use offsets_for_leader_epoch_response::OffsetsForLeaderEpochResponse;
pub use produce_request::{ProduceRequest, ProduceRequestBuilder};
pub use produce_response::{PartitionResponse, ProduceResponse, RecordError};
pub use request_and_size::RequestAndSize;
pub use request_header::RequestHeader;
pub use response_header::ResponseHeader;
pub use sasl_authenticate_request::{SaslAuthenticateRequest, SaslAuthenticateRequestBuilder};
pub use sasl_authenticate_response::SaslAuthenticateResponse;
pub use sasl_handshake_request::{SaslHandshakeRequest, SaslHandshakeRequestBuilder};
pub use sasl_handshake_response::SaslHandshakeResponse;
pub use send_builder::SendBuilder;
pub use transaction_result::TransactionResult;

/// Sentinel value indicating that the partition leader epoch is unknown or not set.
///
/// Corresponds to `RecordBatch.RECORD_BATCH_NO_PARTITION_LEADER_EPOCH` in Java.
pub const RECORD_BATCH_NO_PARTITION_LEADER_EPOCH: i32 = -1;
