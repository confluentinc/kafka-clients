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
pub mod api_versions_request;
pub mod api_versions_response;
pub mod fetch_metadata;
pub mod fetch_request;
pub mod find_coordinator_request;
pub mod find_coordinator_response;
pub mod metadata_request;
pub mod metadata_response;
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

pub use abstract_request::{ConcreteRequest, RequestBuilder};
pub use abstract_response::ConcreteResponse;
pub use api_versions_request::{ApiVersionsRequest, ApiVersionsRequestBuilder};
pub use api_versions_response::{ApiVersionsResponse, ApiVersionsResponseBuilder};
pub use find_coordinator_request::{
    CoordinatorType, FindCoordinatorRequest, FindCoordinatorRequestBuilder, MIN_BATCHED_VERSION,
};
pub use find_coordinator_response::FindCoordinatorResponse;
pub use metadata_request::{MetadataRequest, MetadataRequestBuilder};
pub use metadata_response::{MetadataResponse, PartitionMetadata, TopicMetadata};
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

/// Sentinel value indicating that the partition leader epoch is unknown or not set.
///
/// Corresponds to `RecordBatch.RECORD_BATCH_NO_PARTITION_LEADER_EPOCH` in Java.
pub const RECORD_BATCH_NO_PARTITION_LEADER_EPOCH: i32 = -1;
