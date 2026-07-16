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

//! Abstract response framework for Kafka protocol responses.
//!
//! Corresponds to `org.apache.kafka.common.requests.AbstractResponse`.
//!
//! Java uses an abstract class with per-type subclasses. In Rust we use an enum
//! (`ConcreteResponse`) with a variant for each supported response type. Currently
//! only ApiVersions and Metadata are supported; other variants will be added as
//! their request/response types are translated.

use std::collections::HashMap;
use std::io;

use crate::common::network::ByteBufferSend;
use crate::common::protocol::Message;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors, Readable};

use super::ApiVersionsResponse;
use super::ConsumerGroupHeartbeatResponse;
use super::CreatePartitionsResponse;
use super::CreateTopicsResponse;
use super::DeleteRecordsResponse;
use super::DeleteTopicsResponse;
use super::DescribeClusterResponse;
use super::DescribeConfigsResponse;
use super::FetchResponse;
use super::FindCoordinatorResponse;
use super::IncrementalAlterConfigsResponse;
use super::ListConfigResourcesResponse;
use super::ListOffsetsResponse;
use super::MetadataResponse;
use super::OffsetCommitResponse;
use super::OffsetFetchResponse;
use super::OffsetsForLeaderEpochResponse;
use super::ProduceResponse;
use super::RequestHeader;
use super::ResponseHeader;
use super::SaslAuthenticateResponse;
use super::SaslHandshakeResponse;
use super::SendBuilder;

/// Default throttle time in milliseconds.
pub const DEFAULT_THROTTLE_TIME: i32 = 0;

/// Enum dispatch for all supported Kafka response types.
///
/// Each variant wraps a concrete response struct. Common methods are dispatched
/// via `match` on the variant.
///
/// Variants will be added as response types are translated.
#[derive(Debug, Clone)]
pub enum ConcreteResponse {
    /// An ApiVersions response.
    ApiVersions(ApiVersionsResponse),
    /// A Metadata response.
    Metadata(MetadataResponse),
    /// A Produce response.
    Produce(ProduceResponse),
    /// A Fetch response (consumer fetch loop).
    Fetch(FetchResponse),
    /// A SASL handshake response.
    SaslHandshake(SaslHandshakeResponse),
    /// A SASL authenticate response.
    SaslAuthenticate(SaslAuthenticateResponse),
    /// A FindCoordinator response.
    FindCoordinator(FindCoordinatorResponse),
    /// A ListOffsets response.
    ListOffsets(ListOffsetsResponse),
    /// An OffsetsForLeaderEpoch response.
    OffsetsForLeaderEpoch(OffsetsForLeaderEpochResponse),
    /// A ConsumerGroupHeartbeat response (KIP-848).
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatResponse),
    /// An OffsetCommit response.
    OffsetCommit(OffsetCommitResponse),
    /// An OffsetFetch response.
    OffsetFetch(OffsetFetchResponse),
    /// A CreateTopics response.
    CreateTopics(CreateTopicsResponse),
    /// A DeleteTopics response.
    DeleteTopics(DeleteTopicsResponse),
    /// A CreatePartitions response.
    CreatePartitions(CreatePartitionsResponse),
    /// A DeleteRecords response.
    DeleteRecords(DeleteRecordsResponse),
    /// A DescribeConfigs response.
    DescribeConfigs(DescribeConfigsResponse),
    /// An IncrementalAlterConfigs response.
    IncrementalAlterConfigs(IncrementalAlterConfigsResponse),
    /// A ListConfigResources response.
    ListConfigResources(ListConfigResourcesResponse),
    /// A DescribeCluster response.
    DescribeCluster(DescribeClusterResponse),
}

impl ConcreteResponse {
    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        match self {
            Self::ApiVersions(r) => r.api_key(),
            Self::Metadata(r) => r.api_key(),
            Self::Produce(r) => r.api_key(),
            Self::Fetch(r) => r.api_key(),
            Self::SaslHandshake(r) => r.api_key(),
            Self::SaslAuthenticate(r) => r.api_key(),
            Self::FindCoordinator(r) => r.api_key(),
            Self::ListOffsets(r) => r.api_key(),
            Self::OffsetsForLeaderEpoch(r) => r.api_key(),
            Self::ConsumerGroupHeartbeat(r) => r.api_key(),
            Self::OffsetCommit(r) => r.api_key(),
            Self::OffsetFetch(r) => r.api_key(),
            Self::CreateTopics(r) => r.api_key(),
            Self::DeleteTopics(r) => r.api_key(),
            Self::CreatePartitions(r) => r.api_key(),
            Self::DeleteRecords(r) => r.api_key(),
            Self::DescribeConfigs(r) => r.api_key(),
            Self::IncrementalAlterConfigs(r) => r.api_key(),
            Self::ListConfigResources(r) => r.api_key(),
            Self::DescribeCluster(r) => r.api_key(),
        }
    }

    /// Builds a size-prefixed [`ByteBufferSend`] for network transmission.
    ///
    /// Corresponds to `AbstractResponse.toSend` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_send(&mut self, header: &ResponseHeader, version: i16) -> io::Result<ByteBufferSend> {
        match self {
            Self::ApiVersions(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::Metadata(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::Produce(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::Fetch(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::SaslHandshake(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::SaslAuthenticate(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::FindCoordinator(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ListOffsets(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::OffsetsForLeaderEpoch(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ConsumerGroupHeartbeat(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::OffsetCommit(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::OffsetFetch(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::CreateTopics(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DeleteTopics(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::CreatePartitions(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DeleteRecords(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeConfigs(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::IncrementalAlterConfigs(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::ListConfigResources(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
            Self::DescribeCluster(r) => SendBuilder::build_response_send(header, r.data_mut(), version),
        }
    }

    /// Serializes header and body without a size prefix.
    ///
    /// Corresponds to `AbstractResponse.serializeWithHeader` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn serialize_with_header(&mut self, header: &ResponseHeader, version: i16) -> io::Result<ByteBufferAccessor> {
        match self {
            Self::ApiVersions(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::Metadata(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::Produce(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::Fetch(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::SaslHandshake(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::SaslAuthenticate(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::FindCoordinator(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ListOffsets(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::OffsetsForLeaderEpoch(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ConsumerGroupHeartbeat(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::OffsetCommit(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::OffsetFetch(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::CreateTopics(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DeleteTopics(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::CreatePartitions(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DeleteRecords(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeConfigs(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::IncrementalAlterConfigs(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::ListConfigResources(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::DescribeCluster(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
        }
    }

    /// Serializes just the response body (no header, no size prefix).
    ///
    /// Corresponds to `AbstractResponse.serialize` in Java (visible for testing).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn serialize(&mut self, version: i16) -> io::Result<ByteBufferAccessor> {
        match self {
            Self::ApiVersions(r) => Self::serialize_body(r.data_mut(), version),
            Self::Metadata(r) => Self::serialize_body(r.data_mut(), version),
            Self::Produce(r) => Self::serialize_body(r.data_mut(), version),
            Self::Fetch(r) => Self::serialize_body(r.data_mut(), version),
            Self::SaslHandshake(r) => Self::serialize_body(r.data_mut(), version),
            Self::SaslAuthenticate(r) => Self::serialize_body(r.data_mut(), version),
            Self::FindCoordinator(r) => Self::serialize_body(r.data_mut(), version),
            Self::ListOffsets(r) => Self::serialize_body(r.data_mut(), version),
            Self::OffsetsForLeaderEpoch(r) => Self::serialize_body(r.data_mut(), version),
            Self::ConsumerGroupHeartbeat(r) => Self::serialize_body(r.data_mut(), version),
            Self::OffsetCommit(r) => Self::serialize_body(r.data_mut(), version),
            Self::OffsetFetch(r) => Self::serialize_body(r.data_mut(), version),
            Self::CreateTopics(r) => Self::serialize_body(r.data_mut(), version),
            Self::DeleteTopics(r) => Self::serialize_body(r.data_mut(), version),
            Self::CreatePartitions(r) => Self::serialize_body(r.data_mut(), version),
            Self::DeleteRecords(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeConfigs(r) => Self::serialize_body(r.data_mut(), version),
            Self::IncrementalAlterConfigs(r) => Self::serialize_body(r.data_mut(), version),
            Self::ListConfigResources(r) => Self::serialize_body(r.data_mut(), version),
            Self::DescribeCluster(r) => Self::serialize_body(r.data_mut(), version),
        }
    }

    /// Serializes a message body at a given version.
    fn serialize_body(msg: &mut impl Message, version: i16) -> io::Result<ByteBufferAccessor> {
        let mut cache = crate::common::protocol::ObjectSerializationCache::new();
        let size = Message::size(msg, &mut cache, version)?;
        let mut buf = ByteBufferAccessor::new(size as usize);
        Message::write(msg, &mut buf, &cache, version)?;
        buf.flip();
        Ok(buf)
    }

    /// Returns the error counts for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        match self {
            Self::ApiVersions(r) => r.error_counts(),
            Self::Metadata(r) => r.error_counts(),
            Self::Produce(r) => r.error_counts(),
            Self::Fetch(r) => r.error_counts(),
            Self::SaslHandshake(r) => r.error_counts(),
            Self::SaslAuthenticate(r) => r.error_counts(),
            Self::FindCoordinator(r) => r.error_counts(),
            Self::ListOffsets(r) => r.error_counts(),
            Self::OffsetsForLeaderEpoch(r) => r.error_counts(),
            Self::ConsumerGroupHeartbeat(r) => r.error_counts(),
            Self::OffsetCommit(r) => r.error_counts(),
            Self::OffsetFetch(r) => r.error_counts(),
            Self::CreateTopics(r) => r.error_counts(),
            Self::DeleteTopics(r) => r.error_counts(),
            Self::CreatePartitions(r) => r.error_counts(),
            Self::DeleteRecords(r) => r.error_counts(),
            Self::DescribeConfigs(r) => r.error_counts(),
            Self::IncrementalAlterConfigs(r) => r.error_counts(),
            Self::ListConfigResources(r) => r.error_counts(),
            Self::DescribeCluster(r) => r.error_counts(),
        }
    }

    /// Returns the throttle time in milliseconds.
    ///
    /// Returns 0 if the response schema does not support this field.
    pub fn throttle_time_ms(&self) -> i32 {
        match self {
            Self::ApiVersions(r) => r.throttle_time_ms(),
            Self::Metadata(r) => r.throttle_time_ms(),
            Self::Produce(r) => r.throttle_time_ms(),
            Self::Fetch(r) => r.throttle_time_ms(),
            Self::SaslHandshake(r) => r.throttle_time_ms(),
            Self::SaslAuthenticate(r) => r.throttle_time_ms(),
            Self::FindCoordinator(r) => r.throttle_time_ms(),
            Self::ListOffsets(r) => r.throttle_time_ms(),
            Self::OffsetsForLeaderEpoch(r) => r.throttle_time_ms(),
            Self::ConsumerGroupHeartbeat(r) => r.throttle_time_ms(),
            Self::OffsetCommit(r) => r.throttle_time_ms(),
            Self::OffsetFetch(r) => r.throttle_time_ms(),
            Self::CreateTopics(r) => r.throttle_time_ms(),
            Self::DeleteTopics(r) => r.throttle_time_ms(),
            Self::CreatePartitions(r) => r.throttle_time_ms(),
            Self::DeleteRecords(r) => r.throttle_time_ms(),
            Self::DescribeConfigs(r) => r.throttle_time_ms(),
            Self::IncrementalAlterConfigs(r) => r.throttle_time_ms(),
            Self::ListConfigResources(r) => r.throttle_time_ms(),
            Self::DescribeCluster(r) => r.throttle_time_ms(),
        }
    }

    /// Sets the throttle time in the response if the schema supports it.
    /// Otherwise, this is a no-op.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        match self {
            Self::ApiVersions(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::Metadata(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::Produce(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::Fetch(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::SaslHandshake(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::SaslAuthenticate(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::FindCoordinator(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ListOffsets(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::OffsetsForLeaderEpoch(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ConsumerGroupHeartbeat(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::OffsetCommit(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::OffsetFetch(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::CreateTopics(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DeleteTopics(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::CreatePartitions(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DeleteRecords(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeConfigs(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::IncrementalAlterConfigs(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::ListConfigResources(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
            Self::DescribeCluster(r) => r.maybe_set_throttle_time_ms(throttle_time_ms),
        }
    }

    /// Returns whether the client should throttle upon receiving this response.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        match self {
            Self::ApiVersions(r) => r.should_client_throttle(version),
            Self::Metadata(r) => r.should_client_throttle(version),
            Self::Produce(r) => r.should_client_throttle(version),
            Self::Fetch(r) => r.should_client_throttle(version),
            Self::SaslHandshake(r) => r.should_client_throttle(version),
            Self::SaslAuthenticate(r) => r.should_client_throttle(version),
            Self::FindCoordinator(r) => r.should_client_throttle(version),
            Self::ListOffsets(r) => r.should_client_throttle(version),
            Self::OffsetsForLeaderEpoch(r) => r.should_client_throttle(version),
            Self::ConsumerGroupHeartbeat(r) => r.should_client_throttle(version),
            Self::OffsetCommit(r) => r.should_client_throttle(version),
            Self::OffsetFetch(r) => r.should_client_throttle(version),
            Self::CreateTopics(r) => r.should_client_throttle(version),
            Self::DeleteTopics(r) => r.should_client_throttle(version),
            Self::CreatePartitions(r) => r.should_client_throttle(version),
            Self::DeleteRecords(r) => r.should_client_throttle(version),
            Self::DescribeConfigs(r) => r.should_client_throttle(version),
            Self::IncrementalAlterConfigs(r) => r.should_client_throttle(version),
            Self::ListConfigResources(r) => r.should_client_throttle(version),
            Self::DescribeCluster(r) => r.should_client_throttle(version),
        }
    }

    /// Parses a response from a buffer that contains both the response header and the
    /// response body.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The response header cannot be parsed
    /// - The correlation id in the response does not match the request
    /// - The response body cannot be parsed
    pub fn parse_response(buffer: &mut dyn Readable, request_header: &RequestHeader) -> io::Result<Self> {
        let api_key = request_header.api_key();
        let api_version = request_header.api_version();

        let response_header = ResponseHeader::parse(buffer, api_key.response_header_version(api_version))?;

        if request_header.correlation_id() != response_header.correlation_id() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Correlation id for response ({}) does not match request ({}), request header: {}",
                    response_header.correlation_id(),
                    request_header.correlation_id(),
                    request_header
                ),
            ));
        }

        Self::parse(api_key, buffer, api_version)
    }

    /// Parses a response body from the buffer for the given API key and version.
    ///
    /// For ApiVersions, the readable must be a `ByteBufferAccessor` to support
    /// the fallback-to-v0 parsing logic.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key is not supported or parsing fails.
    pub fn parse(api_key: &ApiKeys, readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        match *api_key {
            ApiKeys::API_VERSIONS => {
                // ApiVersionsResponse.parse requires a ByteBufferAccessor for snapshot_remaining
                // Since we receive &mut dyn Readable, we read remaining bytes and construct one.
                let remaining = readable.remaining();
                let bytes = readable.read_array(remaining)?;
                let mut buf = ByteBufferAccessor::from_bytes(bytes);
                let response = ApiVersionsResponse::parse(&mut buf, version)?;
                Ok(Self::ApiVersions(response))
            },
            ApiKeys::METADATA => {
                let response = MetadataResponse::parse(readable, version)?;
                Ok(Self::Metadata(response))
            },
            ApiKeys::PRODUCE => {
                let response = ProduceResponse::parse(readable, version)?;
                Ok(Self::Produce(response))
            },
            ApiKeys::FETCH => {
                let data = crate::fetch_response_data::FetchResponseData::read(readable, version)?;
                Ok(Self::Fetch(FetchResponse::new(data)))
            },
            ApiKeys::SASL_HANDSHAKE => {
                let response = SaslHandshakeResponse::parse(readable, version)?;
                Ok(Self::SaslHandshake(response))
            },
            ApiKeys::SASL_AUTHENTICATE => {
                let response = SaslAuthenticateResponse::parse(readable, version)?;
                Ok(Self::SaslAuthenticate(response))
            },
            ApiKeys::FIND_COORDINATOR => {
                let response = FindCoordinatorResponse::parse(readable, version)?;
                Ok(Self::FindCoordinator(response))
            },
            ApiKeys::LIST_OFFSETS => {
                let response = ListOffsetsResponse::parse(readable, version)?;
                Ok(Self::ListOffsets(response))
            },
            ApiKeys::OFFSET_FOR_LEADER_EPOCH => {
                let response = OffsetsForLeaderEpochResponse::parse(readable, version)?;
                Ok(Self::OffsetsForLeaderEpoch(response))
            },
            ApiKeys::CONSUMER_GROUP_HEARTBEAT => {
                let response = ConsumerGroupHeartbeatResponse::parse(readable, version)?;
                Ok(Self::ConsumerGroupHeartbeat(response))
            },
            ApiKeys::OFFSET_COMMIT => {
                let response = OffsetCommitResponse::parse(readable, version)?;
                Ok(Self::OffsetCommit(response))
            },
            ApiKeys::OFFSET_FETCH => {
                let response = OffsetFetchResponse::parse(readable, version)?;
                Ok(Self::OffsetFetch(response))
            },
            ApiKeys::CREATE_TOPICS => {
                let response = CreateTopicsResponse::parse(readable, version)?;
                Ok(Self::CreateTopics(response))
            },
            ApiKeys::DELETE_TOPICS => {
                let response = DeleteTopicsResponse::parse(readable, version)?;
                Ok(Self::DeleteTopics(response))
            },
            ApiKeys::CREATE_PARTITIONS => {
                let response = CreatePartitionsResponse::parse(readable, version)?;
                Ok(Self::CreatePartitions(response))
            },
            ApiKeys::DELETE_RECORDS => {
                let response = DeleteRecordsResponse::parse(readable, version)?;
                Ok(Self::DeleteRecords(response))
            },
            ApiKeys::DESCRIBE_CONFIGS => {
                let response = DescribeConfigsResponse::parse(readable, version)?;
                Ok(Self::DescribeConfigs(response))
            },
            ApiKeys::INCREMENTAL_ALTER_CONFIGS => {
                let response = IncrementalAlterConfigsResponse::parse(readable, version)?;
                Ok(Self::IncrementalAlterConfigs(response))
            },
            ApiKeys::LIST_CONFIG_RESOURCES => {
                let response = ListConfigResourcesResponse::parse(readable, version)?;
                Ok(Self::ListConfigResources(response))
            },
            ApiKeys::DESCRIBE_CLUSTER => {
                let response = DescribeClusterResponse::parse(readable, version)?;
                Ok(Self::DescribeCluster(response))
            },
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("ApiKey {} is not currently handled in parse_response", api_key.name()),
            )),
        }
    }
}

impl std::fmt::Display for ConcreteResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiVersions(r) => write!(f, "{r}"),
            Self::Metadata(r) => write!(f, "{r}"),
            Self::Produce(r) => write!(f, "{r}"),
            Self::Fetch(r) => write!(f, "{r}"),
            Self::SaslHandshake(r) => write!(f, "{r}"),
            Self::SaslAuthenticate(r) => write!(f, "{r}"),
            Self::FindCoordinator(r) => write!(f, "{r}"),
            Self::ListOffsets(r) => write!(f, "{r}"),
            Self::OffsetsForLeaderEpoch(r) => write!(f, "{r}"),
            Self::ConsumerGroupHeartbeat(r) => write!(f, "{r}"),
            Self::OffsetCommit(r) => write!(f, "{r}"),
            Self::OffsetFetch(r) => write!(f, "{r}"),
            Self::CreateTopics(r) => write!(f, "{r}"),
            Self::DeleteTopics(r) => write!(f, "{r}"),
            Self::CreatePartitions(r) => write!(f, "{r}"),
            Self::DeleteRecords(r) => write!(f, "{r}"),
            Self::DescribeConfigs(r) => write!(f, "{r}"),
            Self::IncrementalAlterConfigs(r) => write!(f, "{r}"),
            Self::ListConfigResources(r) => write!(f, "{r}"),
            Self::DescribeCluster(r) => write!(f, "{r}"),
        }
    }
}

/// Helper: counts a single error, returning a map with one entry.
pub fn single_error_count(error: Errors) -> HashMap<Errors, i32> {
    let mut map = HashMap::new();
    map.insert(error, 1);
    map
}

/// Helper: increments the count for the given error in the map.
pub fn update_error_counts(error_counts: &mut HashMap<Errors, i32>, error: Errors) {
    *error_counts.entry(error).or_insert(0) += 1;
}
