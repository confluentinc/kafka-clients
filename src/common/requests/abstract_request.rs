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

//! Abstract request framework for Kafka protocol requests.
//!
//! Corresponds to `org.apache.kafka.common.requests.AbstractRequest`.
//!
//! Java uses an abstract class with per-type subclasses and an inner `Builder`
//! abstract class. In Rust we use:
//! - `ConcreteRequest` enum with a variant for each supported request type
//! - `RequestBuilder` trait for constructing requests at a specific version

use std::io;

use crate::alter_replica_log_dirs_request_data::AlterReplicaLogDirsRequestData;
use crate::api_versions_request_data::ApiVersionsRequestData;
use crate::common::network::ByteBufferSend;
use crate::common::protocol::Message;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Readable};
use crate::consumer_group_heartbeat_request_data::ConsumerGroupHeartbeatRequestData;
use crate::create_partitions_request_data::CreatePartitionsRequestData;
use crate::create_topics_request_data::CreateTopicsRequestData;
use crate::delete_records_request_data::DeleteRecordsRequestData;
use crate::delete_topics_request_data::DeleteTopicsRequestData;
use crate::describe_cluster_request_data::DescribeClusterRequestData;
use crate::describe_configs_request_data::DescribeConfigsRequestData;
use crate::describe_log_dirs_request_data::DescribeLogDirsRequestData;
use crate::fetch_request_data::FetchRequestData;
use crate::find_coordinator_request_data::FindCoordinatorRequestData;
use crate::incremental_alter_configs_request_data::IncrementalAlterConfigsRequestData;
use crate::list_config_resources_request_data::ListConfigResourcesRequestData;
use crate::list_offsets_request_data::ListOffsetsRequestData;
use crate::metadata_request_data::MetadataRequestData;
use crate::offset_commit_request_data::OffsetCommitRequestData;
use crate::offset_fetch_request_data::OffsetFetchRequestData;
use crate::offset_for_leader_epoch_request_data::OffsetForLeaderEpochRequestData;
use crate::produce_request_data::ProduceRequestData;
use crate::sasl_authenticate_request_data::SaslAuthenticateRequestData;
use crate::sasl_handshake_request_data::SaslHandshakeRequestData;

use super::AlterReplicaLogDirsRequest;
use super::ApiVersionsRequest;
use super::ConcreteResponse;
use super::ConsumerGroupHeartbeatRequest;
use super::CreatePartitionsRequest;
use super::CreateTopicsRequest;
use super::DeleteRecordsRequest;
use super::DeleteTopicsRequest;
use super::DescribeClusterRequest;
use super::DescribeConfigsRequest;
use super::DescribeLogDirsRequest;
use super::FetchRequest;
use super::FindCoordinatorRequest;
use super::IncrementalAlterConfigsRequest;
use super::ListConfigResourcesRequest;
use super::ListOffsetsRequest;
use super::MetadataRequest;
use super::OffsetCommitRequest;
use super::OffsetFetchRequest;
use super::OffsetsForLeaderEpochRequest;
use super::ProduceRequest;
use super::RequestAndSize;
use super::RequestHeader;
use super::SaslAuthenticateRequest;
use super::SaslHandshakeRequest;
use super::SendBuilder;

/// Trait for building requests at a specific version.
///
/// Corresponds to the `AbstractRequest.Builder` inner class in Java.
///
/// Each concrete request type provides its own builder that implements this trait.
pub trait RequestBuilder: Send {
    /// Returns the API key for this builder's request type.
    fn api_key(&self) -> &'static ApiKeys;

    /// Returns the oldest allowed version for this builder.
    fn oldest_allowed_version(&self) -> i16;

    /// Returns the latest allowed version for this builder.
    fn latest_allowed_version(&self) -> i16;

    /// Builds the request at the latest allowed version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unsupported or preconditions are violated.
    fn build(&mut self) -> io::Result<ConcreteRequest> {
        self.build_version(self.latest_allowed_version())
    }

    /// Builds the request at the specified version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unsupported or preconditions are violated.
    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest>;
}

/// Enum dispatch for all supported Kafka request types.
///
/// Each variant wraps a concrete request struct. Common methods are dispatched
/// via `match` on the variant.
///
/// Variants will be added as request types are translated.
#[derive(Debug, Clone)]
pub enum ConcreteRequest {
    /// An ApiVersions request.
    ApiVersions(ApiVersionsRequest),
    /// A Metadata request.
    Metadata(MetadataRequest),
    /// A Produce request.
    Produce(ProduceRequest),
    /// A Fetch request (consumer fetch loop).
    Fetch(FetchRequest),
    /// A SASL handshake request.
    SaslHandshake(SaslHandshakeRequest),
    /// A SASL authenticate request.
    SaslAuthenticate(SaslAuthenticateRequest),
    /// A FindCoordinator request.
    FindCoordinator(FindCoordinatorRequest),
    /// A ListOffsets request.
    ListOffsets(ListOffsetsRequest),
    /// An OffsetsForLeaderEpoch request.
    OffsetsForLeaderEpoch(OffsetsForLeaderEpochRequest),
    /// A ConsumerGroupHeartbeat request (KIP-848).
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest),
    /// An OffsetCommit request.
    OffsetCommit(OffsetCommitRequest),
    /// An OffsetFetch request.
    OffsetFetch(OffsetFetchRequest),
    /// A CreateTopics request.
    CreateTopics(CreateTopicsRequest),
    /// A DeleteTopics request.
    DeleteTopics(DeleteTopicsRequest),
    /// A CreatePartitions request.
    CreatePartitions(CreatePartitionsRequest),
    /// A DeleteRecords request.
    DeleteRecords(DeleteRecordsRequest),
    /// A DescribeConfigs request.
    DescribeConfigs(DescribeConfigsRequest),
    /// An IncrementalAlterConfigs request.
    IncrementalAlterConfigs(IncrementalAlterConfigsRequest),
    /// A ListConfigResources request.
    ListConfigResources(ListConfigResourcesRequest),
    /// A DescribeCluster request.
    DescribeCluster(DescribeClusterRequest),
    /// A DescribeLogDirs request.
    DescribeLogDirs(DescribeLogDirsRequest),
    /// An AlterReplicaLogDirs request.
    AlterReplicaLogDirs(AlterReplicaLogDirsRequest),
}

impl ConcreteRequest {
    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        match self {
            Self::ApiVersions(r) => r.version(),
            Self::Metadata(r) => r.version(),
            Self::Produce(r) => r.version(),
            Self::Fetch(r) => r.version(),
            Self::SaslHandshake(r) => r.version(),
            Self::SaslAuthenticate(r) => r.version(),
            Self::FindCoordinator(r) => r.version(),
            Self::ListOffsets(r) => r.version(),
            Self::OffsetsForLeaderEpoch(r) => r.version(),
            Self::ConsumerGroupHeartbeat(r) => r.version(),
            Self::OffsetCommit(r) => r.version(),
            Self::OffsetFetch(r) => r.version(),
            Self::CreateTopics(r) => r.version(),
            Self::DeleteTopics(r) => r.version(),
            Self::CreatePartitions(r) => r.version(),
            Self::DeleteRecords(r) => r.version(),
            Self::DescribeConfigs(r) => r.version(),
            Self::IncrementalAlterConfigs(r) => r.version(),
            Self::ListConfigResources(r) => r.version(),
            Self::DescribeCluster(r) => r.version(),
            Self::DescribeLogDirs(r) => r.version(),
            Self::AlterReplicaLogDirs(r) => r.version(),
        }
    }

    /// Returns the API key of this request.
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
            Self::DescribeLogDirs(r) => r.api_key(),
            Self::AlterReplicaLogDirs(r) => r.api_key(),
        }
    }

    /// Builds a size-prefixed [`ByteBufferSend`] for network transmission.
    ///
    /// Corresponds to `AbstractRequest.toSend` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_send(&mut self, header: &RequestHeader) -> io::Result<ByteBufferSend> {
        match self {
            Self::ApiVersions(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::Metadata(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::Produce(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::Fetch(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::SaslHandshake(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::SaslAuthenticate(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::FindCoordinator(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ListOffsets(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::OffsetsForLeaderEpoch(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ConsumerGroupHeartbeat(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::OffsetCommit(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::OffsetFetch(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::CreateTopics(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DeleteTopics(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::CreatePartitions(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DeleteRecords(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeConfigs(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::IncrementalAlterConfigs(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::ListConfigResources(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeCluster(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::DescribeLogDirs(r) => SendBuilder::build_request_send(header, r.data_mut()),
            Self::AlterReplicaLogDirs(r) => SendBuilder::build_request_send(header, r.data_mut()),
        }
    }

    /// Serializes header and body without a size prefix.
    ///
    /// Corresponds to `AbstractRequest.serializeWithHeader` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if the header API key or version does not match this request,
    /// or if serialization fails.
    pub fn serialize_with_header(&mut self, header: &RequestHeader) -> io::Result<ByteBufferAccessor> {
        if header.api_key() != self.api_key() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Could not build request {:?} with header api key {:?}",
                    self.api_key().name(),
                    header.api_key().name()
                ),
            ));
        }
        if header.api_version() != self.version() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Could not build request version {} with header version {}",
                    self.version(),
                    header.api_version()
                ),
            ));
        }
        let version = self.version();
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
            Self::DescribeLogDirs(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
            Self::AlterReplicaLogDirs(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data_mut(), version)
            },
        }
    }

    /// Serializes just the request body (no header, no size prefix).
    ///
    /// Corresponds to `AbstractRequest.serialize` in Java (visible for testing).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn serialize(&mut self) -> io::Result<ByteBufferAccessor> {
        let version = self.version();
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
            Self::DescribeLogDirs(r) => Self::serialize_body(r.data_mut(), version),
            Self::AlterReplicaLogDirs(r) => Self::serialize_body(r.data_mut(), version),
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

    /// Returns an error response for this request.
    ///
    /// Returns `None` when the request type does not expect a response (e.g.,
    /// Produce with acks=0). In Java, `getErrorResponse()` returns `null` in
    /// those cases.
    pub fn get_error_response(
        &self,
        throttle_time_ms: i32,
        error: &crate::common::protocol::Errors,
    ) -> Option<ConcreteResponse> {
        match self {
            Self::ApiVersions(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::Metadata(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::Produce(r) => r.get_error_response(throttle_time_ms, error),
            Self::Fetch(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::SaslHandshake(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::SaslAuthenticate(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::FindCoordinator(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ListOffsets(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::OffsetsForLeaderEpoch(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ConsumerGroupHeartbeat(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::OffsetCommit(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::OffsetFetch(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::CreateTopics(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DeleteTopics(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::CreatePartitions(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DeleteRecords(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeConfigs(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::IncrementalAlterConfigs(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::ListConfigResources(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeCluster(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::DescribeLogDirs(r) => Some(r.get_error_response(throttle_time_ms, error)),
            Self::AlterReplicaLogDirs(r) => Some(r.get_error_response(throttle_time_ms, error)),
        }
    }

    /// Factory method for parsing a request object based on API key, version, and readable.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key is not supported or parsing fails.
    pub fn parse_request(
        api_key: &ApiKeys,
        api_version: i16,
        readable: &mut dyn Readable,
    ) -> io::Result<RequestAndSize> {
        let buffer_size = readable.remaining();
        let request = Self::do_parse_request(api_key, api_version, readable)?;
        Ok(RequestAndSize::new(request, buffer_size))
    }

    fn do_parse_request(api_key: &ApiKeys, api_version: i16, readable: &mut dyn Readable) -> io::Result<Self> {
        match *api_key {
            ApiKeys::API_VERSIONS => {
                let data = ApiVersionsRequestData::read(readable, api_version)?;
                Ok(Self::ApiVersions(ApiVersionsRequest::new(data, api_version)))
            },
            ApiKeys::METADATA => {
                let data = MetadataRequestData::read(readable, api_version)?;
                Ok(Self::Metadata(MetadataRequest::new(data, api_version)))
            },
            ApiKeys::PRODUCE => {
                let data = ProduceRequestData::read(readable, api_version)?;
                Ok(Self::Produce(ProduceRequest::new(data, api_version)))
            },
            ApiKeys::FETCH => {
                let data = FetchRequestData::read(readable, api_version)?;
                Ok(Self::Fetch(FetchRequest::new(data, api_version)))
            },
            ApiKeys::SASL_HANDSHAKE => {
                let data = SaslHandshakeRequestData::read(readable, api_version)?;
                Ok(Self::SaslHandshake(SaslHandshakeRequest::new(data, api_version)))
            },
            ApiKeys::SASL_AUTHENTICATE => {
                let data = SaslAuthenticateRequestData::read(readable, api_version)?;
                Ok(Self::SaslAuthenticate(SaslAuthenticateRequest::new(data, api_version)))
            },
            ApiKeys::FIND_COORDINATOR => {
                let data = FindCoordinatorRequestData::read(readable, api_version)?;
                Ok(Self::FindCoordinator(FindCoordinatorRequest::new(data, api_version)))
            },
            ApiKeys::LIST_OFFSETS => {
                let data = ListOffsetsRequestData::read(readable, api_version)?;
                Ok(Self::ListOffsets(ListOffsetsRequest::new(data, api_version)))
            },
            ApiKeys::OFFSET_FOR_LEADER_EPOCH => {
                let data = OffsetForLeaderEpochRequestData::read(readable, api_version)?;
                Ok(Self::OffsetsForLeaderEpoch(OffsetsForLeaderEpochRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::CONSUMER_GROUP_HEARTBEAT => {
                let data = ConsumerGroupHeartbeatRequestData::read(readable, api_version)?;
                Ok(Self::ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::OFFSET_COMMIT => {
                let data = OffsetCommitRequestData::read(readable, api_version)?;
                Ok(Self::OffsetCommit(OffsetCommitRequest::new(data, api_version)))
            },
            ApiKeys::OFFSET_FETCH => {
                let data = OffsetFetchRequestData::read(readable, api_version)?;
                Ok(Self::OffsetFetch(OffsetFetchRequest::new(data, api_version)))
            },
            ApiKeys::CREATE_TOPICS => {
                let data = CreateTopicsRequestData::read(readable, api_version)?;
                Ok(Self::CreateTopics(CreateTopicsRequest::new(data, api_version)))
            },
            ApiKeys::DELETE_TOPICS => {
                let data = DeleteTopicsRequestData::read(readable, api_version)?;
                Ok(Self::DeleteTopics(DeleteTopicsRequest::new(data, api_version)))
            },
            ApiKeys::CREATE_PARTITIONS => {
                let data = CreatePartitionsRequestData::read(readable, api_version)?;
                Ok(Self::CreatePartitions(CreatePartitionsRequest::new(data, api_version)))
            },
            ApiKeys::DELETE_RECORDS => {
                let data = DeleteRecordsRequestData::read(readable, api_version)?;
                Ok(Self::DeleteRecords(DeleteRecordsRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_CONFIGS => {
                let data = DescribeConfigsRequestData::read(readable, api_version)?;
                Ok(Self::DescribeConfigs(DescribeConfigsRequest::new(data, api_version)))
            },
            ApiKeys::INCREMENTAL_ALTER_CONFIGS => {
                let data = IncrementalAlterConfigsRequestData::read(readable, api_version)?;
                Ok(Self::IncrementalAlterConfigs(IncrementalAlterConfigsRequest::new(
                    data,
                    api_version,
                )))
            },
            ApiKeys::LIST_CONFIG_RESOURCES => {
                let data = ListConfigResourcesRequestData::read(readable, api_version)?;
                Ok(Self::ListConfigResources(ListConfigResourcesRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_CLUSTER => {
                let data = DescribeClusterRequestData::read(readable, api_version)?;
                Ok(Self::DescribeCluster(DescribeClusterRequest::new(data, api_version)))
            },
            ApiKeys::DESCRIBE_LOG_DIRS => {
                let data = DescribeLogDirsRequestData::read(readable, api_version)?;
                Ok(Self::DescribeLogDirs(DescribeLogDirsRequest::new(data, api_version)))
            },
            ApiKeys::ALTER_REPLICA_LOG_DIRS => {
                let data = AlterReplicaLogDirsRequestData::read(readable, api_version)?;
                Ok(Self::AlterReplicaLogDirs(AlterReplicaLogDirsRequest::new(data, api_version)))
            },
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("ApiKey {} is not currently handled in parse_request", api_key.name()),
            )),
        }
    }
}

impl std::fmt::Display for ConcreteRequest {
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
            Self::DescribeLogDirs(r) => write!(f, "{r}"),
            Self::AlterReplicaLogDirs(r) => write!(f, "{r}"),
        }
    }
}
