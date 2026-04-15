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

use crate::api_versions_request_data::ApiVersionsRequestData;
use crate::common::network::ByteBufferSend;
use crate::common::protocol::message::Message;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Readable};
use crate::metadata_request_data::MetadataRequestData;
use crate::produce_request_data::ProduceRequestData;

use super::abstract_response::ConcreteResponse;
use super::api_versions_request::ApiVersionsRequest;
use super::metadata_request::MetadataRequest;
use super::produce_request::ProduceRequest;
use super::request_and_size::RequestAndSize;
use super::request_header::RequestHeader;
use super::send_builder::SendBuilder;

/// Trait for building requests at a specific version.
///
/// Corresponds to the `AbstractRequest.Builder` inner class in Java.
///
/// Each concrete request type provides its own builder that implements this trait.
pub trait RequestBuilder {
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
    fn build(&self) -> io::Result<ConcreteRequest> {
        self.build_version(self.latest_allowed_version())
    }

    /// Builds the request at the specified version.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unsupported or preconditions are violated.
    fn build_version(&self, version: i16) -> io::Result<ConcreteRequest>;
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
}

impl ConcreteRequest {
    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        match self {
            Self::ApiVersions(r) => r.version(),
            Self::Metadata(r) => r.version(),
            Self::Produce(r) => r.version(),
        }
    }

    /// Returns the API key of this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        match self {
            Self::ApiVersions(r) => r.api_key(),
            Self::Metadata(r) => r.api_key(),
            Self::Produce(r) => r.api_key(),
        }
    }

    /// Builds a size-prefixed [`ByteBufferSend`] for network transmission.
    ///
    /// Corresponds to `AbstractRequest.toSend` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_send(&self, header: &RequestHeader) -> io::Result<ByteBufferSend> {
        match self {
            Self::ApiVersions(r) => SendBuilder::build_request_send(header, r.data()),
            Self::Metadata(r) => SendBuilder::build_request_send(header, r.data()),
            Self::Produce(r) => SendBuilder::build_request_send(header, r.data()),
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
    pub fn serialize_with_header(&self, header: &RequestHeader) -> io::Result<ByteBufferAccessor> {
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
        match self {
            Self::ApiVersions(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data(), r.version())
            },
            Self::Metadata(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data(), r.version())
            },
            Self::Produce(r) => {
                super::request_utils::serialize(header.data(), header.header_version(), r.data(), r.version())
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
    pub fn serialize(&self) -> io::Result<ByteBufferAccessor> {
        match self {
            Self::ApiVersions(r) => Self::serialize_body(r.data(), r.version()),
            Self::Metadata(r) => Self::serialize_body(r.data(), r.version()),
            Self::Produce(r) => Self::serialize_body(r.data(), r.version()),
        }
    }

    /// Serializes a message body at a given version.
    fn serialize_body(msg: &impl Message, version: i16) -> io::Result<ByteBufferAccessor> {
        let mut cache = crate::common::protocol::object_serialization_cache::ObjectSerializationCache::new();
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
        }
    }
}
