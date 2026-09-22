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

//! DescribeCluster request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeClusterRequest`.

use std::io;

use crate::DescribeClusterRequestData;
use crate::DescribeClusterResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::{ConcreteRequest, ConcreteResponse, DescribeClusterResponse, RequestBuilder};

/// A DescribeCluster request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeClusterRequest`.
#[derive(Debug, Clone)]
pub struct DescribeClusterRequest {
    data: DescribeClusterRequestData,
    version: i16,
}

impl DescribeClusterRequest {
    /// The `EndpointType.BROKER` id (KIP-919).
    pub const ENDPOINT_TYPE_BROKER: i8 = 1;

    /// The `EndpointType.CONTROLLER` id (KIP-919).
    pub const ENDPOINT_TYPE_CONTROLLER: i8 = 2;

    /// Creates a new `DescribeClusterRequest` from data and version.
    pub fn new(data: DescribeClusterRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeClusterRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeClusterRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_CLUSTER
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `DescribeClusterRequest.getErrorResponse` (Java uses
    /// `ApiError.fromThrowable`).
    pub fn get_error_response(&self, _throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = DescribeClusterResponseData::new();
        data.set_error_code(error.code());
        data.set_error_message(Some(error.message().to_string()));
        ConcreteResponse::DescribeCluster(DescribeClusterResponse::new(data))
    }

    /// Parses a `DescribeClusterRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeClusterRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeClusterRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeClusterRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DescribeClusterRequest`].
///
/// Corresponds to `DescribeClusterRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DescribeClusterRequestBuilder {
    data: DescribeClusterRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DescribeClusterRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: DescribeClusterRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_CLUSTER.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_CLUSTER.latest_version(),
        }
    }
}

impl RequestBuilder for DescribeClusterRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_CLUSTER
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DescribeCluster(DescribeClusterRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_error_response_sets_error() {
        let request = DescribeClusterRequest::new(DescribeClusterRequestData::new(), 1);
        let response = request.get_error_response(0, &Errors::InvalidRequest);
        if let ConcreteResponse::DescribeCluster(r) = response {
            assert_eq!(r.data().error_code, Errors::InvalidRequest.code());
            assert_eq!(r.data().error_message.as_deref(), Some(Errors::InvalidRequest.message()));
        } else {
            panic!("expected DescribeCluster response");
        }
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = DescribeClusterRequestData::new();
        data.set_include_cluster_authorized_operations(true);
        data.set_endpoint_type(DescribeClusterRequest::ENDPOINT_TYPE_BROKER);
        let mut request = ConcreteRequest::DescribeCluster(DescribeClusterRequest::new(data, 1));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = DescribeClusterRequest::parse(&mut readable, 1).unwrap();
        assert!(parsed.data().include_cluster_authorized_operations);
        assert_eq!(parsed.data().endpoint_type, DescribeClusterRequest::ENDPOINT_TYPE_BROKER);
    }

    /// Byte-level encoding test against a known vector. DescribeCluster v1 is
    /// flexible, so the body is:
    ///   include_cluster_authorized_operations: bool true (0x01)
    ///   endpoint_type: int8 = 1 (0x01)  [present at v1+]
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v1() {
        let mut data = DescribeClusterRequestData::new();
        data.set_include_cluster_authorized_operations(true);
        data.set_endpoint_type(DescribeClusterRequest::ENDPOINT_TYPE_BROKER);
        let mut request = ConcreteRequest::DescribeCluster(DescribeClusterRequest::new(data, 1));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x01, // include_cluster_authorized_operations = true
            0x01, // endpoint_type = 1 (BROKER)
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
