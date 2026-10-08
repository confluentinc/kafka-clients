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

//! ApiVersions request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ApiVersionsRequest`.

use std::io;

use regex::Regex;
use std::sync::LazyLock;

use crate::ApiVersionsRequestData;
use crate::ApiVersionsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractRequest;
use super::ApiVersionsResponse;
use super::ConcreteResponse;
use super::RequestBuilder;

/// Default client software name for this Rust Kafka client.
const DEFAULT_CLIENT_SOFTWARE_NAME: &str = "confluent-kafka-rust";

/// Regex pattern for validating client software name and version.
///
/// Must be alphanumeric, optionally with dots and hyphens in the middle.
static SOFTWARE_NAME_VERSION_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9]([a-zA-Z0-9\.\-]*[a-zA-Z0-9])?$").unwrap());

/// An ApiVersions request.
///
/// Unlike other request types, the broker handles ApiVersions requests with higher
/// versions than supported. It does so by treating the request as if it were v0 and
/// returns a response using the v0 response schema. The reason for this is that the
/// client does not yet know what versions a broker supports when this request is sent,
/// so instead of assuming the lowest supported version, it can use the most recent
/// version and only fallback to the old version when necessary.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest")]
pub struct ApiVersionsRequest {
    data: ApiVersionsRequestData,
    version: i16,
    unsupported_request_version: Option<i16>,
}

impl ApiVersionsRequest {
    /// Creates a new `ApiVersionsRequest` from data and version.
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest#ApiVersionsRequest")]
    pub fn new(data: ApiVersionsRequestData, version: i16) -> Self {
        Self { data, version, unsupported_request_version: None }
    }

    /// Creates a new `ApiVersionsRequest` with an optional unsupported request version.
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest#ApiVersionsRequest")]
    pub fn with_unsupported_request_version(
        data: ApiVersionsRequestData,
        version: i16,
        unsupported_request_version: Option<i16>,
    ) -> Self {
        Self { data, version, unsupported_request_version }
    }

    /// Whether this request was sent with an unsupported version.
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest#hasUnsupportedRequestVersion")]
    pub fn has_unsupported_request_version(&self) -> bool {
        self.unsupported_request_version.is_some()
    }

    /// Whether the request is valid.
    ///
    /// For version >= 5, either both the cluster id and the node id are
    /// specified, or neither is (KIP-1242). For version >= 3, the client
    /// software name and version must match the `SOFTWARE_NAME_VERSION_PATTERN`
    /// regex.
    ///
    /// Translated from `ApiVersionsRequest.isValid` (`ApiVersionsRequest.java:104-117`).
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest#isValid")]
    pub fn is_valid(&self) -> bool {
        if self.version >= 5 {
            // Either cluster ID and node ID are both specified, or neither is.
            if (self.data.cluster_id.is_none() && self.data.node_id != -1)
                || (self.data.cluster_id.is_some() && self.data.node_id == -1)
            {
                return false;
            }
        }

        if self.version >= 3 {
            SOFTWARE_NAME_VERSION_PATTERN.is_match(&self.data.client_software_name)
                && SOFTWARE_NAME_VERSION_PATTERN.is_match(&self.data.client_software_version)
        } else {
            true
        }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest#data")]
    pub fn data(&self) -> &ApiVersionsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ApiVersionsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::API_VERSIONS
    }

    /// Creates an error response for this request.
    ///
    /// Starting from Apache Kafka 2.4 (KIP-511), the ApiKeys field is populated with
    /// the supported versions of the ApiVersionsRequest when an `UNSUPPORTED_VERSION`
    /// error is returned.
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = ApiVersionsResponseData::new();
        data.set_error_code(error.code());

        if self.version >= 1 {
            data.set_throttle_time_ms(throttle_time_ms);
        }

        // Starting from Apache Kafka 2.4 (KIP-511), ApiKeys field is populated with the supported
        // versions of the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
        if *error == Errors::UnsupportedVersion {
            let api_version = ApiVersionsResponse::to_api_version(&ApiKeys::API_VERSIONS);
            data.set_api_keys(vec![api_version]);
        }

        ConcreteResponse::ApiVersions(ApiVersionsResponse::new(data))
    }

    /// Parses an `ApiVersionsRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ApiVersionsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ApiVersionsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ApiVersionsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`ApiVersionsRequest`].
///
/// Corresponds to `ApiVersionsRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest$Builder")]
pub struct Builder {
    data: ApiVersionsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl Builder {
    /// Creates a default builder with the default client software name and the crate version.
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest$Builder#Builder")]
    pub fn new() -> Self {
        let mut data = ApiVersionsRequestData::new();
        data.set_client_software_name(DEFAULT_CLIENT_SOFTWARE_NAME.to_string());
        data.set_client_software_version(env!("CARGO_PKG_VERSION").to_string());
        Self {
            data,
            oldest_allowed_version: ApiKeys::API_VERSIONS.oldest_version(),
            latest_allowed_version: ApiKeys::API_VERSIONS.latest_version(),
        }
    }

    /// Creates a builder that targets a specific version.
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest$Builder#Builder")]
    pub fn with_version(version: i16) -> Self {
        let mut builder = Self::new();
        builder.oldest_allowed_version = version;
        builder.latest_allowed_version = version;
        builder
    }

    /// Creates a builder from custom data and version range.
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest$Builder#Builder")]
    pub fn with_data_oldest_allowed_version_latest_allowed_version(
        data: ApiVersionsRequestData,
        oldest_allowed_version: i16,
        latest_allowed_version: i16,
    ) -> Self {
        Self { data, oldest_allowed_version, latest_allowed_version }
    }

    /// Sets the cluster id the client expects the broker to belong to (v5+,
    /// KIP-1242). Provide it together with [`Self::set_node_id`]: a v5 request
    /// with only one of the two is invalid ([`ApiVersionsRequest::is_valid`]).
    ///
    /// Translated from `ApiVersionsRequest.Builder.setClusterId` (`ApiVersionsRequest.java:59-61`).
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest$Builder#setClusterId")]
    pub fn set_cluster_id(&mut self, cluster_id: Option<&str>) {
        self.data.set_cluster_id(cluster_id.map(str::to_string));
    }

    /// Sets the node id the client expects the broker to have (v5+,
    /// KIP-1242). Provide it together with [`Self::set_cluster_id`].
    ///
    /// Translated from `ApiVersionsRequest.Builder.setNodeId` (`ApiVersionsRequest.java:63-65`).
    #[doc(alias = "org.apache.kafka.common.requests.ApiVersionsRequest$Builder#setNodeId")]
    pub fn set_node_id(&mut self, node_id: i32) {
        self.data.set_node_id(node_id);
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestBuilder for Builder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::API_VERSIONS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        Ok(AbstractRequest::ApiVersions(ApiVersionsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_versions_request_is_valid_v0() {
        let data = ApiVersionsRequestData::new();
        let request = ApiVersionsRequest::new(data, 0);
        assert!(request.is_valid());
    }

    #[test]
    fn test_api_versions_request_is_valid_v3() {
        let mut data = ApiVersionsRequestData::new();
        data.set_client_software_name("my-client".to_string());
        data.set_client_software_version("1.0.0".to_string());
        let request = ApiVersionsRequest::new(data, 3);
        assert!(request.is_valid());
    }

    #[test]
    fn test_api_versions_request_invalid_v3_name() {
        let mut data = ApiVersionsRequestData::new();
        data.set_client_software_name("".to_string());
        data.set_client_software_version("1.0.0".to_string());
        let request = ApiVersionsRequest::new(data, 3);
        assert!(!request.is_valid());
    }

    #[test]
    fn test_api_versions_request_invalid_v3_special_chars() {
        let mut data = ApiVersionsRequestData::new();
        data.set_client_software_name("my client!".to_string());
        data.set_client_software_version("1.0.0".to_string());
        let request = ApiVersionsRequest::new(data, 3);
        assert!(!request.is_valid());
    }

    #[test]
    fn test_builder_default() {
        let builder = Builder::new();
        assert_eq!(*builder.api_key(), ApiKeys::API_VERSIONS);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::API_VERSIONS.oldest_version());
        assert_eq!(builder.latest_allowed_version(), ApiKeys::API_VERSIONS.latest_version());
    }

    #[test]
    fn test_builder_new_version() {
        let builder = Builder::with_version(2);
        assert_eq!(builder.oldest_allowed_version(), 2);
        assert_eq!(builder.latest_allowed_version(), 2);
    }

    #[test]
    fn test_has_unsupported_request_version() {
        let data = ApiVersionsRequestData::new();
        let request = ApiVersionsRequest::new(data.clone(), 0);
        assert!(!request.has_unsupported_request_version());

        let request = ApiVersionsRequest::with_unsupported_request_version(data, 0, Some(99));
        assert!(request.has_unsupported_request_version());
    }

    #[test]
    fn test_get_error_response_unsupported_version() {
        let data = ApiVersionsRequestData::new();
        let request = ApiVersionsRequest::new(data, 1);
        let response = request.get_error_response(100, &Errors::UnsupportedVersion);
        let ConcreteResponse::ApiVersions(r) = &response else {
            panic!("Expected ApiVersions response");
        };
        assert_eq!(r.data().error_code, Errors::UnsupportedVersion.code());
        assert_eq!(r.data().throttle_time_ms, 100);
        // Should have the API_VERSIONS api key in the response
        assert!(!r.data().api_keys.is_empty());
        assert_eq!(r.data().api_keys[0].api_key, ApiKeys::API_VERSIONS.id());
    }

    /// The v5 request (KIP-1242, KAFKA-20246) with `ClusterId` / `NodeId` at
    /// their defaults (`null` / -1), as the client sends it until it knows the
    /// cluster it is talking to. v3+ is flexible, so strings are compact
    /// (unsigned-varint length + 1):
    ///   client_software_name "a": 0x02 0x61
    ///   client_software_version "1": 0x02 0x31
    ///   cluster_id null (compact nullable string): 0x00
    ///   node_id -1: 0xff 0xff 0xff 0xff
    ///   top-level tagged fields: 0x00
    #[test]
    fn test_serialize_known_byte_vector_v5_defaults() {
        let mut data = ApiVersionsRequestData::new();
        data.set_client_software_name("a".to_string())
            .set_client_software_version("1".to_string());
        let mut request = AbstractRequest::ApiVersions(ApiVersionsRequest::new(data, 5));
        let expected: &[u8] = &[
            0x02, 0x61, // client_software_name "a"
            0x02, 0x31, // client_software_version "1"
            0x00, // cluster_id null
            0xff, 0xff, 0xff, 0xff, // node_id -1
            0x00, // top-level tagged fields
        ];
        assert_eq!(request.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    /// The v5 request carrying both `ClusterId` and `NodeId`:
    ///   cluster_id "c": 0x02 0x63
    ///   node_id 7: 0x00 0x00 0x00 0x07
    #[test]
    fn test_serialize_known_byte_vector_v5_cluster_and_node_id() {
        let mut data = ApiVersionsRequestData::new();
        data.set_client_software_name("a".to_string())
            .set_client_software_version("1".to_string())
            .set_cluster_id(Some("c".to_string()))
            .set_node_id(7);
        let mut request = AbstractRequest::ApiVersions(ApiVersionsRequest::new(data, 5));
        let expected: &[u8] = &[
            0x02, 0x61, // client_software_name "a"
            0x02, 0x31, // client_software_version "1"
            0x02, 0x63, // cluster_id "c"
            0x00, 0x00, 0x00, 0x07, // node_id 7
            0x00, // top-level tagged fields
        ];
        assert_eq!(request.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    /// Below v5 both fields are absent from the wire. They are `ignorable`, so a
    /// non-default value is dropped silently rather than rejected (Java's
    /// generator emits the version-gate check only for non-ignorable fields).
    #[test]
    fn test_serialize_known_byte_vector_v4_drops_cluster_and_node_id() {
        let mut data = ApiVersionsRequestData::new();
        data.set_client_software_name("a".to_string())
            .set_client_software_version("1".to_string())
            .set_cluster_id(Some("c".to_string()))
            .set_node_id(7);
        let mut request = AbstractRequest::ApiVersions(ApiVersionsRequest::new(data, 4));
        let expected: &[u8] = &[
            0x02, 0x61, // client_software_name "a"
            0x02, 0x31, // client_software_version "1"
            0x00, // top-level tagged fields
        ];
        assert_eq!(request.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    /// The builder setters (KIP-1242, `ApiVersionsRequest.Builder.setClusterId` /
    /// `setNodeId`) reach the wire: the v5 request built from a default builder
    /// carries the cluster id and node id in place of `null` / -1. The software
    /// name and version are the builder's defaults, `confluent-kafka-rust` and
    /// the crate version.
    #[test]
    fn test_builder_set_cluster_id_and_node_id_known_byte_vector_v5() {
        let mut builder = Builder::new();
        builder.set_cluster_id(Some("c"));
        builder.set_node_id(7);
        let AbstractRequest::ApiVersions(built) = builder.build_version(5).unwrap() else {
            panic!("expected an ApiVersions request");
        };
        assert!(built.is_valid());
        let mut request = AbstractRequest::ApiVersions(built);

        let name = DEFAULT_CLIENT_SOFTWARE_NAME.as_bytes();
        let version = env!("CARGO_PKG_VERSION").as_bytes();
        let mut expected = vec![(name.len() + 1) as u8];
        expected.extend_from_slice(name);
        expected.push((version.len() + 1) as u8);
        expected.extend_from_slice(version);
        expected.extend_from_slice(&[
            0x02, 0x63, // cluster_id "c"
            0x00, 0x00, 0x00, 0x07, // node_id 7
            0x00, // top-level tagged fields
        ]);
        assert_eq!(request.serialize().unwrap().into_buffer().as_slice(), expected.as_slice());
    }

    /// v5 requires the cluster id and node id together (KIP-1242,
    /// `ApiVersionsRequest.isValid`): both or neither is valid, one alone is
    /// not. Below v5 the pair is not checked.
    #[test]
    fn test_is_valid_v5_requires_cluster_id_and_node_id_together() {
        let request = |cluster_id: Option<&str>, node_id: i32, version: i16| {
            let mut data = ApiVersionsRequestData::new();
            data.set_client_software_name("a".to_string())
                .set_client_software_version("1".to_string())
                .set_cluster_id(cluster_id.map(str::to_string))
                .set_node_id(node_id);
            ApiVersionsRequest::new(data, version)
        };
        assert!(request(None, -1, 5).is_valid());
        assert!(request(Some("c"), 0, 5).is_valid());
        assert!(!request(None, 0, 5).is_valid());
        assert!(!request(Some("c"), -1, 5).is_valid());
        assert!(request(None, 0, 4).is_valid());
        assert!(request(Some("c"), -1, 4).is_valid());
        // The pair check comes first, but the v3 software-name check still applies.
        let mut data = ApiVersionsRequestData::new();
        data.set_client_software_name("my client!".to_string())
            .set_client_software_version("1".to_string())
            .set_cluster_id(Some("c".to_string()))
            .set_node_id(0);
        assert!(!ApiVersionsRequest::new(data, 5).is_valid());
    }

    /// The v5 bytes parse back to the same `ClusterId` / `NodeId`.
    #[test]
    fn test_parse_v5_cluster_and_node_id() {
        let bytes = vec![0x02, 0x61, 0x02, 0x31, 0x02, 0x63, 0x00, 0x00, 0x00, 0x07, 0x00];
        let mut readable = crate::common::protocol::ByteBufferAccessor::new(bytes);
        let request = ApiVersionsRequest::parse(&mut readable, 5).unwrap();
        assert_eq!(request.data().client_software_name, "a");
        assert_eq!(request.data().client_software_version, "1");
        assert_eq!(request.data().cluster_id.as_deref(), Some("c"));
        assert_eq!(request.data().node_id, 7);
    }
}
