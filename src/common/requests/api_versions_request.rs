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

use crate::api_versions_request_data::ApiVersionsRequestData;
use crate::api_versions_response_data::ApiVersionsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::ConcreteRequest;
use super::abstract_request::RequestBuilder;
use super::abstract_response::ConcreteResponse;
use super::api_versions_response::ApiVersionsResponse;

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
pub struct ApiVersionsRequest {
    data: ApiVersionsRequestData,
    version: i16,
    unsupported_request_version: Option<i16>,
}

impl ApiVersionsRequest {
    /// Creates a new `ApiVersionsRequest` from data and version.
    pub fn new(data: ApiVersionsRequestData, version: i16) -> Self {
        Self { data, version, unsupported_request_version: None }
    }

    /// Creates a new `ApiVersionsRequest` with an optional unsupported request version.
    pub fn with_unsupported_version(
        data: ApiVersionsRequestData,
        version: i16,
        unsupported_request_version: Option<i16>,
    ) -> Self {
        Self { data, version, unsupported_request_version }
    }

    /// Whether this request was sent with an unsupported version.
    pub fn has_unsupported_request_version(&self) -> bool {
        self.unsupported_request_version.is_some()
    }

    /// Whether the request is valid.
    ///
    /// For version >= 3, the client software name and version must match the
    /// `SOFTWARE_NAME_VERSION_PATTERN` regex.
    pub fn is_valid(&self) -> bool {
        if self.version >= 3 {
            SOFTWARE_NAME_VERSION_PATTERN.is_match(&self.data.client_software_name)
                && SOFTWARE_NAME_VERSION_PATTERN.is_match(&self.data.client_software_version)
        } else {
            true
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ApiVersionsRequestData {
        &self.data
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
pub struct ApiVersionsRequestBuilder {
    data: ApiVersionsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ApiVersionsRequestBuilder {
    /// Creates a default builder with the default client software name and the crate version.
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
    pub fn for_version(version: i16) -> Self {
        let mut builder = Self::new();
        builder.oldest_allowed_version = version;
        builder.latest_allowed_version = version;
        builder
    }

    /// Creates a builder from custom data and version range.
    pub fn from_data(data: ApiVersionsRequestData, oldest_allowed_version: i16, latest_allowed_version: i16) -> Self {
        Self { data, oldest_allowed_version, latest_allowed_version }
    }
}

impl Default for ApiVersionsRequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestBuilder for ApiVersionsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::API_VERSIONS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::ApiVersions(ApiVersionsRequest::new(
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
        let builder = ApiVersionsRequestBuilder::new();
        assert_eq!(*builder.api_key(), ApiKeys::API_VERSIONS);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::API_VERSIONS.oldest_version());
        assert_eq!(builder.latest_allowed_version(), ApiKeys::API_VERSIONS.latest_version());
    }

    #[test]
    fn test_builder_for_version() {
        let builder = ApiVersionsRequestBuilder::for_version(2);
        assert_eq!(builder.oldest_allowed_version(), 2);
        assert_eq!(builder.latest_allowed_version(), 2);
    }

    #[test]
    fn test_has_unsupported_request_version() {
        let data = ApiVersionsRequestData::new();
        let request = ApiVersionsRequest::new(data.clone(), 0);
        assert!(!request.has_unsupported_request_version());

        let request = ApiVersionsRequest::with_unsupported_version(data, 0, Some(99));
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
}
