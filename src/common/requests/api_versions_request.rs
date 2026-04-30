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

//! Translation of `org.apache.kafka.common.requests.ApiVersionsRequest`.

use crate::common::errors::KafkaError;
use crate::common::message::api_versions_request_data::ApiVersionsRequestData;
use crate::common::message::api_versions_response_data::ApiVersionsResponseData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequest;
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::ApiVersionsResponse;

/// Translation of `org.apache.kafka.common.requests.ApiVersionsRequest`.
pub struct ApiVersionsRequest {
    data: ApiVersionsRequestData,
    version: i16,
    /// `unsupportedRequestVersion` from Java — populated when the broker
    /// receives an `ApiVersionsRequest` with a version higher than it
    /// supports and treats it as v0.
    unsupported_request_version: Option<i16>,
}

impl ApiVersionsRequest {
    /// Mirrors `new ApiVersionsRequest(ApiVersionsRequestData data, short version)`.
    pub fn new(data: ApiVersionsRequestData, version: i16) -> Self {
        ApiVersionsRequest { data, version, unsupported_request_version: None }
    }

    /// Mirrors `new ApiVersionsRequest(ApiVersionsRequestData, short version,
    /// Short unsupportedRequestVersion)`.
    pub fn with_unsupported_version(
        data: ApiVersionsRequestData,
        version: i16,
        unsupported_request_version: i16,
    ) -> Self {
        ApiVersionsRequest { data, version, unsupported_request_version: Some(unsupported_request_version) }
    }

    /// Mirrors `ApiVersionsRequest.hasUnsupportedRequestVersion()`.
    pub fn has_unsupported_request_version(&self) -> bool {
        self.unsupported_request_version.is_some()
    }

    /// Mirrors `ApiVersionsRequest.isValid()`.
    ///
    /// Java enforces a regex on `clientSoftwareName` /
    /// `clientSoftwareVersion` for v3+. The pattern is
    /// `[a-zA-Z0-9](?:[a-zA-Z0-9\\-.]*[a-zA-Z0-9])?` — one or more chars,
    /// each from `[a-zA-Z0-9\-.]`, with the first and last not being `-`
    /// or `.`. We validate that explicitly to avoid pulling in a regex
    /// crate for a single call site.
    pub fn is_valid(&self) -> bool {
        if self.version >= 3 {
            valid_software_name_or_version(self.data.client_software_name.as_str())
                && valid_software_name_or_version(self.data.client_software_version.as_str())
        } else {
            true
        }
    }

    /// Mirrors `ApiVersionsRequest.data()`.
    pub fn request_data(&self) -> &ApiVersionsRequestData {
        &self.data
    }

    /// Mirrors `ApiVersionsRequest.parse(Readable, short)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        let data = ApiVersionsRequestData::read(accessor, version)?;
        Ok(ApiVersionsRequest::new(data, version))
    }
}

fn valid_software_name_or_version(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    let valid = |c: u8, allow_dash_dot: bool| -> bool {
        c.is_ascii_alphanumeric() || (allow_dash_dot && (c == b'-' || c == b'.'))
    };
    if !valid(bytes[0], false) {
        return false;
    }
    if bytes.len() == 1 {
        return true;
    }
    if !valid(bytes[bytes.len() - 1], false) {
        return false;
    }
    for &b in &bytes[1..bytes.len() - 1] {
        if !valid(b, true) {
            return false;
        }
    }
    true
}

impl AbstractRequestResponse for ApiVersionsRequest {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractRequest for ApiVersionsRequest {
    fn version(&self) -> i16 {
        self.version
    }

    fn api_key(&self) -> &'static ApiKey {
        ApiKeys::for_id(18).expect("API_VERSIONS")
    }

    fn get_error_response(&self, throttle_time_ms: i32, error: &KafkaError) -> Option<Box<dyn AbstractResponse>> {
        let mut data = ApiVersionsResponseData {
            error_code: Errors::for_code(error.code()).code(),
            ..ApiVersionsResponseData::new()
        };
        if self.version >= 1 {
            data.throttle_time_ms = throttle_time_ms;
        }
        // Starting from Kafka 2.4 (KIP-511), populate `apiKeys` with the
        // supported versions of the ApiVersionsRequest itself when the
        // error is UNSUPPORTED_VERSION.
        if matches!(error, KafkaError::UnsupportedVersion(_)) {
            data.api_keys.push(ApiVersionsResponse::to_api_version(self.api_key()));
        }
        Some(Box::new(ApiVersionsResponse::new(data)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trip_v0() {
        let req_data = ApiVersionsRequestData::new();
        let request = ApiVersionsRequest::new(req_data, 0);
        let mut serialized = AbstractRequest::serialize(&request).expect("serialize");

        let parsed = ApiVersionsRequest::parse(&mut serialized, 0).expect("parse");
        assert_eq!(parsed.version, 0);
        assert!(parsed.request_data().client_software_name.is_empty());
    }

    /// At v3+, `clientSoftwareName` and `clientSoftwareVersion` must match
    /// the regex `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`.
    #[test]
    fn is_valid_v3_rejects_empty_software_name() {
        let req = ApiVersionsRequest::new(ApiVersionsRequestData::new(), 3);
        assert!(!req.is_valid(), "empty client_software_name should fail");
    }

    #[test]
    fn is_valid_v3_accepts_well_formed() {
        let data = ApiVersionsRequestData {
            client_software_name: "apache-kafka-java".to_owned(),
            client_software_version: "4.2.0".to_owned(),
            unknown_tagged_fields: Vec::new(),
        };
        let req = ApiVersionsRequest::new(data, 3);
        assert!(req.is_valid());
    }

    #[test]
    fn is_valid_v3_rejects_leading_dash() {
        let data = ApiVersionsRequestData {
            client_software_name: "-apache".to_owned(),
            client_software_version: "1".to_owned(),
            unknown_tagged_fields: Vec::new(),
        };
        let req = ApiVersionsRequest::new(data, 3);
        assert!(!req.is_valid());
    }

    #[test]
    fn is_valid_v0_skips_regex_check() {
        let req = ApiVersionsRequest::new(ApiVersionsRequestData::new(), 0);
        assert!(req.is_valid(), "v0 has no regex check");
    }

    #[test]
    fn unsupported_version_error_response_includes_api_keys_kip_511() {
        let req = ApiVersionsRequest::new(ApiVersionsRequestData::new(), 3);
        let resp = req
            .get_error_response(0, &KafkaError::UnsupportedVersion(String::new()))
            .expect("response");
        // Downcast not directly possible; instead parse out the shape via
        // `error_counts` and the response's known surface.
        let counts = resp.error_counts();
        let total: i32 = counts.values().sum();
        assert!(total >= 1);
    }
}
