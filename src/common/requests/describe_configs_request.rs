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

//! DescribeConfigs request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeConfigsRequest`.

use std::io;

use crate::DescribeConfigsRequestData;
use crate::DescribeConfigsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_configs_response_data::DescribeConfigsResult;

use super::{ConcreteRequest, ConcreteResponse, DescribeConfigsResponse, RequestBuilder};

/// A DescribeConfigs request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeConfigsRequest`.
#[derive(Debug, Clone)]
pub struct DescribeConfigsRequest {
    data: DescribeConfigsRequestData,
    version: i16,
}

impl DescribeConfigsRequest {
    /// Creates a new `DescribeConfigsRequest` from data and version.
    pub fn new(data: DescribeConfigsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeConfigsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeConfigsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_CONFIGS
    }

    /// Creates an error response for this request, failing every requested
    /// resource with the given error.
    ///
    /// Mirrors `DescribeConfigsRequest.getErrorResponse` (Java uses
    /// `Errors.forException`); the enum-dispatch caller supplies the mapped
    /// [`Errors`] directly.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = DescribeConfigsResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        let results = self
            .data
            .resources
            .iter()
            .map(|resource| {
                let mut result = DescribeConfigsResult::new();
                result.set_error_code(error.code());
                result.set_error_message(Some(error.message().to_string()));
                result.set_resource_name(resource.resource_name.clone());
                result.set_resource_type(resource.resource_type);
                result
            })
            .collect();
        data.set_results(results);
        ConcreteResponse::DescribeConfigs(DescribeConfigsResponse::new(data))
    }

    /// Parses a `DescribeConfigsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeConfigsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeConfigsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeConfigsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DescribeConfigsRequest`].
///
/// Corresponds to `DescribeConfigsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DescribeConfigsRequestBuilder {
    data: DescribeConfigsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DescribeConfigsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: DescribeConfigsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_CONFIGS.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_CONFIGS.latest_version(),
        }
    }
}

impl RequestBuilder for DescribeConfigsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_CONFIGS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DescribeConfigs(DescribeConfigsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_configs_request_data::DescribeConfigsResource;

    fn resource(name: &str, resource_type: i8) -> DescribeConfigsResource {
        let mut r = DescribeConfigsResource::new();
        r.set_resource_name(name.to_string());
        r.set_resource_type(resource_type);
        r.set_configuration_keys(None);
        r
    }

    #[test]
    fn get_error_response_fails_every_resource() {
        let mut data = DescribeConfigsRequestData::new();
        data.set_resources(vec![resource("topic", 2), resource("0", 4)]);
        let request = DescribeConfigsRequest::new(data, 4);
        let response = request.get_error_response(100, &Errors::InvalidRequest);
        if let ConcreteResponse::DescribeConfigs(r) = response {
            assert_eq!(r.data().throttle_time_ms, 100);
            assert_eq!(r.data().results.len(), 2);
            for result in &r.data().results {
                assert_eq!(result.error_code, Errors::InvalidRequest.code());
                assert_eq!(result.error_message.as_deref(), Some(Errors::InvalidRequest.message()));
            }
        } else {
            panic!("expected DescribeConfigs response");
        }
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = DescribeConfigsRequestData::new();
        data.set_resources(vec![resource("topic", 2)]);
        data.set_include_synonyms(true);
        data.set_include_documentation(true);
        let mut request = ConcreteRequest::DescribeConfigs(DescribeConfigsRequest::new(data, 4));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = DescribeConfigsRequest::parse(&mut readable, 4).unwrap();
        assert_eq!(parsed.data().resources.len(), 1);
        assert_eq!(parsed.data().resources[0].resource_name, "topic");
        assert_eq!(parsed.data().resources[0].resource_type, 2);
        assert!(parsed.data().include_synonyms);
        assert!(parsed.data().include_documentation);
    }

    /// Byte-level encoding test against a known vector. DescribeConfigs v4 is a
    /// flexible version, so the body is:
    ///   resources: compact array (len+1 = 0x02)
    ///     resource_type: int8 = 2 (0x02)
    ///     resource_name: compact string "t" (0x02, 0x74)
    ///     configuration_keys: null compact array (0x00)
    ///     _tagged_fields: 0x00
    ///   include_synonyms: bool false (0x00)
    ///   include_documentation: bool false (0x00)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v4() {
        let mut data = DescribeConfigsRequestData::new();
        data.set_resources(vec![resource("t", 2)]);
        let mut request = ConcreteRequest::DescribeConfigs(DescribeConfigsRequest::new(data, 4));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // resources array length + 1
            0x02, // resource_type = 2
            0x02, 0x74, // resource_name "t"
            0x00, // configuration_keys = null
            0x00, // resource tagged fields
            0x00, // include_synonyms = false
            0x00, // include_documentation = false
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
