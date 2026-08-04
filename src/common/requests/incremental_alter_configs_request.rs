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

//! IncrementalAlterConfigs request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.IncrementalAlterConfigsRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::incremental_alter_configs_request_data::IncrementalAlterConfigsRequestData;
use crate::incremental_alter_configs_response_data::{
    AlterConfigsResourceResponse, IncrementalAlterConfigsResponseData,
};

use super::{ConcreteRequest, ConcreteResponse, IncrementalAlterConfigsResponse, RequestBuilder};

/// An IncrementalAlterConfigs request.
///
/// Corresponds to `org.apache.kafka.common.requests.IncrementalAlterConfigsRequest`.
#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsRequest {
    data: IncrementalAlterConfigsRequestData,
    version: i16,
}

impl IncrementalAlterConfigsRequest {
    /// Creates a new `IncrementalAlterConfigsRequest` from data and version.
    pub fn new(data: IncrementalAlterConfigsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &IncrementalAlterConfigsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut IncrementalAlterConfigsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::INCREMENTAL_ALTER_CONFIGS
    }

    /// Creates an error response for this request, failing every resource with
    /// the given error.
    ///
    /// Mirrors `IncrementalAlterConfigsRequest.getErrorResponse` (Java uses
    /// `ApiError.fromThrowable`); the enum-dispatch caller supplies the mapped
    /// [`Errors`] directly.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = IncrementalAlterConfigsResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        let responses = self
            .data
            .resources
            .iter()
            .map(|resource| {
                let mut response = AlterConfigsResourceResponse::new();
                response.set_resource_name(resource.resource_name.clone());
                response.set_resource_type(resource.resource_type);
                response.set_error_code(error.code());
                response.set_error_message(Some(error.message().to_string()));
                response
            })
            .collect();
        data.set_responses(responses);
        ConcreteResponse::IncrementalAlterConfigs(IncrementalAlterConfigsResponse::new(data))
    }

    /// Parses an `IncrementalAlterConfigsRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = IncrementalAlterConfigsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for IncrementalAlterConfigsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Config values are not printed (they may be sensitive), mirroring
        // Java's `maskData`.
        write!(
            f,
            "IncrementalAlterConfigsRequest(version={}, resources={})",
            self.version,
            self.data.resources.len()
        )
    }
}

/// Builder for [`IncrementalAlterConfigsRequest`].
///
/// Corresponds to `IncrementalAlterConfigsRequest.Builder` in Java. The Java
/// `Builder(resources, configs, validateOnly)` constructor folds the admin
/// `AlterConfigOp`/`ConfigResource` types into `IncrementalAlterConfigsRequestData`;
/// in Rust that assembly happens in the admin client (which owns those types)
/// and this builder only wraps the pre-built data (`common::requests` must not
/// depend on the `admin` module).
#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsRequestBuilder {
    data: IncrementalAlterConfigsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl IncrementalAlterConfigsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn from_data(data: IncrementalAlterConfigsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::INCREMENTAL_ALTER_CONFIGS.oldest_version(),
            latest_allowed_version: ApiKeys::INCREMENTAL_ALTER_CONFIGS.latest_version(),
        }
    }
}

impl RequestBuilder for IncrementalAlterConfigsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::INCREMENTAL_ALTER_CONFIGS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::IncrementalAlterConfigs(IncrementalAlterConfigsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incremental_alter_configs_request_data::{AlterConfigsResource, AlterableConfig};

    fn resource(name: &str, resource_type: i8, configs: &[(&str, Option<&str>, i8)]) -> AlterConfigsResource {
        let mut r = AlterConfigsResource::new();
        r.set_resource_name(name.to_string());
        r.set_resource_type(resource_type);
        r.set_configs(
            configs
                .iter()
                .map(|(name, value, op)| {
                    let mut c = AlterableConfig::new();
                    c.set_name((*name).to_string());
                    c.set_value(value.map(str::to_string));
                    c.set_config_operation(*op);
                    c
                })
                .collect(),
        );
        r
    }

    #[test]
    fn get_error_response_fails_every_resource() {
        let mut data = IncrementalAlterConfigsRequestData::new();
        data.set_resources(vec![
            resource("t", 2, &[("retention.ms", Some("1"), 0)]),
            resource("0", 4, &[("log.segment.bytes", Some("2"), 0)]),
        ]);
        let request = IncrementalAlterConfigsRequest::new(data, 1);
        let response = request.get_error_response(100, &Errors::ClusterAuthorizationFailed);
        if let ConcreteResponse::IncrementalAlterConfigs(r) = response {
            assert_eq!(r.data().throttle_time_ms, 100);
            assert_eq!(r.data().responses.len(), 2);
            for resp in &r.data().responses {
                assert_eq!(resp.error_code, Errors::ClusterAuthorizationFailed.code());
            }
        } else {
            panic!("expected IncrementalAlterConfigs response");
        }
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = IncrementalAlterConfigsRequestData::new();
        data.set_resources(vec![resource("t", 2, &[("retention.ms", Some("1000"), 0)])]);
        data.set_validate_only(true);
        let mut request = ConcreteRequest::IncrementalAlterConfigs(IncrementalAlterConfigsRequest::new(data, 1));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = IncrementalAlterConfigsRequest::parse(&mut readable, 1).unwrap();
        assert_eq!(parsed.data().resources.len(), 1);
        assert_eq!(parsed.data().resources[0].resource_name, "t");
        assert_eq!(parsed.data().resources[0].configs.len(), 1);
        assert_eq!(parsed.data().resources[0].configs[0].name, "retention.ms");
        assert_eq!(parsed.data().resources[0].configs[0].value.as_deref(), Some("1000"));
        assert_eq!(parsed.data().resources[0].configs[0].config_operation, 0);
        assert!(parsed.data().validate_only);
    }

    /// Byte-level encoding test against a known vector. IncrementalAlterConfigs
    /// v1 is flexible, so the body is:
    ///   resources: compact array (len+1 = 0x02)
    ///     resource_type: int8 = 2 (0x02)
    ///     resource_name: compact string "t" (0x02, 0x74)
    ///     configs: compact array (len+1 = 0x02)
    ///       name: compact string "k" (0x02, 0x6b)
    ///       config_operation: int8 = 0 (0x00)
    ///       value: compact nullable string "v" (0x02, 0x76)
    ///       _tagged_fields: 0x00
    ///     _tagged_fields: 0x00
    ///   validate_only: bool false (0x00)
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v1() {
        let mut data = IncrementalAlterConfigsRequestData::new();
        data.set_resources(vec![resource("t", 2, &[("k", Some("v"), 0)])]);
        let mut request = ConcreteRequest::IncrementalAlterConfigs(IncrementalAlterConfigsRequest::new(data, 1));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // resources array length + 1
            0x02, // resource_type = 2
            0x02, 0x74, // resource_name "t"
            0x02, // configs array length + 1
            0x02, 0x6b, // name "k"
            0x00, // config_operation = 0
            0x02, 0x76, // value "v"
            0x00, // config tagged fields
            0x00, // resource tagged fields
            0x00, // validate_only = false
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
