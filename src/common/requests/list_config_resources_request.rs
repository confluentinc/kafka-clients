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

//! ListConfigResources request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ListConfigResourcesRequest`.

use std::io;

use crate::common::config::ConfigResourceType;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::list_config_resources_request_data::ListConfigResourcesRequestData;
use crate::list_config_resources_response_data::ListConfigResourcesResponseData;

use super::{ConcreteRequest, ConcreteResponse, ListConfigResourcesResponse, RequestBuilder};

/// A ListConfigResources request.
///
/// Corresponds to `org.apache.kafka.common.requests.ListConfigResourcesRequest`.
#[derive(Debug, Clone)]
pub struct ListConfigResourcesRequest {
    data: ListConfigResourcesRequestData,
    version: i16,
}

impl ListConfigResourcesRequest {
    /// Creates a new `ListConfigResourcesRequest` from data and version.
    pub fn new(data: ListConfigResourcesRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListConfigResourcesRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ListConfigResourcesRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_CONFIG_RESOURCES
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `ListConfigResourcesRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = ListConfigResourcesResponseData::new();
        data.set_error_code(error.code());
        data.set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::ListConfigResources(ListConfigResourcesResponse::new(data))
    }

    /// Parses a `ListConfigResourcesRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListConfigResourcesRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ListConfigResourcesRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListConfigResourcesRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`ListConfigResourcesRequest`].
///
/// Corresponds to `ListConfigResourcesRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ListConfigResourcesRequestBuilder {
    data: ListConfigResourcesRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ListConfigResourcesRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: ListConfigResourcesRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::LIST_CONFIG_RESOURCES.oldest_version(),
            latest_allowed_version: ApiKeys::LIST_CONFIG_RESOURCES.latest_version(),
        }
    }
}

impl RequestBuilder for ListConfigResourcesRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_CONFIG_RESOURCES
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Mirrors `ListConfigResourcesRequest.Builder.build`: v0 only supports
        // the CLIENT_METRICS resource type and carries no resource-types field.
        if version == 0 {
            let types = &self.data.resource_types;
            let only_client_metrics = types.len() == 1 && types[0] == ConfigResourceType::ClientMetrics.id();
            if !only_client_metrics {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "The v0 ListConfigResources only supports CLIENT_METRICS",
                ));
            }
            return Ok(ConcreteRequest::ListConfigResources(ListConfigResourcesRequest::new(
                ListConfigResourcesRequestData::new(),
                version,
            )));
        }
        Ok(ConcreteRequest::ListConfigResources(ListConfigResourcesRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_error_response_sets_error_code() {
        let request = ListConfigResourcesRequest::new(ListConfigResourcesRequestData::new(), 1);
        let response = request.get_error_response(100, &Errors::UnsupportedVersion);
        if let ConcreteResponse::ListConfigResources(r) = response {
            assert_eq!(r.data().error_code, Errors::UnsupportedVersion.code());
            assert_eq!(r.data().throttle_time_ms, 100);
        } else {
            panic!("expected ListConfigResources response");
        }
    }

    #[test]
    fn build_v0_rejects_non_client_metrics() {
        let mut data = ListConfigResourcesRequestData::new();
        data.set_resource_types(vec![ConfigResourceType::Topic.id()]);
        let mut builder = ListConfigResourcesRequestBuilder::new(data);
        assert!(builder.build_version(0).is_err());
    }

    #[test]
    fn build_v0_allows_client_metrics() {
        let mut data = ListConfigResourcesRequestData::new();
        data.set_resource_types(vec![ConfigResourceType::ClientMetrics.id()]);
        let mut builder = ListConfigResourcesRequestBuilder::new(data);
        assert!(builder.build_version(0).is_ok());
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = ListConfigResourcesRequestData::new();
        data.set_resource_types(vec![ConfigResourceType::Topic.id(), ConfigResourceType::Broker.id()]);
        let mut request = ConcreteRequest::ListConfigResources(ListConfigResourcesRequest::new(data, 1));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = ListConfigResourcesRequest::parse(&mut readable, 1).unwrap();
        assert_eq!(parsed.data().resource_types, vec![2, 4]);
    }

    /// Byte-level encoding test against a known vector. ListConfigResources v1
    /// is flexible, so the body is:
    ///   resource_types: compact array of int8 (len+1 = 0x02), [2]
    ///   _tagged_fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v1() {
        let mut data = ListConfigResourcesRequestData::new();
        data.set_resource_types(vec![ConfigResourceType::Topic.id()]);
        let mut request = ConcreteRequest::ListConfigResources(ListConfigResourcesRequest::new(data, 1));
        let bytes = request.serialize().unwrap();
        let expected: &[u8] = &[
            0x02, // resource_types array length + 1
            0x02, // resource_type = 2 (TOPIC)
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.into_buffer().as_slice(), expected);
    }
}
