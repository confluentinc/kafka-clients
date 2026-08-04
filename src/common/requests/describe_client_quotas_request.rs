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

//! DescribeClientQuotas request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeClientQuotasRequest`.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::quota::{ClientQuotaFilter, ClientQuotaFilterComponent, ClientQuotaMatch};
use crate::describe_client_quotas_request_data::{ComponentData, DescribeClientQuotasRequestData};
use crate::describe_client_quotas_response_data::DescribeClientQuotasResponseData;

use super::{ConcreteRequest, ConcreteResponse, DescribeClientQuotasResponse, RequestBuilder};

/// Match type: exact name. These values must not change (wire contract).
pub const MATCH_TYPE_EXACT: i8 = 0;
/// Match type: default name.
pub const MATCH_TYPE_DEFAULT: i8 = 1;
/// Match type: any specified name.
pub const MATCH_TYPE_SPECIFIED: i8 = 2;

/// A DescribeClientQuotas request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeClientQuotasRequest`.
#[derive(Debug, Clone)]
pub struct DescribeClientQuotasRequest {
    data: DescribeClientQuotasRequestData,
    version: i16,
}

impl DescribeClientQuotasRequest {
    /// Creates a new `DescribeClientQuotasRequest` from data and version.
    pub fn new(data: DescribeClientQuotasRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeClientQuotasRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeClientQuotasRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_CLIENT_QUOTAS
    }

    /// Reconstructs the [`ClientQuotaFilter`] from the wire data.
    ///
    /// Mirrors `DescribeClientQuotasRequest.filter()`.
    ///
    /// # Errors
    ///
    /// Returns an error if a component carries an unexpected match type
    /// (mirrors Java's `IllegalArgumentException`).
    pub fn filter(&self) -> io::Result<ClientQuotaFilter> {
        let mut components = Vec::with_capacity(self.data.components.len());
        for component_data in &self.data.components {
            let component = match component_data.match_type {
                MATCH_TYPE_EXACT => ClientQuotaFilterComponent::of_entity(
                    component_data.entity_type.clone(),
                    component_data.r#match.clone().unwrap_or_default(),
                ),
                MATCH_TYPE_DEFAULT => ClientQuotaFilterComponent::of_default_entity(component_data.entity_type.clone()),
                MATCH_TYPE_SPECIFIED => ClientQuotaFilterComponent::of_entity_type(component_data.entity_type.clone()),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unexpected match type: {other}"),
                    ));
                },
            };
            components.push(component);
        }
        if self.data.strict {
            Ok(ClientQuotaFilter::contains_only(components))
        } else {
            Ok(ClientQuotaFilter::contains(components))
        }
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `DescribeClientQuotasRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = DescribeClientQuotasResponseData::new();
        response.set_throttle_time_ms(throttle_time_ms);
        response.set_error_code(error.code());
        response.set_error_message(Some(error.message().to_string()));
        response.set_entries(None);
        ConcreteResponse::DescribeClientQuotas(DescribeClientQuotasResponse::new(response, self.version))
    }

    /// Parses a `DescribeClientQuotasRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeClientQuotasRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeClientQuotasRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeClientQuotasRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DescribeClientQuotasRequest`].
///
/// Corresponds to `DescribeClientQuotasRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DescribeClientQuotasRequestBuilder {
    data: DescribeClientQuotasRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DescribeClientQuotasRequestBuilder {
    /// Creates a builder from a [`ClientQuotaFilter`], mirroring
    /// `DescribeClientQuotasRequest.Builder(ClientQuotaFilter)`.
    pub fn from_filter(filter: &ClientQuotaFilter) -> Self {
        let mut component_data = Vec::with_capacity(filter.components().len());
        for component in filter.components() {
            let mut fd = ComponentData::new();
            fd.set_entity_type(component.entity_type().to_string());
            match component.match_spec() {
                ClientQuotaMatch::Any => {
                    fd.set_match_type(MATCH_TYPE_SPECIFIED);
                    fd.set_match(None);
                },
                ClientQuotaMatch::Exact(name) => {
                    fd.set_match_type(MATCH_TYPE_EXACT);
                    fd.set_match(Some(name.clone()));
                },
                ClientQuotaMatch::Default => {
                    fd.set_match_type(MATCH_TYPE_DEFAULT);
                    fd.set_match(None);
                },
            }
            component_data.push(fd);
        }
        let mut data = DescribeClientQuotasRequestData::new();
        data.set_components(component_data).set_strict(filter.strict());
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_CLIENT_QUOTAS.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_CLIENT_QUOTAS.latest_version(),
        }
    }
}

impl RequestBuilder for DescribeClientQuotasRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_CLIENT_QUOTAS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DescribeClientQuotas(DescribeClientQuotasRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::quota::client_quota_entity::USER;

    #[test]
    fn builder_maps_all_match_types() {
        let filter = ClientQuotaFilter::contains(vec![
            ClientQuotaFilterComponent::of_entity(USER, "u1"),
            ClientQuotaFilterComponent::of_default_entity(USER),
            ClientQuotaFilterComponent::of_entity_type(USER),
        ]);
        let mut builder = DescribeClientQuotasRequestBuilder::from_filter(&filter);
        let ConcreteRequest::DescribeClientQuotas(r) = builder.build().unwrap() else {
            panic!("expected DescribeClientQuotas request");
        };
        let comps = &r.data().components;
        assert_eq!(comps[0].match_type, MATCH_TYPE_EXACT);
        assert_eq!(comps[0].r#match.as_deref(), Some("u1"));
        assert_eq!(comps[1].match_type, MATCH_TYPE_DEFAULT);
        assert_eq!(comps[1].r#match, None);
        assert_eq!(comps[2].match_type, MATCH_TYPE_SPECIFIED);
        assert_eq!(comps[2].r#match, None);
    }

    #[test]
    fn filter_round_trips_through_data() {
        let filter = ClientQuotaFilter::contains_only(vec![
            ClientQuotaFilterComponent::of_entity(USER, "u1"),
            ClientQuotaFilterComponent::of_default_entity(USER),
            ClientQuotaFilterComponent::of_entity_type(USER),
        ]);
        let mut builder = DescribeClientQuotasRequestBuilder::from_filter(&filter);
        let ConcreteRequest::DescribeClientQuotas(r) = builder.build().unwrap() else {
            panic!("expected DescribeClientQuotas request");
        };
        assert_eq!(r.filter().unwrap(), filter);
    }

    #[test]
    fn serialize_parse_round_trip() {
        let filter = ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity(USER, "u1")]);
        let version = ApiKeys::DESCRIBE_CLIENT_QUOTAS.latest_version();
        let mut builder = DescribeClientQuotasRequestBuilder::from_filter(&filter);
        let mut request = builder.build().unwrap();
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = DescribeClientQuotasRequest::parse(&mut readable, version).unwrap();
        assert_eq!(parsed.filter().unwrap(), filter);
    }

    #[test]
    fn filter_rejects_unexpected_match_type() {
        let mut data = DescribeClientQuotasRequestData::new();
        let mut fd = ComponentData::new();
        fd.set_entity_type(USER.to_string()).set_match_type(99).set_match(None);
        data.set_components(vec![fd]);
        let request = DescribeClientQuotasRequest::new(data, 0);
        let err = request.filter().unwrap_err();
        assert!(err.to_string().contains("Unexpected match type: 99"));
    }

    #[test]
    fn known_wire_vector_match_type_encoding() {
        // v1 (flexible) encoding: one component TOPIC-like entity "user" EXACT
        // match "u1", strict=false. Asserts the match-type byte (0=EXACT) is on
        // the wire — a wrong match-type byte is wire-incompatible with Java.
        let filter = ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity("user", "u1")]);
        let mut builder = DescribeClientQuotasRequestBuilder::from_filter(&filter);
        let mut request = builder.build_version(1).unwrap();
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x02, // components: compact array length (1 + 1)
            0x05,
            b'u',
            b's',
            b'e',
            b'r',                   // entity_type = "user" (compact string len 4+1)
            MATCH_TYPE_EXACT as u8, // match_type = 0 (EXACT)
            0x03,
            b'u',
            b'1', // match = "u1" (compact-nullable string len 2+1)
            0x00, // component tagged fields
            0x00, // strict = false
            0x00, // request tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }

    #[test]
    fn known_wire_vector_default_and_any_encoding() {
        // DEFAULT (1) and SPECIFIED/any (2) both encode a null match; assert the
        // distinct match-type bytes and the null (0x00) compact-nullable string.
        for (component, expected_match_type) in [
            (ClientQuotaFilterComponent::of_default_entity("user"), MATCH_TYPE_DEFAULT),
            (ClientQuotaFilterComponent::of_entity_type("user"), MATCH_TYPE_SPECIFIED),
        ] {
            let filter = ClientQuotaFilter::contains(vec![component]);
            let mut builder = DescribeClientQuotasRequestBuilder::from_filter(&filter);
            let mut request = builder.build_version(1).unwrap();
            let bytes = request.serialize().unwrap().into_buffer();
            let expected: Vec<u8> = vec![
                0x02, // components length (1 + 1)
                0x05,
                b'u',
                b's',
                b'e',
                b'r',                      // entity_type = "user"
                expected_match_type as u8, // match_type
                0x00,                      // match = null (compact-nullable string)
                0x00,                      // component tagged fields
                0x00,                      // strict = false
                0x00,                      // request tagged fields
            ];
            assert_eq!(bytes.as_slice(), expected.as_slice());
        }
    }
}
