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

//! DescribeAcls request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeAclsRequest`.

use std::io;

use crate::DescribeAclsRequestData;
use crate::DescribeAclsResponseData;
use crate::common::acl::{AccessControlEntryFilter, AclBindingFilter, AclOperation, AclPermissionType};
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::resource::{PatternType, ResourcePatternFilter, ResourceType};

use super::{ConcreteRequest, ConcreteResponse, DescribeAclsResponse, RequestBuilder};

/// A DescribeAcls request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeAclsRequest`.
#[derive(Debug, Clone)]
pub struct DescribeAclsRequest {
    data: DescribeAclsRequestData,
    version: i16,
}

impl DescribeAclsRequest {
    /// Creates a new `DescribeAclsRequest` from data and version.
    pub fn new(data: DescribeAclsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeAclsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeAclsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_ACLS
    }

    /// Reconstructs the [`AclBindingFilter`] from the wire data.
    ///
    /// Mirrors `DescribeAclsRequest.filter()`.
    pub fn filter(&self) -> AclBindingFilter {
        let rpf = ResourcePatternFilter::new(
            ResourceType::from_code(self.data.resource_type_filter),
            self.data.resource_name_filter.clone(),
            PatternType::from_code(self.data.pattern_type_filter),
        );
        let acef = AccessControlEntryFilter::new(
            self.data.principal_filter.clone(),
            self.data.host_filter.clone(),
            AclOperation::from_code(self.data.operation),
            AclPermissionType::from_code(self.data.permission_type),
        );
        AclBindingFilter::new(rpf, acef)
    }

    /// Creates an error response for this request.
    ///
    /// Mirrors `DescribeAclsRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = DescribeAclsResponseData::new();
        response.set_throttle_time_ms(throttle_time_ms);
        response.set_error_code(error.code());
        response.set_error_message(Some(error.message().to_string()));
        ConcreteResponse::DescribeAcls(DescribeAclsResponse::new(response, self.version))
    }

    /// Parses a `DescribeAclsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeAclsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeAclsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeAclsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DescribeAclsRequest`].
///
/// Corresponds to `DescribeAclsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DescribeAclsRequestBuilder {
    data: DescribeAclsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DescribeAclsRequestBuilder {
    /// Creates a builder from an [`AclBindingFilter`], mirroring
    /// `DescribeAclsRequest.Builder(AclBindingFilter)`.
    pub fn new(filter: &AclBindingFilter) -> Self {
        let pattern_filter = filter.pattern_filter();
        let entry_filter = filter.entry_filter();
        let mut data = DescribeAclsRequestData::new();
        data.set_host_filter(entry_filter.host().map(str::to_string))
            .set_operation(entry_filter.operation().code())
            .set_permission_type(entry_filter.permission_type().code())
            .set_principal_filter(entry_filter.principal().map(str::to_string))
            .set_resource_name_filter(pattern_filter.name().map(str::to_string))
            .set_pattern_type_filter(pattern_filter.pattern_type().code())
            .set_resource_type_filter(pattern_filter.resource_type().code());
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_ACLS.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_ACLS.latest_version(),
        }
    }
}

impl RequestBuilder for DescribeAclsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_ACLS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Mirrors DescribeAclsRequest.normalizeAndValidate. Version 0 was
        // removed in Kafka 4.0 (valid versions 1-3), so the v0 pattern-type
        // normalization is unreachable; the UNKNOWN-elements guard remains.
        if self.data.pattern_type_filter == PatternType::Unknown.code()
            || self.data.resource_type_filter == ResourceType::Unknown.code()
            || self.data.permission_type == AclPermissionType::Unknown.code()
            || self.data.operation == AclOperation::Unknown.code()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("DescribeAclsRequest contains UNKNOWN elements: {:?}", self.data),
            ));
        }
        Ok(ConcreteRequest::DescribeAcls(DescribeAclsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_filter() -> AclBindingFilter {
        AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Topic, Some("t".to_string()), PatternType::Literal),
            AccessControlEntryFilter::new(
                Some("User:x".to_string()),
                Some("host".to_string()),
                AclOperation::Read,
                AclPermissionType::Allow,
            ),
        )
    }

    #[test]
    fn builder_maps_filter_fields() {
        let mut builder = DescribeAclsRequestBuilder::new(&sample_filter());
        let request = builder.build().unwrap();
        let ConcreteRequest::DescribeAcls(r) = request else {
            panic!("expected DescribeAcls request");
        };
        assert_eq!(r.data().resource_type_filter, ResourceType::Topic.code());
        assert_eq!(r.data().resource_name_filter.as_deref(), Some("t"));
        assert_eq!(r.data().pattern_type_filter, PatternType::Literal.code());
        assert_eq!(r.data().principal_filter.as_deref(), Some("User:x"));
        assert_eq!(r.data().host_filter.as_deref(), Some("host"));
        assert_eq!(r.data().operation, AclOperation::Read.code());
        assert_eq!(r.data().permission_type, AclPermissionType::Allow.code());
    }

    #[test]
    fn filter_round_trips_through_data() {
        let mut builder = DescribeAclsRequestBuilder::new(&sample_filter());
        let ConcreteRequest::DescribeAcls(r) = builder.build().unwrap() else {
            panic!("expected DescribeAcls request");
        };
        assert_eq!(r.filter(), sample_filter());
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut builder = DescribeAclsRequestBuilder::new(&sample_filter());
        let version = ApiKeys::DESCRIBE_ACLS.latest_version();
        let ConcreteRequest::DescribeAcls(r) = builder.build().unwrap() else {
            panic!("expected DescribeAcls request");
        };
        let mut request = ConcreteRequest::DescribeAcls(r);
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = DescribeAclsRequest::parse(&mut readable, version).unwrap();
        assert_eq!(parsed.filter(), sample_filter());
    }

    #[test]
    fn build_rejects_unknown_elements() {
        let filter = AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Unknown, Some("t".to_string()), PatternType::Literal),
            AccessControlEntryFilter::new(None, None, AclOperation::Read, AclPermissionType::Allow),
        );
        let mut builder = DescribeAclsRequestBuilder::new(&filter);
        assert!(builder.build().is_err());
    }

    #[test]
    fn known_wire_vector() {
        // v3 (flexible) encoding for filter (TOPIC="t" LITERAL, principal
        // "User:x", host "*", READ, ALLOW).
        let filter = AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Topic, Some("t".to_string()), PatternType::Literal),
            AccessControlEntryFilter::new(
                Some("User:x".to_string()),
                Some("*".to_string()),
                AclOperation::Read,
                AclPermissionType::Allow,
            ),
        );
        let mut builder = DescribeAclsRequestBuilder::new(&filter);
        let mut request = builder.build_version(3).unwrap();
        let bytes = request.serialize().unwrap().into_buffer();
        // int8 resource_type=2 (TOPIC)
        // compact-nullable string "t" (len+1=2), then
        // int8 pattern_type=3 (LITERAL)
        // compact-nullable string "User:x" (len+1=7)
        // compact-nullable string "*" (len+1=2)
        // int8 operation=3 (READ)
        // int8 permission=3 (ALLOW)
        // trailing empty tagged-fields byte 0x00
        let expected: Vec<u8> = vec![
            0x02, // ResourceTypeFilter = TOPIC
            0x02, b't', // ResourceNameFilter = "t"
            0x03, // PatternTypeFilter = LITERAL
            0x07, b'U', b's', b'e', b'r', b':', b'x', // PrincipalFilter = "User:x"
            0x02, b'*', // HostFilter = "*"
            0x03, // Operation = READ
            0x03, // PermissionType = ALLOW
            0x00, // empty tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
