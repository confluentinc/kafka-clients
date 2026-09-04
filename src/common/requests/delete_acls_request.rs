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

//! DeleteAcls request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteAclsRequest`.

use std::io;

use crate::common::acl::{AccessControlEntryFilter, AclBindingFilter, AclOperation, AclPermissionType};
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::resource::{PatternType, ResourcePatternFilter, ResourceType};
use crate::delete_acls_request_data::{DeleteAclsFilter, DeleteAclsRequestData};
use crate::delete_acls_response_data::{DeleteAclsFilterResult, DeleteAclsResponseData};

use super::{ConcreteRequest, ConcreteResponse, DeleteAclsResponse, RequestBuilder};

/// A DeleteAcls request.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteAclsRequest`.
#[derive(Debug, Clone)]
pub struct DeleteAclsRequest {
    data: DeleteAclsRequestData,
    version: i16,
}

impl DeleteAclsRequest {
    /// Creates a new `DeleteAclsRequest` from data and version.
    pub fn new(data: DeleteAclsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DeleteAclsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteAclsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_ACLS
    }

    /// Reconstructs the [`AclBindingFilter`]s from the wire data.
    ///
    /// Mirrors `DeleteAclsRequest.filters()`.
    pub fn filters(&self) -> Vec<AclBindingFilter> {
        self.data.filters.iter().map(Self::acl_binding_filter).collect()
    }

    /// Creates an error response for this request, failing every filter with the
    /// given error.
    ///
    /// Mirrors `DeleteAclsRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut filter_result = DeleteAclsFilterResult::new();
        filter_result.set_error_code(error.code());
        filter_result.set_error_message(Some(error.message().to_string()));
        let filter_results = vec![filter_result; self.data.filters.len()];
        let mut response = DeleteAclsResponseData::new();
        response.set_throttle_time_ms(throttle_time_ms);
        response.set_filter_results(filter_results);
        ConcreteResponse::DeleteAcls(DeleteAclsResponse::new(response, self.version))
    }

    /// Parses a `DeleteAclsRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteAclsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Builds a wire [`DeleteAclsFilter`] from an [`AclBindingFilter`].
    ///
    /// Mirrors `DeleteAclsRequest.deleteAclsFilter`.
    pub fn delete_acls_filter(filter: &AclBindingFilter) -> DeleteAclsFilter {
        let mut wire = DeleteAclsFilter::new();
        wire.set_resource_name_filter(filter.pattern_filter().name().map(str::to_string))
            .set_resource_type_filter(filter.pattern_filter().resource_type().code())
            .set_pattern_type_filter(filter.pattern_filter().pattern_type().code())
            .set_host_filter(filter.entry_filter().host().map(str::to_string))
            .set_operation(filter.entry_filter().operation().code())
            .set_permission_type(filter.entry_filter().permission_type().code())
            .set_principal_filter(filter.entry_filter().principal().map(str::to_string));
        wire
    }

    /// Reconstructs an [`AclBindingFilter`] from a wire [`DeleteAclsFilter`].
    ///
    /// Mirrors `DeleteAclsRequest.aclBindingFilter`.
    fn acl_binding_filter(filter: &DeleteAclsFilter) -> AclBindingFilter {
        let pattern_filter = ResourcePatternFilter::new(
            ResourceType::from_code(filter.resource_type_filter),
            filter.resource_name_filter.clone(),
            PatternType::from_code(filter.pattern_type_filter),
        );
        let entry_filter = AccessControlEntryFilter::new(
            filter.principal_filter.clone(),
            filter.host_filter.clone(),
            AclOperation::from_code(filter.operation),
            AclPermissionType::from_code(filter.permission_type),
        );
        AclBindingFilter::new(pattern_filter, entry_filter)
    }
}

impl std::fmt::Display for DeleteAclsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteAclsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`DeleteAclsRequest`].
///
/// Corresponds to `DeleteAclsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DeleteAclsRequestBuilder {
    data: DeleteAclsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DeleteAclsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: DeleteAclsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::DELETE_ACLS.oldest_version(),
            latest_allowed_version: ApiKeys::DELETE_ACLS.latest_version(),
        }
    }
}

impl RequestBuilder for DeleteAclsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_ACLS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Mirrors DeleteAclsRequest.normalizeAndValidate. Version 0 was removed
        // in Kafka 4.0 (valid versions 1-3), so the v0 pattern-type
        // normalization is unreachable; the UNKNOWN-elements guard remains.
        let unknown = self.data.filters.iter().any(|filter| {
            filter.pattern_type_filter == PatternType::Unknown.code()
                || filter.resource_type_filter == ResourceType::Unknown.code()
                || filter.operation == AclOperation::Unknown.code()
                || filter.permission_type == AclPermissionType::Unknown.code()
        });
        if unknown {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Filters contain UNKNOWN elements, filters: {:?}", self.data.filters),
            ));
        }
        Ok(ConcreteRequest::DeleteAcls(DeleteAclsRequest::new(self.data.clone(), version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_filter() -> AclBindingFilter {
        AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Any, None, PatternType::Literal),
            AccessControlEntryFilter::new(
                Some("User:ANONYMOUS".to_string()),
                None,
                AclOperation::Any,
                AclPermissionType::Any,
            ),
        )
    }

    #[test]
    fn delete_acls_filter_round_trips() {
        let wire = DeleteAclsRequest::delete_acls_filter(&sample_filter());
        assert_eq!(DeleteAclsRequest::acl_binding_filter(&wire), sample_filter());
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = DeleteAclsRequestData::new();
        data.set_filters(vec![DeleteAclsRequest::delete_acls_filter(&sample_filter())]);
        let version = ApiKeys::DELETE_ACLS.latest_version();
        let mut request = ConcreteRequest::DeleteAcls(DeleteAclsRequest::new(data, version));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::from_bytes(bytes.into_buffer());
        let parsed = DeleteAclsRequest::parse(&mut readable, version).unwrap();
        assert_eq!(parsed.filters(), vec![sample_filter()]);
    }

    #[test]
    fn get_error_response_fails_every_filter() {
        let mut data = DeleteAclsRequestData::new();
        data.set_filters(vec![
            DeleteAclsRequest::delete_acls_filter(&sample_filter()),
            DeleteAclsRequest::delete_acls_filter(&sample_filter()),
        ]);
        let request = DeleteAclsRequest::new(data, ApiKeys::DELETE_ACLS.latest_version());
        let ConcreteResponse::DeleteAcls(r) = request.get_error_response(0, &Errors::SecurityDisabled) else {
            panic!("expected DeleteAcls response");
        };
        assert_eq!(r.data().filter_results.len(), 2);
        for result in &r.data().filter_results {
            assert_eq!(result.error_code, Errors::SecurityDisabled.code());
        }
    }

    #[test]
    fn known_wire_vector() {
        // v3 (flexible) encoding for a single filter (ANY type, null name,
        // LITERAL, principal "U", null host, ANY op, ANY perm).
        let filter = AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Any, None, PatternType::Literal),
            AccessControlEntryFilter::new(Some("U".to_string()), None, AclOperation::Any, AclPermissionType::Any),
        );
        let mut data = DeleteAclsRequestData::new();
        data.set_filters(vec![DeleteAclsRequest::delete_acls_filter(&filter)]);
        let mut request = ConcreteRequest::DeleteAcls(DeleteAclsRequest::new(data, 3));
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x02, // Filters: compact array len+1 = 2 (one element)
            0x01, // ResourceTypeFilter = ANY
            0x00, // ResourceNameFilter = null (compact nullable string, 0)
            0x03, // PatternTypeFilter = LITERAL
            0x02, b'U', // PrincipalFilter = "U"
            0x00, // HostFilter = null
            0x01, // Operation = ANY
            0x01, // PermissionType = ANY
            0x00, // element tagged fields
            0x00, // top-level tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
