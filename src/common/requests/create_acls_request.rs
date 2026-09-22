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

//! CreateAcls request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.CreateAclsRequest`.

use std::io;

use crate::CreateAclsRequestData;
use crate::CreateAclsResponseData;
use crate::common::Error;
use crate::common::acl::{AccessControlEntry, AclBinding, AclOperation, AclPermissionType};
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::resource::{PatternType, ResourcePattern, ResourceType};
use crate::create_acls_request_data::AclCreation;
use crate::create_acls_response_data::AclCreationResult;

use super::{ConcreteRequest, ConcreteResponse, CreateAclsResponse, RequestBuilder};

/// A CreateAcls request.
///
/// Corresponds to `org.apache.kafka.common.requests.CreateAclsRequest`.
#[derive(Debug, Clone)]
pub struct CreateAclsRequest {
    data: CreateAclsRequestData,
    version: i16,
}

impl CreateAclsRequest {
    /// Creates a new `CreateAclsRequest` from data and version.
    pub fn new(data: CreateAclsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &CreateAclsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut CreateAclsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_ACLS
    }

    /// Returns the ACL creations in this request.
    pub fn acl_creations(&self) -> &[AclCreation] {
        &self.data.creations
    }

    /// Creates an error response for this request, failing every creation with
    /// the given error.
    ///
    /// Mirrors `CreateAclsRequest.getErrorResponse`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut result = AclCreationResult::new();
        result.set_error_code(error.code());
        result.set_error_message(Some(error.message().to_string()));
        let results = vec![result; self.data.creations.len()];
        let mut response = CreateAclsResponseData::new();
        response.set_throttle_time_ms(throttle_time_ms);
        response.set_results(results);
        ConcreteResponse::CreateAcls(CreateAclsResponse::new(response))
    }

    /// Parses a `CreateAclsRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = CreateAclsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Reconstructs an [`AclBinding`] from a wire [`AclCreation`].
    ///
    /// Mirrors `CreateAclsRequest.aclBinding`.
    ///
    /// # Errors
    ///
    /// Returns an error if the creation carries invalid pattern/permission
    /// components (mirrors Java's constructor exceptions).
    pub fn acl_binding(acl: &AclCreation) -> Result<AclBinding, Error> {
        let pattern = ResourcePattern::new(
            ResourceType::from_code(acl.resource_type),
            acl.resource_name.clone(),
            PatternType::from_code(acl.resource_pattern_type),
        )?;
        let entry = AccessControlEntry::new(
            acl.principal.clone(),
            acl.host.clone(),
            AclOperation::from_code(acl.operation),
            AclPermissionType::from_code(acl.permission_type),
        )?;
        Ok(AclBinding::new(pattern, entry))
    }

    /// Builds a wire [`AclCreation`] from an [`AclBinding`].
    ///
    /// Mirrors `CreateAclsRequest.aclCreation`.
    pub fn acl_creation(binding: &AclBinding) -> AclCreation {
        let mut creation = AclCreation::new();
        creation
            .set_host(binding.entry().host().to_string())
            .set_operation(binding.entry().operation().code())
            .set_permission_type(binding.entry().permission_type().code())
            .set_principal(binding.entry().principal().to_string())
            .set_resource_name(binding.pattern().name().to_string())
            .set_resource_type(binding.pattern().resource_type().code())
            .set_resource_pattern_type(binding.pattern().pattern_type().code());
        creation
    }
}

impl std::fmt::Display for CreateAclsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CreateAclsRequest(version={}, data={:?})", self.version, self.data)
    }
}

/// Builder for [`CreateAclsRequest`].
///
/// Corresponds to `CreateAclsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct CreateAclsRequestBuilder {
    data: CreateAclsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl CreateAclsRequestBuilder {
    /// Creates a builder from existing data.
    pub fn new(data: CreateAclsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::CREATE_ACLS.oldest_version(),
            latest_allowed_version: ApiKeys::CREATE_ACLS.latest_version(),
        }
    }
}

impl RequestBuilder for CreateAclsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CREATE_ACLS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Mirrors CreateAclsRequest.validate. Version 0 was removed in Kafka 4.0
        // (valid versions 1-3), so the v0 pattern-type guard is unreachable; the
        // UNKNOWN-elements guard remains.
        let unknown = self.data.creations.iter().any(|creation| {
            creation.resource_pattern_type == PatternType::Unknown.code()
                || creation.resource_type == ResourceType::Unknown.code()
                || creation.permission_type == AclPermissionType::Unknown.code()
                || creation.operation == AclOperation::Unknown.code()
        });
        if unknown {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("CreatableAcls contain unknown elements: {:?}", self.data.creations),
            ));
        }
        Ok(ConcreteRequest::CreateAcls(CreateAclsRequest::new(self.data.clone(), version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> AclBinding {
        AclBinding::new(
            ResourcePattern::new(ResourceType::Topic, "mytopic3", PatternType::Literal).unwrap(),
            AccessControlEntry::new("User:ANONYMOUS", "*", AclOperation::Describe, AclPermissionType::Allow).unwrap(),
        )
    }

    #[test]
    fn acl_creation_round_trips_to_binding() {
        let creation = CreateAclsRequest::acl_creation(&binding());
        assert_eq!(CreateAclsRequest::acl_binding(&creation).unwrap(), binding());
    }

    #[test]
    fn serialize_parse_round_trip() {
        let mut data = CreateAclsRequestData::new();
        data.set_creations(vec![CreateAclsRequest::acl_creation(&binding())]);
        let version = ApiKeys::CREATE_ACLS.latest_version();
        let mut request = ConcreteRequest::CreateAcls(CreateAclsRequest::new(data, version));
        let bytes = request.serialize().unwrap();
        let mut readable = crate::common::ByteBufferAccessor::new(bytes.into_buffer());
        let parsed = CreateAclsRequest::parse(&mut readable, version).unwrap();
        assert_eq!(parsed.acl_creations().len(), 1);
        assert_eq!(CreateAclsRequest::acl_binding(&parsed.acl_creations()[0]).unwrap(), binding());
    }

    #[test]
    fn get_error_response_fails_every_creation() {
        let mut data = CreateAclsRequestData::new();
        data.set_creations(vec![
            CreateAclsRequest::acl_creation(&binding()),
            CreateAclsRequest::acl_creation(&binding()),
        ]);
        let request = CreateAclsRequest::new(data, ApiKeys::CREATE_ACLS.latest_version());
        let ConcreteResponse::CreateAcls(r) = request.get_error_response(0, &Errors::SecurityDisabled) else {
            panic!("expected CreateAcls response");
        };
        assert_eq!(r.data().results.len(), 2);
        for result in &r.data().results {
            assert_eq!(result.error_code, Errors::SecurityDisabled.code());
        }
    }

    #[test]
    fn known_wire_vector() {
        // v3 (flexible) encoding for a single creation (TOPIC="t" LITERAL,
        // principal "U", host "*", READ, ALLOW).
        let acl = AclBinding::new(
            ResourcePattern::new(ResourceType::Topic, "t", PatternType::Literal).unwrap(),
            AccessControlEntry::new("U", "*", AclOperation::Read, AclPermissionType::Allow).unwrap(),
        );
        let mut data = CreateAclsRequestData::new();
        data.set_creations(vec![CreateAclsRequest::acl_creation(&acl)]);
        let mut request = ConcreteRequest::CreateAcls(CreateAclsRequest::new(data, 3));
        let bytes = request.serialize().unwrap().into_buffer();
        let expected: Vec<u8> = vec![
            0x02, // Creations: compact array len+1 = 2 (one element)
            0x02, // ResourceType = TOPIC
            0x02, b't', // ResourceName = "t"
            0x03, // ResourcePatternType = LITERAL
            0x02, b'U', // Principal = "U"
            0x02, b'*', // Host = "*"
            0x03, // Operation = READ
            0x03, // PermissionType = ALLOW
            0x00, // element tagged fields
            0x00, // top-level tagged fields
        ];
        assert_eq!(bytes.as_slice(), expected.as_slice());
    }
}
