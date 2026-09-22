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

//! DeleteAcls response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteAclsResponse`.

use std::collections::HashMap;
use std::io;

use crate::DeleteAclsResponseData;
use crate::common::Error;
use crate::common::acl::{AccessControlEntry, AclBinding, AclOperation, AclPermissionType};
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::resource::{PatternType, ResourcePattern, ResourceType};
use crate::delete_acls_response_data::{DeleteAclsFilterResult, DeleteAclsMatchingAcl};

use super::AbstractResponse;

/// A DeleteAcls response.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteAclsResponse`.
#[derive(Debug, Clone)]
pub struct DeleteAclsResponse {
    data: DeleteAclsResponseData,
    #[allow(dead_code)]
    version: i16,
}

impl DeleteAclsResponse {
    /// Creates a new `DeleteAclsResponse` from the underlying data and version.
    pub fn new(data: DeleteAclsResponseData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_ACLS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DeleteAclsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteAclsResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns the per-filter results.
    ///
    /// Mirrors `DeleteAclsResponse.filterResults()`.
    pub fn filter_results(&self) -> &[DeleteAclsFilterResult] {
        &self.data.filter_results
    }

    /// Returns the error counts aggregated across all filter results.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for result in &self.data.filter_results {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(result.error_code));
        }
        counts
    }

    /// Parses a `DeleteAclsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteAclsResponseData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }

    /// Builds a wire [`DeleteAclsMatchingAcl`] from a binding and error.
    ///
    /// Mirrors `DeleteAclsResponse.matchingAcl(AclBinding, ApiError)`.
    pub fn matching_acl(acl: &AclBinding, error: Errors, error_message: Option<String>) -> DeleteAclsMatchingAcl {
        let mut wire = DeleteAclsMatchingAcl::new();
        wire.set_error_code(error.code())
            .set_error_message(error_message)
            .set_resource_name(acl.pattern().name().to_string())
            .set_resource_type(acl.pattern().resource_type().code())
            .set_pattern_type(acl.pattern().pattern_type().code())
            .set_host(acl.entry().host().to_string())
            .set_operation(acl.entry().operation().code())
            .set_permission_type(acl.entry().permission_type().code())
            .set_principal(acl.entry().principal().to_string());
        wire
    }

    /// Reconstructs an [`AclBinding`] from a wire [`DeleteAclsMatchingAcl`].
    ///
    /// Mirrors `DeleteAclsResponse.aclBinding`.
    ///
    /// # Errors
    ///
    /// Returns an error if the matching ACL carries invalid pattern/permission
    /// components (mirrors Java's constructor exceptions).
    pub fn acl_binding(matching_acl: &DeleteAclsMatchingAcl) -> Result<AclBinding, Error> {
        let pattern = ResourcePattern::new(
            ResourceType::from_code(matching_acl.resource_type),
            matching_acl.resource_name.clone(),
            PatternType::from_code(matching_acl.pattern_type),
        )?;
        let entry = AccessControlEntry::new(
            matching_acl.principal.clone(),
            matching_acl.host.clone(),
            AclOperation::from_code(matching_acl.operation),
            AclPermissionType::from_code(matching_acl.permission_type),
        )?;
        Ok(AclBinding::new(pattern, entry))
    }
}

impl std::fmt::Display for DeleteAclsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteAclsResponse(data={:?})", self.data)
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
    fn matching_acl_round_trips_to_binding() {
        let matching = DeleteAclsResponse::matching_acl(&binding(), Errors::None, None);
        assert_eq!(DeleteAclsResponse::acl_binding(&matching).unwrap(), binding());
    }

    #[test]
    fn error_counts_aggregates_across_filters() {
        let mut ok = DeleteAclsFilterResult::new();
        ok.set_error_code(Errors::None.code());
        let mut bad = DeleteAclsFilterResult::new();
        bad.set_error_code(Errors::SecurityDisabled.code());
        let mut data = DeleteAclsResponseData::new();
        data.set_filter_results(vec![ok, bad]);
        let response = DeleteAclsResponse::new(data, 3);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.get(&Errors::SecurityDisabled), Some(&1));
    }

    #[test]
    fn should_client_throttle_v1_threshold() {
        let response = DeleteAclsResponse::new(DeleteAclsResponseData::new(), 3);
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }
}
