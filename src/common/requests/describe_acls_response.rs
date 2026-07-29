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

//! DescribeAcls response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeAclsResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::KafkaError;
use crate::common::acl::{AccessControlEntry, AclBinding, AclOperation, AclPermissionType};
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::resource::{PatternType, ResourcePattern, ResourceType};
use crate::describe_acls_response_data::{AclDescription, DescribeAclsResource, DescribeAclsResponseData};

use super::abstract_response::update_error_counts;

/// A DescribeAcls response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeAclsResponse`.
#[derive(Debug, Clone)]
pub struct DescribeAclsResponse {
    data: DescribeAclsResponseData,
    #[allow(dead_code)]
    version: i16,
}

impl DescribeAclsResponse {
    /// Creates a new `DescribeAclsResponse` from the underlying data and
    /// version.
    pub fn new(data: DescribeAclsResponseData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_ACLS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeAclsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeAclsResponseData {
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

    /// Returns the response error code.
    pub fn error_code(&self) -> i16 {
        self.data.error_code
    }

    /// Returns the response error message, if any.
    pub fn error_message(&self) -> Option<&str> {
        self.data.error_message.as_deref()
    }

    /// Returns the resources referenced in the response.
    ///
    /// Mirrors `DescribeAclsResponse.acls()`.
    pub fn acls(&self) -> &[DescribeAclsResource] {
        &self.data.resources
    }

    /// Returns the error counts aggregated for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        counts
    }

    /// Parses a `DescribeAclsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeAclsResponseData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Whether the client should throttle on this response (v1+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }

    /// Flattens the described resources into a list of [`AclBinding`]s.
    ///
    /// Mirrors `DescribeAclsResponse.aclBindings(List<DescribeAclsResource>)`.
    ///
    /// # Errors
    ///
    /// Returns an error if a resource carries pattern/permission combinations
    /// that are invalid for an [`AclBinding`] (mirrors the exceptions Java's
    /// `ResourcePattern`/`AccessControlEntry` constructors would throw).
    pub fn acl_bindings(resources: &[DescribeAclsResource]) -> Result<Vec<AclBinding>, KafkaError> {
        let mut bindings = Vec::new();
        for resource in resources {
            for acl in &resource.acls {
                let pattern = ResourcePattern::new(
                    ResourceType::from_code(resource.resource_type),
                    resource.resource_name.clone(),
                    PatternType::from_code(resource.pattern_type),
                )?;
                let entry = AccessControlEntry::new(
                    acl.principal.clone(),
                    acl.host.clone(),
                    AclOperation::from_code(acl.operation),
                    AclPermissionType::from_code(acl.permission_type),
                )?;
                bindings.push(AclBinding::new(pattern, entry));
            }
        }
        Ok(bindings)
    }

    /// Groups a set of [`AclBinding`]s into wire resources.
    ///
    /// Mirrors `DescribeAclsResponse.aclsResources(Iterable<AclBinding>)`.
    pub fn acls_resources(acls: &[AclBinding]) -> Vec<DescribeAclsResource> {
        // Preserve insertion order of first-seen patterns, mirroring Java's
        // per-pattern grouping (HashMap iteration order is unspecified in Java,
        // so tests must not depend on ordering).
        let mut order: Vec<ResourcePattern> = Vec::new();
        let mut pattern_to_entries: HashMap<ResourcePattern, Vec<AccessControlEntry>> = HashMap::new();
        for acl in acls {
            let entries = pattern_to_entries.entry(acl.pattern().clone()).or_insert_with(|| {
                order.push(acl.pattern().clone());
                Vec::new()
            });
            if !entries.contains(acl.entry()) {
                entries.push(acl.entry().clone());
            }
        }
        let mut resources = Vec::with_capacity(order.len());
        for pattern in order {
            let entries = &pattern_to_entries[&pattern];
            let mut acl_descriptions = Vec::with_capacity(entries.len());
            for ace in entries {
                let mut ad = AclDescription::new();
                ad.set_host(ace.host().to_string())
                    .set_operation(ace.operation().code())
                    .set_permission_type(ace.permission_type().code())
                    .set_principal(ace.principal().to_string());
                acl_descriptions.push(ad);
            }
            let mut dar = DescribeAclsResource::new();
            dar.set_resource_name(pattern.name().to_string())
                .set_pattern_type(pattern.pattern_type().code())
                .set_resource_type(pattern.resource_type().code())
                .set_acls(acl_descriptions);
            resources.push(dar);
        }
        resources
    }
}

impl std::fmt::Display for DescribeAclsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeAclsResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::acl::{AccessControlEntry, AclBinding};
    use crate::common::resource::ResourcePattern;

    fn acl(name: &str, principal: &str, host: &str, op: AclOperation, perm: AclPermissionType) -> AclBinding {
        AclBinding::new(
            ResourcePattern::new(ResourceType::Topic, name, PatternType::Literal).unwrap(),
            AccessControlEntry::new(principal, host, op, perm).unwrap(),
        )
    }

    #[test]
    fn acls_resources_round_trips_to_bindings() {
        let acl1 = acl("mytopic3", "User:ANONYMOUS", "*", AclOperation::Describe, AclPermissionType::Allow);
        let acl2 = acl("mytopic4", "User:ANONYMOUS", "*", AclOperation::Describe, AclPermissionType::Deny);
        let resources = DescribeAclsResponse::acls_resources(&[acl1.clone(), acl2.clone()]);
        let mut bindings = DescribeAclsResponse::acl_bindings(&resources).unwrap();
        bindings.sort_by(|a, b| a.pattern().name().cmp(b.pattern().name()));
        assert_eq!(bindings, vec![acl1, acl2]);
    }

    #[test]
    fn empty_response_has_no_bindings() {
        let data = DescribeAclsResponseData::new();
        let response = DescribeAclsResponse::new(data, 3);
        assert!(DescribeAclsResponse::acl_bindings(response.acls()).unwrap().is_empty());
    }

    #[test]
    fn should_client_throttle_v1_threshold() {
        let response = DescribeAclsResponse::new(DescribeAclsResponseData::new(), 3);
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
    }
}
