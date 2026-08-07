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

//! Internal ACE data storage.
//!
//! Corresponds to `org.apache.kafka.common.acl.AccessControlEntryData`, an
//! internal, package-private class shared by [`AccessControlEntry`] and
//! [`AccessControlEntryFilter`]; hence `pub(crate)`.
//!
//! [`AccessControlEntry`]: super::AccessControlEntry
//! [`AccessControlEntryFilter`]: super::AccessControlEntryFilter

use super::{AclOperation, AclPermissionType};

/// An internal, private class which contains the data stored in
/// `AccessControlEntry` and `AccessControlEntryFilter` objects.
///
/// `principal` and `host` are `None` when they act as wildcards in a filter.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AccessControlEntryData {
    principal: Option<String>,
    host: Option<String>,
    operation: AclOperation,
    permission_type: AclPermissionType,
}

impl AccessControlEntryData {
    pub(crate) fn new(
        principal: Option<String>,
        host: Option<String>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> AccessControlEntryData {
        AccessControlEntryData { principal, host, operation, permission_type }
    }

    pub(crate) fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    pub(crate) fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    pub(crate) fn operation(&self) -> AclOperation {
        self.operation
    }

    pub(crate) fn permission_type(&self) -> AclPermissionType {
        self.permission_type
    }

    /// Returns a string describing an ANY or UNKNOWN field, or `None` if there
    /// is no such field.
    pub(crate) fn find_indefinite_field(&self) -> Option<String> {
        if self.principal.is_none() {
            return Some("Principal is NULL".to_string());
        }
        if self.host.is_none() {
            return Some("Host is NULL".to_string());
        }
        if self.operation == AclOperation::Any {
            return Some("Operation is ANY".to_string());
        }
        if self.operation == AclOperation::Unknown {
            return Some("Operation is UNKNOWN".to_string());
        }
        if self.permission_type == AclPermissionType::Any {
            return Some("Permission type is ANY".to_string());
        }
        if self.permission_type == AclPermissionType::Unknown {
            return Some("Permission type is UNKNOWN".to_string());
        }
        None
    }

    /// Return true if there are any UNKNOWN components.
    pub(crate) fn is_unknown(&self) -> bool {
        self.operation.is_unknown() || self.permission_type.is_unknown()
    }
}

impl std::fmt::Display for AccessControlEntryData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(principal={}, host={}, operation={}, permissionType={})",
            self.principal.as_deref().unwrap_or("<any>"),
            self.host.as_deref().unwrap_or("<any>"),
            self.operation,
            self.permission_type
        )
    }
}
