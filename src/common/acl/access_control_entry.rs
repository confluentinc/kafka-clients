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

//! Access control entries.
//!
//! Corresponds to `org.apache.kafka.common.acl.AccessControlEntry`.

use crate::common::Error;

use super::access_control_entry_data::AccessControlEntryData;
use super::{AccessControlEntryFilter, AclOperation, AclPermissionType};

/// Represents an access control entry. ACEs are a tuple of principal, host,
/// operation, and permissionType.
///
/// Corresponds to `org.apache.kafka.common.acl.AccessControlEntry`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AccessControlEntry {
    pub(crate) data: AccessControlEntryData,
}

impl AccessControlEntry {
    /// Create an instance of an access control entry with the provided
    /// parameters.
    ///
    /// # Arguments
    /// * `principal` - principal
    /// * `host` - host
    /// * `operation` - operation, `ANY` is not an allowed operation
    /// * `permission_type` - permission type, `ANY` is not an allowed type
    ///
    /// # Errors
    ///
    /// Returns [`Error::IllegalArgument`] if `operation` is
    /// [`AclOperation::Any`] or `permission_type` is
    /// [`AclPermissionType::Any`] (mirrors Java's `IllegalArgumentException`).
    pub fn new(
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> Result<AccessControlEntry, Error> {
        if operation == AclOperation::Any {
            return Err(Error::illegal_argument("operation must not be ANY"));
        }
        if permission_type == AclPermissionType::Any {
            return Err(Error::illegal_argument("permissionType must not be ANY"));
        }
        Ok(AccessControlEntry {
            data: AccessControlEntryData::new(Some(principal.into()), Some(host.into()), operation, permission_type),
        })
    }

    /// Return the principal for this entry.
    pub fn principal(&self) -> &str {
        // Invariant: a public `AccessControlEntry` always has a non-null
        // principal (set by `new`).
        self.data.principal().expect("AccessControlEntry principal is always set")
    }

    /// Return the host, or `*` for all hosts.
    pub fn host(&self) -> &str {
        self.data.host().expect("AccessControlEntry host is always set")
    }

    /// Return the `AclOperation`. This method will never return
    /// [`AclOperation::Any`].
    pub fn operation(&self) -> AclOperation {
        self.data.operation()
    }

    /// Return the `AclPermissionType`. This method will never return
    /// [`AclPermissionType::Any`].
    pub fn permission_type(&self) -> AclPermissionType {
        self.data.permission_type()
    }

    /// Create a filter which matches only this `AccessControlEntry`.
    pub fn to_filter(&self) -> AccessControlEntryFilter {
        AccessControlEntryFilter::from_data(self.data.clone())
    }

    /// Return true if this ACE has any UNKNOWN components.
    pub fn is_unknown(&self) -> bool {
        self.data.is_unknown()
    }
}

impl std::fmt::Display for AccessControlEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_any_operation() {
        let err = AccessControlEntry::new("User:x", "*", AclOperation::Any, AclPermissionType::Allow);
        assert!(matches!(err, Err(Error::IllegalArgument(_))));
    }

    #[test]
    fn rejects_any_permission_type() {
        let err = AccessControlEntry::new("User:x", "*", AclOperation::Read, AclPermissionType::Any);
        assert!(matches!(err, Err(Error::IllegalArgument(_))));
    }

    #[test]
    fn accessors() {
        let entry = AccessControlEntry::new("User:x", "host", AclOperation::Read, AclPermissionType::Allow).unwrap();
        assert_eq!(entry.principal(), "User:x");
        assert_eq!(entry.host(), "host");
        assert_eq!(entry.operation(), AclOperation::Read);
        assert_eq!(entry.permission_type(), AclPermissionType::Allow);
        assert!(!entry.is_unknown());
    }

    #[test]
    fn is_unknown_for_unknown_operation() {
        let entry = AccessControlEntry::new("User:x", "host", AclOperation::Unknown, AclPermissionType::Allow).unwrap();
        assert!(entry.is_unknown());
    }
}
