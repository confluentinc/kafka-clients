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

//! Access control entry filters.
//!
//! Corresponds to `org.apache.kafka.common.acl.AccessControlEntryFilter`.

use super::access_control_entry_data::AccessControlEntryData;
use super::{AccessControlEntry, AclOperation, AclPermissionType};

/// Represents a filter which matches access control entries.
///
/// Corresponds to `org.apache.kafka.common.acl.AccessControlEntryFilter`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AccessControlEntryFilter {
    data: AccessControlEntryData,
}

impl AccessControlEntryFilter {
    /// Create an instance of an access control entry filter with the provided
    /// parameters.
    ///
    /// # Arguments
    /// * `principal` - the principal or `None`
    /// * `host` - the host or `None`
    /// * `operation` - operation
    /// * `permission_type` - permission type
    pub fn new(
        principal: Option<String>,
        host: Option<String>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> AccessControlEntryFilter {
        AccessControlEntryFilter { data: AccessControlEntryData::new(principal, host, operation, permission_type) }
    }

    /// This is a non-public constructor used in `AccessControlEntry::to_filter`.
    pub(crate) fn from_data(data: AccessControlEntryData) -> AccessControlEntryFilter {
        AccessControlEntryFilter { data }
    }

    /// A filter which matches any access control entry.
    pub fn any() -> AccessControlEntryFilter {
        AccessControlEntryFilter::new(None, None, AclOperation::Any, AclPermissionType::Any)
    }

    /// Return the principal, or `None`.
    pub fn principal(&self) -> Option<&str> {
        self.data.principal()
    }

    /// Return the host, or `None`. The value `*` means any host.
    pub fn host(&self) -> Option<&str> {
        self.data.host()
    }

    /// Return the `AclOperation`.
    pub fn operation(&self) -> AclOperation {
        self.data.operation()
    }

    /// Return the `AclPermissionType`.
    pub fn permission_type(&self) -> AclPermissionType {
        self.data.permission_type()
    }

    /// Return true if there are any UNKNOWN components.
    pub fn is_unknown(&self) -> bool {
        self.data.is_unknown()
    }

    /// Returns true if this filter matches the given `AccessControlEntry`.
    pub fn matches(&self, other: &AccessControlEntry) -> bool {
        if let Some(principal) = self.principal()
            && principal != other.principal()
        {
            return false;
        }
        if let Some(host) = self.host()
            && host != other.host()
        {
            return false;
        }
        if self.operation() != AclOperation::Any && self.operation() != other.operation() {
            return false;
        }
        self.permission_type() == AclPermissionType::Any || self.permission_type() == other.permission_type()
    }

    /// Returns true if this filter could only match one ACE -- in other words,
    /// if there are no ANY or UNKNOWN fields.
    pub fn matches_at_most_one(&self) -> bool {
        self.find_indefinite_field().is_none()
    }

    /// Returns a string describing an ANY or UNKNOWN field, or `None` if there
    /// is no such field.
    pub fn find_indefinite_field(&self) -> Option<String> {
        self.data.find_indefinite_field()
    }
}

impl std::fmt::Display for AccessControlEntryFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(principal: &str, host: &str, op: AclOperation, pt: AclPermissionType) -> AccessControlEntry {
        AccessControlEntry::new(principal, host, op, pt).unwrap()
    }

    #[test]
    fn any_matches_everything() {
        let filter = AccessControlEntryFilter::any();
        assert!(filter.matches(&entry("User:x", "*", AclOperation::Read, AclPermissionType::Allow)));
        assert!(!filter.matches_at_most_one());
    }

    #[test]
    fn principal_and_permission_filter() {
        let filter =
            AccessControlEntryFilter::new(Some("User:x".to_string()), None, AclOperation::Any, AclPermissionType::Deny);
        assert!(filter.matches(&entry("User:x", "*", AclOperation::Read, AclPermissionType::Deny)));
        assert!(!filter.matches(&entry("User:y", "*", AclOperation::Read, AclPermissionType::Deny)));
        assert!(!filter.matches(&entry("User:x", "*", AclOperation::Read, AclPermissionType::Allow)));
    }
}
