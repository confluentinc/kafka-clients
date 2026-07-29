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

//! ACL bindings.
//!
//! Corresponds to `org.apache.kafka.common.acl.AclBinding`.

use crate::common::resource::ResourcePattern;

use super::{AccessControlEntry, AclBindingFilter};

/// Represents a binding between a resource pattern and an access control entry.
///
/// Corresponds to `org.apache.kafka.common.acl.AclBinding`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AclBinding {
    pattern: ResourcePattern,
    entry: AccessControlEntry,
}

impl AclBinding {
    /// Create an instance of this class with the provided parameters.
    ///
    /// # Arguments
    /// * `pattern` - resource pattern
    /// * `entry` - entry
    pub fn new(pattern: ResourcePattern, entry: AccessControlEntry) -> AclBinding {
        AclBinding { pattern, entry }
    }

    /// Return true if this binding has any UNKNOWN components.
    pub fn is_unknown(&self) -> bool {
        self.pattern.is_unknown() || self.entry.is_unknown()
    }

    /// Return the resource pattern for this binding.
    pub fn pattern(&self) -> &ResourcePattern {
        &self.pattern
    }

    /// Return the access control entry for this binding.
    pub fn entry(&self) -> &AccessControlEntry {
        &self.entry
    }

    /// Create a filter which matches only this `AclBinding`.
    pub fn to_filter(&self) -> AclBindingFilter {
        AclBindingFilter::new(self.pattern.to_filter(), self.entry.to_filter())
    }
}

impl std::fmt::Display for AclBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(pattern={}, entry={})", self.pattern, self.entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::acl::{AccessControlEntryFilter, AclBindingFilter, AclOperation, AclPermissionType};
    use crate::common::resource::{PatternType, ResourcePatternFilter, ResourceType};

    fn acl(
        rt: ResourceType,
        name: &str,
        pt: PatternType,
        principal: &str,
        host: &str,
        op: AclOperation,
        perm: AclPermissionType,
    ) -> AclBinding {
        AclBinding::new(
            ResourcePattern::new(rt, name, pt).unwrap(),
            AccessControlEntry::new(principal, host, op, perm).unwrap(),
        )
    }

    fn acl1() -> AclBinding {
        acl(
            ResourceType::Topic,
            "mytopic",
            PatternType::Literal,
            "User:ANONYMOUS",
            "",
            AclOperation::All,
            AclPermissionType::Allow,
        )
    }
    fn acl2() -> AclBinding {
        acl(
            ResourceType::Topic,
            "mytopic",
            PatternType::Literal,
            "User:*",
            "",
            AclOperation::Read,
            AclPermissionType::Allow,
        )
    }
    fn acl3() -> AclBinding {
        acl(
            ResourceType::Topic,
            "mytopic2",
            PatternType::Literal,
            "User:ANONYMOUS",
            "127.0.0.1",
            AclOperation::Read,
            AclPermissionType::Deny,
        )
    }
    fn unknown_acl() -> AclBinding {
        acl(
            ResourceType::Topic,
            "mytopic2",
            PatternType::Literal,
            "User:ANONYMOUS",
            "127.0.0.1",
            AclOperation::Unknown,
            AclPermissionType::Deny,
        )
    }

    fn any_anonymous() -> AclBindingFilter {
        AclBindingFilter::new(
            ResourcePatternFilter::any(),
            AccessControlEntryFilter::new(
                Some("User:ANONYMOUS".to_string()),
                None,
                AclOperation::Any,
                AclPermissionType::Any,
            ),
        )
    }
    fn any_deny() -> AclBindingFilter {
        AclBindingFilter::new(
            ResourcePatternFilter::any(),
            AccessControlEntryFilter::new(None, None, AclOperation::Any, AclPermissionType::Deny),
        )
    }
    fn any_mytopic() -> AclBindingFilter {
        AclBindingFilter::new(
            ResourcePatternFilter::new(ResourceType::Topic, Some("mytopic".to_string()), PatternType::Literal),
            AccessControlEntryFilter::new(None, None, AclOperation::Any, AclPermissionType::Any),
        )
    }

    #[test]
    fn test_matching() {
        assert_eq!(acl1(), acl1());
        let acl1_copy = acl(
            ResourceType::Topic,
            "mytopic",
            PatternType::Literal,
            "User:ANONYMOUS",
            "",
            AclOperation::All,
            AclPermissionType::Allow,
        );
        assert_eq!(acl1(), acl1_copy);
        assert_eq!(acl2(), acl2());
        assert_ne!(acl1(), acl2());
        assert!(AclBindingFilter::any().matches(&acl1()));
        assert!(AclBindingFilter::any().matches(&acl2()));
        assert!(AclBindingFilter::any().matches(&acl3()));
        assert_eq!(AclBindingFilter::any(), AclBindingFilter::any());
        assert!(any_anonymous().matches(&acl1()));
        assert!(!any_anonymous().matches(&acl2()));
        assert!(any_anonymous().matches(&acl3()));
        assert!(!any_deny().matches(&acl1()));
        assert!(!any_deny().matches(&acl2()));
        assert!(any_deny().matches(&acl3()));
        assert!(any_mytopic().matches(&acl1()));
        assert!(any_mytopic().matches(&acl2()));
        assert!(!any_mytopic().matches(&acl3()));
        assert!(any_anonymous().matches(&unknown_acl()));
        assert!(any_deny().matches(&unknown_acl()));
        assert_eq!(unknown_acl(), unknown_acl());
        assert!(!any_mytopic().matches(&unknown_acl()));
    }

    #[test]
    fn test_unknowns() {
        assert!(!acl1().is_unknown());
        assert!(!acl2().is_unknown());
        assert!(!acl3().is_unknown());
        assert!(!any_anonymous().is_unknown());
        assert!(!any_deny().is_unknown());
        assert!(!any_mytopic().is_unknown());
        assert!(unknown_acl().is_unknown());
    }

    #[test]
    fn test_matches_at_most_one() {
        assert!(acl1().to_filter().find_indefinite_field().is_none());
        assert!(acl2().to_filter().find_indefinite_field().is_none());
        assert!(acl3().to_filter().find_indefinite_field().is_none());
        assert!(!any_anonymous().matches_at_most_one());
        assert!(!any_deny().matches_at_most_one());
        assert!(!any_mytopic().matches_at_most_one());
    }

    #[test]
    fn should_not_throw_on_unknown_pattern_type() {
        AclBinding::new(
            ResourcePattern::new(ResourceType::Topic, "foo", PatternType::Unknown).unwrap(),
            acl1().entry().clone(),
        );
    }

    #[test]
    fn should_not_throw_on_unknown_resource_type() {
        AclBinding::new(
            ResourcePattern::new(ResourceType::Unknown, "foo", PatternType::Literal).unwrap(),
            acl1().entry().clone(),
        );
    }

    #[test]
    fn should_throw_on_match_pattern_type() {
        assert!(ResourcePattern::new(ResourceType::Topic, "foo", PatternType::Match).is_err());
    }

    #[test]
    fn should_throw_on_any_pattern_type() {
        assert!(ResourcePattern::new(ResourceType::Topic, "foo", PatternType::Any).is_err());
    }

    #[test]
    fn should_throw_on_any_resource_type() {
        assert!(ResourcePattern::new(ResourceType::Any, "foo", PatternType::Literal).is_err());
    }
}
