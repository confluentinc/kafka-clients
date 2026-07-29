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

//! ACL permission types.
//!
//! Corresponds to `org.apache.kafka.common.acl.AclPermissionType`.

/// Represents whether an ACL grants or denies permissions.
///
/// Corresponds to `org.apache.kafka.common.acl.AclPermissionType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AclPermissionType {
    /// Represents any `AclPermissionType` which this client cannot understand,
    /// perhaps because this client is too old.
    Unknown,
    /// In a filter, matches any `AclPermissionType`.
    Any,
    /// Disallows access.
    Deny,
    /// Grants access.
    Allow,
}

impl AclPermissionType {
    /// Return the code of this permission type.
    pub fn code(&self) -> i8 {
        match self {
            AclPermissionType::Unknown => 0,
            AclPermissionType::Any => 1,
            AclPermissionType::Deny => 2,
            AclPermissionType::Allow => 3,
        }
    }

    /// Return the `AclPermissionType` with the provided code or
    /// [`AclPermissionType::Unknown`] if one cannot be found.
    pub fn from_code(code: i8) -> AclPermissionType {
        match code {
            0 => AclPermissionType::Unknown,
            1 => AclPermissionType::Any,
            2 => AclPermissionType::Deny,
            3 => AclPermissionType::Allow,
            _ => AclPermissionType::Unknown,
        }
    }

    /// Parse the given string as an ACL permission.
    ///
    /// Returns the `AclPermissionType`, or [`AclPermissionType::Unknown`] if the
    /// string could not be matched (case-insensitive).
    pub fn from_string(str: &str) -> AclPermissionType {
        match str.to_uppercase().as_str() {
            "UNKNOWN" => AclPermissionType::Unknown,
            "ANY" => AclPermissionType::Any,
            "DENY" => AclPermissionType::Deny,
            "ALLOW" => AclPermissionType::Allow,
            _ => AclPermissionType::Unknown,
        }
    }

    /// Return true if this permission type is [`AclPermissionType::Unknown`].
    pub fn is_unknown(&self) -> bool {
        *self == AclPermissionType::Unknown
    }
}

impl std::fmt::Display for AclPermissionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            AclPermissionType::Unknown => "UNKNOWN",
            AclPermissionType::Any => "ANY",
            AclPermissionType::Deny => "DENY",
            AclPermissionType::Allow => "ALLOW",
        };
        write!(f, "{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_round_trips_for_all_variants() {
        for pt in [
            AclPermissionType::Unknown,
            AclPermissionType::Any,
            AclPermissionType::Deny,
            AclPermissionType::Allow,
        ] {
            assert_eq!(AclPermissionType::from_code(pt.code()), pt);
        }
    }

    #[test]
    fn code_values_match_java_wire_values() {
        assert_eq!(AclPermissionType::Unknown.code(), 0);
        assert_eq!(AclPermissionType::Any.code(), 1);
        assert_eq!(AclPermissionType::Deny.code(), 2);
        assert_eq!(AclPermissionType::Allow.code(), 3);
    }

    #[test]
    fn from_string_is_case_insensitive_and_defaults_to_unknown() {
        assert_eq!(AclPermissionType::from_string("allow"), AclPermissionType::Allow);
        assert_eq!(AclPermissionType::from_string("DENY"), AclPermissionType::Deny);
        assert_eq!(AclPermissionType::from_string("bogus"), AclPermissionType::Unknown);
    }

    #[test]
    fn is_unknown() {
        assert!(AclPermissionType::Unknown.is_unknown());
        assert!(!AclPermissionType::Allow.is_unknown());
    }
}
