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

    // Mirrors `AclPermissionTypeTest.AclPermissionTypeTestInfo` in the Java client.
    struct Info {
        ty: AclPermissionType,
        code: i8,
        name: &'static str,
        unknown: bool,
    }

    // Mirrors `AclPermissionTypeTest.INFOS` in the Java client, in declaration order.
    const INFOS: [Info; 4] = [
        Info { ty: AclPermissionType::Unknown, code: 0, name: "unknown", unknown: true },
        Info { ty: AclPermissionType::Any, code: 1, name: "any", unknown: false },
        Info { ty: AclPermissionType::Deny, code: 2, name: "deny", unknown: false },
        Info { ty: AclPermissionType::Allow, code: 3, name: "allow", unknown: false },
    ];

    // Test-local mirror of Java's `AclPermissionType.values()` (declaration order).
    const VALUES: [AclPermissionType; 4] = [
        AclPermissionType::Unknown,
        AclPermissionType::Any,
        AclPermissionType::Deny,
        AclPermissionType::Allow,
    ];

    /// Mirrors `AclPermissionTypeTest.testIsUnknown`.
    #[test]
    fn test_is_unknown() {
        for info in &INFOS {
            assert_eq!(
                info.unknown,
                info.ty.is_unknown(),
                "{} was supposed to have unknown == {}",
                info.ty,
                info.unknown
            );
        }
    }

    /// Mirrors `AclPermissionTypeTest.testCode`.
    #[test]
    fn test_code() {
        assert_eq!(VALUES.len(), INFOS.len());
        for info in &INFOS {
            assert_eq!(
                info.code,
                info.ty.code(),
                "{} was supposed to have code == {}",
                info.ty,
                info.code
            );
            assert_eq!(
                info.ty,
                AclPermissionType::from_code(info.code),
                "AclPermissionType::from_code({}) was supposed to be {}",
                info.code,
                info.ty
            );
        }
        assert_eq!(AclPermissionType::Unknown, AclPermissionType::from_code(120));
    }

    /// Mirrors `AclPermissionTypeTest.testName`.
    #[test]
    fn test_name() {
        for info in &INFOS {
            assert_eq!(
                info.ty,
                AclPermissionType::from_string(info.name),
                "AclPermissionType::from_string({}) was supposed to be {}",
                info.name,
                info.ty
            );
        }
        assert_eq!(AclPermissionType::Unknown, AclPermissionType::from_string("something"));
    }

    /// Mirrors `AclPermissionTypeTest.testExhaustive`.
    #[test]
    fn test_exhaustive() {
        assert_eq!(INFOS.len(), VALUES.len());
        for (i, info) in INFOS.iter().enumerate() {
            assert_eq!(info.ty, VALUES[i]);
        }
    }
}
