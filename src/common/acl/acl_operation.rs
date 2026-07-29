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

//! ACL operations.
//!
//! Corresponds to `org.apache.kafka.common.acl.AclOperation`.

/// Represents an operation which an ACL grants or denies permission to perform.
///
/// Some operations imply other operations:
/// - `ALLOW ALL` implies `ALLOW` everything
/// - `DENY ALL` implies `DENY` everything
/// - `ALLOW READ` implies `ALLOW DESCRIBE`
/// - `ALLOW WRITE` implies `ALLOW DESCRIBE`
/// - `ALLOW DELETE` implies `ALLOW DESCRIBE`
/// - `ALLOW ALTER` implies `ALLOW DESCRIBE`
/// - `ALLOW ALTER_CONFIGS` implies `ALLOW DESCRIBE_CONFIGS`
///
/// Corresponds to `org.apache.kafka.common.acl.AclOperation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AclOperation {
    /// Represents any `AclOperation` which this client cannot understand,
    /// perhaps because this client is too old.
    Unknown,
    /// In a filter, matches any `AclOperation`.
    Any,
    /// `ALL` operation.
    All,
    /// `READ` operation.
    Read,
    /// `WRITE` operation.
    Write,
    /// `CREATE` operation.
    Create,
    /// `DELETE` operation.
    Delete,
    /// `ALTER` operation.
    Alter,
    /// `DESCRIBE` operation.
    Describe,
    /// `CLUSTER_ACTION` operation.
    ClusterAction,
    /// `DESCRIBE_CONFIGS` operation.
    DescribeConfigs,
    /// `ALTER_CONFIGS` operation.
    AlterConfigs,
    /// `IDEMPOTENT_WRITE` operation.
    IdempotentWrite,
    /// `CREATE_TOKENS` operation.
    CreateTokens,
    /// `DESCRIBE_TOKENS` operation.
    DescribeTokens,
    /// `TWO_PHASE_COMMIT` operation.
    TwoPhaseCommit,
}

// Note: we cannot have more than 30 ACL operations without modifying the format
// used to describe ACL operations in MetadataResponse.

impl AclOperation {
    /// Return the code of this operation.
    pub fn code(&self) -> i8 {
        match self {
            AclOperation::Unknown => 0,
            AclOperation::Any => 1,
            AclOperation::All => 2,
            AclOperation::Read => 3,
            AclOperation::Write => 4,
            AclOperation::Create => 5,
            AclOperation::Delete => 6,
            AclOperation::Alter => 7,
            AclOperation::Describe => 8,
            AclOperation::ClusterAction => 9,
            AclOperation::DescribeConfigs => 10,
            AclOperation::AlterConfigs => 11,
            AclOperation::IdempotentWrite => 12,
            AclOperation::CreateTokens => 13,
            AclOperation::DescribeTokens => 14,
            AclOperation::TwoPhaseCommit => 15,
        }
    }

    /// Return the `AclOperation` with the provided code or [`AclOperation::Unknown`]
    /// if one cannot be found.
    pub fn from_code(code: i8) -> AclOperation {
        match code {
            0 => AclOperation::Unknown,
            1 => AclOperation::Any,
            2 => AclOperation::All,
            3 => AclOperation::Read,
            4 => AclOperation::Write,
            5 => AclOperation::Create,
            6 => AclOperation::Delete,
            7 => AclOperation::Alter,
            8 => AclOperation::Describe,
            9 => AclOperation::ClusterAction,
            10 => AclOperation::DescribeConfigs,
            11 => AclOperation::AlterConfigs,
            12 => AclOperation::IdempotentWrite,
            13 => AclOperation::CreateTokens,
            14 => AclOperation::DescribeTokens,
            15 => AclOperation::TwoPhaseCommit,
            _ => AclOperation::Unknown,
        }
    }

    /// Parse the given string as an ACL operation.
    ///
    /// Returns the `AclOperation`, or [`AclOperation::Unknown`] if the string
    /// could not be matched (case-insensitive).
    pub fn from_string(str: &str) -> AclOperation {
        match str.to_uppercase().as_str() {
            "UNKNOWN" => AclOperation::Unknown,
            "ANY" => AclOperation::Any,
            "ALL" => AclOperation::All,
            "READ" => AclOperation::Read,
            "WRITE" => AclOperation::Write,
            "CREATE" => AclOperation::Create,
            "DELETE" => AclOperation::Delete,
            "ALTER" => AclOperation::Alter,
            "DESCRIBE" => AclOperation::Describe,
            "CLUSTER_ACTION" => AclOperation::ClusterAction,
            "DESCRIBE_CONFIGS" => AclOperation::DescribeConfigs,
            "ALTER_CONFIGS" => AclOperation::AlterConfigs,
            "IDEMPOTENT_WRITE" => AclOperation::IdempotentWrite,
            "CREATE_TOKENS" => AclOperation::CreateTokens,
            "DESCRIBE_TOKENS" => AclOperation::DescribeTokens,
            "TWO_PHASE_COMMIT" => AclOperation::TwoPhaseCommit,
            _ => AclOperation::Unknown,
        }
    }

    /// Return true if this operation is [`AclOperation::Unknown`].
    pub fn is_unknown(&self) -> bool {
        *self == AclOperation::Unknown
    }
}

impl std::fmt::Display for AclOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            AclOperation::Unknown => "UNKNOWN",
            AclOperation::Any => "ANY",
            AclOperation::All => "ALL",
            AclOperation::Read => "READ",
            AclOperation::Write => "WRITE",
            AclOperation::Create => "CREATE",
            AclOperation::Delete => "DELETE",
            AclOperation::Alter => "ALTER",
            AclOperation::Describe => "DESCRIBE",
            AclOperation::ClusterAction => "CLUSTER_ACTION",
            AclOperation::DescribeConfigs => "DESCRIBE_CONFIGS",
            AclOperation::AlterConfigs => "ALTER_CONFIGS",
            AclOperation::IdempotentWrite => "IDEMPOTENT_WRITE",
            AclOperation::CreateTokens => "CREATE_TOKENS",
            AclOperation::DescribeTokens => "DESCRIBE_TOKENS",
            AclOperation::TwoPhaseCommit => "TWO_PHASE_COMMIT",
        };
        write!(f, "{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mirrors `AclOperationTest.AclOperationTestInfo` in the Java client.
    struct Info {
        operation: AclOperation,
        code: i8,
        name: &'static str,
        unknown: bool,
    }

    // Mirrors `AclOperationTest.INFOS` in the Java client, in declaration order.
    const INFOS: [Info; 16] = [
        Info { operation: AclOperation::Unknown, code: 0, name: "unknown", unknown: true },
        Info { operation: AclOperation::Any, code: 1, name: "any", unknown: false },
        Info { operation: AclOperation::All, code: 2, name: "all", unknown: false },
        Info { operation: AclOperation::Read, code: 3, name: "read", unknown: false },
        Info { operation: AclOperation::Write, code: 4, name: "write", unknown: false },
        Info { operation: AclOperation::Create, code: 5, name: "create", unknown: false },
        Info { operation: AclOperation::Delete, code: 6, name: "delete", unknown: false },
        Info { operation: AclOperation::Alter, code: 7, name: "alter", unknown: false },
        Info { operation: AclOperation::Describe, code: 8, name: "describe", unknown: false },
        Info {
            operation: AclOperation::ClusterAction,
            code: 9,
            name: "cluster_action",
            unknown: false,
        },
        Info {
            operation: AclOperation::DescribeConfigs,
            code: 10,
            name: "describe_configs",
            unknown: false,
        },
        Info {
            operation: AclOperation::AlterConfigs,
            code: 11,
            name: "alter_configs",
            unknown: false,
        },
        Info {
            operation: AclOperation::IdempotentWrite,
            code: 12,
            name: "idempotent_write",
            unknown: false,
        },
        Info {
            operation: AclOperation::CreateTokens,
            code: 13,
            name: "create_tokens",
            unknown: false,
        },
        Info {
            operation: AclOperation::DescribeTokens,
            code: 14,
            name: "describe_tokens",
            unknown: false,
        },
        Info {
            operation: AclOperation::TwoPhaseCommit,
            code: 15,
            name: "two_phase_commit",
            unknown: false,
        },
    ];

    // Test-local mirror of Java's `AclOperation.values()` (declaration order).
    const VALUES: [AclOperation; 16] = [
        AclOperation::Unknown,
        AclOperation::Any,
        AclOperation::All,
        AclOperation::Read,
        AclOperation::Write,
        AclOperation::Create,
        AclOperation::Delete,
        AclOperation::Alter,
        AclOperation::Describe,
        AclOperation::ClusterAction,
        AclOperation::DescribeConfigs,
        AclOperation::AlterConfigs,
        AclOperation::IdempotentWrite,
        AclOperation::CreateTokens,
        AclOperation::DescribeTokens,
        AclOperation::TwoPhaseCommit,
    ];

    /// Mirrors `AclOperationTest.testIsUnknown`.
    #[test]
    fn test_is_unknown() {
        for info in &INFOS {
            assert_eq!(
                info.unknown,
                info.operation.is_unknown(),
                "{} was supposed to have unknown == {}",
                info.operation,
                info.unknown
            );
        }
    }

    /// Mirrors `AclOperationTest.testCode`.
    #[test]
    fn test_code() {
        assert_eq!(VALUES.len(), INFOS.len());
        for info in &INFOS {
            assert_eq!(
                info.code,
                info.operation.code(),
                "{} was supposed to have code == {}",
                info.operation,
                info.code
            );
            assert_eq!(
                info.operation,
                AclOperation::from_code(info.code),
                "AclOperation::from_code({}) was supposed to be {}",
                info.code,
                info.operation
            );
        }
        assert_eq!(AclOperation::Unknown, AclOperation::from_code(120));
    }

    /// Mirrors `AclOperationTest.testName`.
    #[test]
    fn test_name() {
        for info in &INFOS {
            assert_eq!(
                info.operation,
                AclOperation::from_string(info.name),
                "AclOperation::from_string({}) was supposed to be {}",
                info.name,
                info.operation
            );
        }
        assert_eq!(AclOperation::Unknown, AclOperation::from_string("something"));
    }

    /// Mirrors `AclOperationTest.testExhaustive`.
    #[test]
    fn test_exhaustive() {
        assert_eq!(INFOS.len(), VALUES.len());
        for (i, info) in INFOS.iter().enumerate() {
            assert_eq!(info.operation, VALUES[i]);
        }
    }
}
