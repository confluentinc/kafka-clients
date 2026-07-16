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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_round_trips_for_all_variants() {
        let all = [
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
        for op in all {
            assert_eq!(AclOperation::from_code(op.code()), op);
        }
    }

    #[test]
    fn code_values_match_java_wire_values() {
        assert_eq!(AclOperation::Unknown.code(), 0);
        assert_eq!(AclOperation::Read.code(), 3);
        assert_eq!(AclOperation::Describe.code(), 8);
        assert_eq!(AclOperation::TwoPhaseCommit.code(), 15);
    }

    #[test]
    fn unknown_code_maps_to_unknown() {
        assert_eq!(AclOperation::from_code(100), AclOperation::Unknown);
        assert_eq!(AclOperation::from_code(-1), AclOperation::Unknown);
    }

    #[test]
    fn from_string_is_case_insensitive_and_defaults_to_unknown() {
        assert_eq!(AclOperation::from_string("read"), AclOperation::Read);
        assert_eq!(AclOperation::from_string("ALTER_CONFIGS"), AclOperation::AlterConfigs);
        assert_eq!(AclOperation::from_string("nonsense"), AclOperation::Unknown);
    }

    #[test]
    fn is_unknown() {
        assert!(AclOperation::Unknown.is_unknown());
        assert!(!AclOperation::Read.is_unknown());
    }
}
