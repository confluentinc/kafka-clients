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

//! Resource pattern types.
//!
//! Corresponds to `org.apache.kafka.common.resource.PatternType`.

/// Resource pattern type.
///
/// Corresponds to `org.apache.kafka.common.resource.PatternType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PatternType {
    /// Represents any `PatternType` which this client cannot understand,
    /// perhaps because this client is too old.
    Unknown,
    /// In a filter, matches any resource pattern type.
    Any,
    /// In a filter, will perform pattern matching.
    Match,
    /// A literal resource name.
    ///
    /// A literal name defines the full name of a resource, e.g. topic with name
    /// 'foo', or group with name 'bob'. The special wildcard character `*` can
    /// be used to represent a resource with any name.
    Literal,
    /// A prefixed resource name.
    ///
    /// A prefixed name defines a prefix for a resource, e.g. topics with names
    /// that start with 'foo'.
    Prefixed,
}

impl PatternType {
    /// All pattern types, in declaration (code) order. Mirrors Java's
    /// `PatternType.values()`.
    pub const VALUES: [PatternType; 5] = [
        PatternType::Unknown,
        PatternType::Any,
        PatternType::Match,
        PatternType::Literal,
        PatternType::Prefixed,
    ];

    /// Return the code of this resource pattern type.
    pub fn code(&self) -> i8 {
        match self {
            PatternType::Unknown => 0,
            PatternType::Any => 1,
            PatternType::Match => 2,
            PatternType::Literal => 3,
            PatternType::Prefixed => 4,
        }
    }

    /// Return whether this resource pattern type is [`PatternType::Unknown`].
    pub fn is_unknown(&self) -> bool {
        *self == PatternType::Unknown
    }

    /// Return whether this resource pattern type is a concrete type, rather than
    /// `UNKNOWN` or one of the filter types.
    pub fn is_specific(&self) -> bool {
        *self != PatternType::Unknown && *self != PatternType::Any && *self != PatternType::Match
    }

    /// Return the `PatternType` with the provided code or [`PatternType::Unknown`]
    /// if one cannot be found.
    pub fn from_code(code: i8) -> PatternType {
        match code {
            0 => PatternType::Unknown,
            1 => PatternType::Any,
            2 => PatternType::Match,
            3 => PatternType::Literal,
            4 => PatternType::Prefixed,
            _ => PatternType::Unknown,
        }
    }

    /// Return the `PatternType` with the provided name or [`PatternType::Unknown`]
    /// if one cannot be found.
    ///
    /// Matches Java's case-sensitive `NAME_TO_VALUE` lookup on the enum constant
    /// name.
    pub fn from_string(name: &str) -> PatternType {
        match name {
            "UNKNOWN" => PatternType::Unknown,
            "ANY" => PatternType::Any,
            "MATCH" => PatternType::Match,
            "LITERAL" => PatternType::Literal,
            "PREFIXED" => PatternType::Prefixed,
            _ => PatternType::Unknown,
        }
    }
}

impl std::fmt::Display for PatternType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            PatternType::Unknown => "UNKNOWN",
            PatternType::Any => "ANY",
            PatternType::Match => "MATCH",
            PatternType::Literal => "LITERAL",
            PatternType::Prefixed => "PREFIXED",
        };
        write!(f, "{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_round_trips_for_all_variants() {
        for pt in PatternType::VALUES {
            assert_eq!(PatternType::from_code(pt.code()), pt);
        }
    }

    #[test]
    fn code_values_match_java_wire_values() {
        assert_eq!(PatternType::Unknown.code(), 0);
        assert_eq!(PatternType::Any.code(), 1);
        assert_eq!(PatternType::Match.code(), 2);
        assert_eq!(PatternType::Literal.code(), 3);
        assert_eq!(PatternType::Prefixed.code(), 4);
    }

    #[test]
    fn is_specific_only_for_concrete_types() {
        assert!(!PatternType::Unknown.is_specific());
        assert!(!PatternType::Any.is_specific());
        assert!(!PatternType::Match.is_specific());
        assert!(PatternType::Literal.is_specific());
        assert!(PatternType::Prefixed.is_specific());
    }

    #[test]
    fn from_string_is_case_sensitive_and_defaults_to_unknown() {
        assert_eq!(PatternType::from_string("LITERAL"), PatternType::Literal);
        assert_eq!(PatternType::from_string("literal"), PatternType::Unknown);
        assert_eq!(PatternType::from_string("bogus"), PatternType::Unknown);
    }

    #[test]
    fn from_code_unknown_defaults_to_unknown() {
        assert_eq!(PatternType::from_code(120), PatternType::Unknown);
    }
}
