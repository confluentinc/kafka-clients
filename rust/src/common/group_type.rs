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

//! The group type.
//!
//! Translated from `org.apache.kafka.common.GroupType`.

use std::fmt;

/// The type of a group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.GroupType")]
pub enum GroupType {
    /// An unrecognized group type (e.g. a type newer than this client).
    Unknown,
    /// A consumer group using the new (KIP-848) protocol.
    Consumer,
    /// A classic consumer group.
    Classic,
    /// A share group (KIP-932).
    Share,
    /// A streams group.
    Streams,
}

impl GroupType {
    /// Returns the name of this enum constant, exactly as declared in Java
    /// (`CONSUMER`).
    ///
    /// Java's `Enum.name()`, which `GroupType` inherits. The text Java's
    /// `toString()` returns (`Consumer`, the text the broker sends and
    /// [`parse`](Self::parse) reads) is this type's [`Display`](fmt::Display).
    #[doc(alias = "org.apache.kafka.common.GroupType#name")]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Consumer => "CONSUMER",
            Self::Classic => "CLASSIC",
            Self::Share => "SHARE",
            Self::Streams => "STREAMS",
        }
    }

    /// Parse a string into a consumer group type, in a case-insensitive manner.
    ///
    /// # Arguments
    ///
    /// - `name` - the type name
    ///
    /// # Returns
    ///
    /// The group type, `Unknown` if unrecognized.
    #[doc(alias = "org.apache.kafka.common.GroupType#parse")]
    pub fn parse(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "unknown" => Self::Unknown,
            "consumer" => Self::Consumer,
            "classic" => Self::Classic,
            "share" => Self::Share,
            "streams" => Self::Streams,
            _ => Self::Unknown,
        }
    }
}

/// Java's `toString()`, which returns the private `name` field: the text
/// (`Consumer`) the broker sends and [`GroupType::parse`] reads, not
/// the constant name [`GroupType::name`] returns (the constants of `GroupType`).
impl fmt::Display for GroupType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "Unknown",
            Self::Consumer => "Consumer",
            Self::Classic => "Classic",
            Self::Share => "Share",
            Self::Streams => "Streams",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_is_case_insensitive() {
        assert_eq!(GroupType::parse("consumer"), GroupType::Consumer);
        assert_eq!(GroupType::parse("CONSUMER"), GroupType::Consumer);
        assert_eq!(GroupType::parse("Classic"), GroupType::Classic);
        assert_eq!(GroupType::parse("share"), GroupType::Share);
        assert_eq!(GroupType::parse("Streams"), GroupType::Streams);
    }

    #[test]
    fn parse_unknown_returns_unknown() {
        assert_eq!(GroupType::parse(""), GroupType::Unknown);
        assert_eq!(GroupType::parse("not-a-type"), GroupType::Unknown);
    }

    /// New test, no Java original (Java has no `GroupTypeTest`): every
    /// constant's `name()` is Java's `Enum.name()` and its `Display` is Java's
    /// `toString()` (`GroupType#toString`), and `parse` reads the `toString()` text back.
    #[test]
    fn name_and_display_match_java_for_every_constant() {
        for (group_type, name, display) in [
            (GroupType::Unknown, "UNKNOWN", "Unknown"),
            (GroupType::Consumer, "CONSUMER", "Consumer"),
            (GroupType::Classic, "CLASSIC", "Classic"),
            (GroupType::Share, "SHARE", "Share"),
            (GroupType::Streams, "STREAMS", "Streams"),
        ] {
            assert_eq!(group_type.name(), name);
            assert_eq!(group_type.to_string(), display);
            assert_eq!(GroupType::parse(&group_type.to_string()), group_type);
        }
    }
}
