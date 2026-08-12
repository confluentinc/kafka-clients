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
///
/// Corresponds to `org.apache.kafka.common.GroupType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
    /// The display name of this group type (as used on the wire and in
    /// `toString`).
    ///
    /// Mirrors the private `name` field in Java.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::Consumer => "Consumer",
            Self::Classic => "Classic",
            Self::Share => "Share",
            Self::Streams => "Streams",
        }
    }

    /// Parse a string into a group type, in a case-insensitive manner.
    ///
    /// Returns [`GroupType::Unknown`] if the name is unrecognized (mirrors
    /// Java's `parse`, including the `null` handling that maps to `Unknown`).
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

impl fmt::Display for GroupType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
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

    #[test]
    fn to_string_matches_name() {
        assert_eq!(GroupType::Consumer.to_string(), "Consumer");
        assert_eq!(GroupType::Classic.to_string(), "Classic");
        assert_eq!(GroupType::Unknown.to_string(), "Unknown");
    }
}
