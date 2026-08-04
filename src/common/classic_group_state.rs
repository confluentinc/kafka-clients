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

//! The classic group state.
//!
//! Translated from `org.apache.kafka.common.ClassicGroupState`.

use std::fmt;

/// The state of a classic group.
///
/// Corresponds to `org.apache.kafka.common.ClassicGroupState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ClassicGroupState {
    /// An unrecognized classic group state (e.g. a state newer than this
    /// client).
    Unknown,
    /// The group is preparing to rebalance.
    PreparingRebalance,
    /// The group is completing a rebalance.
    CompletingRebalance,
    /// The group is stable.
    Stable,
    /// The group is dead.
    Dead,
    /// The group is empty.
    Empty,
}

impl ClassicGroupState {
    /// The display name of this classic group state (as used on the wire and in
    /// `toString`).
    ///
    /// Mirrors the private `name` field in Java.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
            Self::Empty => "Empty",
        }
    }

    /// Case-insensitive classic group state lookup by string name.
    ///
    /// Returns [`ClassicGroupState::Unknown`] if the name is unrecognized,
    /// mirroring Java's `parse`.
    pub fn parse(name: &str) -> Self {
        match name.to_uppercase().as_str() {
            "UNKNOWN" => Self::Unknown,
            "PREPARINGREBALANCE" => Self::PreparingRebalance,
            "COMPLETINGREBALANCE" => Self::CompletingRebalance,
            "STABLE" => Self::Stable,
            "DEAD" => Self::Dead,
            "EMPTY" => Self::Empty,
            _ => Self::Unknown,
        }
    }
}

impl fmt::Display for ClassicGroupState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_is_case_insensitive() {
        assert_eq!(ClassicGroupState::parse("stable"), ClassicGroupState::Stable);
        assert_eq!(ClassicGroupState::parse("DEAD"), ClassicGroupState::Dead);
        assert_eq!(ClassicGroupState::parse("Empty"), ClassicGroupState::Empty);
    }

    #[test]
    fn parse_unknown_returns_unknown() {
        assert_eq!(ClassicGroupState::parse(""), ClassicGroupState::Unknown);
        assert_eq!(ClassicGroupState::parse("bogus"), ClassicGroupState::Unknown);
    }

    #[test]
    fn to_string_matches_name() {
        assert_eq!(ClassicGroupState::PreparingRebalance.to_string(), "PreparingRebalance");
    }
}
