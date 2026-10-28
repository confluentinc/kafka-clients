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

/// The classic group state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.ClassicGroupState")]
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
    /// Returns the name of this enum constant, exactly as declared in Java
    /// (`PREPARING_REBALANCE`).
    ///
    /// Java's `Enum.name()`, which `ClassicGroupState` inherits. The text Java's
    /// `toString()` returns (`PreparingRebalance`, the text the broker sends and
    /// [`parse`](Self::parse) reads) is this type's [`Display`](fmt::Display).
    #[doc(alias = "org.apache.kafka.common.ClassicGroupState#name")]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::PreparingRebalance => "PREPARING_REBALANCE",
            Self::CompletingRebalance => "COMPLETING_REBALANCE",
            Self::Stable => "STABLE",
            Self::Dead => "DEAD",
            Self::Empty => "EMPTY",
        }
    }

    /// Case-insensitive classic group state lookup by string name.
    ///
    /// # Arguments
    ///
    /// - `name` - the state name
    ///
    /// # Returns
    ///
    /// The state, `Unknown` if unrecognized.
    #[doc(alias = "org.apache.kafka.common.ClassicGroupState#parse")]
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

/// Java's `toString()`, which returns the private `name` field: the text
/// (`PreparingRebalance`) the broker sends and [`ClassicGroupState::parse`] reads, not
/// the constant name [`ClassicGroupState::name`] returns (the constants of `ClassicGroupState`).
impl fmt::Display for ClassicGroupState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "Unknown",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
            Self::Empty => "Empty",
        })
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

    /// New test, no Java original (Java has no `ClassicGroupStateTest`): every
    /// constant's `name()` is Java's `Enum.name()` and its `Display` is Java's
    /// `toString()` (`ClassicGroupState#toString`), and `parse` reads the `toString()` text back.
    #[test]
    fn name_and_display_match_java_for_every_constant() {
        for (state, name, display) in [
            (ClassicGroupState::Unknown, "UNKNOWN", "Unknown"),
            (
                ClassicGroupState::PreparingRebalance,
                "PREPARING_REBALANCE",
                "PreparingRebalance",
            ),
            (
                ClassicGroupState::CompletingRebalance,
                "COMPLETING_REBALANCE",
                "CompletingRebalance",
            ),
            (ClassicGroupState::Stable, "STABLE", "Stable"),
            (ClassicGroupState::Dead, "DEAD", "Dead"),
            (ClassicGroupState::Empty, "EMPTY", "Empty"),
        ] {
            assert_eq!(state.name(), name);
            assert_eq!(state.to_string(), display);
            assert_eq!(ClassicGroupState::parse(&state.to_string()), state);
        }
    }
}
