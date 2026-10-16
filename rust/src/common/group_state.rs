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

//! The group state.
//!
//! Translated from `org.apache.kafka.common.GroupState`.

use std::collections::HashSet;
use std::fmt;

use crate::common::GroupType;

/// The state of a group.
///
/// Corresponds to `org.apache.kafka.common.GroupState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.GroupState")]
pub enum GroupState {
    /// An unrecognized group state (e.g. a state newer than this client).
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
    /// The group is assigning (consumer/streams groups only).
    Assigning,
    /// The group is reconciling (consumer/streams groups only).
    Reconciling,
    /// The group is not ready (streams groups only).
    NotReady,
}

impl GroupState {
    /// The name of this enum constant, exactly as declared in Java
    /// (`PREPARING_REBALANCE`).
    ///
    /// Java's `Enum.name()`, which `GroupState` inherits. The display text
    /// Java's `toString()` returns (`PreparingRebalance`, the text the broker
    /// sends and [`parse`](Self::parse) reads) is this type's
    /// [`Display`](fmt::Display).
    pub fn name(&self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::PreparingRebalance => "PREPARING_REBALANCE",
            Self::CompletingRebalance => "COMPLETING_REBALANCE",
            Self::Stable => "STABLE",
            Self::Dead => "DEAD",
            Self::Empty => "EMPTY",
            Self::Assigning => "ASSIGNING",
            Self::Reconciling => "RECONCILING",
            Self::NotReady => "NOT_READY",
        }
    }

    /// Case-insensitive group state lookup by string name.
    ///
    /// Returns [`GroupState::Unknown`] if the name is unrecognized, mirroring
    /// Java's `parse`.
    #[doc(alias = "org.apache.kafka.common.GroupState#parse")]
    pub fn parse(name: &str) -> Self {
        match name.to_uppercase().as_str() {
            "UNKNOWN" => Self::Unknown,
            "PREPARINGREBALANCE" => Self::PreparingRebalance,
            "COMPLETINGREBALANCE" => Self::CompletingRebalance,
            "STABLE" => Self::Stable,
            "DEAD" => Self::Dead,
            "EMPTY" => Self::Empty,
            "ASSIGNING" => Self::Assigning,
            "RECONCILING" => Self::Reconciling,
            "NOTREADY" => Self::NotReady,
            _ => Self::Unknown,
        }
    }

    /// Returns the set of group states applicable to the given group type.
    ///
    /// Mirrors `GroupState.groupStatesForType`.
    ///
    /// # Panics
    ///
    /// Panics with `Group type not known` if `group_type` is
    /// [`GroupType::Unknown`], where Java throws
    /// `IllegalArgumentException("Group type not known")`
    /// (`GroupState.java:79-91`). A group listing reports `Unknown` for a type
    /// this client does not know, such as one a newer broker added, so check
    /// for it before calling.
    ///
    /// The panic is a deliberate deviation from Java's catchable exception:
    /// this is a lookup that otherwise always succeeds, and its signature
    /// stays a plain `HashSet` rather than becoming a `Result`.
    #[doc(alias = "org.apache.kafka.common.GroupState#groupStatesForType")]
    pub fn group_states_for_type(group_type: GroupType) -> HashSet<GroupState> {
        match group_type {
            GroupType::Classic => [
                Self::PreparingRebalance,
                Self::CompletingRebalance,
                Self::Stable,
                Self::Dead,
                Self::Empty,
            ]
            .into_iter()
            .collect(),
            GroupType::Consumer => [
                Self::PreparingRebalance,
                Self::CompletingRebalance,
                Self::Stable,
                Self::Dead,
                Self::Empty,
                Self::Assigning,
                Self::Reconciling,
            ]
            .into_iter()
            .collect(),
            GroupType::Streams => [
                Self::Stable,
                Self::Dead,
                Self::Empty,
                Self::Assigning,
                Self::Reconciling,
                Self::NotReady,
            ]
            .into_iter()
            .collect(),
            GroupType::Share => [Self::Stable, Self::Dead, Self::Empty].into_iter().collect(),
            GroupType::Unknown => panic!("Group type not known"),
        }
    }
}

/// Java's `toString()`, which returns the private `name` field: the display
/// text (`PreparingRebalance`) the broker sends and [`GroupState::parse`]
/// reads, not the constant name [`GroupState::name`] returns.
impl fmt::Display for GroupState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "Unknown",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
            Self::Empty => "Empty",
            Self::Assigning => "Assigning",
            Self::Reconciling => "Reconciling",
            Self::NotReady => "NotReady",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_is_case_insensitive() {
        assert_eq!(GroupState::parse("stable"), GroupState::Stable);
        assert_eq!(GroupState::parse("STABLE"), GroupState::Stable);
        assert_eq!(GroupState::parse("PreparingRebalance"), GroupState::PreparingRebalance);
        assert_eq!(GroupState::parse("NotReady"), GroupState::NotReady);
    }

    #[test]
    fn parse_unknown_returns_unknown() {
        assert_eq!(GroupState::parse(""), GroupState::Unknown);
        assert_eq!(GroupState::parse("bogus"), GroupState::Unknown);
    }

    /// New test, no Java original (Java has no `GroupStateTest`): every
    /// constant's `name()` is Java's `Enum.name()` and its `Display` is Java's
    /// `toString()` (`GroupState.java:51-60, 93-96`), and `parse` reads the
    /// `toString()` text back.
    #[test]
    fn name_and_display_match_java_for_every_constant() {
        for (state, name, display) in [
            (GroupState::Unknown, "UNKNOWN", "Unknown"),
            (GroupState::PreparingRebalance, "PREPARING_REBALANCE", "PreparingRebalance"),
            (GroupState::CompletingRebalance, "COMPLETING_REBALANCE", "CompletingRebalance"),
            (GroupState::Stable, "STABLE", "Stable"),
            (GroupState::Dead, "DEAD", "Dead"),
            (GroupState::Empty, "EMPTY", "Empty"),
            (GroupState::Assigning, "ASSIGNING", "Assigning"),
            (GroupState::Reconciling, "RECONCILING", "Reconciling"),
            (GroupState::NotReady, "NOT_READY", "NotReady"),
        ] {
            assert_eq!(state.name(), name);
            assert_eq!(state.to_string(), display);
            assert_eq!(GroupState::parse(&state.to_string()), state);
        }
    }

    /// New test, no Java original: Java's `parse` looks the upper-cased text up
    /// among the upper-cased `toString()` texts (`GroupState.java:62-77`), so a
    /// constant name with an underscore is not found and parses as `UNKNOWN`.
    #[test]
    fn parse_does_not_read_constant_names_with_an_underscore() {
        assert_eq!(GroupState::parse("PREPARING_REBALANCE"), GroupState::Unknown);
        assert_eq!(GroupState::parse("COMPLETING_REBALANCE"), GroupState::Unknown);
        assert_eq!(GroupState::parse("NOT_READY"), GroupState::Unknown);
        assert_eq!(GroupState::parse(GroupState::Stable.name()), GroupState::Stable);
    }

    #[test]
    fn group_states_for_classic() {
        let states = GroupState::group_states_for_type(GroupType::Classic);
        assert_eq!(states.len(), 5);
        assert!(states.contains(&GroupState::PreparingRebalance));
        assert!(states.contains(&GroupState::Empty));
        assert!(!states.contains(&GroupState::Assigning));
    }

    #[test]
    fn group_states_for_consumer() {
        let states = GroupState::group_states_for_type(GroupType::Consumer);
        assert_eq!(states.len(), 7);
        assert!(states.contains(&GroupState::Assigning));
        assert!(states.contains(&GroupState::Reconciling));
        assert!(!states.contains(&GroupState::NotReady));
    }

    #[test]
    fn group_states_for_streams() {
        let states = GroupState::group_states_for_type(GroupType::Streams);
        assert_eq!(states.len(), 6);
        assert!(states.contains(&GroupState::NotReady));
        assert!(!states.contains(&GroupState::PreparingRebalance));
    }

    #[test]
    fn group_states_for_share() {
        let states = GroupState::group_states_for_type(GroupType::Share);
        assert_eq!(states.len(), 3);
        assert!(states.contains(&GroupState::Stable));
    }

    #[test]
    #[should_panic(expected = "Group type not known")]
    fn group_states_for_unknown_panics() {
        GroupState::group_states_for_type(GroupType::Unknown);
    }
}
