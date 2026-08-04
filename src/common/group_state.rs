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
    /// The display name of this group state (as used on the wire and in
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
            Self::Assigning => "Assigning",
            Self::Reconciling => "Reconciling",
            Self::NotReady => "NotReady",
        }
    }

    /// Case-insensitive group state lookup by string name.
    ///
    /// Returns [`GroupState::Unknown`] if the name is unrecognized, mirroring
    /// Java's `parse`.
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
    /// Panics if `type` is [`GroupType::Unknown`], mirroring Java's
    /// `IllegalArgumentException("Group type not known")`.
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

impl fmt::Display for GroupState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
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

    #[test]
    fn to_string_matches_name() {
        assert_eq!(GroupState::CompletingRebalance.to_string(), "CompletingRebalance");
        assert_eq!(GroupState::Empty.to_string(), "Empty");
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
