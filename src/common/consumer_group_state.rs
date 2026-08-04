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

//! The consumer group state.
//!
//! Translated from `org.apache.kafka.common.ConsumerGroupState`. Deprecated
//! since 4.0 in favor of [`GroupState`](crate::common::GroupState); retained
//! because the deprecated `ConsumerGroupListing`/`ConsumerGroupDescription`
//! surfaces still expose it.

use std::fmt;

/// The state of a consumer group.
///
/// Corresponds to `org.apache.kafka.common.ConsumerGroupState` (deprecated
/// since 4.0).
#[deprecated(since = "4.0.0", note = "Use GroupState instead")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConsumerGroupState {
    /// An unrecognized consumer group state.
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
    /// The group is assigning.
    Assigning,
    /// The group is reconciling.
    Reconciling,
}

#[allow(deprecated)]
impl ConsumerGroupState {
    /// The display name of this consumer group state (as used in `toString`).
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
        }
    }

    /// Case-insensitive consumer group state lookup by string name.
    ///
    /// Returns [`ConsumerGroupState::Unknown`] if the name is unrecognized,
    /// mirroring Java's `parse`.
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
            _ => Self::Unknown,
        }
    }
}

#[allow(deprecated)]
impl fmt::Display for ConsumerGroupState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[allow(deprecated)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_is_case_insensitive() {
        assert_eq!(ConsumerGroupState::parse("stable"), ConsumerGroupState::Stable);
        assert_eq!(ConsumerGroupState::parse("ASSIGNING"), ConsumerGroupState::Assigning);
    }

    #[test]
    fn parse_unknown_returns_unknown() {
        assert_eq!(ConsumerGroupState::parse("bogus"), ConsumerGroupState::Unknown);
    }

    #[test]
    fn to_string_matches_name() {
        assert_eq!(ConsumerGroupState::Reconciling.to_string(), "Reconciling");
    }
}
