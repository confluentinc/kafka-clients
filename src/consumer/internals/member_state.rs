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

//! `MemberState` — the per-member state machine for KIP-848 group membership.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.MemberState`.

#![allow(dead_code)]

use std::fmt;

/// Membership state for a single KIP-848 consumer group member.
///
/// Mirrors Java's enum constants. Translation note: Java models the
/// per-variant `previousValidStates` via per-enum-constant bodies; Rust
/// collapses to a single `match` per method.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MemberState {
    /// Member has a group id, but it is not subscribed to any topic to receive
    /// automatic assignments. This will be the state when the member has never
    /// subscribed, or when it has unsubscribed from all topics. While in this
    /// state the member can commit offsets but won't be an active member of the
    /// consumer group (no heartbeats sent).
    Unsubscribed,

    /// Member is attempting to join a consumer group. While in this state, the
    /// member will send heartbeat requests on the interval, with epoch 0, until
    /// it gets a response with an epoch > 0 or a fatal failure. A member
    /// transitions to this state when it tries to join the group for the first
    /// time with a call to subscribe, or when it has been fenced and tries to
    /// re-join.
    Joining,

    /// Member has received a new target assignment (partitions could have been
    /// assigned or revoked), and it is processing it. While in this state, the
    /// member will continue to send heartbeat on the interval, and reconcile
    /// the assignment.
    Reconciling,

    /// Member has completed reconciling an assignment received, and stays in
    /// this state only until the next heartbeat request is sent out to
    /// acknowledge the assignment to the server. This state indicates that the
    /// next heartbeat request must be sent without waiting for the heartbeat
    /// interval to expire. Note that once the ack is sent, the member could go
    /// back to [`Self::Reconciling`] if it still has assignment waiting to be
    /// reconciled.
    Acknowledging,

    /// Member is active in a group and has processed all assignments received.
    /// While in this state, the member will send heartbeats on the interval.
    Stable,

    /// Member transitions to this state when it receives a `UnknownMemberId`
    /// or `FencedMemberEpoch` error from the broker, indicating that it has
    /// been left out of the group. While in this state, the member will stop
    /// sending heartbeats, it will give up its partitions by invoking the user
    /// callbacks for `onPartitionsLost`, and then transition to
    /// [`Self::Joining`] to re-join the group as a new member.
    Fenced,

    /// The member transitions to this state before sending a heartbeat to
    /// leave the group. While in this state, the member will continue sending
    /// heartbeats while it releases its assignment calling the user's
    /// callback. When callbacks complete, the member will transition out of
    /// this state into [`Self::Leaving`] to send a heartbeat to leave the
    /// group.
    PrepareLeaving,

    /// Member has committed offsets and releases its assignment, so it stays
    /// in this state until the next heartbeat request is sent out with epoch
    /// -1 or -2 to effectively leave the group. This state indicates that the
    /// next heartbeat request must be sent without waiting for the heartbeat
    /// interval to expire.
    Leaving,

    /// The member failed with an unrecoverable error received in a heartbeat
    /// response. This is an unrecoverable state where the member won't send
    /// any requests to the broker and cannot perform any other transition.
    Fatal,

    /// The member transitions to this state when the poll timer expires,
    /// indicating that there hasn't been a call to consumer.poll within the
    /// `max.poll.interval.ms`. While in this state, the member will send a
    /// heartbeat to leave the group, invoke the onPartitionsLost callback, and
    /// clear its assignments.
    Stale,
}

impl MemberState {
    /// Returns the set of states that can legally transition INTO `self`.
    ///
    /// Mirrors Java's `MemberState.getPreviousValidStates()`. The list is
    /// `&'static` to avoid per-call allocations.
    pub(crate) fn previous_valid_states(&self) -> &'static [MemberState] {
        match self {
            // STABLE.previousValidStates = Arrays.asList(JOINING, ACKNOWLEDGING, RECONCILING);
            Self::Stable => &[Self::Joining, Self::Acknowledging, Self::Reconciling],
            // RECONCILING.previousValidStates = Arrays.asList(STABLE, JOINING, ACKNOWLEDGING, RECONCILING);
            Self::Reconciling => &[Self::Stable, Self::Joining, Self::Acknowledging, Self::Reconciling],
            // ACKNOWLEDGING.previousValidStates = Collections.singletonList(RECONCILING);
            Self::Acknowledging => &[Self::Reconciling],
            // FATAL.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING, ACKNOWLEDGING,
            //         PREPARE_LEAVING, LEAVING, UNSUBSCRIBED);
            Self::Fatal => &[
                Self::Joining,
                Self::Stable,
                Self::Reconciling,
                Self::Acknowledging,
                Self::PrepareLeaving,
                Self::Leaving,
                Self::Unsubscribed,
            ],
            // FENCED.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING, ACKNOWLEDGING,
            //         PREPARE_LEAVING, LEAVING);
            Self::Fenced => &[
                Self::Joining,
                Self::Stable,
                Self::Reconciling,
                Self::Acknowledging,
                Self::PrepareLeaving,
                Self::Leaving,
            ],
            // JOINING.previousValidStates = Arrays.asList(FENCED, UNSUBSCRIBED, STALE);
            Self::Joining => &[Self::Fenced, Self::Unsubscribed, Self::Stale],
            // PREPARE_LEAVING.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING,
            //         ACKNOWLEDGING, UNSUBSCRIBED);
            Self::PrepareLeaving => &[
                Self::Joining,
                Self::Stable,
                Self::Reconciling,
                Self::Acknowledging,
                Self::Unsubscribed,
            ],
            // LEAVING.previousValidStates = Collections.singletonList(PREPARE_LEAVING);
            Self::Leaving => &[Self::PrepareLeaving],
            // UNSUBSCRIBED.previousValidStates = Arrays.asList(PREPARE_LEAVING, LEAVING, FENCED);
            Self::Unsubscribed => &[Self::PrepareLeaving, Self::Leaving, Self::Fenced],
            // STALE.previousValidStates = Collections.singletonList(LEAVING);
            Self::Stale => &[Self::Leaving],
        }
    }

    /// Returns `true` if the member is in a state where it should reconcile
    /// the new assignment. Expected to be true whenever the member is part of
    /// the group and intends of staying in it (e.g. false when the member is
    /// preparing to leave the group).
    ///
    /// Mirrors Java's `MemberState.canHandleNewAssignment()`.
    pub(crate) fn can_handle_new_assignment(&self) -> bool {
        Self::Reconciling.previous_valid_states().contains(self)
    }

    /// Human-readable name, mirroring Java's enum `name()` output (uppercase
    /// with underscores).
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Unsubscribed => "UNSUBSCRIBED",
            Self::Joining => "JOINING",
            Self::Reconciling => "RECONCILING",
            Self::Acknowledging => "ACKNOWLEDGING",
            Self::Stable => "STABLE",
            Self::Fenced => "FENCED",
            Self::PrepareLeaving => "PREPARE_LEAVING",
            Self::Leaving => "LEAVING",
            Self::Fatal => "FATAL",
            Self::Stale => "STALE",
        }
    }
}

impl fmt::Display for MemberState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the per-variant `previous_valid_states` list against Java's
    /// static initializer block. Any future change to the state machine
    /// breaks this test and forces a behavior-parity review.
    #[test]
    fn previous_valid_states_match_java_static_block() {
        assert_eq!(
            MemberState::Stable.previous_valid_states(),
            &[
                MemberState::Joining,
                MemberState::Acknowledging,
                MemberState::Reconciling
            ]
        );
        assert_eq!(
            MemberState::Reconciling.previous_valid_states(),
            &[
                MemberState::Stable,
                MemberState::Joining,
                MemberState::Acknowledging,
                MemberState::Reconciling
            ]
        );
        assert_eq!(MemberState::Acknowledging.previous_valid_states(), &[MemberState::Reconciling]);
        assert_eq!(
            MemberState::Fatal.previous_valid_states(),
            &[
                MemberState::Joining,
                MemberState::Stable,
                MemberState::Reconciling,
                MemberState::Acknowledging,
                MemberState::PrepareLeaving,
                MemberState::Leaving,
                MemberState::Unsubscribed,
            ]
        );
        assert_eq!(
            MemberState::Fenced.previous_valid_states(),
            &[
                MemberState::Joining,
                MemberState::Stable,
                MemberState::Reconciling,
                MemberState::Acknowledging,
                MemberState::PrepareLeaving,
                MemberState::Leaving,
            ]
        );
        assert_eq!(
            MemberState::Joining.previous_valid_states(),
            &[MemberState::Fenced, MemberState::Unsubscribed, MemberState::Stale]
        );
        assert_eq!(
            MemberState::PrepareLeaving.previous_valid_states(),
            &[
                MemberState::Joining,
                MemberState::Stable,
                MemberState::Reconciling,
                MemberState::Acknowledging,
                MemberState::Unsubscribed,
            ]
        );
        assert_eq!(MemberState::Leaving.previous_valid_states(), &[MemberState::PrepareLeaving]);
        assert_eq!(
            MemberState::Unsubscribed.previous_valid_states(),
            &[MemberState::PrepareLeaving, MemberState::Leaving, MemberState::Fenced]
        );
        assert_eq!(MemberState::Stale.previous_valid_states(), &[MemberState::Leaving]);
    }

    /// Mirrors Java's `MemberState.canHandleNewAssignment()`:
    /// Reconciling.previousValidStates contains STABLE, JOINING, ACKNOWLEDGING,
    /// RECONCILING.
    #[test]
    fn can_handle_new_assignment() {
        assert!(MemberState::Stable.can_handle_new_assignment());
        assert!(MemberState::Joining.can_handle_new_assignment());
        assert!(MemberState::Acknowledging.can_handle_new_assignment());
        assert!(MemberState::Reconciling.can_handle_new_assignment());
        assert!(!MemberState::Unsubscribed.can_handle_new_assignment());
        assert!(!MemberState::Fenced.can_handle_new_assignment());
        assert!(!MemberState::PrepareLeaving.can_handle_new_assignment());
        assert!(!MemberState::Leaving.can_handle_new_assignment());
        assert!(!MemberState::Fatal.can_handle_new_assignment());
        assert!(!MemberState::Stale.can_handle_new_assignment());
    }

    /// Mirrors Java's enum `name()` output.
    #[test]
    fn name_matches_java_enum() {
        assert_eq!(MemberState::Unsubscribed.name(), "UNSUBSCRIBED");
        assert_eq!(MemberState::Joining.name(), "JOINING");
        assert_eq!(MemberState::Reconciling.name(), "RECONCILING");
        assert_eq!(MemberState::Acknowledging.name(), "ACKNOWLEDGING");
        assert_eq!(MemberState::Stable.name(), "STABLE");
        assert_eq!(MemberState::Fenced.name(), "FENCED");
        assert_eq!(MemberState::PrepareLeaving.name(), "PREPARE_LEAVING");
        assert_eq!(MemberState::Leaving.name(), "LEAVING");
        assert_eq!(MemberState::Fatal.name(), "FATAL");
        assert_eq!(MemberState::Stale.name(), "STALE");
    }
}
