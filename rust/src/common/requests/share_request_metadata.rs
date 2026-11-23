// Copyright 2026 Confluent Inc.
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

//! Share-session metadata carried by `ShareFetch` and `ShareAcknowledge` requests.
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareRequestMetadata`.

use std::fmt;

use crate::common::Uuid;

/// The member id and share-session epoch of a share request (KIP-932).
///
/// Corresponds to `org.apache.kafka.common.requests.ShareRequestMetadata`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata")]
pub struct ShareRequestMetadata {
    /// The member ID.
    member_id: Uuid,
    /// The share session epoch.
    epoch: i32,
}

impl ShareRequestMetadata {
    /// The first epoch. When used in a ShareFetch request, indicates that the client
    /// wants to create a session.
    pub const INITIAL_EPOCH: i32 = 0;

    /// An invalid epoch. When used in a ShareFetch request, indicates that the client
    /// wants to close an existing session.
    pub const FINAL_EPOCH: i32 = -1;

    /// Creates the metadata for a member id and a share-session epoch.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#ShareRequestMetadata")]
    pub fn new(member_id: Uuid, epoch: i32) -> Self {
        Self { member_id, epoch }
    }

    /// Whether this session is a new session.
    ///
    /// Returns whether the session epoch is the initial epoch.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#isNewSession")]
    pub fn is_new_session(&self) -> bool {
        self.epoch == Self::INITIAL_EPOCH
    }

    /// Returns the next epoch after `prev_epoch`.
    ///
    /// The next epoch after [`FINAL_EPOCH`](Self::FINAL_EPOCH) (or any negative
    /// epoch) is always `FINAL_EPOCH` itself, and the epoch wraps from
    /// `i32::MAX` to `1`, skipping [`INITIAL_EPOCH`](Self::INITIAL_EPOCH).
    ///
    /// Translates the static `ShareRequestMetadata.nextEpoch(int)`. Java
    /// overloads `nextEpoch` with the instance form [`next_epoch`](Self::next_epoch),
    /// which keeps the plain name; the static takes the `do_` prefix the crate
    /// uses for such a collision.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#nextEpoch")]
    pub fn do_next_epoch(prev_epoch: i32) -> i32 {
        if prev_epoch < 0 {
            // The next epoch after FINAL_EPOCH is always FINAL_EPOCH itself.
            Self::FINAL_EPOCH
        } else if prev_epoch == i32::MAX {
            1
        } else {
            prev_epoch + 1
        }
    }

    /// Returns the metadata of the initial epoch for `member_id`.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#initialEpoch")]
    pub fn initial_epoch(member_id: Uuid) -> Self {
        Self::new(member_id, Self::INITIAL_EPOCH)
    }

    /// Returns the metadata of the next epoch, for the same member.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#nextEpoch")]
    pub fn next_epoch(&self) -> Self {
        Self::new(self.member_id, Self::do_next_epoch(self.epoch))
    }

    /// Returns the metadata that closes the existing session and attempts a new
    /// one: the initial epoch, for the same member.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#nextCloseExistingAttemptNew")]
    pub fn next_close_existing_attempt_new(&self) -> Self {
        Self::new(self.member_id, Self::INITIAL_EPOCH)
    }

    /// Returns the metadata of the final epoch, for the same member.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#finalEpoch")]
    pub fn final_epoch(&self) -> Self {
        Self::new(self.member_id, Self::FINAL_EPOCH)
    }

    /// Returns the member ID.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#memberId")]
    pub fn member_id(&self) -> Uuid {
        self.member_id
    }

    /// Returns the share session epoch.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#epoch")]
    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Whether the session epoch is the final epoch.
    #[doc(alias = "org.apache.kafka.common.requests.ShareRequestMetadata#isFinalEpoch")]
    pub fn is_final_epoch(&self) -> bool {
        self.epoch == Self::FINAL_EPOCH
    }
}

impl fmt::Display for ShareRequestMetadata {
    /// Matches Java's `toString()`: `(memberId=<id>, epoch=INITIAL|FINAL|<n>)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(memberId={}, ", self.member_id)?;
        match self.epoch {
            Self::INITIAL_EPOCH => write!(f, "epoch=INITIAL)"),
            Self::FINAL_EPOCH => write!(f, "epoch=FINAL)"),
            epoch => write!(f, "epoch={epoch})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Java has no `ShareRequestMetadataTest`; these pin each method against the
    // Java body it translates.

    #[test]
    fn test_constants() {
        assert_eq!(0, ShareRequestMetadata::INITIAL_EPOCH);
        assert_eq!(-1, ShareRequestMetadata::FINAL_EPOCH);
    }

    #[test]
    fn test_do_next_epoch() {
        assert_eq!(1, ShareRequestMetadata::do_next_epoch(0));
        assert_eq!(43, ShareRequestMetadata::do_next_epoch(42));
        // Wraps to 1, skipping INITIAL_EPOCH.
        assert_eq!(1, ShareRequestMetadata::do_next_epoch(i32::MAX));
        // Any negative epoch, FINAL_EPOCH included, stays FINAL_EPOCH.
        assert_eq!(-1, ShareRequestMetadata::do_next_epoch(-1));
        assert_eq!(-1, ShareRequestMetadata::do_next_epoch(i32::MIN));
    }

    #[test]
    fn test_epoch_transitions_keep_the_member_id() {
        let member_id = Uuid::new(7, 9);
        let initial = ShareRequestMetadata::initial_epoch(member_id);
        assert_eq!(member_id, initial.member_id());
        assert_eq!(ShareRequestMetadata::INITIAL_EPOCH, initial.epoch());
        assert!(initial.is_new_session());
        assert!(!initial.is_final_epoch());

        let next = initial.next_epoch();
        assert_eq!(member_id, next.member_id());
        assert_eq!(1, next.epoch());
        assert!(!next.is_new_session());

        let reset = next.next_epoch().next_close_existing_attempt_new();
        assert_eq!(member_id, reset.member_id());
        assert_eq!(ShareRequestMetadata::INITIAL_EPOCH, reset.epoch());

        let last = next.final_epoch();
        assert_eq!(member_id, last.member_id());
        assert!(last.is_final_epoch());
        assert!(!last.is_new_session());
        assert!(last.next_epoch().is_final_epoch());
    }

    #[test]
    fn test_display_matches_java_to_string() {
        let member_id = Uuid::new(7, 9);
        let initial = ShareRequestMetadata::initial_epoch(member_id);
        assert_eq!(format!("(memberId={member_id}, epoch=INITIAL)"), initial.to_string());
        assert_eq!(
            format!("(memberId={member_id}, epoch=FINAL)"),
            initial.final_epoch().to_string()
        );
        assert_eq!(
            format!("(memberId={member_id}, epoch=5)"),
            ShareRequestMetadata::new(member_id, 5).to_string()
        );
    }
}
