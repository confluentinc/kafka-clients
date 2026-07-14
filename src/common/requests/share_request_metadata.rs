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

//! Metadata for the next `ShareFetch`/`ShareAcknowledge` request (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareRequestMetadata`.

use std::fmt;

use crate::common::Uuid;

/// The first epoch. When used in a ShareFetch request, indicates that the
/// client wants to create a session.
///
/// Corresponds to `ShareRequestMetadata.INITIAL_EPOCH`.
pub const INITIAL_EPOCH: i32 = 0;

/// An invalid epoch. When used in a ShareFetch request, indicates that the
/// client wants to close an existing session.
///
/// Corresponds to `ShareRequestMetadata.FINAL_EPOCH`.
pub const FINAL_EPOCH: i32 = -1;

/// Metadata carrying the member id and share session epoch for the next
/// `ShareFetch`/`ShareAcknowledge` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareRequestMetadata`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShareRequestMetadata {
    member_id: Uuid,
    epoch: i32,
}

impl ShareRequestMetadata {
    /// Creates metadata with the given member id and epoch.
    pub fn new(member_id: Uuid, epoch: i32) -> Self {
        Self { member_id, epoch }
    }

    /// Creates metadata at the initial epoch for the given member id.
    ///
    /// Corresponds to `ShareRequestMetadata.initialEpoch(Uuid)`.
    pub fn initial_epoch(member_id: Uuid) -> Self {
        Self::new(member_id, INITIAL_EPOCH)
    }

    /// Whether this metadata represents a new session (initial epoch).
    ///
    /// Corresponds to `ShareRequestMetadata.isNewSession()`.
    pub fn is_new_session(&self) -> bool {
        self.epoch == INITIAL_EPOCH
    }

    /// Returns the next epoch after `prev_epoch`.
    ///
    /// Corresponds to `ShareRequestMetadata.nextEpoch(int)`.
    pub fn next_epoch_from(prev_epoch: i32) -> i32 {
        if prev_epoch < 0 {
            // The next epoch after FINAL_EPOCH is always FINAL_EPOCH itself.
            FINAL_EPOCH
        } else if prev_epoch == i32::MAX {
            1
        } else {
            prev_epoch + 1
        }
    }

    /// Returns metadata advanced to the next epoch.
    ///
    /// Corresponds to `ShareRequestMetadata.nextEpoch()`.
    pub fn next_epoch(&self) -> Self {
        Self::new(self.member_id, Self::next_epoch_from(self.epoch))
    }

    /// Returns metadata reset to the initial epoch, used to close an existing
    /// session and attempt to create a new one.
    ///
    /// Corresponds to `ShareRequestMetadata.nextCloseExistingAttemptNew()`.
    pub fn next_close_existing_attempt_new(&self) -> Self {
        Self::new(self.member_id, INITIAL_EPOCH)
    }

    /// Returns metadata at the final epoch, used to close the session.
    ///
    /// Corresponds to `ShareRequestMetadata.finalEpoch()`.
    pub fn final_epoch(&self) -> Self {
        Self::new(self.member_id, FINAL_EPOCH)
    }

    /// Returns the member id.
    pub fn member_id(&self) -> Uuid {
        self.member_id
    }

    /// Returns the share session epoch.
    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Whether this metadata is at the final epoch.
    ///
    /// Corresponds to `ShareRequestMetadata.isFinalEpoch()`.
    pub fn is_final_epoch(&self) -> bool {
        self.epoch == FINAL_EPOCH
    }
}

impl fmt::Display for ShareRequestMetadata {
    /// Matches Java's `toString()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(memberId={}, ", self.member_id)?;
        if self.epoch == INITIAL_EPOCH {
            write!(f, "epoch=INITIAL)")
        } else if self.epoch == FINAL_EPOCH {
            write!(f, "epoch=FINAL)")
        } else {
            write!(f, "epoch={})", self.epoch)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_and_final() {
        let id = Uuid::new(1, 2);
        let initial = ShareRequestMetadata::initial_epoch(id);
        assert!(initial.is_new_session());
        assert!(!initial.is_final_epoch());
        assert_eq!(initial.epoch(), INITIAL_EPOCH);

        let fin = initial.final_epoch();
        assert!(fin.is_final_epoch());
        assert_eq!(fin.epoch(), FINAL_EPOCH);
        assert_eq!(fin.member_id(), id);
    }

    #[test]
    fn test_next_epoch_from_edge_cases() {
        assert_eq!(ShareRequestMetadata::next_epoch_from(FINAL_EPOCH), FINAL_EPOCH);
        assert_eq!(ShareRequestMetadata::next_epoch_from(-5), FINAL_EPOCH);
        assert_eq!(ShareRequestMetadata::next_epoch_from(i32::MAX), 1);
        assert_eq!(ShareRequestMetadata::next_epoch_from(0), 1);
        assert_eq!(ShareRequestMetadata::next_epoch_from(7), 8);
    }

    #[test]
    fn test_next_epoch_instance() {
        let id = Uuid::new(3, 4);
        let m = ShareRequestMetadata::new(id, 5);
        let next = m.next_epoch();
        assert_eq!(next.epoch(), 6);
        assert_eq!(next.member_id(), id);
    }

    #[test]
    fn test_next_close_existing_resets_to_initial() {
        let id = Uuid::new(5, 6);
        let m = ShareRequestMetadata::new(id, 42);
        let reset = m.next_close_existing_attempt_new();
        assert_eq!(reset.epoch(), INITIAL_EPOCH);
        assert!(reset.is_new_session());
    }

    #[test]
    fn test_display() {
        let id = Uuid::new(1, 2);
        assert!(
            ShareRequestMetadata::new(id, INITIAL_EPOCH)
                .to_string()
                .ends_with("epoch=INITIAL)")
        );
        assert!(ShareRequestMetadata::new(id, FINAL_EPOCH).to_string().ends_with("epoch=FINAL)"));
        assert!(ShareRequestMetadata::new(id, 9).to_string().ends_with("epoch=9)"));
    }
}
