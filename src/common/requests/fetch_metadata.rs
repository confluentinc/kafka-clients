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

//! Fetch session metadata used by the KIP-227 incremental fetch protocol.
//!
//! Translated from `org.apache.kafka.common.requests.FetchMetadata`.

#![allow(dead_code)]

use std::fmt;

/// The metadata for a single fetch request: session id + epoch.
///
/// Corresponds to `org.apache.kafka.common.requests.FetchMetadata`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FetchMetadata {
    session_id: i32,
    epoch: i32,
}

impl FetchMetadata {
    /// Returns the epoch immediately following `prev_epoch`.
    ///
    /// Wraps around to `1` on overflow (matching Java's behavior) and keeps
    /// `FINAL_EPOCH` sticky.
    ///
    /// Translates `FetchMetadata.nextEpoch(int)`.
    pub fn next_epoch(prev_epoch: i32) -> i32 {
        if prev_epoch < 0 {
            // The next epoch after FINAL_EPOCH is always FINAL_EPOCH itself.
            FetchMetadata::FINAL_EPOCH
        } else if prev_epoch == i32::MAX {
            1
        } else {
            prev_epoch + 1
        }
    }

    /// The session ID used by clients with no session.
    pub const INVALID_SESSION_ID: i32 = 0;

    /// The first epoch. When used in a fetch request, indicates that the client
    /// wants to create or recreate a session.
    pub const INITIAL_EPOCH: i32 = 0;

    /// An invalid epoch. When used in a fetch request, indicates that the client
    /// wants to close any existing session, and not create a new one.
    pub const FINAL_EPOCH: i32 = -1;

    /// The metadata used when initializing a new `FetchSessionHandler`.
    pub const INITIAL: FetchMetadata = FetchMetadata {
        session_id: FetchMetadata::INVALID_SESSION_ID,
        epoch: FetchMetadata::INITIAL_EPOCH,
    };

    /// The metadata implicitly used for handling older fetch requests that do
    /// not carry fetch metadata.
    pub const LEGACY: FetchMetadata =
        FetchMetadata { session_id: FetchMetadata::INVALID_SESSION_ID, epoch: FetchMetadata::FINAL_EPOCH };

    /// Constructs metadata from an explicit session id and epoch.
    pub fn new(session_id: i32, epoch: i32) -> Self {
        Self { session_id, epoch }
    }

    /// Returns true if this metadata describes a full fetch request.
    pub fn is_full(&self) -> bool {
        self.epoch == FetchMetadata::INITIAL_EPOCH || self.epoch == FetchMetadata::FINAL_EPOCH
    }

    /// Returns the session id.
    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    /// Returns the epoch.
    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Returns metadata indicating the client wants to close the existing
    /// session.
    pub fn next_close_existing(&self) -> Self {
        Self { session_id: self.session_id, epoch: FetchMetadata::FINAL_EPOCH }
    }

    /// Returns metadata indicating the client wants to close the existing
    /// session and create a new one if possible.
    pub fn next_close_existing_attempt_new(&self) -> Self {
        Self { session_id: self.session_id, epoch: FetchMetadata::INITIAL_EPOCH }
    }

    /// Returns metadata for the first incremental fetch in a new session.
    pub fn with_incremental(session_id: i32) -> Self {
        Self { session_id, epoch: FetchMetadata::next_epoch(FetchMetadata::INITIAL_EPOCH) }
    }

    /// Returns metadata for the next incremental fetch.
    pub fn next_incremental(&self) -> Self {
        Self { session_id: self.session_id, epoch: FetchMetadata::next_epoch(self.epoch) }
    }
}

impl fmt::Display for FetchMetadata {
    /// Matches Java's `toString()`:
    /// `(sessionId=INVALID|<id>, epoch=INITIAL|FINAL|<n>)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(sessionId=")?;
        if self.session_id == FetchMetadata::INVALID_SESSION_ID {
            f.write_str("INVALID")?;
        } else {
            write!(f, "{}", self.session_id)?;
        }
        f.write_str(", epoch=")?;
        if self.epoch == FetchMetadata::INITIAL_EPOCH {
            f.write_str("INITIAL")?;
        } else if self.epoch == FetchMetadata::FINAL_EPOCH {
            f.write_str("FINAL")?;
        } else {
            write!(f, "{}", self.epoch)?;
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_epoch_wraparound() {
        assert_eq!(1, FetchMetadata::next_epoch(0));
        assert_eq!(2, FetchMetadata::next_epoch(1));
        assert_eq!(1, FetchMetadata::next_epoch(i32::MAX));
        assert_eq!(
            FetchMetadata::FINAL_EPOCH,
            FetchMetadata::next_epoch(FetchMetadata::FINAL_EPOCH)
        );
        assert_eq!(FetchMetadata::FINAL_EPOCH, FetchMetadata::next_epoch(-5));
    }

    #[test]
    fn test_initial_is_full() {
        assert!(FetchMetadata::INITIAL.is_full());
        assert!(FetchMetadata::LEGACY.is_full());
    }

    #[test]
    fn test_incremental_not_full() {
        let inc = FetchMetadata::with_incremental(42);
        assert!(!inc.is_full());
        assert_eq!(42, inc.session_id());
        assert_eq!(1, inc.epoch());
    }

    #[test]
    fn test_next_close_existing() {
        let inc = FetchMetadata::with_incremental(42);
        let closed = inc.next_close_existing();
        assert_eq!(42, closed.session_id());
        assert_eq!(FetchMetadata::FINAL_EPOCH, closed.epoch());
        assert!(closed.is_full());
    }

    #[test]
    fn test_next_close_existing_attempt_new() {
        let inc = FetchMetadata::with_incremental(42);
        let reset = inc.next_close_existing_attempt_new();
        assert_eq!(42, reset.session_id());
        assert_eq!(FetchMetadata::INITIAL_EPOCH, reset.epoch());
        assert!(reset.is_full());
    }

    #[test]
    fn test_next_incremental_advances_epoch() {
        let m = FetchMetadata::new(5, 7);
        let next = m.next_incremental();
        assert_eq!(5, next.session_id());
        assert_eq!(8, next.epoch());
    }

    #[test]
    fn test_display() {
        assert_eq!("(sessionId=INVALID, epoch=INITIAL)", FetchMetadata::INITIAL.to_string());
        assert_eq!("(sessionId=INVALID, epoch=FINAL)", FetchMetadata::LEGACY.to_string());
        assert_eq!("(sessionId=42, epoch=3)", FetchMetadata::new(42, 3).to_string());
    }
}
