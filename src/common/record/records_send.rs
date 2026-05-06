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

//! Translation of `org.apache.kafka.common.record.RecordsSend`.
//!
//! Java's `RecordsSend` is an abstract class implementing
//! `org.apache.kafka.common.network.Send`. It tracks the bookkeeping state
//! (`maxBytesToWrite`, `remaining`, `pending`) for a single in-flight write
//! and delegates the actual byte-pushing to a subclass-defined
//! `writeTo(channel, previouslyWritten, remaining)` method.
//!
//! ## Phase 3d-3 scope
//!
//! `Send` and `TransferableChannel` live in `common/network/*` (Phase 5).
//! Phase 3d-3 ships the **state-tracking core** of `RecordsSend` so the
//! Phase 5 actor can wire the channel methods on top without redesigning
//! the bookkeeping.
//!
//! Specifically, the Rust struct here:
//!
//! * Stores `records: R`, `max_bytes_to_write`, `remaining`, `pending`.
//! * Exposes [`RecordsSend::completed`], [`RecordsSend::size`],
//!   [`RecordsSend::records`], [`RecordsSend::max_bytes_to_write`],
//!   [`RecordsSend::remaining`], and the bookkeeping mutators
//!   [`RecordsSend::advance`] / [`RecordsSend::set_pending`] that Phase 5
//!   will need.
//! * **Does not** define a `write_to(channel)` method — the
//!   `TransferableChannel` trait isn't translated yet (Phase 5). The
//!   abstract subclass dispatch is deferred to that phase along with the
//!   concrete `Send` trait impl.
//!
//! Phase 5 will:
//!
//! 1. Translate `TransferableChannel` (with `write`,
//!    `transfer_from`, `has_pending_writes`, `write_byte_buffer`).
//! 2. Add a trait `WriteRecordsTo` (or similar) defining the abstract
//!    subclass-supplied `write_to(channel, prev, rem)` method.
//! 3. Add a `write_to(channel)` method on [`RecordsSend`] (or via a `Send`
//!    trait impl) that uses [`Self::advance`] + [`Self::set_pending`] to
//!    drive the loop.

use crate::common::record::BaseRecords;

/// State-tracking core of Java's abstract `RecordsSend<T extends BaseRecords>`.
///
/// Owns the records and the per-write bookkeeping (`maxBytesToWrite`,
/// `remaining`, `pending`). Phase 5 wraps this with the actual
/// `TransferableChannel` write loop.
#[derive(Debug)]
pub struct RecordsSend<R: BaseRecords> {
    records: R,
    max_bytes_to_write: i32,
    remaining: i32,
    pending: bool,
}

impl<R: BaseRecords> RecordsSend<R> {
    /// Construct a new send with `max_bytes_to_write` bytes pending.
    /// Mirrors Java's `RecordsSend(T, int)`.
    pub fn new(records: R, max_bytes_to_write: i32) -> Self {
        RecordsSend { records, max_bytes_to_write, remaining: max_bytes_to_write, pending: false }
    }

    /// Whether the send has finished. Mirrors Java's `completed()`.
    pub fn completed(&self) -> bool {
        self.remaining <= 0 && !self.pending
    }

    /// Total bytes the send was sized for. Mirrors Java's `size()`. Java
    /// returns `long` because `Send.size()` is broad — we keep the parameter
    /// type as `i32` since `RecordsSend` itself only deals with `i32` byte
    /// counts. Cast at the network-layer boundary in Phase 5.
    pub fn size(&self) -> i64 {
        self.max_bytes_to_write as i64
    }

    /// Borrow the records. Mirrors Java's `protected T records()` —
    /// exposed `pub(crate)` because subclasses (e.g. `DefaultRecordsSend`)
    /// in the same module tree need to forward writes to them.
    pub fn records(&self) -> &R {
        &self.records
    }

    /// Maximum bytes the send is configured to write. Mirrors Java's
    /// `maxBytesToWrite()` (used by `DefaultRecordsSend.writeTo` indirectly
    /// via `RecordsSend.writeTo`'s `maxBytesToWrite - remaining`).
    pub fn max_bytes_to_write(&self) -> i32 {
        self.max_bytes_to_write
    }

    /// Bytes still to be written.
    pub fn remaining(&self) -> i32 {
        self.remaining
    }

    /// Whether there are pending writes flushed by the channel buffer.
    pub fn pending(&self) -> bool {
        self.pending
    }

    /// Mutator: advance the bookkeeping by `written` bytes. Used by the
    /// Phase 5 `write_to(channel)` loop after each call to the subclass's
    /// `write_to(channel, previously_written, remaining)`. Java does this
    /// inline in `RecordsSend.writeTo`; we expose it as a method so Phase 5
    /// can compose without having to reach into private fields.
    pub fn advance(&mut self, written: i32) {
        self.remaining -= written;
    }

    /// Mutator: update the `pending` flag (driven from
    /// `channel.hasPendingWrites()` in Java).
    pub fn set_pending(&mut self, pending: bool) {
        self.pending = pending;
    }
}

#[cfg(test)]
mod tests {
    //! Java has no dedicated `RecordsSendTest`; the class is exercised
    //! transitively through `MultiRecordsSendTest` (Phase 5 scope) and the
    //! integration suite. We unit-test the state-tracking core here.

    use super::*;
    use crate::common::record::UnalignedMemoryRecords;

    #[test]
    fn new_initializes_remaining_and_size() {
        let records = UnalignedMemoryRecords::from_vec(vec![0u8; 100]);
        let send = RecordsSend::new(records, 100);
        assert_eq!(send.remaining(), 100);
        assert_eq!(send.size(), 100);
        assert_eq!(send.max_bytes_to_write(), 100);
        assert!(!send.completed(), "fresh send is not yet completed");
        assert!(!send.pending());
    }

    #[test]
    fn completed_when_remaining_is_zero_and_no_pending() {
        let records = UnalignedMemoryRecords::from_vec(vec![0u8; 50]);
        let mut send = RecordsSend::new(records, 50);
        send.advance(50);
        assert!(send.completed());
    }

    #[test]
    fn not_completed_when_pending_even_if_remaining_is_zero() {
        let records = UnalignedMemoryRecords::from_vec(vec![0u8; 50]);
        let mut send = RecordsSend::new(records, 50);
        send.advance(50);
        send.set_pending(true);
        assert!(!send.completed());
        send.set_pending(false);
        assert!(send.completed());
    }

    #[test]
    fn advance_partial_writes() {
        let records = UnalignedMemoryRecords::from_vec(vec![0u8; 100]);
        let mut send = RecordsSend::new(records, 100);
        send.advance(30);
        assert_eq!(send.remaining(), 70);
        assert!(!send.completed());
        send.advance(70);
        assert_eq!(send.remaining(), 0);
        assert!(send.completed());
    }

    #[test]
    fn records_borrow_returns_inner() {
        let payload = vec![1u8, 2, 3, 4, 5];
        let records = UnalignedMemoryRecords::from_vec(payload.clone());
        let send = RecordsSend::new(records, 5);
        assert_eq!(send.records().buffer().as_ref(), payload.as_slice());
    }
}
