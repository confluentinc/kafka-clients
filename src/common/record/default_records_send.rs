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

//! Translation of `org.apache.kafka.common.record.DefaultRecordsSend`.

use crate::common::record::TransferableRecords;
use crate::common::record::records_send::RecordsSend;

/// Concrete `RecordsSend` for any [`TransferableRecords`]. Java's class
/// extends `RecordsSend<T extends TransferableRecords>` and implements the
/// abstract `writeTo(channel, previouslyWritten, remaining)` by delegating
/// to `records().writeTo(channel, previouslyWritten, remaining)`.
///
/// ## Phase 3d-3 scope
///
/// The actual `writeTo(channel, ...)` body lives on
/// `TransferableRecords::write_to(channel, position, length)`, which is
/// deferred to Phase 5 (the `TransferableChannel` trait it requires lives
/// in `common/network/*`).
///
/// What lands here is the typed wrapper that owns a `RecordsSend<R>` and
/// exposes the public surface (`new`, `new_with_size`, `completed`, `size`,
/// `records`, `advance`, `set_pending`).
#[derive(Debug)]
pub struct DefaultRecordsSend<R: TransferableRecords> {
    inner: RecordsSend<R>,
}

impl<R: TransferableRecords> DefaultRecordsSend<R> {
    /// Construct a send sized to the records' full size. Mirrors Java's
    /// `DefaultRecordsSend(T)`.
    pub fn new(records: R) -> Self {
        let size = records.size_in_bytes();
        DefaultRecordsSend::with_max_bytes(records, size)
    }

    /// Construct a send with an explicit byte cap. Mirrors Java's
    /// `DefaultRecordsSend(T, int)`.
    pub fn with_max_bytes(records: R, max_bytes_to_write: i32) -> Self {
        DefaultRecordsSend { inner: RecordsSend::new(records, max_bytes_to_write) }
    }

    /// Whether the send has finished. Mirrors Java's `completed()` (inherited
    /// from `RecordsSend`).
    pub fn completed(&self) -> bool {
        self.inner.completed()
    }

    /// Total bytes the send was sized for. Mirrors Java's `size()`.
    pub fn size(&self) -> i64 {
        self.inner.size()
    }

    /// Borrow the inner records. Mirrors Java's protected `records()`.
    pub fn records(&self) -> &R {
        self.inner.records()
    }

    /// Bytes still to be written.
    pub fn remaining(&self) -> i32 {
        self.inner.remaining()
    }

    /// Whether the channel reports pending buffered writes.
    pub fn pending(&self) -> bool {
        self.inner.pending()
    }

    /// Advance the write bookkeeping by `written` bytes. Phase 5 will call
    /// this from the `write_to(channel)` loop.
    pub fn advance(&mut self, written: i32) {
        self.inner.advance(written);
    }

    /// Update the `pending` flag.
    pub fn set_pending(&mut self, pending: bool) {
        self.inner.set_pending(pending);
    }
}

#[cfg(test)]
mod tests {
    //! Java has no dedicated `DefaultRecordsSendTest`. We exercise the
    //! state-tracking surface here against [`UnalignedMemoryRecords`] (a
    //! concrete `TransferableRecords` impl).

    use super::*;
    use crate::common::record::UnalignedMemoryRecords;

    #[test]
    fn new_uses_records_size_in_bytes() {
        let records = UnalignedMemoryRecords::from_vec(vec![0u8; 256]);
        let send = DefaultRecordsSend::new(records);
        assert_eq!(send.size(), 256);
        assert_eq!(send.remaining(), 256);
    }

    #[test]
    fn with_max_bytes_caps_at_supplied_value() {
        let records = UnalignedMemoryRecords::from_vec(vec![0u8; 256]);
        let send = DefaultRecordsSend::with_max_bytes(records, 100);
        assert_eq!(send.size(), 100);
        assert_eq!(send.remaining(), 100);
    }

    #[test]
    fn advance_completes_when_remaining_zero() {
        let records = UnalignedMemoryRecords::from_vec(vec![0u8; 64]);
        let mut send = DefaultRecordsSend::new(records);
        assert!(!send.completed());
        send.advance(64);
        assert!(send.completed());
    }

    #[test]
    fn pending_blocks_completion() {
        let records = UnalignedMemoryRecords::from_vec(vec![0u8; 32]);
        let mut send = DefaultRecordsSend::new(records);
        send.advance(32);
        send.set_pending(true);
        assert!(!send.completed());
        send.set_pending(false);
        assert!(send.completed());
    }
}
