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

//! A contiguous batch of record acknowledgements (KIP-932).
//!
//! Corresponds to `org.apache.kafka.clients.consumer.internals.AcknowledgementBatch`.

// Phase 1 (M9) translates the share wire/session layer; the share request
// managers that consume these types land in a later phase. Until then, some
// items are exercised only by tests (same precedent as `fetch_session_handler`).
#![allow(dead_code)]

use crate::share_acknowledge_request_data::AcknowledgementBatch as ShareAcknowledgeAckBatch;
use crate::share_fetch_request_data::AcknowledgementBatch as ShareFetchAckBatch;

/// A batch of acknowledgements over a contiguous range of offsets.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.AcknowledgementBatch`.
#[derive(Clone, Debug, Default)]
pub(crate) struct AcknowledgementBatch {
    first_offset: i64,
    last_offset: i64,
    acknowledge_types: Vec<i8>,
}

impl AcknowledgementBatch {
    /// Creates an empty batch with offsets set to `0` and no acknowledge types.
    pub(crate) fn new() -> Self {
        Self { first_offset: 0, last_offset: 0, acknowledge_types: Vec::new() }
    }

    /// Returns the first offset in the batch.
    pub(crate) fn first_offset(&self) -> i64 {
        self.first_offset
    }

    /// Returns the last (inclusive) offset in the batch.
    pub(crate) fn last_offset(&self) -> i64 {
        self.last_offset
    }

    /// Returns a reference to the acknowledge type ids.
    pub(crate) fn acknowledge_types(&self) -> &Vec<i8> {
        &self.acknowledge_types
    }

    /// Returns a mutable reference to the acknowledge type ids.
    pub(crate) fn acknowledge_types_mut(&mut self) -> &mut Vec<i8> {
        &mut self.acknowledge_types
    }

    /// Sets the first offset, returning `self` for chaining.
    pub(crate) fn set_first_offset(&mut self, v: i64) -> &mut Self {
        self.first_offset = v;
        self
    }

    /// Sets the last offset, returning `self` for chaining.
    pub(crate) fn set_last_offset(&mut self, v: i64) -> &mut Self {
        self.last_offset = v;
        self
    }

    /// Sets the acknowledge types, returning `self` for chaining.
    pub(crate) fn set_acknowledge_types(&mut self, v: Vec<i8>) -> &mut Self {
        self.acknowledge_types = v;
        self
    }

    /// Converts this batch into the wire form used by `ShareAcknowledgeRequest`.
    ///
    /// Corresponds to Java's `toShareAcknowledgeRequest()`.
    pub(crate) fn to_share_acknowledge_request(&self) -> ShareAcknowledgeAckBatch {
        let mut batch = ShareAcknowledgeAckBatch::new();
        batch
            .set_first_offset(self.first_offset)
            .set_last_offset(self.last_offset)
            .set_acknowledge_types(self.acknowledge_types.clone());
        batch
    }

    /// Converts this batch into the wire form used by `ShareFetchRequest`.
    ///
    /// Corresponds to Java's `toShareFetchRequest()`.
    pub(crate) fn to_share_fetch_request(&self) -> ShareFetchAckBatch {
        let mut batch = ShareFetchAckBatch::new();
        batch
            .set_first_offset(self.first_offset)
            .set_last_offset(self.last_offset)
            .set_acknowledge_types(self.acknowledge_types.clone());
        batch
    }
}

impl PartialEq for AcknowledgementBatch {
    fn eq(&self, other: &Self) -> bool {
        self.first_offset == other.first_offset
            && self.last_offset == other.last_offset
            && self.acknowledge_types == other.acknowledge_types
    }
}

impl Eq for AcknowledgementBatch {}

impl std::fmt::Display for AcknowledgementBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AcknowledgementBatch(firstOffset={}, lastOffset={}, acknowledgeTypes={:?})",
            self.first_offset, self.last_offset, self.acknowledge_types
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setters_and_conversion() {
        let mut b = AcknowledgementBatch::new();
        b.set_first_offset(3).set_last_offset(7).set_acknowledge_types(vec![1, 1, 2]);
        assert_eq!(b.first_offset(), 3);
        assert_eq!(b.last_offset(), 7);
        assert_eq!(b.acknowledge_types(), &vec![1, 1, 2]);

        let fetch = b.to_share_fetch_request();
        assert_eq!(fetch.first_offset, 3);
        assert_eq!(fetch.last_offset, 7);
        assert_eq!(fetch.acknowledge_types, vec![1, 1, 2]);

        let ack = b.to_share_acknowledge_request();
        assert_eq!(ack.first_offset, 3);
        assert_eq!(ack.last_offset, 7);
        assert_eq!(ack.acknowledge_types, vec![1, 1, 2]);
    }

    #[test]
    fn test_equality() {
        let mut a = AcknowledgementBatch::new();
        a.set_first_offset(0).set_last_offset(1).set_acknowledge_types(vec![1]);
        let mut b = AcknowledgementBatch::new();
        b.set_first_offset(0).set_last_offset(1).set_acknowledge_types(vec![1]);
        assert_eq!(a, b);
        b.set_last_offset(2);
        assert_ne!(a, b);
    }
}
