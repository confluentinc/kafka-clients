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

//! `ConsumerShareInFlightBatchError` — the failure a [`ShareInFlightBatch`] carries.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareInFlightBatchException`.
//!
//! [`ShareInFlightBatch`]: super::ShareInFlightBatch

use std::collections::HashSet;

use crate::common::Error;

/// The failure that ended the decoding of a [`ShareInFlightBatch`]: the cause
/// and the offsets of the records it released or rejected, which the
/// application may acknowledge differently
/// (`ShareFetch#acknowledge(String, int, long, AcknowledgeType)`).
///
/// A crate-private carrier, not an [`Error`] variant (ledger gap G9). The
/// `Consumer` prefix is CLAUDE.md §2's for an error outside `common`. Java's
/// class extends `SerializationException` but is never thrown:
/// `ShareCompletedFetch` stores it on the batch
/// (`ShareInFlightBatch#setException`) and `ShareFetch` /
/// `ShareFetchCollector#collect` read it back; only its `cause()` ever
/// reaches the application.
///
/// [`ShareInFlightBatch`]: super::ShareInFlightBatch
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatchException")]
pub(crate) struct ConsumerShareInFlightBatchError {
    cause: Error,
    offsets: HashSet<i64>,
}

impl ConsumerShareInFlightBatchError {
    /// A failure with its cause and the offsets it covers.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatchException#ShareInFlightBatchException"
    )]
    pub(crate) fn new(cause: Error, offsets: HashSet<i64>) -> Self {
        Self { cause, offsets }
    }

    /// The error that ended the decoding.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatchException#cause")]
    pub(crate) fn cause(&self) -> &Error {
        &self.cause
    }

    /// The offsets of the records the failure released or rejected.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareInFlightBatchException#offsets")]
    pub(crate) fn offsets(&self) -> &HashSet<i64> {
        &self.offsets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accessors() {
        let error = ConsumerShareInFlightBatchError::new(Error::kafka_message("boom"), HashSet::from([3, 5]));
        assert_eq!("boom", error.cause().message());
        assert_eq!(&HashSet::from([3, 5]), error.offsets());
    }
}
