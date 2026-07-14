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

//! Error carrying the offsets affected by a deserialization failure within a
//! share in-flight batch (KIP-932).
//!
//! Corresponds to
//! `org.apache.kafka.clients.consumer.internals.ShareInFlightBatchException`.

// Phase 2 (M9) lands this error type; the share fetch path
// (`ShareCompletedFetch` / `ShareConsumeRequestManager`) that produces and
// consumes it arrives in a later phase.
#![allow(dead_code)]

use std::collections::HashSet;

use crate::common::KafkaError;

/// Wraps the underlying [`KafkaError`] cause together with the set of offsets
/// affected by a deserialization failure in a share in-flight batch.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareInFlightBatchException`,
/// which in Java `extends SerializationException`. It is used as a carrier
/// (cause + offsets) by [`ShareInFlightBatch`](super::share_in_flight_batch),
/// so the Rust translation models it as a dedicated struct implementing
/// [`std::error::Error`] rather than a variant of the shared `KafkaError`
/// enum.
#[derive(Clone, Debug)]
pub(crate) struct ShareInFlightBatchException {
    cause: KafkaError,
    offsets: HashSet<i64>,
}

impl ShareInFlightBatchException {
    /// Constructs a new exception carrying the given cause and offsets.
    ///
    /// Mirrors Java's
    /// `ShareInFlightBatchException(KafkaException cause, Set<Long> offsets)`.
    pub(crate) fn new(cause: KafkaError, offsets: HashSet<i64>) -> Self {
        Self { cause, offsets }
    }

    /// Returns the underlying cause. Mirrors Java's `KafkaException cause()`.
    pub(crate) fn cause(&self) -> &KafkaError {
        &self.cause
    }

    /// Returns the offsets affected by the failure. Mirrors Java's
    /// `Set<Long> offsets()`.
    pub(crate) fn offsets(&self) -> &HashSet<i64> {
        &self.offsets
    }
}

impl std::fmt::Display for ShareInFlightBatchException {
    /// Java's constructor does not set a message on the parent
    /// `SerializationException` (its `getMessage()` is `null`); the Rust
    /// `Display` delegates to the cause so the error remains informative.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.cause)
    }
}

impl std::error::Error for ShareInFlightBatchException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

#[cfg(test)]
mod tests {
    // No dedicated Java test exists for `ShareInFlightBatchException`; its
    // behaviour is exercised by `ShareConsumeRequestManagerTest` (translated
    // in a later phase). These smoke tests cover the accessors.
    use super::*;
    use crate::common::protocol::errors::Errors;

    #[test]
    fn test_accessors() {
        let cause = KafkaError::new(Errors::InvalidRecordState);
        let offsets: HashSet<i64> = [1, 2, 3].into_iter().collect();
        let exception = ShareInFlightBatchException::new(cause, offsets.clone());
        assert_eq!(exception.cause().error(), Errors::InvalidRecordState);
        assert_eq!(exception.offsets(), &offsets);
    }
}
