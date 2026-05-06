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

//! Translation of `org.apache.kafka.clients.producer.internals.ProducerBatch`.
//!
//! **Phase 6a placeholder.** Only the surface needed by
//! [`IncompleteBatches`](super::incomplete_batches::IncompleteBatches) is
//! present in this milestone:
//! - construction from a [`ProduceRequestResult`]
//! - `produce_future()` accessor used by `IncompleteBatches::request_results`
//!
//! Phase 6b fills in the full implementation
//! (`MemoryRecordsBuilder`, `try_append`, `done`, `split`, …). The
//! placeholder is kept compatible with the Java field name
//! (`produceFuture`) so the 6b refill is a strict superset, not a
//! rewrite.

#![allow(dead_code)] // Phase 6b refills with the full `ProducerBatch` implementation.

use std::sync::Arc;

use super::produce_request_result::ProduceRequestResult;

/// Placeholder for the full `ProducerBatch`. See module docs.
pub(crate) struct ProducerBatch {
    produce_future: Arc<ProduceRequestResult>,
}

impl ProducerBatch {
    /// Construct a batch wrapping the given [`ProduceRequestResult`].
    /// Phase 6b will extend this signature with `MemoryRecordsBuilder`
    /// and timing fields.
    pub fn new(produce_future: Arc<ProduceRequestResult>) -> Self {
        ProducerBatch { produce_future }
    }

    /// The shared [`ProduceRequestResult`] this batch produces against.
    /// Mirrors Java's package-private `produceFuture` field accessed by
    /// `IncompleteBatches::requestResults`.
    pub fn produce_future(&self) -> &Arc<ProduceRequestResult> {
        &self.produce_future
    }
}
