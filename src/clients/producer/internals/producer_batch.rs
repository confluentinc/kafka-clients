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

//! Producer batch — a batch of records being accumulated for a single partition.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.ProducerBatch`.
//!
//! This is a minimal stub providing only the fields needed by [`IncompleteBatches`].
//! The full implementation will be completed in Phase 6.

use std::sync::Arc;

use crate::clients::producer::internals::produce_request_result::ProduceRequestResult;

/// A batch of records being accumulated for a single partition.
///
/// Each `ProducerBatch` has a unique identity (by `Arc` address) and contains
/// a [`ProduceRequestResult`] that tracks the batch's completion status.
///
/// This is a minimal stub. The full implementation (Phase 6) will add record
/// accumulation, compression, and serialization.
pub struct ProducerBatch {
    /// The future result of the produce request for this batch.
    pub produce_future: Arc<ProduceRequestResult>,
}
