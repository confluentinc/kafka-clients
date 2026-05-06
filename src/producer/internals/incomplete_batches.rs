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

//! Translation of `org.apache.kafka.clients.producer.internals.IncompleteBatches`.
//!
//! A thread-safe helper class to hold batches that haven't been
//! acknowledged yet (including those which have and have not been
//! sent). Java backs this with a `synchronized (incomplete) { ... }`
//! block guarding a `HashSet<ProducerBatch>` that uses default
//! identity-based `equals`/`hashCode`. Rust mirrors the identity
//! semantics with [`std::sync::Arc::as_ptr`] used as the hash key —
//! the underlying [`HashMap`] therefore distinguishes batches by
//! pointer address, not by structural equality. Using a `Mutex` over
//! the map mirrors Java's `synchronized` block exactly.

#![allow(dead_code)] // Phase 6d (RecordAccumulator) wires this set.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use super::produce_request_result::ProduceRequestResult;
use super::producer_batch::ProducerBatch;

/// See module-level docs.
pub(crate) struct IncompleteBatches {
    /// Map keyed by the raw pointer value of each batch's Arc, so we
    /// can replicate Java's `HashSet<ProducerBatch>` identity-equality
    /// semantics. The map's value is the same Arc, retaining the batch
    /// alive while it is incomplete.
    incomplete: Mutex<HashMap<usize, Arc<ProducerBatch>>>,
}

impl IncompleteBatches {
    pub fn new() -> Self {
        IncompleteBatches { incomplete: Mutex::new(HashMap::new()) }
    }

    /// Add a batch to the incomplete set. Mirrors Java's `add`.
    pub fn add(&self, batch: Arc<ProducerBatch>) {
        let key = Arc::as_ptr(&batch) as usize;
        self.incomplete.lock().unwrap().insert(key, batch);
    }

    /// Remove a batch from the incomplete set. Panics with
    /// `IllegalStateException`-equivalent message if the batch is not
    /// present, matching Java's `if (!removed) throw IllegalStateException`.
    pub fn remove(&self, batch: &Arc<ProducerBatch>) {
        let key = Arc::as_ptr(batch) as usize;
        let removed = self.incomplete.lock().unwrap().remove(&key);
        assert!(
            removed.is_some(),
            "Remove from the incomplete set failed. This should be impossible."
        );
    }

    /// Return a snapshot copy of all incomplete batches. Mirrors
    /// `copyAll` returning `Iterable<ProducerBatch>`.
    pub fn copy_all(&self) -> Vec<Arc<ProducerBatch>> {
        self.incomplete.lock().unwrap().values().cloned().collect()
    }

    /// Return the [`ProduceRequestResult`] of every incomplete batch.
    /// Mirrors `requestResults`.
    pub fn request_results(&self) -> Vec<Arc<ProduceRequestResult>> {
        self.incomplete
            .lock()
            .unwrap()
            .values()
            .map(|batch| Arc::clone(batch.produce_future()))
            .collect()
    }

    /// `true` iff there are no incomplete batches.
    pub fn is_empty(&self) -> bool {
        self.incomplete.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    //! There is no dedicated `IncompleteBatchesTest.java`; the class is
    //! exercised through `RecordAccumulatorTest` (translated in Phase
    //! 6d). The smoke tests below verify add/remove/snapshot semantics
    //! against the placeholder `ProducerBatch` type.

    use super::*;
    use crate::common::record::record_batch::NO_TIMESTAMP;
    use crate::common::topic_partition::TopicPartition;

    fn make_batch() -> Arc<ProducerBatch> {
        let result = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        result.set(0, NO_TIMESTAMP, None);
        Arc::new(ProducerBatch::new(result))
    }

    #[test]
    fn add_remove_round_trip() {
        let inc = IncompleteBatches::new();
        assert!(inc.is_empty());
        let b1 = make_batch();
        let b2 = make_batch();
        inc.add(Arc::clone(&b1));
        inc.add(Arc::clone(&b2));
        assert!(!inc.is_empty());
        assert_eq!(2, inc.copy_all().len());
        assert_eq!(2, inc.request_results().len());

        inc.remove(&b1);
        assert_eq!(1, inc.copy_all().len());
        inc.remove(&b2);
        assert!(inc.is_empty());
    }

    #[test]
    #[should_panic(expected = "Remove from the incomplete set failed.")]
    fn remove_missing_panics() {
        let inc = IncompleteBatches::new();
        let b = make_batch();
        inc.remove(&b);
    }

    #[test]
    fn identity_equality_distinguishes_distinct_arcs() {
        // Two batches built from clones of the same underlying
        // ProduceRequestResult must be treated as distinct (matching
        // Java's identity-based HashSet<ProducerBatch>).
        let result = Arc::new(ProduceRequestResult::new(TopicPartition::new("t", 0)));
        result.set(0, NO_TIMESTAMP, None);
        let b1 = Arc::new(ProducerBatch::new(Arc::clone(&result)));
        let b2 = Arc::new(ProducerBatch::new(Arc::clone(&result)));
        let inc = IncompleteBatches::new();
        inc.add(Arc::clone(&b1));
        inc.add(Arc::clone(&b2));
        assert_eq!(2, inc.copy_all().len());
    }
}
