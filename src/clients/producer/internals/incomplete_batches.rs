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

//! A thread-safe helper class to hold batches that haven't been acknowledged yet
//! (including those which have and have not been sent).
//!
//! Translated from `org.apache.kafka.clients.producer.internals.IncompleteBatches`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::clients::producer::internals::produce_request_result::ProduceRequestResult;
use crate::clients::producer::internals::producer_batch::ProducerBatch;

/// A thread-safe helper class to hold batches that haven't been acknowledged yet
/// (including those which have and have not been sent).
///
/// In Java, identity equality (`==`) on `ProducerBatch` is used via `HashSet`.
/// In Rust, we use `Arc<ProducerBatch>` and pointer-based equality/hashing
/// via a newtype wrapper.
pub struct IncompleteBatches {
    /// The set of incomplete batches, keyed by Arc pointer identity.
    incomplete: Mutex<HashSet<ArcBatchKey>>,
}

/// Wrapper around `Arc<ProducerBatch>` that uses pointer-based equality and hashing,
/// matching Java's identity-based `HashSet<ProducerBatch>`.
#[derive(Clone)]
struct ArcBatchKey(Arc<ProducerBatch>);

impl PartialEq for ArcBatchKey {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ArcBatchKey {}

impl std::hash::Hash for ArcBatchKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl IncompleteBatches {
    /// Create a new empty `IncompleteBatches`.
    pub fn new() -> Self {
        Self { incomplete: Mutex::new(HashSet::new()) }
    }

    /// Add a batch to the incomplete set.
    pub fn add(&self, batch: Arc<ProducerBatch>) {
        let mut incomplete = self.incomplete.lock().unwrap();
        incomplete.insert(ArcBatchKey(batch));
    }

    /// Remove a batch from the incomplete set.
    ///
    /// # Panics
    ///
    /// Panics if the batch was not in the set. This matches Java's `IllegalStateException`
    /// thrown for the same case. The panic is intentional because a batch missing from the
    /// incomplete set indicates a logic bug in the producer (the batch was never added or
    /// was removed twice), not a runtime condition that callers can recover from. Java's
    /// own comment says "This should be impossible." Using `panic!` for impossible
    /// invariant violations is idiomatic Rust (see
    /// [`std::vec::Vec::remove`](https://doc.rust-lang.org/std/vec/struct.Vec.html#panics-6)).
    pub fn remove(&self, batch: &Arc<ProducerBatch>) {
        let mut incomplete = self.incomplete.lock().unwrap();
        let removed = incomplete.remove(&ArcBatchKey(Arc::clone(batch)));
        assert!(removed, "Remove from the incomplete set failed. This should be impossible.");
    }

    /// Return a snapshot copy of all incomplete batches.
    pub fn copy_all(&self) -> Vec<Arc<ProducerBatch>> {
        let incomplete = self.incomplete.lock().unwrap();
        incomplete.iter().map(|k| Arc::clone(&k.0)).collect()
    }

    /// Return the [`ProduceRequestResult`] for each incomplete batch.
    pub fn request_results(&self) -> Vec<Arc<ProduceRequestResult>> {
        let incomplete = self.incomplete.lock().unwrap();
        incomplete.iter().map(|k| Arc::clone(&k.0.produce_future)).collect()
    }

    /// Check if there are no incomplete batches.
    pub fn is_empty(&self) -> bool {
        let incomplete = self.incomplete.lock().unwrap();
        incomplete.is_empty()
    }
}

impl Default for IncompleteBatches {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::topic_partition::TopicPartition;

    fn make_batch(topic: &str, partition: i32) -> Arc<ProducerBatch> {
        let tp = TopicPartition::new(topic.to_string(), partition);
        let result = Arc::new(ProduceRequestResult::new(tp));
        Arc::new(ProducerBatch { produce_future: result })
    }

    #[test]
    fn test_add_and_remove() {
        let batches = IncompleteBatches::new();
        assert!(batches.is_empty());

        let batch1 = make_batch("topic", 0);
        let batch2 = make_batch("topic", 1);

        batches.add(Arc::clone(&batch1));
        batches.add(Arc::clone(&batch2));
        assert!(!batches.is_empty());

        batches.remove(&batch1);
        batches.remove(&batch2);
        assert!(batches.is_empty());
    }

    #[test]
    #[should_panic(expected = "Remove from the incomplete set failed")]
    fn test_remove_not_present_panics() {
        let batches = IncompleteBatches::new();
        let batch = make_batch("topic", 0);
        batches.remove(&batch);
    }

    #[test]
    fn test_copy_all() {
        let batches = IncompleteBatches::new();
        let batch1 = make_batch("topic", 0);
        let batch2 = make_batch("topic", 1);

        batches.add(Arc::clone(&batch1));
        batches.add(Arc::clone(&batch2));

        let all = batches.copy_all();
        assert_eq!(2, all.len());
    }

    #[test]
    fn test_request_results() {
        let batches = IncompleteBatches::new();
        let batch1 = make_batch("topic", 0);
        let batch2 = make_batch("topic", 1);

        batches.add(Arc::clone(&batch1));
        batches.add(Arc::clone(&batch2));

        let results = batches.request_results();
        assert_eq!(2, results.len());
    }

    #[test]
    fn test_identity_based_equality() {
        let batches = IncompleteBatches::new();
        let batch = make_batch("topic", 0);

        // Add the same Arc twice — should only appear once
        batches.add(Arc::clone(&batch));
        batches.add(Arc::clone(&batch));

        let all = batches.copy_all();
        assert_eq!(1, all.len(), "Same Arc added twice should only appear once");
    }

    #[test]
    fn test_different_batches_same_partition() {
        let batches = IncompleteBatches::new();
        // Two different ProducerBatch instances for the same partition
        let batch1 = make_batch("topic", 0);
        let batch2 = make_batch("topic", 0);

        batches.add(Arc::clone(&batch1));
        batches.add(Arc::clone(&batch2));

        let all = batches.copy_all();
        assert_eq!(2, all.len(), "Different Arcs for same partition should be separate entries");
    }
}
