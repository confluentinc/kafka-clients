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

/// A thread-safe helper class to hold batches that haven't been acknowledged yet
/// (including those which have and have not been sent).
///
/// In Java, identity equality (`==`) on `ProducerBatch` is used via `HashSet`.
/// In Rust, since batches in deques are not `Arc`-wrapped, we track
/// `Arc<ProduceRequestResult>` using pointer-based equality. This is sufficient
/// because each `ProducerBatch` has a unique `produce_future` and the primary
/// uses of `IncompleteBatches` are:
/// - `request_results()` for `awaitFlushCompletion`
/// - `is_empty()` for `hasIncomplete`
pub struct IncompleteBatches {
    /// The set of incomplete produce futures, keyed by Arc pointer identity.
    incomplete: Mutex<HashSet<ArcResultKey>>,
}

/// Wrapper around `Arc<ProduceRequestResult>` that uses pointer-based equality and hashing,
/// matching Java's identity-based `HashSet<ProducerBatch>`.
#[derive(Clone)]
struct ArcResultKey(Arc<ProduceRequestResult>);

impl PartialEq for ArcResultKey {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ArcResultKey {}

impl std::hash::Hash for ArcResultKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl IncompleteBatches {
    /// Create a new empty `IncompleteBatches`.
    pub fn new() -> Self {
        Self { incomplete: Mutex::new(HashSet::new()) }
    }

    /// Add a batch's produce future to the incomplete set.
    pub fn add(&self, produce_future: Arc<ProduceRequestResult>) {
        let mut incomplete = self.incomplete.lock().unwrap();
        incomplete.insert(ArcResultKey(produce_future));
    }

    /// Remove a batch's produce future from the incomplete set.
    pub fn remove(&self, produce_future: &Arc<ProduceRequestResult>) {
        let mut incomplete = self.incomplete.lock().unwrap();
        let removed = incomplete.remove(&ArcResultKey(Arc::clone(produce_future)));
        assert!(removed, "Remove from the incomplete set failed. This should be impossible.");
    }

    /// Return a snapshot copy of all incomplete produce futures.
    pub fn copy_all(&self) -> Vec<Arc<ProduceRequestResult>> {
        let incomplete = self.incomplete.lock().unwrap();
        incomplete.iter().map(|k| Arc::clone(&k.0)).collect()
    }

    /// Return the [`ProduceRequestResult`] for each incomplete batch.
    pub fn request_results(&self) -> Vec<Arc<ProduceRequestResult>> {
        self.copy_all()
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

    fn make_future(topic: &str, partition: i32) -> Arc<ProduceRequestResult> {
        let tp = TopicPartition::new(topic.to_string(), partition);
        Arc::new(ProduceRequestResult::new(tp))
    }

    #[test]
    fn test_add_and_remove() {
        let batches = IncompleteBatches::new();
        assert!(batches.is_empty());

        let future1 = make_future("topic", 0);
        let future2 = make_future("topic", 1);

        batches.add(Arc::clone(&future1));
        batches.add(Arc::clone(&future2));
        assert!(!batches.is_empty());

        batches.remove(&future1);
        batches.remove(&future2);
        assert!(batches.is_empty());
    }

    #[test]
    #[should_panic(expected = "Remove from the incomplete set failed")]
    fn test_remove_not_present_panics() {
        let batches = IncompleteBatches::new();
        let future = make_future("topic", 0);
        batches.remove(&future);
    }

    #[test]
    fn test_copy_all() {
        let batches = IncompleteBatches::new();
        let future1 = make_future("topic", 0);
        let future2 = make_future("topic", 1);

        batches.add(Arc::clone(&future1));
        batches.add(Arc::clone(&future2));

        let all = batches.copy_all();
        assert_eq!(2, all.len());
    }

    #[test]
    fn test_request_results() {
        let batches = IncompleteBatches::new();
        let future1 = make_future("topic", 0);
        let future2 = make_future("topic", 1);

        batches.add(Arc::clone(&future1));
        batches.add(Arc::clone(&future2));

        let results = batches.request_results();
        assert_eq!(2, results.len());
    }

    #[test]
    fn test_identity_based_equality() {
        let batches = IncompleteBatches::new();
        let future = make_future("topic", 0);

        // Add the same Arc twice — should only appear once
        batches.add(Arc::clone(&future));
        batches.add(Arc::clone(&future));

        let all = batches.copy_all();
        assert_eq!(1, all.len(), "Same Arc added twice should only appear once");
    }

    #[test]
    fn test_different_futures_same_partition() {
        let batches = IncompleteBatches::new();
        // Two different ProduceRequestResult instances for the same partition
        let future1 = make_future("topic", 0);
        let future2 = make_future("topic", 0);

        batches.add(Arc::clone(&future1));
        batches.add(Arc::clone(&future2));

        let all = batches.copy_all();
        assert_eq!(2, all.len(), "Different Arcs for same partition should be separate entries");
    }
}
