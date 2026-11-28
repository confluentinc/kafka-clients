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

//! Insertion-ordered partition-state container used by the consumer fetcher.
//!
//! Translated from `org.apache.kafka.common.internals.PartitionStates`.

use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;

use crate::common::TopicPartition;

/// Insertion-ordered map from [`TopicPartition`] to a generic per-partition
/// state. Used as a building block for fetch requests where partitions must
/// be rotated round-robin for fairness and serialised efficiently by grouping
/// partitions of the same topic together.
///
/// Partitions are moved to the end of the order as they are fetched, and
/// `set()` re-batches the entries by topic so that all partitions of one
/// topic are contiguous in the iteration order (matching Java's behavior —
/// see the Java javadoc for `PartitionStates.set`). As partitions are moved
/// to the end, the same topic may appear more than once; in the optimal case
/// a topic "wraps around" and appears twice.
///
/// Not thread-safe — like Java's `PartitionStates`. The Rust consumer wraps
/// this inside `SubscriptionState`, which is itself wrapped in an
/// `Arc<Mutex<…>>` by the caller (per `consumer-threading.md` §16). Java's
/// thread-safe `size()` field is dropped — there is no Rust call-site that
/// reads `size` without the outer lock.
#[doc(alias = "org.apache.kafka.common.internals.PartitionStates")]
pub(crate) struct PartitionStates<S> {
    // FxHash (non-cryptographic) keyed by `TopicPartition`, which is internal
    // and not attacker-controlled. `IndexMap` preserves insertion order
    // independent of the hasher, so the Java `LinkedHashMap`-equivalent
    // ordering is unaffected — this is a per-lookup CPU win only (Phase 25).
    map: IndexMap<TopicPartition, S, FxBuildHasher>,
}

#[cfg_attr(not(test), expect(dead_code))]
impl<S> PartitionStates<S> {
    /// Create an empty container.
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#PartitionStates")]
    pub(crate) fn new() -> Self {
        Self { map: IndexMap::with_hasher(FxBuildHasher) }
    }

    /// Replace the contents with the given entries, batched by topic.
    ///
    /// Mirrors Java's `set(Map<TopicPartition, S>)`. Partitions in the input
    /// are first grouped by topic in their iteration order (so a given topic
    /// appears once), then inserted into the underlying ordered map in that
    /// batched order. The grouping yields a layout like
    /// `a0, a1, b1, b0, c0, c1` where `a*`, `b*`, `c*` are contiguous.
    ///
    /// Java takes any `Map` and follows its iteration order, which is the
    /// caller's insertion order for the `LinkedHashMap` that
    /// `SubscriptionState#assignFromSubscribed` passes. The entries are taken
    /// in the order the iterator yields them, so an ordered input (an
    /// `IndexMap`) keeps that order and a `HashMap` keeps its own, as in Java.
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#set")]
    pub(crate) fn set(&mut self, partition_to_state: impl IntoIterator<Item = (TopicPartition, S)>) {
        self.map.clear();
        self.update_internal(partition_to_state);
    }

    /// Java's private `update(Map)`.
    fn update_internal(&mut self, partition_to_state: impl IntoIterator<Item = (TopicPartition, S)>) {
        // Java uses a `LinkedHashMap<String, List<TopicPartition>>` keyed by
        // topic to preserve insertion order. In Rust, `IndexMap` provides the
        // same ordered behavior. The states move with their partitions (no
        // clone).
        let mut topic_to_partitions: IndexMap<String, Vec<(TopicPartition, S)>> = IndexMap::new();
        for (tp, state) in partition_to_state {
            topic_to_partitions.entry(tp.topic().to_string()).or_default().push((tp, state));
        }
        for (_, entries) in topic_to_partitions {
            for (tp, state) in entries {
                self.map.insert(tp, state);
            }
        }
    }

    /// Move the entry for `tp` to the end of the iteration order (no-op if
    /// `tp` is not present).
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#moveToEnd")]
    pub(crate) fn move_to_end(&mut self, tp: &TopicPartition) {
        if let Some((idx, _, _)) = self.map.get_full(tp) {
            let last = self.map.len() - 1;
            if idx != last {
                self.map.move_index(idx, last);
            }
        }
    }

    /// Insert or update the state for `tp`. If the entry already exists, its
    /// position in the iteration order is preserved (matches Java's
    /// `LinkedHashMap.put` semantics).
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#update")]
    pub(crate) fn update(&mut self, tp: TopicPartition, state: S) {
        self.map.insert(tp, state);
    }

    /// Insert or update the state for `tp` and move the entry to the end of
    /// the iteration order. If `tp` is not yet present, it is appended at
    /// the end (since `IndexMap::insert` appends new keys).
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#updateAndMoveToEnd")]
    pub(crate) fn update_and_move_to_end(&mut self, tp: TopicPartition, state: S) {
        // Mirrors Java's `remove(tp); put(tp, state)` to ensure the entry ends
        // up at the tail regardless of whether it previously existed.
        self.map.shift_remove(&tp);
        self.map.insert(tp, state);
    }

    /// Remove the entry for `tp`, if any.
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#remove")]
    pub(crate) fn remove(&mut self, tp: &TopicPartition) {
        self.map.shift_remove(tp);
    }

    /// Remove the entry for `tp` and return its state, if any.
    ///
    /// Convenience for callers (e.g. `SubscriptionState::assign_from_user`)
    /// that need to move per-partition state out of the container and
    /// re-insert it into a fresh map — mirroring Java's
    /// `assignment.stateValue(partition)` followed by
    /// `assignment.set(...)` pattern where the same `TopicPartitionState`
    /// object is preserved across the call.
    pub(crate) fn remove_and_take(&mut self, tp: &TopicPartition) -> Option<S> {
        self.map.shift_remove(tp)
    }

    /// Remove all entries.
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#clear")]
    pub(crate) fn clear(&mut self) {
        self.map.clear();
    }

    /// Whether `tp` has an entry.
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#contains")]
    pub(crate) fn contains(&self, tp: &TopicPartition) -> bool {
        self.map.contains_key(tp)
    }

    /// Borrowed state for `tp`, if any.
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#stateValue")]
    pub(crate) fn state_value(&self, tp: &TopicPartition) -> Option<&S> {
        self.map.get(tp)
    }

    /// Mutable state for `tp`, if any.
    pub(crate) fn state_value_mut(&mut self, tp: &TopicPartition) -> Option<&mut S> {
        self.map.get_mut(tp)
    }

    /// Iterator over partitions in insertion order. Mirrors Java's
    /// `partitionSet()`.
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#partitionSet")]
    pub(crate) fn partition_set(&self) -> impl Iterator<Item = &TopicPartition> {
        self.map.keys()
    }

    /// Iterator over state values in insertion order. Mirrors Java's
    /// `stateIterator()`.
    pub(crate) fn state_iter(&self) -> impl Iterator<Item = &S> {
        self.map.values()
    }

    /// Iterator over `(TopicPartition, state)` pairs in insertion order.
    /// Mirrors Java's `forEach(BiConsumer)`.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&TopicPartition, &S)> {
        self.map.iter()
    }

    /// Number of entries.
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#size")]
    pub(crate) fn size(&self) -> usize {
        self.map.len()
    }

    /// Snapshot of state values as a `Vec`. Mirrors Java's
    /// `partitionStateValues()`.
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStates#partitionStateValues")]
    pub(crate) fn partition_state_values(&self) -> Vec<&S> {
        self.map.values().collect()
    }
}

impl<S> Default for PartitionStates<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    //! Translated from `org.apache.kafka.common.internals.PartitionStatesTest`.
    //!
    //! `PartitionStates` is `pub(crate)` so tests live inline (the integration
    //! test crate cannot name the type).

    use super::*;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// Java's `createMap()`, a `LinkedHashMap` in `put` order.
    fn create_map() -> IndexMap<TopicPartition, String> {
        IndexMap::from([
            (tp("foo", 2), "foo 2".to_string()),
            (tp("blah", 2), "blah 2".to_string()),
            (tp("blah", 1), "blah 1".to_string()),
            (tp("baz", 2), "baz 2".to_string()),
            (tp("foo", 0), "foo 0".to_string()),
            (tp("baz", 3), "baz 3".to_string()),
        ])
    }

    /// Java's `checkState`: the partitions, the size and the
    /// `partitionStateMap()` (an ordered map) equal `expected`, in order.
    fn check_state(states: &PartitionStates<String>, expected: &[(TopicPartition, &str)]) {
        let expected_keys: Vec<TopicPartition> = expected.iter().map(|(tp, _)| tp.clone()).collect();
        let actual_keys: Vec<TopicPartition> = states.partition_set().cloned().collect();
        assert_eq!(expected_keys, actual_keys);
        assert_eq!(expected.len(), states.size());
        let actual: Vec<(TopicPartition, &str)> = states.iter().map(|(tp, s)| (tp.clone(), s.as_str())).collect();
        assert_eq!(expected.to_vec(), actual);
    }

    /// Translated from `PartitionStatesTest.testSet`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStatesTest#testSet")]
    fn test_set() {
        let mut states: PartitionStates<String> = PartitionStates::new();
        let map = create_map();
        states.set(map);
        let expected = [
            (tp("foo", 2), "foo 2"),
            (tp("foo", 0), "foo 0"),
            (tp("blah", 2), "blah 2"),
            (tp("blah", 1), "blah 1"),
            (tp("baz", 2), "baz 2"),
            (tp("baz", 3), "baz 3"),
        ];
        check_state(&states, &expected);

        states.set(IndexMap::new());
        check_state(&states, &[]);
    }

    /// Translated from `PartitionStatesTest.testMoveToEnd`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStatesTest#testMoveToEnd")]
    fn test_move_to_end() {
        let mut states: PartitionStates<String> = PartitionStates::new();
        let map = create_map();
        states.set(map);

        states.move_to_end(&tp("baz", 2));
        let expected = [
            (tp("foo", 2), "foo 2"),
            (tp("foo", 0), "foo 0"),
            (tp("blah", 2), "blah 2"),
            (tp("blah", 1), "blah 1"),
            (tp("baz", 3), "baz 3"),
            (tp("baz", 2), "baz 2"),
        ];
        check_state(&states, &expected);

        states.move_to_end(&tp("foo", 2));
        let expected = [
            (tp("foo", 0), "foo 0"),
            (tp("blah", 2), "blah 2"),
            (tp("blah", 1), "blah 1"),
            (tp("baz", 3), "baz 3"),
            (tp("baz", 2), "baz 2"),
            (tp("foo", 2), "foo 2"),
        ];
        check_state(&states, &expected);

        // no-op
        states.move_to_end(&tp("foo", 2));
        check_state(&states, &expected);

        // partition doesn't exist
        states.move_to_end(&tp("baz", 5));
        check_state(&states, &expected);

        // topic doesn't exist
        states.move_to_end(&tp("aaa", 2));
        check_state(&states, &expected);
    }

    /// Translated from `PartitionStatesTest.testUpdateAndMoveToEnd`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStatesTest#testUpdateAndMoveToEnd")]
    fn test_update_and_move_to_end() {
        let mut states: PartitionStates<String> = PartitionStates::new();
        let map = create_map();
        states.set(map);

        states.update_and_move_to_end(tp("foo", 0), "foo 0 updated".to_string());
        let expected = [
            (tp("foo", 2), "foo 2"),
            (tp("blah", 2), "blah 2"),
            (tp("blah", 1), "blah 1"),
            (tp("baz", 2), "baz 2"),
            (tp("baz", 3), "baz 3"),
            (tp("foo", 0), "foo 0 updated"),
        ];
        check_state(&states, &expected);

        states.update_and_move_to_end(tp("baz", 2), "baz 2 updated".to_string());
        let expected = [
            (tp("foo", 2), "foo 2"),
            (tp("blah", 2), "blah 2"),
            (tp("blah", 1), "blah 1"),
            (tp("baz", 3), "baz 3"),
            (tp("foo", 0), "foo 0 updated"),
            (tp("baz", 2), "baz 2 updated"),
        ];
        check_state(&states, &expected);

        // partition doesn't exist
        states.update_and_move_to_end(tp("baz", 5), "baz 5 new".to_string());
        let expected = [
            (tp("foo", 2), "foo 2"),
            (tp("blah", 2), "blah 2"),
            (tp("blah", 1), "blah 1"),
            (tp("baz", 3), "baz 3"),
            (tp("foo", 0), "foo 0 updated"),
            (tp("baz", 2), "baz 2 updated"),
            (tp("baz", 5), "baz 5 new"),
        ];
        check_state(&states, &expected);

        // topic doesn't exist
        states.update_and_move_to_end(tp("aaa", 2), "aaa 2 new".to_string());
        let expected = [
            (tp("foo", 2), "foo 2"),
            (tp("blah", 2), "blah 2"),
            (tp("blah", 1), "blah 1"),
            (tp("baz", 3), "baz 3"),
            (tp("foo", 0), "foo 0 updated"),
            (tp("baz", 2), "baz 2 updated"),
            (tp("baz", 5), "baz 5 new"),
            (tp("aaa", 2), "aaa 2 new"),
        ];
        check_state(&states, &expected);
    }

    /// Translated from `PartitionStatesTest.testPartitionValues`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStatesTest#testPartitionValues")]
    fn test_partition_values() {
        let mut states: PartitionStates<String> = PartitionStates::new();
        let map = create_map();
        states.set(map);
        let expected = vec!["foo 2", "foo 0", "blah 2", "blah 1", "baz 2", "baz 3"];
        let actual: Vec<&str> = states.partition_state_values().into_iter().map(String::as_str).collect();
        assert_eq!(expected, actual);
    }

    /// Translated from `PartitionStatesTest.testClear`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStatesTest#testClear")]
    fn test_clear() {
        let mut states: PartitionStates<String> = PartitionStates::new();
        let map = create_map();
        states.set(map);
        states.clear();
        check_state(&states, &[]);
    }

    /// Translated from `PartitionStatesTest.testRemove`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.internals.PartitionStatesTest#testRemove")]
    fn test_remove() {
        let mut states: PartitionStates<String> = PartitionStates::new();
        let map = create_map();
        states.set(map);

        states.remove(&tp("foo", 2));
        let expected = [
            (tp("foo", 0), "foo 0"),
            (tp("blah", 2), "blah 2"),
            (tp("blah", 1), "blah 1"),
            (tp("baz", 2), "baz 2"),
            (tp("baz", 3), "baz 3"),
        ];
        check_state(&states, &expected);

        states.remove(&tp("blah", 1));
        let expected = [
            (tp("foo", 0), "foo 0"),
            (tp("blah", 2), "blah 2"),
            (tp("baz", 2), "baz 2"),
            (tp("baz", 3), "baz 3"),
        ];
        check_state(&states, &expected);

        states.remove(&tp("baz", 3));
        let expected = [
            (tp("foo", 0), "foo 0"),
            (tp("blah", 2), "blah 2"),
            (tp("baz", 2), "baz 2"),
        ];
        check_state(&states, &expected);
    }

    /// Sanity: iteration order is stable across calls and consistent across
    /// `partition_set` / `state_iter` / `iter`.
    #[test]
    fn test_iter_consistency() {
        let mut states: PartitionStates<String> = PartitionStates::new();
        states.set(create_map());

        let from_keys: Vec<TopicPartition> = states.partition_set().cloned().collect();
        let from_iter: Vec<TopicPartition> = states.iter().map(|(tp, _)| tp.clone()).collect();
        assert_eq!(from_keys, from_iter);

        let values_from_iter: Vec<String> = states.iter().map(|(_, s)| s.clone()).collect();
        let values_from_state_iter: Vec<String> = states.state_iter().cloned().collect();
        assert_eq!(values_from_iter, values_from_state_iter);
    }

    /// Sanity: `update` on a new key appends, on an existing key keeps the
    /// position.
    #[test]
    fn test_update_in_place() {
        let mut states: PartitionStates<String> = PartitionStates::new();
        states.set(create_map());
        let position_before: Vec<TopicPartition> = states.partition_set().cloned().collect();

        let tp_foo_0 = tp("foo", 0);
        states.update(tp_foo_0.clone(), "foo 0 updated".to_string());
        let position_after: Vec<TopicPartition> = states.partition_set().cloned().collect();
        assert_eq!(position_before, position_after, "in-place update must preserve position");
        assert_eq!(states.state_value(&tp_foo_0), Some(&"foo 0 updated".to_string()));

        // New key — appended at the end.
        let tp_new = tp("new", 0);
        states.update(tp_new.clone(), "new".to_string());
        let keys: Vec<TopicPartition> = states.partition_set().cloned().collect();
        assert_eq!(keys.last(), Some(&tp_new));
    }

    /// Sanity: `contains`, `state_value`, `state_value_mut`.
    #[test]
    fn test_accessors() {
        let mut states: PartitionStates<String> = PartitionStates::new();
        states.set(create_map());

        let tp_foo_0 = tp("foo", 0);
        assert!(states.contains(&tp_foo_0));
        assert!(!states.contains(&tp("missing", 0)));

        assert_eq!(states.state_value(&tp_foo_0), Some(&"foo 0".to_string()));
        assert_eq!(states.state_value(&tp("missing", 0)), None);

        if let Some(s) = states.state_value_mut(&tp_foo_0) {
            *s = "mutated".to_string();
        }
        assert_eq!(states.state_value(&tp_foo_0), Some(&"mutated".to_string()));
    }
}
