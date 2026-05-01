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

//! Translation of `org.apache.kafka.common.TopicPartition`.

use std::fmt;
use std::sync::Arc;

/// A topic name and partition number.
///
/// The topic name is stored as `Arc<str>` so cloning a `TopicPartition` —
/// the producer's `RecordAccumulator` keys its per-partition deque map by
/// `TopicPartition` and clones the key on every send — is a single atomic
/// reference-count bump rather than a string copy. See CLAUDE.md rule 11
/// (hot-path identifier interning).
///
/// Java accepted `null` for the topic name; this Rust translation does
/// not. Producer call sites always have a real topic. Where a "no topic"
/// placeholder is needed (e.g. the `TopicIdPartitionTest` cases that
/// passed `null`), pass an empty `Arc::from("")`.
#[derive(Clone, Debug)]
pub struct TopicPartition {
    topic: Arc<str>,
    partition: i32,
}

impl TopicPartition {
    /// Create a new [`TopicPartition`]. Accepts any value cheaply convertible
    /// to `Arc<str>` (`&str`, `String`, or an existing `Arc<str>`).
    pub fn new(topic: impl Into<Arc<str>>, partition: i32) -> Self {
        Self { topic: topic.into(), partition }
    }

    /// The partition id.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// The topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Borrow the topic as the shared `Arc<str>` so callers building further
    /// keyed structures can share the same allocation.
    pub fn topic_arc(&self) -> &Arc<str> {
        &self.topic
    }
}

impl PartialEq for TopicPartition {
    fn eq(&self, other: &Self) -> bool {
        self.partition == other.partition && *self.topic == *other.topic
    }
}

impl Eq for TopicPartition {}

impl std::hash::Hash for TopicPartition {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hashing through `&str` gives content-based hashing (independent of
        // the Arc's identity), so two `TopicPartition`s built from
        // independently-allocated `Arc<str>` of equal content hash equal —
        // matching `Hash`+`Eq` consistency.
        (*self.topic).hash(state);
        self.partition.hash(state);
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn hash_of<T: Hash>(v: &T) -> u64 {
        let mut h = DefaultHasher::new();
        v.hash(&mut h);
        h.finish()
    }

    #[test]
    fn round_trip_values() {
        // Translation of TopicPartitionTest.testSerializationRoundtrip:
        // Java's Serializable round-trip is irrelevant in Rust, but the
        // intent is to verify `topic()` and `partition()` accessors.
        let topic = "mytopic";
        let part = 5;
        let tp = TopicPartition::new(topic, part);
        assert_eq!(tp.partition(), part);
        assert_eq!(tp.topic(), topic);
    }

    #[test]
    fn equal_when_topic_and_partition_match() {
        let a = TopicPartition::new("foo", 3);
        let b = TopicPartition::new(String::from("foo"), 3);
        assert_eq!(a, b);
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn not_equal_when_topic_differs() {
        let a = TopicPartition::new("foo", 3);
        let b = TopicPartition::new("bar", 3);
        assert_ne!(a, b);
    }

    #[test]
    fn not_equal_when_partition_differs() {
        let a = TopicPartition::new("foo", 3);
        let b = TopicPartition::new("foo", 4);
        assert_ne!(a, b);
    }

    #[test]
    fn display_uses_dash_separator() {
        let tp = TopicPartition::new("topicA", 7);
        assert_eq!(tp.to_string(), "topicA-7");
    }

    #[test]
    fn usable_as_hashmap_key() {
        let mut m: HashMap<TopicPartition, i32> = HashMap::new();
        m.insert(TopicPartition::new("a", 0), 100);
        // Lookup with a freshly-allocated key of equal content.
        assert_eq!(m.get(&TopicPartition::new(String::from("a"), 0)), Some(&100));
    }

    #[test]
    fn cloning_shares_topic_arc() {
        let a = TopicPartition::new("foo", 3);
        let b = a.clone();
        // Same underlying Arc<str> — clone is a refcount bump only.
        assert!(Arc::ptr_eq(a.topic_arc(), b.topic_arc()));
    }
}
