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

//! Translation of `org.apache.kafka.common.TopicIdPartition`.

use std::fmt;
use std::sync::Arc;

use crate::common::{TopicPartition, Uuid};

/// Universally unique identifier with topic id for a topic partition. Makes
/// sure topics recreated with the same name always have unique topic
/// identifiers.
///
/// Like [`TopicPartition`], the topic name is stored as part of the inner
/// `TopicPartition` whose topic field is `Arc<str>` (CLAUDE.md rule 11).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicIdPartition {
    topic_id: Uuid,
    topic_partition: TopicPartition,
}

impl TopicIdPartition {
    /// Create from an explicit topic id and topic partition.
    pub fn new(topic_id: Uuid, topic_partition: TopicPartition) -> Self {
        Self { topic_id, topic_partition }
    }

    /// Create from an explicit topic id, partition number, and topic name.
    pub fn from_parts(topic_id: Uuid, partition: i32, topic: impl Into<Arc<str>>) -> Self {
        Self { topic_id, topic_partition: TopicPartition::new(topic, partition) }
    }

    /// Universally unique id representing this topic partition.
    pub fn topic_id(&self) -> Uuid {
        self.topic_id
    }

    /// The topic name.
    pub fn topic(&self) -> &str {
        self.topic_partition.topic()
    }

    /// The partition id.
    pub fn partition(&self) -> i32 {
        self.topic_partition.partition()
    }

    /// Topic partition representing this instance.
    pub fn topic_partition(&self) -> &TopicPartition {
        &self.topic_partition
    }
}

impl fmt::Display for TopicIdPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}-{}", self.topic_id, self.topic(), self.partition())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic_id_0() -> Uuid {
        Uuid::new(-4_883_993_789_924_556_279_i64, -5_960_309_683_534_398_572_i64)
    }

    fn topic_id_1() -> Uuid {
        Uuid::new(7_759_286_116_672_424_028_i64, -5_081_215_629_859_775_948_i64)
    }

    const TOPIC_NAME_0: &str = "a_topic_name";
    const TOPIC_NAME_1: &str = "another_topic_name";
    const PARTITION_1: i32 = 1;

    #[test]
    fn equals_matches_java_test() {
        let tp0 = TopicPartition::new(TOPIC_NAME_0, PARTITION_1);
        let a = TopicIdPartition::new(topic_id_0(), tp0.clone());
        let b = TopicIdPartition::from_parts(topic_id_0(), PARTITION_1, TOPIC_NAME_0);
        assert_eq!(a, b);
        assert_eq!(b, a);

        // Java used `null` topic; our translation uses empty `""` because
        // `Arc<str>` is non-null. The semantics ("no topic name") are
        // preserved.
        let null_topic_a = TopicIdPartition::from_parts(topic_id_0(), PARTITION_1, "");
        let null_topic_b = TopicIdPartition::new(topic_id_0(), TopicPartition::new("", PARTITION_1));
        assert_eq!(null_topic_a, null_topic_b);

        let c = TopicIdPartition::from_parts(topic_id_1(), PARTITION_1, TOPIC_NAME_1);
        assert_ne!(a, c);
        assert_ne!(c, a);
        assert_ne!(a, null_topic_a);

        let null_topic_c = TopicIdPartition::new(topic_id_1(), TopicPartition::new("", PARTITION_1));
        assert_ne!(null_topic_a, null_topic_c);
    }

    fn hash_of(t: &TopicIdPartition) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        t.hash(&mut h);
        h.finish()
    }

    #[test]
    fn hash_code_matches_for_equal_keys() {
        // Translation of TopicIdPartitionTest.testHashCode — only the
        // "equal hash for equal keys" half is portable; Rust's hasher does
        // not match `Objects.hash` byte-for-byte and we do not try to.
        let a = TopicIdPartition::new(topic_id_0(), TopicPartition::new(TOPIC_NAME_0, PARTITION_1));
        let b = TopicIdPartition::from_parts(topic_id_0(), PARTITION_1, TOPIC_NAME_0);
        assert_eq!(hash_of(&a), hash_of(&b));

        let null_topic_a = TopicIdPartition::from_parts(topic_id_0(), PARTITION_1, "");
        let null_topic_b = TopicIdPartition::new(topic_id_0(), TopicPartition::new("", PARTITION_1));
        assert_eq!(hash_of(&null_topic_a), hash_of(&null_topic_b));

        let c = TopicIdPartition::from_parts(topic_id_1(), PARTITION_1, TOPIC_NAME_1);
        assert_ne!(hash_of(&a), hash_of(&c));
        assert_ne!(hash_of(&a), hash_of(&null_topic_a));

        let null_topic_c = TopicIdPartition::new(topic_id_1(), TopicPartition::new("", PARTITION_1));
        assert_ne!(hash_of(&null_topic_a), hash_of(&null_topic_c));
    }

    #[test]
    fn to_string_format() {
        let a = TopicIdPartition::new(topic_id_0(), TopicPartition::new(TOPIC_NAME_0, PARTITION_1));
        // Java used `null` for empty topic and printed "vDiRhkpVQgmtSLnsAZx7lA:null-1".
        // Rust uses an empty string and prints "vDiRhkpVQgmtSLnsAZx7lA:-1".
        assert_eq!(a.to_string(), "vDiRhkpVQgmtSLnsAZx7lA:a_topic_name-1");

        let empty_topic = TopicIdPartition::from_parts(topic_id_0(), PARTITION_1, "");
        assert_eq!(empty_topic.to_string(), "vDiRhkpVQgmtSLnsAZx7lA:-1");
    }

    #[test]
    fn accessors_round_trip() {
        let a = TopicIdPartition::from_parts(topic_id_0(), 7, "abc");
        assert_eq!(a.topic_id(), topic_id_0());
        assert_eq!(a.partition(), 7);
        assert_eq!(a.topic(), "abc");
        assert_eq!(a.topic_partition(), &TopicPartition::new("abc", 7));
    }
}
