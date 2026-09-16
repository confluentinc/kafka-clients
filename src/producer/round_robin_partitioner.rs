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

//! Round-robin partitioner.
//!
//! Translated from `org.apache.kafka.clients.producer.RoundRobinPartitioner`.

use std::sync::atomic::{AtomicI32, Ordering};

use dashmap::DashMap;

use crate::common::Cluster;
use crate::common::utils::Utils;
use crate::producer::Partitioner;

/// The "Round-Robin" partitioner.
///
/// This partitioning strategy can be used when user wants to distribute the
/// writes to all partitions equally. This is the behaviour regardless of record
/// key hash.
///
/// The record key (if present) is ignored: the partitioner cycles through the
/// topic's partitions in order, choosing only from partitions that currently
/// have a leader (available partitions), so a leaderless partition is skipped.
///
/// A single instance is shared by the producer across tasks, so the per-topic
/// counter is an [`AtomicI32`] inside a concurrent map — the direct translation
/// of Java's `ConcurrentMap<String, AtomicInteger>`.
#[derive(Default)]
pub struct RoundRobinPartitioner {
    /// Per-topic round-robin counter. Java: `topicCounterMap`.
    topic_counter_map: DashMap<String, AtomicI32>,
}

impl RoundRobinPartitioner {
    /// Creates a new `RoundRobinPartitioner`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the next counter value for `topic`, incrementing it.
    ///
    /// Java's `nextValue` uses `computeIfAbsent` + `AtomicInteger.getAndIncrement()`.
    /// Here a `get` fast path avoids taking a shard write-lock once the topic's
    /// counter exists (the steady state, every record after the first); only the
    /// first record for a topic takes the `entry` slow path that allocates the
    /// key. Like Java's `getAndIncrement`, `fetch_add` wraps on overflow, and the
    /// negative wrap is folded back by [`to_positive`] at the call site.
    fn next_value(&self, topic: &str) -> i32 {
        if let Some(counter) = self.topic_counter_map.get(topic) {
            return counter.fetch_add(1, Ordering::Relaxed);
        }
        self.topic_counter_map
            .entry(topic.to_string())
            .or_insert_with(|| AtomicI32::new(0))
            .fetch_add(1, Ordering::Relaxed)
    }
}

impl<K, V> Partitioner<K, V> for RoundRobinPartitioner {
    /// Compute the partition for the given record.
    ///
    /// If there are available (leader-having) partitions for the topic, the next
    /// counter value picks one of them round-robin; otherwise it falls back to
    /// the full partition count. The key/value are ignored.
    ///
    /// # Precondition / panics
    ///
    /// When the topic has **no** partitions at all in `cluster`, the fallback
    /// `Utils::to_positive(next) % 0` divides by zero — Java's identical code throws
    /// `ArithmeticException` there. This is unreachable from
    /// [`KafkaProducer`](crate::producer::KafkaProducer): `wait_on_metadata`
    /// guarantees the topic has partitions before `partition` is called. Per
    /// CLAUDE.md §10.1 this class of panic (division by zero) is acceptable, so
    /// the `i32` return type is preserved rather than made fallible.
    fn partition(
        &self,
        topic: &str,
        _key: Option<&K>,
        _key_bytes: Option<&[u8]>,
        _value: Option<&V>,
        _value_bytes: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32 {
        let next_value = self.next_value(topic);
        let available_partitions = cluster.available_partitions_for_topic(topic);
        if !available_partitions.is_empty() {
            let part = Utils::to_positive(next_value) as usize % available_partitions.len();
            available_partitions[part].partition()
        } else {
            let num_partitions = cluster.partitions_for_topic(topic).len();
            Utils::to_positive(next_value) % num_partitions as i32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Node, PartitionInfo};
    use std::collections::{HashMap, HashSet};

    /// The three nodes used by the Java test's static `NODES` array.
    fn nodes() -> [Node; 3] {
        [
            Node::new(0, "localhost".to_string(), 99),
            Node::new(1, "localhost".to_string(), 100),
            Node::new(2, "localhost".to_string(), 101),
        ]
    }

    /// Translated from `RoundRobinPartitionerTest.testRoundRobinWithUnavailablePartitions`.
    #[test]
    fn test_round_robin_with_unavailable_partitions() {
        let n = nodes();
        // Intentionally make the partition list not in partition order to test the
        // edge cases.
        let partitions = vec![
            PartitionInfo::new("test".to_string(), 1, None, n.to_vec(), n.to_vec()),
            PartitionInfo::new("test".to_string(), 2, Some(n[1].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new("test".to_string(), 0, Some(n[0].clone()), n.to_vec(), n.to_vec()),
        ];
        // When there are some unavailable partitions, we want to make sure that (1)
        // we always pick an available partition, and (2) the available partitions
        // are selected in a round robin way.
        let mut count_for_part0 = 0;
        let mut count_for_part2 = 0;
        let concrete = RoundRobinPartitioner::new();
        let partitioner: &dyn Partitioner<String, String> = &concrete;
        let cluster = Cluster::new_invalid_topics_controller_topic_ids(
            Some("clusterId".to_string()),
            vec![n[0].clone(), n[1].clone(), n[2].clone()],
            partitions,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );
        for _ in 1..=100 {
            let part = partitioner.partition("test", None, None, None, None, &cluster);
            assert!(
                part == 0 || part == 2,
                "We should never choose a leader-less node in round robin"
            );
            if part == 0 {
                count_for_part0 += 1;
            } else {
                count_for_part2 += 1;
            }
        }
        assert_eq!(
            count_for_part0, count_for_part2,
            "The distribution between two available partitions should be even"
        );
    }

    /// Translated from `RoundRobinPartitionerTest.testRoundRobinWithKeyBytes`.
    #[test]
    fn test_round_robin_with_key_bytes() {
        let topic_a = "topicA";
        let topic_b = "topicB";
        let n = nodes();

        let all_partitions = vec![
            PartitionInfo::new(topic_a.to_string(), 0, Some(n[0].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new(topic_a.to_string(), 1, Some(n[1].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new(topic_a.to_string(), 2, Some(n[2].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new(topic_b.to_string(), 0, Some(n[0].clone()), n.to_vec(), n.to_vec()),
        ];
        let test_cluster = Cluster::new_invalid_topics_controller_topic_ids(
            Some("clusterId".to_string()),
            vec![n[0].clone(), n[1].clone(), n[2].clone()],
            all_partitions,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        let mut partition_count: HashMap<i32, i32> = HashMap::new();

        let key_bytes: &[u8] = b"key";
        let concrete = RoundRobinPartitioner::new();
        let partitioner: &dyn Partitioner<String, String> = &concrete;
        for i in 0..30 {
            let partition = partitioner.partition(topic_a, None, Some(key_bytes), None, None, &test_cluster);
            *partition_count.entry(partition).or_insert(0) += 1;

            if i % 5 == 0 {
                partitioner.partition(topic_b, None, Some(key_bytes), None, None, &test_cluster);
            }
        }

        assert_eq!(10, partition_count[&0]);
        assert_eq!(10, partition_count[&1]);
        assert_eq!(10, partition_count[&2]);
    }
}
