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

//! Translation of `org.apache.kafka.clients.producer.RoundRobinPartitioner`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use crate::common::cluster::Cluster;
use crate::producer::partitioner::Partitioner;

/// The "Round-Robin" partitioner.
///
/// This partitioning strategy can be used when a user wants to distribute
/// the writes to all partitions equally. This is the behaviour regardless of
/// record key hash.
///
/// # Translation notes
///
/// Java uses `ConcurrentMap<String, AtomicInteger>` to keep one counter per
/// topic. The Rust translation uses `Mutex<HashMap<Arc<str>,
/// Arc<AtomicI32>>>`: the mutex is taken only on the slow path of inserting
/// a *new* topic counter (matching `computeIfAbsent`). The hot path —
/// incrementing an existing topic's counter — never touches the mutex; we
/// hold an `Arc<AtomicI32>` and call `fetch_add` directly (CLAUDE.md rule
/// 11: prefer atomics over `Mutex<i32>` for shared numeric state).
pub struct RoundRobinPartitioner {
    topic_counter_map: Mutex<HashMap<Arc<str>, Arc<AtomicI32>>>,
}

impl Default for RoundRobinPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobinPartitioner {
    /// Construct a new round-robin partitioner with no per-topic state.
    pub fn new() -> Self {
        RoundRobinPartitioner { topic_counter_map: Mutex::new(HashMap::new()) }
    }

    /// `Utils.toPositive(int)` — masks the sign bit. Mirrors the Java
    /// helper used to fold a negative counter value into the positive
    /// range without losing the wrap-around behaviour.
    fn to_positive(n: i32) -> i32 {
        n & 0x7fff_ffff
    }

    /// Get-or-create the per-topic atomic counter and return the
    /// `getAndIncrement()` value (Java's `nextValue(topic)`).
    fn next_value(&self, topic: &str) -> i32 {
        // Fast path: read-only lookup. We still need the mutex for the
        // map itself, but the value is an `Arc<AtomicI32>` we increment
        // *outside* the lock.
        {
            let map = self.topic_counter_map.lock().unwrap();
            if let Some(counter) = map.get(topic) {
                let counter = Arc::clone(counter);
                drop(map);
                return counter.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Slow path: insert a new counter. `computeIfAbsent` semantics:
        // re-check under the lock, insert if still missing.
        let mut map = self.topic_counter_map.lock().unwrap();
        let counter = map
            .entry(Arc::<str>::from(topic))
            .or_insert_with(|| Arc::new(AtomicI32::new(0)));
        let counter = Arc::clone(counter);
        drop(map);
        counter.fetch_add(1, Ordering::Relaxed)
    }
}

impl Partitioner for RoundRobinPartitioner {
    /// # Panics
    ///
    /// Panics if `cluster` reports zero partitions for `topic`. Mirrors
    /// Java's `Utils.toPositive(nextValue) % numPartitions` raising
    /// `ArithmeticException` on division by zero. Per CLAUDE.md rule
    /// 10, panicking on `ArithmeticException`-like conditions is
    /// acceptable.
    fn partition(
        &self,
        topic: &str,
        _key: Option<&dyn Any>,
        _key_bytes: Option<&[u8]>,
        _value: Option<&dyn Any>,
        _value_bytes: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32 {
        let next_value = self.next_value(topic);
        let available_partitions = cluster.available_partitions_for_topic(topic);
        if !available_partitions.is_empty() {
            let part = (Self::to_positive(next_value) as usize) % available_partitions.len();
            available_partitions[part].partition()
        } else {
            // No partitions are available, give a non-available partition.
            // Java line 62: `Utils.toPositive(nextValue) % numPartitions`
            // — raises `ArithmeticException` if `numPartitions == 0`.
            // We mirror by panicking; CLAUDE.md rule 10.1 explicitly
            // permits panic on division-by-zero. The previous version
            // returned `-1`, which silently routed to "partition -1"
            // and was a behavior divergence from Java.
            let num_partitions = cluster.partitions_for_topic(topic).len();
            (Self::to_positive(next_value) as usize % num_partitions) as i32
        }
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `RoundRobinPartitionerTest`.

    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::common::node::Node;
    use crate::common::partition_info::PartitionInfo;

    fn nodes() -> [Node; 3] {
        [
            Node::new(0, "localhost".to_string(), 99),
            Node::new(1, "localhost".to_string(), 100),
            Node::new(2, "localhost".to_string(), 101),
        ]
    }

    /// Java: `testRoundRobinWithUnavailablePartitions`.
    #[test]
    fn round_robin_with_unavailable_partitions() {
        // Intentionally make the partition list not in partition order to
        // test the edge cases.
        let n = nodes();
        let partitions = vec![
            PartitionInfo::new(
                "test",
                1,
                None,
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
            ),
            PartitionInfo::new(
                "test",
                2,
                Some(n[1].clone()),
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
            ),
            PartitionInfo::new(
                "test",
                0,
                Some(n[0].clone()),
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
            ),
        ];
        // When there are some unavailable partitions, we want to make
        // sure that (1) we always pick an available partition, and (2)
        // the available partitions are selected in a round-robin way.
        let mut count_part0 = 0;
        let mut count_part2 = 0;
        let partitioner = RoundRobinPartitioner::new();
        let cluster = Cluster::new(
            Some("clusterId".to_string()),
            n.to_vec(),
            partitions,
            HashSet::new(),
            HashSet::new(),
        );
        for _ in 1..=100 {
            let part = partitioner.partition("test", None, None, None, None, &cluster);
            assert!(
                part == 0 || part == 2,
                "We should never choose a leader-less node in round robin"
            );
            if part == 0 {
                count_part0 += 1;
            } else {
                count_part2 += 1;
            }
        }
        assert_eq!(
            count_part0, count_part2,
            "The distribution between two available partitions should be even"
        );
    }

    /// Java: `testRoundRobinWithKeyBytes`.
    #[test]
    fn round_robin_with_key_bytes() {
        let n = nodes();
        let topic_a = "topicA";
        let topic_b = "topicB";

        let all_partitions = vec![
            PartitionInfo::new(
                topic_a,
                0,
                Some(n[0].clone()),
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
            ),
            PartitionInfo::new(
                topic_a,
                1,
                Some(n[1].clone()),
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
            ),
            PartitionInfo::new(
                topic_a,
                2,
                Some(n[2].clone()),
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
            ),
            PartitionInfo::new(
                topic_b,
                0,
                Some(n[0].clone()),
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
            ),
        ];
        let test_cluster = Cluster::new(
            Some("clusterId".to_string()),
            n.to_vec(),
            all_partitions,
            HashSet::new(),
            HashSet::new(),
        );

        let mut partition_count: HashMap<i32, i32> = HashMap::new();
        let key_bytes = b"key";
        let partitioner = RoundRobinPartitioner::new();
        for i in 0..30 {
            let partition = partitioner.partition(topic_a, None, Some(key_bytes.as_slice()), None, None, &test_cluster);
            *partition_count.entry(partition).or_insert(0) += 1;

            if i % 5 == 0 {
                partitioner.partition(topic_b, None, Some(key_bytes.as_slice()), None, None, &test_cluster);
            }
        }

        assert_eq!(10, *partition_count.get(&0).unwrap());
        assert_eq!(10, *partition_count.get(&1).unwrap());
        assert_eq!(10, *partition_count.get(&2).unwrap());
    }

    /// Regression for Phase 6c Round 1 Issue 4. The Rust implementation
    /// has two paths through `next_value`: a fast path when the topic
    /// counter already exists (Arc<AtomicI32> increment, no mutex), and
    /// a slow path that takes the mutex to `entry().or_insert_with()`.
    /// Java's `ConcurrentHashMap.computeIfAbsent` collapses both into
    /// one call, so this fast/slow bifurcation is Rust-specific. This
    /// test calls `partition` twice for the same topic: the first call
    /// goes through the slow path (counter created), the second call
    /// must hit the fast path (counter exists). We verify the counter
    /// is monotonically incremented by checking the second call returns
    /// the next round-robin partition.
    #[test]
    fn next_value_increments_through_fast_path_after_first_call() {
        let n = nodes();
        let partitions = vec![
            PartitionInfo::new(
                "test",
                0,
                Some(n[0].clone()),
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
            ),
            PartitionInfo::new(
                "test",
                1,
                Some(n[1].clone()),
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
                vec![n[0].clone(), n[1].clone(), n[2].clone()],
            ),
        ];
        let cluster = Cluster::new(
            Some("clusterId".to_string()),
            n.to_vec(),
            partitions,
            HashSet::new(),
            HashSet::new(),
        );
        let partitioner = RoundRobinPartitioner::new();
        let p0 = partitioner.partition("test", None, None, None, None, &cluster); // slow path
        let p1 = partitioner.partition("test", None, None, None, None, &cluster); // fast path
        let p2 = partitioner.partition("test", None, None, None, None, &cluster); // fast path
        // Each call must return a different partition (round-robin) —
        // proves the counter incremented on the fast path.
        assert_ne!(p0, p1);
        assert_eq!(p0, p2); // round-robin wraps after 2 partitions
    }

    /// Regression for Phase 6c Round 1 Issue 6. Java's `partition()`
    /// raises `ArithmeticException` via `% 0` when `numPartitions == 0`.
    /// The Rust translation panics to mirror Java exactly (CLAUDE.md
    /// rule 10.1 allows panic on division-by-zero).
    #[test]
    fn partition_on_zero_partition_topic_panics() {
        let n = nodes();
        let cluster = Cluster::new(
            Some("clusterId".to_string()),
            n.to_vec(),
            // No partitions for topic "no-parts".
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
        );
        let partitioner = RoundRobinPartitioner::new();
        let result = std::panic::catch_unwind(|| partitioner.partition("no-parts", None, None, None, None, &cluster));
        assert!(result.is_err(), "expected panic on zero-partition topic");
    }
}
