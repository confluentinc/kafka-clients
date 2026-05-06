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

//! Translation of `org.apache.kafka.clients.producer.internals.BuiltInPartitioner`.

#![allow(dead_code)] // Wired up in Phase 6d (RecordAccumulator).

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use arc_swap::ArcSwapOption;

use crate::common::cluster::Cluster;
use crate::common::utils::LogContext;

/// Built-in default partitioner.
///
/// This is just a utility class used directly from `RecordAccumulator`; it
/// does NOT implement the [`Partitioner`](crate::producer::Partitioner)
/// trait — it has a different purpose (sticky-batch partitioning per
/// KIP-794).
///
/// The class keeps track of various bookkeeping required for adaptive
/// sticky partitioning. There is one `BuiltInPartitioner` object per topic.
///
/// # Translation notes
///
/// * Java's `volatile PartitionLoadStats partitionLoadStats` becomes
///   `ArcSwapOption<PartitionLoadStats>` — atomic-swap of an immutable
///   `Arc` snapshot. Reads on the hot path use `load()` (single atomic
///   load).
/// * Java's `AtomicReference<StickyPartitionInfo>` becomes
///   `ArcSwapOption<StickyPartitionInfo>`. The CAS in
///   `peekCurrentPartitionInfo` is replaced by a `compare_and_swap` —
///   `arc-swap` does not expose a CAS primitive, so we use the same
///   "race-and-load" pattern Java does (see `peek_current_partition_info`).
/// * `randomPartition()` is exposed as a virtual hook through a
///   `random_source` field: `Box<dyn Fn() -> i32 + Send + Sync>`. The
///   default uses `rand::Rng` (the closest analogue to
///   `ThreadLocalRandom.current().nextInt()`); tests can inject a
///   sequential mock through [`BuiltInPartitioner::new_with_random_source`].
/// * `producedBytes` inside `StickyPartitionInfo` is `AtomicI32`
///   (CLAUDE.md rule 11) — fetch-add on the hot path.
pub struct BuiltInPartitioner {
    log_prefix: String,
    topic: Arc<str>,
    sticky_batch_size: i32,
    partition_load_stats: ArcSwapOption<PartitionLoadStats>,
    sticky_partition_info: ArcSwapOption<StickyPartitionInfo>,
    random_source: Box<dyn Fn() -> i32 + Send + Sync>,
}

impl BuiltInPartitioner {
    /// Construct a new partitioner.
    ///
    /// * `log_context` — Used for the log prefix.
    /// * `topic` — The topic this partitioner serves.
    /// * `sticky_batch_size` — How much to produce to a partition before
    ///   switching. Must be `>= 1`; mirrors Java's
    ///   `IllegalArgumentException` guard.
    pub fn new(log_context: &LogContext, topic: impl Into<Arc<str>>, sticky_batch_size: i32) -> Self {
        Self::new_with_random_source(log_context, topic, sticky_batch_size, Box::new(default_random_partition))
    }

    /// Construct with a custom random source. Mirrors the test-only
    /// override of `randomPartition()` in Java's
    /// `BuiltInPartitionerTest::SequentialPartitioner`.
    pub fn new_with_random_source(
        log_context: &LogContext,
        topic: impl Into<Arc<str>>,
        sticky_batch_size: i32,
        random_source: Box<dyn Fn() -> i32 + Send + Sync>,
    ) -> Self {
        if sticky_batch_size < 1 {
            // Java throws IllegalArgumentException; we mirror by panic
            // because the constructor is the user-facing entry point and
            // the bad-config case is unrecoverable (CLAUDE.md rule 10
            // allows panic on programmer-error conditions).
            panic!("stickyBatchSize must be >= 1 but got {}", sticky_batch_size);
        }
        BuiltInPartitioner {
            log_prefix: log_context.log_prefix().to_string(),
            topic: topic.into(),
            sticky_batch_size,
            partition_load_stats: ArcSwapOption::empty(),
            sticky_partition_info: ArcSwapOption::empty(),
            random_source,
        }
    }

    /// Calculate the next partition for the topic based on the partition
    /// load stats. Mirrors Java's private `nextPartition`.
    fn next_partition(&self, cluster: &Cluster) -> i32 {
        let random = self.random_partition();

        // Snapshot the volatile field in a local.
        let load_stats = self.partition_load_stats.load_full();
        let partition;

        if let Some(load_stats) = load_stats.as_deref() {
            // Calculate next partition based on load distribution.
            // Note that partitions without leader are excluded from the
            // partitionLoadStats.
            assert!(load_stats.length > 0);
            let cft = &load_stats.cumulative_frequency_table;
            let weighted_random = random.rem_euclid(cft[load_stats.length - 1]);

            // Find the index of the first value strictly greater than
            // `weighted_random`. The Rust slice's `binary_search` returns
            // `Ok(idx)` if found exactly, `Err(insert_idx)` otherwise.
            // The Java code computes `Math.abs(searchResult + 1)`, which
            // produces the index AFTER the matched (or first-greater)
            // element. We mirror that exactly.
            let search_result = cft[..load_stats.length].binary_search(&weighted_random);
            // Java: `Math.abs(searchResult + 1)`. For Ok(idx), Java's
            // searchResult==idx so result is idx+1. For Err(idx), Java's
            // searchResult is `-(idx+1)` so `searchResult+1 = -idx` and
            // `abs(-idx) = idx`. We replicate both branches.
            let partition_index = match search_result {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            };
            assert!(partition_index < load_stats.length);
            partition = load_stats.partition_ids[partition_index];
        } else {
            // No stats; uniform distribution.
            let available_partitions = cluster.available_partitions_for_topic(&self.topic);
            if !available_partitions.is_empty() {
                let idx = (random as usize) % available_partitions.len();
                partition = available_partitions[idx].partition();
            } else {
                let partitions = cluster.partitions_for_topic(&self.topic);
                if partitions.is_empty() {
                    // Java would throw ArithmeticException via the modulo;
                    // mirror by returning -1 (invalid partition).
                    return -1;
                }
                partition = (random as usize % partitions.len()) as i32;
            }
        }

        log::trace!(
            "{}Switching to partition {} in topic {}",
            self.log_prefix,
            partition,
            self.topic
        );
        partition
    }

    /// `Utils.toPositive(ThreadLocalRandom.current().nextInt())` — package-private
    /// hook overridden in `BuiltInPartitionerTest::SequentialPartitioner`.
    fn random_partition(&self) -> i32 {
        (self.random_source)()
    }

    /// Test-only: when partition load stats are defined, return the end
    /// of range for the random number.
    pub fn load_stats_range_end(&self) -> i32 {
        let stats = self.partition_load_stats.load_full();
        let stats = stats.as_deref().expect("partitionLoadStats != null");
        assert!(stats.length > 0);
        stats.cumulative_frequency_table[stats.length - 1]
    }

    /// Peek the currently chosen sticky partition. Works in conjunction
    /// with [`BuiltInPartitioner::is_partition_changed`] and
    /// [`BuiltInPartitioner::update_partition_info`].
    ///
    /// The workflow:
    ///
    /// 1. `peek_current_partition_info` to know which partition to lock.
    /// 2. Lock the partition's batch queue.
    /// 3. `is_partition_changed` under lock to make sure that nobody
    ///    raced us.
    /// 4. Append data to buffer.
    /// 5. `update_partition_info` to update produced bytes and maybe
    ///    switch partition.
    ///
    /// Steps 3-5 must be under the partition's batch queue lock.
    pub fn peek_current_partition_info(&self, cluster: &Cluster) -> Arc<StickyPartitionInfo> {
        let partition_info = self.sticky_partition_info.load_full();
        if let Some(info) = partition_info {
            return info;
        }

        // We're the first to create it. Use `compare_and_swap` from
        // None — if the swap succeeds, we own the new info; otherwise
        // someone raced us and the second `load_full` returns the
        // winner. The same race-resolve pattern Java uses with
        // `AtomicReference.compareAndSet` (java line 150-154).
        let new_info = Arc::new(StickyPartitionInfo::new(self.next_partition(cluster)));
        let prev = self
            .sticky_partition_info
            .compare_and_swap(&None::<Arc<StickyPartitionInfo>>, Some(new_info.clone()));
        if prev.is_none() {
            new_info
        } else {
            // Someone raced us. Reload the winner.
            self.sticky_partition_info
                .load_full()
                .expect("sticky_partition_info race-loser saw a non-None previous value")
        }
    }

    /// Check if the partition was changed by a concurrent thread. Must
    /// be called under the partition's batch queue lock.
    pub fn is_partition_changed(&self, partition_info: &Arc<StickyPartitionInfo>) -> bool {
        // partitionInfo may be a stale snapshot from before another
        // appender raced and switched.
        match self.sticky_partition_info.load_full() {
            Some(current) => !Arc::ptr_eq(&current, partition_info),
            // If our caller had a partitionInfo and now there is none,
            // someone changed it (cleared) — treat as changed.
            None => true,
        }
    }

    /// Update partition info with the number of bytes appended and maybe
    /// switch partition. Must be called under the partition's batch
    /// queue lock.
    ///
    /// `enable_switch` defaults to `true`; pass `false` to suppress the
    /// switch unless `producedBytes >= stickyBatchSize * 2` (used when
    /// the current batch isn't ready to send).
    pub fn update_partition_info(
        &self,
        partition_info: &Arc<StickyPartitionInfo>,
        appended_bytes: i32,
        cluster: &Cluster,
        enable_switch: bool,
    ) {
        // Java has two overloads; the 3-arg one delegates to the 4-arg
        // form with `enable_switch=true`. Callers that want the default
        // can pass `true`.
        // Java: assert partitionInfo == stickyPartitionInfo.get();
        if let Some(current) = self.sticky_partition_info.load_full() {
            debug_assert!(
                Arc::ptr_eq(&current, partition_info),
                "partitionInfo must equal stickyPartitionInfo.get()"
            );
        }

        let produced_bytes = partition_info.produced_bytes.fetch_add(appended_bytes, Ordering::SeqCst) + appended_bytes;

        if produced_bytes >= self.sticky_batch_size * 2 {
            log::trace!(
                "{}Produced {} bytes, exceeding twice the batch size of {} bytes, with switching set to {}",
                self.log_prefix,
                produced_bytes,
                self.sticky_batch_size,
                enable_switch,
            );
        }

        if (produced_bytes >= self.sticky_batch_size && enable_switch) || produced_bytes >= self.sticky_batch_size * 2 {
            // We've produced enough to this partition, switch to next.
            let new_partition_info = Arc::new(StickyPartitionInfo::new(self.next_partition(cluster)));
            self.sticky_partition_info.store(Some(new_partition_info));
        }
    }

    /// Update partition load stats from the queue sizes of each partition.
    /// `queueSizes` is modified in place to avoid allocations.
    ///
    /// * `queue_sizes` — The queue sizes, partitions without leaders are
    ///   excluded.
    /// * `partition_ids` — The partition ids for the queues, partitions
    ///   without leaders are excluded.
    /// * `length` — The logical length of the arrays (could be less): we
    ///   may eliminate some partitions based on latency, but to avoid
    ///   reallocation of the arrays, we just decrement logical length.
    pub fn update_partition_load_stats(&self, queue_sizes: Option<&mut [i32]>, partition_ids: &[i32], length: usize) {
        let queue_sizes = match queue_sizes {
            Some(q) => q,
            None => {
                log::trace!("{}No load stats for topic {}, not using adaptive", self.log_prefix, self.topic);
                self.partition_load_stats.store(None);
                return;
            },
        };
        assert_eq!(queue_sizes.len(), partition_ids.len());
        assert!(length <= queue_sizes.len());

        // queueSizes.len() represents the number of all partitions in
        // the topic; if we have less than 2 partitions, no adaptive
        // logic.
        if length < 1 || queue_sizes.len() < 2 {
            log::trace!(
                "{}The number of partitions is too small: available={}, all={}, not using adaptive for topic {}",
                self.log_prefix,
                length,
                queue_sizes.len(),
                self.topic,
            );
            self.partition_load_stats.store(None);
            return;
        }

        // Calculate max queue size + 1 and check if all sizes are the
        // same.
        let mut max_size_plus1 = queue_sizes[0];
        let mut all_equal = true;
        for &qs in &queue_sizes[1..length] {
            if qs != max_size_plus1 {
                all_equal = false;
            }
            if qs > max_size_plus1 {
                max_size_plus1 = qs;
            }
        }
        max_size_plus1 += 1;

        if all_equal && length == queue_sizes.len() {
            // No need for adaptive logic when all sizes are equal and
            // no partitions were excluded.
            log::trace!(
                "{}All queue lengths are the same, not using adaptive for topic {}",
                self.log_prefix,
                self.topic
            );
            self.partition_load_stats.store(None);
            return;
        }

        // Invert and fold the queue sizes into a cumulative frequency
        // table. Java: queueSizes[0] = maxSizePlus1 - queueSizes[0];
        // queueSizes[i] = maxSizePlus1 - queueSizes[i] + queueSizes[i-1].
        queue_sizes[0] = max_size_plus1 - queue_sizes[0];
        for i in 1..length {
            queue_sizes[i] = max_size_plus1 - queue_sizes[i] + queue_sizes[i - 1];
        }
        log::trace!(
            "{}Partition load stats for topic {}: CFT={:?}, IDs={:?}, length={}",
            self.log_prefix,
            self.topic,
            queue_sizes,
            partition_ids,
            length,
        );
        self.partition_load_stats.store(Some(Arc::new(PartitionLoadStats {
            cumulative_frequency_table: queue_sizes.to_vec(),
            partition_ids: partition_ids.to_vec(),
            length,
        })));
    }
}

/// Default hashing function to choose a partition from the serialized
/// key bytes. Mirrors `BuiltInPartitioner.partitionForKey`.
pub fn partition_for_key(serialized_key: &[u8], num_partitions: i32) -> i32 {
    use crate::common::utils::utils::murmur2;
    // `Utils.toPositive(int)` masks the sign bit. Java: `n & 0x7fffffff`.
    let h = murmur2(serialized_key) & 0x7fff_ffff;
    h % num_partitions
}

/// Info for the current sticky partition. Mirrors Java's static inner
/// class `StickyPartitionInfo`.
pub struct StickyPartitionInfo {
    index: i32,
    /// `producedBytes` — atomic counter. CLAUDE.md rule 11.
    produced_bytes: AtomicI32,
}

impl StickyPartitionInfo {
    /// Construct a new sticky partition info with `producedBytes` at 0.
    pub fn new(index: i32) -> Self {
        StickyPartitionInfo { index, produced_bytes: AtomicI32::new(0) }
    }

    /// The partition index this object refers to.
    pub fn partition(&self) -> i32 {
        self.index
    }
}

/// The partition load stats for each topic that are used for adaptive
/// partition distribution. Java's static inner class `PartitionLoadStats`.
pub struct PartitionLoadStats {
    cumulative_frequency_table: Vec<i32>,
    partition_ids: Vec<i32>,
    length: usize,
}

/// Default `randomPartition` implementation: `toPositive(rand.nextInt())`.
fn default_random_partition() -> i32 {
    use rand::Rng;
    rand::rng().random::<i32>() & 0x7fff_ffff
}

#[cfg(test)]
mod tests {
    //! Translation of `BuiltInPartitionerTest`.

    use std::collections::HashSet;
    use std::sync::atomic::{AtomicI32, Ordering};

    use super::*;
    use crate::common::node::Node;
    use crate::common::partition_info::PartitionInfo;

    fn nodes() -> [Node; 4] {
        [
            Node::new(0, "localhost".to_string(), 99),
            Node::new(1, "localhost".to_string(), 100),
            Node::new(2, "localhost".to_string(), 101),
            Node::new(11, "localhost".to_string(), 102),
        ]
    }

    const TOPIC_A: &str = "topicA";
    const TOPIC_B: &str = "topicB";
    const TOPIC_C: &str = "topicC";

    /// Build a `BuiltInPartitioner` whose `random_partition()` returns
    /// 0, 1, 2, … sequentially. Mirrors the inner `SequentialPartitioner`
    /// fixture in Java.
    fn sequential_partitioner(topic: &str, sticky_batch_size: i32) -> BuiltInPartitioner {
        let counter = std::sync::Arc::new(AtomicI32::new(0));
        let counter_clone = counter.clone();
        BuiltInPartitioner::new_with_random_source(
            &LogContext::new(),
            topic,
            sticky_batch_size,
            Box::new(move || counter_clone.fetch_add(1, Ordering::SeqCst)),
        )
    }

    /// Java: `testStickyPartitioning`.
    #[test]
    fn sticky_partitioning() {
        let n = nodes();
        let all_partitions = vec![
            PartitionInfo::new(TOPIC_A, 0, Some(n[0].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new(TOPIC_A, 1, Some(n[1].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new(TOPIC_A, 2, Some(n[2].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new(TOPIC_B, 0, Some(n[0].clone()), n.to_vec(), n.to_vec()),
        ];
        let test_cluster = Cluster::new(
            Some("clusterId".to_string()),
            n.to_vec(),
            all_partitions,
            HashSet::new(),
            HashSet::new(),
        );

        // Create partitions with "sticky" batch size to accommodate 3
        // records.
        let built_in_partitioner_a = sequential_partitioner(TOPIC_A, 3);

        // Test the partition is not switched until sticky batch size
        // is reached.
        let partition_info = built_in_partitioner_a.peek_current_partition_info(&test_cluster);
        let part_a = partition_info.partition();
        built_in_partitioner_a.update_partition_info(&partition_info, 1, &test_cluster, true);

        let partition_info = built_in_partitioner_a.peek_current_partition_info(&test_cluster);
        assert_eq!(part_a, partition_info.partition());
        built_in_partitioner_a.update_partition_info(&partition_info, 1, &test_cluster, true);

        let partition_info = built_in_partitioner_a.peek_current_partition_info(&test_cluster);
        assert_eq!(part_a, partition_info.partition());
        built_in_partitioner_a.update_partition_info(&partition_info, 1, &test_cluster, true);

        // After producing 3 records, partition must've switched.
        assert_ne!(
            part_a,
            built_in_partitioner_a.peek_current_partition_info(&test_cluster).partition()
        );

        // Check that switching works even when there is one partition.
        let built_in_partitioner_b = sequential_partitioner(TOPIC_B, 1);
        for _ in 0..10 {
            let partition_info = built_in_partitioner_b.peek_current_partition_info(&test_cluster);
            assert_eq!(0, partition_info.partition());
            built_in_partitioner_b.update_partition_info(&partition_info, 1, &test_cluster, true);
        }
    }

    /// Java: `unavailablePartitionsTest`.
    #[test]
    fn unavailable_partitions() {
        // Partition 1 in topic A, partition 0 in topic B and partition
        // 0 in topic C are unavailable.
        let n = nodes();
        let all_partitions = vec![
            PartitionInfo::new(TOPIC_A, 0, Some(n[0].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new(TOPIC_A, 1, None, n.to_vec(), n.to_vec()),
            PartitionInfo::new(TOPIC_A, 2, Some(n[2].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new(TOPIC_B, 0, None, n.to_vec(), n.to_vec()),
            PartitionInfo::new(TOPIC_B, 1, Some(n[0].clone()), n.to_vec(), n.to_vec()),
            PartitionInfo::new(TOPIC_C, 0, None, n.to_vec(), n.to_vec()),
        ];

        let test_cluster = Cluster::new(
            Some("clusterId".to_string()),
            vec![n[0].clone(), n[1].clone(), n[2].clone()],
            all_partitions,
            HashSet::new(),
            HashSet::new(),
        );

        // Sticky batch size of 1 record.
        let built_in_partitioner_a = BuiltInPartitioner::new(&LogContext::new(), TOPIC_A, 1);

        // Assure we never choose partition 1 because it is unavailable.
        let partition_info = built_in_partitioner_a.peek_current_partition_info(&test_cluster);
        let part_a = partition_info.partition();
        built_in_partitioner_a.update_partition_info(&partition_info, 1, &test_cluster, true);

        let mut found_another_part_a = false;
        assert_ne!(1, part_a);
        for _ in 0..100 {
            let partition_info = built_in_partitioner_a.peek_current_partition_info(&test_cluster);
            let another_part_a = partition_info.partition();
            built_in_partitioner_a.update_partition_info(&partition_info, 1, &test_cluster, true);

            assert_ne!(1, another_part_a);
            found_another_part_a = found_another_part_a || another_part_a != part_a;
        }
        assert!(found_another_part_a, "Expected to find partition other than {}", part_a);

        let built_in_partitioner_b = BuiltInPartitioner::new(&LogContext::new(), TOPIC_B, 1);
        // Assure we always choose partition 1 for topic B.
        let partition_info = built_in_partitioner_b.peek_current_partition_info(&test_cluster);
        let part_b = partition_info.partition();
        built_in_partitioner_b.update_partition_info(&partition_info, 1, &test_cluster, true);

        assert_eq!(1, part_b);
        for _ in 0..100 {
            let partition_info = built_in_partitioner_b.peek_current_partition_info(&test_cluster);
            assert_eq!(1, partition_info.partition());
            built_in_partitioner_b.update_partition_info(&partition_info, 1, &test_cluster, true);
        }

        // Assure that we still choose the partition when there are no
        // partitions available.
        let built_in_partitioner_c = BuiltInPartitioner::new(&LogContext::new(), TOPIC_C, 1);
        let partition_info = built_in_partitioner_c.peek_current_partition_info(&test_cluster);
        let part_c = partition_info.partition();
        built_in_partitioner_c.update_partition_info(&partition_info, 1, &test_cluster, true);
        assert_eq!(0, part_c);

        let partition_info = built_in_partitioner_c.peek_current_partition_info(&test_cluster);
        let part_c = partition_info.partition();
        assert_eq!(0, part_c);
    }

    /// Java: `adaptivePartitionsTest`.
    #[test]
    fn adaptive_partitions() {
        let built_in_partitioner = sequential_partitioner(TOPIC_A, 1);
        let n = nodes();

        // Simulate partition queue sizes.
        let mut queue_sizes: [i32; 5] = [5, 0, 3, 0, 1];
        let mut partition_ids: [i32; 5] = [0; 5];
        let mut expected_frequencies: [i32; 5] = [0; 5];
        let mut all_partitions = Vec::new();
        for i in 0..partition_ids.len() {
            partition_ids[i] = i as i32;
            all_partitions.push(PartitionInfo::new(
                TOPIC_A,
                i as i32,
                Some(n[i % n.len()].clone()),
                n.to_vec(),
                n.to_vec(),
            ));
            expected_frequencies[i] = 6 - queue_sizes[i]; // 6 == max(queue_sizes) + 1
        }

        let queue_sizes_len = queue_sizes.len();
        built_in_partitioner.update_partition_load_stats(Some(&mut queue_sizes), &partition_ids, queue_sizes_len);

        let test_cluster = Cluster::new(
            Some("clusterId".to_string()),
            n.to_vec(),
            all_partitions,
            HashSet::new(),
            HashSet::new(),
        );

        // Issue a certain number of partition calls to validate that
        // the partitions are distributed with frequencies reciprocal to
        // the queue sizes. The number of iterations is the last element
        // of the cumulative frequency table * 2 cycles.
        let number_of_cycles = 2;
        let number_of_iterations = built_in_partitioner.load_stats_range_end() * number_of_cycles;
        let mut frequencies: [i32; 5] = [0; 5];

        for _ in 0..number_of_iterations {
            let partition_info = built_in_partitioner.peek_current_partition_info(&test_cluster);
            frequencies[partition_info.partition() as usize] += 1;
            built_in_partitioner.update_partition_info(&partition_info, 1, &test_cluster, true);
        }

        // Verify that frequencies are reciprocal of queue sizes.
        for i in 0..frequencies.len() {
            assert_eq!(
                expected_frequencies[i] * number_of_cycles,
                frequencies[i],
                "Partition {} was chosen {} times",
                i,
                frequencies[i]
            );
        }
    }

    /// Java: `testStickyBatchSizeMoreThatZero`.
    #[test]
    fn sticky_batch_size_more_than_zero_panics_on_zero() {
        let result = std::panic::catch_unwind(|| BuiltInPartitioner::new(&LogContext::new(), TOPIC_A, 0));
        assert!(result.is_err(), "expected panic on stickyBatchSize=0");
    }

    /// Java: `testStickyBatchSizeMoreThatZero` — the positive case.
    #[test]
    fn sticky_batch_size_one_does_not_panic() {
        // Construction should succeed (no panic).
        let _ = BuiltInPartitioner::new(&LogContext::new(), TOPIC_A, 1);
    }
}
