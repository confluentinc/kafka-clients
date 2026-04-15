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

//! Built-in default partitioner.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.BuiltInPartitioner`.
//!
//! This is a utility class used directly from [`RecordAccumulator`], it does not
//! implement the Partitioner interface.
//!
//! The class keeps track of various bookkeeping information required for adaptive
//! sticky partitioning (described in detail in KIP-794). There is one partitioner
//! object per topic.

use std::sync::atomic::{AtomicI32, Ordering};

use log::trace;

use crate::common::cluster::Cluster;
use crate::common::utils::{murmur2, to_positive};

/// Information for the current sticky partition.
///
/// Translated from `BuiltInPartitioner.StickyPartitionInfo`.
pub struct StickyPartitionInfo {
    index: i32,
    produced_bytes: AtomicI32,
}

impl StickyPartitionInfo {
    /// Creates a new `StickyPartitionInfo` for the given partition index.
    pub fn new(index: i32) -> Self {
        Self { index, produced_bytes: AtomicI32::new(0) }
    }

    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.index
    }
}

/// The partition load stats for each topic that are used for adaptive partition
/// distribution.
///
/// Translated from `BuiltInPartitioner.PartitionLoadStats`.
struct PartitionLoadStats {
    cumulative_frequency_table: Vec<i32>,
    partition_ids: Vec<i32>,
    length: usize,
}

impl PartitionLoadStats {
    fn new(cumulative_frequency_table: Vec<i32>, partition_ids: Vec<i32>, length: usize) -> Self {
        debug_assert_eq!(cumulative_frequency_table.len(), partition_ids.len());
        debug_assert!(length <= cumulative_frequency_table.len());
        Self { cumulative_frequency_table, partition_ids, length }
    }
}

/// Built-in default partitioner with adaptive sticky partitioning.
///
/// Translated from `org.apache.kafka.clients.producer.internals.BuiltInPartitioner`.
///
/// There is one partitioner object per topic. The partitioner uses sticky
/// partitioning to avoid switching partitions too frequently, and uses adaptive
/// load stats to distribute records based on queue sizes.
pub struct BuiltInPartitioner {
    topic: String,
    sticky_batch_size: i32,
    partition_load_stats: Option<PartitionLoadStats>,
    sticky_partition_info: Option<StickyPartitionInfo>,
}

impl BuiltInPartitioner {
    /// Creates a new `BuiltInPartitioner`.
    ///
    /// # Arguments
    /// * `topic` - The topic
    /// * `sticky_batch_size` - How much to produce to partition before switch
    ///
    /// # Panics
    /// Panics if `sticky_batch_size` is less than 1.
    pub fn new(topic: &str, sticky_batch_size: i32) -> Self {
        assert!(
            sticky_batch_size >= 1,
            "sticky_batch_size must be >= 1 but got {}",
            sticky_batch_size
        );
        Self {
            topic: topic.to_string(),
            sticky_batch_size,
            partition_load_stats: None,
            sticky_partition_info: None,
        }
    }

    /// Calculate the next partition for the topic based on the partition load stats.
    fn next_partition(&mut self, cluster: &Cluster) -> i32 {
        let random = self.random_partition();

        let partition = if let Some(ref stats) = self.partition_load_stats {
            // Calculate next partition based on load distribution.
            // Note that partitions without leader are excluded from the partition_load_stats.
            debug_assert!(stats.length > 0);

            let cft = &stats.cumulative_frequency_table;
            let weighted_random = random % cft[stats.length - 1];

            // By construction, the cumulative frequency table is sorted, so we can use binary
            // search to find the desired index.
            let search_result = cft[..stats.length].binary_search(&weighted_random);

            // binary_search returns Ok(index) if found, Err(insertion_point) if not found.
            // We need to get the index of the first value that is strictly greater.
            // If found: we need the next index (index + 1).
            // If not found: the insertion_point IS the first value strictly greater.
            let partition_index = match search_result {
                Ok(index) => index + 1,
                Err(insertion_point) => insertion_point,
            };
            debug_assert!(partition_index < stats.length);
            stats.partition_ids[partition_index]
        } else {
            // We don't have stats to do adaptive partitioning (or it's disabled), just switch
            // to the next partition based on uniform distribution.
            let available = cluster.available_partitions_for_topic(&self.topic);
            if !available.is_empty() {
                available[(random as usize) % available.len()].partition()
            } else {
                // We don't have available partitions, just pick one among all partitions.
                let partitions = cluster.partitions_for_topic(&self.topic);
                (random as usize % partitions.len()) as i32
            }
        };

        trace!("Switching to partition {} in topic {}", partition, self.topic);
        partition
    }

    /// Generate a random positive integer for partition selection.
    ///
    /// This method can be overridden in tests to provide deterministic behavior.
    fn random_partition(&mut self) -> i32 {
        to_positive(rand::random::<i32>())
    }

    /// Test-only function. When partition load stats are defined, return the end
    /// of range for the random number.
    pub fn load_stats_range_end(&self) -> i32 {
        let stats = self.partition_load_stats.as_ref().expect("partition_load_stats must be set");
        debug_assert!(stats.length > 0);
        stats.cumulative_frequency_table[stats.length - 1]
    }

    /// Peek currently chosen sticky partition. This method works in conjunction
    /// with [`is_partition_changed`] and [`update_partition_info`]. The workflow is:
    ///
    /// 1. `peek_current_partition_info` is called to know which partition to lock.
    /// 2. Lock partition's batch queue.
    /// 3. `is_partition_changed` under lock to make sure that nobody raced us.
    /// 4. Append data to buffer.
    /// 5. `update_partition_info` to update produced bytes and maybe switch partition.
    ///
    /// It's important that steps 3-5 are under partition's batch queue lock.
    ///
    /// # Arguments
    /// * `cluster` - The cluster information (needed if there is no current partition)
    ///
    /// # Returns
    /// A reference to the sticky partition info.
    pub fn peek_current_partition_info(&mut self, cluster: &Cluster) -> &StickyPartitionInfo {
        if self.sticky_partition_info.is_none() {
            let partition = self.next_partition(cluster);
            self.sticky_partition_info = Some(StickyPartitionInfo::new(partition));
        }
        self.sticky_partition_info.as_ref().unwrap()
    }

    /// Check if partition is changed by a concurrent thread. NOTE this function
    /// needs to be called under the partition's batch queue lock.
    ///
    /// In the Rust implementation, since we use `&mut self` for partition changes,
    /// this always returns `false`. The method is kept for API compatibility.
    pub fn is_partition_changed(&self, partition_info: &StickyPartitionInfo) -> bool {
        // In Rust, since we use &mut self for modifications, there's no concurrent
        // race condition possible. We check pointer identity as in Java.
        match &self.sticky_partition_info {
            Some(current) => !std::ptr::eq(current, partition_info),
            None => true,
        }
    }

    /// Update partition info with the number of bytes appended and maybe switch
    /// partition. NOTE this function needs to be called under the partition's batch
    /// queue lock.
    ///
    /// # Arguments
    /// * `appended_bytes` - The number of bytes appended to this partition
    /// * `cluster` - The cluster information
    pub fn update_partition_info(&mut self, appended_bytes: i32, cluster: &Cluster) {
        self.update_partition_info_with_switch(appended_bytes, cluster, true);
    }

    /// Update partition info with the number of bytes appended and maybe switch
    /// partition. NOTE this function needs to be called under the partition's batch
    /// queue lock.
    ///
    /// # Arguments
    /// * `appended_bytes` - The number of bytes appended to this partition
    /// * `cluster` - The cluster information
    /// * `enable_switch` - If true, switch partition once produced enough bytes
    pub fn update_partition_info_with_switch(&mut self, appended_bytes: i32, cluster: &Cluster, enable_switch: bool) {
        if self.sticky_partition_info.is_none() {
            return;
        }

        let produced_bytes = {
            let info = self.sticky_partition_info.as_ref().unwrap();
            info.produced_bytes.fetch_add(appended_bytes, Ordering::Relaxed) + appended_bytes
        };

        // We're trying to switch partition once we produce sticky_batch_size bytes to a partition
        // but doing so may hinder batching because partition switch may happen while batch isn't
        // ready to send.
        if produced_bytes >= self.sticky_batch_size * 2 {
            trace!(
                "Produced {} bytes, exceeding twice the batch size of {} bytes, with switching set to {}",
                produced_bytes, self.sticky_batch_size, enable_switch
            );
        }

        if (produced_bytes >= self.sticky_batch_size && enable_switch) || produced_bytes >= self.sticky_batch_size * 2 {
            // We've produced enough to this partition, switch to next.
            let new_partition = self.next_partition(cluster);
            self.sticky_partition_info = Some(StickyPartitionInfo::new(new_partition));
        }
    }

    /// Update partition load stats from the queue sizes of each partition.
    /// NOTE: `queue_sizes` are modified in place to avoid allocations.
    ///
    /// # Arguments
    /// * `queue_sizes` - The queue sizes, partitions without leaders are excluded. Modified in place.
    /// * `partition_ids` - The partition ids for the queues
    /// * `length` - The logical length of the arrays (could be less than actual length)
    pub fn update_partition_load_stats(
        &mut self,
        queue_sizes: Option<&mut [i32]>,
        partition_ids: &[i32],
        length: usize,
    ) {
        let queue_sizes = match queue_sizes {
            Some(qs) => qs,
            None => {
                trace!("No load stats for topic {}, not using adaptive", self.topic);
                self.partition_load_stats = None;
                return;
            },
        };
        debug_assert_eq!(queue_sizes.len(), partition_ids.len());
        debug_assert!(length <= queue_sizes.len());

        // The queue_sizes.len() represents the number of all partitions in the topic and if we have
        // less than 2 partitions, there is no need to do adaptive logic.
        if length < 1 || queue_sizes.len() < 2 {
            trace!(
                "The number of partitions is too small: available={}, all={}, not using adaptive for topic {}",
                length,
                queue_sizes.len(),
                self.topic
            );
            self.partition_load_stats = None;
            return;
        }

        // Calculate max queue size + 1 and check if all sizes are the same.
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
            // No need to have complex probability logic when all queue sizes are the same,
            // and we didn't exclude partitions that experience high latencies.
            trace!("All queue lengths are the same, not using adaptive for topic {}", self.topic);
            self.partition_load_stats = None;
            return;
        }

        // Invert and fold the queue size, so that they become separator values in the CFT.
        queue_sizes[0] = max_size_plus1 - queue_sizes[0];
        for i in 1..length {
            queue_sizes[i] = max_size_plus1 - queue_sizes[i] + queue_sizes[i - 1];
        }
        trace!(
            "Partition load stats for topic {}: CFT={:?}, IDs={:?}, length={}",
            self.topic,
            &queue_sizes[..length],
            &partition_ids[..length],
            length
        );
        self.partition_load_stats = Some(PartitionLoadStats::new(queue_sizes.to_vec(), partition_ids.to_vec(), length));
    }

    /// Default hashing function to choose a partition from the serialized key bytes.
    ///
    /// Translated from `BuiltInPartitioner.partitionForKey`.
    pub fn partition_for_key(serialized_key: &[u8], num_partitions: i32) -> i32 {
        to_positive(murmur2(serialized_key)) % num_partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::node::Node;
    use crate::common::partition_info::PartitionInfo;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::AtomicI32;

    fn make_nodes() -> Vec<Node> {
        vec![
            Node::new(0, "localhost".to_string(), 99),
            Node::new(1, "localhost".to_string(), 100),
            Node::new(2, "localhost".to_string(), 101),
            Node::new(11, "localhost".to_string(), 102),
        ]
    }

    const TOPIC_A: &str = "topicA";
    const TOPIC_B: &str = "topicB";
    const TOPIC_C: &str = "topicC";

    /// A test partitioner that uses sequential values instead of random.
    /// Translated from `BuiltInPartitionerTest.SequentialPartitioner`.
    struct SequentialPartitioner {
        inner: BuiltInPartitioner,
        mock_random: AtomicI32,
    }

    impl SequentialPartitioner {
        fn new(topic: &str, sticky_batch_size: i32) -> Self {
            Self {
                inner: BuiltInPartitioner::new(topic, sticky_batch_size),
                mock_random: AtomicI32::new(0),
            }
        }

        fn peek_current_partition_info(&mut self, cluster: &Cluster) -> &StickyPartitionInfo {
            if self.inner.sticky_partition_info.is_none() {
                let partition = self.next_partition(cluster);
                self.inner.sticky_partition_info = Some(StickyPartitionInfo::new(partition));
            }
            self.inner.sticky_partition_info.as_ref().unwrap()
        }

        fn update_partition_info(&mut self, appended_bytes: i32, cluster: &Cluster) {
            self.update_partition_info_with_switch(appended_bytes, cluster, true);
        }

        fn update_partition_info_with_switch(&mut self, appended_bytes: i32, cluster: &Cluster, enable_switch: bool) {
            if self.inner.sticky_partition_info.is_none() {
                return;
            }

            let produced_bytes = {
                let info = self.inner.sticky_partition_info.as_ref().unwrap();
                info.produced_bytes.fetch_add(appended_bytes, Ordering::Relaxed) + appended_bytes
            };

            if (produced_bytes >= self.inner.sticky_batch_size && enable_switch)
                || produced_bytes >= self.inner.sticky_batch_size * 2
            {
                let new_partition = self.next_partition(cluster);
                self.inner.sticky_partition_info = Some(StickyPartitionInfo::new(new_partition));
            }
        }

        fn next_partition(&mut self, cluster: &Cluster) -> i32 {
            let random = to_positive(self.mock_random.fetch_add(1, Ordering::Relaxed));
            if let Some(ref stats) = self.inner.partition_load_stats {
                debug_assert!(stats.length > 0);
                let cft = &stats.cumulative_frequency_table;
                let weighted_random = random % cft[stats.length - 1];
                let search_result = cft[..stats.length].binary_search(&weighted_random);
                let partition_index = match search_result {
                    Ok(index) => index + 1,
                    Err(insertion_point) => insertion_point,
                };
                debug_assert!(partition_index < stats.length);
                stats.partition_ids[partition_index]
            } else {
                let available = cluster.available_partitions_for_topic(&self.inner.topic);
                if !available.is_empty() {
                    available[(random as usize) % available.len()].partition()
                } else {
                    let partitions = cluster.partitions_for_topic(&self.inner.topic);
                    (random as usize % partitions.len()) as i32
                }
            }
        }

        fn update_partition_load_stats(
            &mut self,
            queue_sizes: Option<&mut [i32]>,
            partition_ids: &[i32],
            length: usize,
        ) {
            self.inner.update_partition_load_stats(queue_sizes, partition_ids, length);
        }

        fn load_stats_range_end(&self) -> i32 {
            self.inner.load_stats_range_end()
        }
    }

    fn make_cluster(nodes: &[Node], partitions: Vec<PartitionInfo>) -> Cluster {
        Cluster::new(
            Some("clusterId".to_string()),
            nodes.to_vec(),
            partitions,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        )
    }

    /// Translated from `BuiltInPartitionerTest.testStickyPartitioning`.
    #[test]
    fn test_sticky_partitioning() {
        let nodes = make_nodes();
        let all_partitions = vec![
            PartitionInfo::new(TOPIC_A.to_string(), 0, Some(nodes[0].clone()), nodes.clone(), nodes.clone()),
            PartitionInfo::new(TOPIC_A.to_string(), 1, Some(nodes[1].clone()), nodes.clone(), nodes.clone()),
            PartitionInfo::new(TOPIC_A.to_string(), 2, Some(nodes[2].clone()), nodes.clone(), nodes.clone()),
            PartitionInfo::new(TOPIC_B.to_string(), 0, Some(nodes[0].clone()), nodes.clone(), nodes.clone()),
        ];
        let test_cluster = make_cluster(&nodes, all_partitions);

        // Create partitions with "sticky" batch size to accommodate 3 records.
        let mut partitioner_a = SequentialPartitioner::new(TOPIC_A, 3);

        // Test the partition is not switched until sticky batch size is reached.
        let part_a = partitioner_a.peek_current_partition_info(&test_cluster).partition();
        partitioner_a.update_partition_info(1, &test_cluster);

        assert_eq!(part_a, partitioner_a.peek_current_partition_info(&test_cluster).partition());
        partitioner_a.update_partition_info(1, &test_cluster);

        assert_eq!(part_a, partitioner_a.peek_current_partition_info(&test_cluster).partition());
        partitioner_a.update_partition_info(1, &test_cluster);

        // After producing 3 records, partition must've switched.
        assert_ne!(part_a, partitioner_a.peek_current_partition_info(&test_cluster).partition());

        // Check that switching works even when there is one partition.
        let mut partitioner_b = SequentialPartitioner::new(TOPIC_B, 1);
        for _ in 0..10 {
            let info = partitioner_b.peek_current_partition_info(&test_cluster);
            assert_eq!(0, info.partition());
            partitioner_b.update_partition_info(1, &test_cluster);
        }
    }

    /// Translated from `BuiltInPartitionerTest.unavailablePartitionsTest`.
    #[test]
    fn unavailable_partitions_test() {
        let nodes = make_nodes();
        // Partition 1 in topic A, partition 0 in topic B and partition 0 in topic C are unavailable.
        let all_partitions = vec![
            PartitionInfo::new(TOPIC_A.to_string(), 0, Some(nodes[0].clone()), nodes.clone(), nodes.clone()),
            PartitionInfo::new(
                TOPIC_A.to_string(),
                1,
                None, // unavailable
                nodes.clone(),
                nodes.clone(),
            ),
            PartitionInfo::new(TOPIC_A.to_string(), 2, Some(nodes[2].clone()), nodes.clone(), nodes.clone()),
            PartitionInfo::new(
                TOPIC_B.to_string(),
                0,
                None, // unavailable
                nodes.clone(),
                nodes.clone(),
            ),
            PartitionInfo::new(TOPIC_B.to_string(), 1, Some(nodes[0].clone()), nodes.clone(), nodes.clone()),
            PartitionInfo::new(
                TOPIC_C.to_string(),
                0,
                None, // unavailable
                nodes.clone(),
                nodes.clone(),
            ),
        ];
        let cluster_nodes = vec![nodes[0].clone(), nodes[1].clone(), nodes[2].clone()];
        let test_cluster = make_cluster(&cluster_nodes, all_partitions);

        // Create partitions with "sticky" batch size to accommodate 1 record.
        let mut partitioner_a = BuiltInPartitioner::new(TOPIC_A, 1);

        // Assure we never choose partition 1 because it is unavailable.
        let part_a = partitioner_a.peek_current_partition_info(&test_cluster).partition();
        partitioner_a.update_partition_info(1, &test_cluster);

        let mut found_another_part_a = false;
        assert_ne!(1, part_a);
        for _ in 0..100 {
            let another_part_a = partitioner_a.peek_current_partition_info(&test_cluster).partition();
            partitioner_a.update_partition_info(1, &test_cluster);

            assert_ne!(1, another_part_a);
            found_another_part_a = found_another_part_a || another_part_a != part_a;
        }
        assert!(found_another_part_a, "Expected to find partition other than {}", part_a);

        let mut partitioner_b = BuiltInPartitioner::new(TOPIC_B, 1);
        // Assure we always choose partition 1 for topic B.
        let part_b = partitioner_b.peek_current_partition_info(&test_cluster).partition();
        partitioner_b.update_partition_info(1, &test_cluster);

        assert_eq!(1, part_b);
        for _ in 0..100 {
            let info = partitioner_b.peek_current_partition_info(&test_cluster);
            assert_eq!(1, info.partition());
            partitioner_b.update_partition_info(1, &test_cluster);
        }

        // Assure that we still choose the partition when there are no partitions available.
        let mut partitioner_c = BuiltInPartitioner::new(TOPIC_C, 1);
        let part_c = partitioner_c.peek_current_partition_info(&test_cluster).partition();
        partitioner_c.update_partition_info(1, &test_cluster);
        assert_eq!(0, part_c);

        let part_c = partitioner_c.peek_current_partition_info(&test_cluster).partition();
        assert_eq!(0, part_c);
    }

    /// Translated from `BuiltInPartitionerTest.adaptivePartitionsTest`.
    #[test]
    fn adaptive_partitions_test() {
        let nodes = make_nodes();
        let mut partitioner = SequentialPartitioner::new(TOPIC_A, 1);

        // Simulate partition queue sizes.
        let mut queue_sizes = vec![5, 0, 3, 0, 1];
        let partition_ids: Vec<i32> = (0..queue_sizes.len() as i32).collect();
        let expected_frequencies: Vec<i32> = queue_sizes.iter().map(|&qs| 6 - qs).collect(); // 6 is max(queue_sizes) + 1

        let all_partitions: Vec<PartitionInfo> = (0..partition_ids.len())
            .map(|i| {
                PartitionInfo::new(
                    TOPIC_A.to_string(),
                    i as i32,
                    Some(nodes[i % nodes.len()].clone()),
                    nodes.clone(),
                    nodes.clone(),
                )
            })
            .collect();

        partitioner.update_partition_load_stats(Some(&mut queue_sizes), &partition_ids, partition_ids.len());

        let test_cluster = make_cluster(&nodes, all_partitions);

        // Issue a certain number of partition calls to validate that the partitions would be
        // distributed with frequencies that are reciprocal to the queue sizes.
        let number_of_cycles = 2;
        let number_of_iterations = partitioner.load_stats_range_end() * number_of_cycles;
        let mut frequencies = vec![0i32; queue_sizes.len()];

        for _ in 0..number_of_iterations {
            let partition = partitioner.peek_current_partition_info(&test_cluster).partition();
            frequencies[partition as usize] += 1;
            partitioner.update_partition_info(1, &test_cluster);
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

    /// Translated from `BuiltInPartitionerTest.testStickyBatchSizeMoreThatZero`.
    #[test]
    fn test_sticky_batch_size_more_than_zero() {
        let result = std::panic::catch_unwind(|| BuiltInPartitioner::new(TOPIC_A, 0));
        assert!(result.is_err(), "Expected panic for sticky_batch_size=0");

        // Should not panic
        let _ = BuiltInPartitioner::new(TOPIC_A, 1);
    }
}
