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

#![expect(dead_code)]
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

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::common::Cluster;
use crate::common::Node;
use crate::common::PartitionInfo;
use crate::common::utils::Utils;
use crate::common::utils::internals::LogContext;
use crate::kafka_trace;

/// Hash function used to map a serialized record key to a partition.
///
/// # Deviation from Java parity (Definition-of-Done §7)
///
/// This enum has **no counterpart in the Java Kafka client**. Java's
/// `BuiltInPartitioner.partitionForKey` unconditionally uses murmur2
/// (`Utils.toPositive(Utils.murmur2(key)) % numPartitions`). It is introduced
/// here because this client intentionally changes the *default* key hash to
/// IEEE CRC-32, matching librdkafka's `consistent_random` partitioner
/// (`rd_kafka_msg_partitioner_consistent`: `rd_crc32(key, keylen) %
/// partition_cnt`) rather than the Java default. The motivation is
/// librdkafka migration: the vast majority of existing Confluent client
/// deployments (Go, Python, .NET, C/C++) are librdkafka-based and already
/// partition keyed records by CRC-32, so defaulting to CRC-32 lets this Rust
/// client co-partition with them out of the box.
///
/// Both hashers must remain selectable — CRC-32 for librdkafka parity (the
/// default) and murmur2 for exact Java parity — so the choice is modelled as
/// this `Copy` enum threaded through the (keyed) partition path. It is
/// selected via the `partitioner.type` producer config (see
/// [`ProducerConfig::key_hasher`](crate::producer::ProducerConfig)).
///
/// The keyless (sticky, KIP-794) partitioning path is **not** affected by this
/// enum; it is unchanged from Java.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum KeyHasher {
    /// IEEE 802.3 / zlib CRC-32 (`crc32fast::hash`), taken **unsigned** modulo
    /// the partition count. Matches librdkafka `consistent_random`. This is the
    /// default.
    #[default]
    Crc32,
    /// Kafka's murmur2 (`Utils.toPositive(Utils.murmur2(key)) % numPartitions`).
    /// Exact Java-client parity.
    Murmur2,
}

impl KeyHasher {
    /// Whether a record with this (present, non-ignored) serialized key should
    /// be hashed to a partition, or fall through to the keyless sticky path.
    ///
    /// - [`KeyHasher::Crc32`]: an **empty** key falls through to sticky
    ///   partitioning, matching librdkafka `consistent_random`
    ///   (`rd_kafka_msg_partitioner_consistent_random`: `keylen == 0` routes to
    ///   the random/sticky partitioner).
    /// - [`KeyHasher::Murmur2`]: every non-null key is hashed, including an
    ///   empty one, matching Java (`serializedKey != null` is the only gate).
    #[inline]
    pub(crate) fn hashes_key(self, key: &[u8]) -> bool {
        match self {
            KeyHasher::Crc32 => !key.is_empty(),
            KeyHasher::Murmur2 => true,
        }
    }
}

/// Information for the current sticky partition.
///
/// Translated from `BuiltInPartitioner.StickyPartitionInfo`.
#[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner$StickyPartitionInfo")]
pub struct StickyPartitionInfo {
    index: i32,
    produced_bytes: AtomicI32,
}

impl StickyPartitionInfo {
    /// Creates a new `StickyPartitionInfo` for the given partition index.
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner$StickyPartitionInfo#StickyPartitionInfo"
    )]
    pub fn new(index: i32) -> Self {
        Self { index, produced_bytes: AtomicI32::new(0) }
    }

    /// Returns the partition index.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner$StickyPartitionInfo#partition")]
    pub fn partition(&self) -> i32 {
        self.index
    }
}

/// The partition load stats for each topic that are used for adaptive partition
/// distribution.
///
/// Translated from `BuiltInPartitioner.PartitionLoadStats`.
#[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner$PartitionLoadStats")]
struct PartitionLoadStats {
    cumulative_frequency_table: Vec<i32>,
    partition_ids: Vec<i32>,
    length: usize,
}

impl PartitionLoadStats {
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner$PartitionLoadStats#PartitionLoadStats"
    )]
    fn new(cumulative_frequency_table: Vec<i32>, partition_ids: Vec<i32>, length: usize) -> Self {
        debug_assert_eq!(cumulative_frequency_table.len(), partition_ids.len());
        debug_assert!(length <= cumulative_frequency_table.len());
        Self { cumulative_frequency_table, partition_ids, length }
    }
}

/// The load stats of all the topic's partitions, plus, in rack-aware mode, the load
/// stats of only those whose leader is in the producer's rack.
///
/// Translated from `BuiltInPartitioner.PartitionLoadStatsHolder`.
#[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner$PartitionLoadStatsHolder")]
struct PartitionLoadStatsHolder {
    total: PartitionLoadStats,
    /// `None` unless the partitioner is rack-aware (Java's `null`).
    in_this_rack: Option<PartitionLoadStats>,
}

impl PartitionLoadStatsHolder {
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner$PartitionLoadStatsHolder#PartitionLoadStatsHolder"
    )]
    fn new(total: PartitionLoadStats, in_this_rack: Option<PartitionLoadStats>) -> Self {
        Self { total, in_this_rack }
    }
}

/// Built-in default partitioner with adaptive sticky partitioning.
///
/// Translated from `org.apache.kafka.clients.producer.internals.BuiltInPartitioner`.
///
/// There is one partitioner object per topic. The partitioner uses sticky
/// partitioning to avoid switching partitions too frequently, and uses adaptive
/// load stats to distribute records based on queue sizes.
#[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner")]
pub struct BuiltInPartitioner {
    topic: Arc<str>,
    sticky_batch_size: i32,
    /// Whether the partitioner prioritizes partitions whose leaders are in
    /// [`Self::rack`] (`partitioner.rack.aware`, KIP-1123).
    rack_aware: bool,
    /// The producer's rack (`client.rack`). Shared with the accumulator and every
    /// other topic's partitioner, so it is never copied.
    rack: Arc<str>,
    partition_load_stats_holder: Option<PartitionLoadStatsHolder>,
    sticky_partition_info: Option<StickyPartitionInfo>,
    /// Contextual log message prefix.
    log_context: LogContext,
    /// Test seam replacing Java's `randomPartition()` override (the tests'
    /// `SequentialPartitioner`): when set, [`Self::random_partition`] returns
    /// successive values of this counter. Shared, because Java's
    /// `RecordAccumulatorTest` shares one `mockRandom` across all its topics.
    #[cfg(test)]
    mock_random: Option<Arc<AtomicI32>>,
}

impl BuiltInPartitioner {
    /// Creates a new `BuiltInPartitioner`.
    ///
    /// # Arguments
    /// * `topic` - The topic
    /// * `sticky_batch_size` - How much to produce to partition before switch
    /// * `rack_aware` - Whether the partitioner is rack-aware,
    ///   i.e. prioritizes partitions whose leaders are in the same rack as the producer
    /// * `rack` - The rack of the producer (needed for the rack-aware mode)
    ///
    /// # Panics
    /// Panics if `sticky_batch_size` is less than 1.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#BuiltInPartitioner")]
    pub fn new(topic: &Arc<str>, sticky_batch_size: i32, rack_aware: bool, rack: &Arc<str>) -> Self {
        Self::with_log_context(topic, sticky_batch_size, rack_aware, rack, LogContext::empty())
    }

    /// Creates a new `BuiltInPartitioner` with a `LogContext`.
    ///
    /// # Arguments
    /// * `topic` - The topic
    /// * `sticky_batch_size` - How much to produce to partition before switch
    /// * `rack_aware` - Whether the partitioner is rack-aware,
    ///   i.e. prioritizes partitions whose leaders are in the same rack as the producer
    /// * `rack` - The rack of the producer (needed for the rack-aware mode)
    /// * `log_context` - Contextual log message prefix
    ///
    /// # Panics
    /// Panics if `sticky_batch_size` is less than 1.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#BuiltInPartitioner")]
    pub fn with_log_context(
        topic: &Arc<str>,
        sticky_batch_size: i32,
        rack_aware: bool,
        rack: &Arc<str>,
        log_context: LogContext,
    ) -> Self {
        assert!(
            sticky_batch_size >= 1,
            "sticky_batch_size must be >= 1 but got {}",
            sticky_batch_size
        );
        Self {
            topic: Arc::clone(topic),
            sticky_batch_size,
            rack_aware,
            rack: Arc::clone(rack),
            partition_load_stats_holder: None,
            sticky_partition_info: None,
            log_context,
            #[cfg(test)]
            mock_random: None,
        }
    }

    /// Test-only: makes [`Self::random_partition`] return successive values of
    /// `mock_random`, as Java's tests do by overriding `randomPartition()`.
    #[cfg(test)]
    pub(crate) fn with_mock_random_for_test(mut self, mock_random: Arc<AtomicI32>) -> Self {
        self.mock_random = Some(mock_random);
        self
    }

    /// Whether `partition`'s leader is in this producer's rack. A partition without
    /// a leader, or whose leader has no rack, is not (Java's
    /// `p.leader().hasRack() && p.leader().rack().equals(rack)`).
    #[inline]
    fn is_leader_in_this_rack(&self, partition: &PartitionInfo) -> bool {
        partition.leader().and_then(Node::rack) == Some(&*self.rack)
    }

    /// Calculate the next partition for the topic based on the partition load stats.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#nextPartition")]
    fn next_partition(&mut self, cluster: &Cluster) -> i32 {
        let random = self.random_partition();

        let partition = if let Some(ref holder) = self.partition_load_stats_holder {
            // Calculate next partition based on load distribution.
            // Note that partitions without leader are excluded from the partition_load_stats.

            let stats = match holder.in_this_rack {
                Some(ref in_this_rack) if self.rack_aware && in_this_rack.length > 0 => in_this_rack,
                _ => &holder.total,
            };

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
                // Select only partitions with leaders in this rack if configured so, falling back if none are available.
                //
                // Java collects the in-rack partitions into a new list. This runs on the
                // send path (on every partition switch), so the in-rack partitions are
                // counted and then indexed in place instead: same choice, no allocation.
                let in_this_rack = if self.rack_aware {
                    available.iter().filter(|p| self.is_leader_in_this_rack(p)).count()
                } else {
                    0
                };
                let in_rack_choice = if in_this_rack > 0 {
                    available
                        .iter()
                        .filter(|p| self.is_leader_in_this_rack(p))
                        .nth((random as usize) % in_this_rack)
                } else {
                    None
                };
                match in_rack_choice {
                    Some(p) => p.partition(),
                    None => available[(random as usize) % available.len()].partition(),
                }
            } else {
                // We don't have available partitions, just pick one among all partitions.
                let partitions = cluster.partitions_for_topic(&self.topic);
                (random as usize % partitions.len()) as i32
            }
        };

        kafka_trace!(self.log_context, "Switching to partition {} in topic {}", partition, self.topic);
        partition
    }

    /// Generate a random positive integer for partition selection.
    ///
    /// This method can be overridden in tests to provide deterministic behavior.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#randomPartition")]
    fn random_partition(&mut self) -> i32 {
        #[cfg(test)]
        if let Some(ref mock_random) = self.mock_random {
            return mock_random.fetch_add(1, Ordering::Relaxed);
        }
        Utils::to_positive(rand::random::<i32>())
    }

    /// Test-only function. When partition load stats are defined, return the end
    /// of range for the random number.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#loadStatsRangeEnd")]
    pub fn load_stats_range_end(&self) -> i32 {
        let holder = self
            .partition_load_stats_holder
            .as_ref()
            .expect("partition_load_stats_holder must be set");
        debug_assert!(holder.total.length > 0);
        holder.total.cumulative_frequency_table[holder.total.length - 1]
    }

    /// Test-only function. When the partition load stats of the producer's rack are
    /// defined, return the end of range for the random number.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#loadStatsInThisRackRangeEnd")]
    pub fn load_stats_in_this_rack_range_end(&self) -> i32 {
        let stats = self
            .partition_load_stats_holder
            .as_ref()
            .and_then(|holder| holder.in_this_rack.as_ref())
            .expect("partition_load_stats_holder.in_this_rack must be set");
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#peekCurrentPartitionInfo")]
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
    /// Compares `partition_info` by identity with the current sticky info, as Java does. A
    /// concurrent switch *is* possible: the accumulator drops the partitioner lock between its
    /// peek and the deque lock, and between its two locked blocks. The accumulator does not call
    /// this today, though — its `partition_changed` re-reads the partition instead, so it cannot see
    /// such a switch (Critic 98 S2, deferred to a human decision).
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#isPartitionChanged")]
    pub fn is_partition_changed(&self, partition_info: &StickyPartitionInfo) -> bool {
        // Pointer identity, as in Java.
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#updatePartitionInfo")]
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#updatePartitionInfo")]
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
            kafka_trace!(
                self.log_context,
                "Produced {} bytes, exceeding twice the batch size of {} bytes, with switching set to {}",
                produced_bytes,
                self.sticky_batch_size,
                enable_switch
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
    /// * `partition_ids` - The partition ids for the queues, partitions without leaders are excluded
    /// * `partition_leader_racks` - The racks of partition leaders for the queues, partitions without
    ///   leaders are excluded (`None` for a leader without a rack)
    /// * `length` - The logical length of the arrays (could be less than actual length): we may
    ///   eliminate some partitions based on latency, but to avoid reallocation of the arrays, we
    ///   just decrement logical length
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#updatePartitionLoadStats")]
    pub fn update_partition_load_stats(
        &mut self,
        queue_sizes: Option<&mut [i32]>,
        partition_ids: &[i32],
        partition_leader_racks: &[Option<&str>],
        length: usize,
    ) {
        let queue_sizes = match queue_sizes {
            Some(qs) => qs,
            None => {
                kafka_trace!(self.log_context, "No load stats for topic {}, not using adaptive", self.topic);
                self.partition_load_stats_holder = None;
                return;
            },
        };
        debug_assert_eq!(queue_sizes.len(), partition_ids.len());
        debug_assert_eq!(queue_sizes.len(), partition_leader_racks.len());
        debug_assert!(length <= queue_sizes.len());

        // The queue_sizes.len() represents the number of all partitions in the topic and if we have
        // less than 2 partitions, there is no need to do adaptive logic.
        if length < 1 || queue_sizes.len() < 2 {
            kafka_trace!(
                self.log_context,
                "The number of partitions is too small: available={}, all={}, not using adaptive for topic {}",
                length,
                queue_sizes.len(),
                self.topic
            );
            self.partition_load_stats_holder = None;
            return;
        }

        // Do the same with this-rack-only partitions if rack awareness is enabled.

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
            kafka_trace!(
                self.log_context,
                "All queue lengths are the same, not using adaptive for topic {}",
                self.topic
            );
            self.partition_load_stats_holder = None;
            return;
        }

        // Before inverting and folding, build fully the load stats for this rack, because this depends on the raw queue sizes.
        let partition_load_stats_in_this_rack = self.create_partition_load_stats_for_this_rack_if_needed(
            queue_sizes,
            partition_ids,
            partition_leader_racks,
            length,
        );

        // Invert and fold the queue size, so that they become separator values in the CFT.
        Self::invert_and_fold_queue_size_array(queue_sizes, max_size_plus1, length);

        if log::log_enabled!(log::Level::Trace) {
            match partition_load_stats_in_this_rack {
                Some(ref in_this_rack) if self.rack_aware => kafka_trace!(
                    self.log_context,
                    "Partition load stats for topic {}: CFT={:?}, IDs={:?}, length={}; in producer rack: CFT={:?}, IDs={:?}, length={}",
                    self.topic,
                    &queue_sizes[..length],
                    &partition_ids[..length],
                    length,
                    &in_this_rack.cumulative_frequency_table[..in_this_rack.length],
                    &in_this_rack.partition_ids[..in_this_rack.length],
                    in_this_rack.length
                ),
                _ => {
                    debug_assert!(!self.rack_aware, "rack-aware load stats must be built in rack-aware mode");
                    kafka_trace!(
                        self.log_context,
                        "Partition load stats for topic {}: CFT={:?}, IDs={:?}, length={}",
                        self.topic,
                        &queue_sizes[..length],
                        &partition_ids[..length],
                        length
                    );
                },
            }
        }
        self.partition_load_stats_holder = Some(PartitionLoadStatsHolder::new(
            PartitionLoadStats::new(queue_sizes.to_vec(), partition_ids.to_vec(), length),
            partition_load_stats_in_this_rack,
        ));
    }

    /// Builds the load stats of the partitions whose leader is in the producer's
    /// rack, from the raw (not yet inverted) queue sizes; `None` unless rack-aware.
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#createPartitionLoadStatsForThisRackIfNeeded"
    )]
    fn create_partition_load_stats_for_this_rack_if_needed(
        &self,
        queue_sizes: &[i32],
        partition_ids: &[i32],
        partition_leader_racks: &[Option<&str>],
        length: usize,
    ) -> Option<PartitionLoadStats> {
        if !self.rack_aware {
            return None;
        }
        let mut queue_sizes_in_this_rack = vec![0; length];
        let mut partition_ids_in_this_rack = vec![0; length];
        let mut length_in_this_rack = 0;
        let mut max_size_plus1_in_this_rack = -1;

        for i in 0..length {
            if partition_leader_racks[i] == Some(&*self.rack) {
                queue_sizes_in_this_rack[length_in_this_rack] = queue_sizes[i];
                partition_ids_in_this_rack[length_in_this_rack] = partition_ids[i];

                if queue_sizes[i] > max_size_plus1_in_this_rack {
                    max_size_plus1_in_this_rack = queue_sizes[i];
                }

                length_in_this_rack += 1;
            }
        }
        max_size_plus1_in_this_rack += 1;

        Self::invert_and_fold_queue_size_array(
            &mut queue_sizes_in_this_rack,
            max_size_plus1_in_this_rack,
            length_in_this_rack,
        );
        Some(PartitionLoadStats::new(
            queue_sizes_in_this_rack,
            partition_ids_in_this_rack,
            length_in_this_rack,
        ))
    }

    /// Inverts and folds the first `length` queue sizes in place, so that they become
    /// separator values in the cumulative frequency table.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#invertAndFoldQueueSizeArray")]
    fn invert_and_fold_queue_size_array(queue_sizes: &mut [i32], max_size_plus1: i32, length: usize) {
        if length == 0 {
            return;
        }
        queue_sizes[0] = max_size_plus1 - queue_sizes[0];
        for i in 1..length {
            queue_sizes[i] = max_size_plus1 - queue_sizes[i] + queue_sizes[i - 1];
        }
    }

    /// Hashing function to choose a partition from the serialized key bytes.
    ///
    /// Translated from `BuiltInPartitioner.partitionForKey`, generalised over the
    /// [`KeyHasher`] so the default (CRC-32, librdkafka parity) and the murmur2
    /// (Java parity) variants share one call path. `num_partitions` must be
    /// positive — callers guard this.
    ///
    /// - [`KeyHasher::Crc32`]: `(crc32(key) % num_partitions)` with the modulo
    ///   computed on **unsigned** values, matching librdkafka
    ///   (`rd_crc32(key, keylen) % partition_cnt`, where `rd_crc32_t` is
    ///   `uint32_t` so the `int32_t` count is promoted to unsigned). There is
    ///   deliberately **no** `to_positive`/`& 0x7fffffff` masking here — that
    ///   would give a different partition for keys whose CRC has the high bit
    ///   set.
    /// - [`KeyHasher::Murmur2`]: `Utils.toPositive(Utils.murmur2(key)) %
    ///   num_partitions`, identical to the Java client.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitioner#partitionForKey")]
    pub fn partition_for_key(serialized_key: &[u8], num_partitions: i32, hasher: KeyHasher) -> i32 {
        match hasher {
            KeyHasher::Crc32 => (crc32fast::hash(serialized_key) % (num_partitions as u32)) as i32,
            KeyHasher::Murmur2 => Utils::to_positive(Utils::murmur2(serialized_key)) % num_partitions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    /// Translated from `BuiltInPartitionerTest.NODES`: five brokers over three racks.
    fn make_nodes() -> Vec<Node> {
        vec![
            Node::with_rack(0, "localhost".to_string(), 99, Some("rack0".to_string())),
            Node::with_rack(1, "localhost".to_string(), 100, Some("rack1".to_string())),
            Node::with_rack(2, "localhost".to_string(), 101, Some("rack0".to_string())),
            Node::with_rack(3, "localhost".to_string(), 102, Some("rack1".to_string())),
            Node::with_rack(11, "localhost".to_string(), 103, Some("rack2".to_string())),
        ]
    }

    /// Translated from `BuiltInPartitionerTest.NODES_WITHOUT_RACKS`: the same brokers
    /// with no rack.
    fn make_nodes_without_racks() -> Vec<Node> {
        make_nodes()
            .iter()
            .map(|n| Node::new(n.id(), n.host().to_string(), n.port()))
            .collect()
    }

    const TOPIC_A: &str = "topicA";
    const TOPIC_B: &str = "topicB";
    const TOPIC_C: &str = "topicC";

    fn topic_arc(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    /// `new PartitionInfo(topic, partition, leader, replicas, isr)`.
    fn partition_info(
        topic: &str,
        partition: i32,
        leader: Option<&Node>,
        replicas: &[Node],
        isr: &[Node],
    ) -> PartitionInfo {
        PartitionInfo::new(topic.to_string(), partition, leader.cloned(), replicas.to_vec(), isr.to_vec())
    }

    /// `new BuiltInPartitioner(logContext, topic, stickyBatchSize, rackAware, rack)`.
    fn built_in_partitioner(topic: &str, sticky_batch_size: i32, rack_aware: bool, rack: &str) -> BuiltInPartitioner {
        BuiltInPartitioner::new(&topic_arc(topic), sticky_batch_size, rack_aware, &topic_arc(rack))
    }

    /// Translated from `BuiltInPartitionerTest.SequentialPartitioner`: a partitioner
    /// whose `randomPartition()` returns 0, 1, 2, ... instead of random values.
    ///
    /// Java subclasses `BuiltInPartitioner` and overrides only `randomPartition()`, so
    /// every other method is the production one. This does the same through the
    /// `mock_random` seam, rather than re-implementing the partition choice in the
    /// fixture (definition-of-done.md §12).
    fn sequential_partitioner(topic: &str, sticky_batch_size: i32, rack_aware: bool, rack: &str) -> BuiltInPartitioner {
        built_in_partitioner(topic, sticky_batch_size, rack_aware, rack)
            .with_mock_random_for_test(Arc::new(AtomicI32::new(0)))
    }

    fn make_cluster(nodes: &[Node], partitions: Vec<PartitionInfo>) -> Cluster {
        Cluster::with_invalid_topics_controller_topic_ids(
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
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitionerTest#testStickyPartitioning")]
    fn test_sticky_partitioning() {
        let nodes = make_nodes();
        let all_partitions = vec![
            partition_info(TOPIC_A, 0, Some(&nodes[0]), &nodes, &nodes),
            partition_info(TOPIC_A, 1, Some(&nodes[1]), &nodes, &nodes),
            partition_info(TOPIC_A, 2, Some(&nodes[2]), &nodes, &nodes),
            partition_info(TOPIC_B, 0, Some(&nodes[0]), &nodes, &nodes),
        ];
        let test_cluster = make_cluster(&nodes, all_partitions);

        let rack_aware = false;
        let client_rack_id = "";

        // Create partitions with "sticky" batch size to accommodate 3 records.
        let mut partitioner_a = sequential_partitioner(TOPIC_A, 3, rack_aware, client_rack_id);

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
        let mut partitioner_b = sequential_partitioner(TOPIC_B, 1, rack_aware, client_rack_id);
        for _ in 0..10 {
            let info = partitioner_b.peek_current_partition_info(&test_cluster);
            assert_eq!(0, info.partition());
            partitioner_b.update_partition_info(1, &test_cluster);
        }
    }

    /// Translated from `BuiltInPartitionerTest.testStickyPartitioningWithRackAwareness`.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitionerTest#testStickyPartitioningWithRackAwareness"
    )]
    fn test_sticky_partitioning_with_rack_awareness() {
        let nodes = make_nodes();
        let all_partitions_online = vec![
            partition_info(TOPIC_A, 0, Some(&nodes[0]), &nodes, &nodes),
            partition_info(TOPIC_A, 1, Some(&nodes[1]), &nodes, &nodes),
            partition_info(TOPIC_A, 2, Some(&nodes[2]), &nodes, &nodes),
            partition_info(TOPIC_A, 3, Some(&nodes[3]), &nodes, &nodes),
            partition_info(TOPIC_B, 0, Some(&nodes[0]), &nodes, &nodes),
        ];
        let mut test_cluster = make_cluster(&nodes, all_partitions_online.clone());

        // Create partitions with "sticky" batch size to accommodate 1 record.
        let mut partitioner_a = sequential_partitioner(TOPIC_A, 1, true, nodes[0].rack().unwrap());

        // While partitions in "our" rack are online, the partitioner must switch between them.
        assert_eq!(0, partitioner_a.peek_current_partition_info(&test_cluster).partition());
        partitioner_a.update_partition_info(1, &test_cluster);

        assert_eq!(2, partitioner_a.peek_current_partition_info(&test_cluster).partition());
        partitioner_a.update_partition_info(1, &test_cluster);

        assert_eq!(0, partitioner_a.peek_current_partition_info(&test_cluster).partition());

        // Simulate one partition in "our" rack going offline.
        // The partitioner must select the remaining one.
        let one_partition_offline = vec![
            partition_info(TOPIC_A, 0, Some(&nodes[0]), &nodes, &nodes),
            partition_info(TOPIC_A, 1, Some(&nodes[1]), &nodes, &nodes),
            partition_info(TOPIC_A, 2, None, &nodes, &[]),
            partition_info(TOPIC_A, 3, Some(&nodes[3]), &nodes, &nodes),
            partition_info(TOPIC_B, 0, Some(&nodes[0]), &nodes, &nodes),
        ];
        test_cluster = make_cluster(&nodes, one_partition_offline);
        partitioner_a.update_partition_info(1, &test_cluster);

        assert_eq!(0, partitioner_a.peek_current_partition_info(&test_cluster).partition());
        partitioner_a.update_partition_info(1, &test_cluster);

        assert_eq!(0, partitioner_a.peek_current_partition_info(&test_cluster).partition());

        // Simulate all partitions in "our" rack going offline.
        // The partitioner must start selecting from "non-local" partitions.
        let two_partitions_offline = vec![
            partition_info(TOPIC_A, 0, None, &nodes, &[]),
            partition_info(TOPIC_A, 1, Some(&nodes[1]), &nodes, &nodes),
            partition_info(TOPIC_A, 2, None, &nodes, &[]),
            partition_info(TOPIC_A, 3, Some(&nodes[3]), &nodes, &nodes),
            partition_info(TOPIC_B, 0, Some(&nodes[0]), &nodes, &nodes),
        ];
        test_cluster = make_cluster(&nodes, two_partitions_offline);
        partitioner_a.update_partition_info(1, &test_cluster);
        assert_eq!(3, partitioner_a.peek_current_partition_info(&test_cluster).partition());

        // When the local partitions are back online, the partitioner should again pick them.
        test_cluster = make_cluster(&nodes, all_partitions_online);
        partitioner_a.update_partition_info(1, &test_cluster);
        assert_eq!(0, partitioner_a.peek_current_partition_info(&test_cluster).partition());

        // Test the situation of brokers without racks.
        let nodes_without_racks = make_nodes_without_racks();
        let nwr = &nodes_without_racks;
        let all_partitions_online_without_racks = vec![
            partition_info(TOPIC_A, 0, Some(&nwr[0]), nwr, nwr),
            partition_info(TOPIC_A, 1, Some(&nwr[1]), nwr, nwr),
            partition_info(TOPIC_A, 2, Some(&nwr[2]), nwr, nwr),
            partition_info(TOPIC_A, 3, Some(&nwr[3]), nwr, nwr),
            partition_info(TOPIC_B, 0, Some(&nwr[0]), nwr, nwr),
        ];
        test_cluster = make_cluster(&nodes, all_partitions_online_without_racks);
        for expected_partition in [3, 0, 1, 2, 3] {
            partitioner_a.update_partition_info(1, &test_cluster);
            assert_eq!(
                expected_partition,
                partitioner_a.peek_current_partition_info(&test_cluster).partition()
            );
        }
    }

    /// Translated from `BuiltInPartitionerTest.unavailablePartitionsTest`, a
    /// `@ParameterizedTest` over `(rackAware, rack)`; the `@CsvSource` cases are
    /// looped. Java's `"false,"` passes a `null` rack, which is `""` here (the rack
    /// is unused when not rack-aware).
    #[test]
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitionerTest#unavailablePartitionsTest")]
    fn unavailable_partitions_test() {
        for (rack_aware, rack) in [(false, ""), (true, "rack0"), (true, "rack1"), (true, "rack2")] {
            let nodes = make_nodes();
            // Partition 1 in topic A, partition 0 in topic B and partition 0 in topic C are unavailable partitions.
            let all_partitions = vec![
                partition_info(TOPIC_A, 0, Some(&nodes[0]), &nodes, &nodes),
                partition_info(TOPIC_A, 1, None, &nodes, &nodes),
                partition_info(TOPIC_A, 2, Some(&nodes[2]), &nodes, &nodes),
                partition_info(TOPIC_B, 0, None, &nodes, &nodes),
                partition_info(TOPIC_B, 1, Some(&nodes[0]), &nodes, &nodes),
                partition_info(TOPIC_C, 0, None, &nodes, &nodes),
            ];
            let test_cluster = make_cluster(&nodes[..3], all_partitions);

            // Create partitions with "sticky" batch size to accommodate 1 record.
            let mut partitioner_a = built_in_partitioner(TOPIC_A, 1, rack_aware, rack);

            // Assure we never choose partition 1 because it is unavailable.
            let part_a = partitioner_a.peek_current_partition_info(&test_cluster).partition();
            partitioner_a.update_partition_info(1, &test_cluster);

            let mut found_another_part_a = false;
            assert_ne!(1, part_a, "rack_aware={rack_aware}, rack={rack}");
            for _ in 0..100 {
                let another_part_a = partitioner_a.peek_current_partition_info(&test_cluster).partition();
                partitioner_a.update_partition_info(1, &test_cluster);

                assert_ne!(1, another_part_a, "rack_aware={rack_aware}, rack={rack}");
                found_another_part_a = found_another_part_a || another_part_a != part_a;
            }
            assert!(found_another_part_a, "Expected to find partition other than {}", part_a);

            let mut partitioner_b = built_in_partitioner(TOPIC_B, 1, rack_aware, rack);
            // Assure we always choose partition 1 for topic B.
            let part_b = partitioner_b.peek_current_partition_info(&test_cluster).partition();
            partitioner_b.update_partition_info(1, &test_cluster);

            assert_eq!(1, part_b, "rack_aware={rack_aware}, rack={rack}");
            for _ in 0..100 {
                let info = partitioner_b.peek_current_partition_info(&test_cluster);
                assert_eq!(1, info.partition(), "rack_aware={rack_aware}, rack={rack}");
                partitioner_b.update_partition_info(1, &test_cluster);
            }

            // Assure that we still choose the partition when there are no partitions available.
            let mut partitioner_c = built_in_partitioner(TOPIC_C, 1, rack_aware, rack);
            let part_c = partitioner_c.peek_current_partition_info(&test_cluster).partition();
            partitioner_c.update_partition_info(1, &test_cluster);
            assert_eq!(0, part_c, "rack_aware={rack_aware}, rack={rack}");

            let part_c = partitioner_c.peek_current_partition_info(&test_cluster).partition();
            assert_eq!(0, part_c, "rack_aware={rack_aware}, rack={rack}");
        }
    }

    /// Translated from `BuiltInPartitionerTest.adaptivePartitionsTest`, a
    /// `@ParameterizedTest` over `(brokerRacksArePresent, clientRackAware, clientRack)`;
    /// the `@CsvSource` cases are looped. All these cases exclude rack-aware
    /// partitioning, but ensure various combinations of broker and client rack
    /// settings don't cause problems.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitionerTest#adaptivePartitionsTest")]
    fn adaptive_partitions_test() {
        for (broker_racks_are_present, client_rack_aware, client_rack) in
            [(false, false, ""), (true, false, ""), (false, true, "rack0")]
        {
            let nodes = make_nodes();
            let mut partitioner = sequential_partitioner(TOPIC_A, 1, client_rack_aware, client_rack);

            // Simulate partition queue sizes.
            let mut queue_sizes = vec![5, 0, 3, 0, 1];
            let mut partition_ids = vec![0; queue_sizes.len()];
            let mut partition_racks: Vec<Option<&str>> = vec![None; queue_sizes.len()];
            let mut expected_frequencies = vec![0; queue_sizes.len()];
            let mut all_partitions = Vec::new();
            for i in 0..partition_ids.len() {
                let leader = &nodes[i % nodes.len()];
                partition_ids[i] = i as i32;
                if broker_racks_are_present {
                    partition_racks[i] = leader.rack();
                }
                all_partitions.push(partition_info(TOPIC_A, i as i32, Some(leader), &nodes, &nodes));
                expected_frequencies[i] = 6 - queue_sizes[i]; // 6 is max(queueSizes) + 1
            }

            let length = queue_sizes.len();
            partitioner.update_partition_load_stats(Some(&mut queue_sizes), &partition_ids, &partition_racks, length);

            let test_cluster = make_cluster(&nodes, all_partitions);

            // Issue a certain number of partition calls to validate that the partitions would be
            // distributed with frequencies that are reciprocal to the queue sizes.  The number of
            // iterations is defined by the last element of the cumulative frequency table which is
            // the sum of all frequencies.  We do 2 cycles, just so it's more than 1.
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
                    "Partition {} was chosen {} times (case {broker_racks_are_present},{client_rack_aware},{client_rack})",
                    i,
                    frequencies[i]
                );
            }
        }
    }

    /// Translated from `BuiltInPartitionerTest.adaptivePartitionsTestWithRackAwareness`.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.internals.BuiltInPartitionerTest#adaptivePartitionsTestWithRackAwareness"
    )]
    fn adaptive_partitions_test_with_rack_awareness() {
        let nodes = make_nodes();
        let rack = nodes[0].rack().unwrap();
        let mut partitioner = sequential_partitioner(TOPIC_A, 1, true, rack);

        // Simulate partition queue sizes.
        let mut queue_sizes = vec![5, 0, 3, 0];
        let mut partition_ids = vec![0; queue_sizes.len()];
        let mut partition_racks: Vec<Option<&str>> = vec![None; queue_sizes.len()];
        let mut expected_frequencies = vec![0; queue_sizes.len()];
        let mut all_partitions = Vec::new();
        for i in 0..partition_ids.len() {
            let leader = &nodes[i % nodes.len()];
            partition_ids[i] = i as i32;
            partition_racks[i] = leader.rack();
            all_partitions.push(partition_info(TOPIC_A, i as i32, Some(leader), &nodes, &nodes));

            if leader.rack() == Some(rack) {
                expected_frequencies[i] = 6 - queue_sizes[i]; // 6 is max(queueSizes) + 1
            }
        }

        let length = queue_sizes.len();
        partitioner.update_partition_load_stats(Some(&mut queue_sizes), &partition_ids, &partition_racks, length);

        let mut test_cluster = make_cluster(&nodes, all_partitions);

        // Issue a certain number of partition calls to validate that the partitions would be
        // distributed with frequencies that are reciprocal to the queue sizes.  The number of
        // iterations is defined by the last element of the cumulative frequency table which is
        // the sum of all frequencies.  We do 2 cycles, just so it's more than 1.
        let number_of_cycles = 2;
        let number_of_iterations = partitioner.load_stats_in_this_rack_range_end() * number_of_cycles;
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

        // Simulate one partition in "our" rack going offline.
        // The partitioner must select the remaining one.
        let mut queue_sizes = vec![1, 2, 3];
        let partition_ids = vec![0, 1, 3];
        let partition_racks = vec![nodes[0].rack(), nodes[1].rack(), nodes[3].rack()];
        let length = queue_sizes.len();
        partitioner.update_partition_load_stats(Some(&mut queue_sizes), &partition_ids, &partition_racks, length);

        let one_partition_offline = vec![
            partition_info(TOPIC_A, 0, Some(&nodes[0]), &nodes, &nodes),
            partition_info(TOPIC_A, 1, Some(&nodes[1]), &nodes, &nodes),
            partition_info(TOPIC_A, 2, None, &nodes, &[]),
            partition_info(TOPIC_A, 3, Some(&nodes[3]), &nodes, &nodes),
        ];
        test_cluster = make_cluster(&nodes, one_partition_offline);
        partitioner.peek_current_partition_info(&test_cluster);
        for _ in 0..4 {
            partitioner.update_partition_info(1, &test_cluster);
            assert_eq!(0, partitioner.peek_current_partition_info(&test_cluster).partition());
        }

        // Simulate all partitions in "our" rack going offline.
        // The partitioner must start selecting from "non-local" partitions.
        let mut queue_sizes = vec![1, 2];
        let partition_ids = vec![1, 3];
        let partition_racks = vec![nodes[1].rack(), nodes[3].rack()];
        let length = queue_sizes.len();
        partitioner.update_partition_load_stats(Some(&mut queue_sizes), &partition_ids, &partition_racks, length);

        let two_partitions_offline = vec![
            partition_info(TOPIC_A, 0, None, &nodes, &[]),
            partition_info(TOPIC_A, 1, Some(&nodes[1]), &nodes, &nodes),
            partition_info(TOPIC_A, 2, None, &nodes, &[]),
            partition_info(TOPIC_A, 3, Some(&nodes[3]), &nodes, &nodes),
        ];
        test_cluster = make_cluster(&nodes, two_partitions_offline);
        partitioner.update_partition_info(1, &test_cluster);
        assert_eq!(1, partitioner.peek_current_partition_info(&test_cluster).partition());
        partitioner.update_partition_info(1, &test_cluster);
        assert_eq!(3, partitioner.peek_current_partition_info(&test_cluster).partition());
    }

    /// The rack-aware, non-adaptive choice is made without collecting the in-rack
    /// partitions (Java's `Collectors.toList()`), so it must pick exactly what
    /// indexing Java's filtered list would: the `(random % n)`-th in-rack
    /// partition, in cluster order. Checked against that list for every random
    /// value over two full cycles, for each rack (rack2's only leader is broker 11).
    #[test]
    fn test_rack_aware_uniform_choice_matches_filtered_list() {
        let nodes = make_nodes();
        let partitions: Vec<PartitionInfo> = (0..10)
            .map(|i| partition_info(TOPIC_A, i, Some(&nodes[i as usize % nodes.len()]), &nodes, &nodes))
            .collect();
        let test_cluster = make_cluster(&nodes, partitions);
        for rack in ["rack0", "rack1", "rack2"] {
            let in_rack: Vec<i32> = test_cluster
                .available_partitions_for_topic(TOPIC_A)
                .iter()
                .filter(|p| p.leader().and_then(Node::rack) == Some(rack))
                .map(PartitionInfo::partition)
                .collect();
            assert!(!in_rack.is_empty());
            let mut partitioner = sequential_partitioner(TOPIC_A, 1, true, rack);
            for random in 0..(2 * in_rack.len()) {
                assert_eq!(
                    in_rack[random % in_rack.len()],
                    partitioner.peek_current_partition_info(&test_cluster).partition(),
                    "rack={rack}, random={random}"
                );
                partitioner.update_partition_info(1, &test_cluster);
            }
        }
    }

    /// `definition-of-done.md` §10 / CLAUDE.md §13: a partition switch is on the
    /// send path (it runs inside `RecordAccumulator::append` every `batch.size`
    /// bytes), so the rack-aware choice added by KIP-1123 must not allocate, where
    /// Java collects the in-rack partitions into a new list. Measured for every
    /// mode the switch can take: uniform and adaptive, each with rack awareness off,
    /// on with in-rack partitions, and on with none (the fallback).
    #[test]
    fn test_partition_switch_does_not_allocate() {
        let nodes = make_nodes();
        let partitions: Vec<PartitionInfo> = (0..6)
            .map(|i| partition_info(TOPIC_A, i, Some(&nodes[i as usize % nodes.len()]), &nodes, &nodes))
            .collect();
        let test_cluster = make_cluster(&nodes, partitions);
        for adaptive in [false, true] {
            for (rack_aware, rack) in [(false, ""), (true, "rack0"), (true, "no-such-rack")] {
                let mut partitioner = sequential_partitioner(TOPIC_A, 1, rack_aware, rack);
                if adaptive {
                    let mut queue_sizes = vec![3, 0, 1, 2, 0, 5];
                    let partition_ids: Vec<i32> = (0..6).collect();
                    let racks: Vec<Option<&str>> = (0..6).map(|i| nodes[i % nodes.len()].rack()).collect();
                    partitioner.update_partition_load_stats(Some(&mut queue_sizes), &partition_ids, &racks, 6);
                }
                // Warm up: the first peek creates the sticky partition info.
                partitioner.peek_current_partition_info(&test_cluster);

                let _guard = crate::AllocTrackingGuard::new();
                crate::AllocTrackingGuard::reset();
                for _ in 0..20 {
                    // Sticky batch size 1: every update switches partition.
                    partitioner.update_partition_info(1, &test_cluster);
                    partitioner.peek_current_partition_info(&test_cluster);
                }
                assert_eq!(
                    0,
                    crate::AllocTrackingGuard::count(),
                    "partition switches allocated (adaptive={adaptive}, rack_aware={rack_aware}, rack={rack})"
                );
            }
        }
    }

    /// Translated from `BuiltInPartitionerTest.testStickyBatchSizeMoreThatZero`.
    ///
    /// Java's `IllegalArgumentException` is a panic here: a non-positive sticky
    /// batch size is a programming error (the accumulator passes `batch.size`,
    /// which `KafkaProducer` clamps to at least 1).
    #[test]
    fn test_sticky_batch_size_more_than_zero() {
        let result = std::panic::catch_unwind(|| built_in_partitioner(TOPIC_A, 0, false, ""));
        let panic = result.err().expect("Expected panic for sticky_batch_size=0");
        assert_eq!(
            Some(&"sticky_batch_size must be >= 1 but got 0".to_string()),
            panic.downcast_ref::<String>()
        );

        // Should not panic
        let _ = built_in_partitioner(TOPIC_A, 1, false, "");
    }

    // ---- KeyHasher / partition_for_key (Phase 1: CRC-32 default partitioner) ----

    /// The default [`KeyHasher`] is CRC-32 (librdkafka `consistent_random`
    /// parity), NOT murmur2 (the Java default). This is the deliberate deviation
    /// this phase introduces.
    #[test]
    fn test_key_hasher_default_is_crc32() {
        assert_eq!(KeyHasher::default(), KeyHasher::Crc32);
    }

    /// IEEE 802.3 / zlib CRC-32 golden vectors.
    ///
    /// Generated with Python `zlib.crc32(key)` (identical polynomial to
    /// `crc32fast::hash` — CRC-32/ISO-HDLC, poly 0x04C11DB7 reflected). The
    /// `b"123456789" == 0xCBF43926` entry is the canonical CRC-32 check value.
    /// These prove `crc32fast::hash` matches zlib / librdkafka `rd_crc32`.
    #[test]
    fn test_crc32_golden_vectors() {
        assert_eq!(crc32fast::hash(b""), 0x0000_0000);
        assert_eq!(crc32fast::hash(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32fast::hash(b"abc"), 0x3524_41C2);
        assert_eq!(crc32fast::hash(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32fast::hash(b"The quick brown fox jumps over the lazy dog"), 0x414F_A339);
    }

    /// `partition_for_key` under [`KeyHasher::Crc32`]: `crc32(key) % n` with an
    /// **unsigned** modulo (no `to_positive` masking).
    ///
    /// Expected partitions generated with Python `zlib.crc32(key) % n`.
    /// Includes `b"a"` (crc 0xE8B7BE43) and `b"123456789"` (crc 0xCBF43926),
    /// both of which have the CRC **high bit set** — see
    /// `test_crc32_partition_unsigned_vs_masked` for why that matters.
    #[test]
    fn test_crc32_key_to_partition_table() {
        // (key, n, expected partition) with n in {1, 3, 7, 12, 64}.
        let cases: &[(&[u8], i32, i32)] = &[
            (b"a", 1, 0),
            (b"a", 3, 0),
            (b"a", 7, 4),
            (b"a", 12, 3),
            (b"a", 64, 3),
            (b"abc", 1, 0),
            (b"abc", 3, 0),
            (b"abc", 7, 5),
            (b"abc", 12, 6),
            (b"abc", 64, 2),
            (b"kafka", 1, 0),
            (b"kafka", 3, 2),
            (b"kafka", 7, 6),
            (b"kafka", 12, 11),
            (b"kafka", 64, 23),
            (b"hello", 1, 0),
            (b"hello", 3, 1),
            (b"hello", 7, 2),
            (b"hello", 12, 10),
            (b"hello", 64, 6),
            (b"123456789", 1, 0),
            (b"123456789", 3, 2),
            (b"123456789", 7, 5),
            (b"123456789", 12, 2),
            (b"123456789", 64, 38),
        ];
        for (key, n, expected) in cases {
            assert_eq!(
                BuiltInPartitioner::partition_for_key(key, *n, KeyHasher::Crc32),
                *expected,
                "crc32 key={:?} n={}",
                String::from_utf8_lossy(key),
                n
            );
        }
    }

    /// A key whose CRC-32 has the high bit set must use the **unsigned** modulo,
    /// which differs from a `to_positive`-masked (`& 0x7fffffff`) result.
    ///
    /// `b"a"` -> crc 0xE8B7BE43 (high bit set). Unsigned `% 3 == 0`, but a
    /// masked `to_positive(0xE8B7BE43) % 3 == 1`. Asserting both proves the
    /// implementation does NOT mask (librdkafka parity).
    #[test]
    fn test_crc32_partition_unsigned_vs_masked() {
        let crc = crc32fast::hash(b"a");
        assert_eq!(crc, 0xE8B7_BE43);
        assert_ne!(crc & 0x8000_0000, 0, "precondition: high bit set");

        // Unsigned modulo (what the implementation does).
        assert_eq!(BuiltInPartitioner::partition_for_key(b"a", 3, KeyHasher::Crc32), 0);
        assert_eq!((crc % 3) as i32, 0);

        // A masked modulo would give a DIFFERENT partition — this is the bug we
        // avoid by not calling `Utils::to_positive`.
        assert_eq!(Utils::to_positive(crc as i32) % 3, 1);
    }

    /// `partition_for_key` under [`KeyHasher::Murmur2`] is byte-for-byte the old
    /// (Java-parity) behaviour: `Utils::to_positive(Utils::murmur2(key)) % n`.
    #[test]
    fn test_murmur2_key_to_partition_matches_java_formula() {
        let keys: &[&[u8]] = &[b"a", b"abc", b"kafka", b"hello", b"123456789", b""];
        for n in [1_i32, 3, 7, 12, 64] {
            for key in keys {
                assert_eq!(
                    BuiltInPartitioner::partition_for_key(key, n, KeyHasher::Murmur2),
                    Utils::to_positive(Utils::murmur2(key)) % n,
                    "murmur2 key={:?} n={}",
                    String::from_utf8_lossy(key),
                    n
                );
            }
        }
    }

    /// The empty-key rule ([`KeyHasher::hashes_key`]): under CRC-32 an empty key
    /// is NOT hashed (falls to sticky, librdkafka `consistent_random`); under
    /// murmur2 every key including empty IS hashed (Java parity). A non-empty
    /// key is hashed under both.
    #[test]
    fn test_key_hasher_hashes_key_empty_rule() {
        assert!(!KeyHasher::Crc32.hashes_key(b""));
        assert!(KeyHasher::Crc32.hashes_key(b"k"));
        assert!(KeyHasher::Murmur2.hashes_key(b""));
        assert!(KeyHasher::Murmur2.hashes_key(b"k"));
    }
}
