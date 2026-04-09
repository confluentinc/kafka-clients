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

//! Metadata management for the Kafka client.
//!
//! Corresponds to `org.apache.kafka.clients.Metadata`.
//!
//! This class is shared by the client thread (for partitioning) and the background
//! sender thread. Metadata is maintained for only a subset of topics, which can be
//! added to over time. When we request metadata for a topic we don't have any metadata
//! for, it will trigger a metadata update.
//!
//! Thread safety is ensured via a `Mutex` (corresponding to Java's `synchronized`).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::Mutex;

use log::{debug, error, info, trace};

use crate::common::cluster::Cluster;
use crate::common::cluster_resource::ClusterResource;
use crate::common::internals::ClusterResourceListeners;
use crate::common::node::Node;
use crate::common::protocol::Errors;
use crate::common::requests::NO_PARTITION_LEADER_EPOCH;
use crate::common::requests::metadata_request::MetadataRequestBuilder;
use crate::common::requests::metadata_response::{MetadataResponse, PartitionMetadata};
use crate::common::topic_partition::TopicPartition;
use crate::common::utils::ExponentialBackoff;
use crate::common::uuid::Uuid;

use super::common_client_configs;
use super::metadata_snapshot::MetadataSnapshot;

/// A class encapsulating some of the logic around metadata.
///
/// This class is shared by the client thread (for partitioning) and the background
/// sender thread. Thread safety is ensured via a `Mutex`.
///
/// Corresponds to `org.apache.kafka.clients.Metadata`.
pub struct Metadata {
    // The metadata snapshot is stored inside MetadataInner, protected by the mutex.
    // Java uses `volatile` for this field; in Rust we protect all mutable state
    // behind the mutex for correctness.
    inner: Mutex<MetadataInner>,
}

/// Inner mutable state of `Metadata`, protected by a mutex.
struct MetadataInner {
    refresh_backoff: ExponentialBackoff,
    metadata_expire_ms: i64,
    update_version: i32,
    request_version: i32,
    last_refresh_ms: i64,
    last_successful_refresh_ms: i64,
    attempts: i64,
    fatal_exception: Option<MetadataError>,
    invalid_topics: HashSet<String>,
    unauthorized_topics: HashSet<String>,
    metadata_snapshot: MetadataSnapshot,
    need_full_update: bool,
    need_partial_update: bool,
    equivalent_response_count: i64,
    cluster_resource_listeners: ClusterResourceListeners,
    is_closed: bool,
    last_seen_leader_epochs: HashMap<TopicPartition, i32>,
    bootstrap_addresses: Vec<SocketAddr>,
}

/// Errors from metadata operations.
///
/// Maps to Java's `KafkaException` subtypes used in metadata context:
/// `TopicAuthorizationException`, `InvalidTopicException`, and generic `KafkaException`.
#[derive(Clone, Debug)]
pub enum MetadataError {
    /// Topics that the client is not authorized to access.
    TopicAuthorization(HashSet<String>),
    /// Topics with invalid names.
    InvalidTopic(HashSet<String>),
    /// A fatal error that prevents further metadata updates.
    Fatal(String),
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataError::TopicAuthorization(topics) => {
                write!(f, "TopicAuthorizationException: {:?}", topics)
            },
            MetadataError::InvalidTopic(topics) => {
                write!(f, "InvalidTopicException: {:?}", topics)
            },
            MetadataError::Fatal(msg) => write!(f, "KafkaException: {}", msg),
        }
    }
}

impl std::error::Error for MetadataError {}

/// Result of `new_metadata_request_and_version`.
pub struct MetadataRequestAndVersion {
    /// The request builder.
    pub request_builder: MetadataRequestBuilder,
    /// The request version at the time of creation.
    pub request_version: i32,
    /// Whether this is a partial update.
    pub is_partial_update: bool,
}

/// Represents current leader state known in metadata.
///
/// It is possible that we know the leader, but not the epoch if the metadata is
/// received from a broker which does not support a sufficient Metadata API version.
/// It is also possible that we know of the leader epoch, but not the leader when it
/// is derived from an external source (e.g. a committed offset).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaderAndEpoch {
    /// The leader node, if known.
    pub leader: Option<Node>,
    /// The leader epoch, if known.
    pub epoch: Option<i32>,
}

impl LeaderAndEpoch {
    /// Creates a new `LeaderAndEpoch`.
    pub fn new(leader: Option<Node>, epoch: Option<i32>) -> Self {
        Self { leader, epoch }
    }

    /// Returns a `LeaderAndEpoch` with no leader and no epoch.
    pub fn no_leader_or_epoch() -> Self {
        Self { leader: None, epoch: None }
    }
}

impl fmt::Display for LeaderAndEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LeaderAndEpoch{{leader={:?}, epoch={}}}",
            self.leader,
            self.epoch.map(|e| e.to_string()).unwrap_or_else(|| "absent".to_string()),
        )
    }
}

/// Represents a leader ID and epoch, both optional.
///
/// Used by `update_partition_leadership`.
#[derive(Clone, Debug)]
pub struct LeaderIdAndEpoch {
    /// The leader node ID, if known.
    pub leader_id: Option<i32>,
    /// The leader epoch, if known.
    pub epoch: Option<i32>,
}

impl LeaderIdAndEpoch {
    /// Creates a new `LeaderIdAndEpoch`.
    pub fn new(leader_id: Option<i32>, epoch: Option<i32>) -> Self {
        Self { leader_id, epoch }
    }
}

impl fmt::Display for LeaderIdAndEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LeaderIdAndEpoch{{leaderId={}, epoch={}}}",
            self.leader_id.map(|id| id.to_string()).unwrap_or_else(|| "absent".to_string()),
            self.epoch.map(|e| e.to_string()).unwrap_or_else(|| "absent".to_string()),
        )
    }
}

impl Metadata {
    /// Creates a new `Metadata` instance.
    ///
    /// # Arguments
    /// * `refresh_backoff_ms` - The minimum amount of time between metadata refreshes
    ///   to avoid busy polling
    /// * `refresh_backoff_max_ms` - The maximum amount of time to wait between metadata
    ///   refreshes
    /// * `metadata_expire_ms` - The maximum amount of time that metadata can be retained
    ///   without refresh
    /// * `cluster_resource_listeners` - Listeners notified of cluster resource updates
    pub fn new(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self {
        let refresh_backoff = ExponentialBackoff::new(
            refresh_backoff_ms,
            common_client_configs::RETRY_BACKOFF_EXP_BASE,
            refresh_backoff_max_ms,
            common_client_configs::RETRY_BACKOFF_JITTER,
        )
        .expect("Invalid backoff parameters");

        Self {
            inner: Mutex::new(MetadataInner {
                refresh_backoff,
                metadata_expire_ms,
                last_refresh_ms: 0,
                last_successful_refresh_ms: 0,
                attempts: 0,
                request_version: 0,
                update_version: 0,
                need_full_update: false,
                need_partial_update: false,
                equivalent_response_count: 0,
                cluster_resource_listeners,
                is_closed: false,
                last_seen_leader_epochs: HashMap::new(),
                invalid_topics: HashSet::new(),
                unauthorized_topics: HashSet::new(),
                metadata_snapshot: MetadataSnapshot::empty(),
                fatal_exception: None,
                bootstrap_addresses: Vec::new(),
            }),
        }
    }

    /// Gets the current cluster info without blocking.
    pub fn fetch(&self) -> Cluster {
        let inner = self.inner.lock().unwrap();
        inner.metadata_snapshot.cluster().clone()
    }

    /// Gets the current metadata snapshot.
    pub fn fetch_metadata_snapshot(&self) -> MetadataSnapshot {
        let inner = self.inner.lock().unwrap();
        inner.metadata_snapshot.clone()
    }

    /// Returns the time until the cluster info can be updated (i.e., backoff time has elapsed).
    ///
    /// There are two calculations for backing off based on how many attempts to retrieve
    /// metadata have been made since the last successful response, and how many equivalent
    /// metadata responses have been received.
    pub fn time_to_allow_update(&self, now_ms: i64) -> i64 {
        let mut inner = self.inner.lock().unwrap();
        Self::time_to_allow_update_inner(&mut inner, now_ms)
    }

    fn time_to_allow_update_inner(inner: &mut MetadataInner, now_ms: i64) -> i64 {
        // Calculate the backoff for attempts which acts when metadata responses fail
        let backoff_for_attempts = 0i64.max(
            inner.last_refresh_ms
                + inner
                    .refresh_backoff
                    .backoff(if inner.attempts > 0 { inner.attempts - 1 } else { 0 })
                - now_ms,
        );

        // Periodic updates based on expiration resets the equivalent response count
        if 0i64.max(inner.last_successful_refresh_ms + inner.metadata_expire_ms - now_ms) == 0 {
            inner.equivalent_response_count = 0;
        }

        // Calculate the backoff for equivalent responses
        let equiv_backoff = if inner.equivalent_response_count > 0 {
            inner.refresh_backoff.backoff(inner.equivalent_response_count - 1)
        } else {
            0
        };
        let backoff_for_equivalent = 0i64.max(inner.last_refresh_ms + equiv_backoff - now_ms);

        backoff_for_attempts.max(backoff_for_equivalent)
    }

    /// The next time to update the cluster info.
    ///
    /// This is the maximum of the time the current info will expire and the time the
    /// current info can be updated (i.e. backoff time has elapsed). If an update has
    /// been requested, the metadata expiry time is now.
    pub fn time_to_next_update(&self, now_ms: i64) -> i64 {
        let mut inner = self.inner.lock().unwrap();
        Self::time_to_next_update_inner(&mut inner, now_ms)
    }

    fn time_to_next_update_inner(inner: &mut MetadataInner, now_ms: i64) -> i64 {
        let update_requested = inner.need_full_update || inner.need_partial_update;
        let time_to_expire = if update_requested {
            0
        } else {
            0i64.max(inner.last_successful_refresh_ms + inner.metadata_expire_ms - now_ms)
        };
        time_to_expire.max(Self::time_to_allow_update_inner(inner, now_ms))
    }

    /// Returns the metadata expiry time in milliseconds.
    pub fn metadata_expire_ms(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.metadata_expire_ms
    }

    /// Request an update of the current cluster metadata info.
    ///
    /// # Arguments
    /// * `reset_equivalent_response_backoff` - Whether to reset backing off based on
    ///   consecutive equivalent responses. Should be `false` for retries (e.g. leader
    ///   changed) and `true` when new metadata is being requested (e.g. adding a topic).
    ///
    /// Returns the current `update_version` before the update.
    pub fn request_update(&self, reset_equivalent_response_backoff: bool) -> i32 {
        let mut inner = self.inner.lock().unwrap();
        inner.need_full_update = true;
        if reset_equivalent_response_backoff {
            inner.equivalent_response_count = 0;
        }
        inner.update_version
    }

    /// Request an immediate update for newly requested topics.
    ///
    /// Returns the current `update_version` before the update.
    pub fn request_update_for_new_topics(&self) -> i32 {
        let mut inner = self.inner.lock().unwrap();
        inner.last_refresh_ms = 0;
        inner.need_partial_update = true;
        inner.equivalent_response_count = 0;
        inner.request_version += 1;
        inner.update_version
    }

    /// Updates the last seen epoch if the provided epoch is newer.
    ///
    /// Returns `true` if we updated the last seen epoch.
    ///
    /// # Errors
    /// Returns an error if `leader_epoch` is negative.
    pub fn update_last_seen_epoch_if_newer(
        &self,
        topic_partition: &TopicPartition,
        leader_epoch: i32,
    ) -> Result<bool, MetadataError> {
        if leader_epoch < 0 {
            return Err(MetadataError::Fatal(format!(
                "Invalid leader epoch {} (must be non-negative)",
                leader_epoch
            )));
        }

        let mut inner = self.inner.lock().unwrap();
        let old_epoch = inner.last_seen_leader_epochs.get(topic_partition).copied();

        trace!(
            "Determining if we should replace existing epoch {:?} with new epoch {} for partition {}",
            old_epoch, leader_epoch, topic_partition
        );

        let updated = match old_epoch {
            None => {
                debug!(
                    "Not replacing null epoch with new epoch {} for partition {}",
                    leader_epoch, topic_partition
                );
                false
            },
            Some(old) if leader_epoch > old => {
                debug!(
                    "Updating last seen epoch from {} to {} for partition {}",
                    old, leader_epoch, topic_partition
                );
                inner.last_seen_leader_epochs.insert(topic_partition.clone(), leader_epoch);
                true
            },
            Some(old) => {
                debug!(
                    "Not replacing existing epoch {} with new epoch {} for partition {}",
                    old, leader_epoch, topic_partition
                );
                false
            },
        };

        inner.need_full_update = inner.need_full_update || updated;
        Ok(updated)
    }

    /// Returns the last seen leader epoch for the given partition.
    pub fn last_seen_leader_epoch(&self, topic_partition: &TopicPartition) -> Option<i32> {
        let inner = self.inner.lock().unwrap();
        inner.last_seen_leader_epochs.get(topic_partition).copied()
    }

    /// Checks whether an update has been explicitly requested.
    pub fn update_requested(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.need_full_update || inner.need_partial_update
    }

    /// Adds a cluster update listener.
    pub fn add_cluster_update_listener(&self, listener: Box<dyn crate::common::internals::ClusterResourceListener>) {
        let mut inner = self.inner.lock().unwrap();
        inner.cluster_resource_listeners.maybe_add(listener);
    }

    /// Returns the cached partition info if it exists and a newer leader epoch isn't known about.
    pub fn partition_metadata_if_current(&self, topic_partition: &TopicPartition) -> Option<PartitionMetadata> {
        let inner = self.inner.lock().unwrap();
        Self::partition_metadata_if_current_inner(&inner, topic_partition)
    }

    fn partition_metadata_if_current_inner(
        inner: &MetadataInner,
        topic_partition: &TopicPartition,
    ) -> Option<PartitionMetadata> {
        let epoch = inner.last_seen_leader_epochs.get(topic_partition).copied();
        let partition_metadata = inner.metadata_snapshot.partition_metadata(topic_partition);

        match epoch {
            None => {
                // old cluster format (no epochs)
                partition_metadata.cloned()
            },
            Some(epoch) => partition_metadata
                .filter(|metadata| metadata.leader_epoch.unwrap_or(NO_PARTITION_LEADER_EPOCH) == epoch)
                .cloned(),
        }
    }

    /// Returns the topic IDs mapping (topic name -> topic ID).
    pub fn topic_ids(&self) -> HashMap<String, Uuid> {
        let inner = self.inner.lock().unwrap();
        inner.metadata_snapshot.topic_ids().clone()
    }

    /// Returns the topic names mapping (topic ID -> topic name).
    pub fn topic_names(&self) -> HashMap<Uuid, String> {
        let inner = self.inner.lock().unwrap();
        inner.metadata_snapshot.topic_names().clone()
    }

    /// Returns the current leader and epoch for the given partition.
    pub fn current_leader(&self, topic_partition: &TopicPartition) -> LeaderAndEpoch {
        let inner = self.inner.lock().unwrap();
        let maybe_metadata = Self::partition_metadata_if_current_inner(&inner, topic_partition);

        match maybe_metadata {
            None => LeaderAndEpoch::new(None, inner.last_seen_leader_epochs.get(topic_partition).copied()),
            Some(partition_metadata) => {
                let leader_epoch_opt = partition_metadata.leader_epoch;
                let leader_node_opt = partition_metadata
                    .leader_id
                    .and_then(|id| inner.metadata_snapshot.node_by_id(id).cloned());
                LeaderAndEpoch::new(leader_node_opt, leader_epoch_opt)
            },
        }
    }

    /// Bootstraps the metadata with the given addresses.
    pub fn bootstrap(&self, addresses: Vec<SocketAddr>) {
        let mut inner = self.inner.lock().unwrap();
        inner.need_full_update = true;
        inner.update_version += 1;
        inner.metadata_snapshot = MetadataSnapshot::bootstrap(&addresses);
        inner.bootstrap_addresses = addresses;
    }

    /// Rebootstraps the metadata with the original bootstrap addresses.
    pub fn rebootstrap(&self) {
        let mut inner = self.inner.lock().unwrap();
        let addresses = inner.bootstrap_addresses.clone();
        info!("Rebootstrapping with {:?}", addresses);
        inner.need_full_update = true;
        inner.update_version += 1;
        inner.metadata_snapshot = MetadataSnapshot::bootstrap(&addresses);
    }

    /// Updates metadata assuming the current request version.
    ///
    /// For testing only.
    pub fn update_with_current_request_version(
        &self,
        response: &MetadataResponse,
        is_partial_update: bool,
        now_ms: i64,
    ) {
        let request_version = {
            let inner = self.inner.lock().unwrap();
            inner.request_version
        };
        self.update(request_version, response, is_partial_update, now_ms);
    }

    /// Updates the cluster metadata.
    ///
    /// If topic expiry is enabled, expiry time is set for topics if required and
    /// expired topics are removed from the metadata.
    ///
    /// # Panics
    /// Panics if metadata is closed or response is `None` (since response is a reference,
    /// this can't actually happen).
    pub fn update(&self, request_version: i32, response: &MetadataResponse, is_partial_update: bool, now_ms: i64) {
        let mut inner = self.inner.lock().unwrap();
        assert!(!inner.is_closed, "Update requested after metadata close");

        inner.need_partial_update = request_version < inner.request_version;
        inner.last_refresh_ms = now_ms;
        inner.attempts = 0;
        inner.update_version += 1;
        if !is_partial_update {
            inner.need_full_update = false;
            inner.last_successful_refresh_ms = now_ms;
        }
        // If we subsequently find that the metadata response is not equivalent to the
        // metadata already known, this count is reset to 0 in update_latest_metadata()
        inner.equivalent_response_count += 1;

        let previous_cluster_id = inner.metadata_snapshot.cluster_resource().cluster_id().map(|s| s.to_string());

        inner.metadata_snapshot = Self::handle_metadata_response(&mut inner, response, is_partial_update, now_ms);

        let cluster = inner.metadata_snapshot.cluster().clone();
        Self::maybe_set_metadata_error(&mut inner, &cluster);

        // Remove epochs for topics we no longer retain
        inner
            .last_seen_leader_epochs
            .retain(|tp, _| Self::retain_topic_default(tp.topic(), false, now_ms));

        let new_cluster_id = inner.metadata_snapshot.cluster_resource().cluster_id().map(|s| s.to_string());
        if previous_cluster_id != new_cluster_id {
            info!("Cluster ID: {:?}", new_cluster_id);
        }
        let cluster_resource = inner.metadata_snapshot.cluster_resource();
        inner.cluster_resource_listeners.on_update(&cluster_resource);

        debug!(
            "Updated cluster metadata updateVersion {} to {}",
            inner.update_version, inner.metadata_snapshot
        );
    }

    /// Updates the partition-leadership info in the metadata.
    ///
    /// Both `partition_leaders` and `leader_nodes` override the existing metadata.
    /// Non-overlapping metadata is kept as-is.
    ///
    /// Returns the set of partitions for which leaders were updated.
    pub fn update_partition_leadership(
        &self,
        partition_leaders: &HashMap<TopicPartition, LeaderIdAndEpoch>,
        leader_nodes: &[Node],
    ) -> HashSet<TopicPartition> {
        let mut inner = self.inner.lock().unwrap();

        let mut new_nodes: HashMap<i32, Node> = leader_nodes.iter().map(|n| (n.id(), n.clone())).collect();
        // Insert non-overlapping nodes from existing nodes
        for node in inner.metadata_snapshot.cluster().nodes() {
            new_nodes.entry(node.id()).or_insert_with(|| node.clone());
        }

        let mut update_partition_metadata = Vec::new();

        for (partition, new_leader) in partition_leaders {
            let current_leader = {
                let maybe_metadata = Self::partition_metadata_if_current_inner(&inner, partition);
                match maybe_metadata {
                    None => LeaderAndEpoch::new(None, inner.last_seen_leader_epochs.get(partition).copied()),
                    Some(pm) => {
                        let leader_node_opt =
                            pm.leader_id.and_then(|id| inner.metadata_snapshot.node_by_id(id).cloned());
                        LeaderAndEpoch::new(leader_node_opt, pm.leader_epoch)
                    },
                }
            };

            if new_leader.epoch.is_none() || new_leader.leader_id.is_none() {
                debug!("For {}, incoming leader information is incomplete {}", partition, new_leader);
                continue;
            }

            let new_epoch = new_leader.epoch.unwrap();
            if let Some(current_epoch) = current_leader.epoch
                && new_epoch <= current_epoch
            {
                debug!(
                    "For {}, incoming leader({}) is not-newer than the one in the existing metadata {}, so ignoring.",
                    partition, new_leader, current_leader
                );
                continue;
            }

            let new_leader_id = new_leader.leader_id.unwrap();
            if !new_nodes.contains_key(&new_leader_id) {
                debug!(
                    "For {}, incoming leader({}), the corresponding node information for node-id {} is missing, so ignoring.",
                    partition, new_leader, new_leader_id
                );
                continue;
            }

            let existing_metadata = inner.metadata_snapshot.partition_metadata(partition);
            if existing_metadata.is_none() {
                debug!(
                    "For {}, incoming leader({}), partition metadata is no longer cached, ignoring.",
                    partition, new_leader
                );
                continue;
            }

            let existing = existing_metadata.unwrap();
            let updated_metadata = PartitionMetadata {
                error: existing.error,
                topic_partition: partition.clone(),
                leader_id: new_leader.leader_id,
                leader_epoch: new_leader.epoch,
                replica_ids: existing.replica_ids.clone(),
                in_sync_replica_ids: existing.in_sync_replica_ids.clone(),
                offline_replica_ids: existing.offline_replica_ids.clone(),
            };
            update_partition_metadata.push(updated_metadata);

            inner.last_seen_leader_epochs.insert(partition.clone(), new_epoch);
        }

        if update_partition_metadata.is_empty() {
            debug!("No relevant metadata updates.");
            return HashSet::new();
        }

        let updated_topics: HashSet<String> =
            update_partition_metadata.iter().map(|pm| pm.topic().to_string()).collect();

        // Get topic-ids for updated topics from existing topic-ids
        let existing_topic_ids = inner.metadata_snapshot.topic_ids();
        let topic_ids_for_updated: HashMap<String, Uuid> = updated_topics
            .iter()
            .filter_map(|topic| existing_topic_ids.get(topic).map(|id| (topic.clone(), *id)))
            .collect();

        let updated_partitions: HashSet<TopicPartition> =
            update_partition_metadata.iter().map(|pm| pm.topic_partition.clone()).collect();

        let cluster_id = inner.metadata_snapshot.cluster_resource().cluster_id().map(|s| s.to_string());
        let controller = inner.metadata_snapshot.cluster().controller().cloned();

        inner.metadata_snapshot = inner.metadata_snapshot.merge_with(
            cluster_id,
            new_nodes,
            update_partition_metadata,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            controller,
            topic_ids_for_updated,
            |_topic, _is_internal| true,
        );

        let cluster_resource = inner.metadata_snapshot.cluster_resource();
        inner.cluster_resource_listeners.on_update(&cluster_resource);

        updated_partitions
    }

    fn maybe_set_metadata_error(inner: &mut MetadataInner, cluster: &Cluster) {
        Self::clear_recoverable_errors(inner);
        Self::check_invalid_topics(inner, cluster);
        Self::check_unauthorized_topics(inner, cluster);
    }

    fn check_invalid_topics(inner: &mut MetadataInner, cluster: &Cluster) {
        if !cluster.invalid_topics().is_empty() {
            error!("Metadata response reported invalid topics {:?}", cluster.invalid_topics());
            inner.invalid_topics = cluster.invalid_topics().clone();
        }
    }

    fn check_unauthorized_topics(inner: &mut MetadataInner, cluster: &Cluster) {
        if !cluster.unauthorized_topics().is_empty() {
            error!("Topic authorization failed for topics {:?}", cluster.unauthorized_topics());
            inner.unauthorized_topics = cluster.unauthorized_topics().clone();
        }
    }

    /// Transform a MetadataResponse into a new MetadataSnapshot.
    fn handle_metadata_response(
        inner: &mut MetadataInner,
        metadata_response: &MetadataResponse,
        is_partial_update: bool,
        now_ms: i64,
    ) -> MetadataSnapshot {
        // All encountered topics
        let mut topics = HashSet::new();

        // Retained topics to be passed to the metadata cache
        let mut internal_topics = HashSet::new();
        let mut unauthorized_topics = HashSet::new();
        let mut invalid_topics = HashSet::new();

        let mut partitions = Vec::new();
        let mut topic_ids: HashMap<String, Uuid> = HashMap::new();
        let old_topic_ids = inner.metadata_snapshot.topic_ids().clone();

        for metadata in metadata_response.topic_metadata() {
            let topic_name = metadata.topic().to_string();
            let topic_id = metadata.topic_id();
            topics.insert(topic_name.clone());

            // We can only reason about topic ID changes when both IDs are valid
            let mut old_topic_id: Option<Uuid> = None;
            let effective_topic_id;
            if topic_id != Uuid::zero() {
                topic_ids.insert(topic_name.clone(), topic_id);
                old_topic_id = old_topic_ids.get(&topic_name).copied();
                effective_topic_id = Some(topic_id);
            } else {
                effective_topic_id = None;
            }

            if !Self::retain_topic_with_id(&topic_name, effective_topic_id, metadata.is_internal(), now_ms) {
                continue;
            }

            if metadata.is_internal() {
                internal_topics.insert(topic_name.clone());
            }

            if metadata.error() == Errors::None {
                for partition_metadata in metadata.partition_metadata() {
                    // Even if the partition's metadata includes an error, we need to
                    // handle the update to catch new epochs
                    if let Some(pm) = Self::update_latest_metadata(
                        inner,
                        partition_metadata,
                        metadata_response.has_reliable_leader_epochs(),
                        effective_topic_id,
                        old_topic_id,
                    ) {
                        partitions.push(pm);
                    }

                    if Self::is_invalid_metadata_error(partition_metadata.error) {
                        debug!(
                            "Requesting metadata update for partition {} due to error {:?}",
                            partition_metadata.topic_partition, partition_metadata.error
                        );
                        inner.need_full_update = true;
                        if inner.equivalent_response_count > 0 {
                            // Don't reset, just don't increment further
                        }
                    }
                }
            } else {
                if Self::is_invalid_metadata_error(metadata.error()) {
                    debug!(
                        "Requesting metadata update for topic {} due to error {:?}",
                        topic_name,
                        metadata.error()
                    );
                    inner.need_full_update = true;
                }

                if metadata.error() == Errors::InvalidTopicException {
                    invalid_topics.insert(topic_name.clone());
                } else if metadata.error() == Errors::TopicAuthorizationFailed {
                    unauthorized_topics.insert(topic_name);
                }
            }
        }

        let nodes = metadata_response.brokers_by_id().clone();

        if is_partial_update {
            let topics_ref = topics;
            inner.metadata_snapshot.merge_with(
                metadata_response.cluster_id().map(|s| s.to_string()),
                nodes,
                partitions,
                unauthorized_topics,
                invalid_topics,
                internal_topics,
                metadata_response.controller().cloned(),
                topic_ids,
                |topic, is_internal| {
                    !topics_ref.contains(topic) && Self::retain_topic_default(topic, is_internal, now_ms)
                },
            )
        } else {
            MetadataSnapshot::new(
                metadata_response.cluster_id().map(|s| s.to_string()),
                nodes,
                partitions,
                unauthorized_topics,
                invalid_topics,
                internal_topics,
                metadata_response.controller().cloned(),
                topic_ids,
            )
        }
    }

    /// Compute the latest partition metadata to cache given ordering by leader epochs.
    fn update_latest_metadata(
        inner: &mut MetadataInner,
        partition_metadata: &PartitionMetadata,
        has_reliable_leader_epoch: bool,
        topic_id: Option<Uuid>,
        old_topic_id: Option<Uuid>,
    ) -> Option<PartitionMetadata> {
        let tp = &partition_metadata.topic_partition;
        if has_reliable_leader_epoch && partition_metadata.leader_epoch.is_some() {
            let new_epoch = partition_metadata.leader_epoch.unwrap();
            let current_epoch = inner.last_seen_leader_epochs.get(tp).copied();

            match current_epoch {
                None => {
                    // No previous info, insert new epoch
                    debug!(
                        "Setting the last seen epoch of partition {} to {} since the last known epoch was undefined.",
                        tp, new_epoch
                    );
                    inner.last_seen_leader_epochs.insert(tp.clone(), new_epoch);
                    inner.equivalent_response_count = 0;
                    Some(partition_metadata.clone())
                },
                Some(_) if topic_id.is_some() && topic_id != old_topic_id => {
                    // Topic ID changed (topic deleted and re-created)
                    info!(
                        "Resetting the last seen epoch of partition {} to {} since the associated topicId changed from {:?} to {:?}",
                        tp, new_epoch, old_topic_id, topic_id
                    );
                    inner.last_seen_leader_epochs.insert(tp.clone(), new_epoch);
                    inner.equivalent_response_count = 0;
                    Some(partition_metadata.clone())
                },
                Some(current) if new_epoch >= current => {
                    debug!(
                        "Updating last seen epoch for partition {} from {} to epoch {} from new metadata",
                        tp, current, new_epoch
                    );
                    inner.last_seen_leader_epochs.insert(tp.clone(), new_epoch);
                    if new_epoch > current {
                        inner.equivalent_response_count = 0;
                    }
                    Some(partition_metadata.clone())
                },
                Some(current) => {
                    // Old epoch, ignore
                    debug!(
                        "Got metadata for an older epoch {} (current is {}) for partition {}, not updating",
                        new_epoch, current, tp
                    );
                    inner.metadata_snapshot.partition_metadata(tp).cloned()
                },
            }
        } else {
            // Handle old cluster formats as well as error responses
            inner.last_seen_leader_epochs.remove(tp);
            inner.equivalent_response_count = 0;
            Some(partition_metadata.without_leader_epoch())
        }
    }

    /// Checks if the error is an invalid metadata error that should trigger a re-fetch.
    ///
    /// Returns `true` for error codes whose Java exceptions extend
    /// `InvalidMetadataException`.
    fn is_invalid_metadata_error(error: Errors) -> bool {
        matches!(
            error,
            Errors::UnknownTopicOrPartition
                | Errors::LeaderNotAvailable
                | Errors::NotLeaderOrFollower
                | Errors::ReplicaNotAvailable
                | Errors::ListenerNotFound
                | Errors::FencedLeaderEpoch
                | Errors::UnknownTopicId
                | Errors::NetworkException
                | Errors::KafkaStorageError
                | Errors::InconsistentTopicId
                | Errors::PreferredLeaderNotAvailable
                | Errors::EligibleLeadersNotAvailable
                | Errors::ElectionNotNeeded
        )
    }

    /// If any non-retriable exceptions were encountered during metadata update,
    /// clear and return the exception.
    pub fn maybe_throw_any_exception(&self) -> Result<(), MetadataError> {
        let mut inner = self.inner.lock().unwrap();
        Self::clear_errors_and_maybe_throw_exception(&mut inner, Self::recoverable_exception)
    }

    /// If any non-retriable exceptions were encountered for the specified topic,
    /// return the error. All exceptions from the last metadata update are cleared.
    pub fn maybe_throw_exception_for_topic(&self, topic: &str) -> Result<(), MetadataError> {
        let topic = topic.to_string();
        let mut inner = self.inner.lock().unwrap();
        Self::clear_errors_and_maybe_throw_exception(&mut inner, |i| Self::recoverable_exception_for_topic(i, &topic))
    }

    /// If any fatal exceptions were encountered during metadata update, return the exception.
    pub fn maybe_throw_fatal_exception(&self) -> Result<(), MetadataError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(exception) = inner.fatal_exception.take() {
            return Err(exception);
        }
        Ok(())
    }

    fn clear_errors_and_maybe_throw_exception<F>(
        inner: &mut MetadataInner,
        recoverable_supplier: F,
    ) -> Result<(), MetadataError>
    where
        F: FnOnce(&MetadataInner) -> Option<MetadataError>,
    {
        let metadata_exception = inner.fatal_exception.take().or_else(|| recoverable_supplier(inner));
        Self::clear_recoverable_errors(inner);
        match metadata_exception {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn recoverable_exception(inner: &MetadataInner) -> Option<MetadataError> {
        if !inner.unauthorized_topics.is_empty() {
            Some(MetadataError::TopicAuthorization(inner.unauthorized_topics.clone()))
        } else if !inner.invalid_topics.is_empty() {
            Some(MetadataError::InvalidTopic(inner.invalid_topics.clone()))
        } else {
            None
        }
    }

    fn recoverable_exception_for_topic(inner: &MetadataInner, topic: &str) -> Option<MetadataError> {
        if inner.unauthorized_topics.contains(topic) {
            Some(MetadataError::TopicAuthorization([topic.to_string()].into_iter().collect()))
        } else if inner.invalid_topics.contains(topic) {
            Some(MetadataError::InvalidTopic([topic.to_string()].into_iter().collect()))
        } else {
            None
        }
    }

    fn clear_recoverable_errors(inner: &mut MetadataInner) {
        inner.invalid_topics = HashSet::new();
        inner.unauthorized_topics = HashSet::new();
    }

    /// Record an attempt to update the metadata that failed.
    pub fn failed_update(&self, now: i64) {
        let mut inner = self.inner.lock().unwrap();
        inner.last_refresh_ms = now;
        inner.attempts += 1;
        inner.equivalent_response_count = 0;
    }

    /// Propagate a fatal error which affects the ability to fetch metadata.
    pub fn fatal_error(&self, exception: MetadataError) {
        let mut inner = self.inner.lock().unwrap();
        inner.fatal_exception = Some(exception);
    }

    /// Returns the current metadata update version.
    pub fn update_version(&self) -> i32 {
        let inner = self.inner.lock().unwrap();
        inner.update_version
    }

    /// The last time metadata was successfully updated.
    pub fn last_successful_update(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.last_successful_refresh_ms
    }

    /// Close this metadata instance to indicate that metadata updates are no longer possible.
    pub fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.is_closed = true;
    }

    /// Check if this metadata instance has been closed.
    pub fn is_closed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.is_closed
    }

    /// Creates a new metadata request and version for sending.
    pub fn new_metadata_request_and_version(&self, now_ms: i64) -> MetadataRequestAndVersion {
        let inner = self.inner.lock().unwrap();

        let mut request = None;
        let mut is_partial_update = false;

        // Perform a partial update only if a full update hasn't been requested,
        // and the last successful hasn't exceeded the metadata refresh time.
        if !inner.need_full_update && inner.last_successful_refresh_ms + inner.metadata_expire_ms > now_ms {
            request = Self::new_metadata_request_builder_for_new_topics();
            is_partial_update = true;
        }
        if request.is_none() {
            request = Some(Self::new_metadata_request_builder());
            is_partial_update = false;
        }

        MetadataRequestAndVersion {
            request_builder: request.unwrap(),
            request_version: inner.request_version,
            is_partial_update,
        }
    }

    /// Constructs and returns a metadata request builder for fetching cluster data
    /// and all active topics.
    fn new_metadata_request_builder() -> MetadataRequestBuilder {
        MetadataRequestBuilder::all_topics()
    }

    /// Constructs and returns a metadata request builder for fetching cluster data
    /// and any uncached topics, otherwise `None` if the functionality is not supported.
    ///
    /// The base implementation returns `None`. Subclasses (consumers) override this.
    fn new_metadata_request_builder_for_new_topics() -> Option<MetadataRequestBuilder> {
        None
    }

    /// Based on the topic name, check if the topic metadata should be kept when received
    /// in a metadata response. The default implementation returns `true` for all topics.
    fn retain_topic_default(_topic: &str, _is_internal: bool, _now_ms: i64) -> bool {
        true
    }

    /// Based on the topic name and topic ID, check if the topic metadata should be kept.
    fn retain_topic_with_id(topic_name: &str, _topic_id: Option<Uuid>, is_internal: bool, now_ms: i64) -> bool {
        Self::retain_topic_default(topic_name, is_internal, now_ms)
    }

    /// Returns the cluster resource for the current metadata snapshot.
    pub fn fetch_cluster_resource(&self) -> ClusterResource {
        let inner = self.inner.lock().unwrap();
        inner.metadata_snapshot.cluster_resource()
    }
}

impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Metadata").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ApiKeys;
    use crate::common::internals::{ClusterResourceListener, ClusterResourceListeners};
    use crate::common::requests::request_test_utils;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const REFRESH_BACKOFF_MS: i64 = 100;
    const REFRESH_BACKOFF_MAX_MS: i64 = 1000;
    const METADATA_EXPIRE_MS: i64 = 1000;

    fn new_metadata() -> Metadata {
        Metadata::new(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            ClusterResourceListeners::new(),
        )
    }

    fn empty_metadata_response() -> MetadataResponse {
        request_test_utils::metadata_response(&[], None, -1, Vec::new())
    }

    /// Translated from `MetadataTest.testMetadataUpdateAfterClose`.
    #[test]
    #[should_panic(expected = "Update requested after metadata close")]
    fn test_metadata_update_after_close() {
        let metadata = new_metadata();
        metadata.close();
        metadata.update_with_current_request_version(&empty_metadata_response(), false, 1000);
    }

    fn check_time_to_next_update(refresh_backoff_ms: i64, metadata_expire_ms: i64) {
        let now: i64 = 10000;

        assert!(
            metadata_expire_ms <= now && refresh_backoff_ms <= now,
            "metadataExpireMs and refreshBackoffMs must be smaller than 'now'"
        );

        let larger_of_backoff_and_expire = refresh_backoff_ms.max(metadata_expire_ms);
        // This test intentionally disables exponential backoff (constant backoff)
        let metadata = Metadata::new(
            refresh_backoff_ms,
            refresh_backoff_ms,
            metadata_expire_ms,
            ClusterResourceListeners::new(),
        );

        assert_eq!(0, metadata.time_to_next_update(now));

        // lastSuccessfulRefreshMs updated to now
        metadata.update_with_current_request_version(&empty_metadata_response(), false, now);

        // The last update was successful so the remaining time to expire should be returned
        assert_eq!(larger_of_backoff_and_expire, metadata.time_to_next_update(now));

        // Metadata update requested explicitly
        metadata.request_update(true);
        // Update requested so metadataExpireMs should no longer take effect
        assert_eq!(refresh_backoff_ms, metadata.time_to_next_update(now));

        // Reset needUpdate to false
        metadata.update_with_current_request_version(&empty_metadata_response(), false, now);
        assert_eq!(larger_of_backoff_and_expire, metadata.time_to_next_update(now));

        // Both metadataExpireMs and refreshBackoffMs elapsed
        let now = now + larger_of_backoff_and_expire;
        assert_eq!(0, metadata.time_to_next_update(now));
        assert_eq!(0, metadata.time_to_next_update(now + 1));
    }

    /// Translated from `MetadataTest.testUpdateMetadataAllowedImmediatelyAfterBootstrap`.
    #[test]
    fn test_update_metadata_allowed_immediately_after_bootstrap() {
        let now: i64 = 10000;
        let metadata = Metadata::new(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            ClusterResourceListeners::new(),
        );
        let addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();
        metadata.bootstrap(vec![addr]);

        assert_eq!(0, metadata.time_to_allow_update(now));
        assert_eq!(0, metadata.time_to_next_update(now));
    }

    /// Translated from `MetadataTest.testTimeToNextUpdate`.
    #[test]
    fn test_time_to_next_update() {
        check_time_to_next_update(100, 1000);
        check_time_to_next_update(1000, 100);
        check_time_to_next_update(0, 0);
        check_time_to_next_update(0, 100);
        check_time_to_next_update(100, 0);
    }

    /// Translated from `MetadataTest.testTimeToNextUpdateRetryBackoff`.
    #[test]
    fn test_time_to_next_update_retry_backoff() {
        let metadata = new_metadata();
        let mut now: i64 = 10000;

        // lastRefreshMs updated to now
        metadata.failed_update(now);

        // Backing off. Remaining time until next try should be returned.
        let lower_bound_backoff_ms =
            (REFRESH_BACKOFF_MS as f64 * (1.0 - super::common_client_configs::RETRY_BACKOFF_JITTER)) as i64;
        let upper_bound_backoff_ms =
            (REFRESH_BACKOFF_MS as f64 * (1.0 + super::common_client_configs::RETRY_BACKOFF_JITTER)) as i64;
        let tolerance = upper_bound_backoff_ms - lower_bound_backoff_ms;
        let actual = metadata.time_to_next_update(now);
        assert!(
            (actual - REFRESH_BACKOFF_MS).abs() <= tolerance,
            "Expected ~{} +/- {}, got {}",
            REFRESH_BACKOFF_MS,
            tolerance,
            actual
        );

        // Even though metadata update requested explicitly, still respects backoff
        metadata.request_update(true);
        let actual = metadata.time_to_next_update(now);
        assert!(
            (actual - REFRESH_BACKOFF_MS).abs() <= tolerance,
            "Expected ~{} +/- {}, got {}",
            REFRESH_BACKOFF_MS,
            tolerance,
            actual
        );

        // refreshBackoffMs elapsed
        now += REFRESH_BACKOFF_MS + upper_bound_backoff_ms;
        assert_eq!(0, metadata.time_to_next_update(now));
        assert_eq!(0, metadata.time_to_next_update(now + 1));
    }

    /// Translated from `MetadataTest.testFailedUpdate`.
    #[test]
    fn test_failed_update() {
        let metadata = new_metadata();
        let time: i64 = 100;
        metadata.update_with_current_request_version(&empty_metadata_response(), false, time);

        assert_eq!(100, metadata.time_to_next_update(1000));
        metadata.failed_update(1100);

        let lower_bound_backoff_ms =
            (REFRESH_BACKOFF_MS as f64 * (1.0 - super::common_client_configs::RETRY_BACKOFF_JITTER)) as i64;
        let upper_bound_backoff_ms =
            (REFRESH_BACKOFF_MS as f64 * (1.0 + super::common_client_configs::RETRY_BACKOFF_JITTER)) as i64;
        let tolerance = upper_bound_backoff_ms - lower_bound_backoff_ms;

        let actual = metadata.time_to_next_update(1100);
        assert!(
            (actual - 100).abs() <= tolerance,
            "Expected ~100 +/- {}, got {}",
            tolerance,
            actual
        );
        assert_eq!(100, metadata.last_successful_update());

        metadata.update_with_current_request_version(&empty_metadata_response(), false, time);
        let actual = metadata.time_to_next_update(1000);
        assert!(
            (actual - 100).abs() <= tolerance,
            "Expected ~100 +/- {}, got {}",
            tolerance,
            actual
        );
    }

    /// Translated from `MetadataTest.testClusterListenerGetsNotifiedOfUpdate`.
    #[test]
    fn test_cluster_listener_gets_notified_of_update() {
        let on_update_called = Arc::new(AtomicBool::new(false));
        let cluster_id_holder: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        struct TestListener {
            called: Arc<AtomicBool>,
            cluster_id: Arc<Mutex<Option<String>>>,
        }

        impl ClusterResourceListener for TestListener {
            fn on_update(&self, cluster_resource: &ClusterResource) {
                self.called.store(true, Ordering::SeqCst);
                *self.cluster_id.lock().unwrap() = cluster_resource.cluster_id().map(|s| s.to_string());
            }
        }

        let mut listeners = ClusterResourceListeners::new();
        listeners.add_listener(Box::new(TestListener {
            called: on_update_called.clone(),
            cluster_id: cluster_id_holder.clone(),
        }));

        let metadata = Metadata::new(REFRESH_BACKOFF_MS, REFRESH_BACKOFF_MAX_MS, METADATA_EXPIRE_MS, listeners);

        let addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();
        metadata.bootstrap(vec![addr]);
        assert!(
            !on_update_called.load(Ordering::SeqCst),
            "ClusterResourceListener should not be called when metadata is updated with bootstrap Cluster"
        );

        let mut partition_counts = HashMap::new();
        partition_counts.insert("topic".to_string(), 1);
        partition_counts.insert("topic1".to_string(), 1);
        let metadata_response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &HashMap::new(),
            &partition_counts,
            &|_tp| None,
        );
        metadata.update_with_current_request_version(&metadata_response, false, 100);

        let stored_id = cluster_id_holder.lock().unwrap().clone();
        assert_eq!(
            Some("dummy".to_string()),
            stored_id,
            "MockClusterResourceListener did not get cluster metadata correctly"
        );
        assert!(
            on_update_called.load(Ordering::SeqCst),
            "MockClusterResourceListener should be called when metadata is updated with non-bootstrap Cluster"
        );
    }

    /// Translated from `MetadataTest.testRequestUpdate`.
    #[test]
    fn test_request_update() {
        let metadata = new_metadata();
        assert!(!metadata.update_requested());

        let epochs = [42, 42, 41, 41, 42, 43, 43, 42, 41, 44];
        let update_result = [true, false, false, false, false, true, false, false, false, true];
        let tp = TopicPartition::new("topic".to_string(), 0);

        let metadata_response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(0),
        );
        metadata.update_with_current_request_version(&metadata_response, false, 10);

        for i in 0..epochs.len() {
            let _ = metadata.update_last_seen_epoch_if_newer(&tp, epochs[i]);
            if update_result[i] {
                assert!(metadata.update_requested(), "Expected metadata update to be requested [{}]", i);
            } else {
                assert!(
                    !metadata.update_requested(),
                    "Did not expect metadata update to be requested [{}]",
                    i
                );
            }
            metadata.update_with_current_request_version(&empty_metadata_response(), false, 0);
            assert!(!metadata.update_requested());
        }
    }

    /// Translated from `MetadataTest.testUpdateLastEpoch`.
    #[test]
    fn test_update_last_epoch() {
        let metadata = new_metadata();
        let tp = TopicPartition::new("topic-1".to_string(), 0);

        metadata.update_with_current_request_version(&empty_metadata_response(), false, 0);

        // If we have no leader epoch, this call shouldn't do anything
        assert!(!metadata.update_last_seen_epoch_if_newer(&tp, 0).unwrap());
        assert!(!metadata.update_last_seen_epoch_if_newer(&tp, 1).unwrap());
        assert!(!metadata.update_last_seen_epoch_if_newer(&tp, 2).unwrap());
        assert!(metadata.last_seen_leader_epoch(&tp).is_none());

        // Metadata with newer epoch is handled
        let metadata_response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(10),
        );
        metadata.update_with_current_request_version(&metadata_response, false, 1);
        assert_eq!(Some(10), metadata.last_seen_leader_epoch(&tp));

        // Don't update to an older one
        assert!(!metadata.update_last_seen_epoch_if_newer(&tp, 1).unwrap());
        assert_eq!(Some(10), metadata.last_seen_leader_epoch(&tp));

        // Don't cause update if it's the same one
        assert!(!metadata.update_last_seen_epoch_if_newer(&tp, 10).unwrap());
        assert_eq!(Some(10), metadata.last_seen_leader_epoch(&tp));

        // Update if we see newer epoch
        assert!(metadata.update_last_seen_epoch_if_newer(&tp, 12).unwrap());
        assert_eq!(Some(12), metadata.last_seen_leader_epoch(&tp));

        let metadata_response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(12),
        );
        metadata.update_with_current_request_version(&metadata_response, false, 2);
        assert_eq!(Some(12), metadata.last_seen_leader_epoch(&tp));

        // Don't overwrite metadata with older epoch
        let metadata_response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(11),
        );
        metadata.update_with_current_request_version(&metadata_response, false, 3);
        assert_eq!(Some(12), metadata.last_seen_leader_epoch(&tp));
    }

    /// Translated from `MetadataTest.testEpochUpdateAfterTopicDeletion`.
    #[test]
    fn test_epoch_update_after_topic_deletion() {
        let metadata = new_metadata();
        let tp = TopicPartition::new("topic-1".to_string(), 0);

        metadata.update_with_current_request_version(&empty_metadata_response(), false, 0);

        // Start with a topic with a random topic ID
        let topic_ids: HashMap<String, Uuid> = [("topic-1".to_string(), Uuid::random_uuid())].into_iter().collect();
        let response = request_test_utils::metadata_update_with_ids(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(10),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&response, false, 1);
        assert_eq!(Some(10), metadata.last_seen_leader_epoch(&tp));

        // Topic deleted so Response contains an Error. LeaderEpoch should maintain old value
        let response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &[("topic-1".to_string(), Errors::UnknownTopicOrPartition)].into_iter().collect(),
            &HashMap::new(),
            &|_tp| None,
        );
        metadata.update_with_current_request_version(&response, false, 1);
        assert_eq!(Some(10), metadata.last_seen_leader_epoch(&tp));

        // Create topic-1 again with a different topic ID. LeaderEpoch should update even if lower.
        let new_topic_ids: HashMap<String, Uuid> = [("topic-1".to_string(), Uuid::random_uuid())].into_iter().collect();
        let response = request_test_utils::metadata_update_with_ids(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(5),
            &new_topic_ids,
        );
        metadata.update_with_current_request_version(&response, false, 1);
        assert_eq!(Some(5), metadata.last_seen_leader_epoch(&tp));
    }

    /// Translated from `MetadataTest.testEpochUpdateOnChangedTopicIds`.
    #[test]
    fn test_epoch_update_on_changed_topic_ids() {
        let metadata = new_metadata();
        let tp = TopicPartition::new("topic-1".to_string(), 0);
        let topic_ids: HashMap<String, Uuid> = [("topic-1".to_string(), Uuid::random_uuid())].into_iter().collect();

        metadata.update_with_current_request_version(&empty_metadata_response(), false, 0);

        // Start with a topic with no topic ID
        let response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(100),
        );
        metadata.update_with_current_request_version(&response, false, 1);
        assert_eq!(Some(100), metadata.last_seen_leader_epoch(&tp));

        // If the older topic ID is null, we should go with the new topic ID
        let response = request_test_utils::metadata_update_with_ids(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(10),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&response, false, 2);
        assert_eq!(Some(10), metadata.last_seen_leader_epoch(&tp));

        // Don't cause update if it's the same one
        let response = request_test_utils::metadata_update_with_ids(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(10),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&response, false, 3);
        assert_eq!(Some(10), metadata.last_seen_leader_epoch(&tp));

        // Update if we see newer epoch
        let response = request_test_utils::metadata_update_with_ids(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(12),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&response, false, 4);
        assert_eq!(Some(12), metadata.last_seen_leader_epoch(&tp));

        // We should also update if we see a new topicId even if the epoch is lower
        let new_topic_ids: HashMap<String, Uuid> = [("topic-1".to_string(), Uuid::random_uuid())].into_iter().collect();
        let response = request_test_utils::metadata_update_with_ids(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(3),
            &new_topic_ids,
        );
        metadata.update_with_current_request_version(&response, false, 5);
        assert_eq!(Some(3), metadata.last_seen_leader_epoch(&tp));

        // Update when the topic ID is new and the epoch is higher
        let new_topic_ids2: HashMap<String, Uuid> =
            [("topic-1".to_string(), Uuid::random_uuid())].into_iter().collect();
        let response = request_test_utils::metadata_update_with_ids(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| Some(20),
            &new_topic_ids2,
        );
        metadata.update_with_current_request_version(&response, false, 6);
        assert_eq!(Some(20), metadata.last_seen_leader_epoch(&tp));
    }

    /// Translated from `MetadataTest.testRejectOldMetadata`.
    #[test]
    fn test_reject_old_metadata() {
        let metadata = new_metadata();
        let partition_counts: HashMap<String, i32> = [("topic-1".to_string(), 1)].into_iter().collect();
        let tp = TopicPartition::new("topic-1".to_string(), 0);

        metadata.update_with_current_request_version(&empty_metadata_response(), false, 0);

        // First epoch seen, accept it
        {
            let response = request_test_utils::metadata_update_with_cluster_id(
                "dummy",
                1,
                &HashMap::new(),
                &partition_counts,
                &|_tp| Some(100),
            );
            metadata.update_with_current_request_version(&response, false, 10);
            assert!(metadata.fetch().partition(&tp).is_some());
            assert!(metadata.last_seen_leader_epoch(&tp).is_some());
            assert_eq!(100, metadata.last_seen_leader_epoch(&tp).unwrap());
        }

        // Fake an empty ISR, but with an older epoch, should reject it
        {
            let response = request_test_utils::metadata_update_with_full(
                "dummy",
                1,
                &HashMap::new(),
                &partition_counts,
                &|_tp| Some(99),
                &|error, partition, leader, leader_epoch, replicas, _isr, offline_replicas| PartitionMetadata {
                    error,
                    topic_partition: partition.clone(),
                    leader_id: leader,
                    leader_epoch,
                    replica_ids: replicas,
                    in_sync_replica_ids: Vec::new(),
                    offline_replica_ids: offline_replicas,
                },
                ApiKeys::METADATA.latest_version(),
                &HashMap::new(),
            );
            metadata.update_with_current_request_version(&response, false, 20);
            assert_eq!(1, metadata.fetch().partition(&tp).unwrap().in_sync_replicas().len());
            assert_eq!(100, metadata.last_seen_leader_epoch(&tp).unwrap());
        }

        // Fake an empty ISR, with same epoch, accept it
        {
            let response = request_test_utils::metadata_update_with_full(
                "dummy",
                1,
                &HashMap::new(),
                &partition_counts,
                &|_tp| Some(100),
                &|error, partition, leader, leader_epoch, replicas, _isr, offline_replicas| PartitionMetadata {
                    error,
                    topic_partition: partition.clone(),
                    leader_id: leader,
                    leader_epoch,
                    replica_ids: replicas,
                    in_sync_replica_ids: Vec::new(),
                    offline_replica_ids: offline_replicas,
                },
                ApiKeys::METADATA.latest_version(),
                &HashMap::new(),
            );
            metadata.update_with_current_request_version(&response, false, 20);
            assert_eq!(0, metadata.fetch().partition(&tp).unwrap().in_sync_replicas().len());
            assert_eq!(100, metadata.last_seen_leader_epoch(&tp).unwrap());
        }

        // Empty metadata response, should not keep old partition but should keep the last-seen epoch
        {
            let response = request_test_utils::metadata_update_with_cluster_id(
                "dummy",
                1,
                &HashMap::new(),
                &HashMap::new(),
                &|_tp| None,
            );
            metadata.update_with_current_request_version(&response, false, 20);
            assert!(metadata.fetch().partition(&tp).is_none());
            assert_eq!(100, metadata.last_seen_leader_epoch(&tp).unwrap());
        }

        // Back in the metadata, with old epoch, should not get added
        {
            let response = request_test_utils::metadata_update_with_cluster_id(
                "dummy",
                1,
                &HashMap::new(),
                &partition_counts,
                &|_tp| Some(99),
            );
            metadata.update_with_current_request_version(&response, false, 10);
            assert!(metadata.fetch().partition(&tp).is_none());
            assert_eq!(100, metadata.last_seen_leader_epoch(&tp).unwrap());
        }
    }

    /// Translated from `MetadataTest.testNoEpoch`.
    #[test]
    fn test_no_epoch() {
        let metadata = new_metadata();
        metadata.update_with_current_request_version(&empty_metadata_response(), false, 0);

        let response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &HashMap::new(),
            &[("topic-1".to_string(), 1)].into_iter().collect(),
            &|_tp| None,
        );
        metadata.update_with_current_request_version(&response, false, 10);

        let tp = TopicPartition::new("topic-1".to_string(), 0);

        // no epoch
        assert!(metadata.last_seen_leader_epoch(&tp).is_none());

        // still works
        let pm = metadata.partition_metadata_if_current(&tp);
        assert!(pm.is_some());
        assert_eq!(0, pm.as_ref().unwrap().partition());
        assert_eq!(Some(0), pm.as_ref().unwrap().leader_id);

        // Since epoch was null, this shouldn't update it
        let _ = metadata.update_last_seen_epoch_if_newer(&tp, 10);
        let pm = metadata.partition_metadata_if_current(&tp);
        assert!(pm.is_some());
        assert!(pm.unwrap().leader_epoch.is_none());
    }

    /// Translated from `MetadataTest.testClusterCopy`.
    #[test]
    fn test_cluster_copy() {
        let metadata = new_metadata();
        let mut counts = HashMap::new();
        let mut errors = HashMap::new();
        counts.insert("topic1".to_string(), 2);
        counts.insert("topic2".to_string(), 3);
        counts.insert(crate::common::internals::topic::GROUP_METADATA_TOPIC_NAME.to_string(), 3);
        errors.insert("topic3".to_string(), Errors::InvalidTopicException);
        errors.insert("topic4".to_string(), Errors::TopicAuthorizationFailed);

        let metadata_response =
            request_test_utils::metadata_update_with_cluster_id("dummy", 4, &errors, &counts, &|_tp| None);
        metadata.update_with_current_request_version(&metadata_response, false, 0);

        let cluster = metadata.fetch();
        assert_eq!(Some("dummy"), cluster.cluster_resource().cluster_id());
        assert_eq!(4, cluster.nodes().len());

        // topic counts
        assert_eq!(
            &["topic3".to_string()].into_iter().collect::<HashSet<_>>(),
            cluster.invalid_topics()
        );
        assert_eq!(
            &["topic4".to_string()].into_iter().collect::<HashSet<_>>(),
            cluster.unauthorized_topics()
        );
        assert_eq!(3, cluster.topics().count());
        assert_eq!(
            &[crate::common::internals::topic::GROUP_METADATA_TOPIC_NAME.to_string()]
                .into_iter()
                .collect::<HashSet<_>>(),
            cluster.internal_topics()
        );

        // partition counts
        assert_eq!(2, cluster.partitions_for_topic("topic1").len());
        assert_eq!(3, cluster.partitions_for_topic("topic2").len());

        // Sentinel instances
        let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let from_metadata = MetadataSnapshot::bootstrap(&[address]).cluster().clone();
        let from_cluster = Cluster::bootstrap(&[address]);
        assert_eq!(from_metadata, from_cluster);

        let from_metadata_empty = MetadataSnapshot::empty().cluster().clone();
        let from_cluster_empty = Cluster::empty();
        assert_eq!(from_metadata_empty, from_cluster_empty);
    }

    /// Translated from `MetadataTest.testInvalidTopicError`.
    #[test]
    fn test_invalid_topic_error() {
        let metadata = new_metadata();
        let now: i64 = 10000;

        let invalid_topic = "topic dfsa";
        let invalid_topic_response = request_test_utils::metadata_update_with_cluster_id(
            "clusterId",
            1,
            &[(invalid_topic.to_string(), Errors::InvalidTopicException)]
                .into_iter()
                .collect(),
            &HashMap::new(),
            &|_tp| None,
        );
        metadata.update_with_current_request_version(&invalid_topic_response, false, now);

        let err = metadata.maybe_throw_any_exception().unwrap_err();
        match &err {
            MetadataError::InvalidTopic(topics) => {
                assert_eq!(&[invalid_topic.to_string()].into_iter().collect::<HashSet<_>>(), topics);
            },
            _ => panic!("Expected InvalidTopic error, got {:?}", err),
        }
        // We clear the exception once it has been raised to the user
        assert!(metadata.maybe_throw_any_exception().is_ok());

        // Reset the invalid topic error
        metadata.update_with_current_request_version(&invalid_topic_response, false, now);

        // If we get a good update, the error should clear
        metadata.update_with_current_request_version(&empty_metadata_response(), false, now);
        assert!(metadata.maybe_throw_any_exception().is_ok());
    }

    /// Translated from `MetadataTest.testTopicAuthorizationError`.
    #[test]
    fn test_topic_authorization_error() {
        let metadata = new_metadata();
        let now: i64 = 10000;

        let unauthorized_topic = "foo";
        let unauthorized_response = request_test_utils::metadata_update_with_cluster_id(
            "clusterId",
            1,
            &[(unauthorized_topic.to_string(), Errors::TopicAuthorizationFailed)]
                .into_iter()
                .collect(),
            &HashMap::new(),
            &|_tp| None,
        );
        metadata.update_with_current_request_version(&unauthorized_response, false, now);

        let err = metadata.maybe_throw_any_exception().unwrap_err();
        match &err {
            MetadataError::TopicAuthorization(topics) => {
                assert_eq!(&[unauthorized_topic.to_string()].into_iter().collect::<HashSet<_>>(), topics);
            },
            _ => panic!("Expected TopicAuthorization error, got {:?}", err),
        }
        // We clear the exception once it has been raised
        assert!(metadata.maybe_throw_any_exception().is_ok());

        // Reset the unauthorized topic error
        metadata.update_with_current_request_version(&unauthorized_response, false, now);

        // If we get a good update, the error should clear
        metadata.update_with_current_request_version(&empty_metadata_response(), false, now);
        assert!(metadata.maybe_throw_any_exception().is_ok());
    }

    /// Translated from `MetadataTest.testMetadataTopicErrors`.
    #[test]
    fn test_metadata_topic_errors() {
        let metadata = new_metadata();
        let now: i64 = 10000;

        let mut topic_errors = HashMap::new();
        topic_errors.insert("invalidTopic".to_string(), Errors::InvalidTopicException);
        topic_errors.insert("sensitiveTopic1".to_string(), Errors::TopicAuthorizationFailed);
        topic_errors.insert("sensitiveTopic2".to_string(), Errors::TopicAuthorizationFailed);
        let metadata_response = request_test_utils::metadata_update_with_cluster_id(
            "clusterId",
            1,
            &topic_errors,
            &HashMap::new(),
            &|_tp| None,
        );

        metadata.update_with_current_request_version(&metadata_response, false, now);
        let err = metadata.maybe_throw_exception_for_topic("sensitiveTopic1").unwrap_err();
        match &err {
            MetadataError::TopicAuthorization(topics) => {
                assert_eq!(&["sensitiveTopic1".to_string()].into_iter().collect::<HashSet<_>>(), topics);
            },
            _ => panic!("Expected TopicAuthorization error"),
        }
        // Clear
        assert!(metadata.maybe_throw_any_exception().is_ok());

        metadata.update_with_current_request_version(&metadata_response, false, now);
        let err = metadata.maybe_throw_exception_for_topic("sensitiveTopic2").unwrap_err();
        match &err {
            MetadataError::TopicAuthorization(topics) => {
                assert_eq!(&["sensitiveTopic2".to_string()].into_iter().collect::<HashSet<_>>(), topics);
            },
            _ => panic!("Expected TopicAuthorization error"),
        }
        assert!(metadata.maybe_throw_any_exception().is_ok());

        metadata.update_with_current_request_version(&metadata_response, false, now);
        let err = metadata.maybe_throw_exception_for_topic("invalidTopic").unwrap_err();
        match &err {
            MetadataError::InvalidTopic(topics) => {
                assert_eq!(&["invalidTopic".to_string()].into_iter().collect::<HashSet<_>>(), topics);
            },
            _ => panic!("Expected InvalidTopic error"),
        }
        assert!(metadata.maybe_throw_any_exception().is_ok());

        // Other topics should not throw, but should clear existing exception
        metadata.update_with_current_request_version(&metadata_response, false, now);
        assert!(metadata.maybe_throw_exception_for_topic("anotherTopic").is_ok());
        assert!(metadata.maybe_throw_any_exception().is_ok());
    }

    /// Translated from `MetadataTest.testOutOfBandEpochUpdate`.
    #[test]
    fn test_out_of_band_epoch_update() {
        let metadata = new_metadata();
        let mut partition_counts = HashMap::new();
        partition_counts.insert("topic-1".to_string(), 5);
        let tp = TopicPartition::new("topic-1".to_string(), 0);

        metadata.update_with_current_request_version(&empty_metadata_response(), false, 0);
        assert!(!metadata.update_last_seen_epoch_if_newer(&tp, 99).unwrap());

        // Update epoch to 100
        let response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &HashMap::new(),
            &partition_counts,
            &|_tp| Some(100),
        );
        metadata.update_with_current_request_version(&response, false, 10);
        assert!(metadata.fetch().partition(&tp).is_some());
        assert_eq!(Some(100), metadata.last_seen_leader_epoch(&tp));

        // Simulate a leader epoch from another response
        assert!(metadata.update_last_seen_epoch_if_newer(&tp, 101).unwrap());

        // Cache of partition stays, but current partition info is not available since it's stale
        assert!(metadata.fetch().partition(&tp).is_some());
        assert_eq!(Some(5), metadata.fetch().partition_count_for_topic("topic-1"));
        assert!(metadata.partition_metadata_if_current(&tp).is_none());
        assert_eq!(Some(101), metadata.last_seen_leader_epoch(&tp));

        // Metadata with older epoch is rejected
        metadata.update_with_current_request_version(&response, false, 20);
        assert!(metadata.fetch().partition(&tp).is_some());
        assert_eq!(Some(5), metadata.fetch().partition_count_for_topic("topic-1"));
        assert!(metadata.partition_metadata_if_current(&tp).is_none());
        assert_eq!(Some(101), metadata.last_seen_leader_epoch(&tp));

        // Metadata with equal or newer epoch is accepted
        let response = request_test_utils::metadata_update_with_cluster_id(
            "dummy",
            1,
            &HashMap::new(),
            &partition_counts,
            &|_tp| Some(101),
        );
        metadata.update_with_current_request_version(&response, false, 30);
        assert!(metadata.fetch().partition(&tp).is_some());
        assert_eq!(Some(5), metadata.fetch().partition_count_for_topic("topic-1"));
        assert!(metadata.partition_metadata_if_current(&tp).is_some());
        assert_eq!(Some(101), metadata.last_seen_leader_epoch(&tp));
    }

    /// Translated from `MetadataTest.testRequestVersion`.
    #[test]
    fn test_request_version() {
        let metadata = new_metadata();
        let now: i64 = 10000;

        metadata.request_update(true);
        let v_and_b = metadata.new_metadata_request_and_version(now);
        metadata.update(
            v_and_b.request_version,
            &request_test_utils::metadata_update_with(1, &[("topic".to_string(), 1)].into_iter().collect()),
            false,
            now,
        );
        assert!(!metadata.update_requested());

        // Bump the request version for new topics
        metadata.request_update_for_new_topics();

        // Simulating a bump while a metadata request is in flight
        let v_and_b = metadata.new_metadata_request_and_version(now);
        metadata.request_update_for_new_topics();
        metadata.update(
            v_and_b.request_version,
            &request_test_utils::metadata_update_with(1, &[("topic".to_string(), 1)].into_iter().collect()),
            true,
            now,
        );

        // Metadata update is still needed
        assert!(metadata.update_requested());

        // The next update will resolve it
        let v_and_b = metadata.new_metadata_request_and_version(now);
        metadata.update(
            v_and_b.request_version,
            &request_test_utils::metadata_update_with(1, &[("topic".to_string(), 1)].into_iter().collect()),
            true,
            now,
        );
        assert!(!metadata.update_requested());
    }

    /// Translated from `MetadataTest.testNodeIfOffline`.
    #[test]
    fn test_node_if_offline() {
        let metadata = new_metadata();
        let mut partition_counts = HashMap::new();
        partition_counts.insert("topic-1".to_string(), 1);
        let node0 = Node::new(0, "localhost".to_string(), 9092);
        let node1 = Node::new(1, "localhost".to_string(), 9093);

        let response = request_test_utils::metadata_update_with_full(
            "dummy",
            2,
            &HashMap::new(),
            &partition_counts,
            &|_tp| Some(99),
            &move |error, partition, _leader, leader_epoch, _replicas, _isr, _offline_replicas| PartitionMetadata {
                error,
                topic_partition: partition.clone(),
                leader_id: Some(node0.id()),
                leader_epoch,
                replica_ids: vec![node0.id()],
                in_sync_replica_ids: Vec::new(),
                offline_replica_ids: vec![node1.id()],
            },
            ApiKeys::METADATA.latest_version(),
            &HashMap::new(),
        );
        metadata.update_with_current_request_version(&empty_metadata_response(), false, 0);
        metadata.update_with_current_request_version(&response, false, 10);

        let tp = TopicPartition::new("topic-1".to_string(), 0);

        assert_eq!(Some(0), metadata.fetch().node_if_online(&tp, 0).map(|n| n.id()));
        assert!(metadata.fetch().node_if_online(&tp, 1).is_none());
        assert_eq!(0, metadata.fetch().node_by_id(0).unwrap().id());
        assert_eq!(1, metadata.fetch().node_by_id(1).unwrap().id());
    }

    /// Translated from `MetadataTest.testTopicMetadataOnUpdatePartitionLeadership`.
    #[test]
    fn test_topic_metadata_on_update_partition_leadership() {
        let metadata = new_metadata();
        let topic = "input-topic";
        let topic_id = Uuid::random_uuid();
        let now: i64 = 10000;

        let node1 = Node::new(1, "localhost".to_string(), 9091);
        let node2 = Node::new(2, "localhost".to_string(), 9091);

        let tp0 = TopicPartition::new(topic.to_string(), 0);
        let tp1 = TopicPartition::new(topic.to_string(), 1);

        let partition0 = PartitionMetadata {
            error: Errors::None,
            topic_partition: tp0.clone(),
            leader_id: Some(1),
            leader_epoch: Some(1),
            replica_ids: vec![1, 2],
            in_sync_replica_ids: vec![1, 2],
            offline_replica_ids: Vec::new(),
        };
        let partition1 = PartitionMetadata {
            error: Errors::None,
            topic_partition: tp1.clone(),
            leader_id: Some(1),
            leader_epoch: Some(1),
            replica_ids: vec![1, 2],
            in_sync_replica_ids: vec![1, 2],
            offline_replica_ids: Vec::new(),
        };

        let topic_metadata = crate::common::requests::metadata_response::TopicMetadata {
            error: Errors::None,
            topic: topic.to_string(),
            topic_id,
            is_internal: false,
            partition_metadata: vec![partition0, partition1],
            authorized_operations: crate::common::requests::metadata_response::AUTHORIZED_OPERATIONS_OMITTED,
        };

        let response = request_test_utils::metadata_response(
            &[node1.clone(), node2],
            Some("clusterId"),
            node1.id(),
            vec![topic_metadata],
        );
        metadata.update_with_current_request_version(&response, false, now);

        assert_eq!(2, metadata.fetch().partitions_for_topic(topic).len());
        assert_eq!(1, metadata.fetch().partition(&tp0).unwrap().leader().unwrap().id());
        assert_eq!(1, metadata.fetch().partition(&tp1).unwrap().leader().unwrap().id());

        // partition 1 leader changes from node 1 to node 2
        let mut partition_leaders = HashMap::new();
        partition_leaders.insert(tp1.clone(), LeaderIdAndEpoch::new(Some(2), Some(3)));
        metadata.update_partition_leadership(&partition_leaders, &[node1]);

        assert_eq!(2, metadata.fetch().partitions_for_topic(topic).len());
        assert_eq!(1, metadata.fetch().partition(&tp0).unwrap().leader().unwrap().id());
        assert_eq!(2, metadata.fetch().partition(&tp1).unwrap().leader().unwrap().id());
    }
}
