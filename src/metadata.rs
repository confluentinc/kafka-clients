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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::{kafka_debug, kafka_error, kafka_info, kafka_trace};

use crate::common::Cluster;
use crate::common::ClusterResource;
use crate::common::KafkaError;
use crate::common::Node;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::internals::ClusterResourceListeners;
use crate::common::protocol::Errors;
use crate::common::requests::MetadataRequestBuilder;
use crate::common::requests::RECORD_BATCH_NO_PARTITION_LEADER_EPOCH;
use crate::common::requests::{MetadataResponse, PartitionMetadata};
use crate::common::utils::ExponentialBackoff;
use crate::common::utils::LogContext;

use super::MetadataSnapshot;
use super::common_client_configs;

/// Type alias for the retain topic function used to override topic retention behavior.
///
/// Corresponds to Java's `Metadata.retainTopic()` override pattern used by subclasses.
/// Parameters: `(topic_name, is_internal, now_ms) -> should_retain`.
type RetainTopicFn = dyn Fn(&str, bool, i64) -> bool + Send + Sync;

/// Type alias for a function that builds metadata request builders.
///
/// Used by subclasses (e.g., `ProducerMetadata`) to override metadata request
/// construction. Returns a `MetadataRequestBuilder`.
type MetadataRequestBuilderFn = dyn Fn() -> MetadataRequestBuilder + Send + Sync;

/// Type alias for a post-update callback invoked at the end of `Metadata::update()`.
///
/// Corresponds to Java's pattern of overriding `Metadata.update()` in subclasses
/// (e.g., `ProducerMetadata`) to perform additional work after the base update.
/// Parameters: `(response, is_partial_update, now_ms)`.
type PostUpdateFn = dyn Fn(&MetadataResponse, bool, i64) + Send + Sync;

/// Configuration for overriding `Metadata` behavior, used by subclasses like
/// `ProducerMetadata` that need to customize topic retention, request building,
/// and post-update processing.
///
/// Corresponds to Java's pattern of subclassing `Metadata` to override
/// `retainTopic()`, `newMetadataRequestBuilder()`,
/// `newMetadataRequestBuilderForNewTopics()`, and `update()`.
#[derive(Default)]
pub struct MetadataOverrides {
    /// Optional function to override topic retention behavior.
    /// When `None`, the default (retain all topics) is used.
    pub retain_topic_fn: Option<Box<RetainTopicFn>>,
    /// When `true`, `new_metadata_request_builder_for_new_topics()` returns a
    /// builder, enabling partial metadata requests.
    pub enable_partial_updates: bool,
    /// Optional function to override metadata request construction.
    pub request_builder_fn: Option<Box<MetadataRequestBuilderFn>>,
    /// Optional function to override metadata request construction for new topics.
    pub new_topics_request_builder_fn: Option<Box<MetadataRequestBuilderFn>>,
    /// Optional post-update callback invoked at the end of `update()`.
    pub post_update_fn: Option<Box<PostUpdateFn>>,
}

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
    /// Async notification for waiting on metadata updates.
    ///
    /// Corresponds to Java's `Object.wait()/notifyAll()` on the Metadata instance.
    /// Used by `await_update()` to asynchronously wait until the metadata version changes.
    update_notify: Notify,
    /// Optional custom retain topic function. When set, this overrides
    /// the default `retain_topic_default` behavior.
    ///
    /// Corresponds to Java's `Metadata.retainTopic()` override pattern used by
    /// subclasses (e.g., `ConsumerMetadata`). Lives outside the mutex because
    /// it is set once at construction and never mutated.
    retain_topic_fn: Option<Box<RetainTopicFn>>,
    /// When true, `new_metadata_request_builder_for_new_topics` returns a builder
    /// instead of `None`, enabling partial update requests.
    ///
    /// Corresponds to Java's override of `newMetadataRequestBuilderForNewTopics()`.
    enable_partial_updates: bool,
    /// Optional custom metadata request builder function. When set, overrides the
    /// default `new_metadata_request_builder()` behavior.
    ///
    /// Corresponds to Java's `Metadata.newMetadataRequestBuilder()` override pattern
    /// used by subclasses (e.g., `ProducerMetadata`).
    request_builder_fn: Option<Box<MetadataRequestBuilderFn>>,
    /// Optional custom metadata request builder function for new topics. When set,
    /// overrides the `new_metadata_request_builder_for_new_topics()` behavior.
    ///
    /// Corresponds to Java's `Metadata.newMetadataRequestBuilderForNewTopics()`
    /// override pattern used by subclasses (e.g., `ProducerMetadata`).
    new_topics_request_builder_fn: Option<Box<MetadataRequestBuilderFn>>,
    /// Optional post-update callback invoked at the end of `update()`.
    ///
    /// Corresponds to Java's pattern of overriding `Metadata.update()` in subclasses
    /// (e.g., `ProducerMetadata.update()`) to perform additional work after the base
    /// update completes.
    post_update_fn: Option<Box<PostUpdateFn>>,
    /// Contextual log message prefix.
    ///
    /// Translated from Java's `LogContext logContext` field in `Metadata`.
    log_context: LogContext,
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
    fatal_err: Option<KafkaError>,
    invalid_topics: HashSet<String>,
    unauthorized_topics: HashSet<String>,
    metadata_snapshot: Arc<MetadataSnapshot>,
    need_full_update: bool,
    need_partial_update: bool,
    equivalent_response_count: i64,
    cluster_resource_listeners: ClusterResourceListeners,
    is_closed: bool,
    last_seen_leader_epochs: HashMap<TopicPartition, i32>,
    bootstrap_addresses: Vec<SocketAddr>,
}

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
        Self::with_log_context(
            refresh_backoff_ms,
            refresh_backoff_max_ms,
            metadata_expire_ms,
            cluster_resource_listeners,
            LogContext::empty(),
        )
    }

    /// Creates a new `Metadata` instance with a `LogContext`.
    ///
    /// # Arguments
    /// * `refresh_backoff_ms` - The minimum amount of time between metadata refreshes
    ///   to avoid busy polling
    /// * `refresh_backoff_max_ms` - The maximum amount of time to wait between metadata
    ///   refreshes
    /// * `metadata_expire_ms` - The maximum amount of time that metadata can be retained
    ///   without refresh
    /// * `cluster_resource_listeners` - Listeners notified of cluster resource updates
    /// * `log_context` - Contextual log message prefix
    pub fn with_log_context(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        cluster_resource_listeners: ClusterResourceListeners,
        log_context: LogContext,
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
                metadata_snapshot: Arc::new(MetadataSnapshot::empty()),
                fatal_err: None,
                bootstrap_addresses: Vec::new(),
            }),
            update_notify: Notify::new(),
            retain_topic_fn: None,
            enable_partial_updates: false,
            request_builder_fn: None,
            new_topics_request_builder_fn: None,
            post_update_fn: None,
            log_context,
        }
    }

    /// Creates a new `Metadata` instance with custom behavior overrides.
    ///
    /// This corresponds to the Java pattern of subclassing `Metadata` to override
    /// `retainTopic()`, `newMetadataRequestBuilder()`,
    /// `newMetadataRequestBuilderForNewTopics()`, and `update()`.
    ///
    /// # Arguments
    /// * `refresh_backoff_ms` - The minimum amount of time between metadata refreshes
    /// * `refresh_backoff_max_ms` - The maximum amount of time to wait between metadata
    ///   refreshes
    /// * `metadata_expire_ms` - The maximum amount of time that metadata can be retained
    /// * `cluster_resource_listeners` - Listeners notified of cluster resource updates
    /// * `overrides` - Configuration for overriding default behavior
    pub fn with_overrides(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        cluster_resource_listeners: ClusterResourceListeners,
        overrides: MetadataOverrides,
        log_context: LogContext,
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
                metadata_snapshot: Arc::new(MetadataSnapshot::empty()),
                fatal_err: None,
                bootstrap_addresses: Vec::new(),
            }),
            update_notify: Notify::new(),
            retain_topic_fn: overrides.retain_topic_fn,
            enable_partial_updates: overrides.enable_partial_updates,
            request_builder_fn: overrides.request_builder_fn,
            new_topics_request_builder_fn: overrides.new_topics_request_builder_fn,
            post_update_fn: overrides.post_update_fn,
            log_context,
        }
    }

    /// Gets the current cluster info without blocking.
    pub fn fetch(&self) -> Arc<Cluster> {
        let inner = self.inner.lock().unwrap();
        inner.metadata_snapshot.cluster_arc()
    }

    /// Gets the current metadata snapshot.
    pub fn fetch_metadata_snapshot(&self) -> Arc<MetadataSnapshot> {
        let inner = self.inner.lock().unwrap();
        Arc::clone(&inner.metadata_snapshot)
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
    ) -> Result<bool, KafkaError> {
        if leader_epoch < 0 {
            return Err(KafkaError::fatal(
                Errors::UnknownServerError,
                format!("Invalid leader epoch {} (must be non-negative)", leader_epoch),
            ));
        }

        let mut inner = self.inner.lock().unwrap();
        let old_epoch = inner.last_seen_leader_epochs.get(topic_partition).copied();

        kafka_trace!(
            self.log_context,
            "Determining if we should replace existing epoch {:?} with new epoch {} for partition {}",
            old_epoch,
            leader_epoch,
            topic_partition
        );

        let updated = match old_epoch {
            None => {
                kafka_debug!(
                    self.log_context,
                    "Not replacing null epoch with new epoch {} for partition {}",
                    leader_epoch,
                    topic_partition
                );
                false
            },
            Some(old) if leader_epoch > old => {
                kafka_debug!(
                    self.log_context,
                    "Updating last seen epoch from {} to {} for partition {}",
                    old,
                    leader_epoch,
                    topic_partition
                );
                inner.last_seen_leader_epochs.insert(topic_partition.clone(), leader_epoch);
                true
            },
            Some(old) => {
                kafka_debug!(
                    self.log_context,
                    "Not replacing existing epoch {} with new epoch {} for partition {}",
                    old,
                    leader_epoch,
                    topic_partition
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
    pub fn add_cluster_update_listener(&self, listener: Box<dyn crate::common::ClusterResourceListener>) {
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
                .filter(|metadata| metadata.leader_epoch.unwrap_or(RECORD_BATCH_NO_PARTITION_LEADER_EPOCH) == epoch)
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
        inner.metadata_snapshot = Arc::new(MetadataSnapshot::bootstrap(&addresses));
        inner.bootstrap_addresses = addresses;
    }

    /// Rebootstraps the metadata with the original bootstrap addresses.
    pub fn rebootstrap(&self) {
        let mut inner = self.inner.lock().unwrap();
        let addresses = inner.bootstrap_addresses.clone();
        kafka_info!(self.log_context, "Rebootstrapping with {:?}", addresses);
        inner.need_full_update = true;
        inner.update_version += 1;
        inner.metadata_snapshot = Arc::new(MetadataSnapshot::bootstrap(&addresses));
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
        let retain_fn = &self.retain_topic_fn;
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

        let retain = |topic: &str, is_internal: bool, now: i64| -> bool {
            if let Some(f) = retain_fn {
                f(topic, is_internal, now)
            } else {
                Self::retain_topic_default(topic, is_internal, now)
            }
        };

        inner.metadata_snapshot = Arc::new(Self::handle_metadata_response(
            &mut inner,
            response,
            is_partial_update,
            now_ms,
            &retain,
            &self.log_context,
        ));

        let cluster = inner.metadata_snapshot.cluster_arc();
        Self::maybe_set_metadata_error(&mut inner, &cluster, &self.log_context);

        // Remove epochs for topics we no longer retain
        inner.last_seen_leader_epochs.retain(|tp, _| retain(tp.topic(), false, now_ms));

        let new_cluster_id = inner.metadata_snapshot.cluster_resource().cluster_id().map(|s| s.to_string());
        if previous_cluster_id != new_cluster_id {
            kafka_info!(self.log_context, "Cluster ID: {:?}", new_cluster_id);
        }
        let cluster_resource = inner.metadata_snapshot.cluster_resource();
        inner.cluster_resource_listeners.on_update(&cluster_resource);

        kafka_debug!(
            self.log_context,
            "Updated cluster metadata updateVersion {} to {}",
            inner.update_version,
            inner.metadata_snapshot
        );

        // Release the inner lock before calling the post-update callback to avoid
        // deadlocks — the callback may acquire its own lock (e.g., ProducerMetadata's
        // inner lock).
        drop(inner);

        if let Some(ref post_update) = self.post_update_fn {
            post_update(response, is_partial_update, now_ms);
        }

        // Notify all tasks waiting on metadata updates.
        // Corresponds to Java's notifyAll() at the end of Metadata.update().
        self.update_notify.notify_waiters();
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
                kafka_debug!(
                    self.log_context,
                    "For {}, incoming leader information is incomplete {}",
                    partition,
                    new_leader
                );
                continue;
            }

            let new_epoch = new_leader.epoch.unwrap();
            if let Some(current_epoch) = current_leader.epoch
                && new_epoch <= current_epoch
            {
                kafka_debug!(
                    self.log_context,
                    "For {}, incoming leader({}) is not-newer than the one in the existing metadata {}, so ignoring.",
                    partition,
                    new_leader,
                    current_leader
                );
                continue;
            }

            let new_leader_id = new_leader.leader_id.unwrap();
            if !new_nodes.contains_key(&new_leader_id) {
                kafka_debug!(
                    self.log_context,
                    "For {}, incoming leader({}), the corresponding node information for node-id {} is missing, so ignoring.",
                    partition,
                    new_leader,
                    new_leader_id
                );
                continue;
            }

            let existing_metadata = inner.metadata_snapshot.partition_metadata(partition);
            if existing_metadata.is_none() {
                kafka_debug!(
                    self.log_context,
                    "For {}, incoming leader({}), partition metadata is no longer cached, ignoring.",
                    partition,
                    new_leader
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
            kafka_debug!(self.log_context, "No relevant metadata updates.");
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

        inner.metadata_snapshot = Arc::new(inner.metadata_snapshot.merge_with(
            cluster_id,
            new_nodes,
            update_partition_metadata,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            controller,
            topic_ids_for_updated,
            |_topic, _is_internal| true,
        ));

        let cluster_resource = inner.metadata_snapshot.cluster_resource();
        inner.cluster_resource_listeners.on_update(&cluster_resource);

        updated_partitions
    }

    fn maybe_set_metadata_error(inner: &mut MetadataInner, cluster: &Cluster, log_context: &LogContext) {
        Self::clear_recoverable_errors(inner);
        Self::check_invalid_topics(inner, cluster, log_context);
        Self::check_unauthorized_topics(inner, cluster, log_context);
    }

    fn check_invalid_topics(inner: &mut MetadataInner, cluster: &Cluster, log_context: &LogContext) {
        if !cluster.invalid_topics().is_empty() {
            kafka_error!(
                log_context,
                "Metadata response reported invalid topics {:?}",
                cluster.invalid_topics()
            );
            inner.invalid_topics = cluster.invalid_topics().clone();
        }
    }

    fn check_unauthorized_topics(inner: &mut MetadataInner, cluster: &Cluster, log_context: &LogContext) {
        if !cluster.unauthorized_topics().is_empty() {
            kafka_error!(
                log_context,
                "Topic authorization failed for topics {:?}",
                cluster.unauthorized_topics()
            );
            inner.unauthorized_topics = cluster.unauthorized_topics().clone();
        }
    }

    /// Transform a MetadataResponse into a new MetadataSnapshot.
    fn handle_metadata_response(
        inner: &mut MetadataInner,
        metadata_response: &MetadataResponse,
        is_partial_update: bool,
        now_ms: i64,
        retain_topic: &dyn Fn(&str, bool, i64) -> bool,
        log_context: &LogContext,
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

            if !retain_topic(&topic_name, metadata.is_internal(), now_ms) {
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
                        log_context,
                    ) {
                        partitions.push(pm);
                    }

                    if Self::is_invalid_metadata_error(partition_metadata.error) {
                        kafka_debug!(
                            log_context,
                            "Requesting metadata update for partition {} due to error {:?}",
                            partition_metadata.topic_partition,
                            partition_metadata.error
                        );
                        inner.need_full_update = true;
                        if inner.equivalent_response_count > 0 {
                            // Don't reset, just don't increment further
                        }
                    }
                }
            } else {
                if Self::is_invalid_metadata_error(metadata.error()) {
                    kafka_debug!(
                        log_context,
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
                |topic, is_internal| !topics_ref.contains(topic) && retain_topic(topic, is_internal, now_ms),
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
        log_context: &LogContext,
    ) -> Option<PartitionMetadata> {
        let tp = &partition_metadata.topic_partition;
        if let Some(new_epoch) = partition_metadata.leader_epoch.filter(|_| has_reliable_leader_epoch) {
            let current_epoch = inner.last_seen_leader_epochs.get(tp).copied();

            match current_epoch {
                None => {
                    // No previous info, insert new epoch
                    kafka_debug!(
                        log_context,
                        "Setting the last seen epoch of partition {} to {} since the last known epoch was undefined.",
                        tp,
                        new_epoch
                    );
                    inner.last_seen_leader_epochs.insert(tp.clone(), new_epoch);
                    inner.equivalent_response_count = 0;
                    Some(partition_metadata.clone())
                },
                Some(_) if topic_id.is_some() && topic_id != old_topic_id => {
                    // Topic ID changed (topic deleted and re-created)
                    kafka_info!(
                        log_context,
                        "Resetting the last seen epoch of partition {} to {} since the associated topicId changed from {:?} to {:?}",
                        tp,
                        new_epoch,
                        old_topic_id,
                        topic_id
                    );
                    inner.last_seen_leader_epochs.insert(tp.clone(), new_epoch);
                    inner.equivalent_response_count = 0;
                    Some(partition_metadata.clone())
                },
                Some(current) if new_epoch >= current => {
                    kafka_debug!(
                        log_context,
                        "Updating last seen epoch for partition {} from {} to epoch {} from new metadata",
                        tp,
                        current,
                        new_epoch
                    );
                    inner.last_seen_leader_epochs.insert(tp.clone(), new_epoch);
                    if new_epoch > current {
                        inner.equivalent_response_count = 0;
                    }
                    Some(partition_metadata.clone())
                },
                Some(current) => {
                    // Old epoch, ignore
                    kafka_debug!(
                        log_context,
                        "Got metadata for an older epoch {} (current is {}) for partition {}, not updating",
                        new_epoch,
                        current,
                        tp
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
    /// Returns `true` for error codes whose Java errors extend
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

    /// If any non-retriable errors were encountered during metadata update,
    /// clear and return the error.
    pub fn maybe_return_any_error(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();
        Self::clear_errors_and_maybe_return_error(&mut inner, Self::recoverable_error)
    }

    /// If any non-retriable errors were encountered for the specified topic,
    /// return the error. All errors from the last metadata update are cleared.
    pub fn maybe_return_error_for_topic(&self, topic: &str) -> Result<(), KafkaError> {
        let topic = topic.to_string();
        let mut inner = self.inner.lock().unwrap();
        Self::clear_errors_and_maybe_return_error(&mut inner, |i| Self::recoverable_error_for_topic(i, &topic))
    }

    /// If any fatal errors were encountered during metadata update, return the error.
    pub fn maybe_return_fatal_error(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(err) = inner.fatal_err.take() {
            return Err(err);
        }
        Ok(())
    }

    fn clear_errors_and_maybe_return_error<F>(
        inner: &mut MetadataInner,
        recoverable_supplier: F,
    ) -> Result<(), KafkaError>
    where
        F: FnOnce(&MetadataInner) -> Option<KafkaError>,
    {
        let metadata_error = inner.fatal_err.take().or_else(|| recoverable_supplier(inner));
        Self::clear_recoverable_errors(inner);
        match metadata_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn recoverable_error(inner: &MetadataInner) -> Option<KafkaError> {
        if !inner.unauthorized_topics.is_empty() {
            Some(KafkaError::topic_authorization(inner.unauthorized_topics.clone()))
        } else if !inner.invalid_topics.is_empty() {
            Some(KafkaError::invalid_topics(inner.invalid_topics.clone()))
        } else {
            None
        }
    }

    fn recoverable_error_for_topic(inner: &MetadataInner, topic: &str) -> Option<KafkaError> {
        if inner.unauthorized_topics.contains(topic) {
            Some(KafkaError::topic_authorization([topic.to_string()].into_iter().collect()))
        } else if inner.invalid_topics.contains(topic) {
            Some(KafkaError::invalid_topics([topic.to_string()].into_iter().collect()))
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
    pub fn fatal_error(&self, error: KafkaError) {
        let mut inner = self.inner.lock().unwrap();
        inner.fatal_err = Some(error);
    }

    /// Wait for metadata update until the given version is exceeded or the timeout expires.
    ///
    /// Corresponds to Java's `Metadata.awaitUpdate(int lastVersion, long timeoutMs)`.
    ///
    /// # Arguments
    /// * `last_version` - The metadata version at the time the caller started waiting.
    /// * `timeout_ms` - Maximum time to wait in milliseconds.
    ///
    /// # Errors
    /// Returns a `KafkaError::Timeout` if the metadata version is not updated within
    /// the given timeout.
    pub async fn await_update(&self, last_version: i32, timeout_ms: i64) -> Result<(), KafkaError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64);

        loop {
            // Create the notified future BEFORE checking the condition to avoid
            // missing a notification between the check and the await.
            let notified = self.update_notify.notified();

            {
                let inner = self.inner.lock().unwrap();
                if inner.update_version > last_version {
                    return Ok(());
                }
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(KafkaError::timeout(format!(
                    "Failed to update metadata after {} ms.",
                    timeout_ms
                )));
            }

            if tokio::time::timeout(remaining, notified).await.is_err() {
                // Timed out — check once more under the lock.
                let inner = self.inner.lock().unwrap();
                if inner.update_version > last_version {
                    return Ok(());
                }
                return Err(KafkaError::timeout(format!(
                    "Failed to update metadata after {} ms.",
                    timeout_ms
                )));
            }
        }
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
        // Wake up any tasks waiting for metadata updates so they can detect the close.
        inner.update_version += 1;
        drop(inner);
        self.update_notify.notify_waiters();
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
            request = self.new_metadata_request_builder_for_new_topics();
            is_partial_update = true;
        }
        if request.is_none() {
            request = Some(self.new_metadata_request_builder());
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
    ///
    /// When a custom `request_builder_fn` is set (e.g. by `ProducerMetadata`),
    /// that function is called instead of the default `all_topics()`.
    fn new_metadata_request_builder(&self) -> MetadataRequestBuilder {
        if let Some(f) = &self.request_builder_fn {
            f()
        } else {
            MetadataRequestBuilder::all_topics()
        }
    }

    /// Constructs and returns a metadata request builder for fetching cluster data
    /// and any uncached topics, otherwise `None` if the functionality is not supported.
    ///
    /// When a custom `new_topics_request_builder_fn` is set (e.g. by `ProducerMetadata`),
    /// that function is called. When `enable_partial_updates` is set (e.g. by
    /// `ConsumerMetadata`), the default metadata request builder is returned.
    /// Otherwise returns `None`.
    fn new_metadata_request_builder_for_new_topics(&self) -> Option<MetadataRequestBuilder> {
        if let Some(f) = &self.new_topics_request_builder_fn {
            Some(f())
        } else if self.enable_partial_updates {
            Some(self.new_metadata_request_builder())
        } else {
            None
        }
    }

    /// Based on the topic name, check if the topic metadata should be kept when received
    /// in a metadata response. The default implementation returns `true` for all topics.
    fn retain_topic_default(_topic: &str, _is_internal: bool, _now_ms: i64) -> bool {
        true
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
    use crate::common::ClusterResourceListener;
    use crate::common::internals::ClusterResourceListeners;
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

        let err = metadata.maybe_return_any_error().unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicException);
        match &err {
            KafkaError::InvalidTopic(e) => {
                assert_eq!(
                    e.invalid_topics,
                    [invalid_topic.to_string()].into_iter().collect::<HashSet<_>>()
                );
            },
            _ => panic!("Expected InvalidTopic error, got {:?}", err),
        }
        // We clear the error once it has been raised to the user
        assert!(metadata.maybe_return_any_error().is_ok());

        // Reset the invalid topic error
        metadata.update_with_current_request_version(&invalid_topic_response, false, now);

        // If we get a good update, the error should clear
        metadata.update_with_current_request_version(&empty_metadata_response(), false, now);
        assert!(metadata.maybe_return_any_error().is_ok());
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

        let err = metadata.maybe_return_any_error().unwrap_err();
        assert_eq!(err.error(), Errors::TopicAuthorizationFailed);
        match &err {
            KafkaError::TopicAuthorization(e) => {
                assert_eq!(
                    e.unauthorized_topics,
                    [unauthorized_topic.to_string()].into_iter().collect::<HashSet<_>>()
                );
            },
            _ => panic!("Expected TopicAuthorization error, got {:?}", err),
        }
        // We clear the error once it has been raised
        assert!(metadata.maybe_return_any_error().is_ok());

        // Reset the unauthorized topic error
        metadata.update_with_current_request_version(&unauthorized_response, false, now);

        // If we get a good update, the error should clear
        metadata.update_with_current_request_version(&empty_metadata_response(), false, now);
        assert!(metadata.maybe_return_any_error().is_ok());
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
        let err = metadata.maybe_return_error_for_topic("sensitiveTopic1").unwrap_err();
        assert_eq!(err.error(), Errors::TopicAuthorizationFailed);
        match &err {
            KafkaError::TopicAuthorization(e) => {
                assert_eq!(
                    e.unauthorized_topics,
                    ["sensitiveTopic1".to_string()].into_iter().collect::<HashSet<_>>()
                );
            },
            _ => panic!("Expected TopicAuthorization error"),
        }
        // Clear
        assert!(metadata.maybe_return_any_error().is_ok());

        metadata.update_with_current_request_version(&metadata_response, false, now);
        let err = metadata.maybe_return_error_for_topic("sensitiveTopic2").unwrap_err();
        assert_eq!(err.error(), Errors::TopicAuthorizationFailed);
        match &err {
            KafkaError::TopicAuthorization(e) => {
                assert_eq!(
                    e.unauthorized_topics,
                    ["sensitiveTopic2".to_string()].into_iter().collect::<HashSet<_>>()
                );
            },
            _ => panic!("Expected TopicAuthorization error"),
        }
        assert!(metadata.maybe_return_any_error().is_ok());

        metadata.update_with_current_request_version(&metadata_response, false, now);
        let err = metadata.maybe_return_error_for_topic("invalidTopic").unwrap_err();
        assert_eq!(err.error(), Errors::InvalidTopicException);
        match &err {
            KafkaError::InvalidTopic(e) => {
                assert_eq!(
                    e.invalid_topics,
                    ["invalidTopic".to_string()].into_iter().collect::<HashSet<_>>()
                );
            },
            _ => panic!("Expected InvalidTopic error"),
        }
        assert!(metadata.maybe_return_any_error().is_ok());

        // Other topics should not return an error, but should clear existing error
        metadata.update_with_current_request_version(&metadata_response, false, now);
        assert!(metadata.maybe_return_error_for_topic("anotherTopic").is_ok());
        assert!(metadata.maybe_return_any_error().is_ok());
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

        let topic_metadata = crate::common::requests::TopicMetadata {
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

    /// Translated from `MetadataTest.testIgnoreLeaderEpochInOlderMetadataResponse`.
    ///
    /// Prior to Kafka version 2.4 (which coincides with Metadata version 9), the broker
    /// does not propagate leader epoch information accurately while a reassignment is in
    /// progress, so we cannot rely on it.
    #[test]
    fn test_ignore_leader_epoch_in_older_metadata_response() {
        use crate::common::protocol::Readable;
        use crate::common::protocol::message_util;
        use crate::metadata_response_data::{MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic};

        let metadata = new_metadata();
        let tp = TopicPartition::new("topic".to_string(), 0);

        let mut partition_metadata = MetadataResponsePartition::new();
        partition_metadata.set_partition_index(tp.partition());
        partition_metadata.set_leader_id(5);
        partition_metadata.set_leader_epoch(10);
        partition_metadata.set_replica_nodes(vec![1, 2, 3]);
        partition_metadata.set_isr_nodes(vec![1, 2, 3]);
        partition_metadata.set_offline_replicas(Vec::new());
        partition_metadata.set_error_code(Errors::None.code());

        let mut topic_metadata = MetadataResponseTopic::new();
        topic_metadata.set_name(Some(tp.topic().to_string()));
        topic_metadata.set_error_code(Errors::None.code());
        topic_metadata.set_partitions(vec![partition_metadata]);
        topic_metadata.set_is_internal(false);

        let mut data = MetadataResponseData::new();
        data.set_cluster_id(Some("clusterId".to_string()));
        data.set_controller_id(0);
        data.set_topics(vec![topic_metadata]);
        data.set_brokers(Vec::new());

        // For versions < 9, leader epochs should not be reliable
        for version in ApiKeys::METADATA.oldest_version()..9 {
            let mut readable = message_util::to_byte_buffer_accessor(&mut data, version).unwrap();
            let response = MetadataResponse::parse(&mut readable as &mut dyn Readable, version).unwrap();
            assert!(
                !response.has_reliable_leader_epochs(),
                "Version {} should not have reliable leader epochs",
                version
            );
            metadata.update_with_current_request_version(&response, false, 100);
            let pm = metadata.partition_metadata_if_current(&tp);
            assert!(pm.is_some(), "Partition metadata should be present for version {}", version);
            assert_eq!(
                None,
                pm.unwrap().leader_epoch,
                "Leader epoch should be None for version {}",
                version
            );
        }

        // For versions >= 9, leader epochs should be reliable
        for version in 9..=ApiKeys::METADATA.latest_version() {
            let mut readable = message_util::to_byte_buffer_accessor(&mut data, version).unwrap();
            let response = MetadataResponse::parse(&mut readable as &mut dyn Readable, version).unwrap();
            assert!(
                response.has_reliable_leader_epochs(),
                "Version {} should have reliable leader epochs",
                version
            );
            metadata.update_with_current_request_version(&response, false, 100);
            let pm = metadata.partition_metadata_if_current(&tp);
            assert!(pm.is_some(), "Partition metadata should be present for version {}", version);
            assert_eq!(
                Some(10),
                pm.unwrap().leader_epoch,
                "Leader epoch should be Some(10) for version {}",
                version
            );
        }
    }

    /// Translated from `MetadataTest.testStaleMetadata`.
    #[test]
    fn test_stale_metadata() {
        use crate::metadata_response_data::{MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic};

        let metadata = new_metadata();
        let tp = TopicPartition::new("topic".to_string(), 0);

        let mut partition_metadata = MetadataResponsePartition::new();
        partition_metadata.set_partition_index(tp.partition());
        partition_metadata.set_leader_id(1);
        partition_metadata.set_leader_epoch(10);
        partition_metadata.set_replica_nodes(vec![1, 2, 3]);
        partition_metadata.set_isr_nodes(vec![1, 2, 3]);
        partition_metadata.set_offline_replicas(Vec::new());
        partition_metadata.set_error_code(Errors::None.code());

        let mut topic_metadata = MetadataResponseTopic::new();
        topic_metadata.set_name(Some(tp.topic().to_string()));
        topic_metadata.set_error_code(Errors::None.code());
        topic_metadata.set_partitions(vec![partition_metadata.clone()]);
        topic_metadata.set_is_internal(false);

        let mut data = MetadataResponseData::new();
        data.set_cluster_id(Some("clusterId".to_string()));
        data.set_controller_id(0);
        data.set_topics(vec![topic_metadata.clone()]);
        data.set_brokers(Vec::new());

        metadata.update_with_current_request_version(
            &MetadataResponse::new(data.clone(), ApiKeys::METADATA.latest_version()),
            false,
            100,
        );

        // Older epoch with changed ISR should be ignored
        partition_metadata.set_partition_index(tp.partition());
        partition_metadata.set_leader_id(1);
        partition_metadata.set_leader_epoch(9);
        partition_metadata.set_replica_nodes(vec![1, 2, 3]);
        partition_metadata.set_isr_nodes(vec![1, 2]);
        partition_metadata.set_offline_replicas(Vec::new());
        partition_metadata.set_error_code(Errors::None.code());

        topic_metadata.set_partitions(vec![partition_metadata]);
        data.set_topics(vec![topic_metadata]);

        metadata.update_with_current_request_version(
            &MetadataResponse::new(data, ApiKeys::METADATA.latest_version()),
            false,
            101,
        );
        assert_eq!(Some(10), metadata.last_seen_leader_epoch(&tp));

        let pm = metadata.partition_metadata_if_current(&tp);
        assert!(pm.is_some());
        let pm = pm.unwrap();

        assert_eq!(vec![1, 2, 3], pm.in_sync_replica_ids);
        assert_eq!(Some(10), pm.leader_epoch);
    }

    /// Translated from `MetadataTest.testPartialMetadataUpdate`.
    #[test]
    fn test_partial_metadata_update() {
        let now: i64 = 10000;

        let metadata = Metadata::with_overrides(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            ClusterResourceListeners::new(),
            MetadataOverrides { enable_partial_updates: true, ..MetadataOverrides::default() },
            LogContext::empty(),
        );

        assert!(!metadata.update_requested());

        // Request a metadata update. This must force a full metadata update request.
        metadata.request_update(true);
        let v_and_b = metadata.new_metadata_request_and_version(now);
        assert!(!v_and_b.is_partial_update);
        metadata.update(
            v_and_b.request_version,
            &request_test_utils::metadata_update_with(1, &[("topic".to_string(), 1)].into_iter().collect()),
            false,
            now,
        );
        assert!(!metadata.update_requested());

        // Request a metadata update for a new topic. This should perform a partial metadata update.
        metadata.request_update_for_new_topics();
        let v_and_b = metadata.new_metadata_request_and_version(now);
        assert!(v_and_b.is_partial_update);
        metadata.update(
            v_and_b.request_version,
            &request_test_utils::metadata_update_with(1, &[("topic".to_string(), 1)].into_iter().collect()),
            true,
            now,
        );
        assert!(!metadata.update_requested());

        // Request both types of metadata updates. This should always perform a full update.
        metadata.request_update(true);
        metadata.request_update_for_new_topics();
        let v_and_b = metadata.new_metadata_request_and_version(now);
        assert!(!v_and_b.is_partial_update);
        metadata.update(
            v_and_b.request_version,
            &request_test_utils::metadata_update_with(1, &[("topic".to_string(), 1)].into_iter().collect()),
            false,
            now,
        );
        assert!(!metadata.update_requested());

        // Request only a partial metadata update, but elapse enough time such that a full refresh is needed.
        metadata.request_update_for_new_topics();
        let refresh_time_ms = now + metadata.metadata_expire_ms();
        let v_and_b = metadata.new_metadata_request_and_version(refresh_time_ms);
        assert!(!v_and_b.is_partial_update);
        metadata.update(
            v_and_b.request_version,
            &request_test_utils::metadata_update_with(1, &[("topic".to_string(), 1)].into_iter().collect()),
            true,
            refresh_time_ms,
        );
        assert!(!metadata.update_requested());

        // Request two partial metadata updates that are overlapping.
        metadata.request_update_for_new_topics();
        let v_and_b = metadata.new_metadata_request_and_version(now);
        assert!(v_and_b.is_partial_update);
        metadata.request_update_for_new_topics();
        let overlapping_v_and_b = metadata.new_metadata_request_and_version(now);
        assert!(overlapping_v_and_b.is_partial_update);
        assert!(metadata.update_requested());
        metadata.update(
            v_and_b.request_version,
            &request_test_utils::metadata_update_with(1, &[("topic-1".to_string(), 1)].into_iter().collect()),
            true,
            now,
        );
        assert!(metadata.update_requested());
        metadata.update(
            overlapping_v_and_b.request_version,
            &request_test_utils::metadata_update_with(1, &[("topic-2".to_string(), 1)].into_iter().collect()),
            true,
            now,
        );
        assert!(!metadata.update_requested());
    }

    /// Translated from `MetadataTest.testNodeIfOnlineWhenNotInReplicaSet`.
    #[test]
    fn test_node_if_online_when_not_in_replica_set() {
        let metadata = new_metadata();
        let mut partition_counts = HashMap::new();
        partition_counts.insert("topic-1".to_string(), 1);
        let node0 = Node::new(0, "localhost".to_string(), 9092);

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
                offline_replica_ids: Vec::new(),
            },
            ApiKeys::METADATA.latest_version(),
            &HashMap::new(),
        );
        metadata.update_with_current_request_version(&empty_metadata_response(), false, 0);
        metadata.update_with_current_request_version(&response, false, 10);

        let tp = TopicPartition::new("topic-1".to_string(), 0);

        assert_eq!(1, metadata.fetch().node_by_id(1).unwrap().id());
        assert!(metadata.fetch().node_if_online(&tp, 1).is_none());
    }

    /// Translated from `MetadataTest.testNodeIfOnlineNonExistentTopicPartition`.
    #[test]
    fn test_node_if_online_non_existent_topic_partition() {
        let metadata = new_metadata();
        let metadata_response = request_test_utils::metadata_update_with(2, &HashMap::new());
        metadata.update_with_current_request_version(&metadata_response, false, 0);

        let tp = TopicPartition::new("topic-1".to_string(), 0);

        assert_eq!(0, metadata.fetch().node_by_id(0).unwrap().id());
        assert!(metadata.fetch().partition(&tp).is_none());
        assert!(metadata.fetch().node_if_online(&tp, 0).is_none());
    }

    /// Translated from `MetadataTest.testLeaderMetadataInconsistentWithBrokerMetadata`.
    ///
    /// Tests a reordering scenario which can lead to inconsistent leader state.
    /// A partition initially has one broker offline. That broker comes online and
    /// is elected leader. The client sees these two events in the opposite order.
    #[test]
    fn test_leader_metadata_inconsistent_with_broker_metadata() {
        use crate::metadata_response_data::{
            MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
        };

        let metadata = new_metadata();
        let tp = TopicPartition::new("topic".to_string(), 0);

        let node0 = Node::new(0, "localhost".to_string(), 9092);
        let node1 = Node::new(1, "localhost".to_string(), 9093);
        let node2 = Node::new(2, "localhost".to_string(), 9094);

        // The first metadata received by broker (epoch=10)
        let mut first_partition_metadata = MetadataResponsePartition::new();
        first_partition_metadata.set_partition_index(tp.partition());
        first_partition_metadata.set_error_code(Errors::None.code());
        first_partition_metadata.set_leader_epoch(10);
        first_partition_metadata.set_leader_id(0);
        first_partition_metadata.set_replica_nodes(vec![0, 1, 2]);
        first_partition_metadata.set_isr_nodes(vec![0, 1, 2]);
        first_partition_metadata.set_offline_replicas(Vec::new());

        // The second metadata received has stale metadata (epoch=8)
        let mut second_partition_metadata = MetadataResponsePartition::new();
        second_partition_metadata.set_partition_index(tp.partition());
        second_partition_metadata.set_error_code(Errors::None.code());
        second_partition_metadata.set_leader_epoch(8);
        second_partition_metadata.set_leader_id(1);
        second_partition_metadata.set_replica_nodes(vec![0, 1, 2]);
        second_partition_metadata.set_isr_nodes(vec![1, 2]);
        second_partition_metadata.set_offline_replicas(vec![0]);

        let build_topic_collection =
            |topic: &str, partition_metadata: MetadataResponsePartition| -> Vec<MetadataResponseTopic> {
                let mut topic_metadata = MetadataResponseTopic::new();
                topic_metadata.set_error_code(Errors::None.code());
                topic_metadata.set_name(Some(topic.to_string()));
                topic_metadata.set_is_internal(false);
                topic_metadata.set_partitions(vec![partition_metadata]);
                vec![topic_metadata]
            };

        let build_broker_collection = |nodes: &[&Node]| -> Vec<MetadataResponseBroker> {
            nodes
                .iter()
                .map(|node| {
                    let mut broker = MetadataResponseBroker::new();
                    broker.set_node_id(node.id());
                    broker.set_host(node.host().to_string());
                    broker.set_port(node.port());
                    broker.set_rack(node.rack().map(|r| r.to_string()));
                    broker
                })
                .collect()
        };

        let mut data1 = MetadataResponseData::new();
        data1.set_topics(build_topic_collection(tp.topic(), first_partition_metadata));
        data1.set_brokers(build_broker_collection(&[&node0, &node1, &node2]));
        metadata.update_with_current_request_version(
            &MetadataResponse::new(data1, ApiKeys::METADATA.latest_version()),
            false,
            10,
        );

        let mut data2 = MetadataResponseData::new();
        data2.set_topics(build_topic_collection(tp.topic(), second_partition_metadata));
        data2.set_brokers(build_broker_collection(&[&node1, &node2]));
        metadata.update_with_current_request_version(
            &MetadataResponse::new(data2, ApiKeys::METADATA.latest_version()),
            false,
            20,
        );

        assert!(metadata.fetch().leader_for(&tp).is_none());
        assert_eq!(Some(10), metadata.last_seen_leader_epoch(&tp));
        assert!(metadata.current_leader(&tp).leader.is_none());
    }

    /// Translated from `MetadataTest.testMetadataMerge`.
    #[test]
    fn test_metadata_merge() {
        let now: i64 = 10000;
        let mut topic_ids = HashMap::new();

        let retain_topics: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let retain_topics_clone = retain_topics.clone();

        let metadata = Metadata::with_overrides(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            ClusterResourceListeners::new(),
            MetadataOverrides {
                retain_topic_fn: Some(Box::new(move |topic: &str, _is_internal: bool, _now_ms: i64| -> bool {
                    retain_topics_clone.lock().unwrap().contains(topic)
                })),
                ..MetadataOverrides::default()
            },
            LogContext::empty(),
        );

        // Initialize a metadata instance with two topic variants "old" and "keep". Both will be retained.
        let old_cluster_id = "oldClusterId";
        let old_nodes = 2;
        let mut old_topic_errors = HashMap::new();
        old_topic_errors.insert("oldInvalidTopic".to_string(), Errors::InvalidTopicException);
        old_topic_errors.insert("keepInvalidTopic".to_string(), Errors::InvalidTopicException);
        old_topic_errors.insert("oldUnauthorizedTopic".to_string(), Errors::TopicAuthorizationFailed);
        old_topic_errors.insert("keepUnauthorizedTopic".to_string(), Errors::TopicAuthorizationFailed);
        let mut old_topic_partition_counts = HashMap::new();
        old_topic_partition_counts.insert("oldValidTopic".to_string(), 2);
        old_topic_partition_counts.insert("keepValidTopic".to_string(), 3);

        *retain_topics.lock().unwrap() = [
            "oldInvalidTopic",
            "keepInvalidTopic",
            "oldUnauthorizedTopic",
            "keepUnauthorizedTopic",
            "oldValidTopic",
            "keepValidTopic",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        topic_ids.insert("oldValidTopic".to_string(), Uuid::random_uuid());
        topic_ids.insert("keepValidTopic".to_string(), Uuid::random_uuid());
        let metadata_response = request_test_utils::metadata_update_with_ids(
            old_cluster_id,
            old_nodes,
            &old_topic_errors,
            &old_topic_partition_counts,
            &|_tp| Some(100),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&metadata_response, true, now);
        let metadata_topic_ids1 = metadata.topic_ids();
        for topic in retain_topics.lock().unwrap().iter() {
            assert_eq!(
                metadata_topic_ids1.get(topic).copied(),
                topic_ids.get(topic).copied(),
                "Topic ID mismatch for {}",
                topic
            );
        }

        // Update the metadata to add a new topic variant, "new", which will be retained with "keep".
        // Note this means that all of the "old" topics should be dropped.
        let cluster = metadata.fetch();
        assert_eq!(Some(old_cluster_id), cluster.cluster_resource().cluster_id());
        assert_eq!(old_nodes as usize, cluster.nodes().len());
        assert_eq!(
            &["oldInvalidTopic", "keepInvalidTopic"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
            cluster.invalid_topics()
        );
        assert_eq!(
            &["oldUnauthorizedTopic", "keepUnauthorizedTopic"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
            cluster.unauthorized_topics()
        );
        assert_eq!(
            ["oldValidTopic", "keepValidTopic"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
            cluster.topics().map(|s| s.to_string()).collect::<HashSet<_>>()
        );
        assert_eq!(2, cluster.partitions_for_topic("oldValidTopic").len());
        assert_eq!(3, cluster.partitions_for_topic("keepValidTopic").len());
        let cluster_topic_ids: HashSet<Uuid> = cluster.topic_ids().cloned().collect();
        let expected_topic_ids: HashSet<Uuid> = topic_ids.values().copied().collect();
        assert_eq!(expected_topic_ids, cluster_topic_ids);

        let new_cluster_id = "newClusterId";
        let new_nodes = old_nodes + 1;
        let mut new_topic_errors = HashMap::new();
        new_topic_errors.insert("newInvalidTopic".to_string(), Errors::InvalidTopicException);
        new_topic_errors.insert("newUnauthorizedTopic".to_string(), Errors::TopicAuthorizationFailed);
        let mut new_topic_partition_counts = HashMap::new();
        new_topic_partition_counts.insert("keepValidTopic".to_string(), 2);
        new_topic_partition_counts.insert("newValidTopic".to_string(), 4);

        *retain_topics.lock().unwrap() = [
            "keepInvalidTopic",
            "newInvalidTopic",
            "keepUnauthorizedTopic",
            "newUnauthorizedTopic",
            "keepValidTopic",
            "newValidTopic",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        topic_ids.insert("newValidTopic".to_string(), Uuid::random_uuid());
        let metadata_response = request_test_utils::metadata_update_with_ids(
            new_cluster_id,
            new_nodes,
            &new_topic_errors,
            &new_topic_partition_counts,
            &|_tp| Some(200),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&metadata_response, true, now);
        topic_ids.remove("oldValidTopic");
        let metadata_topic_ids2 = metadata.topic_ids();
        for topic in retain_topics.lock().unwrap().iter() {
            assert_eq!(
                metadata_topic_ids2.get(topic).copied(),
                topic_ids.get(topic).copied(),
                "Topic ID mismatch for {}",
                topic
            );
        }
        assert!(!metadata_topic_ids2.contains_key("oldValidTopic"));

        let cluster = metadata.fetch();
        assert_eq!(Some(new_cluster_id), cluster.cluster_resource().cluster_id());
        assert_eq!(new_nodes as usize, cluster.nodes().len());
        assert_eq!(
            &["keepInvalidTopic", "newInvalidTopic"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
            cluster.invalid_topics()
        );
        assert_eq!(
            &["keepUnauthorizedTopic", "newUnauthorizedTopic"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
            cluster.unauthorized_topics()
        );
        assert_eq!(
            ["keepValidTopic", "newValidTopic"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
            cluster.topics().map(|s| s.to_string()).collect::<HashSet<_>>()
        );
        assert_eq!(2, cluster.partitions_for_topic("keepValidTopic").len());
        assert_eq!(4, cluster.partitions_for_topic("newValidTopic").len());
        let cluster_topic_ids: HashSet<Uuid> = cluster.topic_ids().cloned().collect();
        let expected_topic_ids: HashSet<Uuid> = topic_ids.values().copied().collect();
        assert_eq!(expected_topic_ids, cluster_topic_ids);

        // Perform another metadata update, but this time all topic metadata should be cleared.
        *retain_topics.lock().unwrap() = HashSet::new();

        let metadata_response = request_test_utils::metadata_update_with_ids(
            new_cluster_id,
            new_nodes,
            &new_topic_errors,
            &new_topic_partition_counts,
            &|_tp| Some(300),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&metadata_response, true, now);
        let metadata_topic_ids3 = metadata.topic_ids();
        for topic_name in topic_ids.keys() {
            assert!(
                !metadata_topic_ids3.contains_key(topic_name),
                "Topic {} should not have an ID",
                topic_name
            );
        }

        let cluster = metadata.fetch();
        assert_eq!(Some(new_cluster_id), cluster.cluster_resource().cluster_id());
        assert_eq!(new_nodes as usize, cluster.nodes().len());
        assert!(cluster.invalid_topics().is_empty());
        assert!(cluster.unauthorized_topics().is_empty());
        assert_eq!(0, cluster.topics().count());
        assert_eq!(0, cluster.topic_ids().count());
    }

    /// Translated from `MetadataTest.testMetadataMergeOnIdDowngrade`.
    #[test]
    fn test_metadata_merge_on_id_downgrade() {
        let now: i64 = 10000;
        let mut topic_ids = HashMap::new();

        let retain_topics: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let retain_topics_clone = retain_topics.clone();

        let metadata = Metadata::with_overrides(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            ClusterResourceListeners::new(),
            MetadataOverrides {
                retain_topic_fn: Some(Box::new(move |topic: &str, _is_internal: bool, _now_ms: i64| -> bool {
                    retain_topics_clone.lock().unwrap().contains(topic)
                })),
                ..MetadataOverrides::default()
            },
            LogContext::empty(),
        );

        // Initialize a metadata instance with two topics. Both will be retained.
        let cluster_id = "clusterId";
        let nodes = 2;
        let mut topic_partition_counts = HashMap::new();
        topic_partition_counts.insert("validTopic1".to_string(), 2);
        topic_partition_counts.insert("validTopic2".to_string(), 3);

        *retain_topics.lock().unwrap() = ["validTopic1", "validTopic2"].iter().map(|s| s.to_string()).collect();

        topic_ids.insert("validTopic1".to_string(), Uuid::random_uuid());
        topic_ids.insert("validTopic2".to_string(), Uuid::random_uuid());
        let metadata_response = request_test_utils::metadata_update_with_ids(
            cluster_id,
            nodes,
            &HashMap::new(),
            &topic_partition_counts,
            &|_tp| Some(100),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&metadata_response, true, now);
        let metadata_topic_ids1 = metadata.topic_ids();
        for topic in retain_topics.lock().unwrap().iter() {
            assert_eq!(
                metadata_topic_ids1.get(topic).copied(),
                topic_ids.get(topic).copied(),
                "Topic ID mismatch for {}",
                topic
            );
        }

        // Try removing the topic ID from validTopic1 (simulating receiving a request
        // from a controller with an older IBP)
        topic_ids.remove("validTopic1");
        let metadata_response = request_test_utils::metadata_update_with_ids(
            cluster_id,
            nodes,
            &HashMap::new(),
            &topic_partition_counts,
            &|_tp| Some(200),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&metadata_response, true, now);
        let metadata_topic_ids2 = metadata.topic_ids();
        for topic in retain_topics.lock().unwrap().iter() {
            assert_eq!(
                metadata_topic_ids2.get(topic).copied(),
                topic_ids.get(topic).copied(),
                "Topic ID mismatch for {}",
                topic
            );
        }

        let cluster = metadata.fetch();
        // We still have the topic, but it just doesn't have an ID.
        assert_eq!(
            ["validTopic1", "validTopic2"]
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
            cluster.topics().map(|s| s.to_string()).collect::<HashSet<_>>()
        );
        assert_eq!(2, cluster.partitions_for_topic("validTopic1").len());
        let cluster_topic_ids: HashSet<Uuid> = cluster.topic_ids().cloned().collect();
        let expected_topic_ids: HashSet<Uuid> = topic_ids.values().copied().collect();
        assert_eq!(expected_topic_ids, cluster_topic_ids);
        assert_eq!(Uuid::zero(), cluster.topic_id("validTopic1"));
    }

    /// Translated from `MetadataTest.testConcurrentUpdateAndFetchForSnapshotAndCluster`.
    ///
    /// Tests that concurrently updating Metadata, and fetching the corresponding
    /// MetadataSnapshot and Cluster work as expected, i.e. snapshot and cluster contain
    /// the relevant updates.
    #[test]
    fn test_concurrent_update_and_fetch_for_snapshot_and_cluster() {
        let now: i64 = 10000;
        let metadata = Arc::new(Metadata::new(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            ClusterResourceListeners::new(),
        ));

        // Setup metadata with 10 nodes, 2 topics, topic1 & 2, both to be retained in the update.
        // Both will have leader-epoch 100.
        let old_node_count = 10;
        let topic1 = "test_topic1";
        let topic2 = "test_topic2";
        let topic1_part0 = TopicPartition::new(topic1.to_string(), 0);
        let mut topic_partition_counts = HashMap::new();
        let old_partition_count = 1usize;
        topic_partition_counts.insert(topic1.to_string(), old_partition_count as i32);
        topic_partition_counts.insert(topic2.to_string(), old_partition_count as i32);
        let mut topic_ids = HashMap::new();
        topic_ids.insert(topic1.to_string(), Uuid::random_uuid());
        topic_ids.insert(topic2.to_string(), Uuid::random_uuid());
        let old_leader_epoch = 100;
        let metadata_response = request_test_utils::metadata_update_with_ids(
            "cluster",
            old_node_count,
            &HashMap::new(),
            &topic_partition_counts,
            &|_tp| Some(old_leader_epoch),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&metadata_response, true, now);
        let snapshot = metadata.fetch_metadata_snapshot();
        let cluster = metadata.fetch();
        // Validate metadata snapshot & cluster are setup as expected.
        assert_eq!(cluster.as_ref(), snapshot.cluster());
        assert_eq!(old_node_count as usize, snapshot.cluster().nodes().len());
        assert_eq!(Some(old_partition_count), snapshot.cluster().partition_count_for_topic(topic1));
        assert_eq!(Some(old_partition_count), snapshot.cluster().partition_count_for_topic(topic2));
        assert_eq!(Some(old_leader_epoch), snapshot.leader_epoch_for(&topic1_part0));

        // Setup 6 threads, where 3 are updating metadata & 3 are reading snapshot/cluster.
        // Metadata will be updated with higher # of nodes, partition-counts, leader-epoch.
        let num_threads = 6;
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));
        let at_least_updated = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let new_snapshot: Arc<Mutex<Option<Arc<MetadataSnapshot>>>> = Arc::new(Mutex::new(None));
        let new_cluster: Arc<Mutex<Option<Arc<Cluster>>>> = Arc::new(Mutex::new(None));

        let mut handles = Vec::new();
        for i in 0..num_threads {
            let id = i + 1;
            let metadata_clone = metadata.clone();
            let barrier_clone = barrier.clone();
            let at_least_updated_clone = at_least_updated.clone();
            let new_snapshot_clone = new_snapshot.clone();
            let new_cluster_clone = new_cluster.clone();
            let topic_ids_clone = topic_ids.clone();

            handles.push(std::thread::spawn(move || {
                barrier_clone.wait();
                if id % 2 == 0 {
                    // Thread to update metadata
                    let n_nodes = old_node_count + id as i32;
                    let mut new_topic_partition_counts = HashMap::new();
                    new_topic_partition_counts.insert(topic1.to_string(), old_partition_count as i32 + id as i32);
                    new_topic_partition_counts.insert(topic2.to_string(), old_partition_count as i32 + id as i32);
                    let new_metadata_response = request_test_utils::metadata_update_with_ids(
                        "clusterId",
                        n_nodes,
                        &HashMap::new(),
                        &new_topic_partition_counts,
                        &|_tp| Some(old_leader_epoch + id as i32),
                        &topic_ids_clone,
                    );
                    metadata_clone.update_with_current_request_version(&new_metadata_response, true, now);
                    at_least_updated_clone.store(true, Ordering::SeqCst);
                } else {
                    // Thread to read metadata snapshot, once it's updated
                    while !at_least_updated_clone.load(Ordering::SeqCst) {
                        std::thread::yield_now();
                    }
                    *new_snapshot_clone.lock().unwrap() = Some(metadata_clone.fetch_metadata_snapshot());
                    *new_cluster_clone.lock().unwrap() = Some(metadata_clone.fetch());
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Validate new snapshot is up-to-date. And has higher partition counts, nodes & leader epoch than earlier.
        {
            let snap = new_snapshot.lock().unwrap();
            let snap = snap.as_ref().unwrap();
            let new_node_count = snap.cluster().nodes().len();
            assert!(old_node_count as usize <= new_node_count, "Unexpected value {}", new_node_count);
            let new_partition_count_topic1 = snap.cluster().partition_count_for_topic(topic1).unwrap();
            assert!(
                old_partition_count <= new_partition_count_topic1,
                "Unexpected value {}",
                new_partition_count_topic1
            );
            let new_partition_count_topic2 = snap.cluster().partition_count_for_topic(topic2).unwrap();
            assert!(
                old_partition_count <= new_partition_count_topic2,
                "Unexpected value {}",
                new_partition_count_topic2
            );
            let new_leader_epoch = snap.leader_epoch_for(&topic1_part0).unwrap();
            assert!(old_leader_epoch <= new_leader_epoch, "Unexpected value {}", new_leader_epoch);
        }

        // Validate new cluster is up-to-date. And has higher partition counts, nodes than earlier.
        {
            let clust = new_cluster.lock().unwrap();
            let clust = clust.as_ref().unwrap();
            let new_node_count = clust.nodes().len();
            assert!(old_node_count as usize <= new_node_count, "Unexpected value {}", new_node_count);
            let new_partition_count_topic1 = clust.partition_count_for_topic(topic1).unwrap();
            assert!(
                old_partition_count <= new_partition_count_topic1,
                "Unexpected value {}",
                new_partition_count_topic1
            );
            let new_partition_count_topic2 = clust.partition_count_for_topic(topic2).unwrap();
            assert!(
                old_partition_count <= new_partition_count_topic2,
                "Unexpected value {}",
                new_partition_count_topic2
            );
        }
    }
}
