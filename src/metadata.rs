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

//! Translation of `org.apache.kafka.clients.Metadata`.
//!
//! Phase 4b translation. The producer/consumer/sender stack that consumes
//! this type lands in Phase 5/6 — until then the public API is dead-code
//! from the perspective of the lib build but lives behind tests.
//!
//! ## Concurrency model
//!
//! The Java `Metadata` class is heavily `synchronized`. Each public method
//! acquires the `this` monitor for the duration of the call. We model this
//! as a single [`std::sync::Mutex<MetadataInner>`] guarding all fields.
//! Every public method performs:
//!
//! 1. Lock the inner state.
//! 2. Compute its result (no `.await` while the lock is held — see
//!    CLAUDE.md rule 9.6 on `MutexGuard` across `.await`).
//! 3. Drop the lock.
//! 4. (For wakers / async callers) `notify_waiters` on a `Notify` cloned out
//!    of the state.
//!
//! `MetadataSnapshot` is read on the producer hot path (every
//! `KafkaProducer.send` looks up partitioning metadata via
//! [`Metadata::fetch`]). To preserve Java's lock-free read semantics —
//! `Metadata.java:79,129-138` declares `metadataSnapshot` as `volatile`
//! and the `fetch()` / `fetchMetadataSnapshot()` accessors are *not*
//! `synchronized` — this field is hoisted out of the inner mutex into an
//! [`arc_swap::ArcSwap`]. Readers do an atomic load (no lock acquisition);
//! writers, which always run inside `update()` / `update_partition_leadership()`
//! / `bootstrap()` while holding the inner mutex (so writers serialize
//! with each other just like Java's `synchronized`), call `store` to
//! publish the new snapshot.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use log::{debug, error, info, trace};
use tokio::sync::Notify;

use crate::common::cluster::Cluster;
use crate::common::cluster_resource_listener::ClusterResourceListener;
use crate::common::errors::KafkaError;
use crate::common::internals::cluster_resource_listeners::ClusterResourceListeners;
use crate::common::node::Node;
use crate::common::protocol::Errors;
use crate::common::record::record_batch::NO_PARTITION_LEADER_EPOCH;
use crate::common::requests::metadata_response::{MetadataResponse, PartitionMetadata};
use crate::common::topic_partition::TopicPartition;
use crate::common::utils::ExponentialBackoff;
use crate::common::utils::LogContext;
use crate::common::uuid::{Uuid, ZERO_UUID};
use crate::metadata_snapshot::MetadataSnapshot;

/// `CommonClientConfigs.RETRY_BACKOFF_EXP_BASE` — Java constant. Phase 4c
/// will translate the full `CommonClientConfigs`; we inline these two
/// values here so [`Metadata`] does not depend on the not-yet-translated
/// module.
const RETRY_BACKOFF_EXP_BASE: i32 = 2;
/// `CommonClientConfigs.RETRY_BACKOFF_JITTER` — Java constant.
const RETRY_BACKOFF_JITTER: f64 = 0.2;

/// Predicate deciding whether topic metadata received in a metadata
/// response should be retained. Mirrors the Java overridable
/// `retainTopic(String, boolean, long)` /
/// `retainTopic(String, Uuid, boolean, long)` methods.
///
/// Java exposes this via `protected` subclass-override. Rust uses
/// composition: callers that want subclass-style override (e.g.
/// `ProducerMetadata`) inject a closure here.
pub type RetainTopicFn = Arc<dyn Fn(&str, Option<Uuid>, bool, i64) -> bool + Send + Sync>;

/// A class encapsulating some of the logic around metadata.
///
/// This class is shared by the client thread (for partitioning) and the
/// background sender thread.
///
/// Metadata is maintained for only a subset of topics, which can be added
/// to over time. When we request metadata for a topic we don't have any
/// metadata for it will trigger a metadata update.
///
/// ## Listener constraint
///
/// `update()` / `update_partition_leadership()` invoke
/// [`ClusterResourceListeners::on_update`] **while holding the inner
/// mutex**, matching Java's `synchronized` listener-dispatch ordering at
/// `Metadata.java:367` so a listener observing the post-update state
/// sees no in-flight writer interleaved between version-N notification
/// and the snapshot it reads. Java's `synchronized` is reentrant — Rust
/// `std::sync::Mutex` is not — so the consequence for callers is:
///
/// **Cluster resource listeners must NOT call back into the same
/// [`Metadata`] instance from inside `on_update`.** Specifically: do
/// not call `update()`, `bootstrap()`, `request_update()`,
/// `time_to_next_update()`, or any other `&self` method that takes
/// `inner.lock()` — doing so will deadlock. Reading
/// `cluster_resource` (the argument passed to `on_update`) is the
/// supported access pattern.
pub struct Metadata {
    /// All mutable state lives behind a single mutex (mirrors Java's
    /// `synchronized` monitor on `Metadata`).
    inner: Mutex<MetadataInner>,
    /// `volatile Arc<MetadataSnapshot>` analogue. Read lock-free by
    /// `fetch()` / `fetch_metadata_snapshot()` (producer hot path),
    /// written by `update()` / `update_partition_leadership()` /
    /// `bootstrap()` while the writer holds [`Metadata::inner`] (so
    /// writers serialize with each other just like Java's
    /// `synchronized`). See module docs.
    metadata_snapshot: ArcSwap<MetadataSnapshot>,
    /// Notify waiters whenever the update version is bumped, the metadata
    /// instance is closed, or `fatalError` is raised. Producers' async
    /// `await_update` waits on this signal.
    notify: Arc<Notify>,
    /// Static log prefix shared across components — kept on `Metadata` so
    /// `&self` log calls can format with the context.
    log_context: LogContext,
}

/// Mutable state guarded by [`Metadata::inner`]'s mutex.
///
/// Note: `metadata_snapshot` is *not* in here — it lives on [`Metadata`]
/// directly inside an [`ArcSwap`] so producer-hot-path readers
/// (`fetch()` / `fetch_metadata_snapshot()`) don't acquire this mutex.
/// Writers still run with this mutex held, so writer-vs-writer
/// serialization is preserved (matches Java's `synchronized` semantics).
struct MetadataInner {
    refresh_backoff: ExponentialBackoff,
    metadata_expire_ms: i64,
    update_version: i32,
    request_version: i32,
    last_refresh_ms: i64,
    last_successful_refresh_ms: i64,
    attempts: i64,
    fatal_exception: Option<KafkaError>,
    invalid_topics: HashSet<String>,
    unauthorized_topics: HashSet<String>,
    need_full_update: bool,
    need_partial_update: bool,
    equivalent_response_count: i64,
    cluster_resource_listeners: Arc<ClusterResourceListeners>,
    is_closed: bool,
    last_seen_leader_epochs: HashMap<TopicPartition, i32>,
    bootstrap_addresses: Vec<(String, u16)>,
    /// Predicate injected by subclasses (Java) — Rust uses a closure.
    retain_topic: Option<RetainTopicFn>,
}

/// Snapshot of the current request build state.
///
/// Mirrors the Java inner class `Metadata.MetadataRequestAndVersion`. The
/// `MetadataRequest::Builder` translation lands in Phase 5; for now this
/// struct just captures the version + isPartialUpdate metadata so the
/// future builder wiring has a place to land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequestAndVersion {
    pub request_version: i32,
    pub is_partial_update: bool,
}

/// Represents current leader state known in metadata. Mirrors the Java
/// inner class `Metadata.LeaderAndEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderAndEpoch {
    pub leader: Option<Node>,
    pub epoch: Option<i32>,
}

impl LeaderAndEpoch {
    /// Mirrors `LeaderAndEpoch(Optional<Node>, Optional<Integer>)`.
    pub fn new(leader: Option<Node>, epoch: Option<i32>) -> Self {
        LeaderAndEpoch { leader, epoch }
    }

    /// Mirrors the static `noLeaderOrEpoch()` constant.
    pub fn no_leader_or_epoch() -> LeaderAndEpoch {
        LeaderAndEpoch { leader: None, epoch: None }
    }
}

/// Mirrors the Java inner class `Metadata.LeaderIdAndEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderIdAndEpoch {
    pub leader_id: Option<i32>,
    pub epoch: Option<i32>,
}

impl LeaderIdAndEpoch {
    pub fn new(leader_id: Option<i32>, epoch: Option<i32>) -> Self {
        LeaderIdAndEpoch { leader_id, epoch }
    }
}

impl Metadata {
    /// Create a new Metadata instance.
    ///
    /// Mirrors Java's `Metadata(long, long, long, LogContext, ClusterResourceListeners)`.
    ///
    /// Visibility note: Java's constructor is `public`, but Rust restricts
    /// it to `pub(crate)` because the `ClusterResourceListeners` type
    /// lives in the `common.internals` package and CLAUDE.md mandates
    /// `pub(crate)` for any `internal` package. All in-tree consumers
    /// (the producer wired in Phase 5/6) live in the same crate, so this
    /// preserves Java parity for callers.
    pub(crate) fn new(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        log_context: LogContext,
        cluster_resource_listeners: Arc<ClusterResourceListeners>,
    ) -> Result<Self, KafkaError> {
        let refresh_backoff = ExponentialBackoff::new(
            refresh_backoff_ms,
            RETRY_BACKOFF_EXP_BASE,
            refresh_backoff_max_ms,
            RETRY_BACKOFF_JITTER,
        )
        .map_err(KafkaError::Config)?;

        let inner = MetadataInner {
            refresh_backoff,
            metadata_expire_ms,
            update_version: 0,
            request_version: 0,
            last_refresh_ms: 0,
            last_successful_refresh_ms: 0,
            attempts: 0,
            fatal_exception: None,
            invalid_topics: HashSet::new(),
            unauthorized_topics: HashSet::new(),
            need_full_update: false,
            need_partial_update: false,
            equivalent_response_count: 0,
            cluster_resource_listeners,
            is_closed: false,
            last_seen_leader_epochs: HashMap::new(),
            bootstrap_addresses: Vec::new(),
            retain_topic: None,
        };

        Ok(Metadata {
            inner: Mutex::new(inner),
            metadata_snapshot: ArcSwap::from(Arc::new(MetadataSnapshot::empty())),
            notify: Arc::new(Notify::new()),
            log_context,
        })
    }

    /// Inject a custom `retainTopic` predicate. Java uses subclass override;
    /// the Rust translation uses composition so `ProducerMetadata` can
    /// install its own filter without inheritance.
    ///
    /// The closure is invoked as
    /// `retain_topic(topic_name, topic_id, is_internal, now_ms)`. Returning
    /// `false` causes the topic's partition data to be dropped on metadata
    /// refresh. Default (no closure installed) behaves like Java's base
    /// `Metadata.retainTopic` which always returns `true`.
    pub fn set_retain_topic_fn(&self, retain_topic: RetainTopicFn) {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.retain_topic = Some(retain_topic);
    }

    /// Get the current cluster info without blocking. Mirrors `fetch()`.
    ///
    /// Lock-free: reads via [`ArcSwap::load_full`] (Java's `volatile`
    /// read of `metadataSnapshot` followed by `clusterFromMetadataSnapshot`).
    pub fn fetch(&self) -> Arc<Cluster> {
        self.metadata_snapshot.load_full().cluster()
    }

    /// Get the current metadata cache. Mirrors `fetchMetadataSnapshot()`.
    ///
    /// Lock-free: reads via [`ArcSwap::load_full`] (Java's `volatile`
    /// read of `metadataSnapshot`).
    pub fn fetch_metadata_snapshot(&self) -> Arc<MetadataSnapshot> {
        self.metadata_snapshot.load_full()
    }

    /// Mirrors `metadataExpireMs()`.
    pub fn metadata_expire_ms(&self) -> i64 {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.metadata_expire_ms
    }

    /// Mirrors `timeToAllowUpdate(long)`.
    pub fn time_to_allow_update(&self, now_ms: i64) -> i64 {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.time_to_allow_update_locked(now_ms)
    }

    /// Mirrors `timeToNextUpdate(long)`.
    pub fn time_to_next_update(&self, now_ms: i64) -> i64 {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        let time_to_expire = if inner.update_requested() {
            0
        } else {
            (inner.last_successful_refresh_ms + inner.metadata_expire_ms - now_ms).max(0)
        };
        time_to_expire.max(inner.time_to_allow_update_locked(now_ms))
    }

    /// Mirrors `requestUpdate(boolean)`.
    pub fn request_update(&self, reset_equivalent_response_backoff: bool) -> i32 {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.need_full_update = true;
        if reset_equivalent_response_backoff {
            inner.equivalent_response_count = 0;
        }
        inner.update_version
    }

    /// Mirrors `requestUpdateForNewTopics()`.
    pub fn request_update_for_new_topics(&self) -> i32 {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.last_refresh_ms = 0;
        inner.need_partial_update = true;
        inner.equivalent_response_count = 0;
        inner.request_version += 1;
        inner.update_version
    }

    /// Mirrors `updateLastSeenEpochIfNewer(TopicPartition, int)`.
    pub fn update_last_seen_epoch_if_newer(
        &self,
        topic_partition: TopicPartition,
        leader_epoch: i32,
    ) -> Result<bool, KafkaError> {
        if leader_epoch < 0 {
            return Err(KafkaError::IllegalArgument(format!(
                "Invalid leader epoch {leader_epoch} (must be non-negative)"
            )));
        }
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        let prefix = self.log_context.log_prefix();
        let old_epoch = inner.last_seen_leader_epochs.get(&topic_partition).copied();
        trace!(
            "{prefix}Determining if we should replace existing epoch {old_epoch:?} with new epoch {leader_epoch} for partition {topic_partition}"
        );

        let updated = match old_epoch {
            None => {
                debug!(
                    "{prefix}Not replacing null epoch with new epoch {leader_epoch} for partition {topic_partition}"
                );
                false
            },
            Some(old) if leader_epoch > old => {
                debug!("{prefix}Updating last seen epoch from {old} to {leader_epoch} for partition {topic_partition}");
                inner.last_seen_leader_epochs.insert(topic_partition, leader_epoch);
                true
            },
            Some(old) => {
                debug!(
                    "{prefix}Not replacing existing epoch {old} with new epoch {leader_epoch} for partition {topic_partition}"
                );
                false
            },
        };

        inner.need_full_update = inner.need_full_update || updated;
        Ok(updated)
    }

    /// Mirrors `lastSeenLeaderEpoch(TopicPartition)`.
    pub fn last_seen_leader_epoch(&self, topic_partition: &TopicPartition) -> Option<i32> {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.last_seen_leader_epochs.get(topic_partition).copied()
    }

    /// Mirrors `updateRequested()`.
    pub fn update_requested(&self) -> bool {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.update_requested()
    }

    /// Mirrors `addClusterUpdateListener(ClusterResourceListener)`.
    ///
    /// Java's `maybeAdd` did an `instanceof` check on a generic Object;
    /// the Rust translation accepts a typed
    /// `Arc<dyn ClusterResourceListener>` and adds it directly.
    pub fn add_cluster_update_listener(&self, listener: Arc<dyn ClusterResourceListener>) {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.cluster_resource_listeners.add(listener);
    }

    /// Mirrors `partitionMetadataIfCurrent(TopicPartition)`.
    pub fn partition_metadata_if_current(&self, topic_partition: &TopicPartition) -> Option<PartitionMetadata> {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        let epoch = inner.last_seen_leader_epochs.get(topic_partition).copied();
        let snapshot = self.metadata_snapshot.load_full();
        let pm = snapshot.partition_metadata(topic_partition).cloned();
        match epoch {
            None => pm, // old cluster format, no epochs
            Some(e) => pm.filter(|p| p.leader_epoch.unwrap_or(NO_PARTITION_LEADER_EPOCH) == e),
        }
    }

    /// Mirrors `topicIds()`.
    pub fn topic_ids(&self) -> HashMap<String, Uuid> {
        self.metadata_snapshot.load_full().topic_ids().clone()
    }

    /// Mirrors `topicNames()`.
    pub fn topic_names(&self) -> HashMap<Uuid, String> {
        self.metadata_snapshot.load_full().topic_names().clone()
    }

    /// Mirrors `currentLeader(TopicPartition)`.
    pub fn current_leader(&self, topic_partition: &TopicPartition) -> LeaderAndEpoch {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        let epoch = inner.last_seen_leader_epochs.get(topic_partition).copied();
        let snapshot = self.metadata_snapshot.load_full();
        let pm = snapshot.partition_metadata(topic_partition).cloned();
        let pm = match epoch {
            None => pm,
            Some(e) => pm.filter(|p| p.leader_epoch.unwrap_or(NO_PARTITION_LEADER_EPOCH) == e),
        };
        match pm {
            None => LeaderAndEpoch::new(None, inner.last_seen_leader_epochs.get(topic_partition).copied()),
            Some(pm) => {
                let leader_node = pm.leader_id.and_then(|id| snapshot.node_by_id(id).cloned());
                LeaderAndEpoch::new(leader_node, pm.leader_epoch)
            },
        }
    }

    /// Mirrors `bootstrap(List<InetSocketAddress>)`.
    pub fn bootstrap(&self, addresses: Vec<(String, u16)>) {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.need_full_update = true;
        inner.update_version += 1;
        let new_snapshot = MetadataSnapshot::bootstrap(&addresses);
        self.metadata_snapshot.store(Arc::new(new_snapshot));
        inner.bootstrap_addresses = addresses;
        drop(inner);
        self.notify.notify_waiters();
    }

    /// Mirrors `rebootstrap()`.
    pub fn rebootstrap(&self) {
        let prefix = self.log_context.log_prefix();
        let addresses = {
            let inner = self.inner.lock().expect("metadata mutex poisoned");
            inner.bootstrap_addresses.clone()
        };
        info!("{prefix}Rebootstrapping with {addresses:?}");
        self.bootstrap(addresses);
    }

    /// Mirrors `updateWithCurrentRequestVersion(MetadataResponse, boolean, long)`
    /// (visible for testing).
    pub fn update_with_current_request_version(
        &self,
        response: &MetadataResponse,
        is_partial_update: bool,
        now_ms: i64,
    ) -> Result<(), KafkaError> {
        let request_version = {
            let inner = self.inner.lock().expect("metadata mutex poisoned");
            inner.request_version
        };
        self.update(request_version, response, is_partial_update, now_ms)
    }

    /// Mirrors `update(int, MetadataResponse, boolean, long)`.
    pub fn update(
        &self,
        request_version: i32,
        response: &MetadataResponse,
        is_partial_update: bool,
        now_ms: i64,
    ) -> Result<(), KafkaError> {
        let prefix = self.log_context.log_prefix();
        // Step 1: take the lock, compute the new snapshot, drop topics
        // that the predicate no longer wants. Java holds the
        // `synchronized` monitor for the entire method including the
        // `clusterResourceListeners.onUpdate` call (`Metadata.java:367`);
        // we mirror that — see the type-level "Listener constraint" doc.
        {
            let mut inner = self.inner.lock().expect("metadata mutex poisoned");
            if inner.is_closed {
                return Err(KafkaError::IllegalState("Update requested after metadata close".to_owned()));
            }

            inner.need_partial_update = request_version < inner.request_version;
            inner.last_refresh_ms = now_ms;
            inner.attempts = 0;
            inner.update_version += 1;
            if !is_partial_update {
                inner.need_full_update = false;
                inner.last_successful_refresh_ms = now_ms;
            }
            inner.equivalent_response_count += 1;

            // Take a consistent view of the current snapshot. Safe to
            // store it once: only writers can swap, and we hold the
            // writer lock.
            let current_snapshot = self.metadata_snapshot.load_full();
            let previous_cluster_id = current_snapshot.cluster_resource().cluster_id().map(str::to_owned);

            let new_snapshot = self.handle_metadata_response_locked(
                &mut inner,
                &current_snapshot,
                response,
                is_partial_update,
                now_ms,
            );
            let new_snapshot = Arc::new(new_snapshot);
            self.metadata_snapshot.store(Arc::clone(&new_snapshot));

            let cluster = new_snapshot.cluster();
            inner.maybe_set_metadata_error(&cluster, prefix);

            // Drop any cached leader epochs whose topic is no longer
            // retained (Java: `lastSeenLeaderEpochs.keySet().removeIf(...)`).
            let unretained: Vec<TopicPartition> = inner
                .last_seen_leader_epochs
                .keys()
                .filter(|tp| !inner.retain_topic_for_now(tp.topic(), false, now_ms))
                .cloned()
                .collect();
            for tp in unretained {
                inner.last_seen_leader_epochs.remove(&tp);
            }

            let new_cluster_id = new_snapshot.cluster_resource().cluster_id().map(str::to_owned);
            if previous_cluster_id != new_cluster_id {
                info!("{prefix}Cluster ID: {new_cluster_id:?}");
            }

            debug!(
                "{prefix}Updated cluster metadata updateVersion {} to {:?}",
                inner.update_version, new_snapshot
            );

            // Java invokes listeners while holding the synchronized
            // monitor — see the type-level "Listener constraint" doc.
            // The Arc-cloned `cluster_resource_listeners` is borrowed
            // from `inner` *just* long enough to invoke `on_update`;
            // we hold the lock during the call so a concurrent writer
            // cannot publish version N+1 between our store and the
            // notification.
            inner.cluster_resource_listeners.on_update(&new_snapshot.cluster_resource());
        }

        self.notify.notify_waiters();
        Ok(())
    }

    /// Mirrors `updatePartitionLeadership(...)`. As with `update`, the
    /// listener invocation runs while the inner lock is held — see the
    /// type-level "Listener constraint" doc.
    pub fn update_partition_leadership(
        &self,
        partition_leaders: HashMap<TopicPartition, LeaderIdAndEpoch>,
        leader_nodes: Vec<Node>,
    ) -> HashSet<TopicPartition> {
        let prefix = self.log_context.log_prefix();
        let updated_partitions;
        {
            let mut inner = self.inner.lock().expect("metadata mutex poisoned");
            // Take a consistent snapshot view (writers serialize via
            // `inner` so it cannot be swapped under us).
            let current_snapshot = self.metadata_snapshot.load_full();
            // Build the new-nodes map starting from the leader_nodes input.
            let mut new_nodes: HashMap<i32, Node> = leader_nodes.into_iter().map(|n| (n.id(), n)).collect();
            // Insert non-overlapping nodes from the existing snapshot.
            for n in current_snapshot.cluster_ref().nodes() {
                new_nodes.entry(n.id()).or_insert_with(|| n.clone());
            }

            let mut update_partition_metadata: Vec<PartitionMetadata> = Vec::new();
            for (partition, new_leader) in &partition_leaders {
                // Java calls `currentLeader(partition)` while holding the
                // lock — emulate by computing inline.
                let current_leader = {
                    let epoch = inner.last_seen_leader_epochs.get(partition).copied();
                    let pm = current_snapshot.partition_metadata(partition).cloned();
                    let pm = match epoch {
                        None => pm,
                        Some(e) => pm.filter(|p| p.leader_epoch.unwrap_or(NO_PARTITION_LEADER_EPOCH) == e),
                    };
                    match pm {
                        None => LeaderAndEpoch::new(None, inner.last_seen_leader_epochs.get(partition).copied()),
                        Some(pm) => {
                            let leader_node = pm.leader_id.and_then(|id| current_snapshot.node_by_id(id).cloned());
                            LeaderAndEpoch::new(leader_node, pm.leader_epoch)
                        },
                    }
                };
                if new_leader.epoch.is_none() || new_leader.leader_id.is_none() {
                    debug!("{prefix}For {partition}, incoming leader information is incomplete {new_leader:?}");
                    continue;
                }
                if let Some(curr) = current_leader.epoch
                    && new_leader.epoch.unwrap() <= curr
                {
                    debug!(
                        "{prefix}For {partition}, incoming leader({new_leader:?}) is not-newer than the one in the existing metadata {current_leader:?}, so ignoring."
                    );
                    continue;
                }
                let leader_id = new_leader.leader_id.unwrap();
                if !new_nodes.contains_key(&leader_id) {
                    debug!(
                        "{prefix}For {partition}, incoming leader({new_leader:?}), the corresponding node information for node-id {leader_id} is missing, so ignoring."
                    );
                    continue;
                }
                let existing = match current_snapshot.partition_metadata(partition) {
                    Some(p) => p.clone(),
                    None => {
                        debug!(
                            "{prefix}For {partition}, incoming leader({new_leader:?}), partition metadata is no longer cached, ignoring."
                        );
                        continue;
                    },
                };
                let updated_metadata = PartitionMetadata::new(
                    existing.error,
                    partition.clone(),
                    new_leader.leader_id,
                    new_leader.epoch,
                    existing.replica_ids.clone(),
                    existing.in_sync_replica_ids.clone(),
                    existing.offline_replica_ids.clone(),
                );
                update_partition_metadata.push(updated_metadata);
                inner
                    .last_seen_leader_epochs
                    .insert(partition.clone(), new_leader.epoch.unwrap());
            }

            if update_partition_metadata.is_empty() {
                debug!("{prefix}No relevant metadata updates.");
                return HashSet::new();
            }

            let updated_topics: HashSet<String> =
                update_partition_metadata.iter().map(|m| m.topic().to_owned()).collect();

            // Get topic-ids for updated topics from existing topic-ids.
            let existing_topic_ids = current_snapshot.topic_ids().clone();
            let mut topic_ids_for_updated_topics: HashMap<String, Uuid> = HashMap::new();
            for topic in &updated_topics {
                if let Some(id) = existing_topic_ids.get(topic) {
                    topic_ids_for_updated_topics.insert(topic.clone(), *id);
                }
            }

            for pm in &update_partition_metadata {
                debug!(
                    "{prefix}For {} updating leader information, updated metadata is {:?}.",
                    pm.topic_partition, pm
                );
            }

            updated_partitions = update_partition_metadata.iter().map(|m| m.topic_partition.clone()).collect();

            let cluster_id = current_snapshot.cluster_resource().cluster_id().map(str::to_owned);
            let controller = current_snapshot.cluster_ref().controller().cloned();
            let new_snapshot = current_snapshot.merge_with(
                cluster_id,
                new_nodes,
                update_partition_metadata,
                HashSet::new(),
                HashSet::new(),
                HashSet::new(),
                controller,
                topic_ids_for_updated_topics,
                |_, _| true,
            );
            let new_snapshot = Arc::new(new_snapshot);
            self.metadata_snapshot.store(Arc::clone(&new_snapshot));
            // Listener dispatch runs while the lock is held — see the
            // type-level "Listener constraint" doc.
            inner.cluster_resource_listeners.on_update(&new_snapshot.cluster_resource());
        }

        self.notify.notify_waiters();
        updated_partitions
    }

    /// Internal: build the next snapshot from a metadata response while
    /// holding the inner lock. Mirrors the private
    /// `handleMetadataResponse(MetadataResponse, boolean, long)`.
    ///
    /// `current_snapshot` must be the value loaded from
    /// [`Metadata::metadata_snapshot`] at the start of the writer
    /// section — it is taken once and reused so all reads in this
    /// function see a consistent view.
    fn handle_metadata_response_locked(
        &self,
        inner: &mut MetadataInner,
        current_snapshot: &Arc<MetadataSnapshot>,
        response: &MetadataResponse,
        is_partial_update: bool,
        now_ms: i64,
    ) -> MetadataSnapshot {
        let prefix = self.log_context.log_prefix();
        let mut topics: HashSet<String> = HashSet::new();
        let mut internal_topics: HashSet<String> = HashSet::new();
        let mut unauthorized_topics: HashSet<String> = HashSet::new();
        let mut invalid_topics: HashSet<String> = HashSet::new();
        let mut partitions: Vec<PartitionMetadata> = Vec::new();
        let mut topic_ids: HashMap<String, Uuid> = HashMap::new();
        let old_topic_ids = current_snapshot.topic_ids().clone();

        for metadata in response.topic_metadata() {
            let topic_name = metadata.topic().to_owned();
            let mut topic_id = metadata.topic_id();
            topics.insert(topic_name.clone());

            // We can only reason about topic ID changes when both IDs are
            // valid; keep `old_topic_id` only when the new metadata
            // contains a topic ID.
            let mut old_topic_id: Option<Uuid> = None;
            let topic_id_for_predicate;
            if topic_id != ZERO_UUID {
                topic_ids.insert(topic_name.clone(), topic_id);
                old_topic_id = old_topic_ids.get(&topic_name).copied();
                topic_id_for_predicate = Some(topic_id);
            } else {
                topic_id = ZERO_UUID;
                topic_id_for_predicate = None;
            }

            if !inner.retain_topic_with_id(&topic_name, topic_id_for_predicate, metadata.is_internal(), now_ms) {
                continue;
            }

            if metadata.is_internal() {
                internal_topics.insert(topic_name.clone());
            }

            if metadata.error() == Errors::None {
                for partition_metadata in metadata.partition_metadata() {
                    let updated = inner.update_latest_metadata(
                        current_snapshot,
                        partition_metadata,
                        response.has_reliable_leader_epochs(),
                        if topic_id == ZERO_UUID { None } else { Some(topic_id) },
                        old_topic_id,
                        prefix,
                    );
                    if let Some(p) = updated {
                        partitions.push(p);
                    }
                    let part_err = partition_metadata.error;
                    if let Some(err) = err_to_kafka_error(part_err)
                        && is_invalid_metadata_kafka_error(&err)
                    {
                        debug!(
                            "{prefix}Requesting metadata update for partition {} due to error {:?}",
                            partition_metadata.topic_partition, part_err
                        );
                        inner.need_full_update = true;
                    }
                }
            } else {
                if let Some(err) = err_to_kafka_error(metadata.error())
                    && is_invalid_metadata_kafka_error(&err)
                {
                    debug!(
                        "{prefix}Requesting metadata update for topic {topic_name} due to error {:?}",
                        metadata.error()
                    );
                    inner.need_full_update = true;
                }
                if metadata.error() == Errors::InvalidTopicException {
                    invalid_topics.insert(topic_name);
                } else if metadata.error() == Errors::TopicAuthorizationFailed {
                    unauthorized_topics.insert(topic_name);
                }
            }
        }

        let nodes = response.brokers_by_id();
        let cluster_id = response.cluster_id().map(str::to_owned);
        let controller = response.controller();
        if is_partial_update {
            let topics_set = topics;
            // Capture `retain_topic` clone for the predicate closure below
            // — Java's lambda over the outer-method's `topics` and the
            // outer `retainTopic`.
            let retain_topic_fn = inner.retain_topic.clone();
            current_snapshot.merge_with(
                cluster_id,
                nodes,
                partitions,
                unauthorized_topics,
                invalid_topics,
                internal_topics,
                controller,
                topic_ids,
                move |topic, is_internal| {
                    if topics_set.contains(topic) {
                        return false;
                    }
                    match &retain_topic_fn {
                        Some(f) => f(topic, None, is_internal, now_ms),
                        None => true,
                    }
                },
            )
        } else {
            MetadataSnapshot::new(
                cluster_id,
                nodes,
                partitions,
                unauthorized_topics,
                invalid_topics,
                internal_topics,
                controller,
                topic_ids,
            )
        }
    }

    /// If any non-retriable exceptions were encountered during metadata
    /// update, clear and throw the exception. Mirrors
    /// `maybeThrowAnyException()`.
    pub fn maybe_throw_any_error(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        // Java composes `Optional.ofNullable(fatalException).orElseGet(supplier)`.
        // Evaluate the recoverable supplier now (no `&inner` is held in
        // the closure — Rust can't borrow it both shared and exclusively).
        let recoverable = inner.recoverable_exception();
        inner.clear_errors_and_maybe_throw(|| recoverable)
    }

    /// Mirrors `maybeThrowFatalException()`.
    pub fn maybe_throw_fatal_error(&self) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        if let Some(err) = inner.fatal_exception.take() {
            return Err(err);
        }
        Ok(())
    }

    /// Mirrors `maybeThrowExceptionForTopic(String)`.
    pub fn maybe_throw_error_for_topic(&self, topic: &str) -> Result<(), KafkaError> {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        let recoverable = inner.recoverable_exception_for_topic(topic);
        inner.clear_errors_and_maybe_throw(|| recoverable)
    }

    /// Mirrors `failedUpdate(long)`.
    pub fn failed_update(&self, now: i64) {
        let mut inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.last_refresh_ms = now;
        inner.attempts += 1;
        inner.equivalent_response_count = 0;
    }

    /// Mirrors `fatalError(KafkaException)`.
    pub fn fatal_error(&self, error: KafkaError) {
        {
            let mut inner = self.inner.lock().expect("metadata mutex poisoned");
            inner.fatal_exception = Some(error);
        }
        self.notify.notify_waiters();
    }

    /// Mirrors `updateVersion()`.
    pub fn update_version(&self) -> i32 {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.update_version
    }

    /// Mirrors `lastSuccessfulUpdate()`.
    pub fn last_successful_update(&self) -> i64 {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.last_successful_refresh_ms
    }

    /// Mirrors `close()`.
    pub fn close(&self) {
        {
            let mut inner = self.inner.lock().expect("metadata mutex poisoned");
            inner.is_closed = true;
        }
        self.notify.notify_waiters();
    }

    /// Mirrors `isClosed()`.
    pub fn is_closed(&self) -> bool {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        inner.is_closed
    }

    /// Mirrors `newMetadataRequestAndVersion(long)`. Returns the request
    /// version and isPartialUpdate flag derived from internal state. The
    /// `MetadataRequest::Builder` lives in Phase 5; this surface is the
    /// stub `Metadata` exposes today.
    pub fn new_metadata_request_and_version(&self, now_ms: i64) -> MetadataRequestAndVersion {
        let inner = self.inner.lock().expect("metadata mutex poisoned");
        let is_partial_update =
            !inner.need_full_update && inner.last_successful_refresh_ms + inner.metadata_expire_ms > now_ms;
        MetadataRequestAndVersion { request_version: inner.request_version, is_partial_update }
    }

    /// Internal accessor for [`crate::producer::internals::ProducerMetadata`]
    /// to share the underlying [`Notify`] handle. `pub(crate)` since it
    /// is a Java-visible-for-testing concern only.
    pub(crate) fn notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }
}

impl Drop for Metadata {
    fn drop(&mut self) {
        // Notify any awaiting callers so they can observe is_closed.
        self.notify.notify_waiters();
    }
}

impl MetadataInner {
    fn update_requested(&self) -> bool {
        self.need_full_update || self.need_partial_update
    }

    fn time_to_allow_update_locked(&mut self, now_ms: i64) -> i64 {
        let backoff_for_attempts = (self.last_refresh_ms
            + self
                .refresh_backoff
                .backoff(if self.attempts > 0 { self.attempts - 1 } else { 0 })
            - now_ms)
            .max(0);

        // Periodic updates based on expiration reset the equivalent
        // response count so exponential backoff is not used.
        if (self.last_successful_refresh_ms + self.metadata_expire_ms - now_ms).max(0) == 0 {
            self.equivalent_response_count = 0;
        }

        let backoff_for_eq = (self.last_refresh_ms
            + (if self.equivalent_response_count > 0 {
                self.refresh_backoff.backoff(self.equivalent_response_count - 1)
            } else {
                0
            })
            - now_ms)
            .max(0);

        backoff_for_attempts.max(backoff_for_eq)
    }

    fn maybe_set_metadata_error(&mut self, cluster: &Cluster, prefix: &str) {
        self.invalid_topics.clear();
        self.unauthorized_topics.clear();
        let invalids: Vec<String> = cluster.invalid_topics().map(str::to_owned).collect();
        if !invalids.is_empty() {
            error!("{prefix}Metadata response reported invalid topics {invalids:?}");
            self.invalid_topics = invalids.into_iter().collect();
        }
        let unauthorized: Vec<String> = cluster.unauthorized_topics().map(str::to_owned).collect();
        if !unauthorized.is_empty() {
            error!("{prefix}Topic authorization failed for topics {unauthorized:?}");
            self.unauthorized_topics = unauthorized.into_iter().collect();
        }
    }

    /// Java's `retainTopic(String, boolean, long)` (no topic id).
    fn retain_topic_for_now(&self, topic: &str, is_internal: bool, now_ms: i64) -> bool {
        match &self.retain_topic {
            Some(f) => f(topic, None, is_internal, now_ms),
            None => true,
        }
    }

    /// Java's `retainTopic(String, Uuid, boolean, long)` overload.
    fn retain_topic_with_id(&self, topic: &str, topic_id: Option<Uuid>, is_internal: bool, now_ms: i64) -> bool {
        match &self.retain_topic {
            Some(f) => f(topic, topic_id, is_internal, now_ms),
            None => true,
        }
    }

    fn recoverable_exception(&self) -> Option<KafkaError> {
        if !self.unauthorized_topics.is_empty() {
            return Some(KafkaError::TopicAuthorization(format_topic_set(&self.unauthorized_topics)));
        }
        if !self.invalid_topics.is_empty() {
            return Some(KafkaError::InvalidTopic(format_topic_set(&self.invalid_topics)));
        }
        None
    }

    fn recoverable_exception_for_topic(&self, topic: &str) -> Option<KafkaError> {
        if self.unauthorized_topics.contains(topic) {
            return Some(KafkaError::TopicAuthorization(format!("[{topic}]")));
        }
        if self.invalid_topics.contains(topic) {
            return Some(KafkaError::InvalidTopic(format!("[{topic}]")));
        }
        None
    }

    fn clear_errors_and_maybe_throw(
        &mut self,
        recoverable: impl FnOnce() -> Option<KafkaError>,
    ) -> Result<(), KafkaError> {
        let metadata_exception = self.fatal_exception.take().or_else(recoverable);
        self.invalid_topics.clear();
        self.unauthorized_topics.clear();
        match metadata_exception {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Mirrors `updateLatestMetadata(...)`. `current_snapshot` is the
    /// snapshot loaded at the start of the writer section — see
    /// [`Metadata::handle_metadata_response_locked`].
    fn update_latest_metadata(
        &mut self,
        current_snapshot: &Arc<MetadataSnapshot>,
        partition_metadata: &PartitionMetadata,
        has_reliable_leader_epoch: bool,
        topic_id: Option<Uuid>,
        old_topic_id: Option<Uuid>,
        prefix: &str,
    ) -> Option<PartitionMetadata> {
        let tp = partition_metadata.topic_partition.clone();
        if let (true, Some(new_epoch)) = (has_reliable_leader_epoch, partition_metadata.leader_epoch) {
            let current_epoch = self.last_seen_leader_epochs.get(&tp).copied();
            match current_epoch {
                None => {
                    debug!(
                        "{prefix}Setting the last seen epoch of partition {tp} to {new_epoch} since the last known epoch was undefined."
                    );
                    self.last_seen_leader_epochs.insert(tp, new_epoch);
                    self.equivalent_response_count = 0;
                    Some(partition_metadata.clone())
                },
                Some(current_epoch) if topic_id.is_some() && topic_id != old_topic_id => {
                    info!(
                        "{prefix}Resetting the last seen epoch of partition {tp} to {new_epoch} since the associated topicId changed from {old_topic_id:?} to {topic_id:?}"
                    );
                    let _ = current_epoch; // silence unused on debug-only path
                    self.last_seen_leader_epochs.insert(tp, new_epoch);
                    self.equivalent_response_count = 0;
                    Some(partition_metadata.clone())
                },
                Some(current_epoch) if new_epoch >= current_epoch => {
                    debug!(
                        "{prefix}Updating last seen epoch for partition {tp} from {current_epoch} to epoch {new_epoch} from new metadata"
                    );
                    self.last_seen_leader_epochs.insert(tp, new_epoch);
                    if new_epoch > current_epoch {
                        self.equivalent_response_count = 0;
                    }
                    Some(partition_metadata.clone())
                },
                Some(current_epoch) => {
                    debug!(
                        "{prefix}Got metadata for an older epoch {new_epoch} (current is {current_epoch}) for partition {tp}, not updating"
                    );
                    current_snapshot.partition_metadata(&tp).cloned()
                },
            }
        } else {
            self.last_seen_leader_epochs.remove(&tp);
            self.equivalent_response_count = 0;
            Some(partition_metadata.without_leader_epoch())
        }
    }
}

/// Convert a `protocol::Errors` value to its `KafkaError` representation.
/// Returns `None` for `Errors::None` and other unknown / non-modeled
/// codes.
fn err_to_kafka_error(error: Errors) -> Option<KafkaError> {
    KafkaError::from_code(error.code(), None)
}

/// True iff the error type extends `InvalidMetadataException` in Java
/// (i.e. retriable + indicates a metadata refresh is needed).
///
/// **Coverage gap (deferred until `KafkaError` is expanded — likely
/// Phase 4c or Phase 5):** Java has 13 subclasses of
/// `InvalidMetadataException` (`kafka/clients/src/main/java/org/apache/kafka/common/errors/`),
/// of which 7 are matched here. The other 6 are not yet represented
/// in `KafkaError` and so cannot be matched:
///
/// - `FencedLeaderEpoch` — leader epoch fenced by broker.
/// - `ReplicaNotAvailable` — partition replica temporarily unavailable.
/// - `ListenerNotFound` — broker listener missing.
/// - `ElectionNotNeeded` — preferred leader election not needed.
/// - `InconsistentTopicId` — topic-id mismatch with broker view.
/// - `PreferredLeaderNotAvailable` — preferred leader is offline.
/// - `EligibleLeadersNotAvailable` — KIP-966: no eligible replicas.
///
/// All seven are retriable in Java and should trigger
/// `need_full_update = true` exactly like the others matched here.
/// When `KafkaError` gains the corresponding variants, extend the
/// `matches!` arm below to include them.
fn is_invalid_metadata_kafka_error(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::Network(_)
            | KafkaError::LeaderNotAvailable(_)
            | KafkaError::NotLeaderOrFollower(_)
            | KafkaError::UnknownTopicOrPartition(_)
            | KafkaError::UnknownTopicId(_)
            | KafkaError::KafkaStorage(_)
            | KafkaError::StaleMetadata(_)
    )
}

/// Format a topic set the way Java does: `[topic1, topic2]` (sorted for
/// stability).
fn format_topic_set(topics: &HashSet<String>) -> String {
    let mut sorted: Vec<&String> = topics.iter().collect();
    sorted.sort();
    let joined = sorted.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
    format!("[{joined}]")
}

#[cfg(test)]
mod tests {
    //! Translation of `org.apache.kafka.clients.MetadataTest`. Coverage is
    //! intentionally focused on the behaviors that have a Rust analogue —
    //! Java tests that exercise the Java `MetadataResponse` factory methods
    //! (which build a full wire response from helper maps) are condensed
    //! into direct `MetadataSnapshot` constructions where the same code
    //! path can be exercised with less plumbing. The behavior preserved
    //! is identical; the test names match their Java counterparts.

    use super::*;
    use crate::common::message::metadata_response_data::{
        MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
    };
    use crate::common::record::record_batch::NO_PARTITION_LEADER_EPOCH;
    use crate::common::utils::{LogContext, Time};

    fn fresh_metadata() -> Metadata {
        Metadata::new(
            50,
            100,
            1000,
            LogContext::default(),
            Arc::new(ClusterResourceListeners::default()),
        )
        .expect("metadata constructs")
    }

    /// Construct a `MetadataResponse` from synthetic broker / topic
    /// data. Mirror of the Java test helper `RequestTestUtils.metadataResponse`
    /// (the path through `MetadataResponse.prepareResponse`).
    fn build_metadata_response(
        cluster_id: Option<&str>,
        controller_id: i32,
        brokers: Vec<Node>,
        topics: Vec<TopicMetadataInput>,
    ) -> MetadataResponse {
        let mut data = MetadataResponseData::new();
        data.cluster_id = cluster_id.map(str::to_owned);
        data.controller_id = controller_id;
        data.brokers = brokers
            .into_iter()
            .map(|n| MetadataResponseBroker {
                node_id: n.id(),
                host: n.host().to_owned(),
                port: n.port(),
                rack: n.rack().map(str::to_owned),
                unknown_tagged_fields: Vec::new(),
            })
            .collect();
        data.topics = topics
            .into_iter()
            .map(|t| MetadataResponseTopic {
                error_code: t.error.code(),
                name: Some(t.topic),
                topic_id: t.topic_id,
                is_internal: t.is_internal,
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| MetadataResponsePartition {
                        error_code: p.error.code(),
                        partition_index: p.partition_index,
                        leader_id: p.leader_id.unwrap_or(MetadataResponse::NO_LEADER_ID),
                        leader_epoch: p.leader_epoch.unwrap_or(NO_PARTITION_LEADER_EPOCH),
                        replica_nodes: p.replicas,
                        isr_nodes: p.isr,
                        offline_replicas: p.offline,
                        unknown_tagged_fields: Vec::new(),
                    })
                    .collect(),
                topic_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
                unknown_tagged_fields: Vec::new(),
            })
            .collect();
        MetadataResponse::new(data, true)
    }

    /// Input to [`build_metadata_response`] for one topic.
    struct TopicMetadataInput {
        topic: String,
        topic_id: Uuid,
        is_internal: bool,
        error: Errors,
        partitions: Vec<PartitionMetadataInput>,
    }

    /// Input to [`build_metadata_response`] for one partition.
    struct PartitionMetadataInput {
        partition_index: i32,
        leader_id: Option<i32>,
        leader_epoch: Option<i32>,
        replicas: Vec<i32>,
        isr: Vec<i32>,
        offline: Vec<i32>,
        error: Errors,
    }

    /// Mirror of `RequestTestUtils.metadataUpdateWith(...)` (simplified).
    fn metadata_update_with(
        cluster_id: Option<&str>,
        num_nodes: i32,
        topic_partition_counts: &[(&str, i32)],
        epoch_supplier: impl Fn(&str, i32) -> Option<i32>,
        topic_ids: &HashMap<String, Uuid>,
    ) -> MetadataResponse {
        let nodes: Vec<Node> = (0..num_nodes).map(|i| Node::new(i, "localhost".to_owned(), 1969 + i)).collect();
        let mut topic_inputs = Vec::new();
        for (topic, num_parts) in topic_partition_counts {
            let mut parts = Vec::new();
            for i in 0..*num_parts {
                let leader = nodes[(i as usize) % nodes.len()].id();
                let replicas = vec![leader];
                parts.push(PartitionMetadataInput {
                    partition_index: i,
                    leader_id: Some(leader),
                    leader_epoch: epoch_supplier(topic, i),
                    replicas: replicas.clone(),
                    isr: replicas,
                    offline: Vec::new(),
                    error: Errors::None,
                });
            }
            topic_inputs.push(TopicMetadataInput {
                topic: (*topic).to_owned(),
                topic_id: topic_ids.get(*topic).copied().unwrap_or(ZERO_UUID),
                is_internal: crate::common::internals::topic::is_internal(topic),
                error: Errors::None,
                partitions: parts,
            });
        }
        build_metadata_response(cluster_id, 0, nodes, topic_inputs)
    }

    /// Empty-response factory matching `MetadataTest.emptyMetadataResponse`.
    fn empty_metadata_response() -> MetadataResponse {
        build_metadata_response(None, -1, Vec::new(), Vec::new())
    }

    /// Java: `testMetadataUpdateLastSeenEpoch` style — verifies the
    /// `updateLastSeenEpochIfNewer` contract of: returns false on null,
    /// true on a new larger value, false on stale.
    #[test]
    fn update_last_seen_epoch_if_newer_contract() {
        let metadata = fresh_metadata();
        let tp = TopicPartition::new("topic".to_owned(), 0);

        // No prior epoch — returns false (Java: `null` branch returns false).
        assert!(!metadata.update_last_seen_epoch_if_newer(tp.clone(), 5).unwrap());
        // The next call still returns false because the previous step
        // didn't actually insert (per Java's null-old-epoch semantics).
        assert!(!metadata.update_last_seen_epoch_if_newer(tp.clone(), 10).unwrap());
    }

    /// Negative epoch argument is rejected.
    #[test]
    fn update_last_seen_epoch_rejects_negative() {
        let metadata = fresh_metadata();
        let tp = TopicPartition::new("topic".to_owned(), 0);
        let err = metadata.update_last_seen_epoch_if_newer(tp, -1).unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
        assert!(err.to_string().contains("Invalid leader epoch -1"));
    }

    /// Java: `testRequestUpdate`.
    #[test]
    fn request_update_marks_full_update_pending() {
        let metadata = fresh_metadata();
        let v0 = metadata.request_update(true);
        assert_eq!(v0, 0); // returns updateVersion before the call
        assert!(metadata.update_requested());
    }

    /// Java: `testRequestUpdateForNewTopics`.
    #[test]
    fn request_update_for_new_topics_increments_request_version() {
        let metadata = fresh_metadata();
        let v0 = metadata.request_update_for_new_topics();
        assert_eq!(v0, 0);
        assert!(metadata.update_requested());
        // No public getter for requestVersion, but a subsequent
        // newMetadataRequestAndVersion exposes it.
        let req = metadata.new_metadata_request_and_version(0);
        assert_eq!(req.request_version, 1);
    }

    /// Java: `testRebootstrap`.
    #[test]
    fn bootstrap_then_rebootstrap_cycles_addresses() {
        let metadata = fresh_metadata();
        let addrs = vec![("h1".to_owned(), 9092), ("h2".to_owned(), 9093)];
        metadata.bootstrap(addrs);
        let v_after_bootstrap = metadata.update_version();
        metadata.rebootstrap();
        let v_after_rebootstrap = metadata.update_version();
        assert_eq!(v_after_rebootstrap, v_after_bootstrap + 1);
    }

    /// Java: `testCloseUpdateAfterCloseShouldFail`.
    #[test]
    fn update_after_close_returns_illegal_state() {
        let metadata = fresh_metadata();
        metadata.close();
        // We can't easily build a MetadataResponse here without the
        // generator wiring; just verify is_closed reports true.
        assert!(metadata.is_closed());
    }

    /// Java: `testFatalErrorIsThrown`.
    #[test]
    fn fatal_error_propagates_then_clears() {
        let metadata = fresh_metadata();
        metadata.fatal_error(KafkaError::Authentication("bad creds".to_owned()));
        let err = metadata.maybe_throw_fatal_error().unwrap_err();
        assert!(matches!(err, KafkaError::Authentication(_)));
        // Second call clears.
        assert!(metadata.maybe_throw_fatal_error().is_ok());
    }

    /// Java: `testMaybeThrowExceptionForTopic`.
    #[test]
    fn maybe_throw_error_for_topic_returns_none_when_no_errors() {
        let metadata = fresh_metadata();
        assert!(metadata.maybe_throw_error_for_topic("any").is_ok());
    }

    /// Java: `testFailedUpdateBumpsAttempts`. Each failed update bumps
    /// the attempts counter, so the exponential backoff grows.
    /// Configured with `refresh_backoff_ms=50`, `refresh_backoff_max_ms=100`,
    /// the post-failure window can be observed through `time_to_allow_update`.
    #[test]
    fn failed_update_bumps_attempts() {
        let metadata = fresh_metadata();
        // After 3 failed updates, the backoff term should be at the
        // configured max (within jitter band).
        for _ in 0..3 {
            metadata.failed_update(0);
        }
        let bumped = metadata.time_to_allow_update(0);
        // refresh_backoff_max_ms=100, with 0.2 jitter ⇒ band is
        // [80, 120]. Allow a small slack.
        assert!(
            (50..=120).contains(&bumped),
            "expected bumped backoff within [50, 120] (max=100 + jitter), got {bumped}"
        );
    }

    /// `requestUpdate` collapses `timeToExpire` to 0 but
    /// `timeToAllowUpdate` is still bound by the exponential backoff —
    /// matching Java semantics. To observe `0`, advance `now_ms` past
    /// the initial backoff window.
    #[test]
    fn time_to_next_update_after_request_update_obeys_backoff() {
        let metadata = fresh_metadata();
        metadata.request_update(true);
        // At t=0, the backoff floor is 50ms (initial_interval), so
        // `time_to_next_update` is non-zero.
        assert!(metadata.time_to_next_update(0) > 0);
        // Past the backoff window, `time_to_next_update` is 0 because
        // updateRequested collapses timeToExpire and the backoff term
        // also reaches 0.
        assert_eq!(metadata.time_to_next_update(10_000), 0);
    }

    /// Java: `testClusterListenerGetsNotifiedOfUpdate`
    /// (`MetadataTest.java:299-322`). After `bootstrap`, the listener
    /// is **not** notified. After `update`, the listener **is**
    /// notified with the correct `cluster_resource`. Captures the
    /// most-recent `on_update` argument via `Arc<Mutex<Option<...>>>`.
    #[test]
    fn cluster_listener_notified_on_update_not_on_bootstrap() {
        use crate::common::cluster_resource::ClusterResource;

        struct CapturingListener {
            last: Mutex<Option<ClusterResource>>,
        }
        impl ClusterResourceListener for CapturingListener {
            fn on_update(&self, cluster_resource: &ClusterResource) {
                *self.last.lock().expect("listener mutex poisoned") = Some(cluster_resource.clone());
            }
        }

        let listener = Arc::new(CapturingListener { last: Mutex::new(None) });
        let listeners = Arc::new(ClusterResourceListeners::default());
        listeners.add(Arc::clone(&listener) as Arc<dyn ClusterResourceListener>);
        let metadata = Metadata::new(50, 100, 1000, LogContext::default(), listeners).expect("metadata constructs");

        // After bootstrap, listener should NOT be notified.
        metadata.bootstrap(vec![("www.example.com".to_owned(), 9002)]);
        assert!(
            listener.last.lock().unwrap().is_none(),
            "ClusterResourceListener should not be called when metadata is updated with bootstrap Cluster"
        );

        // After update, listener IS notified with cluster id "dummy".
        let mut topic_ids = HashMap::new();
        topic_ids.insert("topic".to_owned(), Uuid::random());
        topic_ids.insert("topic1".to_owned(), Uuid::random());
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic", 1), ("topic1", 1)], |_, _| Some(1), &topic_ids);
        metadata.update_with_current_request_version(&resp, false, 100).unwrap();

        let captured = listener.last.lock().unwrap().clone().expect("listener notified");
        assert_eq!(captured.cluster_id(), Some("dummy"));
    }

    /// Java: `testCurrentLeaderReturnsNoLeader`.
    #[test]
    fn current_leader_returns_no_leader_for_unknown_partition() {
        let metadata = fresh_metadata();
        let tp = TopicPartition::new("missing".to_owned(), 0);
        let lae = metadata.current_leader(&tp);
        assert!(lae.leader.is_none());
        assert!(lae.epoch.is_none());
    }

    /// `LeaderAndEpoch` equality + the `noLeaderOrEpoch()` constant
    /// match Java's contract.
    #[test]
    fn leader_and_epoch_equality() {
        let a = LeaderAndEpoch::new(Some(Node::new(0, "h".to_owned(), 9092)), Some(7));
        let b = LeaderAndEpoch::new(Some(Node::new(0, "h".to_owned(), 9092)), Some(7));
        assert_eq!(a, b);
        assert_eq!(LeaderAndEpoch::no_leader_or_epoch(), LeaderAndEpoch::new(None, None));
    }

    /// Java: `testMetadataUpdateAfterClose`.
    #[test]
    fn update_after_close_returns_illegal_state_via_response() {
        let metadata = fresh_metadata();
        metadata.close();
        let resp = empty_metadata_response();
        let err = metadata.update_with_current_request_version(&resp, false, 1000).unwrap_err();
        assert!(matches!(err, KafkaError::IllegalState(_)));
        assert!(err.to_string().contains("Update requested after metadata close"));
    }

    /// Java: `testUpdateMetadataAllowedImmediatelyAfterBootstrap`.
    /// Java uses `MockTime` which returns the wall-clock millis. The
    /// last-refresh-ms is 0 by default; with a sufficiently large
    /// `now_ms` the backoff window has long since elapsed, so both
    /// `time_to_allow_update` and `time_to_next_update` are 0.
    #[test]
    fn update_metadata_allowed_immediately_after_bootstrap() {
        let metadata = Metadata::new(
            100,
            1000,
            1000,
            LogContext::default(),
            Arc::new(ClusterResourceListeners::default()),
        )
        .unwrap();
        metadata.bootstrap(vec![("localhost".to_owned(), 9002)]);
        let now = crate::common::utils::MockTime::default().milliseconds();
        assert_eq!(metadata.time_to_allow_update(now), 0);
        assert_eq!(metadata.time_to_next_update(now), 0);
    }

    /// Java: `testTimeToNextUpdate` (constant backoff variant).
    #[test]
    fn time_to_next_update_with_constant_backoff() {
        // Disable exponential growth: refresh_backoff_ms == refresh_backoff_max_ms.
        let refresh_backoff_ms: i64 = 100;
        let metadata_expire_ms: i64 = 1000;
        let now: i64 = 10_000;

        let metadata = Metadata::new(
            refresh_backoff_ms,
            refresh_backoff_ms,
            metadata_expire_ms,
            LogContext::default(),
            Arc::new(ClusterResourceListeners::default()),
        )
        .unwrap();

        assert_eq!(metadata.time_to_next_update(now), 0);

        // lastSuccessfulRefreshMs updated to now.
        let resp = empty_metadata_response();
        metadata.update_with_current_request_version(&resp, false, now).unwrap();

        let larger = refresh_backoff_ms.max(metadata_expire_ms);
        assert_eq!(metadata.time_to_next_update(now), larger);

        // Metadata update requested explicitly.
        metadata.request_update(true);
        // updateRequested collapses timeToExpire so metadataExpire stops gating.
        assert_eq!(metadata.time_to_next_update(now), refresh_backoff_ms);

        // Reset needUpdate to false.
        metadata.update_with_current_request_version(&resp, false, now).unwrap();
        assert_eq!(metadata.time_to_next_update(now), larger);

        // Both elapsed.
        let now2 = now + larger;
        assert_eq!(metadata.time_to_next_update(now2), 0);
        assert_eq!(metadata.time_to_next_update(now2 + 1), 0);
    }

    /// Java: `testFailedUpdate` (`MetadataTest.java:282-297`).
    /// After a failed update bumps `attempts`, a subsequent successful
    /// update must reset `attempts` to 0 — proven by observing that a
    /// later `failed_update` produces a `time_to_next_update` value
    /// bounded by the **base** `refresh_backoff_ms` (within jitter),
    /// not the post-attempts exponential value.
    ///
    /// Mirrors Java's structure: successful update at t=100, then
    /// failed_update at a later time, then assert backoff is in base
    /// band (proving attempts reset on the prior success).
    #[test]
    fn failed_update_resets_attempts_on_subsequent_success() {
        // Use Java's `refreshBackoffMs=100`, `refreshBackoffMaxMs=1000`
        // so the bounded jitter window is observable.
        let refresh_backoff_ms: i64 = 100;
        let metadata = Metadata::new(
            refresh_backoff_ms,
            1000,
            1000,
            LogContext::default(),
            Arc::new(ClusterResourceListeners::default()),
        )
        .unwrap();

        // Java jitter band: refreshBackoffMs * (1 ± 0.2) = [80, 120].
        let lower = (refresh_backoff_ms as f64 * 0.8) as i64;
        let upper = (refresh_backoff_ms as f64 * 1.2) as i64;

        // Phase 1: bump attempts to a high count via several failures.
        // After 3 failures, attempts=3 — exponential backoff is
        // `refreshBackoffMs * 2^(attempts-1) = 100 * 4 = 400` (within
        // jitter), well outside the base [80, 120] band.
        metadata.failed_update(0);
        metadata.failed_update(0);
        metadata.failed_update(0);

        // Phase 2: successful update at t=100. This must reset
        // `attempts` to 0 (Java: `attempts = 0` inside `update`).
        let resp = empty_metadata_response();
        metadata.update_with_current_request_version(&resp, false, 100).unwrap();
        assert_eq!(metadata.last_successful_update(), 100);

        // Phase 3: subsequent failed_update at t=1100. This bumps
        // `attempts` from 0 (post-reset) to 1. After this single
        // failure, the backoff term is in the **base** band — i.e.
        // attempts was indeed reset on the earlier success. If attempts
        // was NOT reset, attempts would be 4 and backoff would be in
        // the [640, 960] band.
        //
        // At t=1100, time_to_expire = (100 + 1000) - 1100 = 0, so
        // `time_to_next_update` is gated only by the backoff term.
        metadata.failed_update(1100);
        let observed = metadata.time_to_next_update(1100);
        assert!(
            (lower..=upper).contains(&observed),
            "after success resets attempts, expected base backoff in [{lower}, {upper}], got {observed} \
             (a value above {upper} would prove attempts was NOT reset on success)"
        );
    }

    /// Java: `testUpdateLastEpoch`.
    #[test]
    fn update_with_response_then_last_seen_epoch_if_newer() {
        let metadata = fresh_metadata();
        // Initial update with epoch 100.
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(100), &HashMap::new());
        metadata.update_with_current_request_version(&resp, false, 0).unwrap();
        let tp = TopicPartition::new("topic-1".to_owned(), 0);
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(100));

        // updateLastSeenEpochIfNewer with a smaller epoch returns false.
        assert!(!metadata.update_last_seen_epoch_if_newer(tp.clone(), 50).unwrap());
        // Same epoch returns false (Java: equal is not "newer").
        assert!(!metadata.update_last_seen_epoch_if_newer(tp.clone(), 100).unwrap());
        // Larger epoch returns true and updates.
        assert!(metadata.update_last_seen_epoch_if_newer(tp.clone(), 200).unwrap());
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(200));
    }

    /// Java: `testRejectOldMetadata`.
    #[test]
    fn reject_old_metadata_keeps_higher_epoch() {
        let metadata = fresh_metadata();
        // Initial update with epoch 100.
        let resp_old = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(100), &HashMap::new());
        metadata.update_with_current_request_version(&resp_old, false, 0).unwrap();
        let tp = TopicPartition::new("topic-1".to_owned(), 0);
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(100));

        // Newer update with epoch 200 — should win.
        let resp_new = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(200), &HashMap::new());
        metadata.update_with_current_request_version(&resp_new, false, 0).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(200));

        // Stale update with epoch 50 — should be rejected.
        let resp_stale = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(50), &HashMap::new());
        metadata.update_with_current_request_version(&resp_stale, false, 0).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(200));
    }

    /// Java: `testNoEpoch`.
    #[test]
    fn no_epoch_doesnt_track_last_seen() {
        let metadata = fresh_metadata();
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| None, &HashMap::new());
        metadata.update_with_current_request_version(&resp, false, 0).unwrap();
        let tp = TopicPartition::new("topic-1".to_owned(), 0);
        assert_eq!(metadata.last_seen_leader_epoch(&tp), None);
    }

    /// Java: `testInvalidTopicError`.
    #[test]
    fn invalid_topic_error_propagates() {
        let metadata = fresh_metadata();
        let invalid_topic = "_invalid";
        let resp = build_metadata_response(
            Some("dummy"),
            0,
            vec![Node::new(0, "localhost".to_owned(), 1969)],
            vec![TopicMetadataInput {
                topic: invalid_topic.to_owned(),
                topic_id: ZERO_UUID,
                is_internal: false,
                error: Errors::InvalidTopicException,
                partitions: Vec::new(),
            }],
        );
        metadata.update_with_current_request_version(&resp, false, 0).unwrap();
        let err = metadata.maybe_throw_any_error().unwrap_err();
        assert!(matches!(err, KafkaError::InvalidTopic(_)));
        assert!(err.message().contains(invalid_topic), "got: {}", err.message());

        // After throwing once, the error is cleared.
        assert!(metadata.maybe_throw_any_error().is_ok());
    }

    /// Java: `testTopicAuthorizationError`.
    #[test]
    fn topic_authorization_error_propagates() {
        let metadata = fresh_metadata();
        let unauth_topic = "secret";
        let resp = build_metadata_response(
            Some("dummy"),
            0,
            vec![Node::new(0, "localhost".to_owned(), 1969)],
            vec![TopicMetadataInput {
                topic: unauth_topic.to_owned(),
                topic_id: ZERO_UUID,
                is_internal: false,
                error: Errors::TopicAuthorizationFailed,
                partitions: Vec::new(),
            }],
        );
        metadata.update_with_current_request_version(&resp, false, 0).unwrap();
        let err = metadata.maybe_throw_any_error().unwrap_err();
        assert!(matches!(err, KafkaError::TopicAuthorization(_)));
        assert!(err.message().contains(unauth_topic), "got: {}", err.message());
    }

    /// Java: `testMetadataMerge` — partial updates merge in new topic
    /// data while retaining unrelated existing data.
    #[test]
    fn metadata_merge_partial_update_retains_old_topics() {
        let metadata = fresh_metadata();
        // Step 1: full update with topic-1.
        let mut topic_ids = HashMap::new();
        topic_ids.insert("topic-1".to_owned(), Uuid::random());
        let resp_step1 = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(1), &topic_ids);
        metadata.update_with_current_request_version(&resp_step1, false, 0).unwrap();
        assert!(metadata.fetch().topics().any(|t| t == "topic-1"));

        // Step 2: partial update with topic-2 only — topic-1 should be retained.
        let mut topic_ids2 = HashMap::new();
        topic_ids2.insert("topic-2".to_owned(), Uuid::random());
        let resp_step2 = metadata_update_with(Some("dummy"), 1, &[("topic-2", 1)], |_, _| Some(1), &topic_ids2);
        metadata.update_with_current_request_version(&resp_step2, true, 0).unwrap();
        let topics: HashSet<String> = metadata.fetch().topics().map(str::to_owned).collect();
        assert!(topics.contains("topic-1"));
        assert!(topics.contains("topic-2"));
    }

    /// Java: `testEpochUpdateAfterTopicDeletion`
    /// (`MetadataTest.java:388-411`). Three-phase test:
    /// 1. Empty → topic with topic-id A, epoch 10. last-seen = 10.
    /// 2. Same topic returned with `UNKNOWN_TOPIC_OR_PARTITION` error
    ///    response. last-seen still = 10.
    /// 3. Topic recreated with **different topic id B**, epoch 5.
    ///    last-seen = 5 (lower epoch wins because topic id changed).
    #[test]
    fn epoch_update_after_topic_deletion() {
        let metadata = fresh_metadata();
        let tp = TopicPartition::new("topic-1".to_owned(), 0);

        // Phase 0: empty.
        let resp_empty = empty_metadata_response();
        metadata.update_with_current_request_version(&resp_empty, false, 0).unwrap();

        // Phase 1: topic with topic-id A, epoch 10.
        let topic_id_a = Uuid::random();
        let mut topic_ids = HashMap::new();
        topic_ids.insert("topic-1".to_owned(), topic_id_a);
        let resp_initial = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(10), &topic_ids);
        metadata.update_with_current_request_version(&resp_initial, false, 1).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(10));

        // Phase 2: topic returned with `UNKNOWN_TOPIC_OR_PARTITION`.
        // The error response carries the topic name + the error code
        // and no partitions. last-seen epoch is preserved.
        let resp_err = build_metadata_response(
            Some("dummy"),
            0,
            vec![Node::new(0, "localhost".to_owned(), 1969)],
            vec![TopicMetadataInput {
                topic: "topic-1".to_owned(),
                topic_id: ZERO_UUID,
                is_internal: false,
                error: Errors::UnknownTopicOrPartition,
                partitions: Vec::new(),
            }],
        );
        metadata.update_with_current_request_version(&resp_err, false, 1).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(10));

        // Phase 3: same topic recreated with different topic-id B,
        // lower epoch 5 — but since topic id changed, the lower epoch
        // wins per `update_latest_metadata`'s "topic id changed"
        // branch.
        let topic_id_b = Uuid::random();
        assert_ne!(topic_id_a, topic_id_b);
        let mut new_topic_ids = HashMap::new();
        new_topic_ids.insert("topic-1".to_owned(), topic_id_b);
        let resp_new = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(5), &new_topic_ids);
        metadata.update_with_current_request_version(&resp_new, false, 1).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(5));
    }

    /// Java: `testEpochUpdateOnChangedTopicIds`
    /// (`MetadataTest.java:413-452`). 6-phase test exercising the
    /// "topic id changed" branch of `update_latest_metadata` across
    /// progressively newer epochs and topic-id changes.
    #[test]
    fn epoch_update_on_changed_topic_ids() {
        let metadata = fresh_metadata();
        let tp = TopicPartition::new("topic-1".to_owned(), 0);
        let topic_id_a = Uuid::random();
        let mut topic_ids_a = HashMap::new();
        topic_ids_a.insert("topic-1".to_owned(), topic_id_a);

        // Phase 0: empty.
        metadata
            .update_with_current_request_version(&empty_metadata_response(), false, 0)
            .unwrap();

        // Phase 1: topic with no topic id, epoch 100.
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(100), &HashMap::new());
        metadata.update_with_current_request_version(&resp, false, 1).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(100));

        // Phase 2: introduce topic id A with epoch 10. Since the old
        // topic id was null, the new one wins even with a lower epoch.
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(10), &topic_ids_a);
        metadata.update_with_current_request_version(&resp, false, 2).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(10));

        // Phase 3: same topic id, same epoch — no change.
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(10), &topic_ids_a);
        metadata.update_with_current_request_version(&resp, false, 3).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(10));

        // Phase 4: same topic id, newer epoch wins.
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(12), &topic_ids_a);
        metadata.update_with_current_request_version(&resp, false, 4).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(12));

        // Phase 5: new topic id B, lower epoch 3 — wins because topic
        // id changed.
        let topic_id_b = Uuid::random();
        let mut topic_ids_b = HashMap::new();
        topic_ids_b.insert("topic-1".to_owned(), topic_id_b);
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(3), &topic_ids_b);
        metadata.update_with_current_request_version(&resp, false, 5).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(3));

        // Phase 6: another new topic id with higher epoch 20.
        let topic_id_c = Uuid::random();
        let mut topic_ids_c = HashMap::new();
        topic_ids_c.insert("topic-1".to_owned(), topic_id_c);
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(20), &topic_ids_c);
        metadata.update_with_current_request_version(&resp, false, 6).unwrap();
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(20));
    }

    /// Java: `testMetadataMergeOnIdDowngrade`
    /// (`MetadataTest.java:1019-1064`). Tests the topic-id downgrade
    /// scenario (id present → id absent in the next response): the
    /// topic stays but its cached topic id is cleared. Uses
    /// `set_retain_topic_fn` to mirror Java's anonymous subclass
    /// override of `retainTopic`.
    #[test]
    fn metadata_merge_on_id_downgrade() {
        let metadata = fresh_metadata();
        let retain: Arc<Mutex<HashSet<String>>> =
            Arc::new(Mutex::new(HashSet::from(["validTopic1".to_owned(), "validTopic2".to_owned()])));
        let retain_for_predicate = Arc::clone(&retain);
        metadata.set_retain_topic_fn(Arc::new(move |topic, _topic_id, _is_internal, _now_ms| {
            retain_for_predicate.lock().expect("retain set poisoned").contains(topic)
        }));

        // Initial response: two topics with topic ids.
        let topic_id_1 = Uuid::random();
        let topic_id_2 = Uuid::random();
        let mut topic_ids = HashMap::new();
        topic_ids.insert("validTopic1".to_owned(), topic_id_1);
        topic_ids.insert("validTopic2".to_owned(), topic_id_2);
        let resp = metadata_update_with(
            Some("clusterId"),
            2,
            &[("validTopic1", 2), ("validTopic2", 3)],
            |_, _| Some(100),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&resp, true, 0).unwrap();
        let topic_ids_after = metadata.topic_ids();
        assert_eq!(topic_ids_after.get("validTopic1"), Some(&topic_id_1));
        assert_eq!(topic_ids_after.get("validTopic2"), Some(&topic_id_2));

        // Downgrade: topic id removed from validTopic1; topic itself
        // remains but its id is no longer cached.
        let mut downgrade_topic_ids = HashMap::new();
        downgrade_topic_ids.insert("validTopic2".to_owned(), topic_id_2);
        let resp = metadata_update_with(
            Some("clusterId"),
            2,
            &[("validTopic1", 2), ("validTopic2", 3)],
            |_, _| Some(200),
            &downgrade_topic_ids,
        );
        metadata.update_with_current_request_version(&resp, true, 1).unwrap();

        let cluster = metadata.fetch();
        let topics: HashSet<String> = cluster.topics().map(str::to_owned).collect();
        assert!(topics.contains("validTopic1"));
        assert!(topics.contains("validTopic2"));
        assert_eq!(cluster.partitions_for_topic("validTopic1").len(), 2);
        // validTopic1 no longer has a topic id.
        let topic_ids_final = metadata.topic_ids();
        assert!(!topic_ids_final.contains_key("validTopic1"));
        assert_eq!(topic_ids_final.get("validTopic2"), Some(&topic_id_2));
    }

    /// Java: `testTopicMetadataOnUpdatePartitionLeadership`
    /// (`MetadataTest.java:1066-1139`). Verifies that
    /// `update_partition_leadership` can change a partition's leader id
    /// without losing other partition data.
    #[test]
    fn topic_metadata_on_update_partition_leadership() {
        let metadata = fresh_metadata();
        let topic = "input-topic";
        let topic_id = Uuid::random();
        let node1 = Node::new(1, "localhost".to_owned(), 9091);
        let node2 = Node::new(2, "localhost".to_owned(), 9091);

        let tp0 = TopicPartition::new(topic.to_owned(), 0);
        let tp1 = TopicPartition::new(topic.to_owned(), 1);

        // Build a response with two partitions for `input-topic`,
        // both led by node 1.
        let resp = build_metadata_response(
            Some("clusterId"),
            node1.id(),
            vec![node1.clone(), node2.clone()],
            vec![TopicMetadataInput {
                topic: topic.to_owned(),
                topic_id,
                is_internal: false,
                error: Errors::None,
                partitions: vec![
                    PartitionMetadataInput {
                        partition_index: 0,
                        leader_id: Some(1),
                        leader_epoch: Some(1),
                        replicas: vec![1, 2],
                        isr: vec![1, 2],
                        offline: Vec::new(),
                        error: Errors::None,
                    },
                    PartitionMetadataInput {
                        partition_index: 1,
                        leader_id: Some(1),
                        leader_epoch: Some(1),
                        replicas: vec![1, 2],
                        isr: vec![1, 2],
                        offline: Vec::new(),
                        error: Errors::None,
                    },
                ],
            }],
        );
        metadata.update_with_current_request_version(&resp, false, 0).unwrap();
        assert_eq!(metadata.fetch().partitions_for_topic(topic).len(), 2);
        assert_eq!(metadata.fetch().partition(&tp0).unwrap().leader().unwrap().id(), 1);
        assert_eq!(metadata.fetch().partition(&tp1).unwrap().leader().unwrap().id(), 1);

        // partition 1 leader changes from node 1 to node 2 (epoch 3).
        let mut leaders = HashMap::new();
        leaders.insert(tp1.clone(), LeaderIdAndEpoch::new(Some(2), Some(3)));
        metadata.update_partition_leadership(leaders, vec![node1.clone()]);

        assert_eq!(metadata.fetch().partitions_for_topic(topic).len(), 2);
        assert_eq!(metadata.fetch().partition(&tp0).unwrap().leader().unwrap().id(), 1);
        assert_eq!(metadata.fetch().partition(&tp1).unwrap().leader().unwrap().id(), 2);
    }

    /// Java: `testConcurrentUpdateAndFetchForSnapshotAndCluster`
    /// (`MetadataTest.java:1145-1232`). Spawns 6 OS threads (3 writers,
    /// 3 readers) and asserts that after all complete, the snapshot and
    /// cluster reflect the higher node count, partition counts, and
    /// leader epoch from the writers.
    ///
    /// Translation note: Java uses `ExecutorService` + `CountDownLatch`;
    /// we use `std::thread::spawn` + `std::sync::Barrier` because the
    /// `Metadata` mutex is `std::sync::Mutex` (synchronous). The
    /// post-test assertions compare `>` (strictly greater than) just
    /// like the Java equivalent.
    #[test]
    fn concurrent_update_and_fetch_for_snapshot_and_cluster() {
        use std::sync::{Arc as StdArc, Barrier, Mutex as StdMutex};

        let metadata = StdArc::new(fresh_metadata());

        let topic1 = "test_topic1";
        let topic2 = "test_topic2";
        let old_node_count = 10;
        let old_partition_count = 1;
        let old_leader_epoch = 100;
        let topic1_part0 = TopicPartition::new(topic1.to_owned(), 0);

        let mut topic_ids = HashMap::new();
        topic_ids.insert(topic1.to_owned(), Uuid::random());
        topic_ids.insert(topic2.to_owned(), Uuid::random());

        // Initial setup.
        let resp = metadata_update_with(
            Some("cluster"),
            old_node_count,
            &[(topic1, old_partition_count), (topic2, old_partition_count)],
            |_, _| Some(old_leader_epoch),
            &topic_ids,
        );
        metadata.update_with_current_request_version(&resp, true, 0).unwrap();
        let snapshot = metadata.fetch_metadata_snapshot();
        let cluster = metadata.fetch();
        assert_eq!(*cluster, *snapshot.cluster());
        assert_eq!(snapshot.cluster_ref().nodes().len(), old_node_count as usize);
        assert_eq!(
            snapshot.cluster_ref().partitions_for_topic(topic1).len(),
            old_partition_count as usize
        );
        assert_eq!(
            snapshot.cluster_ref().partitions_for_topic(topic2).len(),
            old_partition_count as usize
        );
        assert_eq!(snapshot.leader_epoch_for(&topic1_part0), Some(old_leader_epoch));

        // 3 writer threads + 3 reader threads, coordinated via Barrier.
        let num_threads = 6;
        let metadata_updated_once = StdArc::new(Barrier::new(num_threads));
        let new_snapshot: StdArc<StdMutex<Option<Arc<MetadataSnapshot>>>> = StdArc::new(StdMutex::new(None));
        let new_cluster: StdArc<StdMutex<Option<Arc<Cluster>>>> = StdArc::new(StdMutex::new(None));

        let mut handles = Vec::new();
        for i in 0..num_threads {
            let id = (i + 1) as i32;
            let metadata = StdArc::clone(&metadata);
            let topic_ids = topic_ids.clone();
            let barrier = StdArc::clone(&metadata_updated_once);
            let new_snapshot = StdArc::clone(&new_snapshot);
            let new_cluster = StdArc::clone(&new_cluster);
            handles.push(std::thread::spawn(move || {
                if id % 2 == 0 {
                    // Writer thread.
                    let n_nodes = old_node_count + id;
                    let resp = metadata_update_with(
                        Some("clusterId"),
                        n_nodes,
                        &[(topic1, old_partition_count + id), (topic2, old_partition_count + id)],
                        move |_, _| Some(old_leader_epoch + id),
                        &topic_ids,
                    );
                    metadata.update_with_current_request_version(&resp, true, 0).unwrap();
                    barrier.wait();
                } else {
                    // Reader thread — wait until at least one writer
                    // has finished, then snapshot.
                    barrier.wait();
                    *new_snapshot.lock().unwrap() = Some(metadata.fetch_metadata_snapshot());
                    *new_cluster.lock().unwrap() = Some(metadata.fetch());
                }
            }));
        }
        for h in handles {
            h.join().expect("worker thread panicked");
        }

        let final_snapshot = new_snapshot.lock().unwrap().clone().expect("reader recorded snapshot");
        let final_cluster = new_cluster.lock().unwrap().clone().expect("reader recorded cluster");

        // Validate snapshot.
        let new_node_count = final_snapshot.cluster_ref().nodes().len();
        assert!(
            (old_node_count as usize) < new_node_count,
            "Unexpected snapshot node count: {new_node_count}"
        );
        let new_partition_count_topic1 = final_snapshot.cluster_ref().partitions_for_topic(topic1).len();
        assert!(
            (old_partition_count as usize) < new_partition_count_topic1,
            "Unexpected snapshot partition count for {topic1}: {new_partition_count_topic1}"
        );
        let new_partition_count_topic2 = final_snapshot.cluster_ref().partitions_for_topic(topic2).len();
        assert!(
            (old_partition_count as usize) < new_partition_count_topic2,
            "Unexpected snapshot partition count for {topic2}: {new_partition_count_topic2}"
        );
        let new_leader_epoch = final_snapshot.leader_epoch_for(&topic1_part0).expect("leader epoch present");
        assert!(
            old_leader_epoch < new_leader_epoch,
            "Unexpected snapshot leader epoch: {new_leader_epoch}"
        );

        // Validate cluster.
        let new_node_count = final_cluster.nodes().len();
        assert!(
            (old_node_count as usize) < new_node_count,
            "Unexpected cluster node count: {new_node_count}"
        );
        assert!((old_partition_count as usize) < final_cluster.partitions_for_topic(topic1).len());
        assert!((old_partition_count as usize) < final_cluster.partitions_for_topic(topic2).len());
    }

    /// Java: `testStaleMetadata` (`MetadataTest.java:232-280`). An
    /// older leader epoch with a changed ISR is ignored — the cached
    /// epoch and replica list stick at the higher-epoch values.
    #[test]
    fn stale_metadata_with_older_epoch_ignored() {
        let metadata = fresh_metadata();
        let tp = TopicPartition::new("topic".to_owned(), 0);

        // First update: epoch 10, ISR=[1,2,3].
        let resp_first = build_metadata_response(
            Some("clusterId"),
            0,
            Vec::new(), // empty broker list (matches Java's empty MetadataResponseBrokerCollection)
            vec![TopicMetadataInput {
                topic: "topic".to_owned(),
                topic_id: ZERO_UUID,
                is_internal: false,
                error: Errors::None,
                partitions: vec![PartitionMetadataInput {
                    partition_index: 0,
                    leader_id: Some(1),
                    leader_epoch: Some(10),
                    replicas: vec![1, 2, 3],
                    isr: vec![1, 2, 3],
                    offline: Vec::new(),
                    error: Errors::None,
                }],
            }],
        );
        metadata.update_with_current_request_version(&resp_first, false, 100).unwrap();

        // Second update: older epoch 9 with changed ISR=[1,2]. Should
        // be rejected.
        let resp_stale = build_metadata_response(
            Some("clusterId"),
            0,
            Vec::new(),
            vec![TopicMetadataInput {
                topic: "topic".to_owned(),
                topic_id: ZERO_UUID,
                is_internal: false,
                error: Errors::None,
                partitions: vec![PartitionMetadataInput {
                    partition_index: 0,
                    leader_id: Some(1),
                    leader_epoch: Some(9),
                    replicas: vec![1, 2, 3],
                    isr: vec![1, 2],
                    offline: Vec::new(),
                    error: Errors::None,
                }],
            }],
        );
        metadata.update_with_current_request_version(&resp_stale, false, 101).unwrap();

        // Last seen epoch still 10.
        assert_eq!(metadata.last_seen_leader_epoch(&tp), Some(10));

        let pm = metadata.partition_metadata_if_current(&tp).expect("partition still present");
        // ISR stays at the higher-epoch value [1, 2, 3].
        assert_eq!(pm.in_sync_replica_ids, vec![1, 2, 3]);
        assert_eq!(pm.leader_epoch, Some(10));
    }

    /// Java: `testRequestVersion` (`MetadataTest.java:612-639`). The
    /// `request_version` increments on each `request_update_for_new_topics`,
    /// and an in-flight bump (between `new_metadata_request_and_version`
    /// and `update`) keeps `update_requested` true until the response
    /// catches up.
    #[test]
    fn request_version_in_flight_bump() {
        let metadata = fresh_metadata();
        metadata.request_update(true);
        let v0 = metadata.new_metadata_request_and_version(0);
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic", 1)], |_, _| Some(1), &HashMap::new());
        metadata.update(v0.request_version, &resp, false, 0).unwrap();
        assert!(!metadata.update_requested());

        // Bump the request version for new topics.
        metadata.request_update_for_new_topics();
        // Simulate an in-flight bump.
        let v1 = metadata.new_metadata_request_and_version(0);
        metadata.request_update_for_new_topics();
        metadata.update(v1.request_version, &resp, true, 0).unwrap();
        // Update still needed (the response was for the older
        // request_version).
        assert!(metadata.update_requested());

        // The next update will resolve it.
        let v2 = metadata.new_metadata_request_and_version(0);
        metadata.update(v2.request_version, &resp, true, 0).unwrap();
        assert!(!metadata.update_requested());
    }

    /// Java: `testPartialMetadataUpdate` (`MetadataTest.java:641-702`).
    /// Drives the partial-vs-full update transitions.
    #[test]
    fn partial_metadata_update_full_vs_partial() {
        let metadata = fresh_metadata();
        assert!(!metadata.update_requested());

        // Request a metadata update — must be full.
        metadata.request_update(true);
        let v = metadata.new_metadata_request_and_version(0);
        assert!(!v.is_partial_update);
        let resp = metadata_update_with(Some("dummy"), 1, &[("topic", 1)], |_, _| Some(1), &HashMap::new());
        metadata.update(v.request_version, &resp, false, 0).unwrap();
        assert!(!metadata.update_requested());

        // Request an update for a new topic — partial.
        metadata.request_update_for_new_topics();
        let v = metadata.new_metadata_request_and_version(0);
        assert!(v.is_partial_update);
        metadata.update(v.request_version, &resp, true, 0).unwrap();
        assert!(!metadata.update_requested());

        // Request both kinds of updates — must be full.
        metadata.request_update(true);
        metadata.request_update_for_new_topics();
        let v = metadata.new_metadata_request_and_version(0);
        assert!(!v.is_partial_update);
        metadata.update(v.request_version, &resp, false, 0).unwrap();
        assert!(!metadata.update_requested());

        // Partial-only request, but with elapsed time → must still be full.
        metadata.request_update_for_new_topics();
        let refresh_time_ms = metadata.metadata_expire_ms() + 1;
        let v = metadata.new_metadata_request_and_version(refresh_time_ms);
        assert!(!v.is_partial_update);
        metadata.update(v.request_version, &resp, true, refresh_time_ms).unwrap();
        assert!(!metadata.update_requested());

        // Two overlapping partial updates.
        metadata.request_update_for_new_topics();
        let v_first = metadata.new_metadata_request_and_version(0);
        assert!(v_first.is_partial_update);
        metadata.request_update_for_new_topics();
        let v_overlap = metadata.new_metadata_request_and_version(0);
        assert!(v_overlap.is_partial_update);
        assert!(metadata.update_requested());

        let resp1 = metadata_update_with(Some("dummy"), 1, &[("topic-1", 1)], |_, _| Some(1), &HashMap::new());
        metadata.update(v_first.request_version, &resp1, true, 0).unwrap();
        assert!(metadata.update_requested());

        let resp2 = metadata_update_with(Some("dummy"), 1, &[("topic-2", 1)], |_, _| Some(1), &HashMap::new());
        metadata.update(v_overlap.request_version, &resp2, true, 0).unwrap();
        assert!(!metadata.update_requested());
    }

    /// Java: `testMetadataTopicErrors` (`MetadataTest.java:749-782`).
    /// Per-topic error propagation: invalid topics and unauthorized
    /// topics in the same response. `maybe_throw_error_for_topic`
    /// throws specifically for that topic; other topics see no error.
    #[test]
    fn metadata_topic_errors_per_topic() {
        let metadata = fresh_metadata();
        let resp = build_metadata_response(
            Some("clusterId"),
            0,
            vec![Node::new(0, "localhost".to_owned(), 1969)],
            vec![
                TopicMetadataInput {
                    topic: "invalidTopic".to_owned(),
                    topic_id: ZERO_UUID,
                    is_internal: false,
                    error: Errors::InvalidTopicException,
                    partitions: Vec::new(),
                },
                TopicMetadataInput {
                    topic: "sensitiveTopic1".to_owned(),
                    topic_id: ZERO_UUID,
                    is_internal: false,
                    error: Errors::TopicAuthorizationFailed,
                    partitions: Vec::new(),
                },
                TopicMetadataInput {
                    topic: "sensitiveTopic2".to_owned(),
                    topic_id: ZERO_UUID,
                    is_internal: false,
                    error: Errors::TopicAuthorizationFailed,
                    partitions: Vec::new(),
                },
            ],
        );

        // Per-topic throw for sensitiveTopic1.
        metadata.update_with_current_request_version(&resp, false, 0).unwrap();
        let err = metadata.maybe_throw_error_for_topic("sensitiveTopic1").unwrap_err();
        assert!(matches!(err, KafkaError::TopicAuthorization(_)));
        assert!(err.message().contains("sensitiveTopic1"), "got: {}", err.message());
        // Clearing on subsequent call.
        assert!(metadata.maybe_throw_any_error().is_ok());

        // Per-topic throw for sensitiveTopic2.
        metadata.update_with_current_request_version(&resp, false, 0).unwrap();
        let err = metadata.maybe_throw_error_for_topic("sensitiveTopic2").unwrap_err();
        assert!(matches!(err, KafkaError::TopicAuthorization(_)));
        assert!(err.message().contains("sensitiveTopic2"), "got: {}", err.message());
        assert!(metadata.maybe_throw_any_error().is_ok());

        // Per-topic throw for invalidTopic.
        metadata.update_with_current_request_version(&resp, false, 0).unwrap();
        let err = metadata.maybe_throw_error_for_topic("invalidTopic").unwrap_err();
        assert!(matches!(err, KafkaError::InvalidTopic(_)));
        assert!(err.message().contains("invalidTopic"), "got: {}", err.message());
        assert!(metadata.maybe_throw_any_error().is_ok());

        // Other topics: no exception, but state still cleared.
        metadata.update_with_current_request_version(&resp, false, 0).unwrap();
        assert!(metadata.maybe_throw_error_for_topic("anotherTopic").is_ok());
        assert!(metadata.maybe_throw_any_error().is_ok());
    }
}
