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

//! `AbstractFetch` — shared state and behavior for the fetch request
//! managers.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.AbstractFetch` (650 LOC).
//!
//! # Concrete struct, not a trait
//!
//! Java models this as an abstract class with subclasses `Fetcher` (classic
//! protocol, out of scope per `consumer-threading.md` §20) and
//! `FetchRequestManager` (Phase 7b). Java's inheritance-based polymorphism
//! does not map cleanly to Rust, so the Rust translation models
//! `AbstractFetch` as a plain `pub(crate) struct` that
//! [`FetchRequestManager`] (Phase 7b) will compose as a field. Protected
//! Java fields become `pub(crate)` so the composing struct can read /
//! write them.
//!
//! This precedent mirrors the `ConsumerMetadata` / `ProducerMetadata`
//! composition-over-inheritance choice from earlier phases.
//!
//! # Scope of this commit
//!
//! - Fields, constructor, and the simple accessors `has_completed_fetches`,
//!   `has_available_fetches`, `close_session_handler`, `close`.
//! - `prepare_close_fetch_session_requests` — bulk-mark every session as
//!   pending-close and return per-node session builders.
//!
//! The big methods (`prepare_fetch_requests`, `create_fetch_request`,
//! `handle_fetch_success`, `handle_fetch_failure`) are translated as
//! `pub(crate)` methods so Phase 7b's `FetchRequestManager` can invoke
//! them directly without re-implementing the diff logic. Their full Java
//! behavior is preserved; metrics calls and `LogContext` are dropped per
//! the plan.
//!
//! A dedicated `AbstractFetchTest` does not exist in Java — coverage is
//! supplied by `FetchRequestManagerTest` (Phase 7b) and `FetcherTest`
//! (out of scope). Inline tests in this file therefore cover the small
//! pieces a behavioral exercise can't reach through 7b: per-node
//! session-handler lifecycle and the pending-fetch tracking set.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use log::{debug, trace};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

use crate::common::Node;
use crate::common::TopicPartition;
use crate::common::memory::buffer_supplier::BufferSupplier;
use crate::common::protocol::ApiKeys;
use crate::common::requests::fetch_request::{
    CONSUMER_REPLICA_ID, FetchRequestBuilder, INVALID_LOG_START_OFFSET, PartitionData,
};
use crate::common::requests::fetch_response::FetchResponse;
use crate::consumer::internals::completed_fetch::CompletedFetch;
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
use crate::consumer::internals::fetch_buffer::FetchBuffer;
use crate::consumer::internals::fetch_config::FetchConfig;
use crate::consumer::internals::fetch_metrics_aggregator::FetchMetricsAggregator;
use crate::consumer::internals::fetch_metrics_manager::FetchMetricsManager;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::fetch_session_handler::{FetchSessionHandler, FetchSessionRequestData};

/// Shared state and methods used by the fetch request managers
/// (`FetchRequestManager` in Phase 7b).
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.AbstractFetch`.
///
/// # Field visibility
///
/// All fields are `pub(crate)` so the composing
/// [`crate::consumer::internals::request_manager::RequestManager`]
/// implementation in Phase 7b can read / write them. This matches Java's
/// `protected` semantics on the abstract base.
pub(crate) struct AbstractFetch {
    /// Cluster metadata.
    pub(crate) metadata: Arc<ConsumerMetadata>,
    /// Subscription state — Phase 7b updates positions / leadership from
    /// fetch responses through this.
    pub(crate) subscriptions: Arc<Mutex<SubscriptionState>>,
    /// Immutable fetch configuration.
    pub(crate) fetch_config: FetchConfig,
    /// The completed-fetches buffer the bg task feeds.
    pub(crate) fetch_buffer: Arc<FetchBuffer>,
    /// Shared decompression buffer pool used by `CompletedFetch` record
    /// iteration.
    pub(crate) decompression_buffer_supplier: Arc<BufferSupplier>,
    /// IDs of nodes for which a fetch request is in-flight. Java uses a
    /// plain `HashSet<Integer>` because the abstract class is touched
    /// only from the consumer's single network thread; the Rust port
    /// preserves single-task access (Phase 7b's `FetchRequestManager`
    /// owns this struct on the bg task) so a plain HashSet is enough.
    pub(crate) nodes_with_pending_fetch_requests: FxHashSet<i32>,
    /// Whether `close` has been called.
    pub(crate) closed: bool,

    /// Per-node fetch session state.
    ///
    /// FxHash (non-cryptographic) keyed by the internal broker node id
    /// (`i32`); looked up per fetch in `prepare_fetch_requests`. The keys are
    /// not attacker-controlled, so SipHash's DoS resistance buys nothing while
    /// its per-lookup cost shows on the per-fetch path (Phase 25).
    session_handlers: FxHashMap<i32, FetchSessionHandler>,

    /// DIAGNOSTIC (not in Java): per-node instant the in-flight fetch was sent,
    /// used only to log the fetch round-trip time (send -> response) under the
    /// `fetch_diag` log target. RTT localizes where end-to-end latency goes:
    /// a large RTT means the broker held the fetch (fetch.max.wait.ms /
    /// fetch.min.bytes), not consumer-side processing. Opt-in via
    /// `RUST_LOG=fetch_diag=info`; zero cost when that target is disabled.
    fetch_sent_at: FxHashMap<i32, std::time::Instant>,

    /// Per-node negotiated API versions. Java's `AbstractFetch` holds this
    /// to decide, on a KIP-951 leadership change, whether the new leader
    /// supports a usable `OffsetsForLeaderEpoch` version before validating
    /// the position (`maybeValidatePositionForCurrentLeader`). Phase 7a
    /// dropped this parameter; Phase 37 re-introduces it because the
    /// leadership-change branch of `handle_fetch_success` needs it.
    api_versions: Arc<crate::api_versions::ApiVersions>,

    /// Records fetch / lag / lead / latency metrics. Shared (`Arc`) so the
    /// per-response [`FetchMetricsAggregator`] can record bytes/records into the
    /// same sensors once a fetch's partitions are all drained. Phase M3
    /// re-introduces the `FetchMetricsManager` parameter Phase 7a dropped.
    pub(crate) metrics_manager: Arc<FetchMetricsManager>,
}

impl AbstractFetch {
    /// Constructs an `AbstractFetch` for use by a request manager.
    ///
    /// Translates Java's
    /// `AbstractFetch(LogContext, ConsumerMetadata, SubscriptionState,
    ///   FetchConfig, FetchBuffer, FetchMetricsManager, Time, ApiVersions,
    ///   BufferSupplier)`. Drops the `LogContext` (we use the `log` crate) and
    /// `Time` (Phase 7b will plumb a clock if needed for read-replica leasing).
    /// `FetchMetricsManager` is plumbed in Phase M3 (it was dropped by Phase
    /// 7a). `ApiVersions` IS kept (Phase 37): the KIP-951 leadership-change
    /// branch of `handle_fetch_success` needs it to decide whether the new
    /// leader supports a usable `OffsetsForLeaderEpoch` version before
    /// validating the position.
    ///
    /// The `decompression_buffer_supplier` is shared with the consumer's
    /// other decompression call sites (Java's `KafkaConsumer` creates a
    /// single supplier and passes it both here and into ad-hoc
    /// decompression). Callers that don't need sharing can pass
    /// `Arc::new(BufferSupplier::create())`.
    pub(crate) fn new(
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        fetch_config: FetchConfig,
        fetch_buffer: Arc<FetchBuffer>,
        decompression_buffer_supplier: Arc<BufferSupplier>,
        api_versions: Arc<crate::api_versions::ApiVersions>,
        metrics_manager: Arc<FetchMetricsManager>,
    ) -> Self {
        Self {
            metadata,
            subscriptions,
            fetch_config,
            fetch_buffer,
            decompression_buffer_supplier,
            nodes_with_pending_fetch_requests: FxHashSet::default(),
            closed: false,
            session_handlers: FxHashMap::default(),
            fetch_sent_at: FxHashMap::default(),
            api_versions,
            metrics_manager,
        }
    }

    /// Returns whether the buffer has any completed fetches awaiting
    /// return to the user.
    ///
    /// Translates `boolean hasCompletedFetches()`.
    pub(crate) fn has_completed_fetches(&self) -> bool {
        !self.fetch_buffer.is_empty()
    }

    /// Returns whether the buffer has any completed fetches whose
    /// partitions are still fetchable from the subscription's
    /// perspective.
    ///
    /// Translates `boolean hasAvailableFetches()`.
    pub(crate) fn has_available_fetches(&self) -> bool {
        let subs = self.subscriptions.clone();
        self.fetch_buffer.has_completed_fetches(|cf| {
            let guard = subs.lock().expect("SubscriptionState mutex poisoned");
            guard.is_fetchable(&cf.partition)
        })
    }

    /// Returns the session handler for the given node, or `None` if one
    /// has not been created yet.
    ///
    /// Visible for testing; mirrors Java's `protected FetchSessionHandler
    /// sessionHandler(int node)`.
    pub(crate) fn session_handler(&self, node: i32) -> Option<&FetchSessionHandler> {
        self.session_handlers.get(&node)
    }

    /// Mutable version of [`Self::session_handler`].
    pub(crate) fn session_handler_mut(&mut self, node: i32) -> Option<&mut FetchSessionHandler> {
        self.session_handlers.get_mut(&node)
    }

    /// Drops the session handler for the given node.
    ///
    /// Translates `void closeSessionHandler(int node)` (which Java implements
    /// by removing the handler from the map). Used after a session is closed
    /// via FINAL_EPOCH.
    pub(crate) fn close_session_handler(&mut self, node_id: i32) {
        if let Some(handler) = self.session_handlers.remove(&node_id) {
            trace!(
                "Closed fetch session for node {} (session id {})",
                node_id,
                handler.session_id()
            );
        }
    }

    /// Removes a node from the pending-fetch set after a response (or
    /// failure) is observed.
    ///
    /// Mirrors Java's `removePendingFetchRequest`.
    pub(crate) fn remove_pending_fetch_request(&mut self, fetch_target: &Node, session_id: i32) {
        debug!(
            "Removing pending request for fetch session: {} for node: {}",
            session_id,
            fetch_target.id()
        );
        self.nodes_with_pending_fetch_requests.remove(&fetch_target.id());
    }

    /// Marks every session as pending-close and returns the per-node
    /// builders that produce the closing fetch requests.
    ///
    /// Translates `Map<Node, FetchSessionHandler.FetchRequestData>
    /// prepareCloseFetchSessionRequests()`.
    ///
    /// The `nodes` argument supplies the resolved `Node` for each
    /// node-id; Java looks each up via `metadata.fetch().nodeById(...)`.
    /// Skipping the resolution into a parameter keeps the cluster
    /// lookup out of this struct's responsibilities (Phase 7b will do
    /// the lookup before calling).
    pub(crate) fn prepare_close_fetch_session_requests(
        &mut self,
        nodes: &HashMap<i32, Node>,
    ) -> HashMap<i32, FetchSessionRequestData> {
        let mut out: HashMap<i32, FetchSessionRequestData> = HashMap::new();
        // Mark each handler as pending-close, then build with an empty
        // builder so the resulting request only carries the
        // close-existing metadata.
        let node_ids: Vec<i32> = self.session_handlers.keys().copied().collect();
        for node_id in node_ids {
            let handler = self.session_handlers.get_mut(&node_id).expect("just enumerated keys above");
            handler.notify_close();
            // Skip nodes the caller couldn't resolve (Java skips
            // unreachable nodes the same way).
            if !nodes.contains_key(&node_id) {
                debug!(
                    "Skip sending close session request to node {} since it is not reachable",
                    node_id
                );
                continue;
            }
            let builder = handler.new_builder();
            let data = handler.build_request(builder);
            out.insert(node_id, data);
        }
        out
    }

    /// Builds the consumer-side fetch-request builder for the given
    /// target node and pre-built request data.
    ///
    /// Translates `protected FetchRequest.Builder createFetchRequest(Node,
    /// FetchSessionHandler.FetchRequestData)`.
    pub(crate) fn create_fetch_request(
        &mut self,
        fetch_target: &Node,
        request_data: &FetchSessionRequestData,
    ) -> FetchRequestBuilder {
        let max_version = if request_data.can_use_topic_ids {
            ApiKeys::FETCH.latest_version()
        } else {
            12
        };

        // Convert to the `IndexMap<TopicPartition, PartitionData>` the
        // builder expects.
        let to_fetch: IndexMap<TopicPartition, PartitionData> =
            request_data.to_send.iter().map(|(tp, pd)| (tp.clone(), pd.clone())).collect();

        let builder = FetchRequestBuilder::for_consumer(
            max_version,
            self.fetch_config.max_wait_ms,
            self.fetch_config.min_bytes,
            to_fetch,
        )
        .set_isolation_level(self.fetch_config.isolation_level)
        .set_max_bytes(self.fetch_config.max_bytes)
        .set_metadata(request_data.metadata)
        .set_removed(request_data.to_forget.clone())
        .set_replaced(request_data.to_replace.clone())
        .set_rack_id(self.fetch_config.client_rack_id.clone());

        debug!("Sending fetch request to broker {}", fetch_target.id());
        debug!("Adding pending request for node {}", fetch_target.id());
        self.nodes_with_pending_fetch_requests.insert(fetch_target.id());
        // DIAGNOSTIC: stamp the send time so handle_fetch_success can log RTT.
        if log::log_enabled!(target: "fetch_diag", log::Level::Info) {
            self.fetch_sent_at.insert(fetch_target.id(), std::time::Instant::now());
        }
        builder
    }

    /// Handles a successful fetch response from `fetch_target`, applying
    /// the diff to the session handler and enqueuing per-partition
    /// `CompletedFetch`es into the fetch buffer.
    ///
    /// Translates `protected void handleFetchSuccess(Node, FetchRequestData, ClientResponse)`.
    pub(crate) fn handle_fetch_success(
        &mut self,
        fetch_target: &Node,
        request_data: &FetchSessionRequestData,
        response: FetchResponse,
        request_version: i16,
        request_latency_ms: i64,
    ) {
        let session_id = request_data.metadata.session_id();
        // Java's `handler == null` `return` sits INSIDE the `try`, so the
        // `finally { removePendingFetchRequest(...) }` (`AbstractFetch.java:253-255`)
        // still runs. Leaking the node id here would be permanent, not a
        // delay: `prepare_fetch_requests` skips every node in
        // `nodes_with_pending_fetch_requests`, so that broker's partitions
        // would never be fetched again.
        if !self.session_handlers.contains_key(&fetch_target.id()) {
            log::error!(
                "Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
                fetch_target.id()
            );
            self.remove_pending_fetch_request(fetch_target, session_id);
            return;
        }
        let handler = self
            .session_handlers
            .get_mut(&fetch_target.id())
            .expect("presence checked above");

        if !handler.handle_response(&response, request_version) {
            // FETCH_SESSION_TOPIC_ID_ERROR drives a metadata refresh per
            // Java's `metadata.requestUpdate(false)`. Phase 7b's
            // FetchRequestManager owns the metadata-update trigger; we
            // simply mark the response handled here.
            self.remove_pending_fetch_request(fetch_target, session_id);
            return;
        }

        // KIP-951: capture the response's node endpoints BEFORE
        // `into_response_data` consumes the response. Cheap clone of a
        // small Vec that is empty on the happy path; only the leadership-
        // change branch reads it after the loop.
        let response_node_endpoints = response.data().node_endpoints.clone();

        // Phase 20 Fix #2b: MOVE each PartitionData (and its owned record
        // bytes) out of the response rather than cloning it — §27 zero-copy
        // receive contract. `into_response_data` consumes the response, so the
        // record buffer is never copied between the wire-decoded response and
        // the CompletedFetch.
        let response_data = response.into_response_data(handler.session_topic_names(), request_version);

        // DIAGNOSTIC (fetch_diag target): log the fetch round-trip time and
        // payload so we can attribute end-to-end latency. A large RTT means the
        // broker held the fetch open (fetch.max.wait.ms / fetch.min.bytes), i.e.
        // the latency is broker-side wait, NOT consumer-side processing.
        if log::log_enabled!(target: "fetch_diag", log::Level::Info) {
            let rtt_ms = self
                .fetch_sent_at
                .remove(&fetch_target.id())
                .map(|t| t.elapsed().as_millis())
                .unwrap_or(0);
            let parts_with_data = response_data
                .values()
                .filter(|pd| crate::common::requests::fetch_response::records_size(pd) > 0)
                .count();
            let total_bytes: i64 = response_data
                .values()
                .map(|pd| crate::common::requests::fetch_response::records_size(pd) as i64)
                .sum();
            log::info!(
                target: "fetch_diag",
                "fetch response node={} rtt_ms={} parts_with_data={} record_bytes={}",
                fetch_target.id(),
                rtt_ms,
                parts_with_data,
                total_bytes
            );
        }

        // The set of partitions in this response. Translates Java's
        // `Set<TopicPartition> partitions = new HashSet<>(responseData.keySet())`.
        // The per-response aggregator records bytes/records ONCE, after every
        // partition's `CompletedFetch` is drained — never per-record.
        let partitions: HashSet<TopicPartition> = response_data.keys().cloned().collect();
        let metric_aggregator = Arc::new(FetchMetricsAggregator::new(Arc::clone(&self.metrics_manager), partitions));

        let mut needs_wakeup = true;

        // KIP-951: accumulate per-partition new-leader info reported on a
        // leadership-change error, applied to metadata after the loop.
        // Translates Java's `partitionsWithUpdatedLeaderInfo`
        // (`AbstractFetch.java:180`). Lazily allocated like Java — empty on
        // the steady-state happy path (no leadership errors), so the hot
        // path pays only the per-partition `i16` error-code compare below.
        let mut partitions_with_updated_leader_info: HashMap<TopicPartition, crate::metadata::LeaderIdAndEpoch> =
            HashMap::new();

        for (partition, partition_data) in response_data {
            let request_pd = match request_data.to_send.get(&partition).or_else(|| {
                // The Java code also consults `sessionPartitions` which
                // is a superset of `toSend` for incremental fetches —
                // i.e. partitions that exist in the session but were not
                // sent in this round. Look them up via `session_partitions`.
                request_data.session_partitions.get(&partition)
            }) {
                Some(p) => p,
                None => {
                    // "Received fetch response for missing session partition".
                    //
                    // DELIBERATE DIVERGENCE, recorded rather than assumed
                    // (definition-of-done.md §7). Java throws
                    // `IllegalStateException` (`AbstractFetch.java:184-199`),
                    // which aborts the whole response: no `CompletedFetch` is
                    // added for this partition OR any partition after it, and
                    // `fetchBuffer.wakeup()` (`:232`) is skipped because it
                    // sits outside the `finally`. So one malformed entry
                    // discards every well-formed entry beside it and leaves a
                    // `poll()` blocking until its timeout. Rust skips only the
                    // offending partition, keeping the rest of the response and
                    // the wakeup. The condition means the broker echoed a
                    // partition the client never asked for, which is a
                    // protocol-level fault this client cannot act on either
                    // way, so the narrower blast radius is preferred.
                    //
                    // The two message variants follow Java's, including the
                    // `data.metadata().isFull()` split, so the diagnostic a
                    // user reports is comparable with the Java client's.
                    // `toSend` is rendered as its partition keys rather than
                    // the whole map: Java's `PartitionData.toString()` adds
                    // fetch offsets and sizes that are noise for this fault,
                    // and the keys are what identifies the mismatch.
                    if request_data.metadata.is_full() {
                        log::error!(
                            "Response for missing full request partition: partition={}; metadata={}",
                            partition,
                            request_data.metadata
                        );
                    } else {
                        log::error!(
                            "Response for missing session request partition: partition={}; metadata={}; \
                             toSend={:?}; toForget={:?}; toReplace={:?}",
                            partition,
                            request_data.metadata,
                            request_data.to_send.keys().collect::<Vec<_>>(),
                            request_data.to_forget,
                            request_data.to_replace
                        );
                    }
                    continue;
                },
            };

            let fetch_offset = request_pd.fetch_offset;
            debug!(
                "Fetch {} at offset {} for partition {} returned",
                self.fetch_config.isolation_level, fetch_offset, partition
            );

            // KIP-951: when a partition reports a leadership-change error AND
            // carries new leader info, record it for the post-loop metadata
            // update. Translates `AbstractFetch.java:207-214`.
            let partition_error = crate::common::protocol::Errors::for_code(partition_data.error_code);
            if matches!(
                partition_error,
                crate::common::protocol::Errors::NotLeaderOrFollower
                    | crate::common::protocol::Errors::FencedLeaderEpoch
            ) {
                let leader_id = partition_data.current_leader.leader_id;
                let leader_epoch = partition_data.current_leader.leader_epoch;
                debug!(
                    "For {partition}, received error {partition_error:?}, with leaderId {leader_id} leaderEpoch {leader_epoch}"
                );
                if leader_id != -1 && leader_epoch != -1 {
                    partitions_with_updated_leader_info.insert(
                        partition.clone(),
                        crate::metadata::LeaderIdAndEpoch::new(Some(leader_id), Some(leader_epoch)),
                    );
                }
            }

            // `partition` is the owned loop key; move it into the
            // CompletedFetch (its topic is an Arc<str>, so even the prior
            // clone was an Arc bump, not a String copy — §27 topic-name rule).
            let cf = CompletedFetch::new_full(
                self.subscriptions.clone(),
                self.decompression_buffer_supplier.clone(),
                partition,
                partition_data,
                Arc::clone(&metric_aggregator),
                fetch_offset,
            );
            self.fetch_buffer.add(cf);
            needs_wakeup = false;
        }

        // "Wake" the fetch buffer on any response, even if it's empty,
        // to allow the consumer to not block indefinitely.
        if needs_wakeup {
            self.fetch_buffer.wakeup();
        }

        // KIP-951: apply any new-leader info reported on leadership-change
        // errors. Translates `AbstractFetch.java:233-251`. Empty on the
        // happy path — no allocation or lock on the steady-state path.
        if !partitions_with_updated_leader_info.is_empty() {
            // Build the leader node list from the response's node endpoints,
            // skipping `Node.noNode()`-equivalent entries (node_id == -1).
            let mut leader_nodes: Vec<Node> = Vec::new();
            for endpoint in &response_node_endpoints {
                if endpoint.node_id != -1 {
                    leader_nodes.push(Node::new_rack(
                        endpoint.node_id,
                        endpoint.host.clone(),
                        endpoint.port,
                        endpoint.rack.clone(),
                    ));
                }
            }

            let metadata = self.metadata.metadata_arc();
            let updated_partitions =
                metadata.update_partition_leadership(&partitions_with_updated_leader_info, &leader_nodes);
            if !updated_partitions.is_empty() {
                let mut guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
                for tp in &updated_partitions {
                    debug!("For {tp}, as the leader was updated, position will be validated.");
                    let current_leader = metadata.current_leader(tp);
                    guard.maybe_validate_position_for_current_leader(&self.api_versions, tp, &current_leader);
                }
            }
        }

        // Record the fetch request latency. Java uses
        // `resp.destination()` (the broker node id as a string); we pass the
        // node id likewise so the per-node `node-{id}.latency` sensor matches.
        self.metrics_manager
            .record_latency(&fetch_target.id().to_string(), request_latency_ms);

        self.remove_pending_fetch_request(fetch_target, session_id);
    }

    /// Handles a successful close-fetch-session response from `fetch_target`.
    ///
    /// Translates `protected void handleCloseFetchSessionSuccess(Node,
    /// FetchSessionHandler.FetchRequestData, ClientResponse)`. Drops the
    /// node from the pending-fetch set and logs at debug.
    pub(crate) fn handle_close_fetch_session_success(
        &mut self,
        fetch_target: &Node,
        request_data: &FetchSessionRequestData,
    ) {
        let session_id = request_data.metadata.session_id();
        self.remove_pending_fetch_request(fetch_target, session_id);
        debug!(
            "Successfully sent a close message for fetch session: {} to node: {}",
            session_id,
            fetch_target.id()
        );
    }

    /// Handles a failed close-fetch-session response from `fetch_target`.
    ///
    /// Translates `public void handleCloseFetchSessionFailure(Node,
    /// FetchSessionHandler.FetchRequestData, Throwable)`. Drops the node
    /// from the pending-fetch set and logs at debug (Java logs the
    /// throwable; we log the `Error` message).
    pub(crate) fn handle_close_fetch_session_failure(
        &mut self,
        fetch_target: &Node,
        request_data: &FetchSessionRequestData,
        error: &crate::common::Error,
    ) {
        let session_id = request_data.metadata.session_id();
        self.remove_pending_fetch_request(fetch_target, session_id);
        debug!(
            "Unable to send a close message for fetch session: {} to node: {}. \
             This may result in unnecessary fetch sessions at the broker. Cause: {}",
            session_id,
            fetch_target.id(),
            error.message(),
        );
    }

    /// Handles a fetch-request failure.
    ///
    /// Translates `protected void handleFetchFailure(Node, FetchRequestData, Throwable)`.
    pub(crate) fn handle_fetch_failure(
        &mut self,
        fetch_target: &Node,
        request_data: &FetchSessionRequestData,
        error: &crate::common::Error,
    ) {
        let session_id = request_data.metadata.session_id();
        if let Some(handler) = self.session_handlers.get_mut(&fetch_target.id()) {
            handler.handle_error(error);
            // Clear any preferred read replicas for the session's
            // partitions — they may have become invalid.
            let session_tps: Vec<TopicPartition> = handler.session_topic_partitions().into_iter().collect();
            let mut guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            for tp in session_tps {
                guard.clear_preferred_read_replica(&tp);
            }
        }
        self.remove_pending_fetch_request(fetch_target, session_id);
    }

    /// Closes the fetch buffer and decompression supplier. Idempotent.
    ///
    /// Translates `void close(Timer)` (Java's `IdempotentCloser`).
    pub(crate) fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.fetch_buffer.close();
        self.decompression_buffer_supplier.close();
    }

    /// Returns a mutable reference to the session handler for the given
    /// node, creating one if it does not yet exist.
    ///
    /// Mirrors Java's `sessionHandlers.computeIfAbsent(...)` pattern used
    /// by `prepareFetchRequests` to lazily allocate handlers.
    pub(crate) fn session_handler_or_create(&mut self, node_id: i32) -> &mut FetchSessionHandler {
        self.session_handlers
            .entry(node_id)
            .or_insert_with(|| FetchSessionHandler::new(node_id))
    }

    /// Returns the IDs of nodes for which we currently hold a fetch
    /// session handler. Used by
    /// [`crate::consumer::internals::fetch_request_manager::FetchRequestManager::poll_on_close`]
    /// to resolve the per-node `Node` map for close-session requests.
    pub(crate) fn session_handler_ids(&self) -> Vec<i32> {
        self.session_handlers.keys().copied().collect()
    }

    /// Create fetch requests for all nodes for which we have assigned
    /// partitions that have no existing requests in flight.
    ///
    /// Translates `Map<Node, FetchSessionHandler.FetchRequestData>
    /// prepareFetchRequests()`.
    ///
    /// The caller supplies:
    /// - `is_unavailable`: Java's abstract `isUnavailable(Node)` — `true`
    ///   if the node is inside the reconnect backoff window.
    /// - `maybe_throw_auth_failure`: Java's abstract
    ///   `maybeThrowAuthFailure(Node)` — returns `Err` if the node has
    ///   an unresolved authentication failure.
    /// - `current_time_ms`: Java reads `time.milliseconds()` once at the
    ///   start; the caller does the same and passes it in.
    ///
    /// The closures avoid coupling `AbstractFetch` to
    /// `NetworkClientDelegate` (which it does not own — Phase 7b's
    /// `FetchRequestManager` owns the delegate and supplies the
    /// closures).
    pub(crate) fn prepare_fetch_requests(
        &mut self,
        current_time_ms: i64,
        is_unavailable: impl Fn(&Node) -> bool,
        maybe_throw_auth_failure: impl Fn(&Node) -> Result<(), crate::common::Error>,
    ) -> Result<HashMap<i32, (Node, FetchSessionRequestData)>, crate::common::Error> {
        // Update metrics in case there was an assignment change. Java does this
        // first thing in `prepareFetchRequests`. The manager is `Arc`-shared
        // (per-response aggregators hold clones), so its assignment-tracking
        // state uses interior mutability; this call is bg-task-only.
        self.metrics_manager.maybe_update_assignment(&self.subscriptions);

        let cluster = self.metadata.metadata_arc().fetch();

        // Phase 26 (Fix #2): port stock Java's first early-return
        // `if (unfetchableNodes == nodes.size()) return emptyMap`. In steady
        // state every broker has an in-flight fetch (1-fetch-in-flight per
        // broker), so the full computation below (SubscriptionState lock +
        // fetchable scan + buffered-nodes + per-partition node resolution) runs
        // only to return an empty map because every partition's node is skipped
        // (already in `nodes_with_pending_fetch_requests` or unavailable). This
        // up-front check short-circuits that wasted work.
        //
        // STATELESS short-circuit, NOT a cache: it returns empty only when it is
        // genuinely true right now that no node can be fetched (every node is
        // pending or unavailable). The moment a fetch response frees a node
        // (removes it from `nodes_with_pending_fetch_requests`), the next call
        // passes this check and issues normally — no partition can be stranded.
        //
        // Edge: an empty cluster node list (no metadata yet) matches Java's
        // `0 == 0` -> returns empty (nothing to fetch).
        let nodes = cluster.nodes();
        let all_nodes_unfetchable = nodes
            .iter()
            .all(|node| self.nodes_with_pending_fetch_requests.contains(&node.id()) || is_unavailable(node));
        if all_nodes_unfetchable {
            return Ok(HashMap::new());
        }

        // Snapshot the buffered-partitions set, then take the
        // SubscriptionState lock ONCE for the whole preparation (Phase 27
        // Fix #4). The previous shape re-locked the mutex 2-3 times per
        // partition per call and cloned `FetchPosition` (which carries a
        // `Node` → heap `String`s) per partition; cloud profiling showed
        // ~68% of this function's self-time in Arc-refcount + mutex futex
        // traffic. Java's per-query `synchronized` blocks are biased/
        // JIT-elided and its queries return references — holding one guard
        // and borrowing is the closest Rust equivalent. Holding the lock
        // across the loop only narrows the (already benign) interleaving
        // window documented below; no `.await` occurs while it is held
        // (consumer-threading.md §16).
        let buffered = self.fetch_buffer.buffered_partitions();

        let mut guard = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");

        let unbuffered: Vec<TopicPartition> = guard.fetchable_partitions(|tp| !buffered.contains(tp));
        if unbuffered.is_empty() {
            return Ok(HashMap::new());
        }

        // Compute the set of nodes for which we have buffered data —
        // skip these so we don't evict the broker's fetch session cache.
        let buffered_nodes: FxHashSet<i32> =
            self.compute_buffered_nodes(&mut guard, &buffered, current_time_ms, &cluster);

        // For each unbuffered partition, find the target node and add the
        // partition to that node's session-handler builder. These per-fetch
        // temporaries are keyed by internal node id / `TopicPartition`, so
        // they use FxHash (Phase 25) — rebuilt every fetch, never returned.
        let mut node_targets: FxHashMap<i32, Node> = FxHashMap::default();
        let mut fetchable_partitions_by_node: FxHashMap<i32, IndexMap<TopicPartition, PartitionData, FxBuildHasher>> =
            FxHashMap::default();

        for partition in unbuffered {
            // Get position. Java's `positionForPartition` throws
            // `IllegalStateException` if the position is missing
            // (`AbstractFetch.java:508-515`) and Java's `position()`
            // throws if the partition is no longer assigned. In Java the
            // window between `fetchablePartitions()` (snapshot) and
            // `position(tp)` (per-partition query) is narrow because
            // both methods are `synchronized` on the same monitor —
            // Java rarely hits the race in practice. With the single
            // guard held here the window is closed entirely; the
            // transient skip arms below are kept for the `Ok(None)` /
            // unassigned states that remain reachable (e.g. a position
            // never set). See `COMMENTS.1.md` Issue 7.
            //
            // Only `Copy` fields are read out of the position borrow so
            // the borrow ends before the `&mut` replica query below.
            let (position_offset, position_leader_epoch) = match guard.position(&partition) {
                Ok(Some(p)) => {
                    if p.current_leader.leader.is_none() {
                        // Java's `maybeNodeForPosition` empty-leader arm.
                        debug!(
                            "Requesting metadata update for partition {partition} since the position {p} is missing the current leader node"
                        );
                        self.metadata.metadata_arc().request_update(false);
                        continue;
                    }
                    (p.offset, p.current_leader.epoch)
                },
                Ok(None) => {
                    trace!(
                        "Skipping fetch for partition {partition} because it has no position yet \
                         (transient — partition was fetchable at snapshot but lost its position before query)"
                    );
                    continue;
                },
                Err(_e) => {
                    trace!(
                        "Skipping fetch for partition {partition} because it is no longer assigned \
                         (transient rebalance window between fetchable_partitions snapshot and position query)"
                    );
                    continue;
                },
            };

            // Java's `selectReadReplica(partition, leader, currentTimeMs)`
            // inlined (its only callers are this loop and
            // `compute_buffered_nodes`; inlining keeps the borrow scopes on
            // the single guard tractable). Behavior is identical: prefer
            // the (unexpired) read replica when it is online in the
            // cluster snapshot; otherwise clear it, request a metadata
            // update (Java `FetchUtils.requestMetadataUpdate` — performed
            // directly on the held guard), and fall back to the leader.
            let preferred = guard.preferred_read_replica(&partition, current_time_ms);
            let replica_online = match preferred {
                Some(replica_id) => {
                    if cluster.node_if_online(&partition, replica_id).is_some() {
                        Some(replica_id)
                    } else {
                        trace!(
                            "Not fetching from {replica_id} for partition {partition} since it is marked offline or is missing from our metadata, using the leader instead"
                        );
                        // Stale metadata — clear preferred replica and request refresh.
                        self.metadata.metadata_arc().request_update(false);
                        guard.clear_preferred_read_replica(&partition);
                        None
                    }
                },
                None => None,
            };
            // Resolve the `&Node` borrow AFTER the `&mut` calls above. The
            // replica node borrows the cluster snapshot; the leader node
            // borrows the position inside the guard (Java uses exactly
            // these two sources).
            let node: &Node = match replica_online {
                Some(replica_id) => cluster
                    .node_if_online(&partition, replica_id)
                    .expect("node_if_online verified Some above"),
                None => match guard.position(&partition) {
                    Ok(Some(p)) => p.current_leader.leader.as_ref().expect("leader verified Some above"),
                    _ => unreachable!("position verified Some above; guard held continuously"),
                },
            };

            if is_unavailable(node) {
                maybe_throw_auth_failure(node)?;
                trace!(
                    "Skipping fetch for partition {partition} because node {} is awaiting reconnect backoff",
                    node.id()
                );
                continue;
            }
            if self.nodes_with_pending_fetch_requests.contains(&node.id()) {
                trace!(
                    "Skipping fetch for partition {partition} because previous request to {} has not been processed",
                    node.id()
                );
                continue;
            }
            if buffered_nodes.contains(&node.id()) {
                trace!(
                    "Skipping fetch for partition {partition} because its leader node {} hosts buffered partitions",
                    node.id()
                );
                continue;
            }

            // Add to the node's per-fetch partition map. The `Node` is
            // cloned once per distinct node (not per partition).
            let node_id = node.id();
            node_targets.entry(node_id).or_insert_with(|| node.clone());
            // Topic id from the same metadata snapshot (`Cluster` carries
            // the topic-ids map; Java reads `metadata.topicIds()` which is
            // a reference to the same snapshot map — the previous Rust
            // shape deep-cloned the whole `HashMap<String, Uuid>` per call).
            let topic_id = cluster.topic_id(partition.topic());
            let partition_data = PartitionData::new(
                topic_id,
                position_offset,
                INVALID_LOG_START_OFFSET,
                self.fetch_config.fetch_size,
                position_leader_epoch,
            );
            fetchable_partitions_by_node
                .entry(node_id)
                .or_default()
                .insert(partition.clone(), partition_data);

            debug!(
                "Added {} fetch request for partition {partition} at offset {position_offset} to node {}",
                self.fetch_config.isolation_level, node_id
            );
        }

        // Release the SubscriptionState lock before building the session
        // handlers (they only touch `self`).
        drop(guard);

        // Now build the session-handler builders from the per-node
        // partition maps and produce the final `FetchSessionRequestData`.
        let mut out: HashMap<i32, (Node, FetchSessionRequestData)> = HashMap::new();
        for (node_id, partitions) in fetchable_partitions_by_node {
            let node = node_targets
                .remove(&node_id)
                .expect("node was inserted alongside the partition map");
            let handler = self.session_handler_or_create(node_id);
            let mut builder = handler.new_builder();
            for (tp, pd) in partitions {
                builder.add(tp, pd);
            }
            let request_data = handler.build_request(builder);
            out.insert(node_id, (node, request_data));
        }
        Ok(out)
    }

    /// Java's `Set<Integer> bufferedNodes(Set<TopicPartition>, long)`.
    /// Java does not pass `isUnavailable` here either — callers check
    /// availability at the outer prepare-step.
    ///
    /// Phase 27 Fix #4: operates on the caller's already-held
    /// `SubscriptionState` guard instead of re-locking 2× per buffered
    /// partition, and only reads node *ids* (no `FetchPosition` / `Node`
    /// clones). The read-replica selection inlines Java's
    /// `maybeNodeForPosition` → `selectReadReplica` chain with identical
    /// behavior, including the empty-leader / stale-replica metadata-update
    /// side effects.
    fn compute_buffered_nodes(
        &self,
        guard: &mut crate::consumer::internals::subscription_state::SubscriptionState,
        buffered: &HashSet<TopicPartition>,
        current_time_ms: i64,
        cluster: &crate::common::Cluster,
    ) -> FxHashSet<i32> {
        let mut ids: FxHashSet<i32> = FxHashSet::default();
        for partition in buffered {
            if !guard.is_fetchable(partition) {
                continue;
            }
            // Java's `maybeNodeForPosition` empty-position / empty-leader
            // arms. Only the leader id (Copy) is read from the borrow.
            let leader_id = match guard.position(partition) {
                Ok(Some(p)) => match p.current_leader.leader.as_ref() {
                    Some(leader) => leader.id(),
                    None => {
                        debug!(
                            "Requesting metadata update for partition {partition} since the position {p} is missing the current leader node"
                        );
                        self.metadata.metadata_arc().request_update(false);
                        continue;
                    },
                },
                _ => continue,
            };
            // Java's `selectReadReplica` (id-only — the buffered-nodes set
            // stores ids).
            let node_id = match guard.preferred_read_replica(partition, current_time_ms) {
                Some(replica_id) => {
                    if cluster.node_if_online(partition, replica_id).is_some() {
                        replica_id
                    } else {
                        trace!(
                            "Not fetching from {replica_id} for partition {partition} since it is marked offline or is missing from our metadata, using the leader instead"
                        );
                        // Stale metadata — clear preferred replica and
                        // request refresh (Java `FetchUtils.requestMetadataUpdate`,
                        // performed on the held guard).
                        self.metadata.metadata_arc().request_update(false);
                        guard.clear_preferred_read_replica(partition);
                        leader_id
                    }
                },
                None => leader_id,
            };
            ids.insert(node_id);
        }
        ids
    }

    /// Drains the pending-fetch set for testing visibility.
    #[cfg(test)]
    pub(crate) fn pending_fetch_node_ids(&self) -> FxHashSet<i32> {
        self.nodes_with_pending_fetch_requests.clone()
    }
}

impl Drop for AbstractFetch {
    fn drop(&mut self) {
        self.close();
    }
}

/// Helpful constants exposed for `FetchRequestManager`'s callers.
pub(crate) const ABSTRACT_FETCH_CONSUMER_REPLICA_ID: i32 = CONSUMER_REPLICA_ID;
pub(crate) const ABSTRACT_FETCH_INVALID_LOG_START_OFFSET: i64 = INVALID_LOG_START_OFFSET;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::IsolationLevel;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;

    fn make_fetch_config() -> FetchConfig {
        FetchConfig::new(
            1,
            50 * 1024 * 1024,
            500,
            1024 * 1024,
            500,
            true,
            "",
            IsolationLevel::ReadUncommitted,
        )
    }

    fn make_subscriptions() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)))
    }

    fn make_consumer_metadata(subs: Arc<Mutex<SubscriptionState>>) -> Arc<ConsumerMetadata> {
        Arc::new(ConsumerMetadata::new(
            50,
            50,
            50_000,
            false,
            false,
            subs,
            ClusterResourceListeners::new(),
        ))
    }

    fn make_abstract_fetch() -> AbstractFetch {
        let subs = make_subscriptions();
        let metadata = make_consumer_metadata(subs.clone());
        AbstractFetch::new(
            metadata,
            subs,
            make_fetch_config(),
            Arc::new(FetchBuffer::new()),
            Arc::new(BufferSupplier::create()),
            Arc::new(crate::api_versions::ApiVersions::new()),
            FetchMetricsManager::for_test(),
        )
    }

    #[test]
    fn test_initial_state_is_empty() {
        let af = make_abstract_fetch();
        assert!(!af.has_completed_fetches());
        assert!(!af.has_available_fetches());
        assert!(af.pending_fetch_node_ids().is_empty());
        assert!(af.session_handler(1).is_none());
    }

    #[test]
    fn test_session_handler_or_create_is_idempotent() {
        let mut af = make_abstract_fetch();
        let _ = af.session_handler_or_create(1);
        let _ = af.session_handler_or_create(1);
        assert!(af.session_handler(1).is_some());
        // The handler exists exactly once.
        let _ = af.session_handler_or_create(2);
        assert!(af.session_handler(2).is_some());
        assert!(af.session_handler(1).is_some());
    }

    #[test]
    fn test_close_session_handler_removes() {
        let mut af = make_abstract_fetch();
        let _ = af.session_handler_or_create(7);
        assert!(af.session_handler(7).is_some());
        af.close_session_handler(7);
        assert!(af.session_handler(7).is_none());
        // Calling again is a no-op.
        af.close_session_handler(7);
    }

    #[test]
    fn test_remove_pending_fetch_request() {
        let mut af = make_abstract_fetch();
        af.nodes_with_pending_fetch_requests.insert(3);
        af.nodes_with_pending_fetch_requests.insert(4);
        let node = Node::new(3, "h".to_string(), 9092);
        af.remove_pending_fetch_request(&node, 100);
        assert!(!af.pending_fetch_node_ids().contains(&3));
        assert!(af.pending_fetch_node_ids().contains(&4));
    }

    #[test]
    fn test_prepare_close_fetch_session_requests_marks_each_session() {
        let mut af = make_abstract_fetch();
        let _ = af.session_handler_or_create(1);
        let _ = af.session_handler_or_create(2);

        let mut nodes = HashMap::new();
        nodes.insert(1, Node::new(1, "h1".to_string(), 9092));
        // Skip node 2 from the resolved-nodes map — Java would skip it.

        let out = af.prepare_close_fetch_session_requests(&nodes);
        assert_eq!(1, out.len());
        assert!(out.contains_key(&1));
        assert!(!out.contains_key(&2));
    }

    #[test]
    fn test_create_fetch_request_propagates_config() {
        let mut af = make_abstract_fetch();
        let fetch_target = Node::new(1, "host".to_string(), 9092);
        let _ = af.session_handler_or_create(1);

        // Build a no-op session request via the handler.
        let handler = af.session_handler_mut(1).expect("handler");
        let builder = handler.new_builder();
        let request_data = handler.build_request(builder);

        let req_builder = af.create_fetch_request(&fetch_target, &request_data);
        let req = req_builder.build();
        assert_eq!(500, req.max_wait());
        assert_eq!(1, req.min_bytes());
        assert!(af.pending_fetch_node_ids().contains(&1));
    }

    #[test]
    fn test_close_is_idempotent() {
        let mut af = make_abstract_fetch();
        af.close();
        assert!(af.closed);
        af.close(); // no panic
    }

    /// `handle_close_fetch_session_success` removes the node from the
    /// pending-fetch set.
    #[test]
    fn test_handle_close_fetch_session_success_drops_pending() {
        let mut af = make_abstract_fetch();
        af.nodes_with_pending_fetch_requests.insert(5);
        let _ = af.session_handler_or_create(5);
        let handler = af.session_handler_mut(5).expect("handler");
        let builder = handler.new_builder();
        let request_data = handler.build_request(builder);

        let node = Node::new(5, "host".to_string(), 9092);
        af.handle_close_fetch_session_success(&node, &request_data);
        assert!(!af.pending_fetch_node_ids().contains(&5));
    }

    /// `handle_close_fetch_session_failure` removes the node from the
    /// pending-fetch set.
    #[test]
    fn test_handle_close_fetch_session_failure_drops_pending() {
        let mut af = make_abstract_fetch();
        af.nodes_with_pending_fetch_requests.insert(6);
        let _ = af.session_handler_or_create(6);
        let handler = af.session_handler_mut(6).expect("handler");
        let builder = handler.new_builder();
        let request_data = handler.build_request(builder);

        let node = Node::new(6, "host".to_string(), 9092);
        let err = crate::common::Error::local_illegal_state("simulated");
        af.handle_close_fetch_session_failure(&node, &request_data, &err);
        assert!(!af.pending_fetch_node_ids().contains(&6));
    }

    /// `prepare_fetch_requests` returns an empty map when no partitions
    /// are assigned (nothing to fetch). Mirrors the short-circuit at
    /// Java `AbstractFetch.java:430-432`.
    #[test]
    fn test_prepare_fetch_requests_empty_assignment_returns_empty_map() {
        let mut af = make_abstract_fetch();
        let always_available = |_: &Node| false;
        let no_auth_err = |_: &Node| Ok(());
        let result = af.prepare_fetch_requests(100, always_available, no_auth_err);
        assert!(result.unwrap().is_empty());
    }

    /// `prepare_fetch_requests` returns an empty map when every
    /// fetchable partition's leader is already in
    /// `nodes_with_pending_fetch_requests`. The closure is called for
    /// each fetchable partition's resolved node; if it's pending, the
    /// partition is skipped. Verified indirectly via the empty-fetchable
    /// path here — Phase 10's FetchRequestManagerTest with MockClient
    /// will cover the rich cluster-aware cases.
    #[test]
    fn test_prepare_fetch_requests_returns_empty_when_nothing_fetchable() {
        // With no fetchable partitions, the pending-set should be
        // untouched and the result empty regardless of closure behavior.
        let mut af = make_abstract_fetch();
        af.nodes_with_pending_fetch_requests.insert(42);
        let always_available = |_: &Node| false;
        let no_auth_err = |_: &Node| Ok(());
        let result = af.prepare_fetch_requests(100, always_available, no_auth_err);
        assert!(result.unwrap().is_empty());
        // Pending-set untouched.
        assert!(af.pending_fetch_node_ids().contains(&42));
    }

    // ── Phase 26 (Fix #2): up-front skip when no node is fetchable ──────────

    /// Bootstrap the consumer metadata with `num_nodes` brokers (ids 0..N) and a
    /// single topic so `cluster.nodes()` is non-empty. Mirrors the
    /// `bootstrap_metadata_with_topic` helper used by the offsets-manager tests.
    fn bootstrap_nodes(metadata: &ConsumerMetadata, topic: &str, num_nodes: i32, num_partitions: i32) {
        metadata.add_transient_topics(HashSet::from([topic.to_string()]));
        let mut counts = HashMap::new();
        counts.insert(topic.to_string(), num_partitions);
        let response = crate::common::requests::request_test_utils::metadata_update_with(num_nodes, &counts);
        metadata.metadata_arc().update_with_current_request_version(&response, false, 0);
    }

    /// Fix #2 — empty cluster (no metadata yet, zero nodes) returns an empty map.
    /// Matches stock Java's `0 == 0` -> empty: nothing to fetch.
    #[test]
    fn test_prepare_fetch_requests_empty_cluster_returns_empty() {
        let mut af = make_abstract_fetch();
        // No metadata bootstrap -> cluster.nodes() is empty.
        let always_available = |_: &Node| false;
        let no_auth_err = |_: &Node| Ok(());
        let result = af.prepare_fetch_requests(100, always_available, no_auth_err);
        assert!(result.unwrap().is_empty(), "empty cluster must yield an empty fetch map");
    }

    /// Fix #2 — when EVERY cluster node is already in
    /// `nodes_with_pending_fetch_requests`, the up-front short-circuit returns an
    /// empty map WITHOUT running the expensive fetchable scan.
    ///
    /// NOTE (Phase M3): this test previously POISONED the SubscriptionState mutex
    /// to prove the short-circuit ran before any subscription lock. That premise
    /// no longer holds: faithful to Java's `prepareFetchRequests`
    /// (`AbstractFetch.java:423`), the FIRST statement is now
    /// `metricsManager.maybeUpdateAssignment(subscriptions)`, which reads
    /// `subscription.assignmentId()` (a lock). The Phase-26 short-circuit still
    /// avoids the EXPENSIVE per-partition fetchable scan — it just no longer
    /// skips the cheap assignment-id read. We therefore assert the functional
    /// short-circuit (empty map, pending-set untouched) instead of lock-skipping.
    #[test]
    fn test_prepare_fetch_requests_all_nodes_pending_skips_fetchable_scan() {
        let subs = make_subscriptions();
        let metadata = make_consumer_metadata(subs.clone());
        bootstrap_nodes(&metadata, "topic-a", 1, 1);
        let mut af = AbstractFetch::new(
            metadata,
            subs.clone(),
            make_fetch_config(),
            Arc::new(FetchBuffer::new()),
            Arc::new(BufferSupplier::create()),
            Arc::new(crate::api_versions::ApiVersions::new()),
            FetchMetricsManager::for_test(),
        );
        // The only node (id 0) has an in-flight fetch.
        af.nodes_with_pending_fetch_requests.insert(0);

        let always_available = |_: &Node| false;
        let no_auth_err = |_: &Node| Ok(());
        // All nodes pending: short-circuit to an empty map.
        let result = af.prepare_fetch_requests(100, always_available, no_auth_err);
        assert!(
            result.unwrap().is_empty(),
            "all nodes pending must short-circuit to an empty map"
        );
        // Pending-set untouched.
        assert!(af.pending_fetch_node_ids().contains(&0));
    }

    /// Fix #2 — when every cluster node is `is_unavailable`, the short-circuit
    /// returns empty (no node can be fetched right now). Also a freshness check:
    /// it is a pure short-circuit, not a cache.
    #[test]
    fn test_prepare_fetch_requests_all_nodes_unavailable_returns_empty() {
        let subs = make_subscriptions();
        let metadata = make_consumer_metadata(subs.clone());
        bootstrap_nodes(&metadata, "topic-a", 2, 2);
        let mut af = AbstractFetch::new(
            metadata,
            subs,
            make_fetch_config(),
            Arc::new(FetchBuffer::new()),
            Arc::new(BufferSupplier::create()),
            Arc::new(crate::api_versions::ApiVersions::new()),
            FetchMetricsManager::for_test(),
        );
        // No node is pending, but every node is unavailable.
        let all_unavailable = |_: &Node| true;
        let no_auth_err = |_: &Node| Ok(());
        let result = af.prepare_fetch_requests(100, all_unavailable, no_auth_err);
        assert!(
            result.unwrap().is_empty(),
            "all nodes unavailable must short-circuit to an empty map"
        );
    }

    /// Fix #2 — no partition is stranded: once a node is freed from the pending
    /// set (a fetch response arrived), the very next `prepare_fetch_requests`
    /// call passes the up-front short-circuit and issues a fetch for that node's
    /// partition. Proves the short-circuit is stateless (not a stale cache).
    #[test]
    fn test_prepare_fetch_requests_freeing_node_issues_fetch_next_call() {
        let subs = make_subscriptions();
        let metadata = make_consumer_metadata(subs.clone());
        bootstrap_nodes(&metadata, "topic-a", 1, 1);
        let mut af = AbstractFetch::new(
            metadata,
            subs.clone(),
            make_fetch_config(),
            Arc::new(FetchBuffer::new()),
            Arc::new(BufferSupplier::create()),
            Arc::new(crate::api_versions::ApiVersions::new()),
            FetchMetricsManager::for_test(),
        );

        // Assign + seek the partition with a validated position whose leader is
        // node 0 (the bootstrapped broker), so it resolves to a fetch target.
        let partition = TopicPartition::new("topic-a", 0);
        let leader = Node::new(0, "localhost".to_string(), 1969);
        {
            let mut guard = subs.lock().expect("lock");
            let mut set: HashSet<TopicPartition> = HashSet::new();
            set.insert(partition.clone());
            guard.assign_from_user(set).unwrap();
            let position = crate::consumer::internals::subscription_state::FetchPosition::with_leader(
                0,
                Some(0),
                crate::metadata::LeaderAndEpoch::new(Some(leader), Some(0)),
            );
            guard.seek_validated(&partition, position).unwrap();
        }

        let always_available = |_: &Node| false;
        let no_auth_err = |_: &Node| Ok(());

        // Node 0 has an in-flight fetch -> the up-front short-circuit returns
        // empty (the only node is pending).
        af.nodes_with_pending_fetch_requests.insert(0);
        let pending_result = af.prepare_fetch_requests(100, always_available, no_auth_err);
        assert!(
            pending_result.unwrap().is_empty(),
            "with node 0 pending, the short-circuit must return empty"
        );

        // Free node 0 (a fetch response arrived). The next call must NOT be
        // stranded: it issues a fetch for the partition on node 0.
        af.nodes_with_pending_fetch_requests.remove(&0);
        let freed_result = af.prepare_fetch_requests(100, always_available, no_auth_err).unwrap();
        assert!(
            freed_result.contains_key(&0),
            "freeing node 0 must let the next call issue a fetch for its partition (no stall)"
        );
    }

    /// `compute_buffered_nodes` returns an empty set when the buffered
    /// set is empty (smoke test against the cluster-snapshot path).
    #[test]
    fn test_compute_buffered_nodes_empty_set() {
        let af = make_abstract_fetch();
        let cluster = af.metadata.metadata_arc().fetch();
        let mut guard = af.subscriptions.lock().expect("SubscriptionState mutex poisoned");
        let result = af.compute_buffered_nodes(&mut guard, &HashSet::new(), 0, &cluster);
        assert!(result.is_empty());
    }

    // ── Phase 20 Fix #2b: handle_fetch_success moves records, no copy ───────

    use crate::common::compress::Compression;
    use crate::common::record::TimestampType;
    use crate::common::record::internal::{MemoryRecords, SimpleRecord};
    use crate::common::requests::fetch_metadata::INVALID_SESSION_ID;
    use crate::common::serialization::Deserializer;
    use crate::consumer::internals::deserializers::Deserializers;
    use crate::consumer::internals::fetch_collector::{FetchCollector, SystemFetchCollectorTime};
    use crate::fetch_response_data::{FetchResponseData, FetchableTopicResponse, PartitionData as RespPartitionData};

    /// Minimal UTF-8 string deserializer for this test module.
    struct StringDeserializer;
    impl Deserializer<String> for StringDeserializer {
        fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, crate::common::Error> {
            Ok(String::from_utf8_lossy(data).into_owned())
        }
    }

    fn encode_records(starting_offset: i64, count: i32) -> Vec<u8> {
        let records: Vec<SimpleRecord> = (0..count)
            .map(|i| SimpleRecord::new(0, Some(b"key".to_vec()), Some(format!("value-{i}").into_bytes()), vec![]))
            .collect();
        let mr = MemoryRecords::with_records_at_offset(
            2,
            starting_offset,
            Compression::none(),
            TimestampType::CreateTime,
            &records,
        );
        mr.buffer().to_vec()
    }

    /// `handle_fetch_success` MOVES each `PartitionData` (and its owned
    /// record bytes) into the `CompletedFetch` — no clone/copy of the payload
    /// (§27). This drives a full fetch end-to-end: build a session for a
    /// partition, hand it a response carrying a real record batch by value,
    /// and verify the records survive the move (count, offsets, values).
    #[test]
    fn test_handle_fetch_success_moves_records_into_completed_fetch() {
        const COUNT: i32 = 10;
        let subs = make_subscriptions();
        let metadata = make_consumer_metadata(subs.clone());
        let fetch_buffer = Arc::new(FetchBuffer::new());
        let mut af = AbstractFetch::new(
            metadata.clone(),
            subs.clone(),
            make_fetch_config(),
            fetch_buffer.clone(),
            Arc::new(BufferSupplier::create()),
            Arc::new(crate::api_versions::ApiVersions::new()),
            FetchMetricsManager::for_test(),
        );

        let partition = TopicPartition::new("topic-a", 0);
        // Assign + seek so the subscription has a fetch position for the
        // CompletedFetch's offset bookkeeping.
        {
            let mut guard = subs.lock().expect("lock");
            let mut set: HashSet<TopicPartition> = HashSet::new();
            set.insert(partition.clone());
            guard.assign_from_user(set).unwrap();
            guard.seek(&partition, 0).unwrap();
        }

        let node = Node::new(1, "host".to_string(), 9092);
        // Build a full-fetch session containing exactly this partition.
        let handler = af.session_handler_or_create(node.id());
        let mut builder = handler.new_builder();
        builder.add(
            partition.clone(),
            PartitionData::new(
                crate::common::Uuid::ZERO_UUID,
                0,
                INVALID_LOG_START_OFFSET,
                1024 * 1024,
                Some(0),
            ),
        );
        let request_data = handler.build_request(builder);
        af.nodes_with_pending_fetch_requests.insert(node.id());

        // Build the matching v12 fetch response carrying a real record batch.
        let records_bytes = encode_records(0, COUNT);
        let records_len = records_bytes.len();
        let mut rpd = RespPartitionData::new();
        rpd.set_partition_index(0);
        rpd.set_high_watermark(COUNT as i64);
        rpd.set_records(Some(bytes::Bytes::from(records_bytes)));
        let mut topic_resp = FetchableTopicResponse::new();
        topic_resp.set_topic("topic-a".to_string());
        topic_resp.set_partitions(vec![rpd]);
        let mut data = FetchResponseData::new();
        data.set_session_id(INVALID_SESSION_ID);
        data.set_responses(vec![topic_resp]);
        let response = FetchResponse::new(data);
        assert_eq!(
            records_len,
            response.data().responses[0].partitions[0].records.as_ref().unwrap().len()
        );

        // Pass the FetchResponse BY VALUE — its records must move (no copy).
        af.handle_fetch_success(&node, &request_data, response, 12, 0);

        // A CompletedFetch landed in the buffer; the pending request cleared.
        assert!(af.has_completed_fetches(), "expected a CompletedFetch in the buffer");
        assert!(!af.pending_fetch_node_ids().contains(&node.id()));

        // Decode through FetchCollector to prove the moved bytes are intact:
        // the records decode to the expected count, offsets, and values.
        let deserializers: Arc<Deserializers<String, String>> =
            Arc::new(Deserializers::new(Box::new(StringDeserializer), Box::new(StringDeserializer)));
        let collector = FetchCollector::new(
            metadata,
            subs,
            make_fetch_config(),
            deserializers,
            FetchMetricsManager::for_test(),
            Arc::new(SystemFetchCollectorTime),
        );
        let fetch = collector.collect_fetch(&fetch_buffer).unwrap();
        assert_eq!(COUNT as usize, fetch.count(), "all moved records must survive the move");

        let recs = fetch.records_partition(&partition);
        assert_eq!(COUNT as usize, recs.len());
        for (i, rec) in recs.iter().enumerate() {
            assert_eq!(i as i64, rec.offset(), "offset ordering preserved");
            assert_eq!(format!("value-{i}"), *rec.value().expect("value present"));
        }
    }

    /// §27 allocation-budget guard for the `handle_fetch_success` path
    /// (Phase 20 Fix #2b): the per-partition record payload must be MOVED
    /// into the `CompletedFetch`, never cloned. The pre-Phase-20 code did
    /// `response.response_data(...)` (clone of every `PartitionData`,
    /// including its `records: Option<Vec<u8>>`) plus `partition.clone()`.
    ///
    /// We hand `handle_fetch_success` a response with many partitions, each
    /// carrying a large (16 KiB) record buffer, and assert the allocation
    /// count over the move call stays within a tight per-partition budget. A
    /// re-introduced per-partition `Vec<u8>` payload clone would add one heap
    /// allocation per partition (the cloned records buffer), pushing the
    /// count past the budget. Crucially the budget does NOT scale with the
    /// payload SIZE — proving the bytes are not copied.
    #[test]
    fn test_handle_fetch_success_does_not_copy_payload() {
        const PARTITIONS: i32 = 8;
        const PAYLOAD_BYTES: usize = 16 * 1024;
        let subs = make_subscriptions();
        let metadata = make_consumer_metadata(subs.clone());
        let fetch_buffer = Arc::new(FetchBuffer::new());
        let mut af = AbstractFetch::new(
            metadata,
            subs.clone(),
            make_fetch_config(),
            fetch_buffer,
            Arc::new(BufferSupplier::create()),
            Arc::new(crate::api_versions::ApiVersions::new()),
            FetchMetricsManager::for_test(),
        );

        let node = Node::new(1, "host".to_string(), 9092);
        let handler = af.session_handler_or_create(node.id());
        let mut builder = handler.new_builder();
        {
            let mut guard = subs.lock().expect("lock");
            let mut set: HashSet<TopicPartition> = HashSet::new();
            for p in 0..PARTITIONS {
                set.insert(TopicPartition::new("topic-a", p));
            }
            guard.assign_from_user(set).unwrap();
            for p in 0..PARTITIONS {
                guard.seek(&TopicPartition::new("topic-a", p), 0).unwrap();
            }
        }
        for p in 0..PARTITIONS {
            builder.add(
                TopicPartition::new("topic-a", p),
                PartitionData::new(
                    crate::common::Uuid::ZERO_UUID,
                    0,
                    INVALID_LOG_START_OFFSET,
                    1024 * 1024,
                    Some(0),
                ),
            );
        }
        let request_data = handler.build_request(builder);
        af.nodes_with_pending_fetch_requests.insert(node.id());

        // One topic with PARTITIONS partitions, each carrying a real batch
        // padded to PAYLOAD_BYTES so any payload copy would be unmistakable.
        let mut partitions = Vec::new();
        for p in 0..PARTITIONS {
            let mut bytes = encode_records(0, 4);
            bytes.resize(PAYLOAD_BYTES.max(bytes.len()), 0);
            let mut rpd = RespPartitionData::new();
            rpd.set_partition_index(p);
            rpd.set_high_watermark(4);
            rpd.set_records(Some(bytes::Bytes::from(bytes)));
            partitions.push(rpd);
        }
        let mut topic_resp = FetchableTopicResponse::new();
        topic_resp.set_topic("topic-a".to_string());
        topic_resp.set_partitions(partitions);
        let mut data = FetchResponseData::new();
        data.set_session_id(INVALID_SESSION_ID);
        data.set_responses(vec![topic_resp]);
        let response = FetchResponse::new(data);

        // Budget: a small constant per partition for CompletedFetch
        // construction bookkeeping. The move path measures ~55 allocs for 8
        // partitions; a re-added per-partition payload Vec<u8> clone (the
        // pre-Phase-20 `response_data` instead of `into_response_data`) adds
        // exactly one heap allocation per partition (the cloned records
        // buffer). The budget is set tight enough that the clone breaks it but
        // the move passes, and crucially does NOT scale with the payload SIZE
        // (proving no byte copy).
        //
        // Phase M3: the per-response `FetchMetricsAggregator` adds a small,
        // FIXED-per-fetch cost — the `partitions` HashSet clones each
        // TopicPartition once (Java: `new HashSet<>(responseData.keySet())`),
        // plus one aggregator allocation. This is per-fetch / per-partition,
        // NOT per-record (the 4 records/partition do not each allocate — the
        // per-record loop is pure `records_read += 1`). The per-partition
        // budget rises by 1 to cover the aggregator's tracked TopicPartition
        // clone; a payload byte-clone would still add a SECOND alloc/partition
        // and break the budget.
        const PER_PARTITION_BUDGET: usize = 8;
        const OVERHEAD_BUDGET: usize = 3;

        let alloc_count;
        {
            let _guard = crate::test_alloc_tracker::AllocTrackingGuard::new();
            crate::test_alloc_tracker::AllocTrackingGuard::reset();
            af.handle_fetch_success(&node, &request_data, response, 12, 0);
            alloc_count = crate::test_alloc_tracker::AllocTrackingGuard::count();
        }

        let max_allowed = OVERHEAD_BUDGET + PER_PARTITION_BUDGET * (PARTITIONS as usize);
        assert!(
            alloc_count <= max_allowed,
            "handle_fetch_success payload-copy regression: {alloc_count} allocs for {PARTITIONS} partitions \
             (budget {max_allowed}). A per-partition records Vec<u8> clone (response_data instead of \
             into_response_data) would exceed this (consumer-threading.md §27)."
        );
        assert!(af.has_completed_fetches(), "expected CompletedFetch entries in the buffer");
        eprintln!(
            "§27 handle_fetch_success budget: {alloc_count} allocs for {PARTITIONS} partitions \
             of {PAYLOAD_BYTES}-byte payloads (max allowed {max_allowed})"
        );
    }
}
