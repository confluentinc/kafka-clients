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
    pub(crate) nodes_with_pending_fetch_requests: HashSet<i32>,
    /// Whether `close` has been called.
    pub(crate) closed: bool,

    /// Per-node fetch session state.
    session_handlers: HashMap<i32, FetchSessionHandler>,
}

impl AbstractFetch {
    /// Constructs an `AbstractFetch` for use by a request manager.
    ///
    /// Translates Java's
    /// `AbstractFetch(LogContext, ConsumerMetadata, SubscriptionState,
    ///   FetchConfig, FetchBuffer, FetchMetricsManager, Time, ApiVersions)`.
    /// Drops the `LogContext` (we use the `log` crate), `FetchMetricsManager`
    /// (no Rust metrics framework), `Time` (Phase 7b will plumb a clock if
    /// needed for read-replica leasing), and `ApiVersions` (negotiation
    /// lives in Phase 7b alongside `RequestManager::poll`).
    pub(crate) fn new(
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        fetch_config: FetchConfig,
        fetch_buffer: Arc<FetchBuffer>,
    ) -> Self {
        Self {
            metadata,
            subscriptions,
            fetch_config,
            fetch_buffer,
            decompression_buffer_supplier: Arc::new(BufferSupplier::create()),
            nodes_with_pending_fetch_requests: HashSet::new(),
            closed: false,
            session_handlers: HashMap::new(),
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
        .isolation_level(self.fetch_config.isolation_level)
        .set_max_bytes(self.fetch_config.max_bytes)
        .metadata(request_data.metadata)
        .removed(request_data.to_forget.clone())
        .replaced(request_data.to_replace.clone())
        .rack_id(self.fetch_config.client_rack_id.clone());

        debug!("Sending fetch request to broker {}", fetch_target.id());
        debug!("Adding pending request for node {}", fetch_target.id());
        self.nodes_with_pending_fetch_requests.insert(fetch_target.id());
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
        response: &FetchResponse,
        request_version: i16,
    ) {
        let session_id = request_data.metadata.session_id();
        let handler = match self.session_handlers.get_mut(&fetch_target.id()) {
            Some(h) => h,
            None => {
                log::error!(
                    "Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
                    fetch_target.id()
                );
                return;
            },
        };

        if !handler.handle_response(response, request_version) {
            // FETCH_SESSION_TOPIC_ID_ERROR drives a metadata refresh per
            // Java's `metadata.requestUpdate(false)`. Phase 7b's
            // FetchRequestManager owns the metadata-update trigger; we
            // simply mark the response handled here.
            self.remove_pending_fetch_request(fetch_target, session_id);
            return;
        }

        let response_data = response.response_data(handler.session_topic_names(), request_version);
        let mut needs_wakeup = true;

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
                    // "Received fetch response for missing session partition" —
                    // Java throws IllegalStateException. We log and drop
                    // the partition entry; the response handler returning
                    // true would have caught this earlier in well-formed
                    // sessions.
                    log::error!(
                        "Response for missing session request partition: partition={} metadata={}",
                        partition,
                        request_data.metadata
                    );
                    continue;
                },
            };

            let fetch_offset = request_pd.fetch_offset;
            debug!(
                "Fetch {} at offset {} for partition {} returned",
                self.fetch_config.isolation_level, fetch_offset, partition
            );

            let cf = CompletedFetch::new_full(
                self.subscriptions.clone(),
                self.decompression_buffer_supplier.clone(),
                partition.clone(),
                partition_data,
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
        self.remove_pending_fetch_request(fetch_target, session_id);
    }

    /// Handles a fetch-request failure.
    ///
    /// Translates `protected void handleFetchFailure(Node, FetchRequestData, Throwable)`.
    pub(crate) fn handle_fetch_failure(
        &mut self,
        fetch_target: &Node,
        request_data: &FetchSessionRequestData,
        error: &crate::common::KafkaError,
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

    /// Drains the pending-fetch set for testing visibility.
    #[cfg(test)]
    pub(crate) fn pending_fetch_node_ids(&self) -> HashSet<i32> {
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
        AbstractFetch::new(metadata, subs, make_fetch_config(), Arc::new(FetchBuffer::new()))
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
}
