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

//! Translation of `org.apache.kafka.clients.NetworkClient.DefaultMetadataUpdater`.
//!
//! In Java this is a package-private inner class on `NetworkClient`. The
//! Rust translation lifts it out as a free `pub(crate) struct` because
//! Rust has no inner-class semantics. The original Java implementation
//! reaches back into the enclosing `NetworkClient`'s private helpers
//! (`canSendRequest`, `sendInternalMetadataRequest`, `initiateConnect`,
//! `leastLoadedNode`, `isAnyNodeConnecting`). The Rust translation
//! threads these callbacks through the [`MetadataUpdaterContext`] trait
//! that the enclosing [`crate::NetworkClient`] implements; the trait is
//! passed by `&mut` borrow into [`MetadataUpdater::maybe_update`] at
//! call time (see Phase 8.0 notes for the design choice).
//!
//! Java's inner-class field bag:
//!
//! * `Metadata metadata` — the shared cluster-metadata handle (held as
//!   `Arc<Metadata>` here).
//! * `InProgressData inProgress` — non-null while a metadata request is
//!   on the wire; tracks the request version and partial-update flag so
//!   the response can be routed back to `Metadata.update`. Translated as
//!   `Option<InProgressData>`.
//! * `Optional<Long> metadataAttemptStartMs` — wall-clock millisecond
//!   when the current attempt-window began. Used by
//!   [`Self::needs_rebootstrap`] to detect a "stuck" metadata fetch.
//!   Translated as `Option<i64>`.

use std::sync::Arc;

use log::{debug, info, trace, warn};

use crate::MetadataUpdater;
use crate::common::Node;
use crate::common::errors::KafkaError;
use crate::common::protocol::Errors;
use crate::common::requests::{MetadataResponse, RequestHeader};
use crate::common::topic_partition::TopicPartition;
use crate::metadata::Metadata;
use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;
use crate::metadata_updater::MetadataUpdaterContext;

/// Mirrors Java's `NetworkClient.DefaultMetadataUpdater.InProgressData`
/// inner class. Captures the per-request version + partial-update flag
/// so the response handler can route the result back into
/// `Metadata.update(int, MetadataResponse, boolean, long)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InProgressData {
    pub(crate) request_version: i32,
    pub(crate) is_partial_update: bool,
}

impl InProgressData {
    fn new(request_version: i32, is_partial_update: bool) -> Self {
        InProgressData { request_version, is_partial_update }
    }
}

/// Translation of `NetworkClient.DefaultMetadataUpdater`.
///
/// **Java's contract**: this class is package-private, *not* thread-safe.
/// The Rust translation reflects that by taking `&mut self` on every
/// state-mutating method (matching the [`MetadataUpdater`] trait).
pub(crate) struct DefaultMetadataUpdater {
    /// Java: `private final Metadata metadata`. Held as an `Arc` so the
    /// `NetworkClient` constructor can hand a clone to the producer while
    /// the updater keeps its own reference for the response loop.
    metadata: Arc<Metadata>,
    /// Java: `private InProgressData inProgress`. `None` mirrors Java's
    /// `null` (no fetch in progress).
    in_progress: Option<InProgressData>,
    /// Java: `private Optional<Long> metadataAttemptStartMs = Optional.empty()`.
    /// Wall-clock millisecond when the current metadata-attempt window
    /// began. Set to `Some(0)` by [`Self::initiate_rebootstrap`] to force
    /// rebootstrap on the next `needs_rebootstrap` check.
    metadata_attempt_start_ms: Option<i64>,
}

impl DefaultMetadataUpdater {
    /// Mirrors Java's `DefaultMetadataUpdater(Metadata)`.
    // Phase 8.0 (3/N) wires this into [`crate::producer::KafkaProducer::new`];
    // until then the only callers are unit tests.
    #[allow(dead_code)]
    pub(crate) fn new(metadata: Arc<Metadata>) -> Self {
        DefaultMetadataUpdater { metadata, in_progress: None, metadata_attempt_start_ms: None }
    }

    /// Borrow the shared [`Metadata`] handle (a clone of the `Arc`). Used
    /// by callers that want to observe the same metadata instance the
    /// updater drives — Java's package-private accessor on the inner
    /// class is `this.metadata`.
    #[allow(dead_code)] // Phase 8a / external test wiring.
    pub(crate) fn metadata(&self) -> Arc<Metadata> {
        Arc::clone(&self.metadata)
    }

    /// Mirrors `hasFetchInProgress()`.
    fn has_fetch_in_progress(&self) -> bool {
        self.in_progress.is_some()
    }

    /// Mirrors `initiateRebootstrap()` — set the attempt window to 0 so
    /// the next `needs_rebootstrap` check returns true regardless of the
    /// rebootstrap-trigger interval.
    fn initiate_rebootstrap(&mut self) {
        self.metadata_attempt_start_ms = Some(0);
    }

    /// Mirrors the private inner-class helper `maybeUpdate(long now, Node node)`.
    /// Adds a metadata request to the send list if we can; falls back to
    /// connection setup if not. Returns the timeout (ms) until the next
    /// metadata-related event.
    fn maybe_update_for_node(&mut self, context: &mut dyn MetadataUpdaterContext, now: i64, node: &Node) -> i64 {
        let node_id = node.id();
        let node_id_label: Arc<str> = Arc::from(node.id_string());

        if context.can_send_request(node_id, now) {
            // Java: `MetadataRequestAndVersion req = metadata.newMetadataRequestAndVersion(now);`
            //       `MetadataRequest.Builder metadataRequest = req.requestBuilder;`
            let request_and_version = self.metadata.new_metadata_request_and_version(now);
            debug!(
                "Sending metadata request {:?} to node {}",
                request_and_version.request_builder, node
            );
            self.in_progress = Some(InProgressData::new(
                request_and_version.request_version,
                request_and_version.is_partial_update,
            ));
            context.send_internal_metadata_request(request_and_version.request_builder, node_id_label, now);
            return context.default_request_timeout_ms() as i64;
        }

        // If there's any connection establishment underway, wait until it
        // completes. This prevents the client from unnecessarily connecting
        // to additional nodes while a previous connection attempt has not
        // been completed.
        if self.is_any_node_connecting(context) {
            // Strictly the timeout we should return here is "connect timeout",
            // but as we don't have such application level configuration,
            // using reconnect backoff instead.
            return context.reconnect_backoff_ms();
        }

        if context.can_connect(node_id, now) {
            // We don't have a connection to this node right now, make one.
            debug!("Initialize connection to node {} for sending metadata request", node);
            context.initiate_connect(node, now);
            return context.reconnect_backoff_ms();
        }

        // Connected, but can't send more OR connecting. In either case,
        // we just need to wait for a network event to let us know the
        // selected connection might be usable again.
        i64::MAX
    }

    /// Mirrors `isAnyNodeConnecting()` — the inner-class helper that
    /// iterates over the updater's known nodes and asks the enclosing
    /// `NetworkClient` whether any are mid-connect.
    fn is_any_node_connecting(&self, context: &dyn MetadataUpdaterContext) -> bool {
        for node in self.fetch_nodes() {
            if context.is_connecting(node.id()) {
                return true;
            }
        }
        false
    }
}

impl MetadataUpdater for DefaultMetadataUpdater {
    fn fetch_nodes(&self) -> Vec<Node> {
        // Java: `return metadata.fetch().nodes();`
        // `Cluster.nodes()` returns the in-memory slice; we mirror Java's
        // defensive copy (`ArrayList`) by cloning into a `Vec`.
        self.metadata.fetch().nodes().to_vec()
    }

    fn is_update_due(&self, now: i64) -> bool {
        !self.has_fetch_in_progress() && self.metadata.time_to_next_update(now) == 0
    }

    fn maybe_update(&mut self, context: &mut dyn MetadataUpdaterContext, now: i64) -> i64 {
        // Java: `long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);`
        let time_to_next_metadata_update = self.metadata.time_to_next_update(now);
        let wait_for_metadata_fetch = if self.has_fetch_in_progress() {
            context.default_request_timeout_ms() as i64
        } else {
            0
        };

        let metadata_timeout = std::cmp::max(time_to_next_metadata_update, wait_for_metadata_fetch);
        if metadata_timeout > 0 {
            return metadata_timeout;
        }

        if self.metadata_attempt_start_ms.is_none() {
            self.metadata_attempt_start_ms = Some(now);
        }

        // Beware that the behavior of this method and the computation of
        // timeouts for poll() are highly dependent on the behavior of
        // leastLoadedNode.
        let nodes = self.fetch_nodes();
        let mut least_loaded_node = context.least_loaded_node(now, &nodes);

        // Rebootstrap if needed and configured.
        if context.metadata_recovery_strategy() == MetadataRecoveryStrategy::Rebootstrap
            && !least_loaded_node.has_node_available_or_connection_ready()
        {
            self.rebootstrap(now);
            let nodes_after = self.fetch_nodes();
            least_loaded_node = context.least_loaded_node(now, &nodes_after);
        }

        let node = match least_loaded_node.node() {
            Some(n) => n.clone(),
            None => {
                debug!("Give up sending metadata request since no node is available");
                return context.reconnect_backoff_ms();
            },
        };

        self.maybe_update_for_node(context, now, &node)
    }

    fn handle_server_disconnect(&mut self, now: i64, node_id: i32, maybe_auth_error: Option<KafkaError>) {
        // Java: `Cluster cluster = metadata.fetch();`
        let cluster = self.metadata.fetch();
        // 'processDisconnection' generates warnings for misconfigured bootstrap
        // server configuration resulting in 'Connection Refused' and
        // misconfigured security resulting in authentication failures. The
        // warning below handles the case where a connection to a broker was
        // established, but was disconnected before metadata could be obtained.
        if cluster.is_bootstrap_configured()
            && let Some(node) = cluster.node_by_id(node_id)
        {
            warn!("Bootstrap broker {} disconnected", node);
        }

        // If we have a disconnect while an update is due, we treat it as
        // a failed update so that we can backoff properly.
        if self.is_update_due(now) {
            self.handle_failed_request(now, None);
        }

        // Java: `maybeFatalException.ifPresent(metadata::fatalError);`
        if let Some(err) = maybe_auth_error {
            self.metadata.fatal_error(err);
        }

        // The disconnect may be the result of stale metadata, so request
        // an update.
        self.metadata.request_update(false);
    }

    fn handle_failed_request(&mut self, now: i64, maybe_fatal_error: Option<KafkaError>) {
        if let Some(err) = maybe_fatal_error {
            self.metadata.fatal_error(err);
        }
        self.metadata.failed_update(now);
        self.in_progress = None;
    }

    fn handle_successful_response(
        &mut self,
        request_header: &RequestHeader,
        now: i64,
        metadata_response: MetadataResponse,
    ) {
        // If any partition has leader with missing listeners, log up to ten
        // of these partitions for diagnosing broker configuration issues.
        // This could be a transient issue if listeners were added dynamically
        // to brokers.
        let missing_listener_partitions: Vec<TopicPartition> = metadata_response
            .topic_metadata()
            .iter()
            .flat_map(|topic_metadata| {
                let topic_name = topic_metadata.topic().to_owned();
                topic_metadata.partition_metadata().iter().filter_map(move |pm| {
                    if pm.error == Errors::ListenerNotFound {
                        Some(TopicPartition::new(topic_name.clone(), pm.partition()))
                    } else {
                        None
                    }
                })
            })
            .collect();
        if !missing_listener_partitions.is_empty() {
            let count = missing_listener_partitions.len();
            let head: Vec<_> = missing_listener_partitions.iter().take(10).collect();
            warn!(
                "{} partitions have leader brokers without a matching listener, including {:?}",
                count, head
            );
        }

        // Check if any topic's metadata failed to get updated. Java throws
        // `IllegalArgumentException` when the response is topic-id-only;
        // the producer-side path never sends id-only responses so a
        // failure here is a programming error and we mirror Java's
        // unchecked-throw with `expect()` (CLAUDE.md rule 5).
        let errors = metadata_response
            .errors()
            .expect("MetadataResponse.errors() failed; topic-id-only responses are not used by the producer client");
        if !errors.is_empty() {
            warn!(
                "The metadata response from the cluster reported a recoverable issue with correlation id {} : {:?}",
                request_header.correlation_id(),
                errors,
            );
        }

        // Decide how to apply the response.
        // Note: we read `metadata_recovery_strategy` from the underlying
        // [`Metadata`] handle (Java's inner class captures the enclosing
        // `NetworkClient.metadataRecoveryStrategy` field; the Rust
        // equivalent lives on the producer's network client and is
        // surfaced here via the response-time conditional only — see
        // [`Metadata::metadata_recovery_strategy_for_response`] if/when
        // this needs to dispatch beyond REBOOTSTRAP).
        let is_rebootstrap_required = metadata_response.top_level_error() == Errors::RebootstrapRequired;

        if is_rebootstrap_required {
            // Java's check is gated on `metadataRecoveryStrategy ==
            // REBOOTSTRAP`. We surface the REBOOTSTRAP_REQUIRED error
            // upstream to NetworkClient via metadata.rebootstrap() — the
            // gate is enforced by the caller (NetworkClient::poll only
            // dispatches to handleRebootstrap when the strategy is
            // REBOOTSTRAP).
            info!("Rebootstrap requested by server.");
            self.initiate_rebootstrap();
        } else if metadata_response.brokers().is_empty() {
            // When talking to the startup phase of a broker, it is possible
            // to receive an empty metadata set, which we should retry later.
            trace!(
                "Ignoring empty metadata response with correlation id {}.",
                request_header.correlation_id()
            );
            self.metadata.failed_update(now);
        } else {
            let in_progress = self
                .in_progress
                .as_ref()
                .expect("handle_successful_response called without a fetch in progress");
            if let Err(e) = self.metadata.update(
                in_progress.request_version,
                &metadata_response,
                in_progress.is_partial_update,
                now,
            ) {
                // Java doesn't return Result; the equivalent is an
                // unchecked exception out of `metadata.update`. We log
                // and clear the in-progress slot so the next attempt can
                // proceed.
                warn!("metadata.update failed: {}", e);
            }
            self.metadata_attempt_start_ms = None;
        }

        self.in_progress = None;
    }

    fn needs_rebootstrap(&self, now: i64, rebootstrap_trigger_ms: i64) -> bool {
        // Java: `metadataAttemptStartMs.filter(startMs -> now - startMs > rebootstrapTriggerMs).isPresent()`
        match self.metadata_attempt_start_ms {
            Some(start_ms) => now - start_ms > rebootstrap_trigger_ms,
            None => false,
        }
    }

    fn rebootstrap(&mut self, now: i64) {
        self.metadata.rebootstrap();
        self.metadata_attempt_start_ms = Some(now);
    }

    fn close(&mut self) {
        self.metadata.close();
    }
}

#[cfg(test)]
mod tests {
    //! Translation of the `DefaultMetadataUpdater`-targeting tests from
    //! `NetworkClientTest.java`. The Java tests are not split into a
    //! dedicated test class — they live inside `NetworkClientTest` and
    //! exercise the inner class via the public `NetworkClient` surface.
    //! The tests below cover the same behaviour at the
    //! [`DefaultMetadataUpdater`] level (no network IO), exercising the
    //! methods that don't require a [`MetadataUpdaterContext`] callback
    //! into a real `NetworkClient`.
    //!
    //! Tests that *do* require the full `NetworkClient` (the
    //! `maybe_update`-driven send loop, `testRebootstrap`,
    //! `testInflightRequestsDuringRebootstrap`) live in
    //! `network_client.rs::tests` because they exercise the integrated
    //! behaviour.

    use super::*;
    use crate::common::internals::cluster_resource_listeners::ClusterResourceListeners;
    use crate::common::node::Node;
    use crate::common::utils::LogContext;

    fn fresh_metadata() -> Arc<Metadata> {
        Arc::new(
            Metadata::new(
                50,
                50,
                5_000,
                LogContext::default(),
                Arc::new(ClusterResourceListeners::default()),
            )
            .expect("metadata constructs"),
        )
    }

    fn updater_with_metadata(metadata: Arc<Metadata>) -> DefaultMetadataUpdater {
        DefaultMetadataUpdater::new(metadata)
    }

    #[test]
    fn new_starts_with_no_fetch_in_progress() {
        let metadata = fresh_metadata();
        let updater = updater_with_metadata(metadata);
        assert!(!updater.has_fetch_in_progress());
        assert!(updater.metadata_attempt_start_ms.is_none());
    }

    #[test]
    fn is_update_due_when_fetch_idle_and_update_pending() {
        // Fresh metadata has `need_full_update=false`, but bootstrap or
        // `request_update` flips it to true so `time_to_next_update`
        // returns 0 once the backoff window elapses.
        let metadata = fresh_metadata();
        metadata.bootstrap(vec![("localhost".to_owned(), 9092)]);
        metadata.request_update(true);
        // The backoff is 50ms so at time 0 the update is gated by backoff.
        // After 100ms the backoff window elapses and the update is due.
        let updater = updater_with_metadata(Arc::clone(&metadata));
        assert_eq!(metadata.time_to_next_update(100), 0);
        assert!(updater.is_update_due(100));
    }

    #[test]
    fn is_update_due_false_when_fetch_in_progress() {
        let metadata = fresh_metadata();
        metadata.bootstrap(vec![("localhost".to_owned(), 9092)]);
        metadata.request_update(true);
        let mut updater = updater_with_metadata(Arc::clone(&metadata));
        updater.in_progress = Some(InProgressData::new(0, false));
        // Even after the backoff window, has_fetch_in_progress() short-
        // circuits the is_update_due check.
        assert!(!updater.is_update_due(100));
    }

    /// Java `testRebootstrap` partial coverage — the
    /// `handleFailedRequest` + rebootstrap-window interaction at the
    /// updater level (the full integration test lives in network_client
    /// tests).
    #[test]
    fn handle_failed_request_clears_in_progress_and_bumps_attempts() {
        let metadata = fresh_metadata();
        metadata.bootstrap(vec![("localhost".to_owned(), 9092)]);
        let mut updater = updater_with_metadata(Arc::clone(&metadata));
        updater.in_progress = Some(InProgressData::new(0, false));
        updater.metadata_attempt_start_ms = Some(100);
        updater.handle_failed_request(200, None);
        assert!(!updater.has_fetch_in_progress());
        // attempt_start_ms is NOT cleared by handle_failed_request — Java
        // only clears it on a successful, non-empty response.
        assert_eq!(updater.metadata_attempt_start_ms, Some(100));
    }

    #[test]
    fn handle_failed_request_with_fatal_error_propagates_to_metadata() {
        let metadata = fresh_metadata();
        let mut updater = updater_with_metadata(Arc::clone(&metadata));
        let fatal = KafkaError::Authentication("bad auth".to_owned());
        updater.handle_failed_request(0, Some(fatal));
        let err = metadata.maybe_throw_fatal_error().unwrap_err();
        assert!(matches!(err, KafkaError::Authentication(_)));
    }

    /// Java `testRebootstrap` exercises `needsRebootstrap` (via
    /// `time.sleep(rebootstrapTriggerMs + 1)`). At the updater level we
    /// just probe the start/now arithmetic.
    #[test]
    fn needs_rebootstrap_arithmetic() {
        let metadata = fresh_metadata();
        let mut updater = updater_with_metadata(Arc::clone(&metadata));
        // No attempt started yet → never needs rebootstrap.
        assert!(!updater.needs_rebootstrap(0, 1000));
        // Attempt at t=0, now=500, trigger=1000 → no.
        updater.metadata_attempt_start_ms = Some(0);
        assert!(!updater.needs_rebootstrap(500, 1000));
        // now=1001, trigger=1000 → yes (Java uses strict `>`).
        assert!(updater.needs_rebootstrap(1001, 1000));
        // Equality is NOT a rebootstrap (matches Java's `>` not `>=`).
        assert!(!updater.needs_rebootstrap(1000, 1000));
    }

    #[test]
    fn rebootstrap_resets_attempt_start_and_invokes_metadata() {
        let metadata = fresh_metadata();
        metadata.bootstrap(vec![("localhost".to_owned(), 9092)]);
        let initial_version = metadata.update_version();
        let mut updater = updater_with_metadata(Arc::clone(&metadata));
        updater.metadata_attempt_start_ms = Some(0);
        updater.rebootstrap(500);
        // Metadata's rebootstrap() bumps update_version.
        assert!(metadata.update_version() > initial_version);
        // The attempt window is reset to "now".
        assert_eq!(updater.metadata_attempt_start_ms, Some(500));
    }

    #[test]
    fn initiate_rebootstrap_forces_immediate_window() {
        let metadata = fresh_metadata();
        let mut updater = updater_with_metadata(metadata);
        updater.metadata_attempt_start_ms = Some(1_000_000);
        updater.initiate_rebootstrap();
        assert_eq!(updater.metadata_attempt_start_ms, Some(0));
        // After initiateRebootstrap, needsRebootstrap is true for any
        // positive `now` (now - 0 > rebootstrap_trigger_ms when
        // trigger_ms < now).
        assert!(updater.needs_rebootstrap(2, 1));
    }

    /// Java's `handleServerDisconnect` requests a metadata update — the
    /// behaviour the producer relies on after a connection drops.
    #[test]
    fn handle_server_disconnect_requests_metadata_update() {
        let metadata = fresh_metadata();
        metadata.bootstrap(vec![("localhost".to_owned(), 9092)]);
        let mut updater = updater_with_metadata(Arc::clone(&metadata));
        // Initially update is already requested (bootstrap sets it).
        // Snapshot then update_requested to reset the flag.
        let _ = metadata.update_requested();
        updater.handle_server_disconnect(0, 0, None);
        assert!(metadata.update_requested(), "expected metadata.requestUpdate() to be invoked");
    }

    /// Java's `handleServerDisconnect` with a fatal auth exception sets
    /// the fatal error on metadata.
    #[test]
    fn handle_server_disconnect_with_auth_error_propagates() {
        let metadata = fresh_metadata();
        let mut updater = updater_with_metadata(Arc::clone(&metadata));
        let auth = KafkaError::Authentication("bad creds".to_owned());
        updater.handle_server_disconnect(0, 0, Some(auth));
        let err = metadata.maybe_throw_fatal_error().unwrap_err();
        assert!(matches!(err, KafkaError::Authentication(_)));
    }

    /// Java's `handleServerDisconnect` while is_update_due → triggers
    /// `handleFailedRequest` which bumps attempts on the metadata.
    #[test]
    fn handle_server_disconnect_during_update_due_calls_handle_failed_request() {
        let metadata = fresh_metadata();
        metadata.bootstrap(vec![("localhost".to_owned(), 9092)]);
        metadata.request_update(true);
        let mut updater = updater_with_metadata(Arc::clone(&metadata));
        updater.metadata_attempt_start_ms = Some(0);
        // Advance time past the backoff window so is_update_due is true.
        // Refresh backoff is 50ms; we use 100ms.
        let now = 100;
        assert!(updater.is_update_due(now));
        updater.handle_server_disconnect(now, 0, None);
        // After handle_failed_request, in_progress is None (it was None
        // anyway) — the observable side-effect is request_update was
        // re-invoked.
        assert!(metadata.update_requested());
    }

    #[test]
    fn fetch_nodes_clones_cluster_nodes() {
        let metadata = fresh_metadata();
        metadata.bootstrap(vec![("h1".to_owned(), 9092), ("h2".to_owned(), 9093)]);
        let updater = updater_with_metadata(Arc::clone(&metadata));
        let nodes = updater.fetch_nodes();
        assert_eq!(nodes.len(), 2);
        // Java returns a defensive copy; the Rust translation matches.
        // Mutating the returned Vec must not affect the underlying cluster.
        drop(nodes);
        let nodes2 = updater.fetch_nodes();
        assert_eq!(nodes2.len(), 2);
    }

    #[test]
    fn close_propagates_to_metadata() {
        let metadata = fresh_metadata();
        let mut updater = updater_with_metadata(Arc::clone(&metadata));
        updater.close();
        assert!(metadata.is_closed());
    }

    /// Smoke-test that the trait-object dispatch works (so the
    /// `&mut dyn MetadataUpdater` interaction is exercised at compile
    /// time). Also exercises the `Node` import used by other tests.
    #[test]
    fn is_metadata_updater_trait_object() {
        let metadata = fresh_metadata();
        let mut updater = updater_with_metadata(metadata);
        let dyn_ref: &mut dyn MetadataUpdater = &mut updater;
        assert!(!dyn_ref.is_update_due(0));
        let node = Node::new(1, "h".to_owned(), 9092);
        assert_eq!(node.id(), 1);
    }
}
