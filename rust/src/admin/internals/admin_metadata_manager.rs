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

//! Admin-specific cluster metadata holder.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.AdminMetadataManager`.

use std::sync::{Arc, Mutex};

use crate::MetadataUpdater;
use crate::common::requests::MetadataResponse;
use crate::common::requests::RequestHeader;
use crate::common::utils::LogContext;
use crate::common::{Cluster, Error, Node, Uuid};
use crate::{kafka_info, kafka_warn};
use std::collections::HashMap;

/// The metadata-refresh state machine, mirroring Java's `State` enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager$State")]
enum State {
    /// There is no in-flight metadata request and no need for one.
    Quiescent,
    /// A metadata request is desired but not yet in flight.
    UpdateRequested,
    /// A metadata request is in flight.
    UpdatePending,
}

/// Shared mutable state, guarded by a std `Mutex`. Both
/// [`AdminMetadataManager`] (owned by the background task) and
/// [`AdminMetadataUpdater`] (owned by the `NetworkClient`) hold an `Arc` to it,
/// mirroring how Java's inner-class updater shares the outer manager's fields.
struct Inner {
    state: State,
    /// The current cluster (initially [`Cluster::empty`]).
    cluster: Cluster,
    /// The bootstrap cluster, retained for rebootstrap.
    bootstrap_cluster: Cluster,
    /// The last time metadata was updated (epoch ms).
    last_metadata_update_ms: i64,
    /// The last time we attempted to fetch metadata (epoch ms).
    last_metadata_fetch_attempt_ms: i64,
    /// Java's `metadataAttemptStartMs`: the time (epoch ms) when we started
    /// attempts to fetch metadata. If `None`, metadata has not been requested.
    /// This is the start time based on which rebootstrap is triggered if metadata
    /// is not obtained for the configured rebootstrap trigger interval. Set to
    /// `Some(0)` to force rebootstrap immediately.
    metadata_attempt_start_ms: Option<i64>,
    /// A fatal (non-retriable) error to surface from `is_ready`.
    fatal_error: Option<Error>,
}

/// The Admin analog of `ConsumerMetadata` — tracks the current [`Cluster`], the
/// controller, readiness, and the metadata-fetch backoff/deadline state used by
/// the background task to decide when to issue a metadata refresh.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.internals.AdminMetadataManager`. It is
/// touched only from the background task; the internal `Mutex` exists solely to
/// share the cluster with the `NetworkClient`'s [`AdminMetadataUpdater`], not
/// for cross-task concurrency.
#[derive(Clone)]
#[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager")]
pub(crate) struct AdminMetadataManager {
    inner: Arc<Mutex<Inner>>,
    refresh_backoff_ms: i64,
    metadata_expire_ms: i64,
    using_bootstrap_controllers: bool,
    log_context: LogContext,
}

impl AdminMetadataManager {
    /// Creates a manager with the given refresh backoff and metadata expiry.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#AdminMetadataManager")]
    pub(crate) fn new(
        refresh_backoff_ms: i64,
        metadata_expire_ms: i64,
        using_bootstrap_controllers: bool,
        log_context: LogContext,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                state: State::Quiescent,
                cluster: Cluster::empty(),
                bootstrap_cluster: Cluster::empty(),
                last_metadata_update_ms: 0,
                last_metadata_fetch_attempt_ms: 0,
                metadata_attempt_start_ms: None,
                fatal_error: None,
            })),
            refresh_backoff_ms,
            metadata_expire_ms,
            using_bootstrap_controllers,
            log_context,
        }
    }

    /// Returns a [`MetadataUpdater`] sharing this manager's cluster, to be moved
    /// into the `NetworkClient`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#updater")]
    pub(crate) fn updater(&self) -> Box<dyn MetadataUpdater> {
        Box::new(AdminMetadataUpdater { inner: Arc::clone(&self.inner), log_context: self.log_context.clone() })
    }

    /// Whether the client is configured with `bootstrap.controllers` (KIP-919).
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#usingBootstrapControllers")]
    pub(crate) fn using_bootstrap_controllers(&self) -> bool {
        self.using_bootstrap_controllers
    }

    /// Returns whether the manager has usable, non-bootstrap metadata.
    ///
    /// # Errors
    ///
    /// Returns the stored fatal exception (if any), mirroring Java's `isReady`
    /// which rethrows `fatalException`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#isReady")]
    pub(crate) fn is_ready(&self) -> Result<bool, Error> {
        let inner = self.inner.lock().unwrap();
        if let Some(err) = &inner.fatal_error {
            return Err(err.clone());
        }
        if inner.cluster.nodes().is_empty() {
            kafka_warn!(self.log_context, "Metadata is not ready because there are no known nodes.");
            return Ok(false);
        }
        if inner.cluster.is_bootstrap_configured() {
            kafka_warn!(self.log_context, "Metadata is not ready because it contains bootstrap nodes.");
            return Ok(false);
        }
        Ok(true)
    }

    /// The current controller node, if known.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#controller")]
    pub(crate) fn controller(&self) -> Option<Node> {
        self.inner.lock().unwrap().cluster.controller().cloned()
    }

    /// The node with the given id, if known.
    ///
    /// Used by `ConstantNodeIdProvider` (admin-client.md §2).
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#nodeById")]
    pub(crate) fn node_by_id(&self, id: i32) -> Option<Node> {
        self.inner.lock().unwrap().cluster.node_by_id(id).cloned()
    }

    /// Requests a metadata update on the next opportunity.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#requestUpdate")]
    pub(crate) fn request_update(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.state == State::Quiescent {
            inner.state = State::UpdateRequested;
        }
    }

    /// Clears the current controller (used on `NOT_CONTROLLER`) and requests an
    /// update. Mirrors Java's `clearController` + a caller-side `requestUpdate`.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#clearController")]
    pub(crate) fn clear_controller(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.cluster = rebuild_without_controller(&inner.cluster);
    }

    /// The delay in milliseconds before the next metadata fetch is due. When
    /// this returns `0`, the background task fires a metadata call.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#metadataFetchDelayMs")]
    pub(crate) fn metadata_fetch_delay_ms(&self, now: i64) -> i64 {
        let inner = self.inner.lock().unwrap();
        let delay_before_next_expire = 0i64.max(self.metadata_expire_ms - (now - inner.last_metadata_update_ms));
        let delay_before_next_attempt =
            0i64.max(self.refresh_backoff_ms - (now - inner.last_metadata_fetch_attempt_ms));
        match inner.state {
            State::Quiescent => delay_before_next_expire.max(delay_before_next_attempt),
            // Even though an update is requested, respect the backoff.
            State::UpdateRequested => delay_before_next_attempt,
            State::UpdatePending => i64::MAX,
        }
    }

    /// Whether no metadata has been obtained for longer than
    /// `rebootstrap_trigger_ms` since attempts to fetch it started, or a
    /// rebootstrap was initiated.
    ///
    /// In production the `NetworkClient` asks through the updater (which shares
    /// this body); Java's `AdminMetadataManagerTest` asks the manager directly.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#needsRebootstrap")]
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn needs_rebootstrap(&self, now: i64, rebootstrap_trigger_ms: i64) -> bool {
        needs_rebootstrap(&self.inner.lock().unwrap(), now, rebootstrap_trigger_ms)
    }

    /// Transitions to `UPDATE_PENDING`, recording the attempt time, and the
    /// start of the attempts if they have not started yet.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#transitionToUpdatePending")]
    pub(crate) fn transition_to_update_pending(&self, now: i64) {
        let mut inner = self.inner.lock().unwrap();
        inner.state = State::UpdatePending;
        inner.last_metadata_fetch_attempt_ms = now;
        if inner.metadata_attempt_start_ms.is_none() {
            inner.metadata_attempt_start_ms = Some(now);
        }
    }

    /// Applies a successful metadata response's cluster.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#update")]
    pub(crate) fn update(&self, cluster: Cluster, now: i64) {
        update(&mut self.inner.lock().unwrap(), cluster, now);
    }

    /// Makes the next [`needs_rebootstrap`](Self::needs_rebootstrap) answer
    /// `true`, so the `NetworkClient` rebootstraps on its next poll (KIP-1102,
    /// a `REBOOTSTRAP_REQUIRED` metadata response).
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#initiateRebootstrap")]
    pub(crate) fn initiate_rebootstrap(&self) {
        self.inner.lock().unwrap().metadata_attempt_start_ms = Some(0);
    }

    /// Rebootstraps metadata with the cluster previously used for bootstrapping.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#rebootstrap")]
    pub(crate) fn rebootstrap(&self, now: i64) {
        rebootstrap(&mut self.inner.lock().unwrap(), now, &self.log_context);
    }

    /// Records a failed metadata update, storing a fatal exception if the error
    /// is not retriable.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#updateFailed")]
    pub(crate) fn update_failed(&self, error: Error) {
        let mut inner = self.inner.lock().unwrap();
        inner.state = State::Quiescent;
        // We depend on pending calls to request another metadata update.
        if crate::common::requests::RequestUtils::is_fatal_error(&error) {
            inner.fatal_error = Some(error);
        }
    }
}

/// The body of `AdminMetadataManager.update`, shared by the manager and by
/// [`rebootstrap`], which Java runs under the same object.
fn update(inner: &mut Inner, cluster: Cluster, now: i64) {
    if cluster.is_bootstrap_configured() {
        inner.bootstrap_cluster = cluster.clone();
    } else {
        inner.last_metadata_update_ms = now;
    }
    inner.state = State::Quiescent;
    inner.fatal_error = None;
    inner.metadata_attempt_start_ms = None;
    // Only update if the metadata succeeded (has nodes). If a metadata
    // request failed we keep the previous cluster.
    if !cluster.nodes().is_empty() {
        inner.cluster = cluster;
    }
}

/// The body of `AdminMetadataManager.needsRebootstrap`, shared with the updater.
fn needs_rebootstrap(inner: &Inner, now: i64, rebootstrap_trigger_ms: i64) -> bool {
    inner
        .metadata_attempt_start_ms
        .is_some_and(|start_ms| now - start_ms > rebootstrap_trigger_ms)
}

/// The body of `AdminMetadataManager.rebootstrap`, shared with the updater:
/// `update(bootstrapCluster, now)`, then restart the trigger interval at `now`.
fn rebootstrap(inner: &mut Inner, now: i64, log_context: &LogContext) {
    kafka_info!(log_context, "Rebootstrapping with {}", inner.bootstrap_cluster);
    let bootstrap_cluster = inner.bootstrap_cluster.clone();
    update(inner, bootstrap_cluster, now);
    inner.metadata_attempt_start_ms = Some(now);
}

/// Rebuilds a cluster identical to `cluster` but with no controller.
///
/// Mirrors Java's `AdminMetadataManager.clearController`, which constructs a new
/// `Cluster` with `controller = null`, preserving every other field.
fn rebuild_without_controller(cluster: &Cluster) -> Cluster {
    let mut partitions = Vec::new();
    for topic in cluster.topics() {
        partitions.extend(cluster.partitions_for_topic(topic).iter().cloned());
    }
    let topic_ids: HashMap<String, Uuid> = cluster
        .topics()
        .map(|t| (t.to_string(), cluster.topic_id(t)))
        .filter(|(_, id)| *id != Uuid::zero())
        .collect();
    Cluster::with_invalid_topics_controller_topic_ids(
        cluster.cluster_resource().cluster_id().map(str::to_string),
        cluster.nodes().to_vec(),
        partitions,
        cluster.unauthorized_topics().clone(),
        cluster.invalid_topics().clone(),
        cluster.internal_topics().clone(),
        None,
        topic_ids,
    )
}

/// The `NetworkClient`-facing [`MetadataUpdater`] backed by an
/// [`AdminMetadataManager`]. Passive: it never drives its own metadata fetch
/// (the background task issues metadata `Call`s explicitly); it exposes the
/// node list, handles disconnects, and answers the `NetworkClient`'s
/// rebootstrap questions from the manager's state.
///
/// Corresponds to `AdminMetadataManager.AdminMetadataUpdater`.
#[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager$AdminMetadataUpdater")]
struct AdminMetadataUpdater {
    inner: Arc<Mutex<Inner>>,
    log_context: LogContext,
}

impl MetadataUpdater for AdminMetadataUpdater {
    fn fetch_nodes(&self) -> Vec<Node> {
        self.inner.lock().unwrap().cluster.nodes().to_vec()
    }

    fn is_update_due(&self, _now: i64) -> bool {
        false
    }

    fn maybe_update(&mut self, _now: i64) -> i64 {
        // Metadata updates are driven by explicit admin `Call`s, never by the
        // NetworkClient's internal mechanism.
        i64::MAX
    }

    fn handle_server_disconnect(&mut self, _now: i64, _node_id: &str, maybe_auth_error: Option<Error>) {
        let mut inner = self.inner.lock().unwrap();
        // Java: `maybeFatalException.ifPresent(this::updateFailed)`
        // (`AdminMetadataManager.java:127`) — stored unconditionally. The
        // parameter is typed `Optional<AuthenticationException>`, so the
        // `isFatalException` check inside `updateFailed` is statically
        // satisfied and never filters anything out. Rust's weaker
        // `Option<Error>` cannot express that, and the previous `is_fatal_error()`
        // guard here only passed because `NetworkClient` set a fatal flag by
        // hand. With fatality derived from the error code, the guard would
        // silently start dropping authentication failures — so drop the guard
        // instead, matching Java's control flow.
        if let Some(err) = maybe_auth_error {
            inner.fatal_error = Some(err);
        }
        // Ask for a metadata update after a disconnect.
        if inner.state == State::Quiescent {
            inner.state = State::UpdateRequested;
        }
    }

    fn handle_failed_request(&mut self, _now: i64, _maybe_fatal_error: Option<Error>) {
        // Metadata requests are admin `Call`s; failures are handled there.
    }

    fn handle_successful_response(
        &mut self,
        _request_header: &RequestHeader,
        _now: i64,
        _metadata_response: &MetadataResponse,
    ) {
        // Not used; the metadata `Call`'s handle_response applies the cluster.
    }

    fn needs_rebootstrap(&self, now: i64, rebootstrap_trigger_ms: i64) -> bool {
        needs_rebootstrap(&self.inner.lock().unwrap(), now, rebootstrap_trigger_ms)
    }

    fn rebootstrap(&mut self, now: i64) {
        rebootstrap(&mut self.inner.lock().unwrap(), now, &self.log_context);
    }

    fn close(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::utils::{MockTime, Time};
    use std::collections::HashSet;
    use std::net::SocketAddr;

    const REFRESH_BACKOFF_MS: i64 = 100;
    const METADATA_EXPIRE_MS: i64 = 60_000;

    fn mgr() -> AdminMetadataManager {
        AdminMetadataManager::new(REFRESH_BACKOFF_MS, METADATA_EXPIRE_MS, false, LogContext::empty())
    }

    /// Java's `AdminMetadataManagerTest.mockCluster()`.
    fn mock_cluster() -> Cluster {
        let nodes: Vec<Node> = (0..3).map(|i| Node::new(i, "localhost".to_string(), 8121 + i)).collect();
        let controller = nodes[0].clone();
        Cluster::with_invalid_topics_controller_topic_ids(
            Some("mockClusterId".to_string()),
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            Some(controller),
            HashMap::new(),
        )
    }

    /// Translated from `AdminMetadataManagerTest.testNeedsRebootstrap`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManagerTest#testNeedsRebootstrap")]
    fn test_needs_rebootstrap() {
        let time = MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(0, 1_000_000, 0);
        let mgr = mgr();
        let rebootstrap_trigger_ms = 1000;
        let bootstrap_address: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        mgr.update(
            Cluster::bootstrap(&[("localhost".to_string(), bootstrap_address)]),
            time.milliseconds(),
        );
        assert!(!mgr.needs_rebootstrap(time.milliseconds(), rebootstrap_trigger_ms));
        assert!(!mgr.needs_rebootstrap(time.milliseconds() + 2000, rebootstrap_trigger_ms));

        mgr.transition_to_update_pending(time.milliseconds());
        assert!(!mgr.needs_rebootstrap(time.milliseconds(), rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time.milliseconds() + 1001, rebootstrap_trigger_ms));

        time.sleep(100);
        // Java passes a bare `RuntimeException`: any non-fatal error.
        mgr.update_failed(Error::local_illegal_state(""));
        assert!(!mgr.needs_rebootstrap(time.milliseconds() + 900, rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time.milliseconds() + 901, rebootstrap_trigger_ms));

        time.sleep(1000);
        mgr.update(mock_cluster(), time.milliseconds());
        assert!(!mgr.needs_rebootstrap(time.milliseconds(), rebootstrap_trigger_ms));
        assert!(!mgr.needs_rebootstrap(time.milliseconds() + 2000, rebootstrap_trigger_ms));

        time.sleep(1000);
        mgr.transition_to_update_pending(time.milliseconds());
        assert!(!mgr.needs_rebootstrap(time.milliseconds(), rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time.milliseconds() + 1001, rebootstrap_trigger_ms));

        time.sleep(1001);
        assert!(mgr.needs_rebootstrap(time.milliseconds(), rebootstrap_trigger_ms));
        mgr.rebootstrap(time.milliseconds());
        assert!(!mgr.needs_rebootstrap(time.milliseconds(), rebootstrap_trigger_ms));
        assert!(!mgr.needs_rebootstrap(time.milliseconds() + 1000, rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time.milliseconds() + 1001, rebootstrap_trigger_ms));

        mgr.initiate_rebootstrap();
        assert!(mgr.needs_rebootstrap(time.milliseconds(), rebootstrap_trigger_ms));
        mgr.rebootstrap(time.milliseconds());
        assert!(!mgr.needs_rebootstrap(time.milliseconds(), rebootstrap_trigger_ms));
        assert!(!mgr.needs_rebootstrap(time.milliseconds() + 1000, rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time.milliseconds() + 1001, rebootstrap_trigger_ms));
    }

    /// Beyond Java's test: `rebootstrap` puts the bootstrap cluster back, so the
    /// manager is no longer ready and the updater hands the `NetworkClient` the
    /// bootstrap nodes; the updater's `needs_rebootstrap` / `rebootstrap` are the
    /// manager's (`AdminMetadataUpdater`, `AdminMetadataManager.java:142-150`).
    #[test]
    fn rebootstrap_restores_the_bootstrap_cluster_through_the_updater() {
        let mgr = mgr();
        let bootstrap_address: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        mgr.update(Cluster::bootstrap(&[("localhost".to_string(), bootstrap_address)]), 1000);
        mgr.update(mock_cluster(), 1000);
        assert!(mgr.is_ready().unwrap());

        let mut updater = mgr.updater();
        mgr.initiate_rebootstrap();
        assert!(updater.needs_rebootstrap(2000, 1000));
        updater.rebootstrap(2000);

        assert!(!mgr.is_ready().unwrap(), "bootstrap metadata is not ready");
        let ids: Vec<i32> = updater.fetch_nodes().iter().map(Node::id).collect();
        assert_eq!(ids, vec![-1]);
        assert!(!updater.needs_rebootstrap(3000, 1000));
        assert!(updater.needs_rebootstrap(3001, 1000));
    }
}
