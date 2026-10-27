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

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::MetadataUpdater;
use crate::common::errors::BootstrapResolutionError;
use crate::common::requests::MetadataResponse;
use crate::common::requests::RequestHeader;
use crate::common::utils::internals::LogContext;
use crate::common::{Cluster, Error, Node, Uuid};
use crate::{kafka_debug, kafka_info, kafka_trace, kafka_warn};
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
    /// The time in wall-clock milliseconds when we started attempts to fetch
    /// metadata. If `None`, metadata has not been requested. This is the start
    /// time based on which rebootstrap is triggered if metadata is not obtained
    /// for the configured rebootstrap trigger interval. Set to `Some(0)` to
    /// force rebootstrap immediately.
    metadata_attempt_start_ms: Option<i64>,
    /// A fatal (non-retriable) error to surface from `is_ready`.
    fatal_error: Option<Error>,
    /// If this is set, bootstrap DNS resolution permanently failed (KIP-909).
    /// Unlike `fatal_error`, this is never cleared so all subsequent API calls
    /// see it. Java keeps it in a `volatile` field because it is written by the
    /// I/O thread (via the network client) and read by both the I/O thread and
    /// the app thread (via `KafkaAdminClient#enqueue`); here the shared `Mutex`
    /// gives the app-side `submit` the same visibility.
    bootstrap_fatal_error: Option<BootstrapResolutionError>,
}

impl Inner {
    /// Java's `AdminMetadataManager.update(Cluster, long)` body, shared with
    /// [`AdminMetadataUpdater::bootstrap`].
    fn update(&mut self, cluster: Cluster, now: i64) {
        if cluster.is_bootstrap_configured() {
            self.bootstrap_cluster = cluster.clone();
        } else {
            self.last_metadata_update_ms = now;
        }
        self.state = State::Quiescent;
        self.fatal_error = None;
        self.metadata_attempt_start_ms = None;
        // Only update if the metadata succeeded (has nodes). If a metadata
        // request failed we keep the previous cluster.
        if !cluster.nodes().is_empty() {
            self.cluster = cluster;
        }
    }

    /// Java's `bootstrapCluster != null`. The Rust field starts as
    /// [`Cluster::empty`] rather than `null`, and only a bootstrap-configured
    /// cluster is ever stored in it.
    fn is_bootstrapped(&self) -> bool {
        self.bootstrap_cluster.is_bootstrap_configured()
    }

    /// Java's `AdminMetadataManager.needsRebootstrap(long, long)`.
    fn needs_rebootstrap(&self, now: i64, rebootstrap_trigger_ms: i64) -> bool {
        self.metadata_attempt_start_ms
            .is_some_and(|start_ms| now - start_ms > rebootstrap_trigger_ms)
    }

    /// Java's `AdminMetadataManager.rebootstrap(long)`: rebootstrap metadata
    /// with the cluster previously used for bootstrapping.
    fn rebootstrap(&mut self, now: i64, log_context: &LogContext) {
        kafka_info!(log_context, "Rebootstrapping with {}", self.bootstrap_cluster);
        let bootstrap_cluster = self.bootstrap_cluster.clone();
        self.update(bootstrap_cluster, now);
        self.metadata_attempt_start_ms = Some(now);
    }
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
                bootstrap_fatal_error: None,
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
            kafka_debug!(self.log_context, "Metadata is not usable: failed to get metadata. {}", err);
            return Err(err.clone());
        }
        if inner.cluster.nodes().is_empty() {
            kafka_trace!(
                self.log_context,
                "Metadata is not ready: bootstrap nodes have not been initialized yet."
            );
            return Ok(false);
        }
        if inner.cluster.is_bootstrap_configured() {
            kafka_trace!(
                self.log_context,
                "Metadata is not ready: we have not fetched metadata from the bootstrap nodes yet."
            );
            return Ok(false);
        }
        kafka_trace!(self.log_context, "Metadata is ready to use.");
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

    /// Transitions to `UPDATE_PENDING`, recording the attempt time, and the
    /// start of the attempts when none is in progress.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#transitionToUpdatePending")]
    pub(crate) fn transition_to_update_pending(&self, now: i64) {
        let mut inner = self.inner.lock().unwrap();
        inner.state = State::UpdatePending;
        inner.last_metadata_fetch_attempt_ms = now;
        if inner.metadata_attempt_start_ms.is_none() {
            inner.metadata_attempt_start_ms = Some(now);
        }
    }

    /// Whether metadata attempts have gone unanswered for longer than
    /// `rebootstrap_trigger_ms`, or a rebootstrap was requested by
    /// [`Self::initiate_rebootstrap`].
    ///
    /// The `NetworkClient` reads it through [`AdminMetadataUpdater`]'s
    /// `needs_rebootstrap`, which shares the state; Java's
    /// `AdminMetadataManager.needsRebootstrap` is called from the same place.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#needsRebootstrap")]
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn needs_rebootstrap(&self, now: i64, rebootstrap_trigger_ms: i64) -> bool {
        self.inner.lock().unwrap().needs_rebootstrap(now, rebootstrap_trigger_ms)
    }

    /// Forces a rebootstrap on the next check, on a `REBOOTSTRAP_REQUIRED`
    /// metadata response.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#initiateRebootstrap")]
    pub(crate) fn initiate_rebootstrap(&self) {
        self.inner.lock().unwrap().metadata_attempt_start_ms = Some(0);
    }

    /// Rebootstraps metadata with the cluster previously used for
    /// bootstrapping.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#rebootstrap")]
    pub(crate) fn rebootstrap(&self, now: i64) {
        self.inner.lock().unwrap().rebootstrap(now, &self.log_context);
    }

    /// Applies a successful metadata response's cluster.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#update")]
    pub(crate) fn update(&self, cluster: Cluster, now: i64) {
        self.inner.lock().unwrap().update(cluster, now);
    }

    /// Whether the bootstrap cluster has been set, synchronously at
    /// construction or by the `NetworkClient`'s asynchronous bootstrap
    /// resolution (KIP-909).
    ///
    /// It gates `KafkaAdminClient`'s rebootstrap
    /// (`KafkaAdminClient.MetadataUpdateNodeIdProvider#provide`).
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#isBootstrapped")]
    pub(crate) fn is_bootstrapped(&self) -> bool {
        self.inner.lock().unwrap().is_bootstrapped()
    }

    /// The permanent bootstrap DNS resolution failure, if one was recorded
    /// (KIP-909).
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#bootstrapFatalException")]
    pub(crate) fn bootstrap_fatal_error(&self) -> Option<BootstrapResolutionError> {
        self.inner.lock().unwrap().bootstrap_fatal_error.clone()
    }

    /// Records the permanent bootstrap DNS resolution failure.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#recordBootstrapFatalException")]
    fn record_bootstrap_fatal_error(inner: &Mutex<Inner>, error: BootstrapResolutionError) {
        inner.lock().unwrap().bootstrap_fatal_error = Some(error);
    }

    /// Records a failed metadata update and returns to `QUIESCENT`. A fatal
    /// error (`RequestUtils.isFatalException`) is logged as a warning and, when
    /// it is an API error, kept, so that [`is_ready`](Self::is_ready) returns it
    /// until the next successful update; any other error is logged at info
    /// level.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#updateFailed")]
    pub(crate) fn update_failed(&self, error: Error) {
        let mut inner = self.inner.lock().unwrap();
        // We depend on pending calls to request another metadata update
        inner.state = State::Quiescent;

        if crate::common::requests::RequestUtils::is_fatal_error(&error) {
            kafka_warn!(self.log_context, "Fatal error during metadata update: {}", error);
            if let Some(warning) = Self::unsupported_version_warning(&error, self.using_bootstrap_controllers) {
                kafka_warn!(self.log_context, "{}: {}", warning, error);
            }
            // avoid unchecked/unconfirmed cast to ApiException
            if error.is_api_error() {
                inner.fatal_error = Some(error);
            }
        } else {
            kafka_info!(self.log_context, "Metadata update failed: {}", error);
        }
    }

    /// The warning `AdminMetadataManager#updateFailed` adds for a fatal
    /// `UnsupportedVersionException`, which names the API the remote
    /// node does not support: KIP-919's `DESCRIBE_CLUSTER` for a
    /// `bootstrap.controllers` client, `METADATA` otherwise. `None` for any
    /// other error.
    fn unsupported_version_warning(error: &Error, using_bootstrap_controllers: bool) -> Option<&'static str> {
        if !error.is_unsupported_version_error() {
            return None;
        }
        Some(if using_bootstrap_controllers {
            "The remote node is not a CONTROLLER that supports the KIP-919 DESCRIBE_CLUSTER api."
        } else {
            "The remote node is not a BROKER that supports the METADATA api."
        })
    }
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
/// (the background task issues metadata `Call`s explicitly); it only exposes the
/// node list and handles disconnects.
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
        // Java: `maybeFatalException.ifPresent(this::updateFailed)` in
        // `org.apache.kafka.clients.admin.internals.AdminMetadataManager.AdminMetadataUpdater#handleServerDisconnect`
        // stores the error unconditionally. The parameter is typed
        // `Optional<AuthenticationException>`, so the `isFatalException` check
        // inside `updateFailed` is statically satisfied and never filters anything
        // out. Rust's weaker `Option<Error>` cannot express that, and the previous
        // `is_fatal_error()` guard here only passed because `NetworkClient` set a
        // fatal flag by hand. With fatality derived from the error code, the guard
        // would silently start dropping authentication failures — so drop the
        // guard instead, matching Java's control flow.
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

    fn bootstrap_failed(&mut self, error: Error) {
        // Java: `if (exception instanceof BootstrapResolutionException)`.
        if let Error::BootstrapResolution(error) = error {
            AdminMetadataManager::record_bootstrap_fatal_error(&self.inner, error);
        }
    }

    fn is_bootstrapped(&self) -> bool {
        self.inner.lock().unwrap().is_bootstrapped()
    }

    fn needs_rebootstrap(&self, now: i64, rebootstrap_trigger_ms: i64) -> bool {
        self.inner.lock().unwrap().needs_rebootstrap(now, rebootstrap_trigger_ms)
    }

    fn rebootstrap(&mut self, now: i64) {
        self.inner.lock().unwrap().rebootstrap(now, &self.log_context);
    }

    fn bootstrap(&mut self, addresses: Vec<(String, SocketAddr)>) {
        // The `now` argument is unused when the incoming cluster is bootstrap-configured
        // (see AdminMetadataManager#update), so we pass 0 rather than plumbing a clock through
        // the MetadataUpdater interface for a value that would be ignored.
        self.inner.lock().unwrap().update(Cluster::bootstrap(&addresses), 0);
    }

    fn close(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::errors::{AuthenticationError, AuthorizationError};
    use crate::common::utils::{MockTime, Time};
    use std::collections::HashSet;

    /// `AdminMetadataManagerTest.refreshBackoffMs`.
    const REFRESH_BACKOFF_MS: i64 = 100;
    /// `AdminMetadataManagerTest.metadataExpireMs`.
    const METADATA_EXPIRE_MS: i64 = 60_000;

    /// `AdminMetadataManagerTest.mgr`.
    fn mgr() -> AdminMetadataManager {
        AdminMetadataManager::new(REFRESH_BACKOFF_MS, METADATA_EXPIRE_MS, false, LogContext::empty())
    }

    /// Java's `new MockTime()`, whose clock starts at the wall-clock time: any
    /// time past `REFRESH_BACKOFF_MS` behaves the same.
    fn mock_time() -> MockTime {
        MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(0, 1_000_000, 0)
    }

    fn new_manager() -> AdminMetadataManager {
        AdminMetadataManager::new(100, 300_000, false, LogContext::empty())
    }

    fn address() -> (String, SocketAddr) {
        ("localhost".to_string(), "127.0.0.1:9092".parse().unwrap())
    }

    /// KIP-909: `isBootstrapped` is `bootstrapCluster != null` — false until a
    /// bootstrap cluster is installed, synchronously through `update` or by the
    /// `NetworkClient` through the updater's `bootstrap`.
    #[test]
    fn test_is_bootstrapped() {
        let manager = new_manager();
        assert!(!manager.is_bootstrapped());
        manager.update(Cluster::bootstrap(&[address()]), 0);
        assert!(manager.is_bootstrapped());

        let manager = new_manager();
        let mut updater = manager.updater();
        assert!(!updater.is_bootstrapped());
        updater.bootstrap(vec![address()]);
        assert!(updater.is_bootstrapped());
        assert!(manager.is_bootstrapped());
        assert_eq!(updater.fetch_nodes().len(), 1);
        assert!(manager.inner.lock().unwrap().cluster.is_bootstrap_configured());
    }

    /// KIP-909: the updater's `bootstrapFailed` records only a
    /// `BootstrapResolutionException`, and the record is never cleared — not by
    /// a later metadata update either.
    #[test]
    fn test_bootstrap_failed_records_only_the_bootstrap_resolution_error() {
        let manager = new_manager();
        let mut updater = manager.updater();
        assert!(manager.bootstrap_fatal_error().is_none());

        updater.bootstrap_failed(Error::kafka_message("not a bootstrap failure"));
        assert!(manager.bootstrap_fatal_error().is_none());

        updater.bootstrap_failed(Error::BootstrapResolution(BootstrapResolutionError::new("dns")));
        manager.update(Cluster::bootstrap(&[address()]), 0);
        assert_eq!(manager.bootstrap_fatal_error().expect("recorded").message(), "dns");
    }

    /// Java's `mockCluster()` in `AdminMetadataManagerTest`.
    fn mock_cluster() -> Cluster {
        let nodes = vec![
            Node::new(0, "localhost".to_string(), 8121),
            Node::new(1, "localhost".to_string(), 8122),
            Node::new(2, "localhost".to_string(), 8123),
        ];
        let controller = Some(nodes[0].clone());
        Cluster::with_invalid_topics_controller(
            Some("mockClusterId".to_string()),
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            controller,
        )
    }

    /// Translated from `AdminMetadataManagerTest.testSetUsingBootstrapControllers`
    /// (`@ValueSource(booleans = {false, true})`).
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManagerTest#testSetUsingBootstrapControllers")]
    fn test_set_using_bootstrap_controllers() {
        for using_bootstrap_controllers in [false, true] {
            let manager = AdminMetadataManager::new(
                REFRESH_BACKOFF_MS,
                METADATA_EXPIRE_MS,
                using_bootstrap_controllers,
                LogContext::empty(),
            );
            assert_eq!(using_bootstrap_controllers, manager.using_bootstrap_controllers());
        }
    }

    /// Translated from `AdminMetadataManagerTest.testMetadataReady`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManagerTest#testMetadataReady")]
    fn test_metadata_ready() {
        let time = mock_time();
        let mgr = mgr();
        // Metadata is not ready on initialization
        assert!(!mgr.is_ready().unwrap());
        assert_eq!(0, mgr.metadata_fetch_delay_ms(time.milliseconds()));

        // Metadata is not ready when bootstrap servers are set
        let bootstrap_address: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        mgr.update(
            Cluster::bootstrap(&[("localhost".to_string(), bootstrap_address)]),
            time.milliseconds(),
        );
        assert!(!mgr.is_ready().unwrap());
        assert_eq!(0, mgr.metadata_fetch_delay_ms(time.milliseconds()));

        mgr.update(mock_cluster(), time.milliseconds());
        assert!(mgr.is_ready().unwrap());
        assert_eq!(METADATA_EXPIRE_MS, mgr.metadata_fetch_delay_ms(time.milliseconds()));

        time.sleep(METADATA_EXPIRE_MS);
        assert_eq!(0, mgr.metadata_fetch_delay_ms(time.milliseconds()));
    }

    /// Translated from `AdminMetadataManagerTest.testMetadataRefreshBackoff`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManagerTest#testMetadataRefreshBackoff")]
    fn test_metadata_refresh_backoff() {
        let time = mock_time();
        let mgr = mgr();
        mgr.transition_to_update_pending(time.milliseconds());
        assert_eq!(i64::MAX, mgr.metadata_fetch_delay_ms(time.milliseconds()));

        // Java passes a bare `RuntimeException`: any non-fatal error.
        mgr.update_failed(Error::local_illegal_state(""));
        assert_eq!(REFRESH_BACKOFF_MS, mgr.metadata_fetch_delay_ms(time.milliseconds()));

        // Even if we explicitly request an update, the backoff should be respected
        mgr.request_update();
        assert_eq!(REFRESH_BACKOFF_MS, mgr.metadata_fetch_delay_ms(time.milliseconds()));

        time.sleep(REFRESH_BACKOFF_MS);
        assert_eq!(0, mgr.metadata_fetch_delay_ms(time.milliseconds()));
    }

    /// Translated from `AdminMetadataManagerTest.testAuthenticationFailure`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManagerTest#testAuthenticationFailure")]
    fn test_authentication_failure() {
        let time = mock_time();
        let mgr = mgr();
        mgr.transition_to_update_pending(time.milliseconds());
        mgr.update_failed(Error::Authentication(AuthenticationError::new("Authentication failed")));
        assert_eq!(REFRESH_BACKOFF_MS, mgr.metadata_fetch_delay_ms(time.milliseconds()));
        let error = mgr.is_ready().unwrap_err();
        assert!(error.is_authentication_error(), "got {error:?}");
        mgr.update(mock_cluster(), time.milliseconds());
        assert!(mgr.is_ready().unwrap());
    }

    /// Translated from `AdminMetadataManagerTest.testAuthorizationFailure`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManagerTest#testAuthorizationFailure")]
    fn test_authorization_failure() {
        let time = mock_time();
        let mgr = mgr();
        mgr.transition_to_update_pending(time.milliseconds());
        mgr.update_failed(Error::Authorization(AuthorizationError::new("Authorization failed")));
        assert_eq!(REFRESH_BACKOFF_MS, mgr.metadata_fetch_delay_ms(time.milliseconds()));
        let error = mgr.is_ready().unwrap_err();
        assert!(error.is_authorization_error(), "got {error:?}");
        mgr.update(mock_cluster(), time.milliseconds());
        assert!(mgr.is_ready().unwrap());
    }

    /// The KIP-919 branch of `AdminMetadataManager#updateFailed`: a
    /// fatal `UnsupportedVersionException` names the API the remote node lacks,
    /// `DESCRIBE_CLUSTER` for a `bootstrap.controllers` client and `METADATA`
    /// otherwise; other errors add no such warning. The fatal error is kept
    /// either way, and a non-fatal one is not.
    #[test]
    fn update_failed_names_the_unsupported_metadata_api() {
        let unsupported = Error::unsupported_version("The node does not support DESCRIBE_CLUSTER");
        assert_eq!(
            AdminMetadataManager::unsupported_version_warning(&unsupported, true),
            Some("The remote node is not a CONTROLLER that supports the KIP-919 DESCRIBE_CLUSTER api.")
        );
        assert_eq!(
            AdminMetadataManager::unsupported_version_warning(&unsupported, false),
            Some("The remote node is not a BROKER that supports the METADATA api.")
        );
        let mismatched = Error::new(crate::common::protocol::Errors::MismatchedEndpointType);
        assert_eq!(AdminMetadataManager::unsupported_version_warning(&mismatched, true), None);

        for using_bootstrap_controllers in [false, true] {
            let manager = AdminMetadataManager::new(
                REFRESH_BACKOFF_MS,
                METADATA_EXPIRE_MS,
                using_bootstrap_controllers,
                LogContext::empty(),
            );
            manager.update(mock_cluster(), 1000);
            manager.update_failed(Error::timeout("not fatal"));
            assert!(manager.is_ready().unwrap(), "a non-fatal error is not kept");
            manager.update_failed(unsupported.clone());
            let error = manager.is_ready().unwrap_err();
            assert!(error.is_unsupported_version_error(), "got {error:?}");
        }
    }

    /// Translated from `AdminMetadataManagerTest.testNeedsRebootstrap`. Java's
    /// `MockTime` becomes a plain millisecond counter: the manager never reads a
    /// clock, every method takes `now`.
    #[test]
    fn test_needs_rebootstrap() {
        let mgr = AdminMetadataManager::new(100, 60_000, false, LogContext::empty());
        let mut time: i64 = 1_000_000;
        let rebootstrap_trigger_ms = 1000;
        mgr.update(
            Cluster::bootstrap(&[("localhost".to_string(), "127.0.0.1:9999".parse().unwrap())]),
            time,
        );
        assert!(!mgr.needs_rebootstrap(time, rebootstrap_trigger_ms));
        assert!(!mgr.needs_rebootstrap(time + 2000, rebootstrap_trigger_ms));

        mgr.transition_to_update_pending(time);
        assert!(!mgr.needs_rebootstrap(time, rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time + 1001, rebootstrap_trigger_ms));

        time += 100;
        mgr.update_failed(Error::kafka_message(""));
        assert!(!mgr.needs_rebootstrap(time + 900, rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time + 901, rebootstrap_trigger_ms));

        time += 1000;
        mgr.update(mock_cluster(), time);
        assert!(!mgr.needs_rebootstrap(time, rebootstrap_trigger_ms));
        assert!(!mgr.needs_rebootstrap(time + 2000, rebootstrap_trigger_ms));

        time += 1000;
        mgr.transition_to_update_pending(time);
        assert!(!mgr.needs_rebootstrap(time, rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time + 1001, rebootstrap_trigger_ms));

        time += 1001;
        assert!(mgr.needs_rebootstrap(time, rebootstrap_trigger_ms));
        mgr.rebootstrap(time);
        assert!(!mgr.needs_rebootstrap(time, rebootstrap_trigger_ms));
        assert!(!mgr.needs_rebootstrap(time + 1000, rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time + 1001, rebootstrap_trigger_ms));

        mgr.initiate_rebootstrap();
        assert!(mgr.needs_rebootstrap(time, rebootstrap_trigger_ms));
        mgr.rebootstrap(time);
        assert!(!mgr.needs_rebootstrap(time, rebootstrap_trigger_ms));
        assert!(!mgr.needs_rebootstrap(time + 1000, rebootstrap_trigger_ms));
        assert!(mgr.needs_rebootstrap(time + 1001, rebootstrap_trigger_ms));
    }

    /// Beyond Java's test: `rebootstrap` puts the bootstrap cluster back as the
    /// current one, and the updater the `NetworkClient` consults answers from
    /// the same state as the manager.
    #[test]
    fn test_rebootstrap_restores_the_bootstrap_cluster_and_is_shared_with_the_updater() {
        let mgr = AdminMetadataManager::new(100, 60_000, false, LogContext::empty());
        let mut updater = mgr.updater();
        mgr.update(Cluster::bootstrap(&[address()]), 0);
        mgr.update(mock_cluster(), 10);
        assert_eq!(updater.fetch_nodes().len(), 3);

        mgr.initiate_rebootstrap();
        assert!(updater.needs_rebootstrap(10, 5));
        updater.rebootstrap(20);
        assert!(!mgr.needs_rebootstrap(20, 1000));
        assert!(mgr.needs_rebootstrap(1021, 1000));
        let nodes = updater.fetch_nodes();
        assert_eq!(nodes.len(), 1);
        assert!(mgr.inner.lock().unwrap().cluster.is_bootstrap_configured());
    }
}
