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
use crate::kafka_warn;
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

    /// Transitions to `UPDATE_PENDING`, recording the attempt time.
    #[doc(alias = "org.apache.kafka.clients.admin.internals.AdminMetadataManager#transitionToUpdatePending")]
    pub(crate) fn transition_to_update_pending(&self, now: i64) {
        let mut inner = self.inner.lock().unwrap();
        inner.state = State::UpdatePending;
        inner.last_metadata_fetch_attempt_ms = now;
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
    /// Java's only caller gates `KafkaAdminClient`'s rebootstrap on it
    /// (`MetadataUpdateNodeIdProvider.provide()`, `KafkaAdminClient.java:790-797`). The Rust admin client never
    /// rebootstraps (it runs with `MetadataRecoveryStrategy::None`), so that
    /// gate has no counterpart and this is translated for completeness (DoD #2).
    #[cfg_attr(not(test), expect(dead_code))]
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
        let _ = &self.log_context;
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
}
