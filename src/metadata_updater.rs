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

//! Translation of `org.apache.kafka.clients.MetadataUpdater`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.

use std::sync::Arc;

use crate::LeastLoadedNode;
use crate::common::Node;
use crate::common::errors::KafkaError;
use crate::common::requests::{MetadataRequestBuilder, MetadataResponse, RequestHeader};
use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;

/// Callback surface that [`MetadataUpdater::maybe_update`] uses to reach
/// back into the enclosing `NetworkClient`. Java's `DefaultMetadataUpdater`
/// is a (package-private) inner class on `NetworkClient` and reads /
/// mutates the enclosing instance's state directly. The Rust translation
/// can't model inner-class access; instead the `NetworkClient` implements
/// this trait for itself and passes `&mut self` (typed as
/// `&mut dyn MetadataUpdaterContext`) to the updater at call time.
///
/// **Java's contract**: the context methods correspond to Java's
/// `NetworkClient` instance methods invoked by
/// `DefaultMetadataUpdater.maybeUpdate(long, Node)`:
///
/// | Rust method                         | Java equivalent on `NetworkClient` |
/// |-------------------------------------|------------------------------------|
/// | [`Self::least_loaded_node`]         | `leastLoadedNode(long)` (but takes nodes via param to avoid the `metadataUpdater.fetchNodes()` recursion that Java permits) |
/// | [`Self::can_send_request`]          | `canSendRequest(String, long)` |
/// | [`Self::can_connect`]               | `connectionStates.canConnect(String, long)` |
/// | [`Self::is_connecting`]             | `connectionStates.isConnecting(String)` |
/// | [`Self::initiate_connect`]          | `initiateConnect(Node, long)` |
/// | [`Self::send_internal_metadata_request`] | `sendInternalMetadataRequest(MetadataRequest.Builder, String, long)` |
/// | [`Self::reconnect_backoff_ms`]      | `reconnectBackoffMs` (field) |
/// | [`Self::default_request_timeout_ms`]| `defaultRequestTimeoutMs` (field) |
/// | [`Self::metadata_recovery_strategy`]| `metadataRecoveryStrategy` (field) |
pub trait MetadataUpdaterContext {
    /// Mirrors Java's `NetworkClient.leastLoadedNode(long)`, but the
    /// Java method internally calls `metadataUpdater.fetchNodes()` which
    /// would recurse through the trait dispatch — pass the nodes
    /// explicitly so the caller (the updater) controls the source of
    /// truth.
    fn least_loaded_node(&mut self, now: i64, nodes: &[Node]) -> LeastLoadedNode;

    /// Mirrors `canSendRequest(String, long)`.
    fn can_send_request(&self, node_id: i32, now: i64) -> bool;

    /// Mirrors `connectionStates.canConnect(String, long)`.
    fn can_connect(&self, node_id: i32, now: i64) -> bool;

    /// Mirrors `connectionStates.isConnecting(String)`.
    fn is_connecting(&self, node_id: i32) -> bool;

    /// Mirrors `initiateConnect(Node, long)`.
    fn initiate_connect(&mut self, node: &Node, now: i64);

    /// Mirrors `sendInternalMetadataRequest(MetadataRequest.Builder, String, long)`.
    /// The `node_id_label` parameter is the `Arc<str>` form of
    /// `node.id_string()` — sharing it avoids per-call allocation
    /// (CLAUDE.md rule 11).
    ///
    /// Returns `Err(...)` when the underlying `do_send` rejected the
    /// request (`UnsupportedVersionException` on a METADATA api-key
    /// version island, or a `builder.build(version)` failure). The
    /// updater is responsible for routing the failure into its own
    /// `handle_failed_request` path — the `NetworkClient` cannot do
    /// it itself because the updater is owned by the caller's stack
    /// during the take/put window in
    /// [`crate::NetworkClient::poll`].
    fn send_internal_metadata_request(
        &mut self,
        builder: MetadataRequestBuilder,
        node_id_label: Arc<str>,
        now: i64,
    ) -> Result<(), KafkaError>;

    /// Read the enclosing `NetworkClient.reconnectBackoffMs`. Used as
    /// the timeout when no node is connection-ready.
    fn reconnect_backoff_ms(&self) -> i64;

    /// Read the enclosing `NetworkClient.defaultRequestTimeoutMs`. Used
    /// to derive `waitForMetadataFetch` when a fetch is in progress.
    fn default_request_timeout_ms(&self) -> i32;

    /// Read the enclosing `NetworkClient.metadataRecoveryStrategy`.
    fn metadata_recovery_strategy(&self) -> MetadataRecoveryStrategy;
}

/// The interface used by `NetworkClient` to request cluster metadata
/// info to be updated and to retrieve the cluster nodes from such
/// metadata.
///
/// Mirrors Java's `MetadataUpdater`. This is an internal trait — Java's
/// class is package-private.
///
/// **Java's contract**: this class is *not* thread-safe. The Rust trait
/// reflects that by taking `&mut self` on every state-mutating method.
pub trait MetadataUpdater: std::marker::Send {
    /// Gets the current cluster info without blocking. Mirrors
    /// `MetadataUpdater.fetchNodes()`.
    fn fetch_nodes(&self) -> Vec<Node>;

    /// Returns true if an update to the cluster metadata info is due.
    /// Mirrors `MetadataUpdater.isUpdateDue(long)`.
    fn is_update_due(&self, now: i64) -> bool;

    /// Starts a cluster metadata update if needed and possible. Returns
    /// the time until the metadata update (which would be 0 if an update
    /// has been started as a result of this call).
    ///
    /// Mirrors `MetadataUpdater.maybeUpdate(long)`. The `context`
    /// parameter is the Rust-translation hook that gives the updater
    /// access to the enclosing `NetworkClient`'s private helpers; see
    /// [`MetadataUpdaterContext`].
    fn maybe_update(&mut self, context: &mut dyn MetadataUpdaterContext, now: i64) -> i64;

    /// Handle a server disconnect. Mirrors
    /// `MetadataUpdater.handleServerDisconnect(long, String, Optional<AuthenticationException>)`.
    ///
    /// Java keys by `String` (the `Integer.toString(node.id())` connection
    /// id); the Rust signature takes `i32` directly to match the rest of
    /// the network plumbing (`Selectable`, `KafkaClient`,
    /// `InFlightRequests`, `ClusterConnectionStates`) and avoid a per-call
    /// `String` allocation at the network-loop call site (CLAUDE.md rule
    /// 11, NOTES.md "Hot-path identifier interning").
    ///
    /// `maybe_auth_error` mirrors Java's `Optional<AuthenticationException>`
    /// — `Some(...)` only when the disconnect was driven by an auth
    /// failure. The Rust translation projects all `KafkaError`s onto
    /// `Option<KafkaError>`; callers should pass an
    /// [`KafkaError::Authentication`] variant.
    fn handle_server_disconnect(&mut self, now: i64, node_id: i32, maybe_auth_error: Option<KafkaError>);

    /// Handle a metadata request failure. Mirrors
    /// `MetadataUpdater.handleFailedRequest(long, Optional<KafkaException>)`.
    fn handle_failed_request(&mut self, now: i64, maybe_fatal_error: Option<KafkaError>);

    /// Handle responses for metadata requests. Mirrors
    /// `MetadataUpdater.handleSuccessfulResponse(RequestHeader, long, MetadataResponse)`.
    fn handle_successful_response(
        &mut self,
        request_header: &RequestHeader,
        now: i64,
        metadata_response: MetadataResponse,
    );

    /// Returns true if metadata couldn't be fetched for
    /// `rebootstrap_trigger_ms` or if the server requested rebootstrap.
    /// Mirrors the default-implementation
    /// `MetadataUpdater.needsRebootstrap(long, long)`.
    fn needs_rebootstrap(&self, _now: i64, _rebootstrap_trigger_ms: i64) -> bool {
        false
    }

    /// Performs rebootstrap, replacing the existing cluster with the
    /// bootstrap cluster. Mirrors the default-implementation
    /// `MetadataUpdater.rebootstrap(long)`.
    fn rebootstrap(&mut self, _now: i64) {}

    /// Close this updater. Java extends `Closeable`; we model the same
    /// surface with an explicit `close` method (Rust traits do not
    /// automatically inherit `Drop`-style cleanup).
    fn close(&mut self);
}
