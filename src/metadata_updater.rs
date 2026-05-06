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

use crate::common::Node;
use crate::common::errors::KafkaError;
use crate::common::requests::{MetadataResponse, RequestHeader};

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
    /// Mirrors `MetadataUpdater.maybeUpdate(long)`.
    fn maybe_update(&mut self, now: i64) -> i64;

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
