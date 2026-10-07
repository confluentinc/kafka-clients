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

//! Metadata updater trait used by `NetworkClient`.
//!
//! Corresponds to `org.apache.kafka.clients.MetadataUpdater`.
//!
//! This is an internal trait. It is NOT thread-safe — it is intended to be
//! used only from the NetworkClient's single-threaded event loop.

use std::net::SocketAddr;

use crate::common::Node;
use crate::common::requests::MetadataResponse;
use crate::common::requests::RequestHeader;

use crate::common::Error;

/// The interface used by `NetworkClient` to request cluster metadata info to be
/// updated and to retrieve the cluster nodes from such metadata.
///
/// Corresponds to `org.apache.kafka.clients.MetadataUpdater`.
///
/// This is an internal trait. It is NOT thread-safe.
#[doc(alias = "org.apache.kafka.clients.MetadataUpdater")]
pub trait MetadataUpdater: Send {
    /// Gets the current cluster info without blocking.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#fetchNodes")]
    fn fetch_nodes(&self) -> Vec<Node>;

    /// Returns `true` if an update to the cluster metadata info is due.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#isUpdateDue")]
    fn is_update_due(&self, now: i64) -> bool;

    /// Starts a cluster metadata update if needed and possible.
    ///
    /// Returns the time until the metadata update (which would be 0 if an update
    /// has been started as a result of this call).
    ///
    /// If the implementation relies on `NetworkClient` to send requests,
    /// `handle_successful_response` will be invoked after the metadata response
    /// is received.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#maybeUpdate")]
    fn maybe_update(&mut self, now: i64) -> i64;

    /// Handle a server disconnect.
    ///
    /// This provides a mechanism for the `MetadataUpdater` implementation to use
    /// the `NetworkClient` instance for its own requests with special handling for
    /// disconnections of such requests.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#handleServerDisconnect")]
    fn handle_server_disconnect(&mut self, now: i64, node_id: &str, maybe_auth_error: Option<Error>);

    /// Handle a metadata request failure.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#handleFailedRequest")]
    fn handle_failed_request(&mut self, now: i64, maybe_fatal_error: Option<Error>);

    /// Handle responses for metadata requests.
    ///
    /// This provides a mechanism for the `MetadataUpdater` implementation to use
    /// the `NetworkClient` instance for its own requests with special handling for
    /// completed receives of such requests.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#handleSuccessfulResponse")]
    fn handle_successful_response(
        &mut self,
        request_header: &RequestHeader,
        now: i64,
        metadata_response: &MetadataResponse,
    );

    /// Returns `true` if metadata couldn't be fetched for `rebootstrap_trigger_ms`
    /// or if server requested rebootstrap.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#needsRebootstrap")]
    fn needs_rebootstrap(&self, _now: i64, _rebootstrap_trigger_ms: i64) -> bool {
        false
    }

    /// Performs rebootstrap, replacing the existing cluster with the bootstrap cluster.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#rebootstrap")]
    fn rebootstrap(&mut self, _now: i64) {}

    /// Record a permanent bootstrap DNS resolution failure so all API calls see
    /// the same error (KIP-909).
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#bootstrapFailed")]
    fn bootstrap_failed(&mut self, _error: Error) {}

    /// Returns `true` if the metadata has been bootstrapped.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#isBootstrapped")]
    fn is_bootstrapped(&self) -> bool;

    /// Bootstrap the metadata cache with the given addresses: the
    /// `(host, address)` pairs of the bootstrap servers, as
    /// [`ClientUtils::parse_addresses`](crate::ClientUtils::parse_addresses)
    /// returns them (Java's `List<InetSocketAddress>`).
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#bootstrap")]
    fn bootstrap(&mut self, addresses: Vec<(String, SocketAddr)>);

    /// Close this updater.
    #[doc(alias = "org.apache.kafka.clients.MetadataUpdater#close")]
    fn close(&mut self);
}
