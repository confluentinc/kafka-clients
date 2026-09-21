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

// Fail on warnings in development
#![deny(warnings)]

pub mod admin;
pub mod api_versions;
mod client_request;
mod client_response;
mod client_utils;
mod cluster_connection_states;
pub mod common;
mod common_client_configs;
mod connection_state;
pub mod consumer;
mod default_host_resolver;
pub mod fetch_session_handler;
mod host_resolver;
mod in_flight_requests;
mod kafka_client;
mod least_loaded_node;
pub mod metadata;
mod metadata_recovery_strategy;
mod metadata_snapshot;
mod metadata_updater;
#[cfg(test)]
mod mock_client;
mod network_client;
mod network_client_utils;
mod node_api_versions;
pub mod producer;
#[cfg(test)]
mod test_alloc_tracker;

#[cfg(feature = "ffi")]
pub mod ffi;

/// Callback type for request completion.
///
/// Replaces Java's `RequestCompletionHandler` callback interface. Per CLAUDE.md
/// rule 9, Java callbacks are replaced with closures executed after awaiting the
/// corresponding call.
///
/// The callback receives a mutable reference to the [`client_response::ClientResponse`]
/// so it can inspect the response (e.g., extract the response body).
pub type RequestCompletionHandler = Box<dyn FnOnce(&mut client_response::ClientResponse) + Send>;

pub use api_versions::ApiVersions;
pub use client_request::ClientRequest;
pub use client_response::ClientResponse;
pub(crate) use client_utils::ClientUtils;
pub use cluster_connection_states::ClusterConnectionStates;
pub(crate) use common_client_configs::CommonClientConfigs;
pub use connection_state::ConnectionState;
pub use default_host_resolver::DefaultHostResolver;
pub use host_resolver::HostResolver;
pub use in_flight_requests::{InFlightRequest, InFlightRequests};
pub use kafka_client::KafkaClient;
pub use least_loaded_node::LeastLoadedNode;
pub use metadata::Metadata;
pub use metadata_recovery_strategy::MetadataRecoveryStrategy;
pub use metadata_snapshot::MetadataSnapshot;
pub use metadata_updater::MetadataUpdater;
pub use node_api_versions::NodeApiVersions;
// Only the outer class is re-exported: `FetchRequestData` (Rust
// `FetchSessionRequestData`) and `Builder` are Java nested classes
// (`FetchSessionHandler.java:95,231`), so per CLAUDE.md §2 they keep the
// file-module path that plays the outer-class qualifier.
pub use fetch_session_handler::FetchSessionHandler;
#[cfg(test)]
pub(crate) use mock_client::{MockClient, RequestMatcher};
pub(crate) use network_client::{NetworkClient, NetworkClientStatics};
pub(crate) use network_client_utils::NetworkClientUtils;
#[cfg(test)]
pub(crate) use test_alloc_tracker::AllocTrackingGuard;

// Include generated message definitions
#[allow(dead_code, clippy::all)]
pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/generated/mod.rs"));
}

pub use generated::*;
