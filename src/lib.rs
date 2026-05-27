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

pub mod api_versions;
pub mod client_request;
pub mod client_response;
pub(crate) mod client_utils;
pub mod cluster_connection_states;
pub mod common;
pub(crate) mod common_client_configs;
pub mod connection_state;
pub mod consumer;
pub mod fetch_session_handler;
pub mod host_resolver;
pub mod in_flight_requests;
pub mod kafka_client;
pub mod least_loaded_node;
pub mod metadata;
pub mod metadata_recovery_strategy;
pub mod metadata_snapshot;
pub mod metadata_updater;
#[cfg(test)]
pub(crate) mod mock_client;
pub(crate) mod network_client;
pub(crate) mod network_client_utils;
pub mod node_api_versions;
pub mod producer;
#[cfg(test)]
pub(crate) mod test_alloc_tracker;

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
pub use cluster_connection_states::ClusterConnectionStates;
pub use connection_state::ConnectionState;
pub use host_resolver::{DefaultHostResolver, HostResolver};
pub use in_flight_requests::{InFlightRequest, InFlightRequests};
pub use kafka_client::KafkaClient;
pub use least_loaded_node::LeastLoadedNode;
pub use metadata::Metadata;
pub use metadata_recovery_strategy::MetadataRecoveryStrategy;
pub use metadata_snapshot::MetadataSnapshot;
pub use metadata_updater::MetadataUpdater;
pub use node_api_versions::NodeApiVersions;

// Include generated message definitions
#[allow(dead_code, clippy::all)]
pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/generated/mod.rs"));
}

pub use generated::*;
