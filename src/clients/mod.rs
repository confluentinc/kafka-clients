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

//! Client implementations (org.apache.kafka.clients)

#[cfg(not(feature = "skip-generated"))]
pub mod api_versions;
#[cfg(not(feature = "skip-generated"))]
pub mod client_request;
#[cfg(not(feature = "skip-generated"))]
pub mod client_response;
pub mod client_utils;
pub mod cluster_connection_states;
pub mod connection_state;
pub mod host_resolver;
#[cfg(not(feature = "skip-generated"))]
pub mod in_flight_requests;
pub mod least_loaded_node;
pub mod metadata_recovery_strategy;
#[cfg(not(feature = "skip-generated"))]
pub mod node_api_versions;

/// Callback type for request completion.
///
/// Replaces Java's `RequestCompletionHandler` callback interface. Per CLAUDE.md
/// rule 9, Java callbacks are replaced with closures executed after awaiting the
/// corresponding call.
///
/// The callback receives a mutable reference to the [`client_response::ClientResponse`]
/// so it can inspect the response (e.g., extract the response body).
#[cfg(not(feature = "skip-generated"))]
pub type RequestCompletionHandler = Box<dyn FnOnce(&mut client_response::ClientResponse) + Send>;

#[cfg(not(feature = "skip-generated"))]
pub use api_versions::ApiVersions;
#[cfg(not(feature = "skip-generated"))]
pub use client_request::ClientRequest;
#[cfg(not(feature = "skip-generated"))]
pub use client_response::ClientResponse;
pub use cluster_connection_states::ClusterConnectionStates;
pub use connection_state::ConnectionState;
pub use host_resolver::{DefaultHostResolver, HostResolver};
#[cfg(not(feature = "skip-generated"))]
pub use in_flight_requests::{InFlightRequest, InFlightRequests};
pub use least_loaded_node::LeastLoadedNode;
pub use metadata_recovery_strategy::MetadataRecoveryStrategy;
#[cfg(not(feature = "skip-generated"))]
pub use node_api_versions::NodeApiVersions;
