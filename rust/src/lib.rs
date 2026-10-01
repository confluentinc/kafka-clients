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
// The public-surface leak detector: a `pub` item inside a `pub(crate)` module
// that still escapes through a public signature. `private_interfaces` misses
// that shape, and `xtask`'s `check-public-audience` relies on this lint for it.
#![warn(unnameable_types)]

pub mod admin;
// The `org.apache.kafka.clients` classes below are not
// `@InterfaceAudience.Public`, so they are crate-private. They are translated
// in full (DoD #2) though the client calls only part of them, hence the
// `dead_code` allowances.
#[expect(dead_code)]
mod api_versions;
mod client_dns_lookup;
mod client_request;
mod client_response;
mod client_utils;
#[cfg_attr(not(test), expect(dead_code))]
mod cluster_connection_states;
pub mod common;
mod common_client_configs;
mod connection_state;
pub mod consumer;
mod default_host_resolver;
mod fetch_session_handler;
mod host_resolver;
#[cfg_attr(not(test), expect(dead_code))]
mod in_flight_requests;
#[cfg_attr(not(test), expect(dead_code))]
mod kafka_client;
mod least_loaded_node;
#[expect(dead_code)]
mod metadata;
mod metadata_recovery_strategy;
mod metadata_snapshot;
mod metadata_updater;
#[cfg(test)]
mod mock_client;
mod network_client;
mod network_client_utils;
#[cfg_attr(not(test), expect(dead_code))]
mod node_api_versions;
mod preview_warning;
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
#[doc(alias = "org.apache.kafka.clients.RequestCompletionHandler")]
pub(crate) type RequestCompletionHandler = Box<dyn FnOnce(&mut client_response::ClientResponse) + Send>;

pub(crate) use api_versions::ApiVersions;
pub(crate) use client_dns_lookup::ClientDnsLookup;
pub(crate) use client_request::ClientRequest;
pub(crate) use client_response::ClientResponse;
pub(crate) use client_utils::ClientUtils;
pub(crate) use cluster_connection_states::ClusterConnectionStates;
pub(crate) use common_client_configs::CommonClientConfigs;
pub(crate) use connection_state::ConnectionState;
pub(crate) use default_host_resolver::DefaultHostResolver;
pub(crate) use host_resolver::HostResolver;
pub(crate) use in_flight_requests::{InFlightRequest, InFlightRequests};
pub(crate) use kafka_client::KafkaClient;
pub(crate) use least_loaded_node::LeastLoadedNode;
pub(crate) use metadata::Metadata;
pub(crate) use metadata_recovery_strategy::MetadataRecoveryStrategy;
pub(crate) use metadata_snapshot::MetadataSnapshot;
pub(crate) use metadata_updater::MetadataUpdater;
pub(crate) use node_api_versions::NodeApiVersions;
// Only the outer class is re-exported: `FetchRequestData` (Rust
// `FetchSessionRequestData`) and `Builder` are Java nested classes
// (`FetchSessionHandler.java:95,231`), so per CLAUDE.md §2 they keep the
// file-module path that plays the outer-class qualifier.
#[expect(unused_imports)]
pub(crate) use fetch_session_handler::FetchSessionHandler;
#[cfg(test)]
pub(crate) use mock_client::{MockClient, RequestMatcher};
pub(crate) use network_client::{NetworkClient, NetworkClientStatics};
pub(crate) use network_client_utils::NetworkClientUtils;
#[cfg(test)]
pub(crate) use test_alloc_tracker::AllocTrackingGuard;

// Include generated message definitions. They translate Java's generated
// `org.apache.kafka.common.message` classes, whose package is not Public API.
#[expect(dead_code, unused_imports, clippy::all)]
pub(crate) mod generated {
    include!(concat!(env!("OUT_DIR"), "/generated/mod.rs"));
}

pub(crate) use generated::*;

// Test-only message definitions from `generator/test-messages/`, for in-crate
// tests that also need `pub(crate)` types (e.g. `common::message`'s
// `RecordsSerdeTest`). `build.rs` always generates them.
#[cfg(test)]
#[expect(unused_imports, clippy::all)]
pub(crate) mod test_generated {
    include!(concat!(env!("OUT_DIR"), "/test_generated/mod.rs"));
}

// Docker-backed integration tests that need crate-internal types; see the
// module docs. The public-API suites stay in `tests/integration`.
#[cfg(all(test, feature = "integration-tests"))]
mod integration_tests;
// The shared `tests/common` harness, compiled into `integration_tests` by path,
// names the crate `confluent_kafka::` as the external `tests/integration` binary
// does; this alias resolves those paths inside the crate's own test binary.
#[cfg(all(test, feature = "integration-tests"))]
extern crate self as confluent_kafka;
