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

//! Confluent Kafka Rust — translation of the Apache Kafka 4.2 Java client.
//!
//! Phase 1 surfaces only the foundational utilities (errors, time, headers,
//! configuration, UUID, byte/varint encoding, CRC, exponential backoff,
//! topic-name validation). Higher-level types (records, network, producer)
//! are added in subsequent phases.

pub mod api_versions;
pub mod client_dns_lookup;
pub mod client_request;
pub mod client_response;
pub mod client_utils;
pub mod cluster_connection_states;
pub mod common;
pub mod common_client_configs;
pub mod connection_state;
pub mod default_host_resolver;
pub mod host_resolver;
pub mod in_flight_requests;
pub mod kafka_client;
pub mod least_loaded_node;
pub mod manual_metadata_updater;
pub mod metadata;
pub mod metadata_recovery_strategy;
pub mod metadata_snapshot;
pub mod metadata_updater;
pub mod node_api_versions;
pub mod producer;
pub mod request_completion_handler;
pub mod stale_metadata_error;

pub use api_versions::ApiVersions;
pub use client_request::ClientRequest;
pub use client_response::ClientResponse;
pub use cluster_connection_states::ClusterConnectionStates;
pub use connection_state::ConnectionState;
pub use kafka_client::KafkaClient;
pub use least_loaded_node::LeastLoadedNode;
pub use manual_metadata_updater::ManualMetadataUpdater;
pub use metadata_updater::MetadataUpdater;
pub use node_api_versions::NodeApiVersions;
pub use request_completion_handler::RequestCompletionHandler;
