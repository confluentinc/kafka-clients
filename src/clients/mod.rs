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
pub mod connection_state;
pub mod host_resolver;
pub mod least_loaded_node;
pub mod metadata_recovery_strategy;
#[cfg(not(feature = "skip-generated"))]
pub mod node_api_versions;

#[cfg(not(feature = "skip-generated"))]
pub use api_versions::ApiVersions;
pub use connection_state::ConnectionState;
pub use host_resolver::{DefaultHostResolver, HostResolver};
pub use least_loaded_node::LeastLoadedNode;
pub use metadata_recovery_strategy::MetadataRecoveryStrategy;
#[cfg(not(feature = "skip-generated"))]
pub use node_api_versions::NodeApiVersions;
