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

//! Translation of `org.apache.kafka.clients.HostResolver`.

use std::net::IpAddr;

use crate::common::errors::KafkaError;

/// Single-method interface used by `ClientUtils::resolve` and
/// `NetworkClient` to resolve a hostname to a list of IP addresses.
/// Mirrors Java's `HostResolver`.
///
/// Java throws `UnknownHostException`; the Rust translation surfaces
/// failures as `KafkaError::Network(_)` (retriable, librdkafka
/// `_TRANSPORT`).
///
/// `resolve` is **synchronous** because Java's
/// `InetAddress.getAllByName(...)` is also blocking and the producer
/// only invokes it during bootstrap and metadata refresh — never on the
/// per-message hot path. Phase 5/6 may wrap the call in
/// `tokio::task::spawn_blocking` when invoked from an async context.
pub trait HostResolver: Send + Sync {
    /// Resolve `host` to a list of IP addresses. Returns
    /// `KafkaError::Network` on lookup failure (Java
    /// `UnknownHostException`).
    fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, KafkaError>;
}
