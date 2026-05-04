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

//! Translation of `org.apache.kafka.clients.DefaultHostResolver`.

use std::net::{IpAddr, ToSocketAddrs};

use crate::common::errors::KafkaError;
use crate::host_resolver::HostResolver;

/// Default [`HostResolver`] that uses the operating system's blocking
/// resolver. Mirrors Java's `DefaultHostResolver` which calls
/// `InetAddress.getAllByName(host)`.
///
/// **Blocking semantics**: Rust's stdlib `(host, port).to_socket_addrs()`
/// is blocking, matching Java. The producer only invokes this from
/// bootstrap / metadata refresh paths (off the hot send path), so a
/// blocking call is acceptable here. Phase 5 will wrap the call in
/// `tokio::task::spawn_blocking` when invoked from async code.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultHostResolver;

impl DefaultHostResolver {
    pub fn new() -> Self {
        Self
    }
}

impl HostResolver for DefaultHostResolver {
    fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, KafkaError> {
        // Java's `InetAddress.getAllByName(host)` resolves a bare
        // hostname; `ToSocketAddrs` requires a port, so we pass 0 and
        // discard it from each `SocketAddr` to recover the IP only.
        let resolved = (host, 0u16).to_socket_addrs().map_err(|e| {
            KafkaError::Network(format!("Unknown host: {host}: {e}"))
        })?;
        Ok(resolved.map(|sa| sa.ip()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_localhost_succeeds() {
        let resolver = DefaultHostResolver::new();
        let ips = resolver.resolve("localhost").expect("localhost should resolve");
        assert!(!ips.is_empty(), "localhost should yield at least one IP");
    }

    #[test]
    fn resolve_ipv4_literal() {
        let resolver = DefaultHostResolver::new();
        let ips = resolver.resolve("127.0.0.1").expect("ipv4 literal should resolve");
        assert!(ips.iter().any(|ip| ip.is_loopback()));
    }
}
