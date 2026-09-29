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

//! The default host resolver.
//!
//! Translated from `org.apache.kafka.clients.DefaultHostResolver`.

use std::io;
use std::net::IpAddr;

use crate::HostResolver;

/// The default host resolver using Tokio's async DNS resolution.
///
/// Translates `org.apache.kafka.clients.DefaultHostResolver`.
#[derive(Debug, Default, Clone)]
pub struct DefaultHostResolver;

impl DefaultHostResolver {
    /// Creates a new `DefaultHostResolver`.
    pub fn new() -> Self {
        Self
    }
}

impl HostResolver for DefaultHostResolver {
    async fn resolve(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        // Use tokio's async DNS lookup. We pass port 0 since we only need IP addresses.
        let addrs: Vec<IpAddr> = tokio::net::lookup_host(format!("{}:0", host))
            .await?
            .map(|socket_addr| socket_addr.ip())
            .collect();
        if addrs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("No addresses found for host: {}", host),
            ));
        }
        Ok(addrs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resolve_localhost() {
        let resolver = DefaultHostResolver::new();
        let addrs = resolver.resolve("localhost").await.unwrap();
        assert!(!addrs.is_empty());
        // localhost should resolve to a loopback address
        assert!(addrs.iter().any(|a| a.is_loopback()));
    }

    #[tokio::test]
    async fn test_resolve_unknown_host() {
        let resolver = DefaultHostResolver::new();
        let result = resolver.resolve("this.host.does.not.exist.example.invalid").await;
        assert!(result.is_err());
    }
}
