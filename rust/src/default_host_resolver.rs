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
use std::net::{IpAddr, Ipv4Addr};

use crate::HostResolver;

/// The default host resolver using Tokio's async DNS resolution.
///
/// Translates `org.apache.kafka.clients.DefaultHostResolver`.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.DefaultHostResolver")]
pub struct DefaultHostResolver;

impl DefaultHostResolver {
    /// What Java's `InetAddress.getAllByName` returns for an empty host name.
    ///
    /// Java's `DefaultHostResolver.resolve` is `InetAddress.getAllByName(host)`,
    /// and the JDK answers an empty (or `null`) host without a name-service
    /// lookup: `getAllByName` returns `{ impl.loopbackAddress() }`. With the
    /// default `java.net.preferIPv6Addresses=false`, `loopbackAddress()` is the
    /// IPv4 `127.0.0.1` (named `localhost`); only when IPv6 is preferred and an
    /// interface carries `::1` is it the IPv6 loopback. This crate models the
    /// default JVM, the same choice as the IPv4 preference in
    /// `ClientUtils::filter_preferred_addresses`.
    ///
    /// `getaddrinfo("")` fails instead (`EAI_NONAME`), so every resolution
    /// path Java routes through `InetAddress` maps `""` here explicitly: this
    /// resolver and the lookup in `ClientUtils::parse_and_validate_addresses`.
    /// An empty host reaches them from a bootstrap URL such as `":9092"`,
    /// which Java's `HOST_PORT_PATTERN` accepts.
    pub(crate) const EMPTY_HOST_ADDRESS: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

    /// The host name of [`Self::EMPTY_HOST_ADDRESS`]: the JDK's
    /// `loopbackAddress()` builds it as `new Inet4Address("localhost", ...)`.
    ///
    /// It is what Java's `InetSocketAddress.getHostString()` returns for
    /// `new InetSocketAddress("", port)`: the host resolves, so the socket
    /// address keeps no literal host name and falls back to the resolved
    /// address's name. `ClientUtils::parse_and_validate_addresses` uses it as
    /// the bootstrap node host for an empty URL host, so the node is never
    /// empty (`Node::is_empty`) and TLS has a server name.
    pub(crate) const EMPTY_HOST_NAME: &'static str = "localhost";

    /// Creates a new `DefaultHostResolver`.
    pub fn new() -> Self {
        Self
    }
}

impl HostResolver for DefaultHostResolver {
    async fn resolve(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        // `InetAddress.getAllByName("")` is the loopback address, not a lookup.
        if host.is_empty() {
            return Ok(vec![Self::EMPTY_HOST_ADDRESS]);
        }
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

    /// Java's `InetAddress.getAllByName("")` returns `[127.0.0.1]` (the
    /// default-JVM `loopbackAddress()`) without a DNS lookup, whereas
    /// `getaddrinfo("")` fails.
    #[tokio::test]
    async fn test_resolve_empty_host_is_loopback() {
        let resolver = DefaultHostResolver::new();
        let addrs = resolver.resolve("").await.unwrap();
        assert_eq!(addrs, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        assert_eq!(
            crate::ClientUtils::resolve("", &resolver).await.unwrap(),
            vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]
        );
    }

    #[tokio::test]
    async fn test_resolve_unknown_host() {
        let resolver = DefaultHostResolver::new();
        let result = resolver.resolve("this.host.does.not.exist.example.invalid").await;
        assert!(result.is_err());
    }
}
