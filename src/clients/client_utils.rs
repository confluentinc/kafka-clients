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

//! Client utility functions.
//!
//! Translated from `org.apache.kafka.clients.ClientUtils`.

use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use log::warn;

use super::HostResolver;
use crate::common::kafka_error::KafkaError;

/// Resolves a hostname using the given resolver and filters preferred addresses.
///
/// Returns a list containing the first address and subsequent addresses of the same
/// type (IPv4 or IPv6) as the first address.
///
/// # Errors
/// Returns an `io::Error` if the hostname cannot be resolved.
pub async fn resolve<H: HostResolver>(host: &str, host_resolver: &H) -> io::Result<Vec<IpAddr>> {
    let addresses = host_resolver.resolve(host).await?;
    let result = filter_preferred_addresses(&addresses);
    log::debug!("Resolved host {} as {:?}", host, result);
    Ok(result)
}

/// Return a list containing the first address and subsequent addresses
/// that are the same type (IPv4 or IPv6) as the first address.
///
/// The outcome is that all returned addresses are either IPv4 or IPv6.
fn filter_preferred_addresses(all_addresses: &[IpAddr]) -> Vec<IpAddr> {
    if all_addresses.is_empty() {
        return Vec::new();
    }
    let first = all_addresses[0];
    let is_ipv4 = first.is_ipv4();
    all_addresses.iter().filter(|addr| addr.is_ipv4() == is_ipv4).copied().collect()
}

/// Parse and validate a list of bootstrap server URLs into socket addresses.
///
/// Each entry should be a `"host:port"` string. Hostnames are resolved via DNS.
/// Entries that cannot be resolved are logged as warnings and skipped.
///
/// Translated from `ClientUtils.parseAndValidateAddresses(List<String>, ClientDnsLookup)`.
///
/// # Errors
///
/// Returns [`KafkaError::IllegalArgument`] if no valid addresses can be resolved
/// (corresponds to Java's `ConfigException`).
pub fn parse_and_validate_addresses(urls: &[String]) -> Result<Vec<SocketAddr>, KafkaError> {
    let mut addresses = Vec::new();
    for url in urls {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            continue;
        }
        // ToSocketAddrs handles both "ip:port" and "hostname:port" with DNS resolution
        match trimmed.to_socket_addrs() {
            Ok(addrs) => {
                let resolved: Vec<SocketAddr> = addrs.collect();
                if resolved.is_empty() {
                    warn!(
                        "Couldn't resolve server {} from {} as DNS resolution failed",
                        url,
                        super::common_client_configs::BOOTSTRAP_SERVERS_CONFIG
                    );
                } else {
                    addresses.extend(resolved);
                }
            },
            Err(e) => {
                warn!(
                    "Couldn't resolve server {} from {}: {}",
                    url,
                    super::common_client_configs::BOOTSTRAP_SERVERS_CONFIG,
                    e
                );
            },
        }
    }
    if addresses.is_empty() {
        return Err(KafkaError::illegal_argument(format!(
            "No resolvable bootstrap urls given in {}",
            super::common_client_configs::BOOTSTRAP_SERVERS_CONFIG
        )));
    }
    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_filter_preferred_addresses_ipv4_first() {
        let addrs = vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        ];
        let filtered = filter_preferred_addresses(&addrs);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|a| a.is_ipv4()));
    }

    #[test]
    fn test_filter_preferred_addresses_ipv6_first() {
        let addrs = vec![
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 2)),
        ];
        let filtered = filter_preferred_addresses(&addrs);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|a| a.is_ipv6()));
    }

    #[test]
    fn test_filter_preferred_addresses_empty() {
        let filtered = filter_preferred_addresses(&[]);
        assert!(filtered.is_empty());
    }

    /// Translated from `ClientUtilsTest.testParseAndValidateAddresses`.
    #[test]
    fn test_parse_and_validate_addresses_ip_port() {
        let urls = vec!["127.0.0.1:9092".to_string()];
        let result = parse_and_validate_addresses(&urls);
        assert!(result.is_ok());
        let addrs = result.unwrap();
        assert!(!addrs.is_empty());
        assert_eq!(addrs[0].port(), 9092);
    }

    /// Translated from `ClientUtilsTest.testParseAndValidateAddresses` — multiple servers.
    #[test]
    fn test_parse_and_validate_addresses_multiple() {
        let urls = vec!["127.0.0.1:9092".to_string(), "127.0.0.1:9093".to_string()];
        let result = parse_and_validate_addresses(&urls);
        assert!(result.is_ok());
        let addrs = result.unwrap();
        assert_eq!(addrs.len(), 2);
    }

    /// Translated from `ClientUtilsTest.testParseAndValidateAddresses` — empty list.
    #[test]
    fn test_parse_and_validate_addresses_empty() {
        let urls: Vec<String> = Vec::new();
        let result = parse_and_validate_addresses(&urls);
        assert!(result.is_err());
    }

    /// Translated from `ClientUtilsTest.testParseAndValidateAddresses` — unresolvable host.
    #[test]
    fn test_parse_and_validate_addresses_unresolvable() {
        let urls = vec!["this.host.does.not.exist.ever.kafka.test:9092".to_string()];
        let result = parse_and_validate_addresses(&urls);
        assert!(result.is_err());
    }

    /// Test that whitespace-only entries are skipped.
    #[test]
    fn test_parse_and_validate_addresses_whitespace_only() {
        let urls = vec!["  ".to_string(), "".to_string()];
        let result = parse_and_validate_addresses(&urls);
        assert!(result.is_err());
    }

    /// Test that localhost resolution works.
    #[test]
    fn test_parse_and_validate_addresses_localhost() {
        let urls = vec!["localhost:9092".to_string()];
        let result = parse_and_validate_addresses(&urls);
        assert!(result.is_ok());
        let addrs = result.unwrap();
        assert!(!addrs.is_empty());
        assert_eq!(addrs[0].port(), 9092);
    }
}
