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
use std::net::IpAddr;

use super::HostResolver;

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
}
