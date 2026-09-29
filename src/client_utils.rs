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
use crate::common::Error;
use crate::{ClientDnsLookup, CommonClientConfigs};

/// Translates the Java static-utility class `org.apache.kafka.clients.ClientUtils`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
pub struct ClientUtils;

impl ClientUtils {
    /// Resolves a hostname using the given resolver and filters preferred addresses.
    ///
    /// Returns the resolved addresses of a single family: the IPv4 addresses if
    /// any, otherwise the IPv6 ones (see [`Self::filter_preferred_addresses`]
    /// for why IPv4 is preferred rather than the first-listed family).
    ///
    /// # Errors
    /// Returns an `io::Error` if the hostname cannot be resolved.
    pub async fn resolve<H: HostResolver>(host: &str, host_resolver: &H) -> io::Result<Vec<IpAddr>> {
        let addresses = host_resolver.resolve(host).await?;
        let result = Self::filter_preferred_addresses(&addresses);
        log::debug!("Resolved host {} as {:?}", host, result);
        Ok(result)
    }

    /// Return the addresses of a single family (all IPv4 or all IPv6) out of
    /// `all_addresses`, preserving their order: the IPv4 addresses if there
    /// are any, otherwise the IPv6 addresses.
    ///
    /// Translated from `ClientUtils.filterPreferredAddresses`, with one
    /// deliberate deviation in how the family is chosen.
    ///
    /// # Deviation: IPv4 is preferred when both families are present
    ///
    /// Java returns "the first address in `allAddresses` and subsequent
    /// addresses that are a subtype of the first address" — whichever family
    /// the resolver lists first wins. That source has no family preference of
    /// its own; in practice Java clients connect over IPv4 because the JVM's
    /// `InetAddress` resolution orders IPv4 first by default (the
    /// `java.net.preferIPv6Addresses` system property defaults to `false`).
    /// That is JVM behaviour, not Kafka code, and Rust has no equivalent:
    /// `tokio::net::lookup_host` returns the raw `getaddrinfo` order, and on
    /// macOS `getaddrinfo("localhost")` can list `::1` before `127.0.0.1`.
    ///
    /// Translated literally, the client then connected to `::1` only. That
    /// broke the common local setup of a broker in an IPv4-only Docker
    /// network advertising `localhost` (e.g. `confluent local kafka start`):
    /// the Java client works there, the Rust client could not connect. To give
    /// Rust users the behaviour Java users get by default, the IPv4 subset is
    /// chosen whenever the list contains any IPv4 address, independent of
    /// input order. A host that resolves only to IPv6 is unaffected.
    ///
    /// **Regression (dual-stack host, IPv4 unreachable).** When a name has
    /// both A and AAAA records, the IPv6 addresses are discarded here, and
    /// `ClusterConnectionStates` round-robins only the IPv4 ones, so IPv6 is
    /// **never** attempted, even when the IPv4 path is unreachable (for
    /// example an IPv6-only subnet or pod network with IPv4 firewalled). If
    /// `getaddrinfo` listed the AAAA record first, which RFC 6724 ordering
    /// typically does when IPv4 is unroutable, the previous Rust behaviour
    /// connected over IPv6. That topology is now strictly worse and cannot
    /// connect at all. A default-configured JVM behaves the same way, but a
    /// Java user can recover with `-Djava.net.preferIPv6Addresses=true`.
    /// This crate has **no equivalent override**.
    ///
    /// Java offers no Kafka-level knob for this (only the JVM property), so
    /// none is added here.
    fn filter_preferred_addresses(all_addresses: &[IpAddr]) -> Vec<IpAddr> {
        let prefer_ipv4 = all_addresses.iter().any(IpAddr::is_ipv4);
        all_addresses
            .iter()
            .filter(|addr| addr.is_ipv4() == prefer_ipv4)
            .copied()
            .collect()
    }

    /// Parse and validate a list of bootstrap server URLs into socket addresses.
    ///
    /// Each entry should be a `"host:port"` string. Invalid entries (embedded
    /// whitespace, missing or invalid port) cause an immediate error, matching
    /// Java's `ConfigException` behavior.
    ///
    /// Translated from `ClientUtils.parseAndValidateAddresses(List<String>, ClientDnsLookup)`.
    /// The `(AbstractConfig)` overload is covered by callers passing their
    /// config's `bootstrap.servers` and typed `client.dns.lookup`; the
    /// `(List<String>, String)` overload is [`ClientDnsLookup::for_config`]
    /// followed by this method.
    ///
    /// Each returned pair is the host string the client connects to (Java's
    /// `InetSocketAddress.getHostString()`, which becomes the bootstrap
    /// `Node`'s host) and one address it resolved to at validation time. As
    /// in Java, the per-connection resolution happens later, from the host
    /// string, in `ClusterConnectionStates` via [`Self::resolve`]; the resolved
    /// address here only proves the host resolvable (and carries the port).
    ///
    /// Per `client_dns_lookup`, mirroring Java's branches:
    ///
    /// - [`ClientDnsLookup::UseAllDnsIps`]: exactly **one** entry per URL,
    ///   keyed by the literal host from the URL (Java's
    ///   `new InetSocketAddress(host, port)`). The host is not expanded into
    ///   one entry per IP — that would create duplicate bootstrap nodes for
    ///   the same broker; `use_all_dns_ips` walks the IPs at connect time. A
    ///   host that does not resolve is logged and skipped.
    /// - [`ClientDnsLookup::ResolveCanonicalBootstrapServersOnly`]: one entry
    ///   per address the host resolves to (Java's `InetAddress.getAllByName`),
    ///   keyed by that address's canonical host name (Java's
    ///   `getCanonicalHostName()`). A host that does not resolve at all is an
    ///   error (Java's `UnknownHostException` → `ConfigException`); a canonical
    ///   name that does not resolve is logged and skipped.
    ///
    /// # Deviation: canonical host name
    ///
    /// Java's `getCanonicalHostName()` performs a reverse-DNS (PTR) lookup and,
    /// when that fails, returns the address's textual IP. The Rust standard
    /// library (and tokio) expose no reverse-DNS API, and this crate has no DNS
    /// dependency, so the canonical name is always the textual IP — i.e. the
    /// result Java produces when the reverse lookup fails. The per-address
    /// expansion and the resolvability checks are otherwise identical.
    ///
    /// The consequence is that in this mode **the bootstrap `Node`'s host is
    /// always the textual IP**, and that host is the TLS peer host:
    ///
    /// - **TLS (`SSL` / `SASL_SSL`)**: the SNI value and the server name that
    ///   hostname verification checks (on by default,
    ///   `ssl.endpoint.identification.algorithm=https`) are the IP address.
    ///   Brokers whose certificates carry only DNS-name SANs (no IP SAN) are
    ///   therefore rejected during the handshake, and every bootstrap
    ///   connection fails. Java, given working PTR records, connects to the
    ///   FQDN and verification passes. Before `client.dns.lookup` was honoured,
    ///   this crate ignored the setting and connected by host name, so such a
    ///   configuration used to work. The producer, consumer and admin configs
    ///   log a warning for this combination when they are built. Use
    ///   `use_all_dns_ips`, or add IP SANs to the broker certificates.
    /// - **SASL/GSSAPI (Kerberos)**: its service principal is built from the
    ///   host name. That mechanism is not implemented in this crate.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] (Java's `ConfigException`, built with its
    /// single-message constructor, so the message carries no
    /// `Invalid value ... for configuration ...` prefix) if:
    /// - Any URL contains embedded whitespace (newlines, spaces, tabs) after trimming
    ///   leading/trailing whitespace — these indicate user error (e.g., space-separated
    ///   or newline-separated addresses in a single string).
    /// - Any URL is missing a port or has an invalid port number (not 0-65535).
    /// - In canonical mode, a host cannot be resolved at all.
    /// - No valid addresses can be resolved after validation.
    pub fn parse_and_validate_addresses(
        urls: &[String],
        client_dns_lookup: ClientDnsLookup,
    ) -> Result<Vec<(String, SocketAddr)>, Error> {
        Self::parse_and_validate_addresses_with_lookup(
            urls,
            client_dns_lookup,
            |host, port| (host, port).to_socket_addrs().map(Iterator::collect),
            |address| address.to_string(),
        )
    }

    /// Body of [`Self::parse_and_validate_addresses`] with its two name-service
    /// calls injected: `resolve_all` (Java's `InetAddress.getAllByName` /
    /// `InetSocketAddress` resolution) and `canonical_host_name` (Java's
    /// `InetAddress.getCanonicalHostName`). Tests supply fakes here where
    /// `ClientUtilsTest` uses Mockito's `mockStatic` / `mockConstruction`.
    fn parse_and_validate_addresses_with_lookup<R, C>(
        urls: &[String],
        client_dns_lookup: ClientDnsLookup,
        resolve_all: R,
        canonical_host_name: C,
    ) -> Result<Vec<(String, SocketAddr)>, Error>
    where
        R: Fn(&str, u16) -> io::Result<Vec<SocketAddr>>,
        C: Fn(IpAddr) -> String,
    {
        // Java's `new InetSocketAddress(host, port)`: `Some(address)` if the
        // host resolves, `None` for an unresolved address. `InetAddress.getByName`
        // picks one address; take the preferred one, as `resolve` would.
        let resolve_one = |host: &str, port: u16| -> Option<SocketAddr> {
            let resolved = resolve_all(host, port).ok()?;
            let ips: Vec<IpAddr> = resolved.iter().map(SocketAddr::ip).collect();
            let ip = *Self::filter_preferred_addresses(&ips).first()?;
            Some(SocketAddr::new(ip, port))
        };

        let mut addresses = Vec::new();
        for url in urls {
            let trimmed = url.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Reject addresses containing embedded whitespace (spaces, newlines, tabs).
            // Java's HOST_PORT_PATTERN regex rejects these because it anchors the entire
            // string and only allows alphanumeric, -%._: and bracket characters.
            if trimmed.chars().any(|c| c.is_ascii_whitespace()) {
                return Err(Error::config_message(format!(
                    "Invalid url in {}: {}",
                    CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                    url
                )));
            }

            // Parse host and port. Java uses Utils.getHost/getPort with a regex;
            // we parse manually to support IPv4 (host:port) and IPv6 ([host]:port).
            let (host, port) = Self::parse_host_port(trimmed).ok_or_else(|| {
                Error::config_message(format!(
                    "Invalid url in {}: {}",
                    CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                    url
                ))
            })?;

            // Validate port range (Java's InetSocketAddress constructor throws
            // IllegalArgumentException for ports outside 0-65535).
            let port = u16::try_from(port).map_err(|_| {
                Error::config_message(format!(
                    "Invalid port in {}: {}",
                    CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                    url
                ))
            })?;

            match client_dns_lookup {
                ClientDnsLookup::ResolveCanonicalBootstrapServersOnly => {
                    let inet_addresses = resolve_all(host, port).map_err(|_| {
                        Error::config_message(format!(
                            "Unknown host in {}: {}",
                            CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                            url
                        ))
                    })?;
                    for inet_address in inet_addresses {
                        let resolved_canonical_name = canonical_host_name(inet_address.ip());
                        match resolve_one(&resolved_canonical_name, port) {
                            Some(address) => addresses.push((resolved_canonical_name, address)),
                            None => warn!(
                                "Couldn't resolve server {} from {} as DNS resolution of the canonical hostname {} failed for {}",
                                url,
                                CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                                resolved_canonical_name,
                                host
                            ),
                        }
                    }
                },
                ClientDnsLookup::UseAllDnsIps => match resolve_one(host, port) {
                    // Preserve the original hostname: it is what the bootstrap
                    // node connects by (re-resolved per connection attempt) and
                    // what TLS SNI uses (Java's `getHostString()`).
                    Some(address) => addresses.push((host.to_string(), address)),
                    None => warn!(
                        "Couldn't resolve server {} from {} as DNS resolution failed for {}",
                        url,
                        CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                        host
                    ),
                },
            }
        }
        if addresses.is_empty() {
            return Err(Error::config_message(format!(
                "No resolvable bootstrap urls given in {}",
                CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG
            )));
        }
        Ok(addresses)
    }

    /// Parse a `"host:port"` or `"[ipv6]:port"` string into its components.
    ///
    /// Returns `None` if the string doesn't match the expected format
    /// (no port, or no host).
    ///
    /// Translated from Java's `Utils.getHost()` / `Utils.getPort()` which use the
    /// `HOST_PORT_PATTERN` regex.
    fn parse_host_port(url: &str) -> Option<(&str, u32)> {
        // Strip optional protocol prefix (e.g., "http://")
        let s = if let Some(idx) = url.find("://") {
            &url[idx + 3..]
        } else {
            url
        };

        if s.starts_with('[') {
            // IPv6 bracket notation: [host]:port
            let close_bracket = s.find(']')?;
            let host = &s[1..close_bracket];
            let rest = &s[close_bracket + 1..];
            // Must be followed by :port
            let port_str = rest.strip_prefix(':')?;
            let port: u32 = port_str.parse().ok()?;
            Some((host, port))
        } else {
            // IPv4 or hostname: host:port — find the last colon
            let colon_idx = s.rfind(':')?;
            let host = &s[..colon_idx];
            if host.is_empty() {
                return None;
            }
            let port_str = &s[colon_idx + 1..];
            let port: u32 = port_str.parse().ok()?;
            Some((host, port))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    /// Translated from `ClientUtilsTest.testFilterPreferredAddresses`.
    ///
    /// The IPv4-first half matches Java. For the IPv6-first input Java returns
    /// `[::1]` (first family wins); this translation returns the IPv4 subset —
    /// the documented IPv4-preference deviation on `filter_preferred_addresses`.
    #[test]
    fn test_filter_preferred_addresses() {
        let ipv4: IpAddr = "192.0.0.1".parse().unwrap();
        let ipv6: IpAddr = "::1".parse().unwrap();

        let result = ClientUtils::filter_preferred_addresses(&[ipv4, ipv6, ipv4]);
        assert!(result.contains(&ipv4));
        assert!(!result.contains(&ipv6));
        assert_eq!(2, result.len());

        let result = ClientUtils::filter_preferred_addresses(&[ipv6, ipv4, ipv4]);
        assert!(result.contains(&ipv4));
        assert!(!result.contains(&ipv6));
        assert_eq!(2, result.len());
    }

    /// Regression test for a broker advertising `localhost` from an IPv4-only
    /// Docker network, with `localhost` resolving to both `::1` and
    /// `127.0.0.1`: IPv4 must be chosen whatever order the resolver returns.
    #[test]
    fn test_filter_preferred_addresses_prefers_ipv4_regardless_of_order() {
        let v4_loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let v4_other = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let v6_loopback = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let v6_other = IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1));

        let cases: [(&[IpAddr], &[IpAddr]); 6] = [
            (&[v6_loopback, v4_loopback], &[v4_loopback]),
            (&[v4_loopback, v6_loopback], &[v4_loopback]),
            (&[v6_loopback, v6_other, v4_loopback], &[v4_loopback]),
            (&[v6_loopback, v4_other, v6_other, v4_loopback], &[v4_other, v4_loopback]),
            (&[v4_loopback, v6_loopback, v4_other], &[v4_loopback, v4_other]),
            (&[v6_other, v6_loopback, v4_other, v4_loopback], &[v4_other, v4_loopback]),
        ];
        for (input, expected) in cases {
            assert_eq!(ClientUtils::filter_preferred_addresses(input), expected, "input {input:?}");
        }
    }

    /// Without any IPv4 address the IPv6 addresses are all kept, in order.
    #[test]
    fn test_filter_preferred_addresses_ipv6_only() {
        let addrs = [
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 2)),
        ];
        assert_eq!(ClientUtils::filter_preferred_addresses(&addrs), addrs);
    }

    #[test]
    fn test_filter_preferred_addresses_empty() {
        let filtered = ClientUtils::filter_preferred_addresses(&[]);
        assert!(filtered.is_empty());
    }

    /// End to end through `resolve`: a resolver returning `::1` first still
    /// yields only `127.0.0.1` for the connection attempts.
    #[tokio::test]
    async fn test_resolve_prefers_ipv4_when_ipv6_listed_first() {
        struct Ipv6FirstLocalhost;
        impl HostResolver for Ipv6FirstLocalhost {
            async fn resolve(&self, _host: &str) -> io::Result<Vec<IpAddr>> {
                Ok(vec![IpAddr::V6(Ipv6Addr::LOCALHOST), IpAddr::V4(Ipv4Addr::LOCALHOST)])
            }
        }
        assert_eq!(
            ClientUtils::resolve("localhost", &Ipv6FirstLocalhost).await.unwrap(),
            vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]
        );
    }

    /// `ClientUtilsTest.checkWithoutLookup`.
    fn check_without_lookup(urls: &[&str]) -> Result<Vec<(String, SocketAddr)>, Error> {
        let urls: Vec<String> = urls.iter().map(|u| u.to_string()).collect();
        ClientUtils::parse_and_validate_addresses(&urls, ClientDnsLookup::UseAllDnsIps)
    }

    /// `ClientUtilsTest.checkWithLookup`.
    fn check_with_lookup(urls: &[&str]) -> Result<Vec<(String, SocketAddr)>, Error> {
        let urls: Vec<String> = urls.iter().map(|u| u.to_string()).collect();
        ClientUtils::parse_and_validate_addresses(&urls, ClientDnsLookup::ResolveCanonicalBootstrapServersOnly)
    }

    /// Java's `assertThrows(ConfigException.class, ...)`, plus the exact
    /// message, which Java builds with the single-message `ConfigException`
    /// constructor (no `Invalid value ... for configuration ...` prefix).
    fn assert_config_error(result: Result<Vec<(String, SocketAddr)>, Error>, expected: &str) {
        if let Err(error) = &result {
            assert!(error.is_kafka_error(), "ConfigException extends KafkaException: {error:?}");
        }
        match result {
            Err(Error::Config(e)) => assert_eq!(e.message(), expected),
            other => panic!("Expected Config({expected:?}), got: {other:?}"),
        }
    }

    /// Translated from `ClientUtilsTest.testParseAndValidateAddresses`.
    ///
    /// In the default mode each URL yields exactly one entry keyed by the
    /// literal host — never one entry per resolved IP.
    #[test]
    fn test_parse_and_validate_addresses() {
        let addrs = check_without_lookup(&["127.0.0.1:8000"]).unwrap();
        assert_eq!(addrs, vec![("127.0.0.1".to_string(), "127.0.0.1:8000".parse().unwrap())]);

        let addrs = check_without_lookup(&["localhost:8080"]).unwrap();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].0, "localhost");
        assert!(addrs[0].1.ip().is_loopback());
        assert_eq!(addrs[0].1.port(), 8080);

        let addrs = check_without_lookup(&["[::1]:8000"]).unwrap();
        assert_eq!(addrs, vec![("::1".to_string(), "[::1]:8000".parse().unwrap())]);

        let addrs = check_without_lookup(&["[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "localhost:10000"]).unwrap();
        assert_eq!(addrs.len(), 2);
        assert_eq!(
            addrs[0],
            (
                "2001:db8:85a3:8d3:1319:8a2e:370:7348".to_string(),
                "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234".parse().unwrap()
            )
        );
        assert_eq!(addrs[1].0, "localhost");
        assert_eq!(addrs[1].1.port(), 10000);

        let validated_addresses = check_without_lookup(&["localhost:10000"]).unwrap();
        assert_eq!(1, validated_addresses.len());
        let (only_host, only_address) = &validated_addresses[0];
        assert_eq!("localhost", only_host);
        assert_eq!(10000, only_address.port());
    }

    /// Translated from `ClientUtilsTest.testParseAndValidateAddressesWithReverseLookup`.
    ///
    /// Java mocks `InetAddress.getAllByName` / `getCanonicalHostName` with
    /// Mockito; here the same two calls are injected as closures.
    #[test]
    fn test_parse_and_validate_addresses_with_reverse_lookup() {
        // The literal-address checks Java repeats before mocking; with a real
        // resolver an IP literal's canonical name is its textual form.
        let addrs = check_with_lookup(&["127.0.0.1:8000"]).unwrap();
        assert_eq!(addrs, vec![("127.0.0.1".to_string(), "127.0.0.1:8000".parse().unwrap())]);
        let addrs = check_with_lookup(&["[::1]:8000"]).unwrap();
        assert_eq!(addrs, vec![("::1".to_string(), "[::1]:8000".parse().unwrap())]);
        assert!(!check_with_lookup(&["localhost:8080"]).unwrap().is_empty());
        let addrs = check_with_lookup(&["[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "localhost:10000"]).unwrap();
        assert_eq!(addrs[0].0, "2001:db8:85a3:8d3:1319:8a2e:370:7348");
        assert!(addrs[1..].iter().all(|(_, a)| a.port() == 10000));

        let hostname = "example.com";
        let port: u16 = 10000;
        let canonical_hostname1 = "canonical_hostname1";
        let canonical_hostname2 = "canonical_hostname2";
        let ip1: IpAddr = "192.0.2.1".parse().unwrap();
        let ip2: IpAddr = "192.0.2.2".parse().unwrap();
        let ip_canonical1: IpAddr = "198.51.100.1".parse().unwrap();
        let ip_canonical2: IpAddr = "198.51.100.2".parse().unwrap();

        let resolve_all = |host: &str, port: u16| -> io::Result<Vec<SocketAddr>> {
            match host {
                "example.com" => Ok(vec![SocketAddr::new(ip1, port), SocketAddr::new(ip2, port)]),
                "canonical_hostname1" => Ok(vec![SocketAddr::new(ip_canonical1, port)]),
                "canonical_hostname2" => Ok(vec![SocketAddr::new(ip_canonical2, port)]),
                _ => Err(io::Error::new(io::ErrorKind::NotFound, host.to_string())),
            }
        };
        let canonical_host_name = |address: IpAddr| -> String {
            if address == ip1 {
                canonical_hostname1.to_string()
            } else if address == ip2 {
                canonical_hostname2.to_string()
            } else {
                address.to_string()
            }
        };

        let validated_addresses = ClientUtils::parse_and_validate_addresses_with_lookup(
            &[format!("{hostname}:{port}")],
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
            resolve_all,
            canonical_host_name,
        )
        .unwrap();
        assert_eq!(
            validated_addresses,
            vec![
                (canonical_hostname1.to_string(), SocketAddr::new(ip_canonical1, port)),
                (canonical_hostname2.to_string(), SocketAddr::new(ip_canonical2, port)),
            ]
        );

        // The same host in the default mode is NOT expanded: one entry, keyed
        // by the literal host.
        let validated_addresses = ClientUtils::parse_and_validate_addresses_with_lookup(
            &[format!("{hostname}:{port}")],
            ClientDnsLookup::UseAllDnsIps,
            resolve_all,
            canonical_host_name,
        )
        .unwrap();
        assert_eq!(validated_addresses, vec![(hostname.to_string(), SocketAddr::new(ip1, port))]);
    }

    /// Canonical mode: a canonical name that does not resolve is skipped (Java
    /// logs and drops the unresolved `InetSocketAddress`); if none resolve the
    /// "No resolvable" error is returned.
    #[test]
    fn test_parse_and_validate_addresses_with_reverse_lookup_unresolved_canonical_name() {
        let ip1: IpAddr = "192.0.2.1".parse().unwrap();
        let ip2: IpAddr = "192.0.2.2".parse().unwrap();
        let resolve_all = |host: &str, port: u16| -> io::Result<Vec<SocketAddr>> {
            match host {
                "example.com" => Ok(vec![SocketAddr::new(ip1, port), SocketAddr::new(ip2, port)]),
                "good" => Ok(vec![SocketAddr::new(ip1, port)]),
                _ => Err(io::Error::new(io::ErrorKind::NotFound, host.to_string())),
            }
        };
        let urls = ["example.com:9092".to_string()];

        let one_good = ClientUtils::parse_and_validate_addresses_with_lookup(
            &urls,
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
            resolve_all,
            |address| {
                if address == ip1 {
                    "good".to_string()
                } else {
                    "bad".to_string()
                }
            },
        )
        .unwrap();
        assert_eq!(one_good, vec![("good".to_string(), SocketAddr::new(ip1, 9092))]);

        assert_config_error(
            ClientUtils::parse_and_validate_addresses_with_lookup(
                &urls,
                ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
                resolve_all,
                |_| "bad".to_string(),
            ),
            "No resolvable bootstrap urls given in bootstrap.servers",
        );
    }

    /// Canonical mode: a host that does not resolve at all is an error —
    /// Java's `getAllByName` throws `UnknownHostException`, rethrown as
    /// `ConfigException("Unknown host in ...")`. (The default mode only logs
    /// and skips it; see `test_only_bad_hostname`.)
    #[test]
    fn test_parse_and_validate_addresses_with_reverse_lookup_unknown_host() {
        let urls = ["some.invalid.hostname.foo.bar.local:9999".to_string()];
        assert_config_error(
            ClientUtils::parse_and_validate_addresses_with_lookup(
                &urls,
                ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
                |host, _| Err(io::Error::new(io::ErrorKind::NotFound, host.to_string())),
                |address| address.to_string(),
            ),
            "Unknown host in bootstrap.servers: some.invalid.hostname.foo.bar.local:9999",
        );
    }

    /// Translated from `ClientUtilsTest.testValidBrokerAddress`.
    ///
    /// Pins the default-mode shape: one entry per URL, keyed by the literal
    /// host, in URL order — even though `localhost` may resolve to both an
    /// IPv4 and an IPv6 address.
    #[test]
    fn test_valid_broker_address() {
        let addrs = check_without_lookup(&["localhost:9997", "localhost:9998", "localhost:9999"]).unwrap();
        let hosts_and_ports: Vec<(&str, u16)> = addrs.iter().map(|(h, a)| (h.as_str(), a.port())).collect();
        assert_eq!(
            hosts_and_ports,
            vec![("localhost", 9997), ("localhost", 9998), ("localhost", 9999)]
        );
        assert!(addrs.iter().all(|(_, a)| a.ip().is_loopback()));
    }

    /// Translated from `ClientUtilsTest.testInvalidBrokerAddress`
    /// (`@ParameterizedTest` over `provideInvalidBrokerAddressTestCases`).
    ///
    /// Java's second case, `" localhost:9999"` (leading space), is rejected by
    /// Java's `HOST_PORT_PATTERN`; this translation trims leading/trailing
    /// whitespace from each entry first, so that one case is accepted — see
    /// `test_invalid_broker_address_leading_space`.
    #[test]
    fn test_invalid_broker_address() {
        let cases: [&[&str]; 2] = [
            &["localhost:9997\nlocalhost:9998\nlocalhost:9999"],
            // Intentionally provide a single string, as users may provide
            // space-separated brokers, which will be parsed as a single string.
            &["localhost:9997 localhost:9998 localhost:9999"],
        ];
        for addresses in cases {
            assert_config_error(
                check_without_lookup(addresses),
                &format!("Invalid url in bootstrap.servers: {}", addresses[0]),
            );
        }
    }

    /// Leading-space case of `ClientUtilsTest.testInvalidBrokerAddress`.
    ///
    /// Java rejects `" localhost:9999"` because the space fails its
    /// `HOST_PORT_PATTERN` regex. This translation trims leading/trailing
    /// whitespace from each entry (embedded whitespace is still rejected), so
    /// the entry is accepted — a documented, more lenient deviation.
    #[test]
    fn test_invalid_broker_address_leading_space() {
        let addrs = check_without_lookup(&["localhost:9997", "localhost:9998", " localhost:9999"]).unwrap();
        assert_eq!(addrs.len(), 3);
        assert_eq!(addrs[2].0, "localhost");
    }

    /// Translated from `ClientUtilsTest.testInvalidConfig`: an unknown
    /// `client.dns.lookup` string fails `ClientDnsLookup.forConfig` with an
    /// `IllegalArgumentException` before any address is parsed.
    #[test]
    fn test_invalid_config() {
        match ClientDnsLookup::for_config("random.value") {
            Err(Error::LocalIllegalArgument(e)) => {
                assert_eq!(
                    e.message(),
                    "No enum constant org.apache.kafka.clients.ClientDnsLookup.RANDOM.VALUE"
                )
            },
            other => panic!("Expected LocalIllegalArgument, got: {other:?}"),
        }
    }

    /// Translated from `ClientUtilsTest.testNoPort`.
    #[test]
    fn test_no_port() {
        assert_config_error(
            check_without_lookup(&["127.0.0.1"]),
            "Invalid url in bootstrap.servers: 127.0.0.1",
        );
    }

    /// Translated from `ClientUtilsTest.testInvalidPort`.
    #[test]
    fn test_invalid_port() {
        assert_config_error(
            check_without_lookup(&["localhost:70000"]),
            "Invalid port in bootstrap.servers: localhost:70000",
        );
    }

    /// Translated from `ClientUtilsTest.testOnlyBadHostname`.
    ///
    /// Java mocks `InetSocketAddress` to be unresolved; here the injected
    /// resolver fails. The default mode logs and skips the host, leaving no
    /// address.
    #[test]
    fn test_only_bad_hostname() {
        let urls = ["some.invalid.hostname.foo.bar.local:9999".to_string()];
        assert_config_error(
            ClientUtils::parse_and_validate_addresses_with_lookup(
                &urls,
                ClientDnsLookup::UseAllDnsIps,
                |host, _| Err(io::Error::new(io::ErrorKind::NotFound, host.to_string())),
                |address| address.to_string(),
            ),
            "No resolvable bootstrap urls given in bootstrap.servers",
        );
    }

    /// An unresolvable host through the real resolver, in the default mode.
    #[test]
    fn test_parse_and_validate_addresses_unresolvable() {
        assert_config_error(
            check_without_lookup(&["this.host.does.not.exist.ever.kafka.test:9092"]),
            "No resolvable bootstrap urls given in bootstrap.servers",
        );
    }

    /// Empty and whitespace-only entries are skipped (Java skips `null` / empty
    /// URLs), leaving no address.
    #[test]
    fn test_parse_and_validate_addresses_empty() {
        for mode in [
            ClientDnsLookup::UseAllDnsIps,
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
        ] {
            assert_config_error(
                ClientUtils::parse_and_validate_addresses(&[], mode),
                "No resolvable bootstrap urls given in bootstrap.servers",
            );
            assert_config_error(
                ClientUtils::parse_and_validate_addresses(&["  ".to_string(), String::new()], mode),
                "No resolvable bootstrap urls given in bootstrap.servers",
            );
        }
    }

    /// Translated from `ClientUtilsTest.testResolveUnknownHostException`.
    #[tokio::test]
    async fn test_resolve_unknown_host_exception() {
        struct ThrowingHostResolver;
        impl HostResolver for ThrowingHostResolver {
            async fn resolve(&self, host: &str) -> io::Result<Vec<IpAddr>> {
                Err(io::Error::new(io::ErrorKind::NotFound, host.to_string()))
            }
        }
        assert!(
            ClientUtils::resolve("some.invalid.hostname.foo.bar.local", &ThrowingHostResolver)
                .await
                .is_err()
        );
    }

    /// Translated from `ClientUtilsTest.testResolveDnsLookup`.
    #[tokio::test]
    async fn test_resolve_dns_lookup() {
        struct FixedHostResolver(Vec<IpAddr>);
        impl HostResolver for FixedHostResolver {
            async fn resolve(&self, _host: &str) -> io::Result<Vec<IpAddr>> {
                Ok(self.0.clone())
            }
        }
        let addresses: Vec<IpAddr> = vec!["198.51.100.0".parse().unwrap(), "198.51.100.5".parse().unwrap()];
        let resolver = FixedHostResolver(addresses.clone());
        assert_eq!(addresses, ClientUtils::resolve("kafka.apache.org", &resolver).await.unwrap());
    }
}
