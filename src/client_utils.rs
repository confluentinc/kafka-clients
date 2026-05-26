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

//! Translation of `org.apache.kafka.clients.ClientUtils`.
//!
//! # Phase scope
//!
//! Phase 4c translates the producer-reachable, network-independent
//! address/DNS subset of `ClientUtils`:
//!
//! * [`parse_and_validate_addresses`] (both Java overloads collapsed)
//! * [`resolve`]
//! * [`filter_preferred_addresses`]
//! * [`InetSocketAddress`] — a small Rust analogue of Java's
//!   `java.net.InetSocketAddress` providing `host_name()` /
//!   `port()` / `is_unresolved()`.
//!
//! Deferred to Phase 5 (need `Selector` / `ChannelBuilder` / `Metrics`):
//! * `createChannelBuilder`
//! * `createNetworkClient` (all overloads)
//! * `configuredInterceptors`
//! * `configureClusterResourceListeners` — depends on
//!   `AbstractConfig.getConfiguredInstances` plumbing, which is
//!   Phase 5/6.

use std::net::IpAddr;

use crate::client_dns_lookup::ClientDnsLookup;
use crate::common::errors::KafkaError;
use crate::common_client_configs::BOOTSTRAP_SERVERS_CONFIG;
use crate::host_resolver::HostResolver;

/// Lightweight Rust analogue of Java's
/// `java.net.InetSocketAddress`. Java's class wraps a hostname plus a
/// port and may be either resolved (IP cached) or unresolved
/// (hostname only, DNS not yet attempted).
///
/// Stdlib's [`std::net::SocketAddr`] does not preserve hostnames, so
/// we keep our own minimal struct here.
///
/// Instances returned by [`parse_and_validate_addresses`] are always
/// resolved (the DNS lookup happens during parsing); unresolved
/// instances are used only by tests and by intermediate parse paths.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InetSocketAddress {
    host: String,
    port: u16,
    /// `Some(_)` if a DNS resolution succeeded; `None` if the
    /// hostname could not be resolved (Java's `isUnresolved() == true`).
    resolved: Option<IpAddr>,
}

impl InetSocketAddress {
    /// Create an explicitly resolved address. Mirrors Java's
    /// `new InetSocketAddress(InetAddress, int)` constructor (which
    /// always returns a resolved instance).
    pub fn new(host: impl Into<String>, port: u16, resolved: IpAddr) -> Self {
        Self { host: host.into(), port, resolved: Some(resolved) }
    }

    /// Create an unresolved address — analogous to Java's
    /// `InetSocketAddress.createUnresolved(String, int)`.
    pub fn create_unresolved(host: impl Into<String>, port: u16) -> Self {
        Self { host: host.into(), port, resolved: None }
    }

    /// Mirrors Java's `getHostName()`.
    pub fn host_name(&self) -> &str {
        &self.host
    }

    /// Mirrors Java's `getPort()`.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Mirrors Java's `isUnresolved()`.
    pub fn is_unresolved(&self) -> bool {
        self.resolved.is_none()
    }

    /// Returns the resolved IP, if any. There is no Java
    /// `InetSocketAddress.getAddress()` analogue when unresolved
    /// (Java returns null); we return `None`.
    pub fn address(&self) -> Option<IpAddr> {
        self.resolved
    }
}

/// Parse a `host:port` string into a `(host, port)` pair, mirroring
/// `org.apache.kafka.common.utils.Utils.getHost`/`getPort`.
///
/// Java uses a regex pattern:
/// `^(?:[0-9a-zA-Z\-%._]*://)?\[?([0-9a-zA-Z\-%._:]*)\]?:([0-9]+)`
///
/// Rust translation: a hand-rolled parser that accepts:
/// * `host:port`
/// * `[ipv6]:port` (square-bracketed literal)
/// * `scheme://host:port` (scheme is permissive)
///
/// Returns `None` if the input does not match the pattern (Java
/// returns null for both `getHost` and `getPort`).
fn parse_host_port(address: &str) -> Option<(&str, u16)> {
    // Strip optional `scheme://` prefix.
    let after_scheme = match address.find("://") {
        Some(idx) => {
            let scheme = &address[..idx];
            // Java's pattern allows `[0-9a-zA-Z\-%._]*` for the scheme
            // (effectively any of those chars or empty).
            if scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'%' || b == b'.' || b == b'_')
            {
                &address[idx + 3..]
            } else {
                return None;
            }
        },
        None => address,
    };

    // Find `host` and `:port` taking into account possible bracketed IPv6.
    let (host, port_str) = if let Some(rest) = after_scheme.strip_prefix('[') {
        let close = rest.find(']')?;
        let host = &rest[..close];
        let after_bracket = &rest[close + 1..];
        let port_str = after_bracket.strip_prefix(':')?;
        (host, port_str)
    } else {
        // Last colon (so IPv6 without brackets — e.g. raw `::1:9092` —
        // would be ambiguous, but Java's regex captures up to the
        // final `:[0-9]+` so we mirror that).
        let last_colon = after_scheme.rfind(':')?;
        (&after_scheme[..last_colon], &after_scheme[last_colon + 1..])
    };

    // Validate host characters — Java pattern: `[0-9a-zA-Z\-%._:]*`.
    if !host
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'%' || b == b'.' || b == b'_' || b == b':')
    {
        return None;
    }

    // Validate port digits — Java pattern: `[0-9]+`.
    if port_str.is_empty() || !port_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    let port: u32 = port_str.parse().ok()?;
    if port > u16::MAX as u32 {
        // Java throws `IllegalArgumentException` from
        // `new InetSocketAddress(host, port)` when port is out of
        // range. Surface as None so the caller can produce the same
        // ConfigException-style error.
        return None;
    }
    Some((host, port as u16))
}

/// Parse and validate a list of bootstrap-server URLs.
///
/// Mirrors `ClientUtils.parseAndValidateAddresses(List<String>,
/// ClientDnsLookup)`. Returns `KafkaError::Config` on:
/// * malformed `host:port` URL,
/// * port out of range,
/// * unknown host (DNS failure),
/// * empty result (no resolvable URLs).
///
/// Uses a [`crate::default_host_resolver::DefaultHostResolver`] for DNS
/// lookups. For tests that need a stub resolver, use
/// [`parse_and_validate_addresses_with_resolver`].
pub fn parse_and_validate_addresses(
    urls: &[String],
    client_dns_lookup: ClientDnsLookup,
) -> Result<Vec<InetSocketAddress>, KafkaError> {
    let resolver = crate::default_host_resolver::DefaultHostResolver::new();
    parse_and_validate_addresses_with_resolver(urls, client_dns_lookup, &resolver)
}

/// Like [`parse_and_validate_addresses`] but with an injected
/// [`HostResolver`]. Used internally and by tests.
///
/// `RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY` is implemented via the
/// resolver: each resolved IP becomes a separate `InetSocketAddress`
/// keyed by its IP literal as the canonical name. (Stdlib has no
/// reverse-DNS API, and Java's `getCanonicalHostName()` falls back to
/// the IP textual form when reverse-DNS fails — we always exhibit the
/// fallback behavior here. See module-level rustdoc.)
pub fn parse_and_validate_addresses_with_resolver(
    urls: &[String],
    client_dns_lookup: ClientDnsLookup,
    resolver: &dyn HostResolver,
) -> Result<Vec<InetSocketAddress>, KafkaError> {
    let mut addresses = Vec::new();
    for url in urls {
        if url.is_empty() {
            continue;
        }
        let (host, port) = match parse_host_port(url) {
            Some(hp) => hp,
            None => {
                return Err(KafkaError::Config(format!("Invalid url in {BOOTSTRAP_SERVERS_CONFIG}: {url}",)));
            },
        };

        match client_dns_lookup {
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly => {
                let ips = match resolver.resolve(host) {
                    Ok(ips) => ips,
                    Err(_) => {
                        return Err(KafkaError::Config(
                            format!("Unknown host in {BOOTSTRAP_SERVERS_CONFIG}: {url}",),
                        ));
                    },
                };
                for ip in ips {
                    let canonical = ip.to_string();
                    let addr = InetSocketAddress::new(canonical, port, ip);
                    if addr.is_unresolved() {
                        log::warn!(
                            "Couldn't resolve server {url} from {BOOTSTRAP_SERVERS_CONFIG} as DNS resolution of the canonical hostname {canonical} failed for {host}",
                            canonical = addr.host_name(),
                        );
                    } else {
                        addresses.push(addr);
                    }
                }
            },
            ClientDnsLookup::UseAllDnsIps => {
                let resolved = resolver.resolve(host).ok().and_then(|v| v.into_iter().next());
                match resolved {
                    Some(ip) => {
                        addresses.push(InetSocketAddress::new(host, port, ip));
                    },
                    None => {
                        log::warn!(
                            "Couldn't resolve server {url} from {BOOTSTRAP_SERVERS_CONFIG} as DNS resolution failed for {host}",
                        );
                    },
                }
            },
        }
    }
    if addresses.is_empty() {
        return Err(KafkaError::Config(format!(
            "No resolvable bootstrap urls given in {BOOTSTRAP_SERVERS_CONFIG}",
        )));
    }
    Ok(addresses)
}

/// Resolve a host name into a list of preferred addresses (single
/// address family). Mirrors `ClientUtils.resolve(String, HostResolver)`.
pub fn resolve(host: &str, resolver: &dyn HostResolver) -> Result<Vec<IpAddr>, KafkaError> {
    let addresses = resolver.resolve(host)?;
    let result = filter_preferred_addresses(&addresses);
    if log::log_enabled!(log::Level::Debug) {
        let joined = result.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(",");
        log::debug!("Resolved host {host} as {joined}");
    }
    Ok(result)
}

/// Return a list containing the first address in `all_addresses` and
/// subsequent addresses that are of the same address family
/// (IPv4-vs-IPv6) as the first.
///
/// Mirrors `ClientUtils.filterPreferredAddresses(InetAddress[])`.
/// Java keys on the runtime class (`Inet4Address` /
/// `Inet6Address`); Rust keys on [`IpAddr::is_ipv4`] /
/// [`IpAddr::is_ipv6`].
pub fn filter_preferred_addresses(all_addresses: &[IpAddr]) -> Vec<IpAddr> {
    let mut preferred = Vec::new();
    let mut want_ipv4: Option<bool> = None;
    for addr in all_addresses {
        let is_ipv4 = addr.is_ipv4();
        let want = *want_ipv4.get_or_insert(is_ipv4);
        if is_ipv4 == want {
            preferred.push(*addr);
        }
    }
    preferred
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::sync::Mutex;

    /// Test-only `HostResolver` that returns canned addresses for given
    /// hostnames. Mirrors what Java's `Mockito.mockStatic(InetAddress)`
    /// did in `ClientUtilsTest`.
    struct MockHostResolver {
        // Mutex makes the resolver Send + Sync without requiring
        // `&mut self` for resolution.
        table: Mutex<HashMap<String, Vec<IpAddr>>>,
    }

    impl MockHostResolver {
        fn new() -> Self {
            Self { table: Mutex::new(HashMap::new()) }
        }

        fn with_entry(self, host: &str, ips: Vec<IpAddr>) -> Self {
            self.table.lock().unwrap().insert(host.to_string(), ips);
            self
        }
    }

    impl HostResolver for MockHostResolver {
        fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, KafkaError> {
            self.table
                .lock()
                .unwrap()
                .get(host)
                .cloned()
                .ok_or_else(|| KafkaError::Network(format!("Unknown host: {host}")))
        }
    }

    /// Always-failing resolver. Mirrors Java's
    /// `host -> { throw new UnknownHostException(); }` lambda.
    struct ThrowingResolver;
    impl HostResolver for ThrowingResolver {
        fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, KafkaError> {
            Err(KafkaError::Network("test forced".into()))
        }
    }

    /// Translates Java `AddressChangeHostResolver(addresses, addresses)`
    /// — the simplified form where both responses are the same.
    struct CannedAddressResolver(Vec<IpAddr>);
    impl HostResolver for CannedAddressResolver {
        fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, KafkaError> {
            Ok(self.0.clone())
        }
    }

    fn check_without_lookup(urls: &[&str]) -> Result<Vec<InetSocketAddress>, KafkaError> {
        let owned: Vec<String> = urls.iter().map(|s| (*s).to_string()).collect();
        // Use a permissive resolver so DNS in CI does not interfere
        // with the parse-only assertions.
        let resolver = MockHostResolver::new()
            .with_entry("127.0.0.1", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .with_entry("localhost", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .with_entry("::1", vec![IpAddr::V6(Ipv6Addr::LOCALHOST)])
            .with_entry(
                "2001:db8:85a3:8d3:1319:8a2e:370:7348",
                vec![IpAddr::V6(Ipv6Addr::new(
                    0x2001, 0x0db8, 0x85a3, 0x08d3, 0x1319, 0x8a2e, 0x0370, 0x7348,
                ))],
            );
        parse_and_validate_addresses_with_resolver(&owned, ClientDnsLookup::UseAllDnsIps, &resolver)
    }

    fn check_with_lookup(urls: &[&str], resolver: &dyn HostResolver) -> Result<Vec<InetSocketAddress>, KafkaError> {
        let owned: Vec<String> = urls.iter().map(|s| (*s).to_string()).collect();
        parse_and_validate_addresses_with_resolver(
            &owned,
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
            resolver,
        )
    }

    /// Java `testParseAndValidateAddresses`.
    #[test]
    fn test_parse_and_validate_addresses() {
        check_without_lookup(&["127.0.0.1:8000"]).unwrap();
        check_without_lookup(&["localhost:8080"]).unwrap();
        check_without_lookup(&["[::1]:8000"]).unwrap();
        check_without_lookup(&["[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "localhost:10000"]).unwrap();
        let validated = check_without_lookup(&["localhost:10000"]).unwrap();
        assert_eq!(validated.len(), 1);
        let only = &validated[0];
        assert_eq!(only.host_name(), "localhost");
        assert_eq!(only.port(), 10000);
    }

    /// Java `testParseAndValidateAddressesWithReverseLookup`.
    ///
    /// Java mocks `InetAddress.getAllByName` to return mock
    /// `InetAddress` instances whose `getCanonicalHostName()` returns
    /// canned strings. The Rust translation injects a `HostResolver`
    /// that returns two IPs; the canonical mode then produces two
    /// `InetSocketAddress` entries keyed by IP-literal canonical names.
    #[test]
    fn test_parse_and_validate_addresses_with_reverse_lookup() {
        check_without_lookup(&["127.0.0.1:8000"]).unwrap();
        check_without_lookup(&["localhost:8080"]).unwrap();
        check_without_lookup(&["[::1]:8000"]).unwrap();
        check_without_lookup(&["[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "localhost:10000"]).unwrap();

        let port: u16 = 10000;
        let ip1 = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 2));
        let resolver = MockHostResolver::new().with_entry("example.com", vec![ip1, ip2]);

        let validated = check_with_lookup(&["example.com:10000"], &resolver).unwrap();
        assert_eq!(validated.len(), 2);
        for addr in &validated {
            assert_eq!(addr.port(), port);
            // Canonical names fall back to the IP literal — the
            // documented Phase 4c behavior.
            let host = addr.host_name();
            assert!(
                host == ip1.to_string() || host == ip2.to_string(),
                "unexpected canonical host: {host}",
            );
        }
    }

    /// Java `testValidBrokerAddress`.
    #[test]
    fn test_valid_broker_address() {
        let urls = [
            "localhost:9997".to_string(),
            "localhost:9998".to_string(),
            "localhost:9999".to_string(),
        ];
        let resolver = MockHostResolver::new().with_entry("localhost", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        parse_and_validate_addresses_with_resolver(&urls, ClientDnsLookup::UseAllDnsIps, &resolver).unwrap();
    }

    /// Java `testInvalidBrokerAddress` (parameterized — translated as a
    /// loop per CLAUDE.md DoD #3 re `@RepeatedTest` / `@ParameterizedTest`).
    #[test]
    fn test_invalid_broker_address() {
        let cases: &[Vec<&str>] = &[
            vec!["localhost:9997\nlocalhost:9998\nlocalhost:9999"],
            vec!["localhost:9997", "localhost:9998", " localhost:9999"],
            // Intentionally a single string — users may provide
            // space-separated brokers, which are parsed as one URL.
            vec!["localhost:9997 localhost:9998 localhost:9999"],
        ];
        let resolver = MockHostResolver::new().with_entry("localhost", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        for case in cases {
            let owned: Vec<String> = case.iter().map(|s| (*s).to_string()).collect();
            let err = parse_and_validate_addresses_with_resolver(&owned, ClientDnsLookup::UseAllDnsIps, &resolver)
                .unwrap_err();
            assert!(matches!(err, KafkaError::Config(_)), "case={case:?} got {err:?}");
        }
    }

    /// Java `testInvalidConfig` — `ClientDnsLookup.forConfig` rejects
    /// unknown values with `IllegalArgumentException`.
    #[test]
    fn test_invalid_config() {
        let err = ClientDnsLookup::for_config("random.value").unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
    }

    /// Java `testNoPort`.
    #[test]
    fn test_no_port() {
        let err = check_without_lookup(&["127.0.0.1"]).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
    }

    /// Java `testInvalidPort`.
    #[test]
    fn test_invalid_port() {
        let err = check_without_lookup(&["localhost:70000"]).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
    }

    /// Java `testOnlyBadHostname`. Java mocks `InetSocketAddress` to
    /// have `isUnresolved() == true`; the Rust translation uses a
    /// resolver that fails for the bad hostname.
    #[test]
    fn test_only_bad_hostname() {
        // No entries in resolver -> resolution fails -> in
        // USE_ALL_DNS_IPS mode the address is logged-and-skipped, so
        // the final list is empty and we should get the
        // "No resolvable bootstrap urls" error.
        let resolver = MockHostResolver::new();
        let urls = ["some.invalid.hostname.foo.bar.local:9999".to_string()];
        let err =
            parse_and_validate_addresses_with_resolver(&urls, ClientDnsLookup::UseAllDnsIps, &resolver).unwrap_err();
        match err {
            KafkaError::Config(msg) => {
                let expected = format!("No resolvable bootstrap urls given in {BOOTSTRAP_SERVERS_CONFIG}",);
                assert_eq!(msg, expected);
            },
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    /// Java `testFilterPreferredAddresses`.
    #[test]
    fn test_filter_preferred_addresses() {
        let ipv4: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 0, 1));
        let ipv6: IpAddr = IpAddr::V6(Ipv6Addr::LOCALHOST);

        let ipv4_first = vec![ipv4, ipv6, ipv4];
        let result = filter_preferred_addresses(&ipv4_first);
        assert!(result.contains(&ipv4));
        assert!(!result.contains(&ipv6));
        assert_eq!(result.len(), 2);

        let ipv6_first = vec![ipv6, ipv4, ipv4];
        let result = filter_preferred_addresses(&ipv6_first);
        assert!(result.contains(&ipv6));
        assert!(!result.contains(&ipv4));
        assert_eq!(result.len(), 1);
    }

    /// Java `testResolveUnknownHostException`.
    #[test]
    fn test_resolve_unknown_host_exception() {
        let resolver = ThrowingResolver;
        let err = resolve("some.invalid.hostname.foo.bar.local", &resolver).unwrap_err();
        // Java tests `UnknownHostException`; Rust surfaces it as
        // `KafkaError::Network` (the trait contract).
        assert!(matches!(err, KafkaError::Network(_)));
    }

    /// Java `testResolveDnsLookup`.
    #[test]
    fn test_resolve_dns_lookup() {
        let addresses = vec![
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 0)),
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 5)),
        ];
        let resolver = CannedAddressResolver(addresses.clone());
        let result = resolve("kafka.apache.org", &resolver).unwrap();
        assert_eq!(result, addresses);
    }

    // ---- Auxiliary unit tests for the InetSocketAddress shim and the
    //      host:port parser. These are not in the Java tree but verify
    //      the Rust-only utilities. ----

    #[test]
    fn parse_host_port_handles_ipv6_brackets() {
        let (h, p) = parse_host_port("[::1]:8000").unwrap();
        assert_eq!(h, "::1");
        assert_eq!(p, 8000);
    }

    #[test]
    fn parse_host_port_handles_scheme() {
        let (h, p) = parse_host_port("http://example.com:9092").unwrap();
        assert_eq!(h, "example.com");
        assert_eq!(p, 9092);
    }

    #[test]
    fn parse_host_port_rejects_missing_port() {
        assert!(parse_host_port("127.0.0.1").is_none());
    }

    #[test]
    fn parse_host_port_rejects_out_of_range_port() {
        assert!(parse_host_port("localhost:70000").is_none());
    }

    #[test]
    fn inet_socket_address_unresolved() {
        let a = InetSocketAddress::create_unresolved("nope", 9092);
        assert!(a.is_unresolved());
        assert_eq!(a.host_name(), "nope");
        assert_eq!(a.port(), 9092);
        assert!(a.address().is_none());
    }

    #[test]
    fn inet_socket_address_resolved() {
        let a = InetSocketAddress::new("localhost", 9092, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(!a.is_unresolved());
        assert_eq!(a.address(), Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    }
}
