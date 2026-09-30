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
use crate::{ClientDnsLookup, CommonClientConfigs, DefaultHostResolver};

/// Translates the Java static-utility class `org.apache.kafka.clients.ClientUtils`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
pub struct ClientUtils;

impl ClientUtils {
    /// Resolves a hostname using the given resolver and orders the addresses
    /// by preference.
    ///
    /// Returns every resolved address, IPv4 first and then IPv6 (see
    /// [`Self::filter_preferred_addresses`] for why IPv4 comes first and why
    /// IPv6 is kept rather than dropped).
    ///
    /// # Errors
    /// Returns an `io::Error` if the hostname cannot be resolved.
    pub async fn resolve<H: HostResolver>(host: &str, host_resolver: &H) -> io::Result<Vec<IpAddr>> {
        let mut addresses = host_resolver.resolve(host).await?;
        Self::filter_preferred_addresses(&mut addresses);
        log::debug!("Resolved host {} as {:?}", host, addresses);
        Ok(addresses)
    }

    /// Stable-sorts `addresses` so that every IPv4 address comes before every
    /// IPv6 address. Within each family the resolver's order is kept, and
    /// nothing is dropped.
    ///
    /// Translated from `ClientUtils.filterPreferredAddresses`, with a
    /// deliberate deviation (DoD #7): this orders instead of filtering, and
    /// the preferred family is always IPv4 rather than the first-listed one.
    /// The Java name is kept for traceability even though nothing is filtered.
    ///
    /// # Deviation: IPv4 first, IPv6 kept as a fallback
    ///
    /// Java returns "the first address in `allAddresses` and subsequent
    /// addresses that are a subtype of the first address": whichever family
    /// the resolver lists first wins, and the other family is discarded. That
    /// code has no family preference of its own; Java clients connect over
    /// IPv4 in practice because the JVM's `InetAddress` resolution orders IPv4
    /// first by default (`java.net.preferIPv6Addresses=false`). That is JVM
    /// behaviour, not Kafka code, and Rust has no equivalent:
    /// `tokio::net::lookup_host` returns the raw `getaddrinfo` order, and on
    /// macOS `getaddrinfo("localhost")` can list `::1` before `127.0.0.1`.
    /// Translated literally, the client then connected to `::1` only, which
    /// broke the common local setup of a broker in an IPv4-only Docker network
    /// advertising `localhost` (e.g. `confluent local kafka start`): the Java
    /// client works there, the Rust client could not connect.
    ///
    /// So IPv4 addresses are always tried first, as on a default JVM, but the
    /// IPv6 addresses are kept after them instead of being filtered out.
    /// `ClusterConnectionStates` walks the whole resolved list, one address per
    /// connection attempt, before re-resolving (the `use_all_dns_ips`
    /// behaviour), so a dual-stack broker whose IPv4 path is unreachable is
    /// reached over IPv6 on a later attempt.
    ///
    /// This differs from a default-configured JVM, which after ordering IPv4
    /// first keeps only the IPv4 addresses and so never tries IPv6 for a
    /// dual-stack name. The difference only shows once every IPv4 address has
    /// failed: where Java keeps retrying IPv4 (and a Java user would need
    /// `-Djava.net.preferIPv6Addresses=true`), this client falls back to IPv6.
    /// A host that resolves to a single family is unaffected.
    fn filter_preferred_addresses(addresses: &mut [IpAddr]) {
        addresses.sort_by_key(|address| Self::is_less_preferred_family(*address));
    }

    /// The sort key of [`Self::filter_preferred_addresses`]: `false` (sorted
    /// first) for IPv4, `true` for IPv6. Shared with the canonical bootstrap
    /// expansion in [`Self::parse_and_validate_addresses`], so both order the
    /// families the same way.
    fn is_less_preferred_family(address: IpAddr) -> bool {
        !address.is_ipv4()
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
    /// - Any URL does not match Java's `HOST_PORT_PATTERN` — including any
    ///   whitespace, leading or trailing too (e.g. space- or newline-separated
    ///   addresses in a single string). URLs are **not** trimmed here: as in
    ///   Java, trimming is the config layer's job (`ConfigDef`'s `LIST`
    ///   parsing, [`ConfigDef::parse_list`](crate::common::config::ConfigDef)),
    ///   which already strips the whitespace around each comma.
    /// - Any URL has a port number outside 0-65535 ("Invalid port"). As in
    ///   Java, in canonical mode the host is resolved first, so a host that
    ///   cannot be resolved is "Unknown host" whatever its port; digits too
    ///   many for an `int` are "Invalid port" in both modes.
    /// - In canonical mode, a host cannot be resolved at all.
    /// - No valid addresses can be resolved after validation.
    pub fn parse_and_validate_addresses(
        urls: &[String],
        client_dns_lookup: ClientDnsLookup,
    ) -> Result<Vec<(String, SocketAddr)>, Error> {
        Self::parse_and_validate_addresses_with_lookup(
            urls,
            client_dns_lookup,
            |host, port| {
                // `InetAddress.getAllByName("")` (and so `new InetSocketAddress("", port)`)
                // is the loopback address with no lookup; `getaddrinfo("")` fails.
                if host.is_empty() {
                    return Ok(vec![SocketAddr::new(DefaultHostResolver::EMPTY_HOST_ADDRESS, port)]);
                }
                (host, port).to_socket_addrs().map(Iterator::collect)
            },
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
            let mut ips: Vec<IpAddr> = resolved.iter().map(SocketAddr::ip).collect();
            Self::filter_preferred_addresses(&mut ips);
            let ip = *ips.first()?;
            Some(SocketAddr::new(ip, port))
        };

        let mut addresses = Vec::new();
        for url in urls {
            // Java skips only `null` / empty entries — a whitespace-only entry
            // falls through and fails `HOST_PORT_PATTERN` below.
            if url.is_empty() {
                continue;
            }

            // Java's `Utils.getHost` / `Utils.getPort`; `null` from either is
            // "Invalid url".
            let (host, port) = Self::parse_host_port(url).ok_or_else(|| {
                Error::config_message(format!(
                    "Invalid url in {}: {}",
                    CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                    url
                ))
            })?;

            // `Integer.parseInt` overflowing is a `NumberFormatException`, an
            // `IllegalArgumentException`, which Java rethrows as "Invalid port"
            // before either branch runs. The pattern admits digits only, so
            // this fails only on overflow.
            let invalid_port = || {
                Error::config_message(format!(
                    "Invalid port in {}: {}",
                    CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                    url
                ))
            };
            let port = port.parse::<i32>().map_err(|_| invalid_port())?;
            // `InetSocketAddress.checkPort`: a port outside 0-65535 is an
            // `IllegalArgumentException` too, so also "Invalid port".
            let check_port = |port: i32| u16::try_from(port).map_err(|_| invalid_port());

            match client_dns_lookup {
                ClientDnsLookup::ResolveCanonicalBootstrapServersOnly => {
                    // `InetAddress.getAllByName(host)` runs before any port
                    // check, so an unresolvable host is "Unknown host" even
                    // when its port is also out of range. The port given here
                    // is a placeholder: only the addresses are used.
                    let mut inet_addresses = resolve_all(host, 0).map_err(|_| {
                        Error::config_message(format!(
                            "Unknown host in {}: {}",
                            CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                            url
                        ))
                    })?;
                    // Java includes both families here, in `getAllByName`
                    // order, which on a default JVM is IPv4 first
                    // (`java.net.preferIPv6Addresses=false`). `getaddrinfo`
                    // has no such order, so stable-sort IPv4 first with the
                    // same key as `filter_preferred_addresses` (see its doc);
                    // nothing is filtered, here as there.
                    inet_addresses.sort_by_key(|address| Self::is_less_preferred_family(address.ip()));
                    for inet_address in inet_addresses {
                        let resolved_canonical_name = canonical_host_name(inet_address.ip());
                        // `new InetSocketAddress(resolvedCanonicalName, port)`
                        // checks the port after resolving the canonical name.
                        let port = check_port(port)?;
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
                ClientDnsLookup::UseAllDnsIps => {
                    // `new InetSocketAddress(host, port)` resolves the host and
                    // then checks the port, but it catches the
                    // `UnknownHostException` itself, so an out-of-range port is
                    // "Invalid port" whether or not the host resolves. Checking
                    // the port first gives the same result without the lookup.
                    let port = check_port(port)?;
                    match resolve_one(host, port) {
                        // Preserve the original hostname: it is what the
                        // bootstrap node connects by (re-resolved per
                        // connection attempt) and what TLS SNI uses (Java's
                        // `getHostString()`). For an empty host that is the
                        // loopback address's name, `localhost`, since the
                        // resolved `InetSocketAddress` keeps no literal name.
                        Some(address) => {
                            let host = if host.is_empty() {
                                DefaultHostResolver::EMPTY_HOST_NAME
                            } else {
                                host
                            };
                            addresses.push((host.to_string(), address));
                        },
                        None => warn!(
                            "Couldn't resolve server {} from {} as DNS resolution failed for {}",
                            url,
                            CommonClientConfigs::BOOTSTRAP_SERVERS_CONFIG,
                            host
                        ),
                    }
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

    /// Splits `address` into its host and its port digits, or `None` if it
    /// does not match.
    ///
    /// Translated from Java's `Utils.getHost()` / `Utils.getPort()`, which
    /// `matches()` (whole-string) the address against
    ///
    /// ```text
    /// HOST_PORT_PATTERN = ^(?:[0-9a-zA-Z\-%._]*://)?\[?([0-9a-zA-Z\-%._:]*)]?:([0-9]+)
    /// ```
    ///
    /// and return group 1 / group 2. The pattern needs no backtracking search
    /// here: the scheme class excludes `:`, so an optional scheme ends at the
    /// first `://`; the host class excludes `[`, `]` and `/`, and the port is
    /// all digits, so the port separator is the last `:`. Any character outside
    /// those classes — whitespace included — fails the match.
    ///
    /// The host group is `*`, so an empty host (`":9092"`, `"[]:9092"`) matches
    /// and is returned as `""`; resolving it yields the loopback address
    /// ([`DefaultHostResolver::EMPTY_HOST_ADDRESS`]), as `InetAddress` does,
    /// and the bootstrap entry is keyed by its name
    /// ([`DefaultHostResolver::EMPTY_HOST_NAME`]).
    fn parse_host_port(address: &str) -> Option<(&str, &str)> {
        let is_scheme_char = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '%' | '.' | '_');
        let is_host_char = |c: char| is_scheme_char(c) || c == ':';

        // (?:[0-9a-zA-Z\-%._]*://)?
        let rest = match address.find("://") {
            Some(end) if address[..end].chars().all(is_scheme_char) => &address[end + 3..],
            _ => address,
        };
        // \[?
        let rest = rest.strip_prefix('[').unwrap_or(rest);
        // :([0-9]+) at the end
        let separator = rest.rfind(':')?;
        let port = &rest[separator + 1..];
        if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        // ([0-9a-zA-Z\-%._:]*)]?
        let host = &rest[..separator];
        let host = host.strip_suffix(']').unwrap_or(host);
        if !host.chars().all(is_host_char) {
            return None;
        }
        Some((host, port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    /// `filter_preferred_addresses` on a copy of `addresses`.
    fn sorted(addresses: &[IpAddr]) -> Vec<IpAddr> {
        let mut addresses = addresses.to_vec();
        ClientUtils::filter_preferred_addresses(&mut addresses);
        addresses
    }

    /// Translated from `ClientUtilsTest.testFilterPreferredAddresses`.
    ///
    /// Java expects `[192.0.0.1, 192.0.0.1]` for the IPv4-first input and
    /// `[::1]` for the IPv6-first input (first family wins, the other is
    /// dropped). This translation orders instead of filtering, so both inputs
    /// yield the IPv4 addresses first with `::1` kept after them — the
    /// documented deviation on `filter_preferred_addresses`.
    #[test]
    fn test_filter_preferred_addresses() {
        let ipv4: IpAddr = "192.0.0.1".parse().unwrap();
        let ipv6: IpAddr = "::1".parse().unwrap();

        assert_eq!(sorted(&[ipv4, ipv6, ipv4]), vec![ipv4, ipv4, ipv6]);
        assert_eq!(sorted(&[ipv6, ipv4, ipv4]), vec![ipv4, ipv4, ipv6]);
    }

    /// Regression test for a broker advertising `localhost` from an IPv4-only
    /// Docker network, with `localhost` resolving to both `::1` and
    /// `127.0.0.1`: IPv4 must come first whatever order the resolver returns,
    /// and the IPv6 addresses must be kept after it, in resolver order, as the
    /// fallback for a dual-stack broker whose IPv4 is unreachable.
    #[test]
    fn test_filter_preferred_addresses_ipv4_first_regardless_of_order() {
        let v4_loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let v4_other = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let v6_loopback = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let v6_other = IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1));

        let cases: [(&[IpAddr], &[IpAddr]); 6] = [
            (&[v6_loopback, v4_loopback], &[v4_loopback, v6_loopback]),
            (&[v4_loopback, v6_loopback], &[v4_loopback, v6_loopback]),
            (&[v6_loopback, v6_other, v4_loopback], &[v4_loopback, v6_loopback, v6_other]),
            (
                &[v6_loopback, v4_other, v6_other, v4_loopback],
                &[v4_other, v4_loopback, v6_loopback, v6_other],
            ),
            (&[v4_loopback, v6_loopback, v4_other], &[v4_loopback, v4_other, v6_loopback]),
            (
                &[v6_other, v6_loopback, v4_other, v4_loopback],
                &[v4_other, v4_loopback, v6_other, v6_loopback],
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(sorted(input), expected, "input {input:?}");
        }
    }

    /// A single-family list is returned unchanged, in resolver order.
    #[test]
    fn test_filter_preferred_addresses_single_family() {
        let ipv6_only = [
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 2)),
        ];
        assert_eq!(sorted(&ipv6_only), ipv6_only);
        let ipv4_only = [IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), IpAddr::V4(Ipv4Addr::LOCALHOST)];
        assert_eq!(sorted(&ipv4_only), ipv4_only);
    }

    #[test]
    fn test_filter_preferred_addresses_empty() {
        assert!(sorted(&[]).is_empty());
    }

    /// End to end through `resolve`: a resolver returning `::1` first yields
    /// `127.0.0.1` first, with `::1` kept after it as the fallback.
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
            vec![IpAddr::V4(Ipv4Addr::LOCALHOST), IpAddr::V6(Ipv6Addr::LOCALHOST)]
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
    /// Java asserts only `ConfigException`; the message is pinned too. Every
    /// case fails `HOST_PORT_PATTERN`, so it is "Invalid url" naming the
    /// offending entry verbatim — for the second case the space-prefixed
    /// third entry, after the first two were accepted.
    #[test]
    fn test_invalid_broker_address() {
        let cases: [(&[&str], &str); 3] = [
            (
                &["localhost:9997\nlocalhost:9998\nlocalhost:9999"],
                "localhost:9997\nlocalhost:9998\nlocalhost:9999",
            ),
            (&["localhost:9997", "localhost:9998", " localhost:9999"], " localhost:9999"),
            // Intentionally provide a single string, as users may provide
            // space-separated brokers, which will be parsed as a single string.
            (
                &["localhost:9997 localhost:9998 localhost:9999"],
                "localhost:9997 localhost:9998 localhost:9999",
            ),
        ];
        for (addresses, offending) in cases {
            assert_config_error(
                check_without_lookup(addresses),
                &format!("Invalid url in bootstrap.servers: {offending}"),
            );
        }
    }

    /// `HOST_PORT_PATTERN` edge cases: trailing whitespace, whitespace-only
    /// entries and characters outside the host class fail the match
    /// ("Invalid url"); a scheme prefix and an unbracketed IPv6 literal
    /// match it.
    #[test]
    fn test_host_port_pattern() {
        for url in [
            "localhost:9092 ",
            "localhost:9092\t",
            "   ",
            "local/host:9092",
            "a,b:9092",
            "localhost:",
        ] {
            assert_config_error(
                check_without_lookup(&[url]),
                &format!("Invalid url in bootstrap.servers: {url}"),
            );
        }
        assert_eq!(
            ClientUtils::parse_host_port("PLAINTEXT://localhost:9092"),
            Some(("localhost", "9092"))
        );
        assert_eq!(ClientUtils::parse_host_port("[::1]:9092"), Some(("::1", "9092")));
        assert_eq!(ClientUtils::parse_host_port("::1:9092"), Some(("::1", "9092")));
        assert_eq!(ClientUtils::parse_host_port("a b://localhost:9092"), None);
        // The host group is `*`: an empty host matches.
        assert_eq!(ClientUtils::parse_host_port(":9092"), Some(("", "9092")));
        assert_eq!(ClientUtils::parse_host_port("[]:9092"), Some(("", "9092")));
        assert_eq!(ClientUtils::parse_host_port("PLAINTEXT://:9092"), Some(("", "9092")));
    }

    /// An empty host (`":9092"`) is accepted, as in Java, and resolves to the
    /// loopback address the way `InetAddress.getAllByName("")` does, with no
    /// name-service lookup. In `use_all_dns_ips` mode the node host is
    /// `localhost`: Java's `getHostString()` of the resolved
    /// `InetSocketAddress("", port)` is the loopback address's name. In
    /// canonical mode the one address is keyed by its canonical name, the
    /// textual IP (see the canonical-name deviation; Java gets `localhost`
    /// from the PTR lookup). Either way the bootstrap node is not empty, so
    /// `NetworkClient::ready` (which rejects an empty node) can connect to it.
    #[test]
    fn test_empty_host_resolves_to_loopback() {
        let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);
        for url in [":9092", "[]:9092", "PLAINTEXT://:9092"] {
            let urls = [url.to_string()];
            let addrs = ClientUtils::parse_and_validate_addresses(&urls, ClientDnsLookup::UseAllDnsIps).unwrap();
            assert_eq!(addrs, vec![("localhost".to_string(), loopback)], "url {url}");
            let cluster = crate::common::Cluster::bootstrap(&addrs);
            let node = &cluster.nodes()[0];
            assert!(!node.is_empty(), "url {url}: {node}");
            assert_eq!(node.host(), "localhost");

            let addrs =
                ClientUtils::parse_and_validate_addresses(&urls, ClientDnsLookup::ResolveCanonicalBootstrapServersOnly)
                    .unwrap();
            assert_eq!(addrs, vec![("127.0.0.1".to_string(), loopback)], "url {url}");
            assert!(!crate::common::Cluster::bootstrap(&addrs).nodes()[0].is_empty());
        }
    }

    /// Canonical mode keeps both families, IPv4 first, whatever order the
    /// resolver returns them in (Java's `getAllByName` order on a default
    /// JVM); within a family the resolver's order is kept.
    #[test]
    fn test_parse_and_validate_addresses_with_reverse_lookup_dual_stack() {
        let v6a: IpAddr = "2001:db8::1".parse().unwrap();
        let v4a: IpAddr = "192.0.2.1".parse().unwrap();
        let v6b: IpAddr = "2001:db8::2".parse().unwrap();
        let v4b: IpAddr = "192.0.2.2".parse().unwrap();
        let resolve_all = |host: &str, port: u16| -> io::Result<Vec<SocketAddr>> {
            if host == "dual.example" {
                return Ok([v6a, v4a, v6b, v4b].iter().map(|ip| SocketAddr::new(*ip, port)).collect());
            }
            host.parse::<IpAddr>()
                .map(|ip| vec![SocketAddr::new(ip, port)])
                .map_err(|_| io::Error::new(io::ErrorKind::NotFound, host.to_string()))
        };
        let addrs = ClientUtils::parse_and_validate_addresses_with_lookup(
            &["dual.example:9092".to_string()],
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
            resolve_all,
            |address| address.to_string(),
        )
        .unwrap();
        let expected: Vec<(String, SocketAddr)> = [v4a, v4b, v6a, v6b]
            .iter()
            .map(|ip| (ip.to_string(), SocketAddr::new(*ip, 9092)))
            .collect();
        assert_eq!(addrs, expected);
    }

    /// Error precedence between the host and the port, per mode, as in Java:
    /// canonical mode calls `getAllByName(host)` before
    /// `new InetSocketAddress(canonical, port)` checks the port, so an
    /// unresolvable host wins; the default mode's `InetSocketAddress(host,
    /// port)` swallows the unknown host, so the port wins. An `int` overflow
    /// fails `Utils.getPort` before either branch, so it is "Invalid port" in
    /// both modes.
    #[test]
    fn test_invalid_port_versus_unknown_host() {
        let unresolvable = |host: &str, _: u16| -> io::Result<Vec<SocketAddr>> {
            Err(io::Error::new(io::ErrorKind::NotFound, host.to_string()))
        };
        let resolvable = |_: &str, port: u16| -> io::Result<Vec<SocketAddr>> {
            Ok(vec![SocketAddr::new("192.0.2.1".parse().unwrap(), port)])
        };
        let run = |url: &str, mode, resolve_all: &dyn Fn(&str, u16) -> io::Result<Vec<SocketAddr>>| {
            ClientUtils::parse_and_validate_addresses_with_lookup(&[url.to_string()], mode, resolve_all, |a| {
                a.to_string()
            })
        };
        let canonical = ClientDnsLookup::ResolveCanonicalBootstrapServersOnly;
        let default = ClientDnsLookup::UseAllDnsIps;

        assert_config_error(
            run("bad.host:70000", canonical, &unresolvable),
            "Unknown host in bootstrap.servers: bad.host:70000",
        );
        assert_config_error(
            run("good.host:70000", canonical, &resolvable),
            "Invalid port in bootstrap.servers: good.host:70000",
        );
        assert_config_error(
            run("bad.host:70000", default, &unresolvable),
            "Invalid port in bootstrap.servers: bad.host:70000",
        );
        assert_config_error(
            run("good.host:70000", default, &resolvable),
            "Invalid port in bootstrap.servers: good.host:70000",
        );
        for mode in [canonical, default] {
            assert_config_error(
                run("bad.host:99999999999", mode, &unresolvable),
                "Invalid port in bootstrap.servers: bad.host:99999999999",
            );
        }
    }

    /// A port too large for `Integer.parseInt` is a `NumberFormatException`,
    /// i.e. an `IllegalArgumentException`, so Java reports "Invalid port", not
    /// "Invalid url".
    #[test]
    fn test_port_overflowing_int() {
        assert_config_error(
            check_without_lookup(&["localhost:99999999999"]),
            "Invalid port in bootstrap.servers: localhost:99999999999",
        );
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

    /// Empty entries are skipped (Java skips `null` / empty URLs), leaving no
    /// address. A whitespace-only entry is not empty, so it is "Invalid url"
    /// (see `test_host_port_pattern`).
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
                ClientUtils::parse_and_validate_addresses(&[String::new(), String::new()], mode),
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
