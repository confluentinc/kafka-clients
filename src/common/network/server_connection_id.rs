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

//! Translation of `org.apache.kafka.common.network.ServerConnectionId`.

use regex::Regex;
use std::sync::OnceLock;

/// Regex for parsing the `host:port` portion of a connection id. Mirrors
/// the Java pattern `([0-9a-zA-Z\-%._:]*):([0-9]+)` (greedy host on
/// `:` so IPv6 addresses without brackets are captured correctly).
fn host_port_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // The Rust `regex` crate matches the entire input only when the
        // anchors are explicit (`^` / `$`). Java's `Matcher.matches()` is
        // implicitly anchored — we add anchors here for parity.
        Regex::new(r"^([0-9a-zA-Z\-%._:]*):([0-9]+)$").expect("valid regex")
    })
}

/// Uniquely identifies a connection on the broker side. The on-the-wire
/// format is `localHost:localPort-remoteHost:remotePort-processorId-index`.
/// Mirrors `org.apache.kafka.common.network.ServerConnectionId`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServerConnectionId {
    local_host: String,
    local_port: i32,
    remote_host: String,
    remote_port: i32,
    processor_id: i32,
    index: i32,
}

impl ServerConnectionId {
    /// Mirrors the public Java constructor.
    pub fn new(
        local_host: impl Into<String>,
        local_port: i32,
        remote_host: impl Into<String>,
        remote_port: i32,
        processor_id: i32,
        index: i32,
    ) -> Self {
        ServerConnectionId {
            local_host: local_host.into(),
            local_port,
            remote_host: remote_host.into(),
            remote_port,
            processor_id,
            index,
        }
    }

    /// Mirrors `ServerConnectionId.localHost()`.
    pub fn local_host(&self) -> &str {
        &self.local_host
    }

    /// Mirrors `ServerConnectionId.localPort()`.
    pub fn local_port(&self) -> i32 {
        self.local_port
    }

    /// Mirrors `ServerConnectionId.remoteHost()`.
    pub fn remote_host(&self) -> &str {
        &self.remote_host
    }

    /// Mirrors `ServerConnectionId.remotePort()`.
    pub fn remote_port(&self) -> i32 {
        self.remote_port
    }

    /// Mirrors `ServerConnectionId.processorId()`.
    pub fn processor_id(&self) -> i32 {
        self.processor_id
    }

    /// Mirrors `ServerConnectionId.index()`.
    pub fn index(&self) -> i32 {
        self.index
    }

    /// Parse a `host:port` string. Mirrors the package-visible
    /// `ServerConnectionId.parseHostPort(String)`.
    pub fn parse_host_port(connection_string: &str) -> Option<(String, i32)> {
        let captures = host_port_pattern().captures(connection_string)?;
        let host = captures.get(1)?.as_str();
        let port_str = captures.get(2)?.as_str();
        let port = port_str.parse::<i32>().ok()?;
        Some((host.to_owned(), port))
    }

    /// Parse a connection-id string. Mirrors
    /// `ServerConnectionId.fromString(String)`. Returns `None` if the
    /// input does not split into exactly 4 dash-delimited segments or if
    /// any of the host/port/processor/index segments fails to parse.
    pub fn from_string(connection_id_string: &str) -> Option<ServerConnectionId> {
        let split: Vec<&str> = connection_id_string.split('-').collect();
        if split.len() != 4 {
            return None;
        }

        let (local_host, local_port) = Self::parse_host_port(split[0])?;
        let (remote_host, remote_port) = Self::parse_host_port(split[1])?;
        let processor_id: i32 = split[2].parse().ok()?;
        let index: i32 = split[3].parse().ok()?;

        Some(ServerConnectionId { local_host, local_port, remote_host, remote_port, processor_id, index })
    }

    /// Format a connection id from already-resolved endpoint addresses.
    /// Mirrors `ServerConnectionId.generateConnectionId(Socket, int, int)`
    /// after the `Socket` accessors have been resolved.
    ///
    /// The Java overload takes a `java.net.Socket` and reads
    /// `getLocalAddress().getHostAddress()`, `getLocalPort()`,
    /// `getInetAddress().getHostAddress()`, and `getPort()`. We do not
    /// have a `Socket` abstraction in Phase 5a; once Phase 5b wires up
    /// `tokio::net::TcpStream`, a thin wrapper can extract these four
    /// values from the local/peer `SocketAddr` and call this function.
    pub fn generate_connection_id(
        local_host: &str,
        local_port: i32,
        remote_host: &str,
        remote_port: i32,
        processor_id: i32,
        connection_index: i32,
    ) -> String {
        format!("{local_host}:{local_port}-{remote_host}:{remote_port}-{processor_id}-{connection_index}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `ServerConnectionIdTest.testFromString`.
    #[test]
    fn from_string() {
        // Plain host names.
        let id = ServerConnectionId::from_string("localhost:9092-localhost:9093-1-2").expect("present");
        assert_eq!(id.local_host(), "localhost");
        assert_eq!(id.local_port(), 9092);
        assert_eq!(id.remote_host(), "localhost");
        assert_eq!(id.remote_port(), 9093);
        assert_eq!(id.processor_id(), 1);
        assert_eq!(id.index(), 2);

        let id = ServerConnectionId::from_string("localhost:9092-127.0.0.1:9093-0-0").expect("present");
        assert_eq!(id.local_host(), "localhost");
        assert_eq!(id.local_port(), 9092);
        assert_eq!(id.remote_host(), "127.0.0.1");
        assert_eq!(id.remote_port(), 9093);
        assert_eq!(id.processor_id(), 0);
        assert_eq!(id.index(), 0);

        // IPv6 endpoints.
        let id = ServerConnectionId::from_string("2001:db8:0:0:0:0:0:1:9092-127.0.0.1:9093-1-2").expect("present");
        assert_eq!(id.local_host(), "2001:db8:0:0:0:0:0:1");
        assert_eq!(id.local_port(), 9092);
        assert_eq!(id.remote_host(), "127.0.0.1");
        assert_eq!(id.remote_port(), 9093);
        assert_eq!(id.processor_id(), 1);
        assert_eq!(id.index(), 2);

        let id = ServerConnectionId::from_string("2002:db9:1:0:0:0:0:1:9092-2001:db8::1:9093-0-1").expect("present");
        assert_eq!(id.local_host(), "2002:db9:1:0:0:0:0:1");
        assert_eq!(id.local_port(), 9092);
        assert_eq!(id.remote_host(), "2001:db8::1");
        assert_eq!(id.remote_port(), 9093);
        assert_eq!(id.processor_id(), 0);
        assert_eq!(id.index(), 1);
    }

    /// Translation of `ServerConnectionIdTest.testFromStringInvalid`.
    #[test]
    fn from_string_invalid() {
        // Wrong number of dash-segments.
        assert!(ServerConnectionId::from_string("localhost:9092-localhost:9093-1").is_none());
        assert!(ServerConnectionId::from_string("localhost:9092-localhost:9093-1-2-3").is_none());
        // Invalid separator.
        assert!(ServerConnectionId::from_string("localhost-9092-localhost:9093-1-2").is_none());
        assert!(ServerConnectionId::from_string("localhost:9092:localhost-9093-1-2").is_none());
        // No `:` separator in port.
        assert!(ServerConnectionId::from_string("localhost9092-localhost:9093-1-2").is_none());
        assert!(ServerConnectionId::from_string("localhost:9092-localhost9093-1-2").is_none());
        // Invalid port.
        assert!(ServerConnectionId::from_string("localhost:abcd-localhost:9093-1-2").is_none());
        assert!(ServerConnectionId::from_string("localhost:9092-localhost:abcd-1-2").is_none());
        // Invalid processorId.
        assert!(ServerConnectionId::from_string("localhost:9092-localhost:9093-a-2").is_none());
        // Invalid index.
        assert!(ServerConnectionId::from_string("localhost:9092-localhost:9093-1-b").is_none());
        // Invalid IPv6 address (brackets aren't part of the host charset).
        assert!(ServerConnectionId::from_string("[2001:db8:0:0:0:0:0:1]:9092-127.0.0.1:9093-1-2").is_none());
    }

    /// Translation of `ServerConnectionIdTest.testGenerateConnectionId`.
    /// Java mocks a `Socket` via Mockito; we drive the formatter directly.
    #[test]
    fn generate_connection_id() {
        assert_eq!(
            ServerConnectionId::generate_connection_id("127.0.0.1", 9092, "127.0.0.1", 9093, 0, 0),
            "127.0.0.1:9092-127.0.0.1:9093-0-0"
        );
        assert_eq!(
            ServerConnectionId::generate_connection_id("127.0.0.1", 9092, "127.0.0.1", 9093, 1, 2),
            "127.0.0.1:9092-127.0.0.1:9093-1-2"
        );
    }

    /// Translation of `ServerConnectionIdTest.testGenerateConnectionIdIpV6`.
    /// Java's `InetAddress.getHostAddress()` strips IPv6 brackets and
    /// canonicalises the address (e.g. `2001:db8::1` →
    /// `2001:db8:0:0:0:0:0:1`). We pass the canonical form directly,
    /// since address resolution will live in Phase 5b.
    #[test]
    fn generate_connection_id_ipv6() {
        assert_eq!(
            ServerConnectionId::generate_connection_id("2001:db8:0:0:0:0:0:1", 9092, "127.0.0.1", 9093, 1, 2),
            "2001:db8:0:0:0:0:0:1:9092-127.0.0.1:9093-1-2"
        );
        assert_eq!(
            ServerConnectionId::generate_connection_id(
                "2002:db9:1:0:0:0:0:1",
                9092,
                "2001:db8:0:0:0:0:0:1",
                9093,
                1,
                2
            ),
            "2002:db9:1:0:0:0:0:1:9092-2001:db8:0:0:0:0:0:1:9093-1-2"
        );
    }

    /// Translation of `ServerConnectionIdTest.testParseHostPort`.
    #[test]
    fn parse_host_port() {
        let (host, port) = ServerConnectionId::parse_host_port("myhost:9092").expect("present");
        assert_eq!(host, "myhost");
        assert_eq!(port, 9092);

        let (host, port) = ServerConnectionId::parse_host_port("127.0.0.1:9092").expect("present");
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 9092);

        let (host, port) = ServerConnectionId::parse_host_port("2001:db8::1:9092").expect("present");
        assert_eq!(host, "2001:db8::1");
        assert_eq!(port, 9092);
    }

    /// Translation of `ServerConnectionIdTest.testParseHostPortInvalid`.
    #[test]
    fn parse_host_port_invalid() {
        // `-` is not a valid port separator.
        assert!(ServerConnectionId::parse_host_port("myhost-9092").is_none());
        // Missing separator.
        assert!(ServerConnectionId::parse_host_port("myhost9092").is_none());
        // Non-numeric port.
        assert!(ServerConnectionId::parse_host_port("myhost:abcd").is_none());
        // Brackets aren't part of the IPv6 host charset.
        assert!(ServerConnectionId::parse_host_port("[2001:db8::1]:9092").is_none());
    }
}
