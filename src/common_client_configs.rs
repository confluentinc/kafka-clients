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

//! Common client configuration constants.
//!
//! Corresponds to `org.apache.kafka.clients.CommonClientConfigs`.
//! Only the constants needed by the Metadata class are included here;
//! full configuration support will be added as needed.

/// Configuration keys shared by producer, consumer and admin clients.
///
/// Translates the Java static-utility class
/// `org.apache.kafka.clients.CommonClientConfigs`, which has no instance state,
/// so it becomes a unit struct hosting its statics as associated items.
pub struct CommonClientConfigs;

impl CommonClientConfigs {
    /// Config key: `bootstrap.servers`
    pub const BOOTSTRAP_SERVERS_CONFIG: &str = "bootstrap.servers";

    /// Config key: `client.dns.lookup`
    ///
    /// Controls how the client uses DNS lookups. If set to `use_all_dns_ips`,
    /// connect to each returned IP address in sequence until a successful
    /// connection is established. After a disconnection, the next IP is used.
    /// Once all IPs have been used once, the client resolves the IP(s) from the
    /// hostname again (both the JVM and the OS cache DNS name lookups,
    /// however). If set to `resolve_canonical_bootstrap_servers_only`, resolve
    /// each bootstrap address into a list of canonical names. After the
    /// bootstrap phase, this behaves the same as `use_all_dns_ips`.
    ///
    /// (Text of Java's `CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC`. That
    /// constant only feeds `ConfigDef`'s generated documentation, which this
    /// crate does not have, so it is carried here as rustdoc rather than as an
    /// unused string constant. The parsed value is a
    /// [`crate::ClientDnsLookup`].)
    pub const CLIENT_DNS_LOOKUP_CONFIG: &str = "client.dns.lookup";

    /// Config key: `security.protocol`
    pub const SECURITY_PROTOCOL_CONFIG: &str = "security.protocol";

    /// The base for exponential retry backoff.
    pub const RETRY_BACKOFF_EXP_BASE: i32 = 2;

    /// The jitter factor for retry backoff.
    pub const RETRY_BACKOFF_JITTER: f64 = 0.2;
}
