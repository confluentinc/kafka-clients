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
#[doc(alias = "org.apache.kafka.clients.CommonClientConfigs")]
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

    /// Config key: `client.rack`
    ///
    /// A rack identifier for this client. This can be any string value which
    /// indicates where this client is physically located. It corresponds with the
    /// broker config `broker.rack`.
    ///
    /// (Text of Java's `CommonClientConfigs.CLIENT_RACK_DOC`, carried as rustdoc
    /// for the same reason as [`Self::CLIENT_DNS_LOOKUP_CONFIG`]'s.)
    pub const CLIENT_RACK_CONFIG: &str = "client.rack";

    /// Default value of `client.rack`: no rack.
    pub const DEFAULT_CLIENT_RACK: &str = "";

    /// Config key: `bootstrap.resolve.timeout.ms` (KIP-909)
    ///
    /// Selects the client's bootstrap DNS resolution mode. When set to `0`
    /// (the default), DNS is resolved synchronously during client
    /// construction; any failure surfaces as `ConfigException` and no client
    /// instance is created. When set to a positive value, DNS is resolved
    /// asynchronously and this is the maximum amount of time the client will
    /// spend retrying resolution before failing with an unrecoverable
    /// `BootstrapResolutionException` from subsequent API calls (the client
    /// must then be closed and re-created after fixing the underlying DNS or
    /// `bootstrap.servers` configuration issue). Setting this config to a
    /// positive value enables an evolving feature whose compatibility may be
    /// broken in a minor release.
    ///
    /// (Text of Java's `CommonClientConfigs.BOOTSTRAP_RESOLVE_TIMEOUT_MS_DOC`,
    /// carried as rustdoc for the reason given on
    /// [`Self::CLIENT_DNS_LOOKUP_CONFIG`]. In this crate `ConfigException` is
    /// [`Error::Config`](crate::common::Error::Config) and
    /// `BootstrapResolutionException` is
    /// [`BootstrapResolutionError`](crate::common::errors::BootstrapResolutionError).)
    pub const BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: &str = "bootstrap.resolve.timeout.ms";

    /// The default `bootstrap.resolve.timeout.ms`: `0`, synchronous resolution
    /// at construction (KAFKA-20939 restored this default; KIP-909 first
    /// shipped `2 * 60 * 1000`).
    pub const DEFAULT_BOOTSTRAP_RESOLVE_TIMEOUT_MS: i64 = 0;

    /// Config key: `security.protocol`
    pub const SECURITY_PROTOCOL_CONFIG: &str = "security.protocol";

    /// The base for exponential retry backoff.
    pub const RETRY_BACKOFF_EXP_BASE: i32 = 2;

    /// The jitter factor for retry backoff.
    pub const RETRY_BACKOFF_JITTER: f64 = 0.2;
}
