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

    /// Config key: `security.protocol`
    pub const SECURITY_PROTOCOL_CONFIG: &str = "security.protocol";

    /// Config key: `metadata.recovery.strategy`
    ///
    /// Controls how the client recovers when none of the brokers known to it is
    /// available. If set to `none`, the client fails. If set to `rebootstrap`, the
    /// client repeats the bootstrap process using `bootstrap.servers`.
    /// Rebootstrapping is useful when a client communicates with brokers so
    /// infrequently that the set of brokers may change entirely before the client
    /// refreshes metadata. Metadata recovery is triggered when all last-known
    /// brokers appear unavailable simultaneously. Brokers appear unavailable when
    /// disconnected and no current retry attempt is in-progress. Consider
    /// increasing `reconnect.backoff.ms` and `reconnect.backoff.max.ms` and
    /// decreasing `socket.connection.setup.timeout.ms` and
    /// `socket.connection.setup.timeout.max.ms` for the client. Rebootstrap is
    /// also triggered if connection cannot be established to any of the brokers
    /// for `metadata.recovery.rebootstrap.trigger.ms` milliseconds or if server
    /// requests rebootstrap.
    ///
    /// (Text of Java's `CommonClientConfigs.METADATA_RECOVERY_STRATEGY_DOC`,
    /// carried as rustdoc for the reason given at
    /// [`CLIENT_DNS_LOOKUP_CONFIG`](Self::CLIENT_DNS_LOOKUP_CONFIG). The parsed
    /// value is a [`crate::MetadataRecoveryStrategy`].)
    pub const METADATA_RECOVERY_STRATEGY_CONFIG: &str = "metadata.recovery.strategy";

    /// The default `metadata.recovery.strategy`: `MetadataRecoveryStrategy.REBOOTSTRAP.name`.
    pub const DEFAULT_METADATA_RECOVERY_STRATEGY: &str = "rebootstrap";

    /// Config key: `metadata.recovery.rebootstrap.trigger.ms`
    ///
    /// If a client configured to rebootstrap using
    /// `metadata.recovery.strategy=rebootstrap` is unable to obtain metadata from
    /// any of the brokers in the last known metadata for this interval, client
    /// repeats the bootstrap process using `bootstrap.servers` configuration.
    ///
    /// (Text of Java's `CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC`.)
    pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG: &str = "metadata.recovery.rebootstrap.trigger.ms";

    /// The default `metadata.recovery.rebootstrap.trigger.ms`: 5 minutes.
    pub const DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS: i64 = 300 * 1000;

    /// The base for exponential retry backoff.
    pub const RETRY_BACKOFF_EXP_BASE: i32 = 2;

    /// The jitter factor for retry backoff.
    pub const RETRY_BACKOFF_JITTER: f64 = 0.2;
}
