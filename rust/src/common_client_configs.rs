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

use std::collections::HashMap;

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

    /// Config key: `metadata.cluster.check.enable` (KIP-1242)
    ///
    /// Whether the client should send cluster and node information when
    /// connecting to a broker to enable it to check for a misrouted
    /// connection. This configuration is ignored if rebootstrapping is
    /// disabled by setting the configuration `metadata.recovery.strategy=none`.
    /// If the client is connecting to a broker older than Apache Kafka 4.4, no
    /// checking is performed and this configuration has no effect.
    ///
    /// (Text of Java's `CommonClientConfigs.METADATA_CLUSTER_CHECK_ENABLE_DOC`
    /// (`CommonClientConfigs.java:267-271`), carried as rustdoc for the reason
    /// given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`]. The producer, consumer and
    /// admin configs define it as `Type.BOOLEAN` with default `true`.)
    pub const METADATA_CLUSTER_CHECK_ENABLE_CONFIG: &str = "metadata.cluster.check.enable";

    /// Config key: `security.protocol`
    pub const SECURITY_PROTOCOL_CONFIG: &str = "security.protocol";

    /// Config key: `reconnect.backoff.ms`
    ///
    /// The base amount of time to wait before attempting to reconnect to a
    /// given host. This avoids repeatedly connecting to a host in a tight loop.
    /// This backoff applies to all connection attempts by the client to a
    /// broker. This value is the initial backoff value and will increase
    /// exponentially for each consecutive connection failure, up to the
    /// `reconnect.backoff.max.ms` value.
    ///
    /// (Text of Java's `CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC`, carried as
    /// rustdoc for the reason given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`].)
    pub const RECONNECT_BACKOFF_MS_CONFIG: &str = "reconnect.backoff.ms";

    /// Config key: `reconnect.backoff.max.ms`
    ///
    /// The maximum amount of time in milliseconds to wait when reconnecting to
    /// a broker that has repeatedly failed to connect. If provided, the backoff
    /// per host will increase exponentially for each consecutive connection
    /// failure, up to this maximum. After calculating the backoff increase, 20%
    /// random jitter is added to avoid connection storms.
    ///
    /// (Text of Java's `CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC`.)
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &str = "reconnect.backoff.max.ms";

    /// Config key: `retry.backoff.ms`
    ///
    /// The amount of time to wait before attempting to retry a failed request
    /// to a given topic partition. This avoids repeatedly sending requests in a
    /// tight loop under some failure scenarios. This value is the initial
    /// backoff value and will increase exponentially for each failed request,
    /// up to the `retry.backoff.max.ms` value.
    ///
    /// (Text of Java's `CommonClientConfigs.RETRY_BACKOFF_MS_DOC`.)
    pub const RETRY_BACKOFF_MS_CONFIG: &str = "retry.backoff.ms";

    /// Config key: `retry.backoff.max.ms`
    ///
    /// The maximum amount of time in milliseconds to wait when retrying a
    /// request to the broker that has repeatedly failed. If provided, the
    /// backoff per client will increase exponentially for each failed request,
    /// up to this maximum. To prevent all clients from being synchronized upon
    /// retry, a randomized jitter with a factor of 0.2 will be applied to the
    /// backoff, resulting in the backoff falling within a range between 20%
    /// below and 20% above the computed value. If `retry.backoff.ms` is set to
    /// be higher than `retry.backoff.max.ms`, then `retry.backoff.max.ms` will
    /// be used as a constant backoff from the beginning without any exponential
    /// increase.
    ///
    /// (Text of Java's `CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC`.)
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &str = "retry.backoff.max.ms";

    /// Config key: `send.buffer.bytes`
    ///
    /// The size of the TCP send buffer (SO_SNDBUF) to use when sending data.
    /// If the value is -1, the OS default will be used.
    ///
    /// (Text of Java's `CommonClientConfigs.SEND_BUFFER_DOC`, carried as rustdoc
    /// for the reason given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`].)
    pub const SEND_BUFFER_CONFIG: &str = "send.buffer.bytes";

    /// The lowest valid `send.buffer.bytes`: `-1`, the OS default.
    pub const SEND_BUFFER_LOWER_BOUND: i32 = -1;

    /// Config key: `receive.buffer.bytes`
    ///
    /// The size of the TCP receive buffer (SO_RCVBUF) to use when reading
    /// data. If the value is -1, the OS default will be used.
    ///
    /// (Text of Java's `CommonClientConfigs.RECEIVE_BUFFER_DOC`, carried as
    /// rustdoc for the reason given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`].)
    pub const RECEIVE_BUFFER_CONFIG: &str = "receive.buffer.bytes";

    /// The lowest valid `receive.buffer.bytes`: `-1`, the OS default.
    pub const RECEIVE_BUFFER_LOWER_BOUND: i32 = -1;

    /// Config key: `socket.connection.setup.timeout.ms`
    ///
    /// The amount of time the client will wait for the socket connection to be
    /// established. If the connection is not built before the timeout elapses,
    /// clients will close the socket channel. This value is the initial backoff
    /// value and will increase exponentially for each consecutive connection
    /// failure, up to the `socket.connection.setup.timeout.max.ms` value.
    ///
    /// (Text of Java's `CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC`,
    /// carried as rustdoc for the reason given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`].)
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &str = "socket.connection.setup.timeout.ms";

    /// The default `socket.connection.setup.timeout.ms`: 10 seconds.
    pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS: i64 = 10 * 1000;

    /// Config key: `socket.connection.setup.timeout.max.ms`
    ///
    /// The maximum amount of time the client will wait for the socket
    /// connection to be established. The connection setup timeout will increase
    /// exponentially for each consecutive connection failure up to this
    /// maximum. To avoid connection storms, a randomization factor of 0.2 will
    /// be applied to the timeout resulting in a random range between 20% below
    /// and 20% above the computed value.
    ///
    /// (Text of Java's `CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC`,
    /// carried as rustdoc for the reason given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`].)
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &str = "socket.connection.setup.timeout.max.ms";

    /// The default `socket.connection.setup.timeout.max.ms`: 30 seconds.
    pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS: i64 = 30 * 1000;

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

    /// Postprocess the configuration so that exponential backoff is disabled
    /// when reconnect backoff is explicitly configured but the maximum
    /// reconnect backoff is not explicitly configured.
    ///
    /// Java's `postProcessReconnectBackoffConfigs(AbstractConfig config,
    /// Map<String, Object> parsedValues)` (`CommonClientConfigs.java:282-293`)
    /// reads `config.originals()` and returns the values to override. This
    /// crate's configs are not `AbstractConfig`s, so it takes the user's map
    /// (Java's `originals()`) and the parsed `reconnect.backoff.ms`, and
    /// returns the new `reconnect.backoff.max.ms`: the parsed
    /// `reconnect.backoff.ms` when only that key was set, `None` otherwise.
    #[doc(alias = "org.apache.kafka.clients.CommonClientConfigs#postProcessReconnectBackoffConfigs")]
    pub(crate) fn post_process_reconnect_backoff_configs(
        originals: &HashMap<String, String>,
        reconnect_backoff_ms: i64,
    ) -> Option<i64> {
        if !originals.contains_key(Self::RECONNECT_BACKOFF_MAX_MS_CONFIG)
            && originals.contains_key(Self::RECONNECT_BACKOFF_MS_CONFIG)
        {
            log::warn!(
                "Disabling exponential reconnect backoff because {} is set, but {} is not.",
                Self::RECONNECT_BACKOFF_MS_CONFIG,
                Self::RECONNECT_BACKOFF_MAX_MS_CONFIG
            );
            return Some(reconnect_backoff_ms);
        }
        None
    }

    /// Log warning if the exponential backoff is disabled due to initial
    /// backoff value is greater than max backoff value.
    ///
    /// Java's `warnDisablingExponentialBackoff(AbstractConfig config)`
    /// (`CommonClientConfigs.java:300-318`) reads the four values from the
    /// config; this crate's configs are not `AbstractConfig`s, so it takes them.
    #[doc(alias = "org.apache.kafka.clients.CommonClientConfigs#warnDisablingExponentialBackoff")]
    pub(crate) fn warn_disabling_exponential_backoff(
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        connection_setup_timeout_ms: i64,
        connection_setup_timeout_max_ms: i64,
    ) {
        for warning in Self::exponential_backoff_warnings(
            retry_backoff_ms,
            retry_backoff_max_ms,
            connection_setup_timeout_ms,
            connection_setup_timeout_max_ms,
        ) {
            log::warn!("{warning}");
        }
    }

    /// The warnings [`Self::warn_disabling_exponential_backoff`] logs, with
    /// Java's text, in Java's order.
    fn exponential_backoff_warnings(
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        connection_setup_timeout_ms: i64,
        connection_setup_timeout_max_ms: i64,
    ) -> Vec<String> {
        let mut warnings = Vec::new();
        if retry_backoff_ms > retry_backoff_max_ms {
            warnings.push(format!(
                "Configuration '{}' with value '{retry_backoff_ms}' is greater than configuration '{}' with value \
                 '{retry_backoff_max_ms}'. A static backoff with value '{retry_backoff_max_ms}' will be applied.",
                Self::RETRY_BACKOFF_MS_CONFIG,
                Self::RETRY_BACKOFF_MAX_MS_CONFIG,
            ));
        }
        if connection_setup_timeout_ms > connection_setup_timeout_max_ms {
            warnings.push(format!(
                "Configuration '{}' with value '{connection_setup_timeout_ms}' is greater than configuration '{}' \
                 with value '{connection_setup_timeout_max_ms}'. A static connection setup timeout with value \
                 '{connection_setup_timeout_max_ms}' will be applied.",
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
            ));
        }
        warnings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::AdminClientConfig;

    /// Java's `CommonClientConfigsTest.TestConfig` is an `AbstractConfig`
    /// whose `postProcessParsedConfig` calls this class's statics. This
    /// crate has no `AbstractConfig`; `AdminClientConfig` stands in for it,
    /// since its `postProcessParsedConfig` calls the same ones
    /// (`AdminClientConfig.java:318-323`).
    fn test_config(props: &[(&str, &str)]) -> Result<AdminClientConfig, crate::common::Error> {
        let mut map = HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())]);
        map.extend(props.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        AdminClientConfig::new(&map)
    }

    /// Translated from `CommonClientConfigsTest.testExponentialBackoffDefaults`:
    /// with only `reconnect.backoff.ms` set, `reconnect.backoff.max.ms` takes
    /// its value.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.CommonClientConfigsTest#testExponentialBackoffDefaults")]
    fn test_exponential_backoff_defaults() {
        let default_conf = test_config(&[]).unwrap();
        assert_eq!(default_conf.reconnect_backoff_ms(), 50);
        assert_eq!(default_conf.reconnect_backoff_max_ms(), 1000);

        let both_set_config =
            test_config(&[("reconnect.backoff.ms", "123"), ("reconnect.backoff.max.ms", "12345")]).unwrap();
        assert_eq!(both_set_config.reconnect_backoff_ms(), 123);
        assert_eq!(both_set_config.reconnect_backoff_max_ms(), 12345);

        let reconnect_backoff_set_conf = test_config(&[("reconnect.backoff.ms", "123")]).unwrap();
        assert_eq!(reconnect_backoff_set_conf.reconnect_backoff_ms(), 123);
        assert_eq!(reconnect_backoff_set_conf.reconnect_backoff_max_ms(), 123);
    }

    /// The texts of `CommonClientConfigs.warnDisablingExponentialBackoff`'s
    /// two warnings (`CommonClientConfigs.java:303-317`): one when
    /// `retry.backoff.ms` exceeds `retry.backoff.max.ms`, one when
    /// `socket.connection.setup.timeout.ms` exceeds its max. Equal values do
    /// not warn.
    #[test]
    fn test_exponential_backoff_warnings() {
        assert!(CommonClientConfigs::exponential_backoff_warnings(100, 1000, 10_000, 30_000).is_empty());
        assert!(CommonClientConfigs::exponential_backoff_warnings(1000, 1000, 30_000, 30_000).is_empty());
        assert_eq!(
            CommonClientConfigs::exponential_backoff_warnings(2000, 1000, 40_000, 30_000),
            vec![
                "Configuration 'retry.backoff.ms' with value '2000' is greater than configuration \
                 'retry.backoff.max.ms' with value '1000'. A static backoff with value '1000' will be applied."
                    .to_string(),
                "Configuration 'socket.connection.setup.timeout.ms' with value '40000' is greater than \
                 configuration 'socket.connection.setup.timeout.max.ms' with value '30000'. A static connection \
                 setup timeout with value '30000' will be applied."
                    .to_string(),
            ]
        );
    }

    /// `postProcessReconnectBackoffConfigs` overrides `reconnect.backoff.max.ms`
    /// with the parsed `reconnect.backoff.ms` only when the user set the base
    /// and not the max.
    #[test]
    fn test_post_process_reconnect_backoff_configs() {
        let originals = |keys: &[&str]| -> HashMap<String, String> {
            keys.iter().map(|k| (k.to_string(), "1".to_string())).collect()
        };
        assert_eq!(
            CommonClientConfigs::post_process_reconnect_backoff_configs(&originals(&[]), 50),
            None
        );
        assert_eq!(
            CommonClientConfigs::post_process_reconnect_backoff_configs(&originals(&["reconnect.backoff.ms"]), 123),
            Some(123)
        );
        assert_eq!(
            CommonClientConfigs::post_process_reconnect_backoff_configs(
                &originals(&["reconnect.backoff.ms", "reconnect.backoff.max.ms"]),
                123
            ),
            None
        );
        assert_eq!(
            CommonClientConfigs::post_process_reconnect_backoff_configs(&originals(&["reconnect.backoff.max.ms"]), 50),
            None
        );
    }
}
