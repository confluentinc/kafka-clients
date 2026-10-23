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

//! Common client configuration constants and the post-processing helpers the
//! client configs share.
//!
//! Corresponds to `org.apache.kafka.clients.CommonClientConfigs`, which is not
//! `@InterfaceAudience.Public`; it stays crate-private. The constants are the
//! ones the crate reads; `metricsReporters` and `telemetryReporter` are not
//! translated, as they configure class-valued keys (CLAUDE.md §2).

use std::collections::HashMap;

use crate::common::Error;
use crate::common::config::SaslConfigs;
use crate::common::config::abstract_config::{AbstractConfig, ConfigMap};
use crate::common::security::auth::SecurityProtocol;

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

    /// Config key: `metadata.recovery.strategy`
    ///
    /// Controls how the client recovers when none of the brokers known to it
    /// is available. If set to `none`, the client fails. If set to
    /// `rebootstrap`, the client repeats the bootstrap process using
    /// `bootstrap.servers`. Rebootstrapping is useful when a client
    /// communicates with brokers so infrequently that the set of brokers may
    /// change entirely before the client refreshes metadata. Metadata recovery
    /// is triggered when all last-known brokers appear unavailable
    /// simultaneously. Brokers appear unavailable when disconnected and no
    /// current retry attempt is in-progress. Consider increasing
    /// `reconnect.backoff.ms` and `reconnect.backoff.max.ms` and decreasing
    /// `socket.connection.setup.timeout.ms` and
    /// `socket.connection.setup.timeout.max.ms` for the client. Rebootstrap is
    /// also triggered if connection cannot be established to any of the
    /// brokers for `metadata.recovery.rebootstrap.trigger.ms` milliseconds or
    /// if server requests rebootstrap.
    ///
    /// (Text of Java's `CommonClientConfigs.METADATA_RECOVERY_STRATEGY_DOC`
    /// (`CommonClientConfigs.java:248-257`), carried as rustdoc for the reason
    /// given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`].)
    pub const METADATA_RECOVERY_STRATEGY_CONFIG: &str = "metadata.recovery.strategy";

    /// Config key: `metadata.recovery.rebootstrap.trigger.ms`
    ///
    /// If a client configured to rebootstrap using
    /// `metadata.recovery.strategy=rebootstrap` is unable to obtain metadata
    /// from any of the brokers in the last known metadata for this interval,
    /// client repeats the bootstrap process using `bootstrap.servers`
    /// configuration.
    ///
    /// (Text of Java's
    /// `CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC`
    /// (`CommonClientConfigs.java:262-264`), carried as rustdoc for the reason
    /// given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`].)
    pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG: &str = "metadata.recovery.rebootstrap.trigger.ms";

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

    /// The default `security.protocol`.
    pub const DEFAULT_SECURITY_PROTOCOL: &str = "PLAINTEXT";

    /// The base for exponential retry backoff.
    pub const RETRY_BACKOFF_EXP_BASE: i32 = 2;

    /// The jitter factor for retry backoff.
    pub const RETRY_BACKOFF_JITTER: f64 = 0.2;
}

/// The keys and defaults the client config definitions share
/// (`AdminClientConfig`, and the producer and consumer configs as they move
/// onto `ConfigDef`).
impl CommonClientConfigs {
    /// Config key: `send.buffer.bytes`
    pub const SEND_BUFFER_CONFIG: &str = "send.buffer.bytes";
    /// The lowest `send.buffer.bytes`: `-1`, the OS default.
    pub const SEND_BUFFER_LOWER_BOUND: i32 = -1;
    /// Config key: `receive.buffer.bytes`
    pub const RECEIVE_BUFFER_CONFIG: &str = "receive.buffer.bytes";
    /// The lowest `receive.buffer.bytes`: `-1`, the OS default.
    pub const RECEIVE_BUFFER_LOWER_BOUND: i32 = -1;
    /// The default `retry.backoff.ms`.
    pub const DEFAULT_RETRY_BACKOFF_MS: i64 = 100;
    /// The default `retry.backoff.max.ms`.
    pub const DEFAULT_RETRY_BACKOFF_MAX_MS: i64 = 1000;
    /// Config key: `metrics.sample.window.ms`
    pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &str = "metrics.sample.window.ms";
    /// Config key: `metrics.num.samples`
    pub const METRICS_NUM_SAMPLES_CONFIG: &str = "metrics.num.samples";
    /// Config key: `metrics.recording.level`
    pub const METRICS_RECORDING_LEVEL_CONFIG: &str = "metrics.recording.level";
    /// The default `socket.connection.setup.timeout.ms`.
    pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS: i64 = 10 * 1000;
    /// The default `socket.connection.setup.timeout.max.ms`.
    pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS: i64 = 30 * 1000;
    /// The default `metadata.recovery.strategy`: Java's
    /// `MetadataRecoveryStrategy.REBOOTSTRAP.name`.
    pub const DEFAULT_METADATA_RECOVERY_STRATEGY: &str = "rebootstrap";
    /// The default `metadata.recovery.rebootstrap.trigger.ms`.
    pub const DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS: i64 = 300 * 1000;
}

impl CommonClientConfigs {
    /// Config key: `retry.backoff.ms`
    pub const RETRY_BACKOFF_MS_CONFIG: &str = "retry.backoff.ms";

    /// Config key: `retry.backoff.max.ms`
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &str = "retry.backoff.max.ms";

    /// Config key: `reconnect.backoff.ms`
    pub const RECONNECT_BACKOFF_MS_CONFIG: &str = "reconnect.backoff.ms";

    /// Config key: `reconnect.backoff.max.ms`
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &str = "reconnect.backoff.max.ms";

    /// Config key: `socket.connection.setup.timeout.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &str = "socket.connection.setup.timeout.ms";

    /// Config key: `socket.connection.setup.timeout.max.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &str = "socket.connection.setup.timeout.max.ms";

    /// Postprocess the configuration so that exponential backoff is disabled
    /// when reconnect backoff is explicitly configured but the maximum
    /// reconnect backoff is not explicitly configured.
    ///
    /// Returns the updates to apply to the configuration. With `log_warnings`
    /// false it logs nothing (a configuration copied for one more client,
    /// whose original logged it once, as Java builds the config once).
    #[doc(alias = "org.apache.kafka.clients.CommonClientConfigs#postProcessReconnectBackoffConfigs")]
    pub(crate) fn post_process_reconnect_backoff_configs(
        config: &AbstractConfig,
        parsed_values: &ConfigMap,
        log_warnings: bool,
    ) -> ConfigMap {
        let mut rval = HashMap::new();
        let original_config = config.originals_map();
        if !original_config.contains_key(Self::RECONNECT_BACKOFF_MAX_MS_CONFIG)
            && original_config.contains_key(Self::RECONNECT_BACKOFF_MS_CONFIG)
        {
            if log_warnings {
                log::warn!(
                    "Disabling exponential reconnect backoff because {} is set, but {} is not.",
                    Self::RECONNECT_BACKOFF_MS_CONFIG,
                    Self::RECONNECT_BACKOFF_MAX_MS_CONFIG
                );
            }
            rval.insert(
                Self::RECONNECT_BACKOFF_MAX_MS_CONFIG.to_string(),
                parsed_values.get(Self::RECONNECT_BACKOFF_MS_CONFIG).cloned().flatten(),
            );
        }
        rval
    }

    /// Log warning if the exponential backoff is disabled due to initial
    /// backoff value is greater than max backoff value.
    ///
    /// With `log_warnings` false it makes the same reads, so marks the same
    /// keys used, but logs nothing (a configuration copied for one more
    /// client, whose original logged it once, as Java builds the config once).
    ///
    /// # Errors
    ///
    /// The error of a getter: a key is not defined, not a `LONG`, or `null`.
    #[doc(alias = "org.apache.kafka.clients.CommonClientConfigs#warnDisablingExponentialBackoff")]
    pub(crate) fn warn_disabling_exponential_backoff(config: &AbstractConfig, log_warnings: bool) -> Result<(), Error> {
        let retry_backoff_ms = required_long(config, Self::RETRY_BACKOFF_MS_CONFIG)?;
        let retry_backoff_max_ms = required_long(config, Self::RETRY_BACKOFF_MAX_MS_CONFIG)?;
        if log_warnings && retry_backoff_ms > retry_backoff_max_ms {
            log::warn!(
                "Configuration '{}' with value '{}' is greater than configuration '{}' with value '{}'. A static \
                 backoff with value '{}' will be applied.",
                Self::RETRY_BACKOFF_MS_CONFIG,
                retry_backoff_ms,
                Self::RETRY_BACKOFF_MAX_MS_CONFIG,
                retry_backoff_max_ms,
                retry_backoff_max_ms
            );
        }

        let connection_setup_timeout_ms = required_long(config, Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)?;
        let connection_setup_timeout_max_ms =
            required_long(config, Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)?;
        if log_warnings && connection_setup_timeout_ms > connection_setup_timeout_max_ms {
            log::warn!(
                "Configuration '{}' with value '{}' is greater than configuration '{}' with value '{}'. A static \
                 connection setup timeout with value '{}' will be applied.",
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                connection_setup_timeout_ms,
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                connection_setup_timeout_max_ms,
                connection_setup_timeout_max_ms
            );
        }
        Ok(())
    }

    /// Java's `postValidateSaslMechanismConfig`: a SASL `security.protocol`
    /// needs a non-empty `sasl.mechanism`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] `Invalid value null for configuration sasl.mechanism:
    /// When the security.protocol configuration enables SASL, mechanism must
    /// be non-null and non-empty string.`, or the error of a getter.
    #[doc(alias = "org.apache.kafka.clients.CommonClientConfigs#postValidateSaslMechanismConfig")]
    pub(crate) fn post_validate_sasl_mechanism_config(config: &AbstractConfig) -> Result<(), Error> {
        let security_protocol = config
            .get_string(Self::SECURITY_PROTOCOL_CONFIG)?
            .and_then(SecurityProtocol::for_name);
        let client_sasl_mechanism = config.get_string(SaslConfigs::SASL_MECHANISM)?;
        if matches!(
            security_protocol,
            Some(SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl)
        ) && client_sasl_mechanism.is_none_or(str::is_empty)
        {
            return Err(Error::config_name_value_message(
                SaslConfigs::SASL_MECHANISM,
                "null",
                format!(
                    "When the {} configuration enables SASL, mechanism must be non-null and non-empty string.",
                    Self::SECURITY_PROTOCOL_CONFIG
                ),
            ));
        }
        Ok(())
    }

    /// Rust-only, no Java counterpart: a SASL `security.protocol` needs
    /// `sasl.mechanism` to be `PLAIN`, the one mechanism this client
    /// implements. It runs after [`Self::post_validate_sasl_mechanism_config`],
    /// so an empty mechanism keeps Java's error.
    ///
    /// The Java client supports every standard mechanism (`GSSAPI`, `PLAIN`,
    /// `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`) through
    /// `javax.security.sasl`; this client's authenticator only builds a PLAIN
    /// token. Another mechanism is therefore rejected when the configuration
    /// is built, before any credential is read or any byte is sent: accepted,
    /// it would be named in the handshake and then answered with a PLAIN
    /// token, which for SCRAM over `SASL_PLAINTEXT` sends the password in
    /// cleartext (DoD #7). Mechanism names are compared case-sensitively, as
    /// Java's are. Under any other `security.protocol` the key is inert, as in
    /// Java.
    ///
    /// # Errors
    ///
    /// [`Error::LocalUnsupportedOperation`] `SASL mechanism '<m>' is not
    /// supported by this client; only PLAIN is`, or the error of a getter.
    pub(crate) fn post_validate_supported_sasl_mechanism(config: &AbstractConfig) -> Result<(), Error> {
        let security_protocol = config
            .get_string(Self::SECURITY_PROTOCOL_CONFIG)?
            .and_then(SecurityProtocol::for_name);
        if !matches!(
            security_protocol,
            Some(SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl)
        ) {
            return Ok(());
        }
        match config.get_string(SaslConfigs::SASL_MECHANISM)? {
            Some(mechanism) if mechanism != SaslConfigs::PLAIN_MECHANISM => Err(Error::local_unsupported_operation(
                format!("SASL mechanism '{mechanism}' is not supported by this client; only PLAIN is"),
            )),
            _ => Ok(()),
        }
    }
}

/// Java's `long x = config.getLong(key)`. A `null` value fails Java's
/// unboxing with a `NullPointerException`, here an
/// [`Error::LocalIllegalState`]; the client definitions give every such key a
/// non-null default, so it cannot happen there.
fn required_long(config: &AbstractConfig, key: &str) -> Result<i64, Error> {
    config
        .get_long(key)?
        .ok_or_else(|| Error::local_illegal_state(format!("Configuration '{key}' has a null value")))
}

/// Translated from `CommonClientConfigsTest`. `testMetricsReporters` is not
/// translated: `metric.reporters` is class-valued (CLAUDE.md §2).
#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use super::*;
    use crate::common::config::ConfigDef;
    use crate::common::config::abstract_config::string_originals;
    use crate::common::config::config_def::{Importance, ParsedValue, Range, Type, ValidString};

    static TEST_CONFIG_DEF: LazyLock<ConfigDef> = LazyLock::new(|| {
        let names = SecurityProtocol::names();
        ConfigDef::new()
            .define_with_validator(
                CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG,
                Type::Long,
                50i64,
                Range::at_least(0i64),
                Importance::Low,
            )
            .define_with_validator(
                CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG,
                Type::Long,
                1000i64,
                Range::at_least(0i64),
                Importance::Low,
            )
            .define_with_validator(
                CommonClientConfigs::SECURITY_PROTOCOL_CONFIG,
                Type::String,
                CommonClientConfigs::DEFAULT_SECURITY_PROTOCOL,
                ValidString::r#in(&names),
                Importance::Medium,
            )
            .define(
                SaslConfigs::SASL_MECHANISM,
                Type::String,
                SaslConfigs::DEFAULT_SASL_MECHANISM,
                Importance::Medium,
            )
    });

    /// Java's `TestConfig`, whose `postProcessParsedConfig` validates the SASL
    /// mechanism and post-processes the reconnect backoff.
    fn test_config(props: ConfigMap) -> Result<AbstractConfig, Error> {
        AbstractConfig::with_hooks(
            &TEST_CONFIG_DEF,
            props,
            false,
            "TestConfig",
            |parsed| parsed,
            |config| {
                CommonClientConfigs::post_validate_sasl_mechanism_config(config)?;
                Ok(CommonClientConfigs::post_process_reconnect_backoff_configs(
                    config,
                    config.parsed_values(),
                    true,
                ))
            },
        )
    }

    fn props(entries: &[(&str, &str)]) -> ConfigMap {
        string_originals(&entries.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect())
    }

    #[test]
    fn test_exponential_backoff_defaults() {
        let default_conf = test_config(ConfigMap::new()).unwrap();
        assert_eq!(
            default_conf.get_long(CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG).unwrap(),
            Some(50)
        );
        assert_eq!(
            default_conf
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG)
                .unwrap(),
            Some(1000)
        );

        let both_set_config = test_config(props(&[
            (CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG, "123"),
            (CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG, "12345"),
        ]))
        .unwrap();
        assert_eq!(
            both_set_config
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG)
                .unwrap(),
            Some(123)
        );
        assert_eq!(
            both_set_config
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG)
                .unwrap(),
            Some(12345)
        );

        let reconnect_backoff_set_conf =
            test_config(props(&[(CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG, "123")])).unwrap();
        assert_eq!(
            reconnect_backoff_set_conf
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG)
                .unwrap(),
            Some(123)
        );
        assert_eq!(
            reconnect_backoff_set_conf
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG)
                .unwrap(),
            Some(123)
        );
    }

    #[test]
    fn test_invalid_sasl_mechanism() {
        let mut configs = props(&[(
            CommonClientConfigs::SECURITY_PROTOCOL_CONFIG,
            SecurityProtocol::SaslPlaintext.name(),
        )]);
        configs.insert(SaslConfigs::SASL_MECHANISM.to_string(), None);
        let ce = test_config(configs.clone()).unwrap_err();
        assert!(ce.is_config_error(), "{ce:?}");
        assert!(ce.to_string().contains(SaslConfigs::SASL_MECHANISM), "{ce}");
        assert_eq!(
            ce.to_string(),
            "ConfigError: Invalid value null for configuration sasl.mechanism: When the security.protocol \
             configuration enables SASL, mechanism must be non-null and non-empty string."
        );

        configs.insert(
            SaslConfigs::SASL_MECHANISM.to_string(),
            Some(ParsedValue::String(String::new())),
        );
        let ce = test_config(configs).unwrap_err();
        assert!(ce.to_string().contains(SaslConfigs::SASL_MECHANISM), "{ce}");
    }

    static BACKOFF_DEF: LazyLock<ConfigDef> = LazyLock::new(|| {
        ConfigDef::new()
            .define(
                CommonClientConfigs::RETRY_BACKOFF_MS_CONFIG,
                Type::Long,
                100i64,
                Importance::Low,
            )
            .define(
                CommonClientConfigs::RETRY_BACKOFF_MAX_MS_CONFIG,
                Type::Long,
                1000i64,
                Importance::Low,
            )
            .define(
                CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                Type::Long,
                10_000i64,
                Importance::Low,
            )
            .define(
                CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                Type::Long,
                30_000i64,
                Importance::Low,
            )
    });

    /// `warnDisablingExponentialBackoff` only logs, and reads (so marks used)
    /// the four keys.
    #[test]
    fn test_warn_disabling_exponential_backoff_marks_keys_used() {
        let config = AbstractConfig::new(
            &BACKOFF_DEF,
            props(&[
                (CommonClientConfigs::RETRY_BACKOFF_MS_CONFIG, "5000"),
                (CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "60000"),
            ]),
            false,
        )
        .unwrap();
        CommonClientConfigs::warn_disabling_exponential_backoff(&config, true).unwrap();
        assert!(config.unused().is_empty(), "{:?}", config.unused());
    }
}
