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

//! Configuration for the Kafka Consumer.
//!
//! Corresponds to `org.apache.kafka.clients.consumer.ConsumerConfig`: the keys
//! are defined on a `ConfigDef` in Java's order (`ConsumerConfig.java:440-745`)
//! and parsed and validated by it, `postProcessParsedConfig` runs Java's steps
//! (`ConsumerConfig.java:748-757`), and the configuration answers Java's
//! `AbstractConfig` getters.
//!
//! Per `consumer-threading.md` §20 only the KIP-848 `consumer` group protocol
//! is implemented. The default still matches Java (`classic`), so users opt
//! in explicitly; `KafkaConsumer::new` rejects `classic`.
//!
//! Not defined, and so reported by [`ConsumerConfig::unused`] and the
//! client's [`ConsumerConfig::log_unused`] line when set:
//!
//! - `partition.assignment.strategy`, `key.deserializer`, `value.deserializer`,
//!   `metric.reporters` and `interceptor.classes`: lists of classes, which a
//!   configuration never takes (CLAUDE.md §2), and the client-side assignors
//!   are out of scope (`consumer-threading.md` §20). Setting
//!   `partition.assignment.strategy` with `group.protocol=consumer` is still
//!   rejected, as Java rejects it.
//! - `security.providers`: Java's JCA security providers have no counterpart.
//! - `config.providers`: config providers are not translated, so the key is
//!   left undefined and a value set for it is reported as unused, rather than
//!   accepted and ignored.
//! - `enable.metrics.push`: KIP-714 client telemetry is not implemented, so
//!   the key is left undefined and a value set for it is reported as unused,
//!   rather than accepted and ignored.
//!
//! `AbstractConfig`'s `getPassword`, `typeOf`, `originals*()` and `values*()`
//! are not translated yet: each needs a public type the crate does not have
//! (`Password`, `ConfigDef.Type`, Java's `RecordingMap`). See `CHANGELOG.md`.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::LazyLock;
use std::sync::atomic::{self, AtomicI32};

use log::warn;

use crate::common::Error;
use crate::common::config::abstract_config::{AbstractConfig, ConfigMap, ValuesReader, string_originals};
use crate::common::config::config_def::{
    CaseInsensitiveValidString, ConfigDef, DefaultValue, Importance, LambdaValidator, NonEmptyString, ParsedValue,
    Range, Type, ValidList, ValidString, as_string, config_error,
};
use crate::common::config::{SaslConfigs, SslConfigs};
use crate::common::errors::InvalidConfigurationError;
use crate::common::requests::JoinGroupRequest;
use crate::common::security::auth::SecurityProtocol;
use crate::common::utils::Utils;
use crate::consumer::internals::{AutoOffsetResetStrategy, auto_offset_reset_strategy};
use crate::{ClientDnsLookup, CommonClientConfigs};

/// Process-wide counter for deriving a default `client.id`.
///
/// Corresponds to Java's `static AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE`
/// (`ConsumerConfig.java:418`), which starts at 1. Crate-level (not per-config)
/// to match Java's static scope, so successive consumers in one process get
/// distinct ids.
static CONSUMER_CLIENT_ID_SEQUENCE: AtomicI32 = AtomicI32::new(1);

/// Java's `ConsumerConfig.CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS`: the keys not
/// supported for the `CLASSIC` protocol.
const CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS: [&str; 3] = [
    ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG,
    ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
    ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG,
];

/// Java's `ConsumerConfig.CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS`: the keys
/// not supported for the `CONSUMER` protocol.
const CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS: [&str; 5] = [
    ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
    ConsumerConfig::HEARTBEAT_INTERVAL_MS_CONFIG,
    ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG,
    ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
    ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG,
];

/// Java's `ConsumerConfig.CONFIG`, built in its static initializer.
static CONFIG: LazyLock<ConfigDef> = LazyLock::new(|| {
    ConfigDef::new()
        .define_with_validator(
            ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG,
            Type::List,
            ConfigDef::NO_DEFAULT_VALUE,
            ValidList::any_non_duplicate_values(false, false),
            Importance::High,
        )
        .define_with_validator(
            ConsumerConfig::CLIENT_DNS_LOOKUP_CONFIG,
            Type::String,
            ClientDnsLookup::UseAllDnsIps.config_value(),
            ValidString::r#in(&[
                ClientDnsLookup::UseAllDnsIps.config_value(),
                ClientDnsLookup::ResolveCanonicalBootstrapServersOnly.config_value(),
            ]),
            Importance::Medium,
        )
        .define_with_validator(
            ConsumerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_BOOTSTRAP_RESOLVE_TIMEOUT_MS,
            Range::at_least(0_i64),
            Importance::High,
        )
        .define(
            ConsumerConfig::GROUP_ID_CONFIG,
            Type::String,
            DefaultValue::Null,
            Importance::High,
        )
        .define_with_validator(
            ConsumerConfig::GROUP_INSTANCE_ID_CONFIG,
            Type::String,
            DefaultValue::Null,
            NonEmptyString,
            Importance::Medium,
        )
        .define(ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG, Type::Int, 45000, Importance::High)
        .define(ConsumerConfig::HEARTBEAT_INTERVAL_MS_CONFIG, Type::Int, 3000, Importance::High)
        // `partition.assignment.strategy` is a list of classes (CLAUDE.md §2)
        // and the client-side assignors are out of scope: not defined.
        .define_with_validator(
            ConsumerConfig::METADATA_MAX_AGE_CONFIG,
            Type::Long,
            5 * 60 * 1000_i64,
            Range::at_least(0),
            Importance::Low,
        )
        .define(
            ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG,
            Type::Boolean,
            true,
            Importance::Medium,
        )
        .define_with_validator(
            ConsumerConfig::AUTO_COMMIT_INTERVAL_MS_CONFIG,
            Type::Int,
            5000,
            Range::at_least(0),
            Importance::Low,
        )
        .define(ConsumerConfig::CLIENT_ID_CONFIG, Type::String, "", Importance::Low)
        .define(
            ConsumerConfig::CLIENT_RACK_CONFIG,
            Type::String,
            ConsumerConfig::DEFAULT_CLIENT_RACK,
            Importance::Low,
        )
        .define_with_validator(
            ConsumerConfig::MAX_PARTITION_FETCH_BYTES_CONFIG,
            Type::Int,
            ConsumerConfig::DEFAULT_MAX_PARTITION_FETCH_BYTES,
            Range::at_least(0),
            Importance::High,
        )
        .define_with_validator(
            ConsumerConfig::SEND_BUFFER_CONFIG,
            Type::Int,
            128 * 1024,
            Range::at_least(CommonClientConfigs::SEND_BUFFER_LOWER_BOUND),
            Importance::Medium,
        )
        .define_with_validator(
            ConsumerConfig::RECEIVE_BUFFER_CONFIG,
            Type::Int,
            64 * 1024,
            Range::at_least(CommonClientConfigs::RECEIVE_BUFFER_LOWER_BOUND),
            Importance::Medium,
        )
        .define_with_validator(
            ConsumerConfig::FETCH_MIN_BYTES_CONFIG,
            Type::Int,
            ConsumerConfig::DEFAULT_FETCH_MIN_BYTES,
            Range::at_least(0),
            Importance::High,
        )
        .define_with_validator(
            ConsumerConfig::FETCH_MAX_BYTES_CONFIG,
            Type::Int,
            ConsumerConfig::DEFAULT_FETCH_MAX_BYTES,
            Range::at_least(0),
            Importance::Medium,
        )
        .define_with_validator(
            ConsumerConfig::FETCH_MAX_WAIT_MS_CONFIG,
            Type::Int,
            ConsumerConfig::DEFAULT_FETCH_MAX_WAIT_MS,
            Range::at_least(0),
            Importance::Low,
        )
        .define_with_validator(
            ConsumerConfig::RECONNECT_BACKOFF_MS_CONFIG,
            Type::Long,
            50_i64,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            ConsumerConfig::RECONNECT_BACKOFF_MAX_MS_CONFIG,
            Type::Long,
            1000_i64,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            ConsumerConfig::RETRY_BACKOFF_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_RETRY_BACKOFF_MS,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            ConsumerConfig::RETRY_BACKOFF_MAX_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_RETRY_BACKOFF_MAX_MS,
            Range::at_least(0_i64),
            Importance::Low,
        )
        // Java defines `enable.metrics.push` here (`ConsumerConfig.java:570-574`);
        // it is left undefined until KIP-714 client telemetry is implemented.
        .define_with_validator(
            ConsumerConfig::AUTO_OFFSET_RESET_CONFIG,
            Type::String,
            AutoOffsetResetStrategy::LATEST.name(),
            auto_offset_reset_strategy::Validator,
            Importance::Medium,
        )
        .define(ConsumerConfig::CHECK_CRCS_CONFIG, Type::Boolean, true, Importance::Low)
        .define_with_validator(
            ConsumerConfig::METRICS_SAMPLE_WINDOW_MS_CONFIG,
            Type::Long,
            30000_i64,
            Range::at_least(0),
            Importance::Low,
        )
        .define_with_validator(
            ConsumerConfig::METRICS_NUM_SAMPLES_CONFIG,
            Type::Int,
            2,
            Range::at_least(1),
            Importance::Low,
        )
        .define_with_validator(
            ConsumerConfig::METRICS_RECORDING_LEVEL_CONFIG,
            Type::String,
            "INFO",
            ValidString::r#in(&["INFO", "DEBUG", "TRACE"]),
            Importance::Low,
        )
        // `metric.reporters`, `key.deserializer` and `value.deserializer` are
        // classes (CLAUDE.md §2): not defined.
        .define_with_validator(
            ConsumerConfig::REQUEST_TIMEOUT_MS_CONFIG,
            Type::Int,
            30000,
            Range::at_least(0),
            Importance::Medium,
        )
        .define_with_validator(
            ConsumerConfig::DEFAULT_API_TIMEOUT_MS_CONFIG,
            Type::Int,
            60000,
            Range::at_least(0),
            Importance::Medium,
        )
        .define(
            ConsumerConfig::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
            Importance::Medium,
        )
        .define(
            ConsumerConfig::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
            Importance::Medium,
        )
        .define(
            ConsumerConfig::CONNECTIONS_MAX_IDLE_MS_CONFIG,
            Type::Long,
            9 * 60 * 1000_i64,
            Importance::Medium,
        )
        // `interceptor.classes` is a list of classes (CLAUDE.md §2): not defined.
        .define_with_validator(
            ConsumerConfig::MAX_POLL_RECORDS_CONFIG,
            Type::Int,
            ConsumerConfig::DEFAULT_MAX_POLL_RECORDS,
            Range::at_least(1),
            Importance::Medium,
        )
        .define_with_validator(
            ConsumerConfig::MAX_POLL_INTERVAL_MS_CONFIG,
            Type::Int,
            300000,
            Range::at_least(1),
            Importance::Medium,
        )
        .define(
            ConsumerConfig::EXCLUDE_INTERNAL_TOPICS_CONFIG,
            Type::Boolean,
            ConsumerConfig::DEFAULT_EXCLUDE_INTERNAL_TOPICS,
            Importance::Medium,
        )
        .define_internal(
            ConsumerConfig::THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED,
            Type::Boolean,
            false,
            Importance::Low,
        )
        .define_with_validator(
            ConsumerConfig::ISOLATION_LEVEL_CONFIG,
            Type::String,
            ConsumerConfig::DEFAULT_ISOLATION_LEVEL,
            ValidString::r#in(&["read_committed", "read_uncommitted"]),
            Importance::Medium,
        )
        .define(
            ConsumerConfig::ALLOW_AUTO_CREATE_TOPICS_CONFIG,
            Type::Boolean,
            ConsumerConfig::DEFAULT_ALLOW_AUTO_CREATE_TOPICS,
            Importance::Medium,
        )
        .define_with_validator(
            ConsumerConfig::GROUP_PROTOCOL_CONFIG,
            Type::String,
            ConsumerConfig::DEFAULT_GROUP_PROTOCOL,
            CaseInsensitiveValidString::r#in(&["CLASSIC", "CONSUMER"]),
            Importance::High,
        )
        .define(
            ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG,
            Type::String,
            DefaultValue::Null,
            Importance::Medium,
        )
        // security support; `security.providers` (JCA providers) is not defined.
        .define_with_validator(
            CommonClientConfigs::SECURITY_PROTOCOL_CONFIG,
            Type::String,
            CommonClientConfigs::DEFAULT_SECURITY_PROTOCOL,
            CaseInsensitiveValidString::r#in(&SecurityProtocol::names()),
            Importance::Medium,
        )
        .with_client_ssl_support()
        .with_client_sasl_support()
        .define_with_validator(
            CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG,
            Type::String,
            CommonClientConfigs::DEFAULT_METADATA_RECOVERY_STRATEGY,
            CaseInsensitiveValidString::r#in(&["NONE", "REBOOTSTRAP"]),
            Importance::Low,
        )
        .define_with_validator(
            CommonClientConfigs::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
            Range::at_least(0),
            Importance::Low,
        )
        .define(
            CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG,
            Type::Boolean,
            true,
            Importance::Low,
        )
        // Java validates the two share keys with `ShareAcknowledgementMode.Validator`
        // and `ShareAcquireMode.Validator`, translated below. The share consumer
        // is out of scope (`consumer-threading.md` §20), so a valid non-blank
        // value is then rejected under either group protocol by
        // `checkUnsupportedConfigsPostProcess`, which skips a blank value: the
        // validators are what reject a blank one, as in Java.
        .define_with_validator(
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
            Type::String,
            "implicit",
            share_acknowledgement_mode_validator(),
            Importance::Medium,
        )
        // Java's `DEFAULT_SHARE_ACQUIRE_MODE` is `ShareAcquireMode.BATCH_OPTIMIZED.name()`,
        // the enum constant's name, not its lower-case `toString()`.
        .define_with_validator(
            ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG,
            Type::String,
            "BATCH_OPTIMIZED",
            share_acquire_mode_validator(),
            Importance::Medium,
        )
    // Java ends the chain with `config.providers` (`ConsumerConfig.java`
    // 740-745); it is left undefined until config providers are translated.
});

/// Java's `ShareAcknowledgementMode.Validator`: the value must be one of
/// `implicit` or `explicit`, matched exactly (`ShareAcknowledgementMode.fromString`
/// checks it against `Utils.enumOptions`, the constants' lower-case
/// `toString()`).
fn share_acknowledgement_mode_validator() -> LambdaValidator {
    LambdaValidator::with(
        |name, value| {
            let acknowledgement_mode = as_string(value)?;
            if acknowledgement_mode.is_some_and(|mode| ["implicit", "explicit"].contains(&mode)) {
                return Ok(());
            }
            Err(config_error(
                name,
                value,
                format!(
                    "Invalid value `{}` for configuration {name}. The value must either be 'implicit' or 'explicit'.",
                    acknowledgement_mode.unwrap_or("null")
                ),
            ))
        },
        || "[implicit, explicit]".to_owned(),
    )
}

/// Java's `ShareAcquireMode.Validator`: the value must name one of the
/// `ShareAcquireMode` constants, case-insensitively (`ShareAcquireMode.of`
/// upper-cases it before `valueOf`).
fn share_acquire_mode_validator() -> LambdaValidator {
    LambdaValidator::with(
        |name, value| {
            let acquire_mode = as_string(value)?;
            if acquire_mode
                .is_some_and(|mode| ["BATCH_OPTIMIZED", "RECORD_LIMIT"].contains(&mode.to_uppercase().as_str()))
            {
                return Ok(());
            }
            Err(config_error(
                name,
                value,
                format!(
                    "Invalid value `{}` for configuration {name}. The value must either be 'batch_optimized' or 'record_limit'.",
                    acquire_mode.unwrap_or("null")
                ),
            ))
        },
        || "[batch_optimized, record_limit]".to_owned(),
    )
}

/// Configuration for the Kafka Consumer.
///
/// Documentation for the underlying keys can be found in the
/// [Kafka documentation](http://kafka.apache.org/documentation.html#consumerconfigs).
///
/// As in Java, the configuration is built from the property map by
/// [`ConsumerConfig::new`], which parses every key to its `ConfigDef` type,
/// applies its validator and runs Java's `postProcessParsedConfig`. Values are
/// read back with Java's `AbstractConfig` getters ([`Self::get_int`],
/// [`Self::get_string`], ...). Keys it does not define are accepted, as Java
/// accepts them; the consumer reports them, with the defined keys it does not
/// read, in one line when it is built ([`Self::log_unused`]).
///
/// Equality and hashing are Java's `equals` / `hashCode`: two configurations
/// are equal when they were built from equal properties.
///
/// Corresponds to `org.apache.kafka.clients.consumer.ConsumerConfig`.
// `Debug` is derived: every secret the typed fields hold is a `Password`
// inside `sasl_config` / `ssl_config`, which renders as `[hidden]`, and
// `AbstractConfig`'s `Debug` renders the properties as their key set only: the
// map also holds keys this client does not define, and Java never prints a raw
// original value.
#[derive(Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfig")]
pub struct ConsumerConfig {
    /// The originals, the parsed values and the keys used so far.
    pub(crate) config: AbstractConfig,
    /// The keys the typed fields below were read from, which the consumer
    /// marks used when it is built from this configuration (`ValuesReader`).
    pub(crate) read_keys: Vec<&'static str>,

    // --- Group ---
    /// `group.id` — the consumer group identifier. `None` means no group.
    pub(crate) group_id: Option<String>,
    /// `group.instance.id` — static membership identifier.
    pub(crate) group_instance_id: Option<String>,
    /// `group.protocol` — `"classic"` (Java default) or `"consumer"` (KIP-848).
    pub(crate) group_protocol: String,
    /// `group.remote.assignor` — name of the server-side assignor (KIP-848).
    pub(crate) group_remote_assignor: Option<String>,

    // --- Polling ---
    /// `max.poll.records` — the maximum number of records returned in a
    /// single `poll()` call.
    pub(crate) max_poll_records: i32,
    /// `max.poll.interval.ms` — the maximum interval between `poll()` calls.
    pub(crate) max_poll_interval_ms: i32,

    // --- Connection ---
    /// `bootstrap.servers` — initial connection list.
    pub(crate) bootstrap_servers: Vec<String>,
    /// `client.dns.lookup` — DNS lookup behavior.
    pub(crate) client_dns_lookup: ClientDnsLookup,
    /// `client.id` — the client identifier, derived by `new` when unset.
    pub(crate) client_id: String,
    /// `client.rack` — the rack identifier for rack-aware fetching.
    pub(crate) client_rack: String,

    // --- Commit ---
    /// `enable.auto.commit` — periodically commit offsets in the background.
    pub(crate) enable_auto_commit: bool,
    /// `auto.commit.interval.ms` — frequency of auto-commits.
    pub(crate) auto_commit_interval_ms: i32,

    // --- Offsets ---
    /// `auto.offset.reset` — the reset strategy when no offset is found.
    pub(crate) auto_offset_reset: String,

    // --- Fetching ---
    /// `fetch.min.bytes` — minimum bytes the server should return.
    pub(crate) fetch_min_bytes: i32,
    /// `fetch.max.bytes` — maximum bytes the server should return.
    pub(crate) fetch_max_bytes: i32,
    /// `fetch.max.wait.ms` — maximum wait time on the server.
    pub(crate) fetch_max_wait_ms: i32,
    /// `max.partition.fetch.bytes` — per-partition fetch size cap.
    pub(crate) max_partition_fetch_bytes: i32,
    /// `check.crcs` — automatically check CRC32 of consumed records.
    pub(crate) check_crcs: bool,

    // --- Buffers / Sockets ---
    /// `send.buffer.bytes` — TCP send buffer size.
    pub(crate) send_buffer_bytes: i32,
    /// `receive.buffer.bytes` — TCP receive buffer size.
    pub(crate) receive_buffer_bytes: i32,
    /// `socket.connection.setup.timeout.ms`
    pub(crate) socket_connection_setup_timeout_ms: i64,
    /// `socket.connection.setup.timeout.max.ms`
    pub(crate) socket_connection_setup_timeout_max_ms: i64,
    /// `connections.max.idle.ms`
    pub(crate) connections_max_idle_ms: i64,

    // --- Reconnect / Retry ---
    /// `reconnect.backoff.ms`
    pub(crate) reconnect_backoff_ms: i64,
    /// `reconnect.backoff.max.ms`
    pub(crate) reconnect_backoff_max_ms: i64,
    /// `retry.backoff.ms`
    pub(crate) retry_backoff_ms: i64,
    /// `retry.backoff.max.ms`
    pub(crate) retry_backoff_max_ms: i64,
    /// `bootstrap.resolve.timeout.ms` (KIP-909)
    pub(crate) bootstrap_resolve_timeout_ms: i64,

    // --- Timeouts ---
    /// `request.timeout.ms`
    pub(crate) request_timeout_ms: i32,
    /// `default.api.timeout.ms`
    pub(crate) default_api_timeout_ms: i32,

    // --- Metadata ---
    /// `metadata.max.age.ms`
    pub(crate) metadata_max_age_ms: i64,
    /// `metadata.recovery.strategy`
    pub(crate) metadata_recovery_strategy: String,
    /// `metadata.recovery.rebootstrap.trigger.ms`
    pub(crate) metadata_recovery_rebootstrap_trigger_ms: i64,
    /// `metadata.cluster.check.enable` (KIP-1242; `ConsumerConfig.java:723-727`)
    pub(crate) metadata_cluster_check_enable: bool,

    // --- Misc / Behavior ---
    /// `exclude.internal.topics`
    pub(crate) exclude_internal_topics: bool,
    /// `internal.throw.on.fetch.stable.offset.unsupported`
    pub(crate) throw_on_fetch_stable_offset_unsupported: bool,
    /// `isolation.level`
    pub(crate) isolation_level: String,
    /// `allow.auto.create.topics`
    pub(crate) allow_auto_create_topics: bool,

    // --- Metrics ---
    /// `metrics.sample.window.ms`
    pub(crate) metrics_sample_window_ms: i64,
    /// `metrics.num.samples`
    pub(crate) metrics_num_samples: i32,
    /// `metrics.recording.level`
    pub(crate) metrics_recording_level: String,

    // --- Security ---
    /// `security.protocol` - Protocol used to communicate with brokers.
    /// Default: `SecurityProtocol::Plaintext`.
    pub(crate) security_protocol: SecurityProtocol,

    /// SASL configuration (mechanism, JAAS config, credentials).
    pub(crate) sasl_config: SaslConfigs,

    /// SSL/TLS configuration.
    pub(crate) ssl_config: SslConfigs,
}

impl PartialEq for ConsumerConfig {
    /// Java's `AbstractConfig.equals`: the originals are equal.
    fn eq(&self, other: &Self) -> bool {
        self.config == other.config
    }
}

impl Eq for ConsumerConfig {}

impl Hash for ConsumerConfig {
    /// Java's `AbstractConfig.hashCode`, over the originals.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.config.hash(state);
    }
}

impl ConsumerConfig {
    /// A configuration holding every key's `ConfigDef` default, for the
    /// crate's own tests, which then write the fields they need.
    ///
    /// It is parsed from `bootstrap.servers` alone, without
    /// `postProcessParsedConfig`, so `client.id` stays empty and
    /// `enable.auto.commit` keeps its `true` default; `bootstrap_servers` is
    /// then emptied.
    ///
    /// Crate-private, and not `Default`: Java builds a configuration only
    /// through its constructor, and a public default would hand out one that
    /// skipped `new`'s validation, including the required `bootstrap.servers`.
    #[cfg(test)]
    pub(crate) fn defaults() -> Self {
        let props = HashMap::from([(Self::BOOTSTRAP_SERVERS_CONFIG.to_string(), "localhost:9092".to_string())]);
        let config =
            AbstractConfig::new(&CONFIG, string_originals(&props), false).expect("the ConfigDef defaults are valid");
        let mut defaults = Self::from_abstract(config).expect("the ConfigDef defaults are valid");
        defaults.bootstrap_servers = Vec::new();
        defaults
    }

    /// Reads the typed fields from the parsed values, recording the keys read
    /// in `read_keys`.
    fn from_abstract(config: AbstractConfig) -> Result<Self, Error> {
        let mut values = ValuesReader::new(&config);
        let bootstrap_servers = values.list(Self::BOOTSTRAP_SERVERS_CONFIG)?.to_vec();
        let client_dns_lookup = ClientDnsLookup::parse_config_value(values.string(Self::CLIENT_DNS_LOOKUP_CONFIG)?)?;
        let client_id = values.string(Self::CLIENT_ID_CONFIG)?.to_string();
        let client_rack = values.string(Self::CLIENT_RACK_CONFIG)?.to_string();
        // An empty `group.id` is kept, not coerced to `None`: Java's `ConfigDef`
        // does not coerce `""` to null, and
        // `AsyncKafkaConsumer.initializeGroupMetadata` rejects it
        // (`AsyncKafkaConsumer.java:839-844`). Coercing it here would turn
        // that configuration error into a groupless consumer.
        let group_id = values.string_or_null(Self::GROUP_ID_CONFIG)?.map(str::to_string);
        let group_instance_id = values.string_or_null(Self::GROUP_INSTANCE_ID_CONFIG)?.map(str::to_string);
        let group_protocol = values.string(Self::GROUP_PROTOCOL_CONFIG)?.to_string();
        let group_remote_assignor = values
            .string_or_null(Self::GROUP_REMOTE_ASSIGNOR_CONFIG)?
            .filter(|assignor| !assignor.is_empty())
            .map(str::to_string);
        let max_poll_records = values.int(Self::MAX_POLL_RECORDS_CONFIG)?;
        let max_poll_interval_ms = values.int(Self::MAX_POLL_INTERVAL_MS_CONFIG)?;
        let enable_auto_commit = values.boolean(Self::ENABLE_AUTO_COMMIT_CONFIG)?;
        let auto_commit_interval_ms = values.int(Self::AUTO_COMMIT_INTERVAL_MS_CONFIG)?;
        let auto_offset_reset = values.string(Self::AUTO_OFFSET_RESET_CONFIG)?.to_string();
        let fetch_min_bytes = values.int(Self::FETCH_MIN_BYTES_CONFIG)?;
        let fetch_max_bytes = values.int(Self::FETCH_MAX_BYTES_CONFIG)?;
        let fetch_max_wait_ms = values.int(Self::FETCH_MAX_WAIT_MS_CONFIG)?;
        let max_partition_fetch_bytes = values.int(Self::MAX_PARTITION_FETCH_BYTES_CONFIG)?;
        let check_crcs = values.boolean(Self::CHECK_CRCS_CONFIG)?;
        let send_buffer_bytes = values.int(Self::SEND_BUFFER_CONFIG)?;
        let receive_buffer_bytes = values.int(Self::RECEIVE_BUFFER_CONFIG)?;
        let socket_connection_setup_timeout_ms = values.long(Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)?;
        let socket_connection_setup_timeout_max_ms =
            values.long(Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)?;
        let connections_max_idle_ms = values.long(Self::CONNECTIONS_MAX_IDLE_MS_CONFIG)?;
        let reconnect_backoff_ms = values.long(Self::RECONNECT_BACKOFF_MS_CONFIG)?;
        let reconnect_backoff_max_ms = values.long(Self::RECONNECT_BACKOFF_MAX_MS_CONFIG)?;
        let retry_backoff_ms = values.long(Self::RETRY_BACKOFF_MS_CONFIG)?;
        let retry_backoff_max_ms = values.long(Self::RETRY_BACKOFF_MAX_MS_CONFIG)?;
        let bootstrap_resolve_timeout_ms = values.long(Self::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG)?;
        let request_timeout_ms = values.int(Self::REQUEST_TIMEOUT_MS_CONFIG)?;
        let default_api_timeout_ms = values.int(Self::DEFAULT_API_TIMEOUT_MS_CONFIG)?;
        let metadata_max_age_ms = values.long(Self::METADATA_MAX_AGE_CONFIG)?;
        let metadata_recovery_strategy = values
            .string(CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG)?
            .to_string();
        let metadata_recovery_rebootstrap_trigger_ms =
            values.long(CommonClientConfigs::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG)?;
        let metadata_cluster_check_enable =
            values.boolean(CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG)?;
        let exclude_internal_topics = values.boolean(Self::EXCLUDE_INTERNAL_TOPICS_CONFIG)?;
        let throw_on_fetch_stable_offset_unsupported =
            values.boolean(Self::THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED)?;
        let isolation_level = values.string(Self::ISOLATION_LEVEL_CONFIG)?.to_string();
        let allow_auto_create_topics = values.boolean(Self::ALLOW_AUTO_CREATE_TOPICS_CONFIG)?;
        let metrics_sample_window_ms = values.long(Self::METRICS_SAMPLE_WINDOW_MS_CONFIG)?;
        let metrics_num_samples = values.int(Self::METRICS_NUM_SAMPLES_CONFIG)?;
        let metrics_recording_level = values.string(Self::METRICS_RECORDING_LEVEL_CONFIG)?.to_string();
        let protocol = values.string(CommonClientConfigs::SECURITY_PROTOCOL_CONFIG)?;
        // The validator accepted `protocol` case-insensitively.
        let security_protocol = SecurityProtocol::for_name(protocol)
            .ok_or_else(|| Error::local_illegal_state(format!("Unknown security protocol {protocol}")))?;

        // The `ssl.*` and `sasl.*` keys are read by Java's channel builder only
        // for a protocol that uses them (`ChannelBuilders.clientChannelBuilder`),
        // so only then are they marked used.
        let mut ssl_values = ValuesReader::new(&config);
        let ssl_config = SslConfigs::from_values(&mut ssl_values)?;
        if matches!(security_protocol, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl) {
            values.append(ssl_values);
        }
        let mut sasl_values = ValuesReader::new(&config);
        let sasl_config = SaslConfigs::from_values(&mut sasl_values)?;
        if matches!(security_protocol, SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl) {
            values.append(sasl_values);
        }
        let read_keys = values.into_read_keys();

        Ok(Self {
            config,
            read_keys,
            group_id,
            group_instance_id,
            group_protocol,
            group_remote_assignor,
            max_poll_records,
            max_poll_interval_ms,
            bootstrap_servers,
            client_dns_lookup,
            client_id,
            client_rack,
            enable_auto_commit,
            auto_commit_interval_ms,
            auto_offset_reset,
            fetch_min_bytes,
            fetch_max_bytes,
            fetch_max_wait_ms,
            max_partition_fetch_bytes,
            check_crcs,
            send_buffer_bytes,
            receive_buffer_bytes,
            socket_connection_setup_timeout_ms,
            socket_connection_setup_timeout_max_ms,
            connections_max_idle_ms,
            reconnect_backoff_ms,
            reconnect_backoff_max_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            bootstrap_resolve_timeout_ms,
            request_timeout_ms,
            default_api_timeout_ms,
            metadata_max_age_ms,
            metadata_recovery_strategy,
            metadata_recovery_rebootstrap_trigger_ms,
            metadata_cluster_check_enable,
            exclude_internal_topics,
            throw_on_fetch_stable_offset_unsupported,
            isolation_level,
            allow_auto_create_topics,
            metrics_sample_window_ms,
            metrics_num_samples,
            metrics_recording_level,
            security_protocol,
            sasl_config,
            ssl_config,
        })
    }
}

impl ConsumerConfig {
    // --- Config key constants (matching Java string keys exactly) ---

    /// Config key: `group.id`.
    pub const GROUP_ID_CONFIG: &'static str = "group.id";
    /// Config key: `group.instance.id`.
    pub const GROUP_INSTANCE_ID_CONFIG: &'static str = "group.instance.id";
    /// Config key: `group.protocol`.
    pub const GROUP_PROTOCOL_CONFIG: &'static str = "group.protocol";
    /// Default value of `group.protocol`. Matches Java's `DEFAULT_GROUP_PROTOCOL`.
    pub const DEFAULT_GROUP_PROTOCOL: &'static str = "classic";
    /// Config key: `group.remote.assignor`.
    pub const GROUP_REMOTE_ASSIGNOR_CONFIG: &'static str = "group.remote.assignor";

    /// Config key: `max.poll.records`.
    pub const MAX_POLL_RECORDS_CONFIG: &'static str = "max.poll.records";
    /// Default value of `max.poll.records`.
    pub const DEFAULT_MAX_POLL_RECORDS: i32 = 500;
    /// Config key: `max.poll.interval.ms`.
    pub const MAX_POLL_INTERVAL_MS_CONFIG: &'static str = "max.poll.interval.ms";
    /// Config key: `session.timeout.ms`.
    pub const SESSION_TIMEOUT_MS_CONFIG: &'static str = "session.timeout.ms";
    /// Config key: `heartbeat.interval.ms`.
    pub const HEARTBEAT_INTERVAL_MS_CONFIG: &'static str = "heartbeat.interval.ms";

    /// Config key: `bootstrap.servers`.
    pub const BOOTSTRAP_SERVERS_CONFIG: &'static str = "bootstrap.servers";
    /// Config key: `client.dns.lookup`. Java's `ConsumerConfig.java` declares it as its
    /// own public alias of `CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG`.
    pub const CLIENT_DNS_LOOKUP_CONFIG: &'static str = CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG;
    /// Config key: `client.id`.
    pub const CLIENT_ID_CONFIG: &'static str = "client.id";
    /// Config key: `client.rack`.
    pub const CLIENT_RACK_CONFIG: &'static str = crate::CommonClientConfigs::CLIENT_RACK_CONFIG;
    /// Default value of `client.rack`: no rack (`ConsumerConfig.java:267`).
    pub const DEFAULT_CLIENT_RACK: &'static str = crate::CommonClientConfigs::DEFAULT_CLIENT_RACK;

    /// Config key: `enable.auto.commit`.
    pub const ENABLE_AUTO_COMMIT_CONFIG: &'static str = "enable.auto.commit";
    /// Config key: `auto.commit.interval.ms`.
    pub const AUTO_COMMIT_INTERVAL_MS_CONFIG: &'static str = "auto.commit.interval.ms";

    /// Config key: `partition.assignment.strategy`.
    pub const PARTITION_ASSIGNMENT_STRATEGY_CONFIG: &'static str = "partition.assignment.strategy";
    /// Config key: `auto.offset.reset`.
    ///
    /// What to do when there is no initial offset in Kafka or if the current
    /// offset does not exist any more on the server (e.g. because that data has
    /// been deleted):
    ///
    /// * `earliest`: automatically reset the offset to the earliest offset
    /// * `latest`: automatically reset the offset to the latest offset
    /// * `by_duration:<duration>`: automatically reset the offset to a
    ///   configured `<duration>` from the current timestamp. `<duration>` must be
    ///   specified in ISO8601 format (`PnDTnHnMn.nS`). Negative duration is not
    ///   allowed.
    /// * `none`: return an error to the consumer if no previous offset is found
    ///   for the consumer's group
    /// * anything else: return an error to the consumer.
    ///
    /// Note that increasing a topic's partition count while this config is set
    /// to `latest` may cause silent message loss: producers may begin appending
    /// records to a newly created partition before the consumer discovers it,
    /// and `latest` resets the position to the log end offset, skipping any
    /// records produced during that discovery gap.
    ///
    /// To avoid this, prefer `by_duration:<duration>`. When a partition has no
    /// committed offset, `by_duration` determines the starting position by
    /// issuing a `ListOffsets` lookup for `now() - duration`. If the target
    /// timestamp is earlier than the partition's creation time, the lookup
    /// returns the partition's start offset, ensuring that records produced
    /// during the discovery window are still consumed. Size the duration to
    /// cover the worst-case partition-discovery latency for the group protocol
    /// in use:
    ///
    /// * With the `consumer` group protocol (KIP-848), newly assigned
    ///   partitions are pushed on the next group heartbeat, so a value at least
    ///   as large as `group.consumer.heartbeat.interval.ms` (server default
    ///   5000 ms) is sufficient, for example `by_duration:PT5S`.
    /// * With the `classic` group protocol, new partitions are discovered
    ///   through periodic metadata refresh and a subsequent rebalance, so the
    ///   duration must exceed `metadata.max.age.ms` (client default 300000 ms)
    ///   plus the rebalance time, for example `by_duration:PT6M`.
    ///
    /// Consumers with a valid committed offset are unaffected. The reset
    /// applies only to partitions whose offset is missing or out of range, so
    /// `by_duration` does not force existing consumers to replay historical
    /// data on restart.
    ///
    /// (Text of Java's `ConsumerConfig.AUTO_OFFSET_RESET_DOC`,
    /// `ConsumerConfig.java:183-206`, as rewritten by KAFKA-20539. This client
    /// implements only the `consumer` group protocol; the `classic` item is kept
    /// because the setting itself is protocol-independent.)
    pub const AUTO_OFFSET_RESET_CONFIG: &'static str = "auto.offset.reset";

    /// Config key: `fetch.min.bytes`.
    pub const FETCH_MIN_BYTES_CONFIG: &'static str = "fetch.min.bytes";
    /// Default value of `fetch.min.bytes`.
    pub const DEFAULT_FETCH_MIN_BYTES: i32 = 1;
    /// Config key: `fetch.max.bytes`.
    pub const FETCH_MAX_BYTES_CONFIG: &'static str = "fetch.max.bytes";
    /// Default value of `fetch.max.bytes`.
    pub const DEFAULT_FETCH_MAX_BYTES: i32 = 50 * 1024 * 1024;
    /// Config key: `fetch.max.wait.ms`.
    pub const FETCH_MAX_WAIT_MS_CONFIG: &'static str = "fetch.max.wait.ms";
    /// Default value of `fetch.max.wait.ms`.
    pub const DEFAULT_FETCH_MAX_WAIT_MS: i32 = 500;
    /// Config key: `max.partition.fetch.bytes`.
    pub const MAX_PARTITION_FETCH_BYTES_CONFIG: &'static str = "max.partition.fetch.bytes";
    /// Default value of `max.partition.fetch.bytes`.
    pub const DEFAULT_MAX_PARTITION_FETCH_BYTES: i32 = 1024 * 1024;
    /// Config key: `check.crcs`.
    pub const CHECK_CRCS_CONFIG: &'static str = "check.crcs";

    /// Config key: `send.buffer.bytes`.
    pub const SEND_BUFFER_CONFIG: &'static str = "send.buffer.bytes";
    /// Config key: `receive.buffer.bytes`.
    pub const RECEIVE_BUFFER_CONFIG: &'static str = "receive.buffer.bytes";
    /// Config key: `socket.connection.setup.timeout.ms`.
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &'static str = "socket.connection.setup.timeout.ms";
    /// Config key: `socket.connection.setup.timeout.max.ms`.
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &'static str = "socket.connection.setup.timeout.max.ms";
    /// Config key: `connections.max.idle.ms`.
    pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &'static str = "connections.max.idle.ms";

    /// Config key: `reconnect.backoff.ms`.
    pub const RECONNECT_BACKOFF_MS_CONFIG: &'static str = "reconnect.backoff.ms";
    /// Config key: `reconnect.backoff.max.ms`.
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &'static str = "reconnect.backoff.max.ms";
    /// Config key: `retry.backoff.ms`.
    pub const RETRY_BACKOFF_MS_CONFIG: &'static str = "retry.backoff.ms";
    /// Config key: `retry.backoff.max.ms`.
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &'static str = "retry.backoff.max.ms";
    /// Config key: `bootstrap.resolve.timeout.ms` (KIP-909): selects the
    /// client's bootstrap DNS resolution mode. `0` (the default) resolves
    /// `bootstrap.servers` synchronously when the consumer is created, and a
    /// resolution failure fails creation with a config error. A positive value
    /// resolves asynchronously, retrying for at most this long before every
    /// subsequent call fails with an unrecoverable
    /// [`BootstrapResolutionError`](crate::common::errors::BootstrapResolutionError);
    /// the consumer must then be closed and re-created. Setting a positive
    /// value enables an evolving feature whose compatibility may be broken in a
    /// minor release.
    pub const BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG;

    /// Config key: `request.timeout.ms`.
    pub const REQUEST_TIMEOUT_MS_CONFIG: &'static str = "request.timeout.ms";
    /// Config key: `default.api.timeout.ms`.
    pub const DEFAULT_API_TIMEOUT_MS_CONFIG: &'static str = "default.api.timeout.ms";

    /// Config key: `metadata.max.age.ms`.
    pub const METADATA_MAX_AGE_CONFIG: &'static str = "metadata.max.age.ms";

    /// Config key: `exclude.internal.topics`.
    pub const EXCLUDE_INTERNAL_TOPICS_CONFIG: &'static str = "exclude.internal.topics";
    /// Default value of `exclude.internal.topics`.
    pub const DEFAULT_EXCLUDE_INTERNAL_TOPICS: bool = true;
    /// Config key: `internal.throw.on.fetch.stable.offset.unsupported`.
    ///
    /// Package-private in Java (`ConsumerConfig.java:370`), like every
    /// `internal.*` key: Java may remove it in a minor release.
    pub(crate) const THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED: &'static str =
        "internal.throw.on.fetch.stable.offset.unsupported";
    /// Config key: `isolation.level`.
    pub const ISOLATION_LEVEL_CONFIG: &'static str = "isolation.level";
    /// Default for `isolation.level`: `read_committed`, so a consumer returns
    /// only the transactional records that were committed.
    ///
    /// This deviates from Java, whose `DEFAULT_ISOLATION_LEVEL` is
    /// `read_uncommitted` (`ConsumerConfig.java:382`); `read_committed` is
    /// librdkafka's default, which users of the other Confluent clients
    /// expect. Set `isolation.level=read_uncommitted` for Java's behaviour.
    pub const DEFAULT_ISOLATION_LEVEL: &'static str = "read_committed";
    /// Config key: `allow.auto.create.topics`.
    pub const ALLOW_AUTO_CREATE_TOPICS_CONFIG: &'static str = "allow.auto.create.topics";
    /// Default value of `allow.auto.create.topics`.
    pub const DEFAULT_ALLOW_AUTO_CREATE_TOPICS: bool = true;

    /// Config key: `metrics.sample.window.ms`.
    pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &'static str = "metrics.sample.window.ms";
    /// Config key: `metrics.num.samples`.
    pub const METRICS_NUM_SAMPLES_CONFIG: &'static str = "metrics.num.samples";
    /// Config key: `metrics.recording.level`.
    pub const METRICS_RECORDING_LEVEL_CONFIG: &'static str = "metrics.recording.level";

    /// Config key: `share.acknowledgement.mode`.
    pub const SHARE_ACKNOWLEDGEMENT_MODE_CONFIG: &'static str = "share.acknowledgement.mode";
    /// Config key: `share.acquire.mode`.
    pub const SHARE_ACQUIRE_MODE_CONFIG: &'static str = "share.acquire.mode";

    /// Creates a config from a property map: Java's `ConsumerConfig(Map)`.
    ///
    /// Every value is parsed to its `ConfigDef` type and checked by its
    /// validator, then Java's `postProcessParsedConfig`
    /// (`ConsumerConfig.java:748-757`) runs: `postValidateSaslMechanismConfig`,
    /// `warnDisablingExponentialBackoff`, `postProcessReconnectBackoffConfigs`,
    /// `maybeOverrideClientId` (an unset or empty `client.id` becomes
    /// `consumer-<group.id>-<group.instance.id or a process-wide sequence>`),
    /// `maybeOverrideEnableAutoCommit` (`enable.auto.commit` defaults to
    /// `false` without a `group.id`), `checkUnsupportedConfigsPostProcess` and
    /// the `connections.max.idle.ms` warning.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when a value fails to parse or validate, with
    ///   `ConfigDef`'s message; when `bootstrap.servers` is missing; when a SASL
    ///   `security.protocol` has an empty `sasl.mechanism`; when a key the
    ///   `group.protocol` does not support is set (`group.remote.assignor`,
    ///   `share.acknowledgement.mode` or `share.acquire.mode` with `classic`;
    ///   `partition.assignment.strategy`, `heartbeat.interval.ms`,
    ///   `session.timeout.ms` or the two share keys with `consumer`).
    /// - [`Error::InvalidConfiguration`] when `enable.auto.commit` is `true`
    ///   without a `group.id`.
    /// - The error of `JoinGroupRequest.validateGroupInstanceId` for an invalid
    ///   `group.instance.id` when `client.id` is derived from it.
    /// - A
    ///   [`LocalUnsupportedOperationError`](crate::common::LocalUnsupportedOperationError)
    ///   ([`Error::local_unsupported_operation`]) when a SASL
    ///   `security.protocol` has a `sasl.mechanism` other than `PLAIN`: Java
    ///   accepts any mechanism name, but this client implements only PLAIN,
    ///   and a client built with another mechanism would fail only later, at
    ///   authentication.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfig#ConsumerConfig(Map)")]
    pub fn new(props: &HashMap<String, String>) -> Result<Self, Error> {
        Self::build(string_originals(props), None)
    }

    /// The configuration of one more client given this one: the same
    /// properties and this configuration's resolved `client.id`, with its
    /// own record of the keys used, as [`Self::new`] leaves it.
    ///
    /// Java hands each client the `ConsumerConfig` it is given, which took
    /// its one number from the `client.id` sequence when it was built
    /// (`AsyncKafkaConsumer.java:476` reads it), so clients given one
    /// configuration share its id. Building the configuration again would
    /// take a second number; this copy does not. It is not `Clone`
    /// (CLAUDE.md §3).
    ///
    /// Java's client logs nothing about the configuration it is given, so the
    /// copy logs none of the warnings [`Self::new`] logs: it makes the same
    /// parse and the same reads, marking the same keys used and failing the
    /// same way, but logs neither the parse's warning that a list's duplicate
    /// values are removed, nor the post-processing's warnings (the reconnect
    /// and retry backoff, the connection setup timeout,
    /// `connections.max.idle.ms` below `max.poll.interval.ms`). The client given the copy logs its
    /// one unused-keys line
    /// ([`Self::log_unused`]), as Java's client constructor does.
    ///
    /// # Errors
    ///
    /// Fails as [`Self::new`] does; this configuration's properties already
    /// passed it, so it does not in practice.
    #[cfg_attr(
        not(any(feature = "ffi", test)),
        expect(dead_code, reason = "only the C config handle copies a configuration")
    )]
    pub(crate) fn copy_for_client(&self) -> Result<Self, Error> {
        Self::build(self.config.originals_map().clone(), Some(&self.client_id))
    }

    /// Java's `ConsumerConfig(Map)` without its logging: parses `originals`
    /// and post-processes them, `resolved_client_id` standing in for the
    /// sequence number `client.id` would take. With a `resolved_client_id`
    /// (a copy) nothing is logged: not the parse's duplicate-values warning,
    /// not the post-processing's lines.
    fn build(originals: ConfigMap, resolved_client_id: Option<&str>) -> Result<Self, Error> {
        let log_warnings = resolved_client_id.is_none();
        let config = AbstractConfig::with_hooks_and_log_warnings(
            &CONFIG,
            originals,
            false,
            log_warnings,
            "ConsumerConfig",
            |originals| originals,
            |config| Self::post_process_parsed_config(config, resolved_client_id),
        )?;
        Self::from_abstract(config)
    }

    /// Java's `ConsumerConfig.postProcessParsedConfig`: the values that
    /// replace the parsed ones. A `resolved_client_id` is the id a derived
    /// `client.id` takes, in place of a new one; it marks a copy for one more
    /// client ([`Self::copy_for_client`]), whose reads are the same but which
    /// logs none of the warnings: its original logged them once.
    fn post_process_parsed_config(
        config: &AbstractConfig,
        resolved_client_id: Option<&str>,
    ) -> Result<ConfigMap, Error> {
        let log_warnings = resolved_client_id.is_none();
        CommonClientConfigs::post_validate_sasl_mechanism_config(config)?;
        CommonClientConfigs::post_validate_supported_sasl_mechanism(config)?;
        CommonClientConfigs::warn_disabling_exponential_backoff(config, log_warnings)?;
        let mut refined_configs =
            CommonClientConfigs::post_process_reconnect_backoff_configs(config, config.parsed_values(), log_warnings);
        Self::maybe_override_client_id_with_configs(config, &mut refined_configs, resolved_client_id)?;
        Self::maybe_override_enable_auto_commit(config, &mut refined_configs)?;
        Self::check_unsupported_configs_post_process(config)?;
        Self::warn_if_connections_max_idle_ms_lower_than_max_poll_interval_ms(config, log_warnings)?;
        Ok(refined_configs)
    }

    /// Java's `warnIfConnectionsMaxIdleMsLowerThanMaxPollIntervalMs`. With
    /// `log_warnings` false it makes the same reads but logs nothing.
    fn warn_if_connections_max_idle_ms_lower_than_max_poll_interval_ms(
        config: &AbstractConfig,
        log_warnings: bool,
    ) -> Result<(), Error> {
        let group_protocol = config.get_string(Self::GROUP_PROTOCOL_CONFIG)?;
        if !group_protocol.is_some_and(|protocol| protocol.eq_ignore_ascii_case("CLASSIC")) {
            return Ok(());
        }
        let connections_max_idle_ms = config.get_long(Self::CONNECTIONS_MAX_IDLE_MS_CONFIG)?.unwrap_or_default();
        let max_poll_interval_ms = config.get_int(Self::MAX_POLL_INTERVAL_MS_CONFIG)?.unwrap_or_default();
        if log_warnings && connections_max_idle_ms >= 0 && connections_max_idle_ms < i64::from(max_poll_interval_ms) {
            warn!(
                "Configuration '{}' with value '{}' is lower than configuration '{}' with value '{}'. This may cause \
                 the connection to the group coordinator to be closed during an ongoing rebalance, which can \
                 prolong or disrupt group rejoin.",
                Self::CONNECTIONS_MAX_IDLE_MS_CONFIG,
                connections_max_idle_ms,
                Self::MAX_POLL_INTERVAL_MS_CONFIG,
                max_poll_interval_ms
            );
        }
        Ok(())
    }

    /// Java's private `maybeOverrideClientId(Map)`: when `client.id` is unset
    /// or empty, derives `consumer-<group.id>-<group.instance.id>` for a static
    /// member, otherwise `consumer-<group.id>-<n>` from the process-wide
    /// sequence, and puts it in `configs`.
    ///
    /// Java formats a missing `group.id` with `String.format("%s", null)`,
    /// which yields the literal `null`; that is preserved (`consumer-null-1`) so
    /// the wire `client_id` matches a Java consumer with the same
    /// configuration.
    ///
    /// A `resolved_client_id` (the id of the configuration this one copies)
    /// is put in `configs` instead of a derived one, after the same reads and
    /// checks.
    fn maybe_override_client_id_with_configs(
        config: &AbstractConfig,
        configs: &mut ConfigMap,
        resolved_client_id: Option<&str>,
    ) -> Result<(), Error> {
        let client_id = config.get_string(Self::CLIENT_ID_CONFIG)?;
        if client_id.is_none_or(str::is_empty) {
            let group_id = config.get_string(Self::GROUP_ID_CONFIG)?;
            let group_instance_id = config.get_string(Self::GROUP_INSTANCE_ID_CONFIG)?;
            if let Some(group_instance_id) = group_instance_id {
                JoinGroupRequest::validate_group_instance_id(group_instance_id)?;
            }
            let generated_client_id = match resolved_client_id {
                Some(resolved_client_id) => resolved_client_id.to_string(),
                None => {
                    let group_instance_id_part = match group_instance_id {
                        Some(group_instance_id) => group_instance_id.to_string(),
                        None => CONSUMER_CLIENT_ID_SEQUENCE.fetch_add(1, atomic::Ordering::Relaxed).to_string(),
                    };
                    format!("consumer-{}-{group_instance_id_part}", group_id.unwrap_or("null"))
                },
            };
            configs.insert(
                Self::CLIENT_ID_CONFIG.to_string(),
                Some(ParsedValue::String(generated_client_id)),
            );
        }
        Ok(())
    }

    /// Java's `maybeOverrideEnableAutoCommit`: without a `group.id`,
    /// `enable.auto.commit` becomes `false` unless it was set, and setting it
    /// to `true` is an error.
    fn maybe_override_enable_auto_commit(config: &AbstractConfig, configs: &mut ConfigMap) -> Result<(), Error> {
        let group_id = config.get_string(Self::GROUP_ID_CONFIG)?;
        let auto_commit_set = config.originals_map().contains_key(Self::ENABLE_AUTO_COMMIT_CONFIG);
        let enable_auto_commit = if auto_commit_set {
            config.get_boolean(Self::ENABLE_AUTO_COMMIT_CONFIG)?.unwrap_or_default()
        } else {
            false
        };
        if group_id.is_none() {
            // overwrite in case of default group id where the config is not explicitly provided
            if !auto_commit_set {
                configs.insert(Self::ENABLE_AUTO_COMMIT_CONFIG.to_string(), Some(ParsedValue::Boolean(false)));
            } else if enable_auto_commit {
                return Err(Error::InvalidConfiguration(InvalidConfigurationError::new(format!(
                    "{} cannot be set to true when default group id (null) is used.",
                    Self::ENABLE_AUTO_COMMIT_CONFIG
                ))));
            }
        }
        Ok(())
    }

    /// Java's `checkUnsupportedConfigsPostProcess`: the keys the configured
    /// `group.protocol` does not support must not be set.
    fn check_unsupported_configs_post_process(config: &AbstractConfig) -> Result<(), Error> {
        let group_protocol = config.get_string(Self::GROUP_PROTOCOL_CONFIG)?.unwrap_or_default();
        if group_protocol.eq_ignore_ascii_case("CLASSIC") {
            Self::check_unsupported_configs(config, "CLASSIC", &CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS)
        } else if group_protocol.eq_ignore_ascii_case("CONSUMER") {
            Self::check_unsupported_configs(config, "CONSUMER", &CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS)
        } else {
            Ok(())
        }
    }

    /// Java's private `checkUnsupportedConfigsPostProcess(GroupProtocol, List)`.
    fn check_unsupported_configs(
        config: &AbstractConfig,
        group_protocol: &str,
        unsupported_configs: &[&str],
    ) -> Result<(), Error> {
        let mut invalid_configs = Vec::new();
        for &config_name in unsupported_configs {
            // Java reads the key through `originals()`, a `RecordingMap`
            // whose `get` marks it used.
            config.ignore(config_name);
            if let Some(Some(value)) = config.originals_map().get(config_name)
                && !Utils::is_blank(Some(&value.to_string()))
            {
                invalid_configs.push(config_name);
            }
        }
        if invalid_configs.is_empty() {
            Ok(())
        } else {
            Err(Error::config_message(format!(
                "{} cannot be set when {}={group_protocol}",
                invalid_configs.join(", "),
                Self::GROUP_PROTOCOL_CONFIG
            )))
        }
    }

    /// The `SHORT` configuration `key`, `None` for Java's `null`. Reading a
    /// key marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] with Java's `Unknown configuration '<key>'` when
    ///   `key` is not a configuration this class defines.
    /// - [`Error::LocalIllegalArgument`] when `key` is a configuration of
    ///   another type, where Java's cast fails with a `ClassCastException`.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getShort")]
    pub fn get_short(&self, key: &str) -> Result<Option<i16>, Error> {
        self.config.get_short(key)
    }

    /// The `INT` configuration `key`, `None` for Java's `null`. Reading a key
    /// marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getInt")]
    pub fn get_int(&self, key: &str) -> Result<Option<i32>, Error> {
        self.config.get_int(key)
    }

    /// The `LONG` configuration `key`, `None` for Java's `null`. Reading a key
    /// marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getLong")]
    pub fn get_long(&self, key: &str) -> Result<Option<i64>, Error> {
        self.config.get_long(key)
    }

    /// The `DOUBLE` configuration `key`, `None` for Java's `null`. Reading a
    /// key marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getDouble")]
    pub fn get_double(&self, key: &str) -> Result<Option<f64>, Error> {
        self.config.get_double(key)
    }

    /// The `LIST` configuration `key`, borrowed from the parsed values, `None`
    /// for Java's `null`. Reading a key marks it used ([`Self::unused`]).
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getList")]
    pub fn get_list(&self, key: &str) -> Result<Option<&[String]>, Error> {
        self.config.get_list(key)
    }

    /// The `BOOLEAN` configuration `key`, `None` for Java's `null`. Reading a
    /// key marks it used ([`Self::unused`]).
    ///
    /// `enable.auto.commit` answers the value `postProcessParsedConfig`
    /// settled on: `false` without a `group.id` when it was not set.
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getBoolean")]
    pub fn get_boolean(&self, key: &str) -> Result<Option<bool>, Error> {
        self.config.get_boolean(key)
    }

    /// The `STRING` configuration `key`, trimmed, or `None` when it has no
    /// value (Java's `null`, e.g. an unset `group.id`). Reading a key marks it
    /// used ([`Self::unused`]).
    ///
    /// `client.id` answers the resolved id: when the property is unset, `new`
    /// derives it the way Java's `maybeOverrideClientId` does, so this is the
    /// id the consumer reports to the brokers. `security.protocol` answers the
    /// value as written, trimmed (`"sasl_ssl"`), as Java's does: its validator
    /// accepts it case-insensitively without changing it. A `PASSWORD`
    /// configuration that is not set answers `None`, as Java's `null`
    /// default casts to `String` (`SslConfigs.java:135-142`,
    /// `SaslConfigs.java:398`).
    ///
    /// # Errors
    ///
    /// - [`Error::LocalIllegalArgument`] when `key` is a configuration of
    ///   another type (`max.poll.records`, a set `ssl.keystore.key`). Java's
    ///   `getString` casts `get(key)` to `String` (`AbstractConfig.java:212-215`)
    ///   and fails with a `ClassCastException`, which, like
    ///   `IllegalArgumentException`, is not a `KafkaException`.
    /// - [`Error::Config`] with Java's `Unknown configuration '<key>'`
    ///   (`AbstractConfig.java:176-181`) when `key` is not a configuration this
    ///   class defines.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getString")]
    pub fn get_string(&self, key: &str) -> Result<Option<&str>, Error> {
        self.config.get_string(key)
    }

    /// The keys of the properties this configuration was built from that were
    /// never read: through a getter, or by the consumer built from it.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#unused")]
    pub fn unused(&self) -> HashSet<String> {
        self.config.unused()
    }

    /// Marks the configuration `key` used without reading it.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#ignore")]
    pub fn ignore(&self, key: &str) {
        self.config.ignore(key);
    }

    /// Info level log for any unused configurations: one line,
    /// `These configurations '[a, b]' were supplied but are not used yet.`,
    /// and nothing when every key was used.
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#logUnused")]
    pub fn log_unused(&self) {
        self.config.log_unused();
    }

    /// The names of the configurations this class defines.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfig#configNames")]
    pub fn config_names() -> HashSet<String> {
        CONFIG.names()
    }

    /// Marks used the keys the typed settings were read from, as the consumer
    /// does where Java's reads them through the getters.
    pub(crate) fn mark_read_keys_used(&self) {
        for key in &self.read_keys {
            self.config.ignore(key);
        }
    }

    /// Java's `logAll()`, which Java runs when the consumer builds the
    /// configuration with `doLog = true`: every parsed value at INFO.
    pub(crate) fn log_values(&self) {
        self.config.log_all("ConsumerConfig");
    }

    /// `bootstrap.servers`.
    pub(crate) fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }
    /// `client.dns.lookup`.
    pub(crate) fn client_dns_lookup(&self) -> ClientDnsLookup {
        self.client_dns_lookup
    }
    /// `client.id`.
    pub(crate) fn client_id(&self) -> &str {
        &self.client_id
    }
    /// `client.rack`, empty when unset ([`Self::DEFAULT_CLIENT_RACK`]).
    pub(crate) fn client_rack(&self) -> &str {
        &self.client_rack
    }
    /// `group.id`, if any.
    pub(crate) fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }
    /// `group.instance.id`, if any.
    pub(crate) fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }
    /// `group.protocol`.
    pub(crate) fn group_protocol(&self) -> &str {
        &self.group_protocol
    }
    /// `group.remote.assignor`, if any.
    pub(crate) fn group_remote_assignor(&self) -> Option<&str> {
        self.group_remote_assignor.as_deref()
    }
    /// `max.poll.interval.ms`.
    pub(crate) fn max_poll_interval_ms(&self) -> i32 {
        self.max_poll_interval_ms
    }
    /// `enable.auto.commit`.
    pub(crate) fn enable_auto_commit(&self) -> bool {
        self.enable_auto_commit
    }
    /// `auto.commit.interval.ms`.
    pub(crate) fn auto_commit_interval_ms(&self) -> i32 {
        self.auto_commit_interval_ms
    }
    /// `auto.offset.reset`.
    pub(crate) fn auto_offset_reset(&self) -> &str {
        &self.auto_offset_reset
    }
    /// `metadata.recovery.strategy`.
    pub(crate) fn metadata_recovery_strategy(&self) -> &str {
        &self.metadata_recovery_strategy
    }
    /// `internal.throw.on.fetch.stable.offset.unsupported`.
    pub(crate) fn throw_on_fetch_stable_offset_unsupported(&self) -> bool {
        self.throw_on_fetch_stable_offset_unsupported
    }
    /// `request.timeout.ms`.
    pub(crate) fn request_timeout_ms(&self) -> i32 {
        self.request_timeout_ms
    }
    /// `retry.backoff.ms`.
    pub(crate) fn retry_backoff_ms(&self) -> i64 {
        self.retry_backoff_ms
    }
    /// `retry.backoff.max.ms`.
    pub(crate) fn retry_backoff_max_ms(&self) -> i64 {
        self.retry_backoff_max_ms
    }
    /// `bootstrap.resolve.timeout.ms` (KIP-909); see
    /// [`Self::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG`].
    pub(crate) fn bootstrap_resolve_timeout_ms(&self) -> i64 {
        self.bootstrap_resolve_timeout_ms
    }

    /// Derives `client.id` on a configuration whose `client.id` is empty.
    ///
    /// `new` already derives it (`maybeOverrideClientId`, run by
    /// `postProcessParsedConfig`), so this only acts on a configuration built
    /// by the crate's tests from [`Self::defaults`], with the same derivation.
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfig#maybeOverrideClientId")]
    pub(crate) fn maybe_override_client_id(&mut self) -> Result<(), Error> {
        if !self.client_id.is_empty() {
            return Ok(());
        }
        if let Some(group_instance_id) = &self.group_instance_id {
            JoinGroupRequest::validate_group_instance_id(group_instance_id)?;
        }
        let group_id = self.group_id.as_deref().unwrap_or("null");
        let group_instance_id_part = match &self.group_instance_id {
            Some(group_instance_id) => group_instance_id.clone(),
            None => CONSUMER_CLIENT_ID_SEQUENCE.fetch_add(1, atomic::Ordering::Relaxed).to_string(),
        };
        self.client_id = format!("consumer-{group_id}-{group_instance_id_part}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    /// `postValidateSaslMechanismConfig` runs when the config is built, so an unusable
    /// `sasl.mechanism` fails here and not later, when a channel builder is created.
    #[test]
    fn test_sasl_mechanism_is_validated_when_the_config_is_built() {
        let build = |protocol: &str, mechanism: &str| {
            let props = std::collections::HashMap::from([
                ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
                ("security.protocol".to_string(), protocol.to_string()),
                ("sasl.mechanism".to_string(), mechanism.to_string()),
            ]);
            ConsumerConfig::new(&props)
        };

        let err = build("SASL_PLAINTEXT", "SCRAM-SHA-256").expect_err("SCRAM is not implemented");
        assert!(
            err.is_local_unsupported_operation_error(),
            "must be a LocalUnsupportedOperationError: {err:?}"
        );
        assert!(
            err.message().contains("SASL mechanism 'SCRAM-SHA-256' is not supported"),
            "got: {}",
            err.message()
        );

        let err = build("SASL_SSL", "").expect_err("an empty mechanism is invalid with SASL");
        assert!(err.is_config_error(), "must be a ConfigError: {err:?}");
        assert!(
            err.message().contains("mechanism must be non-null and non-empty string"),
            "got: {}",
            err.message()
        );

        build("SASL_PLAINTEXT", "PLAIN").expect("PLAIN is implemented");
        build("PLAINTEXT", "SCRAM-SHA-256").expect("the mechanism only matters with SASL");
    }

    use super::*;
    use crate::common::config::types::Password;
    use crate::test_log_capture::capture_logs;
    use log::Level;

    /// Props holding only `bootstrap.servers`, which the `ConfigDef` defines
    /// with `NO_DEFAULT_VALUE` and so every valid config must carry.
    fn base_props() -> HashMap<String, String> {
        HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())])
    }

    #[test]
    fn test_default_values() {
        let c = ConsumerConfig::defaults();
        assert_eq!(c.max_poll_records, 500);
        assert_eq!(c.max_poll_interval_ms(), 300_000);
        assert_eq!(c.get_int(ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG).unwrap(), Some(45_000));
        assert_eq!(c.get_int(ConsumerConfig::HEARTBEAT_INTERVAL_MS_CONFIG).unwrap(), Some(3_000));
        assert!(c.enable_auto_commit());
        assert_eq!(c.auto_commit_interval_ms(), 5_000);
        assert_eq!(c.auto_offset_reset(), "latest");
        assert_eq!(c.group_protocol(), "classic");
        assert_eq!(c.group_id(), None);
        assert_eq!(c.security_protocol, SecurityProtocol::Plaintext);
        assert_eq!(c.metadata_recovery_strategy(), "rebootstrap");
    }

    #[test]
    fn test_new_basic() {
        let mut props = base_props();
        props.insert("bootstrap.servers".to_string(), "host1:9092,host2:9093".to_string());
        props.insert("group.id".to_string(), "g".to_string());
        props.insert("client.id".to_string(), "my-consumer".to_string());
        props.insert("max.poll.records".to_string(), "100".to_string());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.bootstrap_servers(), &["host1:9092".to_string(), "host2:9093".to_string()]);
        assert_eq!(c.group_id(), Some("g"));
        assert_eq!(c.client_id(), "my-consumer");
        assert_eq!(c.max_poll_records, 100);
    }

    #[test]
    fn test_new_unknown_key_ignored() {
        let mut props = base_props();
        props.insert("unknown.key".to_string(), "value".to_string());
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.bootstrap_servers(), &["localhost:9092".to_string()]);
    }

    /// Boolean config values are parsed case-insensitively after trimming, and an
    /// unparseable value is rejected with Java's message and the original,
    /// untrimmed value. Mirrors `ConfigDef.parseType(BOOLEAN)`
    /// (`ConfigDef.java:703-737`): `equalsIgnoreCase("true")` / `("false")`, else
    /// `ConfigException(name, value, "Expected value to be either true or false")`.
    #[test]
    fn test_parse_bool_matches_java_boolean() {
        let props_with = |raw: &str| {
            // A `group.id`, without which `enable.auto.commit=true` is rejected.
            HashMap::from([
                ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
                ("group.id".to_string(), "g".to_string()),
                ("enable.auto.commit".to_string(), raw.to_string()),
            ])
        };

        // Accepted regardless of case, and after trimming surrounding whitespace.
        for raw in ["true", "True", "TRUE"] {
            let c = ConsumerConfig::new(&props_with(raw))
                .unwrap_or_else(|e| panic!("enable.auto.commit={raw} should be valid: {e:?}"));
            assert!(c.enable_auto_commit(), "for enable.auto.commit={raw}");
        }
        for raw in ["false", "False", " false "] {
            let c = ConsumerConfig::new(&props_with(raw))
                .unwrap_or_else(|e| panic!("enable.auto.commit={raw} should be valid: {e:?}"));
            assert!(!c.enable_auto_commit(), "for enable.auto.commit={raw}");
        }

        // Rejected: a non-boolean string. Java prints the original (untrimmed) value.
        let error =
            ConsumerConfig::new(&props_with("yes")).expect_err("a non-boolean enable.auto.commit must be rejected");
        assert_eq!(
            error.message(),
            "Invalid value yes for configuration enable.auto.commit: Expected value to be either true or false",
        );
        assert!(matches!(error, Error::Config(_)), "expected Error::Config, got {error:?}");
        assert!(error.is_kafka_error());
    }

    /// `metrics.num.samples` is `atLeast(1)` (Java ConsumerConfig). A value
    /// below 1 is rejected with a message asserting the bound.
    #[test]
    fn test_metrics_num_samples_validator() {
        // Valid: >= 1.
        let mut props = base_props();
        props.insert("metrics.num.samples".to_string(), "3".to_string());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.metrics_num_samples, 3);

        // Invalid: 0 (< 1).
        let mut props = base_props();
        props.insert("metrics.num.samples".to_string(), "0".to_string());
        let err = ConsumerConfig::new(&props).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("metrics.num.samples") && msg.contains("at least 1"),
            "unexpected message: {msg}"
        );

        // Invalid: negative.
        let mut props = base_props();
        props.insert("metrics.num.samples".to_string(), "-1".to_string());
        assert!(ConsumerConfig::new(&props).is_err());
    }

    /// `metadata.cluster.check.enable` (KIP-1242): `Type.BOOLEAN`, default `true`
    /// (`ConsumerConfig.java:723-727`).
    #[test]
    fn test_metadata_cluster_check_enable() {
        assert!(ConsumerConfig::new(&base_props()).unwrap().metadata_cluster_check_enable);

        let mut props = base_props();
        props.insert(
            CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG.to_string(),
            "false".to_string(),
        );
        assert!(!ConsumerConfig::new(&props).unwrap().metadata_cluster_check_enable);

        props.insert(
            CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG.to_string(),
            "maybe".to_string(),
        );
        assert_eq!(
            ConsumerConfig::new(&props).unwrap_err().message(),
            "Invalid value maybe for configuration metadata.cluster.check.enable: Expected value to be either true or false"
        );
    }

    /// `metadata.recovery.rebootstrap.trigger.ms` is `atLeast(0)` (Java
    /// `ConsumerConfig.java:717-721`): 0 is accepted, -1 is rejected with
    /// Java's `ConfigDef.Range.atLeast` message.
    #[test]
    fn test_metadata_recovery_rebootstrap_trigger_ms_validator() {
        let mut props = base_props();
        props.insert("metadata.recovery.rebootstrap.trigger.ms".to_string(), "0".to_string());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.metadata_recovery_rebootstrap_trigger_ms, 0);

        let mut props = base_props();
        props.insert("metadata.recovery.rebootstrap.trigger.ms".to_string(), "-1".to_string());
        let err = ConsumerConfig::new(&props).unwrap_err();
        let Error::Config(config_error) = err else {
            panic!("expected a config error, got {err:?}");
        };
        assert_eq!(
            config_error.message(),
            "Invalid value -1 for configuration metadata.recovery.rebootstrap.trigger.ms: \
             Value must be at least 0"
        );
    }

    /// `request.timeout.ms` and `default.api.timeout.ms` are `atLeast(0)`
    /// (Java `ConsumerConfig.java:618-629`): 0 is accepted and a negative
    /// value is rejected when the config is built. A negative value would
    /// otherwise reach `CompletableEvent::calculate_deadline_ms`, which maps a
    /// negative timeout to "no deadline", so every default-timeout API and
    /// `close()` could wait forever.
    #[test]
    fn test_request_and_default_api_timeout_ms_validators() {
        for key in [
            ConsumerConfig::REQUEST_TIMEOUT_MS_CONFIG,
            ConsumerConfig::DEFAULT_API_TIMEOUT_MS_CONFIG,
        ] {
            let mut props = base_props();
            props.insert(key.to_string(), "0".to_string());
            let c = ConsumerConfig::new(&props).unwrap();
            if key == ConsumerConfig::REQUEST_TIMEOUT_MS_CONFIG {
                assert_eq!(c.request_timeout_ms, 0);
            } else {
                assert_eq!(c.default_api_timeout_ms, 0);
            }

            let mut props = base_props();
            props.insert(key.to_string(), "-1".to_string());
            let err = ConsumerConfig::new(&props).unwrap_err();
            let Error::Config(config_error) = err else {
                panic!("expected a config error for {key}, got {err:?}");
            };
            assert_eq!(
                config_error.message(),
                format!("Invalid value -1 for configuration {key}: Value must be at least 0")
            );
        }
    }

    /// `bootstrap.resolve.timeout.ms` (KIP-909): `Type.LONG`, default `0`
    /// (synchronous resolution), `atLeast(0L)` since KAFKA-20939.
    #[test]
    fn test_bootstrap_resolve_timeout_ms() {
        assert_eq!(
            ConsumerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG,
            "bootstrap.resolve.timeout.ms"
        );
        let mut props = HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())]);
        assert_eq!(ConsumerConfig::new(&props).unwrap().bootstrap_resolve_timeout_ms(), 0);

        props.insert("bootstrap.resolve.timeout.ms".to_string(), "3000".to_string());
        assert_eq!(ConsumerConfig::new(&props).unwrap().bootstrap_resolve_timeout_ms(), 3000);

        props.insert("bootstrap.resolve.timeout.ms".to_string(), "-1".to_string());
        match ConsumerConfig::new(&props) {
            Err(Error::Config(e)) => assert_eq!(
                e.message(),
                "Invalid value -1 for configuration bootstrap.resolve.timeout.ms: Value must be at least 0"
            ),
            other => panic!("expected a config error, got {other:?}"),
        }
    }

    /// `metrics.sample.window.ms` is `atLeast(0)` (Java ConsumerConfig). A
    /// negative value is rejected with a message asserting the bound.
    #[test]
    fn test_metrics_sample_window_ms_validator() {
        // Valid: >= 0 (0 is allowed).
        let mut props = base_props();
        props.insert("metrics.sample.window.ms".to_string(), "0".to_string());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.metrics_sample_window_ms, 0);

        let mut props = base_props();
        props.insert("metrics.sample.window.ms".to_string(), "60000".to_string());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.metrics_sample_window_ms, 60_000);

        // Invalid: negative.
        let mut props = base_props();
        props.insert("metrics.sample.window.ms".to_string(), "-1".to_string());
        let err = ConsumerConfig::new(&props).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("metrics.sample.window.ms") && msg.contains("at least 0"),
            "unexpected message: {msg}"
        );
    }

    /// `metrics.recording.level` accepts exactly `INFO`/`DEBUG`/`TRACE`
    /// (case-sensitive, mirroring Java `ConfigDef.ValidString.in`). Any other
    /// value — including lower/mixed case — is rejected with the Java
    /// `ConfigException` wording.
    #[test]
    fn test_metrics_recording_level_validator() {
        for level in ["INFO", "DEBUG", "TRACE"] {
            let mut props = base_props();
            props.insert("metrics.recording.level".to_string(), level.to_string());
            let c = ConsumerConfig::new(&props).unwrap();
            assert_eq!(c.metrics_recording_level, level);
        }

        // Lowercase is rejected (case-sensitive membership check).
        let mut props = base_props();
        props.insert("metrics.recording.level".to_string(), "debug".to_string());
        let err = ConsumerConfig::new(&props).unwrap_err();
        assert!(
            err.to_string().ends_with(
                "Invalid value debug for configuration metrics.recording.level: \
                 String must be one of: INFO, DEBUG, TRACE"
            ),
            "unexpected message: {err}"
        );

        // A wholly unknown value is rejected too.
        let mut props = base_props();
        props.insert("metrics.recording.level".to_string(), "bogus".to_string());
        assert!(ConsumerConfig::new(&props).is_err());
    }

    /// Each of the four `security.protocol` values parses to the right enum.
    #[test]
    fn test_security_protocol_all_values() {
        for (input, expected) in [
            ("PLAINTEXT", SecurityProtocol::Plaintext),
            ("SSL", SecurityProtocol::Ssl),
            ("SASL_PLAINTEXT", SecurityProtocol::SaslPlaintext),
            ("SASL_SSL", SecurityProtocol::SaslSsl),
        ] {
            let mut props = base_props();
            props.insert("security.protocol".to_string(), input.to_string());
            props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
            let c = ConsumerConfig::new(&props).unwrap();
            assert_eq!(c.security_protocol, expected, "for input {input}");
        }
    }

    /// `security.protocol` parsing is case-insensitive (mirrors Java/`for_name`).
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testCaseInsensitiveSecurityProtocol")]
    fn test_case_insensitive_security_protocol() {
        let mut props = base_props();
        props.insert("security.protocol".to_string(), "sasl_ssl".to_string());
        // Only `PLAIN` is accepted under a SASL protocol (`post_validate_supported_sasl_mechanism`).
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.security_protocol, SecurityProtocol::SaslSsl);
    }

    /// Invalid `security.protocol` → `illegal_argument` with asserted message
    /// content (DoD §3): the config key, the bad value, and the valid names.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testInvalidSecurityProtocol")]
    fn test_invalid_security_protocol() {
        let mut props = base_props();
        props.insert("security.protocol".to_string(), "abc".to_string());
        let err = ConsumerConfig::new(&props).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("security.protocol"), "should contain config key, got: {msg}");
        assert!(msg.contains("abc"), "should contain the invalid value, got: {msg}");
        assert!(msg.contains("SASL_SSL"), "should list the valid protocol names, got: {msg}");
    }

    /// `bootstrap.servers` has `NO_DEFAULT_VALUE` (`ConsumerConfig.java:441-443`), so a config
    /// without it fails at parse time with `ConfigDef.parseValue`'s message.
    #[test]
    fn test_missing_bootstrap_servers_rejected_with_exact_message() {
        let err = ConsumerConfig::new(&HashMap::new()).unwrap_err();
        let Error::Config(config_err) = &err else {
            panic!("expected a config error, got: {err:?}");
        };
        assert_eq!(
            config_err.message(),
            "Missing required configuration \"bootstrap.servers\" which has no default value."
        );
    }

    /// `client.dns.lookup` defaults to `use_all_dns_ips` and parses into the
    /// typed [`ClientDnsLookup`], as `ConsumerConfig`'s `ConfigDef` defines it.
    #[test]
    fn test_client_dns_lookup() {
        assert_eq!(ConsumerConfig::CLIENT_DNS_LOOKUP_CONFIG, "client.dns.lookup");
        assert_eq!(
            ConsumerConfig::new(&base_props()).unwrap().client_dns_lookup(),
            ClientDnsLookup::UseAllDnsIps
        );

        let mut props = base_props();
        props.insert(
            CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG.to_string(),
            "resolve_canonical_bootstrap_servers_only".to_string(),
        );
        assert_eq!(
            ConsumerConfig::new(&props).unwrap().client_dns_lookup(),
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly
        );

        props.insert(CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG.to_string(), "default".to_string());
        assert_eq!(
            ConsumerConfig::new(&props).unwrap_err().message(),
            "Invalid value default for configuration client.dns.lookup: String must be one of: \
             use_all_dns_ips, resolve_canonical_bootstrap_servers_only"
        );
    }

    /// `sasl.mechanism` and `sasl.jaas.config` land on `sasl_config`.
    #[test]
    fn test_sasl_config_from_properties() {
        let mut props = base_props();
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"secret\";"
                .to_string(),
        );
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.sasl_config.mechanism, "PLAIN");
        assert_eq!(c.sasl_config.resolve_username(), Some("alice"));
        assert_eq!(c.sasl_config.resolve_password(), Some("secret"));
    }

    /// Rust-only (`CommonClientConfigs::post_validate_supported_sasl_mechanism`):
    /// a SASL `security.protocol` needs `sasl.mechanism=PLAIN`; any other
    /// mechanism, the `GSSAPI` default included, fails the build with
    /// `LocalUnsupportedOperationError`, while an empty one keeps Java's
    /// `ConfigError`. Under `PLAINTEXT` the mechanism is inert, as in Java.
    #[test]
    fn test_unsupported_sasl_mechanism_rejected() {
        let jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";";
        let build = |extra: &[(&str, &str)]| {
            let mut props = base_props();
            for (k, v) in extra {
                props.insert((*k).to_string(), (*v).to_string());
            }
            ConsumerConfig::new(&props)
        };

        let err = build(&[
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanism", "SCRAM-SHA-256"),
            ("sasl.jaas.config", jaas),
        ])
        .unwrap_err();
        assert!(err.is_local_unsupported_operation_error(), "{err:?}");
        assert_eq!(
            err.to_string(),
            "LocalUnsupportedOperationError: SASL mechanism 'SCRAM-SHA-256' is not supported by this client; only \
             PLAIN is"
        );

        // The default mechanism is Java's `GSSAPI`.
        let err = build(&[("security.protocol", "SASL_SSL")]).unwrap_err();
        assert!(err.is_local_unsupported_operation_error(), "{err:?}");
        assert_eq!(
            err.to_string(),
            "LocalUnsupportedOperationError: SASL mechanism 'GSSAPI' is not supported by this client; only PLAIN is"
        );

        // Case-sensitive, as Java's mechanism names are.
        let err = build(&[("security.protocol", "SASL_PLAINTEXT"), ("sasl.mechanism", "plain")]).unwrap_err();
        assert!(err.is_local_unsupported_operation_error(), "{err:?}");

        build(&[("security.protocol", "PLAINTEXT")]).unwrap();
        build(&[
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanism", "PLAIN"),
            ("sasl.jaas.config", jaas),
        ])
        .unwrap();

        // An empty mechanism fails Java's check first.
        let err = build(&[("security.protocol", "SASL_PLAINTEXT"), ("sasl.mechanism", "")]).unwrap_err();
        assert!(err.is_config_error(), "{err:?}");
        assert_eq!(
            err.to_string(),
            "ConfigError: Invalid value null for configuration sasl.mechanism: When the security.protocol \
             configuration enables SASL, mechanism must be non-null and non-empty string."
        );
    }

    /// `{:?}` of a config built from every `Type.PASSWORD` key the client
    /// parses (`sasl.jaas.config` and the three SSL keys) renders none of their
    /// values. The derived `Debug` is safe because the embedded `SaslConfigs`
    /// and `SslConfigs` hold those values as `Password`s. The three SSL
    /// password keys this client does not define render as key names only.
    #[test]
    fn test_debug_redacts_password_configs() {
        let secrets = [
            (
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"jaas-user\" password=\"jaas-S3cr3t\";",
            ),
            ("ssl.truststore.certificates", "TRUSTSTORE-CERTIFICATES-PEM"),
            ("ssl.keystore.key", "KEYSTORE-KEY-PEM"),
            ("ssl.keystore.certificate.chain", "KEYSTORE-CERTIFICATE-CHAIN-PEM"),
        ];
        // Not defined (`SslConfigs` module docs), so held only in the originals.
        let undefined = [
            ("ssl.truststore.password", "truststore-S3cr3t"),
            ("ssl.keystore.password", "keystore-S3cr3t"),
            ("ssl.key.password", "key-S3cr3t"),
        ];
        let mut props: HashMap<String, String> = secrets
            .iter()
            .chain(&undefined)
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        props.insert("bootstrap.servers".to_string(), "visible-host:9092".to_string());
        props.insert("group.id".to_string(), "visible-group".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert("ssl.truststore.location".to_string(), "/visible/truststore.pem".to_string());
        let c = ConsumerConfig::new(&props).unwrap();

        // The secrets did reach the config; they are only hidden from `Debug`.
        assert_eq!(c.sasl_config.resolve_password(), Some("jaas-S3cr3t"));
        assert_eq!(
            c.ssl_config.keystore_key.as_ref().map(Password::value),
            Some("KEYSTORE-KEY-PEM")
        );

        for rendered in [format!("{c:?}"), format!("{c:#?}")] {
            for (key, value) in secrets.iter().chain(&undefined) {
                assert!(!rendered.contains(value), "{key} leaked: {rendered}");
            }
            for fragment in ["jaas-S3cr3t", "jaas-user", "PlainLoginModule", "S3cr3t", "-PEM"] {
                assert!(!rendered.contains(fragment), "{fragment:?} leaked: {rendered}");
            }
            assert_eq!(rendered.matches(Password::HIDDEN).count(), secrets.len(), "{rendered}");
            for visible in [
                "visible-host:9092",
                "visible-group",
                "\"PLAIN\"",
                "/visible/truststore.pem",
            ] {
                assert!(rendered.contains(visible), "{visible} missing: {rendered}");
            }
        }
    }

    /// Empty `sasl.jaas.config` → `None`.
    #[test]
    fn test_sasl_jaas_config_empty_is_none() {
        let mut props = base_props();
        props.insert("sasl.jaas.config".to_string(), String::new());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.sasl_config.jaas_config, None);
    }

    /// `ssl.*` keys land on `ssl_config` via the shared helper.
    #[test]
    fn test_ssl_config_from_properties() {
        let mut props = base_props();
        props.insert("ssl.truststore.location".to_string(), "/path/to/truststore.pem".to_string());
        props.insert("ssl.keystore.location".to_string(), "/path/to/keystore.pem".to_string());
        props.insert("ssl.endpoint.identification.algorithm".to_string(), String::new());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.ssl_config.truststore_location.as_deref(), Some("/path/to/truststore.pem"));
        assert_eq!(c.ssl_config.keystore_location.as_deref(), Some("/path/to/keystore.pem"));
        assert_eq!(c.ssl_config.endpoint_identification_algorithm, "");
    }

    /// Builder selection: a protocol with no certificate requirement
    /// (`SASL_PLAINTEXT` + PLAIN) builds `Ok`; `SASL_SSL` with no truststore
    /// content → error (mirrors `channel_builders` / `SslFactory` validation).
    ///
    /// The `SASL_SSL`-with-*valid*-truststore happy path requires a real
    /// parseable CA certificate, which is exercised in the Docker integration
    /// test `tests/integration/sasl_ssl_consumer_test.rs` (it has access to the
    /// `rcgen`/cluster-generated CA); here we cover the protocol-selection wiring
    /// and the missing-ssl error branch without filesystem/cert dependencies.
    #[test]
    fn test_builder_selection() {
        use crate::common::network::ChannelBuilders;
        use crate::common::utils::internals::LogContext;

        // SASL_PLAINTEXT with PLAIN credentials → Ok (no cert needed).
        let mut props = base_props();
        props.insert("security.protocol".to_string(), "SASL_PLAINTEXT".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";"
                .to_string(),
        );
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.security_protocol, SecurityProtocol::SaslPlaintext);
        let builder = ChannelBuilders::client_channel_builder(
            c.security_protocol,
            Some(&c.ssl_config),
            Some(&c.sasl_config),
            None,
            c.client_id(),
            LogContext::new("[test] ".to_string()),
        );
        assert!(builder.is_ok(), "SASL_PLAINTEXT with valid config should build");

        // SASL_SSL with no ssl_config supplied at all → error: the
        // `channel_builders` SASL_SSL arm requires ssl_config to be present.
        let mut props = base_props();
        props.insert("security.protocol".to_string(), "SASL_SSL".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";"
                .to_string(),
        );
        let c = ConsumerConfig::new(&props).unwrap();
        let builder = ChannelBuilders::client_channel_builder(
            c.security_protocol,
            None, // missing ssl_config content
            Some(&c.sasl_config),
            None,
            c.client_id(),
            LogContext::new("[test] ".to_string()),
        );
        let err = match builder {
            Ok(_) => panic!("SASL_SSL with no ssl_config should fail to build"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("ssl_config"), "error should mention ssl_config, got: {msg}");
    }

    /// Java's `ConfigDef` does not coerce an empty `group.id` to null: it stays
    /// `""` and `AsyncKafkaConsumer.initializeGroupMetadata` rejects it
    /// (`AsyncKafkaConsumer.java:839-844`). Coercing it to `None` here would
    /// turn a hard configuration error into a groupless consumer, which then
    /// fails much later and much less legibly.
    #[test]
    fn test_new_keeps_an_empty_group_id() {
        let mut props = base_props();
        props.insert("bootstrap.servers".to_string(), "host1:9092".to_string());
        props.insert("group.id".to_string(), String::new());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.group_id(), Some(""), "the empty string must survive to the constructor");
    }

    /// An absent `group.id` is still `None` — the two cases stay distinct.
    #[test]
    fn test_new_absent_group_id_is_none() {
        let mut props = base_props();
        props.insert("bootstrap.servers".to_string(), "host1:9092".to_string());
        let c = ConsumerConfig::new(&props).unwrap();
        assert_eq!(c.group_id(), None);
    }

    /// `bootstrap.servers` is a `Type.LIST`: `ConfigDef.parseType` trims the
    /// value and splits it on `\\s*,\\s*`, so whitespace around the commas
    /// and at the ends never reaches `ClientUtils.parseAndValidateAddresses`
    /// (which does not trim, and rejects it).
    #[test]
    fn test_bootstrap_servers_list_parsing() {
        for value in [
            "localhost:1,localhost:2",
            "localhost:1, localhost:2",
            " localhost:1 ,localhost:2 ",
        ] {
            let props = HashMap::from([("bootstrap.servers".to_string(), value.to_string())]);
            let config = ConsumerConfig::new(&props).unwrap();
            assert_eq!(
                config.bootstrap_servers(),
                ["localhost:1".to_string(), "localhost:2".to_string()],
                "{value:?}"
            );
            let addresses = crate::ClientUtils::parse_and_validate_addresses(
                config.bootstrap_servers(),
                config.client_dns_lookup(),
            )
            .unwrap();
            assert_eq!(addresses.len(), 2, "{value:?}");
        }
    }

    /// `bootstrap.servers` is validated with Java's
    /// `ValidList.anyNonDuplicateValues(false, false)` (`ConsumerConfig.java:444`): an empty
    /// element is rejected with `ConfigDef`'s exact message and duplicates are removed
    /// (single-message `ConfigException`, no `Invalid value` prefix). An empty list is rejected too.
    #[test]
    fn test_bootstrap_servers_valid_list() {
        let error_message = |value: &str| {
            let props = HashMap::from([("bootstrap.servers".to_string(), value.to_string())]);
            match ConsumerConfig::new(&props) {
                Err(Error::Config(e)) => e.message().to_string(),
                other => panic!("expected a ConfigError for {value:?}, got {other:?}"),
            }
        };
        for value in ["localhost:9092,,localhost:9093", "a:1, ,b:1", "a:1,"] {
            assert_eq!(
                error_message(value),
                "Configuration 'bootstrap.servers' values must not be empty.",
                "{value:?}"
            );
        }
        // `ConfigDef.parseValue` removes duplicates (with a warning) before validating.
        let props = HashMap::from([("bootstrap.servers".to_string(), "a:1,a:1".to_string())]);
        assert_eq!(ConsumerConfig::new(&props).unwrap().bootstrap_servers(), ["a:1".to_string()]);
        assert_eq!(
            error_message(",,"),
            "Configuration 'bootstrap.servers' values must not be empty."
        );
        for value in ["", "  "] {
            assert_eq!(
                error_message(value),
                "Configuration 'bootstrap.servers' must not be empty. Valid values include: any non-empty value",
                "{value:?}"
            );
        }
        let props = HashMap::from([("bootstrap.servers".to_string(), "a:1,b:1".to_string())]);
        assert_eq!(
            ConsumerConfig::new(&props).unwrap().bootstrap_servers(),
            ["a:1".to_string(), "b:1".to_string()]
        );
    }

    /// Props with `bootstrap.servers` plus the given extra keys.
    fn props_with(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut props = base_props();
        for (k, v) in extra {
            props.insert((*k).to_string(), (*v).to_string());
        }
        props
    }

    /// Parses the `<n>` suffix of a generated `consumer-<group>-<n>` id.
    fn sequence_suffix<'a>(client_id: &'a str, prefix: &str) -> &'a str {
        client_id
            .strip_prefix(prefix)
            .unwrap_or_else(|| panic!("{client_id:?} does not start with {prefix:?}"))
    }

    /// Without `client.id`, the id is `consumer-<group.id>-<n>` and every
    /// config draws a fresh `<n>` from the process-wide sequence.
    #[test]
    fn test_generated_client_id_uses_group_id_and_sequence() {
        let props = props_with(&[(ConsumerConfig::GROUP_ID_CONFIG, "test-group")]);
        let first = ConsumerConfig::new(&props).unwrap();
        let second = ConsumerConfig::new(&props).unwrap();
        let n1: i32 = sequence_suffix(first.client_id(), "consumer-test-group-").parse().unwrap();
        let n2: i32 = sequence_suffix(second.client_id(), "consumer-test-group-").parse().unwrap();
        assert!(n1 >= 1, "{}", first.client_id());
        assert_ne!(first.client_id(), second.client_id());
        // Other tests may draw from the shared sequence concurrently, so only
        // the direction is fixed, not the gap.
        assert!(n2 > n1, "{} then {}", first.client_id(), second.client_id());
    }

    /// A copy keeps the configuration's resolved `client.id` and takes no
    /// sequence number, while the next configuration built takes the next
    /// one, as Java's next `new ConsumerConfig(props)` does. The copy's used
    /// set is its own, as `new` leaves it.
    #[test]
    fn test_copy_for_client_keeps_the_resolved_client_id() {
        let props = HashMap::from([
            (
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                "localhost:9092".to_string(),
            ),
            (ConsumerConfig::GROUP_ID_CONFIG.to_string(), "copy-group".to_string()),
        ]);
        let first = ConsumerConfig::new(&props).unwrap();
        let copy = first.copy_for_client().unwrap();
        let second_copy = first.copy_for_client().unwrap();
        let next = ConsumerConfig::new(&props).unwrap();
        assert_eq!(copy.client_id(), first.client_id());
        assert_eq!(second_copy.client_id(), first.client_id());
        assert_eq!(
            copy.config.get_string(ConsumerConfig::CLIENT_ID_CONFIG).unwrap(),
            Some(first.client_id())
        );
        let n1: i32 = sequence_suffix(first.client_id(), "consumer-copy-group-").parse().unwrap();
        let n2: i32 = sequence_suffix(next.client_id(), "consumer-copy-group-").parse().unwrap();
        // Other tests take numbers concurrently, so the next one is only later.
        assert!(n2 > n1, "{} then {}", first.client_id(), next.client_id());
        assert_eq!(copy.unused(), first.unused());
        first.config.ignore(ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG);
        assert!(copy.unused().contains(ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG));

        let explicit = HashMap::from([
            (
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                "localhost:9092".to_string(),
            ),
            (ConsumerConfig::CLIENT_ID_CONFIG.to_string(), "mine".to_string()),
        ]);
        let config = ConsumerConfig::new(&explicit).unwrap();
        assert_eq!(config.copy_for_client().unwrap().client_id(), "mine");
    }

    /// `new` logs each configuration warning once (the parse's duplicate
    /// values and the post-processing's); a copy
    /// for one more client logs none of them, as Java's client logs nothing
    /// about the config it is given, but makes the same reads, so its used set
    /// is `new`'s.
    #[test]
    fn test_copy_for_client_logs_no_configuration_warning() {
        let props = props_with(&[
            (ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9092"),
            (ConsumerConfig::GROUP_ID_CONFIG, "copy-log-group"),
            (ConsumerConfig::GROUP_PROTOCOL_CONFIG, "classic"),
            (ConsumerConfig::RETRY_BACKOFF_MS_CONFIG, "2000"),
            (ConsumerConfig::RETRY_BACKOFF_MAX_MS_CONFIG, "1000"),
            (ConsumerConfig::RECONNECT_BACKOFF_MS_CONFIG, "100"),
            (CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "40000"),
            (CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, "30000"),
            (ConsumerConfig::CONNECTIONS_MAX_IDLE_MS_CONFIG, "1000"),
            (ConsumerConfig::MAX_POLL_INTERVAL_MS_CONFIG, "300000"),
        ]);
        let (config, new_lines) = capture_logs(|| ConsumerConfig::new(&props).unwrap());
        let warnings = |lines: &[(Level, String)]| {
            lines
                .iter()
                .filter(|(level, _)| *level == Level::Warn)
                .map(|(_, line)| line.split(' ').take(3).collect::<Vec<_>>().join(" "))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            warnings(&new_lines),
            [
                "Configuration key \"bootstrap.servers\"",
                "Configuration 'retry.backoff.ms' with",
                "Configuration 'socket.connection.setup.timeout.ms' with",
                "Disabling exponential reconnect",
                "Configuration 'connections.max.idle.ms' with",
            ],
            "{new_lines:?}"
        );
        let (copy, copy_lines) = capture_logs(|| config.copy_for_client().unwrap());
        assert!(copy_lines.is_empty(), "{copy_lines:?}");
        assert_eq!(copy.unused(), ConsumerConfig::new(&props).unwrap().unused());
    }

    /// A static member uses its `group.instance.id` instead of the sequence.
    #[test]
    fn test_generated_client_id_uses_group_instance_id() {
        let props = props_with(&[
            (ConsumerConfig::GROUP_ID_CONFIG, "test-group"),
            (ConsumerConfig::GROUP_INSTANCE_ID_CONFIG, "inst"),
        ]);
        assert_eq!(ConsumerConfig::new(&props).unwrap().client_id(), "consumer-test-group-inst");
    }

    /// An explicit `client.id` is never overridden.
    #[test]
    fn test_explicit_client_id_preserved() {
        let props = props_with(&[
            (ConsumerConfig::GROUP_ID_CONFIG, "test-group"),
            (ConsumerConfig::CLIENT_ID_CONFIG, "my-consumer"),
        ]);
        assert_eq!(ConsumerConfig::new(&props).unwrap().client_id(), "my-consumer");
    }

    /// Java formats a missing `group.id` with `String.format("%s", null)`,
    /// yielding the literal `null`.
    #[test]
    fn test_generated_client_id_without_group_id() {
        let config = ConsumerConfig::new(&base_props()).unwrap();
        let n: i32 = sequence_suffix(config.client_id(), "consumer-null-").parse().unwrap();
        assert!(n >= 1, "{}", config.client_id());
    }

    /// A `group.instance.id` that breaks the topic-name rules is rejected
    /// while deriving the client id, with `JoinGroupRequest`'s message.
    #[test]
    fn test_generated_client_id_rejects_invalid_group_instance_id() {
        let props = props_with(&[
            (ConsumerConfig::GROUP_ID_CONFIG, "test-group"),
            (ConsumerConfig::GROUP_INSTANCE_ID_CONFIG, "bad/id"),
        ]);
        let err = ConsumerConfig::new(&props).unwrap_err();
        assert!(matches!(err, Error::InvalidConfiguration(_)), "got {err:?}");
        assert!(err.is_kafka_error(), "got {err:?}");
        assert_eq!(
            err.message(),
            "Group instance id is invalid: 'bad/id' contains one or more characters other than ASCII \
             alphanumerics, '.', '_' and '-'"
        );
    }

    /// With an explicit `client.id`, `ConsumerConfig` does not validate
    /// `group.instance.id` here; the consumer constructor does (Java
    /// `GroupRebalanceConfig`, see `AsyncKafkaConsumer::new`).
    #[test]
    fn test_explicit_client_id_skips_group_instance_id_validation() {
        let props = props_with(&[
            (ConsumerConfig::CLIENT_ID_CONFIG, "my-consumer"),
            (ConsumerConfig::GROUP_INSTANCE_ID_CONFIG, "bad/id"),
        ]);
        assert_eq!(ConsumerConfig::new(&props).unwrap().client_id(), "my-consumer");
    }

    /// `ConfigDef.parseType` trims `client.id`, so a blank one generates an id
    /// and a padded one is kept without its padding.
    #[test]
    fn test_client_id_is_trimmed() {
        let blank = props_with(&[
            (ConsumerConfig::GROUP_ID_CONFIG, "test-group"),
            (ConsumerConfig::CLIENT_ID_CONFIG, " "),
        ]);
        let config = ConsumerConfig::new(&blank).unwrap();
        sequence_suffix(config.client_id(), "consumer-test-group-");
        let padded = props_with(&[(ConsumerConfig::CLIENT_ID_CONFIG, " my-consumer ")]);
        assert_eq!(ConsumerConfig::new(&padded).unwrap().client_id(), "my-consumer");
    }

    /// `getString` answers a `STRING` key's value, `null` for an unset
    /// nullable one, the generated `client.id` when the property is unset, a
    /// cast failure for a key of another type, and `Unknown configuration` for
    /// a key the configuration does not hold.
    #[test]
    fn test_get_string() {
        let config = ConsumerConfig::new(&props_with(&[(ConsumerConfig::GROUP_ID_CONFIG, "test-group")])).unwrap();
        assert_eq!(Some("test-group"), config.get_string(ConsumerConfig::GROUP_ID_CONFIG).unwrap());
        assert_eq!(None, config.get_string(ConsumerConfig::GROUP_INSTANCE_ID_CONFIG).unwrap());
        let client_id = config.get_string(ConsumerConfig::CLIENT_ID_CONFIG).unwrap().unwrap();
        assert_eq!(config.client_id(), client_id);
        sequence_suffix(client_id, "consumer-test-group-");

        // Every other `STRING` key, at its Java default.
        let defaults: &[(&str, Option<&str>)] = &[
            (ConsumerConfig::GROUP_PROTOCOL_CONFIG, Some("classic")),
            (ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG, None),
            (ConsumerConfig::CLIENT_DNS_LOOKUP_CONFIG, Some("use_all_dns_ips")),
            (ConsumerConfig::CLIENT_RACK_CONFIG, Some("")),
            (ConsumerConfig::AUTO_OFFSET_RESET_CONFIG, Some("latest")),
            (CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG, Some("rebootstrap")),
            (
                ConsumerConfig::ISOLATION_LEVEL_CONFIG,
                Some(ConsumerConfig::DEFAULT_ISOLATION_LEVEL),
            ),
            (ConsumerConfig::METRICS_RECORDING_LEVEL_CONFIG, Some("INFO")),
            (ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, Some("implicit")),
            (ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG, Some("BATCH_OPTIMIZED")),
            (CommonClientConfigs::SECURITY_PROTOCOL_CONFIG, Some("PLAINTEXT")),
            (SaslConfigs::SASL_MECHANISM, Some("GSSAPI")),
            (SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG, None),
            (SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG, Some("PEM")),
            (SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG, None),
            (SslConfigs::SSL_KEYSTORE_TYPE_CONFIG, Some("PEM")),
            (SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, Some("https")),
        ];
        for (key, expected) in defaults {
            assert_eq!(*expected, config.get_string(key).unwrap(), "for {key}");
        }

        // An unset `PASSWORD` key is Java's `null` default, which casts to
        // `String`; only a set one fails the cast (below).
        for key in [
            SaslConfigs::SASL_JAAS_CONFIG,
            SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
            SslConfigs::SSL_KEYSTORE_KEY_CONFIG,
            SslConfigs::SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
        ] {
            assert_eq!(None, config.get_string(key).unwrap(), "for {key}");
        }

        // Every other `STRING` key, set (padded, as `getString` trims).
        let set: &[(&str, &str, &str)] = &[
            (ConsumerConfig::GROUP_PROTOCOL_CONFIG, "consumer", "consumer"),
            (ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG, " uniform ", "uniform"),
            (
                ConsumerConfig::CLIENT_DNS_LOOKUP_CONFIG,
                "resolve_canonical_bootstrap_servers_only",
                "resolve_canonical_bootstrap_servers_only",
            ),
            (ConsumerConfig::CLIENT_RACK_CONFIG, " rack-1 ", "rack-1"),
            (ConsumerConfig::AUTO_OFFSET_RESET_CONFIG, "earliest", "earliest"),
            (CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG, "none", "none"),
            (ConsumerConfig::ISOLATION_LEVEL_CONFIG, "read_uncommitted", "read_uncommitted"),
            (ConsumerConfig::METRICS_RECORDING_LEVEL_CONFIG, "DEBUG", "DEBUG"),
            (CommonClientConfigs::SECURITY_PROTOCOL_CONFIG, " sasl_ssl ", "sasl_ssl"),
            (SaslConfigs::SASL_MECHANISM, " PLAIN ", "PLAIN"),
            (SslConfigs::SSL_TRUSTSTORE_LOCATION_CONFIG, " /trust.pem ", "/trust.pem"),
            (SslConfigs::SSL_TRUSTSTORE_TYPE_CONFIG, "JKS", "JKS"),
            (SslConfigs::SSL_KEYSTORE_LOCATION_CONFIG, "/key.pem", "/key.pem"),
            (SslConfigs::SSL_KEYSTORE_TYPE_CONFIG, "PKCS12", "PKCS12"),
            (SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "", ""),
        ];
        for (key, value, expected) in set {
            let mut extra = vec![(*key, *value)];
            // `group.remote.assignor` cannot be set with the `classic` protocol.
            if *key == ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG {
                extra.push((ConsumerConfig::GROUP_PROTOCOL_CONFIG, "consumer"));
            }
            // A SASL protocol accepts only the `PLAIN` mechanism.
            if *key == CommonClientConfigs::SECURITY_PROTOCOL_CONFIG {
                extra.push((SaslConfigs::SASL_MECHANISM, "PLAIN"));
            }
            let config = ConsumerConfig::new(&props_with(&extra)).unwrap();
            assert_eq!(Some(*expected), config.get_string(key).unwrap(), "for {key}={value:?}");
        }

        // A key of another type fails the cast, outside the `KafkaException`
        // hierarchy as Java's `ClassCastException` is.
        let other_types: &[(&str, &str)] = &[
            (ConsumerConfig::MAX_POLL_RECORDS_CONFIG, "INT"),
            (ConsumerConfig::METADATA_MAX_AGE_CONFIG, "LONG"),
            (ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG, "BOOLEAN"),
            (ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG, "LIST"),
            (SslConfigs::SSL_KEYSTORE_KEY_CONFIG, "PASSWORD"),
            (SaslConfigs::SASL_JAAS_CONFIG, "PASSWORD"),
        ];
        let passwords = ConsumerConfig::new(&props_with(&[
            (SslConfigs::SSL_KEYSTORE_KEY_CONFIG, "secret"),
            (SaslConfigs::SASL_JAAS_CONFIG, "x required;"),
        ]))
        .unwrap();
        for (key, config_type) in other_types {
            let config = if *config_type == "PASSWORD" {
                &passwords
            } else {
                &config
            };
            match config.get_string(key) {
                Err(e @ Error::LocalIllegalArgument(_)) => {
                    assert_eq!(
                        format!("Configuration '{key}' is of type {config_type}, not STRING"),
                        e.message()
                    );
                    assert!(!e.is_kafka_error());
                },
                other => panic!("expected an IllegalArgumentError for {key}, got {other:?}"),
            }
        }

        match config.get_string("no.such.key") {
            Err(Error::Config(e)) => assert_eq!("Unknown configuration 'no.such.key'", e.message()),
            other => panic!("expected a ConfigError, got {other:?}"),
        }
    }

    /// `client.rack` is a `Type.STRING`, so `ConfigDef.parseType` trims it
    /// (Critic 96 F3); unset, it is `DEFAULT_CLIENT_RACK`, the empty string.
    #[test]
    fn test_client_rack_is_trimmed_and_defaults_to_empty() {
        assert_eq!("client.rack", ConsumerConfig::CLIENT_RACK_CONFIG);
        assert_eq!("", ConsumerConfig::DEFAULT_CLIENT_RACK);
        assert_eq!(
            ConsumerConfig::DEFAULT_CLIENT_RACK,
            ConsumerConfig::new(&props_with(&[])).unwrap().client_rack()
        );
        let padded = props_with(&[(ConsumerConfig::CLIENT_RACK_CONFIG, " rack-1 ")]);
        assert_eq!("rack-1", ConsumerConfig::new(&padded).unwrap().client_rack());
        let blank = props_with(&[(ConsumerConfig::CLIENT_RACK_CONFIG, "  ")]);
        assert_eq!("", ConsumerConfig::new(&blank).unwrap().client_rack());
    }

    /// `group.instance.id` is trimmed before Java's `NonEmptyString` check, so
    /// a blank one is a config error for that key, not the instance-id
    /// validator's `InvalidConfiguration`.
    #[test]
    fn test_blank_group_instance_id_is_config_error() {
        let props = props_with(&[
            (ConsumerConfig::GROUP_ID_CONFIG, "test-group"),
            (ConsumerConfig::GROUP_INSTANCE_ID_CONFIG, " "),
        ]);
        let err = ConsumerConfig::new(&props).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert!(
            err.message().contains(ConsumerConfig::GROUP_INSTANCE_ID_CONFIG),
            "{}",
            err.message()
        );
        assert!(err.message().contains("String must be non-empty"), "{}", err.message());

        let padded = props_with(&[(ConsumerConfig::GROUP_INSTANCE_ID_CONFIG, " inst ")]);
        assert_eq!(ConsumerConfig::new(&padded).unwrap().group_instance_id(), Some("inst"));
    }

    /// `ConfigDef.parseType` trims `group.id`, so the padding reaches neither
    /// the group id nor the generated `client.id` embedding it.
    #[test]
    fn test_group_id_is_trimmed() {
        let config = ConsumerConfig::new(&props_with(&[(ConsumerConfig::GROUP_ID_CONFIG, " g ")])).unwrap();
        assert_eq!(config.group_id(), Some("g"));
        sequence_suffix(config.client_id(), "consumer-g-");
        // A whitespace-only id trims to the empty string, kept as `Some("")`
        // so the consumer constructor rejects it as Java does.
        let config = ConsumerConfig::new(&props_with(&[(ConsumerConfig::GROUP_ID_CONFIG, " ")])).unwrap();
        assert_eq!(config.group_id(), Some(""));
    }

    // -------- Translated from `org.apache.kafka.clients.consumer.ConsumerConfigTest` --------
    //
    // Java reads values back through `AbstractConfig`'s keyed getters. Of
    // those, `getString` is translated as [`ConsumerConfig::get_string`]; the
    // others (`getList`, `getBoolean`, `getInt`, `originals()`) are not, and the
    // crate-private getters and fields stand in for them, which is why these
    // tests live in the crate rather than in `tests/consumer`. Java seeds `key.deserializer` / `value.deserializer` into
    // every property map (`ConsumerConfigTest.java:60`) because its `ConfigDef`
    // defines them with no default; Rust takes the deserializers as constructor
    // arguments to the consumer, and `ConsumerConfig` holds no deserializer state,
    // so the two keys are absent from `base_props`.
    //
    // Skipped tests:
    // - `testAppendDeserializerToConfig`, `testAppendDeserializerToConfigWithException`
    //   — exercise the Java static `appendDeserializerToConfig` helper that mutates a
    //   typed `Map<String, Object>`; there is no equivalent function to test.
    // - `testDefaultPartitionAssignor` — see `test_partition_assignment_strategy_is_unknown`.
    // - `testValidateConfigPropertiesFile` — reads `config/consumer.properties` from
    //   the Apache Kafka source tree; not part of the Rust client.
    // `testInvalidSecurityProtocol` and `testCaseInsensitiveSecurityProtocol` are
    // `test_invalid_security_protocol` and `test_case_insensitive_security_protocol`
    // above.

    /// Translated from `ConsumerConfigTest.ensureDefaultThrowOnUnsupportedStableFlagToFalse`.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#ensureDefaultThrowOnUnsupportedStableFlagToFalse"
    )]
    fn ensure_default_return_on_unsupported_stable_flag_to_false() {
        let config = ConsumerConfig::new(&base_props()).unwrap();
        assert!(!config.throw_on_fetch_stable_offset_unsupported());
    }

    /// `ConsumerConfigTest.testDefaultPartitionAssignor` is not translated: it
    /// checks Java's default `[RangeAssignor, CooperativeStickyAssignor]`, and
    /// the client-side assignors are out of scope (consumer-threading.md §20).
    /// The key is not a consumer config here: it is accepted and reported by
    /// [`ConsumerConfig::unused`].
    #[test]
    fn test_partition_assignment_strategy_is_unknown() {
        let mut props = base_props();
        props.insert(
            ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG.to_string(),
            "org.apache.kafka.clients.consumer.RangeAssignor".to_string(),
        );
        let config = ConsumerConfig::new(&props).unwrap();
        match config.get_string(ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG) {
            Err(Error::Config(e)) => {
                assert_eq!("Unknown configuration 'partition.assignment.strategy'", e.message())
            },
            other => panic!("expected a ConfigError, got {other:?}"),
        }
    }

    /// `security.providers` names JSSE `SecurityProviderCreator` classes, which
    /// have no rustls counterpart: the key is not defined.
    #[test]
    fn test_security_providers_is_unknown() {
        let mut props = base_props();
        props.insert("security.providers".to_string(), "my.Provider".to_string());
        let config = ConsumerConfig::new(&props).unwrap();
        match config.get_string("security.providers") {
            Err(Error::Config(e)) => assert_eq!("Unknown configuration 'security.providers'", e.message()),
            other => panic!("expected a ConfigError, got {other:?}"),
        }
    }

    /// Translated from `ConsumerConfigTest.testOverrideClientId`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testOverrideClientId")]
    fn test_override_client_id() {
        let mut props = base_props();
        props.insert(ConsumerConfig::GROUP_ID_CONFIG.to_string(), "test-group".to_string());
        let config = ConsumerConfig::new(&props).unwrap();
        assert!(!config.client_id().is_empty());
    }

    /// Translated from `ConsumerConfigTest.testInvalidGroupInstanceId`: the
    /// `group.instance.id` empty-string check is enforced.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testInvalidGroupInstanceId")]
    fn test_invalid_group_instance_id() {
        let mut props = base_props();
        props.insert(ConsumerConfig::GROUP_INSTANCE_ID_CONFIG.to_string(), String::new());
        let err = ConsumerConfig::new(&props).unwrap_err();
        assert!(
            err.message().contains(ConsumerConfig::GROUP_INSTANCE_ID_CONFIG),
            "error message should mention the failing config key, got: {}",
            err.message()
        );
    }

    /// Translated from `ConsumerConfigTest.testDefaultConsumerGroupConfig`.
    ///
    /// Java's default for `group.protocol` is `"classic"`
    /// (`ConsumerConfig.DEFAULT_GROUP_PROTOCOL`); `group.remote.assignor` is `null`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testDefaultConsumerGroupConfig")]
    fn test_default_consumer_group_config() {
        let config = ConsumerConfig::new(&base_props()).unwrap();
        assert_eq!(config.group_protocol(), "classic");
        assert_eq!(config.group_remote_assignor(), None);
    }

    /// Translated from `ConsumerConfigTest.testRemoteAssignorConfig`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testRemoteAssignorConfig")]
    fn test_remote_assignor_config() {
        let mut props = base_props();
        let remote_assignor_name = "SomeAssignor";
        let protocol = "consumer";
        props.insert(
            ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG.to_string(),
            remote_assignor_name.to_string(),
        );
        props.insert(ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(), protocol.to_string());
        let config = ConsumerConfig::new(&props).unwrap();
        assert_eq!(config.group_protocol(), protocol);
        assert_eq!(config.group_remote_assignor(), Some(remote_assignor_name));
    }

    /// Translated from `ConsumerConfigTest.testDefaultMetadataRecoveryStrategy`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testDefaultMetadataRecoveryStrategy")]
    fn test_default_metadata_recovery_strategy() {
        let config = ConsumerConfig::new(&base_props()).unwrap();
        assert_eq!(config.metadata_recovery_strategy(), "rebootstrap");
    }

    /// Translated from `ConsumerConfigTest.testInvalidMetadataRecoveryStrategy`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testInvalidMetadataRecoveryStrategy")]
    fn test_invalid_metadata_recovery_strategy() {
        let mut props = base_props();
        props.insert(
            CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG.to_string(),
            "abc".to_string(),
        );
        let err = ConsumerConfig::new(&props).unwrap_err();
        assert!(
            err.message().contains(CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG),
            "error message should mention the failing config key, got: {}",
            err.message()
        );
    }

    /// Translated from `ConsumerConfigTest.testProtocolConfigValidation`.
    ///
    /// Java uses `@ParameterizedTest @CsvSource(...)`; the same inputs are iterated.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testProtocolConfigValidation")]
    fn test_protocol_config_validation() {
        let cases: &[(&str, bool)] = &[
            ("consumer", true),
            ("classic", true),
            ("Consumer", true),
            ("Classic", true),
            ("invalid", false),
        ];
        for (protocol, is_valid) in cases {
            let mut props = base_props();
            props.insert(ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(), protocol.to_string());
            if *is_valid {
                let config = ConsumerConfig::new(&props).unwrap();
                assert_eq!(config.group_protocol(), *protocol);
            } else {
                let err = ConsumerConfig::new(&props).unwrap_err();
                assert!(
                    err.message().contains(ConsumerConfig::GROUP_PROTOCOL_CONFIG),
                    "case {protocol}: {}",
                    err.message()
                );
            }
        }
    }
    /// Translated from `ConsumerConfigTest.testOverrideEnableAutoCommit`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testOverrideEnableAutoCommit")]
    fn test_override_enable_auto_commit() {
        let enable_auto_commit =
            |config: &ConsumerConfig| config.get_boolean(ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG).unwrap();
        // Verify that our default properties (no 'enable.auto.commit' or 'group.id') are valid.
        let mut props = base_props();
        let config = ConsumerConfig::new(&props).unwrap();
        assert_eq!(Some(false), enable_auto_commit(&config));
        assert!(!config.enable_auto_commit());

        // Verify that explicitly disabling 'enable.auto.commit' still works.
        props.insert(ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG.to_string(), "false".to_string());
        assert_eq!(Some(false), enable_auto_commit(&ConsumerConfig::new(&props).unwrap()));

        // Verify that enabling 'enable.auto.commit' but without 'group.id' fails.
        props.insert(ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG.to_string(), "true".to_string());
        let error = ConsumerConfig::new(&props).unwrap_err();
        assert!(error.is_invalid_configuration_error(), "{error:?}");
        assert_eq!(
            "enable.auto.commit cannot be set to true when default group id (null) is used.",
            error.message()
        );

        // Verify that then adding 'group.id' to the mix allows it to pass OK.
        props.insert(ConsumerConfig::GROUP_ID_CONFIG.to_string(), "test-group".to_string());
        let config = ConsumerConfig::new(&props).unwrap();
        assert_eq!(Some(true), enable_auto_commit(&config));
        assert!(config.enable_auto_commit());

        // Now remove the 'enable.auto.commit' flag and verify that it is set to true (the default).
        props.remove(ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG);
        assert_eq!(Some(true), enable_auto_commit(&ConsumerConfig::new(&props).unwrap()));
    }

    /// Translated from `ConsumerConfigTest.testRemoteAssignorWithClassicGroupProtocol`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testRemoteAssignorWithClassicGroupProtocol")]
    fn test_remote_assignor_with_classic_group_protocol() {
        let props = props_with(&[(ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG, "SomeAssignor")]);
        let error = ConsumerConfig::new(&props).unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error:?}");
        assert!(
            error
                .message()
                .contains("group.remote.assignor cannot be set when group.protocol=CLASSIC"),
            "{}",
            error.message()
        );
    }

    /// Translated from `ConsumerConfigTest.testUnsupportedConfigsWithConsumerGroupProtocol`.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.ConsumerConfigTest#testUnsupportedConfigsWithConsumerGroupProtocol"
    )]
    fn test_unsupported_configs_with_consumer_group_protocol() {
        for (config_name, value) in [
            (ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "RoundRobinAssignor"),
            (ConsumerConfig::HEARTBEAT_INTERVAL_MS_CONFIG, "1000"),
            (ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG, "30000"),
        ] {
            let props = props_with(&[
                (ConsumerConfig::GROUP_PROTOCOL_CONFIG, "CONSUMER"),
                (config_name, value),
            ]);
            let error = ConsumerConfig::new(&props).unwrap_err();
            assert!(matches!(error, Error::Config(_)), "{error:?}");
            assert_eq!(
                format!("{config_name} cannot be set when group.protocol=CONSUMER"),
                error.message()
            );
        }
    }

    /// The share-consumer keys are not supported by either group protocol of
    /// `KafkaConsumer` (`CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS`,
    /// `CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS`); every unsupported key set is
    /// listed, in Java's order, and a blank value is not "set".
    #[test]
    fn test_unsupported_configs_are_listed_in_order() {
        let props = props_with(&[
            (ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG, "record_limit"),
            (ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit"),
            (ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG, " "),
        ]);
        assert_eq!(
            "share.acknowledgement.mode, share.acquire.mode cannot be set when group.protocol=CLASSIC",
            ConsumerConfig::new(&props).unwrap_err().message()
        );
        let mut props = props;
        props.insert(ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(), "consumer".to_string());
        props.insert(ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG.to_string(), "30000".to_string());
        assert_eq!(
            "session.timeout.ms, share.acknowledgement.mode, share.acquire.mode cannot be set when \
             group.protocol=CONSUMER",
            ConsumerConfig::new(&props).unwrap_err().message()
        );
        // The checked keys are read through Java's `RecordingMap`, so they are used.
        let config = ConsumerConfig::new(&props_with(&[(ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG, " ")])).unwrap();
        assert!(!config.unused().contains(ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG));
    }

    /// Java's `ShareAcquireMode.Validator` and `ShareAcknowledgementMode.Validator`
    /// reject a blank value at parse time, which the unsupported-config check
    /// would skip, and an unknown one, with their messages; the acquire mode
    /// is matched case-insensitively, the acknowledgement mode exactly.
    #[test]
    fn test_share_mode_validators() {
        for group_protocol in ["classic", "consumer"] {
            for (config_name, value, expected) in [
                (
                    ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG,
                    "",
                    "Invalid value  for configuration share.acquire.mode: Invalid value `` for configuration \
                     share.acquire.mode. The value must either be 'batch_optimized' or 'record_limit'.",
                ),
                (
                    ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG,
                    "  ",
                    "Invalid value  for configuration share.acquire.mode: Invalid value `` for configuration \
                     share.acquire.mode. The value must either be 'batch_optimized' or 'record_limit'.",
                ),
                (
                    ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG,
                    "invalid",
                    "Invalid value invalid for configuration share.acquire.mode: Invalid value `invalid` for \
                     configuration share.acquire.mode. The value must either be 'batch_optimized' or 'record_limit'.",
                ),
                (
                    ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
                    "",
                    "Invalid value  for configuration share.acknowledgement.mode: Invalid value `` for \
                     configuration share.acknowledgement.mode. The value must either be 'implicit' or 'explicit'.",
                ),
                (
                    ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG,
                    "IMPLICIT",
                    "Invalid value IMPLICIT for configuration share.acknowledgement.mode: Invalid value `IMPLICIT` \
                     for configuration share.acknowledgement.mode. The value must either be 'implicit' or \
                     'explicit'.",
                ),
            ] {
                let error = ConsumerConfig::new(&props_with(&[
                    (ConsumerConfig::GROUP_PROTOCOL_CONFIG, group_protocol),
                    (config_name, value),
                ]))
                .unwrap_err();
                assert!(matches!(error, Error::Config(_)), "{error:?}");
                assert_eq!(expected, error.message(), "{group_protocol} {config_name}={value:?}");
            }
        }
        // A valid value passes the validator (and is then rejected as unsupported).
        for value in ["record_limit", "Batch_Optimized", "BATCH_OPTIMIZED"] {
            assert_eq!(
                "share.acquire.mode cannot be set when group.protocol=CLASSIC",
                ConsumerConfig::new(&props_with(&[(ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG, value)]))
                    .unwrap_err()
                    .message()
            );
        }
    }

    /// The `group.protocol` validator is Java's
    /// `CaseInsensitiveValidString.in(CLASSIC, CONSUMER)`, with its message,
    /// which lists the values in the order of Java's `HashSet`.
    #[test]
    fn test_group_protocol_validator_message() {
        let error =
            ConsumerConfig::new(&props_with(&[(ConsumerConfig::GROUP_PROTOCOL_CONFIG, "invalid")])).unwrap_err();
        assert_eq!(
            "Invalid value invalid for configuration group.protocol: String must be one of (case insensitive): \
             CONSUMER, CLASSIC",
            error.message()
        );
    }

    /// Java's `atLeast` validators (`ConsumerConfig.java:440-745`), each with
    /// `ConfigDef.Range`'s message.
    #[test]
    fn test_at_least_validators() {
        let cases: &[(&str, &str, &str)] = &[
            (ConsumerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::METADATA_MAX_AGE_CONFIG, "-1", "0"),
            (ConsumerConfig::AUTO_COMMIT_INTERVAL_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::MAX_PARTITION_FETCH_BYTES_CONFIG, "-1", "0"),
            (ConsumerConfig::SEND_BUFFER_CONFIG, "-2", "-1"),
            (ConsumerConfig::RECEIVE_BUFFER_CONFIG, "-2", "-1"),
            (ConsumerConfig::FETCH_MIN_BYTES_CONFIG, "-1", "0"),
            (ConsumerConfig::FETCH_MAX_BYTES_CONFIG, "-1", "0"),
            (ConsumerConfig::FETCH_MAX_WAIT_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::RECONNECT_BACKOFF_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::RECONNECT_BACKOFF_MAX_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::RETRY_BACKOFF_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::RETRY_BACKOFF_MAX_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::METRICS_SAMPLE_WINDOW_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::METRICS_NUM_SAMPLES_CONFIG, "0", "1"),
            (ConsumerConfig::REQUEST_TIMEOUT_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::DEFAULT_API_TIMEOUT_MS_CONFIG, "-1", "0"),
            (ConsumerConfig::MAX_POLL_RECORDS_CONFIG, "0", "1"),
            (ConsumerConfig::MAX_POLL_INTERVAL_MS_CONFIG, "0", "1"),
            (CommonClientConfigs::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "-1", "0"),
        ];
        for (key, value, bound) in cases {
            let error = ConsumerConfig::new(&props_with(&[(key, value)])).unwrap_err();
            assert!(matches!(error, Error::Config(_)), "{key}: {error:?}");
            assert_eq!(
                format!("Invalid value {value} for configuration {key}: Value must be at least {bound}"),
                error.message()
            );
            // The bound itself is accepted.
            ConsumerConfig::new(&props_with(&[(key, bound)])).unwrap_or_else(|e| panic!("{key}={bound}: {e:?}"));
        }
    }

    /// `isolation.level` is Java's case-sensitive `ValidString.in`.
    #[test]
    fn test_isolation_level_is_case_sensitive() {
        let error = ConsumerConfig::new(&props_with(&[(ConsumerConfig::ISOLATION_LEVEL_CONFIG, "READ_COMMITTED")]))
            .unwrap_err();
        assert_eq!(
            "Invalid value READ_COMMITTED for configuration isolation.level: String must be one of: read_committed, \
             read_uncommitted",
            error.message()
        );
    }

    /// The `connections.max.idle.ms` warning does not fail the configuration.
    #[test]
    fn test_connections_max_idle_ms_lower_than_max_poll_interval_ms_only_warns() {
        let config = ConsumerConfig::new(&props_with(&[
            (ConsumerConfig::CONNECTIONS_MAX_IDLE_MS_CONFIG, "1000"),
            (ConsumerConfig::MAX_POLL_INTERVAL_MS_CONFIG, "2000"),
        ]))
        .unwrap();
        assert_eq!(1000, config.connections_max_idle_ms);
    }

    /// `AbstractConfig`'s typed getters answer every key's `ConfigDef` type.
    #[test]
    fn test_typed_getters() {
        let config = ConsumerConfig::new(&props_with(&[
            (ConsumerConfig::GROUP_ID_CONFIG, "g"),
            (ConsumerConfig::MAX_POLL_RECORDS_CONFIG, "7"),
        ]))
        .unwrap();
        assert_eq!(Some(7), config.get_int(ConsumerConfig::MAX_POLL_RECORDS_CONFIG).unwrap());
        assert_eq!(
            Some(540_000),
            config.get_long(ConsumerConfig::CONNECTIONS_MAX_IDLE_MS_CONFIG).unwrap()
        );
        assert_eq!(Some(true), config.get_boolean(ConsumerConfig::CHECK_CRCS_CONFIG).unwrap());
        assert_eq!(
            Some(&["localhost:9092".to_string()][..]),
            config.get_list(ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG).unwrap()
        );
        assert_eq!(Some("g"), config.get_string(ConsumerConfig::GROUP_ID_CONFIG).unwrap());
        assert_eq!(
            "Configuration 'max.poll.records' is of type INT, not LONG",
            config.get_long(ConsumerConfig::MAX_POLL_RECORDS_CONFIG).unwrap_err().message()
        );
        assert_eq!(
            "Configuration 'check.crcs' is of type BOOLEAN, not SHORT",
            config.get_short(ConsumerConfig::CHECK_CRCS_CONFIG).unwrap_err().message()
        );
        assert_eq!(
            "Configuration 'group.id' is of type STRING, not DOUBLE",
            config.get_double(ConsumerConfig::GROUP_ID_CONFIG).unwrap_err().message()
        );
        assert_eq!(
            "Unknown configuration 'no.such.key'",
            config.get_int("no.such.key").unwrap_err().message()
        );
    }

    /// `configNames`.
    #[test]
    fn test_config_names() {
        let names = ConsumerConfig::config_names();
        for key in [
            ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG,
            ConsumerConfig::GROUP_PROTOCOL_CONFIG,
            ConsumerConfig::THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED,
            ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG,
            SaslConfigs::SASL_MECHANISM,
            SslConfigs::SSL_KEYSTORE_TYPE_CONFIG,
            "ssl.cipher.suites",
        ] {
            assert!(names.contains(key), "{key}");
        }
        for key in [
            ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "security.providers",
            "config.providers",
            "enable.metrics.push",
            "ssl.keystore.password",
            "ssl.key.password",
            "ssl.truststore.password",
            "key.deserializer",
            "metric.reporters",
            "interceptor.classes",
        ] {
            assert!(!names.contains(key), "{key}");
        }
    }

    /// `unused` reports the supplied keys nobody read; the getters, `ignore`
    /// and the consumer's own reads mark keys used.
    #[test]
    fn test_unused_and_ignore() {
        let config = ConsumerConfig::new(&props_with(&[
            (ConsumerConfig::GROUP_ID_CONFIG, "g"),
            (ConsumerConfig::FETCH_MIN_BYTES_CONFIG, "2"),
            (SslConfigs::SSL_KEYSTORE_TYPE_CONFIG, "PKCS12"),
            ("unknown.key", "x"),
        ]))
        .unwrap();
        // `postProcessParsedConfig` read `group.id`; nothing else yet.
        assert_eq!(
            HashSet::from([
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                ConsumerConfig::FETCH_MIN_BYTES_CONFIG.to_string(),
                SslConfigs::SSL_KEYSTORE_TYPE_CONFIG.to_string(),
                "unknown.key".to_string(),
            ]),
            config.unused()
        );
        config.get_int(ConsumerConfig::FETCH_MIN_BYTES_CONFIG).unwrap();
        config.ignore("unknown.key");
        // The consumer's reads: the `ssl.*` keys only for an SSL protocol.
        config.mark_read_keys_used();
        assert_eq!(
            HashSet::from([SslConfigs::SSL_KEYSTORE_TYPE_CONFIG.to_string()]),
            config.unused()
        );
        config.log_unused();
    }

    /// `enable.metrics.push` is not defined until KIP-714 client telemetry is
    /// implemented, so a value set for it is only reported as unused.
    #[test]
    fn test_enable_metrics_push_is_unused() {
        let config = ConsumerConfig::new(&props_with(&[("enable.metrics.push", "true")])).unwrap();
        config.mark_read_keys_used();
        assert_eq!(HashSet::from(["enable.metrics.push".to_string()]), config.unused());
        assert_eq!(
            "Unknown configuration 'enable.metrics.push'",
            config.get_boolean("enable.metrics.push").unwrap_err().message()
        );
    }

    /// Java's `equals` / `hashCode` compare the originals.
    #[test]
    fn test_equals_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        let hash = |config: &ConsumerConfig| {
            let mut hasher = DefaultHasher::new();
            config.hash(&mut hasher);
            hasher.finish()
        };
        let a = ConsumerConfig::new(&props_with(&[(ConsumerConfig::CLIENT_ID_CONFIG, "c")])).unwrap();
        let b = ConsumerConfig::new(&props_with(&[(ConsumerConfig::CLIENT_ID_CONFIG, "c")])).unwrap();
        let c = ConsumerConfig::new(&props_with(&[(ConsumerConfig::CLIENT_ID_CONFIG, "d")])).unwrap();
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));
        assert_ne!(a, c);
    }
}
