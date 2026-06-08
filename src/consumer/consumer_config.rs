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
//! Translated from `org.apache.kafka.clients.consumer.ConsumerConfig`.
//!
//! This is a Rust struct with named fields rather than the Java `ConfigDef`
//! reflection framework. The config-key string constants match Java's, and
//! the default values mirror Java's `ConfigDef.define(...)` arguments.
//!
//! Per `consumer-threading.md` §20, Milestone-8 only supports the KIP-848
//! `consumer` group protocol. The default still matches Java
//! (`classic`) so users opt in explicitly; classic-protocol-only fields
//! such as `partition.assignment.strategy` are accepted silently per §20.
//!
//! Runtime cross-field validation (`postProcessParsedConfig` in Java) is
//! out of scope for Phase 1 — it lives wherever `AsyncKafkaConsumer`
//! constructs its config.

use std::collections::HashMap;

use log::warn;

use crate::common::KafkaError;
use crate::common::config::sasl_configs;
use crate::common::config::ssl_configs;
use crate::common::config::{SaslConfig, SslConfig};
use crate::common::security::SecurityProtocol;
use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;

/// Configuration for the Kafka Consumer.
///
/// Documentation for the underlying keys can be found in the
/// [Kafka documentation](http://kafka.apache.org/documentation.html#consumerconfigs).
///
/// Corresponds to `org.apache.kafka.clients.consumer.ConsumerConfig`.
#[derive(Clone, Debug)]
pub struct ConsumerConfig {
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
    /// `session.timeout.ms` — group session timeout.
    pub(crate) session_timeout_ms: i32,
    /// `heartbeat.interval.ms` — heartbeat interval inside the session.
    pub(crate) heartbeat_interval_ms: i32,

    // --- Connection ---
    /// `bootstrap.servers` — initial connection list.
    pub(crate) bootstrap_servers: Vec<String>,
    /// `client.dns.lookup` — DNS lookup behavior.
    pub(crate) client_dns_lookup: String,
    /// `client.id` — the client identifier. Empty by default.
    pub(crate) client_id: String,
    /// `client.rack` — the rack identifier for rack-aware fetching.
    pub(crate) client_rack: String,

    // --- Commit ---
    /// `enable.auto.commit` — periodically commit offsets in the background.
    pub(crate) enable_auto_commit: bool,
    /// `auto.commit.interval.ms` — frequency of auto-commits.
    pub(crate) auto_commit_interval_ms: i32,

    // --- Assignment / Offsets ---
    /// `partition.assignment.strategy` — list of class names for classic
    /// protocol assignors. Accepted silently per scope.
    pub(crate) partition_assignment_strategy: Vec<String>,
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
    /// `enable.metrics.push`
    pub(crate) enable_metrics_push: bool,
    /// `metrics.sample.window.ms`
    pub(crate) metrics_sample_window_ms: i64,
    /// `metrics.num.samples`
    pub(crate) metrics_num_samples: i32,
    /// `metrics.recording.level`
    pub(crate) metrics_recording_level: String,
    /// `metric.reporters`
    pub(crate) metric_reporter_classes: Vec<String>,

    // --- Deserializers ---
    /// `key.deserializer`
    pub(crate) key_deserializer_class: Option<String>,
    /// `value.deserializer`
    pub(crate) value_deserializer_class: Option<String>,

    // --- Interceptors ---
    /// `interceptor.classes`
    pub(crate) interceptor_classes: Vec<String>,

    // --- Share consumer (accepted silently per scope §20) ---
    /// `share.acknowledgement.mode`
    pub(crate) share_acknowledgement_mode: String,
    /// `share.acquire.mode`
    pub(crate) share_acquire_mode: String,

    // --- Security ---
    /// `security.providers`
    pub(crate) security_providers: Option<String>,
    /// `security.protocol` - Protocol used to communicate with brokers.
    /// Default: `SecurityProtocol::Plaintext`.
    pub(crate) security_protocol: SecurityProtocol,

    /// SASL configuration (mechanism, JAAS config, credentials).
    pub(crate) sasl_config: SaslConfig,

    /// SSL/TLS configuration.
    pub(crate) ssl_config: SslConfig,

    // --- Config providers ---
    /// `config.providers`
    pub(crate) config_providers: Vec<String>,
}

impl Default for ConsumerConfig {
    /// Default values match Java's `ConfigDef.define(...)` second argument
    /// for every key.
    fn default() -> Self {
        Self {
            group_id: None,
            group_instance_id: None,
            group_protocol: "classic".to_string(),
            group_remote_assignor: None,

            max_poll_records: 500,
            max_poll_interval_ms: 300_000,
            session_timeout_ms: 45_000,
            heartbeat_interval_ms: 3_000,

            bootstrap_servers: Vec::new(),
            client_dns_lookup: "use_all_dns_ips".to_string(),
            client_id: String::new(),
            client_rack: String::new(),

            enable_auto_commit: true,
            auto_commit_interval_ms: 5_000,

            partition_assignment_strategy: Vec::new(),
            auto_offset_reset: AutoOffsetResetStrategy::LATEST.name(),

            fetch_min_bytes: 1,
            fetch_max_bytes: 50 * 1024 * 1024,
            fetch_max_wait_ms: 500,
            max_partition_fetch_bytes: 1024 * 1024,
            check_crcs: true,

            send_buffer_bytes: 128 * 1024,
            receive_buffer_bytes: 64 * 1024,
            socket_connection_setup_timeout_ms: 10_000,
            socket_connection_setup_timeout_max_ms: 30_000,
            connections_max_idle_ms: 9 * 60 * 1000,

            reconnect_backoff_ms: 50,
            reconnect_backoff_max_ms: 1_000,
            retry_backoff_ms: 100,
            retry_backoff_max_ms: 1_000,

            request_timeout_ms: 30_000,
            default_api_timeout_ms: 60_000,

            metadata_max_age_ms: 5 * 60 * 1000,
            metadata_recovery_strategy: "rebootstrap".to_string(),
            metadata_recovery_rebootstrap_trigger_ms: 5 * 60 * 1000,

            exclude_internal_topics: true,
            throw_on_fetch_stable_offset_unsupported: false,
            isolation_level: "read_uncommitted".to_string(),
            allow_auto_create_topics: true,

            enable_metrics_push: true,
            metrics_sample_window_ms: 30_000,
            metrics_num_samples: 2,
            metrics_recording_level: "INFO".to_string(),
            metric_reporter_classes: Vec::new(),

            key_deserializer_class: None,
            value_deserializer_class: None,

            interceptor_classes: Vec::new(),

            share_acknowledgement_mode: "implicit".to_string(),
            share_acquire_mode: "batch_optimized".to_string(),

            security_providers: None,
            security_protocol: SecurityProtocol::Plaintext,
            sasl_config: SaslConfig::default(),
            ssl_config: SslConfig::default(),

            config_providers: Vec::new(),
        }
    }
}

impl ConsumerConfig {
    /// Creates a new `ConsumerConfig` with default values and the given
    /// `bootstrap.servers` list.
    pub fn new(bootstrap_servers: Vec<String>) -> Self {
        Self { bootstrap_servers, ..Self::default() }
    }

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
    /// Config key: `client.dns.lookup`.
    pub const CLIENT_DNS_LOOKUP_CONFIG: &'static str = "client.dns.lookup";
    /// Config key: `client.id`.
    pub const CLIENT_ID_CONFIG: &'static str = "client.id";
    /// Config key: `client.rack`.
    pub const CLIENT_RACK_CONFIG: &'static str = "client.rack";

    /// Config key: `enable.auto.commit`.
    pub const ENABLE_AUTO_COMMIT_CONFIG: &'static str = "enable.auto.commit";
    /// Config key: `auto.commit.interval.ms`.
    pub const AUTO_COMMIT_INTERVAL_MS_CONFIG: &'static str = "auto.commit.interval.ms";

    /// Config key: `partition.assignment.strategy`.
    pub const PARTITION_ASSIGNMENT_STRATEGY_CONFIG: &'static str = "partition.assignment.strategy";
    /// Config key: `auto.offset.reset`.
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

    /// Config key: `request.timeout.ms`.
    pub const REQUEST_TIMEOUT_MS_CONFIG: &'static str = "request.timeout.ms";
    /// Config key: `default.api.timeout.ms`.
    pub const DEFAULT_API_TIMEOUT_MS_CONFIG: &'static str = "default.api.timeout.ms";

    /// Config key: `metadata.max.age.ms`.
    pub const METADATA_MAX_AGE_CONFIG: &'static str = "metadata.max.age.ms";
    /// Config key: `metadata.recovery.strategy`.
    pub const METADATA_RECOVERY_STRATEGY_CONFIG: &'static str = "metadata.recovery.strategy";
    /// Config key: `metadata.recovery.rebootstrap.trigger.ms`.
    pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG: &'static str =
        "metadata.recovery.rebootstrap.trigger.ms";

    /// Config key: `exclude.internal.topics`.
    pub const EXCLUDE_INTERNAL_TOPICS_CONFIG: &'static str = "exclude.internal.topics";
    /// Default value of `exclude.internal.topics`.
    pub const DEFAULT_EXCLUDE_INTERNAL_TOPICS: bool = true;
    /// Config key: `internal.throw.on.fetch.stable.offset.unsupported`.
    pub const THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED: &'static str =
        "internal.throw.on.fetch.stable.offset.unsupported";
    /// Config key: `isolation.level`.
    pub const ISOLATION_LEVEL_CONFIG: &'static str = "isolation.level";
    /// Config key: `allow.auto.create.topics`.
    pub const ALLOW_AUTO_CREATE_TOPICS_CONFIG: &'static str = "allow.auto.create.topics";
    /// Default value of `allow.auto.create.topics`.
    pub const DEFAULT_ALLOW_AUTO_CREATE_TOPICS: bool = true;

    /// Config key: `enable.metrics.push`.
    pub const ENABLE_METRICS_PUSH_CONFIG: &'static str = "enable.metrics.push";
    /// Config key: `metrics.sample.window.ms`.
    pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &'static str = "metrics.sample.window.ms";
    /// Config key: `metrics.num.samples`.
    pub const METRICS_NUM_SAMPLES_CONFIG: &'static str = "metrics.num.samples";
    /// Config key: `metrics.recording.level`.
    pub const METRICS_RECORDING_LEVEL_CONFIG: &'static str = "metrics.recording.level";
    /// Config key: `metric.reporters`.
    pub const METRIC_REPORTER_CLASSES_CONFIG: &'static str = "metric.reporters";

    /// Config key: `key.deserializer`.
    pub const KEY_DESERIALIZER_CLASS_CONFIG: &'static str = "key.deserializer";
    /// Config key: `value.deserializer`.
    pub const VALUE_DESERIALIZER_CLASS_CONFIG: &'static str = "value.deserializer";

    /// Config key: `interceptor.classes`.
    pub const INTERCEPTOR_CLASSES_CONFIG: &'static str = "interceptor.classes";

    /// Config key: `share.acknowledgement.mode`.
    pub const SHARE_ACKNOWLEDGEMENT_MODE_CONFIG: &'static str = "share.acknowledgement.mode";
    /// Config key: `share.acquire.mode`.
    pub const SHARE_ACQUIRE_MODE_CONFIG: &'static str = "share.acquire.mode";

    /// Config key: `security.providers`.
    pub const SECURITY_PROVIDERS_CONFIG: &'static str = "security.providers";
    /// Config key: `security.protocol`.
    pub const SECURITY_PROTOCOL_CONFIG: &'static str = "security.protocol";
    /// Config key: `sasl.mechanism`.
    pub const SASL_MECHANISM_CONFIG: &'static str = sasl_configs::SASL_MECHANISM;
    /// Config key: `sasl.jaas.config`.
    pub const SASL_JAAS_CONFIG: &'static str = sasl_configs::SASL_JAAS_CONFIG;

    /// Config key: `config.providers`.
    pub const CONFIG_PROVIDERS_CONFIG: &'static str = "config.providers";

    // -------- Getters --------

    /// `bootstrap.servers`.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }
    /// `client.id`.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    /// `group.id`, if any.
    pub fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }
    /// `group.instance.id`, if any.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }
    /// `group.protocol`.
    pub fn group_protocol(&self) -> &str {
        &self.group_protocol
    }
    /// `group.remote.assignor`, if any.
    pub fn group_remote_assignor(&self) -> Option<&str> {
        self.group_remote_assignor.as_deref()
    }
    /// `max.poll.records`.
    pub fn max_poll_records(&self) -> i32 {
        self.max_poll_records
    }
    /// `max.poll.interval.ms`.
    pub fn max_poll_interval_ms(&self) -> i32 {
        self.max_poll_interval_ms
    }
    /// `session.timeout.ms`.
    pub fn session_timeout_ms(&self) -> i32 {
        self.session_timeout_ms
    }
    /// `heartbeat.interval.ms`.
    pub fn heartbeat_interval_ms(&self) -> i32 {
        self.heartbeat_interval_ms
    }
    /// `enable.auto.commit`.
    pub fn enable_auto_commit(&self) -> bool {
        self.enable_auto_commit
    }
    /// `auto.commit.interval.ms`.
    pub fn auto_commit_interval_ms(&self) -> i32 {
        self.auto_commit_interval_ms
    }
    /// `auto.offset.reset`.
    pub fn auto_offset_reset(&self) -> &str {
        &self.auto_offset_reset
    }
    /// `partition.assignment.strategy`.
    pub fn partition_assignment_strategy(&self) -> &[String] {
        &self.partition_assignment_strategy
    }
    /// `key.deserializer`.
    pub fn key_deserializer_class(&self) -> Option<&str> {
        self.key_deserializer_class.as_deref()
    }
    /// `value.deserializer`.
    pub fn value_deserializer_class(&self) -> Option<&str> {
        self.value_deserializer_class.as_deref()
    }
    /// `security.protocol` - the protocol name (e.g. `"PLAINTEXT"`, `"SASL_SSL"`).
    pub fn security_protocol(&self) -> &str {
        self.security_protocol.name()
    }
    /// `metadata.recovery.strategy`.
    pub fn metadata_recovery_strategy(&self) -> &str {
        &self.metadata_recovery_strategy
    }
    /// `internal.throw.on.fetch.stable.offset.unsupported`.
    pub fn throw_on_fetch_stable_offset_unsupported(&self) -> bool {
        self.throw_on_fetch_stable_offset_unsupported
    }
    /// `request.timeout.ms`.
    pub fn request_timeout_ms(&self) -> i32 {
        self.request_timeout_ms
    }
    /// `retry.backoff.ms`.
    pub fn retry_backoff_ms(&self) -> i64 {
        self.retry_backoff_ms
    }
    /// `retry.backoff.max.ms`.
    pub fn retry_backoff_max_ms(&self) -> i64 {
        self.retry_backoff_max_ms
    }

    // -------- Fluent setters --------

    /// Set `bootstrap.servers`.
    pub fn with_bootstrap_servers(mut self, bootstrap_servers: Vec<String>) -> Self {
        self.bootstrap_servers = bootstrap_servers;
        self
    }
    /// Set `client.id`.
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }
    /// Set `group.id`.
    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }
    /// Set `group.protocol`.
    pub fn with_group_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.group_protocol = protocol.into();
        self
    }
    /// Set `auto.offset.reset`.
    pub fn with_auto_offset_reset(mut self, value: impl Into<String>) -> Self {
        self.auto_offset_reset = value.into();
        self
    }
    /// Set `enable.auto.commit`.
    pub fn with_enable_auto_commit(mut self, value: bool) -> Self {
        self.enable_auto_commit = value;
        self
    }
    /// Set `key.deserializer`.
    pub fn with_key_deserializer_class(mut self, class: impl Into<String>) -> Self {
        self.key_deserializer_class = Some(class.into());
        self
    }
    /// Set `value.deserializer`.
    pub fn with_value_deserializer_class(mut self, class: impl Into<String>) -> Self {
        self.value_deserializer_class = Some(class.into());
        self
    }

    /// Set `request.timeout.ms`.
    pub fn with_request_timeout_ms(mut self, value: i32) -> Self {
        self.request_timeout_ms = value;
        self
    }

    /// Set `retry.backoff.ms`.
    pub fn with_retry_backoff_ms(mut self, value: i64) -> Self {
        self.retry_backoff_ms = value;
        self
    }

    /// Parses a string-typed property map into a typed `ConsumerConfig`.
    ///
    /// Mirrors Java's `new ConsumerConfig(Map<String, String>)`. Unknown keys
    /// are logged as warnings and ignored, matching Java's behavior.
    ///
    /// Construction-time validation matches the Java `ConfigDef` validators
    /// that we have translated so far. Cross-field validation (e.g.
    /// `partition.assignment.strategy` being forbidden under
    /// `group.protocol=consumer`) is deferred until a later phase per
    /// `consumer-threading.md` §20.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] if a value cannot be parsed
    /// for its expected type, or fails its validator.
    pub fn from_properties(props: &HashMap<String, String>) -> Result<Self, KafkaError> {
        // NOTE: 16 of Java's per-field `atLeast(..)` numeric validators
        // (ConsumerConfig.java lines 415-710) are intentionally deferred to
        // Phase 11, when `post_process_parsed_config` is translated. Until
        // that lands, negative / out-of-range values are silently accepted
        // for the following keys (Java line numbers in parentheses):
        //   - `max.poll.interval.ms`                          atLeast(1)  (632)
        //   - `metadata.max.age.ms`                           atLeast(0)  (455)
        //   - `auto.commit.interval.ms`                       atLeast(0)  (466)
        //   - `max.partition.fetch.bytes`                     atLeast(0)  (482)
        //   - `send.buffer.bytes`         atLeast(SEND_BUFFER_LOWER_BOUND) (488)
        //   - `receive.buffer.bytes`   atLeast(RECEIVE_BUFFER_LOWER_BOUND) (494)
        //   - `fetch.min.bytes`                               atLeast(0)  (500)
        //   - `fetch.max.bytes`                               atLeast(0)  (506)
        //   - `fetch.max.wait.ms`                             atLeast(0)  (512)
        //   - `reconnect.backoff.ms`                          atLeast(0L) (518)
        //   - `reconnect.backoff.max.ms`                      atLeast(0L) (524)
        //   - `retry.backoff.ms`                              atLeast(0L) (530)
        //   - `retry.backoff.max.ms`                          atLeast(0L) (536)
        //   - `request.timeout.ms`                            atLeast(0)  (590)
        //   - `default.api.timeout.ms`                        atLeast(0)  (596)
        //   - `metrics.sample.window.ms`                      atLeast(0)  (558)
        //   - `metrics.num.samples`                           atLeast(1)  (564)
        //   - `metadata.recovery.rebootstrap.trigger.ms`      atLeast(0)  (689)
        // The currently-translated validators are `max.poll.records >= 1`
        // and the string-enum keys.
        let mut config = Self::default();

        for (key, value) in props {
            match key.as_str() {
                Self::BOOTSTRAP_SERVERS_CONFIG => {
                    config.bootstrap_servers = split_csv(value);
                },
                Self::CLIENT_DNS_LOOKUP_CONFIG => {
                    config.client_dns_lookup = value.clone();
                },
                Self::CLIENT_ID_CONFIG => {
                    config.client_id = value.clone();
                },
                Self::CLIENT_RACK_CONFIG => {
                    config.client_rack = value.clone();
                },
                Self::GROUP_ID_CONFIG => {
                    config.group_id = if value.is_empty() { None } else { Some(value.clone()) };
                },
                Self::GROUP_INSTANCE_ID_CONFIG => {
                    if value.is_empty() {
                        return Err(KafkaError::illegal_argument(format!(
                            "Invalid value for '{}': must be non-empty",
                            Self::GROUP_INSTANCE_ID_CONFIG
                        )));
                    }
                    config.group_instance_id = Some(value.clone());
                },
                Self::GROUP_PROTOCOL_CONFIG => {
                    // Case-insensitive validation against the enum's lower-case names.
                    let lc = value.to_ascii_lowercase();
                    if lc != "classic" && lc != "consumer" {
                        return Err(KafkaError::illegal_argument(format!(
                            "Invalid value for '{}': {}",
                            Self::GROUP_PROTOCOL_CONFIG,
                            value
                        )));
                    }
                    // Java's `getString` returns the original-case value; preserve it.
                    config.group_protocol = value.clone();
                },
                Self::GROUP_REMOTE_ASSIGNOR_CONFIG => {
                    config.group_remote_assignor = if value.is_empty() { None } else { Some(value.clone()) };
                },
                Self::MAX_POLL_RECORDS_CONFIG => {
                    let v = parse_i32(key, value)?;
                    if v < 1 {
                        return Err(KafkaError::illegal_argument(format!(
                            "Invalid value {v} for configuration {key}: Value must be at least 1"
                        )));
                    }
                    config.max_poll_records = v;
                },
                Self::MAX_POLL_INTERVAL_MS_CONFIG => {
                    config.max_poll_interval_ms = parse_i32(key, value)?;
                },
                Self::SESSION_TIMEOUT_MS_CONFIG => {
                    config.session_timeout_ms = parse_i32(key, value)?;
                },
                Self::HEARTBEAT_INTERVAL_MS_CONFIG => {
                    config.heartbeat_interval_ms = parse_i32(key, value)?;
                },
                Self::ENABLE_AUTO_COMMIT_CONFIG => {
                    config.enable_auto_commit = parse_bool(key, value)?;
                },
                Self::AUTO_COMMIT_INTERVAL_MS_CONFIG => {
                    config.auto_commit_interval_ms = parse_i32(key, value)?;
                },
                Self::PARTITION_ASSIGNMENT_STRATEGY_CONFIG => {
                    // Accepted silently per scope §20.
                    config.partition_assignment_strategy = split_csv(value);
                },
                Self::AUTO_OFFSET_RESET_CONFIG => {
                    // Java validator delegates to AutoOffsetResetStrategy.fromString.
                    AutoOffsetResetStrategy::from_string(value)?;
                    config.auto_offset_reset = value.clone();
                },
                Self::FETCH_MIN_BYTES_CONFIG => {
                    config.fetch_min_bytes = parse_i32(key, value)?;
                },
                Self::FETCH_MAX_BYTES_CONFIG => {
                    config.fetch_max_bytes = parse_i32(key, value)?;
                },
                Self::FETCH_MAX_WAIT_MS_CONFIG => {
                    config.fetch_max_wait_ms = parse_i32(key, value)?;
                },
                Self::MAX_PARTITION_FETCH_BYTES_CONFIG => {
                    config.max_partition_fetch_bytes = parse_i32(key, value)?;
                },
                Self::CHECK_CRCS_CONFIG => {
                    config.check_crcs = parse_bool(key, value)?;
                },
                Self::SEND_BUFFER_CONFIG => {
                    config.send_buffer_bytes = parse_i32(key, value)?;
                },
                Self::RECEIVE_BUFFER_CONFIG => {
                    config.receive_buffer_bytes = parse_i32(key, value)?;
                },
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG => {
                    config.socket_connection_setup_timeout_ms = parse_i64(key, value)?;
                },
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG => {
                    config.socket_connection_setup_timeout_max_ms = parse_i64(key, value)?;
                },
                Self::CONNECTIONS_MAX_IDLE_MS_CONFIG => {
                    config.connections_max_idle_ms = parse_i64(key, value)?;
                },
                Self::RECONNECT_BACKOFF_MS_CONFIG => {
                    config.reconnect_backoff_ms = parse_i64(key, value)?;
                },
                Self::RECONNECT_BACKOFF_MAX_MS_CONFIG => {
                    config.reconnect_backoff_max_ms = parse_i64(key, value)?;
                },
                Self::RETRY_BACKOFF_MS_CONFIG => {
                    config.retry_backoff_ms = parse_i64(key, value)?;
                },
                Self::RETRY_BACKOFF_MAX_MS_CONFIG => {
                    config.retry_backoff_max_ms = parse_i64(key, value)?;
                },
                Self::REQUEST_TIMEOUT_MS_CONFIG => {
                    config.request_timeout_ms = parse_i32(key, value)?;
                },
                Self::DEFAULT_API_TIMEOUT_MS_CONFIG => {
                    config.default_api_timeout_ms = parse_i32(key, value)?;
                },
                Self::METADATA_MAX_AGE_CONFIG => {
                    config.metadata_max_age_ms = parse_i64(key, value)?;
                },
                Self::METADATA_RECOVERY_STRATEGY_CONFIG => {
                    let lc = value.to_ascii_lowercase();
                    if lc != "none" && lc != "rebootstrap" {
                        return Err(KafkaError::illegal_argument(format!(
                            "Invalid value for '{}': {}",
                            Self::METADATA_RECOVERY_STRATEGY_CONFIG,
                            value
                        )));
                    }
                    config.metadata_recovery_strategy = value.clone();
                },
                Self::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG => {
                    config.metadata_recovery_rebootstrap_trigger_ms = parse_i64(key, value)?;
                },
                Self::EXCLUDE_INTERNAL_TOPICS_CONFIG => {
                    config.exclude_internal_topics = parse_bool(key, value)?;
                },
                Self::THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED => {
                    config.throw_on_fetch_stable_offset_unsupported = parse_bool(key, value)?;
                },
                Self::ISOLATION_LEVEL_CONFIG => {
                    let lc = value.to_ascii_lowercase();
                    if lc != "read_committed" && lc != "read_uncommitted" {
                        return Err(KafkaError::illegal_argument(format!(
                            "Invalid value for '{}': {}",
                            Self::ISOLATION_LEVEL_CONFIG,
                            value
                        )));
                    }
                    config.isolation_level = value.clone();
                },
                Self::ALLOW_AUTO_CREATE_TOPICS_CONFIG => {
                    config.allow_auto_create_topics = parse_bool(key, value)?;
                },
                Self::ENABLE_METRICS_PUSH_CONFIG => {
                    config.enable_metrics_push = parse_bool(key, value)?;
                },
                Self::METRICS_SAMPLE_WINDOW_MS_CONFIG => {
                    config.metrics_sample_window_ms = parse_i64(key, value)?;
                },
                Self::METRICS_NUM_SAMPLES_CONFIG => {
                    config.metrics_num_samples = parse_i32(key, value)?;
                },
                Self::METRICS_RECORDING_LEVEL_CONFIG => {
                    let uc = value.to_ascii_uppercase();
                    if uc != "INFO" && uc != "DEBUG" && uc != "TRACE" {
                        return Err(KafkaError::illegal_argument(format!(
                            "Invalid value for '{}': {}",
                            Self::METRICS_RECORDING_LEVEL_CONFIG,
                            value
                        )));
                    }
                    config.metrics_recording_level = value.clone();
                },
                Self::METRIC_REPORTER_CLASSES_CONFIG => {
                    config.metric_reporter_classes = split_csv(value);
                },
                Self::KEY_DESERIALIZER_CLASS_CONFIG => {
                    config.key_deserializer_class = if value.is_empty() { None } else { Some(value.clone()) };
                },
                Self::VALUE_DESERIALIZER_CLASS_CONFIG => {
                    config.value_deserializer_class = if value.is_empty() { None } else { Some(value.clone()) };
                },
                Self::INTERCEPTOR_CLASSES_CONFIG => {
                    // Accepted silently per scope §20.
                    config.interceptor_classes = split_csv(value);
                },
                Self::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG => {
                    // Accepted silently per scope §20.
                    config.share_acknowledgement_mode = value.clone();
                },
                Self::SHARE_ACQUIRE_MODE_CONFIG => {
                    // Accepted silently per scope §20.
                    config.share_acquire_mode = value.clone();
                },
                Self::SECURITY_PROVIDERS_CONFIG => {
                    config.security_providers = if value.is_empty() { None } else { Some(value.clone()) };
                },
                Self::SECURITY_PROTOCOL_CONFIG => {
                    config.security_protocol = SecurityProtocol::for_name(value).ok_or_else(|| {
                        KafkaError::illegal_argument(format!(
                            "Invalid value for '{}': {}. Valid values are: {:?}",
                            Self::SECURITY_PROTOCOL_CONFIG,
                            value,
                            SecurityProtocol::names()
                        ))
                    })?;
                },
                Self::SASL_MECHANISM_CONFIG => {
                    config.sasl_config.mechanism = value.clone();
                },
                Self::SASL_JAAS_CONFIG => {
                    config.sasl_config.jaas_config = if value.is_empty() { None } else { Some(value.clone()) };
                },
                key if key.starts_with("ssl.") => {
                    ssl_configs::apply_ssl_config_key(&mut config.ssl_config, key, value);
                },
                Self::CONFIG_PROVIDERS_CONFIG => {
                    config.config_providers = split_csv(value);
                },
                _ => {
                    warn!("Unknown consumer configuration key: {key}");
                },
            }
        }

        Ok(config)
    }
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .filter_map(|s| {
            let t = s.trim();
            (!t.is_empty()).then(|| t.to_string())
        })
        .collect()
}

fn parse_i32(key: &str, value: &str) -> Result<i32, KafkaError> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|_| KafkaError::illegal_argument(format!("Invalid value for '{}': {}", key, value)))
}

fn parse_i64(key: &str, value: &str) -> Result<i64, KafkaError> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|_| KafkaError::illegal_argument(format!("Invalid value for '{}': {}", key, value)))
}

fn parse_bool(key: &str, value: &str) -> Result<bool, KafkaError> {
    match value.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(KafkaError::illegal_argument(format!("Invalid value for '{}': {}", key, value))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let c = ConsumerConfig::default();
        assert_eq!(c.max_poll_records(), 500);
        assert_eq!(c.max_poll_interval_ms(), 300_000);
        assert_eq!(c.session_timeout_ms(), 45_000);
        assert_eq!(c.heartbeat_interval_ms(), 3_000);
        assert!(c.enable_auto_commit());
        assert_eq!(c.auto_commit_interval_ms(), 5_000);
        assert_eq!(c.auto_offset_reset(), "latest");
        assert_eq!(c.group_protocol(), "classic");
        assert_eq!(c.group_id(), None);
        assert_eq!(c.security_protocol(), "PLAINTEXT");
        assert_eq!(c.metadata_recovery_strategy(), "rebootstrap");
    }

    #[test]
    fn test_from_properties_basic() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "host1:9092,host2:9093".to_string());
        props.insert("group.id".to_string(), "g".to_string());
        props.insert("client.id".to_string(), "my-consumer".to_string());
        props.insert("max.poll.records".to_string(), "100".to_string());
        let c = ConsumerConfig::from_properties(&props).unwrap();
        assert_eq!(c.bootstrap_servers(), &["host1:9092".to_string(), "host2:9093".to_string()]);
        assert_eq!(c.group_id(), Some("g"));
        assert_eq!(c.client_id(), "my-consumer");
        assert_eq!(c.max_poll_records(), 100);
    }

    #[test]
    fn test_from_properties_unknown_key_ignored() {
        let mut props = HashMap::new();
        props.insert("unknown.key".to_string(), "value".to_string());
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        let c = ConsumerConfig::from_properties(&props).unwrap();
        assert_eq!(c.bootstrap_servers(), &["localhost:9092".to_string()]);
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
            let mut props = HashMap::new();
            props.insert("security.protocol".to_string(), input.to_string());
            let c = ConsumerConfig::from_properties(&props).unwrap();
            assert_eq!(c.security_protocol, expected, "for input {input}");
            assert_eq!(c.security_protocol(), expected.name());
        }
    }

    /// `security.protocol` parsing is case-insensitive (mirrors Java/`for_name`).
    #[test]
    fn test_security_protocol_case_insensitive() {
        let mut props = HashMap::new();
        props.insert("security.protocol".to_string(), "sasl_ssl".to_string());
        let c = ConsumerConfig::from_properties(&props).unwrap();
        assert_eq!(c.security_protocol, SecurityProtocol::SaslSsl);
    }

    /// Invalid `security.protocol` → `illegal_argument` with asserted message
    /// content (DoD §3): the config key, the bad value, and the valid names.
    #[test]
    fn test_invalid_security_protocol() {
        let mut props = HashMap::new();
        props.insert("security.protocol".to_string(), "abc".to_string());
        let err = ConsumerConfig::from_properties(&props).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("security.protocol"), "should contain config key, got: {msg}");
        assert!(msg.contains("abc"), "should contain the invalid value, got: {msg}");
        assert!(msg.contains("SASL_SSL"), "should list the valid protocol names, got: {msg}");
    }

    /// `sasl.mechanism` and `sasl.jaas.config` land on `sasl_config`.
    #[test]
    fn test_sasl_config_from_properties() {
        let mut props = HashMap::new();
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"secret\";"
                .to_string(),
        );
        let c = ConsumerConfig::from_properties(&props).unwrap();
        assert_eq!(c.sasl_config.mechanism, "PLAIN");
        assert_eq!(c.sasl_config.resolve_username(), Some("alice"));
        assert_eq!(c.sasl_config.resolve_password(), Some("secret"));
    }

    /// Empty `sasl.jaas.config` → `None`.
    #[test]
    fn test_sasl_jaas_config_empty_is_none() {
        let mut props = HashMap::new();
        props.insert("sasl.jaas.config".to_string(), String::new());
        let c = ConsumerConfig::from_properties(&props).unwrap();
        assert_eq!(c.sasl_config.jaas_config, None);
    }

    /// `ssl.*` keys land on `ssl_config` via the shared helper.
    #[test]
    fn test_ssl_config_from_properties() {
        let mut props = HashMap::new();
        props.insert("ssl.truststore.location".to_string(), "/path/to/truststore.pem".to_string());
        props.insert("ssl.keystore.location".to_string(), "/path/to/keystore.pem".to_string());
        props.insert("ssl.endpoint.identification.algorithm".to_string(), String::new());
        let c = ConsumerConfig::from_properties(&props).unwrap();
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
        use crate::common::network::channel_builders;
        use crate::common::utils::LogContext;

        // SASL_PLAINTEXT with PLAIN credentials → Ok (no cert needed).
        let mut props = HashMap::new();
        props.insert("security.protocol".to_string(), "SASL_PLAINTEXT".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";"
                .to_string(),
        );
        let c = ConsumerConfig::from_properties(&props).unwrap();
        assert_eq!(c.security_protocol, SecurityProtocol::SaslPlaintext);
        let builder = channel_builders::client_channel_builder(
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
        let mut props = HashMap::new();
        props.insert("security.protocol".to_string(), "SASL_SSL".to_string());
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";"
                .to_string(),
        );
        let c = ConsumerConfig::from_properties(&props).unwrap();
        let builder = channel_builders::client_channel_builder(
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
}
