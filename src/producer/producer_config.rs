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

//! Translation of `org.apache.kafka.clients.producer.ProducerConfig`.
//!
//! Configuration for the Kafka Producer.
//!
//! # Milestone-1 deviations
//!
//! Per `design/history/Milestone-1/PLAN.md` the following producer features
//! are out of scope until later phases. `ProducerConfig::new` rejects
//! configurations that try to enable any of them:
//!
//! * `enable.idempotence=true` — idempotent producer is Phase 8.
//!   The default flips from Java's `true` to `false` here.
//! * `transactional.id` set to a non-null/non-empty value — transactional
//!   producer is Phase 9.
//! * `security.protocol ∈ {SASL_PLAINTEXT, SASL_SSL}` — SASL is Phase 9.
//!   Enforced via the `security.protocol` validator restricted to
//!   `{PLAINTEXT, SSL}`.
//!
//! All three rejections raise [`KafkaError::Config`] mirroring Java's
//! `ConfigException` contract.
//!
//! # Skipped Java symbols
//!
//! * `static main(String[] args)` — generates HTML config docs; Phase 7a
//!   does not translate the documentation generator.
//! * `configDef()` — public accessor for the schema. The schema itself is
//!   exposed via [`ProducerConfig::config_def`] returning `&ConfigDef`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI32;

use crate::client_dns_lookup::ClientDnsLookup;
use crate::common::config::abstract_config::AbstractConfig;
use crate::common::config::config_def::{
    CaseInsensitiveValidString, ConfigDef, ConfigValue, Importance, NonEmptyString, Range, Type, ValidList, Validator,
};
use crate::common::config::config_exception;
use crate::common::errors::KafkaError;
use crate::common::record::CompressionType;
use crate::common_client_configs;
use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;

// =====================================================================
// Public config keys. Mirrors Java's `public static final String` block
// in `ProducerConfig`. Many delegate to `CommonClientConfigs` — see Java
// lines 64-307.
//
// NOTE: DO NOT CHANGE either the variable names or the string values.
// They are part of the public API and changes break user code (Java
// comment line 58).
// =====================================================================

/// `bootstrap.servers`
pub const BOOTSTRAP_SERVERS_CONFIG: &str = common_client_configs::BOOTSTRAP_SERVERS_CONFIG;

/// `client.dns.lookup`
pub const CLIENT_DNS_LOOKUP_CONFIG: &str = common_client_configs::CLIENT_DNS_LOOKUP_CONFIG;

/// `metadata.max.age.ms`
pub const METADATA_MAX_AGE_CONFIG: &str = common_client_configs::METADATA_MAX_AGE_CONFIG;

/// `metadata.max.idle.ms`
pub const METADATA_MAX_IDLE_CONFIG: &str = "metadata.max.idle.ms";
const METADATA_MAX_IDLE_DOC: &str = concat!(
    "Controls how long the producer will cache metadata for a topic that's idle. If the elapsed time since a topic ",
    "was last produced to exceeds the metadata idle duration, then the topic's metadata is forgotten and the next ",
    "access to it will force a metadata fetch request.",
);

/// `batch.size`
pub const BATCH_SIZE_CONFIG: &str = "batch.size";
const BATCH_SIZE_DOC: &str = concat!(
    "The producer will attempt to batch records together into fewer requests whenever multiple records are being sent ",
    "to the same partition. This helps performance on both the client and the server. This configuration controls the ",
    "default batch size in bytes. No attempt will be made to batch records larger than this size. Requests sent to ",
    "brokers will contain multiple batches, one for each partition with data available to be sent. A small batch size ",
    "will make batching less common and may reduce throughput (a batch size of zero will disable batching entirely). ",
    "A very large batch size may use memory a bit more wastefully as we will always allocate a buffer of the specified ",
    "batch size in anticipation of additional records.",
);

/// `partitioner.adaptive.partitioning.enable`
pub const PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG: &str = "partitioner.adaptive.partitioning.enable";
const PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_DOC: &str = concat!(
    "When set to 'true', the producer will try to adapt to broker performance and produce more messages to partitions ",
    "hosted on faster brokers. If 'false', the producer will try to distribute messages uniformly. Note: this setting ",
    "has no effect if a custom partitioner is used.",
);

/// `partitioner.availability.timeout.ms`
pub const PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG: &str = "partitioner.availability.timeout.ms";
const PARTITIONER_AVAILABILITY_TIMEOUT_MS_DOC: &str = concat!(
    "If a broker cannot process produce requests from a partition for `partitioner.availability.timeout.ms` time, ",
    "the partitioner treats that partition as not available. If the value is 0, this logic is disabled.",
);

/// `partitioner.ignore.keys`
pub const PARTITIONER_IGNORE_KEYS_CONFIG: &str = "partitioner.ignore.keys";
const PARTITIONER_IGNORE_KEYS_DOC: &str = concat!(
    "When set to 'true' the producer won't use record keys to choose a partition. If 'false', producer would choose ",
    "a partition based on a hash of the key when a key is present. Note: this setting has no effect if a custom ",
    "partitioner is used.",
);

/// `acks`
pub const ACKS_CONFIG: &str = "acks";
const ACKS_DOC: &str = concat!(
    "The number of acknowledgments the producer requires the leader to have received before considering a request ",
    "complete. Allowed values: 0, 1, all (or -1).",
);

/// `linger.ms`
pub const LINGER_MS_CONFIG: &str = "linger.ms";
const LINGER_MS_DOC: &str = concat!(
    "The producer groups together any records that arrive in between request transmissions into a single batched ",
    "request. This setting accomplishes this by adding a small amount of artificial delay. The default changed from ",
    "0 to 5 in Apache Kafka 4.0.",
);

/// `request.timeout.ms`
pub const REQUEST_TIMEOUT_MS_CONFIG: &str = common_client_configs::REQUEST_TIMEOUT_MS_CONFIG;

/// `delivery.timeout.ms`
pub const DELIVERY_TIMEOUT_MS_CONFIG: &str = "delivery.timeout.ms";
const DELIVERY_TIMEOUT_MS_DOC: &str = concat!(
    "An upper bound on the time to report success or failure after a call to `send()` returns. This limits the total ",
    "time that a record will be delayed prior to sending, the time to await acknowledgement from the broker (if ",
    "expected), and the time allowed for retriable send failures.",
);

/// `client.id`
pub const CLIENT_ID_CONFIG: &str = common_client_configs::CLIENT_ID_CONFIG;

/// `send.buffer.bytes`
pub const SEND_BUFFER_CONFIG: &str = common_client_configs::SEND_BUFFER_CONFIG;

/// `receive.buffer.bytes`
pub const RECEIVE_BUFFER_CONFIG: &str = common_client_configs::RECEIVE_BUFFER_CONFIG;

/// `max.request.size`
pub const MAX_REQUEST_SIZE_CONFIG: &str = "max.request.size";
const MAX_REQUEST_SIZE_DOC: &str = concat!(
    "The maximum size of a request in bytes. This setting will limit the number of record batches the producer will ",
    "send in a single request to avoid sending huge requests. This is also effectively a cap on the maximum ",
    "uncompressed record batch size.",
);

/// `reconnect.backoff.ms`
pub const RECONNECT_BACKOFF_MS_CONFIG: &str = common_client_configs::RECONNECT_BACKOFF_MS_CONFIG;
/// `reconnect.backoff.max.ms`
pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &str = common_client_configs::RECONNECT_BACKOFF_MAX_MS_CONFIG;

/// `max.block.ms`
pub const MAX_BLOCK_MS_CONFIG: &str = "max.block.ms";
const MAX_BLOCK_MS_DOC: &str = concat!(
    "The configuration controls how long the `KafkaProducer`'s `send()`, `partitionsFor()`, ",
    "transactional methods and `flush()` methods will block.",
);

/// `buffer.memory`
pub const BUFFER_MEMORY_CONFIG: &str = "buffer.memory";
const BUFFER_MEMORY_DOC: &str = concat!(
    "The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records ",
    "are sent faster than they can be delivered to the server the producer will block for `max.block.ms` after which ",
    "it will fail with an exception.",
);

/// `retry.backoff.ms`
pub const RETRY_BACKOFF_MS_CONFIG: &str = common_client_configs::RETRY_BACKOFF_MS_CONFIG;
/// `retry.backoff.max.ms`
pub const RETRY_BACKOFF_MAX_MS_CONFIG: &str = common_client_configs::RETRY_BACKOFF_MAX_MS_CONFIG;

/// `enable.metrics.push`
pub const ENABLE_METRICS_PUSH_CONFIG: &str = common_client_configs::ENABLE_METRICS_PUSH_CONFIG;

/// `compression.type`
pub const COMPRESSION_TYPE_CONFIG: &str = "compression.type";
const COMPRESSION_TYPE_DOC: &str = concat!(
    "The compression type for all data generated by the producer. The default is none. Valid values are `none`, ",
    "`gzip`, `snappy`, `lz4`, or `zstd`. Compression is of full batches of data.",
);

/// `compression.gzip.level`
pub const COMPRESSION_GZIP_LEVEL_CONFIG: &str = "compression.gzip.level";
/// `compression.lz4.level`
pub const COMPRESSION_LZ4_LEVEL_CONFIG: &str = "compression.lz4.level";
/// `compression.zstd.level`
pub const COMPRESSION_ZSTD_LEVEL_CONFIG: &str = "compression.zstd.level";

/// `metrics.sample.window.ms`
pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &str = common_client_configs::METRICS_SAMPLE_WINDOW_MS_CONFIG;
/// `metrics.num.samples`
pub const METRICS_NUM_SAMPLES_CONFIG: &str = common_client_configs::METRICS_NUM_SAMPLES_CONFIG;
/// `metrics.recording.level`
pub const METRICS_RECORDING_LEVEL_CONFIG: &str = common_client_configs::METRICS_RECORDING_LEVEL_CONFIG;

/// `metric.reporters`
pub const METRIC_REPORTER_CLASSES_CONFIG: &str = common_client_configs::METRIC_REPORTER_CLASSES_CONFIG;

/// `max.in.flight.requests.per.connection` upper bound when idempotence is
/// enabled. The value 5 is aligned with `ProducerStateEntry#NUM_BATCHES_TO_RETAIN`.
///
/// Wired up in Phase 7a (4/N) by `post_process_and_validate_idempotence_configs`.
#[allow(dead_code)]
const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE: i32 = 5;

/// `max.in.flight.requests.per.connection`
pub const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: &str = "max.in.flight.requests.per.connection";
const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC: &str = concat!(
    "The maximum number of unacknowledged requests the client will send on a single connection before blocking. ",
    "Note that if this configuration is set to be greater than 1 and `enable.idempotence` is set to false, there is ",
    "a risk of message reordering after a failed send due to retries. Additionally, enabling idempotence requires ",
    "the value of this configuration to be less than or equal to 5.",
);

/// `retries`
pub const RETRIES_CONFIG: &str = common_client_configs::RETRIES_CONFIG;
const RETRIES_DOC: &str = concat!(
    "Number of times to retry a request that fails with a transient error. Setting a value greater than zero will ",
    "cause the client to resend any record whose send fails with a potentially transient error. Users should ",
    "generally prefer to leave this config unset and instead use `delivery.timeout.ms`.",
);

/// `key.serializer`
pub const KEY_SERIALIZER_CLASS_CONFIG: &str = "key.serializer";
/// Doc string for [`KEY_SERIALIZER_CLASS_CONFIG`].
pub const KEY_SERIALIZER_CLASS_DOC: &str = "Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

/// `value.serializer`
pub const VALUE_SERIALIZER_CLASS_CONFIG: &str = "value.serializer";
/// Doc string for [`VALUE_SERIALIZER_CLASS_CONFIG`].
pub const VALUE_SERIALIZER_CLASS_DOC: &str = "Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";

/// `socket.connection.setup.timeout.ms`
pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &str =
    common_client_configs::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
/// `socket.connection.setup.timeout.max.ms`
pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &str =
    common_client_configs::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

/// `connections.max.idle.ms`
pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &str = common_client_configs::CONNECTIONS_MAX_IDLE_MS_CONFIG;

/// `partitioner.class`
pub const PARTITIONER_CLASS_CONFIG: &str = "partitioner.class";
const PARTITIONER_CLASS_DOC: &str = concat!(
    "Determines which partition to send a record to when records are produced. If not set, the default partitioning ",
    "logic is used.",
);

/// `interceptor.classes`
pub const INTERCEPTOR_CLASSES_CONFIG: &str = "interceptor.classes";
/// Doc string for [`INTERCEPTOR_CLASSES_CONFIG`].
pub const INTERCEPTOR_CLASSES_DOC: &str = concat!(
    "A list of classes to use as interceptors. ",
    "Implementing the <code>org.apache.kafka.clients.producer.ProducerInterceptor</code> interface allows you to ",
    "intercept (and possibly mutate) the records received by the producer before they are published to the Kafka ",
    "cluster. By default, there are no interceptors.",
);

/// `enable.idempotence`
///
/// **Milestone-1 deviation**: the Java default is `true`. The Rust client
/// flips this to `false` for Milestone 1 and rejects an explicit
/// `enable.idempotence=true` at construction. The idempotent producer
/// stack lands in Phase 8.
pub const ENABLE_IDEMPOTENCE_CONFIG: &str = "enable.idempotence";
/// Doc string for [`ENABLE_IDEMPOTENCE_CONFIG`].
pub const ENABLE_IDEMPOTENCE_DOC: &str = concat!(
    "When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. ",
    "If 'false', producer retries due to broker failures, etc., may write duplicates of the retried message in the stream. ",
    "Note that enabling idempotence requires <code>max.in.flight.requests.per.connection</code> to be less than or equal to 5 ",
    "(with message ordering preserved for any allowable value), <code>retries</code> to be greater than 0, and <code>acks</code> ",
    "must be 'all'. ",
    "<p>",
    "Idempotence is enabled by default if no conflicting configurations are set. ",
    "If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled. ",
    "If idempotence is explicitly enabled and conflicting configurations are set, a <code>ConfigException</code> is thrown.",
    "<p>",
    "Milestone-1 deviation: the default is 'false' and an explicit 'true' is rejected at construction (Phase 8 will re-enable). ",
    "See design/history/Milestone-1/PLAN.md.",
);

/// Default for `enable.idempotence`. Java sets this to `true`; Milestone 1
/// flips it to `false` (see [`ENABLE_IDEMPOTENCE_DOC`]).
pub const DEFAULT_ENABLE_IDEMPOTENCE: bool = false;

/// `transaction.timeout.ms`
pub const TRANSACTION_TIMEOUT_CONFIG: &str = "transaction.timeout.ms";
/// Doc string for [`TRANSACTION_TIMEOUT_CONFIG`].
pub const TRANSACTION_TIMEOUT_DOC: &str = concat!(
    "The maximum amount of time in milliseconds that a transaction will remain open before the coordinator ",
    "proactively aborts it. ",
    "The start of the transaction is set at the time that the first partition is added to it. ",
    "If this value is larger than the <code>transaction.max.timeout.ms</code> setting in the broker, the request will fail with a ",
    "<code>InvalidTxnTimeoutException</code> error.",
);

/// `transactional.id`
pub const TRANSACTIONAL_ID_CONFIG: &str = "transactional.id";
/// Doc string for [`TRANSACTIONAL_ID_CONFIG`].
pub const TRANSACTIONAL_ID_DOC: &str = concat!(
    "The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer ",
    "sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed ",
    "prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. ",
    "If a TransactionalId is configured, <code>enable.idempotence</code> is implied. ",
    "By default the TransactionId is not configured, which means transactions cannot be used. ",
    "Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production; ",
    "for development you can change this, by adjusting broker setting ",
    "<code>transaction.state.log.replication.factor</code>.",
);

/// `transaction.two.phase.commit.enable`
pub const TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG: &str = "transaction.two.phase.commit.enable";
const TRANSACTION_TWO_PHASE_COMMIT_ENABLE_DOC: &str = concat!(
    "If set to true, then the broker is informed that the client is participating in two phase commit protocol and ",
    "transactions that this client starts never expire.",
);

/// `security.providers`. Mirrors `SecurityConfig.SECURITY_PROVIDERS_CONFIG`.
pub const SECURITY_PROVIDERS_CONFIG: &str = "security.providers";
const SECURITY_PROVIDERS_DOC: &str =
    "A list of configurable creator classes each returning a provider implementing security algorithms.";

/// `config.providers`. Mirrors `AbstractConfig.CONFIG_PROVIDERS_CONFIG`.
pub const CONFIG_PROVIDERS_CONFIG: &str = "config.providers";
const CONFIG_PROVIDERS_DOC: &str = concat!(
    "Comma-separated alias names for classes implementing the `ConfigProvider` interface. This enables loading ",
    "configuration data from external sources.",
);

// `org.apache.kafka.common.metrics.JmxReporter` — the FQCN Java's
// `JmxReporter.class.getName()` resolves to. Used as the default for
// `metric.reporters`. We keep the string form since the JmxReporter class
// itself is not yet translated.
const JMX_REPORTER_CLASS: &str = "org.apache.kafka.common.metrics.JmxReporter";

// Sensor.RecordingLevel names. Java's enum has INFO, DEBUG, TRACE.
const RECORDING_LEVEL_INFO: &str = "INFO";
const RECORDING_LEVEL_DEBUG: &str = "DEBUG";
const RECORDING_LEVEL_TRACE: &str = "TRACE";

/// Atomic counter used by [`ProducerConfig::maybe_override_client_id`]
/// when the user did not set `client.id` and `transactional.id` is null.
/// Mirrors Java's `private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE`.
///
/// Wired up in Phase 7a (4/N) by `maybe_override_client_id`.
#[allow(dead_code)]
pub(crate) static PRODUCER_CLIENT_ID_SEQUENCE: AtomicI32 = AtomicI32::new(1);

// =====================================================================
// Schema (the singleton `ConfigDef` Java keeps in `private static final
// ConfigDef CONFIG`). We use `OnceLock` for lazy single-init.
// =====================================================================

fn build_config_def() -> ConfigDef {
    let mut def = ConfigDef::new();
    let bootstrap_validator: Arc<dyn Validator> = Arc::new(ValidList::any_non_duplicate_values(false, false));
    let dns_lookup_validator: Arc<dyn Validator> = Arc::new(crate::common::config::config_def::ValidString::in_set([
        ClientDnsLookup::UseAllDnsIps.as_config_str(),
        ClientDnsLookup::ResolveCanonicalBootstrapServersOnly.as_config_str(),
    ]));
    let buffer_memory_validator: Arc<dyn Validator> = Arc::new(Range::at_least(0_f64));
    let retries_validator: Arc<dyn Validator> = Arc::new(Range::between(0, i32::MAX));
    let acks_validator: Arc<dyn Validator> =
        Arc::new(crate::common::config::config_def::ValidString::in_set(["all", "-1", "0", "1"]));
    let compression_type_validator: Arc<dyn Validator> =
        Arc::new(crate::common::config::config_def::ValidString::in_set([
            CompressionType::None.name(),
            CompressionType::Gzip.name(),
            CompressionType::Snappy.name(),
            CompressionType::Lz4.name(),
            CompressionType::Zstd.name(),
        ]));
    let zero_or_more_i32: Arc<dyn Validator> = Arc::new(Range::at_least(0_i32));
    let zero_or_more_i64: Arc<dyn Validator> = Arc::new(Range::at_least(0_f64));
    // Java: `atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND)` /
    // `atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND)`. Both lower
    // bounds are `-1` (Kafka semantics: "use OS default"), so these
    // validators accept `>= -1`.
    let at_least_send_buffer_lower_bound: Arc<dyn Validator> =
        Arc::new(Range::at_least(common_client_configs::SEND_BUFFER_LOWER_BOUND));
    let at_least_recv_buffer_lower_bound: Arc<dyn Validator> =
        Arc::new(Range::at_least(common_client_configs::RECEIVE_BUFFER_LOWER_BOUND));
    let metadata_max_idle_validator: Arc<dyn Validator> = Arc::new(Range::at_least(5_000_f64));
    let one_or_more_i32: Arc<dyn Validator> = Arc::new(Range::at_least(1_i32));
    let recording_level_validator: Arc<dyn Validator> =
        Arc::new(crate::common::config::config_def::ValidString::in_set([
            RECORDING_LEVEL_INFO,
            RECORDING_LEVEL_DEBUG,
            RECORDING_LEVEL_TRACE,
        ]));
    let metric_reporter_validator: Arc<dyn Validator> = Arc::new(ValidList::any_non_duplicate_values(true, false));
    let interceptor_validator: Arc<dyn Validator> = Arc::new(ValidList::any_non_duplicate_values(true, false));
    let config_providers_validator: Arc<dyn Validator> = Arc::new(ValidList::any_non_duplicate_values(true, false));

    // Phase 9b: accept `SASL_PLAINTEXT` and `SASL_SSL` in addition to
    // `PLAINTEXT` / `SSL`. The SASL mechanism narrowing (PLAIN only in
    // Milestone 1) lives downstream in
    // [`Self::post_validate_sasl_mechanism_config_with_milestone_narrowing`].
    let security_protocol_validator: Arc<dyn Validator> = Arc::new(CaseInsensitiveValidString::in_set([
        "PLAINTEXT",
        "SSL",
        "SASL_PLAINTEXT",
        "SASL_SSL",
    ]));

    let metadata_recovery_validator: Arc<dyn Validator> = Arc::new(CaseInsensitiveValidString::in_set([
        MetadataRecoveryStrategy::None.name(),
        MetadataRecoveryStrategy::Rebootstrap.name(),
    ]));

    let transactional_id_validator: Arc<dyn Validator> = Arc::new(NonEmptyString);

    let gzip_validator = make_compression_level_validator(CompressionType::Gzip);
    let lz4_validator = make_compression_level_validator(CompressionType::Lz4);
    let zstd_validator = make_compression_level_validator(CompressionType::Zstd);

    def.define(
        BOOTSTRAP_SERVERS_CONFIG,
        Type::List,
        None,
        Some(bootstrap_validator),
        Importance::High,
        common_client_configs::BOOTSTRAP_SERVERS_DOC,
    )
    .unwrap()
    .define(
        CLIENT_DNS_LOOKUP_CONFIG,
        Type::String,
        Some(ConfigValue::String(ClientDnsLookup::UseAllDnsIps.as_config_str().to_owned())),
        Some(dns_lookup_validator),
        Importance::Medium,
        common_client_configs::CLIENT_DNS_LOOKUP_DOC,
    )
    .unwrap()
    .define(
        BUFFER_MEMORY_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(32 * 1024 * 1024)),
        Some(buffer_memory_validator),
        Importance::High,
        BUFFER_MEMORY_DOC,
    )
    .unwrap()
    .define(
        RETRIES_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(i32::MAX)),
        Some(retries_validator),
        Importance::High,
        RETRIES_DOC,
    )
    .unwrap()
    .define(
        ACKS_CONFIG,
        Type::String,
        Some(ConfigValue::String("all".into())),
        Some(acks_validator),
        Importance::Low,
        ACKS_DOC,
    )
    .unwrap()
    .define(
        COMPRESSION_TYPE_CONFIG,
        Type::String,
        Some(ConfigValue::String(CompressionType::None.name().to_owned())),
        Some(compression_type_validator),
        Importance::High,
        COMPRESSION_TYPE_DOC,
    )
    .unwrap()
    .define(
        COMPRESSION_GZIP_LEVEL_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(CompressionType::Gzip.default_level().expect("gzip default"))),
        Some(gzip_validator),
        Importance::Medium,
        "The compression level to use if compression.type is set to gzip.",
    )
    .unwrap()
    .define(
        COMPRESSION_LZ4_LEVEL_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(CompressionType::Lz4.default_level().expect("lz4 default"))),
        Some(lz4_validator),
        Importance::Medium,
        "The compression level to use if compression.type is set to lz4.",
    )
    .unwrap()
    .define(
        COMPRESSION_ZSTD_LEVEL_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(CompressionType::Zstd.default_level().expect("zstd default"))),
        Some(zstd_validator),
        Importance::Medium,
        "The compression level to use if compression.type is set to zstd.",
    )
    .unwrap()
    .define(
        BATCH_SIZE_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(16_384)),
        Some(zero_or_more_i32.clone()),
        Importance::Medium,
        BATCH_SIZE_DOC,
    )
    .unwrap()
    .define(
        PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG,
        Type::Boolean,
        Some(ConfigValue::Boolean(true)),
        None,
        Importance::Low,
        PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_DOC,
    )
    .unwrap()
    .define(
        PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(0)),
        Some(zero_or_more_i64.clone()),
        Importance::Low,
        PARTITIONER_AVAILABILITY_TIMEOUT_MS_DOC,
    )
    .unwrap()
    .define(
        PARTITIONER_IGNORE_KEYS_CONFIG,
        Type::Boolean,
        Some(ConfigValue::Boolean(false)),
        None,
        Importance::Medium,
        PARTITIONER_IGNORE_KEYS_DOC,
    )
    .unwrap()
    .define(
        LINGER_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(5)),
        Some(zero_or_more_i64.clone()),
        Importance::Medium,
        LINGER_MS_DOC,
    )
    .unwrap()
    .define(
        DELIVERY_TIMEOUT_MS_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(120 * 1000)),
        Some(zero_or_more_i32.clone()),
        Importance::Medium,
        DELIVERY_TIMEOUT_MS_DOC,
    )
    .unwrap()
    .define(
        CLIENT_ID_CONFIG,
        Type::String,
        Some(ConfigValue::String(String::new())),
        None,
        Importance::Medium,
        common_client_configs::CLIENT_ID_DOC,
    )
    .unwrap()
    .define(
        SEND_BUFFER_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(128 * 1024)),
        Some(at_least_send_buffer_lower_bound),
        Importance::Medium,
        common_client_configs::SEND_BUFFER_DOC,
    )
    .unwrap()
    .define(
        RECEIVE_BUFFER_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(32 * 1024)),
        Some(at_least_recv_buffer_lower_bound),
        Importance::Medium,
        common_client_configs::RECEIVE_BUFFER_DOC,
    )
    .unwrap()
    .define(
        MAX_REQUEST_SIZE_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(1024 * 1024)),
        Some(zero_or_more_i32.clone()),
        Importance::Medium,
        MAX_REQUEST_SIZE_DOC,
    )
    .unwrap()
    .define(
        RECONNECT_BACKOFF_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(50)),
        Some(zero_or_more_i64.clone()),
        Importance::Low,
        common_client_configs::RECONNECT_BACKOFF_MS_DOC,
    )
    .unwrap()
    .define(
        RECONNECT_BACKOFF_MAX_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(1000)),
        Some(zero_or_more_i64.clone()),
        Importance::Low,
        common_client_configs::RECONNECT_BACKOFF_MAX_MS_DOC,
    )
    .unwrap()
    .define(
        RETRY_BACKOFF_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(common_client_configs::DEFAULT_RETRY_BACKOFF_MS)),
        Some(zero_or_more_i64.clone()),
        Importance::Low,
        common_client_configs::RETRY_BACKOFF_MS_DOC,
    )
    .unwrap()
    .define(
        RETRY_BACKOFF_MAX_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(common_client_configs::DEFAULT_RETRY_BACKOFF_MAX_MS)),
        Some(zero_or_more_i64.clone()),
        Importance::Low,
        common_client_configs::RETRY_BACKOFF_MAX_MS_DOC,
    )
    .unwrap()
    .define(
        ENABLE_METRICS_PUSH_CONFIG,
        Type::Boolean,
        Some(ConfigValue::Boolean(true)),
        None,
        Importance::Low,
        common_client_configs::ENABLE_METRICS_PUSH_DOC,
    )
    .unwrap()
    .define(
        MAX_BLOCK_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(60 * 1000)),
        Some(zero_or_more_i64.clone()),
        Importance::Medium,
        MAX_BLOCK_MS_DOC,
    )
    .unwrap()
    .define(
        REQUEST_TIMEOUT_MS_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(30 * 1000)),
        Some(zero_or_more_i32.clone()),
        Importance::Medium,
        common_client_configs::REQUEST_TIMEOUT_MS_DOC,
    )
    .unwrap()
    .define(
        METADATA_MAX_AGE_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(5 * 60 * 1000)),
        Some(zero_or_more_i64.clone()),
        Importance::Low,
        common_client_configs::METADATA_MAX_AGE_DOC,
    )
    .unwrap()
    .define(
        METADATA_MAX_IDLE_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(5 * 60 * 1000)),
        Some(metadata_max_idle_validator),
        Importance::Low,
        METADATA_MAX_IDLE_DOC,
    )
    .unwrap()
    .define(
        METRICS_SAMPLE_WINDOW_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(30_000)),
        Some(zero_or_more_i64),
        Importance::Low,
        common_client_configs::METRICS_SAMPLE_WINDOW_MS_DOC,
    )
    .unwrap()
    .define(
        METRICS_NUM_SAMPLES_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(2)),
        Some(one_or_more_i32.clone()),
        Importance::Low,
        common_client_configs::METRICS_NUM_SAMPLES_DOC,
    )
    .unwrap()
    .define(
        METRICS_RECORDING_LEVEL_CONFIG,
        Type::String,
        Some(ConfigValue::String(RECORDING_LEVEL_INFO.to_owned())),
        Some(recording_level_validator),
        Importance::Low,
        common_client_configs::METRICS_RECORDING_LEVEL_DOC,
    )
    .unwrap()
    .define(
        METRIC_REPORTER_CLASSES_CONFIG,
        Type::List,
        Some(ConfigValue::List(vec![JMX_REPORTER_CLASS.to_owned()])),
        Some(metric_reporter_validator),
        Importance::Low,
        common_client_configs::METRIC_REPORTER_CLASSES_DOC,
    )
    .unwrap()
    .define(
        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        Type::Int,
        Some(ConfigValue::Int(5)),
        Some(one_or_more_i32),
        Importance::Low,
        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC,
    )
    .unwrap()
    .define(
        KEY_SERIALIZER_CLASS_CONFIG,
        Type::Class,
        None,
        None,
        Importance::High,
        KEY_SERIALIZER_CLASS_DOC,
    )
    .unwrap()
    .define(
        VALUE_SERIALIZER_CLASS_CONFIG,
        Type::Class,
        None,
        None,
        Importance::High,
        VALUE_SERIALIZER_CLASS_DOC,
    )
    .unwrap()
    .define(
        SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(
            common_client_configs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
        )),
        None,
        Importance::Medium,
        common_client_configs::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC,
    )
    .unwrap()
    .define(
        SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(
            common_client_configs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
        )),
        None,
        Importance::Medium,
        common_client_configs::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC,
    )
    .unwrap()
    // default is set to be a bit lower than the server default (10 min)
    .define(
        CONNECTIONS_MAX_IDLE_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(9 * 60 * 1000)),
        None,
        Importance::Medium,
        common_client_configs::CONNECTIONS_MAX_IDLE_MS_DOC,
    )
    .unwrap()
    .define(
        PARTITIONER_CLASS_CONFIG,
        Type::Class,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        PARTITIONER_CLASS_DOC,
    )
    .unwrap()
    .define(
        INTERCEPTOR_CLASSES_CONFIG,
        Type::List,
        Some(ConfigValue::List(Vec::new())),
        Some(interceptor_validator),
        Importance::Low,
        INTERCEPTOR_CLASSES_DOC,
    )
    .unwrap()
    .define(
        common_client_configs::SECURITY_PROTOCOL_CONFIG,
        Type::String,
        Some(ConfigValue::String(common_client_configs::DEFAULT_SECURITY_PROTOCOL.to_owned())),
        Some(security_protocol_validator),
        Importance::Medium,
        common_client_configs::SECURITY_PROTOCOL_DOC,
    )
    .unwrap()
    .define(
        SECURITY_PROVIDERS_CONFIG,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Low,
        SECURITY_PROVIDERS_DOC,
    )
    .unwrap()
    .with_client_ssl_support()
    .expect("ssl support definitions are static and cannot fail")
    .with_client_sasl_support()
    .expect("sasl support definitions are static and cannot fail")
    .define(
        ENABLE_IDEMPOTENCE_CONFIG,
        Type::Boolean,
        Some(ConfigValue::Boolean(DEFAULT_ENABLE_IDEMPOTENCE)),
        None,
        Importance::Low,
        ENABLE_IDEMPOTENCE_DOC,
    )
    .unwrap()
    .define(
        TRANSACTION_TIMEOUT_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(60_000)),
        None,
        Importance::Low,
        TRANSACTION_TIMEOUT_DOC,
    )
    .unwrap()
    .define(
        TRANSACTIONAL_ID_CONFIG,
        Type::String,
        Some(ConfigValue::Null),
        Some(transactional_id_validator),
        Importance::Low,
        TRANSACTIONAL_ID_DOC,
    )
    .unwrap()
    .define(
        TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG,
        Type::Boolean,
        Some(ConfigValue::Boolean(false)),
        None,
        Importance::Low,
        TRANSACTION_TWO_PHASE_COMMIT_ENABLE_DOC,
    )
    .unwrap()
    .define(
        common_client_configs::METADATA_RECOVERY_STRATEGY_CONFIG,
        Type::String,
        Some(ConfigValue::String(
            common_client_configs::DEFAULT_METADATA_RECOVERY_STRATEGY.to_owned(),
        )),
        Some(metadata_recovery_validator),
        Importance::Low,
        common_client_configs::METADATA_RECOVERY_STRATEGY_DOC,
    )
    .unwrap()
    .define(
        common_client_configs::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
        Type::Long,
        Some(ConfigValue::Long(
            common_client_configs::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
        )),
        Some(Arc::new(Range::at_least(0_f64))),
        Importance::Low,
        common_client_configs::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC,
    )
    .unwrap()
    .define(
        CONFIG_PROVIDERS_CONFIG,
        Type::List,
        Some(ConfigValue::List(Vec::new())),
        Some(config_providers_validator),
        Importance::Low,
        CONFIG_PROVIDERS_DOC,
    )
    .unwrap();
    def
}

fn make_compression_level_validator(kind: CompressionType) -> Arc<dyn Validator> {
    /// Wrapper that adapts the `Box<dyn Fn(i32) -> Result<(), KafkaError>>`
    /// returned by [`CompressionType::level_validator`] to the
    /// [`Validator`] trait used by [`ConfigDef`].
    struct LevelValidator {
        kind: CompressionType,
        check: Box<dyn Fn(i32) -> Result<(), KafkaError> + Send + Sync + 'static>,
    }

    impl std::fmt::Debug for LevelValidator {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LevelValidator").field("kind", &self.kind).finish()
        }
    }

    impl Validator for LevelValidator {
        fn ensure_valid(&self, name: &str, value: &ConfigValue) -> Result<(), KafkaError> {
            let level = value
                .as_i32()
                .ok_or_else(|| config_exception::new(name, value, "Compression level must be an int"))?;
            (self.check)(level).map_err(|e| match e {
                KafkaError::Config(msg) => config_exception::new(name, value, &msg),
                other => other,
            })
        }

        fn description(&self) -> String {
            format!("compression level for {}", self.kind.name())
        }
    }

    Arc::new(LevelValidator { kind, check: kind.level_validator() })
}

fn config() -> &'static ConfigDef {
    static CONFIG: std::sync::OnceLock<ConfigDef> = std::sync::OnceLock::new();
    CONFIG.get_or_init(build_config_def)
}

// =====================================================================
// `ProducerConfig` itself.
// =====================================================================

/// Translation of `org.apache.kafka.clients.producer.ProducerConfig`.
///
/// Wraps an [`AbstractConfig`] (Java's superclass) plus the post-processed
/// view of the parsed values. Constructors mirror Java's two
/// `ProducerConfig(Properties)` / `ProducerConfig(Map<String, Object>)`
/// overloads — both reduce to a single Rust `ProducerConfig::new(props)`
/// over a `HashMap<String, String>`.
pub struct ProducerConfig {
    inner: AbstractConfig,
    /// Post-processed override map applied on top of [`Self::inner`]'s
    /// parsed values. Mirrors Java's `postProcessParsedConfig` mutating the
    /// `parsedValues` map (entries `acks`, `client.id`, and possibly
    /// `enable.idempotence`).
    post_processed: HashMap<String, ConfigValue>,
}

impl std::fmt::Debug for ProducerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerConfig")
            .field("originals", self.inner.originals())
            .field("post_processed", &self.post_processed)
            .finish()
    }
}

impl ProducerConfig {
    /// Construct from raw key-value originals. Mirrors
    /// `ProducerConfig(Map<String, Object>)` after stringification.
    ///
    /// Returns [`KafkaError::Config`] when:
    /// * Any defined key fails its [`ConfigDef`] validator.
    /// * Milestone-1 explicitly rejects the configuration
    ///   (`enable.idempotence=true`, `transactional.id=<set>`, or
    ///   `security.protocol ∈ {SASL_PLAINTEXT, SASL_SSL}`). See the module-level
    ///   docs for rationale and pointers to the re-enable phase.
    pub fn new(props: HashMap<String, String>) -> Result<Self, KafkaError> {
        let inner = AbstractConfig::new(config(), props)?;
        let mut cfg = ProducerConfig { inner, post_processed: HashMap::new() };
        cfg.post_process_parsed_config()?;
        Ok(cfg)
    }

    /// Borrow the [`ConfigDef`] schema. Mirrors `ProducerConfig.configDef()`.
    pub fn config_def() -> &'static ConfigDef {
        config()
    }

    /// All defined config names. Mirrors `ProducerConfig.configNames()`.
    pub fn config_names() -> Vec<&'static str> {
        config().names().collect()
    }

    /// Append the configured serializer instances to the configuration map.
    /// Mirrors Java's static `appendSerializerToConfig`.
    ///
    /// Behaviour:
    /// * If `key_serializer_class` is `Some(class)`, it overwrites the
    ///   `key.serializer` entry with that class name. Otherwise, the entry
    ///   must already be present and non-`None` — a missing/`None` entry
    ///   raises a [`KafkaError::Config`] with `"must be non-null."`,
    ///   matching Java's `ConfigException(name, null, "must be non-null.")`.
    /// * Same for `value_serializer_class` / `value.serializer`.
    ///
    /// In Java, the second/third arguments are concrete `Serializer<?>`
    /// instances and the FQCN is read via `keySerializer.getClass()`. In
    /// Rust we accept the FQCN directly (`Option<&str>`) since Rust does
    /// not perform reflective class loading.
    pub fn append_serializer_to_config(
        configs: &HashMap<String, Option<String>>,
        key_serializer_class: Option<&str>,
        value_serializer_class: Option<&str>,
    ) -> Result<HashMap<String, Option<String>>, KafkaError> {
        let mut new_configs = configs.clone();
        if let Some(name) = key_serializer_class {
            new_configs.insert(KEY_SERIALIZER_CLASS_CONFIG.to_owned(), Some(name.to_owned()));
        } else if !new_configs.get(KEY_SERIALIZER_CLASS_CONFIG).is_some_and(Option::is_some) {
            return Err(config_exception::new(KEY_SERIALIZER_CLASS_CONFIG, "null", "must be non-null."));
        }
        if let Some(name) = value_serializer_class {
            new_configs.insert(VALUE_SERIALIZER_CLASS_CONFIG.to_owned(), Some(name.to_owned()));
        } else if !new_configs.get(VALUE_SERIALIZER_CLASS_CONFIG).is_some_and(Option::is_some) {
            return Err(config_exception::new(
                VALUE_SERIALIZER_CLASS_CONFIG,
                "null",
                "must be non-null.",
            ));
        }
        Ok(new_configs)
    }

    // ---- pass-through accessors ----

    /// Underlying [`AbstractConfig`] superclass instance.
    pub fn inner(&self) -> &AbstractConfig {
        &self.inner
    }

    /// `getString(name)`. Mirrors Java's `AbstractConfig.getString(String)`.
    pub fn get_string(&self, name: &str) -> Result<&str, KafkaError> {
        if let Some(v) = self.post_processed.get(name) {
            return v
                .as_str()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a string: {v:?}")));
        }
        self.inner.get_string(name)
    }

    /// `getInt(name)`.
    pub fn get_int(&self, name: &str) -> Result<i32, KafkaError> {
        if let Some(v) = self.post_processed.get(name) {
            return v
                .as_i32()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not an int: {v:?}")));
        }
        self.inner.get_int(name)
    }

    /// `getLong(name)`.
    pub fn get_long(&self, name: &str) -> Result<i64, KafkaError> {
        if let Some(v) = self.post_processed.get(name) {
            return v
                .as_i64()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a long: {v:?}")));
        }
        self.inner.get_long(name)
    }

    /// `getShort(name)`.
    pub fn get_short(&self, name: &str) -> Result<i16, KafkaError> {
        if let Some(v) = self.post_processed.get(name) {
            return v
                .as_i16()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a short: {v:?}")));
        }
        self.inner.get_short(name)
    }

    /// `getDouble(name)`.
    pub fn get_double(&self, name: &str) -> Result<f64, KafkaError> {
        if let Some(v) = self.post_processed.get(name) {
            return v
                .as_f64()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a double: {v:?}")));
        }
        self.inner.get_double(name)
    }

    /// `getBoolean(name)`.
    pub fn get_boolean(&self, name: &str) -> Result<bool, KafkaError> {
        if let Some(v) = self.post_processed.get(name) {
            return v
                .as_bool()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a boolean: {v:?}")));
        }
        self.inner.get_boolean(name)
    }

    /// `getList(name)`.
    pub fn get_list(&self, name: &str) -> Result<&[String], KafkaError> {
        if let Some(v) = self.post_processed.get(name) {
            return v
                .as_list()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a list: {v:?}")));
        }
        self.inner.get_list(name)
    }

    /// `getClass(name)` — returns the FQCN string (no reflective class
    /// loading in Rust).
    pub fn get_class(&self, name: &str) -> Result<&str, KafkaError> {
        if let Some(v) = self.post_processed.get(name) {
            return v
                .as_str()
                .ok_or_else(|| KafkaError::Config(format!("Configuration '{name}' is not a class: {v:?}")));
        }
        self.inner.get_class(name)
    }

    /// `originals()`.
    pub fn originals(&self) -> &HashMap<String, String> {
        self.inner.originals()
    }

    /// `originalsStrings()`.
    pub fn originals_strings(&self) -> HashMap<String, String> {
        self.inner.originals_strings()
    }

    // -------------------------------------------------------------
    // postProcessParsedConfig — mirrors Java's
    // `protected Map<String, Object> postProcessParsedConfig(Map<String, Object>)`
    // override on AbstractConfig (ProducerConfig.java line 569-577).
    //
    // Order matches Java:
    // 1. CommonClientConfigs.postValidateSaslMechanismConfig
    // 2. CommonClientConfigs.warnDisablingExponentialBackoff
    // 3. CommonClientConfigs.postProcessReconnectBackoffConfigs
    // 4. postProcessAndValidateIdempotenceConfigs
    // 5. maybeOverrideClientId
    //
    // Milestone-1 hard rejections (enable.idempotence=true,
    // transactional.id=<set>) run *between* steps 4 and 5. This
    // ordering preserves Java's `testUpperboundCheckOfEnableIdempotence`
    // error message exactly (the in-flight upper-bound check is part of
    // step 4 and Java throws *before* the brief's Milestone-1 hard
    // rejection runs). Step 5 is the only step that has externally
    // visible side effects (PRODUCER_CLIENT_ID_SEQUENCE increment), so
    // running rejections before it still keeps sequence ids from
    // burning on rejected configurations. Step 1 is a no-op for
    // non-SASL protocols (Milestone-1 enforces that via the
    // `security.protocol` validator).
    // -------------------------------------------------------------

    fn post_process_parsed_config(&mut self) -> Result<(), KafkaError> {
        // Step 1 — validate SASL mechanism iff security.protocol is
        // SASL-bearing.
        //
        // Phase 9b (Milestone-1): security.protocol accepts SASL_PLAINTEXT
        // / SASL_SSL but only `sasl.mechanism = PLAIN` is supported.
        // SCRAM-SHA-256/512, OAUTHBEARER, GSSAPI etc. are rejected here
        // with a Java-parity ConfigException.
        let security_protocol = self
            .inner
            .get_string(common_client_configs::SECURITY_PROTOCOL_CONFIG)?
            .to_owned();
        let sasl_mech = self
            .inner
            .values()
            .get(crate::common::config::sasl_configs::SASL_MECHANISM)
            .and_then(ConfigValue::as_str)
            .map(str::to_owned);
        common_client_configs::post_validate_sasl_mechanism_config(&security_protocol, sasl_mech.as_deref())?;
        // Milestone-1 narrowing: PLAIN only.
        Self::reject_milestone_1_unsupported_sasl_mechanism(&security_protocol, sasl_mech.as_deref())?;

        // Step 2 — log a warning when retry / connection-setup backoff
        // base > max.
        let retry_backoff_ms = self.inner.get_long(RETRY_BACKOFF_MS_CONFIG)?;
        let retry_backoff_max_ms = self.inner.get_long(RETRY_BACKOFF_MAX_MS_CONFIG)?;
        let conn_setup_ms = self.inner.get_long(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)?;
        let conn_setup_max_ms = self.inner.get_long(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)?;
        common_client_configs::warn_disabling_exponential_backoff(
            retry_backoff_ms,
            retry_backoff_max_ms,
            conn_setup_ms,
            conn_setup_max_ms,
        );

        // Step 3 — exponential reconnect-backoff override.
        let originals = self.inner.originals();
        let overrides = common_client_configs::post_process_reconnect_backoff_configs(
            originals.contains_key(RECONNECT_BACKOFF_MS_CONFIG),
            originals.contains_key(RECONNECT_BACKOFF_MAX_MS_CONFIG),
        );
        if overrides.override_max_to_base {
            let base = self.inner.get_long(RECONNECT_BACKOFF_MS_CONFIG)?;
            self.post_processed
                .insert(RECONNECT_BACKOFF_MAX_MS_CONFIG.to_owned(), ConfigValue::Long(base));
        }

        // Milestone-1 hard rejection: transactional.id. This runs
        // *before* step 4 because step 4 itself raises
        // "Cannot set transactional.id without also enabling
        // idempotence" (since Milestone-1 defaults idempotence to
        // false), which would shadow the Milestone-1-specific
        // message.
        self.reject_milestone_1_transactional_id()?;

        // Step 4 — postProcessAndValidateIdempotenceConfigs.
        // This *must* run before the Milestone-1 idempotence-rejection
        // so that `testUpperboundCheckOfEnableIdempotence` (Java) sees
        // the canonical error message verbatim. The Milestone-1
        // rejection for `enable.idempotence=true` runs immediately
        // after.
        self.post_process_and_validate_idempotence_configs()?;

        // Milestone-1 hard rejection: enable.idempotence=true. Runs
        // after step 4 (so the in-flight upperbound check fires first
        // when the user combined `enable.idempotence=true` with a
        // too-large `max.in.flight`) but before step 5 so
        // PRODUCER_CLIENT_ID_SEQUENCE is not advanced.
        self.reject_milestone_1_idempotence()?;

        // Step 5 — maybeOverrideClientId.
        self.maybe_override_client_id()?;

        Ok(())
    }

    /// Milestone-1 hard rejection for `transactional.id`. Runs before the
    /// Java idempotence post-validation because that step raises a
    /// different error ("Cannot set transactional.id without also
    /// enabling idempotence") under our Milestone-1 default
    /// (`enable.idempotence=false`), which would shadow the
    /// Milestone-1-specific message users expect.
    fn reject_milestone_1_transactional_id(&self) -> Result<(), KafkaError> {
        if let Some(raw) = self.inner.originals().get(TRANSACTIONAL_ID_CONFIG)
            && !raw.is_empty()
        {
            return Err(KafkaError::Config(
                "Transactional producer is not supported in this milestone (Milestone-1). Unset transactional.id. See \
                 Milestone-1/PLAN.md."
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Milestone-1 hard rejection for SASL mechanisms other than PLAIN.
    /// Phase 9b accepts `security.protocol = SASL_PLAINTEXT / SASL_SSL`
    /// but only `sasl.mechanism = PLAIN` is supported. SCRAM-SHA-256/512,
    /// OAUTHBEARER, GSSAPI etc. are out of Milestone-1 scope (PLAN.md:365).
    ///
    /// Returns a `KafkaError::Config` with an error message that names
    /// the offending mechanism — matches Java's
    /// `UnsupportedSaslMechanismException` shape (broker-side wire code
    /// 33). Surfaces at config-validation time so the producer can't be
    /// constructed in the first place.
    fn reject_milestone_1_unsupported_sasl_mechanism(
        security_protocol: &str,
        sasl_mechanism: Option<&str>,
    ) -> Result<(), KafkaError> {
        let is_sasl = security_protocol == "SASL_PLAINTEXT" || security_protocol == "SASL_SSL";
        if !is_sasl {
            return Ok(());
        }
        // post_validate_sasl_mechanism_config above already rejected
        // empty/null mechanisms; here we only narrow the supported set
        // for Milestone-1.
        if let Some(mech) = sasl_mechanism
            && !mech.is_empty()
            && mech != "PLAIN"
        {
            return Err(KafkaError::Config(format!(
                "Unsupported SASL mechanism: {mech}. Milestone-1 supports only PLAIN. \
                 See Milestone-1/PLAN.md:365."
            )));
        }
        Ok(())
    }

    /// Milestone-1 hard rejection for `enable.idempotence=true`. Runs
    /// *after* the Java idempotence post-validation so that users who
    /// pair `enable.idempotence=true` with a too-large
    /// `max.in.flight.requests.per.connection` see Java's canonical
    /// upper-bound error message before the Milestone-1 message.
    fn reject_milestone_1_idempotence(&self) -> Result<(), KafkaError> {
        if self.inner.get_boolean(ENABLE_IDEMPOTENCE_CONFIG)? {
            return Err(KafkaError::Config(
                "Idempotent producer is not supported in this milestone (Milestone-1). Set enable.idempotence=false. \
                 See Milestone-1/PLAN.md."
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Mirrors `ProducerConfig.parseAcks(String)` (line 653). Converts
    /// `"all"` → `"-1"`, otherwise parses as a `short` and returns its
    /// canonical string form.
    fn parse_acks(acks_string: &str) -> Result<String, KafkaError> {
        let trimmed = acks_string.trim();
        if trimmed.eq_ignore_ascii_case("all") {
            return Ok("-1".to_owned());
        }
        match trimmed.parse::<i16>() {
            Ok(n) => Ok(n.to_string()),
            Err(_) => Err(KafkaError::Config(format!(
                "Invalid configuration value for 'acks': {acks_string}"
            ))),
        }
    }

    /// Mirrors `ProducerConfig.maybeOverrideClientId(Map)` (line 579).
    /// Sets `client.id` to `producer-<transactional-id-or-seq>` when the
    /// user did not configure one explicitly.
    fn maybe_override_client_id(&mut self) -> Result<(), KafkaError> {
        let user_configured = self.inner.originals().contains_key(CLIENT_ID_CONFIG);
        let refined = if user_configured {
            self.inner.get_string(CLIENT_ID_CONFIG)?.to_owned()
        } else {
            // `transactional.id` may be `Null` (default) or a non-empty
            // string. Milestone-1 rejects non-null/non-empty values
            // upstream, so by the time we get here `transactional.id` is
            // always null/empty.
            let transactional_id = self.inner.values().get(TRANSACTIONAL_ID_CONFIG).and_then(|v| match v {
                ConfigValue::String(s) if !s.is_empty() => Some(s.clone()),
                _ => None,
            });
            match transactional_id {
                Some(s) => format!("producer-{s}"),
                None => format!(
                    "producer-{}",
                    PRODUCER_CLIENT_ID_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                ),
            }
        };
        self.post_processed
            .insert(CLIENT_ID_CONFIG.to_owned(), ConfigValue::String(refined));
        Ok(())
    }

    /// Mirrors `ProducerConfig.postProcessAndValidateIdempotenceConfigs(Map)`
    /// (line 591). Validates idempotence dependencies (`acks=all`,
    /// `retries>0`, `max.in.flight<=5`), updates `acks`, and surfaces the
    /// `transaction.timeout.ms`/`transaction.two.phase.commit.enable`
    /// mutual-exclusion error.
    fn post_process_and_validate_idempotence_configs(&mut self) -> Result<(), KafkaError> {
        // Translate `acks` to its canonical numeric form (Java's
        // `parseAcks`). Always overwritten in `post_processed` so all
        // downstream readers see the canonical form ("-1" or "0"/"1").
        let acks_str = self.inner.get_string(ACKS_CONFIG)?.to_owned();
        let parsed_acks = Self::parse_acks(&acks_str)?;
        self.post_processed
            .insert(ACKS_CONFIG.to_owned(), ConfigValue::String(parsed_acks.clone()));

        let user_configured_idempotence = self.inner.originals().contains_key(ENABLE_IDEMPOTENCE_CONFIG);
        let mut idempotence_enabled = self.inner.get_boolean(ENABLE_IDEMPOTENCE_CONFIG)?;
        let mut should_disable_idempotence = false;

        if idempotence_enabled {
            let retries = self.inner.get_int(RETRIES_CONFIG)?;
            if retries == 0 {
                if user_configured_idempotence {
                    return Err(KafkaError::Config(format!(
                        "Must set {RETRIES_CONFIG} to non-zero when using the idempotent producer."
                    )));
                }
                log::info!("Idempotence will be disabled because {RETRIES_CONFIG} is set to 0.");
                should_disable_idempotence = true;
            }

            // Java parses `acksStr` as `Short.parseShort` — at this
            // point `parsed_acks` is the canonical numeric form so we
            // can parse it directly.
            let acks_short: i16 = parsed_acks
                .parse()
                .map_err(|_| KafkaError::Config(format!("Invalid configuration value for 'acks': {acks_str}")))?;
            if acks_short != -1 {
                if user_configured_idempotence {
                    return Err(KafkaError::Config(format!(
                        "Must set {ACKS_CONFIG} to all in order to use the idempotent producer. Otherwise we cannot \
                         guarantee idempotence."
                    )));
                }
                log::info!(
                    "Idempotence will be disabled because {ACKS_CONFIG} is set to {acks_short}, not set to 'all'."
                );
                should_disable_idempotence = true;
            }

            let in_flight = self.inner.get_int(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)?;
            if MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE < in_flight {
                return Err(KafkaError::Config(format!(
                    "To use the idempotent producer, {MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION} must be set to at most \
                     5. Current value is {in_flight}."
                )));
            }
        }

        if should_disable_idempotence {
            self.post_processed
                .insert(ENABLE_IDEMPOTENCE_CONFIG.to_owned(), ConfigValue::Boolean(false));
            idempotence_enabled = false;
        }

        // Validate `transactional.id` after idempotence post-validation
        // because `enable.idempotence` may have been overridden above.
        let user_configured_transactions = self.inner.originals().contains_key(TRANSACTIONAL_ID_CONFIG);
        if !idempotence_enabled && user_configured_transactions {
            return Err(KafkaError::Config(format!(
                "Cannot set a {TRANSACTIONAL_ID_CONFIG} without also enabling idempotence."
            )));
        }

        // Two-phase commit + transaction timeout mutual exclusion.
        let enable_2pc = self.inner.get_boolean(TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG)?;
        let user_configured_txn_timeout = self.inner.originals().contains_key(TRANSACTION_TIMEOUT_CONFIG);
        if enable_2pc && user_configured_txn_timeout {
            return Err(KafkaError::Config(format!(
                "Cannot set {TRANSACTION_TIMEOUT_CONFIG} when {TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG} is set to \
                 true. Transactions will not expire with two-phase commit enabled."
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
    }

    fn minimal_props() -> HashMap<String, String> {
        map(&[
            (BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
            (
                KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            ),
            (
                VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer",
            ),
        ])
    }

    #[test]
    fn schema_includes_core_keys() {
        let def = ProducerConfig::config_def();
        for key in [
            BOOTSTRAP_SERVERS_CONFIG,
            CLIENT_ID_CONFIG,
            ACKS_CONFIG,
            COMPRESSION_TYPE_CONFIG,
            BATCH_SIZE_CONFIG,
            LINGER_MS_CONFIG,
            ENABLE_IDEMPOTENCE_CONFIG,
            TRANSACTIONAL_ID_CONFIG,
            KEY_SERIALIZER_CLASS_CONFIG,
            VALUE_SERIALIZER_CLASS_CONFIG,
        ] {
            assert!(def.config_key(key).is_some(), "missing producer key: {key}");
        }
    }

    #[test]
    fn schema_includes_ssl_keys() {
        let def = ProducerConfig::config_def();
        assert!(
            def.config_key(crate::common::config::ssl_configs::SSL_PROTOCOL_CONFIG)
                .is_some()
        );
        assert!(
            def.config_key(crate::common::config::ssl_configs::SSL_TRUSTSTORE_TYPE_CONFIG)
                .is_some()
        );
    }

    #[test]
    fn schema_includes_sasl_keys() {
        let def = ProducerConfig::config_def();
        assert!(def.config_key(crate::common::config::sasl_configs::SASL_MECHANISM).is_some());
        assert!(def.config_key(crate::common::config::sasl_configs::SASL_JAAS_CONFIG).is_some());
        // Phase 9b: fresh-impl convenience keys.
        assert!(def.config_key(crate::common::config::sasl_configs::SASL_USERNAME).is_some());
        assert!(def.config_key(crate::common::config::sasl_configs::SASL_PASSWORD).is_some());
    }

    #[test]
    fn config_names_is_non_empty() {
        let names = ProducerConfig::config_names();
        assert!(names.contains(&BOOTSTRAP_SERVERS_CONFIG));
        assert!(names.len() > 30);
    }

    #[test]
    fn enable_idempotence_default_is_false_milestone_1() {
        let def = ProducerConfig::config_def();
        let key = def.config_key(ENABLE_IDEMPOTENCE_CONFIG).unwrap();
        assert_eq!(key.default_value.as_ref().and_then(ConfigValue::as_bool), Some(false));
    }

    #[test]
    fn build_minimal_producer_config_succeeds() {
        let cfg = ProducerConfig::new(minimal_props()).unwrap();
        // bootstrap.servers is a List per the schema.
        let servers = cfg.get_list(BOOTSTRAP_SERVERS_CONFIG).unwrap();
        assert_eq!(servers, &["localhost:9092"]);
        // Defaults that the producer internals consume.
        assert_eq!(cfg.get_int(BATCH_SIZE_CONFIG).unwrap(), 16_384);
        assert_eq!(cfg.get_long(LINGER_MS_CONFIG).unwrap(), 5);
        // post-processed: "all" → "-1" (matches Java parseAcks).
        assert_eq!(cfg.get_string(ACKS_CONFIG).unwrap(), "-1");
        assert!(!cfg.get_boolean(ENABLE_IDEMPOTENCE_CONFIG).unwrap());
    }

    #[test]
    fn missing_key_serializer_is_rejected() {
        let mut props = minimal_props();
        props.remove(KEY_SERIALIZER_CLASS_CONFIG);
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains(KEY_SERIALIZER_CLASS_CONFIG));
    }

    #[test]
    fn append_serializer_to_config_full_args() {
        let configs: HashMap<String, Option<String>> = HashMap::new();
        let merged = ProducerConfig::append_serializer_to_config(&configs, Some("KCls"), Some("VCls")).unwrap();
        assert_eq!(merged.get(KEY_SERIALIZER_CLASS_CONFIG), Some(&Some("KCls".to_owned())));
        assert_eq!(merged.get(VALUE_SERIALIZER_CLASS_CONFIG), Some(&Some("VCls".to_owned())));
    }

    #[test]
    fn append_serializer_to_config_uses_existing_when_none_supplied() {
        let mut configs: HashMap<String, Option<String>> = HashMap::new();
        configs.insert(KEY_SERIALIZER_CLASS_CONFIG.to_owned(), Some("ExistingK".to_owned()));
        configs.insert(VALUE_SERIALIZER_CLASS_CONFIG.to_owned(), Some("ExistingV".to_owned()));
        let merged = ProducerConfig::append_serializer_to_config(&configs, None, None).unwrap();
        assert_eq!(merged.get(KEY_SERIALIZER_CLASS_CONFIG), Some(&Some("ExistingK".to_owned())));
        assert_eq!(merged.get(VALUE_SERIALIZER_CLASS_CONFIG), Some(&Some("ExistingV".to_owned())));
    }

    #[test]
    fn append_serializer_to_config_missing_key_serializer_is_rejected() {
        let mut configs: HashMap<String, Option<String>> = HashMap::new();
        configs.insert(KEY_SERIALIZER_CLASS_CONFIG.to_owned(), None);
        configs.insert(VALUE_SERIALIZER_CLASS_CONFIG.to_owned(), Some("V".to_owned()));
        let err = ProducerConfig::append_serializer_to_config(&configs, None, None).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains(KEY_SERIALIZER_CLASS_CONFIG));
    }

    // =================================================================
    // Translations of `org.apache.kafka.clients.producer.ProducerConfigTest`.
    // The test names match the Java methods (camelCase → snake_case) so
    // future review can grep across both codebases.
    // =================================================================

    const KEY_SERIALIZER_CLASS: &str = "org.apache.kafka.common.serialization.ByteArraySerializer";
    const VALUE_SERIALIZER_CLASS: &str = "org.apache.kafka.common.serialization.StringSerializer";

    /// Java: `testAppendSerializerToConfig`.
    ///
    /// Java passes Serializer instances; Rust passes the FQCN strings since
    /// Rust does not perform reflective class loading.
    #[test]
    fn test_append_serializer_to_config() {
        // Case 1: both serializer classes already in the map, no instances supplied.
        let mut configs: HashMap<String, Option<String>> = HashMap::new();
        configs.insert(KEY_SERIALIZER_CLASS_CONFIG.to_owned(), Some(KEY_SERIALIZER_CLASS.to_owned()));
        configs.insert(
            VALUE_SERIALIZER_CLASS_CONFIG.to_owned(),
            Some(VALUE_SERIALIZER_CLASS.to_owned()),
        );
        let new_configs = ProducerConfig::append_serializer_to_config(&configs, None, None).unwrap();
        assert_eq!(
            new_configs.get(KEY_SERIALIZER_CLASS_CONFIG),
            Some(&Some(KEY_SERIALIZER_CLASS.to_owned()))
        );
        assert_eq!(
            new_configs.get(VALUE_SERIALIZER_CLASS_CONFIG),
            Some(&Some(VALUE_SERIALIZER_CLASS.to_owned()))
        );

        // Case 2: only value class in map, key supplied as instance.
        let mut configs: HashMap<String, Option<String>> = HashMap::new();
        configs.insert(
            VALUE_SERIALIZER_CLASS_CONFIG.to_owned(),
            Some(VALUE_SERIALIZER_CLASS.to_owned()),
        );
        let new_configs =
            ProducerConfig::append_serializer_to_config(&configs, Some(KEY_SERIALIZER_CLASS), None).unwrap();
        assert_eq!(
            new_configs.get(KEY_SERIALIZER_CLASS_CONFIG),
            Some(&Some(KEY_SERIALIZER_CLASS.to_owned()))
        );
        assert_eq!(
            new_configs.get(VALUE_SERIALIZER_CLASS_CONFIG),
            Some(&Some(VALUE_SERIALIZER_CLASS.to_owned()))
        );

        // Case 3: only key class in map, value supplied as instance.
        let mut configs: HashMap<String, Option<String>> = HashMap::new();
        configs.insert(KEY_SERIALIZER_CLASS_CONFIG.to_owned(), Some(KEY_SERIALIZER_CLASS.to_owned()));
        let new_configs =
            ProducerConfig::append_serializer_to_config(&configs, None, Some(VALUE_SERIALIZER_CLASS)).unwrap();
        assert_eq!(
            new_configs.get(KEY_SERIALIZER_CLASS_CONFIG),
            Some(&Some(KEY_SERIALIZER_CLASS.to_owned()))
        );
        assert_eq!(
            new_configs.get(VALUE_SERIALIZER_CLASS_CONFIG),
            Some(&Some(VALUE_SERIALIZER_CLASS.to_owned()))
        );

        // Case 4: empty map, both supplied as instances.
        let configs: HashMap<String, Option<String>> = HashMap::new();
        let new_configs = ProducerConfig::append_serializer_to_config(
            &configs,
            Some(KEY_SERIALIZER_CLASS),
            Some(VALUE_SERIALIZER_CLASS),
        )
        .unwrap();
        assert_eq!(
            new_configs.get(KEY_SERIALIZER_CLASS_CONFIG),
            Some(&Some(KEY_SERIALIZER_CLASS.to_owned()))
        );
        assert_eq!(
            new_configs.get(VALUE_SERIALIZER_CLASS_CONFIG),
            Some(&Some(VALUE_SERIALIZER_CLASS.to_owned()))
        );
    }

    /// Java: `testAppendSerializerToConfigWithException`.
    #[test]
    fn test_append_serializer_to_config_with_exception() {
        // Case 1: key explicitly null in map, value class set, no key
        // serializer supplied — must throw.
        let mut configs: HashMap<String, Option<String>> = HashMap::new();
        configs.insert(KEY_SERIALIZER_CLASS_CONFIG.to_owned(), None);
        configs.insert(
            VALUE_SERIALIZER_CLASS_CONFIG.to_owned(),
            Some(VALUE_SERIALIZER_CLASS.to_owned()),
        );
        let err =
            ProducerConfig::append_serializer_to_config(&configs, None, Some(VALUE_SERIALIZER_CLASS)).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));

        // Case 2: value explicitly null in map, key class set, no value
        // serializer supplied — must throw.
        let mut configs: HashMap<String, Option<String>> = HashMap::new();
        configs.insert(KEY_SERIALIZER_CLASS_CONFIG.to_owned(), Some(KEY_SERIALIZER_CLASS.to_owned()));
        configs.insert(VALUE_SERIALIZER_CLASS_CONFIG.to_owned(), None);
        let err = ProducerConfig::append_serializer_to_config(&configs, Some(KEY_SERIALIZER_CLASS), None).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
    }

    /// Java: `testInvalidCompressionType`.
    #[test]
    fn test_invalid_compression_type() {
        let mut props = minimal_props();
        props.insert(COMPRESSION_TYPE_CONFIG.to_owned(), "abc".to_owned());
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains(COMPRESSION_TYPE_CONFIG));
    }

    /// Java: `testInvalidSecurityProtocol`.
    #[test]
    fn test_invalid_security_protocol() {
        let mut props = minimal_props();
        props.insert(common_client_configs::SECURITY_PROTOCOL_CONFIG.to_owned(), "abc".to_owned());
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains(common_client_configs::SECURITY_PROTOCOL_CONFIG));
    }

    /// Java: `testDefaultMetadataRecoveryStrategy`.
    #[test]
    fn test_default_metadata_recovery_strategy() {
        let cfg = ProducerConfig::new(minimal_props()).unwrap();
        assert_eq!(
            cfg.get_string(common_client_configs::METADATA_RECOVERY_STRATEGY_CONFIG)
                .unwrap(),
            MetadataRecoveryStrategy::Rebootstrap.name(),
        );
    }

    /// Java: `testInvalidMetadataRecoveryStrategy`.
    #[test]
    fn test_invalid_metadata_recovery_strategy() {
        let mut props = minimal_props();
        props.insert(
            common_client_configs::METADATA_RECOVERY_STRATEGY_CONFIG.to_owned(),
            "abc".to_owned(),
        );
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains(common_client_configs::METADATA_RECOVERY_STRATEGY_CONFIG));
    }

    /// Java: `testCaseInsensitiveSecurityProtocol`.
    ///
    /// **Milestone-1 deviation**: Java uses `SASL_SSL.toLowerCase()` here.
    /// SASL is rejected at the validator in Milestone-1 (Phase 9 will
    /// re-enable). We substitute `Ssl` (mixed-case) to exercise the same
    /// case-insensitive behaviour. See design/history/Milestone-1/PLAN.md.
    #[test]
    fn test_case_insensitive_security_protocol() {
        let mixed_case = "Ssl";
        let mut props = minimal_props();
        props.insert(
            common_client_configs::SECURITY_PROTOCOL_CONFIG.to_owned(),
            mixed_case.to_owned(),
        );
        let cfg = ProducerConfig::new(props).unwrap();
        // Originals preserve the user-supplied casing exactly (Java parity).
        assert_eq!(
            cfg.originals()
                .get(common_client_configs::SECURITY_PROTOCOL_CONFIG)
                .map(String::as_str),
            Some(mixed_case),
        );
    }

    /// Java: `testUpperboundCheckOfEnableIdempotence`.
    ///
    /// We exercise the message-content assertion using the path Java
    /// reaches when the user explicitly opts into idempotence and pushes
    /// `max.in.flight.requests.per.connection` beyond 5 — the validator
    /// throws **before** the Milestone-1 `enable.idempotence=true`
    /// rejection (the brief explicitly preserves this test).
    #[test]
    fn test_upperbound_check_of_enable_idempotence() {
        let in_flight = "6";
        let mut props = minimal_props();
        props.insert(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.to_owned(), in_flight.to_owned());
        props.insert(ENABLE_IDEMPOTENCE_CONFIG.to_owned(), "true".to_owned());
        let err = ProducerConfig::new(props).unwrap_err();
        let expected_msg = format!(
            "To use the idempotent producer, {MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION} must be set to at most 5. \
             Current value is {in_flight}."
        );
        assert_eq!(err.message(), expected_msg);

        // With max.in.flight=5 the test would normally pass. Milestone-1
        // rejects `enable.idempotence=true` upstream, which is exercised
        // by `test_idempotence_true_rejected_in_milestone_1`. Asserting
        // that path here would be a duplicate.
    }

    // Java: `testTwoPhaseCommitIncompatibleWithTransactionTimeout`.
    //
    // The Java test additionally sets `enable.idempotence=true` and
    // `transactional.id="test-txn-id"` — both rejected at construction
    // in Milestone-1 (idempotent + transactional producers are out of
    // scope until Phases 8 & 9). However, the underlying invariant that
    // Java's test exercises (`transaction.two.phase.commit.enable=true`
    // is mutually exclusive with a user-supplied `transaction.timeout.ms`)
    // is reachable in Milestone-1 without those two properties. We
    // therefore translate the Milestone-1-reachable subset here. The
    // success cases (2pc=true alone, or timeout alone) are also covered
    // for symmetry with Java.
    //
    // TODO Phase 9: also add the variants that combine 2pc with
    // `enable.idempotence=true` and `transactional.id="test-txn-id"`
    // once those are permitted again — at which point this should be
    // renamed back to `test_two_phase_commit_incompatible_with_transaction_timeout`
    // and the additional setters reinstated to match Java verbatim.
    #[test]
    fn test_two_phase_commit_rejects_explicit_transaction_timeout() {
        // 2pc=true + explicit transaction.timeout.ms must reject.
        let mut props = minimal_props();
        props.insert(TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG.to_owned(), "true".to_owned());
        props.insert(TRANSACTION_TIMEOUT_CONFIG.to_owned(), "60000".to_owned());
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        let msg = err.message();
        let expected_msg = format!(
            "Cannot set {TRANSACTION_TIMEOUT_CONFIG} when {TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG} is set to \
             true. Transactions will not expire with two-phase commit enabled."
        );
        assert_eq!(msg, expected_msg);

        // 2pc=true alone (no explicit timeout) must succeed.
        let mut props = minimal_props();
        props.insert(TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG.to_owned(), "true".to_owned());
        ProducerConfig::new(props).expect("2pc=true without explicit timeout is valid");

        // Explicit timeout alone (no 2pc) must succeed.
        let mut props = minimal_props();
        props.insert(TRANSACTION_TIMEOUT_CONFIG.to_owned(), "60000".to_owned());
        ProducerConfig::new(props).expect("explicit timeout without 2pc is valid");
    }

    /// Java: `testValidateConfigPropertiesFile`.
    ///
    /// Java reads `kafka/config/producer.properties` from disk via
    /// `System.getProperty("user.dir")`. The Rust translation reads our
    /// own copy at `tests/data/producer.properties` (with
    /// `enable.idempotence=true` commented out — Milestone-1 deviation
    /// documented in that file).
    #[test]
    fn test_validate_config_properties_file() {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");
        let path = std::path::Path::new(&manifest).join("tests/data/producer.properties");
        let contents = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        let mut props: HashMap<String, String> = HashMap::new();
        for raw_line in contents.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                props.insert(k.trim().to_owned(), v.trim().to_owned());
            }
        }
        // Constructing must succeed (Milestone-1 deviations listed in
        // tests/data/producer.properties).
        let cfg = ProducerConfig::new(props).expect("producer.properties is valid");
        // Every key in `originals` must be a known schema key.
        let def = ProducerConfig::config_def();
        for key in cfg.originals().keys() {
            assert!(def.config_key(key).is_some(), "Invalid configuration key: {key}");
        }
    }

    // ----- Milestone-1 hard rejections -----

    /// Sets `enable.idempotence=true`; the constructor must reject with a
    /// message that contains the literal `Milestone-1`.
    #[test]
    fn test_idempotence_true_rejected_in_milestone_1() {
        let mut props = minimal_props();
        props.insert(ENABLE_IDEMPOTENCE_CONFIG.to_owned(), "true".to_owned());
        // Default `max.in.flight=5` lets the idempotence-validator pass;
        // the Milestone-1 hard rejection runs *after* the validator.
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        let msg = err.message();
        assert!(msg.contains("Milestone-1"), "got: {msg}");
        assert!(msg.contains("enable.idempotence=false"), "got: {msg}");
    }

    /// Sets `transactional.id=foo`; the constructor must reject with a
    /// message that contains the literal `Milestone-1`.
    #[test]
    fn test_transactional_id_rejected_in_milestone_1() {
        let mut props = minimal_props();
        props.insert(TRANSACTIONAL_ID_CONFIG.to_owned(), "foo".to_owned());
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        let msg = err.message();
        assert!(msg.contains("Milestone-1"), "got: {msg}");
        assert!(msg.contains("transactional.id"), "got: {msg}");
    }

    /// Sets `security.protocol=SASL_SSL`; the validator on the
    /// Phase 9b: `security.protocol=SASL_SSL` is now accepted by the
    /// validator on the key itself (PLAIN-only narrowing lives
    /// downstream). The default `sasl.mechanism` is `GSSAPI` (Java
    /// default), so a bare `SASL_SSL` without an explicit PLAIN
    /// mechanism falls into the Milestone-1 narrowing and surfaces a
    /// "Unsupported SASL mechanism: GSSAPI" config error.
    #[test]
    fn test_sasl_ssl_default_mechanism_rejected_in_milestone_1() {
        let mut props = minimal_props();
        props.insert(
            common_client_configs::SECURITY_PROTOCOL_CONFIG.to_owned(),
            "SASL_SSL".to_owned(),
        );
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(
            err.message().contains("Unsupported SASL mechanism: GSSAPI"),
            "got: {}",
            err.message()
        );
    }

    /// Phase 9b: `security.protocol=SASL_PLAINTEXT` + explicit
    /// `sasl.mechanism=PLAIN` is accepted. The remaining
    /// `sasl.jaas.config` plumbing for actual credentials happens
    /// at producer-construction time (Phase 9b commit 6).
    #[test]
    fn test_sasl_plaintext_plain_mechanism_accepted() {
        let mut props = minimal_props();
        props.insert(
            common_client_configs::SECURITY_PROTOCOL_CONFIG.to_owned(),
            "SASL_PLAINTEXT".to_owned(),
        );
        props.insert(
            crate::common::config::sasl_configs::SASL_MECHANISM.to_owned(),
            "PLAIN".to_owned(),
        );
        let config = ProducerConfig::new(props).expect("SASL_PLAINTEXT + PLAIN must be accepted");
        assert_eq!(
            config
                .inner
                .get_string(common_client_configs::SECURITY_PROTOCOL_CONFIG)
                .unwrap(),
            "SASL_PLAINTEXT"
        );
        assert_eq!(
            config
                .inner
                .get_string(crate::common::config::sasl_configs::SASL_MECHANISM)
                .unwrap(),
            "PLAIN"
        );
    }

    /// Phase 9b: `sasl.mechanism = SCRAM-SHA-512` is rejected (PLAN.md:365)
    /// with a Milestone-1-specific error message naming the offending
    /// mechanism.
    #[test]
    fn test_sasl_scram_mechanism_rejected() {
        let mut props = minimal_props();
        props.insert(
            common_client_configs::SECURITY_PROTOCOL_CONFIG.to_owned(),
            "SASL_PLAINTEXT".to_owned(),
        );
        props.insert(
            crate::common::config::sasl_configs::SASL_MECHANISM.to_owned(),
            "SCRAM-SHA-512".to_owned(),
        );
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(
            err.message().contains("Unsupported SASL mechanism: SCRAM-SHA-512"),
            "got: {}",
            err.message()
        );
    }

    /// Phase 9b: `sasl.mechanism = OAUTHBEARER` is rejected (PLAN.md:365).
    #[test]
    fn test_sasl_oauthbearer_mechanism_rejected() {
        let mut props = minimal_props();
        props.insert(
            common_client_configs::SECURITY_PROTOCOL_CONFIG.to_owned(),
            "SASL_SSL".to_owned(),
        );
        props.insert(
            crate::common::config::sasl_configs::SASL_MECHANISM.to_owned(),
            "OAUTHBEARER".to_owned(),
        );
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(
            err.message().contains("Unsupported SASL mechanism: OAUTHBEARER"),
            "got: {}",
            err.message()
        );
    }

    /// Phase 9b: when `security.protocol = PLAINTEXT / SSL`,
    /// `sasl.mechanism` is irrelevant — the validator must not surface
    /// the SASL-mechanism rejection for non-SASL protocols (PLAN.md
    /// post_validate_sasl_mechanism_config semantics).
    #[test]
    fn test_non_sasl_protocol_ignores_sasl_mechanism() {
        let mut props = minimal_props();
        props.insert(
            common_client_configs::SECURITY_PROTOCOL_CONFIG.to_owned(),
            "PLAINTEXT".to_owned(),
        );
        // Even though SCRAM is in the props, the validator should not
        // care because security.protocol is not SASL-bearing.
        props.insert(
            crate::common::config::sasl_configs::SASL_MECHANISM.to_owned(),
            "SCRAM-SHA-256".to_owned(),
        );
        let _config = ProducerConfig::new(props).expect("PLAINTEXT must not validate SASL mechanism");
    }

    // ----- Auxiliary tests for parseAcks (Java parseAcks line 653) -----

    #[test]
    fn parse_acks_translates_all_to_minus_one() {
        assert_eq!(ProducerConfig::parse_acks("all").unwrap(), "-1");
        assert_eq!(ProducerConfig::parse_acks("All").unwrap(), "-1");
        assert_eq!(ProducerConfig::parse_acks("ALL").unwrap(), "-1");
    }

    #[test]
    fn parse_acks_passes_numeric_through() {
        assert_eq!(ProducerConfig::parse_acks("0").unwrap(), "0");
        assert_eq!(ProducerConfig::parse_acks("1").unwrap(), "1");
        assert_eq!(ProducerConfig::parse_acks("-1").unwrap(), "-1");
    }

    #[test]
    fn parse_acks_rejects_garbage() {
        let err = ProducerConfig::parse_acks("xyz").unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.message().contains("acks"));
    }

    #[test]
    fn maybe_override_client_id_uses_user_value_when_provided() {
        let mut props = minimal_props();
        props.insert(CLIENT_ID_CONFIG.to_owned(), "my-client".to_owned());
        let cfg = ProducerConfig::new(props).unwrap();
        assert_eq!(cfg.get_string(CLIENT_ID_CONFIG).unwrap(), "my-client");
    }

    #[test]
    fn maybe_override_client_id_falls_back_to_sequence() {
        let cfg = ProducerConfig::new(minimal_props()).unwrap();
        let client_id = cfg.get_string(CLIENT_ID_CONFIG).unwrap();
        assert!(
            client_id.starts_with("producer-"),
            "expected 'producer-<seq>' got '{client_id}'"
        );
    }
}
