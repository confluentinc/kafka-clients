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
use crate::common::config::{config_exception, sasl_configs, ssl_configs};
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
pub const KEY_SERIALIZER_CLASS_DOC: &str = "Serializer class for key that implements the `Serializer` interface.";

/// `value.serializer`
pub const VALUE_SERIALIZER_CLASS_CONFIG: &str = "value.serializer";
/// Doc string for [`VALUE_SERIALIZER_CLASS_CONFIG`].
pub const VALUE_SERIALIZER_CLASS_DOC: &str = "Serializer class for value that implements the `Serializer` interface.";

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
    "A list of classes to use as interceptors. Implementing the `ProducerInterceptor` interface allows you to ",
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
    "If 'false', producer retries due to broker failures, etc., may write duplicates. ",
    "Milestone-1 deviation: the default is 'false' and 'true' is rejected at construction (Phase 8 will re-enable). ",
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
    "proactively aborts it.",
);

/// `transactional.id`
pub const TRANSACTIONAL_ID_CONFIG: &str = "transactional.id";
/// Doc string for [`TRANSACTIONAL_ID_CONFIG`].
pub const TRANSACTIONAL_ID_DOC: &str = concat!(
    "The TransactionalId to use for transactional delivery. By default the TransactionId is not configured. ",
    "Milestone-1 deviation: any non-null/non-empty value is rejected at construction (Phase 9 will re-enable).",
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
    let zero_or_more_send_buffer: Arc<dyn Validator> =
        Arc::new(Range::at_least(common_client_configs::SEND_BUFFER_LOWER_BOUND));
    let zero_or_more_recv_buffer: Arc<dyn Validator> =
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

    // Milestone-1 deviation: only `PLAINTEXT` and `SSL` are accepted. Java
    // also accepts `SASL_PLAINTEXT` and `SASL_SSL`. The brief constrains
    // this validator to the two values so a `SASL_*` `security.protocol`
    // surfaces a `ConfigException` whose message contains the literal
    // `security.protocol` (per testInvalidSecurityProtocol).
    let security_protocol_validator: Arc<dyn Validator> =
        Arc::new(CaseInsensitiveValidString::in_set(["PLAINTEXT", "SSL"]));

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
        Some(zero_or_more_send_buffer),
        Importance::Medium,
        common_client_configs::SEND_BUFFER_DOC,
    )
    .unwrap()
    .define(
        RECEIVE_BUFFER_CONFIG,
        Type::Int,
        Some(ConfigValue::Int(32 * 1024)),
        Some(zero_or_more_recv_buffer),
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
}

impl std::fmt::Debug for ProducerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerConfig")
            .field("originals", self.inner.originals())
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
        let cfg = ProducerConfig { inner };
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
        self.inner.get_string(name)
    }

    /// `getInt(name)`.
    pub fn get_int(&self, name: &str) -> Result<i32, KafkaError> {
        self.inner.get_int(name)
    }

    /// `getLong(name)`.
    pub fn get_long(&self, name: &str) -> Result<i64, KafkaError> {
        self.inner.get_long(name)
    }

    /// `getShort(name)`.
    pub fn get_short(&self, name: &str) -> Result<i16, KafkaError> {
        self.inner.get_short(name)
    }

    /// `getDouble(name)`.
    pub fn get_double(&self, name: &str) -> Result<f64, KafkaError> {
        self.inner.get_double(name)
    }

    /// `getBoolean(name)`.
    pub fn get_boolean(&self, name: &str) -> Result<bool, KafkaError> {
        self.inner.get_boolean(name)
    }

    /// `getList(name)`.
    pub fn get_list(&self, name: &str) -> Result<&[String], KafkaError> {
        self.inner.get_list(name)
    }

    /// `getClass(name)` — returns the FQCN string (no reflective class
    /// loading in Rust).
    pub fn get_class(&self, name: &str) -> Result<&str, KafkaError> {
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
    // postProcessParsedConfig — Phase 7a (4/N) will land the full
    // implementation. The Phase 7a (3/N) commit ships the schema and
    // constructor; the post-process step here only enforces the
    // Milestone-1 rejections so the constructor cannot accept an
    // unsupported configuration.
    // -------------------------------------------------------------

    fn post_process_parsed_config(&self) -> Result<(), KafkaError> {
        // Will be expanded in Phase 7a (4/N) to include
        // postProcessReconnectBackoffConfigs, parseAcks,
        // maybeOverrideClientId, and the full idempotence
        // post-validation. For now we only enforce the Milestone-1
        // rejections so an unsupported configuration cannot slip
        // through.
        self.reject_milestone_1_unsupported()
    }

    fn reject_milestone_1_unsupported(&self) -> Result<(), KafkaError> {
        // 1. enable.idempotence=true rejected.
        if self.get_boolean(ENABLE_IDEMPOTENCE_CONFIG)? {
            return Err(KafkaError::Config(
                "Idempotent producer is not supported in this milestone (Milestone-1). Set enable.idempotence=false. \
                 See Milestone-1/PLAN.md."
                    .to_owned(),
            ));
        }
        // 2. transactional.id non-null/non-empty rejected.
        if self.originals().contains_key(TRANSACTIONAL_ID_CONFIG)
            && let Some(raw) = self.originals().get(TRANSACTIONAL_ID_CONFIG)
            && !raw.is_empty()
        {
            return Err(KafkaError::Config(
                "Transactional producer is not supported in this milestone (Milestone-1). Unset transactional.id. \
                 See Milestone-1/PLAN.md."
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

// `sasl_configs` and `ssl_configs` are imported above so that the
// `with_client_*_support` calls in `build_config_def` resolve. Re-export
// is intentionally not done here — callers depend on the canonical paths.
#[allow(unused_imports)]
use sasl_configs as _sasl_anchor;
#[allow(unused_imports)]
use ssl_configs as _ssl_anchor;

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
        assert_eq!(cfg.get_string(ACKS_CONFIG).unwrap(), "all");
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
}
