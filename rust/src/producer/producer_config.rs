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

//! Configuration for the Kafka Producer.
//!
//! Corresponds to `org.apache.kafka.clients.producer.ProducerConfig`: the keys
//! are defined on a `ConfigDef` in Java's order (`ProducerConfig.java:405-618`)
//! and parsed and validated by it, `postProcessParsedConfig` runs Java's steps
//! (`ProducerConfig.java:620-628`), and the configuration answers Java's
//! `AbstractConfig` getters.
//!
//! Not defined, and so reported by [`ProducerConfig::unused`] and the
//! client's [`ProducerConfig::log_unused`] line when set:
//!
//! - `key.serializer`, `value.serializer`, `partitioner.class`,
//!   `metric.reporters` and `interceptor.classes`: classes or lists of
//!   classes, which a configuration never takes (CLAUDE.md §2). A partitioner
//!   is chosen with the Rust-only `partitioner.type` key or set with
//!   [`ProducerConfig::set_partitioner`].
//! - `security.providers`: Java's JCA security providers have no counterpart.
//! - `config.providers` is defined, as Java defines it, but config providers
//!   are not translated, so it is never read.
//!
//! `AbstractConfig`'s `getPassword`, `typeOf`, `originals*()` and `values*()`
//! are not translated yet: each needs a public type the crate does not have
//! (`Password`, `ConfigDef.Type`, Java's `RecordingMap`). `documentationOf`
//! is not translated either: the documentation strings are not kept, since
//! only it and Java's documentation renderers read them. See `CHANGELOG.md`.

use std::any::{Any, type_name};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::LazyLock;
use std::sync::atomic::{self, AtomicI32};

use log::info;

use crate::MetadataRecoveryStrategy;
use crate::common::Error;
use crate::common::config::abstract_config::{AbstractConfig, ConfigMap, ValuesReader, string_originals};
use crate::common::config::config_def::{
    CaseInsensitiveValidString, ConfigDef, DefaultValue, Importance, LambdaValidator, NonEmptyString, ParsedValue,
    Range, Type, ValidList, ValidString, Validator, config_error,
};
use crate::common::config::{SaslConfigs, SslConfigs};
use crate::common::record::internal::CompressionType;
use crate::common::security::auth::SecurityProtocol;
use crate::producer::internals::KeyHasher;
use crate::producer::{Partitioner, RoundRobinPartitioner};
use crate::{ClientDnsLookup, CommonClientConfigs};

/// Process-wide counter for deriving a default `client.id`.
///
/// Corresponds to Java's `static AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE`
/// (`ProducerConfig.java:404`), which starts at 1. Crate-level (not
/// per-config) to match Java's static scope, so successive producers in one
/// process get distinct ids.
static PRODUCER_CLIENT_ID_SEQUENCE: AtomicI32 = AtomicI32::new(1);

/// Maximum number of in-flight requests per connection when idempotence is enabled.
/// Aligned with `ProducerStateEntry.NUM_BATCHES_TO_RETAIN` on the broker.
const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE: i32 = 5;

/// The validator of `partitioner.type`: a `null` value or one of the three
/// built-in partitioner names.
fn partitioner_type_validator() -> LambdaValidator {
    LambdaValidator::with(
        |name, value| {
            let Some(value) = value else {
                return Ok(());
            };
            let partitioner_type = value.to_string();
            if [
                ProducerConfig::CONSISTENT_RANDOM_PARTITIONER,
                ProducerConfig::MURMUR2_RANDOM_PARTITIONER,
                ProducerConfig::ROUND_ROBIN_PARTITIONER,
            ]
            .contains(&partitioner_type.as_str())
            {
                return Ok(());
            }
            Err(config_error(
                name,
                Some(value),
                format!(
                    "Partitioner type {partitioner_type} is not supported; the supported types are {}, {} and {}.",
                    ProducerConfig::CONSISTENT_RANDOM_PARTITIONER,
                    ProducerConfig::MURMUR2_RANDOM_PARTITIONER,
                    ProducerConfig::ROUND_ROBIN_PARTITIONER
                ),
            ))
        },
        || {
            format!(
                "[{}, {}, {}]",
                ProducerConfig::CONSISTENT_RANDOM_PARTITIONER,
                ProducerConfig::MURMUR2_RANDOM_PARTITIONER,
                ProducerConfig::ROUND_ROBIN_PARTITIONER
            )
        },
    )
}

/// The `ConfigDef` level validator of a compression type that has levels.
fn level_validator(compression_type: CompressionType) -> Box<dyn Validator> {
    compression_type
        .level_validator()
        .expect("gzip, lz4 and zstd have compression levels")
}

/// The default level of a compression type that has levels.
fn default_level(compression_type: CompressionType) -> i32 {
    compression_type
        .default_level()
        .expect("gzip, lz4 and zstd have compression levels")
}

static CONFIG: LazyLock<ConfigDef> = LazyLock::new(|| {
    ConfigDef::new()
        .define_with_validator(
            ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
            Type::List,
            ConfigDef::NO_DEFAULT_VALUE,
            ValidList::any_non_duplicate_values(false, false),
            Importance::High,
        )
        .define_with_validator(
            ProducerConfig::CLIENT_DNS_LOOKUP_CONFIG,
            Type::String,
            ClientDnsLookup::UseAllDnsIps.config_value(),
            ValidString::r#in(&[
                ClientDnsLookup::UseAllDnsIps.config_value(),
                ClientDnsLookup::ResolveCanonicalBootstrapServersOnly.config_value(),
            ]),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::BUFFER_MEMORY_CONFIG,
            Type::Long,
            32 * 1024 * 1024_i64,
            Range::at_least(0_i64),
            Importance::High,
        )
        // Internal until the incremental strategy is fully implemented
        .define_internal_with_validator(
            ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG,
            Type::String,
            ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_FULL,
            CaseInsensitiveValidString::r#in(&[
                ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_FULL,
                ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL,
            ]),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::RETRIES_CONFIG,
            Type::Int,
            i32::MAX,
            Range::between(0, i32::MAX),
            Importance::High,
        )
        .define_with_validator(
            ProducerConfig::ACKS_CONFIG,
            Type::String,
            "all",
            ValidString::r#in(&["all", "-1", "0", "1"]),
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::COMPRESSION_TYPE_CONFIG,
            Type::String,
            CompressionType::None.name(),
            ValidString::r#in(&CompressionType::names()),
            Importance::High,
        )
        .define_with_validator(
            ProducerConfig::COMPRESSION_GZIP_LEVEL_CONFIG,
            Type::Int,
            default_level(CompressionType::Gzip),
            level_validator(CompressionType::Gzip),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::COMPRESSION_LZ4_LEVEL_CONFIG,
            Type::Int,
            default_level(CompressionType::Lz4),
            level_validator(CompressionType::Lz4),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::COMPRESSION_ZSTD_LEVEL_CONFIG,
            Type::Int,
            default_level(CompressionType::Zstd),
            level_validator(CompressionType::Zstd),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::BATCH_SIZE_CONFIG,
            Type::Int,
            16384,
            Range::at_least(0),
            Importance::Medium,
        )
        .define(
            ProducerConfig::PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG,
            Type::Boolean,
            true,
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG,
            Type::Long,
            0_i64,
            Range::at_least(0),
            Importance::Low,
        )
        .define(
            ProducerConfig::PARTITIONER_IGNORE_KEYS_CONFIG,
            Type::Boolean,
            false,
            Importance::Medium,
        )
        .define(
            ProducerConfig::PARTITIONER_RACK_AWARE_CONFIG,
            Type::Boolean,
            false,
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::LINGER_MS_CONFIG,
            Type::Long,
            5_i64,
            Range::at_least(0),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG,
            Type::Int,
            120 * 1000,
            Range::at_least(0),
            Importance::Medium,
        )
        .define(ProducerConfig::CLIENT_ID_CONFIG, Type::String, "", Importance::Medium)
        .define(
            ProducerConfig::CLIENT_RACK_CONFIG,
            Type::String,
            ProducerConfig::DEFAULT_CLIENT_RACK,
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::SEND_BUFFER_CONFIG,
            Type::Int,
            128 * 1024,
            Range::at_least(CommonClientConfigs::SEND_BUFFER_LOWER_BOUND),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::RECEIVE_BUFFER_CONFIG,
            Type::Int,
            32 * 1024,
            Range::at_least(CommonClientConfigs::RECEIVE_BUFFER_LOWER_BOUND),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::MAX_REQUEST_SIZE_CONFIG,
            Type::Int,
            1024 * 1024,
            Range::at_least(0),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::RECONNECT_BACKOFF_MS_CONFIG,
            Type::Long,
            50_i64,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::RECONNECT_BACKOFF_MAX_MS_CONFIG,
            Type::Long,
            1000_i64,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::RETRY_BACKOFF_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_RETRY_BACKOFF_MS,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::RETRY_BACKOFF_MAX_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_RETRY_BACKOFF_MAX_MS,
            Range::at_least(0_i64),
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_BOOTSTRAP_RESOLVE_TIMEOUT_MS,
            Range::at_least(0_i64),
            Importance::High,
        )
        .define(ProducerConfig::ENABLE_METRICS_PUSH_CONFIG, Type::Boolean, true, Importance::Low)
        .define_with_validator(
            ProducerConfig::MAX_BLOCK_MS_CONFIG,
            Type::Long,
            60 * 1000_i64,
            Range::at_least(0),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::REQUEST_TIMEOUT_MS_CONFIG,
            Type::Int,
            30 * 1000,
            Range::at_least(0),
            Importance::Medium,
        )
        .define_with_validator(
            ProducerConfig::METADATA_MAX_AGE_CONFIG,
            Type::Long,
            5 * 60 * 1000_i64,
            Range::at_least(0),
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::METADATA_MAX_IDLE_CONFIG,
            Type::Long,
            5 * 60 * 1000_i64,
            Range::at_least(5000),
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::METRICS_SAMPLE_WINDOW_MS_CONFIG,
            Type::Long,
            30000_i64,
            Range::at_least(0),
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::METRICS_NUM_SAMPLES_CONFIG,
            Type::Int,
            2,
            Range::at_least(1),
            Importance::Low,
        )
        .define_with_validator(
            ProducerConfig::METRICS_RECORDING_LEVEL_CONFIG,
            Type::String,
            "INFO",
            ValidString::r#in(&["INFO", "DEBUG", "TRACE"]),
            Importance::Low,
        )
        // `metric.reporters` is a list of classes (CLAUDE.md §2): not defined.
        .define_with_validator(
            ProducerConfig::MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            Type::Int,
            5,
            Range::at_least(1),
            Importance::Low,
        )
        // `key.serializer` and `value.serializer` are classes (CLAUDE.md §2):
        // not defined.
        .define(
            ProducerConfig::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
            Importance::Medium,
        )
        .define(
            ProducerConfig::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
            Type::Long,
            CommonClientConfigs::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
            Importance::Medium,
        )
        // default is set to be a bit lower than the server default (10 min), to avoid both client and server closing
        // connection at same time
        .define(
            ProducerConfig::CONNECTIONS_MAX_IDLE_MS_CONFIG,
            Type::Long,
            9 * 60 * 1000_i64,
            Importance::Medium,
        )
        // `partitioner.class` is a class (CLAUDE.md §2): the Rust-only
        // `partitioner.type` names a built-in partitioner in its place.
        .define_with_validator(
            ProducerConfig::PARTITIONER_TYPE_CONFIG,
            Type::String,
            DefaultValue::Null,
            partitioner_type_validator(),
            Importance::Medium,
        )
        // `interceptor.classes` is a list of classes (CLAUDE.md §2): not defined.
        .define_with_validator(
            CommonClientConfigs::SECURITY_PROTOCOL_CONFIG,
            Type::String,
            CommonClientConfigs::DEFAULT_SECURITY_PROTOCOL,
            CaseInsensitiveValidString::r#in(&SecurityProtocol::names()),
            Importance::Medium,
        )
        // `security.providers` names JCA security providers, which have no
        // counterpart: not defined.
        .with_client_ssl_support()
        .with_client_sasl_support()
        .define(ProducerConfig::ENABLE_IDEMPOTENCE_CONFIG, Type::Boolean, true, Importance::Low)
        .define(ProducerConfig::TRANSACTION_TIMEOUT_CONFIG, Type::Int, 60000, Importance::Low)
        .define_with_validator(
            ProducerConfig::TRANSACTIONAL_ID_CONFIG,
            Type::String,
            DefaultValue::Null,
            NonEmptyString,
            Importance::Low,
        )
        .define(
            ProducerConfig::TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG,
            Type::Boolean,
            false,
            Importance::Low,
        )
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
        .define_with_validator(
            ProducerConfig::CONFIG_PROVIDERS_CONFIG,
            Type::List,
            Vec::<String>::new(),
            ValidList::any_non_duplicate_values(true, false),
            Importance::Low,
        )
});

/// Configuration for the Kafka Producer.
///
/// Documentation for the underlying keys can be found in the
/// [Kafka documentation](http://kafka.apache.org/documentation.html#producerconfigs).
///
/// As in Java, the configuration is built from the property map by
/// [`ProducerConfig::new`], which parses every key to its `ConfigDef` type,
/// applies its validator and runs Java's `postProcessParsedConfig`. Values are
/// read back with Java's `AbstractConfig` getters ([`Self::get_int`],
/// [`Self::get_string`], ...). Keys it does not define are accepted, as Java
/// accepts them; the producer reports them, with the defined keys it does not
/// read, in one line when it is built ([`Self::log_unused`]).
///
/// Equality and hashing are Java's `equals` / `hashCode`: two configurations
/// are equal when they were built from equal properties.
///
/// Corresponds to `org.apache.kafka.clients.producer.ProducerConfig`.
// `Debug` is derived: every secret the typed fields hold is a `Password`
// inside `sasl_config` / `ssl_config`, which renders as `[hidden]`, and
// `AbstractConfig`'s `Debug` renders the properties as their key set only: the
// map also holds keys this client does not define, and Java never prints a raw
// original value. Not `Clone`: the partitioner set with `set_partitioner` is
// owned, as the producer built from the configuration takes it.
#[derive(Debug)]
#[doc(alias = "org.apache.kafka.clients.producer.ProducerConfig")]
pub struct ProducerConfig {
    /// The parsed configuration, Java's `AbstractConfig` state.
    pub(crate) config: AbstractConfig,
    /// The keys the typed fields below were read from, marked used when the
    /// producer is built ([`Self::mark_read_keys_used`]).
    pub(crate) read_keys: Vec<&'static str>,

    // --- Connection ---
    /// `bootstrap.servers` - A list of host/port pairs to use for establishing the
    /// initial connection to the Kafka cluster.
    pub(crate) bootstrap_servers: Vec<String>,

    /// `client.dns.lookup` - Controls how the client uses DNS lookups.
    /// Default: [`ClientDnsLookup::UseAllDnsIps`].
    pub(crate) client_dns_lookup: ClientDnsLookup,

    /// `client.id` - An id string to pass to the server when making requests.
    pub(crate) client_id: String,

    /// `client.rack` - A rack identifier for this client. This can be any string
    /// value which indicates where this client is physically located. It
    /// corresponds with the broker config `broker.rack`. Default:
    /// [`ProducerConfig::DEFAULT_CLIENT_RACK`] (`""`). Used by the built-in
    /// partitioner when `partitioner.rack.aware` is enabled (KIP-1123).
    pub(crate) client_rack: String,

    // --- Security ---
    /// `security.protocol` - Protocol used to communicate with brokers.
    /// Default: `SecurityProtocol::Plaintext`.
    pub(crate) security_protocol: SecurityProtocol,

    /// SASL configuration (mechanism, JAAS config, credentials).
    pub(crate) sasl_config: SaslConfigs,

    /// SSL/TLS configuration.
    pub(crate) ssl_config: SslConfigs,

    // --- Batching ---
    /// `batch.size` - The producer will attempt to batch records together into fewer
    /// requests whenever multiple records are being sent to the same partition.
    /// Default: 16384 bytes.
    pub(crate) batch_size: i32,

    /// `linger.ms` - Upper bound on the delay for batching.
    /// Default: 5 ms (changed from 0 in Apache Kafka 4.0).
    pub(crate) linger_ms: i64,

    // --- Buffering ---
    /// `buffer.memory` - Total bytes of memory the producer can use to buffer records
    /// waiting to be sent. Default: 32 MiB.
    pub(crate) buffer_memory: i64,

    /// `buffer.memory.allocation.strategy` - Controls how the producer allocates memory from
    /// `buffer.memory` for record batches. The following values are supported:
    ///
    /// - `full`: reserves a full `batch.size` up front when a batch is created, regardless of how
    ///   much data it ends up holding. Pool memory therefore scales with the number of active
    ///   partitions.
    /// - `incremental`: allocates memory on demand as records are appended, growing a batch up to
    ///   `batch.size`. Pool memory therefore scales with the data actually buffered rather than
    ///   the number of active partitions, allowing larger `batch.size` values (e.g. for
    ///   high-latency clusters) without reserving `batch.size` for every active partition.
    ///
    /// Default: `full`. Internal (Java's `defineInternal`) until the incremental strategy is fully
    /// implemented. Validated case-insensitively and kept as given; `KafkaProducer` lower-cases it.
    pub(crate) buffer_memory_allocation_strategy: String,

    /// `max.block.ms` - Maximum time `send()` and `partitionsFor()` will block.
    /// Default: 60000 ms.
    pub(crate) max_block_ms: i64,

    // --- Reliability ---
    /// `acks` - Number of acknowledgments the producer requires the leader to have
    /// received. Allowed values: 0, 1, -1 (all). Default: -1 (all).
    pub(crate) acks: i16,

    /// `retries` - Number of times to retry a request that fails with a transient error.
    /// Default: `i32::MAX` (effectively infinite retries bounded by delivery timeout).
    pub(crate) retries: i32,

    /// `delivery.timeout.ms` - Upper bound on time to report success or failure after
    /// `send()` returns. Default: 120000 ms.
    pub(crate) delivery_timeout_ms: i32,

    /// `request.timeout.ms` - Maximum time the client will wait for a response.
    /// Default: 30000 ms.
    pub(crate) request_timeout_ms: i32,

    /// `enable.idempotence` - When true, exactly-once delivery semantics are enabled.
    /// Default: true.
    pub(crate) enable_idempotence: bool,

    // --- Sizing ---
    /// `max.request.size` - Maximum size of a request in bytes.
    /// Default: 1048576 (1 MiB).
    pub(crate) max_request_size: i32,

    /// `max.in.flight.requests.per.connection` - Maximum number of unacknowledged
    /// requests per connection. Default: 5.
    pub(crate) max_in_flight_requests_per_connection: i32,

    // --- Compression ---
    /// `compression.type` - Compression type for all data generated by the producer.
    /// Default: `CompressionType::None`.
    pub(crate) compression_type: CompressionType,

    /// `compression.gzip.level` - The compression level to use if
    /// `compression.type` is `gzip`.
    pub(crate) compression_gzip_level: i32,

    /// `compression.lz4.level` - The compression level to use if
    /// `compression.type` is `lz4`.
    pub(crate) compression_lz4_level: i32,

    /// `compression.zstd.level` - The compression level to use if
    /// `compression.type` is `zstd`.
    pub(crate) compression_zstd_level: i32,

    // --- Connections ---
    /// `connections.max.idle.ms` - Close idle connections after this duration.
    /// Default: 540000 ms (9 minutes).
    pub(crate) connections_max_idle_ms: i64,

    /// `reconnect.backoff.ms` - Base wait time before attempting to reconnect.
    /// Default: 50 ms.
    pub(crate) reconnect_backoff_ms: i64,

    /// `reconnect.backoff.max.ms` - Maximum wait time before reconnection attempts.
    /// Default: 1000 ms.
    pub(crate) reconnect_backoff_max_ms: i64,

    /// `retry.backoff.ms` - Wait time before retrying a failed request.
    /// Default: 100 ms.
    pub(crate) retry_backoff_ms: i64,

    /// `retry.backoff.max.ms` - Maximum wait time before retrying a failed request.
    /// Default: 1000 ms.
    pub(crate) retry_backoff_max_ms: i64,

    /// `bootstrap.resolve.timeout.ms` - Selects the bootstrap DNS resolution
    /// mode (KIP-909): `0` resolves `bootstrap.servers` synchronously at
    /// construction; a positive value resolves asynchronously for at most this
    /// long. Default: 0. See
    /// [`ProducerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG`].
    pub(crate) bootstrap_resolve_timeout_ms: i64,

    // --- Socket ---
    /// `send.buffer.bytes` - TCP send buffer size. Default: 131072 (128 KiB).
    pub(crate) send_buffer_bytes: i32,

    /// `receive.buffer.bytes` - TCP receive buffer size. Default: 32768 (32 KiB).
    pub(crate) receive_buffer_bytes: i32,

    /// `socket.connection.setup.timeout.ms` - Timeout for socket connection setup.
    /// Default: 10000 ms.
    pub(crate) socket_connection_setup_timeout_ms: i64,

    /// `socket.connection.setup.timeout.max.ms` - Maximum timeout for socket connection
    /// setup. Default: 30000 ms.
    pub(crate) socket_connection_setup_timeout_max_ms: i64,

    // --- Metadata ---
    /// `metadata.max.age.ms` - Period after which metadata is force-refreshed.
    /// Default: 300000 ms (5 minutes).
    pub(crate) metadata_max_age_ms: i64,

    /// `metadata.max.idle.ms` - Controls how long the producer will cache metadata for
    /// an idle topic. Default: 300000 ms (5 minutes).
    pub(crate) metadata_max_idle_ms: i64,

    /// `metadata.recovery.strategy` - How the client recovers when none of the
    /// brokers known to it is available. Default: `rebootstrap`
    /// (`ProducerConfig.java:595-601`).
    pub(crate) metadata_recovery_strategy: MetadataRecoveryStrategy,

    /// `metadata.recovery.rebootstrap.trigger.ms` - How long a client configured
    /// to rebootstrap waits without obtaining metadata before it rebootstraps.
    /// Default: 300000 ms (`ProducerConfig.java:602-607`).
    pub(crate) metadata_recovery_rebootstrap_trigger_ms: i64,

    /// `metadata.cluster.check.enable` (KIP-1242) - Whether the client sends the
    /// cluster id and node id it expects in ApiVersions so the broker can detect
    /// a misrouted connection; ignored when `metadata.recovery.strategy=none`.
    /// Default: true (`ProducerConfig.java:608-612`).
    pub(crate) metadata_cluster_check_enable: bool,

    // --- Partitioning ---
    /// `partitioner.adaptive.partitioning.enable` - Adapt to broker performance.
    /// Default: true.
    pub(crate) partitioner_adaptive_partitioning_enable: bool,

    /// `partitioner.availability.timeout.ms` - Timeout to mark a partition as
    /// unavailable. Default: 0 (disabled).
    pub(crate) partitioner_availability_timeout_ms: i64,

    /// `partitioner.ignore.keys` - Ignore record keys for partitioning.
    /// Default: false.
    pub(crate) partitioner_ignore_keys: bool,

    /// `partitioner.rack.aware` - Controls whether the default partitioner is
    /// rack-aware. This has no effect when a custom partitioner is used.
    /// Default: false.
    ///
    /// When `client.rack` is specified and `partitioner.rack.aware=true`, the
    /// sticky partition is chosen from partitions with the leader broker in the
    /// same rack, if at least one is available. If none are available, it falls
    /// back on selecting from all available partitions (KIP-1123). Enabling it
    /// with a blank `client.rack` makes the producer constructor fail with
    /// `"client.rack must be provided if partitioner.rack.aware is enabled"`.
    pub(crate) partitioner_rack_aware: bool,

    /// `partitioner.type` - Selects the partitioning strategy. Default: `None`
    /// (unset), which uses the built-in default partitioner keyed by IEEE CRC-32
    /// ([`KeyHasher::Crc32`], librdkafka `consistent_random` parity).
    ///
    /// Accepted values:
    /// - `ConsistentRandomPartitioner` — the default built-in partitioner with a
    ///   CRC-32 key hash; identical to the unset default.
    /// - `Murmur2RandomPartitioner` — the default built-in partitioner with a
    ///   murmur2 key hash, identical to the Java client's built-in partitioner
    ///   (exact Java parity).
    /// - `RoundRobinPartitioner` — the
    ///   [`RoundRobinPartitioner`](crate::producer::RoundRobinPartitioner), which
    ///   distributes writes evenly across a topic's available partitions and
    ///   ignores the record key. Selecting it **disables adaptive partitioning**,
    ///   exactly as Java does when a `partitioner.class` is configured.
    ///
    /// Any other value is rejected by [`new`](Self::new) as not supported. Java's
    /// `partitioner.class` names a class to load reflectively; Rust has no
    /// reflection, so this key names one of the built-in types above, and a
    /// user-written
    /// [`Partitioner`](crate::producer::Partitioner) is instead set with
    /// [`set_partitioner`](Self::set_partitioner). Unlike Java, this
    /// Rust client's default key hash is CRC-32, not murmur2 — see
    /// `design/current/partitioner.md`.
    pub(crate) partitioner_type: Option<String>,

    /// The [`Partitioner`] set with [`set_partitioner`](Self::set_partitioner),
    /// the value of Java's `partitioner.class`. Its record types are erased,
    /// because `ProducerConfig` is not generic over them;
    /// [`resolve_partitioner`](Self::resolve_partitioner) recovers them for the
    /// producer being built.
    pub(crate) partitioner: Option<ConfiguredPartitioner>,

    // --- Transactions ---
    /// `transactional.id` - TransactionalId for transactional delivery.
    /// Default: `None` (no transactions).
    pub(crate) transactional_id: Option<String>,

    /// `transaction.timeout.ms` - Maximum time a transaction will remain open.
    /// Default: 60000 ms.
    pub(crate) transaction_timeout_ms: i32,

    // --- Metrics ---
    /// `metrics.sample.window.ms` - The window of time a metrics sample is
    /// computed over. Default: 30000 ms.
    pub(crate) metrics_sample_window_ms: i64,

    /// `metrics.num.samples` - The number of samples maintained to compute
    /// metrics. Default: 2.
    pub(crate) metrics_num_samples: i32,

    /// `metrics.recording.level` - The highest recording level for metrics.
    /// One of `INFO`, `DEBUG`, `TRACE`. Default: `INFO`.
    pub(crate) metrics_recording_level: String,

    /// `transaction.two.phase.commit.enable` - Whether the client participates
    /// in two-phase commit (KIP-939), where an external coordinator decides when
    /// to finalize. Default: `false`.
    pub(crate) two_phase_commit_enable: bool,
}

/// The [`Partitioner`] set with [`ProducerConfig::set_partitioner`], with its
/// record types erased.
///
/// No Java counterpart: Java holds the `partitioner.class` value in the config
/// map as an `Object`. It is needed because [`ProducerConfig`] is not generic
/// over the record types the partitioner is written for.
#[derive(Debug)]
pub(crate) struct ConfiguredPartitioner {
    /// A `Box<dyn Partitioner<K, V>>`.
    partitioner: Box<dyn Any + Send + Sync>,
    /// `dyn Partitioner<K, V>`, named in the type mismatch error.
    type_name: &'static str,
}

impl PartialEq for ProducerConfig {
    /// Java's `AbstractConfig.equals`: the originals are equal.
    fn eq(&self, other: &Self) -> bool {
        self.config == other.config
    }
}

impl Eq for ProducerConfig {}

impl Hash for ProducerConfig {
    /// Java's `AbstractConfig.hashCode`, over the originals.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.config.hash(state);
    }
}

impl ProducerConfig {
    /// A configuration holding every key's `ConfigDef` default, for the
    /// crate's own tests, which then write the fields they need.
    ///
    /// It is parsed from `bootstrap.servers` alone, without
    /// `postProcessParsedConfig`, so `client.id` stays empty; `bootstrap_servers`
    /// is then emptied.
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
        let batch_size = values.int(Self::BATCH_SIZE_CONFIG)?;
        let linger_ms = values.long(Self::LINGER_MS_CONFIG)?;
        let buffer_memory = values.long(Self::BUFFER_MEMORY_CONFIG)?;
        let buffer_memory_allocation_strategy =
            values.string(Self::BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG)?.to_string();
        let max_block_ms = values.long(Self::MAX_BLOCK_MS_CONFIG)?;
        // `postProcessParsedConfig` replaced the value with `parseAcks`'s
        // number, which `KafkaProducer` reads with `Short.parseShort`; reading
        // it through `parse_acks` again, a no-op on that number, also covers
        // the test-only `defaults()`, which skips the post-processing.
        let acks_string = Self::parse_acks(values.string(Self::ACKS_CONFIG)?)?;
        let acks = acks_string
            .parse::<i16>()
            .map_err(|_| Error::config_message(format!("Invalid configuration value for 'acks': {acks_string}")))?;
        let retries = values.int(Self::RETRIES_CONFIG)?;
        let delivery_timeout_ms = values.int(Self::DELIVERY_TIMEOUT_MS_CONFIG)?;
        let request_timeout_ms = values.int(Self::REQUEST_TIMEOUT_MS_CONFIG)?;
        let enable_idempotence = values.boolean(Self::ENABLE_IDEMPOTENCE_CONFIG)?;
        let max_request_size = values.int(Self::MAX_REQUEST_SIZE_CONFIG)?;
        let max_in_flight_requests_per_connection = values.int(Self::MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)?;
        // The validator accepted only the compression type names.
        let compression_type = CompressionType::for_name(values.string(Self::COMPRESSION_TYPE_CONFIG)?)?;
        let compression_gzip_level = values.int(Self::COMPRESSION_GZIP_LEVEL_CONFIG)?;
        let compression_lz4_level = values.int(Self::COMPRESSION_LZ4_LEVEL_CONFIG)?;
        let compression_zstd_level = values.int(Self::COMPRESSION_ZSTD_LEVEL_CONFIG)?;
        let connections_max_idle_ms = values.long(Self::CONNECTIONS_MAX_IDLE_MS_CONFIG)?;
        let reconnect_backoff_ms = values.long(Self::RECONNECT_BACKOFF_MS_CONFIG)?;
        let reconnect_backoff_max_ms = values.long(Self::RECONNECT_BACKOFF_MAX_MS_CONFIG)?;
        let retry_backoff_ms = values.long(Self::RETRY_BACKOFF_MS_CONFIG)?;
        let retry_backoff_max_ms = values.long(Self::RETRY_BACKOFF_MAX_MS_CONFIG)?;
        let bootstrap_resolve_timeout_ms = values.long(Self::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG)?;
        let send_buffer_bytes = values.int(Self::SEND_BUFFER_CONFIG)?;
        let receive_buffer_bytes = values.int(Self::RECEIVE_BUFFER_CONFIG)?;
        let socket_connection_setup_timeout_ms = values.long(Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)?;
        let socket_connection_setup_timeout_max_ms =
            values.long(Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)?;
        let metadata_max_age_ms = values.long(Self::METADATA_MAX_AGE_CONFIG)?;
        let metadata_max_idle_ms = values.long(Self::METADATA_MAX_IDLE_CONFIG)?;
        // The validator accepted the strategy case-insensitively, as `forName`
        // reads it.
        let strategy = values.string(CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG)?;
        let metadata_recovery_strategy =
            MetadataRecoveryStrategy::for_name(strategy).map_err(Error::local_illegal_argument)?;
        let metadata_recovery_rebootstrap_trigger_ms =
            values.long(CommonClientConfigs::METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG)?;
        let metadata_cluster_check_enable =
            values.boolean(CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG)?;
        let partitioner_adaptive_partitioning_enable =
            values.boolean(Self::PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG)?;
        let partitioner_availability_timeout_ms = values.long(Self::PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG)?;
        let partitioner_ignore_keys = values.boolean(Self::PARTITIONER_IGNORE_KEYS_CONFIG)?;
        let partitioner_rack_aware = values.boolean(Self::PARTITIONER_RACK_AWARE_CONFIG)?;
        let partitioner_type = values.string_or_null(Self::PARTITIONER_TYPE_CONFIG)?.map(str::to_string);
        let transactional_id = values.string_or_null(Self::TRANSACTIONAL_ID_CONFIG)?.map(str::to_string);
        let transaction_timeout_ms = values.int(Self::TRANSACTION_TIMEOUT_CONFIG)?;
        let metrics_sample_window_ms = values.long(Self::METRICS_SAMPLE_WINDOW_MS_CONFIG)?;
        let metrics_num_samples = values.int(Self::METRICS_NUM_SAMPLES_CONFIG)?;
        let metrics_recording_level = values.string(Self::METRICS_RECORDING_LEVEL_CONFIG)?.to_string();
        let two_phase_commit_enable = values.boolean(Self::TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG)?;
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
            bootstrap_servers,
            client_dns_lookup,
            client_id,
            client_rack,
            security_protocol,
            sasl_config,
            ssl_config,
            batch_size,
            linger_ms,
            buffer_memory,
            buffer_memory_allocation_strategy,
            max_block_ms,
            acks,
            retries,
            delivery_timeout_ms,
            request_timeout_ms,
            enable_idempotence,
            max_request_size,
            max_in_flight_requests_per_connection,
            compression_type,
            compression_gzip_level,
            compression_lz4_level,
            compression_zstd_level,
            connections_max_idle_ms,
            reconnect_backoff_ms,
            reconnect_backoff_max_ms,
            retry_backoff_ms,
            retry_backoff_max_ms,
            bootstrap_resolve_timeout_ms,
            send_buffer_bytes,
            receive_buffer_bytes,
            socket_connection_setup_timeout_ms,
            socket_connection_setup_timeout_max_ms,
            metadata_max_age_ms,
            metadata_max_idle_ms,
            metadata_recovery_strategy,
            metadata_recovery_rebootstrap_trigger_ms,
            metadata_cluster_check_enable,
            partitioner_adaptive_partitioning_enable,
            partitioner_availability_timeout_ms,
            partitioner_ignore_keys,
            partitioner_rack_aware,
            partitioner_type,
            partitioner: None,
            transactional_id,
            transaction_timeout_ms,
            metrics_sample_window_ms,
            metrics_num_samples,
            metrics_recording_level,
            two_phase_commit_enable,
        })
    }
}

impl ProducerConfig {
    // --- Config key constants (matching Java string keys) ---

    /// Config key: `bootstrap.servers`
    pub const BOOTSTRAP_SERVERS_CONFIG: &'static str = "bootstrap.servers";
    /// Config key: `client.dns.lookup`. Java's `ProducerConfig.java` declares it as its
    /// own public alias of `CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG`.
    pub const CLIENT_DNS_LOOKUP_CONFIG: &'static str = CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG;
    /// Config key: `client.id`
    pub const CLIENT_ID_CONFIG: &'static str = "client.id";
    /// Config key: `client.rack`
    pub const CLIENT_RACK_CONFIG: &'static str = CommonClientConfigs::CLIENT_RACK_CONFIG;
    /// Default value of `client.rack`: no rack.
    pub const DEFAULT_CLIENT_RACK: &'static str = CommonClientConfigs::DEFAULT_CLIENT_RACK;
    /// Config key: `batch.size`
    pub const BATCH_SIZE_CONFIG: &'static str = "batch.size";
    /// Config key: `linger.ms`
    pub const LINGER_MS_CONFIG: &'static str = "linger.ms";
    /// Config key: `buffer.memory`
    pub const BUFFER_MEMORY_CONFIG: &'static str = "buffer.memory";
    /// Config key: `buffer.memory.allocation.strategy` (KIP-1332). Internal until the incremental
    /// strategy is fully implemented.
    pub const BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG: &'static str = "buffer.memory.allocation.strategy";
    /// `buffer.memory.allocation.strategy` value: reserve a full `batch.size` per batch up front
    /// (the default).
    pub const BUFFER_MEMORY_ALLOCATION_STRATEGY_FULL: &'static str = "full";
    /// `buffer.memory.allocation.strategy` value: allocate memory on demand as records are
    /// appended.
    pub const BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL: &'static str = "incremental";
    /// Config key: `max.block.ms`
    pub const MAX_BLOCK_MS_CONFIG: &'static str = "max.block.ms";
    /// Config key: `acks`
    pub const ACKS_CONFIG: &'static str = "acks";
    /// Config key: `retries`
    pub const RETRIES_CONFIG: &'static str = "retries";
    /// Config key: `delivery.timeout.ms`
    pub const DELIVERY_TIMEOUT_MS_CONFIG: &'static str = "delivery.timeout.ms";
    /// Config key: `request.timeout.ms`
    pub const REQUEST_TIMEOUT_MS_CONFIG: &'static str = "request.timeout.ms";
    /// Config key: `enable.idempotence`
    pub const ENABLE_IDEMPOTENCE_CONFIG: &'static str = "enable.idempotence";
    /// Config key: `max.request.size`
    pub const MAX_REQUEST_SIZE_CONFIG: &'static str = "max.request.size";
    /// Config key: `max.in.flight.requests.per.connection`
    pub const MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: &'static str = "max.in.flight.requests.per.connection";
    /// Config key: `compression.type`
    pub const COMPRESSION_TYPE_CONFIG: &'static str = "compression.type";
    /// Config key: `connections.max.idle.ms`
    pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &'static str = "connections.max.idle.ms";
    /// Config key: `reconnect.backoff.ms`
    pub const RECONNECT_BACKOFF_MS_CONFIG: &'static str = "reconnect.backoff.ms";
    /// Config key: `reconnect.backoff.max.ms`
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &'static str = "reconnect.backoff.max.ms";
    /// Config key: `retry.backoff.ms`
    pub const RETRY_BACKOFF_MS_CONFIG: &'static str = "retry.backoff.ms";
    /// Config key: `retry.backoff.max.ms`
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &'static str = "retry.backoff.max.ms";
    /// Config key: `bootstrap.resolve.timeout.ms` (KIP-909)
    ///
    /// Selects the client's bootstrap DNS resolution mode. When set to `0` (the
    /// default), DNS is resolved synchronously during client construction; any
    /// failure surfaces as a config error and no client instance is created.
    /// When set to a positive value, DNS is resolved asynchronously and this is
    /// the maximum amount of time the client will spend retrying resolution
    /// before failing with an unrecoverable
    /// [`BootstrapResolutionError`](crate::common::errors::BootstrapResolutionError)
    /// from subsequent API calls (the client must then be closed and re-created
    /// after fixing the underlying DNS or `bootstrap.servers` configuration
    /// issue). Setting this config to a positive value enables an evolving
    /// feature whose compatibility may be broken in a minor release.
    pub const BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: &'static str =
        CommonClientConfigs::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG;
    /// Config key: `send.buffer.bytes`
    pub const SEND_BUFFER_CONFIG: &'static str = "send.buffer.bytes";
    /// Config key: `receive.buffer.bytes`
    pub const RECEIVE_BUFFER_CONFIG: &'static str = "receive.buffer.bytes";
    /// Config key: `metadata.max.age.ms`
    pub const METADATA_MAX_AGE_CONFIG: &'static str = "metadata.max.age.ms";
    /// Config key: `metadata.max.idle.ms`
    pub const METADATA_MAX_IDLE_CONFIG: &'static str = "metadata.max.idle.ms";
    /// Config key: `partitioner.adaptive.partitioning.enable`
    pub const PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG: &'static str =
        "partitioner.adaptive.partitioning.enable";
    /// Config key: `partitioner.availability.timeout.ms`
    pub const PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG: &'static str = "partitioner.availability.timeout.ms";
    /// Config key: `partitioner.ignore.keys`
    pub const PARTITIONER_IGNORE_KEYS_CONFIG: &'static str = "partitioner.ignore.keys";
    /// Config key: `partitioner.rack.aware`
    pub const PARTITIONER_RACK_AWARE_CONFIG: &'static str = "partitioner.rack.aware";
    /// Config key: `partitioner.type`
    // selects a built-in partitioner in place of Java's class-valued `partitioner.class` (CLAUDE.md §2)
    #[doc(alias = "rust-only")]
    pub const PARTITIONER_TYPE_CONFIG: &'static str = "partitioner.type";
    /// Accepted `partitioner.type` value selecting the CRC-32 key hash
    /// (the crate-internal `KeyHasher::Crc32`) — the default, librdkafka `consistent_random` parity.
    // selects a built-in partitioner in place of Java's class-valued `partitioner.class` (CLAUDE.md §2)
    #[doc(alias = "rust-only")]
    pub const CONSISTENT_RANDOM_PARTITIONER: &'static str = "ConsistentRandomPartitioner";
    /// Accepted `partitioner.type` value selecting the murmur2 key hash
    /// (the crate-internal `KeyHasher::Murmur2`) — exact Java-client parity.
    // selects a built-in partitioner in place of Java's class-valued `partitioner.class` (CLAUDE.md §2)
    #[doc(alias = "rust-only")]
    pub const MURMUR2_RANDOM_PARTITIONER: &'static str = "Murmur2RandomPartitioner";
    /// Accepted `partitioner.type` value selecting the
    /// [`RoundRobinPartitioner`](crate::producer::RoundRobinPartitioner).
    // selects a built-in partitioner in place of Java's class-valued `partitioner.class` (CLAUDE.md §2)
    #[doc(alias = "rust-only")]
    pub const ROUND_ROBIN_PARTITIONER: &'static str = "RoundRobinPartitioner";
    /// Config key: `transactional.id`
    pub const TRANSACTIONAL_ID_CONFIG: &'static str = "transactional.id";
    /// Config key: `transaction.timeout.ms`
    pub const TRANSACTION_TIMEOUT_CONFIG: &'static str = "transaction.timeout.ms";
    /// Config key: `metrics.sample.window.ms`
    pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &'static str = "metrics.sample.window.ms";
    /// Config key: `metrics.num.samples`
    pub const METRICS_NUM_SAMPLES_CONFIG: &'static str = "metrics.num.samples";
    /// Config key: `metrics.recording.level`
    pub const METRICS_RECORDING_LEVEL_CONFIG: &'static str = "metrics.recording.level";
    /// Config key: `transaction.two.phase.commit.enable`
    pub const TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG: &'static str = "transaction.two.phase.commit.enable";
    /// Config key: `compression.gzip.level`
    pub const COMPRESSION_GZIP_LEVEL_CONFIG: &'static str = "compression.gzip.level";
    /// Config key: `compression.lz4.level`
    pub const COMPRESSION_LZ4_LEVEL_CONFIG: &'static str = "compression.lz4.level";
    /// Config key: `compression.zstd.level`
    pub const COMPRESSION_ZSTD_LEVEL_CONFIG: &'static str = "compression.zstd.level";
    /// Config key: `enable.metrics.push`
    pub const ENABLE_METRICS_PUSH_CONFIG: &'static str = "enable.metrics.push";
    /// Config key: `socket.connection.setup.timeout.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &'static str = "socket.connection.setup.timeout.ms";
    /// Config key: `socket.connection.setup.timeout.max.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &'static str = "socket.connection.setup.timeout.max.ms";
    /// Config key: `config.providers`
    pub const CONFIG_PROVIDERS_CONFIG: &'static str = "config.providers";

    /// Creates a config from a property map: Java's `ProducerConfig(Map)` and
    /// `ProducerConfig(Properties)`.
    ///
    /// Every value is parsed to its `ConfigDef` type and checked by its
    /// validator, then Java's `postProcessParsedConfig`
    /// (`ProducerConfig.java:620-628`) runs: `postValidateSaslMechanismConfig`,
    /// `warnDisablingExponentialBackoff`, `postProcessReconnectBackoffConfigs`,
    /// `postProcessAndValidateIdempotenceConfigs` (an incompatible `retries` or
    /// `acks` disables idempotence unless it was set explicitly) and
    /// `maybeOverrideClientId` (an unset `client.id` becomes
    /// `producer-<transactional.id or a process-wide sequence>`).
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when a value fails to parse or validate, with
    /// `ConfigDef`'s message; when `bootstrap.servers` is missing; when a SASL
    /// `security.protocol` has an empty `sasl.mechanism`; when the idempotence
    /// settings conflict (`enable.idempotence=true` with `retries=0`, with
    /// `acks` other than `all`, or `max.in.flight.requests.per.connection`
    /// above 5 with idempotence enabled); when `transactional.id` is set
    /// without idempotence; and when `transaction.timeout.ms` is set with
    /// `transaction.two.phase.commit.enable=true`.
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfig#ProducerConfig(Map)")]
    pub fn new(props: &HashMap<String, String>) -> Result<Self, Error> {
        let config = Self::build(string_originals(props), None)?;
        config
            .client_dns_lookup
            .warn_if_tls_hostname_verification_affected(config.security_protocol, &config.ssl_config);
        Ok(config)
    }

    /// The configuration of one more client given this one: the same
    /// properties and this configuration's resolved `client.id`, with its
    /// own record of the keys used and no partitioner, as [`Self::new`]
    /// leaves it.
    ///
    /// Java hands each client the `ProducerConfig` it is given, which took
    /// its one number from the `client.id` sequence when it was built
    /// (`KafkaProducer.java:366` reads it), so clients given one
    /// configuration share its id. Building the configuration again would
    /// take a second number; this copy does not. It is not `Clone`
    /// (CLAUDE.md §3), and it does not repeat `new`'s warnings.
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

    /// Java's `ProducerConfig(Map)` without its logging: parses `originals`
    /// and post-processes them, `resolved_client_id` standing in for the
    /// sequence number `client.id` would take.
    fn build(originals: ConfigMap, resolved_client_id: Option<&str>) -> Result<Self, Error> {
        let config = AbstractConfig::with_hooks(
            &CONFIG,
            originals,
            false,
            "ProducerConfig",
            |originals| originals,
            |config| Self::post_process_parsed_config(config, resolved_client_id),
        )?;
        Self::from_abstract(config)
    }

    /// Java's `ProducerConfig.postProcessParsedConfig`: the values that
    /// replace the parsed ones. A `resolved_client_id` is the id a derived
    /// `client.id` takes, in place of a new one.
    fn post_process_parsed_config(
        config: &AbstractConfig,
        resolved_client_id: Option<&str>,
    ) -> Result<ConfigMap, Error> {
        CommonClientConfigs::post_validate_sasl_mechanism_config(config)?;
        CommonClientConfigs::warn_disabling_exponential_backoff(config)?;
        let mut refined_configs =
            CommonClientConfigs::post_process_reconnect_backoff_configs(config, config.parsed_values());
        Self::post_process_and_validate_idempotence_configs(config, &mut refined_configs)?;
        Self::maybe_override_client_id(config, &mut refined_configs, resolved_client_id)?;
        Ok(refined_configs)
    }

    /// Java's private `maybeOverrideClientId`: a `client.id` the user did not
    /// set becomes `producer-<transactional.id>` when a transactional id is
    /// set, otherwise `producer-<n>` from the process-wide sequence.
    ///
    /// A `resolved_client_id` (the id of the configuration this one copies)
    /// is put in `configs` instead of a derived one, after the same reads.
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfig#maybeOverrideClientId")]
    fn maybe_override_client_id(
        config: &AbstractConfig,
        configs: &mut ConfigMap,
        resolved_client_id: Option<&str>,
    ) -> Result<(), Error> {
        let user_configured_client_id = config.originals_map().contains_key(Self::CLIENT_ID_CONFIG);
        let refined_client_id = if user_configured_client_id {
            config.get_string(Self::CLIENT_ID_CONFIG)?.unwrap_or_default().to_string()
        } else {
            let transactional_id = config.get_string(Self::TRANSACTIONAL_ID_CONFIG)?;
            match (resolved_client_id, transactional_id) {
                (Some(resolved_client_id), _) => resolved_client_id.to_string(),
                (None, Some(transactional_id)) => format!("producer-{transactional_id}"),
                (None, None) => format!(
                    "producer-{}",
                    PRODUCER_CLIENT_ID_SEQUENCE.fetch_add(1, atomic::Ordering::Relaxed)
                ),
            }
        };
        configs.insert(Self::CLIENT_ID_CONFIG.to_string(), Some(ParsedValue::String(refined_client_id)));
        Ok(())
    }

    /// Validates and post-processes the idempotence-dependent configs: Java's
    /// private `postProcessAndValidateIdempotenceConfigs`.
    ///
    /// Java's asymmetry is deliberate and preserved: for `retries` and `acks`,
    /// an incompatible value **silently disables** idempotence unless the user
    /// asked for idempotence explicitly, in which case it is an error. For
    /// `max.in.flight.requests.per.connection` it is **always** an error. The
    /// silent-disable path is what keeps existing non-idempotent configurations
    /// working, so removing it would be a breaking change.
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfig#postProcessAndValidateIdempotenceConfigs")]
    fn post_process_and_validate_idempotence_configs(
        config: &AbstractConfig,
        configs: &mut ConfigMap,
    ) -> Result<(), Error> {
        let original_configs = config.originals_map();
        let acks_str = Self::parse_acks(config.get_string(Self::ACKS_CONFIG)?.unwrap_or_default())?;
        configs.insert(Self::ACKS_CONFIG.to_string(), Some(ParsedValue::String(acks_str.clone())));
        let user_configured_idempotence = original_configs.contains_key(Self::ENABLE_IDEMPOTENCE_CONFIG);
        let mut idempotence_enabled = config.get_boolean(Self::ENABLE_IDEMPOTENCE_CONFIG)?.unwrap_or_default();
        let mut should_disable_idempotence = false;

        // For idempotence producers, values for `retries` and `acks` and `max.in.flight.requests.per.connection` need validation
        if idempotence_enabled {
            let retries = config.get_int(Self::RETRIES_CONFIG)?.unwrap_or_default();
            if retries == 0 {
                if user_configured_idempotence {
                    return Err(Error::config_message(format!(
                        "Must set {} to non-zero when using the idempotent producer.",
                        Self::RETRIES_CONFIG
                    )));
                }
                info!("Idempotence will be disabled because {} is set to 0.", Self::RETRIES_CONFIG);
                should_disable_idempotence = true;
            }

            // `parseAcks` produced a number: "-1", "0" or "1" after the
            // validator.
            let acks = acks_str
                .parse::<i16>()
                .map_err(|_| Error::config_message(format!("Invalid configuration value for 'acks': {acks_str}")))?;
            if acks != -1 {
                if user_configured_idempotence {
                    return Err(Error::config_message(format!(
                        "Must set {} to all in order to use the idempotent producer. Otherwise we cannot guarantee \
                         idempotence.",
                        Self::ACKS_CONFIG
                    )));
                }
                info!(
                    "Idempotence will be disabled because {} is set to {}, not set to 'all'.",
                    Self::ACKS_CONFIG,
                    acks
                );
                should_disable_idempotence = true;
            }

            let in_flight_connection = config.get_int(Self::MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)?.unwrap_or_default();
            if MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE < in_flight_connection {
                return Err(Error::config_message(format!(
                    "To use the idempotent producer, {} must be set to at most 5. Current value is {}.",
                    Self::MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                    in_flight_connection
                )));
            }
        }

        if should_disable_idempotence {
            configs.insert(Self::ENABLE_IDEMPOTENCE_CONFIG.to_string(), Some(ParsedValue::Boolean(false)));
            idempotence_enabled = false;
        }

        // validate `transaction.id` after validating idempotence dependant configs because `enable.idempotence` config might be overridden
        let user_configured_transactions = original_configs.contains_key(Self::TRANSACTIONAL_ID_CONFIG);
        if !idempotence_enabled && user_configured_transactions {
            return Err(Error::config_message(format!(
                "Cannot set a {} without also enabling idempotence.",
                Self::TRANSACTIONAL_ID_CONFIG
            )));
        }

        // Validate that transaction.timeout.ms is not set when transaction.two.phase.commit.enable is true
        // In standard Kafka transactions, the broker enforces transaction.timeout.ms and aborts any
        // transaction that isn't completed in time. With two-phase commit (2PC), an external coordinator
        // decides when to finalize, so broker-side timeouts don't apply. Disallow using both.
        let enable_2pc = config
            .get_boolean(Self::TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG)?
            .unwrap_or_default();
        let user_configured_transaction_timeout = original_configs.contains_key(Self::TRANSACTION_TIMEOUT_CONFIG);
        if enable_2pc && user_configured_transaction_timeout {
            return Err(Error::config_message(format!(
                "Cannot set {} when {} is set to true. Transactions will not expire with two-phase commit enabled.",
                Self::TRANSACTION_TIMEOUT_CONFIG,
                Self::TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG
            )));
        }
        Ok(())
    }

    /// Java's private `parseAcks`: `all` becomes `-1`, any other value its
    /// `short` value, as a string.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] with `Invalid configuration value for 'acks': <value>`
    /// when the value is not `all` and not a `short`. Java's `parseAcks` turns
    /// the `NumberFormatException` into that `ConfigException`
    /// (`ProducerConfig.java:705-711`). The `in("all", "-1", "0", "1")`
    /// validator runs first, so the error is reachable only when this is
    /// called directly, as in Java.
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfig#parseAcks")]
    pub(crate) fn parse_acks(acks_string: &str) -> Result<String, Error> {
        let trimmed = acks_string.trim();
        if trimmed.eq_ignore_ascii_case("all") {
            Ok("-1".to_string())
        } else {
            trimmed
                .parse::<i16>()
                .map(|acks| acks.to_string())
                .map_err(|_| Error::config_message(format!("Invalid configuration value for 'acks': {acks_string}")))
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
    /// `enable.idempotence` answers the value `postProcessParsedConfig`
    /// settled on: `false` when an incompatible `retries` or `acks` disabled
    /// it.
    ///
    /// # Errors
    ///
    /// As [`Self::get_short`].
    #[doc(alias = "org.apache.kafka.common.config.AbstractConfig#getBoolean")]
    pub fn get_boolean(&self, key: &str) -> Result<Option<bool>, Error> {
        self.config.get_boolean(key)
    }

    /// The `STRING` configuration `key`, trimmed, or `None` when it has no
    /// value (Java's `null`, e.g. an unset `transactional.id`). Reading a key
    /// marks it used ([`Self::unused`]).
    ///
    /// `client.id` answers the resolved id: when the property is unset, `new`
    /// derives it the way Java's `maybeOverrideClientId` does, so this is the
    /// id the producer reports to the brokers. `acks` answers the number
    /// Java's `parseAcks` settled on (`"-1"` for `all`). `security.protocol`
    /// answers the value as written, trimmed (`"sasl_ssl"`), as Java's does:
    /// its validator accepts it case-insensitively without changing it. A
    /// `PASSWORD` configuration that is not set answers `None`, as Java's
    /// `null` default casts to `String` (`SslConfigs.java:135-142`,
    /// `SaslConfigs.java:398`).
    ///
    /// # Errors
    ///
    /// - [`Error::LocalIllegalArgument`] when `key` is a configuration of
    ///   another type (`batch.size`, a set `ssl.truststore.password`). Java's
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
    /// never read: through a getter, or by the producer built from it.
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
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfig#configNames")]
    pub fn config_names() -> HashSet<String> {
        CONFIG.names()
    }

    /// Marks used the keys the typed settings were read from, as the producer
    /// does where Java's reads them through the getters.
    pub(crate) fn mark_read_keys_used(&self) {
        for key in &self.read_keys {
            self.config.ignore(key);
        }
    }

    /// Java's `logAll()`, which Java runs when the producer builds the
    /// configuration with `doLog = true`: every parsed value at INFO.
    pub(crate) fn log_values(&self) {
        self.config.log_all("ProducerConfig");
    }

    /// The properties the configuration was built from, Java's `originals()`,
    /// as strings: the map a configured partitioner's `configure` receives.
    pub(crate) fn originals(&self) -> HashMap<String, String> {
        self.config
            .originals_map()
            .iter()
            .filter_map(|(key, value)| value.as_ref().map(|value| (key.clone(), value.to_string())))
            .collect()
    }
    /// Resolves `partitioner.type` to the [`KeyHasher`] used on the built-in
    /// default partitioner's keyed partition path.
    ///
    /// This helper has no direct Java counterpart (Java resolves a `Partitioner`
    /// instance instead); it exists only to map the validated
    /// [`partitioner_type`](Self::partitioner_type) string to the internal
    /// [`KeyHasher`] enum. Only `Murmur2RandomPartitioner` maps to murmur2;
    /// every other accepted value — unset (`None`), `ConsistentRandomPartitioner`,
    /// and `RoundRobinPartitioner` — maps to the CRC-32
    /// default.
    ///
    /// For a `RoundRobinPartitioner` config the hash is moot: the producer uses
    /// the [`RoundRobinPartitioner`](crate::producer::RoundRobinPartitioner)
    /// instance (which ignores the key) rather than the built-in default
    /// partitioner, so this value is never consulted. It is defined here only to
    /// keep the accessor total.
    pub(crate) fn key_hasher(&self) -> KeyHasher {
        match self.partitioner_type.as_deref() {
            Some(Self::MURMUR2_RANDOM_PARTITIONER) => KeyHasher::Murmur2,
            // Unset (`None`), `ConsistentRandomPartitioner`, and
            // `RoundRobinPartitioner` all use the CRC-32 default. `new` rejects
            // every other value, so no other string can reach here.
            _ => KeyHasher::Crc32,
        }
    }

    /// Resolves `partitioner.type` to a built-in [`Partitioner`] instance, if
    /// the configured value names one that has a dedicated partitioner type.
    ///
    /// This is the Rust stand-in for Java's
    /// `config.getConfiguredInstance(PARTITIONER_CLASS_CONFIG, Partitioner.class,
    /// ...)` (`KafkaProducer.java:389-392`), which reflectively loads and
    /// instantiates the named class. Rust has no reflection, so only the
    /// built-in names resolve here:
    ///
    /// - [`ROUND_ROBIN_PARTITIONER`](Self::ROUND_ROBIN_PARTITIONER) →
    ///   `Some(Box::new(RoundRobinPartitioner::new()))`;
    /// - every other accepted value (unset, `ConsistentRandomPartitioner`,
    ///   `Murmur2RandomPartitioner`) → `None`, meaning the built-in default
    ///   partitioner's keyed path is used (see [`key_hasher`](Self::key_hasher)).
    ///
    /// The partitioner set with [`set_partitioner`](Self::set_partitioner)
    /// takes precedence over a `partitioner.type` name.
    ///
    /// `new` has already rejected any value that is not a built-in type, so no
    /// unknown string reaches here.
    ///
    /// # Errors
    ///
    /// Returns [`Error::KafkaError`] when the partitioner set with
    /// `set_partitioner` partitions records of other types than the producer's
    /// `K` / `V`: Java's `getConfiguredInstance` throws
    /// `KafkaException(c.getName() + " is not an instance of " + t.getName())`
    /// when the configured value does not implement the requested type.
    ///
    /// Takes `&mut self` because the partitioner set with `set_partitioner` is
    /// moved out: the producer being built owns it, as a Java producer owns
    /// the instance it creates from `partitioner.class`.
    pub(crate) fn resolve_partitioner<K: 'static, V: 'static>(
        &mut self,
    ) -> Result<Option<Box<dyn Partitioner<K, V>>>, Error> {
        if let Some(configured) = self.partitioner.take() {
            let Ok(partitioner) = configured.partitioner.downcast::<Box<dyn Partitioner<K, V>>>() else {
                return Err(Error::kafka_message(format!(
                    "{} is not an instance of {}",
                    configured.type_name,
                    type_name::<dyn Partitioner<K, V>>()
                )));
            };
            return Ok(Some(*partitioner));
        }
        Ok(match self.partitioner_type.as_deref() {
            Some(Self::ROUND_ROBIN_PARTITIONER) => Some(Box::new(RoundRobinPartitioner::new())),
            _ => None,
        })
    }

    /// Sets the [`Partitioner`] that determines which partition each record
    /// goes to: the value of Java's `partitioner.class`.
    ///
    /// Java names a class, which the producer instantiates by reflection; here
    /// the partitioner itself is the value. [`KafkaProducer::new`](crate::producer::KafkaProducer::new)
    /// `configure`s it with the user configs plus the resolved `client.id`, as
    /// Java does (`KafkaProducer.java:388-395`), and closes it on `close`. As
    /// with any configured partitioner, adaptive partitioning is then disabled.
    /// The producer built from this config takes ownership of the partitioner.
    ///
    /// This replaces a `partitioner.type` given in the properties passed to
    /// [`new`](Self::new), as a later value for the same key would.
    ///
    /// The partitioner's `K` / `V` must be the record types of the producer built
    /// from this config; otherwise `KafkaProducer::new` fails with
    /// "`<partitioner>` is not an instance of `<expected>`".
    // the partitioner set through a setter, not a class config (CLAUDE.md §2)
    #[doc(alias = "rust-only")]
    pub fn set_partitioner<K: 'static, V: 'static>(mut self, partitioner: Box<dyn Partitioner<K, V>>) -> Self {
        self.partitioner_type = None;
        self.partitioner = Some(ConfiguredPartitioner {
            partitioner: Box::new(partitioner),
            type_name: type_name::<dyn Partitioner<K, V>>(),
        });
        self
    }

    /// Whether the user set `key` explicitly.
    ///
    /// Java's `config.originals().containsKey(key)`: a key is "user
    /// configured" iff it appears in the properties the configuration was
    /// built from, not merely because it has a (possibly derived) value.
    pub(crate) fn user_configured(&self, key: &str) -> bool {
        self.config.originals_map().contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::types::Password;

    /// Props holding only `bootstrap.servers`, which the `ConfigDef` defines
    /// with `NO_DEFAULT_VALUE` and so every valid config must carry.
    fn base_props() -> HashMap<String, String> {
        HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())])
    }

    #[test]
    fn test_default_values() {
        let config = ProducerConfig::defaults();
        assert_eq!(config.batch_size, 16384);
        assert_eq!(config.linger_ms, 5);
        assert_eq!(config.buffer_memory, 32 * 1024 * 1024);
        assert_eq!(config.max_block_ms, 60_000);
        assert_eq!(config.acks, -1);
        assert_eq!(config.retries, i32::MAX);
        assert_eq!(config.delivery_timeout_ms, 120_000);
        assert_eq!(config.request_timeout_ms, 30_000);
        assert!(config.enable_idempotence);
        assert_eq!(config.max_request_size, 1_048_576);
        assert_eq!(config.max_in_flight_requests_per_connection, 5);
        assert_eq!(config.compression_type, CompressionType::None);
        assert_eq!(config.connections_max_idle_ms, 540_000);
        assert_eq!(config.reconnect_backoff_ms, 50);
        assert_eq!(config.reconnect_backoff_max_ms, 1000);
        assert_eq!(config.retry_backoff_ms, 100);
        assert_eq!(config.retry_backoff_max_ms, 1000);
        assert_eq!(config.send_buffer_bytes, 131_072);
        assert_eq!(config.receive_buffer_bytes, 32_768);
        assert_eq!(config.metadata_max_age_ms, 300_000);
        assert_eq!(config.metadata_max_idle_ms, 300_000);
        assert_eq!(config.metadata_recovery_strategy, MetadataRecoveryStrategy::Rebootstrap);
        assert_eq!(config.metadata_recovery_rebootstrap_trigger_ms, 300_000);
        assert!(config.partitioner_adaptive_partitioning_enable);
        assert_eq!(config.partitioner_availability_timeout_ms, 0);
        assert!(!config.partitioner_ignore_keys);
        assert!(!config.partitioner_rack_aware);
        assert_eq!(config.client_rack, "");
        assert!(config.partitioner_type.is_none());
        // The default (unset) key hash is CRC-32 (librdkafka parity), NOT the
        // Java-client murmur2 default.
        assert_eq!(config.key_hasher(), KeyHasher::Crc32);
        assert!(config.transactional_id.is_none());
        assert_eq!(config.transaction_timeout_ms, 60_000);
        assert_eq!(config.metrics_sample_window_ms, 30_000);
        assert_eq!(config.metrics_num_samples, 2);
        assert_eq!(config.metrics_recording_level, "INFO");
        // The test-only defaults are parsed from `bootstrap.servers` alone.
        assert_eq!(config.originals(), base_props());
    }

    /// `metadata.recovery.strategy` is a case-insensitive `none` / `rebootstrap`
    /// string (`ProducerConfig.java:595-601`) and
    /// `metadata.recovery.rebootstrap.trigger.ms` a long (`:602-607`). Before
    /// these keys were parsed the producer silently ignored them and never
    /// rebootstrapped (`ClientRebootstrapTest.testProducerRebootstrap`).
    #[test]
    fn test_metadata_recovery_configs() {
        let mut props = base_props();
        props.insert("metadata.recovery.strategy".to_string(), "NONE".to_string());
        props.insert("metadata.recovery.rebootstrap.trigger.ms".to_string(), "1234".to_string());
        let c = ProducerConfig::new(&props).unwrap();
        assert_eq!(c.metadata_recovery_strategy, MetadataRecoveryStrategy::None);
        assert_eq!(c.metadata_recovery_rebootstrap_trigger_ms, 1234);

        let mut props = base_props();
        props.insert("metadata.recovery.strategy".to_string(), "rebootstrap".to_string());
        let c = ProducerConfig::new(&props).unwrap();
        assert_eq!(c.metadata_recovery_strategy, MetadataRecoveryStrategy::Rebootstrap);

        let mut props = base_props();
        props.insert("metadata.recovery.strategy".to_string(), "bogus".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        // `CaseInsensitiveValidString.ensureValid`'s message, which names the
        // valid strings (`ConfigDef.java`).
        assert_eq!(
            err.message(),
            "Invalid value bogus for configuration metadata.recovery.strategy: \
             String must be one of (case insensitive): REBOOTSTRAP, NONE"
        );
    }

    /// `metrics.num.samples` is `atLeast(1)` (Java ProducerConfig /
    /// CommonClientConfigs). A value below 1 is rejected asserting the bound.
    #[test]
    fn test_metrics_num_samples_validator() {
        let mut props = base_props();
        props.insert("metrics.num.samples".to_string(), "3".to_string());
        let c = ProducerConfig::new(&props).unwrap();
        assert_eq!(c.metrics_num_samples, 3);

        let mut props = base_props();
        props.insert("metrics.num.samples".to_string(), "0".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("metrics.num.samples") && msg.contains("at least 1"),
            "unexpected message: {msg}"
        );

        let mut props = base_props();
        props.insert("metrics.num.samples".to_string(), "-1".to_string());
        assert!(ProducerConfig::new(&props).is_err());
    }

    /// `metadata.recovery.rebootstrap.trigger.ms` is `atLeast(0)` (Java
    /// `ProducerConfig.java:602-605`): 0 is accepted, -1 is rejected with
    /// Java's `ConfigDef.Range.atLeast` message.
    #[test]
    fn test_metadata_recovery_rebootstrap_trigger_ms_validator() {
        let mut props = base_props();
        props.insert("metadata.recovery.rebootstrap.trigger.ms".to_string(), "0".to_string());
        let c = ProducerConfig::new(&props).unwrap();
        assert_eq!(c.metadata_recovery_rebootstrap_trigger_ms, 0);

        let mut props = base_props();
        props.insert("metadata.recovery.rebootstrap.trigger.ms".to_string(), "-1".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        let Error::Config(config_error) = err else {
            panic!("expected a config error, got {err:?}");
        };
        assert_eq!(
            config_error.message(),
            "Invalid value -1 for configuration metadata.recovery.rebootstrap.trigger.ms: \
             Value must be at least 0"
        );
    }

    /// `metadata.cluster.check.enable` (KIP-1242): `Type.BOOLEAN`, default `true`
    /// (`ProducerConfig.java:608-612`).
    #[test]
    fn test_metadata_cluster_check_enable() {
        let c = ProducerConfig::new(&base_props()).unwrap();
        assert!(c.metadata_cluster_check_enable);

        let mut props = base_props();
        props.insert(
            CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG.to_string(),
            "false".to_string(),
        );
        assert!(!ProducerConfig::new(&props).unwrap().metadata_cluster_check_enable);

        props.insert(
            CommonClientConfigs::METADATA_CLUSTER_CHECK_ENABLE_CONFIG.to_string(),
            "maybe".to_string(),
        );
        assert_eq!(
            ProducerConfig::new(&props).unwrap_err().message(),
            "Invalid value maybe for configuration metadata.cluster.check.enable: Expected value to be either true or false"
        );
    }

    /// `bootstrap.resolve.timeout.ms` (KIP-909): `Type.LONG`, default `0`
    /// (synchronous resolution), `atLeast(0L)` since KAFKA-20939.
    #[test]
    fn test_bootstrap_resolve_timeout_ms() {
        assert_eq!(
            ProducerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG,
            "bootstrap.resolve.timeout.ms"
        );
        let c = ProducerConfig::new(&base_props()).unwrap();
        assert_eq!(c.bootstrap_resolve_timeout_ms, 0);

        let mut props = base_props();
        props.insert("bootstrap.resolve.timeout.ms".to_string(), "3000".to_string());
        assert_eq!(ProducerConfig::new(&props).unwrap().bootstrap_resolve_timeout_ms, 3000);

        props.insert("bootstrap.resolve.timeout.ms".to_string(), "-1".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        let Error::Config(config_error) = err else {
            panic!("expected a config error, got {err:?}");
        };
        assert_eq!(
            config_error.message(),
            "Invalid value -1 for configuration bootstrap.resolve.timeout.ms: Value must be at least 0"
        );
    }

    /// `metrics.sample.window.ms` is `atLeast(0)` (Java ProducerConfig /
    /// CommonClientConfigs). A negative value is rejected asserting the bound.
    #[test]
    fn test_metrics_sample_window_ms_validator() {
        let mut props = base_props();
        props.insert("metrics.sample.window.ms".to_string(), "0".to_string());
        let c = ProducerConfig::new(&props).unwrap();
        assert_eq!(c.metrics_sample_window_ms, 0);

        let mut props = base_props();
        props.insert("metrics.sample.window.ms".to_string(), "60000".to_string());
        let c = ProducerConfig::new(&props).unwrap();
        assert_eq!(c.metrics_sample_window_ms, 60_000);

        let mut props = base_props();
        props.insert("metrics.sample.window.ms".to_string(), "-1".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("metrics.sample.window.ms") && msg.contains("at least 0"),
            "unexpected message: {msg}"
        );
    }

    /// `metrics.recording.level` accepts exactly `INFO`/`DEBUG`/`TRACE`
    /// (case-sensitive) and rejects anything else. Java uses
    /// `ConfigDef.ValidString.in("INFO", "DEBUG", "TRACE")`, an exact
    /// case-sensitive membership check, so lowercase `debug` is rejected
    /// with a `ConfigException` while `DEBUG` is accepted.
    #[test]
    fn test_metrics_recording_level_validator() {
        // Uppercase enum values are accepted.
        for level in ["INFO", "DEBUG", "TRACE"] {
            let mut props = base_props();
            props.insert("metrics.recording.level".to_string(), level.to_string());
            let c = ProducerConfig::new(&props).unwrap();
            assert_eq!(c.metrics_recording_level, level);
        }

        // Lowercase is rejected (Java is case-sensitive here) with the exact
        // `ConfigException` wording.
        let mut props = base_props();
        props.insert("metrics.recording.level".to_string(), "debug".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        assert!(
            err.to_string().ends_with(
                "Invalid value debug for configuration metrics.recording.level: \
                 String must be one of: INFO, DEBUG, TRACE"
            ),
            "unexpected message: {err}"
        );

        // A wholly unknown value is likewise rejected with the same wording.
        let mut props = base_props();
        props.insert("metrics.recording.level".to_string(), "bogus".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        assert!(
            err.to_string().ends_with(
                "Invalid value bogus for configuration metrics.recording.level: \
                 String must be one of: INFO, DEBUG, TRACE"
            ),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn test_new_basic() {
        let mut props = base_props();
        props.insert("bootstrap.servers".to_string(), "host1:9092,host2:9093".to_string());
        props.insert("client.id".to_string(), "my-producer".to_string());
        props.insert("batch.size".to_string(), "32768".to_string());
        props.insert("linger.ms".to_string(), "10".to_string());
        props.insert("acks".to_string(), "all".to_string());
        props.insert("compression.type".to_string(), "gzip".to_string());
        props.insert("enable.idempotence".to_string(), "false".to_string());

        let config = ProducerConfig::new(&props).unwrap();
        assert_eq!(config.bootstrap_servers, vec!["host1:9092", "host2:9093"]);
        assert_eq!(config.client_id, "my-producer");
        assert_eq!(config.batch_size, 32768);
        assert_eq!(config.linger_ms, 10);
        assert_eq!(config.acks, -1);
        assert_eq!(config.compression_type, CompressionType::Gzip);
        assert!(!config.enable_idempotence);
    }

    /// `bootstrap.servers` has `NO_DEFAULT_VALUE` (`ProducerConfig.java:407-409`), so a config
    /// without it fails at parse time with `ConfigDef.parseValue`'s message.
    #[test]
    fn test_missing_bootstrap_servers_rejected_with_exact_message() {
        let err = ProducerConfig::new(&HashMap::new()).unwrap_err();
        let Error::Config(config_err) = &err else {
            panic!("expected a config error, got: {err:?}");
        };
        assert_eq!(
            config_err.message(),
            "Missing required configuration \"bootstrap.servers\" which has no default value."
        );
    }

    #[test]
    fn test_new_defaults_for_missing() {
        let props = base_props();
        let config = ProducerConfig::new(&props).unwrap();
        // All fields should have default values
        assert_eq!(config.batch_size, 16384);
        assert_eq!(config.linger_ms, 5);
        assert_eq!(config.acks, -1);
    }

    #[test]
    fn test_new_invalid_value() {
        let mut props = base_props();
        props.insert("batch.size".to_string(), "not-a-number".to_string());
        let result = ProducerConfig::new(&props);
        assert!(result.is_err());
    }

    #[test]
    fn test_new_unknown_key_ignored() {
        let mut props = base_props();
        props.insert("unknown.key".to_string(), "value".to_string());
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        let config = ProducerConfig::new(&props).unwrap();
        assert_eq!(config.bootstrap_servers, vec!["localhost:9092"]);
    }

    /// `partitioner.type=ConsistentRandomPartitioner` selects the CRC-32 hash
    /// (identical to the unset default, librdkafka `consistent_random` parity).
    #[test]
    fn test_partitioner_type_consistent_random() {
        let mut props = base_props();
        props.insert("partitioner.type".to_string(), "ConsistentRandomPartitioner".to_string());
        let config = ProducerConfig::new(&props).unwrap();
        assert_eq!(config.partitioner_type.as_deref(), Some("ConsistentRandomPartitioner"));
        assert_eq!(config.key_hasher(), KeyHasher::Crc32);
    }

    /// `partitioner.type=Murmur2RandomPartitioner` selects the murmur2 hash
    /// (exact Java-client parity).
    #[test]
    fn test_partitioner_type_murmur2_random() {
        let mut props = base_props();
        props.insert("partitioner.type".to_string(), "Murmur2RandomPartitioner".to_string());
        let config = ProducerConfig::new(&props).unwrap();
        assert_eq!(config.partitioner_type.as_deref(), Some("Murmur2RandomPartitioner"));
        assert_eq!(config.key_hasher(), KeyHasher::Murmur2);
    }

    /// An unrecognised `partitioner.type` is rejected with the exact message
    /// naming the supported types (DoD §3: error messages are the contract).
    /// A Java fully-qualified class name is not a type, so it is rejected too.
    #[test]
    fn test_partitioner_type_unknown_rejected_with_exact_message() {
        let mut props = base_props();
        props.insert("partitioner.type".to_string(), "com.example.MyPartitioner".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        // `err.to_string()` prepends the variant tag, so assert on the exact
        // inner text with `ends_with`, matching
        // `test_metrics_recording_level_validator` above.
        assert!(
            err.to_string().ends_with(
                "Invalid value com.example.MyPartitioner for configuration partitioner.type: \
                 Partitioner type com.example.MyPartitioner is not supported; the supported types are \
                 ConsistentRandomPartitioner, Murmur2RandomPartitioner and RoundRobinPartitioner."
            ),
            "unexpected message: {err}"
        );

        let mut props = base_props();
        props.insert(
            "partitioner.type".to_string(),
            "org.apache.kafka.clients.producer.RoundRobinPartitioner".to_string(),
        );
        let err = ProducerConfig::new(&props).unwrap_err();
        assert!(
            err.to_string().ends_with(
                "Invalid value org.apache.kafka.clients.producer.RoundRobinPartitioner for configuration \
                 partitioner.type: Partitioner type org.apache.kafka.clients.producer.RoundRobinPartitioner \
                 is not supported; the supported types are ConsistentRandomPartitioner, \
                 Murmur2RandomPartitioner and RoundRobinPartitioner."
            ),
            "unexpected message: {err}"
        );
    }

    /// `partitioner.type=RoundRobinPartitioner` (the simple name) is accepted
    /// and stored verbatim. The key hash is the CRC-32 default (moot: a
    /// RoundRobin producer uses the partitioner instance, not the key hash).
    #[test]
    fn test_partitioner_type_round_robin_simple_name() {
        let mut props = base_props();
        props.insert("partitioner.type".to_string(), "RoundRobinPartitioner".to_string());
        let config = ProducerConfig::new(&props).unwrap();
        assert_eq!(config.partitioner_type.as_deref(), Some("RoundRobinPartitioner"));
        assert_eq!(
            config.partitioner_type.as_deref(),
            Some(ProducerConfig::ROUND_ROBIN_PARTITIONER)
        );
        assert_eq!(config.key_hasher(), KeyHasher::Crc32);
    }

    /// [`resolve_partitioner`](ProducerConfig::resolve_partitioner) returns a
    /// built-in [`RoundRobinPartitioner`] instance for the simple name — the
    /// Rust stand-in for Java's reflective `getConfiguredInstance` of
    /// `partitioner.class` (`KafkaProducer.java:389-392`).
    #[test]
    fn test_resolve_partitioner_round_robin_simple_name() {
        let mut props = base_props();
        props.insert("partitioner.type".to_string(), "RoundRobinPartitioner".to_string());
        let mut config = ProducerConfig::new(&props).unwrap();
        assert!(config.resolve_partitioner::<String, String>().expect("resolves").is_some());
    }

    /// With no `partitioner.type`, `resolve_partitioner` returns `None`: the
    /// built-in default partitioner's keyed path is used, not a partitioner
    /// instance.
    #[test]
    fn test_resolve_partitioner_default_none() {
        let mut config = ProducerConfig::new(&base_props()).unwrap();
        assert!(config.partitioner_type.is_none());
        assert!(config.resolve_partitioner::<String, String>().expect("resolves").is_none());
    }

    /// The other accepted `partitioner.type` values name Java's built-in
    /// random/sticky partitioners, which in this client are handled by the
    /// key-hash path (see [`key_hasher`](ProducerConfig::key_hasher)), not by a
    /// [`Partitioner`] instance. So `resolve_partitioner` returns `None` for
    /// both — only `RoundRobinPartitioner` maps to a dedicated partitioner type.
    #[test]
    fn test_resolve_partitioner_random_names_none() {
        for name in ["ConsistentRandomPartitioner", "Murmur2RandomPartitioner"] {
            let mut props = base_props();
            props.insert("partitioner.type".to_string(), name.to_string());
            let mut config = ProducerConfig::new(&props).unwrap();
            assert!(
                config.resolve_partitioner::<String, String>().expect("resolves").is_none(),
                "{name} must not resolve to a Partitioner instance"
            );
        }
    }

    /// Java's `originals()` holds the user-supplied map verbatim and never
    /// contains a *derived* default. Here `client.id` is not user-set, so the
    /// generated `producer-<n>` id lands on `client_id` but NOT in `originals`.
    #[test]
    fn test_originals_kept_verbatim_without_generated_client_id() {
        let props = props_with(&[("partitioner.type", "RoundRobinPartitioner"), ("acks", "all")]);
        let config = ProducerConfig::new(&props).expect("valid");

        // Verbatim: originals equals the input map exactly (keys and values).
        assert_eq!(config.originals(), props);
        assert_eq!(
            config.originals().get("partitioner.type").map(String::as_str),
            Some("RoundRobinPartitioner")
        );
        assert_eq!(config.originals().get("acks").map(String::as_str), Some("all"));

        // The generated client.id is present on the struct but absent from
        // originals — mirroring Java, where a derived default is not an original.
        assert!(!config.originals().contains_key("client.id"));
        assert!(
            config.client_id.starts_with("producer-"),
            "expected a derived client id, got {}",
            config.client_id
        );
    }

    /// The other direction of the `originals` contract: a *user-set* `client.id`
    /// IS an original (and is preserved as the client id).
    #[test]
    fn test_originals_includes_user_set_client_id() {
        let props = props_with(&[("client.id", "my-client")]);
        let config = ProducerConfig::new(&props).expect("valid");

        assert_eq!(config.originals().get("client.id").map(String::as_str), Some("my-client"));
        assert_eq!(config.client_id, "my-client");
    }

    #[test]
    fn test_new_transactional_id() {
        let config = ProducerConfig::new(&props_with(&[("transactional.id", "my-txn")])).unwrap();
        assert_eq!(config.transactional_id, Some("my-txn".to_string()));

        // Java trims before validating (`ConfigDef.parseType(STRING)`), so a padded
        // id is stored trimmed.
        let config = ProducerConfig::new(&props_with(&[("transactional.id", " my-txn ")])).unwrap();
        assert_eq!(config.transactional_id, Some("my-txn".to_string()));

        // Java's `ConfigDef.NonEmptyString` validator (`ProducerConfig.java:584-587`
        // → `ConfigDef.java:1225-1233`) rejects an empty string; a whitespace-only
        // id trims to empty and is rejected the same way. The error prints the
        // trimmed (empty) value, so "Invalid value " is followed by two spaces.
        for raw in ["", "   "] {
            let error = ProducerConfig::new(&props_with(&[("transactional.id", raw)]))
                .expect_err("an empty transactional.id must be rejected");
            assert_eq!(
                error.message(),
                "Invalid value  for configuration transactional.id: String must be non-empty",
                "for transactional.id={raw:?}"
            );
            assert!(
                matches!(error, Error::Config(_)),
                "for transactional.id={raw:?}: expected Error::Config, got {error:?}"
            );
            assert!(
                error.is_kafka_error(),
                "for transactional.id={raw:?}: a config error is a Kafka error"
            );
        }

        // Only an absent key means "no transactional id" (Java's `null` default).
        assert_eq!(ProducerConfig::new(&base_props()).unwrap().transactional_id, None);
    }

    /// Java's `ProducerConfig` `ConfigDef` validators (`ProducerConfig.java:416-577`):
    /// each bound rejects the value just below it with `ConfigDef.Range.atLeast`'s
    /// message and accepts the bound itself.
    #[test]
    fn test_config_def_range_validators() {
        let cases: [(&str, i64); 20] = [
            ("batch.size", 0),
            ("linger.ms", 0),
            ("buffer.memory", 0),
            ("max.block.ms", 0),
            ("retries", 0),
            ("delivery.timeout.ms", 0),
            ("request.timeout.ms", 0),
            ("max.request.size", 0),
            ("max.in.flight.requests.per.connection", 1),
            ("reconnect.backoff.ms", 0),
            ("reconnect.backoff.max.ms", 0),
            ("retry.backoff.ms", 0),
            ("retry.backoff.max.ms", 0),
            ("bootstrap.resolve.timeout.ms", 0),
            ("send.buffer.bytes", -1),
            ("receive.buffer.bytes", -1),
            ("metadata.max.age.ms", 0),
            ("metadata.max.idle.ms", 5000),
            ("partitioner.availability.timeout.ms", 0),
            ("metrics.sample.window.ms", 0),
        ];
        for (key, min) in cases {
            let below = (min - 1).to_string();
            let error = ProducerConfig::new(&props_with(&[(key, &below)]))
                .expect_err(&format!("{key}={below} must be rejected"));
            assert!(
                matches!(error, Error::Config(_)),
                "{key}: expected Error::Config, got {error:?}"
            );
            assert_eq!(
                error.message(),
                format!("Invalid value {below} for configuration {key}: Value must be at least {min}"),
            );
            let at = min.to_string();
            ProducerConfig::new(&props_with(&[(key, &at)]))
                .unwrap_or_else(|e| panic!("{key}={at} must be accepted: {e}"));
        }
    }

    /// `acks` accepts exactly `all`, `-1`, `0`, `1` (after trimming) and rejects
    /// anything else, mirroring Java's `ConfigDef.ValidString.in("all", "-1",
    /// "0", "1")` (`ProducerConfig.java:430-435`, `ConfigDef.java:1116`), which
    /// runs before `parseAcks`. The error names the trimmed value because the
    /// validator runs on the parsed (trimmed) value; the check is exact-match, so
    /// `ALL` and `2` are rejected while a padded ` all ` is accepted.
    #[test]
    fn test_acks_valid_strings() {
        for (acks, expected) in [("all", -1), (" all ", -1), ("-1", -1), ("0", 0), ("1", 1)] {
            let props = props_with(&[("acks", acks), ("enable.idempotence", "false")]);
            assert_eq!(ProducerConfig::new(&props).unwrap().acks, expected, "acks={acks:?}");
        }
        // Rejected: wrong case, out-of-range numbers and non-numbers. Each is a
        // `ConfigException` (a Kafka error) naming the value.
        for acks in ["ALL", "2", "-2", "100", "invalid"] {
            let error = ProducerConfig::new(&props_with(&[("acks", acks)])).expect_err("rejected");
            assert!(matches!(error, Error::Config(_)), "expected Error::Config, got {error:?}");
            assert_eq!(
                error.message(),
                format!("Invalid value {acks} for configuration acks: String must be one of: all, -1, 0, 1"),
            );
            assert!(error.is_kafka_error(), "for acks={acks}: a config error is a Kafka error");
        }
    }

    #[test]
    fn test_parse_acks() {
        assert_eq!(ProducerConfig::parse_acks("all").unwrap(), "-1");
        assert_eq!(ProducerConfig::parse_acks("ALL").unwrap(), "-1");
        assert_eq!(ProducerConfig::parse_acks("-1").unwrap(), "-1");
        assert_eq!(ProducerConfig::parse_acks("0").unwrap(), "0");
        assert_eq!(ProducerConfig::parse_acks("1").unwrap(), "1");

        // Java's `parseAcks` catches `NumberFormatException` and throws
        // `ConfigException` (`ProducerConfig.java:705-711`), so the class matters
        // as much as the message: `ConfigException extends KafkaException`, and a
        // caller validating a config map with `is_kafka_error()` must see it.
        let error = ProducerConfig::parse_acks("invalid").expect_err("a non-numeric acks is a config error");
        assert!(matches!(error, Error::Config(_)), "expected Error::Config, got {error:?}");
        assert_eq!(error.message(), "Invalid configuration value for 'acks': invalid");
        assert!(error.is_kafka_error());
    }

    /// Boolean config values are parsed case-insensitively after trimming, and an
    /// unparseable value is rejected with Java's message and the original,
    /// untrimmed value. Mirrors `ConfigDef.parseType(BOOLEAN)`
    /// (`ConfigDef.java:703-737`): `equalsIgnoreCase("true")` / `("false")`, else
    /// `ConfigException(name, value, "Expected value to be either true or false")`.
    #[test]
    fn test_parse_bool_matches_java_boolean() {
        // Accepted regardless of case, and after trimming surrounding whitespace.
        for raw in ["true", "True", "TRUE"] {
            let config = ProducerConfig::new(&props_with(&[("enable.idempotence", raw)]))
                .unwrap_or_else(|e| panic!("enable.idempotence={raw} should be valid: {e:?}"));
            assert!(config.enable_idempotence, "for enable.idempotence={raw}");
        }
        for raw in ["false", "False", " false "] {
            let config = ProducerConfig::new(&props_with(&[("enable.idempotence", raw)]))
                .unwrap_or_else(|e| panic!("enable.idempotence={raw} should be valid: {e:?}"));
            assert!(!config.enable_idempotence, "for enable.idempotence={raw}");
        }

        // Rejected: a non-boolean string. Java prints the original (untrimmed) value.
        let error = ProducerConfig::new(&props_with(&[("enable.idempotence", "yes")]))
            .expect_err("a non-boolean enable.idempotence must be rejected");
        assert_eq!(
            error.message(),
            "Invalid value yes for configuration enable.idempotence: Expected value to be either true or false",
        );
        assert!(matches!(error, Error::Config(_)), "expected Error::Config, got {error:?}");
        assert!(error.is_kafka_error());
    }

    /// Every `ConfigException` this config raises must answer `true` to
    /// `is_kafka_error()`, matching `ConfigException extends KafkaException`.
    ///
    /// The six sites are the `acks` `ConfigDef.ValidString` validator
    /// (`ProducerConfig.java:430-435` → `ConfigDef.java:1116`, which runs before
    /// Java's `parseAcks` at `:709`) and the five throws in
    /// `postProcessAndValidateIdempotenceConfigs` (`:655`, `:664`, `:673`, `:687`,
    /// `:697`). They all used to be `IllegalArgumentException`, which sits
    /// *beside* `KafkaException` rather than below it, so `is_kafka_error()`
    /// answered `false` and a bad `acks` slipped past a caller that a bad
    /// `linger.ms` did not.
    #[test]
    fn config_errors_are_inside_the_kafka_error_hierarchy() {
        let cases: [(&[(&str, &str)], &str); 6] = [
            (
                &[("acks", "not-a-number")],
                "Invalid value not-a-number for configuration acks: String must be one of: all, -1, 0, 1",
            ),
            (
                &[("enable.idempotence", "true"), ("retries", "0")],
                "Must set retries to non-zero when using the idempotent producer.",
            ),
            (
                &[("enable.idempotence", "true"), ("acks", "1")],
                "Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.",
            ),
            (
                &[
                    ("enable.idempotence", "true"),
                    ("max.in.flight.requests.per.connection", "6"),
                ],
                "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. \
                 Current value is 6.",
            ),
            (
                &[("enable.idempotence", "false"), ("transactional.id", "txn-id")],
                "Cannot set a transactional.id without also enabling idempotence.",
            ),
            (
                &[
                    ("transactional.id", "txn-id"),
                    ("transaction.two.phase.commit.enable", "true"),
                    ("transaction.timeout.ms", "1000"),
                ],
                "Cannot set transaction.timeout.ms when transaction.two.phase.commit.enable is set to true. \
                 Transactions will not expire with two-phase commit enabled.",
            ),
        ];

        for (props, expected_message) in cases {
            let error = ProducerConfig::new(&props_with(props)).expect_err("expected a config error");
            assert_eq!(error.message(), expected_message, "for {props:?}");
            assert!(
                matches!(error, Error::Config(_)),
                "for {props:?}: expected Error::Config, got {error:?}"
            );
            assert!(error.is_kafka_error(), "for {props:?}: a config error is a Kafka error");
        }
    }

    /// `client.dns.lookup` defaults to `use_all_dns_ips` and parses into the
    /// typed [`ClientDnsLookup`], as `ProducerConfig`'s `ConfigDef` defines it.
    #[test]
    fn test_client_dns_lookup() {
        assert_eq!(ProducerConfig::CLIENT_DNS_LOOKUP_CONFIG, "client.dns.lookup");
        let mut props = base_props();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        assert_eq!(
            ProducerConfig::new(&props).unwrap().client_dns_lookup,
            ClientDnsLookup::UseAllDnsIps
        );

        props.insert(
            CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG.to_string(),
            "resolve_canonical_bootstrap_servers_only".to_string(),
        );
        assert_eq!(
            ProducerConfig::new(&props).unwrap().client_dns_lookup,
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly
        );

        props.insert(CommonClientConfigs::CLIENT_DNS_LOOKUP_CONFIG.to_string(), "default".to_string());
        assert_eq!(
            ProducerConfig::new(&props).unwrap_err().message(),
            "Invalid value default for configuration client.dns.lookup: String must be one of: \
             use_all_dns_ips, resolve_canonical_bootstrap_servers_only"
        );
    }

    /// Translated from `ProducerConfigTest.testInvalidSecurityProtocol`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfigTest#testInvalidSecurityProtocol")]
    fn test_invalid_security_protocol() {
        let mut props = base_props();
        props.insert("security.protocol".to_string(), "abc".to_string());
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("security.protocol"),
            "Error message should contain config key, got: {}",
            msg
        );
    }

    /// Translated from `ProducerConfigTest.testDefaultBufferMemoryAllocationStrategy`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfigTest#testDefaultBufferMemoryAllocationStrategy")]
    fn test_default_buffer_memory_allocation_strategy() {
        let producer_config = ProducerConfig::new(&base_props()).unwrap();
        assert_eq!(
            ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_FULL,
            producer_config.buffer_memory_allocation_strategy
        );
    }

    /// Translated from `ProducerConfigTest.testValidBufferMemoryAllocationStrategy`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfigTest#testValidBufferMemoryAllocationStrategy")]
    fn test_valid_buffer_memory_allocation_strategy() {
        let mut props = base_props();
        props.insert(
            ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG.to_string(),
            ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL.to_string(),
        );
        let producer_config = ProducerConfig::new(&props).unwrap();
        assert_eq!(
            ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL,
            producer_config.buffer_memory_allocation_strategy
        );

        // Rust-only: `CaseInsensitiveValidString` accepts any case, and the value is kept as given.
        props.insert(
            ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG.to_string(),
            "INCREMENTAL".to_string(),
        );
        assert_eq!(
            "INCREMENTAL",
            ProducerConfig::new(&props).unwrap().buffer_memory_allocation_strategy
        );
    }

    /// Translated from `ProducerConfigTest.testInvalidBufferMemoryAllocationStrategy`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfigTest#testInvalidBufferMemoryAllocationStrategy")]
    fn test_invalid_buffer_memory_allocation_strategy() {
        let mut props = base_props();
        props.insert(
            ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG.to_string(),
            "abc".to_string(),
        );
        let err = ProducerConfig::new(&props).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "expected Error::Config, got {err:?}");
        assert!(err.message().contains(ProducerConfig::BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG));
        // Rust-only: the whole `CaseInsensitiveValidString` message.
        assert_eq!(
            err.message(),
            "Invalid value abc for configuration buffer.memory.allocation.strategy: \
             String must be one of (case insensitive): FULL, INCREMENTAL"
        );
    }

    /// An unrecognised `compression.type` is a `ConfigException`, not the
    /// `IllegalArgumentException` `CompressionType.forName` would raise: Java
    /// validates the key with `in(Utils.enumOptions(CompressionType.class))`
    /// (`ProducerConfig.java:436`), so `ConfigDef.ValidString.ensureValid`
    /// (`ConfigDef.java:1116`) rejects the value before the enum lookup runs.
    #[test]
    fn test_invalid_compression_type_is_a_config_error() {
        let mut props = base_props();
        props.insert("compression.type".to_string(), "gzipp".to_string());
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        let err = ProducerConfig::new(&props).expect_err("gzipp is not a compression type");
        assert_eq!(
            err.message(),
            "Invalid value gzipp for configuration compression.type: \
             String must be one of: none, gzip, snappy, lz4, zstd"
        );
        assert!(matches!(err, Error::Config(_)), "expected Error::Config, got {err:?}");
        // `ConfigException extends KafkaException` but is not an `ApiException`.
        assert!(err.is_kafka_error(), "a ConfigException is a KafkaException");
        assert!(!err.is_api_error(), "a ConfigException is not an ApiException");
    }

    /// Every valid `compression.type` name still parses.
    #[test]
    fn test_valid_compression_types_parse() {
        for name in CompressionType::names() {
            let mut props = base_props();
            props.insert("compression.type".to_string(), name.to_string());
            props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
            let config = ProducerConfig::new(&props)
                .unwrap_or_else(|e| panic!("compression.type={name} should be valid: {e:?}"));
            assert_eq!(config.compression_type.name(), name);
        }
    }

    /// Translated from `ProducerConfigTest.testCaseInsensitiveSecurityProtocol`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfigTest#testCaseInsensitiveSecurityProtocol")]
    fn test_case_insensitive_security_protocol() {
        let mut props = base_props();
        props.insert("security.protocol".to_string(), "sasl_ssl".to_string());
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        let config = ProducerConfig::new(&props).unwrap();
        assert_eq!(config.security_protocol, SecurityProtocol::SaslSsl);
    }

    #[test]
    fn test_sasl_config_from_properties() {
        let mut props = base_props();
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"secret\";"
                .to_string(),
        );
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        let config = ProducerConfig::new(&props).unwrap();
        assert_eq!(config.sasl_config.mechanism, "PLAIN");
        assert_eq!(config.sasl_config.resolve_username(), Some("alice"));
        assert_eq!(config.sasl_config.resolve_password(), Some("secret"));
    }

    #[test]
    fn test_ssl_config_from_properties() {
        let mut props = base_props();
        props.insert("ssl.truststore.location".to_string(), "/path/to/truststore.pem".to_string());
        props.insert("ssl.keystore.location".to_string(), "/path/to/keystore.pem".to_string());
        props.insert("ssl.endpoint.identification.algorithm".to_string(), String::new());
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        let config = ProducerConfig::new(&props).unwrap();
        assert_eq!(
            config.ssl_config.truststore_location.as_deref(),
            Some("/path/to/truststore.pem")
        );
        assert_eq!(config.ssl_config.keystore_location.as_deref(), Some("/path/to/keystore.pem"));
        assert_eq!(config.ssl_config.endpoint_identification_algorithm, "");
    }

    // -- Idempotence config validation --------------------------------------
    //
    // Translated from `ProducerConfigTest` and the four pure-config tests in
    // `KafkaProducerTest`. The latter construct a `ProducerConfig` from
    // `Properties` and assert on values without creating a producer or touching
    // the network, so they belong here rather than in a producer test.

    /// Java's `KafkaProducerTest.baseProperties()`.
    fn base_properties() -> HashMap<String, String> {
        HashMap::from([("bootstrap.servers".to_string(), "localhost:9999".to_string())])
    }

    /// KIP-1123 (KAFKA-19193): `partitioner.rack.aware` is a `Type.BOOLEAN` and the
    /// producer's `client.rack` a `Type.STRING` (trimmed by `ConfigDef.parseType`),
    /// defined with the keys and defaults Java's `ProducerConfig` gives them.
    #[test]
    fn test_partitioner_rack_aware_and_client_rack() {
        assert_eq!(ProducerConfig::PARTITIONER_RACK_AWARE_CONFIG, "partitioner.rack.aware");
        assert_eq!(ProducerConfig::CLIENT_RACK_CONFIG, "client.rack");
        assert_eq!(ProducerConfig::CLIENT_RACK_CONFIG, CommonClientConfigs::CLIENT_RACK_CONFIG);
        assert_eq!(ProducerConfig::DEFAULT_CLIENT_RACK, "");

        let mut props = base_props();
        props.insert("partitioner.rack.aware".to_string(), "true".to_string());
        props.insert("client.rack".to_string(), " rack0 ".to_string());
        let config = ProducerConfig::new(&props).expect("valid config");
        assert!(config.partitioner_rack_aware);
        assert_eq!(config.client_rack, "rack0");

        let mut props = base_props();
        props.insert("partitioner.rack.aware".to_string(), "yes".to_string());
        let err = ProducerConfig::new(&props).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "got {err:?}");
        assert_eq!(
            err.message(),
            "Invalid value yes for configuration partitioner.rack.aware: Expected value to be either true or false"
        );
    }

    fn props_with(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut props = base_properties();
        for (key, value) in extra {
            props.insert((*key).to_string(), (*value).to_string());
        }
        props
    }

    /// Translated from `KafkaProducerTest.testOverwriteAcksAndRetriesForIdempotentProducers`.
    #[test]
    fn test_overwrite_acks_and_retries_for_idempotent_producers() {
        let props = props_with(&[("transactional.id", "transactionalId")]);
        let config = ProducerConfig::new(&props).expect("config should be valid");

        assert!(config.enable_idempotence);
        assert_eq!(config.acks, -1);
        assert_eq!(config.retries, i32::MAX);
        // The derived client id form Java asserts on.
        assert_eq!(config.client_id, "producer-transactionalId");
    }

    /// Translated from `KafkaProducerTest.testAcksAndIdempotenceForIdempotentProducers`.
    #[test]
    fn test_acks_and_idempotence_for_idempotent_producers() {
        // Valid: acks=0 with idempotence explicitly off.
        let config =
            ProducerConfig::new(&props_with(&[("acks", "0"), ("enable.idempotence", "false")])).expect("valid");
        assert!(!config.enable_idempotence, "idempotence should be overwritten");
        assert_eq!(config.acks, 0, "acks should be overwritten");

        // Valid: transactional.id alone leaves the idempotence/acks defaults.
        let config = ProducerConfig::new(&props_with(&[("transactional.id", "transactionalId")])).expect("valid");
        assert!(config.enable_idempotence, "idempotence should be set with the default value");
        assert_eq!(config.acks, -1, "acks should be set with the default value");

        // Valid: acks=all with idempotence explicitly off.
        let config =
            ProducerConfig::new(&props_with(&[("acks", "all"), ("enable.idempotence", "false")])).expect("valid");
        assert!(!config.enable_idempotence, "idempotence should be overwritten");
        assert_eq!(config.acks, -1, "acks should be overwritten");

        // Valid: acks=0 with idempotence UNSET silently disables idempotence.
        // This is the path that keeps existing configurations working.
        let config = ProducerConfig::new(&props_with(&[("acks", "0")])).expect("valid");
        assert!(
            !config.enable_idempotence,
            "idempotence should be disabled when acks not set to all and `enable.idempotence` is unset"
        );
        assert_eq!(config.acks, 0, "acks should be set with overridden value");

        // Same for acks=1.
        let config = ProducerConfig::new(&props_with(&[("acks", "1")])).expect("valid");
        assert!(!config.enable_idempotence);
        assert_eq!(config.acks, 1);

        // Invalid: transactional.id without idempotence.
        let error = ProducerConfig::new(&props_with(&[
            ("acks", "0"),
            ("enable.idempotence", "false"),
            ("transactional.id", "transactionalId"),
        ]))
        .expect_err("transactional.id requires idempotence");
        assert_eq!(
            error.message(),
            "Cannot set a transactional.id without also enabling idempotence."
        );

        // Invalid: explicitly enabling idempotence with acks=1 still errors,
        // rather than silently disabling.
        let error = ProducerConfig::new(&props_with(&[("acks", "1"), ("enable.idempotence", "true")]))
            .expect_err("explicit idempotence with acks!=all must error");
        assert_eq!(
            error.message(),
            "Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence."
        );

        // Invalid: acks=0 with a transactional id — idempotence is silently
        // disabled by the acks arm, and the transactional.id check then fails.
        let error = ProducerConfig::new(&props_with(&[("acks", "0"), ("transactional.id", "transactionalId")]))
            .expect_err("transactional producer requires acks=all");
        assert_eq!(
            error.message(),
            "Cannot set a transactional.id without also enabling idempotence."
        );
    }

    /// Translated from `KafkaProducerTest.testRetriesAndIdempotenceForIdempotentProducers`.
    #[test]
    fn test_retries_and_idempotence_for_idempotent_producers() {
        // Valid: retries=0 with idempotence explicitly off.
        let config =
            ProducerConfig::new(&props_with(&[("retries", "0"), ("enable.idempotence", "false")])).expect("valid");
        assert!(!config.enable_idempotence, "idempotence should be overwritten");
        assert_eq!(config.retries, 0, "retries should be overwritten");

        // Valid: retries=0 with idempotence UNSET silently disables idempotence.
        let config = ProducerConfig::new(&props_with(&[("retries", "0")])).expect("valid");
        assert!(
            !config.enable_idempotence,
            "idempotence should be disabled when retries set to 0 and `enable.idempotence` is unset"
        );
        assert_eq!(config.retries, 0, "retries should be set with overridden value");

        // Invalid: transactional.id without idempotence.
        let error = ProducerConfig::new(&props_with(&[
            ("retries", "0"),
            ("enable.idempotence", "false"),
            ("transactional.id", "transactionalId"),
        ]))
        .expect_err("transactional.id requires idempotence");
        assert_eq!(
            error.message(),
            "Cannot set a transactional.id without also enabling idempotence."
        );

        // Invalid: explicitly enabling idempotence with retries=0.
        let error = ProducerConfig::new(&props_with(&[("retries", "0"), ("enable.idempotence", "true")]))
            .expect_err("explicit idempotence with retries=0 must error");
        assert_eq!(
            error.message(),
            "Must set retries to non-zero when using the idempotent producer."
        );

        // Invalid: retries=0 with a transactional id.
        let error = ProducerConfig::new(&props_with(&[("retries", "0"), ("transactional.id", "transactionalId")]))
            .expect_err("transactional producer requires non-zero retries");
        assert_eq!(
            error.message(),
            "Cannot set a transactional.id without also enabling idempotence."
        );
    }

    /// Translated from `KafkaProducerTest.testInflightRequestsAndIdempotenceForIdempotentProducers`.
    #[test]
    fn test_inflight_requests_and_idempotence_for_idempotent_producers() {
        // Valid: in-flight above the cap is fine when idempotence is off.
        let config = ProducerConfig::new(&props_with(&[
            ("max.in.flight.requests.per.connection", "6"),
            ("enable.idempotence", "false"),
        ]))
        .expect("valid");
        assert!(!config.enable_idempotence, "idempotence should be overwritten");
        assert_eq!(config.max_in_flight_requests_per_connection, 6);

        // Invalid: with idempotence on (by default), exceeding the cap is ALWAYS
        // an error — this arm never silently disables, unlike acks and retries.
        let error = ProducerConfig::new(&props_with(&[("max.in.flight.requests.per.connection", "6")]))
            .expect_err("in-flight above 5 must error");
        assert_eq!(
            error.message(),
            "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is 6."
        );

        // Invalid: exactly at the cap, idempotence explicitly off, transactional
        // id set. Pins that the in-flight arm does not mask the transactional-id
        // arm — the in-flight value is legal here, so the error must come from
        // the transactional-id check.
        let error = ProducerConfig::new(&props_with(&[
            ("max.in.flight.requests.per.connection", "5"),
            ("enable.idempotence", "false"),
            ("transactional.id", "transactionalId"),
        ]))
        .expect_err("transactional.id without idempotence must error even at the in-flight cap");
        assert_eq!(
            error.message(),
            "Cannot set a transactional.id without also enabling idempotence."
        );

        // Invalid: above the cap with idempotence explicitly on. Pins that the
        // in-flight arm fires regardless of how idempotence came to be enabled.
        let error = ProducerConfig::new(&props_with(&[
            ("max.in.flight.requests.per.connection", "6"),
            ("enable.idempotence", "true"),
        ]))
        .expect_err("explicit idempotence above the cap must error");
        assert_eq!(
            error.message(),
            "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is 6."
        );

        // Invalid: above the cap with a transactional id. Pins that the in-flight
        // arm fires ahead of the transactional-id arm.
        let error = ProducerConfig::new(&props_with(&[
            ("max.in.flight.requests.per.connection", "6"),
            ("transactional.id", "transactionalId"),
        ]))
        .expect_err("transactional producer above the cap must error");
        assert_eq!(
            error.message(),
            "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is 6."
        );
    }

    /// Translated from `ProducerConfigTest.testUpperboundCheckOfEnableIdempotence`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.producer.ProducerConfigTest#testUpperboundCheckOfEnableIdempotence")]
    fn test_upperbound_check_of_enable_idempotence() {
        let error = ProducerConfig::new(&props_with(&[("max.in.flight.requests.per.connection", "6")]))
            .expect_err("6 exceeds the cap");
        assert_eq!(
            error.message(),
            "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is 6."
        );

        // Exactly at the cap is allowed.
        ProducerConfig::new(&props_with(&[("max.in.flight.requests.per.connection", "5")]))
            .expect("5 is at the cap and must be accepted");
    }

    /// Translated from `ProducerConfigTest.testTwoPhaseCommitIncompatibleWithTransactionTimeout`.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.producer.ProducerConfigTest#testTwoPhaseCommitIncompatibleWithTransactionTimeout"
    )]
    fn test_two_phase_commit_incompatible_with_transaction_timeout() {
        let both = props_with(&[
            ("enable.idempotence", "true"),
            ("transactional.id", "test-txn-id"),
            ("transaction.two.phase.commit.enable", "true"),
            ("transaction.timeout.ms", "60000"),
        ]);
        let error = ProducerConfig::new(&both).expect_err("2PC and timeout conflict");
        assert!(error.message().contains(ProducerConfig::TRANSACTION_TIMEOUT_CONFIG));
        assert!(
            error
                .message()
                .contains(ProducerConfig::TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG)
        );

        // Setting one but not the other is valid.
        let only_2pc = props_with(&[
            ("enable.idempotence", "true"),
            ("transactional.id", "test-txn-id"),
            ("transaction.two.phase.commit.enable", "true"),
        ]);
        ProducerConfig::new(&only_2pc).expect("2PC alone is valid");

        let only_timeout = props_with(&[
            ("enable.idempotence", "true"),
            ("transactional.id", "test-txn-id"),
            ("transaction.two.phase.commit.enable", "false"),
            ("transaction.timeout.ms", "60000"),
        ]);
        ProducerConfig::new(&only_timeout).expect("timeout alone is valid");
    }

    // -- client.id derivation ------------------------------------------------

    /// Java's `maybeOverrideClientId`: an explicit client.id is preserved.
    #[test]
    fn test_explicit_client_id_is_preserved() {
        let config = ProducerConfig::new(&props_with(&[("client.id", "my-client")])).expect("valid");
        assert_eq!(config.client_id, "my-client");
    }

    /// `ConfigDef.parseType` trims `client.id`. The producer keys generation
    /// on the key being present in the originals, not on emptiness
    /// (`ProducerConfig.java:633-635`), so a blank explicit id stays empty,
    /// as in Java. The consumer and admin client generate one instead.
    #[test]
    fn test_explicit_client_id_is_trimmed() {
        let config = ProducerConfig::new(&props_with(&[("client.id", " my-client ")])).expect("valid");
        assert_eq!(config.client_id, "my-client");
        let config = ProducerConfig::new(&props_with(&[("client.id", " ")])).expect("valid");
        assert_eq!(config.client_id, "");
    }

    /// Without an explicit client.id or transactional.id, the derived form is
    /// `producer-<n>` from the process-wide counter.
    #[test]
    fn test_client_id_derived_from_sequence() {
        let first = ProducerConfig::new(&base_properties()).expect("valid");
        let second = ProducerConfig::new(&base_properties()).expect("valid");

        assert!(
            first.client_id.starts_with("producer-"),
            "expected a derived client id, got {}",
            first.client_id
        );
        // The counter is process-wide, so successive configs differ. Compare
        // rather than asserting absolute values, since test order is arbitrary.
        assert_ne!(first.client_id, second.client_id);
        let n: i32 = first.client_id.trim_start_matches("producer-").parse().expect("numeric suffix");
        assert!(n >= 1, "Java's sequence starts at 1");
    }

    /// A copy keeps the configuration's resolved `client.id` and takes no
    /// sequence number, while the next configuration built takes the next
    /// one, as Java's next `new ProducerConfig(props)` does. The generated id
    /// stays out of the copy's originals, and its used set is its own, as
    /// `new` leaves it.
    #[test]
    fn test_copy_for_client_keeps_the_resolved_client_id() {
        let first = ProducerConfig::new(&base_properties()).expect("valid");
        let copy = first.copy_for_client().expect("copied");
        let second_copy = first.copy_for_client().expect("copied");
        let next = ProducerConfig::new(&base_properties()).expect("valid");
        assert_eq!(copy.client_id, first.client_id);
        assert_eq!(second_copy.client_id, first.client_id);
        assert_eq!(
            copy.get_string(ProducerConfig::CLIENT_ID_CONFIG).unwrap(),
            Some(first.client_id.as_str())
        );
        assert!(!copy.user_configured(ProducerConfig::CLIENT_ID_CONFIG));
        let n1: i32 = first.client_id.trim_start_matches("producer-").parse().expect("numeric suffix");
        let n2: i32 = next.client_id.trim_start_matches("producer-").parse().expect("numeric suffix");
        // Other tests take numbers concurrently, so the next one is only later.
        assert!(n2 > n1, "{} then {}", first.client_id, next.client_id);
        let unused = first.unused();
        assert_eq!(second_copy.unused(), unused);
        first.ignore(ProducerConfig::BOOTSTRAP_SERVERS_CONFIG);
        assert_eq!(second_copy.unused(), unused, "the copy's used set is its own");

        let config = ProducerConfig::new(&props_with(&[("client.id", "mine")])).expect("valid");
        assert_eq!(config.copy_for_client().expect("copied").client_id, "mine");
        let config = ProducerConfig::new(&props_with(&[("transactional.id", "t")])).expect("valid");
        assert_eq!(config.copy_for_client().expect("copied").client_id, "producer-t");
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
            let config = ProducerConfig::new(&props).unwrap();
            assert_eq!(
                config.bootstrap_servers,
                ["localhost:1".to_string(), "localhost:2".to_string()],
                "{value:?}"
            );
            let addresses =
                crate::ClientUtils::parse_and_validate_addresses(&config.bootstrap_servers, config.client_dns_lookup)
                    .unwrap();
            assert_eq!(addresses.len(), 2, "{value:?}");
        }
    }

    /// `bootstrap.servers` is validated with Java's
    /// `ValidList.anyNonDuplicateValues(false, false)` (`ProducerConfig.java:410`): an empty
    /// element is rejected with `ConfigDef`'s exact message and duplicates are removed
    /// (single-message `ConfigException`, no `Invalid value` prefix). An empty list is rejected too.
    #[test]
    fn test_bootstrap_servers_valid_list() {
        let error_message = |value: &str| {
            let props = HashMap::from([("bootstrap.servers".to_string(), value.to_string())]);
            match ProducerConfig::new(&props) {
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
        assert_eq!(ProducerConfig::new(&props).unwrap().bootstrap_servers, ["a:1".to_string()]);
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
            ProducerConfig::new(&props).unwrap().bootstrap_servers,
            ["a:1".to_string(), "b:1".to_string()]
        );
    }

    // -- Debug redaction -----------------------------------------------------

    /// `{:?}` renders no secret the user passed, and `originals` only as its
    /// key set. The props hold the seven `Type.PASSWORD` keys this client
    /// parses (`SslConfigs.java:133-137`, `:140`, `SaslConfigs.java:380`), the
    /// two OAuth `Type.PASSWORD` keys it does not parse (`SaslConfigs.java:392`,
    /// `:402`), and an unknown key carrying a serializer credential: Java
    /// prints none of those values. The parsed secrets render as `[hidden]`
    /// through their typed fields, and the non-secret `bootstrap.servers` value
    /// stays visible through its typed field only.
    #[test]
    fn test_debug_hides_secrets_and_renders_originals_as_key_set() {
        let secrets = [
            (
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"jaas-user\" password=\"jaas-S3cr3t\";",
            ),
            ("ssl.truststore.password", "truststore-password-S3cr3t"),
            ("ssl.truststore.certificates", "truststore-certificates-S3cr3t"),
            ("ssl.keystore.password", "keystore-password-S3cr3t"),
            ("ssl.keystore.key", "keystore-key-S3cr3t"),
            ("ssl.keystore.certificate.chain", "keystore-certificate-chain-S3cr3t"),
            ("ssl.key.password", "key-password-S3cr3t"),
            (
                "sasl.oauthbearer.client.credentials.client.secret",
                "oauth-client-secret-S3cr3t",
            ),
            ("sasl.oauthbearer.assertion.private.key.passphrase", "oauth-passphrase-S3cr3t"),
            ("basic.auth.user.info", "registry-key:registry-S3cr3t"),
        ];
        let mut props: HashMap<String, String> = secrets
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        props.insert("bootstrap.servers".to_string(), "visible-host:9092".to_string());
        let config = ProducerConfig::new(&props).unwrap();

        // The secrets did reach the config, typed and raw (so `originals` can
        // still be forwarded, e.g. to a `Partitioner`); they are only hidden
        // from `Debug`.
        assert_eq!(config.sasl_config.resolve_password(), Some("jaas-S3cr3t"));
        assert_eq!(
            config.ssl_config.key_password.as_ref().map(Password::value),
            Some("key-password-S3cr3t")
        );
        for (key, value) in secrets {
            assert_eq!(config.originals().get(key).map(String::as_str), Some(value), "{key}");
        }

        for rendered in [format!("{config:?}"), format!("{config:#?}")] {
            for (key, value) in secrets {
                assert!(!rendered.contains(value), "the value of {key} leaked: {rendered}");
                // Only `originals` renders key names, so this proves the key
                // set is there.
                assert!(rendered.contains(&format!("{key:?}")), "key {key} missing: {rendered}");
            }
            for fragment in ["S3cr3t", "jaas-user", "PlainLoginModule", "registry-key"] {
                assert!(!rendered.contains(fragment), "{fragment:?} leaked: {rendered}");
            }
            // `[hidden]` comes from the seven typed `Password` fields only.
            assert_eq!(rendered.matches(Password::HIDDEN).count(), 7, "{rendered}");
            // The non-secret value renders once, through its typed field and
            // not through `originals`.
            assert_eq!(rendered.matches("visible-host:9092").count(), 1, "{rendered}");
        }

        let rendered = format!("{config:?}");
        assert!(rendered.contains("bootstrap_servers: [\"visible-host:9092\"]"), "{rendered}");
        for field in [
            "jaas_config",
            "truststore_password",
            "truststore_certificates",
            "keystore_password",
            "keystore_key",
            "keystore_certificate_chain",
            "key_password",
        ] {
            let hidden = format!("{field}: Some([hidden])");
            assert!(rendered.contains(&hidden), "{hidden} missing: {rendered}");
        }
        // `originals` renders as its key set, sorted, with no value at all,
        // through the `AbstractConfig` the configuration holds.
        let expected_originals = concat!(
            "ProducerConfig { config: AbstractConfig { originals: [",
            "\"basic.auth.user.info\", ",
            "\"bootstrap.servers\", ",
            "\"sasl.jaas.config\", ",
            "\"sasl.oauthbearer.assertion.private.key.passphrase\", ",
            "\"sasl.oauthbearer.client.credentials.client.secret\", ",
            "\"ssl.key.password\", ",
            "\"ssl.keystore.certificate.chain\", ",
            "\"ssl.keystore.key\", ",
            "\"ssl.keystore.password\", ",
            "\"ssl.truststore.certificates\", ",
            "\"ssl.truststore.password\"",
            "], .. }, ",
        );
        assert!(rendered.starts_with(expected_originals), "{rendered}");
    }

    // Skipped `ProducerConfigTest` tests:
    // - `testAppendSerializerToConfig`, `testAppendSerializerToConfigWithException`
    //   exercise the static `appendSerializerToConfig`, which writes serializer
    //   classes into the config map; serializers are constructor arguments here,
    //   and the config takes no class (CLAUDE.md §2).
    // - `testValidateConfigPropertiesFile` reads `config/producer.properties` from
    //   the Apache Kafka source tree, which is not part of the Rust client.

    /// Translated from `ProducerConfigTest.testDefaultMetadataRecoveryStrategy`.
    #[test]
    fn test_default_metadata_recovery_strategy() {
        let config = ProducerConfig::new(&base_props()).unwrap();
        assert_eq!(
            Some("rebootstrap"),
            config
                .get_string(CommonClientConfigs::METADATA_RECOVERY_STRATEGY_CONFIG)
                .unwrap()
        );
    }

    /// `AbstractConfig`'s typed getters answer every key's `ConfigDef` type,
    /// including the values `postProcessParsedConfig` rewrote.
    #[test]
    fn test_typed_getters() {
        let config = ProducerConfig::new(&props_with(&[
            (ProducerConfig::BATCH_SIZE_CONFIG, "7"),
            (ProducerConfig::ACKS_CONFIG, "all"),
        ]))
        .unwrap();
        assert_eq!(Some(7), config.get_int(ProducerConfig::BATCH_SIZE_CONFIG).unwrap());
        assert_eq!(Some(5), config.get_long(ProducerConfig::LINGER_MS_CONFIG).unwrap());
        assert_eq!(
            Some(true),
            config.get_boolean(ProducerConfig::ENABLE_IDEMPOTENCE_CONFIG).unwrap()
        );
        // `parseAcks` turned `all` into `-1`.
        assert_eq!(Some("-1"), config.get_string(ProducerConfig::ACKS_CONFIG).unwrap());
        assert_eq!(None, config.get_string(ProducerConfig::TRANSACTIONAL_ID_CONFIG).unwrap());
        assert_eq!(
            Some(&["localhost:9999".to_string()][..]),
            config.get_list(ProducerConfig::BOOTSTRAP_SERVERS_CONFIG).unwrap()
        );
        assert_eq!(Some(&[][..]), config.get_list(ProducerConfig::CONFIG_PROVIDERS_CONFIG).unwrap());
        assert_eq!(
            "Configuration 'batch.size' is of type INT, not LONG",
            config.get_long(ProducerConfig::BATCH_SIZE_CONFIG).unwrap_err().message()
        );
        assert_eq!(
            "Configuration 'enable.idempotence' is of type BOOLEAN, not SHORT",
            config
                .get_short(ProducerConfig::ENABLE_IDEMPOTENCE_CONFIG)
                .unwrap_err()
                .message()
        );
        assert_eq!(
            "Configuration 'acks' is of type STRING, not DOUBLE",
            config.get_double(ProducerConfig::ACKS_CONFIG).unwrap_err().message()
        );
        assert_eq!(
            "Unknown configuration 'no.such.key'",
            config.get_int("no.such.key").unwrap_err().message()
        );
    }

    /// `configNames`.
    #[test]
    fn test_config_names() {
        let names = ProducerConfig::config_names();
        for key in [
            ProducerConfig::BOOTSTRAP_SERVERS_CONFIG,
            ProducerConfig::PARTITIONER_TYPE_CONFIG,
            ProducerConfig::COMPRESSION_GZIP_LEVEL_CONFIG,
            ProducerConfig::COMPRESSION_LZ4_LEVEL_CONFIG,
            ProducerConfig::COMPRESSION_ZSTD_LEVEL_CONFIG,
            ProducerConfig::ENABLE_METRICS_PUSH_CONFIG,
            ProducerConfig::CONFIG_PROVIDERS_CONFIG,
            SaslConfigs::SASL_MECHANISM,
            SslConfigs::SSL_KEYSTORE_TYPE_CONFIG,
        ] {
            assert!(names.contains(key), "{key}");
        }
        for key in [
            "partitioner.class",
            "key.serializer",
            "value.serializer",
            "metric.reporters",
            "interceptor.classes",
            "security.providers",
        ] {
            assert!(!names.contains(key), "{key}");
        }
    }

    /// `unused` reports the supplied keys nobody read; the getters, `ignore`
    /// and the producer's own reads mark keys used.
    #[test]
    fn test_unused_and_ignore() {
        let config = ProducerConfig::new(&props_with(&[
            (ProducerConfig::BATCH_SIZE_CONFIG, "2"),
            (ProducerConfig::LINGER_MS_CONFIG, "3"),
            (SslConfigs::SSL_KEYSTORE_TYPE_CONFIG, "PKCS12"),
            ("unknown.key", "x"),
        ]))
        .unwrap();
        // `postProcessParsedConfig` read neither of these.
        let before = config.unused();
        for key in [
            ProducerConfig::BATCH_SIZE_CONFIG,
            ProducerConfig::LINGER_MS_CONFIG,
            SslConfigs::SSL_KEYSTORE_TYPE_CONFIG,
            "unknown.key",
        ] {
            assert!(before.contains(key), "{key}: {before:?}");
        }
        config.get_int(ProducerConfig::BATCH_SIZE_CONFIG).unwrap();
        config.ignore("unknown.key");
        let after = config.unused();
        assert!(!after.contains(ProducerConfig::BATCH_SIZE_CONFIG), "{after:?}");
        assert!(!after.contains("unknown.key"), "{after:?}");
        // The producer's reads: the `ssl.*` keys only for an SSL protocol.
        config.mark_read_keys_used();
        assert_eq!(
            HashSet::from([SslConfigs::SSL_KEYSTORE_TYPE_CONFIG.to_string()]),
            config.unused()
        );
        config.log_unused();
    }

    /// Java's `equals` / `hashCode` compare the originals.
    #[test]
    fn test_equals_and_hash() {
        use std::collections::hash_map::DefaultHasher;
        let hash = |config: &ProducerConfig| {
            let mut hasher = DefaultHasher::new();
            config.hash(&mut hasher);
            hasher.finish()
        };
        let a = ProducerConfig::new(&props_with(&[(ProducerConfig::CLIENT_ID_CONFIG, "c")])).unwrap();
        let b = ProducerConfig::new(&props_with(&[(ProducerConfig::CLIENT_ID_CONFIG, "c")])).unwrap();
        let c = ProducerConfig::new(&props_with(&[(ProducerConfig::CLIENT_ID_CONFIG, "d")])).unwrap();
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));
        assert_ne!(a, c);
    }

    /// The per-codec levels (`compression.gzip.level`, `.lz4.level`,
    /// `.zstd.level`) default to each codec's default level and are checked
    /// by `CompressionType.levelValidator()`.
    #[test]
    fn test_compression_levels() {
        let config = ProducerConfig::new(&base_props()).unwrap();
        assert_eq!(-1, config.compression_gzip_level);
        assert_eq!(9, config.compression_lz4_level);
        assert_eq!(3, config.compression_zstd_level);

        let config = ProducerConfig::new(&props_with(&[
            (ProducerConfig::COMPRESSION_GZIP_LEVEL_CONFIG, "5"),
            (ProducerConfig::COMPRESSION_LZ4_LEVEL_CONFIG, "1"),
            (ProducerConfig::COMPRESSION_ZSTD_LEVEL_CONFIG, "-5"),
        ]))
        .unwrap();
        assert_eq!(5, config.compression_gzip_level);
        assert_eq!(1, config.compression_lz4_level);
        assert_eq!(-5, config.compression_zstd_level);

        for (key, value, message) in [
            (
                ProducerConfig::COMPRESSION_GZIP_LEVEL_CONFIG,
                "0",
                "Invalid value 0 for configuration compression.gzip.level: Value must be between 1 and 9 or equal to -1",
            ),
            (
                ProducerConfig::COMPRESSION_GZIP_LEVEL_CONFIG,
                "10",
                "Invalid value 10 for configuration compression.gzip.level: Value must be between 1 and 9 or equal to -1",
            ),
            (
                ProducerConfig::COMPRESSION_LZ4_LEVEL_CONFIG,
                "18",
                "Invalid value 18 for configuration compression.lz4.level: Value must be no more than 17",
            ),
            (
                ProducerConfig::COMPRESSION_ZSTD_LEVEL_CONFIG,
                "23",
                "Invalid value 23 for configuration compression.zstd.level: Value must be no more than 22",
            ),
        ] {
            let error = ProducerConfig::new(&props_with(&[(key, value)])).unwrap_err();
            assert_eq!(message, error.message(), "{key}={value}");
        }
    }
}
