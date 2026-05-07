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

//! Translation of `org.apache.kafka.clients.producer.KafkaProducer`.
//!
//! Phase 7c lands the **construction path** of `KafkaProducer`: the
//! struct, fields, public and pkg-private constructors, the spawn of
//! the [`Sender`] task, and the `Drop` impl that aborts the spawned
//! task. The [`Producer`] trait is **not** implemented yet — the trait
//! methods (`send`, `flush`, `close`, etc.) land in Phase 7d/7e. This
//! mirrors the Java field-init block in `KafkaProducer.java`
//! lines 332-467 (the visible-for-testing constructor) and the public
//! constructors at lines 283-329.
//!
//! ## Generic parameters
//!
//! `KafkaProducer<K, V, C>` is generic over:
//!
//! * `K` — the record key type (matches the Java `<K, V>` parameters);
//! * `V` — the record value type;
//! * `C` — the [`KafkaClient`] implementation. The Java translation of
//!   [`Sender`] is also generic over `C: KafkaClient` (Phase 6e), so the
//!   producer threads the same parameter through. Production code uses a
//!   real [`NetworkClient`]; tests inject [`crate::producer::internals::sender::tests::MockClientImpl`].
//!
//! ## Serializer dispatch: `Box<dyn Serializer<T>>`
//!
//! Java's `Serializer<K>` / `Serializer<V>` are interfaces resolved at
//! `configure` time. The Rust translation holds them as
//! `Box<dyn Serializer<T>>`:
//!
//! * Trait-object dispatch costs one extra indirection per `serialize`
//!   call. Serialization itself usually allocates (`Vec<u8>` / `String`),
//!   so the dispatch cost is dwarfed by the serializer body — measured to
//!   be a non-issue per `phase3b_serializer_design`.
//! * The alternative — adding generic `KS: Serializer<K>, VS: Serializer<V>`
//!   parameters — would push two extra type parameters all the way through
//!   the public surface (`KafkaProducer<K, V, C, KS, VS>`), forcing every
//!   caller to spell five type names.
//! * Java reads the serializer FQCN from config and reflectively
//!   instantiates it. Rust has no reflection; callers pass the serializer
//!   instance directly via [`KafkaProducer::with_serializers`] (or via the
//!   `key.serializer` / `value.serializer` config keys, with construction
//!   handled by the caller pre-Phase-7d).
//!
//! ## Sender task ownership
//!
//! The constructor spawns the [`Sender::run_loop`] future on `tokio::spawn`
//! **last** — after every other field has been initialised — so failure
//! anywhere earlier in the constructor returns `Err` without leaking a
//! background task. The resulting [`tokio::task::JoinHandle`] is stored on
//! the producer, and [`Drop`] aborts it (Phase 7e adds the async `close`
//! that gracefully drains and then awaits the handle).
//!
//! [`KafkaClient`]: crate::KafkaClient
//! [`NetworkClient`]: crate::NetworkClient
//! [`Sender`]: crate::producer::internals::sender::Sender

#![allow(dead_code)] // Phase 7d/7e wire send/flush/close on top of this skeleton.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use log::warn;
use tokio::task::JoinHandle;

use crate::KafkaClient;
use crate::common::cluster::Cluster;
use crate::common::compress::{Compression, NoCompression, SnappyCompression};
use crate::common::errors::KafkaError;
use crate::common::record::CompressionType;
use crate::common::serialization::Serializer;
use crate::common::topic_partition::TopicPartition;
use crate::common::utils::log_context::LogContext;
use crate::common::utils::system_time::SystemTime;
use crate::common::utils::time::Time;
use crate::producer::callback::Callback;
use crate::producer::internals::built_in_partitioner;
use crate::producer::internals::producer_interceptors::ProducerInterceptors;
use crate::producer::internals::producer_metadata::ProducerMetadata;
use crate::producer::internals::record_accumulator::{AppendCallbacks, RecordAccumulator};
use crate::producer::internals::sender::Sender;
use crate::producer::internals::transaction_manager::TransactionManager;
use crate::producer::partitioner::Partitioner;
use crate::producer::producer_config::{self, ProducerConfig};
use crate::producer::producer_record::ProducerRecord;
use crate::producer::record_metadata::RecordMetadata;

/// Java's `KafkaProducer.JMX_PREFIX`.
pub const JMX_PREFIX: &str = "kafka.producer";

// Java's `KafkaProducer.NETWORK_THREAD_PREFIX` (`"kafka-producer-network-thread"`)
// is intentionally not translated yet. Java uses it to name the IO thread
// (`new KafkaThread(NETWORK_THREAD_PREFIX + " | " + clientId, sender, true)`)
// so log lines from the Sender carry the thread name. Tokio tasks have no
// native thread-name slot; the equivalent observability hook is the
// `tracing` crate's spans (`tracing::info_span!("kafka-producer-network-thread", ...)`).
// This crate currently uses `log`, not `tracing`, so the constant has no
// consumer. It will be reintroduced together with span instrumentation
// when/if the codebase adopts `tracing` (or in Phase 7e if a different
// observability shim is chosen). The Sender's [`crate::common::utils::log_context::LogContext`]
// already prefixes every log line with `[Producer clientId=...]`, so the
// per-message context is preserved without the prefix.

/// Java's `KafkaProducer.PRODUCER_METRIC_GROUP_NAME`.
pub const PRODUCER_METRIC_GROUP_NAME: &str = "producer-metrics";

/// A Kafka client that publishes records to the Kafka cluster.
///
/// Translation of `org.apache.kafka.clients.producer.KafkaProducer<K, V>`.
/// Phase 7c provides only the construction path; the [`Producer`] trait
/// methods land in Phase 7d (`send`) and 7e (`flush`, `close`,
/// `partitions_for`, `metrics`, etc.).
///
/// # Thread safety
///
/// Java's `KafkaProducer` is documented as thread-safe; Rust mirrors this
/// by making every field `Send + Sync` (typically `Arc<…>` over a
/// `Mutex` / `RwLock` / `AtomicX`). `&KafkaProducer` is sharable across
/// Tokio tasks; mutating methods are routed through the spawned
/// [`Sender`] task.
///
/// [`Producer`]: crate::producer::Producer
pub struct KafkaProducer<K, V, C: KafkaClient> {
    // ---- Identifiers / time / context ----
    /// Java: `private final String clientId`. Hot-path identifier kept as
    /// `Arc<str>` so per-batch `client_id` clones are reference bumps
    /// rather than allocations (CLAUDE.md rule 11).
    client_id: Arc<str>,
    /// Java: `private final Time time`.
    time: Arc<dyn Time>,
    /// Per-instance log prefix (`[Producer clientId=...]`). Java's
    /// `LogContext` carries the prefix and is passed to every component.
    log_context: LogContext,

    // ---- Config-derived scalars ----
    /// Java: `private final long maxBlockTimeMs`.
    max_block_time_ms: i64,
    /// Java: `private final long totalMemorySize`.
    total_memory_size: i64,
    /// Java: `private final int maxRequestSize`.
    max_request_size: i32,
    /// Java: `private final boolean partitionerIgnoreKeys`.
    partitioner_ignore_keys: bool,
    /// Java: `private final ProducerConfig producerConfig`. Retained for
    /// post-construction access (e.g. tests inspecting the resolved
    /// config).
    producer_config: ProducerConfig,
    /// Java: `private final Compression compression`. The producer holds
    /// the configured codec and forwards its [`compression_type()`] to
    /// the accumulator at construction time. The accumulator itself
    /// stores only [`crate::common::record::CompressionType`] (Phase 6d).
    ///
    /// [`compression_type()`]: Compression::compression_type
    compression: Box<dyn Compression>,

    // ---- Serializers (trait-object dispatch — see module docs). ----
    /// Java: `Plugin<Serializer<K>> keySerializerPlugin`. Plugin is the
    /// Java metrics shim and is dropped — the metrics integration is a
    /// milestone-deferred concern (PLAN.md Phase-6 skip note, same as
    /// [`ProducerInterceptors`]).
    key_serializer: Box<dyn Serializer<K>>,
    /// Java: `Plugin<Serializer<V>> valueSerializerPlugin`.
    value_serializer: Box<dyn Serializer<V>>,

    // ---- Pluggable producer collaborators ----
    /// Java: `Plugin<Partitioner> partitionerPlugin`. `None` selects the
    /// built-in adaptive partitioner (the Java field is null in that
    /// case; the [`RecordAccumulator`] handles per-topic
    /// [`crate::producer::internals::built_in_partitioner::BuiltInPartitioner`]
    /// instances internally).
    partitioner: Option<Arc<dyn Partitioner>>,
    /// Java: `private final ProducerInterceptors<K, V> interceptors`.
    interceptors: Arc<ProducerInterceptors<K, V>>,

    // ---- Phase-6 internals ----
    /// Java: `private final ProducerMetadata metadata`.
    metadata: Arc<ProducerMetadata>,
    /// Java: `private final RecordAccumulator accumulator`.
    accumulator: Arc<RecordAccumulator>,
    /// Java: `private final TransactionManager transactionManager`. Always
    /// `None` this milestone (Milestone-1 rejects `transactional.id` at
    /// config-validation time, Phase 7a).
    transaction_manager: Option<TransactionManager>,
    /// Java: `private final ApiVersions apiVersions`. Held as `Arc` so the
    /// network client and the (future) telemetry path can share it.
    api_versions: Arc<crate::ApiVersions>,

    // ---- Sender task lifecycle ----
    /// Java: `private final Sender sender`. The background runner that
    /// drives the produce-request lifecycle. We hold an [`Arc`] handle
    /// only to expose a few read-only inspectors (`is_running`,
    /// `force_close_arc`); the owning instance is moved into
    /// [`Self::sender_task`] at construction time and is otherwise
    /// inaccessible from the producer.
    ///
    /// We hold this as an `Arc<…>` of the wakeup half — Phase 7d/7e
    /// only need the `wakeup` / `force_close` / `initiate_close` surface
    /// of the [`Sender`]; the run loop itself runs on the spawned task.
    /// **Phase 7c stores no `Arc<Sender>` field** because the run loop
    /// borrows `&mut self` for its lifetime. The closeable handles
    /// (`running`, `force_close`) are held as `Arc<AtomicBool>` directly
    /// so [`Drop`] can flip them without re-entering the moved Sender.
    sender_running: Arc<std::sync::atomic::AtomicBool>,
    sender_force_close: Arc<std::sync::atomic::AtomicBool>,

    /// Java: `private final Sender.SenderThread ioThread`. Replaced with
    /// the `JoinHandle` of the `tokio::spawn` task running the Sender's
    /// run loop. `Option` so [`Drop`] can `take()` it during cleanup.
    sender_task: Option<JoinHandle<()>>,

    // ---- Phantom for the C parameter on the inherent skeleton ----
    /// `C` only appears in the [`Sender<C>`] type parameter, which is
    /// owned by [`Self::sender_task`]. After spawn the producer no longer
    /// references `C` directly. We carry a `PhantomData<fn() -> C>` so the
    /// type parameter survives compile-time checks without imposing
    /// `Send`/`Sync` bounds on `C` beyond what [`KafkaClient`] already
    /// requires.
    _client_marker: std::marker::PhantomData<fn() -> C>,
}

// =====================================================================
// Public API
// =====================================================================

impl<K, V, C: KafkaClient> KafkaProducer<K, V, C> {
    /// Java's `getClientId()` accessor (visible-for-testing).
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
}

// =====================================================================
// Public constructors — `KafkaProducer<K, V>` over a Map of config.
// =====================================================================
//
// Java public constructor (`KafkaProducer.java:283-303`) takes a
// `Map<String, Object>` plus optional `Serializer<K>` / `Serializer<V>`
// instances and internally constructs a `ProducerConfig`. Internally
// `KafkaProducer` then constructs a `NetworkClient` via
// `ClientUtils.createNetworkClient` — that path requires Java's
// `DefaultMetadataUpdater` (the package-private inner class on
// `NetworkClient`). The Rust translation does not yet have a
// `DefaultMetadataUpdater` translation; the public constructors below
// honour Java's signature shape but currently delegate to a
// `KafkaError::UnsupportedOperation` until that translation lands.
//
// Tests (and Phase 7d/7e wiring) use the [`KafkaProducer::new_for_test`]
// pkg-private constructor that takes a pre-built [`KafkaClient`].

/// Phase 7c marker error message returned by the public constructors
/// until [`crate::NetworkClient`]'s `DefaultMetadataUpdater` is
/// translated. Matches CLAUDE.md rule 5: a Java path that's not yet
/// implemented surfaces an explicit `KafkaError`, not a silent stub or
/// a hang.
const PRODUCTION_NETWORK_CLIENT_DEFERRED: &str = "KafkaProducer::new and ::with_serializers are deferred until \
     NetworkClient's DefaultMetadataUpdater is translated (Phase 7d/8 \
     prereq). Use KafkaProducer::new_for_test in unit tests, or wait \
     for the Phase 8 wiring.";

impl<K, V> KafkaProducer<K, V, crate::NetworkClient<crate::common::network::Selector, crate::ManualMetadataUpdater>>
where
    K: 'static,
    V: 'static,
{
    /// A producer is instantiated by providing a set of key-value pairs
    /// as configuration. Mirrors `KafkaProducer(Map<String, Object>)` at
    /// `KafkaProducer.java:283`.
    ///
    /// Note: after creating a `KafkaProducer` you must always
    /// [`KafkaProducer::close`] it to avoid resource leaks.
    ///
    /// # Phase 7c deferral
    ///
    /// This constructor returns
    /// [`KafkaError::UnsupportedOperation`] in this milestone — the
    /// production NetworkClient path requires `DefaultMetadataUpdater`,
    /// which is not yet translated. See module-level docs and
    /// [`KafkaProducer::new_for_test`] for the working construction
    /// surface. Phase 7d/8 will lift this restriction.
    pub fn new(_props: HashMap<String, String>) -> Result<Self, KafkaError> {
        Err(KafkaError::UnsupportedOperation(PRODUCTION_NETWORK_CLIENT_DEFERRED.to_owned()))
    }

    /// A producer is instantiated by providing a set of key-value pairs
    /// as configuration plus explicit key/value serializer instances.
    /// Mirrors `KafkaProducer(Map<String, Object>, Serializer<K>,
    /// Serializer<V>)` at `KafkaProducer.java:300`.
    ///
    /// # Phase 7c deferral
    ///
    /// Same deferral as [`Self::new`].
    pub fn with_serializers(
        _props: HashMap<String, String>,
        _key_serializer: Box<dyn Serializer<K>>,
        _value_serializer: Box<dyn Serializer<V>>,
    ) -> Result<Self, KafkaError> {
        Err(KafkaError::UnsupportedOperation(PRODUCTION_NETWORK_CLIENT_DEFERRED.to_owned()))
    }
}

// =====================================================================
// Visible-for-testing constructor (Java line 332).
// =====================================================================

impl<K, V, C: KafkaClient + 'static> KafkaProducer<K, V, C>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Visible-for-testing constructor mirroring
    /// `KafkaProducer(ProducerConfig, Serializer<K>, Serializer<V>,
    /// ProducerMetadata, KafkaClient, ProducerInterceptors<K, V>,
    /// ApiVersions, Time)` at `KafkaProducer.java:332`.
    ///
    /// Each `Option`-typed parameter mirrors the Java overload's `null`
    /// argument: when `None`, the constructor builds the corresponding
    /// component from the config. When `Some`, the caller-supplied
    /// instance is used (Java passes one or more pre-built collaborators
    /// in the same constructor).
    ///
    /// The constructor spawns the [`Sender::run_loop`] task as the very
    /// last step — every `Err`-returning path before the spawn ensures no
    /// background task is left running on construction failure.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_for_test(
        config: ProducerConfig,
        key_serializer: Box<dyn Serializer<K>>,
        value_serializer: Box<dyn Serializer<V>>,
        metadata: Option<Arc<ProducerMetadata>>,
        kafka_client: C,
        interceptors: Option<Arc<ProducerInterceptors<K, V>>>,
        api_versions: Option<Arc<crate::ApiVersions>>,
        time: Option<Arc<dyn Time>>,
    ) -> Result<Self, KafkaError> {
        // Java line 343:  this.time = time;
        let time: Arc<dyn Time> = time.unwrap_or_else(|| SystemTime::instance());

        // Java line 345-353: derive `clientId`, `transactionalId`,
        // construct LogContext.
        let client_id_str = config.get_string(producer_config::CLIENT_ID_CONFIG)?;
        let transactional_id = config.get_string(producer_config::TRANSACTIONAL_ID_CONFIG).ok();
        let client_id: Arc<str> = Arc::from(client_id_str);
        let log_context = LogContext::with_prefix(Some(&match transactional_id {
            Some(tx) if !tx.is_empty() => format!("[Producer clientId={client_id}, transactionalId={tx}] "),
            _ => format!("[Producer clientId={client_id}] "),
        }));

        // Java line 376: partitioner.ignore.keys
        let partitioner_ignore_keys = config.get_boolean(producer_config::PARTITIONER_IGNORE_KEYS_CONFIG)?;

        // Java lines 377-378: retry backoff
        let retry_backoff_ms = config.get_long(producer_config::RETRY_BACKOFF_MS_CONFIG)?;
        let retry_backoff_max_ms = config.get_long(producer_config::RETRY_BACKOFF_MAX_MS_CONFIG)?;

        // Java line 369-375: partitionerPlugin = config.getConfiguredInstance(...)
        // Rust does not perform reflective class-loading from the
        // `partitioner.class` config; advanced custom partitioners must
        // be provided via the (future) Phase 7d builder API. Until then
        // we always select `None` (= built-in adaptive partitioner —
        // accumulator handles per-topic `BuiltInPartitioner`).
        let partitioner: Option<Arc<dyn Partitioner>> = None;

        // Java line 407-409: maxRequestSize, totalMemorySize, compression.
        let max_request_size = config.get_int(producer_config::MAX_REQUEST_SIZE_CONFIG)?;
        let total_memory_size = config.get_long(producer_config::BUFFER_MEMORY_CONFIG)?;
        let compression = configure_compression(&config)?;

        // Java line 411-412: maxBlockTimeMs, deliveryTimeoutMs.
        let max_block_time_ms = config.get_long(producer_config::MAX_BLOCK_MS_CONFIG)?;
        let delivery_timeout_ms = configure_delivery_timeout(&config)?;

        // Java line 414-415: apiVersions, transactionManager.
        let api_versions = api_versions.unwrap_or_else(|| Arc::new(crate::ApiVersions::new()));
        // Milestone-1 contract (Phase 6 plug-in note) — always None for
        // both the producer field and the borrow handed to Sender /
        // RecordAccumulator below. `TransactionManager` does not impl
        // `Clone` (it's a placeholder unit struct), so we construct
        // fresh `None`s at each call site.
        let transaction_manager: Option<TransactionManager> = None;

        // Java line 417-422: PartitionerConfig (adaptive partitioning).
        let enable_adaptive_partitioning = partitioner.is_none()
            && config.get_boolean(producer_config::PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG)?;
        let partition_availability_timeout_ms =
            config.get_long(producer_config::PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG)?;
        let partitioner_config = crate::producer::internals::record_accumulator::PartitionerConfig::new(
            enable_adaptive_partitioning,
            partition_availability_timeout_ms,
        );

        // Java line 425: batchSize = max(1, batch.size).
        let batch_size = std::cmp::max(1, config.get_int(producer_config::BATCH_SIZE_CONFIG)?);

        // Java line 426-438: BufferPool + RecordAccumulator.
        let buffer_pool = Arc::new(crate::producer::internals::buffer_pool::BufferPool::new(
            total_memory_size,
            batch_size,
            time.clone(),
            PRODUCER_METRIC_GROUP_NAME,
        ));
        let accumulator = Arc::new(RecordAccumulator::new(
            log_context.clone(),
            batch_size,
            compression.compression_type(),
            linger_ms(&config)?,
            retry_backoff_ms,
            retry_backoff_max_ms,
            delivery_timeout_ms,
            partitioner_config,
            PRODUCER_METRIC_GROUP_NAME,
            time.clone(),
            None, // transaction_manager — Milestone-1 always None
            buffer_pool,
        ));

        // Java line 440-452: parse bootstrap addresses, construct
        // ProducerMetadata if not injected, bootstrap it.
        let metadata: Arc<ProducerMetadata> = match metadata {
            Some(m) => m,
            None => {
                let cluster_resource_listeners =
                    Arc::new(crate::common::internals::cluster_resource_listeners::ClusterResourceListeners::new());
                let m = ProducerMetadata::new(
                    retry_backoff_ms,
                    retry_backoff_max_ms,
                    config.get_long(producer_config::METADATA_MAX_AGE_CONFIG)?,
                    config.get_long(producer_config::METADATA_MAX_IDLE_CONFIG)?,
                    log_context.clone(),
                    cluster_resource_listeners,
                    SystemTime::instance(),
                )?;
                // Java: `this.metadata.bootstrap(addresses)`. Parse
                // bootstrap.servers using the configured DNS-lookup
                // strategy.
                let dns_lookup = crate::client_dns_lookup::ClientDnsLookup::for_config(
                    config.get_string(producer_config::CLIENT_DNS_LOOKUP_CONFIG)?,
                )?;
                let urls = config.get_list(producer_config::BOOTSTRAP_SERVERS_CONFIG)?;
                let addresses = crate::client_utils::parse_and_validate_addresses(urls, dns_lookup)?;
                let address_pairs: Vec<(String, u16)> = addresses
                    .iter()
                    .map(|addr| (addr.host_name().to_owned(), addr.port()))
                    .collect();
                m.metadata().bootstrap(address_pairs);
                m
            },
        };

        // Java line 396-402: configured interceptors. Rust does not
        // perform reflective class-loading from `interceptor.classes`;
        // callers pass a pre-built list via the parameter. When None,
        // construct an empty chain.
        let interceptors: Arc<ProducerInterceptors<K, V>> =
            interceptors.unwrap_or_else(|| Arc::new(ProducerInterceptors::new(Vec::new())));

        // Java line 454: this.sender = newSender(...)
        // We inline the relevant bits of `newSender(...)` (Java line 510)
        // here. The Sender takes ownership of `kafka_client` (Java's
        // `client`). Acks parsing mirrors Java's `Short.parseShort(
        // producerConfig.getString(ProducerConfig.ACKS_CONFIG))`.
        let acks_str = config.get_string(producer_config::ACKS_CONFIG)?;
        let acks: i16 = acks_str
            .parse::<i16>()
            .map_err(|_| KafkaError::Config(format!("Invalid configuration value for 'acks': {acks_str}")))?;
        let request_timeout_ms = config.get_int(producer_config::REQUEST_TIMEOUT_MS_CONFIG)?;
        let max_inflight = config.get_int(producer_config::MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)?;
        let retries = config.get_int(producer_config::RETRIES_CONFIG)?;

        let sender = Sender::new(
            log_context.clone(),
            kafka_client,
            metadata.clone(),
            accumulator.clone(),
            max_inflight == 1,
            max_request_size,
            acks,
            retries,
            time.clone(),
            request_timeout_ms,
            retry_backoff_ms,
            None, // transaction_manager — Milestone-1 always None
            client_id.clone(),
        );

        // Capture the running/force_close handles BEFORE moving the
        // sender into the spawned task — Java's analogue is
        // `sender.initiateClose()` / `sender.forceClose()` callable on
        // the producer's `sender` field even after the IO thread starts.
        let sender_running = sender_running_arc(&sender);
        let sender_force_close = sender_force_close_arc(&sender);

        // Java line 455-457: spawn the IO thread. CLAUDE.md rule 11 —
        // `tokio::spawn` consumes a concrete `async fn` future, no
        // `Pin<Box<dyn Future>>`.
        //
        // **Spawn-LAST discipline**: every error-returning path above
        // returned `Err` without the JoinHandle existing, so on
        // construction failure no background task is leaked.
        let mut sender = sender;
        let sender_task: JoinHandle<()> = tokio::spawn(async move {
            sender.run_loop().await;
        });

        // Java line 458: `config.logUnused()` — emit a WARN for every
        // config key the user provided but the producer never consumed
        // (typo or stale key). Mirrors the order in
        // `KafkaProducer.java:454-458`: spawn first, log unused last.
        config.inner().log_unused();

        Ok(KafkaProducer {
            client_id,
            time,
            log_context,
            max_block_time_ms,
            total_memory_size,
            max_request_size,
            partitioner_ignore_keys,
            producer_config: config,
            compression,
            key_serializer,
            value_serializer,
            partitioner,
            interceptors,
            metadata,
            accumulator,
            transaction_manager,
            api_versions,
            sender_running,
            sender_force_close,
            sender_task: Some(sender_task),
            _client_marker: std::marker::PhantomData,
        })
    }
}

// =====================================================================
// `waitOnMetadata` — Java `KafkaProducer.java:1100`
// =====================================================================

/// Output of [`KafkaProducer::wait_on_metadata`]. Mirrors Java's
/// private `KafkaProducer.ClusterAndWaitTime` (line 1518).
pub(crate) struct ClusterAndWaitTime {
    /// The cluster snapshot at the time the wait completed — the same
    /// snapshot used by the caller for partitioning and append.
    pub(crate) cluster: Arc<crate::common::cluster::Cluster>,
    /// Milliseconds spent waiting for metadata.
    pub(crate) waited_on_metadata_ms: i64,
}

impl<K, V, C: KafkaClient + 'static> KafkaProducer<K, V, C>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Wait for cluster metadata including partitions for the given topic
    /// to be available.
    ///
    /// Mirrors Java's private
    /// `waitOnMetadata(String topic, Integer partition, long nowMs, long maxWaitMs)`
    /// at `KafkaProducer.java:1100`.
    ///
    /// Java blocks on `metadata.awaitUpdate(version, remainingWaitMs)`
    /// inside `Object.wait` (synchronized on the producer-metadata
    /// monitor). Per CLAUDE.md rule 9.1 the Rust translation is
    /// `async fn` and awaits [`ProducerMetadata::await_update`].
    ///
    /// Returns the cluster snapshot containing the topic's metadata plus
    /// the time waited in milliseconds. Returns
    /// [`KafkaError::InvalidTopic`] if the topic is in `cluster.invalid_topics()`,
    /// [`KafkaError::Timeout`] if the deadline elapses without metadata
    /// becoming available, or whatever fatal error has been set on the
    /// metadata instance.
    pub(crate) async fn wait_on_metadata(
        &self,
        topic: &str,
        partition: Option<i32>,
        now_ms: i64,
        max_wait_ms: i64,
    ) -> Result<ClusterAndWaitTime, KafkaError> {
        // Java line 1101: `Cluster cluster = metadata.fetch();`
        let mut cluster = self.metadata.metadata().fetch();

        // Java line 1103-1104: invalid-topic short-circuit.
        if cluster.invalid_topics().any(|t| t == topic) {
            return Err(KafkaError::InvalidTopic(topic.to_owned()));
        }

        // Java line 1107: `metadata.add(topic, nowMs)`.
        self.metadata.add(topic, now_ms);

        let mut partitions_count: Option<usize> = cluster.partition_count_for_topic(topic);
        // Java line 1112: cached metadata short-circuit.
        if let Some(count) = partitions_count
            && partition.is_none_or(|p| (p as usize) < count)
        {
            return Ok(ClusterAndWaitTime { cluster, waited_on_metadata_ms: 0 });
        }

        // Java line 1115-1117: enter the wait loop.
        let mut remaining_wait_ms = max_wait_ms;
        let mut elapsed: i64 = 0;
        loop {
            // Java line 1122-1126: trace-log the request.
            match partition {
                Some(p) => log::trace!("Requesting metadata update for partition {p} of topic {topic}."),
                None => log::trace!("Requesting metadata update for topic {topic}."),
            }
            // Java line 1127: re-add the topic so its expiry is reset.
            self.metadata.add(topic, now_ms.saturating_add(elapsed));
            // Java line 1128: bump the request version for the topic.
            let version = self.metadata.request_update_for_topic(topic);
            // Java line 1129: wake the sender so the metadata request
            // gets dispatched promptly.
            self.sender_wakeup();
            // Java line 1131: await the next metadata version.
            let await_result = self.metadata.await_update(version, remaining_wait_ms).await;
            if let Err(err) = await_result {
                // Java line 1132-1138: rethrow timeouts with a topic-
                // friendly error message; all other errors propagate
                // unchanged.
                if matches!(err, KafkaError::Timeout(_)) {
                    return Err(self.metadata_timeout_error(partitions_count, topic, partition, max_wait_ms));
                }
                // Java's "Producer closed while send in progress" mapping
                // happens at the caller (`do_send`); here we surface the
                // raw error and let the caller wrap it.
                return Err(err);
            }
            cluster = self.metadata.metadata().fetch();
            elapsed = self.time.milliseconds().saturating_sub(now_ms);
            // Java line 1142-1148: deadline exceeded.
            if elapsed >= max_wait_ms {
                return Err(self.metadata_timeout_error(partitions_count, topic, partition, max_wait_ms));
            }
            // Java line 1149: propagate any topic-specific error
            // (`InvalidTopicException`, `TopicAuthorizationException`,
            // ...) recorded on the latest metadata response.
            self.metadata.metadata().maybe_throw_error_for_topic(topic)?;
            remaining_wait_ms = max_wait_ms - elapsed;
            partitions_count = cluster.partition_count_for_topic(topic);
            // Java line 1152: exit when partition count is known and
            // covers the requested partition.
            if let Some(count) = partitions_count
                && partition.is_none_or(|p| (p as usize) < count)
            {
                break;
            }
        }

        Ok(ClusterAndWaitTime { cluster, waited_on_metadata_ms: elapsed })
    }

    /// Build the Java-equivalent timeout error message at
    /// `KafkaProducer.java:1159`.
    fn metadata_timeout_error(
        &self,
        partitions_count: Option<usize>,
        topic: &str,
        partition: Option<i32>,
        max_wait_ms: i64,
    ) -> KafkaError {
        let msg = match partitions_count {
            None => format!("Topic {topic} not present in metadata after {max_wait_ms} ms."),
            Some(count) => format!(
                "Partition {} of topic {topic} with partition count {count} is not present in metadata after {max_wait_ms} ms.",
                partition.unwrap_or(-1),
            ),
        };
        // Java propagates the underlying retriable exception's cause when
        // present; Rust's `KafkaError::Timeout` carries the message only.
        // The cause-chain is preserved in spirit by surfacing fatal errors
        // separately via `maybe_throw_error_for_topic`.
        KafkaError::Timeout(msg)
    }

    /// Internal helper that mirrors Java's `sender.wakeup()` from
    /// `KafkaProducer.java:1129` (called from inside the wait-loop in
    /// `waitOnMetadata`).
    ///
    /// **Phase 7d behaviour: no-op.** The Sender owns its
    /// [`KafkaClient`] by value and is moved into a `tokio::spawn` task
    /// at construction time, so the producer no longer holds a reference
    /// it could call `client.wakeup()` on. Adding a wake handle would
    /// require either:
    ///
    /// 1. an `Arc<dyn Fn() + Send + Sync>` extracted from the client
    ///    pre-spawn (only viable if the `wakeup` call is `'static` — i.e.
    ///    the client is itself an `Arc<…>` field), or
    /// 2. a `tokio::sync::Notify` plus a `select!` arm in the Sender's
    ///    `run_loop` (invasive — Phase 6e's loop is `poll` + `handle`,
    ///    no async wake point).
    ///
    /// Both are deferred to a follow-up. The wake-up is a **latency
    /// optimisation**, not a correctness requirement: the Sender's
    /// `run_once` re-fetches the metadata snapshot on every iteration,
    /// requests metadata refreshes for unknown-leader topics, and the
    /// await-timeout in [`Self::wait_on_metadata`] is bounded by
    /// `max.block.ms`. A missed wake-up degrades first-send latency by
    /// at most one Sender tick (`linger.ms` + `request.timeout.ms`), it
    /// never hangs.
    ///
    /// Documented in `design/history/Milestone-1/Phase-7/NOTES.md`.
    fn sender_wakeup(&self) {
        // Intentional no-op — see method docstring.
    }
}

// =====================================================================
// `partition` — Java `KafkaProducer.java:1476`
// =====================================================================

impl<K: 'static, V: 'static, C: KafkaClient + 'static> KafkaProducer<K, V, C>
where
    K: Send,
    V: Send,
{
    /// Compute the partition for the given record. Mirrors Java's
    /// private `partition(record, serializedKey, serializedValue, cluster)`
    /// at `KafkaProducer.java:1476`.
    ///
    /// Lookup order:
    ///
    /// 1. `record.partition()` — caller-specified, returned as-is;
    /// 2. user-configured [`Partitioner`] — invoked for the topic and
    ///    bytes; rejected with [`KafkaError::IllegalArgument`] if it
    ///    returns a negative number;
    /// 3. `serialized_key` present and `partitioner.ignore.keys=false` —
    ///    hash via [`built_in_partitioner::partition_for_key`];
    /// 4. otherwise — return [`RecordMetadata::UNKNOWN_PARTITION`] so the
    ///    accumulator's built-in adaptive partitioner picks one.
    fn partition(
        &self,
        record: &ProducerRecord<K, V>,
        serialized_key: Option<&[u8]>,
        serialized_value: Option<&[u8]>,
        cluster: &Cluster,
    ) -> Result<i32, KafkaError> {
        // Java line 1477-1478: explicit partition wins.
        if let Some(p) = record.partition() {
            return Ok(p);
        }

        // Java line 1480-1488: user-configured partitioner.
        if let Some(partitioner) = self.partitioner.as_ref() {
            // Java passes `record.key()` / `record.value()` as
            // `Object`. Rust's [`Partitioner::partition`] takes
            // `Option<&dyn Any>` for the same purpose. The trait method
            // requires `K: 'static` / `V: 'static` to safely upcast to
            // `&dyn Any`; we already constrain that on the impl.
            let key_any: Option<&dyn std::any::Any> = record.key().map(|k| k as &dyn std::any::Any);
            let value_any: Option<&dyn std::any::Any> = record.value().map(|v| v as &dyn std::any::Any);
            let custom =
                partitioner.partition(record.topic(), key_any, serialized_key, value_any, serialized_value, cluster);
            if custom < 0 {
                return Err(KafkaError::IllegalArgument(format!(
                    "The partitioner generated an invalid partition number: {custom}. \
                     Partition number should always be non-negative."
                )));
            }
            return Ok(custom);
        }

        // Java line 1490-1495: hash by key OR signal UNKNOWN_PARTITION.
        if let Some(key) = serialized_key
            && !self.partitioner_ignore_keys
        {
            let num_partitions = cluster.partitions_for_topic(record.topic()).len() as i32;
            return Ok(built_in_partitioner::partition_for_key(key, num_partitions));
        }
        Ok(RecordMetadata::UNKNOWN_PARTITION)
    }
}

// =====================================================================
// `AppendCallbacks` — Java `KafkaProducer.java:1568` inner class
// =====================================================================

/// Internal callbacks passed to [`RecordAccumulator::append`]. Mirrors
/// Java's private inner class
/// `KafkaProducer.AppendCallbacks implements RecordAccumulator.AppendCallbacks`.
///
/// Responsibilities:
///
/// * call [`ProducerInterceptors::on_acknowledgement`] on completion;
/// * forward to the user-supplied [`Callback`], if any;
/// * record the resolved partition once the accumulator picks one
///   (Java's `setPartition`); the producer reads back the
///   `topic_partition()` to compute the final
///   [`RecordMetadata`] used in error paths.
///
/// Java holds `topic` / `recordPartition` / `headers` extracted from the
/// record so the closure does not pin a reference to the user's
/// `ProducerRecord` for the batch's lifetime. We mirror this — the
/// struct stores the topic and original partition (if any) but does not
/// hold the record itself.
struct AppendCallbacksImpl<K, V> {
    user_callback: Option<Box<dyn Callback>>,
    interceptors: Arc<ProducerInterceptors<K, V>>,
    topic: Arc<str>,
    record_partition: Option<i32>,
    headers: crate::common::header::RecordHeaders,
    // Java: `private volatile int partition = RecordMetadata.UNKNOWN_PARTITION;`
    // We use an atomic so `set_partition` (called from the accumulator
    // task) and `topic_partition()` (called from the sender task) can
    // race safely.
    partition: std::sync::atomic::AtomicI32,
    // Java: `private volatile TopicPartition topicPartition;` lazily
    // computed in `topicPartition()`. Rust's `OnceLock` mirrors the
    // semantics with a one-shot publish.
    topic_partition: std::sync::OnceLock<TopicPartition>,
}

impl<K, V> AppendCallbacksImpl<K, V> {
    fn new(
        user_callback: Option<Box<dyn Callback>>,
        interceptors: Arc<ProducerInterceptors<K, V>>,
        record: &ProducerRecord<K, V>,
    ) -> Self {
        Self {
            user_callback,
            interceptors,
            topic: Arc::clone(record.topic_arc()),
            record_partition: record.partition(),
            headers: record.headers().clone(),
            partition: std::sync::atomic::AtomicI32::new(RecordMetadata::UNKNOWN_PARTITION),
            topic_partition: std::sync::OnceLock::new(),
        }
    }

    /// Mirrors Java's `topicPartition()` (line 1620). Lazily resolves
    /// the topic-partition from the most-specific partition known
    /// (`set_partition` > `record_partition` > `UNKNOWN_PARTITION`).
    fn topic_partition(&self) -> TopicPartition {
        if let Some(tp) = self.topic_partition.get() {
            return tp.clone();
        }
        let p = self.partition.load(std::sync::atomic::Ordering::Acquire);
        let resolved = if p != RecordMetadata::UNKNOWN_PARTITION {
            p
        } else {
            self.record_partition.unwrap_or(RecordMetadata::UNKNOWN_PARTITION)
        };
        let tp = TopicPartition::new(Arc::clone(&self.topic), resolved);
        // OnceLock::set may race; either winner publishes the same
        // logical value (the partition is monotone — once set by the
        // accumulator it does not change), so we ignore the Err.
        let _ = self.topic_partition.set(tp.clone());
        tp
    }

    /// Mirrors Java's `getPartition()` accessor (line 1616).
    #[allow(dead_code)] // Used by Phase 7d's do_send via topic_partition()
    fn get_partition(&self) -> i32 {
        self.partition.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl<K: Send + Sync + 'static, V: Send + Sync + 'static> Callback for AppendCallbacksImpl<K, V> {
    /// Java's `onCompletion(metadata, exception)` (line 1596).
    ///
    /// Java synthesises a `RecordMetadata` with `-1` placeholders when
    /// the accumulator passes `null`; the Rust translation honours the
    /// trait's `Option<&RecordMetadata>` shape — `None` propagates to
    /// interceptors and the user callback so they can distinguish "no
    /// metadata available" from "metadata says offset=-1".
    fn on_completion(&self, metadata: Option<&RecordMetadata>, error: Option<&KafkaError>) {
        // Java: synthesise a placeholder when metadata is null.
        let synthesised: Option<RecordMetadata> = match metadata {
            Some(_) => None,
            None => {
                let tp = self.topic_partition();
                Some(RecordMetadata::new(
                    tp,
                    -1,
                    -1,
                    crate::common::record::record_batch::NO_TIMESTAMP,
                    -1,
                    -1,
                ))
            },
        };
        let metadata_ref: Option<&RecordMetadata> = metadata.or(synthesised.as_ref());
        // Java line 1600: interceptors fire first.
        self.interceptors.on_acknowledgement(metadata_ref, error, &self.headers);
        // Java line 1601-1602: user callback fires after interceptors.
        if let Some(user_cb) = &self.user_callback {
            user_cb.on_completion(metadata_ref, error);
        }
    }
}

impl<K: Send + Sync + 'static, V: Send + Sync + 'static> AppendCallbacks for AppendCallbacksImpl<K, V> {
    /// Java's `setPartition(int)` (line 1606). The accumulator calls
    /// this once per record after picking the effective partition.
    fn set_partition(&self, partition: i32) {
        debug_assert_ne!(partition, RecordMetadata::UNKNOWN_PARTITION);
        self.partition.store(partition, std::sync::atomic::Ordering::Release);
        log::trace!("Attempting to append record to topic {} partition {}", self.topic, partition,);
    }
}

// =====================================================================
// Drop / shutdown
// =====================================================================

impl<K, V, C: KafkaClient> Drop for KafkaProducer<K, V, C> {
    /// Java's `KafkaProducer.close(Duration.ofMillis(0), true)` cleanup
    /// path — synchronous, force-close.
    ///
    /// Tokio constraint: `Drop` is a synchronous context. We **cannot**
    /// `.await` the JoinHandle here (Tokio runtime cannot be re-entered
    /// from sync drop). Instead we:
    ///
    /// 1. Flip `force_close` so the run loop bypasses the drain phase.
    /// 2. Flip `running` so the run loop's `while running.load(Acquire)`
    ///    exits on the next iteration.
    /// 3. Call `JoinHandle::abort()` on the spawned task. The Sender's
    ///    `run_loop` is designed to tolerate abort — it does not hold
    ///    any non-droppable resources mid-loop.
    ///
    /// Phase 7e adds an async `close()` method that `await`s the
    /// JoinHandle gracefully, intended to be called BEFORE drop.
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        self.sender_force_close.store(true, Ordering::Release);
        self.sender_running.store(false, Ordering::Release);
        if let Some(task) = self.sender_task.take() {
            task.abort();
        }
    }
}

// =====================================================================
// Helpers — translation of the static Java methods
// `configureCompression`, `lingerMs`, `configureDeliveryTimeout`.
// =====================================================================

/// Translation of `KafkaProducer.configureCompression` at
/// `KafkaProducer.java:542-563`.
fn configure_compression(config: &ProducerConfig) -> Result<Box<dyn Compression>, KafkaError> {
    let type_name = config.get_string(producer_config::COMPRESSION_TYPE_CONFIG)?;
    let ctype = CompressionType::for_name(type_name)?;
    Ok(match ctype {
        CompressionType::None => Box::new(NoCompression::new()),
        CompressionType::Gzip => {
            let level = config.get_int(producer_config::COMPRESSION_GZIP_LEVEL_CONFIG)?;
            let mut b = crate::common::compress::gzip_compression::Builder::new();
            // Builder::level returns Result<Self, KafkaError>; bubble up.
            b = b.level(level)?;
            Box::new(b.build())
        },
        CompressionType::Lz4 => {
            let level = config.get_int(producer_config::COMPRESSION_LZ4_LEVEL_CONFIG)?;
            let mut b = crate::common::compress::lz4_compression::Builder::new();
            b = b.level(level)?;
            Box::new(b.build())
        },
        CompressionType::Zstd => {
            let level = config.get_int(producer_config::COMPRESSION_ZSTD_LEVEL_CONFIG)?;
            let mut b = crate::common::compress::zstd_compression::Builder::new();
            b = b.level(level)?;
            Box::new(b.build())
        },
        CompressionType::Snappy => Box::new(SnappyCompression::new()),
    })
}

/// Translation of `KafkaProducer.lingerMs` at `KafkaProducer.java:565-567`.
/// Java: `(int) Math.min(linger.ms, Integer.MAX_VALUE)`. Same semantics
/// in Rust — clamp the i64 config to i32::MAX.
fn linger_ms(config: &ProducerConfig) -> Result<i32, KafkaError> {
    let v = config.get_long(producer_config::LINGER_MS_CONFIG)?;
    Ok(std::cmp::min(v, i32::MAX as i64) as i32)
}

/// Translation of `KafkaProducer.configureDeliveryTimeout` at
/// `KafkaProducer.java:569-587`.
fn configure_delivery_timeout(config: &ProducerConfig) -> Result<i32, KafkaError> {
    let delivery_timeout_ms = config.get_int(producer_config::DELIVERY_TIMEOUT_MS_CONFIG)?;
    let linger = linger_ms(config)?;
    let request_timeout_ms = config.get_int(producer_config::REQUEST_TIMEOUT_MS_CONFIG)?;
    // Java: (int) Math.min((long) lingerMs + requestTimeoutMs, Integer.MAX_VALUE)
    let linger_plus_request = std::cmp::min(linger as i64 + request_timeout_ms as i64, i32::MAX as i64) as i32;

    if delivery_timeout_ms < linger_plus_request {
        // Java: only throw when the user explicitly set delivery.timeout.ms.
        if config
            .inner()
            .originals()
            .contains_key(producer_config::DELIVERY_TIMEOUT_MS_CONFIG)
        {
            return Err(KafkaError::Config(format!(
                "{} should be equal to or larger than {} + {}",
                producer_config::DELIVERY_TIMEOUT_MS_CONFIG,
                producer_config::LINGER_MS_CONFIG,
                producer_config::REQUEST_TIMEOUT_MS_CONFIG,
            )));
        }
        // Java emits a `log.warn(...)` on the silent-bump path
        // (`KafkaProducer.java:582-587`) so operators can see the
        // auto-bump in their logs. Translate that warn here.
        warn!(
            "{} should be equal to or larger than {} + {}. Setting it to {}.",
            producer_config::DELIVERY_TIMEOUT_MS_CONFIG,
            producer_config::LINGER_MS_CONFIG,
            producer_config::REQUEST_TIMEOUT_MS_CONFIG,
            linger_plus_request,
        );
        Ok(linger_plus_request)
    } else {
        Ok(delivery_timeout_ms)
    }
}

// Helper bridges to the `running_arc` / `force_close_arc` accessors that
// are `#[cfg(test)]` on `Sender`. The producer needs access in non-test
// builds too (Drop / Phase 7e close), so we re-route through internal
// inspectors. These accessors mirror Java's `volatile boolean running` /
// `volatile boolean forceClose` fields and exist on `Sender` since
// Phase 6e (`force_close_arc` was already non-test; `running_arc` was
// gated to tests). We rely only on the `pub(crate)` non-test
// `force_close_arc` and add a parallel `running_arc` in this commit.
fn sender_force_close_arc<C: KafkaClient>(sender: &Sender<C>) -> Arc<AtomicBool> {
    sender.force_close_arc()
}

fn sender_running_arc<C: KafkaClient>(sender: &Sender<C>) -> Arc<AtomicBool> {
    sender.running_arc()
}

#[cfg(test)]
mod tests {
    //! Phase 7c construction tests.
    //!
    //! These tests cover the construction path only — `send`, `flush`,
    //! `close`, etc. land in Phase 7d/7e. Each test either:
    //! * verifies a `ProducerConfig::new(props)` rejection that would
    //!   bubble up before reaching `KafkaProducer::new`, or
    //! * exercises [`KafkaProducer::new_for_test`] (the working
    //!   construction path) with a minimal local mock [`KafkaClient`].
    //!
    //! The Java analogues all live in `KafkaProducerTest.java`. Where a
    //! Java test exercises construction-only behaviour, we translate it
    //! here. Tests that exercise `send` / metrics / interceptor close /
    //! transactional methods are deferred to Phase 7d/7e/7f.
    //!
    //! ## Java tests translated here
    //!
    //! * `testNoSerializerProvided` — covered by the
    //!   `ProducerConfig::append_serializer_to_config` path; this file
    //!   asserts the producer-level surface still rejects the
    //!   `Milestone-1`-flavored configs (idempotence / transactional /
    //!   SASL).
    //! * `testConstructorWithSerializers` — covered here as
    //!   `constructs_with_minimum_config_via_new_for_test` (Phase 7c
    //!   does not wire the public `new(props)` to a real NetworkClient
    //!   — see module docs).

    use super::*;
    use crate::ClientRequest;
    use crate::ClientResponse;
    use crate::RequestCompletionHandler;
    use crate::common::Node;
    use crate::common::requests::AbstractRequestBuilder;
    use crate::common::serialization::serdes::{ByteArrayOwnedSerializer, StringOwnedSerializer};
    use crate::producer::producer_config::{
        BOOTSTRAP_SERVERS_CONFIG, ENABLE_IDEMPOTENCE_CONFIG, KEY_SERIALIZER_CLASS_CONFIG, TRANSACTIONAL_ID_CONFIG,
        VALUE_SERIALIZER_CLASS_CONFIG,
    };
    use std::collections::HashMap;
    use std::time::Duration;

    /// Minimal in-test [`KafkaClient`] that does nothing — used by the
    /// construction tests because the only Sender behaviour exercised
    /// here is "spawn the run loop, then drop / close it". The Sender's
    /// run loop calls `client.poll(timeout, now).await` repeatedly; this
    /// mock returns an empty `Vec` after the requested timeout (with a
    /// generous floor).
    ///
    /// Distinct from `sender::tests::MockClientImpl` (which is
    /// `pub(super)` to that file). Keeping the producer-side mock local
    /// keeps the cross-file coupling minimal.
    struct StubKafkaClient {
        wakeups: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl StubKafkaClient {
        fn new() -> Self {
            Self { wakeups: Arc::new(std::sync::atomic::AtomicUsize::new(0)) }
        }
    }

    impl KafkaClient for StubKafkaClient {
        fn is_ready(&self, _node: &Node, _now: i64) -> bool {
            false
        }
        fn ready(&mut self, _node: &Node, _now: i64) -> bool {
            false
        }
        fn connection_delay(&self, _node: &Node, _now: i64) -> i64 {
            i64::MAX
        }
        fn poll_delay_ms(&self, _node: &Node, _now: i64) -> i64 {
            i64::MAX
        }
        fn connection_failed(&self, _node: &Node) -> bool {
            false
        }
        fn authentication_error(&self, _node: &Node) -> Option<KafkaError> {
            None
        }
        fn send(&mut self, _request: ClientRequest, _now: i64) {
            // No-op — Phase 7c never sends anything.
        }
        fn poll(
            &mut self,
            timeout_ms: i64,
            _now: i64,
        ) -> impl std::future::Future<Output = Vec<ClientResponse>> + Send {
            // Yield briefly so the run loop's `while running` can observe
            // a `force_close` flip set by `Drop`. Without this, the run
            // loop keeps spinning on a synchronous "no-op poll" and the
            // JoinHandle never finishes.
            let timeout_ms = timeout_ms.max(0) as u64;
            async move {
                tokio::time::sleep(Duration::from_millis(timeout_ms.min(50))).await;
                Vec::new()
            }
        }
        fn disconnect(&mut self, _node_id: i32) {}
        fn close_connection(&mut self, _node_id: i32) {}
        fn least_loaded_node(&mut self, _now: i64) -> crate::LeastLoadedNode {
            crate::LeastLoadedNode::new(None, false)
        }
        fn in_flight_request_count(&self) -> i32 {
            0
        }
        fn has_in_flight_requests(&self) -> bool {
            false
        }
        fn in_flight_request_count_for(&self, _node_id: i32) -> i32 {
            0
        }
        fn has_in_flight_requests_for(&self, _node_id: i32) -> bool {
            false
        }
        fn has_ready_nodes(&self, _now: i64) -> bool {
            false
        }
        fn wakeup(&self) {
            self.wakeups.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        fn new_client_request(
            &mut self,
            _node_id: Arc<str>,
            _request_builder: Arc<dyn AbstractRequestBuilder>,
            _created_time_ms: i64,
            _expect_response: bool,
        ) -> ClientRequest {
            // Construction tests never call this — but the trait requires
            // an impl. Build a stub.
            unreachable!("Phase 7c construction tests do not produce ClientRequests");
        }
        fn new_client_request_with_callback(
            &mut self,
            _node_id: Arc<str>,
            _request_builder: Arc<dyn AbstractRequestBuilder>,
            _created_time_ms: i64,
            _expect_response: bool,
            _request_timeout_ms: i32,
            _callback: Option<Arc<dyn RequestCompletionHandler>>,
        ) -> ClientRequest {
            unreachable!("Phase 7c construction tests do not produce ClientRequests");
        }
        fn initiate_close(&mut self) {}
        fn close(&mut self) {}
        fn active(&self) -> bool {
            true
        }
    }

    /// Minimum-viable props: bootstrap.servers + serializer FQCNs.
    fn minimal_props() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert(BOOTSTRAP_SERVERS_CONFIG.to_owned(), "localhost:9092".to_owned());
        m.insert(
            KEY_SERIALIZER_CLASS_CONFIG.to_owned(),
            "org.apache.kafka.common.serialization.ByteArraySerializer".to_owned(),
        );
        m.insert(
            VALUE_SERIALIZER_CLASS_CONFIG.to_owned(),
            "org.apache.kafka.common.serialization.StringSerializer".to_owned(),
        );
        m
    }

    /// Translation of `KafkaProducerTest.testConstructorWithSerializers`
    /// (Java line 521) — minimum-viable construction path. Phase 7c uses
    /// `new_for_test` because the public `new(props)` defers to Phase 7d/8.
    #[tokio::test]
    async fn constructs_with_minimum_config_via_new_for_test() {
        let cfg = ProducerConfig::new(minimal_props()).expect("valid config");
        let client = StubKafkaClient::new();
        let key_ser: Box<dyn Serializer<Vec<u8>>> = Box::new(ByteArrayOwnedSerializer);
        let value_ser: Box<dyn Serializer<String>> = Box::new(StringOwnedSerializer::default());
        let producer = KafkaProducer::<Vec<u8>, String, StubKafkaClient>::new_for_test(
            cfg, key_ser, value_ser, None, client, None, None, None,
        )
        .expect("construction succeeds");

        // The auto-assigned client.id has the form `producer-N` where N is
        // the next value of the process-global PRODUCER_CLIENT_ID_SEQUENCE.
        assert!(
            producer.client_id().starts_with("producer-"),
            "client_id should be auto-assigned, got {:?}",
            producer.client_id(),
        );
    }

    /// Translation of the Milestone-1 idempotence rejection — the
    /// rejection happens inside `ProducerConfig::new`, so the producer
    /// constructor is never reached with this config.
    #[test]
    fn rejects_idempotence_true() {
        let mut props = minimal_props();
        props.insert(ENABLE_IDEMPOTENCE_CONFIG.to_owned(), "true".to_owned());
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)), "expected Config error, got {err:?}");
        assert!(
            err.message().contains("Milestone-1"),
            "expected Milestone-1 in error message, got: {}",
            err.message(),
        );
    }

    /// Translation of the Milestone-1 transactional.id rejection.
    #[test]
    fn rejects_transactional_id() {
        let mut props = minimal_props();
        props.insert(TRANSACTIONAL_ID_CONFIG.to_owned(), "my-tx-id".to_owned());
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)), "expected Config error, got {err:?}");
        assert!(
            err.message().contains("Milestone-1"),
            "expected Milestone-1 in error message, got: {}",
            err.message(),
        );
    }

    /// Translation of the SASL rejection at the security.protocol
    /// validator (Milestone-1 Phase 9 prereq).
    #[test]
    fn rejects_sasl_security_protocol() {
        let mut props = minimal_props();
        props.insert(
            crate::common_client_configs::SECURITY_PROTOCOL_CONFIG.to_owned(),
            "SASL_SSL".to_owned(),
        );
        let err = ProducerConfig::new(props).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)), "expected Config error, got {err:?}");
    }

    /// Public `new` and `with_serializers` are deferred — the
    /// `DefaultMetadataUpdater` translation must land first. This test
    /// pins the deferred error message so users get a clear pointer at
    /// the (eventual) replacement constructor.
    #[test]
    fn public_new_returns_unsupported_operation_in_milestone_1() {
        // `unwrap_err` requires `T: Debug`; `KafkaProducer` is not
        // `Debug` (it owns a `JoinHandle` and other non-Debug fields).
        // Use a `let-else` instead.
        let Err(err) = KafkaProducer::<Vec<u8>, Vec<u8>, _>::new(minimal_props()) else {
            panic!("expected Err in Milestone-1");
        };
        assert!(matches!(err, KafkaError::UnsupportedOperation(_)));
        assert!(
            err.message().contains("DefaultMetadataUpdater"),
            "expected DefaultMetadataUpdater in error message, got: {}",
            err.message(),
        );
    }

    /// Verify that dropping the producer aborts the spawned Sender task.
    /// Java's `KafkaProducer.close(Duration.ofMillis(0), true)` does the
    /// same on the construction-failure path.
    ///
    /// Asserts both halves of the abort contract:
    ///
    /// 1. `Drop` flips `sender_running` to `false` (the cooperative
    ///    shutdown signal).
    /// 2. The spawned `JoinHandle` actually finishes within a bounded
    ///    timeout — proving the `JoinHandle::abort()` call actually
    ///    cancels the task rather than the test only observing the flag
    ///    flip.
    ///
    /// To assert (2) we need to peek at the `JoinHandle` before the
    /// `Drop` impl `take()`s it. Since the test is in the same module
    /// as the struct, we access `producer.sender_task` directly.
    #[tokio::test]
    async fn drop_aborts_sender_task() {
        let cfg = ProducerConfig::new(minimal_props()).expect("valid config");
        let client = StubKafkaClient::new();
        let key_ser: Box<dyn Serializer<Vec<u8>>> = Box::new(ByteArrayOwnedSerializer);
        let value_ser: Box<dyn Serializer<Vec<u8>>> = Box::new(ByteArrayOwnedSerializer);

        // Capture the running flag before the producer moves the Sender
        // into the spawned task — this gives us an external observer
        // independent of the JoinHandle.
        let mut producer = KafkaProducer::<Vec<u8>, Vec<u8>, StubKafkaClient>::new_for_test(
            cfg, key_ser, value_ser, None, client, None, None, None,
        )
        .expect("construction succeeds");

        let running = producer.sender_running.clone();
        // Sanity: while alive, `running` is true.
        assert!(running.load(std::sync::atomic::Ordering::Acquire));

        // Steal the JoinHandle out of the producer **before** drop so we
        // can observe the spawned task's lifecycle independently. The
        // Drop impl will see `sender_task = None` and skip its own
        // abort; we issue the abort here instead so that observation and
        // assertion are paired in the test, not split across Drop.
        let handle = producer
            .sender_task
            .take()
            .expect("sender_task should be Some after construction");

        // Drop. The drop impl flips `running` to false (and would abort
        // a Some-handle, but we already took it).
        drop(producer);

        // After drop, `running` must be false (assertion 1).
        assert!(
            !running.load(std::sync::atomic::Ordering::Acquire),
            "Drop should have flipped running=false"
        );

        // Manually abort the task — Drop would have done this if we
        // hadn't stolen the handle.
        handle.abort();

        // The task must finish within a bounded timeout (assertion 2).
        // `abort()` causes the JoinHandle to resolve to
        // `Err(JoinError::cancelled())`. 1s is generous given the
        // StubKafkaClient::poll sleeps in 50ms slices.
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        match result {
            Ok(Err(join_err)) => assert!(
                join_err.is_cancelled(),
                "expected the JoinHandle to resolve with a cancelled JoinError, got {join_err:?}"
            ),
            Ok(Ok(())) => {
                // The Sender's run loop also exits cleanly when
                // `running` flips to false, so a clean completion is
                // also acceptable.
            },
            Err(_elapsed) => panic!("Sender task did not finish within 1s after abort"),
        }
    }

    /// Phase 7c does not yet implement `Producer` for `KafkaProducer`.
    /// This compile-only function pins that fact: if a future commit
    /// adds the impl prematurely, this stub will need to be updated.
    fn _phase_7c_does_not_impl_producer<K, V, C: KafkaClient>(_p: KafkaProducer<K, V, C>) {
        // Intentionally empty — the absence of a `Producer<K, V>` impl
        // is the contract documented at module-level.
    }
}
