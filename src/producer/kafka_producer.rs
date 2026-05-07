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

use tokio::task::JoinHandle;

use crate::KafkaClient;
use crate::common::compress::{Compression, NoCompression, SnappyCompression};
use crate::common::errors::KafkaError;
use crate::common::record::CompressionType;
use crate::common::serialization::Serializer;
use crate::common::utils::log_context::LogContext;
use crate::common::utils::system_time::SystemTime;
use crate::common::utils::time::Time;
use crate::producer::internals::producer_interceptors::ProducerInterceptors;
use crate::producer::internals::producer_metadata::ProducerMetadata;
use crate::producer::internals::record_accumulator::RecordAccumulator;
use crate::producer::internals::sender::Sender;
use crate::producer::internals::transaction_manager::TransactionManager;
use crate::producer::partitioner::Partitioner;
use crate::producer::producer_config::{self, ProducerConfig};

/// Java's `KafkaProducer.JMX_PREFIX`.
pub const JMX_PREFIX: &str = "kafka.producer";

/// Java's `KafkaProducer.NETWORK_THREAD_PREFIX`. Used to build the
/// background-task name so log lines correlate with the Java client.
pub const NETWORK_THREAD_PREFIX: &str = "kafka-producer-network-thread";

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
        // Java: silently bumps to the lower bound.
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
    //! Phase 7c skeleton tests live in the `kafka_producer.rs` file but
    //! are gated to compile-time checks until commit 5 lands the
    //! construction tests. The struct is skeleton-only at this point.

    use super::*;

    /// Compile-only check that the struct's type parameters compose.
    /// Real instantiation requires the public constructor (commit 2).
    fn _assert_type_compiles<K, V, C: KafkaClient>(_p: KafkaProducer<K, V, C>) {}
}
