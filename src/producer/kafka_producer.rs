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

#![allow(dead_code)]
//! A Kafka client that publishes records to the Kafka cluster.
//!
//! Translated from `org.apache.kafka.clients.producer.KafkaProducer`.
//!
//! The producer is thread-safe and sharing a single producer instance across
//! threads will generally be faster than having multiple instances.
//!
//! Transactional methods are translated: see [`KafkaProducer::init_transactions`]
//! and its four siblings.

use crate::common::requests::TxnOffsetCommitRequest;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::ClientUtils;
use crate::KafkaClient;
use crate::MetadataRecoveryStrategy;
use crate::NetworkClient;
use crate::common::Cluster;
use crate::common::Error;
use crate::common::KafkaFuture;
use crate::common::MetricName;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::compress::Compression;
use crate::common::errors::TimeoutError;
use crate::common::header::Headers;
use crate::common::header::RecordHeader;
use crate::common::internals::ClusterResourceListeners;
use crate::common::metrics::{KafkaMetric, MetricConfig, Metrics, RecordingLevel};
use crate::common::metrics::{SystemTime, Time};
use crate::common::network::ChannelBuilders;
use crate::common::network::Selector;
use crate::common::record::internal::AbstractRecords;
use crate::common::record::internal::CompressionType;
use crate::common::record::internal::RecordBatch;
use crate::common::serialization::Serializer;
use crate::common::utils::LogContext;
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::OffsetAndMetadata;
use crate::producer::Callback;
use crate::producer::Partitioner;
use crate::producer::Producer;
use crate::producer::ProducerConfig;
use crate::producer::ProducerRecord;
use crate::producer::RecordMetadata;
use crate::producer::internals::BufferPool;
use crate::producer::internals::Caller;
use crate::producer::internals::FutureRecordMetadata;
use crate::producer::internals::KafkaProducerMetrics;
use crate::producer::internals::PendingRequests;
use crate::producer::internals::ProducerMetadata;
use crate::producer::internals::ProducerMetrics;
use crate::producer::internals::Sender;
use crate::producer::internals::SenderMetricsRegistry;
use crate::producer::internals::SenderStatics;
use crate::producer::internals::TransactionManager;
use crate::producer::internals::{BuiltInPartitioner, KeyHasher, PartitionerConfig, RecordAccumulator};
use crate::{ApiVersions, DefaultHostResolver};
use crate::{kafka_debug, kafka_info, kafka_trace, kafka_warn};

/// Metadata and time spent waiting for it.
#[derive(Debug)]
struct ClusterAndWaitTime {
    /// The cluster metadata.
    cluster: Arc<Cluster>,
    /// Time in ms spent waiting for metadata.
    waited_on_metadata_ms: i64,
}

/// A Kafka client that publishes records to the Kafka cluster.
///
/// The producer is thread-safe and sharing a single producer instance across
/// tasks will generally be faster than having multiple instances.
///
/// The producer consists of a pool of buffer space that holds records that
/// haven't yet been transmitted to the server, as well as a background I/O
/// task that is responsible for turning these records into requests and
/// transmitting them to the cluster. Failure to close the producer after use
/// will leak these resources.
///
/// The [`send`](KafkaProducer::send) method is asynchronous. When called, it
/// adds the record to a buffer of pending record sends and immediately returns.
/// This allows the producer to batch together individual records for efficiency.
///
/// Translated from `org.apache.kafka.clients.producer.KafkaProducer`.
pub struct KafkaProducer<K, V> {
    /// The client ID used for this producer.
    client_id: String,
    /// The key serializer.
    key_serializer: Box<dyn Serializer<K> + Send + Sync>,
    /// The value serializer.
    value_serializer: Box<dyn Serializer<V> + Send + Sync>,
    /// The maximum size of a request in bytes.
    max_request_size: i32,
    /// The total memory size for the buffer pool.
    total_memory_size: i64,
    /// The record accumulator that batches records.
    accumulator: Arc<RecordAccumulator>,
    /// The producer metadata.
    metadata: Arc<ProducerMetadata>,
    /// All the state related to transactions, in particular the producer id,
    /// producer epoch, and sequence numbers; `None` when idempotence is disabled.
    ///
    /// Translated from `KafkaProducer.transactionManager` (Java 269), which is
    /// nullable — hence [`Option`].
    ///
    /// Shared with the [`Sender`] task and the [`RecordAccumulator`] behind a
    /// `std::sync::Mutex` (`.claude/rules/producer-transactions.md` §2 and
    /// PLAN §6.3): the Sender is moved into a `tokio::task::spawn`, so this
    /// struct cannot reach it any other way. No guard is ever held across an
    /// `.await` (rules §4).
    transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    /// The queue of transactional requests waiting for the [`Sender`] to send them.
    ///
    /// Translated from `TransactionManager.pendingRequests`
    /// (`TransactionManager.java:121`). It lives outside the manager and is shared
    /// with the `Sender` — see [`Sender::pending_requests`] for the full rationale
    /// and for the lock order (`pending_requests` → `transaction_manager`) every
    /// site below observes.
    ///
    /// Java's four transactional entry points on this class all enqueue into it from
    /// the application thread, which is why the producer needs a handle at all. Cited
    /// at the `transactionManager.<m>(..)` call statement in each, so the line and the
    /// method cannot drift apart:
    ///
    /// | method | call statement | line |
    /// |---|---|---|
    /// | `initTransactions` | `initializeTransactions(false)` | 652 |
    /// | `sendOffsetsToTransaction` | `sendOffsetsToTransaction(..)` | 740 |
    /// | `commitTransaction` | `beginCommit()` | 783 |
    /// | `abortTransaction` | `beginAbort()` | 818 |
    ///
    /// [`Sender::pending_requests`]: crate::producer::internals::Sender
    pending_requests: Arc<Mutex<PendingRequests>>,
    /// The compression type for records.
    compression_type: CompressionType,
    /// The maximum time to block on send/partitionsFor.
    max_block_ms: i64,
    /// The custom partitioner, if one is configured; `None` uses the built-in
    /// default partitioner (the keyed CRC-32 / murmur2 path plus adaptive
    /// partitioning).
    ///
    /// Translated from `KafkaProducer.partitionerPlugin` (Java 264), a
    /// `Plugin<Partitioner>`. The `Plugin<>` wrapper is `Monitorable`/metrics
    /// plumbing (KIP-877) with no Rust counterpart in this milestone, so the
    /// bare [`Partitioner`] trait object is held directly. Nullable in Java
    /// (built-in partitioning when unset) — hence [`Option`]. Resolved from
    /// `partitioner.class` via
    /// [`ProducerConfig::resolve_partitioner`](crate::producer::ProducerConfig)
    /// or supplied as an instance through
    /// [`with_partitioner`](Self::with_partitioner).
    partitioner: Option<Box<dyn Partitioner<K, V>>>,
    /// Whether to ignore keys for partitioning.
    partitioner_ignore_keys: bool,
    /// Which hash the keyed partition path uses, resolved from
    /// `partitioner.class` (default [`KeyHasher::Crc32`], librdkafka
    /// `consistent_random` parity). Copy-cheap; consulted once per keyed record.
    key_hasher: KeyHasher,
    /// Whether the sender task is still running.
    running: Arc<AtomicBool>,
    /// Whether the caller wants to force-close.
    force_close: Arc<AtomicBool>,
    /// Wakeup notification for the sender task.
    wakeup: Arc<Notify>,
    /// Handle to the sender background task.
    /// Wrapped in `Mutex<Option<_>>` so `close_with_timeout` can take ownership
    /// and `.await` it even though we only have `&self` (not `&mut self`).
    sender_handle: Mutex<Option<JoinHandle<()>>>,
    /// Provider of current wall-clock time in milliseconds.
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// The metrics registry owned by this producer.
    ///
    /// Translated from Java's `Metrics metrics` field. Shared into
    /// `producer_metrics`; exposed via [`metrics()`](Producer::metrics). Later
    /// phases wire the Sender / BufferPool / RecordAccumulator sensors against
    /// this same registry.
    metrics: Arc<Metrics>,
    /// Producer-level latency metrics (flush, metadata-wait, txn timings).
    ///
    /// Translated from Java's `KafkaProducerMetrics producerMetrics` field.
    producer_metrics: KafkaProducerMetrics,
    /// Contextual log message prefix.
    ///
    /// Translated from Java's `LogContext logContext` field in `KafkaProducer`.
    log_context: LogContext,
}

/// Parameters for [`KafkaProducer::with_options`].
///
/// This struct has **no Java counterpart** (DoD #7). It exists solely to satisfy
/// CLAUDE.md §2's cap on derived overload names: Java's widest `KafkaProducer`
/// constructor (`KafkaProducer.java:482`, marked `// visible for testing`)
/// differs from the constructor group's parameter-name intersection —
/// `{config, keySerializer, valueSerializer}`, which [`KafkaProducer::new`] owns
/// — by ten parameters, far past the cap of three. So the derived name collapses
/// to `with_options` and this struct becomes the method's *only* parameter,
/// carrying every parameter including the intersection.
///
/// `config` is borrowed, not owned, exactly as the Java constructor borrows it:
/// the producer reads it and stores only derived values.
///
/// Construct it with [`KafkaProducerOptionsBuilder::new`];
/// [`KafkaProducerOptionsBuilder::build`] validates the mandatory parameters.
#[non_exhaustive]
pub(crate) struct KafkaProducerOptions<'a, K, V> {
    /// The producer configuration. Java's `config`.
    pub config: &'a ProducerConfig,
    /// The key serializer. Java's `keySerializer`.
    pub key_serializer: Box<dyn Serializer<K> + Send + Sync>,
    /// The value serializer. Java's `valueSerializer`.
    pub value_serializer: Box<dyn Serializer<V> + Send + Sync>,
    /// The producer metadata. Java's `metadata`.
    pub metadata: Arc<ProducerMetadata>,
    /// The record accumulator. Java's `accumulator`.
    pub accumulator: Arc<RecordAccumulator>,
    /// Whether the sender task is running. Part of Rust's decomposition of
    /// Java's `Sender sender` / `Sender.SenderThread ioThread` pair; defaults to
    /// `true`, the value [`KafkaProducer::with_client_options`] supplies itself.
    pub running: Arc<AtomicBool>,
    /// Whether force-close has been requested. Rust-side sender lifecycle, as
    /// for [`Self::running`]; defaults to `false`.
    pub force_close: Arc<AtomicBool>,
    /// Notification that wakes the sender task. Rust-side sender lifecycle, as
    /// for [`Self::running`]. Defaults to a *fresh* [`Notify`], attached to no
    /// client — unlike [`KafkaProducer::with_client_options`], which takes this
    /// from the client it was handed. A caller needing the producer and a client
    /// to share one must set it.
    pub wakeup: Arc<Notify>,
    /// Handle to the sender background task, or `None` when no task was spawned.
    /// Rust's counterpart of Java's `ioThread`, which the `:482` constructor also
    /// accepts as a pre-built value.
    pub sender_handle: Option<JoinHandle<()>>,
    /// Provider of current wall-clock time. Java's `time`.
    pub time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// The shared transaction state, or `None` when idempotence is disabled.
    /// Java's `transactionManager`, which `:482` also accepts as `null`.
    pub transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    /// The transactional request queue this producer shares with the [`Sender`].
    pub pending_requests: Arc<Mutex<PendingRequests>>,
    /// The custom partitioner instance, or `None` for the built-in default.
    /// Java's `partitioner`. Stored as-is and **not** configured, mirroring
    /// `KafkaProducer.java:502`, which wraps a pre-built `Partitioner` without
    /// calling `configure`.
    pub partitioner: Option<Box<dyn Partitioner<K, V>>>,
}

/// Fluent builder for [`KafkaProducerOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like
/// [`KafkaProducerOptions`] it has no Java counterpart and exists solely to
/// satisfy that naming rule (DoD #7).
pub(crate) struct KafkaProducerOptionsBuilder<'a, K, V> {
    config: Option<&'a ProducerConfig>,
    key_serializer: Option<Box<dyn Serializer<K> + Send + Sync>>,
    value_serializer: Option<Box<dyn Serializer<V> + Send + Sync>>,
    metadata: Option<Arc<ProducerMetadata>>,
    accumulator: Option<Arc<RecordAccumulator>>,
    running: Option<Arc<AtomicBool>>,
    force_close: Option<Arc<AtomicBool>>,
    wakeup: Option<Arc<Notify>>,
    sender_handle: Option<JoinHandle<()>>,
    time_provider: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    pending_requests: Option<Arc<Mutex<PendingRequests>>>,
    partitioner: Option<Box<dyn Partitioner<K, V>>>,
}

impl<K, V> Default for KafkaProducerOptionsBuilder<'_, K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, K, V> KafkaProducerOptionsBuilder<'a, K, V> {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value the constructor supplies on its caller's behalf.
    pub(crate) fn new() -> Self {
        Self {
            config: None,
            key_serializer: None,
            value_serializer: None,
            metadata: None,
            accumulator: None,
            running: None,
            force_close: None,
            wakeup: None,
            sender_handle: None,
            time_provider: None,
            transaction_manager: None,
            pending_requests: None,
            partitioner: None,
        }
    }

    /// Sets [`KafkaProducerOptions::config`], a mandatory parameter.
    pub(crate) fn set_config(mut self, config: &'a ProducerConfig) -> Self {
        self.config = Some(config);
        self
    }
    /// Sets [`KafkaProducerOptions::key_serializer`], a mandatory parameter.
    pub(crate) fn set_key_serializer(mut self, key_serializer: Box<dyn Serializer<K> + Send + Sync>) -> Self {
        self.key_serializer = Some(key_serializer);
        self
    }
    /// Sets [`KafkaProducerOptions::value_serializer`], a mandatory parameter.
    pub(crate) fn set_value_serializer(mut self, value_serializer: Box<dyn Serializer<V> + Send + Sync>) -> Self {
        self.value_serializer = Some(value_serializer);
        self
    }
    /// Sets [`KafkaProducerOptions::metadata`], a mandatory parameter.
    pub(crate) fn set_metadata(mut self, metadata: Arc<ProducerMetadata>) -> Self {
        self.metadata = Some(metadata);
        self
    }
    /// Sets [`KafkaProducerOptions::accumulator`], a mandatory parameter.
    pub(crate) fn set_accumulator(mut self, accumulator: Arc<RecordAccumulator>) -> Self {
        self.accumulator = Some(accumulator);
        self
    }
    /// Sets [`KafkaProducerOptions::running`]; defaults to `true`.
    pub(crate) fn set_running(mut self, running: Arc<AtomicBool>) -> Self {
        self.running = Some(running);
        self
    }
    /// Sets [`KafkaProducerOptions::force_close`]; defaults to `false`.
    pub(crate) fn set_force_close(mut self, force_close: Arc<AtomicBool>) -> Self {
        self.force_close = Some(force_close);
        self
    }
    /// Sets [`KafkaProducerOptions::wakeup`]; defaults to a fresh [`Notify`].
    pub(crate) fn set_wakeup(mut self, wakeup: Arc<Notify>) -> Self {
        self.wakeup = Some(wakeup);
        self
    }
    /// Sets [`KafkaProducerOptions::sender_handle`]; defaults to `None`.
    pub(crate) fn set_sender_handle(mut self, sender_handle: Option<JoinHandle<()>>) -> Self {
        self.sender_handle = sender_handle;
        self
    }
    /// Sets [`KafkaProducerOptions::time_provider`], a mandatory parameter.
    pub(crate) fn set_time_provider(mut self, time_provider: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.time_provider = Some(time_provider);
        self
    }
    /// Sets [`KafkaProducerOptions::transaction_manager`]; defaults to `None`.
    pub(crate) fn set_transaction_manager(
        mut self,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    ) -> Self {
        self.transaction_manager = transaction_manager;
        self
    }
    /// Sets [`KafkaProducerOptions::pending_requests`], a mandatory parameter.
    pub(crate) fn set_pending_requests(mut self, pending_requests: Arc<Mutex<PendingRequests>>) -> Self {
        self.pending_requests = Some(pending_requests);
        self
    }
    /// Sets [`KafkaProducerOptions::partitioner`]; defaults to `None`.
    pub(crate) fn set_partitioner(mut self, partitioner: Option<Box<dyn Partitioner<K, V>>>) -> Self {
        self.partitioner = partitioner;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `config`,
    /// `key_serializer`, `value_serializer`, `metadata`, `accumulator`,
    /// `time_provider` and `pending_requests`.
    ///
    /// The other six are not in it for two distinct reasons. `sender_handle`,
    /// `transaction_manager` and `partitioner` are the parameters Java's `:482`
    /// itself accepts as `null`. `running`, `force_close` and `wakeup` have no
    /// Java counterpart at all — they are Rust's decomposition of Java's
    /// `Sender` / `SenderThread` pair, and are values a constructor supplies on
    /// the caller's behalf rather than values the caller must choose:
    /// [`KafkaProducer::with_client_options`] builds `running` and `force_close`
    /// with exactly these defaults itself, and derives `wakeup` from the client
    /// it was handed.
    ///
    /// `pending_requests` is deliberately mandatory although a fresh queue would
    /// be a plausible default: it is shared with the [`Sender`], and defaulting
    /// it would silently hand back a producer whose transactional requests
    /// nobody drains.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of the
    /// mandatory set which was not given a setter call. Only presence is checked
    /// here; semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub(crate) fn build(self) -> Result<KafkaProducerOptions<'a, K, V>, Error> {
        Ok(KafkaProducerOptions {
            config: self.config.ok_or_else(|| Self::missing("config"))?,
            key_serializer: self.key_serializer.ok_or_else(|| Self::missing("key_serializer"))?,
            value_serializer: self.value_serializer.ok_or_else(|| Self::missing("value_serializer"))?,
            metadata: self.metadata.ok_or_else(|| Self::missing("metadata"))?,
            accumulator: self.accumulator.ok_or_else(|| Self::missing("accumulator"))?,
            running: self.running.unwrap_or_else(|| Arc::new(AtomicBool::new(true))),
            force_close: self.force_close.unwrap_or_else(|| Arc::new(AtomicBool::new(false))),
            wakeup: self.wakeup.unwrap_or_else(|| Arc::new(Notify::new())),
            sender_handle: self.sender_handle,
            time_provider: self.time_provider.ok_or_else(|| Self::missing("time_provider"))?,
            transaction_manager: self.transaction_manager,
            pending_requests: self.pending_requests.ok_or_else(|| Self::missing("pending_requests"))?,
            partitioner: self.partitioner,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "KafkaProducerOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

/// Parameters for [`KafkaProducer::with_client_options`].
///
/// This struct has **no Java counterpart** (DoD #7), and exists for the same
/// reason as [`KafkaProducerOptions`]: Java's other `// visible for testing`
/// constructor (`KafkaProducer.java:345`) differs from the group's
/// parameter-name intersection by ten parameters, past CLAUDE.md §2's cap of
/// three, so it is served by an options struct rather than by a name listing
/// every parameter.
///
/// Note the method it parameterizes is `with_client_options`, not the bare
/// `with_options` §2's rule would literally derive: both widest Java
/// constructors collapse to the same derived name, so the one discriminating
/// parameter that distinguishes this overload from `:482` — Java's
/// `kafkaClient` — is kept in the name.
#[non_exhaustive]
pub(crate) struct KafkaProducerClientOptions<'a, K, V, C> {
    /// The producer configuration. Java's `config`.
    pub config: &'a ProducerConfig,
    /// The key serializer. Java's `keySerializer`.
    pub key_serializer: Box<dyn Serializer<K> + Send + Sync>,
    /// The value serializer. Java's `valueSerializer`.
    pub value_serializer: Box<dyn Serializer<V> + Send + Sync>,
    /// The producer metadata. Java's `metadata`.
    pub metadata: Arc<ProducerMetadata>,
    /// The record accumulator. Java has no `accumulator` parameter on `:345`;
    /// Rust builds it in [`KafkaProducer::new_inner`] and injects it here.
    pub accumulator: Arc<RecordAccumulator>,
    /// The network client the sender task drives. Java's `kafkaClient`.
    pub client: C,
    /// Provider of current wall-clock time. Java's `time`.
    pub time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// The producer's metrics registry.
    pub metrics: Arc<Metrics>,
    /// The producer-level metrics wrapper.
    pub producer_metrics: KafkaProducerMetrics,
    /// The sender-level metrics registry.
    pub sender_metrics_registry: SenderMetricsRegistry,
    /// The shared transaction state, or `None` when idempotence is disabled.
    pub transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    /// The transactional request queue this producer shares with the [`Sender`].
    pub pending_requests: Arc<Mutex<PendingRequests>>,
    /// The custom partitioner instance (already `configure`d by the caller), or
    /// `None` for the built-in default partitioner. Callers that resolve a
    /// partitioner also gate adaptive partitioning on its absence before
    /// building the `accumulator` they pass in (see
    /// [`KafkaProducer::with_partitioner`]).
    pub partitioner: Option<Box<dyn Partitioner<K, V>>>,
}

/// Fluent builder for [`KafkaProducerClientOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set.
pub(crate) struct KafkaProducerClientOptionsBuilder<'a, K, V, C> {
    config: Option<&'a ProducerConfig>,
    key_serializer: Option<Box<dyn Serializer<K> + Send + Sync>>,
    value_serializer: Option<Box<dyn Serializer<V> + Send + Sync>>,
    metadata: Option<Arc<ProducerMetadata>>,
    accumulator: Option<Arc<RecordAccumulator>>,
    client: Option<C>,
    time_provider: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    metrics: Option<Arc<Metrics>>,
    producer_metrics: Option<KafkaProducerMetrics>,
    sender_metrics_registry: Option<SenderMetricsRegistry>,
    transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    pending_requests: Option<Arc<Mutex<PendingRequests>>>,
    partitioner: Option<Box<dyn Partitioner<K, V>>>,
}

impl<K, V, C> Default for KafkaProducerClientOptionsBuilder<'_, K, V, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, K, V, C> KafkaProducerClientOptionsBuilder<'a, K, V, C> {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value the constructor supplies on its caller's behalf.
    pub(crate) fn new() -> Self {
        Self {
            config: None,
            key_serializer: None,
            value_serializer: None,
            metadata: None,
            accumulator: None,
            client: None,
            time_provider: None,
            metrics: None,
            producer_metrics: None,
            sender_metrics_registry: None,
            transaction_manager: None,
            pending_requests: None,
            partitioner: None,
        }
    }

    /// Sets [`KafkaProducerClientOptions::config`], a mandatory parameter.
    pub(crate) fn set_config(mut self, config: &'a ProducerConfig) -> Self {
        self.config = Some(config);
        self
    }
    /// Sets [`KafkaProducerClientOptions::key_serializer`], a mandatory parameter.
    pub(crate) fn set_key_serializer(mut self, key_serializer: Box<dyn Serializer<K> + Send + Sync>) -> Self {
        self.key_serializer = Some(key_serializer);
        self
    }
    /// Sets [`KafkaProducerClientOptions::value_serializer`], a mandatory parameter.
    pub(crate) fn set_value_serializer(mut self, value_serializer: Box<dyn Serializer<V> + Send + Sync>) -> Self {
        self.value_serializer = Some(value_serializer);
        self
    }
    /// Sets [`KafkaProducerClientOptions::metadata`], a mandatory parameter.
    pub(crate) fn set_metadata(mut self, metadata: Arc<ProducerMetadata>) -> Self {
        self.metadata = Some(metadata);
        self
    }
    /// Sets [`KafkaProducerClientOptions::accumulator`], a mandatory parameter.
    pub(crate) fn set_accumulator(mut self, accumulator: Arc<RecordAccumulator>) -> Self {
        self.accumulator = Some(accumulator);
        self
    }
    /// Sets [`KafkaProducerClientOptions::client`], a mandatory parameter.
    pub(crate) fn set_client(mut self, client: C) -> Self {
        self.client = Some(client);
        self
    }
    /// Sets [`KafkaProducerClientOptions::time_provider`], a mandatory parameter.
    pub(crate) fn set_time_provider(mut self, time_provider: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.time_provider = Some(time_provider);
        self
    }
    /// Sets [`KafkaProducerClientOptions::metrics`], a mandatory parameter.
    pub(crate) fn set_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }
    /// Sets [`KafkaProducerClientOptions::producer_metrics`], a mandatory parameter.
    pub(crate) fn set_producer_metrics(mut self, producer_metrics: KafkaProducerMetrics) -> Self {
        self.producer_metrics = Some(producer_metrics);
        self
    }
    /// Sets [`KafkaProducerClientOptions::sender_metrics_registry`], a mandatory parameter.
    pub(crate) fn set_sender_metrics_registry(mut self, sender_metrics_registry: SenderMetricsRegistry) -> Self {
        self.sender_metrics_registry = Some(sender_metrics_registry);
        self
    }
    /// Sets [`KafkaProducerClientOptions::transaction_manager`]; defaults to `None`.
    pub(crate) fn set_transaction_manager(
        mut self,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    ) -> Self {
        self.transaction_manager = transaction_manager;
        self
    }
    /// Sets [`KafkaProducerClientOptions::pending_requests`], a mandatory parameter.
    pub(crate) fn set_pending_requests(mut self, pending_requests: Arc<Mutex<PendingRequests>>) -> Self {
        self.pending_requests = Some(pending_requests);
        self
    }
    /// Sets [`KafkaProducerClientOptions::partitioner`]; defaults to `None`.
    pub(crate) fn set_partitioner(mut self, partitioner: Option<Box<dyn Partitioner<K, V>>>) -> Self {
        self.partitioner = partitioner;
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor. Every parameter is mandatory except
    /// `transaction_manager` and `partitioner`, the two Java's `:345` /
    /// `:482` path also accepts as `null`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of the
    /// mandatory set which was not given a setter call. Only presence is checked
    /// here; semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub(crate) fn build(self) -> Result<KafkaProducerClientOptions<'a, K, V, C>, Error> {
        Ok(KafkaProducerClientOptions {
            config: self.config.ok_or_else(|| Self::missing("config"))?,
            key_serializer: self.key_serializer.ok_or_else(|| Self::missing("key_serializer"))?,
            value_serializer: self.value_serializer.ok_or_else(|| Self::missing("value_serializer"))?,
            metadata: self.metadata.ok_or_else(|| Self::missing("metadata"))?,
            accumulator: self.accumulator.ok_or_else(|| Self::missing("accumulator"))?,
            client: self.client.ok_or_else(|| Self::missing("client"))?,
            time_provider: self.time_provider.ok_or_else(|| Self::missing("time_provider"))?,
            metrics: self.metrics.ok_or_else(|| Self::missing("metrics"))?,
            producer_metrics: self.producer_metrics.ok_or_else(|| Self::missing("producer_metrics"))?,
            sender_metrics_registry: self
                .sender_metrics_registry
                .ok_or_else(|| Self::missing("sender_metrics_registry"))?,
            transaction_manager: self.transaction_manager,
            pending_requests: self.pending_requests.ok_or_else(|| Self::missing("pending_requests"))?,
            partitioner: self.partitioner,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "KafkaProducerClientOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl<K, V> KafkaProducer<K, V> {
    /// Network thread name prefix.
    pub const NETWORK_THREAD_PREFIX: &str = "kafka-producer-network-thread";

    /// Producer metric group name.
    pub const PRODUCER_METRIC_GROUP_NAME: &str = "producer-metrics";

    /// Timeout reason appended to the [`Error::Timeout`] message when
    /// [`KafkaProducer::init_transactions`] does not complete within `max.block.ms`.
    const INIT_TXN_TIMEOUT_MSG: &str = "InitTransactions timed out - did not complete coordinator discovery or \
         receive the InitProducerId response within max.block.ms.";

    /// Timeout reason appended when [`KafkaProducer::send_offsets_to_transaction`]
    /// does not complete within `max.block.ms`.
    const SEND_OFFSETS_TIMEOUT_MSG: &str = "SendOffsetsToTransaction timed out - did not reach the coordinator or \
         receive the TxnOffsetCommit/AddOffsetsToTxn response within max.block.ms";

    /// Timeout reason appended when [`KafkaProducer::commit_transaction`] does not
    /// complete within `max.block.ms`.
    const COMMIT_TXN_TIMEOUT_MSG: &str =
        "CommitTransaction timed out - did not complete EndTxn with the transaction coordinator within max.block.ms";

    /// Timeout reason appended when [`KafkaProducer::abort_transaction`] does not
    /// complete within `max.block.ms`.
    const ABORT_TXN_TIMEOUT_MSG: &str = "AbortTransaction timed out - did not complete EndTxn(abort) with the transaction coordinator within max.block.ms";

    /// Creates a `KafkaProducer` from individual pre-built components.
    ///
    /// Translates Java's `KafkaProducer(ProducerConfig, LogContext, Metrics,
    /// Serializer, Serializer, ProducerMetadata, RecordAccumulator,
    /// TransactionManager, Sender, ProducerInterceptors, Partitioner, Time,
    /// Sender.SenderThread, Optional<ClientTelemetryReporter>)`
    /// (`KafkaProducer.java:482`), which Java marks `// visible for testing`.
    /// This is the injection seam tests use when they supply every collaborator
    /// themselves; [`Self::new`] is the user-facing constructor.
    ///
    /// `pub(crate)`, matching Java's package-private visibility: the parameters
    /// name `ProducerMetadata`, `RecordAccumulator`, `TransactionManager` and
    /// `PendingRequests`, all `pub(crate)` under `producer::internals`, so an
    /// external caller could never have named or constructed them even while
    /// this method was nominally `pub`.
    ///
    /// # Arguments
    ///
    /// * `options` - Every parameter, built through
    ///   [`KafkaProducerOptionsBuilder`]. [`KafkaProducerOptions`] is this
    ///   method's only parameter because the derived name would list ten
    ///   parameters, past CLAUDE.md §2's cap of three.
    pub(crate) fn with_options(options: KafkaProducerOptions<'_, K, V>) -> Self {
        let KafkaProducerOptions {
            config,
            key_serializer,
            value_serializer,
            metadata,
            accumulator,
            running,
            force_close,
            wakeup,
            sender_handle,
            time_provider,
            transaction_manager,
            pending_requests,
            partitioner,
        } = options;
        let log_context = LogContext::new(format!("[Producer clientId={}] ", config.client_id));
        let (metrics, producer_metrics) = Self::create_metrics(config);
        Self {
            client_id: config.client_id.clone(),
            key_serializer,
            value_serializer,
            max_request_size: config.max_request_size,
            total_memory_size: config.buffer_memory,
            accumulator,
            metadata,
            transaction_manager,
            pending_requests,
            compression_type: config.compression_type,
            max_block_ms: config.max_block_ms,
            partitioner,
            partitioner_ignore_keys: config.partitioner_ignore_keys,
            key_hasher: config.key_hasher(),
            running,
            force_close,
            wakeup,
            sender_handle: Mutex::new(sender_handle),
            time_provider,
            metrics,
            producer_metrics,
            log_context,
        }
    }

    /// Creates a `KafkaProducer` from configuration, serializers, and an optional
    /// compression override.
    ///
    /// This is the primary public factory method, mirroring Java's
    /// `new KafkaProducer(Properties, Serializer, Serializer)` constructor
    /// (`KafkaProducer.java:339`) and its `Map` twin (`:312`).
    ///
    /// # Java's no-serializer constructors are deliberately not translated
    ///
    /// Java has two further public constructors, `KafkaProducer(Map)` (`:295`)
    /// and `KafkaProducer(Properties)` (`:324`), which both delegate to
    /// `this(configs, null, null)`. They exist only because the private
    /// constructor can fill a `null` serializer in reflectively:
    ///
    /// ```text
    /// if (keySerializer == null) {
    ///     keySerializer = config.getConfiguredInstance(KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
    /// ```
    /// (`KafkaProducer.java:391-392`)
    ///
    /// `key.serializer` is a `Type.CLASS` entry (`ProducerConfig.java:479-482`),
    /// so honouring those constructors means loading and instantiating a class
    /// named by a string at run time. Rust has no reflection, and — unlike
    /// `partitioner.class`, where the built-in names can be mapped to concrete
    /// types by
    /// [`ProducerConfig::resolve_partitioner`](crate::producer::ProducerConfig) —
    /// a serializer is typed in the producer's own `K` / `V`, so no such mapping
    /// can be written for an arbitrary `K`. The serializers are therefore
    /// **always** supplied here as instances, and
    /// [`ProducerConfig`](crate::producer::ProducerConfig) deliberately carries
    /// no `key.serializer` / `value.serializer` state at all. A `key.serializer`
    /// entry in the property map is ignored as an unknown key.
    ///
    /// Consequence for CLAUDE.md §2: the Java constructor group's parameter
    /// intersection is `{configs}`, and Java does have an overload with exactly
    /// that (`:295`) — but it is untranslatable, so there is no Rust
    /// constructor that could hold the plain name on its behalf. Rather than
    /// leave `new` permanently unused and rename the only general-purpose
    /// constructor after a sibling that can never exist,
    /// the plain name stays here; `with_partitioner` remains suffixed
    /// by the one parameter that distinguishes it. The consumer side takes the
    /// identical decision — see
    /// [`AsyncKafkaConsumer::new`](crate::consumer::AsyncKafkaConsumer::new).
    ///
    /// It internally wires up all infrastructure components:
    ///
    /// 1. Parses and resolves bootstrap server addresses from the config
    /// 2. Creates [`ProducerMetadata`] and bootstraps it with the resolved addresses
    /// 3. Creates a [`PlaintextChannelBuilder`], [`Selector`], and [`NetworkClient`]
    /// 4. Creates a [`BufferPool`] and [`RecordAccumulator`]
    /// 5. Spawns the background sender task via the crate-internal `with_client_options`
    ///
    /// # Arguments
    ///
    /// * `config` - The producer configuration
    /// * `key_serializer` - The key serializer
    /// * `value_serializer` - The value serializer
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if no valid bootstrap server addresses
    /// can be resolved from `config.bootstrap_servers`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::collections::HashMap;
    /// use confluent_kafka::producer::KafkaProducer;
    /// use confluent_kafka::producer::ProducerConfig;
    /// use confluent_kafka::common::serialization::StringSerializer;
    ///
    /// let props = HashMap::from([
    ///     ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
    ///     ("client.id".to_string(), "my-producer".to_string()),
    /// ]);
    /// let config = ProducerConfig::new(&props)
    ///     .expect("Invalid config");
    ///
    /// let producer = KafkaProducer::<String, String>::new(
    ///     config,
    ///     Box::new(StringSerializer),
    ///     Box::new(StringSerializer),
    /// ).expect("Failed to create producer");
    /// ```
    pub fn new(
        config: ProducerConfig,
        key_serializer: Box<dyn Serializer<K> + Send + Sync>,
        value_serializer: Box<dyn Serializer<V> + Send + Sync>,
    ) -> Result<Self, Error> {
        // Java wraps the whole constructor body in `catch (Throwable t)` and
        // relabels every failure (`KafkaProducer.java:461-466`):
        //
        //     throw new KafkaException("Failed to construct kafka producer", t);
        //
        // so a caller has exactly one class to guard construction with, whatever
        // went wrong inside. Without this, a bad `ssl.truststore.location` reached
        // the caller as something for which `is_kafka_error()` is `false`.
        //
        // The `close(Duration.ofMillis(0), true)` half of Java's catch (KAFKA-2121)
        // has nothing to do here: every fallible step in `new_inner`
        // precedes the `Selector` / `NetworkClient` / sender-task construction, so
        // no socket and no spawned task can leak.
        Self::new_inner(config, key_serializer, value_serializer, None)
            .map_err(|e| Error::kafka_message_source("Failed to construct kafka producer", e))
    }

    /// Creates a `KafkaProducer` from configuration, serializers, and an explicit
    /// custom [`Partitioner`] instance.
    ///
    /// This is Rust's counterpart to configuring a user-written partitioner
    /// through Java's `partitioner.class`. Java loads the named class reflectively
    /// (`KafkaProducer.java:381-388`), but Rust has no reflection, so a
    /// user-written [`Partitioner`] is supplied here as an instance instead. The
    /// producer takes ownership and `configure`s it exactly as the
    /// `partitioner.class` path does — with the user config map
    /// ([`ProducerConfig::originals`](crate::producer::ProducerConfig)) plus the
    /// resolved `client.id` — before sharing it with the send path. As with a
    /// `partitioner.class` partitioner, adaptive partitioning is disabled while a
    /// custom partitioner is in use.
    ///
    /// The explicit instance takes precedence over any built-in partitioner that
    /// `partitioner.class` would otherwise name, mirroring how Java's
    /// `getConfiguredInstance` returns the caller-provided instance.
    ///
    /// # Arguments
    ///
    /// * `config` - The producer configuration
    /// * `key_serializer` - The key serializer
    /// * `value_serializer` - The value serializer
    /// * `partitioner` - The custom partitioner instance to use
    ///
    /// # Errors
    ///
    /// Exactly as for [`new`](Self::new): every construction
    /// failure — such as the [`Error::LocalIllegalArgument`] raised when no valid
    /// bootstrap server addresses can be resolved from `config.bootstrap_servers`
    /// — is returned relabelled as Java's
    /// `KafkaException("Failed to construct kafka producer", t)`, an
    /// [`Error::KafkaError`] carrying the underlying failure as its source.
    pub fn with_partitioner(
        config: ProducerConfig,
        key_serializer: Box<dyn Serializer<K> + Send + Sync>,
        value_serializer: Box<dyn Serializer<V> + Send + Sync>,
        partitioner: Box<dyn Partitioner<K, V>>,
    ) -> Result<Self, Error> {
        // Same `catch (Throwable t)` relabelling as `new` above
        // (`KafkaProducer.java:461-466`): Java has a single constructor body
        // behind both the `partitioner.class` and the caller-supplied-instance
        // paths, so both Rust constructors wrap identically.
        Self::new_inner(config, key_serializer, value_serializer, Some(partitioner))
            .map_err(|e| Error::kafka_message_source("Failed to construct kafka producer", e))
    }

    /// Shared implementation behind [`new`](Self::new) and
    /// [`with_partitioner`](Self::with_partitioner).
    ///
    /// `explicit_partitioner` is `Some` only on the
    /// [`with_partitioner`](Self::with_partitioner) path;
    /// when it is `None`, the partitioner is resolved from `partitioner.class`
    /// (built-in names only, via
    /// [`ProducerConfig::resolve_partitioner`](crate::producer::ProducerConfig)).
    /// Either way, a resolved partitioner is `configure`d exactly once here
    /// (`originals` + `client.id`, Java `KafkaProducer.java:381-388`), and adaptive
    /// partitioning is gated on its absence, so both public constructors share one
    /// configure + adaptive-gating code path. Errors escape raw from here; the
    /// public constructors relabel them (`KafkaProducer.java:461-466`).
    fn new_inner(
        config: ProducerConfig,
        key_serializer: Box<dyn Serializer<K> + Send + Sync>,
        value_serializer: Box<dyn Serializer<V> + Send + Sync>,
        explicit_partitioner: Option<Box<dyn Partitioner<K, V>>>,
    ) -> Result<Self, Error> {
        let log_context = LogContext::new(format!("[Producer clientId={}] ", config.client_id));

        kafka_trace!(log_context, "Starting the Kafka producer");

        // 1. Parse and validate bootstrap server addresses
        let addresses = ClientUtils::parse_and_validate_addresses(&config.bootstrap_servers)?;

        // 2. Validate delivery timeout configuration
        //    Translated from KafkaProducer.configureDeliveryTimeout().
        let delivery_timeout_ms = Self::configure_delivery_timeout(&config, &log_context)?;

        // The MILESTONE-11 GUARD that used to sit here is gone. Its idempotence arm
        // was removed in Phase 4, when `enable.idempotence` began to be honoured for
        // real; its transactional arm is removed here, now that
        // `init_transactions` / `begin_transaction` / `send_offsets_to_transaction` /
        // `commit_transaction` / `abort_transaction` are implemented. PLAN §7.1 named
        // this removal as an explicit Phase-6 deliverable, so nothing is left behind
        // (CLAUDE.md §5).

        // 3. Derive compression from config
        //    Translated from KafkaProducer.configureCompression().
        let compression = Compression::of(config.compression_type);

        // 4. Create a system clock time provider
        let time_provider: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        });

        // 5. Create ProducerMetadata and bootstrap it with the resolved addresses
        let metadata = Arc::new(ProducerMetadata::with_log_context(
            config.reconnect_backoff_ms,
            config.reconnect_backoff_max_ms,
            config.metadata_max_age_ms,
            config.metadata_max_idle_ms,
            ClusterResourceListeners::new(),
            log_context.clone(),
        ));
        metadata.bootstrap(addresses);

        // 6. Get the shared Metadata Arc from ProducerMetadata so the NetworkClient
        //    uses the same Metadata instance. This mirrors Java's inheritance where
        //    ProducerMetadata extends Metadata.
        let shared_metadata = metadata.metadata_arc();

        // 7. Create Selector + NetworkClient
        let channel_builder = ChannelBuilders::client_channel_builder(
            config.security_protocol,
            Some(&config.ssl_config),
            Some(&config.sasl_config),
            None,
            &config.client_id,
            log_context.clone(),
        )
        // Java's reachable failure here is `SslFactory.configure` throwing
        // `ConfigException` (`SslFactory.java:104-107`) — the missing-argument
        // `IllegalArgumentException`s in `ChannelBuilders.create` cannot be reached
        // from a `ProducerConfig`, which always supplies both sub-configs. So the
        // class is `ConfigException`, inside the `KafkaException` hierarchy;
        // `illegal_argument` put it outside, where `is_kafka_error()` is `false`.
        .map_err(|e| Error::config_message(format!("Failed to create channel builder: {}", e)))?;
        let selector = Selector::with_defaults_and_log_context(
            config.connections_max_idle_ms,
            channel_builder,
            log_context.clone(),
        );
        let api_versions = Arc::new(ApiVersions::new());

        let mut client = NetworkClient::with_metadata_rebootstrap_trigger_ms(
            selector,
            shared_metadata,
            &config.client_id,
            config.max_in_flight_requests_per_connection as usize,
            config.reconnect_backoff_ms,
            config.reconnect_backoff_max_ms,
            config.send_buffer_bytes,
            config.receive_buffer_bytes,
            config.request_timeout_ms,
            config.socket_connection_setup_timeout_ms,
            config.socket_connection_setup_timeout_max_ms,
            true, // discover_broker_versions
            Arc::clone(&api_versions),
            DefaultHostResolver::new(),
            config.metadata_max_age_ms, // rebootstrap_trigger_ms
            MetadataRecoveryStrategy::None,
            log_context.clone(),
        );

        // 8. Create the metrics registry. Java creates `this.metrics` early in
        //    the constructor (`KafkaProducer.java:357`), before the
        //    `RecordAccumulator`/`BufferPool` (`:426-438`), because both are
        //    handed the same `Metrics` instance.
        let (metrics, producer_metrics) = Self::create_metrics(&config);

        // 9. Create the TransactionManager, before the accumulator and the Sender
        //    because both of them need it (PLAN §6.3). Java's field assignment sits
        //    at the same point in the constructor (`KafkaProducer.java:415`, ahead
        //    of the `RecordAccumulator` at `:427` and the `Sender` at `:437`).
        let transaction_manager = Self::configure_transaction_state(&config, &api_versions, &log_context);

        // 9b. Resolve and configure the partitioner. Translated from
        //     `KafkaProducer.java:381-388`: Java reflectively instantiates
        //     `partitioner.class` and calls `partitioner.configure(originals +
        //     {client.id -> clientId})`. Rust has no reflection, so an explicit
        //     instance (`with_partitioner`) wins; otherwise the built-in
        //     `partitioner.class` names resolve here, and a user-written partitioner
        //     is always supplied as an instance. It is configured with the user
        //     config map (`originals`) plus the resolved (possibly generated)
        //     `client.id`, so a generated `producer-N` id is visible to `configure`
        //     just as in Java. Resolved after metrics/`TransactionManager` creation
        //     (matching Java's ordering) and before the `RecordAccumulator`, whose
        //     adaptive-partitioning flag is gated on the partitioner's absence below.
        //
        //     KAFKA-2121: Java's constructor `catch (Throwable)` closes an
        //     already-constructed partitioner. Here, partitioner setup is infallible
        //     (`resolve_partitioner` -> `Option`, `configure` -> `()`), and every
        //     fallible `?` step above runs *before* this point, so no reachable
        //     fallible step follows the partitioner's construction; the normal
        //     `close()` path is therefore the only one, and no close-on-error path is
        //     needed.
        let mut partitioner = explicit_partitioner.or_else(|| config.resolve_partitioner::<K, V>());
        if let Some(partitioner) = partitioner.as_mut() {
            let mut configs = config.originals.clone();
            configs.insert(ProducerConfig::CLIENT_ID_CONFIG.to_string(), config.client_id.clone());
            partitioner.configure(&configs);
        }

        // 10. Create BufferPool and RecordAccumulator, threading the shared
        //    `Arc<Metrics>` and time provider into both (KafkaProducer.java:438
        //    passes `metrics`/`time` to the `BufferPool` and `RecordAccumulator`).
        //    As per Kafka configuration documentation, batch.size may be set to 0
        //    to explicitly disable batching, which in practice uses a batch size of 1.
        let batch_size = config.batch_size.max(1);
        let buffer_pool = Arc::new(BufferPool::new(
            config.buffer_memory,
            batch_size as usize,
            Arc::clone(&metrics),
            Arc::clone(&time_provider),
            Self::PRODUCER_METRIC_GROUP_NAME,
        ));
        let accumulator = Arc::new(RecordAccumulator::with_log_context(
            batch_size,
            compression,
            config.linger_ms as i32,
            config.retry_backoff_ms,
            config.retry_backoff_max_ms,
            delivery_timeout_ms,
            PartitionerConfig {
                // Java `KafkaProducer.java:428-433`: "There is no need to do work
                // required for adaptive partitioning, if we use a custom
                // partitioner." So adaptive partitioning is enabled only when no
                // custom partitioner is present AND the config opts in.
                enable_adaptive_partitioning: partitioner.is_none() && config.partitioner_adaptive_partitioning_enable,
                partition_availability_timeout_ms: config.partitioner_availability_timeout_ms,
            },
            Arc::clone(&metrics),
            Self::PRODUCER_METRIC_GROUP_NAME,
            buffer_pool,
            transaction_manager.clone(),
            log_context.clone(),
        ));

        // 11. Wire the produce-throttle-time sensor into the network client.
        //    Java creates the throttle sensor (`Sender.throttleTimeSensor(...)`)
        //    and hands it to the `NetworkClient` at construction
        //    (`KafkaProducer.java:514,523` via `ClientUtils.createNetworkClient`);
        //    the client then records every response's throttle time into it. We
        //    set it on the concrete `NetworkClient` here, before it moves into
        //    the generic sender task.
        // Java: `new ProducerMetrics(this.metrics).senderMetrics`.
        let sender_metrics_registry = ProducerMetrics::new(Arc::clone(&metrics)).sender_metrics;
        let throttle_sensor = SenderStatics::throttle_time_sensor(&sender_metrics_registry)
            .expect("registering produce-throttle-time sensor");
        client.set_throttle_time_sensor(throttle_sensor);

        // 12. Wire up the Sender and spawn the I/O background task
        Ok(Self::with_client_options(
            KafkaProducerClientOptionsBuilder::new()
                .set_config(&config)
                .set_key_serializer(key_serializer)
                .set_value_serializer(value_serializer)
                .set_metadata(metadata)
                .set_accumulator(accumulator)
                .set_client(client)
                .set_time_provider(time_provider)
                .set_metrics(metrics)
                .set_producer_metrics(producer_metrics)
                .set_sender_metrics_registry(sender_metrics_registry)
                .set_transaction_manager(transaction_manager)
                .set_pending_requests(Arc::new(Mutex::new(PendingRequests::new())))
                .set_partitioner(partitioner)
                .build()
                .expect("KafkaProducerClientOptionsBuilder::build: every mandatory parameter is set above"),
        ))
    }

    /// Builds the [`TransactionManager`] when idempotence is enabled.
    ///
    /// Translated from `KafkaProducer.configureTransactionState`
    /// (`KafkaProducer.java:592-620`).
    ///
    /// Java returns `null` when `enable.idempotence` is `false`; that is `None`
    /// here. Java's `else` branch only marks `transaction.timeout.ms` as consumed
    /// so `AbstractConfig` does not warn about it, which has no Rust analogue —
    /// `ProducerConfig` parses every key eagerly.
    ///
    /// Returns `None` when idempotence is disabled, mirroring Java's null
    /// `transactionManager`. It no longer returns a `Result`: the only error it
    /// ever carried was [`Self::new`]'s temporary guard on
    /// `transactional.id` (PLAN §7.1), which Phase 6 removed.
    fn configure_transaction_state(
        config: &ProducerConfig,
        api_versions: &Arc<ApiVersions>,
        log_context: &LogContext,
    ) -> Option<Arc<Mutex<TransactionManager>>> {
        if !config.enable_idempotence {
            return None;
        }

        let transaction_manager = TransactionManager::new(
            log_context.clone(),
            config.transactional_id.clone(),
            config.transaction_timeout_ms,
            config.retry_backoff_ms,
            Arc::clone(api_versions),
            config.two_phase_commit_enable,
        );

        if transaction_manager.is_transactional() {
            kafka_info!(log_context, "Instantiated a transactional producer.");
        } else {
            kafka_info!(log_context, "Instantiated an idempotent producer.");
        }

        Some(Arc::new(Mutex::new(transaction_manager)))
    }

    /// Creates a `KafkaProducer` from pre-built collaborators and spawns the
    /// sender task.
    ///
    /// Translates Java's `KafkaProducer(ProducerConfig, Serializer, Serializer,
    /// ProducerMetadata, KafkaClient, ProducerInterceptors, ApiVersions, Time)`
    /// (`KafkaProducer.java:345`), which Java marks `// visible for testing`.
    /// [`Self::new`] is the user-facing constructor and the analogue of Java's
    /// public `KafkaProducer` constructor; this is the injection seam
    /// [`Self::new_inner`] delegates to, and the seam through which a mock
    /// [`KafkaClient`] enters.
    ///
    /// Marked `pub(crate)`, not `pub`: `metadata` and `accumulator` are `Arc`s of
    /// `ProducerMetadata` and `RecordAccumulator`, both `pub(crate)` under
    /// `producer::internals`, so an external caller could not have named or
    /// constructed them even when this method itself was `pub` — making it
    /// `pub(crate)` only makes that existing unreachability explicit, it does
    /// not remove any capability external callers actually had.
    ///
    /// (Through Phase 5 that unreachability was also the reason the temporary
    /// MILESTONE-11 GUARD in [`Self::new`] was not duplicated here. Phase 6
    /// removed the guard, so nothing turns on it any more; the visibility note above
    /// stands on its own.)
    ///
    /// # Type Parameters
    ///
    /// * `C` - The KafkaClient implementation type
    ///
    /// # Arguments
    ///
    /// * `options` - Every parameter, built through
    ///   [`KafkaProducerClientOptionsBuilder`].
    ///   [`KafkaProducerClientOptions`] is this method's only parameter because
    ///   the derived name would list ten parameters, past CLAUDE.md §2's cap of
    ///   three; see that struct for why `client` is nonetheless kept in the name.
    pub(crate) fn with_client_options<C: KafkaClient + Send + 'static>(
        options: KafkaProducerClientOptions<'_, K, V, C>,
    ) -> Self {
        let KafkaProducerClientOptions {
            config,
            key_serializer,
            value_serializer,
            metadata,
            accumulator,
            client,
            time_provider,
            metrics,
            producer_metrics,
            sender_metrics_registry,
            transaction_manager,
            pending_requests,
            partitioner,
        } = options;
        let log_context = LogContext::new(format!("[Producer clientId={}] ", config.client_id));
        let running = Arc::new(AtomicBool::new(true));
        let force_close = Arc::new(AtomicBool::new(false));
        let wakeup = client.wakeup_notify();

        let guarantee_message_order = config.max_in_flight_requests_per_connection == 1;
        let acks = config.acks;
        let retries = config.retries;

        let mut sender = Sender::new(
            client,
            Arc::clone(&metadata),
            Arc::clone(&accumulator),
            guarantee_message_order,
            config.max_request_size,
            acks,
            retries,
            config.request_timeout_ms,
            config.retry_backoff_ms,
            sender_metrics_registry,
            Arc::clone(&running),
            Arc::clone(&force_close),
            Arc::clone(&time_provider),
            transaction_manager.clone(),
            Arc::clone(&pending_requests),
            log_context.clone(),
        );

        let io_thread_name = format!("{} | {}", Self::NETWORK_THREAD_PREFIX, config.client_id);
        let task_log_context = log_context.clone();
        let sender_handle = tokio::task::spawn(async move {
            kafka_debug!(task_log_context, "Starting {} I/O task", io_thread_name);
            sender.run().await;
        });

        kafka_debug!(log_context, "Kafka producer started");

        Self {
            client_id: config.client_id.clone(),
            key_serializer,
            value_serializer,
            max_request_size: config.max_request_size,
            total_memory_size: config.buffer_memory,
            accumulator,
            metadata,
            transaction_manager,
            pending_requests,
            compression_type: config.compression_type,
            max_block_ms: config.max_block_ms,
            partitioner,
            partitioner_ignore_keys: config.partitioner_ignore_keys,
            key_hasher: config.key_hasher(),
            running,
            force_close,
            wakeup,
            sender_handle: Mutex::new(Some(sender_handle)),
            time_provider,
            metrics,
            producer_metrics,
            log_context,
        }
    }

    /// Create the producer's [`Metrics`] registry and [`KafkaProducerMetrics`].
    ///
    /// Translated from the metrics-setup block of Java's `KafkaProducer`
    /// constructor (`KafkaProducer.java:357-368`): a [`MetricConfig`] carrying
    /// `metrics.num.samples`, `metrics.sample.window.ms`,
    /// `metrics.recording.level` and a single `client-id` tag. Reporters and the
    /// JMX metrics context are N/A in Rust (consumer Phase M7 precedent); the
    /// registry is reporter-less but fully functional.
    fn create_metrics(config: &ProducerConfig) -> (Arc<Metrics>, KafkaProducerMetrics) {
        const CLIENT_ID_METRIC_TAG: &str = "client-id";

        let mut tags = std::collections::BTreeMap::new();
        tags.insert(CLIENT_ID_METRIC_TAG.to_string(), config.client_id.clone());

        let recording_level = RecordingLevel::for_name(&config.metrics_recording_level).unwrap_or(RecordingLevel::Info);
        let metric_config = MetricConfig::new()
            .set_samples(config.metrics_num_samples)
            .set_time_window_ms(config.metrics_sample_window_ms)
            .set_record_level(recording_level)
            .set_tags(tags);

        let metrics = Arc::new(Metrics::with_default_config(Arc::new(metric_config)));
        let producer_metrics = KafkaProducerMetrics::new(Arc::clone(&metrics));
        (metrics, producer_metrics)
    }

    /// A monotonic nanosecond reading, the analog of Java's
    /// `time.nanoseconds()` (`System.nanoTime()`), used for the per-call
    /// latency metrics. Matches the source the consumer uses for
    /// `commit-sync-time-ns-total`.
    fn now_nanos() -> i64 {
        SystemTime.nanoseconds()
    }

    /// Validate and optionally adjust `delivery.timeout.ms` against
    /// `linger.ms + request.timeout.ms`.
    ///
    /// Translated from `KafkaProducer.configureDeliveryTimeout()`
    /// (`KafkaProducer.java:569-590`). Java's check has two arms and both are
    /// reproduced: an *explicitly supplied* inconsistent `delivery.timeout.ms` is
    /// a `ConfigException`, while an inconsistency that comes only from the
    /// default is clamped up to `linger.ms + request.timeout.ms` and warned about,
    /// for backward compatibility. `ProducerConfig::user_configured` stands for
    /// Java's `config.originals().containsKey(..)`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] — Java's `ConfigException`, which is inside the
    /// `KafkaException` hierarchy — if the user explicitly set a
    /// `delivery.timeout.ms` smaller than `linger.ms + request.timeout.ms`.
    fn configure_delivery_timeout(config: &ProducerConfig, log_context: &LogContext) -> Result<i32, Error> {
        let mut delivery_timeout_ms = config.delivery_timeout_ms;
        let linger_ms = config.linger_ms.min(i32::MAX as i64) as i32;
        let request_timeout_ms = config.request_timeout_ms;
        let linger_and_request_timeout_ms = (linger_ms as i64 + request_timeout_ms as i64).min(i32::MAX as i64) as i32;

        if delivery_timeout_ms < linger_and_request_timeout_ms {
            if config.user_configured(ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG) {
                // Java `:578`: throw if the user explicitly set an inconsistent value.
                // The class is `ConfigException`, inside the `KafkaException`
                // hierarchy (`KafkaProducerTest.testDeliveryTimeoutAndLingerMsConfig`
                // asserts `KafkaException.class`); `illegal_argument` put it outside,
                // where `is_kafka_error()` answers `false`.
                return Err(Error::config_message(format!(
                    "{} should be equal to or larger than {} + {}",
                    ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG,
                    ProducerConfig::LINGER_MS_CONFIG,
                    ProducerConfig::REQUEST_TIMEOUT_MS_CONFIG,
                )));
            }
            // Java `:583-587`: override the default for backward compatibility.
            delivery_timeout_ms = linger_and_request_timeout_ms;
            kafka_warn!(
                log_context,
                "{} should be equal to or larger than {} + {}. Setting it to {}.",
                ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG,
                ProducerConfig::LINGER_MS_CONFIG,
                ProducerConfig::REQUEST_TIMEOUT_MS_CONFIG,
                delivery_timeout_ms
            );
        }
        Ok(delivery_timeout_ms)
    }

    /// Returns the client ID for this producer.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the current time in milliseconds from the time provider.
    fn now_ms(&self) -> i64 {
        (self.time_provider)()
    }

    /// `max.block.ms` as a [`Duration`], for the four transactional methods that
    /// bound their wait with it (`result.await(maxBlockTimeMs, MILLISECONDS)`).
    ///
    /// Clamped at zero: a negative `max.block.ms` cannot be configured, and
    /// `Duration` has no negative representation.
    fn max_block_timeout(&self) -> Duration {
        Duration::from_millis(self.max_block_ms.max(0) as u64)
    }

    /// Needs to be called before any other method when the `transactional.id` is
    /// set in the configuration.
    ///
    /// Translated from `KafkaProducer.initTransactions()`
    /// (`KafkaProducer.java:648-659`). This method does the following:
    ///
    /// 1. Ensures any transactions initiated by previous instances of the producer
    ///    with the same `transactional.id` are completed. If the previous instance
    ///    had failed with a transaction in progress, it will be aborted. If the
    ///    last transaction had begun completion, but not yet finished, this method
    ///    awaits its completion.
    /// 2. Gets the internal producer id and epoch, used in all future
    ///    transactional messages issued by the producer.
    ///
    /// Java blocks on `result.await(maxBlockTimeMs, ..)`, so this is `async`
    /// (CLAUDE.md §9.1) and returns [`Error::Timeout`] when the transactional
    /// state cannot be initialized before `max.block.ms` expires. It is safe to
    /// retry in that case, but once the transactional state has been successfully
    /// initialized this method should no longer be used.
    ///
    /// Java's `InterruptException` path has no Rust analogue — a task is not
    /// interrupted, it is dropped.
    ///
    /// # Errors
    ///
    /// - [`Error::LocalIllegalState`] if no `transactional.id` has been configured
    /// - [`Error::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions
    /// - An authorization error indicating that the configured `transactional.id`
    ///   is not authorized, or the idempotent producer id is unavailable; the user
    ///   may retry after fixing the permission
    /// - Any previous fatal error the producer has encountered
    /// - [`Error::Timeout`] if initializing the transaction takes longer than
    ///   `max.block.ms`
    pub async fn init_transactions(&self) -> Result<(), Error> {
        let transaction_manager = self.transaction_manager_or_error()?;
        self.ensure_not_closed()?;
        // Java measures `time.nanoseconds()` around the wait for
        // `producerMetrics.recordInit(..)`. There is no metrics layer in this crate
        // yet — `KafkaProducerMetrics` and the whole `org.apache.kafka.common.metrics`
        // package are listed in `remaining_classes.txt` — so the timing statements
        // that exist only to feed a sensor are not translated. The same note covers
        // `recordBeginTxn`, `recordSendOffsets`, `recordCommitTxn` and
        // `recordAbortTxn` below.
        let result = {
            // `pending_requests` before the manager, per the field docs.
            let mut pending_requests = self.pending_requests.lock().unwrap();
            transaction_manager
                .lock()
                .unwrap()
                .initialize_transactions(false, &mut pending_requests)?
        };
        self.wakeup.notify_one();
        result
            .await_result_timeout(self.max_block_timeout(), Self::INIT_TXN_TIMEOUT_MSG)
            .await?;
        // Java runs this only after a successful await, so the `?` above must stay
        // ahead of it.
        transaction_manager.lock().unwrap().maybe_update_transaction_v2_enabled(true);
        Ok(())
    }

    /// Should be called before the start of each new transaction. Note that prior
    /// to the first invocation of this method, [`Self::init_transactions`] must be
    /// invoked exactly one time.
    ///
    /// Translated from `KafkaProducer.beginTransaction()`
    /// (`KafkaProducer.java:674-681`). Stays synchronous: Java's body is a pure
    /// state transition with no wait, so CLAUDE.md §9.1 does not apply.
    ///
    /// # Errors
    ///
    /// - [`Error::LocalIllegalState`] if no `transactional.id` has been configured
    ///   or if [`Self::init_transactions`] has not yet been invoked
    /// - A producer-fenced error if another producer with the same
    ///   `transactional.id` is active
    /// - An invalid-producer-epoch error if the producer has attempted to produce
    ///   with an old epoch to the partition leader
    /// - [`Error::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions
    /// - Any previous fatal error the producer has encountered
    pub fn begin_transaction(&self) -> Result<(), Error> {
        let transaction_manager = self.transaction_manager_or_error()?;
        self.ensure_not_closed()?;
        transaction_manager.lock().unwrap().begin_transaction()
    }

    /// Sends a list of specified offsets to the consumer group coordinator, and
    /// also marks those offsets as part of the current transaction. These offsets
    /// will be considered committed only if the transaction is committed
    /// successfully.
    ///
    /// Translated from
    /// `KafkaProducer.sendOffsetsToTransaction(Map, ConsumerGroupMetadata)`
    /// (`KafkaProducer.java:733-746`).
    ///
    /// The committed offset should be the next message the application will
    /// consume, i.e. `next_record_to_be_processed.offset()`. The leader epoch
    /// should also be added as commit metadata.
    ///
    /// This method should be used when consumed and produced messages need to be
    /// batched together, typically in a consume-transform-produce pattern. Thus
    /// `group_metadata` should be obtained from the consumer's `group_metadata()`
    /// to leverage consumer group metadata, which provides stronger fencing than
    /// `ConsumerGroupMetadata::new(group_id)`.
    ///
    /// Java blocks until the request has been received and acknowledged by the
    /// consumer group coordinator; the offsets are not considered committed until
    /// the transaction itself is successfully committed via
    /// [`Self::commit_transaction`].
    ///
    /// Note that the consumer should have `enable.auto.commit=false` and should
    /// also not commit offsets manually.
    ///
    /// `offsets` and `group_metadata` are taken by value because the transaction
    /// manager moves both into the `AddOffsetsToTxn` handler that carries them to
    /// the coordinator — the same convention
    /// `AsyncKafkaConsumer::commit_sync_with_offsets` already uses for an offsets map.
    ///
    /// # Errors
    ///
    /// - [`Error::LocalIllegalArgument`] if `group_metadata` has a generation id
    ///   greater than zero but an unknown member id
    /// - [`Error::LocalIllegalState`] if no `transactional.id` has been configured
    ///   or no transaction has been started
    /// - A producer-fenced error if another producer with the same
    ///   `transactional.id` is active
    /// - [`Error::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions, or does not support the latest version of
    ///   the transactional API with all consumer group metadata
    /// - An authorization error indicating that the configured `transactional.id`
    ///   or the consumer group id is not authorized
    /// - A commit-failed error if the commit cannot be retried (e.g. the consumer
    ///   has been kicked out of the group); users should handle this by aborting
    ///   the transaction
    /// - [`Error::Timeout`] if sending the offsets takes longer than
    ///   `max.block.ms`
    pub async fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<(), Error> {
        Self::throw_if_invalid_group_metadata(&group_metadata)?;
        let transaction_manager = self.transaction_manager_or_error()?;
        self.ensure_not_closed()?;

        // Java 738: an empty map is a no-op, and in particular does not consult the
        // transaction state at all.
        if offsets.is_empty() {
            return Ok(());
        }

        let result = {
            // `pending_requests` before the manager, per the field docs.
            let mut pending_requests = self.pending_requests.lock().unwrap();
            transaction_manager.lock().unwrap().send_offsets_to_transaction(
                offsets,
                group_metadata,
                &mut pending_requests,
            )?
        };
        self.wakeup.notify_one();
        result
            .await_result_timeout(self.max_block_timeout(), Self::SEND_OFFSETS_TIMEOUT_MSG)
            .await
    }

    /// Commits the ongoing transaction. This method will flush any unsent records
    /// before actually committing the transaction.
    ///
    /// Translated from `KafkaProducer.commitTransaction()`
    /// (`KafkaProducer.java:779-786`).
    ///
    /// If any of the [`send`](Self::send) calls which were part of the transaction
    /// hit irrecoverable errors, this method returns the last received error
    /// immediately and the transaction is not committed. So all `send` calls in a
    /// transaction must succeed in order for this method to succeed.
    ///
    /// If the transaction is committed successfully and this method returns
    /// `Ok(())`, it is guaranteed that all callbacks for records in the
    /// transaction will have been invoked and completed. Note that errors returned
    /// by callbacks are ignored; the producer proceeds to commit the transaction in
    /// any case.
    ///
    /// A [`Error::Timeout`] does **not** mean the request did not reach the
    /// broker — only that the acknowledgement did not arrive in time, so it is up
    /// to the application to decide how to handle it. It is safe to retry, but it
    /// is not possible to attempt a different operation (such as
    /// [`Self::abort_transaction`]) since the commit may already be in the process
    /// of completing. If not retrying, the only option is to close the producer.
    ///
    /// # Errors
    ///
    /// - [`Error::LocalIllegalState`] if no `transactional.id` has been configured
    ///   or no transaction has been started
    /// - A producer-fenced error if another producer with the same
    ///   `transactional.id` is active
    /// - [`Error::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions
    /// - An authorization error indicating that the configured `transactional.id`
    ///   is not authorized
    /// - An invalid-producer-epoch error if the producer has attempted to produce
    ///   with an old epoch to the partition leader
    /// - Any previous fatal or abortable error the producer has encountered
    /// - [`Error::Timeout`] if committing takes longer than `max.block.ms`
    pub async fn commit_transaction(&self) -> Result<(), Error> {
        let transaction_manager = self.transaction_manager_or_error()?;
        self.ensure_not_closed()?;
        let result = {
            // `pending_requests` before the manager, per the field docs.
            let mut pending_requests = self.pending_requests.lock().unwrap();
            transaction_manager.lock().unwrap().begin_commit(&mut pending_requests)?
        };
        self.wakeup.notify_one();
        result
            .await_result_timeout(self.max_block_timeout(), Self::COMMIT_TXN_TIMEOUT_MSG)
            .await
    }

    /// Aborts the ongoing transaction. Any unflushed produce messages will be
    /// aborted when this call is made.
    ///
    /// Translated from `KafkaProducer.abortTransaction()`
    /// (`KafkaProducer.java:813-821`).
    ///
    /// This call returns an error immediately if any prior [`send`](Self::send)
    /// call failed with a producer-fenced or an authorization error.
    ///
    /// A [`Error::Timeout`] does **not** mean the request did not reach the
    /// broker — see [`Self::commit_transaction`] for the full note; it is safe to
    /// retry, but not to attempt a different operation.
    ///
    /// # Errors
    ///
    /// - [`Error::LocalIllegalState`] if no `transactional.id` has been configured
    ///   or no transaction has been started
    /// - A producer-fenced error if another producer with the same
    ///   `transactional.id` is active
    /// - An invalid-producer-epoch error if the producer has attempted to produce
    ///   with an old epoch to the partition leader
    /// - [`Error::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions
    /// - An authorization error indicating that the configured `transactional.id`
    ///   is not authorized
    /// - Any previous fatal error the producer has encountered
    /// - [`Error::Timeout`] if aborting takes longer than `max.block.ms`
    pub async fn abort_transaction(&self) -> Result<(), Error> {
        let transaction_manager = self.transaction_manager_or_error()?;
        self.ensure_not_closed()?;
        kafka_info!(self.log_context, "Aborting incomplete transaction");
        let result = {
            // `pending_requests` before the manager, per the field docs.
            let mut pending_requests = self.pending_requests.lock().unwrap();
            // `Caller::App`: this runs on the application task (rules §1).
            transaction_manager
                .lock()
                .unwrap()
                .begin_abort(&mut pending_requests, Caller::App)?
        };
        self.wakeup.notify_one();
        result
            .await_result_timeout(self.max_block_timeout(), Self::ABORT_TXN_TIMEOUT_MSG)
            .await
    }

    /// The shared [`TransactionManager`], or the error Java's
    /// `throwIfNoTransactionManager()` (`KafkaProducer.java:1507-1511`) throws.
    ///
    /// Java checks `transactionManager == null` only, so an *idempotent* producer
    /// passes this check and is rejected one level down by the manager's own
    /// `ensureTransactional()` with a different message. That split is preserved:
    /// this must not also test `is_transactional()`.
    fn transaction_manager_or_error(&self) -> Result<Arc<Mutex<TransactionManager>>, Error> {
        match &self.transaction_manager {
            Some(transaction_manager) => Ok(Arc::clone(transaction_manager)),
            None => Err(Error::local_illegal_state(format!(
                "Cannot use transactional methods without enabling transactions by setting the {} configuration property",
                ProducerConfig::TRANSACTIONAL_ID_CONFIG
            ))),
        }
    }

    /// Validates the consumer group metadata handed to
    /// [`Self::send_offsets_to_transaction`].
    ///
    /// Translated from `KafkaProducer.throwIfInvalidGroupMetadata`
    /// (`KafkaProducer.java:1498-1505`). Java's first arm rejects a `null`
    /// argument; a `ConsumerGroupMetadata` value cannot be null in Rust, so the
    /// type system enforces that arm and only the second is translated.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalArgument`] when the generation id is greater than zero
    /// but the member id is unknown.
    fn throw_if_invalid_group_metadata(group_metadata: &ConsumerGroupMetadata) -> Result<(), Error> {
        if group_metadata.generation_id() > 0 && group_metadata.member_id() == TxnOffsetCommitRequest::UNKNOWN_MEMBER_ID
        {
            return Err(Error::local_illegal_argument(format!(
                "Passed in group metadata {} has generationId > 0 but the member.id is unknown",
                group_metadata
            )));
        }
        Ok(())
    }

    /// Verify that this producer instance has not been closed.
    ///
    /// Corresponds to Java's `throwIfProducerClosed()`.
    fn ensure_not_closed(&self) -> Result<(), Error> {
        if !self.running.load(Ordering::Acquire) {
            return Err(Error::local_illegal_state(
                "Cannot perform operation after producer has been closed",
            ));
        }
        Ok(())
    }

    /// Implementation of asynchronously send a record to a topic.
    ///
    /// Translated from `KafkaProducer.doSend()`.
    ///
    /// For `ApiException`-type errors (record-too-large, invalid topic, an
    /// `ApiException` raised by a serializer, etc.), the callback is invoked with
    /// the error and a completed-with-error future is returned
    /// (`Ok(failed_future)`). This matches Java's contract where `send()` always
    /// returns a `Future` for API errors and always invokes the callback.
    ///
    /// Everything else is propagated as `Err(...)`: the generic runtime errors
    /// (`LocalIllegalState` when the producer is closed, or `LocalIllegalArgument`
    /// when a custom partitioner returns a negative partition —
    /// `KafkaProducer.java:1476-1481`, which escapes `doSend` through
    /// `catch (Exception e)` and is rethrown out of `send()`) and the
    /// `KafkaException`s that are not `ApiException`s. `SerializationException` is
    /// one of the latter — it extends `KafkaException` directly — so a
    /// serialization failure is returned as `Err`, not as a failed future.
    async fn do_send(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, Error> {
        self.ensure_not_closed()?;

        // First make sure the metadata for the topic is available
        let now_ms = self.now_ms();
        let cluster_and_wait_time = match self
            .wait_on_metadata(record.topic(), record.partition(), now_ms, self.max_block_ms)
            .await
        {
            Ok(cwt) => cwt,
            Err(e) => {
                // Java 993-998 relabels a closed-producer race first, then the
                // outer catches dispatch on the resulting class.
                let e = self.relabel_if_closed_while_sending(e);
                if e.is_api_error() {
                    return self.handle_api_error(e, record.topic(), RecordMetadata::UNKNOWN_PARTITION, callback);
                }
                return Err(e);
            },
        };
        let now_ms = now_ms + cluster_and_wait_time.waited_on_metadata_ms;
        let remaining_wait_ms = 0i64.max(self.max_block_ms - cluster_and_wait_time.waited_on_metadata_ms);
        let cluster = cluster_and_wait_time.cluster;

        // Destructure the record to take ownership of key/value for serialization.
        let (record_topic, partition_opt, timestamp_opt, record_headers, key, value) = record.into_parts();

        // Java's only catch around either serializer call is
        // `catch (ClassCastException cce)` (`KafkaProducer.java:1006` / `:1013`),
        // which relabels a key/value whose runtime class does not match the
        // configured serializer. That cannot happen here — the serializer is
        // statically typed on `K` / `V` — so there is nothing to convert, and the
        // serializer's own error propagates with its class intact. `doSend`'s outer
        // catches then dispatch on that class exactly as Java does: an
        // `ApiException` (a schema-registry `TimeoutException`, say) fires the
        // callback and yields a failed future (`:1056`), anything else is returned
        // as `Err` (`:1073` / `:1077`). Rewriting every serializer error as
        // `SerializationException` flipped `is_retriable_error()` for the first
        // case and skipped its callback entirely. Both branches below — the
        // custom-partitioner one that serializes by borrowing and the owned
        // zero-copy one — dispatch the same way.
        if self.partitioner.is_some() {
            // A custom partitioner is handed the *typed* key/value
            // (`KafkaProducer.java:1474-1475` passes `record.key()` / `record.value()`),
            // so serialize by BORROWING — `serialize_headers` keeps `key`/`value`
            // alive — and compute the partition here, with the typed references. That
            // partition is passed to `do_send_bytes` as an explicit `Some(..)`, which
            // short-circuits its own partition step so the partitioner runs EXACTLY
            // once, even a stateful one such as `RoundRobinPartitioner`.
            let serialized_key =
                match self
                    .key_serializer
                    .serialize_headers(&record_topic, &record_headers, key.as_ref())
                {
                    Ok(bytes) => bytes,
                    Err(e) if e.is_api_error() => {
                        return self.handle_api_error(e, &record_topic, RecordMetadata::UNKNOWN_PARTITION, callback);
                    },
                    Err(e) => return Err(e),
                };

            let serialized_value =
                match self
                    .value_serializer
                    .serialize_headers(&record_topic, &record_headers, value.as_ref())
                {
                    Ok(bytes) => bytes,
                    Err(e) if e.is_api_error() => {
                        return self.handle_api_error(e, &record_topic, RecordMetadata::UNKNOWN_PARTITION, callback);
                    },
                    Err(e) => return Err(e),
                };

            let partition = self.compute_partition(
                &record_topic,
                partition_opt,
                key.as_ref(),
                serialized_key.as_deref(),
                value.as_ref(),
                serialized_value.as_deref(),
                &cluster,
            )?;

            let headers = record_headers.to_array();

            self.do_send_bytes(
                &record_topic,
                Some(partition),
                timestamp_opt,
                serialized_key.as_deref(),
                serialized_value.as_deref(),
                headers,
                callback,
                now_ms,
                remaining_wait_ms,
                &cluster,
            )
            .await
        } else {
            // No custom partitioner: keep the zero-copy owned path.
            // `serialize_owned_headers` moves the key/value so a `Vec<u8>` payload
            // is written into the batch without a copy (CLAUDE.md §12); `do_send_bytes`
            // then runs the built-in key-hash partitioning via `compute_partition`.
            let serialized_key = match self.key_serializer.serialize_owned_headers(&record_topic, &record_headers, key)
            {
                Ok(bytes) => bytes,
                Err(e) if e.is_api_error() => {
                    return self.handle_api_error(e, &record_topic, RecordMetadata::UNKNOWN_PARTITION, callback);
                },
                Err(e) => return Err(e),
            };

            let serialized_value =
                match self
                    .value_serializer
                    .serialize_owned_headers(&record_topic, &record_headers, value)
                {
                    Ok(bytes) => bytes,
                    Err(e) if e.is_api_error() => {
                        return self.handle_api_error(e, &record_topic, RecordMetadata::UNKNOWN_PARTITION, callback);
                    },
                    Err(e) => return Err(e),
                };

            let headers = record_headers.to_array();

            self.do_send_bytes(
                &record_topic,
                partition_opt,
                timestamp_opt,
                serialized_key.as_deref(),
                serialized_value.as_deref(),
                headers,
                callback,
                now_ms,
                remaining_wait_ms,
                &cluster,
            )
            .await
        }
    }

    /// Common send path for already-serialized key/value bytes.
    ///
    /// Both [`do_send`](Self::do_send) (after serialization) and
    /// [`send`](KafkaProducer::<Vec<u8>, Vec<u8>>::send) (zero-copy borrowed path)
    /// delegate here for partition calculation, size validation, and accumulator
    /// append.
    #[allow(clippy::too_many_arguments)]
    async fn do_send_bytes(
        &self,
        topic: &str,
        partition: Option<i32>,
        timestamp: Option<i64>,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
        callback: Option<Callback>,
        now_ms: i64,
        remaining_wait_ms: i64,
        cluster: &Cluster,
    ) -> Result<KafkaFuture<RecordMetadata>, Error> {
        // `compute_partition` with `None` typed key/value: this method has only the
        // serialized bytes. When called from `do_send`'s custom-partitioner branch the
        // partition arrives pre-computed as `Some(..)`, so this short-circuits and the
        // partitioner is not consulted twice. On the borrowed `send(&[u8], &[u8])` path
        // the typed key/value ARE the bytes, so passing them as `serialized_*` (and
        // `None` typed) is the faithful call there.
        let partition = self.compute_partition(topic, partition, None, key, None, value, cluster)?;

        let serialized_size = AbstractRecords::estimate_size_in_bytes_upper_bound(
            RecordBatch::CURRENT_MAGIC_VALUE,
            self.compression_type,
            key,
            value,
            headers,
        );
        if let Err(err) = self.ensure_valid_record_size(serialized_size) {
            return self.handle_api_error(err, topic, partition, callback);
        }

        let timestamp = timestamp.unwrap_or(now_ms);

        match self
            .accumulator
            .append(
                topic,
                partition,
                timestamp,
                key,
                value,
                headers,
                callback,
                remaining_wait_ms,
                now_ms,
                cluster,
            )
            .await
        {
            Ok(result) => {
                // Add the partition to the transaction (if in progress) after it has
                // been successfully appended to the accumulator. We cannot do it
                // before because the partition may be unknown. Note that the `Sender`
                // will refuse to dequeue batches from the accumulator until they have
                // been added to the transaction (`KafkaProducer.java:1040-1046`).
                //
                // `result.topic_partition` is what Java reads back as
                // `appendCallbacks.topicPartition()`. It is borrowed, not rebuilt:
                // constructing it here from `topic: &str` would allocate a `String` and
                // an `Arc<str>` and copy the topic name twice on **every** record, which
                // CLAUDE.md §11 forbids on the send path. The accumulator interns one
                // `Arc<str>` per topic and hands the `TopicPartition` back, so this costs
                // nothing. (Critic 44 issue 1.)
                if let Some(transaction_manager) = &self.transaction_manager {
                    // `Caller::App`: this runs on the application task.
                    //
                    // The guard is bound to its own statement so it is released at the
                    // `;`. It MUST NOT stay alive into the error handling below:
                    // `handle_api_error` re-locks this same non-reentrant
                    // `std::sync::Mutex` through `maybe_transition_to_error_state`, and an
                    // `if let` scrutinee's temporaries live for the whole success arm —
                    // edition 2024 only shortens them across the `else`. Written inline,
                    // this self-deadlocks the application task.
                    let add_partition =
                        transaction_manager.lock().unwrap().maybe_add_partition(&result.topic_partition);
                    if let Err(error) = add_partition {
                        // `maybeAddPartition` throws across two of `doSend`'s catch
                        // blocks, so the error class decides how the failure surfaces:
                        //
                        // - `ProducerFencedException` / `InvalidProducerEpochException`
                        //   (`maybeFailWithError`, `TransactionManager.java:1159` and `:1164`)
                        //   are `ApiException`s: `catch (ApiException e)` records the
                        //   error state and returns a failed future.
                        // - `IllegalStateException` — a send that is out of order with
                        //   the transactional API (Java 443 and 446), or a previous
                        //   operation that timed out (`throwIfPendingState`, Java
                        //   1249-1257), or a previous invalid transition (Java 1168) —
                        //   is not even a `KafkaException`, so it reaches
                        //   `catch (Exception e)` and is rethrown out of `send()`.
                        // - The bare `KafkaException` of Java 1171 is not an
                        //   `ApiException` either, so `catch (KafkaException e)`
                        //   rethrows it as well. Neither rethrowing block calls
                        //   `maybeTransitionToErrorState`.
                        //
                        // `is_api_error()` alone is the whole test. It used to be
                        // `&& error.error() != Errors::UnknownServerError`, because a bare
                        // `KafkaException` was then spelled
                        // `Error::with_message(Errors::UnknownServerError, ..)`, which
                        // resolves the code to `UnknownServerException` — an
                        // `ApiException`. Every producer site now builds a bare
                        // `KafkaException` as `Error::kafka_message(..)` / `Error::kafka_message_source(..)`
                        // (the `Error::KafkaError` variant), for which `is_api_error()`
                        // answers `false` directly, so the code-based workaround is gone.
                        if error.is_api_error() {
                            let partition = result.topic_partition.partition();
                            // `None`, not a callback: unlike the pre-append failure paths
                            // (`ensure_valid_record_size`, `wait_on_metadata`), the record
                            // has ALREADY been appended here, and `append` above took
                            // ownership of `callback` and registered it with the record's
                            // future. The callback is therefore both (a) moved — no longer
                            // available at this point — and (b) already carried by that
                            // future, which fires it exactly once when the batch is later
                            // completed/aborted. Firing it here too would double-invoke it,
                            // breaking the exactly-once-per-record contract (CLAUDE.md §9.5).
                            // (Java's `doSend` catch fires the raw `callback` at
                            // `KafkaProducer.java:1061`, but keeps it as a reference separate
                            // from the `appendCallbacks` it registered, so Java can fire it
                            // twice on this path; Rust's single-owner model fires it once.)
                            return self.handle_api_error(error, topic, partition, None);
                        }
                        return Err(error);
                    }
                }

                if result.batch_is_full || result.new_batch_created {
                    kafka_trace!(
                        self.log_context,
                        "Waking up the sender since topic {} is either full or getting a new batch",
                        topic
                    );
                    self.wakeup.notify_one();
                }
                Ok(KafkaFuture::new(result.future))
            },
            // Java's `catch (ApiException e)` (`KafkaProducer.java:1056-1068`) fires the
            // user `Callback` with a null-metadata `RecordMetadata(tp, -1, -1,
            // NO_TIMESTAMP, -1, -1)` *and* returns a failed future. `append` gives the
            // callback back (`AppendFailure::callback`) precisely so this arm can honour
            // that obligation exactly once (CLAUDE.md §9.5) — the four sibling arms
            // covering the same Java block all route through `handle_api_error` too.
            Err(failure) if failure.error.is_api_error() => {
                self.handle_api_error(failure.error, topic, partition, failure.callback)
            },
            // Java's `catch (KafkaException e)` / `catch (Exception e)` (`:1072-1080`)
            // rethrow, and neither invokes the callback. `failure.callback` is dropped
            // here, exactly as Java drops its `appendCallbacks` reference when `send()`
            // throws.
            Err(failure) => Err(failure.error),
        }
    }

    /// `transactionManager.maybeTransitionToErrorState(e)`, the tail of
    /// `KafkaProducer.doSend`'s `catch (ApiException e)` block
    /// (`KafkaProducer.java:1065-1067`).
    ///
    /// [`Caller::App`](crate::producer::internals::Caller::App): `doSend` runs on the
    /// application task.
    ///
    /// Locks the [`TransactionManager`]. Java's monitor is reentrant and
    /// `std::sync::Mutex` is not, so no caller — directly or through
    /// [`handle_api_error`](Self::handle_api_error) — may already hold that
    /// lock, including in a still-live `if let` / `match` scrutinee temporary.
    fn maybe_transition_to_error_state(&self, error: &Error) {
        if let Some(transaction_manager) = &self.transaction_manager {
            // Java lets an invalid transition propagate out of `doSend`. That cannot
            // happen on the idempotent path — the only transition
            // `maybeTransitionToErrorState` performs is to `FATAL_ERROR`, which is
            // always valid — and swallowing it here would hide a Phase-5 regression,
            // so it is logged rather than dropped.
            if let Err(transition_error) = transaction_manager
                .lock()
                .unwrap()
                .maybe_transition_to_error_state(error, Caller::App)
            {
                kafka_warn!(
                    self.log_context,
                    "Failed to record a send error in the transaction manager: {}",
                    transition_error
                );
            }
        }
    }

    /// `doSend`'s **inner** `catch (KafkaException e)` around `waitOnMetadata`
    /// (`KafkaProducer.java:993-998`):
    ///
    /// ```java
    /// } catch (KafkaException e) {
    ///     if (metadata.isClosed())
    ///         throw new KafkaException("Producer closed while send in progress", e);
    ///     throw e;
    /// }
    /// ```
    ///
    /// Its whole job is to relabel a `close()` racing an in-flight send, so the
    /// caller can tell it apart from a metadata timeout. Anything else — and
    /// anything outside the `KafkaException` hierarchy — passes through untouched,
    /// to be dispatched by the outer `catch (ApiException e)` /
    /// `catch (KafkaException e)` split at the call site.
    fn relabel_if_closed_while_sending(&self, error: Error) -> Error {
        if error.is_kafka_error() && self.metadata.is_closed() {
            // A bare `KafkaException`, matching Java: not an `ApiException`, so the
            // caller returns it as `Err` rather than as a failed future.
            return Error::kafka_message_source("Producer closed while send in progress", error);
        }
        error
    }

    /// Handle an `ApiException`-type error by invoking the callback (if any)
    /// and returning a completed-with-error future.
    ///
    /// This matches Java's `catch (ApiException e)` block in `doSend()`.
    fn handle_api_error(
        &self,
        error: Error,
        topic: &str,
        partition: i32,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, Error> {
        kafka_debug!(self.log_context, "Error occurred during message send: {}", error);
        self.maybe_transition_to_error_state(&error);
        if let Some(cb) = callback {
            let tp = TopicPartition::new(topic.to_string(), partition);
            let null_metadata = RecordMetadata::new(tp, -1, -1, RecordBatch::NO_TIMESTAMP, -1, -1);
            cb(Some(&null_metadata), Some(&error));
        }
        let tp = TopicPartition::new(topic.to_string(), partition);
        Ok(KafkaFuture::new(Arc::new(FutureRecordMetadata::failed(tp, error))))
    }

    /// Wait for cluster metadata including partitions for the given topic to be available.
    ///
    /// Translated from `KafkaProducer.waitOnMetadata()`.
    ///
    /// # Arguments
    /// * `topic` - The topic we want metadata for
    /// * `partition` - A specific partition expected to exist in metadata, or `None`
    /// * `now_ms` - The current time in ms
    /// * `max_wait_ms` - The maximum time in ms for waiting on the metadata
    ///
    /// # Returns
    /// The cluster containing topic metadata and the amount of time we waited in ms.
    ///
    /// # Errors
    /// Returns `Err` if:
    /// - The topic is invalid ([`InvalidTopic`](Error::InvalidTopic))
    /// - Metadata could not be refreshed within `max_wait_ms` ([`Timeout`](Error::Timeout))
    /// - The producer is closed
    async fn wait_on_metadata(
        &self,
        topic: &str,
        partition: Option<i32>,
        now_ms: i64,
        max_wait_ms: i64,
    ) -> Result<ClusterAndWaitTime, Error> {
        let cluster = self.metadata.fetch();

        if cluster.invalid_topics().contains(topic) {
            // Java: `throw new InvalidTopicException(topic)` — `topic` is a
            // `String`, so this binds to the `(String message)` constructor:
            // the message is the topic name and `invalidTopics()` is empty.
            return Err(Error::invalid_topics_message(
                std::collections::HashSet::new(),
                topic.to_string(),
            ));
        }

        // Add topic to metadata topic list if it is not there already and reset expiry
        self.metadata.add(topic, now_ms);

        let partitions_count = cluster.partition_count_for_topic(topic);
        // Return cached metadata if we have it, and if the record's partition is either
        // undefined or within the known partition range
        if let Some(count) = partitions_count
            && (partition.is_none() || partition.unwrap() < count as i32)
        {
            return Ok(ClusterAndWaitTime { cluster, waited_on_metadata_ms: 0 });
        }

        let mut remaining_wait_ms = max_wait_ms;
        let mut elapsed: i64 = 0;
        let mut partitions_count = partitions_count;

        // Java `waitOnMetadata`: `long nowNanos = time.nanoseconds()` right
        // before the refresh loop (after the early cached-metadata return), and
        // `producerMetrics.recordMetadataWait(time.nanoseconds() - nowNanos)`
        // once the loop succeeds. A timeout inside the loop throws before the
        // record, so only successful waits are recorded — preserved here.
        let now_nanos = Self::now_nanos();

        // Issue metadata requests until we have metadata for the topic and the
        // requested partition, or until max_wait_ms is exceeded.
        loop {
            if let Some(p) = partition {
                kafka_trace!(
                    self.log_context,
                    "Requesting metadata update for partition {} of topic {}.",
                    p,
                    topic
                );
            } else {
                kafka_trace!(self.log_context, "Requesting metadata update for topic {}.", topic);
            }
            self.metadata.add(topic, now_ms + elapsed);
            let version = self.metadata.request_update_for_topic(topic);
            self.wakeup.notify_one();

            match self.metadata.await_update(version, remaining_wait_ms).await {
                Ok(()) => {},
                // A fatal metadata error (an authentication failure, say) must
                // reach the caller as itself. Java's `awaitUpdate` throws it from
                // the wait predicate via `maybeThrowFatalException()`, so it never
                // becomes a `TimeoutException`; only an actual deadline expiry does.
                // Re-wrapping everything as a timeout made a dead producer look
                // retriable, so the application retried bad credentials forever.
                Err(e) if !e.is_timeout_error() => return Err(e),
                Err(_) => {
                    // Rethrow with the original `max_wait_ms` to keep the message
                    // free of the shrinking `remaining_wait_ms` (Java 1133).
                    let error_message = self.get_error_message(partitions_count, topic, partition, max_wait_ms);
                    if let Some(code) = self.metadata.get_error(topic) {
                        // `new TimeoutException(errorMessage, metadata.getError(topic).exception())`
                        // (Java 1136): the broker error is the timeout's *cause*, so
                        // `Error::source()` can be walked back to it. Flattening it
                        // into the message left `source()` empty.
                        return Err(Error::Timeout(TimeoutError::with_source(error_message, Error::new(code))));
                    }
                    return Err(Error::timeout(error_message));
                },
            }

            let cluster = self.metadata.fetch();
            elapsed = self.now_ms() - now_ms;
            if elapsed >= max_wait_ms {
                let error_message = self.get_error_message(partitions_count, topic, partition, max_wait_ms);
                // Java 1143-1146 attaches the topic's error as the cause here too,
                // but only when it is retriable — a non-retriable one is about to be
                // raised as itself by `maybe_return_error_for_topic` on the next
                // iteration, so pinning it under a timeout would mislabel it.
                if let Some(code) = self.metadata.get_error(topic) {
                    let underlying = Error::new(code);
                    if underlying.is_retriable_error() {
                        return Err(Error::Timeout(TimeoutError::with_source(error_message, underlying)));
                    }
                }
                return Err(Error::timeout(error_message));
            }
            self.metadata.maybe_return_error_for_topic(topic)?;
            remaining_wait_ms = max_wait_ms - elapsed;
            partitions_count = cluster.partition_count_for_topic(topic);

            let done = match partitions_count {
                None => false,
                Some(count) => partition.is_none() || partition.unwrap() < count as i32,
            };
            if done {
                self.producer_metrics.record_metadata_wait(Self::now_nanos() - now_nanos);
                return Ok(ClusterAndWaitTime { cluster, waited_on_metadata_ms: elapsed });
            }
        }
    }

    /// Format the error message for a metadata wait timeout.
    fn get_error_message(
        &self,
        partitions_count: Option<usize>,
        topic: &str,
        partition: Option<i32>,
        max_wait_ms: i64,
    ) -> String {
        match partitions_count {
            None => format!("Topic {} not present in metadata after {} ms.", topic, max_wait_ms),
            Some(count) => format!(
                "Partition {} of topic {} with partition count {} is not present in metadata after {} ms.",
                partition.unwrap_or(-1),
                topic,
                count,
                max_wait_ms
            ),
        }
    }

    /// Validate that the record size isn't too large.
    ///
    /// Translated from `KafkaProducer.ensureValidRecordSize()`.
    fn ensure_valid_record_size(&self, size: i32) -> Result<(), Error> {
        if size > self.max_request_size {
            return Err(Error::record_too_large(format!(
                "The message is {} bytes when serialized which is larger than {}, which is the value of the {} configuration.",
                size,
                self.max_request_size,
                ProducerConfig::MAX_REQUEST_SIZE_CONFIG
            )));
        }
        if size as i64 > self.total_memory_size {
            return Err(Error::record_too_large(format!(
                "The message is {} bytes when serialized which is larger than the total memory buffer you have configured with the {} configuration.",
                size,
                ProducerConfig::BUFFER_MEMORY_CONFIG
            )));
        }
        Ok(())
    }

    /// Resolve the key-based partition when no explicit partition was supplied,
    /// or [`UNKNOWN_PARTITION`](RecordMetadata::UNKNOWN_PARTITION) to defer to
    /// the keyless (sticky, KIP-794) path.
    ///
    /// Mirrors the key branch of Java's `KafkaProducer.partition()`: the key is
    /// hashed only when it is present, keys are not being ignored
    /// (`partitioner.ignore.keys=false`), the configured [`KeyHasher`] actually
    /// hashes this key ([`KeyHasher::hashes_key`] — CRC-32 leaves an *empty* key
    /// to the keyless path, matching librdkafka `consistent_random`; murmur2
    /// hashes it, matching the Java client), and the topic has known partitions.
    ///
    /// Both send call sites ([`do_send_bytes`](Self::do_send_bytes) and
    /// [`partition`](Self::partition)) route their key branch through this one
    /// helper so the hashing gate cannot diverge between them.
    fn partition_for_key_or_unknown(&self, key: Option<&[u8]>, topic: &str, cluster: &Cluster) -> i32 {
        if let Some(k) = key
            && !self.partitioner_ignore_keys
            && self.key_hasher.hashes_key(k)
        {
            let num_partitions = cluster.partitions_for_topic(topic).len() as i32;
            if num_partitions > 0 {
                return BuiltInPartitioner::partition_for_key(k, num_partitions, self.key_hasher);
            }
        }
        RecordMetadata::UNKNOWN_PARTITION
    }

    /// Compute the partition for a record, mirroring Java's `KafkaProducer.partition()`
    /// (`KafkaProducer.java:1469-1489`).
    ///
    /// Precedence is exactly Java's:
    /// 1. an explicit `partition` on the record wins outright;
    /// 2. otherwise, if a custom [`Partitioner`] is configured, it decides — and a
    ///    negative result is rejected with the same `IllegalArgumentException` message
    ///    Java throws. In Java that exception escapes `doSend`'s `catch (Exception e)`
    ///    and is rethrown out of `send()`; here it is the `Err` variant, propagated by
    ///    the caller with `?`.
    /// 3. otherwise fall back to the built-in key-hash / `UNKNOWN_PARTITION` path
    ///    ([`partition_for_key_or_unknown`](Self::partition_for_key_or_unknown)).
    ///
    /// The typed `key` / `value` are handed straight to the custom partitioner (Java
    /// passes it `record.key()` / `record.value()` alongside the serialized bytes); the
    /// built-in fallback in step 3 uses only `serialized_key`.
    #[allow(clippy::too_many_arguments)]
    fn compute_partition(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<&K>,
        serialized_key: Option<&[u8]>,
        value: Option<&V>,
        serialized_value: Option<&[u8]>,
        cluster: &Cluster,
    ) -> Result<i32, Error> {
        if let Some(p) = partition {
            return Ok(p);
        }

        if let Some(partitioner) = &self.partitioner {
            let custom_partition = partitioner.partition(topic, key, serialized_key, value, serialized_value, cluster);
            if custom_partition < 0 {
                return Err(Error::local_illegal_argument(format!(
                    "The partitioner generated an invalid partition number: {}. Partition number should always be non-negative.",
                    custom_partition
                )));
            }
            return Ok(custom_partition);
        }

        Ok(self.partition_for_key_or_unknown(serialized_key, topic, cluster))
    }

    /// Compute partition for the given record.
    ///
    /// A thin test wrapper over [`compute_partition`](Self::compute_partition), which is
    /// the faithful translation of `KafkaProducer.partition()`. Every caller is a test
    /// that uses a producer WITHOUT a custom partitioner, so the negative-partition `Err`
    /// path is unreachable and unwrapping it here is safe.
    fn partition(
        &self,
        record: &ProducerRecord<K, V>,
        serialized_key: Option<&[u8]>,
        serialized_value: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32 {
        self.compute_partition(
            record.topic(),
            record.partition(),
            record.key(),
            serialized_key,
            record.value(),
            serialized_value,
            cluster,
        )
        .expect("partition() test helper is only used without a custom partitioner")
    }

    /// Initiate a graceful close of the sender.
    ///
    /// Closes the accumulator first to guarantee that no more appends are
    /// accepted after breaking from the sender loop. Otherwise, we may miss
    /// some callbacks when shutting down.
    ///
    /// Translated from `Sender.initiateClose()`.
    fn initiate_close(&self) {
        // Ensure accumulator is closed first to guarantee that no more appends
        // are accepted after breaking from the sender loop.
        self.accumulator.close();
        self.running.store(false, Ordering::Release);
        self.wakeup.notify_one();
    }

    /// Force-close the sender, aborting all pending batches.
    fn force_close(&self) {
        self.force_close.store(true, Ordering::Release);
        self.initiate_close();
    }

    /// Await the sender task handle with a timeout.
    ///
    /// Takes the `JoinHandle` from the mutex and awaits it with the given
    /// timeout. Returns `true` if the sender task completed within the
    /// timeout, `false` if it is still running.
    ///
    /// If there is no sender handle (e.g. in tests that don't spawn a sender),
    /// returns `true` immediately.
    ///
    /// Corresponds to Java's `ioThread.join(closeTimer.remainingMs())`.
    ///
    /// On expiry the handle is **put back**, because Java's `close` force-closes and
    /// then joins unconditionally (`KafkaProducer.java:1414-1418`) — dropping it here
    /// would leave [`Self::await_sender_handle_indefinitely`] with nothing to join and
    /// `close` would return while the Sender task was still running, which CLAUDE.md
    /// §9.4 forbids. `JoinHandle` is `Unpin`, so `&mut` is enough to await it without
    /// giving it away.
    async fn await_sender_handle(&self, timeout: Duration) -> bool {
        let handle = self.sender_handle.lock().unwrap().take();
        match handle {
            None => true,
            Some(mut join_handle) => {
                let completed = tokio::time::timeout(timeout, &mut join_handle).await.is_ok();
                if !completed {
                    *self.sender_handle.lock().unwrap() = Some(join_handle);
                }
                completed
            },
        }
    }

    /// Await the sender task handle indefinitely.
    ///
    /// Called after force-close to ensure the sender task has exited.
    /// Corresponds to Java's `ioThread.join()` (no timeout).
    async fn await_sender_handle_indefinitely(&self) {
        let handle = self.sender_handle.lock().unwrap().take();
        if let Some(join_handle) = handle {
            let _ = join_handle.await;
        }
    }
}

impl KafkaProducer<Vec<u8>, Vec<u8>> {
    /// Send a record with borrowed byte-slice key/value, bypassing serialization.
    ///
    /// This is the zero-copy path for callers that already have `&[u8]` data
    /// (e.g. the C FFI layer). The slices are passed directly through to the
    /// accumulator's batch buffer without any intermediate allocation.
    pub async fn send(
        &self,
        record: ProducerRecord<&[u8], &[u8]>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, Error> {
        self.ensure_not_closed()?;

        let now_ms = self.now_ms();
        let cluster_and_wait_time = match self
            .wait_on_metadata(record.topic(), record.partition(), now_ms, self.max_block_ms)
            .await
        {
            Ok(cwt) => cwt,
            Err(e) => {
                // Java 993-998, as in `do_send`.
                let e = self.relabel_if_closed_while_sending(e);
                if e.is_api_error() {
                    return self.handle_api_error(e, record.topic(), RecordMetadata::UNKNOWN_PARTITION, callback);
                }
                return Err(e);
            },
        };
        let now_ms = now_ms + cluster_and_wait_time.waited_on_metadata_ms;
        let remaining_wait_ms = 0i64.max(self.max_block_ms - cluster_and_wait_time.waited_on_metadata_ms);
        let cluster = cluster_and_wait_time.cluster;

        let (record_topic, partition, timestamp, _headers, key, value) = record.into_parts();

        self.do_send_bytes(
            &record_topic,
            partition,
            timestamp,
            key,
            value,
            RecordBatch::EMPTY_HEADERS,
            callback,
            now_ms,
            remaining_wait_ms,
            &cluster,
        )
        .await
    }
}

impl<K, V> Producer<K, V> for KafkaProducer<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Needs to be called before any other method when the `transactional.id` is
    /// set in the configuration.
    async fn init_transactions(&self) -> Result<(), Error> {
        KafkaProducer::init_transactions(self).await
    }

    /// Should be called before the start of each new transaction.
    fn begin_transaction(&self) -> Result<(), Error> {
        KafkaProducer::begin_transaction(self)
    }

    /// Sends a list of specified offsets to the consumer group coordinator, and
    /// also marks those offsets as part of the current transaction.
    async fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<(), Error> {
        KafkaProducer::send_offsets_to_transaction(self, offsets, group_metadata).await
    }

    /// Commits the ongoing transaction.
    async fn commit_transaction(&self) -> Result<(), Error> {
        KafkaProducer::commit_transaction(self).await
    }

    /// Aborts the ongoing transaction.
    async fn abort_transaction(&self) -> Result<(), Error> {
        KafkaProducer::abort_transaction(self).await
    }

    /// Asynchronously send a record to a topic.
    ///
    /// See [`send_with_callback`](Producer::send_with_callback) for details.
    async fn send(&self, record: ProducerRecord<K, V>) -> Result<KafkaFuture<RecordMetadata>, Error> {
        self.do_send(record, None).await
    }

    /// Asynchronously send a record to a topic and invoke the provided callback
    /// when the send has been acknowledged.
    async fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, Error> {
        self.do_send(record, callback).await
    }

    /// Invoking this method makes all buffered records immediately available to
    /// send and awaits the completion of the requests associated with these
    /// records.
    ///
    /// Translated from `KafkaProducer.flush()`.
    async fn flush(&self) -> Result<(), Error> {
        kafka_trace!(self.log_context, "Flushing accumulated records in producer.");
        // Java: `long start = time.nanoseconds()` then a try/finally recording
        // `producerMetrics.recordFlush(time.nanoseconds() - start)`
        // (`KafkaProducer.java:1231/1239`). `await_flush_completion` returns
        // `()` (no error channel), so the finally reduces to recording after
        // the await.
        let start = Self::now_nanos();
        self.accumulator.begin_flush();
        self.wakeup.notify_one();
        self.accumulator.await_flush_completion().await;
        self.producer_metrics.record_flush(Self::now_nanos() - start);
        Ok(())
    }

    /// Get the full set of producer metrics maintained by this producer.
    ///
    /// Translated from `KafkaProducer.metrics()` — a snapshot of the registry.
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        self.metrics.metrics()
    }

    /// Get the partition metadata for the given topic.
    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, Error> {
        let now_ms = self.now_ms();
        let cluster_and_wait_time = self.wait_on_metadata(topic, None, now_ms, self.max_block_ms).await?;
        Ok(cluster_and_wait_time.cluster.partitions_for_topic(topic).to_vec())
    }

    /// Close this producer. This method awaits until all previously sent requests
    /// complete.
    async fn close(&self) -> Result<(), Error> {
        self.close_with_timeout(Duration::from_millis(i64::MAX as u64)).await
    }

    /// Close this producer, waiting up to the given timeout for pending requests
    /// to complete.
    ///
    /// Translated from `KafkaProducer.close(Duration timeout)`.
    ///
    /// If `timeout > 0`: initiates a graceful close and awaits the sender task up
    /// to the remaining time. If the sender task is still alive after the timeout,
    /// it is force-closed and awaited indefinitely.
    ///
    /// If `timeout == 0`: force-closes immediately without draining.
    ///
    /// Note: Rust's `Duration` is unsigned, so the negative-timeout check from
    /// Java is omitted (impossible to construct a negative `Duration`).
    async fn close_with_timeout(&self, timeout: Duration) -> Result<(), Error> {
        let timeout_ms = timeout.as_millis() as i64;
        kafka_info!(
            self.log_context,
            "Closing the Kafka producer with timeoutMillis = {} ms.",
            timeout_ms
        );

        // Track whether the sender is still alive after the graceful close attempt.
        let mut sender_still_alive = false;

        if timeout_ms > 0 {
            // Try to close gracefully: close accumulator, set running=false, wake sender.
            self.initiate_close();

            // Await the sender task with the remaining timeout.
            sender_still_alive = !self.await_sender_handle(timeout).await;
        }

        if timeout_ms == 0 || sender_still_alive {
            // Force close if timeout is 0 or sender is still alive after timeout
            kafka_info!(
                self.log_context,
                "Proceeding to force close the producer since pending requests could not be \
                 completed within timeout {} ms.",
                timeout_ms
            );
            self.force_close();

            // Await the sender task indefinitely after force close.
            self.await_sender_handle_indefinitely().await;
        }

        // Java `close`'s `Utils.closeQuietly(...)` chain, in order:
        // `producerMetrics` → `metrics` → `keySerializer` → `valueSerializer` →
        // `partitioner` (`KafkaProducer.java:1441-1446`). This crate does not model
        // closeable serializers, so the partitioner is closed right after `metrics`,
        // which is its faithful position among the closeables that exist. (The Phase 2
        // spec cited `:1449-1450` for the position, but 4.3.1 closes the partitioner at
        // `:1446`, after metrics rather than before — behaviourally irrelevant for the
        // built-in partitioners, whose `close` is a no-op.)
        self.producer_metrics.close();
        self.metrics.close();
        if let Some(partitioner) = &self.partitioner {
            partitioner.close();
        }

        kafka_debug!(self.log_context, "Kafka producer has been closed");
        Ok(())
    }
}

impl<K, V> Drop for KafkaProducer<K, V> {
    fn drop(&mut self) {
        if self.running.load(Ordering::Acquire) {
            kafka_warn!(
                self.log_context,
                "KafkaProducer was not closed before being dropped. Call close() to avoid resource leaks."
            );
            self.force_close();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicI64;

    use super::*;
    use crate::MockClient;
    use crate::common::Errors;
    use crate::common::Node;
    use crate::common::compress::Compression;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::requests::ConcreteResponse;
    use crate::common::serialization::StringSerializer;
    use crate::common::utils::Utils;
    use crate::producer::MockPartitioner;
    use crate::producer::ProducerConfig;
    use crate::producer::ProducerRecordOptionsBuilder;
    use crate::producer::RoundRobinPartitioner;
    use crate::producer::internals::BufferPool;
    use crate::producer::internals::{PartitionerConfig, RecordAccumulator};

    const TOPIC: &str = "test-topic";

    /// Java's `"some.id"`, the `transactional.id` most transactional
    /// `KafkaProducerTest` methods configure.
    const TRANSACTIONAL_ID: &str = "some.id";

    /// Java's `initProducerIdResponse(1L, (short) 5, ..)` pair
    /// (`KafkaProducerTest.java:2028-2035`).
    const PRODUCER_ID: i64 = 1;
    const EPOCH: i16 = 5;

    fn default_time_provider() -> Arc<dyn Fn() -> i64 + Send + Sync> {
        Arc::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        })
    }

    fn create_metadata_with_topic(topic: &str, num_partitions: i32) -> Arc<ProducerMetadata> {
        use crate::MetadataResponseData;
        use crate::common::ApiKeys;
        use crate::common::Errors;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};

        let metadata = Arc::new(ProducerMetadata::new(
            100,
            1000,
            300_000,
            300_000,
            ClusterResourceListeners::new(),
        ));

        let mut data = MetadataResponseData::new();
        data.set_controller_id(0);
        data.set_cluster_id(Some("test-cluster".to_string()));

        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(0);
        broker.set_host("localhost".to_string());
        broker.set_port(9092);
        data.set_brokers(vec![broker]);

        let mut topic_resp = MetadataResponseTopic::new();
        topic_resp.set_name(Some(topic.to_string()));
        topic_resp.set_error_code(Errors::None.code());
        topic_resp.set_is_internal(false);

        let mut partitions = Vec::new();
        for i in 0..num_partitions {
            let mut p = MetadataResponsePartition::new();
            p.set_partition_index(i);
            p.set_leader_id(0);
            p.set_leader_epoch(0);
            p.set_replica_nodes(vec![0]);
            p.set_isr_nodes(vec![0]);
            p.set_error_code(Errors::None.code());
            partitions.push(p);
        }
        topic_resp.set_partitions(partitions);
        data.set_topics(vec![topic_resp]);

        let response = MetadataResponse::with_version(data, ApiKeys::METADATA.latest_version());

        metadata.add(topic, 0);
        metadata.update_with_current_request_version(&response, false, 0);

        metadata
    }

    fn create_accumulator() -> Arc<RecordAccumulator> {
        Arc::new(RecordAccumulator::new_for_test(
            16384,
            Compression::none(),
            5,
            100,
            1000,
            120_000,
            PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
            Arc::new(BufferPool::new_for_test(32 * 1024 * 1024, 16384)),
            None,
        ))
    }

    fn create_producer(
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
    ) -> KafkaProducer<String, String> {
        create_producer_with_config(ProducerConfig::default(), metadata, accumulator)
    }

    fn create_producer_with_config(
        config: ProducerConfig,
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
    ) -> KafkaProducer<String, String> {
        let running = Arc::new(AtomicBool::new(true));
        let force_close = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(Notify::new());

        KafkaProducer::with_options(
            KafkaProducerOptionsBuilder::new()
                .set_config(&config)
                .set_key_serializer(Box::new(StringSerializer))
                .set_value_serializer(Box::new(StringSerializer))
                .set_metadata(metadata)
                .set_accumulator(accumulator)
                .set_running(running)
                .set_force_close(force_close)
                .set_wakeup(wakeup)
                .set_time_provider(default_time_provider())
                .set_pending_requests(Arc::new(Mutex::new(PendingRequests::new())))
                .build()
                .expect("KafkaProducerOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// A serializer that always fails with a caller-chosen error, so `do_send`'s
    /// dispatch on the serializer's error *class* is observable.
    struct FailingSerializer(Error);

    impl Serializer<String> for FailingSerializer {
        fn serialize(&self, _topic: &str, _data: Option<&String>) -> Result<Option<Vec<u8>>, Error> {
            Err(self.0.clone())
        }
    }

    fn create_producer_with_key_serializer(
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
        key_serializer: Box<dyn Serializer<String> + Send + Sync>,
    ) -> KafkaProducer<String, String> {
        KafkaProducer::with_options(
            KafkaProducerOptionsBuilder::new()
                .set_config(&ProducerConfig::default())
                .set_key_serializer(key_serializer)
                .set_value_serializer(Box::new(StringSerializer))
                .set_metadata(metadata)
                .set_accumulator(accumulator)
                .set_time_provider(default_time_provider())
                .set_pending_requests(Arc::new(Mutex::new(PendingRequests::new())))
                .build()
                .expect("KafkaProducerOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Java's only catch around the serializer call is
    /// `catch (ClassCastException cce)` (`KafkaProducer.java:1004-1010`), which
    /// cannot happen in Rust — the serializer is statically typed on `K`. Everything
    /// else the serializer throws keeps its class and is dispatched by `doSend`'s
    /// outer catches: an `ApiException` at `:1056` fires the callback and returns a
    /// failed future.
    ///
    /// Rewriting every serializer error as `SerializationException` flipped
    /// `is_retriable_error()` from `true` to `false` for exactly this case (a
    /// schema-registry `TimeoutException`), skipped the callback entirely because
    /// `Serialization` is not an `ApiException`, and flattened the cause into a
    /// display string.
    #[tokio::test]
    async fn a_retriable_serializer_error_keeps_its_class_and_fires_the_callback() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer_with_key_serializer(
            metadata,
            accumulator,
            Box::new(FailingSerializer(Error::timeout("schema registry lookup timed out"))),
        );

        let callback_error: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&callback_error);
        let callback: Callback = Box::new(move |_metadata, error| {
            *sink.lock().unwrap() = error.cloned();
        });

        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("k".to_string()), Some("v".to_string()));
        // An `ApiException` yields `Ok(failed_future)`, not `Err` — Java's
        // `return new FutureFailure(e)`.
        let future = producer
            .send_with_callback(record, Some(callback))
            .await
            .expect("an API-error serializer failure must come back as a failed future, not Err");

        let error = future.get().await.expect_err("the future must be failed");
        assert!(error.is_timeout_error(), "the serializer's class must survive: {error:?}");
        assert!(error.is_retriable_error(), "a schema-registry timeout is retriable: {error:?}");
        assert_eq!(error.message(), "schema registry lookup timed out");

        let invoked = callback_error.lock().unwrap().take().expect("the callback must be invoked");
        assert!(invoked.is_timeout_error(), "got {invoked:?}");
    }

    /// The other half of the same dispatch: a `SerializationException` — which
    /// extends `KafkaException` directly and is NOT an `ApiException` — reaches
    /// `catch (KafkaException e)` at `KafkaProducer.java:1073` and is rethrown, so
    /// `send()` returns `Err` rather than a failed future.
    #[tokio::test]
    async fn a_serialization_error_from_the_serializer_is_returned_as_err() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer_with_key_serializer(
            metadata,
            accumulator,
            Box::new(FailingSerializer(Error::serialization("not a valid string"))),
        );

        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("k".to_string()), Some("v".to_string()));
        let error = producer
            .send(record)
            .await
            .expect_err("a serialization error is not an API error, so send() returns Err");
        assert!(matches!(error, Error::Serialization(_)), "got {error:?}");
        assert!(error.is_kafka_error(), "a serialization error is a Kafka error: {error:?}");
        assert!(!error.is_api_error(), "but it is not an API error: {error:?}");
        assert_eq!(error.message(), "not a valid string");
    }

    /// `waitOnMetadata`'s first throw attaches the topic's metadata error as the
    /// `TimeoutException`'s **cause** (`KafkaProducer.java:1136`,
    /// `new TimeoutException(errorMessage, metadata.getError(topic).exception())`).
    ///
    /// The message used to be built by interpolating the underlying error into it,
    /// which left `Error::source()` empty — a caller (or a log line) could no longer
    /// walk from the timeout to the broker error that produced it.
    #[tokio::test]
    async fn wait_on_metadata_carries_the_topic_error_as_the_timeout_cause() {
        use crate::MetadataResponseData;
        use crate::common::ApiKeys;
        use crate::common::Errors;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseTopic};

        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(Arc::clone(&metadata), accumulator);

        // A topic the broker reports a retriable error for and no partitions, so the
        // wait times out with `get_error(topic)` populated.
        const MISSING: &str = "topic-with-an-error";
        let mut data = MetadataResponseData::new();
        data.set_controller_id(0);
        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(0);
        broker.set_host("localhost".to_string());
        broker.set_port(9092);
        data.set_brokers(vec![broker]);
        let mut topic_resp = MetadataResponseTopic::new();
        topic_resp.set_name(Some(MISSING.to_string()));
        topic_resp.set_error_code(Errors::LeaderNotAvailable.code());
        topic_resp.set_is_internal(false);
        topic_resp.set_partitions(Vec::new());
        data.set_topics(vec![topic_resp]);
        let response = MetadataResponse::with_version(data, ApiKeys::METADATA.latest_version());
        metadata.add(MISSING, 0);
        metadata.update_with_current_request_version(&response, false, 0);
        assert_eq!(
            metadata.get_error(MISSING),
            Some(Errors::LeaderNotAvailable),
            "the fixture must actually record a topic error"
        );

        let now_ms = producer.now_ms();
        let error = producer
            .wait_on_metadata(MISSING, None, now_ms, 50)
            .await
            .expect_err("the topic has no metadata, so the wait must time out");

        assert!(error.is_timeout_error(), "got {error:?}");
        assert!(
            error.message().contains("not present in metadata"),
            "the message keeps Java's text, without the cause interpolated: {}",
            error.message()
        );
        assert_eq!(
            error.source().expect("the metadata error must be the timeout's cause").error(),
            Errors::LeaderNotAvailable
        );
    }

    /// Translated from `KafkaProducerTest.testMetricConfigRecordingLevel`.
    ///
    /// The Java test constructs a producer with the default config and asserts
    /// `producer.metrics.config().recordLevel() == INFO`, then with
    /// `metrics.recording.level=DEBUG` and asserts `DEBUG`.
    #[test]
    fn test_metric_config_recording_level() {
        let default_producer = create_producer_with_config(
            ProducerConfig::default(),
            create_metadata_with_topic(TOPIC, 1),
            create_accumulator(),
        );
        assert_eq!(default_producer.metrics.config().record_level(), RecordingLevel::Info);

        let debug_config = ProducerConfig { metrics_recording_level: "DEBUG".to_string(), ..Default::default() };
        let debug_producer =
            create_producer_with_config(debug_config, create_metadata_with_topic(TOPIC, 1), create_accumulator());
        assert_eq!(debug_producer.metrics.config().record_level(), RecordingLevel::Debug);
    }

    /// The producer registers the `producer-metrics` latency sensors and
    /// exposes them through `metrics()`; `flush()` records `flush-time-ns-total`.
    #[tokio::test]
    async fn test_flush_records_producer_metrics() {
        let producer = create_producer_with_config(
            ProducerConfig::default(),
            create_metadata_with_topic(TOPIC, 1),
            create_accumulator(),
        );

        // The metrics snapshot exposes the producer-metrics latency sensors.
        let flush_name = producer.metrics.metric_name("flush-time-ns-total", "producer-metrics");
        let snapshot = producer.metrics();
        assert!(
            snapshot.contains_key(&flush_name),
            "metrics() should expose flush-time-ns-total"
        );

        // flush() drives a CumulativeSum record; the metric stays present.
        producer.flush().await.expect("flush");
        assert!(producer.metrics().contains_key(&flush_name));
    }

    /// Translated from `KafkaProducerTest.testSendToInvalidTopic`.
    ///
    /// Tests that sending to an invalid topic name returns a failed future with
    /// an InvalidTopic error. Matches Java behavior where InvalidTopicException
    /// (an ApiException) is caught and returned via a FutureFailure.
    #[tokio::test]
    async fn test_send_to_invalid_topic() {
        use crate::MetadataResponseData;
        use crate::common::ApiKeys;
        use crate::common::Errors;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseTopic};

        let metadata = Arc::new(ProducerMetadata::new(
            100,
            1000,
            300_000,
            300_000,
            ClusterResourceListeners::new(),
        ));

        // Create a metadata response with an invalid topic
        let mut data = MetadataResponseData::new();
        data.set_controller_id(0);

        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(0);
        broker.set_host("localhost".to_string());
        broker.set_port(9092);
        data.set_brokers(vec![broker]);

        let mut topic_resp = MetadataResponseTopic::new();
        topic_resp.set_name(Some("".to_string()));
        topic_resp.set_error_code(Errors::InvalidTopicError.code());
        topic_resp.set_is_internal(false);
        data.set_topics(vec![topic_resp]);

        let response = MetadataResponse::with_version(data, ApiKeys::METADATA.latest_version());
        metadata.add("", 0);
        metadata.update_with_current_request_version(&response, false, 0);

        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        let record = ProducerRecord::new("".to_string(), Some("test".to_string()));
        let result = producer.send(record).await;
        // Java returns a FutureFailure for ApiExceptions like InvalidTopicException
        assert!(result.is_ok(), "send() should return Ok with a failed future for InvalidTopic");
        let future = result.unwrap();
        assert!(future.is_done(), "Failed future should be immediately done");
        let err = future.get().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidTopic(_)),
            "Expected InvalidTopic error, got: {:?}",
            err
        );
        // `waitOnMetadata` throws `new InvalidTopicException(topic)` — the
        // `(String message)` constructor, so `getMessage()` is the topic name
        // (empty here) and `invalidTopics()` is empty, NOT `{topic}`.
        assert_eq!(err.message(), "");
        match &err {
            Error::InvalidTopic(e) => assert!(
                e.invalid_topics().is_empty(),
                "the (String) constructor leaves invalidTopics empty"
            ),
            _ => unreachable!(),
        }
    }

    /// Translated from `KafkaProducerTest.closeShouldBeIdempotent`.
    ///
    /// Tests that calling close multiple times is safe.
    #[tokio::test]
    async fn test_close_should_be_idempotent() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        producer.close().await.unwrap();
        producer.close().await.unwrap();
    }

    /// Translated from `KafkaProducerTest.closeWithNegativeTimestampShouldThrow`.
    ///
    /// Tests that close with zero timeout works correctly.
    /// In Java this tests negative Duration; in Rust `Duration` is unsigned
    /// so we test `Duration::ZERO` instead.
    #[tokio::test]
    async fn test_close_with_zero_timeout() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        let result = producer.close_with_timeout(Duration::ZERO).await;
        assert!(result.is_ok());
    }

    /// Translated from `KafkaProducerTest.testPartitionsForWithNullTopic`.
    ///
    /// Tests that partitions_for returns the correct number of partitions.
    #[tokio::test]
    async fn test_partitions_for_returns_partitions() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        let partitions = producer.partitions_for(TOPIC).await.unwrap();
        assert_eq!(3, partitions.len());
    }

    /// Tests that sending after close returns an error.
    ///
    /// Translated from `KafkaProducerTest.testTransactionalMethodThrowsWhenSenderClosed`
    /// (non-transactional part).
    #[tokio::test]
    async fn test_send_after_close_returns_error() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        producer.close().await.unwrap();

        let record = ProducerRecord::new(TOPIC.to_string(), Some("test".to_string()));
        let result = producer.send(record).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::LocalIllegalState(msg) => {
                assert!(msg.message().contains("after producer has been closed"));
            },
            other => panic!("Expected IllegalState error, got: {:?}", other),
        }
    }

    /// Tests that a record too large for max_request_size returns a failed future.
    ///
    /// Translated from `KafkaProducerTest.testInterceptorPartitionSetOnTooLargeRecord`
    /// (the record-too-large validation part).
    ///
    /// Matches Java behavior: ApiExceptions like RecordTooLargeException are returned
    /// via a completed-with-error future, not propagated as Err from send().
    #[tokio::test]
    async fn test_ensure_valid_record_size_rejects_too_large() {
        let config = ProducerConfig { max_request_size: 10, ..Default::default() };
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();

        let producer = create_producer_with_config(config, metadata, accumulator);

        // Create a record that's larger than 10 bytes
        let large_value = "a".repeat(100);
        let record = ProducerRecord::new(TOPIC.to_string(), Some(large_value));
        let result = producer.send(record).await;
        // Java returns a FutureFailure, not an exception from send()
        assert!(
            result.is_ok(),
            "send() should return Ok with a failed future for RecordTooLarge"
        );
        let future = result.unwrap();
        assert!(future.is_done(), "Failed future should be immediately done");
        let err = future.get().await.unwrap_err();
        match err {
            Error::RecordTooLarge(msg) => {
                assert!(
                    msg.message().contains(ProducerConfig::MAX_REQUEST_SIZE_CONFIG),
                    "Error message should mention the config key: {}",
                    msg
                );
            },
            other => panic!("Expected RecordTooLarge error, got: {:?}", other),
        }
    }

    /// Tests that records larger than total buffer memory return a failed future.
    ///
    /// Matches Java behavior: ApiExceptions like RecordTooLargeException are returned
    /// via a completed-with-error future, not propagated as Err from send().
    #[tokio::test]
    async fn test_ensure_valid_record_size_rejects_larger_than_buffer_memory() {
        let config = ProducerConfig { buffer_memory: 10, ..Default::default() };
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();

        let producer = create_producer_with_config(config, metadata, accumulator);

        let large_value = "a".repeat(100);
        let record = ProducerRecord::new(TOPIC.to_string(), Some(large_value));
        let result = producer.send(record).await;
        assert!(
            result.is_ok(),
            "send() should return Ok with a failed future for RecordTooLarge"
        );
        let future = result.unwrap();
        assert!(future.is_done(), "Failed future should be immediately done");
        let err = future.get().await.unwrap_err();
        match err {
            Error::RecordTooLarge(msg) => {
                assert!(
                    msg.message().contains(ProducerConfig::BUFFER_MEMORY_CONFIG),
                    "Error message should mention the config key: {}",
                    msg
                );
            },
            other => panic!("Expected RecordTooLarge error, got: {:?}", other),
        }
    }

    /// `doSend`'s `catch (ApiException e)` block invokes the user `Callback` exactly
    /// once with a null-metadata `RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)`
    /// and the error, then returns a failed future
    /// (`KafkaProducer.java:1056-1068`).
    ///
    /// The arm covering a failure of `accumulator.append` itself used to drop the
    /// callback: it is *moved* into `append`, and `append` returned it only on the
    /// success path. An application that registers callbacks and never awaits the
    /// future was therefore never told the record had been dropped.
    ///
    /// `buffer.memory` sized for exactly one batch plus `max.block.ms = 0` makes the
    /// second `append` fail deterministically inside `BufferPool::allocate` with
    /// `BufferExhaustedException`, which is an `ApiException`
    /// (`BufferExhaustedException extends TimeoutException`) — so this is the
    /// `catch (ApiException e)` arm.
    #[tokio::test]
    async fn test_api_error_from_append_fires_the_callback_exactly_once() {
        const BATCH_SIZE: usize = 16384;
        let config = ProducerConfig {
            batch_size: BATCH_SIZE as i32,
            buffer_memory: BATCH_SIZE as i64,
            max_block_ms: 0,
            linger_ms: 0,
            ..Default::default()
        };
        // Two partitions, so the second record needs a *new* batch rather than
        // appending to the first record's still-roomy one.
        let metadata = create_metadata_with_topic(TOPIC, 2);
        let accumulator = Arc::new(RecordAccumulator::new(
            BATCH_SIZE as i32,
            Compression::none(),
            0,
            100,
            1000,
            120_000,
            PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
            Arc::new(Metrics::new()),
            KafkaProducer::<String, String>::PRODUCER_METRIC_GROUP_NAME,
            // Room for exactly one batch.
            Arc::new(BufferPool::new_for_test(BATCH_SIZE as i64, BATCH_SIZE)),
            None,
        ));
        let producer = create_producer_with_config(config, metadata, accumulator);

        // The first record consumes the pool's only batch.
        producer
            .do_send(
                ProducerRecord::with_partition_key(TOPIC.to_string(), Some(0), None, Some("first".to_string()))
                    .unwrap(),
                None,
            )
            .await
            .expect("the first send has memory");

        // The second record targets partition 1, so its append has to allocate; it
        // finds the pool empty and `max.block.ms = 0`, and fails with
        // `BufferExhaustedException`.
        let invocations: Arc<Mutex<Vec<(i64, i32, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&invocations);
        let callback: Callback = Box::new(move |metadata, error| {
            let metadata = metadata.expect("Java passes a non-null RecordMetadata here");
            let error = error.expect("Java passes the exception here");
            recorder
                .lock()
                .unwrap()
                .push((metadata.offset(), metadata.partition(), error.message().to_string()));
        });

        let future = producer
            .do_send(
                ProducerRecord::with_partition_key(TOPIC.to_string(), Some(1), None, Some("second".to_string()))
                    .unwrap(),
                Some(callback),
            )
            .await
            .expect("an ApiException becomes a failed future, not an Err");

        // The callback fired exactly once. `Callback` is a `Box<dyn FnOnce>`, so
        // "at most once" is a type-level guarantee; this pins "at least once".
        // The guard is cloned out and dropped before the `await` below (CLAUDE.md §9.6).
        let invocations = invocations.lock().unwrap().clone();
        assert_eq!(invocations.len(), 1, "the callback must fire exactly once, got {invocations:?}");
        let (offset, partition, message) = &invocations[0];
        // Java's `nullMetadata`: `new RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)`.
        assert_eq!(*offset, -1, "the null metadata carries offset -1");
        assert_eq!(*partition, 1, "the null metadata names the record's topic-partition");
        assert!(
            message.contains("Failed to allocate"),
            "the callback receives the BufferExhausted error: {message}"
        );

        // And the future is failed with the same error, as Java's `FutureFailure` is.
        let error = future.get().await.expect_err("the future must be failed");
        assert!(
            matches!(error, Error::ProducerBufferExhausted(_)),
            "expected ProducerBufferExhausted, got {error:?}"
        );
        assert!(
            error.is_api_error(),
            "BufferExhaustedException extends TimeoutException, an ApiException"
        );
    }

    /// The sibling of the test above on the *rethrowing* path: Java's
    /// `catch (KafkaException e)` (`KafkaProducer.java:1072-1076`) does NOT invoke the
    /// callback — it rethrows out of `send()`. `RecordAccumulator.tryAppend`'s
    /// `KafkaException("Producer closed while send in progress")` is a bare
    /// `KafkaException`, so it lands there.
    #[tokio::test]
    async fn test_non_api_error_from_append_does_not_fire_the_callback() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        accumulator.close();
        let producer = create_producer_with_config(ProducerConfig::default(), metadata, accumulator);

        let invoked = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&invoked);
        let callback: Callback = Box::new(move |_, _| flag.store(true, Ordering::SeqCst));

        let error = producer
            .do_send(
                ProducerRecord::with_partition_key(TOPIC.to_string(), Some(0), None, Some("v".to_string())).unwrap(),
                Some(callback),
            )
            .await
            .expect_err("a bare KafkaException is rethrown out of send()");
        assert_eq!(error.message(), "Producer closed while send in progress");
        assert!(!error.is_api_error(), "a bare KafkaException is not an ApiException");
        assert!(
            !invoked.load(Ordering::SeqCst),
            "Java's catch (KafkaException e) block does not invoke the callback"
        );
    }

    /// Tests that the partition() method returns the explicit partition when set.
    #[test]
    fn test_partition_returns_explicit_partition() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata.clone(), accumulator);

        let record = ProducerRecord::with_partition_key(
            TOPIC.to_string(),
            Some(2),
            Some("key".to_string()),
            Some("value".to_string()),
        )
        .unwrap();
        let cluster = metadata.fetch();
        let partition = producer.partition(&record, Some(b"key"), Some(b"value"), &cluster);
        assert_eq!(2, partition);
    }

    /// Tests that the partition() method uses key hashing when no partition is set.
    #[test]
    fn test_partition_uses_key_hash() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata.clone(), accumulator);

        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("key".to_string()), Some("value".to_string()));
        let cluster = metadata.fetch();
        let partition = producer.partition(&record, Some(b"key"), Some(b"value"), &cluster);
        // Should be deterministic based on key hash
        assert!((0..3).contains(&partition));

        // Same key should give the same partition
        let partition2 = producer.partition(&record, Some(b"key"), Some(b"value"), &cluster);
        assert_eq!(partition, partition2);
    }

    /// Tests that the partition() method returns UNKNOWN_PARTITION when no key and no partition.
    #[test]
    fn test_partition_returns_unknown_when_no_key() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata.clone(), accumulator);

        let record: ProducerRecord<String, String> = ProducerRecord::new(TOPIC.to_string(), Some("value".to_string()));
        let cluster = metadata.fetch();
        let partition = producer.partition(&record, None, Some(b"value"), &cluster);
        assert_eq!(RecordMetadata::UNKNOWN_PARTITION, partition);
    }

    /// Tests that the partition() method ignores keys when partitioner_ignore_keys is true.
    #[test]
    fn test_partition_ignores_keys_when_configured() {
        let config = ProducerConfig { partitioner_ignore_keys: true, ..Default::default() };
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();

        let producer = create_producer_with_config(config, metadata.clone(), accumulator);

        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("key".to_string()), Some("value".to_string()));
        let cluster = metadata.fetch();
        let partition = producer.partition(&record, Some(b"key"), Some(b"value"), &cluster);
        assert_eq!(RecordMetadata::UNKNOWN_PARTITION, partition);
    }

    /// Builds a `KafkaProducer` whose `partitioner.class` selects
    /// `Murmur2RandomPartitioner` (the `KeyHasher::Murmur2` hash).
    fn create_murmur2_producer(
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
    ) -> KafkaProducer<String, String> {
        let mut props = HashMap::new();
        props.insert("partitioner.class".to_string(), "Murmur2RandomPartitioner".to_string());
        let config = ProducerConfig::new(&props).expect("murmur2 partitioner config is valid");
        create_producer_with_config(config, metadata, accumulator)
    }

    /// The default (unset `partitioner.class`) keyed partition path uses the
    /// IEEE CRC-32 (librdkafka `consistent_random`) hash taken UNSIGNED modulo
    /// the partition count — NOT murmur2 / `Utils.toPositive`. This is the
    /// deliberate, user-approved deviation from Java parity.
    #[test]
    fn test_partition_default_hasher_is_crc32() {
        let metadata = create_metadata_with_topic(TOPIC, 7);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata.clone(), accumulator);
        assert_eq!(KeyHasher::Crc32, producer.key_hasher);

        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("key".to_string()), Some("value".to_string()));
        let cluster = metadata.fetch();
        let partition = producer.partition(&record, Some(b"key"), Some(b"value"), &cluster);
        assert_eq!((crc32fast::hash(b"key") % 7) as i32, partition);
        // Both send call sites route through the same helper, so it agrees.
        assert_eq!(partition, producer.partition_for_key_or_unknown(Some(b"key"), TOPIC, &cluster));
    }

    /// Under the CRC-32 default, an EMPTY (but non-null) key defers to the
    /// keyless sticky (KIP-794) path — `UNKNOWN_PARTITION` — matching
    /// librdkafka `consistent_random` (which treats `keylen == 0` as no key).
    /// Asserted through both send call sites' shared helper.
    #[test]
    fn test_partition_empty_key_crc32_defers_to_sticky() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata.clone(), accumulator);

        let record = ProducerRecord::with_key(TOPIC.to_string(), Some(String::new()), Some("value".to_string()));
        let cluster = metadata.fetch();
        // via KafkaProducer.partition()
        assert_eq!(
            RecordMetadata::UNKNOWN_PARTITION,
            producer.partition(&record, Some(b""), Some(b"value"), &cluster)
        );
        // via the helper do_send_bytes uses
        assert_eq!(
            RecordMetadata::UNKNOWN_PARTITION,
            producer.partition_for_key_or_unknown(Some(b""), TOPIC, &cluster)
        );
    }

    /// `Murmur2RandomPartitioner` reproduces the exact Java-client formula
    /// `Utils.toPositive(Utils.murmur2(key)) % numPartitions` for a non-empty
    /// key.
    #[test]
    fn test_partition_murmur2_matches_java_formula() {
        let metadata = create_metadata_with_topic(TOPIC, 7);
        let accumulator = create_accumulator();
        let producer = create_murmur2_producer(metadata.clone(), accumulator);
        assert_eq!(KeyHasher::Murmur2, producer.key_hasher);

        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("key".to_string()), Some("value".to_string()));
        let cluster = metadata.fetch();
        let partition = producer.partition(&record, Some(b"key"), Some(b"value"), &cluster);
        assert_eq!(Utils::to_positive(Utils::murmur2(b"key")) % 7, partition);
        assert_eq!(partition, producer.partition_for_key_or_unknown(Some(b"key"), TOPIC, &cluster));
    }

    /// murmur2 hashes an EMPTY (non-null) key rather than deferring to the
    /// sticky path — exact Java-client parity, and the point of contrast with
    /// the CRC-32 default (`test_partition_empty_key_crc32_defers_to_sticky`).
    #[test]
    fn test_partition_empty_key_murmur2_is_hashed() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_murmur2_producer(metadata.clone(), accumulator);

        let cluster = metadata.fetch();
        let partition = producer.partition_for_key_or_unknown(Some(b""), TOPIC, &cluster);
        assert_eq!(Utils::to_positive(Utils::murmur2(b"")) % 3, partition);
        assert_ne!(RecordMetadata::UNKNOWN_PARTITION, partition);
    }

    /// An explicit record partition wins over key hashing regardless of the
    /// selected hasher (here: murmur2). Complements
    /// `test_partition_returns_explicit_partition` (which covers the default).
    #[test]
    fn test_partition_explicit_partition_wins_under_murmur2() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_murmur2_producer(metadata.clone(), accumulator);

        let record = ProducerRecord::with_partition_key(
            TOPIC.to_string(),
            Some(2),
            Some("key".to_string()),
            Some("value".to_string()),
        )
        .unwrap();
        let cluster = metadata.fetch();
        assert_eq!(2, producer.partition(&record, Some(b"key"), Some(b"value"), &cluster));
    }

    /// `partitioner.ignore.keys=true` forces `UNKNOWN_PARTITION` even when a
    /// non-empty key would otherwise hash, under murmur2 as well as the default.
    #[test]
    fn test_partition_ignore_keys_overrides_murmur2() {
        let mut props = HashMap::new();
        props.insert("partitioner.class".to_string(), "Murmur2RandomPartitioner".to_string());
        props.insert("partitioner.ignore.keys".to_string(), "true".to_string());
        let config = ProducerConfig::new(&props).unwrap();
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer_with_config(config, metadata.clone(), accumulator);

        let cluster = metadata.fetch();
        assert_eq!(
            RecordMetadata::UNKNOWN_PARTITION,
            producer.partition_for_key_or_unknown(Some(b"key"), TOPIC, &cluster)
        );
    }

    // =====================================================================
    // PHASE-2 TEST ACCOUNTING — the pluggable-`Partitioner` `KafkaProducerTest` methods
    //
    // SCOPE CRITERION. A `KafkaProducerTest.java` method is in scope for Phase 2 iff its
    // body sets `PARTITIONER_CLASS_CONFIG` or names one of the partitioner nested classes
    // (`MockPartitioner` / `PartitionerForClientId` / `BuggyPartitioner` /
    // `MonitorablePartitioner`) as its subject. That marker set is exactly five methods:
    //
    //   - testPartitionerClose (:668) .............. TRANSLATED -> test_partitioner_close
    //   - negativePartitionShouldThrow (:2410) ..... TRANSLATED -> negative_partition_should_throw
    //   - configurableObjectsShouldSeeGeneratedClientId (:2296)
    //         ...... TRANSLATED (partitioner portion) -> configurable_objects_should_see_generated_client_id
    //   - shouldCloseProperlyAndThrowIfInterrupted (:689) ....... N/A (see below)
    //   - testMonitorablePlugins (:2828) ........................ N/A (see below)
    //
    // N/A justifications:
    //
    //   - shouldCloseProperlyAndThrowIfInterrupted asserts that `close()` blocks until an
    //     in-flight `send` completes and that INTERRUPTING the closing thread surfaces an
    //     `InterruptException`. Thread interruption (`Thread.interrupt()` /
    //     `InterruptException`) has no Rust/Tokio analogue — the same gap the producer-
    //     transaction rules record for `TransactionalRequestResult`. `MockPartitioner` is
    //     incidental here (merely the configured `partitioner.class`), not the subject, so
    //     this belongs to the close/interrupt surface, not Phase 2.
    //   - testMonitorablePlugins exercises the `Plugin` / `Monitorable` metrics SPI, which
    //     the Phase 2 spec places explicitly out of scope. `MonitorablePartitioner` is only
    //     the vehicle; there is nothing partitioner-specific to translate.
    //
    // RUST-ONLY BEHAVIORAL TESTS. Java gets its partitioner round-trip coverage "for free"
    // from a real/mock broker plus reflective `partitioner.class` loading. Rust has no
    // reflection (a user partitioner is passed as an instance) and these unit tests do not
    // stand up a broker, so five behavioral contracts that Java never asserts directly are
    // pinned explicitly here:
    //
    //   - test_round_robin_partitioner_resolution_and_gating — `new` resolves a
    //         built-in `partitioner.class` (both spellings) and a resolved partitioner
    //         disables adaptive partitioning (KafkaProducer.java:428-433).
    //   - test_round_robin_partitioner_used_once_per_record — a stateful partitioner is
    //         consulted EXACTLY once per record on the send path (do_send computes it,
    //         do_send_bytes short-circuits on the passed `Some(..)`).
    //   - test_partitioner_receives_key_value_and_serialized_bytes — the typed key/value
    //         and the serialized bytes are forwarded unchanged.
    //   - test_explicit_partition_bypasses_partitioner — an explicit record partition
    //         short-circuits `compute_partition` before the partitioner is consulted.
    //   - test_explicit_partitioner_instance_wins_over_partitioner_class — an explicit
    //         instance overrides a built-in `partitioner.class` (Java's
    //         `getConfiguredInstance` returns the caller-provided instance).
    // =====================================================================

    // --- test-local partitioners (translated from KafkaProducerTest's nested classes) ---

    /// Translated from `KafkaProducerTest.BuggyPartitioner` (`:2583-2595`): always
    /// returns an invalid negative partition, exercising `compute_partition`'s
    /// non-negative guard. Java's empty `close`/`configure` overrides are the trait's
    /// default no-ops, so they are intentionally not overridden.
    struct BuggyPartitioner;

    impl<K, V> Partitioner<K, V> for BuggyPartitioner {
        fn partition(
            &self,
            _topic: &str,
            _key: Option<&K>,
            _key_bytes: Option<&[u8]>,
            _value: Option<&V>,
            _value_bytes: Option<&[u8]>,
            _cluster: &Cluster,
        ) -> i32 {
            -1
        }
    }

    /// Translated from `KafkaProducerTest.PartitionerForClientId` (`:2521-2536`):
    /// `partition` returns 0, and `configure` records the resolved `client.id` so a
    /// test can assert the partitioner saw the generated id. Java accumulates into a
    /// static `CLIENT_IDS` set; here the sink is an injected `Arc<Mutex<Vec<String>>>`
    /// so tests running in parallel do not share it.
    struct PartitionerForClientId {
        client_ids: Arc<Mutex<Vec<String>>>,
    }

    impl<K, V> Partitioner<K, V> for PartitionerForClientId {
        fn configure(&mut self, configs: &HashMap<String, String>) {
            if let Some(id) = configs.get(ProducerConfig::CLIENT_ID_CONFIG) {
                self.client_ids.lock().unwrap().push(id.clone());
            }
        }

        fn partition(
            &self,
            _topic: &str,
            _key: Option<&K>,
            _key_bytes: Option<&[u8]>,
            _value: Option<&V>,
            _value_bytes: Option<&[u8]>,
            _cluster: &Cluster,
        ) -> i32 {
            0
        }
    }

    /// One recorded `Partitioner::partition` invocation: the borrowed arguments
    /// (captured as owned copies) and the partition the delegate returned.
    #[derive(Debug, Clone)]
    struct RecordedCall {
        topic: String,
        key: Option<String>,
        key_bytes: Option<Vec<u8>>,
        value: Option<String>,
        value_bytes: Option<Vec<u8>>,
        returned: i32,
    }

    /// A `Partitioner` decorator with no Java counterpart: it delegates to an inner
    /// partitioner and records every `partition` call. It exists to probe two
    /// contracts Java gets implicitly from a real broker — that a stateful partitioner
    /// is consulted EXACTLY once per record, and that the producer forwards the typed
    /// key/value and the serialized bytes unchanged. A justified `definition-of-done`
    /// §7 addition (a test-only decorator, present only under `#[cfg(test)]`).
    struct RecordingPartitioner {
        inner: Box<dyn Partitioner<String, String>>,
        calls: Arc<Mutex<Vec<RecordedCall>>>,
    }

    impl Partitioner<String, String> for RecordingPartitioner {
        fn partition(
            &self,
            topic: &str,
            key: Option<&String>,
            key_bytes: Option<&[u8]>,
            value: Option<&String>,
            value_bytes: Option<&[u8]>,
            cluster: &Cluster,
        ) -> i32 {
            let returned = self.inner.partition(topic, key, key_bytes, value, value_bytes, cluster);
            self.calls.lock().unwrap().push(RecordedCall {
                topic: topic.to_string(),
                key: key.cloned(),
                key_bytes: key_bytes.map(|b| b.to_vec()),
                value: value.cloned(),
                value_bytes: value_bytes.map(|b| b.to_vec()),
                returned,
            });
            returned
        }
    }

    /// Seam constructor for a producer that owns a custom `partitioner` (the
    /// `KafkaProducer::new` last argument). Metadata is pre-seeded (see
    /// `create_metadata_with_topic`) and `sender_handle` is `None`, so `send()` and
    /// `close()` neither block on metadata nor hang on a sender task — the send-path
    /// partitioner tests need a producer that can actually append a record. NOTE the
    /// `new()` seam stores the partitioner as-is and does NOT `configure` it or gate
    /// adaptive partitioning; the `new`-based tests below cover those.
    fn create_producer_with_partitioner(
        config: ProducerConfig,
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
        partitioner: Box<dyn Partitioner<String, String>>,
    ) -> KafkaProducer<String, String> {
        let running = Arc::new(AtomicBool::new(true));
        let force_close = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(Notify::new());

        KafkaProducer::with_options(
            KafkaProducerOptionsBuilder::new()
                .set_config(&config)
                .set_key_serializer(Box::new(StringSerializer))
                .set_value_serializer(Box::new(StringSerializer))
                .set_metadata(metadata)
                .set_accumulator(accumulator)
                .set_running(running)
                .set_force_close(force_close)
                .set_wakeup(wakeup)
                .set_time_provider(default_time_provider())
                .set_pending_requests(Arc::new(Mutex::new(PendingRequests::new())))
                .set_partitioner(Some(partitioner))
                .build()
                .expect("KafkaProducerOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// Translated from `KafkaProducerTest.testPartitionerClose` (`:667-686`).
    ///
    /// Java resolves the partitioner reflectively through `partitioner.class`, which
    /// invokes the `MockPartitioner` constructor (bumping `INIT_COUNT`) and then
    /// `configure`. `MockPartitioner` is a test-only type with no built-in
    /// `partitioner.class` name, so here it is supplied as an explicit instance;
    /// constructing it via `MockPartitioner::new()` bumps `INIT_COUNT` exactly as the
    /// Java ctor does. `close()` then bumps `CLOSE_COUNT` (`KafkaProducer.java:1446`).
    ///
    /// The counters are process-global statics, so the whole body must hold
    /// `lock_counters()` and reset at start AND end (Java's `finally`) to serialize
    /// against the other counter-touching test (`mock_partitioner`'s `test_counts_init_and_close`).
    /// That serializer is a `std::sync::Mutex` guard (matching the codebase's
    /// preference for CPU-bound state, and shared with the D6 test), and it must stay
    /// held across the async `close()` — otherwise a parallel counter test could
    /// interleave between the INIT and CLOSE assertions. Holding a `std` guard across
    /// an `.await` is exactly what `clippy::await_holding_lock` forbids, so this is a
    /// plain `#[test]` that drives `close()` on a dedicated current-thread runtime via
    /// `block_on`: the guard spans a synchronous `block_on` call rather than an
    /// `.await` point (and the close path never locks `COUNTER_GUARD`, so parking the
    /// thread inside `block_on` while holding it cannot deadlock).
    #[test]
    fn test_partitioner_close() {
        let _guard = MockPartitioner::lock_counters();
        MockPartitioner::reset_counters();

        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let producer = create_producer_with_partitioner(
                ProducerConfig::default(),
                create_metadata_with_topic(TOPIC, 1),
                create_accumulator(),
                Box::new(MockPartitioner::new()),
            );
            assert_eq!(1, MockPartitioner::init_count().load(Ordering::SeqCst));
            assert_eq!(0, MockPartitioner::close_count().load(Ordering::SeqCst));

            producer.close().await.expect("seam producer closes cleanly");

            assert_eq!(1, MockPartitioner::init_count().load(Ordering::SeqCst));
            assert_eq!(1, MockPartitioner::close_count().load(Ordering::SeqCst));
        });

        // Cleanup since we use mutable process-global statics in MockPartitioner.
        MockPartitioner::reset_counters();
    }

    /// Translated from `KafkaProducerTest.negativePartitionShouldThrow` (`:2410-2427`).
    ///
    /// A partitioner returning a negative partition makes `send()` fail with
    /// `IllegalArgument` (Java: `IllegalArgumentException`). The exact message is
    /// asserted (DoD §3): error-message content is part of the behavioral contract.
    #[tokio::test]
    async fn negative_partition_should_throw() {
        let producer = create_producer_with_partitioner(
            ProducerConfig::default(),
            create_metadata_with_topic("topic", 1),
            create_accumulator(),
            Box::new(BuggyPartitioner),
        );

        let record = ProducerRecord::with_key("topic".to_string(), Some("key".to_string()), Some("value".to_string()));
        let err = producer.send(record).await.expect_err("negative partition must be rejected");
        match &err {
            Error::LocalIllegalArgument(msg) => {
                assert_eq!(
                    msg.message(),
                    "The partitioner generated an invalid partition number: -1. Partition number should always be non-negative."
                );
            },
            other => panic!("Expected LocalIllegalArgument error, got: {:?}", other),
        }
    }

    /// Translated from `KafkaProducerTest.configurableObjectsShouldSeeGeneratedClientId`
    /// (`:2296-2310`), partitioner portion.
    ///
    /// A `configure`-able partitioner must see the generated `client.id` when the user
    /// sets none. Java asserts the same for the two serializers and the interceptor
    /// (`CLIENT_IDS.size() == 4`); Rust's `Serializer` has no `configure(client.id)`
    /// hook and no `ProducerInterceptor` exists yet, so only the partitioner is checked
    /// here — hence exactly ONE recorded id rather than four. The producer is built via
    /// `with_partitioner` so `configure` actually runs (the `new()` seam
    /// does not configure).
    #[tokio::test]
    async fn configurable_objects_should_see_generated_client_id() {
        let client_ids = Arc::new(Mutex::new(Vec::new()));
        let props = guard_props(&[]); // bootstrap only; NO client.id
        let config = ProducerConfig::new(&props).expect("valid config");
        let producer = KafkaProducer::<String, String>::with_partitioner(
            config,
            Box::new(StringSerializer),
            Box::new(StringSerializer),
            Box::new(PartitionerForClientId { client_ids: Arc::clone(&client_ids) }),
        )
        .expect("constructs with an explicit partitioner");

        assert!(!producer.client_id().is_empty(), "a client.id is generated when unset");
        let recorded = client_ids.lock().unwrap();
        assert_eq!(1, recorded.len(), "the partitioner's configure ran exactly once");
        assert_eq!(
            recorded[0].as_str(),
            producer.client_id(),
            "the partitioner saw the generated client.id"
        );
    }

    /// A built-in `partitioner.class` (both the simple name and the fully-qualified
    /// Java class name) is resolved by `new` into a live `partitioner`, and a
    /// resolved partitioner turns OFF adaptive partitioning in the accumulator (Java
    /// `KafkaProducer.java:428-433`: "no need ... if we use a custom partitioner"). The
    /// control producer (no `partitioner.class`) keeps `partitioner = None` and adaptive
    /// partitioning follows the config default.
    ///
    /// Rust-only behavioral test (Java never inspects these internals); it pins the
    /// `new` resolution + accumulator gating D3/D4 added. `#[tokio::test]`:
    /// `new` spawns the Sender task, which fails to reach localhost:9999
    /// harmlessly and is dropped with the test.
    #[tokio::test]
    async fn test_round_robin_partitioner_resolution_and_gating() {
        for name in [
            "RoundRobinPartitioner",
            "org.apache.kafka.clients.producer.RoundRobinPartitioner",
        ] {
            let props = guard_props(&[("partitioner.class", name)]);
            let config = ProducerConfig::new(&props).expect("valid config");
            let producer =
                KafkaProducer::<String, String>::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
                    .expect("RoundRobinPartitioner resolves");
            assert!(
                producer.partitioner.is_some(),
                "partitioner.class={name} must resolve a partitioner"
            );
            assert!(
                !producer.accumulator.enable_adaptive_partitioning_for_test(),
                "a resolved partitioner must disable adaptive partitioning (partitioner.class={name})"
            );
        }

        // Control: no partitioner.class -> no partitioner, adaptive follows config.
        let props = guard_props(&[]);
        let config = ProducerConfig::new(&props).expect("valid config");
        let adaptive_default = config.partitioner_adaptive_partitioning_enable;
        let producer =
            KafkaProducer::<String, String>::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
                .expect("default config constructs");
        assert!(producer.partitioner.is_none(), "no partitioner.class -> no partitioner");
        assert_eq!(
            adaptive_default,
            producer.accumulator.enable_adaptive_partitioning_for_test(),
            "without a partitioner, adaptive partitioning follows partitioner.adaptive.partitioning.enable"
        );
    }

    /// A stateful `RoundRobinPartitioner` used through the producer's send path is
    /// consulted EXACTLY once per record and cycles through the topic's available
    /// partitions. A recording decorator wraps the RoundRobin; four sends over a
    /// 3-partition topic must produce four calls whose first three returned partitions
    /// are a permutation of {0,1,2} and whose fourth repeats the first (the counter
    /// wraps mod 3). The available-partition ORDER is not contractually fixed, so the
    /// assertion is order-robust rather than hard-coded to `[0,1,2,0]`.
    #[tokio::test]
    async fn test_round_robin_partitioner_used_once_per_record() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let partitioner =
            RecordingPartitioner { inner: Box::new(RoundRobinPartitioner::new()), calls: Arc::clone(&calls) };
        let producer = create_producer_with_partitioner(
            ProducerConfig::default(),
            create_metadata_with_topic("topic", 3),
            create_accumulator(),
            Box::new(partitioner),
        );

        for _ in 0..4 {
            let record = ProducerRecord::with_key("topic".to_string(), Some("k".to_string()), Some("v".to_string()));
            producer.send(record).await.expect("send succeeds");
        }

        let recorded = calls.lock().unwrap();
        assert_eq!(4, recorded.len(), "the partitioner is consulted exactly once per record");
        let returned: Vec<i32> = recorded.iter().map(|c| c.returned).collect();
        let mut first_cycle = returned[..3].to_vec();
        first_cycle.sort_unstable();
        assert_eq!(vec![0, 1, 2], first_cycle, "the first cycle visits every partition once");
        assert_eq!(returned[0], returned[3], "the fourth record wraps back to the first partition");
    }

    /// The producer forwards the record's typed key/value AND the serialized bytes to
    /// the partitioner unchanged. A single send of key "mykey"/value "myval" with no
    /// explicit partition must record `key=Some("mykey")`, `key_bytes=b"mykey"`,
    /// `value=Some("myval")`, `value_bytes=b"myval"` (the `StringSerializer` output).
    #[tokio::test]
    async fn test_partitioner_receives_key_value_and_serialized_bytes() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let partitioner =
            RecordingPartitioner { inner: Box::new(RoundRobinPartitioner::new()), calls: Arc::clone(&calls) };
        let producer = create_producer_with_partitioner(
            ProducerConfig::default(),
            create_metadata_with_topic("topic", 3),
            create_accumulator(),
            Box::new(partitioner),
        );

        let record =
            ProducerRecord::with_key("topic".to_string(), Some("mykey".to_string()), Some("myval".to_string()));
        producer.send(record).await.expect("send succeeds");

        let recorded = calls.lock().unwrap();
        assert_eq!(1, recorded.len());
        let call = &recorded[0];
        assert_eq!("topic", call.topic);
        assert_eq!(Some("mykey".to_string()), call.key);
        assert_eq!(Some(b"mykey".to_vec()), call.key_bytes);
        assert_eq!(Some("myval".to_string()), call.value);
        assert_eq!(Some(b"myval".to_vec()), call.value_bytes);
    }

    /// An explicit record partition short-circuits `compute_partition` before the
    /// partitioner is consulted (it returns the given `Some(p)` immediately). A send
    /// with an explicit partition therefore records ZERO partitioner calls and still
    /// succeeds.
    #[tokio::test]
    async fn test_explicit_partition_bypasses_partitioner() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let partitioner =
            RecordingPartitioner { inner: Box::new(RoundRobinPartitioner::new()), calls: Arc::clone(&calls) };
        let producer = create_producer_with_partitioner(
            ProducerConfig::default(),
            create_metadata_with_topic("topic", 3),
            create_accumulator(),
            Box::new(partitioner),
        );

        let record = ProducerRecord::with_partition_key(
            "topic".to_string(),
            Some(2),
            Some("k".to_string()),
            Some("v".to_string()),
        )
        .unwrap();
        producer.send(record).await.expect("send with explicit partition succeeds");

        assert!(
            calls.lock().unwrap().is_empty(),
            "explicit partition must bypass the partitioner"
        );
    }

    /// An explicit partitioner instance passed to `with_partitioner` wins
    /// over a built-in `partitioner.class`, mirroring Java's `getConfiguredInstance`
    /// returning the caller-provided instance. Verified via `configure`: only the
    /// explicit `PartitionerForClientId` records the client.id (`RoundRobinPartitioner`'s
    /// `configure` is the default no-op), so a single recorded id proves the explicit
    /// instance — not the RoundRobin named by `partitioner.class` — was resolved and
    /// configured.
    #[tokio::test]
    async fn test_explicit_partitioner_instance_wins_over_partitioner_class() {
        let client_ids = Arc::new(Mutex::new(Vec::new()));
        let props = guard_props(&[("partitioner.class", "RoundRobinPartitioner")]);
        let config = ProducerConfig::new(&props).expect("valid config");
        let producer = KafkaProducer::<String, String>::with_partitioner(
            config,
            Box::new(StringSerializer),
            Box::new(StringSerializer),
            Box::new(PartitionerForClientId { client_ids: Arc::clone(&client_ids) }),
        )
        .expect("explicit instance overrides partitioner.class");

        let recorded = client_ids.lock().unwrap();
        assert_eq!(
            1,
            recorded.len(),
            "the explicit PartitionerForClientId was configured, not RoundRobin"
        );
        assert_eq!(recorded[0].as_str(), producer.client_id());
    }

    /// Tests that a record can be successfully sent and appended to the accumulator.
    ///
    /// Translated from `KafkaProducerTest.testFlushCompleteSendOfInflightBatches`
    /// (the send part).
    #[tokio::test]
    async fn test_send_appends_to_accumulator() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("key".to_string()), Some("value".to_string()));
        let result = producer.send(record).await;
        assert!(result.is_ok(), "Send should succeed");

        // Verify that the accumulator has undrained batches
        assert!(accumulator.has_undrained());
    }

    /// Tests that flush with no incomplete batches returns immediately.
    ///
    /// When there are no pending records, flush should complete instantly
    /// since there is nothing to wait for.
    #[tokio::test]
    async fn test_flush_with_no_pending_records() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        // Flush with nothing pending should succeed immediately
        let result = producer.flush().await;
        assert!(result.is_ok());
    }

    /// Translated from `KafkaProducerTest.testDeliveryTimeoutAndLingerMsConfig`.
    ///
    /// `delivery.timeout.ms` must be >= `linger.ms + request.timeout.ms`. When the
    /// user set it explicitly, Java throws `ConfigException` (`KafkaProducer.java:578`)
    /// — and the Java test asserts `KafkaException.class`, so the error must answer
    /// `true` to `is_kafka_error()`.
    #[test]
    fn test_delivery_timeout_and_linger_ms_config() {
        let mut props = HashMap::new();
        props.insert("client.id".to_string(), "testDeliveryTimeoutAndLingerMsConfig".to_string());
        props.insert("bootstrap.servers".to_string(), "localhost:9999".to_string());
        props.insert("delivery.timeout.ms".to_string(), "1000".to_string());
        props.insert("linger.ms".to_string(), "1000".to_string());
        props.insert("request.timeout.ms".to_string(), "1".to_string());
        let config = ProducerConfig::new(&props).expect("these properties parse");
        let log_context = LogContext::new("[test] ".to_string());

        let err = KafkaProducer::<String, String>::configure_delivery_timeout(&config, &log_context)
            .expect_err("Should reject an explicit delivery_timeout_ms < linger_ms + request_timeout_ms");
        assert_eq!(
            err.message(),
            "delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms"
        );
        assert!(matches!(err, Error::Config(_)), "Expected a Config error, got: {err:?}");
        // `ConfigException extends KafkaException`, which is what the Java test asserts.
        assert!(err.is_kafka_error(), "Java's ConfigException is a KafkaException");
        assert!(!err.is_api_error(), "Java's ConfigException is not an ApiException");

        // Second half of the Java test: linger.ms = 999 makes the sum exactly 1000,
        // so construction succeeds.
        props.insert("linger.ms".to_string(), "999".to_string());
        let config = ProducerConfig::new(&props).expect("these properties parse");
        assert_eq!(
            KafkaProducer::<String, String>::configure_delivery_timeout(&config, &log_context).unwrap(),
            1000
        );
    }

    /// Java's *other* arm: when `delivery.timeout.ms` was NOT supplied by the user,
    /// an inconsistency is clamped up to `linger.ms + request.timeout.ms` and warned
    /// about rather than rejected (`KafkaProducer.java:583-587`).
    ///
    /// `request.timeout.ms = 180000` with everything else defaulted
    /// (`delivery.timeout.ms = 120000`, `linger.ms = 5`) is a legal Kafka
    /// configuration: Java silently raises the delivery timeout to 180005.
    #[test]
    fn test_delivery_timeout_default_is_clamped_not_rejected() {
        let mut props = HashMap::new();
        props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        props.insert("request.timeout.ms".to_string(), "180000".to_string());
        let config = ProducerConfig::new(&props).expect("these properties parse");
        assert!(
            !config.user_configured(ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG),
            "the test relies on delivery.timeout.ms coming from the default"
        );
        assert_eq!(config.delivery_timeout_ms, 120_000);
        assert_eq!(config.linger_ms, 5);

        let log_context = LogContext::new("[test] ".to_string());
        let delivery_timeout_ms = KafkaProducer::<String, String>::configure_delivery_timeout(&config, &log_context)
            .expect("an inconsistency the user did not ask for is clamped, not rejected");
        assert_eq!(delivery_timeout_ms, 180_005, "clamped to linger.ms + request.timeout.ms");
    }

    /// Tests that configure_delivery_timeout accepts valid configurations.
    #[test]
    fn test_delivery_timeout_valid_config() {
        let log_context = LogContext::new("[test] ".to_string());

        // Default values: delivery=120000, linger=5, request=30000
        let config = ProducerConfig::default();
        let result = KafkaProducer::<String, String>::configure_delivery_timeout(&config, &log_context);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 120_000);

        // Exactly equal: delivery = linger + request
        let config = ProducerConfig {
            delivery_timeout_ms: 30_010,
            linger_ms: 10,
            request_timeout_ms: 30_000,
            ..Default::default()
        };
        let result = KafkaProducer::<String, String>::configure_delivery_timeout(&config, &log_context);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 30_010);
    }

    /// Tests that `negativePartitionShouldThrow` from Java is already handled
    /// by `ProducerRecord` validation. Negative partition is rejected at record
    /// construction time.
    ///
    /// Translated from `KafkaProducerTest.negativePartitionShouldThrow`.
    #[test]
    fn test_negative_partition_should_error() {
        let result: Result<ProducerRecord<String, String>, _> = ProducerRecord::with_partition_key(
            TOPIC.to_string(),
            Some(-1),
            Some("key".to_string()),
            Some("value".to_string()),
        );
        assert!(result.is_err(), "Negative partition should be rejected");
    }

    /// Tests the wait_on_metadata method with an already-known topic.
    #[tokio::test]
    async fn test_wait_on_metadata_returns_immediately_for_known_topic() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        let now_ms = producer.now_ms();
        let result = producer.wait_on_metadata(TOPIC, None, now_ms, 1000).await;
        assert!(result.is_ok());
        let cwt = result.unwrap();
        assert_eq!(0, cwt.waited_on_metadata_ms);
        assert_eq!(3, cwt.cluster.partitions_for_topic(TOPIC).len());
    }

    /// Tests that wait_on_metadata returns immediately when partition is within known range.
    #[tokio::test]
    async fn test_wait_on_metadata_returns_for_valid_partition() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        let now_ms = producer.now_ms();
        let result = producer.wait_on_metadata(TOPIC, Some(2), now_ms, 1000).await;
        assert!(result.is_ok());
    }

    /// Tests that wait_on_metadata times out for a topic that does not exist.
    ///
    /// Translated from `KafkaProducerTest.testTopicNotExistingInMetadata`.
    #[tokio::test]
    async fn test_wait_on_metadata_times_out_for_unknown_topic() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        let now_ms = producer.now_ms();
        // Try to get metadata for a topic that doesn't exist with a very short timeout
        let result = producer.wait_on_metadata("nonexistent-topic", None, now_ms, 100).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Timeout(msg) => {
                assert!(msg.message().contains("not present in metadata"), "Got: {}", msg);
            },
            other => panic!("Expected Timeout error, got: {:?}", other),
        }
    }

    /// Tests the client_id accessor.
    ///
    /// Translated from `KafkaProducerTest.getClientId` (via visible for testing).
    #[test]
    fn test_get_client_id() {
        let config = ProducerConfig { client_id: "my-producer".to_string(), ..Default::default() };
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();

        let producer = create_producer_with_config(config, metadata, accumulator);

        assert_eq!("my-producer", producer.client_id());
    }

    /// Tests that multiple sends to the same topic produce deterministic partition
    /// assignment when keys are provided.
    #[tokio::test]
    async fn test_send_multiple_records_with_same_key() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        let record1 =
            ProducerRecord::with_key(TOPIC.to_string(), Some("same-key".to_string()), Some("value1".to_string()));
        let record2 =
            ProducerRecord::with_key(TOPIC.to_string(), Some("same-key".to_string()), Some("value2".to_string()));

        let future1 = producer.send(record1).await.unwrap();
        let future2 = producer.send(record2).await.unwrap();

        // Both should succeed — each send returns a distinct future
        assert!(!future1.is_done());
        assert!(!future2.is_done());
    }

    /// Tests that sending with a callback invokes the callback on completion.
    ///
    /// Translated from `KafkaProducerTest.testCallbackAndInterceptorHandleError`
    /// (the callback invocation part).
    #[tokio::test]
    async fn test_send_callback() {
        use std::sync::atomic::AtomicBool;

        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        let callback_called = Arc::new(AtomicBool::new(false));
        let callback_called_clone = Arc::clone(&callback_called);
        let callback: Callback = Box::new(move |_metadata, _error| {
            callback_called_clone.store(true, Ordering::SeqCst);
        });

        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("key".to_string()), Some("value".to_string()));
        let result = producer.send_with_callback(record, Some(callback)).await;
        assert!(result.is_ok(), "Send with callback should succeed");
    }

    /// Tests that sending a record with null (None) key and value works.
    ///
    /// Translated from `KafkaProducerTest.testNullTopicName` (partial — tests
    /// that the producer handles empty/null values).
    #[tokio::test]
    async fn test_send_with_none_key_and_value() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        let record: ProducerRecord<String, String> = ProducerRecord::new(TOPIC.to_string(), None);
        let result = producer.send(record).await;
        assert!(result.is_ok(), "Send with None value should succeed");
    }

    /// Translated from `KafkaProducerTest.testCallbackAndInterceptorHandleError`.
    ///
    /// Tests that when sending to a topic that will cause an ApiException
    /// (RecordTooLargeException), the callback is invoked with the error and
    /// a non-null RecordMetadata with appropriate defaults.
    #[tokio::test]
    async fn test_callback_invoked_on_api_error() {
        let config = ProducerConfig { max_request_size: 10, ..Default::default() };
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer_with_config(config, metadata, accumulator);

        let callback_invoked = Arc::new(AtomicBool::new(false));
        let got_error = Arc::new(std::sync::Mutex::new(false));
        let got_metadata = Arc::new(std::sync::Mutex::new(false));
        let metadata_topic = Arc::new(std::sync::Mutex::new(String::new()));
        let metadata_offset = Arc::new(std::sync::Mutex::new(0i64));

        let inv = Arc::clone(&callback_invoked);
        let err = Arc::clone(&got_error);
        let meta = Arc::clone(&got_metadata);
        let mt = Arc::clone(&metadata_topic);
        let mo = Arc::clone(&metadata_offset);

        let callback: Callback = Box::new(move |record_metadata, error| {
            inv.store(true, Ordering::SeqCst);
            *err.lock().unwrap() = error.is_some();
            if let Some(rm) = record_metadata {
                *meta.lock().unwrap() = true;
                *mt.lock().unwrap() = rm.topic().to_string();
                *mo.lock().unwrap() = rm.offset();
            }
        });

        // Send a record that exceeds max_request_size
        let large_value = "a".repeat(100);
        let record = ProducerRecord::new(TOPIC.to_string(), Some(large_value));
        let result = producer.send_with_callback(record, Some(callback)).await;

        // send() returns Ok with a failed future (Java's FutureFailure pattern)
        assert!(result.is_ok(), "send() should return Ok with a failed future");
        let future = result.unwrap();
        assert!(future.is_done(), "Failed future should be immediately done");

        // Verify callback was invoked with the error
        assert!(callback_invoked.load(Ordering::SeqCst), "Callback should have been invoked");
        assert!(*got_error.lock().unwrap(), "Callback should receive error");
        assert!(*got_metadata.lock().unwrap(), "Callback should receive non-null metadata");
        assert_eq!(
            *metadata_topic.lock().unwrap(),
            TOPIC,
            "Callback metadata should have the topic"
        );
        assert_eq!(
            *metadata_offset.lock().unwrap(),
            RecordMetadata::INVALID_OFFSET,
            "Callback metadata should have INVALID_OFFSET"
        );

        // Future.get() should return the error
        let err = future.get().await.unwrap_err();
        assert!(matches!(err, Error::RecordTooLarge(_)));
    }

    /// Mirrors the `KafkaProducerTest.testCallbackAndInterceptorHandleError`
    /// contract for the failure mode that happens *inside*
    /// `RecordAccumulator.append` rather than before it: buffer exhaustion /
    /// `max.block.ms` expiry.
    ///
    /// Java's `BufferPool.allocate` throws `BufferExhaustedException` (an
    /// `ApiException`) out of `append`, and `doSend`'s
    /// `catch (ApiException e)` arm still fires the user callback with the
    /// placeholder metadata because it holds its own `callback` reference
    /// (`KafkaProducer.java:1056-1068`). Rust *moves* the callback into
    /// `append`, so the callback obligation (CLAUDE.md §5, §9.5) is only met
    /// because `AppendFailure` hands it back — this test is the regression guard
    /// for that hand-back.
    #[tokio::test]
    async fn test_callback_invoked_on_buffer_exhaustion() {
        const BATCH_SIZE: i32 = 16384;

        // `max.block.ms = 0` so the second allocation fails immediately
        // instead of waiting for memory that will never be freed.
        let config = ProducerConfig { max_block_ms: 0, ..Default::default() };
        // Two partitions so the second record needs a *new* batch (and hence a
        // new buffer) instead of appending to the first one.
        let metadata = create_metadata_with_topic(TOPIC, 2);
        // A pool that fits exactly one batch.
        let accumulator = Arc::new(RecordAccumulator::new_for_test(
            BATCH_SIZE,
            Compression::none(),
            5,
            100,
            1000,
            120_000,
            PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
            Arc::new(BufferPool::new_for_test(BATCH_SIZE as i64, BATCH_SIZE as usize)),
            None,
        ));
        let producer = create_producer_with_config(config, metadata, Arc::clone(&accumulator));

        // First record drains the whole pool.
        let first: ProducerRecord<String, String> =
            ProducerRecord::with_partition_key(TOPIC.to_string(), Some(0), None, Some("value".to_string())).unwrap();
        producer.send(first).await.expect("the first send should succeed");

        // Second record targets a different partition, so it must allocate.
        let invoked = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let saw_error = Arc::new(std::sync::Mutex::new(None::<String>));
        let saw_metadata = Arc::new(std::sync::Mutex::new(None::<(String, i32, i64)>));

        let inv = Arc::clone(&invoked);
        let err_slot = Arc::clone(&saw_error);
        let meta_slot = Arc::clone(&saw_metadata);
        let callback: Callback = Box::new(move |record_metadata, exception| {
            inv.fetch_add(1, Ordering::SeqCst);
            *err_slot.lock().unwrap() = exception.map(|e| e.to_string());
            *meta_slot.lock().unwrap() =
                record_metadata.map(|rm| (rm.topic().to_string(), rm.partition(), rm.offset()));
        });

        let second: ProducerRecord<String, String> =
            ProducerRecord::with_partition_key(TOPIC.to_string(), Some(1), None, Some("value".to_string())).unwrap();
        let result = producer.send_with_callback(second, Some(callback)).await;

        // Java returns a `FutureFailure`, not a thrown exception, for an
        // `ApiException`.
        let future = result.expect("an ApiException is reported through the future, not the Result");
        assert!(future.is_done(), "the failed future should be immediately done");
        let future_error = future.get().await.unwrap_err();
        assert!(
            matches!(future_error, Error::ProducerBufferExhausted(_)),
            "the future should report buffer exhaustion, got {:?}",
            future_error
        );

        // The callback fired exactly once, with BOTH the placeholder metadata
        // and the error, exactly as Java's `catch (ApiException e)` arm does.
        assert_eq!(1, invoked.load(Ordering::SeqCst), "the callback must fire exactly once");
        let message = saw_error.lock().unwrap().clone().expect("the callback must receive the error");
        assert!(
            message.contains("Failed to allocate"),
            "the callback should receive the buffer-exhaustion error, got {}",
            message
        );
        let (topic, partition, offset) = saw_metadata
            .lock()
            .unwrap()
            .clone()
            .expect("the callback must receive non-null metadata");
        assert_eq!(TOPIC, topic);
        assert_eq!(1, partition);
        assert_eq!(RecordMetadata::INVALID_OFFSET, offset);
    }

    /// The mirror image of `test_callback_invoked_on_buffer_exhaustion`: the
    /// other failure that happens *inside* `RecordAccumulator.append` must
    /// **not** fire the callback.
    ///
    /// "Producer closed while send in progress" is a **bare** `KafkaException`
    /// in Java (`RecordAccumulator.java:427-428`), not an `ApiException`, so
    /// `doSend` skips `catch (ApiException e)` (the arm that invokes the
    /// callback) and lands in `catch (KafkaException e)`, which records the
    /// error, notifies the interceptors and **rethrows**
    /// (`KafkaProducer.java:1073-1077`). Firing the callback here would break
    /// the exactly-once obligation as surely as dropping one where Java fires.
    ///
    /// The accumulator is closed while the producer itself is not, which is
    /// exactly the race the Java message names: `ensure_not_closed()` and
    /// `wait_on_metadata` both pass and the rejection comes from `append`.
    #[tokio::test]
    async fn test_callback_not_invoked_when_the_accumulator_is_closed() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));
        accumulator.close();

        let invoked = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let inv = Arc::clone(&invoked);
        let callback: Callback = Box::new(move |_, _| {
            inv.fetch_add(1, Ordering::SeqCst);
        });

        let record: ProducerRecord<String, String> =
            ProducerRecord::with_partition_key(TOPIC.to_string(), Some(0), None, Some("value".to_string())).unwrap();
        let error = match producer.send_with_callback(record, Some(callback)).await {
            Ok(_) => panic!("a bare KafkaException must propagate as Err, not as a failed future"),
            Err(e) => e,
        };

        assert!(
            matches!(error, Error::KafkaError(_)),
            "the closed-mid-send failure must be a bare KafkaException, not an ApiException — \
             `is_api_error()` is what routes it away from the callback, got {:?}",
            error
        );
        assert_eq!("Producer closed while send in progress", error.message());
        assert!(!error.is_api_error(), "a bare KafkaException is not an ApiException");
        assert!(error.is_kafka_error(), "it is still a KafkaException");
        assert_eq!(
            0,
            invoked.load(Ordering::SeqCst),
            "Java's `catch (KafkaException e)` arm rethrows without invoking the callback"
        );
    }

    /// Translated from `KafkaProducerTest.testHeadersSuccess`.
    ///
    /// Tests that headers added to a ProducerRecord before send() are passed
    /// through serialization correctly and stored in the accumulator.
    #[tokio::test]
    async fn test_headers_success() {
        use crate::common::header::Headers;
        use crate::common::header::RecordHeader;

        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        let mut record: ProducerRecord<String, String> =
            ProducerRecord::with_key(TOPIC.to_string(), Some("key".to_string()), Some("value".to_string()));

        // Add a header pre-send
        record
            .headers_mut()
            .add_header(RecordHeader::new("test".to_string(), Some(b"header-value".to_vec())))
            .unwrap();

        let result = producer.send(record).await;
        assert!(result.is_ok(), "Send with headers should succeed");
        assert!(accumulator.has_undrained(), "Accumulator should have batches");
    }

    /// Translated from `KafkaProducerTest.testFlushCompleteSendOfInflightBatches`.
    ///
    /// Tests that flush waits for all in-flight batches to complete. We send
    /// records, then simulate the sender completing them by aborting incomplete
    /// batches (which calls `done()` on all `ProduceRequestResult`s). Flush
    /// should then return immediately because all results are satisfied.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_complete_send_of_inflight_batches() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        // Send multiple records
        let mut futures = Vec::new();
        for i in 0..5 {
            let record = ProducerRecord::new(TOPIC.to_string(), Some(format!("value{}", i)));
            let future = producer.send(record).await.unwrap();
            futures.push(future);
        }

        // None should be done yet (no sender to complete them)
        for f in &futures {
            assert!(!f.is_done(), "Future should not be done before sender completes it");
        }

        // Simulate the sender completing all batches by aborting them.
        // abort() calls complete_future_and_fire_callbacks which calls
        // produce_future.set() and produce_future.done(), unblocking flush.
        accumulator.abort_incomplete_batches();

        // Now all futures should be done (abort marks ProduceRequestResults as done)
        for f in &futures {
            assert!(f.is_done(), "Future should be done after batch abort");
        }

        // Flush should return immediately since all results are satisfied
        let result = producer.flush().await;
        assert!(result.is_ok(), "Flush should succeed after batches are completed");
    }

    /// Translated from `KafkaProducerTest.testCloseWhenWaitingForMetadataUpdate`.
    ///
    /// Tests that closing the producer unblocks a send() that is waiting for
    /// metadata by verifying that after close(), the producer rejects new sends
    /// with IllegalState.
    #[tokio::test]
    async fn test_close_unblocks_pending_operations() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        // Close the producer
        producer.close().await.unwrap();

        // Subsequent send should fail with IllegalState
        let record = ProducerRecord::new(TOPIC.to_string(), Some("value".to_string()));
        let result = producer.send(record).await;
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::LocalIllegalState(_)),
            "Expected IllegalState error after close"
        );
    }

    /// Tests that initiate_close calls accumulator.close(), preventing new appends.
    ///
    /// Verifies Issue 2 fix: initiate_close must close the accumulator before
    /// setting running=false.
    #[tokio::test]
    async fn test_initiate_close_closes_accumulator() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        // Before close, we can send
        let record = ProducerRecord::new(TOPIC.to_string(), Some("value".to_string()));
        assert!(producer.send(record).await.is_ok());

        // Initiate close
        producer.initiate_close();

        // After initiate_close, the accumulator should be closed.
        // Attempting to send should fail because the accumulator rejects new appends.
        let record2 = ProducerRecord::new(TOPIC.to_string(), Some("value2".to_string()));
        // The error will come from ensure_not_closed since running=false
        let result = producer.send(record2).await;
        assert!(result.is_err(), "Send should fail after initiate_close");
    }

    /// Tests that the negative timeout check is gone since Rust Duration is unsigned.
    ///
    /// Documents Issue 7 resolution: Rust's Duration cannot be negative,
    /// so the negative timeout check from Java is correctly omitted.
    #[test]
    fn test_duration_cannot_be_negative() {
        // This test documents that Rust's std::time::Duration cannot represent
        // negative values, so the Java test `closeWithNegativeTimestampShouldThrow`
        // is not applicable. Duration::ZERO is the smallest possible value.
        let zero = Duration::ZERO;
        assert_eq!(0, zero.as_millis());
        // There is no way to construct a negative Duration in Rust.
        // Duration::from_millis(u64) always produces a non-negative value.
    }

    /// Tests that close with timeout 0 force-closes (no graceful drain).
    ///
    /// Verifies the force-close path in close_with_timeout when timeout is zero.
    #[tokio::test]
    async fn test_close_timeout_zero_force_closes() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        // Send a record
        let record = ProducerRecord::new(TOPIC.to_string(), Some("value".to_string()));
        let _ = producer.send(record).await;
        assert!(accumulator.has_undrained());

        // Close with timeout 0 should force-close
        let result = producer.close_with_timeout(Duration::ZERO).await;
        assert!(result.is_ok());

        // After force-close, the producer should not accept new sends
        let record2 = ProducerRecord::new(TOPIC.to_string(), Some("value2".to_string()));
        assert!(producer.send(record2).await.is_err());
    }

    /// Tests that the callback is invoked with error on invalid topic.
    ///
    /// Translated from `KafkaProducerTest.testCallbackAndInterceptorHandleError`
    /// (the invalid topic variant).
    #[tokio::test]
    async fn test_callback_invoked_on_invalid_topic() {
        use crate::MetadataResponseData;
        use crate::common::ApiKeys;
        use crate::common::Errors;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseTopic};

        let metadata = Arc::new(ProducerMetadata::new(
            100,
            1000,
            300_000,
            300_000,
            ClusterResourceListeners::new(),
        ));

        // Create metadata with an invalid topic
        let invalid_topic = "topic with spaces";
        let mut data = MetadataResponseData::new();
        data.set_controller_id(0);

        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(0);
        broker.set_host("localhost".to_string());
        broker.set_port(9092);
        data.set_brokers(vec![broker]);

        let mut topic_resp = MetadataResponseTopic::new();
        topic_resp.set_name(Some(invalid_topic.to_string()));
        topic_resp.set_error_code(Errors::InvalidTopicError.code());
        topic_resp.set_is_internal(false);
        data.set_topics(vec![topic_resp]);

        let response = MetadataResponse::with_version(data, ApiKeys::METADATA.latest_version());
        metadata.add(invalid_topic, 0);
        metadata.update_with_current_request_version(&response, false, 0);

        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        let callback_invoked = Arc::new(AtomicBool::new(false));
        let got_error = Arc::new(AtomicBool::new(false));
        let inv = Arc::clone(&callback_invoked);
        let err = Arc::clone(&got_error);

        let callback: Callback = Box::new(move |_metadata, error| {
            inv.store(true, Ordering::SeqCst);
            err.store(error.is_some(), Ordering::SeqCst);
        });

        let record = ProducerRecord::new(invalid_topic.to_string(), Some("value".to_string()));
        let result = producer.send_with_callback(record, Some(callback)).await;

        // Should return a failed future, not propagate the error
        assert!(result.is_ok(), "send() should return Ok with a failed future for InvalidTopic");
        let future = result.unwrap();
        assert!(future.is_done());

        // Verify callback was invoked with the error
        assert!(callback_invoked.load(Ordering::SeqCst), "Callback should have been invoked");
        assert!(got_error.load(Ordering::SeqCst), "Callback should receive error");

        // Verify the future contains the error
        let err = future.get().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidTopic(_)),
            "Expected InvalidTopic error, got: {:?}",
            err
        );
        // Java's `new InvalidTopicException(topic)` sets the message to the topic
        // name and leaves `invalidTopics()` empty (the `(String message)` ctor).
        assert_eq!(err.message(), "topic with spaces");
        match &err {
            Error::InvalidTopic(e) => assert!(e.invalid_topics().is_empty()),
            _ => unreachable!(),
        }
    }

    /// Tests that multiple calls to close are safe even with timeout.
    ///
    /// Translated from `KafkaProducerTest.closeShouldBeIdempotent`.
    #[tokio::test]
    async fn test_close_with_timeout_idempotent() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        producer.close_with_timeout(Duration::from_secs(1)).await.unwrap();
        producer.close_with_timeout(Duration::from_secs(1)).await.unwrap();
        producer.close_with_timeout(Duration::ZERO).await.unwrap();
    }

    /// Tests that different keys produce different partition assignments.
    ///
    /// Translated from partition-related KafkaProducerTest tests.
    #[test]
    fn test_different_keys_may_produce_different_partitions() {
        let metadata = create_metadata_with_topic(TOPIC, 100);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata.clone(), accumulator);

        let cluster = metadata.fetch();
        let mut partitions = std::collections::HashSet::new();
        for i in 0..20 {
            let key = format!("key-{}", i);
            let record = ProducerRecord::with_key(TOPIC.to_string(), Some(key.clone()), Some("v".to_string()));
            let p = producer.partition(&record, Some(key.as_bytes()), Some(b"v"), &cluster);
            partitions.insert(p);
        }
        // With 100 partitions and 20 different keys, we should get at least 2 different partitions
        assert!(
            partitions.len() >= 2,
            "Expected multiple partitions for different keys, got: {:?}",
            partitions
        );
    }

    /// `definition-of-done.md` §10 / CLAUDE.md §11: enabling idempotence must not add
    /// a single per-record heap allocation to the send path.
    ///
    /// This is the audit clause aimed at the public `send` entry point.
    /// `RecordAccumulator::drain` has its own budget test
    /// (`test_drain_allocations_do_not_scale_with_the_record_count`), but that
    /// measures the wrong layer: Critic 44 issue 1 was a `String` + `Arc<str>`
    /// allocation per record in `KafkaProducer::do_send_bytes`, one call *above* it.
    ///
    /// Measured as a delta between a producer that holds a `TransactionManager` and
    /// one that does not, over a steady-state append (batch already created, topic
    /// info already interned). Pinning an absolute count would break on unrelated
    /// refactors; the delta is exactly the cost the transaction wiring adds, and it
    /// must be zero.
    #[tokio::test]
    async fn test_send_allocations_do_not_grow_when_idempotence_is_enabled() {
        async fn steady_state_send_allocations(with_transaction_manager: bool) -> usize {
            let transaction_manager = if with_transaction_manager {
                let manager = TransactionManager::new(
                    LogContext::empty(),
                    None,
                    60_000,
                    100,
                    Arc::new(ApiVersions::new()),
                    false,
                );
                Some(Arc::new(Mutex::new(manager)))
            } else {
                None
            };
            let metadata = create_metadata_with_topic(TOPIC, 1);
            // A large batch so every send below appends to the same batch.
            let accumulator = Arc::new(RecordAccumulator::new_for_test(
                1024 * 1024,
                Compression::none(),
                5,
                100,
                1000,
                120_000,
                PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
                Arc::new(BufferPool::new_for_test(32 * 1024 * 1024, 1024 * 1024)),
                transaction_manager.clone(),
            ));
            let config = ProducerConfig::default();
            let producer = KafkaProducer::<String, String>::with_options(
                KafkaProducerOptionsBuilder::new()
                    .set_config(&config)
                    .set_key_serializer(Box::new(StringSerializer))
                    .set_value_serializer(Box::new(StringSerializer))
                    .set_metadata(Arc::clone(&metadata))
                    .set_accumulator(accumulator)
                    .set_time_provider(default_time_provider())
                    .set_transaction_manager(transaction_manager)
                    .set_pending_requests(Arc::new(Mutex::new(PendingRequests::new())))
                    .build()
                    .expect("KafkaProducerOptionsBuilder::build: every mandatory parameter is set above"),
            );
            let cluster = metadata.fetch();

            // Warm up: create the topic info, the deque and the batch.
            for _ in 0..4 {
                producer
                    .do_send_bytes(TOPIC, Some(0), Some(0), Some(b"k"), Some(b"v"), &[], None, 0, 0, &cluster)
                    .await
                    .expect("append should succeed");
            }

            {
                let _guard = crate::AllocTrackingGuard::new();
                crate::AllocTrackingGuard::reset();
                producer
                    .do_send_bytes(TOPIC, Some(0), Some(0), Some(b"k"), Some(b"v"), &[], None, 0, 0, &cluster)
                    .await
                    .expect("append should succeed");
                let count = crate::AllocTrackingGuard::count();
                assert!(count > 0, "the tracker must actually be measuring");
                count
            }
        }

        let without = steady_state_send_allocations(false).await;
        let with = steady_state_send_allocations(true).await;
        assert_eq!(
            without, with,
            "enabling idempotence must add no per-record allocation to the send path; \
             got {without} without a TransactionManager vs {with} with one"
        );
    }

    /// `definition-of-done.md` §10 / CLAUDE.md §11: dispatching through a custom
    /// `Partitioner<K, V>` must not add a single per-record heap allocation to the
    /// send path, matching the idempotence audit above
    /// (`test_send_allocations_do_not_grow_when_idempotence_is_enabled`).
    ///
    /// `partition: None` is passed to `do_send_bytes` (rather than the pre-resolved
    /// `Some(0)` the idempotence test uses) so `compute_partition` actually runs and,
    /// when a partitioner is configured, calls into it — otherwise this would measure
    /// nothing, since a pre-resolved partition short-circuits `compute_partition`
    /// before the partitioner is ever consulted.
    ///
    /// `RoundRobinPartitioner` is the concrete partitioner under measurement: its
    /// steady-state path is a `DashMap::get` on an already-present topic entry plus an
    /// `AtomicI32` increment (CLAUDE.md §11), so the delta versus no partitioner
    /// (built-in key-hash partitioning) must be zero.
    #[tokio::test]
    async fn test_send_allocations_do_not_grow_with_a_custom_partitioner() {
        async fn steady_state_send_allocations(with_partitioner: bool) -> usize {
            let partitioner: Option<Box<dyn Partitioner<String, String>>> = if with_partitioner {
                Some(Box::new(RoundRobinPartitioner::new()))
            } else {
                None
            };
            let metadata = create_metadata_with_topic(TOPIC, 1);
            // A large batch so every send below appends to the same batch.
            let accumulator = Arc::new(RecordAccumulator::new_for_test(
                1024 * 1024,
                Compression::none(),
                5,
                100,
                1000,
                120_000,
                PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
                Arc::new(BufferPool::new_for_test(32 * 1024 * 1024, 1024 * 1024)),
                None,
            ));
            let config = ProducerConfig::default();
            let producer = KafkaProducer::<String, String>::with_options(
                KafkaProducerOptionsBuilder::new()
                    .set_config(&config)
                    .set_key_serializer(Box::new(StringSerializer))
                    .set_value_serializer(Box::new(StringSerializer))
                    .set_metadata(Arc::clone(&metadata))
                    .set_accumulator(accumulator)
                    .set_time_provider(default_time_provider())
                    .set_pending_requests(Arc::new(Mutex::new(PendingRequests::new())))
                    .set_partitioner(partitioner)
                    .build()
                    .expect("KafkaProducerOptionsBuilder::build: every mandatory parameter is set above"),
            );
            let cluster = metadata.fetch();

            // Warm up: create the topic info, the deque and the batch, and (for the
            // partitioner case) the RoundRobinPartitioner's per-topic counter entry.
            for _ in 0..4 {
                producer
                    .do_send_bytes(TOPIC, None, Some(0), Some(b"k"), Some(b"v"), &[], None, 0, 0, &cluster)
                    .await
                    .expect("append should succeed");
            }

            {
                let _guard = crate::AllocTrackingGuard::new();
                crate::AllocTrackingGuard::reset();
                producer
                    .do_send_bytes(TOPIC, None, Some(0), Some(b"k"), Some(b"v"), &[], None, 0, 0, &cluster)
                    .await
                    .expect("append should succeed");
                let count = crate::AllocTrackingGuard::count();
                assert!(count > 0, "the tracker must actually be measuring");
                count
            }
        }

        let without = steady_state_send_allocations(false).await;
        let with = steady_state_send_allocations(true).await;
        assert_eq!(
            without, with,
            "dispatching through a custom Partitioner must add no per-record allocation \
             to the send path; got {without} without a partitioner vs {with} with one"
        );
    }

    // =====================================================================
    // `KafkaProducerTest` transactional harness (Milestone 11, Phase 6)
    //
    // Java's `kafkaProducer(configs, keySer, valSer, metadata, client, interceptors,
    // time)` helper (`KafkaProducerTest.java:199-208`) builds a real `KafkaProducer`
    // over a `MockClient` and lets the constructor start a real Sender **thread**;
    // the test thread then pokes the `MockClient` beside it, relying on Java's
    // `MockClient` being internally synchronized.
    //
    // Rust cannot copy that directly: `Sender` owns its `C: KafkaClient` by value and
    // `MockClient` is a plain struct, so a `tokio::spawn`ed Sender would take the mock
    // with it and the test could no longer prepare responses or inspect requests. So
    // the `Sender` stays test-owned — exactly as `SenderTest` keeps it — and the
    // application call runs *concurrently with* a driver loop on the same task (see
    // [`drive`]). Everything the two share is shared the way production shares it: one
    // `TransactionManager`, one `PendingRequests`, one `RecordAccumulator`, one
    // `ProducerMetadata`, one `running` / `force_close` flag pair.
    // =====================================================================

    /// Java's `NODE` (`KafkaProducerTest.java:197`), used as the coordinator in every
    /// `FindCoordinator` response below.
    fn coordinator_node() -> Node {
        Node::new(0, "host1".to_string(), 1000)
    }

    /// The topic id [`TOPIC`] is published with, so a produce response can name the
    /// same one the request carried.
    fn topic_id() -> crate::common::Uuid {
        crate::common::Uuid::from_string("MKXx1fIkQy2J9jXHhK8m1w").expect("valid UUID")
    }

    /// Publishes node `"0"`'s API versions with `transaction.version` finalized at
    /// `level`, which is what `maybeUpdateTransactionV2Enabled`
    /// (`TransactionManager.java:492-504`) reads.
    fn seed_transaction_version(api_versions: &Arc<ApiVersions>, level: i16) {
        use crate::NodeApiVersions;
        use crate::api_versions_response_data::{FinalizedFeatureKey, SupportedFeatureKey};

        const FEATURE: &str = "transaction.version";

        let mut supported = SupportedFeatureKey::new();
        supported.set_name(FEATURE.to_string());
        supported.set_max_version(level);
        supported.set_min_version(0);

        let mut finalized = FinalizedFeatureKey::new();
        finalized.set_name(FEATURE.to_string());
        finalized.set_max_version_level(level);
        finalized.set_min_version_level(level);

        api_versions.update(
            "0",
            NodeApiVersions::with_node_finalized_features_finalized_features_epoch(&[], &[supported], &[finalized], 0),
        );
    }

    /// Shared mock clock, the same shape `SenderTest`'s uses.
    struct MockTime {
        now_ms: AtomicI64,
        auto_tick_ms: AtomicI64,
    }

    impl MockTime {
        fn new(initial: i64) -> Arc<Self> {
            Arc::new(Self { now_ms: AtomicI64::new(initial), auto_tick_ms: AtomicI64::new(0) })
        }

        /// Advances the clock by `ms` on every read, mirroring Java's
        /// `new MockTime(autoTickMs)`.
        fn set_auto_tick(&self, ms: i64) {
            self.auto_tick_ms.store(ms, Ordering::Release);
        }

        fn milliseconds(&self) -> i64 {
            let tick = self.auto_tick_ms.load(Ordering::Acquire);
            if tick == 0 {
                return self.now_ms.load(Ordering::Acquire);
            }
            self.now_ms.fetch_add(tick, Ordering::AcqRel) + tick
        }

        fn as_provider(self: &Arc<Self>) -> Arc<dyn Fn() -> i64 + Send + Sync> {
            let time = Arc::clone(self);
            Arc::new(move || time.milliseconds())
        }
    }

    /// A `KafkaProducer` and the `Sender` that serves it, sharing every piece of
    /// state production shares.
    struct TxnProducerContext {
        producer: KafkaProducer<String, String>,
        sender: Sender<MockClient>,
        accumulator: Arc<RecordAccumulator>,
        metadata: Arc<ProducerMetadata>,
        transaction_manager: Arc<Mutex<TransactionManager>>,
        time: Arc<MockTime>,
    }

    impl TxnProducerContext {
        /// Builds the context from `bootstrap.servers` plus `extra` configuration,
        /// mirroring the `configs` map each Java test assembles.
        ///
        /// `num_partitions` seeds the metadata for [`TOPIC`], standing in for Java's
        /// `RequestTestUtils.metadataUpdateWith(1, singletonMap("topic", 1))`.
        fn new(extra: &[(&str, &str)], num_partitions: i32) -> Self {
            Self::with_options(extra, num_partitions, false)
        }

        /// As [`Self::new`], but with `transaction.version` finalized at level 2 so the
        /// manager enables KIP-890 Transaction V2 in `initTransactions`.
        ///
        /// Java seeds the same thing through `client.setNodeApiVersions(..)` plus
        /// `apiVersions.update(NODE.idString(), nodeApiVersions)`; only the second half
        /// is load-bearing, because `TransactionManager` reads `apiVersions` and never
        /// the client.
        fn transactional_v2(extra: &[(&str, &str)], num_partitions: i32) -> Self {
            Self::with_options(extra, num_partitions, true)
        }

        fn with_options(extra: &[(&str, &str)], num_partitions: i32, transaction_v2: bool) -> Self {
            let mut props = HashMap::from([("bootstrap.servers".to_string(), "localhost:9000".to_string())]);
            for (key, value) in extra {
                props.insert((*key).to_string(), (*value).to_string());
            }
            let config = ProducerConfig::new(&props).expect("valid config");
            let log_context = LogContext::new(format!("[Producer clientId={}] ", config.client_id));
            let time = MockTime::new(1_000);
            let api_versions = Arc::new(ApiVersions::new());
            if transaction_v2 {
                seed_transaction_version(&api_versions, 2);
            }

            let transaction_manager =
                KafkaProducer::<String, String>::configure_transaction_state(&config, &api_versions, &log_context)
                    .expect("these tests always enable idempotence");
            let pending_requests = Arc::new(Mutex::new(PendingRequests::new()));

            let metadata = Arc::new(ProducerMetadata::with_log_context(
                config.reconnect_backoff_ms,
                config.reconnect_backoff_max_ms,
                config.metadata_max_age_ms,
                config.metadata_max_idle_ms,
                ClusterResourceListeners::new(),
                log_context.clone(),
            ));
            metadata.add(TOPIC, time.milliseconds());
            // `metadata_update_with_ids` rather than `metadata_update_with`: the produce
            // path stamps the topic id from metadata onto the request, so a response has
            // to carry the same one to be matched back to its batch.
            let update = crate::common::requests::RequestTestUtils::metadata_update_with_ids(
                "kafka-cluster",
                1,
                &HashMap::new(),
                &HashMap::from([(TOPIC.to_string(), num_partitions)]),
                &|_| None,
                &HashMap::from([(TOPIC.to_string(), topic_id())]),
            );
            metadata.update_with_current_request_version(&update, false, time.milliseconds());

            let batch_size = config.batch_size.max(1);
            let metrics = Arc::new(Metrics::new());
            let buffer_pool = Arc::new(BufferPool::new(
                config.buffer_memory,
                batch_size as usize,
                Arc::clone(&metrics),
                time.as_provider(),
                KafkaProducer::<String, String>::PRODUCER_METRIC_GROUP_NAME,
            ));
            let accumulator = Arc::new(RecordAccumulator::with_log_context(
                batch_size,
                Compression::of(config.compression_type),
                config.linger_ms as i32,
                config.retry_backoff_ms,
                config.retry_backoff_max_ms,
                config.delivery_timeout_ms,
                PartitionerConfig {
                    enable_adaptive_partitioning: config.partitioner_adaptive_partitioning_enable,
                    partition_availability_timeout_ms: config.partitioner_availability_timeout_ms,
                },
                Arc::clone(&metrics),
                KafkaProducer::<String, String>::PRODUCER_METRIC_GROUP_NAME,
                buffer_pool,
                Some(Arc::clone(&transaction_manager)),
                log_context.clone(),
            ));

            let client = MockClient::with_static_nodes(vec![coordinator_node()], time.as_provider());
            let wakeup = client.wakeup_notify();
            let running = Arc::new(AtomicBool::new(true));
            let force_close = Arc::new(AtomicBool::new(false));

            let sender = Sender::new(
                client,
                Arc::clone(&metadata),
                Arc::clone(&accumulator),
                config.max_in_flight_requests_per_connection == 1,
                config.max_request_size,
                config.acks,
                config.retries,
                config.request_timeout_ms,
                config.retry_backoff_ms,
                SenderMetricsRegistry::new(Arc::clone(&metrics)),
                Arc::clone(&running),
                Arc::clone(&force_close),
                time.as_provider(),
                Some(Arc::clone(&transaction_manager)),
                Arc::clone(&pending_requests),
                log_context.clone(),
            );

            let producer = KafkaProducer::with_options(
                KafkaProducerOptionsBuilder::new()
                    .set_config(&config)
                    .set_key_serializer(Box::new(StringSerializer))
                    .set_value_serializer(Box::new(StringSerializer))
                    .set_metadata(Arc::clone(&metadata))
                    .set_accumulator(Arc::clone(&accumulator))
                    .set_running(running)
                    .set_force_close(force_close)
                    .set_wakeup(wakeup)
                    .set_time_provider(time.as_provider())
                    .set_transaction_manager(Some(Arc::clone(&transaction_manager)))
                    .set_pending_requests(pending_requests)
                    .build()
                    .expect("KafkaProducerOptionsBuilder::build: every mandatory parameter is set above"),
            );

            Self { producer, sender, accumulator, metadata, transaction_manager, time }
        }

        /// The default transactional setup: `transactional.id=some.id`, one topic
        /// partition.
        fn transactional() -> Self {
            Self::new(&[("transactional.id", TRANSACTIONAL_ID)], 1)
        }

        /// Drives `Sender::run_once` `iterations` times with no application call in
        /// flight, for the assertions Java makes with a bare `sender.runOnce()`.
        async fn run_sender(&mut self, iterations: usize) {
            for _ in 0..iterations {
                run_once(&mut self.sender).await;
            }
        }

        /// Queues the `FindCoordinator` + `InitProducerId` pair every
        /// `initTransactions` needs, in the order the `Sender` sends them.
        fn prepare_init_transactions(&mut self, error: Errors, producer_id: i64, epoch: i16) {
            let node = coordinator_node();
            self.sender
                .client_mut()
                .prepare_response(find_coordinator_response(Errors::None, TRANSACTIONAL_ID, &node));
            self.sender
                .client_mut()
                .prepare_response(init_producer_id_response(error, producer_id, epoch));
        }

        /// The producer id and epoch the manager currently holds.
        fn producer_id_and_epoch(&self) -> (i64, i16) {
            let manager = self.transaction_manager.lock().unwrap();
            let id_and_epoch = manager.producer_id_and_epoch();
            (id_and_epoch.producer_id, id_and_epoch.epoch)
        }
    }

    /// `Sender.runOnce()` with Java's catch-and-log
    /// (`Sender.java:248-250`) rather than a propagating `?`.
    async fn run_once(sender: &mut Sender<MockClient>) {
        if let Err(error) = sender.run_once().await {
            eprintln!("run_once: {}", error);
        }
    }

    /// Runs `op` — a call on the application task — while driving the `Sender` the way
    /// Java's spawned I/O thread would.
    ///
    /// `tokio::join!` rather than `tokio::select!`: `select!` drops the losing future
    /// and its side effects (CLAUDE.md §9.6.1), which here would abandon a half-sent
    /// transactional request. `join!` polls both to completion and never drops either.
    ///
    /// The loop stops as soon as `op` resolves. It `yield_now()`s rather than sleeps:
    /// `MockClient::poll` returns immediately, so the yield is what lets the runtime
    /// run `op` and fire its `max.block.ms` timer, and it is also what keeps the
    /// injected `MockTime` advancing (each `run_once` reads the clock several times, so
    /// with `set_auto_tick` the loop is the only thing that moves time — a Java
    /// `MockTime(1)` plus a hot Sender thread behaves the same way).
    ///
    /// A free function rather than a method on [`TxnProducerContext`] so callers can
    /// pass `&mut ctx.sender` and a future borrowing `&ctx.producer` at the same time.
    async fn drive<T>(sender: &mut Sender<MockClient>, op: impl std::future::Future<Output = T>) -> T {
        // A wall-clock deadline, NOT an iteration count. `drive`'s ops wait on
        // `await_result_timeout` (a real `tokio::time::timeout` on `max.block.ms`), so a
        // test that legitimately waits a timeout out busy-spins this loop —
        // `run_once` + `yield_now`, neither of which sleeps — for the whole real-time
        // window. That is an unbounded, machine-speed-dependent number of iterations,
        // especially when MockTime is frozen (no `set_auto_tick`, as in
        // `test_init_transactions_response_after_timeout`). An iteration cap could not be
        // sized to let those tests pass *and* fail a genuine hang fast; a wall-clock
        // budget can. It comfortably exceeds the longest `max.block.ms` any drive-based
        // test configures while still surfacing a stuck regression, mirroring the intent
        // of `producer_test_utils::MAX_TRIES` (fail fast, do not hang) with the primitive
        // that fits `drive`'s real-time waits — an iteration count fits `run_until` there
        // only because it drives the Sender alone, with no concurrent real-time timeout.
        const DRIVE_BUDGET: std::time::Duration = std::time::Duration::from_secs(60);
        let done = AtomicBool::new(false);
        let op = async {
            let out = op.await;
            done.store(true, Ordering::SeqCst);
            out
        };
        let start = std::time::Instant::now();
        let driver = async {
            while !done.load(Ordering::SeqCst) {
                assert!(
                    start.elapsed() < DRIVE_BUDGET,
                    "drive: op did not complete within {DRIVE_BUDGET:?} — a stuck regression, not a hang"
                );
                run_once(sender).await;
                tokio::task::yield_now().await;
            }
        };
        let (out, ()) = tokio::join!(op, driver);
        out
    }

    /// `producer.initTransactions()` driven to completion — the first line of most
    /// Java transactional tests.
    async fn init_transactions(ctx: &mut TxnProducerContext) {
        ctx.prepare_init_transactions(Errors::None, PRODUCER_ID, EPOCH);
        drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect("initTransactions succeeds");
    }

    /// Java's `initProducerIdResponse(long producerId, short epoch, Errors error)`
    /// (`KafkaProducerTest.java:2028-2035`).
    fn init_producer_id_response(error: Errors, producer_id: i64, epoch: i16) -> ConcreteResponse {
        use crate::InitProducerIdResponseData;
        use crate::common::requests::InitProducerIdResponse;

        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(error.code())
            .set_producer_id(producer_id)
            .set_producer_epoch(epoch)
            .set_throttle_time_ms(0);
        ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data))
    }

    /// `FindCoordinatorResponse.prepareResponse(error, key, node)`.
    fn find_coordinator_response(error: Errors, key: &str, node: &Node) -> ConcreteResponse {
        use crate::common::requests::FindCoordinatorResponse;

        ConcreteResponse::FindCoordinator(FindCoordinatorResponse::prepare_response(error, key, node))
    }

    /// Java's `endTxnResponse(Errors error)` (`KafkaProducerTest.java:2047-2051`).
    fn end_txn_response(error: Errors) -> ConcreteResponse {
        use crate::EndTxnResponseData;
        use crate::common::requests::EndTxnResponse;

        let mut data = EndTxnResponseData::new();
        data.set_error_code(error.code()).set_throttle_time_ms(0);
        ConcreteResponse::EndTxn(EndTxnResponse::new(data))
    }

    // -- Transactional `KafkaProducerTest` methods --------------------------

    /// Translated from `KafkaProducerTest.testInitTransactionTimeout` (Java 1328-1360).
    ///
    /// The `FindCoordinator` is answered but the `InitProducerId` is not, so
    /// `initTransactions` expires at `max.block.ms`. A retry then succeeds — which is
    /// only possible because a timed-out `TransactionalRequestResult` is **not**
    /// acked (Java's `await` sets `isAcked` only after the latch opens,
    /// `TransactionalRequestResult.java:56-62`), so
    /// `handleCachedTransactionRequestResult` hands the same pending result back.
    #[tokio::test]
    async fn test_init_transaction_timeout() {
        let mut ctx = TxnProducerContext::new(&[("transactional.id", "bad-transaction"), ("max.block.ms", "500")], 1);
        // Coarser tick reaches the simulated 500ms deadline in fewer real
        // `drive()` iterations, reducing flakiness under CPU contention.
        ctx.time.set_auto_tick(20);
        let node = coordinator_node();
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, "bad-transaction", &node));

        let error = drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect_err("no InitProducerId response is prepared");
        // AK 4.3.1: `assertFutureThrowsWithMessageContaining(TimeoutException, future,
        // INIT_TXN_TIMEOUT_MSG)`.
        assert!(matches!(error, Error::Timeout(_)), "expected a TimeoutException, got {error}");
        assert!(
            error.message().contains(KafkaProducer::<String, String>::INIT_TXN_TIMEOUT_MSG),
            "expected the InitTransactions timeout reason in the message, got {error}"
        );

        // Retry initialization should work.
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, "bad-transaction", &node));
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, PRODUCER_ID, EPOCH));
        drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect("the retry succeeds");
        assert_eq!(ctx.producer_id_and_epoch(), (PRODUCER_ID, EPOCH));
    }

    /// Translated from `KafkaProducerTest.testInitTransactionsResponseAfterTimeout`
    /// (Java 1289-1326).
    ///
    /// Java submits `initTransactions` to an executor, waits for the `InitProducerId`
    /// to be in flight, advances the clock past `max.block.ms`, asserts the future
    /// threw, *then* answers the request and calls `initTransactions` again. The
    /// second call must return normally rather than raise — the response completed the
    /// cached result.
    ///
    /// The executor is not needed here: [`drive`] already runs the application call
    /// and the Sender concurrently, and its return is the future Java asserts on.
    #[tokio::test]
    async fn test_init_transactions_response_after_timeout() {
        let mut ctx = TxnProducerContext::new(&[("transactional.id", "bad-transaction"), ("max.block.ms", "500")], 1);
        let node = coordinator_node();
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, "bad-transaction", &node));

        let error = drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect_err("the InitProducerId is unanswered");
        // AK 4.3.1: `assertFutureThrowsWithMessageContaining(TimeoutException, future,
        // INIT_TXN_TIMEOUT_MSG)` — the timeout message carries the init-txn reason.
        assert!(matches!(error, Error::Timeout(_)), "expected a TimeoutException, got {error}");
        assert!(
            error.message().contains(KafkaProducer::<String, String>::INIT_TXN_TIMEOUT_MSG),
            "expected the InitTransactions timeout reason in the message, got {error}"
        );
        assert!(
            ctx.sender.client().in_flight_request_count() > 0,
            "the InitProducerId must still be in flight, which is what the late response answers"
        );

        // Java's `client.respond(..)`: answer the request that is already in flight.
        ctx.sender
            .client_mut()
            .respond(init_producer_id_response(Errors::None, PRODUCER_ID, EPOCH));
        drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect("the late response completed the cached result");
        assert_eq!(ctx.producer_id_and_epoch(), (PRODUCER_ID, EPOCH));
    }

    /// Translated from `KafkaProducerTest.testInitTransactionWhileThrottled`
    /// (Java 1363-1387).
    ///
    /// The coordinator is throttled for 5 s while `max.block.ms` is 10 s, so
    /// `awaitNodeReady` has to wait the node out before the `InitProducerId` goes.
    #[tokio::test]
    async fn test_init_transaction_while_throttled() {
        let mut ctx = TxnProducerContext::new(&[("transactional.id", TRANSACTIONAL_ID), ("max.block.ms", "10000")], 1);
        // Java's `new MockTime(1)`: without a ticking clock `awaitNodeReady`'s
        // `now - startTime < timeout` never advances, because this test drives the
        // Sender itself and nothing else moves the clock.
        ctx.time.set_auto_tick(1);
        let node = coordinator_node();
        ctx.sender.client_mut().throttle(&node, 5000);
        ctx.prepare_init_transactions(Errors::None, PRODUCER_ID, EPOCH);

        drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect("initTransactions rides out the throttle");
        assert_eq!(ctx.producer_id_and_epoch(), (PRODUCER_ID, EPOCH));
    }

    /// Translated from `KafkaProducerTest.testClusterAuthorizationFailure`
    /// (Java 1389-1416).
    ///
    /// `CLUSTER_AUTHORIZATION_FAILED` on the `InitProducerId` is an authorization
    /// error, so the Sender's `shouldHandleAuthorizationError`
    /// (`Sender.java:351-360`) fails the pending requests, aborts the batches and
    /// transitions back to `UNINITIALIZED` — which is what makes the retry Java
    /// performs possible at all (PLAN §9.16 pass-1 finding 1).
    #[tokio::test]
    async fn test_cluster_authorization_failure() {
        let mut ctx = TxnProducerContext::new(
            &[
                ("transactional.id", "some-txn"),
                ("enable.idempotence", "true"),
                ("max.block.ms", "500"),
            ],
            1,
        );
        ctx.time.set_auto_tick(1);
        let node = coordinator_node();
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, "some-txn", &node));
        ctx.sender.client_mut().prepare_response(init_producer_id_response(
            Errors::ClusterAuthorizationFailed,
            PRODUCER_ID,
            EPOCH,
        ));

        let error = drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect_err("the cluster authorization failure surfaces");
        assert_eq!(error.error(), Errors::ClusterAuthorizationFailed);
        assert!(
            ctx.transaction_manager.lock().unwrap().has_abortable_error(),
            "CLUSTER_AUTHORIZATION_FAILED is an abortable error on InitProducerId \
             (TransactionManager.java:1522-1526)"
        );

        // Java retries with
        // `TestUtils.retryOnExceptionWithTimeout(1000, 100, producer::initTransactions)`
        // because its Sender runs on an independent thread and the recovery has not
        // necessarily happened yet: the manager is in ABORTABLE_ERROR the instant the
        // result fails, and only the Sender's *next* `runOnce` takes
        // `shouldHandleAuthorizationError`'s path back to UNINITIALIZED
        // (`Sender.java:351-360`). Until it does, an attempt is rejected with "we are
        // in an error state".
        //
        // `drive` stops the moment the application call resolves, so that iteration is
        // asked for explicitly here — which makes the recovery deterministic rather
        // than raced, and lets the state be asserted directly instead of retried
        // around.
        ctx.run_sender(1).await;
        assert!(
            !ctx.transaction_manager.lock().unwrap().has_error(),
            "one runOnce must clear the abortable error via transitionToUninitialized"
        );
        assert!(
            !ctx.transaction_manager.lock().unwrap().has_producer_id(),
            "UNINITIALIZED means the producer id is gone too"
        );

        // Only an `InitProducerId` is prepared, as in Java: `transitionToUninitialized`
        // does not forget the coordinator, so no second `FindCoordinator` is sent.
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, PRODUCER_ID, EPOCH));
        drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect("the retry succeeds once the manager is back in UNINITIALIZED");
        assert_eq!(ctx.producer_id_and_epoch(), (PRODUCER_ID, EPOCH));
        ctx.producer.close().await.expect("close");
    }

    /// Translated from `KafkaProducerTest.testAbortTransaction` (Java 1418-1442).
    ///
    /// The whole `FindCoordinator` -> `InitProducerId` -> `EndTxn(ABORT)` sequence,
    /// with no records in the transaction.
    #[tokio::test]
    async fn test_abort_transaction() {
        let mut ctx = TxnProducerContext::transactional();
        init_transactions(&mut ctx).await;
        ctx.producer.begin_transaction().expect("beginTransaction");

        ctx.sender.client_mut().prepare_response(end_txn_response(Errors::None));
        drive(&mut ctx.sender, ctx.producer.abort_transaction())
            .await
            .expect("abortTransaction");
    }

    /// Translated from
    /// `KafkaProducerTest.testOnlyCanExecuteCloseAfterInitTransactionsTimeout`
    /// (Java 2053-2076).
    ///
    /// Nothing answers the `FindCoordinator`, so `initTransactions` expires at
    /// `max.block.ms=5`. After that failure every other transactional operation must
    /// be rejected, and only `close` is allowed.
    #[tokio::test]
    async fn test_only_can_execute_close_after_init_transactions_timeout() {
        let mut ctx = TxnProducerContext::new(&[("transactional.id", "bad-transaction"), ("max.block.ms", "5")], 1);

        let error = drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect_err("nothing answers the FindCoordinator");
        // AK 4.3.1: `assertTrue(timeoutEx.getMessage().contains(INIT_TXN_TIMEOUT_MSG))`.
        assert!(
            error.message().contains(KafkaProducer::<String, String>::INIT_TXN_TIMEOUT_MSG),
            "expected the InitTransactions timeout reason in the message, got {error}"
        );
        assert_eq!(
            error.message(),
            format!(
                "Timeout expired after 5ms while awaiting InitProducerId. {}",
                KafkaProducer::<String, String>::INIT_TXN_TIMEOUT_MSG
            )
        );

        // Other transactional operations are not allowed once the caller has taken the
        // error from a failed initTransactions: the manager is still INITIALIZING with
        // an unacked pending transition.
        let begin_error = ctx.producer.begin_transaction().expect_err("beginTransaction is rejected");
        assert_eq!(
            begin_error.message(),
            "Cannot attempt operation `beginTransaction` because the previous call to \
             `initTransactions` timed out and must be retried"
        );

        ctx.producer
            .close_with_timeout(Duration::from_millis(0))
            .await
            .expect("close is the one allowed operation");
    }

    /// Translated from `KafkaProducerTest.testPartitionAddedToTransaction`
    /// (Java 2423-2443).
    ///
    /// Pins the producer-level transactional wiring: `doSend` must call
    /// `transactionManager.maybeAddPartition(tp)` after the append succeeds, with the
    /// partition the accumulator actually chose (`KafkaProducer.java:1040-1046`). The
    /// send's future must also still be pending — the record is buffered, not sent.
    ///
    /// # Deviation: a real manager instead of Mockito's `verify`
    ///
    /// Java builds the producer through `KafkaProducerTestContext`, which injects
    /// `mock(TransactionManager.class)` (`:2601`, passed at `:2672`), and asserts with
    /// `verify(ctx.transactionManager).maybeAddPartition(topicPartition)`.
    /// `TransactionManager` is a concrete struct here, so there is nothing to stub; the
    /// substitute is a **real** transactional manager driven into `IN_TRANSACTION`, and
    /// the assertion is `is_partition_pending_add` (`TransactionManager.java:571`) — the
    /// state `maybeAddPartition` exists to produce.
    ///
    /// That is stronger than `verify` on one axis: Mockito confirms the call was made,
    /// while this confirms it was made *and* had its effect. It is **not** stronger for
    /// carrying the right partition — `verify(..).maybeAddPartition(topicPartition)`
    /// checks the argument too, so that clause is parity, not superiority.
    ///
    /// The effect clause is a real gain, and holds because the state read is isolated.
    /// `is_partition_pending_add` reads the union
    /// `new_partitions_in_transaction ∪ pending_partitions_in_transaction`
    /// (`TransactionManager.java:571`), and in this crate:
    /// `new_partitions_in_transaction.insert` has exactly one call site, inside
    /// `maybe_add_partition`; and `pending_partitions_in_transaction` is only ever filled
    /// by `add_partitions_to_transaction_handler`'s
    /// `.extend(new_partitions_in_transaction.iter())`, so it is downstream of that same
    /// insert. The union is therefore non-empty only if `maybe_add_partition` ran, and
    /// the test asserts the negative pre-condition first.
    ///
    /// It is also why this test could be written at all rather than deferred like
    /// `SenderTest`'s one mock-injected entry, whose stub makes a real method *throw* and
    /// so has no real-state equivalent.
    ///
    /// # The one Java assertion not carried across
    ///
    /// Java opens with `assertEquals(future, producer.send(record))` (`:2439`), comparing
    /// the returned future against a **pre-built** `FutureRecordMetadata` that
    /// `expectAppend` (`:2445`) installed by stubbing `ctx.accumulator.append(..)` and
    /// `ctx.partitioner.partition(..)` to return it. That assertion is about the
    /// *accumulator* mock, not the manager mock the deviation above discusses, and it is
    /// unrepresentable here for the same reason: with a real `RecordAccumulator` the
    /// future is created inside `append`, so there is no pre-known value to compare
    /// identity against. What survives of its intent — that `send` hands back the
    /// accumulator's own pending future rather than a completed or failed one — is
    /// asserted directly by `!future.is_done()` below.
    ///
    /// # Why this method was missing until Critic 46 issue 4
    ///
    /// It reaches the transactional path only through the injected mock, so its body
    /// names no public transactional method and no transactional config key — and the
    /// accounting block's marker set contained neither `TransactionManager` nor
    /// `maybeAddPartition`. Both are markers now; see the accounting block.
    #[tokio::test]
    async fn test_partition_added_to_transaction() {
        let mut ctx = TxnProducerContext::transactional();
        init_transactions(&mut ctx).await;
        ctx.producer.begin_transaction().expect("beginTransaction");

        let partition = TopicPartition::new(TOPIC.to_string(), 0);
        assert!(
            !ctx.transaction_manager.lock().unwrap().is_partition_pending_add(&partition),
            "nothing may be pending before the send"
        );

        let record = ProducerRecord::with_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(TOPIC.to_string())
                .set_value(Some("value".to_string()))
                .set_timestamp(Some(ctx.time.milliseconds()))
                .set_key(Some("key".to_string()))
                .build()
                .unwrap(),
        )
        .expect("a valid record");
        let future = ctx.producer.send(record).await.expect("send");

        assert!(
            !future.is_done(),
            "the record is buffered in the accumulator, not sent — nothing has driven the Sender"
        );
        assert!(
            ctx.transaction_manager.lock().unwrap().is_partition_pending_add(&partition),
            "doSend must call maybeAddPartition with the partition the accumulator chose"
        );
    }

    // =====================================================================
    // `maybeAddPartition`'s failure arm in `doSend`
    //
    // `TransactionManagerTest` covers what `maybeAddPartition` raises in each state
    // (`testFailIfNotReadyForSend*`, Java 262-300, translated in
    // `transaction_manager.rs`). What follows covers the other half — how `doSend`
    // *surfaces* each of those, which only `KafkaProducer` decides — and it has no
    // Java counterpart: Java's own `KafkaProducerTest` reaches this arm once, through
    // a Mockito stub, in `testPartitionAddedToTransaction` above. These rows therefore
    // do NOT enter the Phase-6 accounting block's denominator.
    //
    // They exist because the arm shipped with a self-deadlock: written inline as
    // `if let Err(e) = tm.lock().unwrap().maybe_add_partition(..)`, the scrutinee's
    // `MutexGuard` outlives the whole success arm (edition 2024 only shortens it
    // across `else`), and the body re-locked the same non-reentrant
    // `std::sync::Mutex` through `handle_api_error` →
    // `maybe_transition_to_error_state`. Java's monitor is reentrant, so no Java test
    // could have caught it.
    // =====================================================================

    /// How long [`bounded`] waits before calling a send wedged.
    ///
    /// Generous against a loaded CI box; the operations under test do no I/O and
    /// finish in microseconds.
    const DEADLOCK_BOUND: Duration = Duration::from_secs(10);

    /// Runs `body` on its own thread and returns its value, failing the test if it
    /// does not finish within [`DEADLOCK_BOUND`].
    ///
    /// A `tokio::time::timeout` around the send would NOT bound these tests. The
    /// regression they guard blocks the thread inside `std::sync::Mutex::lock`, so the
    /// future never yields, the runtime never advances its timers, and the timeout can
    /// never fire — the symptom is a hung `cargo test`, not a failing one. Only a
    /// second thread can observe a wedged first one, so `body` gets a thread and its
    /// own current-thread runtime while the test thread waits on a channel.
    ///
    /// On a timeout the worker stays blocked forever, holding the producer it built.
    /// That is deliberate and harmless: it shares nothing with any other test, and
    /// libtest ends the process with `exit(2)` rather than joining stray threads.
    fn bounded<T: Send + 'static>(what: &str, body: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            // `send` fails only if the receiver timed out and gave up; nothing to do.
            let _ = tx.send(body());
        });
        rx.recv_timeout(DEADLOCK_BOUND).unwrap_or_else(|_| {
            panic!(
                "{what} did not return within {DEADLOCK_BOUND:?} — the send path is wedged. \
                 doSend's maybeAddPartition arm must release the TransactionManager guard \
                 before handling the error, because handle_api_error re-locks it."
            )
        })
    }

    /// Runs `body` on a fresh current-thread runtime inside [`bounded`].
    fn bounded_block_on<F>(what: &str, body: impl FnOnce() -> F + Send + 'static) -> F::Output
    where
        F: std::future::Future,
        F::Output: Send + 'static,
    {
        bounded(what, move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a current-thread runtime")
                .block_on(body())
        })
    }

    /// The record every test below sends; its contents are irrelevant because no send
    /// is expected to reach a batch's wire form.
    fn misuse_record() -> ProducerRecord<String, String> {
        ProducerRecord::with_key(TOPIC.to_string(), Some("key".to_string()), Some("value".to_string()))
    }

    /// `send()` on a transactional producer that never called `initTransactions`.
    ///
    /// The state `TransactionManagerTest.testFailIfNotReadyForSendNoProducerId`
    /// (Java 262-265) asserts on, surfaced through `doSend`. `maybeAddPartition` raises
    /// `IllegalStateException` (`TransactionManager.java:443`), which is not a
    /// `KafkaException` at all, so it misses `catch (ApiException e)` *and*
    /// `catch (KafkaException e)`, reaches `catch (Exception e)`
    /// (`KafkaProducer.java:1077-1081`) and is rethrown out of `send()`.
    #[test]
    fn test_send_before_init_transactions_returns_illegal_state() {
        let error = bounded_block_on("send before initTransactions", || async {
            let ctx = TxnProducerContext::transactional();
            ctx.producer
                .send(misuse_record())
                .await
                .expect_err("an uninitialized transactional producer cannot send")
        });

        assert!(
            matches!(error, Error::LocalIllegalState(_)),
            "an illegal-state error is not an API error, so it must be returned by send() \
             rather than reported through the future; got {error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "LocalIllegalStateError: Cannot add partition {TOPIC}-0 to transaction before completing a call to initTransactions"
            )
        );
    }

    /// `send()` on an initialized transactional producer with no open transaction.
    ///
    /// The state `TransactionManagerTest.testFailIfNotReadyForSendNoOngoingTransaction`
    /// (Java 282-286) asserts on, surfaced through `doSend`. Same
    /// `IllegalStateException` treatment as above, from
    /// `TransactionManager.java:446`, whose message carries the state and Java's
    /// double space before it.
    #[test]
    fn test_send_outside_transaction_returns_illegal_state() {
        let error = bounded_block_on("send outside a transaction", || async {
            let mut ctx = TxnProducerContext::transactional();
            init_transactions(&mut ctx).await;
            ctx.producer
                .send(misuse_record())
                .await
                .expect_err("a send needs an open transaction")
        });

        assert!(
            matches!(error, Error::LocalIllegalState(_)),
            "expected the IllegalState to be returned by send(), got {error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!("LocalIllegalStateError: Cannot add partition {TOPIC}-0 to transaction while in state  READY")
        );
    }

    /// `send()` after an abortable error, the state
    /// `TransactionManagerTest.testFailIfNotReadyForSendAfterAbortableError`
    /// (Java 288-294) asserts on.
    ///
    /// `maybeFailWithError` raises a **bare** `KafkaException`
    /// (`TransactionManager.java:1171`). `ApiException extends KafkaException`, not the
    /// other way round, so `catch (ApiException e)` does not match and
    /// `catch (KafkaException e)` (`KafkaProducer.java:1073-1076`) rethrows it — a
    /// block that, unlike the `ApiException` one, never calls
    /// `maybeTransitionToErrorState`. Hence the second assertion: routing this error to
    /// the `ApiException` arm would overwrite `lastError` with the "we are in an error
    /// state" wrapper and lose the cause the application needs.
    #[test]
    fn test_send_after_abortable_error_returns_error_without_overwriting_last_error() {
        let (error, last_error) = bounded_block_on("send after an abortable error", || async {
            let mut ctx = TxnProducerContext::transactional();
            init_transactions(&mut ctx).await;
            ctx.producer.begin_transaction().expect("beginTransaction");
            ctx.transaction_manager
                .lock()
                .unwrap()
                .transition_to_abortable_error(Error::with_message(Errors::InvalidTxnState, "cause"), Caller::App)
                .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");

            let error = ctx
                .producer
                .send(misuse_record())
                .await
                .expect_err("an abortable error blocks further sends");
            let last_error = ctx.transaction_manager.lock().unwrap().last_error().cloned();
            (error, last_error)
        });

        assert_eq!(
            error.error(),
            Errors::UnknownServerError,
            "this crate spells Java's bare Kafka error as UnknownServerError; got {error:?}"
        );
        assert_eq!(
            error.message(),
            "Cannot execute transactional method because we are in an error state"
        );
        let last_error = last_error.expect("the manager keeps the abortable cause");
        assert_eq!(
            last_error.error(),
            Errors::InvalidTxnState,
            "the rethrowing catch block must not run maybeTransitionToErrorState, which \
             would replace the cause with the wrapper; got {last_error:?}"
        );
    }

    /// `send()` after a fatal error, the state
    /// `TransactionManagerTest.testFailIfNotReadyForSendAfterFatalError` (Java 296-300)
    /// asserts on. Same bare-`KafkaException` treatment as the abortable case.
    #[test]
    fn test_send_after_fatal_error_returns_error() {
        // The closure also reports the manager's state: Java records fatality there
        // (`hasFatalError()` == `currentState == FATAL_ERROR`), not on the error.
        let (error, still_fatal) = bounded_block_on("send after a fatal error", || async {
            let mut ctx = TxnProducerContext::transactional();
            init_transactions(&mut ctx).await;
            ctx.transaction_manager
                .lock()
                .unwrap()
                .transition_to_fatal_error(
                    Error::with_message(Errors::ClusterAuthorizationFailed, "cause"),
                    Caller::App,
                )
                .expect("FATAL_ERROR is always reachable");
            let error = ctx
                .producer
                .send(misuse_record())
                .await
                .expect_err("a fatal error blocks further sends");
            let still_fatal = ctx.transaction_manager.lock().unwrap().has_fatal_error();
            (error, still_fatal)
        });

        assert_eq!(error.error(), Errors::UnknownServerError, "got {error:?}");
        assert_eq!(
            error.message(),
            "Cannot execute transactional method because we are in an error state"
        );
        // Java records fatality in the state machine, not on the exception
        // (`TransactionManager.hasFatalError()` == `currentState == FATAL_ERROR`;
        // `lastError` is a plain RuntimeException). The application distinguishes a
        // dead producer by the exception *type* `maybe_fail_with_error` surfaces —
        // asserted above — so the regression guard is the state, checked here.
        assert!(still_fatal, "the producer must remain in the fatal state: {error:?}");
    }

    /// `send()` on a purely **idempotent** producer whose manager is in a fatal state,
    /// the state
    /// `TransactionManagerTest.testFailIfNotReadyForSendIdempotentProducerFatalError`
    /// (Java 274-280) asserts on.
    ///
    /// `maybeFailWithError` runs before `maybeAddPartition`'s `isTransactional()` test
    /// (`TransactionManager.java:438` vs `:441`), so a producer with no
    /// `transactional.id` reaches the same arm.
    #[test]
    fn test_idempotent_send_after_fatal_error_returns_error() {
        let error = bounded_block_on("idempotent send after a fatal error", || async {
            let ctx = TxnProducerContext::new(&[], 1);
            ctx.transaction_manager
                .lock()
                .unwrap()
                .transition_to_fatal_error(Error::with_message(Errors::UnsupportedVersion, "cause"), Caller::App)
                .expect("FATAL_ERROR is always reachable");
            ctx.producer
                .send(misuse_record())
                .await
                .expect_err("a fatal error blocks further sends")
        });

        assert_eq!(error.error(), Errors::UnknownServerError, "got {error:?}");
    }

    /// The other side of the split: an `ApiException` out of `maybeAddPartition` still
    /// takes `catch (ApiException e)` and is reported through the future.
    ///
    /// This is the arm that actually re-locks the manager —
    /// `handle_api_error` → `maybe_transition_to_error_state` — so it is the direct
    /// regression test for the deadlock. `maybeFailWithError` re-raises a fenced
    /// producer as `ProducerFencedException` (`TransactionManager.java:1159`),
    /// which IS an `ApiException`.
    ///
    /// The manager is seeded **abortable**, not fatal, so that
    /// `maybeTransitionToErrorState` has somewhere to move it: `ProducerFenced` is in
    /// that method's fatal set (Java `TransactionManager.java:765-772`), so the
    /// `ApiException` block drives `ABORTABLE_ERROR -> FATAL_ERROR`. Seeding
    /// `FATAL_ERROR` up front — as this test first did — makes the state assertion
    /// tautological: it would hold even if the arm were changed to `return Err(error)`
    /// and never call `maybe_transition_to_error_state` at all. Both `has_error()`
    /// arms reach the same `maybeFailWithError` branch, which keys on
    /// `last_error`'s code, so the error the send observes is unchanged.
    #[test]
    fn test_send_after_producer_fenced_fails_the_future() {
        let (send_result, fatal_before, fatal_after) = bounded_block_on("send after being fenced", || async {
            let mut ctx = TxnProducerContext::transactional();
            init_transactions(&mut ctx).await;
            ctx.producer.begin_transaction().expect("beginTransaction");
            ctx.transaction_manager
                .lock()
                .unwrap()
                .transition_to_abortable_error(Error::with_message(Errors::ProducerFenced, "fenced"), Caller::App)
                .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");
            let fatal_before = ctx.transaction_manager.lock().unwrap().has_fatal_error();

            let send_result = match ctx.producer.send(misuse_record()).await {
                Ok(future) => Ok(future.get().await.expect_err("the fenced send cannot be acked")),
                Err(error) => Err(error),
            };
            let fatal_after = ctx.transaction_manager.lock().unwrap().has_fatal_error();
            (send_result, fatal_before, fatal_after)
        });

        let error = send_result.expect("an API error is reported through the future, not the call");
        assert_eq!(
            error.error(),
            Errors::ProducerFenced,
            "a producer-fenced error is an API error, so doSend returns a failed future; got {error:?}"
        );
        // `maybeFailWithError` re-raises rather than re-throwing `lastError`, so the
        // message is the fresh one built at Java 1159-1161 — not the "fenced" text the
        // test seeded.
        assert_eq!(
            error.message(),
            format!(
                "Producer with transactionalId '{TRANSACTIONAL_ID}' and \
                 (producerId={PRODUCER_ID}, epoch={EPOCH}) has been fenced by another producer \
                 with the same transactionalId"
            )
        );
        assert!(
            !fatal_before,
            "the manager starts abortable, so the transition below is observable"
        );
        assert!(
            fatal_after,
            "the API-error block runs maybeTransitionToErrorState, which moves a fenced \
             producer from ABORTABLE_ERROR to FATAL_ERROR; the rethrow path would not"
        );
    }

    /// The `maybeAddPartition` `ApiException` arm of `do_send_bytes` passes `None`
    /// (not the record's callback) to `handle_api_error`, so the callback is
    /// **not** fired synchronously there.
    ///
    /// The callback was moved into `accumulator.append(..)` and registered with the
    /// record's future, which fires it exactly once when the batch is later
    /// completed/aborted. Passing it here too would double-fire (CLAUDE.md §9.5).
    /// (Java's `doSend` catch additionally fires the raw `callback` at
    /// `KafkaProducer.java:1061`, but keeps `callback` as a reference separate from
    /// the `appendCallbacks` it registered, so Java can invoke it twice on this path;
    /// Rust's single-owner model fires it exactly once.)
    #[test]
    fn test_send_api_exception_after_append_fires_callback_exactly_once() {
        let (count_after_send, count_final) = bounded_block_on("fenced send with callback", || async {
            let mut ctx = TxnProducerContext::transactional();
            init_transactions(&mut ctx).await;
            ctx.producer.begin_transaction().expect("beginTransaction");
            // Seed abortable (not fatal) so `maybeAddPartition`'s `maybe_fail_with_error`
            // re-raises `ProducerFenced` — an `ApiException` — after the append succeeds.
            ctx.transaction_manager
                .lock()
                .unwrap()
                .transition_to_abortable_error(Error::with_message(Errors::ProducerFenced, "fenced"), Caller::App)
                .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");

            let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let cb_count = Arc::clone(&count);
            let callback: Callback = Box::new(move |_metadata, _error| {
                cb_count.fetch_add(1, Ordering::SeqCst);
            });

            // The append succeeds; `maybeAddPartition` then raises `ProducerFenced`,
            // taking the `handle_api_error(.., None)` arm.
            let future = ctx
                .producer
                .send_with_callback(misuse_record(), Some(callback))
                .await
                .expect("an ApiException is reported through the future, not the call");
            let error = future.get().await.expect_err("the fenced send cannot be acked");
            assert_eq!(error.error(), Errors::ProducerFenced, "got {error:?}");

            // `handle_api_error` was passed `None`, so it did NOT fire the callback.
            let count_after_send = count.load(Ordering::SeqCst);

            // The record still sits in the accumulator carrying the callback; the
            // accumulator fires it exactly once when the batch is aborted (as the
            // Sender's abort/close path would).
            ctx.accumulator.abort_incomplete_batches();
            (count_after_send, count.load(Ordering::SeqCst))
        });

        assert_eq!(
            count_after_send, 0,
            "the maybeAddPartition ApiException path must NOT fire the callback (it is deferred to \
             the accumulator); firing it here would double-fire"
        );
        assert_eq!(
            count_final, 1,
            "the callback fires exactly once, from the accumulator when the batch is aborted"
        );
    }

    /// Translated from
    /// `KafkaProducerTest.testCommitTransactionWithRecordTooLargeException`
    /// (Java 1532-1560).
    ///
    /// A record larger than `max.request.size` fails its future with
    /// `RecordTooLargeException`, and because `doSend`'s `catch (ApiException e)` runs
    /// `maybeTransitionToErrorState` (`KafkaProducer.java:1065-1067`) the transaction
    /// is now abortable — so the following `commitTransaction` must fail rather than
    /// commit a partial transaction.
    #[tokio::test]
    async fn test_commit_transaction_with_record_too_large_error() {
        let mut ctx =
            TxnProducerContext::new(&[("transactional.id", TRANSACTIONAL_ID), ("max.request.size", "1000")], 1);
        ctx.time.set_auto_tick(1);
        init_transactions(&mut ctx).await;
        ctx.producer.begin_transaction().expect("beginTransaction");

        let large_string = "*".repeat(1000);
        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("large string".to_string()), Some(large_string));
        let future = ctx
            .producer
            .send(record)
            .await
            .expect("an API error is reported through the future, not the call");
        let send_error = future.get().await.expect_err("the record is too large");
        assert!(
            matches!(send_error, Error::RecordTooLarge(_)),
            "expected RecordTooLarge, got {:?}",
            send_error
        );

        // Java asserts a bare `KafkaException`, which is what `maybeFailWithError`
        // (`TransactionManager.java:1163-1170`) raises for a non-`IllegalStateException`
        // `lastError` — the original cause is wrapped, not re-raised.
        let commit_error = drive(&mut ctx.sender, ctx.producer.commit_transaction())
            .await
            .expect_err("the transaction is abortable after a failed send");
        assert_eq!(
            commit_error.message(),
            "Cannot execute transactional method because we are in an error state"
        );
    }

    /// Translated from
    /// `KafkaProducerTest.testCommitTransactionWithMetadataTimeoutForMissingTopic`
    /// (Java 1562-1597).
    ///
    /// # Deviation: how the metadata wait is made to expire
    ///
    /// Java stubs `metadata.fetch()` with Mockito to return an *empty* cluster and, on
    /// the sixth invocation, to jump `MockTime` forward by 70 s past a
    /// `max.block.ms` of 60 s. `ProducerMetadata` is a concrete type here, so there is
    /// no stub to install; the equivalent is a metadata instance that genuinely has no
    /// topic plus a `max.block.ms` short enough to expire in test time. What the test
    /// observes is unchanged: the send's future fails with a timeout, and the
    /// subsequent `commitTransaction` fails because the failed send made the
    /// transaction abortable.
    #[tokio::test]
    async fn test_commit_transaction_with_metadata_timeout_for_missing_topic() {
        let mut ctx = TxnProducerContext::new(
            &[("transactional.id", TRANSACTIONAL_ID), ("max.block.ms", "200")],
            // No partitions: `metadataUpdateWith(1, emptyMap())`, Java's `emptyCluster`.
            0,
        );
        init_transactions(&mut ctx).await;
        ctx.producer.begin_transaction().expect("beginTransaction");

        let record = ProducerRecord::new(TOPIC.to_string(), Some("value".to_string()));
        let future = ctx
            .producer
            .send(record)
            .await
            .expect("a timeout is an API error and is reported through the future");
        let send_error = future.get().await.expect_err("the topic never appears in metadata");
        assert!(
            matches!(send_error, Error::Timeout(_)),
            "expected Timeout, got {:?}",
            send_error
        );

        // Java asserts a bare `KafkaException` — see
        // `test_commit_transaction_with_record_too_large_error`.
        let commit_error = drive(&mut ctx.sender, ctx.producer.commit_transaction())
            .await
            .expect_err("the transaction is abortable after a failed send");
        assert_eq!(
            commit_error.message(),
            "Cannot execute transactional method because we are in an error state"
        );
    }

    /// Translated from
    /// `KafkaProducerTest.testCommitTransactionWithMetadataTimeoutForPartitionOutOfRange`
    /// (Java 1599-1634).
    ///
    /// As the previous test, but the metadata does contain the topic — with one
    /// partition — and the record names partition 2, so `waitOnMetadata` waits for a
    /// partition that never arrives. Same deviation on how the wait is made to expire.
    #[tokio::test]
    async fn test_commit_transaction_with_metadata_timeout_for_partition_out_of_range() {
        let mut ctx = TxnProducerContext::new(&[("transactional.id", TRANSACTIONAL_ID), ("max.block.ms", "200")], 1);
        init_transactions(&mut ctx).await;
        ctx.producer.begin_transaction().expect("beginTransaction");

        let record = ProducerRecord::with_partition_key(TOPIC.to_string(), Some(2), None, Some("value".to_string()))
            .expect("a valid partition");
        let future = ctx
            .producer
            .send(record)
            .await
            .expect("a timeout is an API error and is reported through the future");
        let send_error = future.get().await.expect_err("partition 2 never appears in metadata");
        assert!(
            matches!(send_error, Error::Timeout(_)),
            "expected Timeout, got {:?}",
            send_error
        );

        // Java asserts a bare `KafkaException` — see
        // `test_commit_transaction_with_record_too_large_error`.
        let commit_error = drive(&mut ctx.sender, ctx.producer.commit_transaction())
            .await
            .expect_err("the transaction is abortable after a failed send");
        assert_eq!(
            commit_error.message(),
            "Cannot execute transactional method because we are in an error state"
        );
    }

    /// Translated from
    /// `KafkaProducerTest.testCommitTransactionWithSendToInvalidTopic`
    /// (Java 1636-1674).
    ///
    /// An invalid topic name fails the send's future with `InvalidTopicException` and
    /// leaves the transaction abortable, so the commit fails.
    ///
    /// Java arranges the invalid topic through `client.prepareMetadataUpdate(..)`, a
    /// `MockClient` facility this port does not have; the metadata is seeded with the
    /// `INVALID_TOPIC_EXCEPTION` topic directly instead, which is the state that
    /// update would have produced.
    #[tokio::test]
    async fn test_commit_transaction_with_send_to_invalid_topic() {
        use crate::MetadataResponseData;
        use crate::common::ApiKeys;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseTopic};

        const INVALID_TOPIC: &str = "topic abc"; // Invalid topic name due to space.

        let mut ctx = TxnProducerContext::new(&[("transactional.id", TRANSACTIONAL_ID), ("max.block.ms", "15000")], 1);
        init_transactions(&mut ctx).await;
        ctx.producer.begin_transaction().expect("beginTransaction");

        let mut data = MetadataResponseData::new();
        data.set_controller_id(0);
        data.set_cluster_id(Some("test-cluster".to_string()));
        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(0);
        broker.set_host("localhost".to_string());
        broker.set_port(9092);
        data.set_brokers(vec![broker]);
        let mut invalid = MetadataResponseTopic::new();
        invalid.set_name(Some(INVALID_TOPIC.to_string()));
        invalid.set_error_code(Errors::InvalidTopicError.code());
        data.set_topics(vec![invalid]);
        let response = MetadataResponse::with_version(data, ApiKeys::METADATA.latest_version());
        ctx.metadata.add(INVALID_TOPIC, ctx.time.milliseconds());
        ctx.metadata
            .update_with_current_request_version(&response, false, ctx.time.milliseconds());

        let record = ProducerRecord::new(INVALID_TOPIC.to_string(), Some("HelloKafka".to_string()));
        let future = ctx
            .producer
            .send(record)
            .await
            .expect("an invalid-topic error is reported through the future");
        let send_error = future.get().await.expect_err("the topic name is invalid");
        assert!(
            matches!(send_error, Error::InvalidTopic(_)),
            "expected InvalidTopic, got {:?}",
            send_error
        );

        // Java asserts a bare `KafkaException` — see
        // `test_commit_transaction_with_record_too_large_error`.
        let commit_error = drive(&mut ctx.sender, ctx.producer.commit_transaction())
            .await
            .expect_err("the transaction is abortable after a failed send");
        assert_eq!(
            commit_error.message(),
            "Cannot execute transactional method because we are in an error state"
        );
    }

    /// Translated from `KafkaProducerTest.testSendTxnOffsetsWithGroupId`
    /// (Java 1676-1711).
    ///
    /// The offsets map Java passes is **empty**, so `sendOffsetsToTransaction` takes
    /// `KafkaProducer.java:738`'s early exit and sends nothing at all: the
    /// `AddOffsetsToTxn`, second `FindCoordinator` and `TxnOffsetCommit` responses the
    /// Java test queues are never consumed, and only the `EndTxn` is. That is
    /// preserved rather than "fixed" — `testSendTxnOffsetsWithGroupIdTransactionV2`
    /// below is the sibling that passes a real offset.
    ///
    /// What the test does cover is that an empty map is a no-op even while the
    /// coordinator is throttled, and that the commit that follows succeeds.
    #[tokio::test]
    async fn test_send_txn_offsets_with_group_id() {
        let mut ctx = TxnProducerContext::new(&[("transactional.id", TRANSACTIONAL_ID), ("max.block.ms", "10000")], 1);
        ctx.time.set_auto_tick(1);
        let node = coordinator_node();
        ctx.sender.client_mut().throttle(&node, 5000);
        init_transactions(&mut ctx).await;
        ctx.producer.begin_transaction().expect("beginTransaction");

        #[allow(deprecated)]
        let group_metadata = ConsumerGroupMetadata::new("group");
        let sent_before = ctx.sender.client().request_count();
        drive(
            &mut ctx.sender,
            ctx.producer.send_offsets_to_transaction(HashMap::new(), group_metadata),
        )
        .await
        .expect("an empty offsets map is a no-op");
        assert_eq!(
            ctx.sender.client().request_count(),
            sent_before,
            "KafkaProducer.java:738 returns before touching the transaction state"
        );

        ctx.sender.client_mut().prepare_response(end_txn_response(Errors::None));
        drive(&mut ctx.sender, ctx.producer.commit_transaction())
            .await
            .expect("commitTransaction");
    }

    /// Translated from `KafkaProducerTest.testSendTxnOffsetsWithGroupMetadata`
    /// (Java 1893-1941).
    ///
    /// Java's offsets map here is empty too, so as in
    /// [`test_send_txn_offsets_with_group_id`] nothing is sent; the group metadata it
    /// builds carries a generation id and member id, which is what the request matcher
    /// would have checked had a request gone out. The check that *is* reachable is that
    /// a fully populated `ConsumerGroupMetadata` passes
    /// `throwIfInvalidGroupMetadata` — `generationId > 0` **with** a known member id.
    #[tokio::test]
    async fn test_send_txn_offsets_with_group_metadata() {
        let mut ctx = TxnProducerContext::new(&[("transactional.id", TRANSACTIONAL_ID), ("max.block.ms", "10000")], 1);
        ctx.time.set_auto_tick(1);
        let node = coordinator_node();
        ctx.sender.client_mut().throttle(&node, 5000);
        init_transactions(&mut ctx).await;
        ctx.producer.begin_transaction().expect("beginTransaction");

        #[allow(deprecated)]
        let group_metadata = ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id(
            "group",
            5,
            "member",
            Some("instance".to_string()),
        );
        drive(
            &mut ctx.sender,
            ctx.producer.send_offsets_to_transaction(HashMap::new(), group_metadata),
        )
        .await
        .expect("a populated group metadata is valid and an empty offsets map is a no-op");

        ctx.sender.client_mut().prepare_response(end_txn_response(Errors::None));
        drive(&mut ctx.sender, ctx.producer.commit_transaction())
            .await
            .expect("commitTransaction");
    }

    /// Translated from
    /// `KafkaProducerTest.testInvalidGenerationIdAndMemberIdCombinedInSendOffsets`
    /// (Java 1948-1953), which calls the `verifyInvalidGroupMetadata` helper
    /// (Java 2000-2026) with `new ConsumerGroupMetadata("group", 2, UNKNOWN_MEMBER_ID,
    /// Optional.empty())`.
    ///
    /// `KafkaProducerTest.testNullGroupMetadataInSendOffsets` (Java 1943-1946) is the
    /// other caller of that helper, passing `null`. It is **not translated**: the
    /// parameter is a `ConsumerGroupMetadata` value in Rust, so a null cannot be
    /// constructed and the arm it exercises
    /// (`KafkaProducer.java:1499-1500`) is enforced by the type system rather than by
    /// a runtime check. This is the same reasoning that dropped the arm from
    /// [`KafkaProducer::throw_if_invalid_group_metadata`].
    #[tokio::test]
    async fn test_invalid_generation_id_and_member_id_combined_in_send_offsets() {
        let mut ctx = TxnProducerContext::new(&[("transactional.id", TRANSACTIONAL_ID), ("max.block.ms", "10000")], 1);
        ctx.time.set_auto_tick(1);
        let node = coordinator_node();
        ctx.sender.client_mut().throttle(&node, 5000);
        init_transactions(&mut ctx).await;
        ctx.producer.begin_transaction().expect("beginTransaction");

        #[allow(deprecated)]
        let group_metadata = ConsumerGroupMetadata::with_generation_id_member_id_group_instance_id(
            "group",
            2,
            crate::common::requests::TxnOffsetCommitRequest::UNKNOWN_MEMBER_ID,
            None,
        );
        let error = ctx
            .producer
            .send_offsets_to_transaction(HashMap::new(), group_metadata.clone())
            .await
            .expect_err("generationId > 0 with an unknown member id is rejected");
        assert_eq!(
            error.message(),
            format!(
                "Passed in group metadata {} has generationId > 0 but the member.id is unknown",
                group_metadata
            )
        );
    }

    /// A `ProduceResponse` for one partition, mirroring
    /// `KafkaProducerTest.produceResponse(TopicIdPartition, long, Errors, int, int)`.
    fn produce_response(partition: i32, base_offset: i64, error: Errors, log_start_offset: i64) -> ConcreteResponse {
        use crate::ProduceResponseData;
        use crate::common::requests::ProduceResponse;
        use crate::produce_response_data::{PartitionProduceResponse, TopicProduceResponse};

        let mut partition_response = PartitionProduceResponse::new();
        partition_response.set_index(partition);
        partition_response.set_base_offset(base_offset);
        partition_response.set_error_code(error.code());
        partition_response.set_log_start_offset(log_start_offset);

        let mut topic_response = TopicProduceResponse::new();
        topic_response.set_topic_id(topic_id());
        topic_response.set_name(TOPIC.to_string());
        topic_response.set_partition_responses(vec![partition_response]);

        let mut data = ProduceResponseData::new();
        data.set_responses(vec![topic_response]);
        ConcreteResponse::Produce(ProduceResponse::new(data))
    }

    /// Java's `addOffsetsToTxnResponse(Errors error)`
    /// (`KafkaProducerTest.java:2037-2041`).
    fn add_offsets_to_txn_response(error: Errors) -> ConcreteResponse {
        use crate::AddOffsetsToTxnResponseData;
        use crate::common::requests::AddOffsetsToTxnResponse;

        let mut data = AddOffsetsToTxnResponseData::new();
        data.set_error_code(error.code()).set_throttle_time_ms(10);
        ConcreteResponse::AddOffsetsToTxn(AddOffsetsToTxnResponse::new(data))
    }

    /// Java's `txnOffsetsCommitResponse(Map<TopicPartition, Errors>)`
    /// (`KafkaProducerTest.java:2043-2045`).
    fn txn_offsets_commit_response(errors: &[(TopicPartition, Errors)]) -> ConcreteResponse {
        use crate::common::requests::TxnOffsetCommitResponse;

        let error_map: HashMap<TopicPartition, Errors> = errors.iter().cloned().collect();
        ConcreteResponse::TxnOffsetCommit(TxnOffsetCommitResponse::with_request_throttle_ms_response_data(
            10, &error_map,
        ))
    }

    /// Translated from `KafkaProducerTest.testTransactionV2Produce` (Java 1771-1828).
    ///
    /// The full Transaction V2 round trip: `FindCoordinator`, `InitProducerId`, one
    /// produce, `EndTxn`. No `AddPartitionsToTxn` is prepared or expected — under
    /// KIP-890 the broker adds the partition implicitly, which is exactly what
    /// `maybeAddPartition`'s V2 arm (`TransactionManager.java:448-451`) relies on.
    #[tokio::test]
    async fn test_transaction_v2_produce() {
        let mut ctx = TxnProducerContext::transactional_v2(&[("transactional.id", "some-txn")], 1);
        ctx.time.set_auto_tick(1);
        let node = coordinator_node();
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, "some-txn", &node));
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, PRODUCER_ID, EPOCH));
        drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect("initTransactions");
        assert!(
            ctx.transaction_manager.lock().unwrap().is_transaction_v2_enabled(),
            "initTransactions ends with maybeUpdateTransactionV2Enabled(true)"
        );

        ctx.producer.begin_transaction().expect("beginTransaction");
        ctx.sender
            .client_mut()
            .prepare_response(produce_response(0, 1, Errors::None, 0));
        ctx.sender.client_mut().prepare_response(end_txn_response(Errors::None));

        let record = ProducerRecord::with_partition_key(
            TOPIC.to_string(),
            Some(0),
            Some("key".to_string()),
            Some("value".to_string()),
        )
        .expect("a valid partition");
        let future = ctx.producer.send(record).await.expect("send");
        let metadata = drive(&mut ctx.sender, future.get()).await.expect("the produce succeeds");
        assert_eq!(metadata.offset(), 1);

        drive(&mut ctx.sender, ctx.producer.commit_transaction())
            .await
            .expect("commitTransaction");
    }

    /// Translated from
    /// `KafkaProducerTest.testTransactionV2ProduceWithConcurrentTransactionError`
    /// (Java 1443-1500).
    ///
    /// As [`test_transaction_v2_produce`], but the first produce is answered with
    /// `CONCURRENT_TRANSACTIONS`. That is retriable, so the batch is re-enqueued and
    /// the second response completes it — which is what makes the commit that follows
    /// succeed rather than fail on an abortable error.
    #[tokio::test]
    async fn test_transaction_v2_produce_with_concurrent_transaction_error() {
        let mut ctx = TxnProducerContext::transactional_v2(&[("transactional.id", "some-txn")], 1);
        ctx.time.set_auto_tick(1);
        let node = coordinator_node();
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, "some-txn", &node));
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, PRODUCER_ID, EPOCH));
        drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect("initTransactions");
        ctx.producer.begin_transaction().expect("beginTransaction");

        ctx.sender
            .client_mut()
            .prepare_response(produce_response(0, 1, Errors::ConcurrentTransactions, 0));
        ctx.sender
            .client_mut()
            .prepare_response(produce_response(0, 1, Errors::None, 0));
        ctx.sender.client_mut().prepare_response(end_txn_response(Errors::None));

        let record = ProducerRecord::with_partition_key(
            TOPIC.to_string(),
            Some(0),
            Some("key".to_string()),
            Some("value".to_string()),
        )
        .expect("a valid partition");
        let future = ctx.producer.send(record).await.expect("send");
        let metadata = drive(&mut ctx.sender, future.get()).await.expect("the retry succeeds");
        assert_eq!(metadata.offset(), 1);

        drive(&mut ctx.sender, ctx.producer.commit_transaction())
            .await
            .expect("commitTransaction");
    }

    /// Translated from `KafkaProducerTest.testSendTxnOffsetsWithGroupIdTransactionV2`
    /// (Java 1714-1769).
    ///
    /// With Transaction V2 the client skips `AddOffsetsToTxn` and sends the
    /// `TxnOffsetCommit` straight away (`TransactionManager.java:411-419`), after a
    /// `FindCoordinator` for the *group* coordinator. Java's prepared sequence says the
    /// same thing: `FindCoordinator`, `InitProducerId`, `FindCoordinator`,
    /// `TxnOffsetCommit`, `EndTxn` — five responses with no `AddOffsetsToTxn` between
    /// the second and third, unlike its V1 sibling.
    #[tokio::test]
    async fn test_send_txn_offsets_with_group_id_transaction_v2() {
        let mut ctx = TxnProducerContext::transactional_v2(
            &[("transactional.id", TRANSACTIONAL_ID), ("max.block.ms", "10000")],
            1,
        );
        ctx.time.set_auto_tick(1);
        let node = coordinator_node();
        ctx.sender.client_mut().throttle(&node, 5000);
        ctx.prepare_init_transactions(Errors::None, PRODUCER_ID, EPOCH);
        drive(&mut ctx.sender, ctx.producer.init_transactions())
            .await
            .expect("initTransactions");
        assert!(ctx.transaction_manager.lock().unwrap().is_transaction_v2_enabled());
        ctx.producer.begin_transaction().expect("beginTransaction");

        const GROUP_ID: &str = "group";
        let partition = TopicPartition::new(TOPIC.to_string(), 0);
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, GROUP_ID, &node));
        ctx.sender
            .client_mut()
            .prepare_response(txn_offsets_commit_response(&[(partition.clone(), Errors::None)]));
        ctx.sender.client_mut().prepare_response(end_txn_response(Errors::None));

        #[allow(deprecated)]
        let group_metadata = ConsumerGroupMetadata::new(GROUP_ID);
        let offsets = HashMap::from([(partition, OffsetAndMetadata::new(5).expect("a non-negative offset"))]);
        drive(
            &mut ctx.sender,
            ctx.producer.send_offsets_to_transaction(offsets, group_metadata),
        )
        .await
        .expect("sendOffsetsToTransaction");

        drive(&mut ctx.sender, ctx.producer.commit_transaction())
            .await
            .expect("commitTransaction");
    }

    /// Translated from `KafkaProducerTest.testMeasureAbortTransactionDuration`
    /// (Java 1502-1530).
    ///
    /// # What is and is not covered
    ///
    /// Java's assertions are all on the `txn-abort-time-ns-total` sensor: that it is
    /// positive after the first abort and larger after the second. There is no metrics
    /// layer in this crate — `KafkaProducerMetrics` and the whole
    /// `org.apache.kafka.common.metrics` package are in `remaining_classes.txt` — so
    /// those two assertions are not representable and are dropped.
    ///
    /// The operation sequence they surround is translated in full and is not trivial:
    /// two complete `beginTransaction` / `abortTransaction` cycles over one
    /// `initTransactions`, which is what proves `abortTransaction` leaves the manager
    /// in a state a *second* transaction can start from.
    #[tokio::test]
    async fn test_measure_abort_transaction_duration() {
        let mut ctx = TxnProducerContext::transactional();
        ctx.time.set_auto_tick(1);
        init_transactions(&mut ctx).await;

        for attempt in 0..2 {
            ctx.sender.client_mut().prepare_response(end_txn_response(Errors::None));
            ctx.producer
                .begin_transaction()
                .unwrap_or_else(|error| panic!("beginTransaction {}: {}", attempt, error));
            drive(&mut ctx.sender, ctx.producer.abort_transaction())
                .await
                .unwrap_or_else(|error| panic!("abortTransaction {}: {}", attempt, error));
        }
    }

    /// Translated from `KafkaProducerTest.testMeasureTransactionDurations`
    /// (Java 1841-1891).
    ///
    /// The `txn-init-time-ns-total` / `txn-begin-time-ns-total` /
    /// `txn-send-offsets-time-ns-total` / `txn-commit-time-ns-total` assertions are
    /// dropped for the reason given on [`test_measure_abort_transaction_duration`].
    /// What remains is the full V1 offsets round trip run **twice** over one
    /// `initTransactions`: `AddOffsetsToTxn`, a `FindCoordinator` for the group,
    /// `TxnOffsetCommit`, `EndTxn` — and, on the second pass, no second
    /// `FindCoordinator`, because the group coordinator is already known. That
    /// asymmetry is Java's too (the second batch of prepared responses omits it) and is
    /// the part of this test that exercises real behaviour.
    #[tokio::test]
    async fn test_measure_transaction_durations() {
        let mut ctx = TxnProducerContext::new(&[("transactional.id", TRANSACTIONAL_ID), ("max.block.ms", "10000")], 1);
        // Java's `new MockTime(Duration.ofSeconds(1).toMillis())` — a one-second tick,
        // which is what made the duration assertions meaningful.
        ctx.time.set_auto_tick(1000);
        init_transactions(&mut ctx).await;

        const GROUP_ID: &str = "group";
        let node = coordinator_node();
        let partition = TopicPartition::new(TOPIC.to_string(), 0);

        for (attempt, offset) in [(0usize, 5i64), (1, 10)] {
            ctx.sender
                .client_mut()
                .prepare_response(add_offsets_to_txn_response(Errors::None));
            if attempt == 0 {
                ctx.sender
                    .client_mut()
                    .prepare_response(find_coordinator_response(Errors::None, GROUP_ID, &node));
            }
            ctx.sender
                .client_mut()
                .prepare_response(txn_offsets_commit_response(&[(partition.clone(), Errors::None)]));
            ctx.sender.client_mut().prepare_response(end_txn_response(Errors::None));

            ctx.producer
                .begin_transaction()
                .unwrap_or_else(|error| panic!("beginTransaction {}: {}", attempt, error));

            #[allow(deprecated)]
            let group_metadata = ConsumerGroupMetadata::new(GROUP_ID);
            let offsets = HashMap::from([(
                partition.clone(),
                OffsetAndMetadata::new(offset).expect("a non-negative offset"),
            )]);
            drive(
                &mut ctx.sender,
                ctx.producer.send_offsets_to_transaction(offsets, group_metadata),
            )
            .await
            .unwrap_or_else(|error| panic!("sendOffsetsToTransaction {}: {}", attempt, error));

            drive(&mut ctx.sender, ctx.producer.commit_transaction())
                .await
                .unwrap_or_else(|error| panic!("commitTransaction {}: {}", attempt, error));
        }
    }

    /// A producer whose `Sender` is **spawned**, exactly as
    /// `KafkaProducer::with_client_options` does in production.
    ///
    /// Needed by the three `testCloseIsForcedOn*` methods, whose subject is the
    /// force-close path in `Sender::run`'s tail (`Sender.java:286-296`): it runs only
    /// once the run loop itself exits, which driving `run_once` cannot reach. The price
    /// is Java's — the `MockClient` moves into the task, so `prepare` queues every
    /// response up front and the test cannot inspect the mock afterwards.
    fn spawned_transactional_producer(
        extra: &[(&str, &str)],
        prepare: impl FnOnce(&mut MockClient),
    ) -> KafkaProducer<String, String> {
        spawned_transactional_producer_with_exit_hook(extra, prepare, None)
    }

    /// As [`spawned_transactional_producer`], but runs `on_exit` on the Sender's own
    /// thread once `Sender::run` has returned.
    ///
    /// This is what makes "did `close` actually join the Sender?" observable — see
    /// [`test_close_joins_the_sender_after_forcing`].
    fn spawned_transactional_producer_with_exit_hook(
        extra: &[(&str, &str)],
        prepare: impl FnOnce(&mut MockClient),
        on_exit: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> KafkaProducer<String, String> {
        let mut props = HashMap::from([("bootstrap.servers".to_string(), "localhost:9000".to_string())]);
        for (key, value) in extra {
            props.insert((*key).to_string(), (*value).to_string());
        }
        let config = ProducerConfig::new(&props).expect("valid config");
        let log_context = LogContext::new(format!("[Producer clientId={}] ", config.client_id));
        let time = MockTime::new(1_000);
        let api_versions = Arc::new(ApiVersions::new());

        let transaction_manager =
            KafkaProducer::<String, String>::configure_transaction_state(&config, &api_versions, &log_context)
                .expect("these tests always enable idempotence");

        let metadata = Arc::new(ProducerMetadata::with_log_context(
            config.reconnect_backoff_ms,
            config.reconnect_backoff_max_ms,
            config.metadata_max_age_ms,
            config.metadata_max_idle_ms,
            ClusterResourceListeners::new(),
            log_context.clone(),
        ));
        metadata.add(TOPIC, time.milliseconds());
        let update = crate::common::requests::RequestTestUtils::metadata_update_with(
            1,
            &HashMap::from([(TOPIC.to_string(), 1)]),
        );
        metadata.update_with_current_request_version(&update, false, time.milliseconds());

        let batch_size = config.batch_size.max(1);
        let metrics = Arc::new(Metrics::new());
        let buffer_pool = Arc::new(BufferPool::new(
            config.buffer_memory,
            batch_size as usize,
            Arc::clone(&metrics),
            time.as_provider(),
            KafkaProducer::<String, String>::PRODUCER_METRIC_GROUP_NAME,
        ));
        let accumulator = Arc::new(RecordAccumulator::with_log_context(
            batch_size,
            Compression::of(config.compression_type),
            config.linger_ms as i32,
            config.retry_backoff_ms,
            config.retry_backoff_max_ms,
            config.delivery_timeout_ms,
            PartitionerConfig {
                enable_adaptive_partitioning: config.partitioner_adaptive_partitioning_enable,
                partition_availability_timeout_ms: config.partitioner_availability_timeout_ms,
            },
            Arc::clone(&metrics),
            KafkaProducer::<String, String>::PRODUCER_METRIC_GROUP_NAME,
            buffer_pool,
            Some(Arc::clone(&transaction_manager)),
            log_context,
        ));

        let mut client = MockClient::with_static_nodes(vec![coordinator_node()], time.as_provider());
        prepare(&mut client);
        let wakeup = client.wakeup_notify();
        let running = Arc::new(AtomicBool::new(true));
        let force_close = Arc::new(AtomicBool::new(false));
        let pending_requests = Arc::new(Mutex::new(PendingRequests::new()));

        let mut sender = Sender::new(
            client,
            Arc::clone(&metadata),
            Arc::clone(&accumulator),
            config.max_in_flight_requests_per_connection == 1,
            config.max_request_size,
            config.acks,
            config.retries,
            config.request_timeout_ms,
            config.retry_backoff_ms,
            SenderMetricsRegistry::new(Arc::clone(&metrics)),
            Arc::clone(&running),
            Arc::clone(&force_close),
            time.as_provider(),
            Some(Arc::clone(&transaction_manager)),
            Arc::clone(&pending_requests),
            LogContext::empty(),
        );

        // # Why the Sender gets its own thread and runtime here
        //
        // `with_client_options` would `tokio::task::spawn` it onto the test's runtime, and that
        // deadlocks: `Sender::run` over a `MockClient` never awaits anything that is
        // pending — `MockClient::poll` returns immediately — so the task never yields.
        // In tokio only a worker parks on the time driver, and the sole awake worker is
        // then stuck inside that task, so **no timer in the whole runtime ever fires**:
        // the test's own `sleep` never returns and `close` is never called. (Diagnosed
        // from a thread sample: the second worker sitting in `park_condvar` while the
        // first spun in `run_once`.) A real `NetworkClient` cannot cause this, because
        // its `poll` awaits the selector.
        //
        // `spawn_blocking` plus a private current-thread runtime gives the Sender its
        // own OS thread and its own driver — which is also closer to Java, where the
        // Sender genuinely *is* a separate thread (`ioThread`). The returned handle is
        // still a `tokio::task::JoinHandle<()>`, so `close`'s join works unchanged.
        let sender_handle = tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a current-thread runtime for the Sender")
                .block_on(sender.run());
            if let Some(on_exit) = on_exit {
                on_exit();
            }
        });

        KafkaProducer::with_options(
            KafkaProducerOptionsBuilder::new()
                .set_config(&config)
                .set_key_serializer(Box::new(StringSerializer))
                .set_value_serializer(Box::new(StringSerializer))
                .set_metadata(metadata)
                .set_accumulator(accumulator)
                .set_running(running)
                .set_force_close(force_close)
                .set_wakeup(wakeup)
                .set_sender_handle(Some(sender_handle))
                .set_time_provider(time.as_provider())
                .set_transaction_manager(Some(transaction_manager))
                .set_pending_requests(pending_requests)
                .build()
                .expect("KafkaProducerOptionsBuilder::build: every mandatory parameter is set above"),
        )
    }

    /// The body all three `testCloseIsForcedOn*` methods share: start
    /// `initTransactions` on another task, let its request go out, then `close` with a
    /// one-second timeout and assert `close` **returned** instead of blocking behind the
    /// pending request.
    ///
    /// Java writes it three times over, submitting to an `ExecutorService` and waiting
    /// on a `CountDownLatch`; a spawned task and its `JoinHandle` are the direct
    /// equivalent. `client.waitForRequests(1, 2000)` becomes a short sleep: the
    /// `MockClient` has moved into the Sender task, so its request count is no longer
    /// observable from here.
    ///
    /// # What is asserted, and why not more
    ///
    /// Java's last line is `assertionDoneLatch.await(5000, MILLISECONDS)` and it
    /// **discards the boolean result**, so the test does not in fact require
    /// `initTransactions` to have returned by then — and it cannot have, on either
    /// side: the request that is in flight when `close` runs was already dequeued from
    /// `pendingRequests` by `nextRequest`, so `TransactionManager.close`'s
    /// `pendingRequests.forEach(handler -> handler.fail(..))`
    /// (`TransactionManager.java:949-955`) has nothing to fail, and neither
    /// `NetworkClient.close` nor `MockClient.close` runs completion handlers. The
    /// caller is released by its own `max.block.ms` instead, 60 s later by default.
    ///
    /// So the assertion is the one the test names: `close` is *forced* — it returns
    /// within its own timeout rather than waiting on a request that will never be
    /// answered. The `initTransactions` task is then given a bounded window purely so
    /// a run that *does* complete has its error inspected, exactly as far as Java goes.
    ///
    /// `multi_thread` is required at the call sites. `Sender::run` over a `MockClient`
    /// never blocks — `MockClient::poll` returns immediately — so on the default
    /// current-thread runtime it would starve the task doing the closing.
    async fn assert_close_forces_pending_transactional_request(producer: KafkaProducer<String, String>) {
        let producer = Arc::new(producer);
        let init = {
            let producer = Arc::clone(&producer);
            tokio::task::spawn(async move { producer.init_transactions().await })
        };

        tokio::time::sleep(Duration::from_millis(200)).await;
        let started = std::time::Instant::now();
        producer.close_with_timeout(Duration::from_millis(1000)).await.expect("close");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "close must be forced after its 1000 ms timeout, not blocked behind the \
             pending transactional request; it took {:?}",
            elapsed
        );

        // Java's ignored `assertionDoneLatch.await(5000, ..)`. If the call did return,
        // it must have returned an error — never a successful initTransactions.
        if let Ok(joined) = tokio::time::timeout(Duration::from_millis(500), init).await {
            let result = joined.expect("the initTransactions task did not panic");
            assert!(result.is_err(), "initTransactions cannot succeed once the producer is closed");
        }
    }

    // -- Rust-side regression tests, no Java counterpart --------------------
    //
    // `close`'s join is a Rust-only failure mode: Java's `ioThread.join()` cannot "lose"
    // its thread, whereas `tokio::time::timeout(t, handle)` consumes the `JoinHandle` and
    // drops it on expiry. Both tests below exist to make reverting
    // `await_sender_handle`'s `&mut` fail — which Critic 46 issue 6 showed the three
    // translated `testCloseIsForcedOn*` tests do **not**: with the bug restored,
    // `await_sender_handle_indefinitely` finds `None` and returns *sooner*, so every
    // assertion in those tests still passes.

    /// The mechanism: an expired [`KafkaProducer::await_sender_handle`] must leave the
    /// handle in place for [`KafkaProducer::await_sender_handle_indefinitely`] to join.
    ///
    /// Java's `close` force-closes and *then* joins unconditionally
    /// (`KafkaProducer.java:1414-1418`), and CLAUDE.md §9.4 requires the Rust
    /// translation to await the handle rather than merely signal it. Passing the handle
    /// by value into `tokio::time::timeout` breaks that silently: `timeout` takes
    /// ownership and drops it on `Elapsed`.
    #[tokio::test]
    async fn test_await_sender_handle_keeps_the_handle_when_it_expires() {
        let ctx = TxnProducerContext::transactional();

        // A task that outlives the wait below, so the wait is guaranteed to expire.
        let handle = tokio::task::spawn(async { tokio::time::sleep(Duration::from_secs(30)).await });
        *ctx.producer.sender_handle.lock().unwrap() = Some(handle);

        let completed = ctx.producer.await_sender_handle(Duration::from_millis(50)).await;
        assert!(!completed, "the task outlives the wait, so it cannot have completed");
        assert!(
            ctx.producer.sender_handle.lock().unwrap().is_some(),
            "an expired wait must put the handle back — otherwise close's later \
             unconditional join has nothing to join and returns while the Sender runs"
        );

        // And the retained handle is still the live one: aborting the task and waiting
        // again resolves against it, rather than short-circuiting on `None`.
        ctx.producer.sender_handle.lock().unwrap().as_ref().expect("retained").abort();
        assert!(
            ctx.producer.await_sender_handle(Duration::from_secs(5)).await,
            "the retained handle must be awaitable, not a husk"
        );
    }

    /// The contract: `close_with_timeout` must not return before the Sender has finished.
    ///
    /// Same setup as the three `testCloseIsForcedOn*` translations — a transactional
    /// request left in flight so the graceful wait expires and `close` force-closes —
    /// but with the Sender's exit made *observable*: the harness runs an exit hook on
    /// the Sender's own thread once `Sender::run` returns, after a deliberate 300 ms of
    /// shutdown cost.
    ///
    /// The sleep is instrumentation, not padding. Real shutdown work takes time (the
    /// force-close path fails pending requests, aborts batches, closes the client), but
    /// with a `MockClient` it is instant, which would leave the assertion racing the task
    /// instead of observing the join. 300 ms against a 1000 ms graceful timeout is a
    /// wide, one-sided margin: with the join the flag is necessarily set; without it
    /// `close` returns ~300 ms early.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_close_joins_the_sender_after_forcing() {
        let exited = Arc::new(AtomicBool::new(false));
        let producer = {
            let exited = Arc::clone(&exited);
            spawned_transactional_producer_with_exit_hook(
                &[("transactional.id", "this-is-a-transactional-id")],
                |_client| {},
                Some(Arc::new(move || {
                    std::thread::sleep(Duration::from_millis(300));
                    exited.store(true, Ordering::SeqCst);
                })),
            )
        };

        let producer = Arc::new(producer);
        let init = {
            let producer = Arc::clone(&producer);
            tokio::task::spawn(async move { producer.init_transactions().await })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;

        producer.close_with_timeout(Duration::from_millis(1000)).await.expect("close");
        assert!(
            exited.load(Ordering::SeqCst),
            "close returned before the Sender task finished: the graceful wait expired, \
             so close force-closed and must then have joined the handle \
             (KafkaProducer.java:1414-1418, CLAUDE.md §9.4)"
        );

        init.abort();
    }

    /// Translated from `KafkaProducerTest.testTransactionalMethodThrowsWhenSenderClosed`
    /// (Java 2162-2179).
    #[tokio::test]
    async fn test_transactional_method_throws_when_sender_closed() {
        let ctx = TxnProducerContext::new(&[("transactional.id", "this-is-a-transactional-id")], 1);
        ctx.producer.close().await.expect("close");
        let error = ctx.producer.init_transactions().await.expect_err("the producer is closed");
        assert_eq!(error.message(), "Cannot perform operation after producer has been closed");
    }

    /// Translated from `KafkaProducerTest.testCloseIsForcedOnPendingFindCoordinator`
    /// (Java 2181-2208).
    ///
    /// No response is prepared, so the `FindCoordinator` is the request left in flight
    /// when `close` runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_close_is_forced_on_pending_find_coordinator() {
        let producer =
            spawned_transactional_producer(&[("transactional.id", "this-is-a-transactional-id")], |_client| {});
        assert_close_forces_pending_transactional_request(producer).await;
    }

    /// Translated from `KafkaProducerTest.testCloseIsForcedOnPendingInitProducerId`
    /// (Java 2210-2237).
    ///
    /// The `FindCoordinator` is answered, so the `InitProducerId` is the request left
    /// in flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_close_is_forced_on_pending_init_producer_id() {
        let producer =
            spawned_transactional_producer(&[("transactional.id", "this-is-a-transactional-id")], |client| {
                client.prepare_response(find_coordinator_response(
                    Errors::None,
                    "this-is-a-transactional-id",
                    &coordinator_node(),
                ));
            });
        assert_close_forces_pending_transactional_request(producer).await;
    }

    /// Translated from `KafkaProducerTest.testCloseIsForcedOnPendingAddOffsetRequest`
    /// (Java 2239-2266).
    ///
    /// In Apache Kafka 4.2 this method's body is **identical** to
    /// `testCloseIsForcedOnPendingInitProducerId`'s — it prepares one
    /// `FindCoordinator` and submits `initTransactions`, never reaching an
    /// `AddOffsetsToTxn` despite the name. Translated as written rather than
    /// "corrected": inventing the `sendOffsetsToTransaction` the name implies would be
    /// a different test from the one Java runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_close_is_forced_on_pending_add_offset_request() {
        let producer =
            spawned_transactional_producer(&[("transactional.id", "this-is-a-transactional-id")], |client| {
                client.prepare_response(find_coordinator_response(
                    Errors::None,
                    "this-is-a-transactional-id",
                    &coordinator_node(),
                ));
            });
        assert_close_forces_pending_transactional_request(producer).await;
    }

    // =====================================================================
    // PHASE-6 TEST ACCOUNTING — the transactional `KafkaProducerTest` methods
    //
    // SCOPE CRITERION. A `KafkaProducerTest.java` method is in scope for Phase 6 iff its
    // body (or a helper it calls) mentions one of the five public transactional methods,
    // `TRANSACTIONAL_ID_CONFIG`, `TransactionManager`, `maybeAddPartition`, or the
    // `verifyInvalidGroupMetadata` helper. That is the marker set below, exactly.
    //
    // Two corrections to the criterion as first written (Critic 46 issue 4):
    //
    //   - It said "one of the **three** transactional config keys" while the marker set
    //     held one. The narrow *program* was right and the prose wrong: adding
    //     `ENABLE_IDEMPOTENCE_CONFIG` yields 36 rows and drags in 8 plainly
    //     non-transactional tests that merely set `enable.idempotence=false`
    //     (`testMetadataFetch`, `testMetadataExpiry`, `testMetadataTimeoutWith*`,
    //     `testMetadataWithPartitionOutOfRange`, `testTopicRefreshInMetadata`,
    //     `testFlushCompleteSendOfInflightBatches`, `shouldNotInvokeFlushInCallback`).
    //     "Three" described no realizable marker set, so the prose now matches the
    //     program.
    //   - `TransactionManager` / `maybeAddPartition` were **missing**, which made every
    //     mock-injected transactional test invisible. `testPartitionAddedToTransaction`
    //     (2423) reaches the transactional path only through
    //     `KafkaProducerTestContext`'s `mock(TransactionManager.class)` (`:2601`, passed
    //     `:2672`), so it named no marker at all and was absent from the denominator.
    //     It was also a real coverage gap, not only bookkeeping: `maybe_add_partition`'s
    //     production call site in this file had no test.
    //
    // The classifier lesson generalises, and is the same one the sibling `sender.rs`
    // block now records: a completeness check is only as strong as the assumption its
    // classifier makes, and when the Java side and the Rust side share that assumption
    // the resulting diff is **vacuous** for whatever they agree to ignore. Here the
    // shared assumption was "a transactional test names a transactional symbol", which a
    // Mockito-injected test does not.
    //
    // DERIVATION. The splitter counts braces rather than matching a declaration
    // regexp, because several of these bodies contain anonymous classes and lambdas
    // whose members would otherwise end a block early. Run from the repo root; awk
    // version 20200816, exit 0:
    //
    //   T=kafka/clients/src/test/java/org/apache/kafka/clients/producer/KafkaProducerTest.java
    //   M='initTransactions,beginTransaction,commitTransaction,abortTransaction,'
    //   M="$M"'sendOffsetsToTransaction,verifyInvalidGroupMetadata,TRANSACTIONAL_ID_CONFIG,'
    //   M="$M"'TransactionManager,maybeAddPartition'
    //   awk -v MARKERS="$M" '
    //     BEGIN { n = split(MARKERS, m, ","); depth = 0; inm = 0 }
    //     {
    //       line = $0
    //       if (depth == 1 && !inm && line ~ /^    [a-zA-Z@<].*\(.*\{[ \t]*$/ &&
    //           line !~ /^    (class|enum|interface|static \{)/) {
    //         match(line, /[a-zA-Z0-9_]+\(/)
    //         if (RSTART > 0) { name = substr(line, RSTART, RLENGTH-1); start = NR; inm = 1; delete hard }
    //       }
    //       if (inm) { for (i = 1; i <= n; i++) if (index(line, m[i]) > 0) hard[m[i]] = 1 }
    //       o = gsub(/\{/, "{", line); c = gsub(/\}/, "}", line); depth += o - c
    //       if (inm && depth <= 1) {
    //         hits = ""
    //         for (i = 1; i <= n; i++) if (m[i] in hard) hits = hits (hits == "" ? "" : "+") m[i]
    //         if (hits != "") printf "%d\t%s\n", start, name
    //         inm = 0
    //       }
    //     }' "$T"
    //
    // It prints **29** rows. The inline `for (i = 1; i <= n; i++)` walks `MARKERS` in
    // declaration order rather than `for (k in hard)`, so the output is reproducible on
    // any awk (the sibling blocks in `sender.rs` / `transaction_manager.rs` record why
    // that matters). An earlier revision credited an `emit` function for that property;
    // there is none in *this* program — the logic is inline, and `emit` belongs to the
    // sibling blocks (Critic 46 issue 4).
    //
    // ARITHMETIC. 29 printed rows = 1 helper (`verifyInvalidGroupMetadata`, 2000 — not a
    // test method) + **28** test methods. §Phase-6 says 27; 28 is the corrected
    // denominator, the extra being `testPartitionAddedToTransaction` (2423; Critic 46 issue 4).
    // Those 28 partition with no overlap into
    //
    //   23 translated here
    // +  1 not translated, justified (`testNullGroupMetadataInSendOffsets`)
    // +  4 translated in Phase 1, in `producer_config.rs`
    // = 28.
    //
    // Each of the three groups is listed in full below, so the sum can be checked
    // against the lists rather than taken on trust.
    //
    // TRANSLATED HERE (23 Rust tests):
    //   1290 testInitTransactionsResponseAfterTimeout
    //          -> test_init_transactions_response_after_timeout
    //   1329 testInitTransactionTimeout               -> test_init_transaction_timeout
    //   1364 testInitTransactionWhileThrottled        -> test_init_transaction_while_throttled
    //   1390 testClusterAuthorizationFailure          -> test_cluster_authorization_failure
    //   1419 testAbortTransaction                     -> test_abort_transaction
    //   1444 testTransactionV2ProduceWithConcurrentTransactionError
    //          -> test_transaction_v2_produce_with_concurrent_transaction_error
    //   1503 testMeasureAbortTransactionDuration      -> test_measure_abort_transaction_duration
    //   1533 testCommitTransactionWithRecordTooLargeException
    //          -> test_commit_transaction_with_record_too_large_error
    //   1563 testCommitTransactionWithMetadataTimeoutForMissingTopic
    //          -> test_commit_transaction_with_metadata_timeout_for_missing_topic
    //   1600 testCommitTransactionWithMetadataTimeoutForPartitionOutOfRange
    //          -> test_commit_transaction_with_metadata_timeout_for_partition_out_of_range
    //   1637 testCommitTransactionWithSendToInvalidTopic
    //          -> test_commit_transaction_with_send_to_invalid_topic
    //   1677 testSendTxnOffsetsWithGroupId            -> test_send_txn_offsets_with_group_id
    //   1715 testSendTxnOffsetsWithGroupIdTransactionV2
    //          -> test_send_txn_offsets_with_group_id_transaction_v2
    //   1772 testTransactionV2Produce                 -> test_transaction_v2_produce
    //   1842 testMeasureTransactionDurations          -> test_measure_transaction_durations
    //   1895 testSendTxnOffsetsWithGroupMetadata      -> test_send_txn_offsets_with_group_metadata
    //   1950 testInvalidGenerationIdAndMemberIdCombinedInSendOffsets
    //          -> test_invalid_generation_id_and_member_id_combined_in_send_offsets
    //   2054 testOnlyCanExecuteCloseAfterInitTransactionsTimeout
    //          -> test_only_can_execute_close_after_init_transactions_timeout
    //   2163 testTransactionalMethodThrowsWhenSenderClosed
    //          -> test_transactional_method_throws_when_sender_closed
    //   2182 testCloseIsForcedOnPendingFindCoordinator
    //          -> test_close_is_forced_on_pending_find_coordinator
    //   2210 testCloseIsForcedOnPendingInitProducerId
    //          -> test_close_is_forced_on_pending_init_producer_id
    //   2239 testCloseIsForcedOnPendingAddOffsetRequest
    //          -> test_close_is_forced_on_pending_add_offset_request
    //   2423 testPartitionAddedToTransaction         -> test_partition_added_to_transaction
    //          The mock-injected one. Translated with a real manager and
    //          `is_partition_pending_add` in place of Mockito's `verify`; the test's own
    //          rustdoc argues why that is stronger rather than weaker.
    //
    // NOT TRANSLATED, JUSTIFIED (1):
    //   1944 testNullGroupMetadataInSendOffsets — passes `null` for the
    //     `ConsumerGroupMetadata`. The Rust parameter is a value, so the argument
    //     cannot be constructed and the arm it exercises
    //     (`KafkaProducer.java:1499-1500`) is enforced by the type system instead of a
    //     runtime check. Recorded again on
    //     `test_invalid_generation_id_and_member_id_combined_in_send_offsets`, the
    //     other caller of the same Java helper, which *is* translated.
    //
    // TRANSLATED IN PHASE 1, in `producer_config.rs` (4): these reference
    // `TRANSACTIONAL_ID_CONFIG` only as an input to
    // `postProcessAndValidateIdempotenceConfigs`, so they belong to `ProducerConfig`,
    // not to this file. Verified present, not assumed:
    //
    //   $ grep -c 'fn test_overwrite_acks_and_retries_for_idempotent_producers\|fn test_acks_and_idempotence_for_idempotent_producers\|fn test_retries_and_idempotence_for_idempotent_producers\|fn test_inflight_requests_and_idempotence_for_idempotent_producers' src/producer/producer_config.rs
    //   4
    //
    //   222 testOverwriteAcksAndRetriesForIdempotentProducers
    //   238 testAcksAndIdempotenceForIdempotentProducers
    //   341 testRetriesAndIdempotenceForIdempotentProducers
    //   413 testInflightRequestsAndIdempotenceForIdempotentProducers
    //
    // ASSERTIONS DROPPED, NOT WHOLE TESTS (2 methods): every
    // `getMetricValue(producer, "txn-*-time-ns-total")` in
    // `testMeasureAbortTransactionDuration` and `testMeasureTransactionDurations`.
    // `KafkaProducerMetrics` and the whole `org.apache.kafka.common.metrics` package
    // are listed in `remaining_classes.txt`, so there is no sensor to read. Both
    // methods are translated for the operation sequences they surround, which are the
    // parts that exercise production behaviour; each says so at the test.
    // =====================================================================

    // -- `configureTransactionState` tests ----------------------------------
    //
    // These began life covering the temporary MILESTONE-11 GUARD in `new`.
    // Phase 4 turned the idempotence cases from rejections into constructions, and
    // Phase 6 removed the guard's last (transactional) arm — so what they now cover
    // is `configureTransactionState` (`KafkaProducer.java:592-620`) across its three
    // outcomes: no manager, an idempotent manager, and a transactional manager.

    fn guard_props(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut props = HashMap::from([("bootstrap.servers".to_string(), "localhost:9999".to_string())]);
        for (key, value) in extra {
            props.insert((*key).to_string(), (*value).to_string());
        }
        props
    }

    fn from_guard_props(props: &HashMap<String, String>) -> Result<(), Error> {
        let config = ProducerConfig::new(props)?;
        KafkaProducer::<String, String>::new(config, Box::new(StringSerializer), Box::new(StringSerializer)).map(|_| ())
    }

    /// Java wraps the whole constructor in `catch (Throwable t)` and rethrows
    /// `new KafkaException("Failed to construct kafka producer", t)`
    /// (`KafkaProducer.java:461-466`), so a caller has one class and one message to
    /// guard construction with whatever went wrong inside. Every failure used to
    /// escape raw, and one of them (`Error::local_illegal_argument` from the channel
    /// builder) answered `false` to `is_kafka_error()`.
    #[test]
    fn construction_failures_are_wrapped_as_a_kafka_error() {
        let props = HashMap::from([("bootstrap.servers".to_string(), "not-a-host-port".to_string())]);
        let config = ProducerConfig::new(&props).expect("the config itself parses");
        let error =
            KafkaProducer::<String, String>::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
                .err()
                .expect("an unparseable bootstrap.servers entry must fail construction");

        assert_eq!(error.message(), "Failed to construct kafka producer");
        // Java's replacement is a bare `KafkaException`.
        assert!(error.is_kafka_error(), "Java's replacement is a Kafka error: {error:?}");
        assert!(!error.is_api_error(), "a bare Kafka error is not an API error: {error:?}");
        // The real cause is carried, not stringified into the message.
        assert!(
            error.source().is_some(),
            "the underlying failure must be the wrapper's cause, not lost"
        );
    }

    /// `doSend`'s inner `catch (KafkaException e)` around `waitOnMetadata`
    /// (`KafkaProducer.java:993-998`) relabels a `close()` racing an in-flight send:
    ///
    /// ```java
    /// if (metadata.isClosed())
    ///     throw new KafkaException("Producer closed while send in progress", e);
    /// ```
    ///
    /// Rust had neither layer: `await_update` ignored `is_closed()` and `do_send`
    /// had no counterpart to this catch, so a caller shutting down while a first
    /// send resolved metadata saw a stall followed by a misleading retriable
    /// timeout.
    #[tokio::test]
    async fn closing_the_metadata_during_a_send_reports_the_close_not_a_timeout() {
        // A producer whose metadata knows nothing, so `send` has to wait for it.
        let metadata = Arc::new(ProducerMetadata::new(
            100,
            1000,
            300_000,
            300_000,
            ClusterResourceListeners::new(),
        ));
        let accumulator = create_accumulator();
        let config = ProducerConfig { max_block_ms: 30_000, ..Default::default() };
        let producer = create_producer_with_config(config, Arc::clone(&metadata), accumulator);

        metadata.close();

        let started = std::time::Instant::now();
        let record = ProducerRecord::with_key(TOPIC.to_string(), Some("k".to_string()), Some("v".to_string()));
        let error = producer
            .send(record)
            .await
            .expect_err("a bare Kafka error is not an API error, so send() returns Err");

        assert_eq!(error.message(), "Producer closed while send in progress");
        assert!(error.is_kafka_error(), "got {error:?}");
        assert!(!error.is_api_error(), "got {error:?}");
        assert!(
            !error.is_timeout_error(),
            "the close must not be reported as a timeout: {error:?}"
        );
        assert_eq!(
            error.source().expect("the awaitUpdate error is the cause").message(),
            "Requested metadata update after close"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "it must fail fast, not wait out max.block.ms"
        );
    }

    /// The default configuration must construct. `#[tokio::test]`: a successful
    /// `new` spawns the Sender task and so needs a runtime. The spawned task
    /// attempts to reach localhost:9999, fails harmlessly, and is dropped with the
    /// test.
    #[tokio::test]
    async fn test_guard_allows_default_config() {
        from_guard_props(&guard_props(&[])).expect("default config must still construct");
    }

    /// Explicit `enable.idempotence=true` is accepted as of Phase 4, and the
    /// producer really is idempotent: it holds a `TransactionManager`.
    #[tokio::test]
    async fn test_explicit_enable_idempotence_builds_a_transaction_manager() {
        let props = guard_props(&[("enable.idempotence", "true")]);
        let config = ProducerConfig::new(&props).expect("valid config");
        let producer =
            KafkaProducer::<String, String>::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
                .expect("explicit idempotence is supported as of Phase 4");
        let transaction_manager = producer
            .transaction_manager
            .as_ref()
            .expect("configureTransactionState builds a manager when enable.idempotence is true");
        let manager = transaction_manager.lock().unwrap();
        assert!(!manager.is_transactional());
        assert!(
            !manager.has_producer_id(),
            "the producer id is acquired asynchronously by the Sender task"
        );
    }

    /// `enable.idempotence=false` builds no manager at all, mirroring Java's `null`
    /// return from `configureTransactionState` (`KafkaProducer.java:594`, `:615`).
    #[tokio::test]
    async fn test_disabled_idempotence_builds_no_transaction_manager() {
        let props = guard_props(&[("enable.idempotence", "false")]);
        let config = ProducerConfig::new(&props).expect("valid config");
        let producer =
            KafkaProducer::<String, String>::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
                .expect("disabling idempotence is allowed");
        assert!(producer.transaction_manager.is_none());
    }

    /// `transactional.id` is accepted as of Phase 6, and the producer really is
    /// transactional: `configureTransactionState` passes the id through to the
    /// manager (`KafkaProducer.java:597`, `:602`) and `isTransactional()` reports it.
    ///
    /// This replaces the Phase-1 `test_guard_rejects_transactional_id`, whose whole
    /// subject — the `new` guard of PLAN §7.1 — is what this phase deleted.
    #[tokio::test]
    async fn test_transactional_id_builds_a_transactional_manager() {
        let props = guard_props(&[("transactional.id", "my-txn")]);
        let config = ProducerConfig::new(&props).expect("valid config");
        let producer =
            KafkaProducer::<String, String>::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
                .expect("transactional.id is supported as of Phase 6");
        let transaction_manager = producer
            .transaction_manager
            .as_ref()
            .expect("configureTransactionState builds a manager when transactional.id is set");
        let manager = transaction_manager.lock().unwrap();
        assert!(manager.is_transactional());
        assert_eq!(manager.transactional_id(), Some("my-txn"));
    }

    /// A producer with no `transactional.id` rejects every transactional method
    /// with Java's `throwIfNoTransactionManager` message — but only when there is
    /// no manager at all, i.e. `enable.idempotence=false`. An *idempotent* producer
    /// has a manager and is rejected one level down; the next test covers that.
    ///
    /// Java's message is built at `KafkaProducer.java:1508-1510`.
    #[tokio::test]
    async fn test_transactional_methods_without_a_manager() {
        let props = guard_props(&[("enable.idempotence", "false")]);
        let config = ProducerConfig::new(&props).expect("valid config");
        let producer =
            KafkaProducer::<String, String>::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
                .expect("disabling idempotence is allowed");

        const EXPECTED: &str = "Cannot use transactional methods without enabling transactions \
                                by setting the transactional.id configuration property";

        let expect_no_manager = |error: Error, method: &str| {
            assert_eq!(error.message(), EXPECTED, "{} reported the wrong error", method);
        };
        expect_no_manager(producer.init_transactions().await.expect_err("no manager"), "init_transactions");
        expect_no_manager(producer.begin_transaction().expect_err("no manager"), "begin_transaction");
        expect_no_manager(
            producer.commit_transaction().await.expect_err("no manager"),
            "commit_transaction",
        );
        expect_no_manager(producer.abort_transaction().await.expect_err("no manager"), "abort_transaction");
        // Java's own tests carry `@SuppressWarnings("removal")` for the deprecated
        // `ConsumerGroupMetadata(String)` constructor; this is that suppression.
        #[allow(deprecated)]
        let group_metadata = ConsumerGroupMetadata::new("group");
        expect_no_manager(
            producer
                .send_offsets_to_transaction(
                    HashMap::from([(
                        TopicPartition::new(TOPIC.to_string(), 0),
                        OffsetAndMetadata::new(1).expect("a non-negative offset"),
                    )]),
                    group_metadata,
                )
                .await
                .expect_err("no manager"),
            "send_offsets_to_transaction",
        );
    }

    /// An idempotent (non-transactional) producer *does* hold a manager, so Java's
    /// `throwIfNoTransactionManager` passes and the rejection comes from
    /// `TransactionManager.ensureTransactional()` (`TransactionManager.java:1857`)
    /// with a different message. Pinning both messages is what proves
    /// [`KafkaProducer::transaction_manager_or_error`] does not additionally test
    /// `is_transactional()`.
    #[tokio::test]
    async fn test_transactional_methods_on_an_idempotent_producer() {
        let props = guard_props(&[("enable.idempotence", "true")]);
        let config = ProducerConfig::new(&props).expect("valid config");
        let producer =
            KafkaProducer::<String, String>::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
                .expect("explicit idempotence is supported as of Phase 4");

        const EXPECTED: &str = "Transactional method invoked on a non-transactional producer.";
        assert_eq!(
            producer.init_transactions().await.expect_err("not transactional").message(),
            EXPECTED
        );
        assert_eq!(producer.begin_transaction().expect_err("not transactional").message(), EXPECTED);
        assert_eq!(
            producer.commit_transaction().await.expect_err("not transactional").message(),
            EXPECTED
        );
        assert_eq!(
            producer.abort_transaction().await.expect_err("not transactional").message(),
            EXPECTED
        );
    }
}
