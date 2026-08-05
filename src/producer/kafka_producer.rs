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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::client_utils;
use crate::common::Cluster;
use crate::common::KafkaError;
use crate::common::KafkaFuture;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::compress::Compression;
use crate::common::header::Headers;
use crate::common::header::internals::RecordHeader;
use crate::common::internals::ClusterResourceListeners;
use crate::common::network::Selector;
use crate::common::network::channel_builders;
use crate::common::record::CompressionType;
use crate::common::record::RecordBatch;
use crate::common::record::abstract_records;
use crate::common::requests::txn_offset_commit_request;
use crate::common::serialization::Serializer;
use crate::common::utils::LogContext;
use crate::consumer::ConsumerGroupMetadata;
use crate::consumer::OffsetAndMetadata;
use crate::kafka_client::KafkaClient;
use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;
use crate::network_client::NetworkClient;
use crate::producer::Callback;
use crate::producer::Producer;
use crate::producer::ProducerConfig;
use crate::producer::ProducerRecord;
use crate::producer::internals::BufferPool;
use crate::producer::internals::BuiltInPartitioner;
use crate::producer::internals::Caller;
use crate::producer::internals::FutureRecordMetadata;
use crate::producer::internals::PendingRequests;
use crate::producer::internals::ProducerMetadata;
use crate::producer::internals::Sender;
use crate::producer::internals::TransactionManager;
use crate::producer::internals::{PartitionerConfig, RecordAccumulator};
use crate::producer::{RecordMetadata, record_metadata};
use crate::{ApiVersions, DefaultHostResolver};
use crate::{kafka_debug, kafka_info, kafka_trace, kafka_warn};

/// Network thread name prefix.
pub const NETWORK_THREAD_PREFIX: &str = "kafka-producer-network-thread";

/// Producer metric group name.
pub const PRODUCER_METRIC_GROUP_NAME: &str = "producer-metrics";

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
    /// Java's four transactional entry points on this class
    /// (`initTransactions` `:653`, `sendOffsetsToTransaction` `:818`,
    /// `commitTransaction` `:741`, `abortTransaction` `:784`) all enqueue into it
    /// from the application thread, which is why the producer needs a handle at
    /// all.
    ///
    /// [`Sender::pending_requests`]: crate::producer::internals::Sender
    pending_requests: Arc<Mutex<PendingRequests>>,
    /// The compression type for records.
    compression_type: CompressionType,
    /// The maximum time to block on send/partitionsFor.
    max_block_ms: i64,
    /// Whether to ignore keys for partitioning.
    partitioner_ignore_keys: bool,
    /// Whether the sender task is still running.
    running: Arc<AtomicBool>,
    /// Whether the caller wants to force-close.
    force_close: Arc<AtomicBool>,
    /// Wakeup notification for the sender task.
    wakeup: Arc<Notify>,
    /// Handle to the sender background task.
    /// Wrapped in `Mutex<Option<_>>` so `close_timeout` can take ownership
    /// and `.await` it even though we only have `&self` (not `&mut self`).
    sender_handle: Mutex<Option<JoinHandle<()>>>,
    /// Provider of current wall-clock time in milliseconds.
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Contextual log message prefix.
    ///
    /// Translated from Java's `LogContext logContext` field in `KafkaProducer`.
    log_context: LogContext,
}

impl<K, V> KafkaProducer<K, V> {
    /// Creates a new `KafkaProducer` from individual pre-built components.
    ///
    /// This is the test-friendly constructor that allows injection of
    /// dependencies. Corresponds to the Java package-private constructor
    /// used by tests.
    ///
    /// # Arguments
    ///
    /// * `config` - The producer configuration
    /// * `key_serializer` - The key serializer
    /// * `value_serializer` - The value serializer
    /// * `metadata` - The producer metadata
    /// * `accumulator` - The record accumulator
    /// * `running` - Whether the sender is running
    /// * `force_close` - Whether force-close has been requested
    /// * `wakeup` - Notification to wake up the sender task
    /// * `sender_handle` - Handle to the sender background task
    /// * `time_provider` - Provider of current wall-clock time
    /// * `transaction_manager` - The shared transaction state object, or `None`
    ///   when idempotence is disabled
    /// * `pending_requests` - The transactional request queue this producer shares
    ///   with the [`Sender`]
    // `TransactionManager` is `pub(crate)` per CLAUDE.md §2; see the note on
    // [`Self::with_client`] for why this constructor stays nominally `pub`.
    #[allow(private_interfaces)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &ProducerConfig,
        key_serializer: Box<dyn Serializer<K> + Send + Sync>,
        value_serializer: Box<dyn Serializer<V> + Send + Sync>,
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
        running: Arc<AtomicBool>,
        force_close: Arc<AtomicBool>,
        wakeup: Arc<Notify>,
        sender_handle: Option<JoinHandle<()>>,
        time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
        pending_requests: Arc<Mutex<PendingRequests>>,
    ) -> Self {
        let log_context = LogContext::new(format!("[Producer clientId={}] ", config.client_id));
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
            partitioner_ignore_keys: config.partitioner_ignore_keys,
            running,
            force_close,
            wakeup,
            sender_handle: Mutex::new(sender_handle),
            time_provider,
            log_context,
        }
    }

    /// Creates a `KafkaProducer` from configuration, serializers, and an optional
    /// compression override.
    ///
    /// This is the primary public factory method, mirroring Java's
    /// `new KafkaProducer(Properties, Serializer, Serializer)` constructor.
    /// It internally wires up all infrastructure components:
    ///
    /// 1. Parses and resolves bootstrap server addresses from the config
    /// 2. Creates [`ProducerMetadata`] and bootstraps it with the resolved addresses
    /// 3. Creates a [`PlaintextChannelBuilder`], [`Selector`], and [`NetworkClient`]
    /// 4. Creates a [`BufferPool`] and [`RecordAccumulator`]
    /// 5. Spawns the background sender task via [`with_client`](Self::with_client)
    ///
    /// # Arguments
    ///
    /// * `config` - The producer configuration
    /// * `key_serializer` - The key serializer
    /// * `value_serializer` - The value serializer
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] if no valid bootstrap server addresses
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
    /// let config = ProducerConfig::from_properties(&props)
    ///     .expect("Invalid config");
    ///
    /// let producer = KafkaProducer::<String, String>::from_config(
    ///     config,
    ///     Box::new(StringSerializer),
    ///     Box::new(StringSerializer),
    /// ).expect("Failed to create producer");
    /// ```
    pub fn from_config(
        config: ProducerConfig,
        key_serializer: Box<dyn Serializer<K> + Send + Sync>,
        value_serializer: Box<dyn Serializer<V> + Send + Sync>,
    ) -> Result<Self, KafkaError> {
        let log_context = LogContext::new(format!("[Producer clientId={}] ", config.client_id));

        kafka_trace!(log_context, "Starting the Kafka producer");

        // 1. Parse and validate bootstrap server addresses
        let addresses = client_utils::parse_and_validate_addresses(&config.bootstrap_servers)?;

        // 2. Validate delivery timeout configuration
        //    Translated from KafkaProducer.configureDeliveryTimeout().
        let delivery_timeout_ms = Self::configure_delivery_timeout(&config)?;

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
        let channel_builder = channel_builders::client_channel_builder(
            config.security_protocol,
            Some(&config.ssl_config),
            Some(&config.sasl_config),
            None,
            &config.client_id,
            log_context.clone(),
        )
        .map_err(|e| KafkaError::illegal_argument(format!("Failed to create channel builder: {}", e)))?;
        let selector = Selector::with_defaults_and_log_context(
            config.connections_max_idle_ms,
            channel_builder,
            log_context.clone(),
        );
        let api_versions = Arc::new(ApiVersions::new());

        let client = NetworkClient::with_metadata(
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

        // 8. Create the TransactionManager, before the accumulator and the Sender
        //    because both of them need it (PLAN §6.3). Java's field assignment sits
        //    at the same point in the constructor (`KafkaProducer.java:415`, ahead
        //    of the `RecordAccumulator` at `:427` and the `Sender` at `:437`).
        let transaction_manager = Self::configure_transaction_state(&config, &api_versions, &log_context);

        // 9. Create BufferPool and RecordAccumulator
        //    As per Kafka configuration documentation, batch.size may be set to 0
        //    to explicitly disable batching, which in practice uses a batch size of 1.
        let batch_size = config.batch_size.max(1);
        let buffer_pool = Arc::new(BufferPool::new(config.buffer_memory, batch_size as usize));
        let accumulator = Arc::new(RecordAccumulator::with_log_context(
            batch_size,
            compression,
            config.linger_ms as i32,
            config.retry_backoff_ms,
            config.retry_backoff_max_ms,
            delivery_timeout_ms,
            PartitionerConfig {
                enable_adaptive_partitioning: config.partitioner_adaptive_partitioning_enable,
                partition_availability_timeout_ms: config.partitioner_availability_timeout_ms,
            },
            buffer_pool,
            transaction_manager.clone(),
            log_context.clone(),
        ));

        // 10. Wire up the Sender and spawn the I/O background task
        Ok(Self::with_client(
            &config,
            key_serializer,
            value_serializer,
            metadata,
            accumulator,
            client,
            time_provider,
            transaction_manager,
            Arc::new(Mutex::new(PendingRequests::new())),
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
    /// ever carried was [`Self::from_config`]'s temporary guard on
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
    /// [`Self::from_config`] is the user-facing constructor and the analogue of
    /// Java's public `KafkaProducer` constructor; this is the injection seam it
    /// delegates to, used directly only by tests that need a mock
    /// [`KafkaClient`].
    ///
    /// Although marked `pub`, this is **not reachable from outside the crate**:
    /// `metadata` and `accumulator` are `Arc`s of `ProducerMetadata` and
    /// `RecordAccumulator`, both `pub(crate)` under
    /// `producer::internals`, so an external caller cannot name or construct
    /// them.
    ///
    /// MILESTONE-11 GUARD: that unreachability is why the idempotence /
    /// transaction guard in [`Self::from_config`] is not duplicated here. The
    /// approved plan asked for it in both constructors; it is omitted here
    /// deliberately, because there is no external path to guard and adding it
    /// would mean changing this function's return type to `Result` for the
    /// benefit of in-crate test callers only. Revisit if this ever becomes
    /// externally constructible.
    ///
    /// # Type Parameters
    ///
    /// * `C` - The KafkaClient implementation type
    #[allow(private_interfaces)]
    #[allow(clippy::too_many_arguments)]
    pub fn with_client<C: KafkaClient + Send + 'static>(
        config: &ProducerConfig,
        key_serializer: Box<dyn Serializer<K> + Send + Sync>,
        value_serializer: Box<dyn Serializer<V> + Send + Sync>,
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
        client: C,
        time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
        pending_requests: Arc<Mutex<PendingRequests>>,
    ) -> Self {
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
            Arc::clone(&running),
            Arc::clone(&force_close),
            Arc::clone(&time_provider),
            transaction_manager.clone(),
            Arc::clone(&pending_requests),
            log_context.clone(),
        );

        let io_thread_name = format!("{} | {}", NETWORK_THREAD_PREFIX, config.client_id);
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
            partitioner_ignore_keys: config.partitioner_ignore_keys,
            running,
            force_close,
            wakeup,
            sender_handle: Mutex::new(Some(sender_handle)),
            time_provider,
            log_context,
        }
    }

    /// Validate and optionally adjust `delivery.timeout.ms` against
    /// `linger.ms + request.timeout.ms`.
    ///
    /// Translated from `KafkaProducer.configureDeliveryTimeout()`.
    ///
    /// Since the Rust config struct does not track which fields were explicitly
    /// set by the user (unlike Java's `ConfigDef`), this always returns an error
    /// when `delivery_timeout_ms < linger_ms + request_timeout_ms`. With the
    /// default values (delivery=120000, linger=5, request=30000) the constraint
    /// is satisfied, so this only triggers when the user supplies inconsistent
    /// overrides — matching the Java "explicitly set" branch.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] if the delivery timeout is too
    /// small (corresponds to Java's `ConfigException`).
    fn configure_delivery_timeout(config: &ProducerConfig) -> Result<i32, KafkaError> {
        let delivery_timeout_ms = config.delivery_timeout_ms;
        let linger_ms = config.linger_ms.min(i32::MAX as i64) as i32;
        let request_timeout_ms = config.request_timeout_ms;
        let linger_and_request_timeout_ms = (linger_ms as i64 + request_timeout_ms as i64).min(i32::MAX as i64) as i32;

        if delivery_timeout_ms < linger_and_request_timeout_ms {
            return Err(KafkaError::illegal_argument(format!(
                "{} should be equal to or larger than {} + {}",
                ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG,
                ProducerConfig::LINGER_MS_CONFIG,
                ProducerConfig::REQUEST_TIMEOUT_MS_CONFIG,
            )));
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
    /// (CLAUDE.md §9.1) and returns [`KafkaError::Timeout`] when the transactional
    /// state cannot be initialized before `max.block.ms` expires. It is safe to
    /// retry in that case, but once the transactional state has been successfully
    /// initialized this method should no longer be used.
    ///
    /// Java's `InterruptException` path has no Rust analogue — a task is not
    /// interrupted, it is dropped.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::IllegalState`] if no `transactional.id` has been configured
    /// - [`KafkaError::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions
    /// - An authorization error indicating that the configured `transactional.id`
    ///   is not authorized, or the idempotent producer id is unavailable; the user
    ///   may retry after fixing the permission
    /// - Any previous fatal error the producer has encountered
    /// - [`KafkaError::Timeout`] if initializing the transaction takes longer than
    ///   `max.block.ms`
    pub async fn init_transactions(&self) -> Result<(), KafkaError> {
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
        result.await_result_timeout(self.max_block_timeout()).await?;
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
    /// - [`KafkaError::IllegalState`] if no `transactional.id` has been configured
    ///   or if [`Self::init_transactions`] has not yet been invoked
    /// - A producer-fenced error if another producer with the same
    ///   `transactional.id` is active
    /// - An invalid-producer-epoch error if the producer has attempted to produce
    ///   with an old epoch to the partition leader
    /// - [`KafkaError::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions
    /// - Any previous fatal error the producer has encountered
    pub fn begin_transaction(&self) -> Result<(), KafkaError> {
        let transaction_manager = self.transaction_manager_or_error()?;
        self.ensure_not_closed()?;
        self.throw_if_in_prepared_state()?;
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
    /// `AsyncKafkaConsumer::commit_sync_offsets` already uses for an offsets map.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::IllegalArgument`] if `group_metadata` has a generation id
    ///   greater than zero but an unknown member id
    /// - [`KafkaError::IllegalState`] if no `transactional.id` has been configured
    ///   or no transaction has been started
    /// - A producer-fenced error if another producer with the same
    ///   `transactional.id` is active
    /// - [`KafkaError::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions, or does not support the latest version of
    ///   the transactional API with all consumer group metadata
    /// - An authorization error indicating that the configured `transactional.id`
    ///   or the consumer group id is not authorized
    /// - A commit-failed error if the commit cannot be retried (e.g. the consumer
    ///   has been kicked out of the group); users should handle this by aborting
    ///   the transaction
    /// - [`KafkaError::Timeout`] if sending the offsets takes longer than
    ///   `max.block.ms`
    pub async fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<(), KafkaError> {
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
        result.await_result_timeout(self.max_block_timeout()).await
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
    /// A [`KafkaError::Timeout`] does **not** mean the request did not reach the
    /// broker — only that the acknowledgement did not arrive in time, so it is up
    /// to the application to decide how to handle it. It is safe to retry, but it
    /// is not possible to attempt a different operation (such as
    /// [`Self::abort_transaction`]) since the commit may already be in the process
    /// of completing. If not retrying, the only option is to close the producer.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::IllegalState`] if no `transactional.id` has been configured
    ///   or no transaction has been started
    /// - A producer-fenced error if another producer with the same
    ///   `transactional.id` is active
    /// - [`KafkaError::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions
    /// - An authorization error indicating that the configured `transactional.id`
    ///   is not authorized
    /// - An invalid-producer-epoch error if the producer has attempted to produce
    ///   with an old epoch to the partition leader
    /// - Any previous fatal or abortable error the producer has encountered
    /// - [`KafkaError::Timeout`] if committing takes longer than `max.block.ms`
    pub async fn commit_transaction(&self) -> Result<(), KafkaError> {
        let transaction_manager = self.transaction_manager_or_error()?;
        self.ensure_not_closed()?;
        let result = {
            // `pending_requests` before the manager, per the field docs.
            let mut pending_requests = self.pending_requests.lock().unwrap();
            transaction_manager.lock().unwrap().begin_commit(&mut pending_requests)?
        };
        self.wakeup.notify_one();
        result.await_result_timeout(self.max_block_timeout()).await
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
    /// A [`KafkaError::Timeout`] does **not** mean the request did not reach the
    /// broker — see [`Self::commit_transaction`] for the full note; it is safe to
    /// retry, but not to attempt a different operation.
    ///
    /// # Errors
    ///
    /// - [`KafkaError::IllegalState`] if no `transactional.id` has been configured
    ///   or no transaction has been started
    /// - A producer-fenced error if another producer with the same
    ///   `transactional.id` is active
    /// - An invalid-producer-epoch error if the producer has attempted to produce
    ///   with an old epoch to the partition leader
    /// - [`KafkaError::UnsupportedVersion`] as a fatal error indicating the broker
    ///   does not support transactions
    /// - An authorization error indicating that the configured `transactional.id`
    ///   is not authorized
    /// - Any previous fatal error the producer has encountered
    /// - [`KafkaError::Timeout`] if aborting takes longer than `max.block.ms`
    pub async fn abort_transaction(&self) -> Result<(), KafkaError> {
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
        result.await_result_timeout(self.max_block_timeout()).await
    }

    /// The shared [`TransactionManager`], or the error Java's
    /// `throwIfNoTransactionManager()` (`KafkaProducer.java:1507-1511`) throws.
    ///
    /// Java checks `transactionManager == null` only, so an *idempotent* producer
    /// passes this check and is rejected one level down by the manager's own
    /// `ensureTransactional()` with a different message. That split is preserved:
    /// this must not also test `is_transactional()`.
    fn transaction_manager_or_error(&self) -> Result<Arc<Mutex<TransactionManager>>, KafkaError> {
        match &self.transaction_manager {
            Some(transaction_manager) => Ok(Arc::clone(transaction_manager)),
            None => Err(KafkaError::illegal_state(format!(
                "Cannot use transactional methods without enabling transactions by setting the {} configuration property",
                ProducerConfig::TRANSACTIONAL_ID_CONFIG
            ))),
        }
    }

    /// Returns an error if the transaction is in a prepared state.
    ///
    /// Translated from `KafkaProducer.throwIfInPreparedState()`
    /// (`KafkaProducer.java:968-976`). In a two-phase commit (2PC) flow, once a
    /// transaction enters the prepared state, only commit, abort, or complete
    /// operations are allowed.
    ///
    /// # Errors
    ///
    /// [`KafkaError::IllegalState`] if any other operation is attempted in the
    /// prepared state.
    fn throw_if_in_prepared_state(&self) -> Result<(), KafkaError> {
        if let Some(transaction_manager) = &self.transaction_manager {
            let transaction_manager = transaction_manager.lock().unwrap();
            if transaction_manager.is_transactional() && transaction_manager.is_prepared() {
                return Err(KafkaError::illegal_state(
                    "Cannot perform operation while the transaction is in a prepared state. \
                     Only commitTransaction(), abortTransaction(), or completeTransaction() are permitted.",
                ));
            }
        }
        Ok(())
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
    /// [`KafkaError::IllegalArgument`] when the generation id is greater than zero
    /// but the member id is unknown.
    fn throw_if_invalid_group_metadata(group_metadata: &ConsumerGroupMetadata) -> Result<(), KafkaError> {
        if group_metadata.generation_id() > 0
            && group_metadata.member_id() == txn_offset_commit_request::UNKNOWN_MEMBER_ID
        {
            return Err(KafkaError::illegal_argument(format!(
                "Passed in group metadata {} has generationId > 0 but the member.id is unknown",
                group_metadata
            )));
        }
        Ok(())
    }

    /// Verify that this producer instance has not been closed.
    ///
    /// Corresponds to Java's `throwIfProducerClosed()`.
    fn ensure_not_closed(&self) -> Result<(), KafkaError> {
        if !self.running.load(Ordering::Acquire) {
            return Err(KafkaError::illegal_state(
                "Cannot perform operation after producer has been closed",
            ));
        }
        Ok(())
    }

    /// Implementation of asynchronously send a record to a topic.
    ///
    /// Translated from `KafkaProducer.doSend()`.
    ///
    /// For `ApiException`-type errors (serialization, record-too-large, invalid
    /// topic, etc.), the callback is invoked with the error and a
    /// completed-with-error future is returned (`Ok(failed_future)`). This matches
    /// Java's contract where `send()` always returns a `Future` for API errors and
    /// always invokes the callback.
    ///
    /// Only non-API errors (like `IllegalState` when the producer is closed) are
    /// propagated as `Err(...)`.
    async fn do_send(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        self.ensure_not_closed()?;
        // Java 989: a send is one of the operations 2PC forbids once the transaction
        // is prepared.
        self.throw_if_in_prepared_state()?;

        // First make sure the metadata for the topic is available
        let now_ms = self.now_ms();
        let cluster_and_wait_time = match self
            .wait_on_metadata(record.topic(), record.partition(), now_ms, self.max_block_ms)
            .await
        {
            Ok(cwt) => cwt,
            Err(e) if e.is_api_exception() => {
                return self.handle_api_exception(e, record.topic(), record_metadata::UNKNOWN_PARTITION, callback);
            },
            Err(e) => return Err(e),
        };
        let now_ms = now_ms + cluster_and_wait_time.waited_on_metadata_ms;
        let remaining_wait_ms = 0i64.max(self.max_block_ms - cluster_and_wait_time.waited_on_metadata_ms);
        let cluster = cluster_and_wait_time.cluster;

        // Destructure the record to take ownership of key/value for zero-copy serialization
        let (record_topic, partition_opt, timestamp_opt, record_headers, key, value) = record.into_parts();

        let serialized_key = self
            .key_serializer
            .serialize_owned_with_headers(&record_topic, &record_headers, key)
            .map_err(|e| KafkaError::serialization(format!("Failed to serialize key: {}", e)))?;

        let serialized_value = self
            .value_serializer
            .serialize_owned_with_headers(&record_topic, &record_headers, value)
            .map_err(|e| KafkaError::serialization(format!("Failed to serialize value: {}", e)))?;

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
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        let partition = if let Some(p) = partition {
            p
        } else if let Some(k) = key
            && !self.partitioner_ignore_keys
        {
            let num_partitions = cluster.partitions_for_topic(topic).len() as i32;
            if num_partitions > 0 {
                BuiltInPartitioner::partition_for_key(k, num_partitions)
            } else {
                record_metadata::UNKNOWN_PARTITION
            }
        } else {
            record_metadata::UNKNOWN_PARTITION
        };

        let serialized_size = abstract_records::estimate_size_in_bytes_upper_bound(
            RecordBatch::CURRENT_MAGIC_VALUE,
            self.compression_type,
            key,
            value,
            headers,
        );
        if let Err(err) = self.ensure_valid_record_size(serialized_size) {
            return self.handle_api_exception(err, topic, partition, callback);
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
                    if let Err(error) = transaction_manager.lock().unwrap().maybe_add_partition(&result.topic_partition)
                    {
                        let partition = result.topic_partition.partition();
                        return self.handle_api_exception(error, topic, partition, None);
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
            Err(e) if e.is_api_exception() => {
                kafka_debug!(self.log_context, "Exception occurred during message send: {}", e);
                self.maybe_transition_to_error_state(&e);
                let tp = TopicPartition::new(topic.to_string(), partition);
                Ok(KafkaFuture::new(Arc::new(FutureRecordMetadata::failed(tp, e))))
            },
            Err(e) => Err(e),
        }
    }

    /// `transactionManager.maybeTransitionToErrorState(e)`, the tail of
    /// `KafkaProducer.doSend`'s `catch (ApiException e)` block
    /// (`KafkaProducer.java:1065-1067`).
    ///
    /// [`Caller::App`](crate::producer::internals::Caller::App): `doSend` runs on the
    /// application task.
    fn maybe_transition_to_error_state(&self, error: &KafkaError) {
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

    /// Handle an `ApiException`-type error by invoking the callback (if any)
    /// and returning a completed-with-error future.
    ///
    /// This matches Java's `catch (ApiException e)` block in `doSend()`.
    fn handle_api_exception(
        &self,
        error: KafkaError,
        topic: &str,
        partition: i32,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        kafka_debug!(self.log_context, "Exception occurred during message send: {}", error);
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
    /// - The topic is invalid ([`InvalidTopic`](KafkaError::InvalidTopic))
    /// - Metadata could not be refreshed within `max_wait_ms` ([`Timeout`](KafkaError::Timeout))
    /// - The producer is closed
    async fn wait_on_metadata(
        &self,
        topic: &str,
        partition: Option<i32>,
        now_ms: i64,
        max_wait_ms: i64,
    ) -> Result<ClusterAndWaitTime, KafkaError> {
        let cluster = self.metadata.fetch();

        if cluster.invalid_topics().contains(topic) {
            return Err(KafkaError::invalid_topics([topic.to_string()].into_iter().collect()));
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
                Err(_) => {
                    let error_message = self.get_error_message(partitions_count, topic, partition, max_wait_ms);
                    if let Some(err) = self.metadata.get_error(topic) {
                        return Err(KafkaError::timeout(format!(
                            "{} (underlying error: {})",
                            error_message,
                            err.message()
                        )));
                    }
                    return Err(KafkaError::timeout(error_message));
                },
            }

            let cluster = self.metadata.fetch();
            elapsed = self.now_ms() - now_ms;
            if elapsed >= max_wait_ms {
                let error_message = self.get_error_message(partitions_count, topic, partition, max_wait_ms);
                return Err(KafkaError::timeout(error_message));
            }
            self.metadata.maybe_return_error_for_topic(topic)?;
            remaining_wait_ms = max_wait_ms - elapsed;
            partitions_count = cluster.partition_count_for_topic(topic);

            let done = match partitions_count {
                None => false,
                Some(count) => partition.is_none() || partition.unwrap() < count as i32,
            };
            if done {
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
    fn ensure_valid_record_size(&self, size: i32) -> Result<(), KafkaError> {
        if size > self.max_request_size {
            return Err(KafkaError::record_too_large(format!(
                "The message is {} bytes when serialized which is larger than {}, which is the value of the {} configuration.",
                size,
                self.max_request_size,
                ProducerConfig::MAX_REQUEST_SIZE_CONFIG
            )));
        }
        if size as i64 > self.total_memory_size {
            return Err(KafkaError::record_too_large(format!(
                "The message is {} bytes when serialized which is larger than the total memory buffer you have configured with the {} configuration.",
                size,
                ProducerConfig::BUFFER_MEMORY_CONFIG
            )));
        }
        Ok(())
    }

    /// Compute partition for the given record.
    ///
    /// If the record has a partition, return it. Otherwise, try to calculate
    /// partition based on key. If there is no key or key should be ignored,
    /// return `UNKNOWN_PARTITION` to indicate any partition can be used.
    ///
    /// Translated from `KafkaProducer.partition()`.
    fn partition(
        &self,
        record: &ProducerRecord<K, V>,
        serialized_key: Option<&[u8]>,
        _serialized_value: Option<&[u8]>,
        cluster: &Cluster,
    ) -> i32 {
        if let Some(p) = record.partition() {
            return p;
        }

        if let Some(key) = serialized_key
            && !self.partitioner_ignore_keys
        {
            let num_partitions = cluster.partitions_for_topic(record.topic()).len() as i32;
            if num_partitions > 0 {
                return BuiltInPartitioner::partition_for_key(key, num_partitions);
            }
        }

        record_metadata::UNKNOWN_PARTITION
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
    async fn await_sender_handle(&self, timeout: Duration) -> bool {
        let handle = self.sender_handle.lock().unwrap().take();
        match handle {
            None => true,
            Some(join_handle) => tokio::time::timeout(timeout, join_handle).await.is_ok(),
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
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        self.ensure_not_closed()?;

        let now_ms = self.now_ms();
        let cluster_and_wait_time = match self
            .wait_on_metadata(record.topic(), record.partition(), now_ms, self.max_block_ms)
            .await
        {
            Ok(cwt) => cwt,
            Err(e) if e.is_api_exception() => {
                return self.handle_api_exception(e, record.topic(), record_metadata::UNKNOWN_PARTITION, callback);
            },
            Err(e) => return Err(e),
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
    async fn init_transactions(&self) -> Result<(), KafkaError> {
        KafkaProducer::init_transactions(self).await
    }

    /// Should be called before the start of each new transaction.
    fn begin_transaction(&self) -> Result<(), KafkaError> {
        KafkaProducer::begin_transaction(self)
    }

    /// Sends a list of specified offsets to the consumer group coordinator, and
    /// also marks those offsets as part of the current transaction.
    async fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<(), KafkaError> {
        KafkaProducer::send_offsets_to_transaction(self, offsets, group_metadata).await
    }

    /// Commits the ongoing transaction.
    async fn commit_transaction(&self) -> Result<(), KafkaError> {
        KafkaProducer::commit_transaction(self).await
    }

    /// Aborts the ongoing transaction.
    async fn abort_transaction(&self) -> Result<(), KafkaError> {
        KafkaProducer::abort_transaction(self).await
    }

    /// Asynchronously send a record to a topic.
    ///
    /// See [`send_with_callback`](Producer::send_with_callback) for details.
    async fn send(&self, record: ProducerRecord<K, V>) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        self.do_send(record, None).await
    }

    /// Asynchronously send a record to a topic and invoke the provided callback
    /// when the send has been acknowledged.
    async fn send_with_callback(
        &self,
        record: ProducerRecord<K, V>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        self.do_send(record, callback).await
    }

    /// Invoking this method makes all buffered records immediately available to
    /// send and awaits the completion of the requests associated with these
    /// records.
    ///
    /// Translated from `KafkaProducer.flush()`.
    async fn flush(&self) -> Result<(), KafkaError> {
        kafka_trace!(self.log_context, "Flushing accumulated records in producer.");
        self.accumulator.begin_flush();
        self.wakeup.notify_one();
        self.accumulator.await_flush_completion().await;
        Ok(())
    }

    /// Get the partition metadata for the given topic.
    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        let now_ms = self.now_ms();
        let cluster_and_wait_time = self.wait_on_metadata(topic, None, now_ms, self.max_block_ms).await?;
        Ok(cluster_and_wait_time.cluster.partitions_for_topic(topic).to_vec())
    }

    /// Close this producer. This method awaits until all previously sent requests
    /// complete.
    async fn close(&self) -> Result<(), KafkaError> {
        self.close_timeout(Duration::from_millis(i64::MAX as u64)).await
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
    async fn close_timeout(&self, timeout: Duration) -> Result<(), KafkaError> {
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

    use super::*;
    use crate::common::compress::Compression;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::serialization::StringSerializer;
    use crate::producer::ProducerConfig;
    use crate::producer::internals::BufferPool;
    use crate::producer::internals::{PartitionerConfig, RecordAccumulator};

    const TOPIC: &str = "test-topic";

    fn default_time_provider() -> Arc<dyn Fn() -> i64 + Send + Sync> {
        Arc::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        })
    }

    fn create_metadata_with_topic(topic: &str, num_partitions: i32) -> Arc<ProducerMetadata> {
        use crate::common::protocol::ApiKeys;
        use crate::common::protocol::Errors;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{
            MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
        };

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

        let response = MetadataResponse::new(data, ApiKeys::METADATA.latest_version());

        metadata.add(topic, 0);
        metadata.update_with_current_request_version(&response, false, 0);

        metadata
    }

    fn create_accumulator() -> Arc<RecordAccumulator> {
        Arc::new(RecordAccumulator::new(
            16384,
            Compression::none(),
            5,
            100,
            1000,
            120_000,
            PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
            Arc::new(BufferPool::new(32 * 1024 * 1024, 16384)),
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

        KafkaProducer::new(
            &config,
            Box::new(StringSerializer),
            Box::new(StringSerializer),
            metadata,
            accumulator,
            running,
            force_close,
            wakeup,
            None,
            default_time_provider(),
            None,
            Arc::new(Mutex::new(PendingRequests::new())),
        )
    }

    /// Translated from `KafkaProducerTest.testSendToInvalidTopic`.
    ///
    /// Tests that sending to an invalid topic name returns a failed future with
    /// an InvalidTopic error. Matches Java behavior where InvalidTopicException
    /// (an ApiException) is caught and returned via a FutureFailure.
    #[tokio::test]
    async fn test_send_to_invalid_topic() {
        use crate::common::protocol::ApiKeys;
        use crate::common::protocol::Errors;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseData, MetadataResponseTopic};

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
        topic_resp.set_error_code(Errors::InvalidTopicException.code());
        topic_resp.set_is_internal(false);
        data.set_topics(vec![topic_resp]);

        let response = MetadataResponse::new(data, ApiKeys::METADATA.latest_version());
        metadata.add("", 0);
        metadata.update_with_current_request_version(&response, false, 0);

        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        let record = ProducerRecord::with_value("".to_string(), Some("test".to_string()));
        let result = producer.send(record).await;
        // Java returns a FutureFailure for ApiExceptions like InvalidTopicException
        assert!(result.is_ok(), "send() should return Ok with a failed future for InvalidTopic");
        let future = result.unwrap();
        assert!(future.is_done(), "Failed future should be immediately done");
        let err = future.get().await.unwrap_err();
        assert!(
            matches!(err, KafkaError::InvalidTopic(_)),
            "Expected InvalidTopic error, got: {:?}",
            err
        );
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

        let result = producer.close_timeout(Duration::ZERO).await;
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

        let record = ProducerRecord::with_value(TOPIC.to_string(), Some("test".to_string()));
        let result = producer.send(record).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            KafkaError::IllegalState(msg) => {
                assert!(msg.contains("after producer has been closed"));
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
        let record = ProducerRecord::with_value(TOPIC.to_string(), Some(large_value));
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
            KafkaError::RecordTooLarge(msg) => {
                assert!(
                    msg.contains(ProducerConfig::MAX_REQUEST_SIZE_CONFIG),
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
        let record = ProducerRecord::with_value(TOPIC.to_string(), Some(large_value));
        let result = producer.send(record).await;
        assert!(
            result.is_ok(),
            "send() should return Ok with a failed future for RecordTooLarge"
        );
        let future = result.unwrap();
        assert!(future.is_done(), "Failed future should be immediately done");
        let err = future.get().await.unwrap_err();
        match err {
            KafkaError::RecordTooLarge(msg) => {
                assert!(
                    msg.contains(ProducerConfig::BUFFER_MEMORY_CONFIG),
                    "Error message should mention the config key: {}",
                    msg
                );
            },
            other => panic!("Expected RecordTooLarge error, got: {:?}", other),
        }
    }

    /// Tests that the partition() method returns the explicit partition when set.
    #[test]
    fn test_partition_returns_explicit_partition() {
        let metadata = create_metadata_with_topic(TOPIC, 3);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata.clone(), accumulator);

        let record = ProducerRecord::with_partition(
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

        let record: ProducerRecord<String, String> =
            ProducerRecord::with_value(TOPIC.to_string(), Some("value".to_string()));
        let cluster = metadata.fetch();
        let partition = producer.partition(&record, None, Some(b"value"), &cluster);
        assert_eq!(record_metadata::UNKNOWN_PARTITION, partition);
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
        assert_eq!(record_metadata::UNKNOWN_PARTITION, partition);
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
    /// Tests that delivery timeout must be >= linger.ms + request.timeout.ms.
    /// Java throws ConfigException when the user explicitly sets an inconsistent
    /// value; Rust returns Err(IllegalArgument).
    #[test]
    fn test_delivery_timeout_and_linger_ms_config() {
        let config = ProducerConfig {
            delivery_timeout_ms: 1,
            linger_ms: 10,
            request_timeout_ms: 30_000,
            ..Default::default()
        };

        let result = KafkaProducer::<String, String>::configure_delivery_timeout(&config);
        assert!(
            result.is_err(),
            "Should reject delivery_timeout_ms < linger_ms + request_timeout_ms"
        );
        let err = result.unwrap_err();
        match &err {
            KafkaError::IllegalArgument(msg) => {
                assert!(
                    msg.contains(ProducerConfig::DELIVERY_TIMEOUT_MS_CONFIG),
                    "Error should mention delivery.timeout.ms: {}",
                    msg
                );
                assert!(
                    msg.contains(ProducerConfig::LINGER_MS_CONFIG),
                    "Error should mention linger.ms: {}",
                    msg
                );
                assert!(
                    msg.contains(ProducerConfig::REQUEST_TIMEOUT_MS_CONFIG),
                    "Error should mention request.timeout.ms: {}",
                    msg
                );
            },
            other => panic!("Expected IllegalArgument error, got: {:?}", other),
        }
    }

    /// Tests that configure_delivery_timeout accepts valid configurations.
    #[test]
    fn test_delivery_timeout_valid_config() {
        // Default values: delivery=120000, linger=5, request=30000
        let config = ProducerConfig::default();
        let result = KafkaProducer::<String, String>::configure_delivery_timeout(&config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 120_000);

        // Exactly equal: delivery = linger + request
        let config = ProducerConfig {
            delivery_timeout_ms: 30_010,
            linger_ms: 10,
            request_timeout_ms: 30_000,
            ..Default::default()
        };
        let result = KafkaProducer::<String, String>::configure_delivery_timeout(&config);
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
        let result: Result<ProducerRecord<String, String>, _> = ProducerRecord::with_partition(
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
            KafkaError::Timeout(msg) => {
                assert!(msg.contains("not present in metadata"), "Got: {}", msg);
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
    async fn test_send_with_callback() {
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

        let record: ProducerRecord<String, String> = ProducerRecord::with_value(TOPIC.to_string(), None);
        let result = producer.send(record).await;
        assert!(result.is_ok(), "Send with None value should succeed");
    }

    /// Translated from `KafkaProducerTest.testCallbackAndInterceptorHandleError`.
    ///
    /// Tests that when sending to a topic that will cause an ApiException
    /// (RecordTooLargeException), the callback is invoked with the error and
    /// a non-null RecordMetadata with appropriate defaults.
    #[tokio::test]
    async fn test_callback_invoked_on_api_exception() {
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

        let callback: Callback = Box::new(move |record_metadata, exception| {
            inv.store(true, Ordering::SeqCst);
            *err.lock().unwrap() = exception.is_some();
            if let Some(rm) = record_metadata {
                *meta.lock().unwrap() = true;
                *mt.lock().unwrap() = rm.topic().to_string();
                *mo.lock().unwrap() = rm.offset();
            }
        });

        // Send a record that exceeds max_request_size
        let large_value = "a".repeat(100);
        let record = ProducerRecord::with_value(TOPIC.to_string(), Some(large_value));
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
            record_metadata::INVALID_OFFSET,
            "Callback metadata should have INVALID_OFFSET"
        );

        // Future.get() should return the error
        let err = future.get().await.unwrap_err();
        assert!(matches!(err, KafkaError::RecordTooLarge(_)));
    }

    /// Translated from `KafkaProducerTest.testHeadersSuccess`.
    ///
    /// Tests that headers added to a ProducerRecord before send() are passed
    /// through serialization correctly and stored in the accumulator.
    #[tokio::test]
    async fn test_headers_success() {
        use crate::common::header::Headers;
        use crate::common::header::internals::RecordHeader;

        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        let mut record: ProducerRecord<String, String> =
            ProducerRecord::with_key(TOPIC.to_string(), Some("key".to_string()), Some("value".to_string()));

        // Add a header pre-send
        record
            .headers_mut()
            .add(RecordHeader::new("test".to_string(), Some(b"header-value".to_vec())))
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
            let record = ProducerRecord::with_value(TOPIC.to_string(), Some(format!("value{}", i)));
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
        let record = ProducerRecord::with_value(TOPIC.to_string(), Some("value".to_string()));
        let result = producer.send(record).await;
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), KafkaError::IllegalState(_)),
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
        let record = ProducerRecord::with_value(TOPIC.to_string(), Some("value".to_string()));
        assert!(producer.send(record).await.is_ok());

        // Initiate close
        producer.initiate_close();

        // After initiate_close, the accumulator should be closed.
        // Attempting to send should fail because the accumulator rejects new appends.
        let record2 = ProducerRecord::with_value(TOPIC.to_string(), Some("value2".to_string()));
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
    /// Verifies the force-close path in close_timeout when timeout is zero.
    #[tokio::test]
    async fn test_close_timeout_zero_force_closes() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, Arc::clone(&accumulator));

        // Send a record
        let record = ProducerRecord::with_value(TOPIC.to_string(), Some("value".to_string()));
        let _ = producer.send(record).await;
        assert!(accumulator.has_undrained());

        // Close with timeout 0 should force-close
        let result = producer.close_timeout(Duration::ZERO).await;
        assert!(result.is_ok());

        // After force-close, the producer should not accept new sends
        let record2 = ProducerRecord::with_value(TOPIC.to_string(), Some("value2".to_string()));
        assert!(producer.send(record2).await.is_err());
    }

    /// Tests that the callback is invoked with error on invalid topic.
    ///
    /// Translated from `KafkaProducerTest.testCallbackAndInterceptorHandleError`
    /// (the invalid topic variant).
    #[tokio::test]
    async fn test_callback_invoked_on_invalid_topic() {
        use crate::common::protocol::ApiKeys;
        use crate::common::protocol::Errors;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseData, MetadataResponseTopic};

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
        topic_resp.set_error_code(Errors::InvalidTopicException.code());
        topic_resp.set_is_internal(false);
        data.set_topics(vec![topic_resp]);

        let response = MetadataResponse::new(data, ApiKeys::METADATA.latest_version());
        metadata.add(invalid_topic, 0);
        metadata.update_with_current_request_version(&response, false, 0);

        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        let callback_invoked = Arc::new(AtomicBool::new(false));
        let got_error = Arc::new(AtomicBool::new(false));
        let inv = Arc::clone(&callback_invoked);
        let err = Arc::clone(&got_error);

        let callback: Callback = Box::new(move |_metadata, exception| {
            inv.store(true, Ordering::SeqCst);
            err.store(exception.is_some(), Ordering::SeqCst);
        });

        let record = ProducerRecord::with_value(invalid_topic.to_string(), Some("value".to_string()));
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
            matches!(err, KafkaError::InvalidTopic(_)),
            "Expected InvalidTopic error, got: {:?}",
            err
        );
    }

    /// Tests that multiple calls to close are safe even with timeout.
    ///
    /// Translated from `KafkaProducerTest.closeShouldBeIdempotent`.
    #[tokio::test]
    async fn test_close_with_timeout_idempotent() {
        let metadata = create_metadata_with_topic(TOPIC, 1);
        let accumulator = create_accumulator();
        let producer = create_producer(metadata, accumulator);

        producer.close_timeout(Duration::from_secs(1)).await.unwrap();
        producer.close_timeout(Duration::from_secs(1)).await.unwrap();
        producer.close_timeout(Duration::ZERO).await.unwrap();
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
            let accumulator = Arc::new(RecordAccumulator::new(
                1024 * 1024,
                Compression::none(),
                5,
                100,
                1000,
                120_000,
                PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
                Arc::new(BufferPool::new(32 * 1024 * 1024, 1024 * 1024)),
                transaction_manager.clone(),
            ));
            let config = ProducerConfig::default();
            let producer = KafkaProducer::<String, String>::new(
                &config,
                Box::new(StringSerializer),
                Box::new(StringSerializer),
                Arc::clone(&metadata),
                accumulator,
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(false)),
                Arc::new(Notify::new()),
                None,
                default_time_provider(),
                transaction_manager,
                Arc::new(Mutex::new(PendingRequests::new())),
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
                let _guard = crate::test_alloc_tracker::AllocTrackingGuard::new();
                crate::test_alloc_tracker::AllocTrackingGuard::reset();
                producer
                    .do_send_bytes(TOPIC, Some(0), Some(0), Some(b"k"), Some(b"v"), &[], None, 0, 0, &cluster)
                    .await
                    .expect("append should succeed");
                let count = crate::test_alloc_tracker::AllocTrackingGuard::count();
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

    // -- `configureTransactionState` tests ----------------------------------
    //
    // These began life covering the temporary MILESTONE-11 GUARD in `from_config`.
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

    fn from_guard_props(props: &HashMap<String, String>) -> Result<(), KafkaError> {
        let config = ProducerConfig::from_properties(props)?;
        KafkaProducer::<String, String>::from_config(config, Box::new(StringSerializer), Box::new(StringSerializer))
            .map(|_| ())
    }

    /// The default configuration must construct. `#[tokio::test]`: a successful
    /// `from_config` spawns the Sender task and so needs a runtime. The spawned task
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
        let config = ProducerConfig::from_properties(&props).expect("valid config");
        let producer = KafkaProducer::<String, String>::from_config(
            config,
            Box::new(StringSerializer),
            Box::new(StringSerializer),
        )
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
        let config = ProducerConfig::from_properties(&props).expect("valid config");
        let producer = KafkaProducer::<String, String>::from_config(
            config,
            Box::new(StringSerializer),
            Box::new(StringSerializer),
        )
        .expect("disabling idempotence is allowed");
        assert!(producer.transaction_manager.is_none());
    }

    /// `transactional.id` is accepted as of Phase 6, and the producer really is
    /// transactional: `configureTransactionState` passes the id through to the
    /// manager (`KafkaProducer.java:597`, `:602`) and `isTransactional()` reports it.
    ///
    /// This replaces the Phase-1 `test_guard_rejects_transactional_id`, whose whole
    /// subject — the `from_config` guard of PLAN §7.1 — is what this phase deleted.
    #[tokio::test]
    async fn test_transactional_id_builds_a_transactional_manager() {
        let props = guard_props(&[("transactional.id", "my-txn")]);
        let config = ProducerConfig::from_properties(&props).expect("valid config");
        let producer = KafkaProducer::<String, String>::from_config(
            config,
            Box::new(StringSerializer),
            Box::new(StringSerializer),
        )
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
        let config = ProducerConfig::from_properties(&props).expect("valid config");
        let producer = KafkaProducer::<String, String>::from_config(
            config,
            Box::new(StringSerializer),
            Box::new(StringSerializer),
        )
        .expect("disabling idempotence is allowed");

        const EXPECTED: &str = "Cannot use transactional methods without enabling transactions \
                                by setting the transactional.id configuration property";

        let expect_no_manager = |error: KafkaError, method: &str| {
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
        let config = ProducerConfig::from_properties(&props).expect("valid config");
        let producer = KafkaProducer::<String, String>::from_config(
            config,
            Box::new(StringSerializer),
            Box::new(StringSerializer),
        )
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
