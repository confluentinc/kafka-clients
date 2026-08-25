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
use crate::common::protocol::Errors;
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
    /// (Through Phase 5 that unreachability was also the reason the temporary
    /// MILESTONE-11 GUARD in [`Self::from_config`] was not duplicated here. Phase 6
    /// removed the guard, so nothing turns on it any more; the visibility note above
    /// stands on its own.)
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
                    //
                    // The guard is bound to its own statement so it is released at the
                    // `;`. It MUST NOT stay alive into the error handling below:
                    // `handle_api_exception` re-locks this same non-reentrant
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
                        let is_api_exception = error.is_api_exception()
                            // This crate spells a bare `KafkaException` as
                            // `Errors::UnknownServerError` for want of a wire code
                            // (`transaction_manager.rs`, `maybe_fail_with_error`), which
                            // `is_api_exception` cannot tell from a genuine
                            // `UnknownServerException`. It is unambiguous here: this arm
                            // sees only what `maybeAddPartition` raises locally, never a
                            // broker error. Misfiling it would overwrite `last_error`
                            // with "we are in an error state" and lose the real cause.
                            && error.error() != Errors::UnknownServerError;
                        if is_api_exception {
                            let partition = result.topic_partition.partition();
                            return self.handle_api_exception(error, topic, partition, None);
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
    ///
    /// Locks the [`TransactionManager`]. Java's monitor is reentrant and
    /// `std::sync::Mutex` is not, so no caller — directly or through
    /// [`handle_api_exception`](Self::handle_api_exception) — may already hold that
    /// lock, including in a still-live `if let` / `match` scrutinee temporary.
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
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        self.ensure_not_closed()?;
        // This method is `doSend`'s zero-copy twin, so it owes the same two entry
        // guards (`KafkaProducer.java:988-989`). Without this a 2PC caller could send
        // through the FFI path while the transaction was prepared, which
        // `Self::do_send` refuses.
        self.throw_if_in_prepared_state()?;

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
    use std::sync::atomic::AtomicI64;

    use super::*;
    use crate::common::Node;
    use crate::common::compress::Compression;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::protocol::Errors;
    use crate::common::requests::ConcreteResponse;
    use crate::common::serialization::StringSerializer;
    use crate::mock_client::MockClient;
    use crate::producer::ProducerConfig;
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
        use crate::api_versions_response_data::{FinalizedFeatureKey, SupportedFeatureKey};
        use crate::node_api_versions::NodeApiVersions;

        const FEATURE: &str = "transaction.version";

        let mut supported = SupportedFeatureKey::new();
        supported.set_name(FEATURE.to_string());
        supported.set_max_version(level);
        supported.set_min_version(0);

        let mut finalized = FinalizedFeatureKey::new();
        finalized.set_name(FEATURE.to_string());
        finalized.set_max_version_level(level);
        finalized.set_min_version_level(level);

        api_versions.update("0", NodeApiVersions::new(&[], &[supported], &[finalized], 0));
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
            let config = ProducerConfig::from_properties(&props).expect("valid config");
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
            let update = crate::common::requests::request_test_utils::metadata_update_with_ids(
                "kafka-cluster",
                1,
                &HashMap::new(),
                &HashMap::from([(TOPIC.to_string(), num_partitions)]),
                &|_| None,
                &HashMap::from([(TOPIC.to_string(), topic_id())]),
            );
            metadata.update_with_current_request_version(&update, false, time.milliseconds());

            let batch_size = config.batch_size.max(1);
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
                Arc::new(BufferPool::new(config.buffer_memory, batch_size as usize)),
                Some(Arc::clone(&transaction_manager)),
                log_context.clone(),
            ));

            let client = MockClient::new(vec![coordinator_node()], time.as_provider());
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
                Arc::clone(&running),
                Arc::clone(&force_close),
                time.as_provider(),
                Some(Arc::clone(&transaction_manager)),
                Arc::clone(&pending_requests),
                log_context.clone(),
            );

            let producer = KafkaProducer::new(
                &config,
                Box::new(StringSerializer),
                Box::new(StringSerializer),
                Arc::clone(&metadata),
                Arc::clone(&accumulator),
                running,
                force_close,
                wakeup,
                None,
                time.as_provider(),
                Some(Arc::clone(&transaction_manager)),
                pending_requests,
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
        let done = AtomicBool::new(false);
        let op = async {
            let out = op.await;
            done.store(true, Ordering::SeqCst);
            out
        };
        let driver = async {
            while !done.load(Ordering::SeqCst) {
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
        use crate::common::requests::InitProducerIdResponse;
        use crate::init_producer_id_response_data::InitProducerIdResponseData;

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
        use crate::common::requests::EndTxnResponse;
        use crate::end_txn_response_data::EndTxnResponseData;

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
        assert_eq!(error.message(), "Timeout expired after 500ms while awaiting InitProducerId");

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
        assert_eq!(error.message(), "Timeout expired after 500ms while awaiting InitProducerId");
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
        assert_eq!(error.message(), "Timeout expired after 5ms while awaiting InitProducerId");

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
            .close_timeout(Duration::from_millis(0))
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

        let record = ProducerRecord::new(
            TOPIC.to_string(),
            None,
            Some(ctx.time.milliseconds()),
            Some("key".to_string()),
            Some("value".to_string()),
            None,
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
    // `std::sync::Mutex` through `handle_api_exception` →
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
                 before handling the error, because handle_api_exception re-locks it."
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
            matches!(error, KafkaError::IllegalState(_)),
            "IllegalStateException is not an ApiException, so it must be returned by send() \
             rather than reported through the future; got {error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "IllegalStateError: Cannot add partition {TOPIC}-0 to transaction before completing a call to initTransactions"
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
            matches!(error, KafkaError::IllegalState(_)),
            "expected the IllegalState to be returned by send(), got {error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!("IllegalStateError: Cannot add partition {TOPIC}-0 to transaction while in state  READY")
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
                .transition_to_abortable_error(KafkaError::with_message(Errors::InvalidTxnState, "cause"), Caller::App)
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
            "this crate spells Java's bare KafkaException as UnknownServerError; got {error:?}"
        );
        assert_eq!(
            error.to_string(),
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
        let error = bounded_block_on("send after a fatal error", || async {
            let mut ctx = TxnProducerContext::transactional();
            init_transactions(&mut ctx).await;
            ctx.transaction_manager
                .lock()
                .unwrap()
                .transition_to_fatal_error(
                    KafkaError::with_message(Errors::ClusterAuthorizationFailed, "cause"),
                    Caller::App,
                )
                .expect("FATAL_ERROR is always reachable");
            ctx.producer
                .send(misuse_record())
                .await
                .expect_err("a fatal error blocks further sends")
        });

        assert_eq!(error.error(), Errors::UnknownServerError, "got {error:?}");
        assert_eq!(
            error.to_string(),
            "Cannot execute transactional method because we are in an error state"
        );
        // A fatal-state error surfaced from `maybe_fail_with_error` must report
        // `is_fatal()` so an application testing `if !err.is_fatal() { retry }`
        // does not retry a dead (fenced/fatally-errored) producer forever. This
        // is the production-path regression for the fatal-branch stamp added to
        // `TransactionManager::maybe_fail_with_error`.
        assert!(error.is_fatal(), "a fatal-state error must report is_fatal(): {error:?}");
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
                .transition_to_fatal_error(KafkaError::with_message(Errors::UnsupportedVersion, "cause"), Caller::App)
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
    /// `handle_api_exception` → `maybe_transition_to_error_state` — so it is the direct
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
                .transition_to_abortable_error(KafkaError::with_message(Errors::ProducerFenced, "fenced"), Caller::App)
                .expect("IN_TRANSACTION -> ABORTABLE_ERROR is valid");
            let fatal_before = ctx.transaction_manager.lock().unwrap().has_fatal_error();

            let send_result = match ctx.producer.send(misuse_record()).await {
                Ok(future) => Ok(future.get().await.expect_err("the fenced send cannot be acked")),
                Err(error) => Err(error),
            };
            let fatal_after = ctx.transaction_manager.lock().unwrap().has_fatal_error();
            (send_result, fatal_before, fatal_after)
        });

        let error = send_result.expect("an ApiException is reported through the future, not the call");
        assert_eq!(
            error.error(),
            Errors::ProducerFenced,
            "ProducerFencedException is an ApiException, so doSend returns a failed future; got {error:?}"
        );
        // `maybeFailWithError` re-raises rather than re-throwing `lastError`, so the
        // message is the fresh one built at Java 1159-1161 — not the "fenced" text the
        // test seeded.
        assert_eq!(
            error.to_string(),
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
            "the ApiException block runs maybeTransitionToErrorState, which moves a fenced \
             producer from ABORTABLE_ERROR to FATAL_ERROR; the rethrow path would not"
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
    async fn test_commit_transaction_with_record_too_large_exception() {
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
            .expect("an ApiException is reported through the future, not the call");
        let send_error = future.get().await.expect_err("the record is too large");
        assert!(
            matches!(send_error, KafkaError::RecordTooLarge(_)),
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

        let record = ProducerRecord::with_value(TOPIC.to_string(), Some("value".to_string()));
        let future = ctx
            .producer
            .send(record)
            .await
            .expect("a timeout is an ApiException and is reported through the future");
        let send_error = future.get().await.expect_err("the topic never appears in metadata");
        assert!(
            matches!(send_error, KafkaError::Timeout(_)),
            "expected Timeout, got {:?}",
            send_error
        );

        // Java asserts a bare `KafkaException` — see
        // `test_commit_transaction_with_record_too_large_exception`.
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

        let record = ProducerRecord::with_partition(TOPIC.to_string(), Some(2), None, Some("value".to_string()))
            .expect("a valid partition");
        let future = ctx
            .producer
            .send(record)
            .await
            .expect("a timeout is an ApiException and is reported through the future");
        let send_error = future.get().await.expect_err("partition 2 never appears in metadata");
        assert!(
            matches!(send_error, KafkaError::Timeout(_)),
            "expected Timeout, got {:?}",
            send_error
        );

        // Java asserts a bare `KafkaException` — see
        // `test_commit_transaction_with_record_too_large_exception`.
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
        use crate::common::protocol::ApiKeys;
        use crate::common::requests::MetadataResponse;
        use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseData, MetadataResponseTopic};

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
        invalid.set_error_code(Errors::InvalidTopicException.code());
        data.set_topics(vec![invalid]);
        let response = MetadataResponse::new(data, ApiKeys::METADATA.latest_version());
        ctx.metadata.add(INVALID_TOPIC, ctx.time.milliseconds());
        ctx.metadata
            .update_with_current_request_version(&response, false, ctx.time.milliseconds());

        let record = ProducerRecord::with_value(INVALID_TOPIC.to_string(), Some("HelloKafka".to_string()));
        let future = ctx
            .producer
            .send(record)
            .await
            .expect("an InvalidTopicException is reported through the future");
        let send_error = future.get().await.expect_err("the topic name is invalid");
        assert!(
            matches!(send_error, KafkaError::InvalidTopic(_)),
            "expected InvalidTopic, got {:?}",
            send_error
        );

        // Java asserts a bare `KafkaException` — see
        // `test_commit_transaction_with_record_too_large_exception`.
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
        let group_metadata = ConsumerGroupMetadata::with_details("group", 5, "member", Some("instance".to_string()));
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
        let group_metadata = ConsumerGroupMetadata::with_details(
            "group",
            2,
            crate::common::requests::txn_offset_commit_request::UNKNOWN_MEMBER_ID,
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
        use crate::common::requests::ProduceResponse;
        use crate::produce_response_data::{PartitionProduceResponse, ProduceResponseData, TopicProduceResponse};

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
        use crate::add_offsets_to_txn_response_data::AddOffsetsToTxnResponseData;
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
        ConcreteResponse::TxnOffsetCommit(TxnOffsetCommitResponse::from_error_map(10, &error_map))
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

        let record = ProducerRecord::with_partition(
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

        let record = ProducerRecord::with_partition(
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
    /// [`KafkaProducer::with_client`] does in production.
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
        let config = ProducerConfig::from_properties(&props).expect("valid config");
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
        let update = crate::common::requests::request_test_utils::metadata_update_with(
            1,
            &HashMap::from([(TOPIC.to_string(), 1)]),
        );
        metadata.update_with_current_request_version(&update, false, time.milliseconds());

        let batch_size = config.batch_size.max(1);
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
            Arc::new(BufferPool::new(config.buffer_memory, batch_size as usize)),
            Some(Arc::clone(&transaction_manager)),
            log_context,
        ));

        let mut client = MockClient::new(vec![coordinator_node()], time.as_provider());
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
            Arc::clone(&running),
            Arc::clone(&force_close),
            time.as_provider(),
            Some(Arc::clone(&transaction_manager)),
            Arc::clone(&pending_requests),
            LogContext::empty(),
        );

        // # Why the Sender gets its own thread and runtime here
        //
        // `with_client` would `tokio::task::spawn` it onto the test's runtime, and that
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

        KafkaProducer::new(
            &config,
            Box::new(StringSerializer),
            Box::new(StringSerializer),
            metadata,
            accumulator,
            running,
            force_close,
            wakeup,
            Some(sender_handle),
            time.as_provider(),
            Some(transaction_manager),
            pending_requests,
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
        producer.close_timeout(Duration::from_millis(1000)).await.expect("close");
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

    /// The contract: `close_timeout` must not return before the Sender has finished.
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

        producer.close_timeout(Duration::from_millis(1000)).await.expect("close");
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
    //          -> test_commit_transaction_with_record_too_large_exception
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
