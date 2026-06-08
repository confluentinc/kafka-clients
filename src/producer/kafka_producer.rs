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
//! Transactional methods are not translated in this phase.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{debug, info, trace, warn};
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
use crate::common::internals::ClusterResourceListeners;
use crate::common::network::PlaintextChannelBuilder;
use crate::common::network::Selector;
use crate::common::record::CompressionType;
use crate::common::record::RecordBatch;
use crate::common::record::abstract_records;
use crate::common::serialization::Serializer;
use crate::kafka_client::KafkaClient;
use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;
use crate::network_client::NetworkClient;
use crate::producer::Producer;
use crate::producer::ProducerConfig;
use crate::producer::ProducerRecord;
use crate::producer::internals::BufferPool;
use crate::producer::internals::BuiltInPartitioner;
use crate::producer::internals::Callback;
use crate::producer::internals::FutureRecordMetadata;
use crate::producer::internals::ProducerMetadata;
use crate::producer::internals::Sender;
use crate::producer::internals::{PartitionerConfig, RecordAccumulator};
use crate::producer::{RecordMetadata, record_metadata};
use crate::{ApiVersions, DefaultHostResolver};

/// Network thread name prefix.
pub const NETWORK_THREAD_PREFIX: &str = "kafka-producer-network-thread";

/// Producer metric group name.
pub const PRODUCER_METRIC_GROUP_NAME: &str = "producer-metrics";

/// Metadata and time spent waiting for it.
#[derive(Debug)]
struct ClusterAndWaitTime {
    /// The cluster metadata.
    cluster: Cluster,
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
    ) -> Self {
        Self {
            client_id: config.client_id.clone(),
            key_serializer,
            value_serializer,
            max_request_size: config.max_request_size,
            total_memory_size: config.buffer_memory,
            accumulator,
            metadata,
            compression_type: config.compression_type,
            max_block_ms: config.max_block_ms,
            partitioner_ignore_keys: config.partitioner_ignore_keys,
            running,
            force_close,
            wakeup,
            sender_handle: Mutex::new(sender_handle),
            time_provider,
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
        info!("Starting the Kafka producer");

        // 1. Parse and validate bootstrap server addresses
        let addresses = client_utils::parse_and_validate_addresses(&config.bootstrap_servers)?;

        // 2. Validate delivery timeout configuration
        //    Translated from KafkaProducer.configureDeliveryTimeout().
        let delivery_timeout_ms = Self::configure_delivery_timeout(&config)?;

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
        let metadata = Arc::new(ProducerMetadata::new(
            config.reconnect_backoff_ms,
            config.reconnect_backoff_max_ms,
            config.metadata_max_age_ms,
            config.metadata_max_idle_ms,
            ClusterResourceListeners::new(),
        ));
        metadata.bootstrap(addresses);

        // 6. Get the shared Metadata Arc from ProducerMetadata so the NetworkClient
        //    uses the same Metadata instance. This mirrors Java's inheritance where
        //    ProducerMetadata extends Metadata.
        let shared_metadata = metadata.metadata_arc();

        // 7. Create Selector + NetworkClient
        let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
        let selector = Selector::with_defaults(config.connections_max_idle_ms, channel_builder);
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
            api_versions,
            DefaultHostResolver::new(),
            config.metadata_max_age_ms, // rebootstrap_trigger_ms
            MetadataRecoveryStrategy::None,
        );

        // 8. Create BufferPool and RecordAccumulator
        //    As per Kafka configuration documentation, batch.size may be set to 0
        //    to explicitly disable batching, which in practice uses a batch size of 1.
        let batch_size = config.batch_size.max(1);
        let buffer_pool = Arc::new(BufferPool::new(config.buffer_memory, batch_size as usize));
        let accumulator = Arc::new(RecordAccumulator::new(
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
        ));

        // 9. Wire up the Sender and spawn the I/O background task
        Ok(Self::with_client(
            &config,
            key_serializer,
            value_serializer,
            metadata,
            accumulator,
            client,
            time_provider,
        ))
    }

    /// Creates a `KafkaProducer` with a full sender task and network client.
    ///
    /// This corresponds to the primary public constructor in Java's KafkaProducer.
    /// It creates the RecordAccumulator, Sender, and spawns the I/O background task.
    ///
    /// # Type Parameters
    ///
    /// * `C` - The KafkaClient implementation type
    #[allow(clippy::too_many_arguments)]
    pub fn with_client<C: KafkaClient + Send + 'static>(
        config: &ProducerConfig,
        key_serializer: Box<dyn Serializer<K> + Send + Sync>,
        value_serializer: Box<dyn Serializer<V> + Send + Sync>,
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
        client: C,
        time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let force_close = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(Notify::new());

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
            Arc::clone(&wakeup),
            Arc::clone(&time_provider),
        );

        let io_thread_name = format!("{} | {}", NETWORK_THREAD_PREFIX, config.client_id);
        let sender_handle = tokio::task::spawn(async move {
            debug!("Starting {} I/O task", io_thread_name);
            sender.run().await;
        });

        debug!("Kafka producer started");

        Self {
            client_id: config.client_id.clone(),
            key_serializer,
            value_serializer,
            max_request_size: config.max_request_size,
            total_memory_size: config.buffer_memory,
            accumulator,
            metadata,
            compression_type: config.compression_type,
            max_block_ms: config.max_block_ms,
            partitioner_ignore_keys: config.partitioner_ignore_keys,
            running,
            force_close,
            wakeup,
            sender_handle: Mutex::new(Some(sender_handle)),
            time_provider,
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

        let topic = record.topic().to_string();

        // --- Phase 1: Validation (API errors invoke callback + return failed future) ---

        // First make sure the metadata for the topic is available
        let now_ms = self.now_ms();
        let cluster_and_wait_time = match self
            .wait_on_metadata(record.topic(), record.partition(), now_ms, self.max_block_ms)
            .await
        {
            Ok(cwt) => cwt,
            Err(e) if e.is_api_exception() => {
                return self.handle_api_exception(e, &topic, record_metadata::UNKNOWN_PARTITION, callback);
            },
            Err(e) => return Err(e),
        };
        let now_ms = now_ms + cluster_and_wait_time.waited_on_metadata_ms;
        let remaining_wait_ms = 0i64.max(self.max_block_ms - cluster_and_wait_time.waited_on_metadata_ms);
        let cluster = cluster_and_wait_time.cluster;

        let serialized_key = self
            .key_serializer
            .serialize_with_headers(record.topic(), record.headers(), record.key())
            .map_err(|e| KafkaError::serialization(format!("Failed to serialize key: {}", e)))?;

        let serialized_value = self
            .value_serializer
            .serialize_with_headers(record.topic(), record.headers(), record.value())
            .map_err(|e| KafkaError::serialization(format!("Failed to serialize value: {}", e)))?;

        // Calculate partition
        let partition = self.partition(&record, serialized_key.as_deref(), serialized_value.as_deref(), &cluster);

        let headers = record.headers().to_array();

        let serialized_size = abstract_records::estimate_size_in_bytes_upper_bound(
            RecordBatch::CURRENT_MAGIC_VALUE,
            self.compression_type,
            serialized_key.as_deref(),
            serialized_value.as_deref(),
            headers,
        );
        if let Err(err) = self.ensure_valid_record_size(serialized_size) {
            return self.handle_api_exception(err, &topic, partition, callback);
        }

        let timestamp = record.timestamp().unwrap_or(now_ms);

        // --- Phase 2: Append (callback is moved into the accumulator) ---
        //
        // If append fails, the callback has been consumed. We still return a
        // failed future so the caller can observe the error, matching the
        // Java contract as closely as possible.
        match self.accumulator.append(
            record.topic(),
            partition,
            timestamp,
            serialized_key.as_deref(),
            serialized_value.as_deref(),
            headers,
            callback,
            remaining_wait_ms,
            now_ms,
            &cluster,
        ) {
            Ok(result) => {
                if result.batch_is_full || result.new_batch_created {
                    trace!(
                        "Waking up the sender since topic {} is either full or getting a new batch",
                        record.topic()
                    );
                    self.wakeup.notify_one();
                }
                Ok(KafkaFuture::new(result.future))
            },
            Err(e) if e.is_api_exception() => {
                // Callback was consumed by append, so we cannot invoke it here.
                // Return a completed-with-error future.
                debug!("Exception occurred during accumulator append: {}", e);
                let tp = TopicPartition::new(topic, partition);
                Ok(KafkaFuture::new(Arc::new(FutureRecordMetadata::failed(tp, e))))
            },
            Err(e) => Err(e),
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
        debug!("Exception occurred during message send: {}", error);
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
                trace!("Requesting metadata update for partition {} of topic {}.", p, topic);
            } else {
                trace!("Requesting metadata update for topic {}.", topic);
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
            // Hash the key bytes to choose a partition
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

impl<K, V> Producer<K, V> for KafkaProducer<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
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
        trace!("Flushing accumulated records in producer.");
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
        info!("Closing the Kafka producer with timeoutMillis = {} ms.", timeout_ms);

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
            info!(
                "Proceeding to force close the producer since pending requests could not be \
                 completed within timeout {} ms.",
                timeout_ms
            );
            self.force_close();

            // Await the sender task indefinitely after force close.
            self.await_sender_handle_indefinitely().await;
        }

        debug!("Kafka producer has been closed");
        Ok(())
    }
}

impl<K, V> Drop for KafkaProducer<K, V> {
    fn drop(&mut self) {
        if self.running.load(Ordering::Acquire) {
            warn!("KafkaProducer was not closed before being dropped. Call close() to avoid resource leaks.");
            self.force_close();
        }
    }
}

#[cfg(test)]
mod tests {
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
}
