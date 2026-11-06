// Copyright 2026 Confluent Inc.
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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// Java's `org.apache.kafka.clients.producer.ProducerConfig`;
// `cargo xtask check-generated` checks it is up to date.

/**
 * The constants of Java's `org.apache.kafka.clients.producer.ProducerConfig`:
 * every `public static final` field, evaluated from the Java source, is a
 * static property of the same name; a deprecated one is left out. A `List` or
 * `Set` constant is a frozen array in Java's order. The constants Java's class
 * inherits from `AbstractConfig` (`CONFIG_PROVIDERS_CONFIG`, ...) are
 * properties of the class too. The `ConfigDef` machinery is not translated (a
 * configuration is a plain object of these keys, and the `ConfigDef` types are
 * the types the client checks), so `ProducerConfig` is not instantiable:
 * constructing it throws `TypeError`. Not generated:
 * `ProducerConfig(Properties)`, `ProducerConfig(Map<String, Object>)`,
 * `configNames()`, `configDef()`, `main(String[])`, the `ConfigDef` machinery
 * or methods that take a type the client does not translate (`ConfigDef`,
 * `AbstractConfig`).
 *
 * Java: `org.apache.kafka.clients.producer.ProducerConfig`.
 */
export class ProducerConfig {
  /**
   * A list of host/port pairs used to establish the initial connection to the
   * Kafka cluster. Clients use this list to bootstrap and discover the full set
   * of Kafka brokers. While the order of servers in the list does not matter,
   * we recommend including more than one server to ensure resilience if any
   * servers are down. This list does not need to contain the entire set of
   * brokers, as Kafka clients automatically manage and update connections to
   * the cluster efficiently. This list must be in the form
   * `host1:port1,host2:port2,...`.
   */
  static readonly BOOTSTRAP_SERVERS_CONFIG: string = 'bootstrap.servers';
  /**
   * Controls how the client uses DNS lookups. If set to `use_all_dns_ips`,
   * connect to each returned IP address in sequence until a successful
   * connection is established. After a disconnection, the next IP is used. Once
   * all IPs have been used once, the client resolves the IP(s) from the
   * hostname again (both the JVM and the OS cache DNS name lookups, however).
   * If set to `resolve_canonical_bootstrap_servers_only`, resolve each
   * bootstrap address into a list of canonical names. After the bootstrap
   * phase, this behaves the same as `use_all_dns_ips`.
   */
  static readonly CLIENT_DNS_LOOKUP_CONFIG: string = 'client.dns.lookup';
  /**
   * The period of time in milliseconds after which we force a refresh of
   * metadata even if we haven't seen any partition leadership changes to
   * proactively discover any new brokers or partitions.
   */
  static readonly METADATA_MAX_AGE_CONFIG: string = 'metadata.max.age.ms';
  /**
   * Controls how long the producer will cache metadata for a topic that's idle.
   * If the elapsed time since a topic was last produced to exceeds the metadata
   * idle duration, then the topic's metadata is forgotten and the next access
   * to it will force a metadata fetch request.
   */
  static readonly METADATA_MAX_IDLE_CONFIG: string = 'metadata.max.idle.ms';
  /**
   * The producer will attempt to batch records together into fewer requests
   * whenever multiple records are being sent to the same partition. This helps
   * performance on both the client and the server. This configuration controls
   * the default batch size in bytes.
   *
   * No attempt will be made to batch records larger than this size.
   *
   * Requests sent to brokers will contain multiple batches, one for each
   * partition with data available to be sent.
   *
   * A small batch size will make batching less common and may reduce throughput
   * (a batch size of zero will disable batching entirely). A very large batch
   * size may use memory a bit more wastefully as we will always allocate a
   * buffer of the specified batch size in anticipation of additional records.
   *
   * Note: This setting gives the upper bound of the batch size to be sent. If
   * we have fewer than this many bytes accumulated for this partition, we will
   * 'linger' for the `linger.ms` time waiting for more records to show up. This
   * `linger.ms` setting defaults to 5, which means the producer will wait for
   * 5ms or until the record batch is of `batch.size` (whichever happens first)
   * before sending the record batch. Note that broker backpressure can result
   * in a higher effective linger time than this setting. The default changed
   * from 0 to 5 in Apache Kafka 4.0 as the efficiency gains from larger batches
   * typically result in similar or lower producer latency despite the increased
   * linger.
   */
  static readonly BATCH_SIZE_CONFIG: string = 'batch.size';
  /**
   * When set to 'true', the producer will try to adapt to broker performance
   * and produce more messages to partitions hosted on faster brokers. If
   * 'false', the producer will try to distribute messages uniformly. Note: this
   * setting has no effect if a custom partitioner is used.
   */
  static readonly PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG: string = 'partitioner.adaptive.partitioning.enable';
  /**
   * If a broker cannot process produce requests from a partition for
   * `partitioner.availability.timeout.ms` time, the partitioner treats that
   * partition as not available. If the value is 0, this logic is disabled.
   * Note: this setting has no effect if a custom partitioner is used or
   * `partitioner.adaptive.partitioning.enable` is set to 'false'.
   */
  static readonly PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG: string = 'partitioner.availability.timeout.ms';
  /**
   * When set to 'true' the producer won't use record keys to choose a
   * partition. If 'false', producer would choose a partition based on a hash of
   * the key when a key is present. Note: this setting has no effect if a custom
   * partitioner is used.
   */
  static readonly PARTITIONER_IGNORE_KEYS_CONFIG: string = 'partitioner.ignore.keys';
  /**
   * Controls whether the default partitioner is rack-aware. This has no effect
   * when a custom partitioner is used.
   */
  static readonly PARTITIONER_RACK_AWARE_CONFIG: string = 'partitioner.rack.aware';
  /**
   * The number of acknowledgments the producer requires the leader to have
   * received before considering a request complete. This controls the
   * durability of records that are sent. The following settings are allowed:
   *
   * - `acks=0` If set to zero then the producer will not wait for any
   *   acknowledgment from the server at all. The record will be immediately
   *   added to the socket buffer and considered sent. No guarantee can be made
   *   that the server has received the record in this case, and the `retries`
   *   configuration will not take effect (as the client won't generally know of
   *   any failures). The offset given back for each record will always be set
   *   to `-1`.
   * - `acks=1` This will mean the leader will write the record to its local log
   *   but will respond without awaiting full acknowledgement from all
   *   followers. In this case should the leader fail immediately after
   *   acknowledging the record but before the followers have replicated it then
   *   the record will be lost.
   * - `acks=all` This means the leader will wait for the full set of in-sync
   *   replicas to acknowledge the record. This guarantees that the record will
   *   not be lost as long as at least one in-sync replica remains alive. This
   *   is the strongest available guarantee. This is equivalent to the acks=-1
   *   setting.
   *
   * Note that enabling idempotence requires this config value to be 'all'. If
   * conflicting configurations are set and idempotence is not explicitly
   * enabled, idempotence is disabled.
   */
  static readonly ACKS_CONFIG: string = 'acks';
  /**
   * The producer groups together any records that arrive in between request
   * transmissions into a single batched request. Normally this occurs only
   * under load when records arrive faster than they can be sent out. However in
   * some circumstances the client may want to reduce the number of requests
   * even under moderate load. This setting accomplishes this by adding a small
   * amount of artificial delay&mdash;that is, rather than immediately sending
   * out a record, the producer will wait for up to the given delay to allow
   * other records to be sent so that the sends can be batched together. This
   * can be thought of as analogous to Nagle's algorithm in TCP. This setting
   * gives the upper bound on the delay for batching: once we get `batch.size`
   * worth of records for a partition it will be sent immediately regardless of
   * this setting, however if we have fewer than this many bytes accumulated for
   * this partition we will 'linger' for the specified time waiting for more
   * records to show up. This setting defaults to 5 (i.e. 5ms delay). Increasing
   * `linger.ms=50`, for example, would have the effect of reducing the number
   * of requests sent but would add up to 50ms of latency to records sent in the
   * absence of load.The default changed from 0 to 5 in Apache Kafka 4.0 as the
   * efficiency gains from larger batches typically result in similar or lower
   * producer latency despite the increased linger.
   */
  static readonly LINGER_MS_CONFIG: string = 'linger.ms';
  /**
   * The configuration controls the maximum amount of time the client will wait
   * for the response of a request. If the response is not received before the
   * timeout elapses the client will resend the request if necessary or fail the
   * request if retries are exhausted. This should be larger than
   * `replica.lag.time.max.ms` (a broker configuration) to reduce the
   * possibility of message duplication due to unnecessary producer retries.
   */
  static readonly REQUEST_TIMEOUT_MS_CONFIG: string = 'request.timeout.ms';
  /**
   * An upper bound on the time to report success or failure after a call to
   * `send()` returns. This limits the total time that a record will be delayed
   * prior to sending, the time to await acknowledgement from the broker (if
   * expected), and the time allowed for retriable send failures. The producer
   * may report failure to send a record earlier than this config if either an
   * unrecoverable error is encountered, the retries have been exhausted, or the
   * record is added to a batch which reached an earlier delivery expiration
   * deadline. The value of this config should be greater than or equal to the
   * sum of `request.timeout.ms` and `linger.ms`.
   */
  static readonly DELIVERY_TIMEOUT_MS_CONFIG: string = 'delivery.timeout.ms';
  /**
   * An id string to pass to the server when making requests. The purpose of
   * this is to be able to track the source of requests beyond just ip/port by
   * allowing a logical application name to be included in server-side request
   * logging.
   */
  static readonly CLIENT_ID_CONFIG: string = 'client.id';
  /**
   * A rack identifier for this client. This can be any string value which
   * indicates where this client is physically located. It corresponds with the
   * broker config 'broker.rack'
   */
  static readonly CLIENT_RACK_CONFIG: string = 'client.rack';
  /**
   * Java's `ProducerConfig.DEFAULT_CLIENT_RACK`.
   */
  static readonly DEFAULT_CLIENT_RACK: string = '';
  /**
   * The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If
   * the value is -1, the OS default will be used.
   */
  static readonly SEND_BUFFER_CONFIG: string = 'send.buffer.bytes';
  /**
   * The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If
   * the value is -1, the OS default will be used.
   */
  static readonly RECEIVE_BUFFER_CONFIG: string = 'receive.buffer.bytes';
  /**
   * The maximum size of a request in bytes. This setting will limit the number
   * of record batches the producer will send in a single request to avoid
   * sending huge requests. This is also effectively a cap on the maximum
   * uncompressed record batch size. Note that the server has its own cap on the
   * record batch size (after compression if compression is enabled) which may
   * be different from this.
   */
  static readonly MAX_REQUEST_SIZE_CONFIG: string = 'max.request.size';
  /**
   * The base amount of time to wait before attempting to reconnect to a given
   * host. This avoids repeatedly connecting to a host in a tight loop. This
   * backoff applies to all connection attempts by the client to a broker. This
   * value is the initial backoff value and will increase exponentially for each
   * consecutive connection failure, up to the `reconnect.backoff.max.ms` value.
   */
  static readonly RECONNECT_BACKOFF_MS_CONFIG: string = 'reconnect.backoff.ms';
  /**
   * The maximum amount of time in milliseconds to wait when reconnecting to a
   * broker that has repeatedly failed to connect. If provided, the backoff per
   * host will increase exponentially for each consecutive connection failure,
   * up to this maximum. After calculating the backoff increase, 20% random
   * jitter is added to avoid connection storms.
   */
  static readonly RECONNECT_BACKOFF_MAX_MS_CONFIG: string = 'reconnect.backoff.max.ms';
  /**
   * The configuration controls how long the `KafkaProducer`'s `send()`,
   * `partitionsFor()`, `initTransactions()`, `sendOffsetsToTransaction()`,
   * `commitTransaction()` and `abortTransaction()` methods will block. For
   * `send()` this timeout bounds the total time waiting for both metadata fetch
   * and buffer allocation (blocking in the user-supplied serializers or
   * partitioner is not counted against this timeout). For `partitionsFor()`
   * this timeout bounds the time spent waiting for metadata if it is
   * unavailable. The transaction-related methods always block, but may timeout
   * if the transaction coordinator could not be discovered or did not respond
   * within the timeout.
   */
  static readonly MAX_BLOCK_MS_CONFIG: string = 'max.block.ms';
  /**
   * The total bytes of memory the producer can use to buffer records waiting to
   * be sent to the server. If records are sent faster than they can be
   * delivered to the server the producer will block for `max.block.ms` after
   * which it will fail with an exception.
   *
   * This setting should correspond roughly to the total memory the producer
   * will use, but is not a hard bound since not all memory the producer uses is
   * used for buffering. Some additional memory will be used for compression (if
   * compression is enabled) as well as for maintaining in-flight requests.
   */
  static readonly BUFFER_MEMORY_CONFIG: string = 'buffer.memory';
  /**
   * Controls how the producer allocates memory from `buffer.memory` for record
   * batches. The following values are supported:
   *
   * - `full`: reserves a full `batch.size` up front when a batch is created,
   *   regardless of how much data it ends up holding. Pool memory therefore
   *   scales with the number of active partitions.
   * - `incremental`: allocates memory on demand as records are appended,
   *   growing a batch up to `batch.size`. Pool memory therefore scales with the
   *   data actually buffered rather than the number of active partitions,
   *   allowing larger `batch.size` values (e.g. for high-latency clusters)
   *   without reserving `batch.size` for every active partition.
   */
  static readonly BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG: string = 'buffer.memory.allocation.strategy';
  /**
   * Java's `ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_FULL`.
   */
  static readonly BUFFER_MEMORY_ALLOCATION_STRATEGY_FULL: string = 'full';
  /**
   * Java's `ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL`.
   */
  static readonly BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL: string = 'incremental';
  /**
   * The amount of time to wait before attempting to retry a failed request to a
   * given topic partition. This avoids repeatedly sending requests in a tight
   * loop under some failure scenarios. This value is the initial backoff value
   * and will increase exponentially for each failed request, up to the
   * `retry.backoff.max.ms` value.
   */
  static readonly RETRY_BACKOFF_MS_CONFIG: string = 'retry.backoff.ms';
  /**
   * The maximum amount of time in milliseconds to wait when retrying a request
   * to the broker that has repeatedly failed. If provided, the backoff per
   * client will increase exponentially for each failed request, up to this
   * maximum. To prevent all clients from being synchronized upon retry, a
   * randomized jitter with a factor of 0.2 will be applied to the backoff,
   * resulting in the backoff falling within a range between 20% below and 20%
   * above the computed value. If `retry.backoff.ms` is set to be higher than
   * `retry.backoff.max.ms`, then `retry.backoff.max.ms` will be used as a
   * constant backoff from the beginning without any exponential increase
   */
  static readonly RETRY_BACKOFF_MAX_MS_CONFIG: string = 'retry.backoff.max.ms';
  /**
   * Selects the client's bootstrap DNS resolution mode. When set to `0` (the
   * default), DNS is resolved synchronously during client construction; any
   * failure surfaces as `ConfigError` and no client instance is created. When
   * set to a positive value, DNS is resolved asynchronously and this is the
   * maximum amount of time the client will spend retrying resolution before
   * failing with an unrecoverable `BootstrapResolutionError` from subsequent
   * API calls (the client must then be closed and re-created after fixing the
   * underlying DNS or `bootstrap.servers` configuration issue). Setting this
   * config to a positive value enables an evolving feature whose compatibility
   * may be broken in a minor release.
   */
  static readonly BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: string = 'bootstrap.resolve.timeout.ms';
  /**
   * Whether to enable pushing of client metrics to the cluster, if the cluster
   * has a client metrics subscription which matches this client.
   */
  static readonly ENABLE_METRICS_PUSH_CONFIG: string = 'enable.metrics.push';
  /**
   * The documentation of `ProducerConfig.ENABLE_METRICS_PUSH_CONFIG`.
   */
  static readonly ENABLE_METRICS_PUSH_DOC: string = 'Whether to enable pushing of client metrics to the cluster, if the cluster has a client metrics subscription which matches this client.';
  /**
   * The compression type for all data generated by the producer. The default is
   * none (i.e. no compression). Valid values are `none`, `gzip`, `snappy`,
   * `lz4`, or `zstd`. Compression is of full batches of data, so the efficacy
   * of batching will also impact the compression ratio (more batching means
   * better compression).
   */
  static readonly COMPRESSION_TYPE_CONFIG: string = 'compression.type';
  /**
   * The compression level to use if compression.type is set to `gzip`.
   */
  static readonly COMPRESSION_GZIP_LEVEL_CONFIG: string = 'compression.gzip.level';
  /**
   * The compression level to use if compression.type is set to `lz4`.
   */
  static readonly COMPRESSION_LZ4_LEVEL_CONFIG: string = 'compression.lz4.level';
  /**
   * The compression level to use if compression.type is set to `zstd`.
   */
  static readonly COMPRESSION_ZSTD_LEVEL_CONFIG: string = 'compression.zstd.level';
  /**
   * The window of time a metrics sample is computed over.
   */
  static readonly METRICS_SAMPLE_WINDOW_MS_CONFIG: string = 'metrics.sample.window.ms';
  /**
   * The number of samples maintained to compute metrics.
   */
  static readonly METRICS_NUM_SAMPLES_CONFIG: string = 'metrics.num.samples';
  /**
   * The highest recording level for metrics. It has three levels for recording
   * metrics - info, debug, and trace. INFO level records only essential metrics
   * necessary for monitoring system performance and health. It collects vital
   * data without gathering too much detail, making it suitable for production
   * environments where minimal overhead is desired. DEBUG level records most
   * metrics, providing more detailed information about the system's operation.
   * It's useful for development and testing environments where you need deeper
   * insights to debug and fine-tune the application. TRACE level records all
   * possible metrics, capturing every detail about the system's performance and
   * operation. It's best for controlled environments where in-depth analysis is
   * required, though it can introduce significant overhead.
   */
  static readonly METRICS_RECORDING_LEVEL_CONFIG: string = 'metrics.recording.level';
  /**
   * A list of classes to use as metrics reporters. Implementing the
   * `org.apache.kafka.common.metrics.MetricsReporter` interface allows plugging
   * in classes that will be notified of new metric creation. When custom
   * reporters are set and `org.apache.kafka.common.metrics.JmxReporter` is
   * needed, it has to be explicitly added to the list.
   */
  static readonly METRIC_REPORTER_CLASSES_CONFIG: string = 'metric.reporters';
  /**
   * The maximum number of unacknowledged requests the client will send on a
   * single connection before blocking. Note that if this configuration is set
   * to be greater than 1 and `enable.idempotence` is set to false, there is a
   * risk of message reordering after a failed send due to retries (i.e., if
   * retries are enabled); if retries are disabled or if `enable.idempotence` is
   * set to true, ordering will be preserved. Additionally, enabling idempotence
   * requires the value of this configuration to be less than or equal to 5,
   * because broker only retains at most 5 batches for each producer. If the
   * value is more than 5, previous batches may be removed on broker side.
   */
  static readonly MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: string = 'max.in.flight.requests.per.connection';
  /**
   * Number of times to retry a request that fails with a transient error.
   * Setting a value greater than zero will cause the client to resend any
   * record whose send fails with a potentially transient error. Requests will
   * be retried this many times until they succeed, fail with a non-transient
   * error, or the `delivery.timeout.ms` expires. Note that this automatic retry
   * will simply resend the same record upon receiving the error. Setting a
   * value of zero will disable this automatic retry behaviour, so that the
   * transient errors will be propagated to the application to be handled. Users
   * should generally prefer to leave this config unset and instead use
   * `delivery.timeout.ms` to control retry behavior.
   *
   * Enabling idempotence requires this config value to be greater than 0. If
   * conflicting configurations are set and idempotence is not explicitly
   * enabled, idempotence is disabled.
   *
   * Allowing retries while setting `enable.idempotence` to `false` and
   * `max.in.flight.requests.per.connection` to greater than 1 will potentially
   * change the ordering of records because if two batches are sent to a single
   * partition, and the first fails and is retried but the second succeeds, then
   * the records in the second batch may appear first.
   */
  static readonly RETRIES_CONFIG: string = 'retries';
  /**
   * Serializer class for key that implements the
   * `org.apache.kafka.common.serialization.Serializer` interface.
   */
  static readonly KEY_SERIALIZER_CLASS_CONFIG: string = 'key.serializer';
  /**
   * The documentation of `ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG`.
   */
  static readonly KEY_SERIALIZER_CLASS_DOC: string = 'Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.';
  /**
   * Serializer class for value that implements the
   * `org.apache.kafka.common.serialization.Serializer` interface.
   */
  static readonly VALUE_SERIALIZER_CLASS_CONFIG: string = 'value.serializer';
  /**
   * The documentation of `ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG`.
   */
  static readonly VALUE_SERIALIZER_CLASS_DOC: string = 'Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.';
  /**
   * The amount of time the client will wait for the socket connection to be
   * established. If the connection is not built before the timeout elapses,
   * clients will close the socket channel. This value is the initial backoff
   * value and will increase exponentially for each consecutive connection
   * failure, up to the `socket.connection.setup.timeout.max.ms` value.
   */
  static readonly SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: string = 'socket.connection.setup.timeout.ms';
  /**
   * The maximum amount of time the client will wait for the socket connection
   * to be established. The connection setup timeout will increase exponentially
   * for each consecutive connection failure up to this maximum. To avoid
   * connection storms, a randomization factor of 0.2 will be applied to the
   * timeout resulting in a random range between 20% below and 20% above the
   * computed value.
   */
  static readonly SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: string = 'socket.connection.setup.timeout.max.ms';
  /**
   * Close idle connections after the number of milliseconds specified by this
   * config.
   */
  static readonly CONNECTIONS_MAX_IDLE_MS_CONFIG: string = 'connections.max.idle.ms';
  /**
   * Determines which partition to send a record to when records are produced.
   * Available options are:
   *
   * - If not set, the default partitioning logic is used. This strategy send
   *   records to a partition until at least batch.size bytes is produced to the
   *   partition. It works with the strategy:
   *   1. If no partition is specified but a key is present, choose a partition
   *      based on a hash of the key.
   *   2. If no partition or key is present, choose the sticky partition that
   *      changes when at least `batch.size` bytes are produced to the
   *      partition.
   *   3. If `client.rack` is specified and `partitioner.rack.aware=true`, the
   *      sticky partition is chosen from partitions with the leader broker in
   *      the same rack, if at least one is available. If none are available, it
   *      falls back on selecting from all available partitions.
   * - `org.apache.kafka.clients.producer.RoundRobinPartitioner`: A partitioning
   *   strategy where each record in a series of consecutive records is sent to
   *   a different partition, regardless of whether the 'key' is provided or
   *   not, until partitions run out and the process starts over again. Note:
   *   There's a known issue that will cause uneven distribution when a new
   *   batch is created. See KAFKA-9965 for more detail.
   *
   * Implementing the `org.apache.kafka.clients.producer.Partitioner` interface
   * allows you to plug in a custom partitioner.
   */
  static readonly PARTITIONER_CLASS_CONFIG: string = 'partitioner.class';
  /**
   * A list of classes to use as interceptors. Implementing the
   * `org.apache.kafka.clients.producer.ProducerInterceptor` interface allows
   * you to intercept (and possibly mutate) the records received by the producer
   * before they are published to the Kafka cluster. By default, there are no
   * interceptors.
   */
  static readonly INTERCEPTOR_CLASSES_CONFIG: string = 'interceptor.classes';
  /**
   * The documentation of `ProducerConfig.INTERCEPTOR_CLASSES_CONFIG`.
   */
  static readonly INTERCEPTOR_CLASSES_DOC: string = 'A list of classes to use as interceptors. Implementing the <code>org.apache.kafka.clients.producer.ProducerInterceptor</code> interface allows you to intercept (and possibly mutate) the records received by the producer before they are published to the Kafka cluster. By default, there are no interceptors.';
  /**
   * When set to 'true', the producer will ensure that exactly one copy of each
   * message is written in the stream. If 'false', producer retries due to
   * broker failures, etc., may write duplicates of the retried message in the
   * stream. Note that enabling idempotence requires
   * `max.in.flight.requests.per.connection` to be less than or equal to 5 (with
   * message ordering preserved for any allowable value), `retries` to be
   * greater than 0, and `acks` must be 'all'.
   *
   * Idempotence is enabled by default if no conflicting configurations are set.
   * If conflicting configurations are set and idempotence is not explicitly
   * enabled, idempotence is disabled. If idempotence is explicitly enabled and
   * conflicting configurations are set, a `ConfigError` is thrown.
   */
  static readonly ENABLE_IDEMPOTENCE_CONFIG: string = 'enable.idempotence';
  /**
   * The documentation of `ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG`.
   */
  static readonly ENABLE_IDEMPOTENCE_DOC: string = 'When set to \'true\', the producer will ensure that exactly one copy of each message is written in the stream. If \'false\', producer retries due to broker failures, etc., may write duplicates of the retried message in the stream. Note that enabling idempotence requires <code>max.in.flight.requests.per.connection</code> to be less than or equal to 5 (with message ordering preserved for any allowable value), <code>retries</code> to be greater than 0, and <code>acks</code> must be \'all\'. <p>Idempotence is enabled by default if no conflicting configurations are set. If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled. If idempotence is explicitly enabled and conflicting configurations are set, a <code>ConfigException</code> is thrown.';
  /**
   * The maximum amount of time in milliseconds that a transaction will remain
   * open before the coordinator proactively aborts it. The start of the
   * transaction is set at the time that the first partition is added to it. If
   * this value is larger than the `transaction.max.timeout.ms` setting in the
   * broker, the request will fail with a `InvalidTxnTimeoutError` error.
   */
  static readonly TRANSACTION_TIMEOUT_CONFIG: string = 'transaction.timeout.ms';
  /**
   * The documentation of `ProducerConfig.TRANSACTION_TIMEOUT_CONFIG`.
   */
  static readonly TRANSACTION_TIMEOUT_DOC: string = 'The maximum amount of time in milliseconds that a transaction will remain open before the coordinator proactively aborts it. The start of the transaction is set at the time that the first partition is added to it. If this value is larger than the <code>transaction.max.timeout.ms</code> setting in the broker, the request will fail with a <code>InvalidTxnTimeoutException</code> error.';
  /**
   * The TransactionalId to use for transactional delivery. This enables
   * reliability semantics which span multiple producer sessions since it allows
   * the client to guarantee that transactions using the same TransactionalId
   * have been completed prior to starting any new transactions. If no
   * TransactionalId is provided, then the producer is limited to idempotent
   * delivery. If a TransactionalId is configured, `enable.idempotence` is
   * implied. By default the TransactionId is not configured, which means
   * transactions cannot be used. Note that, by default, transactions require a
   * cluster of at least three brokers which is the recommended setting for
   * production; for development you can change this, by adjusting broker
   * setting `transaction.state.log.replication.factor`.
   */
  static readonly TRANSACTIONAL_ID_CONFIG: string = 'transactional.id';
  /**
   * The documentation of `ProducerConfig.TRANSACTIONAL_ID_CONFIG`.
   */
  static readonly TRANSACTIONAL_ID_DOC: string = 'The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. If a TransactionalId is configured, <code>enable.idempotence</code> is implied. By default the TransactionId is not configured, which means transactions cannot be used. Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production; for development you can change this, by adjusting broker setting <code>transaction.state.log.replication.factor</code>.';
  /**
   * If set to true, then the broker is informed that the client is
   * participating in two phase commit protocol and transactions that this
   * client starts never expire.
   */
  static readonly TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG: string = 'transaction.two.phase.commit.enable';
  /**
   * A list of configurable creator classes each returning a provider
   * implementing security algorithms. These classes should implement the
   * `org.apache.kafka.common.security.auth.SecurityProviderCreator` interface.
   */
  static readonly SECURITY_PROVIDERS_CONFIG: string = 'security.providers';
  /**
   * Java's `AbstractConfig.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY`.
   */
  static readonly AUTOMATIC_CONFIG_PROVIDERS_PROPERTY: string = 'org.apache.kafka.automatic.config.providers';
  /**
   * Comma-separated alias names for classes implementing the `ConfigProvider`
   * interface. This enables loading configuration data (such as passwords, API
   * keys, and other credentials) from external sources. For example, see
   * [Configuration
   * Providers](https://kafka.apache.org/documentation/#config_providers).
   */
  static readonly CONFIG_PROVIDERS_CONFIG: string = 'config.providers';
  /**
   * The documentation of `AbstractConfig.CONFIG_PROVIDERS_CONFIG`.
   */
  static readonly CONFIG_PROVIDERS_DOC: string = 'Comma-separated alias names for classes implementing the <code>ConfigProvider</code> interface. This enables loading configuration data (such as passwords, API keys, and other credentials) from external sources. For example, see <a href="https://kafka.apache.org/documentation/#config_providers">Configuration Providers</a>.';

  /**
   * Not instantiable: throws `TypeError`.
   *
   * @param args - ignored
   * @throws `TypeError` always
   */
  constructor(...args: unknown[]) {
    void args;
    throw new TypeError('ProducerConfig is not instantiable: its ConfigDef machinery is not ' +
      'translated; a configuration is a plain object of its keys');
  }
}
