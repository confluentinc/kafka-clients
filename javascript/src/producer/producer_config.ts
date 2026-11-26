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
 * Configuration for the Kafka Producer. Documentation for these configurations
 * can be found in the [Kafka
 * documentation](http://kafka.apache.org/documentation.html#producerconfigs)
 *
 * @remarks *(deviation)* Constants only: `new` throws `TypeError`.
 */
export class ProducerConfig {
  /**
   * `bootstrap.servers`
   */
  static readonly BOOTSTRAP_SERVERS_CONFIG: string = 'bootstrap.servers';
  /**
   * `client.dns.lookup`
   */
  static readonly CLIENT_DNS_LOOKUP_CONFIG: string = 'client.dns.lookup';
  /**
   * `metadata.max.age.ms`
   */
  static readonly METADATA_MAX_AGE_CONFIG: string = 'metadata.max.age.ms';
  /**
   * `metadata.max.idle.ms`
   */
  static readonly METADATA_MAX_IDLE_CONFIG: string = 'metadata.max.idle.ms';
  /**
   * `batch.size`
   */
  static readonly BATCH_SIZE_CONFIG: string = 'batch.size';
  /**
   * `partitioner.adaptive.partitioning.enable`
   */
  static readonly PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG: string = 'partitioner.adaptive.partitioning.enable';
  /**
   * `partitioner.availability.timeout.ms`
   */
  static readonly PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG: string = 'partitioner.availability.timeout.ms';
  /**
   * `partitioner.ignore.keys`
   */
  static readonly PARTITIONER_IGNORE_KEYS_CONFIG: string = 'partitioner.ignore.keys';
  /**
   * `partitioner.rack.aware`
   */
  static readonly PARTITIONER_RACK_AWARE_CONFIG: string = 'partitioner.rack.aware';
  /**
   * `acks`
   */
  static readonly ACKS_CONFIG: string = 'acks';
  /**
   * `linger.ms`
   */
  static readonly LINGER_MS_CONFIG: string = 'linger.ms';
  /**
   * `request.timeout.ms`
   */
  static readonly REQUEST_TIMEOUT_MS_CONFIG: string = 'request.timeout.ms';
  /**
   * `delivery.timeout.ms`
   */
  static readonly DELIVERY_TIMEOUT_MS_CONFIG: string = 'delivery.timeout.ms';
  /**
   * `client.id`
   */
  static readonly CLIENT_ID_CONFIG: string = 'client.id';
  /**
   * `client.rack`
   */
  static readonly CLIENT_RACK_CONFIG: string = 'client.rack';
  /**
   * Java's `ProducerConfig.DEFAULT_CLIENT_RACK`.
   */
  static readonly DEFAULT_CLIENT_RACK: string = '';
  /**
   * `send.buffer.bytes`
   */
  static readonly SEND_BUFFER_CONFIG: string = 'send.buffer.bytes';
  /**
   * `receive.buffer.bytes`
   */
  static readonly RECEIVE_BUFFER_CONFIG: string = 'receive.buffer.bytes';
  /**
   * `max.request.size`
   */
  static readonly MAX_REQUEST_SIZE_CONFIG: string = 'max.request.size';
  /**
   * `reconnect.backoff.ms`
   */
  static readonly RECONNECT_BACKOFF_MS_CONFIG: string = 'reconnect.backoff.ms';
  /**
   * `reconnect.backoff.max.ms`
   */
  static readonly RECONNECT_BACKOFF_MAX_MS_CONFIG: string = 'reconnect.backoff.max.ms';
  /**
   * `max.block.ms`
   */
  static readonly MAX_BLOCK_MS_CONFIG: string = 'max.block.ms';
  /**
   * `buffer.memory`
   */
  static readonly BUFFER_MEMORY_CONFIG: string = 'buffer.memory';
  /**
   * `buffer.memory.allocation.strategy`
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
   * `retry.backoff.ms`
   */
  static readonly RETRY_BACKOFF_MS_CONFIG: string = 'retry.backoff.ms';
  /**
   * `retry.backoff.max.ms`
   */
  static readonly RETRY_BACKOFF_MAX_MS_CONFIG: string = 'retry.backoff.max.ms';
  /**
   * `bootstrap.resolve.timeout.ms`
   */
  static readonly BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: string = 'bootstrap.resolve.timeout.ms';
  /**
   * `enable.metrics.push`
   */
  static readonly ENABLE_METRICS_PUSH_CONFIG: string = 'enable.metrics.push';
  /**
   * The documentation of `ProducerConfig.ENABLE_METRICS_PUSH_CONFIG`.
   */
  static readonly ENABLE_METRICS_PUSH_DOC: string = 'Whether to enable pushing of client metrics to the cluster, if the cluster has a client metrics subscription which matches this client.';
  /**
   * `compression.type`
   */
  static readonly COMPRESSION_TYPE_CONFIG: string = 'compression.type';
  /**
   * `compression.gzip.level`
   */
  static readonly COMPRESSION_GZIP_LEVEL_CONFIG: string = 'compression.gzip.level';
  /**
   * `compression.lz4.level`
   */
  static readonly COMPRESSION_LZ4_LEVEL_CONFIG: string = 'compression.lz4.level';
  /**
   * `compression.zstd.level`
   */
  static readonly COMPRESSION_ZSTD_LEVEL_CONFIG: string = 'compression.zstd.level';
  /**
   * `metrics.sample.window.ms`
   */
  static readonly METRICS_SAMPLE_WINDOW_MS_CONFIG: string = 'metrics.sample.window.ms';
  /**
   * `metrics.num.samples`
   */
  static readonly METRICS_NUM_SAMPLES_CONFIG: string = 'metrics.num.samples';
  /**
   * `metrics.recording.level`
   */
  static readonly METRICS_RECORDING_LEVEL_CONFIG: string = 'metrics.recording.level';
  /**
   * `metric.reporters`
   */
  static readonly METRIC_REPORTER_CLASSES_CONFIG: string = 'metric.reporters';
  /**
   * `max.in.flight.requests.per.connection`
   */
  static readonly MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION: string = 'max.in.flight.requests.per.connection';
  /**
   * `retries`
   */
  static readonly RETRIES_CONFIG: string = 'retries';
  /**
   * `key.serializer`
   */
  static readonly KEY_SERIALIZER_CLASS_CONFIG: string = 'key.serializer';
  /**
   * The documentation of `ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG`.
   */
  static readonly KEY_SERIALIZER_CLASS_DOC: string = 'Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.';
  /**
   * `value.serializer`
   */
  static readonly VALUE_SERIALIZER_CLASS_CONFIG: string = 'value.serializer';
  /**
   * The documentation of `ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG`.
   */
  static readonly VALUE_SERIALIZER_CLASS_DOC: string = 'Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.';
  /**
   * `socket.connection.setup.timeout.ms`
   */
  static readonly SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: string = 'socket.connection.setup.timeout.ms';
  /**
   * `socket.connection.setup.timeout.max.ms`
   */
  static readonly SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: string = 'socket.connection.setup.timeout.max.ms';
  /**
   * `connections.max.idle.ms`
   */
  static readonly CONNECTIONS_MAX_IDLE_MS_CONFIG: string = 'connections.max.idle.ms';
  /**
   * `partitioner.class`
   */
  static readonly PARTITIONER_CLASS_CONFIG: string = 'partitioner.class';
  /**
   * `interceptor.classes`
   */
  static readonly INTERCEPTOR_CLASSES_CONFIG: string = 'interceptor.classes';
  /**
   * The documentation of `ProducerConfig.INTERCEPTOR_CLASSES_CONFIG`.
   */
  static readonly INTERCEPTOR_CLASSES_DOC: string = 'A list of classes to use as interceptors. Implementing the <code>org.apache.kafka.clients.producer.ProducerInterceptor</code> interface allows you to intercept (and possibly mutate) the records received by the producer before they are published to the Kafka cluster. By default, there are no interceptors.';
  /**
   * `enable.idempotence`
   */
  static readonly ENABLE_IDEMPOTENCE_CONFIG: string = 'enable.idempotence';
  /**
   * The documentation of `ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG`.
   */
  static readonly ENABLE_IDEMPOTENCE_DOC: string = 'When set to \'true\', the producer will ensure that exactly one copy of each message is written in the stream. If \'false\', producer retries due to broker failures, etc., may write duplicates of the retried message in the stream. Note that enabling idempotence requires <code>max.in.flight.requests.per.connection</code> to be less than or equal to 5 (with message ordering preserved for any allowable value), <code>retries</code> to be greater than 0, and <code>acks</code> must be \'all\'. <p>Idempotence is enabled by default if no conflicting configurations are set. If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled. If idempotence is explicitly enabled and conflicting configurations are set, a <code>ConfigException</code> is thrown.';
  /**
   * ` transaction.timeout.ms `
   */
  static readonly TRANSACTION_TIMEOUT_CONFIG: string = 'transaction.timeout.ms';
  /**
   * The documentation of `ProducerConfig.TRANSACTION_TIMEOUT_CONFIG`.
   */
  static readonly TRANSACTION_TIMEOUT_DOC: string = 'The maximum amount of time in milliseconds that a transaction will remain open before the coordinator proactively aborts it. The start of the transaction is set at the time that the first partition is added to it. If this value is larger than the <code>transaction.max.timeout.ms</code> setting in the broker, the request will fail with a <code>InvalidTxnTimeoutException</code> error.';
  /**
   * ` transactional.id `
   */
  static readonly TRANSACTIONAL_ID_CONFIG: string = 'transactional.id';
  /**
   * The documentation of `ProducerConfig.TRANSACTIONAL_ID_CONFIG`.
   */
  static readonly TRANSACTIONAL_ID_DOC: string = 'The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. If a TransactionalId is configured, <code>enable.idempotence</code> is implied. By default the TransactionId is not configured, which means transactions cannot be used. Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production; for development you can change this, by adjusting broker setting <code>transaction.state.log.replication.factor</code>.';
  /**
   * ` transaction.two.phase.commit.enable `
   */
  static readonly TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG: string = 'transaction.two.phase.commit.enable';
  /**
   * `security.providers`
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
   * Not instantiable: throws `TypeError`. Private, so TypeScript rejects `new` too.
   *
   * @param args - ignored
   * @throws `TypeError` always
   */
  private constructor(...args: unknown[]) {
    void args;
    throw new TypeError('ProducerConfig is not instantiable: its ConfigDef machinery is not ' +
      'translated; a configuration is a plain object of its keys');
  }

  // Java's constants are static finals: the class is frozen once its static
  // fields are set, so none can be reassigned.
  static {
    Object.freeze(this);
  }
}
