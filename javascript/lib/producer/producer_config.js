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

'use strict';

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
class ProducerConfig {
  /** @readonly @type {string} */
  static BOOTSTRAP_SERVERS_CONFIG = 'bootstrap.servers';
  /** @readonly @type {string} */
  static CLIENT_DNS_LOOKUP_CONFIG = 'client.dns.lookup';
  /** @readonly @type {string} */
  static METADATA_MAX_AGE_CONFIG = 'metadata.max.age.ms';
  /** @readonly @type {string} */
  static METADATA_MAX_IDLE_CONFIG = 'metadata.max.idle.ms';
  /** @readonly @type {string} */
  static BATCH_SIZE_CONFIG = 'batch.size';
  /** @readonly @type {string} */
  static PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG = 'partitioner.adaptive.partitioning.enable';
  /** @readonly @type {string} */
  static PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG = 'partitioner.availability.timeout.ms';
  /** @readonly @type {string} */
  static PARTITIONER_IGNORE_KEYS_CONFIG = 'partitioner.ignore.keys';
  /** @readonly @type {string} */
  static PARTITIONER_RACK_AWARE_CONFIG = 'partitioner.rack.aware';
  /** @readonly @type {string} */
  static ACKS_CONFIG = 'acks';
  /** @readonly @type {string} */
  static LINGER_MS_CONFIG = 'linger.ms';
  /** @readonly @type {string} */
  static REQUEST_TIMEOUT_MS_CONFIG = 'request.timeout.ms';
  /** @readonly @type {string} */
  static DELIVERY_TIMEOUT_MS_CONFIG = 'delivery.timeout.ms';
  /** @readonly @type {string} */
  static CLIENT_ID_CONFIG = 'client.id';
  /** @readonly @type {string} */
  static CLIENT_RACK_CONFIG = 'client.rack';
  /** @readonly @type {string} */
  static DEFAULT_CLIENT_RACK = '';
  /** @readonly @type {string} */
  static SEND_BUFFER_CONFIG = 'send.buffer.bytes';
  /** @readonly @type {string} */
  static RECEIVE_BUFFER_CONFIG = 'receive.buffer.bytes';
  /** @readonly @type {string} */
  static MAX_REQUEST_SIZE_CONFIG = 'max.request.size';
  /** @readonly @type {string} */
  static RECONNECT_BACKOFF_MS_CONFIG = 'reconnect.backoff.ms';
  /** @readonly @type {string} */
  static RECONNECT_BACKOFF_MAX_MS_CONFIG = 'reconnect.backoff.max.ms';
  /** @readonly @type {string} */
  static MAX_BLOCK_MS_CONFIG = 'max.block.ms';
  /** @readonly @type {string} */
  static BUFFER_MEMORY_CONFIG = 'buffer.memory';
  /** @readonly @type {string} */
  static BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG = 'buffer.memory.allocation.strategy';
  /** @readonly @type {string} */
  static BUFFER_MEMORY_ALLOCATION_STRATEGY_FULL = 'full';
  /** @readonly @type {string} */
  static BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL = 'incremental';
  /** @readonly @type {string} */
  static RETRY_BACKOFF_MS_CONFIG = 'retry.backoff.ms';
  /** @readonly @type {string} */
  static RETRY_BACKOFF_MAX_MS_CONFIG = 'retry.backoff.max.ms';
  /** @readonly @type {string} */
  static BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG = 'bootstrap.resolve.timeout.ms';
  /** @readonly @type {string} */
  static ENABLE_METRICS_PUSH_CONFIG = 'enable.metrics.push';
  /** @readonly @type {string} */
  static ENABLE_METRICS_PUSH_DOC = 'Whether to enable pushing of client metrics to the cluster, if the cluster has a client metrics subscription which matches this client.';
  /** @readonly @type {string} */
  static COMPRESSION_TYPE_CONFIG = 'compression.type';
  /** @readonly @type {string} */
  static COMPRESSION_GZIP_LEVEL_CONFIG = 'compression.gzip.level';
  /** @readonly @type {string} */
  static COMPRESSION_LZ4_LEVEL_CONFIG = 'compression.lz4.level';
  /** @readonly @type {string} */
  static COMPRESSION_ZSTD_LEVEL_CONFIG = 'compression.zstd.level';
  /** @readonly @type {string} */
  static METRICS_SAMPLE_WINDOW_MS_CONFIG = 'metrics.sample.window.ms';
  /** @readonly @type {string} */
  static METRICS_NUM_SAMPLES_CONFIG = 'metrics.num.samples';
  /** @readonly @type {string} */
  static METRICS_RECORDING_LEVEL_CONFIG = 'metrics.recording.level';
  /** @readonly @type {string} */
  static METRIC_REPORTER_CLASSES_CONFIG = 'metric.reporters';
  /** @readonly @type {string} */
  static MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 'max.in.flight.requests.per.connection';
  /** @readonly @type {string} */
  static RETRIES_CONFIG = 'retries';
  /** @readonly @type {string} */
  static KEY_SERIALIZER_CLASS_CONFIG = 'key.serializer';
  /** @readonly @type {string} */
  static KEY_SERIALIZER_CLASS_DOC = 'Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.';
  /** @readonly @type {string} */
  static VALUE_SERIALIZER_CLASS_CONFIG = 'value.serializer';
  /** @readonly @type {string} */
  static VALUE_SERIALIZER_CLASS_DOC = 'Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.';
  /** @readonly @type {string} */
  static SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = 'socket.connection.setup.timeout.ms';
  /** @readonly @type {string} */
  static SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = 'socket.connection.setup.timeout.max.ms';
  /** @readonly @type {string} */
  static CONNECTIONS_MAX_IDLE_MS_CONFIG = 'connections.max.idle.ms';
  /** @readonly @type {string} */
  static PARTITIONER_CLASS_CONFIG = 'partitioner.class';
  /** @readonly @type {string} */
  static INTERCEPTOR_CLASSES_CONFIG = 'interceptor.classes';
  /** @readonly @type {string} */
  static INTERCEPTOR_CLASSES_DOC = 'A list of classes to use as interceptors. Implementing the <code>org.apache.kafka.clients.producer.ProducerInterceptor</code> interface allows you to intercept (and possibly mutate) the records received by the producer before they are published to the Kafka cluster. By default, there are no interceptors.';
  /** @readonly @type {string} */
  static ENABLE_IDEMPOTENCE_CONFIG = 'enable.idempotence';
  /** @readonly @type {string} */
  static ENABLE_IDEMPOTENCE_DOC = 'When set to \'true\', the producer will ensure that exactly one copy of each message is written in the stream. If \'false\', producer retries due to broker failures, etc., may write duplicates of the retried message in the stream. Note that enabling idempotence requires <code>max.in.flight.requests.per.connection</code> to be less than or equal to 5 (with message ordering preserved for any allowable value), <code>retries</code> to be greater than 0, and <code>acks</code> must be \'all\'. <p>Idempotence is enabled by default if no conflicting configurations are set. If conflicting configurations are set and idempotence is not explicitly enabled, idempotence is disabled. If idempotence is explicitly enabled and conflicting configurations are set, a <code>ConfigException</code> is thrown.';
  /** @readonly @type {string} */
  static TRANSACTION_TIMEOUT_CONFIG = 'transaction.timeout.ms';
  /** @readonly @type {string} */
  static TRANSACTION_TIMEOUT_DOC = 'The maximum amount of time in milliseconds that a transaction will remain open before the coordinator proactively aborts it. The start of the transaction is set at the time that the first partition is added to it. If this value is larger than the <code>transaction.max.timeout.ms</code> setting in the broker, the request will fail with a <code>InvalidTxnTimeoutException</code> error.';
  /** @readonly @type {string} */
  static TRANSACTIONAL_ID_CONFIG = 'transactional.id';
  /** @readonly @type {string} */
  static TRANSACTIONAL_ID_DOC = 'The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. If a TransactionalId is configured, <code>enable.idempotence</code> is implied. By default the TransactionId is not configured, which means transactions cannot be used. Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production; for development you can change this, by adjusting broker setting <code>transaction.state.log.replication.factor</code>.';
  /** @readonly @type {string} */
  static TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG = 'transaction.two.phase.commit.enable';
  /** @readonly @type {string} */
  static SECURITY_PROVIDERS_CONFIG = 'security.providers';
  /** @readonly @type {string} */
  static AUTOMATIC_CONFIG_PROVIDERS_PROPERTY = 'org.apache.kafka.automatic.config.providers';
  /** @readonly @type {string} */
  static CONFIG_PROVIDERS_CONFIG = 'config.providers';
  /** @readonly @type {string} */
  static CONFIG_PROVIDERS_DOC = 'Comma-separated alias names for classes implementing the <code>ConfigProvider</code> interface. This enables loading configuration data (such as passwords, API keys, and other credentials) from external sources. For example, see <a href="https://kafka.apache.org/documentation/#config_providers">Configuration Providers</a>.';

  /**
   * Not instantiable: throws `TypeError`.
   *
   * @param {...unknown} args
   */
  constructor(...args) {
    void args;
    throw new TypeError('ProducerConfig is not instantiable: its ConfigDef machinery is not ' +
      'translated; a configuration is a plain object of its keys');
  }
}

module.exports = { ProducerConfig };
