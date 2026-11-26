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
// Java's `org.apache.kafka.clients.admin.AdminClientConfig`;
// `cargo xtask check-generated` checks it is up to date.

/**
 * The AdminClient configuration class, which also contains constants for
 * configuration entry names.
 *
 * @remarks *(deviation)* Constants only: `new` throws `TypeError`.
 */
export class AdminClientConfig {
  /**
   * `bootstrap.servers`
   */
  static readonly BOOTSTRAP_SERVERS_CONFIG: string = 'bootstrap.servers';
  /**
   * `bootstrap.controllers`
   */
  static readonly BOOTSTRAP_CONTROLLERS_CONFIG: string = 'bootstrap.controllers';
  /**
   * The documentation of `AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG`.
   */
  static readonly BOOTSTRAP_CONTROLLERS_DOC: string = 'A list of host/port pairs to use for establishing the initial connection to the KRaft controller quorum. This list should be in the form <code>host1:port1,host2:port2,...</code>.';
  /**
   * `client.dns.lookup`
   */
  static readonly CLIENT_DNS_LOOKUP_CONFIG: string = 'client.dns.lookup';
  /**
   * `reconnect.backoff.ms`
   */
  static readonly RECONNECT_BACKOFF_MS_CONFIG: string = 'reconnect.backoff.ms';
  /**
   * `reconnect.backoff.max.ms`
   */
  static readonly RECONNECT_BACKOFF_MAX_MS_CONFIG: string = 'reconnect.backoff.max.ms';
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
   * The documentation of `AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG`.
   */
  static readonly ENABLE_METRICS_PUSH_DOC: string = 'Whether to enable pushing of client metrics to the cluster, if the cluster has a client metrics subscription which matches this client.';
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
   * `request.timeout.ms`
   */
  static readonly REQUEST_TIMEOUT_MS_CONFIG: string = 'request.timeout.ms';
  /**
   * An id string to pass to the server when making requests. The purpose of
   * this is to be able to track the source of requests beyond just ip/port by
   * allowing a logical application name to be included in server-side request
   * logging.
   */
  static readonly CLIENT_ID_CONFIG: string = 'client.id';
  /**
   * The period of time in milliseconds after which we force a refresh of
   * metadata even if we haven't seen any partition leadership changes to
   * proactively discover any new brokers or partitions.
   */
  static readonly METADATA_MAX_AGE_CONFIG: string = 'metadata.max.age.ms';
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
   * A list of classes to use as metrics reporters. Implementing the
   * `org.apache.kafka.common.metrics.MetricsReporter` interface allows plugging
   * in classes that will be notified of new metric creation. When custom
   * reporters are set and `org.apache.kafka.common.metrics.JmxReporter` is
   * needed, it has to be explicitly added to the list.
   */
  static readonly METRIC_REPORTER_CLASSES_CONFIG: string = 'metric.reporters';
  /**
   * The number of samples maintained to compute metrics.
   */
  static readonly METRICS_NUM_SAMPLES_CONFIG: string = 'metrics.num.samples';
  /**
   * The window of time a metrics sample is computed over.
   */
  static readonly METRICS_SAMPLE_WINDOW_MS_CONFIG: string = 'metrics.sample.window.ms';
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
   * Protocol used to communicate with brokers.
   */
  static readonly SECURITY_PROTOCOL_CONFIG: string = 'security.protocol';
  /**
   * Java's `AdminClientConfig.DEFAULT_SECURITY_PROTOCOL`.
   */
  static readonly DEFAULT_SECURITY_PROTOCOL: string = 'PLAINTEXT';
  /**
   * It is recommended to set the value to either `MAX_VALUE` or zero, and use
   * corresponding timeout parameters to control how long a client should retry
   * a request. Setting a value greater than zero will cause the client to
   * resend any request that fails with a potentially transient error. Setting a
   * value of zero will lead to transient errors not being retried, and they
   * will be propagated to the application to be handled.
   */
  static readonly RETRIES_CONFIG: string = 'retries';
  /**
   * The `default.api.timeout.ms` configuration key.
   */
  static readonly DEFAULT_API_TIMEOUT_MS_CONFIG: string = 'default.api.timeout.ms';
  /**
   * Controls how the client recovers when none of the brokers known to it is
   * available. If set to `none`, the client fails. If set to `rebootstrap`, the
   * client repeats the bootstrap process using `bootstrap.servers`.
   * Rebootstrapping is useful when a client communicates with brokers so
   * infrequently that the set of brokers may change entirely before the client
   * refreshes metadata. Metadata recovery is triggered when all last-known
   * brokers appear unavailable simultaneously. Brokers appear unavailable when
   * disconnected and no current retry attempt is in-progress. Consider
   * increasing `reconnect.backoff.ms` and `reconnect.backoff.max.ms` and
   * decreasing `socket.connection.setup.timeout.ms` and
   * `socket.connection.setup.timeout.max.ms` for the client. Rebootstrap is
   * also triggered if connection cannot be established to any of the brokers
   * for `metadata.recovery.rebootstrap.trigger.ms` milliseconds or if server
   * requests rebootstrap.
   */
  static readonly METADATA_RECOVERY_STRATEGY_CONFIG: string = 'metadata.recovery.strategy';
  /**
   * The documentation of `AdminClientConfig.METADATA_RECOVERY_STRATEGY_CONFIG`.
   */
  static readonly METADATA_RECOVERY_STRATEGY_DOC: string = 'Controls how the client recovers when none of the brokers known to it is available. If set to <code>none</code>, the client fails. If set to <code>rebootstrap</code>, the client repeats the bootstrap process using <code>bootstrap.servers</code>. Rebootstrapping is useful when a client communicates with brokers so infrequently that the set of brokers may change entirely before the client refreshes metadata. Metadata recovery is triggered when all last-known brokers appear unavailable simultaneously. Brokers appear unavailable when disconnected and no current retry attempt is in-progress. Consider increasing <code>reconnect.backoff.ms</code> and <code>reconnect.backoff.max.ms</code> and decreasing <code>socket.connection.setup.timeout.ms</code> and <code>socket.connection.setup.timeout.max.ms</code> for the client. Rebootstrap is also triggered if connection cannot be established to any of the brokers for <code>metadata.recovery.rebootstrap.trigger.ms</code> milliseconds or if server requests rebootstrap.';
  /**
   * Java's `AdminClientConfig.DEFAULT_METADATA_RECOVERY_STRATEGY`.
   */
  static readonly DEFAULT_METADATA_RECOVERY_STRATEGY: string = 'rebootstrap';
  /**
   * If a client configured to rebootstrap using
   * `metadata.recovery.strategy=rebootstrap` is unable to obtain metadata from
   * any of the brokers in the last known metadata for this interval, client
   * repeats the bootstrap process using `bootstrap.servers` configuration.
   */
  static readonly METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG: string = 'metadata.recovery.rebootstrap.trigger.ms';
  /**
   * The documentation of
   * `AdminClientConfig.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG`.
   */
  static readonly METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC: string = 'If a client configured to rebootstrap using <code>metadata.recovery.strategy=rebootstrap</code> is unable to obtain metadata from any of the brokers in the last known metadata for this interval, client repeats the bootstrap process using <code>bootstrap.servers</code> configuration.';
  /**
   * Java's
   * `AdminClientConfig.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS`.
   */
  static readonly DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS: bigint = 300000n;
  /**
   * Whether the client should send cluster and node information when connecting
   * to a broker to enable it to check for a misrouted connection. This
   * configuration is ignored if rebootstrapping is disabled by setting the
   * configuration `metadata.recovery.strategy=none`. If the client is
   * connecting to a broker older than Apache Kafka 4.4, no checking is
   * performed and this configuration has no effect.
   */
  static readonly METADATA_CLUSTER_CHECK_ENABLE_CONFIG: string = 'metadata.cluster.check.enable';
  /**
   * The documentation of
   * `AdminClientConfig.METADATA_CLUSTER_CHECK_ENABLE_CONFIG`.
   */
  static readonly METADATA_CLUSTER_CHECK_ENABLE_DOC: string = 'Whether the client should send cluster and node information when connecting to a broker to enable it to check for a misrouted connection. This configuration is ignored if rebootstrapping is disabled by setting the configuration <code>metadata.recovery.strategy=none</code>. If the client is connecting to a broker older than Apache Kafka 4.4, no checking is performed and this configuration has no effect.';
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
    throw new TypeError('AdminClientConfig is not instantiable: its ConfigDef machinery is not ' +
      'translated; a configuration is a plain object of its keys');
  }

  // Java's constants are static finals: the class is frozen once its static
  // fields are set, so none can be reassigned.
  static {
    Object.freeze(this);
  }
}
