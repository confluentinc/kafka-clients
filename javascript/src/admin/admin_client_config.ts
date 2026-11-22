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
 * The constants of Java's `org.apache.kafka.clients.admin.AdminClientConfig`:
 * every `public static final` field, evaluated from the Java source, is a
 * static property of the same name; a deprecated one is left out. A `List`
 * constant is a frozen array and a `Set` constant a read-only set whose `add`,
 * `delete` and `clear` throw `UnsupportedOperationError`, both in Java's order.
 * The constants Java's class inherits from `AbstractConfig`
 * (`CONFIG_PROVIDERS_CONFIG`, ...) are properties of the class too. The
 * `ConfigDef` machinery is not translated (a configuration is a plain object of
 * these keys, and the `ConfigDef` types are the types the client checks), so
 * `AdminClientConfig` is not instantiable: constructing it throws `TypeError`.
 * Not generated: `AdminClientConfig(Map<?, ?>)`, `configNames()`,
 * `configDef()`, `main(String[])`, the `ConfigDef` machinery or methods that
 * take a type the client does not translate (`ConfigDef`, `AbstractConfig`).
 *
 * Java: `org.apache.kafka.clients.admin.AdminClientConfig`.
 */
export class AdminClientConfig {
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
   * A list of host/port pairs to use for establishing the initial connection to
   * the KRaft controller quorum. This list should be in the form
   * `host1:port1,host2:port2,...`.
   */
  static readonly BOOTSTRAP_CONTROLLERS_CONFIG: string = 'bootstrap.controllers';
  /**
   * The documentation of `AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG`.
   */
  static readonly BOOTSTRAP_CONTROLLERS_DOC: string = 'A list of host/port pairs to use for establishing the initial connection to the KRaft controller quorum. This list should be in the form <code>host1:port1,host2:port2,...</code>.';
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
   * The documentation of `AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG`.
   */
  static readonly ENABLE_METRICS_PUSH_DOC: string = 'Whether to enable pushing of client metrics to the cluster, if the cluster has a client metrics subscription which matches this client.';
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
   * The configuration controls the maximum amount of time the client will wait
   * for the response of a request. If the response is not received before the
   * timeout elapses the client will resend the request if necessary or fail the
   * request if retries are exhausted.
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
}
