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
// Java's `org.apache.kafka.clients.consumer.ConsumerConfig`;
// `cargo xtask check-generated` checks it is up to date.

/**
 * The consumer configuration keys
 */
export class ConsumerConfig {
  /**
   * Java's `ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS`.
   */
  static readonly ASSIGN_FROM_SUBSCRIBED_ASSIGNORS: ReadonlyArray<string> = Object.freeze(['range', 'roundrobin', 'sticky', 'cooperative-sticky']);
  /**
   * `group.id`
   */
  static readonly GROUP_ID_CONFIG: string = 'group.id';
  /**
   * `group.instance.id`
   */
  static readonly GROUP_INSTANCE_ID_CONFIG: string = 'group.instance.id';
  /**
   * `max.poll.records`
   */
  static readonly MAX_POLL_RECORDS_CONFIG: string = 'max.poll.records';
  /**
   * Java's `ConsumerConfig.DEFAULT_MAX_POLL_RECORDS`.
   */
  static readonly DEFAULT_MAX_POLL_RECORDS: number = 500;
  /**
   * `max.poll.interval.ms`
   */
  static readonly MAX_POLL_INTERVAL_MS_CONFIG: string = 'max.poll.interval.ms';
  /**
   * `session.timeout.ms`
   */
  static readonly SESSION_TIMEOUT_MS_CONFIG: string = 'session.timeout.ms';
  /**
   * `heartbeat.interval.ms`
   */
  static readonly HEARTBEAT_INTERVAL_MS_CONFIG: string = 'heartbeat.interval.ms';
  /**
   * `group.protocol`
   */
  static readonly GROUP_PROTOCOL_CONFIG: string = 'group.protocol';
  /**
   * Java's `ConsumerConfig.DEFAULT_GROUP_PROTOCOL`.
   */
  static readonly DEFAULT_GROUP_PROTOCOL: string = 'classic';
  /**
   * The documentation of `ConsumerConfig.GROUP_PROTOCOL_CONFIG`.
   */
  static readonly GROUP_PROTOCOL_DOC: string = 'The group protocol that the consumer uses. The supported values are <code>classic</code> or <code>consumer</code>. The default value is <code>classic</code>.';
  /**
   * `group.remote.assignor`
   */
  static readonly GROUP_REMOTE_ASSIGNOR_CONFIG: string = 'group.remote.assignor';
  /**
   * Java's `ConsumerConfig.DEFAULT_GROUP_REMOTE_ASSIGNOR`.
   */
  static readonly DEFAULT_GROUP_REMOTE_ASSIGNOR: string | null = null;
  /**
   * The documentation of `ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG`.
   */
  static readonly GROUP_REMOTE_ASSIGNOR_DOC: string = 'The name of the server-side assignor to use. If not specified, the group coordinator will pick the first assignor defined in the broker config group.consumer.assignors.This configuration is applied only if <code>group.protocol</code> is set to "consumer".';
  /**
   * `bootstrap.servers`
   */
  static readonly BOOTSTRAP_SERVERS_CONFIG: string = 'bootstrap.servers';
  /**
   * `client.dns.lookup`
   */
  static readonly CLIENT_DNS_LOOKUP_CONFIG: string = 'client.dns.lookup';
  /**
   * `bootstrap.resolve.timeout.ms`
   */
  static readonly BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: string = 'bootstrap.resolve.timeout.ms';
  /**
   * `enable.auto.commit`
   */
  static readonly ENABLE_AUTO_COMMIT_CONFIG: string = 'enable.auto.commit';
  /**
   * `auto.commit.interval.ms`
   */
  static readonly AUTO_COMMIT_INTERVAL_MS_CONFIG: string = 'auto.commit.interval.ms';
  /**
   * `partition.assignment.strategy`
   */
  static readonly PARTITION_ASSIGNMENT_STRATEGY_CONFIG: string = 'partition.assignment.strategy';
  /**
   * `auto.offset.reset`
   */
  static readonly AUTO_OFFSET_RESET_CONFIG: string = 'auto.offset.reset';
  /**
   * The documentation of `ConsumerConfig.AUTO_OFFSET_RESET_CONFIG`.
   */
  static readonly AUTO_OFFSET_RESET_DOC: string = 'What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted): <ul><li>earliest: automatically reset the offset to the earliest offset</li><li>latest: automatically reset the offset to the latest offset</li><li>by_duration:&lt;duration&gt;: automatically reset the offset to a configured &lt;duration&gt; from the current timestamp. &lt;duration&gt; must be specified in ISO8601 format (PnDTnHnMn.nS). Negative duration is not allowed.</li><li>none: throw exception to the consumer if no previous offset is found for the consumer\'s group</li><li>anything else: throw exception to the consumer.</li></ul><p>Note that increasing a topic\'s partition count while this config is set to <code>latest</code> may cause silent message loss: producers may begin appending records to a newly created partition before the consumer discovers it, and <code>latest</code> resets the position to the log end offset, skipping any records produced during that discovery gap.</p><p>To avoid this, prefer <code>by_duration:&lt;duration&gt;</code>. When a partition has no committed offset, <code>by_duration</code> determines the starting position by issuing a <code>ListOffsets</code> lookup for <code>now() - duration</code>. If the target timestamp is earlier than the partition\'s creation time, the lookup returns the partition\'s start offset, ensuring that records produced during the discovery window are still consumed. Size the duration to cover the worst-case partition-discovery latency for the group protocol in use:</p><ul><li>With the <code>consumer</code> group protocol (KIP-848), newly assigned partitions are pushed on the next group heartbeat, so a value at least as large as <code>group.consumer.heartbeat.interval.ms</code> (server default 5000&nbsp;ms) is sufficient, for example <code>by_duration:PT5S</code>.</li><li>With the <code>classic</code> group protocol, new partitions are discovered through periodic metadata refresh and a subsequent rebalance, so the duration must exceed <code>metadata.max.age.ms</code> (client default 300000&nbsp;ms) plus the rebalance time, for example <code>by_duration:PT6M</code>.</li></ul><p>Consumers with a valid committed offset are unaffected. The reset applies only to partitions whose offset is missing or out of range, so <code>by_duration</code> does not force existing consumers to replay historical data on restart.</p>';
  /**
   * `fetch.min.bytes`
   */
  static readonly FETCH_MIN_BYTES_CONFIG: string = 'fetch.min.bytes';
  /**
   * Java's `ConsumerConfig.DEFAULT_FETCH_MIN_BYTES`.
   */
  static readonly DEFAULT_FETCH_MIN_BYTES: number = 1;
  /**
   * `fetch.max.bytes`
   */
  static readonly FETCH_MAX_BYTES_CONFIG: string = 'fetch.max.bytes';
  /**
   * Java's `ConsumerConfig.DEFAULT_FETCH_MAX_BYTES`.
   */
  static readonly DEFAULT_FETCH_MAX_BYTES: number = 52428800;
  /**
   * `fetch.max.wait.ms`
   */
  static readonly FETCH_MAX_WAIT_MS_CONFIG: string = 'fetch.max.wait.ms';
  /**
   * Java's `ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS`.
   */
  static readonly DEFAULT_FETCH_MAX_WAIT_MS: number = 500;
  /**
   * `metadata.max.age.ms`
   */
  static readonly METADATA_MAX_AGE_CONFIG: string = 'metadata.max.age.ms';
  /**
   * `max.partition.fetch.bytes`
   */
  static readonly MAX_PARTITION_FETCH_BYTES_CONFIG: string = 'max.partition.fetch.bytes';
  /**
   * Java's `ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES`.
   */
  static readonly DEFAULT_MAX_PARTITION_FETCH_BYTES: number = 1048576;
  /**
   * `send.buffer.bytes`
   */
  static readonly SEND_BUFFER_CONFIG: string = 'send.buffer.bytes';
  /**
   * `receive.buffer.bytes`
   */
  static readonly RECEIVE_BUFFER_CONFIG: string = 'receive.buffer.bytes';
  /**
   * `client.id`
   */
  static readonly CLIENT_ID_CONFIG: string = 'client.id';
  /**
   * `client.rack`
   */
  static readonly CLIENT_RACK_CONFIG: string = 'client.rack';
  /**
   * Java's `ConsumerConfig.DEFAULT_CLIENT_RACK`.
   */
  static readonly DEFAULT_CLIENT_RACK: string = '';
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
   * `enable.metrics.push`
   */
  static readonly ENABLE_METRICS_PUSH_CONFIG: string = 'enable.metrics.push';
  /**
   * The documentation of `ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG`.
   */
  static readonly ENABLE_METRICS_PUSH_DOC: string = 'Whether to enable pushing of client metrics to the cluster, if the cluster has a client metrics subscription which matches this client.';
  /**
   * `retry.backoff.max.ms`
   */
  static readonly RETRY_BACKOFF_MAX_MS_CONFIG: string = 'retry.backoff.max.ms';
  /**
   * `metrics.sample.window.ms`
   */
  static readonly METRICS_SAMPLE_WINDOW_MS_CONFIG: string = 'metrics.sample.window.ms';
  /**
   * `metrics.num.samples`
   */
  static readonly METRICS_NUM_SAMPLES_CONFIG: string = 'metrics.num.samples';
  /**
   * `metrics.log.level`
   */
  static readonly METRICS_RECORDING_LEVEL_CONFIG: string = 'metrics.recording.level';
  /**
   * `metric.reporters`
   */
  static readonly METRIC_REPORTER_CLASSES_CONFIG: string = 'metric.reporters';
  /**
   * `check.crcs`
   */
  static readonly CHECK_CRCS_CONFIG: string = 'check.crcs';
  /**
   * `key.deserializer`
   */
  static readonly KEY_DESERIALIZER_CLASS_CONFIG: string = 'key.deserializer';
  /**
   * The documentation of `ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG`.
   */
  static readonly KEY_DESERIALIZER_CLASS_DOC: string = 'Deserializer class for key that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.';
  /**
   * `value.deserializer`
   */
  static readonly VALUE_DESERIALIZER_CLASS_CONFIG: string = 'value.deserializer';
  /**
   * The documentation of `ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG`.
   */
  static readonly VALUE_DESERIALIZER_CLASS_DOC: string = 'Deserializer class for value that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.';
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
   * `default.api.timeout.ms`
   */
  static readonly DEFAULT_API_TIMEOUT_MS_CONFIG: string = 'default.api.timeout.ms';
  /**
   * `interceptor.classes`
   */
  static readonly INTERCEPTOR_CLASSES_CONFIG: string = 'interceptor.classes';
  /**
   * The documentation of `ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG`.
   */
  static readonly INTERCEPTOR_CLASSES_DOC: string = 'A list of classes to use as interceptors. Implementing the <code>org.apache.kafka.clients.consumer.ConsumerInterceptor</code> interface allows you to intercept (and possibly mutate) records received by the consumer. By default, there are no interceptors.';
  /**
   * `exclude.internal.topics`
   */
  static readonly EXCLUDE_INTERNAL_TOPICS_CONFIG: string = 'exclude.internal.topics';
  /**
   * Java's `ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS`.
   */
  static readonly DEFAULT_EXCLUDE_INTERNAL_TOPICS: boolean = true;
  /**
   * `isolation.level`
   */
  static readonly ISOLATION_LEVEL_CONFIG: string = 'isolation.level';
  /**
   * The documentation of `ConsumerConfig.ISOLATION_LEVEL_CONFIG`.
   */
  static readonly ISOLATION_LEVEL_DOC: string = 'Controls how to read messages written transactionally. If set to <code>read_committed</code>, consumer.poll() will only return transactional messages which have been committed. If set to <code>read_uncommitted</code> (the default), consumer.poll() will return all messages, even transactional messages which have been aborted. Non-transactional messages will be returned unconditionally in either mode. <p>Messages will always be returned in offset order. Hence, in  <code>read_committed</code> mode, consumer.poll() will only return messages up to the last stable offset (LSO), which is the one less than the offset of the first open transaction. In particular any messages appearing after messages belonging to ongoing transactions will be withheld until the relevant transaction has been completed. As a result, <code>read_committed</code> consumers will not be able to read up to the high watermark when there are in flight transactions.</p><p> Further, when in <code>read_committed</code> the seekToEnd method will return the LSO</p>';
  /**
   * Java's `ConsumerConfig.DEFAULT_ISOLATION_LEVEL`.
   */
  static readonly DEFAULT_ISOLATION_LEVEL: string = 'read_uncommitted';
  /**
   * `allow.auto.create.topics`
   */
  static readonly ALLOW_AUTO_CREATE_TOPICS_CONFIG: string = 'allow.auto.create.topics';
  /**
   * Java's `ConsumerConfig.DEFAULT_ALLOW_AUTO_CREATE_TOPICS`.
   */
  static readonly DEFAULT_ALLOW_AUTO_CREATE_TOPICS: boolean = true;
  /**
   * `security.providers`
   */
  static readonly SECURITY_PROVIDERS_CONFIG: string = 'security.providers';
  /**
   * `share.acknowledgement.mode`
   */
  static readonly SHARE_ACKNOWLEDGEMENT_MODE_CONFIG: string = 'share.acknowledgement.mode';
  /**
   * `share.acquire.mode`
   */
  static readonly SHARE_ACQUIRE_MODE_CONFIG: string = 'share.acquire.mode';
  /**
   * Java's `ConsumerConfig.DEFAULT_SHARE_ACQUIRE_MODE`.
   */
  static readonly DEFAULT_SHARE_ACQUIRE_MODE: string = 'BATCH_OPTIMIZED';
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
    throw new TypeError('ConsumerConfig is not instantiable: its ConfigDef machinery is not ' +
      'translated; a configuration is a plain object of its keys');
  }
}
