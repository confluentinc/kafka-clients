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

//! Common client configuration constants and the post-processing helpers the
//! client configs share.
//!
//! Corresponds to `org.apache.kafka.clients.CommonClientConfigs`, which is not
//! `@InterfaceAudience.Public`; it stays crate-private. The constants are the
//! ones the crate reads; `metricsReporters` and `telemetryReporter` are not
//! translated, as they configure class-valued keys (CLAUDE.md §2).

use std::collections::HashMap;

use crate::common::Error;
use crate::common::config::SaslConfigs;
use crate::common::config::abstract_config::{AbstractConfig, ConfigMap};
use crate::common::security::auth::SecurityProtocol;

/// Configuration keys shared by producer, consumer and admin clients.
///
/// Translates the Java static-utility class
/// `org.apache.kafka.clients.CommonClientConfigs`, which has no instance state,
/// so it becomes a unit struct hosting its statics as associated items.
#[doc(alias = "org.apache.kafka.clients.CommonClientConfigs")]
pub struct CommonClientConfigs;

impl CommonClientConfigs {
    /// Config key: `bootstrap.servers`
    pub const BOOTSTRAP_SERVERS_CONFIG: &str = "bootstrap.servers";

    /// Config key: `client.dns.lookup`
    ///
    /// Controls how the client uses DNS lookups. If set to `use_all_dns_ips`,
    /// connect to each returned IP address in sequence until a successful
    /// connection is established. After a disconnection, the next IP is used.
    /// Once all IPs have been used once, the client resolves the IP(s) from the
    /// hostname again (both the JVM and the OS cache DNS name lookups,
    /// however). If set to `resolve_canonical_bootstrap_servers_only`, resolve
    /// each bootstrap address into a list of canonical names. After the
    /// bootstrap phase, this behaves the same as `use_all_dns_ips`.
    ///
    /// (Text of Java's `CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC`. That
    /// constant only feeds `ConfigDef`'s generated documentation, which this
    /// crate does not have, so it is carried here as rustdoc rather than as an
    /// unused string constant. The parsed value is a
    /// [`crate::ClientDnsLookup`].)
    pub const CLIENT_DNS_LOOKUP_CONFIG: &str = "client.dns.lookup";

    /// Config key: `client.rack`
    ///
    /// A rack identifier for this client. This can be any string value which
    /// indicates where this client is physically located. It corresponds with the
    /// broker config `broker.rack`.
    ///
    /// (Text of Java's `CommonClientConfigs.CLIENT_RACK_DOC`, carried as rustdoc
    /// for the same reason as [`Self::CLIENT_DNS_LOOKUP_CONFIG`]'s.)
    pub const CLIENT_RACK_CONFIG: &str = "client.rack";

    /// Default value of `client.rack`: no rack.
    pub const DEFAULT_CLIENT_RACK: &str = "";

    /// Config key: `bootstrap.resolve.timeout.ms` (KIP-909)
    ///
    /// Selects the client's bootstrap DNS resolution mode. When set to `0`
    /// (the default), DNS is resolved synchronously during client
    /// construction; any failure surfaces as `ConfigException` and no client
    /// instance is created. When set to a positive value, DNS is resolved
    /// asynchronously and this is the maximum amount of time the client will
    /// spend retrying resolution before failing with an unrecoverable
    /// `BootstrapResolutionException` from subsequent API calls (the client
    /// must then be closed and re-created after fixing the underlying DNS or
    /// `bootstrap.servers` configuration issue). Setting this config to a
    /// positive value enables an evolving feature whose compatibility may be
    /// broken in a minor release.
    ///
    /// (Text of Java's `CommonClientConfigs.BOOTSTRAP_RESOLVE_TIMEOUT_MS_DOC`,
    /// carried as rustdoc for the reason given on
    /// [`Self::CLIENT_DNS_LOOKUP_CONFIG`]. In this crate `ConfigException` is
    /// [`Error::Config`](crate::common::Error::Config) and
    /// `BootstrapResolutionException` is
    /// [`BootstrapResolutionError`](crate::common::errors::BootstrapResolutionError).)
    pub const BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG: &str = "bootstrap.resolve.timeout.ms";

    /// The default `bootstrap.resolve.timeout.ms`: `0`, synchronous resolution
    /// at construction (KAFKA-20939 restored this default; KIP-909 first
    /// shipped `2 * 60 * 1000`).
    pub const DEFAULT_BOOTSTRAP_RESOLVE_TIMEOUT_MS: i64 = 0;

    /// Config key: `metadata.recovery.strategy`
    ///
    /// Controls how the client recovers when none of the brokers known to it
    /// is available. If set to `none`, the client fails. If set to
    /// `rebootstrap`, the client repeats the bootstrap process using
    /// `bootstrap.servers`. Rebootstrapping is useful when a client
    /// communicates with brokers so infrequently that the set of brokers may
    /// change entirely before the client refreshes metadata. Metadata recovery
    /// is triggered when all last-known brokers appear unavailable
    /// simultaneously. Brokers appear unavailable when disconnected and no
    /// current retry attempt is in-progress. Consider increasing
    /// `reconnect.backoff.ms` and `reconnect.backoff.max.ms` and decreasing
    /// `socket.connection.setup.timeout.ms` and
    /// `socket.connection.setup.timeout.max.ms` for the client. Rebootstrap is
    /// also triggered if connection cannot be established to any of the
    /// brokers for `metadata.recovery.rebootstrap.trigger.ms` milliseconds or
    /// if server requests rebootstrap.
    ///
    /// (Text of Java's `CommonClientConfigs.METADATA_RECOVERY_STRATEGY_DOC`
    /// (`CommonClientConfigs.java:248-257`), carried as rustdoc for the reason
    /// given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`].)
    pub const METADATA_RECOVERY_STRATEGY_CONFIG: &str = "metadata.recovery.strategy";

    /// Config key: `metadata.recovery.rebootstrap.trigger.ms`
    ///
    /// If a client configured to rebootstrap using
    /// `metadata.recovery.strategy=rebootstrap` is unable to obtain metadata
    /// from any of the brokers in the last known metadata for this interval,
    /// client repeats the bootstrap process using `bootstrap.servers`
    /// configuration.
    ///
    /// (Text of Java's
    /// `CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC`
    /// (`CommonClientConfigs.java:262-264`), carried as rustdoc for the reason
    /// given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`].)
    pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG: &str = "metadata.recovery.rebootstrap.trigger.ms";

    /// Config key: `metadata.cluster.check.enable` (KIP-1242)
    ///
    /// Whether the client should send cluster and node information when
    /// connecting to a broker to enable it to check for a misrouted
    /// connection. This configuration is ignored if rebootstrapping is
    /// disabled by setting the configuration `metadata.recovery.strategy=none`.
    /// If the client is connecting to a broker older than Apache Kafka 4.4, no
    /// checking is performed and this configuration has no effect.
    ///
    /// (Text of Java's `CommonClientConfigs.METADATA_CLUSTER_CHECK_ENABLE_DOC`
    /// (`CommonClientConfigs.java:267-271`), carried as rustdoc for the reason
    /// given on [`Self::CLIENT_DNS_LOOKUP_CONFIG`]. The producer, consumer and
    /// admin configs define it as `Type.BOOLEAN` with default `true`.)
    pub const METADATA_CLUSTER_CHECK_ENABLE_CONFIG: &str = "metadata.cluster.check.enable";

    /// Config key: `security.protocol`
    pub const SECURITY_PROTOCOL_CONFIG: &str = "security.protocol";

    /// The default `security.protocol`.
    pub const DEFAULT_SECURITY_PROTOCOL: &str = "PLAINTEXT";

    /// The base for exponential retry backoff.
    pub const RETRY_BACKOFF_EXP_BASE: i32 = 2;

    /// The jitter factor for retry backoff.
    pub const RETRY_BACKOFF_JITTER: f64 = 0.2;
}

/// The keys, defaults and documentation the client config definitions share
/// (`AdminClientConfig`, and the producer and consumer configs as they move
/// onto `ConfigDef`).
impl CommonClientConfigs {
    /// Config key: `send.buffer.bytes`
    pub const SEND_BUFFER_CONFIG: &str = "send.buffer.bytes";
    /// The lowest `send.buffer.bytes`: `-1`, the OS default.
    pub const SEND_BUFFER_LOWER_BOUND: i32 = -1;
    /// Config key: `receive.buffer.bytes`
    pub const RECEIVE_BUFFER_CONFIG: &str = "receive.buffer.bytes";
    /// The lowest `receive.buffer.bytes`: `-1`, the OS default.
    pub const RECEIVE_BUFFER_LOWER_BOUND: i32 = -1;
    /// The default `retry.backoff.ms`.
    pub const DEFAULT_RETRY_BACKOFF_MS: i64 = 100;
    /// The default `retry.backoff.max.ms`.
    pub const DEFAULT_RETRY_BACKOFF_MAX_MS: i64 = 1000;
    /// Config key: `enable.metrics.push`
    pub const ENABLE_METRICS_PUSH_CONFIG: &str = "enable.metrics.push";
    /// Config key: `metrics.sample.window.ms`
    pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &str = "metrics.sample.window.ms";
    /// Config key: `metrics.num.samples`
    pub const METRICS_NUM_SAMPLES_CONFIG: &str = "metrics.num.samples";
    /// Config key: `metrics.recording.level`
    pub const METRICS_RECORDING_LEVEL_CONFIG: &str = "metrics.recording.level";
    /// The default `socket.connection.setup.timeout.ms`.
    pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS: i64 = 10 * 1000;
    /// The default `socket.connection.setup.timeout.max.ms`.
    pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS: i64 = 30 * 1000;
    /// The default `metadata.recovery.strategy`: Java's
    /// `MetadataRecoveryStrategy.REBOOTSTRAP.name`.
    pub const DEFAULT_METADATA_RECOVERY_STRATEGY: &str = "rebootstrap";
    /// The default `metadata.recovery.rebootstrap.trigger.ms`.
    pub const DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS: i64 = 300 * 1000;

    /// Documentation of the `BOOTSTRAP_SERVERS` key: Java's `CommonClientConfigs.BOOTSTRAP_SERVERS_DOC`.
    pub const BOOTSTRAP_SERVERS_DOC: &str = "A list of host/port pairs used to establish the initial \
        connection to the Kafka cluster. Clients use this list to bootstrap and discover the full set of \
        Kafka brokers. While the order of servers in the list does not matter, we recommend including more \
        than one server to ensure resilience if any servers are down. This list does not need to contain the \
        entire set of brokers, as Kafka clients automatically manage and update connections to the cluster \
        efficiently. This list must be in the form <code>host1:port1,host2:port2,...</code>.";
    /// Documentation of the `CLIENT_DNS_LOOKUP` key: Java's `CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC`.
    pub const CLIENT_DNS_LOOKUP_DOC: &str = "Controls how the client uses DNS lookups. If set to \
        <code>use_all_dns_ips</code>, connect to each returned IP address in sequence until a successful \
        connection is established. After a disconnection, the next IP is used. Once all IPs have been used \
        once, the client resolves the IP(s) from the hostname again (both the JVM and the OS cache DNS name \
        lookups, however). If set to <code>resolve_canonical_bootstrap_servers_only</code>, resolve each \
        bootstrap address into a list of canonical names. After the bootstrap phase, this behaves the same as \
        <code>use_all_dns_ips</code>.";
    /// Documentation of the `BOOTSTRAP_RESOLVE_TIMEOUT_MS` key: Java's `CommonClientConfigs.BOOTSTRAP_RESOLVE_TIMEOUT_MS_DOC`.
    pub const BOOTSTRAP_RESOLVE_TIMEOUT_MS_DOC: &str = "Selects the client's bootstrap DNS resolution mode. \
        When set to <code>0</code> (the default), DNS is resolved synchronously during client construction; \
        any failure surfaces as <code>ConfigException</code> and no client instance is created. When set to a \
        positive value, DNS is resolved asynchronously and this is the maximum amount of time the client will \
        spend retrying resolution before failing with an unrecoverable \
        <code>BootstrapResolutionException</code> from subsequent API calls (the client must then be closed \
        and re-created after fixing the underlying DNS or <code>bootstrap.servers</code> configuration \
        issue). Setting this config to a positive value enables an evolving feature whose compatibility may \
        be broken in a minor release.";
    /// Documentation of the `METADATA_MAX_AGE` key: Java's `CommonClientConfigs.METADATA_MAX_AGE_DOC`.
    pub const METADATA_MAX_AGE_DOC: &str = "The period of time in milliseconds after which we force a refresh \
        of metadata even if we haven't seen any partition leadership changes to proactively discover any new \
        brokers or partitions.";
    /// Documentation of the `SEND_BUFFER` key: Java's `CommonClientConfigs.SEND_BUFFER_DOC`.
    pub const SEND_BUFFER_DOC: &str = "The size of the TCP send buffer (SO_SNDBUF) to use when sending data. \
        If the value is -1, the OS default will be used.";
    /// Documentation of the `RECEIVE_BUFFER` key: Java's `CommonClientConfigs.RECEIVE_BUFFER_DOC`.
    pub const RECEIVE_BUFFER_DOC: &str = "The size of the TCP receive buffer (SO_RCVBUF) to use when reading \
        data. If the value is -1, the OS default will be used.";
    /// Documentation of the `CLIENT_ID` key: Java's `CommonClientConfigs.CLIENT_ID_DOC`.
    pub const CLIENT_ID_DOC: &str = "An id string to pass to the server when making requests. The purpose of \
        this is to be able to track the source of requests beyond just ip/port by allowing a logical \
        application name to be included in server-side request logging.";
    /// Documentation of the `RECONNECT_BACKOFF_MS` key: Java's `CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC`.
    pub const RECONNECT_BACKOFF_MS_DOC: &str = "The base amount of time to wait before attempting to \
        reconnect to a given host. This avoids repeatedly connecting to a host in a tight loop. This backoff \
        applies to all connection attempts by the client to a broker. This value is the initial backoff value \
        and will increase exponentially for each consecutive connection failure, up to the \
        <code>reconnect.backoff.max.ms</code> value.";
    /// Documentation of the `RECONNECT_BACKOFF_MAX_MS` key: Java's `CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC`.
    pub const RECONNECT_BACKOFF_MAX_MS_DOC: &str = "The maximum amount of time in milliseconds to wait when \
        reconnecting to a broker that has repeatedly failed to connect. If provided, the backoff per host \
        will increase exponentially for each consecutive connection failure, up to this maximum. After \
        calculating the backoff increase, 20% random jitter is added to avoid connection storms.";
    /// Documentation of the `RETRIES` key: Java's `CommonClientConfigs.RETRIES_DOC`.
    pub const RETRIES_DOC: &str = "It is recommended to set the value to either <code>MAX_VALUE</code> or \
        zero, and use corresponding timeout parameters to control how long a client should retry a request. \
        Setting a value greater than zero will cause the client to resend any request that fails with a \
        potentially transient error. Setting a value of zero will lead to transient errors not being retried, \
        and they will be propagated to the application to be handled.";
    /// Documentation of the `RETRY_BACKOFF_MS` key: Java's `CommonClientConfigs.RETRY_BACKOFF_MS_DOC`.
    pub const RETRY_BACKOFF_MS_DOC: &str = "The amount of time to wait before attempting to retry a failed \
        request to a given topic partition. This avoids repeatedly sending requests in a tight loop under \
        some failure scenarios. This value is the initial backoff value and will increase exponentially for \
        each failed request, up to the <code>retry.backoff.max.ms</code> value.";
    /// Documentation of the `RETRY_BACKOFF_MAX_MS` key: Java's `CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC`.
    pub const RETRY_BACKOFF_MAX_MS_DOC: &str = "The maximum amount of time in milliseconds to wait when \
        retrying a request to the broker that has repeatedly failed. If provided, the backoff per client will \
        increase exponentially for each failed request, up to this maximum. To prevent all clients from being \
        synchronized upon retry, a randomized jitter with a factor of 0.2 will be applied to the backoff, \
        resulting in the backoff falling within a range between 20% below and 20% above the computed value. \
        If <code>retry.backoff.ms</code> is set to be higher than <code>retry.backoff.max.ms</code>, then \
        <code>retry.backoff.max.ms</code> will be used as a constant backoff from the beginning without any \
        exponential increase";
    /// Documentation of the `ENABLE_METRICS_PUSH` key: Java's `CommonClientConfigs.ENABLE_METRICS_PUSH_DOC`.
    pub const ENABLE_METRICS_PUSH_DOC: &str = "Whether to enable pushing of client metrics to the cluster, if \
        the cluster has a client metrics subscription which matches this client.";
    /// Documentation of the `METRICS_SAMPLE_WINDOW_MS` key: Java's `CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC`.
    pub const METRICS_SAMPLE_WINDOW_MS_DOC: &str = "The window of time a metrics sample is computed over.";
    /// Documentation of the `METRICS_NUM_SAMPLES` key: Java's `CommonClientConfigs.METRICS_NUM_SAMPLES_DOC`.
    pub const METRICS_NUM_SAMPLES_DOC: &str = "The number of samples maintained to compute metrics.";
    /// Documentation of the `METRICS_RECORDING_LEVEL` key: Java's `CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC`.
    pub const METRICS_RECORDING_LEVEL_DOC: &str = "The highest recording level for metrics. It has three \
        levels for recording metrics - info, debug, and trace.\n \nINFO level records only essential metrics \
        necessary for monitoring system performance and health. It collects vital data without gathering too \
        much detail, making it suitable for production environments where minimal overhead is \
        desired.\n\nDEBUG level records most metrics, providing more detailed information about the system's \
        operation. It's useful for development and testing environments where you need deeper insights to \
        debug and fine-tune the application.\n\nTRACE level records all possible metrics, capturing every \
        detail about the system's performance and operation. It's best for controlled environments where \
        in-depth analysis is required, though it can introduce significant overhead.";
    /// Documentation of the `SECURITY_PROTOCOL` key: Java's `CommonClientConfigs.SECURITY_PROTOCOL_DOC`.
    pub const SECURITY_PROTOCOL_DOC: &str = "Protocol used to communicate with brokers.";
    /// Documentation of the `SOCKET_CONNECTION_SETUP_TIMEOUT_MS` key: Java's `CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC`.
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC: &str = "The amount of time the client will wait for the \
        socket connection to be established. If the connection is not built before the timeout elapses, \
        clients will close the socket channel. This value is the initial backoff value and will increase \
        exponentially for each consecutive connection failure, up to the \
        <code>socket.connection.setup.timeout.max.ms</code> value.";
    /// Documentation of the `SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS` key: Java's `CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC`.
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC: &str = "The maximum amount of time the client will \
        wait for the socket connection to be established. The connection setup timeout will increase \
        exponentially for each consecutive connection failure up to this maximum. To avoid connection storms, \
        a randomization factor of 0.2 will be applied to the timeout resulting in a random range between 20% \
        below and 20% above the computed value.";
    /// Documentation of the `CONNECTIONS_MAX_IDLE_MS` key: Java's `CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC`.
    pub const CONNECTIONS_MAX_IDLE_MS_DOC: &str = "Close idle connections after the number of milliseconds \
        specified by this config.";
    /// Documentation of the `REQUEST_TIMEOUT_MS` key: Java's `CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC`.
    pub const REQUEST_TIMEOUT_MS_DOC: &str = "The configuration controls the maximum amount of time the \
        client will wait for the response of a request. If the response is not received before the timeout \
        elapses the client will resend the request if necessary or fail the request if retries are exhausted.";
    /// Documentation of the `DEFAULT_API_TIMEOUT_MS` key: Java's `CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC`.
    pub const DEFAULT_API_TIMEOUT_MS_DOC: &str = "Specifies the timeout (in milliseconds) for client APIs. \
        This configuration is used as the default timeout for all client operations that do not specify a \
        <code>timeout</code> parameter.";
    /// Documentation of the `METADATA_RECOVERY_STRATEGY` key: Java's `CommonClientConfigs.METADATA_RECOVERY_STRATEGY_DOC`.
    pub const METADATA_RECOVERY_STRATEGY_DOC: &str = "Controls how the client recovers when none of the \
        brokers known to it is available. If set to <code>none</code>, the client fails. If set to \
        <code>rebootstrap</code>, the client repeats the bootstrap process using \
        <code>bootstrap.servers</code>. Rebootstrapping is useful when a client communicates with brokers so \
        infrequently that the set of brokers may change entirely before the client refreshes metadata. \
        Metadata recovery is triggered when all last-known brokers appear unavailable simultaneously. Brokers \
        appear unavailable when disconnected and no current retry attempt is in-progress. Consider increasing \
        <code>reconnect.backoff.ms</code> and <code>reconnect.backoff.max.ms</code> and decreasing \
        <code>socket.connection.setup.timeout.ms</code> and \
        <code>socket.connection.setup.timeout.max.ms</code> for the client. Rebootstrap is also triggered if \
        connection cannot be established to any of the brokers for \
        <code>metadata.recovery.rebootstrap.trigger.ms</code> milliseconds or if server requests rebootstrap.";
    /// Documentation of the `METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS` key: Java's `CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC`.
    pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC: &str = "If a client configured to rebootstrap \
        using <code>metadata.recovery.strategy=rebootstrap</code> is unable to obtain metadata from any of \
        the brokers in the last known metadata for this interval, client repeats the bootstrap process using \
        <code>bootstrap.servers</code> configuration.";
    /// Documentation of the `METADATA_CLUSTER_CHECK_ENABLE` key: Java's `CommonClientConfigs.METADATA_CLUSTER_CHECK_ENABLE_DOC`.
    pub const METADATA_CLUSTER_CHECK_ENABLE_DOC: &str = "Whether the client should send cluster and node \
        information when connecting to a broker to enable it to check for a misrouted connection. This \
        configuration is ignored if rebootstrapping is disabled by setting the configuration \
        <code>metadata.recovery.strategy=none</code>. If the client is connecting to a broker older than \
        Apache Kafka 4.4, no checking is performed and this configuration has no effect.";
    /// Documentation of the `CLIENT_RACK` key: Java's `CommonClientConfigs.CLIENT_RACK_DOC`.
    pub const CLIENT_RACK_DOC: &str = "A rack identifier for this client. This can be any string value which \
        indicates where this client is physically located. It corresponds with the broker config 'broker.rack'";
    /// Documentation of the `GROUP_ID` key: Java's `CommonClientConfigs.GROUP_ID_DOC`.
    pub const GROUP_ID_DOC: &str = "A unique string that identifies the consumer group this consumer belongs \
        to. This property is required if the consumer uses either the group management functionality by using \
        <code>subscribe(topic)</code> or the Kafka-based offset management strategy.";
    /// Documentation of the `GROUP_INSTANCE_ID` key: Java's `CommonClientConfigs.GROUP_INSTANCE_ID_DOC`.
    pub const GROUP_INSTANCE_ID_DOC: &str = "A unique identifier of the consumer instance provided by the end \
        user. Only non-empty strings are permitted. If set, the consumer is treated as a static member, which \
        means that only one instance with this ID is allowed in the consumer group at any time. This can be used \
        in combination with a larger session timeout to avoid group rebalances caused by transient unavailability \
        (e.g. process restarts). If not set, the consumer will join the group as a dynamic member, which is the \
        traditional behavior.";
    /// Documentation of the `MAX_POLL_INTERVAL_MS` key: Java's `CommonClientConfigs.MAX_POLL_INTERVAL_MS_DOC`.
    pub const MAX_POLL_INTERVAL_MS_DOC: &str = "The maximum delay between invocations of poll() when using \
        consumer group management. This places an upper bound on the amount of time that the consumer can be \
        idle before fetching more records. If poll() is not called before expiration of this timeout, then the \
        consumer is considered failed and the group will rebalance in order to reassign the partitions to \
        another member. For consumers using a non-null <code>group.instance.id</code> which reach this timeout, \
        partitions will not be immediately reassigned. Instead, the consumer will stop sending heartbeats and \
        partitions will be reassigned after expiration of the session timeout (defined by the client config \
        <code>session.timeout.ms</code> if using the Classic rebalance protocol, or by the broker config \
        <code>group.consumer.session.timeout.ms</code> if using the Consumer protocol). This mirrors the \
        behavior of a static consumer which has shutdown. In the Classic consumer, \
        <code>connections.max.idle.ms</code> should be set to a value greater than or equal to this timeout to \
        avoid closing the connection to the group coordinator during an ongoing rebalance. This is particularly \
        important in large groups, where a rebalance may take longer to complete while the coordinator waits \
        for all members to rejoin.";
    /// Documentation of the `SESSION_TIMEOUT_MS` key: Java's `CommonClientConfigs.SESSION_TIMEOUT_MS_DOC`.
    pub const SESSION_TIMEOUT_MS_DOC: &str = "The timeout used to detect client failures when using Kafka's \
        group management facility. The client sends periodic heartbeats to indicate its liveness to the broker. \
        If no heartbeats are received by the broker before the expiration of this session timeout, then the \
        broker will remove this client from the group and initiate a rebalance. Note that the value must be in \
        the allowable range as configured in the broker configuration by <code>group.min.session.timeout.ms</code> \
        and <code>group.max.session.timeout.ms</code>. Note that this client configuration is not supported when \
        <code>group.protocol</code> is set to \"consumer\". In that case, session timeout is controlled by the \
        broker config <code>group.consumer.session.timeout.ms</code>.";
    /// Documentation of the `HEARTBEAT_INTERVAL_MS` key: Java's `CommonClientConfigs.HEARTBEAT_INTERVAL_MS_DOC`.
    pub const HEARTBEAT_INTERVAL_MS_DOC: &str = "The expected time between heartbeats to the consumer \
        coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the \
        consumer's session stays active and to facilitate rebalancing when new consumers join or leave the \
        group. This config is only supported if <code>group.protocol</code> is set to \"classic\". In that \
        case, the value must be set lower than <code>session.timeout.ms</code>, but typically should be set no \
        higher than 1/3 of that value. It can be adjusted even lower to control the expected time for normal \
        rebalances.If <code>group.protocol</code> is set to \"consumer\", this config is not supported, as the \
        heartbeat interval is controlled by the broker with <code>group.consumer.heartbeat.interval.ms</code>.";
}

impl CommonClientConfigs {
    /// Config key: `retry.backoff.ms`
    pub const RETRY_BACKOFF_MS_CONFIG: &str = "retry.backoff.ms";

    /// Config key: `retry.backoff.max.ms`
    pub const RETRY_BACKOFF_MAX_MS_CONFIG: &str = "retry.backoff.max.ms";

    /// Config key: `reconnect.backoff.ms`
    pub const RECONNECT_BACKOFF_MS_CONFIG: &str = "reconnect.backoff.ms";

    /// Config key: `reconnect.backoff.max.ms`
    pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &str = "reconnect.backoff.max.ms";

    /// Config key: `socket.connection.setup.timeout.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &str = "socket.connection.setup.timeout.ms";

    /// Config key: `socket.connection.setup.timeout.max.ms`
    pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &str = "socket.connection.setup.timeout.max.ms";

    /// Postprocess the configuration so that exponential backoff is disabled
    /// when reconnect backoff is explicitly configured but the maximum
    /// reconnect backoff is not explicitly configured.
    ///
    /// Returns the updates to apply to the configuration.
    #[doc(alias = "org.apache.kafka.clients.CommonClientConfigs#postProcessReconnectBackoffConfigs")]
    pub(crate) fn post_process_reconnect_backoff_configs(
        config: &AbstractConfig,
        parsed_values: &ConfigMap,
    ) -> ConfigMap {
        let mut rval = HashMap::new();
        let original_config = config.originals_map();
        if !original_config.contains_key(Self::RECONNECT_BACKOFF_MAX_MS_CONFIG)
            && original_config.contains_key(Self::RECONNECT_BACKOFF_MS_CONFIG)
        {
            log::warn!(
                "Disabling exponential reconnect backoff because {} is set, but {} is not.",
                Self::RECONNECT_BACKOFF_MS_CONFIG,
                Self::RECONNECT_BACKOFF_MAX_MS_CONFIG
            );
            rval.insert(
                Self::RECONNECT_BACKOFF_MAX_MS_CONFIG.to_string(),
                parsed_values.get(Self::RECONNECT_BACKOFF_MS_CONFIG).cloned().flatten(),
            );
        }
        rval
    }

    /// Log warning if the exponential backoff is disabled due to initial
    /// backoff value is greater than max backoff value.
    ///
    /// # Errors
    ///
    /// The error of a getter: a key is not defined, not a `LONG`, or `null`.
    #[doc(alias = "org.apache.kafka.clients.CommonClientConfigs#warnDisablingExponentialBackoff")]
    pub(crate) fn warn_disabling_exponential_backoff(config: &AbstractConfig) -> Result<(), Error> {
        let retry_backoff_ms = required_long(config, Self::RETRY_BACKOFF_MS_CONFIG)?;
        let retry_backoff_max_ms = required_long(config, Self::RETRY_BACKOFF_MAX_MS_CONFIG)?;
        if retry_backoff_ms > retry_backoff_max_ms {
            log::warn!(
                "Configuration '{}' with value '{}' is greater than configuration '{}' with value '{}'. A static \
                 backoff with value '{}' will be applied.",
                Self::RETRY_BACKOFF_MS_CONFIG,
                retry_backoff_ms,
                Self::RETRY_BACKOFF_MAX_MS_CONFIG,
                retry_backoff_max_ms,
                retry_backoff_max_ms
            );
        }

        let connection_setup_timeout_ms = required_long(config, Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)?;
        let connection_setup_timeout_max_ms =
            required_long(config, Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)?;
        if connection_setup_timeout_ms > connection_setup_timeout_max_ms {
            log::warn!(
                "Configuration '{}' with value '{}' is greater than configuration '{}' with value '{}'. A static \
                 connection setup timeout with value '{}' will be applied.",
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                connection_setup_timeout_ms,
                Self::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                connection_setup_timeout_max_ms,
                connection_setup_timeout_max_ms
            );
        }
        Ok(())
    }

    /// Java's `postValidateSaslMechanismConfig`: a SASL `security.protocol`
    /// needs a non-empty `sasl.mechanism`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] `Invalid value null for configuration sasl.mechanism:
    /// When the security.protocol configuration enables SASL, mechanism must
    /// be non-null and non-empty string.`, or the error of a getter.
    #[doc(alias = "org.apache.kafka.clients.CommonClientConfigs#postValidateSaslMechanismConfig")]
    pub(crate) fn post_validate_sasl_mechanism_config(config: &AbstractConfig) -> Result<(), Error> {
        let security_protocol = config
            .get_string(Self::SECURITY_PROTOCOL_CONFIG)?
            .and_then(SecurityProtocol::for_name);
        let client_sasl_mechanism = config.get_string(SaslConfigs::SASL_MECHANISM)?;
        if matches!(
            security_protocol,
            Some(SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl)
        ) && client_sasl_mechanism.is_none_or(str::is_empty)
        {
            return Err(Error::config_name_value_message(
                SaslConfigs::SASL_MECHANISM,
                "null",
                format!(
                    "When the {} configuration enables SASL, mechanism must be non-null and non-empty string.",
                    Self::SECURITY_PROTOCOL_CONFIG
                ),
            ));
        }
        Ok(())
    }
}

/// Java's `long x = config.getLong(key)`. A `null` value fails Java's
/// unboxing with a `NullPointerException`, here an
/// [`Error::LocalIllegalState`]; the client definitions give every such key a
/// non-null default, so it cannot happen there.
fn required_long(config: &AbstractConfig, key: &str) -> Result<i64, Error> {
    config
        .get_long(key)?
        .ok_or_else(|| Error::local_illegal_state(format!("Configuration '{key}' has a null value")))
}

/// Translated from `CommonClientConfigsTest`. `testMetricsReporters` is not
/// translated: `metric.reporters` is class-valued (CLAUDE.md §2).
#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use super::*;
    use crate::common::config::ConfigDef;
    use crate::common::config::abstract_config::string_originals;
    use crate::common::config::config_def::{Importance, ParsedValue, Range, Type, ValidString};

    static TEST_CONFIG_DEF: LazyLock<ConfigDef> = LazyLock::new(|| {
        let names = SecurityProtocol::names();
        ConfigDef::new()
            .define_with_validator(
                CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG,
                Type::Long,
                50i64,
                Range::at_least(0i64),
                Importance::Low,
                "",
            )
            .define_with_validator(
                CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG,
                Type::Long,
                1000i64,
                Range::at_least(0i64),
                Importance::Low,
                "",
            )
            .define_with_validator(
                CommonClientConfigs::SECURITY_PROTOCOL_CONFIG,
                Type::String,
                CommonClientConfigs::DEFAULT_SECURITY_PROTOCOL,
                ValidString::r#in(&names),
                Importance::Medium,
                "",
            )
            .define(
                SaslConfigs::SASL_MECHANISM,
                Type::String,
                SaslConfigs::DEFAULT_SASL_MECHANISM,
                Importance::Medium,
                "",
            )
    });

    /// Java's `TestConfig`, whose `postProcessParsedConfig` validates the SASL
    /// mechanism and post-processes the reconnect backoff.
    fn test_config(props: ConfigMap) -> Result<AbstractConfig, Error> {
        AbstractConfig::with_hooks(
            &TEST_CONFIG_DEF,
            props,
            false,
            "TestConfig",
            |parsed| parsed,
            |config| {
                CommonClientConfigs::post_validate_sasl_mechanism_config(config)?;
                Ok(CommonClientConfigs::post_process_reconnect_backoff_configs(
                    config,
                    config.parsed_values(),
                ))
            },
        )
    }

    fn props(entries: &[(&str, &str)]) -> ConfigMap {
        string_originals(&entries.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect())
    }

    #[test]
    fn test_exponential_backoff_defaults() {
        let default_conf = test_config(ConfigMap::new()).unwrap();
        assert_eq!(
            default_conf.get_long(CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG).unwrap(),
            Some(50)
        );
        assert_eq!(
            default_conf
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG)
                .unwrap(),
            Some(1000)
        );

        let both_set_config = test_config(props(&[
            (CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG, "123"),
            (CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG, "12345"),
        ]))
        .unwrap();
        assert_eq!(
            both_set_config
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG)
                .unwrap(),
            Some(123)
        );
        assert_eq!(
            both_set_config
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG)
                .unwrap(),
            Some(12345)
        );

        let reconnect_backoff_set_conf =
            test_config(props(&[(CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG, "123")])).unwrap();
        assert_eq!(
            reconnect_backoff_set_conf
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MS_CONFIG)
                .unwrap(),
            Some(123)
        );
        assert_eq!(
            reconnect_backoff_set_conf
                .get_long(CommonClientConfigs::RECONNECT_BACKOFF_MAX_MS_CONFIG)
                .unwrap(),
            Some(123)
        );
    }

    #[test]
    fn test_invalid_sasl_mechanism() {
        let mut configs = props(&[(
            CommonClientConfigs::SECURITY_PROTOCOL_CONFIG,
            SecurityProtocol::SaslPlaintext.name(),
        )]);
        configs.insert(SaslConfigs::SASL_MECHANISM.to_string(), None);
        let ce = test_config(configs.clone()).unwrap_err();
        assert!(ce.is_config_error(), "{ce:?}");
        assert!(ce.to_string().contains(SaslConfigs::SASL_MECHANISM), "{ce}");
        assert_eq!(
            ce.to_string(),
            "ConfigError: Invalid value null for configuration sasl.mechanism: When the security.protocol \
             configuration enables SASL, mechanism must be non-null and non-empty string."
        );

        configs.insert(
            SaslConfigs::SASL_MECHANISM.to_string(),
            Some(ParsedValue::String(String::new())),
        );
        let ce = test_config(configs).unwrap_err();
        assert!(ce.to_string().contains(SaslConfigs::SASL_MECHANISM), "{ce}");
    }

    static BACKOFF_DEF: LazyLock<ConfigDef> = LazyLock::new(|| {
        ConfigDef::new()
            .define(
                CommonClientConfigs::RETRY_BACKOFF_MS_CONFIG,
                Type::Long,
                100i64,
                Importance::Low,
                "",
            )
            .define(
                CommonClientConfigs::RETRY_BACKOFF_MAX_MS_CONFIG,
                Type::Long,
                1000i64,
                Importance::Low,
                "",
            )
            .define(
                CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                Type::Long,
                10_000i64,
                Importance::Low,
                "",
            )
            .define(
                CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                Type::Long,
                30_000i64,
                Importance::Low,
                "",
            )
    });

    /// `warnDisablingExponentialBackoff` only logs, and reads (so marks used)
    /// the four keys.
    #[test]
    fn test_warn_disabling_exponential_backoff_marks_keys_used() {
        let config = AbstractConfig::new(
            &BACKOFF_DEF,
            props(&[
                (CommonClientConfigs::RETRY_BACKOFF_MS_CONFIG, "5000"),
                (CommonClientConfigs::SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "60000"),
            ]),
            false,
        )
        .unwrap();
        CommonClientConfigs::warn_disabling_exponential_backoff(&config).unwrap();
        assert!(config.unused().is_empty(), "{:?}", config.unused());
    }
}
