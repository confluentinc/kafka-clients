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

//! Translation of `org.apache.kafka.clients.CommonClientConfigs`.
//!
//! Configurations shared by Kafka client applications: producer,
//! consumer, connect, etc.
//!
//! # Phase scope
//!
//! Phase 4c translates the producer-relevant subset:
//! - All `pub const` config keys, defaults, and `_DOC` strings.
//! - [`post_process_reconnect_backoff_configs`] (deferred-style logic
//!   adapted to a returned map).
//! - [`warn_disabling_exponential_backoff`].
//! - [`post_validate_sasl_mechanism_config`].
//!
//! The following Java methods are intentionally absent from this
//! translation and will land in Phase 5 once their dependencies exist:
//! - `metricsReporters(...)` — needs the `MetricsReporter` trait
//!   (Phase 5/6).
//! - `telemetryReporter(...)` — needs `ClientTelemetryReporter`
//!   (Phase 5/6).
//! - `MetricsReporterContextImpl` (inner class, not in the producer
//!   path) — Phase 5.

use crate::common::errors::KafkaError;

// ---------------------------------------------------------------------
// NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE
// PUBLIC API AND CHANGE WILL BREAK USER CODE. (Mirrors the same
// warning in `CommonClientConfigs.java`.)
// ---------------------------------------------------------------------

pub const BOOTSTRAP_SERVERS_CONFIG: &str = "bootstrap.servers";
pub const BOOTSTRAP_SERVERS_DOC: &str = concat!(
    "A list of host/port pairs used to establish the initial connection to the Kafka cluster. ",
    "Clients use this list to bootstrap and discover the full set of Kafka brokers. ",
    "While the order of servers in the list does not matter, we recommend including more than one server to ensure resilience if any servers are down. ",
    "This list does not need to contain the entire set of brokers, as Kafka clients automatically manage and update connections to the cluster efficiently. ",
    "This list must be in the form <code>host1:port1,host2:port2,...</code>.",
);

pub const CLIENT_DNS_LOOKUP_CONFIG: &str = "client.dns.lookup";
pub const CLIENT_DNS_LOOKUP_DOC: &str = concat!(
    "Controls how the client uses DNS lookups. ",
    "If set to <code>use_all_dns_ips</code>, connect to each returned IP ",
    "address in sequence until a successful connection is established. ",
    "After a disconnection, the next IP is used. Once all IPs have been ",
    "used once, the client resolves the IP(s) from the hostname again ",
    "(both the JVM and the OS cache DNS name lookups, however). ",
    "If set to <code>resolve_canonical_bootstrap_servers_only</code>, ",
    "resolve each bootstrap address into a list of canonical names. After ",
    "the bootstrap phase, this behaves the same as <code>use_all_dns_ips</code>.",
);

pub const METADATA_MAX_AGE_CONFIG: &str = "metadata.max.age.ms";
pub const METADATA_MAX_AGE_DOC: &str = "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.";

pub const SEND_BUFFER_CONFIG: &str = "send.buffer.bytes";
pub const SEND_BUFFER_DOC: &str = "The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS default will be used.";
pub const SEND_BUFFER_LOWER_BOUND: i32 = -1;

pub const RECEIVE_BUFFER_CONFIG: &str = "receive.buffer.bytes";
pub const RECEIVE_BUFFER_DOC: &str = "The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS default will be used.";
pub const RECEIVE_BUFFER_LOWER_BOUND: i32 = -1;

pub const CLIENT_ID_CONFIG: &str = "client.id";
pub const CLIENT_ID_DOC: &str = "An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.";

pub const CLIENT_RACK_CONFIG: &str = "client.rack";
pub const CLIENT_RACK_DOC: &str = "A rack identifier for this client. This can be any string value which indicates where this client is physically located. It corresponds with the broker config 'broker.rack'";
pub const DEFAULT_CLIENT_RACK: &str = "";

pub const RECONNECT_BACKOFF_MS_CONFIG: &str = "reconnect.backoff.ms";
pub const RECONNECT_BACKOFF_MS_DOC: &str = concat!(
    "The base amount of time to wait before attempting to reconnect to a given host. ",
    "This avoids repeatedly connecting to a host in a tight loop. This backoff applies to all connection attempts by the client to a broker. ",
    "This value is the initial backoff value and will increase exponentially for each consecutive connection failure, up to the <code>reconnect.backoff.max.ms</code> value.",
);

pub const RECONNECT_BACKOFF_MAX_MS_CONFIG: &str = "reconnect.backoff.max.ms";
pub const RECONNECT_BACKOFF_MAX_MS_DOC: &str = concat!(
    "The maximum amount of time in milliseconds to wait when reconnecting to a broker that has repeatedly failed to connect. ",
    "If provided, the backoff per host will increase exponentially for each consecutive connection failure, up to this maximum. After calculating the backoff increase, 20% random jitter is added to avoid connection storms.",
);

pub const RETRIES_CONFIG: &str = "retries";
pub const RETRIES_DOC: &str = concat!(
    "It is recommended to set the value to either <code>MAX_VALUE</code> or zero, and use corresponding timeout parameters to control how long a client should retry a request.",
    " Setting a value greater than zero will cause the client to resend any request that fails with a potentially transient error.",
    " Setting a value of zero will lead to transient errors not being retried, and they will be propagated to the application to be handled.",
);

pub const RETRY_BACKOFF_MS_CONFIG: &str = "retry.backoff.ms";
pub const RETRY_BACKOFF_MS_DOC: &str = concat!(
    "The amount of time to wait before attempting to retry a failed request to a given topic partition. ",
    "This avoids repeatedly sending requests in a tight loop under some failure scenarios. This value is the initial backoff value and will increase exponentially for each failed request, ",
    "up to the <code>retry.backoff.max.ms</code> value.",
);
pub const DEFAULT_RETRY_BACKOFF_MS: i64 = 100;

pub const RETRY_BACKOFF_MAX_MS_CONFIG: &str = "retry.backoff.max.ms";
pub const RETRY_BACKOFF_MAX_MS_DOC: &str = concat!(
    "The maximum amount of time in milliseconds to wait when retrying a request to the broker that has repeatedly failed. ",
    "If provided, the backoff per client will increase exponentially for each failed request, up to this maximum. To prevent all clients from being synchronized upon retry, ",
    "a randomized jitter with a factor of 0.2 will be applied to the backoff, resulting in the backoff falling within a range between 20% below and 20% above the computed value. ",
    "If <code>retry.backoff.ms</code> is set to be higher than <code>retry.backoff.max.ms</code>, then <code>retry.backoff.max.ms</code> will be used as a constant backoff from the beginning without any exponential increase",
);
pub const DEFAULT_RETRY_BACKOFF_MAX_MS: i64 = 1000;

pub const RETRY_BACKOFF_EXP_BASE: i32 = 2;
pub const RETRY_BACKOFF_JITTER: f64 = 0.2;

pub const ENABLE_METRICS_PUSH_CONFIG: &str = "enable.metrics.push";
pub const ENABLE_METRICS_PUSH_DOC: &str = "Whether to enable pushing of client metrics to the cluster, if the cluster has a client metrics subscription which matches this client.";

pub const METRICS_SAMPLE_WINDOW_MS_CONFIG: &str = "metrics.sample.window.ms";
pub const METRICS_SAMPLE_WINDOW_MS_DOC: &str = "The window of time a metrics sample is computed over.";

pub const METRICS_NUM_SAMPLES_CONFIG: &str = "metrics.num.samples";
pub const METRICS_NUM_SAMPLES_DOC: &str = "The number of samples maintained to compute metrics.";

pub const METRICS_RECORDING_LEVEL_CONFIG: &str = "metrics.recording.level";
pub const METRICS_RECORDING_LEVEL_DOC: &str = concat!(
    "The highest recording level for metrics. It has three levels for recording metrics - info, debug, and trace.\n",
    " \n",
    "INFO level records only essential metrics necessary for monitoring system performance and health. It collects vital data without gathering too much detail, making it suitable for production environments where minimal overhead is desired.\n",
    "\n",
    "DEBUG level records most metrics, providing more detailed information about the system's operation. It's useful for development and testing environments where you need deeper insights to debug and fine-tune the application.\n",
    "\n",
    "TRACE level records all possible metrics, capturing every detail about the system's performance and operation. It's best for controlled environments where in-depth analysis is required, though it can introduce significant overhead.",
);

pub const METRIC_REPORTER_CLASSES_CONFIG: &str = "metric.reporters";
pub const METRIC_REPORTER_CLASSES_DOC: &str = concat!(
    "A list of classes to use as metrics reporters. ",
    "Implementing the <code>org.apache.kafka.common.metrics.MetricsReporter</code> interface allows plugging in classes that will be notified of new metric creation. ",
    "When custom reporters are set and <code>org.apache.kafka.common.metrics.JmxReporter</code> is needed, it has to be explicitly added to the list.",
);

pub const METRICS_CONTEXT_PREFIX: &str = "metrics.context.";

pub const SECURITY_PROTOCOL_CONFIG: &str = "security.protocol";
pub const SECURITY_PROTOCOL_DOC: &str = "Protocol used to communicate with brokers.";
pub const DEFAULT_SECURITY_PROTOCOL: &str = "PLAINTEXT";

pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG: &str = "socket.connection.setup.timeout.ms";
pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC: &str = concat!(
    "The amount of time the client will wait for the socket connection to be established. ",
    "If the connection is not built before the timeout elapses, clients will close the socket channel. ",
    "This value is the initial backoff value and will increase exponentially for each consecutive connection failure, ",
    "up to the <code>socket.connection.setup.timeout.max.ms</code> value.",
);
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS: i64 = 10 * 1000;

pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG: &str = "socket.connection.setup.timeout.max.ms";
pub const SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC: &str = concat!(
    "The maximum amount of time the client will wait for the socket connection to be established. ",
    "The connection setup timeout will increase exponentially for each consecutive connection failure up to this maximum. To avoid connection storms, ",
    "a randomization factor of 0.2 will be applied to the timeout resulting in a random range between 20% below and 20% above the computed value.",
);
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS: i64 = 30 * 1000;

pub const CONNECTIONS_MAX_IDLE_MS_CONFIG: &str = "connections.max.idle.ms";
pub const CONNECTIONS_MAX_IDLE_MS_DOC: &str =
    "Close idle connections after the number of milliseconds specified by this config.";

pub const REQUEST_TIMEOUT_MS_CONFIG: &str = "request.timeout.ms";
pub const REQUEST_TIMEOUT_MS_DOC: &str = concat!(
    "The configuration controls the maximum amount of time the client will wait ",
    "for the response of a request. If the response is not received before the timeout ",
    "elapses the client will resend the request if necessary or fail the request if ",
    "retries are exhausted.",
);

pub const DEFAULT_LIST_KEY_SERDE_INNER_CLASS: &str = "default.list.key.serde.inner";
pub const DEFAULT_LIST_KEY_SERDE_INNER_CLASS_DOC: &str = concat!(
    "Default inner class of list serde for key that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface. ",
    "This configuration will be read if and only if <code>default.key.serde</code> configuration is set to <code>org.apache.kafka.common.serialization.Serdes.ListSerde</code>",
);

pub const DEFAULT_LIST_VALUE_SERDE_INNER_CLASS: &str = "default.list.value.serde.inner";
pub const DEFAULT_LIST_VALUE_SERDE_INNER_CLASS_DOC: &str = concat!(
    "Default inner class of list serde for value that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface. ",
    "This configuration will be read if and only if <code>default.value.serde</code> configuration is set to <code>org.apache.kafka.common.serialization.Serdes.ListSerde</code>",
);

pub const DEFAULT_LIST_KEY_SERDE_TYPE_CLASS: &str = "default.list.key.serde.type";
pub const DEFAULT_LIST_KEY_SERDE_TYPE_CLASS_DOC: &str = concat!(
    "Default class for key that implements the <code>java.util.List</code> interface. ",
    "This configuration will be read if and only if <code>default.key.serde</code> configuration is set to <code>org.apache.kafka.common.serialization.Serdes.ListSerde</code> ",
    "Note when list serde class is used, one needs to set the inner serde class that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface via 'default.list.key.serde.inner'",
);

pub const DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS: &str = "default.list.value.serde.type";
pub const DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS_DOC: &str = concat!(
    "Default class for value that implements the <code>java.util.List</code> interface. ",
    "This configuration will be read if and only if <code>default.value.serde</code> configuration is set to <code>org.apache.kafka.common.serialization.Serdes.ListSerde</code> ",
    "Note when list serde class is used, one needs to set the inner serde class that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface via 'default.list.value.serde.inner'",
);

pub const GROUP_ID_CONFIG: &str = "group.id";
pub const GROUP_ID_DOC: &str = "A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the group management functionality by using <code>subscribe(topic)</code> or the Kafka-based offset management strategy.";

pub const GROUP_INSTANCE_ID_CONFIG: &str = "group.instance.id";
pub const GROUP_INSTANCE_ID_DOC: &str = concat!(
    "A unique identifier of the consumer instance provided by the end user. ",
    "Only non-empty strings are permitted. If set, the consumer is treated as a static member, ",
    "which means that only one instance with this ID is allowed in the consumer group at any time. ",
    "This can be used in combination with a larger session timeout to avoid group rebalances caused by transient unavailability ",
    "(e.g. process restarts). If not set, the consumer will join the group as a dynamic member, which is the traditional behavior.",
);

pub const MAX_POLL_INTERVAL_MS_CONFIG: &str = "max.poll.interval.ms";
pub const MAX_POLL_INTERVAL_MS_DOC: &str = concat!(
    "The maximum delay between invocations of poll() when using ",
    "consumer group management. This places an upper bound on the amount of time that the consumer can be idle ",
    "before fetching more records. If poll() is not called before expiration of this timeout, then the consumer ",
    "is considered failed and the group will rebalance in order to reassign the partitions to another member. ",
    "For consumers using a non-null <code>group.instance.id</code> which reach this timeout, partitions will not be immediately reassigned. ",
    "Instead, the consumer will stop sending heartbeats and partitions will be reassigned ",
    "after expiration of the session timeout (defined by the client config <code>session.timeout.ms</code> if using the Classic rebalance protocol, or by the broker config <code>group.consumer.session.timeout.ms</code> if using the Consumer protocol). ",
    "This mirrors the behavior of a static consumer which has shutdown.",
);

pub const REBALANCE_TIMEOUT_MS_CONFIG: &str = "rebalance.timeout.ms";
pub const REBALANCE_TIMEOUT_MS_DOC: &str = concat!(
    "The maximum allowed time for each worker to join the group ",
    "once a rebalance has begun. This is basically a limit on the amount of time needed for all tasks to ",
    "flush any pending data and commit offsets. If the timeout is exceeded, then the worker will be removed ",
    "from the group, which will cause offset commit failures.",
);

pub const SESSION_TIMEOUT_MS_CONFIG: &str = "session.timeout.ms";
pub const SESSION_TIMEOUT_MS_DOC: &str = concat!(
    "The timeout used to detect client failures when using ",
    "Kafka's group management facility. The client sends periodic heartbeats to indicate its liveness ",
    "to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, ",
    "then the broker will remove this client from the group and initiate a rebalance. Note that the value ",
    "must be in the allowable range as configured in the broker configuration by <code>group.min.session.timeout.ms</code> ",
    "and <code>group.max.session.timeout.ms</code>. Note that this client configuration is not supported when <code>group.protocol</code> ",
    "is set to \"consumer\". In that case, session timeout is controlled by the broker config <code>group.consumer.session.timeout.ms</code>.",
);

pub const HEARTBEAT_INTERVAL_MS_CONFIG: &str = "heartbeat.interval.ms";
pub const HEARTBEAT_INTERVAL_MS_DOC: &str = concat!(
    "The expected time between heartbeats to the consumer ",
    "coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the ",
    "consumer's session stays active and to facilitate rebalancing when new consumers join or leave the group. ",
    "This config is only supported if <code>group.protocol</code> is set to \"classic\". In that case, ",
    "the value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher ",
    "than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.",
    "If <code>group.protocol</code> is set to \"consumer\", this config is not supported, as ",
    "the heartbeat interval is controlled by the broker with <code>group.consumer.heartbeat.interval.ms</code>.",
);

pub const DEFAULT_API_TIMEOUT_MS_CONFIG: &str = "default.api.timeout.ms";
pub const DEFAULT_API_TIMEOUT_MS_DOC: &str = concat!(
    "Specifies the timeout (in milliseconds) for client APIs. ",
    "This configuration is used as the default timeout for all client operations that do not specify a <code>timeout</code> parameter.",
);

pub const METADATA_RECOVERY_STRATEGY_CONFIG: &str = "metadata.recovery.strategy";
pub const METADATA_RECOVERY_STRATEGY_DOC: &str = concat!(
    "Controls how the client recovers when none of the brokers known to it is available. ",
    "If set to <code>none</code>, the client fails. If set to <code>rebootstrap</code>, ",
    "the client repeats the bootstrap process using <code>bootstrap.servers</code>. ",
    "Rebootstrapping is useful when a client communicates with brokers so infrequently ",
    "that the set of brokers may change entirely before the client refreshes metadata. ",
    "Metadata recovery is triggered when all last-known brokers appear unavailable simultaneously. ",
    "Brokers appear unavailable when disconnected and no current retry attempt is in-progress. ",
    "Consider increasing <code>reconnect.backoff.ms</code> and <code>reconnect.backoff.max.ms</code> and ",
    "decreasing <code>socket.connection.setup.timeout.ms</code> and <code>socket.connection.setup.timeout.max.ms</code> ",
    "for the client. Rebootstrap is also triggered if connection cannot be established to any of the brokers for ",
    "<code>metadata.recovery.rebootstrap.trigger.ms</code> milliseconds or if server requests rebootstrap.",
);
/// Default `metadata.recovery.strategy` — the lowercase name of
/// `MetadataRecoveryStrategy::Rebootstrap`. Mirrors Java's
/// `MetadataRecoveryStrategy.REBOOTSTRAP.name`.
pub const DEFAULT_METADATA_RECOVERY_STRATEGY: &str = "rebootstrap";

pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG: &str = "metadata.recovery.rebootstrap.trigger.ms";
pub const METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC: &str = concat!(
    "If a client configured to rebootstrap using ",
    "<code>metadata.recovery.strategy=rebootstrap</code> is unable to obtain metadata from any of the brokers in the last known ",
    "metadata for this interval, client repeats the bootstrap process using <code>bootstrap.servers</code> configuration.",
);
pub const DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS: i64 = 300 * 1000;

// `SaslConfigs.SASL_MECHANISM` is a Phase-5 config; we inline the
// string literal here rather than depend on a not-yet-translated
// module. When `SaslConfigs` lands, replace this constant with the
// import.
const SASL_MECHANISM_KEY: &str = "sasl.mechanism";

/// Outcome of [`post_process_reconnect_backoff_configs`]. Mirrors
/// Java's return-an-overrides-map style: the caller (a Java
/// `AbstractConfig.postProcessParsedConfig` override) merges these
/// entries on top of the parsed config.
///
/// In the Rust translation we expose only the boolean "should override
/// reconnect.backoff.max.ms?" flag, which is the only effect the Java
/// method has. The override value is always equal to the
/// `reconnect.backoff.ms` value, so callers can compute it directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectBackoffOverrides {
    /// True when `reconnect.backoff.ms` was explicitly set in the
    /// originals but `reconnect.backoff.max.ms` was not. Java's method
    /// would return a one-entry map overriding the max to the same
    /// value as the base.
    pub override_max_to_base: bool,
}

/// Postprocess the configuration so that exponential backoff is
/// disabled when reconnect backoff is explicitly configured but the
/// maximum reconnect backoff is not explicitly configured.
///
/// Mirrors `CommonClientConfigs.postProcessReconnectBackoffConfigs`.
/// Returns whether `reconnect.backoff.max.ms` should be overridden to
/// match `reconnect.backoff.ms`, given the set of *originally provided*
/// keys.
pub fn post_process_reconnect_backoff_configs(
    originals_contains_reconnect_backoff_ms: bool,
    originals_contains_reconnect_backoff_max_ms: bool,
) -> ReconnectBackoffOverrides {
    let override_max_to_base = !originals_contains_reconnect_backoff_max_ms && originals_contains_reconnect_backoff_ms;
    if override_max_to_base {
        log::warn!(
            "Disabling exponential reconnect backoff because {RECONNECT_BACKOFF_MS_CONFIG} is set, but {RECONNECT_BACKOFF_MAX_MS_CONFIG} is not.",
        );
    }
    ReconnectBackoffOverrides { override_max_to_base }
}

/// Log warnings when the configured backoff base is greater than the
/// max, or the connection-setup timeout base is greater than the max.
/// Mirrors `CommonClientConfigs.warnDisablingExponentialBackoff`.
pub fn warn_disabling_exponential_backoff(
    retry_backoff_ms: i64,
    retry_backoff_max_ms: i64,
    connection_setup_timeout_ms: i64,
    connection_setup_timeout_max_ms: i64,
) {
    if retry_backoff_ms > retry_backoff_max_ms {
        log::warn!(
            "Configuration '{RETRY_BACKOFF_MS_CONFIG}' with value '{retry_backoff_ms}' is greater than configuration '{RETRY_BACKOFF_MAX_MS_CONFIG}' with value '{retry_backoff_max_ms}'. A static backoff with value '{retry_backoff_max_ms}' will be applied.",
        );
    }

    if connection_setup_timeout_ms > connection_setup_timeout_max_ms {
        log::warn!(
            "Configuration '{SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG}' with value '{connection_setup_timeout_ms}' is greater than configuration '{SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG}' with value '{connection_setup_timeout_max_ms}'. A static connection setup timeout with value '{connection_setup_timeout_max_ms}' will be applied.",
        );
    }
}

/// Validate the SASL mechanism config. Mirrors
/// `CommonClientConfigs.postValidateSaslMechanismConfig`.
///
/// `security_protocol` is the parsed `security.protocol` config value
/// (e.g. `"SASL_PLAINTEXT"`, `"SASL_SSL"`, `"PLAINTEXT"`,
/// `"SSL"`). When the protocol is one of the SASL-bearing variants,
/// `sasl_mechanism` must be non-empty; otherwise a
/// `KafkaError::Config` is returned (Java `ConfigException`).
pub fn post_validate_sasl_mechanism_config(
    security_protocol: &str,
    sasl_mechanism: Option<&str>,
) -> Result<(), KafkaError> {
    let is_sasl = security_protocol == "SASL_PLAINTEXT" || security_protocol == "SASL_SSL";
    if !is_sasl {
        return Ok(());
    }
    let mechanism = sasl_mechanism.unwrap_or("");
    if mechanism.is_empty() {
        return Err(KafkaError::Config(format!(
            "Invalid value null for configuration {SASL_MECHANISM_KEY}: When the {SECURITY_PROTOCOL_CONFIG} configuration enables SASL, mechanism must be non-null and non-empty string.",
        )));
    }
    Ok(())
}

// Compile-time invariant: the Rebootstrap variant's name() must equal
// `DEFAULT_METADATA_RECOVERY_STRATEGY`.
const _: () = {
    // const-eval in Rust 2024 is sufficient to compare &str equality.
    let a = DEFAULT_METADATA_RECOVERY_STRATEGY.as_bytes();
    let b = b"rebootstrap";
    assert!(a.len() == b.len());
    let mut i = 0;
    while i < a.len() {
        assert!(a[i] == b[i]);
        i += 1;
    }
};

// Java test mapping (kafka/clients/.../CommonClientConfigsTest.java):
//
// * `testExponentialBackoffDefaults` (lines 91-115) — Java exercises
//   `AbstractConfig` defaults plus `postProcessReconnectBackoffConfigs`
//   wired through `postProcessParsedConfig`. The `AbstractConfig` /
//   `ConfigDef` types are Phase-5 territory. The pure backoff logic
//   that test exercises is covered here by
//   `post_process_reconnect_backoff_overrides_max_when_only_base_set`,
//   `post_process_reconnect_backoff_no_override_when_both_set`,
//   `post_process_reconnect_backoff_no_override_when_neither_set`, and
//   `post_process_reconnect_backoff_no_override_when_only_max_set`.
//   Re-translate the full integration test in Phase 5 once
//   `AbstractConfig` lands.
//
// * `testInvalidSaslMechanism` (lines 117-128) — Java exercises
//   `postValidateSaslMechanismConfig` through `AbstractConfig`. The
//   pure validation logic is covered here by
//   `post_validate_sasl_requires_mechanism_for_sasl_plaintext`,
//   `post_validate_sasl_requires_mechanism_for_sasl_ssl`,
//   `post_validate_sasl_ok_for_plaintext`, and
//   `post_validate_sasl_accepts_non_empty_mechanism`. Re-translate
//   the integration test in Phase 5 once `AbstractConfig` lands.
//
// * `testMetricsReporters` (lines 130-151) — DEFERRED to Phase 5.
//   Java calls `CommonClientConfigs.metricsReporters(String, AbstractConfig)`
//   which instantiates `MetricsReporter` plugin classes from the
//   `metric.reporters` config. Both the `metricsReporters` static
//   method and the `MetricsReporter` trait it depends on are not
//   translated in Phase 4c (they are Phase 5/6). When the trait
//   lands, this test re-translates as: with `metric.reporters=`
//   defaulting to `JmxReporter`, the helper returns 1 reporter; with
//   it set to "", it returns 0; with it set to `JmxReporter`, it
//   returns 1; with it set to `JmxReporter,MyJmxReporter`, it
//   returns 2.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_recovery_strategy::MetadataRecoveryStrategy;

    // Java reference: `DEFAULT_METADATA_RECOVERY_STRATEGY` reads the
    // `name` field on `MetadataRecoveryStrategy.REBOOTSTRAP`. The const-
    // eval block above verifies the literal at compile time; this test
    // verifies the same equality through the Rust enum.
    #[test]
    fn default_metadata_recovery_strategy_matches_enum() {
        assert_eq!(DEFAULT_METADATA_RECOVERY_STRATEGY, MetadataRecoveryStrategy::Rebootstrap.name(),);
    }

    #[test]
    fn retry_backoff_constants() {
        assert_eq!(DEFAULT_RETRY_BACKOFF_MS, 100);
        assert_eq!(DEFAULT_RETRY_BACKOFF_MAX_MS, 1000);
        assert_eq!(RETRY_BACKOFF_EXP_BASE, 2);
        assert!((RETRY_BACKOFF_JITTER - 0.2).abs() < f64::EPSILON);
    }

    #[test]
    fn socket_connection_setup_timeout_defaults() {
        assert_eq!(DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS, 10_000);
        assert_eq!(DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS, 30_000);
    }

    #[test]
    fn metadata_recovery_rebootstrap_default() {
        assert_eq!(DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS, 300_000);
    }

    #[test]
    fn post_process_reconnect_backoff_overrides_max_when_only_base_set() {
        let r = post_process_reconnect_backoff_configs(true, false);
        assert!(r.override_max_to_base);
    }

    #[test]
    fn post_process_reconnect_backoff_no_override_when_both_set() {
        let r = post_process_reconnect_backoff_configs(true, true);
        assert!(!r.override_max_to_base);
    }

    #[test]
    fn post_process_reconnect_backoff_no_override_when_neither_set() {
        let r = post_process_reconnect_backoff_configs(false, false);
        assert!(!r.override_max_to_base);
    }

    #[test]
    fn post_process_reconnect_backoff_no_override_when_only_max_set() {
        let r = post_process_reconnect_backoff_configs(false, true);
        assert!(!r.override_max_to_base);
    }

    #[test]
    fn post_validate_sasl_ok_for_plaintext() {
        post_validate_sasl_mechanism_config("PLAINTEXT", None).unwrap();
        post_validate_sasl_mechanism_config("SSL", Some("")).unwrap();
    }

    #[test]
    fn post_validate_sasl_requires_mechanism_for_sasl_plaintext() {
        let err = post_validate_sasl_mechanism_config("SASL_PLAINTEXT", None).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.to_string().contains(SASL_MECHANISM_KEY));

        let err = post_validate_sasl_mechanism_config("SASL_PLAINTEXT", Some("")).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
        assert!(err.to_string().contains(SASL_MECHANISM_KEY));
    }

    #[test]
    fn post_validate_sasl_requires_mechanism_for_sasl_ssl() {
        let err = post_validate_sasl_mechanism_config("SASL_SSL", None).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));

        let err = post_validate_sasl_mechanism_config("SASL_SSL", Some("")).unwrap_err();
        assert!(matches!(err, KafkaError::Config(_)));
    }

    #[test]
    fn post_validate_sasl_accepts_non_empty_mechanism() {
        post_validate_sasl_mechanism_config("SASL_PLAINTEXT", Some("PLAIN")).unwrap();
        post_validate_sasl_mechanism_config("SASL_SSL", Some("SCRAM-SHA-256")).unwrap();
    }
}
