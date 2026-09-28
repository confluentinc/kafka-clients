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

//! Cluster configuration descriptor for integration tests.
//!
//! Describes the shape of a Kafka cluster that a group of tests needs.
//! Tests with identical `ClusterConfig` share one container, keeping
//! Docker container startup overhead amortized across many tests.

use std::collections::BTreeMap;

/// Describes cluster requirements for a group of tests.
///
/// Tests with identical `ClusterConfig` share one container.
/// The `Hash` and `Eq` implementations ensure that identical
/// configurations map to the same pool entry.
///
/// Every container exposes all four security protocols
/// (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL), so tests choose
/// which listener to connect to rather than requesting a specific
/// security mode.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct ClusterConfig {
    /// Number of brokers (default: 1).
    pub brokers: u16,
    /// Extra server properties (key=value pairs set as environment variables).
    ///
    /// `BTreeMap` is used instead of `HashMap` so that `Hash` is deterministic.
    pub server_properties: BTreeMap<String, String>,
}

impl ClusterConfig {
    /// Multi-broker cluster with no extra properties.
    pub fn with_brokers(brokers: u16) -> Self {
        Self { brokers, server_properties: BTreeMap::new() }
    }

    /// Single broker with custom server properties.
    pub fn with_properties(props: BTreeMap<String, String>) -> Self {
        Self { brokers: 1, server_properties: props }
    }
}

impl Default for ClusterConfig {
    /// Default single-broker cluster with no extra properties.
    fn default() -> Self {
        Self { brokers: 1, server_properties: BTreeMap::new() }
    }
}

/// Canonical KIP-848 3-broker cluster config shared across the consumer
/// integration suites.
///
/// The `server_properties` here are the *superset* of the broker tuning knobs
/// that the individual `PlaintextConsumer*Test` suites used to set in their own
/// private config helpers. All of these are harmless to apply universally:
///
/// - `GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer` enables KIP-848.
/// - `OFFSETS_TOPIC_REPLICATION_FACTOR=3` / `OFFSETS_TOPIC_NUM_PARTITIONS=1`
///   shape the internal offsets topic.
/// - `GROUP_MIN_SESSION_TIMEOUT_MS=100` / `GROUP_MAX_SESSION_TIMEOUT_MS=60000`
///   only *widen* the accepted session-timeout window.
/// - `GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS=500` /
///   `GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS=500` only *speed up* the
///   KIP-848 heartbeat round-trip so rebalances settle fast.
/// - `GROUP_INITIAL_REBALANCE_DELAY_MS=10` speeds the first rebalance.
///
/// The only behaviorally-observable difference between suites is
/// `NUM_PARTITIONS` (auto-created topics get this many partitions, since the
/// harness has no admin client — see [`super::test_context`]). It is therefore
/// the single parameter: suites that mirror Java's `createTopic(name, 2, ...)`
/// pass `2`, the single-partition suites pass `1`.
///
/// Routing every compatible suite through this one helper keeps the cluster
/// pool ([`super::cluster_pool`]) keyed on a single `ClusterConfig` per distinct
/// `num_partitions`, so they all share one container instead of each starting
/// its own.
/// Single-broker cluster with the standard KRaft authorizer enabled, used by
/// the ACL admin RPC integration tests (finding #11).
///
/// `KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer`
/// turns on ACL enforcement; `KAFKA_SUPER_USERS=User:ANONYMOUS` grants the
/// test client (which connects over the PLAINTEXT listener as the anonymous
/// principal) blanket access so it is never locked out of managing ACLs.
pub fn authorizer_single_broker() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_AUTHORIZER_CLASS_NAME".to_string(),
        "org.apache.kafka.metadata.authorizer.StandardAuthorizer".to_string(),
    );
    props.insert("KAFKA_SUPER_USERS".to_string(), "User:ANONYMOUS".to_string());
    ClusterConfig::with_properties(props)
}

/// Single-broker cluster on which a **real ACL denial is reachable**: the
/// `StandardAuthorizer` is enabled, `User:ANONYMOUS` is deliberately *not* a super
/// user, and everything is allowed when no ACL matches.
///
/// # Why the third property is required, not a convenience
///
/// Dropping `allow.everyone.if.no.acl.found` (so it takes Kafka's default
/// `false`) does not merely lock the test client out — **the broker never starts
/// at all**. Measured: the container's own `CONTROLLER_REGISTRATION` and
/// `BROKER_REGISTRATION` requests are refused and the node shuts down after the
/// 90 s readiness wait, because this fixture maps the `CONTROLLER` and `BROKER`
/// listeners to PLAINTEXT (`kafka_cluster.rs`'s
/// `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`), so the broker authenticates to *itself*
/// as `User:ANONYMOUS` — the very principal the test client uses. Super-user
/// status for the broker and for the test client is one bit here, and separating
/// them would need an authenticated inter-broker listener.
///
/// With the implicit allow in place the broker boots, the client can still manage
/// ACLs and topics, and an **explicit DENY** for `User:ANONYMOUS` still binds —
/// `StandardAuthorizer` gives a matching DENY precedence over the implicit allow.
/// That is what makes a genuine, live-authorizer denial observable, which
/// `PLAN-multilanguage-admin.md` §D3 recorded as unreachable on the assumption
/// that `User:ANONYMOUS` had to be a super user.
///
/// Kept separate from [`authorizer_single_broker`] rather than replacing it: the
/// ACL round-trip scenarios need a principal that may freely manage ACLs *and*
/// read every topic, and this fixture's DENY rules would interfere with them.
pub fn authorizer_deny_reachable_single_broker() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_AUTHORIZER_CLASS_NAME".to_string(),
        "org.apache.kafka.metadata.authorizer.StandardAuthorizer".to_string(),
    );
    // Any principal that is not the one the test client authenticates as.
    props.insert("KAFKA_SUPER_USERS".to_string(), "User:nobody".to_string());
    props.insert("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND".to_string(), "true".to_string());
    ClusterConfig::with_properties(props)
}

pub fn kip848_3_broker(num_partitions: u16) -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer".to_string(),
    );
    props.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "3".to_string());
    props.insert("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS".to_string(), "1".to_string());
    props.insert("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS".to_string(), "100".to_string());
    props.insert("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS".to_string(), "60000".to_string());
    props.insert("KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS".to_string(), "500".to_string());
    props.insert("KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS".to_string(), "500".to_string());
    props.insert("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS".to_string(), "10".to_string());
    props.insert("KAFKA_NUM_PARTITIONS".to_string(), num_partitions.to_string());
    let mut cfg = ClusterConfig::with_brokers(3);
    cfg.server_properties = props;
    cfg
}
