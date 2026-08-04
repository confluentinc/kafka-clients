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

//! Shared integration-test assertion helpers.
//!
//! Translated from `kafka.utils.TestUtils` (Scala) and
//! `org.apache.kafka.test.TestUtils` (Java). Only the polling helpers the Rust
//! integration tests need are translated; the rest of Java's `TestUtils` is
//! covered by the per-test harness in this module (`TestContext`,
//! `ClusterConfig`, ...).

use std::future::Future;
use std::time::{Duration, Instant};

use confluent_kafka::admin::{Admin, CreateTopicsOptions, DescribeTopicsOptions, NewTopic};
use confluent_kafka::common::TopicCollection;

/// Default maximum time to wait for a condition.
///
/// Mirrors `org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS`.
pub const DEFAULT_MAX_WAIT_MS: u64 = 15_000;

/// Default pause between condition evaluations.
///
/// Mirrors the `pause` default of Scala `TestUtils.waitUntilTrue` (which is
/// `100L`, matching `org.apache.kafka.test.TestUtils.DEFAULT_POLL_INTERVAL_MS`).
pub const DEFAULT_PAUSE_MS: u64 = 100;

/// Waits until `condition` returns `true`, panicking with `msg` if it has not
/// become true within [`DEFAULT_MAX_WAIT_MS`].
///
/// Mirrors `TestUtils.waitUntilTrue(condition, msg)`. Java's `condition` is a
/// synchronous `() => Boolean`; here it is an async closure so callers can
/// `await` an admin/consumer RPC inside it.
pub async fn wait_until_true<F, Fut>(condition: F, msg: &str)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    wait_until_true_with_timeout(condition, msg, DEFAULT_MAX_WAIT_MS, DEFAULT_PAUSE_MS).await;
}

/// Waits until `condition` returns `true`, panicking with `msg` if it has not
/// become true within `wait_time_ms`, polling every `pause` milliseconds.
///
/// Mirrors `TestUtils.waitUntilTrue(condition, msg, waitTimeMs, pause)`,
/// including the `waitTimeMs.min(pause)` sleep interval.
pub async fn wait_until_true_with_timeout<F, Fut>(mut condition: F, msg: &str, wait_time_ms: u64, pause: u64)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start_time = Instant::now();
    loop {
        if condition().await {
            return;
        }
        // Java: `if (System.currentTimeMillis() > startTime + waitTimeMs) fail(msg)`.
        // A failed assertion is a panic in Rust, as with `assert!`.
        if start_time.elapsed() > Duration::from_millis(wait_time_ms) {
            panic!("{msg}");
        }
        tokio::time::sleep(Duration::from_millis(wait_time_ms.min(pause))).await;
    }
}

/// Maximum time to wait for topic metadata to reach every broker.
///
/// Mirrors the explicit `waitTimeMs = 60000L` that
/// `TestUtils.waitForAllPartitionsMetadata` passes (not the 15s default).
pub const TOPIC_METADATA_PROPAGATION_WAIT_MS: u64 = 60_000;

/// Returns the partition count for `topic`, or `None` if the broker being
/// queried does not (yet) know the topic.
///
/// A freshly created topic is not immediately visible on every broker, so
/// `describe_topics` can legitimately answer `UnknownTopicOrPartition` for a
/// short window after `create_topics` returns. That is "not propagated yet",
/// not a failure — hence `Option` rather than an error.
pub async fn try_partition_count(admin: &dyn Admin, topic: &str) -> Option<usize> {
    admin
        .describe_topics(
            TopicCollection::of_topic_names(vec![topic.to_string()]),
            DescribeTopicsOptions::new(),
        )
        .all_topic_names()
        .expect("described by name")
        .get()
        .await
        .ok()
        .and_then(|described| described.get(topic).map(|description| description.partitions().len()))
}

/// Waits until `topic` is reported as having exactly `expected_num_partitions`
/// partitions.
///
/// Mirrors `TestUtils.waitForAllPartitionsMetadata(brokers, topic,
/// expectedNumPartitions)`, including its 60s bound and failure message. Java
/// inspects each broker's `metadataCache` directly; the client-observable
/// equivalent over the wire is `describe_topics` reporting the count.
pub async fn wait_for_all_partitions_metadata(admin: &dyn Admin, topic: &str, expected_num_partitions: usize) {
    wait_until_true_with_timeout(
        || async { try_partition_count(admin, topic).await == Some(expected_num_partitions) },
        &format!("Topic [{topic}] metadata not propagated after 60000 ms"),
        TOPIC_METADATA_PROPAGATION_WAIT_MS,
        DEFAULT_PAUSE_MS,
    )
    .await;
}

/// Creates `topic` and does not return until its metadata has propagated.
///
/// Mirrors `TestUtils.createTopicWithAdmin`, which calls
/// `waitForAllPartitionsMetadata` before returning. That wait is the whole point
/// of the helper: `create_topics` completes as soon as the controller accepts the
/// request, so a describe issued immediately afterwards can still be answered
/// `UnknownTopicOrPartition` by a broker that has not caught up. Tests that
/// create a topic and then immediately assert on it MUST go through here.
pub async fn create_topic(admin: &dyn Admin, topic: &str, num_partitions: i32, replication_factor: i16) {
    admin
        .create_topics(
            &[NewTopic::new(topic.to_string(), num_partitions, replication_factor)],
            CreateTopicsOptions::new(),
        )
        .all()
        .get()
        .await
        .expect("create topic");

    wait_for_all_partitions_metadata(admin, topic, num_partitions as usize).await;
}
