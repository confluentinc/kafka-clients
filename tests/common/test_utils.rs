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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};

use confluent_kafka::admin::{
    Admin, AdminClientConfig, CreateTopicsOptions, DescribeTopicsOptions, KafkaAdminClient, NewTopic,
};
use confluent_kafka::common::TopicCollection;

use super::test_context::TestContext;

/// Default maximum time to wait for a condition.
///
/// Mirrors `org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS`.
pub const DEFAULT_MAX_WAIT_MS: u64 = 15_000;

/// How long [`wait_for_all_partitions_metadata_with_context`] gives its admin
/// client to shut down. Generous: it has no in-flight work beyond the describes
/// it just made, so this only bounds a pathological hang.
const ADMIN_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default pause between condition evaluations.
///
/// Mirrors the `pause` default of Scala `TestUtils.waitUntilTrue` (which is
/// `100L`, matching `org.apache.kafka.test.TestUtils.DEFAULT_POLL_INTERVAL_MS`).
pub const DEFAULT_PAUSE_MS: u64 = 100;

/// Waits until `condition` returns `true`, panicking with `msg` if it has not
/// become true within `wait_time_ms`, polling every `pause` milliseconds.
///
/// Mirrors `TestUtils.waitUntilTrue(condition, msg, waitTimeMs, pause)`,
/// including the `waitTimeMs.min(pause)` sleep interval. Java's `condition` is a
/// synchronous `() => Boolean`; here it is an async closure so callers can
/// `await` an RPC inside it.
///
/// Java's two-argument overload (defaulting to [`DEFAULT_MAX_WAIT_MS`] /
/// [`DEFAULT_PAUSE_MS`]) is not translated because no caller needs it — the
/// propagation waits here pass an explicit bound. Prefer
/// [`retry_on_error_with_timeout`] for read-back *assertions*: it keeps the
/// assertion as the source of truth and reports the last failure, whereas this
/// helper can only report `msg`.
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

/// Retries `body` until it succeeds, or panics with the last failure once
/// `timeout` has elapsed.
///
/// Mirrors `org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout(
/// timeoutMs, pollIntervalMs, runnable)`: re-run the whole
/// read-back-and-assert block until it stops failing, and on timeout surface
/// the *last* failure so the assertion message provides the context. Java uses
/// this shape around quota read-back assertions in
/// `ClientQuotasRequestTest` (e.g. `verifyIpQuotas`,
/// `testDescribeClientQuotasMatchExact`), which is the closest counterpart to
/// the admin read-back assertions here.
///
/// **Deviation from Java, deliberate:** Java's `runnable` signals failure by
/// throwing `AssertionError`, which the helper catches. The Rust equivalent
/// would be catching a panic from `assert!`, but doing that around an `async`
/// body means `AssertUnwindSafe` over a future and retrying after a unwind that
/// may have poisoned a `std::sync::Mutex` — and this crate deliberately relies
/// on mutex poisoning (`consumer-threading.md` §16), so a caught-and-retried
/// panic could retry against poisoned state. It would also print a panic per
/// attempt. So `body` returns `Result<(), String>` instead: `Err` is the
/// retryable failure and carries the message Java's assertion would have. The
/// retry/timeout/last-failure semantics are identical.
///
/// Java's `NoRetryException` short-circuit has no caller here and is not
/// translated; add it if a caller needs to abort early.
pub async fn retry_on_error_with_timeout<F, Fut>(timeout: Duration, body: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    retry_on_error_with_timeout_poll(timeout, Duration::from_millis(DEFAULT_PAUSE_MS), body).await;
}

/// [`retry_on_error_with_timeout`] with an explicit poll interval.
///
/// Mirrors the three-argument Java overload, including its
/// `Math.min(pollIntervalMs, timeoutMs)` sleep.
pub async fn retry_on_error_with_timeout_poll<F, Fut>(timeout: Duration, poll_interval: Duration, mut body: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let expected_end = Instant::now() + timeout;
    loop {
        match body().await {
            Ok(()) => return,
            Err(failure) => {
                // Java: `if (expectedEnd <= System.currentTimeMillis()) throw t;`
                if Instant::now() >= expected_end {
                    panic!("Assertion failed after {}ms: {failure}", timeout.as_millis());
                }
            },
        }
        tokio::time::sleep(poll_interval.min(timeout)).await;
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
/// `describe_topics_with_topics` can legitimately answer `UnknownTopicOrPartition` for a
/// short window after `create_topics` returns. That is "not propagated yet",
/// not a failure — hence `Option` rather than an error.
pub async fn try_partition_count(admin: &dyn Admin, topic: &str) -> Option<usize> {
    admin
        .describe_topics_with_topics_options(
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
/// equivalent over the wire is `describe_topics_with_topics` reporting the count.
pub async fn wait_for_all_partitions_metadata(admin: &dyn Admin, topic: &str, expected_num_partitions: usize) {
    wait_until_true_with_timeout(
        || async { try_partition_count(admin, topic).await == Some(expected_num_partitions) },
        &format!("Topic [{topic}] metadata not propagated after 60000 ms"),
        TOPIC_METADATA_PROPAGATION_WAIT_MS,
        DEFAULT_PAUSE_MS,
    )
    .await;
}

/// [`wait_for_all_partitions_metadata`] with the admin client managed for you:
/// built from `ctx`'s bootstrap servers and closed before returning.
///
/// Prefer this over the raw form. Callers that only need the wait were each
/// repeating the same config block and a `close`, which is ceremony that can be
/// forgotten — a leaked admin client keeps a background task and its connections
/// alive for the rest of the test binary.
///
/// Use the raw [`wait_for_all_partitions_metadata`] only when you already hold an
/// admin client for other work (as [`create_topic`] does), so it is not built and
/// torn down twice.
pub async fn wait_for_all_partitions_metadata_with_context(
    ctx: &TestContext,
    topic: &str,
    expected_num_partitions: usize,
) {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("client.id".to_string(), "test-utils-metadata-wait".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    let config = AdminClientConfig::new(&props).expect("valid admin config");
    let admin: Box<dyn Admin> = Box::new(KafkaAdminClient::new(config).expect("admin client"));
    wait_for_all_partitions_metadata(admin.as_ref(), topic, expected_num_partitions).await;
    admin.close_with_timeout(ADMIN_CLOSE_TIMEOUT).await;
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
    create_topic_with_configs(admin, topic, num_partitions, replication_factor, BTreeMap::new()).await;
}

/// [`create_topic`] with topic-level configs — the `topicConfig` parameter of
/// Java's `TestUtils.createTopicWithAdmin` (`TestUtils.scala:832-853`). An
/// empty map sends no configs, exactly like [`create_topic`].
pub async fn create_topic_with_configs(
    admin: &dyn Admin,
    topic: &str,
    num_partitions: i32,
    replication_factor: i16,
    configs: BTreeMap<String, String>,
) {
    let mut new_topic = NewTopic::with_num_partitions_replication_factor(
        topic.to_string(),
        Some(num_partitions),
        Some(replication_factor),
    );
    if !configs.is_empty() {
        new_topic = new_topic.set_configs(configs);
    }
    admin
        .create_topics_with_options(&[new_topic], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");

    wait_for_all_partitions_metadata(admin, topic, num_partitions as usize).await;
}
