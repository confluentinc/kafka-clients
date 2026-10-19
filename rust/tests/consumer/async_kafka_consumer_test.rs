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

//! Unit-level smoke tests for the production `AsyncKafkaConsumer` ctor
//! reachable via the `KafkaConsumer::new` factory (Phase 12 commit (4/N)).
//!
//! These tests deliberately do NOT require a live broker — they target
//! a closed localhost port. The Kafka client's metadata bootstrap path
//! does not synchronously connect, so the ctor succeeds even when the
//! peer refuses connection. This exercises:
//!
//! 1. The `KafkaConsumer::<K, V>::new(...)` factory wires the
//!    `GroupProtocol::Consumer` arm through to
//!    `AsyncKafkaConsumer::new(config, kd, vd)`.
//! 2. The full RequestManagers + bg-task scaffold builds and spawns
//!    without panic.
//! 3. `Consumer` dispatch works for sync accessors (`assignment`), both on
//!    the concrete `KafkaConsumer` and boxed as
//!    `Box<dyn DynConsumer<K, V>>`.
//! 4. `close().await` through `Box<dyn DynConsumer<K, V>>` cleanly tears
//!    down the bg task with no broker.
//!
//! Docker-free coverage; the integration tests in
//! `tests/integration/consumer_test.rs` exercise the same path against
//! a real broker.

use std::collections::HashMap;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::KafkaConsumer;

/// Local string deserializer for tests — `tests/integration` uses a
/// similar inline impl pending a shared `StringDeserializer` in
/// `common::serialization`. Returns the input bytes as a UTF-8 `String`.
struct StringDeserializer;

impl Deserializer<String> for StringDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, Error> {
        String::from_utf8(data.to_vec()).map_err(|e| Error::serialization(format!("invalid utf-8: {}", e)))
    }
}

/// Build a `ConsumerConfig` pointing at a closed-on-purpose localhost
/// port. `127.0.0.1:1` is reliably-refused on every CI runner.
fn make_smoke_config() -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), "127.0.0.1:1".to_string()),
        ("group.id".to_string(), "smoke-test-group".to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("client.id".to_string(), "smoke-test-consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    ConsumerConfig::new(&props).expect("smoke-test config should validate")
}

/// Smoke: the production ctor succeeds against a refuses-connection
/// broker (metadata.bootstrap is non-connecting), trait dispatch works
/// for a sync accessor on the concrete type and through `DynConsumer`, and
/// `close().await` through `DynConsumer` cleanly tears down the bg task.
/// Covers commit (4/N) of Phase 12.
#[tokio::test(flavor = "multi_thread")]
async fn new_consumer_builds_and_closes_against_refused_broker() {
    let consumer = KafkaConsumer::<String, String>::new(
        make_smoke_config(),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("KafkaConsumer::new should succeed even without a reachable broker");

    // Static trait dispatch on the concrete type: a fresh consumer has no
    // assignment.
    assert!(consumer.assignment().is_empty(), "a new consumer has no assignment");

    // Dynamic dispatch through `Box<dyn DynConsumer<K, V>>`: the same
    // production consumer behind the type-erased view.
    let mut consumer: Box<dyn confluent_kafka::consumer::DynConsumer<String, String>> = Box::new(consumer);
    assert!(
        consumer.assignment().is_empty(),
        "assignment() through DynConsumer should match the concrete call",
    );

    // Bg-task graceful shutdown: signal close + drop the join handle.
    // Should NOT hang even though we never reached a broker.
    consumer
        .close()
        .await
        .expect("close should succeed against a never-connected broker");
}

/// Smoke: `GroupProtocol::Classic` continues to return
/// `Error::unsupported_version` after the Phase-12 factory swap.
/// Regression guard for the classic-protocol gate at
/// `src/consumer/mod.rs`.
#[tokio::test(flavor = "multi_thread")]
async fn new_consumer_rejects_classic_group_protocol() {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), "127.0.0.1:1".to_string()),
        ("group.protocol".to_string(), "classic".to_string()),
    ]);
    let config = ConsumerConfig::new(&props).expect("config should validate");

    let result =
        KafkaConsumer::<String, String>::new(config, Box::new(StringDeserializer), Box::new(StringDeserializer));

    let err = result.err().expect("classic protocol must be rejected");
    assert!(
        matches!(err, Error::UnsupportedVersion(_)),
        "expected UnsupportedVersion, got: {:?}",
        err
    );
}

/// Translated from `KafkaConsumerTest.testPauseFlagPreservedForRetainedPartitionOnManualAssignmentChange`
/// (36aab4fddd), CONSUMER arm (the CLASSIC arm is out of scope,
/// consumer-threading.md §20): changing a manual assignment keeps the pause
/// state of a partition that stays assigned, and a re-added partition starts
/// unpaused. No broker is needed: `assign` / `pause` are applied by the
/// background task locally.
#[tokio::test(flavor = "multi_thread")]
#[doc(
    alias = "org.apache.kafka.clients.consumer.KafkaConsumerTest#testPauseFlagPreservedForRetainedPartitionOnManualAssignmentChange"
)]
async fn test_pause_flag_preserved_for_retained_partition_on_manual_assignment_change() {
    use confluent_kafka::common::TopicPartition;
    use std::collections::HashSet;

    let mut consumer = KafkaConsumer::<String, String>::new(
        make_smoke_config(),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("KafkaConsumer::new");
    let tp0 = TopicPartition::new("test", 0);
    let tp1 = TopicPartition::new("test", 1);

    // Manually assign two partitions and pause one of them.
    consumer.assign(vec![tp0.clone(), tp1.clone()]).await.unwrap();
    consumer.pause(std::slice::from_ref(&tp0)).await.unwrap();
    assert_eq!(HashSet::from([tp0.clone()]), consumer.paused());

    // Change the assignment while keeping tp0 assigned.
    consumer.assign(vec![tp0.clone()]).await.unwrap();
    assert_eq!(HashSet::from([tp0.clone()]), consumer.assignment());
    assert_eq!(
        HashSet::from([tp0.clone()]),
        consumer.paused(),
        "The pause state of the partition that remains assigned must be preserved"
    );

    // A partition that is (re)added to the assignment starts unpaused.
    consumer.assign(vec![tp0.clone(), tp1]).await.unwrap();
    assert_eq!(HashSet::from([tp0]), consumer.paused());

    consumer.close().await.expect("close");
}

/// Asserts the shape of `consumer.metrics()`: the `kafka-metrics-count`
/// meta-metric is always there, and the rest is non-empty before `close()`
/// and empty after it. Translates the `assertMetricsMap` helper of
/// `KafkaConsumerTest` (KAFKA-19542, 9a28bd23ad), messages included.
fn assert_metrics_map(
    consumer: &dyn confluent_kafka::consumer::DynConsumer<String, String>,
    metrics_should_be_present: bool,
) {
    // Copy the map because we're going to modify it.
    let mut metrics = consumer.metrics();

    // There's a meta-metric named "count" that is automatically added to the metrics map.
    let count_metric_name = metrics
        .keys()
        .find(|metric_name| metric_name.name() == "count" && metric_name.group() == "kafka-metrics-count")
        .cloned();

    // Make sure the meta-metric is present and has an entry.
    let count_metric_name =
        count_metric_name.expect("The \"count\" meta-metric was unexpectedly missing from the Consumer metrics");
    assert!(
        metrics.remove(&count_metric_name).is_some(),
        "The \"count\" meta-metric key was removed from the Consumer metrics map, but it unexpectedly had no entry"
    );

    if metrics_should_be_present {
        assert!(
            !metrics.is_empty(),
            "The consumer should have created metrics, but they are unexpectedly empty"
        );
    } else {
        let expected: Vec<String> = Vec::new();
        let mut actual: Vec<String> = metrics
            .keys()
            .map(|metric_name| format!("{}:{}", metric_name.group(), metric_name.name()))
            .collect();
        actual.sort();
        assert_eq!(
            expected, actual,
            "The consumer should have removed its metrics on close(), but there are metrics remaining"
        );
    }
}

/// Translated from `KafkaConsumerTest.testMetricsRemovedOnClose` (KAFKA-19542,
/// 9a28bd23ad), CONSUMER arm (the CLASSIC arm is out of scope,
/// consumer-threading.md §20): after `close()` the only metric left in
/// `metrics()` is the `kafka-metrics-count` meta-metric. Like Java, the
/// consumer has no `group.id`, so no group request manager is built.
#[tokio::test(flavor = "multi_thread")]
#[doc(alias = "org.apache.kafka.clients.consumer.KafkaConsumerTest#testMetricsRemovedOnClose")]
async fn test_metrics_removed_on_close() {
    use confluent_kafka::consumer::CloseOptions;
    use std::time::Duration;

    let props = HashMap::from([
        ("group.protocol".to_string(), "consumer".to_string()),
        ("bootstrap.servers".to_string(), "localhost:9999".to_string()),
    ]);
    let mut consumer = KafkaConsumer::<String, String>::new(
        ConsumerConfig::new(&props).expect("config should validate"),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("KafkaConsumer::new");

    assert_metrics_map(&consumer, true);
    consumer
        .close_with_options(CloseOptions::new_with_timeout(Duration::ZERO))
        .await
        .expect("close");
    assert_metrics_map(&consumer, false);
}

/// Translated from
/// `KafkaConsumerTest.testConsumerBootstrapResolutionExceptionPropagatedToPoll`
/// (KIP-909), for `GroupProtocol.CONSUMER`; the `CLASSIC` case is out of
/// scope (consumer-threading.md §20: `KafkaConsumer::new` rejects it before
/// any bootstrap work). With a positive `bootstrap.resolve.timeout.ms` and
/// an unresolvable host the consumer is created; `poll` fails with the
/// `BootstrapResolutionException` once the budget runs out, and so does
/// every later call.
#[tokio::test(flavor = "multi_thread")]
#[doc(
    alias = "org.apache.kafka.clients.consumer.KafkaConsumerTest#testConsumerBootstrapResolutionExceptionPropagatedToPoll"
)]
async fn test_consumer_bootstrap_resolution_error_propagated_to_poll() {
    // Use an invalid hostname that will fail DNS resolution (using RFC 6761 reserved .invalid TLD)
    let props = HashMap::from([
        (
            ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            "unresolvable.invalid:9092".to_string(),
        ),
        // Set a short bootstrap timeout so the test doesn't take too long
        (
            ConsumerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG.to_string(),
            "3000".to_string(),
        ),
        (ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(), "consumer".to_string()),
        (ConsumerConfig::GROUP_ID_CONFIG.to_string(), "test-group".to_string()),
    ]);
    let mut consumer = KafkaConsumer::<String, String>::new(
        ConsumerConfig::new(&props).unwrap(),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("a positive timeout defers resolution, so construction succeeds");

    // Subscribe to a topic to trigger metadata fetch
    consumer.subscribe_with_topics(vec!["test-topic".to_string()]).await.unwrap();

    // Poll continuously until we get the BootstrapResolutionException
    // The error should be returned after bootstrap.resolve.timeout.ms expires
    let start = std::time::Instant::now();
    let max_wait = std::time::Duration::from_millis(15000); // 15 seconds max to prevent test hanging
    let error = loop {
        assert!(
            start.elapsed() < max_wait,
            "Expected the BootstrapResolutionError within {}ms",
            max_wait.as_millis()
        );
        if let Err(error) = consumer.poll(std::time::Duration::from_millis(100)).await
            && error.is_bootstrap_resolution_error()
        {
            break error;
        }
    };

    // Verify the error message contains information about DNS resolution failure
    assert_eq!(
        error.message(),
        "Failed to resolve bootstrap servers after 3000ms. \
         Please check your bootstrap.servers configuration and DNS settings."
    );

    // After the first failure, any further API call must also fail. This guards against
    // accidentally clearing the bootstrap error from the metadata layer.
    let again = consumer.poll(std::time::Duration::from_millis(100)).await;
    assert!(
        again.as_ref().is_err_and(Error::is_bootstrap_resolution_error),
        "got {:?}",
        again.err()
    );
    consumer.close().await.unwrap();
}

/// Translated from
/// `KafkaConsumerTest.testConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero`
/// (KAFKA-20939), for `GroupProtocol.CONSUMER` (`CLASSIC` is out of scope,
/// as above): the default `bootstrap.resolve.timeout.ms=0` resolves DNS
/// synchronously in the constructor, so an unresolvable host fails
/// construction with the `ConfigException` wrapped in the constructor's
/// `KafkaException`, and no consumer instance is created.
#[tokio::test(flavor = "multi_thread")]
#[doc(
    alias = "org.apache.kafka.clients.consumer.KafkaConsumerTest#testConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero"
)]
async fn test_consumer_constructor_fails_with_config_error_on_unresolvable_bootstrap_when_timeout_zero() {
    let props = HashMap::from([
        (
            ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            "unresolvable.invalid:9092".to_string(),
        ),
        (ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(), "consumer".to_string()),
        (ConsumerConfig::GROUP_ID_CONFIG.to_string(), "test-group".to_string()),
    ]);
    // `bootstrap.resolve.timeout.ms` stays at its default of 0, asserted by the
    // crate's `consumer_config::tests::test_bootstrap_resolve_timeout_ms`.
    let config = ConsumerConfig::new(&props).unwrap();
    let error =
        KafkaConsumer::<String, String>::new(config, Box::new(StringDeserializer), Box::new(StringDeserializer))
            .err()
            .expect("construction must fail");
    assert_eq!(error.message(), "Failed to construct kafka consumer");
    let cause: &Error = std::error::Error::source(&error)
        .and_then(|e| e.downcast_ref::<Error>())
        .expect("the cause is a crate Error");
    assert!(cause.is_config_error(), "got {cause:?}");
    assert_eq!(cause.message(), "No resolvable bootstrap urls given in bootstrap.servers");
}
