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

//! Consumer integration tests run against all three backends (native Rust,
//! Python, C) via [`multilanguage_consumer_test!`].
//!
//! Only the consumer is the system-under-test on the backend; setup records
//! are produced with a native in-process Rust producer (the producer is
//! incidental fixture). Scope is the supported consumer surface, including the
//! two user callbacks — the rebalance listener and the offset-commit callback —
//! observed through [`ConsumerCallbackLog`] rather than by handing an
//! in-process `dyn` object across the wire. Regex pattern subscription stays
//! native-Rust-only (see plaintext_consumer_*.rs).
//!
//! Requires `--features multilanguage-tests`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::{Metric, TopicPartition};
use confluent_kafka::consumer::Consumer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::callback_log::{CallbackLogEntry, ConsumerCallbackLog, KIND_ASSIGNED, KIND_COMMIT, KIND_REVOKED};
use crate::common::test_context::TestContext;
use crate::multilanguage_consumer_test;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Consumer config for the backend under test. `bootstrap` must be reachable
/// from the backend (container listener for python/c, host loopback for rust).
fn consumer_config(bootstrap: &str, group_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("client.id".to_string(), "multilang-consumer".to_string()),
    ])
}

fn bootstrap_for<F: ConsumerBackendFactory>(factory: &F, ctx: &TestContext) -> String {
    if factory.needs_container_bootstrap() {
        ctx.container_bootstrap_servers().to_string()
    } else {
        ctx.bootstrap_servers().to_string()
    }
}

/// Produce `records` to `topic` with a native in-process Rust producer on the
/// host-loopback bootstrap (the producer always runs in this process).
async fn produce(ctx: &TestContext, topic: &str, records: &[(&str, &str)]) {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
    ]);
    let config = ProducerConfig::from_properties(&props).expect("producer config");
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> =
        KafkaProducer::from_config(config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer))
            .expect("create producer");
    for &(k, v) in records {
        let record = ProducerRecord::new_key(topic.to_string(), Some(b(k)), Some(b(v)));
        // Fully-qualified trait call: KafkaProducer also has an inherent
        // 2-arg send(record, callback) that would otherwise shadow this.
        let fut = Producer::send(&producer, record).await.expect("send");
        fut.get_timeout(Duration::from_secs(30)).await.expect("produce");
    }
    producer.close().await.expect("close producer");
}

/// Poll until at least `want` records are collected or `deadline` elapses.
/// Returns the records as (key, value) byte pairs in receive order.
async fn collect(
    consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
    want: usize,
    deadline: Duration,
) -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> {
    let start = Instant::now();
    let mut out = Vec::new();
    while out.len() < want && start.elapsed() < deadline {
        let records = consumer.poll(Duration::from_millis(500)).await.expect("poll");
        for r in records {
            out.push((r.key().map(|k| k.to_vec()), r.value().map(|v| v.to_vec())));
        }
    }
    out
}

/// Poll the consumer until its callback log contains an entry of `kind`, or
/// `deadline` elapses. Returns the last snapshot either way, so callers can
/// assert and print the whole log on failure.
///
/// Polling is what makes progress: per `consumer-threading.md` §31 both the
/// rebalance listener and the offset-commit callback run on the *caller's* task
/// inside `poll` / `commit_*` / `close`, so a test that only sleeps would never
/// see them. For the gRPC backends each poll is a `Poll` RPC on the server-side
/// consumer, and the log is one further RPC behind.
async fn poll_until_kind(
    consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
    log: &ConsumerCallbackLog,
    kind: &str,
    deadline: Duration,
) -> Vec<CallbackLogEntry> {
    let start = Instant::now();
    loop {
        let entries = log.entries().await.expect("read consumer callback log");
        if entries.iter().any(|e| e.kind == kind) || start.elapsed() >= deadline {
            return entries;
        }
        consumer.poll(Duration::from_millis(500)).await.expect("poll");
    }
}

/// All log entries of one `kind`.
fn of_kind<'a>(entries: &'a [CallbackLogEntry], kind: &str) -> Vec<&'a CallbackLogEntry> {
    entries.iter().filter(|e| e.kind == kind).collect()
}

// ---------------------------------------------------------------------------
// Test bodies — generic over ConsumerBackendFactory
// ---------------------------------------------------------------------------

async fn assign_and_consume<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_assign_consume");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1"), ("k2", "v2")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    consumer
        .assign(vec![TopicPartition::new(topic.clone(), 0)])
        .await
        .expect("assign");

    let got = collect(&mut consumer, 3, Duration::from_secs(20)).await;
    let values: Vec<Vec<u8>> = got.iter().map(|(_, v)| v.clone().unwrap()).collect();
    assert_eq!(values, vec![b("v0"), b("v1"), b("v2")], "{} backend", factory.name());

    consumer.close().await.expect("close");
}

async fn subscribe_and_consume<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_subscribe_consume");
    produce(ctx, &topic, &[("k", "hello")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    consumer.subscribe_topics(vec![topic.clone()]).await.expect("subscribe");
    assert_eq!(consumer.subscription(), [topic.clone()].into_iter().collect());

    let got = collect(&mut consumer, 1, Duration::from_secs(20)).await;
    assert_eq!(got.len(), 1, "{} backend", factory.name());
    assert_eq!(got[0].1.clone().unwrap(), b("hello"));

    consumer.close().await.expect("close");
}

async fn commit_and_committed<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_commit");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");
    let got = collect(&mut consumer, 2, Duration::from_secs(20)).await;
    assert_eq!(got.len(), 2, "{} backend", factory.name());

    // Position advanced past the 2 records; commit it and read it back.
    let position = consumer.position(&tp).await.expect("position");
    assert_eq!(position, 2, "{} backend", factory.name());
    consumer.commit_sync().await.expect("commit_sync");
    let committed = consumer.committed(std::slice::from_ref(&tp)).await.expect("committed");
    assert_eq!(committed.get(&tp).map(|o| o.offset()), Some(2), "{} backend", factory.name());

    consumer.close().await.expect("close");
}

async fn seek_and_offsets<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_seek");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1"), ("k2", "v2")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    // beginning/end offsets bracket the 3 produced records.
    let begin = consumer.beginning_offsets(std::slice::from_ref(&tp)).await.expect("beginning");
    let end = consumer.end_offsets(std::slice::from_ref(&tp)).await.expect("end");
    assert_eq!(begin.get(&tp), Some(&0), "{} backend", factory.name());
    assert_eq!(end.get(&tp), Some(&3), "{} backend", factory.name());

    // Seek to offset 1 and consume from there.
    consumer.seek_offset(tp.clone(), 1).await.expect("seek");
    let got = collect(&mut consumer, 2, Duration::from_secs(20)).await;
    let values: Vec<Vec<u8>> = got.iter().map(|(_, v)| v.clone().unwrap()).collect();
    assert_eq!(values, vec![b("v1"), b("v2")], "{} backend", factory.name());

    consumer.close().await.expect("close");
}

async fn pause_resume<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_pause");
    produce(ctx, &topic, &[("k", "v")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    consumer.pause(std::slice::from_ref(&tp)).await.expect("pause");
    assert!(consumer.paused().contains(&tp), "{} backend", factory.name());
    // While paused, poll returns nothing.
    let paused_poll = consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(paused_poll.count(), 0, "{} backend", factory.name());

    consumer.resume(std::slice::from_ref(&tp)).await.expect("resume");
    assert!(!consumer.paused().contains(&tp));
    let got = collect(&mut consumer, 1, Duration::from_secs(20)).await;
    assert_eq!(got.len(), 1, "{} backend", factory.name());

    consumer.close().await.expect("close");
}

async fn seek_to_beginning_end<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_seek_ends");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    // Consume both, then rewind to the beginning and re-consume them.
    assert_eq!(collect(&mut consumer, 2, Duration::from_secs(20)).await.len(), 2);
    consumer
        .seek_to_beginning(std::slice::from_ref(&tp))
        .await
        .expect("seek_to_beginning");
    assert_eq!(
        collect(&mut consumer, 2, Duration::from_secs(20)).await.len(),
        2,
        "{} backend",
        factory.name()
    );

    // Seek to end: position is now the log end, so poll yields nothing new.
    consumer.seek_to_end(std::slice::from_ref(&tp)).await.expect("seek_to_end");
    let at_end = consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(at_end.count(), 0, "{} backend", factory.name());

    consumer.close().await.expect("close");
}

async fn unsubscribe_clears_subscription<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_unsubscribe");
    produce(ctx, &topic, &[("k", "v")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    consumer.subscribe_topics(vec![topic.clone()]).await.expect("subscribe");
    // Poll once so the subscription takes effect, then unsubscribe.
    let _ = collect(&mut consumer, 1, Duration::from_secs(20)).await;
    consumer.unsubscribe().await.expect("unsubscribe");
    assert!(consumer.subscription().is_empty(), "{} backend", factory.name());

    consumer.close().await.expect("close");
}

async fn partitions_for_metadata<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_partitions_for");
    produce(ctx, &topic, &[("k", "v")]).await; // ensure the topic exists

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let infos = consumer.partitions_for(&topic).await.expect("partitions_for");
    assert!(!infos.is_empty(), "{} backend: expected >=1 partition", factory.name());
    assert!(
        infos.iter().any(|p| p.topic() == topic && p.partition() == 0),
        "{} backend",
        factory.name()
    );

    consumer.close().await.expect("close");
}

async fn offsets_for_times_lookup<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_offsets_for_times");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    // Timestamp 0 (epoch) resolves to the earliest offset, i.e. 0.
    let spec = HashMap::from([(tp.clone(), 0i64)]);
    let result = consumer.offsets_for_times(spec).await.expect("offsets_for_times");
    assert_eq!(result.get(&tp).map(|o| o.offset()), Some(0), "{} backend", factory.name());

    consumer.close().await.expect("close");
}

async fn list_topics_contains<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_list_topics");
    produce(ctx, &topic, &[("k", "v")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let topics = consumer.list_topics().await.expect("list_topics");
    assert!(
        topics.contains_key(&topic),
        "{} backend: {topic} missing from list_topics",
        factory.name()
    );

    consumer.close().await.expect("close");
}

async fn commit_explicit_offsets<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    use confluent_kafka::consumer::OffsetAndMetadata;
    let topic = ctx.topic("ml_commit_explicit");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new_metadata(1, "ck").expect("oam"))]);
    consumer.commit_sync_offsets(offsets).await.expect("commit_sync_offsets");
    let committed = consumer.committed(std::slice::from_ref(&tp)).await.expect("committed");
    let entry = committed.get(&tp).expect("committed entry");
    assert_eq!(entry.offset(), 1, "{} backend", factory.name());
    assert_eq!(entry.metadata(), "ck", "{} backend", factory.name());

    consumer.close().await.expect("close");
}

/// `metrics()` reports the backend's real registry across every backend.
///
/// This is the end-to-end check on the Milestone-9 `metrics()` wiring: for the
/// Python / C backends the snapshot crosses the `Metrics` RPC, the Python
/// binding or C++ server, and the `kafka_consumer_MetricMap_t` FFI surface
/// before being rebuilt client-side. A backend that silently reported an empty
/// map (the pre-wiring behaviour) fails here.
///
/// Assertions are deliberately structural rather than value-based: metric
/// *values* depend on timing and broker behaviour, but the registry's shape does
/// not. After a consume, the fetch-manager group must exist and carry the
/// per-partition lag/lead family that `FetchMetricsManager` registers on first
/// sight of a partition.
async fn metrics_reports_backend_registry<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_metrics");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1")]).await;

    let mut consumer = factory
        .create(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    // Consume first: the per-partition lag/lead sensors are registered on the
    // first fetch that sees the partition, so an un-consumed consumer would
    // legitimately have no per-partition metrics yet.
    let got = collect(&mut consumer, 2, Duration::from_secs(20)).await;
    assert_eq!(got.len(), 2, "{} backend: setup consume failed", factory.name());

    let snapshot = consumer.metrics();
    assert!(
        !snapshot.is_empty(),
        "{} backend: metrics() returned an empty map — the backend registry is not wired through",
        factory.name()
    );

    // The client-level fetch group is always registered by the consumer ctor.
    let groups: std::collections::HashSet<&str> = snapshot.keys().map(|n| n.group()).collect();
    assert!(
        groups.contains("consumer-fetch-manager-metrics"),
        "{} backend: no consumer-fetch-manager-metrics group in {:?}",
        factory.name(),
        groups
    );
    assert!(
        groups.contains("consumer-metrics"),
        "{} backend: no consumer-metrics group in {:?}",
        factory.name(),
        groups
    );

    // `records-lag-max` is a client-level (untagged) fetch metric; its presence
    // proves the fetch sensors survived the round-trip with names intact.
    assert!(
        snapshot.keys().any(|n| n.name() == "records-lag-max"),
        "{} backend: records-lag-max missing from metrics()",
        factory.name()
    );

    // Per-partition detail sensors are tagged with topic + partition. Their
    // presence proves tags survived the round-trip (a name-keyed map would have
    // collapsed them).
    let has_partition_tagged = snapshot
        .keys()
        .any(|n| n.tags().get("topic").map(|t| t == &topic).unwrap_or(false) && n.tags().contains_key("partition"));
    assert!(
        has_partition_tagged,
        "{} backend: no topic/partition-tagged metric for {topic}; tags did not survive",
        factory.name()
    );

    // Every entry must yield a readable value (the snapshot is a real reading,
    // not a placeholder). `records-lag-max` is a Double.
    let lag_max = snapshot
        .iter()
        .find(|(n, _)| n.name() == "records-lag-max")
        .map(|(_, m)| m.metric_value())
        .expect("records-lag-max present");
    assert!(
        matches!(lag_max, confluent_kafka::common::MetricValue::Double(_)),
        "{} backend: records-lag-max should be a Double, got {lag_max:?}",
        factory.name()
    );

    consumer.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// Callback coverage (rebalance listener / offset-commit callback)
// ---------------------------------------------------------------------------

/// A rebalance listener registered through each backend's own binding is
/// invoked with the right partitions, for both `on_partitions_assigned` and
/// `on_partitions_revoked`.
///
/// The revoke is forced by *changing* the subscription (topic_a -> topic_b):
/// the group then hands back a target assignment without topic_a-0, and
/// reconciliation revokes it. `unsubscribe()` would not do — it fires
/// `on_partitions_lost`, not `revoked` (see `consumer-threading.md` §31).
async fn rebalance_listener_logs_assigned_and_revoked<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic_a = ctx.topic("ml_listener_a");
    let topic_b = ctx.topic("ml_listener_b");
    produce(ctx, &topic_a, &[("k", "va")]).await;
    produce(ctx, &topic_b, &[("k", "vb")]).await;

    let (mut consumer, log) = factory
        .create_with_callback_log(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic_a}-grp")))
        .await
        .expect("create consumer with callback log");

    log.subscribe_with_logging_listener(&mut consumer, vec![topic_a.clone()])
        .await
        .expect("subscribe with logging listener");
    let entries = poll_until_kind(&mut consumer, &log, KIND_ASSIGNED, Duration::from_secs(30)).await;
    let assigned = of_kind(&entries, KIND_ASSIGNED);
    assert!(
        !assigned.is_empty(),
        "{} backend: no {KIND_ASSIGNED} entry logged; log = {entries:?}",
        factory.name()
    );
    assert!(
        assigned.iter().any(|e| e.has_partition(&topic_a, 0)),
        "{} backend: no {KIND_ASSIGNED} entry naming {topic_a}-0; log = {entries:?}",
        factory.name()
    );
    assert!(
        assigned.iter().all(|e| e.error.is_empty()),
        "{} backend: listener reported an error; log = {entries:?}",
        factory.name()
    );

    // Re-register the listener with the new subscription: a *replacing*
    // subscribe releases the previous registration (SubscriptionState only
    // clears the listener on subscribe, never on unsubscribe), and it is the
    // listener registered when the revocation happens that gets invoked.
    log.subscribe_with_logging_listener(&mut consumer, vec![topic_b.clone()])
        .await
        .expect("re-subscribe with logging listener");
    let entries = poll_until_kind(&mut consumer, &log, KIND_REVOKED, Duration::from_secs(30)).await;
    let revoked = of_kind(&entries, KIND_REVOKED);
    assert!(
        revoked.iter().any(|e| e.has_partition(&topic_a, 0)),
        "{} backend: no {KIND_REVOKED} entry naming {topic_a}-0 after switching subscription; log = {entries:?}",
        factory.name()
    );

    consumer.close().await.expect("close");
}

/// An offset-commit callback registered through each backend's own binding is
/// invoked with the committed offsets and no error.
async fn commit_async_callback_logs_offsets<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_commit_cb");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1")]).await;

    let (mut consumer, log) = factory
        .create_with_callback_log(consumer_config(&bootstrap_for(factory, ctx), &format!("{topic}-grp")))
        .await
        .expect("create consumer with callback log");
    // assign() rather than subscribe() so no rebalance interleaves with the
    // commit — this test is about the commit callback only.
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");
    assert_eq!(
        collect(&mut consumer, 2, Duration::from_secs(20)).await.len(),
        2,
        "{} backend",
        factory.name()
    );

    log.commit_async_with_logging_callback(&mut consumer)
        .await
        .expect("commit_async with logging callback");
    // §31: the callback runs on the app task during a later poll/commit/close.
    let entries = poll_until_kind(&mut consumer, &log, KIND_COMMIT, Duration::from_secs(20)).await;
    let commits = of_kind(&entries, KIND_COMMIT);
    assert!(
        !commits.is_empty(),
        "{} backend: no {KIND_COMMIT} entry logged; log = {entries:?}",
        factory.name()
    );
    assert!(
        commits.iter().any(|e| e.offset_for(&topic, 0) == Some(2)),
        "{} backend: expected a {KIND_COMMIT} entry with {topic}-0 -> 2; log = {entries:?}",
        factory.name()
    );
    assert!(
        commits.iter().all(|e| e.error.is_empty()),
        "{} backend: commit callback reported an error; log = {entries:?}",
        factory.name()
    );

    // The callback's offsets must match what was actually committed.
    let committed = consumer.committed(std::slice::from_ref(&tp)).await.expect("committed");
    assert_eq!(committed.get(&tp).map(|o| o.offset()), Some(2), "{} backend", factory.name());

    consumer.close().await.expect("close");
}

multilanguage_consumer_test!(test_ml_metrics, metrics_reports_backend_registry);
multilanguage_consumer_test!(test_ml_assign_and_consume, assign_and_consume);
multilanguage_consumer_test!(test_ml_subscribe_and_consume, subscribe_and_consume);
multilanguage_consumer_test!(test_ml_commit_and_committed, commit_and_committed);
multilanguage_consumer_test!(test_ml_seek_and_offsets, seek_and_offsets);
multilanguage_consumer_test!(test_ml_pause_resume, pause_resume);
multilanguage_consumer_test!(test_ml_seek_to_beginning_end, seek_to_beginning_end);
multilanguage_consumer_test!(test_ml_unsubscribe, unsubscribe_clears_subscription);
multilanguage_consumer_test!(test_ml_partitions_for, partitions_for_metadata);
multilanguage_consumer_test!(test_ml_offsets_for_times, offsets_for_times_lookup);
multilanguage_consumer_test!(test_ml_list_topics, list_topics_contains);
multilanguage_consumer_test!(test_ml_commit_explicit_offsets, commit_explicit_offsets);
multilanguage_consumer_test!(
    test_ml_rebalance_listener_logs_assigned_and_revoked,
    rebalance_listener_logs_assigned_and_revoked
);
multilanguage_consumer_test!(test_ml_commit_async_callback_logs_offsets, commit_async_callback_logs_offsets);
