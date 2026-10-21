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
//! in-process `dyn` object across the wire. Broker-side pattern subscription
//! (`SubscriptionPattern`) crosses the wire like a topic list; client-side
//! `java.util.regex.Pattern` subscription is not translated at all.
//!
//! Requires `--features multilanguage-tests`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::{Error, Metric, TopicPartition};
use confluent_kafka::consumer::{
    CloseOptions, DynConsumer, GroupMembershipOperation, OffsetAndMetadata, SubscriptionPattern,
};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::admin_backend::{AdminBackend, admin_for, create_topic};
use crate::common::backend_factory::{ConsumerBackendFactory, RustNativeFactory};
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
/// from the backend (container listener for Python/C, host loopback for Rust).
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

/// [`consumer_config`] for the backend's reachable bootstrap over the run's
/// selected protocol, with the matching security keys injected. Every backend —
/// native and gRPC/container alike — connects over the run's protocol; the keys
/// travel through the gRPC config map to the containerized client.
fn consumer_config_for<F: ConsumerBackendFactory>(
    factory: &F,
    ctx: &TestContext,
    group_id: &str,
) -> HashMap<String, String> {
    let mut config = consumer_config(
        ctx.protocol_bootstrap_servers_for(factory.needs_container_bootstrap()),
        group_id,
    );
    ctx.apply_security(&mut config);
    config
}

/// Produce `records` to `topic` with a native in-process Rust producer on the
/// host-loopback bootstrap (the producer always runs in this process), over the
/// run's selected protocol.
async fn produce(ctx: &TestContext, topic: &str, records: &[(&str, &str)]) {
    let mut props = HashMap::from([
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
    ]);
    ctx.configure(&mut props);
    let config = ProducerConfig::new(&props).expect("producer config");
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> =
        KafkaProducer::new(config, ByteArraySerializer::default(), ByteArraySerializer::default())
            .expect("create producer");
    for &(k, v) in records {
        let record = ProducerRecord::with_key(topic.to_string(), Some(b(k)), Some(b(v)));
        // Fully-qualified trait call: KafkaProducer also has an inherent
        // 2-arg send(record, callback) that would otherwise shadow this.
        let fut = Producer::send(&producer, record).await.expect("send");
        fut.get_with_timeout(Duration::from_secs(30)).await.expect("produce");
    }
    producer.close().await.expect("close producer");
}

/// Poll until at least `want` records are collected or `deadline` elapses.
/// Returns the records as (key, value) byte pairs in receive order.
async fn collect(
    consumer: &mut Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>,
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

/// Poll the consumer until its callback log contains an entry of `kind` that
/// satisfies `wanted`, or `deadline` elapses. Returns the last snapshot either
/// way, so callers can assert and print the whole log on failure.
///
/// Waiting for the *expected* entry, not the first one of its kind, matters:
/// a KIP-848 member's first reconciliation can complete with an empty
/// assignment (a pattern the coordinator has not matched yet, or a topic its
/// metadata does not have yet), and the listener then logs an `assigned`
/// entry with no partitions before the one the scenario expects.
///
/// Polling is what makes progress: per `consumer-threading.md` §31 both the
/// rebalance listener and the offset-commit callback run on the *caller's* task
/// inside `poll` / `commit_*` / `close`, so a test that only sleeps would never
/// see them. For the gRPC backends each poll is a `Poll` RPC on the server-side
/// consumer, and the log is one further RPC behind.
async fn poll_until_entry(
    consumer: &mut Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>,
    log: &ConsumerCallbackLog,
    kind: &str,
    wanted: impl Fn(&CallbackLogEntry) -> bool,
    deadline: Duration,
) -> Vec<CallbackLogEntry> {
    let start = Instant::now();
    loop {
        let entries = log.entries().await.expect("read consumer callback log");
        if entries.iter().any(|e| e.kind == kind && wanted(e)) || start.elapsed() >= deadline {
            return entries;
        }
        consumer.poll(Duration::from_millis(500)).await.expect("poll");
    }
}

/// Create `topic` with `partitions` partitions through a native admin client,
/// waiting for its metadata to propagate. The admin is fixture, like the
/// producer: only the consumer is under test.
async fn create_topic_with_partitions(ctx: &TestContext, topic: &str, partitions: i32) {
    let admin = admin_for(&RustNativeFactory, ctx).await;
    create_topic(&admin, topic, partitions, 1).await;
    admin.close(Some(Duration::from_secs(5))).await.expect("close fixture admin");
}

/// Poll until the consumer's assignment has exactly `want` partitions, or
/// `deadline` elapses; returns the last assignment either way.
async fn poll_until_assigned(
    consumer: &mut Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>,
    want: usize,
    deadline: Duration,
) -> std::collections::HashSet<TopicPartition> {
    let start = Instant::now();
    loop {
        let assignment = consumer.assignment();
        if assignment.len() == want || start.elapsed() >= deadline {
            return assignment;
        }
        consumer.poll(Duration::from_millis(500)).await.expect("poll");
    }
}

/// A broker-side (RE2/J) pattern matching exactly `topic`: the test topic
/// names are `[a-z0-9_]` only, so the name is its own regex, and the trailing
/// `.*` keeps it a pattern rather than a literal.
fn pattern_for(topic: &str) -> SubscriptionPattern {
    SubscriptionPattern::new(format!("{topic}.*"))
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    consumer.subscribe_with_topics(vec![topic.clone()]).await.expect("subscribe");
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
    consumer.seek_with_offset(tp.clone(), 1).await.expect("seek");
    let got = collect(&mut consumer, 2, Duration::from_secs(20)).await;
    let values: Vec<Vec<u8>> = got.iter().map(|(_, v)| v.clone().unwrap()).collect();
    assert_eq!(values, vec![b("v1"), b("v2")], "{} backend", factory.name());

    consumer.close().await.expect("close");
}

async fn pause_resume<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_pause");
    produce(ctx, &topic, &[("k", "v")]).await;

    let mut consumer = factory
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    consumer.subscribe_with_topics(vec![topic.clone()]).await.expect("subscribe");
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::with_metadata(1, "ck").expect("oam"))]);
    consumer
        .commit_sync_with_offsets(offsets)
        .await
        .expect("commit_sync_with_offsets");
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
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
        .create_with_callback_log(consumer_config_for(factory, ctx, &format!("{topic_a}-grp")))
        .await
        .expect("create consumer with callback log");

    log.subscribe_with_logging_listener(&mut consumer, vec![topic_a.clone()])
        .await
        .expect("subscribe with logging listener");
    let entries = poll_until_entry(
        &mut consumer,
        &log,
        KIND_ASSIGNED,
        |e| e.has_partition(&topic_a, 0),
        Duration::from_secs(30),
    )
    .await;
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
    let entries = poll_until_entry(
        &mut consumer,
        &log,
        KIND_REVOKED,
        |e| e.has_partition(&topic_a, 0),
        Duration::from_secs(30),
    )
    .await;
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
        .create_with_callback_log(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
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
    let entries = poll_until_entry(
        &mut consumer,
        &log,
        KIND_COMMIT,
        |e| e.offset_for(&topic, 0) == Some(2),
        Duration::from_secs(20),
    )
    .await;
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

// ---------------------------------------------------------------------------
// Pattern subscription (SubscriptionPattern, KIP-848)
// ---------------------------------------------------------------------------

/// Java's `subscribe(SubscriptionPattern)`: the group coordinator resolves the
/// RE2/J pattern against the cluster's topics, and the consumer is assigned
/// and consumes the matching topic like a named one.
async fn subscribe_pattern_and_consume<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_pattern_consume");
    produce(ctx, &topic, &[("k0", "p0"), ("k1", "p1")]).await;

    let mut consumer = factory
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    consumer
        .subscribe_with_pattern(pattern_for(&topic))
        .await
        .expect("subscribe with pattern");

    let got = collect(&mut consumer, 2, Duration::from_secs(30)).await;
    let values: Vec<Vec<u8>> = got.iter().map(|(_, v)| v.clone().unwrap()).collect();
    assert_eq!(values, vec![b("p0"), b("p1")], "{} backend", factory.name());
    assert_eq!(
        consumer.assignment(),
        [TopicPartition::new(topic.clone(), 0)].into_iter().collect(),
        "{} backend: the pattern's one matching partition is assigned",
        factory.name()
    );

    consumer.close().await.expect("close");
}

/// Java's `subscribe(SubscriptionPattern, ConsumerRebalanceListener)`: the
/// listener the backend's own binding registers is invoked with the matching
/// topic's partition on assignment, and with it again on revocation when the
/// subscription switches to a pattern matching only another topic — the
/// pattern twin of [`rebalance_listener_logs_assigned_and_revoked`].
async fn pattern_listener_logs_assigned_and_revoked<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic_a = ctx.topic("ml_pattern_listener_a");
    let topic_b = ctx.topic("ml_pattern_listener_b");
    produce(ctx, &topic_a, &[("k", "va")]).await;
    produce(ctx, &topic_b, &[("k", "vb")]).await;

    let (mut consumer, log) = factory
        .create_with_callback_log(consumer_config_for(factory, ctx, &format!("{topic_a}-grp")))
        .await
        .expect("create consumer with callback log");

    log.subscribe_pattern_with_logging_listener(&mut consumer, pattern_for(&topic_a))
        .await
        .expect("subscribe pattern with logging listener");
    let entries = poll_until_entry(
        &mut consumer,
        &log,
        KIND_ASSIGNED,
        |e| e.has_partition(&topic_a, 0),
        Duration::from_secs(30),
    )
    .await;
    let assigned = of_kind(&entries, KIND_ASSIGNED);
    assert!(
        assigned.iter().any(|e| e.has_partition(&topic_a, 0)),
        "{} backend: no {KIND_ASSIGNED} entry naming {topic_a}-0; log = {entries:?}",
        factory.name()
    );
    assert!(
        assigned.iter().all(|e| !e.has_partition(&topic_b, 0)),
        "{} backend: {topic_b} does not match the pattern, so it is never assigned; log = {entries:?}",
        factory.name()
    );
    assert!(
        assigned.iter().all(|e| e.error.is_empty()),
        "{} backend: listener reported an error; log = {entries:?}",
        factory.name()
    );

    log.subscribe_pattern_with_logging_listener(&mut consumer, pattern_for(&topic_b))
        .await
        .expect("re-subscribe pattern with logging listener");
    let entries = poll_until_entry(
        &mut consumer,
        &log,
        KIND_REVOKED,
        |e| e.has_partition(&topic_a, 0),
        Duration::from_secs(30),
    )
    .await;
    assert!(
        of_kind(&entries, KIND_REVOKED).iter().any(|e| e.has_partition(&topic_a, 0)),
        "{} backend: no {KIND_REVOKED} entry naming {topic_a}-0 after switching pattern; log = {entries:?}",
        factory.name()
    );
    let entries = poll_until_entry(
        &mut consumer,
        &log,
        KIND_ASSIGNED,
        |e| e.has_partition(&topic_b, 0),
        Duration::from_secs(30),
    )
    .await;
    assert!(
        of_kind(&entries, KIND_ASSIGNED).iter().any(|e| e.has_partition(&topic_b, 0)),
        "{} backend: the new pattern's {topic_b}-0 is assigned; log = {entries:?}",
        factory.name()
    );

    consumer.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// commitAsync(offsets, callback)
// ---------------------------------------------------------------------------

/// Java's `commitAsync(Map, OffsetCommitCallback)`: the callback the backend's
/// binding registers reports exactly the offsets it was given — not the
/// consumed position — with no error, and the broker then reports them as
/// committed, metadata included.
async fn commit_async_offsets_callback_logs_offsets<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_commit_offsets_cb");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1"), ("k2", "v2")]).await;

    let (mut consumer, log) = factory
        .create_with_callback_log(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer with callback log");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");
    assert_eq!(
        collect(&mut consumer, 3, Duration::from_secs(20)).await.len(),
        3,
        "{} backend",
        factory.name()
    );

    // Offset 1, not the consumed position 3: the callback must echo the map it
    // was handed.
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::with_metadata(1, "explicit").expect("oam"))]);
    log.commit_async_offsets_with_logging_callback(&mut consumer, offsets)
        .await
        .expect("commit_async with offsets and logging callback");
    let entries = poll_until_entry(
        &mut consumer,
        &log,
        KIND_COMMIT,
        |e| e.offset_for(&topic, 0) == Some(1),
        Duration::from_secs(20),
    )
    .await;
    let commits = of_kind(&entries, KIND_COMMIT);
    assert_eq!(
        commits.len(),
        1,
        "{} backend: one commitAsync, one callback; log = {entries:?}",
        factory.name()
    );
    assert_eq!(
        commits[0].offset_for(&topic, 0),
        Some(1),
        "{} backend: the callback reports the explicit offset; log = {entries:?}",
        factory.name()
    );
    assert_eq!(
        commits[0].partitions,
        vec![(topic.clone(), 0)],
        "{} backend: the callback reports exactly the committed partition",
        factory.name()
    );
    assert!(
        commits[0].error.is_empty(),
        "{} backend: commit callback reported an error: {}",
        factory.name(),
        commits[0].error
    );

    let committed = consumer.committed(std::slice::from_ref(&tp)).await.expect("committed");
    let entry = committed.get(&tp).expect("committed entry");
    assert_eq!(entry.offset(), 1, "{} backend", factory.name());
    assert_eq!(entry.metadata(), "explicit", "{} backend", factory.name());

    consumer.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// The timed overloads (`Duration timeout`)
// ---------------------------------------------------------------------------

/// A timeout every timed call below passes: long enough to never expire on a
/// healthy broker, and never the default, so a backend that dropped the
/// argument would call a different overload.
const EXPLICIT_TIMEOUT: Duration = Duration::from_secs(20);

/// `poll(Duration)` with an explicit timeout: it returns the produced records
/// as soon as they arrive, and on a drained partition it waits out about the
/// requested timeout before returning nothing (Java's "blocks up to the
/// timeout"), which a backend sending a different timeout would miss.
async fn poll_with_explicit_timeout<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_timed_poll");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1")]).await;

    let mut consumer = factory
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    let mut got = Vec::new();
    let start = Instant::now();
    while got.len() < 2 && start.elapsed() < Duration::from_secs(30) {
        let records = consumer.poll(Duration::from_secs(5)).await.expect("poll");
        got.extend(records.into_iter().map(|r| r.value().map(|v| v.to_vec())));
    }
    assert_eq!(got, vec![Some(b("v0")), Some(b("v1"))], "{} backend", factory.name());

    let wait = Duration::from_millis(1500);
    let start = Instant::now();
    let empty = consumer.poll(wait).await.expect("poll on a drained partition");
    let elapsed = start.elapsed();
    assert_eq!(empty.count(), 0, "{} backend: nothing left to fetch", factory.name());
    assert!(
        elapsed >= Duration::from_millis(1000) && elapsed < Duration::from_secs(10),
        "{} backend: poll({wait:?}) on a drained partition should wait about the timeout, took {elapsed:?}",
        factory.name()
    );

    consumer.close().await.expect("close");
}

/// `commitSync(Duration)`, `commitSync(Map, Duration)`, `committed(Set,
/// Duration)` and `position(TopicPartition, Duration)` return the broker's
/// values. `position` with a zero timeout on a partition whose position is not
/// yet known is Java's `TimeoutException`: the offset lookup cannot complete in
/// zero time, which proves the timeout reaches the consumer.
async fn timed_commit_committed_and_position<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_timed_commit");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1"), ("k2", "v2")]).await;

    let mut consumer = factory
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    let err = consumer
        .position_with_timeout(&tp, Duration::ZERO)
        .await
        .expect_err("a zero timeout cannot resolve an unknown position");
    assert!(
        matches!(err, Error::Timeout(_)),
        "{} backend: position(tp, 0) on an unknown position is a timeout, got {err:?}",
        factory.name()
    );

    assert_eq!(collect(&mut consumer, 3, Duration::from_secs(20)).await.len(), 3);
    assert_eq!(
        consumer.position_with_timeout(&tp, EXPLICIT_TIMEOUT).await.expect("position"),
        3,
        "{} backend: the position after consuming three records",
        factory.name()
    );

    consumer
        .commit_sync_with_timeout(EXPLICIT_TIMEOUT)
        .await
        .expect("commit_sync with timeout");
    let committed = consumer
        .committed_with_timeout(std::slice::from_ref(&tp), EXPLICIT_TIMEOUT)
        .await
        .expect("committed with timeout");
    assert_eq!(
        committed.get(&tp).map(OffsetAndMetadata::offset),
        Some(3),
        "{} backend: commitSync(Duration) commits the consumed position",
        factory.name()
    );

    consumer
        .commit_sync_with_offsets_timeout(
            HashMap::from([(tp.clone(), OffsetAndMetadata::with_metadata(2, "timed").expect("oam"))]),
            EXPLICIT_TIMEOUT,
        )
        .await
        .expect("commit_sync with offsets and timeout");
    let committed = consumer
        .committed_with_timeout(std::slice::from_ref(&tp), EXPLICIT_TIMEOUT)
        .await
        .expect("committed with timeout");
    let entry = committed.get(&tp).expect("committed entry");
    assert_eq!(entry.offset(), 2, "{} backend", factory.name());
    assert_eq!(entry.metadata(), "timed", "{} backend", factory.name());

    consumer.close().await.expect("close");
}

/// `beginningOffsets(Collection, Duration)`, `endOffsets(Collection, Duration)`
/// and `offsetsForTimes(Map, Duration)` return the broker's offsets. A zero
/// timeout makes Java's `beginningOrEndOffset` return an empty map at once
/// (`AsyncKafkaConsumer`, "to align with the classic consumer"), so the zero
/// calls only pass when the timeout crosses the wire.
async fn timed_offset_lookups<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_timed_offsets");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1"), ("k2", "v2"), ("k3", "v3")]).await;

    let mut consumer = factory
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tps = std::slice::from_ref(&tp);

    assert_eq!(
        consumer
            .beginning_offsets_with_timeout(tps, EXPLICIT_TIMEOUT)
            .await
            .expect("beginning"),
        HashMap::from([(tp.clone(), 0)]),
        "{} backend",
        factory.name()
    );
    assert_eq!(
        consumer.end_offsets_with_timeout(tps, EXPLICIT_TIMEOUT).await.expect("end"),
        HashMap::from([(tp.clone(), 4)]),
        "{} backend",
        factory.name()
    );
    assert!(
        consumer
            .beginning_offsets_with_timeout(tps, Duration::ZERO)
            .await
            .expect("beginning, 0")
            .is_empty(),
        "{} backend: beginningOffsets with a zero timeout returns an empty map",
        factory.name()
    );
    assert!(
        consumer
            .end_offsets_with_timeout(tps, Duration::ZERO)
            .await
            .expect("end, 0")
            .is_empty(),
        "{} backend: endOffsets with a zero timeout returns an empty map",
        factory.name()
    );

    // Timestamp 0 resolves to the first record; a timestamp in the future to
    // none, which Java reports as a null value and the wire omits.
    let found = consumer
        .offsets_for_times_with_timeout(HashMap::from([(tp.clone(), 0)]), EXPLICIT_TIMEOUT)
        .await
        .expect("offsets_for_times with timeout");
    let first = found.get(&tp).expect("timestamp 0 resolves");
    assert_eq!(first.offset(), 0, "{} backend", factory.name());
    assert!(first.timestamp() > 0, "{} backend: the record's create time", factory.name());
    let future = consumer
        .offsets_for_times_with_timeout(HashMap::from([(tp.clone(), i64::MAX / 2)]), EXPLICIT_TIMEOUT)
        .await
        .expect("offsets_for_times in the future");
    assert!(
        !future.contains_key(&tp),
        "{} backend: no record at or after a future timestamp, got {future:?}",
        factory.name()
    );

    consumer.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// seek(TopicPartition, OffsetAndMetadata)
// ---------------------------------------------------------------------------

/// Java's `seek(TopicPartition, OffsetAndMetadata)` with a leader epoch: the
/// position moves to the given offset and consumption resumes there. The
/// epoch is the partition's real one (0, a fresh single-replica topic), so
/// the position validation the epoch triggers succeeds.
async fn seek_with_offset_and_metadata_resumes_there<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_seek_oam");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1"), ("k2", "v2"), ("k3", "v3")]).await;

    let mut consumer = factory
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");
    assert_eq!(collect(&mut consumer, 4, Duration::from_secs(20)).await.len(), 4);

    consumer
        .seek_with_offset_and_metadata(
            tp.clone(),
            OffsetAndMetadata::with_leader_epoch_metadata(2, Some(0), "seek").expect("oam"),
        )
        .await
        .expect("seek with offset and metadata");
    assert_eq!(
        consumer.position(&tp).await.expect("position"),
        2,
        "{} backend: the position is the seeked offset",
        factory.name()
    );
    let got = collect(&mut consumer, 2, Duration::from_secs(20)).await;
    let values: Vec<Vec<u8>> = got.iter().map(|(_, v)| v.clone().unwrap()).collect();
    assert_eq!(values, vec![b("v2"), b("v3")], "{} backend", factory.name());

    consumer.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// close(CloseOptions), observed by a second member of the group
// ---------------------------------------------------------------------------

/// Two members share a two-partition topic: the consumer under test and a
/// native Rust member that observes the group. Returns both, each holding one
/// partition.
async fn two_member_group<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
    topic: &str,
    group: &str,
) -> (Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>, Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>) {
    create_topic_with_partitions(ctx, topic, 2).await;
    let mut under_test = factory
        .create(consumer_config_for(factory, ctx, group))
        .await
        .expect("create consumer under test");
    under_test
        .subscribe_with_topics(vec![topic.to_string()])
        .await
        .expect("subscribe consumer under test");
    assert_eq!(
        poll_until_assigned(&mut under_test, 2, Duration::from_secs(30)).await.len(),
        2,
        "{} backend: the first member gets both partitions",
        factory.name()
    );

    let mut observer = RustNativeFactory
        .create(consumer_config_for(&RustNativeFactory, ctx, group))
        .await
        .expect("create observer");
    observer
        .subscribe_with_topics(vec![topic.to_string()])
        .await
        .expect("subscribe observer");
    // Both members must poll for the group to hand one partition over: the
    // first member revokes, the second is then assigned.
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        observer.poll(Duration::from_millis(300)).await.expect("observer poll");
        under_test.poll(Duration::from_millis(300)).await.expect("poll");
        if observer.assignment().len() == 1 && under_test.assignment().len() == 1 {
            return (under_test, observer);
        }
    }
    panic!(
        "{} backend: the two members never split the partitions: under test {:?}, observer {:?}",
        factory.name(),
        under_test.assignment(),
        observer.assignment()
    );
}

/// `close(CloseOptions.groupMembershipOperation(LEAVE_GROUP))` sends the leave
/// heartbeat (`ConsumerMembershipManager.leaveGroupEpoch` is -1), so the
/// coordinator hands the leaver's partition to the remaining member at once —
/// well inside the 45 s session timeout that would otherwise have to expire.
async fn close_with_leave_group_hands_over_partitions<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_close_leave");
    let group = format!("{topic}-grp");
    let (mut under_test, mut observer) = two_member_group(ctx, factory, &topic, &group).await;

    under_test
        .close_with_options(
            CloseOptions::with_operation(GroupMembershipOperation::LeaveGroup).with_timeout(Duration::from_secs(30)),
        )
        .await
        .expect("close with LEAVE_GROUP");
    let assignment = poll_until_assigned(&mut observer, 2, Duration::from_secs(20)).await;
    assert_eq!(
        assignment.len(),
        2,
        "{} backend: after a LEAVE_GROUP close the other member owns both partitions, got {assignment:?}",
        factory.name()
    );

    observer.close().await.expect("close observer");
}

/// `close(CloseOptions.groupMembershipOperation(REMAIN_IN_GROUP))` on a dynamic
/// member sends no leave heartbeat (`ConsumerMembershipManager.isLeavingGroup`
/// is false for it), so the coordinator keeps the member and its partition
/// until the session timeout (45 s) expires: the other member stays on one
/// partition throughout an observation window well below that.
async fn close_with_remain_in_group_keeps_the_member<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_close_remain");
    let group = format!("{topic}-grp");
    let (mut under_test, mut observer) = two_member_group(ctx, factory, &topic, &group).await;

    under_test
        .close_with_options(
            CloseOptions::with_operation(GroupMembershipOperation::RemainInGroup).with_timeout(Duration::from_secs(30)),
        )
        .await
        .expect("close with REMAIN_IN_GROUP");
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        observer.poll(Duration::from_millis(500)).await.expect("observer poll");
        assert_eq!(
            observer.assignment().len(),
            1,
            "{} backend: a REMAIN_IN_GROUP close keeps the member, so its partition is not reassigned \
             before the session timeout",
            factory.name()
        );
    }

    observer.close().await.expect("close observer");
}

// ---------------------------------------------------------------------------
// wakeup()
// ---------------------------------------------------------------------------

/// Java's `wakeup()` from another thread breaks a blocking `poll`: the poll
/// fails with `WakeupException` long before its timeout, the exception carries
/// no message (Java's `WakeupException` has none), and the next poll proceeds
/// normally.
async fn wakeup_breaks_a_blocking_poll<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_wakeup");
    produce(ctx, &topic, &[("k", "v")]).await;

    let (mut consumer, log) = factory
        .create_with_callback_log(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");
    assert_eq!(collect(&mut consumer, 1, Duration::from_secs(20)).await.len(), 1);

    let waker = log.wakeup_handle(consumer.as_ref());
    let wake = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        waker.wakeup().await;
    });
    let start = Instant::now();
    let err = consumer
        .poll(Duration::from_secs(30))
        .await
        .expect_err("wakeup() must break the blocking poll");
    let elapsed = start.elapsed();
    wake.await.expect("wakeup task");
    assert!(
        matches!(err, Error::Wakeup(_)),
        "{} backend: a woken poll is a WakeupException, got {err:?}",
        factory.name()
    );
    assert_eq!(
        err.message(),
        "",
        "{} backend: Java's WakeupException carries no message",
        factory.name()
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "{} backend: the poll returned at the wakeup, not its timeout, took {elapsed:?}",
        factory.name()
    );

    // The wakeup is consumed by the call it broke.
    let records = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect("the poll after a wakeup proceeds normally");
    assert_eq!(records.count(), 0, "{} backend: nothing new to fetch", factory.name());

    consumer.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// groupMetadata(), currentLag(), assignment() after a poll
// ---------------------------------------------------------------------------

/// After a subscribed consumer joined and consumed: `assignment()` is the
/// subscribed partition, and `groupMetadata()` names the group with a real
/// member id and the member's epoch as generation (KIP-848) and no
/// `group.instance.id` for a dynamic member.
async fn group_metadata_and_assignment_after_poll<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_group_metadata");
    let group = format!("{topic}-grp");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1"), ("k2", "v2")]).await;

    let mut consumer = factory
        .create(consumer_config_for(factory, ctx, &group))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.subscribe_with_topics(vec![topic.clone()]).await.expect("subscribe");
    assert_eq!(collect(&mut consumer, 3, Duration::from_secs(30)).await.len(), 3);

    assert_eq!(
        consumer.assignment(),
        [tp.clone()].into_iter().collect(),
        "{} backend: the subscribed topic's one partition is assigned",
        factory.name()
    );
    let metadata = consumer.group_metadata();
    assert_eq!(metadata.group_id(), group, "{} backend", factory.name());
    assert!(
        !metadata.member_id().is_empty(),
        "{} backend: a joined member has a member id",
        factory.name()
    );
    assert!(
        metadata.generation_id() > 0,
        "{} backend: a joined member's epoch is positive, got {}",
        factory.name(),
        metadata.generation_id()
    );
    assert_eq!(
        metadata.group_instance_id(),
        None,
        "{} backend: a dynamic member has no group.instance.id",
        factory.name()
    );

    consumer.close().await.expect("close");
}

/// Java's `currentLag(TopicPartition)` reads `SubscriptionState.partitionLag`:
/// the last fetch's high watermark minus the position. It is empty before the
/// partition was fetched, and 0 once every record was consumed. (For a
/// partition that is not assigned Java throws `IllegalStateException("No
/// current assignment for partition …")`, which this scenario does not
/// exercise.)
async fn current_lag_after_consuming_everything<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ml_current_lag");
    produce(ctx, &topic, &[("k0", "v0"), ("k1", "v1"), ("k2", "v2")]).await;

    let mut consumer = factory
        .create(consumer_config_for(factory, ctx, &format!("{topic}-grp")))
        .await
        .expect("create consumer");
    let tp = TopicPartition::new(topic.clone(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");
    assert_eq!(
        consumer.current_lag(&tp).await.expect("current_lag"),
        None,
        "{} backend: no lag before the partition was fetched",
        factory.name()
    );
    assert_eq!(collect(&mut consumer, 3, Duration::from_secs(30)).await.len(), 3);
    assert_eq!(
        consumer.current_lag(&tp).await.expect("current_lag"),
        Some(0),
        "{} backend: everything was consumed, so the lag is 0",
        factory.name()
    );

    consumer.close().await.expect("close");
}

multilanguage_consumer_test!(test_ml_subscribe_pattern_and_consume, subscribe_pattern_and_consume);
multilanguage_consumer_test!(
    test_ml_pattern_listener_logs_assigned_and_revoked,
    pattern_listener_logs_assigned_and_revoked
);
multilanguage_consumer_test!(
    test_ml_commit_async_offsets_callback_logs_offsets,
    commit_async_offsets_callback_logs_offsets
);
multilanguage_consumer_test!(test_ml_poll_with_explicit_timeout, poll_with_explicit_timeout);
multilanguage_consumer_test!(test_ml_timed_commit_committed_and_position, timed_commit_committed_and_position);
multilanguage_consumer_test!(test_ml_timed_offset_lookups, timed_offset_lookups);
multilanguage_consumer_test!(
    test_ml_seek_with_offset_and_metadata_resumes_there,
    seek_with_offset_and_metadata_resumes_there
);
multilanguage_consumer_test!(
    test_ml_close_with_leave_group_hands_over_partitions,
    close_with_leave_group_hands_over_partitions
);
multilanguage_consumer_test!(
    test_ml_close_with_remain_in_group_keeps_the_member,
    close_with_remain_in_group_keeps_the_member
);
multilanguage_consumer_test!(test_ml_wakeup_breaks_a_blocking_poll, wakeup_breaks_a_blocking_poll);
multilanguage_consumer_test!(
    test_ml_group_metadata_and_assignment_after_poll,
    group_metadata_and_assignment_after_poll
);
multilanguage_consumer_test!(
    test_ml_current_lag_after_consuming_everything,
    current_lag_after_consuming_everything
);

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
