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

//! Helpers shared by the consumer scenarios that
//! [`crate::multilanguage_consumer_test`] instantiates once per backend.
//!
//! Available with only `integration-tests`, as the macro is: a converted
//! consumer integration test still runs its `__rust` arm under `make
//! test-integration`, so the helpers its body calls must compile there too.
//! Only the consumer is the system under test; the producer here is an
//! incidental fixture that always runs in this process.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::consumer::DynConsumer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::callback_log::{CallbackLogEntry, ConsumerCallbackLog};
use crate::common::test_context::TestContext;

/// The bytes of `s`.
pub fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Consumer config for the backend under test. `bootstrap` must be reachable
/// from the backend (container listener for Python/C, host loopback for Rust).
pub fn consumer_config(bootstrap: &str, group_id: &str) -> HashMap<String, String> {
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
pub fn consumer_config_for<F: ConsumerBackendFactory>(
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
pub async fn produce(ctx: &TestContext, topic: &str, records: &[(&str, &str)]) {
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
pub async fn collect(
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
pub async fn poll_until_entry(
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

/// Expand a consumer scenario that Java runs on both
/// `group.consumer.assignment.interval.ms` values (KIP-1263, KAFKA-20291:
/// `@ClusterTests` with `0` and `1000`) into three
/// [`crate::multilanguage_consumer_test`] invocations, as
/// `consumer_bounce_test.rs` does for its tests:
///
/// - `$name` on `$cfg`, for a broker older than 4.3, which has no
///   assignment interval (its behaviour is the `0` arm's); it skips itself on a
///   4.3+ broker, where its default interval (1000) would duplicate the `1000`
///   arm;
/// - `$name_assignment_interval_0` and `$name_assignment_interval_1000`, Java's
///   two arms, on `$cfg` with the interval set; they skip themselves on an
///   older broker, which ignores the config.
///
/// A skipped arm says so on stderr, outside libtest's capture
/// ([`crate::common::cluster_config::log_assignment_interval_skip`]), and runs
/// on `$cfg` itself
/// ([`crate::common::cluster_config::assignment_interval_arm`]), so it starts
/// no cluster of its own.
#[macro_export]
macro_rules! assignment_interval_consumer_test {
    ($name:ident, $body:ident, $cfg:expr) => {
        ::paste::paste! {
            async fn [<$body _without_assignment_interval>]<F: $crate::common::backend_factory::ConsumerBackendFactory>(
                ctx: &mut $crate::common::test_context::TestContext,
                factory: &F,
            ) {
                if $crate::common::cluster_config::broker_has_assignment_interval() {
                    $crate::common::cluster_config::log_assignment_interval_skip(
                        stringify!($name),
                        factory.name(),
                        "the broker has group.consumer.assignment.interval.ms; Java's two arms run instead",
                    );
                    return;
                }
                $body(ctx, factory).await;
            }

            async fn [<$body _with_assignment_interval_0>]<F: $crate::common::backend_factory::ConsumerBackendFactory>(
                ctx: &mut $crate::common::test_context::TestContext,
                factory: &F,
            ) {
                if !$crate::common::cluster_config::broker_has_assignment_interval() {
                    $crate::common::cluster_config::log_assignment_interval_skip(
                        stringify!([<$name _assignment_interval_0>]),
                        factory.name(),
                        "group.consumer.assignment.interval.ms is a 4.3 broker config",
                    );
                    return;
                }
                $body(ctx, factory).await;
            }

            async fn [<$body _with_assignment_interval_1000>]<F: $crate::common::backend_factory::ConsumerBackendFactory>(
                ctx: &mut $crate::common::test_context::TestContext,
                factory: &F,
            ) {
                if !$crate::common::cluster_config::broker_has_assignment_interval() {
                    $crate::common::cluster_config::log_assignment_interval_skip(
                        stringify!([<$name _assignment_interval_1000>]),
                        factory.name(),
                        "group.consumer.assignment.interval.ms is a 4.3 broker config",
                    );
                    return;
                }
                $body(ctx, factory).await;
            }

            $crate::multilanguage_consumer_test!($name, [<$body _without_assignment_interval>], $cfg);
            $crate::multilanguage_consumer_test!(
                [<$name _assignment_interval_0>],
                [<$body _with_assignment_interval_0>],
                $crate::common::cluster_config::assignment_interval_arm($cfg, 0)
            );
            $crate::multilanguage_consumer_test!(
                [<$name _assignment_interval_1000>],
                [<$body _with_assignment_interval_1000>],
                $crate::common::cluster_config::assignment_interval_arm($cfg, 1000)
            );
        }
    };
}
