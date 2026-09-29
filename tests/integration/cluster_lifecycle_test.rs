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

//! Smoke tests for the harness's broker lifecycle
//! (`KafkaCluster::{shutdown_broker, start_broker, wait_for_ready_brokers}`),
//! the Rust counterpart of Java's `ClusterInstance.shutdownBroker` /
//! `startBroker` / `waitForReadyBrokers` used by `ConsumerBounceTest`,
//! `ProducerFailureHandlingTest` and friends.
//!
//! These are harness tests, not translations of a Java test: they pin the
//! guarantees later fault-injection translations rely on — a stopped broker
//! disappears from the cluster and the ISR while the dedicated controller keeps
//! the cluster writable, and a restarted broker comes back with the same id,
//! address and data.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use confluent_kafka::admin::{Admin, AdminClientConfig, DescribeTopicsOptions, KafkaAdminClient};
use confluent_kafka::common::TopicCollection;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::{ByteArrayDeserializer, ByteArraySerializer};
use confluent_kafka::consumer::{ConsumerConfig, KafkaConsumer};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::cluster_config::{ClusterConfig, Type};
use crate::common::kafka_cluster::{SASL_PASSWORD, SASL_USERNAME};
use crate::common::test_context::TestContext;
use crate::common::test_utils::{DEFAULT_PAUSE_MS, create_topic, wait_until_true_with_timeout};

/// Bound for the cluster to reflect a broker stop/start (ISR, `DescribeCluster`).
const PROPAGATION_WAIT_MS: u64 = 60_000;

fn admin(bootstrap: &str) -> KafkaAdminClient {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "cluster-lifecycle-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "10000".to_string()),
        ("default.api.timeout.ms".to_string(), "10000".to_string()),
    ]);
    KafkaAdminClient::new(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client")
}

fn producer(bootstrap: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    producer_with_security(bootstrap, Vec::new())
}

/// A producer with extra `security.protocol` / `ssl.*` / `sasl.*` settings.
fn producer_with_security(bootstrap: &str, security: Vec<(&str, String)>) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "cluster-lifecycle-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "60000".to_string()),
        ("delivery.timeout.ms".to_string(), "120000".to_string()),
    ]);
    props.extend(security.into_iter().map(|(key, value)| (key.to_string(), value)));
    let config = ProducerConfig::new(&props).expect("valid producer config");
    KafkaProducer::new(config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer)).expect("build producer")
}

/// Sends `values` to partition 0 of `topic` and waits for every ack.
async fn send_all(producer: &KafkaProducer<Vec<u8>, Vec<u8>>, topic: &str, values: std::ops::Range<usize>) {
    let mut futures = Vec::new();
    for i in values {
        let record = ProducerRecord::with_partition_key(
            topic.to_string(),
            Some(0),
            Some(format!("key {i}").into_bytes()),
            Some(format!("value {i}").into_bytes()),
        )
        .expect("build record");
        futures.push(
            <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
                .await
                .expect("send"),
        );
    }
    for future in futures {
        future
            .get_with_timeout(Duration::from_secs(120))
            .await
            .expect("record acknowledged");
    }
}

/// Reads partition 0 of `topic` from the beginning until `expected` records
/// have arrived, returning `(offset, value)` pairs.
async fn consume_all(bootstrap: &str, topic: &str, expected: usize) -> Vec<(i64, String)> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "cluster-lifecycle-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("build consumer");
    consumer
        .assign(vec![TopicPartition::new(topic.to_string(), 0)])
        .await
        .expect("assign");

    let mut records = Vec::new();
    let start = Instant::now();
    while records.len() < expected && start.elapsed() < Duration::from_secs(60) {
        for record in &consumer.poll(Duration::from_secs(1)).await.expect("poll") {
            let value = String::from_utf8(record.value().expect("value").clone()).expect("utf-8 value");
            records.push((record.offset(), value));
        }
    }
    consumer.close().await.expect("consumer close");
    records
}

/// Broker ids reported by `DescribeCluster`.
async fn described_broker_ids(admin: &dyn Admin) -> Option<BTreeSet<i32>> {
    let nodes = admin.describe_cluster().nodes().get().await.ok()?;
    Some(nodes.iter().map(|node| node.id()).collect())
}

/// `(leader, isr)` of partition 0 of `topic`.
async fn leader_and_isr(admin: &dyn Admin, topic: &str) -> Option<(Option<i32>, BTreeSet<i32>)> {
    let described = admin
        .describe_topics_with_topics_options(
            TopicCollection::of_topic_names(vec![topic.to_string()]),
            DescribeTopicsOptions::new(),
        )
        .all_topic_names()
        .expect("described by name")
        .get()
        .await
        .ok()?;
    let partition = described.get(topic)?.partitions().first()?.clone();
    Some((
        partition.leader().map(|node| node.id()),
        partition.isr().iter().map(|node| node.id()).collect(),
    ))
}

async fn wait_for_described_brokers(admin: &dyn Admin, expected: &BTreeSet<i32>) {
    wait_until_true_with_timeout(
        || async { described_broker_ids(admin).await.as_ref() == Some(expected) },
        &format!("DescribeCluster never reported exactly brokers {expected:?}"),
        PROPAGATION_WAIT_MS,
        DEFAULT_PAUSE_MS,
    )
    .await;
}

async fn wait_for_isr(admin: &dyn Admin, topic: &str, expected: &BTreeSet<i32>) {
    wait_until_true_with_timeout(
        || async { leader_and_isr(admin, topic).await.is_some_and(|(_, isr)| isr == *expected) },
        &format!("ISR of {topic}-0 never became {expected:?}"),
        PROPAGATION_WAIT_MS,
        DEFAULT_PAUSE_MS,
    )
    .await;
}

/// On a dedicated `KRAFT` cluster (3 brokers, 1 controller): stopping the
/// partition leader removes it from `DescribeCluster` and the ISR while the
/// cluster stays writable and the controller keeps serving metadata writes;
/// restarting it brings it back under the same id and address, and the ISR
/// recovers.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_and_start_broker_kraft_three_brokers() {
    let mut ctx = TestContext::new(ClusterConfig::kraft_dedicated(3, 1)).await;
    let cluster = ctx.cluster();
    assert_eq!(cluster.cluster_type(), Type::Kraft);
    let all_brokers = BTreeSet::from([0, 1, 2]);
    assert_eq!(cluster.broker_ids(), all_brokers);
    assert_eq!(cluster.alive_broker_ids(), all_brokers);
    assert_eq!(cluster.controller_ids(), BTreeSet::from([3000]));
    assert_eq!(cluster.broker_bound_ports().len(), 3);
    cluster.wait_for_ready_brokers().await;

    let topic = ctx.topic("lifecycle_rf3");
    let later_topic = ctx.topic("lifecycle_created_while_down");
    let bootstrap = ctx.bootstrap_servers().to_string();
    let admin = admin(&bootstrap);
    create_topic(&admin, &topic, 1, 3).await;
    wait_for_isr(&admin, &topic, &all_brokers).await;

    let before = producer(&bootstrap);
    send_all(&before, &topic, 0..10).await;
    before.close().await.expect("producer close");

    // Stop the partition leader, so the restart also exercises leader failover.
    let (leader, _) = leader_and_isr(&admin, &topic).await.expect("describe topic");
    let victim = leader.expect("partition has a leader");
    let victim_address = ctx.cluster().broker_bootstrap_servers(victim);
    ctx.cluster().shutdown_broker(victim).await;

    let survivors: BTreeSet<i32> = all_brokers.iter().copied().filter(|id| *id != victim).collect();
    assert_eq!(ctx.cluster().alive_broker_ids(), survivors);
    wait_for_described_brokers(&admin, &survivors).await;
    wait_for_isr(&admin, &topic, &survivors).await;
    ctx.cluster().wait_for_ready_brokers().await;

    // The dedicated controller keeps the quorum: metadata writes still succeed.
    create_topic(&admin, &later_topic, 1, 2).await;

    // And the data path still works with one broker down.
    let during = producer(&bootstrap);
    send_all(&during, &topic, 10..20).await;
    during.close().await.expect("producer close");
    let consumed = consume_all(&bootstrap, &topic, 20).await;
    assert_eq!(consumed.len(), 20, "consumed {consumed:?}");

    ctx.cluster().start_broker(victim).await;
    assert_eq!(ctx.cluster().alive_broker_ids(), all_brokers);
    ctx.cluster().wait_for_ready_brokers().await;
    wait_for_described_brokers(&admin, &all_brokers).await;
    wait_for_isr(&admin, &topic, &all_brokers).await;

    // Same id and same advertised address as before the restart.
    let nodes = admin.describe_cluster().nodes().get().await.expect("describe cluster");
    let restarted = nodes.iter().find(|node| node.id() == victim).expect("restarted broker listed");
    assert_eq!(format!("{}:{}", restarted.host(), restarted.port()), victim_address);

    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// On a dedicated single-broker `KRAFT` cluster: a restarted broker keeps its
/// log (with one replica, lost data could not be re-replicated), a producer
/// created before the shutdown reconnects to the same bootstrap address, and
/// the SSL / SASL_PLAINTEXT / SASL_SSL listeners of the broker-only node serve
/// clients after the restart.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_and_start_single_broker_keeps_data_and_address() {
    let mut ctx = TestContext::new(ClusterConfig::kraft_dedicated(1, 1)).await;
    assert_eq!(ctx.cluster().broker_ids(), BTreeSet::from([0]));
    let topic = ctx.topic("lifecycle_rf1");
    let bootstrap = ctx.bootstrap_servers().to_string();
    assert_eq!(bootstrap, ctx.cluster().broker_bootstrap_servers(0));

    let admin = admin(&bootstrap);
    create_topic(&admin, &topic, 1, 1).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;

    let producer = producer(&bootstrap);
    send_all(&producer, &topic, 0..10).await;

    ctx.cluster().shutdown_broker(0).await;
    assert!(ctx.cluster().alive_broker_ids().is_empty());
    ctx.cluster().start_broker(0).await;
    assert_eq!(ctx.cluster().alive_broker_ids(), BTreeSet::from([0]));
    ctx.cluster().wait_for_ready_brokers().await;

    // The pre-shutdown producer reconnects on the unchanged address.
    send_all(&producer, &topic, 10..20).await;
    producer.close().await.expect("producer close");

    // Every client listener works on a broker-only node, also after a restart.
    let ca = ctx.ca_cert_pem().to_string();
    let sasl_jaas = format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{SASL_USERNAME}\" password=\"{SASL_PASSWORD}\";"
    );
    let ssl = vec![
        ("ssl.truststore.certificates", ca.clone()),
        // The client connects via 127.0.0.1, which the broker cert does not name.
        ("ssl.endpoint.identification.algorithm", String::new()),
    ];
    let sasl = vec![("sasl.mechanism", "PLAIN".to_string()), ("sasl.jaas.config", sasl_jaas)];
    let listeners = [
        (ctx.ssl_bootstrap_servers().to_string(), "SSL", ssl.clone()),
        (
            ctx.sasl_plaintext_bootstrap_servers().to_string(),
            "SASL_PLAINTEXT",
            sasl.clone(),
        ),
        (ctx.sasl_ssl_bootstrap_servers().to_string(), "SASL_SSL", [ssl, sasl].concat()),
    ];
    for (offset, (listener_bootstrap, protocol, settings)) in (20..).zip(listeners) {
        let mut security = settings;
        security.push(("security.protocol", protocol.to_string()));
        let secured = producer_with_security(&listener_bootstrap, security);
        send_all(&secured, &topic, offset..offset + 1).await;
        secured.close().await.expect("producer close");
    }

    let consumed = consume_all(&bootstrap, &topic, 23).await;
    let expected: Vec<(i64, String)> = (0..23).map(|i| (i as i64, format!("value {i}"))).collect();
    assert_eq!(consumed, expected, "records written before the restart must survive it");
}

/// Stopping a broker of a pooled cluster is refused: it would leak into every
/// other test sharing that cluster.
#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "broker lifecycle requires a dedicated cluster")]
async fn test_shutdown_broker_rejects_pooled_cluster() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let broker = *ctx.cluster().broker_ids().first().expect("one broker");
    ctx.cluster().shutdown_broker(broker).await;
}
