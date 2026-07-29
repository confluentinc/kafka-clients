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

//! MANUAL SMOKE TEST — NOT part of the CI-checked suite.
//!
//! Walks through every Tier 1 `Admin` RPC, in order, against a real 3-broker
//! Kafka 4.2.0 cluster (each broker with 2 log dirs) started via Docker
//! (testcontainers), with verbose `println!` narration at every step so a
//! human can visually confirm each API call really talked to a real broker
//! and got a real result back.
//!
//! This file is temporary scratch tooling, not meant to be committed:
//!   - it is registered with a single `mod admin_smoke_test_manual;` line
//!     added to `tests/integration/main.rs` — revert that line
//!     (`git checkout -- tests/integration/main.rs`) when done.
//!   - delete this file (`rm tests/integration/admin_smoke_test_manual.rs`)
//!     when done.
//!
//! Run with:
//!   cargo test --features integration-tests --test integration \
//!     admin_smoke_test_manual -- --nocapture --test-threads=1

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use confluent_kafka::admin::{
    Admin, AdminClientConfig, AlterConfigOp, AlterConfigsOptions, AlterPartitionReassignmentsOptions,
    AlterReplicaLogDirsOptions, ConfigEntry, CreatePartitionsOptions, CreateTopicsOptions, DeleteRecordsOptions,
    DeleteTopicsOptions, DescribeClusterOptions, DescribeConfigsOptions, DescribeLogDirsOptions,
    DescribeReplicaLogDirsOptions, DescribeTopicsOptions, ElectLeadersOptions, ListConfigResourcesOptions,
    ListOffsetsOptions, ListPartitionReassignmentsOptions, ListTopicsOptions, NewPartitionReassignment,
    NewPartitions, NewTopic, OffsetSpec, OpType, RecordsToDelete, new_admin_client,
};
use confluent_kafka::common::config::{ConfigResource, ConfigResourceType};
use confluent_kafka::common::serialization::{Deserializer, StringSerializer};
use confluent_kafka::common::{ElectionType, KafkaError, TopicCollection, TopicPartition, TopicPartitionReplica};
use confluent_kafka::consumer::{ConsumerConfig, new_consumer};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// Local string deserializer (mirrors the same inline pattern used in
/// `consumer_test.rs` — there's no shared `StringDeserializer` yet).
struct StringDeserializer;

impl Deserializer<String> for StringDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
        String::from_utf8(data.to_vec()).map_err(|e| KafkaError::serialization(format!("invalid utf-8: {e}")))
    }
}

fn section(n: &str, title: &str) {
    println!("\n========================================================");
    println!(" [{n}] {title}");
    println!("========================================================");
}

fn admin_for(bootstrap: &str) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "smoke-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    new_admin_client(AdminClientConfig::from_properties(&props).expect("valid admin config")).expect("admin client")
}

fn producer_config(bootstrap: &str) -> ProducerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "smoke-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "0".to_string()),
    ]);
    ProducerConfig::from_properties(&props).expect("valid producer config")
}

fn consumer_config(bootstrap: &str, group_id: &str) -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "smoke-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    ConsumerConfig::from_properties(&props).expect("valid consumer config")
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_smoke_test() {
    println!("\n########################################################");
    println!("# ADMIN API MANUAL SMOKE TEST — starting");
    println!("# Exercises every Tier 1 Admin RPC against a real broker.");
    println!("########################################################");

    section("SETUP", "Starting a 3-broker Kafka 4.2.0 cluster via Docker (testcontainers)");
    println!("Each broker gets 2 log dirs (/tmp/kafka-logs-0, /tmp/kafka-logs-1) so alterReplicaLogDirs is exercisable.");
    let mut cluster_cfg = ClusterConfig::with_brokers(3);
    cluster_cfg
        .server_properties
        .insert("KAFKA_LOG_DIRS".to_string(), "/tmp/kafka-logs-0,/tmp/kafka-logs-1".to_string());
    let mut ctx = TestContext::new(cluster_cfg).await;
    println!("Cluster is up. Bootstrap servers: {}", ctx.bootstrap_servers());
    println!("(Run `docker ps --filter ancestor=apache/kafka:4.2.0` in another terminal right now to see it.)");

    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("smoke_test_topic");
    println!("Using topic name: {topic}");

    // ---------------------------------------------------------------
    section("1/19", "Admin.createTopics — create the smoke-test topic");
    let result = admin.create_topics(&[NewTopic::new(topic.clone(), 3, 3)], CreateTopicsOptions::new());
    result.all().get().await.expect("createTopics failed");
    println!("OK: created '{topic}' with 3 partitions, replication factor 3");

    // ---------------------------------------------------------------
    section("2/19", "Admin.listTopics — confirm it's visible");
    let names = admin.list_topics(ListTopicsOptions::new()).names().get().await.expect("listTopics failed");
    println!("OK: {} topic(s) visible cluster-wide; our topic present = {}", names.len(), names.contains(&topic));

    // ---------------------------------------------------------------
    section("3/19", "Admin.describeTopics — inspect partition/replica layout");
    let described = admin
        .describe_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DescribeTopicsOptions::new())
        .all_topic_names()
        .expect("by name")
        .get()
        .await
        .expect("describeTopics failed");
    let desc = &described[&topic];
    println!("OK: '{}' has {} partition(s):", desc.name(), desc.partitions().len());
    for p in desc.partitions() {
        let replicas: Vec<i32> = p.replicas().iter().map(|n| n.id()).collect();
        println!("  partition {}: leader={:?} replicas={:?}", p.partition(), p.leader().map(|n| n.id()), replicas);
    }

    // ---------------------------------------------------------------
    section("PRODUCE", "Producing 20 messages to partition 0 (real Producer, not Admin)");
    let producer: KafkaProducer<String, String> =
        KafkaProducer::from_config(producer_config(ctx.bootstrap_servers()), Box::new(StringSerializer), Box::new(StringSerializer))
            .expect("build producer");
    for i in 0..20 {
        let rec = ProducerRecord::with_partition(
            topic.clone(),
            Some(0),
            Some(format!("key-{i}")),
            Some(format!("value-{i}")),
        )
        .expect("build record");
        let fut = producer.send(rec).await.expect("send failed");
        let meta = fut.get_timeout(Duration::from_secs(30)).await.expect("ack failed");
        if i == 0 || i == 19 {
            println!("  produced record {i}: partition={} offset={}", meta.partition(), meta.offset());
        }
    }
    producer.close().await.expect("producer close failed");
    println!("OK: produced 20 records to {topic}-0");

    // ---------------------------------------------------------------
    section("CONSUME", "Consuming those 20 messages back (real Consumer, not Admin)");
    let mut consumer = new_consumer::<String, String>(
        consumer_config(ctx.bootstrap_servers(), &ctx.group_id("smoke")),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )
    .expect("new_consumer failed");
    consumer.subscribe(vec![topic.clone()]).await.expect("subscribe failed");
    let mut collected = 0usize;
    let start = Instant::now();
    while collected < 20 && start.elapsed() < Duration::from_secs(30) {
        let records = consumer.poll(Duration::from_secs(5)).await.expect("poll failed");
        for r in &records {
            collected += 1;
            if collected <= 3 || collected == 20 {
                println!(
                    "  consumed #{collected}: partition={} offset={} key={:?} value={:?}",
                    r.partition(),
                    r.offset(),
                    r.key(),
                    r.value()
                );
            }
        }
    }
    consumer.close().await.expect("consumer close failed");
    println!("OK: consumed {collected}/20 records");

    // ---------------------------------------------------------------
    section("4/19", "Admin.describeCluster — nodes, controller, cluster id");
    let cluster_result = admin.describe_cluster(DescribeClusterOptions::new());
    let nodes = cluster_result.nodes().get().await.expect("describe_cluster nodes failed");
    let controller = cluster_result.controller().get().await.expect("describe_cluster controller failed");
    let cluster_id = cluster_result.cluster_id().get().await.expect("describe_cluster cluster_id failed");
    println!("OK: cluster_id={cluster_id}");
    println!("  nodes: {:?}", nodes.iter().map(|n| n.id()).collect::<Vec<_>>());
    println!("  controller: {:?}", controller.map(|n| n.id()));
    let broker_id = nodes.first().expect("at least one broker").id();

    // ---------------------------------------------------------------
    section("5/19", "Admin.describeConfigs — topic config (retention.ms etc.)");
    let topic_resource = ConfigResource::new(ConfigResourceType::Topic, topic.clone());
    let describe_result = admin.describe_configs(&[topic_resource.clone()], DescribeConfigsOptions::new());
    let config = describe_result.values()[&topic_resource].get().await.expect("describeConfigs failed");
    let entries: Vec<_> = config.entries().collect();
    println!("OK: topic '{}' has {} config entries; sample:", topic, entries.len());
    for e in entries.iter().take(3) {
        println!("  {} = {:?}", e.name(), e.value());
    }

    // ---------------------------------------------------------------
    section("6/19", "Admin.incrementalAlterConfigs — SET retention.ms=123456789, then verify");
    let set_op = AlterConfigOp::new(ConfigEntry::new("retention.ms".to_string(), Some("123456789".to_string())), OpType::Set);
    let mut alter_configs = HashMap::new();
    alter_configs.insert(topic_resource.clone(), vec![set_op]);
    admin
        .incremental_alter_configs(&alter_configs, AlterConfigsOptions::new())
        .all()
        .get()
        .await
        .expect("incrementalAlterConfigs failed");
    let after = admin
        .describe_configs(&[topic_resource.clone()], DescribeConfigsOptions::new())
        .values()[&topic_resource]
        .get()
        .await
        .expect("describeConfigs (after alter) failed");
    let retention = after.entries().find(|e| e.name() == "retention.ms").and_then(|e| e.value().map(|v| v.to_string()));
    println!("OK: retention.ms is now {retention:?} (expected Some(\"123456789\"))");

    // ---------------------------------------------------------------
    section("7/19", "Admin.listConfigResources — every config resource in the cluster");
    let resources = admin
        .list_config_resources(&HashSet::new(), ListConfigResourcesOptions::new())
        .all()
        .get()
        .await
        .expect("listConfigResources failed");
    println!("OK: {} config resource(s), including our topic = {}", resources.len(), resources.contains(&topic_resource));

    // ---------------------------------------------------------------
    section("8/19", "Admin.createPartitions — increase partition count 3 -> 5");
    let mut new_partitions = HashMap::new();
    new_partitions.insert(topic.clone(), NewPartitions::increase_to(5));
    admin
        .create_partitions(&new_partitions, CreatePartitionsOptions::new())
        .all()
        .get()
        .await
        .expect("createPartitions failed");
    let after_desc = admin
        .describe_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DescribeTopicsOptions::new())
        .all_topic_names()
        .expect("by name")
        .get()
        .await
        .expect("describeTopics (after createPartitions) failed");
    println!("OK: '{}' now has {} partitions (was 3)", topic, after_desc[&topic].partitions().len());

    // ---------------------------------------------------------------
    section("9/19", "Admin.listOffsets — earliest/latest for the produced partition");
    let tp = TopicPartition::new(topic.clone(), 0);
    let earliest = admin
        .list_offsets(&HashMap::from([(tp.clone(), OffsetSpec::earliest())]), ListOffsetsOptions::new())
        .partition_result(&tp)
        .expect("earliest offset was attempted")
        .get()
        .await
        .expect("listOffsets earliest failed");
    let latest = admin
        .list_offsets(&HashMap::from([(tp.clone(), OffsetSpec::latest())]), ListOffsetsOptions::new())
        .partition_result(&tp)
        .expect("latest offset was attempted")
        .get()
        .await
        .expect("listOffsets latest failed");
    println!("OK: {}-0 earliest offset={} latest offset={} (expected 0 and 20)", topic, earliest.offset(), latest.offset());

    // ---------------------------------------------------------------
    section("10/19", "Admin.describeLogDirs — per-broker log directory contents");
    let log_dirs_result = admin.describe_log_dirs(&[broker_id], DescribeLogDirsOptions::new());
    let dirs = log_dirs_result.descriptions()[&broker_id].get().await.expect("describeLogDirs failed");
    println!("OK: broker {broker_id} reports {} log dir(s)", dirs.len());
    for (path, d) in dirs.iter() {
        println!("  {path}: {} replica(s)", d.replica_infos().len());
    }

    // ---------------------------------------------------------------
    section("11/19", "Admin.describeReplicaLogDirs — where partition 0's replica currently lives");
    let replica = TopicPartitionReplica::new(topic.clone(), 0, broker_id);
    let replica_info = admin
        .describe_replica_log_dirs(std::slice::from_ref(&replica), DescribeReplicaLogDirsOptions::new())
        .values()[&replica]
        .get()
        .await
        .expect("describeReplicaLogDirs failed");
    let current_dir = replica_info.current_replica_log_dir().expect("current dir").to_string();
    println!("OK: replica ({}, 0, broker {}) currently lives in {}", topic, broker_id, current_dir);

    // ---------------------------------------------------------------
    section("12/19", "Admin.alterReplicaLogDirs — move that replica to the other log dir");
    let target_dir = if current_dir == "/tmp/kafka-logs-0" { "/tmp/kafka-logs-1" } else { "/tmp/kafka-logs-0" };
    let assignment = HashMap::from([(replica.clone(), target_dir.to_string())]);
    admin
        .alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new())
        .values()[&replica]
        .get()
        .await
        .expect("alterReplicaLogDirs failed");
    println!("OK: requested move to {target_dir}; polling describeReplicaLogDirs until it lands there...");
    for _ in 0..30 {
        let info = admin
            .describe_replica_log_dirs(std::slice::from_ref(&replica), DescribeReplicaLogDirsOptions::new())
            .values()[&replica]
            .get()
            .await
            .expect("describeReplicaLogDirs (poll) failed");
        if info.current_replica_log_dir() == Some(target_dir) {
            println!("  confirmed: replica now in {target_dir}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ---------------------------------------------------------------
    section("13/19", "Admin.electLeaders — preferred-leader election (may be a no-op if already preferred)");
    let elect_result = admin.elect_leaders(ElectionType::Preferred, Some(HashSet::from([tp.clone()])), ElectLeadersOptions::new());
    let errors = elect_result.partitions().get().await.expect("electLeaders failed");
    println!("OK: electLeaders returned {} result(s); error for {}-0 = {:?}", errors.len(), topic, errors.get(&tp));

    // ---------------------------------------------------------------
    section("14/19", "Admin.listPartitionReassignments — before (expect none in flight)");
    let before = admin
        .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
        .reassignments()
        .get()
        .await
        .expect("listPartitionReassignments (before) failed");
    println!("OK: {} reassignment(s) currently in flight", before.len());

    // ---------------------------------------------------------------
    section("15/19", "Admin.alterPartitionReassignments — move partition 0 to a different broker set");
    // Rotate the 3 distinct broker ids so the new leader (first element)
    // differs from the current one, without repeating any broker.
    let mut rotated_ids: Vec<i32> = nodes.iter().map(|n| n.id()).collect();
    rotated_ids.rotate_left(1);
    let other_broker = rotated_ids[0];
    let new_assignment =
        NewPartitionReassignment::new(rotated_ids.clone()).expect("build reassignment (need 3 distinct replicas)");
    let mut reassignments = HashMap::new();
    reassignments.insert(tp.clone(), Some(new_assignment));
    admin
        .alter_partition_reassignments(&reassignments, AlterPartitionReassignmentsOptions::new())
        .values()[&tp]
        .get()
        .await
        .expect("alterPartitionReassignments failed");
    println!("OK: requested reassignment of {}-0 to lead with broker {}", topic, other_broker);

    // ---------------------------------------------------------------
    section("16/19", "Admin.listPartitionReassignments — after (poll until it completes)");
    for _ in 0..30 {
        let after = admin
            .list_partition_reassignments(Some(HashSet::from([tp.clone()])), ListPartitionReassignmentsOptions::new())
            .reassignments()
            .get()
            .await
            .expect("listPartitionReassignments (after) failed");
        if after.is_empty() {
            println!("  confirmed: reassignment completed (no longer in flight)");
            break;
        }
        println!("  still in flight: {:?}", after.get(&tp).map(|r| (r.replicas(), r.adding_replicas(), r.removing_replicas())));
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // ---------------------------------------------------------------
    section("17/19", "Admin.deleteRecords — delete everything before offset 10");
    let mut to_delete = HashMap::new();
    to_delete.insert(tp.clone(), RecordsToDelete::before_offset(10));
    let delete_result = admin.delete_records(&to_delete, DeleteRecordsOptions::new());
    let low_watermark = delete_result.low_watermarks()[&tp]
        .get()
        .await
        .expect("deleteRecords failed")
        .low_watermark();
    println!("OK: deleted records before offset 10; new low watermark = {low_watermark} (expected 10)");

    // ---------------------------------------------------------------
    section("18/19", "Admin.deleteTopics — cleanup");
    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("deleteTopics failed");
    println!("OK: deleted topic '{topic}'");

    // ---------------------------------------------------------------
    section("19/19", "Admin.close — shut down the admin client");
    admin.close(Duration::from_secs(5)).await;
    println!("OK: admin client closed");

    ctx.cleanup().await;
    println!("\n########################################################");
    println!("# ALL 15 Admin RPCs + produce/consume exercised successfully.");
    println!("# Cluster torn down. Smoke test complete.");
    println!("########################################################\n");
}
