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

//! Integration tests for the `KafkaAdminClient` elections / reassignments /
//! offsets RPCs against a real Kafka 4.2.0 broker.
//!
//! Mirrors the electLeaders / alterPartitionReassignments /
//! listPartitionReassignments / listOffsets scenarios of Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine
//! (including the `AdminApiDriver` partition-leader lookup for `listOffsets`)
//! end to end rather than the `MockClient` unit-test harness.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, AlterPartitionReassignmentsOptions, CreateTopicsOptions, DescribeClusterOptions,
    ElectLeadersOptions, ListOffsetsOptions, ListPartitionReassignmentsOptions, NewPartitionReassignment, NewTopic,
    OffsetSpec, new_admin_client,
};
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::{ElectionType, TopicPartition};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// Build an admin client pointed at the cluster's PLAINTEXT listener.
fn admin_for(bootstrap_servers: &str) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "integration-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    let config = AdminClientConfig::from_properties(&props).expect("valid admin config");
    new_admin_client(config).expect("admin client")
}

/// Produce `num` records to `(topic, partition)`, waiting for the broker acks.
async fn produce_records(bootstrap: &str, topic: &str, partition: i32, num: usize) {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-offsets-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
    ]);
    let config = ProducerConfig::from_properties(&props).expect("valid producer config");
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> =
        KafkaProducer::from_config(config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer))
            .expect("build producer");
    let mut last = None;
    for i in 0..num {
        let record = ProducerRecord::with_partition(
            topic.to_string(),
            Some(partition),
            Some(format!("key {i}").into_bytes()),
            Some(format!("value {i}").into_bytes()),
        )
        .expect("build record");
        last = Some(
            <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
                .await
                .expect("send"),
        );
    }
    producer.flush().await.expect("flush");
    if let Some(f) = last {
        f.get_timeout(Duration::from_secs(30)).await.expect("last send");
    }
    producer.close().await.expect("producer close");
}

/// `listOffsets` for a produced topic returns the expected earliest / latest /
/// max-timestamp offsets. Exercises the `AdminApiDriver` +
/// `PartitionLeaderStrategy` lookup→fulfillment path end to end.
#[tokio::test]
async fn test_list_offsets_earliest_latest_max_timestamp() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_list_offsets");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");
    let num_records = 10;
    produce_records(ctx.bootstrap_servers(), &topic, 0, num_records).await;

    let tp = TopicPartition::new(topic.clone(), 0);

    // Earliest: offset 0.
    let earliest = admin
        .list_offsets(
            &HashMap::from([(tp.clone(), OffsetSpec::earliest())]),
            ListOffsetsOptions::new(),
        )
        .partition_result(&tp)
        .unwrap()
        .get()
        .await
        .expect("earliest offset");
    assert_eq!(earliest.offset(), 0, "earliest offset should be 0");

    // Latest: log end offset == number of produced records.
    let latest = admin
        .list_offsets(&HashMap::from([(tp.clone(), OffsetSpec::latest())]), ListOffsetsOptions::new())
        .partition_result(&tp)
        .unwrap()
        .get()
        .await
        .expect("latest offset");
    assert_eq!(
        latest.offset(),
        num_records as i64,
        "latest offset should equal the record count"
    );

    // MaxTimestamp: the offset of the record with the largest timestamp. Records
    // produced back-to-back can share the same millisecond timestamp, in which
    // case the broker returns the *earliest* offset carrying that max timestamp,
    // so the exact offset is timing-dependent — assert only that a valid offset
    // in range and a real timestamp are returned (this still drives the v7+
    // MAX_TIMESTAMP spec path end to end).
    let max_ts = admin
        .list_offsets(
            &HashMap::from([(tp.clone(), OffsetSpec::max_timestamp())]),
            ListOffsetsOptions::new(),
        )
        .partition_result(&tp)
        .unwrap()
        .get()
        .await
        .expect("max timestamp offset");
    assert!(
        max_ts.offset() >= 0 && max_ts.offset() < num_records as i64,
        "max-timestamp offset should be a valid offset in [0, {num_records})"
    );
    assert!(max_ts.timestamp() >= 0, "max-timestamp offset carries a real timestamp");

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// `electLeaders(PREFERRED)` on a healthy topic returns a per-partition result.
/// On a single-broker cluster the preferred replica is already the leader, so
/// the broker reports `ELECTION_NOT_NEEDED`; the important behavior under test
/// is that the request round-trips and a per-partition result is returned.
#[tokio::test]
async fn test_elect_preferred_leaders() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_elect_leaders");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");

    let tp = TopicPartition::new(topic.clone(), 0);
    let result = admin.elect_leaders(
        ElectionType::Preferred,
        Some([tp.clone()].into_iter().collect()),
        ElectLeadersOptions::new(),
    );
    let partitions = result.partitions().get().await.expect("elect leaders result");
    // The election was attempted for our partition.
    let outcome = partitions.get(&tp).expect("a result for the requested partition");
    // Either the election succeeded (None) or it was not needed because the
    // preferred replica is already the leader (single-broker cluster).
    if let Some(error) = outcome {
        assert_eq!(
            error.error(),
            Errors::ElectionNotNeeded,
            "on a single-broker cluster the only expected error is ELECTION_NOT_NEEDED"
        );
    }

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// `alterPartitionReassignments` moves a partition's replica set on a
/// multi-broker cluster, then `listPartitionReassignments` reflects the
/// in-progress reassignment (or an empty map once it has already completed).
#[tokio::test]
async fn test_alter_and_list_partition_reassignments() {
    let mut ctx = TestContext::new(ClusterConfig::with_brokers(3)).await;
    let admin = admin_for(ctx.bootstrap_servers());

    // Discover the broker ids.
    let nodes = admin
        .describe_cluster(DescribeClusterOptions::new())
        .nodes()
        .get()
        .await
        .expect("describe cluster nodes");
    assert!(nodes.len() >= 2, "this test requires a multi-broker cluster");
    let broker_ids: Vec<i32> = nodes.iter().map(|n| n.id()).collect();

    let topic = ctx.topic("admin_reassignments");
    // Replication factor 1: a single replica we can move between brokers.
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");

    let tp = TopicPartition::new(topic.clone(), 0);

    // Find the current leader (its sole replica) and pick a different target.
    let described = admin
        .describe_topics(
            confluent_kafka::common::TopicCollection::of_topic_names(vec![topic.clone()]),
            confluent_kafka::admin::DescribeTopicsOptions::new(),
        )
        .all_topic_names()
        .unwrap()
        .get()
        .await
        .expect("describe topic");
    let current_leader = described[&topic].partitions()[0].leader().expect("partition has a leader").id();
    let target = *broker_ids.iter().find(|&&id| id != current_leader).expect("a different broker");

    // Initiate the reassignment to the target broker.
    let alter = admin.alter_partition_reassignments(
        &HashMap::from([(tp.clone(), Some(NewPartitionReassignment::new(vec![target]).unwrap()))]),
        AlterPartitionReassignmentsOptions::new(),
    );
    alter.values()[&tp].get().await.expect("reassignment initiated");

    // List the reassignments. A single RF-1 move may complete before we observe
    // it, so accept either an in-progress entry for our partition or an empty
    // map; the call itself must succeed without error.
    let reassignments = admin
        .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
        .reassignments()
        .get()
        .await
        .expect("list reassignments");
    if let Some(ongoing) = reassignments.get(&tp) {
        assert!(
            ongoing.replicas().contains(&target),
            "the in-progress reassignment targets the destination broker"
        );
    }

    // Eventually the partition's replica set reflects the move. Poll describe.
    let mut moved = false;
    for _ in 0..30 {
        let described = admin
            .describe_topics(
                confluent_kafka::common::TopicCollection::of_topic_names(vec![topic.clone()]),
                confluent_kafka::admin::DescribeTopicsOptions::new(),
            )
            .all_topic_names()
            .unwrap()
            .get()
            .await
            .expect("describe topic");
        let replicas: Vec<i32> = described[&topic].partitions()[0].replicas().iter().map(|n| n.id()).collect();
        if replicas == vec![target] {
            moved = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        moved,
        "the partition's replica set should eventually be the reassignment target"
    );

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}
