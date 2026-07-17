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

//! Integration tests for the `KafkaAdminClient` log-directory RPCs against a
//! real Kafka 4.2.0 broker.
//!
//! Mirrors the describeLogDirs / describeReplicaLogDirs / alterReplicaLogDirs
//! scenarios in Java's `KafkaAdminClientIntegrationTest`, exercising the real
//! network engine end to end rather than the `MockClient` unit-test harness.
//!
//! # Single- vs multi-log-dir brokers
//!
//! A genuine cross-log-directory replica move requires a broker configured with
//! more than one log directory. [`test_alter_replica_log_dirs_cross_dir_move`]
//! starts a dedicated single broker with two `KAFKA_LOG_DIRS` to exercise the
//! real move; the default cluster fixture has a single log directory, so the
//! move-to-unknown-directory case there only asserts the expected rejection
//! error round-trips.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, AlterReplicaLogDirsOptions, CreateTopicsOptions, DescribeClusterOptions,
    DescribeLogDirsOptions, DescribeReplicaLogDirsOptions, NewTopic, new_admin_client,
};
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::TopicPartitionReplica;
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::ByteArraySerializer;
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

/// Produce `num` records to `(topic, partition)`, waiting for the broker acks so
/// the partition has a non-zero on-disk size.
async fn produce_records(bootstrap: &str, topic: &str, partition: i32, num: usize) {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-log-dirs-producer".to_string()),
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

/// Returns the id of the first node reported by `describe_cluster`.
async fn first_broker_id(admin: &dyn Admin) -> i32 {
    let nodes = admin
        .describe_cluster(DescribeClusterOptions::new())
        .nodes()
        .get()
        .await
        .expect("describe cluster nodes");
    nodes.first().expect("at least one broker").id()
}

#[tokio::test]
async fn test_describe_log_dirs_returns_dirs_with_replica_sizes() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_describe_log_dirs");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");
    produce_records(ctx.bootstrap_servers(), &topic, 0, 10).await;

    let broker_id = first_broker_id(admin.as_ref()).await;
    let result = admin.describe_log_dirs(&[broker_id], DescribeLogDirsOptions::new());
    let descriptions = result.descriptions()[&broker_id]
        .get()
        .await
        .expect("describe log dirs for broker");
    assert!(!descriptions.is_empty(), "broker should report at least one log directory");

    // Exactly one log directory holds our produced partition; find it.
    let tp = TopicPartition::new(topic.clone(), 0);
    let (log_dir, description) = descriptions
        .iter()
        .find(|(_, d)| d.replica_infos().contains_key(&tp))
        .expect("a log directory should contain the produced partition");
    assert!(!log_dir.is_empty(), "log directory path should be non-empty");
    assert!(description.error().is_none(), "healthy log directory has no error");
    let replica_info = &description.replica_infos()[&tp];
    assert!(replica_info.size() >= 0, "replica size should be reported");
    assert!(!replica_info.is_future(), "current replica is not a future replica");

    // `all_descriptions` succeeds when every requested broker responds.
    let all = result.all_descriptions().get().await.expect("all descriptions");
    assert!(all.contains_key(&broker_id));

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_describe_replica_log_dirs_returns_current_dir() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_describe_replica_log_dirs");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");
    produce_records(ctx.bootstrap_servers(), &topic, 0, 5).await;

    let broker_id = first_broker_id(admin.as_ref()).await;
    let replica = TopicPartitionReplica::new(topic.clone(), 0, broker_id);
    let result = admin.describe_replica_log_dirs(std::slice::from_ref(&replica), DescribeReplicaLogDirsOptions::new());
    let info = result.values()[&replica].get().await.expect("describe replica log dirs");

    assert!(
        info.current_replica_log_dir().is_some_and(|d| !d.is_empty()),
        "replica should report its current log directory"
    );
    // No move is in progress, so there is no future log directory.
    assert_eq!(info.future_replica_log_dir(), None);
    assert_eq!(info.future_replica_offset_lag(), -1);

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_alter_replica_log_dirs_nonexistent_dir_errors() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_alter_replica_log_dirs_bad_dir");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");

    let broker_id = first_broker_id(admin.as_ref()).await;
    let replica = TopicPartitionReplica::new(topic.clone(), 0, broker_id);
    // The default single-broker fixture has one log directory, so moving to a
    // directory the broker does not manage is rejected. A genuine cross-dir
    // move is exercised by `test_alter_replica_log_dirs_cross_dir_move`.
    let assignment = HashMap::from([(replica.clone(), "/nonexistent/kafka-logs".to_string())]);
    let result = admin.alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new());
    let err = result.values()[&replica]
        .get()
        .await
        .expect_err("moving to an unknown dir must fail");
    assert!(
        matches!(
            err.error(),
            Errors::LogDirNotFound | Errors::KafkaStorageError | Errors::ReplicaNotAvailable
        ),
        "unexpected error for unknown log dir: {:?}",
        err.error()
    );

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

#[tokio::test]
async fn test_alter_replica_log_dirs_cross_dir_move() {
    // A dedicated single broker with two log directories, so an actual
    // cross-directory replica move is possible.
    let props = BTreeMap::from([("KAFKA_LOG_DIRS".to_string(), "/tmp/kafka-logs-0,/tmp/kafka-logs-1".to_string())]);
    let mut ctx = TestContext::new(ClusterConfig::with_properties(props)).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_alter_replica_log_dirs_move");
    admin
        .create_topics(&[NewTopic::new(topic.clone(), 1, 1)], CreateTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("create topic");
    produce_records(ctx.bootstrap_servers(), &topic, 0, 10).await;

    let broker_id = first_broker_id(admin.as_ref()).await;
    let replica = TopicPartitionReplica::new(topic.clone(), 0, broker_id);

    // Discover the replica's current directory, then pick the other one.
    let current = admin
        .describe_replica_log_dirs(std::slice::from_ref(&replica), DescribeReplicaLogDirsOptions::new())
        .values()[&replica]
        .get()
        .await
        .expect("describe replica log dirs")
        .current_replica_log_dir()
        .expect("current log dir")
        .to_string();
    let target = if current == "/tmp/kafka-logs-0" {
        "/tmp/kafka-logs-1"
    } else {
        "/tmp/kafka-logs-0"
    };

    let assignment = HashMap::from([(replica.clone(), target.to_string())]);
    let result = admin.alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new());
    result.values()[&replica].get().await.expect("cross-dir move accepted");

    // After the move is requested, describe should show the target as either the
    // (in-progress) future directory or the (completed) current directory.
    let info = admin
        .describe_replica_log_dirs(std::slice::from_ref(&replica), DescribeReplicaLogDirsOptions::new())
        .values()[&replica]
        .get()
        .await
        .expect("describe replica log dirs after move");
    let now_here = info.current_replica_log_dir() == Some(target);
    let moving_here = info.future_replica_log_dir() == Some(target);
    assert!(
        now_here || moving_here,
        "expected replica to be on or moving to {target}, got current={:?} future={:?}",
        info.current_replica_log_dir(),
        info.future_replica_log_dir()
    );

    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}
