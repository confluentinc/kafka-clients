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

//! Integration tests for the admin log-directory RPCs against a real Kafka 4.2.0
//! broker.
//!
//! Mirrors the describeLogDirs / describeReplicaLogDirs / alterReplicaLogDirs
//! scenarios in Java's `KafkaAdminClientIntegrationTest`, exercising the real
//! network engine end to end rather than the `MockClient` unit-test harness.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and
//! the C FFI. With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion, and it drives the same production `Admin` trait against the same
//! broker as the single-backend tests these scenarios were converted from.
//!
//! # Single- vs multi-log-dir brokers
//!
//! A genuine cross-log-directory replica move requires a broker configured with
//! more than one log directory. [`alter_replica_log_dirs_cross_dir_move`] starts
//! a dedicated single broker with two `KAFKA_LOG_DIRS` to exercise the real move;
//! the default cluster fixture has a single log directory, so the
//! move-to-unknown-directory case there only asserts the expected rejection
//! error round-trips.
//!
//! # What a single PLAINTEXT node cannot reach
//!
//! Two states of the wire shape are unreachable on this fixture and are recorded
//! rather than silently untested:
//!
//!   - **`LogDirDescription.error`** — the value-level error of
//!     `admin_service.proto`'s envelope exception 3. The broker sets it per log
//!     directory from `LogDirFailureChannel`, i.e. only for a directory it has
//!     marked offline after an I/O failure (`DescribeLogDirsResponse` carries
//!     `KAFKA_STORAGE_ERROR` for those in `ReplicaManager.describeLogDirs`).
//!     Provoking it needs a broker whose disk fails mid-run. Every healthy
//!     directory reports `error == None`, which is what
//!     [`describe_log_dirs_returns_dirs_with_replica_sizes`] asserts, and all
//!     three servers carry the field.
//!   - **A per-broker error entry in `describeLogDirs`**, and any multi-broker
//!     fan-out. One broker means one entry, which always answers.
//!
//! `alterReplicaLogDirs` is not a cross-*broker* operation in Java either: the
//! request is routed to the broker that owns the replica
//! (`AlterReplicaLogDirsRequest` is per-broker), so "move a replica to another
//! broker" is not a state this RPC has. Reassignment does that, and is covered by
//! `admin_elections_reassignments_offsets_test`.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use confluent_kafka::admin::{
    AlterReplicaLogDirsOptions, DescribeClusterOptions, DescribeLogDirsOptions, DescribeReplicaLogDirsOptions,
};
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::TopicPartitionReplica;
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::admin_backend::{
    AdminBackend, ReplicaLogDirInfoView, admin_for, all_of, all_of_exactly, create_topic,
};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::multilanguage_admin_test;

/// Produce `num` records to `(topic, partition)`, waiting for the broker acks so
/// the partition has a non-zero on-disk size.
///
/// Always the *native* producer, whichever admin backend is under test: the
/// scenario needs bytes on the broker's disk, and which language wrote them is
/// not what these scenarios compare. `bootstrap` is therefore the host loopback
/// address, not the container listener.
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
async fn first_broker_id<B: AdminBackend>(admin: &B) -> i32 {
    admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: describe cluster: {e}", admin.name()))
        .nodes
        .first()
        .unwrap_or_else(|| panic!("{} backend: at least one broker", admin.name()))
        .id()
}

/// Describes one replica's log dirs, panicking with the backend's name on either
/// failure level.
async fn replica_info<B: AdminBackend>(admin: &B, replica: &TopicPartitionReplica) -> ReplicaLogDirInfoView {
    let described = admin
        .describe_replica_log_dirs(std::slice::from_ref(replica), DescribeReplicaLogDirsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: describe replica log dirs: {e}", admin.name()));
    described
        .get(replica)
        .unwrap_or_else(|| panic!("{} backend: {replica:?} missing from the result", admin.name()))
        .as_ref()
        .unwrap_or_else(|e| panic!("{} backend: describe replica log dirs: {e}", admin.name()))
        .clone()
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

async fn describe_log_dirs_returns_dirs_with_replica_sizes<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_describe_log_dirs");
    create_topic(&admin, &topic, 1, 1).await;
    produce_records(ctx.bootstrap_servers(), &topic, 0, 10).await;

    let broker_id = first_broker_id(&admin).await;
    let described = admin
        .describe_log_dirs(&[broker_id], DescribeLogDirsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe log dirs: {e}"));
    let descriptions = described[&broker_id]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe log dirs for broker: {e}"));
    assert!(
        !descriptions.is_empty(),
        "{backend} backend: broker should report at least one log directory"
    );

    // Exactly one log directory holds our produced partition; find it. This is
    // the nested level of the value — broker -> log dir -> replica info — which a
    // backend that flattened the map away could not answer.
    let tp = TopicPartition::new(topic.clone(), 0);
    let (log_dir, description) = descriptions
        .iter()
        .find(|(_, d)| d.replica_infos().contains_key(&tp))
        .unwrap_or_else(|| {
            panic!("{backend} backend: a log directory should contain the produced partition, got {descriptions:?}")
        });
    assert!(!log_dir.is_empty(), "{backend} backend: log directory path should be non-empty");
    // The log dir's own error (envelope exception 3) is absent for a healthy
    // directory; the set branch needs a disk failure and is unreachable here (see
    // the module docs).
    assert!(
        description.error().is_none(),
        "{backend} backend: healthy log directory has no error, got {:?}",
        description.error()
    );
    let info = &description.replica_infos()[&tp];
    assert!(
        info.size() >= 0,
        "{backend} backend: replica size should be reported, got {}",
        info.size()
    );
    assert!(!info.is_future(), "{backend} backend: current replica is not a future replica");

    // Java's `all_descriptions()` succeeds when every requested broker responded,
    // which for the resolved map is the `all_of` fold.
    all_of(&described).unwrap_or_else(|e| panic!("{backend} backend: all descriptions: {e}"));
    assert!(
        described.contains_key(&broker_id),
        "{backend} backend: the requested broker must be keyed in the result, got {:?}",
        described.keys().collect::<Vec<_>>()
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

async fn describe_replica_log_dirs_returns_current_dir<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_describe_replica_log_dirs");
    create_topic(&admin, &topic, 1, 1).await;
    produce_records(ctx.bootstrap_servers(), &topic, 0, 5).await;

    let broker_id = first_broker_id(&admin).await;
    let replica = TopicPartitionReplica::new(topic.clone(), 0, broker_id);
    let info = replica_info(&admin, &replica).await;

    assert!(
        info.current_replica_log_dir.as_deref().is_some_and(|d| !d.is_empty()),
        "{backend} backend: replica should report its current log directory, got {:?}",
        info.current_replica_log_dir
    );
    // No move is in progress, so there is no future log directory. Absent must
    // stay absent rather than arriving as "" — the null-vs-empty distinction the
    // wire's `optional string` exists for.
    assert_eq!(
        info.future_replica_log_dir, None,
        "{backend} backend: no move is pending, so there is no future log dir"
    );
    assert_eq!(
        info.future_replica_offset_lag, -1,
        "{backend} backend: Java reports -1 when there is no future replica"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

async fn alter_replica_log_dirs_nonexistent_dir_errors<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_alter_replica_log_dirs_bad_dir");
    create_topic(&admin, &topic, 1, 1).await;

    let broker_id = first_broker_id(&admin).await;
    let replica = TopicPartitionReplica::new(topic.clone(), 0, broker_id);
    // The default single-broker fixture has one log directory, so moving to a
    // directory the broker does not manage is rejected. A genuine cross-dir move
    // is exercised by `alter_replica_log_dirs_cross_dir_move`.
    let assignment = HashMap::from([(replica.clone(), "/nonexistent/kafka-logs".to_string())]);
    let altered = admin
        .alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: alter replica log dirs: {e}"));
    // The *call* succeeds and the failure is per replica.
    let err = altered[&replica]
        .as_ref()
        .expect_err(&format!("{backend} backend: moving to an unknown dir must fail"));
    assert!(
        matches!(
            err.error(),
            Errors::LogDirNotFound | Errors::KafkaStorageError | Errors::ReplicaNotAvailable
        ),
        "{backend} backend: unexpected error for unknown log dir: {:?}",
        err.error()
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

async fn alter_replica_log_dirs_cross_dir_move<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_alter_replica_log_dirs_move");
    create_topic(&admin, &topic, 1, 1).await;
    produce_records(ctx.bootstrap_servers(), &topic, 0, 10).await;

    let broker_id = first_broker_id(&admin).await;
    let replica = TopicPartitionReplica::new(topic.clone(), 0, broker_id);

    // Discover the replica's current directory, then pick the other one.
    let current = replica_info(&admin, &replica)
        .await
        .current_replica_log_dir
        .unwrap_or_else(|| panic!("{backend} backend: current log dir"));
    let target = if current == "/tmp/kafka-logs-0" {
        "/tmp/kafka-logs-1"
    } else {
        "/tmp/kafka-logs-0"
    };

    let assignment = HashMap::from([(replica.clone(), target.to_string())]);
    let altered = admin
        .alter_replica_log_dirs(&assignment, AlterReplicaLogDirsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: alter replica log dirs: {e}"));
    // `all_of_exactly` rather than `all_of`: the fold alone returns Ok for an
    // empty map, so a backend that answered with no entries at all would look like
    // an accepted move.
    all_of_exactly(
        &admin,
        &altered,
        std::slice::from_ref(&replica),
        "alterReplicaLogDirs moving one replica across directories",
    );

    // After the move is requested, describe should show the target as either the
    // (in-progress) future directory or the (completed) current directory.
    let info = replica_info(&admin, &replica).await;
    let now_here = info.current_replica_log_dir.as_deref() == Some(target);
    let moving_here = info.future_replica_log_dir.as_deref() == Some(target);
    assert!(
        now_here || moving_here,
        "{backend} backend: expected replica to be on or moving to {target}, got current={:?} future={:?}",
        info.current_replica_log_dir,
        info.future_replica_log_dir
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// A dedicated single broker with two log directories, so an actual
/// cross-directory replica move is possible.
fn two_log_dir_cluster() -> ClusterConfig {
    ClusterConfig::with_properties(BTreeMap::from([(
        "KAFKA_LOG_DIRS".to_string(),
        "/tmp/kafka-logs-0,/tmp/kafka-logs-1".to_string(),
    )]))
}

multilanguage_admin_test!(
    test_describe_log_dirs_returns_dirs_with_replica_sizes,
    describe_log_dirs_returns_dirs_with_replica_sizes
);
multilanguage_admin_test!(
    test_describe_replica_log_dirs_returns_current_dir,
    describe_replica_log_dirs_returns_current_dir
);
multilanguage_admin_test!(
    test_alter_replica_log_dirs_nonexistent_dir_errors,
    alter_replica_log_dirs_nonexistent_dir_errors
);
multilanguage_admin_test!(
    test_alter_replica_log_dirs_cross_dir_move,
    alter_replica_log_dirs_cross_dir_move,
    two_log_dir_cluster()
);
