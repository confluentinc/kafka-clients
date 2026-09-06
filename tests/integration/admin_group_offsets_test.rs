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

//! Integration tests for the consumer-group-offset admin RPCs
//! (`listConsumerGroupOffsets` / `alterConsumerGroupOffsets` /
//! `deleteConsumerGroupOffsets`) against a real Kafka 4.2.0 broker.
//!
//! Mirrors the offset-management scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine and the
//! `CoordinatorStrategy` lookup end to end rather than the `MockClient`
//! unit-test harness.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and
//! the C FFI. With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion, and it drives the same production `Admin` trait against the same
//! broker as the single-backend tests these were converted from.
//!
//! The producers and consumers here are always the **native** Rust client against
//! the *host* listener: neither is the object under test, and the gRPC backends
//! could not reach the host loopback anyway. Only the admin client varies.
//!
//! The distinction these scenarios exist to pin is
//! `listConsumerGroupOffsets`' **nullable map value**: Java's per-partition value
//! is `OffsetAndMetadata | null`, where null means "this group has no committed
//! offset for that partition" — not a committed offset of 0. Both bindings carry
//! an explicit discriminant for it (`kafka_admin_OffsetAndMetadataMap_has_offset`,
//! admin.py's inner `None`), and
//! [`delete_consumer_group_offsets_on_inactive_group`] is what observes the
//! present → absent transition end to end.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use confluent_kafka::admin::{
    AlterConsumerGroupOffsetsOptions, DeleteConsumerGroupOffsetsOptions, GroupOffsets, ListConsumerGroupOffsetsOptions,
    ListConsumerGroupOffsetsSpec,
};
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::{ByteArraySerializer, Deserializer};
use confluent_kafka::common::{Error, TopicPartition};
use confluent_kafka::consumer::{Consumer, ConsumerConfig, OffsetAndMetadata, new_consumer};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::admin_backend::{AdminBackend, admin_for, all_of_exactly, create_topic};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::cluster_config::kip848_3_broker;
use crate::common::test_context::TestContext;
use crate::multilanguage_admin_test;

const NUM_PARTITIONS: i32 = 2;

/// Local byte-array deserializer (the crate exports `ByteArraySerializer` but no
/// symmetric `ByteArrayDeserializer`).
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

type BytesConsumer = Box<dyn Consumer<Vec<u8>, Vec<u8>>>;

fn consumer_config(bootstrap: &str, group_id: &str) -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), group_id.to_string()),
    ]);
    ConsumerConfig::from_properties(&props).expect("invalid consumer test config")
}

fn new_bytes_consumer(bootstrap: &str, group_id: &str) -> BytesConsumer {
    new_consumer::<Vec<u8>, Vec<u8>>(
        consumer_config(bootstrap, group_id),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed")
}

/// Subscribe and poll until the KIP-848 group has reconciled (partitions
/// assigned).
async fn subscribe_and_join(consumer: &mut BytesConsumer, topic: &str) {
    consumer
        .subscribe_topics(vec![topic.to_string()])
        .await
        .expect("subscribe should succeed");
    for _ in 0..60 {
        let _ = consumer.poll(Duration::from_millis(500)).await;
        if !consumer.assignment().is_empty() {
            return;
        }
    }
    panic!("consumer never received a partition assignment for topic {topic}");
}

async fn produce_records(bootstrap: &str, tp: &TopicPartition, num: usize) {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "5".to_string()),
    ]);
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> = KafkaProducer::from_config(
        ProducerConfig::from_properties(&props).expect("producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("build producer");
    let mut last = None;
    for i in 0..num {
        let record = ProducerRecord::new_partition_timestamp_key(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(1_700_000_000_000 + i as i64),
            Some(format!("k{i}").into_bytes()),
            Some(format!("v{i}").into_bytes()),
        )
        .expect("valid producer record");
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

/// Lists one group's committed offsets, for the whole group (Java's *unset*
/// partition collection).
///
/// The result is per group *and* per partition — the two-level shape
/// `describeLogDirs` established — so this reads the group's key out of the outer
/// map and hands back the inner one. Asking for exactly the requested group key
/// (rather than folding over whatever came back) is what catches a short response.
async fn list_offsets<B: AdminBackend>(admin: &B, group_id: &str) -> GroupOffsets {
    let spec = HashMap::from([(group_id.to_string(), ListConsumerGroupOffsetsSpec::new())]);
    let outcomes = admin
        .list_consumer_group_offsets(&spec, ListConsumerGroupOffsetsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: list consumer group offsets: {e}", admin.name()));
    all_of_exactly(
        admin,
        &outcomes,
        &[group_id.to_string()],
        "listConsumerGroupOffsets for one group",
    );
    outcomes[group_id]
        .as_ref()
        .unwrap_or_else(|e| panic!("{} backend: list offsets for {group_id}: {e}", admin.name()))
        .clone()
}

/// The committed offset for `tp`, or `None` if the group has none.
///
/// Collapses the two ways "no offset" can arrive — the partition missing from the
/// map, and the partition present with Java's null value — because both mean the
/// same thing to a caller. The scenarios that care about the *distinction* assert
/// on the map directly.
fn committed(offsets: &GroupOffsets, tp: &TopicPartition) -> Option<i64> {
    offsets.get(tp).and_then(|o| o.as_ref()).map(OffsetAndMetadata::offset)
}

// ---------------------------------------------------------------------------
// listConsumerGroupOffsets
// ---------------------------------------------------------------------------

/// A live consumer commits explicit offsets; `list_consumer_group_offsets`
/// reports them.
async fn list_consumer_group_offsets_matches_committed<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_offsets_list");
    let group_id = ctx.group_id("g_offsets_list");
    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;

    let mut consumer = new_bytes_consumer(&bootstrap, &group_id);
    subscribe_and_join(&mut consumer, &topic).await;

    let tp0 = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);
    let to_commit = HashMap::from([
        (tp0.clone(), OffsetAndMetadata::new(5).unwrap()),
        (tp1.clone(), OffsetAndMetadata::new(3).unwrap()),
    ]);
    consumer.commit_sync_offsets(to_commit).await.expect("commit_sync");

    let listed = list_offsets(&admin, &group_id).await;
    assert_eq!(
        committed(&listed, &tp0),
        Some(5),
        "{backend} backend: partition 0's committed offset"
    );
    assert_eq!(
        committed(&listed, &tp1),
        Some(3),
        "{backend} backend: partition 1's committed offset"
    );
    // The two offsets are deliberately distinct so that pairing offsets with
    // partitions by *position* rather than by key fails one of the two assertions
    // above. (An `assert_ne!(committed(tp0), committed(tp1))` after them would be
    // entailed by `Some(5) != Some(3)` and could never fire on its own; it used to
    // stand here and is deliberately gone.)
    //
    // What is *not* entailed, and is asserted here, is that no third partition was
    // invented: the group subscribed to a two-partition topic and committed both,
    // so the reported key set is exactly those two.
    assert_eq!(
        listed.keys().cloned().collect::<HashSet<_>>(),
        HashSet::from([tp0.clone(), tp1.clone()]),
        "{backend} backend: the listing must report exactly the two committed partitions, got {listed:?}"
    );

    drop(consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// The per-group partition selection is honoured, in **both** directions, and
/// `require_stable` crosses.
///
/// Added, not converted. `ListConsumerGroupOffsetsSpec.topicPartitions` is Java's
/// *unset* collection by default — "every partition the group has committed
/// offsets for" — and an explicitly empty one selects nothing. Those are two
/// different requests, and unlike `removeMembersFromConsumerGroup`'s member list
/// (which Java's constructor refuses to build empty) this one is fully
/// constructible, so the distinction is observable in both directions:
///
///   - unset lists both committed partitions;
///   - `Some([tp0])` lists only partition 0 — so an encoder that widened an
///     explicit selection to "everything" fails;
///   - `Some([])` lists nothing — so an encoder that collapsed a present-but-empty
///     list into the unset form fails.
///
/// That third case is the direction `electLeaders` could not reach on a healthy
/// cluster, and it is what makes this the strongest null-vs-empty check in the
/// admin harness.
///
/// `ListConsumerGroupOffsetsOptions::require_stable` is exercised at the same
/// time: it is plumbed through all four backends and otherwise never set. With no
/// in-flight transaction it must agree with the default, and a backend that
/// dropped it would still agree — so what this proves is that a non-default value
/// crosses and is accepted, not that the flag changes the answer. Distinguishing
/// it needs an open transaction on `__consumer_offsets`, which the producer
/// transaction API cannot set up here.
async fn list_consumer_group_offsets_honours_the_partition_selection<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_offsets_selection");
    let group_id = ctx.group_id("g_offsets_selection");
    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;

    let mut consumer = new_bytes_consumer(&bootstrap, &group_id);
    subscribe_and_join(&mut consumer, &topic).await;
    let tp0 = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);
    consumer
        .commit_sync_offsets(HashMap::from([
            (tp0.clone(), OffsetAndMetadata::new(4).unwrap()),
            (tp1.clone(), OffsetAndMetadata::new(6).unwrap()),
        ]))
        .await
        .expect("commit_sync");

    /// Lists with the given spec and returns the inner map.
    async fn with_spec<B: AdminBackend>(
        admin: &B,
        group_id: &str,
        spec: ListConsumerGroupOffsetsSpec,
        options: ListConsumerGroupOffsetsOptions,
        what: &str,
    ) -> GroupOffsets {
        let outcomes = admin
            .list_consumer_group_offsets(&HashMap::from([(group_id.to_string(), spec)]), options)
            .await
            .unwrap_or_else(|e| panic!("{} backend: list offsets ({what}): {e}", admin.name()));
        all_of_exactly(admin, &outcomes, &[group_id.to_string()], what);
        outcomes[group_id]
            .as_ref()
            .unwrap_or_else(|e| panic!("{} backend: list offsets ({what}): {e}", admin.name()))
            .clone()
    }

    // (a) Unset selection: Java's null collection, i.e. every committed partition.
    let all = with_spec(
        &admin,
        &group_id,
        ListConsumerGroupOffsetsSpec::new(),
        ListConsumerGroupOffsetsOptions::new(),
        "unset partition selection",
    )
    .await;
    assert_eq!(
        committed(&all, &tp0),
        Some(4),
        "{backend} backend: an unset selection lists partition 0"
    );
    assert_eq!(
        committed(&all, &tp1),
        Some(6),
        "{backend} backend: an unset selection lists partition 1"
    );

    // (b) Explicit selection of one partition: only that one is reported.
    let only_zero = with_spec(
        &admin,
        &group_id,
        ListConsumerGroupOffsetsSpec::new().topic_partitions(Some(vec![tp0.clone()])),
        ListConsumerGroupOffsetsOptions::new(),
        "explicit selection of partition 0",
    )
    .await;
    assert_eq!(
        committed(&only_zero, &tp0),
        Some(4),
        "{backend} backend: the explicitly selected partition is reported"
    );
    // `committed()` maps "absent from the map" and "present with Java's null
    // value" to the same `None`, so it cannot tell "not reported" from "reported
    // with no offset" — and the property this sub-case means is the former. Assert
    // it directly rather than leaving the message stronger than the check.
    assert!(
        !only_zero.contains_key(&tp1),
        "{backend} backend: an explicit selection of {tp0} must not report {tp1} at all; seeing it here means the \
         selection was widened to the whole group, got {only_zero:?}"
    );

    // (c) Explicitly *empty* selection: nothing. This is the direction that a
    // backend collapsing present-but-empty into the unset form gets wrong, and the
    // direction electLeaders' partition set cannot reach on a healthy cluster.
    let none_selected = with_spec(
        &admin,
        &group_id,
        ListConsumerGroupOffsetsSpec::new().topic_partitions(Some(Vec::new())),
        ListConsumerGroupOffsetsOptions::new(),
        "explicitly empty partition selection",
    )
    .await;
    assert!(
        none_selected.values().all(Option::is_none),
        "{backend} backend: an explicitly empty selection reports no committed offset; a non-empty answer here \
         means the empty list was encoded as Java's unset collection, got {none_selected:?}"
    );

    // (d) require_stable crosses and is accepted. With no in-flight transaction
    // the answer must match the default one.
    let stable = with_spec(
        &admin,
        &group_id,
        ListConsumerGroupOffsetsSpec::new(),
        ListConsumerGroupOffsetsOptions::new().set_require_stable(true),
        "require_stable=true",
    )
    .await;
    assert_eq!(
        committed(&stable, &tp0),
        Some(4),
        "{backend} backend: with no in-flight transaction require_stable does not change the committed offset"
    );
    assert_eq!(committed(&stable, &tp1), Some(6));

    drop(consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// alterConsumerGroupOffsets
// ---------------------------------------------------------------------------

/// After the consumer leaves, `alter_consumer_group_offsets` rewinds the
/// committed offset, and a fresh consumer resumes from the altered position.
async fn alter_consumer_group_offsets_and_resume<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_offsets_alter");
    let group_id = ctx.group_id("g_offsets_alter");
    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;
    let tp0 = TopicPartition::new(topic.clone(), 0);

    produce_records(&bootstrap, &tp0, 10).await;

    // Consumer A joins and commits offset 10 (the log end), then leaves.
    let mut consumer_a = new_bytes_consumer(&bootstrap, &group_id);
    subscribe_and_join(&mut consumer_a, &topic).await;
    consumer_a
        .commit_sync_offsets(HashMap::from([(tp0.clone(), OffsetAndMetadata::new(10).unwrap())]))
        .await
        .expect("commit");
    consumer_a.close().await.expect("close A");
    drop(consumer_a);
    // Give the coordinator time to observe the member leave (empty group).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Alter the committed offset back to 5 while the group is empty. The metadata
    // string travels with it: Java's OffsetAndMetadata carries one and both
    // bindings forward it, so a dropped metadata field shows up in the listing
    // below.
    let altered = admin
        .alter_consumer_group_offsets(
            &group_id,
            &HashMap::from([(tp0.clone(), OffsetAndMetadata::new_metadata(5, "rewound by admin").unwrap())]),
            AlterConsumerGroupOffsetsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: alter consumer group offsets: {e}"));
    all_of_exactly(
        &admin,
        &altered,
        std::slice::from_ref(&tp0),
        "alterConsumerGroupOffsets on an empty group",
    );

    let listed = list_offsets(&admin, &group_id).await;
    assert_eq!(
        committed(&listed, &tp0),
        Some(5),
        "{backend} backend: the altered offset is what the group has committed"
    );
    assert_eq!(
        listed[&tp0].as_ref().map(OffsetAndMetadata::metadata),
        Some("rewound by admin"),
        "{backend} backend: the metadata committed with the offset must survive the round trip"
    );

    // Consumer B resumes from the altered offset (5) and reads records 5..10.
    // Subscribe and poll in one loop so the very first partition-0 record is
    // captured (the join itself drives fetching).
    let mut consumer_b = new_bytes_consumer(&bootstrap, &group_id);
    consumer_b.subscribe_topics(vec![topic.clone()]).await.expect("subscribe B");
    let mut first_offset = None;
    for _ in 0..60 {
        let records = consumer_b.poll(Duration::from_millis(500)).await.expect("poll B");
        if let Some(record) = (&records).into_iter().find(|r| r.partition() == 0) {
            first_offset = Some(record.offset());
            break;
        }
    }
    assert_eq!(
        first_offset,
        Some(5),
        "{backend} backend: consumer B should resume from the altered offset"
    );

    drop(consumer_b);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// deleteConsumerGroupOffsets
// ---------------------------------------------------------------------------

/// `delete_consumer_group_offsets` on an inactive group removes the committed
/// offset — and the *other* partition's commit survives.
///
/// This is the scenario that observes Java's nullable map value end to end: the
/// deleted partition goes from "present with an offset" to "no committed offset",
/// which a backend that decoded the null as offset 0 would report as `Some(0)`.
async fn delete_consumer_group_offsets_on_inactive_group<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_offsets_delete");
    let group_id = ctx.group_id("g_offsets_delete");
    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;
    let tp0 = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);

    let mut consumer = new_bytes_consumer(&bootstrap, &group_id);
    subscribe_and_join(&mut consumer, &topic).await;
    // Both partitions are committed, so the deletion can be shown to be scoped to
    // the one that was asked for.
    consumer
        .commit_sync_offsets(HashMap::from([
            (tp0.clone(), OffsetAndMetadata::new(7).unwrap()),
            (tp1.clone(), OffsetAndMetadata::new(9).unwrap()),
        ]))
        .await
        .expect("commit");
    consumer.close().await.expect("close");
    drop(consumer);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Both committed offsets are present before deletion.
    let before = list_offsets(&admin, &group_id).await;
    assert_eq!(committed(&before, &tp0), Some(7), "{backend} backend: before deletion");
    assert_eq!(committed(&before, &tp1), Some(9), "{backend} backend: before deletion");

    let deleted = admin
        .delete_consumer_group_offsets(
            &group_id,
            &HashSet::from([tp0.clone()]),
            DeleteConsumerGroupOffsetsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete consumer group offsets: {e}"));
    all_of_exactly(
        &admin,
        &deleted,
        std::slice::from_ref(&tp0),
        "deleteConsumerGroupOffsets on an inactive group",
    );

    // After deletion the partition has no committed offset — Java's null map
    // value, or absent from the map entirely. Not offset 0.
    let after = list_offsets(&admin, &group_id).await;
    assert_eq!(
        committed(&after, &tp0),
        None,
        "{backend} backend: the committed offset for {tp0} should be gone after delete, got {:?}",
        after.get(&tp0)
    );
    assert_eq!(
        committed(&after, &tp1),
        Some(9),
        "{backend} backend: deleting {tp0}'s offset must not touch {tp1}'s"
    );

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// `delete_consumer_group_offsets` on an ACTIVE (subscribed) group fails.
async fn delete_consumer_group_offsets_on_active_group_errors<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_offsets_active");
    let group_id = ctx.group_id("g_offsets_active");
    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;
    let tp0 = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_bytes_consumer(&bootstrap, &group_id);
    subscribe_and_join(&mut consumer, &topic).await;
    // Keep the member alive across the delete attempt.
    let _ = consumer.poll(Duration::from_millis(200)).await;

    let deleted = admin
        .delete_consumer_group_offsets(
            &group_id,
            &HashSet::from([tp0.clone()]),
            DeleteConsumerGroupOffsetsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete consumer group offsets: {e}"));
    // The broker rejects deletion of a partition the group is actively subscribed
    // to (GROUP_SUBSCRIBED_TO_TOPIC), per partition rather than for the whole call.
    let err = deleted
        .get(&tp0)
        .unwrap_or_else(|| panic!("{backend} backend: {tp0} missing from the delete result"))
        .as_ref()
        .expect_err(&format!(
            "{backend} backend: deleting offsets of a partition an active group is subscribed to should fail"
        ));
    assert_eq!(
        err.error(),
        Errors::GroupSubscribedToTopic,
        "{backend} backend: unexpected error deleting active-group offsets: {err:?}"
    );

    drop(consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_ml_admin_list_consumer_group_offsets_matches_committed,
    list_consumer_group_offsets_matches_committed,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_list_consumer_group_offsets_honours_the_partition_selection,
    list_consumer_group_offsets_honours_the_partition_selection,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_alter_consumer_group_offsets_and_resume,
    alter_consumer_group_offsets_and_resume,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_delete_consumer_group_offsets_on_inactive_group,
    delete_consumer_group_offsets_on_inactive_group,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_delete_consumer_group_offsets_on_active_group_errors,
    delete_consumer_group_offsets_on_active_group_errors,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
