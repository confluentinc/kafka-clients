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

//! Integration tests for the `KafkaAdminClient` group listing / describe RPCs
//! against a real Kafka 4.2.0 broker.
//!
//! Mirrors the group-management scenarios in Java's `KafkaAdminClientIntegrationTest`
//! / `PlaintextConsumerTest` (list / describe against a live KIP-848 consumer
//! group), exercising the real network engine, the `CoordinatorStrategy` lookup,
//! and the broker-enumeration `Call` idiom end to end rather than the
//! `MockClient` unit-test harness.
//!
//! The classic-protocol fallback state machine (`ConsumerGroupDescribe` ->
//! `DescribeGroups` on `UNSUPPORTED_VERSION` / `GROUP_ID_NOT_FOUND`) is covered
//! by unit tests only: this client can only *create* KIP-848 consumer groups
//! (`consumer-threading.md` §20 scopes the classic `ClassicKafkaConsumer` out),
//! so there is no way to bring a live classic group into existence here. See
//! `src/admin/internals/describe_consumer_groups_handler.rs`
//! (`test_group_id_not_found_message_preserved_across_fallback`) and
//! `src/admin/kafka_admin_client.rs`
//! (`test_describe_groups_with_both_unsupported_apis`) for that coverage.

use std::collections::HashMap;
use std::time::Duration;

#[allow(deprecated)]
use confluent_kafka::admin::ListConsumerGroupsOptions;
use confluent_kafka::admin::{
    Admin, AdminClientConfig, DeleteConsumerGroupsOptions, DescribeConsumerGroupsOptions, ListGroupsOptions,
    MemberToRemove, RemoveMembersFromConsumerGroupOptions, new_admin_client,
};
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::common::{Error, GroupState, GroupType};
use confluent_kafka::consumer::{Consumer, ConsumerConfig, new_consumer};

use crate::common::cluster_config::kip848_3_broker;
use crate::common::test_context::TestContext;
use crate::common::test_utils::create_topic;

/// Auto-created topics get this many partitions (see [`kip848_3_broker`]).
const NUM_PARTITIONS: i32 = 2;

/// Local byte-array deserializer (the crate exports `ByteArraySerializer` but no
/// symmetric `ByteArrayDeserializer`); identical to what such a struct would do.
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

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

/// Build a KIP-848 (`group.protocol=consumer`) `ConsumerConfig`.
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

/// Build a KIP-848 `ConsumerConfig` for a static member (with a
/// `group.instance.id`), so it can be targeted by
/// `remove_members_from_consumer_group`.
fn static_consumer_config(bootstrap: &str, group_id: &str, instance_id: &str) -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), format!("integration-test-consumer-{instance_id}")),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("group.instance.id".to_string(), instance_id.to_string()),
    ]);
    ConsumerConfig::from_properties(&props).expect("invalid consumer test config")
}

/// Subscribe the consumer to `topic` and poll until it has been assigned
/// partitions (i.e. the KIP-848 group has reconciled and is `Stable`).
/// Returns the assigned partition count.
async fn subscribe_and_join(consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>, topic: &str) -> usize {
    consumer
        .subscribe(vec![topic.to_string()])
        .await
        .expect("subscribe should succeed");
    for _ in 0..60 {
        // Short poll keeps the member alive and drives reconciliation.
        let _ = consumer.poll(Duration::from_millis(500)).await;
        if !consumer.assignment().is_empty() {
            return consumer.assignment().len();
        }
    }
    panic!("consumer never received a partition assignment for topic {topic}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_groups_and_list_consumer_groups_show_live_group() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("admin_groups_list");
    let group_id = ctx.group_id("g_list");

    // Create the topic explicitly so the assignment is deterministic.
    create_topic(admin.as_ref(), &topic, NUM_PARTITIONS, 1).await;

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        consumer_config(ctx.bootstrap_servers(), &group_id),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");
    subscribe_and_join(&mut consumer, &topic).await;

    // (a) list_groups: the created group appears with type Consumer, state Stable.
    let mut found_stable = false;
    for _ in 0..40 {
        // Keep heartbeating while we poll the coordinator's group registry.
        let _ = consumer.poll(Duration::from_millis(200)).await;
        let groups = admin
            .list_groups(ListGroupsOptions::new())
            .all()
            .get()
            .await
            .expect("list groups");
        if let Some(g) = groups.iter().find(|g| g.group_id() == group_id) {
            assert_eq!(
                g.group_type(),
                Some(GroupType::Consumer),
                "live group should be a KIP-848 consumer group"
            );
            if g.group_state() == Some(GroupState::Stable) {
                found_stable = true;
                break;
            }
        }
    }
    assert!(found_stable, "list_groups should report {group_id} as Stable");

    // list_consumer_groups (deprecated) also reports it.
    #[allow(deprecated)]
    let consumer_groups = admin
        .list_consumer_groups(ListConsumerGroupsOptions::new())
        .all()
        .get()
        .await
        .expect("list consumer groups");
    assert!(
        consumer_groups.iter().any(|g| g.group_id() == group_id),
        "list_consumer_groups should report {group_id}"
    );

    drop(consumer);
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_describe_consumer_groups_live_group() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("admin_groups_describe");
    let group_id = ctx.group_id("g_describe");

    create_topic(admin.as_ref(), &topic, NUM_PARTITIONS, 1).await;

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        consumer_config(ctx.bootstrap_servers(), &group_id),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");
    let assigned = subscribe_and_join(&mut consumer, &topic).await;
    assert_eq!(assigned, NUM_PARTITIONS as usize, "sole member should own every partition");

    // (b) describe_consumer_groups on the live group: one member owning all partitions.
    let mut ok = false;
    for _ in 0..40 {
        let _ = consumer.poll(Duration::from_millis(200)).await;
        let described = admin
            .describe_consumer_groups(std::slice::from_ref(&group_id), DescribeConsumerGroupsOptions::new())
            .described_groups();
        let desc = described[&group_id].clone().get().await.expect("describe group");
        assert_eq!(desc.group_id(), group_id);
        assert_eq!(desc.group_type(), GroupType::Consumer);
        if desc.group_state() == GroupState::Stable && desc.members().len() == 1 {
            let member = &desc.members()[0];
            let owned: std::collections::HashSet<i32> =
                member.assignment().topic_partitions().iter().map(|tp| tp.partition()).collect();
            assert_eq!(
                owned.len(),
                NUM_PARTITIONS as usize,
                "the sole member should own all partitions"
            );
            assert!(member.assignment().topic_partitions().iter().all(|tp| tp.topic() == topic));
            ok = true;
            break;
        }
    }
    assert!(
        ok,
        "describe_consumer_groups should report {group_id} Stable with one fully-assigned member"
    );

    drop(consumer);
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_describe_consumer_groups_nonexistent_group() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let missing = ctx.group_id("g_does_not_exist");

    // (c) Describing a group that was never created. The dual-protocol handler
    // first issues `ConsumerGroupDescribe` (-> GROUP_ID_NOT_FOUND) then falls
    // back to the classic `DescribeGroups`. Whether the coordinator surfaces
    // GROUP_ID_NOT_FOUND or a classic "Dead" placeholder group on that fallback
    // is broker-version dependent, so we accept either faithful outcome — the
    // one thing that must NOT happen is a live/`Stable` group being reported.
    let described = admin
        .describe_consumer_groups(std::slice::from_ref(&missing), DescribeConsumerGroupsOptions::new())
        .described_groups();
    match described[&missing].clone().get().await {
        Err(err) => {
            assert_eq!(
                err.error(),
                Errors::GroupIdNotFound,
                "nonexistent group should fail with GROUP_ID_NOT_FOUND, got: {err}"
            );
        },
        Ok(desc) => {
            assert!(
                desc.members().is_empty(),
                "nonexistent group must have no members, got state {:?}",
                desc.group_state()
            );
            assert_ne!(
                desc.group_state(),
                GroupState::Stable,
                "nonexistent group must not be reported as Stable"
            );
        },
    }

    ctx.cleanup().await;
}

/// (a) An empty (member-less but retained) group can be deleted, and (b)
/// deleting a group that still has an active member fails with a non-retriable
/// `NON_EMPTY_GROUP` error.
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_consumer_groups_empty_and_non_empty() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("admin_delete_groups");
    let empty_group = ctx.group_id("g_delete_empty");
    let live_group = ctx.group_id("g_delete_live");

    create_topic(admin.as_ref(), &topic, NUM_PARTITIONS, 1).await;

    // Bring a group up, commit an offset so it is retained, then close the
    // consumer so the group becomes empty (member-less) but still exists.
    {
        let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
            consumer_config(ctx.bootstrap_servers(), &empty_group),
            Box::new(ByteArrayDeserializer),
            Box::new(ByteArrayDeserializer),
        )
        .expect("new_consumer should succeed");
        subscribe_and_join(&mut consumer, &topic).await;
        let _ = consumer.poll(Duration::from_millis(500)).await;
        consumer.commit_sync().await.expect("commit offsets");
        consumer.close().await.expect("close consumer");
    }

    // (b) A group with an active member cannot be deleted (NON_EMPTY_GROUP).
    let mut live_consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        consumer_config(ctx.bootstrap_servers(), &live_group),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");
    subscribe_and_join(&mut live_consumer, &topic).await;
    let _ = live_consumer.poll(Duration::from_millis(200)).await;

    let non_empty_err = admin
        .delete_consumer_groups(std::slice::from_ref(&live_group), DeleteConsumerGroupsOptions::new())
        .deleted_groups()[&live_group]
        .clone()
        .get()
        .await
        .expect_err("deleting a group with active members must fail");
    assert_eq!(
        non_empty_err.error(),
        Errors::NonEmptyGroup,
        "deleting a non-empty group should fail with NON_EMPTY_GROUP, got: {non_empty_err}"
    );
    assert!(!non_empty_err.is_retriable(), "NON_EMPTY_GROUP is a non-retriable error");

    // (a) The empty group deletes successfully.
    admin
        .delete_consumer_groups(std::slice::from_ref(&empty_group), DeleteConsumerGroupsOptions::new())
        .deleted_groups()[&empty_group]
        .clone()
        .get()
        .await
        .expect("deleting an empty group should succeed");

    // ... and is gone afterwards.
    let described = admin
        .describe_consumer_groups(std::slice::from_ref(&empty_group), DescribeConsumerGroupsOptions::new())
        .described_groups();
    match described[&empty_group].clone().get().await {
        Err(err) => assert_eq!(err.error(), Errors::GroupIdNotFound),
        Ok(desc) => assert!(desc.members().is_empty(), "deleted group must have no members"),
    }

    drop(live_consumer);
    ctx.cleanup().await;
}

/// (c) `remove_members_from_consumer_group` for a single static member removes
/// it, and its partitions are reassigned to the other member.
#[tokio::test(flavor = "multi_thread")]
async fn test_remove_one_member_from_consumer_group() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("admin_remove_member");
    let group_id = ctx.group_id("g_remove_one");

    create_topic(admin.as_ref(), &topic, NUM_PARTITIONS, 1).await;

    // Two static members share the topic's partitions.
    let mut member_one = new_consumer::<Vec<u8>, Vec<u8>>(
        static_consumer_config(ctx.bootstrap_servers(), &group_id, "instance-1"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");
    let mut member_two = new_consumer::<Vec<u8>, Vec<u8>>(
        static_consumer_config(ctx.bootstrap_servers(), &group_id, "instance-2"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");
    member_one.subscribe(vec![topic.clone()]).await.expect("subscribe m1");
    member_two.subscribe(vec![topic.clone()]).await.expect("subscribe m2");
    // Drive both members until the group reconciles to two members.
    for _ in 0..60 {
        let _ = member_one.poll(Duration::from_millis(300)).await;
        let _ = member_two.poll(Duration::from_millis(300)).await;
        if !member_one.assignment().is_empty() && !member_two.assignment().is_empty() {
            break;
        }
    }

    // Remove instance-1 by its group.instance.id.
    let options =
        RemoveMembersFromConsumerGroupOptions::new([MemberToRemove::new("instance-1")]).expect("non-empty members");
    admin
        .remove_members_from_consumer_group(&group_id, options)
        .all()
        .get()
        .await
        .expect("removing a static member should succeed");

    // instance-2 should reconcile to owning all partitions after the removal.
    let mut reassigned = false;
    for _ in 0..60 {
        let _ = member_two.poll(Duration::from_millis(300)).await;
        if member_two.assignment().len() == NUM_PARTITIONS as usize {
            reassigned = true;
            break;
        }
    }
    assert!(
        reassigned,
        "the remaining member should be reassigned all partitions after removal"
    );

    drop(member_one);
    drop(member_two);
    ctx.cleanup().await;
}

/// (d) `remove_members_from_consumer_group` with `removeAll` empties the group.
#[tokio::test(flavor = "multi_thread")]
async fn test_remove_all_members_from_consumer_group() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("admin_remove_all");
    let group_id = ctx.group_id("g_remove_all");

    create_topic(admin.as_ref(), &topic, NUM_PARTITIONS, 1).await;

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        static_consumer_config(ctx.bootstrap_servers(), &group_id, "instance-1"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");
    subscribe_and_join(&mut consumer, &topic).await;

    // removeAll: no specific members provided.
    admin
        .remove_members_from_consumer_group(&group_id, RemoveMembersFromConsumerGroupOptions::default())
        .all()
        .get()
        .await
        .expect("removeAll should succeed");

    // The group should have no active members afterwards.
    let mut emptied = false;
    for _ in 0..40 {
        let described = admin
            .describe_consumer_groups(std::slice::from_ref(&group_id), DescribeConsumerGroupsOptions::new())
            .described_groups();
        match described[&group_id].clone().get().await {
            Ok(desc) if desc.members().is_empty() => {
                emptied = true;
                break;
            },
            Err(err) if err.error() == Errors::GroupIdNotFound => {
                emptied = true;
                break;
            },
            _ => {},
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(emptied, "removeAll should leave the group with no active members");

    drop(consumer);
    ctx.cleanup().await;
}
