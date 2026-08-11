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

//! Integration tests for the admin group listing / describe / deletion RPCs
//! against a real Kafka 4.2.0 broker.
//!
//! Mirrors the group-management scenarios in Java's
//! `KafkaAdminClientIntegrationTest` / `PlaintextConsumerTest` (list / describe
//! against a live KIP-848 consumer group), exercising the real network engine,
//! the `CoordinatorStrategy` lookup, and the broker-enumeration `Call` idiom end
//! to end rather than the `MockClient` unit-test harness.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and
//! the C FFI — a disagreement between them shows up as three backends agreeing
//! and one not (see `design/history/Milestone-11/PLAN-multilanguage-admin.md`).
//! With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion, and it drives the same production `Admin` trait against the same
//! broker as the single-backend tests these scenarios were converted from.
//!
//! **This file is where the defect that motivated the whole multilanguage-admin
//! milestone would have been caught.** `ConsumerGroupDescription.coordinator()`
//! returned a fabricated `Node` (`host=""`, `port=-1`) where the broker was at
//! `127.0.0.1:19092`, and it survived 3389 Rust, 249 C and 261 Python tests
//! because the only assertion anywhere compared `coordinator().map(Node::id)` —
//! which a fabricated `Node::new(id, "", -1)` satisfies.
//! [`assert_real_coordinator`] now checks the whole endpoint against the node
//! list the *same backend's* `describeCluster` reports, so it holds for the
//! container backends (which see the broker's container listener) as well as for
//! the native one (host loopback).
//!
//! The consumers these scenarios drive are always the **native** Rust client
//! against the *host* listener: a consumer is not the object under test here, and
//! the gRPC backends could not reach the host loopback anyway. Only the admin
//! client varies with the backend.
//!
//! Two coverage limits, recorded rather than implied away:
//!
//!   - **A classic group with *members* is unreachable; an empty one is not.** An
//!     earlier revision of this doc claimed a live classic group could not exist
//!     at all, because this client can only *create* KIP-848 consumer groups
//!     (`consumer-threading.md` §20 scopes `ClassicKafkaConsumer` out). The
//!     premise is right and the conclusion does not follow: a classic group does
//!     not need a classic *consumer*. An admin offset commit creates one —
//!     `OffsetMetadataManager.validateOffsetCommit` catches
//!     `GroupIdNotFoundException`, and when the request's generation id is
//!     negative it calls `getOrMaybeCreateClassicGroup(groupId, true)` and
//!     accepts the commit
//!     (`group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java:458-467`).
//!     `GenerationIdOrMemberEpoch` defaults to `-1`
//!     (`clients/src/main/resources/common/message/OffsetCommitRequest.json:46`)
//!     and `AlterConsumerGroupOffsetsHandler` never sets it
//!     (`src/admin/internals/alter_consumer_group_offsets_handler.rs:107-111`), so
//!     one `alter_consumer_group_offsets` call on a never-consumed group id leaves
//!     a simple classic group behind in state `Empty`. That is what
//!     [`describe_a_simple_classic_group`] drives, and it is the only route to
//!     `ClassicGroupDescription`'s **value** arm, to
//!     `describeConsumerGroups`' classic-fallback value path, and to
//!     `is_simple_consumer_group == true` on either type.
//!
//!     What stays unreachable is a classic group with **members**, and with it
//!     `ConsumerProtocol::deserialize_assignment`:
//!     `DescribeClassicGroupsHandler` only decodes a member's raw assignment
//!     bytes when the coordinator populates them, which happens solely in the
//!     `isInState(STABLE)` branch of `GroupMetadataManager.describeGroups`
//!     (`group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java:744-757`)
//!     — a stable classic group with a joined member. That stays covered by the
//!     unit tests in `src/consumer/internals/consumer_protocol.rs` and
//!     `src/admin/internals/describe_classic_groups_handler.rs`.
//!   - **`ConsumerGroupDescription`'s `state()` / `group_state()` pair cannot be
//!     caught transposed.** Java defines
//!     `state() == ConsumerGroupState.parse(groupState().toString())`, and the two
//!     enums' constant names coincide for all eight `ConsumerGroupState` values,
//!     so the only state that separates them is `GroupState.NOT_READY`
//!     (`clients/src/main/java/org/apache/kafka/common/GroupState.java:60`), which
//!     has no `ConsumerGroupState` counterpart and parses to `Unknown`.
//!     `NOT_READY` is reachable **only for a STREAMS group** — `GroupState`'s
//!     javadoc table and `groupStatesForType` (`GroupState.java:80-89`) list it
//!     under STREAMS alone — and a streams group is not something
//!     `describeConsumerGroups` can return, since the coordinator's
//!     `consumerGroup(...)` lookup answers `GROUP_ID_NOT_FOUND` for a group of
//!     another type. So a transposition of that pair is invisible *by
//!     construction*, not merely unreached on this fixture. A **dropped** field is
//!     not invisible: `MultilanguageAdmin::check_derived_state` rejects a wire
//!     `state` that disagrees with the value Java derives, so every scenario here
//!     exercises that check.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[allow(deprecated)]
use confluent_kafka::admin::ListConsumerGroupsOptions;
use confluent_kafka::admin::{
    AlterConsumerGroupOffsetsOptions, DeleteConsumerGroupsOptions, DescribeClassicGroupsOptions,
    DescribeClusterOptions, DescribeConsumerGroupsOptions, ListGroupsOptions, MemberToRemove,
    RemoveMembersFromConsumerGroupOptions,
};
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::common::{ClassicGroupState, GroupState, GroupType, KafkaError, Node, TopicPartition};
use confluent_kafka::consumer::{Consumer, ConsumerConfig, OffsetAndMetadata, new_consumer};

use crate::common::admin_backend::{AdminBackend, admin_for, all_of_exactly, create_topic};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::test_context::TestContext;
use crate::multilanguage_admin_test;

/// Auto-created topics get this many partitions (see [`kip848_3_broker`]).
const NUM_PARTITIONS: i32 = 2;

/// [`kip848_3_broker`] with a *partitioned* `__consumer_offsets`, so several
/// groups can be served by different coordinators.
///
/// `kip848_3_broker` pins `KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS=1`, which makes one
/// broker the coordinator for every group and leaves the `CoordinatorStrategy`
/// fan-out — the multi-step lookup that resolves each key to its own broker before
/// the real request — with only ever one destination. Three partitions let a batch
/// of groups hash to different brokers, so
/// [`describe_consumer_groups_batches_several_groups`] exercises the merge path
/// too. Used by that scenario alone; `backend_pool` keys containers by
/// `(kind, broker_network)`, so it simply starts its own cluster.
fn kip848_partitioned_offsets(num_partitions: u16) -> ClusterConfig {
    let mut cfg = kip848_3_broker(num_partitions);
    cfg.server_properties
        .insert("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS".to_string(), "3".to_string());
    cfg
}

/// Local byte-array deserializer (the crate exports `ByteArraySerializer` but no
/// symmetric `ByteArrayDeserializer`); identical to what such a struct would do.
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
        Ok(data.to_vec())
    }
}

type BytesConsumer = Box<dyn Consumer<Vec<u8>, Vec<u8>>>;

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

fn new_bytes_consumer(bootstrap: &str, group_id: &str) -> BytesConsumer {
    new_consumer::<Vec<u8>, Vec<u8>>(
        consumer_config(bootstrap, group_id),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed")
}

fn new_static_bytes_consumer(bootstrap: &str, group_id: &str, instance_id: &str) -> BytesConsumer {
    new_consumer::<Vec<u8>, Vec<u8>>(
        static_consumer_config(bootstrap, group_id, instance_id),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed")
}

/// Subscribe the consumer to `topic` and poll until it has been assigned
/// partitions (i.e. the KIP-848 group has reconciled and is `Stable`).
/// Returns the assigned partition count.
async fn subscribe_and_join(consumer: &mut BytesConsumer, topic: &str) -> usize {
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

/// Asserts that a group coordinator is a real broker endpoint, cross-checked
/// against the node list the *same backend* reports for the cluster.
///
/// This is the assertion the milestone exists for. Three separate things are
/// checked, and the fabricated `Node { id, host: "", port: -1 }` that shipped
/// once fails the first two:
///
///   1. the host is non-empty and the port is positive;
///   2. the id names a broker `describeCluster` knows about;
///   3. that broker's host and port match the coordinator's exactly.
///
/// `describeCluster` is queried through the same backend rather than compared
/// against `ctx.bootstrap_servers()`, because the container backends see the
/// broker's *container* listener (`broker1:19092`) while the native one sees the
/// host loopback. Both are correct; what must hold on all four is that the two
/// admin RPCs agree with each other.
async fn assert_real_coordinator<B: AdminBackend>(admin: &B, coordinator: Option<&Node>, what: &str) {
    let backend = admin.name();
    let node = coordinator.unwrap_or_else(|| panic!("{backend} backend: {what} reported no coordinator at all"));
    assert!(
        !node.host().is_empty(),
        "{backend} backend: {what}'s coordinator has an empty host ({node:?}); a coordinator built from the \
         request scope rather than from the resolved node looks exactly like this"
    );
    assert!(
        node.port() > 0,
        "{backend} backend: {what}'s coordinator has a non-positive port ({node:?})"
    );

    let cluster = admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe cluster: {e}"));
    let matching = cluster.nodes.iter().find(|n| n.id() == node.id()).unwrap_or_else(|| {
        panic!(
            "{backend} backend: {what}'s coordinator {node:?} names broker {} , which describeCluster does not \
                 report; it knows {:?}",
            node.id(),
            cluster.nodes
        )
    });
    assert_eq!(
        (matching.host(), matching.port()),
        (node.host(), node.port()),
        "{backend} backend: {what}'s coordinator endpoint must match the one describeCluster reports for broker {}",
        node.id()
    );
}

// ---------------------------------------------------------------------------
// listGroups / listConsumerGroups
// ---------------------------------------------------------------------------

/// A live KIP-848 group appears in `listGroups` as a `Consumer`-type `Stable`
/// group, and in the deprecated `listConsumerGroups` too.
async fn list_groups_and_list_consumer_groups_show_live_group<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_groups_list");
    let group_id = ctx.group_id("g_list");

    // Create the topic explicitly so the assignment is deterministic.
    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;

    let mut consumer = new_bytes_consumer(&bootstrap, &group_id);
    subscribe_and_join(&mut consumer, &topic).await;

    // (a) list_groups: the created group appears with type Consumer, state Stable.
    let mut found_stable = false;
    for _ in 0..40 {
        // Keep heartbeating while we poll the coordinator's group registry.
        let _ = consumer.poll(Duration::from_millis(200)).await;
        let listed = admin
            .list_groups(ListGroupsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: list groups: {e}"));
        // `errors()` is Java's unkeyed per-broker failure list; on a healthy
        // cluster it must be empty, and `all()` is the fold that says so.
        listed
            .all()
            .unwrap_or_else(|e| panic!("{backend} backend: list groups reported a per-broker error: {e}"));
        if let Some(g) = listed.valid.iter().find(|g| g.group_id() == group_id) {
            assert_eq!(
                g.group_type(),
                Some(GroupType::Consumer),
                "{backend} backend: live group should be a KIP-848 consumer group"
            );
            if g.group_state() == Some(GroupState::Stable) {
                found_stable = true;
                break;
            }
        }
    }
    assert!(
        found_stable,
        "{backend} backend: list_groups should report {group_id} as Stable"
    );

    // list_consumer_groups (deprecated) also reports it.
    #[allow(deprecated)]
    let consumer_groups = admin
        .list_consumer_groups(ListConsumerGroupsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list consumer groups: {e}"));
    consumer_groups
        .all()
        .unwrap_or_else(|e| panic!("{backend} backend: list consumer groups reported a per-broker error: {e}"));
    assert!(
        consumer_groups.valid.iter().any(|g| g.group_id() == group_id),
        "{backend} backend: list_consumer_groups should report {group_id}"
    );

    drop(consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// `ListGroupsOptions`' three filters actually restrict the listing.
///
/// Added, not converted. Every scenario in G1–G4 otherwise passes a bare
/// `XOptions::new()`, so `group_states`, `types` and `protocol_types` were
/// plumbed through four layers and never populated — the same gap the G1 Critic
/// recorded for `authorized_operations` and `list_internal`. Each filter is
/// exercised in **both** directions (a value that must include our group and one
/// that must exclude it), because an encoder that dropped a filter entirely would
/// pass every inclusive assertion on its own.
///
/// The two kinds of filter reach the answer differently, which is why both are
/// worth driving from four backends: `group_states` and `types` become the
/// broker's `ListGroupsRequest.StatesFilter` / `TypesFilter`, while
/// `protocol_types` is applied **client-side** after the response arrives
/// (`src/admin/kafka_admin_client.rs`, mirroring Java's
/// `KafkaAdminClient.listGroups`).
///
/// The protocol-type value is read off the group's own listing rather than
/// hard-coded, so the scenario cannot fail over a broker-version difference in
/// what a KIP-848 group's protocol type is called.
async fn list_groups_filters_restrict_the_listing<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_groups_filters");
    let group_id = ctx.group_id("g_filters");

    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;
    let mut consumer = new_bytes_consumer(&bootstrap, &group_id);
    subscribe_and_join(&mut consumer, &topic).await;

    // Wait for Stable, then read the group's own protocol type off the listing.
    let mut protocol = None;
    for _ in 0..40 {
        let _ = consumer.poll(Duration::from_millis(200)).await;
        let listed = admin
            .list_groups(ListGroupsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: list groups: {e}"));
        if let Some(g) = listed
            .valid
            .iter()
            .find(|g| g.group_id() == group_id && g.group_state() == Some(GroupState::Stable))
        {
            protocol = Some(g.protocol().to_string());
            break;
        }
    }
    let protocol = protocol.unwrap_or_else(|| panic!("{backend} backend: {group_id} never became Stable"));

    /// Whether a filtered listing contains `group_id`.
    async fn lists<B: AdminBackend>(admin: &B, options: ListGroupsOptions, group_id: &str, what: &str) -> bool {
        let listed = admin
            .list_groups(options)
            .await
            .unwrap_or_else(|e| panic!("{} backend: list groups ({what}): {e}", admin.name()));
        listed.all().unwrap_or_else(|e| {
            panic!(
                "{} backend: list groups ({what}) reported a per-broker error: {e}",
                admin.name()
            )
        });
        listed.valid.iter().any(|g| g.group_id() == group_id)
    }

    // group_states: Stable includes it, Empty excludes it (it has a live member).
    assert!(
        lists(
            &admin,
            ListGroupsOptions::new().in_group_states(HashSet::from([GroupState::Stable])),
            &group_id,
            "states=Stable",
        )
        .await,
        "{backend} backend: a Stable-only listing must include the live group {group_id}"
    );
    assert!(
        !lists(
            &admin,
            ListGroupsOptions::new().in_group_states(HashSet::from([GroupState::Empty])),
            &group_id,
            "states=Empty",
        )
        .await,
        "{backend} backend: an Empty-only listing must not include the live group {group_id}; seeing it here means \
         the states filter never reached the broker"
    );

    // types: Consumer includes it, Classic excludes it.
    assert!(
        lists(
            &admin,
            ListGroupsOptions::new().with_types(HashSet::from([GroupType::Consumer])),
            &group_id,
            "types=Consumer",
        )
        .await,
        "{backend} backend: a Consumer-only listing must include the KIP-848 group {group_id}"
    );
    assert!(
        !lists(
            &admin,
            ListGroupsOptions::new().with_types(HashSet::from([GroupType::Classic])),
            &group_id,
            "types=Classic",
        )
        .await,
        "{backend} backend: a Classic-only listing must not include the KIP-848 group {group_id}; seeing it here \
         means the types filter never reached the broker"
    );

    // protocol_types: the group's own protocol includes it, a made-up one does
    // not. This filter is applied client-side, so it exercises a different layer.
    assert!(
        lists(
            &admin,
            ListGroupsOptions::new().with_protocol_types(HashSet::from([protocol.clone()])),
            &group_id,
            "protocol_types=self",
        )
        .await,
        "{backend} backend: filtering by the group's own protocol type ({protocol:?}) must include {group_id}"
    );
    assert!(
        !lists(
            &admin,
            ListGroupsOptions::new().with_protocol_types(HashSet::from(["not-a-protocol-type".to_string()])),
            &group_id,
            "protocol_types=bogus",
        )
        .await,
        "{backend} backend: filtering by a protocol type no group uses must exclude {group_id}; seeing it here \
         means the protocol-type filter was dropped"
    );

    drop(consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// describeConsumerGroups / describeClassicGroups
// ---------------------------------------------------------------------------

/// `describe_consumer_groups` on a live group reports one member owning every
/// partition — and a coordinator with a real endpoint.
async fn describe_consumer_groups_live_group<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_groups_describe");
    let group_id = ctx.group_id("g_describe");

    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;

    let mut consumer = new_bytes_consumer(&bootstrap, &group_id);
    let assigned = subscribe_and_join(&mut consumer, &topic).await;
    assert_eq!(
        assigned, NUM_PARTITIONS as usize,
        "{backend} backend: sole member should own every partition"
    );

    let mut described_stable = None;
    for _ in 0..40 {
        let _ = consumer.poll(Duration::from_millis(200)).await;
        let described = admin
            .describe_consumer_groups(std::slice::from_ref(&group_id), DescribeConsumerGroupsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: describe consumer groups: {e}"));
        let desc = described
            .get(&group_id)
            .unwrap_or_else(|| panic!("{backend} backend: {group_id} missing from the describe result"))
            .as_ref()
            .unwrap_or_else(|e| panic!("{backend} backend: describe group {group_id}: {e}"))
            .clone();
        assert_eq!(desc.group_id(), group_id);
        assert_eq!(
            desc.group_type(),
            GroupType::Consumer,
            "{backend} backend: a KIP-848 group describes as Consumer"
        );
        if desc.group_state() == GroupState::Stable && desc.members().len() == 1 {
            described_stable = Some(desc);
            break;
        }
    }
    let desc = described_stable.unwrap_or_else(|| {
        panic!("{backend} backend: describe_consumer_groups should report {group_id} Stable with one member")
    });

    let member = &desc.members()[0];
    let owned: HashSet<i32> = member.assignment().topic_partitions().iter().map(|tp| tp.partition()).collect();
    assert_eq!(
        owned.len(),
        NUM_PARTITIONS as usize,
        "{backend} backend: the sole member should own all partitions, got {owned:?}"
    );
    assert!(
        member.assignment().topic_partitions().iter().all(|tp| tp.topic() == topic),
        "{backend} backend: every assigned partition belongs to {topic}"
    );
    // The member's own identity crosses too: a dynamic member has no
    // group.instance.id (Java's empty Optional), which must not arrive as "".
    assert!(
        !member.consumer_id().is_empty(),
        "{backend} backend: the broker assigns every member a consumer id"
    );
    assert!(
        !member.client_id().is_empty(),
        "{backend} backend: the member reports the client.id it connected with"
    );
    assert!(
        !member.host().is_empty(),
        "{backend} backend: the broker reports the member's host"
    );
    assert_eq!(
        member.group_instance_id(),
        None,
        "{backend} backend: a member without group.instance.id must report Java's empty Optional, not \"\""
    );

    // The coordinator: the field this whole milestone exists for.
    assert_real_coordinator(&admin, desc.coordinator(), "describeConsumerGroups").await;

    // `authorized_operations` is Java's *nullable* set. The request did not ask
    // for them, so the broker reports none at all — absent, which is not the same
    // as an empty set.
    assert!(
        desc.authorized_operations().is_none(),
        "{backend} backend: without includeAuthorizedOperations the broker reports no operations at all, got {:?}",
        desc.authorized_operations()
    );

    drop(consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// `describe_consumer_groups(includeAuthorizedOperations)` populates the
/// nullable operation set, and the members' KIP-848 epochs cross.
///
/// Added, not converted. Two things only this scenario reaches:
///
///   - `DescribeConsumerGroupsOptions.includeAuthorizedOperations(true)` — the
///     option is plumbed through all four backends and otherwise never set, and
///     the *absent* case is what every other scenario sees. A `User:ANONYMOUS`
///     caller on an authorizer-less KRaft broker is a super user, so the set
///     comes back populated rather than empty (the same reachability surprise G2
///     recorded for `describeCluster`).
///   - `groupEpoch` / `targetAssignmentEpoch` / `memberEpoch`, Java `Optional`s
///     that are empty for a classic group and present for a KIP-848 one. An
///     encoder that decoded an absent epoch as 0 passes elsewhere; here a present
///     epoch must be `>= 1`, because a group that has reconciled has bumped it.
async fn describe_consumer_groups_reports_operations_and_epochs<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_groups_ops");
    let group_id = ctx.group_id("g_ops");

    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;
    let mut consumer = new_bytes_consumer(&bootstrap, &group_id);
    subscribe_and_join(&mut consumer, &topic).await;

    let mut described = None;
    for _ in 0..40 {
        let _ = consumer.poll(Duration::from_millis(200)).await;
        let outcomes = admin
            .describe_consumer_groups(
                std::slice::from_ref(&group_id),
                DescribeConsumerGroupsOptions::new().include_authorized_operations(true),
            )
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: describe consumer groups: {e}"));
        let desc = outcomes
            .get(&group_id)
            .unwrap_or_else(|| panic!("{backend} backend: {group_id} missing from the describe result"))
            .as_ref()
            .unwrap_or_else(|e| panic!("{backend} backend: describe group {group_id}: {e}"))
            .clone();
        if desc.group_state() == GroupState::Stable && !desc.members().is_empty() {
            described = Some(desc);
            break;
        }
    }
    let desc = described.unwrap_or_else(|| panic!("{backend} backend: {group_id} never became Stable with a member"));

    // A KRaft broker with no authorizer still computes the operations, because
    // User:ANONYMOUS is a super user. So the *populated* case is the reachable
    // one here and the null case is what the no-option scenario above covers.
    let operations = desc.authorized_operations().unwrap_or_else(|| {
        panic!(
            "{backend} backend: with includeAuthorizedOperations the broker reports a set; absent here means the \
             option never reached it"
        )
    });
    assert!(
        !operations.is_empty(),
        "{backend} backend: a super user's authorized operations on a group are not empty"
    );

    // KIP-848 epochs. Present, and a reconciled group has bumped them past 0 —
    // which is what separates "the epoch crossed" from "an absent Optional was
    // decoded as 0".
    let group_epoch = desc
        .group_epoch()
        .unwrap_or_else(|| panic!("{backend} backend: a KIP-848 group reports a group epoch"));
    assert!(
        group_epoch >= 1,
        "{backend} backend: a reconciled group's epoch is at least 1, got {group_epoch}"
    );
    let target_epoch = desc
        .target_assignment_epoch()
        .unwrap_or_else(|| panic!("{backend} backend: a KIP-848 group reports a target assignment epoch"));
    assert!(
        target_epoch >= 1,
        "{backend} backend: a reconciled group's target assignment epoch is at least 1, got {target_epoch}"
    );
    let member_epoch = desc.members()[0]
        .member_epoch()
        .unwrap_or_else(|| panic!("{backend} backend: a KIP-848 member reports a member epoch"));
    assert!(
        member_epoch >= 1,
        "{backend} backend: a reconciled member's epoch is at least 1, got {member_epoch}"
    );

    drop(consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// `describe_consumer_groups` over **several** groups reports exactly the
/// requested key set, each with a real coordinator.
///
/// Added, not converted. Every converted scenario describes a single group, and a
/// one-key response cannot detect a backend that keyed its entries by loop index,
/// dropped an entry, or answered with a short list — the silently-reduced-
/// cardinality failure the G1 Critic found once already in `listTopics`. The
/// assertion is [`all_of_exactly`], which fails on a response whose key set is not
/// exactly the requested one, rather than the cardinality-blind `all_of` fold.
///
/// It also runs on a cluster with **three `__consumer_offsets` partitions**
/// instead of one, so the three groups can hash to different coordinators and the
/// `CoordinatorStrategy` lookup has to fan out and merge. Which coordinator a
/// group lands on is `Utils.abs(groupId.hashCode) % numPartitions`, so the number
/// of distinct coordinators is not something a test can pin — the scenario asserts
/// the per-group invariant (every coordinator is a real broker `describeCluster`
/// knows) for each group independently, which holds however they hash.
async fn describe_consumer_groups_batches_several_groups<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_groups_batch");

    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;

    let group_ids: Vec<String> = (0..3).map(|i| ctx.group_id(&format!("g_batch_{i}"))).collect();
    let mut consumers = Vec::new();
    for group_id in &group_ids {
        let mut consumer = new_bytes_consumer(&bootstrap, group_id);
        subscribe_and_join(&mut consumer, &topic).await;
        consumers.push(consumer);
    }

    // Poll every member while waiting for all three groups to be describable.
    let mut described = None;
    for _ in 0..40 {
        for consumer in consumers.iter_mut() {
            let _ = consumer.poll(Duration::from_millis(100)).await;
        }
        let outcomes = admin
            .describe_consumer_groups(&group_ids, DescribeConsumerGroupsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: describe consumer groups: {e}"));
        let all_stable = group_ids
            .iter()
            .all(|id| matches!(outcomes.get(id), Some(Ok(desc)) if desc.group_state() == GroupState::Stable));
        if all_stable {
            described = Some(outcomes);
            break;
        }
    }
    let described = described.unwrap_or_else(|| panic!("{backend} backend: all three groups should become Stable"));

    // Exactly the requested keys, and every one of them succeeded. This is the
    // check `all_of` alone cannot make.
    all_of_exactly(&admin, &described, &group_ids, "describeConsumerGroups over three groups");

    for group_id in &group_ids {
        let desc = described[group_id]
            .as_ref()
            .unwrap_or_else(|e| panic!("{backend} backend: describe {group_id}: {e}"));
        assert_eq!(
            desc.group_id(),
            group_id,
            "{backend} backend: each entry's description must carry the group id it is keyed by; a mismatch here \
             means entries were paired with keys by position"
        );
        assert_real_coordinator(&admin, desc.coordinator(), &format!("describeConsumerGroups({group_id})")).await;
    }

    drop(consumers);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// Describing a group that was never created.
///
/// The dual-protocol handler first issues `ConsumerGroupDescribe` (->
/// GROUP_ID_NOT_FOUND) then falls back to the classic `DescribeGroups`. Whether
/// the coordinator surfaces GROUP_ID_NOT_FOUND or a classic "Dead" placeholder
/// group on that fallback is broker-version dependent, so either faithful outcome
/// is accepted — the one thing that must NOT happen is a live/`Stable` group
/// being reported.
async fn describe_consumer_groups_nonexistent_group<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let missing = ctx.group_id("g_does_not_exist");

    let described = admin
        .describe_consumer_groups(std::slice::from_ref(&missing), DescribeConsumerGroupsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe consumer groups: {e}"));
    match described
        .get(&missing)
        .unwrap_or_else(|| panic!("{backend} backend: {missing} missing from the describe result"))
    {
        Err(err) => {
            assert_eq!(
                err.error(),
                Errors::GroupIdNotFound,
                "{backend} backend: nonexistent group should fail with GROUP_ID_NOT_FOUND, got: {err}"
            );
        },
        Ok(desc) => {
            assert!(
                desc.members().is_empty(),
                "{backend} backend: nonexistent group must have no members, got state {:?}",
                desc.group_state()
            );
            assert_ne!(
                desc.group_state(),
                GroupState::Stable,
                "{backend} backend: nonexistent group must not be reported as Stable"
            );
        },
    }

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// `describe_classic_groups` on a live **KIP-848** group answers
/// `GROUP_ID_NOT_FOUND` per key.
///
/// Added, not converted, and it is the only end-to-end exercise of
/// `describeClassicGroups` this fixture can reach — see the module docs for why a
/// live classic group is not creatable here. It is nonetheless deterministic
/// rather than a compromise: `GroupMetadataManager.describeGroups`
/// (`group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java:735-786`)
/// resolves the group through `classicGroup(groupId, committedOffset)`, which
/// throws `GroupIdNotFoundException` for a group that is not a `ClassicGroup`,
/// and for request version >= 6 the coordinator then reports
/// `GROUP_ID_NOT_FOUND` with group state `Dead`.
///
/// What that covers: the whole `describeClassicGroups` path — the wire wrapper,
/// the `CoordinatorStrategy` lookup, and the per-key **error** arm of
/// `DescribeClassicGroupsEntry`, which no other scenario takes. It also pins that
/// the two describe RPCs are not aliases of one another: the same group id
/// resolves for `describeConsumerGroups` and not for `describeClassicGroups`, so
/// a backend that routed one to the other fails here.
async fn describe_classic_groups_rejects_a_kip848_group<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_classic_describe");
    let group_id = ctx.group_id("g_classic");

    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;
    let mut consumer = new_bytes_consumer(&bootstrap, &group_id);
    subscribe_and_join(&mut consumer, &topic).await;
    let _ = consumer.poll(Duration::from_millis(200)).await;

    // The consumer-protocol describe resolves it...
    let as_consumer = admin
        .describe_consumer_groups(std::slice::from_ref(&group_id), DescribeConsumerGroupsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe consumer groups: {e}"));
    as_consumer
        .get(&group_id)
        .unwrap_or_else(|| panic!("{backend} backend: {group_id} missing from the consumer describe result"))
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: the live group must describe as a consumer group: {e}"));

    // ...and the classic describe does not.
    let as_classic = admin
        .describe_classic_groups(std::slice::from_ref(&group_id), DescribeClassicGroupsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe classic groups: {e}"));
    let err = as_classic
        .get(&group_id)
        .unwrap_or_else(|| panic!("{backend} backend: {group_id} missing from the classic describe result"))
        .as_ref()
        .expect_err(&format!(
            "{backend} backend: a KIP-848 group is not a classic group, so describeClassicGroups must fail for it"
        ));
    assert_eq!(
        err.error(),
        Errors::GroupIdNotFound,
        "{backend} backend: the coordinator reports GROUP_ID_NOT_FOUND for a group that is not a ClassicGroup, \
         got: {err}"
    );

    drop(consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// A **simple classic group**, created by an admin offset commit, describes
/// successfully through *both* describe RPCs.
///
/// # Why this scenario exists
///
/// It is the only route to four things the rest of the file cannot reach, and
/// three of them were previously encoded by three servers and decoded by one
/// client without ever carrying a value:
///
///   1. `ClassicGroupDescription`'s whole **value** arm — `protocol`,
///      `protocolData`, `state`, `members`, `authorizedOperations` and, above
///      all, `coordinator`.
///   2. **`ClassicGroupDescription::coordinator()` is a second, independent
///      decode site** from `ConsumerGroupDescription`'s
///      (`MultilanguageAdmin::classic_group_description` vs
///      `::consumer_group_description`), and it was the one
///      [`assert_real_coordinator`] was never pointed at. The fabricated-`Node`
///      defect this whole milestone exists for had an unguarded twin on the
///      classic path; this scenario guards it.
///   3. `describeConsumerGroups`' **classic-fallback value** path.
///      `DescribeConsumerGroupsHandler.handleError` moves a `GROUP_ID_NOT_FOUND`
///      group into `useClassicGroupApi` and retries with `DescribeGroups`
///      (`clients/src/main/java/org/apache/kafka/clients/admin/internals/DescribeConsumerGroupsHandler.java:398-412`);
///      `handledClassicGroupResponse` then builds a `ConsumerGroupDescription`
///      with `GroupType.CLASSIC` and `isSimpleConsumerGroup =
///      protocolType.isEmpty()` (`:251-305`). The Rust translation
///      (`src/admin/internals/describe_consumer_groups_handler.rs`) was
///      unit-tested but never crossed a language boundary.
///   4. `is_simple_consumer_group == true` on either type. Java *derives* it, so
///      `MultilanguageAdmin` re-derives it and fails the call when the wire
///      disagrees — but with only `Consumer`-type groups in the suite,
///      `GroupListing`'s check only ever compared `false == false` and
///      `ClassicGroupDescription`'s never ran at all. A backend hardcoding
///      `false` was invisible, which is the exact failure mode the check exists
///      to prevent.
///
/// # Why an offset commit creates a classic group
///
/// See the module doc: a negative generation id on an unknown group makes
/// `OffsetMetadataManager.validateOffsetCommit` create a simple group
/// (`OffsetMetadataManager.java:458-467`), and the admin handler never sets the
/// generation id, so `-1` is what the wire carries
/// (`OffsetCommitRequest.json:46`). Describing that group then takes the
/// **non-STABLE** branch of `GroupMetadataManager.describeGroups`
/// (`GroupMetadataManager.java:759-767`), which returns a *value* with an empty
/// protocol type and no members — not the `GROUP_ID_NOT_FOUND` error its sibling
/// [`describe_classic_groups_rejects_a_kip848_group`] asserts for a KIP-848
/// group. The two scenarios together pin that the RPC discriminates by group
/// *type* rather than by existence.
async fn describe_a_simple_classic_group<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let topic = ctx.topic("admin_simple_classic");
    let group_id = ctx.group_id("g_simple_classic");

    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;

    // No consumer ever joins this group id: the commit itself is what creates it,
    // as a *simple* (classic, protocol-less) group.
    let tp = TopicPartition::new(topic.clone(), 0);
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(1).expect("valid offset"))]);
    let committed = admin
        .alter_consumer_group_offsets(&group_id, &offsets, AlterConsumerGroupOffsetsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commit an offset for a never-consumed group: {e}"));
    all_of_exactly(&admin, &committed, std::slice::from_ref(&tp), "alterConsumerGroupOffsets");

    // (1) describeClassicGroups now takes its value arm.
    let as_classic = admin
        .describe_classic_groups(std::slice::from_ref(&group_id), DescribeClassicGroupsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe classic groups: {e}"));
    let classic = as_classic
        .get(&group_id)
        .unwrap_or_else(|| panic!("{backend} backend: {group_id} missing from the classic describe result"))
        .as_ref()
        .unwrap_or_else(|e| {
            panic!("{backend} backend: a simple classic group must describe as a classic group, got: {e}")
        });

    assert_eq!(classic.group_id(), group_id, "{backend} backend: the described group id");
    // A group created by an offset commit has no protocol type and no protocol
    // data: `getOrMaybeCreateClassicGroup` leaves `protocolType` empty. Asserting
    // the empty string rather than skipping the field is what keeps a backend
    // that substituted the group id (a transposition) visible.
    assert_eq!(classic.protocol(), "", "{backend} backend: a simple group has no protocol type");
    assert_eq!(classic.protocol_data(), "", "{backend} backend: a simple group has no protocol data");
    assert_eq!(
        classic.state(),
        ClassicGroupState::Empty,
        "{backend} backend: a group with a committed offset and no members is Empty, got {:?}",
        classic.state()
    );
    assert!(
        classic.members().is_empty(),
        "{backend} backend: no member ever joined, got {:?}",
        classic.members()
    );
    // (4) Java derives this from `protocol.isEmpty()`, and it is `true` here — the
    // first time either derivation check in `MultilanguageAdmin` sees anything but
    // `false`.
    assert!(
        classic.is_simple_consumer_group(),
        "{backend} backend: a group with an empty protocol type is a simple consumer group"
    );
    // The broker computes authorized operations even with no authorizer
    // configured, but `DescribeClassicGroupsOptions` does not request them by
    // default, so they are absent — which must not decode as an empty set.
    assert_eq!(
        classic.authorized_operations(),
        None,
        "{backend} backend: authorized operations were not requested, so they must be absent rather than empty"
    );
    // (2) The unguarded twin.
    assert_real_coordinator(&admin, classic.coordinator(), "describeClassicGroups").await;

    // (3) describeConsumerGroups reaches the same group through its classic
    // fallback: the KIP-848 describe answers GROUP_ID_NOT_FOUND, the handler
    // retries with DescribeGroups, and the result is a CLASSIC-typed description.
    let as_consumer = admin
        .describe_consumer_groups(std::slice::from_ref(&group_id), DescribeConsumerGroupsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe consumer groups: {e}"));
    let consumer = as_consumer
        .get(&group_id)
        .unwrap_or_else(|| panic!("{backend} backend: {group_id} missing from the consumer describe result"))
        .as_ref()
        .unwrap_or_else(|e| {
            panic!("{backend} backend: the classic fallback must describe a simple classic group, got: {e}")
        });
    assert_eq!(
        consumer.group_type(),
        GroupType::Classic,
        "{backend} backend: the fallback path reports GroupType.CLASSIC, got {:?}",
        consumer.group_type()
    );
    assert_eq!(
        consumer.group_state(),
        GroupState::Empty,
        "{backend} backend: the fallback path reports the classic group's state"
    );
    assert!(
        consumer.is_simple_consumer_group(),
        "{backend} backend: handledClassicGroupResponse sets isSimpleConsumerGroup = protocolType.isEmpty()"
    );
    assert_eq!(
        consumer.partition_assignor(),
        "",
        "{backend} backend: a simple group has no assignor"
    );
    // The fallback builds the description from a `DescribeGroups` response, which
    // carries no group epoch or target-assignment epoch at all.
    assert_eq!(
        consumer.group_epoch(),
        None,
        "{backend} backend: DescribeGroups carries no group epoch"
    );
    assert_eq!(
        consumer.target_assignment_epoch(),
        None,
        "{backend} backend: DescribeGroups carries no target-assignment epoch"
    );
    assert_real_coordinator(&admin, consumer.coordinator(), "describeConsumerGroups (classic fallback)").await;

    // And the inclusive direction of the types filter for a non-`Consumer` type,
    // which is only exercised in its exclusive direction elsewhere
    // (`list_groups_filters`).
    let listed = admin
        .list_groups(ListGroupsOptions::new().with_types(HashSet::from([GroupType::Classic])))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list groups (types=Classic): {e}"));
    listed
        .all()
        .unwrap_or_else(|e| panic!("{backend} backend: list groups (types=Classic) reported a per-broker error: {e}"));
    let listing = listed
        .valid
        .iter()
        .find(|g| g.group_id() == group_id)
        .unwrap_or_else(|| {
            panic!(
                "{backend} backend: a Classic-only listing must include the simple classic group {group_id}, got {:?}",
                listed.valid
            )
        });
    assert_eq!(
        listing.group_type(),
        Some(GroupType::Classic),
        "{backend} backend: the listing's own type"
    );
    // `GroupListing.isSimpleConsumerGroup()` is `type == CLASSIC &&
    // protocol.isEmpty()`, so this is where its derivation check first sees
    // `true`.
    assert!(
        listing.is_simple_consumer_group(),
        "{backend} backend: a protocol-less classic group listing is a simple consumer group"
    );

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// deleteConsumerGroups
// ---------------------------------------------------------------------------

/// (a) An empty (member-less but retained) group can be deleted, and (b)
/// deleting a group that still has an active member fails with a non-retriable
/// `NON_EMPTY_GROUP` error.
async fn delete_consumer_groups_empty_and_non_empty<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_delete_groups");
    let empty_group = ctx.group_id("g_delete_empty");
    let live_group = ctx.group_id("g_delete_live");

    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;

    // Bring a group up, commit an offset so it is retained, then close the
    // consumer so the group becomes empty (member-less) but still exists.
    {
        let mut consumer = new_bytes_consumer(&bootstrap, &empty_group);
        subscribe_and_join(&mut consumer, &topic).await;
        let _ = consumer.poll(Duration::from_millis(500)).await;
        consumer.commit_sync().await.expect("commit offsets");
        consumer.close().await.expect("close consumer");
    }

    // (b) A group with an active member cannot be deleted (NON_EMPTY_GROUP).
    let mut live_consumer = new_bytes_consumer(&bootstrap, &live_group);
    subscribe_and_join(&mut live_consumer, &topic).await;
    let _ = live_consumer.poll(Duration::from_millis(200)).await;

    let deleted = admin
        .delete_consumer_groups(std::slice::from_ref(&live_group), DeleteConsumerGroupsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete consumer groups: {e}"));
    let non_empty_err = deleted
        .get(&live_group)
        .unwrap_or_else(|| panic!("{backend} backend: {live_group} missing from the delete result"))
        .as_ref()
        .expect_err(&format!("{backend} backend: deleting a group with active members must fail"));
    assert_eq!(
        non_empty_err.error(),
        Errors::NonEmptyGroup,
        "{backend} backend: deleting a non-empty group should fail with NON_EMPTY_GROUP, got: {non_empty_err}"
    );
    assert!(
        !non_empty_err.is_retriable(),
        "{backend} backend: NON_EMPTY_GROUP is a non-retriable error"
    );

    // (a) The empty group deletes successfully.
    let deleted = admin
        .delete_consumer_groups(std::slice::from_ref(&empty_group), DeleteConsumerGroupsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete consumer groups: {e}"));
    all_of_exactly(
        &admin,
        &deleted,
        std::slice::from_ref(&empty_group),
        "deleteConsumerGroups on an empty group",
    );

    // ... and is gone afterwards.
    let described = admin
        .describe_consumer_groups(std::slice::from_ref(&empty_group), DescribeConsumerGroupsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe consumer groups: {e}"));
    match described
        .get(&empty_group)
        .unwrap_or_else(|| panic!("{backend} backend: {empty_group} missing from the describe result"))
    {
        Err(err) => assert_eq!(
            err.error(),
            Errors::GroupIdNotFound,
            "{backend} backend: a deleted group is GROUP_ID_NOT_FOUND, got: {err}"
        ),
        Ok(desc) => assert!(
            desc.members().is_empty(),
            "{backend} backend: deleted group must have no members"
        ),
    }

    drop(live_consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// removeMembersFromConsumerGroup
// ---------------------------------------------------------------------------

/// `remove_members_from_consumer_group` for a single static member removes it,
/// and its partitions are reassigned to the other member.
///
/// This is the *explicit-members* arm of the `removeAll` discriminant: the
/// per-member result map is keyed by `group.instance.id`, so it is also the only
/// scenario that exercises `RemoveMembersFromConsumerGroupResult::member_result`.
async fn remove_one_member_from_consumer_group<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_remove_member");
    let group_id = ctx.group_id("g_remove_one");

    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;

    // Two static members share the topic's partitions.
    let mut member_one = new_static_bytes_consumer(&bootstrap, &group_id, "instance-1");
    let mut member_two = new_static_bytes_consumer(&bootstrap, &group_id, "instance-2");
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
    assert!(
        !options.remove_all(),
        "{backend} backend: options built from a non-empty member set are not removeAll"
    );
    let removed = admin
        .remove_members_from_consumer_group(&group_id, options)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: remove members: {e}"));
    // The result is keyed per member, unlike the removeAll arm, which reports
    // nothing per key at all. `all_of_exactly` is what pins the key set: an
    // `all_of` fold over an empty response would report success.
    all_of_exactly(
        &admin,
        &removed,
        &["instance-1".to_string()],
        "removeMembersFromConsumerGroup for one static member",
    );

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
        "{backend} backend: the remaining member should be reassigned all partitions after removal"
    );

    drop(member_one);
    drop(member_two);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// `remove_members_from_consumer_group` with `removeAll` empties the group, and
/// reports nothing per key.
///
/// This is the *absent-selection* arm of the discriminant that
/// `admin_service.proto`'s `RemoveMembersFromConsumerGroupRequest` documents. It
/// is genuinely preserved rather than re-derived from emptiness: the C entry
/// points take a dedicated `bool remove_all`, `admin.py`'s `_remove_members_rows`
/// computes `members is None` into its own column, and the wire wraps the list in
/// an `optional MemberToRemoveList`. The two directions are covered here and in
/// [`remove_members_rejects_an_explicitly_empty_selection`].
async fn remove_all_members_from_consumer_group<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("admin_remove_all");
    let group_id = ctx.group_id("g_remove_all");

    create_topic(&admin, &topic, NUM_PARTITIONS, 1).await;

    let mut consumer = new_static_bytes_consumer(&bootstrap, &group_id, "instance-1");
    subscribe_and_join(&mut consumer, &topic).await;

    // removeAll: no specific members provided.
    let options = RemoveMembersFromConsumerGroupOptions::default();
    assert!(
        options.remove_all(),
        "{backend} backend: default options are Java's no-argument constructor, i.e. removeAll"
    );
    let removed = admin
        .remove_members_from_consumer_group(&group_id, options)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: removeAll should succeed: {e}"));
    // In removeAll mode Java's `memberResult` is not applicable, so there is no
    // per-key outcome at all — `all()` was the only observable, and it is the
    // `Ok` above.
    assert!(
        removed.is_empty(),
        "{backend} backend: removeAll reports no per-member outcome (Java's memberResult refuses in that mode), \
         got {removed:?}"
    );

    // The group should have no active members afterwards.
    let mut emptied = false;
    for _ in 0..40 {
        let described = admin
            .describe_consumer_groups(std::slice::from_ref(&group_id), DescribeConsumerGroupsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: describe consumer groups: {e}"));
        match described.get(&group_id) {
            Some(Ok(desc)) if desc.members().is_empty() => {
                emptied = true;
                break;
            },
            Some(Err(err)) if err.error() == Errors::GroupIdNotFound => {
                emptied = true;
                break;
            },
            _ => {},
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        emptied,
        "{backend} backend: removeAll should leave the group with no active members"
    );

    drop(consumer);
    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// An explicitly **empty** member selection is rejected, and is not silently
/// promoted to `removeAll`.
///
/// Added, not converted. This is the other half of the `removeAll` distinction,
/// and the dangerous direction: `removeAll` empties the whole group, so treating
/// "an empty member list" as "remove everything" would destroy state a caller
/// never asked to touch.
///
/// Java makes the two different at the constructor:
/// `RemoveMembersFromConsumerGroupOptions(Collection)` throws
/// `IllegalArgumentException("Invalid empty members has been provided")` on an
/// empty collection
/// (`clients/src/main/java/org/apache/kafka/clients/admin/RemoveMembersFromConsumerGroupOptions.java:33-37`),
/// while the no-argument constructor is the removeAll form; `removeAll()` is then
/// literally `members.isEmpty()` (`:57-59`).
///
/// **Scope of what this proves, stated exactly.** The rejection is client-side
/// and happens in the *harness's* input type: `RemoveMembersFromConsumerGroupOptions`
/// is what `AdminBackend` takes, and it cannot be constructed empty, so a
/// scenario cannot put a present-but-empty member list on the wire at all. The
/// first half below therefore asserts the Rust/Java constructor gate and its exact
/// message — identical work on all four arms, since it never leaves the test
/// process. The second half *does* cross the wire, and asserts the weaker but
/// backend-specific property that `removeAll` is not rejected as though it were an
/// empty explicit selection.
///
/// Reaching the present-but-empty wire state would need a harness escape hatch
/// that builds the proto directly, which the native arm could not participate in;
/// it is reported as a coverage limit rather than papered over. What keeps the two
/// states apart in production is the code path, not this scenario: an
/// `optional MemberToRemoveList` on the wire, the C entry points' dedicated
/// `bool remove_all`, and `admin.py`'s `members is None` column — never an
/// emptiness test.
async fn remove_members_rejects_an_explicitly_empty_selection<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();
    let group_id = ctx.group_id("g_remove_empty");

    // Java's constructor is the gate, and Rust mirrors it — so the empty
    // selection is unrepresentable as an options object in the first place. That
    // is the assertion: every backend's binding must refuse it the same way,
    // rather than one of them defaulting to the destructive removeAll form.
    let rejected = RemoveMembersFromConsumerGroupOptions::new(Vec::<MemberToRemove>::new());
    let err = rejected.expect_err(&format!(
        "{backend} backend: an empty member collection must be rejected, not silently treated as removeAll"
    ));
    assert!(
        matches!(err, KafkaError::IllegalArgument(_)),
        "{backend} backend: Java throws IllegalArgumentException here, got {err:?}"
    );
    assert_eq!(
        err.message(),
        "Invalid empty members has been provided",
        "{backend} backend: the exact Java message is part of the contract"
    );

    // And the removeAll form really is the *other* value, not the same one: it is
    // accepted, and it targets a group that does not exist, so the coordinator —
    // not a client-side guard — decides the outcome. Whatever that outcome is, it
    // must be the same on all four backends.
    let removed = admin
        .remove_members_from_consumer_group(&group_id, RemoveMembersFromConsumerGroupOptions::default())
        .await;
    match removed {
        // A group with no members: the broker may accept the (empty) removal or
        // reject it. Both are faithful; what must not happen is a client-side
        // IllegalArgument, which would mean the default options were treated as
        // an empty explicit selection.
        Ok(outcomes) => assert!(
            outcomes.is_empty(),
            "{backend} backend: removeAll never reports a per-member outcome, got {outcomes:?}"
        ),
        // Discriminated by *message*, not by variant, and deliberately so.
        //
        // The variant test that used to stand here (`!matches!(err,
        // KafkaError::IllegalArgument(_))`) passes only *because* of an open
        // deferred defect. Java's outcome for the branch this call can land in
        // **is** an `IllegalArgumentException`: `removeAll` resolves its member
        // list through `getMembersFromGroup`
        // (`clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java:4169-4187`),
        // and an empty list reaches `LeaveGroupRequest.Builder`, which throws
        // `IllegalArgumentException("leaving members should not be empty")`
        // (`clients/src/main/java/org/apache/kafka/common/requests/LeaveGroupRequest.java:45-46`).
        // Rust reproduces the message at
        // `src/common/requests/leave_group_request.rs:170` but surfaces it as
        // `UNSUPPORTED_VERSION`, because `RequestBuilder::build_version` returns
        // an `io::Error` and `src/network_client.rs:507` stamps every such
        // failure `UnsupportedVersionError` — which is DEFERRED 3 in
        // `COMMENTS.DONE.1.md`. The moment DEFERRED 3 is fixed to
        // `KafkaError::illegal_argument`, a variant test here would start failing
        // on all four backends for a *correct* client.
        //
        // What this scenario actually means is "not the options-constructor
        // rejection", and that gate has one exact message, which survives the
        // DEFERRED-3 fix untouched.
        Err(err) => assert_ne!(
            err.message(),
            "Invalid empty members has been provided",
            "{backend} backend: removeAll must not be rejected by the empty-collection constructor gate, got {err:?}"
        ),
    }

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_ml_admin_list_groups_and_list_consumer_groups_show_live_group,
    list_groups_and_list_consumer_groups_show_live_group,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_list_groups_filters_restrict_the_listing,
    list_groups_filters_restrict_the_listing,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_describe_consumer_groups_live_group,
    describe_consumer_groups_live_group,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_describe_consumer_groups_reports_operations_and_epochs,
    describe_consumer_groups_reports_operations_and_epochs,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_describe_consumer_groups_batches_several_groups,
    describe_consumer_groups_batches_several_groups,
    kip848_partitioned_offsets(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_describe_consumer_groups_nonexistent_group,
    describe_consumer_groups_nonexistent_group,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_describe_a_simple_classic_group,
    describe_a_simple_classic_group,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_describe_classic_groups_rejects_a_kip848_group,
    describe_classic_groups_rejects_a_kip848_group,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_delete_consumer_groups_empty_and_non_empty,
    delete_consumer_groups_empty_and_non_empty,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_remove_one_member_from_consumer_group,
    remove_one_member_from_consumer_group,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_remove_all_members_from_consumer_group,
    remove_all_members_from_consumer_group,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
multilanguage_admin_test!(
    test_ml_admin_remove_members_rejects_an_explicitly_empty_selection,
    remove_members_rejects_an_explicitly_empty_selection,
    kip848_3_broker(NUM_PARTITIONS as u16)
);
