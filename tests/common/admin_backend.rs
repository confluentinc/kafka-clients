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

//! The harness-local admin surface driven by the multilanguage integration
//! tests, plus the native-Rust implementation of it.
//!
//! See `design/history/Milestone-11/PLAN-multilanguage-admin.md` §D1 for the
//! decision this file implements.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, AlterConfigOp, AlterConfigsOptions, AlterConsumerGroupOffsetsOptions,
    AlterPartitionReassignmentsOptions, AlterReplicaLogDirsOptions, ClassicGroupDescription, Config, ConfigEntry,
    ConfigSource, ConfigType, ConsumerGroupDescription, CreatePartitionsOptions, CreateTopicsOptions,
    CreateTopicsResult, DeleteConsumerGroupOffsetsOptions, DeleteConsumerGroupsOptions, DeleteRecordsOptions,
    DeleteTopicsOptions, DeletedRecords, DescribeClassicGroupsOptions, DescribeClusterOptions,
    DescribeConfigsOptions, DescribeConsumerGroupsOptions, DescribeLogDirsOptions, DescribeReplicaLogDirsOptions,
    DescribeTopicsOptions, ElectLeadersOptions, GroupListing, GroupOffsets, ListConfigResourcesOptions,
    ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsSpec, ListGroupsOptions, ListOffsetsOptions,
    ListOffsetsResultInfo, ListPartitionReassignmentsOptions, ListTopicsOptions, LogDirDescription, MockAdminClient,
    NewPartitionReassignment, NewPartitions, NewTopic, OffsetSpec, PartitionReassignment, RecordsToDelete,
    RemoveMembersFromConsumerGroupOptions, TopicDescription, TopicListing, TopicMetadataAndConfig, new_admin_client,
};
#[allow(deprecated)]
use confluent_kafka::admin::{
    ClientMetricsResourceListing, ConsumerGroupListing, ListClientMetricsResourcesOptions, ListConsumerGroupsOptions,
};
use confluent_kafka::common::acl::AclOperation;
use confluent_kafka::common::config::{ConfigResource, ConfigResourceType};
use confluent_kafka::common::{
    ElectionType, KafkaError, KafkaFuture, Node, TopicCollection, TopicPartition, TopicPartitionReplica, Uuid,
};
use confluent_kafka::consumer::OffsetAndMetadata;

use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::common::test_utils::{DEFAULT_PAUSE_MS, TOPIC_METADATA_PROPAGATION_WAIT_MS, wait_until_true_with_timeout};

/// Timeout that stands for Java's no-argument `Admin.close()`, which delegates
/// to `close(Duration.ofMillis(Long.MAX_VALUE))`. Same convention the C FFI
/// uses for a negative `timeout_ms` (`src/ffi/admin.rs::close_timeout`).
fn close_timeout(timeout: Option<Duration>) -> Duration {
    timeout.unwrap_or_else(|| Duration::from_millis(i64::MAX as u64))
}

/// The admin surface the multilanguage scenarios are written against.
///
/// # Why this is not the production [`Admin`] trait
///
/// The consumer harness implements the real `Consumer` trait, so consumer
/// scenarios are generic over production code. Admin cannot do that, for two
/// independent reasons:
///
///   - 33 of the 46 `*Result` types declare `pub(crate) fn new` (e.g.
///     `src/admin/list_topics_result.rs`), so an integration test — a separate
///     crate — cannot construct one to return from `Admin::list_topics`.
///   - Completing a future later needs `KafkaFutureImpl`
///     (`src/common/kafka_future.rs`), also `pub(crate)`; the only public
///     constructor is `KafkaFuture::completed`.
///
/// Both visibilities are faithful to Java (`CreateTopicsResult`'s constructor is
/// package-private and `KafkaFutureImpl` lives in
/// `org.apache.kafka.common.internals`, which CLAUDE.md maps to `pub(crate)`),
/// so widening them to make a test helper compile is not an option.
///
/// # DoD #7 justification (a type with no Java counterpart)
///
/// `AdminBackend` is test scaffolding, exactly as
/// [`ProducerBackendFactory`](crate::common::backend_factory::ProducerBackendFactory)
/// and
/// [`ConsumerBackendFactory`](crate::common::backend_factory::ConsumerBackendFactory)
/// already are — neither exists in Java either. It models what actually crosses
/// a language boundary: both bindings collapse per-key futures *before*
/// returning (Python's `_run_sync` hands back a resolved dict; the C `_async`
/// entry points fire their callback with a fully-built result struct), so
/// already-resolved plain data is the honest wire shape rather than a
/// simplification. Per-key granularity is still asserted — later slices carry it
/// as `HashMap<K, Result<V, KafkaError>>` rather than as futures.
///
/// Knowingly accepted consequence: the harness cannot assert that an Admin
/// method *returns before* its futures resolve. That property is untestable
/// through any binding (both are eager at the boundary) and stays covered by the
/// unit tests in `src/admin`, which use the real trait.
///
/// Methods are `async fn` in the trait (hence `#[allow(async_fn_in_trait)]`,
/// matching the existing backend factories) and return already-resolved plain
/// data; the gRPC implementation awaits one round-trip per call.
///
/// # Signature conventions for the RPC methods (slices G1..G6)
///
/// The 38 committed admin integration tests under `tests/integration/admin_*_test.rs`
/// are the scenario source, and they are converted to run on this trait rather
/// than rewritten. These four conventions are what make that conversion
/// mechanical; a method that departs from them forces its call sites to be
/// restructured.
///
///   1. **Per-key methods return `HashMap<K, Result<V, KafkaError>>`.** Today a
///      body reads one key out of a per-key accessor and awaits it —
///      `result.values()[&topic].get().await`,
///      `result.topic_name_values().unwrap()[&topic].get().await`,
///      `result.low_watermarks()[&tp].get().await`. Against an owned map of
///      per-key `Result`s that becomes `map[&topic].clone()` /
///      `map.get(&topic).unwrap()`, with the same `expect` / `expect_err` and
///      the same `err.error() == Errors::X` assertion after it. `K` needs
///      `Hash + Eq`; the key types in use are `String`, `TopicPartition` and
///      `ConfigResource`.
///   2. **The `.all()`-shaped call sites are served by the same map.** Most
///      bodies use only `.all().get().await.expect(...)`, i.e. "every key
///      succeeded". That is a fold over the returned map, not a second method,
///      so no `*_all` variants are needed.
///   3. **Options stay positional parameters**, exactly as on the production
///      trait — every existing call site passes a bare `XOptions::new()` and
///      keeps doing so. (No body uses a builder method on an options struct
///      yet, but the parameter must be there for the ones that will.)
///   4. **An RPC whose Java result exposes several independent futures returns
///      one struct with all of them resolved.** `describe_cluster` is the case:
///      `admin_cluster_configs_test.rs` holds the result and awaits
///      `.nodes()`, `.controller()` and `.cluster_id()` off it separately. A
///      method returning a single future cannot express that; a method
///      returning a struct of the three resolved values can, and eager
///      resolution is already the shape both bindings hand back.
#[allow(async_fn_in_trait)]
pub trait AdminBackend {
    /// Create a batch of topics.
    ///
    /// `Admin::create_topics` + awaiting `CreateTopicsResult`'s per-topic
    /// futures. The per-key value is Java's `TopicMetadataAndConfig`, which
    /// itself holds either the metadata or an exception its accessors rethrow —
    /// so a topic can succeed here while its metadata is unavailable.
    async fn create_topics(
        &self,
        new_topics: &[NewTopic],
        options: CreateTopicsOptions,
    ) -> Result<Outcomes<String, TopicMetadataAndConfig>, KafkaError>;

    /// Delete topics by name (`TopicCollection::of_topic_names`).
    async fn delete_topics(
        &self,
        names: &[String],
        options: DeleteTopicsOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError>;

    /// Delete topics by id (`TopicCollection::of_topic_ids`).
    async fn delete_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DeleteTopicsOptions,
    ) -> Result<Outcomes<Uuid, ()>, KafkaError>;

    /// List the topics in the cluster, keyed by topic name.
    ///
    /// Not an [`Outcomes`]: Java's `ListTopicsResult` holds one
    /// `KafkaFuture<Map<String, TopicListing>>`, so an individual listing can
    /// never fail. `names()` is this map's key set and `listings()` its values.
    async fn list_topics(&self, options: ListTopicsOptions) -> Result<HashMap<String, TopicListing>, KafkaError>;

    /// Describe topics by name (`TopicCollection::of_topic_names`).
    async fn describe_topics(
        &self,
        names: &[String],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<String, TopicDescription>, KafkaError>;

    /// Describe topics by id (`TopicCollection::of_topic_ids`).
    async fn describe_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<Uuid, TopicDescription>, KafkaError>;

    /// Increase the partition counts of the given topics.
    async fn create_partitions(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError>;

    /// Delete records before the given offset of each partition.
    async fn delete_records(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> Result<Outcomes<TopicPartition, DeletedRecords>, KafkaError>;

    /// Describe the cluster: its id, brokers, controller and (optionally) the
    /// operations the caller is authorized to perform on it.
    ///
    /// Convention #4: Java's `DescribeClusterResult` exposes four *independent*
    /// futures, so the resolved values arrive as one [`ClusterDescription`] and
    /// any failure is a whole-call failure — which is exactly what both bindings
    /// do (`ClusterDescription` in `admin.py`, the four direct attribute
    /// accessors on `kafka_admin_DescribeClusterResult_t`).
    async fn describe_cluster(&self, options: DescribeClusterOptions) -> Result<ClusterDescription, KafkaError>;

    /// Describe the configuration of each resource.
    ///
    /// The value is [`ConfigView`] rather than the production [`Config`]; see
    /// [`ConfigEntryView`] for why.
    async fn describe_configs(
        &self,
        resources: &[ConfigResource],
        options: DescribeConfigsOptions,
    ) -> Result<Outcomes<ConfigResource, ConfigView>, KafkaError>;

    /// Incrementally alter the configuration of each resource.
    async fn incremental_alter_configs(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ) -> Result<Outcomes<ConfigResource, ()>, KafkaError>;

    /// List the cluster's config resources whose type is in
    /// `config_resource_types`; an empty set requests every supported type.
    ///
    /// Not an [`Outcomes`]: Java's `ListConfigResourcesResult` holds a single
    /// `KafkaFuture<Collection<ConfigResource>>`.
    async fn list_config_resources(
        &self,
        config_resource_types: &HashSet<ConfigResourceType>,
        options: ListConfigResourcesOptions,
    ) -> Result<Vec<ConfigResource>, KafkaError>;

    /// List the cluster's client-metrics resources (KIP-714).
    ///
    /// Deprecated in Java since 4.1 in favour of
    /// `listConfigResources(Set.of(CLIENT_METRICS))`, and carried here because
    /// both bindings still expose it. Not an [`Outcomes`], for the same reason as
    /// [`AdminBackend::list_config_resources`].
    #[allow(deprecated)]
    async fn list_client_metrics_resources(
        &self,
        options: ListClientMetricsResourcesOptions,
    ) -> Result<Vec<ClientMetricsResourceListing>, KafkaError>;

    /// Query the log directories of each broker.
    ///
    /// The per-broker value is *nested* — Java's future resolves to
    /// `Map<String, LogDirDescription>` keyed by log-dir path — and each
    /// [`LogDirDescription`] carries its own `error()` for a directory that is
    /// offline or unreadable even though the broker answered. That is the
    /// value-carries-its-own-error case (`admin_service.proto`'s envelope
    /// exception 3), distinct from the per-broker `Err` in this map.
    async fn describe_log_dirs(
        &self,
        brokers: &[i32],
        options: DescribeLogDirsOptions,
    ) -> Result<Outcomes<i32, HashMap<String, LogDirDescription>>, KafkaError>;

    /// Move each replica to the given log directory.
    async fn alter_replica_log_dirs(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ()>, KafkaError>;

    /// Query which log directory hosts each replica, and which one it is moving
    /// to.
    ///
    /// The value is [`ReplicaLogDirInfoView`] rather than the production
    /// `ReplicaLogDirInfo`; see there for why.
    async fn describe_replica_log_dirs(
        &self,
        replicas: &[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ReplicaLogDirInfoView>, KafkaError>;

    /// Elect a leader for each of `partitions`, or for **every** partition in
    /// the cluster when `partitions` is `None` (Java's null `Set`).
    ///
    /// The two are not interchangeable and the difference is observable: Java's
    /// `ReplicationControlManager.electLeaders`
    /// (`ReplicationControlManager.java:1507`) takes a separate branch for a
    /// null set and there **omits** every partition whose outcome is
    /// `ELECTION_NOT_NEEDED`, whereas the explicit branch always returns one
    /// result per requested partition. `Some(empty set)` is a third thing again:
    /// an empty selection, i.e. a no-op.
    ///
    /// The per-partition value is void: Java's `partitions()` resolves to
    /// `Map<TopicPartition, Optional<Throwable>>`, so `Ok(())` here is Java's
    /// empty `Optional` — the election succeeded for that partition. The outer
    /// `Err` is wider than for the other void RPCs, because Java holds a
    /// *single* future for the whole map (see `admin_service.proto`'s
    /// `ElectLeadersRequest`).
    async fn elect_leaders(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
        options: ElectLeadersOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError>;

    /// Start or cancel a reassignment of each partition's replica set.
    ///
    /// A `None` value **cancels** (reverts) that partition's ongoing
    /// reassignment — Java's empty `Optional` (`Admin.java:1142-1143`) — which is
    /// not the same as a `NewPartitionReassignment` with no replicas, a state
    /// Java rejects outright and that `NewPartitionReassignment::new` therefore
    /// cannot even construct.
    async fn alter_partition_reassignments(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
        options: AlterPartitionReassignmentsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError>;

    /// List the ongoing partition reassignments, restricted to `partitions` or
    /// over the whole cluster when it is `None` (Java's `Optional.empty()`).
    ///
    /// Not an [`Outcomes`]: Java's `ListPartitionReassignmentsResult` holds one
    /// `KafkaFuture<Map<TopicPartition, PartitionReassignment>>`, so no
    /// individual reassignment can fail. Only partitions with an ongoing
    /// reassignment appear, so the map can be smaller than the request — and is
    /// empty on a quiet cluster.
    async fn list_partition_reassignments(
        &self,
        partitions: Option<HashSet<TopicPartition>>,
        options: ListPartitionReassignmentsOptions,
    ) -> Result<HashMap<TopicPartition, PartitionReassignment>, KafkaError>;

    /// Look up one offset per partition, each selected by an [`OffsetSpec`].
    ///
    /// The only G3 RPC that reaches the broker through the `AdminApiDriver` /
    /// `PartitionLeaderStrategy` multi-step engine (a partition-leader lookup
    /// before the real request) rather than the simple `Call`/retry path — see
    /// `.claude/rules/admin-client.md` §2.
    async fn list_offsets(
        &self,
        topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ListOffsetsResultInfo>, KafkaError>;

    /// List every group in the cluster.
    ///
    /// Not an [`Outcomes`] and not a plain `Vec`: Java's `ListGroupsResult`
    /// splits **one** future into `valid()` listings and an *unkeyed* `errors()`
    /// collection, and the two are independent — a partial success has both
    /// non-empty. See [`Listings`].
    async fn list_groups(&self, options: ListGroupsOptions) -> Result<Listings<GroupListing>, KafkaError>;

    /// List the consumer groups in the cluster.
    ///
    /// Deprecated in Java since 4.1 in favour of
    /// [`AdminBackend::list_groups`], which covers every group type, and carried
    /// here because both bindings still expose it. Same [`Listings`] shape.
    #[allow(deprecated)]
    async fn list_consumer_groups(
        &self,
        options: ListConsumerGroupsOptions,
    ) -> Result<Listings<ConsumerGroupListing>, KafkaError>;

    /// Describe the given groups, classic or KIP-848 consumer protocol.
    async fn describe_consumer_groups(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ConsumerGroupDescription>, KafkaError>;

    /// Describe the given groups, classic protocol only.
    async fn describe_classic_groups(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> Result<Outcomes<String, ClassicGroupDescription>, KafkaError>;

    /// List each group's committed offsets.
    ///
    /// Per-key by group id, and the value is *nested* — Java's per-group future
    /// resolves to a whole [`GroupOffsets`] map, whose values are themselves
    /// nullable: `None` means the group has no committed offset for that
    /// partition, which is not a committed offset of 0. Same two-level shape as
    /// [`AdminBackend::describe_log_dirs`], and not the
    /// value-carries-its-own-error case — no level of this value holds an error.
    async fn list_consumer_group_offsets(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<String, GroupOffsets>, KafkaError>;

    /// Commit offsets on behalf of `group_id`.
    ///
    /// The per-partition value is void. The outer `Err` is wider than for the
    /// per-key void RPCs, for the same reason as
    /// [`AdminBackend::elect_leaders`]: `AlterConsumerGroupOffsetsResult` holds a
    /// *single* future over the whole map, so its failure is a whole-call
    /// failure. With an empty `offsets` there is no per-partition slot at all, so
    /// that whole-call error is the only observable.
    async fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError>;

    /// Delete `group_id`'s committed offsets for `partitions`.
    ///
    /// Same single-future shape as
    /// [`AdminBackend::alter_consumer_group_offsets`].
    async fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError>;

    /// Delete the given groups. Per-group void, with one future per key, so the
    /// outer `Err` has its ordinary narrow meaning.
    async fn delete_consumer_groups(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError>;

    /// Remove members from `group_id`, keyed by `group.instance.id`.
    ///
    /// `options` carries the member selection, and its two legal states are not
    /// interchangeable: `RemoveMembersFromConsumerGroupOptions::default()` is
    /// Java's no-argument constructor ("remove every member"), while
    /// `new(members)` **rejects** an empty collection
    /// (`RemoveMembersFromConsumerGroupOptions.java:33-37`), so `removeAll()` is
    /// literally `members.isEmpty()` (`:57-59`) — Java's own emptiness rule, not
    /// a binding shortcut.
    ///
    /// In `removeAll` mode the returned map is **empty**: Java's `memberResult`
    /// refuses in that mode, so `all()` is the only observable and any failure is
    /// the outer `Err`. That is what `src/ffi/admin.rs`'s
    /// `submit_remove_members_from_consumer_group` does, so all four backends
    /// agree on it.
    async fn remove_members_from_consumer_group(
        &self,
        group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError>;

    /// Close the admin client, joining its background task.
    ///
    /// `timeout` of `None` is Java's no-argument `close()`. Java's
    /// `Admin.close(Duration)` is `void` and so is the Rust `Admin::close`; the
    /// `Result` here exists because the gRPC backends can fail at the transport
    /// or binding level, which is a harness failure the scenario must see rather
    /// than a Kafka-level error.
    async fn close(&self, timeout: Option<Duration>) -> Result<(), KafkaError>;

    /// Short backend label used in assertion messages.
    fn name(&self) -> &'static str;
}

/// The already-resolved per-key outcomes of one Admin RPC: convention #1 of
/// [`AdminBackend`]'s signature rules, named so the method signatures stay
/// readable. The outer `Result` on every method is the whole-call failure that
/// precedes any per-key future (a synchronous throw, an unknown handle, a
/// transport error); this map is what a Java caller would read out of the
/// `*Result`'s per-key `KafkaFuture`s.
pub type Outcomes<K, V> = HashMap<K, Result<V, KafkaError>>;

/// The already-resolved outcome of an RPC whose Java `*Result` splits **one**
/// future into `valid()` and an *unkeyed* `errors()` collection: `listGroups` and
/// `listConsumerGroups`.
///
/// # DoD #7 justification (a type with no Java counterpart)
///
/// Java exposes the two collections as three views (`all()` / `valid()` /
/// `errors()`) over a single `KafkaFuture<Collection<Object>>`, so there is no
/// class to translate — but there is nothing to key either, which rules out
/// [`Outcomes`]. Both bindings already collapse the pair exactly this way:
/// `admin.py`'s `list_groups` returns `([GroupListing], [KafkaError])` and the C
/// handle exposes `_valid_count` / `_get_valid` next to `_error_count` /
/// `_get_error` as two independent lists. So this is the honest resolved shape,
/// and it is convention #4 of [`AdminBackend`] applied to a split rather than to
/// several independent futures.
///
/// **`errors` must not be indexed by `valid` position.** There is no
/// correspondence between the two lists; a partial success (one broker answered,
/// another failed) has both non-empty and of unrelated lengths. Java's `all()` is
/// the fold that fails if `errors` is non-empty, which [`Listings::all`]
/// provides.
// No `PartialEq`: `KafkaError` is not comparable (it carries a message and a
// source), so `errors` cannot be. Scenarios compare `valid` and read
// `errors`' codes.
#[derive(Clone, Debug)]
pub struct Listings<T> {
    /// Java `valid()`.
    pub valid: Vec<T>,
    /// Java `errors()`, unkeyed.
    pub errors: Vec<KafkaError>,
}

impl<T> Listings<T> {
    /// Java's `all()`: the listings if every broker answered, otherwise the
    /// first error.
    ///
    /// Java's `all()` completes exceptionally with one of the failures without
    /// specifying which; this picks the first, and because the fold runs
    /// identically for all four backends over the same pair it cannot make
    /// backends disagree. Same reasoning as [`all_of`].
    pub fn all(&self) -> Result<&[T], KafkaError> {
        match self.errors.first() {
            Some(error) => Err(error.clone()),
            None => Ok(&self.valid),
        }
    }
}

/// Folds per-key outcomes the way Java's `*Result.all()` does: `Err` if any key
/// failed, otherwise `Ok`.
///
/// Most converted scenario bodies only ever used `.all().get().await`, so this
/// keeps them a one-liner instead of a per-key loop.
///
/// **Error-selection rule:** when more than one key failed, *which* error is
/// reported is unspecified. That is not laziness — it is the Java contract.
/// `KafkaFuture.allOf` delegates to `CompletableFuture.allOf`, whose javadoc
/// says only that the result "completes exceptionally with a
/// CompletionException holding this exception as its cause" for one of the
/// failures, never which; and the Rust `KafkaFuture::all_of` polls a `Vec`
/// whose order comes from `HashMap` iteration. Because this fold runs
/// identically for all four backends over the same map, an unspecified choice
/// cannot make backends disagree — it only affects which message a failing
/// assertion prints. A scenario that must assert a *particular* key's error
/// reads that key out of the map instead, which every converted body that cares
/// does.
pub fn all_of<K, V>(outcomes: &Outcomes<K, V>) -> Result<(), KafkaError> {
    for outcome in outcomes.values() {
        if let Err(e) = outcome {
            return Err(e.clone());
        }
    }
    Ok(())
}

/// [`all_of`], plus the completeness check `all_of` cannot make: that the
/// response carried an outcome for **exactly** the requested keys.
///
/// # Why this exists
///
/// `all_of` folds over whatever keys the response happened to contain, and an
/// empty map folds to `Ok`. `MultilanguageAdmin` builds its map purely from the
/// response's `entries`, so a backend that answered with *no* entries — or with
/// one entry where two were requested — passes `all_of` silently. The G1 Critic
/// found exactly that class of defect once already (a `listTopics` null guard
/// dropping an entry), and converting a `values()[&key]` lookup (which panics on
/// a missing key) into an `all_of` fold retires the only completeness check the
/// scenario had.
///
/// So a converted body that asserts "the batch succeeded" should use this
/// instead, naming the keys it asked for. Panics rather than returning an error,
/// because a short response is a harness/backend disagreement rather than a
/// Kafka-level outcome, and the panic message names the backend.
pub fn all_of_exactly<K, V, B>(admin: &B, outcomes: &Outcomes<K, V>, expected: &[K], what: &str)
where
    K: std::hash::Hash + Eq + std::fmt::Debug,
    B: AdminBackend,
{
    let backend = admin.name();
    // Compared as sets, not as sorted vectors: entry order is unspecified by the
    // wire contract (`admin_service.proto`) and several key types are not `Ord`.
    let got: HashSet<&K> = outcomes.keys().collect();
    let want: HashSet<&K> = expected.iter().collect();
    assert_eq!(
        got, want,
        "{backend} backend: {what} must report an outcome for exactly the requested keys; a short response would \
         otherwise fold to a silent success"
    );
    all_of(outcomes).unwrap_or_else(|e| panic!("{backend} backend: {what}: {e}"));
}

// ---------------------------------------------------------------------------
// Harness value types (slice G2)
//
// Every G1 input, option and value type crossed as the *production* public
// type, because all of them have public constructors as well as public getters.
// Three G2 values do not, and each needs a local stand-in. DoD #7: these are
// not new domain concepts, they are the same data behind a constructor a test
// crate cannot call — and the visibility that stops it is faithful to Java in
// every case, so widening it is not an option.
//
// Slice G3 added no view type: `ListOffsetsResultInfo::new`,
// `PartitionReassignment::new` and `NewPartitionReassignment::new` are all
// public (checked, per G2's rule to grep `fn new`'s visibility for every value
// type rather than assume either way), so its four RPCs cross entirely as
// production types.
//
// Slice G4 added none either, for the same reason: `GroupListing::new`,
// `ConsumerGroupListing::new`, `ConsumerGroupDescription::new`,
// `ClassicGroupDescription::new`, `MemberDescription::new`,
// `MemberAssignment::new`, `MemberToRemove::new` and
// `OffsetAndMetadata::{new, with_metadata, with_leader_epoch}` are all public, so
// the nine group RPCs cross entirely as production types. It did add
// [`Listings`], but that is not a stand-in for an unreachable constructor — it is
// the resolved form of a Java result shape that has no class at all.
// ---------------------------------------------------------------------------

/// The four resolved attributes of Java's `DescribeClusterResult`.
///
/// Java has no such class: the result exposes four independent `KafkaFuture`s.
/// Neither binding has a `KafkaFuture` either, so both already collapse them —
/// `admin.py` returns one `ClusterDescription` and raises if any of the four
/// failed, and the C handle exposes the four attributes with a single whole-call
/// error. This is convention #4 of [`AdminBackend`], and the shape the committed
/// `admin_cluster_configs_test` already read off one result object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterDescription {
    /// Java `DescribeClusterResult.clusterId()`.
    pub cluster_id: String,
    /// Java `nodes()`.
    pub nodes: Vec<Node>,
    /// Java `controller()`, nullable: `None` when the cluster reported no
    /// current controller.
    pub controller: Option<Node>,
    /// Java `authorizedOperations()`, nullable: `None` is "the broker did not
    /// report them", which is not "reported that none are authorized".
    pub authorized_operations: Option<BTreeSet<AclOperation>>,
}

/// One configuration synonym, standing in for the production `ConfigSynonym`.
///
/// `ConfigSynonym::new` is `pub(crate)` — faithfully, because Java's
/// `ConfigEntry.ConfigSynonym(String, String, ConfigSource)` constructor is
/// package-private (`ConfigEntry.java:243`) — so a test crate cannot build one,
/// and a gRPC backend cannot rebuild what the wire carried.
///
/// `source` is the `ConfigSource` *enum constant name*, which is what both
/// bindings expose (`kafka_admin_ConfigEntry_synonym_source` returns a string;
/// `admin.py`'s `ConfigSynonym.source` is that string). Java's `ConfigSource`
/// has no numeric id, so the name is the contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigSynonymView {
    /// Java `ConfigSynonym.name()`, which may differ from the entry's name.
    pub name: String,
    /// Java `value()`, null for a sensitive config.
    pub value: Option<String>,
    /// Java `source()` as its enum constant name.
    pub source: String,
}

/// One configuration entry as `describeConfigs` reports it, standing in for the
/// production [`ConfigEntry`].
///
/// The blocker is [`ConfigSynonymView`]: `ConfigEntry::with_metadata` is public
/// but takes `Vec<ConfigSynonym>`, whose constructor is not. Dropping synonyms
/// to keep the production type was rejected — `describeConfigs` reports all nine
/// `ConfigEntry` fields through *every* binding (`kafka_admin_ConfigEntry_*`,
/// `admin.py`'s `_to_full_config_entry`), so unlike `createTopics` (where five
/// fields are the real C-boundary limit, see [`comparable_config`]) there is
/// nothing here that only one backend can produce. Silently dropping four of
/// them would retire real differential coverage.
///
/// `is_default` is carried alongside `source` even though Java derives it
/// (`source == DEFAULT_CONFIG`), because both bindings expose both — a backend
/// on which the two disagree is a finding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigEntryView {
    /// Java `ConfigEntry.name()`.
    pub name: String,
    /// Java `value()`: `None` when unset *or* suppressed because the config is
    /// sensitive.
    pub value: Option<String>,
    /// Java `isDefault()`.
    pub is_default: bool,
    /// Java `isSensitive()`.
    pub is_sensitive: bool,
    /// Java `isReadOnly()`.
    pub is_read_only: bool,
    /// Java `source()` as its enum constant name, e.g. `"STATIC_BROKER_CONFIG"`.
    /// `None` only if a backend failed to report it at all — `describeConfigs`
    /// always carries a source on all four.
    pub source: Option<String>,
    /// Java `type()` as its enum constant name, e.g. `"LONG"`.
    pub config_type: Option<String>,
    /// Java `documentation()`, nullable.
    pub documentation: Option<String>,
    /// Java `synonyms()`, in precedence order. Empty unless the request set
    /// `include_synonyms`.
    pub synonyms: Vec<ConfigSynonymView>,
}

/// The configuration of one resource, standing in for the production [`Config`]
/// because its entries are [`ConfigEntryView`]s.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigView {
    /// Java `Config.entries()`.
    pub entries: Vec<ConfigEntryView>,
}

impl ConfigView {
    /// The entry named `name`, or `None` — Java's `Config.get(String)`, and the
    /// accessor every converted scenario body uses.
    pub fn get(&self, name: &str) -> Option<&ConfigEntryView> {
        self.entries.iter().find(|entry| entry.name == name)
    }
}

/// Where one replica lives and where it is moving to, standing in for the
/// production `ReplicaLogDirInfo`.
///
/// `ReplicaLogDirInfo::new` is `pub(crate)` — faithfully, because Java's
/// `DescribeReplicaLogDirsResult.ReplicaLogDirInfo` constructors are
/// package-private (`DescribeReplicaLogDirsResult.java:71,75`) — so a test crate
/// cannot build one. Field-for-field identical to the production type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaLogDirInfoView {
    /// Java `currentReplicaLogDir()`: `None` when the broker hosts no replica of
    /// that partition.
    pub current_replica_log_dir: Option<String>,
    /// Java `currentReplicaOffsetLag()`, -1 when there is none to report.
    pub current_replica_offset_lag: i64,
    /// Java `futureReplicaLogDir()`: `None` when no move is pending.
    pub future_replica_log_dir: Option<String>,
    /// Java `futureReplicaOffsetLag()`, -1 when no move is pending.
    pub future_replica_offset_lag: i64,
}

/// Java's implicit `ConfigSource.name()`.
///
/// Mirrors `config_source_name` in `src/ffi/admin.rs`, which is the C boundary's
/// spelling and therefore what the wire carries; `ConfigSource` has no numeric
/// id in Java, so the constant name is the contract. Private there, so the
/// mapping is restated rather than shared — and `src/ffi/admin.rs`'s
/// `config_source_name_matches_java_enum_constant_names` unit test pins the same
/// table against Java.
fn config_source_name(source: ConfigSource) -> &'static str {
    match source {
        ConfigSource::DynamicTopicConfig => "DYNAMIC_TOPIC_CONFIG",
        ConfigSource::DynamicBrokerLoggerConfig => "DYNAMIC_BROKER_LOGGER_CONFIG",
        ConfigSource::DynamicBrokerConfig => "DYNAMIC_BROKER_CONFIG",
        ConfigSource::DynamicDefaultBrokerConfig => "DYNAMIC_DEFAULT_BROKER_CONFIG",
        ConfigSource::DynamicClientMetricsConfig => "DYNAMIC_CLIENT_METRICS_CONFIG",
        ConfigSource::DynamicGroupConfig => "DYNAMIC_GROUP_CONFIG",
        ConfigSource::StaticBrokerConfig => "STATIC_BROKER_CONFIG",
        ConfigSource::DefaultConfig => "DEFAULT_CONFIG",
        ConfigSource::Unknown => "UNKNOWN",
    }
}

/// Java's implicit `ConfigType.name()`. See [`config_source_name`].
fn config_type_name(config_type: ConfigType) -> &'static str {
    match config_type {
        ConfigType::Unknown => "UNKNOWN",
        ConfigType::Boolean => "BOOLEAN",
        ConfigType::String => "STRING",
        ConfigType::Int => "INT",
        ConfigType::Short => "SHORT",
        ConfigType::Long => "LONG",
        ConfigType::Double => "DOUBLE",
        ConfigType::List => "LIST",
        ConfigType::Class => "CLASS",
        ConfigType::Password => "PASSWORD",
    }
}

/// Projects a production [`Config`] onto the [`ConfigView`] the four backends
/// are compared on. Every field is carried; only the two enums are rendered as
/// the names the C boundary uses.
fn config_view(config: &Config) -> ConfigView {
    ConfigView {
        entries: config
            .entries()
            .map(|entry| ConfigEntryView {
                name: entry.name().to_string(),
                value: entry.value().map(str::to_string),
                is_default: entry.is_default(),
                is_sensitive: entry.is_sensitive(),
                is_read_only: entry.is_read_only(),
                source: Some(config_source_name(entry.source()).to_string()),
                config_type: Some(config_type_name(entry.config_type()).to_string()),
                documentation: entry.documentation().map(str::to_string),
                synonyms: entry
                    .synonyms()
                    .iter()
                    .map(|synonym| ConfigSynonymView {
                        name: synonym.name().to_string(),
                        value: synonym.value().map(str::to_string),
                        source: config_source_name(synonym.source()).to_string(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// RustNativeAdmin — drives src/admin in-process.
// ---------------------------------------------------------------------------

/// Backend that drives the native Rust [`Admin`] implementation directly. This
/// is the baseline the python / python_async / c backends are compared against.
///
/// Every method calls the real (sync) `Admin` method and then awaits the
/// `KafkaFuture`s it returned, which is exactly what the bindings do internally
/// before handing a result back to their caller.
pub struct RustNativeAdmin {
    admin: Box<dyn Admin>,
}

impl RustNativeAdmin {
    /// Build a network-backed admin client from `config`.
    pub fn from_config(config: &HashMap<String, String>) -> Result<Self, KafkaError> {
        let config = AdminClientConfig::from_properties(config)?;
        Ok(Self { admin: new_admin_client(config)? })
    }

    /// Build a broker-less [`MockAdminClient`] with `num_brokers` brokers.
    pub fn mock(num_brokers: i32) -> Self {
        Self { admin: Box::new(MockAdminClient::create(num_brokers)) }
    }
}

impl AdminBackend for RustNativeAdmin {
    async fn create_topics(
        &self,
        new_topics: &[NewTopic],
        options: CreateTopicsOptions,
    ) -> Result<Outcomes<String, TopicMetadataAndConfig>, KafkaError> {
        let result = self.admin.create_topics(new_topics, options);
        let mut outcomes = HashMap::new();
        for (name, created) in result.values() {
            // `values()` is Java's `KafkaFuture<Void>` view: it fails only if the
            // creation itself failed. The metadata is a second, independent
            // level — see `metadata_of`.
            let outcome = match created.get().await {
                Err(e) => Err(e),
                Ok(()) => Ok(metadata_of(&result, &name).await),
            };
            outcomes.insert(name, outcome);
        }
        Ok(outcomes)
    }

    async fn delete_topics(
        &self,
        names: &[String],
        options: DeleteTopicsOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError> {
        let result = self
            .admin
            .delete_topics(TopicCollection::of_topic_names(names.to_vec()), options);
        let values = result
            .topic_name_values()
            .ok_or_else(|| KafkaError::illegal_state("deleteTopics(ofTopicNames) did not return name-keyed futures"))?;
        Ok(resolve(values.iter().map(|(name, f)| (name.clone(), f.clone()))).await)
    }

    async fn delete_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DeleteTopicsOptions,
    ) -> Result<Outcomes<Uuid, ()>, KafkaError> {
        let result = self
            .admin
            .delete_topics(TopicCollection::of_topic_ids(topic_ids.to_vec()), options);
        let values = result
            .topic_id_values()
            .ok_or_else(|| KafkaError::illegal_state("deleteTopics(ofTopicIds) did not return id-keyed futures"))?;
        Ok(resolve(values.iter().map(|(id, f)| (*id, f.clone()))).await)
    }

    async fn list_topics(&self, options: ListTopicsOptions) -> Result<HashMap<String, TopicListing>, KafkaError> {
        self.admin.list_topics(options).names_to_listings().get().await
    }

    async fn describe_topics(
        &self,
        names: &[String],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<String, TopicDescription>, KafkaError> {
        let result = self
            .admin
            .describe_topics(TopicCollection::of_topic_names(names.to_vec()), options);
        let values = result.topic_name_values().ok_or_else(|| {
            KafkaError::illegal_state("describeTopics(ofTopicNames) did not return name-keyed futures")
        })?;
        Ok(resolve(values.iter().map(|(name, f)| (name.clone(), f.clone()))).await)
    }

    async fn describe_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<Uuid, TopicDescription>, KafkaError> {
        let result = self
            .admin
            .describe_topics(TopicCollection::of_topic_ids(topic_ids.to_vec()), options);
        let values = result
            .topic_id_values()
            .ok_or_else(|| KafkaError::illegal_state("describeTopics(ofTopicIds) did not return id-keyed futures"))?;
        Ok(resolve(values.iter().map(|(id, f)| (*id, f.clone()))).await)
    }

    async fn create_partitions(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError> {
        let result = self.admin.create_partitions(new_partitions, options);
        Ok(resolve(result.values().iter().map(|(name, f)| (name.clone(), f.clone()))).await)
    }

    async fn delete_records(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> Result<Outcomes<TopicPartition, DeletedRecords>, KafkaError> {
        let result = self.admin.delete_records(records_to_delete, options);
        Ok(resolve(result.low_watermarks().iter().map(|(tp, f)| (tp.clone(), f.clone()))).await)
    }

    async fn describe_cluster(&self, options: DescribeClusterOptions) -> Result<ClusterDescription, KafkaError> {
        let result = self.admin.describe_cluster(options);
        // All four are awaited before any error is reported, so none is
        // abandoned; when more than one failed, the first in Java's declaration
        // order wins. Identical to the FFI's `submit_describe_cluster`, so the
        // four backends pick the same error out of a multi-failure.
        let nodes = result.nodes().get().await;
        let controller = result.controller().get().await;
        let cluster_id = result.cluster_id().get().await;
        let authorized_operations = result.authorized_operations().get().await;
        Ok(ClusterDescription {
            nodes: nodes?,
            controller: controller?,
            cluster_id: cluster_id?,
            authorized_operations: authorized_operations?,
        })
    }

    async fn describe_configs(
        &self,
        resources: &[ConfigResource],
        options: DescribeConfigsOptions,
    ) -> Result<Outcomes<ConfigResource, ConfigView>, KafkaError> {
        let result = self.admin.describe_configs(resources, options);
        let mut outcomes = HashMap::with_capacity(result.values().len());
        for (resource, future) in result.values() {
            outcomes.insert(resource.clone(), future.get().await.map(|config| config_view(&config)));
        }
        Ok(outcomes)
    }

    async fn incremental_alter_configs(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ) -> Result<Outcomes<ConfigResource, ()>, KafkaError> {
        let result = self.admin.incremental_alter_configs(configs, options);
        Ok(resolve(result.values().iter().map(|(r, f)| (r.clone(), f.clone()))).await)
    }

    async fn list_config_resources(
        &self,
        config_resource_types: &HashSet<ConfigResourceType>,
        options: ListConfigResourcesOptions,
    ) -> Result<Vec<ConfigResource>, KafkaError> {
        self.admin
            .list_config_resources(config_resource_types, options)
            .all()
            .get()
            .await
    }

    #[allow(deprecated)]
    async fn list_client_metrics_resources(
        &self,
        options: ListClientMetricsResourcesOptions,
    ) -> Result<Vec<ClientMetricsResourceListing>, KafkaError> {
        self.admin.list_client_metrics_resources(options).all().get().await
    }

    async fn describe_log_dirs(
        &self,
        brokers: &[i32],
        options: DescribeLogDirsOptions,
    ) -> Result<Outcomes<i32, HashMap<String, LogDirDescription>>, KafkaError> {
        let result = self.admin.describe_log_dirs(brokers, options);
        Ok(resolve(result.descriptions().iter().map(|(broker, f)| (*broker, f.clone()))).await)
    }

    async fn alter_replica_log_dirs(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ()>, KafkaError> {
        let result = self.admin.alter_replica_log_dirs(replica_assignment, options);
        Ok(resolve(result.values().iter().map(|(r, f)| (r.clone(), f.clone()))).await)
    }

    async fn describe_replica_log_dirs(
        &self,
        replicas: &[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ReplicaLogDirInfoView>, KafkaError> {
        let result = self.admin.describe_replica_log_dirs(replicas, options);
        let mut outcomes = HashMap::with_capacity(result.values().len());
        for (replica, future) in result.values() {
            let outcome = future.get().await.map(|info| ReplicaLogDirInfoView {
                current_replica_log_dir: info.current_replica_log_dir().map(str::to_string),
                current_replica_offset_lag: info.current_replica_offset_lag(),
                future_replica_log_dir: info.future_replica_log_dir().map(str::to_string),
                future_replica_offset_lag: info.future_replica_offset_lag(),
            });
            outcomes.insert(replica.clone(), outcome);
        }
        Ok(outcomes)
    }

    async fn elect_leaders(
        &self,
        election_type: ElectionType,
        partitions: Option<HashSet<TopicPartition>>,
        options: ElectLeadersOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError> {
        // One future for the whole map, so its failure is the outer `Err`; the
        // per-partition `Optional<Throwable>` inside becomes the inner `Result`.
        // Same shape as the FFI's `submit_elect_leaders`, which likewise returns
        // `partitions()` unchanged.
        let outcomes = self
            .admin
            .elect_leaders(election_type, partitions, options)
            .partitions()
            .get()
            .await?;
        Ok(outcomes
            .into_iter()
            .map(|(tp, error)| (tp, error.map_or(Ok(()), Err)))
            .collect())
    }

    async fn alter_partition_reassignments(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
        options: AlterPartitionReassignmentsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError> {
        let result = self.admin.alter_partition_reassignments(reassignments, options);
        Ok(resolve(result.values().iter().map(|(tp, f)| (tp.clone(), f.clone()))).await)
    }

    async fn list_partition_reassignments(
        &self,
        partitions: Option<HashSet<TopicPartition>>,
        options: ListPartitionReassignmentsOptions,
    ) -> Result<HashMap<TopicPartition, PartitionReassignment>, KafkaError> {
        self.admin
            .list_partition_reassignments(partitions, options)
            .reassignments()
            .get()
            .await
    }

    async fn list_offsets(
        &self,
        topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ListOffsetsResultInfo>, KafkaError> {
        let result = self.admin.list_offsets(topic_partition_offsets, options);
        // `ListOffsetsResult` exposes its futures through `partitionResult(tp)`
        // rather than as a map, so the *requested* keys drive the collection —
        // and a key the call did not attempt is a whole-call `Err`, not a
        // missing entry. Identical to the FFI's `submit_list_offsets`, so all
        // four backends answer with the same key set.
        let mut outcomes = HashMap::with_capacity(topic_partition_offsets.len());
        for tp in topic_partition_offsets.keys() {
            outcomes.insert(tp.clone(), result.partition_result(tp)?.get().await);
        }
        Ok(outcomes)
    }

    async fn list_groups(&self, options: ListGroupsOptions) -> Result<Listings<GroupListing>, KafkaError> {
        let result = self.admin.list_groups(options);
        // Both views are awaited before either error is reported, so neither is
        // abandoned. Identical to the FFI's `submit_list_groups`.
        let valid = result.valid().get().await;
        let errors = result.errors().get().await;
        Ok(Listings { valid: valid?, errors: errors? })
    }

    #[allow(deprecated)]
    async fn list_consumer_groups(
        &self,
        options: ListConsumerGroupsOptions,
    ) -> Result<Listings<ConsumerGroupListing>, KafkaError> {
        let result = self.admin.list_consumer_groups(options);
        let valid = result.valid().get().await;
        let errors = result.errors().get().await;
        Ok(Listings { valid: valid?, errors: errors? })
    }

    async fn describe_consumer_groups(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ConsumerGroupDescription>, KafkaError> {
        let result = self.admin.describe_consumer_groups(group_ids, options);
        Ok(resolve(result.described_groups().into_iter()).await)
    }

    async fn describe_classic_groups(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> Result<Outcomes<String, ClassicGroupDescription>, KafkaError> {
        let result = self.admin.describe_classic_groups(group_ids, options);
        Ok(resolve(result.described_groups().into_iter()).await)
    }

    async fn list_consumer_group_offsets(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<String, GroupOffsets>, KafkaError> {
        let result = self.admin.list_consumer_group_offsets(group_specs, options);
        // The *requested* group ids drive the collection, and a group the call
        // did not attempt is a whole-call `Err` rather than a missing entry —
        // Java's `partitionsToOffsetAndMetadata(groupId)` throws
        // `IllegalArgumentException` there. Identical to the FFI's
        // `submit_list_consumer_group_offsets`, so all four backends answer with
        // the same key set.
        let mut outcomes = HashMap::with_capacity(group_specs.len());
        for group_id in group_specs.keys() {
            let future = result.partitions_to_offset_and_metadata_for_group(group_id)?;
            outcomes.insert(group_id.clone(), future.get().await);
        }
        Ok(outcomes)
    }

    async fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError> {
        let result = self.admin.alter_consumer_group_offsets(group_id, offsets, options);
        if offsets.is_empty() {
            // No per-partition slot exists, so the single future's failure is
            // the only observable. Same branch as the FFI's
            // `submit_alter_consumer_group_offsets`.
            result.all().get().await?;
            return Ok(HashMap::new());
        }
        let mut outcomes = HashMap::with_capacity(offsets.len());
        for tp in offsets.keys() {
            outcomes.insert(tp.clone(), result.partition_result(tp).get().await);
        }
        Ok(outcomes)
    }

    async fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, KafkaError> {
        let result = self.admin.delete_consumer_group_offsets(group_id, partitions, options);
        if partitions.is_empty() {
            result.all().get().await?;
            return Ok(HashMap::new());
        }
        let mut outcomes = HashMap::with_capacity(partitions.len());
        for tp in partitions {
            outcomes.insert(tp.clone(), result.partition_result(tp)?.get().await);
        }
        Ok(outcomes)
    }

    async fn delete_consumer_groups(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError> {
        let result = self.admin.delete_consumer_groups(group_ids, options);
        Ok(resolve(result.deleted_groups().into_iter()).await)
    }

    async fn remove_members_from_consumer_group(
        &self,
        group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> Result<Outcomes<String, ()>, KafkaError> {
        // The member set has to be read off the options before they are moved
        // into the call, exactly as the FFI's
        // `submit_remove_members_from_consumer_group` does.
        let members: Vec<_> = options.members().iter().cloned().collect();
        let result = self.admin.remove_members_from_consumer_group(group_id, options);
        if members.is_empty() {
            // `removeAll` mode: Java's `memberResult` is not applicable, so
            // `all()` is the only observable and the map stays empty.
            result.all().get().await?;
            return Ok(HashMap::new());
        }
        let mut outcomes = HashMap::with_capacity(members.len());
        for member in &members {
            let future = result.member_result(member)?;
            outcomes.insert(member.group_instance_id().to_string(), future.get().await);
        }
        Ok(outcomes)
    }

    async fn close(&self, timeout: Option<Duration>) -> Result<(), KafkaError> {
        self.admin.close(close_timeout(timeout)).await;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "rust"
    }
}

/// Awaits every per-key `KafkaFuture` and collects the outcomes.
///
/// This is what both bindings do internally before handing a result back to
/// their caller (`admin.py`'s `_run_sync`, the C `_async` entry points' result
/// struct), so doing it here is what makes the four backends comparable.
async fn resolve<K, V>(futures: impl Iterator<Item = (K, KafkaFuture<V>)>) -> Outcomes<K, V>
where
    K: std::hash::Hash + Eq,
    V: Clone + Send + Sync + 'static,
{
    let mut outcomes = HashMap::new();
    for (key, future) in futures {
        outcomes.insert(key, future.get().await);
    }
    outcomes
}

/// Reassembles `CreateTopicsResult`'s `TopicMetadataAndConfig` for `topic` from
/// the public per-field accessors.
///
/// Java hands the object itself to the caller; Rust keeps
/// `CreateTopicsResult::futures()` `pub(crate)` (correctly — Java's field is
/// private too), so the only public route is the four `topicId` /
/// `numPartitions` / `replicationFactor` / `config` views. Every one of them is
/// `then_apply_try` over the *same* source object, so they all fail with the same
/// error when the broker reported no usable metadata, which is exactly the
/// `TopicMetadataAndConfig(KafkaException)` state.
async fn metadata_of(result: &CreateTopicsResult, topic: &str) -> TopicMetadataAndConfig {
    let (topic_id, num_partitions, replication_factor, config) = (
        result.topic_id(topic).get().await,
        result.num_partitions(topic).get().await,
        result.replication_factor(topic).get().await,
        result.config(topic).get().await,
    );
    match (topic_id, num_partitions, replication_factor, config) {
        (Ok(id), Ok(partitions), Ok(replication), Ok(config)) => {
            TopicMetadataAndConfig::new(id, partitions, replication, comparable_config(&config))
        },
        // Any accessor failing means the object carries an exception. Report the
        // first one so the harness sees the same error a Java caller would.
        (id, partitions, replication, config) => TopicMetadataAndConfig::with_error(
            id.err()
                .or_else(|| partitions.err())
                .or_else(|| replication.err())
                .or_else(|| config.err())
                .unwrap_or_else(|| KafkaError::illegal_state("createTopics metadata accessors disagreed on success")),
        ),
    }
}

/// Projects a [`Config`] onto the five fields that survive *every* binding's
/// `createTopics` result, so all four backends are compared on identical
/// information.
///
/// `kafka_admin_TopicMetadataAndConfig_config_*` exposes name / value /
/// is_default / is_sensitive / is_read_only and nothing else, and `admin.py`'s
/// `_to_config_entry` mirrors that. The native client *does* know
/// `ConfigEntry::source()` here, and leaving it in place would let a scenario
/// assert on a field only one of the four backends can ever produce — a green
/// `__rust` arm and three red ones, for no defect. Dropping it here makes that
/// trap unreachable. `is_default` is preserved by re-deriving the only source
/// value it depends on (`ConfigSource::DefaultConfig`).
fn comparable_config(config: &Config) -> Config {
    Config::new(
        config
            .entries()
            .map(|entry| {
                ConfigEntry::with_metadata(
                    entry.name().to_string(),
                    entry.value().map(str::to_string),
                    if entry.is_default() {
                        ConfigSource::DefaultConfig
                    } else {
                        ConfigSource::Unknown
                    },
                    entry.is_sensitive(),
                    entry.is_read_only(),
                    Vec::new(),
                    ConfigType::Unknown,
                    None,
                )
            })
            .collect::<Vec<_>>(),
    )
}

// ---------------------------------------------------------------------------
// Scenario helpers
//
// The `AdminBackend` twins of the `&dyn Admin` helpers in
// `crate::common::test_utils`. Two copies exist only while the conversion is in
// flight: the ten admin integration tests still on the production trait
// (slices G2..G6) use the `test_utils` versions, and those disappear with the
// last of them. Same Java sources, same bounds, same failure messages.
// ---------------------------------------------------------------------------

/// Admin config for the backend under test. `bootstrap` must be reachable from
/// the backend (container listener for python/c, host loopback for rust).
///
/// The timeouts match `admin_topics_test`'s original `admin_for`, which every
/// converted scenario inherits.
pub fn admin_config(bootstrap: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "multilang-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ])
}

/// Pick the bootstrap address this factory's backend can actually reach: the
/// gRPC backends run in containers and need the broker's container listener,
/// native rust uses the host loopback.
pub fn bootstrap_for<F: AdminBackendFactory>(factory: &F, ctx: &TestContext) -> String {
    if factory.needs_container_bootstrap() {
        ctx.container_bootstrap_servers().to_string()
    } else {
        ctx.bootstrap_servers().to_string()
    }
}

/// Build the admin client for the backend under test, panicking with the
/// backend's name on failure.
pub async fn admin_for<F: AdminBackendFactory>(factory: &F, ctx: &TestContext) -> F::Admin {
    factory
        .create(admin_config(&bootstrap_for(factory, ctx)))
        .await
        .unwrap_or_else(|e| panic!("{} backend: create admin client: {e}", factory.name()))
}

/// Returns the partition count for `topic`, or `None` if the broker being
/// queried does not (yet) know it.
///
/// [`crate::common::test_utils::try_partition_count`] for an [`AdminBackend`].
pub async fn try_partition_count<B: AdminBackend>(admin: &B, topic: &str) -> Option<usize> {
    admin
        .describe_topics(&[topic.to_string()], DescribeTopicsOptions::new())
        .await
        .ok()
        .and_then(|described| match described.get(topic) {
            Some(Ok(description)) => Some(description.partitions().len()),
            _ => None,
        })
}

/// Waits until `topic` is reported as having exactly `expected_num_partitions`
/// partitions.
///
/// [`crate::common::test_utils::wait_for_all_partitions_metadata`] for an
/// [`AdminBackend`], including Java's 60s bound and failure message.
pub async fn wait_for_all_partitions_metadata<B: AdminBackend>(admin: &B, topic: &str, expected_num_partitions: usize) {
    wait_until_true_with_timeout(
        || async { try_partition_count(admin, topic).await == Some(expected_num_partitions) },
        &format!("Topic [{topic}] metadata not propagated after 60000 ms"),
        TOPIC_METADATA_PROPAGATION_WAIT_MS,
        DEFAULT_PAUSE_MS,
    )
    .await;
}

/// Creates `topic` and does not return until its metadata has propagated.
///
/// [`crate::common::test_utils::create_topic`] for an [`AdminBackend`]; see
/// there for why the propagation wait is the whole point of the helper.
pub async fn create_topic<B: AdminBackend>(admin: &B, topic: &str, num_partitions: i32, replication_factor: i16) {
    let created = admin
        .create_topics(
            &[NewTopic::new(topic.to_string(), num_partitions, replication_factor)],
            CreateTopicsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{} backend: create topic {topic}: {e}", admin.name()));
    all_of(&created).unwrap_or_else(|e| panic!("{} backend: create topic {topic}: {e}", admin.name()));

    wait_for_all_partitions_metadata(admin, topic, num_partitions as usize).await;
}

/// Polls `list_topics` until `topic` is present (or absent, per `present`),
/// tolerating the metadata-propagation window after a create or delete.
///
/// The `wait_until_listed` of the original `admin_topics_test`, with the same
/// 50 × 200ms bound, expressed through
/// [`wait_until_true_with_timeout`](crate::common::test_utils::wait_until_true_with_timeout)
/// so a timeout fails with a message instead of a bare `false`.
pub async fn wait_until_listed<B: AdminBackend>(admin: &B, topic: &str, present: bool) {
    wait_until_true_with_timeout(
        || async {
            let names = admin
                .list_topics(ListTopicsOptions::new())
                .await
                .unwrap_or_else(|e| panic!("{} backend: list topics: {e}", admin.name()));
            names.contains_key(topic) == present
        },
        &format!(
            "{} backend: topic [{topic}] {} listed after 10000 ms",
            admin.name(),
            if present { "still not" } else { "still" }
        ),
        10_000,
        200,
    )
    .await;
}
