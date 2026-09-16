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

use confluent_kafka::admin::ConfigEntryOptionsBuilder;
use confluent_kafka::admin::config_entry::{ConfigSource, ConfigType};
use confluent_kafka::admin::{
    AbortTransactionOptions, AbortTransactionSpec, Admin, AdminClientConfig, AlterClientQuotasOptions, AlterConfigOp,
    AlterConfigsOptions, AlterConsumerGroupOffsetsOptions, AlterPartitionReassignmentsOptions,
    AlterReplicaLogDirsOptions, AlterUserScramCredentialsOptions, ClassicGroupDescription, Config, ConfigEntry,
    ConsumerGroupDescription, CreateAclsOptions, CreateDelegationTokenOptions, CreatePartitionsOptions,
    CreateTopicsOptions, CreateTopicsResult, DeleteAclsOptions, DeleteConsumerGroupOffsetsOptions,
    DeleteConsumerGroupsOptions, DeleteRecordsOptions, DeleteTopicsOptions, DeletedRecords, DescribeAclsOptions,
    DescribeClassicGroupsOptions, DescribeClientQuotasOptions, DescribeClusterOptions, DescribeConfigsOptions,
    DescribeConsumerGroupsOptions, DescribeDelegationTokenOptions, DescribeFeaturesOptions, DescribeLogDirsOptions,
    DescribeProducersOptions, DescribeReplicaLogDirsOptions, DescribeTopicsOptions, DescribeTransactionsOptions,
    DescribeUserScramCredentialsOptions, ElectLeadersOptions, ExpireDelegationTokenOptions, FeatureUpdate,
    FenceProducersOptions, FilterResults, FinalizedVersionRange, GroupListing, GroupOffsets,
    ListConfigResourcesOptions, ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsSpec, ListGroupsOptions,
    ListOffsetsOptions, ListOffsetsResultInfo, ListPartitionReassignmentsOptions, ListTopicsOptions,
    ListTransactionsOptions, LogDirDescription, MockAdminClient, NewPartitionReassignment, NewPartitions, NewTopic,
    OffsetSpec, PartitionProducerState, PartitionReassignment, RecordsToDelete, RemoveMembersFromConsumerGroupOptions,
    RenewDelegationTokenOptions, SupportedVersionRange, TerminateTransactionOptions, TopicDescription, TopicListing,
    TopicMetadataAndConfig, TransactionDescription, TransactionListing, UpdateFeaturesOptions,
    UserScramCredentialAlteration, UserScramCredentialsDescription, new_admin_client,
};
#[allow(deprecated)]
use confluent_kafka::admin::{
    ClientMetricsResourceListing, ConsumerGroupListing, ListClientMetricsResourcesOptions, ListConsumerGroupsOptions,
};
use confluent_kafka::common::Errors;
use confluent_kafka::common::acl::{AclBinding, AclBindingFilter, AclOperation};
use confluent_kafka::common::config::{ConfigResource, ConfigResourceType};
use confluent_kafka::common::quota::{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter};
use confluent_kafka::common::security::token::delegation::DelegationToken;
use confluent_kafka::common::utils::ProducerIdAndEpoch;
use confluent_kafka::common::{
    ElectionType, Error, KafkaFuture, Node, TopicCollection, TopicPartition, TopicPartitionReplica, Uuid,
};
use confluent_kafka::consumer::OffsetAndMetadata;

use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::common::test_utils::{
    DEFAULT_PAUSE_MS, TOPIC_METADATA_PROPAGATION_WAIT_MS, retry_on_error_with_timeout, wait_until_true_with_timeout,
};

/// Timeout that stands for Java's no-argument `Admin.close()`, which delegates
/// to `close(Duration.ofMillis(Long.MAX_VALUE))`. Same convention the C FFI
/// uses for a negative `timeout_ms` (`src/ffi/admin.rs::close_with_timeout`).
fn close_with_timeout(timeout: Option<Duration>) -> Duration {
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
/// as `HashMap<K, Result<V, Error>>` rather than as futures.
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
///   1. **Per-key methods return `HashMap<K, Result<V, Error>>`.** Today a
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
    ) -> Result<Outcomes<String, TopicMetadataAndConfig>, Error>;

    /// Delete topics by name (`TopicCollection::of_topic_names`).
    async fn delete_topics(
        &self,
        names: &[String],
        options: DeleteTopicsOptions,
    ) -> Result<Outcomes<String, ()>, Error>;

    /// Delete topics by id (`TopicCollection::of_topic_ids`).
    async fn delete_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DeleteTopicsOptions,
    ) -> Result<Outcomes<Uuid, ()>, Error>;

    /// List the topics in the cluster, keyed by topic name.
    ///
    /// Not an [`Outcomes`]: Java's `ListTopicsResult` holds one
    /// `KafkaFuture<Map<String, TopicListing>>`, so an individual listing can
    /// never fail. `names()` is this map's key set and `listings()` its values.
    async fn list_topics(&self, options: ListTopicsOptions) -> Result<HashMap<String, TopicListing>, Error>;

    /// Describe topics by name (`TopicCollection::of_topic_names`).
    async fn describe_topics_with_topics(
        &self,
        names: &[String],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<String, TopicDescription>, Error>;

    /// Describe topics by id (`TopicCollection::of_topic_ids`).
    async fn describe_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<Uuid, TopicDescription>, Error>;

    /// Increase the partition counts of the given topics.
    async fn create_partitions(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> Result<Outcomes<String, ()>, Error>;

    /// Delete records before the given offset of each partition.
    async fn delete_records(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> Result<Outcomes<TopicPartition, DeletedRecords>, Error>;

    /// Describe the cluster: its id, brokers, controller and (optionally) the
    /// operations the caller is authorized to perform on it.
    ///
    /// Convention #4: Java's `DescribeClusterResult` exposes four *independent*
    /// futures, so the resolved values arrive as one [`ClusterDescription`] and
    /// any failure is a whole-call failure — which is exactly what both bindings
    /// do (`ClusterDescription` in `admin.py`, the four direct attribute
    /// accessors on `kafka_admin_DescribeClusterResult_t`).
    async fn describe_cluster(&self, options: DescribeClusterOptions) -> Result<ClusterDescription, Error>;

    /// Describe the configuration of each resource.
    ///
    /// The value is [`ConfigView`] rather than the production [`Config`]; see
    /// [`ConfigEntryView`] for why.
    async fn describe_configs(
        &self,
        resources: &[ConfigResource],
        options: DescribeConfigsOptions,
    ) -> Result<Outcomes<ConfigResource, ConfigView>, Error>;

    /// Incrementally alter the configuration of each resource.
    async fn incremental_alter_configs(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ) -> Result<Outcomes<ConfigResource, ()>, Error>;

    /// List the cluster's config resources whose type is in
    /// `config_resource_types`; an empty set requests every supported type.
    ///
    /// Not an [`Outcomes`]: Java's `ListConfigResourcesResult` holds a single
    /// `KafkaFuture<Collection<ConfigResource>>`.
    async fn list_config_resources(
        &self,
        config_resource_types: &HashSet<ConfigResourceType>,
        options: ListConfigResourcesOptions,
    ) -> Result<Vec<ConfigResource>, Error>;

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
    ) -> Result<Vec<ClientMetricsResourceListing>, Error>;

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
    ) -> Result<Outcomes<i32, HashMap<String, LogDirDescription>>, Error>;

    /// Move each replica to the given log directory.
    async fn alter_replica_log_dirs(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ()>, Error>;

    /// Query which log directory hosts each replica, and which one it is moving
    /// to.
    ///
    /// The value is [`ReplicaLogDirInfoView`] rather than the production
    /// `ReplicaLogDirInfo`; see there for why.
    async fn describe_replica_log_dirs(
        &self,
        replicas: &[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ReplicaLogDirInfoView>, Error>;

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
    ) -> Result<Outcomes<TopicPartition, ()>, Error>;

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
    ) -> Result<Outcomes<TopicPartition, ()>, Error>;

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
    ) -> Result<HashMap<TopicPartition, PartitionReassignment>, Error>;

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
    ) -> Result<Outcomes<TopicPartition, ListOffsetsResultInfo>, Error>;

    /// List every group in the cluster.
    ///
    /// Not an [`Outcomes`] and not a plain `Vec`: Java's `ListGroupsResult`
    /// splits **one** future into `valid()` listings and an *unkeyed* `errors()`
    /// collection, and the two are independent — a partial success has both
    /// non-empty. See [`Listings`].
    async fn list_groups(&self, options: ListGroupsOptions) -> Result<Listings<GroupListing>, Error>;

    /// List the consumer groups in the cluster.
    ///
    /// Deprecated in Java since 4.1 in favour of
    /// [`AdminBackend::list_groups`], which covers every group type, and carried
    /// here because both bindings still expose it. Same [`Listings`] shape.
    #[allow(deprecated)]
    async fn list_consumer_groups(
        &self,
        options: ListConsumerGroupsOptions,
    ) -> Result<Listings<ConsumerGroupListing>, Error>;

    /// Describe the given groups, classic or KIP-848 consumer protocol.
    async fn describe_consumer_groups(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ConsumerGroupDescription>, Error>;

    /// Describe the given groups, classic protocol only.
    async fn describe_classic_groups(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> Result<Outcomes<String, ClassicGroupDescription>, Error>;

    /// List each group's committed offsets.
    ///
    /// Per-key by group id, and the value is *nested* — Java's per-group future
    /// resolves to a whole [`GroupOffsets`] map, whose values are themselves
    /// nullable: `None` means the group has no committed offset for that
    /// partition, which is not a committed offset of 0. Same two-level shape as
    /// [`AdminBackend::describe_log_dirs`], and not the
    /// value-carries-its-own-error case — no level of this value holds an error.
    async fn list_consumer_group_offsets_with_group_specs(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<String, GroupOffsets>, Error>;

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
    ) -> Result<Outcomes<TopicPartition, ()>, Error>;

    /// Delete `group_id`'s committed offsets for `partitions`.
    ///
    /// Same single-future shape as
    /// [`AdminBackend::alter_consumer_group_offsets`].
    async fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, Error>;

    /// Delete the given groups. Per-group void, with one future per key, so the
    /// outer `Err` has its ordinary narrow meaning.
    async fn delete_consumer_groups(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ()>, Error>;

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
    ) -> Result<Outcomes<String, ()>, Error>;

    /// Create the given ACL bindings, keyed by the binding itself.
    ///
    /// Java keys `CreateAclsResult.values()` by the whole `AclBinding` it was
    /// asked to create, so two identical bindings in one batch collapse to one
    /// key — Java's own behaviour, since the map is built from the request.
    async fn create_acls(
        &self,
        acls: &[AclBinding],
        options: CreateAclsOptions,
    ) -> Result<Outcomes<AclBinding, ()>, Error>;

    /// List the ACL bindings matching `filter`.
    ///
    /// Not an [`Outcomes`]: Java's `DescribeAclsResult` holds a single
    /// `KafkaFuture<Collection<AclBinding>>`, so an individual binding can never
    /// fail and an empty result is a successful "nothing matched".
    async fn describe_acls(
        &self,
        filter: &AclBindingFilter,
        options: DescribeAclsOptions,
    ) -> Result<Vec<AclBinding>, Error>;

    /// Delete every ACL matching each filter, keyed by the filter.
    ///
    /// The per-filter value is *nested* **and** carries its own errors: Java's
    /// per-filter future resolves to a whole [`FilterResults`], one
    /// `FilterResult { binding, exception }` per ACL the filter matched. So a
    /// filter can succeed here — it matched — while an individual matched ACL
    /// failed to delete. That is `admin_service.proto`'s envelope exception 3,
    /// and `deleteAcls` is the third and last of the three RPCs that reach it
    /// (`createTopics` and `describeLogDirs` are the others).
    async fn delete_acls(
        &self,
        filters: &[AclBindingFilter],
        options: DeleteAclsOptions,
    ) -> Result<Outcomes<AclBindingFilter, FilterResults>, Error>;

    /// Describe the client quotas matching `filter`, keyed by entity.
    ///
    /// Not an [`Outcomes`]: Java's `DescribeClientQuotasResult` holds one
    /// `KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>>`. A quota that
    /// has been *removed* is absent from the inner map rather than reported as
    /// zero, which is the only observable that separates a removal from a
    /// zero-valued set.
    async fn describe_client_quotas(
        &self,
        filter: &ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> Result<HashMap<ClientQuotaEntity, HashMap<String, f64>>, Error>;

    /// Apply each entity's quota changes. Per-entity void, with one future per
    /// key, so the outer `Err` has its ordinary narrow meaning.
    ///
    /// An `Op` whose value is `None` **removes** that quota (Java's null
    /// `Double`); every finite double including 0.0 is a legal quota value, so
    /// the two are not interchangeable.
    async fn alter_client_quotas(
        &self,
        entries: &[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> Result<Outcomes<ClientQuotaEntity, ()>, Error>;

    /// Describe each user's SCRAM credentials, keyed by user name. An empty
    /// `users` requests every user, which is Java's no-argument overload.
    ///
    /// Java's `DescribeUserScramCredentialsResult` holds one future over the raw
    /// response data and exposes three views (`all()` / `users()` /
    /// `description(user)`); this is the per-user shape that subsumes all three,
    /// identical to what `src/ffi/admin.rs`'s
    /// `submit_describe_user_scram_credentials` composes and what `admin.py`
    /// returns, so all four backends answer with the same key set.
    ///
    /// The broker never returns the salted password or the salt, so the value
    /// carries only the mechanism and iteration count per credential.
    async fn describe_user_scram_credentials(
        &self,
        users: &[String],
        options: DescribeUserScramCredentialsOptions,
    ) -> Result<Outcomes<String, UserScramCredentialsDescription>, Error>;

    /// Apply each SCRAM credential upsertion / deletion. Per-user void, one
    /// future per key.
    async fn alter_user_scram_credentials(
        &self,
        alterations: &[UserScramCredentialAlteration],
        options: AlterUserScramCredentialsOptions,
    ) -> Result<Outcomes<String, ()>, Error>;

    /// Create a delegation token.
    ///
    /// Not an [`Outcomes`]: `CreateDelegationTokenResult` holds a single
    /// `KafkaFuture<DelegationToken>`.
    async fn create_delegation_token(&self, options: CreateDelegationTokenOptions) -> Result<DelegationToken, Error>;

    /// Renew the token identified by `hmac`, returning its new expiry timestamp.
    async fn renew_delegation_token(&self, hmac: &[u8], options: RenewDelegationTokenOptions) -> Result<i64, Error>;

    /// Expire the token identified by `hmac`, returning the expiry timestamp.
    ///
    /// An `expiry_time_period_ms` of -1 means *expire immediately* rather than
    /// "use the broker default", which is the sentinel the other two token
    /// options use.
    async fn expire_delegation_token(&self, hmac: &[u8], options: ExpireDelegationTokenOptions) -> Result<i64, Error>;

    /// List the delegation tokens the caller may see, optionally filtered by
    /// owner (`options.owners()`; `None` is Java's unset filter).
    async fn describe_delegation_token(
        &self,
        options: DescribeDelegationTokenOptions,
    ) -> Result<Vec<DelegationToken>, Error>;

    /// Describe the cluster's supported and finalized feature versions.
    ///
    /// The value is [`FeatureMetadataView`] rather than the production
    /// `FeatureMetadata`; see there for why.
    async fn describe_features(&self, options: DescribeFeaturesOptions) -> Result<FeatureMetadataView, Error>;

    /// Raise (or downgrade) the finalized level of each feature. Per-feature
    /// void, one future per key.
    ///
    /// The outer `Err` also carries a *synchronous* failure, which this is the
    /// only RPC in the harness to have: the Rust `Admin::update_features` returns
    /// `Result<UpdateFeaturesResult, Error>` because Java's `updateFeatures`
    /// throws `IllegalArgumentException` for an empty map or a blank feature
    /// name, before any future exists.
    async fn update_features(
        &self,
        feature_updates: &HashMap<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ) -> Result<Outcomes<String, ()>, Error>;

    /// Describe the active producers of each partition.
    ///
    /// `options.broker_id()` is Java's `OptionalInt`: unset queries each
    /// partition's leader (`PartitionLeaderStrategy`), set sends the request
    /// straight to that broker (`StaticBrokerStrategy`). The two must agree on a
    /// single-broker cluster.
    async fn describe_producers(
        &self,
        partitions: &[TopicPartition],
        options: DescribeProducersOptions,
    ) -> Result<Outcomes<TopicPartition, PartitionProducerState>, Error>;

    /// Describe each transactional id's current transaction.
    async fn describe_transactions(
        &self,
        transactional_ids: &[String],
        options: DescribeTransactionsOptions,
    ) -> Result<Outcomes<String, TransactionDescription>, Error>;

    /// Abort the transaction described by `spec` on its partition.
    ///
    /// Not an [`Outcomes`], and not even a keyed response: Java's
    /// `AbortTransactionResult` exposes exactly one method, `all()`, over a
    /// per-partition map that is private, and the RPC takes exactly one spec, so
    /// there is one key by construction. Both bindings collapse it the same way
    /// — the C entry points return no result handle at all and `admin.py`
    /// resolves to `None`.
    async fn abort_transaction(
        &self,
        spec: AbortTransactionSpec,
        options: AbortTransactionOptions,
    ) -> Result<(), Error>;

    /// Forcibly terminate `transactional_id`'s ongoing transaction.
    ///
    /// Same single-void-future shape as
    /// [`AdminBackend::abort_transaction`]: `TerminateTransactionResult` exposes
    /// only `result()`.
    async fn force_terminate_transaction(
        &self,
        transactional_id: &str,
        options: TerminateTransactionOptions,
    ) -> Result<(), Error>;

    /// List the cluster's transactions, keyed by the broker that reported them.
    ///
    /// Driven from Java's `byBrokerId()` rather than `all()` or
    /// `allByBrokerId()`: it is the only one of the three views that keeps a
    /// **per-broker** future, so a listing that succeeded on one broker and
    /// failed on another reports both instead of discarding the successful half.
    /// Same choice `src/ffi/admin.rs`'s `submit_list_transactions` makes. The
    /// outer `Err` is the failure of the broker-*discovery* step, which in Java
    /// fails all three views together.
    async fn list_transactions(
        &self,
        options: ListTransactionsOptions,
    ) -> Result<Outcomes<i32, Vec<TransactionListing>>, Error>;

    /// Fence out any producer currently using each transactional id, returning
    /// the newly allocated producer id and epoch.
    ///
    /// Java never exposes the `ProducerIdAndEpoch` as one value —
    /// `producerId(id)` and `epochId(id)` are two `then_apply` projections of the
    /// same per-id future — so the pair is reassembled here exactly as
    /// `src/ffi/admin.rs`'s `submit_fence_producers` does. Both projections
    /// resolve from the same future, so they fail together.
    async fn fence_producers(
        &self,
        transactional_ids: &[String],
        options: FenceProducersOptions,
    ) -> Result<Outcomes<String, ProducerIdAndEpoch>, Error>;

    /// Close the admin client, joining its background task.
    ///
    /// `timeout` of `None` is Java's no-argument `close()`. Java's
    /// `Admin.close(Duration)` is `void` and so is the Rust `Admin::close`; the
    /// `Result` here exists because the gRPC backends can fail at the transport
    /// or binding level, which is a harness failure the scenario must see rather
    /// than a Kafka-level error.
    async fn close(&self, timeout: Option<Duration>) -> Result<(), Error>;

    /// Short backend label used in assertion messages.
    fn name(&self) -> &'static str;
}

/// The already-resolved per-key outcomes of one Admin RPC: convention #1 of
/// [`AdminBackend`]'s signature rules, named so the method signatures stay
/// readable. The outer `Result` on every method is the whole-call failure that
/// precedes any per-key future (a synchronous throw, an unknown handle, a
/// transport error); this map is what a Java caller would read out of the
/// `*Result`'s per-key `KafkaFuture`s.
pub type Outcomes<K, V> = HashMap<K, Result<V, Error>>;

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
/// `admin.py`'s `list_groups` returns `([GroupListing], [Error])` and the C
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
// No `PartialEq`: `Error` is not comparable (it carries a message and a
// source), so `errors` cannot be. Scenarios compare `valid` and read
// `errors`' codes.
#[derive(Clone, Debug)]
pub struct Listings<T> {
    /// Java `valid()`.
    pub valid: Vec<T>,
    /// Java `errors()`, unkeyed.
    pub errors: Vec<Error>,
}

impl<T> Listings<T> {
    /// Java's `all()`: the listings if every broker answered, otherwise the
    /// first error.
    ///
    /// Java's `all()` completes exceptionally with one of the failures without
    /// specifying which; this picks the first, and because the fold runs
    /// identically for all four backends over the same pair it cannot make
    /// backends disagree. Same reasoning as [`all_of`].
    pub fn all(&self) -> Result<&[T], Error> {
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
pub fn all_of<K, V>(outcomes: &Outcomes<K, V>) -> Result<(), Error> {
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
///
/// # Which arms the key-set half can fail on
///
/// Only the gRPC ones. Several [`RustNativeAdmin`] methods build their `Outcomes`
/// map by iterating the *requested* keys, so `got == want` holds by construction
/// and the key-set assertion is inert on `__rust` (Critic round-15 LOW 4). The
/// `all_of` half has teeth on every arm. A commit message or ledger entry that
/// cites this helper as a strengthening must therefore say it is a strengthening
/// under `--features multilanguage-tests`, not natively.
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
// Slice G5 added exactly one, [`FeatureMetadataView`]: `FeatureMetadata::new` is
// `pub(crate)`. Everything else in that slice crosses as a production type,
// checked one by one rather than assumed — `AclBinding::new`,
// `AclBindingFilter::new`, `AccessControlEntry::new`,
// `AccessControlEntryFilter::new`, `ResourcePattern::new`,
// `ResourcePatternFilter::new`, `ClientQuotaEntity::new`,
// `ClientQuotaFilterComponent::{of_entity, of_default_entity, of_entity_type}`,
// `ClientQuotaAlteration::new`, `Op::new`, `ScramCredentialInfo::new`,
// `UserScramCredentialsDescription::new`, `FilterResult::new`,
// `FilterResults::new`, `DelegationToken::new`, `TokenInformation::with_token_requester`,
// `KafkaPrincipal::with_token_authenticated`, `SupportedVersionRange::new` and
// `FinalizedVersionRange::new` are all public. (`ClientQuotaFilter::new` is
// private, but it is an *input* the harness only reads, and its three public
// factories cover both `strict` values.)
//
// Slice G4 added none either, for the same reason: `GroupListing::new`,
// `ConsumerGroupListing::with_group_state_group_type`, `ConsumerGroupDescription::new`,
// `ClassicGroupDescription::new`, `MemberDescription::new`,
// `MemberAssignment::new`, `MemberToRemove::new` and
// `OffsetAndMetadata::{new, new_metadata, new_leader_epoch_metadata}` are all public, so
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
/// The blocker is [`ConfigSynonymView`]: `ConfigEntry::with_options` is public
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

/// The cluster's feature versions, standing in for the production
/// `FeatureMetadata`.
///
/// `FeatureMetadata::new` is `pub(crate)` — faithfully, because Java's sole
/// `FeatureMetadata` constructor is package-private (`FeatureMetadata.java:38`,
/// no access modifier) — so a test crate cannot build one and a gRPC
/// backend cannot rebuild what the wire carried. Field-for-field identical to the
/// production type, and the two range types inside it *are* the production ones
/// (`SupportedVersionRange::new` and `FinalizedVersionRange::new` are both
/// public, checked rather than assumed, per the rule G2 set for every value
/// type).
///
/// The two maps are independent: a feature can be supported without being
/// finalized, so neither their sizes nor their key sets need agree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureMetadataView {
    /// Java `FeatureMetadata.finalizedFeatures()`.
    pub finalized_features: HashMap<String, FinalizedVersionRange>,
    /// Java `finalizedFeaturesEpoch()`, an `Optional<Long>`: `None` is "the
    /// cluster reported no epoch", which is not epoch 0.
    pub finalized_features_epoch: Option<i64>,
    /// Java `supportedFeatures()`.
    pub supported_features: HashMap<String, SupportedVersionRange>,
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
    pub fn from_config(config: &HashMap<String, String>) -> Result<Self, Error> {
        let config = AdminClientConfig::new(config)?;
        Ok(Self { admin: new_admin_client(config)? })
    }

    /// Build a broker-less [`MockAdminClient`] with `num_brokers` brokers.
    ///
    /// # Errors
    ///
    /// Propagates [`MockAdminClient::create`]'s rejection of `num_brokers < 1`,
    /// which is Java's `brokers.get(0)` throw.
    pub fn mock(num_brokers: i32) -> Result<Self, Error> {
        Ok(Self { admin: Box::new(MockAdminClient::create(num_brokers)?) })
    }
}

impl AdminBackend for RustNativeAdmin {
    async fn create_topics(
        &self,
        new_topics: &[NewTopic],
        options: CreateTopicsOptions,
    ) -> Result<Outcomes<String, TopicMetadataAndConfig>, Error> {
        let result = self.admin.create_topics_with_options(new_topics, options);
        let mut outcomes = HashMap::new();
        for (name, created) in result.values() {
            // `values()` is Java's `KafkaFuture<Void>` view: it fails only if the
            // creation itself failed. The metadata is a second, independent
            // level — see `metadata_of`.
            let outcome = match created.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await {
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
    ) -> Result<Outcomes<String, ()>, Error> {
        let result = self
            .admin
            .delete_topics_with_options(TopicCollection::of_topic_names(names.to_vec()), options);
        let values = result.topic_name_values().ok_or_else(|| {
            Error::local_illegal_state("deleteTopics(ofTopicNames) did not return name-keyed futures")
        })?;
        Ok(resolve(values.iter().map(|(name, f)| (name.clone(), f.clone()))).await)
    }

    async fn delete_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DeleteTopicsOptions,
    ) -> Result<Outcomes<Uuid, ()>, Error> {
        let result = self
            .admin
            .delete_topics_with_options(TopicCollection::of_topic_ids(topic_ids.to_vec()), options);
        let values = result
            .topic_id_values()
            .ok_or_else(|| Error::local_illegal_state("deleteTopics(ofTopicIds) did not return id-keyed futures"))?;
        Ok(resolve(values.iter().map(|(id, f)| (*id, f.clone()))).await)
    }

    async fn list_topics(&self, options: ListTopicsOptions) -> Result<HashMap<String, TopicListing>, Error> {
        self.admin
            .list_topics_with_options(options)
            .names_to_listings()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn describe_topics_with_topics(
        &self,
        names: &[String],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<String, TopicDescription>, Error> {
        let result = self
            .admin
            .describe_topics_with_topics_options(TopicCollection::of_topic_names(names.to_vec()), options);
        let values = result.topic_name_values().ok_or_else(|| {
            Error::local_illegal_state("describeTopics(ofTopicNames) did not return name-keyed futures")
        })?;
        Ok(resolve(values.iter().map(|(name, f)| (name.clone(), f.clone()))).await)
    }

    async fn describe_topics_by_ids(
        &self,
        topic_ids: &[Uuid],
        options: DescribeTopicsOptions,
    ) -> Result<Outcomes<Uuid, TopicDescription>, Error> {
        let result = self
            .admin
            .describe_topics_with_topics_options(TopicCollection::of_topic_ids(topic_ids.to_vec()), options);
        let values = result
            .topic_id_values()
            .ok_or_else(|| Error::local_illegal_state("describeTopics(ofTopicIds) did not return id-keyed futures"))?;
        Ok(resolve(values.iter().map(|(id, f)| (*id, f.clone()))).await)
    }

    async fn create_partitions(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        options: CreatePartitionsOptions,
    ) -> Result<Outcomes<String, ()>, Error> {
        let result = self.admin.create_partitions_with_options(new_partitions, options);
        Ok(resolve(result.values().iter().map(|(name, f)| (name.clone(), f.clone()))).await)
    }

    async fn delete_records(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        options: DeleteRecordsOptions,
    ) -> Result<Outcomes<TopicPartition, DeletedRecords>, Error> {
        let result = self.admin.delete_records_with_options(records_to_delete, options);
        Ok(resolve(result.low_watermarks().iter().map(|(tp, f)| (tp.clone(), f.clone()))).await)
    }

    async fn describe_cluster(&self, options: DescribeClusterOptions) -> Result<ClusterDescription, Error> {
        let result = self.admin.describe_cluster_with_options(options);
        // All four are awaited before any error is reported, so none is
        // abandoned; when more than one failed, the first in Java's declaration
        // order wins. Identical to the FFI's `submit_describe_cluster`, so the
        // four backends pick the same error out of a multi-failure.
        let nodes = result.nodes().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
        let controller = result.controller().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
        let cluster_id = result.cluster_id().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
        let authorized_operations = result.authorized_operations().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
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
    ) -> Result<Outcomes<ConfigResource, ConfigView>, Error> {
        let result = self.admin.describe_configs_with_options(resources, options);
        let mut outcomes = HashMap::with_capacity(result.values().len());
        for (resource, future) in result.values() {
            outcomes.insert(
                resource.clone(),
                future
                    .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
                    .await
                    .map(|config| config_view(&config)),
            );
        }
        Ok(outcomes)
    }

    async fn incremental_alter_configs(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        options: AlterConfigsOptions,
    ) -> Result<Outcomes<ConfigResource, ()>, Error> {
        let result = self.admin.incremental_alter_configs_with_options(configs, options);
        Ok(resolve(result.values().iter().map(|(r, f)| (r.clone(), f.clone()))).await)
    }

    async fn list_config_resources(
        &self,
        config_resource_types: &HashSet<ConfigResourceType>,
        options: ListConfigResourcesOptions,
    ) -> Result<Vec<ConfigResource>, Error> {
        self.admin
            .list_config_resources_with_options(config_resource_types, options)
            .all()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    #[allow(deprecated)]
    async fn list_client_metrics_resources(
        &self,
        options: ListClientMetricsResourcesOptions,
    ) -> Result<Vec<ClientMetricsResourceListing>, Error> {
        self.admin
            .list_client_metrics_resources_with_options(options)
            .all()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn describe_log_dirs(
        &self,
        brokers: &[i32],
        options: DescribeLogDirsOptions,
    ) -> Result<Outcomes<i32, HashMap<String, LogDirDescription>>, Error> {
        let result = self.admin.describe_log_dirs_with_options(brokers, options);
        Ok(resolve(result.descriptions().iter().map(|(broker, f)| (*broker, f.clone()))).await)
    }

    async fn alter_replica_log_dirs(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        options: AlterReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ()>, Error> {
        let result = self.admin.alter_replica_log_dirs_with_options(replica_assignment, options);
        Ok(resolve(result.values().iter().map(|(r, f)| (r.clone(), f.clone()))).await)
    }

    async fn describe_replica_log_dirs(
        &self,
        replicas: &[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> Result<Outcomes<TopicPartitionReplica, ReplicaLogDirInfoView>, Error> {
        let result = self.admin.describe_replica_log_dirs_with_options(replicas, options);
        let mut outcomes = HashMap::with_capacity(result.values().len());
        for (replica, future) in result.values() {
            let outcome = future
                .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
                .await
                .map(|info| ReplicaLogDirInfoView {
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
    ) -> Result<Outcomes<TopicPartition, ()>, Error> {
        // One future for the whole map, so its failure is the outer `Err`; the
        // per-partition `Optional<Throwable>` inside becomes the inner `Result`.
        // Same shape as the FFI's `submit_elect_leaders`, which likewise returns
        // `partitions()` unchanged.
        let outcomes = self
            .admin
            .elect_leaders_with_options(election_type, partitions, options)
            .partitions()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
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
    ) -> Result<Outcomes<TopicPartition, ()>, Error> {
        let result = self.admin.alter_partition_reassignments_with_options(reassignments, options);
        Ok(resolve(result.values().iter().map(|(tp, f)| (tp.clone(), f.clone()))).await)
    }

    async fn list_partition_reassignments(
        &self,
        partitions: Option<HashSet<TopicPartition>>,
        options: ListPartitionReassignmentsOptions,
    ) -> Result<HashMap<TopicPartition, PartitionReassignment>, Error> {
        self.admin
            .list_partition_reassignments_with_partitions_options(partitions, options)
            .reassignments()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn list_offsets(
        &self,
        topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
        options: ListOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ListOffsetsResultInfo>, Error> {
        let result = self.admin.list_offsets_with_options(topic_partition_offsets, options);
        // `ListOffsetsResult` exposes its futures through `partitionResult(tp)`
        // rather than as a map, so the *requested* keys drive the collection —
        // and a key the call did not attempt is a whole-call `Err`, not a
        // missing entry. Identical to the FFI's `submit_list_offsets`, so all
        // four backends answer with the same key set.
        let mut outcomes = HashMap::with_capacity(topic_partition_offsets.len());
        for tp in topic_partition_offsets.keys() {
            outcomes.insert(
                tp.clone(),
                result.partition_result(tp)?.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
            );
        }
        Ok(outcomes)
    }

    async fn list_groups(&self, options: ListGroupsOptions) -> Result<Listings<GroupListing>, Error> {
        let result = self.admin.list_groups_with_options(options);
        // Both views are awaited before either error is reported, so neither is
        // abandoned. Identical to the FFI's `submit_list_groups`.
        let valid = result.valid().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
        let errors = result.errors().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
        Ok(Listings { valid: valid?, errors: errors? })
    }

    #[allow(deprecated)]
    async fn list_consumer_groups(
        &self,
        options: ListConsumerGroupsOptions,
    ) -> Result<Listings<ConsumerGroupListing>, Error> {
        let result = self.admin.list_consumer_groups_with_options(options);
        let valid = result.valid().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
        let errors = result.errors().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
        Ok(Listings { valid: valid?, errors: errors? })
    }

    async fn describe_consumer_groups(
        &self,
        group_ids: &[String],
        options: DescribeConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ConsumerGroupDescription>, Error> {
        let result = self.admin.describe_consumer_groups_with_options(group_ids, options);
        Ok(resolve(result.described_groups().into_iter()).await)
    }

    async fn describe_classic_groups(
        &self,
        group_ids: &[String],
        options: DescribeClassicGroupsOptions,
    ) -> Result<Outcomes<String, ClassicGroupDescription>, Error> {
        let result = self.admin.describe_classic_groups_with_options(group_ids, options);
        Ok(resolve(result.described_groups().into_iter()).await)
    }

    async fn list_consumer_group_offsets_with_group_specs(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<String, GroupOffsets>, Error> {
        let result = self
            .admin
            .list_consumer_group_offsets_with_group_specs_options(group_specs, options);
        // The *requested* group ids drive the collection, and a group the call
        // did not attempt is a whole-call `Err` rather than a missing entry —
        // Java's `partitionsToOffsetAndMetadata(groupId)` throws
        // `IllegalArgumentException` there. Identical to the FFI's
        // `submit_list_consumer_group_offsets`, so all four backends answer with
        // the same key set.
        let mut outcomes = HashMap::with_capacity(group_specs.len());
        for group_id in group_specs.keys() {
            let future = result.partitions_to_offset_and_metadata_for_group(group_id)?;
            outcomes.insert(group_id.clone(), future.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await);
        }
        Ok(outcomes)
    }

    async fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        options: AlterConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, Error> {
        let result = self.admin.alter_consumer_group_offsets_with_options(group_id, offsets, options);
        if offsets.is_empty() {
            // No per-partition slot exists, so the single future's failure is
            // the only observable. Same branch as the FFI's
            // `submit_alter_consumer_group_offsets`.
            result.all().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await?;
            return Ok(HashMap::new());
        }
        let mut outcomes = HashMap::with_capacity(offsets.len());
        for tp in offsets.keys() {
            outcomes.insert(
                tp.clone(),
                result.partition_result(tp).get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
            );
        }
        Ok(outcomes)
    }

    async fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: &HashSet<TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> Result<Outcomes<TopicPartition, ()>, Error> {
        let result = self
            .admin
            .delete_consumer_group_offsets_with_options(group_id, partitions, options);
        if partitions.is_empty() {
            result.all().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await?;
            return Ok(HashMap::new());
        }
        let mut outcomes = HashMap::with_capacity(partitions.len());
        for tp in partitions {
            outcomes.insert(
                tp.clone(),
                result.partition_result(tp)?.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
            );
        }
        Ok(outcomes)
    }

    async fn delete_consumer_groups(
        &self,
        group_ids: &[String],
        options: DeleteConsumerGroupsOptions,
    ) -> Result<Outcomes<String, ()>, Error> {
        let result = self.admin.delete_consumer_groups_with_options(group_ids, options);
        Ok(resolve(result.deleted_groups().into_iter()).await)
    }

    async fn remove_members_from_consumer_group(
        &self,
        group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> Result<Outcomes<String, ()>, Error> {
        // The member set has to be read off the options before they are moved
        // into the call, exactly as the FFI's
        // `submit_remove_members_from_consumer_group` does.
        let members: Vec<_> = options.members().iter().cloned().collect();
        let result = self.admin.remove_members_from_consumer_group_with_options(group_id, options);
        if members.is_empty() {
            // `removeAll` mode: Java's `memberResult` is not applicable, so
            // `all()` is the only observable and the map stays empty.
            result.all().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await?;
            return Ok(HashMap::new());
        }
        let mut outcomes = HashMap::with_capacity(members.len());
        for member in &members {
            let future = result.member_result(member)?;
            outcomes.insert(
                member.group_instance_id().to_string(),
                future.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
            );
        }
        Ok(outcomes)
    }

    async fn create_acls(
        &self,
        acls: &[AclBinding],
        options: CreateAclsOptions,
    ) -> Result<Outcomes<AclBinding, ()>, Error> {
        let result = self.admin.create_acls_with_options(acls, options);
        Ok(resolve(result.values().iter().map(|(binding, f)| (binding.clone(), f.clone()))).await)
    }

    async fn describe_acls(
        &self,
        filter: &AclBindingFilter,
        options: DescribeAclsOptions,
    ) -> Result<Vec<AclBinding>, Error> {
        self.admin
            .describe_acls_with_options(filter, options)
            .values()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn delete_acls(
        &self,
        filters: &[AclBindingFilter],
        options: DeleteAclsOptions,
    ) -> Result<Outcomes<AclBindingFilter, FilterResults>, Error> {
        let result = self.admin.delete_acls_with_options(filters, options);
        Ok(resolve(result.values().iter().map(|(filter, f)| (filter.clone(), f.clone()))).await)
    }

    async fn describe_client_quotas(
        &self,
        filter: &ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> Result<HashMap<ClientQuotaEntity, HashMap<String, f64>>, Error> {
        self.admin
            .describe_client_quotas_with_options(filter, options)
            .entities()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn alter_client_quotas(
        &self,
        entries: &[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> Result<Outcomes<ClientQuotaEntity, ()>, Error> {
        let result = self.admin.alter_client_quotas_with_options(entries, options);
        Ok(resolve(result.values().iter().map(|(entity, f)| (entity.clone(), f.clone()))).await)
    }

    async fn describe_user_scram_credentials(
        &self,
        users: &[String],
        options: DescribeUserScramCredentialsOptions,
    ) -> Result<Outcomes<String, UserScramCredentialsDescription>, Error> {
        let result = self.admin.describe_user_scram_credentials_with_users_options(users, options);
        // Java's three views composed into the per-user shape, exactly as
        // `src/ffi/admin.rs`'s `submit_describe_user_scram_credentials` does — so
        // all four backends answer with the same key set and the same errors.
        //
        //   - `all()` succeeds only when every user's error code is NONE or
        //     RESOURCE_NOT_FOUND, so when it does its keys are the complete user
        //     set and no row carries an error;
        //   - when it fails, `users()` still lists every user whose error is not
        //     RESOURCE_NOT_FOUND — necessarily including the one that failed
        //     `all()` — and `description(user)` yields that user's own error.
        //     The users omitted at that point are exactly the ones Java's `all()`
        //     also declines to report.
        //   - if the response future itself failed, all three fail with the same
        //     error and it becomes the whole-call `Err`; and if the composition
        //     yields no rows at all, the `all()` error is returned rather than
        //     dropped (the empty-key-set trap).
        let all_error = match result.all().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await {
            Ok(map) => {
                return Ok(map.into_iter().map(|(user, description)| (user, Ok(description))).collect());
            },
            Err(e) => e,
        };
        let listed = result.users().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await?;
        let mut outcomes = HashMap::with_capacity(listed.len());
        for user in listed {
            let outcome = result.description(&user).get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
            outcomes.insert(user, outcome);
        }
        if outcomes.is_empty() {
            return Err(all_error);
        }
        Ok(outcomes)
    }

    async fn alter_user_scram_credentials(
        &self,
        alterations: &[UserScramCredentialAlteration],
        options: AlterUserScramCredentialsOptions,
    ) -> Result<Outcomes<String, ()>, Error> {
        let result = self.admin.alter_user_scram_credentials_with_options(alterations, options);
        Ok(resolve(result.values().iter().map(|(user, f)| (user.clone(), f.clone()))).await)
    }

    async fn create_delegation_token(&self, options: CreateDelegationTokenOptions) -> Result<DelegationToken, Error> {
        self.admin
            .create_delegation_token_with_options(options)
            .delegation_token()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn renew_delegation_token(&self, hmac: &[u8], options: RenewDelegationTokenOptions) -> Result<i64, Error> {
        self.admin
            .renew_delegation_token_with_options(hmac, options)
            .expiry_timestamp()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn expire_delegation_token(&self, hmac: &[u8], options: ExpireDelegationTokenOptions) -> Result<i64, Error> {
        self.admin
            .expire_delegation_token_with_options(hmac, options)
            .expiry_timestamp()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn describe_delegation_token(
        &self,
        options: DescribeDelegationTokenOptions,
    ) -> Result<Vec<DelegationToken>, Error> {
        self.admin
            .describe_delegation_token_with_options(options)
            .delegation_tokens()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn describe_features(&self, options: DescribeFeaturesOptions) -> Result<FeatureMetadataView, Error> {
        let metadata = self
            .admin
            .describe_features_with_options(options)
            .feature_metadata()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await?;
        Ok(FeatureMetadataView {
            finalized_features: metadata.finalized_features().clone(),
            finalized_features_epoch: metadata.finalized_features_epoch(),
            supported_features: metadata.supported_features().clone(),
        })
    }

    async fn update_features(
        &self,
        feature_updates: &HashMap<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ) -> Result<Outcomes<String, ()>, Error> {
        // The only RPC whose Rust submission is fallible: an empty map or a blank
        // feature name is an `IllegalArgumentException` in Java, thrown before any
        // future exists, so it is the whole-call `Err` here.
        let result = self.admin.update_features_with_options(feature_updates, options)?;
        Ok(resolve(result.values().iter().map(|(feature, f)| (feature.clone(), f.clone()))).await)
    }

    async fn describe_producers(
        &self,
        partitions: &[TopicPartition],
        options: DescribeProducersOptions,
    ) -> Result<Outcomes<TopicPartition, PartitionProducerState>, Error> {
        let result = self.admin.describe_producers_with_options(partitions, options);
        // `DescribeProducersResult` exposes `partitionResult(tp)` rather than a
        // map, so the *requested* keys drive the collection and a partition the
        // call did not attempt is a whole-call `Err`. A repeated partition
        // collapses to one key, as Java's `Map` does. Identical to the FFI's
        // `submit_describe_producers`, so all four backends answer with the same
        // key set.
        let mut outcomes = HashMap::with_capacity(partitions.len());
        for tp in partitions {
            if outcomes.contains_key(tp) {
                continue;
            }
            outcomes.insert(
                tp.clone(),
                result.partition_result(tp)?.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
            );
        }
        Ok(outcomes)
    }

    async fn describe_transactions(
        &self,
        transactional_ids: &[String],
        options: DescribeTransactionsOptions,
    ) -> Result<Outcomes<String, TransactionDescription>, Error> {
        let result = self.admin.describe_transactions_with_options(transactional_ids, options);
        // `description(id)` rather than a map, so the requested ids drive the
        // collection — the `describeProducers` shape, and the FFI's
        // `submit_describe_transactions`.
        let mut outcomes = HashMap::with_capacity(transactional_ids.len());
        for id in transactional_ids {
            if outcomes.contains_key(id) {
                continue;
            }
            outcomes.insert(
                id.clone(),
                result.description(id)?.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
            );
        }
        Ok(outcomes)
    }

    async fn abort_transaction(
        &self,
        spec: AbortTransactionSpec,
        options: AbortTransactionOptions,
    ) -> Result<(), Error> {
        self.admin
            .abort_transaction_with_options(spec, options)
            .all()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn force_terminate_transaction(
        &self,
        transactional_id: &str,
        options: TerminateTransactionOptions,
    ) -> Result<(), Error> {
        self.admin
            .force_terminate_transaction_with_options(transactional_id, options)
            .result()
            .get_with_timeout(NATIVE_FUTURE_TIMEOUT)
            .await
    }

    async fn list_transactions(
        &self,
        options: ListTransactionsOptions,
    ) -> Result<Outcomes<i32, Vec<TransactionListing>>, Error> {
        let result = self.admin.list_transactions_with_options(options);
        // `by_broker_id()` keeps the per-broker future, so a broker that failed
        // is one entry error rather than a whole-call failure; only the
        // broker-discovery future's own failure is the outer `Err`. Same view the
        // FFI's `submit_list_transactions` drives.
        let by_broker = result.by_broker_id().get_with_timeout(NATIVE_FUTURE_TIMEOUT).await?;
        let mut outcomes = HashMap::with_capacity(by_broker.len());
        for (broker_id, future) in by_broker {
            outcomes.insert(broker_id, future.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await);
        }
        Ok(outcomes)
    }

    async fn fence_producers(
        &self,
        transactional_ids: &[String],
        options: FenceProducersOptions,
    ) -> Result<Outcomes<String, ProducerIdAndEpoch>, Error> {
        let result = self.admin.fence_producers_with_options(transactional_ids, options);
        // Java has no accessor for the pair, only the two `then_apply`
        // projections `producerId(id)` and `epochId(id)`. They resolve from the
        // same per-id future, so they succeed or fail together and awaiting both
        // is not a second request (`KafkaFuture::get` is re-callable) — exactly
        // what the FFI's `submit_fence_producers` does.
        let mut outcomes = HashMap::with_capacity(transactional_ids.len());
        for id in transactional_ids {
            if outcomes.contains_key(id) {
                continue;
            }
            let producer_id = result.producer_id(id)?.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
            let epoch = result.epoch_id(id)?.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await;
            let outcome = match (producer_id, epoch) {
                (Ok(producer_id), Ok(epoch)) => Ok(ProducerIdAndEpoch::new(producer_id, epoch)),
                // Both projections share one future, so the two errors are the
                // same one; report whichever is present.
                (Err(e), _) | (_, Err(e)) => Err(e),
            };
            outcomes.insert(id.clone(), outcome);
        }
        Ok(outcomes)
    }

    async fn close(&self, timeout: Option<Duration>) -> Result<(), Error> {
        self.admin.close_with_timeout(close_with_timeout(timeout)).await;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "rust"
    }
}

/// How long any single `RustNativeAdmin` await may take before it fails instead
/// of hanging.
///
/// Every admin integration test predating this harness awaited with an explicit
/// `get_with_timeout(Duration::from_secs(30))`, and the first conversions replaced
/// that with an unbounded `get()`. `admin_config` sets
/// `default.api.timeout.ms=30000`, so a broker-side stall still fails on its
/// own; what an unbounded `get()` loses is the bound on a **client-side future
/// that is never completed at all** — no API timeout fires for it, so the test
/// binary hangs and takes every other entry down with it, which CLAUDE.md §5
/// singles out as worse than an explicit error. The gRPC backends already have a
/// channel deadline, so this restores the same guarantee on the native arm.
const NATIVE_FUTURE_TIMEOUT: Duration = Duration::from_secs(30);

/// Awaits every per-key `KafkaFuture` and collects the outcomes.
///
/// This is what both bindings do internally before handing a result back to
/// their caller (`admin.py`'s `_run_sync`, the C `_async` entry points' result
/// struct), so doing it here is what makes the four backends comparable. Each
/// await is bounded — see [`NATIVE_FUTURE_TIMEOUT`].
async fn resolve<K, V>(futures: impl Iterator<Item = (K, KafkaFuture<V>)>) -> Outcomes<K, V>
where
    K: std::hash::Hash + Eq,
    V: Clone + Send + Sync + 'static,
{
    let mut outcomes = HashMap::new();
    for (key, future) in futures {
        outcomes.insert(key, future.get_with_timeout(NATIVE_FUTURE_TIMEOUT).await);
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
        result.topic_id(topic).get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
        result.num_partitions(topic).get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
        result.replication_factor(topic).get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
        result.config(topic).get_with_timeout(NATIVE_FUTURE_TIMEOUT).await,
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
                .unwrap_or_else(|| Error::local_illegal_state("createTopics metadata accessors disagreed on success")),
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
                let options = ConfigEntryOptionsBuilder::new()
                    .set_name(entry.name().to_string())
                    .set_value(entry.value().map(str::to_string))
                    .set_source(if entry.is_default() {
                        ConfigSource::DefaultConfig
                    } else {
                        ConfigSource::Unknown
                    })
                    .set_is_sensitive(entry.is_sensitive())
                    .set_is_read_only(entry.is_read_only())
                    .build()
                    .unwrap();
                ConfigEntry::with_options(options)
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
        .describe_topics_with_topics(&[topic.to_string()], DescribeTopicsOptions::new())
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
            &[NewTopic::with_num_partitions_replication_factor(
                topic.to_string(),
                Some(num_partitions),
                Some(replication_factor),
            )],
            CreateTopicsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{} backend: create topic {topic}: {e}", admin.name()));
    all_of(&created).unwrap_or_else(|e| panic!("{} backend: create topic {topic}: {e}", admin.name()));

    wait_for_all_partitions_metadata(admin, topic, num_partitions as usize).await;
}

/// Commits `offsets` for `group_id`, retrying for as long as the coordinator
/// still answers `UNKNOWN_TOPIC_OR_PARTITION` because a freshly created topic
/// has not reached it, then asserts through [`all_of_exactly`] that the commit
/// reported an outcome for exactly the requested partitions.
///
/// # Why the retry, rather than a longer wait before the call
///
/// `KafkaApis.handleOffsetCommitRequest` validates every requested partition
/// against **the receiving broker's own metadata cache**, answering
/// `UNKNOWN_TOPIC_OR_PARTITION` when `getLeaderAndIsr` finds no such partition
/// there (`core/src/main/scala/kafka/server/KafkaApis.scala:313-335`). The
/// receiving broker is the group's *coordinator*, picked by hashing the group id
/// over `__consumer_offsets`, so on a multi-broker cluster it is usually not the
/// broker that answered the `describeTopics` inside [`create_topic`]. A topic
/// that has propagated to the broker the admin client happened to ask has not
/// necessarily propagated to the coordinator.
///
/// Java closes that window by reading **every** broker's metadata cache
/// directly: `TestUtils.waitForAllPartitionsMetadata` is
/// `brokers.forall { _.metadataCache.numPartitions(topic) == n }`
/// (`core/src/test/scala/unit/kafka/utils/TestUtils.scala:832-853`). A client
/// cannot reproduce that check — it cannot pin `describeTopics` to a broker of
/// its choosing, because the by-names `Call` is issued through
/// `NodeProvider::LeastLoaded` (`src/admin/kafka_admin_client.rs:4872-4875`),
/// which answers from whichever broker is least loaded — so **no** amount of
/// waiting before the call can establish the per-broker
/// precondition Java asserts, and waiting for a leader would not either
/// (`getLeaderAndIsr` is already present with `leader = -1`; its absence means
/// the partition is missing from that broker's image, not that an election is
/// pending). The client-observable equivalent is Java's other idiom for exactly
/// this problem, `TestUtils.retryOnExceptionWithTimeout`: re-run the operation
/// until it stops failing, bounded by the propagation bound.
///
/// The retry is sound because the failing attempt has no effect: when no
/// requested partition validates, `KafkaApis` completes the response itself and
/// never reaches the coordinator, so no group is created and no offset is stored
/// (`KafkaApis.scala:343-346`). Re-committing the same offsets is idempotent
/// regardless.
///
/// Only `UNKNOWN_TOPIC_OR_PARTITION` is retried. Any other failure panics out of
/// the loop on the first attempt — `retry_on_error_with_timeout` catches the
/// `Err` return, not panics — so this cannot turn a genuine backend defect into
/// a 60-second timeout, and [`all_of_exactly`]'s completeness check still runs
/// unweakened on the attempt that gets through.
pub async fn alter_consumer_group_offsets_awaiting_propagation<B: AdminBackend>(
    admin: &B,
    group_id: &str,
    offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
    what: &str,
) {
    let expected: Vec<TopicPartition> = offsets.keys().cloned().collect();
    let expected = expected.as_slice();
    retry_on_error_with_timeout(Duration::from_millis(TOPIC_METADATA_PROPAGATION_WAIT_MS), || async move {
        let backend = admin.name();
        let outcomes = admin
            .alter_consumer_group_offsets(group_id, offsets, AlterConsumerGroupOffsetsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: {what}: {e}"));
        if let Some(tp) = outcomes.iter().find_map(|(tp, outcome)| match outcome {
            Err(e) if e.error() == Errors::UnknownTopicOrPartition => Some(tp),
            _ => None,
        }) {
            return Err(format!(
                "{backend} backend: {what}: {tp} has not propagated to {group_id}'s coordinator yet"
            ));
        }
        all_of_exactly(admin, &outcomes, expected, what);
        Ok(())
    })
    .await;
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
