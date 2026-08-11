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

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, Config, ConfigEntry, ConfigSource, ConfigType, CreatePartitionsOptions,
    CreateTopicsOptions, CreateTopicsResult, DeleteRecordsOptions, DeleteTopicsOptions, DeletedRecords,
    DescribeTopicsOptions, ListTopicsOptions, MockAdminClient, NewPartitions, NewTopic, RecordsToDelete,
    TopicDescription, TopicListing, TopicMetadataAndConfig, new_admin_client,
};
use confluent_kafka::common::{KafkaError, KafkaFuture, TopicCollection, TopicPartition, Uuid};

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
