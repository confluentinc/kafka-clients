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

//! An in-memory [`Admin`] implementation for tests.
//!
//! Corresponds to `org.apache.kafka.clients.admin.MockAdminClient` (restricted
//! to the topic, cluster, and config methods that are in scope through Tier 1
//! Phase 3).

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use crate::admin::FilterResults;
use crate::admin::{
    AbortTransactionOptions, AbortTransactionResult, AbortTransactionSpec, DescribeProducersOptions,
    DescribeProducersResult, DescribeTransactionsOptions, DescribeTransactionsResult, FenceProducersOptions,
    FenceProducersResult, ListTransactionsOptions, ListTransactionsResult, PartitionProducerState,
    TerminateTransactionOptions, TerminateTransactionResult, TransactionDescription, TransactionListing,
};
use crate::admin::{
    Admin, AlterClientQuotasOptions, AlterClientQuotasResult, AlterConfigOp, AlterConfigsOptions, AlterConfigsResult,
    AlterConsumerGroupOffsetsOptions, AlterConsumerGroupOffsetsResult, AlterPartitionReassignmentsOptions,
    AlterPartitionReassignmentsResult, AlterReplicaLogDirsOptions, AlterReplicaLogDirsResult, ClassicGroupDescription,
    Config, ConfigEntry, ConsumerGroupDescription, CreateAclsOptions, CreateAclsResult, CreateDelegationTokenOptions,
    CreateDelegationTokenResult, CreatePartitionsOptions, CreatePartitionsResult, CreateTopicsOptions,
    CreateTopicsResult, DeleteAclsOptions, DeleteAclsResult, DeleteConsumerGroupOffsetsOptions,
    DeleteConsumerGroupOffsetsResult, DeleteConsumerGroupsOptions, DeleteConsumerGroupsResult, DeleteRecordsOptions,
    DeleteRecordsResult, DeleteTopicsOptions, DeleteTopicsResult, DeletedRecords, DescribeAclsOptions,
    DescribeAclsResult, DescribeClassicGroupsOptions, DescribeClassicGroupsResult, DescribeClientQuotasOptions,
    DescribeClientQuotasResult, DescribeClusterOptions, DescribeClusterResult, DescribeConfigsOptions,
    DescribeConfigsResult, DescribeConsumerGroupsOptions, DescribeConsumerGroupsResult, DescribeDelegationTokenOptions,
    DescribeDelegationTokenResult, DescribeFeaturesOptions, DescribeFeaturesResult, DescribeLogDirsOptions,
    DescribeLogDirsResult, DescribeReplicaLogDirsOptions, DescribeReplicaLogDirsResult, DescribeTopicsOptions,
    DescribeTopicsResult, ElectLeadersOptions, ElectLeadersResult, ExpireDelegationTokenOptions,
    ExpireDelegationTokenResult, FeatureMetadata, FeatureUpdate, FinalizedVersionRange, GroupListing, GroupOffsets,
    ListConfigResourcesOptions, ListConfigResourcesResult, ListConsumerGroupOffsetsOptions,
    ListConsumerGroupOffsetsResult, ListConsumerGroupOffsetsSpec, ListGroupsOptions, ListGroupsResult,
    ListOffsetsOptions, ListOffsetsResult, ListOffsetsResultInfo, ListPartitionReassignmentsOptions,
    ListPartitionReassignmentsResult, ListTopicsOptions, ListTopicsResult, LogDirDescription, NewPartitionReassignment,
    NewPartitions, NewTopic, OffsetSpec, OpType, PartitionReassignment, RecordsToDelete,
    RemoveMembersFromConsumerGroupOptions, RemoveMembersFromConsumerGroupResult, RenewDelegationTokenOptions,
    RenewDelegationTokenResult, ReplicaInfo, ReplicaLogDirInfo, SupportedVersionRange, TopicDescription, TopicListing,
    TopicMetadataAndConfig, UpdateFeaturesOptions, UpdateFeaturesResult, UpgradeType,
};
use crate::admin::{
    AlterUserScramCredentialsOptions, AlterUserScramCredentialsResult, DescribeUserScramCredentialsOptions,
    DescribeUserScramCredentialsResult, UserScramCredentialAlteration,
};
#[allow(deprecated)]
use crate::admin::{
    ClientMetricsResourceListing, ConsumerGroupListing, ListClientMetricsResourcesOptions,
    ListClientMetricsResourcesResult, ListConsumerGroupsOptions, ListConsumerGroupsResult,
};
use crate::common::ElectionType;
use crate::common::KafkaFuture;
use crate::common::acl::{AclBinding, AclBindingFilter, AclOperation};
use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::kafka_future::KafkaFutureImpl;
use crate::common::protocol::Errors;
use crate::common::quota::{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter};
use crate::common::requests::describe_log_dirs_response::UNKNOWN_VOLUME_BYTES;
use crate::common::security::auth::KafkaPrincipal;
use crate::common::security::token::delegation::{DelegationToken, TokenInformation};
use crate::common::utils::ProducerIdAndEpoch;
use crate::common::{Error, Node, TopicCollection, TopicPartition, TopicPartitionInfo, TopicPartitionReplica, Uuid};
use crate::common::{GroupState, GroupType};
use crate::consumer::OffsetAndMetadata;
use crate::consumer::internals::consumer_protocol::PROTOCOL_TYPE;
use crate::describe_user_scram_credentials_response_data::DescribeUserScramCredentialsResponseData;
use crate::leave_group_request_data::MemberIdentity;

use std::collections::{BTreeSet, HashSet};

/// Default cluster id used by the mock (matches Java's `DEFAULT_CLUSTER_ID`).
const DEFAULT_CLUSTER_ID: &str = "4A5xz_QZTB2CtL4wc0X0Jw";

/// The default log directories seeded per broker (mirrors Java's
/// `MockAdminClient.DEFAULT_LOG_DIRS`).
const DEFAULT_LOG_DIRS: &[&str] = &["/tmp/kafka-logs"];

/// Internal per-topic metadata held by the mock.
#[derive(Clone, Debug)]
struct TopicMetadata {
    topic_id: Uuid,
    is_internal: bool,
    partitions: Vec<TopicPartitionInfo>,
    // One log dir per partition (Java's `TopicMetadata.partitionLogDirs`),
    // taken from the first log dir of each partition's leader broker.
    partition_log_dirs: Vec<String>,
    // Read by `describe_configs` / `incremental_alter_configs`. Java's
    // `TopicMetadata.configs` is never null (defaults to an empty map); the
    // Rust `Option` treats `None` as an empty map.
    configs: Option<BTreeMap<String, String>>,
    marked_for_deletion: bool,
    fetches_remaining_until_visible: i32,
}

/// Mutable state, guarded by a mutex (mirrors Java's `synchronized` methods).
#[derive(Debug)]
struct State {
    brokers: Vec<Node>,
    #[allow(dead_code)]
    controller: Node,
    #[allow(dead_code)]
    cluster_id: String,
    all_topics: BTreeMap<String, TopicMetadata>,
    topic_ids: BTreeMap<String, Uuid>,
    topic_names: BTreeMap<Uuid, String>,
    default_partitions: i32,
    default_replication_factor: i16,
    timeout_next_requests: i32,
    // Per-broker config maps (index = broker id), mirroring Java's
    // `brokerConfigs`. Each is seeded with `default.replication.factor`.
    broker_configs: Vec<BTreeMap<String, String>>,
    // Client-metrics subscription configs, keyed by resource name.
    client_metrics_configs: BTreeMap<String, BTreeMap<String, String>>,
    // Group configs, keyed by group id.
    group_configs: BTreeMap<String, BTreeMap<String, String>>,
    // Defaults overlaid onto group configs on read (mirrors Java's
    // `defaultGroupConfigs`; empty for the `create(num_brokers)` builder).
    default_group_configs: BTreeMap<String, String>,
    // Per-broker list of log directories (index = broker id), mirroring Java's
    // `brokerLogDirs`. Seeded with `DEFAULT_LOG_DIRS` for each broker.
    broker_log_dirs: Vec<Vec<String>>,
    // Pending replica moves recorded by `alter_replica_log_dirs`, keyed by
    // replica (mirrors Java's `replicaMoves`).
    replica_moves: HashMap<TopicPartitionReplica, ReplicaLogDirInfo>,
    // Current partition reassignments, keyed by partition (mirrors Java's
    // `reassignments`).
    reassignments: HashMap<TopicPartition, NewPartitionReassignment>,
    // Per-partition beginning / end offsets seeded via `update_beginning_offsets`
    // / `update_end_offsets` (mirrors Java's `beginningOffsets` / `endOffsets`).
    beginning_offsets: HashMap<TopicPartition, i64>,
    end_offsets: HashMap<TopicPartition, i64>,
    // Committed consumer-group offsets seeded via `update_consumer_group_offsets`,
    // returned by `list_consumer_group_offsets` (mirrors Java's `committedOffsets`).
    committed_offsets: HashMap<TopicPartition, i64>,
    // In-memory delegation tokens (mirrors Java's `allTokens`).
    all_tokens: Vec<DelegationToken>,
    // Current finalized feature levels, keyed by feature name (mirrors Java's
    // `featureLevels`). Mutated by `update_features` unless `validate_only`.
    feature_levels: HashMap<String, i16>,
    // Minimum supported feature levels, keyed by feature name (mirrors Java's
    // `minSupportedFeatureLevels`).
    min_supported_feature_levels: HashMap<String, i16>,
    // Maximum supported feature levels, keyed by feature name (mirrors Java's
    // `maxSupportedFeatureLevels`).
    max_supported_feature_levels: HashMap<String, i16>,
}

/// An in-memory [`Admin`] implementation for tests.
///
/// Corresponds to `org.apache.kafka.clients.admin.MockAdminClient`. The topic,
/// cluster, and config methods are implemented through Tier 1 Phase 3; other
/// RPCs will be added with their tiers. All futures returned are immediately
/// resolved.
#[derive(Debug)]
pub struct MockAdminClient {
    state: Mutex<State>,
}

impl MockAdminClient {
    /// Creates a mock with `num_brokers` brokers (`localhost:1000+id`),
    /// controller = broker 0, default partitions 1 and default replication
    /// factor `min(num_brokers, 3)` — matching Java's `Builder` defaults.
    ///
    /// # Errors
    ///
    /// Returns [`Error::local_illegal_argument`] when `num_brokers` is less than
    /// one. Java throws instead of producing a broker-less mock: its
    /// `Builder.build()` reads `brokers.get(0)` for the controller
    /// (`MockAdminClient.java:210`), which raises `IndexOutOfBoundsException` on
    /// the empty list `numBrokers(0)` leaves behind, and `numBrokers(-1)` raises
    /// from `brokers.subList(0, -1)` (`:152`) before `build()` is even reached
    /// (kafka `a18251bae0`). Reaching either requires an explicit
    /// `numBrokers(n)` call — Java's `Builder()` constructor defaults to
    /// `numBrokers(1)` (`:136`) — so this rejects only a request the caller made
    /// deliberately.
    ///
    /// This is a `Result` rather than a `panic!` because the argument is a
    /// caller-supplied count that the caller can trivially correct, which is
    /// CLAUDE.md §10.2 ("return a `Result` when Java code throws … even if
    /// unchecked but recoverable") and not §10.1's unrecoverable case. It also
    /// keeps [`crate::ffi::admin`] free of a panic that could unwind out of
    /// `kafka_admin_MockAdminClient_new` and abort the process.
    pub fn create(num_brokers: i32) -> Result<Self, Error> {
        let brokers: Vec<Node> = (0..num_brokers)
            .map(|id| Node::new(id, "localhost".to_string(), 1000 + id))
            .collect();
        // Java's `controller == null ? brokers.get(0) : controller`
        // (`MockAdminClient.java:210`): the controller is always an element of
        // `brokers`, never a fabricated node absent from `nodes()`.
        let controller = match brokers.first() {
            Some(node) => node.clone(),
            None => {
                return Err(Error::local_illegal_argument(format!(
                    "num_brokers must be at least 1, was {num_brokers}"
                )));
            },
        };
        let default_replication_factor = num_brokers.clamp(0, 3) as i16;
        // Seed one config map per broker with `default.replication.factor`
        // (mirrors Java's constructor).
        let broker_configs: Vec<BTreeMap<String, String>> = (0..num_brokers)
            .map(|_| {
                let mut config = BTreeMap::new();
                config.insert("default.replication.factor".to_string(), default_replication_factor.to_string());
                config
            })
            .collect();
        Ok(Self {
            state: Mutex::new(State {
                brokers,
                controller,
                cluster_id: DEFAULT_CLUSTER_ID.to_string(),
                all_topics: BTreeMap::new(),
                topic_ids: BTreeMap::new(),
                topic_names: BTreeMap::new(),
                default_partitions: 1,
                default_replication_factor,
                timeout_next_requests: 0,
                broker_configs,
                client_metrics_configs: BTreeMap::new(),
                group_configs: BTreeMap::new(),
                default_group_configs: BTreeMap::new(),
                broker_log_dirs: (0..num_brokers)
                    .map(|_| DEFAULT_LOG_DIRS.iter().map(|s| (*s).to_string()).collect())
                    .collect(),
                replica_moves: HashMap::new(),
                reassignments: HashMap::new(),
                beginning_offsets: HashMap::new(),
                end_offsets: HashMap::new(),
                committed_offsets: HashMap::new(),
                all_tokens: Vec::new(),
                feature_levels: HashMap::new(),
                min_supported_feature_levels: HashMap::new(),
                max_supported_feature_levels: HashMap::new(),
            }),
        })
    }

    /// Seeds the finalized feature levels, along with the minimum and maximum
    /// supported feature levels, returned by `describe_features` and consulted
    /// by `update_features`.
    ///
    /// Mirrors Java's `MockAdminClient.Builder.featureLevels` /
    /// `minSupportedFeatureLevels` / `maxSupportedFeatureLevels`.
    pub fn set_feature_levels(
        &self,
        feature_levels: HashMap<String, i16>,
        min_supported_feature_levels: HashMap<String, i16>,
        max_supported_feature_levels: HashMap<String, i16>,
    ) {
        let mut state = self.state.lock().unwrap();
        state.feature_levels = feature_levels;
        state.min_supported_feature_levels = min_supported_feature_levels;
        state.max_supported_feature_levels = max_supported_feature_levels;
    }

    // --- seeding mutators ---------------------------------------------------
    //
    // Convention for this group: a seeding helper whose Java counterpart is a
    // `void` method that *throws* returns `Result<(), Error>` and reuses
    // Java's message verbatim, rather than panicking. Java's throws here are all
    // catchable `IllegalArgumentException`s, and CLAUDE.md §10.2 asks for a
    // `Result` for a recoverable Java throw even when it is unchecked. A helper
    // whose Java counterpart cannot fail (`updateBeginningOffsets`,
    // `updateEndOffsets`, `updateConsumerGroupOffsets`, `timeoutNextRequest`)
    // stays infallible.

    /// Seeds the beginning offsets returned by `list_offsets` for the given
    /// partitions.
    ///
    /// Mirrors `MockAdminClient.updateBeginningOffsets`.
    pub fn update_beginning_offsets(&self, new_offsets: HashMap<TopicPartition, i64>) {
        let mut state = self.state.lock().unwrap();
        state.beginning_offsets.extend(new_offsets);
    }

    /// Seeds the end offsets returned by `list_offsets` for the given
    /// partitions.
    ///
    /// Mirrors `MockAdminClient.updateEndOffsets`.
    pub fn update_end_offsets(&self, new_offsets: HashMap<TopicPartition, i64>) {
        let mut state = self.state.lock().unwrap();
        state.end_offsets.extend(new_offsets);
    }

    /// Seeds the committed consumer-group offsets returned by
    /// `list_consumer_group_offsets` for the given partitions.
    ///
    /// Mirrors `MockAdminClient.updateConsumerGroupOffsets`.
    pub fn update_consumer_group_offsets(&self, new_offsets: HashMap<TopicPartition, i64>) {
        let mut state = self.state.lock().unwrap();
        state.committed_offsets.extend(new_offsets);
    }

    /// Overrides the log directories for a broker (mirrors Java's
    /// `Builder.brokerLogDirs`). Useful for exercising multi-log-dir replica
    /// moves in tests.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if `broker_id` is not one of the
    /// mock's brokers. Java has no equivalent setter — `Builder.brokerLogDirs`
    /// installs the whole list up front — so this message has no Java
    /// counterpart; it exists because indexing `brokerLogDirs` out of range
    /// would otherwise panic.
    pub fn set_broker_log_dirs(&self, broker_id: i32, log_dirs: Vec<String>) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        let slot = usize::try_from(broker_id)
            .ok()
            .and_then(|id| state.broker_log_dirs.get_mut(id))
            .ok_or_else(|| Error::local_illegal_argument(format!("Broker {broker_id} does not exist.")))?;
        *slot = log_dirs;
        Ok(())
    }

    /// Adds an existing topic to the mock's state.
    ///
    /// Mirrors `MockAdminClient.addTopic`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] with Java's message if the topic
    /// was already added, or if any partition names a broker the mock does not
    /// have as its leader, in its replica list, or in its ISR
    /// (`MockAdminClient.java:296-309`).
    pub fn add_topic(
        &self,
        internal: bool,
        name: &str,
        partitions: Vec<TopicPartitionInfo>,
        configs: Option<BTreeMap<String, String>>,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        if state.all_topics.contains_key(name) {
            return Err(Error::local_illegal_argument(format!("Topic {name} was already added.")));
        }
        // Java validates every partition against the broker list before touching
        // `brokerLogDirs` (MockAdminClient.java:298-309). Note that
        // `brokers.contains(partition.leader())` is false for a null leader, so
        // Java rejects a leaderless partition here too — which is why the log-dir
        // loop's `partition.leader() != null` guard below can never fail once
        // these checks have passed.
        for partition in &partitions {
            let known_leader = partition.leader().is_some_and(|leader| state.brokers.contains(leader));
            if !known_leader {
                return Err(Error::local_illegal_argument("Leader broker unknown"));
            }
            if !partition.replicas().iter().all(|node| state.brokers.contains(node)) {
                return Err(Error::local_illegal_argument("Unknown brokers in replica list"));
            }
            if !partition.isr().iter().all(|node| state.brokers.contains(node)) {
                return Err(Error::local_illegal_argument("Unknown brokers in isr list"));
            }
        }
        // Each partition starts on the first log directory of its leader broker.
        // Indexing `broker_log_dirs` by the leader id is sound because the check
        // above established that the leader is one of `state.brokers`, whose ids
        // are exactly `0..broker_log_dirs.len()`. The *inner* `get(0)` is not
        // established by anything — Java's `brokerLogDirs.get(id).get(0)`
        // (MockAdminClient.java:312-314) throws `IndexOutOfBoundsException` for a
        // broker configured with no log directory — so it is surfaced as an error
        // rather than panicked on (CLAUDE.md §10.2).
        let mut partition_log_dirs: Vec<String> = Vec::with_capacity(partitions.len());
        for leader in partitions.iter().filter_map(TopicPartitionInfo::leader) {
            let dirs = &state.broker_log_dirs[leader.id() as usize];
            let first = dirs.first().ok_or_else(|| {
                Error::local_illegal_argument(format!("Broker {} has no log directories.", leader.id()))
            })?;
            partition_log_dirs.push(first.clone());
        }
        let topic_id = Uuid::random_uuid();
        state.topic_ids.insert(name.to_string(), topic_id);
        state.topic_names.insert(topic_id, name.to_string());
        state.all_topics.insert(
            name.to_string(),
            TopicMetadata {
                topic_id,
                is_internal: internal,
                partitions,
                partition_log_dirs,
                configs,
                marked_for_deletion: false,
                fetches_remaining_until_visible: 0,
            },
        );
        Ok(())
    }

    /// Marks a topic for deletion so `describe_topics` treats it as absent.
    ///
    /// Mirrors `MockAdminClient.markTopicForDeletion`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] with Java's message if the topic
    /// does not exist (`MockAdminClient.java:328-330`).
    pub fn mark_topic_for_deletion(&self, name: &str) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        let topic = state
            .all_topics
            .get_mut(name)
            .ok_or_else(|| Error::local_illegal_argument(format!("Topic {name} did not exist.")))?;
        topic.marked_for_deletion = true;
        Ok(())
    }

    /// Causes the next `number_of_requests` operations to fail with a timeout.
    pub fn timeout_next_request(&self, number_of_requests: i32) {
        self.state.lock().unwrap().timeout_next_requests = number_of_requests;
    }
}

fn timeout_error() -> Error {
    Error::timeout("The mock timed out the request.".to_string())
}

/// Current wall-clock time in milliseconds since the Unix epoch, mirroring
/// Java's `System.currentTimeMillis()` used by the mock's delegation-token
/// methods.
fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as i64
}

/// Computes the `PartitionReassignment` for a partition from the mock's stored
/// reassignments and topic metadata.
///
/// Mirrors `MockAdminClient.findPartitionReassignment`
/// (`MockAdminClient.java:1182-1210`). Returns `Ok(None)` if there is no stored
/// reassignment for the partition.
///
/// # Errors
///
/// Returns an error when a stored reassignment references a partition with no
/// metadata. Java throws a bare `RuntimeException` from both of these branches;
/// since `list_partition_reassignments` cannot throw, the caller fails the
/// result's future instead (CLAUDE.md §10.1 — a panic here would unwind into C
/// through the FFI). This is *not* only reachable through internal corruption:
/// `delete_topics` removes the topic from `all_topics` without pruning
/// `reassignments`, exactly as Java's does, so any legal
/// alter-reassignment/delete-topic/list-reassignments sequence reaches it.
fn find_partition_reassignment(
    state: &State,
    partition: &TopicPartition,
) -> Result<Option<PartitionReassignment>, Error> {
    let Some(reassignment) = state.reassignments.get(partition) else {
        return Ok(None);
    };
    let metadata = state.all_topics.get(partition.topic()).ok_or_else(|| {
        Error::local_illegal_state(format!(
            "Internal MockAdminClient logic error: found reassignment for {partition}, but no TopicMetadata"
        ))
    })?;
    let info = metadata.partitions.get(partition.partition() as usize).ok_or_else(|| {
        Error::local_illegal_state(format!(
            "Internal MockAdminClient logic error: found reassignment for {partition}, but no TopicPartitionInfo"
        ))
    })?;
    let target_replicas = reassignment.target_replicas();
    let mut replicas = Vec::new();
    let mut removing_replicas = Vec::new();
    let mut adding_replicas: Vec<i32> = target_replicas.to_vec();
    for node in info.replicas() {
        replicas.push(node.id());
        if !target_replicas.contains(&node.id()) {
            removing_replicas.push(node.id());
        }
        if let Some(pos) = adding_replicas.iter().position(|&id| id == node.id()) {
            adding_replicas.remove(pos);
        }
    }
    Ok(Some(PartitionReassignment::new(replicas, adding_replicas, removing_replicas)))
}

fn config_from_new_topic(new_topic: &NewTopic) -> Config {
    let entries = new_topic
        .config_map()
        .map(|configs| {
            configs
                .iter()
                .map(|(k, v)| ConfigEntry::new(k.clone(), Some(v.clone())))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Config::new(entries)
}

/// Builds a [`Config`] from an in-memory config map.
///
/// Corresponds to `MockAdminClient.toConfigObject`.
fn to_config_object(map: &BTreeMap<String, String>) -> Config {
    let entries = map
        .iter()
        .map(|(k, v)| ConfigEntry::new(k.clone(), Some(v.clone())))
        .collect::<Vec<_>>();
    Config::new(entries)
}

/// Applies a sequence of [`AlterConfigOp`]s to an in-memory config map.
///
/// Returns an error for an unsupported op type (mirrors Java's
/// `InvalidRequestException`). `Append` / `Subtract` are list-type operations
/// that Java's mock does not implement, matching its `default` branch.
fn apply_alter_ops(map: &mut BTreeMap<String, String>, ops: &[AlterConfigOp]) -> Result<(), Error> {
    for op in ops {
        match op.op_type() {
            OpType::Set => {
                map.insert(
                    op.config_entry().name().to_string(),
                    op.config_entry().value().unwrap_or_default().to_string(),
                );
            },
            OpType::Delete => {
                map.remove(op.config_entry().name());
            },
            other => {
                return Err(Error::with_message(
                    Errors::InvalidRequest,
                    format!("Unsupported op type {other:?}"),
                ));
            },
        }
    }
    Ok(())
}

/// Reads the config description for a single resource.
///
/// Corresponds to `MockAdminClient.getResourceDescription`.
fn get_resource_description(state: &mut State, resource: &ConfigResource) -> Result<Config, Error> {
    match resource.resource_type() {
        ConfigResourceType::Broker => {
            let broker_id: usize = resource.name().parse().map_err(|_| {
                Error::with_message(Errors::InvalidRequest, format!("Broker {} not found.", resource.name()))
            })?;
            match state.broker_configs.get(broker_id) {
                Some(config) => Ok(to_config_object(config)),
                None => Err(Error::with_message(
                    Errors::InvalidRequest,
                    format!("Broker {} not found.", resource.name()),
                )),
            }
        },
        ConfigResourceType::Topic => {
            if let Some(metadata) = state.all_topics.get_mut(resource.name())
                && !metadata.marked_for_deletion
            {
                if metadata.fetches_remaining_until_visible > 0 {
                    metadata.fetches_remaining_until_visible = (metadata.fetches_remaining_until_visible - 1).max(0);
                } else {
                    let config = metadata.configs.clone().unwrap_or_default();
                    return Ok(to_config_object(&config));
                }
            }
            Err(Error::with_message(
                Errors::UnknownTopicOrPartition,
                format!("Resource {resource} not found."),
            ))
        },
        ConfigResourceType::ClientMetrics => {
            let resource_name = resource.name();
            if resource_name.is_empty() {
                return Err(Error::with_message(Errors::InvalidRequest, "Empty resource name"));
            }
            let config = state.client_metrics_configs.get(resource_name).cloned().unwrap_or_default();
            Ok(to_config_object(&config))
        },
        ConfigResourceType::Group => {
            let resource_name = resource.name();
            if resource_name.is_empty() {
                return Err(Error::with_message(Errors::InvalidRequest, "Empty resource name"));
            }
            let mut group_config = state.group_configs.get(resource_name).cloned().unwrap_or_default();
            // Overlay defaults for keys not already present (Java's `putIfAbsent`).
            for (k, v) in &state.default_group_configs {
                group_config.entry(k.clone()).or_insert_with(|| v.clone());
            }
            Ok(to_config_object(&group_config))
        },
        _ => Err(Error::unsupported_version("Not implemented yet")),
    }
}

/// Applies an incremental config alteration to a single resource.
///
/// Corresponds to `MockAdminClient.handleIncrementalResourceAlteration`.
fn handle_incremental_resource_alteration(
    state: &mut State,
    resource: &ConfigResource,
    ops: &[AlterConfigOp],
) -> Result<(), Error> {
    match resource.resource_type() {
        ConfigResourceType::Broker => {
            let broker_id: usize = resource.name().parse().map_err(|_| {
                Error::with_message(Errors::InvalidRequest, format!("no such broker as {}", resource.name()))
            })?;
            if broker_id >= state.broker_configs.len() {
                return Err(Error::with_message(
                    Errors::InvalidRequest,
                    format!("no such broker as {broker_id}"),
                ));
            }
            let mut new_map = state.broker_configs[broker_id].clone();
            apply_alter_ops(&mut new_map, ops)?;
            state.broker_configs[broker_id] = new_map;
            Ok(())
        },
        ConfigResourceType::Topic => {
            let metadata = state.all_topics.get_mut(resource.name()).ok_or_else(|| {
                Error::with_message(Errors::UnknownTopicOrPartition, format!("No such topic as {}", resource.name()))
            })?;
            let mut new_map = metadata.configs.clone().unwrap_or_default();
            apply_alter_ops(&mut new_map, ops)?;
            metadata.configs = Some(new_map);
            Ok(())
        },
        ConfigResourceType::ClientMetrics => {
            let resource_name = resource.name();
            if resource_name.is_empty() {
                return Err(Error::with_message(Errors::InvalidRequest, "Empty resource name"));
            }
            let mut new_map = state.client_metrics_configs.get(resource_name).cloned().unwrap_or_default();
            apply_alter_ops(&mut new_map, ops)?;
            state.client_metrics_configs.insert(resource_name.to_string(), new_map);
            Ok(())
        },
        ConfigResourceType::Group => {
            let resource_name = resource.name();
            if resource_name.is_empty() {
                return Err(Error::with_message(Errors::InvalidRequest, "Empty resource name"));
            }
            let mut new_map = state.group_configs.get(resource_name).cloned().unwrap_or_default();
            apply_alter_ops(&mut new_map, ops)?;
            state.group_configs.insert(resource_name.to_string(), new_map);
            Ok(())
        },
        _ => Err(Error::unsupported_version("Not implemented yet")),
    }
}

#[async_trait]
impl Admin for MockAdminClient {
    fn create_topics(&self, new_topics: &[NewTopic], _options: CreateTopicsOptions) -> CreateTopicsResult {
        let mut state = self.state.lock().unwrap();
        let mut result: HashMap<String, crate::common::KafkaFuture<TopicMetadataAndConfig>> = HashMap::new();

        if state.timeout_next_requests > 0 {
            for new_topic in new_topics {
                let handle: KafkaFutureImpl<TopicMetadataAndConfig> = KafkaFutureImpl::new();
                handle.complete_with_error(timeout_error());
                result.insert(new_topic.name().to_string(), handle.future());
            }
            state.timeout_next_requests -= 1;
            return CreateTopicsResult::new(result);
        }

        for new_topic in new_topics {
            let handle: KafkaFutureImpl<TopicMetadataAndConfig> = KafkaFutureImpl::new();
            let topic_name = new_topic.name().to_string();

            if state.all_topics.contains_key(&topic_name) {
                handle.complete_with_error(Error::with_message(
                    Errors::TopicAlreadyExists,
                    format!("Topic {topic_name} exists already."),
                ));
                result.insert(topic_name, handle.future());
                continue;
            }

            let mut replication_factor = new_topic.replication_factor();
            if replication_factor == -1 {
                replication_factor = state.default_replication_factor;
            }
            if replication_factor as usize > state.brokers.len() {
                handle.complete_with_error(Error::with_message(
                    Errors::InvalidReplicationFactor,
                    format!(
                        "Replication factor: {} is larger than brokers: {}",
                        new_topic.replication_factor(),
                        state.brokers.len()
                    ),
                ));
                result.insert(topic_name, handle.future());
                continue;
            }

            let replicas: Vec<Node> = state.brokers[..replication_factor as usize].to_vec();
            let mut number_of_partitions = new_topic.num_partitions();
            if number_of_partitions == -1 {
                number_of_partitions = state.default_partitions;
            }
            let leader = state.brokers[0].clone();
            // Every partition of this topic shares the same leader (above), so
            // check its log directories once, before building `partitions`.
            // Java's `brokerLogDirs.get(id).get(0)` (MockAdminClient.java:413)
            // throws an unchecked `IndexOutOfBoundsException` for a broker with
            // no log directories; `add_topic` already translates the identical
            // situation as a per-topic error instead of a panic (CLAUDE.md
            // §10.2), so `create_topics` gets the same treatment here.
            if state.broker_log_dirs[leader.id() as usize].is_empty() {
                handle.complete_with_error(Error::local_illegal_argument(format!(
                    "Broker {} has no log directories.",
                    leader.id()
                )));
                result.insert(topic_name, handle.future());
                continue;
            }
            let partitions: Vec<TopicPartitionInfo> = (0..number_of_partitions)
                .map(|i| {
                    TopicPartitionInfo::new(
                        i,
                        Some(leader.clone()),
                        replicas.clone(),
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                    )
                })
                .collect();
            // Partitions start off on the first log directory of each broker.
            let partition_log_dirs: Vec<String> = partitions
                .iter()
                .filter_map(|p| p.leader().map(|l| state.broker_log_dirs[l.id() as usize][0].clone()))
                .collect();

            let topic_id = Uuid::random_uuid();
            state.topic_ids.insert(topic_name.clone(), topic_id);
            state.topic_names.insert(topic_id, topic_name.clone());
            state.all_topics.insert(
                topic_name.clone(),
                TopicMetadata {
                    topic_id,
                    is_internal: false,
                    partitions,
                    partition_log_dirs,
                    configs: new_topic.config_map().cloned(),
                    marked_for_deletion: false,
                    fetches_remaining_until_visible: 0,
                },
            );
            handle.complete(TopicMetadataAndConfig::new(
                topic_id,
                number_of_partitions,
                replication_factor as i32,
                config_from_new_topic(new_topic),
            ));
            result.insert(topic_name, handle.future());
        }

        CreateTopicsResult::new(result)
    }

    fn delete_topics(&self, topics: TopicCollection, _options: DeleteTopicsOptions) -> DeleteTopicsResult {
        let mut state = self.state.lock().unwrap();
        match topics {
            TopicCollection::TopicNames(names) => {
                let mut result = HashMap::new();
                for name in names {
                    let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
                    if state.timeout_next_requests > 0 {
                        handle.complete_with_error(timeout_error());
                    } else if state.all_topics.remove(&name).is_none() {
                        handle.complete_with_error(Error::with_message(
                            Errors::UnknownTopicOrPartition,
                            format!("Topic {name} does not exist."),
                        ));
                    } else {
                        if let Some(id) = state.topic_ids.remove(&name) {
                            state.topic_names.remove(&id);
                        }
                        handle.complete(());
                    }
                    result.insert(name, handle.future());
                }
                if state.timeout_next_requests > 0 {
                    state.timeout_next_requests -= 1;
                }
                DeleteTopicsResult::of_topic_names(result)
            },
            TopicCollection::TopicIds(ids) => {
                let mut result = HashMap::new();
                for id in ids {
                    let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
                    if state.timeout_next_requests > 0 {
                        handle.complete_with_error(timeout_error());
                    } else {
                        let name = state.topic_names.remove(&id);
                        let removed = name.as_ref().is_some_and(|n| state.all_topics.remove(n).is_some());
                        if !removed {
                            handle.complete_with_error(Error::with_message(
                                Errors::UnknownTopicOrPartition,
                                format!("Topic {id} does not exist."),
                            ));
                        } else {
                            if let Some(n) = name {
                                state.topic_ids.remove(&n);
                            }
                            handle.complete(());
                        }
                    }
                    result.insert(id, handle.future());
                }
                if state.timeout_next_requests > 0 {
                    state.timeout_next_requests -= 1;
                }
                DeleteTopicsResult::of_topic_ids(result)
            },
        }
    }

    fn list_topics(&self, _options: ListTopicsOptions) -> ListTopicsResult {
        let mut state = self.state.lock().unwrap();
        let handle: KafkaFutureImpl<HashMap<String, TopicListing>> = KafkaFutureImpl::new();

        if state.timeout_next_requests > 0 {
            handle.complete_with_error(timeout_error());
            state.timeout_next_requests -= 1;
            return ListTopicsResult::new(handle.future());
        }

        let mut listings = HashMap::new();
        for (name, metadata) in state.all_topics.iter_mut() {
            if metadata.fetches_remaining_until_visible > 0 {
                metadata.fetches_remaining_until_visible -= 1;
            } else {
                listings.insert(
                    name.clone(),
                    TopicListing::new(name.clone(), metadata.topic_id, metadata.is_internal),
                );
            }
        }
        handle.complete(listings);
        ListTopicsResult::new(handle.future())
    }

    fn describe_topics(&self, topics: TopicCollection, _options: DescribeTopicsOptions) -> DescribeTopicsResult {
        let mut state = self.state.lock().unwrap();
        match topics {
            TopicCollection::TopicNames(names) => {
                let mut result = HashMap::new();
                let timing_out = state.timeout_next_requests > 0;
                for requested in &names {
                    let handle: KafkaFutureImpl<TopicDescription> = KafkaFutureImpl::new();
                    if timing_out {
                        handle.complete_with_error(timeout_error());
                        result.insert(requested.clone(), handle.future());
                        continue;
                    }
                    match state.all_topics.get(requested) {
                        Some(metadata) if !metadata.marked_for_deletion => {
                            handle.complete(TopicDescription::with_authorized_operations(
                                requested.clone(),
                                metadata.is_internal,
                                metadata.partitions.clone(),
                                // Java's mock passes `Collections.emptySet()` here, i.e. a
                                // reported-but-empty set rather than null.
                                Some(std::collections::BTreeSet::new()),
                                metadata.topic_id,
                            ));
                        },
                        _ => {
                            handle.complete_with_error(Error::with_message(
                                Errors::UnknownTopicOrPartition,
                                format!("Topic {requested} not found."),
                            ));
                        },
                    }
                    result.insert(requested.clone(), handle.future());
                }
                if timing_out {
                    state.timeout_next_requests -= 1;
                }
                DescribeTopicsResult::of_topic_names(result)
            },
            TopicCollection::TopicIds(ids) => {
                let mut result = HashMap::new();
                let timing_out = state.timeout_next_requests > 0;
                for requested in &ids {
                    let handle: KafkaFutureImpl<TopicDescription> = KafkaFutureImpl::new();
                    if timing_out {
                        handle.complete_with_error(timeout_error());
                        result.insert(*requested, handle.future());
                        continue;
                    }
                    let found = state
                        .topic_names
                        .get(requested)
                        .and_then(|name| state.all_topics.get(name).map(|m| (name.clone(), m)))
                        .filter(|(_, m)| !m.marked_for_deletion);
                    match found {
                        Some((name, metadata)) => {
                            handle.complete(TopicDescription::with_authorized_operations(
                                name,
                                metadata.is_internal,
                                metadata.partitions.clone(),
                                // Java's mock passes `Collections.emptySet()` here, i.e. a
                                // reported-but-empty set rather than null.
                                Some(std::collections::BTreeSet::new()),
                                *requested,
                            ));
                        },
                        None => {
                            handle.complete_with_error(Error::with_message(
                                Errors::UnknownTopicId,
                                format!("Topic id {requested} not found."),
                            ));
                        },
                    }
                    result.insert(*requested, handle.future());
                }
                if timing_out {
                    state.timeout_next_requests -= 1;
                }
                DescribeTopicsResult::of_topic_ids(result)
            },
        }
    }

    fn create_partitions(
        &self,
        new_partitions: &HashMap<String, NewPartitions>,
        _options: CreatePartitionsOptions,
    ) -> CreatePartitionsResult {
        // Java's `MockAdminClient.createPartitions` (MockAdminClient.java:626-628)
        // throws `UnsupportedOperationException("Not implemented yet")`. Per
        // `.claude/rules/admin-client.md` §9 the Rust mock returns an
        // "unsupported" `Error` per key instead of panicking (faithful
        // translation of the Java behavior).
        let mut result = HashMap::new();
        for topic in new_partitions.keys() {
            let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            result.insert(topic.clone(), handle.future());
        }
        CreatePartitionsResult::new(result)
    }

    fn delete_records(
        &self,
        records_to_delete: &HashMap<TopicPartition, RecordsToDelete>,
        _options: DeleteRecordsOptions,
    ) -> DeleteRecordsResult {
        // Java's `MockAdminClient.deleteRecords` (MockAdminClient.java:631-638)
        // returns an empty result for an empty request and otherwise throws
        // `UnsupportedOperationException("Not implemented yet")`. Per
        // `.claude/rules/admin-client.md` §9 the non-empty case returns an
        // "unsupported" `Error` per key instead of panicking (faithful
        // translation of the Java behavior).
        let mut result = HashMap::new();
        for topic_partition in records_to_delete.keys() {
            let handle: KafkaFutureImpl<DeletedRecords> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            result.insert(topic_partition.clone(), handle.future());
        }
        DeleteRecordsResult::new(result)
    }

    fn describe_producers(
        &self,
        partitions: &[TopicPartition],
        _options: DescribeProducersOptions,
    ) -> DescribeProducersResult {
        // Java's `MockAdminClient.describeProducers` (MockAdminClient.java:1368-1370)
        // throws `UnsupportedOperationException("Not implemented yet")`. Per
        // `.claude/rules/admin-client.md` §9 the Rust mock returns an
        // "unsupported" `Error` per key instead of panicking (faithful
        // translation of the Java behavior).
        let mut result = HashMap::new();
        for topic_partition in partitions {
            let handle: KafkaFutureImpl<PartitionProducerState> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            result.insert(topic_partition.clone(), handle.future());
        }
        DescribeProducersResult::new(result)
    }

    fn abort_transaction(
        &self,
        spec: AbortTransactionSpec,
        _options: AbortTransactionOptions,
    ) -> AbortTransactionResult {
        // Java's `MockAdminClient.abortTransaction` (MockAdminClient.java:1378-1381)
        // throws `UnsupportedOperationException("Not implemented yet")`. Per
        // `.claude/rules/admin-client.md` §9 the Rust mock returns an
        // "unsupported" `Error` per key instead of panicking (faithful
        // translation of the Java behavior).
        let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
        AbortTransactionResult::new(HashMap::from([(spec.topic_partition().clone(), handle.future())]))
    }

    fn describe_transactions(
        &self,
        transactional_ids: &[String],
        _options: DescribeTransactionsOptions,
    ) -> DescribeTransactionsResult {
        // Java's `MockAdminClient.describeTransactions` (MockAdminClient.java:1373-1375)
        // throws `UnsupportedOperationException("Not implemented yet")`. Per
        // `.claude/rules/admin-client.md` §9 the Rust mock returns an
        // "unsupported" `Error` per key instead of panicking (faithful
        // translation of the Java behavior).
        let mut result = HashMap::new();
        for id in transactional_ids {
            let handle: KafkaFutureImpl<TransactionDescription> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            result.insert(id.clone(), handle.future());
        }
        DescribeTransactionsResult::new(result)
    }

    fn fence_producers(&self, transactional_ids: &[String], _options: FenceProducersOptions) -> FenceProducersResult {
        // Java's `MockAdminClient.fenceProducers` (MockAdminClient.java:1393-1395)
        // throws `UnsupportedOperationException("Not implemented yet")`. Per
        // `.claude/rules/admin-client.md` §9 the Rust mock returns an
        // "unsupported" `Error` per key instead of panicking (faithful
        // translation of the Java behavior).
        let mut result = HashMap::new();
        for id in transactional_ids {
            let handle: KafkaFutureImpl<ProducerIdAndEpoch> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            result.insert(id.clone(), handle.future());
        }
        FenceProducersResult::new(result)
    }

    fn list_transactions(&self, _options: ListTransactionsOptions) -> ListTransactionsResult {
        // Java's `MockAdminClient.listTransactions` (MockAdminClient.java:1388-1390)
        // throws `UnsupportedOperationException("Not implemented yet")`. Per
        // `.claude/rules/admin-client.md` §9 the Rust mock completes the
        // top-level future exceptionally instead of panicking.
        let handle: KafkaFutureImpl<HashMap<i32, KafkaFuture<Vec<TransactionListing>>>> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
        ListTransactionsResult::new(handle.future())
    }

    fn force_terminate_transaction(
        &self,
        transactional_id: &str,
        options: TerminateTransactionOptions,
    ) -> TerminateTransactionResult {
        // Java's `MockAdminClient.forceTerminateTransaction` throws
        // `UnsupportedOperationException("Not implemented yet")` directly
        // (MockAdminClient.java:1383-1386) — it does *not* delegate. It is the
        // production `KafkaAdminClient.forceTerminateTransaction` that delegates
        // ("Simply leverage the existing fenceProducers implementation",
        // KafkaAdminClient.java:4848-4864). The Rust mock mirrors that production
        // delegation, which lands in the mock's own `fence_producers` — itself
        // "Not implemented yet" — so the resulting future carries the same
        // "unsupported" error Java's mock throws, by a different route.
        let mut fence_options = FenceProducersOptions::new();
        if options.timeout_ms().is_some() {
            fence_options = fence_options.set_timeout_ms(options.timeout_ms());
        }
        let ids = vec![transactional_id.to_string()];
        let fence_result = self.fence_producers(&ids, fence_options);
        let future = fence_result
            .fenced_producers()
            .get(transactional_id)
            .cloned()
            .expect("the transactional id was included in the fenceProducers request");
        TerminateTransactionResult::new(future)
    }

    fn describe_cluster(&self, _options: DescribeClusterOptions) -> DescribeClusterResult {
        let mut state = self.state.lock().unwrap();
        let nodes: KafkaFutureImpl<Vec<Node>> = KafkaFutureImpl::new();
        let controller: KafkaFutureImpl<Option<Node>> = KafkaFutureImpl::new();
        let cluster_id: KafkaFutureImpl<String> = KafkaFutureImpl::new();
        let authorized_operations: KafkaFutureImpl<Option<BTreeSet<AclOperation>>> = KafkaFutureImpl::new();

        if state.timeout_next_requests > 0 {
            let err = timeout_error();
            nodes.complete_with_error(err.clone());
            controller.complete_with_error(err.clone());
            cluster_id.complete_with_error(err.clone());
            authorized_operations.complete_with_error(err);
            state.timeout_next_requests -= 1;
        } else {
            nodes.complete(state.brokers.clone());
            controller.complete(Some(state.controller.clone()));
            cluster_id.complete(state.cluster_id.clone());
            // Java completes with an empty set (not null).
            authorized_operations.complete(Some(BTreeSet::new()));
        }
        DescribeClusterResult::new(
            nodes.future(),
            controller.future(),
            cluster_id.future(),
            authorized_operations.future(),
        )
    }

    fn describe_configs(
        &self,
        config_resources: &[ConfigResource],
        _options: DescribeConfigsOptions,
    ) -> DescribeConfigsResult {
        let mut state = self.state.lock().unwrap();

        if state.timeout_next_requests > 0 {
            let mut result = HashMap::new();
            for resource in config_resources {
                let handle: KafkaFutureImpl<Config> = KafkaFutureImpl::new();
                handle.complete_with_error(timeout_error());
                result.insert(resource.clone(), handle.future());
            }
            state.timeout_next_requests -= 1;
            return DescribeConfigsResult::new(result);
        }

        let mut result = HashMap::new();
        for resource in config_resources {
            let handle: KafkaFutureImpl<Config> = KafkaFutureImpl::new();
            match get_resource_description(&mut state, resource) {
                Ok(config) => handle.complete(config),
                Err(e) => handle.complete_with_error(e),
            };
            result.insert(resource.clone(), handle.future());
        }
        DescribeConfigsResult::new(result)
    }

    fn incremental_alter_configs(
        &self,
        configs: &HashMap<ConfigResource, Vec<AlterConfigOp>>,
        _options: AlterConfigsOptions,
    ) -> AlterConfigsResult {
        let mut state = self.state.lock().unwrap();
        let mut result = HashMap::new();
        for (resource, ops) in configs {
            let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            match handle_incremental_resource_alteration(&mut state, resource, ops) {
                Ok(()) => handle.complete(()),
                Err(e) => handle.complete_with_error(e),
            };
            result.insert(resource.clone(), handle.future());
        }
        AlterConfigsResult::new(result)
    }

    fn list_config_resources(
        &self,
        config_resource_types: &HashSet<ConfigResourceType>,
        _options: ListConfigResourcesOptions,
    ) -> ListConfigResourcesResult {
        let state = self.state.lock().unwrap();
        let handle: KafkaFutureImpl<Vec<ConfigResource>> = KafkaFutureImpl::new();
        // Collect into a set to de-duplicate, mirroring Java's `HashSet`.
        let mut config_resources: HashSet<ConfigResource> = HashSet::new();
        let all = config_resource_types.is_empty();

        if all || config_resource_types.contains(&ConfigResourceType::Topic) {
            for name in state.all_topics.keys() {
                config_resources.insert(ConfigResource::new(ConfigResourceType::Topic, name.clone()));
            }
        }
        if all || config_resource_types.contains(&ConfigResourceType::Broker) {
            for i in 0..state.brokers.len() {
                config_resources.insert(ConfigResource::new(ConfigResourceType::Broker, i.to_string()));
            }
        }
        if all || config_resource_types.contains(&ConfigResourceType::BrokerLogger) {
            for i in 0..state.brokers.len() {
                config_resources.insert(ConfigResource::new(ConfigResourceType::BrokerLogger, i.to_string()));
            }
        }
        if all || config_resource_types.contains(&ConfigResourceType::ClientMetrics) {
            for name in state.client_metrics_configs.keys() {
                config_resources.insert(ConfigResource::new(ConfigResourceType::ClientMetrics, name.clone()));
            }
        }
        if all || config_resource_types.contains(&ConfigResourceType::Group) {
            for name in state.group_configs.keys() {
                config_resources.insert(ConfigResource::new(ConfigResourceType::Group, name.clone()));
            }
        }
        handle.complete(config_resources.into_iter().collect());
        ListConfigResourcesResult::new(handle.future())
    }

    #[allow(deprecated)]
    fn list_client_metrics_resources(
        &self,
        _options: ListClientMetricsResourcesOptions,
    ) -> ListClientMetricsResourcesResult {
        let state = self.state.lock().unwrap();
        let handle: KafkaFutureImpl<Vec<ClientMetricsResourceListing>> = KafkaFutureImpl::new();
        // Mirrors Java's `MockAdminClient.listClientMetricsResources`, which
        // maps every client-metrics config key to a `ClientMetricsResourceListing`.
        let listings = state
            .client_metrics_configs
            .keys()
            .map(ClientMetricsResourceListing::new)
            .collect();
        handle.complete(listings);
        ListClientMetricsResourcesResult::new(handle.future())
    }

    /// Mirrors `MockAdminClient.describeLogDirs`.
    fn describe_log_dirs(&self, brokers: &[i32], _options: DescribeLogDirsOptions) -> DescribeLogDirsResult {
        let state = self.state.lock().unwrap();
        let mut unwrapped: HashMap<i32, HashMap<String, LogDirDescription>> = HashMap::new();
        for &broker in brokers {
            unwrapped.entry(broker).or_default();
        }

        // Two deliberate divergences below, both skipping where Java throws.
        // Java dereferences `partitionLogDirs.get(0)` and
        // `unwrappedResults.get(node.id())` unchecked
        // (`MockAdminClient.java:1082-1083`), so a topic with no log dirs raises
        // `IndexOutOfBoundsException` and a replica on a broker the caller did
        // not ask about raises an NPE. Both are latent defects in a test helper
        // rather than a contract, and reproducing them would mean panicking in a
        // public API (CLAUDE.md §10.1), so each case is skipped instead.
        for (topic_name, meta) in &state.all_topics {
            // For tests, we assume there will always be only 1 log-dir entry.
            let Some(log_dir) = meta.partition_log_dirs.first() else {
                continue;
            };
            for tp_info in &meta.partitions {
                for node in tp_info.replicas() {
                    let Some(map) = unwrapped.get_mut(&node.id()) else {
                        continue;
                    };
                    let existing = map
                        .remove(log_dir)
                        .unwrap_or_else(|| LogDirDescription::new(None, HashMap::new()));
                    let mut replica_infos = existing.replica_infos().clone();
                    replica_infos.insert(
                        TopicPartition::new(topic_name.clone(), tp_info.partition()),
                        ReplicaInfo::new(0, 0, false),
                    );
                    map.insert(
                        log_dir.clone(),
                        LogDirDescription::with_volume_bytes(
                            existing.error().cloned(),
                            replica_infos,
                            existing.total_bytes().unwrap_or(UNKNOWN_VOLUME_BYTES),
                            existing.usable_bytes().unwrap_or(UNKNOWN_VOLUME_BYTES),
                        ),
                    );
                }
            }
        }

        let results = unwrapped
            .into_iter()
            .map(|(broker, map)| {
                let handle: KafkaFutureImpl<HashMap<String, LogDirDescription>> = KafkaFutureImpl::new();
                handle.complete(map);
                (broker, handle.future())
            })
            .collect();
        DescribeLogDirsResult::new(results)
    }

    /// Mirrors `MockAdminClient.alterReplicaLogDirs`.
    fn alter_replica_log_dirs(
        &self,
        replica_assignment: &HashMap<TopicPartitionReplica, String>,
        _options: AlterReplicaLogDirsOptions,
    ) -> AlterReplicaLogDirsResult {
        let mut state = self.state.lock().unwrap();
        let mut results = HashMap::new();
        for (replica, new_log_dir) in replica_assignment {
            let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            results.insert(replica.clone(), handle.future());

            let dirs = state.broker_log_dirs.get(replica.broker_id() as usize);
            if dirs.is_none() {
                handle.complete_with_error(Error::with_message(
                    Errors::ReplicaNotAvailable,
                    format!("Can't find {replica}"),
                ));
                continue;
            }
            if !dirs.unwrap().contains(new_log_dir) {
                handle.complete_with_error(Error::with_message(
                    Errors::KafkaStorageError,
                    format!("Log directory {new_log_dir} is offline"),
                ));
                continue;
            }
            // `usize::try_from` (not an `i32` comparison) rejects a negative
            // partition number outright: `(len as i32) > replica.partition()`
            // is true for *any* non-negative length when the partition is
            // negative, so the old guard let a negative index through and
            // `as usize` then wrapped it to a huge index, panicking on the
            // `Vec` indexing below. `TopicPartitionReplica` has no
            // constructor-time validation (mirroring Java), so this was
            // reachable through the public `alter_replica_log_dirs` surface.
            let move_info = usize::try_from(replica.partition()).ok().and_then(|idx| {
                state.all_topics.get(replica.topic()).and_then(|meta| {
                    (idx < meta.partitions.len()).then(|| {
                        ReplicaLogDirInfo::new(
                            Some(meta.partition_log_dirs[idx].clone()),
                            0,
                            Some(new_log_dir.clone()),
                            0,
                        )
                    })
                })
            });
            match move_info {
                Some(info) => {
                    state.replica_moves.insert(replica.clone(), info);
                    handle.complete(());
                },
                None => {
                    handle.complete_with_error(Error::with_message(
                        Errors::ReplicaNotAvailable,
                        format!("Can't find {replica}"),
                    ));
                },
            }
        }
        AlterReplicaLogDirsResult::new(results)
    }

    /// Mirrors `MockAdminClient.describeReplicaLogDirs`.
    fn describe_replica_log_dirs(
        &self,
        replicas: &[TopicPartitionReplica],
        _options: DescribeReplicaLogDirsOptions,
    ) -> DescribeReplicaLogDirsResult {
        let state = self.state.lock().unwrap();
        let mut results = HashMap::new();
        for replica in replicas {
            // Replicas of unknown topics are silently omitted from the result,
            // mirroring Java's `if (topicMetadata != null)` guard.
            let Some(meta) = state.all_topics.get(replica.topic()) else {
                continue;
            };
            let handle: KafkaFutureImpl<ReplicaLogDirInfo> = KafkaFutureImpl::new();
            // `currentLogDir(replica)`: null if the partition has no log dir.
            // As in `alter_replica_log_dirs` above, `usize::try_from` rejects a
            // negative partition number instead of letting an `i32`
            // comparison (asymmetric around negative numbers) admit it and
            // then wrap to a huge index on the `as usize` cast below.
            let current_log_dir = usize::try_from(replica.partition())
                .ok()
                .filter(|&idx| idx < meta.partition_log_dirs.len())
                .map(|idx| meta.partition_log_dirs[idx].clone());
            match current_log_dir {
                None => {
                    handle.complete(ReplicaLogDirInfo::default());
                },
                Some(dir) => {
                    let info = state
                        .replica_moves
                        .get(replica)
                        .cloned()
                        .unwrap_or_else(|| ReplicaLogDirInfo::new(Some(dir), 0, None, 0));
                    handle.complete(info);
                },
            }
            results.insert(replica.clone(), handle.future());
        }
        DescribeReplicaLogDirsResult::new(results)
    }

    /// Mirrors `MockAdminClient.electLeaders`, which throws
    /// `UnsupportedOperationException("Not implemented yet")`
    /// (`MockAdminClient.java:797`). Translated to a future failed with an
    /// "unsupported" `Error` (CLAUDE.md §10.1: no panic in public API).
    fn elect_leaders(
        &self,
        _election_type: ElectionType,
        _partitions: Option<HashSet<TopicPartition>>,
        _options: ElectLeadersOptions,
    ) -> ElectLeadersResult {
        let handle: KafkaFutureImpl<HashMap<TopicPartition, Option<Error>>> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
        ElectLeadersResult::new(handle.future())
    }

    /// Mirrors `MockAdminClient.alterPartitionReassignments`.
    fn alter_partition_reassignments(
        &self,
        reassignments: &HashMap<TopicPartition, Option<NewPartitionReassignment>>,
        _options: AlterPartitionReassignmentsOptions,
    ) -> AlterPartitionReassignmentsResult {
        let mut state = self.state.lock().unwrap();
        let mut futures = HashMap::new();
        for (partition, new_reassignment) in reassignments {
            let future: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            let topic_metadata = state.all_topics.get(partition.topic());
            let out_of_range = partition.partition() < 0
                || topic_metadata.is_none_or(|m| (m.partitions.len() as i32) <= partition.partition());
            if out_of_range {
                future.complete_with_error(Error::new(Errors::UnknownTopicOrPartition));
            } else if let Some(reassignment) = new_reassignment {
                state.reassignments.insert(partition.clone(), reassignment.clone());
                future.complete(());
            } else {
                state.reassignments.remove(partition);
                future.complete(());
            }
            futures.insert(partition.clone(), future.future());
        }
        AlterPartitionReassignmentsResult::new(futures)
    }

    /// Mirrors `MockAdminClient.listPartitionReassignments`.
    ///
    /// Java throws a `RuntimeException` from `findPartitionReassignment` when a
    /// stored reassignment names a topic that is no longer in `allTopics`
    /// (`MockAdminClient.java:1186-1192`). A synchronous throw is not
    /// representable in this signature, so the result's single future is failed
    /// instead — the same accommodation `list_offsets` makes for a
    /// `TimestampSpec` (CLAUDE.md §10.1).
    fn list_partition_reassignments(
        &self,
        partitions: Option<HashSet<TopicPartition>>,
        _options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult {
        let state = self.state.lock().unwrap();
        let mut map: HashMap<TopicPartition, PartitionReassignment> = HashMap::new();
        let requested: Vec<TopicPartition> = match partitions {
            Some(set) => set.into_iter().collect(),
            None => state.reassignments.keys().cloned().collect(),
        };
        let handle: KafkaFutureImpl<HashMap<TopicPartition, PartitionReassignment>> = KafkaFutureImpl::new();
        for partition in requested {
            match find_partition_reassignment(&state, &partition) {
                Ok(Some(reassignment)) => {
                    map.insert(partition, reassignment);
                },
                Ok(None) => {},
                Err(e) => {
                    handle.complete_with_error(e);
                    return ListPartitionReassignmentsResult::new(handle.future());
                },
            }
        }
        handle.complete(map);
        ListPartitionReassignmentsResult::new(handle.future())
    }

    /// Mirrors `MockAdminClient.listOffsets`.
    ///
    /// Java throws `UnsupportedOperationException` for a `TimestampSpec`
    /// (`MockAdminClient.java:1230`); since a synchronous throw is not
    /// representable in this signature, the affected partition's future is
    /// failed with an "unsupported" `Error` (CLAUDE.md §10.1).
    fn list_offsets(
        &self,
        topic_partition_offsets: &HashMap<TopicPartition, OffsetSpec>,
        _options: ListOffsetsOptions,
    ) -> ListOffsetsResult {
        let state = self.state.lock().unwrap();
        let mut futures = HashMap::new();
        for (tp, spec) in topic_partition_offsets {
            let future: KafkaFutureImpl<ListOffsetsResultInfo> = KafkaFutureImpl::new();
            match spec {
                OffsetSpec::Timestamp(_) => {
                    future.complete_with_error(Error::unsupported_version("Not implemented yet"));
                },
                OffsetSpec::Earliest => {
                    let offset = state.beginning_offsets.get(tp).copied().unwrap_or(-1);
                    future.complete(ListOffsetsResultInfo::new(offset, -1, None));
                },
                _ => {
                    let offset = state.end_offsets.get(tp).copied().unwrap_or(-1);
                    future.complete(ListOffsetsResultInfo::new(offset, -1, None));
                },
            }
            futures.insert(tp.clone(), future.future());
        }
        ListOffsetsResult::new(futures)
    }

    fn list_groups(&self, _options: ListGroupsOptions) -> ListGroupsResult {
        // Mirrors Java's `MockAdminClient.listGroups`: one CONSUMER/STABLE
        // GroupListing per seeded group config.
        let state = self.state.lock().unwrap();
        let listings: Vec<Result<GroupListing, Error>> = state
            .group_configs
            .keys()
            .map(|g| {
                Ok(GroupListing::new(
                    g.clone(),
                    Some(GroupType::Consumer),
                    PROTOCOL_TYPE,
                    Some(GroupState::Stable),
                ))
            })
            .collect();
        let handle: KafkaFutureImpl<Vec<Result<GroupListing, Error>>> = KafkaFutureImpl::new();
        handle.complete(listings);
        ListGroupsResult::new(handle.future())
    }

    #[allow(deprecated)]
    fn list_consumer_groups(&self, _options: ListConsumerGroupsOptions) -> ListConsumerGroupsResult {
        // Mirrors Java's `MockAdminClient.listConsumerGroups`: a simple
        // ConsumerGroupListing per seeded group config.
        let state = self.state.lock().unwrap();
        let listings: Vec<Result<ConsumerGroupListing, Error>> = state
            .group_configs
            .keys()
            .map(|g| Ok(ConsumerGroupListing::new(g.clone(), None, None, false)))
            .collect();
        let handle: KafkaFutureImpl<Vec<Result<ConsumerGroupListing, Error>>> = KafkaFutureImpl::new();
        handle.complete(listings);
        ListConsumerGroupsResult::new(handle.future())
    }

    fn describe_consumer_groups(
        &self,
        group_ids: &[String],
        _options: DescribeConsumerGroupsOptions,
    ) -> DescribeConsumerGroupsResult {
        // Java's `MockAdminClient.describeConsumerGroups` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:735). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future rather than a panic.
        let mut futures = HashMap::new();
        for group_id in group_ids {
            let handle: KafkaFutureImpl<ConsumerGroupDescription> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            futures.insert(group_id.clone(), handle.future());
        }
        DescribeConsumerGroupsResult::new(futures)
    }

    fn describe_classic_groups(
        &self,
        group_ids: &[String],
        _options: DescribeClassicGroupsOptions,
    ) -> DescribeClassicGroupsResult {
        // Java's `MockAdminClient.describeClassicGroups` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:1478). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future rather than a panic.
        let mut futures = HashMap::new();
        for group_id in group_ids {
            let handle: KafkaFutureImpl<ClassicGroupDescription> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            futures.insert(group_id.clone(), handle.future());
        }
        DescribeClassicGroupsResult::new(futures)
    }

    fn list_consumer_group_offsets(
        &self,
        group_specs: &HashMap<String, ListConsumerGroupOffsetsSpec>,
        _options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult {
        // Java ignores the group and assumes each test works on a single group;
        // more than one group is "Not implemented yet"
        // (MockAdminClient.java:750). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future rather than a panic.
        if group_specs.len() != 1 {
            let futures = group_specs
                .keys()
                .map(|group| {
                    let handle: KafkaFutureImpl<GroupOffsets> = KafkaFutureImpl::new();
                    handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
                    (group.clone(), handle.future())
                })
                .collect();
            return ListConsumerGroupOffsetsResult::new(futures);
        }

        let (group, spec) = group_specs.iter().next().expect("exactly one group");
        // `None` topic partitions (or an empty list) means "all partitions".
        let include_all = spec.get_topic_partitions().is_none_or(<[_]>::is_empty);
        let state = self.state.lock().unwrap();
        // Java builds each row with `new OffsetAndMetadata(entry.getValue())`
        // inline in the collect (MockAdminClient.java:756), and that constructor
        // rejects a negative offset with
        // `IllegalArgumentException("Invalid negative offset")`
        // (OffsetAndMetadata.java:49-50). A negative offset *is* seedable and
        // nothing upstream establishes otherwise: `updateConsumerGroupOffsets` is
        // an unvalidated `putAll` (MockAdminClient.java:1493-1495), which the
        // Rust mock mirrors. Java's throw is a catchable `RuntimeException`;
        // a Rust panic here would instead unwind out of the `extern "C"` FFI
        // wrapper — `admin_sync_future_op` runs the submit closure inline on the
        // calling thread — and abort the process. So the error is surfaced on the
        // group's future, which the trait signature can carry
        // (CLAUDE.md §10.1/§10.2, admin-client.md §9).
        let offsets: Result<GroupOffsets, Error> = state
            .committed_offsets
            .iter()
            .filter(|(tp, _)| include_all || spec.get_topic_partitions().is_some_and(|tps| tps.contains(tp)))
            .map(|(tp, &offset)| OffsetAndMetadata::new(offset).map(|committed| (tp.clone(), Some(committed))))
            .collect();
        drop(state);

        let handle: KafkaFutureImpl<GroupOffsets> = KafkaFutureImpl::new();
        match offsets {
            Ok(offsets) => {
                handle.complete(offsets);
            },
            Err(error) => {
                handle.complete_with_error(error);
            },
        }
        ListConsumerGroupOffsetsResult::new(HashMap::from([(group.clone(), handle.future())]))
    }

    fn alter_consumer_group_offsets(
        &self,
        _group_id: &str,
        _offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        _options: AlterConsumerGroupOffsetsOptions,
    ) -> AlterConsumerGroupOffsetsResult {
        // Java's `MockAdminClient.alterConsumerGroupOffsets` throws
        // `UnsupportedOperationException("Not implement yet")`
        // (MockAdminClient.java:1213 — note Java's own typo "implement"). Per
        // admin-client.md §9 the Rust mock surfaces that as an exceptional
        // future rather than a panic.
        let handle: KafkaFutureImpl<HashMap<TopicPartition, Errors>> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::unsupported_version("Not implement yet"));
        AlterConsumerGroupOffsetsResult::new(handle.future())
    }

    fn delete_consumer_group_offsets(
        &self,
        _group_id: &str,
        partitions: &HashSet<TopicPartition>,
        _options: DeleteConsumerGroupOffsetsOptions,
    ) -> DeleteConsumerGroupOffsetsResult {
        // Java's `MockAdminClient.deleteConsumerGroupOffsets` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:783). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future rather than a panic.
        let handle: KafkaFutureImpl<HashMap<TopicPartition, Errors>> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
        DeleteConsumerGroupOffsetsResult::new(handle.future(), partitions.clone())
    }

    fn delete_consumer_groups(
        &self,
        group_ids: &[String],
        _options: DeleteConsumerGroupsOptions,
    ) -> DeleteConsumerGroupsResult {
        // Java's `MockAdminClient.deleteConsumerGroups` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:773-775). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future per group rather than a panic.
        let mut futures = HashMap::new();
        for group_id in group_ids {
            let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            futures.insert(group_id.clone(), handle.future());
        }
        DeleteConsumerGroupsResult::new(futures)
    }

    fn remove_members_from_consumer_group(
        &self,
        _group_id: &str,
        options: RemoveMembersFromConsumerGroupOptions,
    ) -> RemoveMembersFromConsumerGroupResult {
        // Java's `MockAdminClient.removeMembersFromConsumerGroup` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:801-803). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future rather than a panic.
        let handle: KafkaFutureImpl<HashMap<MemberIdentity, Errors>> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
        RemoveMembersFromConsumerGroupResult::new(handle.future(), options.members().clone())
    }

    fn create_acls(&self, acls: &[AclBinding], _options: CreateAclsOptions) -> CreateAclsResult {
        // Java's `MockAdminClient.createAcls` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:806-808). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future per binding rather than a
        // panic.
        let mut futures = HashMap::new();
        for acl in acls {
            let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            futures.insert(acl.clone(), handle.future());
        }
        CreateAclsResult::new(futures)
    }

    fn describe_acls(&self, _filter: &AclBindingFilter, _options: DescribeAclsOptions) -> DescribeAclsResult {
        // Java's `MockAdminClient.describeAcls` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:811-813). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future rather than a panic.
        let handle: KafkaFutureImpl<Vec<AclBinding>> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
        DescribeAclsResult::new(handle.future())
    }

    fn delete_acls(&self, filters: &[AclBindingFilter], _options: DeleteAclsOptions) -> DeleteAclsResult {
        // Java's `MockAdminClient.deleteAcls` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:816-818). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future per filter rather than a panic.
        let mut futures = HashMap::new();
        for filter in filters {
            let handle: KafkaFutureImpl<FilterResults> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            futures.insert(filter.clone(), handle.future());
        }
        DeleteAclsResult::new(futures)
    }

    fn describe_client_quotas(
        &self,
        _filter: &ClientQuotaFilter,
        _options: DescribeClientQuotasOptions,
    ) -> DescribeClientQuotasResult {
        // Java's `MockAdminClient.describeClientQuotas` throws
        // `UnsupportedOperationException("Not implement yet")`
        // (MockAdminClient.java:1243-1245). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future rather than a panic.
        let handle: KafkaFutureImpl<HashMap<ClientQuotaEntity, HashMap<String, f64>>> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::unsupported_version("Not implement yet"));
        DescribeClientQuotasResult::new(handle.future())
    }

    fn alter_client_quotas(
        &self,
        entries: &[ClientQuotaAlteration],
        _options: AlterClientQuotasOptions,
    ) -> AlterClientQuotasResult {
        // Java's `MockAdminClient.alterClientQuotas` throws
        // `UnsupportedOperationException("Not implement yet")`
        // (MockAdminClient.java:1248-1250). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future per entity rather than a panic.
        let mut futures = HashMap::new();
        for entry in entries {
            let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implement yet"));
            futures.insert(entry.entity().clone(), handle.future());
        }
        AlterClientQuotasResult::new(futures)
    }

    fn describe_user_scram_credentials(
        &self,
        _users: &[String],
        _options: DescribeUserScramCredentialsOptions,
    ) -> DescribeUserScramCredentialsResult {
        // Java's `MockAdminClient.describeUserScramCredentials` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:1251-1254). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future rather than a panic.
        let handle: KafkaFutureImpl<DescribeUserScramCredentialsResponseData> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
        DescribeUserScramCredentialsResult::new(handle.future())
    }

    fn alter_user_scram_credentials(
        &self,
        alterations: &[UserScramCredentialAlteration],
        _options: AlterUserScramCredentialsOptions,
    ) -> AlterUserScramCredentialsResult {
        // Java's `MockAdminClient.alterUserScramCredentials` throws
        // `UnsupportedOperationException("Not implemented yet")`
        // (MockAdminClient.java:1256-1259). Per admin-client.md §9 the Rust mock
        // surfaces that as an exceptional future per user rather than a panic.
        let mut futures = HashMap::new();
        for alteration in alterations {
            let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            handle.complete_with_error(Error::unsupported_version("Not implemented yet"));
            futures.insert(alteration.user().to_string(), handle.future());
        }
        AlterUserScramCredentialsResult::new(futures)
    }

    fn create_delegation_token(&self, options: CreateDelegationTokenOptions) -> CreateDelegationTokenResult {
        // Mirrors MockAdminClient.createDelegationToken: reject any non-User
        // renewer, otherwise mint a token whose id doubles as its HMAC and
        // whose owner is the first renewer, and store it in `all_tokens`.
        let handle: KafkaFutureImpl<DelegationToken> = KafkaFutureImpl::new();
        for renewer in options.renewers() {
            if renewer.principal_type() != KafkaPrincipal::USER_TYPE {
                handle.complete_with_error(Error::with_message(Errors::InvalidPrincipalType, ""));
                return CreateDelegationTokenResult::new(handle.future());
            }
        }

        // Java uses `options.renewers().get(0)` as the owner
        // (MockAdminClient.java:652), which throws `IndexOutOfBoundsException`
        // when no renewer was supplied. That is a catchable `RuntimeException`
        // in Java, but an index panic in Rust — and every FFI path runs this
        // inline on the calling thread, so it would unwind across an
        // `extern "C"` boundary and abort the process. Per CLAUDE.md §10.1 the
        // future is completed exceptionally instead.
        let Some(owner) = options.renewers().first().cloned() else {
            handle.complete_with_error(Error::local_illegal_argument(
                "createDelegationToken requires at least one renewer: MockAdminClient makes the first renewer the owner",
            ));
            return CreateDelegationTokenResult::new(handle.future());
        };

        let token_id = Uuid::random_uuid().to_string();
        // The delegation token's HMAC is the UTF-8 bytes of the token id.
        let token_info = TokenInformation::new(
            token_id.clone(),
            owner,
            options.renewers().to_vec(),
            current_time_millis(),
            options.max_lifetime_ms(),
            -1,
        );
        let token = DelegationToken::new(token_info, token_id.into_bytes());
        self.state.lock().unwrap().all_tokens.push(token.clone());
        handle.complete(token);
        CreateDelegationTokenResult::new(handle.future())
    }

    fn renew_delegation_token(&self, hmac: &[u8], options: RenewDelegationTokenOptions) -> RenewDelegationTokenResult {
        // Mirrors MockAdminClient.renewDelegationToken: update the expiry of
        // every matching token; error if none matched.
        let handle: KafkaFutureImpl<i64> = KafkaFutureImpl::new();
        let expiry_timestamp = options.renew_time_period_ms();
        let mut token_found = false;
        {
            let mut state = self.state.lock().unwrap();
            for token in &mut state.all_tokens {
                if token.hmac() == hmac {
                    token.token_info_mut().set_expiry_timestamp(expiry_timestamp);
                    token_found = true;
                }
            }
        }
        if token_found {
            handle.complete(expiry_timestamp);
        } else {
            handle.complete_with_error(Error::with_message(Errors::DelegationTokenNotFound, ""));
        }
        RenewDelegationTokenResult::new(handle.future())
    }

    fn expire_delegation_token(
        &self,
        hmac: &[u8],
        options: ExpireDelegationTokenOptions,
    ) -> ExpireDelegationTokenResult {
        // Mirrors MockAdminClient.expireDelegationToken: remove matching tokens
        // whose expiry period is the `-1` sentinel or already in the past;
        // error if none matched.
        let handle: KafkaFutureImpl<i64> = KafkaFutureImpl::new();
        let expiry_timestamp = options.expiry_time_period_ms();
        let now = current_time_millis();
        let mut token_found = false;
        let mut tokens_to_remove = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            for token in &state.all_tokens {
                if token.hmac() == hmac {
                    if expiry_timestamp == -1 || expiry_timestamp < now {
                        tokens_to_remove.push(token.clone());
                    }
                    token_found = true;
                }
            }
            if token_found {
                state.all_tokens.retain(|token| !tokens_to_remove.contains(token));
            }
        }
        if token_found {
            handle.complete(expiry_timestamp);
        } else {
            handle.complete_with_error(Error::with_message(Errors::DelegationTokenNotFound, ""));
        }
        ExpireDelegationTokenResult::new(handle.future())
    }

    fn describe_delegation_token(&self, options: DescribeDelegationTokenOptions) -> DescribeDelegationTokenResult {
        // Mirrors MockAdminClient.describeDelegationToken: no owners filter
        // returns every token; otherwise only tokens whose owner is in the
        // filter. (Java NPEs on a null owners list; the Rust option models the
        // nullable field, and both a null and an empty filter return all
        // tokens — matching the real client's "null describes all" contract.)
        let handle: KafkaFutureImpl<Vec<DelegationToken>> = KafkaFutureImpl::new();
        let state = self.state.lock().unwrap();
        let tokens = match options.owners() {
            // Null or empty owners filter -> describe all tokens.
            None | Some([]) => state.all_tokens.clone(),
            Some(owners) => state
                .all_tokens
                .iter()
                .filter(|token| owners.contains(token.token_info().owner()))
                .cloned()
                .collect(),
        };
        handle.complete(tokens);
        DescribeDelegationTokenResult::new(handle.future())
    }

    fn describe_features(&self, _options: DescribeFeaturesOptions) -> DescribeFeaturesResult {
        // Mirrors MockAdminClient.describeFeatures: derive finalized and
        // supported ranges from the seeded feature-level maps.
        let state = self.state.lock().unwrap();
        let handle: KafkaFutureImpl<FeatureMetadata> = KafkaFutureImpl::new();

        let mut finalized_features = HashMap::new();
        let mut supported_features = HashMap::new();
        for (feature, &level) in &state.feature_levels {
            let min = state.min_supported_feature_levels.get(feature).copied().unwrap_or(0);
            let max = state.max_supported_feature_levels.get(feature).copied().unwrap_or(0);
            match (FinalizedVersionRange::new(level, level), SupportedVersionRange::new(min, max)) {
                (Ok(finalized), Ok(supported)) => {
                    finalized_features.insert(feature.clone(), finalized);
                    supported_features.insert(feature.clone(), supported);
                },
                (Err(e), _) | (_, Err(e)) => {
                    handle.complete_with_error(e);
                    return DescribeFeaturesResult::new(handle.future());
                },
            }
        }

        handle.complete(FeatureMetadata::new(finalized_features, Some(123), supported_features));
        DescribeFeaturesResult::new(handle.future())
    }

    fn update_features(
        &self,
        feature_updates: &HashMap<String, FeatureUpdate>,
        options: UpdateFeaturesOptions,
    ) -> Result<UpdateFeaturesResult, Error> {
        // Mirrors MockAdminClient.updateFeatures: validate each update against
        // the seeded version bounds; the first failure aborts the whole batch.
        let mut state = self.state.lock().unwrap();
        let mut error: Option<Error> = None;
        for (feature, update) in feature_updates {
            let cur = state.feature_levels.get(feature).copied().unwrap_or(0);
            let next = update.max_version_level();
            let min = state.min_supported_feature_levels.get(feature).copied().unwrap_or(0);
            let max = state.max_supported_feature_levels.get(feature).copied().unwrap_or(0);
            if let Err(message) = validate_feature_update(cur, next, min, max, update.upgrade_type()) {
                error = Some(invalid_update_version(feature, next, &message));
                break;
            }
        }

        let mut results: HashMap<String, crate::common::KafkaFuture<()>> = HashMap::new();
        for (feature, update) in feature_updates {
            let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            match &error {
                None => {
                    handle.complete(());
                    if !options.validate_only() {
                        state.feature_levels.insert(feature.clone(), update.max_version_level());
                    }
                },
                Some(e) => {
                    handle.complete_with_error(e.clone());
                },
            }
            results.insert(feature.clone(), handle.future());
        }

        Ok(UpdateFeaturesResult::new(results))
    }

    async fn close(&self, _timeout: Duration) {
        // Nothing to close for the in-memory mock.
    }
}

/// Validates a single feature update against the mock's seeded version bounds,
/// returning the inner error message on failure. Mirrors the `switch` /
/// bounds checks inside `MockAdminClient.updateFeatures`.
fn validate_feature_update(cur: i16, next: i16, min: i16, max: i16, upgrade_type: UpgradeType) -> Result<(), String> {
    match upgrade_type {
        UpgradeType::Unknown => return Err("Invalid upgrade type.".to_string()),
        UpgradeType::Upgrade => {
            if cur > next {
                return Err("Can't upgrade to lower version.".to_string());
            }
        },
        UpgradeType::SafeDowngrade => {
            if cur < next {
                return Err("Can't downgrade to newer version.".to_string());
            }
        },
        UpgradeType::UnsafeDowngrade => {
            if cur < next {
                return Err("Can't downgrade to newer version.".to_string());
            }
            // Simulate a scenario where all the even feature levels are unsafe
            // to downgrade from. Mirrors Java exactly: the inner
            // `SAFE_DOWNGRADE` guard can never fire in this `UNSAFE_DOWNGRADE`
            // branch, so the loop only walks `cur` down to `next`.
            let mut cur = cur;
            while next != cur {
                cur -= 1;
            }
        },
    }
    if next < min {
        return Err(format!("Can't downgrade below {min}"));
    }
    if next > max {
        return Err(format!("Can't upgrade above {max}"));
    }
    Ok(())
}

/// Composes the mock's `InvalidRequestException` for a rejected feature update.
/// Mirrors `MockAdminClient.invalidUpdateVersion`.
fn invalid_update_version(feature: &str, version: i16, message: &str) -> Error {
    Error::with_message(
        Errors::InvalidRequest,
        format!("Invalid update version {version} for feature {feature}. {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin() -> MockAdminClient {
        MockAdminClient::create(3).expect("num_brokers is at least 1")
    }

    // --- Builder broker-count validation (MockAdminClient.java:152, :210) ----

    /// Java's `Builder.build()` reads `brokers.get(0)` for the controller
    /// (`MockAdminClient.java:210`), so `numBrokers(0)` throws
    /// `IndexOutOfBoundsException` rather than yielding a broker-less mock.
    #[test]
    fn create_rejects_zero_brokers_rather_than_fabricating_a_controller() {
        let err = MockAdminClient::create(0).expect_err("zero brokers must be rejected");
        assert!(
            matches!(err, Error::LocalIllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert_eq!(err.message(), "num_brokers must be at least 1, was 0");
    }

    /// `numBrokers(-1)` throws even earlier in Java, from `brokers.subList(0, -1)`
    /// (`MockAdminClient.java:152`).
    #[test]
    fn create_rejects_a_negative_broker_count() {
        let err = MockAdminClient::create(-1).expect_err("a negative count must be rejected");
        assert_eq!(err.message(), "num_brokers must be at least 1, was -1");
    }

    /// The controller Java picks is `brokers.get(0)` — an element of the broker
    /// list, hence always present in `describeCluster().nodes()`. This pins that
    /// invariant for every valid count; the count at which the fabricated
    /// `Node::new(0, "localhost", 1000)` fallback broke it is now unreachable, and
    /// is covered by `create_rejects_zero_brokers_rather_than_fabricating_a_controller`.
    #[tokio::test]
    async fn controller_is_always_one_of_the_seeded_nodes() {
        for num_brokers in 1..=3 {
            let mock = MockAdminClient::create(num_brokers).expect("num_brokers is at least 1");
            let described = mock.describe_cluster(DescribeClusterOptions::new());
            let nodes = described.nodes().get().await.expect("nodes");
            let controller = described.controller().get().await.expect("controller").expect("a controller");
            assert!(
                nodes.contains(&controller),
                "controller {controller:?} is absent from nodes {nodes:?} for num_brokers={num_brokers}"
            );
            assert_eq!(controller.id(), 0);
            assert_eq!(nodes.len(), num_brokers as usize);
        }
    }

    /// A mock seeded with a single feature `feature` at level 3, supported over
    /// the range [1, 5] (mirrors the shape used by Java's `MockAdminClient`
    /// feature tests).
    fn admin_with_features() -> MockAdminClient {
        let mock = MockAdminClient::create(1).expect("num_brokers is at least 1");
        mock.set_feature_levels(
            HashMap::from([("feature".to_string(), 3i16)]),
            HashMap::from([("feature".to_string(), 1i16)]),
            HashMap::from([("feature".to_string(), 5i16)]),
        );
        mock
    }

    async fn update_one(mock: &MockAdminClient, next: i16, upgrade_type: UpgradeType) -> Result<(), Error> {
        let updates = HashMap::from([("feature".to_string(), FeatureUpdate::new(next, upgrade_type).unwrap())]);
        let result = mock.update_features(&updates, UpdateFeaturesOptions::new()).unwrap();
        result.values()["feature"].get().await
    }

    #[tokio::test]
    async fn mock_describe_features_returns_seeded_ranges() {
        let mock = admin_with_features();
        let metadata = mock
            .describe_features(DescribeFeaturesOptions::new())
            .feature_metadata()
            .get()
            .await
            .unwrap();
        assert_eq!(metadata.finalized_features_epoch(), Some(123));
        assert_eq!(
            metadata.finalized_features()["feature"],
            FinalizedVersionRange::new(3, 3).unwrap()
        );
        assert_eq!(
            metadata.supported_features()["feature"],
            SupportedVersionRange::new(1, 5).unwrap()
        );
    }

    #[tokio::test]
    async fn mock_update_features_upgrade_succeeds_and_mutates_level() {
        let mock = admin_with_features();
        update_one(&mock, 4, UpgradeType::Upgrade).await.unwrap();
        // The finalized level is now 4.
        let metadata = mock
            .describe_features(DescribeFeaturesOptions::new())
            .feature_metadata()
            .get()
            .await
            .unwrap();
        assert_eq!(
            metadata.finalized_features()["feature"],
            FinalizedVersionRange::new(4, 4).unwrap()
        );
    }

    #[tokio::test]
    async fn mock_update_features_validate_only_does_not_mutate() {
        let mock = admin_with_features();
        let updates = HashMap::from([("feature".to_string(), FeatureUpdate::new(4, UpgradeType::Upgrade).unwrap())]);
        let result = mock
            .update_features(&updates, UpdateFeaturesOptions::new().set_validate_only(true))
            .unwrap();
        result.values()["feature"].get().await.unwrap();
        // Level unchanged because validate_only was set.
        let metadata = mock
            .describe_features(DescribeFeaturesOptions::new())
            .feature_metadata()
            .get()
            .await
            .unwrap();
        assert_eq!(
            metadata.finalized_features()["feature"],
            FinalizedVersionRange::new(3, 3).unwrap()
        );
    }

    #[tokio::test]
    async fn mock_update_features_rejects_upgrade_to_lower_version() {
        let mock = admin_with_features();
        let err = update_one(&mock, 2, UpgradeType::Upgrade).await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        assert_eq!(
            err.message(),
            "Invalid update version 2 for feature feature. Can't upgrade to lower version."
        );
    }

    #[tokio::test]
    async fn mock_update_features_rejects_safe_downgrade_to_newer_version() {
        let mock = admin_with_features();
        let err = update_one(&mock, 4, UpgradeType::SafeDowngrade).await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        assert_eq!(
            err.message(),
            "Invalid update version 4 for feature feature. Can't downgrade to newer version."
        );
    }

    #[tokio::test]
    async fn mock_update_features_rejects_upgrade_above_max() {
        let mock = admin_with_features();
        let err = update_one(&mock, 6, UpgradeType::Upgrade).await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        assert_eq!(
            err.message(),
            "Invalid update version 6 for feature feature. Can't upgrade above 5"
        );
    }

    #[tokio::test]
    async fn mock_update_features_rejects_downgrade_below_min() {
        let mock = admin_with_features();
        // next = 0 (a deletion), with SAFE_DOWNGRADE; min is 1.
        let err = update_one(&mock, 0, UpgradeType::SafeDowngrade).await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        assert_eq!(
            err.message(),
            "Invalid update version 0 for feature feature. Can't downgrade below 1"
        );
    }

    #[tokio::test]
    async fn mock_update_features_rejects_unknown_upgrade_type() {
        let mock = admin_with_features();
        let err = update_one(&mock, 2, UpgradeType::Unknown).await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        assert_eq!(
            err.message(),
            "Invalid update version 2 for feature feature. Invalid upgrade type."
        );
    }

    #[tokio::test]
    async fn mock_update_features_unsafe_downgrade_succeeds() {
        let mock = admin_with_features();
        // cur = 3, next = 2, UNSAFE_DOWNGRADE: allowed (the mock's even-level
        // guard is dead code, mirroring Java).
        update_one(&mock, 2, UpgradeType::UnsafeDowngrade).await.unwrap();
        let metadata = mock
            .describe_features(DescribeFeaturesOptions::new())
            .feature_metadata()
            .get()
            .await
            .unwrap();
        assert_eq!(
            metadata.finalized_features()["feature"],
            FinalizedVersionRange::new(2, 2).unwrap()
        );
    }

    #[tokio::test]
    async fn create_then_list_and_describe() {
        let client = admin();
        let result = client.create_topics(
            &[NewTopic::new_num_partitions_replication_factor("t", Some(2), Some(2))],
            CreateTopicsOptions::new(),
        );
        result.all().get().await.unwrap();
        assert_eq!(result.num_partitions("t").get().await.unwrap(), 2);
        assert_eq!(result.replication_factor("t").get().await.unwrap(), 2);

        let names = client.list_topics(ListTopicsOptions::new()).names().get().await.unwrap();
        assert!(names.contains("t"));

        let desc = client
            .describe_topics(
                TopicCollection::of_topic_names(vec!["t".to_string()]),
                DescribeTopicsOptions::new(),
            )
            .all_topic_names()
            .unwrap()
            .get()
            .await
            .unwrap();
        assert_eq!(desc["t"].partitions().len(), 2);
    }

    #[tokio::test]
    async fn create_existing_topic_fails_with_topic_exists() {
        let client = admin();
        client
            .create_topics(
                &[NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(1))],
                CreateTopicsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();
        let result = client.create_topics(
            &[NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(1))],
            CreateTopicsOptions::new(),
        );
        let err = result.values()["t"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::TopicAlreadyExists);
        assert_eq!(err.message(), "Topic t exists already.");
    }

    #[tokio::test]
    async fn create_with_replication_factor_too_large_fails() {
        let client = MockAdminClient::create(1).expect("num_brokers is at least 1");
        let result = client.create_topics(
            &[NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(5))],
            CreateTopicsOptions::new(),
        );
        let err = result.values()["t"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidReplicationFactor);
    }

    #[tokio::test]
    async fn mock_create_topics_rejects_a_leader_with_no_log_directories() {
        let client = admin();
        client.set_broker_log_dirs(0, Vec::new()).expect("broker 0 exists");
        let result = client.create_topics(
            &[NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(1))],
            CreateTopicsOptions::new(),
        );
        let err = result.values()["t"].get().await.unwrap_err();
        assert_eq!(err.message(), "Broker 0 has no log directories.");
    }

    #[tokio::test]
    async fn describe_nonexistent_topic_is_unknown_topic() {
        let client = admin();
        let result = client.describe_topics(
            TopicCollection::of_topic_names(vec!["missing".to_string()]),
            DescribeTopicsOptions::new(),
        );
        let err = result.topic_name_values().unwrap()["missing"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicOrPartition);
        assert_eq!(err.message(), "Topic missing not found.");
    }

    #[tokio::test]
    async fn delete_then_gone() {
        let client = admin();
        client
            .create_topics(
                &[NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(1))],
                CreateTopicsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();
        client
            .delete_topics(
                TopicCollection::of_topic_names(vec!["t".to_string()]),
                DeleteTopicsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();
        let names = client.list_topics(ListTopicsOptions::new()).names().get().await.unwrap();
        assert!(!names.contains("t"));
    }

    #[tokio::test]
    async fn delete_missing_topic_fails() {
        let client = admin();
        let result = client.delete_topics(
            TopicCollection::of_topic_names(vec!["nope".to_string()]),
            DeleteTopicsOptions::new(),
        );
        let err = result.topic_name_values().unwrap()["nope"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicOrPartition);
    }

    #[tokio::test]
    async fn timeout_next_request_times_out_create() {
        let client = admin();
        client.timeout_next_request(1);
        let result = client.create_topics(
            &[NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(1))],
            CreateTopicsOptions::new(),
        );
        assert!(matches!(result.values()["t"].get().await, Err(Error::Timeout(_))));
        // Next request succeeds.
        let result2 = client.create_topics(
            &[NewTopic::new_num_partitions_replication_factor("t2", Some(1), Some(1))],
            CreateTopicsOptions::new(),
        );
        result2.all().get().await.unwrap();
    }

    #[tokio::test]
    async fn describe_cluster_returns_brokers_and_controller() {
        let client = admin();
        let result = client.describe_cluster(DescribeClusterOptions::new());
        let nodes = result.nodes().get().await.unwrap();
        assert_eq!(nodes.len(), 3);
        let controller = result.controller().get().await.unwrap();
        assert_eq!(controller.unwrap().id(), 0);
        assert_eq!(result.cluster_id().get().await.unwrap(), DEFAULT_CLUSTER_ID);
        assert!(result.authorized_operations().get().await.unwrap().unwrap().is_empty());
    }

    #[tokio::test]
    async fn describe_cluster_timeout_recovers_on_next_call() {
        let client = admin();
        client.timeout_next_request(1);
        // First call times out on every future.
        let timed_out = client.describe_cluster(DescribeClusterOptions::new());
        assert!(matches!(timed_out.nodes().get().await, Err(Error::Timeout(_))));
        assert!(matches!(timed_out.controller().get().await, Err(Error::Timeout(_))));
        assert!(matches!(timed_out.cluster_id().get().await, Err(Error::Timeout(_))));
        // The counter is decremented, so the next call succeeds.
        let recovered = client.describe_cluster(DescribeClusterOptions::new());
        assert_eq!(recovered.nodes().get().await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn describe_configs_topic_returns_stored_configs() {
        let client = admin();
        let mut configs = BTreeMap::new();
        configs.insert("retention.ms".to_string(), "1000".to_string());
        let new_topic = NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(1)).configs(configs);
        client
            .create_topics(&[new_topic], CreateTopicsOptions::new())
            .all()
            .get()
            .await
            .unwrap();

        let resource = ConfigResource::new(ConfigResourceType::Topic, "t".to_string());
        let result = client.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
        let config = result.values()[&resource].get().await.unwrap();
        assert_eq!(config.get("retention.ms").unwrap().value(), Some("1000"));
    }

    #[tokio::test]
    async fn describe_configs_broker_returns_default_replication_factor() {
        let client = admin();
        let resource = ConfigResource::new(ConfigResourceType::Broker, "0".to_string());
        let result = client.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
        let config = result.values()[&resource].get().await.unwrap();
        assert_eq!(config.get("default.replication.factor").unwrap().value(), Some("3"));
    }

    #[tokio::test]
    async fn describe_configs_unknown_topic_is_unknown_topic_error() {
        let client = admin();
        let resource = ConfigResource::new(ConfigResourceType::Topic, "missing".to_string());
        let result = client.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
        let err = result.values()[&resource].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicOrPartition);
        assert_eq!(err.message(), "Resource ConfigResource(type=Topic, name='missing') not found.");
    }

    #[tokio::test]
    async fn describe_configs_unknown_broker_is_invalid_request() {
        let client = admin();
        let resource = ConfigResource::new(ConfigResourceType::Broker, "99".to_string());
        let result = client.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
        let err = result.values()[&resource].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        assert_eq!(err.message(), "Broker 99 not found.");
    }

    #[tokio::test]
    async fn describe_configs_timeout_recovers_on_next_call() {
        let client = admin();
        client.timeout_next_request(1);
        let resource = ConfigResource::new(ConfigResourceType::Broker, "0".to_string());
        let timed_out = client.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
        assert!(matches!(timed_out.values()[&resource].get().await, Err(Error::Timeout(_))));
        let recovered = client.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
        recovered.values()[&resource].get().await.unwrap();
    }

    #[tokio::test]
    async fn incremental_alter_configs_topic_set_and_delete() {
        let client = admin();
        client
            .create_topics(
                &[NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(1))],
                CreateTopicsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();
        let resource = ConfigResource::new(ConfigResourceType::Topic, "t".to_string());

        // SET.
        let set_op = AlterConfigOp::new(
            ConfigEntry::new("retention.ms".to_string(), Some("42".to_string())),
            OpType::Set,
        );
        let mut configs = HashMap::new();
        configs.insert(resource.clone(), vec![set_op]);
        client
            .incremental_alter_configs(&configs, AlterConfigsOptions::new())
            .all()
            .get()
            .await
            .unwrap();

        let described = client.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
        assert_eq!(
            described.values()[&resource]
                .get()
                .await
                .unwrap()
                .get("retention.ms")
                .unwrap()
                .value(),
            Some("42")
        );

        // DELETE.
        let delete_op = AlterConfigOp::new(ConfigEntry::new("retention.ms".to_string(), None), OpType::Delete);
        let mut configs = HashMap::new();
        configs.insert(resource.clone(), vec![delete_op]);
        client
            .incremental_alter_configs(&configs, AlterConfigsOptions::new())
            .all()
            .get()
            .await
            .unwrap();
        let described = client.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
        assert!(described.values()[&resource].get().await.unwrap().get("retention.ms").is_none());
    }

    #[tokio::test]
    async fn incremental_alter_configs_unknown_topic_is_unknown_topic_error() {
        let client = admin();
        let resource = ConfigResource::new(ConfigResourceType::Topic, "missing".to_string());
        let op = AlterConfigOp::new(ConfigEntry::new("k".to_string(), Some("v".to_string())), OpType::Set);
        let mut configs = HashMap::new();
        configs.insert(resource.clone(), vec![op]);
        let result = client.incremental_alter_configs(&configs, AlterConfigsOptions::new());
        let err = result.values()[&resource].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicOrPartition);
        assert_eq!(err.message(), "No such topic as missing");
    }

    #[tokio::test]
    async fn incremental_alter_configs_client_metrics_creates_resource() {
        let client = admin();
        let resource = ConfigResource::new(ConfigResourceType::ClientMetrics, "cm".to_string());
        let op = AlterConfigOp::new(
            ConfigEntry::new("interval.ms".to_string(), Some("5000".to_string())),
            OpType::Set,
        );
        let mut configs = HashMap::new();
        configs.insert(resource.clone(), vec![op]);
        client
            .incremental_alter_configs(&configs, AlterConfigsOptions::new())
            .all()
            .get()
            .await
            .unwrap();

        // The new client-metrics resource now shows up in list_config_resources.
        let listed = client
            .list_config_resources(
                &HashSet::from([ConfigResourceType::ClientMetrics]),
                ListConfigResourcesOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();
        assert!(listed.contains(&resource));
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn list_client_metrics_resources_returns_seeded_names() {
        use crate::admin::{ClientMetricsResourceListing, ListClientMetricsResourcesOptions};
        let client = admin();

        // Empty when nothing has been seeded, mirroring Java's mock over an
        // empty `clientMetricsConfigs`.
        let empty = client
            .list_client_metrics_resources(ListClientMetricsResourcesOptions::new())
            .all()
            .get()
            .await
            .unwrap();
        assert!(empty.is_empty());

        // Seed two client-metrics resources via incrementalAlterConfigs.
        for name in ["one", "two"] {
            let resource = ConfigResource::new(ConfigResourceType::ClientMetrics, name.to_string());
            let op = AlterConfigOp::new(
                ConfigEntry::new("interval.ms".to_string(), Some("5000".to_string())),
                OpType::Set,
            );
            let mut configs = HashMap::new();
            configs.insert(resource, vec![op]);
            client
                .incremental_alter_configs(&configs, AlterConfigsOptions::new())
                .all()
                .get()
                .await
                .unwrap();
        }

        let listed = client
            .list_client_metrics_resources(ListClientMetricsResourcesOptions::new())
            .all()
            .get()
            .await
            .unwrap();
        let expected: HashSet<ClientMetricsResourceListing> = [
            ClientMetricsResourceListing::new("one"),
            ClientMetricsResourceListing::new("two"),
        ]
        .into_iter()
        .collect();
        assert_eq!(listed.into_iter().collect::<HashSet<_>>(), expected);
    }

    #[tokio::test]
    async fn incremental_alter_configs_empty_client_metrics_name_is_invalid_request() {
        let client = admin();
        let resource = ConfigResource::new(ConfigResourceType::ClientMetrics, String::new());
        let op = AlterConfigOp::new(ConfigEntry::new("k".to_string(), Some("v".to_string())), OpType::Set);
        let mut configs = HashMap::new();
        configs.insert(resource.clone(), vec![op]);
        let result = client.incremental_alter_configs(&configs, AlterConfigsOptions::new());
        let err = result.values()[&resource].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        assert_eq!(err.message(), "Empty resource name");
    }

    #[tokio::test]
    async fn list_config_resources_all_types_when_empty() {
        let client = admin();
        client
            .create_topics(
                &[NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(1))],
                CreateTopicsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();
        let listed = client
            .list_config_resources(&HashSet::new(), ListConfigResourcesOptions::new())
            .all()
            .get()
            .await
            .unwrap();
        let set: HashSet<ConfigResource> = listed.into_iter().collect();
        assert!(set.contains(&ConfigResource::new(ConfigResourceType::Topic, "t".to_string())));
        // 3 brokers -> broker 0..2 and broker-logger 0..2.
        for i in 0..3 {
            assert!(set.contains(&ConfigResource::new(ConfigResourceType::Broker, i.to_string())));
            assert!(set.contains(&ConfigResource::new(ConfigResourceType::BrokerLogger, i.to_string())));
        }
    }

    #[tokio::test]
    async fn list_config_resources_filters_by_type() {
        let client = admin();
        client
            .create_topics(
                &[NewTopic::new_num_partitions_replication_factor("t", Some(1), Some(1))],
                CreateTopicsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();
        let listed = client
            .list_config_resources(&HashSet::from([ConfigResourceType::Topic]), ListConfigResourcesOptions::new())
            .all()
            .get()
            .await
            .unwrap();
        assert_eq!(listed, vec![ConfigResource::new(ConfigResourceType::Topic, "t".to_string())]);
    }

    // --- Delegation tokens ---
    //
    // There are ZERO Java client-side unit tests for delegation tokens anywhere
    // in `clients/src/test/java` (Milestone-11 finding #10). The tests below are
    // NEW, written against `MockAdminClient`'s real in-memory logic
    // (MockAdminClient.java ~641-725), which is the authoritative behavioral
    // reference for this phase.

    fn user(name: &str) -> KafkaPrincipal {
        KafkaPrincipal::new(KafkaPrincipal::USER_TYPE, name)
    }

    /// New test, no Java original: a non-`User`-type renewer is rejected with
    /// `InvalidPrincipalType` and the exact (empty) message Java uses.
    #[tokio::test]
    async fn create_delegation_token_rejects_non_user_renewer() {
        let client = admin();
        let options = CreateDelegationTokenOptions::new().set_renewers(vec![KafkaPrincipal::new("Group", "admins")]);
        let err = client
            .create_delegation_token(options)
            .delegation_token()
            .get()
            .await
            .unwrap_err();
        assert_eq!(err.error(), Errors::InvalidPrincipalType);
        assert_eq!(err.message(), "");
    }

    /// New test, no Java original: with no renewer at all the future completes
    /// exceptionally rather than panicking. Java's
    /// `options.renewers().get(0)` (MockAdminClient.java:652) throws a catchable
    /// `IndexOutOfBoundsException` here; an index panic in Rust would unwind
    /// across the C FFI boundary and abort the process, so the mock reports it
    /// as an error instead. `CreateDelegationTokenOptions::new()` defaults the
    /// renewer list to empty, so this is the *default* call.
    #[tokio::test]
    async fn create_delegation_token_without_a_renewer_reports_an_error() {
        let client = admin();
        let err = client
            .create_delegation_token(CreateDelegationTokenOptions::new())
            .delegation_token()
            .get()
            .await
            .unwrap_err();
        assert_eq!(
            err.message(),
            "createDelegationToken requires at least one renewer: MockAdminClient makes the first renewer the owner"
        );
        // Nothing was stored, so a describe still finds no tokens.
        assert!(
            client
                .describe_delegation_token(DescribeDelegationTokenOptions::new())
                .delegation_tokens()
                .get()
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// New test, no Java original: a created token is owned by the first
    /// renewer, has the `-1` (unexpired) sentinel, and is listed by an
    /// unfiltered describe.
    #[tokio::test]
    async fn create_then_describe_lists_token() {
        let client = admin();
        let options = CreateDelegationTokenOptions::new()
            .set_renewers(vec![user("alice")])
            .set_max_lifetime_ms(1000);
        let token = client.create_delegation_token(options).delegation_token().get().await.unwrap();
        assert_eq!(token.token_info().owner(), &user("alice"));
        assert_eq!(token.token_info().max_timestamp(), 1000);
        assert_eq!(token.token_info().expiry_timestamp(), -1);
        assert_eq!(token.hmac(), token.token_info().token_id().as_bytes());

        let listed = client
            .describe_delegation_token(DescribeDelegationTokenOptions::new())
            .delegation_tokens()
            .get()
            .await
            .unwrap();
        assert_eq!(listed, vec![token]);
    }

    /// New test, no Java original: renewing an unknown HMAC fails with
    /// `DelegationTokenNotFound`; renewing a known HMAC updates the expiry.
    #[tokio::test]
    async fn renew_delegation_token_found_and_not_found() {
        let client = admin();
        let unknown = client
            .renew_delegation_token(b"nope", RenewDelegationTokenOptions::new().set_renew_time_period_ms(10))
            .expiry_timestamp()
            .get()
            .await
            .unwrap_err();
        assert_eq!(unknown.error(), Errors::DelegationTokenNotFound);
        assert_eq!(unknown.message(), "");

        let token = client
            .create_delegation_token(CreateDelegationTokenOptions::new().set_renewers(vec![user("alice")]))
            .delegation_token()
            .get()
            .await
            .unwrap();
        let expiry = client
            .renew_delegation_token(token.hmac(), RenewDelegationTokenOptions::new().set_renew_time_period_ms(4242))
            .expiry_timestamp()
            .get()
            .await
            .unwrap();
        assert_eq!(expiry, 4242);
    }

    /// New test, no Java original: expiring an unknown HMAC fails with
    /// `DelegationTokenNotFound`.
    #[tokio::test]
    async fn expire_delegation_token_not_found() {
        let client = admin();
        let err = client
            .expire_delegation_token(b"nope", ExpireDelegationTokenOptions::new())
            .expiry_timestamp()
            .get()
            .await
            .unwrap_err();
        assert_eq!(err.error(), Errors::DelegationTokenNotFound);
        assert_eq!(err.message(), "");
    }

    /// New test, no Java original: expiring with the `-1` sentinel removes the
    /// token so a later describe no longer lists it.
    #[tokio::test]
    async fn expire_delegation_token_negative_one_removes_token() {
        let client = admin();
        let token = client
            .create_delegation_token(CreateDelegationTokenOptions::new().set_renewers(vec![user("alice")]))
            .delegation_token()
            .get()
            .await
            .unwrap();

        let expiry = client
            .expire_delegation_token(token.hmac(), ExpireDelegationTokenOptions::new().set_expiry_time_period_ms(-1))
            .expiry_timestamp()
            .get()
            .await
            .unwrap();
        assert_eq!(expiry, -1);

        let listed = client
            .describe_delegation_token(DescribeDelegationTokenOptions::new())
            .delegation_tokens()
            .get()
            .await
            .unwrap();
        assert!(listed.is_empty());
    }

    /// New test, no Java original: a describe with an `owners` filter returns
    /// only tokens whose owner matches.
    #[tokio::test]
    async fn describe_delegation_token_owners_filter() {
        let client = admin();
        let token_alice = client
            .create_delegation_token(CreateDelegationTokenOptions::new().set_renewers(vec![user("alice")]))
            .delegation_token()
            .get()
            .await
            .unwrap();
        let _token_bob = client
            .create_delegation_token(CreateDelegationTokenOptions::new().set_renewers(vec![user("bob")]))
            .delegation_token()
            .get()
            .await
            .unwrap();

        let listed = client
            .describe_delegation_token(DescribeDelegationTokenOptions::new().set_owners(Some(vec![user("alice")])))
            .delegation_tokens()
            .get()
            .await
            .unwrap();
        assert_eq!(listed, vec![token_alice]);
    }

    /// Seeds one topic and reassigns its only partition.
    async fn admin_with_reassignment() -> (MockAdminClient, TopicPartition) {
        let client = admin();
        let new_topic = NewTopic::new_num_partitions_replication_factor("rt", Some(1), Some(3));
        client
            .create_topics(std::slice::from_ref(&new_topic), CreateTopicsOptions::new())
            .all()
            .get()
            .await
            .unwrap();

        let tp = TopicPartition::new("rt".to_string(), 0);
        let target = NewPartitionReassignment::new(vec![1, 2]).unwrap();
        client
            .alter_partition_reassignments(
                &HashMap::from([(tp.clone(), Some(target))]),
                AlterPartitionReassignmentsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();
        (client, tp)
    }

    #[tokio::test]
    async fn alter_then_list_partition_reassignments_reports_adding_and_removing() {
        let (client, tp) = admin_with_reassignment().await;

        let listed = client
            .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
            .reassignments()
            .get()
            .await
            .unwrap();
        let reassignment = &listed[&tp];
        // The mock seeds every partition with all three brokers as replicas, so
        // targeting {1, 2} removes broker 0 and adds nothing.
        assert_eq!(reassignment.replicas(), &[0, 1, 2]);
        assert_eq!(reassignment.adding_replicas(), &[] as &[i32]);
        assert_eq!(reassignment.removing_replicas(), &[0]);

        // An empty `Optional` cancels (`MockAdminClient.java:1160-1162`).
        client
            .alter_partition_reassignments(
                &HashMap::from([(tp.clone(), None)]),
                AlterPartitionReassignmentsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();
        let listed = client
            .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
            .reassignments()
            .get()
            .await
            .unwrap();
        assert!(listed.is_empty());
    }

    /// Regression: `delete_topics` drops the topic from `all_topics` without
    /// pruning `reassignments` (as Java's does), so a subsequent
    /// `list_partition_reassignments` reaches `findPartitionReassignment`'s
    /// "no TopicMetadata" branch. Java throws a `RuntimeException` there; this
    /// client must fail the future rather than panic, because the panic would
    /// unwind out of a C FFI entry point.
    #[tokio::test]
    async fn list_partition_reassignments_after_topic_deletion_fails_the_future() {
        let (client, _tp) = admin_with_reassignment().await;
        client
            .delete_topics(
                TopicCollection::of_topic_names(vec!["rt".to_string()]),
                DeleteTopicsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();

        let error = client
            .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
            .reassignments()
            .get()
            .await
            .unwrap_err();
        assert_eq!(
            error.message(),
            "Internal MockAdminClient logic error: found reassignment for rt-0, but no TopicMetadata"
        );
    }

    /// Regression for `findPartitionReassignment`'s *second* guard
    /// (`MockAdminClient.java:1190-1192`): the topic exists again, but the
    /// stale reassignment names a partition index the recreated topic no longer
    /// has.
    ///
    /// This branch is dead in Java — `metadata.partitions` is an `ArrayList`,
    /// so `get(i)` on an out-of-range index throws `IndexOutOfBoundsException`
    /// and can never return null — but it is live in Rust, where
    /// `Vec::get(i)` returns `None`. Rust therefore reports Java's intended
    /// message instead of Java's `IndexOutOfBoundsException`, and above all
    /// does not panic across the FFI boundary.
    #[tokio::test]
    async fn list_partition_reassignments_after_topic_shrink_fails_the_future() {
        let client = admin();
        let wide = NewTopic::new_num_partitions_replication_factor("rt2", Some(2), Some(3));
        client
            .create_topics(std::slice::from_ref(&wide), CreateTopicsOptions::new())
            .all()
            .get()
            .await
            .unwrap();

        // Reassign the *second* partition, so the index survives the topic's
        // removal but not its recreation.
        let tp = TopicPartition::new("rt2".to_string(), 1);
        let target = NewPartitionReassignment::new(vec![1, 2]).unwrap();
        client
            .alter_partition_reassignments(
                &HashMap::from([(tp.clone(), Some(target))]),
                AlterPartitionReassignmentsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();

        client
            .delete_topics(
                TopicCollection::of_topic_names(vec!["rt2".to_string()]),
                DeleteTopicsOptions::new(),
            )
            .all()
            .get()
            .await
            .unwrap();

        let narrow = NewTopic::new_num_partitions_replication_factor("rt2", Some(1), Some(3));
        client
            .create_topics(std::slice::from_ref(&narrow), CreateTopicsOptions::new())
            .all()
            .get()
            .await
            .unwrap();

        let error = client
            .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
            .reassignments()
            .get()
            .await
            .unwrap_err();
        assert_eq!(
            error.message(),
            "Internal MockAdminClient logic error: found reassignment for rt2-1, but no TopicPartitionInfo"
        );
    }
    // --- add_topic broker validation (MockAdminClient.java:296-309) ----------

    /// Builds a `TopicPartitionInfo` with the given leader / replicas / isr and
    /// no offline, ELR or last-known-ELR replicas.
    fn partition_info(partition: i32, leader: Option<Node>, replicas: Vec<Node>, isr: Vec<Node>) -> TopicPartitionInfo {
        TopicPartitionInfo::new(partition, leader, replicas, isr, Vec::new(), Vec::new())
    }

    /// The nodes `MockAdminClient::create` seeds, so a test can name a broker
    /// the mock actually has (`Node::new(id, "localhost", 1000 + id)`).
    fn seeded_broker(id: i32) -> Node {
        Node::new(id, "localhost".to_string(), 1000 + id)
    }

    /// A node no seeded broker equals, for the "unknown broker" arms.
    fn unknown_broker() -> Node {
        Node::new(99, "elsewhere".to_string(), 9999)
    }

    #[test]
    fn mock_add_topic_accepts_partitions_whose_brokers_are_all_known() {
        let mock = admin();
        let leader = seeded_broker(0);
        let replicas = vec![seeded_broker(0), seeded_broker(1)];
        mock.add_topic(
            false,
            "topic",
            vec![partition_info(0, Some(leader), replicas.clone(), replicas)],
            None,
        )
        .expect("every named broker is one of the mock's three");
    }

    #[test]
    fn mock_add_topic_rejects_a_duplicate_topic() {
        let mock = admin();
        let leader = seeded_broker(0);
        let partitions = vec![partition_info(
            0,
            Some(leader),
            vec![seeded_broker(0)],
            vec![seeded_broker(0)],
        )];
        mock.add_topic(false, "topic", partitions.clone(), None).unwrap();
        let error = mock.add_topic(false, "topic", partitions, None).unwrap_err();
        assert!(
            matches!(error, Error::LocalIllegalArgument(_)),
            "Java throws IllegalArgumentException: {error:?}"
        );
        assert_eq!(error.message(), "Topic topic was already added.");
    }

    #[tokio::test]
    async fn mock_add_topic_rejects_an_unknown_leader() {
        let mock = admin();
        let partitions = vec![partition_info(
            0,
            Some(unknown_broker()),
            vec![seeded_broker(0)],
            vec![],
        )];
        let error = mock.add_topic(false, "topic", partitions, None).unwrap_err();
        assert_eq!(error.message(), "Leader broker unknown");
        // Java's `brokers.contains(null)` is false for a leaderless partition, so
        // it takes this same branch rather than reaching the log-dir loop.
        let leaderless = vec![partition_info(0, None, vec![seeded_broker(0)], vec![])];
        let error = mock.add_topic(false, "other", leaderless, None).unwrap_err();
        assert_eq!(error.message(), "Leader broker unknown");
        // Neither rejected topic was recorded.
        let listed = mock
            .list_topics(ListTopicsOptions::new())
            .names()
            .get()
            .await
            .expect("listTopics succeeds");
        assert!(listed.is_empty(), "a rejected add_topic must not be recorded: {listed:?}");
    }

    #[test]
    fn mock_add_topic_rejects_unknown_brokers_in_the_replica_list() {
        let mock = admin();
        let partitions = vec![partition_info(
            0,
            Some(seeded_broker(0)),
            vec![seeded_broker(0), unknown_broker()],
            vec![],
        )];
        let error = mock.add_topic(false, "topic", partitions, None).unwrap_err();
        assert_eq!(error.message(), "Unknown brokers in replica list");
    }

    #[test]
    fn mock_add_topic_rejects_unknown_brokers_in_the_isr_list() {
        let mock = admin();
        // The replica list is fine here, so only the ISR check can fire -- which
        // pins the check order as well as the message.
        let partitions = vec![partition_info(
            0,
            Some(seeded_broker(0)),
            vec![seeded_broker(0)],
            vec![unknown_broker()],
        )];
        let error = mock.add_topic(false, "topic", partitions, None).unwrap_err();
        assert_eq!(error.message(), "Unknown brokers in isr list");
    }

    #[test]
    fn mock_add_topic_rejects_a_leader_with_no_log_directories() {
        let mock = admin();
        mock.set_broker_log_dirs(0, Vec::new()).expect("broker 0 exists");
        let partitions = vec![partition_info(
            0,
            Some(seeded_broker(0)),
            vec![seeded_broker(0)],
            vec![],
        )];
        let error = mock.add_topic(false, "topic", partitions, None).unwrap_err();
        assert_eq!(error.message(), "Broker 0 has no log directories.");
    }

    #[test]
    fn mock_set_broker_log_dirs_rejects_an_unknown_broker() {
        let mock = admin();
        let error = mock.set_broker_log_dirs(7, vec!["/data".to_string()]).unwrap_err();
        assert_eq!(error.message(), "Broker 7 does not exist.");
        let error = mock.set_broker_log_dirs(-1, vec!["/data".to_string()]).unwrap_err();
        assert_eq!(error.message(), "Broker -1 does not exist.");
    }

    #[test]
    fn mock_mark_topic_for_deletion_rejects_an_unknown_topic() {
        let mock = admin();
        let error = mock.mark_topic_for_deletion("nope").unwrap_err();
        assert!(
            matches!(error, Error::LocalIllegalArgument(_)),
            "Java throws IllegalArgumentException: {error:?}"
        );
        assert_eq!(error.message(), "Topic nope did not exist.");
    }

    // --- list_consumer_group_offsets with a negative seeded offset -----------

    #[tokio::test]
    async fn mock_list_consumer_group_offsets_rejects_a_negative_seeded_offset() {
        // `updateConsumerGroupOffsets` is an unvalidated `putAll` in Java
        // (MockAdminClient.java:1493-1495), so -1 -- Kafka's own invalid-offset
        // sentinel -- is seedable. Java then throws
        // `IllegalArgumentException("Invalid negative offset")` from
        // `new OffsetAndMetadata(...)` while building the row
        // (MockAdminClient.java:756, OffsetAndMetadata.java:49-50). The Rust mock
        // must surface a `Error`, not panic: the FFI runs this inline on the
        // caller's thread, so a panic would unwind out of `extern "C"`.
        let mock = admin();
        let tp = TopicPartition::new("topic".to_string(), 0);
        mock.update_consumer_group_offsets(HashMap::from([(tp.clone(), -1i64)]));

        let specs = HashMap::from([("group".to_string(), ListConsumerGroupOffsetsSpec::new())]);
        let error = mock
            .list_consumer_group_offsets(&specs, ListConsumerGroupOffsetsOptions::new())
            .partitions_to_offset_and_metadata()
            .expect("exactly one group was requested")
            .get()
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::LocalIllegalArgument(_)),
            "Java throws IllegalArgumentException: {error:?}"
        );
        assert_eq!(error.message(), "Invalid negative offset");

        // The mock is still usable afterwards -- which is the assertion that
        // distinguishes "returned an error" from "aborted the process".
        mock.update_consumer_group_offsets(HashMap::from([(tp.clone(), 7i64)]));
        let offsets = mock
            .list_consumer_group_offsets(&specs, ListConsumerGroupOffsetsOptions::new())
            .partitions_to_offset_and_metadata()
            .expect("exactly one group was requested")
            .get()
            .await
            .expect("a non-negative offset lists cleanly");
        assert_eq!(offsets[&tp].as_ref().map(OffsetAndMetadata::offset), Some(7));
    }

    #[tokio::test]
    async fn mock_list_consumer_group_offsets_negative_offset_outside_the_selection_is_ignored() {
        // The rejection follows Java's filter: a negative offset for a partition
        // the spec did not select is never turned into an `OffsetAndMetadata`, so
        // it cannot fail the call.
        let mock = admin();
        let selected = TopicPartition::new("topic".to_string(), 0);
        let other = TopicPartition::new("topic".to_string(), 1);
        mock.update_consumer_group_offsets(HashMap::from([(selected.clone(), 5i64), (other, -1i64)]));

        let specs = HashMap::from([(
            "group".to_string(),
            ListConsumerGroupOffsetsSpec::new().topic_partitions(Some(vec![selected.clone()])),
        )]);
        let offsets = mock
            .list_consumer_group_offsets(&specs, ListConsumerGroupOffsetsOptions::new())
            .partitions_to_offset_and_metadata()
            .expect("exactly one group was requested")
            .get()
            .await
            .expect("the unselected negative offset is filtered out before the constructor");
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[&selected].as_ref().map(OffsetAndMetadata::offset), Some(5));
    }
}
