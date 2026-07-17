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

use crate::admin::{
    Admin, AlterConfigOp, AlterConfigsOptions, AlterConfigsResult, Config, ConfigEntry, CreatePartitionsOptions,
    CreatePartitionsResult, CreateTopicsOptions, CreateTopicsResult, DeleteRecordsOptions, DeleteRecordsResult,
    DeleteTopicsOptions, DeleteTopicsResult, DeletedRecords, DescribeClusterOptions, DescribeClusterResult,
    DescribeConfigsOptions, DescribeConfigsResult, DescribeTopicsOptions, DescribeTopicsResult,
    ListConfigResourcesOptions, ListConfigResourcesResult, ListTopicsOptions, ListTopicsResult, NewPartitions,
    NewTopic, OpType, RecordsToDelete, TopicDescription, TopicListing, TopicMetadataAndConfig,
};
use crate::common::acl::AclOperation;
use crate::common::config::{ConfigResource, ConfigResourceType};
use crate::common::kafka_future::KafkaFutureImpl;
use crate::common::protocol::Errors;
use crate::common::{KafkaError, Node, TopicCollection, TopicPartition, TopicPartitionInfo, Uuid};

use std::collections::{BTreeSet, HashSet};

/// Default cluster id used by the mock (matches Java's `DEFAULT_CLUSTER_ID`).
const DEFAULT_CLUSTER_ID: &str = "4A5xz_QZTB2CtL4wc0X0Jw";

/// Internal per-topic metadata held by the mock.
#[derive(Clone, Debug)]
struct TopicMetadata {
    topic_id: Uuid,
    is_internal: bool,
    partitions: Vec<TopicPartitionInfo>,
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
    pub fn create(num_brokers: i32) -> Self {
        let brokers: Vec<Node> = (0..num_brokers)
            .map(|id| Node::new(id, "localhost".to_string(), 1000 + id))
            .collect();
        let controller = brokers
            .first()
            .cloned()
            .unwrap_or_else(|| Node::new(0, "localhost".to_string(), 1000));
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
        Self {
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
            }),
        }
    }

    /// Adds an existing topic to the mock's state.
    ///
    /// # Panics
    ///
    /// Panics if the topic was already added (mirrors Java's
    /// `IllegalArgumentException`).
    pub fn add_topic(
        &self,
        internal: bool,
        name: &str,
        partitions: Vec<TopicPartitionInfo>,
        configs: Option<BTreeMap<String, String>>,
    ) {
        let mut state = self.state.lock().unwrap();
        assert!(!state.all_topics.contains_key(name), "Topic {name} was already added.");
        let topic_id = Uuid::random_uuid();
        state.topic_ids.insert(name.to_string(), topic_id);
        state.topic_names.insert(topic_id, name.to_string());
        state.all_topics.insert(
            name.to_string(),
            TopicMetadata {
                topic_id,
                is_internal: internal,
                partitions,
                configs,
                marked_for_deletion: false,
                fetches_remaining_until_visible: 0,
            },
        );
    }

    /// Marks a topic for deletion so `describe_topics` treats it as absent.
    ///
    /// # Panics
    ///
    /// Panics if the topic does not exist (mirrors Java).
    pub fn mark_topic_for_deletion(&self, name: &str) {
        let mut state = self.state.lock().unwrap();
        let topic = state
            .all_topics
            .get_mut(name)
            .unwrap_or_else(|| panic!("Topic {name} did not exist."));
        topic.marked_for_deletion = true;
    }

    /// Causes the next `number_of_requests` operations to fail with a timeout.
    pub fn timeout_next_request(&self, number_of_requests: i32) {
        self.state.lock().unwrap().timeout_next_requests = number_of_requests;
    }
}

fn timeout_error() -> KafkaError {
    KafkaError::Timeout("The mock timed out the request.".to_string())
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
fn apply_alter_ops(map: &mut BTreeMap<String, String>, ops: &[AlterConfigOp]) -> Result<(), KafkaError> {
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
                return Err(KafkaError::with_message(
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
fn get_resource_description(state: &mut State, resource: &ConfigResource) -> Result<Config, KafkaError> {
    match resource.resource_type() {
        ConfigResourceType::Broker => {
            let broker_id: usize = resource.name().parse().map_err(|_| {
                KafkaError::with_message(Errors::InvalidRequest, format!("Broker {} not found.", resource.name()))
            })?;
            match state.broker_configs.get(broker_id) {
                Some(config) => Ok(to_config_object(config)),
                None => Err(KafkaError::with_message(
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
            Err(KafkaError::with_message(
                Errors::UnknownTopicOrPartition,
                format!("Resource {resource} not found."),
            ))
        },
        ConfigResourceType::ClientMetrics => {
            let resource_name = resource.name();
            if resource_name.is_empty() {
                return Err(KafkaError::with_message(Errors::InvalidRequest, "Empty resource name"));
            }
            let config = state.client_metrics_configs.get(resource_name).cloned().unwrap_or_default();
            Ok(to_config_object(&config))
        },
        ConfigResourceType::Group => {
            let resource_name = resource.name();
            if resource_name.is_empty() {
                return Err(KafkaError::with_message(Errors::InvalidRequest, "Empty resource name"));
            }
            let mut group_config = state.group_configs.get(resource_name).cloned().unwrap_or_default();
            // Overlay defaults for keys not already present (Java's `putIfAbsent`).
            for (k, v) in &state.default_group_configs {
                group_config.entry(k.clone()).or_insert_with(|| v.clone());
            }
            Ok(to_config_object(&group_config))
        },
        _ => Err(KafkaError::unsupported_version("Not implemented yet")),
    }
}

/// Applies an incremental config alteration to a single resource.
///
/// Corresponds to `MockAdminClient.handleIncrementalResourceAlteration`.
fn handle_incremental_resource_alteration(
    state: &mut State,
    resource: &ConfigResource,
    ops: &[AlterConfigOp],
) -> Result<(), KafkaError> {
    match resource.resource_type() {
        ConfigResourceType::Broker => {
            let broker_id: usize = resource.name().parse().map_err(|_| {
                KafkaError::with_message(Errors::InvalidRequest, format!("no such broker as {}", resource.name()))
            })?;
            if broker_id >= state.broker_configs.len() {
                return Err(KafkaError::with_message(
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
                KafkaError::with_message(
                    Errors::UnknownTopicOrPartition,
                    format!("No such topic as {}", resource.name()),
                )
            })?;
            let mut new_map = metadata.configs.clone().unwrap_or_default();
            apply_alter_ops(&mut new_map, ops)?;
            metadata.configs = Some(new_map);
            Ok(())
        },
        ConfigResourceType::ClientMetrics => {
            let resource_name = resource.name();
            if resource_name.is_empty() {
                return Err(KafkaError::with_message(Errors::InvalidRequest, "Empty resource name"));
            }
            let mut new_map = state.client_metrics_configs.get(resource_name).cloned().unwrap_or_default();
            apply_alter_ops(&mut new_map, ops)?;
            state.client_metrics_configs.insert(resource_name.to_string(), new_map);
            Ok(())
        },
        ConfigResourceType::Group => {
            let resource_name = resource.name();
            if resource_name.is_empty() {
                return Err(KafkaError::with_message(Errors::InvalidRequest, "Empty resource name"));
            }
            let mut new_map = state.group_configs.get(resource_name).cloned().unwrap_or_default();
            apply_alter_ops(&mut new_map, ops)?;
            state.group_configs.insert(resource_name.to_string(), new_map);
            Ok(())
        },
        _ => Err(KafkaError::unsupported_version("Not implemented yet")),
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
                handle.complete_exceptionally(timeout_error());
                result.insert(new_topic.name().to_string(), handle.future());
            }
            state.timeout_next_requests -= 1;
            return CreateTopicsResult::new(result);
        }

        for new_topic in new_topics {
            let handle: KafkaFutureImpl<TopicMetadataAndConfig> = KafkaFutureImpl::new();
            let topic_name = new_topic.name().to_string();

            if state.all_topics.contains_key(&topic_name) {
                handle.complete_exceptionally(KafkaError::with_message(
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
                handle.complete_exceptionally(KafkaError::with_message(
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

            let topic_id = Uuid::random_uuid();
            state.topic_ids.insert(topic_name.clone(), topic_id);
            state.topic_names.insert(topic_id, topic_name.clone());
            state.all_topics.insert(
                topic_name.clone(),
                TopicMetadata {
                    topic_id,
                    is_internal: false,
                    partitions,
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
                        handle.complete_exceptionally(timeout_error());
                    } else if state.all_topics.remove(&name).is_none() {
                        handle.complete_exceptionally(KafkaError::with_message(
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
                        handle.complete_exceptionally(timeout_error());
                    } else {
                        let name = state.topic_names.remove(&id);
                        let removed = name.as_ref().is_some_and(|n| state.all_topics.remove(n).is_some());
                        if !removed {
                            handle.complete_exceptionally(KafkaError::with_message(
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
            handle.complete_exceptionally(timeout_error());
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
                        handle.complete_exceptionally(timeout_error());
                        result.insert(requested.clone(), handle.future());
                        continue;
                    }
                    match state.all_topics.get(requested) {
                        Some(metadata) if !metadata.marked_for_deletion => {
                            handle.complete(TopicDescription::with_authorized_operations(
                                requested.clone(),
                                metadata.is_internal,
                                metadata.partitions.clone(),
                                std::collections::BTreeSet::new(),
                                metadata.topic_id,
                            ));
                        },
                        _ => {
                            handle.complete_exceptionally(KafkaError::with_message(
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
                        handle.complete_exceptionally(timeout_error());
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
                                std::collections::BTreeSet::new(),
                                *requested,
                            ));
                        },
                        None => {
                            handle.complete_exceptionally(KafkaError::with_message(
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
        // "unsupported" `KafkaError` per key instead of panicking (faithful
        // translation of the Java behavior).
        let mut result = HashMap::new();
        for topic in new_partitions.keys() {
            let handle: KafkaFutureImpl<()> = KafkaFutureImpl::new();
            handle.complete_exceptionally(KafkaError::unsupported_version("Not implemented yet"));
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
        // "unsupported" `KafkaError` per key instead of panicking (faithful
        // translation of the Java behavior).
        let mut result = HashMap::new();
        for topic_partition in records_to_delete.keys() {
            let handle: KafkaFutureImpl<DeletedRecords> = KafkaFutureImpl::new();
            handle.complete_exceptionally(KafkaError::unsupported_version("Not implemented yet"));
            result.insert(topic_partition.clone(), handle.future());
        }
        DeleteRecordsResult::new(result)
    }

    fn describe_cluster(&self, _options: DescribeClusterOptions) -> DescribeClusterResult {
        let mut state = self.state.lock().unwrap();
        let nodes: KafkaFutureImpl<Vec<Node>> = KafkaFutureImpl::new();
        let controller: KafkaFutureImpl<Option<Node>> = KafkaFutureImpl::new();
        let cluster_id: KafkaFutureImpl<String> = KafkaFutureImpl::new();
        let authorized_operations: KafkaFutureImpl<Option<BTreeSet<AclOperation>>> = KafkaFutureImpl::new();

        if state.timeout_next_requests > 0 {
            let err = timeout_error();
            nodes.complete_exceptionally(err.clone());
            controller.complete_exceptionally(err.clone());
            cluster_id.complete_exceptionally(err.clone());
            authorized_operations.complete_exceptionally(err);
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
                handle.complete_exceptionally(timeout_error());
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
                Err(e) => handle.complete_exceptionally(e),
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
                Err(e) => handle.complete_exceptionally(e),
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

    async fn close(&self, _timeout: Duration) {
        // Nothing to close for the in-memory mock.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin() -> MockAdminClient {
        MockAdminClient::create(3)
    }

    #[tokio::test]
    async fn create_then_list_and_describe() {
        let client = admin();
        let result = client.create_topics(&[NewTopic::new("t", 2, 2)], CreateTopicsOptions::new());
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
            .create_topics(&[NewTopic::new("t", 1, 1)], CreateTopicsOptions::new())
            .all()
            .get()
            .await
            .unwrap();
        let result = client.create_topics(&[NewTopic::new("t", 1, 1)], CreateTopicsOptions::new());
        let err = result.values()["t"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::TopicAlreadyExists);
        assert_eq!(err.message(), "Topic t exists already.");
    }

    #[tokio::test]
    async fn create_with_replication_factor_too_large_fails() {
        let client = MockAdminClient::create(1);
        let result = client.create_topics(&[NewTopic::new("t", 1, 5)], CreateTopicsOptions::new());
        let err = result.values()["t"].get().await.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidReplicationFactor);
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
            .create_topics(&[NewTopic::new("t", 1, 1)], CreateTopicsOptions::new())
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
        let result = client.create_topics(&[NewTopic::new("t", 1, 1)], CreateTopicsOptions::new());
        assert!(matches!(result.values()["t"].get().await, Err(KafkaError::Timeout(_))));
        // Next request succeeds.
        let result2 = client.create_topics(&[NewTopic::new("t2", 1, 1)], CreateTopicsOptions::new());
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
        assert!(matches!(timed_out.nodes().get().await, Err(KafkaError::Timeout(_))));
        assert!(matches!(timed_out.controller().get().await, Err(KafkaError::Timeout(_))));
        assert!(matches!(timed_out.cluster_id().get().await, Err(KafkaError::Timeout(_))));
        // The counter is decremented, so the next call succeeds.
        let recovered = client.describe_cluster(DescribeClusterOptions::new());
        assert_eq!(recovered.nodes().get().await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn describe_configs_topic_returns_stored_configs() {
        let client = admin();
        let mut configs = BTreeMap::new();
        configs.insert("retention.ms".to_string(), "1000".to_string());
        let new_topic = NewTopic::new("t", 1, 1).configs(configs);
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
        assert!(matches!(timed_out.values()[&resource].get().await, Err(KafkaError::Timeout(_))));
        let recovered = client.describe_configs(std::slice::from_ref(&resource), DescribeConfigsOptions::new());
        recovered.values()[&resource].get().await.unwrap();
    }

    #[tokio::test]
    async fn incremental_alter_configs_topic_set_and_delete() {
        let client = admin();
        client
            .create_topics(&[NewTopic::new("t", 1, 1)], CreateTopicsOptions::new())
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
            .create_topics(&[NewTopic::new("t", 1, 1)], CreateTopicsOptions::new())
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
            .create_topics(&[NewTopic::new("t", 1, 1)], CreateTopicsOptions::new())
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
}
