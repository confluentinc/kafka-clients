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
//! to the topic methods that are in scope for Tier 1 Phase 1).

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use crate::admin::{
    Admin, Config, ConfigEntry, CreatePartitionsOptions, CreatePartitionsResult, CreateTopicsOptions,
    CreateTopicsResult, DeleteRecordsOptions, DeleteRecordsResult, DeleteTopicsOptions, DeleteTopicsResult,
    DeletedRecords, DescribeTopicsOptions, DescribeTopicsResult, ListTopicsOptions, ListTopicsResult, NewPartitions,
    NewTopic, RecordsToDelete, TopicDescription, TopicListing, TopicMetadataAndConfig,
};
use crate::common::kafka_future::KafkaFutureImpl;
use crate::common::protocol::Errors;
use crate::common::{KafkaError, Node, TopicCollection, TopicPartition, TopicPartitionInfo, Uuid};

/// Default cluster id used by the mock (matches Java's `DEFAULT_CLUSTER_ID`).
const DEFAULT_CLUSTER_ID: &str = "4A5xz_QZTB2CtL4wc0X0Jw";

/// Internal per-topic metadata held by the mock.
#[derive(Clone, Debug)]
struct TopicMetadata {
    topic_id: Uuid,
    is_internal: bool,
    partitions: Vec<TopicPartitionInfo>,
    // Stored for describeConfigs (Tier 1 Phase 3); not read by the topic RPCs.
    #[allow(dead_code)]
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
}

/// An in-memory [`Admin`] implementation for tests.
///
/// Corresponds to `org.apache.kafka.clients.admin.MockAdminClient`. Only the
/// topic methods are implemented in Phase 1; other RPCs will be added with
/// their tiers. All futures returned are immediately resolved.
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
        // Java's `MockAdminClient.createPartitions` throws
        // `UnsupportedOperationException("Not implemented yet")`. Per
        // `.claude/rules/admin-client.md` §9 the Rust mock returns an
        // "unsupported" `KafkaError` per key instead of panicking (documented
        // deviation).
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
        // Java's `MockAdminClient.deleteRecords` returns an empty result for an
        // empty request and otherwise throws
        // `UnsupportedOperationException("Not implemented yet")`. Per
        // `.claude/rules/admin-client.md` §9 the non-empty case returns an
        // "unsupported" `KafkaError` per key instead of panicking (documented
        // deviation).
        let mut result = HashMap::new();
        for topic_partition in records_to_delete.keys() {
            let handle: KafkaFutureImpl<DeletedRecords> = KafkaFutureImpl::new();
            handle.complete_exceptionally(KafkaError::unsupported_version("Not implemented yet"));
            result.insert(topic_partition.clone(), handle.future());
        }
        DeleteRecordsResult::new(result)
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
}
