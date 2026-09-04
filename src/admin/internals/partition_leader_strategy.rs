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

//! Lookup strategy for APIs which target partition leaders.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.internals.PartitionLeaderStrategy`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, MetadataRequestBuilder, RequestBuilder};
use crate::common::utils::LogContext;
use crate::common::{Error, TopicPartition};
use crate::{kafka_debug, kafka_error};

use super::admin_api_future::{AdminApiFuture, UNKNOWN_BROKER_ID};
use super::admin_api_lookup_strategy::{AdminApiLookupStrategy, LookupResult};
use super::api_request_scope::ApiRequestScope;
use super::partition_leader_cache::PartitionLeaderCache;
use crate::common::kafka_future::{KafkaFuture, KafkaFutureImpl};

/// Base driver implementation for APIs which target partition leaders.
///
/// Corresponds to `PartitionLeaderStrategy`.
pub(crate) struct PartitionLeaderStrategy {
    log_context: LogContext,
    tolerate_unknown_topics: bool,
}

impl PartitionLeaderStrategy {
    /// Creates a strategy that tolerates unknown-topic errors (retries them).
    pub(crate) fn new(log_context: LogContext) -> Self {
        Self::with_tolerate_unknown_topics(log_context, true)
    }

    /// Creates a strategy, controlling whether unknown-topic errors are
    /// tolerated (retried) or treated as fatal.
    pub(crate) fn with_tolerate_unknown_topics(log_context: LogContext, tolerate_unknown_topics: bool) -> Self {
        Self { log_context, tolerate_unknown_topics }
    }

    /// Handles a topic-level error, failing all requested partitions for the
    /// topic when the error is fatal.
    ///
    /// Mirrors `handleTopicError`.
    fn handle_topic_error(
        &self,
        topic: &str,
        topic_error: Errors,
        request_partitions: &HashSet<TopicPartition>,
        failed: &mut HashMap<TopicPartition, Error>,
    ) {
        match topic_error {
            Errors::UnknownTopicOrPartition if !self.tolerate_unknown_topics => {
                kafka_error!(self.log_context, "Received unknown topic error for topic {}", topic);
                // Java's closure parameter is `tp` and it fills the *partition*
                // slot; the topic appears only inside the backticks
                // (`PartitionLeaderStrategy.java:88-90`). Printing the topic in
                // both slots left every per-partition future with the same message
                // (finding 244).
                self.fail_all_partitions_for_topic(topic, request_partitions, failed, |tp| {
                    Error::with_message(
                        topic_error,
                        format!(
                            "Failed to fetch metadata for partition {tp} because metadata for topic `{topic}` \
                             could not be found"
                        ),
                    )
                });
            },
            Errors::UnknownTopicOrPartition | Errors::LeaderNotAvailable | Errors::BrokerNotAvailable => {
                kafka_debug!(
                    self.log_context,
                    "Metadata request for topic {} returned topic-level error {:?}. Will retry",
                    topic,
                    topic_error
                );
            },
            Errors::TopicAuthorizationFailed => {
                kafka_error!(
                    self.log_context,
                    "Received authorization failure for topic {} in `Metadata` response",
                    topic
                );
                let topic_owned = topic.to_string();
                self.fail_all_partitions_for_topic(topic, request_partitions, failed, |tp| {
                    Error::topic_authorization_message(
                        HashSet::from([topic_owned.clone()]),
                        format!("Failed to fetch metadata for partition {tp} due to topic authorization failure"),
                    )
                });
            },
            Errors::InvalidTopicError => {
                kafka_error!(
                    self.log_context,
                    "Received invalid topic error for topic {} in `Metadata` response",
                    topic
                );
                let topic_owned = topic.to_string();
                self.fail_all_partitions_for_topic(topic, request_partitions, failed, |tp| {
                    Error::invalid_topics_message(
                        HashSet::from([topic_owned.clone()]),
                        format!("Failed to fetch metadata for partition {tp} due to invalid topic `{topic_owned}`"),
                    )
                });
            },
            _ => {
                kafka_error!(
                    self.log_context,
                    "Received unexpected error for topic {} in `Metadata` response",
                    topic
                );
                self.fail_all_partitions_for_topic(topic, request_partitions, failed, |tp| {
                    Error::with_message(
                        topic_error,
                        format!(
                            "Failed to fetch metadata for partition {tp} due to unexpected error for topic `{topic}`"
                        ),
                    )
                });
            },
        }
    }

    /// Fails every requested partition for the given topic using
    /// `error_generator`.
    ///
    /// Mirrors `failAllPartitionsForTopic`.
    fn fail_all_partitions_for_topic(
        &self,
        topic: &str,
        partitions: &HashSet<TopicPartition>,
        failed: &mut HashMap<TopicPartition, Error>,
        error_generator: impl Fn(&TopicPartition) -> Error,
    ) {
        for tp in partitions {
            if tp.topic() == topic {
                failed.insert(tp.clone(), error_generator(tp));
            }
        }
    }

    /// Handles a partition-level error, failing the partition when the error is
    /// unexpected (retriable errors are left out of the result for retry).
    ///
    /// Mirrors `handlePartitionError`.
    fn handle_partition_error(
        &self,
        topic_partition: &TopicPartition,
        partition_error: Errors,
        failed: &mut HashMap<TopicPartition, Error>,
    ) {
        match partition_error {
            Errors::NotLeaderOrFollower
            | Errors::ReplicaNotAvailable
            | Errors::LeaderNotAvailable
            | Errors::BrokerNotAvailable
            | Errors::KafkaStorageError
            | Errors::UnknownTopicOrPartition => {
                kafka_debug!(
                    self.log_context,
                    "Metadata request for partition {} returned partition-level error {:?}. Will retry",
                    topic_partition,
                    partition_error
                );
            },
            _ => {
                kafka_error!(
                    self.log_context,
                    "Received unexpected error for partition {} in `Metadata` response",
                    topic_partition
                );
                failed.insert(
                    topic_partition.clone(),
                    Error::with_message(
                        partition_error,
                        format!("Unexpected error during metadata lookup for {topic_partition}"),
                    ),
                );
            },
        }
    }
}

impl AdminApiLookupStrategy<TopicPartition> for PartitionLeaderStrategy {
    fn lookup_scope(&self, _key: &TopicPartition) -> ApiRequestScope {
        // Metadata requests can group topic partitions arbitrarily, so they can
        // all share the same request scope.
        ApiRequestScope::SingleLookup
    }

    fn build_request(&self, partitions: &HashSet<TopicPartition>) -> Box<dyn RequestBuilder> {
        let mut topics: Vec<&str> = Vec::new();
        for tp in partitions {
            if !topics.contains(&tp.topic()) {
                topics.push(tp.topic());
            }
        }
        Box::new(MetadataRequestBuilder::new(Some(&topics), false))
    }

    fn handle_response(
        &self,
        request_partitions: &HashSet<TopicPartition>,
        response: &ConcreteResponse,
    ) -> LookupResult<TopicPartition> {
        let ConcreteResponse::Metadata(response) = response else {
            // Java fails the call once (`KafkaAdminClient.java:1387-1391`); an empty
            // result would silently re-issue the lookup until the deadline. See
            // `LookupResult::failed_all`.
            return LookupResult::failed_all(
                request_partitions,
                Error::local_illegal_state("PartitionLeaderStrategy received an unexpected response type"),
            );
        };
        let mut failed: HashMap<TopicPartition, Error> = HashMap::new();
        let mut mapped: HashMap<TopicPartition, i32> = HashMap::new();

        for topic_metadata in &response.data().topics {
            let topic = topic_metadata.name.as_deref().unwrap_or("");
            let topic_error = Errors::for_code(topic_metadata.error_code);
            if topic_error != Errors::None {
                self.handle_topic_error(topic, topic_error, request_partitions, &mut failed);
                continue;
            }

            for partition_metadata in &topic_metadata.partitions {
                let topic_partition = TopicPartition::new(topic, partition_metadata.partition_index);
                let partition_error = Errors::for_code(partition_metadata.error_code);

                if !request_partitions.contains(&topic_partition) {
                    // The `Metadata` response always returns all partitions for
                    // requested topics, so we filter any we are not interested in.
                    continue;
                }

                if partition_error != Errors::None {
                    self.handle_partition_error(&topic_partition, partition_error, &mut failed);
                    continue;
                }

                let leader_id = partition_metadata.leader_id;
                if leader_id >= 0 {
                    mapped.insert(topic_partition, leader_id);
                } else {
                    kafka_debug!(
                        self.log_context,
                        "Metadata request for {} returned no error, but the leader is unknown. Will retry",
                        topic_partition
                    );
                }
            }
        }
        LookupResult::new(failed, mapped)
    }
}

/// An [`AdminApiFuture`] that starts with a pre-fetched key-to-broker mapping
/// (kept up to date as metadata is fetched), so repeated identical calls can
/// skip the lookup stage.
///
/// Corresponds to `PartitionLeaderStrategy.PartitionLeaderFuture`.
pub(crate) struct PartitionLeaderFuture<V: Clone + Send + Sync + 'static> {
    request_keys: HashSet<TopicPartition>,
    partition_leader_cache: Arc<PartitionLeaderCache>,
    futures: HashMap<TopicPartition, KafkaFutureImpl<V>>,
}

impl<V: Clone + Send + Sync + 'static> PartitionLeaderFuture<V> {
    /// Creates a future bundle for the given request keys, backed by the shared
    /// leader cache.
    pub(crate) fn new(
        request_keys: HashSet<TopicPartition>,
        partition_leader_cache: Arc<PartitionLeaderCache>,
    ) -> Self {
        let futures = request_keys.iter().map(|tp| (tp.clone(), KafkaFutureImpl::new())).collect();
        Self { request_keys, partition_leader_cache, futures }
    }

    /// Returns the per-partition public futures (the `*Result` view).
    ///
    /// Mirrors `all`.
    pub(crate) fn all(&self) -> HashMap<TopicPartition, KafkaFuture<V>> {
        self.futures.iter().map(|(tp, f)| (tp.clone(), f.future())).collect()
    }
}

impl<V: Clone + Send + Sync + 'static> AdminApiFuture<TopicPartition, V> for PartitionLeaderFuture<V> {
    fn lookup_keys(&self) -> HashSet<TopicPartition> {
        self.futures.keys().cloned().collect()
    }

    fn cached_key_broker_id_mapping(&self) -> HashMap<TopicPartition, i32> {
        let cache = self.partition_leader_cache.get(&self.request_keys);
        self.request_keys
            .iter()
            .map(|tp| (tp.clone(), cache.get(tp).copied().unwrap_or(UNKNOWN_BROKER_ID)))
            .collect()
    }

    fn complete(&self, values: HashMap<TopicPartition, V>) {
        for (key, value) in values {
            if let Some(future) = self.futures.get(&key) {
                future.complete(value);
            }
        }
    }

    fn complete_lookup(&self, broker_id_mapping: HashMap<TopicPartition, i32>) {
        self.partition_leader_cache.put(&broker_id_mapping);
    }

    fn complete_with_error(&self, errors: HashMap<TopicPartition, Error>) {
        self.partition_leader_cache.remove(errors.keys());
        for (key, error) in errors {
            if let Some(future) = self.futures.get(&key) {
                future.complete_with_error(error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::MetadataResponse;
    use crate::metadata_response_data::{MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic};

    fn strategy() -> PartitionLeaderStrategy {
        PartitionLeaderStrategy::new(LogContext::new("[test] "))
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, partition)
    }

    fn partition_with_leader(partition: i32, leader_id: i32, replicas: Vec<i32>) -> MetadataResponsePartition {
        let mut p = MetadataResponsePartition::new();
        p.set_partition_index(partition);
        p.set_error_code(Errors::None.code());
        p.set_leader_id(leader_id);
        p.set_replica_nodes(replicas.clone());
        p.set_isr_nodes(replicas);
        p
    }

    fn partition_with_error(partition: i32, error: Errors) -> MetadataResponsePartition {
        let mut p = MetadataResponsePartition::new();
        p.set_partition_index(partition);
        p.set_error_code(error.code());
        p
    }

    fn response_with_partition_data(entries: Vec<(TopicPartition, MetadataResponsePartition)>) -> ConcreteResponse {
        let mut data = MetadataResponseData::new();
        let mut topics: Vec<MetadataResponseTopic> = Vec::new();
        for (topic_partition, partition) in entries {
            let topic = topic_partition.topic().to_string();
            if let Some(existing) = topics.iter_mut().find(|t| t.name.as_deref() == Some(topic.as_str())) {
                existing.partitions.push(partition);
            } else {
                let mut t = MetadataResponseTopic::new();
                t.set_name(Some(topic));
                t.set_error_code(Errors::None.code());
                t.set_partitions(vec![partition]);
                topics.push(t);
            }
        }
        data.set_topics(topics);
        ConcreteResponse::Metadata(MetadataResponse::new(data, ApiKeys::METADATA.latest_version()))
    }

    fn response_with_topic_error(topic: &str, error: Errors) -> ConcreteResponse {
        let mut data = MetadataResponseData::new();
        let mut t = MetadataResponseTopic::new();
        t.set_name(Some(topic.to_string()));
        t.set_error_code(error.code());
        data.set_topics(vec![t]);
        ConcreteResponse::Metadata(MetadataResponse::new(data, ApiKeys::METADATA.latest_version()))
    }

    fn handle(keys: &[TopicPartition], response: &ConcreteResponse) -> LookupResult<TopicPartition> {
        let set: HashSet<TopicPartition> = keys.iter().cloned().collect();
        strategy().handle_response(&set, response)
    }

    #[test]
    fn build_lookup_request() {
        let s = strategy();
        let all: HashSet<TopicPartition> =
            [tp("foo", 0), tp("bar", 0), tp("foo", 1), tp("baz", 0)].into_iter().collect();
        let request = s.build_request(&all).build().unwrap();
        if let ConcreteRequestMatch::Metadata(topics, auto) = request_topics(&request) {
            assert_eq!(topics.into_iter().collect::<HashSet<_>>(), HashSet::from(["foo", "bar", "baz"]));
            assert!(!auto);
        } else {
            panic!("expected metadata request");
        }

        let foo_only: HashSet<TopicPartition> = all.iter().filter(|t| t.topic() == "foo").cloned().collect();
        let partial = s.build_request(&foo_only).build().unwrap();
        if let ConcreteRequestMatch::Metadata(topics, auto) = request_topics(&partial) {
            assert_eq!(topics.into_iter().collect::<HashSet<_>>(), HashSet::from(["foo"]));
            assert!(!auto);
        } else {
            panic!("expected metadata request");
        }
    }

    enum ConcreteRequestMatch<'a> {
        Metadata(Vec<&'a str>, bool),
        Other,
    }

    fn request_topics(request: &crate::common::requests::ConcreteRequest) -> ConcreteRequestMatch<'_> {
        if let crate::common::requests::ConcreteRequest::Metadata(m) = request {
            ConcreteRequestMatch::Metadata(m.topics().unwrap_or_default(), m.allow_auto_topic_creation())
        } else {
            ConcreteRequestMatch::Other
        }
    }

    #[test]
    fn topic_authorization_failure() {
        let result = handle(
            &[tp("foo", 0)],
            &response_with_topic_error("foo", Errors::TopicAuthorizationFailed),
        );
        assert_eq!(
            result.failed_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([tp("foo", 0)])
        );
        let err = result.failed_keys.get(&tp("foo", 0)).unwrap();
        assert_eq!(
            err.message(),
            "Failed to fetch metadata for partition foo-0 due to topic authorization failure"
        );
        match err {
            Error::TopicAuthorization(e) => {
                assert_eq!(e.unauthorized_topics(), &HashSet::from(["foo".to_string()]));
            },
            other => panic!("expected TopicAuthorization, got {other:?}"),
        }
    }

    #[test]
    fn invalid_topic_error() {
        let result = handle(&[tp("foo", 0)], &response_with_topic_error("foo", Errors::InvalidTopicError));
        let err = result.failed_keys.get(&tp("foo", 0)).unwrap();
        assert_eq!(
            err.message(),
            "Failed to fetch metadata for partition foo-0 due to invalid topic `foo`"
        );
        match err {
            Error::InvalidTopic(e) => {
                assert_eq!(e.invalid_topics(), &HashSet::from(["foo".to_string()]));
            },
            other => panic!("expected InvalidTopic, got {other:?}"),
        }
    }

    #[test]
    fn unexpected_topic_error() {
        let result = handle(&[tp("foo", 0)], &response_with_topic_error("foo", Errors::UnknownServerError));
        let err = result.failed_keys.get(&tp("foo", 0)).unwrap();
        assert_eq!(err.error(), Errors::UnknownServerError);
    }

    #[test]
    fn retriable_topic_errors() {
        for error in [
            Errors::UnknownTopicOrPartition,
            Errors::LeaderNotAvailable,
            Errors::BrokerNotAvailable,
        ] {
            let result = handle(&[tp("foo", 0)], &response_with_topic_error("foo", error));
            assert!(result.failed_keys.is_empty());
            assert!(result.mapped_keys.is_empty());
        }
    }

    #[test]
    fn retriable_partition_errors() {
        for error in [
            Errors::NotLeaderOrFollower,
            Errors::ReplicaNotAvailable,
            Errors::LeaderNotAvailable,
            Errors::BrokerNotAvailable,
            Errors::KafkaStorageError,
        ] {
            let response = response_with_partition_data(vec![(tp("foo", 0), partition_with_error(0, error))]);
            let result = handle(&[tp("foo", 0)], &response);
            assert!(result.failed_keys.is_empty());
            assert!(result.mapped_keys.is_empty());
        }
    }

    #[test]
    fn unexpected_partition_error() {
        let response =
            response_with_partition_data(vec![(tp("foo", 0), partition_with_error(0, Errors::UnknownServerError))]);
        let result = handle(&[tp("foo", 0)], &response);
        let err = result.failed_keys.get(&tp("foo", 0)).unwrap();
        assert_eq!(err.error(), Errors::UnknownServerError);
    }

    #[test]
    fn partition_successfully_mapped() {
        let response = response_with_partition_data(vec![
            (tp("foo", 0), partition_with_leader(0, 5, vec![5, 6, 7])),
            (tp("bar", 1), partition_with_leader(1, 1, vec![2, 1, 3])),
        ]);
        let result = handle(&[tp("foo", 0), tp("bar", 1)], &response);
        assert!(result.failed_keys.is_empty());
        assert_eq!(result.mapped_keys.get(&tp("foo", 0)), Some(&5));
        assert_eq!(result.mapped_keys.get(&tp("bar", 1)), Some(&1));
    }

    #[test]
    fn ignore_unrequested_partitions() {
        let response = response_with_partition_data(vec![
            (tp("foo", 0), partition_with_leader(0, 5, vec![5, 6, 7])),
            (tp("foo", 1), partition_with_error(1, Errors::UnknownServerError)),
        ]);
        let result = handle(&[tp("foo", 0)], &response);
        assert!(result.failed_keys.is_empty());
        assert_eq!(
            result.mapped_keys.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([tp("foo", 0)])
        );
        assert_eq!(result.mapped_keys.get(&tp("foo", 0)), Some(&5));
    }

    /// Regression for finding 244: Java's closure parameter is `tp` and it fills
    /// the *partition* slot, while the topic appears only inside the backticks:
    /// `topicError.exception("Failed to fetch metadata for partition " + tp + " because metadata for topic \`" + topic + "\` could not be found")`
    /// (`PartitionLeaderStrategy.java:88-90`). Printing the topic in both slots
    /// left every per-partition future with an indistinguishable message.
    #[test]
    fn intolerant_unknown_topic_names_the_partition() {
        let strategy = PartitionLeaderStrategy::with_tolerate_unknown_topics(LogContext::new("[test] "), false);
        let keys: HashSet<TopicPartition> = [tp("foo", 0), tp("foo", 1)].into_iter().collect();
        let result =
            strategy.handle_response(&keys, &response_with_topic_error("foo", Errors::UnknownTopicOrPartition));

        assert_eq!(result.failed_keys.keys().cloned().collect::<HashSet<_>>(), keys);
        for partition in [0, 1] {
            let err = result.failed_keys.get(&tp("foo", partition)).unwrap();
            assert_eq!(
                err.message(),
                format!(
                    "Failed to fetch metadata for partition foo-{partition} because metadata for topic `foo` \
                     could not be found"
                )
            );
            assert_eq!(err.error(), Errors::UnknownTopicOrPartition);
        }
    }

    #[test]
    fn retry_if_leader_unknown() {
        let response = response_with_partition_data(vec![(tp("foo", 0), partition_with_leader(0, -1, vec![5, 6, 7]))]);
        let result = handle(&[tp("foo", 0)], &response);
        assert!(result.failed_keys.is_empty());
        assert!(result.mapped_keys.is_empty());
    }
}
