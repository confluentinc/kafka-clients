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

//! Producer-specific metadata management.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.ProducerMetadata`.
//!
//! Extends [`Metadata`] with topic-level expiry tracking and new topic awareness.
//! Topics are cached with expiry timestamps, and metadata requests are scoped to
//! only the topics the producer cares about (rather than all topics).

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use log::debug;

use crate::common::internals::ClusterResourceListeners;
use crate::common::protocol::Errors;
use crate::common::requests::MetadataRequestBuilder;
use crate::common::requests::MetadataResponse;
use crate::common::utils::LogContext;
use crate::metadata::Metadata;

/// Producer-specific inner state, protected by its own mutex.
///
/// This state is separate from `Metadata`'s inner state to avoid lock ordering
/// issues. The `retain_topic_fn` and `request_builder_fn` closures capture a
/// reference to this mutex and read from it while `Metadata`'s inner lock is held.
struct ProducerMetadataInner {
    /// Topics with their expiry timestamps (now_ms + metadata_idle_ms at time of add).
    topics: HashMap<String, i64>,
    /// Topics that have been recently added and not yet confirmed in a metadata response.
    new_topics: HashSet<String>,
    /// The per-topic errors from the last metadata response.
    errors: Option<HashMap<String, Errors>>,
    /// The idle time after which an unused topic is removed from the metadata cache.
    metadata_idle_ms: i64,
}

/// Producer metadata — extends [`Metadata`] with topic expiry tracking.
///
/// Translated from `org.apache.kafka.clients.producer.internals.ProducerMetadata`.
///
/// In Java, this is a subclass of `Metadata`. In Rust, we use composition: a
/// `ProducerMetadata` wraps a `Metadata` and adds producer-specific state in a
/// separate mutex-protected inner struct. The `Metadata` hooks
/// (`retain_topic_fn`, `request_builder_fn`, `new_topics_request_builder_fn`)
/// are set at construction to delegate to the producer state.
pub struct ProducerMetadata {
    /// The underlying metadata instance, wrapped in `Arc` so it can be shared
    /// with the `NetworkClient` (which also needs `Arc<Metadata>`). This mirrors
    /// Java's inheritance model where `ProducerMetadata extends Metadata` and
    /// both the producer and network client use the same object.
    metadata: Arc<Metadata>,
    /// Producer-specific mutable state.
    inner: Arc<Mutex<ProducerMetadataInner>>,
}

impl ProducerMetadata {
    /// Create a new `ProducerMetadata`.
    ///
    /// # Arguments
    /// * `refresh_backoff_ms` - The minimum amount of time between metadata refreshes
    /// * `refresh_backoff_max_ms` - The maximum amount of time to wait between metadata refreshes
    /// * `metadata_expire_ms` - The maximum amount of time that metadata can be retained
    /// * `metadata_idle_ms` - If a topic hasn't been accessed for this many ms, it is removed
    /// * `cluster_resource_listeners` - Listeners notified of cluster resource updates
    pub fn new(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        metadata_idle_ms: i64,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self {
        Self::with_log_context(
            refresh_backoff_ms,
            refresh_backoff_max_ms,
            metadata_expire_ms,
            metadata_idle_ms,
            cluster_resource_listeners,
            LogContext::empty(),
        )
    }

    /// Creates a new `ProducerMetadata` with a `LogContext`.
    ///
    /// # Arguments
    /// * `refresh_backoff_ms` - The minimum amount of time between metadata refreshes
    /// * `refresh_backoff_max_ms` - The maximum amount of time to wait between metadata
    ///   refreshes
    /// * `metadata_expire_ms` - The maximum amount of time that metadata can be retained
    /// * `metadata_idle_ms` - The idle time after which an unused topic is removed
    /// * `cluster_resource_listeners` - Listeners notified of cluster resource updates
    /// * `log_context` - Contextual log message prefix
    pub fn with_log_context(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        metadata_idle_ms: i64,
        cluster_resource_listeners: ClusterResourceListeners,
        log_context: LogContext,
    ) -> Self {
        let inner = Arc::new(Mutex::new(ProducerMetadataInner {
            topics: HashMap::new(),
            new_topics: HashSet::new(),
            errors: None,
            metadata_idle_ms,
        }));

        // Closure for retain_topic_fn: checks the topics map and removes expired entries
        let retain_inner = Arc::clone(&inner);
        let retain_topic_fn = Box::new(move |topic: &str, _is_internal: bool, now_ms: i64| -> bool {
            let mut state = retain_inner.lock().unwrap();
            let expire_ms = state.topics.get(topic).copied();
            match expire_ms {
                None => false,
                Some(_) if state.new_topics.contains(topic) => true,
                Some(expiry) if expiry <= now_ms => {
                    debug!(
                        "Removing unused topic {} from the metadata list, expiryMs {} now {}",
                        topic, expiry, now_ms
                    );
                    state.topics.remove(topic);
                    false
                },
                Some(_) => true,
            }
        });

        // Closure for request_builder_fn: returns a builder with just the known topics
        let builder_inner = Arc::clone(&inner);
        let request_builder_fn: Box<dyn Fn() -> MetadataRequestBuilder + Send + Sync> = Box::new(move || {
            let state = builder_inner.lock().unwrap();
            let topics: Vec<&str> = state.topics.keys().map(|s| s.as_str()).collect();
            MetadataRequestBuilder::new(Some(&topics), true)
        });

        // Closure for new_topics_request_builder_fn: returns a builder with just the new topics
        let new_topics_inner = Arc::clone(&inner);
        let new_topics_request_builder_fn: Box<dyn Fn() -> MetadataRequestBuilder + Send + Sync> =
            Box::new(move || {
                let state = new_topics_inner.lock().unwrap();
                let topics: Vec<&str> = state.new_topics.iter().map(|s| s.as_str()).collect();
                MetadataRequestBuilder::new(Some(&topics), true)
            });

        // Closure for post_update_fn: tracks per-topic errors and removes confirmed
        // topics from the new_topics set. This corresponds to Java's
        // ProducerMetadata.update() override of Metadata.update().
        let post_update_inner = Arc::clone(&inner);
        let post_update_fn = Box::new(move |response: &MetadataResponse, _is_partial: bool, _now_ms: i64| {
            let mut state = post_update_inner.lock().unwrap();
            state.errors = Some(response.errors());

            // Remove all topics in the response that are in the new topic set. Note
            // that if an error was encountered for a new topic's metadata, then any
            // work to resolve the error will include the topic in a full metadata update.
            if !state.new_topics.is_empty() {
                for topic_metadata in response.topic_metadata() {
                    state.new_topics.remove(&topic_metadata.topic);
                }
            }
        });

        let metadata = Arc::new(Metadata::with_overrides(
            refresh_backoff_ms,
            refresh_backoff_max_ms,
            metadata_expire_ms,
            cluster_resource_listeners,
            crate::metadata::MetadataOverrides {
                retain_topic_fn: Some(retain_topic_fn),
                enable_partial_updates: true,
                request_builder_fn: Some(request_builder_fn),
                new_topics_request_builder_fn: Some(new_topics_request_builder_fn),
                post_update_fn: Some(post_update_fn),
            },
            log_context,
        ));

        Self { metadata, inner }
    }

    /// Add a topic to the metadata cache with the given timestamp.
    ///
    /// If the topic is new, triggers a metadata update for new topics.
    pub fn add(&self, topic: &str, now_ms: i64) {
        let mut state = self.inner.lock().unwrap();
        let idle_ms = state.metadata_idle_ms;
        let previous = state.topics.insert(topic.to_string(), now_ms + idle_ms);
        if previous.is_none() {
            state.new_topics.insert(topic.to_string());
            // Drop lock before calling metadata method to avoid potential deadlock
            drop(state);
            self.metadata.request_update_for_new_topics();
        }
    }

    /// Request a metadata update for a specific topic.
    ///
    /// If the topic is a new topic, triggers a partial update. Otherwise triggers
    /// a full update.
    pub fn request_update_for_topic(&self, topic: &str) -> i32 {
        let state = self.inner.lock().unwrap();
        let is_new = state.new_topics.contains(topic);
        drop(state);

        if is_new {
            self.metadata.request_update_for_new_topics()
        } else {
            self.metadata.request_update(false)
        }
    }

    /// Returns the set of all tracked topic names (visible for testing).
    pub fn topics(&self) -> HashSet<String> {
        let state = self.inner.lock().unwrap();
        state.topics.keys().cloned().collect()
    }

    /// Returns the set of new (unconfirmed) topic names (visible for testing).
    pub fn new_topics(&self) -> HashSet<String> {
        let state = self.inner.lock().unwrap();
        state.new_topics.clone()
    }

    /// Returns whether the given topic is in the metadata cache.
    pub fn contains_topic(&self, topic: &str) -> bool {
        let state = self.inner.lock().unwrap();
        state.topics.contains_key(topic)
    }

    /// Get the error for a specific topic from the last metadata response.
    pub fn get_error(&self, topic: &str) -> Option<Errors> {
        let state = self.inner.lock().unwrap();
        state.errors.as_ref().and_then(|e| e.get(topic).copied())
    }

    /// Returns a shared reference to the underlying [`Metadata`].
    ///
    /// This `Arc` can be passed to the [`NetworkClient`] so that metadata updates
    /// from the network layer are visible to the producer, mirroring Java's
    /// inheritance model where `ProducerMetadata extends Metadata`.
    ///
    /// [`NetworkClient`]: crate::network_client::NetworkClient
    pub fn metadata_arc(&self) -> Arc<Metadata> {
        Arc::clone(&self.metadata)
    }
}

impl Deref for ProducerMetadata {
    type Target = Metadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Node;
    use crate::common::protocol::ApiKeys;
    use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseData, MetadataResponseTopic};

    const REFRESH_BACKOFF_MS: i64 = 100;
    const REFRESH_BACKOFF_MAX_MS: i64 = 1000;
    const METADATA_EXPIRE_MS: i64 = 60_000;
    const METADATA_IDLE_MS: i64 = 300_000;

    fn new_producer_metadata() -> ProducerMetadata {
        ProducerMetadata::new(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            METADATA_IDLE_MS,
            ClusterResourceListeners::new(),
        )
    }

    fn make_metadata_response(topics: &[&str], nodes: &[Node]) -> MetadataResponse {
        let mut data = MetadataResponseData::new();
        data.set_controller_id(if nodes.is_empty() { -1 } else { nodes[0].id() });
        data.set_cluster_id(Some("test-cluster-id".to_string()));

        let broker_list: Vec<MetadataResponseBroker> = nodes
            .iter()
            .map(|n| {
                let mut broker = MetadataResponseBroker::new();
                broker.set_node_id(n.id());
                broker.set_host(n.host().to_string());
                broker.set_port(n.port());
                broker
            })
            .collect();
        data.set_brokers(broker_list);

        let topic_list: Vec<MetadataResponseTopic> = topics
            .iter()
            .map(|t| {
                let mut topic = MetadataResponseTopic::new();
                topic.set_name(Some(t.to_string()));
                topic.set_error_code(Errors::None.code());
                topic.set_is_internal(false);
                topic.set_partitions(Vec::new());
                topic
            })
            .collect();
        data.set_topics(topic_list);

        MetadataResponse::new(data, ApiKeys::METADATA.latest_version())
    }

    /// Translated from `ProducerMetadataTest.testTimeToNextUpdateOverwriteBackoff`.
    #[test]
    fn test_time_to_next_update_overwrite_backoff() {
        let now: i64 = 10_000;
        let metadata = new_producer_metadata();

        // New topic added to fetch set and update requested. It should allow immediate update.
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, now);
        metadata.add("new-topic", now);
        assert_eq!(0, metadata.time_to_next_update(now));

        // Even though add is called, immediate update isn't necessary if the new topic set
        // isn't containing a new topic.
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, now);
        metadata.add("new-topic", now);
        assert_eq!(METADATA_EXPIRE_MS, metadata.time_to_next_update(now));

        // If the new set of topics containing a new topic then it should allow immediate update.
        metadata.add("another-new-topic", now);
        assert_eq!(0, metadata.time_to_next_update(now));
    }

    /// Translated from `ProducerMetadataTest.testTopicExpiry`.
    #[test]
    fn test_topic_expiry() {
        let mut time: i64 = 0;
        let metadata = new_producer_metadata();
        let topic1 = "topic1";

        // Test that topic is expired if not used within the expiry interval
        metadata.add(topic1, time);
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(metadata.contains_topic(topic1));

        time += METADATA_IDLE_MS;
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(!metadata.contains_topic(topic1), "Unused topic not expired");

        // Test that topic is not expired if used within the expiry interval
        let topic2 = "topic2";
        metadata.add(topic2, time);
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        for _ in 0..3 {
            time += METADATA_IDLE_MS / 2;
            metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
            assert!(metadata.contains_topic(topic2), "Topic expired even though in use");
            metadata.add(topic2, time);
        }

        // Add a new topic, but update its metadata after the expiry would have occurred.
        // The topic should still be retained.
        let topic3 = "topic3";
        metadata.add(topic3, time);
        time += METADATA_IDLE_MS * 2;
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(metadata.contains_topic(topic3), "Topic expired while awaiting metadata");
    }

    /// Translated from `ProducerMetadataTest.testMetadataPartialUpdate`.
    #[test]
    fn test_metadata_partial_update() {
        let mut now: i64 = 10_000;
        let metadata = new_producer_metadata();

        // Add a new topic and fetch its metadata in a partial update.
        let topic1 = "topic-one";
        metadata.add(topic1, now);
        assert!(metadata.update_requested());
        assert_eq!(0, metadata.time_to_next_update(now));
        assert_eq!(metadata.topics(), [topic1.to_string()].into_iter().collect::<HashSet<_>>());
        assert_eq!(metadata.new_topics(), [topic1.to_string()].into_iter().collect::<HashSet<_>>());

        // Perform the partial update. Verify the topic is no longer considered "new".
        now += 1000;
        metadata.update_with_current_request_version(&response_with_topics(&[topic1]), true, now);
        assert!(!metadata.update_requested());
        assert_eq!(metadata.topics(), [topic1.to_string()].into_iter().collect::<HashSet<_>>());
        assert!(metadata.new_topics().is_empty());

        // Add the topic again. It should not be considered "new".
        metadata.add(topic1, now);
        assert!(!metadata.update_requested());
        assert!(metadata.time_to_next_update(now) > 0);
        assert_eq!(metadata.topics(), [topic1.to_string()].into_iter().collect::<HashSet<_>>());
        assert!(metadata.new_topics().is_empty());

        // Add two new topics. However, we'll only apply a partial update for one of them.
        now += 1000;
        let topic2 = "topic-two";
        metadata.add(topic2, now);

        now += 1000;
        let topic3 = "topic-three";
        metadata.add(topic3, now);

        assert!(metadata.update_requested());
        assert_eq!(0, metadata.time_to_next_update(now));
        assert_eq!(
            metadata.topics(),
            [topic1, topic2, topic3].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
        );
        assert_eq!(
            metadata.new_topics(),
            [topic2, topic3].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
        );

        // Perform the partial update for a subset of the new topics.
        now += 1000;
        assert!(metadata.update_requested());
        metadata.update_with_current_request_version(&response_with_topics(&[topic2]), true, now);
        assert_eq!(
            metadata.topics(),
            [topic1, topic2, topic3].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
        );
        assert_eq!(metadata.new_topics(), [topic3.to_string()].into_iter().collect::<HashSet<_>>());
    }

    /// Translated from `ProducerMetadataTest.testRequestUpdateForTopic`.
    #[test]
    fn test_request_update_for_topic() {
        let mut now: i64 = 10_000;
        let metadata = new_producer_metadata();
        let topic1 = "topic-1";
        let topic2 = "topic-2";

        // Add the topics to the metadata.
        metadata.add(topic1, now);
        metadata.add(topic2, now);
        assert!(metadata.update_requested());

        // Request an update for topic1. Since the topic is considered new, it should not trigger
        // the metadata to require a full update.
        metadata.request_update_for_topic(topic1);
        assert!(metadata.update_requested());

        // Perform the partial update. Verify no additional (full) updates are requested.
        now += 1000;
        metadata.update_with_current_request_version(&response_with_topics(&[topic1]), true, now);
        assert!(!metadata.update_requested());

        // Request an update for topic1 again. Such a request may occur when the leader
        // changes, which may affect many topics, and should therefore request a full update.
        metadata.request_update_for_topic(topic1);
        assert!(metadata.update_requested());

        // Perform a partial update for the topic. This should not clear the full update.
        now += 1000;
        metadata.update_with_current_request_version(&response_with_topics(&[topic1]), true, now);
        assert!(metadata.update_requested());

        // Perform the full update. This should clear the update request.
        now += 1000;
        metadata.update_with_current_request_version(&response_with_topics(&[topic1, topic2]), false, now);
        assert!(!metadata.update_requested());
    }

    #[test]
    fn test_contains_topic() {
        let now: i64 = 10_000;
        let metadata = new_producer_metadata();

        assert!(!metadata.contains_topic("topic1"));
        metadata.add("topic1", now);
        assert!(metadata.contains_topic("topic1"));
    }

    #[test]
    fn test_get_error() {
        let now: i64 = 10_000;
        let metadata = new_producer_metadata();

        // Before any update, errors should be None
        assert!(metadata.get_error("topic1").is_none());

        metadata.add("topic1", now);
        let response = response_with_topics(&["topic1"]);
        metadata.update_with_current_request_version(&response, false, now);

        // After update with Errors::None, get_error should return None
        // because Java's MetadataResponse.errors() only includes non-NONE errors.
        assert!(metadata.get_error("topic1").is_none());

        // Test with an actual error
        metadata.add("error-topic", now);
        let response = response_with_error_topics(&[("error-topic", Errors::LeaderNotAvailable)]);
        metadata.update_with_current_request_version(&response, false, now);

        let error = metadata.get_error("error-topic");
        assert!(error.is_some());
        assert_eq!(error.unwrap(), Errors::LeaderNotAvailable);
    }

    /// Test that a new topic is not expired even if time has passed beyond idle.
    #[test]
    fn test_new_topic_not_expired() {
        let now: i64 = 10_000;
        let metadata = new_producer_metadata();

        metadata.add("new-topic", now);

        // Even if time passes beyond idle, new topics should be retained
        let expired_time = now + METADATA_IDLE_MS + 1;

        // Response does NOT include the new topic, so it stays in new_topics
        let response = response_with_topics(&[]);
        metadata.request_update(true);
        metadata.update_with_current_request_version(&response, false, expired_time);

        // The topic should still be in the topics map because it's in new_topics
        assert!(metadata.contains_topic("new-topic"), "new topic should not be expired");
    }

    /// Helper: create a metadata response with the given topic names.
    fn response_with_topics(topics: &[&str]) -> MetadataResponse {
        let node = Node::new(0, "localhost".to_string(), 9092);
        make_metadata_response(topics, &[node])
    }

    /// Helper: create a metadata response with the given topic names and error codes.
    fn response_with_error_topics(topics: &[(&str, Errors)]) -> MetadataResponse {
        let node = Node::new(0, "localhost".to_string(), 9092);
        let mut data = MetadataResponseData::new();
        data.set_controller_id(node.id());
        data.set_cluster_id(Some("test-cluster-id".to_string()));

        let mut broker = MetadataResponseBroker::new();
        broker.set_node_id(node.id());
        broker.set_host(node.host().to_string());
        broker.set_port(node.port());
        data.set_brokers(vec![broker]);

        let topic_list: Vec<MetadataResponseTopic> = topics
            .iter()
            .map(|(t, err)| {
                let mut topic = MetadataResponseTopic::new();
                topic.set_name(Some(t.to_string()));
                topic.set_error_code(err.code());
                topic.set_is_internal(false);
                topic.set_partitions(Vec::new());
                topic
            })
            .collect();
        data.set_topics(topic_list);

        MetadataResponse::new(data, ApiKeys::METADATA.latest_version())
    }

    /// Helper: create a metadata response with the metadata's current topic set.
    fn response_with_current_topics(metadata: &ProducerMetadata) -> MetadataResponse {
        let topics = metadata.topics();
        let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
        response_with_topics(&topic_refs)
    }
}
