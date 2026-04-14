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
//! Corresponds to `org.apache.kafka.clients.producer.internals.ProducerMetadata`.
//!
//! `ProducerMetadata` wraps the base `Metadata` via composition (Java uses
//! inheritance: `ProducerMetadata extends Metadata`). It adds:
//!
//! - Topic idle expiry: topics not accessed for `metadata_idle_ms` are removed
//! - New-topic tracking: newly added topics trigger partial metadata updates
//! - `await_update()`: async wait until the metadata version advances
//! - Topic-specific metadata request builders

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use log::debug;

use crate::clients::metadata::Metadata;
use crate::common::internals::ClusterResourceListeners;
use crate::common::protocol::Errors;
use crate::common::requests::metadata_request::MetadataRequestBuilder;
use crate::common::requests::metadata_response::MetadataResponse;
use crate::errors::ErrorCode;

/// Producer-specific metadata management.
///
/// Wraps `Metadata` and adds topic idle expiry, new-topic tracking, and
/// the ability to wait for metadata version changes.
///
/// Thread safety: the inner `topics` and `new_topics` maps are protected by
/// a `Mutex`, matching Java's `synchronized` methods.
///
/// Corresponds to `org.apache.kafka.clients.producer.internals.ProducerMetadata`.
pub struct ProducerMetadata {
    /// The underlying `Metadata` instance (shared with `NetworkClient`'s
    /// `DefaultMetadataUpdater`).
    metadata: Arc<Metadata>,
    /// Topics with their expiry timestamps.
    ///
    /// When a topic is added, its expiry is set to `now + metadata_idle_ms`.
    /// Topics whose expiry has passed are removed during `retain_topic()`.
    topics: Arc<Mutex<HashMap<String, i64>>>,
    /// Topics that have been added but not yet received in a metadata response.
    ///
    /// These topics trigger partial metadata updates for faster first-fetch.
    new_topics: Arc<Mutex<HashSet<String>>>,
    /// Maximum idle time before a topic is removed from tracking.
    metadata_idle_ms: i64,
    /// Per-topic errors from the last metadata response.
    ///
    /// Shared with the `update_listener_fn` closure so that errors are
    /// captured regardless of whether the update comes through
    /// `update_with_current_request_version()` or directly through
    /// `Metadata.update()` (via `DefaultMetadataUpdater`).
    errors: Arc<Mutex<Option<HashMap<String, Errors>>>>,
}

impl ProducerMetadata {
    /// Creates a new `ProducerMetadata`.
    ///
    /// The underlying `Metadata` is created with appropriate overrides for
    /// topic retention, request builders, and update listeners.
    ///
    /// # Arguments
    /// * `refresh_backoff_ms` - Minimum time between metadata refreshes
    /// * `refresh_backoff_max_ms` - Maximum backoff between metadata refreshes
    /// * `metadata_expire_ms` - Maximum time before metadata is considered stale
    /// * `metadata_idle_ms` - Maximum idle time before a topic is removed
    /// * `cluster_resource_listeners` - Listeners notified of cluster resource updates
    pub fn new(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        metadata_idle_ms: i64,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self {
        let topics: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(HashMap::new()));
        let new_topics: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let errors: Arc<Mutex<Option<HashMap<String, Errors>>>> = Arc::new(Mutex::new(None));

        // Closures that capture Arc references to the topic maps.
        let topics_for_retain = Arc::clone(&topics);
        let new_topics_for_retain = Arc::clone(&new_topics);
        let retain_topic_fn: Box<crate::clients::metadata::RetainTopicFn> =
            Box::new(move |topic: &str, _is_internal: bool, now_ms: i64| -> bool {
                let mut topics = topics_for_retain.lock().unwrap();
                let expire_ms = topics.get(topic).copied();
                match expire_ms {
                    None => false,
                    Some(_) if new_topics_for_retain.lock().unwrap().contains(topic) => true,
                    Some(expiry) if expiry <= now_ms => {
                        debug!(
                            "Removing unused topic {} from the metadata list, expiryMs {} now {}",
                            topic, expiry, now_ms
                        );
                        topics.remove(topic);
                        false
                    },
                    Some(_) => true,
                }
            });

        let topics_for_builder = Arc::clone(&topics);
        let metadata_request_builder_fn: Box<dyn Fn() -> MetadataRequestBuilder + Send + Sync> = Box::new(move || {
            let topics = topics_for_builder.lock().unwrap();
            let topic_names: Vec<String> = topics.keys().cloned().collect();
            let topic_refs: Vec<&str> = topic_names.iter().map(|s| s.as_str()).collect();
            MetadataRequestBuilder::new(Some(&topic_refs), true)
        });

        let new_topics_for_builder = Arc::clone(&new_topics);
        let metadata_request_builder_for_new_topics_fn: Box<dyn Fn() -> Option<MetadataRequestBuilder> + Send + Sync> =
            Box::new(move || {
                let new_topics = new_topics_for_builder.lock().unwrap();
                if new_topics.is_empty() {
                    None
                } else {
                    let topic_names: Vec<String> = new_topics.iter().cloned().collect();
                    let topic_refs: Vec<&str> = topic_names.iter().map(|s| s.as_str()).collect();
                    Some(MetadataRequestBuilder::new(Some(&topic_refs), true))
                }
            });

        let new_topics_for_listener = Arc::clone(&new_topics);
        let errors_for_listener = Arc::clone(&errors);
        let update_listener_fn: Box<crate::clients::metadata::MetadataUpdateListenerFn> =
            Box::new(move |response: &MetadataResponse, _is_partial_update: bool| {
                // Store per-topic errors from the response, matching Java's
                // ProducerMetadata.update() which sets `this.errors = response.errors()`.
                // This is done in the listener (rather than only in
                // update_with_current_request_version) so errors are captured when
                // DefaultMetadataUpdater calls Metadata.update() directly.
                {
                    let mut errors = errors_for_listener.lock().unwrap();
                    *errors = Some(response.errors());
                }
                let mut new_topics = new_topics_for_listener.lock().unwrap();
                if !new_topics.is_empty() {
                    for topic_metadata in response.topic_metadata() {
                        new_topics.remove(topic_metadata.topic());
                    }
                }
            });

        let metadata = Arc::new(Metadata::with_overrides(
            refresh_backoff_ms,
            refresh_backoff_max_ms,
            metadata_expire_ms,
            cluster_resource_listeners,
            Some(retain_topic_fn),
            true, // enable_partial_updates (ProducerMetadata supports partial updates)
            Some(metadata_request_builder_fn),
            Some(metadata_request_builder_for_new_topics_fn),
            Some(update_listener_fn),
        ));

        ProducerMetadata { metadata, topics, new_topics, metadata_idle_ms, errors }
    }

    /// Add a topic to the set of topics being tracked.
    ///
    /// If the topic is new (not previously tracked), it is also added to the
    /// new-topics set, which triggers a partial metadata update for faster
    /// first-fetch.
    ///
    /// Corresponds to Java's `ProducerMetadata.add(String topic, long nowMs)`.
    pub fn add(&self, topic: &str, now_ms: i64) {
        let mut topics = self.topics.lock().unwrap();
        if topics.insert(topic.to_string(), now_ms + self.metadata_idle_ms).is_none() {
            // Topic is new -- add to new_topics set and request a partial update.
            let mut new_topics = self.new_topics.lock().unwrap();
            new_topics.insert(topic.to_string());
            drop(new_topics);
            drop(topics);
            self.metadata.request_update_for_new_topics();
        }
    }

    /// Request a metadata update for the given topic.
    ///
    /// If the topic is in the new-topics set, requests a partial update.
    /// Otherwise, requests a full update.
    ///
    /// Returns the current update version before the request.
    ///
    /// Corresponds to Java's `ProducerMetadata.requestUpdateForTopic(String topic)`.
    pub fn request_update_for_topic(&self, topic: &str) -> i32 {
        let new_topics = self.new_topics.lock().unwrap();
        if new_topics.contains(topic) {
            drop(new_topics);
            self.metadata.request_update_for_new_topics()
        } else {
            drop(new_topics);
            self.metadata.request_update(false)
        }
    }

    /// Returns the set of topics currently being tracked.
    ///
    /// Visible for testing.
    ///
    /// Corresponds to Java's `ProducerMetadata.topics()`.
    pub fn topics(&self) -> HashSet<String> {
        let topics = self.topics.lock().unwrap();
        topics.keys().cloned().collect()
    }

    /// Returns the set of new topics (not yet received in a metadata response).
    ///
    /// Visible for testing.
    ///
    /// Corresponds to Java's `ProducerMetadata.newTopics()`.
    pub fn new_topics_set(&self) -> HashSet<String> {
        let new_topics = self.new_topics.lock().unwrap();
        new_topics.clone()
    }

    /// Returns `true` if the given topic is currently being tracked.
    ///
    /// Corresponds to Java's `ProducerMetadata.containsTopic(String topic)`.
    pub fn contains_topic(&self, topic: &str) -> bool {
        let topics = self.topics.lock().unwrap();
        topics.contains_key(topic)
    }

    /// Wait for the metadata update version to advance beyond `last_version`.
    ///
    /// Returns `Ok(())` when the version has advanced, or `Err` on timeout,
    /// fatal error, or close.
    ///
    /// Corresponds to Java's `ProducerMetadata.awaitUpdate(int lastVersion, long timeoutMs)`.
    pub async fn await_update(&self, last_version: i32, timeout_ms: i64) -> crate::errors::Result<()> {
        let deadline = if timeout_ms <= 0 {
            tokio::time::Instant::now()
        } else {
            tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms as u64)
        };

        let notify = self.metadata.notify().clone();

        loop {
            // Check for fatal error first. Map the common::kafka_error::KafkaError
            // to errors::KafkaError.
            if let Err(fatal) = self.metadata.maybe_return_fatal_error() {
                return Err(crate::errors::KafkaError::new(ErrorCode::Unexpected, fatal.to_string()));
            }

            // Check if version has advanced or metadata is closed.
            if self.metadata.update_version() > last_version {
                return Ok(());
            }

            if self.metadata.is_closed() {
                return Err(crate::errors::KafkaError::new(
                    ErrorCode::Unexpected,
                    "Requested metadata update after close",
                ));
            }

            // Calculate remaining wait time.
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(crate::errors::KafkaError::new(
                    ErrorCode::TimedOut,
                    format!(
                        "Failed to update metadata after {} ms. If you see this error in tests, \
                         it might mean that the metadata was never updated or the timeout was too small.",
                        timeout_ms
                    ),
                ));
            }

            // Wait for notification or timeout.
            match tokio::time::timeout(remaining, notify.notified()).await {
                Ok(()) => {
                    // Notified -- loop to re-check conditions.
                },
                Err(_) => {
                    // Timeout -- will be caught at the top of the loop.
                },
            }
        }
    }

    /// Returns the error for the given topic from the last metadata response,
    /// or `None` if there was no error.
    ///
    /// Corresponds to Java's `ProducerMetadata.getError(String topic)`.
    pub fn get_error(&self, topic: &str) -> Option<Errors> {
        let errors = self.errors.lock().unwrap();
        errors.as_ref().and_then(|map| map.get(topic).copied())
    }

    /// Returns a reference to the underlying `Metadata` instance.
    ///
    /// Used by `KafkaProducer` and `Sender` to access base metadata methods
    /// like `fetch()`, `request_update()`, `update_version()`, etc.
    pub fn metadata(&self) -> &Arc<Metadata> {
        &self.metadata
    }

    /// Delegates to `Metadata.request_update()`.
    pub fn request_update(&self, reset_equivalent_response_backoff: bool) -> i32 {
        self.metadata.request_update(reset_equivalent_response_backoff)
    }

    /// Delegates to `Metadata.update_version()`.
    pub fn update_version(&self) -> i32 {
        self.metadata.update_version()
    }

    /// Delegates to `Metadata.update_requested()`.
    pub fn update_requested(&self) -> bool {
        self.metadata.update_requested()
    }

    /// Delegates to `Metadata.time_to_next_update()`.
    pub fn time_to_next_update(&self, now_ms: i64) -> i64 {
        self.metadata.time_to_next_update(now_ms)
    }

    /// Delegates to `Metadata.fetch()`.
    pub fn fetch(&self) -> crate::common::cluster::Cluster {
        self.metadata.fetch()
    }

    /// Delegates to `Metadata.fetch_metadata_snapshot()`.
    pub fn fetch_metadata_snapshot(&self) -> crate::clients::metadata_snapshot::MetadataSnapshot {
        self.metadata.fetch_metadata_snapshot()
    }

    /// Delegates to `Metadata.update_with_current_request_version()`.
    ///
    /// Per-topic errors are stored by the `update_listener_fn` callback which
    /// is invoked by `Metadata.update()`, ensuring errors are captured
    /// regardless of whether this method or `Metadata.update()` is called
    /// directly (e.g. by `DefaultMetadataUpdater`).
    pub fn update_with_current_request_version(
        &self,
        response: &MetadataResponse,
        is_partial_update: bool,
        now_ms: i64,
    ) {
        self.metadata
            .update_with_current_request_version(response, is_partial_update, now_ms);
    }

    /// Propagates a fatal error and wakes all waiters.
    ///
    /// Corresponds to Java's `ProducerMetadata.fatalError()` which calls
    /// `super.fatalError()` + `notifyAll()`.
    pub fn fatal_error(&self, error: crate::common::kafka_error::KafkaError) {
        self.metadata.fatal_error(error);
    }

    /// Close this metadata instance and wake all waiters.
    ///
    /// Corresponds to Java's `ProducerMetadata.close()` which calls
    /// `super.close()` + `notifyAll()`.
    pub fn close(&self) {
        self.metadata.close();
    }

    /// Delegates to `Metadata.is_closed()`.
    pub fn is_closed(&self) -> bool {
        self.metadata.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::request_test_utils;

    const REFRESH_BACKOFF_MS: i64 = 100;
    const REFRESH_BACKOFF_MAX_MS: i64 = 1000;
    const METADATA_EXPIRE_MS: i64 = 1000;
    const METADATA_IDLE_MS: i64 = 60_000;

    fn create_test_metadata() -> ProducerMetadata {
        ProducerMetadata::new(
            REFRESH_BACKOFF_MS,
            REFRESH_BACKOFF_MAX_MS,
            METADATA_EXPIRE_MS,
            METADATA_IDLE_MS,
            ClusterResourceListeners::new(),
        )
    }

    fn response_with_topics(topics: &HashSet<String>) -> MetadataResponse {
        let partition_counts: HashMap<String, i32> = topics.iter().map(|t| (t.clone(), 1)).collect();
        request_test_utils::metadata_update_with(1, &partition_counts)
    }

    fn response_with_current_topics(metadata: &ProducerMetadata) -> MetadataResponse {
        let topics = metadata.topics();
        response_with_topics(&topics)
    }

    /// Translated from Java's `ProducerMetadataTest.testMetadata()`.
    ///
    /// Tests basic add/contains/retain behavior and that metadata updates
    /// propagate correctly.
    #[tokio::test]
    async fn test_metadata() {
        let metadata = create_test_metadata();
        let mut time: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let topic = "my-topic";
        metadata.add(topic, time);

        metadata.update_with_current_request_version(&response_with_topics(&HashSet::new()), false, time);
        assert!(metadata.time_to_next_update(time) > 0, "No update needed.");
        metadata.request_update(true);
        assert!(metadata.time_to_next_update(time) > 0, "Still no update needed due to backoff");
        // Advance time past the backoff period.
        time +=
            (REFRESH_BACKOFF_MS as f64 * (1.0 + crate::clients::common_client_configs::RETRY_BACKOFF_JITTER)) as i64;
        assert_eq!(
            0,
            metadata.time_to_next_update(time),
            "Update needed now that backoff time expired"
        );

        // Spawn async tasks that wait for metadata updates, simulating Java's
        // asyncFetch threads.
        let md1 = Arc::new(metadata);
        let md2 = Arc::clone(&md1);
        let md3 = Arc::clone(&md1);
        let topic_owned = topic.to_string();
        let topic_owned2 = topic.to_string();

        let t1 = tokio::spawn(async move {
            while md2.fetch().partitions_for_topic(&topic_owned).is_empty() {
                let version = md2.request_update(false);
                md2.await_update(version, 500).await.unwrap();
            }
        });

        let t2 = tokio::spawn(async move {
            while md3.fetch().partitions_for_topic(&topic_owned2).is_empty() {
                let version = md3.request_update(false);
                md3.await_update(version, 500).await.unwrap();
            }
        });

        // Perform metadata updates while tasks are waiting, simulating
        // KafkaProducer's metadata update sequence.
        while !t1.is_finished() || !t2.is_finished() {
            if md1.time_to_next_update(time) == 0 {
                md1.update_with_current_request_version(&response_with_current_topics(&md1), false, time);
                time += (REFRESH_BACKOFF_MS as f64
                    * (1.0 + crate::clients::common_client_configs::RETRY_BACKOFF_JITTER))
                    as i64;
            }
            tokio::task::yield_now().await;
        }

        t1.await.unwrap();
        t2.await.unwrap();

        assert!(md1.time_to_next_update(time) > 0, "No update needed.");
        time += METADATA_EXPIRE_MS;
        assert_eq!(0, md1.time_to_next_update(time), "Update needed due to stale metadata.");
    }

    /// Translated from Java's `ProducerMetadataTest.testMetadataAwaitAfterClose()`.
    ///
    /// Tests that `await_update()` returns an error after `close()`.
    #[tokio::test]
    async fn test_metadata_await_after_close() {
        let metadata = create_test_metadata();
        let time: i64 = 0;
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(metadata.time_to_next_update(time) > 0, "No update needed.");

        metadata.close();

        let version = metadata.request_update(false);
        let result = metadata.await_update(version, 500).await;
        assert!(result.is_err(), "Should fail after close");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Requested metadata update after close"),
            "Error should mention close: {}",
            err
        );
    }

    /// Translated from Java's `ProducerMetadataTest.testMetadataUpdateWaitTime()`.
    ///
    /// Tests that `await_update()` with timeout 0 returns immediately with a
    /// timeout error, and that a longer timeout also eventually times out.
    #[tokio::test]
    async fn test_metadata_update_wait_time() {
        let metadata = create_test_metadata();
        let time: i64 = 0;
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(metadata.time_to_next_update(time) > 0, "No update needed.");

        // With timeout 0, should return immediately.
        let version = metadata.request_update(true);
        let result = metadata.await_update(version, 0).await;
        assert!(result.is_err(), "Should timeout with wait time 0");
        assert_eq!(result.unwrap_err().code(), ErrorCode::TimedOut);

        // With a small timeout, should also timeout (no updates happening).
        let version = metadata.request_update(true);
        let result = metadata.await_update(version, 100).await;
        assert!(result.is_err(), "Should timeout with small wait time");
        assert_eq!(result.unwrap_err().code(), ErrorCode::TimedOut);
    }

    /// Translated from Java's `ProducerMetadataTest.testTimeToNextUpdateOverwriteBackoff()`.
    ///
    /// Tests that adding a new topic allows immediate update, but adding an
    /// already-known topic does not.
    #[tokio::test]
    async fn test_time_to_next_update_overwrite_backoff() {
        let metadata = create_test_metadata();
        let now: i64 = 10000;

        // New topic added to fetch set and update requested. It should allow immediate update.
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, now);
        metadata.add("new-topic", now);
        assert_eq!(0, metadata.time_to_next_update(now));

        // Even though add is called, immediate update isn't necessary if the topic
        // set doesn't contain a new topic.
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, now);
        metadata.add("new-topic", now);
        assert_eq!(METADATA_EXPIRE_MS, metadata.time_to_next_update(now));

        // If the new set of topics contains a new topic then it should allow immediate update.
        metadata.add("another-new-topic", now);
        assert_eq!(0, metadata.time_to_next_update(now));
    }

    /// Translated from Java's `ProducerMetadataTest.testTopicExpiry()`.
    ///
    /// Tests that:
    /// 1. Topics expire after `metadata_idle_ms` of inactivity
    /// 2. Topics are retained if accessed within the idle interval
    /// 3. New topics are retained even past idle time until metadata arrives
    #[tokio::test]
    async fn test_topic_expiry() {
        let metadata = create_test_metadata();
        let mut time: i64 = 0;

        // Test 1: Topic expires after idle period.
        let topic1 = "topic1";
        metadata.add(topic1, time);
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(metadata.contains_topic(topic1));

        time += METADATA_IDLE_MS;
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(!metadata.contains_topic(topic1), "Unused topic not expired");

        // Test 2: Topic not expired if used within the idle interval.
        let topic2 = "topic2";
        metadata.add(topic2, time);
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        for _ in 0..3 {
            time += METADATA_IDLE_MS / 2;
            metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
            assert!(metadata.contains_topic(topic2), "Topic expired even though in use");
            metadata.add(topic2, time);
        }

        // Test 3: New topic retained even past idle time until metadata arrives.
        let topic3 = "topic3";
        metadata.add(topic3, time);
        time += METADATA_IDLE_MS * 2;
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(metadata.contains_topic(topic3), "Topic expired while awaiting metadata");
    }

    /// Translated from Java's `ProducerMetadataTest.testMetadataWaitAbortedOnFatalException()`.
    ///
    /// Tests that `await_update()` returns an error when a fatal error is set.
    #[tokio::test]
    async fn test_metadata_wait_aborted_on_fatal_error() {
        let metadata = create_test_metadata();
        metadata.fatal_error(crate::common::kafka_error::KafkaError::fatal(
            Errors::SaslAuthenticationFailed,
            "Fatal exception from test",
        ));
        let result = metadata.await_update(0, 1000).await;
        assert!(result.is_err(), "Should fail on fatal error");
    }

    /// Translated from Java's `ProducerMetadataTest.testMetadataPartialUpdate()`.
    ///
    /// Tests that new topics trigger partial updates and are correctly removed
    /// from the new-topics set after receiving metadata.
    #[tokio::test]
    async fn test_metadata_partial_update() {
        let metadata = create_test_metadata();
        let mut now: i64 = 10000;

        // Add a new topic and fetch its metadata in a partial update.
        let topic1 = "topic-one";
        metadata.add(topic1, now);
        assert!(metadata.update_requested());
        assert_eq!(0, metadata.time_to_next_update(now));
        assert_eq!(metadata.topics(), [topic1.to_string()].into_iter().collect::<HashSet<_>>());
        assert_eq!(
            metadata.new_topics_set(),
            [topic1.to_string()].into_iter().collect::<HashSet<_>>()
        );

        // Perform the partial update. Verify the topic is no longer considered "new".
        now += 1000;
        metadata.update_with_current_request_version(
            &response_with_topics(&[topic1.to_string()].into_iter().collect()),
            true,
            now,
        );
        assert!(!metadata.update_requested());
        assert_eq!(metadata.topics(), [topic1.to_string()].into_iter().collect::<HashSet<_>>());
        assert!(
            metadata.new_topics_set().is_empty(),
            "new_topics should be empty after partial update: {:?}",
            metadata.new_topics_set()
        );

        // Add the topic again. It should not be considered "new".
        metadata.add(topic1, now);
        assert!(!metadata.update_requested());
        assert!(metadata.time_to_next_update(now) > 0);
        assert_eq!(metadata.topics(), [topic1.to_string()].into_iter().collect::<HashSet<_>>());
        assert!(metadata.new_topics_set().is_empty());

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
            metadata.new_topics_set(),
            [topic2, topic3].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
        );

        // Perform the partial update for a subset of the new topics.
        now += 1000;
        assert!(metadata.update_requested());
        metadata.update_with_current_request_version(
            &response_with_topics(&[topic2.to_string()].into_iter().collect()),
            true,
            now,
        );
        assert_eq!(
            metadata.topics(),
            [topic1, topic2, topic3].iter().map(|s| s.to_string()).collect::<HashSet<_>>()
        );
        assert_eq!(
            metadata.new_topics_set(),
            [topic3.to_string()].into_iter().collect::<HashSet<_>>()
        );
    }

    /// Translated from Java's `ProducerMetadataTest.testRequestUpdateForTopic()`.
    ///
    /// Tests that `request_update_for_topic()` triggers partial vs full updates
    /// depending on whether the topic is new.
    #[tokio::test]
    async fn test_request_update_for_topic() {
        let metadata = create_test_metadata();
        let mut now: i64 = 10000;

        let topic1 = "topic-1";
        let topic2 = "topic-2";

        // Add the topics to the metadata.
        metadata.add(topic1, now);
        metadata.add(topic2, now);
        assert!(metadata.update_requested());

        // Request an update for topic1. Since the topic is considered new, it should
        // not trigger the metadata to require a full update.
        metadata.request_update_for_topic(topic1);
        assert!(metadata.update_requested());

        // Perform the partial update. Verify no additional (full) updates are requested.
        now += 1000;
        metadata.update_with_current_request_version(
            &response_with_topics(&[topic1.to_string()].into_iter().collect()),
            true,
            now,
        );
        assert!(!metadata.update_requested());

        // Request an update for topic1 again. Such a request may occur when the leader
        // changes, which may affect many topics, and should therefore request a full update.
        metadata.request_update_for_topic(topic1);
        assert!(metadata.update_requested());

        // Perform a partial update for the topic. This should not clear the full update.
        now += 1000;
        metadata.update_with_current_request_version(
            &response_with_topics(&[topic1.to_string()].into_iter().collect()),
            true,
            now,
        );
        assert!(metadata.update_requested());

        // Perform the full update. This should clear the update request.
        now += 1000;
        metadata.update_with_current_request_version(
            &response_with_topics(&[topic1.to_string(), topic2.to_string()].into_iter().collect()),
            false,
            now,
        );
        assert!(!metadata.update_requested());
    }

    /// Translated from Java's `ProducerMetadataTest.testAwaitUpdate()` pattern
    /// (implicit in testMetadata).
    ///
    /// Tests that `await_update()` returns when the version advances.
    #[tokio::test]
    async fn test_await_update() {
        let metadata = Arc::new(create_test_metadata());
        let md_for_update = Arc::clone(&metadata);

        let version = metadata.update_version();

        // Spawn a task that waits for the update.
        let wait_handle = tokio::spawn(async move { metadata.await_update(version, 5000).await });

        // Give the wait task time to start.
        tokio::task::yield_now().await;

        // Perform the update.
        md_for_update.update_with_current_request_version(&response_with_topics(&HashSet::new()), false, 0);

        // The wait should complete.
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), wait_handle)
            .await
            .expect("await_update should complete")
            .expect("task should not panic");

        assert!(result.is_ok(), "await_update should succeed after update");
    }

    /// Translated from Java's `ProducerMetadataTest.testMetadataEquivalentResponsesBackoff()`.
    ///
    /// Tests that equivalent metadata responses trigger exponential backoff.
    #[tokio::test]
    async fn test_metadata_equivalent_responses_backoff() {
        let metadata = create_test_metadata();
        let mut time: i64 = 0;

        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(metadata.time_to_next_update(time) > 0, "No update needed");
        metadata.request_update(false);
        assert!(metadata.time_to_next_update(time) > 0, "Still no update needed due to backoff");
        time +=
            (REFRESH_BACKOFF_MS as f64 * (1.0 + crate::clients::common_client_configs::RETRY_BACKOFF_JITTER)) as i64;
        metadata.update_with_current_request_version(&response_with_current_topics(&metadata), false, time);
        assert!(
            metadata.time_to_next_update(time) > 0,
            "No update needed after equivalent metadata response"
        );
        metadata.request_update(false);
        assert!(metadata.time_to_next_update(time) > 0, "Still no update needed due to backoff");
        assert!(
            metadata.time_to_next_update(time + REFRESH_BACKOFF_MS) > 0,
            "Still no update needed due to exponential backoff"
        );
        time += (REFRESH_BACKOFF_MS as f64
            * crate::clients::common_client_configs::RETRY_BACKOFF_EXP_BASE as f64
            * (1.0 + crate::clients::common_client_configs::RETRY_BACKOFF_JITTER)) as i64;
        assert_eq!(
            0,
            metadata.time_to_next_update(time),
            "Update needed now that backoff time expired"
        );

        metadata.close();
        let version = metadata.request_update(false);
        let result = metadata.await_update(version, 500).await;
        assert!(result.is_err(), "Should fail after close");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Requested metadata update after close"),
        );
    }
}
