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

//! Translation of `org.apache.kafka.clients.producer.internals.ProducerMetadata`.

#![allow(dead_code)] // Phase 6e (KafkaProducer) wires the public surface.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::debug;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::common::errors::KafkaError;
use crate::common::internals::cluster_resource_listeners::ClusterResourceListeners;
use crate::common::protocol::Errors;
use crate::common::requests::metadata_response::MetadataResponse;
use crate::common::utils::LogContext;
use crate::common::utils::Time;
use crate::metadata::Metadata;

/// Producer-side extension of [`Metadata`].
///
/// Java's `ProducerMetadata extends Metadata` — Rust uses composition.
/// This struct holds an `Arc<Metadata>` and adds:
/// - per-topic expiry tracking (`topics`, `metadata_idle_ms`),
/// - a `new_topics` set used for partial metadata updates,
/// - a `retain_topic` predicate installed on the inner [`Metadata`] so
///   the parent's `update()` only keeps partition data for topics we care
///   about.
///
/// Visibility: `pub(crate)` per CLAUDE.md rule for `internals` packages.
pub(crate) struct ProducerMetadata {
    metadata: Arc<Metadata>,
    /// All mutable producer-side state under one mutex (mirrors Java's
    /// `synchronized` monitor on the `ProducerMetadata` instance for
    /// these fields).
    state: Arc<Mutex<ProducerState>>,
    /// `notifyAll()` from Java translates to a tokio `Notify` waker.
    notify: Arc<Notify>,
    metadata_idle_ms: i64,
    log_context: LogContext,
    time: Arc<dyn Time>,
}

/// Mutable state owned by [`ProducerMetadata`].
struct ProducerState {
    /// Topics with their expiry deadline (millisecond wall-clock).
    topics: HashMap<String, i64>,
    /// Topics added since the last metadata update — these are still
    /// "new" until the response comes back including them.
    new_topics: HashSet<String>,
    /// Errors keyed by topic from the last metadata response, populated
    /// by `update`.
    errors: HashMap<String, Errors>,
}

impl ProducerMetadata {
    /// Mirrors Java's
    /// `ProducerMetadata(long, long, long, long, LogContext, ClusterResourceListeners, Time)`.
    pub(crate) fn new(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        metadata_idle_ms: i64,
        log_context: LogContext,
        cluster_resource_listeners: Arc<ClusterResourceListeners>,
        time: Arc<dyn Time>,
    ) -> Result<Arc<Self>, KafkaError> {
        let metadata = Arc::new(Metadata::new(
            refresh_backoff_ms,
            refresh_backoff_max_ms,
            metadata_expire_ms,
            log_context.clone(),
            cluster_resource_listeners,
        )?);

        let state = Arc::new(Mutex::new(ProducerState {
            topics: HashMap::new(),
            new_topics: HashSet::new(),
            errors: HashMap::new(),
        }));

        let notify = metadata.notify_handle();
        let producer = Arc::new(ProducerMetadata { metadata, state, notify, metadata_idle_ms, log_context, time });

        // Install the `retainTopic` predicate on the inner `Metadata`.
        // Java's `ProducerMetadata.retainTopic(String, boolean, long)`
        // override is invoked for every topic in `Metadata.update` to
        // decide whether to retain partition data. We replicate by
        // installing a closure that captures the producer's state.
        let state_for_predicate = Arc::clone(&producer.state);
        let log_context_for_predicate = producer.log_context.clone();
        producer
            .metadata
            .set_retain_topic_fn(Arc::new(move |topic, _topic_id, _is_internal, now_ms| {
                ProducerMetadata::retain_topic_inner(&state_for_predicate, &log_context_for_predicate, topic, now_ms)
            }));

        Ok(producer)
    }

    /// Borrow the underlying [`Metadata`] handle (a clone of the `Arc`).
    pub(crate) fn metadata(&self) -> Arc<Metadata> {
        Arc::clone(&self.metadata)
    }

    /// Mirrors `add(String, long)`.
    pub(crate) fn add(&self, topic: &str, now_ms: i64) {
        let mut state = self.state.lock().expect("producer metadata mutex poisoned");
        let prior = state.topics.insert(topic.to_owned(), now_ms + self.metadata_idle_ms);
        if prior.is_none() {
            state.new_topics.insert(topic.to_owned());
            // Drop the lock before calling into Metadata which takes its
            // own lock — preserves the Java order (synchronized block on
            // ProducerMetadata releases before invoking
            // requestUpdateForNewTopics on the same monitor).
            drop(state);
            self.metadata.request_update_for_new_topics();
        }
    }

    /// Mirrors `requestUpdateForTopic(String)`.
    pub(crate) fn request_update_for_topic(&self, topic: &str) -> i32 {
        let is_new = {
            let state = self.state.lock().expect("producer metadata mutex poisoned");
            state.new_topics.contains(topic)
        };
        if is_new {
            self.metadata.request_update_for_new_topics()
        } else {
            self.metadata.request_update(false)
        }
    }

    /// Mirrors the package-private `topics()` (visible for testing).
    pub(crate) fn topics(&self) -> HashSet<String> {
        let state = self.state.lock().expect("producer metadata mutex poisoned");
        state.topics.keys().cloned().collect()
    }

    /// Mirrors the package-private `newTopics()` (visible for testing).
    pub(crate) fn new_topics(&self) -> HashSet<String> {
        let state = self.state.lock().expect("producer metadata mutex poisoned");
        state.new_topics.clone()
    }

    /// Mirrors `containsTopic(String)`.
    pub(crate) fn contains_topic(&self, topic: &str) -> bool {
        let state = self.state.lock().expect("producer metadata mutex poisoned");
        state.topics.contains_key(topic)
    }

    /// Producer-side `retainTopic` predicate. The `Mutex` is briefly
    /// re-entered to mutate `topics` when an entry expires; the borrow
    /// is short and never crosses an `.await`.
    fn retain_topic_inner(
        state: &Arc<Mutex<ProducerState>>,
        log_context: &LogContext,
        topic: &str,
        now_ms: i64,
    ) -> bool {
        let mut state = state.lock().expect("producer metadata mutex poisoned");
        let expire_ms = match state.topics.get(topic).copied() {
            Some(v) => v,
            None => return false,
        };
        if state.new_topics.contains(topic) {
            return true;
        }
        if expire_ms <= now_ms {
            debug!(
                "{}Removing unused topic {topic} from the metadata list, expiryMs {expire_ms} now {now_ms}",
                log_context.log_prefix()
            );
            state.topics.remove(topic);
            return false;
        }
        true
    }

    /// Wait for metadata update until the current version is larger than
    /// the last version we know of. Mirrors `awaitUpdate(int, long)`.
    ///
    /// Java's `awaitUpdate` is `synchronized` and blocks on `wait()`
    /// inside `Time.waitObject`. Rust translates this to an `async fn`
    /// that awaits a [`Notify`] signaling waker; the parent
    /// [`Metadata::update`] / [`Metadata::fatal_error`] / [`Metadata::close`]
    /// all `notify_waiters` so multiple awaiters wake.
    ///
    /// Returns `Err(KafkaError::Timeout)` if the deadline elapses
    /// without `update_version > last_version`. Returns
    /// `Err(KafkaError::Generic)` if the metadata instance is closed
    /// while awaiting (mirrors Java's `KafkaException("Requested
    /// metadata update after close")`). Returns the most recent fatal
    /// exception if one is set during the wait.
    pub(crate) async fn await_update(&self, last_version: i32, timeout_ms: i64) -> Result<(), KafkaError> {
        // The deadline is anchored against the tokio runtime clock so
        // tests using `MockTime` (which does not auto-advance) still
        // see the timeout fire. Java's `Time.waitObject(...)` uses the
        // same `time` source for both deadline and waiter wake-up, but
        // `MockTime` overrides `waitObject` to advance the mock clock
        // during the wait — that override has no Rust analogue, so we
        // bind the deadline to wall time. The behavioural contract for
        // callers is unchanged: a timeout of N ms returns an error
        // after at most N ms of real time.
        let deadline = if !(0..=i64::MAX / 2).contains(&timeout_ms) {
            None
        } else {
            Some(Instant::now() + Duration::from_millis(timeout_ms.max(0) as u64))
        };

        // Critical: register a `Notified` future *before* the first
        // predicate check so we don't miss a wake-up that races with
        // the check (tokio::sync::Notify only wakes futures that are
        // registered when `notify_waiters` fires).
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);

            // Predicate check (Java: `updateVersion() > lastVersion ||
            // isClosed()`). Also propagates fatal exceptions.
            self.metadata.maybe_throw_fatal_error()?;
            if self.metadata.update_version() > last_version {
                return Ok(());
            }
            if self.metadata.is_closed() {
                return Err(KafkaError::Generic("Requested metadata update after close".to_owned()));
            }

            // Compute remaining wall-time budget.
            let now = Instant::now();
            let wait_until = match deadline {
                Some(d) if d <= now => {
                    return Err(KafkaError::Timeout(format!(
                        "Timeout of {timeout_ms} ms waiting for metadata update"
                    )));
                },
                Some(d) => d,
                None => now + Duration::from_secs(60 * 60 * 24 * 365), // effectively "no timeout"
            };

            // Wait for the next `notify_waiters` from `update`,
            // `fatal_error`, or `close`, bounded by the deadline.
            let _ = tokio::time::timeout_at(wait_until, &mut notified).await;
            // Loop continues — predicate re-checked.
        }
    }

    /// Mirrors `Metadata.updateWithCurrentRequestVersion(MetadataResponse, boolean, long)`
    /// (inherited; visible for testing). Resolves the current request
    /// version from the underlying [`Metadata`] then calls
    /// [`Self::update`] so producer-side state (per-topic errors, the
    /// new-topic set) is refreshed.
    pub(crate) fn update_with_current_request_version(
        &self,
        response: &MetadataResponse,
        is_partial_update: bool,
        now_ms: i64,
    ) -> Result<(), KafkaError> {
        let request_version = self.metadata.new_metadata_request_and_version(now_ms).request_version;
        self.update(request_version, response, is_partial_update, now_ms)
    }

    /// Mirrors `update(int, MetadataResponse, boolean, long)`. Calls
    /// the parent `update` then updates producer-specific state and
    /// wakes any `await_update` callers.
    pub(crate) fn update(
        &self,
        request_version: i32,
        response: &MetadataResponse,
        is_partial_update: bool,
        now_ms: i64,
    ) -> Result<(), KafkaError> {
        self.metadata.update(request_version, response, is_partial_update, now_ms)?;

        let mut state = self.state.lock().expect("producer metadata mutex poisoned");
        // Refresh the per-topic error map.
        //
        // Java's `ProducerMetadata.update` propagates any
        // `IllegalArgumentException` from `MetadataResponse.errors()` —
        // see `ProducerMetadata.java:136`. The exception is only thrown
        // when a topic in the response has no name (the response was
        // built with topic IDs only and the caller should be using
        // `errorsByTopicId()`). The producer client always operates on
        // name-keyed responses, so receiving a topic-id-only response
        // here is a programming error in the calling stack — `expect()`
        // matches Java's "throw the unchecked IllegalArgumentException"
        // behavior. Replacing the previous `unwrap_or_default()` which
        // silently swallowed malformed-response errors and left
        // `get_error()` returning `None` (CLAUDE.md rule 5).
        state.errors = response
            .errors()
            .expect("ProducerMetadata.update received a topic-id-only MetadataResponse; use errorsByTopicId");

        // Remove all topics in the response that are in the new topic
        // set. Note that if an error was encountered for a new topic's
        // metadata, then any work to resolve the error will include the
        // topic in a full metadata update.
        if !state.new_topics.is_empty() {
            for metadata in response.topic_metadata() {
                state.new_topics.remove(metadata.topic());
            }
        }
        drop(state);
        // Wake awaiters.
        self.notify.notify_waiters();
        Ok(())
    }

    /// Mirrors `getError(String)`.
    pub(crate) fn get_error(&self, topic: &str) -> Option<Errors> {
        let state = self.state.lock().expect("producer metadata mutex poisoned");
        state.errors.get(topic).copied()
    }

    /// Mirrors `fatalError(KafkaException)`. The parent's `fatalError`
    /// already wakes the `Notify`, but Java's override calls
    /// `notifyAll()` again. Re-notifying is a no-op so we don't have
    /// to.
    pub(crate) fn fatal_error(&self, error: KafkaError) {
        self.metadata.fatal_error(error);
    }

    /// Mirrors `close()`. The parent's `close` already wakes the
    /// `Notify`.
    pub(crate) fn close(&self) {
        self.metadata.close();
    }

    /// Mirrors `metadataIdleMs` getter.
    pub(crate) fn metadata_idle_ms(&self) -> i64 {
        self.metadata_idle_ms
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `ProducerMetadataTest`.

    use super::*;
    use crate::common::message::metadata_response_data::{
        MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
    };
    use crate::common::node::Node;
    use crate::common::record::record_batch::NO_PARTITION_LEADER_EPOCH;
    use crate::common::utils::MockTime;
    use crate::common::uuid::ZERO_UUID;
    use std::time::Duration as StdDuration;

    /// Construct a `MetadataResponse` matching Java's
    /// `RequestTestUtils.metadataUpdateWith(numNodes, partitionCounts)`
    /// — the helper used by `responseWithTopics` /
    /// `responseWithCurrentTopics` in `ProducerMetadataTest`.
    fn metadata_response_with_topics(num_nodes: i32, topic_partition_counts: &[(&str, i32)]) -> MetadataResponse {
        let nodes: Vec<Node> = (0..num_nodes).map(|i| Node::new(i, "localhost".to_owned(), 1969 + i)).collect();
        let mut data = MetadataResponseData::new();
        data.cluster_id = Some("dummy".to_owned());
        data.controller_id = 0;
        data.brokers = nodes
            .iter()
            .map(|n| MetadataResponseBroker {
                node_id: n.id(),
                host: n.host().to_owned(),
                port: n.port(),
                rack: n.rack().map(str::to_owned),
                unknown_tagged_fields: Vec::new(),
            })
            .collect();
        data.topics = topic_partition_counts
            .iter()
            .map(|(topic, num_parts)| MetadataResponseTopic {
                error_code: 0,
                name: Some((*topic).to_owned()),
                topic_id: ZERO_UUID,
                is_internal: false,
                partitions: (0..*num_parts)
                    .map(|i| {
                        let leader = nodes[(i as usize) % nodes.len().max(1)].id();
                        MetadataResponsePartition {
                            error_code: 0,
                            partition_index: i,
                            leader_id: leader,
                            leader_epoch: NO_PARTITION_LEADER_EPOCH,
                            replica_nodes: vec![leader],
                            isr_nodes: vec![leader],
                            offline_replicas: Vec::new(),
                            unknown_tagged_fields: Vec::new(),
                        }
                    })
                    .collect(),
                topic_authorized_operations: -1,
                unknown_tagged_fields: Vec::new(),
            })
            .collect();
        MetadataResponse::new(data, true)
    }

    /// Mirrors `responseWithCurrentTopics()` — builds a single-partition
    /// metadata response covering exactly the topics currently held by
    /// the producer-metadata instance.
    fn response_with_current_topics(pm: &ProducerMetadata) -> MetadataResponse {
        let topics: Vec<String> = pm.topics().into_iter().collect();
        let counts: Vec<(&str, i32)> = topics.iter().map(|t| (t.as_str(), 1)).collect();
        metadata_response_with_topics(1, &counts)
    }

    fn fresh_producer_metadata(time: Arc<dyn Time>) -> Arc<ProducerMetadata> {
        ProducerMetadata::new(
            50,
            100,
            1000,
            60_000,
            LogContext::default(),
            Arc::new(ClusterResourceListeners::default()),
            time,
        )
        .expect("producer metadata constructs")
    }

    /// Like [`fresh_producer_metadata`] but with the same backoff /
    /// expiry constants Java's `ProducerMetadataTest` uses
    /// (`refreshBackoffMs=100`, `refreshBackoffMaxMs=1000`,
    /// `metadataExpireMs=1000`, `METADATA_IDLE_MS=60_000`).
    fn java_compat_producer_metadata(time: Arc<dyn Time>) -> Arc<ProducerMetadata> {
        ProducerMetadata::new(
            100,
            1000,
            1000,
            60_000,
            LogContext::default(),
            Arc::new(ClusterResourceListeners::default()),
            time,
        )
        .expect("producer metadata constructs")
    }

    /// Java: `testMetadataAwaitAfterClose`.
    #[tokio::test]
    async fn await_update_throws_after_close() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = fresh_producer_metadata(time.clone());
        pm.close();
        let err = pm.await_update(0, 1000).await.unwrap_err();
        assert!(matches!(err, KafkaError::Generic(_)));
        assert!(err.to_string().contains("Requested metadata update after close"));
    }

    /// Java: `testNotifyClosed` — close should wake awaiters.
    #[tokio::test]
    async fn close_wakes_await_update() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = fresh_producer_metadata(time.clone());
        let pm_clone = Arc::clone(&pm);
        let handle = tokio::spawn(async move {
            // Wait a long timeout — should be interrupted by close().
            pm_clone.await_update(0, 60_000).await
        });
        // Allow the spawned task to enter `await_update`.
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        pm.close();
        let res = handle.await.unwrap();
        // Either Ok (if version somehow got bumped) or
        // KafkaError::Generic("Requested metadata update after close").
        match res {
            Ok(()) => {},
            Err(KafkaError::Generic(m)) => assert!(m.contains("after close"), "got: {m}"),
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    /// Java: `testTimeToNextUpdate`/topic-add behaviour: adding a topic
    /// triggers a partial update via the parent.
    #[test]
    fn add_topic_marks_partial_update() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = fresh_producer_metadata(time);
        pm.add("topic1", 0);
        assert!(pm.contains_topic("topic1"));
        assert!(pm.new_topics().contains("topic1"));
        assert!(pm.metadata().update_requested());
    }

    /// Adding the same topic twice does not re-bump request version.
    #[test]
    fn add_topic_idempotent() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = fresh_producer_metadata(time);
        pm.add("topic1", 0);
        let v1 = pm.metadata().new_metadata_request_and_version(0).request_version;
        pm.add("topic1", 0);
        let v2 = pm.metadata().new_metadata_request_and_version(0).request_version;
        assert_eq!(v1, v2);
    }

    /// Java: `testMetadataPartialUpdate`-style — `request_update_for_topic`
    /// dispatches based on whether the topic is new.
    #[test]
    fn request_update_for_topic_dispatches() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = fresh_producer_metadata(time);
        pm.add("topic1", 0);
        assert!(pm.new_topics().contains("topic1"));
        // Calling request_update_for_topic on a new topic invokes
        // request_update_for_new_topics — which bumps request_version.
        let v0 = pm.metadata().new_metadata_request_and_version(0).request_version;
        pm.request_update_for_topic("topic1");
        let v1 = pm.metadata().new_metadata_request_and_version(0).request_version;
        assert_eq!(v1, v0 + 1);

        // For a non-new topic, request_update is called — no
        // request_version bump.
        let v_before = pm.metadata().new_metadata_request_and_version(0).request_version;
        pm.request_update_for_topic("not-a-topic");
        let v_after = pm.metadata().new_metadata_request_and_version(0).request_version;
        assert_eq!(v_before, v_after);
    }

    /// `containsTopic` returns true after `add`, false otherwise.
    #[test]
    fn contains_topic() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = fresh_producer_metadata(time);
        assert!(!pm.contains_topic("topic1"));
        pm.add("topic1", 0);
        assert!(pm.contains_topic("topic1"));
    }

    /// `metadata_idle_ms` is returned as-is.
    #[test]
    fn metadata_idle_ms_returns_value() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = fresh_producer_metadata(time);
        assert_eq!(pm.metadata_idle_ms(), 60_000);
    }

    // Note: previously had a second test named
    // `await_update_returns_after_close_synchronously` that exercised
    // exactly the same path as `await_update_throws_after_close`
    // above (close → await with `update_version()` as last_version
    // → expect KafkaError::Generic). Removed in Phase 4b review
    // (Issue 14) as a duplicate; the path is fully covered by the
    // first test.

    /// Java: `testTimeoutOnAwaitUpdate` — `awaitUpdate` returns a
    /// `KafkaError::Timeout` if the deadline elapses.
    #[tokio::test]
    async fn await_update_times_out() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = fresh_producer_metadata(time);
        let err = pm.await_update(pm.metadata().update_version(), 50).await.unwrap_err();
        assert!(matches!(err, KafkaError::Timeout(_)), "got {err:?}");
    }

    /// Java: `testTopicExpiry` (`ProducerMetadataTest.java:182-213`).
    /// Three-phase contract:
    /// 1. add → wait METADATA_IDLE_MS → next update drops the topic.
    /// 2. add → repeated re-add inside the idle window keeps the topic.
    /// 3. add a new topic, then update only after expiry would have
    ///    elapsed — topic is still retained because it never had a
    ///    chance to expire (the predicate runs only on update).
    #[test]
    fn topic_expiry() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = java_compat_producer_metadata(time);
        let metadata_idle_ms = pm.metadata_idle_ms();

        // Phase 1: topic added, then expires after the idle window.
        let mut now: i64 = 0;
        let topic1 = "topic1";
        pm.add(topic1, now);
        let resp = response_with_current_topics(&pm);
        pm.update_with_current_request_version(&resp, false, now).unwrap();
        assert!(pm.contains_topic(topic1));

        now += metadata_idle_ms;
        let resp = response_with_current_topics(&pm);
        pm.update_with_current_request_version(&resp, false, now).unwrap();
        assert!(!pm.contains_topic(topic1), "Unused topic not expired");

        // Phase 2: re-adding inside the window keeps the topic alive.
        let topic2 = "topic2";
        pm.add(topic2, now);
        let resp = response_with_current_topics(&pm);
        pm.update_with_current_request_version(&resp, false, now).unwrap();
        for _ in 0..3 {
            now += metadata_idle_ms / 2;
            let resp = response_with_current_topics(&pm);
            pm.update_with_current_request_version(&resp, false, now).unwrap();
            assert!(pm.contains_topic(topic2), "Topic expired even though in use");
            pm.add(topic2, now);
        }

        // Phase 3: adding a topic and updating after the would-be
        // expiry — the topic is still retained because the response
        // (which carries it) prevents its predicate from removing it.
        let topic3 = "topic3";
        pm.add(topic3, now);
        now += metadata_idle_ms * 2;
        let resp = response_with_current_topics(&pm);
        pm.update_with_current_request_version(&resp, false, now).unwrap();
        assert!(pm.contains_topic(topic3), "Topic expired while awaiting metadata");
    }

    /// Java: `testMetadataWaitAbortedOnFatalException`
    /// (`ProducerMetadataTest.java:216-219`). When a fatal error is
    /// raised, `await_update` returns the error immediately on the
    /// next predicate check.
    #[tokio::test]
    async fn metadata_wait_aborted_on_fatal_error() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = fresh_producer_metadata(time);
        pm.fatal_error(KafkaError::Authentication("Fatal exception from test".to_owned()));
        let err = pm.await_update(0, 1000).await.unwrap_err();
        assert!(matches!(err, KafkaError::Authentication(_)), "got {err:?}");
    }

    /// Java: `testTimeToNextUpdateOverwriteBackoff`
    /// (`ProducerMetadataTest.java:163-180`). Adding a new topic
    /// overrides the backoff so the next update can fire immediately.
    #[test]
    fn time_to_next_update_overwrite_backoff() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = java_compat_producer_metadata(time);
        let now: i64 = 10_000;

        // New topic added to fetch set and update requested. It should
        // allow immediate update.
        let resp = response_with_current_topics(&pm);
        pm.update_with_current_request_version(&resp, false, now).unwrap();
        pm.add("new-topic", now);
        assert_eq!(pm.metadata().time_to_next_update(now), 0);

        // Even though `add` is called, immediate update isn't necessary
        // if the new-topic set isn't growing.
        let resp = response_with_current_topics(&pm);
        pm.update_with_current_request_version(&resp, false, now).unwrap();
        pm.add("new-topic", now);
        // After update, the new-topic set is empty, the topic is
        // already known, time-to-next-update is bounded by metadata
        // expire ms (1000) which is >= refresh_backoff_ms (100), so it
        // returns the larger of those.
        assert_eq!(pm.metadata().time_to_next_update(now), 1000);

        // If the new set of topics contains a new topic, allow
        // immediate update again.
        pm.add("another-new-topic", now);
        assert_eq!(pm.metadata().time_to_next_update(now), 0);
    }

    /// Java: `testMetadataPartialUpdate`
    /// (`ProducerMetadataTest.java:222-267`). Drives the new-topic vs.
    /// retained-topic transitions through several partial updates.
    #[test]
    fn metadata_partial_update_lifecycle() {
        let time: Arc<dyn Time> = MockTime::arc();
        let pm = java_compat_producer_metadata(time);
        let mut now: i64 = 10_000;

        // Add a new topic and fetch its metadata in a partial update.
        let topic1 = "topic-one";
        pm.add(topic1, now);
        assert!(pm.metadata().update_requested());
        assert_eq!(pm.metadata().time_to_next_update(now), 0);
        assert_eq!(pm.topics(), HashSet::from([topic1.to_owned()]));
        assert_eq!(pm.new_topics(), HashSet::from([topic1.to_owned()]));

        // Perform the partial update. Verify the topic is no longer
        // considered "new".
        now += 1000;
        let resp = metadata_response_with_topics(1, &[(topic1, 1)]);
        pm.update_with_current_request_version(&resp, true, now).unwrap();
        assert!(!pm.metadata().update_requested());
        assert_eq!(pm.topics(), HashSet::from([topic1.to_owned()]));
        assert_eq!(pm.new_topics(), HashSet::new());

        // Add the topic again. It should not be considered "new".
        pm.add(topic1, now);
        assert!(!pm.metadata().update_requested());
        assert!(pm.metadata().time_to_next_update(now) > 0);
        assert_eq!(pm.topics(), HashSet::from([topic1.to_owned()]));
        assert_eq!(pm.new_topics(), HashSet::new());

        // Add two new topics, but apply a partial update for only one.
        now += 1000;
        let topic2 = "topic-two";
        pm.add(topic2, now);

        now += 1000;
        let topic3 = "topic-three";
        pm.add(topic3, now);

        assert!(pm.metadata().update_requested());
        assert_eq!(pm.metadata().time_to_next_update(now), 0);
        assert_eq!(
            pm.topics(),
            HashSet::from([topic1.to_owned(), topic2.to_owned(), topic3.to_owned()])
        );
        assert_eq!(pm.new_topics(), HashSet::from([topic2.to_owned(), topic3.to_owned()]));

        // Perform the partial update for a subset of the new topics.
        now += 1000;
        assert!(pm.metadata().update_requested());
        let resp = metadata_response_with_topics(1, &[(topic2, 1)]);
        pm.update_with_current_request_version(&resp, true, now).unwrap();
        assert_eq!(
            pm.topics(),
            HashSet::from([topic1.to_owned(), topic2.to_owned(), topic3.to_owned()])
        );
        assert_eq!(pm.new_topics(), HashSet::from([topic3.to_owned()]));
    }
}
