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

//! `TopicMetadataRequestManager` — manages the state of topic metadata
//! requests issued on behalf of `list_topics()` and `partitions_for(topic)`.
//!
//! Returns a [`PollResult`] when a request is ready to be sent and tracks
//! the per-request [`RequestState`] so backoff is enforced before a new
//! attempt is issued. Once a request is completed successfully or times
//! out, its corresponding entry is removed from the inflight list.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager`.

#![allow(dead_code)]

use std::collections::HashMap;

use tokio::sync::oneshot;

use crate::common::KafkaError;
use crate::common::PartitionInfo;
use crate::common::protocol::Errors;
use crate::common::requests::{MetadataRequestBuilder, MetadataResponse, RequestBuilder};
use crate::consumer::ConsumerConfig;

use super::network_client_delegate::{PollResult, UnsentRequest};
use super::request_manager::RequestManager;
use super::timed_request_state::TimedRequestState;

/// Result returned by [`TopicMetadataRequestManager::request_topic_metadata`]
/// and [`TopicMetadataRequestManager::request_all_topics_metadata`].
///
/// Mirrors Java's `CompletableFuture<Map<String, List<PartitionInfo>>>` —
/// the receiver resolves with the topic → partition-info map on success or
/// a [`KafkaError`] on failure (timeout, invalid topic, authorization
/// failure, ...).
pub(crate) type TopicMetadataResult = Result<HashMap<String, Vec<PartitionInfo>>, KafkaError>;

/// Manages the state of topic metadata requests. This manager handles the
/// `listTopics` and `partitionsFor` consumer API calls.
///
/// Java: `org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager`.
pub(crate) struct TopicMetadataRequestManager {
    /// Pending requests. Mirrors Java's
    /// `LinkedList<TopicMetadataRequestState> inflightRequests`. Each entry
    /// carries its own [`RequestState`] for independent backoff.
    inflight_requests: Vec<TopicMetadataRequestState>,
    /// Mirrors Java's `boolean allowAutoTopicCreation`.
    allow_auto_topic_creation: bool,
    /// Mirrors Java's `long retryBackoffMs`.
    retry_backoff_ms: i64,
    /// Mirrors Java's `long retryBackoffMaxMs`.
    retry_backoff_max_ms: i64,
    /// Monotonically-increasing id assigned to every new
    /// [`TopicMetadataRequestState`]. Replaces Java's `this`-reference
    /// identity for matching responses back to their pending request.
    next_request_id: u64,
}

/// Per-request state. Mirrors Java's inner class
/// `TopicMetadataRequestManager.TopicMetadataRequestState` — composes a
/// [`TimedRequestState`] (which itself extends `RequestState` in Java) with
/// the request's `topic` (or `None` for all-topics), the unique request
/// id, and a one-shot ack channel used to resolve the caller's future.
pub(crate) struct TopicMetadataRequestState {
    /// Unique id used to match responses back to this state object.
    id: u64,
    /// `None` means "all topics" (Java: `allTopics == true`).
    topic: Option<String>,
    timed_state: TimedRequestState,
    /// Mirrors Java's `CompletableFuture future`. `None` once the future
    /// has been completed (idempotent: a double-complete is dropped).
    ack: Option<oneshot::Sender<TopicMetadataResult>>,
}

impl TopicMetadataRequestState {
    /// Returns the topic this state was created for, or `None` for an
    /// all-topics request. Java: `topic()`.
    pub(crate) fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    /// Returns this state's unique id. Tests use it to drive responses
    /// back through [`TopicMetadataRequestManager::on_response`] /
    /// [`TopicMetadataRequestManager::on_failure`].
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Returns the remaining backoff in milliseconds before another send
    /// attempt is permitted. Java: `remainingBackoffMs(long)`.
    pub(crate) fn remaining_backoff_ms(&self, current_time_ms: i64) -> i64 {
        self.timed_state.remaining_backoff_ms(current_time_ms)
    }

    /// Returns `true` if this request's deadline has passed at the given
    /// current time. Java: `isExpired()` (the Java version internally
    /// calls `timer.update()` then `timer.isExpired()`).
    pub(crate) fn is_expired(&self, current_time_ms: i64) -> bool {
        self.timed_state.is_expired(current_time_ms)
    }

    /// Idempotent sender. Returns `true` if this call delivered the result;
    /// subsequent calls are no-ops (matches Java's `CompletableFuture`
    /// semantics — the *first* completion wins).
    fn complete(&mut self, result: TopicMetadataResult) -> bool {
        if let Some(tx) = self.ack.take() {
            // Drop the result silently if the receiver was already
            // released (caller no longer cares about the future).
            let _ = tx.send(result);
            true
        } else {
            false
        }
    }
}

impl TopicMetadataRequestManager {
    /// Constructs a new [`TopicMetadataRequestManager`]. Mirrors Java's
    /// `TopicMetadataRequestManager(LogContext, Time, ConsumerConfig)`.
    pub(crate) fn new(config: &ConsumerConfig) -> Self {
        Self {
            inflight_requests: Vec::new(),
            allow_auto_topic_creation: config.allow_auto_create_topics,
            retry_backoff_ms: config.retry_backoff_ms(),
            retry_backoff_max_ms: config.retry_backoff_max_ms(),
            next_request_id: 0,
        }
    }

    /// Visible-for-testing accessor for the inflight-requests slice.
    /// Java: package-private `inflightRequests()`.
    pub(crate) fn inflight_requests(&self) -> &[TopicMetadataRequestState] {
        &self.inflight_requests
    }

    /// Enqueue a new request for the metadata of a single topic, returning
    /// a receiver the caller awaits for the result.
    ///
    /// Java: `requestTopicMetadata(String topic, long deadlineMs)`.
    pub(crate) fn request_topic_metadata(
        &mut self,
        topic: String,
        deadline_ms: i64,
    ) -> oneshot::Receiver<TopicMetadataResult> {
        self.enqueue(Some(topic), deadline_ms)
    }

    /// Enqueue a new request for the metadata of all topics, returning a
    /// receiver the caller awaits for the result.
    ///
    /// Java: `requestAllTopicsMetadata(long deadlineMs)`.
    pub(crate) fn request_all_topics_metadata(&mut self, deadline_ms: i64) -> oneshot::Receiver<TopicMetadataResult> {
        self.enqueue(None, deadline_ms)
    }

    fn enqueue(&mut self, topic: Option<String>, deadline_ms: i64) -> oneshot::Receiver<TopicMetadataResult> {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        let (tx, rx) = oneshot::channel();
        let timed_state = TimedRequestState::new(
            "TopicMetadataRequestState",
            self.retry_backoff_ms,
            self.retry_backoff_max_ms,
            deadline_ms,
        );
        self.inflight_requests
            .push(TopicMetadataRequestState { id, topic, timed_state, ack: Some(tx) });
        rx
    }

    /// Drives the bg-task response path for `request_id`. Translates the
    /// Java `whenComplete((response, exception) -> { ... handleResponse(response) ... })`
    /// callback bound at request-creation time.
    ///
    /// Phase 10 wires this from the bg task: after observing the
    /// [`oneshot::Receiver`] returned from each request's
    /// [`UnsentRequest::take_response_receiver`], the task calls
    /// [`Self::on_response`] (success path) or [`Self::on_failure`]
    /// (error path).
    ///
    /// On a retriable error or a partial-failure response, the inflight
    /// request stays in the queue so the next [`Self::poll`] iteration can
    /// re-issue it (after backoff). On a fatal error or success, the
    /// inflight request is removed and the user-facing future resolved.
    ///
    /// Java: `TopicMetadataRequestState::handleResponse(ClientResponse)`.
    pub(crate) fn on_response(&mut self, request_id: u64, current_time_ms: i64, response: &MetadataResponse) {
        match Self::handle_topic_metadata_response(response) {
            Ok(partition_infos) => {
                if let Some(idx) = self.find_inflight_index(request_id) {
                    let mut state = self.inflight_requests.remove(idx);
                    state.complete(Ok(partition_infos));
                }
            },
            Err(error) => {
                // Treat the synthesised error like a generic
                // `handleError(exception, completionTimeMs)`. Retriable →
                // back off and try again; non-retriable → complete with
                // the error and remove from inflight.
                self.on_failure(request_id, current_time_ms, error);
            },
        }
    }

    /// Drives the bg-task error path for `request_id`. Mirrors Java's
    /// `handleError(Throwable, long)`:
    ///
    /// - For a retriable error: if the deadline has passed, complete with
    ///   a [`KafkaError::timeout`] and remove the inflight request.
    ///   Otherwise call `on_failed_attempt` to extend the backoff and
    ///   leave the request in the queue.
    /// - For any other (fatal) error: complete the future with the error
    ///   and remove the inflight request.
    pub(crate) fn on_failure(&mut self, request_id: u64, current_time_ms: i64, error: KafkaError) {
        let Some(idx) = self.find_inflight_index(request_id) else {
            return;
        };
        if error.is_retriable() {
            if self.inflight_requests[idx].is_expired(current_time_ms) {
                let mut state = self.inflight_requests.remove(idx);
                state.complete(Err(KafkaError::timeout(
                    "Timeout expired while fetching topic metadata".to_string(),
                )));
            } else {
                self.inflight_requests[idx].timed_state.on_failed_attempt(current_time_ms);
            }
        } else {
            let mut state = self.inflight_requests.remove(idx);
            state.complete(Err(error));
        }
    }

    fn find_inflight_index(&self, request_id: u64) -> Option<usize> {
        self.inflight_requests.iter().position(|s| s.id == request_id)
    }

    /// Java: private `handleTopicMetadataResponse(MetadataResponse)`.
    /// Returns the topic → partition-info map on success, or a
    /// [`KafkaError`] mirroring the Java exception path:
    ///
    /// - `TopicAuthorizationException` if any unauthorized topics are
    ///   present in the response.
    /// - `InvalidTopicException` if any topic has
    ///   `Errors::InvalidTopicException`.
    /// - The retriable error itself (wrapped in
    ///   [`KafkaError::with_message`]) if any topic has a retriable error
    ///   (e.g. `Errors::LeaderNotAvailable`).
    /// - A generic [`KafkaError`] otherwise.
    ///
    /// `Errors::UnknownTopicOrPartition` is treated as "topic absent" and
    /// simply omitted from the returned map — matching Java's `continue`
    /// branch.
    fn handle_topic_metadata_response(response: &MetadataResponse) -> TopicMetadataResult {
        let cluster = response.build_cluster();

        let unauthorized_topics = cluster.unauthorized_topics();
        if !unauthorized_topics.is_empty() {
            return Err(KafkaError::topic_authorization(unauthorized_topics.clone()));
        }

        for (topic, error) in response.errors() {
            // Skip "topic absent" — Java's `continue`.
            if error == Errors::UnknownTopicOrPartition {
                continue;
            }
            if error == Errors::InvalidTopicException {
                return Err(KafkaError::with_message(
                    Errors::InvalidTopicException,
                    format!("Topic '{topic}' is invalid"),
                ));
            }
            // Java: `error.exception() instanceof RetriableException` →
            // throw the exception (retriable, so callers retry).
            if error.is_retriable() {
                return Err(KafkaError::new(error));
            }
            return Err(KafkaError::with_message(
                error,
                format!("Unexpected error fetching metadata for topic {topic}"),
            ));
        }

        let mut result: HashMap<String, Vec<PartitionInfo>> = HashMap::new();
        for topic in cluster.topics() {
            let infos = cluster.partitions_for_topic(topic).to_vec();
            result.insert(topic.to_string(), infos);
        }
        Ok(result)
    }
}

impl RequestManager for TopicMetadataRequestManager {
    /// Returns a [`PollResult`] carrying any inflight requests that are
    /// ready to be sent at `current_time_ms`. Expired requests are pruned
    /// and their futures completed with a [`KafkaError::timeout`] before
    /// the live requests are walked. The Java idiom of two passes (prune,
    /// then iterate) is preserved.
    ///
    /// Java: `poll(long currentTimeMs)`.
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        // First pass: expire stale requests (mirrors Java's
        // `requestStateIterator.remove()` for `requestState.isExpired()`).
        // Walk from the tail so removals don't shift indices we still
        // need to visit.
        let mut idx = 0;
        while idx < self.inflight_requests.len() {
            if self.inflight_requests[idx].is_expired(current_time_ms) {
                let mut state = self.inflight_requests.remove(idx);
                state.complete(Err(KafkaError::timeout(
                    "Timeout expired while fetching topic metadata".to_string(),
                )));
                // Don't advance idx — the next element has shifted left.
                continue;
            }
            idx += 1;
        }

        // Second pass: build UnsentRequest entries for everything that
        // can_send_request at `current_time_ms`.
        let mut unsent = Vec::new();
        for state in self.inflight_requests.iter_mut() {
            if !state.timed_state.can_send_request(current_time_ms) {
                continue;
            }
            state.timed_state.on_send_attempt(current_time_ms);

            let builder: Box<dyn RequestBuilder> = match state.topic.as_deref() {
                Some(topic) => Box::new(MetadataRequestBuilder::new(Some(&[topic]), self.allow_auto_topic_creation)),
                None => Box::new(MetadataRequestBuilder::all_topics()),
            };
            unsent.push(UnsentRequest::new(builder, None));
        }

        if unsent.is_empty() {
            PollResult::empty()
        } else {
            PollResult::new(0, unsent)
        }
    }

    // No `signal_close` override — Java's TopicMetadataRequestManager
    // does not override it either (inherits the no-op default).
}

#[cfg(test)]
mod tests {
    use crate::client_response::ClientResponse;
    use crate::common::Node;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{ConcreteResponse, RequestHeader};
    use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseData, MetadataResponseTopic};

    use super::*;

    const RETRY_BACKOFF_MS: i64 = 100;

    fn setup_manager() -> TopicMetadataRequestManager {
        let mut config =
            ConsumerConfig::new(vec!["localhost:9092".to_string()]).with_retry_backoff_ms(RETRY_BACKOFF_MS);
        // Java's `ALLOW_AUTO_CREATE_TOPICS_CONFIG = false` matches Java's
        // `testSetup` properties. The field is `pub(crate)` so we set it
        // directly in tests (no public builder method exposed).
        config.allow_auto_create_topics = false;
        TopicMetadataRequestManager::new(&config)
    }

    /// Mirrors Java's `mockCluster(numNodes, controllerIndex)` — builds a
    /// list of broker nodes from `0..numNodes` on consecutive ports.
    fn mock_nodes(num_nodes: i32) -> Vec<Node> {
        (0..num_nodes)
            .map(|i| Node::new(i, "localhost".to_string(), 8121 + i))
            .collect()
    }

    /// Builds a `MetadataResponse` carrying one topic with `error_code = error`.
    /// Mirrors Java's `buildTopicMetadataClientResponse(unsent, topic, error)`.
    fn build_topic_metadata_response(topic: &str, error: Errors) -> MetadataResponse {
        let mut topic_meta = MetadataResponseTopic::new();
        topic_meta.set_name(Some(topic.to_string()));
        topic_meta.set_error_code(error.code());
        topic_meta.set_partitions(Vec::new());
        topic_meta.set_is_internal(false);

        let nodes = mock_nodes(3);
        let brokers: Vec<MetadataResponseBroker> = nodes
            .iter()
            .map(|n| {
                let mut b = MetadataResponseBroker::new();
                b.set_node_id(n.id());
                b.set_host(n.host().to_string());
                b.set_port(n.port());
                b
            })
            .collect();

        let mut data = MetadataResponseData::new();
        data.set_cluster_id(Some("mockClusterId".to_string()));
        data.set_controller_id(0);
        data.set_brokers(brokers);
        data.set_topics(vec![topic_meta]);
        MetadataResponse::new(data, ApiKeys::METADATA.latest_version())
    }

    /// Builds a `MetadataResponse` carrying two topics (`topic1`, `topic2`),
    /// both with `error_code = error`. Mirrors Java's
    /// `buildAllTopicsMetadataClientResponse(unsent, error)`.
    fn build_all_topics_metadata_response(error: Errors) -> MetadataResponse {
        let mut t1 = MetadataResponseTopic::new();
        t1.set_name(Some("topic1".to_string()));
        t1.set_error_code(error.code());
        t1.set_partitions(Vec::new());
        t1.set_is_internal(false);

        let mut t2 = MetadataResponseTopic::new();
        t2.set_name(Some("topic2".to_string()));
        t2.set_error_code(error.code());
        t2.set_partitions(Vec::new());
        t2.set_is_internal(false);

        let nodes = mock_nodes(3);
        let brokers: Vec<MetadataResponseBroker> = nodes
            .iter()
            .map(|n| {
                let mut b = MetadataResponseBroker::new();
                b.set_node_id(n.id());
                b.set_host(n.host().to_string());
                b.set_port(n.port());
                b
            })
            .collect();

        let mut data = MetadataResponseData::new();
        data.set_cluster_id(Some("mockClusterId".to_string()));
        data.set_controller_id(0);
        data.set_brokers(brokers);
        data.set_topics(vec![t1, t2]);
        MetadataResponse::new(data, ApiKeys::METADATA.latest_version())
    }

    /// Translated from `TopicMetadataRequestManagerTest.testPoll_SuccessfulRequestTopicMetadata`.
    #[test]
    fn test_poll_successful_request_topic_metadata() {
        let mut manager = setup_manager();
        let _rx = manager.request_topic_metadata("hello".to_string(), i64::MAX);
        let res = manager.poll(100);
        assert_eq!(1, res.unsent_requests.len());
    }

    /// Translated from `TopicMetadataRequestManagerTest.testPoll_SuccessfulRequestAllTopicsMetadata`.
    #[test]
    fn test_poll_successful_request_all_topics_metadata() {
        let mut manager = setup_manager();
        let _rx = manager.request_all_topics_metadata(i64::MAX);
        let res = manager.poll(100);
        assert_eq!(1, res.unsent_requests.len());
    }

    /// Drives one round of `poll -> on_response` for a single-topic
    /// request. Mirrors Java's `testTopicExceptionAndInflightRequests`
    /// inner shape: poll once, fire the response via the manager, observe
    /// inflight state.
    fn drive_topic_response(manager: &mut TopicMetadataRequestManager, topic: &str, error: Errors) {
        let res = manager.poll(100);
        assert_eq!(1, res.unsent_requests.len());
        // Java cross-checks the request builder is `MetadataRequest`. We
        // do the same: the trait's `api_key()` is the explicit witness.
        let unsent = res.unsent_requests.into_iter().next().unwrap();
        let api_key = unsent.request_builder().expect("builder still present").api_key();
        assert_eq!(&ApiKeys::METADATA, api_key);

        let response = build_topic_metadata_response(topic, error);
        let request_id = manager.inflight_requests()[0].id();
        manager.on_response(request_id, 100, &response);
    }

    /// Translated from `TopicMetadataRequestManagerTest.testTopicExceptionAndInflightRequests`
    /// (parameterized: UNKNOWN_TOPIC_OR_PARTITION, INVALID_TOPIC_EXCEPTION,
    /// UNKNOWN_SERVER_ERROR, NETWORK_EXCEPTION, NONE).
    #[test]
    fn test_topic_exception_and_inflight_requests_unknown_topic_or_partition() {
        topic_exception_and_inflight(Errors::UnknownTopicOrPartition, false);
    }
    #[test]
    fn test_topic_exception_and_inflight_requests_invalid_topic_exception() {
        topic_exception_and_inflight(Errors::InvalidTopicException, false);
    }
    #[test]
    fn test_topic_exception_and_inflight_requests_unknown_server_error() {
        topic_exception_and_inflight(Errors::UnknownServerError, false);
    }
    #[test]
    fn test_topic_exception_and_inflight_requests_network_exception() {
        topic_exception_and_inflight(Errors::NetworkException, true);
    }
    #[test]
    fn test_topic_exception_and_inflight_requests_none() {
        topic_exception_and_inflight(Errors::None, false);
    }

    fn topic_exception_and_inflight(error: Errors, should_retry: bool) {
        let topic = "hello";
        let mut manager = setup_manager();
        let _rx = manager.request_topic_metadata(topic.to_string(), i64::MAX);
        drive_topic_response(&mut manager, topic, error);
        let inflights = manager.inflight_requests();
        if should_retry {
            assert_eq!(1, inflights.len());
            assert_eq!(Some(topic), inflights[0].topic());
        } else {
            assert_eq!(0, inflights.len());
        }
    }

    /// Translated from `TopicMetadataRequestManagerTest.testAllTopicsExceptionAndInflightRequests`
    /// (parameterized: UNKNOWN_TOPIC_OR_PARTITION, INVALID_TOPIC_EXCEPTION,
    /// UNKNOWN_SERVER_ERROR, NETWORK_EXCEPTION, NONE).
    #[test]
    fn test_all_topics_exception_and_inflight_requests_unknown_topic_or_partition() {
        all_topics_exception_and_inflight(Errors::UnknownTopicOrPartition, false);
    }
    #[test]
    fn test_all_topics_exception_and_inflight_requests_invalid_topic_exception() {
        all_topics_exception_and_inflight(Errors::InvalidTopicException, false);
    }
    #[test]
    fn test_all_topics_exception_and_inflight_requests_unknown_server_error() {
        all_topics_exception_and_inflight(Errors::UnknownServerError, false);
    }
    #[test]
    fn test_all_topics_exception_and_inflight_requests_network_exception() {
        all_topics_exception_and_inflight(Errors::NetworkException, true);
    }
    #[test]
    fn test_all_topics_exception_and_inflight_requests_none() {
        all_topics_exception_and_inflight(Errors::None, false);
    }

    fn all_topics_exception_and_inflight(error: Errors, should_retry: bool) {
        let mut manager = setup_manager();
        let _rx = manager.request_all_topics_metadata(i64::MAX);
        let res = manager.poll(100);
        assert_eq!(1, res.unsent_requests.len());
        let response = build_all_topics_metadata_response(error);
        let request_id = manager.inflight_requests()[0].id();
        manager.on_response(request_id, 100, &response);
        let inflights = manager.inflight_requests();
        if should_retry {
            assert_eq!(1, inflights.len());
        } else {
            assert_eq!(0, inflights.len());
        }
    }

    /// Translated from `TopicMetadataRequestManagerTest.testExpiringRequest`.
    /// Drive a request with a 1000ms deadline, fail twice with a retriable
    /// error, sleep past the deadline, and observe both the inflight
    /// queue empty *and* the future completed exceptionally.
    #[test]
    fn test_expiring_request() {
        let topic = "hello";
        let mut manager = setup_manager();

        let now = 0_i64;
        let deadline_ms = now + 1000;
        let mut rx = manager.request_topic_metadata(topic.to_string(), deadline_ms);
        assert_eq!(1, manager.inflight_requests().len());

        // Poll #1 — fail with REQUEST_TIMED_OUT (retriable).
        let res = manager.poll(now);
        assert_eq!(1, res.unsent_requests.len());
        let response = build_topic_metadata_response(topic, Errors::RequestTimedOut);
        let request_id = manager.inflight_requests()[0].id();
        manager.on_response(request_id, now, &response);

        // Sleep 500ms (past the 100ms backoff, still inside the deadline).
        let now = now + 500;
        let res = manager.poll(now);
        assert_eq!(1, res.unsent_requests.len());
        let response = build_topic_metadata_response(topic, Errors::RequestTimedOut);
        let request_id = manager.inflight_requests()[0].id();
        manager.on_response(request_id, now, &response);

        // Sleep 1000ms more — now past the deadline. Poll expires the
        // request: future resolves with Err, inflight goes empty.
        let now = now + 1000;
        let res = manager.poll(now);
        assert_eq!(0, res.unsent_requests.len());
        assert_eq!(0, manager.inflight_requests().len());
        match rx.try_recv() {
            Ok(Err(_)) => {},
            other => panic!("expected exceptional completion, got {other:?}"),
        }
    }

    /// Translated from `TopicMetadataRequestManagerTest.testHardFailures`
    /// (parameterized: TimeoutException, KafkaException, NetworkException).
    /// Sends a request, then calls `on_failure` directly on the handler
    /// to mimic a `completeExceptionally(exception)` on the underlying
    /// future.
    #[test]
    fn test_hard_failures_timeout() {
        hard_failures(KafkaError::timeout("timeout"));
    }

    #[test]
    fn test_hard_failures_kafka_exception() {
        // Java's `KafkaException` is non-retriable by default. The Rust
        // analog with no specific error code is `KafkaError::Generic`
        // with `Errors::UnknownServerError` (also non-retriable per the
        // Rust `Errors::is_retriable` table).
        hard_failures(KafkaError::with_message(Errors::UnknownServerError, "non-retriable exception"));
    }

    #[test]
    fn test_hard_failures_network_exception() {
        // Java's `NetworkException` is retriable
        // (`extends RetriableException`).
        hard_failures(KafkaError::new(Errors::NetworkException));
    }

    fn hard_failures(error: KafkaError) {
        let topic = "hello";
        let mut manager = setup_manager();
        let _rx = manager.request_topic_metadata(topic.to_string(), i64::MAX);
        let res = manager.poll(0);
        assert_eq!(1, res.unsent_requests.len());

        let retriable = error.is_retriable();
        let request_id = manager.inflight_requests()[0].id();
        manager.on_failure(request_id, 0, error);

        if retriable {
            assert!(!manager.inflight_requests().is_empty());
        } else {
            assert!(manager.inflight_requests().is_empty());
        }
    }

    /// Translated from `TopicMetadataRequestManagerTest.testNetworkTimeout`.
    /// Drives the `on_failure(TimeoutException)` path explicitly and
    /// verifies the exponential-backoff math.
    #[test]
    fn test_network_timeout() {
        let topic = "hello";
        let mut manager = setup_manager();
        let _rx = manager.request_topic_metadata(topic.to_string(), i64::MAX);
        let res = manager.poll(0);
        assert_eq!(1, res.unsent_requests.len());

        // A second poll at the same instant: backoff is enforced
        // *after* the send attempt above (the state's `request_in_flight`
        // flag is true). `on_send_attempt` flipped the flag in the
        // previous poll, so the second poll sees no eligible requests.
        let res2 = manager.poll(0);
        assert_eq!(0, res2.unsent_requests.len());

        // Mimic a network timeout via `on_failure`.
        let request_id = manager.inflight_requests()[0].id();
        manager.on_failure(request_id, 0, KafkaError::timeout("network timeout"));

        // Read the backoff the manager computed and sleep one ms short of
        // it — the next poll must still be empty.
        let backoff_ms = manager.inflight_requests()[0].remaining_backoff_ms(0);
        let now = backoff_ms - 1;
        let res2 = manager.poll(now);
        assert_eq!(0, res2.unsent_requests.len());

        let now = now + 1;
        let res2 = manager.poll(now);
        assert_eq!(1, res2.unsent_requests.len());

        // Resolve the second attempt with NONE — inflight goes empty.
        let response = build_topic_metadata_response(topic, Errors::None);
        let request_id = manager.inflight_requests()[0].id();
        manager.on_response(request_id, now, &response);
        assert!(manager.inflight_requests().is_empty());
    }

    /// Regression test: when the response carries `TopicAuthorizationFailed`,
    /// the future resolves with `KafkaError::TopicAuthorization` and the
    /// inflight request is removed. Mirrors Java's
    /// `throw new TopicAuthorizationException(unauthorizedTopics)` branch.
    #[test]
    fn test_topic_authorization_error_is_fatal() {
        let topic = "hello";
        let mut manager = setup_manager();
        let mut rx = manager.request_topic_metadata(topic.to_string(), i64::MAX);
        let res = manager.poll(0);
        assert_eq!(1, res.unsent_requests.len());

        // Build a response that reports `topic` as unauthorized via the
        // `topics_by_error(TopicAuthorizationFailed)` channel that
        // `MetadataResponse::build_cluster` then propagates to
        // `cluster.unauthorized_topics()`.
        let response = build_topic_metadata_response(topic, Errors::TopicAuthorizationFailed);
        let request_id = manager.inflight_requests()[0].id();
        manager.on_response(request_id, 0, &response);

        assert!(manager.inflight_requests().is_empty(), "topic auth failure removes inflight");
        let received = rx.try_recv().expect("response delivered");
        let err = received.expect_err("authorization is a fatal error");
        assert!(
            matches!(err, KafkaError::TopicAuthorization(_)),
            "expected TopicAuthorization, got {err}"
        );
    }

    /// Builds an `UnsentRequest` end-to-end and verifies it is a
    /// `MetadataRequest` carrying the topic the manager was asked for —
    /// pinning the request shape Java's
    /// `assertInstanceOf(MetadataRequest.class, abstractRequest)` checks.
    #[test]
    fn test_builds_metadata_request_for_topic() {
        let topic = "hello";
        let mut manager = setup_manager();
        let _rx = manager.request_topic_metadata(topic.to_string(), i64::MAX);
        let res = manager.poll(0);
        assert_eq!(1, res.unsent_requests.len());
        let unsent = res.unsent_requests.into_iter().next().unwrap();
        let builder = unsent.request_builder().expect("builder still present");
        assert_eq!(&ApiKeys::METADATA, builder.api_key());
        // Java cross-checks `assertInstanceOf(MetadataRequest.class, ...)`;
        // the Rust equivalent is matching on the `ConcreteRequest`
        // variant the builder produces.
        let concrete = builder.build().expect("builder.build() ok");
        let metadata = match &concrete {
            crate::common::requests::ConcreteRequest::Metadata(m) => m,
            other => panic!("expected Metadata, got {other:?}"),
        };
        let topic_names = metadata.topics().expect("not all-topics");
        assert_eq!(vec![topic], topic_names);
    }

    /// Builds an `UnsentRequest` for an all-topics request — pins the
    /// `MetadataRequest.Builder.allTopics()` analog.
    #[test]
    fn test_builds_metadata_request_for_all_topics() {
        let mut manager = setup_manager();
        let _rx = manager.request_all_topics_metadata(i64::MAX);
        let res = manager.poll(0);
        assert_eq!(1, res.unsent_requests.len());
        let unsent = res.unsent_requests.into_iter().next().unwrap();
        let builder = unsent.request_builder().expect("builder still present");
        assert_eq!(&ApiKeys::METADATA, builder.api_key());
        let concrete = builder.build().expect("builder.build() ok");
        let metadata = match &concrete {
            crate::common::requests::ConcreteRequest::Metadata(m) => m,
            other => panic!("expected Metadata, got {other:?}"),
        };
        assert!(metadata.is_all_topics(), "all-topics request has no topic list");
    }

    /// Tail-of-list regression: a request created later in time but with
    /// a closer deadline should expire independently of earlier
    /// requests. Verifies the per-state `TimedRequestState` is wired up
    /// correctly.
    #[test]
    fn test_per_request_expiration_is_independent() {
        let mut manager = setup_manager();
        let _rx1 = manager.request_topic_metadata("a".to_string(), i64::MAX);
        let _rx2 = manager.request_topic_metadata("b".to_string(), 500);

        // At time 1000, only the "b" request is expired.
        let res = manager.poll(1000);
        assert_eq!(1, res.unsent_requests.len(), "only the 'a' request remains pollable");
        assert_eq!(1, manager.inflight_requests().len());
        assert_eq!(Some("a"), manager.inflight_requests()[0].topic());
    }

    /// Pinning regression: dropping the receiver before completion must
    /// not panic. The manager's idempotent `complete` swallows the
    /// `Err` from `oneshot::Sender::send`.
    #[test]
    fn test_dropped_receiver_is_silent() {
        let mut manager = setup_manager();
        let rx = manager.request_topic_metadata("hello".to_string(), i64::MAX);
        drop(rx);

        let res = manager.poll(0);
        assert_eq!(1, res.unsent_requests.len());

        let request_id = manager.inflight_requests()[0].id();
        // Should not panic even though the receiver is gone.
        manager.on_failure(request_id, 0, KafkaError::new(Errors::UnknownServerError));
        assert!(manager.inflight_requests().is_empty());
    }

    /// Plumbs an end-to-end `oneshot::Receiver` resolution via the
    /// manager's `on_response`. Mirrors what Phase 10's bg task does:
    /// take the response receiver from the `UnsentRequest`, fire the
    /// completion through the manager, observe the user-facing future.
    /// Java cluster build behavior: a topic with `error == NONE` *and*
    /// at least one partition appears in `cluster.topics()`; a
    /// partitionless topic does not, mirroring Java's
    /// `MetadataResponse.buildCluster()`.
    #[test]
    fn test_request_future_resolves_with_partitions() {
        use crate::metadata_response_data::MetadataResponsePartition;
        let topic = "hello";
        let mut manager = setup_manager();
        let mut rx = manager.request_topic_metadata(topic.to_string(), i64::MAX);
        let res = manager.poll(0);
        assert_eq!(1, res.unsent_requests.len());

        // Build a response that carries a single partition so the
        // topic actually shows up in `cluster.topics()` (Java behavior
        // is identical — a topic-metadata entry with an empty partition
        // list never registers in the Cluster object's
        // `partitionsByTopic`).
        let mut p0 = MetadataResponsePartition::new();
        p0.set_partition_index(0);
        p0.set_leader_id(0);
        p0.set_leader_epoch(0);
        p0.set_replica_nodes(vec![0]);
        p0.set_isr_nodes(vec![0]);
        p0.set_offline_replicas(Vec::new());
        p0.set_error_code(Errors::None.code());

        let mut topic_meta = MetadataResponseTopic::new();
        topic_meta.set_name(Some(topic.to_string()));
        topic_meta.set_error_code(Errors::None.code());
        topic_meta.set_partitions(vec![p0]);
        topic_meta.set_is_internal(false);

        let nodes = mock_nodes(1);
        let brokers: Vec<MetadataResponseBroker> = nodes
            .iter()
            .map(|n| {
                let mut b = MetadataResponseBroker::new();
                b.set_node_id(n.id());
                b.set_host(n.host().to_string());
                b.set_port(n.port());
                b
            })
            .collect();

        let mut data = MetadataResponseData::new();
        data.set_cluster_id(Some("mockClusterId".to_string()));
        data.set_controller_id(0);
        data.set_brokers(brokers);
        data.set_topics(vec![topic_meta]);
        let response = MetadataResponse::new(data, ApiKeys::METADATA.latest_version());

        let request_id = manager.inflight_requests()[0].id();
        manager.on_response(request_id, 0, &response);

        let received = rx.try_recv().expect("response delivered");
        let map = received.expect("ok response");
        assert!(map.contains_key(topic), "topic present in result map: {map:?}");
    }

    /// Mirrors the Java helper `buildTopicMetadataClientResponse` shape
    /// only insofar as we want to test the manager's
    /// `ClientResponse`-typed routing surface. The bg task in Phase 10
    /// turns a `ClientResponse` into a `(request_id, &MetadataResponse)`
    /// pair before calling `on_response`; this test pins the shape of
    /// that translation.
    #[test]
    fn test_client_response_route() {
        let topic = "hello";
        let mut manager = setup_manager();
        let _rx = manager.request_topic_metadata(topic.to_string(), i64::MAX);
        let res = manager.poll(0);
        let mut unsent = res.unsent_requests.into_iter().next().unwrap();
        // Build a synthesised ClientResponse so the test exercises the
        // exact handshake the bg task will use.
        let header = RequestHeader::new(&ApiKeys::METADATA, ApiKeys::METADATA.latest_version(), "", 1).unwrap();
        let metadata_response = build_topic_metadata_response(topic, Errors::None);
        let response = ClientResponse::with_timeout(
            header,
            None,
            "0",
            0,
            0,
            false,
            false,
            None,
            None,
            Some(ConcreteResponse::Metadata(metadata_response.clone())),
        );
        // Drive the handler so the response receiver resolves.
        unsent.handler().on_complete(response);
        let rx = unsent.take_response_receiver().expect("rx still present");
        let client_response = rx.blocking_recv().unwrap().unwrap();
        let body = match client_response.response_body() {
            Some(ConcreteResponse::Metadata(m)) => m.clone(),
            other => panic!("expected Metadata response, got {other:?}"),
        };
        let request_id = manager.inflight_requests()[0].id();
        manager.on_response(request_id, 0, &body);
        assert!(manager.inflight_requests().is_empty());
    }

    /// Verifies that the manager's `request_id` allocation is unique
    /// across concurrent requests for the same topic (Java uses
    /// `this`-identity, we use a monotonic counter).
    #[test]
    fn test_request_ids_are_unique_per_request() {
        let mut manager = setup_manager();
        let _rx1 = manager.request_topic_metadata("hello".to_string(), i64::MAX);
        let _rx2 = manager.request_topic_metadata("hello".to_string(), i64::MAX);
        assert_eq!(2, manager.inflight_requests().len());
        let id1 = manager.inflight_requests()[0].id();
        let id2 = manager.inflight_requests()[1].id();
        assert_ne!(id1, id2, "concurrent requests for the same topic get distinct ids");
    }
}
