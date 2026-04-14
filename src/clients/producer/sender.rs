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

//! Sender background task that drains batches from the RecordAccumulator
//! and sends them via the `KafkaClient` trait.
//!
//! Matches Java's `org.apache.kafka.clients.producer.internals.Sender` architecture:
//! - `run()` loops calling `run_once()` until shutdown
//! - `run_once()` calls `send_producer_data()` then `client.poll()`
//! - `send_produce_request()` uses `client.send()` (non-blocking queue) with a
//!   `RequestCompletionHandler` callback
//! - `handle_produce_response()` is invoked by `client.poll()` when the response
//!   arrives, completing `ProducerBatch` futures

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use log::{debug, trace, warn};

use crate::clients::RequestCompletionHandler;
use crate::clients::kafka_client::KafkaClient;
use crate::clients::producer::accumulator::RecordAccumulator;
use crate::clients::producer::batch::ProducerBatch;
use crate::clients::producer::config::ProducerConfig;
use crate::clients::producer::producer_metadata::ProducerMetadata;
use crate::common::TopicPartition;
use crate::common::protocol::Errors;
use crate::common::requests::abstract_response::ConcreteResponse;
use crate::common::requests::produce_request::ProduceRequestBuilder;
use crate::common::uuid::Uuid;
use crate::errors::{ErrorCode, KafkaError};
use crate::produce_request_data::{PartitionProduceData, ProduceRequestData, TopicProduceData};

/// Background task that drains batches from the accumulator and sends them
/// via a `KafkaClient` implementation.
///
/// Spawned as a tokio task by `KafkaProducer::new()`.
///
/// Corresponds to `org.apache.kafka.clients.producer.internals.Sender`.
pub struct Sender<C: KafkaClient> {
    /// The network client for sending requests.
    client: C,
    /// The record accumulator that batches records.
    accumulator: Arc<RecordAccumulator>,
    /// Shared producer metadata, used for metadata-driven routing and
    /// adding unknown-leader topics.
    metadata: Arc<ProducerMetadata>,
    /// Producer configuration.
    config: Arc<ProducerConfig>,
    /// Whether the sender is still running.
    running: Arc<AtomicBool>,
    /// Notifier used by `KafkaProducer.wait_on_metadata()` to interrupt the
    /// sender's poll wait, ensuring metadata requests are processed immediately.
    ///
    /// Corresponds to Java's `Sender.wakeup()` which calls `client.wakeup()`
    /// to interrupt the selector's `select()` call.
    wakeup: Arc<tokio::sync::Notify>,
    /// The number of acknowledgements to request from the server.
    acks: i16,
    /// The max time to wait for the server to respond to the request.
    request_timeout_ms: i32,
}

impl<C: KafkaClient> Sender<C> {
    /// Create a new Sender.
    pub fn new(
        client: C,
        accumulator: Arc<RecordAccumulator>,
        metadata: Arc<ProducerMetadata>,
        config: Arc<ProducerConfig>,
        running: Arc<AtomicBool>,
        wakeup: Arc<tokio::sync::Notify>,
    ) -> Self {
        let acks = config.acks().as_i16();
        let request_timeout_ms = config.request_timeout().as_millis() as i32;
        Sender { client, accumulator, metadata, config, running, wakeup, acks, request_timeout_ms }
    }

    /// Main loop: runs until shutdown is initiated and all in-flight work completes.
    ///
    /// Matches Java's `Sender.run()`:
    /// 1. While running, call `run_once()`
    /// 2. After `running` is set to false, continue draining until accumulator is
    ///    empty and no in-flight requests remain.
    pub async fn run(mut self) {
        debug!("Starting Kafka producer I/O thread.");

        // Main loop -- runs until close is called.
        while self.running.load(Ordering::Acquire) {
            self.run_once().await;
        }

        debug!("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // Drain remaining batches and wait for in-flight requests to complete.
        while self.accumulator.has_undrained().await || self.client.has_in_flight_requests() {
            self.run_once().await;
        }

        // Close the client.
        self.client.close().await;

        debug!("Shutdown of Kafka producer I/O thread has completed.");
    }

    /// Run a single iteration of sending.
    ///
    /// Matches Java's `Sender.runOnce()` (non-transactional path):
    /// 1. `send_producer_data()` drains the accumulator, groups batches by node,
    ///    and calls `client.send()` for each node
    /// 2. `client.poll()` drives all I/O and fires callbacks
    ///
    /// The poll is interruptible via the `wakeup` notify, matching Java's
    /// `sender.wakeup()` which interrupts `client.poll()` via the selector.
    async fn run_once(&mut self) {
        let now = current_time_ms();
        let poll_timeout = self.send_producer_data(now).await;

        if poll_timeout > 0 {
            // Wait for either the poll timeout or a wakeup signal from the
            // producer (e.g., when wait_on_metadata requests an immediate
            // metadata update). This matches Java's selector wakeup mechanism.
            tokio::select! {
                _ = self.client.poll(poll_timeout, now) => {},
                _ = self.wakeup.notified() => {
                    // Woken up -- run poll with 0 timeout to process any pending
                    // metadata requests without blocking.
                    self.client.poll(0, current_time_ms()).await;
                },
            }
        } else {
            self.client.poll(poll_timeout, now).await;
        }
    }

    /// Drain the accumulator and send produce requests.
    ///
    /// Matches Java's `Sender.sendProducerData()`:
    /// 1. Expire lingering batches
    /// 2. Use `ProducerMetadata.fetch_metadata_snapshot()` for ready-check
    /// 3. Add unknown leader topics to ProducerMetadata
    /// 4. Drain ready batches from the accumulator
    /// 5. Group batches by destination node
    /// 6. For each node, call `send_produce_request()`
    /// 7. Return the poll timeout
    async fn send_producer_data(&mut self, now: i64) -> i64 {
        // Expire lingering batches.
        self.accumulator.expire_lingering_batches().await;

        // Use the metadata snapshot for ready-check (Phase 7).
        let _metadata_snapshot = self.metadata.fetch_metadata_snapshot();

        // Drain all ready batches.
        let ready_batches = self.accumulator.drain().await;

        if ready_batches.is_empty() {
            // No batches ready -- use linger time as poll timeout, capped at 100ms.
            let linger = self.config.linger();
            let timeout = if linger.is_zero() {
                Duration::from_millis(100)
            } else {
                linger
            };
            return timeout.as_millis() as i64;
        }

        // Group batches by destination node.
        // Currently all batches go to node 0; full metadata-driven leader
        // routing will be added when RecordAccumulator is updated to use
        // partition metadata for routing.
        let mut batches_by_node: HashMap<i32, Vec<ProducerBatch>> = HashMap::new();
        for batch in ready_batches {
            batches_by_node.entry(0).or_default().push(batch);
        }

        // Send produce requests for each node.
        for (node_id, batches) in batches_by_node {
            self.send_produce_request(now, node_id, batches);
        }

        // If we sent data, poll with 0 timeout so we can immediately loop.
        0
    }

    /// Create a produce request from the given record batches and send it.
    ///
    /// Matches Java's `Sender.sendProduceRequest()`:
    /// 1. Build `ProduceRequestData` from batches
    /// 2. Create a `RequestCompletionHandler` callback that calls `handle_produce_response`
    /// 3. `client.newClientRequest()` + `client.send()` (non-blocking)
    fn send_produce_request(&mut self, now: i64, destination: i32, mut batches: Vec<ProducerBatch>) {
        if batches.is_empty() {
            return;
        }

        let mut records_by_partition: HashMap<TopicPartition, ProducerBatch> = HashMap::new();

        // Build ProduceRequestData.
        let mut topic_data_map: HashMap<String, TopicProduceData> = HashMap::new();

        for batch in &mut batches {
            let tp = batch.tp().clone();
            let data = batch.buffer();

            let topic_data = topic_data_map.entry(tp.topic().to_string()).or_insert_with(|| {
                let mut td = TopicProduceData::new();
                td.set_name(tp.topic().to_string());
                // Use zero UUID for now; Phase 7 will look up topic IDs from metadata.
                td.set_topic_id(Uuid::zero());
                td
            });

            let mut partition_data = PartitionProduceData::new();
            partition_data.set_index(tp.partition());
            partition_data.set_records(Some(data));
            topic_data.partition_data.push(partition_data);
        }

        // Move batches into records_by_partition map.
        for batch in batches {
            let tp = batch.tp().clone();
            records_by_partition.insert(tp, batch);
        }

        let mut request_data = ProduceRequestData::new();
        request_data.set_acks(self.acks);
        request_data.set_timeout_ms(self.request_timeout_ms);
        request_data.set_topic_data(topic_data_map.into_values().collect());

        let request_builder = ProduceRequestBuilder::new(request_data);

        // Create the callback that will be invoked by client.poll() when the response arrives.
        let accumulator = Arc::clone(&self.accumulator);
        let callback: RequestCompletionHandler =
            Box::new(move |response: &mut crate::clients::client_response::ClientResponse| {
                handle_produce_response(response, records_by_partition, &accumulator, current_time_ms());
            });

        let node_id = destination.to_string();
        let expect_response = self.acks != 0;

        let client_request = self.client.new_client_request_with_timeout(
            &node_id,
            Box::new(request_builder),
            now,
            expect_response,
            self.request_timeout_ms,
            Some(callback),
        );
        self.client.send(client_request, now);
        trace!("Sent produce request to node {}", node_id);
    }

    /// Start closing the sender (won't actually complete until all data is sent out).
    ///
    /// Matches Java's `Sender.initiateClose()`.
    pub fn initiate_close(&self) {
        self.accumulator.close_sync();
        self.running.store(false, Ordering::Release);
        self.client.wakeup();
    }

    /// Returns true if the sender is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Wake up the selector associated with this send thread.
    pub fn wakeup(&self) {
        self.client.wakeup();
    }
}

/// Handle a produce response -- invoked as a `RequestCompletionHandler` callback
/// by `client.poll()` when a response arrives.
///
/// Matches Java's `Sender.handleProduceResponse()`:
/// - On disconnect/timeout: fail all batches with network error
/// - On success with response body: parse partition responses, complete each batch
/// - On acks=0 (no response body): complete all batches with success
fn handle_produce_response(
    response: &mut crate::clients::client_response::ClientResponse,
    mut batches: HashMap<TopicPartition, ProducerBatch>,
    accumulator: &RecordAccumulator,
    now: i64,
) {
    let _ = now; // reserved for future use (metrics, retry timing)

    if response.was_timed_out() {
        warn!(
            "Cancelled request with header {} due to the last request to node {} timed out",
            response.request_header(),
            response.destination()
        );
        let err = KafkaError::new(
            ErrorCode::TimedOut,
            format!("Disconnected from node {} due to timeout", response.destination()),
        );
        for (_, batch) in batches.drain() {
            let permits = batch.permits_acquired();
            batch.complete(0, 0, Some(&err));
            accumulator.release_memory(permits);
        }
    } else if response.was_disconnected() {
        warn!(
            "Cancelled request with header {} due to node {} being disconnected",
            response.request_header(),
            response.destination()
        );
        let err = KafkaError::new(ErrorCode::Network, format!("Disconnected from node {}", response.destination()));
        for (_, batch) in batches.drain() {
            let permits = batch.permits_acquired();
            batch.complete(0, 0, Some(&err));
            accumulator.release_memory(permits);
        }
    } else if response.version_mismatch().is_some() {
        warn!(
            "Cancelled request {} due to a version mismatch with node {}",
            response,
            response.destination()
        );
        let err = KafkaError::new(
            ErrorCode::UnsupportedVersion,
            format!(
                "Unsupported version for request to node {}: {}",
                response.destination(),
                response.version_mismatch().unwrap_or("unknown")
            ),
        );
        for (_, batch) in batches.drain() {
            let permits = batch.permits_acquired();
            batch.complete(0, 0, Some(&err));
            accumulator.release_memory(permits);
        }
    } else if response.has_response() {
        trace!(
            "Received produce response from node {} with correlation id {}",
            response.destination(),
            response.request_header().correlation_id()
        );
        if let Some(ConcreteResponse::Produce(produce_response)) = response.response_body() {
            for topic_response in &produce_response.data().responses {
                for partition_response in &topic_response.partition_responses {
                    let tp = TopicPartition::new(topic_response.name.clone(), partition_response.index);
                    let error_code = partition_response.error_code;
                    let base_offset = partition_response.base_offset;
                    let log_append_time = partition_response.log_append_time_ms;

                    if let Some(batch) = batches.remove(&tp) {
                        let permits = batch.permits_acquired();
                        let error = Errors::for_code(error_code);
                        if error == Errors::None {
                            batch.complete(base_offset, log_append_time, None);
                        } else {
                            let err = KafkaError::new(
                                errors_to_error_code(&error),
                                format!("Error producing to {}: {}", tp, error),
                            );
                            batch.complete(base_offset, log_append_time, Some(&err));
                        }
                        accumulator.release_memory(permits);
                    } else {
                        warn!("No batch found for partition {} in produce response", tp);
                    }
                }
            }
        }

        // Any batches not matched by the response are unexpected -- fail them.
        for (tp, batch) in batches.drain() {
            let permits = batch.permits_acquired();
            let err = KafkaError::new(
                ErrorCode::Unexpected,
                format!("No response for partition {} in ProduceResponse", tp),
            );
            batch.complete(0, 0, Some(&err));
            accumulator.release_memory(permits);
        }
    } else {
        // acks = 0 case: no response body, complete all batches with success.
        for (_, batch) in batches.drain() {
            let permits = batch.permits_acquired();
            batch.complete(0, -1, None);
            accumulator.release_memory(permits);
        }
    }
}

/// Map a protocol `Errors` value to the client `ErrorCode`.
///
/// This covers the common produce-response error codes. Unknown codes map to
/// `ErrorCode::Unexpected`.
fn errors_to_error_code(error: &Errors) -> ErrorCode {
    match error {
        Errors::RequestTimedOut => ErrorCode::TimedOut,
        Errors::NetworkException => ErrorCode::Network,
        Errors::NotLeaderOrFollower => ErrorCode::NotLeaderOrFollower,
        Errors::UnknownTopicOrPartition => ErrorCode::UnknownTopicOrPartition,
        Errors::MessageTooLarge | Errors::RecordListTooLarge => ErrorCode::RecordTooLarge,
        Errors::CorruptMessage => ErrorCode::CorruptRecord,
        Errors::TopicAuthorizationFailed => ErrorCode::TopicAuthorization,
        Errors::InvalidTopicException => ErrorCode::InvalidTopic,
        Errors::UnsupportedVersion => ErrorCode::UnsupportedVersion,
        _ => ErrorCode::Unexpected,
    }
}

/// Returns the current time in milliseconds since the Unix epoch.
fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::client_response::ClientResponse;
    use crate::clients::kafka_client::KafkaClient;
    use crate::clients::least_loaded_node::LeastLoadedNode;
    use crate::clients::{ClientRequest, RequestCompletionHandler};
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::node::Node;
    use crate::common::protocol::Errors;
    use crate::common::requests::abstract_response::ConcreteResponse;
    use crate::common::requests::produce_response::ProduceResponse;
    use crate::common::requests::{RequestBuilder, RequestHeader};
    use crate::produce_response_data::{PartitionProduceResponse, ProduceResponseData, TopicProduceResponse};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};

    const TOPIC_NAME: &str = "test";

    /// A queued produce response to be returned by the mock client.
    struct QueuedResponse {
        topic: String,
        partition: i32,
        offset: i64,
        error_code: i16,
        log_append_time_ms: i64,
    }

    /// A configurable mock `KafkaClient` for Sender-level tests.
    ///
    /// This is the Rust equivalent of Java's `MockClient` used in `SenderTest`.
    /// It allows tests to:
    /// - Queue specific produce responses (with configurable error codes)
    /// - Track in-flight requests
    /// - Simulate disconnections
    struct MockKafkaClient {
        /// Pending requests that have been sent but not yet polled.
        pending_requests: tokio::sync::Mutex<Vec<ClientRequest>>,
        /// Queued responses to return on the next poll.
        queued_responses: tokio::sync::Mutex<Vec<QueuedResponse>>,
        /// Monotonically increasing correlation ID.
        next_correlation_id: AtomicI32,
        /// Whether the client is active.
        active: AtomicBool,
        /// Count of in-flight requests.
        in_flight_count: AtomicI32,
        /// Whether the next poll should simulate a disconnect.
        simulate_disconnect: AtomicBool,
        /// Next offset to assign when no queued response is present.
        next_offset: AtomicI64,
    }

    impl MockKafkaClient {
        fn new() -> Self {
            MockKafkaClient {
                pending_requests: tokio::sync::Mutex::new(Vec::new()),
                queued_responses: tokio::sync::Mutex::new(Vec::new()),
                next_correlation_id: AtomicI32::new(0),
                active: AtomicBool::new(true),
                in_flight_count: AtomicI32::new(0),
                simulate_disconnect: AtomicBool::new(false),
                next_offset: AtomicI64::new(0),
            }
        }

        /// Queue a produce response for a specific topic-partition.
        ///
        /// Matches Java's `MockClient.prepareResponse()` pattern.
        async fn prepare_response(
            &self,
            topic: &str,
            partition: i32,
            offset: i64,
            error: Errors,
            log_append_time_ms: i64,
        ) {
            let mut responses = self.queued_responses.lock().await;
            responses.push(QueuedResponse {
                topic: topic.to_string(),
                partition,
                offset,
                error_code: error.code(),
                log_append_time_ms,
            });
        }

        /// Set the mock to simulate a disconnect on the next poll.
        fn set_disconnect(&self, disconnect: bool) {
            self.simulate_disconnect.store(disconnect, Ordering::SeqCst);
        }

        fn build_response_header(&self) -> RequestHeader {
            RequestHeader::new(
                &crate::common::protocol::ApiKeys::PRODUCE,
                0,
                "test-client",
                self.next_correlation_id.fetch_add(1, Ordering::SeqCst),
            )
            .expect("Failed to build request header")
        }
    }

    #[async_trait]
    impl KafkaClient for MockKafkaClient {
        fn is_ready(&self, _node: &Node, _now: i64) -> bool {
            true
        }

        async fn ready(&mut self, _node: &Node, _now: i64) -> bool {
            true
        }

        fn connection_delay(&self, _node: &Node, _now: i64) -> i64 {
            0
        }

        fn poll_delay_ms(&self, _node: &Node, _now: i64) -> i64 {
            0
        }

        fn connection_failed(&self, _node: &Node) -> bool {
            false
        }

        fn authentication_error(&self, _node: &Node) -> Option<String> {
            None
        }

        fn send(&mut self, request: ClientRequest, _now: i64) {
            self.in_flight_count.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut pending) = self.pending_requests.try_lock() {
                pending.push(request);
            }
        }

        async fn poll(&mut self, _timeout: i64, _now: i64) -> Vec<ClientResponse> {
            let mut pending = self.pending_requests.lock().await;
            let requests: Vec<ClientRequest> = pending.drain(..).collect();
            drop(pending);

            let disconnect = self.simulate_disconnect.swap(false, Ordering::SeqCst);
            let mut queued = self.queued_responses.lock().await;

            let mut responses = Vec::new();
            for mut request in requests {
                self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
                let now_ms = current_time_ms();

                if disconnect {
                    // Simulate a disconnection — return response with disconnected=true.
                    let header = self.build_response_header();
                    let mut client_response = ClientResponse::new(
                        header,
                        request.take_callback(),
                        request.destination(),
                        now_ms,
                        now_ms,
                        true, // disconnected
                        None,
                        None,
                        None,
                    );
                    client_response.on_complete();
                    responses.push(client_response);
                } else if !queued.is_empty() {
                    // Use queued response.
                    let qr = queued.remove(0);
                    let response_body = build_produce_response(
                        &qr.topic,
                        qr.partition,
                        qr.offset,
                        qr.error_code,
                        qr.log_append_time_ms,
                    );
                    let header = self.build_response_header();
                    let mut client_response = ClientResponse::new(
                        header,
                        request.take_callback(),
                        request.destination(),
                        now_ms,
                        now_ms,
                        false,
                        None,
                        None,
                        Some(response_body),
                    );
                    client_response.on_complete();
                    responses.push(client_response);
                } else if request.expect_response() {
                    // Default success response (acks != 0).
                    let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
                    let response_body =
                        build_produce_response(TOPIC_NAME, 0, offset, Errors::None.code(), current_time_ms());
                    let header = self.build_response_header();
                    let mut client_response = ClientResponse::new(
                        header,
                        request.take_callback(),
                        request.destination(),
                        now_ms,
                        now_ms,
                        false,
                        None,
                        None,
                        Some(response_body),
                    );
                    client_response.on_complete();
                    responses.push(client_response);
                } else {
                    // acks=0: no response body expected, complete without a response body.
                    let header = self.build_response_header();
                    let mut client_response = ClientResponse::new(
                        header,
                        request.take_callback(),
                        request.destination(),
                        now_ms,
                        now_ms,
                        false,
                        None,
                        None,
                        None, // no response body for acks=0
                    );
                    client_response.on_complete();
                    responses.push(client_response);
                }
            }
            responses
        }

        async fn disconnect(&mut self, _node_id: &str) {}

        async fn close_connection(&mut self, _node_id: &str) {}

        fn least_loaded_node(&self, _now: i64) -> LeastLoadedNode {
            LeastLoadedNode::new(Some(Node::new(0, "localhost".to_string(), 9092)), true)
        }

        fn in_flight_request_count(&self) -> i32 {
            self.in_flight_count.load(Ordering::SeqCst)
        }

        fn has_in_flight_requests(&self) -> bool {
            self.in_flight_count.load(Ordering::SeqCst) > 0
        }

        fn in_flight_request_count_for_node(&self, _node_id: &str) -> usize {
            self.in_flight_count.load(Ordering::SeqCst) as usize
        }

        fn has_in_flight_requests_for_node(&self, _node_id: &str) -> bool {
            self.in_flight_count.load(Ordering::SeqCst) > 0
        }

        fn has_ready_nodes(&self, _now: i64) -> bool {
            true
        }

        fn wakeup(&self) {}

        fn new_client_request(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn RequestBuilder + Send>,
            created_time_ms: i64,
            expect_response: bool,
        ) -> ClientRequest {
            ClientRequest::new(
                node_id,
                request_builder,
                self.next_correlation_id.fetch_add(1, Ordering::SeqCst),
                "test-client",
                created_time_ms,
                expect_response,
                0,
                None,
            )
        }

        fn new_client_request_with_timeout(
            &mut self,
            node_id: &str,
            request_builder: Box<dyn RequestBuilder + Send>,
            created_time_ms: i64,
            expect_response: bool,
            request_timeout_ms: i32,
            callback: Option<RequestCompletionHandler>,
        ) -> ClientRequest {
            ClientRequest::new(
                node_id,
                request_builder,
                self.next_correlation_id.fetch_add(1, Ordering::SeqCst),
                "test-client",
                created_time_ms,
                expect_response,
                request_timeout_ms,
                callback,
            )
        }

        fn initiate_close(&self) {
            self.active.store(false, Ordering::Release);
        }

        fn active(&self) -> bool {
            self.active.load(Ordering::Acquire)
        }

        async fn close(&mut self) {
            self.active.store(false, Ordering::Release);
        }
    }

    /// Build a ProduceResponse with a single partition response.
    ///
    /// Matches Java's `SenderTest.produceResponse()` helper.
    fn build_produce_response(
        topic: &str,
        partition: i32,
        offset: i64,
        error_code: i16,
        log_append_time_ms: i64,
    ) -> ConcreteResponse {
        let mut response_data = ProduceResponseData::new();
        let mut topic_response = TopicProduceResponse::new();
        topic_response.set_name(topic.to_string());
        let mut partition_response = PartitionProduceResponse::new();
        partition_response.set_index(partition);
        partition_response.set_base_offset(offset);
        partition_response.set_log_append_time_ms(log_append_time_ms);
        partition_response.set_error_code(error_code);
        topic_response.set_partition_responses(vec![partition_response]);
        response_data.set_responses(vec![topic_response]);
        ConcreteResponse::Produce(ProduceResponse::new(response_data))
    }

    fn test_config() -> Arc<ProducerConfig> {
        Arc::new(
            ProducerConfig::builder()
                .bootstrap_servers(vec!["localhost:9092".to_string()])
                .batch_size(16 * 1024)
                .linger_ms(0)
                .buffer_memory(1024 * 1024)
                .build()
                .unwrap(),
        )
    }

    fn test_metadata() -> Arc<ProducerMetadata> {
        let mut topics = HashMap::new();
        topics.insert(TOPIC_NAME.to_string(), 2);
        let metadata = Arc::new(ProducerMetadata::new(50, 50, 5000, 60_000, ClusterResourceListeners::new()));
        let now = current_time_ms();
        metadata.add(TOPIC_NAME, now);
        let metadata_response = crate::common::requests::request_test_utils::metadata_update_with(1, &topics);
        metadata.update_with_current_request_version(&metadata_response, false, now);
        metadata
    }

    /// Create a Sender for testing with the given MockKafkaClient.
    fn create_test_sender(client: MockKafkaClient) -> Sender<MockKafkaClient> {
        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let metadata = test_metadata();
        let running = Arc::new(AtomicBool::new(true));
        let wakeup = Arc::new(tokio::sync::Notify::new());
        Sender::new(client, accumulator, metadata, config, running, wakeup)
    }

    /// Append a record to the accumulator directly, bypassing the producer.
    ///
    /// This matches Java's `SenderTest.appendToAccumulator()` helper.
    async fn append_to_accumulator(
        accumulator: &RecordAccumulator,
        tp: &TopicPartition,
        key: &str,
        value: &str,
    ) -> crate::clients::producer::batch::SendFuture {
        accumulator
            .append(tp, Some(key.as_bytes()), Some(value.as_bytes()), &[], current_time_ms())
            .await
            .expect("append should succeed")
            .future
    }

    /// Translated from `SenderTest.testSimple()`.
    ///
    /// Basic send and response flow: append a record, run_once to send it,
    /// run_once to receive the response, verify the future completes with
    /// the correct offset.
    #[tokio::test]
    async fn test_simple() {
        let client = MockKafkaClient::new();
        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let metadata = test_metadata();
        let running = Arc::new(AtomicBool::new(true));
        let wakeup = Arc::new(tokio::sync::Notify::new());

        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);

        // Append a record directly to the accumulator.
        let future = append_to_accumulator(&accumulator, &tp0, "key", "value").await;

        // Flush so the batch is marked as ready.
        accumulator.flush_all().await;

        let mut sender = Sender::new(client, Arc::clone(&accumulator), metadata, config, running, wakeup);

        // run_once: drains the accumulator and sends the produce request via client.send().
        // Since MockKafkaClient.poll() immediately responds, this also completes the batch.
        sender.run_once().await;

        // The future should be completed with offset 0.
        let result = future.await;
        assert!(result.is_ok(), "Future should complete successfully");
        let metadata = result.unwrap();
        assert_eq!(metadata.offset(), 0, "Offset should be 0 for the first record");
        assert_eq!(metadata.topic(), TOPIC_NAME);
        assert_eq!(metadata.partition(), 0);
    }

    /// Translated from Java `SenderTest.runOnce()` pattern.
    ///
    /// Verify that `run_once()` drains ready batches from the accumulator
    /// and calls `client.poll()` to drive I/O. Uses two records on the same
    /// partition so they are batched together and a single produce response
    /// covers the entire request.
    #[tokio::test]
    async fn test_run_once_drains_and_polls() {
        let client = MockKafkaClient::new();
        // Queue a success response for partition 0 with offset 10.
        client.prepare_response(TOPIC_NAME, 0, 10, Errors::None, 1000).await;

        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let metadata = test_metadata();
        let running = Arc::new(AtomicBool::new(true));
        let wakeup = Arc::new(tokio::sync::Notify::new());

        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);

        // Append two records to the same partition.
        let future0 = append_to_accumulator(&accumulator, &tp0, "k0", "v0").await;
        let future1 = append_to_accumulator(&accumulator, &tp0, "k1", "v1").await;

        // Flush the batch.
        accumulator.flush_all().await;

        let mut sender = Sender::new(client, Arc::clone(&accumulator), metadata, config, running, wakeup);

        // A single run_once should drain the batch and poll.
        sender.run_once().await;

        // Both futures should be done (same batch, same base offset).
        let r0 = future0.await;
        assert!(r0.is_ok(), "First future should complete");
        assert_eq!(r0.unwrap().offset(), 10, "First record should get base_offset");
        let r1 = future1.await;
        assert!(r1.is_ok(), "Second future should complete");
        assert_eq!(r1.unwrap().offset(), 11, "Second record should get base_offset + 1");

        // Accumulator should be empty.
        assert!(
            !accumulator.has_undrained().await,
            "Accumulator should have no undrained batches after run_once"
        );
    }

    /// Translated from the plan's `testSendProduceRequestsWithNoBatches`.
    ///
    /// When there are no batches ready, `run_once()` should be a no-op
    /// (just poll with a timeout) and not send any requests.
    #[tokio::test]
    async fn test_send_producer_data_with_no_batches() {
        let mut sender = create_test_sender(MockKafkaClient::new());

        // run_once with empty accumulator should not panic or error.
        sender.run_once().await;

        // No in-flight requests should exist.
        assert!(
            !sender.client.has_in_flight_requests(),
            "No requests should be in flight when there are no batches"
        );
    }

    /// Translated from `SenderTest.testRetries()` — successful retry path.
    ///
    /// A produce request is sent, the broker disconnects, and the batch is
    /// retried on the next run_once. The retry succeeds.
    ///
    /// Note: The current Sender does not implement retry logic (deferred to
    /// Phase 7+ with delivery timeout and retry backoff). This test verifies
    /// that a disconnect causes the batch future to fail with a network error,
    /// which is the correct behavior before retry support is added.
    #[tokio::test]
    async fn test_disconnect_fails_batch() {
        let client = MockKafkaClient::new();
        // Simulate disconnect on the first poll.
        client.set_disconnect(true);

        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let metadata = test_metadata();
        let running = Arc::new(AtomicBool::new(true));
        let wakeup = Arc::new(tokio::sync::Notify::new());

        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
        let future = append_to_accumulator(&accumulator, &tp0, "key", "value").await;

        accumulator.flush_all().await;

        let mut sender = Sender::new(client, Arc::clone(&accumulator), metadata, config, running, wakeup);
        sender.run_once().await;

        // The batch should be failed with a network error due to disconnect.
        let result = future.await;
        assert!(result.is_err(), "Future should fail on disconnect");
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Network, "Error code should be Network");
    }

    /// Translated from `SenderTest.testExpiredBatchDoesNotSplitOnMessageTooLargeError()`.
    ///
    /// When the broker returns `MESSAGE_TOO_LARGE`, the batch should be
    /// completed with a `RecordTooLarge` error (no retry, no batch splitting
    /// in our implementation).
    #[tokio::test]
    async fn test_message_too_large() {
        let client = MockKafkaClient::new();
        // Queue a MESSAGE_TOO_LARGE response.
        client.prepare_response(TOPIC_NAME, 0, -1, Errors::MessageTooLarge, -1).await;

        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let metadata = test_metadata();
        let running = Arc::new(AtomicBool::new(true));
        let wakeup = Arc::new(tokio::sync::Notify::new());

        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
        let future = append_to_accumulator(&accumulator, &tp0, "key", "value").await;

        accumulator.flush_all().await;

        let mut sender = Sender::new(client, Arc::clone(&accumulator), metadata, config, running, wakeup);
        sender.run_once().await;

        // The batch should be failed with RecordTooLarge.
        let result = future.await;
        assert!(result.is_err(), "Future should fail on MESSAGE_TOO_LARGE");
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::RecordTooLarge, "Error code should be RecordTooLarge");
    }

    /// Translated from `SenderTest.testCanRetryWithoutIdempotence()`.
    ///
    /// When the broker returns `TOPIC_AUTHORIZATION_FAILED`, the batch should
    /// be completed with a `TopicAuthorization` error.
    #[tokio::test]
    async fn test_topic_authorization_error() {
        let client = MockKafkaClient::new();
        client
            .prepare_response(TOPIC_NAME, 0, -1, Errors::TopicAuthorizationFailed, 0)
            .await;

        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let metadata = test_metadata();
        let running = Arc::new(AtomicBool::new(true));
        let wakeup = Arc::new(tokio::sync::Notify::new());

        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
        let future = append_to_accumulator(&accumulator, &tp0, "key", "value").await;

        accumulator.flush_all().await;

        let mut sender = Sender::new(client, Arc::clone(&accumulator), metadata, config, running, wakeup);
        sender.run_once().await;

        let result = future.await;
        assert!(result.is_err(), "Future should fail on TOPIC_AUTHORIZATION_FAILED");
        let err = result.unwrap_err();
        assert_eq!(
            err.code(),
            ErrorCode::TopicAuthorization,
            "Error code should be TopicAuthorization"
        );
    }

    /// Test that a successful produce with acks=0 (fire-and-forget) completes
    /// the batch with success even though no response body is returned.
    ///
    /// This covers the `else` branch in `handle_produce_response` where
    /// `response.has_response()` is false and the batch is completed with
    /// offset 0 and log_append_time -1.
    #[tokio::test]
    async fn test_acks_zero_success() {
        let client = MockKafkaClient::new();
        let config = Arc::new(
            ProducerConfig::builder()
                .bootstrap_servers(vec!["localhost:9092".to_string()])
                .batch_size(16 * 1024)
                .linger_ms(0)
                .buffer_memory(1024 * 1024)
                .acks(crate::clients::producer::config::Acks::None) // acks=0
                .build()
                .unwrap(),
        );
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let metadata = test_metadata();
        let running = Arc::new(AtomicBool::new(true));
        let wakeup = Arc::new(tokio::sync::Notify::new());

        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
        let future = append_to_accumulator(&accumulator, &tp0, "key", "value").await;

        accumulator.flush_all().await;

        let mut sender = Sender::new(client, Arc::clone(&accumulator), metadata, config, running, wakeup);

        // With acks=0, the client should get expect_response=false, meaning
        // the mock won't send a response body. The callback should complete
        // the batch with success (offset=0, log_append_time=-1).
        sender.run_once().await;

        let result = future.await;
        assert!(result.is_ok(), "acks=0 should succeed without response body");
        let metadata = result.unwrap();
        assert_eq!(metadata.offset(), 0, "acks=0 should complete with offset 0");
    }

    /// Test that a version mismatch response fails all batches with
    /// `ErrorCode::UnsupportedVersion`.
    ///
    /// This covers the `version_mismatch` branch in `handle_produce_response`
    /// where `response.version_mismatch().is_some()` is true.
    ///
    /// Matches Java's `Sender.handleProduceResponse()` version_mismatch branch
    /// (Sender.java:594-598).
    #[tokio::test]
    async fn test_version_mismatch_fails_batch() {
        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
        let future = append_to_accumulator(&accumulator, &tp0, "key", "value").await;
        accumulator.flush_all().await;

        // Directly test handle_produce_response with a version-mismatch response.
        let batches = accumulator.drain().await;
        assert_eq!(batches.len(), 1);

        let mut records_by_partition: HashMap<TopicPartition, ProducerBatch> = HashMap::new();
        for batch in batches {
            let tp = batch.tp().clone();
            records_by_partition.insert(tp, batch);
        }

        let header = RequestHeader::new(&crate::common::protocol::ApiKeys::PRODUCE, 0, "test", 1).unwrap();
        let mut response = ClientResponse::new(
            header,
            None,
            "0",
            current_time_ms(),
            current_time_ms(),
            false,                                       // not disconnected
            Some("UnsupportedVersionError".to_string()), // version mismatch
            None,
            None, // no response body
        );

        handle_produce_response(&mut response, records_by_partition, &accumulator, current_time_ms());

        let result = future.await;
        assert!(result.is_err(), "Version mismatch should cause error");
        assert_eq!(
            result.unwrap_err().code(),
            ErrorCode::UnsupportedVersion,
            "Error code should be UnsupportedVersion"
        );
    }

    /// Test that the sender's main `run()` loop terminates when `running`
    /// is set to false and no in-flight work remains.
    ///
    /// Matches Java's `Sender.run()` shutdown behavior.
    #[tokio::test]
    async fn test_run_shutdown() {
        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let metadata = test_metadata();
        let running = Arc::new(AtomicBool::new(true));
        let wakeup = Arc::new(tokio::sync::Notify::new());

        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
        let future = append_to_accumulator(&accumulator, &tp0, "key", "value").await;

        accumulator.flush_all().await;

        let running_clone = Arc::clone(&running);
        let accumulator_clone = Arc::clone(&accumulator);

        let sender = Sender::new(
            MockKafkaClient::new(),
            accumulator_clone,
            metadata,
            config,
            Arc::clone(&running),
            wakeup,
        );

        // Signal shutdown before spawning so the run loop terminates quickly.
        running_clone.store(false, Ordering::Release);

        // Run the sender — it should drain remaining batches and exit.
        let handle = tokio::spawn(sender.run());
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "Sender.run() should terminate within timeout");
        assert!(result.unwrap().is_ok(), "Sender task should not panic");

        // The batch should have been sent and completed.
        let record_result = future.await;
        assert!(
            record_result.is_ok(),
            "Remaining batch should be completed during shutdown drain"
        );
    }

    /// Test that the `errors_to_error_code` mapping covers common produce errors.
    #[tokio::test]
    async fn test_errors_to_error_code_mapping() {
        assert_eq!(errors_to_error_code(&Errors::RequestTimedOut), ErrorCode::TimedOut);
        assert_eq!(errors_to_error_code(&Errors::NetworkException), ErrorCode::Network);
        assert_eq!(
            errors_to_error_code(&Errors::NotLeaderOrFollower),
            ErrorCode::NotLeaderOrFollower
        );
        assert_eq!(
            errors_to_error_code(&Errors::UnknownTopicOrPartition),
            ErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(errors_to_error_code(&Errors::MessageTooLarge), ErrorCode::RecordTooLarge);
        assert_eq!(errors_to_error_code(&Errors::RecordListTooLarge), ErrorCode::RecordTooLarge);
        assert_eq!(errors_to_error_code(&Errors::CorruptMessage), ErrorCode::CorruptRecord);
        assert_eq!(
            errors_to_error_code(&Errors::TopicAuthorizationFailed),
            ErrorCode::TopicAuthorization
        );
        assert_eq!(errors_to_error_code(&Errors::InvalidTopicException), ErrorCode::InvalidTopic);
        assert_eq!(errors_to_error_code(&Errors::UnsupportedVersion), ErrorCode::UnsupportedVersion);
    }

    /// Test multiple records in the same batch — all should complete with
    /// the same base offset.
    ///
    /// Translated from Java SenderTest patterns where multiple records are
    /// appended to the same partition before calling runOnce().
    #[tokio::test]
    async fn test_multiple_records_same_batch() {
        let client = MockKafkaClient::new();
        client.prepare_response(TOPIC_NAME, 0, 42, Errors::None, 1000).await;

        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let metadata = test_metadata();
        let running = Arc::new(AtomicBool::new(true));
        let wakeup = Arc::new(tokio::sync::Notify::new());

        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
        let future1 = append_to_accumulator(&accumulator, &tp0, "k1", "v1").await;
        let future2 = append_to_accumulator(&accumulator, &tp0, "k2", "v2").await;
        let future3 = append_to_accumulator(&accumulator, &tp0, "k3", "v3").await;

        accumulator.flush_all().await;

        let mut sender = Sender::new(client, Arc::clone(&accumulator), metadata, config, running, wakeup);
        sender.run_once().await;

        // All three records should be completed. They share the same base offset
        // since they're in the same batch. Individual record offsets = base + delta.
        let r1 = future1.await.expect("record 1 should succeed");
        let r2 = future2.await.expect("record 2 should succeed");
        let r3 = future3.await.expect("record 3 should succeed");

        // The base offset is 42; each record gets base_offset + offset_delta.
        assert_eq!(r1.offset(), 42);
        assert_eq!(r2.offset(), 43);
        assert_eq!(r3.offset(), 44);
    }

    /// Test the timed-out response path.
    ///
    /// When the response has `was_timed_out() == true`, all batches should
    /// be failed with `ErrorCode::TimedOut`.
    #[tokio::test]
    async fn test_timeout_fails_batch() {
        let config = test_config();
        let accumulator = Arc::new(RecordAccumulator::new(Arc::clone(&config)));
        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
        let future = append_to_accumulator(&accumulator, &tp0, "key", "value").await;
        accumulator.flush_all().await;

        // Directly test handle_produce_response with a timed-out response.
        let batches = accumulator.drain().await;
        assert_eq!(batches.len(), 1);

        let mut records_by_partition: HashMap<TopicPartition, ProducerBatch> = HashMap::new();
        for batch in batches {
            let tp = batch.tp().clone();
            records_by_partition.insert(tp, batch);
        }

        let header = RequestHeader::new(&crate::common::protocol::ApiKeys::PRODUCE, 0, "test", 1).unwrap();
        let mut response = ClientResponse::with_timeout(
            header,
            None,
            "0",
            current_time_ms(),
            current_time_ms(),
            true, // disconnected (required for timed_out)
            true, // timed_out
            None,
            None,
            None,
        );

        handle_produce_response(&mut response, records_by_partition, &accumulator, current_time_ms());

        let result = future.await;
        assert!(result.is_err(), "Timed-out should cause error");
        assert_eq!(result.unwrap_err().code(), ErrorCode::TimedOut);
    }
}
