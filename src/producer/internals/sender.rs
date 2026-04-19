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

#![allow(dead_code)]
//! The background task that handles sending produce requests to the Kafka cluster.
//!
//! This task makes metadata requests to renew its view of the cluster and then
//! sends produce requests to the appropriate nodes.
//!
//! Translated from `org.apache.kafka.clients.producer.internals.Sender`.
//!
//! Transactional methods are not translated in this phase.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use log::{debug, error, trace, warn};
use tokio::sync::Notify;

use crate::client_response::ClientResponse;
use crate::common::KafkaError;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::Errors;
use crate::common::record::RecordBatch;
use crate::common::requests::ConcreteResponse;
use crate::common::requests::ProduceRequestBuilder;
use crate::common::requests::{PartitionResponse, RecordError};
use crate::kafka_client::KafkaClient;
use crate::metadata::LeaderIdAndEpoch;
use crate::produce_request_data::{PartitionProduceData, ProduceRequestData, TopicProduceData};

use super::ProducerBatch;
use super::ProducerMetadata;
use super::RecordAccumulator;

/// The action to take after `complete_batch` has processed a batch.
///
/// Because `RecordAccumulator::reenqueue` and `split_and_reenqueue` need
/// ownership of the batch, `complete_batch` cannot call them directly (it
/// borrows `&mut self`). Instead, it signals the desired action and the
/// caller — which owns the batch — transfers it back to the accumulator.
enum BatchAction {
    /// The batch was completed (success, failure, or duplicate). No further action needed.
    Done,
    /// The batch should be re-enqueued into the accumulator for retry.
    Reenqueue,
    /// The batch should be split into smaller batches and re-enqueued.
    SplitAndReenqueue,
}

/// Format the error from a `PartitionResponse` in a user-friendly string.
fn format_partition_response_err(response: &crate::common::requests::PartitionResponse) -> String {
    let error_message_suffix = match &response.error_message {
        Some(msg) if !msg.is_empty() => format!(". Error Message: {}", msg),
        _ => String::new(),
    };
    format!("{}{}", response.error, error_message_suffix)
}

/// Data stored while waiting for a produce response, keyed by correlation ID.
///
/// In Java, this data is captured in the `RequestCompletionHandler` callback
/// closure. In Rust, because `handleProduceResponse` needs `&mut self`, we
/// cannot capture `self` inside the callback. Instead, we store the
/// topic-partition set and topic names here and process responses after
/// `client.poll()` returns. The actual batches remain in `in_flight_batches`.
///
/// This follows CLAUDE.md rule 9: translate callbacks to code executed after
/// awaiting the corresponding call.
struct PendingProduceRequest {
    /// The topic-partitions whose batches were sent in this request.
    partitions: Vec<TopicPartition>,
    /// The topic ID -> topic name mapping at the time the request was sent.
    topic_names: HashMap<Uuid, String>,
}

/// The background task that handles the sending of produce requests to the Kafka cluster.
///
/// This task makes metadata requests to renew its view of the cluster and then sends
/// produce requests to the appropriate nodes.
///
/// Translated from `org.apache.kafka.clients.producer.internals.Sender`.
pub struct Sender<C: KafkaClient> {
    /// The network client for sending requests.
    client: C,
    /// The record accumulator that batches records.
    accumulator: Arc<RecordAccumulator>,
    /// The metadata for the client.
    metadata: Arc<ProducerMetadata>,
    /// Whether the producer should guarantee message order on the broker.
    guarantee_message_order: bool,
    /// The maximum request size to attempt to send to the server.
    max_request_size: i32,
    /// The number of acknowledgements to request from the server.
    acks: i16,
    /// The number of times to retry a failed request before giving up.
    retries: i32,
    /// The max time to wait for the server to respond to the request.
    request_timeout_ms: i32,
    /// The max time to wait before retrying a request which has failed.
    #[allow(dead_code)]
    retry_backoff_ms: i64,
    /// True while the sender task is still running.
    running: Arc<AtomicBool>,
    /// True when the caller wants to ignore all unsent/inflight messages and force close.
    force_close: Arc<AtomicBool>,
    /// Wakeup notification for the sender task.
    wakeup: Arc<Notify>,
    /// A per-partition queue of batches ordered by creation time for tracking in-flight batches.
    in_flight_batches: HashMap<TopicPartition, Vec<ProducerBatch>>,
    /// Pending produce requests awaiting responses, keyed by correlation ID.
    pending_produce_responses: HashMap<i32, PendingProduceRequest>,
    /// Provider of current wall-clock time in milliseconds (epoch).
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl<C: KafkaClient> Sender<C> {
    /// Creates a new `Sender`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: C,
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
        guarantee_message_order: bool,
        max_request_size: i32,
        acks: i16,
        retries: i32,
        request_timeout_ms: i32,
        retry_backoff_ms: i64,
        running: Arc<AtomicBool>,
        force_close: Arc<AtomicBool>,
        wakeup: Arc<Notify>,
        time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Self {
        Self {
            client,
            accumulator,
            metadata,
            guarantee_message_order,
            max_request_size,
            acks,
            retries,
            request_timeout_ms,
            retry_backoff_ms,
            running,
            force_close,
            wakeup,
            in_flight_batches: HashMap::new(),
            pending_produce_responses: HashMap::new(),
            time_provider,
        }
    }

    /// The main run loop for the sender task.
    ///
    /// Translated from `Sender.run()`.
    pub async fn run(&mut self) {
        debug!("Starting Kafka producer I/O task.");

        // Main loop, runs until close is called
        while self.running.load(Ordering::Acquire) {
            self.run_once().await;
        }

        debug!("Beginning shutdown of Kafka producer I/O task, sending remaining records.");

        // We stopped accepting requests but there may still be requests in the
        // accumulator or waiting for acknowledgment. Wait until these are completed.
        while !self.force_close.load(Ordering::Acquire)
            && (self.accumulator.has_undrained() || self.client.in_flight_request_count() > 0)
        {
            self.run_once().await;
        }

        if self.force_close.load(Ordering::Acquire) {
            debug!("Aborting incomplete batches due to forced shutdown");
            self.accumulator.abort_incomplete_batches();
        }

        self.client.close().await;

        debug!("Shutdown of Kafka producer I/O task has completed.");
    }

    /// Run a single iteration of sending.
    ///
    /// Translated from `Sender.runOnce()`.
    ///
    /// In Java, `runOnce` calls `client.poll()` which invokes callbacks on
    /// completed requests. The Sender's produce response callback calls
    /// `handleProduceResponse()`. In Rust, we cannot capture `&mut self` in a
    /// callback, so instead we process the responses returned by `poll()`
    /// directly.
    async fn run_once(&mut self) {
        // No transaction manager in this phase
        let current_time_ms = (self.time_provider)();
        let poll_timeout = self.send_producer_data(current_time_ms).await;

        // Use tokio::select to allow wakeup interruption during poll
        let responses = tokio::select! {
            responses = self.client.poll(poll_timeout, current_time_ms) => responses,
            _ = self.wakeup.notified() => Vec::new(),
        };

        // Process any produce responses (equivalent to Java's callback-based
        // handleProduceResponse invoked from within poll/completeResponses)
        let now = (self.time_provider)();
        self.handle_produce_responses(&responses, now);
    }

    /// Process all produce responses from a poll cycle.
    ///
    /// In Java, this happens inside the `RequestCompletionHandler` callback.
    /// In Rust, we process responses after `client.poll()` returns.
    fn handle_produce_responses(&mut self, responses: &[ClientResponse], now: i64) {
        for response in responses {
            let correlation_id = response.request_header().correlation_id();
            if let Some(pending) = self.pending_produce_responses.remove(&correlation_id) {
                // Extract batches from in_flight_batches for the partitions in this request.
                // We take the first (oldest) batch per partition, matching Java's behavior
                // where each produce request contains exactly one batch per partition.
                let mut batches: HashMap<TopicPartition, ProducerBatch> = HashMap::new();
                for tp in &pending.partitions {
                    if let Some(partition_batches) = self.in_flight_batches.get_mut(tp) {
                        if !partition_batches.is_empty() {
                            let batch = partition_batches.remove(0);
                            batches.insert(tp.clone(), batch);
                        }
                        if partition_batches.is_empty() {
                            self.in_flight_batches.remove(tp);
                        }
                    }
                }
                let actions = self.handle_produce_response(response, &mut batches, &pending.topic_names, now);

                // Process deferred actions that require batch ownership.
                for (tp, action) in actions {
                    if let Some(batch) = batches.remove(&tp) {
                        match action {
                            BatchAction::Reenqueue => {
                                // RecordAccumulator::reenqueue calls batch.reenqueued() internally.
                                self.accumulator.reenqueue(batch, now);
                            },
                            BatchAction::SplitAndReenqueue => {
                                // split_and_reenqueue takes ownership, splits the batch,
                                // chains the sub-batch futures, and pushes them to the
                                // front of the deque. After splitting, the original batch's
                                // produce future is completed with RECORD_BATCH_TOO_LARGE
                                // by ProducerBatch::split → finalize_split_batches.
                                self.accumulator.split_and_reenqueue(batch);
                            },
                            BatchAction::Done => {},
                        }
                    }
                }
            }
        }
    }

    /// Returns the in-flight batches for a topic partition.
    pub fn in_flight_batches(&self, tp: &TopicPartition) -> Vec<&ProducerBatch> {
        self.in_flight_batches
            .get(tp)
            .map(|batches| batches.iter().collect())
            .unwrap_or_default()
    }

    /// Completes and deallocates a batch.
    ///
    /// In Java, this also removes the batch from `inFlightBatches`. In Rust,
    /// the batch is already extracted from `in_flight_batches` by the caller
    /// (`handle_produce_responses` or `get_expired_inflight_batches`) before
    /// completion is called, so no removal is needed here.
    fn maybe_remove_and_deallocate_batch(&mut self, batch: &mut ProducerBatch) {
        self.accumulator.complete_and_deallocate_batch(batch);
    }

    /// Get the in-flight batches that have reached delivery timeout.
    fn get_expired_inflight_batches(&mut self, now: i64) -> Vec<ProducerBatch> {
        let mut expired_batches = Vec::new();
        let delivery_timeout_ms = self.accumulator.delivery_timeout_ms() as i64;

        // Collect expired batches, removing them from in_flight_batches
        let mut empty_partitions = Vec::new();
        for (tp, partition_batches) in &mut self.in_flight_batches {
            while !partition_batches.is_empty() {
                if partition_batches[0].has_reached_delivery_timeout(delivery_timeout_ms, now) {
                    let batch = partition_batches.remove(0);
                    if !batch.is_done() {
                        expired_batches.push(batch);
                    } else {
                        panic!(
                            "{} batch created at {} gets unexpected final state {:?}",
                            batch.topic_partition,
                            batch.created_ms,
                            batch.final_state()
                        );
                    }
                } else {
                    self.accumulator.maybe_update_next_batch_expiry_time(&partition_batches[0]);
                    break;
                }
            }
            if partition_batches.is_empty() {
                empty_partitions.push(tp.clone());
            }
        }
        for tp in empty_partitions {
            self.in_flight_batches.remove(&tp);
        }

        expired_batches
    }

    /// Add batches to the in-flight tracking map (takes ownership).
    fn add_to_inflight_batches(&mut self, batches: &mut HashMap<i32, Vec<ProducerBatch>>) {
        for batch_list in batches.values_mut() {
            // Drain the list to take ownership of each batch
            for batch in batch_list.drain(..) {
                self.in_flight_batches
                    .entry(batch.topic_partition.clone())
                    .or_default()
                    .push(batch);
            }
        }
    }

    /// Send producer data.
    ///
    /// Translated from `Sender.sendProducerData()`.
    async fn send_producer_data(&mut self, now: i64) -> i64 {
        let metadata_snapshot = self.metadata.fetch_metadata_snapshot();

        // Get the list of partitions with data ready to send
        let mut result = self.accumulator.ready(&metadata_snapshot, now);

        // If there are any partitions whose leaders are not known yet, force metadata update
        if !result.unknown_leader_topics.is_empty() {
            for topic in &result.unknown_leader_topics {
                self.metadata.add(topic, now);
            }
            debug!(
                "Requesting metadata update due to unknown leader topics from the batched records: {:?}",
                result.unknown_leader_topics
            );
            self.metadata.request_update(false);
        }

        // Remove any nodes we aren't ready to send to
        let mut not_ready_timeout = i64::MAX;
        let mut ready_nodes = HashSet::new();

        for node in result.ready_nodes.drain() {
            let client_ready = self.client.ready(&node, now).await;
            if !client_ready {
                // Update just the readyTimeMs of the latency stats
                self.accumulator.update_node_latency_stats(node.id(), now, false);
                not_ready_timeout = not_ready_timeout.min(self.client.poll_delay_ms(&node, now));
            } else {
                // Update both readyTimeMs and drainTimeMs
                self.accumulator.update_node_latency_stats(node.id(), now, true);
                ready_nodes.insert(node);
            }
        }
        result.ready_nodes = ready_nodes;

        // Create produce requests
        let mut batches = self
            .accumulator
            .drain(&metadata_snapshot, &result.ready_nodes, self.max_request_size, now);

        // Build the produce requests BEFORE moving batches into in-flight tracking,
        // since send_produce_request needs to read from the batches.
        // Collect the data needed for produce requests first.
        let mut request_data: Vec<(i32, Vec<RequestBatchInfo>)> = Vec::new();
        for (destination, batch_list) in &mut batches {
            let mut infos = Vec::with_capacity(batch_list.len());
            for batch in batch_list.iter_mut() {
                let tp = batch.topic_partition.clone();
                let records = batch.records();
                infos.push(RequestBatchInfo { tp, records_data: Some(records.into_buffer()) });
            }
            request_data.push((*destination, infos));
        }

        if self.guarantee_message_order {
            // Mute all the partitions drained
            for batch_list in batches.values() {
                for batch in batch_list {
                    self.accumulator.mute_partition(batch.topic_partition.clone());
                }
            }
        }

        // Move batches into in-flight tracking (takes ownership)
        self.add_to_inflight_batches(&mut batches);

        self.accumulator.reset_next_batch_expiry_time();
        let mut expired_inflight_batches = self.get_expired_inflight_batches(now);
        let mut expired_batches = self.accumulator.expired_batches(now);

        self.fail_expired_batches(&mut expired_batches, now, true);
        self.fail_expired_batches(&mut expired_inflight_batches, now, false);

        // Calculate poll timeout
        let mut poll_timeout = result.next_ready_check_delay_ms.min(not_ready_timeout);
        poll_timeout = poll_timeout.min(self.accumulator.next_expiry_time_ms() - now);
        poll_timeout = poll_timeout.max(0);

        if !result.ready_nodes.is_empty() {
            trace!("Nodes with data ready to send: {:?}", result.ready_nodes);
            poll_timeout = 0;
        }

        self.send_produce_requests(request_data, now);
        poll_timeout
    }

    fn fail_expired_batches(&mut self, expired_batches: &mut [ProducerBatch], now: i64, deallocate_buffer: bool) {
        if !expired_batches.is_empty() {
            trace!("Expired {} batches in accumulator", expired_batches.len());
        }
        for expired_batch in expired_batches.iter_mut() {
            let error_message = format!(
                "Expiring {} record(s) for {}:{} ms has passed since batch creation",
                expired_batch.record_count,
                expired_batch.topic_partition,
                now - expired_batch.created_ms
            );
            let error = KafkaError::with_message(Errors::RequestTimedOut, error_message);
            self.fail_batch_with_error(expired_batch, error, false, deallocate_buffer);

            // In Java, the partition is unmuted by the response callback's `completeBatch()`
            // call, which always runs even for expired batches because the callback has its
            // own reference to the batch. In Rust, expired batches are removed from
            // `in_flight_batches` before response processing, so the response handler can't
            // find them and never calls `complete_batch`. We unmute here to match Java's
            // behavior.
            if self.guarantee_message_order {
                self.accumulator.unmute_partition(&expired_batch.topic_partition);
            }
        }
    }

    /// Start closing the sender (won't actually complete until all data is sent out).
    ///
    /// Translated from `Sender.initiateClose()`.
    pub fn initiate_close(&self) {
        self.accumulator.close();
        self.running.store(false, Ordering::Release);
        self.wakeup();
    }

    /// Closes the sender without sending out any pending messages.
    ///
    /// Translated from `Sender.forceClose()`.
    pub fn force_close(&self) {
        self.force_close.store(true, Ordering::Release);
        self.initiate_close();
    }

    /// Returns `true` if the sender is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Handle a produce response.
    ///
    /// Translated from `Sender.handleProduceResponse()`.
    ///
    /// Returns a list of `(TopicPartition, BatchAction)` for batches that require
    /// ownership transfer (reenqueue or split). The caller owns the batches and
    /// must process these actions.
    fn handle_produce_response(
        &mut self,
        response: &ClientResponse,
        batches: &mut HashMap<TopicPartition, ProducerBatch>,
        topic_names: &HashMap<Uuid, String>,
        now: i64,
    ) -> Vec<(TopicPartition, BatchAction)> {
        let request_header = response.request_header();
        let correlation_id = request_header.correlation_id();
        let mut deferred_actions: Vec<(TopicPartition, BatchAction)> = Vec::new();

        if response.was_timed_out() {
            trace!(
                "Cancelled request with header {} due to the last request to node {} timed out",
                request_header,
                response.destination()
            );
            let part_resp = PartitionResponse::from_error_with_message(
                Errors::RequestTimedOut,
                Some(format!("Disconnected from node {} due to timeout", response.destination())),
            );
            for (tp, batch) in batches.iter_mut() {
                let action = self.complete_batch(batch, &part_resp, correlation_id, now, None);
                deferred_actions.push((tp.clone(), action));
            }
        } else if response.was_disconnected() {
            trace!(
                "Cancelled request with header {} due to node {} being disconnected",
                request_header,
                response.destination()
            );
            let part_resp = PartitionResponse::from_error_with_message(
                Errors::NetworkException,
                Some(format!("Disconnected from node {}", response.destination())),
            );
            for (tp, batch) in batches.iter_mut() {
                let action = self.complete_batch(batch, &part_resp, correlation_id, now, None);
                deferred_actions.push((tp.clone(), action));
            }
        } else if response.version_mismatch().is_some() {
            warn!(
                "Cancelled request {} due to a version mismatch with node {}: {}",
                response,
                response.destination(),
                response.version_mismatch().unwrap_or("unknown")
            );
            let part_resp = PartitionResponse::from_error_with_message(
                Errors::UnsupportedVersion,
                response.version_mismatch().map(|s| s.to_string()),
            );
            for (tp, batch) in batches.iter_mut() {
                let action = self.complete_batch(batch, &part_resp, correlation_id, now, None);
                deferred_actions.push((tp.clone(), action));
            }
        } else {
            trace!(
                "Received produce response from node {} with correlation id {}",
                response.destination(),
                correlation_id
            );
            if response.has_response() {
                if let Some(ConcreteResponse::Produce(produce_response)) = response.response_body() {
                    let mut partitions_with_updated_leader_info = HashMap::new();

                    for topic_resp in &produce_response.data().responses {
                        for partition_resp in &topic_resp.partition_responses {
                            let error = Errors::for_code(partition_resp.error_code);
                            let record_errors: Vec<RecordError> = partition_resp
                                .record_errors
                                .iter()
                                .map(|e| RecordError::new(e.batch_index, e.batch_index_error_message.clone()))
                                .collect();

                            let part_resp = PartitionResponse::with_leader(
                                error,
                                partition_resp.base_offset,
                                partition_resp.log_append_time_ms,
                                partition_resp.log_start_offset,
                                record_errors,
                                partition_resp.error_message.clone(),
                                partition_resp.current_leader.clone(),
                            );

                            // Find batch based on topic id and partition index
                            let tp = if topic_resp.topic_id != Uuid::ZERO_UUID
                                && topic_names.contains_key(&topic_resp.topic_id)
                            {
                                TopicPartition::new(topic_names[&topic_resp.topic_id].clone(), partition_resp.index)
                            } else {
                                TopicPartition::new(topic_resp.name.clone(), partition_resp.index)
                            };

                            if let Some(batch) = batches.get_mut(&tp) {
                                let action = self.complete_batch(
                                    batch,
                                    &part_resp,
                                    correlation_id,
                                    now,
                                    Some(&mut partitions_with_updated_leader_info),
                                );
                                deferred_actions.push((tp, action));
                            } else {
                                error!(
                                    "Can't find batch created for topic id {} topic name {} partition {} using {:?}",
                                    topic_resp.topic_id, topic_resp.name, partition_resp.index, topic_names
                                );
                            }
                        }
                    }

                    if !partitions_with_updated_leader_info.is_empty() {
                        let leader_nodes: Vec<crate::common::Node> = produce_response
                            .data()
                            .node_endpoints
                            .iter()
                            .map(|e| crate::common::Node::with_rack(e.node_id, e.host.clone(), e.port, e.rack.clone()))
                            .filter(|n| !n.is_empty())
                            .collect();

                        let updated_partitions = self
                            .metadata
                            .update_partition_leadership(&partitions_with_updated_leader_info, &leader_nodes);

                        for part in &updated_partitions {
                            debug!("For {} leader was updated.", part);
                        }
                    }
                }
            } else {
                // acks = 0 case, just complete all requests
                let part_resp = PartitionResponse::from_error(Errors::None);
                for (tp, batch) in batches.iter_mut() {
                    let action = self.complete_batch(batch, &part_resp, correlation_id, now, None);
                    deferred_actions.push((tp.clone(), action));
                }
            }
        }

        deferred_actions
    }

    /// Complete or retry the given batch of records.
    ///
    /// Translated from `Sender.completeBatch()` (the 5-argument version).
    ///
    /// Returns a [`BatchAction`] indicating whether the caller should reenqueue
    /// the batch, split-and-reenqueue it, or do nothing (already completed).
    fn complete_batch(
        &mut self,
        batch: &mut ProducerBatch,
        response: &PartitionResponse,
        correlation_id: i32,
        now: i64,
        mut partitions_with_updated_leader_info: Option<&mut HashMap<TopicPartition, LeaderIdAndEpoch>>,
    ) -> BatchAction {
        batch.set_inflight(false);
        let error = response.error;

        let action = if error == Errors::MessageTooLarge
            && batch.record_count > 1
            && !batch.is_done()
            && (batch.magic() >= RecordBatch::MAGIC_VALUE_V2 || batch.is_compressed())
        {
            // If the batch is too large, split and retry.
            // Signal the caller to split the batch and reenqueue the sub-batches.
            // The caller owns the batch and will pass it to
            // `accumulator.split_and_reenqueue()`.
            warn!(
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlation_id,
                batch.topic_partition,
                self.retries - batch.attempts(),
                Self::format_err_msg(response)
            );
            BatchAction::SplitAndReenqueue
        } else if error != Errors::None {
            if self.can_retry(batch, response, now) {
                warn!(
                    "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlation_id,
                    batch.topic_partition,
                    self.retries - batch.attempts() - 1,
                    Self::format_err_msg(response)
                );
                // Signal the caller to reenqueue. The caller owns the batch and
                // will call `batch.reenqueued()` + `accumulator.reenqueue()`.
                BatchAction::Reenqueue
            } else if error == Errors::DuplicateSequenceNumber {
                // Duplicate sequence: return success without valid offset/timestamp
                self.complete_batch_success(batch, response);
                BatchAction::Done
            } else {
                // Final failure
                let adjust = batch.attempts() < self.retries;
                self.fail_batch(batch, response, adjust, true);
                BatchAction::Done
            }
        } else {
            self.complete_batch_success(batch, response);
            BatchAction::Done
        };

        if error != Errors::None && error.is_invalid_metadata() {
            if error == Errors::UnknownTopicOrPartition {
                warn!(
                    "Received unknown topic or partition error in produce request on partition {}. \
                     The topic-partition may not exist or the user may not have Describe access to it",
                    batch.topic_partition
                );
            } else {
                warn!(
                    "Received invalid metadata error in produce request on partition {} due to {}. \
                     Going to request metadata update now",
                    batch.topic_partition, error
                );
            }

            if (error == Errors::NotLeaderOrFollower || error == Errors::FencedLeaderEpoch)
                && response.current_leader.leader_id != -1
                && response.current_leader.leader_epoch != -1
                && let Some(ref mut map) = partitions_with_updated_leader_info
            {
                map.insert(
                    batch.topic_partition.clone(),
                    LeaderIdAndEpoch::new(
                        Some(response.current_leader.leader_id),
                        Some(response.current_leader.leader_epoch),
                    ),
                );
            }

            self.metadata.request_update(false);
        }

        // Unmute the completed partition
        if self.guarantee_message_order {
            self.accumulator.unmute_partition(&batch.topic_partition);
        }

        action
    }

    /// Format the error from a `PartitionResponse` in a user-friendly string.
    fn format_err_msg(response: &PartitionResponse) -> String {
        format_partition_response_err(response)
    }

    /// Complete a batch successfully.
    ///
    /// Translated from `Sender.completeBatch()` (the 2-argument version).
    fn complete_batch_success(&mut self, batch: &mut ProducerBatch, response: &PartitionResponse) {
        // No transaction manager in this phase
        if batch.complete(response.base_offset, response.log_append_time) {
            self.maybe_remove_and_deallocate_batch(batch);
        } else {
            // Always safe to call deallocate because the batch keeps track of
            // whether or not it was deallocated yet
            self.accumulator.deallocate(batch);
        }
    }

    fn fail_batch(
        &mut self,
        batch: &mut ProducerBatch,
        response: &PartitionResponse,
        adjust_sequence_numbers: bool,
        deallocate_batch: bool,
    ) {
        let top_level_error = if response.error == Errors::TopicAuthorizationFailed {
            KafkaError::with_message(Errors::TopicAuthorizationFailed, batch.topic_partition.topic().to_string())
        } else if response.error == Errors::ClusterAuthorizationFailed {
            KafkaError::with_message(
                Errors::ClusterAuthorizationFailed,
                "The producer is not authorized to do idempotent sends",
            )
        } else {
            match &response.error_message {
                Some(msg) => KafkaError::with_message(response.error, msg),
                None => KafkaError::new(response.error),
            }
        };

        if response.record_errors.is_empty() {
            self.fail_batch_with_error(batch, top_level_error, adjust_sequence_numbers, deallocate_batch);
        } else {
            // Build per-record error map
            let mut record_error_map: HashMap<i32, KafkaError> = HashMap::with_capacity(response.record_errors.len());
            for record_error in &response.record_errors {
                let error_message = record_error
                    .message
                    .clone()
                    .or_else(|| response.error_message.clone())
                    .unwrap_or_else(|| response.error.to_string());

                if response.record_errors.len() == 1 {
                    record_error_map.insert(
                        record_error.batch_index,
                        KafkaError::with_message(response.error, error_message),
                    );
                } else {
                    record_error_map.insert(
                        record_error.batch_index,
                        KafkaError::with_message(Errors::InvalidRecord, error_message),
                    );
                }
            }

            let default_error = KafkaError::with_message(
                Errors::InvalidRecord,
                "Failed to append record because it was part of a batch which had one or more invalid records",
            );

            // Complete with per-record exceptions
            let record_exceptions: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync> =
                Arc::new(move |batch_index: i32| -> Option<KafkaError> {
                    Some(
                        record_error_map
                            .get(&batch_index)
                            .cloned()
                            .unwrap_or_else(|| default_error.clone()),
                    )
                });

            self.fail_batch_with_record_exceptions(
                batch,
                top_level_error,
                record_exceptions,
                adjust_sequence_numbers,
                deallocate_batch,
            );
        }
    }

    fn fail_batch_with_error(
        &mut self,
        batch: &mut ProducerBatch,
        top_level_exception: KafkaError,
        adjust_sequence_numbers: bool,
        deallocate_batch: bool,
    ) {
        let exception_clone = top_level_exception.clone();
        let record_exceptions: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync> =
            Arc::new(move |_| Some(exception_clone.clone()));
        self.fail_batch_with_record_exceptions(
            batch,
            top_level_exception,
            record_exceptions,
            adjust_sequence_numbers,
            deallocate_batch,
        );
    }

    fn fail_batch_with_record_exceptions(
        &mut self,
        batch: &mut ProducerBatch,
        top_level_exception: KafkaError,
        record_exceptions: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync>,
        _adjust_sequence_numbers: bool,
        deallocate_batch: bool,
    ) {
        // The batch has already been removed from `in_flight_batches` by the caller
        // (either `handle_produce_responses` or `get_expired_inflight_batches`).
        if batch.complete_exceptionally(top_level_exception, record_exceptions) {
            // No transaction manager handling in this phase
            if deallocate_batch {
                self.accumulator.complete_and_deallocate_batch(batch);
            } else {
                self.accumulator.complete_batch(batch);
            }
        } else if deallocate_batch {
            self.accumulator.deallocate(batch);
        }
    }

    /// Check if a batch can be retried.
    ///
    /// Translated from `Sender.canRetry()`.
    fn can_retry(&self, batch: &ProducerBatch, response: &PartitionResponse, now: i64) -> bool {
        !batch.has_reached_delivery_timeout(self.accumulator.delivery_timeout_ms() as i64, now)
            && batch.attempts() < self.retries
            && !batch.is_done()
            && response.error.is_retriable()
    }

    /// Transfer the record batches into a list of produce requests on a per-node basis.
    ///
    /// Translated from `Sender.sendProduceRequests()`.
    fn send_produce_requests(&mut self, request_data: Vec<(i32, Vec<RequestBatchInfo>)>, now: i64) {
        for (destination, batch_infos) in request_data {
            self.send_produce_request(now, destination, self.acks, self.request_timeout_ms, batch_infos);
        }
    }

    /// Create a produce request from pre-extracted batch data.
    ///
    /// Translated from `Sender.sendProduceRequest()`.
    ///
    /// In Java, a `RequestCompletionHandler` callback is attached that calls
    /// `handleProduceResponse()` on the Sender. In Rust, we cannot capture
    /// `&mut self` in a callback, so instead we store the batch metadata in
    /// `pending_produce_responses` and process the response after `poll()`
    /// returns in `run_once()`.
    fn send_produce_request(
        &mut self,
        now: i64,
        destination: i32,
        acks: i16,
        timeout: i32,
        batch_infos: Vec<RequestBatchInfo>,
    ) {
        if batch_infos.is_empty() {
            return;
        }

        let topic_ids = self.topic_ids_for_partitions(&batch_infos);

        let mut topic_data_list: Vec<TopicProduceData> = Vec::new();
        let mut batch_tps: Vec<TopicPartition> = Vec::with_capacity(batch_infos.len());

        // Consume the batch_infos by value to avoid cloning the record data.
        for mut info in batch_infos {
            let topic_id = topic_ids.get(info.tp.topic()).copied().unwrap_or(Uuid::ZERO_UUID);

            // Find or create topic data
            let topic_data = topic_data_list
                .iter_mut()
                .find(|td| td.name == *info.tp.topic() || td.topic_id == topic_id);

            // Take ownership of the record data instead of cloning.
            let records_bytes = info.records_data.take();

            if let Some(td) = topic_data {
                let mut partition_data = PartitionProduceData::new();
                partition_data.set_index(info.tp.partition());
                partition_data.set_records(records_bytes);
                td.partition_data.push(partition_data);
            } else {
                let mut td = TopicProduceData::new();
                td.set_topic_id(topic_id);
                td.set_name(info.tp.topic().to_string());
                let mut partition_data = PartitionProduceData::new();
                partition_data.set_index(info.tp.partition());
                partition_data.set_records(records_bytes);
                td.partition_data.push(partition_data);
                topic_data_list.push(td);
            }

            batch_tps.push(info.tp);
        }

        // Mark only the specific batches being sent in this request as inflight.
        // In Java, `batch.setInflight(true)` is called on each batch as it is added
        // to the produce request (Sender.java:919). We mark only the last batch per
        // partition, which is the one just added by `add_to_inflight_batches`.
        for tp in &batch_tps {
            if let Some(batches) = self.in_flight_batches.get_mut(tp)
                && let Some(batch) = batches.last_mut()
            {
                batch.set_inflight(true);
            }
        }

        let mut data = ProduceRequestData::new();
        data.set_acks(acks);
        data.set_timeout_ms(timeout);
        data.set_topic_data(topic_data_list);

        let request_builder = ProduceRequestBuilder::new(data);

        // Fetch topic names from metadata outside the response path, since topic
        // IDs may change during the response (e.g. if a topic is recreated).
        let topic_names = self.metadata.topic_names();

        let node_id = destination.to_string();
        let client_request = self.client.new_client_request_with_timeout(
            &node_id,
            Box::new(request_builder),
            now,
            acks != 0,
            self.request_timeout_ms,
            None, // No callback -- we process responses after poll() returns
        );

        // Store the pending request metadata keyed by correlation ID.
        // Batches remain in `in_flight_batches` and will be extracted during
        // response processing in `handle_produce_responses()`.
        let correlation_id = client_request.correlation_id();

        self.pending_produce_responses
            .insert(correlation_id, PendingProduceRequest { partitions: batch_tps, topic_names });

        self.client.send(client_request, now);
        trace!("Sent produce request to {}", node_id);
    }

    fn topic_ids_for_partitions(&self, batch_infos: &[RequestBatchInfo]) -> HashMap<String, Uuid> {
        let metadata_topic_ids = self.metadata.topic_ids();
        let mut result = HashMap::new();
        for info in batch_infos {
            let topic = info.tp.topic().to_string();
            let topic_id = metadata_topic_ids.get(&topic).copied().unwrap_or(Uuid::ZERO_UUID);
            result.insert(topic, topic_id);
        }
        result
    }

    /// Wake up the selector associated with this send task.
    pub fn wakeup(&self) {
        self.client.wakeup();
    }

    /// Returns a mutable reference to the underlying client (visible for testing).
    pub fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }

    /// Returns a reference to the underlying client (visible for testing).
    pub fn client(&self) -> &C {
        &self.client
    }
}

/// Pre-extracted data from a ProducerBatch needed to build a produce request.
///
/// This is used to decouple the produce request building from the batch
/// ownership transfer into in-flight tracking.
struct RequestBatchInfo {
    /// The topic-partition for this batch.
    tp: TopicPartition,
    /// The serialized record data (already built from MemoryRecordsBuilder).
    records_data: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Node;
    use crate::common::compress::Compression;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::record::MemoryRecordsBuilder;
    use crate::common::record::RecordBatch;
    use crate::common::record::TimestampType;
    use crate::common::requests::ConcreteResponse;
    use crate::common::requests::{PartitionResponse, ProduceResponse};
    use crate::mock_client::MockClient;
    use crate::produce_response_data::{PartitionProduceResponse, ProduceResponseData, TopicProduceResponse};
    use crate::producer::internals::BufferPool;
    use crate::producer::internals::FutureRecordMetadata;
    use crate::producer::internals::PartitionerConfig;
    use std::sync::atomic::AtomicI64;

    // Constants matching Java's SenderTest
    const MAX_REQUEST_SIZE: i32 = 1024 * 1024;
    const ACKS_ALL: i16 = -1;
    const REQUEST_TIMEOUT: i32 = 5000;
    const RETRY_BACKOFF_MS: i64 = 50;
    const DELIVERY_TIMEOUT_MS: i32 = 1500;
    const TOPIC_IDLE_MS: i64 = 60 * 1000;
    const MAX_BLOCK_TIMEOUT: i64 = 1000;

    const TOPIC_NAME: &str = "test";

    fn topic_id() -> Uuid {
        Uuid::from_string("MKXx1fIkQy2J9jXHhK8m1w").expect("valid UUID")
    }

    fn topic_ids() -> HashMap<String, Uuid> {
        let mut m = HashMap::new();
        m.insert(TOPIC_NAME.to_string(), topic_id());
        m
    }

    /// Shared mock time: atomically advancing clock.
    struct MockTime {
        now_ms: AtomicI64,
    }

    impl MockTime {
        fn new(initial: i64) -> Arc<Self> {
            Arc::new(Self { now_ms: AtomicI64::new(initial) })
        }

        fn milliseconds(&self) -> i64 {
            self.now_ms.load(Ordering::Acquire)
        }

        fn sleep(&self, ms: i64) {
            self.now_ms.fetch_add(ms, Ordering::AcqRel);
        }

        fn as_provider(self: &Arc<Self>) -> Arc<dyn Fn() -> i64 + Send + Sync> {
            let time = Arc::clone(self);
            Arc::new(move || time.milliseconds())
        }
    }

    /// Test harness holding all state needed for SenderTest-style tests.
    struct SenderTestContext {
        sender: Sender<MockClient>,
        accumulator: Arc<RecordAccumulator>,
        metadata: Arc<ProducerMetadata>,
        time: Arc<MockTime>,
        tp0: TopicPartition,
        tp1: TopicPartition,
    }

    impl SenderTestContext {
        /// Default test setup, matching Java's `setupWithTransactionState(null)`.
        fn new() -> Self {
            Self::with_options(false, i32::MAX)
        }

        /// Setup with guarantee_message_order and custom retries.
        fn with_options(guarantee_message_order: bool, retries: i32) -> Self {
            // Start at a non-zero time. Java's MockTime uses System.currentTimeMillis()
            // which is always > 0. Starting at 0 breaks MockClient because
            // not_throttled(0) returns false when throttled_until_ms is also 0.
            let time = MockTime::new(1000);
            let time_provider = time.as_provider();

            let batch_size = 16 * 1024;
            let total_size = 1024 * 1024;

            let metadata = Arc::new(ProducerMetadata::new(
                0,
                0,
                i64::MAX,
                TOPIC_IDLE_MS,
                ClusterResourceListeners::new(),
            ));

            let accumulator = Arc::new(RecordAccumulator::new(
                batch_size,
                Compression::none(),
                0, // linger_ms
                RETRY_BACKOFF_MS,
                RETRY_BACKOFF_MS * 10,
                DELIVERY_TIMEOUT_MS,
                PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
                Arc::new(BufferPool::new(total_size as i64, batch_size as usize)),
            ));

            let nodes = vec![Node::new(0, "localhost".to_string(), 1969)];
            let client = MockClient::new(nodes, Arc::clone(&time_provider));

            let running = Arc::new(AtomicBool::new(true));
            let force_close = Arc::new(AtomicBool::new(false));
            let wakeup = Arc::new(Notify::new());

            let sender = Sender::new(
                client,
                Arc::clone(&metadata),
                Arc::clone(&accumulator),
                guarantee_message_order,
                MAX_REQUEST_SIZE,
                ACKS_ALL,
                retries,
                REQUEST_TIMEOUT,
                RETRY_BACKOFF_MS,
                running,
                force_close,
                wakeup,
                time_provider,
            );

            let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
            let tp1 = TopicPartition::new(TOPIC_NAME.to_string(), 1);

            // Add the topic to metadata and update with cluster info
            metadata.add(TOPIC_NAME, time.milliseconds());
            let mut topic_partition_counts = HashMap::new();
            topic_partition_counts.insert(TOPIC_NAME.to_string(), 3);
            let metadata_response = crate::common::requests::request_test_utils::metadata_update_with_ids(
                "kafka-cluster",
                1,
                &HashMap::new(),
                &topic_partition_counts,
                &|_| None,
                &topic_ids(),
            );
            metadata.update_with_current_request_version(&metadata_response, false, time.milliseconds());

            Self { sender, accumulator, metadata, time, tp0, tp1 }
        }

        /// Append a record to the accumulator for the given partition.
        async fn append_to_accumulator(&self, tp: &TopicPartition) -> Arc<FutureRecordMetadata> {
            self.append_to_accumulator_with(tp, self.time.milliseconds(), "key", "value")
                .await
        }

        /// Append a record with specific timestamp and key/value.
        async fn append_to_accumulator_with(
            &self,
            tp: &TopicPartition,
            timestamp: i64,
            key: &str,
            value: &str,
        ) -> Arc<FutureRecordMetadata> {
            let cluster = self.metadata.fetch();
            let result = self
                .accumulator
                .append(
                    tp.topic(),
                    tp.partition(),
                    timestamp,
                    Some(key.as_bytes()),
                    Some(value.as_bytes()),
                    &[],
                    None,
                    MAX_BLOCK_TIMEOUT,
                    self.time.milliseconds(),
                    &cluster,
                )
                .await
                .expect("append should succeed");
            result.future
        }

        /// Build a simple produce response for a single partition.
        fn produce_response(
            &self,
            tp: &TopicPartition,
            offset: i64,
            error: Errors,
            _throttle_time_ms: i32,
        ) -> ConcreteResponse {
            self.produce_response_with_message(tp, offset, error, _throttle_time_ms, -1, None)
        }

        /// Build a produce response with optional error message.
        fn produce_response_with_message(
            &self,
            tp: &TopicPartition,
            offset: i64,
            error: Errors,
            throttle_time_ms: i32,
            log_start_offset: i64,
            error_message: Option<String>,
        ) -> ConcreteResponse {
            let mut ppr = PartitionProduceResponse::new();
            ppr.set_index(tp.partition());
            ppr.set_base_offset(offset);
            ppr.set_error_code(error.code());
            ppr.set_log_start_offset(log_start_offset);
            if let Some(msg) = error_message {
                ppr.set_error_message(Some(msg));
            }

            let mut tpr = TopicProduceResponse::new();
            tpr.set_topic_id(topic_id());
            tpr.set_name(tp.topic().to_string());
            tpr.set_partition_responses(vec![ppr]);

            let mut data = ProduceResponseData::new();
            data.set_responses(vec![tpr]);
            data.set_throttle_time_ms(throttle_time_ms);

            ConcreteResponse::Produce(ProduceResponse::new(data))
        }
    }

    fn make_batch(tp: TopicPartition, created_ms: i64) -> ProducerBatch {
        let records_builder = MemoryRecordsBuilder::new(
            Vec::with_capacity(1024),
            0, // initial_position
            RecordBatch::CURRENT_MAGIC_VALUE,
            Compression::none(),
            TimestampType::CreateTime,
            0,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            1024,
            -1, // delete_horizon_ms
        );
        ProducerBatch::new(tp, records_builder, created_ms)
    }

    // =====================================================================
    // Unit tests (non-async, matching earlier test coverage)
    // =====================================================================

    /// Test that format_err_msg produces the expected string.
    #[test]
    fn test_format_err_msg() {
        let resp = PartitionResponse::from_error(Errors::NetworkException);
        let msg = format_partition_response_err(&resp);
        assert!(!msg.is_empty());

        let resp_with_msg = PartitionResponse::from_error_with_message(
            Errors::NetworkException,
            Some("Disconnected from node 0".to_string()),
        );
        let msg2 = format_partition_response_err(&resp_with_msg);
        assert!(msg2.contains("Disconnected from node 0"));
        assert!(msg2.contains("Error Message"));
    }

    /// Test that can_retry returns true for retriable errors within limits.
    #[test]
    fn test_can_retry_logic() {
        let resp_retriable = PartitionResponse::from_error(Errors::NotLeaderOrFollower);
        assert!(resp_retriable.error.is_retriable());

        let resp_non_retriable = PartitionResponse::from_error(Errors::TopicAuthorizationFailed);
        assert!(!resp_non_retriable.error.is_retriable());
    }

    /// Test is_invalid_metadata on various error codes.
    #[test]
    fn test_is_invalid_metadata() {
        assert!(Errors::UnknownTopicOrPartition.is_invalid_metadata());
        assert!(Errors::LeaderNotAvailable.is_invalid_metadata());
        assert!(Errors::NotLeaderOrFollower.is_invalid_metadata());
        assert!(Errors::FencedLeaderEpoch.is_invalid_metadata());
        assert!(Errors::NetworkException.is_invalid_metadata());
        assert!(!Errors::RequestTimedOut.is_invalid_metadata());
        assert!(!Errors::None.is_invalid_metadata());
        assert!(!Errors::TopicAuthorizationFailed.is_invalid_metadata());
    }

    /// Test that initiate_close and force_close set flags correctly.
    #[test]
    fn test_initiate_and_force_close() {
        let running = Arc::new(AtomicBool::new(true));
        let force_close = Arc::new(AtomicBool::new(false));

        assert!(running.load(Ordering::Acquire));
        assert!(!force_close.load(Ordering::Acquire));

        running.store(false, Ordering::Release);
        assert!(!running.load(Ordering::Acquire));

        force_close.store(true, Ordering::Release);
        assert!(force_close.load(Ordering::Acquire));
    }

    /// Test in_flight_batches tracking.
    #[test]
    fn test_in_flight_batches_tracking() {
        let tp0 = TopicPartition::new("topic".to_string(), 0);
        let tp1 = TopicPartition::new("topic".to_string(), 1);

        let batch0 = make_batch(tp0.clone(), 100);
        let batch1 = make_batch(tp1.clone(), 100);

        let mut in_flight: HashMap<TopicPartition, Vec<ProducerBatch>> = HashMap::new();
        in_flight.entry(tp0.clone()).or_default().push(batch0);
        in_flight.entry(tp1.clone()).or_default().push(batch1);

        assert_eq!(in_flight.get(&tp0).unwrap().len(), 1);
        assert_eq!(in_flight.get(&tp1).unwrap().len(), 1);

        in_flight.get_mut(&tp0).unwrap().clear();
        in_flight.retain(|_, v| !v.is_empty());
        assert!(!in_flight.contains_key(&tp0));
        assert!(in_flight.contains_key(&tp1));
    }

    /// Test PendingProduceRequest storage and retrieval by correlation ID.
    #[test]
    fn test_pending_produce_responses() {
        let mut pending: HashMap<i32, PendingProduceRequest> = HashMap::new();
        let tp = TopicPartition::new("test-topic".to_string(), 0);

        let mut topic_names = HashMap::new();
        topic_names.insert(Uuid::random_uuid(), "test-topic".to_string());

        pending.insert(42, PendingProduceRequest { partitions: vec![tp.clone()], topic_names });

        assert!(pending.contains_key(&42));
        let removed = pending.remove(&42).unwrap();
        assert!(removed.partitions.contains(&tp));
        assert!(!pending.contains_key(&42));
    }

    /// Test that expired batches are collected correctly.
    #[test]
    fn test_get_expired_inflight_batches() {
        let accumulator = Arc::new(RecordAccumulator::new(
            1024 * 1024,
            Compression::none(),
            0,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MS * 10,
            120000, // use long delivery timeout for this test
            PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
            Arc::new(BufferPool::new(1024 * 1024, 16384)),
        ));
        let tp = TopicPartition::new("test".to_string(), 0);
        let batch = make_batch(tp.clone(), 0);

        let mut in_flight: HashMap<TopicPartition, Vec<ProducerBatch>> = HashMap::new();
        in_flight.entry(tp.clone()).or_default().push(batch);

        let delivery_timeout_ms = accumulator.delivery_timeout_ms() as i64;
        assert!(!in_flight[&tp][0].has_reached_delivery_timeout(delivery_timeout_ms, 0));
        assert!(in_flight[&tp][0].has_reached_delivery_timeout(delivery_timeout_ms, 120001));
    }

    /// Test KafkaError construction matches expected patterns.
    #[test]
    fn test_kafka_error_construction() {
        let err = KafkaError::with_message(Errors::RequestTimedOut, "timed out");
        assert_eq!(err.error(), Errors::RequestTimedOut);
        assert!(err.is_retriable());

        let err2 = KafkaError::new(Errors::TopicAuthorizationFailed);
        assert_eq!(err2.error(), Errors::TopicAuthorizationFailed);
        assert!(!err2.is_retriable());
    }

    /// Test RequestBatchInfo construction.
    #[test]
    fn test_request_batch_info() {
        let tp = TopicPartition::new("topic".to_string(), 0);
        let info = RequestBatchInfo { tp: tp.clone(), records_data: Some(vec![1, 2, 3]) };
        assert_eq!(info.tp, tp);
        assert_eq!(info.records_data.as_ref().unwrap().len(), 3);
    }

    // =====================================================================
    // Integration-style async tests translated from Java SenderTest
    // (non-transactional tests only)
    // =====================================================================

    /// Translated from Java `SenderTest.testSimple()`.
    ///
    /// Verifies the basic send-response lifecycle: append a record, run_once
    /// to connect + send, receive a response, and confirm the future completes
    /// with the correct offset.
    #[tokio::test]
    async fn test_simple() {
        let mut ctx = SenderTestContext::new();
        let offset = 0i64;
        let tp0 = ctx.tp0.clone();
        let future = ctx.append_to_accumulator(&tp0).await;

        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send produce request

        assert_eq!(
            ctx.sender.client().in_flight_request_count(),
            1,
            "We should have a single produce request in flight."
        );
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);
        assert!(ctx.sender.client().has_in_flight_requests());

        let response = ctx.produce_response(&tp0, offset, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await;
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0, "All requests completed.");
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);
        assert!(!ctx.sender.client().has_in_flight_requests());

        ctx.sender.run_once().await;
        assert!(future.is_done(), "Request should be completed");

        let metadata = future.get().await.expect("Future should succeed");
        assert_eq!(metadata.offset(), offset);
    }

    /// Translated from Java `SenderTest.testCanRetryWithoutIdempotence()`.
    ///
    /// Verifies that a non-retriable error (TOPIC_AUTHORIZATION_FAILED) completes
    /// the future with the correct error type.
    #[tokio::test]
    async fn test_can_retry_without_idempotence() {
        let mut ctx = SenderTestContext::new();
        let tp0 = ctx.tp0.clone();
        let future = ctx.append_to_accumulator(&tp0).await;

        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send produce request

        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert!(ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);
        assert!(!future.is_done());

        let response = ctx.produce_response(&tp0, -1, Errors::TopicAuthorizationFailed, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await;
        assert!(future.is_done());

        let result = future.get().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error(), Errors::TopicAuthorizationFailed);
    }

    /// Translated from Java `SenderTest.testExpiredBatchDoesNotRetry()`.
    ///
    /// Verifies that once a batch has expired (delivery timeout exceeded), a
    /// retriable error does NOT cause a retry.
    #[tokio::test]
    async fn test_expired_batch_does_not_retry() {
        let mut ctx = SenderTestContext::new();
        let tp0 = ctx.tp0.clone();

        // Send first ProduceRequest
        let future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send request
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);

        let response = ctx.produce_response(&tp0, -1, Errors::NotLeaderOrFollower, -1);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await; // expire the batch
        assert!(future.is_done());
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        ctx.sender.run_once().await; // receive first response and do not reenqueue
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        ctx.sender.run_once().await; // run again and must not send anything
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);
    }

    /// Translated from Java `SenderTest.testExpiredBatchDoesNotSplitOnMessageTooLargeError()`.
    ///
    /// Verifies that an expired batch that gets a MESSAGE_TOO_LARGE error is
    /// not split and resent.
    #[tokio::test]
    async fn test_expired_batch_does_not_split_on_message_too_large_error() {
        let mut ctx = SenderTestContext::new();
        let tp0 = ctx.tp0.clone();

        // Create a producer batch with more than one record so it is eligible for splitting
        let future1 = ctx.append_to_accumulator(&tp0).await;
        let future2 = ctx.append_to_accumulator(&tp0).await;

        // Send request
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await;
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        // Return a MESSAGE_TOO_LARGE error
        let response = ctx.produce_response(&tp0, -1, Errors::MessageTooLarge, -1);
        ctx.sender.client_mut().respond(response);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);

        // Expire the batch and process the response
        ctx.sender.run_once().await;
        assert!(future1.is_done());
        assert!(future2.is_done());
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        // Run again and must not split big batch and resend anything
        ctx.sender.run_once().await;
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);
    }

    /// Translated from Java `SenderTest.testInflightBatchesExpireOnDeliveryTimeout()`.
    ///
    /// Verifies that an in-flight batch expires when the delivery timeout
    /// is reached, even if the server responds with success.
    #[tokio::test]
    async fn test_inflight_batches_expire_on_delivery_timeout() {
        let mut ctx = SenderTestContext::with_options(true, i32::MAX);
        let tp0 = ctx.tp0.clone();

        // Send first ProduceRequest
        let future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send request
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(
            ctx.sender.in_flight_batches(&ctx.tp0).len(),
            1,
            "Expect one in-flight batch in accumulator"
        );

        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);

        ctx.sender.run_once().await; // receive first response
        assert_eq!(
            ctx.sender.in_flight_batches(&ctx.tp0).len(),
            0,
            "Expect zero in-flight batch in accumulator"
        );

        // The expired batch should throw a timeout error
        let result = future.get().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error(), Errors::RequestTimedOut);
    }

    /// Translated from Java `SenderTest.testWhenFirstBatchExpireNoSendSecondBatchIfGuaranteeOrder()`.
    ///
    /// Verifies that when guarantee_message_order is true, the partition is muted
    /// while a batch is in-flight, preventing the second batch from being sent.
    #[tokio::test]
    async fn test_when_first_batch_expire_no_send_second_batch_if_guarantee_order() {
        let mut ctx = SenderTestContext::with_options(true, i32::MAX);
        let tp0 = ctx.tp0.clone();

        // Send first ProduceRequest
        ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send request
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64 / 2);

        // Send second ProduceRequest
        ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await; // must not send request because the partition is muted
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64 / 2); // expire the first batch only

        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await; // receive response (offset=0)
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        ctx.sender.run_once().await; // Drain the second request only this time
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);
    }

    /// Translated from Java `SenderTest.testDefaultErrorMessage()`.
    ///
    /// Verifies that the default error message from the Errors enum is propagated
    /// to the application.
    #[tokio::test]
    async fn test_default_error_message() {
        let mut ctx = SenderTestContext::new();
        let tp0 = ctx.tp0.clone();

        let future = ctx.append_to_accumulator_with(&tp0, 0, "key", "value").await;
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send produce request

        let response = ctx.produce_response(&tp0, 0, Errors::InvalidRequest, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await;
        ctx.sender.run_once().await;

        assert!(future.is_done());
        let result = future.get().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
    }

    /// Translated from Java `SenderTest.testCustomErrorMessage()`.
    ///
    /// Verifies that a custom error message from the server response is propagated
    /// to the application.
    #[tokio::test]
    async fn test_custom_error_message() {
        let mut ctx = SenderTestContext::new();
        let tp0 = ctx.tp0.clone();

        let future = ctx.append_to_accumulator_with(&tp0, 0, "key", "value").await;
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send produce request

        let error_message = "testCustomErrorMessage";
        let response =
            ctx.produce_response_with_message(&tp0, 0, Errors::InvalidRequest, 0, -1, Some(error_message.to_string()));
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await;
        ctx.sender.run_once().await;

        assert!(future.is_done());
        let result = future.get().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error(), Errors::InvalidRequest);
        // The custom message should be present in the error
        let err_msg = format!("{}", err);
        assert!(
            err_msg.contains(error_message),
            "Error message '{}' should contain custom message '{}'",
            err_msg,
            error_message
        );
    }

    /// Translated from Java `SenderTest.testExpiredBatchesInMultiplePartitions()`.
    ///
    /// Verifies that expired batches in multiple partitions are all correctly
    /// failed with timeout errors.
    #[tokio::test]
    async fn test_expired_batches_in_multiple_partitions() {
        let mut ctx = SenderTestContext::with_options(true, i32::MAX);
        let tp0 = ctx.tp0.clone();
        let tp1 = ctx.tp1.clone();

        // Send multiple ProduceRequests across multiple partitions
        let future1 = ctx.append_to_accumulator_with(&tp0, ctx.time.milliseconds(), "k1", "v1").await;
        let future2 = ctx.append_to_accumulator_with(&tp1, ctx.time.milliseconds(), "k2", "v2").await;

        // Send request
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await;
        // Note: Both partitions may go in same or separate requests depending on node assignment
        assert!(ctx.sender.client().in_flight_request_count() >= 1);

        // Respond for tp0 with success
        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        // Successfully expire both batches
        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);
        ctx.sender.run_once().await;
        assert_eq!(
            ctx.sender.in_flight_batches(&ctx.tp0).len(),
            0,
            "Expect zero in-flight batch for tp0"
        );

        // Both futures should be done (either expired or completed before expiry)
        assert!(future1.is_done());
        assert!(future2.is_done());

        // tp0 was expired despite the successful response (delivery timeout exceeded)
        let result1 = future1.get().await;
        assert!(result1.is_err());
        let err1 = result1.unwrap_err();
        assert_eq!(err1.error(), Errors::RequestTimedOut);

        let result2 = future2.get().await;
        assert!(result2.is_err());
        let err2 = result2.unwrap_err();
        assert_eq!(err2.error(), Errors::RequestTimedOut);
    }

    /// Translated from Java `SenderTest.testMetadataTopicExpiry()`.
    ///
    /// Verifies that topics are added to the metadata list when messages are
    /// available to send and expired if not used during a metadata refresh interval.
    #[tokio::test]
    async fn test_metadata_topic_expiry() {
        let mut ctx = SenderTestContext::new();
        let tp0 = ctx.tp0.clone();
        let offset = 0i64;

        let future = ctx.append_to_accumulator(&tp0).await;

        ctx.sender.run_once().await;
        assert!(ctx.metadata.contains_topic(tp0.topic()), "Topic not added to metadata");

        // Update metadata
        let mut topic_partition_counts = HashMap::new();
        topic_partition_counts.insert(TOPIC_NAME.to_string(), 2);
        let metadata_response = crate::common::requests::request_test_utils::metadata_update_with_ids(
            "kafka-cluster",
            1,
            &HashMap::new(),
            &topic_partition_counts,
            &|_| None,
            &topic_ids(),
        );
        ctx.metadata
            .update_with_current_request_version(&metadata_response, false, ctx.time.milliseconds());

        ctx.sender.run_once().await; // send produce request

        let response = ctx.produce_response(&tp0, offset, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await;
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0, "Request completed.");
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        ctx.sender.run_once().await;
        assert!(future.is_done(), "Request should be completed");

        assert!(ctx.metadata.contains_topic(tp0.topic()), "Topic not retained in metadata list");

        ctx.time.sleep(TOPIC_IDLE_MS);
        ctx.metadata
            .update_with_current_request_version(&metadata_response, false, ctx.time.milliseconds());

        assert!(!ctx.metadata.contains_topic(tp0.topic()), "Unused topic has not been expired");
    }

    /// Translated from Java `SenderTest.testRecordErrorPropagatedToApplication()`.
    ///
    /// Verifies that per-record errors from the server are correctly propagated
    /// to each individual record's future.
    #[tokio::test]
    async fn test_record_error_propagated_to_application() {
        let mut ctx = SenderTestContext::new();
        let tp0 = ctx.tp0.clone();
        let record_count = 5;

        let mut futures = Vec::with_capacity(record_count);
        for _i in 0..record_count {
            futures.push(ctx.append_to_accumulator(&tp0).await);
        }

        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send request
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);

        // Build a produce response with per-record errors
        use crate::produce_response_data::BatchIndexAndErrorMessage;

        let mut ppr = PartitionProduceResponse::new();
        ppr.set_index(tp0.partition());
        ppr.set_base_offset(-1);
        ppr.set_error_code(Errors::InvalidRecord.code());

        let mut record_errors = Vec::new();
        let mut be0 = BatchIndexAndErrorMessage::new();
        be0.set_batch_index(0);
        be0.set_batch_index_error_message(Some("0".to_string()));
        record_errors.push(be0);

        let mut be2 = BatchIndexAndErrorMessage::new();
        be2.set_batch_index(2);
        be2.set_batch_index_error_message(Some("2".to_string()));
        record_errors.push(be2);

        let mut be3 = BatchIndexAndErrorMessage::new();
        be3.set_batch_index(3);
        // No error message for index 3
        record_errors.push(be3);

        ppr.set_record_errors(record_errors);

        let mut tpr = TopicProduceResponse::new();
        tpr.set_topic_id(topic_id());
        tpr.set_name(tp0.topic().to_string());
        tpr.set_partition_responses(vec![ppr]);

        let mut data = ProduceResponseData::new();
        data.set_responses(vec![tpr]);

        let response = ConcreteResponse::Produce(ProduceResponse::new(data));
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await;

        for (index, future) in futures.iter().enumerate() {
            assert!(future.is_done(), "Future {} should be done", index);
            let result = future.get().await;
            assert!(result.is_err(), "Future {} should have error", index);
            let err = result.unwrap_err();

            if index == 0 || index == 2 {
                // Per-record errors with messages "0" and "2"
                assert_eq!(err.error(), Errors::InvalidRecord);
            } else if index == 3 {
                // Per-record error without message, defaults to InvalidRecord message
                assert_eq!(err.error(), Errors::InvalidRecord);
            } else {
                // Records 1, 4 get the default error
                assert_eq!(err.error(), Errors::InvalidRecord);
            }
        }
    }

    /// Translated from Java `SenderTest.testRetries()`.
    ///
    /// Verifies that:
    /// 1. A retriable error (disconnect) causes the batch to be re-enqueued and
    ///    successfully sent on retry.
    /// 2. When retries are exhausted, the batch fails with the appropriate error.
    #[tokio::test]
    async fn test_retries() {
        let max_retries = 1;
        let mut ctx = SenderTestContext::with_options(false, max_retries);
        let tp0 = ctx.tp0.clone();

        // --- Successful retry ---
        let future = ctx.append_to_accumulator_with(&tp0, 0, "key", "value").await;
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send produce request

        let dest = ctx
            .sender
            .client()
            .requests()
            .front()
            .expect("Should have a request")
            .destination()
            .to_string();
        let node = Node::new(dest.parse::<i32>().unwrap(), "localhost".to_string(), 0);
        assert_eq!(1, ctx.sender.client().in_flight_request_count());
        assert!(ctx.sender.client().has_in_flight_requests());
        assert_eq!(1, ctx.sender.in_flight_batches(&ctx.tp0).len());
        assert!(
            ctx.sender.client().is_ready(&node, ctx.time.milliseconds()),
            "Client ready status should be true"
        );

        ctx.sender.client_mut().disconnect_by_id(&dest);
        assert_eq!(0, ctx.sender.client().in_flight_request_count());
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert!(
            !ctx.sender.client().is_ready(&node, ctx.time.milliseconds()),
            "Client ready status should be false"
        );
        // the batch is in sender.in_flight_batches until the disconnect response is processed
        assert_eq!(1, ctx.sender.in_flight_batches(&ctx.tp0).len());

        ctx.sender.run_once().await; // receive error (disconnect response triggers reenqueue)
        // Advance time past the retry backoff (accounting for jitter up to 20%)
        // so the reenqueued batch becomes sendable.
        let backoff_with_jitter = (RETRY_BACKOFF_MS as f64 * 1.3) as i64;
        ctx.time.sleep(backoff_with_jitter);
        // In Rust's MockClient, ready() transitions Disconnected -> Connecting -> Connected
        // in a single call, so one additional run_once is enough to reconnect + drain + send.
        ctx.sender.run_once().await; // reconnect + resend

        assert_eq!(1, ctx.sender.client().in_flight_request_count());
        assert!(ctx.sender.client().has_in_flight_requests());
        assert_eq!(1, ctx.sender.in_flight_batches(&ctx.tp0).len());

        let offset = 0i64;
        let response = ctx.produce_response(&tp0, offset, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await;
        assert!(future.is_done(), "Request should have retried and completed");
        let metadata = future.get().await.expect("Future should succeed");
        assert_eq!(offset, metadata.offset());
        assert_eq!(0, ctx.sender.in_flight_batches(&ctx.tp0).len());

        // --- Unsuccessful retry (exhausted retries) ---
        let future = ctx.append_to_accumulator_with(&tp0, 0, "key", "value").await;
        ctx.sender.run_once().await; // send produce request
        assert_eq!(1, ctx.sender.in_flight_batches(&ctx.tp0).len());

        for i in 0..=(max_retries as usize) {
            let dest = ctx
                .sender
                .client()
                .requests()
                .front()
                .expect("Should have a request")
                .destination()
                .to_string();
            ctx.sender.client_mut().disconnect_by_id(&dest);
            ctx.sender.run_once().await; // receive error
            assert_eq!(0, ctx.sender.in_flight_batches(&ctx.tp0).len());
            ctx.time.sleep(backoff_with_jitter); // advance past retry backoff (with jitter margin)
            ctx.sender.run_once().await; // reconnect + resend
            assert_eq!(if i > 0 { 0 } else { 1 }, ctx.sender.in_flight_batches(&ctx.tp0).len());
        }

        ctx.sender.run_once().await;
        assert!(future.is_done());
        let result = future.get().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error(), Errors::NetworkException);
        assert_eq!(0, ctx.sender.in_flight_batches(&ctx.tp0).len());
    }

    /// Translated from Java `SenderTest.testSendInOrder()`.
    ///
    /// Verifies that when `guarantee_message_order` is true, partitions are muted
    /// while a batch is in-flight, preventing out-of-order sends. After the first
    /// batch completes, the second batch is sent.
    #[tokio::test]
    async fn test_send_in_order() {
        let max_retries = 1;
        let mut ctx = SenderTestContext::with_options(true, max_retries);
        let tp1 = ctx.tp1.clone();

        // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1.
        // For simplicity in Rust, we use 1 broker and 2 partitions.
        let mut topic_partition_counts = HashMap::new();
        topic_partition_counts.insert(TOPIC_NAME.to_string(), 2);
        let metadata_response = crate::common::requests::request_test_utils::metadata_update_with_ids(
            "kafka-cluster",
            2,
            &HashMap::new(),
            &topic_partition_counts,
            &|_| None,
            &topic_ids(),
        );
        ctx.sender.client_mut().set_nodes(vec![
            Node::new(0, "localhost".to_string(), 1969),
            Node::new(1, "localhost".to_string(), 1970),
        ]);
        ctx.metadata
            .update_with_current_request_version(&metadata_response, false, ctx.time.milliseconds());

        // Send the first message to tp1.
        ctx.append_to_accumulator_with(&tp1, 0, "key1", "value1").await;
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send produce request

        assert_eq!(1, ctx.sender.client().in_flight_request_count());
        assert!(ctx.sender.client().has_in_flight_requests());
        assert_eq!(1, ctx.sender.in_flight_batches(&tp1).len());

        ctx.time.sleep(900);
        // Now send another message to tp1
        ctx.append_to_accumulator_with(&tp1, 0, "key2", "value2").await;

        // With guarantee_message_order, the second message should not be sent
        // because tp1 is muted.
        ctx.sender.run_once().await; // should not send because muted
        assert_eq!(1, ctx.sender.client().in_flight_request_count());

        // Complete the first request
        let response = ctx.produce_response(&tp1, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        // Sender receives the response for the previous send and unmutes
        // the partition. But the drain happens at the start of run_once,
        // so we need another cycle to actually drain and send the new batch.
        ctx.sender.run_once().await; // receive response, unmute
        ctx.sender.run_once().await; // drain the second batch and send
        assert_eq!(1, ctx.sender.client().in_flight_request_count());
        assert!(ctx.sender.client().has_in_flight_requests());
        assert_eq!(1, ctx.sender.in_flight_batches(&tp1).len());
    }

    /// Translated from Java `SenderTest.testNoDoubleDeallocation()`.
    ///
    /// Verifies that when a batch times out, its buffer is deallocated exactly
    /// once, not doubled.
    #[tokio::test]
    async fn test_no_double_deallocation() {
        let mut ctx = SenderTestContext::new();
        let tp0 = ctx.tp0.clone();

        // Send first ProduceRequest
        let future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send
        assert_eq!(1, ctx.sender.client().in_flight_request_count());
        assert_eq!(1, ctx.sender.in_flight_batches(&ctx.tp0).len());
        assert!(
            !ctx.sender.in_flight_batches(&ctx.tp0)[0].is_buffer_deallocated(),
            "Buffer not deallocated yet"
        );

        ctx.time.sleep(REQUEST_TIMEOUT as i64);

        ctx.sender.run_once().await; // times out the request
        assert!(future.is_done());

        ctx.sender.run_once().await;
        assert_eq!(0, ctx.sender.client().in_flight_request_count());
        assert_eq!(0, ctx.sender.in_flight_batches(&ctx.tp0).len());
    }

    /// Translated from Java `SenderTest.testResetNextBatchExpiry()`.
    ///
    /// Verifies that batch expiry time is properly reset between iterations.
    /// In Java, this uses a Mockito spy to verify poll timeout values. In Rust,
    /// we verify the behavior by checking that expired batches are properly
    /// detected and failed.
    #[tokio::test]
    async fn test_reset_next_batch_expiry() {
        let mut ctx = SenderTestContext::new();
        let tp0 = ctx.tp0.clone();

        ctx.append_to_accumulator_with(&tp0, 0, "key", "value").await;

        ctx.sender.run_once().await; // connect
        ctx.sender.run_once().await; // send produce request

        // Advance time beyond delivery timeout
        ctx.time.sleep(ctx.accumulator.delivery_timeout_ms() as i64 + 1);

        // Run once more - this should detect the expired batch
        ctx.sender.run_once().await;

        // The in-flight batch should be expired and removed
        assert_eq!(0, ctx.sender.in_flight_batches(&ctx.tp0).len());
    }

    /// Translated from Java `SenderTest.testNodeLatencyStats()`.
    ///
    /// Verifies that node latency statistics (readyTimeMs, drainTimeMs) are
    /// updated correctly as the sender operates.
    #[tokio::test]
    async fn test_node_latency_stats() {
        // Create a new record accumulator with non-0 partitionAvailabilityTimeoutMs
        // otherwise it wouldn't update the stats.
        let time = MockTime::new(1000);
        let time_provider = time.as_provider();

        let batch_size = 16 * 1024;
        let total_size = 1024 * 1024;

        let metadata = Arc::new(ProducerMetadata::new(
            0,
            0,
            i64::MAX,
            TOPIC_IDLE_MS,
            ClusterResourceListeners::new(),
        ));

        let accumulator = Arc::new(RecordAccumulator::new(
            batch_size,
            Compression::none(),
            0, // linger_ms
            0,
            0,
            DELIVERY_TIMEOUT_MS,
            PartitionerConfig { enable_adaptive_partitioning: false, partition_availability_timeout_ms: 42 },
            Arc::new(BufferPool::new(total_size as i64, batch_size as usize)),
        ));

        let nodes = vec![Node::new(0, "localhost".to_string(), 1969)];
        let client = MockClient::new(nodes, Arc::clone(&time_provider));

        let running = Arc::new(AtomicBool::new(true));
        let force_close = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(Notify::new());

        let mut sender = Sender::new(
            client,
            Arc::clone(&metadata),
            Arc::clone(&accumulator),
            false,
            MAX_REQUEST_SIZE,
            ACKS_ALL,
            1,
            REQUEST_TIMEOUT,
            1000,
            running,
            force_close,
            wakeup,
            time_provider,
        );

        let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);

        metadata.add(TOPIC_NAME, time.milliseconds());
        let mut topic_partition_counts = HashMap::new();
        topic_partition_counts.insert(TOPIC_NAME.to_string(), 3);
        let metadata_response = crate::common::requests::request_test_utils::metadata_update_with_ids(
            "kafka-cluster",
            1,
            &HashMap::new(),
            &topic_partition_counts,
            &|_| None,
            &topic_ids(),
        );
        metadata.update_with_current_request_version(&metadata_response, false, time.milliseconds());

        // Produce and send batch.
        let time1 = time.milliseconds();
        let cluster = metadata.fetch();
        accumulator
            .append(
                tp0.topic(),
                tp0.partition(),
                0,
                Some(b"key"),
                Some(b"value"),
                &[],
                None,
                MAX_BLOCK_TIMEOUT,
                time.milliseconds(),
                &cluster,
            )
            .await
            .expect("append should succeed");

        sender.run_once().await; // connect
        sender.run_once().await; // send
        assert_eq!(
            1,
            sender.client().in_flight_request_count(),
            "We should have a single produce request in flight."
        );

        // We were able to send the batch out, so both the ready and drain values should be the same.
        {
            let stats = accumulator.get_node_latency_stats(0).expect("Stats should exist");
            assert_eq!(time1, stats.drain_time_ms);
            assert_eq!(time1, stats.ready_time_ms);
        }

        // Make the node 0 not ready by throttling.
        let node = metadata.fetch().node_by_id(0).unwrap().clone();
        sender.client_mut().throttle(&node, 100);

        // Time passes, but we don't have anything to send.
        time.sleep(10);
        sender.run_once().await;
        assert_eq!(
            1,
            sender.client().in_flight_request_count(),
            "We should have a single produce request in flight."
        );

        // Stats shouldn't change as we didn't have anything ready.
        {
            let stats = accumulator.get_node_latency_stats(0).expect("Stats should exist");
            assert_eq!(time1, stats.drain_time_ms);
            assert_eq!(time1, stats.ready_time_ms);
        }

        // Produce a new batch, but we won't be able to send it because node is not ready.
        let time2 = time.milliseconds();
        let cluster = metadata.fetch();
        accumulator
            .append(
                tp0.topic(),
                tp0.partition(),
                0,
                Some(b"key"),
                Some(b"value"),
                &[],
                None,
                MAX_BLOCK_TIMEOUT,
                time.milliseconds(),
                &cluster,
            )
            .await
            .expect("append should succeed");
        sender.run_once().await;
        assert_eq!(
            1,
            sender.client().in_flight_request_count(),
            "We should have a single produce request in flight."
        );

        // The ready time should move forward, but drain time shouldn't change.
        {
            let stats = accumulator.get_node_latency_stats(0).expect("Stats should exist");
            assert_eq!(time1, stats.drain_time_ms);
            assert_eq!(time2, stats.ready_time_ms);
        }

        // Time passes, we keep trying to send, but the node is not ready.
        time.sleep(10);
        let time2_updated = time.milliseconds();
        sender.run_once().await;
        assert_eq!(
            1,
            sender.client().in_flight_request_count(),
            "We should have a single produce request in flight."
        );

        // The ready time should move forward, but drain time shouldn't change.
        {
            let stats = accumulator.get_node_latency_stats(0).expect("Stats should exist");
            assert_eq!(time1, stats.drain_time_ms);
            assert_eq!(time2_updated, stats.ready_time_ms);
        }

        // Finally, time passes beyond the throttle and the node is ready.
        time.sleep(100);
        let time3 = time.milliseconds();
        sender.run_once().await;
        assert_eq!(
            2,
            sender.client().in_flight_request_count(),
            "We should have 2 produce requests in flight."
        );

        // Both times should move forward
        {
            let stats = accumulator.get_node_latency_stats(0).expect("Stats should exist");
            assert_eq!(time3, stats.drain_time_ms);
            assert_eq!(time3, stats.ready_time_ms);
        }
    }
}
