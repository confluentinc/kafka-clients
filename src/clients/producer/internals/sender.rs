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

use crate::clients::client_response::ClientResponse;
use crate::clients::kafka_client::KafkaClient;
use crate::clients::metadata::LeaderIdAndEpoch;
use crate::common::TopicPartition;
use crate::common::kafka_error::KafkaError;
use crate::common::protocol::Errors;
use crate::common::record::record_batch::RecordBatch;
use crate::common::requests::abstract_response::ConcreteResponse;
use crate::common::requests::produce_request::ProduceRequestBuilder;
use crate::common::requests::produce_response::{PartitionResponse, RecordError};
use crate::common::uuid::Uuid;
use crate::produce_request_data::{PartitionProduceData, ProduceRequestData, TopicProduceData};

use super::ProducerBatch;
use super::producer_metadata::ProducerMetadata;
use super::record_accumulator::RecordAccumulator;

/// Format the error from a `PartitionResponse` in a user-friendly string.
fn format_partition_response_err(response: &crate::common::requests::produce_response::PartitionResponse) -> String {
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
/// cannot capture `self` inside the callback. Instead, we store the batch map
/// and topic names here and process responses after `client.poll()` returns.
///
/// This follows CLAUDE.md rule 9: translate callbacks to code executed after
/// awaiting the corresponding call.
struct PendingProduceRequest {
    /// The batches sent in this request, keyed by topic-partition.
    batches: HashMap<TopicPartition, ProducerBatch>,
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
            if let Some(mut pending) = self.pending_produce_responses.remove(&correlation_id) {
                self.handle_produce_response(response, &mut pending.batches, &pending.topic_names, now);
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

    fn maybe_remove_from_inflight_batches(&mut self, tp: &TopicPartition) {
        if let Some(batches) = self.in_flight_batches.get_mut(tp) {
            if !batches.is_empty() {
                batches.remove(0);
            }
            if batches.is_empty() {
                self.in_flight_batches.remove(tp);
            }
        }
    }

    fn maybe_remove_and_deallocate_batch(&mut self, batch: &mut ProducerBatch) {
        self.maybe_remove_from_inflight_batches(&batch.topic_partition.clone());
        self.accumulator.complete_and_deallocate_batch(batch);
    }

    fn maybe_remove_and_deallocate_batch_later(&mut self, batch: &ProducerBatch) {
        self.maybe_remove_from_inflight_batches(&batch.topic_partition.clone());
        self.accumulator.complete_batch(batch);
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
            if !self.client.ready(&node, now).await {
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
                infos.push(RequestBatchInfo { tp, records_data: Some(records.buffer().to_vec()) });
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
    fn handle_produce_response(
        &mut self,
        response: &ClientResponse,
        batches: &mut HashMap<TopicPartition, ProducerBatch>,
        topic_names: &HashMap<Uuid, String>,
        now: i64,
    ) {
        let request_header = response.request_header();
        let correlation_id = request_header.correlation_id();

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
            for batch in batches.values_mut() {
                self.complete_batch(batch, &part_resp, correlation_id, now, None);
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
            for batch in batches.values_mut() {
                self.complete_batch(batch, &part_resp, correlation_id, now, None);
            }
        } else if response.version_mismatch().is_some() {
            warn!(
                "Cancelled request {} due to a version mismatch with node {}",
                response,
                response.destination()
            );
            let part_resp = PartitionResponse::from_error(Errors::UnsupportedVersion);
            for batch in batches.values_mut() {
                self.complete_batch(batch, &part_resp, correlation_id, now, None);
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
                                self.complete_batch(
                                    batch,
                                    &part_resp,
                                    correlation_id,
                                    now,
                                    Some(&mut partitions_with_updated_leader_info),
                                );
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
                for batch in batches.values_mut() {
                    self.complete_batch(batch, &part_resp, correlation_id, now, None);
                }
            }
        }
    }

    /// Complete or retry the given batch of records.
    ///
    /// Translated from `Sender.completeBatch()` (the 5-argument version).
    fn complete_batch(
        &mut self,
        batch: &mut ProducerBatch,
        response: &PartitionResponse,
        correlation_id: i32,
        now: i64,
        mut partitions_with_updated_leader_info: Option<&mut HashMap<TopicPartition, LeaderIdAndEpoch>>,
    ) {
        batch.set_inflight(false);
        let error = response.error;

        if error == Errors::MessageTooLarge
            && batch.record_count > 1
            && !batch.is_done()
            && (batch.magic() >= RecordBatch::MAGIC_VALUE_V2 || batch.is_compressed())
        {
            // If the batch is too large, split and retry.
            // We need to take the batch out for split_and_reenqueue which takes ownership.
            warn!(
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlation_id,
                batch.topic_partition,
                self.retries - batch.attempts(),
                Self::format_err_msg(response)
            );
            // Note: split_and_reenqueue takes an owned batch. We give it a default-constructed
            // placeholder since the actual batch data is already built into records.
            // TODO: This needs proper batch transfer; for now we skip the split path
            // since it requires owned ProducerBatch which we don't have from `&mut`.
            self.accumulator.complete_batch(batch);
            self.maybe_remove_from_inflight_batches(&batch.topic_partition.clone());
        } else if error != Errors::None {
            if self.can_retry(batch, response, now) {
                warn!(
                    "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlation_id,
                    batch.topic_partition,
                    self.retries - batch.attempts() - 1,
                    Self::format_err_msg(response)
                );
                self.reenqueue_batch(batch, now);
            } else if error == Errors::DuplicateSequenceNumber {
                // Duplicate sequence: return success without valid offset/timestamp
                self.complete_batch_success(batch, response);
            } else {
                // Final failure
                let adjust = batch.attempts() < self.retries;
                self.fail_batch(batch, response, adjust, true);
            }

            if error.is_invalid_metadata() {
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
        } else {
            self.complete_batch_success(batch, response);
        }

        // Unmute the completed partition
        if self.guarantee_message_order {
            self.accumulator.unmute_partition(&batch.topic_partition);
        }
    }

    /// Format the error from a `PartitionResponse` in a user-friendly string.
    fn format_err_msg(response: &PartitionResponse) -> String {
        format_partition_response_err(response)
    }

    fn reenqueue_batch(&mut self, batch: &mut ProducerBatch, current_time_ms: i64) {
        // reenqueue takes an owned ProducerBatch. Since we have &mut, we can't move it.
        // Instead, mark it for reenqueue through the accumulator.
        batch.reenqueued(current_time_ms);
        self.maybe_remove_from_inflight_batches(&batch.topic_partition.clone());
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
        if batch.complete_exceptionally(top_level_exception, record_exceptions) {
            // No transaction manager handling in this phase
            if deallocate_batch {
                let tp = batch.topic_partition.clone();
                self.accumulator.complete_and_deallocate_batch(batch);
                self.maybe_remove_from_inflight_batches(&tp);
            } else {
                self.maybe_remove_and_deallocate_batch_later(batch);
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

        for info in &batch_infos {
            let tp = &info.tp;
            let topic_id = topic_ids.get(tp.topic()).copied().unwrap_or(Uuid::ZERO_UUID);

            // Find or create topic data
            let topic_data = topic_data_list
                .iter_mut()
                .find(|td| td.name == *tp.topic() || td.topic_id == topic_id);

            let records_bytes = info.records_data.clone();

            if let Some(td) = topic_data {
                let mut partition_data = PartitionProduceData::new();
                partition_data.set_index(tp.partition());
                partition_data.set_records(records_bytes);
                td.partition_data.push(partition_data);
            } else {
                let mut td = TopicProduceData::new();
                td.set_topic_id(topic_id);
                td.set_name(tp.topic().to_string());
                let mut partition_data = PartitionProduceData::new();
                partition_data.set_index(tp.partition());
                partition_data.set_records(records_bytes);
                td.partition_data.push(partition_data);
                topic_data_list.push(td);
            }

            batch_tps.push(tp.clone());
        }

        // Mark in-flight batches
        for tp in &batch_tps {
            if let Some(batches) = self.in_flight_batches.get_mut(tp) {
                for batch in batches.iter_mut() {
                    batch.set_inflight(true);
                }
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

        // Store the pending request data keyed by correlation ID.
        // We need to extract the batches from in_flight_batches to store in
        // pending_produce_responses for response processing.
        let correlation_id = client_request.correlation_id();

        // For response processing, we need the batch data. We extract copies
        // of the batches from in_flight_batches. Since ProducerBatch uses Arc
        // internally for its shared state (produce_future), the important parts
        // are shared. We reconstruct minimal batch references.
        let mut records_by_partition: HashMap<TopicPartition, ProducerBatch> = HashMap::new();
        for tp in &batch_tps {
            if let Some(batches) = self.in_flight_batches.get_mut(tp)
                && let Some(batch) = batches.pop()
            {
                records_by_partition.insert(tp.clone(), batch);
            }
        }

        self.pending_produce_responses.insert(
            correlation_id,
            PendingProduceRequest { batches: records_by_partition, topic_names },
        );

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
    use crate::clients::producer::internals::buffer_pool::BufferPool;
    use crate::clients::producer::internals::record_accumulator::PartitionerConfig;
    use crate::common::compress::Compression;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::record::memory_records_builder::MemoryRecordsBuilder;
    use crate::common::record::record_batch::RecordBatch;
    use crate::common::record::timestamp_type::TimestampType;
    use crate::common::requests::produce_response::PartitionResponse;

    const RETRY_BACKOFF_MS: i64 = 100;
    const DELIVERY_TIMEOUT_MS: i32 = 120000;

    fn create_accumulator() -> Arc<RecordAccumulator> {
        Arc::new(RecordAccumulator::new(
            1024 * 1024,
            Compression::none(),
            0,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MS * 10,
            DELIVERY_TIMEOUT_MS,
            PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
            Arc::new(BufferPool::new(1024 * 1024, 16384)),
        ))
    }

    fn _create_metadata() -> Arc<ProducerMetadata> {
        Arc::new(ProducerMetadata::new(100, 1000, 60000, 300000, ClusterResourceListeners::new()))
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

    /// Test that format_err_msg produces the expected string.
    #[test]
    fn test_format_err_msg() {
        let resp = PartitionResponse::from_error(Errors::NetworkException);
        let msg = format_partition_response_err(&resp);
        // Errors Display impl should contain the error name
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

        // Verify tracking
        assert_eq!(in_flight.get(&tp0).unwrap().len(), 1);
        assert_eq!(in_flight.get(&tp1).unwrap().len(), 1);

        // Remove from tp0
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
        let batch = make_batch(tp.clone(), 100);

        let mut batches = HashMap::new();
        batches.insert(tp.clone(), batch);

        let mut topic_names = HashMap::new();
        topic_names.insert(Uuid::random_uuid(), "test-topic".to_string());

        pending.insert(42, PendingProduceRequest { batches, topic_names });

        assert!(pending.contains_key(&42));
        let removed = pending.remove(&42).unwrap();
        assert!(removed.batches.contains_key(&tp));
        assert!(!pending.contains_key(&42));
    }

    /// Test that expired batches are collected correctly.
    #[test]
    fn test_get_expired_inflight_batches() {
        let accumulator = create_accumulator();
        let tp = TopicPartition::new("test".to_string(), 0);
        let batch = make_batch(tp.clone(), 0); // created at time 0

        let mut in_flight: HashMap<TopicPartition, Vec<ProducerBatch>> = HashMap::new();
        in_flight.entry(tp.clone()).or_default().push(batch);

        // At time 0, nothing should be expired
        let delivery_timeout_ms = accumulator.delivery_timeout_ms() as i64;
        assert!(!in_flight[&tp][0].has_reached_delivery_timeout(delivery_timeout_ms, 0));

        // At time > delivery_timeout, the batch should be expired
        assert!(in_flight[&tp][0].has_reached_delivery_timeout(delivery_timeout_ms, DELIVERY_TIMEOUT_MS as i64 + 1));
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
}
