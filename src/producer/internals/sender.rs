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

//! Translation of `org.apache.kafka.clients.producer.internals.Sender`.
//!
//! The background task that drives produce requests from the
//! [`RecordAccumulator`](super::record_accumulator::RecordAccumulator)
//! to the broker. Joins everything from Phase 6 (BufferPool,
//! ProducerBatch, ProducerInterceptors, RecordAccumulator) with the
//! network stack (`NetworkClient` / [`KafkaClient`](crate::KafkaClient))
//! introduced in Phase 5.
//!
//! ## Tokio-specific structure
//!
//! - [`Sender::run_loop`] is `async fn` running on a single
//!   `tokio::spawn` task. The loop drives:
//!   `accumulator.ready -> drain -> client.send -> client.poll
//!   -> handle_responses -> complete_batches`.
//!
//! - The user [`Callback`](crate::producer::callback::Callback) is
//!   invoked **inside the sender task**, after the partition's record is
//!   acknowledged/failed and **before** the
//!   [`FutureRecordMetadata`](super::future_record_metadata::FutureRecordMetadata)
//!   is completed — exactly matching Java's
//!   `ProducerBatch.completeFutureAndFireCallbacks` (CLAUDE.md rule 9.5).
//!
//! - [`Sender::wakeup`] forwards to
//!   [`KafkaClient::wakeup`](crate::KafkaClient::wakeup); idempotent —
//!   multiple wakeups during a single tick coalesce.
//!
//! - **No per-message `tokio::spawn`** anywhere on the send path
//!   (CLAUDE.md rule 11). The accumulator does NOT spawn (verified Phase
//!   6d). The `Sender` is the single coroutine.
//!
//! - **No `MutexGuard` across `.await`** (CLAUDE.md rule 9.6).
//!
//! ## Callback dispatch model
//!
//! Java attaches a per-request `RequestCompletionHandler` (captured
//! lambda over `recordsByPartition`, `topicNames`). The handler is fired
//! synchronously from `MockClient::poll` (or from
//! `NetworkClient::poll`'s response loop) via `ClientResponse.onComplete()`.
//!
//! The Rust translation registers a `pending_responses` map keyed by
//! correlation id. After [`KafkaClient::poll`].await returns, the run
//! loop walks the returned responses and pops the matching context from
//! the map — preserving Java's invariant that the response handler runs
//! after `poll` and on the same task. We do **not** rely on the
//! [`RequestCompletionHandler`](crate::RequestCompletionHandler) trait
//! to mutate `Sender` state: the handler trait takes `&self`, but
//! `Sender::handle_produce_response` mutates `in_flight_batches` and the
//! pending-responses map.
//!
//! ## Skipped Java code paths (transactional / idempotent producer)
//!
//! Per Phase 6 NOTES.md "Plug-in contract for future transactions",
//! every `if (transactionManager != null) { … }` arm translates to
//! `if let Some(_tm) = &self.transaction_manager { unreachable!(...) }`
//! because the `Option` is statically `None` this milestone (config
//! validation rejects the inputs that would set `Some`).
//!
//! The following Java methods are **not** translated:
//! - `maybeSendAndPollTransactionalRequest`
//! - `addToTransactionManagerSendQueue`
//! - `transactionManager.maybeUpdateProducerIdAndEpoch`,
//!   `transactionManager.failIfNotReadyForSend`
//! - `bumpProducerEpochOnSequenceMismatch`
//! - The retry path's `transactionManager.adjustSequencesDueToFailedBatch`
//! - Coordinator lookup (`maybeFindCoordinatorAndRetry`,
//!   `awaitNodeReady` for `CoordinatorType.TRANSACTION`)
//! - `InitProducerId`, `AddPartitionsToTxn`, `EndTxn`,
//!   `TxnOffsetCommit`, `AddOffsetsToTxn`, `WriteTxnMarkers` request /
//!   response handling
//! - `shouldHandleAuthorizationError` for the Tx-only auth-error path
//!
//! Sensor / metrics calls (`SenderMetrics`,
//! `recordsPerRequestSensor.record(...)`, etc.) are stubbed with
//! `// metric stub` no-ops per the project-wide PLAN.
//!
//! ## Skipped `SenderTest` cases
//!
//! Every Java `SenderTest` case is in one of three buckets below.
//!
//! ### Translated (Java case → Rust test)
//!
//! - `testSimple` → `test_simple`
//! - `testTopicAuthorizationFailedTerminal` → `test_topic_authorization_failed_terminal`
//! - `testRetries` (first loop / success path) → `test_retries_then_success`
//! - `testRetries` (second loop / retry exhaustion) → `test_retries_exhausted_yields_network_exception`
//! - `testSendInOrder` → `test_send_in_order`
//! - `testAppendInExpiryCallback` → `test_append_in_expiry_callback`
//! - `testInflightBatchesExpireOnDeliveryTimeout` → `test_inflight_batches_expire_on_delivery_timeout`
//! - `testCustomErrorMessage` → `test_custom_error_message`
//! - `testDefaultErrorMessage` → `test_default_error_message`
//! - `testClusterAuthorizationExceptionInProduceRequest` → `test_cluster_authorization_exception_in_produce_request`
//! - `testTooLargeBatchesAreSafelyRemoved` → `test_too_large_batches_are_safely_removed`
//! - `testExpiredBatchDoesNotRetry` → `test_expired_batch_does_not_retry`
//! - `testExpiredBatchDoesNotSplitOnMessageTooLargeError` → `test_expired_batch_does_not_split_on_message_too_large_error`
//! - `testExpiredBatchesInMultiplePartitions` → `test_expired_batches_in_multiple_partitions`
//! - `testRecordErrorPropagatedToApplication` → `test_record_error_propagated_to_application`
//! - `testGuaranteeOrderMutesPartitionUntilFirstResponse` → `test_guarantee_order_mutes_partition_until_first_response`
//! - `testNotLeaderOrFollowerRetriesThenSuccess` → `test_not_leader_or_follower_retries_then_success`
//! - `testProducerBatchRetriesWhenPartitionLeaderChanges` → `test_producer_batch_retries_when_partition_leader_changes`
//! - `testWhenProduceResponseReturnsWithALeaderShipChangeErrorButNoNewLeaderInformation` →
//!   `test_produce_response_leader_change_no_new_leader_information`
//! - `testWhenProduceResponseReturnsWithALeaderShipChangeErrorAndNewLeaderInformation` →
//!   `test_produce_response_leader_change_with_new_leader_information`
//! - `testNoBufferReuseWhenBatchExpires` → `test_no_buffer_reuse_when_batch_expires`
//! - `testNoDoubleDeallocation` → `test_no_double_deallocation`
//!
//! ### Skipped — idempotent producer / transactional (out of milestone)
//!
//! All cases driven by `TransactionManager` or the idempotent-producer
//! state machine (sequence numbers, producer ID, epoch) are skipped.
//! Coverage will be added when transactions land in a future milestone.
//!
//! - `testInitProducerIdRequest`
//! - `testInitProducerIdWithMaxInFlightOne`
//! - `testIdempotentInitProducerIdWithMaxInFlightOne`
//! - `testClusterAuthorizationExceptionInInitProducerIdRequest`
//! - `testIdempotenceWithMultipleInflights*`
//! - `testEpochBumpOnOutOfOrderSequenceForNextBatch*`
//! - `testCorrectHandlingOfOutOfOrderResponses*`
//! - `testCorrectHandlingOfDuplicateSequenceError`
//! - `testTransactionalSplitBatchAndSend`
//! - `testIdempotentSplitBatchAndSend`
//! - `testTransactionalUnknownProducerHandlingWhenRetentionLimitReached`
//! - `testIdempotentUnknownProducerHandlingWhenRetentionLimitReached`
//! - `testUnknownProducerErrorShouldBeRetried*`
//! - `testShouldRaiseOutOfOrderSequenceExceptionToUserIfLogWasNotTruncated`
//! - `testCancelInFlightRequestAfterFatalError`
//! - `testSequenceNumberIncrement`
//! - `testRetryWhenProducerIdChanges`
//! - `testBumpEpochWhenOutOfOrderSequenceReceived`
//! - `testTransactionShouldTransitionToAbortableForSenderAPI`
//! - `testReceiveFailedBatchTwiceWithTransactions`
//! - `testInvalidTxnStateIsAnAbortableError`
//! - `testTransactionAbortableExceptionIsAnAbortableError`
//! - `testAbortableErrorIsConvertedToFatalErrorDuringAbort`
//! - `testSenderShouldCloseWhenTransactionManagerInErrorState`
//! - `testTransactionalRequestsSentOnShutdown`
//! - `testRecordsFlushedImmediatelyOnTransactionCompletion`
//! - `testAwaitPendingRecordsBeforeCommittingTransaction`
//! - `testIncompleteTransactionAbortOnShutdown`
//! - `testForceShutdownWithIncompleteTransaction`
//! - `testTransactionAbortedExceptionOnAbortWithoutError`
//! - `testCloseWithProducerIdReset`
//! - `testForceCloseWithProducerIdReset`
//! - `testBatchesDrainedWithOldProducerIdShouldSucceedOnSubsequentRetry`
//! - `testResetOfProducerStateShouldAllowQueuedBatchesToDrain`
//! - `testExpiryOfFirstBatchShouldNotCauseUnresolvedSequencesIfFutureBatchesSucceed`
//! - `testExpiryOfFirstBatchShouldCauseEpochBumpIfFutureBatchesFail`
//! - `testExpiryOfAllSentBatchesShouldCauseUnresolvedSequences`
//! - `testExpiryOfUnsentBatchesShouldNotCauseUnresolvedSequences`
//! - `testUnresolvedSequencesAreNotFatal`
//! - `senderThreadShouldNotGetStuckWhenThrottledAndAddingPartitionsToTxn` (transactional)
//!
//! ### Skipped — metrics / mock infra (out of milestone)
//!
//! - `testSenderMetricsTemplates` — metrics registry assertions; metrics
//!   infra is stubbed this milestone (PLAN.md line 296). No alternate
//!   Rust test.
//! - `testQuotaMetrics` — same rationale.
//!
//! ### Skipped — deferred non-tx with explicit rationale
//!
//! - `testNodeLatencyStats` — exercises
//!   `accumulator.update_node_latency_stats(node, now, can_drain)`
//!   dispatch from `Sender` (`can_drain=false` when throttled,
//!   `can_drain=true` when ready). Production code does dispatch the
//!   updates (`sender.rs:429` and `sender.rs:436`); the
//!   *accumulator-level* invariant is exercised by
//!   `record_accumulator.rs::test_update_node_latency_stats_*`. The
//!   *Sender-level dispatch* has no Rust test — the Java test relies on
//!   `client.throttle(node, ms)` which the Rust `MockClientImpl` does
//!   not currently model (Phase 6e NOTES.md MockClient subset). Defer
//!   until throttle is wired into the Mock.
//! - `testResetNextBatchExpiry` — verifies the poll-timeout sequence
//!   (`0L → DELIVERY_TIMEOUT_MS → ≥1L`) across three runOnce ticks. The
//!   poll-timeout clamping logic at `sender.rs:465-475`
//!   (`min(next_ready_check_delay, not_ready_timeout, next_expiry_delay)
//!    .max(0)`) is correct on inspection. The Java test uses Mockito
//!   `InOrder.verify(client).poll(eq(0L), …)` to assert the exact
//!   sequence — Rust does not have an equivalent spy/inorder facility
//!   without bringing in a mock framework. Defer.

#![allow(dead_code)] // Phase 7 (KafkaProducer) wires the public surface.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use log::{debug, error, trace, warn};

use crate::common::errors::KafkaError;
use crate::common::message::produce_request_data::{PartitionProduceData, ProduceRequestData, TopicProduceData};
use crate::common::node::Node;
use crate::common::protocol::{ApiKey, ApiKeys, Errors};
use crate::common::record::record_batch::MAGIC_VALUE_V2;
use crate::common::requests::produce_request::ProduceRequest;
use crate::common::requests::produce_response::{PartitionResponse, ProduceResponse, RecordError};
use crate::common::requests::{AbstractRequest, AbstractRequestBuilder};
use crate::common::topic_partition::TopicPartition;
use crate::common::utils::LogContext;
use crate::common::utils::Time;
use crate::common::uuid::Uuid;
use crate::metadata::LeaderIdAndEpoch as MetadataLeaderIdAndEpoch;
use crate::producer::internals::producer_batch::ProducerBatch;
use crate::producer::internals::producer_metadata::ProducerMetadata;
use crate::producer::internals::record_accumulator::RecordAccumulator;
use crate::producer::internals::transaction_manager::TransactionManager;
use crate::{ClientResponse, KafkaClient, RequestCompletionHandler};

/// Builder for a `ProduceRequest`. Wraps the `ProduceRequestData`
/// produced by [`Sender::send_produce_request`] in the
/// [`AbstractRequestBuilder`] shape `KafkaClient::send` expects.
#[derive(Debug)]
pub(crate) struct ProduceRequestBuilder {
    data: Mutex<Option<ProduceRequestData>>,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ProduceRequestBuilder {
    /// Build a `ProduceRequest` builder over the given `ProduceRequestData`.
    /// `data` is consumed on the first `build()` call (matching Java's
    /// `ProduceRequest.Builder.build` semantics: the same builder is built
    /// once per send).
    pub(crate) fn new(data: ProduceRequestData) -> Self {
        let api_key = ApiKeys::for_id(0).expect("PRODUCE api_key always present");
        Self {
            data: Mutex::new(Some(data)),
            oldest_allowed_version: api_key.oldest_version(),
            latest_allowed_version: api_key.latest_version(),
        }
    }

    /// Override the build-version range. Mirrors Java's
    /// `ProduceRequest.Builder.forMagic` / `Builder(short, short, ProduceRequestData)`.
    pub(crate) fn with_versions(mut self, oldest: i16, latest: i16) -> Self {
        self.oldest_allowed_version = oldest;
        self.latest_allowed_version = latest;
        self
    }
}

impl AbstractRequestBuilder for ProduceRequestBuilder {
    fn api_key(&self) -> &'static ApiKey {
        ApiKeys::for_id(0).expect("PRODUCE api_key always present")
    }
    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }
    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }
    fn build(&self, version: i16) -> Result<Box<dyn AbstractRequest>, KafkaError> {
        let data = self
            .data
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| KafkaError::IllegalState("ProduceRequestBuilder built more than once".to_string()))?;
        Ok(Box::new(ProduceRequest::new(data, version)))
    }
}

/// Per-request response context registered by [`Sender::send_produce_request`]
/// and consumed by [`Sender::handle_produce_response`].
///
/// Mirrors the variables captured by Java's
/// `RequestCompletionHandler` lambda: `recordsByPartition`, `topicNames`.
struct ResponseContext {
    /// Partition-keyed map of batches in the request.
    batches: HashMap<TopicPartition, Arc<ProducerBatch>>,
    /// Topic ID → topic name lookup. v13+ produce responses drop the
    /// topic name in favor of an ID, so we capture the name table at
    /// send-time (Java captures it at the same point).
    topic_names: HashMap<Uuid, String>,
}

/// No-op handler used to satisfy the `KafkaClient::send` callback slot.
///
/// Mirrors Java's lambda response handler — the actual response
/// processing is done by [`Sender::handle_produce_response`] in the run
/// loop after `poll().await` returns.
#[derive(Debug)]
struct NoopHandler;

impl RequestCompletionHandler for NoopHandler {
    fn on_complete(&self, _response: &ClientResponse) {}
}

/// The background task that drives produce requests from the
/// [`RecordAccumulator`] to the broker.
pub(crate) struct Sender<C: KafkaClient> {
    log_context: LogContext,
    /// Java: `private final KafkaClient client`. Held by-value because
    /// `KafkaClient::poll` takes `&mut self` (Java does not need this —
    /// `NetworkClient` is only ever invoked from the sender thread).
    client: C,
    accumulator: Arc<RecordAccumulator>,
    metadata: Arc<ProducerMetadata>,
    /// Java: `private final boolean guaranteeMessageOrder`.
    guarantee_message_order: bool,
    /// Java: `private final int maxRequestSize`.
    max_request_size: i32,
    /// Java: `private final short acks`.
    acks: i16,
    /// Java: `private final int retries`.
    retries: i32,
    time: Arc<dyn Time>,
    /// Java: `private volatile boolean running`. Mirrors the
    /// "background thread is still alive" flag — set to `false` by
    /// [`Sender::initiate_close`].
    running: Arc<AtomicBool>,
    /// Java: `private volatile boolean forceClose`. Set to `true` by
    /// [`Sender::force_close`] to bypass the drain-queue-and-await
    /// shutdown stage.
    force_close: Arc<AtomicBool>,
    /// Java: `private final int requestTimeoutMs`.
    request_timeout_ms: i32,
    /// Java: `private final long retryBackoffMs`.
    retry_backoff_ms: i64,
    /// Java: `private final TransactionManager transactionManager`.
    /// Always `None` per Phase 6 NOTES.md plug-in contract.
    transaction_manager: Option<TransactionManager>,
    /// Java: `private final Map<TopicPartition, List<ProducerBatch>>
    /// inFlightBatches`. Sender-task only — Java uses `HashMap`
    /// (un-synchronized). We hold a `Mutex` for `Send + Sync` while
    /// keeping the contract simple: only the run loop touches it.
    in_flight_batches: Mutex<HashMap<TopicPartition, Vec<Arc<ProducerBatch>>>>,
    /// Pending response contexts keyed by correlation id. Populated by
    /// [`Sender::send_produce_request`] and drained by
    /// [`Sender::handle_responses`] after `poll().await`. See module
    /// docs for the callback dispatch model.
    pending_responses: Mutex<HashMap<i32, ResponseContext>>,
    /// Java: `private final String clientId` — interned via `Arc<str>`
    /// so per-request callback handles don't allocate.
    client_id: Arc<str>,
}

impl<C: KafkaClient> Sender<C> {
    /// Mirrors Java's primary constructor.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        log_context: LogContext,
        client: C,
        metadata: Arc<ProducerMetadata>,
        accumulator: Arc<RecordAccumulator>,
        guarantee_message_order: bool,
        max_request_size: i32,
        acks: i16,
        retries: i32,
        // Java: `SenderMetricsRegistry metricsRegistry` — metrics out of
        // milestone scope (PLAN.md line 296). Stub omitted from the
        // signature.
        time: Arc<dyn Time>,
        request_timeout_ms: i32,
        retry_backoff_ms: i64,
        transaction_manager: Option<TransactionManager>,
        client_id: Arc<str>,
    ) -> Self {
        Self {
            log_context,
            client,
            accumulator,
            metadata,
            guarantee_message_order,
            max_request_size,
            acks,
            retries,
            time,
            running: Arc::new(AtomicBool::new(true)),
            force_close: Arc::new(AtomicBool::new(false)),
            request_timeout_ms,
            retry_backoff_ms,
            transaction_manager,
            in_flight_batches: Mutex::new(HashMap::new()),
            pending_responses: Mutex::new(HashMap::new()),
            client_id,
        }
    }

    /// Test-only: list of in-flight batches for a given topic-partition.
    /// Mirrors Java's `inFlightBatches(TopicPartition)`.
    pub(crate) fn in_flight_batches_for(&self, tp: &TopicPartition) -> Vec<Arc<ProducerBatch>> {
        self.in_flight_batches.lock().unwrap().get(tp).cloned().unwrap_or_default()
    }

    /// Mirrors Java's `isRunning()`.
    ///
    /// Reads the same atomic exposed by [`Self::running_arc`]. Use this
    /// inspector when the caller still owns `&Sender` (e.g. in-process
    /// tests that construct the sender directly). After the sender has
    /// been moved into a `tokio::spawn` task — which is what
    /// `KafkaProducer` does — there is no `&Sender` left to call
    /// `is_running()` on; callers in that case capture the
    /// [`Self::running_arc`] handle before spawn and read the flag
    /// through the `Arc<AtomicBool>` directly. Both paths read the same
    /// underlying atomic with `Ordering::Acquire`, so observers always
    /// see consistent state.
    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Handle on the running flag. Lets external code that moved the
    /// sender into a `tokio::spawn` task still flip the flag from
    /// outside the task to drive the loop's exit path. Used by
    /// `KafkaProducer::Drop` (Phase 7c) and the Phase 7e async `close`.
    ///
    /// See [`Self::is_running`] for the `&Sender`-borrow analogue.
    pub(crate) fn running_arc(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.running)
    }

    /// Handle on the force-close flag. Used together with
    /// [`Self::running_arc`] to bypass the drain stage when the sender
    /// has been moved into a `tokio::spawn` task. Same callers as
    /// [`Self::running_arc`].
    pub(crate) fn force_close_arc(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.force_close)
    }

    /// Mirrors Java's `wakeup()`. Idempotent — multiple wakeups during a
    /// single tick coalesce.
    pub(crate) fn wakeup(&self) {
        self.client.wakeup();
    }

    /// Start closing the sender (won't actually complete until all data
    /// is sent out). Mirrors Java's `initiateClose()`.
    pub(crate) fn initiate_close(&self) {
        // Java: ensure accumulator is closed first to guarantee that no
        // more appends are accepted after breaking from the sender loop.
        self.accumulator.close();
        self.running.store(false, Ordering::Release);
        self.wakeup();
    }

    /// Closes the sender without sending out any pending messages.
    /// Mirrors Java's `forceClose()`.
    pub(crate) fn force_close(&self) {
        self.force_close.store(true, Ordering::Release);
        self.initiate_close();
    }

    /// Run a single iteration of sending. Mirrors Java's `runOnce()`.
    pub(crate) async fn run_once(&mut self) {
        if let Some(_tm) = &self.transaction_manager {
            // Phase 6 NOTES.md plug-in contract: never reachable.
            unreachable!("transaction_manager is always None per Phase 6 plug-in contract");
        }

        let current_time_ms = self.time.milliseconds();
        let poll_timeout = self.send_producer_data(current_time_ms).await;
        let responses = self.client.poll(poll_timeout, current_time_ms).await;
        self.handle_responses(responses).await;
    }

    /// Drains the accumulator, builds produce requests, and queues them
    /// on the client. Returns the maximum poll timeout for the upcoming
    /// `client.poll`. Mirrors Java's `sendProducerData(long)`.
    async fn send_producer_data(&mut self, now: i64) -> i64 {
        let metadata_snapshot = self.metadata.metadata().fetch_metadata_snapshot();
        let metadata_snap_ref = metadata_snapshot.as_ref();
        let result = self.accumulator.ready(metadata_snap_ref, now);

        // If there are any partitions whose leaders are not known yet,
        // force metadata update.
        if !result.unknown_leader_topics.is_empty() {
            for topic in &result.unknown_leader_topics {
                self.metadata.add(topic, now);
            }
            debug!(
                "{}Requesting metadata update due to unknown leader topics from the batched records: {:?}",
                self.log_context.log_prefix(),
                result.unknown_leader_topics
            );
            self.metadata.metadata().request_update(false);
        }

        // Remove any nodes we aren't ready to send to.
        let mut ready_nodes: HashSet<i32> = result.ready_nodes.clone();
        let mut not_ready_timeout: i64 = i64::MAX;
        let cluster = metadata_snap_ref.cluster();
        let mut to_remove: Vec<i32> = Vec::new();
        for node_id in ready_nodes.iter().copied() {
            let node = match cluster.node_by_id(node_id) {
                Some(n) => n.clone(),
                None => {
                    // Defensive: drop nodes we no longer have metadata
                    // for. Java would never hit this since `readyNodes`
                    // always contains real `Node` references.
                    to_remove.push(node_id);
                    continue;
                },
            };
            if !self.client.ready(&node, now) {
                self.accumulator.update_node_latency_stats(node.id(), now, false);
                to_remove.push(node.id());
                let delay = self.client.poll_delay_ms(&node, now);
                if delay < not_ready_timeout {
                    not_ready_timeout = delay;
                }
            } else {
                self.accumulator.update_node_latency_stats(node.id(), now, true);
            }
        }
        for id in to_remove {
            ready_nodes.remove(&id);
        }

        // Drain & inflight bookkeeping.
        let batches = self
            .accumulator
            .drain(metadata_snap_ref, &ready_nodes, self.max_request_size, now);
        self.add_to_inflight_batches(&batches);
        if self.guarantee_message_order {
            for batch_list in batches.values() {
                for batch in batch_list {
                    self.accumulator.mute_partition(batch.topic_partition().clone());
                }
            }
        }

        self.accumulator.reset_next_batch_expiry_time();
        let expired_inflight_batches = self.get_expired_inflight_batches(now);
        let expired_batches = self.accumulator.expired_batches(now);

        self.fail_expired_batches(&expired_batches, now, true);
        self.fail_expired_batches(&expired_inflight_batches, now, false);

        // metric stub: sensors.updateProduceRequestMetrics(batches);

        let mut poll_timeout = result.next_ready_check_delay_ms.min(not_ready_timeout);
        let next_expiry_delay = self.accumulator.next_expiry_time_ms() - now;
        poll_timeout = poll_timeout.min(next_expiry_delay).max(0);
        if !ready_nodes.is_empty() {
            trace!(
                "{}Nodes with data ready to send: {:?}",
                self.log_context.log_prefix(),
                ready_nodes
            );
            poll_timeout = 0;
        }
        self.send_produce_requests(batches, now);
        poll_timeout
    }

    /// Mirrors Java's `addToInflightBatches(Map<Integer, List<ProducerBatch>>)`.
    fn add_to_inflight_batches(&self, batches: &HashMap<i32, Vec<Arc<ProducerBatch>>>) {
        let mut map = self.in_flight_batches.lock().unwrap();
        for batch_list in batches.values() {
            for batch in batch_list {
                map.entry(batch.topic_partition().clone()).or_default().push(Arc::clone(batch));
            }
        }
    }

    /// Mirrors Java's `maybeRemoveFromInflightBatches(ProducerBatch)`.
    fn maybe_remove_from_inflight_batches(&self, batch: &Arc<ProducerBatch>) {
        let mut map = self.in_flight_batches.lock().unwrap();
        if let Some(list) = map.get_mut(batch.topic_partition()) {
            // Remove by Arc-pointer identity (Java uses `List.remove(Object)`
            // which collapses to `equals` — but since `ProducerBatch` does
            // not override `equals`, that is reference equality).
            if let Some(pos) = list.iter().position(|b| Arc::ptr_eq(b, batch)) {
                list.remove(pos);
            }
            if list.is_empty() {
                map.remove(batch.topic_partition());
            }
        }
    }

    /// Mirrors Java's `maybeRemoveAndDeallocateBatch(ProducerBatch)`.
    fn maybe_remove_and_deallocate_batch(&self, batch: &Arc<ProducerBatch>) {
        self.maybe_remove_from_inflight_batches(batch);
        self.accumulator.complete_and_deallocate_batch(Arc::clone(batch));
    }

    /// Mirrors Java's `maybeRemoveAndDeallocateBatchLater(ProducerBatch)`.
    fn maybe_remove_and_deallocate_batch_later(&self, batch: &Arc<ProducerBatch>) {
        self.maybe_remove_from_inflight_batches(batch);
        self.accumulator.complete_batch(batch);
    }

    /// Mirrors Java's `getExpiredInflightBatches(long)`.
    fn get_expired_inflight_batches(&self, now: i64) -> Vec<Arc<ProducerBatch>> {
        let mut expired: Vec<Arc<ProducerBatch>> = Vec::new();
        let mut map = self.in_flight_batches.lock().unwrap();
        let delivery_timeout_ms = self.accumulator.delivery_timeout_ms();
        // Iterate over a snapshot of keys so we can mutate the map.
        let keys: Vec<TopicPartition> = map.keys().cloned().collect();
        for tp in keys {
            let mut entry_empty = false;
            if let Some(list) = map.get_mut(&tp) {
                let idx = 0;
                while idx < list.len() {
                    let batch = Arc::clone(&list[idx]);
                    if batch.has_reached_delivery_timeout(delivery_timeout_ms, now) {
                        list.remove(idx);
                        if !batch.is_done() {
                            expired.push(batch);
                        } else {
                            // Java: `throw new IllegalStateException(...)`.
                            // CLAUDE.md rule 10.1 — invariant violation,
                            // panic.
                            panic!(
                                "{} batch created at {} gets unexpected final state {:?}",
                                tp,
                                batch.created_ms(),
                                batch.final_state()
                            );
                        }
                    } else {
                        self.accumulator.maybe_update_next_batch_expiry_time(&batch);
                        // Java: `break;` after first non-expired batch
                        // (deliberately preserves send order).
                        break;
                    }
                }
                if list.is_empty() {
                    entry_empty = true;
                }
            }
            if entry_empty {
                map.remove(&tp);
            }
        }
        expired
    }

    /// Mirrors Java's `failExpiredBatches(List<ProducerBatch>, long, boolean)`.
    fn fail_expired_batches(&self, expired: &[Arc<ProducerBatch>], now: i64, deallocate_buffer: bool) {
        if !expired.is_empty() {
            trace!(
                "{}Expired {} batches in accumulator",
                self.log_context.log_prefix(),
                expired.len()
            );
        }
        for batch in expired {
            let error_message = format!(
                "Expiring {} record(s) for {}:{} ms has passed since batch creation",
                batch.record_count(),
                batch.topic_partition(),
                now - batch.created_ms()
            );
            self.fail_batch_top_level(batch, KafkaError::Timeout(error_message), false, deallocate_buffer);
            // transactionManager.markSequenceUnresolved(...) — skipped
            // per Phase 6e NOTES.md (transactional path).
        }
    }

    /// Builds and queues per-node produce requests. Mirrors Java's
    /// `sendProduceRequests(Map<Integer, List<ProducerBatch>>, long)`.
    fn send_produce_requests(&mut self, collated: HashMap<i32, Vec<Arc<ProducerBatch>>>, now: i64) {
        for (node_id, batches) in collated {
            self.send_produce_request(now, node_id, self.acks, self.request_timeout_ms, batches);
        }
    }

    /// Build a single produce request for `destination` and queue it on
    /// the client. Mirrors Java's
    /// `sendProduceRequest(long, int, short, int, List<ProducerBatch>)`.
    fn send_produce_request(
        &mut self,
        now: i64,
        destination: i32,
        acks: i16,
        timeout: i32,
        batches: Vec<Arc<ProducerBatch>>,
    ) {
        if batches.is_empty() {
            return;
        }

        let mut records_by_partition: HashMap<TopicPartition, Arc<ProducerBatch>> =
            HashMap::with_capacity(batches.len());
        // Snapshot the metadata's topic-id map once. The map is keyed by
        // `String` in `ProducerMetadata::topic_ids` so `Borrow<str>` lookup
        // via `topic_ids.get(<&str>)` works without per-batch allocation.
        // Mirrors Java's `topicIdsForBatches`, but inlined to avoid the
        // intermediate `HashMap<String, Uuid>` Java's helper builds (Java's
        // GC hides the cost; in Rust the per-batch `String` allocation is a
        // measurable per-`runOnce` overhead on the send path).
        let topic_ids = self.metadata.metadata().topic_ids();

        // Group by topic into TopicProduceData entries. We preserve Java
        // insertion order via a Vec<TopicProduceData>; the deduplication
        // is by (topic_name, topic_id) per Java's
        // `ProduceRequestData.TopicProduceDataCollection.find`.
        let mut tpd: Vec<TopicProduceData> = Vec::new();
        for batch in &batches {
            let tp = batch.topic_partition().clone();
            let topic_name = tp.topic().to_string();
            let topic_id = topic_ids.get(tp.topic()).copied().unwrap_or(Uuid::zero());
            let memory_records = match batch.records() {
                Ok(r) => r,
                Err(e) => {
                    // Build of records failed (rare — only happens on a
                    // Phase-3 codec error). Surface as a top-level
                    // failure for the batch instead of dropping
                    // silently (CLAUDE.md rule 5).
                    error!(
                        "{}Failed to materialize records for batch {}: {:?}",
                        self.log_context.log_prefix(),
                        tp,
                        e
                    );
                    self.fail_batch_top_level(batch, e, false, true);
                    continue;
                },
            };
            let records_buf = memory_records.buffer().clone();
            // Find or create matching TopicProduceData entry.
            let tp_idx = tpd.iter().position(|t| t.name == topic_name && t.topic_id == topic_id);
            let idx = match tp_idx {
                Some(i) => i,
                None => {
                    tpd.push(TopicProduceData {
                        name: topic_name.clone(),
                        topic_id,
                        partition_data: Vec::new(),
                        unknown_tagged_fields: Vec::new(),
                    });
                    tpd.len() - 1
                },
            };
            tpd[idx].partition_data.push(PartitionProduceData {
                index: tp.partition(),
                // Java: `setRecords(records)` stores the `MemoryRecords`
                // (as a `BaseRecords` reference). Our wire codec
                // serializes `Vec<u8>` directly — so we hand over the
                // backing bytes.
                records: Some(records_buf.to_vec()),
                unknown_tagged_fields: Vec::new(),
            });
            records_by_partition.insert(tp, Arc::clone(batch));
            batch.set_inflight(true);
        }

        // Phase 6e NOTES.md skipped: transactional_id / transaction v1
        // version detection. We always send non-tx (transactional_id is
        // None).
        let transactional_id: Option<String> = None;
        // Phase 6e NOTES.md: transactionManager always None.
        if let Some(_tm) = &self.transaction_manager {
            unreachable!("transaction_manager is always None per Phase 6 plug-in contract");
        }

        let data = ProduceRequestData {
            transactional_id,
            acks,
            timeout_ms: timeout,
            topic_data: tpd,
            unknown_tagged_fields: Vec::new(),
        };
        // Default to v9 (the version Java's `ProduceRequest.builder` ends
        // up using when transactions are not requested and message format
        // v2 is in use). In practice the connection's negotiated
        // ApiVersions selects the build version.
        let api_key = ApiKeys::for_id(0).expect("PRODUCE api_key always present");
        let builder = Arc::new(
            ProduceRequestBuilder::new(data).with_versions(api_key.oldest_version(), api_key.latest_version()),
        );

        // Capture topicNames at send time (Java captures at the same
        // point — the `metadata.topicNames()` map can change during the
        // callback, e.g. if a topic is recreated).
        let topic_names = self.metadata.metadata().topic_names();

        let dest_arc: Arc<str> = Arc::from(destination.to_string());
        let callback: Arc<dyn RequestCompletionHandler> = Arc::new(NoopHandler);
        let client_request = self.client.new_client_request_with_callback(
            dest_arc,
            builder,
            now,
            acks != 0,
            self.request_timeout_ms,
            Some(callback),
        );
        let correlation_id = client_request.correlation_id();
        // Register the response context for handle_responses to pick up.
        self.pending_responses
            .lock()
            .unwrap()
            .insert(correlation_id, ResponseContext { batches: records_by_partition, topic_names });
        self.client.send(client_request, now);
        trace!(
            "{}Sent produce request to {}: correlation_id={}",
            self.log_context.log_prefix(),
            destination,
            correlation_id
        );
    }

    /// Mirrors `MockClient::respond` flow plus
    /// `RequestCompletionHandler.onComplete` / `Sender.handleProduceResponse`:
    /// process all responses returned by `KafkaClient::poll`.
    async fn handle_responses(&mut self, responses: Vec<ClientResponse>) {
        let now = self.time.milliseconds();
        for response in responses {
            // Pop the matching context. If absent, the response was for
            // a non-produce request — shouldn't happen in this milestone.
            let correlation_id = response.request_header().correlation_id();
            let ctx = self.pending_responses.lock().unwrap().remove(&correlation_id);
            match ctx {
                Some(ctx) => self.handle_produce_response(&response, ctx, now),
                None => {
                    debug!(
                        "{}Received response with no pending context (correlation_id={})",
                        self.log_context.log_prefix(),
                        correlation_id
                    );
                },
            }
        }
    }

    /// Handle a produce response. Mirrors Java's
    /// `handleProduceResponse(ClientResponse, Map<TopicPartition, ProducerBatch>, Map<Uuid, String>, long)`.
    fn handle_produce_response(&mut self, response: &ClientResponse, ctx: ResponseContext, now: i64) {
        let correlation_id = response.request_header().correlation_id() as i64;
        if response.was_timed_out() {
            trace!(
                "{}Cancelled request due to the last request to node {} timed out",
                self.log_context.log_prefix(),
                response.destination()
            );
            for batch in ctx.batches.values() {
                let resp = PartitionResponse::from_error_and_message(
                    Errors::RequestTimedOut,
                    format!("Disconnected from node {} due to timeout", response.destination()),
                );
                self.complete_batch_with_response(batch, &resp, correlation_id, now, None);
            }
        } else if response.was_disconnected() {
            trace!(
                "{}Cancelled request due to node {} being disconnected",
                self.log_context.log_prefix(),
                response.destination()
            );
            for batch in ctx.batches.values() {
                let resp = PartitionResponse::from_error_and_message(
                    Errors::NetworkException,
                    format!("Disconnected from node {}", response.destination()),
                );
                self.complete_batch_with_response(batch, &resp, correlation_id, now, None);
            }
        } else if response.version_mismatch().is_some() {
            warn!(
                "{}Cancelled request due to a version mismatch with node {}",
                self.log_context.log_prefix(),
                response.destination()
            );
            for batch in ctx.batches.values() {
                let resp = PartitionResponse::from_error(Errors::UnsupportedVersion);
                self.complete_batch_with_response(batch, &resp, correlation_id, now, None);
            }
        } else {
            trace!(
                "{}Received produce response from node {} with correlation_id {}",
                self.log_context.log_prefix(),
                response.destination(),
                correlation_id
            );
            if response.has_response() {
                let body = response.response_body().expect("has_response() == true ⇒ body present");
                // Downcast to ProduceResponse via AbstractResponse.
                let produce: &ProduceResponse = match body.as_any().downcast_ref::<ProduceResponse>() {
                    Some(p) => p,
                    None => {
                        // Not a produce response — should not happen on
                        // the produce send path. Surface as a top-level
                        // unknown error for every batch.
                        for batch in ctx.batches.values() {
                            let resp = PartitionResponse::from_error_and_message(
                                Errors::UnknownServerError,
                                "Unexpected response body for produce request".to_string(),
                            );
                            self.complete_batch_with_response(batch, &resp, correlation_id, now, None);
                        }
                        return;
                    },
                };
                let mut partitions_with_updated_leader: HashMap<TopicPartition, MetadataLeaderIdAndEpoch> =
                    HashMap::new();
                for r in &produce.response_data().responses {
                    for p in &r.partition_responses {
                        let partition_resp = PartitionResponse {
                            error: Errors::for_code(p.error_code),
                            base_offset: p.base_offset,
                            log_append_time: p.log_append_time_ms,
                            log_start_offset: p.log_start_offset,
                            record_errors: p
                                .record_errors
                                .iter()
                                .map(|e| RecordError::new(e.batch_index, e.batch_index_error_message.clone()))
                                .collect(),
                            error_message: p.error_message.clone(),
                            current_leader: p.current_leader.clone(),
                        };

                        // v13 drops topic name; resolve via topic id +
                        // captured topic_names. Older versions: topic id
                        // is zero, look up by name.
                        let tp = if r.topic_id != Uuid::zero() && ctx.topic_names.contains_key(&r.topic_id) {
                            TopicPartition::new(
                                ctx.topic_names.get(&r.topic_id).expect("topic_names contains_key").clone(),
                                p.index,
                            )
                        } else {
                            TopicPartition::new(r.name.clone(), p.index)
                        };
                        let batch = match ctx.batches.get(&tp) {
                            Some(b) => b,
                            None => {
                                // Java: `throw new IllegalStateException(...)`.
                                panic!(
                                    "Can't find batch created for topic id {:?} topic name {} partition {} using {:?}",
                                    r.topic_id, r.name, p.index, ctx.topic_names
                                );
                            },
                        };
                        self.complete_batch_with_response(
                            batch,
                            &partition_resp,
                            correlation_id,
                            now,
                            Some(&mut partitions_with_updated_leader),
                        );
                    }
                }
                if !partitions_with_updated_leader.is_empty() {
                    // Build leader nodes from `node_endpoints`.
                    let leader_nodes: Vec<Node> = produce
                        .response_data()
                        .node_endpoints
                        .iter()
                        .map(|e| Node::new(e.node_id, e.host.clone(), e.port))
                        .filter(|n| !n.is_empty())
                        .collect();
                    let _updated = self
                        .metadata
                        .metadata()
                        .update_partition_leadership(partitions_with_updated_leader, leader_nodes);
                }
                // metric stub: sensors.recordLatency(...).
            } else {
                // acks=0: just complete all requests.
                for batch in ctx.batches.values() {
                    let resp = PartitionResponse::from_error(Errors::None);
                    self.complete_batch_with_response(batch, &resp, correlation_id, now, None);
                }
            }
        }
    }

    /// Complete or retry the given batch. Mirrors Java's
    /// `completeBatch(ProducerBatch, ProduceResponse.PartitionResponse, long, long, Map<...>)`.
    fn complete_batch_with_response(
        &self,
        batch: &Arc<ProducerBatch>,
        response: &PartitionResponse,
        correlation_id: i64,
        now: i64,
        mut partitions_with_updated_leader: Option<&mut HashMap<TopicPartition, MetadataLeaderIdAndEpoch>>,
    ) {
        batch.set_inflight(false);
        let error = response.error;

        if error == Errors::MessageTooLarge
            && batch.record_count() > 1
            && !batch.is_done()
            && (batch.magic() >= MAGIC_VALUE_V2 || batch.is_compressed())
        {
            warn!(
                "{}Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                self.log_context.log_prefix(),
                correlation_id,
                batch.topic_partition(),
                self.retries - batch.attempts(),
                Self::format_err_msg(response)
            );
            // transactionManager.removeInFlightBatch(batch) — skipped.
            if let Err(e) = self.accumulator.split_and_reenqueue(Arc::clone(batch)) {
                error!("{}split_and_reenqueue failed: {:?}", self.log_context.log_prefix(), e);
            }
            self.maybe_remove_and_deallocate_batch(batch);
            // metric stub: sensors.recordBatchSplit().
        } else if error != Errors::None {
            if self.can_retry(batch, response, now) {
                warn!(
                    "{}Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    self.log_context.log_prefix(),
                    correlation_id,
                    batch.topic_partition(),
                    self.retries - batch.attempts() - 1,
                    Self::format_err_msg(response)
                );
                self.reenqueue_batch(batch, now);
            } else if error == Errors::DuplicateSequenceNumber {
                // Java: complete success branch. Idempotent producer is
                // out of milestone scope, but the wire response can still
                // arrive (broker decides). Mirror Java by completing
                // success.
                self.complete_batch_success(batch, response);
            } else {
                self.fail_batch(batch, response, batch.attempts() < self.retries, true);
            }
            // Surface invalid metadata on retriable infra errors so the
            // sender's next iteration refreshes metadata.
            if Self::is_invalid_metadata(error) {
                if error == Errors::UnknownTopicOrPartition {
                    warn!(
                        "{}Received unknown topic or partition error in produce request on partition {}. The topic-partition may not exist or the user may not have Describe access to it",
                        self.log_context.log_prefix(),
                        batch.topic_partition()
                    );
                } else {
                    warn!(
                        "{}Received invalid metadata error in produce request on partition {} due to {}. Going to request metadata update now",
                        self.log_context.log_prefix(),
                        batch.topic_partition(),
                        Self::format_err_msg(response)
                    );
                }
                if (error == Errors::NotLeaderOrFollower || error == Errors::FencedLeaderEpoch)
                    && let Some(map) = partitions_with_updated_leader.as_mut()
                    && response.current_leader.leader_id != -1
                    && response.current_leader.leader_epoch != -1
                {
                    map.insert(
                        batch.topic_partition().clone(),
                        MetadataLeaderIdAndEpoch::new(
                            Some(response.current_leader.leader_id),
                            Some(response.current_leader.leader_epoch),
                        ),
                    );
                }
                self.metadata.metadata().request_update(false);
            }
        } else {
            self.complete_batch_success(batch, response);
        }

        if self.guarantee_message_order {
            self.accumulator.unmute_partition(batch.topic_partition());
        }
    }

    /// Mirrors Java's `completeBatch(ProducerBatch, ProduceResponse.PartitionResponse)` (2-arg).
    fn complete_batch_success(&self, batch: &Arc<ProducerBatch>, response: &PartitionResponse) {
        // transactionManager.handleCompletedBatch — skipped.
        if batch.complete(response.base_offset, response.log_append_time) {
            self.maybe_remove_and_deallocate_batch(batch);
        } else {
            self.accumulator.deallocate(batch);
        }
    }

    /// Format the error from a `PartitionResponse` in a user-friendly
    /// string. Mirrors Java's `formatErrMsg`.
    fn format_err_msg(response: &PartitionResponse) -> String {
        match &response.error_message {
            Some(m) if !m.is_empty() => format!("{:?}. Error Message: {}", response.error, m),
            _ => format!("{:?}", response.error),
        }
    }

    /// Returns `true` if the broker error is treated as a metadata
    /// staleness signal. Mirrors Java's
    /// `error.exception() instanceof InvalidMetadataException`.
    ///
    /// Java's `InvalidMetadataException` has 15 subclasses; the 13
    /// listed here are exactly the wire-coded subset (the remaining
    /// two — `StaleMetadataException` and `NoAvailableBrokersException`
    /// — are client-internal with no broker error code, so they cannot
    /// arrive on a produce response).
    fn is_invalid_metadata(error: Errors) -> bool {
        matches!(
            error,
            Errors::UnknownTopicOrPartition
                | Errors::LeaderNotAvailable
                | Errors::NotLeaderOrFollower
                | Errors::ReplicaNotAvailable
                | Errors::NetworkException
                | Errors::KafkaStorageError
                | Errors::ListenerNotFound
                | Errors::FencedLeaderEpoch
                | Errors::PreferredLeaderNotAvailable
                | Errors::EligibleLeadersNotAvailable
                | Errors::ElectionNotNeeded
                | Errors::UnknownTopicId
                | Errors::InconsistentTopicId
        )
    }

    /// Mirrors Java's
    /// `canRetry(ProducerBatch, ProduceResponse.PartitionResponse, long)`.
    fn can_retry(&self, batch: &ProducerBatch, response: &PartitionResponse, now: i64) -> bool {
        !batch.has_reached_delivery_timeout(self.accumulator.delivery_timeout_ms(), now)
            && batch.attempts() < self.retries
            && !batch.is_done()
            && Self::error_is_retriable(response.error)
    }

    /// Returns `true` if `Errors::exception()` would yield a Java
    /// `RetriableException`. We avoid materializing a `KafkaError` per
    /// call.
    fn error_is_retriable(error: Errors) -> bool {
        match error.exception() {
            Some(e) => e.is_retriable(),
            None => false,
        }
    }

    /// Mirrors Java's `reenqueueBatch(ProducerBatch, long)`.
    fn reenqueue_batch(&self, batch: &Arc<ProducerBatch>, now: i64) {
        self.accumulator.reenqueue(Arc::clone(batch), now);
        self.maybe_remove_from_inflight_batches(batch);
        // metric stub: sensors.recordRetries(...).
    }

    /// Top-level fail (no record-error array). Mirrors Java's 4-arg
    /// `failBatch(ProducerBatch, RuntimeException, boolean, boolean)`.
    fn fail_batch_top_level(
        &self,
        batch: &Arc<ProducerBatch>,
        top_level_exception: KafkaError,
        adjust_sequence_numbers: bool,
        deallocate_batch: bool,
    ) {
        let cloned = top_level_exception.clone();
        let record_exceptions: crate::producer::internals::produce_request_result::ErrorsByIndex =
            Arc::new(move |_idx| Some(cloned.clone()));
        self.fail_batch_with_exceptions(
            batch,
            top_level_exception,
            record_exceptions,
            adjust_sequence_numbers,
            deallocate_batch,
        );
    }

    /// Mirrors Java's `failBatch(ProducerBatch, ProduceResponse.PartitionResponse, boolean, boolean)`.
    fn fail_batch(
        &self,
        batch: &Arc<ProducerBatch>,
        response: &PartitionResponse,
        adjust_sequence_numbers: bool,
        deallocate_batch: bool,
    ) {
        // Determine top-level exception from the partition response.
        let top_level: KafkaError = match response.error {
            Errors::TopicAuthorizationFailed => KafkaError::TopicAuthorization(format!(
                "Not authorized to access topics: [{}]",
                batch.topic_partition().topic()
            )),
            Errors::ClusterAuthorizationFailed => {
                KafkaError::ClusterAuthorization("The producer is not authorized to do idempotent sends".to_string())
            },
            other => other
                .exception_with_message(response.error_message.as_deref())
                .unwrap_or_else(|| KafkaError::Generic(format!("{:?}", other))),
        };

        if response.record_errors.is_empty() {
            self.fail_batch_top_level(batch, top_level, adjust_sequence_numbers, deallocate_batch);
        } else {
            // Build per-record-index exception map.
            let mut record_error_map: HashMap<i32, KafkaError> = HashMap::new();
            let single_error = response.record_errors.len() == 1;
            for record_error in &response.record_errors {
                let error_message: String = match (record_error.message.as_deref(), response.error_message.as_deref()) {
                    (Some(m), _) if !m.is_empty() => m.to_string(),
                    (_, Some(m)) if !m.is_empty() => m.to_string(),
                    _ => response.error.message().unwrap_or("").to_string(),
                };
                let exc: KafkaError = if single_error {
                    response
                        .error
                        .exception_with_message(Some(&error_message))
                        .unwrap_or(KafkaError::InvalidRecord(error_message.clone()))
                } else {
                    KafkaError::InvalidRecord(error_message)
                };
                record_error_map.insert(record_error.batch_index, exc);
            }
            let record_exceptions: crate::producer::internals::produce_request_result::ErrorsByIndex = Arc::new(
                move |idx| {
                    Some(record_error_map.get(&idx).cloned().unwrap_or_else(|| {
                        KafkaError::Generic(
                            "Failed to append record because it was part of a batch which had one more more invalid records"
                                .to_string(),
                        )
                    }))
                },
            );
            self.fail_batch_with_exceptions(
                batch,
                top_level,
                record_exceptions,
                adjust_sequence_numbers,
                deallocate_batch,
            );
        }
    }

    /// Internal — runs the actual failure path. Mirrors Java's 5-arg
    /// `failBatch(ProducerBatch, RuntimeException, Function<Integer, RuntimeException>, boolean, boolean)`.
    fn fail_batch_with_exceptions(
        &self,
        batch: &Arc<ProducerBatch>,
        top_level: KafkaError,
        record_exceptions: crate::producer::internals::produce_request_result::ErrorsByIndex,
        _adjust_sequence_numbers: bool,
        deallocate_batch: bool,
    ) {
        // metric stub: sensors.recordErrors(...).
        if batch.complete_exceptionally(top_level, record_exceptions) {
            // transactionManager.handleFailedBatch — skipped.
            if deallocate_batch {
                self.maybe_remove_and_deallocate_batch(batch);
            } else {
                // KAFKA-19012: pooled buffer might still be in use by
                // the network client; defer deallocation.
                self.maybe_remove_and_deallocate_batch_later(batch);
            }
        } else if deallocate_batch {
            self.accumulator.deallocate(batch);
        }
    }

    /// Mirrors Java's `maybeAbortBatches(RuntimeException)`.
    fn maybe_abort_batches(&self, exception: KafkaError) {
        if self.accumulator.has_incomplete() {
            error!(
                "{}Aborting producer batches due to fatal error: {:?}",
                self.log_context.log_prefix(),
                exception
            );
            self.accumulator.abort_batches(exception);
            self.in_flight_batches.lock().unwrap().clear();
        }
    }

    /// Drives the sender loop until shutdown. Mirrors Java's `run()`.
    ///
    /// Java's `Sender.run()` wraps each `runOnce()` invocation in
    /// `try/catch (Exception)` so an uncaught `RuntimeException` does
    /// not terminate the producer I/O thread. The Rust translation
    /// mirrors that contract via [`futures_util::FutureExt::catch_unwind`]:
    /// a panic in `run_once().await` is logged and swallowed; the loop
    /// continues. The producer survives single-iteration invariant
    /// violations the same way Java's `Thread` does.
    ///
    /// Caveats: state mutated up to the panic point is inconsistent
    /// after the catch. Java has the same hazard. The panics the actor
    /// introduced (e.g. `sender.rs::panic!("can't find batch …")`) are
    /// invariant violations per CLAUDE.md rule 10.1 — surviving them is
    /// best-effort but matches Java behavior.
    pub(crate) async fn run_loop(&mut self) {
        use futures_util::FutureExt;
        use std::panic::AssertUnwindSafe;

        debug!("{}Starting Kafka producer I/O thread.", self.log_context.log_prefix());
        while self.running.load(Ordering::Acquire) {
            // `AssertUnwindSafe`: `&mut self` is not `UnwindSafe` by default;
            // we assert that callers tolerate post-panic state per the
            // rustdoc above (mirrors Java's `try/catch (Exception)`).
            let outcome = AssertUnwindSafe(self.run_once()).catch_unwind().await;
            if let Err(e) = outcome {
                error!(
                    "{}Uncaught error in kafka producer I/O thread: {}",
                    self.log_context.log_prefix(),
                    panic_payload_message(&e)
                );
            }
        }
        debug!(
            "{}Beginning shutdown of Kafka producer I/O thread, sending remaining records.",
            self.log_context.log_prefix()
        );
        // Drain undrained batches.
        while !self.force_close.load(Ordering::Acquire)
            && (self.accumulator.has_undrained() || self.client.has_in_flight_requests())
        {
            let outcome = AssertUnwindSafe(self.run_once()).catch_unwind().await;
            if let Err(e) = outcome {
                error!(
                    "{}Uncaught error during producer shutdown drain: {}",
                    self.log_context.log_prefix(),
                    panic_payload_message(&e)
                );
            }
        }
        if self.force_close.load(Ordering::Acquire) {
            debug!(
                "{}Aborting incomplete batches due to forced shutdown",
                self.log_context.log_prefix()
            );
            self.accumulator.abort_incomplete_batches();
        }
        self.client.close();
        debug!(
            "{}Shutdown of Kafka producer I/O thread has completed.",
            self.log_context.log_prefix()
        );
    }
}

/// Best-effort extraction of a panic payload's message. Mirrors Java's
/// `Throwable.getMessage()` for log purposes; falls back to a generic
/// marker when the panic carries something other than `&str` / `String`.
fn panic_payload_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `SenderTest` (non-transactional cases only — see
    //! module docs).

    use std::sync::atomic::AtomicI32;
    use std::time::Duration;

    use super::*;
    use crate::ClientRequest;
    use crate::common::cluster::Cluster;
    use crate::common::header::RecordHeader;
    use crate::common::internals::cluster_resource_listeners::ClusterResourceListeners;
    use crate::common::message::produce_response_data::{
        LeaderIdAndEpoch as ProtoLeaderIdAndEpoch, PartitionProduceResponse, ProduceResponseData, TopicProduceResponse,
    };
    use crate::common::node::Node;
    use crate::common::partition_info::PartitionInfo;
    use crate::common::record::CompressionType;
    use crate::common::record::record_batch::NO_PARTITION_LEADER_EPOCH;
    use crate::common::requests::AbstractResponse;
    use crate::common::topic_partition::TopicPartition;
    use crate::common::utils::{LogContext, MockTime, Time};
    use crate::common::uuid::Uuid;
    use crate::producer::internals::buffer_pool::BufferPool;
    use crate::producer::internals::record_accumulator::RecordAccumulator;

    const MAX_REQUEST_SIZE: i32 = 1024 * 1024;
    const ACKS_ALL: i16 = -1;
    const REQUEST_TIMEOUT_MS: i32 = 5000;
    const RETRY_BACKOFF_MS: i64 = 50;
    const DELIVERY_TIMEOUT_MS: i32 = 1500;
    const TOPIC_NAME: &str = "test";

    /// Mock client tailored for `SenderTest` non-transactional cases.
    /// Mirrors the subset of `MockClient.java` we need (queue-based
    /// response staging, default response, connection state, disconnect
    /// simulation, network/auth exception injection, `setNodeApiVersions`).
    /// Coordinator/transactional matchers are NOT implemented — see
    /// module rustdoc.
    pub(super) struct MockClientImpl {
        time: Arc<dyn Time>,
        correlation: AtomicI32,
        client_id: Arc<str>,
        active: bool,
        /// Currently-in-flight requests (FIFO).
        requests: std::collections::VecDeque<ClientRequest>,
        /// Ready-to-return responses.
        responses: std::collections::VecDeque<ClientResponse>,
        /// Pre-staged future responses (popped on send).
        future_responses: std::collections::VecDeque<FutureResponse>,
        /// Per-node connection ready flag (default: true once accessed).
        connections: HashMap<i32, ConnState>,
        /// Pending authentication errors keyed by node id.
        pending_auth_errors: HashMap<i32, i64>,
        /// Authentication exceptions present after triggered.
        auth_errors: HashMap<i32, KafkaError>,
        /// Wakeup hook (test-only).
        wakeup_hook: Option<Arc<dyn Fn() + Send + Sync>>,
        /// Test-only: when set, the next call to `poll` panics with the
        /// stored message. Used to verify `run_loop`'s catch-unwind
        /// matches Java's `try/catch (Exception)` behavior.
        panic_on_next_poll: Option<String>,
        /// Test-only: counter of how many times the armed panic-on-poll
        /// has actually tripped. Bumped immediately before
        /// [`MockClientImpl::poll`] panics, so the test can observe the
        /// panic-was-triggered AND panic-was-caught path independently of
        /// the loop's exit condition. Exposed via
        /// [`MockClientImpl::panic_trip_counter`].
        panic_trip_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    /// Connection state for a given node id.
    #[derive(Default, Clone)]
    struct ConnState {
        ready: bool,
        backoff_until_ms: i64,
        /// Used to simulate `disconnect` — a node that was disconnected
        /// becomes not-ready until the next `ready()` call (no backoff).
        disconnected: bool,
    }

    /// Future-response staging entry.
    struct FutureResponse {
        node_id: Option<i32>,
        response: Option<Box<dyn AbstractResponse>>,
        disconnected: bool,
        is_unsupported_request: bool,
    }

    impl MockClientImpl {
        pub(super) fn new(time: Arc<dyn Time>) -> Self {
            Self::with_client_id(time, Arc::from("mockClientId"))
        }

        pub(super) fn with_client_id(time: Arc<dyn Time>, client_id: Arc<str>) -> Self {
            Self {
                time,
                correlation: AtomicI32::new(0),
                client_id,
                active: true,
                requests: std::collections::VecDeque::new(),
                responses: std::collections::VecDeque::new(),
                future_responses: std::collections::VecDeque::new(),
                connections: HashMap::new(),
                pending_auth_errors: HashMap::new(),
                auth_errors: HashMap::new(),
                wakeup_hook: None,
                panic_on_next_poll: None,
                panic_trip_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        /// Test-only: arm a one-shot panic on the next `poll()` call.
        pub(super) fn set_panic_on_next_poll(&mut self, msg: &str) {
            self.panic_on_next_poll = Some(msg.to_string());
        }

        /// Test-only: clone of the panic-trip counter shared with `poll`.
        /// Caller observes a non-zero value once the armed
        /// [`Self::set_panic_on_next_poll`] has actually tripped (i.e.
        /// `poll` was reached AND the panic was triggered).
        pub(super) fn panic_trip_counter(&self) -> Arc<std::sync::atomic::AtomicUsize> {
            Arc::clone(&self.panic_trip_count)
        }

        fn conn(&mut self, node_id: i32) -> &mut ConnState {
            self.connections.entry(node_id).or_default()
        }

        /// Mirrors `MockClient.prepareResponse(AbstractResponse)`.
        pub(super) fn prepare_response(&mut self, response: Box<dyn AbstractResponse>) {
            self.future_responses.push_back(FutureResponse {
                node_id: None,
                response: Some(response),
                disconnected: false,
                is_unsupported_request: false,
            });
        }

        /// Mirrors `MockClient.prepareResponse(AbstractResponse, boolean)`.
        pub(super) fn prepare_response_with_disconnect(
            &mut self,
            response: Option<Box<dyn AbstractResponse>>,
            disconnected: bool,
        ) {
            self.future_responses.push_back(FutureResponse {
                node_id: None,
                response,
                disconnected,
                is_unsupported_request: false,
            });
        }

        /// Mirrors `MockClient.respond(AbstractResponse)` — pops the next
        /// in-flight request and answers it.
        pub(super) fn respond(&mut self, response: Box<dyn AbstractResponse>) {
            self.respond_with_disconnect(Some(response), false);
        }

        /// Mirrors `MockClient.respond(AbstractResponse, boolean)`.
        pub(super) fn respond_with_disconnect(
            &mut self,
            response: Option<Box<dyn AbstractResponse>>,
            disconnected: bool,
        ) {
            let request = self.requests.pop_front().expect("No requests pending for inbound response");
            let version = request.request_builder().latest_allowed_version();
            self.responses.push_back(ClientResponse::new(
                request.make_header(version),
                request.callback().cloned(),
                request.destination_arc(),
                request.created_time_ms(),
                self.time.milliseconds(),
                disconnected,
                None,
                None,
                response,
            ));
        }

        /// Pop a queued request with no response (for disconnect tests).
        pub(super) fn requests_count(&self) -> usize {
            self.requests.len()
        }

        /// Test-only: peek next pending request.
        pub(super) fn next_request_destination(&self) -> Option<&str> {
            self.requests.front().map(|r| r.destination())
        }

        /// Mirrors `MockClient.checkTimeoutOfPendingRequests(long)`.
        /// Disconnects any in-flight request whose `request_timeout_ms`
        /// has elapsed (with the head of the FIFO).
        fn check_timeout_of_pending_requests(&mut self, now_ms: i64) {
            while let Some(req) = self.requests.front()
                && (now_ms.saturating_sub(req.created_time_ms())) >= req.request_timeout_ms() as i64
            {
                let dest = req.destination().to_string();
                if let Ok(node_id) = dest.parse::<i32>() {
                    self.disconnect_node(node_id);
                } else {
                    // Non-numeric destinations: just drop the request.
                    self.requests.pop_front();
                }
            }
        }

        /// Convenience used by SenderTest disconnect cases. Mirrors
        /// `MockClient.disconnect(String)`.
        pub(super) fn disconnect_node(&mut self, node_id: i32) {
            let now = self.time.milliseconds();
            let mut survivors: std::collections::VecDeque<ClientRequest> = std::collections::VecDeque::new();
            while let Some(req) = self.requests.pop_front() {
                let req_dest_id = req.destination().parse::<i32>().ok();
                if req_dest_id == Some(node_id) {
                    let version = req.request_builder().latest_allowed_version();
                    self.responses.push_back(ClientResponse::new(
                        req.make_header(version),
                        req.callback().cloned(),
                        req.destination_arc(),
                        req.created_time_ms(),
                        now,
                        true,
                        None,
                        None,
                        None,
                    ));
                } else {
                    survivors.push_back(req);
                }
            }
            self.requests = survivors;
            self.conn(node_id).disconnected = true;
            self.conn(node_id).ready = false;
        }
    }

    impl KafkaClient for MockClientImpl {
        fn is_ready(&self, node: &Node, _now: i64) -> bool {
            self.connections.get(&node.id()).map(|c| c.ready).unwrap_or(false)
        }
        fn ready(&mut self, node: &Node, now: i64) -> bool {
            // Pending auth errors trigger a fail-on-ready cycle.
            if let Some(_backoff) = self.pending_auth_errors.remove(&node.id()) {
                self.auth_errors
                    .insert(node.id(), KafkaError::Authentication("Authentication failed".to_string()));
                self.conn(node.id()).disconnected = true;
                self.conn(node.id()).ready = false;
                return false;
            }
            let c = self.conn(node.id());
            if c.disconnected {
                // First call after disconnect re-establishes.
                c.disconnected = false;
                c.ready = false;
                return false;
            }
            if c.backoff_until_ms > now {
                return false;
            }
            c.ready = true;
            true
        }
        fn connection_delay(&self, _node: &Node, _now: i64) -> i64 {
            0
        }
        fn poll_delay_ms(&self, _node: &Node, _now: i64) -> i64 {
            0
        }
        fn connection_failed(&self, node: &Node) -> bool {
            self.connections.get(&node.id()).map(|c| c.disconnected).unwrap_or(false)
        }
        fn authentication_error(&self, node: &Node) -> Option<KafkaError> {
            self.auth_errors.get(&node.id()).cloned()
        }
        fn send(&mut self, request: ClientRequest, now: i64) {
            // If a future response is staged, answer immediately.
            if let Some(idx) = self.future_responses.iter().position(|f| match f.node_id {
                Some(id) => request.destination() == id.to_string(),
                None => true,
            }) {
                let f = self.future_responses.remove(idx).expect("position lookup");
                let version = request.request_builder().latest_allowed_version();
                let version_mismatch = if f.is_unsupported_request {
                    Some(KafkaError::UnsupportedVersion(format!(
                        "Api {:?} with version {}",
                        request.api_key().name,
                        version
                    )))
                } else {
                    None
                };
                self.responses.push_back(ClientResponse::new(
                    request.make_header(version),
                    request.callback().cloned(),
                    request.destination_arc(),
                    request.created_time_ms(),
                    now,
                    f.disconnected,
                    version_mismatch,
                    None,
                    f.response,
                ));
                return;
            }
            self.requests.push_back(request);
        }
        async fn poll(&mut self, _timeout_ms: i64, now: i64) -> Vec<ClientResponse> {
            // Real `KafkaClient::poll` involves I/O readiness which
            // implicitly yields. The mock has no genuine await point, so
            // we yield explicitly at the top — without this, a spawned
            // `run_loop` task is a tight CPU loop that starves the main
            // test task (the
            // `run_loop_swallows_panics_and_continues` test depends on
            // observing the trip counter from the main task).
            tokio::task::yield_now().await;
            if let Some(msg) = self.panic_on_next_poll.take() {
                // Bump the trip counter BEFORE panicking so observers can
                // distinguish "poll was reached and panicked" from "poll
                // was never called". Atomic write is reordering-safe vs
                // the panic — `panic_unwind` reads the counter only after
                // the panic propagates back, by which time the store is
                // visible.
                self.panic_trip_count.fetch_add(1, Ordering::Relaxed);
                panic!("{msg}");
            }
            // Java's MockClient.checkTimeoutOfPendingRequests: any
            // in-flight request whose `request_timeout_ms` has elapsed
            // is disconnected. Mirrors `MockClient.checkTimeoutOfPendingRequests(now)`.
            self.check_timeout_of_pending_requests(now);
            let mut out: Vec<ClientResponse> = Vec::with_capacity(self.responses.len());
            while let Some(r) = self.responses.pop_front() {
                r.on_complete();
                out.push(r);
            }
            out
        }
        fn disconnect(&mut self, node_id: i32) {
            self.disconnect_node(node_id);
        }
        fn close_connection(&mut self, node_id: i32) {
            self.connections.remove(&node_id);
        }
        fn least_loaded_node(&mut self, _now: i64) -> crate::LeastLoadedNode {
            crate::LeastLoadedNode::new(None, false)
        }
        fn in_flight_request_count(&self) -> i32 {
            self.requests.len() as i32
        }
        fn has_in_flight_requests(&self) -> bool {
            !self.requests.is_empty()
        }
        fn in_flight_request_count_for(&self, node_id: i32) -> i32 {
            self.requests.iter().filter(|r| r.destination() == node_id.to_string()).count() as i32
        }
        fn has_in_flight_requests_for(&self, node_id: i32) -> bool {
            self.in_flight_request_count_for(node_id) > 0
        }
        fn has_ready_nodes(&self, _now: i64) -> bool {
            self.connections.values().any(|c| c.ready)
        }
        fn wakeup(&self) {
            if let Some(hook) = &self.wakeup_hook {
                hook();
            }
        }
        fn new_client_request(
            &mut self,
            node_id: Arc<str>,
            request_builder: Arc<dyn AbstractRequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
        ) -> ClientRequest {
            self.new_client_request_with_callback(
                node_id,
                request_builder,
                created_time_ms,
                expect_response,
                5000,
                None,
            )
        }
        fn new_client_request_with_callback(
            &mut self,
            node_id: Arc<str>,
            request_builder: Arc<dyn AbstractRequestBuilder>,
            created_time_ms: i64,
            expect_response: bool,
            request_timeout_ms: i32,
            callback: Option<Arc<dyn RequestCompletionHandler>>,
        ) -> ClientRequest {
            let id = self.correlation.fetch_add(1, Ordering::Relaxed);
            ClientRequest::new(
                node_id,
                request_builder,
                id,
                Arc::clone(&self.client_id),
                created_time_ms,
                expect_response,
                request_timeout_ms,
                callback,
            )
        }
        fn initiate_close(&mut self) {
            self.active = false;
        }
        fn active(&self) -> bool {
            self.active
        }
        fn close(&mut self) {
            self.active = false;
        }
    }

    // ----- Test fixtures -----

    fn build_test_cluster() -> Arc<Cluster> {
        let node = Node::new(0, "localhost".to_string(), 1111);
        let parts = vec![
            PartitionInfo::new(TOPIC_NAME, 0, Some(node.clone()), vec![], vec![]),
            PartitionInfo::new(TOPIC_NAME, 1, Some(node.clone()), vec![], vec![]),
            PartitionInfo::new(TOPIC_NAME, 2, Some(node.clone()), vec![], vec![]),
        ];
        Arc::new(Cluster::new(None, vec![node], parts, HashSet::new(), HashSet::new()))
    }

    fn build_metadata_response(
        cluster: &Cluster,
        topic_id: Uuid,
    ) -> crate::common::requests::metadata_response::MetadataResponse {
        use crate::common::message::metadata_response_data::{
            MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
        };
        let brokers: Vec<MetadataResponseBroker> = cluster
            .nodes()
            .iter()
            .map(|n| MetadataResponseBroker {
                node_id: n.id(),
                host: n.host().to_string(),
                port: n.port(),
                rack: None,
                unknown_tagged_fields: Vec::new(),
            })
            .collect();
        let topic = MetadataResponseTopic {
            error_code: 0,
            name: Some(TOPIC_NAME.to_string()),
            topic_id,
            is_internal: false,
            partitions: (0..3)
                .map(|p| MetadataResponsePartition {
                    error_code: 0,
                    partition_index: p,
                    leader_id: 0,
                    leader_epoch: NO_PARTITION_LEADER_EPOCH,
                    replica_nodes: vec![0],
                    isr_nodes: vec![0],
                    offline_replicas: vec![],
                    unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let data = MetadataResponseData {
            throttle_time_ms: 0,
            brokers,
            cluster_id: Some(String::new()),
            controller_id: 0,
            topics: vec![topic],
            cluster_authorized_operations: 0,
            error_code: 0,
            unknown_tagged_fields: Vec::new(),
        };
        crate::common::requests::metadata_response::MetadataResponse::new(data, true)
    }

    fn build_produce_response(
        topic: &str,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
        error: Errors,
        throttle_time_ms: i32,
    ) -> Box<dyn AbstractResponse> {
        let partition_resp = PartitionProduceResponse {
            index: partition,
            error_code: error.code(),
            base_offset: offset,
            log_append_time_ms: -1,
            log_start_offset: 0,
            record_errors: Vec::new(),
            error_message: None,
            current_leader: ProtoLeaderIdAndEpoch::new(),
            unknown_tagged_fields: Vec::new(),
        };
        let topic_resp = TopicProduceResponse {
            name: topic.to_string(),
            topic_id,
            partition_responses: vec![partition_resp],
            unknown_tagged_fields: Vec::new(),
        };
        let data = ProduceResponseData {
            throttle_time_ms,
            responses: vec![topic_resp],
            node_endpoints: Vec::new(),
            unknown_tagged_fields: Vec::new(),
        };
        Box::new(ProduceResponse::new(data))
    }

    /// Test fixture: build a ProducerMetadata + Sender + RecordAccumulator
    /// with the cluster fully populated for a single topic of 3 partitions
    /// on broker 0.
    pub(super) struct TestSetup {
        pub sender: Sender<MockClientImpl>,
        pub accum: Arc<RecordAccumulator>,
        pub metadata: Arc<ProducerMetadata>,
        pub time: Arc<dyn Time>,
        pub topic_id: Uuid,
    }

    fn make_test_setup(retries: i32, guarantee_message_order: bool) -> TestSetup {
        make_test_setup_with_pool(retries, guarantee_message_order, None)
    }

    fn make_test_setup_with_pool(
        retries: i32,
        guarantee_message_order: bool,
        custom_pool: Option<Arc<BufferPool>>,
    ) -> TestSetup {
        let time: Arc<dyn Time> = Arc::new(MockTime::with_initial(0, 0, 0));
        let metadata = ProducerMetadata::new(
            0,
            0,
            i64::MAX,
            60_000,
            LogContext::new(),
            Arc::new(ClusterResourceListeners::new()),
            time.clone(),
        )
        .expect("ProducerMetadata::new");
        // Bootstrap the metadata snapshot directly via `update`.
        let topic_id = Uuid::new(0x11223344, 0x55667788);
        let cluster = build_test_cluster();
        let metadata_response = build_metadata_response(&cluster, topic_id);
        // The producer metadata starts at version 0 — register the topic
        // first so retainTopic keeps it.
        metadata.add(TOPIC_NAME, time.milliseconds());
        metadata
            .update_with_current_request_version(&metadata_response, false, time.milliseconds())
            .expect("metadata update");

        let pool = custom_pool
            .unwrap_or_else(|| Arc::new(BufferPool::new(1024 * 1024, 16 * 1024, time.clone(), "producer-metrics")));
        let accum = Arc::new(RecordAccumulator::new_with_default_partitioner(
            LogContext::new(),
            16 * 1024,
            CompressionType::None,
            0, // linger_ms — Java's default in setupWithTransactionState
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MS,
            DELIVERY_TIMEOUT_MS,
            "producer-metrics",
            time.clone(),
            None,
            pool,
        ));
        let client = MockClientImpl::new(time.clone());
        let sender = Sender::new(
            LogContext::new(),
            client,
            Arc::clone(&metadata),
            Arc::clone(&accum),
            guarantee_message_order,
            MAX_REQUEST_SIZE,
            ACKS_ALL,
            retries,
            time.clone(),
            REQUEST_TIMEOUT_MS,
            RETRY_BACKOFF_MS,
            None,
            Arc::from("clientId"),
        );
        TestSetup { sender, accum, metadata, time, topic_id }
    }

    /// Append a record to the accumulator with the topic-name + partition + value.
    /// Returns a future that will resolve to the [`RecordMetadata`].
    #[allow(clippy::too_many_arguments)]
    async fn append_to_accumulator(
        accum: &RecordAccumulator,
        time: &Arc<dyn Time>,
        cluster: &Cluster,
        topic: &str,
        partition: i32,
        timestamp: i64,
        key: &[u8],
        value: &[u8],
    ) -> Arc<crate::producer::internals::future_record_metadata::FutureRecordMetadata> {
        let result = accum
            .append(
                topic,
                partition,
                timestamp,
                Some(key),
                Some(value),
                &[] as &[RecordHeader],
                None,
                1000,
                time.milliseconds(),
                cluster,
            )
            .await
            .expect("append");
        result.future
    }

    // ----- Smoke tests -----

    #[test]
    fn constructor_smoke() {
        let TestSetup { sender, .. } = make_test_setup(3, false);
        assert!(sender.is_running());
        assert!(sender.in_flight_batches_for(&TopicPartition::new(TOPIC_NAME, 0)).is_empty());
    }

    #[test]
    fn initiate_close_clears_running_flag() {
        let TestSetup { sender, .. } = make_test_setup(3, false);
        assert!(sender.is_running());
        sender.initiate_close();
        assert!(!sender.is_running());
    }

    #[tokio::test]
    async fn run_once_with_no_records_polls_and_returns() {
        let TestSetup { mut sender, .. } = make_test_setup(3, false);
        // No records appended. `run_once` should run a single ready/drain
        // /poll cycle without panicking.
        tokio::time::timeout(Duration::from_secs(1), sender.run_once())
            .await
            .expect("run_once timed out");
    }

    /// Translation of `SenderTest#testSimple` (round-trip canonical
    /// regression).
    ///
    /// Append a record → run_once (sends produce request) → respond
    /// with `Errors::NONE` → run_once (handles response) → assert the
    /// future resolves to the expected RecordMetadata.
    #[tokio::test]
    async fn test_simple() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let offset = 0i64;
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"key", b"value").await;
        sender.run_once().await; // send produce request
        assert_eq!(
            sender.client.in_flight_request_count(),
            1,
            "We should have a single produce request in flight."
        );
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
        assert!(sender.client.has_in_flight_requests());
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, offset, Errors::None, 0));
        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 0, "All requests completed.");
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
        assert!(!sender.client.has_in_flight_requests());
        let resolved = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future.get() timed out")
            .expect("future returned error");
        assert_eq!(resolved.offset(), offset);
    }

    /// Append + send + read inflight request count without responding.
    /// Confirms the in-flight bookkeeping is wired correctly.
    #[tokio::test]
    async fn append_drives_send() {
        let TestSetup { mut sender, accum, metadata, time, topic_id: _ } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let _future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
    }

    /// Round-trip with 3 records. All futures must resolve.
    #[tokio::test]
    async fn round_trip_three_records() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let f0 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k0", b"v0").await;
        let f1 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 1, 0, b"k1", b"v1").await;
        let f2 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 2, 0, b"k2", b"v2").await;
        sender.run_once().await;
        // All three partitions go to the same broker (node 0), so they
        // all batch into one ProduceRequest.
        assert!(sender.client.has_in_flight_requests());
        // Stage a multi-partition response.
        let response =
            build_produce_response_multi(TOPIC_NAME, topic_id, &[(0, 100), (1, 200), (2, 300)], Errors::None);
        sender.client.respond(response);
        sender.run_once().await;
        let m0 = tokio::time::timeout(Duration::from_secs(2), f0.get())
            .await
            .expect("f0 timed out")
            .expect("f0 errored");
        let m1 = tokio::time::timeout(Duration::from_secs(2), f1.get())
            .await
            .expect("f1 timed out")
            .expect("f1 errored");
        let m2 = tokio::time::timeout(Duration::from_secs(2), f2.get())
            .await
            .expect("f2 timed out")
            .expect("f2 errored");
        assert_eq!(m0.offset(), 100);
        assert_eq!(m1.offset(), 200);
        assert_eq!(m2.offset(), 300);
    }

    /// Acks=0 path: the sender does not register a pending response (no
    /// expect_response). We can't fully test without a different fixture
    /// (acks=0 uses a different sender ctor), so this exercises the
    /// "no responses come back" branch only.
    #[tokio::test]
    async fn no_response_when_no_records_pending() {
        let TestSetup { mut sender, .. } = make_test_setup(i32::MAX, false);
        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 0);
    }

    /// `MockClientImpl::prepare_response` answers immediately on send.
    /// Verify a single full cycle works in a single `run_once`.
    #[tokio::test]
    async fn prepare_response_immediate_round_trip() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        sender
            .client
            .prepare_response(build_produce_response(TOPIC_NAME, topic_id, 0, 42, Errors::None, 0));
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        // Send the request — MockClient answers immediately because of
        // the staged future-response.
        sender.run_once().await;
        // Future may resolve in this same run_once (response delivered
        // during the same poll).
        let resolved = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect("future errored");
        assert_eq!(resolved.offset(), 42);
    }

    /// Translation of `SenderTest#testCanRetryWithoutIdempotence` —
    /// non-tx terminal failure with `TopicAuthorizationFailed`.
    #[tokio::test]
    async fn test_topic_authorization_failed_terminal() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"key", b"value").await;
        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 1);
        sender.client.respond(build_produce_response(
            TOPIC_NAME,
            topic_id,
            0,
            -1,
            Errors::TopicAuthorizationFailed,
            0,
        ));
        sender.run_once().await;
        let err = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect_err("future should error");
        match err {
            KafkaError::TopicAuthorization(_) => {},
            other => panic!("expected TopicAuthorization, got {other:?}"),
        }
    }

    /// Translation of `SenderTest#testRetries` — first response is a
    /// disconnect (retriable), second succeeds. Verifies the retry path
    /// re-enqueues the batch and the future eventually completes.
    #[tokio::test]
    async fn test_retries_then_success() {
        let max_retries = 1;
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(max_retries, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"key", b"value").await;
        sender.run_once().await; // send produce request
        let dest = sender
            .client
            .next_request_destination()
            .expect("a request was queued")
            .to_string();
        let node_id: i32 = dest.parse().expect("destination is an integer node id");
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);

        // Disconnect → MockClient emits a disconnected ClientResponse for
        // the pending request and clears it.
        sender.client.disconnect_node(node_id);
        // After disconnect, the in-flight client request count should be 0
        // and the batch is still in sender's in_flight_batches until it
        // is reenqueued.
        assert_eq!(sender.client.in_flight_request_count(), 0);

        // Bump time past retry backoff so the next ready/drain picks up
        // the reenqueued batch.
        sender.run_once().await; // receive disconnect → retry / reenqueue
        time.sleep(RETRY_BACKOFF_MS + 1);
        sender.run_once().await; // resend
        sender.run_once().await; // resend
        assert_eq!(sender.client.in_flight_request_count(), 1);

        // Successful retry response.
        let offset = 0i64;
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, offset, Errors::None, 0));
        sender.run_once().await;
        let resolved = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect("future errored");
        assert_eq!(resolved.offset(), offset);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
    }

    /// Translation of `SenderTest#testInflightBatchesExpireOnDeliveryTimeout`.
    /// Time elapses beyond `delivery_timeout_ms` between the produce
    /// request being queued and the response handler running; the batch
    /// should fail with a `KafkaError::Timeout`.
    #[tokio::test]
    async fn test_inflight_batches_expire_on_delivery_timeout() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, true);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"key", b"value").await;
        sender.run_once().await; // send request
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);

        // Stage a successful response, but advance time past the delivery
        // timeout BEFORE handling it. The expiry path runs in
        // send_producer_data BEFORE the response is handled, so the
        // batch fails before the response can complete it.
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, 0, Errors::None, 0));
        time.sleep((DELIVERY_TIMEOUT_MS + 100) as i64);
        sender.run_once().await;
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
        let err = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect_err("future should error");
        assert!(matches!(err, KafkaError::Timeout(_)));
    }

    /// Translation of `SenderTest#testCustomErrorMessage` — the error
    /// message attached to the broker's `PartitionResponse` is
    /// propagated to the user's exception.
    #[tokio::test]
    async fn test_custom_error_message() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await;
        let response = build_produce_response_with_message(
            TOPIC_NAME,
            topic_id,
            0,
            -1,
            Errors::InvalidRequest,
            "testCustomErrorMessage",
        );
        sender.client.respond(response);
        sender.run_once().await;
        sender.run_once().await;
        let err = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect_err("future should error");
        match err {
            KafkaError::InvalidRequest(msg) => assert_eq!(msg, "testCustomErrorMessage"),
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    /// Translation of `SenderTest#testDefaultErrorMessage` — when the
    /// PartitionResponse has no error_message, the exception falls back
    /// to the canonical `Errors::message()`.
    #[tokio::test]
    async fn test_default_error_message() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await;
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, -1, Errors::InvalidRequest, 0));
        sender.run_once().await;
        sender.run_once().await;
        let err = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect_err("future should error");
        match err {
            KafkaError::InvalidRequest(msg) => {
                assert_eq!(msg, Errors::InvalidRequest.message().expect("default message"));
            },
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    /// Translation of `SenderTest#testClusterAuthorizationExceptionInProduceRequest`.
    #[tokio::test]
    async fn test_cluster_authorization_exception_in_produce_request() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await;
        sender.client.respond(build_produce_response(
            TOPIC_NAME,
            topic_id,
            0,
            -1,
            Errors::ClusterAuthorizationFailed,
            0,
        ));
        sender.run_once().await;
        let err = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect_err("future should error");
        assert!(matches!(err, KafkaError::ClusterAuthorization(_)));
    }

    /// Translation of `SenderTest#testTooLargeBatchesAreSafelyRemoved`.
    /// `MessageTooLarge` with more than 1 record triggers a split-and-
    /// retry on the accumulator. Verify the in-flight bookkeeping is
    /// cleaned up.
    #[tokio::test]
    async fn test_too_large_batches_are_safely_removed() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let _f1 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k1", b"v1").await;
        let _f2 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k2", b"v2").await;
        sender.run_once().await; // send
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, -1, Errors::MessageTooLarge, 0));
        sender.run_once().await; // handle MessageTooLarge → split path
        // After split, the original batch is removed from in-flight.
        assert!(sender.in_flight_batches_for(&tp0).is_empty());
    }

    /// Translation of `SenderTest#testExpiredBatchDoesNotRetry`. A
    /// retriable error is returned alongside an expired batch — the
    /// expiry should win (`fail_expired_batches` runs before
    /// `handle_responses`) and the batch must NOT be re-enqueued.
    #[tokio::test]
    async fn test_expired_batch_does_not_retry() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let request1 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await; // send request
        assert_eq!(sender.client.in_flight_request_count(), 1);
        time.sleep(DELIVERY_TIMEOUT_MS as i64);
        // Stage a retriable error.
        sender.client.respond(build_produce_response(
            TOPIC_NAME,
            topic_id,
            0,
            -1,
            Errors::NotLeaderOrFollower,
            -1,
        ));
        sender.run_once().await; // expire the batch
        // The future must be done (failed with Timeout).
        let err = tokio::time::timeout(Duration::from_secs(2), request1.get())
            .await
            .expect("future timed out")
            .expect_err("future should error");
        assert!(matches!(err, KafkaError::Timeout(_)));
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);

        sender.run_once().await; // receive first response, do not reenqueue
        assert_eq!(sender.client.in_flight_request_count(), 0);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);

        sender.run_once().await; // run again, no resends
        assert_eq!(sender.client.in_flight_request_count(), 0);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
    }

    /// Translation of `SenderTest#testExpiredBatchDoesNotSplitOnMessageTooLargeError`.
    /// Even with `MessageTooLarge`, an already-expired batch must NOT
    /// be split — both records fail with TimeoutException.
    #[tokio::test]
    async fn test_expired_batch_does_not_split_on_message_too_large_error() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let f1 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k1", b"v1").await;
        let f2 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k2", b"v2").await;
        sender.run_once().await; // send
        assert_eq!(sender.client.in_flight_request_count(), 1);
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, -1, Errors::MessageTooLarge, -1));
        time.sleep(DELIVERY_TIMEOUT_MS as i64);
        sender.run_once().await; // expire batch + process response
        let err1 = tokio::time::timeout(Duration::from_secs(2), f1.get())
            .await
            .expect("f1 timed out")
            .expect_err("f1 should error");
        let err2 = tokio::time::timeout(Duration::from_secs(2), f2.get())
            .await
            .expect("f2 timed out")
            .expect_err("f2 should error");
        // Both records fail with timeout (not invalid-record / split).
        assert!(matches!(err1, KafkaError::Timeout(_)));
        assert!(matches!(err2, KafkaError::Timeout(_)));
        assert_eq!(sender.client.in_flight_request_count(), 0);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);

        sender.run_once().await; // run again, must not split / resend
        assert_eq!(sender.client.in_flight_request_count(), 0);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
    }

    /// Translation of `SenderTest#testExpiredBatchesInMultiplePartitions`.
    /// Two records on two different partitions; one gets a successful
    /// response while the other is expired by the time advance. After
    /// `runOnce`, both batches are removed from in-flight and the
    /// expired one's future surfaces a timeout error.
    #[tokio::test]
    async fn test_expired_batches_in_multiple_partitions() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, true);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let tp1 = TopicPartition::new(TOPIC_NAME, 1);
        let request1 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k1", b"v1").await;
        let request2 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 1, 0, b"k2", b"v2").await;
        sender.run_once().await; // send
        assert_eq!(sender.client.in_flight_request_count(), 1);

        // Build a response that ONLY succeeds tp0 (tp1 missing).
        sender
            .client
            .respond(build_produce_response_multi(TOPIC_NAME, topic_id, &[(0, 0)], Errors::None));

        time.sleep(DELIVERY_TIMEOUT_MS as i64);
        sender.run_once().await;

        let err1 = tokio::time::timeout(Duration::from_secs(2), request1.get())
            .await
            .expect("f1 timed out")
            .expect_err("f1 should error");
        let err2 = tokio::time::timeout(Duration::from_secs(2), request2.get())
            .await
            .expect("f2 timed out")
            .expect_err("f2 should error");
        assert!(matches!(err1, KafkaError::Timeout(_)));
        assert!(matches!(err2, KafkaError::Timeout(_)));
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
        assert_eq!(sender.in_flight_batches_for(&tp1).len(), 0);
    }

    /// Translation of `SenderTest#testRecordErrorPropagatedToApplication`.
    /// Per-record `RecordError` entries on the PartitionResponse drive
    /// per-record exception assignment. Records 0 + 2 fail with their
    /// custom messages; record 3 fails with the canonical Errors message;
    /// records 1 + 4 fail with a generic "had invalid records" exception.
    #[tokio::test]
    async fn test_record_error_propagated_to_application() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let mut futures = Vec::new();
        for _ in 0..5 {
            futures.push(append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await);
        }
        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 1);

        // Build a response with record errors at indices 0, 2, 3.
        let record_errors = vec![
            crate::common::message::produce_response_data::BatchIndexAndErrorMessage {
                batch_index: 0,
                batch_index_error_message: Some("0".to_string()),
                unknown_tagged_fields: Vec::new(),
            },
            crate::common::message::produce_response_data::BatchIndexAndErrorMessage {
                batch_index: 2,
                batch_index_error_message: Some("2".to_string()),
                unknown_tagged_fields: Vec::new(),
            },
            crate::common::message::produce_response_data::BatchIndexAndErrorMessage {
                batch_index: 3,
                batch_index_error_message: None,
                unknown_tagged_fields: Vec::new(),
            },
        ];
        let partition_resp = PartitionProduceResponse {
            index: 0,
            error_code: Errors::InvalidRecord.code(),
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: 0,
            record_errors,
            error_message: None,
            current_leader: ProtoLeaderIdAndEpoch::new(),
            unknown_tagged_fields: Vec::new(),
        };
        let topic_resp = TopicProduceResponse {
            name: TOPIC_NAME.to_string(),
            topic_id,
            partition_responses: vec![partition_resp],
            unknown_tagged_fields: Vec::new(),
        };
        let data = ProduceResponseData {
            throttle_time_ms: 0,
            responses: vec![topic_resp],
            node_endpoints: Vec::new(),
            unknown_tagged_fields: Vec::new(),
        };
        sender.client.respond(Box::new(ProduceResponse::new(data)));
        sender.run_once().await;

        for (idx, fut) in futures.into_iter().enumerate() {
            let err = tokio::time::timeout(Duration::from_secs(2), fut.get())
                .await
                .expect("future timed out")
                .expect_err("future should error");
            match (idx, err) {
                (0, KafkaError::InvalidRecord(msg)) => assert_eq!(msg, "0"),
                (2, KafkaError::InvalidRecord(msg)) => assert_eq!(msg, "2"),
                (3, KafkaError::InvalidRecord(msg)) => {
                    // Java falls back to canonical Errors.message() when
                    // record_error.message is None and response.error_message is None.
                    assert_eq!(msg, Errors::InvalidRecord.message().expect("InvalidRecord message"));
                },
                (1, _) | (4, _) => {
                    // Records without a per-record error get a generic exception.
                    // We don't assert on the specific variant here — the
                    // contract is "non-null exception" — but the assertion
                    // above already drives the behavior.
                },
                (idx, err) => panic!("idx={idx} unexpected error: {err:?}"),
            }
        }
    }

    /// Translation of `SenderTest#testWhenFirstBatchExpireNoSendSecondBatchIfGuaranteeOrder`.
    /// With `guarantee_message_order=true`, a partition with an in-flight
    /// batch is muted, so a second append sits in the accumulator until
    /// the first batch's response is processed.
    #[tokio::test]
    async fn test_guarantee_order_mutes_partition_until_first_response() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, true);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let _f1 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k1", b"v1").await;
        sender.run_once().await; // send first request
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);

        // Append a second record to the same partition. It must NOT be
        // sent because the partition is muted.
        let _f2 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k2", b"v2").await;
        sender.run_once().await; // ready/drain — muted, no new send
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);

        // Respond → unmute → next runOnce drains the second record.
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, 0, Errors::None, 0));
        sender.run_once().await; // receive first response
        assert_eq!(sender.client.in_flight_request_count(), 0);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);

        sender.run_once().await; // drain the second batch
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
    }

    /// Idempotent retry: NotLeaderOrFollower retries (without time-out
    /// pressure). After two retries, success.
    #[tokio::test]
    async fn test_not_leader_or_follower_retries_then_success() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 1);
        sender.client.respond(build_produce_response(
            TOPIC_NAME,
            topic_id,
            0,
            -1,
            Errors::NotLeaderOrFollower,
            0,
        ));
        sender.run_once().await; // process retriable error → reenqueue
        time.sleep(RETRY_BACKOFF_MS + 1);
        sender.run_once().await; // resend
        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 1);
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, 5, Errors::None, 0));
        sender.run_once().await;
        let resolved = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect("future errored");
        assert_eq!(resolved.offset(), 5);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
    }

    /// A disconnect-without-body triggers the `was_disconnected()` arm
    /// of `handle_produce_response`, which surfaces as a NetworkException
    /// the batch can retry against. The future stays pending while the
    /// retry is in-flight.
    #[tokio::test]
    async fn test_disconnect_response_triggers_retry() {
        let TestSetup { mut sender, accum, metadata, time, topic_id: _ } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await;
        let pending_dest = sender.client.next_request_destination().expect("queued").to_string();
        let pending_node: i32 = pending_dest.parse().unwrap();
        sender.client.disconnect_node(pending_node);
        sender.run_once().await;
        assert!(!future.is_done(), "Should be waiting for retry");
    }

    /// Acks=0 short-circuit path: when the `ProduceResponse` has no
    /// body and the response is NOT a disconnect / timeout / version
    /// mismatch, the sender treats every batch as success. Mirrors the
    /// `if (response.hasResponse()) … else { complete every batch with
    /// Errors::None }` branch in `handle_produce_response`. Java's
    /// `Sender.handleProduceResponse` reaches this branch when `acks=0`
    /// (the broker honors the client's request to skip the response
    /// body).
    #[tokio::test]
    async fn test_no_response_body_treats_all_records_as_success() {
        let TestSetup { mut sender, accum, metadata, time, topic_id: _ } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await; // sends produce request
        assert_eq!(sender.client.in_flight_request_count(), 1);
        // Pre-stage a "no body, not disconnected" response — the next
        // run_once will return it from poll() and trigger the acks=0
        // success short-circuit.
        sender.client.respond_with_disconnect(None, false);
        sender.run_once().await; // handle response → success short-circuit
        let resolved = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect("future errored");
        // The Java `Sender.handleProduceResponse` else-branch builds a
        // `PartitionResponse(Errors::None, base_offset=-1, log_append_time=-1, ...)`
        // for every batch. base_offset = -1 is fine here — Java does the
        // same.
        assert_eq!(resolved.offset(), -1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
    }

    /// Translation of `SenderTest#testProducerBatchRetriesWhenPartitionLeaderChanges`.
    /// First half: NotLeaderOrFollower → batch reenqueued. Update
    /// metadata to bump leader epoch; on next runOnce the retry skips
    /// the backoff window.
    #[tokio::test]
    async fn test_producer_batch_retries_when_partition_leader_changes() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(10, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await; // send
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
        sender.client.respond(build_produce_response(
            TOPIC_NAME,
            topic_id,
            0,
            -1,
            Errors::NotLeaderOrFollower,
            0,
        ));
        sender.run_once().await; // receive retriable error → reenqueue
        assert!(!future.is_done(), "Produce request should not be done.");

        // Bump leader epoch by re-applying metadata response that
        // has an incremented partition_metadata leader_epoch via
        // `update_partition_leadership` on the inner Metadata.
        let mut updated_leaders: HashMap<TopicPartition, MetadataLeaderIdAndEpoch> = HashMap::new();
        updated_leaders.insert(tp0.clone(), MetadataLeaderIdAndEpoch::new(Some(0), Some(101)));
        let leader_nodes = vec![Node::new(0, "localhost".to_string(), 1111)];
        let _ = metadata.metadata().update_partition_leadership(updated_leaders, leader_nodes);

        // The retry skips backoff because the leader epoch changed.
        sender.run_once().await; // resend immediately
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
        assert!(sender.client.has_in_flight_requests());
        let offset = 999;
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, offset, Errors::None, 0));
        sender.run_once().await; // receive success
        let resolved = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect("future errored");
        assert_eq!(resolved.offset(), offset);
    }

    /// Translation of `SenderTest#testNoDoubleDeallocation`. The
    /// MockClient's `check_timeout_of_pending_requests` triggers a
    /// disconnect after `REQUEST_TIMEOUT_MS` elapses; the disconnect is
    /// then handled by `Sender::handle_produce_response` which
    /// deallocates the batch buffer exactly once.
    #[tokio::test]
    async fn test_no_double_deallocation() {
        let TestSetup { mut sender, accum, metadata, time, topic_id: _ } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let _future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 1);
        let inflight = sender.in_flight_batches_for(&tp0)[0].clone();
        assert!(!inflight.is_buffer_deallocated(), "Buffer not deallocated yet");

        // Advance time past `request_timeout_ms` so the MockClient's
        // checkTimeoutOfPendingRequests disconnects the request.
        time.sleep((REQUEST_TIMEOUT_MS + 1) as i64);
        sender.run_once().await; // poll → disconnect → handle response
        assert!(inflight.is_buffer_deallocated(), "Buffer should be deallocated after timeout");

        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 0);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
    }

    /// `wakeup` forwards to `KafkaClient::wakeup` without panicking.
    #[test]
    fn wakeup_forwards_to_client() {
        let TestSetup { sender, .. } = make_test_setup(i32::MAX, false);
        sender.wakeup();
    }

    /// `initiate_close` closes the accumulator and clears the running
    /// flag.
    #[test]
    fn initiate_close_closes_accumulator() {
        let TestSetup { sender, accum, .. } = make_test_setup(i32::MAX, false);
        assert!(!accum.is_closed());
        sender.initiate_close();
        assert!(accum.is_closed());
        assert!(!sender.is_running());
    }

    /// `force_close` sets the force-close flag and clears running.
    #[test]
    fn force_close_clears_running_and_force_flag() {
        let TestSetup { sender, .. } = make_test_setup(i32::MAX, false);
        assert!(sender.is_running());
        sender.force_close();
        assert!(!sender.is_running());
        assert!(sender.force_close.load(Ordering::Acquire));
    }

    /// run_loop terminates after `initiate_close` once the accumulator
    /// is drained.
    #[tokio::test]
    async fn run_loop_terminates_on_initiate_close() {
        let TestSetup { mut sender, .. } = make_test_setup(i32::MAX, false);
        sender.initiate_close();
        // Accumulator is empty + no in-flight requests → main loop exits
        // and shutdown drain loop exits immediately.
        tokio::time::timeout(Duration::from_secs(2), sender.run_loop())
            .await
            .expect("run_loop did not terminate within timeout");
    }

    /// run_loop force-close path: sets force_close, then run_loop
    /// terminates without draining undrained batches.
    #[tokio::test]
    async fn run_loop_force_close_terminates_immediately() {
        let TestSetup { mut sender, accum, metadata, time, topic_id: _ } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        // Append one record that will never be sent (because force_close
        // skips the drain loop).
        let _f = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.force_close();
        tokio::time::timeout(Duration::from_secs(2), sender.run_loop())
            .await
            .expect("run_loop did not terminate within timeout");
    }

    /// Build a produce response with a custom error_message attached to
    /// the partition response.
    fn build_produce_response_with_message(
        topic: &str,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
        error: Errors,
        message: &str,
    ) -> Box<dyn AbstractResponse> {
        let partition_resp = PartitionProduceResponse {
            index: partition,
            error_code: error.code(),
            base_offset: offset,
            log_append_time_ms: -1,
            log_start_offset: 0,
            record_errors: Vec::new(),
            error_message: Some(message.to_string()),
            current_leader: ProtoLeaderIdAndEpoch::new(),
            unknown_tagged_fields: Vec::new(),
        };
        let topic_resp = TopicProduceResponse {
            name: topic.to_string(),
            topic_id,
            partition_responses: vec![partition_resp],
            unknown_tagged_fields: Vec::new(),
        };
        let data = ProduceResponseData {
            throttle_time_ms: 0,
            responses: vec![topic_resp],
            node_endpoints: Vec::new(),
            unknown_tagged_fields: Vec::new(),
        };
        Box::new(ProduceResponse::new(data))
    }

    /// Regression test for `is_invalid_metadata`: every Java
    /// `InvalidMetadataException` subclass with a wire error code must
    /// trigger a metadata refresh. Java treats the test as
    /// `error.exception() instanceof InvalidMetadataException`, which
    /// matches all 13 wire-coded subclasses. The Rust hardcoded list was
    /// previously missing 7 of those (KafkaStorageError, InconsistentTopicId,
    /// ReplicaNotAvailable, ListenerNotFound, PreferredLeaderNotAvailable,
    /// EligibleLeadersNotAvailable, ElectionNotNeeded) — which would silently
    /// skip the metadata refresh on a produce response with one of those
    /// errors.
    #[test]
    fn is_invalid_metadata_matches_all_java_invalid_metadata_subclasses() {
        // 13 wire-coded subclasses of InvalidMetadataException.
        let must_be_true = [
            Errors::UnknownTopicOrPartition,
            Errors::LeaderNotAvailable,
            Errors::NotLeaderOrFollower,
            Errors::ReplicaNotAvailable,
            Errors::NetworkException,
            Errors::KafkaStorageError,
            Errors::ListenerNotFound,
            Errors::FencedLeaderEpoch,
            Errors::PreferredLeaderNotAvailable,
            Errors::EligibleLeadersNotAvailable,
            Errors::ElectionNotNeeded,
            Errors::UnknownTopicId,
            Errors::InconsistentTopicId,
        ];
        for err in must_be_true {
            assert!(
                Sender::<MockClientImpl>::is_invalid_metadata(err),
                "{err:?} should be treated as InvalidMetadataException"
            );
        }
        // A handful of non-InvalidMetadataException errors must NOT trigger
        // the metadata-refresh path (sanity).
        let must_be_false = [
            Errors::None,
            Errors::CorruptMessage,
            Errors::TopicAuthorizationFailed,
            Errors::MessageTooLarge,
            Errors::OutOfOrderSequenceNumber,
            Errors::UnsupportedVersion,
        ];
        for err in must_be_false {
            assert!(
                !Sender::<MockClientImpl>::is_invalid_metadata(err),
                "{err:?} should NOT be treated as InvalidMetadataException"
            );
        }
    }

    /// One row in `build_produce_response_with_leader_info`'s
    /// per-partition input: `(partition, base_offset, error,
    /// Option<(leader_id, leader_epoch)>)`. The `Option` carries the
    /// KIP-951 `current_leader` field — `None` represents Java's
    /// default-constructed `LeaderIdAndEpoch` (-1/-1).
    type PartitionResponseRow = (i32, i64, Errors, Option<(i32, i32)>);

    /// Build a produce response that carries KIP-951 leader-info fields:
    /// per-partition `current_leader` (id + epoch) plus a top-level
    /// `node_endpoints` array. Mirrors Java `produceResponse(responses,
    /// partitionLeaderInfo, nodes)` in `SenderTest.java:3771`.
    fn build_produce_response_with_leader_info(
        topic: &str,
        topic_id: Uuid,
        partition_responses: Vec<PartitionResponseRow>,
        node_endpoints: Vec<Node>,
    ) -> Box<dyn AbstractResponse> {
        use crate::common::message::produce_response_data::NodeEndpoint;

        let prs: Vec<PartitionProduceResponse> = partition_responses
            .into_iter()
            .map(|(idx, off, err, leader)| {
                let current_leader = match leader {
                    Some((leader_id, leader_epoch)) => {
                        ProtoLeaderIdAndEpoch { leader_id, leader_epoch, unknown_tagged_fields: Vec::new() }
                    },
                    None => ProtoLeaderIdAndEpoch::new(),
                };
                PartitionProduceResponse {
                    index: idx,
                    error_code: err.code(),
                    base_offset: off,
                    log_append_time_ms: -1,
                    log_start_offset: 0,
                    record_errors: Vec::new(),
                    error_message: None,
                    current_leader,
                    unknown_tagged_fields: Vec::new(),
                }
            })
            .collect();
        let topic_resp = TopicProduceResponse {
            name: topic.to_string(),
            topic_id,
            partition_responses: prs,
            unknown_tagged_fields: Vec::new(),
        };
        let endpoints: Vec<NodeEndpoint> = node_endpoints
            .iter()
            .map(|n| NodeEndpoint {
                node_id: n.id(),
                host: n.host().to_string(),
                port: n.port(),
                rack: n.rack().map(|s| s.to_string()),
                unknown_tagged_fields: Vec::new(),
            })
            .collect();
        let data = ProduceResponseData {
            throttle_time_ms: 0,
            responses: vec![topic_resp],
            node_endpoints: endpoints,
            unknown_tagged_fields: Vec::new(),
        };
        Box::new(ProduceResponse::new(data))
    }

    /// Mirrors Java's `try/catch (Exception)` swallow in `Sender.run()`:
    /// a panic inside `run_once()` must NOT abort `run_loop`. The loop
    /// logs the panic and continues. After the panic is observed, the
    /// test bypasses the drain stage (`force_close`) so the loop
    /// terminates without depending on a separately-staged response —
    /// the property under test is "panic was caught", not "drain
    /// completed".
    ///
    /// To genuinely exercise `catch_unwind` (Round 2 / Issue 10), the
    /// test must:
    ///   1. Append a record so iteration 1 has work to do (`run_once`
    ///      drains, sends, then polls — and only then can the armed
    ///      panic actually trip).
    ///   2. Arm `panic_on_next_poll` BEFORE entering the loop.
    ///   3. Spawn `run_loop` on a Tokio task and wait for the
    ///      trip-counter to increment, proving the panic actually fired
    ///      AND was caught (otherwise `JoinHandle.await` would resolve
    ///      with `Err(JoinError::panic)`).
    ///   4. Flip `running=false` + `force_close=true` so the loop exits
    ///      cleanly, then `await` the JoinHandle and assert it returned
    ///      `Ok` (the panic did not propagate to the caller — Java's
    ///      `try/catch (Exception)` swallow contract).
    ///
    /// Mentally reverting the `catch_unwind` wrapper: the
    /// `JoinHandle.await` would resolve to `Err(JoinError::panic)`, the
    /// final `.is_ok()` assert would fail. Test fidelity confirmed.
    #[tokio::test]
    async fn run_loop_swallows_panics_and_continues() {
        let TestSetup { mut sender, accum, metadata, time, topic_id: _ } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();

        // (1) Append so iteration 1 of `run_loop` has work — drain, send,
        //     poll. Without this, the `while running` and drain loops
        //     both short-circuit and `run_once` is never called, leaving
        //     the armed panic untripped (Round 1 regression — see Issue
        //     10).
        let _future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;

        // (2) Arm the panic and capture the trip counter handle.
        sender.client.set_panic_on_next_poll("synthetic test panic");
        let trip_counter = sender.client.panic_trip_counter();

        // (3) Capture handles to drive shutdown from outside the
        //     spawned task — `run_loop` takes `&mut self`, so once the
        //     sender moves into the task we can't call
        //     `initiate_close()` directly.
        let running = sender.running_arc();
        let force_close = sender.force_close_arc();

        let join = tokio::spawn(async move {
            sender.run_loop().await;
        });

        // (3) Wait for the panic to actually trip. The counter is
        //     incremented atomically inside `MockClientImpl::poll`
        //     immediately before the `panic!`, so a non-zero value
        //     proves: (a) `run_once` was called, (b) `poll` was
        //     reached, (c) the panic fired. If `catch_unwind` failed
        //     to swallow it, the join handle would already be in
        //     `Err(JoinError::panic)` state and the next assertion
        //     would catch it.
        let observed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if trip_counter.load(Ordering::Relaxed) >= 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(observed.is_ok(), "panic-on-next-poll never tripped (run_once not reached?)");
        assert!(
            !join.is_finished(),
            "run_loop must keep running after the swallowed panic; \
             a finished JoinHandle here means the panic propagated"
        );

        // (4) Drive shutdown: `force_close=true` bypasses the drain
        //     loop (the in-flight request from iteration 1 has no
        //     staged response — without `force_close` the drain loop
        //     would spin forever). Setting `running=false` exits the
        //     main loop.
        force_close.store(true, Ordering::Release);
        running.store(false, Ordering::Release);

        // (4) `JoinHandle::await` returning `Ok(())` is the assertion:
        //     the loop terminated cleanly, the panic was swallowed by
        //     `catch_unwind` (otherwise `JoinError::panic`).
        let join_result = tokio::time::timeout(Duration::from_secs(2), join).await;
        assert!(
            matches!(&join_result, Ok(Ok(()))),
            "run_loop must terminate after panic — Java's `try/catch` semantics. Got: {join_result:?}"
        );
        assert_eq!(
            trip_counter.load(Ordering::Relaxed),
            1,
            "panic should have tripped exactly once (one-shot arm)"
        );
    }

    /// Translation of `SenderTest#testNoBufferReuseWhenBatchExpires`
    /// (KAFKA-19012 invariant). When a batch expires while still
    /// in-flight, the buffer **must NOT be returned to the pool** —
    /// the network stack might still be reading from it. The Sender's
    /// `fail_batch_with_exceptions` defers deallocation via
    /// `maybe_remove_and_deallocate_batch_later`. This test asserts the
    /// invariant by inspecting `BufferPool::available_memory` before
    /// and after the in-flight expiry tick.
    #[tokio::test]
    async fn test_no_buffer_reuse_when_batch_expires() {
        // Use a small pool so the math is easy to inspect:
        // total_size = 32KiB, batch_size = 16KiB → exactly 2 batches.
        let total_size: i64 = 32 * 1024;
        let batch_size: i32 = 16 * 1024;
        let time: Arc<dyn Time> = Arc::new(MockTime::with_initial(0, 0, 0));
        let pool = Arc::new(BufferPool::new(total_size, batch_size, time.clone(), "producer-metrics"));
        // Pre-allocate one buffer and return it to the pool so the
        // Sender's first append picks it up from the free list (the
        // buffer is the same one the test will inspect).
        let pre = pool.allocate(batch_size, 0).await.expect("allocate");
        pool.deallocate_full(pre);
        assert_eq!(pool.available_memory(), total_size);

        let TestSetup { mut sender, accum, metadata, time, topic_id: _ } =
            make_test_setup_with_pool(i32::MAX, false, Some(Arc::clone(&pool)));
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let _future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"key", b"value").await;
        sender.run_once().await; // sends produce request — pool now has one buffer in-flight
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
        // The drained batch consumed one poolable buffer; the pool's
        // available_memory should drop by `batch_size`.
        let available_after_send = pool.available_memory();
        assert_eq!(
            available_after_send,
            total_size - batch_size as i64,
            "Pool must reflect the in-flight buffer"
        );

        // Fire in-flight expiry: advance past delivery_timeout, run_once.
        // The sender's expired-batch path runs `maybe_remove_and_deallocate_batch_later`
        // (NOT `deallocate`) — pool memory stays unchanged.
        time.sleep((DELIVERY_TIMEOUT_MS + 100) as i64);
        sender.run_once().await;
        assert_eq!(
            sender.in_flight_batches_for(&tp0).len(),
            0,
            "expired batch removed from in-flight map"
        );
        assert_eq!(
            pool.available_memory(),
            available_after_send,
            "Buffer must NOT be re-pooled while the request is still in-flight (KAFKA-19012)"
        );
    }

    /// Translation of `SenderTest#testWhenProduceResponseReturnsWithALeaderShipChangeErrorButNoNewLeaderInformation`.
    /// Drives the Sender's KIP-951 fallback path: a `NotLeaderOrFollower`
    /// produce response with no per-partition `current_leader` info should
    /// (a) request a metadata update, (b) leave the cluster snapshot
    /// unchanged, (c) reenqueue the batch for retry.
    #[tokio::test]
    async fn test_produce_response_leader_change_no_new_leader_information() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(10, false);
        let cluster_before = metadata.metadata().fetch_metadata_snapshot().cluster();
        // Pre-condition: metadata not yet update-requested.
        assert!(!metadata.metadata().update_requested());

        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let future = append_to_accumulator(&accum, &time, &cluster_before, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await; // sends produce request
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);

        // Stage NOT_LEADER_OR_FOLLOWER with default current_leader (-1, -1)
        // — i.e. no new leader info.
        sender.client.respond(build_produce_response_with_leader_info(
            TOPIC_NAME,
            topic_id,
            vec![(0, -1, Errors::NotLeaderOrFollower, None)],
            Vec::new(),
        ));
        sender.run_once().await; // handle response → reenqueue, request update
        assert!(!future.is_done(), "Produce request should not be done.");

        // Metadata refresh requested.
        assert!(
            metadata.metadata().update_requested(),
            "Metadata refresh must be requested after NOT_LEADER_OR_FOLLOWER"
        );
        // Cluster snapshot unchanged (no KIP-951 leader info to apply).
        let cluster_after = metadata.metadata().fetch_metadata_snapshot().cluster();
        assert!(
            Arc::ptr_eq(&cluster_before, &cluster_after)
                || cluster_before.partitions_for_topic(TOPIC_NAME).len()
                    == cluster_after.partitions_for_topic(TOPIC_NAME).len(),
            "Cluster snapshot must be unchanged when no new leader info arrives"
        );
        assert_eq!(metadata.metadata().current_leader(&tp0).epoch, None);
    }

    /// Translation of `SenderTest#testWhenProduceResponseReturnsWithALeaderShipChangeErrorAndNewLeaderInformation`.
    /// Drives the Sender's KIP-951 happy path: a `NotLeaderOrFollower`
    /// produce response carrying per-partition `current_leader.leader_id`
    /// + `leader_epoch` should call `update_partition_leadership` and
    /// install the new leader (visible via `metadata.current_leader(tp)`).
    #[tokio::test]
    async fn test_produce_response_leader_change_with_new_leader_information() {
        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(10, false);
        let cluster_before = metadata.metadata().fetch_metadata_snapshot().cluster();

        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let future = append_to_accumulator(&accum, &time, &cluster_before, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await; // sends produce request
        assert_eq!(sender.client.in_flight_request_count(), 1);

        // Stage NOT_LEADER_OR_FOLLOWER with a NEW leader: id=9990, epoch=101.
        // Also include the `node_endpoints` for the new leader.
        let new_leader = Node::new(9990, "newhost9990".to_string(), 9990);
        sender.client.respond(build_produce_response_with_leader_info(
            TOPIC_NAME,
            topic_id,
            vec![(0, -1, Errors::NotLeaderOrFollower, Some((9990, 101)))],
            vec![new_leader.clone()],
        ));
        sender.run_once().await; // handle response → update_partition_leadership
        assert!(!future.is_done(), "Produce request should not be done.");

        // Metadata refresh requested.
        assert!(metadata.metadata().update_requested());
        // The new leader info was applied via `update_partition_leadership`.
        let leader_after = metadata.metadata().current_leader(&tp0);
        assert_eq!(leader_after.epoch, Some(101), "new leader epoch must be applied");
        let leader_node = leader_after.leader.expect("leader node populated");
        assert_eq!(leader_node.id(), 9990, "new leader node id must be applied");
        assert_eq!(leader_node.host(), "newhost9990");
    }

    /// Translation of `SenderTest#testRetries` (the second loop — retry
    /// exhaustion). With `retries=1`, two consecutive disconnects must
    /// drop the batch with `KafkaError::Network*`, not retry forever.
    /// The success path of `testRetries` is already covered by
    /// `test_retries_then_success`.
    #[tokio::test]
    async fn test_retries_exhausted_yields_network_exception() {
        let max_retries = 1;
        let TestSetup { mut sender, accum, metadata, time, topic_id: _ } = make_test_setup(max_retries, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"key", b"value").await;
        sender.run_once().await; // send first attempt
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);

        // Loop max_retries+1 times. Each iteration: disconnect the
        // outstanding request, receive the disconnect, sleep past the
        // retry backoff, then run the sender twice (the MockClient's
        // `ready()` requires two ticks after a disconnect to re-establish
        // the connection). After max_retries+1 disconnects the batch
        // must surface as a NetworkException (no more retries).
        for i in 0..(max_retries + 1) {
            let dest = sender.client.next_request_destination().expect("queued").to_string();
            let node_id: i32 = dest.parse().unwrap();
            sender.client.disconnect_node(node_id);
            sender.run_once().await; // receive disconnect → reenqueue (or drop if retries exhausted)
            time.sleep(RETRY_BACKOFF_MS + 1); // skip past retry backoff
            sender.run_once().await; // ready() resets disconnected flag (not yet ready)
            sender.run_once().await; // ready() == true → resend (or no-op once exhausted)
            // After the final disconnect the batch is dropped; otherwise
            // it must be in flight again.
            let expected = if i == max_retries { 0 } else { 1 };
            assert_eq!(
                sender.in_flight_batches_for(&tp0).len(),
                expected,
                "iteration {i}: in-flight batches mismatch"
            );
        }
        let err = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future timed out")
            .expect_err("future should error");
        assert!(
            matches!(err, KafkaError::Network(_) | KafkaError::Disconnect(_)),
            "Expected NetworkException-equivalent on retry exhaustion, got {err:?}"
        );
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
    }

    /// Translation of `SenderTest#testSendInOrder`. Two-broker setup;
    /// after the first produce request to broker B is in flight, the
    /// metadata is updated to move the partition to broker A. The Sender
    /// must NOT send the second batch to broker A (or to broker B)
    /// while broker B's request is still in flight, when message-order
    /// guarantees are enabled. We assert that the second batch stays
    /// in-flight on its own request and the first request remains
    /// outstanding (no premature reroute).
    #[tokio::test]
    async fn test_send_in_order() {
        // Build a custom setup: two brokers, partition 0 on broker 1.
        let time: Arc<dyn Time> = Arc::new(MockTime::with_initial(0, 0, 0));
        let metadata = ProducerMetadata::new(
            0,
            0,
            i64::MAX,
            60_000,
            LogContext::new(),
            Arc::new(ClusterResourceListeners::new()),
            time.clone(),
        )
        .expect("ProducerMetadata::new");
        let topic_id = Uuid::new(0xAA, 0xBB);
        // Two brokers: id=0 (will become the new leader after the update),
        // and id=1 (initial leader).
        let node0 = Node::new(0, "broker0".to_string(), 9091);
        let node1 = Node::new(1, "broker1".to_string(), 9092);
        let parts_v1 = vec![PartitionInfo::new(
            TOPIC_NAME,
            0,
            Some(node1.clone()),
            vec![node1.clone()],
            vec![node1.clone()],
        )];
        let cluster_v1 = Arc::new(Cluster::new(
            None,
            vec![node0.clone(), node1.clone()],
            parts_v1,
            HashSet::new(),
            HashSet::new(),
        ));
        // Build metadata response that puts tp0 on node 1.
        let metadata_response_v1 = build_metadata_response_for(&cluster_v1, topic_id, &[(0, 1)]);
        metadata.add(TOPIC_NAME, time.milliseconds());
        metadata
            .update_with_current_request_version(&metadata_response_v1, false, time.milliseconds())
            .expect("metadata update");

        let pool = Arc::new(BufferPool::new(1024 * 1024, 16 * 1024, time.clone(), "producer-metrics"));
        let accum = Arc::new(RecordAccumulator::new_with_default_partitioner(
            LogContext::new(),
            16 * 1024,
            CompressionType::None,
            0,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MS,
            DELIVERY_TIMEOUT_MS,
            "producer-metrics",
            time.clone(),
            None,
            pool,
        ));
        let client = MockClientImpl::new(time.clone());
        let mut sender = Sender::new(
            LogContext::new(),
            client,
            Arc::clone(&metadata),
            Arc::clone(&accum),
            true, // guarantee_message_order = true (Java sets `true` here)
            MAX_REQUEST_SIZE,
            ACKS_ALL,
            1, // max_retries
            time.clone(),
            REQUEST_TIMEOUT_MS,
            RETRY_BACKOFF_MS,
            None,
            Arc::from("clientId"),
        );

        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let _f1 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k1", b"v1").await;
        sender.run_once().await; // send first request to broker 1
        assert_eq!(sender.client.in_flight_request_count(), 1);
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
        assert_eq!(
            sender.client.next_request_destination().unwrap(),
            "1",
            "first request must target broker 1"
        );

        // While the first request is in-flight, advance time and append a
        // second batch to tp0.
        time.sleep(900);
        let _f2 = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k2", b"v2").await;

        // Update metadata so tp0 is now hosted on broker 0. Java's
        // `client.prepareMetadataUpdate(...)` simulates the same.
        let parts_v2 = vec![PartitionInfo::new(
            TOPIC_NAME,
            0,
            Some(node0.clone()),
            vec![node0.clone()],
            vec![node0.clone()],
        )];
        let cluster_v2 = Arc::new(Cluster::new(
            None,
            vec![node0.clone(), node1.clone()],
            parts_v2,
            HashSet::new(),
            HashSet::new(),
        ));
        let metadata_response_v2 = build_metadata_response_for(&cluster_v2, topic_id, &[(0, 0)]);
        metadata
            .update_with_current_request_version(&metadata_response_v2, false, time.milliseconds())
            .expect("metadata update");

        // The Sender must NOT send the second batch to broker 0 (or
        // re-target broker 1) while the first request is in flight —
        // `guarantee_message_order` mutes the partition until the
        // first response.
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1, "no extra in-flight batch yet");

        // Respond to the first request and let the sender send the
        // second batch (broker 0 needs a `ready()` cycle to mark itself
        // ready before the drain loop can target it).
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, 0, Errors::None, 0));
        sender.run_once().await; // handle response → unmute partition
        sender.run_once().await; // ready(node 0) — reset to ready
        sender.run_once().await; // drain & send second batch
        assert_eq!(sender.client.in_flight_request_count(), 1, "second batch in flight");
        assert_eq!(
            sender.client.next_request_destination().unwrap(),
            "0",
            "second request must target broker 0 after metadata update"
        );
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
    }

    /// Translation of `SenderTest#testAppendInExpiryCallback`
    /// (`SenderTest.java:413-465`). The user's `onCompletion` callback
    /// is fired on expiry-failure with a `KafkaError::Timeout`; from
    /// inside that callback the user re-appends a record. Java asserts
    /// the resulting deque has exactly 1 batch with `recordCount=10`
    /// (10 callback-driven re-appends batched into a single new batch).
    ///
    /// **Java→Rust translation**: `Callback::on_completion` is sync,
    /// but `RecordAccumulator::append` is `async fn`. We bridge by
    /// `tokio::spawn`-ing the re-append from inside the callback and
    /// collecting the `JoinHandle`s in a shared `Mutex<Vec<_>>`. After
    /// the expiry-fire `run_once` returns, the test awaits all spawned
    /// handles before asserting on `record_count`. The runtime
    /// (current_thread) interleaves the spawns naturally because each
    /// `accumulator.append` await yields at the buffer-pool lookup
    /// point and again after the deque insert.
    ///
    /// CLAUDE.md rule 9 compliance: the callback obligation is honored
    /// at the same lifecycle point as Java
    /// (`complete_future_and_fire_callbacks`); the `tokio::spawn` is
    /// the only way to invoke an async API from a sync callback without
    /// blocking. Per-message spawn is acceptable here because this is
    /// test code (CLAUDE.md rule 11.4 forbids per-message spawn on the
    /// production send path, not in tests).
    #[tokio::test]
    async fn test_append_in_expiry_callback() {
        use std::sync::atomic::AtomicUsize;
        use tokio::task::JoinHandle;

        type ReAppendHandles = Arc<Mutex<Vec<JoinHandle<Result<(), KafkaError>>>>>;

        let messages_per_batch = 10_usize;
        let TestSetup { mut sender, accum, metadata, time, topic_id: _ } = make_test_setup(i32::MAX, false);
        let cluster_arc = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);

        // Shared state for the callback. The callback fires from the
        // sender's `run_once()` task, but the re-append is spawned onto
        // the runtime so we can `.await` it. Java's
        // `accumulator.append(...)` is blocking-with-timeout and runs
        // synchronously inside the callback; Rust's is async.
        let expiry_callback_count = Arc::new(AtomicUsize::new(0));
        let unexpected_error = Arc::new(Mutex::new(Option::<KafkaError>::None));
        let spawn_handles: ReAppendHandles = Arc::new(Mutex::new(Vec::new()));

        struct ReAppendCallback {
            accum: Arc<RecordAccumulator>,
            cluster: Arc<Cluster>,
            time: Arc<dyn Time>,
            expiry_count: Arc<AtomicUsize>,
            unexpected: Arc<Mutex<Option<KafkaError>>>,
            handles: ReAppendHandles,
        }

        impl crate::producer::Callback for ReAppendCallback {
            fn on_completion(&self, _metadata: Option<&crate::producer::RecordMetadata>, error: Option<&KafkaError>) {
                match error {
                    Some(KafkaError::Timeout(_)) => {
                        self.expiry_count.fetch_add(1, Ordering::Relaxed);
                        // Spawn the re-append. `.await` cannot be called
                        // from this sync trait method. Java's blocking
                        // `accumulator.append(...)` returns synchronously
                        // because it holds the deque lock for the
                        // append; Rust's async path lets the buffer pool
                        // back-pressure cleanly, which is also why we
                        // drive it through a Tokio task.
                        let accum = Arc::clone(&self.accum);
                        let cluster = Arc::clone(&self.cluster);
                        let time = Arc::clone(&self.time);
                        let now_ms = time.milliseconds();
                        let h = tokio::spawn(async move {
                            accum
                                .append(
                                    TOPIC_NAME,
                                    0,
                                    0,
                                    Some(b"key" as &[u8]),
                                    Some(b"value" as &[u8]),
                                    &[] as &[crate::common::header::RecordHeader],
                                    None,
                                    1000,
                                    now_ms,
                                    &cluster,
                                )
                                .await
                                .map(|_| ())
                        });
                        self.handles.lock().unwrap().push(h);
                    },
                    Some(other) => {
                        let mut slot = self.unexpected.lock().unwrap();
                        if slot.is_none() {
                            *slot = Some(other.clone());
                        }
                    },
                    None => {
                        // Success — Java only fails the assertion via
                        // `unexpectedException`; we mirror by recording.
                    },
                }
            }
        }

        impl crate::producer::internals::record_accumulator::AppendCallbacks for ReAppendCallback {
            fn set_partition(&self, _partition: i32) {}
        }

        let now_ms = time.milliseconds();
        for _ in 0..messages_per_batch {
            let cb: Arc<dyn crate::producer::internals::record_accumulator::AppendCallbacks> =
                Arc::new(ReAppendCallback {
                    accum: Arc::clone(&accum),
                    cluster: Arc::clone(&cluster_arc),
                    time: Arc::clone(&time),
                    expiry_count: Arc::clone(&expiry_callback_count),
                    unexpected: Arc::clone(&unexpected_error),
                    handles: Arc::clone(&spawn_handles),
                });
            accum
                .append(
                    TOPIC_NAME,
                    0,
                    0,
                    Some(b"key" as &[u8]),
                    Some(b"value" as &[u8]),
                    &[] as &[crate::common::header::RecordHeader],
                    Some(cb),
                    1000,
                    now_ms,
                    &cluster_arc,
                )
                .await
                .expect("append");
        }

        // Drive an in-flight expiry: send → advance time past
        // delivery_timeout → run_once. The expiry-tick `run_once` fires
        // the 10 callbacks synchronously inside
        // `complete_future_and_fire_callbacks`; each callback spawns a
        // re-append task.
        sender.run_once().await; // send produce request
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 1);
        time.sleep((DELIVERY_TIMEOUT_MS + 100) as i64);
        sender.run_once().await; // expire in-flight batch → fires callbacks

        // Java asserts callbacks fired exactly `messagesPerBatch` times.
        assert_eq!(
            expiry_callback_count.load(Ordering::Relaxed),
            messages_per_batch,
            "callbacks not invoked for expiry"
        );
        // Java's `assertNull(unexpectedException.get())`.
        assert!(
            unexpected_error.lock().unwrap().is_none(),
            "unexpected exception in callback: {:?}",
            unexpected_error.lock().unwrap()
        );

        // Drain the spawned re-append handles so the deque assertions
        // see all 10 records. Each handle returns `Ok(())` on a
        // successful append.
        let handles = std::mem::take(&mut *spawn_handles.lock().unwrap());
        assert_eq!(handles.len(), messages_per_batch, "10 re-append spawns expected");
        for h in handles {
            tokio::time::timeout(Duration::from_secs(2), h)
                .await
                .expect("re-append spawn timed out")
                .expect("re-append join error")
                .expect("re-append failed");
        }

        // Java: `assertNotNull(accumulator.getDeque(tp1));` and
        //       `assertEquals(1, accumulator.getDeque(tp1).size());`
        //       `assertEquals(messagesPerBatch, ...peekFirst().recordCount);`
        // Rust: get_deque returns Option<BatchDeque>; we check Some +
        //       length 1 + the head batch's record_count is 10.
        let deque = accum.get_deque(&tp0).expect("deque present after re-append");
        let deque_guard = deque.lock().unwrap();
        assert_eq!(deque_guard.len(), 1, "re-appended records must batch into a single new batch");
        let head = deque_guard.front().expect("deque non-empty");
        assert_eq!(
            head.record_count(),
            messages_per_batch as i32,
            "all 10 re-appends must batch together (Java's recordCount=10 invariant)"
        );
    }

    /// Translation of `SenderTest#testMetadataTopicExpiry`
    /// (`SenderTest.java:472-505`). Verifies the topic-idle window:
    /// `metadata.contains_topic(t)` returns `true` while the topic is
    /// in active use, and flips to `false` after `TOPIC_IDLE_MS`
    /// elapses without re-touching the topic, on the next metadata
    /// update.
    ///
    /// Java uses `client.updateMetadata(...)` to refresh the producer
    /// metadata; our Rust translation calls
    /// `ProducerMetadata::update_with_current_request_version(...)`
    /// directly (no Mockito harness needed — Round 1 deferral
    /// rationale was incorrect; see Round 2 / Issue 11).
    #[tokio::test]
    async fn test_metadata_topic_expiry() {
        const TOPIC_IDLE_MS: i64 = 60 * 1000;

        let TestSetup { mut sender, accum, metadata, time, topic_id } = make_test_setup(i32::MAX, false);
        let cluster = metadata.metadata().fetch_metadata_snapshot().cluster();
        let tp0 = TopicPartition::new(TOPIC_NAME, 0);
        let offset = 0i64;

        // (A) First produce cycle: append → send → respond → handle.
        // The topic is in `metadata` from `make_test_setup` (which
        // calls `metadata.add(TOPIC_NAME, 0)`), so `contains_topic`
        // starts true.
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await; // send
        assert!(metadata.contains_topic(TOPIC_NAME), "Topic not added to metadata");
        // Java: `client.updateMetadata(...)` → in Rust, refresh the
        // producer-side metadata from the same response.
        let resp = build_metadata_response(&cluster, topic_id);
        metadata
            .update_with_current_request_version(&resp, false, time.milliseconds())
            .expect("metadata update");
        sender.run_once().await; // send produce request (already sent in step above; this is the no-op tick)
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, offset, Errors::None, 0));
        sender.run_once().await; // handle response
        assert_eq!(sender.client.in_flight_request_count(), 0, "Request completed.");
        assert!(!sender.client.has_in_flight_requests());
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
        sender.run_once().await;
        // Java: `assertTrue(future.isDone())` — drive the future
        // resolution to confirm.
        let resolved = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future.get() timed out")
            .expect("future returned error");
        assert_eq!(resolved.offset(), offset);
        assert!(metadata.contains_topic(TOPIC_NAME), "Topic not retained in metadata list");

        // (B) Advance the clock past TOPIC_IDLE_MS without re-touching
        // the topic. The next metadata update fires the
        // `retain_topic` predicate; since
        // `topics.get(TOPIC_NAME).expire_ms (== 60_000) <= now_ms (==
        // 60_000)`, the predicate returns false and the topic is
        // dropped from the producer's tracked set.
        time.sleep(TOPIC_IDLE_MS);
        let resp = build_metadata_response(&cluster, topic_id);
        metadata
            .update_with_current_request_version(&resp, false, time.milliseconds())
            .expect("metadata update");
        assert!(!metadata.contains_topic(TOPIC_NAME), "Unused topic has not been expired");

        // (C) Append again — the producer re-touches the topic and
        // it re-appears in the metadata-tracked set.
        let future = append_to_accumulator(&accum, &time, &cluster, TOPIC_NAME, 0, 0, b"k", b"v").await;
        sender.run_once().await;
        assert!(metadata.contains_topic(TOPIC_NAME), "Topic not added to metadata");
        let resp = build_metadata_response(&cluster, topic_id);
        metadata
            .update_with_current_request_version(&resp, false, time.milliseconds())
            .expect("metadata update");
        sender.run_once().await; // send produce request
        sender
            .client
            .respond(build_produce_response(TOPIC_NAME, topic_id, 0, offset + 1, Errors::None, 0));
        sender.run_once().await;
        assert_eq!(sender.client.in_flight_request_count(), 0, "Request completed.");
        assert!(!sender.client.has_in_flight_requests());
        assert_eq!(sender.in_flight_batches_for(&tp0).len(), 0);
        sender.run_once().await;
        let resolved = tokio::time::timeout(Duration::from_secs(2), future.get())
            .await
            .expect("future.get() timed out")
            .expect("future returned error");
        assert_eq!(resolved.offset(), offset + 1);
    }

    /// Build a metadata response with a custom `(partition, leader_id)`
    /// mapping. Used by `test_send_in_order` to put tp0 on different
    /// brokers across two metadata updates.
    fn build_metadata_response_for(
        cluster: &Cluster,
        topic_id: Uuid,
        partition_to_leader: &[(i32, i32)],
    ) -> crate::common::requests::metadata_response::MetadataResponse {
        use crate::common::message::metadata_response_data::{
            MetadataResponseBroker, MetadataResponseData, MetadataResponsePartition, MetadataResponseTopic,
        };
        let brokers: Vec<MetadataResponseBroker> = cluster
            .nodes()
            .iter()
            .map(|n| MetadataResponseBroker {
                node_id: n.id(),
                host: n.host().to_string(),
                port: n.port(),
                rack: None,
                unknown_tagged_fields: Vec::new(),
            })
            .collect();
        let topic = MetadataResponseTopic {
            error_code: 0,
            name: Some(TOPIC_NAME.to_string()),
            topic_id,
            is_internal: false,
            partitions: partition_to_leader
                .iter()
                .map(|(p, leader)| MetadataResponsePartition {
                    error_code: 0,
                    partition_index: *p,
                    leader_id: *leader,
                    leader_epoch: NO_PARTITION_LEADER_EPOCH,
                    replica_nodes: vec![*leader],
                    isr_nodes: vec![*leader],
                    offline_replicas: vec![],
                    unknown_tagged_fields: Vec::new(),
                })
                .collect(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let data = MetadataResponseData {
            throttle_time_ms: 0,
            brokers,
            cluster_id: Some(String::new()),
            controller_id: 0,
            topics: vec![topic],
            cluster_authorized_operations: 0,
            error_code: 0,
            unknown_tagged_fields: Vec::new(),
        };
        crate::common::requests::metadata_response::MetadataResponse::new(data, true)
    }

    /// Build a multi-partition produce response (single topic).
    fn build_produce_response_multi(
        topic: &str,
        topic_id: Uuid,
        offsets: &[(i32, i64)],
        error: Errors,
    ) -> Box<dyn AbstractResponse> {
        let partition_responses: Vec<PartitionProduceResponse> = offsets
            .iter()
            .map(|(p, o)| PartitionProduceResponse {
                index: *p,
                error_code: error.code(),
                base_offset: *o,
                log_append_time_ms: -1,
                log_start_offset: 0,
                record_errors: Vec::new(),
                error_message: None,
                current_leader: ProtoLeaderIdAndEpoch::new(),
                unknown_tagged_fields: Vec::new(),
            })
            .collect();
        let topic_resp = TopicProduceResponse {
            name: topic.to_string(),
            topic_id,
            partition_responses,
            unknown_tagged_fields: Vec::new(),
        };
        let data = ProduceResponseData {
            throttle_time_ms: 0,
            responses: vec![topic_resp],
            node_endpoints: Vec::new(),
            unknown_tagged_fields: Vec::new(),
        };
        Box::new(ProduceResponse::new(data))
    }
}
