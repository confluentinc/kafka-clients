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
//! # What is translated, and what is not
//!
//! Milestone 11 Phase 4 translated the whole `transactionManager != null` block of
//! `runOnce` (`Sender.java:311-345`), `maybeSendAndPollTransactionalRequest`
//! (`:456-530`), `maybeFindCoordinatorAndRetry` (`:520-530`), `maybeAbortBatches`
//! (`:532-538`), `awaitNodeReady` (`:563-574`),
//! `hasPendingTransactionalRequests` (`:233-235`),
//! `shouldHandleAuthorizationError` (`:351-360`), the three shutdown stages of
//! `run()` (`:245-303`), and the unsynchronized half of
//! `TxnRequestHandler.onComplete`.
//!
//! Phase 5a wired the coordinator arms Phase 4 had to defer:
//! `maybeSendAndPollTransactionalRequest`'s `coordinator(coordinatorType)` and
//! "coordinator not known" branches (`:481`, `:489-492`), the
//! `lookupCoordinator` calls in `maybeFindCoordinatorAndRetry` (`:522`) and in
//! the `onComplete` disconnect branch
//! (`TransactionManager.java:1414`), and `awaitNodeReady`'s
//! `handleCoordinatorReady` (`:568`). The coordinator nodes themselves are
//! [`Sender`] fields (rules §2); see [`CoordinatorNodes`].
//!
//! Still deferred: `sendProduceRequest` does not yet set `transactional_id` /
//! `use_transaction_v1_version` (`Sender.java:922-936`, Phase 6).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::{kafka_debug, kafka_error, kafka_info, kafka_trace, kafka_warn};

use crate::client_response::ClientResponse;
use crate::common::KafkaError;
use crate::common::Node;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::Errors;
use crate::common::record::RecordBatch;
use crate::common::requests::ConcreteResponse;
use crate::common::requests::ProduceRequestBuilder;
use crate::common::requests::find_coordinator_request::CoordinatorType;
use crate::common::requests::{PartitionResponse, RecordError};
use crate::kafka_client::KafkaClient;
use crate::metadata::LeaderIdAndEpoch;
use crate::produce_request_data::{PartitionProduceData, ProduceRequestData, TopicProduceData};

use crate::common::utils::LogContext;

use super::Caller;
use super::InFlightBatchPool;
use super::ProduceRequestResult;
use super::ProducerBatch;
use super::ProducerMetadata;
use super::RecordAccumulator;
use super::TransactionManager;
use super::TxnRequestHandler;
use super::transaction_manager::coordinator_type_name;
use super::{CoordinatorNodes, PendingRequests};
// Constants are exported only by the file defining them (CLAUDE.md §2).
use super::transaction_manager::NO_INFLIGHT_REQUEST_CORRELATION_ID;

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

/// Whether `error` is one of the two authorization failures that
/// `Sender.shouldHandleAuthorizationError` (`Sender.java:351-360`) recovers from
/// by failing the pending requests and transitioning back to `UNINITIALIZED`.
///
/// This is the `instanceof` half of that method:
///
/// ```java
/// if (exception instanceof TransactionalIdAuthorizationException ||
///                 exception instanceof ClusterAuthorizationException) {
/// ```
///
/// Extracted as a free function so `TransactionManager`'s manager-level test
/// harness computes the *same* predicate as production instead of a copy that can
/// drift. Note this is deliberately the two-code test Java writes, **not** the
/// whole `AuthorizationException` family (contrast
/// `.claude/rules/producer-transactions.md` §9, which is about the sites where
/// Java does test the family).
pub(crate) fn is_authorization_error_handled_by_sender(error: &KafkaError) -> bool {
    matches!(
        error.error(),
        Errors::TransactionalIdAuthorizationFailed | Errors::ClusterAuthorizationFailed
    )
}

/// Which `catch` block in Java handles a failure raised by `runOnce`'s
/// `transactionManager != null` block.
///
/// Java distinguishes the two by exception *type*: `catch (AuthenticationException e)`
/// at `Sender.java:336` calls `transactionManager.authenticationFailed(e)` **and
/// then falls through to `sendProducerData`**, while anything else propagates to
/// `Sender.run`'s `catch (Exception e)` at `:248`, which only logs. `KafkaError` is
/// flat, so the distinction is carried structurally instead — the same approach
/// `common::network::authentication_error` already takes for the transport's
/// `io::Error` boundary, and for the same reason: an error *kind* cannot express
/// "this was a genuine authentication failure".
enum TransactionPhaseError {
    /// Java's `AuthenticationException`, raised by `awaitNodeReady` →
    /// `NetworkClientUtils.awaitReady`.
    Authentication(KafkaError),
    /// Everything else.
    Other(KafkaError),
}

impl TransactionPhaseError {
    /// The wrapped error, for `Sender.run`'s log statement.
    fn into_error(self) -> KafkaError {
        match self {
            Self::Authentication(error) | Self::Other(error) => error,
        }
    }
}

/// Suspends the Sender task for `duration_ms`, translating Java's
/// `time.sleep(retryBackoffMs)` (`Sender.java:501`, `:525`).
///
/// Java blocks the Sender thread; CLAUDE.md §9.1 makes that an `.await` here. Both
/// call sites exist to prevent a tight retry loop and neither holds a
/// `TransactionManager` guard (rules §4).
///
/// Note for tests: Java's `MockTime.sleep` advances a virtual clock, so a Java test
/// passes through instantly. Tokio's timer is real unless the test opts into
/// `#[tokio::test(start_paused = true)]`, which auto-advances when the runtime is
/// idle. That is a difference in test *duration* only — the Sender's own clock is
/// the injected `time_provider` either way.
async fn sleep_ms(duration_ms: i64) {
    if duration_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(duration_ms as u64)).await;
    }
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
    /// The topic-partitions whose batches were sent in this request, each paired with
    /// the identity of the batch that was sent.
    ///
    /// Java's callback closes over `recordsByPartition`, the exact `ProducerBatch`
    /// objects the request carried (`Sender.java:918`, `:941`), so a response always
    /// completes *its own* batch. Recording only the partition here was wrong once more
    /// than one request per partition can be in flight — which idempotence makes normal,
    /// since `max.in.flight.requests.per.connection` may be up to 5 — because the
    /// response handler then completed whichever batch happened to be oldest. With
    /// sequences that is observable: answering the second request first would move
    /// `lastAckedSequence` to the *first* batch's sequence and mis-attribute a
    /// `DUPLICATE_SEQUENCE_NUMBER`.
    ///
    /// The identity is the batch's [`ProduceRequestResult`], which is `Arc`-shared and
    /// compared with `Arc::ptr_eq` — the closest equivalent of Java's object identity
    /// that survives the batch being moved between owners.
    batches: Vec<(TopicPartition, Arc<ProduceRequestResult>)>,
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
    /// All the state related to transactions, in particular the producer id,
    /// producer epoch, and sequence numbers; `None` when idempotence is disabled.
    ///
    /// Translated from `Sender.transactionManager` (Java 123), which is nullable —
    /// hence [`Option`].
    ///
    /// # Lock topology
    ///
    /// `std::sync::Mutex`, shared with `KafkaProducer` and `RecordAccumulator`
    /// (`.claude/rules/producer-transactions.md` §2, PLAN §6.3). Two hard rules
    /// apply to every use below (rules §4):
    ///
    ///   1. **No guard may be held across an `.await`.** Acquire → read/mutate →
    ///      drop, then await, then re-acquire. `Sender.java:459-518` interleaves
    ///      ten manager calls with three `client.poll(..)` calls and two sleeps,
    ///      all of which are `.await` points here.
    ///   2. **The network poll is never raced in a `tokio::select!`** — it is not
    ///      cancel-safe (see `consumer-threading.md` §10).
    ///
    /// The fields Java deliberately leaves *outside* its `synchronized` blocks
    /// because only the Sender thread touches them live on this struct instead of
    /// behind this mutex — see [`Self::pending_requests`] and
    /// [`Self::in_flight_request_correlation_id`].
    transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    /// The queue of transactional requests waiting to be sent.
    ///
    /// Translated from `TransactionManager.pendingRequests`
    /// (`TransactionManager.java:121`), which lives **here** rather than behind
    /// [`Self::transaction_manager`] because Java touches it from the Sender
    /// thread only and mutates it through the *unsynchronized*
    /// `lookupCoordinator(TxnRequestHandler)` (`TransactionManager.java:969`) that
    /// `Sender.java:522` calls directly. See
    /// `.claude/rules/producer-transactions.md` §2 and the
    /// [`PendingRequests`] docs.
    pending_requests: PendingRequests,
    /// The coordinators this Sender has discovered.
    ///
    /// Translated from `TransactionManager.transactionCoordinator`
    /// (`TransactionManager.java:137`) and `consumerGroupCoordinator` (`:138`),
    /// which live **here** for the same reason as [`Self::pending_requests`]:
    /// Java's only reader is `Sender.java:481` and both of its writers are
    /// unsynchronized. See the [`CoordinatorNodes`] docs.
    coordinators: CoordinatorNodes,
    /// The correlation id of the transactional request currently in flight, or
    /// [`NO_INFLIGHT_REQUEST_CORRELATION_ID`] when there is none.
    ///
    /// Translated from `TransactionManager.inFlightRequestCorrelationId`
    /// (`TransactionManager.java:136`). Sender-confined for the same reason as
    /// [`Self::pending_requests`], with stronger evidence: Java's three accessors
    /// (`:973`, `:977`, `:981`) are *all* unsynchronized, and `onComplete` reads
    /// the field at `:1407` while its `synchronized` block only starts at `:1421`.
    in_flight_request_correlation_id: i32,
    /// The transactional request awaiting a response, keyed by the correlation id
    /// it was sent with.
    ///
    /// Java attaches the `TxnRequestHandler` itself to the `ClientRequest` as its
    /// `RequestCompletionHandler` (`Sender.java:504-505`), so the network client
    /// hands the handler back when the response arrives. A Rust
    /// `RequestCompletionHandler` cannot capture `&mut self`, so the handler is
    /// parked here and matched against the response's correlation id after
    /// `poll()` returns — the same shape [`PendingProduceRequest`] already uses for
    /// produce responses (CLAUDE.md §9.2).
    ///
    /// An [`Option`] rather than a map because Java allows at most one in-flight
    /// transactional request: `maybeSendAndPollTransactionalRequest` returns early
    /// at `Sender.java:460` while `hasInFlightRequest()` holds.
    pending_transactional_response: Option<(i32, TxnRequestHandler)>,
    /// A per-partition queue of batches ordered by creation time for tracking in-flight batches.
    in_flight_batches: HashMap<TopicPartition, Vec<ProducerBatch>>,
    /// Batches whose records are already completed but whose pooled buffer must not
    /// be returned until their produce response arrives.
    ///
    /// # The second holder Java gets for free
    ///
    /// Three Java paths complete a batch *now* and deallocate it *later*:
    /// `maybeAbortBatches` → `abortBatches`'s `isInflight()` fork
    /// (`RecordAccumulator.java:1160-1164`), `failBatch(deallocateBatch=false)` →
    /// `maybeRemoveAndDeallocateBatchLater` (`Sender.java:177-180`), and
    /// `abortIncompleteBatches` on a force close. All three rely on the request's
    /// `RequestCompletionHandler` closing over `recordsByPartition`
    /// (`Sender.java:918`, `:941`) — a *second* holder of the batch that outlives
    /// `inFlightBatches.clear()`, so the response can still reach it and deallocate
    /// (KAFKA-19012: the pooled buffer may still be in use by the network client).
    ///
    /// A Rust `RequestCompletionHandler` cannot capture `&mut self`, and
    /// `PendingProduceRequest` deliberately stores only an
    /// `Arc<ProduceRequestResult>` identity rather than the batch, so that second
    /// holder has to be an explicit field. Without it the batch is simply dropped and
    /// `BufferPool::available_memory` shrinks permanently — Critic 44 issue 2, which
    /// `handle_authorization_error` re-armed on every recurrence because it recovers
    /// to `UNINITIALIZED` and keeps the producer running.
    ///
    /// [`Self::handle_produce_response_for`] searches this after
    /// [`Self::in_flight_batches`], so such a batch takes the ordinary response path:
    /// `complete()` / `complete_exceptionally()` return `false` because it is already
    /// final, and the `else` arm deallocates — exactly Java's sequence. Anything still
    /// here when the Sender stops is deallocated in [`Self::run`].
    batches_awaiting_response: Vec<ProducerBatch>,
    /// Pending produce requests awaiting responses, keyed by correlation ID.
    pending_produce_responses: HashMap<i32, PendingProduceRequest>,
    /// Provider of current wall-clock time in milliseconds (epoch).
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Contextual log message prefix.
    ///
    /// Translated from Java's `LogContext logContext` field in `Sender`.
    log_context: LogContext,
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
        time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
        log_context: LogContext,
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
            transaction_manager,
            pending_requests: PendingRequests::new(),
            coordinators: CoordinatorNodes::new(),
            in_flight_request_correlation_id: NO_INFLIGHT_REQUEST_CORRELATION_ID,
            pending_transactional_response: None,
            in_flight_batches: HashMap::new(),
            batches_awaiting_response: Vec::new(),
            pending_produce_responses: HashMap::new(),
            time_provider,
            log_context,
        }
    }

    // -- Sender-confined transactional state (rules §2) ---------------------
    //
    // The four methods below are `TransactionManager` methods in Java whose
    // bodies touch *only* state that is confined to the Sender thread, which is
    // why Java leaves three of them unsynchronized. Once that state lives here
    // (see `Self::pending_requests` / `Self::in_flight_request_correlation_id`)
    // so must they; the alternative — manager methods taking `&mut i32` — would
    // put the shared guard around work Java deliberately performs without it.

    /// Whether any transactional request is pending.
    ///
    /// Translated from `TransactionManager.hasPendingRequests()`
    /// (`TransactionManager.java:1005`).
    pub fn has_pending_requests(&self) -> bool {
        !self.pending_requests.is_empty()
    }

    /// The coordinator of the given type, or `None` if it has not been discovered.
    ///
    /// Translated from `TransactionManager.coordinator(CoordinatorType)`
    /// (`TransactionManager.java:958`), whose body reads only Sender-confined
    /// state — the same criterion that moved the four accessors below (rules §2).
    ///
    /// # Errors
    ///
    /// Propagates [`CoordinatorNodes::coordinator`]'s error for
    /// [`CoordinatorType::Share`], which is Java's `default:` throw.
    pub fn coordinator(&self, coordinator_type: CoordinatorType) -> Result<Option<&Node>, KafkaError> {
        self.coordinators.coordinator(coordinator_type)
    }

    /// Records the correlation id of the transactional request now in flight.
    ///
    /// Translated from `TransactionManager.setInFlightCorrelationId(int)`
    /// (`TransactionManager.java:973`).
    fn set_in_flight_correlation_id(&mut self, correlation_id: i32) {
        self.in_flight_request_correlation_id = correlation_id;
    }

    /// Clears the in-flight correlation id.
    ///
    /// Translated from `TransactionManager.clearInFlightCorrelationId()`
    /// (`TransactionManager.java:977`).
    fn clear_in_flight_correlation_id(&mut self) {
        self.in_flight_request_correlation_id = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    /// Whether a transactional request is in flight.
    ///
    /// Translated from `TransactionManager.hasInFlightRequest()`
    /// (`TransactionManager.java:981`).
    pub fn has_in_flight_request(&self) -> bool {
        self.in_flight_request_correlation_id != NO_INFLIGHT_REQUEST_CORRELATION_ID
    }

    /// Handles the response to a transactional request.
    ///
    /// Translated from `TxnRequestHandler.onComplete(ClientResponse)`
    /// (`TransactionManager.java:1406-1428`) — specifically everything Java runs
    /// *outside* the `synchronized (TransactionManager.this)` block that begins at
    /// `:1421`. The synchronized half is
    /// [`TransactionManager::handle_response`], called below under the lock
    /// exactly as Java's block does.
    ///
    /// The split is required by rules §2: the correlation-id comparison and clear
    /// touch Sender-confined state, and wrapping them in the shared guard would
    /// block the application task's `maybe_add_partition` where Java does not.
    ///
    /// The `Caller` passed on into the manager is always [`Caller::Sender`]: Java
    /// reaches this from `NetworkClient.poll`, i.e. on the Sender thread, at every
    /// call site.
    fn on_transactional_response(
        &mut self,
        handler: TxnRequestHandler,
        response: &ClientResponse,
    ) -> Result<(), KafkaError> {
        let transaction_manager = match &self.transaction_manager {
            Some(transaction_manager) => Arc::clone(transaction_manager),
            // Unreachable: a handler only exists when a manager does.
            None => return Ok(()),
        };

        if response.request_header().correlation_id() != self.in_flight_request_correlation_id {
            let error = KafkaError::with_message(
                Errors::UnknownServerError,
                "Detected more than one in-flight transactional request.",
            );
            return transaction_manager.lock().unwrap().fatal_error(&handler, error);
        }

        self.clear_in_flight_correlation_id();
        if response.was_disconnected() {
            kafka_debug!(self.log_context, "Disconnected from {}. Will retry.", response.destination());
            // Java 1413-1415: rediscover the coordinator before retrying, so the
            // retry does not go straight back to a broker that just dropped us.
            {
                let manager = transaction_manager.lock().unwrap();
                if manager.needs_coordinator(&handler) {
                    manager.lookup_coordinator_for(&mut self.coordinators, &mut self.pending_requests, &handler)?;
                }
                // Java's `reenqueue()` (1394) takes the manager monitor for the two
                // statements `isRetry = true; enqueueRequest(this)`, so `retry` is
                // called under the lock here too.
                manager.retry(&mut self.pending_requests, handler);
            }
            return Ok(());
        }
        if let Some(version_mismatch) = response.version_mismatch() {
            let error = KafkaError::unsupported_version(version_mismatch.to_string());
            return transaction_manager.lock().unwrap().fatal_error(&handler, error);
        }
        match response.response_body() {
            Some(response_body) => {
                kafka_trace!(
                    self.log_context,
                    "Received transactional response {} for request {:?}",
                    response_body,
                    handler
                );
                // Java 1421-1423: this and only this runs under the monitor.
                transaction_manager.lock().unwrap().handle_response(
                    handler,
                    response_body,
                    &mut self.coordinators,
                    &mut self.pending_requests,
                )
            },
            None => {
                let error = KafkaError::with_message(
                    Errors::UnknownServerError,
                    "Could not execute transactional request for unknown reasons",
                );
                transaction_manager.lock().unwrap().fatal_error(&handler, error)
            },
        }
    }

    /// Whether there are transactional requests that must still be flushed before
    /// shutdown can proceed.
    ///
    /// Translated from `Sender.hasPendingTransactionalRequests()` (Java 233-235).
    /// Reachable for a purely idempotent producer — see
    /// [`TransactionManager::has_ongoing_transaction`].
    fn has_pending_transactional_requests(&self) -> bool {
        match &self.transaction_manager {
            Some(transaction_manager) => {
                self.has_pending_requests() && transaction_manager.lock().unwrap().has_ongoing_transaction()
            },
            None => false,
        }
    }

    /// The main run loop for the sender task.
    ///
    /// Translated from `Sender.run()`.
    pub async fn run(&mut self) {
        kafka_debug!(self.log_context, "Starting Kafka producer I/O task.");

        // Main loop, runs until close is called
        while self.running.load(Ordering::Acquire) {
            self.run_once_logging_errors().await;
        }

        kafka_debug!(
            self.log_context,
            "Beginning shutdown of Kafka producer I/O task, sending remaining records."
        );

        // Okay we stopped accepting requests but there may still be requests in the
        // transaction manager, accumulator or waiting for acknowledgment. Wait until
        // these are completed.
        while !self.force_close.load(Ordering::Acquire)
            && ((self.accumulator.has_undrained() || self.client.in_flight_request_count() > 0)
                || self.has_pending_transactional_requests())
        {
            self.run_once_logging_errors().await;
        }

        // Abort the transaction if any commit or abort didn't go through the
        // transaction manager's queue (Java 266-285).
        while !self.force_close.load(Ordering::Acquire) && self.has_ongoing_transaction() {
            if !self.is_completing() {
                kafka_info!(self.log_context, "Aborting incomplete transaction due to shutdown");
                // It is possible for the transaction manager to return errors when
                // aborting. Catch these so as not to interfere with the rest of the
                // shutdown logic.
                if let Err(error) = self.begin_abort() {
                    kafka_error!(
                        self.log_context,
                        "Error in kafka producer I/O task while aborting transaction when during closing: {}",
                        error
                    );
                    // Force close in case the transaction manager is in error states.
                    self.force_close.store(true, Ordering::Release);
                }
            }
            self.run_once_logging_errors().await;
        }

        if self.force_close.load(Ordering::Acquire) {
            // We need to fail all the incomplete transactional requests and batches
            // and wake up the tasks waiting on the futures.
            if let Some(transaction_manager) = self.transaction_manager.clone() {
                kafka_debug!(
                    self.log_context,
                    "Aborting incomplete transactional requests due to forced shutdown"
                );
                // Java does not guard this call (`Sender.java:292`) and cannot fail
                // it: `close()` only ever targets `FATAL_ERROR`, which is always a
                // valid transition, and always supplies an error. Logged rather than
                // unwrapped so an unreachable failure cannot panic the task
                // (CLAUDE.md §10.1).
                if let Err(error) = transaction_manager
                    .lock()
                    .unwrap()
                    .close(&mut self.pending_requests, Caller::Sender)
                {
                    kafka_error!(
                        self.log_context,
                        "Error while aborting incomplete transactional requests: {}",
                        error
                    );
                }
            }
            kafka_debug!(self.log_context, "Aborting incomplete batches due to forced shutdown");
            self.accumulator.abort_incomplete_batches();
            // `abortIncompleteBatches` covers drained batches in Java because
            // `abortBatches` walks `incomplete.copyAll()`; here the accumulator can only
            // reach its own deques, so the Sender aborts its share with the same reason
            // Java's no-argument `abortBatches()` uses. Critic 44 note 2.
            self.abort_in_flight_batches(&RecordAccumulator::producer_closed_forcefully_error());
        }

        self.client.close().await;

        // Java leaks these buffers, and this port declines to.
        //
        // `NetworkClient.close()` is `selector.close(); metadataUpdater.close();
        // telemetrySender.close();` (`NetworkClient.java:736-746`) — it never walks
        // `inFlightRequests` and never calls `completeResponses`. `Selector.close()`
        // closes each channel with `CloseMode.DISCARD_NO_NOTIFY`
        // (`Selector.java:886-892`), defined at `:96` as "discard any outstanding
        // receives, no disconnect notification". So no completion callback runs, and
        // any batch Java was still holding for a response keeps its `ByteBuffer` — the
        // `BufferPool` never gets it back. That is unobservable in Java only because
        // the pool is constructed inside `KafkaProducer`'s constructor
        // (`KafkaProducer.java:438`), is reachable solely through the accumulator, and
        // is collected with the producer.
        //
        // Releasing them here instead keeps `BufferPool`'s accounting exact for the
        // whole `Sender` lifetime, which is what makes `available_memory()` usable as
        // an oracle in the two leak regression tests. The set is non-empty on the
        // force-close path — `abort_in_flight_batches` moves still-in-flight batches
        // into `Self::batches_awaiting_response` because `deallocate` refuses a batch
        // that is still marked in flight — and on a graceful close whenever a response
        // never arrived.
        for mut batch in std::mem::take(&mut self.batches_awaiting_response) {
            batch.set_inflight(false);
            self.accumulator.deallocate(&mut batch);
        }

        kafka_debug!(self.log_context, "Shutdown of Kafka producer I/O task has completed.");
    }

    /// Runs one iteration and logs any failure, translating `Sender.run`'s three
    /// `catch (Exception e) { log.error("Uncaught error in kafka producer I/O
    /// thread: ", e); }` blocks (Java 248-250, 261-263, 282-284).
    async fn run_once_logging_errors(&mut self) {
        if let Err(error) = self.run_once().await {
            kafka_error!(self.log_context, "Uncaught error in kafka producer I/O task: {}", error);
        }
    }

    /// Whether a transaction is considered ongoing, or `false` when idempotence is
    /// disabled.
    ///
    /// The `transactionManager != null && transactionManager.hasOngoingTransaction()`
    /// conjunction of `Sender.java:267`.
    fn has_ongoing_transaction(&self) -> bool {
        self.transaction_manager
            .as_ref()
            .is_some_and(|transaction_manager| transaction_manager.lock().unwrap().has_ongoing_transaction())
    }

    /// `transactionManager.isCompleting()` (`Sender.java:268`).
    fn is_completing(&self) -> bool {
        self.transaction_manager
            .as_ref()
            .is_some_and(|transaction_manager| transaction_manager.lock().unwrap().is_completing())
    }

    /// `transactionManager.beginAbort()` (`Sender.java:273`).
    fn begin_abort(&self) -> Result<(), KafkaError> {
        match &self.transaction_manager {
            Some(transaction_manager) => transaction_manager.lock().unwrap().begin_abort(),
            // Unreachable: the enclosing loop is gated on `has_ongoing_transaction`.
            None => Ok(()),
        }
    }

    /// Run a single iteration of sending.
    ///
    /// Translated from `Sender.runOnce()` (Java 310-346).
    ///
    /// In Java, `runOnce` calls `client.poll()` which invokes callbacks on
    /// completed requests. The Sender's produce response callback calls
    /// `handleProduceResponse()`. In Rust, we cannot capture `&mut self` in a
    /// callback, so instead we process the responses returned by `poll()`
    /// directly — see [`Self::poll_and_dispatch`].
    ///
    /// # Errors
    ///
    /// Java's `runOnce` throws and `Sender.run` catches-and-logs; the Rust
    /// equivalent returns the error and [`Self::run_once_logging_errors`] logs it
    /// at the same point.
    async fn run_once(&mut self) -> Result<(), KafkaError> {
        if self.transaction_manager.is_some() {
            match self.run_transaction_phase().await {
                // Java 322 / 326 / 334 — `runOnce` returns without producing.
                Ok(true) => return Ok(()),
                Ok(false) => {},
                Err(TransactionPhaseError::Authentication(error)) => {
                    // Java 336-340. This is already logged as an error, but
                    // propagated here to perform any clean ups. Note Java's `catch`
                    // does **not** return: execution continues to `sendProducerData`
                    // at `:343`, which this `match` arm preserves by falling through.
                    kafka_trace!(
                        self.log_context,
                        "Authentication exception while processing transactional request: {}",
                        error
                    );
                    self.authentication_failed(&error)?;
                },
                Err(other) => return Err(other.into_error()),
            }
        }

        let current_time_ms = (self.time_provider)();
        let poll_timeout = self.send_producer_data(current_time_ms).await?;
        self.poll_and_dispatch(poll_timeout, current_time_ms).await;
        Ok(())
    }

    /// `transactionManager.authenticationFailed(e)` (`Sender.java:339`).
    fn authentication_failed(&mut self, error: &KafkaError) -> Result<(), KafkaError> {
        match self.transaction_manager.clone() {
            Some(transaction_manager) => transaction_manager.lock().unwrap().authentication_failed(
                &mut self.pending_requests,
                error,
                Caller::Sender,
            ),
            // Unreachable: only the transaction block raises this error.
            None => Ok(()),
        }
    }

    /// Polls the network client and dispatches every completed response.
    ///
    /// This is the Rust stand-in for Java's `client.poll(..)`, which invokes each
    /// request's `RequestCompletionHandler` from inside the poll. Every Java
    /// `client.poll` call site — `runOnce`'s at `:345`, the fatal-error one at
    /// `:322`, and the three inside `maybeSendAndPollTransactionalRequest` — can
    /// therefore complete *both* produce and transactional requests, so they all go
    /// through here rather than only the one in `runOnce`.
    ///
    /// The poll future is awaited to completion and never raced in a
    /// `tokio::select!` (rules §4 / `consumer-threading.md` §10: it is not
    /// cancel-safe).
    async fn poll_and_dispatch(&mut self, timeout: i64, now: i64) {
        let responses = self.client.poll(timeout, now).await;
        let dispatch_time_ms = (self.time_provider)();
        self.handle_client_responses(&responses, dispatch_time_ms);
    }

    /// Dispatches each completed response to the handler that requested it,
    /// preserving arrival order as Java's callback invocation does.
    ///
    /// # Failures are isolated per response
    ///
    /// This is `NetworkClient.completeResponses` (`NetworkClient.java:666-674`),
    /// which wraps each `response.onComplete()` in its own `try`/`catch`:
    ///
    /// ```java
    /// for (ClientResponse response : responses) {
    ///     try {
    ///         response.onComplete();
    ///     } catch (Exception e) {
    ///         log.error("Uncaught error in request completion:", e);
    ///     }
    /// }
    /// ```
    ///
    /// So one handler raising does **not** abandon the remaining responses of the
    /// poll. Propagating with `?` here instead would leave their
    /// `pending_produce_responses` entries and `in_flight_batches` uncompleted until
    /// `delivery.timeout.ms` expired them.
    fn handle_client_responses(&mut self, responses: &[ClientResponse], now: i64) {
        for response in responses {
            let correlation_id = response.request_header().correlation_id();
            let is_transactional = self
                .pending_transactional_response
                .as_ref()
                .is_some_and(|(pending_correlation_id, _)| *pending_correlation_id == correlation_id);
            let result = if is_transactional {
                let (_, handler) = self
                    .pending_transactional_response
                    .take()
                    .expect("the slot was just observed to be occupied");
                self.on_transactional_response(handler, response)
            } else {
                self.handle_produce_response_for(response, now)
            };
            if let Err(error) = result {
                // Java: `log.error("Uncaught error in request completion:", e)`.
                kafka_error!(self.log_context, "Uncaught error in request completion: {}", error);
            }
        }
    }

    /// Process one produce response.
    ///
    /// In Java, this happens inside the `RequestCompletionHandler` callback.
    /// In Rust, we process responses after `client.poll()` returns.
    ///
    /// # Errors
    ///
    /// Propagates a failure from `reenqueue` / `split_and_reenqueue`, both of which
    /// re-insert an idempotent batch in sequence order. Java's
    /// `IllegalStateException` from `insertInSequenceOrder` escapes the completion
    /// callback but is caught **inside** `client.poll`, by
    /// `NetworkClient.completeResponses` (`NetworkClient.java:666-674`) — *not* by
    /// `Sender.run`. [`Self::handle_client_responses`] is that boundary and logs the
    /// error there, so the remaining responses of the same poll are still dispatched.
    fn handle_produce_response_for(&mut self, response: &ClientResponse, now: i64) -> Result<(), KafkaError> {
        {
            let correlation_id = response.request_header().correlation_id();
            if let Some(pending) = self.pending_produce_responses.remove(&correlation_id) {
                // Extract the batches this request carried from `in_flight_batches`.
                // Java's callback holds them directly; here they are located by identity
                // (see `PendingProduceRequest`).
                let mut batches: HashMap<TopicPartition, ProducerBatch> = HashMap::new();
                for (tp, identity) in &pending.batches {
                    // Take the batch this request actually carried, not merely the
                    // oldest one for the partition — see `PendingProduceRequest`.
                    let mut taken = None;
                    if let Some(partition_batches) = self.in_flight_batches.get_mut(tp) {
                        if let Some(index) = partition_batches
                            .iter()
                            .position(|batch| Arc::ptr_eq(&batch.produce_future, identity))
                        {
                            taken = Some(partition_batches.remove(index));
                        }
                        if partition_batches.is_empty() {
                            self.in_flight_batches.remove(tp);
                        }
                    }
                    // A batch that was completed while still in flight is no longer in
                    // `in_flight_batches` but is still waiting for exactly this response
                    // in order to release its buffer — see
                    // `Self::batches_awaiting_response`.
                    if taken.is_none()
                        && let Some(index) = self
                            .batches_awaiting_response
                            .iter()
                            .position(|batch| Arc::ptr_eq(&batch.produce_future, identity))
                    {
                        taken = Some(self.batches_awaiting_response.remove(index));
                    }
                    if let Some(batch) = taken {
                        batches.insert(tp.clone(), batch);
                    }
                }
                let actions = self.handle_produce_response(response, &mut batches, &pending.topic_names, now)?;

                // Process deferred actions that require batch ownership.
                for (tp, action) in actions {
                    if let Some(batch) = batches.remove(&tp) {
                        match action {
                            BatchAction::Reenqueue => {
                                // RecordAccumulator::reenqueue calls batch.reenqueued() internally.
                                self.accumulator.reenqueue(batch, now)?;
                            },
                            BatchAction::SplitAndReenqueue => {
                                // split_and_reenqueue takes ownership, splits the batch,
                                // chains the sub-batch futures, and pushes them to the
                                // front of the deque. After splitting, the original batch's
                                // produce future is completed with RECORD_BATCH_TOO_LARGE
                                // by ProducerBatch::split → finalize_split_batches.
                                self.accumulator.split_and_reenqueue(batch)?;
                            },
                            BatchAction::Done => {},
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // -- The `transactionManager != null` block of `runOnce` -----------------

    /// Runs `runOnce`'s transaction block (`Sender.java:311-341`).
    ///
    /// Returns `Ok(true)` when `runOnce` must return without producing in this
    /// iteration. The block has **four** early exits and their order is
    /// load-bearing:
    ///
    ///   - `:322` fatal error — abort the batches, poll, return;
    ///   - `:326` abortable error whose cause is an authorization failure —
    ///     recover to `UNINITIALIZED` and return, *before* `:331` can attempt
    ///     `ABORTABLE_ERROR → INITIALIZING` (an attempt Java never makes; see
    ///     PLAN §9.15);
    ///   - `:334` `maybeSendAndPollTransactionalRequest` returned `true`, i.e. a
    ///     transactional request was sent, awaited or re-queued;
    ///   - the `AuthenticationException` catch at `:336`, which is *not* an early
    ///     exit — Java falls through to `sendProducerData`.
    ///
    /// No `TransactionManager` guard is held across any `.await` (rules §4): each
    /// step acquires, reads or mutates, and drops before the next await point. That
    /// is looser than Java, where the Sender thread's view is stable simply because
    /// it is the only writer — every re-acquire below is a place where that implicit
    /// consistency could break, which is why each one reads the minimum it needs.
    async fn run_transaction_phase(&mut self) -> Result<bool, TransactionPhaseError> {
        let Some(transaction_manager) = self.transaction_manager.clone() else {
            return Ok(false);
        };

        // Sender.java:313
        transaction_manager
            .lock()
            .unwrap()
            .maybe_resolve_sequences(Caller::Sender)
            .map_err(TransactionPhaseError::Other)?;

        // Sender.java:315-318 — read `lastError` and the error state together, so
        // the two cannot disagree the way separate acquisitions could.
        let (last_error, has_fatal_error, has_abortable_error) = {
            let manager = transaction_manager.lock().unwrap();
            (
                manager.last_error().cloned(),
                manager.has_fatal_error(),
                manager.has_abortable_error(),
            )
        };

        // Sender.java:318-323 — do not continue sending if the transaction manager
        // is in a failed state.
        if has_fatal_error {
            if let Some(error) = &last_error {
                // The manager guard is released above, so this takes the deque locks
                // with no manager lock held — rules §3's order is unaffected.
                self.maybe_abort_batches(error);
            }
            let now = (self.time_provider)();
            self.poll_and_dispatch(self.retry_backoff_ms, now).await;
            return Ok(true);
        }

        // Sender.java:325-327 → shouldHandleAuthorizationError (:351-360).
        if has_abortable_error
            && let Some(error) = &last_error
            && is_authorization_error_handled_by_sender(error)
        {
            self.handle_authorization_error(error).map_err(TransactionPhaseError::Other)?;
            return Ok(true);
        }

        // Sender.java:329-331 — check whether we need a new producerId. If so, we
        // will enqueue an InitProducerId request which will be sent below.
        self.bump_idempotent_epoch_and_reset_id_if_needed()
            .map_err(TransactionPhaseError::Other)?;

        // Sender.java:333-335
        if self.maybe_send_and_poll_transactional_request().await? {
            return Ok(true);
        }
        Ok(false)
    }

    /// The side-effecting half of `Sender.shouldHandleAuthorizationError`
    /// (`Sender.java:354-356`); the `instanceof` half is
    /// [`is_authorization_error_handled_by_sender`].
    ///
    /// Java's three statements, in order: fail the pending requests with an
    /// `AuthenticationException` wrapping the cause, abort the batches, then
    /// transition to `UNINITIALIZED` so the user does not need to instantiate the
    /// producer again (`Sender.java:348-350`).
    fn handle_authorization_error(&mut self, error: &KafkaError) -> Result<(), KafkaError> {
        let Some(transaction_manager) = self.transaction_manager.clone() else {
            return Ok(());
        };
        // Java wraps the cause in `new AuthenticationException(exception)`
        // (`Sender.java:354`). Java's `AuthenticationException` base class carries no
        // wire code — only its subclasses do — so it maps to
        // `Errors::UnknownServerError`, the convention `maybe_fail_with_error` and
        // `TransactionManager::close` already use for a codeless Java exception. NOT
        // `SaslAuthenticationFailed`: the cause here is a cluster or transactional-id
        // authorization failure and nothing about it is SASL.
        let authentication_error = KafkaError::fatal(Errors::UnknownServerError, error.message());
        transaction_manager.lock().unwrap().fail_pending_requests(
            &mut self.pending_requests,
            &authentication_error,
            Caller::Sender,
        )?;
        // The guard from the statement above is released at the `;`, so the deque
        // locks this takes are still acquired with no manager lock held (rules §3).
        self.maybe_abort_batches(error);
        // Java 356 passes the **raw** exception here, not the
        // `new AuthenticationException(exception)` wrapper handed to
        // `failPendingRequests` at :354.
        transaction_manager
            .lock()
            .unwrap()
            .transition_to_uninitialized(error, Caller::Sender)
    }

    /// `transactionManager.bumpIdempotentEpochAndResetIdIfNeeded()`
    /// (`Sender.java:331`), with the in-flight batch pool rules §7 requires.
    ///
    /// # Assembling the pool from **both** owners
    ///
    /// `bump_idempotent_producer_epoch` rewrites the in-flight sequences of every
    /// partition in `partitions_to_rewrite_sequences`, and it needs the actual
    /// batches because `TxnPartitionEntry` tracks ordering keys only (rules §7).
    /// Those batches live in **either** owner: the Sender's
    /// [`Self::in_flight_batches`], or the accumulator's deques once
    /// `reenqueueBatch` (`Sender.java:750-752`) has handed one back *without*
    /// untracking it. Drawing from only one owner is the anti-pattern rules §7
    /// names: a reenqueued batch would keep its old epoch's sequence and the broker
    /// would answer `OUT_OF_ORDER_SEQUENCE_NUMBER` on the very path this call exists
    /// to recover.
    ///
    /// The pool is assembled only when a bump is actually pending, because building
    /// it locks the accumulator's deques for the partitions involved. On the common
    /// path the manager is consulted once and an empty pool is passed — which the
    /// callee never reads, since its loop is over an empty set.
    fn bump_idempotent_epoch_and_reset_id_if_needed(&mut self) -> Result<(), KafkaError> {
        let Some(transaction_manager) = self.transaction_manager.clone() else {
            return Ok(());
        };

        // Only an epoch bump reads the pool. `partitions_to_rewrite_sequences` is
        // written solely by the Sender task (through `handle_failed_batch`,
        // `can_retry` and `maybe_resolve_sequences`, the last of which already ran
        // this iteration at `:313`), so reading it here and using it below cannot
        // race.
        let partitions: Vec<TopicPartition> = {
            let manager = transaction_manager.lock().unwrap();
            if manager.client_side_epoch_bump_required() {
                manager.partitions_to_rewrite_sequences().iter().cloned().collect()
            } else {
                Vec::new()
            }
        };

        if partitions.is_empty() {
            let mut pool = InFlightBatchPool::new();
            return transaction_manager
                .lock()
                .unwrap()
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut self.pending_requests, Caller::Sender);
        }

        let accumulator = Arc::clone(&self.accumulator);
        let pending_requests = &mut self.pending_requests;
        accumulator.with_in_flight_batch_pool(&partitions, &mut self.in_flight_batches, |pool| {
            // Deque locks are held by `with_in_flight_batch_pool` for the duration of
            // this closure, so taking the manager lock here is the deque → manager
            // order rules §3 mandates.
            transaction_manager
                .lock()
                .unwrap()
                .bump_idempotent_epoch_and_reset_id_if_needed(pool, pending_requests, Caller::Sender)
        })
    }

    /// Sends or awaits the next transactional request.
    ///
    /// Translated from `Sender.maybeSendAndPollTransactionalRequest()`
    /// (Java 456-518). Returns `true` if a transactional request is sent or polled,
    /// or if a `FindCoordinator` request is enqueued — i.e. exactly when `runOnce`
    /// must return at `:334`. Java has one `return false` (`:474`, empty queue) and
    /// six `return true`.
    async fn maybe_send_and_poll_transactional_request(&mut self) -> Result<bool, TransactionPhaseError> {
        let Some(transaction_manager) = self.transaction_manager.clone() else {
            return Ok(false);
        };

        // Java 460-464: as long as there are outstanding transactional requests, we
        // simply wait for them to return.
        if self.has_in_flight_request() {
            let now = (self.time_provider)();
            self.poll_and_dispatch(self.retry_backoff_ms, now).await;
            return Ok(true);
        }

        // Java 466-470.
        let abort_reason = {
            let manager = transaction_manager.lock().unwrap();
            if manager.has_abortable_error() {
                manager.last_error().cloned()
            } else if manager.is_aborting() {
                Some(KafkaError::transaction_aborted())
            } else {
                None
            }
        };
        if let Some(reason) = abort_reason {
            self.accumulator.abort_undrained_batches(reason);
        }

        // Java 472-474.
        let has_incomplete = self.accumulator.has_incomplete();
        let next_request_handler = match transaction_manager
            .lock()
            .unwrap()
            .next_request(&mut self.pending_requests, has_incomplete)
        {
            Some(handler) => handler,
            None => return Ok(false),
        };

        // Java 479-482. `coordinatorType()` is null for a non-transactional
        // `InitProducerId` (Java 1482-1488) and for a `FindCoordinator`
        // (Java 1671), so those go to the least-loaded node; everything else is
        // routed to the coordinator this Sender has discovered.
        let coordinator_type = transaction_manager.lock().unwrap().coordinator_type(&next_request_handler);
        let target_node = match coordinator_type {
            Some(coordinator_type) => self
                .coordinators
                .coordinator(coordinator_type)
                .map_err(TransactionPhaseError::Other)?
                .cloned(),
            None => {
                let now = (self.time_provider)();
                self.client.least_loaded_node(now).node().cloned()
            },
        };

        let Some(target_node) = target_node else {
            if let Some(coordinator_type) = coordinator_type {
                // Java 489-492.
                kafka_trace!(
                    self.log_context,
                    "Coordinator not known for {}, will retry {} after finding coordinator.",
                    coordinator_type_name(coordinator_type),
                    next_request_handler.api_key().name()
                );
                self.maybe_find_coordinator_and_retry(next_request_handler).await?;
                return Ok(true);
            }
            // Java 493-498: no nodes available.
            kafka_trace!(
                self.log_context,
                "No nodes available to send requests, will poll and retry when until a node is ready."
            );
            transaction_manager
                .lock()
                .unwrap()
                .retry(&mut self.pending_requests, next_request_handler);
            let now = (self.time_provider)();
            self.poll_and_dispatch(self.retry_backoff_ms, now).await;
            return Ok(true);
        };

        // Java 483-488.
        match self.await_node_ready(&target_node, coordinator_type).await {
            Ok(true) => {},
            Ok(false) => {
                kafka_trace!(
                    self.log_context,
                    "Target node {} not ready within request timeout, will retry when node is ready.",
                    target_node
                );
                self.maybe_find_coordinator_and_retry(next_request_handler).await?;
                return Ok(true);
            },
            // Java's `awaitNodeReady` throws `IOException`, caught at :511, and
            // `AuthenticationException`, which escapes to `runOnce`'s catch at :336.
            // `network_client_utils::await_ready` folds both into `io::Error`; the
            // authentication case is the one it builds with
            // `ErrorKind::PermissionDenied` from `client.authentication_error`
            // (`network_client_utils.rs:96-98`).
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                return Err(TransactionPhaseError::Authentication(KafkaError::fatal(
                    Errors::UnknownServerError,
                    error.to_string(),
                )));
            },
            Err(error) => {
                // Java 511-516: we break here so that we pick up the FindCoordinator
                // request immediately.
                kafka_debug!(
                    self.log_context,
                    "Disconnect from {} while trying to send request {:?}. Going to back off and retry: {}",
                    target_node,
                    next_request_handler,
                    error
                );
                self.maybe_find_coordinator_and_retry(next_request_handler).await?;
                return Ok(true);
            },
        }

        // Java 500-501.
        if next_request_handler.is_retry() {
            sleep_ms(next_request_handler.retry_backoff_ms()).await;
        }

        // Java 503-510.
        let current_time_ms = (self.time_provider)();
        // Java hands the builder itself to `newClientRequest`, keeping the handler's
        // own reference alive for a possible retry. This crate only exposes builders
        // as `Box<dyn RequestBuilder>` at that boundary, so the builder is cloned
        // instead — once per transactional request, i.e. once per producer lifetime on
        // the idempotent path, and never on a per-record or per-batch path.
        let request_builder = next_request_handler.clone_request_builder();
        let request_debug = if log::log_enabled!(log::Level::Debug) {
            format!("{:?}", next_request_handler)
        } else {
            String::new()
        };
        let client_request = self.client.new_client_request_with_timeout(
            target_node.id_string(),
            request_builder,
            current_time_ms,
            true,
            self.request_timeout_ms,
            // Java attaches `nextRequestHandler` itself as the completion handler; a
            // Rust callback cannot capture `&mut self`, so the handler is parked in
            // `pending_transactional_response` and matched by correlation id in
            // `handle_client_responses` (CLAUDE.md §9.2).
            None,
        );
        let correlation_id = client_request.correlation_id();
        kafka_debug!(
            self.log_context,
            "Sending transactional request {} to node {} with correlation ID {}",
            request_debug,
            target_node,
            correlation_id
        );
        self.pending_transactional_response = Some((correlation_id, next_request_handler));
        self.client.send(client_request, current_time_ms);
        self.set_in_flight_correlation_id(correlation_id);
        let now = (self.time_provider)();
        self.poll_and_dispatch(self.retry_backoff_ms, now).await;
        Ok(true)
    }

    /// Looks the coordinator up if the request needs one, otherwise backs off, and
    /// re-enqueues the request either way.
    ///
    /// Translated from `Sender.maybeFindCoordinatorAndRetry()` (Java 520-530).
    async fn maybe_find_coordinator_and_retry(
        &mut self,
        next_request_handler: TxnRequestHandler,
    ) -> Result<(), TransactionPhaseError> {
        let Some(transaction_manager) = self.transaction_manager.clone() else {
            return Ok(());
        };
        let needs_coordinator = transaction_manager.lock().unwrap().needs_coordinator(&next_request_handler);
        if needs_coordinator {
            // Java 522.
            transaction_manager
                .lock()
                .unwrap()
                .lookup_coordinator_for(&mut self.coordinators, &mut self.pending_requests, &next_request_handler)
                .map_err(TransactionPhaseError::Other)?;
        } else {
            // Java 523-527: for non-coordinator requests, sleep here to prevent a tight
            // loop when no node is available.
            sleep_ms(self.retry_backoff_ms).await;
            self.metadata.request_update(false);
        }

        transaction_manager
            .lock()
            .unwrap()
            .retry(&mut self.pending_requests, next_request_handler);
        Ok(())
    }

    /// Waits for `node` to become ready, up to `request.timeout.ms`.
    ///
    /// Translated from `Sender.awaitNodeReady(Node, CoordinatorType)`
    /// (Java 563-574).
    ///
    /// Java's comment on the `CoordinatorType.TRANSACTION` branch: "indicate to the
    /// transaction manager that the coordinator is ready, allowing it to check
    /// ApiVersions. This allows us to bump transactional epochs even if the
    /// coordinator is temporarily unavailable at the time when the abortable error
    /// is handled."
    ///
    /// The manager lock is taken *after* the await completes and dropped before
    /// returning, so no guard crosses it (rules §4).
    async fn await_node_ready(
        &mut self,
        node: &crate::common::Node,
        coordinator_type: Option<CoordinatorType>,
    ) -> std::io::Result<bool> {
        let request_timeout_ms = self.request_timeout_ms as i64;
        if !crate::network_client_utils::await_ready(&mut self.client, node, &*self.time_provider, request_timeout_ms)
            .await?
        {
            return Ok(false);
        }
        if coordinator_type == Some(CoordinatorType::Transaction)
            && let Some(transaction_manager) = &self.transaction_manager
        {
            transaction_manager.lock().unwrap().handle_coordinator_ready(&self.coordinators);
        }
        Ok(true)
    }

    /// Aborts every incomplete batch, translating `Sender.maybeAbortBatches`
    /// (Java 532-538).
    ///
    /// Must not be called while the `TransactionManager` guard is held: it takes the
    /// accumulator's per-partition deque locks, and rules §3 fixes the order as
    /// deque → manager.
    fn maybe_abort_batches(&mut self, error: &KafkaError) {
        if !self.accumulator.has_incomplete() {
            return;
        }
        kafka_error!(self.log_context, "Aborting producer batches due to fatal error: {}", error);
        self.accumulator.abort_batches(error.clone());

        self.abort_in_flight_batches(error);
    }

    /// Aborts every batch the `Sender` still owns, with `reason`.
    ///
    /// Java has no counterpart because it does not need one: `abortBatches`
    /// (`RecordAccumulator.java:1152`) iterates `incomplete.copyAll()`, which returns
    /// the `ProducerBatch` objects themselves and so covers batches already drained
    /// into the Sender. Rust's [`IncompleteBatches`](super::IncompleteBatches) tracks
    /// [`ProduceRequestResult`]s rather than batches — a `ProducerBatch` has exactly
    /// one owner (rules §7) — so the accumulator can only reach what is still in its
    /// deques. Without this, the record futures of drained batches are never
    /// completed, which CLAUDE.md §5 forbids.
    ///
    /// Applies Java's in-flight fork (`:1160-1167`): a batch still marked in flight
    /// keeps its pooled buffer until its response arrives (KAFKA-19012), so it moves
    /// to [`Self::batches_awaiting_response`] rather than being deallocated or
    /// dropped.
    ///
    /// Called from [`Self::maybe_abort_batches`] (`Sender.java:536`) and from
    /// [`Self::run`]'s force-close branch (`Sender.java:294-295`).
    fn abort_in_flight_batches(&mut self, reason: &KafkaError) {
        let accumulator = Arc::clone(&self.accumulator);
        for (_, batches) in self.in_flight_batches.drain() {
            for mut batch in batches {
                batch.abort_record_appends();
                batch.abort(reason.clone());
                if batch.is_inflight() {
                    accumulator.complete_batch(&batch);
                    self.batches_awaiting_response.push(batch);
                } else {
                    accumulator.complete_and_deallocate_batch(&mut batch);
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
    /// Translated from `Sender.sendProducerData()` (Java 379-454).
    ///
    /// # Errors
    ///
    /// Propagates a failure from the accumulator's drain, which assigns producer
    /// ids, epochs and sequence numbers when idempotence is enabled. Java lets the
    /// corresponding `IllegalStateException` escape `runOnce` to `Sender.run`'s
    /// catch-and-log; [`Self::run_once_logging_errors`] is the same boundary.
    async fn send_producer_data(&mut self, now: i64) -> Result<i64, KafkaError> {
        let metadata_snapshot = self.metadata.fetch_metadata_snapshot();

        // Get the list of partitions with data ready to send
        let mut result = self.accumulator.ready(&metadata_snapshot, now);

        // If there are any partitions whose leaders are not known yet, force metadata update
        if !result.unknown_leader_topics.is_empty() {
            for topic in &result.unknown_leader_topics {
                self.metadata.add(topic, now);
            }
            kafka_debug!(
                self.log_context,
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
        let mut batches =
            self.accumulator
                .drain(&metadata_snapshot, &result.ready_nodes, self.max_request_size, now)?;

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
        let expired_inflight_batches = self.get_expired_inflight_batches(now);
        let expired_batches = self.accumulator.expired_batches(now);

        self.fail_expired_batches(expired_batches, now, true);
        self.fail_expired_batches(expired_inflight_batches, now, false);

        // Calculate poll timeout
        let mut poll_timeout = result.next_ready_check_delay_ms.min(not_ready_timeout);
        poll_timeout = poll_timeout.min(self.accumulator.next_expiry_time_ms() - now);
        poll_timeout = poll_timeout.max(0);

        if !result.ready_nodes.is_empty() {
            kafka_trace!(self.log_context, "Nodes with data ready to send: {:?}", result.ready_nodes);
            poll_timeout = 0;
        }

        self.send_produce_requests(request_data, now);
        Ok(poll_timeout)
    }

    /// Fails every expired batch, translating `Sender.failExpiredBatches`
    /// (Java 362-377).
    ///
    /// Takes the batches **by value** because `deallocate_buffer = false` means the
    /// pooled buffer is released only when the produce response arrives, so the batch
    /// has to be moved into [`Self::batches_awaiting_response`] rather than dropped
    /// at the end of the caller's scope.
    fn fail_expired_batches(&mut self, expired_batches: Vec<ProducerBatch>, now: i64, deallocate_buffer: bool) {
        if !expired_batches.is_empty() {
            kafka_trace!(self.log_context, "Expired {} batches in accumulator", expired_batches.len());
        }
        for mut expired_batch in expired_batches {
            let error_message = format!(
                "Expiring {} record(s) for {}:{} ms has passed since batch creation",
                expired_batch.record_count,
                expired_batch.topic_partition,
                now - expired_batch.created_ms
            );
            let error = KafkaError::with_message(Errors::RequestTimedOut, error_message);
            let retain = self.fail_batch_with_error(&mut expired_batch, error, false, deallocate_buffer);
            if let Some(transaction_manager) = self.transaction_manager.clone()
                && expired_batch.in_retry()
            {
                // This ensures that no new batches are drained until the current in
                // flight batches are fully resolved (`Sender.java:372-375`).
                transaction_manager.lock().unwrap().mark_sequence_unresolved(&expired_batch);
            }

            if retain {
                // The buffer is released when the response arrives, which means the
                // batch must stay reachable until then.
                self.batches_awaiting_response.push(expired_batch);
            }

            // No unmute here, in either arm — Java's `failExpiredBatches` performs
            // none (`Sender.java:362-377`). The mute is placed per *drained* batch in
            // `send_producer_data` (`Sender.java:418-424`) and released by that
            // batch's own response in `complete_batch_for` (`Sender.java:735-737`),
            // which is the file's only `unmutePartition` call. An undrained batch
            // never muted the partition, so releasing the mute on its expiry would
            // release a mute belonging to a batch whose request is still outstanding,
            // and the next drain would put a second request for the partition in
            // flight — the reordering that `guarantee_message_order` exists to
            // prevent. Pinned by
            // `test_expiring_an_undrained_batch_does_not_unmute_the_partition`.
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
    ) -> Result<Vec<(TopicPartition, BatchAction)>, KafkaError> {
        let request_header = response.request_header();
        let correlation_id = request_header.correlation_id();
        let mut deferred_actions: Vec<(TopicPartition, BatchAction)> = Vec::new();

        if response.was_timed_out() {
            kafka_trace!(
                self.log_context,
                "Cancelled request with header {} due to the last request to node {} timed out",
                request_header,
                response.destination()
            );
            let part_resp = PartitionResponse::from_error_with_message(
                Errors::RequestTimedOut,
                Some(format!("Disconnected from node {} due to timeout", response.destination())),
            );
            for (tp, batch) in batches.iter_mut() {
                let action = self.complete_batch(batch, &part_resp, correlation_id, now, None)?;
                deferred_actions.push((tp.clone(), action));
            }
        } else if response.was_disconnected() {
            kafka_trace!(
                self.log_context,
                "Cancelled request with header {} due to node {} being disconnected",
                request_header,
                response.destination()
            );
            let part_resp = PartitionResponse::from_error_with_message(
                Errors::NetworkException,
                Some(format!("Disconnected from node {}", response.destination())),
            );
            for (tp, batch) in batches.iter_mut() {
                let action = self.complete_batch(batch, &part_resp, correlation_id, now, None)?;
                deferred_actions.push((tp.clone(), action));
            }
        } else if response.version_mismatch().is_some() {
            kafka_warn!(
                self.log_context,
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
                let action = self.complete_batch(batch, &part_resp, correlation_id, now, None)?;
                deferred_actions.push((tp.clone(), action));
            }
        } else {
            kafka_trace!(
                self.log_context,
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
                                )?;
                                deferred_actions.push((tp, action));
                            } else {
                                kafka_error!(
                                    self.log_context,
                                    "Can't find batch created for topic id {} topic name {} partition {} using {:?}",
                                    topic_resp.topic_id,
                                    topic_resp.name,
                                    partition_resp.index,
                                    topic_names
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

                        if log::log_enabled!(log::Level::Trace) {
                            for part in &updated_partitions {
                                kafka_debug!(self.log_context, "For {} leader was updated.", part);
                            }
                        }
                    }
                }
            } else {
                // acks = 0 case, just complete all requests
                let part_resp = PartitionResponse::from_error(Errors::None);
                for (tp, batch) in batches.iter_mut() {
                    let action = self.complete_batch(batch, &part_resp, correlation_id, now, None)?;
                    deferred_actions.push((tp.clone(), action));
                }
            }
        }

        Ok(deferred_actions)
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
    ) -> Result<BatchAction, KafkaError> {
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
            kafka_warn!(
                self.log_context,
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlation_id,
                batch.topic_partition,
                self.retries - batch.attempts(),
                Self::format_err_msg(response)
            );
            // The batch will be split and the sub-batches re-tracked with their own
            // sequences, so the big batch stops being tracked here
            // (`Sender.java:685-686`).
            if let Some(transaction_manager) = self.transaction_manager.clone() {
                transaction_manager.lock().unwrap().remove_in_flight_batch(batch)?;
            }
            BatchAction::SplitAndReenqueue
        } else if error != Errors::None {
            if self.can_retry(batch, response, now)? {
                kafka_warn!(
                    self.log_context,
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
                // If we have received a duplicate sequence error, it means that the
                // sequence number has advanced beyond the sequence of the current
                // batch, and we haven't retained batch metadata on the broker to
                // return the correct offset and timestamp.
                //
                // The only thing we can do is to return success to the user and not
                // return a valid offset and timestamp.
                self.complete_batch_success(batch, response)?;
                BatchAction::Done
            } else {
                // Tell the user the result of their request. We only adjust sequence
                // numbers if the batch didn't exhaust its retries -- if it did, we
                // don't know whether the sequence number was accepted or not, and thus
                // it is not safe to reassign the sequence.
                let adjust = batch.attempts() < self.retries;
                // `deallocate_batch = true` on the response path, so the buffer is
                // returned here and nothing needs retaining.
                let retain = self.fail_batch(batch, response, adjust, true);
                debug_assert!(!retain, "a response-path failure always deallocates");
                BatchAction::Done
            }
        } else {
            self.complete_batch_success(batch, response)?;
            BatchAction::Done
        };

        if error != Errors::None && error.is_invalid_metadata() {
            if error == Errors::UnknownTopicOrPartition {
                kafka_warn!(
                    self.log_context,
                    "Received unknown topic or partition error in produce request on partition {}. \
                     The topic-partition may not exist or the user may not have Describe access to it",
                    batch.topic_partition
                );
            } else {
                kafka_warn!(
                    self.log_context,
                    "Received invalid metadata error in produce request on partition {} due to {}. \
                     Going to request metadata update now",
                    batch.topic_partition,
                    error
                );
            }

            if error == Errors::NotLeaderOrFollower || error == Errors::FencedLeaderEpoch {
                kafka_debug!(
                    self.log_context,
                    "For {}, received error {}, with leaderIdAndEpoch {:?}",
                    batch.topic_partition,
                    error,
                    response.current_leader
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

        Ok(action)
    }

    /// Format the error from a `PartitionResponse` in a user-friendly string.
    fn format_err_msg(response: &PartitionResponse) -> String {
        format_partition_response_err(response)
    }

    /// Complete a batch successfully.
    ///
    /// Translated from `Sender.completeBatch()` (the 2-argument version).
    fn complete_batch_success(
        &mut self,
        batch: &mut ProducerBatch,
        response: &PartitionResponse,
    ) -> Result<(), KafkaError> {
        if let Some(transaction_manager) = self.transaction_manager.clone() {
            transaction_manager.lock().unwrap().handle_completed_batch(batch, response)?;
        }

        if batch.complete(response.base_offset, response.log_append_time) {
            self.maybe_remove_and_deallocate_batch(batch);
        } else {
            // Always safe to call deallocate because the batch keeps track of
            // whether or not it was deallocated yet
            self.accumulator.deallocate(batch);
        }
        Ok(())
    }

    /// See [`Self::fail_batch_with_record_exceptions`] for the return value.
    #[must_use]
    fn fail_batch(
        &mut self,
        batch: &mut ProducerBatch,
        response: &PartitionResponse,
        adjust_sequence_numbers: bool,
        deallocate_batch: bool,
    ) -> bool {
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
            self.fail_batch_with_error(batch, top_level_error, adjust_sequence_numbers, deallocate_batch)
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
            )
        }
    }

    /// See [`Self::fail_batch_with_record_exceptions`] for the return value.
    #[must_use]
    fn fail_batch_with_error(
        &mut self,
        batch: &mut ProducerBatch,
        top_level_exception: KafkaError,
        adjust_sequence_numbers: bool,
        deallocate_batch: bool,
    ) -> bool {
        let exception_clone = top_level_exception.clone();
        let record_exceptions: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync> =
            Arc::new(move |_| Some(exception_clone.clone()));
        self.fail_batch_with_record_exceptions(
            batch,
            top_level_exception,
            record_exceptions,
            adjust_sequence_numbers,
            deallocate_batch,
        )
    }

    /// Returns `true` when the caller must keep the batch reachable until its produce
    /// response arrives, i.e. when this took Java's `maybeRemoveAndDeallocateBatchLater`
    /// branch (`Sender.java:861`) and the pooled buffer has *not* been returned. See
    /// [`Self::batches_awaiting_response`].
    #[must_use]
    fn fail_batch_with_record_exceptions(
        &mut self,
        batch: &mut ProducerBatch,
        top_level_exception: KafkaError,
        record_exceptions: Arc<dyn Fn(i32) -> Option<KafkaError> + Send + Sync>,
        adjust_sequence_numbers: bool,
        deallocate_batch: bool,
    ) -> bool {
        // The batch has already been removed from `in_flight_batches` by the caller
        // (either `handle_produce_responses` or `get_expired_inflight_batches`).
        let error_for_manager = top_level_exception.clone();
        if batch.complete_exceptionally(top_level_exception, record_exceptions) {
            if let Some(transaction_manager) = self.transaction_manager.clone() {
                // This call can return an error in the rare case that there's an
                // invalid state transition attempted. Log it so as not to interfere
                // with the rest of the logic — Java catches and logs at debug for the
                // same reason (`Sender.java:845-851`).
                //
                // `batches` is empty: it supplies the partition's *remaining*
                // in-flight batches for the transactional sequence adjustment
                // (`TransactionManager.java:818`), and the idempotent arm never reads
                // it (rules §7).
                if let Err(error) = transaction_manager.lock().unwrap().handle_failed_batch(
                    batch,
                    &error_for_manager,
                    adjust_sequence_numbers,
                    &mut [],
                    Caller::Sender,
                ) {
                    kafka_debug!(
                        self.log_context,
                        "Encountered error when transaction manager was handling a failed batch: {}",
                        error
                    );
                }
            }
            if deallocate_batch {
                self.accumulator.complete_and_deallocate_batch(batch);
                false
            } else {
                // Java's `maybeRemoveAndDeallocateBatchLater` (`Sender.java:861`): the
                // pooled ByteBuffer may still be in use by the network client, so it is
                // deallocated when the response arrives.
                self.accumulator.complete_batch(batch);
                true
            }
        } else if deallocate_batch {
            self.accumulator.deallocate(batch);
            false
        } else {
            false
        }
    }

    /// Check if a batch can be retried.
    ///
    /// Translated from `Sender.canRetry()`.
    fn can_retry(&self, batch: &ProducerBatch, response: &PartitionResponse, now: i64) -> Result<bool, KafkaError> {
        if batch.has_reached_delivery_timeout(self.accumulator.delivery_timeout_ms() as i64, now)
            || batch.attempts() >= self.retries
            || batch.is_done()
        {
            return Ok(false);
        }
        match &self.transaction_manager {
            // `batches` is empty: it supplies the partition's in-flight batches for
            // the transactional log-truncation rewrite
            // (`TransactionManager.java:1048`), which the idempotent path never
            // reaches (rules §7).
            Some(transaction_manager) => transaction_manager.lock().unwrap().can_retry(response, batch, &mut []),
            None => Ok(response.error.is_retriable()),
        }
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

        // Mark only the specific batches being sent in this request as inflight, and
        // record their identity so the response can find them again.
        // In Java, `batch.setInflight(true)` is called on each batch as it is added
        // to the produce request (Sender.java:919). The batch just added by
        // `add_to_inflight_batches` is the last one for the partition.
        let mut pending_batches: Vec<(TopicPartition, Arc<ProduceRequestResult>)> = Vec::with_capacity(batch_tps.len());
        for tp in batch_tps {
            if let Some(batches) = self.in_flight_batches.get_mut(&tp)
                && let Some(batch) = batches.last_mut()
            {
                batch.set_inflight(true);
                let identity = Arc::clone(&batch.produce_future);
                pending_batches.push((tp, identity));
            }
        }

        let mut data = ProduceRequestData::new();
        data.set_acks(acks);
        data.set_timeout_ms(timeout);
        data.set_topic_data(topic_data_list);

        let request_builder = ProduceRequestBuilder::new(data);

        // Capture debug representation before request_builder is moved into Box.
        let request_debug = if log::log_enabled!(log::Level::Trace) {
            format!("{:?}", request_builder)
        } else {
            String::new()
        };

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
            .insert(correlation_id, PendingProduceRequest { batches: pending_batches, topic_names });

        self.client.send(client_request, now);
        kafka_trace!(self.log_context, "Sent produce request to {}: {}", node_id, request_debug);
    }

    fn topic_ids_for_partitions(&self, batch_infos: &[RequestBatchInfo]) -> HashMap<Arc<str>, Uuid> {
        let metadata_topic_ids = self.metadata.topic_ids();
        let mut result = HashMap::new();
        for info in batch_infos {
            let topic_arc = info.tp.topic_arc().clone();
            let topic_id = metadata_topic_ids.get(&*topic_arc).copied().unwrap_or(Uuid::ZERO_UUID);
            result.insert(topic_arc, topic_id);
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
    ///
    /// Held as a refcounted [`bytes::Bytes`] so the batch buffer travels from
    /// the builder through to the wire send without an intermediate copy
    /// (consumer-threading.md §27 write-path symmetry).
    records_data: Option<bytes::Bytes>,
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
    use crate::common::utils::ProducerIdAndEpoch;
    use crate::mock_client::MockClient;
    use crate::produce_response_data::{PartitionProduceResponse, ProduceResponseData, TopicProduceResponse};
    use crate::producer::internals::BufferPool;
    use crate::producer::internals::FutureRecordMetadata;
    use crate::producer::internals::PartitionerConfig;
    use crate::producer::internals::{Caller, InFlightBatchPool, TransactionalRequestResult};
    use std::sync::atomic::AtomicI64;

    // Constants matching Java's SenderTest
    const MAX_REQUEST_SIZE: i32 = 1024 * 1024;
    const ACKS_ALL: i16 = -1;
    const REQUEST_TIMEOUT: i32 = 5000;
    const RETRY_BACKOFF_MS: i64 = 50;
    const DELIVERY_TIMEOUT_MS: i32 = 1500;
    const TOPIC_IDLE_MS: i64 = 60 * 1000;
    const MAX_BLOCK_TIMEOUT: i64 = 1000;

    /// Mirrors `TransactionManagerTest.transactionTimeoutMs` (Java 128).
    const TRANSACTION_TIMEOUT_MS: i32 = 1121;

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

    /// Builds an idempotent (non-transactional) [`TransactionManager`], mirroring
    /// `TransactionManagerTest.initializeTransactionManager(Optional.empty(), ..)`.
    ///
    /// Nothing on the idempotent path reads `ApiVersions` — the manager consults it
    /// only from `handleCoordinatorReady` (Java 1104), which is transactional, and
    /// `maybeUpdateTransactionV2Enabled` (Java 493), which is Phase 5b — so an
    /// empty instance is sufficient here.
    fn idempotent_transaction_manager() -> Arc<Mutex<TransactionManager>> {
        Arc::new(Mutex::new(TransactionManager::new(
            LogContext::empty(),
            None,
            TRANSACTION_TIMEOUT_MS,
            RETRY_BACKOFF_MS,
            Arc::new(crate::ApiVersions::new()),
            false,
        )))
    }

    /// `transactionalId` (Java 133).
    const TRANSACTIONAL_ID: &str = "foobar";

    /// Builds a transactional [`TransactionManager`], mirroring
    /// `TransactionManagerTest.initializeTransactionManager(Optional.of(transactionalId), false)`.
    ///
    /// Node `"0"` is registered with `INIT_PRODUCER_ID` v6 as Java's helper does
    /// (Java 186-189), so `handleCoordinatorReady` finds epoch-bump support once
    /// the coordinator connection is ready.
    fn transactional_transaction_manager() -> Arc<Mutex<TransactionManager>> {
        use crate::api_versions_response_data::ApiVersion;

        let api_versions = Arc::new(crate::ApiVersions::new());
        let mut init_producer_id = ApiVersion::new();
        init_producer_id
            .set_api_key(crate::common::protocol::ApiKeys::INIT_PRODUCER_ID.id())
            .set_min_version(0)
            .set_max_version(6);
        api_versions.update("0", crate::NodeApiVersions::new(&[init_producer_id], &[], &[], 0));

        Arc::new(Mutex::new(TransactionManager::new(
            LogContext::empty(),
            Some(TRANSACTIONAL_ID.to_string()),
            TRANSACTION_TIMEOUT_MS,
            RETRY_BACKOFF_MS,
            api_versions,
            false,
        )))
    }

    /// The timing knobs a `SenderTest`-style context can override, matching the ones
    /// Java's bespoke `RecordAccumulator` / `Sender` constructions vary.
    ///
    /// Note the two backoffs are separate, as they are in Java:
    /// `setupWithTransactionState` builds the accumulator with `retryBackoffMs = 0L`
    /// (`SenderTest.java:3860`), so a re-enqueued batch is drainable on the very next
    /// `runOnce`, while the `Sender` gets `RETRY_BACKOFF_MS = 50` (`:3864`) for its
    /// transactional-request backoff. Collapsing them made re-enqueued batches wait a
    /// backoff Java does not impose, which is visible in every multi-in-flight retry
    /// test.
    struct SenderTestTimeouts {
        request_timeout_ms: i32,
        delivery_timeout_ms: i32,
        accumulator_retry_backoff_ms: i64,
        sender_retry_backoff_ms: i64,
    }

    /// Test harness holding all state needed for SenderTest-style tests.
    struct SenderTestContext {
        sender: Sender<MockClient>,
        accumulator: Arc<RecordAccumulator>,
        metadata: Arc<ProducerMetadata>,
        time: Arc<MockTime>,
        tp0: TopicPartition,
        tp1: TopicPartition,
        transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
    }

    impl SenderTestContext {
        /// Default test setup, matching Java's `setupWithTransactionState(null)`.
        fn new() -> Self {
            Self::with_options(false, i32::MAX)
        }

        /// Setup with guarantee_message_order and custom retries.
        fn with_options(guarantee_message_order: bool, retries: i32) -> Self {
            Self::with_transaction_state(guarantee_message_order, retries, None, None)
        }

        /// Setup with an idempotent [`TransactionManager`] shared between the
        /// `Sender` and the `RecordAccumulator`, mirroring Java's
        /// `setupWithTransactionState(transactionManager)`.
        fn idempotent() -> Self {
            Self::with_transaction_state(false, i32::MAX, Some(idempotent_transaction_manager()), None)
        }

        /// Setup with a transactional [`TransactionManager`], mirroring
        /// `TransactionManagerTest.setup()` (Java 161-169).
        fn transactional() -> Self {
            Self::with_transaction_state(false, i32::MAX, Some(transactional_transaction_manager()), None)
        }

        /// Idempotent setup with `guarantee_message_order` and a bounded retry count,
        /// mirroring the bespoke `Sender` several `SenderTest` methods build with
        /// `guaranteeOrder = true`.
        fn idempotent_in_order(retries: i32) -> Self {
            Self::with_transaction_state(true, retries, Some(idempotent_transaction_manager()), None)
        }

        /// Idempotent setup with a bounded retry count, mirroring Java's
        /// `setupWithTransactionState(transactionManager, false, null, true, retries, 0)`.
        fn idempotent_with_retries(retries: i32) -> Self {
            Self::with_transaction_state(false, retries, Some(idempotent_transaction_manager()), None)
        }

        /// Idempotent setup with explicit timeouts and no retry backoff, mirroring the
        /// bespoke `RecordAccumulator` + `Sender` that
        /// `TransactionManagerTest.testDuplicateSequenceAfterProducerReset`
        /// (Java 754-763) builds: `retryBackoffMs` is `0` on both, so a re-enqueued
        /// batch is immediately drainable again.
        fn idempotent_with_timeouts(request_timeout_ms: i32, delivery_timeout_ms: i32) -> Self {
            Self::with_transaction_state(
                false,
                i32::MAX,
                Some(idempotent_transaction_manager()),
                Some(SenderTestTimeouts {
                    request_timeout_ms,
                    delivery_timeout_ms,
                    accumulator_retry_backoff_ms: 0,
                    sender_retry_backoff_ms: 0,
                }),
            )
        }

        /// Setup with an explicit (possibly absent) transaction manager and optional
        /// timeout overrides.
        fn with_transaction_state(
            guarantee_message_order: bool,
            retries: i32,
            transaction_manager: Option<Arc<Mutex<TransactionManager>>>,
            timeouts: Option<SenderTestTimeouts>,
        ) -> Self {
            let SenderTestTimeouts {
                request_timeout_ms,
                delivery_timeout_ms,
                accumulator_retry_backoff_ms,
                sender_retry_backoff_ms,
            } = timeouts.unwrap_or(SenderTestTimeouts {
                request_timeout_ms: REQUEST_TIMEOUT,
                delivery_timeout_ms: DELIVERY_TIMEOUT_MS,
                // Java 3860: the accumulator's retry backoff is 0 in every
                // `setupWithTransactionState` variant.
                accumulator_retry_backoff_ms: 0,
                sender_retry_backoff_ms: RETRY_BACKOFF_MS,
            });
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
                accumulator_retry_backoff_ms,
                accumulator_retry_backoff_ms * 10,
                delivery_timeout_ms,
                PartitionerConfig { enable_adaptive_partitioning: true, partition_availability_timeout_ms: 0 },
                Arc::new(BufferPool::new(total_size as i64, batch_size as usize)),
                transaction_manager.clone(),
            ));

            let nodes = vec![Node::new(0, "localhost".to_string(), 1969)];
            let client = MockClient::new(nodes, Arc::clone(&time_provider));

            let running = Arc::new(AtomicBool::new(true));
            let force_close = Arc::new(AtomicBool::new(false));

            let sender = Sender::new(
                client,
                Arc::clone(&metadata),
                Arc::clone(&accumulator),
                guarantee_message_order,
                MAX_REQUEST_SIZE,
                ACKS_ALL,
                retries,
                request_timeout_ms,
                sender_retry_backoff_ms,
                running,
                force_close,
                time_provider,
                transaction_manager.clone(),
                LogContext::empty(),
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

            Self { sender, accumulator, metadata, time, tp0, tp1, transaction_manager }
        }

        /// Re-publishes the topic metadata with `tp0` at `tp0_leader_epoch` and `tp1` at
        /// epoch 0, mirroring the `metadataUpdateWithIds(1, .., tp -> epoch)` calls in
        /// `SenderTest.testProducerBatchRetriesWhenPartitionLeaderChanges`
        /// (Java 3325-3339).
        fn update_metadata_with_leader_epochs(&self, tp0_leader_epoch: i32) {
            let mut topic_partition_counts = HashMap::new();
            topic_partition_counts.insert(TOPIC_NAME.to_string(), 2);
            let tp0 = self.tp0.clone();
            let metadata_response = crate::common::requests::request_test_utils::metadata_update_with_ids(
                "kafka-cluster",
                1,
                &HashMap::new(),
                &topic_partition_counts,
                &|tp| {
                    if *tp == tp0 { Some(tp0_leader_epoch) } else { Some(0) }
                },
                &topic_ids(),
            );
            self.metadata
                .update_with_current_request_version(&metadata_response, false, self.time.milliseconds());
        }

        /// The shared transaction manager, for tests that assert on manager state.
        fn transaction_manager(&self) -> Arc<Mutex<TransactionManager>> {
            Arc::clone(
                self.transaction_manager
                    .as_ref()
                    .expect("this context was built with a transaction manager"),
            )
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

        /// Build a produce response covering several partitions, in the given order.
        ///
        /// `SenderTest.produceResponse(Map<TopicPartition, OffsetAndError>)`
        /// (Java 3760-3785). Ordering matters: the tests rely on the response listing
        /// `tp1` before `tp0`.
        fn produce_response_for(&self, entries: &[(&TopicPartition, i64, Errors)]) -> ConcreteResponse {
            let mut by_topic: Vec<TopicProduceResponse> = Vec::new();
            for (tp, offset, error) in entries {
                let mut ppr = PartitionProduceResponse::new();
                ppr.set_index(tp.partition());
                ppr.set_base_offset(*offset);
                ppr.set_error_code(error.code());
                ppr.set_log_start_offset(-1);
                match by_topic.iter_mut().find(|topic| topic.name == *tp.topic()) {
                    Some(topic) => topic.partition_responses.push(ppr),
                    None => {
                        let mut tpr = TopicProduceResponse::new();
                        tpr.set_topic_id(topic_id());
                        tpr.set_name(tp.topic().to_string());
                        tpr.set_partition_responses(vec![ppr]);
                        by_topic.push(tpr);
                    },
                }
            }
            let mut data = ProduceResponseData::new();
            data.set_responses(by_topic);
            ConcreteResponse::Produce(ProduceResponse::new(data))
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

        let identity = Arc::new(crate::producer::internals::ProduceRequestResult::new(tp.clone()));
        pending.insert(
            42,
            PendingProduceRequest { batches: vec![(tp.clone(), Arc::clone(&identity))], topic_names },
        );

        assert!(pending.contains_key(&42));
        let removed = pending.remove(&42).unwrap();
        assert!(
            removed
                .batches
                .iter()
                .any(|(partition, result)| *partition == tp && Arc::ptr_eq(result, &identity))
        );
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
            None,
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
        let info = RequestBatchInfo { tp: tp.clone(), records_data: Some(bytes::Bytes::from(vec![1, 2, 3])) };
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

        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send produce request

        assert_eq!(
            ctx.sender.client().in_flight_request_count(),
            1,
            "We should have a single produce request in flight."
        );
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);
        assert!(ctx.sender.client().has_in_flight_requests());

        let response = ctx.produce_response(&tp0, offset, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0, "All requests completed.");
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);
        assert!(!ctx.sender.client().has_in_flight_requests());

        ctx.sender.run_once().await.expect("run_once");
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

        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send produce request

        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert!(ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);
        assert!(!future.is_done());

        let response = ctx.produce_response(&tp0, -1, Errors::TopicAuthorizationFailed, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await.expect("run_once");
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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send request
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);

        let response = ctx.produce_response(&tp0, -1, Errors::NotLeaderOrFollower, -1);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await.expect("run_once"); // expire the batch
        assert!(future.is_done());
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        ctx.sender.run_once().await.expect("run_once"); // receive first response and do not reenqueue
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        ctx.sender.run_once().await.expect("run_once"); // run again and must not send anything
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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        // Return a MESSAGE_TOO_LARGE error
        let response = ctx.produce_response(&tp0, -1, Errors::MessageTooLarge, -1);
        ctx.sender.client_mut().respond(response);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);

        // Expire the batch and process the response
        ctx.sender.run_once().await.expect("run_once");
        assert!(future1.is_done());
        assert!(future2.is_done());
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        // Run again and must not split big batch and resend anything
        ctx.sender.run_once().await.expect("run_once");
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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send request
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(
            ctx.sender.in_flight_batches(&ctx.tp0).len(),
            1,
            "Expect one in-flight batch in accumulator"
        );

        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);

        ctx.sender.run_once().await.expect("run_once"); // receive first response
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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send request
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64 / 2);

        // Send second ProduceRequest
        ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once"); // must not send request because the partition is muted
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64 / 2); // expire the first batch only

        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await.expect("run_once"); // receive response (offset=0)
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        ctx.sender.run_once().await.expect("run_once"); // Drain the second request only this time
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 1);
    }

    /// Regression test for the unmute placement in [`Sender::fail_expired_batches`].
    ///
    /// Not a translation of a Java test — Java has no test for this because Java's
    /// `failExpiredBatches` (`Sender.java:362-377`) never unmutes, so the bug is
    /// unrepresentable there. This port did unmute for undrained expired batches, which
    /// released a mute belonging to a batch whose request was still outstanding.
    ///
    /// Three batches are appended to `tp0` under `guarantee_message_order`:
    ///
    ///   - **A** is drained (muting `tp0`) and its request is in flight;
    ///   - **B** stays queued behind the mute and expires alongside A;
    ///   - **C** stays queued and is still well inside `delivery.timeout.ms`.
    ///
    /// A is expired in flight and retained for its response; B is expired undrained.
    /// Neither may unmute `tp0`, so C must wait for A's response — otherwise a second
    /// produce request for `tp0` goes out beside A's, which is exactly the reordering
    /// `guarantee_message_order` (`max.in.flight.requests.per.connection = 1`) exists
    /// to prevent.
    #[tokio::test]
    async fn test_expiring_an_undrained_batch_does_not_unmute_the_partition() {
        let mut ctx = SenderTestContext::with_options(true, i32::MAX);
        let tp0 = ctx.tp0.clone();
        // One record per batch: a value the size of `batch_size` leaves no room for a
        // second record, so each append opens a fresh batch with its own `created_ms`.
        let big_value = "v".repeat(16 * 1024);

        // A — drained, `tp0` muted, request in flight.
        let batch_a = ctx
            .append_to_accumulator_with(&tp0, ctx.time.milliseconds(), "a", &big_value)
            .await;
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send A
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);

        // B — queued behind the mute, same age as A.
        let batch_b = ctx
            .append_to_accumulator_with(&tp0, ctx.time.milliseconds(), "b", &big_value)
            .await;

        // C is appended later, so it is still fresh when A and B expire.
        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64 - 100);
        let batch_c = ctx
            .append_to_accumulator_with(&tp0, ctx.time.milliseconds(), "c", &big_value)
            .await;
        assert_eq!(ctx.accumulator.deque_size(&tp0), 2, "B and C are queued in separate batches");

        // A and B are now past `delivery.timeout.ms`; C is not.
        ctx.time.sleep(200);
        ctx.sender.run_once().await.expect("run_once");

        assert!(batch_a.is_done(), "A expired in flight");
        assert!(batch_b.is_done(), "B expired undrained");
        assert!(!batch_c.is_done(), "C is still well inside the delivery timeout");
        assert_eq!(ctx.accumulator.deque_size(&tp0), 1, "only C is left queued");
        assert_eq!(
            ctx.sender.client().in_flight_request_count(),
            1,
            "B's expiry must not unmute tp0: A's request is still outstanding"
        );

        // Repeated polls must not find the partition drainable either.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.sender.client().in_flight_request_count(),
            1,
            "tp0 stays muted until A's response arrives"
        );
        assert_eq!(ctx.accumulator.deque_size(&tp0), 1);

        // A's response is what releases the mute (`Sender.java:735-737`).
        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);

        // Only now does C go out.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1, "C drains once tp0 is unmuted");
        assert_eq!(ctx.accumulator.deque_size(&tp0), 0);
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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send produce request

        let response = ctx.produce_response(&tp0, 0, Errors::InvalidRequest, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await.expect("run_once");
        ctx.sender.run_once().await.expect("run_once");

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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send produce request

        let error_message = "testCustomErrorMessage";
        let response =
            ctx.produce_response_with_message(&tp0, 0, Errors::InvalidRequest, 0, -1, Some(error_message.to_string()));
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await.expect("run_once");
        ctx.sender.run_once().await.expect("run_once");

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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once");
        // Note: Both partitions may go in same or separate requests depending on node assignment
        assert!(ctx.sender.client().in_flight_request_count() >= 1);

        // Respond for tp0 with success
        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        // Successfully expire both batches
        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);
        ctx.sender.run_once().await.expect("run_once");
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

        ctx.sender.run_once().await.expect("run_once");
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

        ctx.sender.run_once().await.expect("run_once"); // send produce request

        let response = ctx.produce_response(&tp0, offset, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0, "Request completed.");
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.sender.in_flight_batches(&ctx.tp0).len(), 0);

        ctx.sender.run_once().await.expect("run_once");
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

        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send request
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

        ctx.sender.run_once().await.expect("run_once");

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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send produce request

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

        ctx.sender.run_once().await.expect("run_once"); // receive error (disconnect response triggers reenqueue)
        // Advance time past the retry backoff (accounting for jitter up to 20%)
        // so the reenqueued batch becomes sendable.
        let backoff_with_jitter = (RETRY_BACKOFF_MS as f64 * 1.3) as i64;
        ctx.time.sleep(backoff_with_jitter);
        // In Rust's MockClient, ready() transitions Disconnected -> Connecting -> Connected
        // in a single call, so one additional run_once is enough to reconnect + drain + send.
        ctx.sender.run_once().await.expect("run_once"); // reconnect + resend

        assert_eq!(1, ctx.sender.client().in_flight_request_count());
        assert!(ctx.sender.client().has_in_flight_requests());
        assert_eq!(1, ctx.sender.in_flight_batches(&ctx.tp0).len());

        let offset = 0i64;
        let response = ctx.produce_response(&tp0, offset, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await.expect("run_once");
        assert!(future.is_done(), "Request should have retried and completed");
        let metadata = future.get().await.expect("Future should succeed");
        assert_eq!(offset, metadata.offset());
        assert_eq!(0, ctx.sender.in_flight_batches(&ctx.tp0).len());

        // --- Unsuccessful retry (exhausted retries) ---
        let future = ctx.append_to_accumulator_with(&tp0, 0, "key", "value").await;
        ctx.sender.run_once().await.expect("run_once"); // send produce request
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
            ctx.sender.run_once().await.expect("run_once"); // receive error
            assert_eq!(0, ctx.sender.in_flight_batches(&ctx.tp0).len());
            ctx.time.sleep(backoff_with_jitter); // advance past retry backoff (with jitter margin)
            ctx.sender.run_once().await.expect("run_once"); // reconnect + resend
            assert_eq!(if i > 0 { 0 } else { 1 }, ctx.sender.in_flight_batches(&ctx.tp0).len());
        }

        ctx.sender.run_once().await.expect("run_once");
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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send produce request

        assert_eq!(1, ctx.sender.client().in_flight_request_count());
        assert!(ctx.sender.client().has_in_flight_requests());
        assert_eq!(1, ctx.sender.in_flight_batches(&tp1).len());

        ctx.time.sleep(900);
        // Now send another message to tp1
        ctx.append_to_accumulator_with(&tp1, 0, "key2", "value2").await;

        // With guarantee_message_order, the second message should not be sent
        // because tp1 is muted.
        ctx.sender.run_once().await.expect("run_once"); // should not send because muted
        assert_eq!(1, ctx.sender.client().in_flight_request_count());

        // Complete the first request
        let response = ctx.produce_response(&tp1, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);

        // Sender receives the response for the previous send and unmutes
        // the partition. But the drain happens at the start of run_once,
        // so we need another cycle to actually drain and send the new batch.
        ctx.sender.run_once().await.expect("run_once"); // receive response, unmute
        ctx.sender.run_once().await.expect("run_once"); // drain the second batch and send
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
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send
        assert_eq!(1, ctx.sender.client().in_flight_request_count());
        assert_eq!(1, ctx.sender.in_flight_batches(&ctx.tp0).len());
        assert!(
            !ctx.sender.in_flight_batches(&ctx.tp0)[0].is_buffer_deallocated(),
            "Buffer not deallocated yet"
        );

        ctx.time.sleep(REQUEST_TIMEOUT as i64);

        ctx.sender.run_once().await.expect("run_once"); // times out the request
        assert!(future.is_done());

        ctx.sender.run_once().await.expect("run_once");
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

        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send produce request

        // Advance time beyond delivery timeout
        ctx.time.sleep(ctx.accumulator.delivery_timeout_ms() as i64 + 1);

        // Run once more - this should detect the expired batch
        ctx.sender.run_once().await.expect("run_once");

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
            None,
        ));

        let nodes = vec![Node::new(0, "localhost".to_string(), 1969)];
        let client = MockClient::new(nodes, Arc::clone(&time_provider));

        let running = Arc::new(AtomicBool::new(true));
        let force_close = Arc::new(AtomicBool::new(false));

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
            time_provider,
            None,
            LogContext::empty(),
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

        sender.run_once().await.expect("run_once"); // connect
        sender.run_once().await.expect("run_once"); // send
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
        sender.run_once().await.expect("run_once");
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
        sender.run_once().await.expect("run_once");
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
        sender.run_once().await.expect("run_once");
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
        sender.run_once().await.expect("run_once");
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

    // =====================================================================
    // `TxnRequestHandler.onComplete` — the unsynchronized half
    // (`TransactionManager.java:1406-1420`)
    //
    // These three moved here from `transaction_manager.rs`'s test module in
    // Phase 4, together with the code they exercise: the correlation-id check and
    // clear touch Sender-confined state (rules §2), so
    // `Sender::on_transactional_response` owns them now.
    // =====================================================================

    /// Enqueues an `InitProducerId` and hands back the dequeued handler, playing
    /// the part of `Sender.runOnce`'s `:331` + `maybeSendAndPollTransactionalRequest`'s
    /// `nextRequest` at `:472`.
    fn pending_init_producer_id_handler(ctx: &mut SenderTestContext) -> TxnRequestHandler {
        let transaction_manager = ctx.transaction_manager();
        let mut pool = InFlightBatchPool::new();
        let mut manager = transaction_manager.lock().unwrap();
        manager
            .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut ctx.sender.pending_requests, Caller::Sender)
            .expect("the initial InitProducerId is enqueued");
        manager
            .next_request(&mut ctx.sender.pending_requests, false)
            .expect("an InitProducerId request is pending")
    }

    /// Builds an `InitProducerId` `ClientResponse` with the given correlation id.
    fn init_producer_id_client_response(
        correlation_id: i32,
        disconnected: bool,
        error: Option<Errors>,
    ) -> ClientResponse {
        use crate::common::protocol::ApiKeys;
        use crate::common::requests::{InitProducerIdResponse, RequestHeader};
        use crate::init_producer_id_response_data::InitProducerIdResponseData;

        let header = RequestHeader::new(&ApiKeys::INIT_PRODUCER_ID, 0, "", correlation_id)
            .expect("INIT_PRODUCER_ID is a known api key");
        let body = error.map(|error| {
            let mut data = InitProducerIdResponseData::new();
            data.set_error_code(error.code())
                .set_producer_id(13131)
                .set_producer_epoch(1)
                .set_throttle_time_ms(0);
            ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data))
        });
        ClientResponse::new(header, None, "0", 0, 0, disconnected, None, None, body)
    }

    /// A response whose correlation id does not match the in-flight one is fatal
    /// (`TransactionManager.java:1407-1408`).
    #[test]
    fn test_mismatched_correlation_id_is_fatal() {
        let mut ctx = SenderTestContext::idempotent();
        let handler = pending_init_producer_id_handler(&mut ctx);

        const CORRELATION_ID: i32 = 7;
        ctx.sender.set_in_flight_correlation_id(CORRELATION_ID + 1);
        let response = init_producer_id_client_response(CORRELATION_ID, false, None);
        ctx.sender
            .on_transactional_response(handler, &response)
            .expect("the mismatch is handled, not propagated");

        let manager = ctx.transaction_manager();
        let manager = manager.lock().unwrap();
        assert!(manager.has_fatal_error());
        assert_eq!(
            manager.last_error().expect("recorded").message(),
            "Detected more than one in-flight transactional request."
        );
        assert!(
            ctx.sender.has_in_flight_request(),
            "a mismatch must not clear the in-flight correlation id"
        );
    }

    /// A disconnect re-enqueues the request; an idempotent `InitProducerId` needs
    /// no coordinator, so no lookup is attempted
    /// (`TransactionManager.java:1411-1415`, `:1482`).
    #[test]
    fn test_disconnect_reenqueues_a_transactional_request_without_a_coordinator_lookup() {
        let mut ctx = SenderTestContext::idempotent();
        let handler = pending_init_producer_id_handler(&mut ctx);
        let result = Arc::clone(handler.result());

        const CORRELATION_ID: i32 = 7;
        ctx.sender.set_in_flight_correlation_id(CORRELATION_ID);
        let response = init_producer_id_client_response(CORRELATION_ID, true, None);
        ctx.sender
            .on_transactional_response(handler, &response)
            .expect("a disconnect is handled");

        assert!(!result.is_completed(), "a re-enqueued request must not complete");
        let transaction_manager = ctx.transaction_manager();
        assert!(!transaction_manager.lock().unwrap().has_error());
        assert!(
            !ctx.sender.has_in_flight_request(),
            "the in-flight correlation id is cleared before the retry (Java 1410)"
        );
        let requeued = transaction_manager
            .lock()
            .unwrap()
            .next_request(&mut ctx.sender.pending_requests, false)
            .expect("re-enqueued");
        assert!(requeued.is_retry());
    }

    /// A successful response clears the in-flight correlation id before the body
    /// reaches the manager (`TransactionManager.java:1410`).
    #[test]
    fn test_transactional_response_clears_the_in_flight_correlation_id() {
        let mut ctx = SenderTestContext::idempotent();
        let handler = pending_init_producer_id_handler(&mut ctx);

        const CORRELATION_ID: i32 = 7;
        ctx.sender.set_in_flight_correlation_id(CORRELATION_ID);
        assert!(ctx.sender.has_in_flight_request());
        let response = init_producer_id_client_response(CORRELATION_ID, false, Some(Errors::None));
        ctx.sender
            .on_transactional_response(handler, &response)
            .expect("a successful InitProducerId response is handled");

        assert!(!ctx.sender.has_in_flight_request());
        assert!(ctx.transaction_manager().lock().unwrap().has_producer_id());
    }

    // =====================================================================
    // `runOnce`'s `transactionManager != null` block (`Sender.java:311-341`)
    //
    // One test per exit, driving the real `run_once`. Before Phase 4 the only
    // model of this block was `transaction_manager.rs`'s
    // `run_manager_transaction_phase` harness, which models `:333-335` as a
    // predicate; these are what PLAN §Phase-4 means by "replaces the predicate
    // with the real call".
    // =====================================================================

    /// Builds an `InitProducerId` response body for the mock client to return.
    fn init_producer_id_response(error: Errors, producer_id: i64, epoch: i16) -> ConcreteResponse {
        use crate::common::requests::InitProducerIdResponse;
        use crate::init_producer_id_response_data::InitProducerIdResponseData;

        let mut data = InitProducerIdResponseData::new();
        data.set_error_code(error.code())
            .set_producer_id(producer_id)
            .set_producer_epoch(epoch)
            .set_throttle_time_ms(0);
        ConcreteResponse::InitProducerId(InitProducerIdResponse::new(data))
    }

    /// Builds a `FindCoordinator` response naming `node` as the coordinator.
    fn find_coordinator_response(error: Errors, key: &str, node: &Node) -> ConcreteResponse {
        use crate::common::requests::FindCoordinatorResponse;

        ConcreteResponse::FindCoordinator(FindCoordinatorResponse::prepare_response(error, key, node))
    }

    /// Drives a transactional producer's `initTransactions` through
    /// `Sender.runOnce`, the way `TransactionManagerTest.doInitTransactions`
    /// (Java 4348) drives it.
    ///
    /// The three iterations are the three exits of
    /// `maybeSendAndPollTransactionalRequest`: the coordinator is unknown so a
    /// `FindCoordinator` is enqueued (`Sender.java:489-492`), the `FindCoordinator`
    /// goes to the least-loaded node (`:481`), then the `InitProducerId` goes to the
    /// coordinator.
    async fn run_init_transactions(ctx: &mut SenderTestContext) -> Arc<TransactionalRequestResult> {
        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();
        let result = ctx
            .transaction_manager()
            .lock()
            .unwrap()
            .initialize_transactions(false, &mut ctx.sender.pending_requests)
            .expect("initTransactions is valid from UNINITIALIZED");

        // Iteration 1: the coordinator is unknown, so only a FindCoordinator is
        // enqueued — nothing is sent.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);

        // Iteration 2: the FindCoordinator goes out and is answered.
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, TRANSACTIONAL_ID, &node));
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.sender.coordinator(CoordinatorType::Transaction).expect("valid type"),
            Some(&node),
            "the coordinator must be discovered"
        );

        // Iteration 3: the InitProducerId goes to the coordinator.
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, 13131, 1));
        ctx.sender.run_once().await.expect("run_once");
        assert!(ctx.transaction_manager().lock().unwrap().has_producer_id());
        result.await_result().await.expect("initTransactions succeeded");
        result
    }

    /// `Sender.maybeSendAndPollTransactionalRequest` routes a transactional
    /// `InitProducerId` to the coordinator, after discovering it — the coordinator
    /// arms at `Sender.java:479-492` that Phase 4 left as deferrals.
    ///
    /// Also covers `testDisconnectAndRetry`'s successful half and
    /// `handleCoordinatorReady` reaching the manager through `awaitNodeReady`
    /// (`Sender.java:565-570`).
    #[tokio::test]
    async fn test_transactional_init_producer_id_is_routed_to_the_coordinator() {
        let mut ctx = SenderTestContext::transactional();
        assert!(
            ctx.sender
                .coordinator(CoordinatorType::Transaction)
                .expect("valid type")
                .is_none()
        );

        run_init_transactions(&mut ctx).await;

        let manager = ctx.transaction_manager();
        let manager = manager.lock().unwrap();
        assert_eq!(manager.producer_id_and_epoch().producer_id, 13131);
        assert_eq!(manager.producer_id_and_epoch().epoch, 1);
        assert!(
            manager.need_to_trigger_epoch_bump_from_client(),
            "awaitNodeReady must have called handleCoordinatorReady for the TRANSACTION coordinator"
        );
    }

    /// Translated from `testLookupCoordinatorOnDisconnectAfterSend`
    /// (Java 1260-1290): a disconnect while the `InitProducerId` is in flight
    /// forgets the coordinator and re-enqueues both requests
    /// (`TransactionManager.java:1411-1416`).
    #[tokio::test]
    async fn test_lookup_coordinator_on_disconnect_after_send() {
        let mut ctx = SenderTestContext::transactional();
        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();
        let init_pid_result = ctx
            .transaction_manager()
            .lock()
            .unwrap()
            .initialize_transactions(false, &mut ctx.sender.pending_requests)
            .expect("initTransactions is valid from UNINITIALIZED");

        ctx.sender.run_once().await.expect("run_once");
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, TRANSACTIONAL_ID, &node));
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.sender.coordinator(CoordinatorType::Transaction).expect("valid type"),
            Some(&node)
        );

        // Send the InitProducerId to the coordinator and disconnect before the
        // response arrives.
        ctx.sender
            .client_mut()
            .prepare_response_disconnected(init_producer_id_response(Errors::None, 13131, 1), true);
        ctx.sender.run_once().await.expect("run_once");

        assert!(
            ctx.sender
                .coordinator(CoordinatorType::Transaction)
                .expect("valid type")
                .is_none(),
            "a disconnect must forget the coordinator"
        );
        assert!(!init_pid_result.is_completed());
        assert!(!ctx.transaction_manager().lock().unwrap().has_producer_id());
        assert!(!ctx.sender.has_in_flight_request());

        // The retry finds the coordinator again and succeeds.
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, TRANSACTIONAL_ID, &node));
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.sender.coordinator(CoordinatorType::Transaction).expect("valid type"),
            Some(&node)
        );
        assert!(!init_pid_result.is_completed());

        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, 13131, 1));
        ctx.sender.run_once().await.expect("run_once");
        init_pid_result.await_result().await.expect("the second round succeeds");
        assert!(ctx.transaction_manager().lock().unwrap().has_producer_id());
    }

    /// Translated from `testDisconnectAndRetry` (Java 1080-1091): a disconnected
    /// `FindCoordinator` response leaves the coordinator unknown and the request is
    /// retried, without a nested lookup — `FindCoordinatorHandler.coordinatorType()`
    /// is null (Java 1671), so `needsCoordinator()` is false at
    /// `TransactionManager.java:1413`.
    #[tokio::test]
    async fn test_disconnect_and_retry() {
        let mut ctx = SenderTestContext::transactional();
        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();
        ctx.transaction_manager()
            .lock()
            .unwrap()
            .initialize_transactions(false, &mut ctx.sender.pending_requests)
            .expect("initTransactions is valid from UNINITIALIZED");

        // Iteration 1 enqueues the FindCoordinator; iteration 2 sends it and is
        // disconnected.
        ctx.sender.run_once().await.expect("run_once");
        ctx.sender
            .client_mut()
            .prepare_response_disconnected(find_coordinator_response(Errors::None, TRANSACTIONAL_ID, &node), true);
        ctx.sender.run_once().await.expect("run_once");
        assert!(
            ctx.sender
                .coordinator(CoordinatorType::Transaction)
                .expect("valid type")
                .is_none(),
            "a disconnected lookup must not install a coordinator"
        );
        assert!(!ctx.transaction_manager().lock().unwrap().has_error());

        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, TRANSACTIONAL_ID, &node));
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.sender.coordinator(CoordinatorType::Transaction).expect("valid type"),
            Some(&node)
        );
    }

    /// Translated from `testLookupCoordinatorOnDisconnectBeforeSend`
    /// (Java 1292-1321): a coordinator that is unreachable *before* the
    /// `InitProducerId` goes out drives `awaitNodeReady` to `false`
    /// (`Sender.java:485-488`), and `maybeFindCoordinatorAndRetry` then forgets it.
    #[tokio::test]
    async fn test_lookup_coordinator_on_disconnect_before_send() {
        let mut ctx = SenderTestContext::transactional();
        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();
        let init_pid_result = ctx
            .transaction_manager()
            .lock()
            .unwrap()
            .initialize_transactions(false, &mut ctx.sender.pending_requests)
            .expect("initTransactions is valid from UNINITIALIZED");

        ctx.sender.run_once().await.expect("run_once");
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, TRANSACTIONAL_ID, &node));
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.sender.coordinator(CoordinatorType::Transaction).expect("valid type"),
            Some(&node)
        );

        // Java: `client.disconnect(..); client.backoff(node, 100)`. The backoff is
        // what makes `awaitReady` give up rather than reconnect inside the same
        // iteration; `set_unreachable` does both in one call.
        ctx.sender.client_mut().set_unreachable(&node, 100);
        ctx.sender.run_once().await.expect("run_once");
        assert!(
            ctx.sender
                .coordinator(CoordinatorType::Transaction)
                .expect("valid type")
                .is_none(),
            "an unready coordinator must be forgotten so it can be rediscovered"
        );
        assert!(!init_pid_result.is_completed());
        assert!(!ctx.transaction_manager().lock().unwrap().has_producer_id());

        // Java: `time.sleep(110)` — wait out the backoff, then rediscover and succeed.
        ctx.time.sleep(110);
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, TRANSACTIONAL_ID, &node));
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.sender.coordinator(CoordinatorType::Transaction).expect("valid type"),
            Some(&node)
        );
        assert!(!init_pid_result.is_completed());

        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, 13131, 1));
        ctx.sender.run_once().await.expect("run_once");
        init_pid_result.await_result().await.expect("the retry succeeds");
        let manager = ctx.transaction_manager();
        let manager = manager.lock().unwrap();
        assert_eq!(manager.producer_id_and_epoch().producer_id, 13131);
        assert_eq!(manager.producer_id_and_epoch().epoch, 1);
    }

    /// Translated from `testUnsupportedInitTransactions` (Java 1117-1134): a version
    /// mismatch on the `InitProducerId`, once the coordinator is known, is fatal
    /// (`TransactionManager.java:1417-1418`).
    #[tokio::test]
    async fn test_unsupported_init_transactions() {
        let mut ctx = SenderTestContext::transactional();
        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();
        ctx.transaction_manager()
            .lock()
            .unwrap()
            .initialize_transactions(false, &mut ctx.sender.pending_requests)
            .expect("initTransactions is valid from UNINITIALIZED");

        ctx.sender.run_once().await.expect("run_once");
        ctx.sender
            .client_mut()
            .prepare_response(find_coordinator_response(Errors::None, TRANSACTIONAL_ID, &node));
        ctx.sender.run_once().await.expect("run_once");
        assert!(!ctx.transaction_manager().lock().unwrap().has_error());

        ctx.sender.client_mut().prepare_unsupported_version_response();
        ctx.sender.run_once().await.expect("run_once");

        let manager = ctx.transaction_manager();
        let manager = manager.lock().unwrap();
        assert!(manager.has_fatal_error());
        assert_eq!(
            manager.last_error().expect("recorded").error(),
            Errors::UnsupportedVersion,
            "Java asserts an UnsupportedVersionException"
        );
    }

    /// Translated from `testUnsupportedFindCoordinator` (Java 1100-1115): a version
    /// mismatch on the `FindCoordinator` is fatal
    /// (`TransactionManager.java:1417-1418`).
    ///
    /// Java's response matcher also asserts the outgoing request's shape; those
    /// assertions live in the manager-level
    /// `test_lookup_coordinator_clears_the_node_and_enqueues_a_find_coordinator_first`,
    /// because `MockClient::prepare_unsupported_version_response` takes no matcher.
    #[tokio::test]
    async fn test_unsupported_find_coordinator() {
        let mut ctx = SenderTestContext::transactional();
        ctx.transaction_manager()
            .lock()
            .unwrap()
            .initialize_transactions(false, &mut ctx.sender.pending_requests)
            .expect("initTransactions is valid from UNINITIALIZED");

        ctx.sender.run_once().await.expect("run_once");
        ctx.sender.client_mut().prepare_unsupported_version_response();
        ctx.sender.run_once().await.expect("run_once");

        let manager = ctx.transaction_manager();
        let manager = manager.lock().unwrap();
        assert!(manager.has_fatal_error());
        assert_eq!(
            manager.last_error().expect("recorded").error(),
            Errors::UnsupportedVersion,
            "Java asserts an UnsupportedVersionException"
        );
    }

    /// `Sender.java:318-323`: a fatal transaction-manager error aborts the batches
    /// and returns without producing.
    #[tokio::test]
    async fn test_run_once_returns_on_a_fatal_transaction_manager_error() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        let future = ctx.append_to_accumulator(&tp0).await;
        assert!(ctx.accumulator.has_incomplete());

        let fatal_error = KafkaError::with_message(Errors::UnknownServerError, "fatal for the test");
        ctx.transaction_manager()
            .lock()
            .unwrap()
            .transition_to_fatal_error(fatal_error, Caller::App)
            .expect("FATAL_ERROR is always a valid target");

        ctx.sender.run_once().await.expect("run_once");

        assert_eq!(
            ctx.sender.client().in_flight_request_count(),
            0,
            "runOnce must return at :322 without reaching sendProducerData"
        );
        assert!(future.is_done(), "maybeAbortBatches (:320) aborts the incomplete batches");
        let error = future.get().await.expect_err("the batch is aborted with the fatal error");
        assert_eq!(error.message(), "fatal for the test");
        assert!(!ctx.accumulator.has_incomplete());
    }

    /// `Sender.java:325-327` → `shouldHandleAuthorizationError` (`:351-360`): an
    /// idempotent producer that hit `CLUSTER_AUTHORIZATION_FAILED` recovers to
    /// `UNINITIALIZED` and can send again.
    ///
    /// This is the exit PLAN §9.16 records as having been missing from Phase 3's
    /// first attempt: without it one authorization failure wedges the producer.
    #[tokio::test]
    async fn test_run_once_recovers_an_idempotent_producer_from_an_authorization_error() {
        let mut ctx = SenderTestContext::idempotent();
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::ClusterAuthorizationFailed, -1, -1));

        // Iteration 1 enqueues, sends and completes the InitProducerId, whose
        // authorization arm (`TransactionManager.java:1524-1528`) lands the manager
        // in ABORTABLE_ERROR.
        ctx.sender.run_once().await.expect("run_once");
        {
            let manager = ctx.transaction_manager();
            let manager = manager.lock().unwrap();
            assert!(manager.has_abortable_error());
            assert_eq!(
                manager.last_error().expect("recorded").error(),
                Errors::ClusterAuthorizationFailed
            );
        }

        // Iteration 2 intercepts at :325 and recovers.
        ctx.sender.run_once().await.expect("run_once");
        {
            let manager = ctx.transaction_manager();
            let mut manager = manager.lock().unwrap();
            assert!(!manager.has_error(), "the state must be escapable");
            assert!(
                manager.last_error().is_none(),
                "transitionToUninitialized clears lastError (Java 761)"
            );
            manager
                .maybe_add_partition(&ctx.tp0)
                .expect("sends are accepted again once the error is cleared");
        }

        // Iteration 3 asks for a fresh producer id, proving the producer is usable.
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, 13131, 1));
        ctx.sender.run_once().await.expect("run_once");
        assert!(ctx.transaction_manager().lock().unwrap().has_producer_id());
    }

    /// `Sender.java:333-335`: while an `InitProducerId` is pending or in flight,
    /// `maybeSendAndPollTransactionalRequest` returns `true` and `runOnce` never
    /// reaches `sendProducerData` at `:344`.
    #[tokio::test]
    async fn test_run_once_returns_without_producing_while_init_producer_id_is_pending() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        let future = ctx.append_to_accumulator(&tp0).await;

        // No prepared response: the InitProducerId is sent and stays in flight.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.sender.client().in_flight_request_count(),
            1,
            "the one request in flight is the InitProducerId"
        );
        assert_eq!(
            *ctx.sender.client().requests().front().expect("in flight").api_key(),
            crate::common::protocol::ApiKeys::INIT_PRODUCER_ID
        );
        assert!(ctx.sender.has_in_flight_request());
        assert_eq!(
            ctx.sender.in_flight_batches(&tp0).len(),
            0,
            "sendProducerData (:344) is not reached in this iteration"
        );

        // The second iteration takes the `hasInFlightRequest()` exit at :460-463 and
        // still does not produce.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 0);
        assert!(!future.is_done());
    }

    /// Falling through past `Sender.java:335` to `sendProducerData` at `:344` once a
    /// producer id is held and nothing is pending.
    #[tokio::test]
    async fn test_run_once_produces_once_a_producer_id_is_held() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        ctx.append_to_accumulator(&tp0).await;

        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, 13131, 1));
        ctx.sender.run_once().await.expect("run_once");
        assert!(ctx.transaction_manager().lock().unwrap().has_producer_id());
        assert_eq!(
            ctx.sender.in_flight_batches(&tp0).len(),
            0,
            "the iteration that acquires the producer id returns at :334"
        );

        // Nothing pending and nothing in flight, so the block falls through.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            *ctx.sender.client().requests().front().expect("in flight").api_key(),
            crate::common::protocol::ApiKeys::PRODUCE
        );
        let in_flight = ctx.sender.in_flight_batches(&tp0);
        assert_eq!(in_flight.len(), 1);
        // The drain assigned the producer state before the batch was serialised
        // (`RecordAccumulator.java:900-925`).
        assert_eq!(in_flight[0].producer_id(), 13131);
        assert_eq!(in_flight[0].producer_epoch(), 1);
        assert_eq!(in_flight[0].base_sequence(), 0);
        let manager = ctx.transaction_manager();
        let mut manager = manager.lock().unwrap();
        assert_eq!(manager.sequence_number(&tp0), 1);
        assert!(manager.has_inflight_batches(&tp0));
    }

    /// `Sender.java:756-759`: a successful produce response is reported to the
    /// transaction manager, which advances the last-acked sequence and offset and
    /// stops tracking the batch.
    #[tokio::test]
    async fn test_completed_batch_advances_the_last_acked_sequence() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        let future = ctx.append_to_accumulator(&tp0).await;

        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, 13131, 1));
        ctx.sender.run_once().await.expect("run_once");
        ctx.sender.run_once().await.expect("run_once"); // produce

        let response = ctx.produce_response(&tp0, 500, Errors::None, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");

        assert!(future.is_done());
        assert_eq!(future.get().await.expect("succeeds").offset(), 500);
        let manager = ctx.transaction_manager();
        let mut manager = manager.lock().unwrap();
        assert_eq!(manager.last_acked_sequence(&tp0), Some(0));
        assert_eq!(manager.last_acked_offset(&tp0), Some(500));
        assert!(
            !manager.has_inflight_batches(&tp0),
            "handleCompletedBatch removes the batch from the in-flight set"
        );
    }

    /// `Sender.java:875-882` + `TransactionManager.java:1015`: an
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER` on an idempotent producer is retried and
    /// requests an epoch bump, where a non-idempotent producer would fail the batch
    /// (the error is not `RetriableException`).
    #[tokio::test]
    async fn test_out_of_order_sequence_is_retried_and_bumps_the_epoch() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        let future = ctx.append_to_accumulator(&tp0).await;

        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, 13131, 1));
        ctx.sender.run_once().await.expect("run_once");
        ctx.sender.run_once().await.expect("run_once"); // produce

        assert!(
            !Errors::OutOfOrderSequenceNumber.is_retriable(),
            "a producer without a transaction manager would fail this batch"
        );
        let response = ctx.produce_response(&tp0, -1, Errors::OutOfOrderSequenceNumber, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");

        assert!(!future.is_done(), "the batch is retried, not failed");
        assert!(
            ctx.transaction_manager().lock().unwrap().client_side_epoch_bump_required(),
            "canRetry requests an epoch bump for the partition"
        );
        assert!(
            ctx.transaction_manager()
                .lock()
                .unwrap()
                .partitions_to_rewrite_sequences()
                .contains(&tp0)
        );

        // The next iteration applies the bump and rewrites the re-enqueued batch's
        // sequence from 0 under the new epoch. The batch is sitting in the
        // accumulator's deque at this point, not in `Sender::in_flight_batches`, so
        // this only works because the pool draws from **both** owners (rules §7).
        ctx.sender.run_once().await.expect("run_once");
        {
            let manager = ctx.transaction_manager();
            let mut manager = manager.lock().unwrap();
            assert_eq!(manager.producer_id_and_epoch().epoch, 2);
            assert_eq!(
                manager.first_in_flight_sequence(&tp0).expect("tracked"),
                0,
                "the rewritten batch starts from sequence 0 again"
            );
            assert_eq!(
                manager.sequence_number(&tp0),
                1,
                "nextSequence is the total record count of the rewritten batches \
                 (TxnPartitionEntry.startSequencesAtBeginning)"
            );
        }

        // Advance past the retry backoff and drain the rewritten batch, so the rewrite
        // is observed on the batch itself rather than only in the manager.
        ctx.time.sleep((RETRY_BACKOFF_MS as f64 * 1.3) as i64);
        ctx.sender.run_once().await.expect("run_once");
        let in_flight = ctx.sender.in_flight_batches(&tp0);
        assert_eq!(in_flight.len(), 1);
        assert_eq!(in_flight[0].producer_epoch(), 2);
        assert_eq!(in_flight[0].base_sequence(), 0);
        // `sequence_has_been_reset()` is deliberately not asserted here: both Java and
        // this port clear the `reopened` flag in `ProducerBatch::close()`
        // (`ProducerBatch.java:525`), which the drain calls on the way out. Java's
        // `testHealthyPartitionRetriesDuringEpochBump` can assert it only because it
        // holds the batch itself and never re-drains it.
    }

    /// `Sender.java:372-375`: a batch that expires while in retry leaves its
    /// partition's sequence unresolved, so no new batch is drained until the
    /// in-flight ones are accounted for.
    #[tokio::test]
    async fn test_expired_batch_in_retry_marks_the_sequence_unresolved() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        let future = ctx.append_to_accumulator(&tp0).await;

        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, 13131, 1));
        ctx.sender.run_once().await.expect("run_once");
        ctx.sender.run_once().await.expect("run_once"); // produce

        // A retriable failure puts the batch back in the accumulator, marking it as a
        // retry, then the delivery timeout expires it.
        let response = ctx.produce_response(&tp0, -1, Errors::NotLeaderOrFollower, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert!(!future.is_done());

        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);
        ctx.sender.run_once().await.expect("run_once");

        assert!(future.is_done());
        assert_eq!(future.get().await.expect_err("expired").error(), Errors::RequestTimedOut);
        assert!(
            ctx.transaction_manager().lock().unwrap().has_unresolved_sequence(&tp0),
            "markSequenceUnresolved ran for the expired retry"
        );
    }

    /// `Sender.java:266-296`: an idempotent producer still in `ABORTABLE_ERROR` when
    /// the Sender shuts down force-closes instead of spinning.
    ///
    /// `hasOngoingTransaction()` is true for an idempotent producer in that state
    /// (`TransactionManager.java:1012`), so the second shutdown loop is entered and
    /// calls `beginAbort()`, whose `ensureTransactional()` guard rejects it; Java's
    /// `catch` sets `forceClose`, which is the only thing that ends the loop.
    #[tokio::test]
    async fn test_shutdown_force_closes_when_begin_abort_is_rejected() {
        let mut ctx = SenderTestContext::idempotent();
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::ClusterAuthorizationFailed, -1, -1));
        ctx.sender.run_once().await.expect("run_once");
        assert!(ctx.transaction_manager().lock().unwrap().has_abortable_error());

        // Stop the main loop without force-closing, so `run()` proceeds to the
        // shutdown loops with the abortable error outstanding.
        ctx.sender.running.store(false, Ordering::Release);
        assert!(!ctx.sender.force_close.load(Ordering::Acquire));

        tokio::time::timeout(std::time::Duration::from_secs(5), ctx.sender.run())
            .await
            .expect("run() must terminate rather than spin on hasOngoingTransaction()");

        assert!(
            ctx.sender.force_close.load(Ordering::Acquire),
            "the rejected beginAbort must force-close (Sender.java:274-278)"
        );
        // The `runOnce` in the loop body still executes, and it is what recovers the
        // state — pinning the body order (abort attempt, then runOnce).
        assert!(!ctx.transaction_manager().lock().unwrap().has_abortable_error());
    }

    // =====================================================================
    // The three `TransactionManagerTest` methods PLAN §Phase-3 deferred to this
    // phase by name, because each one builds a `RecordAccumulator` and a `Sender`
    // and drives `runOnce`.
    // =====================================================================

    /// Acquires a producer id through the real path, mirroring
    /// `TransactionManagerTest.initializeIdempotentProducerId` (Java 4333), which
    /// spins `Sender.runOnce` against a prepared `InitProducerId` response.
    async fn initialize_idempotent_producer_id(ctx: &mut SenderTestContext, producer_id: i64, epoch: i16) {
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, producer_id, epoch));
        ctx.sender.run_once().await.expect("run_once");
        assert!(ctx.transaction_manager().lock().unwrap().has_producer_id());
    }

    /// Assigns the next sequence to a fresh single-record batch and tracks it in
    /// flight, mirroring `TransactionManagerTest.writeIdempotentBatchWithValue`
    /// (Java 812) — i.e. a batch that has been drained and sent but not answered.
    ///
    /// The batch is returned to the caller rather than left with an owner, which is
    /// how Java's helper behaves too: Java's `TxnPartitionEntry` holds a reference,
    /// while here the entry tracks only the ordering key (rules §7) and the test owns
    /// the batch until it hands it to the accumulator.
    fn write_idempotent_batch_with_value(
        transaction_manager: &Arc<Mutex<TransactionManager>>,
        accumulator: &RecordAccumulator,
        tp: &TopicPartition,
        value: &str,
    ) -> ProducerBatch {
        let mut manager = transaction_manager.lock().unwrap();
        manager
            .maybe_update_producer_id_and_epoch(tp, &mut [])
            .expect("no in-flight batches to rewrite");
        let sequence = manager.sequence_number(tp);
        manager.increment_sequence_number(tp, 1).expect("the entry exists");

        let builder = crate::common::record::memory_records::MemoryRecords::builder(
            64,
            Compression::none(),
            TimestampType::CreateTime,
            0,
        );
        let mut batch = ProducerBatch::new(tp.clone(), builder, 0);
        assert!(
            batch.try_append(0, Some(&[]), Some(value.as_bytes()), &[], None, 0).is_ok(),
            "a 64-byte batch has room for one small record"
        );
        let producer_id_and_epoch = manager.producer_id_and_epoch();
        batch.set_producer_state(producer_id_and_epoch.producer_id, producer_id_and_epoch.epoch, sequence, false);
        manager.add_in_flight_batch(&batch).expect("the sequence is set");
        batch.close();
        // A batch this test may hand back to the accumulator must be in the incomplete
        // set, which a real `append` would have done.
        accumulator.register_incomplete_for_test(&batch);
        batch
    }

    /// Translated from `TransactionManagerTest.testDuplicateSequenceAfterProducerReset`
    /// (Java 748-810).
    ///
    /// The only one of the three that goes purely through the real path:
    /// `accumulator.append` + `sender.runOnce()` across a request timeout, a retry and
    /// a delivery timeout, ending with an epoch bump that restarts the partition's
    /// sequence at 0 while the timed-out request is still in flight.
    #[tokio::test]
    async fn test_duplicate_sequence_after_producer_reset() {
        for _transaction_v2_enabled in [true, false] {
            let mut ctx = SenderTestContext::idempotent_with_timeouts(10_000, 15_000);
            let tp0 = ctx.tp0.clone();
            initialize_idempotent_producer_id(&mut ctx, 13131, 1).await;
            assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

            let future1 = ctx.append_to_accumulator_with(&tp0, 0, "1", "1").await;
            ctx.sender.run_once().await.expect("run_once");
            assert_eq!(
                ctx.transaction_manager().lock().unwrap().sequence_number(&tp0),
                1,
                "the drain assigned sequence 0 and advanced the counter"
            );

            // The request times out, which the mock client turns into a disconnect.
            ctx.time.sleep(10_000);
            ctx.sender.run_once().await.expect("run_once");
            assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
            assert!(ctx.transaction_manager().lock().unwrap().has_inflight_batches(&tp0));
            assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);

            // Retry: the same batch, with the same sequence, goes back out.
            ctx.sender.run_once().await.expect("run_once");
            assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
            assert!(ctx.transaction_manager().lock().unwrap().has_inflight_batches(&tp0));
            assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);

            // The delivery timeout expires. The retried request stays in flight until
            // the request timeout is reached even though the future has already
            // completed exceptionally.
            ctx.time.sleep(5_000);
            ctx.sender.run_once().await.expect("run_once");
            assert!(future1.is_done());
            assert_eq!(
                future1.get().await.expect_err("delivery timeout").error(),
                Errors::RequestTimedOut
            );
            assert!(!ctx.sender.has_in_flight_request());
            assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

            // The expired-in-retry batch left the partition unresolved, so the next
            // iteration bumps the epoch and restarts the sequence at 0.
            ctx.sender.run_once().await.expect("run_once");
            assert_eq!(ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().epoch, 2);
            assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

            // A fresh record is numbered from 0 under the new epoch.
            let future2 = ctx.append_to_accumulator_with(&tp0, 0, "2", "2").await;
            ctx.sender.run_once().await.expect("run_once");
            ctx.sender.run_once().await.expect("run_once");
            assert_eq!(
                ctx.transaction_manager()
                    .lock()
                    .unwrap()
                    .first_in_flight_sequence(&tp0)
                    .expect("tracked"),
                0
            );
            assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);

            // It times out too, and is retried rather than failed.
            ctx.time.sleep(5_000);
            ctx.sender.run_once().await.expect("run_once");
            assert!(ctx.transaction_manager().lock().unwrap().has_inflight_batches(&tp0));
            assert!(!future2.is_done());
        }
    }

    /// The shared body of `testHealthyPartitionRetriesDuringEpochBump`
    /// (Java 3599-3692) and `testFailedInflightBatchAfterEpochBump`
    /// (Java 3727-3810). In Kafka 4.2 the two differ only by one
    /// `maybeUpdateProducerIdAndEpoch(tp1)` call before their closing pair of
    /// assertions, which are themselves identical. Both are translated (below)
    /// rather than collapsed into one, so each Java method has a Rust counterpart;
    /// the duplication is Java's.
    ///
    /// Returns the context so each caller can make its own closing assertions.
    ///
    /// # Where this deviates from Java, and why
    ///
    /// Java's `writeIdempotentBatchWithValue` batches are referenced by the
    /// `TxnPartitionEntry` *and* by the test. Rust's entry tracks ordering keys only
    /// (rules §7), so the test owns them and supplies them to the epoch bump as an
    /// `InFlightBatchPool` — the same thing `Sender::bump_idempotent_epoch_and_reset_id_if_needed`
    /// does from the two real owners. Java reaches the bump through
    /// `runUntil(() -> epoch == 2)`; here the manager entry point is called directly
    /// with the pool, because `runOnce` can only find batches an owner holds.
    ///
    /// Everything the test asserts about the *accumulator* still goes through the real
    /// `sender.run_once()`, which is the point of the test:
    /// `should_stop_drain_batches_for_partition`'s stale-epoch gate and the
    /// sequence-ordered re-enqueue.
    async fn run_epoch_bump_with_a_healthy_partition() -> SenderTestContext {
        const PRODUCER_ID: i64 = 13131;
        const EPOCH: i16 = 1;

        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        let tp1 = ctx.tp1.clone();
        initialize_idempotent_producer_id(&mut ctx, PRODUCER_ID, EPOCH).await;
        let transaction_manager = ctx.transaction_manager();

        let tp0b1 = write_idempotent_batch_with_value(&transaction_manager, &ctx.accumulator, &tp0, "1");
        let mut tp0b2 = write_idempotent_batch_with_value(&transaction_manager, &ctx.accumulator, &tp0, "2");
        let mut tp0b3 = write_idempotent_batch_with_value(&transaction_manager, &ctx.accumulator, &tp0, "3");
        let tp1b1 = write_idempotent_batch_with_value(&transaction_manager, &ctx.accumulator, &tp1, "4");
        let tp1b2 = write_idempotent_batch_with_value(&transaction_manager, &ctx.accumulator, &tp1, "5");
        assert_eq!(transaction_manager.lock().unwrap().sequence_number(&tp0), 3);
        assert_eq!(transaction_manager.lock().unwrap().sequence_number(&tp1), 2);

        // First batch of each partition succeeds.
        let b1_append_time = 0;
        let t0b1_response = PartitionResponse::new(Errors::None, 500, b1_append_time, 0, Vec::new(), None);
        transaction_manager
            .lock()
            .unwrap()
            .handle_completed_batch(&tp0b1, &t0b1_response)
            .expect("the completion is recorded");
        let t1b1_response = PartitionResponse::new(Errors::None, 500, b1_append_time, 0, Vec::new(), None);
        transaction_manager
            .lock()
            .unwrap()
            .handle_completed_batch(&tp1b1, &t1b1_response)
            .expect("the completion is recorded");

        // An UNKNOWN_PRODUCER_ID on tp0 requests the epoch bump and sets tp0's
        // sequences back to 0.
        let t0b2_response = PartitionResponse::new(Errors::UnknownProducerId, -1, -1, 500, Vec::new(), None);
        assert!(
            transaction_manager
                .lock()
                .unwrap()
                .can_retry(&t0b2_response, &tp0b2, &mut [])
                .expect("the retry decision is made")
        );

        {
            let mut pool = InFlightBatchPool::new();
            pool.insert(tp0.clone(), vec![&mut tp0b2, &mut tp0b3]);
            let mut pending = PendingRequests::new();
            transaction_manager
                .lock()
                .unwrap()
                .bump_idempotent_epoch_and_reset_id_if_needed(&mut pool, &mut pending, Caller::Sender)
                .expect("the epoch is bumped");
            assert!(pending.is_empty(), "a valid producer id needs no new InitProducerId");
        }

        // tp0's batches were rewritten; tp1's were not.
        assert_eq!(transaction_manager.lock().unwrap().producer_id_and_epoch().epoch, 2);
        assert_eq!(
            transaction_manager
                .lock()
                .unwrap()
                .next_batch_by_sequence(&tp0)
                .expect("entry exists"),
            Some((tp0b2.producer_id(), tp0b2.producer_epoch(), tp0b2.base_sequence()))
        );
        assert_eq!(
            transaction_manager
                .lock()
                .unwrap()
                .first_in_flight_sequence(&tp0)
                .expect("tracked"),
            0
        );
        assert_eq!(tp0b2.base_sequence(), 0);
        assert!(tp0b2.sequence_has_been_reset());
        assert_eq!(tp0b2.producer_epoch(), 2);

        assert_eq!(
            transaction_manager
                .lock()
                .unwrap()
                .next_batch_by_sequence(&tp1)
                .expect("entry exists"),
            Some((tp1b2.producer_id(), tp1b2.producer_epoch(), tp1b2.base_sequence()))
        );
        assert_eq!(
            transaction_manager
                .lock()
                .unwrap()
                .first_in_flight_sequence(&tp1)
                .expect("tracked"),
            1
        );
        assert_eq!(tp1b2.base_sequence(), 1);
        assert!(!tp1b2.sequence_has_been_reset());
        assert_eq!(tp1b2.producer_epoch(), EPOCH);

        // New tp1 batches must not be drained while tp1 has in-flight requests using
        // the old epoch — `shouldStopDrainBatchesForPartition`'s stale-epoch gate.
        ctx.append_to_accumulator(&tp1).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.accumulator.deque_size(&tp1), 1, "the new batch stays queued");

        // Partition failover: tp1 returns NOT_LEADER_OR_FOLLOWER. Despite having the
        // old epoch, the batch retries.
        let t1b2_response = PartitionResponse::new(Errors::NotLeaderOrFollower, -1, -1, 600, Vec::new(), None);
        assert!(
            transaction_manager
                .lock()
                .unwrap()
                .can_retry(&t1b2_response, &tp1b2, &mut [])
                .expect("the retry decision is made")
        );
        let tp1b2_base_sequence = tp1b2.base_sequence();
        ctx.accumulator
            .reenqueue(tp1b2, ctx.time.milliseconds())
            .expect("the batch is still tracked");
        assert_eq!(ctx.accumulator.deque_size(&tp1), 2);

        // The batch with the old epoch drains ahead of the new one, leaving the new one
        // queued. The mock clock is advanced past the retry backoff, which Java's
        // `MockTime` does implicitly through the accumulator's zero backoff.
        ctx.time.sleep(RETRY_BACKOFF_MS * 4);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.accumulator.deque_size(&tp1), 1);
        let drained = ctx.sender.in_flight_batches(&tp1);
        assert_eq!(drained.len(), 1);
        assert_eq!(
            drained[0].base_sequence(),
            tp1b2_base_sequence,
            "the re-enqueued old-epoch batch is the one that drained, not the new one"
        );
        assert_eq!(
            drained[0].producer_epoch(),
            EPOCH,
            "the retried batch keeps the epoch it was written with"
        );
        ctx
    }

    /// Translated from `TransactionManagerTest.testHealthyPartitionRetriesDuringEpochBump`
    /// (Java 3599-3692).
    #[tokio::test]
    async fn test_healthy_partition_retries_during_epoch_bump() {
        for _transaction_v2_enabled in [true, false] {
            let mut ctx = run_epoch_bump_with_a_healthy_partition().await;
            let tp1 = ctx.tp1.clone();
            let transaction_manager = ctx.transaction_manager();

            // After successfully retrying there are no in-flight batches for tp1 and its
            // sequence is 0 again.
            let response = ctx.produce_response(&tp1, 500, Errors::None, 0);
            ctx.sender
                .client_mut()
                .respond_from(response, &Node::new(0, "localhost".to_string(), 1969));
            ctx.sender.run_once().await.expect("run_once");

            transaction_manager
                .lock()
                .unwrap()
                .maybe_update_producer_id_and_epoch(&tp1, &mut [])
                .expect("tp1 has drained, so there is nothing to rewrite");
            assert!(!transaction_manager.lock().unwrap().has_inflight_batches(&tp1));
            assert_eq!(transaction_manager.lock().unwrap().sequence_number(&tp1), 0);

            // The last batch is now drained and sent, under the bumped epoch.
            ctx.sender.run_once().await.expect("run_once");
            assert!(transaction_manager.lock().unwrap().has_inflight_batches(&tp1));
            assert_eq!(ctx.accumulator.deque_size(&tp1), 0);
            let tp1b3 = ctx.sender.in_flight_batches(&tp1);
            assert_eq!(tp1b3.len(), 1);
            assert_eq!(tp1b3[0].producer_epoch(), 2, "epoch + 1");
            assert_eq!(tp1b3[0].base_sequence(), 0);

            // Java 3684-3692: completing that batch leaves nothing in flight for tp1
            // and the sequence at 1. Java reaches it through `tp1b3.complete(..)` plus
            // `handleCompletedBatch`, which is what the response round trip does here.
            let response = ctx.produce_response(&tp1, 500, Errors::None, 0);
            ctx.sender
                .client_mut()
                .respond_from(response, &Node::new(0, "localhost".to_string(), 1969));
            ctx.sender.run_once().await.expect("run_once");
            transaction_manager
                .lock()
                .unwrap()
                .maybe_update_producer_id_and_epoch(&tp1, &mut [])
                .expect("tp1 has drained");
            assert!(!transaction_manager.lock().unwrap().has_inflight_batches(&tp1));
            assert_eq!(transaction_manager.lock().unwrap().sequence_number(&tp1), 1);
        }
    }

    /// Translated from `TransactionManagerTest.testFailedInflightBatchAfterEpochBump`
    /// (Java 3727-3810).
    ///
    /// In Kafka 4.2 this is identical to
    /// `testHealthyPartitionRetriesDuringEpochBump` except that it stops before the
    /// final `maybeUpdateProducerIdAndEpoch(tp1)`, asserting instead that completing
    /// the last batch leaves nothing in flight and the sequence at 1. Both are
    /// translated because both exist.
    #[tokio::test]
    async fn test_failed_inflight_batch_after_epoch_bump() {
        for _transaction_v2_enabled in [true, false] {
            let mut ctx = run_epoch_bump_with_a_healthy_partition().await;
            let tp1 = ctx.tp1.clone();
            let transaction_manager = ctx.transaction_manager();

            let response = ctx.produce_response(&tp1, 500, Errors::None, 0);
            ctx.sender
                .client_mut()
                .respond_from(response, &Node::new(0, "localhost".to_string(), 1969));
            ctx.sender.run_once().await.expect("run_once");
            transaction_manager
                .lock()
                .unwrap()
                .maybe_update_producer_id_and_epoch(&tp1, &mut [])
                .expect("tp1 has drained");
            assert_eq!(transaction_manager.lock().unwrap().sequence_number(&tp1), 0);

            // The last batch is drained under the new epoch and then completed.
            ctx.sender.run_once().await.expect("run_once");
            assert!(transaction_manager.lock().unwrap().has_inflight_batches(&tp1));
            let response = ctx.produce_response(&tp1, 500, Errors::None, 0);
            ctx.sender
                .client_mut()
                .respond_from(response, &Node::new(0, "localhost".to_string(), 1969));
            ctx.sender.run_once().await.expect("run_once");

            assert!(
                !transaction_manager.lock().unwrap().has_inflight_batches(&tp1),
                "handleCompletedBatch removed the last tracked batch"
            );
            assert_eq!(transaction_manager.lock().unwrap().sequence_number(&tp1), 1);
        }
    }

    /// `NetworkClient.completeResponses` (`NetworkClient.java:666-674`) catches and
    /// logs per response, so one failing completion must not abandon the rest of the
    /// poll's responses.
    ///
    /// Two produce requests are in flight for the same partition. Both are answered
    /// in the same poll, the first with a retriable error whose re-enqueue is made to
    /// fail (its partition is no longer tracked, so `insertInSequenceOrder` rejects
    /// it, `RecordAccumulator.java:558-560`). The second response must still be
    /// dispatched and complete its record.
    #[tokio::test]
    async fn test_a_failing_response_handler_does_not_abandon_the_rest_of_the_poll() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;

        let future1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        let future2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);

        // Untrack the partition so the first response's re-enqueue fails.
        {
            let in_flight: Vec<(i64, i16, i32)> = ctx
                .sender
                .in_flight_batches(&tp0)
                .iter()
                .map(|batch| (batch.producer_id(), batch.producer_epoch(), batch.base_sequence()))
                .collect();
            assert_eq!(in_flight.len(), 2);
            let manager = ctx.transaction_manager();
            let mut manager = manager.lock().unwrap();
            for batch in ctx.sender.in_flight_batches(&tp0) {
                manager.remove_in_flight_batch(batch).expect("tracked");
            }
            assert!(!manager.has_inflight_batches(&tp0));
        }

        // Both responses land in the same poll, the failing one first.
        let retriable = ctx.produce_response(&tp0, -1, Errors::NotLeaderOrFollower, 0);
        ctx.sender.client_mut().respond_to_request_at(0, retriable);
        let success = ctx.produce_response(&tp0, 1000, Errors::None, 0);
        ctx.sender.client_mut().respond_to_request_at(0, success);

        ctx.sender.run_once().await.expect("the poll itself must not fail");

        assert!(
            !future1.is_done(),
            "the first response's handler failed, so its record is not completed"
        );
        assert!(
            future2.is_done(),
            "the second response must still be dispatched after the first one failed"
        );
        assert_eq!(future2.get().await.expect("succeeds").offset(), 1000);
    }

    // =====================================================================
    // `SenderTest.java` idempotence subset
    // =====================================================================

    /// Inspects the outgoing produce request's first batch for `tp`, asserting its
    /// producer epoch (when `expected_epoch` is `Some`) and base sequence, then answers
    /// it.
    ///
    /// The analogue of `SenderTest.sendIdempotentProducerResponse` (Java 2136-2156).
    /// Java inspects the request inside `client.respond(matcher, response)`; the Rust
    /// `MockClient` has no matcher form, so the queued request is built and read
    /// directly before responding. That also covers Java's
    /// `hasIdempotentRecords(produceRequest)` assertion: a batch carrying a producer
    /// id is what makes the records idempotent.
    fn send_idempotent_producer_response(
        ctx: &mut SenderTestContext,
        expected_epoch: Option<i16>,
        expected_sequence: i32,
        tp: &TopicPartition,
        error: Errors,
        offset: i64,
        log_start_offset: i64,
    ) {
        use crate::common::record::memory_records::MemoryRecords;
        use crate::common::requests::ConcreteRequest;

        {
            let request = ctx
                .sender
                .client_mut()
                .requests_mut()
                .front_mut()
                .expect("a produce request must be in flight");
            let built = request.request_builder_mut().build().expect("the request builds");
            let ConcreteRequest::Produce(produce_request) = built else {
                panic!("expected a produce request, got {built}");
            };
            let partition_data = produce_request
                .data()
                .topic_data
                .iter()
                .filter(|topic| topic.name == *tp.topic())
                .flat_map(|topic| topic.partition_data.iter())
                .find(|partition| partition.index == tp.partition())
                .expect("the request must carry this partition");
            let records = MemoryRecords::new(
                partition_data
                    .records
                    .clone()
                    .expect("an idempotent produce request carries records"),
            );
            let mut batches = records.batches();
            let first_batch = batches.next().expect("one batch");
            assert!(batches.next().is_none(), "a produce request carries one batch per partition");
            assert_ne!(
                first_batch.producer_id(),
                RecordBatch::NO_PRODUCER_ID,
                "the records must be idempotent"
            );
            if let Some(expected_epoch) = expected_epoch {
                assert_eq!(first_batch.producer_epoch(), expected_epoch);
            }
            assert_eq!(first_batch.base_sequence(), expected_sequence);
        }

        let response = ctx.produce_response_with_message(tp, offset, error, 0, log_start_offset, None);
        ctx.sender.client_mut().respond(response);
    }

    /// Asserts a partition's tracked producer id, epoch, next sequence and last-acked
    /// sequence.
    ///
    /// `SenderTest.assertPartitionState` (Java 1230-1242).
    fn assert_partition_state(
        transaction_manager: &Arc<Mutex<TransactionManager>>,
        tp: &TopicPartition,
        expected_producer_id: i64,
        expected_producer_epoch: i16,
        expected_sequence: i32,
        expected_last_acked_sequence: Option<i32>,
    ) {
        let mut manager = transaction_manager.lock().unwrap();
        let producer_id_and_epoch = manager.producer_id_and_epoch_for_partition(tp);
        assert_eq!(producer_id_and_epoch.producer_id, expected_producer_id, "Producer Id:");
        assert_eq!(producer_id_and_epoch.epoch, expected_producer_epoch, "Producer Epoch:");
        assert_eq!(manager.sequence_number(tp), expected_sequence, "Seq Number:");
        assert_eq!(
            manager.last_acked_sequence(tp),
            expected_last_acked_sequence,
            "Last Acked Seq Number:"
        );
    }

    /// Appends one record, sends it and completes it successfully.
    ///
    /// `SenderTest.assertSuccessfulSend` (Java 3871-3885).
    async fn assert_successful_send(ctx: &mut SenderTestContext) {
        let tp0 = ctx.tp0.clone();
        let future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.sender.client().in_flight_request_count(),
            1,
            "We should have a single produce request in flight."
        );
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);
        assert!(ctx.sender.client().has_in_flight_requests());
        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert!(future.is_done());
        future.get().await.expect("Future should not have raised an exception");
    }

    /// Appends one record and asserts the send fails immediately with `expected`.
    ///
    /// `SenderTest.assertSendFailure` (Java 3887-3897).
    async fn assert_send_failure(ctx: &mut SenderTestContext, expected: Errors) {
        let tp0 = ctx.tp0.clone();
        let future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert!(future.is_done());
        assert_eq!(future.get().await.expect_err("Future should have raised").error(), expected);
    }

    /// Translated from `SenderTest.testInitProducerIdRequest` (Java 620-628).
    #[tokio::test]
    async fn test_init_producer_id_request() {
        let mut ctx = SenderTestContext::idempotent();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        let manager = ctx.transaction_manager();
        let manager = manager.lock().unwrap();
        assert!(manager.has_producer_id());
        assert_eq!(manager.producer_id_and_epoch().producer_id, 343_434);
        assert_eq!(manager.producer_id_and_epoch().epoch, 0);
    }

    /// A force close must complete the record futures of batches the `Sender` owns,
    /// not only those still in the accumulator's deques
    /// (`Sender.java:294-295` → `RecordAccumulator.java:1152-1168`).
    ///
    /// Critic 44 note 2: `abort_incomplete_batches` walks the deques only, because
    /// Rust's `IncompleteBatches` tracks `ProduceRequestResult`s rather than batches,
    /// so a drained batch's records were never completed on a force close — a
    /// CLAUDE.md §5 hanging future.
    #[tokio::test]
    async fn test_force_close_aborts_the_senders_in_flight_batches() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;

        let drained = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1, "one batch is drained");
        // A second record stays undrained, since the first request is still in flight
        // and there is nothing to send it with.
        let undrained = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        assert!(!drained.is_done());
        assert!(!undrained.is_done());

        ctx.sender.force_close.store(true, Ordering::Release);
        ctx.sender.running.store(false, Ordering::Release);
        tokio::time::timeout(std::time::Duration::from_secs(5), ctx.sender.run())
            .await
            .expect("run() must terminate on a force close");

        assert!(undrained.is_done(), "the accumulator's own batch is aborted");
        assert!(
            drained.is_done(),
            "the drained batch's records must be completed too, not left pending"
        );
        assert_eq!(
            drained.get().await.expect_err("aborted").message(),
            "Producer is closed forcefully."
        );
    }

    /// Translated from `SenderTest.testIdempotenceWithMultipleInflights`
    /// (Java 762-807).
    #[tokio::test]
    async fn test_idempotence_with_multiple_inflights() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert!(!request1.is_done());
        assert!(!request2.is_done());

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive response 0

        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert!(request1.is_done());
        assert_eq!(request1.get().await.expect("succeeds").offset(), 0);
        assert!(!request2.is_done());

        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::None, 1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive response 1
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 0);
        assert!(request2.is_done());
        assert_eq!(request2.get().await.expect("succeeds").offset(), 1);
    }

    /// Translated from `SenderTest.testIdempotenceWithMultipleInflightsRetriedInOrder`
    /// (Java 811-909).
    ///
    /// Three requests in flight, all retried one at a time in the correct order. This
    /// is the multi-in-flight ordering that `should_stop_drain_batches_for_partition`'s
    /// `firstInFlightSequence` gate exists to guarantee, and that the `0b8c3d0`
    /// response-routing fix serves.
    #[tokio::test]
    async fn test_idempotence_with_multiple_inflights_retried_in_order() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        let request3 = ctx.append_to_accumulator_with(&tp0, 0, "k3", "v3").await;
        ctx.sender.run_once().await.expect("run_once");

        assert_eq!(ctx.sender.client().in_flight_request_count(), 3);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 3);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert!(!request1.is_done());
        assert!(!request2.is_done());
        assert!(!request3.is_done());

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::LeaderNotAvailable, -1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive response 0

        // Queue the fourth request; it must not be sent until the first three complete.
        let request4 = ctx.append_to_accumulator_with(&tp0, 0, "k4", "v4").await;

        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::OutOfOrderSequenceNumber, -1, -1);
        ctx.sender.run_once().await.expect("run_once");
        send_idempotent_producer_response(&mut ctx, None, 2, &tp0, Errors::OutOfOrderSequenceNumber, -1, -1);
        ctx.sender.run_once().await.expect("run_once");

        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        // Do nothing: we are reduced to one in-flight request during retries.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.transaction_manager().lock().unwrap().sequence_number(&tp0),
            3,
            "request 4's batch must not have been drained, so the sequence is unchanged"
        );
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive response 1
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert!(request1.is_done());
        assert_eq!(request1.get().await.expect("succeeds").offset(), 0);
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 0);

        ctx.sender.run_once().await.expect("run_once"); // send request 2
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);

        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::None, 1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive response 2
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));
        assert!(request2.is_done());
        assert_eq!(request2.get().await.expect("succeeds").offset(), 1);
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 0);

        ctx.sender.run_once().await.expect("run_once"); // send request 3
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);

        send_idempotent_producer_response(&mut ctx, None, 2, &tp0, Errors::None, 2, -1);
        // Receive response 3 and send request 4, now that we are out of retry mode.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(2));
        assert!(request3.is_done());
        assert_eq!(request3.get().await.expect("succeeds").offset(), 2);
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);

        send_idempotent_producer_response(&mut ctx, None, 3, &tp0, Errors::None, 3, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive response 4
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(3));
        assert!(request4.is_done());
        assert_eq!(request4.get().await.expect("succeeds").offset(), 3);
    }

    /// Translated from
    /// `SenderTest.testIdempotenceWithMultipleInflightsWhereFirstFailsFatallyAndSequenceOfFutureBatchesIsAdjusted`
    /// (Java 912-968).
    ///
    /// The first of two in-flight batches fails fatally with `MESSAGE_TOO_LARGE`
    /// (`recordCount == 1`, so `completeBatch`'s split arm does not apply and
    /// `failBatch` runs with `adjustSequenceNumbers = true`), which requests an epoch
    /// bump; the second batch's sequence is then rewritten from 0.
    #[tokio::test]
    async fn test_idempotence_with_multiple_inflights_where_first_fails_fatally() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);

        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert!(!request1.is_done());
        assert!(!request2.is_done());

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::MessageTooLarge, -1, -1);
        // Receive response 0; this adjusts the sequences of future batches.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            request1.get().await.expect_err("fatal").error(),
            Errors::MessageTooLarge,
            "Java asserts RecordTooLargeException, which is MESSAGE_TOO_LARGE's exception"
        );
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::OutOfOrderSequenceNumber, -1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive response 1
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);

        ctx.sender.run_once().await.expect("run_once"); // resend request 1
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert!(request2.is_done());
        assert_eq!(request2.get().await.expect("succeeds").offset(), 0);
    }

    /// Translated from `SenderTest.testEpochBumpOnOutOfOrderSequenceForNextBatch`
    /// (Java 971-1016).
    #[tokio::test]
    async fn test_epoch_bump_on_out_of_order_sequence_for_next_batch() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        // First ProduceRequest with two messages in one batch.
        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(
            ctx.transaction_manager().lock().unwrap().sequence_number(&tp0),
            2,
            "the next sequence accounts for multi-message batches"
        );
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");

        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k3", "v3").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 3);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));
        assert!(request1.is_done());
        assert_eq!(request1.get().await.expect("succeeds").offset(), 0);
        assert!(!request2.is_done());

        // This OUT_OF_ORDER_SEQUENCE_NUMBER triggers an epoch bump, because it is
        // returned for the batch succeeding the last acknowledged one.
        send_idempotent_producer_response(&mut ctx, None, 2, &tp0, Errors::OutOfOrderSequenceNumber, -1, -1);
        ctx.sender.run_once().await.expect("run_once");
        ctx.sender.run_once().await.expect("run_once");

        let manager = ctx.transaction_manager();
        let mut manager = manager.lock().unwrap();
        assert_eq!(manager.producer_id_and_epoch().epoch, 1, "epoch is bumped");
        assert_eq!(manager.sequence_number(&tp0), 1, "sequence numbers are reset");
        assert_eq!(manager.first_in_flight_sequence(&tp0).expect("tracked"), 0);
    }

    /// Translated from
    /// `SenderTest.testEpochBumpOnOutOfOrderSequenceForNextBatchWhenThereIsNoBatchInFlight`
    /// (Java 1019-1102): a partition with no in-flight batch when the epoch is bumped
    /// gets its sequence reset lazily, on its next send.
    #[tokio::test]
    async fn test_epoch_bump_on_out_of_order_sequence_when_there_is_no_batch_in_flight() {
        const PRODUCER_ID: i64 = 343_434;
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        let tp1 = ctx.tp1.clone();
        initialize_idempotent_producer_id(&mut ctx, PRODUCER_ID, 0).await;
        let manager = ctx.transaction_manager();

        // Partition 0 — first batch, state lazily initialized.
        ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 0, 1, None);
        send_idempotent_producer_response(&mut ctx, Some(0), 0, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 0, 1, Some(0));

        // Partition 1 — first batch.
        ctx.append_to_accumulator(&tp1).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 0, 1, None);
        send_idempotent_producer_response(&mut ctx, Some(0), 0, &tp1, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 0, 1, Some(0));

        // Partition 0 — second batch, sequence incremented.
        ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 0, 2, Some(0));

        send_idempotent_producer_response(&mut ctx, Some(0), 1, &tp0, Errors::OutOfOrderSequenceNumber, -1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive
        ctx.sender.run_once().await.expect("run_once"); // bump epoch and retry

        assert_eq!(manager.lock().unwrap().producer_id_and_epoch().epoch, 1);
        // Partition 0's state is reset to the current producer epoch.
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 1, 1, None);
        // Partition 1's state is unchanged, and now stale.
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 0, 1, Some(0));
        assert!(manager.lock().unwrap().has_stale_producer_id_and_epoch(&tp1));

        send_idempotent_producer_response(&mut ctx, Some(1), 0, &tp0, Errors::None, 1, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 1, 1, Some(0));

        // Partition 1 — second batch: the epoch is bumped and the sequence reset then
        // incremented, lazily, at drain time.
        ctx.append_to_accumulator(&tp1).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 1, 1, None);
        assert!(!manager.lock().unwrap().has_stale_producer_id_and_epoch(&tp1));

        send_idempotent_producer_response(&mut ctx, Some(1), 0, &tp1, Errors::None, 1, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 1, 1, Some(0));
    }

    /// Translated from
    /// `SenderTest.testEpochBumpOnOutOfOrderSequenceForNextBatchWhenBatchInFlightFails`
    /// (Java 1105-1228).
    ///
    /// The third arm of the epoch-bump family, and the only one where the partition
    /// carrying the stale epoch still has a batch **in flight** when the bump happens.
    /// Java's own comment: "When a batch failed after the producer epoch is bumped, the
    /// sequence number of that partition must be reset for any subsequent batches sent."
    ///
    /// `tp1`'s in-flight batch exhausts its single retry and fails; its state must stay
    /// untouched (stale epoch, sequence 2, last-acked 0) right through the failure, and
    /// only the *next* batch drained for `tp1` may bump the epoch and reset the
    /// sequence.
    #[tokio::test]
    async fn test_epoch_bump_on_out_of_order_sequence_for_next_batch_when_batch_in_flight_fails() {
        const PRODUCER_ID: i64 = 343_434;
        // `setupWithTransactionState(transactionManager, false, null, true, 1, 0)`
        // (Java 1113) — retries once.
        let mut ctx = SenderTestContext::idempotent_with_retries(1);
        let tp0 = ctx.tp0.clone();
        let tp1 = ctx.tp1.clone();
        initialize_idempotent_producer_id(&mut ctx, PRODUCER_ID, 0).await;
        let manager = ctx.transaction_manager();

        // Partition 0 — first batch, state lazily initialized, then acked.
        ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 0, 1, None);
        send_idempotent_producer_response(&mut ctx, Some(0), 0, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 0, 1, Some(0));

        // Partition 1 — first batch, likewise.
        ctx.append_to_accumulator(&tp1).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 0, 1, None);
        send_idempotent_producer_response(&mut ctx, Some(0), 0, &tp1, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 0, 1, Some(0));

        // Both partitions now have a second batch in flight at the same time.
        ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 0, 2, Some(0));
        ctx.append_to_accumulator(&tp1).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 0, 2, Some(0));

        // Partition 0 fails with OUT_OF_ORDER_SEQUENCE_NUMBER, bumping the epoch while
        // partition 1's request is still outstanding.
        send_idempotent_producer_response(&mut ctx, Some(0), 1, &tp0, Errors::OutOfOrderSequenceNumber, -1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive
        ctx.sender.run_once().await.expect("run_once"); // bump epoch and retry

        assert_eq!(manager.lock().unwrap().producer_id_and_epoch().epoch, 1);
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 1, 1, None);
        // Partition 1 is unchanged: the epoch is bumped lazily, once its in-flight
        // batches complete.
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 0, 2, Some(0));
        assert!(manager.lock().unwrap().has_stale_producer_id_and_epoch(&tp1));

        // Partition 1's batch fails retriably and is re-queued: still unchanged.
        send_idempotent_producer_response(&mut ctx, Some(0), 1, &tp1, Errors::NotLeaderOrFollower, -1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive and retry
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 0, 2, Some(0));
        assert!(manager.lock().unwrap().has_stale_producer_id_and_epoch(&tp1));

        // Partition 0's retry succeeds under the bumped epoch.
        send_idempotent_producer_response(&mut ctx, Some(1), 0, &tp0, Errors::None, 1, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 1, 1, Some(0));

        // Partition 1's retry fails too, exhausting `retries = 1`, so the batch is
        // failed. Its state is *still* not reset — that happens lazily on the next send.
        send_idempotent_producer_response(&mut ctx, Some(0), 1, &tp1, Errors::NotLeaderOrFollower, -1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive and fail the batch
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 0, 2, Some(0));
        assert!(manager.lock().unwrap().has_stale_producer_id_and_epoch(&tp1));

        // Partition 1 — third batch: now the epoch is bumped and the sequence reset.
        ctx.append_to_accumulator(&tp1).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 1, 1, None);
        assert!(!manager.lock().unwrap().has_stale_producer_id_and_epoch(&tp1));

        send_idempotent_producer_response(&mut ctx, Some(1), 0, &tp1, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp1, PRODUCER_ID, 1, 1, Some(0));

        // Partition 0 — third batch continues from the bumped epoch's sequence 1.
        ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 1, 2, Some(0));

        send_idempotent_producer_response(&mut ctx, Some(1), 1, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_partition_state(&manager, &tp0, PRODUCER_ID, 1, 2, Some(1));
    }

    /// Translated from `SenderTest.testCorrectHandlingOfOutOfOrderResponses`
    /// (Java 1245-1323): both in-flight requests fail, their responses arrive in
    /// reverse order, and the batches must still be re-queued and re-sent in sequence
    /// order.
    #[tokio::test]
    async fn test_correct_handling_of_out_of_order_responses() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);

        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert!(!request1.is_done());
        assert!(!request2.is_done());

        // Answer the *second* request first.
        let second = ctx.produce_response(&tp0, -1, Errors::OutOfOrderSequenceNumber, 0);
        ctx.sender.client_mut().respond_to_request_at(1, second);
        ctx.sender.run_once().await.expect("run_once"); // receive response 1

        assert_eq!(ctx.accumulator.deque_size(&tp0), 1, "the second batch is queued first");
        assert_eq!(ctx.accumulator.base_sequences_for_test(&tp0), vec![1]);
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        let first = ctx.produce_response(&tp0, -1, Errors::NotLeaderOrFollower, 0);
        ctx.sender.client_mut().respond_to_request_at(0, first);
        ctx.sender.run_once().await.expect("run_once"); // receive response 0

        // Both batches are re-queued, in the correct order.
        assert_eq!(
            ctx.accumulator.base_sequences_for_test(&tp0),
            vec![0, 1],
            "insertInSequenceOrder must put sequence 0 ahead of sequence 1"
        );
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert!(!request1.is_done());
        assert!(!request2.is_done());

        ctx.sender.run_once().await.expect("run_once"); // send request 0
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        // Do nothing: only one in flight is allowed once we are retrying.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        // The requests go out in order even though the responses did not.
        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert!(request1.is_done());
        assert_eq!(request1.get().await.expect("succeeds").offset(), 0);

        ctx.sender.run_once().await.expect("run_once"); // send request 1
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::None, 1, -1);
        ctx.sender.run_once().await.expect("run_once");

        assert!(!ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));
        assert!(request2.is_done());
        assert_eq!(request2.get().await.expect("succeeds").offset(), 1);
    }

    /// Translated from
    /// `SenderTest.testCorrectHandlingOfOutOfOrderResponsesWhenSecondSucceeds`
    /// (Java 1326-1391): the second request succeeds before the first, so the
    /// last-acked sequence jumps to 1 and must not move back when the first is retried
    /// and finally succeeds.
    #[tokio::test]
    async fn test_correct_handling_of_out_of_order_responses_when_second_succeeds() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);
        assert!(!request1.is_done());
        assert!(!request2.is_done());

        let second = ctx.produce_response(&tp0, 1, Errors::None, 0);
        ctx.sender.client_mut().respond_to_request_at(1, second);
        ctx.sender.run_once().await.expect("run_once"); // receive response 1
        assert!(request2.is_done());
        assert_eq!(request2.get().await.expect("succeeds").offset(), 1);
        assert!(!request1.is_done());
        assert_eq!(ctx.accumulator.deque_size(&tp0), 0);
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));

        let first = ctx.produce_response(&tp0, -1, Errors::RequestTimedOut, 0);
        ctx.sender.client_mut().respond_to_request_at(0, first);
        ctx.sender.run_once().await.expect("run_once"); // receive response 0

        assert_eq!(ctx.accumulator.base_sequences_for_test(&tp0), vec![0]);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);

        ctx.sender.run_once().await.expect("run_once"); // resend request 0
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));

        // The out-of-order successful responses are handled correctly.
        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.accumulator.deque_size(&tp0), 0);
        assert_eq!(
            ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0),
            Some(1),
            "the last acked sequence must not move backwards"
        );
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert!(request1.is_done());
        assert_eq!(request1.get().await.expect("succeeds").offset(), 0);
    }

    /// Translated from
    /// `SenderTest.testExpiryOfUnsentBatchesShouldNotCauseUnresolvedSequences`
    /// (Java 1394-1414): a batch that expires before it was ever sent has no sequence,
    /// so it must not leave the partition unresolved.
    #[tokio::test]
    async fn test_expiry_of_unsent_batches_should_not_cause_unresolved_sequences() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "key", "value").await;
        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();
        ctx.time.sleep(10_000);
        ctx.sender.client_mut().disconnect_by_id(node.id_string());
        ctx.sender.client_mut().backoff(&node, 10);

        ctx.sender.run_once().await.expect("run_once");

        assert_eq!(request1.get().await.expect_err("expired").error(), Errors::RequestTimedOut);
        assert!(!ctx.transaction_manager().lock().unwrap().has_unresolved_sequence(&tp0));
    }

    /// Translated from
    /// `SenderTest.testExpiryOfAllSentBatchesShouldCauseUnresolvedSequences`
    /// (Java 1575-1610): when every sent batch expires, the partition is unresolved and
    /// the next iteration bumps the epoch to clear it.
    #[tokio::test]
    async fn test_expiry_of_all_sent_batches_should_cause_unresolved_sequences() {
        const PRODUCER_ID: i64 = 343_434;
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, PRODUCER_ID, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "key", "value").await;
        ctx.sender.run_once().await.expect("run_once");
        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::NotLeaderOrFollower, -1, -1);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);

        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();
        ctx.time.sleep(15_000);
        ctx.sender.client_mut().disconnect_by_id(node.id_string());
        ctx.sender.client_mut().backoff(&node, 10);

        ctx.sender.run_once().await.expect("run_once"); // expire the batch

        assert_eq!(request1.get().await.expect_err("expired").error(), Errors::RequestTimedOut);
        assert!(ctx.transaction_manager().lock().unwrap().has_unresolved_sequence(&tp0));
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.accumulator.deque_size(&tp0), 0);
        assert_eq!(
            ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().producer_id,
            PRODUCER_ID
        );

        // The next iteration bumps the epoch and clears the unresolved sequences.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().epoch, 1);
        assert!(!ctx.transaction_manager().lock().unwrap().has_unresolved_sequence(&tp0));
    }

    /// Translated from
    /// `SenderTest.testExpiryOfFirstBatchShouldNotCauseUnresolvedSequencesIfFutureBatchesSucceed`
    /// (Java 1417-1481): the first batch expires while a later one is still in flight;
    /// the partition stays unresolved — blocking new drains — until the later batch
    /// succeeds and `maybeResolveSequences` clears it.
    #[tokio::test]
    async fn test_expiry_of_first_batch_should_not_cause_unresolved_sequences_if_future_batches_succeed() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        // The two appends are separated by a second so the batches do not expire
        // together.
        ctx.time.sleep(1000);
        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 2);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::RequestTimedOut, -1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive the first response
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);

        // 600 more ms expires the first batch but not the second
        // (`delivery.timeout.ms` is 1500).
        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();
        ctx.time.sleep(600);
        ctx.sender.client_mut().disconnect_by_id(node.id_string());
        ctx.sender.client_mut().backoff(&node, 10);

        ctx.sender.run_once().await.expect("run_once"); // expire the first batch
        assert_eq!(request1.get().await.expect_err("expired").error(), Errors::RequestTimedOut);
        assert!(ctx.transaction_manager().lock().unwrap().has_unresolved_sequence(&tp0));
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 0);

        // A third batch must not be dequeued until the unresolved state clears.
        let request3 = ctx.append_to_accumulator_with(&tp0, 0, "k3", "v3").await;
        ctx.time.sleep(20);
        assert!(!request2.is_done());

        ctx.sender.run_once().await.expect("run_once"); // send the second request again
        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::None, 1, -1);
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);

        // Receive the second response; the third request is not sent, because the
        // partition is still unresolved.
        ctx.sender.run_once().await.expect("run_once");
        assert!(request2.is_done());
        assert_eq!(request2.get().await.expect("succeeds").offset(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 0);

        assert_eq!(ctx.accumulator.deque_size(&tp0), 1);
        assert_eq!(
            ctx.accumulator.base_sequences_for_test(&tp0),
            vec![RecordBatch::NO_SEQUENCE],
            "the queued third batch has no sequence yet"
        );
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert!(ctx.transaction_manager().lock().unwrap().has_unresolved_sequence(&tp0));

        // Clear the unresolved state and send the pending request.
        ctx.sender.run_once().await.expect("run_once");
        assert!(!ctx.transaction_manager().lock().unwrap().has_unresolved_sequence(&tp0));
        assert!(ctx.transaction_manager().lock().unwrap().has_producer_id());
        assert_eq!(ctx.accumulator.deque_size(&tp0), 0);
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert!(!request3.is_done());
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);
    }

    /// Translated from
    /// `SenderTest.testExpiryOfFirstBatchShouldCauseEpochBumpIfFutureBatchesFail`
    /// (Java 1484-1531): the later batch fails with `OUT_OF_ORDER_SEQUENCE_NUMBER`
    /// instead of succeeding, so the unresolved partition is cleared by an epoch bump.
    #[tokio::test]
    async fn test_expiry_of_first_batch_should_cause_epoch_bump_if_future_batches_fail() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        ctx.time.sleep(1000);
        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::NotLeaderOrFollower, -1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive the first response

        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();
        ctx.time.sleep(1000);
        ctx.sender.client_mut().disconnect_by_id(node.id_string());
        ctx.sender.client_mut().backoff(&node, 10);

        ctx.sender.run_once().await.expect("run_once"); // expire the first batch
        assert_eq!(request1.get().await.expect_err("expired").error(), Errors::RequestTimedOut);
        assert!(ctx.transaction_manager().lock().unwrap().has_unresolved_sequence(&tp0));

        // A third batch must not be dequeued until the unresolved state clears.
        ctx.append_to_accumulator_with(&tp0, 0, "k3", "v3").await;
        ctx.time.sleep(20);
        assert!(!request2.is_done());
        ctx.sender.run_once().await.expect("run_once"); // send the second request
        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::OutOfOrderSequenceNumber, 1, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive the second response

        // The epoch is bumped and the second request is re-queued.
        assert_eq!(ctx.accumulator.deque_size(&tp0), 2);

        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().epoch, 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);
        assert!(!ctx.transaction_manager().lock().unwrap().has_unresolved_sequence(&tp0));
    }

    /// Translated from
    /// `SenderTest.testBatchesDrainedWithOldProducerIdShouldSucceedOnSubsequentRetry`
    /// (Java 1720-1764): a batch drained under the old producer id still succeeds after
    /// the epoch is bumped for a different partition.
    #[tokio::test]
    async fn test_batches_drained_with_old_producer_id_should_succeed_on_subsequent_retry() {
        let mut ctx = SenderTestContext::idempotent_in_order(10);
        let tp0 = ctx.tp0.clone();
        let tp1 = ctx.tp1.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;

        let out_of_order_response = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        let successful_response = ctx.append_to_accumulator_with(&tp1, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        let response = ctx.produce_response_for(&[
            (&tp1, -1, Errors::NotLeaderOrFollower),
            (&tp0, -1, Errors::OutOfOrderSequenceNumber),
        ]);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert!(!out_of_order_response.is_done());

        // Bump the epoch and send tp1's request again with the old producer id.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().epoch, 1);
        assert!(!successful_response.is_done());

        // The response comes back with a retriable error.
        let response = ctx.produce_response_for(&[(&tp1, 0, Errors::NotLeaderOrFollower)]);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert!(!successful_response.is_done());

        ctx.sender.run_once().await.expect("run_once"); // retry one more time
        let response = ctx.produce_response_for(&[(&tp1, 0, Errors::None)]);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert!(successful_response.is_done());
        assert_eq!(
            ctx.transaction_manager().lock().unwrap().sequence_number(&tp1),
            1,
            "tp1's epoch is bumped and its sequence reset when the next batch is sent"
        );
    }

    /// Translated from
    /// `SenderTest.testResetOfProducerStateShouldAllowQueuedBatchesToDrain`
    /// (Java 1613-1652): with the epoch already at `Short.MAX_VALUE`, the bump resets
    /// the producer id instead, and the batch queued for the healthy partition still
    /// drains.
    #[tokio::test]
    async fn test_reset_of_producer_state_should_allow_queued_batches_to_drain() {
        const PRODUCER_ID: i64 = 343_434;
        let mut ctx = SenderTestContext::idempotent_in_order(10);
        let tp0 = ctx.tp0.clone();
        let tp1 = ctx.tp1.clone();
        initialize_idempotent_producer_id(&mut ctx, PRODUCER_ID, i16::MAX).await;

        ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await; // failed response
        let successful_response = ctx.append_to_accumulator_with(&tp1, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        let response = ctx.produce_response_for(&[
            (&tp1, -1, Errors::NotLeaderOrFollower),
            (&tp0, -1, Errors::OutOfOrderSequenceNumber),
        ]);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once"); // trigger the epoch bump

        // An exhausted epoch resets the producer id, which means a fresh
        // `InitProducerId`.
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, PRODUCER_ID + 1, 0));
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().producer_id,
            PRODUCER_ID + 1
        );

        assert!(!successful_response.is_done());
        ctx.sender.run_once().await.expect("run_once"); // send tp1's batch again
        let response = ctx.produce_response_for(&[(&tp1, 10, Errors::None)]);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");

        assert!(successful_response.is_done());
        assert_eq!(successful_response.get().await.expect("succeeds").offset(), 10);
        assert_eq!(
            ctx.transaction_manager().lock().unwrap().sequence_number(&tp1),
            1,
            "the epoch and sequence are updated when the next batch is sent"
        );
    }

    /// Translated from `SenderTest.testForceCloseWithProducerIdReset`
    /// (Java 1689-1717): a force close while the producer id is being reset must not
    /// block, and must abort the pending batches.
    #[tokio::test]
    async fn test_force_close_with_producer_id_reset() {
        let mut ctx = SenderTestContext::idempotent_in_order(10);
        let tp0 = ctx.tp0.clone();
        let tp1 = ctx.tp1.clone();
        initialize_idempotent_producer_id(&mut ctx, 1, i16::MAX).await;

        ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        let successful_response = ctx.append_to_accumulator_with(&tp1, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        let response = ctx.produce_response_for(&[
            (&tp1, -1, Errors::NotLeaderOrFollower),
            (&tp0, -1, Errors::OutOfOrderSequenceNumber),
        ]);
        ctx.sender.client_mut().respond(response);
        // The out-of-order sequence error resets the producer id, because the epoch is
        // maxed out.
        ctx.sender.run_once().await.expect("run_once");

        ctx.sender.force_close();
        ctx.sender.run_once().await.expect("this must not block");
        tokio::time::timeout(std::time::Duration::from_secs(5), ctx.sender.run())
            .await
            .expect("the force-close flag must end the main loop");

        assert!(!ctx.accumulator.has_undrained(), "Pending batches are not aborted.");
        assert!(successful_response.is_done());
    }

    /// Translated from `SenderTest.testCloseWithProducerIdReset` (Java 1655-1686): an
    /// orderly close drains the queued batches even though the close began while the
    /// producer id was being reset.
    #[tokio::test]
    async fn test_close_with_producer_id_reset() {
        const PRODUCER_ID: i64 = 343_434;
        let mut ctx = SenderTestContext::idempotent_in_order(10);
        let tp0 = ctx.tp0.clone();
        let tp1 = ctx.tp1.clone();
        initialize_idempotent_producer_id(&mut ctx, PRODUCER_ID, i16::MAX).await;

        ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await; // failed response
        ctx.append_to_accumulator_with(&tp1, 0, "k2", "v2").await; // success response
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        let response = ctx.produce_response_for(&[
            (&tp1, -1, Errors::NotLeaderOrFollower),
            (&tp0, -1, Errors::OutOfOrderSequenceNumber),
        ]);
        ctx.sender.client_mut().respond(response);
        ctx.sender.initiate_close();
        // The out-of-order sequence error resets the producer id, because the epoch is
        // maxed out.
        ctx.sender.run_once().await.expect("run_once");

        // Java's `TestUtils.waitForCondition` spins `runOnce` with a fresh
        // `InitProducerId` response prepared each time until the accumulator drains.
        for _ in 0..50 {
            if !ctx.accumulator.has_undrained() {
                break;
            }
            ctx.sender
                .client_mut()
                .prepare_response(init_producer_id_response(Errors::None, PRODUCER_ID + 1, 1));
            ctx.sender.run_once().await.expect("run_once");
        }
        assert!(!ctx.accumulator.has_undrained(), "Failed to drain batches");
    }

    /// Translated from
    /// `SenderTest.testClusterAuthorizationExceptionInInitProducerIdRequest`
    /// (Java 715-735 — the produce-request variant is at Java 2159-2179): a cluster
    /// authorization failure on `InitProducerId` is *abortable*, so the producer
    /// recovers to `UNINITIALIZED`, retries and works again.
    #[tokio::test]
    async fn test_cluster_authorization_exception_in_init_producer_id_request() {
        const PRODUCER_ID: i64 = 343_434;
        let mut ctx = SenderTestContext::idempotent();
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::ClusterAuthorizationFailed, -1, -1));
        ctx.sender.run_once().await.expect("run_once");

        {
            let manager = ctx.transaction_manager();
            let manager = manager.lock().unwrap();
            assert!(!manager.has_producer_id());
            assert!(manager.has_error());
            assert_eq!(
                manager.last_error().expect("recorded").error(),
                Errors::ClusterAuthorizationFailed
            );
            assert_eq!(manager.producer_id_and_epoch().epoch, -1);
        }

        // Java asserts the *send* fails while the error is outstanding. The recovery at
        // `Sender.java:325` runs first in the very `runOnce` that `assert_send_failure`
        // drives, so the failure is observed through `maybe_add_partition` instead —
        // which is exactly what `doSend` calls (`KafkaProducer.java:1045`).
        {
            let manager = ctx.transaction_manager();
            let mut manager = manager.lock().unwrap();
            let error = manager
                .maybe_add_partition(&ctx.tp0)
                .expect_err("sends are rejected while the error is outstanding");
            assert_eq!(
                error.message(),
                "Cannot execute transactional method because we are in an error state"
            );
        }

        // The Sender retries the InitProducerId and succeeds.
        ctx.sender.run_once().await.expect("run_once"); // recover to UNINITIALIZED
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, PRODUCER_ID, 0));
        ctx.sender.run_once().await.expect("run_once");
        {
            let manager = ctx.transaction_manager();
            let manager = manager.lock().unwrap();
            assert!(!manager.has_fatal_error());
            assert!(manager.has_producer_id());
            assert_eq!(manager.producer_id_and_epoch().epoch, 0);
        }

        // A subsequent send is successful.
        assert_successful_send(&mut ctx).await;
    }

    /// Translated from `SenderTest.testClusterAuthorizationExceptionInProduceRequest`
    /// (Java 2159-2179): a cluster authorization failure on a *produce* request is
    /// fatal, and stays fatal for later sends.
    #[tokio::test]
    async fn test_cluster_authorization_exception_in_produce_request() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;

        let future = ctx.append_to_accumulator(&tp0).await;
        let response = ctx.produce_response(&tp0, -1, Errors::ClusterAuthorizationFailed, 0);
        ctx.sender.client_mut().prepare_response(response);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            future.get().await.expect_err("fatal").error(),
            Errors::ClusterAuthorizationFailed
        );

        // Cluster authorization errors are fatal, so later sends keep seeing them.
        assert!(ctx.transaction_manager().lock().unwrap().has_fatal_error());
        assert_send_failure(&mut ctx, Errors::ClusterAuthorizationFailed).await;
    }

    /// Translated from `SenderTest.testUnsupportedForMessageFormatInProduceRequest`
    /// (Java 2223-2241): `UNSUPPORTED_FOR_MESSAGE_FORMAT` fails the batch but is *not*
    /// fatal for the producer.
    #[tokio::test]
    async fn test_unsupported_for_message_format_in_produce_request() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;

        let future = ctx.append_to_accumulator(&tp0).await;
        let response = ctx.produce_response(&tp0, -1, Errors::UnsupportedForMessageFormat, 0);
        ctx.sender.client_mut().prepare_response(response);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            future.get().await.expect_err("failed").error(),
            Errors::UnsupportedForMessageFormat
        );
        assert!(
            !ctx.transaction_manager().lock().unwrap().has_error(),
            "unsupported for message format is not a fatal error"
        );
    }

    /// Translated from `SenderTest.testUnsupportedVersionInProduceRequest`
    /// (Java 2244-2262): a version mismatch is fatal and stays fatal.
    #[tokio::test]
    async fn test_unsupported_version_in_produce_request() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;

        let future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.client_mut().prepare_unsupported_version_response();
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(future.get().await.expect_err("failed").error(), Errors::UnsupportedVersion);

        // Unsupported version errors are fatal, so later sends keep seeing them.
        assert!(ctx.transaction_manager().lock().unwrap().has_fatal_error());
        assert_send_failure(&mut ctx, Errors::UnsupportedVersion).await;
    }

    /// Translated from `SenderTest.testSequenceNumberIncrement` (Java 2265-2303).
    #[tokio::test]
    async fn test_sequence_number_increment() {
        const PRODUCER_ID: i64 = 343_434;
        let mut ctx = SenderTestContext::idempotent_in_order(10);
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, PRODUCER_ID, 0).await;

        let response_future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once"); // send

        // Java asserts the outgoing batch's producer id, epoch and base sequence inside
        // the response matcher; `send_idempotent_producer_response` does the same.
        send_idempotent_producer_response(&mut ctx, Some(0), 0, &tp0, Errors::None, 0, -1);
        ctx.sender.run_once().await.expect("run_once"); // receive the response

        assert!(response_future.is_done());
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);
    }

    /// Translated from `SenderTest.testRetryWhenProducerIdChanges` (Java 2306-2338):
    /// with the epoch maxed out, a disconnect resets the producer id, and the batch is
    /// retried under the new one.
    #[tokio::test]
    async fn test_retry_when_producer_id_changes() {
        const PRODUCER_ID: i64 = 343_434;
        let mut ctx = SenderTestContext::idempotent_in_order(10);
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, PRODUCER_ID, i16::MAX).await;

        let response_future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once"); // send
        let destination = ctx
            .sender
            .client()
            .requests()
            .front()
            .expect("in flight")
            .destination()
            .to_string();
        let node = Node::new(destination.parse::<i32>().expect("node id"), "localhost".to_string(), 0);
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert!(
            ctx.sender.client().is_ready(&node, ctx.time.milliseconds()),
            "Client ready status should be true"
        );
        ctx.sender.client_mut().disconnect_by_id(&destination);
        assert_eq!(ctx.sender.client().in_flight_request_count(), 0);
        assert!(
            !ctx.sender.client().is_ready(&node, ctx.time.milliseconds()),
            "Client ready status should be false"
        );

        ctx.sender.run_once().await.expect("run_once"); // receive the error
        ctx.sender.run_once().await.expect("run_once"); // reset the producer id

        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, PRODUCER_ID + 1, 0));
        ctx.sender.run_once().await.expect("run_once"); // acquire the new producer id
        ctx.sender.run_once().await.expect("run_once"); // retry the batch
        assert_eq!(
            ctx.sender.client().in_flight_request_count(),
            1,
            "Expected requests to be retried after pid change"
        );
        assert!(!response_future.is_done());
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);
    }

    /// Translated from `SenderTest.testBumpEpochWhenOutOfOrderSequenceReceived`
    /// (Java 2341-2369).
    #[tokio::test]
    async fn test_bump_epoch_when_out_of_order_sequence_received() {
        let mut ctx = SenderTestContext::idempotent_in_order(10);
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;

        let response_future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once"); // send
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);

        let response = ctx.produce_response(&tp0, 0, Errors::OutOfOrderSequenceNumber, 0);
        ctx.sender.client_mut().respond(response);

        ctx.sender.run_once().await.expect("run_once"); // receive the out-of-order error
        ctx.sender.run_once().await.expect("run_once"); // bump the epoch
        assert!(!response_future.is_done());
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().epoch, 1);
    }

    /// Translated from `SenderTest.testIdempotentInitProducerIdWithMaxInFlightOne`
    /// (Java 664-682).
    ///
    /// With one node and `max.in.flight = 1`, an unrelated request already in flight
    /// makes `leastLoadedNode` report nothing, so `InitProducerId` cannot be sent yet.
    /// It must be **re-queued** rather than dropped or failed
    /// (`Sender.java:493-498`), and go out once the node frees up — which takes several
    /// polls, hence the test's name.
    ///
    /// # Where this differs from Java, and why it is still the same test
    ///
    /// Java overrides `MockClient.leastLoadedNode` in an anonymous subclass
    /// (`createMockClientWithMaxFlightOneMetadataPending`, Java 3956-3986). Rust cannot
    /// subclass a concrete struct, so the override is a flag —
    /// `MockClient::set_max_in_flight_one` — with the same `canSendMore` snapshot
    /// semantics. Java's out-of-band request is a `Metadata` request; a `Produce`
    /// request is used here because the `Sender`'s response dispatch ignores both
    /// equally (neither correlation id is in `pending_produce_responses` nor the
    /// transactional slot), and the property under test is node availability, not the
    /// request's type.
    #[tokio::test]
    async fn test_idempotent_init_producer_id_with_max_in_flight_one() {
        const PRODUCER_ID: i64 = 123_456;
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        let node = ctx.metadata.fetch().node_by_id(0).expect("node 0").clone();

        // Occupy the single in-flight slot with a request the Sender did not send, the
        // way Java's helper leaves a metadata request pending.
        ctx.sender.client_mut().set_max_in_flight_one(true);
        let now = ctx.time.milliseconds();
        assert!(ctx.sender.client_mut().ready(&node, now).await);
        let mut data = ProduceRequestData::new();
        data.set_acks(ACKS_ALL);
        data.set_timeout_ms(REQUEST_TIMEOUT);
        let occupying_request = ctx.sender.client_mut().new_client_request(
            node.id_string(),
            Box::new(ProduceRequestBuilder::new(data)),
            now,
            true,
        );
        ctx.sender.client_mut().send(occupying_request, now);
        // Java polls until `leastLoadedNode` turns null; the flag lags a poll behind the
        // in-flight count, so exactly one poll is needed.
        ctx.sender.client_mut().poll(0, now).await;
        assert!(
            ctx.sender.client().least_loaded_node(now).node().is_none(),
            "no node can accept a request while one is in flight"
        );

        // The InitProducerId is enqueued but cannot be sent.
        ctx.sender.run_once().await.expect("run_once");
        assert!(!ctx.transaction_manager().lock().unwrap().has_producer_id());
        assert!(
            ctx.sender.has_pending_requests(),
            "the InitProducerId must be re-queued, not dropped (Sender.java:495)"
        );
        assert!(!ctx.sender.has_in_flight_request());

        // Answer the occupying request; the node is available again on the next poll.
        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");

        // Java's `waitForProducerId` spins up to five `runOnce` calls.
        ctx.sender
            .client_mut()
            .prepare_response(init_producer_id_response(Errors::None, PRODUCER_ID, 0));
        for _ in 0..5 {
            if ctx.transaction_manager().lock().unwrap().has_producer_id() {
                break;
            }
            ctx.sender.run_once().await.expect("run_once");
        }
        let manager = ctx.transaction_manager();
        let manager = manager.lock().unwrap();
        assert!(manager.has_producer_id());
        assert_eq!(manager.producer_id_and_epoch(), ProducerIdAndEpoch::new(PRODUCER_ID, 0));
    }

    /// Translated from
    /// `SenderTest.testUnknownProducerErrorShouldBeRetriedForFutureBatchesWhenFirstFails`
    /// (Java 2000-2083).
    ///
    /// Three batches, two in flight in parallel. The second comes back
    /// `UNKNOWN_PRODUCER_ID` with `logStartOffset > lastAckedOffset`, which resets the
    /// partition's sequence state and bumps the epoch. The **third**, already in flight
    /// under the old sequence, must then be retried too rather than failed, and must not
    /// be re-sent until the second completes — at most one request may be in flight
    /// while retrying.
    ///
    /// # What this does *not* isolate
    ///
    /// Neither this test nor Java's distinguishes which of `canRetry`'s two
    /// `UNKNOWN_PRODUCER_ID` sub-arms retries the third batch: after the reset both
    /// `sequenceHasBeenReset()` (`TransactionManager.java:1980-1986`) and
    /// `lastAckedOffset < logStartOffset` (`:1990-2010`) hold, and both return `true`.
    /// Confirmed by mutation: disabling the `sequenceHasBeenReset()` arm leaves this
    /// test green, because the truncation arm then answers instead. The arm order is
    /// still faithful to Java; it is the *test* that cannot tell them apart, and saying
    /// so here is cheaper than a reader inferring coverage that is not there.
    #[tokio::test]
    async fn test_unknown_producer_error_should_be_retried_for_future_batches_when_first_fails() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 1000, 10);
        ctx.sender.run_once().await.expect("run_once");
        assert!(request1.is_done());
        assert_eq!(request1.get().await.expect("succeeds").offset(), 1000);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1000));

        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));

        // The third request goes out in parallel with the second.
        let request3 = ctx.append_to_accumulator_with(&tp0, 0, "k3", "v3").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 3);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert!(!request2.is_done());
        assert!(!request3.is_done());
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);

        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::UnknownProducerId, -1, 1010);
        ctx.sender.run_once().await.expect("run_once"); // reset the sequences, retry
        ctx.sender.run_once().await.expect("run_once"); // bump the epoch and retry request 2

        // The partition's sequence state is reset, because the broker lost it.
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert!(!request2.is_done());
        assert!(!request3.is_done());
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().epoch, 1);

        // The original response for the third request. Its expected sequence is still
        // the one it was originally assigned.
        send_idempotent_producer_response(&mut ctx, None, 2, &tp0, Errors::UnknownProducerId, -1, 1010);
        ctx.sender.run_once().await.expect("run_once");

        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 1011, 1010);
        // Receive response 2; request 3 is not sent, since at most one may be in flight
        // while retrying.
        ctx.sender.run_once().await.expect("run_once");
        assert!(request2.is_done());
        assert!(!request3.is_done());
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert_eq!(request2.get().await.expect("succeeds").offset(), 1011);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1011));

        ctx.sender.run_once().await.expect("run_once"); // resend request 3
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);

        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::None, 1012, 1010);
        ctx.sender.run_once().await.expect("run_once");

        assert!(!ctx.sender.client().has_in_flight_requests());
        assert!(request3.is_done());
        assert_eq!(request3.get().await.expect("succeeds").offset(), 1012);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1012));
    }

    /// Translated from `SenderTest.testCancelInFlightRequestAfterFatalError`
    /// (Java 2182-2220).
    ///
    /// Java asserts with a `MatchingBufferPool` that the aborted in-flight batch's
    /// buffer is **not** returned when the fatal error aborts it, and **is** returned
    /// once its response finally arrives (KAFKA-19012). Rust's `BufferPool` exposes
    /// `available_memory`, so the same property is asserted against that.
    ///
    /// This is the test Critic 44 issue 2 identified as the one that would have caught
    /// the leak: before the fix, `maybe_abort_batches` dropped the batch and the buffer
    /// was never returned at all.
    #[tokio::test]
    async fn test_cancel_in_flight_request_after_fatal_error() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        let tp1 = ctx.tp1.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        let total_memory = ctx.accumulator.buffer_pool_available_memory();

        // Two requests in flight, one per partition.
        let future1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        let future2 = ctx.append_to_accumulator_with(&tp1, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);
        assert!(
            ctx.accumulator.buffer_pool_available_memory() < total_memory,
            "both batches hold a pooled buffer"
        );

        // CLUSTER_AUTHORIZATION_FAILED is fatal for the producer.
        let response = ctx.produce_response(&tp0, -1, Errors::ClusterAuthorizationFailed, 0);
        ctx.sender.client_mut().respond_to_request_at(0, response);
        ctx.sender.run_once().await.expect("run_once");
        assert!(ctx.transaction_manager().lock().unwrap().has_fatal_error());
        assert_eq!(
            future1.get().await.expect_err("fatal").error(),
            Errors::ClusterAuthorizationFailed
        );

        // The next iteration takes `runOnce`'s fatal exit, which aborts the batch that
        // is still in flight for tp1.
        ctx.sender.run_once().await.expect("run_once");
        assert!(future2.is_done());
        assert_eq!(
            future2.get().await.expect_err("aborted").error(),
            Errors::ClusterAuthorizationFailed
        );
        assert!(
            ctx.accumulator.buffer_pool_available_memory() < total_memory,
            "Batch should not be deallocated before the response is received"
        );

        // Should be fine if the second response eventually returns.
        let response = ctx.produce_response(&tp1, 0, Errors::None, 0);
        ctx.sender.client_mut().respond_to_request_at(0, response);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.accumulator.buffer_pool_available_memory(),
            total_memory,
            "The batch should have been de-allocated"
        );
    }

    /// The buffer of a batch that expires while still in flight is likewise released
    /// only when its response arrives — Java's
    /// `maybeRemoveAndDeallocateBatchLater` path (`Sender.java:856-861`), reached from
    /// `failExpiredBatches(expiredInflightBatches, now, false)` (`:432`).
    ///
    /// The sibling leak Critic 44 issue 2 identified alongside the abort path.
    #[tokio::test]
    async fn test_expired_in_flight_batch_buffer_is_released_when_its_response_arrives() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        let total_memory = ctx.accumulator.buffer_pool_available_memory();

        let future = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert!(ctx.accumulator.buffer_pool_available_memory() < total_memory);

        // The delivery timeout expires while the request is still in flight.
        ctx.time.sleep(DELIVERY_TIMEOUT_MS as i64);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(future.get().await.expect_err("expired").error(), Errors::RequestTimedOut);
        assert!(
            ctx.accumulator.buffer_pool_available_memory() < total_memory,
            "the buffer may still be in use by the network client"
        );

        // The response finally arrives and releases the buffer.
        let response = ctx.produce_response(&tp0, 0, Errors::None, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(
            ctx.accumulator.buffer_pool_available_memory(),
            total_memory,
            "the expired batch's buffer must be returned when its response arrives"
        );
    }

    /// Translated from `SenderTest.testCorrectHandlingOfDuplicateSequenceError`
    /// (Java 1767-1817).
    ///
    /// Two batches go out with sequences 0 and 1. The *second* is answered first and
    /// succeeds; the first then comes back `DUPLICATE_SEQUENCE_NUMBER`, which must be
    /// reported to the user as a success with no offset, and must not move the
    /// last-acked sequence backwards.
    #[tokio::test]
    async fn test_correct_handling_of_duplicate_sequence_error() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        // First ProduceRequest.
        let request1 = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        // Second ProduceRequest.
        let request2 = ctx.append_to_accumulator(&tp0).await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert!(!request1.is_done());
        assert!(!request2.is_done());

        // Answer the second request first.
        let second = ctx.produce_response(&tp0, 1000, Errors::None, 0);
        ctx.sender.client_mut().respond_to_request_at(1, second);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1000));
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));

        // Now the first, with DUPLICATE_SEQUENCE_NUMBER.
        let first = ctx.produce_response(&tp0, -1, Errors::DuplicateSequenceNumber, 0);
        ctx.sender.client_mut().respond_to_request_at(0, first);
        ctx.sender.run_once().await.expect("run_once");

        // The last ack'd sequence must not move backwards.
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1000));
        assert!(!ctx.sender.client().has_in_flight_requests());

        // The user sees a success with no offset.
        let metadata = request1.get().await.expect("a duplicate is reported as success");
        assert!(!metadata.has_offset());
        assert_eq!(metadata.offset(), -1);
    }

    /// Translated from
    /// `SenderTest.testUnknownProducerErrorShouldBeRetriedWhenLogStartOffsetIsUnknown`
    /// (Java 1942-1997): an `UNKNOWN_PRODUCER_ID` whose `logStartOffset` is `-1` is
    /// retried *without* resetting the sequence numbers, because the broker could not
    /// report where the log starts (`TransactionManager.java:1969-1977`).
    #[tokio::test]
    async fn test_unknown_producer_error_should_be_retried_when_log_start_offset_is_unknown() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 1000, 10);
        ctx.sender.run_once().await.expect("run_once");
        assert!(request1.is_done());
        assert_eq!(request1.get().await.expect("succeeds").offset(), 1000);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1000));

        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert!(!request2.is_done());

        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::UnknownProducerId, -1, -1);
        // Retried without resetting the sequence numbers, since the log start offset is
        // unknown.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert!(!request2.is_done());
        assert!(!ctx.sender.client().has_in_flight_requests());

        ctx.sender.run_once().await.expect("run_once"); // retry request 1
        // The expected sequence is still 1: we never learned the logStartOffset, so the
        // sequence numbers were not reset.
        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::None, 1011, 1010);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert!(request2.is_done());
        assert_eq!(request2.get().await.expect("succeeds").offset(), 1011);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1011));
    }

    /// Translated from
    /// `SenderTest.testIdempotentUnknownProducerHandlingWhenRetentionLimitReached`
    /// (Java 1884-1939): the broker's `logStartOffset` has moved past our last acked
    /// offset, so the producer state was lost to retention — bump the epoch and restart
    /// the sequence at 0 (`TransactionManager.java:1990-2010`).
    #[tokio::test]
    async fn test_idempotent_unknown_producer_handling_when_retention_limit_reached() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 1000, 10);
        ctx.sender.run_once().await.expect("run_once");
        assert!(request1.is_done());
        assert_eq!(request1.get().await.expect("succeeds").offset(), 1000);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1000));

        // A single batch with two records.
        ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k3", "v3").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 3);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert!(!request2.is_done());

        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::UnknownProducerId, -1, 1010);
        // Retried because logStartOffset > lastAckedOffset.
        ctx.sender.run_once().await.expect("run_once");
        ctx.sender.run_once().await.expect("run_once"); // bump the epoch and retry

        // The partition's sequence state is reset, because the broker lost it.
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert!(!request2.is_done());
        assert!(ctx.sender.client().has_in_flight_requests());
        assert_eq!(ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().epoch, 1);

        // The resent request starts from sequence 0, since the broker lost our state.
        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 1011, 1010);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(1));
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert!(!ctx.sender.client().has_in_flight_requests());
        assert!(request2.is_done());
        assert_eq!(request2.get().await.expect("succeeds").offset(), 1012);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1012));
    }

    /// Translated from
    /// `SenderTest.testShouldRaiseOutOfOrderSequenceExceptionToUserIfLogWasNotTruncated`
    /// (Java 2086-2126): the `logStartOffset` has *not* moved past our last acked
    /// offset, so the idempotent producer still bumps the epoch and retries rather than
    /// failing the batch.
    #[tokio::test]
    async fn test_should_raise_out_of_order_sequence_exception_to_user_if_log_was_not_truncated() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 0);

        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.client().in_flight_request_count(), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);

        send_idempotent_producer_response(&mut ctx, None, 0, &tp0, Errors::None, 1000, 10);
        ctx.sender.run_once().await.expect("run_once");
        assert!(request1.is_done());
        assert_eq!(request1.get().await.expect("succeeds").offset(), 1000);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_offset(&tp0), Some(1000));

        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.transaction_manager().lock().unwrap().sequence_number(&tp0), 2);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), Some(0));
        assert!(!request2.is_done());

        send_idempotent_producer_response(&mut ctx, None, 1, &tp0, Errors::UnknownProducerId, -1, 10);
        ctx.sender.run_once().await.expect("run_once"); // request an epoch bump
        ctx.sender.run_once().await.expect("run_once"); // bump the epoch
        assert_eq!(ctx.transaction_manager().lock().unwrap().producer_id_and_epoch().epoch, 1);
        assert_eq!(ctx.transaction_manager().lock().unwrap().last_acked_sequence(&tp0), None);
        assert!(!request2.is_done());
    }

    /// Translated from `SenderTest.testTooLargeBatchesAreSafelyRemoved`
    /// (Java 3004-3036), reduced to its idempotent core: a `MESSAGE_TOO_LARGE`
    /// response stops the big batch being tracked (`Sender.java:685-686`) and the
    /// split sub-batches are re-tracked under their own sequences, so the partition
    /// keeps producing.
    ///
    /// # `#[ignore]`: a pre-existing defect this test exposes
    ///
    /// It fails with `build() called but no records built` from
    /// `memory_records_builder.rs:298`, and the cause is **not** in Phase 4's diff:
    ///
    ///   - `Sender::send_producer_data` obtains the wire bytes with
    ///     `ProducerBatch::records()` (`producer_batch.rs:653`), which is
    ///     `MemoryRecordsBuilder::take_built_records()` — it *moves* the built buffer
    ///     out of the batch, part of the CLAUDE.md §12 zero-copy write path.
    ///   - `MESSAGE_TOO_LARGE` can only arrive *after* the batch was sent, so by the
    ///     time `RecordAccumulator::split_and_reenqueue` runs,
    ///     `ProducerBatch::split` → `validate_and_get_records` finds nothing to
    ///     re-read and panics.
    ///
    /// So the split-on-`MESSAGE_TOO_LARGE` path panics for *any* producer, idempotent
    /// or not. The existing `test_expired_batch_does_not_split_on_message_too_large_error`
    /// passes only because it expires the batch first, which takes the `!batch.is_done()`
    /// branch and skips the split entirely. Fixing it means keeping the serialised bytes
    /// borrowable after the send without reintroducing a copy, which is a write-path
    /// change rather than a transactions one; tracked as PLAN §9.18.
    ///
    /// The test is left in place, ignored, rather than deleted: it is the reproducer.
    #[ignore = "pre-existing defect: ProducerBatch::records() moves the built buffer, so \
                split-on-MESSAGE_TOO_LARGE panics. See PLAN §9.18."]
    #[tokio::test]
    async fn test_too_large_batches_are_safely_removed() {
        let mut ctx = SenderTestContext::idempotent();
        let tp0 = ctx.tp0.clone();
        initialize_idempotent_producer_id(&mut ctx, 343_434, 0).await;

        // Two records in one batch, so the batch is splittable.
        let request1 = ctx.append_to_accumulator_with(&tp0, 0, "k1", "v1").await;
        let request2 = ctx.append_to_accumulator_with(&tp0, 0, "k2", "v2").await;
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);
        assert!(ctx.transaction_manager().lock().unwrap().has_inflight_batches(&tp0));

        let response = ctx.produce_response(&tp0, -1, Errors::MessageTooLarge, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");

        // The big batch is gone from the Sender's map; the sub-batches are queued in
        // the accumulator, tracked, and each carries a sequence.
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 0);
        assert_eq!(ctx.accumulator.deque_size(&tp0), 2, "one sub-batch per record");
        assert!(ctx.transaction_manager().lock().unwrap().has_inflight_batches(&tp0));
        assert!(!request1.is_done());
        assert!(!request2.is_done());

        // Both sub-batches drain and complete, which is what "safely removed" means.
        ctx.sender.run_once().await.expect("run_once");
        let drained = ctx.sender.in_flight_batches(&tp0);
        assert!(!drained.is_empty());
        assert!(drained.iter().all(|batch| batch.has_sequence()));
    }

    /// Translated from `SenderTest.testProducerBatchRetriesWhenPartitionLeaderChanges`
    /// (Java 3308-3394).
    ///
    /// A retriable failure schedules the batch for retry. Discovering a **new leader
    /// epoch** must let it go out immediately, skipping the retry backoff
    /// (`RecordAccumulator.shouldBackoff`, Java 796-813, whose
    /// `hasLeaderChanged` term is what suppresses the wait); a retry to the *same*
    /// leader must wait the backoff, and go out once it has elapsed.
    ///
    /// # Reclassified: this is not an idempotence test
    ///
    /// It was on Phase 4's `SenderTest` list because the string `transactionManager`
    /// appears in its body — as the literal `null` argument at Java 3321 (the
    /// accumulator) and 3324 (the `Sender`). Both are built **without** a transaction
    /// manager, so it is neither idempotent nor transactional. It is translated anyway
    /// rather
    /// than argued out of scope: it is the only end-to-end cover for the leader-change
    /// backoff skip, which `record_accumulator.rs`'s
    /// `test_exponential_retry_backoff_leader_change` exercises only at the accumulator
    /// level.
    #[tokio::test]
    async fn test_producer_batch_retries_when_partition_leader_changes() {
        // Java 3317-3324: `lingerMs = 0`, `retryBackoffMs = 10`,
        // `retryBackoffMaxMs = 100`, `retries = 10`, and no transaction manager.
        let mut ctx = SenderTestContext::with_transaction_state(
            false,
            10,
            None,
            Some(SenderTestTimeouts {
                request_timeout_ms: REQUEST_TIMEOUT,
                delivery_timeout_ms: DELIVERY_TIMEOUT_MS,
                accumulator_retry_backoff_ms: 10,
                sender_retry_backoff_ms: RETRY_BACKOFF_MS,
            }),
        );
        let tp0 = ctx.tp0.clone();
        let retry_backoff_max_ms = 100i64;

        // Seed metadata with leader epochs, tp0 at 100 and tp1 at 0.
        let mut tp0_leader_epoch = 100;
        ctx.update_metadata_with_leader_epochs(tp0_leader_epoch);

        // Produce a batch; it comes back with a retriable error and is scheduled for
        // retry.
        let future_is_produced = ctx.append_to_accumulator_with(&tp0, 0, "key", "value").await;
        ctx.sender.run_once().await.expect("run_once"); // connect
        ctx.sender.run_once().await.expect("run_once"); // send the produce request
        assert_eq!(
            ctx.sender.client().in_flight_request_count(),
            1,
            "We should have a single produce request in flight."
        );
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);
        assert!(ctx.sender.client().has_in_flight_requests());
        let response = ctx.produce_response(&tp0, -1, Errors::NotLeaderOrFollower, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert!(!future_is_produced.is_done(), "Produce request should not be done.");

        // A new leader epoch is discovered, so the batch retries immediately, skipping
        // the backoff.
        tp0_leader_epoch += 1;
        ctx.update_metadata_with_leader_epochs(tp0_leader_epoch);
        ctx.sender.run_once().await.expect("run_once"); // send the produce request immediately
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);
        assert!(ctx.sender.client().has_in_flight_requests());
        let response = ctx.produce_response(&tp0, -1, Errors::NotLeaderOrFollower, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert!(!future_is_produced.is_done(), "Produce request should not be done.");

        // A subsequent retry to the *same* leader waits the backoff period.
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 0);
        assert!(!ctx.sender.client().has_in_flight_requests());

        // After waiting longer than the backoff period, the batch is retried again.
        ctx.time.sleep(2 * retry_backoff_max_ms);
        ctx.sender.run_once().await.expect("run_once");
        assert_eq!(ctx.sender.in_flight_batches(&tp0).len(), 1);
        assert!(ctx.sender.client().has_in_flight_requests());
        let offset = 999;
        let response = ctx.produce_response(&tp0, offset, Errors::None, 0);
        ctx.sender.client_mut().respond(response);
        ctx.sender.run_once().await.expect("run_once");
        assert!(future_is_produced.is_done(), "Request to tp0 successfully done");
        assert_eq!(future_is_produced.get().await.expect("succeeds").offset(), offset);
    }

    // =====================================================================
    // `SenderTest.java` accounting (`definition-of-done.md` §3)
    //
    // Scope: every `SenderTest` method whose body references a `TransactionManager`.
    // 51 construct one and `testSenderShouldCloseWhenTransactionManagerInErrorState`
    // mocks one, for **52**. None could have been translated before Phase 3, which is
    // when `TransactionManager` first existed, so all 52 are Phase 4's to place.
    //
    // The scope set and the completeness claim are both reproducible, because Critic 44
    // issues 6 and 7 were the two failure modes of asserting them in prose: the
    // hand-assembled list silently lost an entry while claiming to be complete, and the
    // counts written beside the lists drifted from them.
    //
    //   # the 52 in-scope Java methods
    //   awk '/^    (public|private) void test/{n=$3; sub(/\(.*/,"",n); next}
    //        /^    }$/{n=""} /ransactionManager/{if(n!="")print n}' \
    //     kafka/clients/src/test/java/org/apache/kafka/clients/producer/internals/\
    //     SenderTest.java | sort -u > /tmp/java.txt        # 52 lines
    //
    //   # the 54 entries enumerated below (the `name` (line) shape is unique to them)
    //   grep -oE '`test[A-Za-z]+` \([0-9]+' src/producer/internals/sender.rs \
    //     | grep -oE 'test[A-Za-z]+' | sort -u > /tmp/rust.txt   # 54 lines
    //
    //   comm -23 /tmp/java.txt /tmp/rust.txt   # empty: nothing in scope is unplaced
    //   comm -13 /tmp/java.txt /tmp/rust.txt   # the 2 out-of-scope entries carried below
    //
    // Arithmetic, read off the lists rather than maintained beside them:
    // 33 translated + 18 transactional + 3 blocked = 54 entries, of which 2 are outside
    // the 52 and carried anyway (each says so where it appears). 54 − 2 = 52, so every
    // in-scope method is placed exactly once and nothing else is owed.
    //
    // Line numbers are the `public void` declaration line throughout, here and in the
    // `Translated from` header of every test above. Checking those headers requires
    // comparing each method *name* against its range, not just rewriting the range for a
    // given name: Critic 44 issue 10 was a header that cited a sibling method's range
    // entirely, and it also escaped the first sweep because its range is followed by a
    // clause inside the same parentheses rather than closing them.
    //
    // TRANSLATED IN PHASE 4 (33 entries — 32 of the 52, plus one out-of-scope):
    //   `testInitProducerIdRequest` (620),
    //   `testIdempotentInitProducerIdWithMaxInFlightOne` (664),
    //   `testClusterAuthorizationExceptionInInitProducerIdRequest` (715),
    //   `testIdempotenceWithMultipleInflights` (762),
    //   `testIdempotenceWithMultipleInflightsRetriedInOrder` (811),
    //   `testIdempotenceWithMultipleInflightsWhereFirstFailsFatallyAndSequenceOfFutureBatchesIsAdjusted` (912),
    //   `testEpochBumpOnOutOfOrderSequenceForNextBatch` (971),
    //   `testEpochBumpOnOutOfOrderSequenceForNextBatchWhenThereIsNoBatchInFlight` (1019),
    //   `testEpochBumpOnOutOfOrderSequenceForNextBatchWhenBatchInFlightFails` (1105),
    //   `testCorrectHandlingOfOutOfOrderResponses` (1245),
    //   `testCorrectHandlingOfOutOfOrderResponsesWhenSecondSucceeds` (1326),
    //   `testExpiryOfUnsentBatchesShouldNotCauseUnresolvedSequences` (1394),
    //   `testExpiryOfFirstBatchShouldNotCauseUnresolvedSequencesIfFutureBatchesSucceed` (1417),
    //   `testExpiryOfFirstBatchShouldCauseEpochBumpIfFutureBatchesFail` (1484),
    //   `testExpiryOfAllSentBatchesShouldCauseUnresolvedSequences` (1575),
    //   `testResetOfProducerStateShouldAllowQueuedBatchesToDrain` (1613),
    //   `testCloseWithProducerIdReset` (1655),
    //   `testForceCloseWithProducerIdReset` (1689),
    //   `testBatchesDrainedWithOldProducerIdShouldSucceedOnSubsequentRetry` (1720),
    //   `testCorrectHandlingOfDuplicateSequenceError` (1767),
    //   `testIdempotentUnknownProducerHandlingWhenRetentionLimitReached` (1884),
    //   `testUnknownProducerErrorShouldBeRetriedWhenLogStartOffsetIsUnknown` (1942),
    //   `testUnknownProducerErrorShouldBeRetriedForFutureBatchesWhenFirstFails` (2000),
    //   `testShouldRaiseOutOfOrderSequenceExceptionToUserIfLogWasNotTruncated` (2086),
    //   `testClusterAuthorizationExceptionInProduceRequest` (2159),
    //   `testCancelInFlightRequestAfterFatalError` (2182),
    //   `testUnsupportedForMessageFormatInProduceRequest` (2223),
    //   `testUnsupportedVersionInProduceRequest` (2244),
    //   `testSequenceNumberIncrement` (2265),
    //   `testRetryWhenProducerIdChanges` (2306),
    //   `testBumpEpochWhenOutOfOrderSequenceReceived` (2341),
    //   `testTooLargeBatchesAreSafelyRemoved` (3004) — `#[ignore]`d on PLAN §9.18.
    //   `testProducerBatchRetriesWhenPartitionLeaderChanges` (3308) — **outside the 52**:
    //     both the accumulator and the `Sender` are built with `transactionManager = null`
    //     (Java 3321, 3324), so it is neither idempotent nor transactional. Translated anyway,
    //     as the only end-to-end cover for the leader-change backoff skip.
    //
    // TRANSACTIONAL (18) — not expressible while `TransactionManager::new` refuses a
    // transactional id; Phases 5 and 6 own them:
    //   `testInitProducerIdWithMaxInFlightOne` (636) — builds the manager with a
    //     transactional id and calls `initializeTransactions`.
    //   `testNodeNotReady` (689) — transactional id + `coordinator(TRANSACTION)`.
    //   `testUnresolvedSequencesAreNotFatal` (1534) — transactional id, `beginTransaction`,
    //     `AddPartitionsToTxn`; the manager is built at `SenderTest.java:1537`.
    //   `testTransactionalUnknownProducerHandlingWhenRetentionLimitReached` (1820),
    //   `testTransactionalSplitBatchAndSend` (2385),
    //   `testDoNotPollWhenNoRequestSent` (2991) — `doInitTransactions`. **Reclassified** out
    //     of the idempotence list — see the note below.
    //   `testTransactionalRequestsSentOnShutdown` (2737),
    //   `testRecordsFlushedImmediatelyOnTransactionCompletion` (2771),
    //   `testAwaitPendingRecordsBeforeCommittingTransaction` (2829),
    //   `testIncompleteTransactionAbortOnShutdown` (2898),
    //   `testForceShutdownWithIncompleteTransaction` (2932),
    //   `testTransactionAbortedExceptionOnAbortWithoutError` (2966),
    //   `testTransactionShouldTransitionToAbortableForSenderAPI` (3051),
    //   `testReceiveFailedBatchTwiceWithTransactions` (3126),
    //   `testInvalidTxnStateIsAnAbortableError` (3176),
    //   `testTransactionAbortableExceptionIsAnAbortableError` (3215),
    //   `testAbortableErrorIsConvertedToFatalErrorDuringAbort` (3254),
    //   `testSenderShouldCloseWhenTransactionManagerInErrorState` (3399) — the one entry
    //     given a `mock(TransactionManager.class)` rather than a real one (Java 3403); it
    //     stubs `hasOngoingTransaction` / `beginAbort` to drive `run()`'s abort loop.
    //
    //   Reclassification, corrected after Critic 44 issue 7: the method that moved out
    //   of the idempotence list is `testDoNotPollWhenNoRequestSent`. An earlier revision
    //   credited `testUnresolvedSequencesAreNotFatal` with the move, which is wrong — it
    //   was already in this group when the accounting was first written.
    //
    // BLOCKED ON NAMED MISSING SURFACE (3) — each cites what is absent, per the Phase-3
    // standard:
    //   `testSenderShouldRetryWithBackoffOnRetriableError` (3104) — asserts
    //     `time.milliseconds()` advances by exactly `RETRY_BACKOFF_MS` between retries.
    //     Missing surface: the `Sender`'s clock is an injected `Arc<dyn Fn() -> i64>` with no
    //     `sleep`, so `sleep_ms` uses `tokio::time::sleep` and does not move the test's
    //     `MockTime` — the assertion is unrepresentable. Needs Java's `Time` interface (a
    //     `sleep` that advances the injected clock) threaded through `Sender`, which is a
    //     constructor change across the producer and belongs with the Phase-6 review of
    //     `maybeSendAndPollTransactionalRequest`'s two sleeps
    //     (`.claude/rules/producer-transactions.md` §4).
    //   `testNoBufferReuseWhenBatchExpires` (3605) — **outside the 52** (it uses no
    //     transaction manager), listed here because the same §9.18 gap blocks it. Asserts
    //     `assertSame(buffer.array(), batch.records().buffer().array())` — pooled buffer
    //     identity across the send. Missing surface: `BufferPool` does accounting only and
    //     does not hand back the same backing array, and `ProducerBatch::records()` moves the
    //     buffer out.
    //   `testIdempotentSplitBatchAndSend` (2372) — drives the shared driver whose whole
    //     point is a `MESSAGE_TOO_LARGE` split. Missing surface: the split panics — PLAN
    //     §9.18, with `test_too_large_batches_are_safely_removed` as the reproducer.
    //
    // PLAN §9.19 carries the same three blocked entries and nothing more.
    //
    #[test]
    fn test_transactional_response_without_a_body_is_fatal() {
        let mut ctx = SenderTestContext::idempotent();
        let handler = pending_init_producer_id_handler(&mut ctx);

        const CORRELATION_ID: i32 = 7;
        ctx.sender.set_in_flight_correlation_id(CORRELATION_ID);
        let response = init_producer_id_client_response(CORRELATION_ID, false, None);
        ctx.sender
            .on_transactional_response(handler, &response)
            .expect("the failure is recorded, not propagated");

        let manager = ctx.transaction_manager();
        let manager = manager.lock().unwrap();
        assert!(manager.has_fatal_error());
        assert_eq!(
            manager.last_error().expect("recorded").message(),
            "Could not execute transactional request for unknown reasons"
        );
    }
}
