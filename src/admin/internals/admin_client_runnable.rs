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

//! The single background task that drives admin requests.
//!
//! Translated from `KafkaAdminClient.AdminClientRunnable`. It is generic over a
//! [`KafkaClient`] (mirroring the producer's `Sender<C>`), so unit tests can
//! drive it over `MockClient` while production uses `NetworkClient`. There is
//! exactly one such task per `KafkaAdminClient` instance
//! (`.claude/rules/admin-client.md` §2).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use tokio::sync::mpsc;

use crate::client_response::ClientResponse;
use crate::common::errors::{DisconnectError, TimeoutError};
use crate::common::protocol::Errors;
use crate::common::requests::{ConcreteResponse, MetadataRequestBuilder, RequestBuilder};
use crate::common::utils::{ExponentialBackoff, LogContext};
use crate::common::{Error, Node};
use crate::kafka_client::KafkaClient;
use crate::{kafka_debug, kafka_error, kafka_info, kafka_trace};

use super::admin_metadata_manager::AdminMetadataManager;
use super::call::{Call, HandleResult, MaybeRetryOutcome, NodeProvider};

/// Sentinel for "no hard-shutdown deadline set".
///
/// Plays the role of Java's `KafkaAdminClient.INVALID_SHUTDOWN_TIME`: it is
/// lower than every reachable deadline, so the "is an earlier deadline already
/// installed?" comparison in `KafkaAdminClient::close` orders the same way.
pub(crate) const NO_HARD_SHUTDOWN: i64 = i64::MIN;

/// The base poll timeout cap, mirroring Java's `1_200_000` upper bound.
const MAX_POLL_TIMEOUT_MS: i64 = 1_200_000;

/// Computes the remaining time until `deadline`, clamped to the `i32` range.
///
/// Mirrors `KafkaAdminClient.calcTimeoutMsRemainingAsInt`.
fn calc_timeout_ms_remaining_as_int(now: i64, deadline: i64) -> i32 {
    (deadline - now).clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

/// A node with the calls assigned to it awaiting send.
struct NodeCalls {
    node: Node,
    calls: Vec<Call>,
}

/// A call whose request is in flight, keyed in `correlation_id_to_calls`.
struct InFlightCall {
    node_id_string: String,
    call: Call,
}

/// Shared shutdown signalling between the `KafkaAdminClient` (app side) and the
/// background task.
pub(crate) struct ShutdownSignal {
    /// Set once `close` has begun.
    pub(crate) closing: AtomicBool,
    /// The hard-shutdown deadline in epoch ms, or [`NO_HARD_SHUTDOWN`].
    pub(crate) hard_shutdown_deadline_ms: AtomicI64,
}

impl ShutdownSignal {
    /// Creates a signal in the "not closing" state.
    pub(crate) fn new() -> Self {
        Self {
            closing: AtomicBool::new(false),
            hard_shutdown_deadline_ms: AtomicI64::new(NO_HARD_SHUTDOWN),
        }
    }
}

/// The background task that assigns nodes to calls, sends requests, and routes
/// responses back to per-call hooks.
///
/// Translated from `KafkaAdminClient.AdminClientRunnable`.
pub(crate) struct AdminClientRunnable<C: KafkaClient> {
    client: C,
    metadata_manager: AdminMetadataManager,
    /// App-thread → I/O-task handoff of freshly submitted calls.
    admin_rx: mpsc::UnboundedReceiver<Call>,
    /// Calls not yet assigned to a node.
    pending_calls: Vec<Call>,
    /// Calls assigned to a node, awaiting send, keyed by node id.
    calls_to_send: HashMap<i32, NodeCalls>,
    /// Node ids (as strings) with a call currently in flight (at most one each).
    calls_in_flight: HashSet<String>,
    /// In-flight calls keyed by correlation id.
    correlation_id_to_calls: HashMap<i32, InFlightCall>,
    /// Per-node connection-readiness deadlines, keyed by node id.
    node_ready_deadlines: HashMap<i32, i64>,
    /// Retry backoff.
    retry_backoff: ExponentialBackoff,
    retry_backoff_ms: i64,
    max_retries: i32,
    request_timeout_ms: i32,
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    shutdown: Arc<ShutdownSignal>,
    log_context: LogContext,
}

impl<C: KafkaClient> AdminClientRunnable<C> {
    /// Creates a new runnable.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: C,
        metadata_manager: AdminMetadataManager,
        admin_rx: mpsc::UnboundedReceiver<Call>,
        retry_backoff: ExponentialBackoff,
        retry_backoff_ms: i64,
        max_retries: i32,
        request_timeout_ms: i32,
        time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
        shutdown: Arc<ShutdownSignal>,
        log_context: LogContext,
    ) -> Self {
        Self {
            client,
            metadata_manager,
            admin_rx,
            pending_calls: Vec::new(),
            calls_to_send: HashMap::new(),
            calls_in_flight: HashSet::new(),
            correlation_id_to_calls: HashMap::new(),
            node_ready_deadlines: HashMap::new(),
            retry_backoff,
            retry_backoff_ms,
            max_retries,
            request_timeout_ms,
            time_provider,
            shutdown,
            log_context,
        }
    }

    /// A mutable reference to the underlying client (visible for testing).
    #[cfg(test)]
    pub(crate) fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }

    /// The metadata manager (visible for testing), so a test can force a refresh
    /// and observe the state the internal metadata call leaves it in.
    #[cfg(test)]
    pub(crate) fn metadata_manager(&self) -> &AdminMetadataManager {
        &self.metadata_manager
    }

    /// Whether the loop would terminate now (visible for testing).
    #[cfg(test)]
    pub(crate) fn should_exit_for_test(&self, now: i64) -> bool {
        self.should_exit(now)
    }

    /// Whether any external (user-submitted) call is active (visible for
    /// testing).
    #[cfg(test)]
    pub(crate) fn has_active_external_calls_for_test(&self) -> bool {
        self.has_active_external_calls()
    }

    /// Whether any call at all — internal or external — is active (visible for
    /// testing).
    #[cfg(test)]
    pub(crate) fn has_active_calls_for_test(&self) -> bool {
        !self.pending_calls.is_empty() || !self.calls_to_send.is_empty() || !self.correlation_id_to_calls.is_empty()
    }

    /// The main run loop. Translated from `AdminClientRunnable.run` /
    /// `processRequests`.
    pub(crate) async fn run(&mut self) {
        use futures_util::FutureExt;

        kafka_debug!(self.log_context, "Starting the Kafka admin client I/O task.");

        // Java wraps the loop in `try { processRequests(); } finally { ... }`
        // (`KafkaAdminClient.java:1459-1476`), and it is the `finally` that
        // guarantees every pending call is failed — however `processRequests`
        // terminated. Straight-line code after the loop is NOT that guarantee: a
        // panic anywhere inside `run_once` skipped both `fail_all_remaining` and
        // `client.close()`, leaving every outstanding `KafkaFuture` hanging forever
        // with no error ever delivered and the socket open.
        //
        // `AssertUnwindSafe` is needed because `&mut Self` is not `UnwindSafe`; the
        // only thing done with `self` afterwards is the cleanup Java's `finally`
        // does, which is what must run on this path.
        let outcome = std::panic::AssertUnwindSafe(self.process_requests()).catch_unwind().await;
        if let Err(payload) = outcome {
            kafka_error!(
                self.log_context,
                "Uncaught error in the Kafka admin client I/O task: {:?}",
                payload
            );
        }

        // finally: time out any remaining calls, then close the client.
        let now = (self.time_provider)();
        self.fail_all_remaining(now);
        self.client.close().await;
        kafka_debug!(self.log_context, "Shutdown of the Kafka admin client I/O task has completed.");
    }

    /// The `try` body of Java's `AdminClientRunnable.run` — `processRequests()`
    /// (`KafkaAdminClient.java:1461`). Split out so [`run`](Self::run) can wrap it
    /// and still reach its `finally` after a panic.
    async fn process_requests(&mut self) {
        loop {
            self.run_once().await;
            let now = (self.time_provider)();
            if self.should_exit(now) {
                break;
            }
        }
    }

    /// Whether the loop should terminate. Translated from
    /// `AdminClientRunnable.threadShouldExit`.
    fn should_exit(&self, now: i64) -> bool {
        if !self.shutdown.closing.load(Ordering::Acquire) {
            return false;
        }
        if !self.has_active_external_calls() {
            kafka_trace!(
                self.log_context,
                "All work has been completed, and the I/O task is now exiting."
            );
            return true;
        }
        let deadline = self.shutdown.hard_shutdown_deadline_ms.load(Ordering::Acquire);
        if deadline != NO_HARD_SHUTDOWN && now >= deadline {
            kafka_info!(
                self.log_context,
                "Forcing a hard I/O task shutdown. Requests in progress will be aborted."
            );
            return true;
        }
        false
    }

    /// Whether any **external** (user-submitted) call is still active.
    ///
    /// Translated from `AdminClientRunnable.hasActiveExternalCalls`: internal
    /// calls are deliberately ignored. The metadata refresh
    /// (`make_metadata_call`) is internal and is re-created on every backoff
    /// expiry, so counting it would keep the loop alive forever whenever the
    /// bootstrap brokers are unreachable — making `close()` block until the hard
    /// shutdown deadline, which for Java's no-argument `Admin.close()`
    /// (`Duration::from_millis(i64::MAX)`, clamped to a year like Java's) means
    /// for all practical purposes never.
    fn has_active_external_calls(&self) -> bool {
        self.pending_calls.iter().any(|call| !call.internal)
            || self
                .calls_to_send
                .values()
                .any(|node_calls| node_calls.calls.iter().any(|call| !call.internal))
            || self.correlation_id_to_calls.values().any(|in_flight| !in_flight.call.internal)
    }

    /// A single iteration of the request-processing loop.
    ///
    /// Translated from `AdminClientRunnable.processRequests` — phase ordering is
    /// the Java contract.
    pub(crate) async fn run_once(&mut self) {
        // 1. Drain freshly submitted calls into pending.
        self.drain_new_calls();

        let now = (self.time_provider)();

        // 2. Time out expired calls; base poll timeout.
        let mut poll_timeout = MAX_POLL_TIMEOUT_MS.min(self.handle_timeouts(now).await);

        // Once `close()` has been called, bound the poll by the time remaining
        // to the hard-shutdown deadline, so the loop is guaranteed to reach
        // `should_exit` no later than that deadline. Without this an in-flight
        // external call keeps `should_exit` false while the poll itself waits on
        // the (far larger) call deadline, and `close(timeout)` overruns by up to
        // `request.timeout.ms`. Mirrors `KafkaAdminClient.java:1500-1502`.
        let hard_shutdown_deadline_ms = self.shutdown.hard_shutdown_deadline_ms.load(Ordering::Acquire);
        if hard_shutdown_deadline_ms != NO_HARD_SHUTDOWN {
            poll_timeout = poll_timeout.min(hard_shutdown_deadline_ms.saturating_sub(now));
        }

        // 3. Assign nodes to pending calls.
        poll_timeout = poll_timeout.min(self.maybe_drain_pending_calls(now));

        // 4. Maybe issue a metadata refresh call.
        let metadata_fetch_delay_ms = self.metadata_manager.metadata_fetch_delay_ms(now);
        if metadata_fetch_delay_ms == 0 {
            self.metadata_manager.transition_to_update_pending(now);
            let metadata_call = self.make_metadata_call(now);
            let mut leftover = Vec::new();
            self.maybe_drain_pending_call(metadata_call, now, &mut leftover);
            self.pending_calls.append(&mut leftover);
        }

        // 5. Send eligible calls.
        poll_timeout = poll_timeout.min(self.send_eligible_calls(now).await);

        if metadata_fetch_delay_ms > 0 {
            poll_timeout = poll_timeout.min(metadata_fetch_delay_ms);
        }
        if !self.pending_calls.is_empty() {
            poll_timeout = poll_timeout.min(self.retry_backoff_ms);
        }

        // 6. Poll the network. The poll must run to completion — it is not
        //    cancel-safe (consumer-threading.md §10); wake it via the selector's
        //    wakeup primitive instead of racing it in a select!.
        let responses = self.client.poll(poll_timeout.max(0), now).await;

        // 7. Unassign calls whose target node's connection failed.
        let failed_nodes: HashSet<i32> = self
            .calls_to_send
            .values()
            .filter(|nc| self.client.connection_failed(&nc.node))
            .map(|nc| nc.node.id())
            .collect();
        if !failed_nodes.is_empty() {
            self.unassign_unsent_calls(|node| failed_nodes.contains(&node.id()));
        }

        // 8. Handle responses.
        let now = (self.time_provider)();
        self.handle_responses(now, responses).await;
    }

    /// Drains freshly submitted calls, clearing any assigned node.
    ///
    /// Translated from `drainNewCalls` / `transitionToPendingAndClearList`.
    fn drain_new_calls(&mut self) {
        while let Ok(mut call) = self.admin_rx.try_recv() {
            call.cur_node = None;
            self.pending_calls.push(call);
        }
    }

    /// Times out expired pending / to-send / in-flight calls and returns the
    /// smallest remaining timeout among the survivors.
    ///
    /// Translated from `TimeoutProcessor` plus `timeoutPendingCalls` /
    /// `timeoutCallsToSend` / `timeoutCallsInFlight`.
    async fn handle_timeouts(&mut self, now: i64) -> i64 {
        let mut next_timeout = i32::MAX;

        // Pending calls.
        let pending = std::mem::take(&mut self.pending_calls);
        let survivors = self.timeout_calls(now, &mut next_timeout, "Timed out waiting for a node assignment.", pending);
        self.pending_calls.extend(survivors);

        // Calls awaiting send.
        let node_ids: Vec<i32> = self.calls_to_send.keys().copied().collect();
        for node_id in node_ids {
            let calls = std::mem::take(&mut self.calls_to_send.get_mut(&node_id).unwrap().calls);
            let survivors = self.timeout_calls(now, &mut next_timeout, "Timed out waiting to send the call.", calls);
            if survivors.is_empty() {
                self.calls_to_send.remove(&node_id);
            } else {
                self.calls_to_send.get_mut(&node_id).unwrap().calls = survivors;
            }
        }

        // In-flight calls: disconnect the node instead of failing directly (the
        // disconnect surfaces as a response that fails the call).
        let mut to_disconnect = Vec::new();
        for in_flight in self.correlation_id_to_calls.values() {
            let remaining = calc_timeout_ms_remaining_as_int(now, in_flight.call.deadline_ms);
            if remaining < 0 {
                to_disconnect.push(in_flight.node_id_string.clone());
            } else {
                next_timeout = next_timeout.min(remaining);
            }
        }
        for node_id_string in to_disconnect {
            self.client.disconnect(&node_id_string).await;
        }

        next_timeout as i64
    }

    /// Fails expired calls in `calls`, returning the survivors and folding their
    /// remaining timeouts into `next_timeout`.
    fn timeout_calls(&mut self, now: i64, next_timeout: &mut i32, msg: &str, calls: Vec<Call>) -> Vec<Call> {
        let mut survivors = Vec::new();
        for call in calls {
            let remaining = calc_timeout_ms_remaining_as_int(now, call.deadline_ms);
            if remaining < 0 {
                let err = Error::timeout(format!("{} Call: {}", msg, call.call_name));
                self.fail_call(call, now, err);
            } else {
                *next_timeout = (*next_timeout).min(remaining);
                survivors.push(call);
            }
        }
        survivors
    }

    /// Assigns nodes to pending calls, returning the smallest backoff delay.
    ///
    /// Translated from `maybeDrainPendingCalls`.
    fn maybe_drain_pending_calls(&mut self, now: i64) -> i64 {
        let mut poll_timeout = i64::MAX;
        let calls = std::mem::take(&mut self.pending_calls);
        let mut still_pending = Vec::new();
        for call in calls {
            if now < call.next_allowed_try_ms {
                poll_timeout = poll_timeout.min(call.next_allowed_try_ms - now);
                still_pending.push(call);
            } else {
                self.maybe_drain_pending_call(call, now, &mut still_pending);
            }
        }
        // Any calls re-queued by fail_call during this loop precede the still-pending set.
        self.pending_calls.append(&mut still_pending);
        poll_timeout
    }

    /// Attempts to assign a node to a single call.
    ///
    /// Translated from `maybeDrainPendingCall`.
    fn maybe_drain_pending_call(&mut self, mut call: Call, now: i64, still_pending: &mut Vec<Call>) {
        match call.node_provider.provide(&self.metadata_manager, &self.client, now) {
            Ok(Some(node)) => {
                kafka_trace!(self.log_context, "Assigned {} to node {}", call.call_name, node);
                call.cur_node = Some(node.clone());
                self.calls_to_send
                    .entry(node.id())
                    .or_insert_with(|| NodeCalls { node: node.clone(), calls: Vec::new() })
                    .calls
                    .push(call);
            },
            Ok(None) => {
                if call.handle_node_unavailable(&self.metadata_manager, now) {
                    // The call took corrective action (e.g. sent its keys back to
                    // the lookup stage); it is dropped here rather than left
                    // pending. Mirrors `maybeDrainPendingCall` returning early
                    // when `handleNodeUnavailable` is true.
                } else {
                    kafka_trace!(self.log_context, "Unable to assign {} to a node.", call.call_name);
                    still_pending.push(call);
                }
            },
            Err(err) => self.fail_call(call, now, err),
        }
    }

    /// Sends at most one eligible call per ready node.
    ///
    /// Translated from `sendEligibleCalls`.
    async fn send_eligible_calls(&mut self, now: i64) -> i64 {
        let mut poll_timeout = i64::MAX;
        let node_ids: Vec<i32> = self.calls_to_send.keys().copied().collect();
        for node_id in node_ids {
            let node = match self.calls_to_send.get(&node_id) {
                Some(nc) if !nc.calls.is_empty() => nc.node.clone(),
                _ => {
                    self.calls_to_send.remove(&node_id);
                    continue;
                },
            };
            let node_id_string = node.id_string().to_string();

            if self.calls_in_flight.contains(&node_id_string) {
                // Still waiting for other calls to finish on this node.
                self.node_ready_deadlines.remove(&node_id);
                continue;
            }

            if !self.client.ready(&node, now).await {
                match self.node_ready_deadlines.get(&node_id).copied() {
                    Some(deadline) => {
                        if now >= deadline {
                            // Node too slow to become ready: revoke + disconnect.
                            let calls = std::mem::take(&mut self.calls_to_send.get_mut(&node_id).unwrap().calls);
                            self.transition_to_pending(calls);
                            self.client.disconnect(&node_id_string).await;
                            self.node_ready_deadlines.remove(&node_id);
                            self.calls_to_send.remove(&node_id);
                            continue;
                        }
                        poll_timeout = poll_timeout.min(deadline - now);
                    },
                    None => {
                        self.node_ready_deadlines.insert(node_id, now + self.request_timeout_ms as i64);
                    },
                }
                poll_timeout = poll_timeout.min(self.client.poll_delay_ms(&node, now));
                continue;
            }

            let remaining_request_time = match self.node_ready_deadlines.remove(&node_id) {
                None => self.request_timeout_ms,
                Some(deadline) => calc_timeout_ms_remaining_as_int(now, deadline),
            };

            // Send exactly one call to this node (Java breaks after the first).
            loop {
                let mut call = match self.calls_to_send.get_mut(&node_id) {
                    Some(nc) if !nc.calls.is_empty() => nc.calls.remove(0),
                    _ => break,
                };
                let timeout_ms = remaining_request_time.min(calc_timeout_ms_remaining_as_int(now, call.deadline_ms));
                let request_builder = match call.create_request(timeout_ms) {
                    Ok(rb) => rb,
                    Err(err) => {
                        // `new KafkaException(String.format("Internal error sending %s
                        // to %s.", call.callName, node), t)`
                        // (`KafkaAdminClient.java:1295-1297`): a bare `KafkaException`
                        // — so `is_kafka_error()` is `true` and `is_api_error()` is
                        // `false` — carrying the original as its cause. Neither the
                        // class nor the cause survived being flattened into an
                        // `LocalIllegalState` with the message text appended.
                        let wrapped = Error::kafka_message_source(
                            format!("Internal error sending {} to {}.", call.call_name, node),
                            err,
                        );
                        self.fail_call(call, now, wrapped);
                        continue;
                    },
                };
                let client_request = self.client.new_client_request_with_timeout(
                    &node_id_string,
                    request_builder,
                    now,
                    true,
                    timeout_ms,
                    None,
                );
                let correlation_id = client_request.correlation_id();
                self.client.send(client_request, now);
                self.calls_in_flight.insert(node_id_string.clone());
                self.correlation_id_to_calls
                    .insert(correlation_id, InFlightCall { node_id_string: node_id_string.clone(), call });
                break;
            }
        }
        poll_timeout
    }

    /// Moves the given calls back to `pending_calls`, clearing their node.
    fn transition_to_pending(&mut self, calls: Vec<Call>) {
        for mut call in calls {
            call.cur_node = None;
            self.pending_calls.push(call);
        }
    }

    /// Unassigns calls whose node satisfies `should_unassign`, moving them back
    /// to `pending_calls`.
    ///
    /// Translated from `unassignUnsentCalls`.
    fn unassign_unsent_calls(&mut self, should_unassign: impl Fn(&Node) -> bool) {
        let node_ids: Vec<i32> = self.calls_to_send.keys().copied().collect();
        for node_id in node_ids {
            let (node, is_empty) = {
                let nc = self.calls_to_send.get(&node_id).unwrap();
                (nc.node.clone(), nc.calls.is_empty())
            };
            if is_empty {
                self.calls_to_send.remove(&node_id);
                continue;
            }
            if should_unassign(&node) {
                self.node_ready_deadlines.remove(&node_id);
                let calls = std::mem::take(&mut self.calls_to_send.get_mut(&node_id).unwrap().calls);
                self.transition_to_pending(calls);
                self.calls_to_send.remove(&node_id);
            }
        }
    }

    /// Routes responses to their calls' hooks.
    ///
    /// Translated from `handleResponses`.
    async fn handle_responses(&mut self, now: i64, mut responses: Vec<ClientResponse>) {
        for mut response in responses.drain(..) {
            let correlation_id = response.request_header().correlation_id();
            let Some(InFlightCall { node_id_string, mut call }) = self.correlation_id_to_calls.remove(&correlation_id)
            else {
                // Unknown correlation id → internal server error; disconnect.
                self.client.disconnect(response.destination()).await;
                continue;
            };
            if response.destination() != node_id_string {
                // Inconsistency between the two maps; skip.
                self.correlation_id_to_calls
                    .insert(correlation_id, InFlightCall { node_id_string, call });
                continue;
            }
            self.calls_in_flight.remove(&node_id_string);

            if let Some(version_mismatch) = response.version_mismatch() {
                let err = Error::unsupported_version(version_mismatch.to_string());
                self.fail_call(call, now, err);
            } else if response.was_disconnected() {
                let auth_error = call.cur_node.as_ref().and_then(|node| self.client.authentication_error(node));
                let err = match auth_error {
                    // Java: `call.fail(now, client.authenticationException(node))`
                    // (`KafkaAdminClient.java:1373-1376`) — the object the channel
                    // raised, whichever `AuthenticationException` subclass that was.
                    // `KafkaClient::authentication_error` now hands that object
                    // over, so it is forwarded unchanged: rebuilding it here as the
                    // base class (or worse, hardcoding `SaslAuthenticationFailed`,
                    // finding 242) reported code 58 for a TLS certificate rejection
                    // on a connection that never performed a SASL exchange.
                    Some(error) => error,
                    // `new DisconnectException(...)`
                    // (`KafkaAdminClient.java:1377-1379`). Not `Errors::NetworkError`:
                    // this is a purely client-side event, and `DisconnectError`'s own
                    // file documents that it therefore carries no protocol code.
                    // Reporting code 13 to the user meant a caller matching
                    // `Error::Disconnect(_)` never fired.
                    None => Error::Disconnect(DisconnectError::new(format!(
                        "Cancelled {} request with correlation id {} due to node {} being disconnected",
                        call.call_name, correlation_id, node_id_string
                    ))),
                };
                self.fail_call(call, now, err);
            } else {
                match response.take_response_body() {
                    Some(body) => {
                        let result = call.handle_response(&body, now);
                        match result {
                            HandleResult::Done => {
                                if call.internal {
                                    // Metadata refreshed → force reassignment against new metadata.
                                    self.unassign_unsent_calls(|_| true);
                                }
                            },
                            HandleResult::NewCall(new_call) => {
                                self.pending_calls.push(*new_call);
                            },
                            HandleResult::Retry(err) => {
                                self.fail_call(call, now, err);
                            },
                        }
                    },
                    None => {
                        let err = Error::local_illegal_state(format!(
                            "Received an empty response body for {} request with correlation id {}",
                            call.call_name, correlation_id
                        ));
                        self.fail_call(call, now, err);
                    },
                }
            }
        }
    }

    /// The central retry decision. Translated verbatim from `Call.fail`.
    fn fail_call(&mut self, mut call: Call, now: i64, error: Error) {
        if let Some(node) = call.cur_node.take() {
            self.node_ready_deadlines.remove(&node.id());
        }
        // If the admin client is closing, we can't retry.
        if self.shutdown.closing.load(Ordering::Acquire) {
            call.handle_failure(&error);
            return;
        }
        // Protocol downgrade + retry does not count against the retry budget
        // (that is why `tries` is not incremented here).
        if error.error() == Errors::UnsupportedVersion && call.handle_unsupported_version() {
            kafka_debug!(
                self.log_context,
                "{} attempting protocol downgrade and then retry.",
                call.call_name
            );
            self.pending_calls.push(call);
            return;
        }
        call.next_allowed_try_ms = now + self.retry_backoff.backoff(call.tries as i64);
        call.tries += 1;

        // If the call has timed out, fail.
        if calc_timeout_ms_remaining_as_int(now, call.deadline_ms) <= 0 {
            self.handle_timeout_failure(call, now, error);
            return;
        }
        // If the exception is not retriable, fail.
        if !error.is_retriable_error() {
            call.handle_failure(&error);
            return;
        }
        // If we are out of retries, fail.
        if call.tries > self.max_retries {
            self.handle_timeout_failure(call, now, error);
            return;
        }
        // Otherwise, retry. `maybeRetry` re-queues into pending calls by
        // default; the driver's override may instead re-issue lookup requests
        // (on a disconnect) and take over, leaving nothing to re-queue.
        match call.maybe_retry(&error, now) {
            MaybeRetryOutcome::Requeue => self.pending_calls.push(call),
            MaybeRetryOutcome::Handled => {},
        }
    }

    /// Wraps a non-timeout cause as a timeout and fails the call terminally.
    ///
    /// Translated from `Call.handleTimeoutFailure`.
    fn handle_timeout_failure(&mut self, mut call: Call, now: i64, cause: Error) {
        let error = if cause.error() == Errors::RequestTimedOut {
            cause
        } else {
            // `new TimeoutException(this + " timed out at " + now + " after " + tries
            // + " attempt(s)", cause)` (`KafkaAdminClient.java:962-963`), where `this`
            // renders through `Call.toString()` (`:1001-1004`).
            //
            // The message used to gain an invented `"Aborted due to timeout: "`
            // prefix (a string that appears nowhere in the Kafka tree), drop the
            // `Call(...)` rendering, and append the cause as text — which left
            // `Error::source()` empty where Java's `getCause()` is populated.
            let message = format!("{} timed out at {} after {} attempt(s)", call, now, call.tries);
            Error::Timeout(TimeoutError::new_source(message, cause))
        };
        call.handle_failure(&error);
    }

    /// Fails all remaining calls (finally-block on shutdown).
    fn fail_all_remaining(&mut self, now: i64) {
        let msg = "The AdminClient thread has exited.";
        // Ensure the closing flag is set so fail_call routes to handle_failure.
        self.shutdown.closing.store(true, Ordering::Release);

        self.drain_new_calls();
        let pending = std::mem::take(&mut self.pending_calls);
        for call in pending {
            let mut call = call;
            call.handle_failure(&Error::timeout(format!("{} Call: {}", msg, call.call_name)));
        }
        let calls_to_send = std::mem::take(&mut self.calls_to_send);
        for (_node_id, nc) in calls_to_send {
            for mut call in nc.calls {
                call.handle_failure(&Error::timeout(format!("{} Call: {}", msg, call.call_name)));
            }
        }
        let in_flight = std::mem::take(&mut self.correlation_id_to_calls);
        for (_cid, mut in_flight) in in_flight {
            in_flight
                .call
                .handle_failure(&Error::timeout(format!("{} Call: {}", msg, in_flight.call.call_name)));
        }
        let _ = now;
    }

    /// Builds the internal broker metadata refresh call.
    ///
    /// Translated from `makeBrokerMetadataCall` (the only variant needed while
    /// `bootstrap.controllers` is unsupported — see the phase self-review).
    fn make_metadata_call(&self, now: i64) -> Call {
        let deadline = now + self.request_timeout_ms as i64;
        let mm_ok = self.metadata_manager.clone();
        let mm_fail = self.metadata_manager.clone();
        Call::new_internal(
            "fetchMetadata",
            deadline,
            NodeProvider::MetadataUpdate,
            Box::new(|_timeout_ms| {
                // Empty topic list: request brokers + controller only, matching Java.
                Ok(
                    Box::new(MetadataRequestBuilder::new_topics_allow_auto_topic_creation(Some(&[]), true))
                        as Box<dyn RequestBuilder>,
                )
            }),
            Box::new(move |response, now, _cur_node| {
                // Java does `(MetadataResponse) abstractResponse` unguarded
                // (`KafkaAdminClient.java:1668`) and relies on the `:1387`
                // `catch (Throwable t)` → `call.fail(now, t)` →
                // `metadataManager.updateFailed(e)` to leave `UPDATE_PENDING`.
                //
                // Swallowing the mismatch ran neither `update` nor `update_failed`,
                // while `run_once` had already called
                // `transition_to_update_pending`. `metadata_fetch_delay_ms` returns
                // `i64::MAX` in that state, so the client never refreshed metadata
                // again for its whole lifetime — and `HandleResult::Done` on an
                // internal call also re-queues the pending calls against the
                // permanently stale metadata.
                let ConcreteResponse::Metadata(metadata_response) = response else {
                    return HandleResult::Retry(Error::local_illegal_state(
                        "Expected a Metadata response for the internal metadata call",
                    ));
                };
                mm_ok.update(metadata_response.build_cluster(), now);
                HandleResult::Done
            }),
            Box::new(move |error| {
                mm_fail.update_failed(error.clone());
            }),
        )
    }
}
