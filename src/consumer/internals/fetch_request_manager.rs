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

//! `FetchRequestManager` — implements the `RequestManager` interface for
//! the fetch loop, composing [`AbstractFetch`] (Phase 7a).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.FetchRequestManager`.
//!
//! # Composition over inheritance
//!
//! Java's class `extends AbstractFetch implements RequestManager`. The
//! Rust port keeps `AbstractFetch` as a concrete `pub(crate) struct`
//! field, mirroring the Phase 7a design decision. This avoids the
//! lifetime/dispatch issues that come with translating Java inheritance
//! to Rust trait objects.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use log::trace;
use tokio::sync::{Notify, mpsc, oneshot};

use crate::common::memory::buffer_supplier::BufferSupplier;
use crate::common::protocol::Errors;
use crate::common::requests::ConcreteResponse;
use crate::common::requests::fetch_response::FetchResponse;
use crate::common::{KafkaError, Node};
use crate::consumer::internals::abstract_fetch::AbstractFetch;
use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
use crate::consumer::internals::fetch_buffer::FetchBuffer;
use crate::consumer::internals::fetch_config::FetchConfig;
use crate::consumer::internals::network_client_delegate::{PollResult, UnsentRequest};
use crate::consumer::internals::request_manager::RequestManager;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::fetch_session_handler::FetchSessionRequestData;

/// Callback that supplies the "is this node currently unreachable due to
/// reconnect backoff?" predicate.
///
/// Phase 7b's [`FetchRequestManager`] receives this from the bg task at
/// `RequestManager::poll` time (the bg task can read
/// `NetworkClientDelegate::is_unavailable`). The callback owns no
/// borrowed state to keep the type bounds simple.
pub(crate) type IsUnavailableFn = Arc<dyn Fn(&Node) -> bool + Send + Sync + 'static>;

/// Callback that returns `Err(...)` when the node has a pending
/// authentication failure (Java's `maybeThrowAuthFailure`).
pub(crate) type MaybeAuthFailureFn = Arc<dyn Fn(&Node) -> Result<(), KafkaError> + Send + Sync + 'static>;

/// Always-available stub for [`IsUnavailableFn`]: every node is
/// reachable. Used by tests and as a default when no delegate is wired.
pub(crate) fn always_available() -> IsUnavailableFn {
    Arc::new(|_| false)
}

/// No-op stub for [`MaybeAuthFailureFn`].
pub(crate) fn no_auth_failure() -> MaybeAuthFailureFn {
    Arc::new(|_| Ok(()))
}

/// Envelope for routing a fetch-request completion (or its
/// transport-level failure) from the spawned response forwarder back
/// to the fetch manager's next `poll(now)` call.
///
/// Mirrors Java's
/// `AbstractFetch.createFetchRequest(...).whenComplete((clientResponse, exception) -> { handleFetchSuccess / handleFetchFailure })`
/// dispatch, but defers the `&mut AbstractFetch` access to the next
/// bg-task `poll(now)` cycle so the cross-task hand-off stays serialized.
///
/// The fields are carried by **ownership** through the channel — in
/// particular the `FetchResponse` carries `Bytes` partition payloads,
/// so moving the enum across the channel does NOT copy the receive-path
/// record bytes (consumer-threading.md §27 zero-copy invariant
/// preserved — the spawned forwarder does NOT decode the response).
///
/// `for_close = true` routes to
/// `AbstractFetch::handle_close_fetch_session_success` /
/// `handle_close_fetch_session_failure` (close-fetch-session) instead of
/// the normal fetch handlers; `poll_on_close` builders set this.
pub(crate) enum PendingFetchCompletion {
    /// Broker returned a `FetchResponse`. The forwarder captures the
    /// response body, the per-node request data, the fetch target, and
    /// the request API version (needed by Java's
    /// `handleFetchSuccess` to dispatch through `responseData(version)`).
    /// The drain applies the success path on `&mut AbstractFetch`
    /// inside the next `poll(now)`.
    Response {
        fetch_target: Node,
        request_data: FetchSessionRequestData,
        response: FetchResponse,
        request_version: i16,
        for_close: bool,
    },
    /// Transport-level failure (network error, in-flight cancellation,
    /// type mismatch on the response body). The drain calls
    /// `AbstractFetch::handle_fetch_failure`.
    Failure {
        fetch_target: Node,
        request_data: FetchSessionRequestData,
        error: KafkaError,
        for_close: bool,
    },
}

/// `FetchRequestManager` — owns an [`AbstractFetch`] and produces fetch
/// `UnsentRequest`s in response to `RequestManager::poll`.
pub(crate) struct FetchRequestManager {
    /// Shared state with the Phase 10 bg task (`AbstractFetch` from 7a).
    abstract_fetch: AbstractFetch,
    /// Pending fetch-request creation acks. Matches Java's single
    /// `CompletableFuture<Void> pendingFetchRequestFuture` semantics:
    /// concurrent callers' acks accumulate in the same `Vec`, and ALL
    /// are completed together on the next `pollInternal` (Java does
    /// this via `whenComplete` chaining; Rust collects them in a single
    /// slot and resolves them in one shot).
    pending_fetch_requests: Option<Vec<oneshot::Sender<Result<(), KafkaError>>>>,
    /// Node-availability callbacks supplied by the consumer bg task. They
    /// are stored as `Arc<dyn Fn>` so the bg task can plug in
    /// [`crate::consumer::internals::network_client_delegate::NetworkClientDelegate`]
    /// indirectly without `FetchRequestManager` holding a direct
    /// reference to the delegate.
    is_unavailable: IsUnavailableFn,
    maybe_throw_auth_failure: MaybeAuthFailureFn,
    /// Cloned into each spawned response forwarder so the forwarder can
    /// route the fetch response back through `poll(now)`'s drain step.
    /// See [`PendingFetchCompletion`] for the rationale.
    pending_completion_tx: mpsc::UnboundedSender<PendingFetchCompletion>,
    /// Drained by [`Self::drain_pending_completions`] at the top of
    /// every `poll(now)` / `poll_on_close(now)` call. Held directly (no
    /// outer `Mutex`) because the fetch manager is single-owner — only
    /// the bg-task `poll(now)` cycle touches the receiver.
    /// (`mpsc::UnboundedReceiver` is `Send` but not `Sync`;
    /// single-ownership keeps it sound.)
    pending_completion_rx: mpsc::UnboundedReceiver<PendingFetchCompletion>,
    /// Cloned into each spawned response forwarder. After a forwarder
    /// enqueues a [`PendingFetchCompletion`], it pokes this `Notify` so the
    /// bg task's network poll returns at a safe boundary and the next
    /// `run_once` drains the completion (→ `FetchBuffer::add`) promptly.
    ///
    /// Without this, a fetch response that arrives on the wire while the bg
    /// task is parked in its network poll is not handled until the poll's
    /// `maximumTimeToWait` elapses (up to `MAX_POLL_TIMEOUT_MS`). When the
    /// consumer has caught up — so there is no backlog keeping `run_once`
    /// cycling — that adds up to ~`fetch.max.wait.ms` of latency per record
    /// even though the data was already on the socket. This is the same wake
    /// the application-event enqueue path uses (it is a clone of the bg
    /// task's `event_notify`); a default no-listener `Notify` is used until
    /// the production path installs the real one via
    /// [`Self::set_completion_notify`].
    completion_notify: Arc<Notify>,
}

impl FetchRequestManager {
    /// Constructs a `FetchRequestManager` from explicit dependencies.
    ///
    /// Translates the 9-arg Java constructor. Drops the `LogContext` /
    /// `FetchMetricsManager` / `ApiVersions` parameters (we don't use
    /// them on this path yet — see Phase 7a plan).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        metadata: Arc<ConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        fetch_config: FetchConfig,
        fetch_buffer: Arc<FetchBuffer>,
        decompression_buffer_supplier: Arc<BufferSupplier>,
        is_unavailable: IsUnavailableFn,
        maybe_throw_auth_failure: MaybeAuthFailureFn,
    ) -> Self {
        let (pending_completion_tx, pending_completion_rx) = mpsc::unbounded_channel();
        Self {
            abstract_fetch: AbstractFetch::new(
                metadata,
                subscriptions,
                fetch_config,
                fetch_buffer,
                decompression_buffer_supplier,
            ),
            pending_fetch_requests: None,
            is_unavailable,
            maybe_throw_auth_failure,
            pending_completion_tx,
            pending_completion_rx,
            completion_notify: Arc::new(Notify::new()),
        }
    }

    /// Installs the bg task's wakeup `Notify` (a clone of `event_notify`) so
    /// response forwarders can wake the network poll when a fetch completion
    /// is ready. See [`Self::completion_notify`]. Called by the production
    /// consumer wiring after construction; tests that don't drive a real bg
    /// task can leave the default no-listener `Notify` in place.
    pub(crate) fn set_completion_notify(&mut self, notify: Arc<Notify>) {
        self.completion_notify = notify;
    }

    /// Signals that the consumer wants requests to be created for the
    /// broker nodes to fetch the next batch of records.
    ///
    /// Translates Java's
    /// `CompletableFuture<Void> createFetchRequests()`.
    ///
    /// Java chains a single `pendingFetchRequestFuture` via `whenComplete`
    /// so concurrent callers all complete on ONE `pollInternal`. The
    /// Rust port collects all acks in a single slot; the next `poll`
    /// completes them together.
    pub(crate) fn create_fetch_requests(&mut self) -> oneshot::Receiver<Result<(), KafkaError>> {
        let (tx, rx) = oneshot::channel();
        self.pending_fetch_requests.get_or_insert_with(Vec::new).push(tx);
        rx
    }

    /// Enqueue an ack supplied by the Phase 5 `CreateFetchRequestsEvent`.
    ///
    /// This is the Phase 10 wiring entry point — the bg task receives
    /// the event and immediately calls `enqueue_create_fetch_requests`
    /// to enqueue the ack. The next `poll(current_time_ms)` completes
    /// all accumulated acks together (Java's single-slot
    /// `pendingFetchRequestFuture` semantics).
    pub(crate) fn enqueue_create_fetch_requests(&mut self, ack: oneshot::Sender<Result<(), KafkaError>>) {
        self.pending_fetch_requests.get_or_insert_with(Vec::new).push(ack);
    }

    /// Borrowed access to the underlying `AbstractFetch`. Used by the bg
    /// task's `whenComplete` wiring (Phase 10).
    pub(crate) fn abstract_fetch(&self) -> &AbstractFetch {
        &self.abstract_fetch
    }

    /// Mutable access to the underlying `AbstractFetch`. Used by the bg
    /// task to feed `handle_fetch_success` / `handle_fetch_failure`
    /// (Phase 10 wiring).
    pub(crate) fn abstract_fetch_mut(&mut self) -> &mut AbstractFetch {
        &mut self.abstract_fetch
    }

    /// Internal helper: build a `PollResult` by running
    /// `prepare_fetch_requests` and wrapping each result in an
    /// `UnsentRequest`. Mirrors Java's `pollInternal`.
    ///
    /// `for_close = true` switches `prepare_fetch_requests` for
    /// `prepare_close_fetch_session_requests` (`poll_on_close` path).
    fn poll_internal(&mut self, current_time_ms: i64, for_close: bool) -> PollResult {
        // Java's `pendingFetchRequestFuture` semantics: take the whole
        // slot out atomically; all callers' acks resolve together with
        // a single result.
        let Some(pending_acks) = self.pending_fetch_requests.take() else {
            // No explicit request for creating fetch requests was issued
            // — short-circuit.
            return PollResult::empty();
        };

        let prepared = if for_close {
            // Java's `pollOnClose` builds requests against the resolved
            // node map (every reachable session-holding node).
            let cluster = self.abstract_fetch.metadata.metadata_arc().fetch();
            let mut nodes_by_id = std::collections::HashMap::new();
            // Include every node we currently hold a session for, but
            // only if it's reachable (matches Java's null/unavailable
            // skip).
            for node_id in self.abstract_fetch.session_handler_ids() {
                if let Some(node) = cluster.node_by_id(node_id)
                    && !(self.is_unavailable)(node)
                {
                    nodes_by_id.insert(node_id, node.clone());
                }
            }
            self.abstract_fetch
                .prepare_close_fetch_session_requests(&nodes_by_id)
                .into_iter()
                .filter_map(|(node_id, data)| nodes_by_id.remove(&node_id).map(|n| (node_id, (n, data))))
                .collect()
        } else {
            let is_unavailable = self.is_unavailable.clone();
            let maybe_throw_auth_failure = self.maybe_throw_auth_failure.clone();
            match self.abstract_fetch.prepare_fetch_requests(
                current_time_ms,
                move |n| is_unavailable(n),
                move |n| maybe_throw_auth_failure(n),
            ) {
                Ok(map) => map,
                Err(e) => {
                    // Java: completes ALL chained pendingFetchRequestFuture
                    // callers exceptionally and returns a "dummy" empty
                    // PollResult to avoid interrupting other request
                    // managers.
                    for tx in pending_acks {
                        // Cheap KafkaError clone via String reformat.
                        let cloned = KafkaError::illegal_state(e.message().to_string());
                        let _ = tx.send(Err(cloned));
                    }
                    return PollResult::empty();
                },
            }
        };

        if prepared.is_empty() {
            // Complete ALL pending acks with Ok(()) and return empty.
            //
            // Only wake a consumer blocked in `FetchBuffer::await_wakeup` if
            // there is genuinely nothing coming. When nodes already have an
            // in-flight fetch, `prepare_fetch_requests` skipped them — a
            // response is on the way and will wake the buffer via `add`, so
            // waking here is wrong: the app's `await_wakeup` would return,
            // re-trigger `createFetchRequests` (empty again — same in-flight
            // skip), and wake again, busy-looping. Wait for the in-flight
            // fetch instead. Only an empty pending-set means no fetch is
            // outstanding (no fetchable partitions / all paused), in which
            // case waking avoids a needless wait.
            //
            // (Java wakes unconditionally here, but its `poll()` does not
            // re-issue `createFetchRequests` on every loop iteration the way
            // the Rust poll loop does, so Java does not spin. The Rust poll
            // loop ensures a fetch is in flight before blocking, which makes
            // the unconditional wake a spin — hence this guard.)
            if self.abstract_fetch.nodes_with_pending_fetch_requests.is_empty() {
                self.abstract_fetch.fetch_buffer.wakeup();
            }
            for tx in pending_acks {
                let _ = tx.send(Ok(()));
            }
            return PollResult::empty();
        }

        // Build the per-node UnsentRequest list and ack ALL pending
        // create-fetch-requests callers together (Java's
        // `pendingFetchRequestFuture` single-slot semantics).
        //
        // For each per-node `UnsentRequest`, take the response
        // receiver out and spawn a forwarder that converts the
        // resolved `ClientResponse` (or transport error) into a
        // `PendingFetchCompletion` envelope and routes it back through
        // the mpsc channel. The next `poll(now)` / `poll_on_close(now)`
        // call's `drain_pending_completions` then dispatches into
        // `AbstractFetch::handle_fetch_*` on `&mut self`, mirroring
        // Java's `whenComplete((response, exception) -> { ... })`
        // lambda on `AbstractFetch.createFetchRequest`. CLAUDE.md §11
        // hot-path note: one spawn per FetchRequest (per-broker batch),
        // NOT per-record — the forwarder lives outside any per-record
        // loop, and the response body bytes (a `Bytes` buffer inside
        // `FetchResponse`) travel by **ownership** through the channel,
        // never copied (consumer-threading.md §27).
        let mut requests: Vec<UnsentRequest> = Vec::with_capacity(prepared.len());
        for (_node_id, (target_node, request_data)) in prepared {
            let builder = self.abstract_fetch.create_fetch_request(&target_node, &request_data);
            let mut unsent = UnsentRequest::new(Box::new(builder), Some(target_node.clone()));

            let response_rx = unsent.take_response_receiver().expect("receiver fresh");
            let tx = self.pending_completion_tx.clone();
            let completion_notify = Arc::clone(&self.completion_notify);
            let request_data_for_forwarder = request_data.clone();
            let fetch_target_for_forwarder = target_node.clone();
            let for_close_flag = for_close;
            tokio::spawn(async move {
                let completion = match response_rx.await {
                    Ok(Ok(mut client_response)) => {
                        let request_version = client_response.request_header().api_version();
                        match client_response.take_response_body() {
                            Some(ConcreteResponse::Fetch(resp)) => PendingFetchCompletion::Response {
                                fetch_target: fetch_target_for_forwarder,
                                request_data: request_data_for_forwarder,
                                response: resp,
                                request_version,
                                for_close: for_close_flag,
                            },
                            _ => PendingFetchCompletion::Failure {
                                fetch_target: fetch_target_for_forwarder,
                                request_data: request_data_for_forwarder,
                                error: KafkaError::new(Errors::UnknownServerError),
                                for_close: for_close_flag,
                            },
                        }
                    },
                    Ok(Err(err)) => PendingFetchCompletion::Failure {
                        fetch_target: fetch_target_for_forwarder,
                        request_data: request_data_for_forwarder,
                        error: err,
                        for_close: for_close_flag,
                    },
                    Err(_recv) => PendingFetchCompletion::Failure {
                        fetch_target: fetch_target_for_forwarder,
                        request_data: request_data_for_forwarder,
                        error: KafkaError::new(Errors::NetworkException),
                        for_close: for_close_flag,
                    },
                };
                // Receiver lives as long as the fetch manager; ignore
                // the send error in case the manager has been dropped
                // during a shutdown race.
                let _ = tx.send(completion);
                // Wake the bg task so the next `run_once` drains this
                // completion (→ `FetchBuffer::add`) promptly, instead of the
                // response sitting in the channel until the network poll's
                // `maximumTimeToWait` elapses. See `completion_notify`.
                completion_notify.notify_one();
            });
            requests.push(unsent);
        }

        for tx in pending_acks {
            let _ = tx.send(Ok(()));
        }
        trace!("FetchRequestManager: produced {} fetch requests", requests.len());
        PollResult::with_requests(requests)
    }

    /// Drains the [`PendingFetchCompletion`] mpsc channel into
    /// `AbstractFetch::handle_fetch_*` / `handle_close_fetch_session_*`.
    /// Called at the top of [`RequestManager::poll`] and
    /// [`RequestManager::poll_on_close`].
    ///
    /// Java reference: the `whenComplete` lambda body on
    /// `AbstractFetch.createFetchRequest` — `handleFetchSuccess(...)` /
    /// `handleFetchFailure(...)` (or `handleCloseFetchSessionSuccess` /
    /// `handleCloseFetchSessionFailure` for the close path). The Java
    /// code runs the lambda on the network-IO thread; Rust runs the
    /// equivalent work here on the bg-task to keep all
    /// `&mut AbstractFetch` access serialized through `poll(now)` and
    /// to preserve §27 zero-copy (the response body bytes were moved
    /// by ownership through the channel — `handle_fetch_success`
    /// iterates the borrowed `&FetchResponse` and builds
    /// `CompletedFetch` entries that hold their own `Arc<Bytes>`
    /// slices without copying).
    ///
    /// **§16 audit**: between draining the channel and calling
    /// `handle_fetch_success/_failure`, the only `Mutex` acquired is
    /// `self.abstract_fetch.subscriptions` inside `handle_fetch_failure`
    /// (which `AbstractFetch` already holds for the duration of that
    /// call). No `MutexGuard` is held across an `.await` here — the
    /// `try_recv` loop is synchronous and the cross-call dispatch is
    /// synchronous too.
    fn drain_pending_completions(&mut self) {
        while let Ok(completion) = self.pending_completion_rx.try_recv() {
            match completion {
                PendingFetchCompletion::Response {
                    fetch_target,
                    request_data,
                    response,
                    request_version,
                    for_close,
                } => {
                    if for_close {
                        self.abstract_fetch
                            .handle_close_fetch_session_success(&fetch_target, &request_data);
                    } else {
                        self.abstract_fetch.handle_fetch_success(
                            &fetch_target,
                            &request_data,
                            &response,
                            request_version,
                        );
                    }
                },
                PendingFetchCompletion::Failure { fetch_target, request_data, error, for_close } => {
                    if for_close {
                        self.abstract_fetch
                            .handle_close_fetch_session_failure(&fetch_target, &request_data, &error);
                    } else {
                        self.abstract_fetch.handle_fetch_failure(&fetch_target, &request_data, &error);
                    }
                },
            }
        }
    }
}

impl RequestManager for FetchRequestManager {
    /// Translates Java's
    /// `PollResult poll(long currentTimeMs)` — produces the fetch
    /// requests for the next round, if any pending
    /// `CreateFetchRequestsEvent` ack is outstanding.
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        // 0. Drain any pending fetch completions from prior spawned
        // forwarders. State-update happens before request-building so
        // the next `prepare_fetch_requests` observes the
        // post-completion `nodes_with_pending_fetch_requests` set.
        self.drain_pending_completions();
        self.poll_internal(current_time_ms, false)
    }

    /// Translates Java's
    /// `PollResult pollOnClose(long currentTimeMs)` — produces the
    /// close-fetch-session requests.
    fn poll_on_close(&mut self, current_time_ms: i64) -> PollResult {
        // Drain any pending completions one last time so close paths
        // observe the post-completion state.
        self.drain_pending_completions();
        // Java's pollOnClose unconditionally enqueues a fresh ack so
        // pollInternal has something to satisfy.
        let (tx, _rx) = oneshot::channel();
        self.pending_fetch_requests.get_or_insert_with(Vec::new).push(tx);
        self.poll_internal(current_time_ms, true)
    }

    fn signal_close(&mut self) {
        // Java's `signalClose` is a no-op for the fetch manager
        // (close-mode wiring is gated on `pollOnClose`).
    }
}

impl Drop for FetchRequestManager {
    fn drop(&mut self) {
        // Fail any outstanding pending acks so callers don't hang on a
        // dropped receiver.
        if let Some(pending) = self.pending_fetch_requests.take() {
            for tx in pending {
                let _ = tx.send(Err(KafkaError::illegal_state(
                    "FetchRequestManager dropped with pending CreateFetchRequests ack",
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::IsolationLevel;
    use crate::common::TopicPartition;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    fn make_subscriptions() -> Arc<Mutex<SubscriptionState>> {
        Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)))
    }

    fn make_consumer_metadata(subs: Arc<Mutex<SubscriptionState>>) -> Arc<ConsumerMetadata> {
        Arc::new(ConsumerMetadata::new(
            50,
            50,
            50_000,
            false,
            false,
            subs,
            ClusterResourceListeners::new(),
        ))
    }

    fn make_fetch_config() -> FetchConfig {
        FetchConfig::new(
            1,
            50 * 1024 * 1024,
            500,
            1024 * 1024,
            500,
            true,
            "",
            IsolationLevel::ReadUncommitted,
        )
    }

    fn make_manager() -> FetchRequestManager {
        let subs = make_subscriptions();
        let metadata = make_consumer_metadata(subs.clone());
        FetchRequestManager::new(
            metadata,
            subs,
            make_fetch_config(),
            Arc::new(FetchBuffer::new()),
            Arc::new(crate::common::memory::buffer_supplier::BufferSupplier::create()),
            always_available(),
            no_auth_failure(),
        )
    }

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    /// `poll` returns an empty result when no pending
    /// `CreateFetchRequestsEvent` ack is outstanding (Java's
    /// `pendingFetchRequestFuture == null` short-circuit).
    #[test]
    fn test_poll_no_pending_returns_empty() {
        let mut mgr = make_manager();
        let result = mgr.poll(100);
        assert!(result.unsent_requests.is_empty());
        assert_eq!(PollResult::WAIT_FOREVER, result.time_until_next_poll_ms);
    }

    /// `poll` returns an empty result when there are no fetchable
    /// partitions (Java completes the pending future with `null` and
    /// returns `PollResult.EMPTY`).
    #[tokio::test]
    async fn test_poll_empty_partitions_completes_ack() {
        let mut mgr = make_manager();
        let rx = mgr.create_fetch_requests();
        let result = mgr.poll(100);
        assert!(result.unsent_requests.is_empty());
        // The ack should have been completed Ok(()).
        let received = rx.await.expect("ack receiver");
        assert!(received.is_ok());
    }

    /// Regression (latency): when `prepare_fetch_requests` returns empty
    /// because nodes already have an in-flight fetch, `poll` must NOT wake
    /// the `FetchBuffer` — a response is on the way and will wake it via
    /// `add`. Waking here busy-loops a consumer blocked in
    /// `FetchBuffer::await_wakeup` (it returns, re-triggers
    /// `createFetchRequests`, which is empty again via the same in-flight
    /// skip, wakes again...). See `design/current/consumer-latency-findings.md`.
    #[tokio::test]
    async fn test_poll_empty_with_inflight_does_not_wake_buffer() {
        let mut mgr = make_manager();
        let buffer = mgr.abstract_fetch.fetch_buffer.clone();
        // Simulate an in-flight fetch to node 1.
        mgr.abstract_fetch.nodes_with_pending_fetch_requests.insert(1);
        let rx = mgr.create_fetch_requests();
        let _ = mgr.poll(100);
        assert!(rx.await.expect("ack receiver").is_ok());
        // The buffer must NOT have been woken: `await_wakeup` blocks for
        // ~the full timeout rather than returning immediately.
        let start = Instant::now();
        buffer.await_wakeup(Duration::from_millis(120)).await;
        assert!(
            start.elapsed() >= Duration::from_millis(80),
            "await_wakeup returned early ({:?}) — buffer was woken despite an in-flight fetch",
            start.elapsed()
        );
    }

    /// Counterpart: with NO in-flight fetch and nothing fetchable, `poll`
    /// DOES wake the buffer so a blocked consumer does not wait needlessly
    /// (Java's unconditional wake for the genuinely-nothing-to-fetch case).
    #[tokio::test]
    async fn test_poll_empty_no_inflight_wakes_buffer() {
        let mut mgr = make_manager();
        let buffer = mgr.abstract_fetch.fetch_buffer.clone();
        let rx = mgr.create_fetch_requests();
        let _ = mgr.poll(100);
        assert!(rx.await.expect("ack receiver").is_ok());
        // The buffer was woken → `await_wakeup` returns promptly.
        let start = Instant::now();
        buffer.await_wakeup(Duration::from_millis(500)).await;
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "await_wakeup blocked ({:?}) — buffer was not woken when nothing is in flight",
            start.elapsed()
        );
    }

    /// `create_fetch_requests` and `enqueue_create_fetch_requests` both
    /// accumulate in the same pending slot; ONE poll satisfies ALL
    /// pending acks together (Java's `pendingFetchRequestFuture` chain).
    #[tokio::test]
    async fn test_create_fetch_requests_completes_all_pending_together() {
        let mut mgr = make_manager();
        let rx1 = mgr.create_fetch_requests();
        let (tx2, rx2) = oneshot::channel();
        mgr.enqueue_create_fetch_requests(tx2);

        // ONE poll satisfies BOTH (Java's single-slot semantics).
        let _ = mgr.poll(0);
        assert!(rx1.await.unwrap().is_ok());
        assert!(rx2.await.unwrap().is_ok());
    }

    /// `poll_on_close` enqueues an internal ack and produces an empty
    /// result when there are no resolved nodes / sessions.
    #[test]
    fn test_poll_on_close_with_no_sessions() {
        let mut mgr = make_manager();
        let result = mgr.poll_on_close(100);
        assert!(result.unsent_requests.is_empty());
    }

    /// `Drop` fails any outstanding pending acks instead of leaving them
    /// hung.
    #[tokio::test]
    async fn test_drop_completes_pending_acks_with_error() {
        let rx = {
            let mut mgr = make_manager();
            mgr.create_fetch_requests()
        };
        // After mgr is dropped, the receiver should resolve with an Err.
        let result = rx.await.unwrap();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("FetchRequestManager dropped"));
    }

    /// `signal_close` is a no-op (Java parity).
    #[test]
    fn test_signal_close_is_noop() {
        let mut mgr = make_manager();
        mgr.signal_close();
        // No state to assert — the contract is "doesn't panic".
    }

    /// `abstract_fetch` accessors return the underlying composed
    /// `AbstractFetch` for the Phase 10 bg task to feed responses into.
    #[test]
    fn test_abstract_fetch_accessor() {
        let mgr = make_manager();
        assert!(!mgr.abstract_fetch().has_completed_fetches());
    }

    /// `prepare_fetch_requests` produces a no-op when no partitions are
    /// fetchable.
    #[test]
    fn test_poll_no_fetchable_partitions() {
        let mut mgr = make_manager();
        let _rx = mgr.create_fetch_requests();
        let result = mgr.poll(100);
        assert!(result.unsent_requests.is_empty());
    }

    /// Translation of the FIFO + ack contract for the Phase 5
    /// `CreateFetchRequestsEvent` integration. Mirrors Java's
    /// `createFetchRequests` chaining.
    #[tokio::test]
    async fn test_enqueue_ack_completes_on_poll() {
        let mut mgr = make_manager();
        let (tx, rx) = oneshot::channel();
        mgr.enqueue_create_fetch_requests(tx);
        let _ = mgr.poll(0);
        let received = rx.await.expect("ack receiver");
        assert!(received.is_ok());
    }

    /// Use TopicPartition + HashSet to ensure the test-only imports above
    /// are exercised (silences dead-import warnings).
    #[test]
    fn test_imports_smoke() {
        let _ = tp("x", 0);
        let _: HashSet<TopicPartition> = HashSet::new();
    }

    /// Bootstraps the manager's cluster snapshot with a single node so
    /// `poll_on_close` can resolve `cluster.node_by_id(node_id)` to
    /// produce a close-fetch-session `UnsentRequest`.
    ///
    /// `request_test_utils::metadata_update_with(1, ...)` seeds exactly
    /// one node with id=0 (see `metadata_update_with_full` — node IDs are
    /// `0..num_nodes`). So `node_id` MUST be 0 with this helper.
    fn bootstrap_cluster_node(mgr: &FetchRequestManager, node_id: i32, topic: &str, partitions: i32) {
        use std::collections::HashMap;
        debug_assert_eq!(node_id, 0, "bootstrap_cluster_node only emits node id=0 — pass node_id=0");
        mgr.abstract_fetch
            .metadata
            .add_transient_topics(HashSet::from([topic.to_string()]));
        let mut counts = HashMap::new();
        counts.insert(topic.to_string(), partitions);
        let response = crate::common::requests::request_test_utils::metadata_update_with(1, &counts);
        mgr.abstract_fetch
            .metadata
            .metadata_arc()
            .update_with_current_request_version(&response, false, 0);
    }

    /// Build a synthesised `ClientResponse` carrying a `FetchResponse`
    /// (or `None` for a disconnect-style response). Mirrors the
    /// `build_list_offsets_client_response` helper in
    /// `offsets_request_manager.rs`.
    fn build_fetch_client_response(response: Option<FetchResponse>) -> crate::client_response::ClientResponse {
        use crate::common::protocol::ApiKeys;
        use crate::common::requests::request_header::RequestHeader;
        let header = RequestHeader::new(&ApiKeys::FETCH, ApiKeys::FETCH.latest_version(), "", 1).expect("header");
        crate::client_response::ClientResponse::with_timeout(
            header,
            None,
            "0",
            0,
            0,
            false,
            false,
            None,
            None,
            response.map(ConcreteResponse::Fetch),
        )
    }

    /// Phase 12.5 (4/N) regression — response routing via the
    /// `PendingFetchCompletion` mpsc channel on the **failure** path.
    /// Drive `poll_on_close(now)` so a close-fetch-session
    /// `UnsentRequest` is emitted, fire `on_failure` on the request's
    /// handler, and then the next `poll_on_close` call's drain
    /// dispatches into `AbstractFetch::handle_close_fetch_session_failure`,
    /// which removes the node from
    /// `nodes_with_pending_fetch_requests`.
    ///
    /// This is the test that would have caught the Phase-12 audit
    /// response-routing gap on the fetch path (audit verdict: BROKEN,
    /// no production callsite of `take_response_receiver`).
    #[tokio::test]
    async fn test_response_routing_failure_path() {
        let mut mgr = make_manager();
        // Seed the cluster with node id=0 so `poll_on_close` can resolve
        // the node, and create a session handler so the close path
        // produces a request.
        bootstrap_cluster_node(&mgr, 0, "t", 1);
        let _ = mgr.abstract_fetch.session_handler_or_create(0);
        // Insert node 0 into the pending-fetch set so we can observe
        // it being removed by the failure-path drain.
        mgr.abstract_fetch.nodes_with_pending_fetch_requests.insert(0);

        // `poll_on_close` builds a close-fetch-session request for the
        // session-holding node.
        let result = mgr.poll_on_close(100);
        assert_eq!(1, result.unsent_requests.len(), "expected one close-fetch-session request");
        let unsent = result.unsent_requests.into_iter().next().unwrap();

        // Fire a transport-level retriable failure through the handler.
        unsent
            .handler()
            .on_failure(0, KafkaError::new(crate::common::protocol::Errors::NetworkException));

        // Wait deterministically for the drain on the next `poll(now)`
        // to observe the failure and remove node 0 from the pending set.
        // The observable: `pending_fetch_node_ids()` no longer contains
        // node 0 after `handle_close_fetch_session_failure` runs.
        //
        // NOTE: drive `poll(now)` (not `poll_on_close`) — both call
        // `drain_pending_completions` at their top, but `poll_on_close`
        // also unconditionally enqueues a fresh ack and re-emits a
        // close-fetch-session request, which calls `create_fetch_request`
        // and RE-INSERTS the node into the pending-fetch set. Using
        // `poll` keeps the loop drain-only.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        loop {
            let _ = mgr.poll(200);
            if !mgr.abstract_fetch.pending_fetch_node_ids().contains(&0) {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("node 0 remained in pending-fetch set; drain did not observe the spawned forwarder's failure");
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(
            !mgr.abstract_fetch.pending_fetch_node_ids().contains(&0),
            "handle_close_fetch_session_failure must remove the node from the pending-fetch set"
        );
    }

    /// Phase 12.5 (4/N) regression — response routing via the
    /// `PendingFetchCompletion` mpsc channel on the **success** path.
    /// Drive `poll_on_close(now)`, synthesise a successful
    /// `FetchResponse`, fire `on_complete` on the handler, then
    /// observe `AbstractFetch::handle_close_fetch_session_success`
    /// removing the node from the pending-fetch set on the next
    /// drain.
    #[tokio::test]
    async fn test_response_routing_success_path() {
        use crate::fetch_response_data::FetchResponseData;
        let mut mgr = make_manager();
        bootstrap_cluster_node(&mgr, 0, "t", 1);
        let _ = mgr.abstract_fetch.session_handler_or_create(0);
        mgr.abstract_fetch.nodes_with_pending_fetch_requests.insert(0);

        let result = mgr.poll_on_close(100);
        assert_eq!(1, result.unsent_requests.len());
        let unsent = result.unsent_requests.into_iter().next().unwrap();

        // Synthesise an empty but successful `FetchResponse`.
        let mut data = FetchResponseData::new();
        data.set_error_code(0);
        data.set_session_id(0);
        data.set_throttle_time_ms(0);
        let resp = FetchResponse::new(data);
        unsent.handler().on_complete(build_fetch_client_response(Some(resp)));

        // Wait deterministically for the drain on the next `poll(now)`
        // to observe the success and remove node 0. See the failure-path
        // test for why we use `poll` not `poll_on_close` here.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        loop {
            let _ = mgr.poll(200);
            if !mgr.abstract_fetch.pending_fetch_node_ids().contains(&0) {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("node 0 remained in pending-fetch set; drain did not observe the spawned forwarder's success");
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(
            !mgr.abstract_fetch.pending_fetch_node_ids().contains(&0),
            "handle_close_fetch_session_success must remove the node from the pending-fetch set"
        );
    }
}
