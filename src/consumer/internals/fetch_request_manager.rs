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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use log::trace;
use tokio::sync::oneshot;

use crate::common::memory::buffer_supplier::BufferSupplier;
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

/// `FetchRequestManager` — owns an [`AbstractFetch`] and produces fetch
/// `UnsentRequest`s in response to `RequestManager::poll`.
pub(crate) struct FetchRequestManager {
    /// Shared state with the Phase 10 bg task (`AbstractFetch` from 7a).
    abstract_fetch: AbstractFetch,
    /// Queue of pending fetch-request creation acks. FIFO. The Java
    /// equivalent is a single `CompletableFuture<Void>` slot
    /// (`pendingFetchRequestFuture`); the Rust port uses a `VecDeque` to
    /// support multiple in-flight `CreateFetchRequestsEvent` enqueues
    /// (Java chains them via `whenComplete`, the Rust port simply
    /// processes them in order at the next `poll`).
    pending_fetch_requests: VecDeque<oneshot::Sender<Result<(), KafkaError>>>,
    /// Node-availability callbacks supplied by the consumer bg task. They
    /// are stored as `Arc<dyn Fn>` so the bg task can plug in
    /// [`crate::consumer::internals::network_client_delegate::NetworkClientDelegate`]
    /// indirectly without `FetchRequestManager` holding a direct
    /// reference to the delegate.
    is_unavailable: IsUnavailableFn,
    maybe_throw_auth_failure: MaybeAuthFailureFn,
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
        Self {
            abstract_fetch: AbstractFetch::new(
                metadata,
                subscriptions,
                fetch_config,
                fetch_buffer,
                decompression_buffer_supplier,
            ),
            pending_fetch_requests: VecDeque::new(),
            is_unavailable,
            maybe_throw_auth_failure,
        }
    }

    /// Signals that the consumer wants requests to be created for the
    /// broker nodes to fetch the next batch of records.
    ///
    /// Translates Java's
    /// `CompletableFuture<Void> createFetchRequests()`.
    ///
    /// The Java code chains a single pending future via `whenComplete`;
    /// the Rust port simply enqueues the ack on a FIFO. Both signal
    /// "the next `poll` will produce requests".
    pub(crate) fn create_fetch_requests(&mut self) -> oneshot::Receiver<Result<(), KafkaError>> {
        let (tx, rx) = oneshot::channel();
        self.pending_fetch_requests.push_back(tx);
        rx
    }

    /// Enqueue an ack supplied by the Phase 5 `CreateFetchRequestsEvent`.
    ///
    /// This is the Phase 10 wiring entry point — the bg task receives
    /// the event and immediately calls `enqueue_create_fetch_requests`
    /// to enqueue the ack. The next `poll(current_time_ms)` either
    /// completes it with `Ok(())` (requests are dispatched) or
    /// `Err(err)` (no fetch-requestable partitions).
    pub(crate) fn enqueue_create_fetch_requests(&mut self, ack: oneshot::Sender<Result<(), KafkaError>>) {
        self.pending_fetch_requests.push_back(ack);
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

    /// Notifies the underlying `AbstractFetch` of a successful fetch
    /// response. Phase 10 wires this via the response receiver's
    /// `whenComplete` analog.
    pub(crate) fn on_fetch_response(
        &mut self,
        fetch_target: &Node,
        request_data: &FetchSessionRequestData,
        response: &FetchResponse,
        request_version: i16,
    ) {
        self.abstract_fetch
            .handle_fetch_success(fetch_target, request_data, response, request_version);
    }

    /// Notifies the underlying `AbstractFetch` of a failed fetch
    /// response. Phase 10 wires this via the response receiver's
    /// `whenComplete` analog.
    pub(crate) fn on_fetch_failure(
        &mut self,
        fetch_target: &Node,
        request_data: &FetchSessionRequestData,
        error: &KafkaError,
    ) {
        self.abstract_fetch.handle_fetch_failure(fetch_target, request_data, error);
    }

    /// Internal helper: build a `PollResult` by running
    /// `prepare_fetch_requests` and wrapping each result in an
    /// `UnsentRequest`. Mirrors Java's `pollInternal`.
    ///
    /// `for_close = true` switches `prepare_fetch_requests` for
    /// `prepare_close_fetch_session_requests` (`poll_on_close` path).
    fn poll_internal(&mut self, current_time_ms: i64, for_close: bool) -> PollResult {
        if self.pending_fetch_requests.is_empty() {
            // No explicit request for creating fetch requests was issued
            // — short-circuit.
            return PollResult::empty();
        }

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
                    // Java: completes pending future exceptionally and
                    // returns a "dummy" empty PollResult to avoid
                    // interrupting other request managers.
                    if let Some(tx) = self.pending_fetch_requests.pop_front() {
                        let _ = tx.send(Err(e));
                    }
                    return PollResult::empty();
                },
            }
        };

        if prepared.is_empty() {
            // No fetchable partitions: wake the buffer so a polling
            // consumer doesn't wait needlessly, complete the next
            // pending ack, and return empty.
            self.abstract_fetch.fetch_buffer.wakeup();
            if let Some(tx) = self.pending_fetch_requests.pop_front() {
                let _ = tx.send(Ok(()));
            }
            return PollResult::empty();
        }

        // Build the per-node UnsentRequest list and ack the pending
        // create-fetch-requests caller.
        let mut requests: Vec<UnsentRequest> = Vec::with_capacity(prepared.len());
        for (_node_id, (target_node, request_data)) in prepared {
            let builder = self.abstract_fetch.create_fetch_request(&target_node, &request_data);
            let unsent = UnsentRequest::new(Box::new(builder), Some(target_node));
            // Phase 10 wires `whenComplete` here — the bg task awaits
            // `unsent.take_response_receiver()` and dispatches into
            // `on_fetch_response` / `on_fetch_failure`. Phase 7b just
            // returns the request.
            requests.push(unsent);
        }

        if let Some(tx) = self.pending_fetch_requests.pop_front() {
            let _ = tx.send(Ok(()));
        }
        trace!("FetchRequestManager: produced {} fetch requests", requests.len());
        PollResult::with_requests(requests)
    }
}

impl RequestManager for FetchRequestManager {
    /// Translates Java's
    /// `PollResult poll(long currentTimeMs)` — produces the fetch
    /// requests for the next round, if any pending
    /// `CreateFetchRequestsEvent` ack is outstanding.
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        self.poll_internal(current_time_ms, false)
    }

    /// Translates Java's
    /// `PollResult pollOnClose(long currentTimeMs)` — produces the
    /// close-fetch-session requests.
    fn poll_on_close(&mut self, current_time_ms: i64) -> PollResult {
        // Java's pollOnClose unconditionally enqueues a fresh ack so
        // pollInternal has something to satisfy.
        let (tx, _rx) = oneshot::channel();
        self.pending_fetch_requests.push_back(tx);
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
        while let Some(tx) = self.pending_fetch_requests.pop_front() {
            let _ = tx.send(Err(KafkaError::illegal_state(
                "FetchRequestManager dropped with pending CreateFetchRequests ack",
            )));
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

    /// `create_fetch_requests` and `enqueue_create_fetch_requests` both
    /// add to the same FIFO; they ack in insertion order.
    #[tokio::test]
    async fn test_create_fetch_requests_fifo() {
        let mut mgr = make_manager();
        let rx1 = mgr.create_fetch_requests();
        let (tx2, rx2) = oneshot::channel();
        mgr.enqueue_create_fetch_requests(tx2);

        // First poll satisfies rx1, second satisfies rx2.
        let _ = mgr.poll(0);
        assert!(rx1.await.unwrap().is_ok());
        let _ = mgr.poll(0);
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
}
