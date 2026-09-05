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

//! `NetworkClientDelegate` — wraps a [`KafkaClient`] with per-request
//! timers, an `unsent_requests` queue, and metadata-error propagation.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.NetworkClientDelegate`.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::client_response::ClientResponse;
use crate::common::protocol::Errors;
use crate::common::requests::RequestBuilder;
use crate::common::{Error, Node};
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::async_consumer_metrics::AsyncConsumerMetrics;
use crate::consumer::internals::events::background_event::BackgroundEvent;
use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;
use crate::kafka_client::KafkaClient;
use crate::metadata::Metadata;
use crate::network_client_utils;

/// Result returned from [`super::request_manager::RequestManager::poll`].
///
/// Either carries a list of requests to dispatch through the delegate or,
/// when nothing can be sent yet, the time (in ms) the caller can wait
/// before re-polling.
///
/// Java: `NetworkClientDelegate.PollResult`.
pub(crate) struct PollResult {
    /// How long the caller can sleep before there is anything new for the
    /// manager to do, when `unsent_requests` is empty.
    pub time_until_next_poll_ms: i64,
    /// Requests the manager is ready to dispatch immediately.
    pub unsent_requests: Vec<UnsentRequest>,
    /// Nodes for which the manager wants the bg task to call
    /// [`NetworkClientDelegate::try_connect`].
    ///
    /// Java mirrors this by having request managers call
    /// `networkClientDelegate.tryConnect(node)` directly from within their
    /// `poll(...)` body (a `void` side effect). The Rust manager has no
    /// handle on the delegate (the bg task owns it), so the manager
    /// instead emits a hint on the `PollResult` and the bg task drains
    /// `try_connect` and calls `NetworkClientDelegate::try_connect` for
    /// each node before processing `unsent_requests`. The bg-task-side
    /// consumer of this slot lands in Phase 10 commit 7
    /// (ConsumerNetworkThread `runOnce`).
    ///
    /// Currently emitted only by
    /// [`super::offsets_request_manager::OffsetsRequestManager`] when
    /// `NodeApiVersions` are missing for a broker scheduled to receive an
    /// `OffsetsForLeaderEpoch` request.
    pub try_connect: Vec<Node>,
}

impl PollResult {
    /// Java: `PollResult.WAIT_FOREVER`. Sentinel meaning "the manager has
    /// nothing further to do until external state changes."
    pub(crate) const WAIT_FOREVER: i64 = i64::MAX;

    /// An empty result with a "wait forever" hint.
    ///
    /// Java: `PollResult.EMPTY`.
    pub(crate) fn empty() -> Self {
        Self::from_wait(Self::WAIT_FOREVER)
    }

    /// A result with the given wait time and an empty request list.
    ///
    /// Java: `new PollResult(long timeUntilNextPollMs)`.
    pub(crate) fn from_wait(time_until_next_poll_ms: i64) -> Self {
        Self { time_until_next_poll_ms, unsent_requests: Vec::new(), try_connect: Vec::new() }
    }

    /// A result carrying the given requests and `WAIT_FOREVER` wait time.
    ///
    /// Java: `new PollResult(List<UnsentRequest>)`.
    pub(crate) fn with_requests(unsent_requests: Vec<UnsentRequest>) -> Self {
        Self {
            time_until_next_poll_ms: Self::WAIT_FOREVER,
            unsent_requests,
            try_connect: Vec::new(),
        }
    }

    /// A result carrying a single request.
    ///
    /// Java: `new PollResult(UnsentRequest)`.
    pub(crate) fn single(request: UnsentRequest) -> Self {
        Self::with_requests(vec![request])
    }

    /// Full constructor: explicit wait time + request list.
    ///
    /// Java: `new PollResult(long timeUntilNextPollMs, List<UnsentRequest>)`.
    pub(crate) fn new(time_until_next_poll_ms: i64, unsent_requests: Vec<UnsentRequest>) -> Self {
        Self { time_until_next_poll_ms, unsent_requests, try_connect: Vec::new() }
    }
}

impl fmt::Debug for PollResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollResult")
            .field("time_until_next_poll_ms", &self.time_until_next_poll_ms)
            .field("unsent_requests.len", &self.unsent_requests.len())
            .field("try_connect.len", &self.try_connect.len())
            .finish()
    }
}

/// A request enqueued by a [`super::request_manager::RequestManager`] for
/// the bg task to dispatch through the delegate.
///
/// Java: `NetworkClientDelegate.UnsentRequest`. Carries:
///
/// - `request_builder` — the polymorphic builder (`RequestBuilder` trait
///   object) that the delegate uses to compile a `ClientRequest`.
/// - `handler` — the completion sink the bg task fills in once the
///   response arrives. Idempotent (only the first call to
///   `on_complete`/`on_failure` wins).
/// - `node` — the target broker. `None` lets the delegate pick the
///   `least_loaded_node`.
/// - `deadline_ms` and `enqueue_time_ms` — set by the delegate on
///   [`super::network_client_delegate::NetworkClientDelegate::add`] (and
///   `add_all`); `-1` means "not yet enqueued".
pub(crate) struct UnsentRequest {
    /// `Some` while the request is on the unsent queue; consumed
    /// (`None`) once the delegate dispatches it via `do_send` and
    /// transfers ownership to the underlying [`ClientRequest`].
    request_builder: Option<Box<dyn RequestBuilder>>,
    handler: FutureCompletionHandler,
    /// Receiver paired with [`Self::handler`]. The bg task awaits this
    /// (after dispatching the request) to learn when the response or
    /// failure arrives. `Option<...>` so that callers wiring up a
    /// `whenComplete`-equivalent can `take()` it (Phase 6 (6/7) does this
    /// inside [`super::coordinator_request_manager::CoordinatorRequestManager`]).
    response_rx: Option<oneshot::Receiver<Result<ClientResponse, Error>>>,
    node: Option<Node>,
    /// Absolute wall-clock millisecond deadline at which the request
    /// expires. Set by the delegate on `add` to `now + request_timeout_ms`.
    /// `-1` if not yet enqueued (Java leaves the `Timer` null in this case).
    deadline_ms: i64,
    /// Time when the request was enqueued, for metric collection. `-1`
    /// before [`super::network_client_delegate::NetworkClientDelegate::add`]
    /// sets it (Phase 6 (5/7)).
    enqueue_time_ms: i64,
}

impl UnsentRequest {
    /// Constructs a new [`UnsentRequest`] addressing the given node (or
    /// any node if `node` is `None`).
    ///
    /// Java: `new UnsentRequest(AbstractRequest.Builder<?>, Optional<Node>)`.
    pub(crate) fn new(request_builder: Box<dyn RequestBuilder>, node: Option<Node>) -> Self {
        let (handler, rx) = FutureCompletionHandler::new_with_receiver();
        Self {
            request_builder: Some(request_builder),
            handler,
            response_rx: Some(rx),
            node,
            deadline_ms: -1,
            enqueue_time_ms: -1,
        }
    }

    /// Takes ownership of the request builder, leaving `None`. Called by
    /// [`NetworkClientDelegate::do_send`] when the request is being
    /// transferred into a [`ClientRequest`]. After this returns
    /// `Some(...)`, [`Self::request_builder`] returns a placeholder
    /// reference; callers should not look at the builder after dispatch.
    pub(crate) fn take_request_builder(&mut self) -> Box<dyn RequestBuilder> {
        self.request_builder.take().expect("request_builder already consumed")
    }

    /// Takes the response receiver (if not already taken). Mirrors the
    /// Java pattern of registering a `whenComplete` callback on the
    /// `handler.future()`: the manager / bg task takes ownership of the
    /// receiver so it can `.await` the completion.
    pub(crate) fn take_response_receiver(&mut self) -> Option<oneshot::Receiver<Result<ClientResponse, Error>>> {
        self.response_rx.take()
    }

    /// Returns a clone of the completion handler.
    ///
    /// Java: `handler()`. Cloning is safe — the handler is an
    /// `Arc<Mutex<Option<oneshot::Sender>>>` and idempotent completion
    /// is preserved regardless of which clone the bg task uses.
    pub(crate) fn handler(&self) -> FutureCompletionHandler {
        self.handler.clone()
    }

    /// Returns a reference to the request builder, or `None` if it has
    /// already been consumed via [`Self::take_request_builder`].
    ///
    /// Java: `requestBuilder()`.
    pub(crate) fn request_builder(&self) -> Option<&dyn RequestBuilder> {
        self.request_builder.as_deref()
    }

    /// Returns a mutable reference to the request builder, or `None` if it
    /// has already been consumed via [`Self::take_request_builder`].
    ///
    /// Mirrors [`Self::request_builder`] but allows driving the builder
    /// forward through `RequestBuilder::build_version`, which serializes the
    /// underlying message (`&mut self`).
    pub(crate) fn request_builder_mut(&mut self) -> Option<&mut dyn RequestBuilder> {
        match self.request_builder {
            Some(ref mut b) => Some(b.as_mut()),
            None => None,
        }
    }

    /// Returns the optional target node.
    ///
    /// Java: `node()`.
    pub(crate) fn node(&self) -> Option<&Node> {
        self.node.as_ref()
    }

    /// Returns the deadline (absolute ms) or `-1` if not yet enqueued.
    pub(crate) fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }

    /// Returns the enqueue timestamp (ms) or `-1` if not yet enqueued.
    pub(crate) fn enqueue_time_ms(&self) -> i64 {
        self.enqueue_time_ms
    }

    /// Set deadline / enqueue timestamp — invoked by the delegate when the
    /// request is added to the unsent queue (Phase 6 (5/7)).
    pub(crate) fn set_timing(&mut self, deadline_ms: i64, enqueue_time_ms: i64) {
        self.deadline_ms = deadline_ms;
        self.enqueue_time_ms = enqueue_time_ms;
    }
}

impl fmt::Debug for UnsentRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let api_key = self
            .request_builder
            .as_ref()
            .map(|b| b.api_key().name())
            .unwrap_or("<consumed>");
        f.debug_struct("UnsentRequest")
            .field("api_key", &api_key)
            .field("node", &self.node)
            .field("deadline_ms", &self.deadline_ms)
            .field("enqueue_time_ms", &self.enqueue_time_ms)
            .finish()
    }
}

/// Idempotent completion handle for an [`UnsentRequest`].
///
/// Mirrors Phase 5's [`super::events::completable_event::CompletableEventHandle`]
/// pattern: `Arc<Mutex<Option<oneshot::Sender<...>>>>`. Multiple completion
/// calls are safe — only the first call wins; subsequent calls are no-ops
/// (matches Java's `CompletableFuture.complete` / `completeExceptionally`).
///
/// Java: `NetworkClientDelegate.FutureCompletionHandler`.
///
/// The handle is `Clone` so the bg task can hand off completion to a
/// request manager (via `whenComplete`-equivalent) while keeping its own
/// copy for cleanup paths.
#[derive(Clone)]
pub(crate) struct FutureCompletionHandler {
    inner: Arc<FutureCompletionInner>,
}

struct FutureCompletionInner {
    /// Idempotent sender slot. Synchronous `Mutex` because the critical
    /// section is a single `Option::take` and is never held across an
    /// `.await`.
    sender: Mutex<Option<oneshot::Sender<Result<ClientResponse, Error>>>>,
    /// Time (ms) at which `on_complete` / `on_failure` was first called.
    /// Recorded with `set_completion_time_ms` regardless of whether the
    /// receiver is still alive (Java: `responseCompletionTimeMs`).
    completion_time_ms: Mutex<i64>,
}

impl FutureCompletionHandler {
    /// Constructs a fresh handle paired with a receiver — matching Java's
    /// implicit "the handler owns a `CompletableFuture` that you can
    /// `.get()`" contract. The bg task (Phase 6 (5/7)) reads completion
    /// results off the receiver; the manager calls `on_complete` /
    /// `on_failure` on the handle. Each `UnsentRequest` owns the
    /// receiver inside `UnsentRequest::new`.
    pub(crate) fn new_with_receiver() -> (Self, oneshot::Receiver<Result<ClientResponse, Error>>) {
        let (tx, rx) = oneshot::channel();
        let inner = Arc::new(FutureCompletionInner { sender: Mutex::new(Some(tx)), completion_time_ms: Mutex::new(0) });
        (Self { inner }, rx)
    }

    /// Java: `onFailure(long currentTimeMs, RuntimeException e)`. Records
    /// the completion time and completes the receiver with `Err(error)`.
    /// Idempotent — only the first call wins.
    pub(crate) fn on_failure(&self, current_time_ms: i64, error: Error) {
        self.set_completion_time(current_time_ms);
        let sender_opt = {
            let mut guard = match self.inner.sender.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.take()
        };
        if let Some(tx) = sender_opt {
            // Result ignored: if the receiver was already dropped the
            // future is effectively cancelled, which is fine.
            let _ = tx.send(Err(error));
        }
    }

    /// Java: `onComplete(ClientResponse response)`. Dispatches on the
    /// response state and either completes the receiver with `Ok(...)`
    /// or, when an authentication / disconnect / version-mismatch
    /// occurred, with the synthesised error.
    ///
    /// Sends the owned `ClientResponse` through the receiver on success.
    pub(crate) fn on_complete(&self, response: ClientResponse) {
        let completion_time_ms = response.received_time_ms();
        if let Some(auth_error) = response.authentication_error() {
            // Java: `onFailure(completionTimeMs, response.authenticationException())`
            // (`NetworkClientDelegate.java:443-444`) — the object, unchanged. The
            // response now carries the typed error, so the class the channel raised
            // is what the request's future completes with; hardcoding
            // `SASL_AUTHENTICATION_FAILED` reported code 58 for a TLS certificate
            // rejection on a connection that never performed a SASL exchange.
            self.on_failure(completion_time_ms, auth_error.clone());
            return;
        }
        if response.was_disconnected() {
            self.on_failure(completion_time_ms, Error::new(crate::common::protocol::Errors::NetworkError));
            return;
        }
        if let Some(msg) = response.version_mismatch() {
            self.on_failure(completion_time_ms, Error::unsupported_version(msg.to_string()));
            return;
        }
        self.set_completion_time(completion_time_ms);
        let sender_opt = {
            let mut guard = match self.inner.sender.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.take()
        };
        if let Some(tx) = sender_opt {
            let _ = tx.send(Ok(response));
        }
    }

    /// Reads enough state from a `&mut ClientResponse` to drive completion
    /// without consuming the response, then dispatches via [`Self::on_complete`].
    ///
    /// Used to bridge between the [`crate::RequestCompletionHandler`]
    /// callback (`Box<dyn FnOnce(&mut ClientResponse)>`) supplied by the
    /// Rust `KafkaClient::new_client_request_with_timeout` API and the
    /// owned-response receiver paired with the handler.
    pub(crate) fn on_complete_ref(&self, response: &mut ClientResponse) {
        // Rebuild an owned `ClientResponse` from the mutable reference by
        // taking the response body (which is the only non-Clone field
        // that carries real payload).
        let request_header = response.request_header().clone();
        let destination = response.destination().to_string();
        let received_time_ms = response.received_time_ms();
        let disconnected = response.was_disconnected();
        let timed_out = response.was_timed_out();
        let version_mismatch = response.version_mismatch().map(|s| s.to_string());
        let authentication_error = response.authentication_error().cloned();
        let body = response.take_response_body();
        let owned = ClientResponse::with_timeout(
            request_header,
            None,
            &destination,
            received_time_ms - response.request_latency_ms(),
            received_time_ms,
            disconnected,
            timed_out,
            version_mismatch,
            authentication_error,
            body,
        );
        self.on_complete(owned);
    }

    /// Java: `completionTimeMs()`. Time (ms) at which `on_complete` /
    /// `on_failure` was first called. Zero before completion.
    pub(crate) fn completion_time_ms(&self) -> i64 {
        match self.inner.completion_time_ms.lock() {
            Ok(g) => *g,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }

    /// `true` if the sender slot has been consumed (either by
    /// `on_complete` or `on_failure`).
    pub(crate) fn is_done(&self) -> bool {
        let guard = match self.inner.sender.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.is_none()
    }

    fn set_completion_time(&self, current_time_ms: i64) {
        let mut guard = match self.inner.completion_time_ms.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Java's onComplete sets responseCompletionTimeMs only on the
        // successful path; onFailure unconditionally records the time.
        // Either way, the *first* completion time wins because we take
        // the sender slot before recording.
        *guard = current_time_ms;
    }
}

impl fmt::Debug for FutureCompletionHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FutureCompletionHandler")
            .field("is_done", &self.is_done())
            .field("completion_time_ms", &self.completion_time_ms())
            .finish()
    }
}

/// Wraps a [`KafkaClient`] with per-request timers, an unsent-requests
/// queue, and metadata-error propagation.
///
/// Translated from
/// `org.apache.kafka.clients.consumer.internals.NetworkClientDelegate`.
///
/// # Generics vs `Box<dyn KafkaClient>`
///
/// [`KafkaClient::ready`] and [`KafkaClient::poll`] return `impl Future`,
/// which makes [`KafkaClient`] non-object-safe. The delegate is therefore
/// generic over `K: KafkaClient + Send`; the bg task (Phase 10) holds a
/// concrete `K = NetworkClient<...>` and tests use [`crate::mock_client::MockClient`].
///
/// # `async fn poll`
///
/// Java's `poll(timeoutMs, currentTimeMs)` is sync because the Java
/// `KafkaClient::poll` is sync-blocking on a Java thread. In Rust,
/// [`KafkaClient::poll`] is `async`, so [`NetworkClientDelegate::poll`]
/// must also be `async`.
pub(crate) struct NetworkClientDelegate<K: KafkaClient + Send> {
    client: K,
    background_event_handler: Arc<BackgroundEventHandler>,
    metadata: Arc<Metadata>,
    client_id: String,
    request_timeout_ms: i32,
    retry_backoff_ms: i64,
    unsent_requests: VecDeque<UnsentRequest>,
    metadata_error: Option<Error>,
    notify_metadata_errors_via_error_queue: bool,
    /// Async-consumer metrics (`AsyncConsumerMetrics`). `None` until wired
    /// post-construction by the live consumer (M4/M5 setter precedent);
    /// tests leave it unset and the record points become no-ops.
    async_consumer_metrics: Option<Arc<AsyncConsumerMetrics>>,
}

impl<K: KafkaClient + Send> NetworkClientDelegate<K> {
    /// Construct a new delegate.
    ///
    /// Java: `NetworkClientDelegate(Time, ConsumerConfig, LogContext,
    /// KafkaClient, Metadata, BackgroundEventHandler, boolean,
    /// AsyncConsumerMetrics)`. The `AsyncConsumerMetrics` is wired
    /// post-construction via [`Self::set_async_consumer_metrics`] (Phase M6,
    /// M4/M5 setter precedent) rather than passed to the constructor.
    pub(crate) fn new(
        config: &ConsumerConfig,
        client: K,
        metadata: Arc<Metadata>,
        background_event_handler: Arc<BackgroundEventHandler>,
        notify_metadata_errors_via_error_queue: bool,
    ) -> Self {
        Self {
            client,
            background_event_handler,
            metadata,
            client_id: config.client_id().to_string(),
            request_timeout_ms: config.request_timeout_ms(),
            retry_backoff_ms: config.retry_backoff_ms(),
            unsent_requests: VecDeque::new(),
            metadata_error: None,
            notify_metadata_errors_via_error_queue,
            async_consumer_metrics: None,
        }
    }

    /// Wires the `AsyncConsumerMetrics` post-construction (M4/M5 setter
    /// precedent — keeps the existing `new` signature and test call sites
    /// untouched). Java passes `AsyncConsumerMetrics` to the constructor.
    pub(crate) fn set_async_consumer_metrics(&mut self, metrics: Arc<AsyncConsumerMetrics>) {
        self.async_consumer_metrics = Some(metrics);
    }

    /// Visible-for-testing accessor for the unsent-requests queue.
    /// Java: package-private `unsentRequests()`.
    pub(crate) fn unsent_requests(&self) -> &VecDeque<UnsentRequest> {
        &self.unsent_requests
    }

    /// Visible-for-testing accessor for the underlying client.
    ///
    /// Used by Phase 10 commit 8 to read counters off `CountingClient`
    /// — the test wrapper that records `delegate.poll(...)` call counts
    /// (replacing Mockito's `verify(client).poll(...)` in Java tests).
    #[cfg(test)]
    pub(crate) fn client_for_test_ref(&self) -> &K {
        &self.client
    }

    /// Visible-for-testing mutable accessor (matches Java's
    /// `unsentRequests()` semantics: tests can `poll()` / `iterator()`).
    pub(crate) fn unsent_requests_mut(&mut self) -> &mut VecDeque<UnsentRequest> {
        &mut self.unsent_requests
    }

    /// Returns the number of in-flight requests (delegates to the
    /// underlying [`KafkaClient`]).
    ///
    /// Java: `inflightRequestCount()`.
    pub(crate) fn inflight_request_count(&self) -> i32 {
        self.client.in_flight_request_count()
    }

    /// Returns `true` if the node is disconnected and unavailable for
    /// immediate reconnection (i.e. inside the reconnect backoff window).
    ///
    /// Java: `isUnavailable(Node)`.
    pub(crate) fn is_unavailable(&self, node: &Node, current_time_ms: i64) -> bool {
        network_client_utils::is_unavailable(&self.client, node, current_time_ms)
    }

    /// Returns an authentication error for the given node, if any.
    ///
    /// Java: `maybeThrowAuthFailure(Node)` (throws on auth failure), which
    /// delegates to `NetworkClientUtils.maybeThrowAuthFailure` and rethrows
    /// `client.authenticationException(node)` verbatim
    /// (`NetworkClientUtils.java:141-145`) — so the class is returned unchanged
    /// rather than rebuilt as a SASL failure.
    pub(crate) fn maybe_return_auth_failure(&self, node: &Node) -> Result<(), Error> {
        match self.client.authentication_error(node) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Initiate a connection if currently possible — primarily useful for
    /// resetting the failed status of a socket.
    ///
    /// Java: `tryConnect(Node)`.
    pub(crate) async fn try_connect(&mut self, node: &Node, current_time_ms: i64) {
        network_client_utils::try_connect(&mut self.client, node, current_time_ms).await;
    }

    /// Returns `true` if there is at least one in-flight request or an
    /// unsent request.
    ///
    /// Java: `hasAnyPendingRequests()`.
    pub(crate) fn has_any_pending_requests(&self) -> bool {
        self.client.has_in_flight_requests() || !self.unsent_requests.is_empty()
    }

    /// Returns and clears the most recent metadata error.
    ///
    /// Java: `getAndClearMetadataError()`.
    pub(crate) fn get_and_clear_metadata_error(&mut self) -> Option<Error> {
        self.metadata_error.take()
    }

    /// Returns the least-loaded node from the underlying client.
    ///
    /// Java: `leastLoadedNode()`.
    pub(crate) fn least_loaded_node(&self, current_time_ms: i64) -> Option<Node> {
        self.client.least_loaded_node(current_time_ms).node().cloned()
    }

    /// Wake up the underlying client (used to break it out of a blocking
    /// `poll`).
    ///
    /// Java: `wakeup()`.
    pub(crate) fn wakeup(&self) {
        self.client.wakeup();
    }

    /// Returns a lock-free handle to the underlying selector's wakeup
    /// primitive — Java's `Selector.wakeup()`.
    ///
    /// This is the consumer's single "make the background task stop waiting"
    /// channel. Firing it returns an in-progress `poll()` at a safe boundary
    /// WITHOUT cancelling it, which matters because the poll is not
    /// cancellation-safe (see
    /// `design/current/consumer-join-stall-rootcause.md`). Being an `Arc<Notify>`
    /// it needs no lock, so the app side can fire it while the bg task holds the
    /// delegate mutex for the whole poll.
    ///
    /// Held by `ApplicationEventHandler` (event enqueue + the §31 ack path),
    /// `FetchRequestManager` (fetch-completion signal), and the close handle
    /// (`wakeup` / `signal_close`). Unlike [`WakeupTrigger`], it has no
    /// user-visible effect and no disabled state.
    pub(crate) fn wakeup_handle(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.client.wakeup_handle()
    }

    /// Returns `true` if the node has previously failed to connect and is
    /// inside the connection-delay backoff window.
    ///
    /// Java: `nodeUnavailable(Node)`.
    pub(crate) fn node_unavailable(&self, node: &Node, current_time_ms: i64) -> bool {
        self.client.connection_failed(node) && self.client.connection_delay(node, current_time_ms) > 0
    }

    /// Close the underlying client.
    ///
    /// Java: `close()` (declared `throws IOException`).
    pub(crate) async fn close(&mut self) -> Result<(), Error> {
        self.client.close().await;
        Ok(())
    }

    /// Adds an unsent request to the queue, stamping its deadline (`now +
    /// request_timeout_ms`) and enqueue time.
    ///
    /// Java: `add(UnsentRequest)`.
    pub(crate) fn add(&mut self, mut request: UnsentRequest, current_time_ms: i64) {
        request.set_timing(current_time_ms.saturating_add(self.request_timeout_ms as i64), current_time_ms);
        self.unsent_requests.push_back(request);
    }

    /// Adds all unsent requests from the slice to the queue.
    ///
    /// Java: `addAll(List<UnsentRequest>)`.
    pub(crate) fn add_all(&mut self, requests: Vec<UnsentRequest>, current_time_ms: i64) {
        for r in requests {
            self.add(r, current_time_ms);
        }
    }

    /// Adds the requests carried by `poll_result` and returns its
    /// `time_until_next_poll_ms` hint.
    ///
    /// Java: `addAll(PollResult)`.
    pub(crate) fn add_all_from_poll_result(&mut self, poll_result: PollResult, current_time_ms: i64) -> i64 {
        let PollResult { time_until_next_poll_ms, unsent_requests, try_connect: _ } = poll_result;
        // The `try_connect` slot is consumed by the bg task (Phase 10
        // commit 7) — it loops `delegate.try_connect(node, now).await`
        // BEFORE calling `add_all_from_poll_result`, so this helper
        // simply drops the field. This keeps the
        // `NetworkClientDelegate::add_all_from_poll_result` API
        // behavior-faithful to Java's `addAll(PollResult)`.
        self.add_all(unsent_requests, current_time_ms);
        time_until_next_poll_ms
    }

    /// Try to send unsent requests, poll for responses, and check
    /// disconnected nodes.
    ///
    /// `on_close = false` matches Java's default `poll(timeoutMs, now)`;
    /// `true` is the `poll(timeoutMs, now, true)` overload used during
    /// shutdown to drop unsent requests with no assigned node.
    ///
    /// Java: `poll(timeoutMs, currentTimeMs)` and `poll(timeoutMs,
    /// currentTimeMs, onClose)`.
    pub(crate) async fn poll(&mut self, timeout_ms: i64, current_time_ms: i64, on_close: bool) {
        self.try_send(current_time_ms).await;

        // Java: pollTimeoutMs = !unsent.isEmpty() ? min(retryBackoff,
        // timeoutMs) : timeoutMs.
        let poll_timeout_ms = if !self.unsent_requests.is_empty() {
            self.retry_backoff_ms.min(timeout_ms)
        } else {
            timeout_ms
        };

        // Drain responses — KafkaClient::poll fires registered callbacks
        // internally via `ClientResponse::on_complete`, so the
        // FutureCompletionHandler attached to each UnsentRequest sees
        // its result here.
        let _responses = self.client.poll(poll_timeout_ms, current_time_ms).await;
        // Compute a fresh time to mirror Java's `updatedNow` capture
        // after the (potentially blocking) poll. We don't have a
        // `Time` source plumbed in; callers thread `current_time_ms`.
        self.maybe_propagate_metadata_error(current_time_ms);
        self.check_disconnects(current_time_ms, on_close);
        // Java NCD:169 — record the unsent-requests queue size at the end of
        // poll (per bg poll, not per-record).
        if let Some(metrics) = &self.async_consumer_metrics {
            metrics.record_unsent_requests_queue_size(self.unsent_requests.len() as i32, current_time_ms);
        }
    }

    /// Convenience: `poll(timeout_ms, current_time_ms, false)`.
    pub(crate) async fn poll_default(&mut self, timeout_ms: i64, current_time_ms: i64) {
        self.poll(timeout_ms, current_time_ms, false).await;
    }

    /// Convenience: `poll(timeout_ms, current_time_ms, true)`.
    pub(crate) async fn poll_on_close(&mut self, timeout_ms: i64, current_time_ms: i64) {
        self.poll(timeout_ms, current_time_ms, true).await;
    }

    /// Iterates the unsent-requests queue and dispatches every request
    /// whose target node is ready. Expired requests are pulled out and
    /// completed with a [`Error::Timeout`].
    ///
    /// Java: package-private `trySend(long currentTimeMs)`.
    async fn try_send(&mut self, current_time_ms: i64) {
        // We pull each request out, decide whether to keep it (re-queue),
        // and at the end swap the rebuilt queue back in. This sidesteps
        // borrow issues from holding `&mut self.client` while iterating
        // `&mut self.unsent_requests`.
        let mut requeue: VecDeque<UnsentRequest> = VecDeque::with_capacity(self.unsent_requests.len());
        let mut queue = std::mem::take(&mut self.unsent_requests);
        while let Some(mut unsent) = queue.pop_front() {
            // Java: `unsent.timer.update(currentTimeMs)` then `isExpired`.
            if unsent.deadline_ms() >= 0 && current_time_ms >= unsent.deadline_ms() {
                let timeout_ms = unsent.deadline_ms().saturating_sub(unsent.enqueue_time_ms());
                // Java NCD:203 — record the queue time when an expired request
                // is removed. Java uses `time.milliseconds()`; the Rust
                // delegate threads `current_time_ms` (its `updatedNow`
                // approximation), so use that for the removal timestamp.
                self.record_unsent_requests_queue_time(&unsent, current_time_ms);
                unsent.handler().on_failure(
                    current_time_ms,
                    Error::timeout(format!("Failed to send request after {timeout_ms} ms.")),
                );
                continue;
            }
            if !self.do_send(&mut unsent, current_time_ms).await {
                // Not ready yet — re-queue and retry next poll.
                requeue.push_back(unsent);
            } else {
                // Java NCD:214 — record the queue time when a request is
                // successfully sent and removed from the queue.
                self.record_unsent_requests_queue_time(&unsent, current_time_ms);
            }
        }
        self.unsent_requests = requeue;
    }

    /// Records the time a request spent in the unsent-requests queue, when
    /// it is removed (sent, expired, or disconnected). Java computes
    /// `time.milliseconds() - unsent.enqueueTimeMs()`. A request that was
    /// never stamped (`enqueue_time_ms == -1`) is skipped.
    fn record_unsent_requests_queue_time(&self, unsent: &UnsentRequest, current_time_ms: i64) {
        if let Some(metrics) = &self.async_consumer_metrics
            && unsent.enqueue_time_ms() >= 0
        {
            metrics.record_unsent_requests_queue_time(current_time_ms - unsent.enqueue_time_ms());
        }
    }

    /// Attempt to dispatch one request. Returns `true` if the request was
    /// sent (and should be removed from the queue), `false` if it should
    /// be retried next poll.
    ///
    /// Java: package-private `doSend(UnsentRequest, long)`.
    async fn do_send(&mut self, unsent: &mut UnsentRequest, current_time_ms: i64) -> bool {
        // Pick the target node: the request's preference, or the
        // least-loaded node returned by the client.
        let node = match unsent.node().cloned() {
            Some(n) => n,
            None => match self.client.least_loaded_node(current_time_ms).node() {
                Some(n) => n.clone(),
                None => {
                    log::debug!("No broker available to send the request: {unsent:?}. Retrying.");
                    return false;
                },
            },
        };
        if self.node_unavailable(&node, current_time_ms) {
            log::debug!("No broker available to send the request: {unsent:?}. Retrying.");
            return false;
        }

        if !self.client.ready(&node, current_time_ms).await {
            log::debug!(
                "Node is not ready, handle the request in the next event loop: node={node}, request={unsent:?}"
            );
            return false;
        }

        // Build the ClientRequest with a callback that drives the
        // FutureCompletionHandler. The handler is cloned (shares
        // Arc<Inner>) so the request's owner can still inspect it after
        // dispatch via `unsent.handler()`.
        let handler_for_callback = unsent.handler();
        let callback: crate::RequestCompletionHandler = Box::new(move |response: &mut ClientResponse| {
            handler_for_callback.on_complete_ref(response);
        });

        // Steal the builder out of `unsent` — once sent, we no longer
        // need it on the queue. We replace it with a no-op placeholder.
        let builder = unsent.take_request_builder();
        let request_timeout = unsent.deadline_ms().saturating_sub(current_time_ms).max(0) as i32;
        let request = self.client.new_client_request_with_timeout(
            node.id_string(),
            builder,
            current_time_ms,
            true,
            request_timeout,
            Some(callback),
        );
        self.client.send(request, current_time_ms);
        true
    }

    /// Check unsent requests for disconnected target nodes.
    ///
    /// Java: protected `checkDisconnects(long, boolean)`.
    fn check_disconnects(&mut self, current_time_ms: i64, on_close: bool) {
        let mut requeue: VecDeque<UnsentRequest> = VecDeque::with_capacity(self.unsent_requests.len());
        let mut queue = std::mem::take(&mut self.unsent_requests);
        while let Some(unsent) = queue.pop_front() {
            match unsent.node() {
                Some(n) if self.client.connection_failed(n) => {
                    // Java NCD:243-244 hands `client.authenticationException(node)`
                    // to `onFailure` unchanged, and `onFailure` substitutes
                    // `DisconnectException.INSTANCE` when it is null
                    // (`NetworkClientDelegate.java:427-434`).
                    let err = match self.client.authentication_error(n) {
                        Some(error) => error,
                        None => Error::new(Errors::NetworkError),
                    };
                    // Java NCD:242 — record queue time on disconnect removal.
                    self.record_unsent_requests_queue_time(&unsent, current_time_ms);
                    unsent.handler().on_failure(current_time_ms, err);
                },
                None if on_close => {
                    log::debug!("Removing unsent request because the client is closing: {unsent:?}");
                    // Java NCD:248 — record queue time on close removal.
                    self.record_unsent_requests_queue_time(&unsent, current_time_ms);
                    unsent.handler().on_failure(current_time_ms, Error::new(Errors::NetworkError));
                },
                _ => requeue.push_back(unsent),
            }
        }
        self.unsent_requests = requeue;
    }

    /// Propagate the latest metadata error either to the
    /// `BackgroundEventHandler` (if `notifyMetadataErrorsViaErrorQueue`)
    /// or store it locally for `get_and_clear_metadata_error`.
    fn maybe_propagate_metadata_error(&mut self, current_time_ms: i64) {
        if let Err(err) = self.metadata.maybe_return_any_error() {
            if self.notify_metadata_errors_via_error_queue {
                // Best-effort: if the receiver was dropped (consumer
                // closed), there is nowhere to deliver the error.
                let _ = self
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: err }, current_time_ms);
            } else {
                self.metadata_error = Some(err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use tokio::sync::mpsc;

    use super::*;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::requests::{
        ConcreteResponse, FindCoordinatorRequestBuilder, FindCoordinatorResponse, MetadataRequestBuilder,
        RequestBuilder,
    };
    use crate::find_coordinator_request_data::FindCoordinatorRequestData;
    use crate::mock_client::MockClient;

    const GROUP_ID: &str = "group";
    const REQUEST_TIMEOUT_MS: i32 = 5_000;

    fn mock_node() -> Node {
        Node::new(0, "localhost".to_string(), 99)
    }

    fn test_config() -> ConsumerConfig {
        ConsumerConfig::new(vec!["localhost:9092".to_string()])
            .with_client_id("test-client")
            .with_group_id(GROUP_ID)
            .with_request_timeout_ms(REQUEST_TIMEOUT_MS)
    }

    /// Helper builder for a `FindCoordinator` `UnsentRequest` targeting
    /// the `group` GROUP_ID.
    fn new_unsent_find_coordinator_request() -> UnsentRequest {
        let mut data = FindCoordinatorRequestData::new();
        data.set_key(GROUP_ID.to_string());
        data.set_key_type(crate::common::requests::CoordinatorType::Group.id());
        let builder: Box<dyn RequestBuilder> = Box::new(FindCoordinatorRequestBuilder::new(data));
        UnsentRequest::new(builder, None)
    }

    /// Creates a fresh delegate + BackgroundEvent receiver pair for use
    /// in tests. The receiver is held by the caller; the handler is
    /// owned by the delegate (Arc-wrapped) so the test can inspect
    /// metadata-error events as needed.
    fn new_delegate(
        time: Arc<AtomicI64>,
        notify_via_queue: bool,
    ) -> (
        NetworkClientDelegate<MockClient>,
        Arc<Metadata>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let handler = Arc::new(BackgroundEventHandler::new(tx));
        let time_provider: Arc<dyn Fn() -> i64 + Send + Sync> = {
            let time = Arc::clone(&time);
            Arc::new(move || time.load(Ordering::SeqCst))
        };
        let client = MockClient::new_nodes(vec![mock_node()], Arc::clone(&time_provider));
        let metadata = Arc::new(Metadata::new(100, 1_000, 60_000, ClusterResourceListeners::new()));
        let config = test_config();
        let delegate = NetworkClientDelegate::new(&config, client, Arc::clone(&metadata), handler, notify_via_queue);
        (delegate, metadata, rx)
    }

    #[test]
    fn poll_result_empty_uses_wait_forever() {
        let res = PollResult::empty();
        assert_eq!(res.time_until_next_poll_ms, PollResult::WAIT_FOREVER);
        assert!(res.unsent_requests.is_empty());
    }

    #[test]
    fn poll_result_from_wait_carries_value() {
        let res = PollResult::from_wait(500);
        assert_eq!(res.time_until_next_poll_ms, 500);
        assert!(res.unsent_requests.is_empty());
    }

    #[test]
    fn unsent_request_defaults() {
        let builder: Box<dyn RequestBuilder> = Box::new(MetadataRequestBuilder::all_topics());
        let req = UnsentRequest::new(builder, None);
        assert_eq!(req.deadline_ms(), -1);
        assert_eq!(req.enqueue_time_ms(), -1);
        assert!(req.node().is_none());
    }

    /// Translated from `NetworkClientDelegateTest.testPollResultTimer`.
    /// Verifies that `add_all_from_poll_result` returns the wait hint
    /// even when the poll result carries one or zero requests.
    #[tokio::test(flavor = "current_thread")]
    async fn test_poll_result_timer() {
        let time = Arc::new(AtomicI64::new(0));
        let (mut ncd, _meta, _rx) = new_delegate(Arc::clone(&time), false);
        let req = new_unsent_find_coordinator_request();
        let success = PollResult::new(10, vec![req]);
        assert_eq!(10, ncd.add_all_from_poll_result(success, time.load(Ordering::SeqCst)));

        let failure = PollResult::new(10, Vec::new());
        assert_eq!(10, ncd.add_all_from_poll_result(failure, time.load(Ordering::SeqCst)));
    }

    /// Translated from `NetworkClientDelegateTest.testEnsureCorrectCompletionTimeOnFailure`.
    /// The completion time stored on the handler must reflect the time
    /// of the failure, not a later sleep.
    #[test]
    fn test_ensure_correct_completion_time_on_failure() {
        let unsent = new_unsent_find_coordinator_request();
        let handler = unsent.handler();
        handler.on_failure(0, Error::timeout("x"));
        // Subsequent "sleeps" must not advance the completion time.
        assert_eq!(0, handler.completion_time_ms());
    }

    /// Translated from `NetworkClientDelegateTest.testEnsureTimerSetOnAdd`.
    /// Verifies that `add` and `add_all` stamp the timing fields on
    /// every request enqueued.
    #[test]
    fn test_ensure_timer_set_on_add() {
        let time = Arc::new(AtomicI64::new(0));
        let (mut ncd, _meta, _rx) = new_delegate(Arc::clone(&time), false);
        let req = new_unsent_find_coordinator_request();
        assert_eq!(req.deadline_ms(), -1);
        ncd.add(req, time.load(Ordering::SeqCst));
        assert_eq!(1, ncd.unsent_requests().len());
        let head = ncd.unsent_requests().front().unwrap();
        assert_eq!(REQUEST_TIMEOUT_MS as i64, head.deadline_ms() - head.enqueue_time_ms());

        // add_all path.
        let req2 = new_unsent_find_coordinator_request();
        ncd.add_all(vec![req2], time.load(Ordering::SeqCst));
        assert_eq!(2, ncd.unsent_requests().len());
        let last = ncd.unsent_requests().back().unwrap();
        assert_eq!(REQUEST_TIMEOUT_MS as i64, last.deadline_ms() - last.enqueue_time_ms());
    }

    /// Translated from `NetworkClientDelegateTest.testHasAnyPendingRequests`
    /// (the portion that exercises the queue-management invariants —
    /// "unsent" before poll, "in-flight" after dispatch). The `client.poll`
    /// step that drains the responses is exercised by
    /// [`test_successful_response`].
    #[tokio::test(flavor = "current_thread")]
    async fn test_has_any_pending_requests() {
        // Start at a non-zero time so MockClient.not_throttled (strict `>`)
        // returns true on the very first send.
        let time = Arc::new(AtomicI64::new(1));
        let (mut ncd, _meta, _rx) = new_delegate(Arc::clone(&time), false);
        let req = new_unsent_find_coordinator_request();
        ncd.add(req, time.load(Ordering::SeqCst));

        // Unsent
        assert!(ncd.has_any_pending_requests());
        assert!(!ncd.unsent_requests().is_empty());

        // Stage a response so client.poll completes the request.
        let response = FindCoordinatorResponse::prepare_response(Errors::None, GROUP_ID, &mock_node());
        ncd.client.prepare_response(ConcreteResponse::FindCoordinator(response));

        ncd.poll(0, time.load(Ordering::SeqCst), false).await;

        // Response delivered — queue is empty and no in-flight remains.
        assert!(ncd.unsent_requests().is_empty());
    }

    /// Translated from `NetworkClientDelegateTest.testSuccessfulResponse`.
    /// End-to-end: enqueue a FindCoordinator request, stage a success
    /// response on the mock client, poll the delegate, and verify the
    /// future resolves.
    #[tokio::test(flavor = "current_thread")]
    async fn test_successful_response() {
        // Start at a non-zero time so MockClient.not_throttled (strict `>`)
        // returns true on the very first send.
        let time = Arc::new(AtomicI64::new(1));
        let (mut ncd, _meta, _rx) = new_delegate(Arc::clone(&time), false);
        let mut req = new_unsent_find_coordinator_request();
        let mut rx = req.take_response_receiver().expect("receiver still present");

        let response = FindCoordinatorResponse::prepare_response(Errors::None, GROUP_ID, &mock_node());
        ncd.client.prepare_response(ConcreteResponse::FindCoordinator(response));

        ncd.add(req, time.load(Ordering::SeqCst));
        ncd.poll(0, time.load(Ordering::SeqCst), false).await;

        // The handler's oneshot must resolve with Ok(ClientResponse).
        let result = rx.try_recv().expect("response delivered");
        match result {
            Ok(_) => {},
            Err(err) => panic!("expected Ok response, got: {err}"),
        }
    }

    /// Translated from `NetworkClientDelegateTest.testPropagateMetadataError`.
    /// Verifies the legacy path: metadata errors are stored locally and
    /// returned by `get_and_clear_metadata_error`.
    #[tokio::test(flavor = "current_thread")]
    async fn test_propagate_metadata_error() {
        let time = Arc::new(AtomicI64::new(0));
        let (mut ncd, meta, _rx) = new_delegate(Arc::clone(&time), false);
        meta.fatal_error(Error::timeout("Test auth failure"));
        assert!(ncd.get_and_clear_metadata_error().is_none());

        ncd.poll(0, time.load(Ordering::SeqCst), false).await;

        let metadata_error = ncd.get_and_clear_metadata_error().expect("error captured");
        assert!(metadata_error.message().contains("Test auth failure"), "got: {metadata_error}");
    }

    /// Translated from `NetworkClientDelegateTest.testPropagateMetadataErrorWithErrorEvent`.
    /// Verifies the `notify_metadata_errors_via_error_queue = true`
    /// path: errors flow as `BackgroundEvent::Error` through the
    /// handler's channel.
    #[tokio::test(flavor = "current_thread")]
    async fn test_propagate_metadata_error_with_error_event() {
        let time = Arc::new(AtomicI64::new(0));
        let (mut ncd, meta, mut bg_rx) = new_delegate(Arc::clone(&time), true);
        meta.fatal_error(Error::timeout("Test auth failure"));

        ncd.poll(0, time.load(Ordering::SeqCst), false).await;

        let envelope = bg_rx.try_recv().expect("metadata error delivered to bg queue");
        match envelope.event {
            BackgroundEvent::Error { error } => {
                assert!(error.message().contains("Test auth failure"), "got: {error}");
            },
            other => panic!("expected BackgroundEvent::Error, got {}", other.type_name()),
        }
    }

    #[test]
    fn future_completion_handler_idempotent_send() {
        let (handle, rx) = FutureCompletionHandler::new_with_receiver();
        assert!(!handle.is_done());

        handle.on_failure(123, Error::timeout("boom"));
        assert!(handle.is_done());
        assert_eq!(handle.completion_time_ms(), 123);

        // Second call must not double-send; matches Java's
        // CompletableFuture.completeExceptionally idempotence.
        handle.on_failure(456, Error::timeout("again"));

        // Receiver resolves with the *first* error only (the second
        // send into the consumed sender slot is dropped).
        let received = rx.blocking_recv().expect("sender alive until first send");
        match received {
            Err(Error::Timeout(msg)) => assert_eq!(msg.message(), "boom"),
            Err(other) => panic!("expected timeout error, got: {other}"),
            Ok(_) => panic!("expected error variant, got Ok"),
        }
    }

    /// Translated from `NetworkClientDelegateTest.testEnsureCorrectCompletionTimeOnComplete`.
    /// Sibling of `testEnsureCorrectCompletionTimeOnFailure`: on the
    /// success path (`on_complete`), the handler's completion-time
    /// records the response's `received_time_ms`, not any later time.
    #[test]
    fn test_ensure_correct_completion_time_on_complete() {
        let unsent = new_unsent_find_coordinator_request();
        let handler = unsent.handler();
        let received_time_ms = 1_234_i64;

        // Build a synthetic, non-disconnected response carrying a
        // FindCoordinator body. The handler's on_complete pulls
        // `received_time_ms` off the response and stores it.
        let header = crate::common::requests::RequestHeader::new_request_api_key_request_version_client_id_options(
            &crate::common::protocol::ApiKeys::FIND_COORDINATOR,
            0,
            "",
            crate::common::requests::RequestHeaderOptions::new(1),
        )
        .expect("header ok");
        let body = FindCoordinatorResponse::prepare_response(Errors::None, GROUP_ID, &mock_node());
        let response = ClientResponse::with_timeout(
            header,
            None,
            "0",
            received_time_ms,
            received_time_ms,
            false,
            false,
            None,
            None,
            Some(ConcreteResponse::FindCoordinator(body)),
        );

        handler.on_complete(response);
        assert_eq!(received_time_ms, handler.completion_time_ms());
    }

    /// Translated from `NetworkClientDelegateTest.testTimeoutBeforeSend`.
    /// Marks the only node unreachable so `do_send` never succeeds, then
    /// advances time past `request_timeout_ms`; the expiry branch of
    /// `try_send` fires `on_failure(Error::Timeout)` and the
    /// receiver resolves with that error.
    #[tokio::test(flavor = "current_thread")]
    async fn test_timeout_before_send() {
        // Start at a non-zero time so MockClient.not_throttled (strict
        // `>`) returns true on the first send attempt — though for this
        // test the request never actually sends.
        let time = Arc::new(AtomicI64::new(1));
        let (mut ncd, _meta, _rx) = new_delegate(Arc::clone(&time), false);

        // Mark the sole node unreachable for the full request timeout
        // window. `set_unreachable` also disconnects the connection.
        ncd.client.set_unreachable(&mock_node(), REQUEST_TIMEOUT_MS as i64);

        let mut req = new_unsent_find_coordinator_request();
        let mut rx = req.take_response_receiver().expect("receiver still present");
        ncd.add(req, time.load(Ordering::SeqCst));

        // First poll: do_send returns false because the node is
        // unreachable. Request stays on the unsent queue.
        ncd.poll(0, time.load(Ordering::SeqCst), false).await;
        assert!(!ncd.unsent_requests().is_empty());

        // Advance past the request's deadline.
        time.fetch_add(REQUEST_TIMEOUT_MS as i64, Ordering::SeqCst);

        // Second poll: try_send sees `current_time_ms >= deadline_ms`
        // and fires `on_failure(Error::timeout(...))`.
        ncd.poll(0, time.load(Ordering::SeqCst), false).await;
        assert!(ncd.unsent_requests().is_empty(), "expired request was removed");

        let received = rx.try_recv().expect("response delivered");
        match received {
            Err(Error::Timeout(_)) => {},
            Err(other) => panic!("expected Timeout, got: {other}"),
            Ok(_) => panic!("expected error variant, got Ok"),
        }
    }

    /// Translated from `NetworkClientDelegateTest.testTimeoutAfterSend`.
    /// Sends a request successfully, then advances time past
    /// `request_timeout_ms` so the underlying `MockClient::poll` times
    /// the in-flight request out (Java: `DisconnectException`; Rust:
    /// `Error` carrying `Errors::NetworkError`).
    #[tokio::test(flavor = "current_thread")]
    async fn test_timeout_after_send() {
        let time = Arc::new(AtomicI64::new(1));
        let (mut ncd, _meta, _rx) = new_delegate(Arc::clone(&time), false);

        let mut req = new_unsent_find_coordinator_request();
        let mut rx = req.take_response_receiver().expect("receiver still present");
        ncd.add(req, time.load(Ordering::SeqCst));

        // First poll dispatches the request successfully — node is
        // reachable, so `do_send` puts it into `client.requests`.
        ncd.poll(0, time.load(Ordering::SeqCst), false).await;
        assert!(ncd.unsent_requests().is_empty(), "request was sent");
        assert!(ncd.client.has_in_flight_requests(), "request is in-flight");

        // Advance past the request's timeout. `MockClient::poll` will
        // detect the expired in-flight request, disconnect the node,
        // and synthesise a `disconnected=true` `ClientResponse` — the
        // FutureCompletionHandler then routes that to
        // `on_failure(Error::new(Errors::NetworkError))`.
        time.fetch_add(REQUEST_TIMEOUT_MS as i64, Ordering::SeqCst);
        ncd.poll(0, time.load(Ordering::SeqCst), false).await;

        let received = rx.try_recv().expect("response delivered");
        match received {
            Err(err) => {
                // Java surfaces a `DisconnectException` here.
                assert_eq!(Errors::NetworkError, err.error(), "expected a network error, got: {err}");
            },
            Ok(_) => panic!("expected disconnect error, got Ok"),
        }
    }

    /// Translated from `NetworkClientDelegateTest.testPollWithOnClose`.
    /// Exercises the `on_close = true` overload via
    /// `poll_on_close`: the poll still drains responses, but `check_disconnects`
    /// also drops unsent requests with no assigned node. Here the
    /// request has the node resolved via `least_loaded_node` (not stored
    /// on `UnsentRequest`), so the in-flight survives onClose; the
    /// final poll respond-and-drain empties the queue.
    #[tokio::test(flavor = "current_thread")]
    async fn test_poll_with_on_close() {
        let time = Arc::new(AtomicI64::new(1));
        let (mut ncd, _meta, _rx) = new_delegate(Arc::clone(&time), false);

        let req = new_unsent_find_coordinator_request();
        ncd.add(req, time.load(Ordering::SeqCst));

        // First poll (on_close=false): request dispatches successfully.
        ncd.poll(0, time.load(Ordering::SeqCst), false).await;
        assert!(ncd.has_any_pending_requests(), "in-flight after dispatch");

        // Poll on close: the in-flight has no `node` on `UnsentRequest`
        // (it was resolved via least_loaded_node inside do_send, never
        // written back), so `check_disconnects` does not affect the
        // in-flight. The MockClient retains the in-flight request.
        ncd.poll_on_close(0, time.load(Ordering::SeqCst)).await;
        assert!(ncd.has_any_pending_requests(), "still pending after on-close poll");

        // Respond to the in-flight request (Java: `client.respond(...)`),
        // then poll-on-close again to drain.
        let response = FindCoordinatorResponse::prepare_response(Errors::None, GROUP_ID, &mock_node());
        ncd.client.respond(ConcreteResponse::FindCoordinator(response));
        ncd.poll_on_close(0, time.load(Ordering::SeqCst)).await;
        assert!(!ncd.has_any_pending_requests(), "drained after response");
    }

    /// Translated from `NetworkClientDelegateTest.testCheckDisconnectsWithOnClose`.
    /// The `node == None && on_close` branch in `check_disconnects`:
    /// requests that were never sent (because the only node was
    /// unreachable) are removed and completed with `NetworkException`
    /// when the delegate polls on close.
    #[tokio::test(flavor = "current_thread")]
    async fn test_check_disconnects_with_on_close() {
        let time = Arc::new(AtomicI64::new(1));
        let (mut ncd, _meta, _rx) = new_delegate(Arc::clone(&time), false);

        let mut req = new_unsent_find_coordinator_request();
        let mut rx = req.take_response_receiver().expect("receiver still present");
        ncd.add(req, time.load(Ordering::SeqCst));

        // Mark the sole node unreachable so `do_send` cannot succeed.
        // `set_unreachable` also calls `disconnect_node`, which would
        // surface as `connection_failed` on a subsequent poll once
        // `backing_off_until_ms` is set by `ready()`.
        ncd.client.set_unreachable(&mock_node(), REQUEST_TIMEOUT_MS as i64);

        // Poll with on_close = false: do_send fails (unreachable), so
        // the request stays in the unsent queue.
        ncd.poll(0, time.load(Ordering::SeqCst), false).await;
        assert!(ncd.has_any_pending_requests());

        // Poll with on_close = true: `check_disconnects` matches
        // `None if on_close` (the unsent never had a node assigned to
        // its `UnsentRequest` field) and fires
        // `on_failure(Error::new(Errors::NetworkError))`.
        ncd.poll_on_close(0, time.load(Ordering::SeqCst)).await;
        assert!(!ncd.has_any_pending_requests(), "unsent dropped on close");

        let received = rx.try_recv().expect("response delivered");
        match received {
            // Java surfaces a `DisconnectException` here.
            Err(err) => assert_eq!(Errors::NetworkError, err.error(), "expected a network error, got: {err}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }
}
