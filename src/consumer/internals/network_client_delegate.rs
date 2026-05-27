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

//! `NetworkClientDelegate` — Phase 6 skeleton.
//!
//! This commit lands the [`PollResult`] / [`UnsentRequest`] /
//! [`FutureCompletionHandler`] types referenced by
//! [`super::request_manager::RequestManager`]. The full
//! [`NetworkClientDelegate`] struct (with `unsent_requests`, `poll`,
//! `add_all`, etc.) is filled in by Phase 6 (5/7).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.NetworkClientDelegate`.

// Phase 6 (4/7) lands these types so the `RequestManager` trait compiles;
// the full delegate impl follows in Phase 6 (5/7).
#![allow(dead_code)]

use std::fmt;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::client_response::ClientResponse;
use crate::common::requests::RequestBuilder;
use crate::common::{KafkaError, Node};

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
        Self { time_until_next_poll_ms, unsent_requests: Vec::new() }
    }

    /// A result carrying the given requests and `WAIT_FOREVER` wait time.
    ///
    /// Java: `new PollResult(List<UnsentRequest>)`.
    pub(crate) fn with_requests(unsent_requests: Vec<UnsentRequest>) -> Self {
        Self { time_until_next_poll_ms: Self::WAIT_FOREVER, unsent_requests }
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
        Self { time_until_next_poll_ms, unsent_requests }
    }
}

impl fmt::Debug for PollResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollResult")
            .field("time_until_next_poll_ms", &self.time_until_next_poll_ms)
            .field("unsent_requests.len", &self.unsent_requests.len())
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
    request_builder: Box<dyn RequestBuilder>,
    handler: FutureCompletionHandler,
    /// Receiver paired with [`Self::handler`]. The bg task awaits this
    /// (after dispatching the request) to learn when the response or
    /// failure arrives. `Option<...>` so that callers wiring up a
    /// `whenComplete`-equivalent can `take()` it (Phase 6 (6/7) does this
    /// inside [`super::coordinator_request_manager::CoordinatorRequestManager`]).
    response_rx: Option<oneshot::Receiver<Result<ClientResponse, KafkaError>>>,
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
            request_builder,
            handler,
            response_rx: Some(rx),
            node,
            deadline_ms: -1,
            enqueue_time_ms: -1,
        }
    }

    /// Takes the response receiver (if not already taken). Mirrors the
    /// Java pattern of registering a `whenComplete` callback on the
    /// `handler.future()`: the manager / bg task takes ownership of the
    /// receiver so it can `.await` the completion.
    pub(crate) fn take_response_receiver(&mut self) -> Option<oneshot::Receiver<Result<ClientResponse, KafkaError>>> {
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

    /// Returns a reference to the request builder.
    ///
    /// Java: `requestBuilder()`.
    pub(crate) fn request_builder(&self) -> &dyn RequestBuilder {
        self.request_builder.as_ref()
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
        f.debug_struct("UnsentRequest")
            .field("api_key", &self.request_builder.api_key().name())
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
    sender: Mutex<Option<oneshot::Sender<Result<ClientResponse, KafkaError>>>>,
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
    pub(crate) fn new_with_receiver() -> (Self, oneshot::Receiver<Result<ClientResponse, KafkaError>>) {
        let (tx, rx) = oneshot::channel();
        let inner = Arc::new(FutureCompletionInner { sender: Mutex::new(Some(tx)), completion_time_ms: Mutex::new(0) });
        (Self { inner }, rx)
    }

    /// Java: `onFailure(long currentTimeMs, RuntimeException e)`. Records
    /// the completion time and completes the receiver with `Err(error)`.
    /// Idempotent — only the first call wins.
    pub(crate) fn on_failure(&self, current_time_ms: i64, error: KafkaError) {
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
    pub(crate) fn on_complete(&self, response: ClientResponse) {
        let completion_time_ms = response.received_time_ms();
        if let Some(msg) = response.authentication_error() {
            self.on_failure(
                completion_time_ms,
                KafkaError::with_message(crate::common::protocol::Errors::SaslAuthenticationFailed, msg.to_string()),
            );
            return;
        }
        if response.was_disconnected() {
            self.on_failure(
                completion_time_ms,
                KafkaError::new(crate::common::protocol::Errors::NetworkException),
            );
            return;
        }
        if let Some(msg) = response.version_mismatch() {
            self.on_failure(completion_time_ms, KafkaError::unsupported_version(msg.to_string()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::{MetadataRequestBuilder, RequestBuilder};

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

    #[test]
    fn future_completion_handler_idempotent_send() {
        let (handle, rx) = FutureCompletionHandler::new_with_receiver();
        assert!(!handle.is_done());

        handle.on_failure(123, KafkaError::timeout("boom"));
        assert!(handle.is_done());
        assert_eq!(handle.completion_time_ms(), 123);

        // Second call must not double-send; matches Java's
        // CompletableFuture.completeExceptionally idempotence.
        handle.on_failure(456, KafkaError::timeout("again"));

        // Receiver resolves with the *first* error only (the second
        // send into the consumed sender slot is dropped).
        let received = rx.blocking_recv().expect("sender alive until first send");
        match received {
            Err(KafkaError::Timeout(msg)) => assert_eq!(msg, "boom"),
            Err(other) => panic!("expected timeout error, got: {other}"),
            Ok(_) => panic!("expected error variant, got Ok"),
        }
    }
}
