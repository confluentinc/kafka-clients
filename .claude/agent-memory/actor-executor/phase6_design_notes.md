---
name: phase6-design-notes
description: Milestone-8 Phase 6 (NetworkClientDelegate, RequestManager, CoordinatorRequestManager, RequestManagers skeleton) — design decisions, tricky patterns, and Phase-7+ extension points.
metadata:
  type: project
---

# Phase 6 design notes

Milestone-8 Phase-6 lands `NetworkClientDelegate` + `RequestManager` + `CoordinatorRequestManager` + a skeleton `RequestManagers` container, plus the `FindCoordinator` request/response wrappers needed by the manager.

## Why: documents the non-obvious translation decisions that Phase 7+ (Fetch, Commit, Heartbeat) inherit. Records the patterns used so future managers stay consistent.

## How to apply: when extending `RequestManagers` (Phase 7-9), use `Option<...>` slots and update `entries()`. When wiring a manager's response callback, take `UnsentRequest::take_response_receiver()` and route through the manager's `on_response`-style method.

## Key patterns

### `FutureCompletionHandler` bridges sync callback to async receiver

The `RequestCompletionHandler` callback type (`Box<dyn FnOnce(&mut ClientResponse)>`) cannot consume the response — it gets `&mut ClientResponse`. The bridge to the async-style `oneshot::Receiver<Result<ClientResponse, KafkaError>>` is `FutureCompletionHandler::on_complete_ref(&mut response)`:
- Clones what's clonable (header, dest string, error messages).
- Takes (`mem::take`-style) the `response_body: Option<ConcreteResponse>` via the new `ClientResponse::take_response_body()`.
- Rebuilds an owned `ClientResponse` with callback=None.
- Dispatches to `on_complete(owned)` which handles auth/disconnect/version-mismatch and sends through the oneshot.

### `NetworkClientDelegate<K: KafkaClient + Send>` is generic, not boxed

`KafkaClient` has `impl Future` return types -> non-object-safe. Generics-only dispatch. Bg task (Phase 10) will hold `NetworkClient<...>`; tests use `MockClient`.

### `UnsentRequest` carries `Option<Box<dyn RequestBuilder>>`

`take_request_builder()` consumes the builder when dispatching. Reason: the builder is `!Default` so we can't leave a zombie value behind. Callers should not look at `request_builder()` after dispatch.

### `UnsentRequest::take_response_receiver()` for Phase 10 routing

Each `UnsentRequest::new` creates a paired `(handler, receiver)`. The receiver is held in `Option<oneshot::Receiver<...>>` inside the request. A manager that wants to react to completion (`CoordinatorRequestManager`) takes ownership of the receiver via `take_response_receiver()` before handing the request to `NetworkClientDelegate::add`. The bg task (Phase 10) will await it and route to `manager.on_response(now, &response)`.

### `RequestManager::poll` is sync `fn`, no `#[async_trait]`

Java contract is "no network I/O occurs in this method". Sync `fn` per DoD §11. `Send + 'static` bounds so `Box<dyn RequestManager>` upcasting works for `RequestManagers::entries() -> Vec<&mut dyn RequestManager>`.

### `MockClient` test edge case: `not_throttled(now)` is strict `>`

`MockConnectionState::not_throttled(now)` is `now > self.throttled_until_ms`. With `throttled_until_ms = 0` (default) and `now = 0`, this returns `false` and `MockClient::send` panics with "Cannot send ... destination not ready". Workaround in tests: start time at `1`, not `0`. Look for the pattern `Arc::new(AtomicI64::new(1))`.

### Empty group_id check replaces Java's `requireNonNull(groupId)`

Java throws NPE; Rust panics with "group_id must not be empty" — `assert!(!group_id.is_empty())` in `CoordinatorRequestManager::new`. Test uses `#[should_panic(expected = "...")]`.

### Phase 6 `RequestManagers` is a SKELETON

Only `coordinator: Option<CoordinatorRequestManager>` slot. Reserved-slot comments mark where Phase 7-9 plug in. `entries()` returns `Vec<&mut dyn RequestManager>`; when more fields land, switch to a field-by-field destructuring pattern for borrow-splitting.

### Out-of-scope items dropped from Java's NetworkClientDelegate

- `AsyncConsumerMetrics` parameter (no Rust metrics framework yet)
- `ClientTelemetrySender`, `Sensor` parameters
- `RequestManagers::supplier(...)` static factory (Phase 10)
- All Share/Streams variants

### Errors enum mapping for CoordinatorRequestManager

- `CoordinatorLoadInProgress` (14) -> retriable, backoff only
- `CoordinatorNotAvailable` (15) -> retriable
- `NotCoordinator` (16) -> retriable
- `GroupAuthorizationFailed` (30) -> fatal, store `KafkaError::group_authorization(group_id)`
- `NetworkException` (13) -> the Rust analog of Java's `DisconnectException`, used by `handle_coordinator_disconnect`

## Phase 7-9 extension checklist

For each new manager:
1. `pub(crate) mod <name>;` in `src/consumer/internals/mod.rs`.
2. Inline `#[cfg(test)] mod tests {...}` because everything is `pub(crate)`.
3. `impl RequestManager for <Manager>` — `fn poll(&mut self, now: i64) -> PollResult` is the contract.
4. Add an `Option<...>` (or non-optional) field to `RequestManagers` and update `entries()`.
5. Tests should drive `manager.poll(...)` -> handle the resulting `UnsentRequest` -> build a `ClientResponse` -> fire `unsent.handler().on_complete(response)` -> hand the response body to `manager.on_response(...)`. Mirrors the bg-task pipeline.

## Files added/modified

- `src/common/requests/find_coordinator_request.rs` (NEW, 261 LOC)
- `src/common/requests/find_coordinator_response.rs` (NEW, 251 LOC)
- `src/common/requests/abstract_request.rs` (added `FindCoordinator` variant)
- `src/common/requests/abstract_response.rs` (added `FindCoordinator` variant)
- `src/common/requests/mod.rs` (re-exports)
- `src/client_response.rs` (added `take_response_body`)
- `src/consumer/consumer_config.rs` (added accessors: `request_timeout_ms`, `retry_backoff_ms`, `retry_backoff_max_ms` + `with_*` setters)
- `src/consumer/internals/request_state.rs` (NEW, 230 LOC)
- `src/consumer/internals/timed_request_state.rs` (NEW, 230 LOC)
- `src/consumer/internals/request_manager.rs` (NEW, 110 LOC)
- `src/consumer/internals/network_client_delegate.rs` (NEW, 800 LOC including tests)
- `src/consumer/internals/coordinator_request_manager.rs` (NEW, 410 LOC)
- `src/consumer/internals/request_managers.rs` (NEW, 165 LOC)
- `src/consumer/internals/mod.rs` (5 new pub(crate) mod lines)

## Test counts (delta)

- Baseline: 1057 lib + 36 consumer integration
- After Phase 6: 1107 lib (+50) + 36 consumer integration (unchanged)
- After Phase 6 critic-fix round (N=1): 1113 lib (+6) — 5 new in NetworkClientDelegate
  (test_timeout_before_send, test_timeout_after_send, test_ensure_correct_completion_time_on_complete,
  test_poll_with_on_close, test_check_disconnects_with_on_close) + 1 new in
  CoordinatorRequestManager (test_on_failed_response_timeout_is_retriable_not_fatal).

## Critic-fix round lessons (N=1, COMMENTS.1.md #1-5)

1. **`KafkaError::Timeout::is_retriable()` was `false`** — because the variant
   has no embedded `Errors` code, `kafka_error().is_some_and(...)` returned
   `None`. Java's `TimeoutException extends RetriableException` is retriable.
   Fix: special-case `matches!(self, Self::Timeout(_)) || ...` in
   `src/common/kafka_error.rs:is_retriable()`. Without this fix, Phase 10
   would route timeouts as fatal errors through every request manager's
   `on_failed_response`. Phase 7+ must double-check the same shape on any
   new `KafkaError` variants that don't carry an `Errors` code.

2. **MockClient::set_unreachable behavior** — `set_unreachable(node, ms)`
   marks `unreachable_until_ms` AND calls `disconnect_node` (which moves
   in-flight requests to `responses` as disconnect responses). The
   subsequent `do_send` sees `state=Disconnected`, `is_unreachable=true`,
   so `ready()` sets `backing_off_until_ms = now + 100` and returns false.
   This makes `node_unavailable` true on the NEXT poll. Subtle ordering:
   first poll fails on unreachable; second poll fails on backoff.

3. **MockClient::poll timeout handling** — `MockClient::poll` checks
   in-flight `elapsed >= request_timeout_ms` and calls `disconnect_node`.
   That puts a `disconnected=true` `ClientResponse` on `responses`, which
   poll then drains, firing the callback. The callback's path is:
   `client.poll` → `response.on_complete()` → callback → `handler.on_complete_ref`
   → sees `was_disconnected=true` → `on_failure(NetworkException)`.

4. **Test-the-internal-state pattern for log-output tests** — Java tests
   that use `LogCaptureAppender` to assert warnings translate to direct
   internal-state assertions in Rust (since we have no equivalent log
   capture). Tests in the same file as the struct can read private
   fields directly — no `pub(crate)` relaxation needed.

5. **Deferred-test rationale comments** — DoD §3 requires explaining why
   each skipped Java test is skipped. The PLAN.md may say it, but the
   test module should also have a comment block listing the deferred
   test names with one-line reasons. Pattern for `RequestManagers`:
   inline-comment block at top of `mod tests` listing both
   `testMemberStateListenerRegistered` (Phase 10 supplier) and
   `testStreamMemberStateListenerRegistered` (Streams out of scope per §20).

6. **`prepare_response` vs `respond`** — `MockClient::prepare_response(...)`
   pushes onto `future_responses` which is consumed by the NEXT `send()`.
   `MockClient::respond(...)` takes from `requests` (already-sent) and
   pushes a `ClientResponse` onto `responses` (drained by `poll`).
   For tests where the request was sent in a prior `poll()`, use
   `respond` to deliver the response. For tests where the request hasn't
   been sent yet but you want to stage the response in advance, use
   `prepare_response`.

## Pre-existing `cargo doc --no-deps` failure (11 errors)

11 rustdoc errors exist on the baseline branch: `[FutureRecordMetadata]` (private item ref), and generated-code links like `Array[0]`. Phase 6 does not introduce additional errors. The plan's verification step "cargo doc --no-deps builds" is technically failing pre-existing; flag in self-review if reviewer asks.
