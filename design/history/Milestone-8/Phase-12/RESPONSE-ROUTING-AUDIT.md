# Phase 12 — Response-routing audit across all 6 consumer RequestManagers

Audit of how each Rust `RequestManager` routes the broker response for the
`UnsentRequest`s it emits in `PollResult::unsent_requests`. The Java contract
attaches a `whenComplete((response, throwable) -> ...)` callback to every
`UnsentRequest` it returns; the Rust translation chose a
`take_response_receiver()` → `tokio::spawn(forwarder)` pattern. This audit
records, for each of the six RMs, whether that pattern is actually wired in
production code.

## Reference points (verified)

- `network_client_delegate.rs:171-181` — `UnsentRequest::new` unconditionally
  builds an `(FutureCompletionHandler, oneshot::Receiver)` pair and stores
  the receiver in `response_rx: Some(rx)`. No `new_with_receiver`-style
  alternative is exposed to callers; the receiver is created **every time**.
  Production callsites of `UnsentRequest::new` therefore must call
  `take_response_receiver()` to obtain the receiver, or the receiver is
  dropped when the `UnsentRequest` itself is consumed by `do_send`.
- `network_client_delegate.rs:196-198` — `take_response_receiver()` is the
  only way to extract the receiver:
  `` `self.response_rx.take()` `` returning `Option<...>`. Idempotent.
- `network_client_delegate.rs:732-735` — `do_send` attaches a
  `RequestCompletionHandler` that calls `handler.on_complete_ref(response)`
  when the broker reply arrives. The handler's
  `Arc<Mutex<Option<oneshot::Sender>>>` then fires
  `tx.send(Ok(response))` (line 353); the `let _ =` silently swallows the
  `SendError` when the receiver has been dropped.
- `consumer_network_thread.rs::run_once` (lines 325-700): grep returns
  **zero** `take_response_receiver` calls in this file. The bg-task neither
  takes nor routes the receivers itself; it relies on each RM having
  attached its own forwarder before returning the `UnsentRequest`.

The canonical WIRED pattern (used by `commit_request_manager.rs` and
`offsets_request_manager.rs`) is therefore:

```rust
let mut unsent = UnsentRequest::new(Box::new(builder), Some(node));
let response_rx = unsent.take_response_receiver().expect("receiver fresh");
let state_for_handler = Arc::clone(&self.shared); // or Arc<...Inner>
tokio::spawn(async move {
    match response_rx.await {
        Ok(Ok(client_response)) => handle_success(state_for_handler, client_response),
        Ok(Err(err))            => handle_failure(state_for_handler, err),
        Err(_recv)              => handle_failure(state_for_handler, NetworkException),
    }
});
unsent
```

The pattern requires the RM to either (a) own its mutable state via
`Arc<...Inner>` interior-mutability (commit RM does this) or (b) own an
`mpsc::Sender<PendingCompletion>` so the forwarder posts back to the RM's
own `&mut self` drain loop (offsets RM does this).

---

## 1. `coordinator_request_manager.rs`

- **Request build site**: `coordinator_request_manager.rs:240` —
  ``UnsentRequest::new(builder, None)`` — bare constructor, no receiver
  extraction.
- **`take_response_receiver` call**: NONE in production code. Only
  appearances are:
    - `coordinator_request_manager.rs:230` (rustdoc claim, no code)
    - `coordinator_request_manager.rs:336` (test helper
      `expect_find_coordinator_request` — drives the path manually).
- **Response await site**: NONE. The receiver returned from the
  `UnsentRequest::new` call at line 240 is never extracted; when
  `do_send` consumes the `UnsentRequest`, the receiver is dropped.
- **State-update on response**: `on_response(now_ms, &FindCoordinatorResponse)`
  (line 166) and `on_failed_response(now_ms, KafkaError)` (line 203). These
  methods exist and update `self.coordinator`, `self.fatal_error`,
  `self.request_state` — but are only invoked from tests
  (`coordinator_request_manager.rs:345`). No production callsite.
- **Java equivalent**:
  `kafka/clients/.../CoordinatorRequestManager.java:113-132` —
  `makeFindCoordinatorRequest(currentTimeMs)` builds the `UnsentRequest`
  and on the same return statement calls
  `` `return unsentRequest.whenComplete((clientResponse, throwable) -> { ... onResponse(...) / onFailedResponse(...) })` ``
  (line 123).
- **Verdict**: **BROKEN**.
- **Test coverage**: Unit tests (`test_successful_response` at line 352,
  `test_on_failed_response_timeout_is_retriable_not_fatal` at line 414)
  call `manager.on_response(...)` directly via the `expect_find_coordinator_request`
  helper — they bypass the bg-task path. **No test exercises the
  production response-routing path.**
- **Behavioral impact if BROKEN**: After `FindCoordinator` is sent, the
  response is delivered into a dropped receiver. The
  `CoordinatorRequestManager::coordinator` field stays `None` forever;
  `poll()` keeps returning a `FindCoordinator` request every backoff
  period (line 256-264 only short-circuits when `coordinator.is_some()`).
  The consumer is stuck in JOINING because the membership manager cannot
  send `ConsumerGroupHeartbeat` to an unknown coordinator. Confirmed by
  the four integration tests at `tests/integration/consumer_test.rs:213,
  295, 339, 418` all marked `#[ignore]` blaming this exact gap.

---

## 2. `consumer_heartbeat_request_manager.rs`

- **Request build site**: `consumer_heartbeat_request_manager.rs:278` —
  ``UnsentRequest::new(builder, node)`` inside `build_heartbeat_request`
  (lines 268-279). Called from three sites: `poll` line 421 (poll-timer
  expired leave path), `poll` line 443 (normal heartbeat), and
  `poll_on_close` line 458 (close-time leave).
- **`take_response_receiver` call**: NONE — grep against this file
  returns zero hits.
- **Response await site**: NONE. The receiver inside the `UnsentRequest`
  returned at line 278 is silently dropped when `do_send` consumes the
  request.
- **State-update on response**: `abstract_heartbeat_request_manager.rs:293`
  defines `on_successful_response(new_heartbeat_interval_ms, now)` which
  updates `heartbeat_request_state` interval and resets backoff. Java's
  `onResponse` also calls `membershipManager.onHeartbeatSuccess(response)`
  to advance the KIP-848 state machine. `on_failure` at
  `abstract_heartbeat_request_manager.rs:304` updates the backoff state.
  Neither is reachable from the bg task — they are reached only from
  unit tests.
- **Java equivalent**:
  `kafka/clients/.../AbstractHeartbeatRequestManager.java:292-310` —
  `pollInternal` calls `buildHeartbeatRequest()`, then on the same
  return statement attaches
  `` `request.whenComplete((response, exception) -> { if (response != null) onResponse(...) else onFailure(...) })` ``
  (line 296 for non-ignoreResponse path, line 309 for the
  ignoreResponse-true leave-group path that still records completion
  time).
- **Verdict**: **BROKEN**.
- **Test coverage**: Unit tests like `heartbeat_on_startup`,
  `heartbeat_outside_interval`, `timer_not_due` exercise the **request
  emission** side but **not** the response dispatch — they verify a
  `PollResult` was returned, not that an arrival drives `on_successful_response`.
  No test exists where (a) a synthesised `ClientResponse` is fed to the
  handler and (b) the manager's `heartbeat_request_state` /
  `membership_manager` state is observed to advance.
- **Behavioral impact if BROKEN**: Even after `FindCoordinator` succeeds
  (e.g. by hand-completing in a test), the membership state machine
  never advances past `JOINING`. The Heartbeat response carrying the
  assignment is dropped, so `MemberState` never reaches `STABLE`.
  Symptom: `consumer.poll()` returns no records, then after
  `session.timeout.ms` (default 45s) the broker fences the member
  because heartbeats are sent but never reset their backoff timer based
  on the broker-supplied interval.

---

## 3. `commit_request_manager.rs` — CANONICAL WIRED RM

- **Request build sites**:
    - `commit_request_manager.rs:1413` — OffsetCommit:
      ``UnsentRequest::new(Box::new(builder), coordinator_node)``
    - `commit_request_manager.rs:1479` — OffsetFetch:
      ``UnsentRequest::new(Box::new(builder), coordinator_node)``
- **`take_response_receiver` calls**:
    - `commit_request_manager.rs:1414` —
      ``let response_rx = unsent.take_response_receiver().expect("receiver fresh");``
    - `commit_request_manager.rs:1480` — same shape.
- **Response await sites**:
    - `commit_request_manager.rs:1416-1428` — `tokio::spawn` awaits
      `response_rx`, then dispatches into
      `handle_offset_commit_response(&inner_for_handler, request, body)`
      on success, or `request.complete_err(err)` on failure / receiver
      drop.
    - `commit_request_manager.rs:1490-1525` — same pattern for OffsetFetch;
      additionally drains the matching entry from
      `inflight_offset_fetches` (lines 1516-1524) inside the spawned
      task, closing a Phase-9 leak.
- **State-update on response**:
    - `handle_offset_commit_response` (line 1533) → routes the
      `OffsetCommitResponse` to `classify_and_complete_commit` (line
      1548) which writes errors back through
      `request.complete_err(...)` / completes the inner future.
    - `handle_offset_fetch_response` writes through `future_tx` on the
      request state.
- **Java equivalent**:
  `kafka/clients/.../CommitRequestManager.java` — `buildOffsetCommitRequest`
  and `buildOffsetFetchRequest` both attach `whenComplete` on the
  returned `UnsentRequest`. Pattern parity is exact.
- **Verdict**: **WIRED**.
- **Test coverage**: Translation-level unit tests in
  `commit_request_manager.rs` exercise the spawned-task path by driving
  `unsent.handler().on_complete(client_response)` directly (search for
  `on_complete` / `pending_completions` in this file). The Phase 12
  smoke-test gate (`run_once_drives_commit_poll_with_coordinator` at
  `consumer_network_thread.rs:1888`) verifies the **request enqueue**
  side — not the response side — but the in-module unit tests cover
  the dispatch.
- **Behavioral impact**: N/A — wired correctly.

---

## 4. `fetch_request_manager.rs`

- **Request build site**: `fetch_request_manager.rs:266` —
  ``UnsentRequest::new(Box::new(builder), Some(target_node))`` inside
  the per-node loop of `poll_internal`.
- **`take_response_receiver` call**: NONE in production. The only
  appearance is the comment at `fetch_request_manager.rs:267-270`:
  ``// Phase 10 wires `whenComplete` here — the bg task awaits
  // `unsent.take_response_receiver()` and dispatches into
  // `on_fetch_response` / `on_fetch_failure`. Phase 7b just
  // returns the request.`` — i.e. the author at Phase 7b explicitly
  flagged the deferral, Phase 10 never landed it.
- **Response await site**: NONE. The receiver is dropped when `do_send`
  consumes the per-node `UnsentRequest`.
- **State-update on response**: Methods exist —
    - `fetch_request_manager.rs:166` `on_fetch_response(fetch_target,
      request_data, response, request_version)` → calls
      `AbstractFetch::handle_fetch_success` (`abstract_fetch.rs:299`),
      which appends to `FetchBuffer` so `FetchCollector` can later
      yield `ConsumerRecords`.
    - `fetch_request_manager.rs:180` `on_fetch_failure` → calls
      `AbstractFetch::handle_fetch_failure` (`abstract_fetch.rs:424`)
      which updates session-handler state and records metrics.

  Neither method is reachable from the bg task in production.
- **Java equivalent**:
  `kafka/clients/.../FetchRequestManager.java:156-168` — each per-node
  `UnsentRequest` has `whenComplete(responseHandler)` chained on the
  same statement, where `responseHandler` dispatches to
  `successHandler.handle(...)` / `errorHandler.handle(...)` — bound by
  `AbstractFetch.java` to the success/failure handlers above.
- **Verdict**: **BROKEN**.
- **Test coverage**: Unit tests (lines 382-484) exercise `pending_fetch_requests`
  ack semantics, empty-partition short-circuit, drop-cancellation, and
  `signal_close` — but **none** synthesises a `FetchResponse`, drives
  `handler.on_complete(...)`, and asserts that `FetchBuffer` received
  the records. Integration test `test_assign_partitions_and_poll`
  (`tests/integration/consumer_test.rs:295`) `#[ignore]`d for exactly
  this reason.
- **Behavioral impact if BROKEN**: `consumer.poll()` returns
  `ConsumerRecords::empty()` indefinitely. Even with FindCoordinator
  and Heartbeat fixed by hand, no records ever flow because
  `FetchBuffer` is never populated. The integration test
  `test_assign_partitions_and_poll` skips coordinator entirely (uses
  `assign`, not `subscribe`) and STILL fails on this gap.

---

## 5. `offsets_request_manager.rs`

- **Request build sites** (three):
    - `offsets_request_manager.rs:326` (in `build_list_offsets_requests`,
      the `fetch_offsets` path) —
      ``UnsentRequest::new(Box::new(builder), Some(node))``
    - `offsets_request_manager.rs:852` (in the reset-position path,
      `reset_positions_if_needed`) — same.
    - `offsets_request_manager.rs:924` (in
      `send_offsets_for_leader_epoch_requests_and_validate_positions`)
      — same.
- **`take_response_receiver` calls**:
    - `offsets_request_manager.rs:327` — paired with build site 326.
    - `offsets_request_manager.rs:854` — paired with 852.
    - `offsets_request_manager.rs:925` — paired with 924.
- **Response await sites**:
    - `offsets_request_manager.rs:331-341` — `tokio::spawn` forwarder
      posts `PendingCompletion::ListOffsetsForFetchOffsets { state,
      node_partitions, result }` onto `pending_completions_tx` (the
      manager's mpsc channel).
    - `offsets_request_manager.rs:858-868` — forwarder posts
      `PendingCompletion::ListOffsetsForReset`.
    - `offsets_request_manager.rs:928-934` — forwarder posts
      `PendingCompletion::OffsetsForLeaderEpoch`.
    - All three are drained by
      `drain_pending_completions(current_time_ms)` at line 1369,
      called from `RequestManager::poll` at line 1637.
- **State-update on response**: `drain_pending_completions` dispatches
  to `apply_partial_result` (line 396), `handle_list_offsets_for_reset`,
  and `handle_offsets_for_leader_epoch`, all of which write through
  `subscription_state`, `requests_to_retry`, `cached_update_positions_exception`,
  and the waiter fan-out.
- **Java equivalent**:
  `kafka/clients/.../OffsetsRequestManager.java:620-632, 729-758` —
  each `UnsentRequest` has `whenComplete((response, error) -> { ... })`
  attached on construction.
- **Verdict**: **WIRED**.
- **Test coverage**: Extensive — the per-test driver
  `complete_first_unsent_with_response` (line 2628) feeds a synthetic
  `ListOffsetsResponse` into `unsent.handler().on_complete(...)`, yields
  the runtime so the spawned forwarder posts the `PendingCompletion`,
  then re-polls the manager so `drain_pending_completions` routes the
  result. Specifically `fetch_offsets_success_single_partition` (line
  2697) and follow-on tests exercise all three response paths end-to-end.
- **Behavioral impact**: N/A — wired correctly.

---

## 6. `topic_metadata_request_manager.rs`

- **Request build site**: `topic_metadata_request_manager.rs:344` —
  ``unsent.push(UnsentRequest::new(builder, None));`` inside
  `RequestManager::poll`. No receiver extraction at this site.
- **`take_response_receiver` call**: only **one**, at
  `topic_metadata_request_manager.rs:896` — and it is inside a TEST
  (`test_client_response_route` at line 872). No production call.
- **Response await site**: NONE in production. The rustdoc at lines
  184-199 (`on_response` doc) describes the intended bg-task wire-up
  using the same template language as the coordinator RM's
  misrepresenting rustdoc — it says "Phase 10 wires this from the bg
  task" but Phase 10 did not.
- **State-update on response**: `on_response` (line 200) → drains the
  matching `inflight_requests` entry and completes its `ack: oneshot`
  via `state.complete(Ok(partition_infos))`. `on_failure` (line 227)
  applies retry backoff or completes with the error. Both exist as
  callable methods but no production codepath invokes them.
- **Java equivalent**:
  `kafka/clients/.../TopicMetadataRequestManager.java:198-225` —
  `createUnsentRequest(request)` builds the `UnsentRequest` and calls
  `` `unsent.whenComplete((response, exception) -> { ... handleResponse(response) / handleError(exception, completionTimeMs) })` ``
  at line 204.
- **Verdict**: **BROKEN**.
- **Test coverage**: One unit test, `test_client_response_route` (line
  872-905), demonstrates the wire-up by manually calling
  `unsent.handler().on_complete(response)` then `manager.on_response(...)`
  — i.e. **the test exists for the harness pattern, but no production
  call ever pairs the receiver with the manager.**
- **Behavioral impact if BROKEN**: `consumer.partitions_for(topic).await`
  and `consumer.list_topics().await` hang forever. The
  `TopicMetadataApplicationEvent` handler enqueues a metadata-fetch
  request whose `oneshot::Receiver` (the `ack` field on
  `TopicMetadataRequestState`) is never completed because the
  inflight entry is never removed from the queue. Inflight requests
  accumulate indefinitely; each `poll()` iteration re-sends them after
  the backoff, accumulating duplicate requests on the broker.

---

## Summary

### 1. Verdict table

| RM | Verdict | Rust build line | Rust receiver-take | Test coverage Y/N |
|---|---|---|---|---|
| `coordinator_request_manager` | **BROKEN** | 240 | NONE (only line 230 rustdoc) | N |
| `consumer_heartbeat_request_manager` | **BROKEN** | 278 | NONE | N |
| `commit_request_manager` | **WIRED** | 1413, 1479 | 1414, 1480 | Y |
| `fetch_request_manager` | **BROKEN** | 266 | NONE (only line 268 comment) | N |
| `offsets_request_manager` | **WIRED** | 326, 852, 924 | 327, 854, 925 | Y |
| `topic_metadata_request_manager` | **BROKEN** | 344 | NONE (only line 896 in TEST) | N |

Count: **4 BROKEN / 2 WIRED.**

### 2. Canonical WIRED pattern (extracted from `commit_request_manager.rs:1410-1429`)

```rust
// Build the unsent request, register a completion handler that
// dispatches the response into the request state's `future_tx`.
let coordinator_node = inner.coordinator_node();
let mut unsent = UnsentRequest::new(Box::new(builder), coordinator_node);
let response_rx = unsent.take_response_receiver().expect("receiver fresh");
let inner_for_handler = Arc::clone(inner);
tokio::spawn(async move {
    match response_rx.await {
        Ok(Ok(mut client_response)) => {
            handle_offset_commit_response(&inner_for_handler, request, client_response.take_response_body());
        },
        Ok(Err(err)) => {
            request.complete_err(err);
        },
        Err(_recv_err) => {
            request.complete_err(KafkaError::new(Errors::NetworkException));
        },
    }
});
unsent
```

Key invariants:

- `take_response_receiver()` is called on the **same** `UnsentRequest`
  that is about to be moved into the `PollResult`. Calling it later
  (after `PollResult` construction) is impossible because the receiver
  is `&mut self`-only and the request has been moved.
- `Arc::clone` of an `*Inner` (interior-mutability) — or `Arc::clone`
  on an `mpsc::Sender<PendingCompletion>` (channel-back pattern used
  by `offsets_request_manager`) — is what lets the spawned forwarder
  reach the RM's state without aliasing the `&mut self` that `poll`
  holds.
- All three terminal arms — `Ok(Ok(...))`, `Ok(Err(err))`,
  `Err(_recv_err)` — write **some** completion back, never silently
  dropping the request (CLAUDE.md §5).

### 3. Per-RM fix recipe (BROKEN ones)

#### `coordinator_request_manager` — ~40 LOC

1. Refactor `CoordinatorRequestManager` to interior mutability:
   wrap its mutable state (`coordinator`, `fatal_error`, `request_state`,
   `time_marked_unknown_ms`, `total_disconnected_min`) in a
   `CoordinatorRequestManagerInner` and access via
   `Arc<Mutex<CoordinatorRequestManagerInner>>`. Mirrors
   `CommitRequestManagerInner`.
2. Modify `make_find_coordinator_request` to take `&Arc<Mutex<Inner>>`
   instead of `&mut self`, build the request, call
   `take_response_receiver`, and `tokio::spawn` a forwarder that calls
   `on_response` / `on_failed_response` on the inner state.
3. Delete the misleading rustdoc at lines 226-233; rewrite to describe
   the actual wire-up.
4. Estimate: ~40 LOC + small test that synthesises a
   `FindCoordinatorResponse` via `handler.on_complete(...)` and asserts
   the coordinator field is populated after a yield.

#### `consumer_heartbeat_request_manager` — ~50 LOC

1. The heartbeat RM already wraps state in `AbstractHeartbeatRequestManager`
   (the `inner` field, line 358); on first inspection the existing
   structure does NOT use `Arc<Mutex<...>>` for `inner` — it holds
   `inner` directly. Add `Arc<Mutex<AbstractHeartbeatRequestManager>>`
   (or `Arc<...Inner>`) so the spawned forwarder can reach it.
   Alternatively follow the `offsets_request_manager` channel-back
   pattern: forward `PendingCompletion::Heartbeat { response }` onto
   an `mpsc::UnboundedSender` and drain in `poll`.
2. Refactor `build_heartbeat_request` (line 268) to call
   `take_response_receiver` and spawn a forwarder.
3. The forwarder must call **both** `on_successful_response` /
   `on_failure` on the abstract base AND
   `membership_manager.on_heartbeat_success` / `on_heartbeat_failure`
   so the KIP-848 state machine advances. This is the load-bearing
   side-effect.
4. Estimate: ~50 LOC + a regression test that drives a synthesised
   `ConsumerGroupHeartbeatResponse` and observes
   `membership_manager.state()` advancing past `JOINING`.

#### `fetch_request_manager` — ~70 LOC

1. Two callsites in `poll_internal`: line 266 (normal fetch) and a
   `pollOnClose` analog (the `for_close=true` branch). Both go through
   the same per-node loop, so the fix is a single per-node
   `take_response_receiver` + `tokio::spawn` block inside the loop.
2. The spawned forwarder needs access to `AbstractFetch` to call
   `handle_fetch_success` / `handle_fetch_failure`. `AbstractFetch`
   today is owned `&mut self` on the manager; the cleanest fix is the
   `offsets_request_manager` channel-back pattern: spawn a forwarder
   that sends `PendingFetchCompletion { fetch_target, request_data,
   response_or_error }` on an `mpsc::UnboundedSender`, and drain in
   `RequestManager::poll`. This keeps `&mut AbstractFetch` access on
   the single bg-task.
3. Note that `FetchSessionRequestData` must be `Clone`-able to be
   captured into the spawned task; verify (likely already is).
4. Estimate: ~70 LOC + a regression test that synthesises a
   `FetchResponse` for a single TP and asserts `FetchBuffer` receives
   the `CompletedFetch`.

#### `topic_metadata_request_manager` — ~40 LOC

1. The `inflight_requests: Vec<TopicMetadataRequestState>` already
   gives a per-request handle; the existing test at line 872 shows
   the exact handshake. Production wiring needs the per-request
   `request_id` to be captured into the spawned forwarder, then
   `on_response(request_id, ...)` / `on_failure(request_id, ...)`
   called via channel-back into the next `poll`.
2. Adopt either:
    - `Arc<Mutex<Self>>` so the forwarder calls `on_response` directly
      (simple), or
    - `mpsc::UnboundedSender<PendingMetadataCompletion>` drained in
      `poll` (consistent with offsets/fetch).
3. Estimate: ~40 LOC + un-ignore for any `partitions_for` /
   `list_topics` test path.

**Total estimate**: ~200 LOC across the four RMs, plus ~80 LOC of
test additions, plus the small structural decision (interior mutability
vs. channel-back) per RM. Mechanical work, but each RM is its own
context — there is no single "fix it once" lever.

### 4. Risk matrix (user-visible bugs)

| RM | User-visible bug |
|---|---|
| `coordinator_request_manager` | Consumer stuck in JOINING forever — every `subscribe()`-based test hangs at the FindCoordinator step. Confirmed by integration tests. |
| `consumer_heartbeat_request_manager` | Even if FindCoordinator succeeds, membership state machine never leaves JOINING → fenced after `session.timeout.ms` (~45s default). No assignment delivered, so no records polled. |
| `fetch_request_manager` | `consumer.poll(timeout)` returns `ConsumerRecords::empty()` indefinitely. `FetchBuffer` never populated. The only RM whose breakage is independent of group state — `assign()`-based code paths still hit this. |
| `offsets_request_manager` | N/A (WIRED). |
| `topic_metadata_request_manager` | `consumer.partitions_for(topic).await` and `consumer.list_topics().await` hang forever; inflight metadata requests accumulate and re-send. |
| `commit_request_manager` | N/A (WIRED). |

Note on the previously-claimed "`commit_sync` hangs" — `commit_sync` is
NOT broken in isolation because the **commit** RM is wired; however
`commit_sync` typically requires a discovered coordinator, so it hangs
**transitively** through the coordinator gap.

### 5. Phase-10 PLAN.md claim vs. shipped code

Phase-10 PLAN.md scope statement (lines 231-284) does NOT enumerate
per-RM response routing as a deliverable. The `whenComplete` translation
notes in PLAN.md are scoped to:

- `AsyncPollEvent` (PLAN.md:239-249) — "Java chains
  `updateFetchPositions` → `markValidatePositionsComplete` →
  `createFetchRequests` → `event.completeSuccessfully()` via
  `CompletableFuture::whenComplete`. Rust translates the chain to a
  detached `tokio::spawn` ...". This is the **AsyncPoll event handler**,
  not per-RM response routing.
- `UnsubscribeEvent` (PLAN.md:257-259) — "Mirrors Java's
  `whenComplete(complete(event.future()))` pattern."
- `LeaveGroupOnCloseEvent` (PLAN.md:265-267) — spawns the leave-group
  future, completes the handle on resolution.

PLAN.md never makes a commitment of the form "Phase 10 will wire
FindCoordinator / Heartbeat / Fetch / Metadata response routing
through `take_response_receiver`." Grepping for "FindCoordinator",
"Heartbeat response", "Metadata response" in
`design/history/Milestone-8/Phase-10/PLAN.md` returns zero hits.

**The shipped code matches the PLAN.md commitment.** Two RMs (commit,
offsets) were wired in earlier phases — Phase 9 for commit, Phase 7b/8
for offsets — because they had a localised reason to spawn forwarders
(commit-state retry chain, list-offsets multi-node fan-in). The other
four were structurally deferred to "Phase 10's bg-task wiring" by inline
comments at the build sites (line 230 coordinator, lines 188-192
topic_metadata, lines 267-270 fetch). Phase 10 did the bg-task spawn
+ `add_all_from_poll_result` + `network_client.poll` plumbing —
correctly — but **never added the per-RM forwarders the inline comments
predicted**. The gap is structural, not regression: Phase 10 did exactly
what PLAN.md said; PLAN.md just did not say enough.

### 6. Phase-11 test coverage — how do tests pass without exercising response routing?

`tests/consumer/async_kafka_consumer_test.rs` contains exactly two
tests (`wc -l` 116 lines total):

1. `new_consumer_builds_and_closes_against_refused_broker` (line 74) —
   ctor + immediate `close()` against a refused TCP socket. Never sends
   a request that can complete. The close path's `LeaveGroupOnClose`
   times out after `request.timeout.ms` (Issue 9 in COMMENTS.1.md) but
   the test asserts only that the ctor and close return — not that any
   response arrived. Exercise of response routing: **zero**.
2. `new_consumer_rejects_classic_group_protocol` (line 100) — verifies
   that `new_consumer(... group.protocol=classic ...)` returns
   `KafkaError::UnsupportedVersion` from the factory. No network call
   at all. Exercise of response routing: **zero**.

Phase-11 tests pass because they were intentionally scoped to NOT
require an end-to-end broker round-trip. The four integration tests in
`tests/integration/consumer_test.rs` that DO require a round-trip are
all `#[ignore]`d (lines 213, 295, 339, 418) blaming the response-routing
gap — which is exactly the gap this audit confirms.

No Phase-11 test is "suspicious" — they are tight ctor/teardown
assertions. The Phase-11 unit-style tests in the RM modules
(coordinator at line 352, heartbeat at lines 396+, fetch at 382+,
topic_metadata at 872) sidestep production routing by manually driving
`unsent.handler().on_complete(...)` then calling the manager's
`on_response`/`on_failure` directly. This pattern is correct for unit
tests but tells nothing about whether the bg task connects the two.

### 7. Bottom line

- **Total LOC estimate to fix all 4 BROKEN RMs**: ~200 LOC of
  production code + ~80 LOC of unit/regression tests = **~280 LOC**.
- **Confidence**: Medium-high on the estimate per RM, because two
  RMs already shipped the pattern (commit, offsets) and the per-RM
  refactor is mechanical. Medium on the structural-decision risk:
  whether each RM gets `Arc<...Inner>` interior mutability vs. an
  mpsc channel-back is per-RM and may surface secondary issues
  (e.g. heartbeat's `membership_manager` already holds locks that
  the forwarder will need to acquire — there is a §16-style risk of
  holding a `MutexGuard` across `.await`).
- **Recommended path**: **(b) defer to Phase 12.5**. Reasoning:
    1. The audit shows the gap is structural (carries over from Phase
       10's scope), not a Phase-12 regression. Phase 12 already shipped
       its DoD (commit poll wire-up, single notifier, shared atomic,
       max_time_to_wait, integration tests with `#[ignore]` markers
       pointing at the gap).
    2. The fix touches three more RMs in mechanical-but-non-trivial
       ways. Each needs its own design discussion about
       `Arc<Mutex<Inner>>` vs. mpsc channel-back, plus a regression
       test, plus un-ignoring an integration test. Bundling all four
       fixes into one phase keeps the architectural decision coherent
       (pick one pattern, apply uniformly), instead of re-litigating
       per RM as later phases find each new gap.
    3. Path (a) — extend Phase 12 — adds significant scope to a phase
       that is otherwise done; the Manager would have to re-open commits
       4-6 and re-run Critic rounds. Path (c) — re-open Phase 10 — is
       worse: rewriting history makes the next milestone harder to
       review.
    4. A standalone Phase 12.5 with a tight charter ("wire response
       routing for the four BROKEN RMs; un-ignore all four integration
       tests; demonstrate the canonical pattern's uniformity") sizes
       cleanly and keeps the Critic loop focused.

   Either (a) or (b) lands the same code. (b) keeps phase boundaries
   clean. The Phase-12 closeout should call this audit out by name
   so Phase 12.5 inherits a precise scope.
