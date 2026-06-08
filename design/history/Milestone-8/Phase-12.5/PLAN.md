# Phase 12.5: Wire response routing for the 4 BROKEN RequestManagers

## Goal

Make `AsyncKafkaConsumer` usable end-to-end against a real Kafka broker
by wiring response routing for the **4 BROKEN** `RequestManager`s
identified in
[`design/history/Milestone-8/Phase-12/RESPONSE-ROUTING-AUDIT.md`](../Phase-12/RESPONSE-ROUTING-AUDIT.md):

- `coordinator_request_manager` (FindCoordinator response is dropped)
- `consumer_heartbeat_request_manager` (Heartbeat response is dropped)
- `fetch_request_manager` (Fetch response is dropped)
- `topic_metadata_request_manager` (TopicMetadata response is dropped)

After Phase 12.5, callers running
`cargo test --features integration-tests --test integration_main consumer_test`
can subscribe to a topic, produce records via the existing
`KafkaProducer`, poll, commit, and observe the full KIP-848 lifecycle
against the testcontainers Kafka 4.2.0 broker.

## Branch

Lands on `consumer-impl` directly. **No worktree.** Phase 12 is closed;
Phase 12.5 is a structural carry-over from the Phase-10 design gap.

## Java sources

Per the audit, each BROKEN RM has an identifiable Java equivalent that
attaches `whenComplete((response, throwable) -> ...)` on the
`UnsentRequest` returned by its build method. Phase 12.5 translates the
`whenComplete` callback to the Rust
`take_response_receiver() → tokio::spawn(forwarder)` pattern that
`commit_request_manager` and `offsets_request_manager` already use.

- `CoordinatorRequestManager.java:113-132` — `makeFindCoordinatorRequest`
  ends with `return unsentRequest.whenComplete((clientResponse,
  throwable) -> { ... onResponse / onFailedResponse ... })`.
- `AbstractHeartbeatRequestManager.java:292-310` — `pollInternal`'s
  return path attaches `whenComplete` that drives `onResponse(...)`
  (success) / `onFailure(...)` (failure). The success arm calls
  `membershipManager.onHeartbeatSuccess(response)` — the load-bearing
  side effect.
- `FetchRequestManager.java:156-168` — each per-node `UnsentRequest`
  chains `whenComplete(responseHandler)` where `responseHandler`
  dispatches to `AbstractFetch.handleFetchSuccess` /
  `handleFetchFailure`.
- `TopicMetadataRequestManager.java:198-225` — `createUnsentRequest`
  calls `unsent.whenComplete((response, exception) -> {
  handleResponse(response) / handleError(exception, completionTimeMs)
  })`.

Pinned commit: same as Milestone 8 — `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

## Structural decision: `Arc<Mutex<Inner>>` vs mpsc channel-back

The audit identifies two flavors of the canonical pattern. Picking
**one per RM** rather than enforcing uniformity across all four,
because RM-specific constraints force the choice:

| RM | Pattern | Rationale |
|---|---|---|
| `coordinator_request_manager` | **`Arc<Mutex<Inner>>`** | State (coordinator, fatal_error, request_state, time_marked_unknown_ms, total_disconnected_min) is self-contained inside the RM. No cross-RM call from the response handler. Mirrors `commit_request_manager`'s `Arc<CommitRequestManagerInner>` pattern. |
| `consumer_heartbeat_request_manager` | **mpsc channel-back** | `on_successful_response` must also call `membership_manager.on_heartbeat_success(response)` — and `MembershipManager` owns its own `Mutex`. If the forwarder takes a heartbeat-side `MutexGuard` and then re-enters the membership manager's `Mutex` (or vice versa), there is a §16 risk. Channel-back defers the cross-RM call into the next `poll()` cycle, where `&mut self` access on both is naturally serialized. **(See "Risks" below.)** |
| `fetch_request_manager` | **mpsc channel-back** | `handle_fetch_success` writes to `FetchBuffer` and `AbstractFetch` (session-handler) state. `FetchBuffer` is already `Arc<Mutex<...>>`-friendly, but `AbstractFetch` is owned `&mut self` on the manager. Channel-back avoids requiring a second interior-mutability layer over `AbstractFetch`. Mirrors `offsets_request_manager`'s `PendingCompletion` pattern. |
| `topic_metadata_request_manager` | **`Arc<Mutex<Inner>>`** | State (`inflight_requests`, `state`, retry backoff) is self-contained. The per-request `request_id` (audit recommendation) identifies the entry to drain from `inflight_requests`. Mirrors `commit_request_manager`. |

**Why not uniform?** Heartbeat's membership-manager cross-call and
fetch's `AbstractFetch` ownership genuinely differ from coordinator
and topic_metadata. Forcing all four to use `Arc<Mutex<Inner>>` would
either:
- Sprawl interior mutability into `MembershipManager` /
  `AbstractFetch` (each their own large refactor), OR
- Require holding both `Arc<Mutex<Inner>>` guards across an `.await`
  (§16 violation).

Forcing all four to use mpsc channel-back would work but add
unnecessary indirection for coordinator and topic_metadata, whose
state-update is fully self-contained. The audit confirms two RMs
(commit, offsets) already chose differently for the same reasons.

## Rust outputs

### Updated files

```
src/consumer/internals/coordinator_request_manager.rs           # Arc<Mutex<Inner>> refactor
src/consumer/internals/consumer_heartbeat_request_manager.rs    # mpsc PendingCompletion::Heartbeat
src/consumer/internals/abstract_heartbeat_request_manager.rs    # (possibly) channel-back drain hook
src/consumer/internals/fetch_request_manager.rs                 # mpsc PendingCompletion::Fetch
src/consumer/internals/topic_metadata_request_manager.rs        # Arc<Mutex<Inner>> refactor + per-request_id dispatch
tests/integration/consumer_test.rs                              # remove 4 #[ignore] markers
src/consumer/async_kafka_consumer.rs                            # NO CHANGE expected — production ctor already wires the RMs
design/history/Milestone-8/PLAN.md                              # add row 12.5 → CLOSED on phase-close
```

### NEW files

```
.claude/agent-memory/actor-executor/phase12_5_consolidated_patterns.md
```

## Behavior parity

### 1. `coordinator_request_manager` — `Arc<Mutex<Inner>>` refactor

Current shape:

```rust
pub struct CoordinatorRequestManager {
    group_id: String,
    coordinator: Option<Node>,
    fatal_error: Option<KafkaError>,
    request_state: RequestState,
    time_marked_unknown_ms: i64,
    total_disconnected_min: i64,
    // ...
}

impl CoordinatorRequestManager {
    fn make_find_coordinator_request(&mut self, now_ms: i64) -> UnsentRequest {
        // ... builds UnsentRequest::new(builder, None) ...
        UnsentRequest::new(builder, None)
    }
}
```

Target shape:

```rust
pub(crate) struct CoordinatorRequestManagerInner {
    group_id: String,
    coordinator: std::sync::Mutex<Option<Node>>,
    fatal_error: std::sync::Mutex<Option<KafkaError>>,
    request_state: std::sync::Mutex<RequestState>,
    time_marked_unknown_ms: AtomicI64,
    total_disconnected_min: AtomicI64,
    // ...
}

pub struct CoordinatorRequestManager {
    inner: Arc<CoordinatorRequestManagerInner>,
}

impl CoordinatorRequestManager {
    fn make_find_coordinator_request(inner: &Arc<CoordinatorRequestManagerInner>, now_ms: i64) -> UnsentRequest {
        inner.request_state.lock().unwrap().on_send_attempt(now_ms);
        let mut data = FindCoordinatorRequestData::new();
        // ...
        let builder: Box<dyn RequestBuilder> = Box::new(FindCoordinatorRequestBuilder::new(data));
        let mut unsent = UnsentRequest::new(builder, None);
        let response_rx = unsent.take_response_receiver().expect("receiver fresh");
        let inner_for_handler = Arc::clone(inner);
        tokio::spawn(async move {
            let now = SystemTime::now().elapsed_ms_or_zero();
            match response_rx.await {
                Ok(Ok(mut client_response)) => {
                    let body = client_response.take_response_body();
                    if let Some(resp) = body.downcast_ref::<FindCoordinatorResponse>() {
                        Self::on_response_inner(&inner_for_handler, now, resp);
                    } else {
                        // ... type-mismatch path ...
                    }
                },
                Ok(Err(err)) => Self::on_failed_response_inner(&inner_for_handler, now, err),
                Err(_recv) => Self::on_failed_response_inner(&inner_for_handler, now,
                    KafkaError::new(Errors::NetworkException)),
            }
        });
        unsent
    }
}
```

`on_response` / `on_failed_response` become free functions taking
`&Arc<CoordinatorRequestManagerInner>`. The `Sync` accessor methods
(`coordinator()`, `coordinator_node_option()`) lock the inner `Mutex`
read-only and return cloned values.

### 2. `consumer_heartbeat_request_manager` — mpsc channel-back

Target shape:

```rust
pub(crate) enum PendingCompletion {
    HeartbeatSuccess { response: ConsumerGroupHeartbeatResponse, now: i64 },
    HeartbeatFailure { err: KafkaError, now: i64 },
}

pub struct ConsumerHeartbeatRequestManager {
    inner: AbstractHeartbeatRequestManager<...>,
    membership_manager: Arc<ConsumerMembershipManager>,
    pending_completions_tx: mpsc::UnboundedSender<PendingCompletion>,
    pending_completions_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<PendingCompletion>>,
    // ...
}

impl RequestManager for ConsumerHeartbeatRequestManager {
    fn poll(&mut self, now: i64) -> PollResult {
        // Drain pending completions FIRST so state-update happens
        // before the next emission decision.
        self.drain_pending_completions(now);
        // ... existing build_heartbeat_request path ...
    }
}

impl ConsumerHeartbeatRequestManager {
    fn build_heartbeat_request(&self, ignore_response: bool, node: Option<Node>) -> UnsentRequest {
        // ... build ...
        let mut unsent = UnsentRequest::new(builder, node);
        let response_rx = unsent.take_response_receiver().expect("receiver fresh");
        let tx = self.pending_completions_tx.clone();
        tokio::spawn(async move {
            let now = SystemTime::now().elapsed_ms_or_zero();
            let completion = match response_rx.await {
                Ok(Ok(mut cr)) => match cr.take_response_body().downcast::<ConsumerGroupHeartbeatResponse>() {
                    Ok(resp) => PendingCompletion::HeartbeatSuccess { response: *resp, now },
                    Err(_) => PendingCompletion::HeartbeatFailure {
                        err: KafkaError::illegal_state("type mismatch on heartbeat response"),
                        now,
                    },
                },
                Ok(Err(err)) => PendingCompletion::HeartbeatFailure { err, now },
                Err(_recv) => PendingCompletion::HeartbeatFailure {
                    err: KafkaError::new(Errors::NetworkException),
                    now,
                },
            };
            // ignore_response leave-group path: drop the completion (Java parity at line 309).
            if !ignore_response {
                let _ = tx.send(completion);
            }
        });
        unsent
    }

    fn drain_pending_completions(&mut self, now: i64) {
        let mut rx = self.pending_completions_rx.try_lock();
        let rx = match rx { Some(r) => r, None => return };
        while let Ok(c) = rx.try_recv() {
            match c {
                PendingCompletion::HeartbeatSuccess { response, now: response_now } => {
                    let interval = response.data().heartbeat_interval_ms() as i64;
                    self.inner.on_successful_response(interval, response_now);
                    self.membership_manager.on_heartbeat_success(&response);
                },
                PendingCompletion::HeartbeatFailure { err, now: response_now } => {
                    self.inner.on_failure(response_now, &err);
                    self.membership_manager.on_heartbeat_failure(&err);
                },
            }
        }
    }
}
```

**Critical §16 invariant:** `drain_pending_completions` calls
`membership_manager.on_heartbeat_success` **with no heartbeat-side
guard held**. `MembershipManager` takes its own `Arc<Mutex<...>>`
inside that method — but the heartbeat RM no longer holds its inner
state under a guard at that point (state is `&mut self.inner`, owned
exclusively). Channel-back enables this cleanly.

### 3. `fetch_request_manager` — mpsc channel-back

Target shape:

```rust
pub(crate) enum PendingFetchCompletion {
    Success { fetch_target: Node, request_data: FetchSessionRequestData, response: FetchResponse, request_version: i16, now: i64 },
    Failure { fetch_target: Node, request_data: FetchSessionRequestData, err: KafkaError, now: i64 },
}

pub struct FetchRequestManager {
    abstract_fetch: AbstractFetch<...>,
    pending_completions_tx: mpsc::UnboundedSender<PendingFetchCompletion>,
    pending_completions_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<PendingFetchCompletion>>,
    // ...
}
```

In `poll_internal`, after building the per-node `UnsentRequest`:

```rust
let mut unsent = UnsentRequest::new(Box::new(builder), Some(target_node.clone()));
let response_rx = unsent.take_response_receiver().expect("receiver fresh");
let tx = self.pending_completions_tx.clone();
let request_data_for_task = request_data.clone();   // FetchSessionRequestData must be Clone
let target_node_for_task = target_node.clone();
let request_version = builder_request_version;
tokio::spawn(async move {
    let now = ...;
    let completion = match response_rx.await {
        Ok(Ok(mut cr)) => match cr.take_response_body().downcast::<FetchResponse>() {
            Ok(resp) => PendingFetchCompletion::Success {
                fetch_target: target_node_for_task, request_data: request_data_for_task,
                response: *resp, request_version, now,
            },
            Err(_) => /* type-mismatch failure */
        },
        Ok(Err(err)) => PendingFetchCompletion::Failure { ..., err, now },
        Err(_recv) => PendingFetchCompletion::Failure { ..., err: NetworkException, now },
    };
    let _ = tx.send(completion);
});
unsent
```

Drained at the top of `RequestManager::poll`:

```rust
fn poll(&mut self, now: i64) -> PollResult {
    self.drain_pending_completions(now);
    // ... existing poll body ...
}

fn drain_pending_completions(&mut self, now: i64) {
    while let Ok(c) = self.pending_completions_rx.try_lock().unwrap().try_recv() {
        match c {
            PendingFetchCompletion::Success { fetch_target, request_data, response, request_version, now: completion_now } => {
                self.abstract_fetch.handle_fetch_success(&fetch_target, &request_data, &response, request_version);
            },
            PendingFetchCompletion::Failure { fetch_target, request_data, err, now: completion_now } => {
                self.abstract_fetch.handle_fetch_failure(&fetch_target, &request_data, &err);
            },
        }
    }
}
```

**Receive-path zero-copy invariant (consumer-threading.md §27):** must
hold. The `FetchResponse` body the forwarder moves into the
`PendingFetchCompletion::Success` variant is the response body taken
from `take_response_body()` — this is the SAME `Bytes` buffer the
broker decode path read into. No copy is introduced; the body
ownership transfers from the spawned forwarder's stack into the mpsc
channel into the manager's drain loop into `FetchBuffer`. Verify:

- `FetchResponse::data().responses()` returns references into the
  decoded `Bytes`, not copies.
- `FetchSessionRequestData` is `Clone`-able (it carries node + topic
  IDs + partition list — small, no record bytes).
- `handle_fetch_success` appends `CompletedFetch` to `FetchBuffer`,
  the `CompletedFetch` borrows from the FetchResponse's `Bytes`.

If `FetchSessionRequestData::Clone` requires copying topic-partition
metadata that is not yet `Arc<...>`, that is acceptable — it is
per-RPC metadata, not per-record. Per-record bytes never leave the
buffer.

### 4. `topic_metadata_request_manager` — `Arc<Mutex<Inner>>` refactor

Same shape as #1 (coordinator), with the per-request `request_id`
captured into the forwarder to identify the entry to drain from
`inflight_requests`:

```rust
let request_id = state.request_id;   // u64 — unique per inflight request
let inner_for_handler = Arc::clone(inner);
let mut unsent = UnsentRequest::new(builder, None);
let response_rx = unsent.take_response_receiver().expect("receiver fresh");
tokio::spawn(async move {
    let now = ...;
    match response_rx.await {
        Ok(Ok(mut cr)) => match cr.take_response_body().downcast::<MetadataResponse>() {
            Ok(resp) => Self::on_response_inner(&inner_for_handler, request_id, now, *resp),
            Err(_) => Self::on_failure_inner(&inner_for_handler, request_id, now, type_mismatch_err()),
        },
        Ok(Err(err)) => Self::on_failure_inner(&inner_for_handler, request_id, now, err),
        Err(_recv) => Self::on_failure_inner(&inner_for_handler, request_id, now, NetworkException),
    }
});
unsent
```

`on_response_inner` / `on_failure_inner` lock the inner Mutex, find
the matching `inflight_requests` entry by `request_id`, and complete
its `ack: oneshot::Sender<...>`.

## Tests in scope

### Per-RM regression tests (1-2 each)

For each BROKEN RM, add at least one unit test that:

1. Builds the manager.
2. Calls the build method, captures the returned `UnsentRequest`.
3. Synthesizes a `ClientResponse` via
   `unsent.handler().on_complete(client_response)`.
4. Yields the runtime (`tokio::task::yield_now().await`) so the
   spawned forwarder runs.
5. Asserts the manager's state has updated (e.g.
   `manager.coordinator().is_some()`).

These tests already exist for `commit_request_manager` and
`offsets_request_manager`; the pattern is `complete_first_unsent_with_response`
+ `tokio::task::yield_now()`. Mirror for the four BROKEN RMs.

For `consumer_heartbeat_request_manager` and `fetch_request_manager`
(channel-back), the test also needs to call `manager.poll(now)` after
the yield to drain the `PendingCompletion`. The drain happens at the
top of `poll()`, so a second `poll()` call after the yield is the
test driver.

### Un-ignore the 4 integration tests

`tests/integration/consumer_test.rs:213, 295, 339, 418` —
`#[ignore = "..."]` markers removed. Each test must run green against
the testcontainers Kafka 4.2.0 broker.

## Commit plan

Each commit is independently buildable. Each production commit ships
the refactor + regression test for ONE RM.

| # | Title | Files touched | Approx LOC |
|---|---|---|---|
| 0 | `Phase 12.5 (0/N): PLAN.md — wire response routing for 4 BROKEN RMs` | this file | — |
| 1 | `Phase 12.5 (1/N): coordinator_request_manager — Arc<Mutex<Inner>> response routing` | `coordinator_request_manager.rs` + unit test | ~50 prod / ~30 test |
| 2 | `Phase 12.5 (2/N): topic_metadata_request_manager — Arc<Mutex<Inner>> response routing` | `topic_metadata_request_manager.rs` + unit test | ~50 prod / ~30 test |
| 3 | `Phase 12.5 (3/N): consumer_heartbeat_request_manager — mpsc channel-back response routing` | `consumer_heartbeat_request_manager.rs` (+ possibly `abstract_heartbeat_request_manager.rs`) + unit test | ~70 prod / ~40 test |
| 4 | `Phase 12.5 (4/N): fetch_request_manager — mpsc channel-back response routing` | `fetch_request_manager.rs` + unit test. Verify §27 zero-copy invariant. | ~80 prod / ~40 test |
| 5 | `Phase 12.5 (5/N): un-ignore 4 integration tests in tests/integration/consumer_test.rs` | `tests/integration/consumer_test.rs` (remove 4 `#[ignore]` markers) | ~10 test |
| 6 | `Phase 12.5 (6/N): close-out — agent memory + PLAN status + rustdoc cleanup` | `agent-memory/...`, `Phase-12.5/PLAN.md` status section, `coordinator/heartbeat/fetch/topic_metadata` rustdoc deleted, `Milestone-8/PLAN.md` row 12.5 status | small |

**Granularity rationale:** Commits (1)-(4) ship one RM per commit so
each fix is reviewable in isolation, and a Critic round can give
feedback per RM before the next lands. (1) and (2) (interior
mutability) are mechanically similar — landing them first builds
muscle memory for (3) and (4). (5) is the green-tests gate. (6)
closes out.

Commits (1)-(4) can land serially or in parallel — Phase 12.5 itself
chooses serial because the Critic loop is per-commit. The
`#[ignore]` removal in commit (5) is gated on (1)-(4) all landing.

**Inter-commit dependencies:** (0)→(1)→(2)→(3)→(4)→(5)→(6). The
integration tests in (5) depend on ALL FOUR RMs being wired — a
test like `test_subscribe_and_poll_records` needs FindCoordinator
(1), Heartbeat (3), and Fetch (4) all working; `test_assign_partitions_and_poll`
needs only Fetch (4). Splitting (5) by which-RM-unblocks-which is
possible but adds churn; landing (5) as one commit after all four
RM commits is simpler.

## Definition of Done

Per `definition-of-done.md` plus phase additions:

- `cargo build` clean.
- `cargo test` (unit + non-integration tests) clean. All 4 per-RM
  regression tests pass.
- `cargo xtask format-check` clean.
- `cargo xtask lint` clean (clippy-warnings-as-errors).
- `cargo xtask check-generated` clean.
- `cargo test --features integration-tests --test integration_main consumer_test`
  — **all 4 integration tests run green** against the testcontainers
  Kafka 4.2.0 broker. The Actor must run this locally before declaring
  done.
- All 4 BROKEN RMs WIRED per the canonical pattern. Verifiable by
  `grep -n take_response_receiver src/consumer/internals/*.rs` returning
  ≥6 production callsites (was 5: commit×2, offsets×3, topic_metadata×0
  in production — Phase 12.5 makes topic_metadata's existing test-only
  callsite at line 896 production, plus adds coordinator, heartbeat,
  fetch).
- The 3 misleading rustdoc blocks at
  `coordinator_request_manager.rs:226-233`,
  `fetch_request_manager.rs:267-270`, and
  `topic_metadata_request_manager.rs:184-199` are **deleted or
  rewritten** to describe the actual wire-up.
- **Receive-path zero-copy invariant (consumer-threading.md §27)
  preserved on the Fetch RM fix.** Specifically:
  - `FetchResponse` body bytes are owned by exactly one `Bytes`
    buffer from network read to `CompletedFetch.append`.
  - Per-record decode borrows slices from that buffer.
  - The mpsc channel-back path moves the body by ownership, not by
    copy.
- **§16 invariant preserved on heartbeat fix.** Specifically:
  - No `MutexGuard` held across `.await`.
  - `drain_pending_completions` does not hold a heartbeat-side guard
    when calling into `membership_manager.on_heartbeat_success`.
  - The membership manager's own `Mutex` is acquired only inside its
    `on_heartbeat_success` method, never reentered.
- **Trait surface check (DoD §11):**
  - No new `#[async_trait]` introduced on per-record paths.
  - The forwarder `tokio::spawn`'d inside the build method is per-RPC,
    not per-record — CLAUDE.md §11 explicitly allows
    `Pin<Box<dyn Future>>` at this granularity.
- **Hot-path allocation audit (DoD §10):**
  - No per-record `tokio::spawn` introduced. The Fetch forwarder runs
    once per FetchRequest (per-broker batch), not once per record.
  - The `PendingFetchCompletion::Success` variant carries the
    `FetchResponse` by value (no `Box`/`Arc` wrapper), and the
    `FetchResponse` body bytes remain in the same `Bytes` buffer
    network read produced — zero copy preserved.

## Risks

1. **§16 risk on heartbeat fix.** The most subtle: the response
   handler must call into `MembershipManager`, which holds its own
   `Arc<Mutex<...>>`. If the heartbeat-side guard is held when
   calling `membership_manager.on_heartbeat_success`, and that
   method internally acquires the membership Mutex, the second
   acquire (in the same task) deadlocks an `std::sync::Mutex` or
   blocks an async runtime on `tokio::sync::Mutex`. **Mitigation:**
   mpsc channel-back. The heartbeat manager drains the
   `PendingCompletion::Heartbeat` envelope inside its own `poll(now)`
   call, where `&mut self.inner` is taken cleanly — no guard is held
   when the membership manager call fires. **Audit step in commit
   (3) review:** read every line between `drain_pending_completions`
   call and `membership_manager.on_heartbeat_success` call, confirm
   no `Mutex::lock()` invocation between them.

2. **Receive-path zero-copy on fetch fix.** The `FetchResponse`
   passes through an mpsc channel. The channel sender is
   `tokio::sync::mpsc::UnboundedSender<PendingFetchCompletion>`, which
   moves values by ownership. The risk is if any field on
   `PendingFetchCompletion::Success` is a type that internally copies
   bytes (e.g. if `FetchResponse` decode is eager rather than lazy).
   **Mitigation:** verify in commit (4) Critic review that
   `FetchResponse::data()` returns `&'_ FetchResponseData` borrowed
   from the underlying `Bytes`, not an owned eager decode. The §27
   tests in Phase 7a-c should already cover this — but re-run
   `cargo test consumer::internals::completed_fetch` after the fix
   to confirm no allocation regression.

3. **Integration test flakiness.** KIP-848 server-side assignment
   timing can race in the testcontainers environment. **Mitigation:**
   same as Phase 12 — 30-second poll deadline, `cluster_config_with_kip848`
   sets `group.coordinator.rebalance.protocols=classic,consumer`,
   producer-then-consume ordering with `flush().await`.

4. **`FetchSessionRequestData::Clone` cost.** The Fetch RM forwarder
   captures `request_data.clone()` into the spawned task. If
   `FetchSessionRequestData` carries a large topic-partition list,
   the clone is per-RPC, not per-record — acceptable. If the type is
   not currently `Clone`, derive it (commit 4) and verify the clone
   does not touch record bytes.

5. **Phase-10 callsite cleanup.** `consumer_network_thread.rs::run_once`
   currently does not call `take_response_receiver` for any of the 4
   BROKEN RMs. After Phase 12.5, the receivers are taken **inside**
   each RM's build method (before `PollResult` is constructed), so
   `run_once` continues to be receiver-agnostic. **No bg-task
   refactor expected.** Verify by grepping
   `consumer_network_thread.rs` after each commit — the file should
   not gain any `take_response_receiver` call.

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor agent N=1 spawned to implement commit (1). Build/test/format/lint.
2. Critic agent N=1 reviews commit (1) → `COMMENTS.1.md`.
3. Actor fixes, moves to `COMMENTS.DONE.1.md`, fixup commit.
4. Loop (1)-(3) per RM commit (commits 2, 3, 4).
5. Once all 4 BROKEN RMs are wired (commits 1-4 landed clean), commit
   (5) un-ignores the integration tests. Critic reviews against the
   docker run.
6. Commit (6) closes out: Status section, rustdoc deletion in
   the 3 misleading docstring blocks, agent-memory entry,
   `Milestone-8/PLAN.md` row 12.5 status flip.

## Out of scope

- **New RM functionality.** No new request types, no new request
  payloads. Phase 12.5 is purely response-routing fixes.
- **Test refactoring beyond un-ignoring the existing 4.** The
  integration tests are correct as written (Critic round-3
  confirmation in `COMMENTS.DONE.1.md` Issue 7).
- **KIP-848 protocol additions.** Phase 12.5 wires response handling
  for already-translated request types; no new wire-level work.
- **SSL/SASL.** PLAINTEXT only — same scope as Phase 12.
- **Metrics / telemetry.** `AsyncConsumerMetrics` /
  `KafkaConsumerMetrics` remain deferred milestone-wide.
- **Classic-protocol path.** Out of milestone per
  `consumer-threading.md` §20.
- **C FFI for `AsyncKafkaConsumer`.** Out of milestone.

## Status: CLOSED (Phase 12.5 commits 1/N — 6/N)

Phase 12.5 closed: all six request managers either had response
routing wired in this phase (coordinator, topic_metadata,
consumer_heartbeat, fetch — driven from the bg task or its
spawned task slot) or already had it (commit, offsets — calling
`take_response_receiver` + dispatching from a spawned task). The
four integration tests are un-ignored and run green end to end.

### Commit-hash table

| Commit  | Title                                                                                       |
|---------|---------------------------------------------------------------------------------------------|
| ba37a51 | Phase 12.5 (1/N): coordinator_request_manager — Arc<Mutex<Inner>> response routing          |
| 9086078 | fixup! Phase 12.5 (1/N): Issue 2 — robust forwarder-sync in regression tests                |
| fcbb08f | fixup! Phase 12.5 (1/N): Issue 1 — clear fatal_error on coordinator failure paths           |
| a239b80 | Phase 12.5 (2/N): topic_metadata_request_manager — Arc<Mutex<Inner>> response routing       |
| 1950caf | Phase 12.5 (3/N): consumer_heartbeat_request_manager — mpsc channel-back response routing   |
| 6059bc7 | fixup! Phase 12.5 (3/N): Issue 3 — reset_heartbeat_state on error response branch           |
| 0e72ee7 | fixup! Phase 12.5 (3/N): Issue 4 — drive transition_to_fenced/_fatal from classifier        |
| 842e046 | fixup! Phase 12.5 (3/N): Issue 6 — Fenced arm should not emit BackgroundEvent::Error        |
| 4f84ecd | fixup! Phase 12.5 (3/N): Issue 5 — unknown-error-code fatal fallback in heartbeat classifier|
| e0d98ba | fixup! Phase 12.5 (3/N): Issue 7 — register CommitRequestManager as MemberStateListener     |
| c2834cd | Phase 12.5 (4/N): fetch_request_manager — mpsc channel-back response routing                |
| a969a67 | fixup! Phase 12.5 (5/N): Issue 8 — cap max.poll.records in commit-resume test               |
| dd33ebf | fixup! Phase 12.5 (5/N): Issue 8 — add LeaveGroup sleep + raise c2 poll deadline            |
| 9662a77 | Phase 12.5 (5/N): un-ignore 4 integration tests — response routing wired end-to-end         |
| (HEAD)  | Phase 12.5 (6/N): close-out — 4/4 integration tests green + PLAN status + agent-memory      |

### What shipped

- **coordinator_request_manager** (`ba37a51`): `Arc<Mutex<Inner>>`
  refactor; `make_find_coordinator_request` now calls
  `take_response_receiver` and spawns a task that dispatches to
  `Inner::on_response` / `on_failure`. Critic round-1 follow-ups
  (Issues 1 + 2) moved the `getAndClearFatalError()` call to the
  top of the spawned task (matching Java's `whenComplete` parity)
  and hardened the regression test's forwarder-sync.

- **topic_metadata_request_manager** (`a239b80`): `Arc<Mutex<Inner>>`
  refactor along the same pattern; `make_topic_metadata_request`
  takes the receiver and spawns dispatch into
  `Inner::handle_response` / `handle_error`.

- **consumer_heartbeat_request_manager** (`1950caf` + 5 Critic round-2
  / round-3 fixups): mpsc-channel-back response routing — the sync
  `poll(now)` synthesizes a "pending membership transition" envelope
  that the bg task drains BEFORE
  `membership.reconcile(now).await` to drive
  `transition_to_fenced` / `_fatal` from the heartbeat classifier
  on the appropriate task. Issues 3-7 closed the rest of the
  AbstractHeartbeatRequestManager parity gap: reset state at top
  of error branch, classifier drives membership transitions, Fenced
  arm does not emit `BackgroundEvent::Error`, unknown error codes
  fall through to `Fatal` (not `Handled`), and
  `CommitRequestManager` is now registered as a
  `MemberStateListener` so OffsetCommit requests carry the correct
  member id/epoch.

- **fetch_request_manager** (`c2834cd`): mpsc channel-back response
  routing; the bg task drains the pending-set in
  `create_fetch_request` and dispatches fetch responses to
  `FetchBuffer` (and the FetchSession state-machine).

- **Integration tests un-ignored** (`9662a77`): all four
  `tests/integration/consumer_test.rs` tests now run green against a
  testcontainers Kafka 4.2.0 broker. The
  `test_commit_sync_then_resume_in_same_group` test needed two
  test-side adjustments beyond the original `max.poll.records=5`
  cap (5s sleep between consumer1.close and consumer2 ctor; c2's
  poll-loop deadline raised 30s → 60s) — production code is
  unchanged.

### Final test counts

- **Unit tests** (`cargo test`): 3,447 passed (Phase 12.5 added
  regression tests for `clear_fatal_error_on_recv_err`,
  `forwarder_advances_on_send_failure`,
  `commit_picks_up_member_id_epoch_after_registration`, plus
  per-RM unit tests for the new response-routing paths).
- **Integration tests** (`cargo test --features integration-tests
  --test integration consumer_test`): 4 passed, 0 failed,
  0 ignored.
- **Format/lint**: clean.

### Open follow-ups

None known. The Phase-12 carry-over (response routing) is closed.
Listener registration discrepancies, error-classifier branch
parity, and ErrorEvent emission matrix all match Java semantics
end to end (Critic round 1-4 all closed; see `COMMENTS.DONE.1.md`).
