# Phase 12 — Critic round 2

Re-reviewed fixups `db3f793`, `8e2540c`, `6114cb0` against the Java
contract and the round-1 issue resolutions.

| Round-1 Issue | Verdict | Evidence |
|---|---|---|
| 1 — commit poll wire-up | **resolved correctly** | `run_once` (consumer_network_thread.rs:445-454) takes coord+commit handles atomically, calls `commit_arc.poll_with_coordinator(&mut coord_g, now)` between coordinator and heartbeat (Java `entries()` order). `cleanup()` adds explicit `drain_pending_offset_commit_requests()` (line 770-773). Regression test passes. |
| 2 — single notifier | **resolved correctly** | Production ctor builds `group_metadata`/`group_assignment_snapshot`/`state_notifier` once (lines 906-917), registers the SAME `state_notifier` Arc on membership manager (line 920), then threads the SAME Arcs into `AsyncKafkaConsumerComponents` (lines 1141-1143). `new_with_components` consumes the fields directly — no second notifier built (lines 1162-1164). Regression test cast-to-`Arc<dyn MemberStateListener>` and dispatches via the listener trait method; the consumer-side `group_metadata()` observes the write. |
| 3 — shared `Arc<AtomicI64>` | **resolved correctly** | Three-way identity: ctor builds `Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))` (async_kafka_consumer.rs:1063), `Arc::clone` passed to `ConsumerNetworkThread::new` (line 1074), original Arc moved into `components.max_time_to_wait_ms` (line 1130) → struct field (line 1171) → read by `maximum_time_to_wait_ms()` (line 1376). Bg-task writes via `self.cached_max_time_to_wait_ms.store(...)` (consumer_network_thread.rs:544). All five Phase-10 test callsites pass `Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))`. |
| 4 — auth-closure FIXME | **resolved correctly** | Inline comment at async_kafka_consumer.rs:900-913 explicitly characterizes the `maybe_throw_auth_failure` no-op as a correctness gap (not a perf cost). `FIXME(phase-9-sasl)` marker added at line 915 with Java reference (`AbstractFetch.java:452-457`). |
| 5 — orphan-drop | **resolved (subsumed by Issue 2)** | `let _ = (group_metadata, group_assignment_snapshot);` and the "intentionally dropped" rationale are gone — `grep -n "let _ = (group_metadata"` returns no hits. The Arcs are now properly threaded through components. |
| 6 — stale `max_time_to_wait_ms` comment | **resolved** | Comment block at `async_kafka_consumer.rs:1092-1100` rewritten to describe the actual wiring (Arc seeded with `MAX_POLL_TIMEOUT_MS`, bg-task writes every iteration, app-side accessor reads through shared Arc). Folded into Phase 12 commit (4/N). |

Cleared to proceed with Phase 12 commits (5)-(7). Round-1 + round-2 issues are
fully resolved; no open comments.

---

# Phase 12 — Critic round 3

Reviewed commits (4) `188ddf0`, (5) `4b48abe`, (6) `7bea0a4`. The Actor
flagged a scope-question issue: 4/4 integration tests `#[ignore]`-gated
on a claimed pre-existing Phase-10 response-routing gap.

## Issue 7: SCOPE-QUESTION — FindCoordinator / Heartbeat / Fetch response routing is not wired
- **File**: `src/consumer/internals/coordinator_request_manager.rs:234-241`,
  `src/consumer/internals/consumer_heartbeat_request_manager.rs:268-279`,
  `src/consumer/internals/fetch_request_manager.rs:264-272`
- **Severity**: Bug (scope-question — does Phase 12 close out red?)
- **Java Reference**: `CoordinatorRequestManager.java:113-132`,
  `AbstractHeartbeatRequestManager.java:296-309`,
  `AbstractFetch.java` (the `whenComplete`-attaching call site).
- **Description**: Independent verification confirms the Actor's claim
  in `tests/integration/consumer_test.rs:30-65`: three RMs build their
  `UnsentRequest` without registering any response-dispatch path,
  causing the response oneshot to fire into a dropped receiver and
  `on_response` / `on_failed_response` / `on_heartbeat_success` to
  never be invoked.

  Evidence:

  1. `UnsentRequest::new` (network_client_delegate.rs:171-181)
     unconditionally constructs an `oneshot::Receiver` and stores it
     in `response_rx: Some(rx)`. The receiver lives inside the
     `UnsentRequest` until someone `take()`s it via
     `take_response_receiver()`.

  2. `do_send` (network_client_delegate.rs:732-735) attaches a
     `RequestCompletionHandler` callback to the `ClientRequest` that
     invokes `handler.on_complete_ref(response)` when `KafkaClient::poll`
     fires it. `on_complete` (line 324-355) takes the sender out of
     the handler's `Mutex<Option<oneshot::Sender>>` and writes
     `Ok(response)` — **success-path-only**, no separate failure
     dispatch beyond the embedded `was_disconnected` / auth-error /
     version-mismatch branches.

  3. `try_send` (network_client_delegate.rs:680-696) pops the
     `UnsentRequest` off `self.unsent_requests`, calls
     `do_send(&mut unsent, ...)`, and on success **drops `unsent`**
     without re-queueing. Dropping `UnsentRequest` drops the
     still-alive `Option<oneshot::Receiver>` inside `response_rx`.
     When `on_complete` later tries to `tx.send(Ok(response))`
     (line 353), the receiver side is already closed; the `let _ =`
     silently swallows the `SendError`. **The response is lost.**

  4. `grep take_response_receiver src/consumer/internals/*.rs`
     returns production hits only in:
     - `commit_request_manager.rs:1414, 1480` — `tokio::spawn` from
       inside `build_offset_commit_unsent_request` /
       `build_offset_fetch_unsent_request` dispatches into
       `handle_offset_commit_response` / `handle_offset_fetch_response`.
     - `offsets_request_manager.rs:327, 854, 925` — `tokio::spawn`
       dispatches into the list-offsets handler.
     - `topic_metadata_request_manager.rs:896` — same pattern.

     For `coordinator_request_manager`, the only mention of
     `take_response_receiver` is **in a rustdoc comment** (line 230)
     claiming the bg task takes the receiver — false. For
     `consumer_heartbeat_request_manager`, there is **no mention at
     all**. For `fetch_request_manager`, the comment at line 267-270
     explicitly admits "Phase 10 wires `whenComplete` here … Phase 7b
     just returns the request" — the deferral was acknowledged but
     never landed.

  5. `consumer_network_thread.rs::run_once` (line 325-700) does not
     contain a single `take_response_receiver` call. Its
     response-dispatch role is purely `delegate.poll(...)` (line
     514-553), which fires the `RequestCompletionHandler` callback on
     the `ClientRequest` — which writes into the (already-dropped)
     receiver for `coordinator` / `heartbeat` / `fetch`.

  6. Phase 10's PLAN.md (lines 235-284) describes `whenComplete`-style
     wiring only for `AsyncPollEvent`, not for the per-RM response
     dispatch. A `grep` for "FindCoordinator" or "Heartbeat response"
     in `Phase-10/PLAN.md` returns zero hits.

  7. The Phase-10 test `run_once_drives_commit_poll_with_coordinator`
     (consumer_network_thread.rs:1888-1985) asserts only that the
     commit request **lands on the unsent queue**, not that the
     response is routed back to the commit manager. So no existing
     test exercises the response-routing path through the bg task,
     consistent with the gap being a real omission.

  Java contract (verified): `CoordinatorRequestManager.java:118-131`
  builds the `UnsentRequest` and **attaches a `whenComplete` callback**
  on the same line via `return unsentRequest.whenComplete(...)`. The
  callback drives `onResponse` / `onFailedResponse`. The Rust
  `UnsentRequest` has no `when_complete` method (grep
  `network_client_delegate.rs` confirms); the translation chose a
  `take_response_receiver` → spawn-a-task model, but only 3 of 6 RMs
  use it.

- **Expected**: All RMs that emit an `UnsentRequest` register a
  response dispatcher — either via the `tokio::spawn` pattern that
  commit/offsets/topic_metadata already use (RM-internal, requires
  the RM to hold a clone of its own `Arc<Mutex<...>>` or to use
  interior-mutability `Arc<*Inner>` like commit does), or via a
  bg-task-side router that walks each `PollResult.unsent_requests`,
  calls `take_response_receiver()`, and spawns a dispatch task per
  request. Either fix is local — there is no need for a "substantial
  refactor".

- **Actual**: 3 of 6 RMs (`coordinator`, `consumer_heartbeat`, `fetch`)
  silently drop the response. The consumer never leaves JOINING, the
  heartbeat never advances, and fetch never delivers records — exactly
  the symptom the Actor observed against the live broker.

### Correction of the Actor's analysis

The Actor's module docstring at `tests/integration/consumer_test.rs:62-65`
calls the fix a "substantial refactor (the bg task needs a per-RM
response-router task per pending request, mirroring Java's
`whenComplete` callback chain)". This **overstates the scope**. The
working RMs (commit, offsets, topic_metadata) show the fix is small:

```rust
// Pattern used in commit_request_manager.rs:1413-1428
let mut unsent = UnsentRequest::new(Box::new(builder), coordinator_node);
let response_rx = unsent.take_response_receiver().expect("receiver fresh");
let inner_for_handler = Arc::clone(inner);
tokio::spawn(async move {
    match response_rx.await {
        Ok(Ok(mut client_response)) => { handle_response(&inner_for_handler, ...); },
        Ok(Err(err)) => { request.complete_err(err); },
        Err(_recv_err) => { /* receiver dropped */ },
    }
});
unsent
```

For `coordinator_request_manager`, applying this pattern requires
`make_find_coordinator_request` to have access to an `Arc<Mutex<Self>>`
or to be refactored to interior-mutability (`Arc<CoordinatorRequestManagerInner>`,
mirroring `CommitRequestManagerInner`). This is a 1-RM refactor, not
an architectural reshuffle. Same shape applies to
`consumer_heartbeat_request_manager` and `fetch_request_manager`.

### Recommended phase outcome

- **(i) Ship as-is with `#[ignore]`d tests** — defensible given the
  scope creep (3 RMs need refactor + Arc-of-self plumbing). But:
  - The `#[ignore]` reason in `test_assign_partitions_and_poll`
    blames "FetchRequestManager response routing", but the test
    additionally needs the fetch path's per-record decode loop to
    work end-to-end against the broker. Verify the assign-only test
    really only blocks on fetch routing, or surface the additional
    dependency in the docstring.
  - The rustdoc on `coordinator_request_manager.rs:226-233` and
    (implied) `consumer_heartbeat_request_manager.rs:266-267` are
    **actively wrong** — they claim the bg task does the routing.
    Update those docstrings to point at the gap and the carry-over
    issue, so a future reader does not chase a non-existent code path.
  - Open a carry-over issue (Phase 12b? Phase 13 prerequisite?)
    with the precise scope: 3 RMs × ~30 LOC of `tokio::spawn` plumbing
    + 1 Arc<Mutex<Self>>-or-interior-mutability decision per RM +
    un-ignore the 4 integration tests.

- **(ii) Extend Phase 12 scope and fix the gap** — the size analysis
  argues this is less work than the Actor's docstring claims (~90 LOC
  + 3 plumbing decisions). Given the integration tests were the
  highest-value Phase 12 deliverable (PLAN.md §"Definition of Done"
  treats them as load-bearing), fixing in-phase preserves the
  green-tests-or-shrink-scope invariant the Manager has used elsewhere.

- **(iii) Hybrid** — land the smallest of the three (coordinator
  alone) inside Phase 12 because it unblocks `test_subscribe_and_poll_records`
  ONLY IF heartbeat also lands. Heartbeat-without-fetch unblocks
  nothing. So hybrid collapses into either (i) or (ii). No middle
  ground.

My recommendation: **(ii)** if the Actor confirms the refactor fits in
~1-2 days of work (the pattern is mechanical), else **(i)** with the
rustdoc fixes above. Either way, the 4 integration tests as written
are correct; the question is when they go green.

## Issue 8: rustdoc comment misrepresents code state
- **File**: `src/consumer/internals/coordinator_request_manager.rs:226-233`
- **Severity**: Bug (documentation lie that misleads future readers)
- **Description**: The rustdoc claims "in Rust the bg task (Phase 10)
  takes the response receiver via `UnsentRequest::take_response_receiver`
  and routes the result to `Self::on_response` / `Self::on_failed_response`."
  This is **false**: no production code path calls
  `take_response_receiver` for `coordinator_request_manager`, and the
  bg task at `run_once` does not dispatch coordinator responses
  (verified via `grep take_response_receiver
  src/consumer/internals/`). The receiver is dropped silently when
  the `UnsentRequest` is consumed by `do_send`.
- **Expected**: The docstring should read approximately "Java
  registers a `whenComplete` callback on the `UnsentRequest`'s
  future. Rust's translation deferred this wire-up — the receiver is
  not currently routed to `on_response`. Tracking: Phase 12b
  carry-over." Or — better — the comment is removed when Issue 7's
  fix lands.
- **Actual**: Future readers (Critic agents, Actor agents, Manager)
  will follow the docstring's pointer to `run_once`, fail to find a
  `take_response_receiver` call there, and waste time tracing the
  routing path that does not exist.

## Issue 9: smoke test ctor likely takes 30s for `close().await`
- **File**: `tests/consumer/async_kafka_consumer_test.rs:74-93`
- **Severity**: Test-quality (not a bug per se, but slows CI)
- **Description**: `new_consumer_builds_and_closes_against_refused_broker`
  uses `group.id = "smoke-test-group"`, which means `close()` runs
  `leave_group_on_close` (async_kafka_consumer.rs:3520-3559). That
  function calls `submit_and_drain` against a deadline derived from
  `request_timeout_ms` (default 30,000 ms). Without a coordinator
  (broker refuses), the `LeaveGroupOnClose` event never resolves
  successfully — `submit_and_drain` waits the full 30s, hits
  `KafkaError::Timeout`, the `Err(KafkaError::Timeout(_))` arm at
  line 3547 swallows the error, and close proceeds. Net: the smoke
  test takes ~30s per invocation. Repeated across CI runs this
  adds up.
- **Expected**: Either (a) set `request.timeout.ms` and
  `default.api.timeout.ms` to a small value (e.g. 1000ms) in
  `make_smoke_config()` so the close path resolves quickly, or (b)
  use a groupless config (no `group.id`) which short-circuits
  `leave_group_on_close` immediately (line 3521-3523). The second
  is the safer choice — exercises the same ctor + `close()` path,
  no group-coordinator side-effects.
- **Actual**: Smoke test takes 30s in the no-broker case. Verify by
  running `time cargo test --test consumer
  new_consumer_builds_and_closes_against_refused_broker`. If the
  observed runtime is < 5s, this Issue is moot; otherwise apply (b).

## Notes on commits (5) and (6) — assertion-quality review

The four integration tests in `tests/integration/consumer_test.rs`
are well-structured: deterministic key/value generation, deadline
loops with 30s timeouts, broker setup via `cluster_config_with_kip848`
that does NOT mutate the default `ClusterConfig` hash key (PLAN.md
requirement), and clean teardown via `consumer.close().await` at
the end of every flow. The assertions are tight (e.g. monotonic
offset check per partition, exact-count produced-vs-collected key
match). These will exercise the production path correctly once
Issue 7 is resolved.

One small observation on `test_commit_sync_then_resume_in_same_group`
(consumer 2 loop, line 387-396): the early-break heuristic
`if total > 0 && start.elapsed() > Duration::from_secs(10) { break; }`
relies on the broker not delivering stragglers after 10s. If the
broker is slow, consumer 2 might break out with `total == 1` and
the assertion `total <= 5` still holds even though more records
are pending. Tighter assertion: `assert_eq!(total, 5, ...)` after
draining for the full 30s. The current assertion is permissive but
not wrong.

