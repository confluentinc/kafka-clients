# Phase 41 — `ConsumerHandle`: rebalance-listener reentrancy + cross-task wakeup

**Actor:** 41 · **Critic:** 41 · **Branch:** consumer-impl · **Type:**
production design change (NOT test-parity). Public-API addition + bg-task
state-machine rework. Perf scrutiny mandatory (the bg loop was hand-tuned in
Phase 27/28 — see Perf Contract below).

## Goal

Close the two coupled gaps left open after the test-parity effort
(`consumer_remaining_work` memory), both rooted in the same fact — **Java's
`Consumer` is a freely-shareable thread-safe reference; Rust's is `&mut self`
+ a non-`Clone` `Box<dyn Consumer>`**:

1. **Cross-task `wakeup()`** — Java calls `consumer.wakeup()` from another
   thread while the owner blocks in `poll()`/`position()`. Rust cannot hand a
   `&consumer` across a task boundary.
2. **In-callback rebalance-listener reentrancy** — Java listeners call
   `consumer.assign/seek/pause/resume/position/committed/beginningOffsets/
   commit` from *inside* `onPartitionsAssigned/Revoked` by capturing the
   consumer in the closure. Rust's `Arc<dyn ConsumerRebalanceListener>`
   (`&self`) has no path to the `&mut self` consumer, AND the bg task is
   frozen during the callback (blocker b, below).

Closes the 10 `#[ignore]`d tests: 8 in
`tests/integration/plaintext_consumer_callback_test.rs` + 2 wakeup-during-
`position` tests in `tests/integration/plaintext_consumer_test.rs`.

## Locked design decision (user-approved 2026-06-24): Option A

Introduce one **`ConsumerHandle`** (`Clone + Send + Sync`) obtained from the
consumer that exposes `wakeup()` **and** the reentrant-safe consumer ops. The
user captures it into their listener struct — the Rust equivalent of Java
capturing the `consumer` variable. This:

- keeps the `ConsumerRebalanceListener` trait **byte-for-byte Java-identical**
  (methods still take only `partitions`, `&self`);
- **replaces** the Phase-40 `WakeupHandle` (one non-Java addition, not two);
- mirrors Java's "capture the consumer" idiom most closely.

Rejected: **B** (context-param on listener methods — deviates the listener
trait signature from Java, two mechanisms); **C** (match Java exactly, fix
neither — incompatible with the goal). End-user rationale (the only delta vs
Java for an app author is "capture `consumer.handle()` instead of `consumer`";
all other differences — async/`.await`, `Result` vs exceptions, `&mut self`
ownership, named-struct listener, explicit `close().await` — are inherent to
async Rust and identical under A/B/C) is recorded in §Design Rationale below.

## The two blockers (verified against current code)

**Blocker (a) — listener has no path to the consumer.** Listener is
`Arc<dyn ConsumerRebalanceListener>` / `&self`; consumer ops are
`async fn(&mut self)`; `Box<dyn Consumer>` is not `Clone`. Fixed by the
handle.

**Blocker (b) — bg task is frozen for the whole callback.** `run_once`
(`consumer_network_thread.rs:394`) Phase 2.5 (`:622-626`) calls
`membership.reconcile(now,false).await`, which → `invoke_rebalance_callback`
(`abstract_membership_manager.rs:843`) enqueues
`ConsumerRebalanceListenerCallbackNeeded` then **`ack_rx.await`** (`:870`). The
bg task suspends there and does NOT re-enter Phase 1
(`process_application_events`, `:396`) until `run_once` returns — which it
can't until the ack arrives. Any reentrant op that routes through the bg task
(`assign/seek/pause/resume/position/committed/beginning_offsets` — all go
through `submit_and_drain`, e.g. `pause` at `async_kafka_consumer.rs:3960`)
therefore deadlocks. The sync getters (`assignment/subscription/paused`,
`:1747-1772`) only lock `SubscriptionState` and do NOT need blocker (b)
fixed.

Java does not freeze: `ConsumerNetworkThread.runOnce()` fires the callback,
sets `reconciliation_in_progress`, and **returns**; reconcile is a
`CompletableFuture` chain (`revokeAndAssign(...).whenComplete(...)`). The
network thread keeps spinning — heartbeats, fetches, and the reentrant
application events the listener submits all continue — while the membership
state stays `RECONCILING` until the callback future completes. §31's inline
`ack_rx.await` is the deviation that introduced the freeze.

## Sub-phases (Actor implements in order; Critic reviews after each)

### 41a — `ConsumerHandle` type + wakeup fold-in (blocker a + wakeup)

- Add `pub struct ConsumerHandle` (`Clone`), composed from already-`Arc`-shared
  state: `application_event_handler: Arc<ApplicationEventHandler>`,
  `subscriptions: Arc<Mutex<SubscriptionState>>`,
  `completable_event_reaper: Arc<Mutex<CompletableEventReaper>>`,
  `wakeup_trigger: WakeupTrigger`, `bg_wakeup: Arc<dyn Fn()+Send+Sync>`,
  `time`, `default_api_timeout_ms`. (Mock variant keeps the `Arc<AtomicBool>`
  flag, as `WakeupHandle::for_mock` does today.)
- Methods exposed (the reentrant-safe subset):
  - sync: `wakeup()`, `assignment()`, `subscription()`, `paused()`
  - async: `assign()`, `seek()`/`seek_to_beginning()`/`seek_to_end()`,
    `pause()`, `resume()`, `position()`, `committed()`,
    `beginning_offsets()`, `end_offsets()`, `offsets_for_times()`,
    `commit_sync()`, `commit_async()`
  - NOT exposed: `poll`, `subscribe`, `unsubscribe`, `close` (lifecycle/
    ownership ops; Java does not invoke these reentrantly from callbacks).
- The async handle ops submit an `ApplicationEvent` and await its completion
  **without draining background events** (the handle cannot own
  `background_event_rx`/the invoker — those stay on `&mut self`). They MUST
  still honor wakeup via the `wakeup_trigger` `select!` (mirror the
  `enable_wakeup` arm of `process_background_events_until`). Extract the
  submit+await-with-wakeup core into a helper callable from both `&mut self`
  (which keeps the bg-drain via `submit_and_drain`) and the handle (no drain).
  Do NOT duplicate the deadline/timeout logic.
- Replace `WakeupHandle` → `ConsumerHandle`. Rename
  `Consumer::wakeup_handle()` → `Consumer::handle() -> ConsumerHandle` (trait
  + `AsyncKafkaConsumer` + `MockConsumer` impls + re-export in
  `consumer/mod.rs`). Update the existing 2 wakeup tests to `handle()`.
- §1/§2 trait-surface rules still hold: handle is plain (no `#[async_trait]`
  needed unless its async methods go through `dyn` — they don't; it's a
  concrete struct, so concrete `async fn` is fine and preferred per CLAUDE §11).
- Perf: the handle holds only `Arc`/cheap clones; constructing one is off the
  hot path (per-listener-registration / per-wakeup-setup, not per-record).

### 41b — non-blocking reconcile callback (blocker b)

Rework so the bg loop does NOT block inside `reconcile` awaiting the listener
ack. Translate Java's `revokeAndAssign(...).whenComplete(...)` chain into an
explicit cross-iteration state on the membership manager:

- `invoke_rebalance_callback` no longer `ack_rx.await`s inline. Instead it
  enqueues the callback-needed event, **stores the `oneshot::Receiver` + a
  `CallbackPending { method, partitions, … }` state** on the membership
  manager, and returns so `run_once` can complete.
- `run_once` Phase 2.5 (or `reconcile`'s entry) **`try_recv`s** the stored ack
  receiver each iteration (alloc-free, no blocking). On `Empty` it leaves the
  state machine in `RECONCILING`/`reconciliation_in_progress=true` and returns;
  on `Ok(result)` it resumes the post-callback steps
  (`enable_partitions_awaiting_callback`, the assign/revoke completion, the
  next callback in the revoke→assign sequence, or the failure path).
- The app side, after running the listener and sending the ack (in
  `process_background_events`, `async_kafka_consumer.rs:2418`), **pokes the
  bg-wakeup `Notify`** so the bg loop wakes promptly and observes the ack —
  rather than waiting out the selector poll timeout. Reuse the existing wakeup
  primitive; do NOT shrink `poll_wait_time_ms` (that would busy-spin — see
  Perf Contract).
- Preserve the revoke→assign ordering, `can_commit` semantics, the
  no-listener short-circuit (`:859`), error handling (`maybe_wrap_as_kafka_
  error`, log-and-continue), close-during-callback, and wakeup-during-callback
  behavior. The existing single-callback component tests
  (`consumer_membership_manager.rs` `reconcile_emits_assigned_callback_and_acks`
  etc.) MUST still pass unchanged.
- Update **`consumer-threading.md` §31**: replace "The bg task **awaits** the
  matching `oneshot::Receiver`. Rebalance state does not advance until the
  callback completes." with the non-blocking-loop description (loop continues;
  only the membership *state transition* is gated on the ack). Update the
  anti-pattern bullet accordingly (the real anti-pattern is *advancing the
  state* before the ack — NOT *blocking the loop*). Add the busy-spin
  anti-pattern (forcing `poll_wait_time_ms→0` while a callback is pending).
  This CLAUDE-rule change requires the agent-roles.md process — call it out in
  the COMMENTS.DONE entry; do not change CLAUDE.md itself.

### 41c — un-ignore tests + new regressions + MockConsumer

- Un-ignore the 8 callback tests; rewrite each listener as a named struct
  capturing a `ConsumerHandle` and calling the op the Java test calls. Keep
  the Java assertion shape. (These are integration tests — compile-gated under
  `feature="integration-tests"`, run in CI with Docker, compile-only here.)
- Un-ignore the 2 wakeup-during-`position` tests (now via `handle().wakeup()`).
- Component-level (NON-Docker) tests for blocker (b): (1) bg loop continues a
  reentrant `pause`/`position` submitted from inside a callback to completion
  (the deadlock regression); (2) membership state does NOT advance until the
  listener ack arrives (block the listener on a test-held channel, observe no
  advance, release, observe advance) — this is §31's required test #2; verify
  whether it already exists before adding. Confirm §31's required test #1
  (`commit_sync` from inside revoke) is covered by the un-ignored integration
  test and/or add a component analogue.
- `MockConsumer::handle()` returns a `ConsumerHandle` (mock variant).

## Perf Contract (Critic MUST verify — non-negotiable)

1. **Steady state (no rebalance): zero added cost beyond one branch.** The bg
   loop already calls `reconcile` every iteration; the only addition when no
   callback is pending is a flag/`Option::is_none` check. No new per-iteration
   allocation (CLAUDE §11) — the stored ack receiver is an `Option` field, not
   a per-iteration `Box`. No new `Arc` clone or lock on the steady-state path.
2. **No busy-spin during the callback window.** `poll_wait_time_ms` MUST NOT be
   driven toward 0 while a callback is pending; the selector poll keeps
   blocking normally and is woken by the app-side `Notify` poke when the ack is
   ready. The during-callback CPU goes from "parked/zero" to "normal event-loop
   cadence" (correct, Java-faithful, bounded by the callback's brief duration)
   — but NOT to a hot spin.
3. **Handle construction off the hot path** (per-listener / per-wakeup, never
   per-record). Handle holds only cheap `Arc`/clone state.
4. No `#[async_trait]`/`Pin<Box<dyn Future>>` introduced on any per-record
   path. `ConsumerHandle` is a concrete struct with concrete `async fn`.

## Definition of Done (this phase)

- `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check`
  all green. Integration suite compiles (Docker not required locally; gated as
  the existing suites).
- The 10 previously-ignored tests are un-ignored and pass (integration ones
  verified in CI / by reasoning + compile; the 2 component regressions run
  locally).
- §31 rule text updated and the CLAUDE-rule change flagged per agent-roles.md.
- Perf Contract items 1–4 explicitly checked in the Critic pass.
- No TODO/FIXME; Apache-2.0 headers on new files; comments translated from the
  Java contract where applicable.
- Commit incrementally per sub-phase with clear messages; fixups reference the
  introducing commit when addressing COMMENTS.41.md.

## Design Rationale (end-user view — for the COMMENTS.DONE record)

For an app author expecting a Java-like API, Option A's *only* added
difference vs Java is capturing `consumer.handle()` instead of `consumer`.
Everything else that feels non-Java — `.await` on blocking calls inside a
tokio runtime, `Result<_, KafkaError>` instead of thrown exceptions
(`WakeupException` → `Err(e) where e.is_wakeup()`), `&mut self` making
single-threaded access a compile-time guarantee instead of a runtime
convention, the listener as a named struct + `#[async_trait]` impl instead of
an anonymous inner class, and explicit `close().await` (no try-with-resources,
because `Drop` can't await) — is inherent to async Rust and identical under
options B and C. A keeps the listener trait Java-identical and recovers Java's
shareable-reference capability with the minimum possible surface delta.
