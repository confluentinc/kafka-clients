---
name: phase10-critic-round3-patterns
description: Phase 10 Critic round-3 fix patterns — reaper-side timeout for secondary handles, doc-nit causality reviews
metadata:
  type: feedback
---

Patterns surfaced by Phase 10 Critic N=1 round-3 (covering fixup commits
`d41c649`, `73d7a65`, `910aeb3`, `16e04a7`). One real finding + two doc
nits — pattern below applies to future phases that work with
oneshot::Sender-backed handles or doc comments asserting Java causal
ordering.

## Pattern 1: dropping an un-completed oneshot::Sender is NOT "leave pending forever"

**Why:** R3-1 — `process_commit_async` / `process_commit_sync`
empty-manager arms tried to mirror Java's "leave `offsetsReady`
uncompleted" by doing `let _ = offsets_ready;`. In Java that means the
receiver-side `ConsumerUtils.getResult(offsetsReady, timer)` waits the
user-supplied timeout and then throws `TimeoutException`. In Rust,
dropping the `CompletableEventHandle<T>` drops its `Arc<HandleInner>`
strong ref; when no other strong ref exists, the inner
`oneshot::Sender` is dropped and the receiver resolves with
`RecvError` IMMEDIATELY — not after the deadline.

The previous regression test masked the bug because it captured
`ready_probe = offsets_ready.erased()` BEFORE the variant moved the
handle. The probe held a strong ref, keeping the sender alive, so
`try_recv()` returned `Empty` as expected in the test. Production
had no such probe, so the receiver would see `RecvError` immediately.

**How to apply:** When Java says "future stays pending until the
user-side timer fires", the Rust translation MUST hold a strong ref
to the handle's `Arc<HandleInner>` for the duration of that wait, AND
must complete the inner sender with a `KafkaError::Timeout` when the
deadline elapses. The application-event reaper is the right place to
park such handles: `reaper.add(handle.erased())` keeps a strong ref
AND drives `fail_with_timeout(KafkaError::Timeout(...))` on the next
`reap(now)` call where `now > deadline_ms`. Same `Arc<Mutex<Reaper>>`
the bg-task holds — both production and tests share one instance.

Test rigor: the regression test must (a) verify the reaper now
tracks the secondary handle (`size() == 1`,
`reaper.contains(&ready_probe)` — note `contains` uses `inner_id`,
not `Arc::ptr_eq`, so a freshly-erased probe matches); (b) verify
the receiver is `Empty` before the deadline; (c) call
`reaper.reap(deadline_ms + 1)`; (d) `await` the receiver and assert
the variant is `KafkaError::Timeout(msg)` with the expected
diagnostic substring; (e) verify the reaper drops the entry
afterwards. DON'T rely on a probe strong ref to keep the test
green — the probe must be a witness, not life support.

## Pattern 2: Java causal-ordering claims in comments need source-line citation

**Why:** Round-3 noted two doc nits — the `mm.reconcile(true)`
rationale comments said Java passes `true` "because
`updateTimerAndMaybeCommit` will have run by the time the
membership advances". Java actually calls `maybeReconcile(true)`
BEFORE `updateTimerAndMaybeCommit` (lines 717-718 vs 722). The bool
value passed is correct; the rationale was wrong.

The mistake was that the Rust comment justified Java's behavior in
terms of a different Java site that ran first — but the actual Java
ordering puts that site SECOND. The comment's causal direction was
inverted.

**How to apply:** When writing a comment that says "Java passes X
here because Y happens first", cite the Java file:line for BOTH X
and Y, and verify the line numbers (not just file names) actually
support the ordering claim. If the Java comment block above the
call gives the rationale, quote it. The right shape for
poll-time-entry-points is: "Java passes `true` because this is the
pre-fetch entry point (Java line N) — any pending offsets can be
safely flushed via the auto-commit-before-rebalance path INSIDE
the method, before any new fetching starts." Not: "because the
NEXT step does X."

## Pattern 3: AEP owns reaper too, not just CNT

**Why:** Round-3 R3-1 fix added an `application_event_reaper:
Arc<Mutex<CompletableEventReaper>>` field to
`ApplicationEventProcessor`. Previously only
`ConsumerNetworkThread` held the reaper, registering events via
`process_application_events`. But that registers only the PRIMARY
handle via `event.erased_handle()`. Secondary handles (like the
`offsets_ready` in `CommitAsync` / `CommitSync`) need a separate
registration call from the AEP arm that owns them.

**How to apply:** If an event variant carries a secondary completion
handle (i.e. a second `CompletableEventHandle<U>` beyond the
primary), the AEP arm that processes that variant is the natural
registration point for the secondary's reaper entry. The reaper
must be a shared `Arc<Mutex<...>>` between CNT and AEP — they're
not separate reapers, they're the same instance. The CNT
constructor and the AEP constructor both take it; in tests, the
fixture wires the same `Arc` into both. New field accessors:
production call sites pass the reaper as a new arg; test fixtures
expose it so tests can drive `reap(now)` directly and observe
state.
