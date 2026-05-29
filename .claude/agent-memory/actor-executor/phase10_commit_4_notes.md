---
name: phase10-commit-4-notes
description: Milestone-8 Phase 10 commit 4/N — ApplicationEventProcessor sync-dispatch table; patterns for deferred-async-arm wiring, RequestManagers ownership choice, and Arc<Mutex<RequestManagers>> lock discipline
metadata:
  type: project
---

# Phase 10 (4/N): ApplicationEventProcessor — dispatch table + non-async arms

## What landed

`src/consumer/internals/events/application_event_processor.rs` — Phase-10
first landing of [[application-event-processor]]. Sync arms only this
commit; async-dispatch arms (AsyncPoll, Commit*, FetchCommittedOffsets,
ListOffsets, CheckAndUpdatePositions, TopicMetadata, AllTopicsMetadata,
Unsubscribe, CreateFetchRequests, LeaveGroupOnClose,
ConsumerRebalanceListenerCallbackCompleted) deferred to commit 5/N.

## Patterns worth recording

### 1. Deferred-async-arm wiring (CLAUDE.md §5 "no TODO/FIXME")

PLAN.md splits the processor across commits 4 (sync) and 5 (async). To
satisfy "no TODO" while keeping commit 4 independently buildable, each
deferred-async arm fails its handle with
`KafkaError::unsupported_version("X is wired in Phase 10 commit 5/N
(async-dispatch arms)")`. This:

- Surfaces an explicit error to the app side rather than silently
  dropping the handle (§28 of `consumer-threading.md`).
- Will be replaced wholesale in commit 5 — the dispatch-arm body is the
  only thing that changes; the surrounding tree of `match` cases stays.
- Lets commit 4's reviewer reason about the sync arms in isolation.

The commit message explicitly enumerates which arms are deferred so a
critic doesn't flag them as missing.

### 2. Java event-variants with missing fields → ADD the field, don't fork

Java's `SubscriptionChangeEvent` superclass carries
`Optional<ConsumerRebalanceListener> listener`. Three Rust variants
(`TopicSubscriptionChange`, `TopicPatternSubscriptionChange`,
`TopicRe2JPatternSubscriptionChange`) were translated in Phase 5 without
this field — there were no construction sites yet, so the gap went
unnoticed. Commit 4 adds the field as
`listener: Option<Arc<dyn ConsumerRebalanceListener>>` to each variant.

§28 says don't drop Java fields. The right action when an existing
variant is missing a Java field is to **add the field**, not to "invent
a workaround" or to defer.

### 3. `RequestManagers` ownership pattern

Java holds `RequestManagers` as a plain field on the bg thread. Rust
needs `&mut` access from both `ApplicationEventProcessor::process` and
the bg-task's `run_once` loop, so the container is wrapped in
`Arc<Mutex<RequestManagers>>`.

Lock discipline (extends §16 to `RequestManagers`):

- The mutex is held briefly on the bg task only (single-threaded
  acquisition), so contention is nil.
- Never call `.await` while holding the guard.
- Don't nest locks (`request_managers` → `subscriptions` etc.) unless
  the inner manager is known not to lock back into the outer.

### 4. Missing methods: don't auto-add unless invoked this commit

`ConsumerMembershipManager::consumer_rebalance_listener_callback_completed`
exists in Java but not in Rust. The dispatch arm
`ConsumerRebalanceListenerCallbackCompleted` would call it. Because the
full §31 bidirectional handshake (callback-needed event +
pending-future map on the membership manager) isn't wired until commit
5, this commit's arm currently only logs a warning. The membership
method will be added in commit 5 alongside the wiring.

Avoided: adding the membership method here as a stub. Would have
required either (a) a no-op that's incorrect, or (b) a half-correct
implementation that commit 5 has to rewrite. Better to defer cleanly.

### 5. `is_closing()` accessors on managers for test verification

Java tests use `Mockito.verify(commitManager).signalClose()`. Rust
can't verify trait-method invocations the same way, so we observed
the side effect instead — added `is_closing(&self) -> bool` accessors
on both `CommitRequestManager` and `CoordinatorRequestManager` that
mirror the observable side of `signal_close()`.

### 6. `Cluster::topics()` returns `impl Iterator<Item = &str>`

Not `Vec<String>` or `HashSet<String>` — the natural translation of
`cluster.topics().stream()` is `cluster.topics().filter(...)`. Don't
call `.iter()` on it (you get the wrong error message about `Headers`).

### 7. `metadata.request_update_for_new_topics()` returns the CURRENT
version (does NOT bump `update_version`)

Confusingly named — it bumps `request_version`, not `update_version`.
A Java test that does `when(metadata.requestUpdateForNewTopics()).thenReturn(1)`
and asserts `processor.metadataVersionSnapshot() == 1` is testing that
the processor *recorded* the return value, not that the version
*advanced*. Translate the test as "snapshot equals current
update_version", not "snapshot > initial".

### 8. `clippy::let_underscore_future` on `oneshot::Receiver`

`fetch_offsets` returns `oneshot::Receiver<...>` which IS a `Future`.
`let _ = offsets_mgr.fetch_offsets(...)` trips `let_underscore_future`.
Use `std::mem::drop(offsets_mgr.fetch_offsets(...))` for the
fire-and-forget pattern (Java's `metadata.fetchOffsets(ts, false)` not
chaining `.whenComplete`).

## Tests

- 23 inline tests covering every sync arm (assignment change with/without
  group id, with exception; reset, seek, seek-with-exception; pause,
  resume; subscription changes — concrete, regex, RE2J — with success +
  illegal-state; current lag; commit-on-close; stop-find-coordinator;
  dispatch-table sanity; deferred-async-arm sanity).
- Java tests targeting async arms (commit-sync/async, fetch-committed,
  async-poll, etc.) are deferred to commit 6/N alongside the test
  translation pass.

## Test fixture pattern

The Java `setupProcessor(boolean withGroupId)` translates to a Rust
`Fixture` struct that holds the `processor`, `request_managers`,
`metadata`, and `subscriptions`. With group-id wires up all the
managers; without group-id leaves the group-only slots `None`.

The heartbeat manager wires its OWN coordinator (`Arc<Mutex<...>>`),
distinct from the owned `RequestManagers.coordinator` slot. This
duplication is OK for sync-arm tests because the processor never
reaches the heartbeat-side coordinator — phase 11 / commit 7 will
reconcile the two ownership patterns when the bg-task wiring lands.
