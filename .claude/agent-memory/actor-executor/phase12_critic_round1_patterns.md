---
name: phase12-critic-round1-patterns
description: Phase-12 Critic round-1 fix patterns — bg-task RM polling parity, single-Arc state-notifier, shared Arc<AtomicI64>, &self via interior mutability
metadata:
  type: feedback
---

Phase 12 Critic round-1 review (commits 1-3) surfaced four patterns
worth carrying forward:

## 1. Bg-task RM polling parity with Java's `entries()`

**Rule**: every Java `RequestManager` subclass MUST be polled per
`run_once`. If the Rust `RequestManagers::entries()` skips a slot (for
Arc-sharing reasons), the bg-task `run_once` MUST drive that slot
explicitly between the appropriate before/after-membership entries.

**Why**: Phase-12 Critic Issue 1 — `CommitRequestManager` was
Arc-shared (skipped from `entries()`), and the bg-task only polled
`coordinator` explicitly. Net effect: `commit_sync()`, `committed()`,
async-commit, and auto-commit all hung in production because
`unsent_offset_commits` / `unsent_offset_fetches` were never drained.
The dyn-trait `CommitRequestManager::poll` stub returning
`PollResult::empty()` made the bug invisible until end-to-end testing
would have flagged it.

**How to apply**: when a `RequestManager` is held as `Arc<X>` rather
than owned, audit `run_once` for explicit driving. Comments in
`request_managers.rs::entries()` should list every skipped slot AND
where it's driven explicitly. Mirror Java's `entries()` order
(`coordinator → commit → heartbeat → membership → offsets → ...`).

## 2. Single-Arc state-notifier consolidation via components struct

**Rule**: when a Java field is conceptually `AtomicReference<T>` shared
between a listener (bg-side writer) and an accessor (app-side reader),
the Rust translation MUST build the backing `Arc<Mutex<T>>` ONCE and
thread it through both registration sites.

**Why**: Phase-12 Critic Issue 2 — production ctor and
`new_with_components` each built their OWN `ConsumerStateNotifier` with
its OWN backing Arcs. Writes via the bg-task-registered notifier never
reached the slot the app-side accessor read.

**How to apply**: when a struct has BOTH a "constructed by tests" path
(via a components struct) AND a "constructed in production" path,
prefer making the shared Arc fields **mandatory** on the components
struct. The type system enforces "build once, thread through" — no
"option (b) defer to None for test path", because that re-opens the
gap. Test fixtures pay a small cost (build the Arcs locally) for the
discipline.

## 3. Shared `Arc<AtomicI64>` via ctor parameter

**Rule**: when an `AtomicI64` slot is written by one task (bg) and read
by another (app), pass the `Arc<AtomicI64>` into BOTH ctors. Do NOT
let either ctor construct its own — one will silently get a fresh
disconnected cell.

**Why**: Phase-12 Critic Issue 3 — `ConsumerNetworkThread::new`
internally constructed
`Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))` while the production
ctor constructed `Arc::new(AtomicI64::new(0))` for the consumer
struct. Two cells, never synchronized.

**How to apply**: signature pattern is
`fn new(..., shared_slot: Arc<AtomicI64>)`. The ctor MAY seed the slot
with a default (e.g. `MAX_POLL_TIMEOUT_MS`) but MUST NOT re-allocate.
This is the same pattern as `Arc<Mutex<...>>` shared state — extends
to atomics that are read across task boundaries.

## 4. `&self` on methods that mutate only via interior mutability

**Rule**: if a method's only "mutation" is through `Mutex`-guarded
fields on an `Arc<Inner>`, the signature SHOULD be `&self`, not
`&mut self`. This lets the method be called through shared `Arc<...>`
handles without exclusive ownership.

**Why**: Phase-12 Critic Issue 1 fix —
`CommitRequestManager::poll_with_coordinator` was `&mut self`, which
prevented the bg-task from calling it through
`RequestManagers::commit_handle() -> Option<Arc<CommitRequestManager>>`
without `Arc::get_mut` (only works when refcount = 1). Changing to
`&self` (semantically correct given interior mutability) unblocks
shared-handle dispatch with zero behavioral change.

**How to apply**: when reviewing a method on `Self { inner: Arc<Inner> }`
where `Inner` uses `Mutex`/`RwLock`/atomics, ask "does this method
actually need to mutate `self` directly?". If not, prefer `&self`.
Six `let mut manager` in existing tests no longer need `mut` — a
mild sign that the original `&mut self` was over-restrictive.

## Bonus pattern: Java `pollOnClose` parity for skipped slots

If a slot is skipped from `entries()`, both `run_once` AND `cleanup`
must drive it. Phase-12 added
`commit.drain_pending_offset_commit_requests()` in `cleanup()`
(mirroring Java's `CommitRequestManager.pollOnClose` →
`drainPendingOffsetCommitRequests`) alongside the `run_once` wiring.
The `poll_on_close` dyn-trait stub returning `PollResult::empty()` is
not enough — close-path commits would silently disappear without the
explicit drive.

Related: [[phase10_consolidated_patterns]] §3 — Arc<Mutex<RequestManagers>>
discipline, run_once 7-phase ordering — extends to "every skipped slot
needs explicit drive in BOTH run_once AND cleanup".
