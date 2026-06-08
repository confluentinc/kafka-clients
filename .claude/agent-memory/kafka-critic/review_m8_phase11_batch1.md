---
name: review-m8-phase11-batch1
description: Patterns from reviewing Phase-11 batch 1 (commits 1-3) — AsyncKafkaConsumer struct + subscribe/unsubscribe/assign + §31 skeleton. Covers gotchas for the next batch reviews.
metadata:
  type: reference
---

# Phase-11 batch-1 review patterns

## Recurring Java contract drops that look harmless but aren't

### `acquireAndEnsureOpen` is NOT just the reentrancy guard

Java's `acquireAndEnsureOpen()` does TWO things:
1. The `currentThread`/`refCount` reentrancy check — PLAN deferral #4 drops
   this rationally (`&mut self` enforces single-caller at compile time).
2. The `if (this.closed) throw IllegalStateException` check — this is NOT
   subsumed by `&mut self`. Methods that drop "acquireAndEnsureOpen" silently
   drop BOTH.

When auditing any state-read or async method, check if the Java equivalent
calls `acquireAndEnsureOpen()`. If yes, the Rust translation should call
`ensure_open()` (the closed-check half). Sync `fn` methods that return
non-`Result` types can't surface the error — either:
- panic on `is_closed()` (Java-faithful), or
- explicitly document the divergence and add a skip-rationale for the
  matching Java test.

**Affected methods in commit 2/N:** `assignment`, `subscription`, `paused`,
`client_id`, `current_lag`, `group_metadata`. Filed as Issue 3.

### `throwIfGroupIdNotDefined` inside `groupMetadata()`

Java's `groupMetadata()` calls `throwIfGroupIdNotDefined` (Java line 1431).
Rust currently returns a stub for groupless consumers. The Java test
`testGroupMetadataAfterCreationWithGroupIdIsNull` asserts the exception is
thrown. Either translate that test as a skip with rationale OR change the
trait surface. Filed as Issue 4.

## §31 invocation patterns to verify in remaining batches

### `process_background_events` must be called at the TOP of every blocking-style API

PLAN.md §11.2 lists: `poll`, `commit_sync`, `unsubscribe`, `close`,
`position`, `committed`, `beginning_offsets`, `end_offsets`,
`offsets_for_times`. NOT in the list: `subscribe`, `assign`. So subscribe/assign
not calling it in commit 3 is correct.

### `process_background_events` is ONE-SHOT in commit 3

Java's `processBackgroundEvents(future, timer, ignoreErrorPredicate)` is the
iterative loop. Rust commit-3 `unsubscribe()` brackets `add_and_get.await`
with two single-shot drains. This creates a DEADLOCK risk if the bg-task
posts a `ConsumerRebalanceListenerCallbackNeeded` envelope DURING the unsubscribe
rebalance — the ack never gets sent. Commit-4 lands the iterative loop.

**Verification check for commit 4:** look for
`process_background_events_until(future, deadline)` or equivalent.

### `backgroundEventReaper.reap(time)` is part of `processBackgroundEvents`

Java line 2222. Rust skipped it in commit 3. Verify commit 4 lands it.

## Test translation gotchas

### Java's `assertEquals("exact-message", t.getMessage())` requires exact match in Rust

DoD §3 explicitly demands this. Avoid `msg.contains("substring")` —
production typos slip through.

### Skip rationale must list ALL untranslated Java tests

If a Java test surface has 4 assertions and Rust translates 1, the OTHER 3
need a line each in the skip section even if the rationale is "Rust type
system unrepresentable" (null Arc, null Vec elements, etc.). The current
batch missed listing some null-listener / does-not-throw cases from
`testSubscribeToRe2JPatternValidation`.

### Test names must match what the test body asserts

`assign_generates_assignment_change_event_and_clears_subscription` — name
promises a clear-subscription assertion the body doesn't make. Either
rename or add the assertion.

## State-store-before-confirm pattern

When the Rust impl mirrors a Java "all-or-nothing" event-driven change
(e.g. subscribe stores listener inside the event), the Rust may add a
parallel app-side state field (e.g. `self.rebalance_listener`) for
`process_background_events` to read. The store-then-await ordering matters:
if `add_and_get` fails, the app-side state is half-updated. Move the
store AFTER the await. Filed as Issue 8.

## Translation patterns OK to keep — do NOT flag

- `Box<dyn Fn() + Send + Sync>` closures for type-erasing the
  `ConsumerNetworkThread<K>` lifecycle hooks (`signal_close`, `wakeup`,
  `await_join`) — legitimate Rust-side adaptation, no Java equivalent.
- `AsyncKafkaConsumerComponents` bundle struct — Java has 20-arg test ctor,
  Rust bundles them. Idiomatic.
- Passing the listener per-call to the invoker (vs. Java's stored field) —
  documented divergence, sound rationale.
- Returning owned `HashSet<...>` from `assignment` / `subscription` /
  `paused` instead of Java's `Collections.unmodifiableSet(...)` — Rust
  idiomatic. Flag only as informational (Issue 9 in batch 1).

## Heuristic for "is this a real bug or a documented seam?"

If the Actor's commit message AND the in-source doc-comment AND the PLAN.md
all reference the same "lands in commit X" deferral, it's a documented seam.
Flag as a verify-in-commit-X note, not as a blocking bug. If only ONE of
those mentions it, dig deeper — likely a real gap.
