---
name: m8-phase10-round2-patterns
description: M8 Phase-10 round-2 (AEP + ConsumerNetworkThread) review patterns — phase-ordering invariants, missed `canCommit` predicate, exception-cache scope mismatch, transient-topic leak
metadata:
  type: project
---

# Phase-10 round-2 critic patterns (commits 3a–9/N)

These patterns surfaced reviewing the
`ApplicationEventProcessor` + `ConsumerNetworkThread` translations
against `OffsetsRequestManager.java` and `ConsumerNetworkThread.java`.

## 1. "Membership skipped from entries() — drive it elsewhere" introduces a phase-ordering risk

Round 1's aside flagged that Rust's `RequestManagers::entries()`
intentionally skips `ConsumerMembershipManager` and the bg task must
drive `reconcile()` directly. The round-2 wiring puts the reconcile
call AS A SEPARATE PHASE (between Phase 2 entries-poll and Phase 4
network-poll), but Java's order interleaves membership BETWEEN
heartbeat and offsets WITHIN the entries-poll loop.

**Detection heuristic:** When an entries-poll loop has a manager
skipped with the rationale "drive elsewhere", check the **positional
invariant** Java relies on, not just "did we call it once per
iteration". Java's `entries()` order is load-bearing — managers later
in the list see state mutated by earlier ones in the SAME iteration.

**Concrete heuristic:** Read `RequestManagers.java` constructor's
`list.add(...)` sequence. Any Rust translation that splits this into
"main loop + tail call" is suspect unless the tail call's manager
sits LAST in the Java list. Membership is NOT last in Java's list.

## 2. `maybeReconcile(boolean canCommit)` — the predicate often gets dropped

Java has two callers:
- `entries().poll()` → `maybeReconcile(false)` (skip when autoCommit
  enabled and offsets not safe to commit).
- `process(AsyncPollEvent)` → `maybeReconcile(true)` (proceed because
  `updateTimerAndMaybeCommit` just ran).

When the Rust signature is `reconcile(&self, current_time_ms: i64)`
with no `can_commit` parameter, both callers collapse onto the
permissive (true-equivalent) path. Trap to look for: a reconcile body
with no `if (autoCommitEnabled && !canCommit) return;` short-circuit.

**Pattern:** Whenever a Java method's signature has a `boolean` /
`enum` parameter that gates an early return, ensure the Rust
translation either:
- Carries the parameter through, OR
- Documents which branch is "the chosen behavior" and why the other
  branch is acceptable to drop.

A silent collapse is a real divergence even if the code "looks fine".

## 3. Exception-cache scope mismatch in `update_fetch_positions`

Java's `cacheExceptionIfEventExpired` is registered ONLY on the inner
result of `updatePositionsWithOffsets`. The outer `updateFetchPositions`
result has NO caching hook. The outer try-catch (which catches
`validatePositionsIfNeeded()`-thrown errors) therefore completes the
result exceptionally WITHOUT caching.

A Rust translation that collapses both layers into a single
"on-error: cache + send-Err" path causes synchronous validate errors
to be cached and re-delivered next call — double-delivery.

**Detection heuristic:** When Java has nested CompletableFuture
chains where the `whenComplete` is registered on the INNER future
(not the outermost result), check the Rust translation places the
side-effect ONLY on the inner future's resolution path. If the outer
try/catch in Java does NOT register the hook, the Rust outer-error
path must NOT trigger the side-effect either.

## 4. `clearTransientTopics` after `fetchOffsets` is easy to drop

Java's `OffsetsRequestManager.fetchOffsets` registers a `whenComplete`
on the global result that calls `metadata.clearTransientTopics()`.
The Rust translation can comfortably forget this — there's no
compiler signal, no test failure (because typical tests build one
manager per test), and the inline rationale "transient topics are
eventually consistent" sounds plausible.

But over the lifetime of a long-lived consumer, every `endOffsets` /
`offsetsForTimes` / `currentLag` adds topics that never get cleared.

**Pattern:** When a Java method has a one-time global side effect at
the END of an async chain (`whenComplete` calling a cleanup), the
Rust translation must wire it into BOTH the success-completion path
AND the failure-completion path. Java's `whenComplete` fires on both.

## 5. "Eager-fail both handles" is plausible but needs a pinning test

When Java leaves a secondary handle un-completed in an error branch
(relying on the app-side `getResult(handle, timeoutMs)` to surface a
TimeoutException), the Rust temptation is to eagerly fail both
handles for cleaner error surface. The deviation may even be an
improvement.

But without a regression test that asserts the secondary handle's
state, subsequent reviewers / refactors can silently revert. Flag
the absence of the test, not the design choice.

## 6. Reviewer checklist for AEP-style dispatch tables

For each `process_*` arm in a translated `ApplicationEventProcessor`:

1. Find the Java `process(XxxEvent)` method.
2. Walk every branch (try-block lines, catch-block, finally) and
   verify the Rust covers each.
3. Cross-check Java's `whenComplete` chains: each chain registers a
   callback on a SPECIFIC future — the Rust translation must trigger
   the equivalent side-effect on the SAME future's completion, not
   on the outer event's completion.
4. Verify the empty-manager / missing-dependency branch: Java often
   has subtle behavior (incomplete future leading to API timeout)
   that the Rust eager-fail collapses.
5. For each `markXxxComplete()` call, verify timing vs the
   surrounding chain — Java places these synchronously between the
   manager call and the `whenComplete`; Rust must do the same.
