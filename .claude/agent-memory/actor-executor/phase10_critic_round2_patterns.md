---
name: phase10-critic-round2-patterns
description: Phase 10 Critic round-2 fix patterns — Java entries() positional invariants, canCommit gates, whenComplete-on-both-branches hooks, test-only erased-handle probes
metadata:
  type: feedback
---

Patterns surfaced by Phase 10 Critic N=1 round-2 (covering commits
3a–9). Five findings, four fixups. Each pattern below is one
load-bearing rule for future phase work.

## Pattern 1: positional invariants inside Java's `entries()` walk are load-bearing

**Why:** R2-1 — `membership.reconcile()` was placed AFTER all entries
poll, not BETWEEN heartbeat and offsets as Java does. The semantic
difference: a reconcile may transition the subscription state, and
Java's `offsets.poll()` / `fetch.poll()` then observe the post-reconcile
state in the SAME iteration. Rust's misplacement delayed that visibility
by one iteration.

**How to apply:** When a Rust manager iterates Java's
`RequestManagers.entries()` and SKIPS a slot (e.g. membership, held as
`Arc` shared with heartbeat), the side-effect of that slot's
`poll(now)` body MUST run at the Java-equivalent position, NOT trailing
the iteration. Add a boundary accessor (`membership_boundary()`) and
`split_off()` the collected `PollResult` vec at that index. The "before"
half feeds `add_all` first, then the skipped slot's side-effect runs,
then the "after" half. Don't append the side-effect to the end of the
loop — that's a one-iteration semantic regression.

## Pattern 2: Java's boolean gate parameters are part of the public contract

**Why:** R2-2 — `ConsumerMembershipManager::reconcile(now)` had no
`can_commit: bool` parameter. Java's
`AbstractMembershipManager.maybeReconcile(boolean canCommit)` has it
because `entries().poll()` passes `false` and `process(AsyncPollEvent)`
passes `true`. Collapsing the parameter into a single signature meant
the per-iteration call could advance reconciliation with un-committed
offsets — exactly what the gate exists to prevent.

**How to apply:** Don't drop a Java boolean parameter "because the Rust
call sites all happen to pass the same value today". Trace each call
site and confirm. The asymmetry is the contract. The gate
`if auto_commit_enabled && !can_commit { return Ok(()); }` lives
between the short-circuit-ACK arm and `mark_reconciliation_in_progress`
to mirror Java's source location.

## Pattern 3: synchronous error paths in async-mirrored code must not invent caching that Java does not register

**Why:** R2-3 — `update_fetch_positions` outer sync error path called
`maybe_cache_update_positions_exception`, but Java's outer
`catch (Exception e)` only calls
`result.completeExceptionally(maybeWrapAsKafkaException(e))` and does
NOT register a `whenComplete` hook. The `cacheExceptionIfEventExpired`
hook is registered exclusively inside `updatePositionsWithOffsets`
(the inner async path). Caching from the outer catch caused
double-delivery: caller saw the error this call AND next call.

**How to apply:** Translating Java's `try { ... } catch (Exception e)
{ result.completeExceptionally(...); }` outer block: the Rust
equivalent is `match inner_result { Err((tx, err)) => tx.send(Err(err)) }`
— nothing more. The `whenComplete` hooks Java registers on inner
futures translate to inline blocks in the spawned-continuation path;
they MUST NOT execute on the synchronous error fall-through. If a
test-only path needs to seed the cache, do it via a `pub(crate) fn
set_cached_*_for_test(...)` helper, not by hijacking the production
caching path.

## Pattern 4: `whenComplete` fires on BOTH success and failure — wire both

**Why:** R2-4 — `metadata.clear_transient_topics()` was wired into
neither completion path. Java's `whenComplete((result, error) -> {
metadata.clearTransientTopics(); ... })` fires on success AND on
failure. Skipping it caused unbounded transient-topic growth across
the consumer lifetime.

**How to apply:** When translating a Java `whenComplete` hook, the
Rust equivalent has TWO completion sites: the success branch (route
waiters Ok, then run the hook body) and the failure branch (route
waiters Err, then run the hook body). If `fail_request_state` is a
free `fn` that doesn't have access to whatever the hook needs (here:
`metadata`), promote it to a method taking `&Arc<Self>` (or
equivalent) — don't leave the failure branch unhooked because of a
signature inconvenience.

## Pattern 5: intentional deviations from Java need both a code comment AND a regression test

**Why:** R2-5 — the AEP empty-commit-manager branch eagerly failed
BOTH the primary handle AND `offsets_ready`. The deviation was
documented inline as "strictly faithful to Java's contract for the
primary while avoiding the timeout dance for the secondary", but no
test pinned the dual-fail. The Critic flagged that a future change
could silently revert to Java behavior, and the comment alone could
not catch it.

**How to apply:** Two-step rule when intentionally deviating from
Java: (a) inline code comment explaining the deviation AND why it is
safe; (b) a regression test that asserts the deviation explicitly
(not just the primary effect). For variant-completion contracts, this
means: snapshot the `erased()` of the to-be-moved handle BEFORE the
variant moves it, then assert `is_done()` / `!is_done()` on the
probe plus `try_recv() == Empty` on the receiver. The resolution
for R2-5 was actually to UNDO the deviation (Java behavior wins), but
the pattern still applies for any future deviation.

## Pattern 6: deleting dead code helpers when their last caller goes away

**Why:** R2-3 fix removed the only caller of
`maybe_cache_update_positions_exception` (the sync error path).
Leaving the helper as dead code would invite a future caller to
mis-use it.

**How to apply:** When a bug fix removes the last production caller
of a private helper, delete the helper too. If the spawned
continuation still uses the same logic, inline it where it lives now
(the spawn body already does the lock + write inline; the helper was
duplicating that code). Net result: one less surface for the next
bug to land on. Replace with a one-line comment block that points to
where the equivalent code lives now (so the next reader of Java's
`cacheExceptionIfEventExpired` can find the Rust equivalent).
