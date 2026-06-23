---
name: phase10-critic-round1-patterns
description: Phase 10 Critic round-1 fix patterns — auto-commit lifecycle, Java retry-gate order parity, predicate guards, test name parity
metadata:
  type: feedback
---

Patterns surfaced by Phase 10 Critic N=1 round (commit `d3ac959`)
covering `CommitRequestManager` auto-commit and rebalance-flush paths.

## Pattern 1: don't ship "Phase N will replace this" stubs into Phase N

**Why:** Phase 9 left a comment `// Phase 10 will replace this with the
actual subscriptions.allConsumed() snapshot.` in `maybe_auto_commit_async`.
Phase 10 (commit 2.5/N) wired the missing dependency
(`subscriptions: Arc<Mutex<SubscriptionState>>`) but the next commit at
the same scope (commit 2/N which closed wire-prereq #6) didn't update the
stub — the docstring claimed the wire-prereq was closed, but the body
still flipped the inflight flag and returned without enqueueing a
request. Result: a blocker bug shipped through TWO commits and a Critic
review.

**How to apply:** when fixing a wire-prereq that "depends on Phase N
landing", check ALL phase-marker comments referencing that phase across
the file and remove/translate the stubs, not just the one at the docstring
call site. The stub comment IS the symptom — if it's still there, the
body probably is too. Grep for `Phase {current_phase}` before claiming a
wire-prereq is closed.

## Pattern 2: translate the FULL Java predicate, not just the OR branches

**Why:** Java's
`isStaleEpochErrorAndValidEpochAvailable` requires BOTH conditions
(`StaleMemberEpoch` AND `memberInfo.memberEpoch.isPresent()`). Rust
collapsed this to `err.error() == Errors::StaleMemberEpoch`, dropping
the second guard. Consequence: a consumer that has left the group
(epoch == None) would enter the retry loop on a `StaleMemberEpoch`
response, loop until the deadline expired, and surface `Timeout`
instead of the original `StaleMemberEpoch` error. User-observable
contract differs.

**How to apply:** when translating Java compound predicates (`x && y`,
`(a || b) && c`), translate EVERY conjunct. If the helper method is a
private `is*()` predicate, read it — don't assume it's a one-liner.
Capture state-snapshot conjuncts (`memberInfo.memberEpoch.isPresent()`)
ONCE at driver entry into a local `bool`, since they can't change across
retries within a single async driver invocation. Useful to flag in
Critic reviews: "predicate has N conjuncts in Java but only M in Rust".

## Pattern 3: preserve Java's check ORDER inside retry gates

**Why:** Java's `autoCommitSyncBeforeRebalanceWithRetries` checks:
(1) `requestAttempt.isExpired()` → Timeout, (2) `UnknownTopicOrPartition`
→ fatal, (3) else retry. Rust had UTOP first, so when BOTH conditions
held (deadline past AND UTOP error) the wrong error type surfaced.
The error semantics differ: Java's Timeout wraps the UTOP message;
Rust's raw UTOP carries the topic-deleted semantics. Both are
non-retriable but distinguishable by callers doing
`matches!(err, KafkaError::Timeout(_))`.

**How to apply:** when translating Java if/else-if chains inside a
retry loop, copy the order exactly. Even when individual branches
appear semantically independent, the order is the contract when they
can both fire. Add a parity test that constructs the
double-positive scenario and asserts which branch wins.

## Pattern 4: test names must match what's asserted

**Why:** A test named `entries_returns_managers_in_registration_order`
that only asserts `entries.len() == 5` doesn't protect order — a
regression swapping push positions still passes. Per DoD §3,
assertions must be meaningful enough to fail on regression.

**How to apply:** when you can't easily achieve a strong assertion
(e.g. trait objects of types with only default trait methods aren't
distinguishable at runtime), DOWNGRADE the test name to match what's
actually checked (`entries_returns_correct_count_when_five_slots_populated`).
Don't leave a name that promises more than the body delivers. If
order verification is important, find a per-implementation observable
(distinct `maximum_time_to_wait` returns, or a `pub(crate) fn
debug_name()` hook on the trait).

## Pattern 5: Java's `requestAutoCommit` always resets the timer, even on empty offsets

**Why:** Java's `maybeAutoCommitAsync` calls `resetAutoCommitTimer()`
*after* `requestAutoCommit(requestState)` — which short-circuits when
offsets are empty. So even when no request goes out, the timer is reset
to a fresh interval. Easy to get wrong: a translation that
short-circuits on empty offsets BEFORE the timer reset would never
fire auto-commit when subscriptions are empty.

**How to apply:** when translating Java state machines, look at the
order of calls relative to early-returns. `resetAutoCommitTimer()`
before `if offsets.is_empty() { return }` is what Java does. Watch
for "unconditional side effects after potentially short-circuited
operations".
