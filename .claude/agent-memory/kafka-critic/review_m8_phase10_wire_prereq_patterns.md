---
name: m8-phase10-wire-prereq-patterns
description: M8 Phase-10 wire-prereqs review — patterns for "outer hook closed, inner driver still stubbed"; Java retry-gate predicate translation; order-of-check parity
metadata:
  type: project
---

# Phase-10 wire-prereqs (commits 1/N — 2.5/N) critic patterns

## 1. "Outer hook + inner stub" — wire-prereq false-closes

When a Phase plan lists a hook like
`CommitRequestManager::updateTimerAndMaybeCommit` as a wire-prereq, the
actor may translate the *outer signature* (the new `pub(crate) fn` the
processor calls) without translating the *inner body* that does the
actual side effect. The compile-and-test gate passes because tests
target the visible side effect (timer reset, flag flip) and not the
non-visible side effect (request enqueued onto a pending queue).

**Detection heuristic:** When an actor claims a wire-prereq is "closed,"
re-read the Java method body line-by-line and grep the Rust for every
side effect Java has. If Java calls `requestAutoCommit(requestState)`
which calls `pendingRequests.addOffsetCommitRequest(...)`, the Rust
must reach a `pending.unsent_offset_commits.push_back(...)`. If a test
exists but only asserts the most visible side effect, it is not a
regression guard.

**Concrete miss in this batch:** `maybe_auto_commit_async`
(`commit_request_manager.rs:1031-1057`) still emits no request because
the inline comment "We don't have `SubscriptionState` plumbed in for
Phase 9 yet" was not updated when 2.5/N plumbed it in. The
`update_timer_and_maybe_commit` hook is a thin pass-through to this
stub. **Wire-prereq #6 is therefore not actually closed** despite the
commit message claim.

## 2. Java `isStaleEpochErrorAndValidEpochAvailable` predicate

Java has a 2-clause predicate:

```java
return error instanceof StaleMemberEpochException
    && memberInfo.memberEpoch.isPresent();
```

Easy to miss the `memberEpoch.isPresent()` arm when translating. The
predicate gates retry inside `autoCommitSyncBeforeRebalanceWithRetries`
and `fetchOffsetsWithRetries` (Java lines 349, 559).

If translated as just `err.error() == Errors::StaleMemberEpoch`, the
retry loop runs even when `member_info.member_epoch == None`, eventually
surfacing `Timeout` instead of the original error. Edge case but a
real divergence — the consumer that has left the group should not
retry.

## 3. Order-of-check parity in retry gates

Java:

```java
if (error instanceof RetriableException || isStaleEpochError(error)) {
    if (requestAttempt.isExpired())          { ... timeout }
    else if (error instanceof UnknownTopicOrPartitionException) { ... fatal }
    else                                      { ... retry }
}
```

Easy to translate as:

```rust
if !retriable_for_rebalance       { ... }      // OK
if err == UnknownTopicOrPartition { ... fatal }  // SWAPPED with deadline
current_time_ms += backoff;
if current_time_ms >= deadline_ms { ... timeout }
// retry
```

The check order matters when **both** "expired" AND
`UnknownTopicOrPartition` are true: Java surfaces wrapped Timeout, Rust
surfaces the original error. Subtle but real.

**Sub-pattern:** when the Java check is `requestAttempt.isExpired()`
(state-keyed) and the Rust uses a local advancing `current_time_ms`
counter, the two are NOT equivalent. The Rust version checks the
counter *after* a fake-clock bump; Java's check is independent of the
retry backoff window.

## 4. Test name promises more than the body checks

Test `entries_returns_managers_in_registration_order` asserts only
`entries.len() == 5`. No per-slot identity check. A regression that
swaps the `if let Some(...) list.push(...)` order would still pass.

**Pattern:** when reviewing tests, read the asserts before reading the
name. If the name promises an order/property check but the assertions
only verify count/presence, file as a test-rigor nit per DoD §3.

## 5. Stale comment as defect smell

The inline comment "Phase 10 will replace this with the actual
`subscriptions.allConsumed()` snapshot" in `maybe_auto_commit_async`
is the symptom of the #1 issue above. When grep'ing a Phase-N
diff, search for "Phase N will replace" / "deferred to Phase N" /
"Phase N+1 wires" comments to find missed work in the same phase.

## Critic action items

When reviewing Phase-10-style "wire-prereq closure" commits:

1. For each prereq claimed closed, find the Java method body and walk
   each side effect — confirm the Rust touches the equivalent state.
2. Grep for the prior phase's deferral comments
   ("Phase N will plumb", "deferred to Phase N+1") inside the modified
   files; any such comment that's still present is suspect.
3. For retry / classification gates, copy the Java if/else ladder
   verbatim and diff against the Rust ladder line-by-line.
4. Tests added for "wire-prereq closure" commits often only assert the
   visible side effect; cross-check the assertion against the full
   Java side-effect set.
