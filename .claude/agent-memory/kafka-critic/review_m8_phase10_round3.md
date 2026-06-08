---
name: review-m8-phase10-round3
description: Phase 10 round-3 fixup review patterns — Rust drop semantics vs Java's "never-completed" futures; probe-as-test-only strong-ref smell
metadata:
  type: feedback
---

Patterns surfaced by Phase 10 Critic N=1 round-3 (verifying the 4
fixup commits for R2-1..R2-5). One new finding (R3-1) and a handful
of "audited but not flagged" nits documented in COMMENTS.1.md.

## Pattern 1: `let _ = handle;` is NOT equivalent to Java's "future stays uncompleted"

**Why:** R3-1 — when actor restored Java behavior by replacing
`offsets_ready.complete_exceptionally(err)` with `let _ = offsets_ready;`,
the assumption was "the handle is left un-completed, matches Java".
But Rust's drop semantics force the `oneshot::Sender` to drop the
moment the handle's last `Arc<HandleInner>` strong ref hits zero. The
receiver then resolves with `tokio::sync::oneshot::error::RecvError`
— NOT a hang and NOT a timeout. Java's `CompletableFuture` has no
analog: it stays pending forever until either an explicit completion
or a separate timer fires.

**How to apply:** When the Critic asks to "leave the handle
un-completed to match Java", the actor must ensure that either:
(a) the handle is registered with the reaper so the reaper holds a
strong ref AND will explicitly fail it with `KafkaError::timeout(...)`
on deadline, OR
(b) the call site is documented to ALSO emit `KafkaError::timeout(...)`
directly (no drop-and-wait pattern).
Option (a) gives precise Java parity; option (b) is acceptable if
the caller's app-side path is well-defined.

A pure `let _ = handle;` produces RecvError on the receiver — which
is a third, distinct behavior different from both Java and what the
finding asked for. Flag this in any fix that drops a handle without
either registering it with the reaper or eagerly completing it.

## Pattern 2: test-only `erased()` probes can mask production drop behavior

**Why:** R3-1 — the regression test snapshots
`ready_probe = offsets_ready.erased()` BEFORE the variant moves the
handle. The probe is a CLONED `Arc<HandleInner>`, so when the variant
drops its handle, the inner stays alive (probe holds the second ref).
`try_recv()` then correctly returns `Empty` IN THE TEST. But in
production no probe exists; the receiver gets `Closed`/`RecvError`.

**How to apply:** When reviewing a test that uses `handle.erased()`
or any helper that clones the underlying `Arc<HandleInner>` as a
probe, check whether the probe alters the strong-ref count enough to
mask production drop behavior. If the test's purpose is to assert
"the receiver is in a particular state after handle drop", the test
must NOT hold an extra strong ref via probe. Recommended pattern: a
test-only `is_done_after_drop_probe` that uses a `Weak<HandleInner>`
instead of `Arc<HandleInner>` to observe state without keeping the
inner alive.

## Pattern 3: Java-source-line comments must be verified against the actual Java line numbers and ordering

**Why:** R2-2 fixup comment in
`events/application_event_processor.rs:1185-1192` (and the symmetric
comment in `consumer_membership_manager.rs:478-481`) says "Pass
`can_commit = true`: at this site Java passes `true` because
`updateTimerAndMaybeCommit` will have run by the time the membership
advances". This is causally backwards — Java's
`process(AsyncPollEvent)` at lines 717-722 calls
`consumerMembershipManager.maybeReconcile(true)` BEFORE
`updateTimerAndMaybeCommit(...)`, not after.

**How to apply:** When the actor writes a comment that explains a
parameter value via Java's ordering, the Critic must open Java's
source at the cited line and verify the ordering. The behavior here
is still correct (the bool value IS `true`), but the comment's
reasoning is wrong and will mislead future readers. Flag as a doc
nit even when the behavior is right.

## Pattern 4: regression tests for "clear-all" operations are necessarily imprecise

**Why:** R2-4 — `clear_transient_topics()` clears the entire set.
The regression test asserts the set is empty after `fetch_offsets`
completes. This catches "did we call clear at all?" but cannot
distinguish "called once" from "called twice". Without the
defense-in-depth double-fire guard in `apply_partial_result` /
`fail_request_state` (`if guard.completed { return; }`), the test
would still pass for both correct and incorrect call counts.

**How to apply:** Document the defensive guard explicitly when the
test alone cannot pin the contract. Prefer "clear only my keys"
APIs (`clear_transient_topics_for(topics: &HashSet<String>)`) over
"clear all" APIs when precision matters — though in this case Java's
own API is "clear all", so the imprecision is intentional and
mirrored.

## Pattern 5: a split_off-based boundary in a test-only path needs a fixture sanity check

**Why:** R2-1 — `membership_boundary()` returns `count(coordinator) +
count(commit) + count(consumer_heartbeat)`. In production all three
are typically `Some`, so boundary=3. In `with_dyn_managers` (test-only),
all three are `None`, so boundary=0 and ALL dyn managers go to the
"after-membership" half. This means `membership.reconcile()` is
called BEFORE any dyn manager polls in tests. Existing tests assert
call counts, not order, so they pass — but the test fixture's
implicit ordering is now different from what the fixture's authors
may have assumed.

**How to apply:** When a refactor introduces a boundary into a walk
that test fixtures interact with, audit the test fixtures
specifically for whether they (a) assume a specific order and (b)
would silently pass with a different order. If they do, either fix
the fixture to use the production-equivalent slot population or
document the test-only divergence.
