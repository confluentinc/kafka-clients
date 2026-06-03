---
name: review-m8-phase8b-patterns
description: Patterns and pitfalls from Milestone-8 Phase 8b critic review (membership + heartbeat managers, §31 reconcile)
metadata:
  type: feedback
---

# M8 Phase 8b review patterns — membership manager + heartbeat manager + §31 reconcile

## §31 listener-presence check is mandatory or contract is broken

When translating `ConsumerMembershipManager.invokeOnPartitionsXxxCallback`,
Java guards with `if (listener.isPresent())` BEFORE enqueueing the
`CompletableFuture`-style event. If Rust drops this guard and always
enqueues to the `oneshot::Sender<Result<...>>`-backed event channel,
the bg task will hang on `ack_rx.await` whenever no listener is
registered — because nothing on the app side will send `Ok(())`.

**How to audit**: in `reconcile` / `transition_to_fenced` /
`transition_to_fatal`, grep for `invoke_rebalance_callback` and check
that EITHER (a) the call site has a `subscriptions.rebalance_listener().is_some()`
guard, OR (b) the implementation of `invoke_rebalance_callback` itself
short-circuits and returns `Ok(())` when no listener is registered.
Option (b) is acceptable but must be documented in §31.

## Listener `Err` ≠ rebalance success in Java

Java's `userCallbackResult.whenComplete(...)` propagates the failure
into `revocationResult.completeExceptionally(callbackError)`, halting
the reconciliation chain. A Rust translation that catches the
listener-Err and returns `Ok(())` from `reconcile` silently masks the
failure — the bg task has no way to know.

**How to audit**: in `reconcile`, every place that awaits
`invoke_rebalance_callback(...)`, on `Err(e)`, the function should
either return `Err(e)` or write a structured event the bg task can
observe. Logging alone loses the signal.

## `unsafe impl Send` is almost always wrong

`Arc<Mutex<T>>`, `Vec<Arc<dyn Trait + Send + Sync>>`, `Option<String>`
are all auto-Send. If a struct compiles fine without `unsafe impl Send`,
adding one is either dead code or papering over a real `!Send` field.

**How to audit**: any `unsafe impl Send for FooManager {}` should
trigger a check of every field for non-Send types. If none, the impl
is gratuitous and should be removed. If one, the manual attestation
should be justified with a comment explaining why it's sound.

## Plan-vs-reality test-count inflation (~3x)

Phase 8b's PLAN.md claimed 88 + 150+ test cases. Reality (counted via
`grep -E '@Test|@ParameterizedTest'`) was 31 + 93. When translating
test files, the per-case translation count is the bar, not LOC.

**How to audit**: count `@Test`/`@ParameterizedTest` annotations in
the Java source file. Don't trust LOC-based estimates.

## DoD §3 per-case rationale

A blanket "Mockito-heavy cases deferred" in PLAN.md does NOT satisfy
DoD §3. Each un-translated Java test needs a one-line rationale in
the Rust test module (a `// Deferred: requires X` comment OR a
COMMENTS entry). Many "Mockito-heavy" tests can be translated by
constructing real `SubscriptionState` with the right input — Mockito
is used for *convenience*, not *necessity*.

**How to audit**: spot-check 5-10 un-translated Java tests. If their
Mockito use is limited to `when(subscriptionState.hasAutoAssignedPartitions()).thenReturn(true)`,
they can be translated by building a real `SubscriptionState` and
calling `assign(...)` or similar setup.

## Public API surfaces vs lower-level helpers

`transition_to_sending_leave_group` ≠ `leave_group` / `leave_group_on_close`.
The former is a state-machine primitive; the latter is the public API
that the close-handshake calls. Translating only the primitive looks
"good enough" but Phase 11's `Consumer::close()` has nothing to call.

**How to audit**: grep the Java public-method list against the Rust
translation. Methods marked `public` (not `protected` / `package-private`)
must all have Rust translations.

## `Timer.update(now)` vs `Timer.reset(...)` ≠ same semantics

Java's `Timer.update(now)` ALSO checks `isExpired()` and triggers
side effects (e.g., `maybeRejoinStaleMember()`). A naive Rust
translation that just updates a field misses the side effects. When
the Rust implementation is "absolute deadline" rather than "elapsed",
the field stored is dead AND the expiry-check side effect is lost.

**How to audit**: every `Timer.update(now)` / `Timer.reset(...)` call
in Java should have a Rust counterpart that performs both the time
adjustment AND any conditional side-effect at that call site.

## Doc-comment drift in async refactors

When refactoring a Java `CompletableFuture` chain into Rust `async fn`,
doc comments that describe the "Phase X will drive this" may become
out of date when the actor inlines the behavior. E.g., Phase 8b's
`transition_to_fatal` doc says "Phase 8b we do not invoke the
onPartitionsLost callback here" — but the code DOES invoke it. Such
drift is a code-review red flag.

**How to audit**: read every multi-line doc comment in the file against
the code below it. If the comment claims "we don't do X" but the code
does X, flag it.
