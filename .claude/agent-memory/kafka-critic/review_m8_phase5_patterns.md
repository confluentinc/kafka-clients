---
name: review-m8-phase5-patterns
description: Patterns from reviewing Milestone-8 Phase-5 event-layer translation (ApplicationEvent / BackgroundEvent enums, CompletableEventReaper, WakeupTrigger)
metadata:
  type: feedback
---

# M8 Phase 5 review patterns

Translation patterns to watch for when reviewing the consumer event-passing
layer (ApplicationEvent / BackgroundEvent / CompletableEventReaper /
WakeupTrigger).

**Why:** these recurred across the Phase-5 review and several are easy to
miss because the PLAN.md itself can encode the mistake.

**How to apply:** check these patterns whenever reviewing event-enum
translations, future-style channel events, or reaper-style timeout
trackers.

## 1. `CompletableApplicationEvent<T>` → completable variant — always

Every Java event extending `CompletableApplicationEvent<T>` or
`CompletableBackgroundEvent<T>` MUST land in Rust as a variant carrying a
`handle: CompletableEventHandle<T>` (or equivalent). Bare `ApplicationEvent`
subclasses (no `Completable` prefix) become non-completable variants.

Phase-5 missed this 4 times: `AssignmentChange`, `LeaveGroupOnClose`,
`UpdatePatternSubscription`, and arguably `AsyncPoll` (Java's special
non-blocking design). The plan-doc encoded the same errors, so the impl
matched the plan but neither matched Java.

To verify: read the Java event's `public class X extends ...` line. If
it extends `CompletableApplicationEvent<T>` (note the generic), the Rust
variant needs a `CompletableEventHandle<T>` field. The `T` is significant —
e.g. `CommitEvent extends CompletableApplicationEvent<Map<TopicPartition,
OffsetAndMetadata>>` returns the committed offsets, not `()`.

## 2. Java events with multiple `CompletableFuture` fields

`CommitEvent` has TWO futures: the main `CompletableFuture<Map<...>>` AND
a secondary `offsetsReady: CompletableFuture<Void>`. The secondary future
signals "offsets are ready to commit" (used when `offsets` parameter is
`Optional.empty()`, meaning "commit all consumed").

Don't collapse two-future events into a single handle. Read the full class
fields, not just the `extends` line.

## 3. `Optional<T>` parameters that carry sentinel meaning

Java's `CommitEvent` uses `Optional<Map<TopicPartition, OffsetAndMetadata>>
offsets` where `None` means "commit all consumed". Translating to bare
`HashMap` loses that semantic — the caller can no longer express the
sentinel without materializing the entire current-position map.

Always read the Java javadoc on parameters — `Optional.empty()` often means
something semantically distinct from "empty collection".

## 4. `WakeupTrigger` rotating-token disable() semantics

The PLAN.md says `disable()` "replaces the token with a permanently
cancelled sentinel". The impl uses an `AtomicBool disabled` flag instead
and leaves the token un-cancelled. **This is correct** — Java's
`disableWakeups()` also does not cancel the current task; it only changes
the marker so future `wakeup()` calls are no-ops.

Don't reject this design just because the plan's wording differs. Verify
against Java behavior, not against the plan.

## 5. Java `BlockingQueue.drainTo(events)` clears the supplied collection

Java's `CompletableEventReaper.reap(Collection<?> events)` calls
`events.clear()` after iterating. The supplied collection is mutated in
place. Translating to `impl IntoIterator<Item = ...>` makes this
physically impossible — the caller must remember to drain separately.

When reviewing Rust impls of Java methods that mutate their input
collections, check for this side-effect loss. Either change the signature
to `&mut Vec<...>` and call `.clear()`, or prominently document the
caller's drain obligation.

## 6. Count semantics: increment before or after the boolean side-effect

Java code often does:
```java
if (event.future().isDone()) continue;
count++;
if (event.future().completeExceptionally(error)) { log.debug... }
```
The count is incremented BEFORE the side-effect call, so it represents
"events that were not done at the check time" — regardless of races.

Rust code may incorrectly gate the increment on the boolean:
```rust
if !handle.is_done() {
    if handle.fail_with_timeout(error) { count += 1; }  // ← race-sensitive
}
```
Under a concurrent completion that wins the race, Rust returns N, Java
returns N+1. Subtle, but visible in tests that mock concurrent timing.

## 7. `Arc::ptr_eq` does NOT match Java's per-event identity

If a Rust type provides a `erased(&self) -> Arc<dyn Trait>` method that
creates a NEW `Arc` each call, `Arc::ptr_eq` will return `false` for two
calls on the same handle. Java's reference-equality semantic relies on
the same Java object having ONE reference shared across call sites — that
invariant doesn't auto-translate to Rust's `Arc::new(...)` constructors.

Either cache the `Arc<dyn Trait>` inside the wrapped struct so repeated
`erased()` calls return the same `Arc`, OR define equality on the
underlying shared pointer (`Arc::as_ptr(&self.inner)`).

## 8. `MetadataErrorNotifiableEvent` and other Java marker-interfaces

When a Java event implements an interface with one method (e.g.
`MetadataErrorNotifiableEvent.onMetadataError`), the trait must be
translated even though the events are folded into a Rust enum, because
the dispatcher (processor) needs to dispatch on it uniformly. Don't drop
"single-method marker interfaces" — they are a real dispatch surface.

Verify by grepping Java for `implements MetadataErrorNotifiableEvent` —
if more than one event implements it, the trait carries dispatch
semantics and must survive translation.

## 9. Public vs internal type variants in event return shape

Java has `OffsetAndTimestamp` (public, rejects negatives) and
`OffsetAndTimestampInternal` (internal, allows negatives as sentinels).
The bg task uses `Internal` to represent "no offset found"; the app side
translates to the public type at the boundary.

When the Rust enum uses the public type directly, the sentinel
representation is lost. Either translate the `Internal` type or wrap in
`Option<T>` to recover the sentinel semantic.

## 10. AsyncPoll-style non-completable two-stage events

Java's `AsyncPollEvent extends ApplicationEvent` (NOT
`CompletableApplicationEvent`) and uses volatile flags + explicit
`completeSuccessfully` / `completeExceptionally` / `markValidatePositionsComplete`
methods. This is a deliberate non-blocking design — the app side polls
flags between iterations, never blocks on a future.

Don't shoehorn it into `CompletableEventHandle`. Translate as an explicit
state struct (with `AtomicBool` flags + a `Mutex<Option<KafkaError>>`)
inside the variant. Otherwise the two-stage poll semantics are lost.
