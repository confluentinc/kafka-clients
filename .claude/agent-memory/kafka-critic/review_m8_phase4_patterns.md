---
name: review-m8-phase4-patterns
description: SubscriptionState/ConsumerMetadata review — debug_assert in release-mode silently masks Java IllegalStateException; Display format divergences from Java toString; HashSet vs Java TreeSet wire-order
metadata:
  type: feedback
---

Patterns to watch for in `consumer/internals` translations:

## debug_assert vs Java IllegalStateException

In Rust, `debug_assert!` is a no-op in release builds. When translating
Java code that throws `IllegalStateException` on a programmer-error path,
`debug_assert!` is NOT equivalent — it masks the inconsistency silently in
production.

**Specific anti-pattern**: A `transition_state(new_state, run_if_transitioned)`
helper that mirrors Java's `transitionState(FetchState, Runnable)` should
either `panic!` or `return Err(KafkaError::illegal_state(...))` per
CLAUDE.md §10.

**Why:** Java contract is "throw IllegalStateException in all builds"; a
release-only no-op leaves state inconsistent. Found in M8 Phase 4
`subscription_state.rs:320-334`.

**How to apply:** Whenever translating Java `throw new IllegalStateException`
inside a synchronized method body, the Rust translation must surface the
error in release builds — never `debug_assert!`.

## Display/toString format divergences

Java's `HashSet.toString()` / `AbstractCollection.toString()` produces
`[elem0, elem1]` (no quotes). Rust's `{:?}` on `Vec<String>` produces
`["elem0", "elem1"]` (with quotes). Tests asserting on substring matches
catch most divergences, but not all.

**How to apply:** When the Rust `Display` impl mirrors a Java `toString`,
verify the inner-element format too. If Java prints `assignment.partitionStateValues()`
(opaque `Class@hash` strings), Rust shouldn't print partition names — pick
either *match exactly* or *intentional divergence with rustdoc note*.

## HashSet/Vec collection from BTreeSet/TreeSet

Java's `TreeSet<Uuid>.iterator()` gives sorted iteration; Rust's
`BTreeSet<Uuid>.iter()` is the same. But if intermediate `collect::<HashSet<_>>()`
appears between the source and a downstream consumer, the ordering is lost.

**How to apply:** When the Java code passes a `TreeSet` (or any sorted set)
to a builder/serializer, the Rust translation should either pass the
`BTreeSet` directly or collect into a `Vec` from the sorted iteration.
HashSet/HashMap intermediates that re-randomize the order are a
divergence. Specifically watch
`MetadataRequestBuilder::for_topic_ids(&HashSet<Uuid>)` for this pattern.

## Tests inline vs tests/

When all translated types are `pub(crate)`, integration tests in `tests/`
can't name them — inline `#[cfg(test)] mod tests` is the correct choice.
The actor's deviation from "tests/" in the plan is justified for
`pub(crate)` types. Don't flag inline tests as a deviation when types are
crate-private.
