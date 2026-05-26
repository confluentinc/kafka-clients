---
name: review-m8-phase2-patterns
description: Phase 2 trait surface — Clone-on-panic-recovery leaks bounds; &mut weakens immutable-input guarantees; pub(crate)+dead_code is scope creep
metadata:
  type: feedback
---

# Milestone-8 Phase-2 review patterns

Phase 2 introduced the public trait surface for the consumer (Consumer<K,V>,
Deserializer, ConsumerInterceptor, ConsumerRebalanceListener,
OffsetCommitCallback) plus the internal containers (Deserializers,
ConsumerInterceptors).

## Repeating translation pitfalls to look for in Phase 3+ work

1. **Ownership-transfer translations of Java methods that returned a new
   object require `K: Clone, V: Clone` to emulate Java's "previous-good
   reference" semantics on exception.** Java's `ConsumerInterceptors.onConsume`
   keeps the prior result by simply not reassigning a local on exception.
   Rust translating to `fn on_consume(records: Foo<K,V>) -> Foo<K,V>` forces
   either (a) a clone-before-call to recover, or (b) a signature change to
   `&mut Foo<K,V>` or `Result<...>`. The clone path leaks bounds into the
   `Consumer<K, V>` trait surface and to user-facing K, V types — Java has
   no such constraint.

   **Why it matters**: this is the kind of bound that propagates through
   the entire consumer module once it's introduced. Flag it during the
   trait-design phase, not after Phase 11 hits the compile error.

   **How to apply**: when reviewing any "container holds Vec<Box<dyn Trait<K,V>>>"
   and the trait's method takes ownership of K/V-bearing data, check the
   exception/panic recovery path. If it clones, the bound leaks. The PLAN
   should pick a recovery strategy explicitly.

2. **Watch for "clone before every call" in panic-recovery loops.** A
   panic is the exceptional path; cloning before every call pays the cost
   on the happy path too. Even at per-batch granularity (not the
   CLAUDE.md §11 hot path), this is a perf regression vs Java's
   reference-passing.

3. **`assertEquals(complexValue, complexValue)` translations to count-only
   assertions are weaker than Java.** Java relies on `equals()` for full
   structural comparison. If the Rust type lacks `PartialEq`, the
   translation usually degrades to a count check + key-set check, which
   does not catch content mutation. Per DoD §3, the assertion strength is
   part of the behavioral contract. Encourage deriving `PartialEq` over a
   weakened assertion.

4. **Java interface parameters → concrete Rust struct parameters lose
   polymorphism.** Java's `Headers` is an interface; translating to
   `&RecordHeaders` (the concrete impl) closes the door on test doubles
   and alternative implementations. PLAN.md spec snippets that say
   `&Headers` (the trait) should not silently degrade to the concrete
   type.

5. **"Pause and ask" gates in PLAN.md get bypassed when the dependency
   is obviously needed.** Even if the PLAN's `#[async_trait]` requirement
   implies `async-trait` must be added, the PLAN's "pause and ask"
   instruction is procedural. Note in COMMENTS.<N>.md when the gate was
   skipped, even if the outcome was correct.

## Inline tests in `pub(crate)` internals — accepted pattern

The Actor moved `ConsumerInterceptorsTest` from
`tests/consumer/internals/consumer_interceptors_test.rs` to inline
`#[cfg(test)] mod tests` because integration tests cannot reach
`pub(crate)` items. This matches the producer's
`src/producer/internals/buffer_pool.rs` precedent — accept this pattern,
do not flag as "didn't follow PLAN file layout".

However: if a `tests/consumer/internals/` directory was created and left
empty, flag it as a cleanup nit so it doesn't mislead future
contributors.

## Verified safe patterns in this phase (do NOT regress)

- `Send + 'static` (no `Sync`) on `Consumer<K, V>` and
  `ConsumerInterceptor<K, V>`. Rationale: `&mut self` means no shared
  reference; `Box<dyn>` single ownership means no `Arc` clone.
- `Send + Sync + 'static` on `ConsumerRebalanceListener` and
  `OffsetCommitCallback`. Rationale: stored in `Arc<dyn>` for the
  clone-out-of-channel pattern in consumer-threading.md §31.
- `#[async_trait]` on per-rebalance / per-poll dispatch traits; NEVER on
  per-record (`Deserializer`, anything inside the fetch path).
- `trait_surface_check.rs` with `_assert_object_safe` + `_assert_send`,
  intentionally NO `_assert_sync` for `Consumer<K, V>`. If a future change
  adds `_assert_sync`, that is itself a bug.

## Fixup patterns (post-resolution of #1/#2)

6. **`pub(crate) + #[allow(dead_code)] + "Phase N will use this"` is
   scope creep.** When a phase adds a `pub(crate)` API not present in
   Java and the only callers are inside `#[cfg(test)] mod tests`, that
   API should be `#[cfg(test)]`-gated. DoD §7 forbids Rust-only
   additions without production-side justification, and "Phase N will
   use it" is a deferred-completion marker. `#[cfg(test)]` drops both
   `dead_code` and `clippy::type_complexity` allows for free.

   **How to apply**: for any `pub(crate) fn X` with `#[allow(dead_code)]`,
   `grep -rn X src/`. If the only hit is inside a `#[cfg(test)] mod`,
   file a MINOR finding to swap visibility.

7. **Java structurally-immutable input cannot be reproduced by Rust
   `&mut T`.** When Java's `T` has `private final` fields plus
   unmodifiable views and Rust translates to `fn on_consume(&mut T)`,
   the panic-safety guarantee weakens from structural ("input cannot be
   corrupted") to contractual ("well-behaved interceptor"). The
   canonical Rust idiom `std::mem::take(records)` *itself* violates the
   contract by committing to an empty intermediate state.

   **Why it matters**: the in-repo test fixture often demonstrates the
   exact pattern the docs warn against. If the test passes only because
   the panic is positioned before `mem::take`, the docs are
   under-specified. Flag the doc as needing tightening; either
   acknowledge `mem::take` as an anti-pattern or add a regression test
   showing the failure mode.

   **How to apply**: when a Java method that returns `T` (where `T` is
   structurally immutable) gets translated to `fn (&mut T)`, look at:
   - Does the rustdoc honestly call out the weaker guarantee?
   - Does the test fixture use `mem::take`/`mem::replace` before
     building the replacement? If yes, the panic-safety property is
     ordering-dependent in a way Java's is not.
