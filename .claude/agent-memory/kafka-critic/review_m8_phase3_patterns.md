---
name: review-m8-phase3-patterns
description: Patterns from reviewing MockConsumer translation (Milestone-8 Phase-3) — sync-vs-async traits, signed/unsigned offset, listener inline invocation
metadata:
  type: feedback
---

Patterns observed during Milestone-8 Phase-3 review (MockConsumer
translation, commits `0c8d0a9..eff6200`).

**Why:** These translation-pattern recurrences predict future bugs in
the rest of Milestone-8 (`AsyncKafkaConsumer`, request managers, etc.)
and are worth checking for proactively in later phases.

**How to apply:** When reviewing future consumer-module translations,
specifically check:

1. **`&self`-vs-Java-`position(tp)` shape mismatch**: any Java method
   that calls into a position/state mutator (`position`,
   `updateFetchPosition`, `validatePartitionsForCurrentLeader`) and is
   accessed via a Rust `&self` accessor. The Rust impl will be forced
   to either (a) skip the mutation (behavior divergence), (b) use
   interior mutability (potentially hiding races), or (c) return None
   silently. Check the rustdoc surfaces the divergence and any silent
   fallback is intentional. `MockConsumer::current_lag` is the
   canonical example.

2. **Listener invocation thread (consumer-threading.md §31)**: every
   call site that invokes `ConsumerRebalanceListener::on_partitions_*`,
   `OffsetCommitCallback::on_complete`, or any `&self` async trait
   method on a user-supplied Arc must `.await` **inline on the caller's
   task** — NEVER `tokio::spawn` and never on the background task.
   Listener-from-Arc deref + `.await` is the cheap pattern; check that
   no `SubscriptionState` lock guard is held across the `.await`.

3. **Java `ensureNotClosed` ordering**: Java consumer methods generally
   `ensureNotClosed()` BEFORE doing any work. Rust translations
   sometimes compute inputs first then dispatch into a helper that
   checks. Behavior is identical (both return errors) but ordering of
   side effects diverges — flag for visibility, not as a bug.

4. **Java `commitAsync(map, cb)` callback contract**: Java invokes
   `callback.onComplete(offsets, null)` synchronously inside the
   commit method (line 357). The Rust translation must `.await` the
   callback before returning — fire-and-forget would break the
   contract. Check for `tokio::spawn` on the callback path.

5. **Map iteration + mid-iteration mutation**: Java often uses
   `Iterator.remove()` on `HashMap.entrySet()` (e.g.
   `MockConsumer.poll()` line 309, 314). Rust cannot do this; the
   correct pattern is `let recs = map.remove(&k).expect(...)` →
   process → if non-empty, `map.insert(k, kept)`. Check that paused/
   skipped entries are NOT removed (the Java code skips
   `partitionsIter.remove()` when nothing was drained).

6. **`KafkaError::Wakeup` variant**: Wakeup is non-retriable,
   non-fatal, non-`ApiException`, no protocol error code. Verify all
   exhaustive matches on `KafkaError` (notably `kafka_error()`,
   `message()`, `is_api_exception()`, `Display`) handle the new
   variant; the compiler will catch outright misses, but watch for
   wildcard `_` arms hiding intent.

7. **Trait object-safety regression risk**: when a `Consumer<K, V>`
   impl adds a new generic method that has `Self: Sized` bounds (e.g.
   from `Clone`, `Default`), the `Box<dyn Consumer<K, V>>` coercion
   breaks. The `mock_consumer_is_consumer_trait_object` regression
   test guards against this; preserve it in future phases.

8. **`PollTask`-style boxed-closure type aliases**: think twice
   before re-exporting at the module root. The user-facing API uses
   type inference from `Box::new(|c| ...)`; the alias only needs to be
   reachable from the inherent-impl method signature. Public
   re-export widens the API surface for no clear benefit.

9. **Inline-comment Java-line traces drift from actual Java semantics
   during fixup loops**: When a fixup commit tightens behavior beyond
   Java's, the inline comment justifying the change may trace the
   wrong Java call chain (confusing `commitSync()` chain with
   `commitAsync()` chain in `MockConsumer.java`). Verify the comment's
   "Java line X → Y" trace by reading the Java source directly, not by
   trusting the commit message. Also check for **stale
   cross-references**: when an inline comment says "same trade-off as
   `foo()`", verify `foo()` still behaves that way after sibling
   fixups in the same loop. Example: `commit_async_with_callback`
   referenced "same trade-off as `commit_sync()`" but `commit_sync()`
   was retightened in a separate fixup commit (`1912761`) earlier in
   the same loop.
