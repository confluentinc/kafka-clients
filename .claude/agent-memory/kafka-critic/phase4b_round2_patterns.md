---
name: Phase 4b Round 2 review patterns
description: Verified-good translation patterns for Java synchronized + volatile -> Rust Mutex + ArcSwap, listener non-reentrancy, and concurrent reader/writer test shape
type: project
---

Round-2 verification confirmed three translation patterns as good for the metadata-stack family. Useful as positive precedent for future stacks (consumer subscriptions, transaction state, etc.).

**ArcSwap as the "volatile Arc" analogue.** Java's `volatile metadataSnapshot` field with non-`synchronized` accessors maps cleanly to `ArcSwap<MetadataSnapshot>` on the outer struct (NOT inside `MetadataInner`). Readers do `metadata_snapshot.load_full()` (no lock); writers do `metadata_snapshot.store(Arc::new(new))` from inside the writer mutex (so writer-vs-writer ordering is preserved). The key invariant the actor got right: writers `load_full()` exactly once at the start of the writer section and reuse that Arc throughout the update, so all internal computations see a consistent view. `arc-swap = "1"` is the established crate; Cargo justification comment cites `Metadata.java:79,129-138`.

**Why:** A `Mutex<MetadataSnapshot>`-style design serializes producer hot-path readers behind any in-flight writer and is a measurable regression vs Java's volatile reads.

**How to apply:** When reviewing a Java field marked `volatile` with non-synchronized getters AND synchronized writers, the canonical Rust shape is `ArcSwap<T>` on the outer struct. Flag a `Mutex<T>`-only translation as a perf regression. Verify writers don't `load_full()` repeatedly — they should snapshot once and reuse.

**Listener non-reentrancy adaptation.** Java's `synchronized` is reentrant; `std::sync::Mutex` is not. Java code that calls `clusterResourceListeners.onUpdate(...)` inside a `synchronized` block can have a listener call back into the same monitor and not deadlock. Rust must dispatch the listener inside the lock for ordering correctness BUT add a type-level rustdoc constraint that listeners must not call back into the same instance.

**Why:** Dispatching the listener outside the lock lets a concurrent writer race in and complete update N+1 between the lock-drop and the listener call, so a listener notified for version N can observe the version-N+1 snapshot. This is observable when the listener captures `metadata.fetch_metadata_snapshot()`.

**How to apply:** When reviewing translations of Java `synchronized` blocks that contain listener/callback dispatch, verify (a) the dispatch happens inside the Rust lock, (b) the snapshot/state is published via ArcSwap *before* the dispatch so the listener observes post-update state, and (c) there is a rustdoc-visible "Listener constraint" doc on the type warning that listeners must not re-enter.

**Concurrent reader/writer test shape.** Java's `ExecutorService.submit(...)` + `CountDownLatch.await()` + post-test assertions on monotonically-increasing fields (node count, partition count, leader epoch) maps cleanly to `std::thread::spawn(...)` + `std::sync::Barrier` for a synchronous-Mutex-backed type. Critically: Java does NOT test concurrent-during-update reads; readers wait at the latch, writers count down after committing. So the Rust translation also doesn't need to assert mid-update consistency — only post-update visibility.

**Why:** A `tokio::spawn` + `tokio::sync::Barrier` translation would be wrong for a `std::sync::Mutex`-backed type — Tokio task scheduling can starve writers and you'd be testing async coordination, not the metadata structure.

**How to apply:** When reviewing concurrency tests, match the Java synchronization primitive (latch ↔ barrier, executor ↔ thread::spawn, future.get ↔ join). The post-test assertions should be `>` (strictly greater) on monotonic fields — equality assertions would race-fail. Verify no `tokio::*` primitives are used for sync-Mutex types.

## Other Round-2 lessons

- **`expect()` vs `propagate Result` for "this can't happen" error variants.** When Java's API has an exception that's only thrown on a programming error (e.g. `MetadataResponse.errors()` throws when used on a topic-id-only response, but the producer client always builds name-keyed responses), `expect()` with a documenting message is acceptable IF the comment names the invariant. Don't accept silent `unwrap_or_default()` — that violates CLAUDE.md rule 5.
- **Strengthening "weak" test assertions.** When a test says "asserts X happened" but only checks a side-effect that would be true under multiple paths (e.g. `last_successful_update == N` proves the update happened, NOT that `attempts` was reset), the strengthening pattern is: do the operation, then do *another* operation that would fail differently if the invariant is broken. Here: success-path resets attempts, so a *subsequent* failed_update should land in base-jitter-band, not exponentially-grown band.
- **Duplicate test detection.** Two tests with different names exercising exactly the same path (close → await with current update_version → expect Generic) is a maintenance hazard. Inline comment about the removal is good practice; just deleting silently is worse.
