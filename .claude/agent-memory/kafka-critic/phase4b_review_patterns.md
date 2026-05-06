---
name: Phase 4b review patterns — metadata stack translation
description: Patterns and recurring traps observed reviewing Metadata/MetadataSnapshot/ProducerMetadata translations
type: project
---

Phase 4b review surfaced four recurring translation traps for the metadata stack and similar stateful caches. Use these when reviewing future phases that touch shared mutable state with hot-path readers.

**Why:** The actor's Phase-4b commit had clean DoD checks but skipped 7 Java tests on grounds that did not survive verification, and silently introduced a hot-path lock and a swallowed-error site. Mechanical green is not enough; verify deferrals and inspect lock scopes around hot reads.

**How to apply:** When reviewing a translation of Java code with `volatile` fields, `synchronized` methods, and protected overridable methods:

1. **Verify "covered by other test" claims with 1:1 mapping.** When the actor defers a test as "covered by `FooTest`," open both Java sources and grep `FooTest` for assertions on the deferred test's specific call path. A test on `Metadata.updatePartitionLeadership` is NOT covered by `MetadataSnapshotTest.mergeWith` — they exercise different methods. The actor's note must cite test name + line range, not a generic claim.

2. **`volatile` field reads must not become locked reads.** Java `volatile T` field with non-`synchronized` getters (e.g. `Metadata.fetch()` reading `metadataSnapshot`) is a lock-free atomic-load. Translating to `Mutex<Inner>` and acquiring the lock for the read serializes readers behind writers — a measurable perf regression on producer hot paths. Look for `Arc<X>` shared state where Java uses `volatile` — recommend `arc_swap::ArcSwap<X>` or split out into `RwLock<Arc<X>>` so reads are independent of the writer lock.

3. **Concurrency stress tests are translatable, not Java-specific.** `ExecutorService` + `CountDownLatch` stress tests like `testConcurrentUpdateAndFetchForSnapshotAndCluster` translate directly to `tokio::sync::Barrier` + `tokio::spawn` or `std::sync::Barrier` + `std::thread::spawn`. The property under test (data-race freedom, snapshot consistency) is language-independent. A "no Rust analogue" deferral on this kind of test is almost always wrong.

4. **Listener-invocation lock scope changes behavioral semantics.** Java holds the synchronized monitor while invoking `clusterResourceListeners.onUpdate(...)` — listeners observe the update atomically. Rust dropping the lock before the callback allows another writer to interleave between the lock-drop and the listener invocation, so the listener can be notified about update N while the actual `metadata_snapshot` is already at N+1. Either match Java's lock scope (and document the no-reentry constraint, since `std::sync::Mutex` is non-reentrant) or document the deviation explicitly.

5. **`unwrap_or_default()` on a `Result` from a Java method that throws is a silent behavioral deviation.** Java `MetadataResponse.errors()` throws `IllegalArgumentException` for malformed responses; `errors = response.errors()` would propagate. Rust `response.errors().unwrap_or_default()` swallows the exception and substitutes an empty map, leaving downstream `getError(topic)` to return `None` for what was actually a malformed response. Always grep `unwrap_or_default()` and `unwrap_or(...)` against translations of `throws`-declared Java methods.

6. **Deferred-list omissions are harder to spot than explicit deferrals.** Phase 4b had 7 explicit deferrals plus ~9 silent omissions of MetadataTest cases. When reviewing, count: total `@Test` annotations in the Java file vs. total `#[test]` / `#[tokio::test]` in the Rust file plus the explicit deferral list. If the numbers don't add up, find the silent skips.

7. **Test-name/assertion mismatch.** Watch for tests with detailed names like `failed_update_resets_attempts_on_subsequent_success` whose assertion is just `last_successful_update() == 100` (proves the update happened, not that attempts was reset). The Java test typically has a stronger assertion — check the Java assertion list and confirm the Rust test asserts the same property.
