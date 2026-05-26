---
name: Phase-7e review patterns
description: KafkaProducer public surface (flush/close/partitions_for/metrics + partitioner.class factory) — review patterns and deferred-ctor verification
type: project
---

Recurring patterns I saw / verified in Phase 7e (Critic 7, Round 1).

# `tokio::time::timeout(timeout, JoinHandle).await` consumes the handle

In `close_inner`'s graceful path, the actor used:

```rust
match tokio::time::timeout(timeout, handle).await {
    Ok(Ok(())) => { /* clean exit */ },
    Ok(Err(join_err)) => { /* panic */ },
    Err(_elapsed) => { /* timeout — handle is gone */ },
}
```

In the `Err(_elapsed)` arm the `handle` is consumed — there's no way to abort or re-await it. Java's analogue (`ioThread.join(remainingMs)` then later `ioThread.join()` if alive) keeps the thread reference around for the force-close fallback. The Rust translation has to choose between: (a) `tokio::select!` between `sleep(timeout)` and the JoinHandle, so the handle survives the timeout, or (b) `let timed_out = tokio::time::timeout(timeout, &mut handle).await.is_err();` if `handle` is `&mut`.

**Pattern**: when reviewing close paths, check whether the JoinHandle is consumable in the timeout-elapsed branch. If not, the function's `Ok(())` return diverges from Java's "ioThread terminated" post-condition.

# Deferred public ctor — what to check

When a public constructor returns `UnsupportedOperation` deferred to a later phase, verify:

1. The deferred-error message names a SPECIFIC Java class / phase as the lift point (Phase 7e's `PRODUCTION_NETWORK_CLIENT_DEFERRED` cites `DefaultMetadataUpdater` and Phase 8 — good).
2. The rustdoc points at the working alternative (`new_for_test` + `ManualMetadataUpdater`) — good.
3. NOTES.md lists the lift point as a Phase 8 carry-over with a one-paragraph rationale — good.
4. The `impl Producer for KafkaProducer` is otherwise complete on the private `new_for_test` surface — verified end-to-end through Phase 7e tests.

If any of (1)-(4) are missing, the deferral is "stuck" rather than "scheduled." This phase passes all four.

# Idempotent close via `Arc<AtomicBool>` — correct shape

```rust
if self.closed.swap(true, Ordering::AcqRel) {
    return Ok(());
}
```

Combined with `Mutex<Option<JoinHandle>>::take()`, this gives both:
- second `close()` returns `Ok(())` immediately without re-awaiting a consumed handle;
- `Drop` after explicit close sees `None` and skips the abort (no double-abort).

Test pattern: assert post-close `producer.sender_task.lock().unwrap().is_none()` AND call `close()` a second time within a tight `tokio::time::timeout(100ms, ...)` to verify the fast-path. Phase 7e does both.

# Java-vs-Rust trait-method gap discovery procedure

For each Producer trait method on the public surface:
1. Search the trait declaration (`producer.rs`) — confirm method appears.
2. Search the Java source (`KafkaProducer.java`) — confirm Java method exists. (For Phase 7e: `listTopics()` is NOT on `KafkaProducer` — confirmed by grep; correctly absent from the trait.)
3. If Java method exists but Rust trait omits, that's a Blocking gap.
4. If Rust trait declares a method Java doesn't have, that's a "new struct/trait" DoD#7 violation — flag.

Phase 7e: every trait method has a Java analogue. No new methods.

# Test-coverage gap: pre-public-ctor multi-record concurrency tests

When the public ctor is deferred, tests that depend on a full MockClient roundtrip get harder. Phase 7e's `flush_waits_for_pending_record_to_complete` reaches into the accumulator directly because `MockClientImpl` is `pub(super)` to `sender.rs`. Java's analogue (`testFlushCompleteSendOfInflightBatches`) sends 50 records via `producer.send()`. The single-record variant exercises the same code path but doesn't pin "all 50 futures resolve after flush." This is a legitimate Suggestion-level gap, not Blocking — the path is the same.

**Pattern**: when reviewing tests on a deferred-ctor surface, check whether a test was simplified due to the test-mock visibility, and flag it for the Phase that hoists the mock.

# Empty-test-body trap

`partitioner_class_fqcn_round_robin_resolves` ends with:
```rust
let _ = partitioner;
let _ = cluster;
```
The rustdoc claims "downcast via Arc::as_ref's `&dyn Partitioner` ... probe behavior" but the body never calls into the partitioner. The test reduces to `producer.partitioner.is_some()` — identical to the simple-name test. The end-to-end behavior IS covered by the round-robin distribution test, so coverage is fine, but the test-pair has redundant signal.

**Pattern**: scan tests with explanatory rustdoc that mentions "downcast" / "probe behavior" — verify the body actually performs that probe. `let _ = x` followed by no further usage is a smell.

# `partitioner.is_none() && adaptive_enabled` — verify exact Java line

Java line 417: `enableAdaptivePartitioning = partitionerPlugin.get() == null && config.getBoolean(...)`. The Rust factory must thread its result through this check correctly — when the user supplies `partitioner.class=RoundRobinPartitioner`, `partitioner.is_some()` and adaptive is disabled. Both branches matter: an unset config → `None` → adaptive enabled (if config bool is true) → built-in sticky-with-adaptive. Phase 7e gets this right.

# `flush`-from-callback deadlock — documented as bounded

Java throws `KafkaException` if `Thread.currentThread() == ioThread` in `flush()`. Tokio has no thread-identity comparator (until `tokio::task::id()` matures). The Rust translation:
- Documents the deadlock in the `flush` rustdoc body.
- Lists it as a Phase 8 carry-over in NOTES.md.
- Flags it as bounded: a user calling `flush().await` from a `Callback` body would deadlock the runtime, observable to the developer at runtime.

**Pattern**: when Java has a thread-identity guard, the Rust impl must either (a) replicate via `tokio::task::id()` if that API is stable, or (b) document the divergence with a "bounded" rationale (caller deadlocks observably) and a Phase-N lift plan.

# Verified-clean checks (memorize the shape)

- `Mutex::lock()` followed by `.take()` — the `MutexGuard` is bound to a temporary that drops at end-of-statement. Safe to use before `.await` even though it looks like the lock is "held" textually.
- `tokio::time::timeout(_, async_op)` does NOT introduce a select! with side-effecting branches; it's just a deadline wrapper.
- Empty-stub trait method `fn metrics(&self) -> ProducerMetrics { ProducerMetrics::new() }` — costs one HashMap allocation per call. Acceptable since `metrics()` is not on the send path.
