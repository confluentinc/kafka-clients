---
name: Phase-6b review patterns
description: ProducerBatch translation: unsafe Send/Sync audit, OnceLock idempotence, per-record bifurcation drift, hot-path zero-copy on try_append/split, missing-method scan against Java accessors
type: project
---

Phase 6b (ProducerBatch) review patterns and traps to apply when reviewing Producer-internals translations.

## What I learned reviewing this phase

**1. `unsafe impl Send + Sync` audit checklist for self-referential types behind a Mutex.**
The `MemoryRecordsBuilder` is `!Send + !Sync` because of a `*mut ByteBufferOutputStream` raw-pointer field plus a `Box<dyn Write + 'static>` codec writer (no `Send` bound). To allow `Arc<ProducerBatch>` to cross task boundaries, the translation needs `unsafe impl Send + Sync for ProducerBatch`. Don't reject this on sight — verify:
- Every read/write of the inner `MemoryRecordsBuilder` flows through a single `Mutex` lock guard.
- No method returns `&MemoryRecordsBuilder` or stashes one with a borrow that escapes the lock guard.
- The raw pointer's pointee lives on the heap (`Box<...>`) so its address is stable across moves of the outer struct.
- The pointee's content (`ByteBufferOutputStream` here) is `Send` — only the raw-pointer wrapper is not.
- The `Box<dyn Write>` writers (gzip/zstd/snappy/lz4 wrappers) are `Send` in practice; the unsafe impl is the assertion.

The criterion for filing: name a *specific* method or path that hands out a reference outside the lock. Otherwise it's a verified-safe pattern.

**2. `OnceLock<FinalState>` for AtomicReference CAS-once. Verified safe.** Java's `AtomicReference<FinalState>.compareAndSet(null, X)` translates 1:1 to `OnceLock::set(X).is_ok()`. The "loser" path (second call) gets `Err`, which the code interprets as "already finalized; act as no-op." Don't file this — it's a clean translation pattern.

**3. `catch_unwind` around user callback fan-out.** Java catches `Exception` (not `Throwable`); Rust's `catch_unwind` catches unwinding panics. Functionally equivalent for "user callback panicked, log and keep firing remaining callbacks." Boundary check: the closure should capture as little state as possible; `AssertUnwindSafe` is acceptable IF post-catch the code only continues iterating already-snapshotted data, never mutates `self`. Filing criterion: name a specific aliasing/UnwindSafe violation.

**4. Lifecycle ordering: `set` → fire callbacks → `done`.** Mirrors Java's `completeFutureAndFireCallbacks`. Critical because users may chain "callback runs and writes shared state, then await future and read that state." Reverse the order (signal future before firing callbacks) and the Sender-side tests will see callbacks running AFTER the user's `.await` resolves. Verify the order matches Java's `ProducerBatch.java:303-323`.

**5. Per-record error bifurcation: bifurcate on `record_exceptions.is_none()`, NOT on `record_exceptions(i).is_none()`.**
Java's logic: `if (recordExceptions == null) { success } else { error }`. The closure is allowed to return null (Java passes `null, null` to onCompletion in that case — likely a user bug but Java's behavior).
Rust's `record_exceptions.as_ref().and_then(|f| f(i))` flattens both into a single `Option<Err>`, then matches on the result — which silently flips a present-but-returning-None closure into the success path. Through the public API this divergence is unreachable, but it's worth flagging as a Suggestion: a `debug_assert!` in the `Some(f)` branch tightens the invariant.

**6. `chain` future plumbing through split.** Java's `tryAppendForSplit` does `thunk.future.chain(future)` and `this.thunks.add(thunk)` — transferring the original thunk to the split batch. Rust creates a fresh `Thunk { callback, future: new_future }` rather than transferring. Both are correct as long as:
- The user-facing original future's chain points at the new_future.
- The split batch's `complete_future_and_fire_callbacks` reads metadata from the split batch's `produce_future`, not the parent's.
The Rust path uses `metadata_for(i, &thunk.future)` which correctly reads `self.produce_future.base_offset()` for the split batch.

**7. Hot-path zero-copy for `try_append`/`split`.**
- `try_append`: `Option<&[u8]>` for key/value, `&[RecordHeader]` for headers, flowing directly into `records_builder.append`. Per record allocations: `Arc<FutureRecordMetadata>` (unavoidable, shared with caller), amortized `Vec<Thunk>` push.
- `split`: `Box<dyn Record + 'a>` per iteration step is unavoidable due to the trait-object iterator. Acceptable for the rare split path (CLAUDE.md rule 12 calls split out as "rarely hit"). Borrowed `&[u8]` flows directly into `try_append_for_split`.

**8. Missing-method scan trap.** When reviewing a public-class translation, grep the Java source for ALL `public/private/void/boolean/long/int/short/byte/double/float\s+\w+\s*\(` declarations and cross-check. Don't rely on the Actor's "all methods translated" claim. In Phase 6b, `buffer()` (Java line 543, used by RecordAccumulator at line 1053) was missed. The `#![allow(dead_code)]` annotation said "Phase 6d will use these accessors" but `buffer()` was not in the implemented set.

**9. Tautological test assertions.** Watch for `assert!(res.is_ok() || res.is_err())` and similar — every Result is one or the other. The test passes regardless of behavior. File as Suggestion (test quality), not Bug. Other patterns:
- `assert!(matches!(x, Some(_)) || matches!(x, None))` for an `Option`
- `assert!(true)` in a code path that's reachable
- `let _ = CONSTANT;` as a "smoke check" that does nothing

**10. Public-API safety enforcing invariants Java doesn't.** Rust's typing can rule out states Java only catches at runtime (e.g., NPE on null `recordExceptions`). When a test bypasses the public API to "smoke" the divergence, the test is documenting an internal behavior that can't surface through the public API. Flag if the documentation is misleading; don't flag if it's a parity-test.

**11. Translation patterns I'm marking as verified-good:**
- `Mutex<MutState>` wrapping the builder + thunks + per-batch counters; `OnceLock` for CAS-once final state; `AtomicI32` for the `attempts` counter (read concurrently from `maybe_update_leader_epoch`).
- Pre-snapshot mutable state under the lock (`std::mem::take(&mut state.thunks)`) THEN fire user callbacks outside the lock to avoid re-entrancy hazard.
- `metadata_for(i, &thunk.future)` reading from `self.produce_future` (split-batch-correct because thunks are transferred to the split batch and `self` is the split batch).

## Filing thresholds applied

- Verified-safe `unsafe impl Send + Sync` (audit complete) → no filing.
- `catch_unwind` + `AssertUnwindSafe` boundary tight → no filing.
- Missing public method (Java has it, Rust doesn't) → file as Bug (Missing Requirement) even if not Blocking for this phase.
- Tautological assertion → file as Suggestion (test quality).
- Behavior drift unreachable through public API → file as Suggestion (with debug_assert remediation).

## Carry-forward for Phase 6c (Partitioners, Interceptors, ProducerRecord)

- `ProducerRecord<K, V>` is generic. The hot-path is `&[u8]` post-serialization. Verify `topic()` returns `&str` (`Arc<str>` storage), no `String` clone per send.
- `BuiltInPartitioner` uses `AtomicI32` for sticky-partition index per CLAUDE.md rule 11. Don't accept `Mutex<i32>`.
- `ProducerInterceptors::on_send` returns the (possibly modified) record by value, not boxed.
- The interceptor list is `Vec<Box<dyn ProducerInterceptor>>` — verify the trait is `Send + Sync + 'static`.
