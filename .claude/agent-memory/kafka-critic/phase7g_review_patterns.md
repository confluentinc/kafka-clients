---
name: Phase 7g review patterns
description: KafkaFuture<T> + Producer::send two-phase restoration — Java parity, master-constraint smell-check, object-safe trait + send-path allocation audit
type: project
---

Phase 7g revert of Phase-7b `send()` collapse, decomposed into 5 commits:
1. `ce517b0` port `KafkaFuture<T>` + `KafkaFutureOps<T>` wrapper in isolation
2. `76ab120` `FutureRecordMetadata impls KafkaFutureOps<RecordMetadata>` bridge
3. `4e8a0ff` `Producer::send -> Result<KafkaFuture<RecordMetadata>, _>` + concrete impl + integration tests rewired
4. `2c21a2f` rustdoc honesty (Phase 7b vs 7g history note on `do_send` doc + FutureRecordMetadata module doc)
5. `2e66bb3` actor memory update

**Why:** Phase 7g represents a successful "revert + redesign-from-Java" pattern. The user explicitly forbade looking at the master branch (which had the working two-phase shape), forcing the actor to derive the translation from Java sources + CLAUDE.md rules. The translation came out clean and principled. Useful as a future-reference for similar "principled rework" tasks.

**How to apply:**

1. **For object-safe Future-like trait wrappers**: the right shape is
   ```rust
   pub struct KafkaFuture<T: Send + 'static> { inner: Arc<dyn KafkaFutureOps<T>> }
   pub(crate) trait KafkaFutureOps<T: Send>: Send + Sync {
       fn get<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<T, KafkaError>> + Send + 'a>>;
       fn is_done(&self) -> bool;
   }
   ```
   - `dyn Trait` where `Trait: Send + Sync` *does* auto-implement Send + Sync (verified with rustc). So `Arc<dyn KafkaFutureOps<T>>` is auto-Send+Sync without an explicit `+ Send + Sync` in the dyn list. Useful to remember; many critics over-cite this as a missing bound.
   - `Pin<Box<dyn Future>>` is the right shape on the trait method when the wrapper holds `Arc<dyn Trait>` — GATs would technically allow an associated-type future but aren't dyn-compatible. One boxed future per `.get().await` call, paid by callers who choose to wait. Distinct from the per-`send` `Arc<dyn Trait>` allocation.

2. **Verifying "master-constraint" smell when user forbids looking at master**:
   - `git branch -a` — does `master` even exist on this clone?
   - `git log <range> --format="%B" | grep -i master` — any references in commit bodies?
   - `git diff <range> -- src/ | grep -i master` — any in source comments/doc/test names?
   - Structural smells: structural choices that have multiple reasonable answers (trait method shape, error-mapping, field ordering) — are they justifiable from Java + CLAUDE.md alone? If a choice only makes sense as a copy of an existing Rust codebase, that's a soft signal.
   - A clean "absent in all four" plus directly-Java-justifiable structural choices is a strong constraint-honored signal.

3. **Java-parity pin verification checklist** for `send` shape regression:
   - Type-level: does the test exercise an API surface that *would not compile* under the old shape? (e.g. calling `is_done()` on a non-future type)
   - Behavior-level: does the test exercise a behavior that *would not converge* under the old shape? (e.g. using a stub that never acks — the old inline-await shape would hang)
   - The good pin has BOTH axes. The `send_returns_pending_kafka_future` test has both:
     - Type-level: `RecordMetadata::is_done()` doesn't exist, compile-fails
     - Behavior-level: stub never acks, inline-await would hang the test
   - When reviewing parity pins, demand both axes. One-axis pins are weaker.

4. **Send-path allocation audit short form**: count `Arc::new` and `Box::new` on the send path. Phase 7g target = exactly one `Arc::new` (which is the existing `Arc<FutureRecordMetadata>` in `accumulator.append`); the `Arc<dyn KafkaFutureOps<T>>` is an unsizing coercion (zero-cost). Any second `Arc::new` or any `Box::new` in `Producer::send` / `send_with_callback` is a regression.

5. **Sync-throw vs failed-future deviation**: Java's `doSend` returns `new FutureFailure(e)` for `ApiException` (a synchronously-completed-failed `Future`), and `throws` for other exceptions. The Rust translation collapses both into outer `Result::Err`. This is acceptable IF:
   - User callback fires for `ApiException` before returning Err (preserves the "user gets notified" parity)
   - Interceptor `on_send_error` fires for all error types (preserves the "interceptor sees every error" parity)
   - Both verified in `do_send` (lines 1245-1267 in the Phase-7g shape) via `err.is_api_exception()` branch
