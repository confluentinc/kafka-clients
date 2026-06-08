---
name: phase2-design-notes
description: Milestone-8 Phase 2 trait surface decisions — &mut on ConsumerInterceptor::on_consume, inline tests for pub(crate), Drop bounds on generic interceptor container, build-off-to-side panic pattern
metadata:
  type: project
---

Milestone-8 Phase 2 ships the public trait surface (Consumer<K, V>,
ConsumerRebalanceListener, OffsetCommitCallback, ConsumerInterceptor,
Deserializer<T>) plus internal container types (ConsumerInterceptors,
Deserializers).

**Why:** Records the non-obvious translation decisions made during the
phase so future phases (3 = MockConsumer, 11 = AsyncKafkaConsumer) and
reviewers know why these shapes were chosen.

**How to apply:** Reference when implementing types that interact with
the Phase 2 traits, or when adding new generic containers that hold
`Box<dyn>` and have a `Drop` impl with `catch_unwind`.

## Key decisions

- **`ConsumerInterceptor::on_consume` takes `&mut ConsumerRecords<K, V>`**
  (NOT owned value, NOT requiring `K: Clone, V: Clone`). The initial
  Phase 2 design used owned ConsumerRecords + Clone to support panic
  recovery (next interceptor sees previous-good batch via pre-clone),
  but that leaked `K: Clone, V: Clone` into the public `Consumer<K, V>`
  trait — bounds Java does not impose. Fixed in commit 71aa270:
  `&mut` form with catch_unwind, where panic-recovery becomes an
  interceptor-side discipline (build off to the side, commit at the
  end via `*records = new_batch;`).

- **`ConsumerRecord` / `ConsumerRecords` derive `PartialEq, Eq, Debug`**
  (gated bounds, NOT Clone). The derives let tests use `assert_eq!`
  against entire batches matching Java's `assertEquals(...)`. Bounds
  gated by the derive so users with non-`PartialEq` K/V are unaffected.
  All field types support PartialEq/Eq: Arc<str>, i*, TimestampType,
  RecordHeaders, OffsetAndMetadata, IndexMap, HashMap.

- **Anti-pattern: `std::mem::take(records)` followed by fallible work**.
  Documented explicitly in `ConsumerInterceptor::on_consume` rustdoc
  and `ConsumerInterceptors` rustdoc. `mem::take` writes
  `ConsumerRecords::default()` (empty) to `*records` immediately; if
  anything fallible runs between the take and the final assignment,
  the next interceptor sees an empty batch — not the previous-good
  batch. Java's structural immutability prevents this; Rust must rely
  on interceptor discipline (build off to the side, single
  non-fallible commit). The `FilterConsumerInterceptor` test fixture
  is the in-repo reference example of the recommended pattern.

- **Generic containers with `catch_unwind` in `Drop` need `'static`
  bounds on the struct**: `catch_unwind` requires `Send + 'static` on the
  closure. Drop impls in Rust cannot have stricter bounds than the
  struct, so we put `K: 'static, V: 'static` on the struct itself:
  `pub(crate) struct ConsumerInterceptors<K: 'static, V: 'static>` and
  `pub(crate) struct Deserializers<K: 'static, V: 'static>`. The trait
  objects inside (`Box<dyn ConsumerInterceptor<K, V>>`) already require
  `'static`, so this bound is always satisfied in practice.

- **Inline `#[cfg(test)] mod tests` for `pub(crate)` items**: Tests for
  `ConsumerInterceptors` live in
  `src/consumer/internals/consumer_interceptors.rs` (NOT in
  `tests/consumer/internals/consumer_interceptors_test.rs` as the PLAN
  suggested) because pub(crate) is unreachable from the tests/ binary.
  Producer's `internals/buffer_pool` uses the same pattern. The PLAN's
  path suggestion was a guideline that conflicts with the visibility
  requirement.

- **Trait surface check uses `pub use` from `tests/consumer/`**:
  `tests/consumer/trait_surface_check.rs` is reachable via the regular
  public API only — it verifies `Box<dyn Consumer<K, V>>` constructs and
  is `Send`. Intentionally NO `_assert_sync` (Consumer is Send-only).

- **Test interceptors use `Arc<FilterState>` instead of raw pointers**:
  The Java test toggles `throwExceptionOnConsume`/`throwExceptionOnCommit`
  on the interceptor instance after it's been added to the container.
  Initial implementation used `*const T` raw pointers to alias into the
  Box; switched to `Arc<FilterState>` shared between the interceptor's
  `self.state` and the test's read-side handle. Safer and no `unsafe`.

- **async-trait dependency added**: Was not previously in Cargo.toml.
  Used by `Consumer<K, V>`, `ConsumerRebalanceListener`,
  `OffsetCommitCallback`. NOT used by `Deserializer<T>` or
  `ConsumerInterceptor<K, V>` (both sync per DoD §11).

- **`new_consumer` factory is a Phase 2 stub**: Returns
  `KafkaError::unsupported_version` for both `GroupProtocol::Consumer`
  (AsyncKafkaConsumer not yet implemented — Phase 11) and
  `GroupProtocol::Classic` (deferred per consumer-threading.md §20).
  Signature is frozen; Phase 11 swaps the Consumer arm body only.
