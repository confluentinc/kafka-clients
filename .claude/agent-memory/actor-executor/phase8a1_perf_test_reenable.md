---
name: Phase 8a.1 perf test re-enable
description: Patterns from re-enabling tests/integration/performance_test.rs against Phase 7g send shape - compile-only-gate hides runtime config gate
type: project
---

# Phase 8a.1 perf test re-enable

Re-enabled `tests/integration/performance_test.rs` against the Phase 7g
`Result<KafkaFuture<RecordMetadata>, KafkaError>` send shape.

## Patterns

1. **`KafkaProducer::from_config` is the third `pub + #[doc(hidden)]`
   on Java's non-API surface.** Same visibility-correction pattern as
   `DefaultMetadataUpdater` (Phase 8a.0 `45b70a2`) and
   `SupportsDefaultSerializer`. The reason recurs: `tests/integration/*`
   is a downstream crate, not same-crate test code, so `pub(crate)`
   makes the type/method unreachable. Whenever a Critic disposition
   demotes a `pub` constructor/type to `pub(crate)` "because only test
   code uses it", verify whether that test code lives in `src/...::tests`
   (same-crate, `pub(crate)` works) or in `tests/integration/*`
   (downstream crate, requires `pub`).

   **Why:** `c050fe8`'s Suggestion-2 disposition demoted `from_config`
   to `pub(crate)`; this turned out to block the perf test once it was
   re-enabled. The `pub + #[doc(hidden)]` shape is the safe answer
   that keeps the constructor reachable without growing docs.rs.
   **How to apply:** Audit any "demote pub → pub(crate) since only
   tests use it" suggestion against the test-code's actual crate
   membership before accepting.

2. **`ProducerConfig::new` runs Java's required-key validator BEFORE
   `from_config` swaps in the supplied serializer instances.** The
   `key.serializer` / `value.serializer` keys are *ignored* by
   `from_config` (the explicit `Box<dyn Serializer<T>>` arg wins) but
   the validator still demands the key be present.

   **Why:** Hit this on first smoke-run — compile passed, runtime
   panicked with "Missing required configuration `key.serializer`".
   The `with_serializers` path has the same gate; `producer_smoke_test.rs`
   ships placeholder FQCNs (`org.apache.kafka.common.serialization.ByteArraySerializer`)
   for both keys. **How to apply:** When wiring a new caller through
   `ProducerConfig::new` → `from_config`, also include FQCN placeholders
   for the two serializer keys. The Phase 8.0 `log::warn!` on
   FQCN-set-but-ignored fires for the placeholders, which is the
   documented expected behavior.

3. **`ByteArraySerializer` is `Serializer<[u8]>`; `ByteArrayOwnedSerializer`
   is `Serializer<Vec<u8>>`.** For `KafkaProducer<Vec<u8>, Vec<u8>>` the
   owned variant is the right one. The bare `ByteArraySerializer` exists
   for the hot-path zero-copy `serialize_to` call when the source is
   `&[u8]`.

   **Why:** Tripped on this — perf test imported `ByteArraySerializer`
   and the type checker rejected it. **How to apply:** Match the smoke
   test's pattern: `Box<dyn Serializer<Vec<u8>>> = Box::new(ByteArrayOwnedSerializer)`.

4. **`ProducerRecord::with_key(...)` returns `Result<Self, _>` now.**
   Old code shapes that just bind to a variable need `.expect(...)` or
   `?`. **How to apply:** Mechanical fix; smoke test uses `.expect`
   form because test inputs are static and a panic = test bug.

5. **Compile-gate ≠ runtime-gate.** The brief said "smoke-run from
   Step 4 — verify the test is runnable." If I'd skipped that and just
   wired in the mod after compile-check, I would have shipped a test
   that compiles but panics on every run. Always run the smoke-run gate
   even for "purely mechanical" re-enables.
