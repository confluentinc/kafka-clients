---
name: review-m10-phase2-share-ffi
description: M10 Phase 2 share-consumer C-FFI teardown-UAF fix soundness + test-teeth heuristics
metadata:
  type: project
---

Milestone-10 Phase 2 = share-consumer C FFI (`src/ffi/share_consumer.rs` +
shared `src/ffi/records.rs`). Reviewed the teardown-safe guard rewrite that
diverges from the reference `src/ffi/consumer.rs` on
`origin/dev/c_and_python_consumer_bindings`.

**Why:** The teardown rewrite was flagged as the highest-risk item; the fix is
sound and the reference genuinely has the UAF it fixes.

**How to apply (reusable analysis facts):**
- tokio **multi-thread `Runtime::drop` BLOCKS** until worker threads join (that's
  why dropping a runtime inside an async ctx panics). So `drop(runtime.take())`
  before `drop(handle)` guarantees no spawned task dereferences the `&'static`
  handle after the box is freed. The reference's `runtime.shutdown_background()`
  is **non-blocking** → spawned task can `consumer_mut(&'static freed_handle)`
  and its completion job can `release(&'static freed_handle.owner)` → real UAF
  (SIGBUS under parallel runner). The sibling regular-consumer FFI shares this
  bug; flag it when that FFI lands.
- The sound fix has TWO independent mechanisms — audit both: (1) guard owner cell
  is `Arc<AtomicU64>` so completion jobs release through their own clone (job
  closures must capture ONLY owner + owned result handles, never the handle
  `hs`); (2) blocking `drop(runtime)` before the box free. Reverting either
  reintroduces UAF.
- Async-bridge invariant to re-verify every phase: acquire-at-submit →
  build-handles-after-await → release-in-job-before-callback; capture `&'static
  Handle` not `*mut` (keeps future Send); inline-reject path takes no guard so
  must NOT release.

**Test-teeth heuristics (led to Phase-2 findings):**
- A "destroy-after-in-flight-async" regression test on a MOCK is usually a
  no-op: the mock op finishes + enqueues its job before `destroy` runs, so the
  protected in-flight window is never hit. Demand a loop (100-1000x) and/or a
  test-controlled barrier that blocks the mock op until destroy is entered.
- `MockShareConsumer::acknowledge*` ignore the record (`_record`) and always
  return `Ok(())`. So acknowledge tests over the mock validate only
  guard+forwarding, NOT record consumption or the `IllegalState "The record
  cannot be acknowledged."` message (needs broker/integration). Same no-op-mock
  trap likely applies to other mock methods — check the mock body before trusting
  an FFI test that drives it.

**Verified non-findings (don't re-report):**
- `Bytes::copy_from_slice` in `MockShareConsumer_add_record` is an ingestion
  boundary (caller-owned C buffer), not a §27/§12 zero-copy violation. Receive
  path (poll→accessors) stays zero-copy via `.as_ptr()` borrows.
- `illegal_state` for guard rejection is acceptable: no
  `KafkaError::concurrent_modification` on this branch (reference had one).
- ack-mode need not be re-seeded in the FFI: `ShareConsumerConfig::from_properties`
  defaults `share.acknowledgement.mode=implicit` and `build_share_consumer` seeds
  it (mod.rs:649,912). blank-group.id rejected in build_share_consumer (mod.rs:654).
- `box_records` flat `*const ConsumerRecord` index is sound: `ConsumerRecords`
  owns `IndexMap<TP, Vec<ConsumerRecord>>`; `&ConsumerRecords` iter borrows into
  heap Vecs; boxing/into_raw never moves them.
- `cargo xtask lint` DOES run clippy `--features ffi` (main.rs:141-147) — green
  lint is meaningful for the C surface, not a blind spot.
- Enum discriminants ACCEPT=1..RENEW=4 match `AcknowledgeType::id()` wire ids.
</content>
