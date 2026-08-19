---
name: review-m8-phase40
description: Phase 40 public-API integration tests — unsafe wakeup helper UB, provisioner-offset/timestamp contamination, offsets_for_times lossy null
metadata:
  type: project
---

Phase 40 = 20 single-broker public-API integration tests (plaintext_consumer_test.rs ×18, consumer_topic_creation_test.rs ×2), no prod change.

**Unsafe wakeup helper = UB (not just a smell).** `fire_wakeup_during` round-trips `*const Box<dyn Consumer>` through usize, derefs to `&dyn Consumer` and calls `wakeup(&self)` on a spawned task while main task holds `&mut **consumer` across `position_timeout`'s await. A live `&mut T` + concurrently-formed `&T` to the SAME object is UB under Stacked/Tree Borrows REGARDLESS of which fields are touched — the SAFETY comment's "only touches disjoint Arc-backed state" is a value-level fact, not the soundness criterion. **Why:** `&mut` confers whole-object noalias; forming `&` invalidates it.
- **How to apply:** when a test uses `unsafe` to call an `&self` method from another task while holding `&mut self`, it's UB. Safe fix = expose a `Send + 'static` handle (clone BEFORE the borrow). Here the consumer has NO public shareable wakeup handle (`Consumer::wakeup(&self)` only; factory returns bare `Box<dyn Consumer>`), so file an API gap. Internal precedent exists: `network_client_delegate.rs:594 wakeup_handle()->Arc<Notify>`.

**Provisioner contamination pattern (TWO bugs this phase).** Rust integration harness has no admin client, so tests fake `createTopic(n,parts)` with `ensure_topic_with_2_partitions` = a `with_partition` (no-timestamp) `__provisioner__` record at offset 0. Java's `createTopic` makes an EMPTY topic. Two failure modes:
1. **seek-to-hard-0 + read records[0]** (headers test): record under test lands at offset 1 (provisioner at 0); `seek(tp,0)` + `consume_records(1)` returns the provisioner → header assertions panic. Fix: offset expectations by `base = end_offset(...)`, like every OTHER provisioned test does.
2. **offsets_for_times timestamp index** (fetch-offsets-for-time test): provisioner at offset 0 has broker-stamped wall-clock CreateTime (large +ve). `offsetsForTimes(ts)` returns EARLIEST offset with ts>=target, so it resolves to the provisioner (offset 0, big ts) not the real `ts==0` record at `base`. The in-code comment even DESCRIBED the fix ("create distinct topics without provisioner") but the code never applied it — describing-but-not-applying is a recurring trap.
- **How to apply:** any provisioned test that asserts on absolute offset 0, or on a timestamp→offset mapping, is suspect. `with_partition`/`with_partition`-no-ts ⇒ broker stamps wall-clock CreateTime.

**offsets_for_times lossy null.** Rust returns `HashMap<TP, OffsetAndTimestamp>` (non-nullable value); Java returns map with present-key→null for "queried but unresolved". Rust collapses to key-ABSENT. Loses Java's present-null vs absent distinction API-wide (not just zero-timeout). Acceptable for milestone but should be a documented return-type contract note. Test faithfully adapts (`!contains_key`).

**Acceptable-deviation calls validated this phase:** pause-not-preserved test asserts "tp drops from assignment" — STRONGER than Java's `consumeAndVerifyRecords(...,0,...)` numRecords=0 no-op, so fine. Interceptor skip justified (no public seam: `new` builds empty chain, `new_with_components` pub(crate)); but `TODO(milestone-N-interceptors)` at async_kafka_consumer.rs:868 violates CLAUDE.md §5. Gating centralized in main.rs `#![cfg(feature="integration-tests")]`, no per-file cfg.
