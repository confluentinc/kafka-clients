---
name: phase40-critic-round1-patterns
description: Phase 40 Critic round-1 fixes — safe shareable WakeupHandle API (closes cross-task wakeup gap), provisioner-timestamp test bugs, TODO→doc, offsets_for_times lossy-mapping doc
metadata:
  type: feedback
---

Reusable fix patterns from Phase 40 Critic round 1 (commit fixup of 4ad5a11).
Links: [[phase39_critic_round1_patterns]], [[phase11_critic_batch2_patterns]].

1. **Cross-task wakeup() needs a Send+'static handle, NOT unsafe pointers.**
   A test that fires `consumer.wakeup()` from another task while the main task
   holds `&mut consumer` across an await CANNOT use a raw-pointer round-trip
   (`*const Box<dyn Consumer> as usize` → `&dyn Consumer`): forming any `&T`
   while a `&mut T` to the same object is live is UB under Stacked/Tree Borrows
   regardless of which fields are touched. **Why:** the optimizer derives
   `noalias` from the `&mut`. **How to apply:** expose a `WakeupHandle`
   (Clone+Send+'static) that captures only the Arc-backed wakeup state — for
   AsyncKafkaConsumer that's the `WakeupTrigger` (already Arc/watch-backed) +
   the bg-task notify closure (change `Box<dyn Fn>`→`Arc<dyn Fn>` so it clones);
   for MockConsumer change `AtomicBool`→`Arc<AtomicBool>`. Add
   `Consumer::wakeup_handle(&self) -> WakeupHandle` on the trait + both impls.
   Obtain the handle BEFORE the `&mut` borrow, move it into the spawned task.
   This is a real Java-parity API gap (Java `Consumer` is freely shareable for
   `wakeup()`), perf-neutral (off hot path, opt-in clone only).

2. **Provisioner records poison timestamp/offset-pinning tests.**
   `ensure_topic_with_2_partitions` writes a `__provisioner__` record at offset
   0 with a broker wall-clock `CreateTime` ts. Two failure modes: (a) a test
   that `seek(tp, 0)` + reads `records[0]` reads the provisioner not the real
   record — fix by `seek(tp, base)` where `base = end_offset(...)`; (b) an
   `offsets_for_times(ts=small)` test resolves to the provisioner (its huge
   wall-clock ts is `>=` any small target) — fix by NOT provisioning: send the
   timestamped records directly (first send auto-creates with
   `KAFKA_NUM_PARTITIONS=2`), so offset 0 IS a real `ts==0` record and base=0.

3. **CLAUDE.md §5: convert TODO→documented-limitation comment, keep the
   detail.** A live `TODO(...)` in production for an untranslated feature
   (config interceptor.classes loading) must lose the TODO/FIXME token but keep
   the precise gap description + the inject seam (`new_with_components`,
   pub(crate)).

4. **Lossy Java→Rust map shape needs a contract note on the return type.**
   `offsets_for_times` returns `HashMap<TP, OffsetAndTimestamp>` (non-nullable
   value), so unresolved partitions are OMITTED (key absent) vs Java's
   present-with-null. Document on BOTH the trait methods AND the
   `OffsetAndTimestamp` type rustdoc so callers porting `keySet()` iteration
   aren't surprised.

5. **xtask lint does NOT cover integration-tests cfg.** Verify touched
   integration files with `cargo clippy --features integration-tests --test
   integration` and confirm zero warnings reference YOUR file specifically
   (pre-existing warnings in sibling integration files are out of scope).
