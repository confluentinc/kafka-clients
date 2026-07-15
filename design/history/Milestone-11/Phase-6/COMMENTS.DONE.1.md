# Critic 1 — Milestone 11 Phase 6 BLOCKING findings — RESOLVED

Both blocking findings from the Phase 6 review are fixed (fixups `225dd26`,
`b637a27` on `milestone9-share-consumer`).

## RESOLVED: `acknowledge(record, RENEW)` silently lost record re-delivery
- **Fixup**: `225dd26` (fixup! blocker 1, `ff3950f`).
- **Resolution**: RENEW re-delivery is now implemented faithfully. `acknowledge(_,
  RENEW)` captures a CLONE of the renewed record (new
  `ShareInFlightBatch.renew_records` + a gated `ConsumerRecord: Clone` derive) —
  the rare path only; the hot ACCEPT/RELEASE/REJECT path records only the offset
  (no clone, §27/§11-compliant). `take_acknowledged_records` routes the captured
  clone (or the still-in-flight object) into `renewing_records`; `renew` /
  `take_renewals` cycle it back into in-flight for re-delivery on a later poll,
  matching Java (`ShareInFlightBatch.java:115-184`, `ShareConsumerImpl.java:709-724`).
  `ShareConsumerImpl<K,V>` gains a share-only `K/V: Clone` bound.
  `test_explicit_mode_renew_and_acknowledge_on_poll` is un-`#[ignore]`d and passes,
  and additionally asserts a post-renew `acknowledge(rec, ACCEPT)` on the
  re-delivered record succeeds (would `Err` if offset tracking had been dropped).

## RESOLVED: `RequestManagers::entries()` share poll order reversed
- **Fixup**: `b637a27` (fixup! blocker 3, `7623a72`).
- **Resolution**: `entries()` now polls `share_heartbeat` BEFORE `share_consume`,
  matching Java's share order `shareHeartbeat → shareMembership → shareConsume`
  (`RequestManagers.java:123-128`), so the consume manager acts on membership
  state already advanced by the heartbeat manager in the same `run_once` iteration
  (§10). Added a `share_membership: Option<Arc<ShareMembershipManager>>` slot
  (Arc-shared with `share_heartbeat`, the analog of
  `consumer_membership`/`consumer_heartbeat`), skipped from `entries()` like
  `consumer_membership` (a `&mut dyn RequestManager` cannot be produced from a
  shared `Arc`); its standalone reconcile driving is tracked for Phase 7. The
  misleading `share_consume` field doc/comment is corrected.

Verify: `cargo build`, `cargo test --lib` (2250 pass / 1 ignore / 0 fail),
`cargo xtask format`, `cargo xtask lint` — all green.

---

# Critic 1 — Phase 6 fixup RE-REVIEW regression — RESOLVED

## RESOLVED: `collect()` in-place restructuring clobbered `acquisition_lock_timeout_ms` on an empty collect
- **Fixup**: `daf4c47` (fixup! `373f307` — the ShareConsumerImpl impl commit; the
  regression was introduced by the in-place `collect` restructuring in `225dd26`).
- **Resolution**: `collect`'s first (non-renewal) branch now assigns
  `self.current_fetch = fetch` ONLY when the freshly collected fetch is non-empty,
  matching Java's `poll` guard (`ShareConsumerImpl.java:628-629`). An empty collect
  leaves `current_fetch` untouched, so it retains the `acquisition_lock_timeout_ms`
  from the last non-empty fetch (which survives `take_records` /
  `take_acknowledged_records`). Renewal-state handling is unchanged (branch gated on
  `!has_renewals()`). Added
  `test_acquisition_lock_timeout_retained_across_empty_poll`: poll returns records
  (asserts `Some(30_000)`), then an empty poll still returns `Some(30_000)` (would be
  `None` under the regressed code).

Verify: `cargo build`, `cargo test --lib` (2251 pass / 1 ignore / 0 fail),
`cargo xtask format`, `cargo xtask lint` — all green.
