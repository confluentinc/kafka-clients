---
name: review-notify-waiters-race
description: tokio Notify notify_waiters() is race-free with create-future-then-check-flag; enable()/pin! are NOT required — corrects an earlier wrong note
metadata:
  type: feedback
---

`tokio::sync::Notify` + `notify_waiters()` is **already race-free** with the
pattern:

    let notified = self.notify.notified();
    if flag.load(SeqCst) { return ...; }
    notified.await;

Do NOT flag a missing `tokio::pin!` / `Notified::enable()` here.

**Why:** verified against the vendored tokio 1.52.0 source.
`Notify::notified()` captures `notify_waiters_calls` at *construction*
(`sync/notify.rs:565-575`), and `poll_notified`'s `State::Init` arm compares the
captured value against the live counter twice (`:1124` before the lock, `:1156`
with the lock held), returning `Poll::Ready` when they differ. So a
`notify_waiters()` landing between construction and the first poll is detected,
not lost. `enable()` matters for `notify_one()` permit ordering, not for the
broadcast path.

The setter must still store its flag **before** calling `notify_waiters()` — that
part is a real thing to check.

**How to apply:** when reviewing a `Notify`-based completion primitive, check
(a) which notify method is used, (b) flag-store-before-notify ordering, and
(c) whether an outer `loop` re-checks after wake. Only require
`pin!` + `enable()` for `notify_one()`-based waits where permit stealing
matters. An earlier memory of mine
(`review_m8_phase7a_fix_cycle.md`) asserted `enable()` was mandatory for
`notify_waiters()`; that was wrong and has been corrected there.

Related: [[review-m11-phase1]], [[review-m8-phase7a-fix-cycle]]
