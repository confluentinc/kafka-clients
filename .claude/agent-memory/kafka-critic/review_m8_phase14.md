---
name: review-m8-phase14
description: Phase 14 bg-event wakeup — Notify-as-Selector.wakeup translation, cancel-safety audit heuristics
metadata:
  type: project
---

Phase 14 fixed the ~5s fetch-latency bug: `ApplicationEventHandler::add()` sent
on the mpsc channel but never woke the bg task, which was parked in
`poll_default` for up to `MAX_POLL_TIMEOUT_MS=5000`. Java's `add()` is two
statements — `queue.add(event); wakeupNetworkThread()` — and only the first was
translated. Fix: shared `Arc<tokio::sync::Notify>`, `notify_one()` after send,
new `_ = self.event_notify.notified()` arm in `run_once`'s Phase-4 `select!`.

**Why this is a recurring translation-bug class:** when §10 mandates draining
via `try_recv` instead of `recv().await`, the channel send no longer wakes the
consumer task. Any Java `add`/enqueue that is followed by a `wakeup()` /
`Selector.wakeup()` MUST get a separate Rust wake primitive. Audit: for every
mpsc producer feeding a bg task that drains via `try_recv`, grep the Java for a
`wakeup()` call right after the enqueue.

**Cancel-safety audit heuristic for `Notify` in a `select!` arm:** dropping a
notified-but-unconsumed `notified()` future is safe (restores the permit) only
in modern tokio. Verify the actual `Cargo.lock` tokio version (1.52 here) rather
than trusting the PLAN's claim. The permit-restore-on-drop is what makes the
`poll_default`-wins race lossless.

**`biased` + Notify starvation check:** a `notified()` arm placed before the
poll arm does NOT starve the poll, because the permit is consumed once per
first-poll; only a continuous enqueue stream keeps it ready (same back-pressure
as Java). Shutdown/user-wakeup token arm must stay first.

**Test-teeth pattern that worked:** a mock client with a `poll_block` gate
(AtomicBool default false, only the regression test flips it) that parks `poll`
forever, plus a `tokio::time::timeout` guard on `run_once`. Pre-fix the select!
hangs → timeout fails; post-fix the stored permit drives the new arm. The
default-false gate means no other test can block on it — no flakiness.

This was a clean fix — no false positives to report, no rule change needed.
The metrics gap (Java's recordApplicationEventQueueSize in add()) is the
project-wide AsyncConsumerMetrics deferral, NOT a regression — don't flag it.
