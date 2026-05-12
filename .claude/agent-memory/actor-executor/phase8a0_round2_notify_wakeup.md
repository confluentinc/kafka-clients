---
name: Phase 8a.0 Round 2 - Notify-based wakeup
description: `tokio::sync::Notify` pattern for cross-task wake of a `tokio::select!`-parked Selector; replaces Java's `selector.wakeup()`
type: project
---

Phase 8a.0 Round 2 Suggestion 1 replaced the long-standing no-op
`KafkaProducer::sender_wakeup` (Phase 7d stub) with a real
`tokio::sync::Notify`-backed wake mechanism. Pre-fix, every clean
close on a healthy broker took ~30s because the only wake-up
mechanism bounding Sender wake-up latency was the
`default.request.timeout.ms` cap inside `Selector::poll`'s
`tokio::select!` sleep arm.

**Why:** Java's `selector.wakeup()` is exactly the
`Notify::notify_one()` primitive — wake a thread parked in
`Selector::select(timeout)`. The Rust translation needed an
equivalent for the same Sender→close-drain contract.

**How to apply:**

1. **Where the Notify lives.** The `Selector` owns
   `wakeup_notify: Arc<Notify>`. Its `Selector::wakeup()` impl
   calls `notify_one()`. Its `Selector::poll`'s `tokio::select!`
   gets a new `_ = wakeup_notify.notified() => {}` arm that short-
   circuits the sleep-for-timeout arm.

2. **How callers reach it after `Selector` is moved into a task.**
   `Selector::wakeup_notify_handle() -> Arc<Notify>` clones the
   handle pre-move. `KafkaProducer::new` extracts the handle from
   the freshly-built Selector before `NetworkClient::new` takes
   ownership, stores it on the producer as
   `sender_wakeup_notify: Option<Arc<Notify>>`. Then
   `KafkaProducer::sender_wakeup` becomes `notify_one()` on the
   handle, or no-op when `None` (mock-injected clients).

3. **Cancellation safety.** `Notify::notified` is documented
   cancellation-safe per CLAUDE.md rule 9.6: a pending permit
   survives the losing-arm drop. No side effects in the arm body
   (just `=> {}`), so the wakeup arm losing to e.g. the
   read-readiness arm is invariant-preserving.

4. **Why `Option<Arc<Notify>>` not just `Arc<Notify>`.**
   `KafkaProducer::new_for_test` accepts an injected mock
   `KafkaClient` whose internal Selector (if any) isn't visible
   to the producer. The `Option::None` represents "no Selector to
   wake" — sender_wakeup becomes a documented no-op, matching the
   pre-Round-2 behaviour. Production constructors always supply
   `Some(handle)`.

5. **Belt-and-suspenders backstop.** `NetworkClient::poll`'s
   `effective_timeout = ... .min(self.default_request_timeout_ms
   as i64)` cap remains. Pre-Suggestion-1 it was load-bearing
   (the only wake mechanism); post-Suggestion-1 it is a defensive
   floor against a missed-wake regression. Documented in
   network_client.rs:1153 — do not remove even if it looks
   redundant.

**Lib-test regression coverage:** Two tests added in
`src/common/network/selector.rs::tests`:
- `poll_wakes_when_notify_one_is_called` — pins the Notify arm.
- `poll_wakes_when_socket_becomes_readable` — pins the
  read-readiness arm (the Phase 8a.0 `480d304` fix).

Both wrap the inner `selector.poll(5000)` call in
`tokio::time::timeout(2_000ms, ...)` so a regression fails the
test in 1-2 s instead of the full 5 s poll timeout. Inner elapsed
assertion bounds the wake at 200 ms (Notify) / 500 ms (read).

**Measurement:** Manual integration run, 50 small records on
localhost: close drained in **2.697 ms** vs. ~30s pre-fix.
Three consecutive Round 2 verification runs: 4.2 / 2.2 / 3.6 ms.

**Files touched in `397dc09`:** `src/common/network/selector.rs`,
`src/producer/kafka_producer.rs`. Only two files — the Notify is
a pure Selector concern; KafkaProducer's only role is to extract
and store the handle. NetworkClient is unaware of the Notify
(it just calls `selector.poll(timeout)` and lets the Selector
internals handle the wake).
