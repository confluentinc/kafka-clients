---
name: Phase 8a.0 Round 2 review patterns
description: Verified-good fix shapes for Java selector.wakeup() -> Tokio Notify translation; close-drain end-to-end vs wake-primitive timing distinction
type: project
---

## Verified-good fix shapes (Phase 8a.0 Round 2)

### `Selector::wakeup` no-op -> `Arc<Notify>` with `notified()` select-arm

**Shape**:
- `Selector` owns `wakeup_notify: Arc<Notify>` field.
- `Selector::wakeup_notify_handle() -> Arc<Notify>` accessor returns `Arc::clone(&self.wakeup_notify)`.
- `Selector::poll`'s `tokio::select!` adds `_ = wakeup_notify.notified() => {}` arm with `biased;` so wake fires before falling into the 30s sleep.
- `KafkaProducer` stores `Option<Arc<Notify>>` extracted via `selector.wakeup_notify_handle()` **before** the Selector is moved into `NetworkClient::new(selector, ...)`. Critical: extract pre-move.
- `KafkaProducer::sender_wakeup` calls `notify.notify_one()` (or no-ops for the `None` test path).

**Why this works**:
- `Notify::notified()` is documented cancellation-safe — losing the arm in a `select!` does not lose the pending permit.
- `notify_one` is a constant-time atomic CAS — no allocation, no spawn. Matches Java's `Selector.wakeup()` semantics (single-permit coalescing — multiple `notify_one`s collapse to one until the next `notified()` call).
- Extracting the `Arc<Notify>` pre-move gives the producer a handle that outlives the move of `Selector` into `NetworkClient` into the Sender's `tokio::spawn` task.

**`None` path audit**: search every `KafkaProducer::new_for_test` callsite and confirm all are `#[cfg(test)]`. The `None` no-op must be unreachable from production constructors (`new`, `with_serializers`, `from_config`).

### `default.request.timeout.ms` cap as belt-and-suspenders

Pre-Suggestion-1 the `effective_timeout = timeout_ms.min(metadata_timeout).min(default_request_timeout_ms)` cap in `NetworkClient::poll` was load-bearing — the only wake bound. With the Notify wake landed, the cap becomes defensive. **Rustdoc must explicitly say "Do not remove this `.min()` even if it looks redundant"** — a future refactor that "simplifies" the cap would silently regress to `i64::MAX`.

## Close-drain timing distinction — wake-primitive vs end-to-end

When evaluating "how fast does close drain after a Notify fix":

- **Wake primitive latency** (microseconds): the Notify itself fires in ~us on a healthy executor. Lib-level test (`poll_wakes_when_notify_one_is_called`) asserts < 200 ms with a generous bound; the real number is single-digit ms at worst.
- **End-to-end close-drain latency** (milliseconds): integration test wall-clock for "close called → 50 records acked + drained". This includes broker-side fsync (acks=all on a Testcontainer round-trip). 2-5 ms is honest, NOT a regression — wake latency is dwarfed by broker latency.

When an Actor reports "close drained in microseconds" but the test wall-clock shows ms, **the ms is correct** — Actor was conflating wake-primitive latency with end-to-end drain. Do not file this as a regression.

## Watchdog test ceiling sizing

For close-drain or wake-bound tests:
- Pick a ceiling ~1000× the measured drain on the actual platform.
- Use strict-less (`<`) not `<=` so regression is deterministic.
- Pick small enough to fail-fast on a missed-wake regression (5 s for the wake path, not 30 s+).
- The pre-fix backstop timeout (`default.request.timeout.ms`, 30 s) is NOT the right test ceiling — that's the floor the test must detect a violation of.

## Round 1 follow-up verification checklist (applied this round)

1. Trace the `Arc<X>` end-to-end with grep + read the constructor: same instance throughout the wiring chain.
2. Check the `select!` keyword (`biased;` vs none) and verify the arm body is empty / no side effects.
3. Audit the `Option<X>` `None` arm: is it reachable only from `#[cfg(test)]` callers?
4. Verify the regression test would **fail** if the fix were reverted in-place (mental simulation: which arm now blocks for the full timeout?).
5. Confirm the rustdoc on any belt-and-suspenders backstop says "do not remove" with a specific reason.
6. Cross-check the archive: every Round-1 finding has a `Disposition: Fixed in commit <sha>` line citing the fixup SHA.
