---
name: Phase-8a.0 review patterns
description: Tokio-Selector scheduling-layer translation patterns; no-op-wakeup root cause; "fix-points-to-deeper-bug" review framing
type: project
---

# Phase 8a.0 review patterns — Tokio Selector + Sender wake-up

## Headline finding pattern: "the fix is correct, but the test now shows a deeper bug"

Phase 8a.0 fixed a scheduling-layer omission (`Selector::poll` didn't wake on socket read-readiness). The fix is correct and unblocks the integration test. But the same test now reveals a **30.005s consistent close-drain timing** that the fix doesn't address: the producer's `sender_wakeup()` is a documented Phase-7d no-op, so when a producer is mid-`selector.poll(30000)` and records are appended to the accumulator, nothing wakes the Sender — it sleeps for the full request-timeout window.

**Why this matters for reviews**: when a fix unblocks a previously-hanging test, the test starts producing measurable timing data that exposes secondary bugs. Always check if the elapsed-time numbers in the now-passing test are explained by:
1. The thing the fix addressed (expected).
2. A separate, latent bug the fix didn't touch (filed as a finding).
3. A test artifact (only acceptable if the timing is contractually irrelevant).

## The 30.00s clustering: how to recognize it

If a test shows times clustered at exactly the value of `default.request.timeout.ms` (with sub-microsecond precision), suspect a Tokio `tokio::time::sleep(timeout)` arm that's the **only** load-bearing wake in the path. The trace pattern:

1. Producer call path: `KafkaProducer::send` → `do_send` → `accumulator.append` → `sender_wakeup()` (no-op) → return future awaiting broker ack.
2. Sender is blocked in `client.poll(timeout)` → `selector.poll(min(timeout, default_request_timeout_ms))`. With nothing in-flight, the only wakers are timer + connect-event mpsc + read-readiness arm.
3. Read-readiness can't fire because broker is silent (no in-flight request → no response).
4. Sender wakes when the 30s timer hits, processes the just-appended batches, sends produce requests, gets acks. **The send→ack latency is ~30s + actual broker latency.**

Verifying the root cause: read `KafkaProducer::sender_wakeup` body. If it's a `// Intentional no-op`, the bug is right there.

## Tokio Selector wake-on-read fix anatomy

Java's `nio.Selector.select(timeout)` wakes on any of: `OP_READ`, `OP_WRITE`, `OP_CONNECT`, `OP_ACCEPT`, or explicit `Selector.wakeup()`.

Rust translation needs equivalents:
- `OP_CONNECT` → connect-task mpsc (already present in Phase 5c-2).
- `OP_READ` → per-channel `TcpStream::poll_peek` against a 1-byte scratch buffer. Non-destructive — the byte stays in the kernel buffer. **Add this** if `Selector::poll` only races against `tokio::time::sleep`.
- `OP_WRITE` → can be modeled as `has_send()` short-circuit in `has_immediate_work` (always retry the write next tick when something is queued). Acceptable approximation as long as the next tick comes quickly. Becomes a latency problem if combined with a long sleep.
- `Selector.wakeup()` → `tokio::sync::Notify` field on Selector + arm in the `select!`. **This is currently missing** — see Suggestion 1 in COMMENTS.8.md Phase 8a.0.

## `Sync` bound on `Box<dyn Transport + Send>` — when it's justified

If you need to hold `&(dyn TransportLayer + Sync)` references across an `.await`, the trait object must be `Sync`. `&T: Send` iff `T: Sync`. Production transports (`PlaintextTransportLayer`, `SslTransportLayer`) are typically already `Sync` (no `Cell`/`RefCell` interior mutability that isn't `Sync`-safe). Adding `+ Sync` to the alias just makes the implicit constraint explicit.

This is NOT scope creep when the Selector legitimately needs to cross `.await` with borrowed references — it's load-bearing.

## Hex-fixture nuance: who emitted the bytes

PLAN.md says "capture hex fixtures from the Java client and assert bytes literally". Two interpretations:
1. **Strong**: bytes captured from a Java `KafkaProducer` emission, asserted equal to Rust client's emission.
2. **Weak**: bytes the Rust client emits, verified by a Java broker accepting them.

The strong form catches "Rust emits structurally different bytes that happen to be wire-compatible by accident". The weak form catches "Rust emits bytes the broker rejects". Both are useful. When reviewing a fixture, check the rustdoc and the bytes — if they were captured from the broker's *response*, that's the strong form (broker emits). If captured from the client's request, that's the weak form unless explicitly recaptured from Java's emission.

Severity for "weak-form only" fixture: **Nit** (the broker acceptance is a strong wire-compat signal; PLAN.md's intent is met in spirit).

## When the actor packs unrelated fixes into a Phase fixup

Phase 8a.0's fixup commit `480d304` touched only the new wake-on-read fix per `git diff 480d304^ 480d304`. But the **cumulative diff from prior accepted commits** (Phase 5c-2 round-2 fixups) shows a much larger surface (LRU `VecDeque → BTreeSet`, `clear()` reordering, `connect_socket` socket options). When reviewing a "small" Phase X.Y fixup, always check:
- `git diff X.Y_fixup^ X.Y_fixup` — what THIS commit actually changes.
- `git diff <last-accepted-baseline> X.Y_fixup` — the full cumulative surface.

Don't conflate the two. The first defines the new review scope; the second is the audit trail.

## Verification fast-checks for "fix correctness" claims

For a Tokio-async fix:
1. `grep "tokio::select!" src/path/to/changed.rs` — count arms, audit cancellation safety per CLAUDE.md 9.6.
2. `grep "unsafe" src/path/to/changed.rs` — should be zero new unsafe (unless an exception is documented).
3. Run `cargo test --lib` to spot-check passing count.
4. Run `cargo build --features integration-tests` to confirm downstream-crate types still resolve.
5. `grep "TODO\|FIXME"` in changed files.

All four took <5 minutes total in Phase 8a.0 review. High-yield.
