---
name: phase8a-round1-patterns
description: Phase 8a smoke-test scaffold review patterns; NOTES.md scope-vs-DoD conflict trap, Arc<AtomicBool> as cheaper alternative to Arc<NetworkSend>, #[doc(hidden)] cordon audit checklist
metadata:
  type: project
---

# Phase 8a Round 1 review patterns — scaffold + send-ref-share + visibility cordon

Cross-references: [[phase8_0_review_patterns]], [[phase8a0_review_patterns]], [[phase8a0_round2_patterns]].

## NOTES.md textual conflict trap — sub-phase table vs DoD checklist

`Phase-8/NOTES.md` has **two** specifications of what 8a must assert:

1. **Sub-phase table row (line 18)**: "assert acks + RecordMetadata partition/offset shape" — partition consistency + monotonic offsets explicitly tagged to **8b**.
2. **DoD #3 sub-checklist (lines 79-88)**: lists partition consistency + monotonic offsets as required for every sub-phase test.

These two read differently. The user's review-prompt brief surfaced this as a question. Resolution choice: defer to the sub-phase table (more specific) and amend DoD #3 to clarify "8b-onward" for those items.

**Pattern for future critic reviews**: when NOTES.md contains both a per-sub-phase table AND a general DoD checklist, both are claims about scope. If they disagree, raise as a **Suggestion** for Manager clarification, not a Blocking. The Actor can't be faulted for following one over the other; the spec needs to be the single source of truth before close.

## Send-reference sharing: `Arc<AtomicBool>` over `Arc<NetworkSend>`

The phase-8a.2 review brief expected `Arc<NetworkSend>` for share. Actor chose `Arc<AtomicBool>` wrapped in a `SendCompletion` handle. **The cheaper choice is also correct** — `InFlightRequests::can_send_more` reads only one bit (`peekFirst().send.completed()`); sharing the full `NetworkSend` would require either `Arc<Mutex<NetworkSend>>` (lock on hot path) or pervasive interior mutability. The `Arc<AtomicBool>` shares exactly what the contract reads. Java pays the same one-allocation-per-send cost (`new InFlightRequest` + `new NetworkSend`); Rust pays one `Arc::new(AtomicBool::new(...))`.

**Verification path**:
1. Read `InFlightRequests.canSendMore` in Java (`InFlightRequests.java:90-103`). Confirm only `send.completed()` is dereferenced.
2. Read `NetworkClient.doSend` (`NetworkClient.java:601-618`). Confirm the shared `Send` reference is only used to call `completed()` from the inflight side — nothing else.
3. If the contract reads only the bit, `Arc<AtomicBool>` is sufficient and **preferred** over `Arc<NetworkSend>` for hot-path economy.

**Test-fidelity gotcha exposed by the fix**: a passing pre-fix test (`disconnect_with_multiple_in_flights_fans_out_in_order`) only passed because the gating bug existed (back-to-back `send()` without intervening `poll()` was incorrectly accepted). The Actor correctly identified and re-aligned the test with Java's `NetworkClientTest.testDisconnectWithMultipleInFlights:1057-1098`, where `MockSelector.send()` queues to `initiatedSends` and only `poll()` flushes (`MockSelector.java:142`). **Whenever a gating bug is fixed, audit every existing test that ran through the gate to verify it still tests what it claims.**

## `#[doc(hidden)]` visibility-cordon audit checklist

When Java has a package-private inner class or private constructor that needs to be `pub` in Rust to support downstream test crates, the cordon is:

1. The item itself: `#[doc(hidden)] pub struct X` / `#[doc(hidden)] pub trait Y` / `#[doc(hidden)] pub fn z(...)`.
2. The containing module if it was `pub(crate)` and is now `pub`: `#[doc(hidden)] pub mod ...`.
3. Rustdoc explaining (a) why this is `pub` (Rust visibility forced it), (b) why `#[doc(hidden)]` (Java has no public surface to match), (c) which downstream caller requires it. Cite the Phase / Critic-comment SHA that originated the decision.
4. Not re-exported via `pub use` in `src/lib.rs` or the module root. If it is, the cordon is broken — the symbol re-emerges on the public surface.

**Verification grep**: `grep -rn "pub use" src/ | grep <ItemName>` should return zero hits for any `#[doc(hidden)]`-cordoned item.

In Phase 8a all three promoted items (`DefaultMetadataUpdater`, `SupportsDefaultSerializer`, `KafkaProducer::from_config`) pass all four cordon checks. Pattern works.

## Watchdog-timeout vs broker-RTT-latency confusion

The Round-2 archive for Phase 8a.0 already documented this, but Phase 8a's `close_flushes_pending_inflight` rustdoc carries the old wording ("close drains in microseconds — milliseconds at worst"). Wake-primitive latency (Notify CAS) is microseconds; end-to-end close-drain wall-clock is milliseconds (broker-fsync round-trip on `acks=all`). **When a Phase-N comment archive corrects a framing, grep follow-up phases to make sure the corrected framing propagated everywhere — rustdoc inside test files lags.**

## Critic verification fast-checks (Phase 8a Round 1, all <5 min total)

1. `cargo xtask format-check` + `cargo xtask lint` — green.
2. `cargo build --features integration-tests` — confirms scaffold + production compile together.
3. `cargo test --features integration-tests producer_smoke -- --nocapture` — one full run; confirm both tests pass; note close-drain ms.
4. `git diff <baseline>^ <head> -- src/` piped through `grep -E "^\+" | grep -iE "String|to_owned"` — quickly catches new `String` allocations in production. Filter out test/format-string noise.
5. `cargo test --lib` count before/after — quantifies test addition.
6. `grep -rn "tokio::spawn" src/ | grep -v "//\|test"` — confirms no new per-message spawn.
7. For each `#[doc(hidden)]`-cordoned item: `grep -rn "pub use" src/ | grep <ItemName>` returns zero.

All seven took <5 minutes in Phase 8a. High-yield ceiling.
