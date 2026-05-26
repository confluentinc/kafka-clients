---
name: Phase-6e Round-2 review patterns
description: Verified-good fix shapes from Round-2 6e + a defective regression-test pattern (panic-swallow test that bypasses the wrapped code by closing the loop first)
type: reference
---

Round-2 of Phase 6e Sender review. 8 issues filed in Round 1, all reportedly addressed in 3 fixups. Verifications:

## Verified-good fix shapes
- **Wire-error subclass audit (Issue 1)**: `is_invalid_metadata` extended from 6 → 13 subclasses with a regression test that explicitly enumerates all 13 wire-coded `InvalidMetadataException` subclasses + 6 non-subclass negatives. Verified via `grep -lr "extends InvalidMetadataException" kafka/ | grep -v Stale | grep -v NoAvailable | grep -v metadata/`. All wire codes match Java's `Errors.java`. Helper is wired at `complete_batch` line 1005, the right metadata-refresh gate.
- **Hot-path String elimination (Issue 6)**: `topic_ids_for_batches` inlined into `send_produce_request` — pre-batch `tp.topic().to_string()` removed, `topic_ids.get(tp.topic())` uses `Borrow<str>` directly. Note: `topic_ids()` itself still clones `HashMap<String, Uuid>` once per request (not per batch) — this is a one-time per send_produce_request cost, not per-batch.
- **Pool-accounting buffer-reuse test (Issue 2)**: Java asserts `assertNotSame(buffer.array(), newBuffer.array())` after expiry; Rust translation uses `pool.available_memory()` invariant before/after expiry tick. Different mechanism, equivalent invariant: if the production code mistakenly called `pool.deallocate(buffer)`, `available_memory` would increase. Test exposes the canonical KAFKA-19012 bug.
- **KIP-951 leader-info path (Issue 3)**: Two new tests via `build_produce_response_with_leader_info` helper. Both drive Sender's INTERNAL extraction path (not bypassing via external metadata mutation). Public observable post-extraction is `metadata.current_leader(tp).leader.id` and `.epoch`. Test verifies both no-info and with-info variants.

## New defects to file
- **Panic-swallow regression test bypasses the catch-unwind code (Issue 8)**: Test arms `panic_on_next_poll` then calls `initiate_close()` (sets `running=false`) BEFORE `run_loop()`. With no records appended, the drain loop is also skipped (`has_undrained=false && has_in_flight_requests=false`). So `run_loop` returns immediately without ever calling `run_once` or `poll`. The panic is armed but never triggered. The test passes because run_loop returns OK on running=false, not because the catch_unwind worked. **Fix**: append a record before initiate_close so the drain loop has work, then arm the panic; that triggers the panic in the drain loop iteration.
- **Deferred test rationale doesn't hold (Issue 4 partial)**: `testMetadataTopicExpiry` defer rationale claims "requires Java-style metadata spy harness." But the Java test uses ONLY public `metadata.containsTopic(topic)` — no Mockito spy. The Rust `ProducerMetadata::contains_topic` is `pub(crate)` and accessible from the sender test module. The test is translatable using existing Phase 4 infrastructure.

## Defensible deferrals
- `testResetNextBatchExpiry`: heavily uses Mockito `inOrder.verify(client).poll(eq(0L), …)`. Translating it requires recording poll-timeout history in MockClientImpl. Defensible defer with documented rationale; clamping logic noted as "correct on inspection."
- `testNodeLatencyStats`: requires `client.throttle(node, ms)` which Rust MockClientImpl does not model. Accumulator-level invariant covered elsewhere; sender-dispatch invariant deferred. Reasonable.

## Verified-good Cargo.toml hygiene
- `futures-util = "0.3"` is an existing project dependency. Pre-existed since initial branch. CLAUDE.md rule 1 not triggered. AssertUnwindSafe is sound for &mut self when caller documents the post-panic state caveat (loop continues; state may be inconsistent, mirrors Java).

## Audit checklist for similar fixups
1. **Wire-coded subclass audits**: grep `extends X` in kafka/, cross-reference Java `Errors.java` for wire codes, ensure regression test enumerates exhaustively.
2. **Test scaffolding for catch-unwind**: trace whether the harness (initiate_close, no records) bypasses the wrapped block. The test is broken if the wrapped block never runs.
3. **Deferred-test rationale**: check the Java test for ACTUAL Mockito usage — `spy()`, `verify()`, `InOrder`, `eq()`, etc. — vs. ordinary public API calls dressed as "spy."
4. **Hot-path String elimination**: confirm intermediate map removal but check that `metadata.topic_ids()` itself isn't a per-call clone of a HashMap (this is OK once per request but not once per batch).
5. **`#[cfg(test)]` boundary**: verify all new MockClientImpl methods are inside the test module and not exposed in production.
