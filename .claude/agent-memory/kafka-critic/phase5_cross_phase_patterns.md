---
name: Phase 5 cross-phase review patterns
description: Recurring axes across Phase 5a/5b/5c/5d to carry into Phase 6 (Producer Internals)
type: project
---

Phase 5 is closed (5a + 5b + 5c + 5d): network primitives, TransportLayer/SslTransportLayer/KafkaChannel/ChannelBuilders, Selector + connection-state/InFlightRequests/ApiVersions/MetadataUpdater, NetworkClient + NetworkClientUtils.

**Why:** Each Phase 5 sub-phase surfaced bug patterns I'll re-encounter in Phase 6. The recurring axes below are worth carrying forward; they are not derivable from CLAUDE.md or any single file.

**How to apply:** When starting Phase 6 reviews, scan first for these axes — they have hit-rates that justify a structured pass each round.

## Recurring high-yield axes

1. **Multi-arm fan-out completeness**: when Java has `if (X) {...} else if (Y) {...}`, verify ALL arms are translated AND each arm has at least one regression test. Phase 5d's `do_send` UnsupportedVersion is the canonical example — the `is_internal && api_key==METADATA` arm was silently dropped on the first pass. Phase 6 producer code has equivalent fan-outs in `Sender.completeBatch` (transactional vs idempotent vs neither).

2. **Test name overclaim — assertion scope < name**: e.g. `disconnect_marks_node_failed_AND_RESPECTS_BACKOFF` only asserted `connection_failed=true`, not the backoff windows. Always verify: does the test body assert *every clause* in the name? If it asserts only one clause, either rename the test or extend the body. Phase 6 examples likely: `transaction_aborted_clears_in_flight_AND_resets_producer_id`.

3. **Mock-fixture default divergence from Java**: e.g. `make_request` test fixture had `expect_response=true` while Java uses `false`. Mock fixture defaults are sticky — a future test author copies the fixture and inherits the divergence. Phase 6 will introduce `MockProducer` and `MockTransactionManager`; audit constructor defaults against Java's first-use site.

4. **Untested 60-line code paths**: large translated methods (`do_send` UnsupportedVersion arm: ~25 LOC; `least_loaded_node`: ~60 LOC; `handle_api_versions_response`: ~40 LOC) frequently have ZERO direct tests because the trait surface accidentally elides them. Grep the diff for non-trivial methods (>20 LOC) and trace the test surface coverage explicitly.

5. **"Deferred to a later phase" tracking integrity**: Phase 5 deferred sites (telemetry, transactional, rebootstrap, NodeApiVersionsTest, KIP-511 fallback) are scattered across 5a–5d. Each round, verify the deferral notes still cover what the actor claims they cover. If a deferral note says "covered by NodeApiVersionsTest in 5d", check that test exists in 5d before signing off Phase 5.

6. **Wire-protocol byte-vs-round-trip**: Phase 5 had multiple instances where round-trip tests passed but byte-level encoding diverged from Java. Phase 6 producer batches use the v2 record-batch format — re-test against captured Java fixtures, not just round-trips.

7. **Cancellation safety in `tokio::select!`**: Phase 5c-2 surfaced this for Selector polling; Phase 6 producer's `Sender.run` loop will have `select!`-style waits on (a) client.poll, (b) record accumulator `wait_for_data`, (c) shutdown signal. Each branch must be cancellation-safe — no in-progress mutation that gets dropped on a competing branch firing.

8. **Hot-path allocation regression**: CLAUDE.md DoD #10 mandates an audit. Phase 5d's `do_send` still serializes header+body to a contiguous `Vec<u8>` then wraps as `ByteBufferSend` — Phase 6 must rip this out and use `SendBuilder` zero-copy directly.

9. **`pub` vs `pub(crate)` discipline**: Phase 5b-3 (`KafkaChannel`, `mute()`, etc.) and 5c-1 (`ClusterConnectionStates`) both had over-broad visibility. Java package-private translates to Rust `pub(crate)`. Phase 6 internal classes (`RecordAccumulator`, `Sender`, `BatchAppendCallbacks`) must follow.

10. **`Arc<str>` end-to-end**: producer hot path (CLAUDE.md rule 11) — topic names, client IDs, transactional ids must be `Arc<str>`, not `String`. Phase 5 enforced this for `client_id`/`destination`. Phase 6 will introduce `topic`-keyed deques and partition-leader maps; verify no silent `String` clones.

## Specific Phase 6 watch-list (carryover obligations)

- `DefaultMetadataUpdater` (Phase 5d stubbed `ManualMetadataUpdater` only) — verify Phase 6 wires `handle_failed_request`, `handle_successful_response`, and `request_update_for_new_topics`.
- `Sender.completeBatch` exception fan-out (UnknownTopicOrPartition, NotEnoughReplicas, InvalidProducerEpoch, etc.) — each error code MUST have its own arm-specific test, not a generic "any error" assertion.
- `RecordAccumulator.append` fast-path: zero allocation per record (no String clone on topic name, no Box<dyn Future>, no per-record spawn).
- `ProducerBatch.done`: callback fan-out for batch-of-N records — each record's `RequestCompletionHandler` must fire exactly once at the same lifecycle point as Java.
- Idempotent producer epoch bump on `ProducerFencedException` — Java's `TransactionManager.maybeUpdateLastAckedSequence` has tricky ordering; verify the Rust translation orders the epoch bump BEFORE the in-flight reset.
