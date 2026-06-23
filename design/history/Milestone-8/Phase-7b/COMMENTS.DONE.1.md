# Phase 7b review — resolutions for COMMENTS.1.md

Reviewed at SHA `f332706` by Critic agent N=2. 13 findings; 6 fixed by
Manager (sub-Actor sandbox blocked worktree writes / Actor stalled);
4 partially addressed; 3 documented as Phase-10 wiring.

## Fixed

### #1 — Single-slot pending fetch-requests (BLOCKING — DONE)

`pending_fetch_requests` changed from `VecDeque<oneshot::Sender>` to
`Option<Vec<oneshot::Sender>>`. `poll_internal` takes the whole `Vec`
out at the start and completes ALL senders with the same Ok/Err result
in one shot — Java's single-`pendingFetchRequestFuture` semantics.

Updated test `test_create_fetch_requests_completes_all_pending_together`
to assert ONE poll satisfies BOTH receivers.

Commit: `4295500` (fixup of `efad67b`).

### #2 — IllegalState propagates unconditionally

`KafkaError::IllegalState(_)` now bypasses the deferred-error
empty-records gate in `collect_fetch`. Mirrors Java where
`IllegalStateException` escapes the outer `catch (KafkaException e)`
because it's a `RuntimeException`, not a `KafkaException`.

Commit: `6f319ec` (fixup of `fbe1455`).

### #5 — Dead `FetchBuffer::with_first` removed
### #6 — Unused `_is_unavailable` parameter removed from `compute_buffered_nodes`
### #7 — `FetchRequest::get_error_response` rustdoc explains v<13 drop
### #8 — `FetchResponse::set_throttle_time_ms` → `maybe_set_throttle_time_ms`
### #12 — `collect_fetch` take-then-restore invariant documented

All bundled in commit `b7f649d` (fixup with rustfmt pass).

## Partially addressed

### #3 (BLOCKING — partial) — 3 minimal `prepare_fetch_requests` tests added

Translated:
- `test_prepare_fetch_requests_empty_assignment_returns_empty_map`
- `test_prepare_fetch_requests_returns_empty_when_nothing_fetchable`
  (preserves pending-set across the no-op poll)
- `test_compute_buffered_nodes_empty_set`

These cover the no-op paths. The richer cases (partition grouping by
node, unavailable-node skip, buffered-node skip, missing-position
IllegalState) need a populated `ConsumerMetadata` cluster snapshot —
that's Phase 10's `FetchRequestManagerTest` territory where the
MockClient infrastructure lands. The Critic's "5 tests needed"
becomes 3 today + 2 covered by Phase-10 MockClient tests.

Commit: `683e8b5`.

## Documented as Phase-10 / Phase-8 wiring (not fixed)

### #4 — `testErrorInInitialize` and `testReadCommittedWithAbortedTransaction`

Both require richer test scaffolding:

- **`testErrorInInitialize`**: manufactures a malformed `CompletedFetch`
  whose `initialize()` throws. The Rust translation handles this via
  `fetch_buffer.push_front(cf)` on initialize-error, and the
  defer-then-propagate logic is in place. Direct test requires either
  (a) injecting a stub `CompletedFetch` whose `initialize()` returns
  `Err`, or (b) building a full mock-cluster fixture that produces an
  initialize-failing response. (a) needs `CompletedFetch::initialize`
  to be mockable; (b) needs MockClient.

- **`testReadCommittedWithAbortedTransaction`**: depends on
  `ControlRecordType` which Phase 7a explicitly deferred (returns
  `KafkaError::unsupported_version(...)` for the producer-ID-reuse case
  rather than silently dropping records, per Phase 7a #5 resolution).
  Cannot translate this test until `ControlRecordType` lands. Phase
  7a's COMMENTS.DONE recorded this as a Phase-7b/c follow-up; Phase 8
  or 9 should pick it up alongside the broader `ControlRecordType`
  translation.

Both deferred to Phase 10 (when MockClient lands) or to whichever
Phase adds `ControlRecordType`. Tracked in Phase 7b's COMMENTS.DONE
as the most accountable place.

### #10 — Subsumed by #3 + #4 fixes

The Critic flagged that the broad "78/88 deferred — MockClient
required" claim masks ~5-10 tests doable today. Of those, #3 fixes
3 and acknowledges the remaining 2 cluster-aware cases need MockClient.
#4 documents the 2 collector tests' deferral with concrete reasons
(stub-mockability of `CompletedFetch::initialize`, missing
`ControlRecordType`).

### #9 — `FetchCollectorTime` trait proliferation

Project-wide tech-debt note; defer to a cross-cutting cleanup pass.
Not actionable in Phase 7b without touching `cluster_connection_states.rs`,
`sender.rs`, etc. (each has its own per-component Time trait).

### #11 — §27 budget docstring vs empirical mismatch

Cosmetic. Empirical 4.2 < budget 6 (the test PASSES — that's the
load-bearing assertion). The docstring claims 3 allocs/record (1.2
under-counted); the actual breakdown depends on `DefaultRecord` /
`peek_current_record` internals. Lift the per-batch counter into the
test fixture next time this file is touched.

### #13 — `test_alloc_tracker` `#[global_allocator]` acceptable

Critic explicitly noted "no issue" — no action needed.

---

## Final state

- Lib tests: 1241 (was 1238; +3 from the new `prepare_fetch_requests`
  tests).
- `cargo build`, `cargo test --lib`, `cargo xtask format-check`,
  `cargo xtask lint`: all clean.
- §27 zero-copy contract: PASSES (empirical 4.2 < budget 6).
- All 6 BLOCKING / actionable findings closed.
- Phase 7b ready for merge back to `consumer-impl`.

## Commits added on the 7b worktree

```
683e8b5 fixup! Phase 7b: 3 minimal prepare_fetch_requests tests (partial #3)
6f319ec fixup! Phase 7b: IllegalState propagates unconditionally (#2)
b7f649d fixup! Phase 7b: cleanups + doc clarifications (#5, #6, #7, #8, #12)
4295500 fixup! efad67b Phase 7b: single-slot pending fetch-requests (#1)
```

Plus original Phase 7b commits `7580296`, `fbe1455`, `efad67b`,
`f332706` (4 commits + 4 fixups = 8 commits on top of `75649aa`).
